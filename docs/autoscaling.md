# Autoscaling a Kodansu fleet

This is what to scale on, why the obvious signals are wrong, and what a
scaled-up replica costs the client that lands on it (#362).

It assumes the fleet is already interchangeable: any replica serves any group,
with no owner, no peer discovery and no per-group state (#359, #360). Before
that, every scaling event moved ~1/N of the groups to a replica whose state was
cold, so autoscaling and forwarding could not both be enabled.

## Why the obvious signals are wrong

**Connection count.** Kafka clients hold persistent connections and heartbeat
continuously. An idle consumer group looks identical to a busy one, and the
count never falls to zero while any client stays attached.

**Requests in flight.** A broker holds requests open by design: `Fetch` waits out
`max.wait.ms`, `JoinGroup` and `SyncGroup` long-poll across the rebalance
window. A fleet serving nothing but empty fetches reports the same
requests-in-flight as one saturated with produce.

**CPU.** Honest as a backstop, and useless as the primary: the broker's work is
dominated by object-store round trips, so a replica can be fully occupied at low
CPU and idle at moderate CPU while a maintenance pass compacts.

## The signal

Two up-down counters, and the scaler subtracts:

| metric | meaning |
|---|---|
| `tansu_requests_in_flight` | requests being served — working **or** parked |
| `tansu_requests_parked` | requests inside a long poll: waiting on a clock or on another client |

**Busy = in flight − parked.** It is a *concurrency* figure, not a throughput
one, which is what makes the target value portable: it is directly comparable to
how much work a replica can have in progress, and it does not have to be
re-derived when the hardware or the record size changes. It is zero for a fleet
holding a thousand idle long polls, and it moves the moment real work arrives.

What counts as parked:

- a `Fetch` inside `max.wait.ms` with nothing to return;
- a `JoinGroup` or `SyncGroup` waiting out a rebalance window.

What does **not**:

- a request waiting on the object store. That is work in progress, and it is
  work a second replica could be doing in parallel — which is exactly what the
  scaler should react to. The signal is therefore closer to "requests this
  replica is responsible for right now" than to CPU utilisation, and that is
  deliberate.

`tansu_requests_parked` is emitted from two crates — the fetch poll lives in the
storage engine, the group polls in the broker — so it arrives under two
instrumentation scopes. `sum()` over it, as the expressions below do across
replicas anyway.

Throughput cross-checks already exist and are worth graphing next to it:
`tansu_bytes_received`, `tansu_bytes_sent`, `tansu_request_duration`.

> `tansu_request_duration` is a **latency** SLI, not a load signal. A 5-second
> empty fetch records 5 000 ms and did no work; reading that histogram as
> saturation is the same mistake as reading requests-in-flight.

## Getting it to Prometheus

Metrics are **pushed over OTLP** — there is no scrape endpoint (see
[Observability](../README.md#observability)). Point `--otlp-endpoint-url` at a
collector, or at Prometheus itself with its OTLP receiver enabled, which is what
`compose.yaml` wires up locally:

```shell
just otel-up
```

Both counters are OTLP *sums* with delta-friendly semantics; the Prometheus OTLP
receiver lands them as gauges named `tansu_requests_in_flight` and
`tansu_requests_parked`, one series per replica.

An instrument is created on its first recording, so a replica that has never
parked a request has **no series at all** rather than a series at zero. Every
expression below therefore ends in `or vector(0)`.

## The scaler

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: kodansu
spec:
  scaleTargetRef:
    name: kodansu
  minReplicaCount: 2
  maxReplicaCount: 20
  # Above the drain (#361), so a replica KEDA removes finishes what it is
  # holding rather than being cut. See `terminationGracePeriodSeconds` in the
  # deployment, which must also exceed it.
  cooldownPeriod: 120
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 300
  triggers:
    - type: prometheus
      # Set explicitly, and read the note below before changing the query.
      metricType: AverageValue
      metadata:
        serverAddress: http://prometheus.monitoring.svc:9090
        # Busy requests across the whole fleet: in flight, minus the ones only
        # waiting. A *total*, deliberately not divided by replica count.
        query: |
          sum(tansu_requests_in_flight) - sum(tansu_requests_parked or vector(0))
        threshold: "8"
```

> **The query is a fleet total and `threshold` is the per-replica target.** With
> `metricType: AverageValue` — KEDA's default for this scaler — the HPA computes
> `ceil(query / threshold)`, so the division by replica count happens *there*.
> Dividing in the query as well divides twice: a fleet of ten replicas at 7.9
> busy each asks for `ceil(7.9 / 8)` and is scaled to **one**. The failure is
> silent, immediate and total, which is why `metricType` is written out here
> rather than left to the default.
>
> The other consistent pairing is `metricType: Value` with a query that *does*
> divide by `count(tansu_requests_in_flight)`. Either works; mixing them does
> not.

**`minReplicaCount: 2`, not 0.** On a mutualised multi-tenant fleet scale-to-zero
only fires when *every* tenant is idle, so in production it effectively never
does; the value here is N↔N+1. Two rather than one because a single replica has
no headroom for a rolling restart. Scale-to-zero remains a real dev/staging
property and a correctness consequence of statelessness — set
`minReplicaCount: 0` there if it is useful.

**The threshold is a starting point, not a recommendation.** Eight busy requests
per replica is a plausible first target for the object-store-bound workload this
was written against; measure yours against `tansu_request_duration` and move it.

For one data point from a real fleet: a ten-replica production broker measured
**121 requests in flight, 42 of them parked** — so 79 busy, 7.9 per replica
(p50 7.8, p90 9.2). A third of what requests-in-flight reported was waiting on
nothing, which is the whole argument for subtracting. Eight lands that fleet at
about the size it was already running at by hand, which is what makes it a
usable starting value rather than a confirmation of anything.

**Scale-down is deliberately slow.** A five-minute stabilisation window and a
two-minute cooldown, because removing a replica costs its clients a reconnect
even with the drain, and a fleet that oscillates pays that repeatedly for
nothing.

## Cold start

A scaled-up replica has to answer `ApiVersions`, `Metadata` and
`FindCoordinator`, and read the group objects, inside the client's
`request.timeout.ms` — 30 s by default.

Measured, from nothing to a `Metadata` answered over a real socket
(`tansu-broker/tests/cold_start.rs`, over `memory://`):

| | |
|---|---|
| build — storage container, schema assertion, coordinator | ~2 ms |
| **to a client's first answered request** | **~11 ms** |
| client budget | 30 000 ms |

Three orders of magnitude of headroom, which is the answer the issue wanted
measured rather than assumed. Two honest caveats: this is over `memory://`, so a
real cold start adds the object-store round trips for the schema object and the
topic metadata — tens of milliseconds, not seconds — and it excludes the
container image pull, which is Kubernetes' half of the budget and by far the
larger one. **If a scale-out is ever slow, look at the image pull first.**

The test asserts a 3-second bound, far above the measurement and far below the
client's. It is there to catch a change in *kind* — a listing on the startup
path, a synchronous warm-up — rather than to pin a number that varies with the
machine.

## What is not covered here

A load test that scales a real fleet up and down on EKS. Everything above is
measurable in the repository; that one needs a cluster, and the numbers it
produces belong next to this table when it has been run.
