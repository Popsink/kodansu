<div align="center">

# Kodansu 🗃️
a stateless Kafka-compatible broker whose only durable state is an object store

<br>

[![License](https://img.shields.io/badge/License-Apache-165dfc.svg)](LICENSE)
&nbsp;
[![Built with Rust](https://img.shields.io/badge/built_with-Rust-165dfc.svg?logo=rust)](https://www.rust-lang.org/)

<br>

</div>

## What Kodansu is

Kodansu speaks the Apache Kafka wire protocol and keeps nothing of its own. There is
no local disk, no ZooKeeper, no KRaft quorum and no inter-broker replication: every
record it accepts is persisted into an object store — [S3][aws-s3], Google Cloud
Storage, or an in-memory store for tests — and durability is the bucket's. S3 alone is
designed to exceed [99.999999999% (11 nines)][aws-s3-storage-classes].

**Every replica is interchangeable.** All of them report themselves as node `111`
behind the same advertised listener, so a client cannot tell them apart — it talks to
whichever one the load balancer picked. Replicas coordinate *only* through the object
store, so scaling is `kubectl scale`: no ordinals, no StatefulSet, no membership list,
nothing to rebalance.

It ships as one statically linked binary:

| Subcommand | Role |
|---|---|
| `broker` | the Kafka API broker — default when no subcommand is given |
| `topic` | create, list and delete topics |
| `user` | administer SASL/SCRAM credentials |

Kodansu is Popsink's fork of [tansu-io/tansu][github-com-tansu-io], driven by one
workload: **CDC with high topic fan-out** — thousands of topics, a handful of records
each per poll. At that shape the bill is S3 *requests*, not bandwidth or storage, and
an object-per-produced-batch layout is what makes it expensive. Most of what follows
is a consequence of that.

## Hosted

There is also a managed Kodansu at **[kodansu.com][kodansu-com]** — a cluster on
infrastructure that is not yours to operate, reachable at a bootstrap URL with
SASL/SCRAM credentials.

It is **concierge** right now, which is a deliberate stage rather than a missing
feature: a person reads each request and provisions the cluster by hand. So there is
no signup, no console and no price yet, and no SLA is offered because none would be
honest while that is true. It is free while it lasts, and the point of it is to find
out what breaks against workloads that are not ours.

Running it yourself is what this repository is for, and nothing here is held back to
make the hosted one worth paying for: the broker enforces limits, the control plane
that decides them and bills for them is what is not in this tree.

## How Kodansu diverged

Relative to the tree it forked from:

- **Object store only.** The PostgreSQL, libSQL, SlateDB and Turso backends are gone,
  and so are the schema-registry validation (Protobuf / Avro / JSON Schema) and the
  Iceberg / Delta / Parquet lakehouse sinks (#96). One storage abstraction, one real
  backend.
- **Prefix-coalesced segments are the only layout written.** The per-topic
  `records/{topic}/{partition}/…` object-per-batch layout has been removed from the
  write path entirely (epic #171). See [Storage model](#storage-model) below.
- **Leaseless multi-writer produce.** Any replica may append to any prefix: the
  create-only segment-sequence CAS *is* the offset arbiter (#82/#86). No lease, no
  fencing epoch on the hot path, no produce routing, no broker-to-broker membership.
- **Retention and compaction are per prefix and whole-segment**, run on the
  maintenance loop of every replica, coordinated by first-arrival recency skip rather
  than by an ordinal or a coordinator.
- **Consumer groups need no configuration and no pod identity.** A group's state is
  three objects with three write regimes, so any replica may coordinate any group
  (#359) — there is no owner to elect, no peer set to discover and no second
  listener (#360).
- **Kafka retention semantics.** An absent `cleanup.policy` reads as Kafka's `delete`
  with a 7-day `retention.ms`, and both defaults are settable broker-wide — including
  retain-forever. This is a behaviour change worth reading twice; see
  [Retention](#retention).
- **Metrics are OTLP push only.** There is no Prometheus scrape endpoint any more.

## Quickstart

Kodansu picks up any existing environment and loads what it finds in `.env`. Copy the
template first:

```shell
cp example.env .env
```

The `just` recipes bring up their own infrastructure. For an S3 broker on a local
[minio][min-io] — this creates the bucket and starts the broker:

```shell
just broker-s3
```

> The shipped `AWS_ENDPOINT` is `http://minio:9000`, which is minio's name *inside*
> Compose. `just broker-s3` runs the binary on the host, so set
> `AWS_ENDPOINT="http://localhost:9000"` in `.env` first — `example.env` has the line
> commented out and ready.

For an ephemeral broker with no infrastructure at all:

```shell
just broker-memory
```

`just broker` runs a debug broker with whatever `.env` says, alongside the full
observability stack (minio, Prometheus, Grafana). Either way the broker listens on
`localhost:9092` and the ordinary Kafka CLI works against it:

```shell
kafka-topics \
  --bootstrap-server localhost:9092 \
  --partitions=3 \
  --replication-factor=1 \
  --create --topic test
```

```shell
echo "hello world" | kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic test
```

```shell
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --group test-consumer-group \
  --topic test \
  --from-beginning \
  --property print.offset=true \
  --property print.partition=true
```

`kafka-topics --describe` reports node `111` as leader and sole ISR for every
partition. That node is whichever replica handled your request — **all replicas are
node 111**.

## Storage model

Everything Kodansu writes is an immutable **segment** object:

```
clusters/{cluster}/prefixes/{org.env.conn}/segments/{seq:020}.seg
```

A **prefix** is the first three dotted components of a topic name — for CDC, one
connector. A flush window produces exactly **one object per prefix**, multiplexing the
batches of every `(topic, partition)` sub-stream that arrived in that window. This is
the point of the design: PUTs collapse from roughly `(topics × flushes)` to
`(connectors × flushes)`, and fetch is served from a footer index plus one ranged GET
instead of a per-fetch `LIST`.

A topic whose `cleanup.policy` contains `compact` is the exception: it is routed to a
prefix equal to its own name, so per-key compaction never has to look at another
topic's keys. Each topic's routing is pinned create-only on first use, so it can never
drift between replicas.

Four properties matter to anyone operating or reading a bucket:

- **Create-only.** No object is ever mutated, which is also why GCS is safe here: the
  ~1 mutation/s/object cap is never approached on the produce or fetch path.
- **Self-describing.** Each segment carries a footer index listing every sub-stream's
  offset range, byte range and max timestamp. A reader resolves
  `(topic, partition, offset)` from the footer, never from the object name.
- **Sequence order is not offset order.** Compaction writes a merged segment covering
  old offsets under a fresh, higher sequence, so a high sequence can hold low offsets.
- **Retention is whole-segment and per prefix**, under the longest `retention.ms`
  among the prefix's topics. Compaction merges cold segments into fewer, larger ones
  to keep the footer index bounded.

The segment frame and footer are a **published contract**, not an internal detail —
an external S3-direct reader can decode any sub-stream from the object alone. It is
specified in [docs/virtual-topics-format.md](docs/virtual-topics-format.md); the
design rationale and the leaseless arbiter are in
[docs/design-multiwriter-segments.md](docs/design-multiwriter-segments.md).

## Running the broker

The broker subcommand is the default, and every option has a default:

```shell
tansu
```

The options that matter in a deployment, each with its environment variable:

| Option | Env | Default | Meaning |
|---|---|---|---|
| `--cluster-id` | `CLUSTER_ID` | `tansu_cluster` | All replicas of one cluster must agree. Also the `clusters/{cluster}/` object prefix. |
| `--listener-url` | `LISTENER_URL` | `tcp://0.0.0.0:9092` | Where the broker listens. |
| `--advertised-listener-url` | `ADVERTISED_LISTENER_URL` | `tcp://localhost:9092` | What clients are told in metadata — the load balancer, not the pod. |
| `--storage-engine` | `STORAGE_ENGINE` | `memory://tansu/` | Storage URL; see below. |
| `--otlp-endpoint-url` | `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Where metrics and traces are pushed. |
| `--authentication` | — | off | When present, clients must authenticate. |
| `--cert` / `--key` | — | — | TLS certificate chain and private key (PEM). Both or neither. Given both, `--listener-url` serves TLS only and a plaintext client is refused; an unreadable or mismatched pair fails startup. |
| `--default-cleanup-policy` | `DEFAULT_CLEANUP_POLICY` | `delete` | Applied to topics created without one. |
| `--default-retention-ms` | `DEFAULT_RETENTION` | `7days` | Applied to `delete`-policy topics created without a `retention.ms`. Accepts a duration, or `-1`/`infinite`/`forever`. |
| `--super-users` | `SUPER_USERS` | — | Principals allowed everything without consulting an ACL, comma separated (`User:admin,User:ops`). Only meaningful with `--authentication`. |
| `--quota-producer-byte-rate` | `QUOTA_PRODUCER_BYTE_RATE` | — | Default produce bytes/second for a principal the cluster's quotas do not name. Only meaningful with `--authentication`. |
| `--quota-consumer-byte-rate` | `QUOTA_CONSUMER_BYTE_RATE` | — | Default fetch bytes/second, likewise. |
| `--quota-request-rate` | `QUOTA_REQUEST_RATE` | — | Default requests/second, likewise. |
| `--quota-fleet-size` | `QUOTA_FLEET_SIZE` | `1` | How many replicas a configured quota is shared between. `1` enforces each limit on every replica, as Apache Kafka does. |

The URL and address options accept `${VAR}` references, expanded when the argument is
parsed.

### Storage engines

The engine is chosen by URL scheme:

| Scheme | Use |
|---|---|
| `s3://bucket/` | S3, minio, or any S3-compatible store |
| `gs://bucket/` | Google Cloud Storage |
| `memory://name/` | in-process; tests, demos, local experiments |
| `null://name/` | discards writes; for isolating broker cost in benchmarks |

Credentials come from the environment following the usual `object_store` conventions
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, and
`AWS_ENDPOINT` + `AWS_ALLOW_HTTP` for a local minio).

The storage URL's **query string tunes the write path** — coalescing thresholds,
compaction triggers, maintenance coordination:

```
s3://my-bucket/?coalesce_linger=300ms&coalesce_batches=128&coalesce_bytes=4m
```

Every key, its default and how to choose a value is in
[docs/storage-tuning.md](docs/storage-tuning.md).

### Retention

An **absent `cleanup.policy` is read as Kafka's `delete`**, and a `delete`-policy topic
with no `retention.ms` gets the broker default of 7 days. Clearing
`DEFAULT_CLEANUP_POLICY` does *not* buy infinite retention — the engine still reads an
absent policy as `delete`.

To retain forever, say so explicitly:

- per topic: `retention.ms=-1`, the engine's only spelling of retain-forever;
- broker-wide: `DEFAULT_RETENTION=forever` (`-1` and `infinite` are accepted too).

### Topics

```shell
tansu topic create taxi --partitions 3
tansu topic list
tansu topic delete taxi
```

`--config key=value` may be repeated to set topic configuration at creation.

### Authentication

Start the broker with `--authentication` and clients must authenticate with
SASL/SCRAM. Credentials are administered over the Kafka `AlterUserScramCredentials`
API:

```shell
tansu user create alice hunter2 --mechanism scram512
tansu user delete alice --mechanism scram512
```

`scram256` and `scram512` are supported; `scram512` is the default.

Credentials are stored under the cluster's own prefix, one object per principal
per mechanism, so every replica sees a user the moment it is created and a
deletion takes effect everywhere without waiting anything out. What is stored is
what SCRAM checks a proof against, never the password — but it is still enough
to impersonate the user, so **the bucket's access control is what protects it**,
exactly as it is for the committed offsets beside it.

> **Create the first users before turning `--authentication` on.** `tansu user`
> speaks plaintext and has no SASL options, so it cannot talk to a broker that
> is already refusing unauthenticated clients — and a broker with authentication
> on and no users is one nobody can connect to. Start the cluster without the
> flag, create the users, then restart with it. The same order applies to
> `--super-users`: the principal named there has to exist.

## Running more than one replica

Produce, fetch and maintenance need no configuration to scale:

- **Produce** — any replica may append to any prefix. The create-only
  segment-sequence CAS assigns offsets, so there is no lease to acquire and no
  request to route. A writer that loses the create race folds the winner's footer,
  re-derives its sub-stream bases and retries at the next sequence.
- **Fetch** — any replica, from the immutable segments and their cached footers.
- **Maintenance** (retention and compaction) — runs on every replica. Each tick a
  replica enumerates prefixes in its own shuffled order and skips any prefix a peer
  maintained within `maintenance_recency`, so N maintainers partition the work by
  first arrival rather than duplicating it N×. Set `maintenance_recency` to ~0.9× your
  maintenance interval.

**Consumer groups need nothing either**, and that is a change: they used to be the
one thing on this page that did. A group's state was a single object mutated by etag
CAS, so with N replicas behind one load balancer its Join and Sync long-polls
scattered across replicas and thrashed that CAS until membership never quiesced. The
answer was to route each group to a deterministic owner, which needed stable pod
identity, headless-Service DNS, pod-to-pod addressability and a second listener — the
four things an autoscaled fleet has least of.

That is gone. A group is now three objects with three write regimes (#359): each
member's liveness and subscription in its own document, written by that member alone
at most once per session/2; the group's composition in one CAS'd object that changes
only when the membership does; and the leader's assignment written **create-only**,
so the write that liveness churn used to starve has no etag to lose a race on. There
is nothing left for concurrent replicas to contend on, so there is nothing to route
(#360).

The practical consequence: **replicas are interchangeable.** One can be added or
removed under consumer load and the groups it was serving do not notice — no cold
owner, no DNS convergence window, no configuration to keep in step.

### Quotas

Authorization says *whether* a principal may write; nothing said *how much*, and
`throttle_time_ms` was a hardcoded zero on every response. On a broker whose
cost is object-store requests, the request rate against the object store was a
property of who happened to be connected rather than of anything configured
(#384).

Three dimensions — `producer_byte_rate`, `consumer_byte_rate` and a
`request_rate` — written against the `user` entity over the standard admin APIs,
so `kafka-configs.sh` and `rpk` configure them unchanged:

```shell
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type users --entity-default \
  --add-config 'producer_byte_rate=1048576,request_rate=500'
```

Like authorization, it is armed by `--authentication` and off without it: no
principal, nothing to write a limit against.

The throttle is **answered, then waited for** (KIP-219). The response goes out
immediately carrying `throttle_time_ms`, and the connection is muted for that
long *between* requests — never inside one, because a request delayed in flight
would tell the autoscaler above that a fleet refusing traffic is a fleet
saturated by it. `tansu_throttled_requests` and `tansu_throttled_time` are where
the wait shows up instead.

**[docs/quotas.md](docs/quotas.md)** has the keys, the `kafka-configs.sh`
invocations, the convergence behaviour, and the honest caveat: the accounting is
per replica, so a fleet's effective limit is the configured one times the
replica count unless `--quota-fleet-size` is set.

### Autoscaling

There is no scrape endpoint, and the two figures a scaler would otherwise reach
for both lie about a broker: connection count never falls while a client stays
attached, and requests-in-flight is inflated by long polls that are *waiting*
rather than working. Kodansu exports both halves of the correction —
`tansu_requests_in_flight` and `tansu_requests_parked` — and the signal is their
difference. **[docs/autoscaling.md](docs/autoscaling.md)** has the PromQL, a KEDA
`ScaledObject`, and the measured cold start against the client's timeout budget.

### Stopping a replica

The broker holds requests open by design: `Fetch` waits out `max.wait.ms`,
`JoinGroup` and `SyncGroup` long-poll across the rebalance window. On `SIGTERM`
a replica **drains** rather than dropping what it is holding (#361):

1. it closes its listener, so the load balancer's health check fails and a
   client arriving anyway is *refused* rather than queued in a backlog nobody
   will accept;
2. every in-flight request answers — the group long polls return what they have,
   which is what they return at their own deadline, and a `Fetch` finishes
   inside its own `max.wait.ms`;
3. connections sitting **idle between requests** are closed, which every client
   handles by reconnecting. That is what keeps the drain fast: a Kafka client
   keeps its connections open between polls, so a drain that waited for
   connections to *end* would wait out its whole grace period on every
   shutdown and then cut them regardless;
4. the process exits, or gives up after 30 seconds and cuts what is left — with
   a `WARN` naming how many requests it cut, because from the client side that
   is indistinguishable from a network fault and nothing else records it.

**Set `terminationGracePeriodSeconds` above 35.** The broker's own patience is
35 s and its drain gives up at 30; a grace period below that means the kernel
sends `SIGKILL` while the drain is still running, and the drain is decorative.

Note what it is *not* sized against: the longest long poll. It does not need to
be, because the polls are cut short by the same signal — a member waiting out
half its session timeout answers as soon as the replica is asked to stop. A
scale-in event therefore costs a round trip, not a rebalance: no generation is
minted, so no client re-partitions.

### Authorization

ACLs are Kafka's own — resource type, name, pattern type, principal, host,
operation, permission — so `kafka-acls.sh` and every operator tool work
unchanged, and `PREFIXED` is what scopes a principal to one namespace:

```shell
kafka-acls.sh --add --allow-principal User:alice \
  --operation Read --operation Write \
  --topic tenant-a. --resource-pattern-type prefixed
```

The broker has **no notion of a tenant**. It knows that a principal may touch
the resources a pattern selects; whether `alice` "is" tenant A is a convention
in the rules you write.

Three things follow from Kafka's model and are worth knowing before turning it
on:

- **Enforcement follows `--authentication`.** Without it there are no
  principals, so there is nothing to evaluate and nothing is refused. That is
  also why turning authentication on is what arms authorization.
- **No rule is not permission.** A principal with nothing written about it is
  refused, which is the only tenable default for a mutualised fleet.
- **Set `--super-users`.** Those two together mean a cluster with no ACLs
  refuses `CreateAcls` like everything else, and can never be given any. A super
  user is the way in. The broker warns at startup if none is configured. Write
  it the way a rule is written — `User:admin`, not `admin` — because it is
  compared against the same principal a rule names.

A grant of `READ` also grants `DESCRIBE` — a client that may read a topic must
be able to see it exists — but a *denial* of `READ` does not deny `DESCRIBE`.
Implication runs one way, as it does in Kafka.

Enforced today on **produce**, **fetch**, **CreateTopics**, **DeleteTopics** and
the three **ACL APIs** themselves. Two things follow from Kafka's model and
surprise people:

- **Creating a topic needs `CREATE` on the cluster**, not on the topic. The
  topic does not exist yet, so there is nothing for a topic rule to select — and
  a rule on a name nobody has taken would be a rule on the whole namespace. On a
  mutualised fleet, creating topics is an operator's job.
- **Reading the ACLs needs `DESCRIBE` on the cluster.** The rules say what every
  principal may do, which on a mutualised fleet names the other tenants.

`Metadata` and `ListGroups` are filtered, so a principal does not see the names
of topics and groups it may not describe — on a mutualised fleet, the list of
topics is the list of tenants. The two shapes answer differently, as Kafka
answers them: a topic a client **named** comes back with
`TOPIC_AUTHORIZATION_FAILED`, because silence would read as "does not exist"
and send it to create the topic instead of fixing its ACLs; a request that
lists **everything** simply omits what it may not see.

The consumer group APIs take `READ` on the group — `JoinGroup`, `SyncGroup`,
`Heartbeat`, `LeaveGroup`, `OffsetCommit`, and `DESCRIBE` for `OffsetFetch`.
`DeleteGroups` takes `DELETE`, because a principal that may participate in a
group has no business destroying it.

One coarseness worth knowing: `OffsetFetch` naming several groups is refused as
a whole if any one of them is, because its response carries a single error code
across every group it answers. Ask for one group at a time to see them
individually.

## Observability

Metrics and traces are **pushed over OTLP** to `--otlp-endpoint-url`. There is no
scrape endpoint. The Compose stack wires this up locally — Prometheus with its OTLP
receiver enabled and Grafana with provisioned dashboards:

```shell
just otel-up      # minio + Prometheus + Grafana + the broker container
just grafana-ui   # opens http://localhost:3000
```

`just jaeger-up` adds Jaeger for traces, on `http://localhost:16686`.

## Documentation

| Document | What it covers |
|---|---|
| [docs/quotas.md](docs/quotas.md) | Client quotas: the three dimensions, configuring them with `kafka-configs.sh`, and why the accounting is per replica |
| [docs/storage-tuning.md](docs/storage-tuning.md) | Every storage-URL tuning key: coalescing, compaction, maintenance coordination |
| [docs/virtual-topics-format.md](docs/virtual-topics-format.md) | The segment frame and footer — the contract for external S3-direct readers |
| [docs/design-multiwriter-segments.md](docs/design-multiwriter-segments.md) | Why the create-only segment sequence is the offset arbiter |
| [docs/migration-scos.md](docs/migration-scos.md) | Operator runbook for the lease → leaseless cutover (historical) |
| [docs/sarama.md](docs/sarama.md) | Driving the broker with the Go Sarama client |
| [docs/rename-kodansu.md](docs/rename-kodansu.md) | Checklist for closing the `tansu` → `kodansu` gap described below |
| [CLAUDE.md](CLAUDE.md) | Repository layout, crate roles, build and test invocations |

## Development

`just` is the task runner and loads `.env` automatically:

```shell
just                 # fmt, build, test, clippy
just test-workspace  # the full test suite
just ci              # minio, for the integration tests
```

The `fuzz` crate needs a C++ libfuzzer toolchain that is not always present locally;
exclude it when invoking cargo by hand. [CLAUDE.md](CLAUDE.md) has the rest.

## Feedback

Please [raise an issue][kodansu-issues] if you encounter a problem.

## Attribution and license

Kodansu is a fork of [tansu][github-com-tansu-io] by Peter Morgan, and is licensed,
like tansu, under [Apache 2.0][apache-license]. Upstream copyright headers are
preserved throughout.

**On the name:** the project is Kodansu, but the binary, the crates and the container
image are all still called `tansu` (`ghcr.io/popsink/tansu`). Renaming those is a
separate, breaking change; this documentation runs ahead of it deliberately.

[apache-license]: https://www.apache.org/licenses/LICENSE-2.0
[aws-s3]: https://en.wikipedia.org/wiki/Amazon_S3
[aws-s3-storage-classes]: https://aws.amazon.com/s3/storage-classes/
[github-com-tansu-io]: https://github.com/tansu-io/tansu
[kodansu-com]: https://kodansu.com
[kodansu-issues]: https://github.com/Popsink/tansu/issues
[min-io]: https://min.io
