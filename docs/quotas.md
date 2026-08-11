# Client quotas

How much a principal may ask of a Kodansu fleet, and what is honest about the
answer (#384).

`throttle_time_ms` used to be a hardcoded zero on every response — forty-eight
call sites — and nothing anywhere rate-limited anything. On a disk-backed broker
that is a fairness problem. Here it is a cost problem: what this broker spends
money on is object-store requests, and #40, #108 and #140 were all about cutting
that rate. Nothing stopped one client from putting it back.

#363 gave a mutualised fleet an authorization boundary. Authorization says
*whether* a principal may write; a quota says *how much*. For a fleet with more
than one tenant they are two halves of the same boundary.

## Off unless authentication is on

No principal, no quota — the same switch `authorized()` uses, for the same
reason. A broker started without `--authentication` has no principals, so there
is nothing to write a limit against, and it behaves exactly as it did before
this existed. So does an authenticated connection whose principal no quota
names.

## What can be limited

| key | unit | counted from |
|---|---|---|
| `producer_byte_rate` | bytes/second | the record batches a `Produce` **request** carries |
| `consumer_byte_rate` | bytes/second | the record batches a `Fetch` **response** returns |
| `request_rate` | requests/second | every request, of every API |

The first two are Kafka's own spellings, so `kafka-configs.sh` and `rpk`
configure them unchanged. Bytes are counted from the record batches rather than
the frame: that is what Kafka's quotas measure, it is what an operator sizes a
limit against, and it does not make a client pay for this broker's framing.

A fetch is charged what it *returned*, not what it asked for. Charging
`max_bytes` would throttle a caught-up consumer that is reading nothing.

**`request_rate` is not Kafka's `request_percentage`.** That one is a share of a
request-handler thread's time, and this broker has no such pool to divide. What
a request costs here is object-store operations, which track the *count* of
requests and not the time they took. `request_percentage` is refused rather than
accepted and quietly measured as something else — it is the one dimension that
sees a metadata storm, so getting it wrong would matter.

Any other key is refused with `INVALID_CONFIG`. Entities other than `user` —
Kafka's `client-id` and `ip` — are refused with `INVALID_REQUEST`: a client id
is chosen by the client and can be changed by it, and an address is the load
balancer's, not the tenant's.

## Configuring

Over the standard admin APIs, `AlterClientQuotas` and `DescribeClientQuotas`
(KIP-546). Their codecs were already generated from the Kafka descriptors and
routed by nobody — the same gap ACLs had before #363 and SCRAM had before #381.

```shell
# One tenant.
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type users --entity-name alice \
  --add-config 'producer_byte_rate=10485760,consumer_byte_rate=10485760'

# Everybody else.
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type users --entity-default \
  --add-config 'producer_byte_rate=1048576,request_rate=500'

# Read it back.
kafka-configs.sh --bootstrap-server localhost:9092 \
  --describe --entity-type users --entity-name alice

# Take it away.
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type users --entity-name alice \
  --delete-config 'producer_byte_rate'
```

`rpk cluster quotas alter --default user --add producer_byte_rate=1048576` does
the same thing.

Writing quotas needs `ALTER_CONFIGS` on the cluster and describing them needs
`DESCRIBE_CONFIGS`, as Kafka requires. A principal that can raise its own quota
has no quota.

A named principal falls back to the default **per key**: give `alice` a
`producer_byte_rate` of her own and she keeps the cluster default's
`consumer_byte_rate`. Most-specific-entity-wins would silently unlimit a
consumer the moment somebody limited its producer.

`--describe` reports what was *written*, not what is in force. A principal
nothing names describes empty even though the default applies to it — reporting
the default under its name would show a limit no `--alter` wrote and no
`--delete-config` can remove.

### A default from the command line

So that a broker with no control plane in front of it is still protected by
something:

```shell
tansu broker --authentication \
  --quota-producer-byte-rate 1048576 \
  --quota-consumer-byte-rate 4194304 \
  --quota-request-rate 500
```

`QUOTA_PRODUCER_BYTE_RATE`, `QUOTA_CONSUMER_BYTE_RATE` and `QUOTA_REQUEST_RATE`
are the environment equivalents. Anything the cluster's own quotas configure
overrides these: the control plane, when there is one, is the authority.

Omit an option for no limit. `-1` is refused rather than read as "unlimited" —
next door, `retention.ms=-1` *does* mean forever, and a broker that took it here
as a limit of nothing would refuse every produce on the fleet.

## Where the wait happens

KIP-219, and this is the part that is not incidental.

The delay is computed, the response is answered **immediately** with it in
`throttle_time_ms`, and the connection is then muted for that long *between*
requests. Nothing sleeps inside a request.

That is because of #362. A throttled request is waiting, not working. A throttle
that slept inside a request would be counted in `tansu_requests_in_flight` — in
flight, not parked — so a fleet deliberately refusing traffic would report
itself as a fleet saturated by it, and the scaler would add replicas to serve
load the broker has just decided not to serve. It would also fold the throttle
into `tansu_request_duration`, which is a latency SLI: `docs/autoscaling.md`
already says a 5-second empty fetch records 5 000 ms and did no work.

The wait is not counted as `Parked` either. Parked is *subtracted* from in
flight to get `busy`, and counting a wait that was never counted as in flight
would push that expression negative.

So a fleet throttling hard reports `busy` at or near zero, and
`tansu_request_duration` percentiles are unchanged by throttling. The time shows
up in exactly one place:

| metric | meaning |
|---|---|
| `tansu_throttled_requests` | requests answered with a non-zero `throttle_time_ms` |
| `tansu_throttled_time` | milliseconds connections have spent muted |

Graph those next to the busy expression. A fleet that is *not* scaling because
it is throttling looks, on the busy signal alone, exactly like a fleet with
nothing to do — these two are the difference.

## How a limit converges

A token bucket per principal per dimension, held in memory on the replica:

- one second of unused rate may be carried forward, so the first request of an
  idle client is not throttled — a rate is not a lump and a request is;
- debt is carried, so a request larger than one throttle can repay keeps being
  charged for until it is paid off. That is what makes the *sustained* rate
  equal the configured rate rather than merely non-zero;
- a single throttle is capped at **10 seconds**, so a client's next request is
  not left unread past its own `request.timeout.ms` (30 s by default). The cap
  bounds the answer, not the limit: the unpaid debt is still owed;
- debt itself is bounded at 60 seconds' worth, so a client that blasted once and
  stopped is not throttled indefinitely afterwards.

The quotas themselves are read from the object store into a snapshot held for 5
seconds — the window the ACLs use, for the same reason. A limit an operator
applies takes effect across the fleet within it, and a busy topic costs no reads
to enforce.

If a replica cannot read the quotas it serves the last snapshot it had, and the
broker's own command-line defaults if it never had one. **This fails open where
authorization fails closed**, deliberately: not throttling for a few seconds
costs money, and throttling everybody because a GET timed out is an outage.

## The honest caveat: the accounting is per replica

**A limit is enforced independently on each replica, so the fleet's effective
limit is the configured one times the replica count.**

That is Apache Kafka's semantics too — Kafka quotas are per broker — so an
operator moving from Kafka gets what they expect, and `--quota-fleet-size 1`
(the default) is that behaviour.

A genuinely fleet-wide limit needs shared accounting, and on this broker shared
means the object store: a read and a write on the hot path that exists to avoid
exactly that. It is not worth it, and there is no membership to count anyway — a
replica does not know its peers exist, which is the whole of #360.

So a fleet-wide *intent* is expressed by declaring how many replicas there are:

```shell
tansu broker --authentication --quota-fleet-size 8 --quota-producer-byte-rate 8388608
```

Every replica then enforces 1 MiB/s, and a fleet of eight allows 8 MiB/s. This
is approximate, and it is **wrong while the fleet is mid-scale**: at four
replicas the fleet allows half of what was configured, at sixteen it allows
double. Set it to the steady-state replica count, or to KEDA's
`maxReplicaCount` if under-allowing is safer for you than over-allowing.

`--quota-fleet-size` is a count and nothing more. It does not name a replica,
ask for a hostname, or require any replica to be reachable from any other; #360
removed all of that and it is not coming back.

## What is deliberately not here

Quota *policy* — which principal gets which limit — along with usage
aggregation and billing. Those belong to whatever operates a fleet, not to a
broker. What is here is the enforcement point and the standard API for
configuring it.

## See also

- `docs/autoscaling.md` — the load signal a throttle must not corrupt (#362)
- #363 — authorization, the other half of the boundary
- KIP-13 (quotas), KIP-219 (throttle before the wait), KIP-546 (the admin API)
