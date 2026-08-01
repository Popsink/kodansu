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
| `proxy` | a Kafka API proxy |
| `perf` | a produce-side benchmark |

Kodansu is Popsink's fork of [tansu-io/tansu][github-com-tansu-io], driven by one
workload: **CDC with high topic fan-out** — thousands of topics, a handful of records
each per poll. At that shape the bill is S3 *requests*, not bandwidth or storage, and
an object-per-produced-batch layout is what makes it expensive. Most of what follows
is a consequence of that.

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
- **Consumer groups survive N replicas behind one load balancer**, via optional
  forward-to-owner coordination over rendezvous hashing on headless-Service DNS.
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
| `--cert` / `--key` | — | — | TLS certificate and key (PEM). |
| `--default-cleanup-policy` | `DEFAULT_CLEANUP_POLICY` | `delete` | Applied to topics created without one. |
| `--default-retention-ms` | `DEFAULT_RETENTION` | `7days` | Applied to `delete`-policy topics created without a `retention.ms`. Accepts a duration, or `-1`/`infinite`/`forever`. |
| `--group-forwarding` | `GROUP_FORWARDING` | off | Forward each consumer group's coordination APIs to its owner replica. |
| `--group-forward-peer-dns` | `GROUP_FORWARD_PEER_DNS` | — | Headless-Service hostname whose A/AAAA records list the eligible owners. |
| `--internal-listener-url` | `INTERNAL_LISTENER_URL` | `tcp://0.0.0.0:9093` | Broker-to-broker listener that forwarded group requests reach. |
| `--pod-ip` | `POD_IP` | — | This replica's own address; in Kubernetes, from the Downward API. |

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

**Consumer groups are the one thing that needs configuring.** A group's state is a
single object mutated by etag CAS. With N replicas behind one load balancer, a group's
Join and Sync long-polls scatter across replicas and thrash that CAS, so membership
never quiesces. Enabling forwarding makes each group's coordination run on exactly one
deterministic owner:

```shell
GROUP_FORWARDING=true
GROUP_FORWARD_PEER_DNS=kodansu-headless.my-namespace.svc.cluster.local
POD_IP=<the pod's own IP, from the Downward API>
```

Owners are chosen by rendezvous hashing over the peer set discovered from that
hostname's A/AAAA records, so removing one peer reassigns only that peer's groups.
Ownership is **soft**: every write stays conditional on the object's etag, so DNS skew
during a rolling restart degrades to ordinary CAS retries, never to corruption — and
an empty or unresolvable peer set falls back to purely local coordination. Forwarding
is off by default.

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
| [docs/storage-tuning.md](docs/storage-tuning.md) | Every storage-URL tuning key: coalescing, compaction, maintenance coordination |
| [docs/virtual-topics-format.md](docs/virtual-topics-format.md) | The segment frame and footer — the contract for external S3-direct readers |
| [docs/design-multiwriter-segments.md](docs/design-multiwriter-segments.md) | Why the create-only segment sequence is the offset arbiter |
| [docs/migration-scos.md](docs/migration-scos.md) | Operator runbook for the lease → leaseless cutover (historical) |
| [docs/sarama.md](docs/sarama.md) | Driving the broker with the Go Sarama client |
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
[kodansu-issues]: https://github.com/Popsink/tansu/issues
[min-io]: https://min.io
