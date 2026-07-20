# Storage tuning (S3 / GCS object-store backend)

The object-store backend batches writes to keep the steady-state S3/GCS request
cost down. The batching thresholds are tunable per deployment from the storage
URL query string, so you can adapt them to your workload without recompiling.

Omitting every key below reproduces the shipped defaults.

## Produce coalescing (`produce_coalesce`)

With `produce_coalesce=true`, the broker buffers the batches produced to a
partition within a *linger window* and flushes them as a single `records/`
object, instead of one object per batch (#50). This cuts both the `PutObject`
count on produce and the matching `GetObject` count on fetch. Transactional,
lake-sink and compacted topics always bypass the buffer.

A buffer flushes on whichever trigger fires first:

| Query param | Default | Meaning |
|---|---|---|
| `coalesce_linger` | `50ms` | Max time a batch waits to be coalesced. Bounds the added produce latency. |
| `coalesce_batches` | `64` | Flush once this many batches have accumulated for the partition. |
| `coalesce_bytes` | `1M` | Flush once the buffered payload reaches this size. |

Durations (`coalesce_linger`) and sizes (`coalesce_bytes`) use the same
`human-units` grammar as `batch_max_delay` / `batch_min_size`. Durations take an
SI-style suffix (`250ms`, `5s`). Sizes take a single-letter binary suffix
`k`/`m`/`g`/`t` — each a power of 1024, so `1m` = 1 MiB and `4m` = 4 MiB (`4M`
is accepted too; the letter is case-insensitive). `coalesce_batches` is a plain
integer count.

### Choosing a linger

The right linger depends on the **per-partition** batch arrival rate, not the
cluster-wide produce rate:

- **Few, very busy partitions** (each receiving many batches per linger window):
  the default `50ms` already merges most batches. Raising it buys little.
- **Many individually-slow partitions** (high topic/partition fan-out, each
  partition receiving well under one batch per linger): a `50ms` linger has
  almost nothing to merge — a partition that receives one batch every few
  hundred ms never has two batches in the same window. Widening the linger to
  `250ms`–`500ms` lets a slow partition accumulate more batches per flush, at
  the cost of up to that much extra produce latency.

> **Structural ceiling.** Per-partition coalescing *cannot* reduce PUTs on a
> partition that is individually below ~1 batch per linger — there is no linger
> value that merges batches which never coexist in the buffer, short of a linger
> so large it violates the latency budget. On a workload of thousands of
> trickle-rate partitions, widening the linger has diminishing returns past the
> per-partition inter-arrival time.
>
> The complementary lever is **fewer, larger batches at the source**: raise the
> Kafka *producer*'s `linger.ms` / `batch.size` (connector-side) so the broker
> receives fewer, fatter batches to persist in the first place. Server-side
> coalescing and client-side batching compose — the broker can only merge what
> the client sends it as distinct batches.

Validate any change by sweeping the param in one deployment and watching S3
`PutRequests` (CloudWatch) against produce p99 latency, rather than adopting a
figure blind.

## Producer idempotency checkpoint

For idempotent/`acks=all` producers, the durable per-producer sequence state in
`producers/{id}.json` is checkpointed lazily (#48): the in-memory sequence
advances on every batch, and the object is written at most once per interval or
per N batches, whichever comes first.

| Query param | Default | Meaning |
|---|---|---|
| `producer_checkpoint_batches` | `64` | Checkpoint after this many idempotent batches. |
| `producer_checkpoint_interval` | `250ms` | Checkpoint at least this often while advancing. |

This is safety-bounded: on an unclean restart the persisted sequence lags the
acked one by at most one window and is reconciled by a max-merge on replay —
never moved backwards, never lost. Widening the interval to `2s`–`5s` is cheap
and reduces checkpoint PUTs, though the gain is small unless you have many
active idempotent producers (the object count scales with *distinct producers*,
not partitions).

## Prefix coalescing — virtual topics (`prefix_coalesce`)

With `prefix_coalesce=true`, the broker coalesces per **connector prefix**
(`org.env.conn` — the first three dotted components of a topic) rather than per
partition (#56). The batches produced across *every* topic under a prefix within
a linger window are flushed into one shared, immutable segment object:

```
clusters/{cluster}/prefixes/{org.env.conn}/segments/{seq:020}.seg
```

This is for **high topic fan-out** CDC (thousands of topics, a handful of events
each per poll), where per-partition coalescing (`produce_coalesce`) still leaves
one PUT per topic. Prefix coalescing collapses PUTs from ~`(topics × flushes)`
to ~`(connectors × flushes)`, and serves fetch from the segment footer + a
ranged GET, retiring the per-fetch `records/` LIST. Off by default; when on it
takes precedence over `produce_coalesce` for eligible batches. Transactional,
control and compacted batches always stay on the legacy per-object path, as do
large **backfill/snapshot** batches (already S3-efficient and parallel, #62).

Segments are keyed by a monotonic sequence, not by offset; each carries a
self-describing footer with every `(topic, partition)` sub-stream's offset
range, byte range and max timestamp. A single writer per prefix is enforced
coordinator-free by an S3 conditional-write lease
(`prefixes/{prefix}/lease.json`), and retention is whole-segment, per-prefix,
under a uniform retention (the longest `retention.ms` among the prefix's topics).

| Query param | Default | Meaning |
|---|---|---|
| `prefix_coalesce` | `false` | Coalesce per connector prefix into shared segments. |

The linger / batch-count / byte thresholds are shared with `produce_coalesce`
(`coalesce_linger` / `coalesce_batches` / `coalesce_bytes`, above).

**GCS:** safe by construction. The segment data objects are create-only
(immutable) and the read path is footer-only (no per-flush-mutated manifest), so
nothing on the produce or fetch hot path mutates a hot object — the ~1/s/object
mutation cap (#13) is never approached. The only mutated control object is the
per-prefix lease, which renews about once per lease term (≫ 1 s), never per
flush.

**Migration / coexistence:** a topic created before the flag keeps its per-topic
`records/` objects; once opted in, new data goes to segments and the old objects
stay readable until retention drains them. A fetch spanning the cutover stitches
the legacy region and the segment region into one continuous offset sequence.
Default off is byte-for-byte the current behaviour.

**Single-broker for now.** Single-writer-per-prefix is enforced by the S3 lease,
but the produce-routing layer that would send a fenced writer's retry to the
owning broker (`consistent_hash(prefix) → broker`) is not yet implemented. Until
it is, enable `prefix_coalesce` only on single-broker deployments (Tansu's
default node model); on multiple brokers a producer landing on a non-owner would
be fenced persistently.

**External S3-direct readers** must understand the segment frame + footer to read
a coalesced prefix; the format is the published contract (see
`docs/virtual-topics-format.md`), tracked for the reference reader in
kotatsu#82.

## Coalescing vs the `batch_*` request batcher

There are two independent write-batching paths; **enable one or the other, not
both**:

- `produce_coalesce` (above) — coalesces **per partition**, inside the
  object-store layer. This is the supported path for **high topic/partition
  fan-out** workloads and the one the thresholds above tune.
- `batch_min_size` / `batch_max_delay` — the `ProduceRequestBatcher`, which
  merges **per producer** before the storage layer (see #53 for the
  same-base-offset ack fix on this path). Better suited to few busy producers.

## Example

```
s3://my-bucket/?produce_coalesce=true&coalesce_linger=300ms&coalesce_batches=128&coalesce_bytes=4m&producer_checkpoint_interval=5s
```

Coalesces produce with a 300 ms linger (or 128 batches / 4 MiB, whichever comes
first) and checkpoints idempotent producers at most every 5 s.
