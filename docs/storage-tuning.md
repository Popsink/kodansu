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
