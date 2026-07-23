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
| `prefix_compact_min_segments` | `256` | Compact a prefix once it holds more than this many live segments (`0` disables). |
| `prefix_compact_target_bytes` | `64m` | Target size of a merged segment. |
| `prefix_compact_keep_hot` | `16` | Newest segments never compacted (leaves the active tail alone). |
| `maintenance_recency` | `9m` | A prefix maintained (compacted/expired) within this window is skipped by other maintainer replicas. Set to ~0.9× your `maintenance_interval`; `0` disables (every maintainer works every prefix). |

The linger / batch-count / byte thresholds are shared with `produce_coalesce`
(`coalesce_linger` / `coalesce_batches` / `coalesce_bytes`, above).

### Scaling maintenance across stateless replicas (`maintenance_recency`)

Compaction and segment retention run on the maintenance loop of every replica.
Without coordination each maintainer would re-list and re-work every prefix each
tick, so N maintainers duplicate the work N× with no throughput gain.

The maintainers coordinate **statelessly and coordinator-free** — no ordinal, no
StatefulSet, no membership list, exactly like the broker (they can be a plain
ReplicaSet). Each tick a maintainer enumerates the full prefix set, shuffles it
with a per-process seed so replicas sweep in independent orders, and for each
prefix:

- if the per-prefix compaction lease shows it was maintained within
  `maintenance_recency`, it is skipped (no `segments/` LIST, no work) — a peer
  just did it;
- otherwise the maintainer claims the lease (recording the time), does the
  retention + compaction for that prefix under that one claim, and moves on.

So N maintainers partition the prefixes by first-arrival: aggregate throughput
scales ~N, each prefix is maintained ~once per interval by ~one replica, and the
discovery LIST is paid ~once per prefix instead of N×. The per-prefix lease
remains the correctness guard, so a race only ever wastes one lease GET (the
loser is fenced) — never double-compacts. Scaling maintainers up or down needs
no config change and nothing to rebalance. Set `maintenance_recency` to ~0.9× of
your `maintenance_interval` (a maintained prefix must stay skipped for just under
one interval, so it is re-maintained exactly once per interval); `0` reverts to
every-maintainer-works-every-prefix.

### Bounding the segment count (linger vs. compaction)

Segments are written one object per flush window per prefix and are never
mutated, so the number of live segments per prefix grows as:

```
S ≈ flush_rate × retention
```

At the default `coalesce_linger` of 50 ms a continuously-active connector writes
up to ~20 segments/s, so `S` reaches ~10^5 in hours and ~10^6 within days over a
7-day retention. Reads no longer pay per-fetch S3 requests for this (the footer
index caches immutable footers and refreshes incrementally), but the index's
memory footprint and per-fetch scan still scale with `S`. Two levers keep it
bounded:

- **`coalesce_linger`** — the cheap, immediate lever. Widening it to `1s`–`2s`
  divides `S` by 20–40. Per-topic pressure no longer applies in prefix mode
  (many topics share one segment), so a wide linger is appropriate here.
- **Compaction** (`prefix_compact_*`, on by default) — merges old segments into
  fewer, larger ones. It runs on the maintenance loop and each tick **drains a
  prefix down to `prefix_compact_min_segments`**, so `S` is bounded to roughly
  that threshold plus one maintenance interval's worth of new segments — keeping
  the footer-index footprint and per-fetch scan flat. Widen the linger too if
  the between-tick growth is large. Compaction merges the epoch-fenced view
  (never fuses a stale/overlapping segment), writes the merged segment
  create-only before deleting the originals (no object mutation — GCS-safe), and
  is coordinated by a **separate compaction lease** (`compaction-lease.json`) so
  it runs on the maintenance workers without holding or fencing the produce
  writer. Set `prefix_compact_min_segments=0` to disable.

  A maintainer also keeps an in-memory, self-healing skip-hint per prefix so it
  does not re-`LIST` `segments/` for a prefix that is provably still below the
  trigger: it records the segment count seen at the last `LIST`, projects it
  forward at a conservative bound on the growth rate, and skips the `LIST` while
  the projection stays under `prefix_compact_min_segments`. Prefixes near the
  trigger or visibly growing are re-`LIST`ed early, and every prefix is
  re-`LIST`ed at least once per staleness window regardless, so an idle→busy
  prefix is still caught. This retires the residual per-prefix discovery-`LIST`
  floor — the bulk of prefixes sit bounded and quiet — with no config knob and
  no correctness impact (skipping only delays discovery; compaction is
  idempotent and the cap is soft). It is the compaction analogue of the
  whole-segment-retention skip that keeps the maintainer off the `segments/`
  `LIST` of a prefix whose oldest segment is still within retention.

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

**Multi-broker.** On the lease path, single-writer-per-prefix is enforced by the
S3 lease, so `prefix_coalesce` alone is single-broker: on multiple brokers a
producer landing on a non-owner is fenced persistently (no produce-routing layer
exists to forward it). The multi-replica answer is the leaseless seq-CAS arbiter
(`prefix_leaseless`, #86): the create-only segment-sequence CAS assigns offsets
directly, so any replica may append to any prefix with no lease and no routing.
See `docs/migration-scos.md` for the quiesce-and-flip cutover.

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
