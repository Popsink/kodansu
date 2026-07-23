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

## Recent-write cache (`recent_cache_bytes`)

With `recent_cache_bytes` set, the broker keeps the bytes of recently written
`records/` objects and prefix segments in memory and serves fetches from that
cache instead of re-downloading from the object store — a tailing consumer no
longer pays a GET for data the same process uploaded moments earlier. The
cache is coherent by construction (objects are immutable and create-only, so
a hit can never be stale) and a miss always falls through to the store, so
multi-replica behaviour is unchanged: a consumer landing on a replica that did
not write the data simply pays today's GET. See
`docs/design-recent-write-cache.md`.

| Query param | Default | Meaning |
|---|---|---|
| `recent_cache_bytes` | `0` (disabled) | Total bytes of recently produced objects held in memory; evicted oldest-written-first. |
| `fetch_flush_floor` | `50ms` | How soon a fetch parked at the tail may flush a pending coalesce buffer, from the buffer's oldest batch. |

The consumer side is wake-on-write: a fetch long-poll parks on a per-partition
produce signal instead of polling, and while parked it arms a flush of any
pending coalesce buffer at `fetch_flush_floor` instead of the full
`coalesce_linger`. This makes the linger safe to widen aggressively (seconds):
a partition/prefix with a waiting consumer still flushes every
`fetch_flush_floor`, while one nobody is tailing batches for the whole linger
— PUT cadence follows consumer demand. The floor is also the guard rail: an
aggressively polling consumer cannot push the flush rate above one per floor
per partition/prefix, so at the defaults (floor = old default linger) the
observable flush rate is unchanged.

Sizing: for ~100% tail hits the budget must cover the consumer lag window —
`budget ≈ ingest_rate × max_tolerated_consumer_lag` (e.g. 10 MB/s and
consumers within 30 s of the tail → ~300 MiB). The budget is per broker
process; add it to the pod memory limit. Objects larger than 8 MiB
(backfill/snapshot class) are never cached. Watch
`tansu_recent_write_cache_bytes` / `_hits` / `_misses`, and
`_evicted_unread`: a sustained unread-eviction rate means the budget is too
small for the lag window (or nothing is tailing).

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

The linger / batch-count / byte thresholds are shared with `produce_coalesce`
(`coalesce_linger` / `coalesce_batches` / `coalesce_bytes`, above).

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

## Benchmarking under realistic S3 latency

Localhost minio's GET/PUT round trip is well under a millisecond — two to
three orders of magnitude faster than same-region S3 (typically ~15-25ms).
At that scale, `recent_cache_bytes` and `coalesce_*` tuning can look like a
no-op in a local A/B: both configurations finish inside the same measurement
tick, because there's no real round-trip cost for either of them to save.
Every threshold and tradeoff described above is priced in request latency,
so a local benchmark that eliminates that latency is measuring the wrong
thing — real S3's dominant cost is network RTT, not disk seek time, and
localhost minio has none of either.

`just s3-latency-up` starts minio behind a
[toxiproxy](https://github.com/Shopify/toxiproxy) proxy with a configurable
injected round-trip latency (default 20ms, split evenly across the proxy's
upstream/downstream legs), so tuning sweeps run against a request cost that
approximates production instead of localhost's near-zero RTT:

```shell
just s3-latency-up            # minio + toxiproxy, 20ms round trip, ready on :19000
just s3-latency-up 40         # ...or any other round trip in ms
just toxiproxy-latency 5      # change the injected latency on an already-running proxy
just broker-s3-latency        # convenience: build + bring up + run a debug broker against it
```

Point a broker or `tansu perf` run at it via `AWS_ENDPOINT=http://localhost:19000`
(not minio's own `:9000` — that bypasses the proxy) and `STORAGE_ENGINE=s3://tansu/?...`.
`tansu perf <topic> consume --consumers=N` (long-polls a partition with `N`
independent fetchers from the same offset) is the fan-out shape
`recent_cache_bytes` targets — N consumer groups tailing one hot partition —
and is the tool to reach for when A/B-ing the cache: produce a burst through
one broker, then consume it with and without `recent_cache_bytes` set,
comparing drain time and per-fetch latency, not just aggregate MB/s (which
saturates within one measurement tick for small bursts even when the
underlying per-fetch cost differs sharply).

`just toxiproxy-down` / `just docker-compose-down` tear it down. The `latency`
compose profile keeps it out of the default `just ci` / `just s3-up` path, so
normal dev and test flows are unaffected.
