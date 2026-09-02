# Storage tuning (S3 / GCS object-store backend)

The object-store backend batches writes to keep the steady-state S3/GCS request
cost down. The batching thresholds are tunable per deployment from the storage
URL query string, so you can adapt them to your workload without recompiling.

Omitting every key below reproduces the shipped defaults.

## Produce coalescing

The broker buffers produced batches within a *linger window* and flushes them as
one object instead of one object per batch. This cuts both the `PutObject` count
on produce and the matching `GetObject` count on fetch.

Coalescing is always **per connector prefix** into a shared segment (see
[Prefix coalescing](#prefix-coalescing--virtual-topics) below) — that is the only
layout the broker writes. It is not a mode you turn on.

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

### The linger is also a per-connection request-rate ceiling

A broker connection is served **one request at a time**: the loop reads a frame,
answers it, and only then reads the next. (Apache Kafka does the same — it mutes
the channel while a request is in flight.) So a connection's request rate is
exactly `1 / per-request latency`, and for produce that latency is
`linger + PUT`.

At the default `50ms` linger that is **~19 produce requests per second, per
connection** — measured over `memory://`, where the PUT costs microseconds, so
the figure is the linger and nothing else:

| | requests/s | per request |
|---|---|---|
| one connection | 19.1 | 52.3 ms |
| eight connections | 148.9 | — |

The second row is the important one: throughput scales with connections, so this
is **not** a broker-wide limit and **not** the object store. It is per
connection, and the cost is the linger.

The trap is that a single client cannot fill its own linger window. Nothing more
is read from that connection until the request in flight is answered, so the
buffer holds exactly one of its batches and waits out the full window for
batches that cannot arrive. The linger coalesces across *connections*, never
within one.

What this means in practice:

- **A fleet of low-rate, small-batch producers** — CDC, event-at-a-time, low
  `linger.ms` client-side — is bounded at ~`1/linger` requests per second per
  connection whatever the broker is doing. Message throughput comes from
  client-side batching: raise the *producer*'s `linger.ms` / `batch.size` so each
  request carries more.
- **Latency accumulates rather than erroring.** A client pacing faster than
  `1/linger` builds a queue in its own send buffer; delivery latency grows
  without bound and no error is surfaced.
- **`acks=0` makes that queue invisible.** The delivery report fires when the
  request is written to the socket, so a client is told "sent" for records the
  broker has not read yet, and process exit discards them. The broker no longer
  compounds this by answering an `acks=0` produce (#440) — that response
  desynchronised the correlation-id stream and made clients drop the connection
  outright — but the queue itself is the linger's doing.

Lowering `coalesce_linger` raises the per-connection ceiling proportionally and
raises the segment PUT rate by the same factor. That is the whole trade, and the
cost side is quantified under
[What `coalesce_linger` is worth](#what-coalesce_linger-is-worth-and-where-it-stops).

#### A wide request costs one window, not one per partition

`1 / (linger + PUT)` is the ceiling **per request**, whatever that request is
worth. It used not to be: the partitions of a produce were written one after the
other, each `produce` awaited to completion before the next began, and since a
produce completes only when its coalescing window has flushed, an N-partition
request cost **N × linger** end to end. That is what made the same paced
producer measurably *slower* against eight partitions than against one (#439) —
the reading at the time was "more batches, more requests", and the truth was one
request paying eight flush windows.

The partitions of a request are independent logs and nothing in the protocol
orders them against each other, so they are now written at once, bounded like
every other fan-out in the engine. A request naming 8 partitions costs one
window; one naming 32 costs one window.

Two entries naming the *same* `(topic, partition)` in one request still run in
the order the client wrote them, as the batches within one entry always have. No
Kafka client sends that shape and the protocol does not say what it means, but a
hand-rolled one that does gets ordering rather than a race.

This does not move the per-connection ceiling — that is still one request at a
time per connection, and still `1 / (linger + PUT)`. It removes the penalty for
*width*, which is the dimension a CDC workload grows in.

## `segment_format` — the footer version this deployment writes

```
s3://my-bucket/?segment_format=4
```

| Value | |
|---|---|
| `3` (default) | Sub-streams are keyed by `(topic, partition)` **name**. |
| `4` | Sub-streams of topics created from now on are keyed by `(topic_id, partition)`. |

Raising it is what makes **a topic deleted and recreated under the same name
start at offset 0** instead of continuing its predecessor's offsets (#442).
Everything else about the segment is unchanged.

The mechanism is why it is a flag rather than a default. A segment is immutable
and shared, and is reclaimed whole only once every sub-stream in it is past
retention — so a deleted topic's slices cannot be removed. Keyed by name, a
same-named successor found them; what kept it from *serving* them was the
truncation floor `DeleteTopics` leaves behind, which is also why its watermarks
read `(10, 13)` where Apache Kafka answers `(0, 3)`. Keyed by id, a recreation is
a different uuid, so it is a different sub-stream: the old slices are unreachable
by construction and the new log starts where an empty log starts.

**The identity is pinned per topic at creation and never changes.** A topic that
existed before the flip stays name-keyed for its whole life, records and all —
its segments have no id to match, so nothing about it moves. Only topics created
after the flip are id-keyed. That is what makes the flip safe to do on a live
bucket, and it means the fix applies to a topic from its *next* recreation
onwards, not retroactively.

### Rolling it out

1. Deploy the binary everywhere. Every reader in this release accepts v4
   segments; nothing writes one.
2. Confirm every **external** S3-direct reader (kotatsu and anything else built
   on `docs/virtual-topics-format.md`) is on a build that accepts v4 *and*
   resolves a sub-stream by `substream_id`. A reader that keys by name against an
   id-keyed bucket does not fail — it silently answers with a *predecessor's*
   records for any recreated name.
3. Set `segment_format=4`.

Step 2 is the one that cannot be skipped. A reader that rejects v4 fails cleanly,
but it fails on the **whole prefix**, not on the topic that caused it — the
version follows the writer, so the first flush after the flip makes every new
segment v4 everywhere.

**Flipping back is not a rollback.** A topic created under v4 has its
`substream_id` pinned for its lifetime, and a v3 writer cannot express that
identity — it refuses the produce (`KAFKA_STORAGE_ERROR`, retriable) rather than
writing records under a key nothing reads. Those topics stop being writable until
the flag goes back on. A binary from *before* this release does not model
`substream_id` at all and would take the quiet path instead, which is why step 1
precedes step 3.

## `message_max_bytes` — the largest batch this broker accepts

Kafka's `message.max.bytes`, defaulting to Kafka's own value (`1048588` — 1 MiB
plus the record-batch overhead it allows on top). A batch above it is refused
with `MESSAGE_TOO_LARGE` before anything is buffered.

```
s3://my-bucket/?message_max_bytes=8m
```

Matching Kafka's default exactly is the point: an application that fits here
fits on a stock Kafka, and one that does not finds out now rather than on the
day it is migrated or replicated onto one. Client-side `max.request.size` is
also 1 MiB by default, so nothing reaches this limit without having been
configured past it on purpose.

**If your producers were configured past it, raise this before upgrading.** A
deployment that has been sending batches larger than 1 MiB has been relying on
the absence of the cap; it will start seeing `MESSAGE_TOO_LARGE` otherwise.
`tansu_api_requests` by `api_key=0` against your producers' error rate is the
signal, and the fix is one query-string key.

An unparseable value keeps the default and logs a warning: a size limit that
silently became something else is worse than one that was ignored, because the
operator believes a number that is not in force.

## Producer idempotency checkpoint — removed

There is nothing to tune here any more. The lazily-checkpointed per-producer
sequence object `producers/{id}.json` (#48) went with the legacy write path
(#178): idempotent dedup is derived from the segment flush's folded
`ProducerTable` (#88), which is written with the segment and has no debounce.

`producer_checkpoint_batches` and `producer_checkpoint_interval` were parsed but
ignored from #178 until #227, and are now in the deprecated-key list: a storage
URL still carrying them starts and logs one warning, rather than failing.

## Prefix coalescing — virtual topics

The broker coalesces per **connector prefix** (`org.env.conn` — by default the
first three dotted components of a topic; see
[the prefix shape](#prefix_depth--prefix_separator--the-shape-of-the-prefix))
rather than per partition (#56). The batches produced across *every* topic under
a prefix within a linger window are flushed into one shared, immutable segment
object:

```
clusters/{cluster}/prefixes/{org.env.conn}/segments/{seq:020}.seg
```

This is for **high topic fan-out** CDC (thousands of topics, a handful of events
each per poll), where coalescing per partition still leaves one PUT per topic.
Prefix coalescing collapses PUTs from ~`(topics × flushes)` to
~`(connectors × flushes)`, and serves fetch from the segment footer + a ranged
GET, retiring the per-fetch `records/` LIST.

Transactional and control batches share segments like everything else (#174),
and so do large **backfill/snapshot** batches: a bulk batch trips a widened byte
threshold and flushes as roughly its own segment (#90), keeping the 1-PUT parity
the old bypass gave. Compacted topics (`cleanup.policy` containing `compact`)
are no exception: they write into segments too, but are routed to a prefix equal
to **their own topic name** rather than to the shared connector prefix, so per-key
compaction only ever touches that one topic's keys (#175). Each topic's routing
is pinned create-only on first use and then served from memory (#236). The
`compacted_segments` and `compacted_carryover` flags that once gated all of this
are gone (#222) — a storage URL still carrying them starts and logs one warning.

Segments are keyed by a monotonic sequence, not by offset; each carries a
self-describing footer with every `(topic, partition)` sub-stream's offset
range, byte range and max timestamp. Any replica may append to any prefix: the
create-only segment-sequence CAS is the offset arbiter (#86), so there is no
lease and no cross-broker produce routing. Retention is whole-segment,
per-prefix, under a uniform retention (the longest `retention.ms` among the
prefix's topics).

| Query param | Default | Meaning |
|---|---|---|
| `prefix_depth` | `3` | Leading components of the topic name that form its prefix. `0` gives every topic its own prefix (no cross-topic coalescing). Sealed per cluster — see below. |
| `prefix_separator` | `.` | What those components are separated by. An empty value is ignored with a warning. Sealed per cluster — see below. |
| `prefix_compact_min_segments` | `256` | Compact a prefix once it holds more than this many live segments (`0` disables). |
| `prefix_compact_target_bytes` | `16m` | Target size of a merged segment. Kept modest because the merged create currently shares the producer tail create-CAS namespace (#130); a larger target lengthens each merged PUT, loses the create race more often, and re-uploads its whole payload on retry, amplifying S3 write cost. The live segment count is bounded by `prefix_compact_min_segments` (a count trigger), not by this size. |
| `prefix_compact_keep_hot` | `16` | Newest segments never compacted (leaves the active tail alone). |
| `maintenance_interval` | `10m` | How often this replica runs retention + compaction. `never` disables it entirely — the setting for a serving broker fleet that leaves storage maintenance to a dedicated maintainer. Do not spell that as a very large duration: a period still schedules a pass on every replica at once when it comes round. Ticks are wall-clock aligned, so replicas share a schedule whatever time their pods started. Evicting idle coordinator group state is *not* on this clock and keeps running under `never`. |
| `maintenance_recency` | `9m` | A prefix maintained (compacted/expired) within this window is skipped by other maintainer replicas. Set to ~0.9× your `maintenance_interval`; `0` disables (every maintainer works every prefix). |
| `flush_max_elapsed` | `10s` | Wall-clock budget for a flush's create-CAS conflict-correction loop. When it runs out the flush yields to the competing writer and the produce is rejected *retriably*. It is a floor on attempts, not a hard deadline: the loop always makes at least 3 real attempts, and will not start an attempt it expects to overshoot. Raise it if you see `leaseless flush exhausted retries` with a small `attempts` and a large `put_ms` — that is a slow bucket, not contention. |
| `watermark_hint_ttl` | `5s` | Freshness window of the in-memory high-watermark view (#500): the per-partition hint and the prefix index answer `ListOffsets` and the fetch path's end-of-log check without a listing while younger than this. Widen it to match the cadence of periodic watermark readers — a sink fleet whose lag diagnostic calls `endOffsets` every 30s stays entirely on the zero-request path with `watermark_hint_ttl=30s`. The price is cross-replica visibility: a batch produced on a *peer* replica can stay invisible to this replica's `ListOffsets(LATEST)` **and fetch** for up to the window (same-replica produce advances the hint immediately, and a single-replica deployment gives the window away for free). Staleness only ever *under*-reports the end — a consumer re-reads, never skips. Offset assignment does not depend on it for correctness: the create-CAS reconciles against the real tail on conflict. |

The linger / batch-count / byte thresholds are the `coalesce_linger` /
`coalesce_batches` / `coalesce_bytes` keys documented above.

### `prefix_depth` / `prefix_separator` — the shape of the prefix

```
s3://my-bucket/?prefix_depth=2&prefix_separator=_
```

The prefix is the first `prefix_depth` components of the topic name, split on
`prefix_separator`. `a.b.c.d` at the defaults gives `a.b.c`; at
`prefix_depth=2`, `a.b`; at `prefix_depth=4` or `prefix_depth=0`, `a.b.c.d`
(a topic with fewer components than the depth is always its own prefix, and
depth `0` means every topic is). With `prefix_separator=_`, `a_b_c_d` gives
`a_b_c`.

The defaults reproduce the pre-#464 derivation byte for byte, so a storage URL
carrying neither key keeps its exact object layout.

Get this wrong in either direction and the epic stops paying. Too coarse and
unrelated tenants share a segment — and therefore share a *retention*, since
retention is whole-segment and per-prefix under the longest `retention.ms` in
the prefix. Too fine and every topic is its own prefix, which is one PUT per
topic per flush: the bill #56 exists to remove.

**The shape is sealed for the life of a cluster.** The first store built against
a bucket writes `clusters/{cluster}/prefix-shape.json` create-only
(`{"depth":3,"separator":"."}`); every store built after it reads that object and
**fails to start** if its configuration disagrees, naming both shapes. An
existing bucket that has always run on the defaults seals `3` / `.` on its next
start, which is what its data already says. Replicas starting together race for
the create and converge on the winner.

The seal read is the first object-store request a store makes, so a bucket that
is unreachable or misconfigured now fails the build rather than the first
produce.

Failing to start is the point. Since #236 each topic's routing prefix is pinned
at creation, so produce and fetch keep using the prefix a topic's segments are
actually under whatever the URL says — no records are lost or misrouted by a
shape change. Retention and compaction do **not** read that pin; they re-derive
the prefix from the topic name, because resolving each topic's pin would cost a
GET per topic per tick on a cold pod. Change the depth on a live cluster without
the seal and the two halves disagree: the sweeps visit prefixes that hold
nothing, so old segments stop expiring and stop compacting, live segments per
prefix grow without bound, and **nothing is logged** — the empty prefixes they
visit really are clean. A failed rollout is a much better outcome than a silent
one.

Changing the shape on a populated bucket is therefore a migration (rewrite the
objects, or dual-read both layouts), not a config change. There is no support
for it today.

### Scaling maintenance across stateless replicas (`maintenance_recency`)

Compaction and segment retention run on the maintenance loop of every replica
that has one — a replica started with `maintenance_interval=never` has no
maintenance loop at all, which is how a serving broker fleet hands the work to a
dedicated maintainer deployment. Without coordination each maintainer would
re-list and re-work every prefix each tick, so N maintainers duplicate the work
N× with no throughput gain.

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

Setting it *below* the interval quietly disables the skip: the window lapses
before the next tick, so no prefix is ever skipped for recency and the claim is
decided entirely by the lease race. That is not incorrect — the lease is the
correctness guard — but it costs a lease GET per prefix per maintainer per tick
for nothing. `tansu_maintenance_prefixes{outcome="recent"}` at zero against a
non-zero `claimed` is what that looks like.

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
  prefix toward `prefix_compact_min_segments`**, so `S` is bounded to roughly
  that threshold plus one maintenance interval's worth of new segments — keeping
  the footer-index footprint and per-fetch scan flat. Widen the linger too if
  the between-tick growth is large.

  A tick keeps sweeping the prefixes it claimed that are still over the trigger
  until a sweep merges nothing, rather than stopping after one pass and idling
  out the interval, so the interval bounds how often work *starts* and not how
  much a tick does. Watch `tansu_prefix_drain_stops` by `reason`: anything other
  than `drained` means a prefix's backlog outlived the tick, and
  `tansu_maintenance_duration` against your interval says whether that is the
  interval's fault. Compaction merges the epoch-fenced view
  (never fuses a stale/overlapping segment), writes the merged segment
  create-only before deleting the originals (no object mutation — GCS-safe), and
  is coordinated by a **separate compaction lease** (`compaction-lease.json`) so
  it runs on the maintenance workers without holding or fencing the produce
  writer. Set `prefix_compact_min_segments=0` to disable.

**GCS:** safe by construction. The segment data objects are create-only
(immutable) and the read path is footer-only (no per-flush-mutated manifest), so
nothing on the produce or fetch hot path mutates a hot object — the ~1/s/object
mutation cap (#13) is never approached.

**Coexistence with pre-segment data:** a topic that predates segments keeps its
per-topic `records/` objects; new data goes to segments and the old objects stay
readable until retention drains them. A fetch spanning the cutover stitches the
legacy region and the segment region into one continuous offset sequence.

**Multi-broker.** Any replica may append to any prefix: the create-only
segment-sequence CAS assigns offsets directly (#86), so there is no lease, no
fencing epoch and no cross-broker produce routing. A writer that loses the
create race folds the winner's footer, re-derives its sub-stream bases and
retries at the next sequence.

**External S3-direct readers** must understand the segment frame + footer to read
a coalesced prefix; the format is the published contract (see
`docs/virtual-topics-format.md`), tracked for the reference reader in
kotatsu#82.

## The S3 request bill

Everything above is a latency/memory trade with a price attached, and the price
is worth writing down because the intuition is wrong. **This backend's cost is
requests, not bytes.** Measured on a production fleet (10 brokers, 4 maintainers,
eu-west-3) absorbing **0.23 MiB/s** of produce: 441 S3 requests/s, ~$37/day of
requests, against a storage and transfer bill that rounds to nothing at that
volume.

eu-west-3 S3 Standard prices `PUT/COPY/POST/LIST` at $0.0054/1 000 and `GET` at
$0.00043/1 000 — a PUT is **12.6× a GET**. Error responses are billed, so a 404
probe and a 304 revalidation each cost a request. Deletes are free.

| class | rate | $/day | share |
|---|---|---|---|
| `put_opts` (+ errors) | 34 /s | $15.90 | 43 % |
| `get_opts` (+ 404/304) | 388 /s | $14.42 | 39 % |
| `list*` | 15 /s | $6.86 | 18 % |
| `delete_stream` | 4 /s | — | free |

### Where the PUTs actually go

The tempting conclusion from a 43 % PUT share is to reach for `coalesce_linger`.
On that fleet it would have addressed under a third of it. Split by key class:

| PUT class | rate | share of PUTs | $/day |
|---|---|---|---|
| `group` | 22.7 /s | **67 %** | $10.59 |
| `segment` | 10.5 /s | 31 % | $4.90 |
| everything else | 1.4 /s | 4 % | $0.65 |

Consumer-group state is the largest single line item on the whole bill — larger
than segment writes, larger than any read plane — on a fleet serving 32
`Heartbeat`/s and 2.7 `OffsetCommit`/s. That is ~0.7 group PUTs and ~5.3 group
GETs *per heartbeat*, and it scales with group and member count rather than with
data. Check `tansu_objectstore_cache_requests{class="group"}` before tuning the
write path.

### What `coalesce_linger` is worth, and where it stops

Segments are one PUT per flush window **per prefix**, so the segment PUT rate is
`active prefixes / linger` and is independent of how little data arrived. At
`coalesce_linger=1s` that fleet wrote 10.5 segments/s — ~11 continuously-active
prefixes — for 0.23 MiB/s, an average of 7.4 KiB per object.

The linger divides several things at once, and the indirect savings are the
larger ones:

| linger | segment PUT/s | segments/day | direct $/day saved | produce latency added |
|---|---|---|---|---|
| `1s` (that fleet) | 10.5 | 907 k | — | ≤ 1 s |
| `2s` | 5.3 | 454 k | $2.45 | ≤ 2 s |
| `5s` | 2.1 | 181 k | $3.92 | ≤ 5 s |

Halving the segment creation rate also halves what compaction has to merge, the
live segment count `S`, the footer-index memory that scales with it, and the
`segment` GET and 404 planes that scale with it — on that fleet, 179 segment
GETs/s and 47 segment 404s/s.

**The latency budget is the ceiling, and it binds before the cost curve does.**
On that fleet 1 s is the limit of what the workload will accept, so the table
above stops being a choice at its first row: the linger is not a lever there, and
`S` has to be bounded by compaction converging (`prefix_compact_*`, and see the
drain notes above) rather than by writing fewer segments. Read the two together —
a linger you cannot raise means the compaction side is the only side left.

The complementary lever that costs no broker-side latency is **fewer, larger
batches at the source**: the Kafka *producer*'s `linger.ms` / `batch.size`
connector-side, which changes what arrives rather than how long it waits here.

Do not adopt a figure blind. Sweep it in one deployment and watch
`tansu_objectstore_cache_requests{method="put_opts",class="segment"}`,
`tansu_prefix_segment_flushes` and produce p99 together.

### Reading the request metrics without being misled

These series have been misread in cost analyses, so:

- **`tansu_prefix_segments_live` is the bucket; `tansu_prefix_index_entries` is a
  replica's index.** The first is recorded from a whole-prefix listing — the cold
  build and the five-minutely reconciling pass (#408) — so it is the object
  store's own answer for that prefix, and `max by (prefix)` is the freshest
  observation across the fleet. The second is the size of *that process's* cached
  footer index, which is a different quantity and is per replica by nature: the
  incremental refresh is add-only below the tail, so an entry for a segment a peer
  retired survives until a listing drops it (`tansu_prefix_index_reconciled`
  counts the drops), and four maintainers once reported 17 517, 14 374, 13 932 and
  67 for the same prefix at the same instant. Read the gap between the two on one
  prefix as that replica's index staleness (#408), never as a compaction backlog;
  and do not sum either across replicas.
- **A prefix does not converge on `prefix_compact_min_segments`.** That is the
  *trigger*, not a target: compaction merges up to
  `prefix_compact_target_bytes`, so the floor a prefix can reach is its retained
  bytes divided by that target. A prefix holding 15 GiB at a 16 MiB target cannot
  hold fewer than ~970 segments however well compaction runs, and reading its
  1 490 against a trigger of 256 as a 5.8× backlog is a category error. Compare
  `tansu_prefix_segments_live` with `bytes / prefix_compact_target_bytes` for that
  prefix instead.
- **`tansu_objectstore_cache_entries` is a counter, not a size.** It counts etag
  memo *insertions*. `tansu_objectstore_cache_size` is the size.
- **A cache "miss" is not a lost request.** The etag memo answers a
  revalidation locally when the caller presents a matching `If-None-Match`; a
  miss just means the GET goes to the store, which most classes' reads do
  unconditionally anyway. The aggregate hit rate is therefore meaningless —
  read it per class. `meta` runs at ~98 %, which is the plane the memo exists
  for; classes nothing revalidates sit at zero and always will.
- **`get_opts` latency differs by deployment.** On that fleet the maintainers saw
  a 24 ms mean and the brokers 350 ms against the same bucket. Same region, same
  objects; the difference is on the client side.

## Coalescing vs the `batch_*` request batcher

There are two independent write-batching paths:

- Prefix coalescing (above) — always on, inside the object-store layer. This is
  the path for **high topic/partition fan-out** workloads and the one the
  thresholds above tune.
- `batch_min_size` / `batch_max_delay` — the `ProduceRequestBatcher`, which
  merges **per producer** before the storage layer (see #53 for the
  same-base-offset ack fix on this path). Better suited to few busy producers,
  and stacks on top of prefix coalescing rather than replacing it.

## Example

```
s3://my-bucket/?coalesce_linger=300ms&coalesce_batches=128&coalesce_bytes=4m
```

Coalesces produce with a 300 ms linger, or 128 batches, or 4 MiB — whichever
comes first.
