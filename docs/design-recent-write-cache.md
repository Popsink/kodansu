# Design: LRW recent-write cache (in-memory producer→consumer pipe)

> **North star.** A tailing consumer should never pay an object-store GET for
> bytes this process uploaded moments earlier. The create-only, immutable
> object layout makes an in-memory byte cache coherent *by construction* — no
> invalidation protocol, no consistency relaxation: visibility stays gated on
> the durable create, only the byte transport changes.

Status: phases 1 (the cache) and 2 (wake-on-write + fetch-triggered flush)
implemented. Phase 3 is sketched and deliberately unscoped.

## Problem

The metadata read path is already memoized (offset hints #40, footer index
#60, etag cache, compacted-policy memo #113), so a warm fetch pays **zero**
object requests to *locate* data — but every data read is still a GET: one per
`records/` object (`fetch_legacy_records`), one ranged GET per covering
segment (`fetch_prefix_coalesced`). The dominant CDC workload is a consumer
tailing its topic seconds behind the producer, so in steady state the broker
downloads from S3 exactly the bytes it uploaded from the same process moments
before. That round-trip costs latency, GET request charges, and — because the
consumer path depends on flush visibility — it couples consumer freshness to
the produce flush cadence, which is what stops `coalesce_linger` from being
widened further (the PUT-rate lever, see storage-tuning).

## Why a byte cache is trivially coherent here

Three shipped invariants do all the work:

1. **Objects are immutable and create-only.** A `records/{offset}.batch` or
   `segments/{seq}.seg` object is never mutated; its name is minted by a
   conditional create.
2. **Names are never reused.** Offsets are monotonic per partition; segment
   sequences are protected by the durable `next_seq_floor` (#77), raised
   write-ahead of every delete. The single exception is topic
   delete + re-create, where `records/` offsets restart at 0 — handled by an
   explicit purge on `delete_topic`.
3. **Visibility is decided elsewhere.** What a consumer may read is gated by
   the high watermark / offset-stage machinery and the footer index, all of
   which sit *above* the byte transport. The cache substitutes where bytes
   come from, never whether an offset is readable.

Hence a cache hit can never be stale, and a miss always falls through to the
object store — per-partition, per-topic, and per-replica safe with no
cross-pod protocol (a replica simply misses on data it did not write and pays
today's GET).

## Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | **Evict least-recently-written (FIFO), not LRU.** | The workload is read-once tail consumption: recency of *write* predicts the next read. No touch-on-read bookkeeping, and a replay consumer scanning old offsets cannot pollute the hot tail (its reads never reorder the queue). |
| D2 | **Populate on write only** (produce flush / batch create), not read-through. | The bytes are in hand at PUT time — population is one `Bytes` refcount, zero extra requests. Read-through would let replay scans and cross-pod backfills evict the tail; it can be added later behind its own budget if cross-pod hit rate matters. |
| D3 | **Compaction does not populate.** | A merged segment is cold data by definition; inserting it evicts the hot tail for bytes nobody is tailing. |
| D4 | **Entries above 8 MiB are not cached.** | A backfill/snapshot object is replay traffic, not tail traffic — one insert would evict ~everything else. |
| D5 | **Size-bounded (`recent_cache_bytes`), off by default (0).** | First size-bounded cache in `DynoStore`; default-off keeps the shipped memory profile byte-for-byte. On is one URL param. |
| D6 | **Whole objects, keyed by object path.** | The key *is* the coherence argument (create-only names). Segment sub-streams are served by slicing the cached object with the footer's `byte_start..byte_len` — zero-copy, same routing as the ranged GET. |

## Phase 1: the cache

`RecentWriteCache` (`tansu-storage/src/dynostore/recent.rs`), one per process,
shared across connections like every other `DynoStore` map
(`Arc<Mutex<…>>`; clones share it).

**Populate** (verbatim object bytes, after the create succeeds — never
before durability):

- `assign_and_create` — legacy `records/` objects: single-batch produce and
  the #50 coalesced flush;
- `assign_and_create_segment` — lease-path prefix segments (#57), gated off
  for compaction (D3);
- the leaseless SCOS flush (#86) — on a won sequence CAS and on an
  ambiguous-PUT nonce adoption (#89), where our payload is provably the
  object's bytes.

**Consult**:

- `fetch_legacy_records`: by object path, before the per-object GET (the
  LIST stays — the listing is the offset index on this path);
- `fetch_prefix_coalesced`: by segment path, slicing the footer entry's byte
  range instead of the ranged GET.

**Account and evict**: exact byte accounting (full `Bytes` length), FIFO
write-order eviction while over budget. A purge on `delete_topic` covers the
one name-reuse case. Metrics: `tansu_recent_write_cache_{hits,misses,bytes}`
and `evicted_unread` — a nonzero unread-eviction rate means the budget is
undersized for the consumer lag window.

**Sizing.** For ~100% tail hits the budget must cover the lag window:
`budget ≈ ingest_rate × max_tolerated_consumer_lag` (e.g. 10 MB/s and
consumers ≤ 30 s behind → ~300 MiB). A consumer lagging past the window falls
through to S3 — graceful degradation. Per process: add the budget to the pod
memory limit.

## Phase 2: pipe, not poll — and lazier PUTs

- **Wake-on-write.** `Storage::await_produce(watch, max_wait)` replaces the
  fetch long-poll's blind `sleep(remaining/2)`: a per-topition
  `tokio::sync::Notify`, signalled by `set_high` whenever a topition's
  high-watermark hint advances, wakes the parked fetch the moment new records
  are readable. The missed-wakeup race is closed by ordering: `set_high`
  advances the hint *before* notifying, and the waiter enables its `Notified`
  *before* checking the hint. Read-committed fetches watch the response high
  watermark rather than the drained offset, so an open transaction's LSO lag
  cannot spin the loop. Engines without a signal keep the polling sleep via
  the trait default. Produce-flush → notify → fetch served from the cache:
  the in-memory pipe.
- **Fetch-triggered flush.** A parked long-poll arms a flush trigger on any
  non-empty coalesce buffer it is watching, firing at `fetch_flush_floor`
  (default 50ms — the old default linger) from the buffer's oldest batch.
  Then `coalesce_linger` can widen to seconds (segment count and PUT rate
  scale down linearly, see storage-tuning) while end-to-end latency stays
  demand-bounded: an active consumer forces flushes at the floor; no
  consumer, no hurry. The floor is also the PUT-cadence guard — at most one
  trigger per buffer, never before the floor, and a drained buffer starts a
  fresh age — so an aggressively polling consumer cannot degrade coalescing
  below one flush per floor per partition/prefix. Producer-ack latency
  remains bounded by whichever fires first (floor under consumption, linger
  otherwise), and acks stay gated on durability.

## Phase 3 (unscoped): speculative tail reads — here be dragons

Serving *pre-flush* buffer contents would remove the last flush-latency
window, but a buffered batch has no offset until the create CAS wins: under
SCOS a conflict re-derives bases, so any pre-flush offset shown to a consumer
is provisional. The failure mode is not staleness but **offset reuse after a
crash or lost CAS** — a consumer commits offset N, the data is never durable,
and N is later minted for different records: silent corruption.

If ever built (flag-gated), the hardening is a write-ahead **offset
reservation floor** (the `next_seq_floor` pattern applied to offsets,
amortized to one small CAS per many windows) so a lost window becomes an
offset **gap** — which Kafka consumers tolerate (compaction creates gaps) —
never reuse. Producer acks are never relaxed. Under leaseless multi-writer
the reservation would have to join the fold protocol; realistically this is
single-writer-only, and phase 2's fetch-triggered flush already collapses the
window under consumer demand. Recommendation: don't build it.

## Non-goals

- Cross-pod cache coherence or sharing — there is none, by design; a remote
  replica's consumer falls through to S3 (today's behaviour, bounded by the
  same hint/footer TTLs).
- Caching metadata objects (`meta.json`, watermarks, footers) — already
  covered by the etag cache, offset hints and the footer index.
- Changing produce-ack or high-watermark semantics in any way.

## References

- `docs/storage-tuning.md` — `recent_cache_bytes` and the coalescing levers.
- `docs/design-multiwriter-segments.md` — SCOS; why offsets exist only after
  the create CAS (the phase 3 constraint).
- `docs/virtual-topics-format.md` — segment layout the byte-range slicing
  relies on.
- #50 / #57 — coalesced write buffers (the population points). #60 — footer
  index (the routing the cache slots under). #77 — seq floor (the name-reuse
  guarantee).
