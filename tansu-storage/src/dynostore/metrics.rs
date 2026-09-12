// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
// Copyright ⓒ 2026 Popsink SAS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Every OpenTelemetry instrument this engine records. Gathered here because
//! a metric is read by the dashboard that queries it, not by the code next to
//! it, and because it is the one concern every other module in this directory
//! touches.

use super::*;

/// `segments/` listings issued to refresh a prefix index (#112), by `path`
/// (`forced` = the produce-path fold, `ttl` = a read-path refresh) and `reason`
/// (why the cheaper tail probe could not answer). Tier-1 requests, ~12x the price
/// of the GET the probe replaces them with, so the ratio of this to
/// [`PREFIX_TAIL_PROBES`] is the saving.
pub(super) static PREFIX_INDEX_LISTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_index_lists")
        .with_description("segments/ listings issued to refresh a prefix index")
        .build()
});

/// Prefix-index refreshes answered by the tail probe instead of a listing (#112),
/// by `path` and `outcome` (`up_to_date` = nothing new was there, `extended` = new
/// segments were folded from their probe GETs).
pub(super) static PREFIX_TAIL_PROBES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_tail_probes")
        .with_description("prefix-index refreshes served by a tail probe, not a listing")
        .build()
});

/// Ranged GETs of segment *record* bytes on the fetch path (#117) — the
/// tier-2 requests a consumer-side cache would target. Footer GETs are already
/// served from the in-memory index and are not counted here.
pub(super) static SEGMENT_DATA_GETS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_segment_data_gets")
        .with_description("ranged GETs of segment record bytes served to consumers")
        .build()
});

/// Bytes requested by those GETs (#117), so the request/byte trade of caching
/// whole objects instead of ranges can be costed rather than guessed.
pub(super) static SEGMENT_DATA_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_segment_data_bytes")
        .with_description("bytes requested by ranged GETs of segment record bytes")
        .build()
});

/// Segment-data GETs of an object this pod already read within
/// [`SEGMENT_READ_TRACE_TTL`] (#117), labelled `overlap`:
///
/// - `same_range` — the identical span was fetched again: what the block cache
///   proposed in #117 would have served.
/// - `other_range` — a different span of the same object: co-prefix sub-streams
///   reading disjoint slices, which a range-keyed cache **cannot** serve and only
///   a whole-object cache could.
///
/// The split between these two is the number #117's design hinges on; a low total
/// closes the issue instead.
pub(super) static SEGMENT_DATA_GET_REPEATS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_segment_data_get_repeats")
        .with_description("segment-data GETs of a recently-read object, by range overlap")
        .build()
});

/// Segments written (one create-only PUT per flush window per prefix, #57).
pub(super) static SEGMENT_FLUSHES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_flushes")
        .with_description("prefix-coalesced segment objects written")
        .build()
});

/// Compaction runs refused rather than performed, by reason (#461).
///
/// Expected to be **flat zero**. A non-zero value means compaction met a run it
/// could not merge without losing records and declined. Since #399 that costs
/// one refusal per newly met seam per process, not a permanent stall: the break
/// is memoized ([`PrefixCaches`]) and later runs are selected around
/// it, so the segments on either side still merge. A *sustained* rate here
/// therefore means seams are being newly discovered — run `tansu audit` on the
/// bucket to see whether records are still being lost, or the fleet is merely
/// restarting and re-learning old ones.
pub(super) static SEGMENT_COMPACT_REFUSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_compact_refused")
        .with_description("compaction runs refused to avoid losing records")
        .build()
});

/// Sub-stream entries the fenced view served **clipped** (#461): an entry whose
/// `base_offset` fell inside the range already covered but whose tail reached
/// past it, so only the tail is served.
///
/// Expected to be **flat zero**: the one healthy overlap is a merged segment's
/// originals, and those are wholly inside what the merge covers — dropped, not
/// clipped. A non-zero value means a prefix carries the overlap shape whose
/// *drop* used to destroy records; the clip serves them, and `tansu audit` on
/// the bucket names the prefixes and segments so the writer that produced the
/// overlap can be found.
pub(super) static SEGMENT_OVERLAP_CLIPPED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_substream_overlap_clipped")
        .with_description("sub-stream entries served clipped to the fenced frontier")
        .build()
});

/// Segments merged away by compaction (#66) — bounds live segment count.
pub(super) static SEGMENT_COMPACTIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_compactions")
        .with_description("segments merged away by prefix compaction")
        .build()
});

/// Records removed by the per-key compaction pass over a compacted topic's
/// dedicated prefix (#175) — the signal that key-based cleanup is actually
/// reclaiming superseded values, which `SEGMENT_COMPACTIONS` (a byte-identical
/// merge) cannot show.
pub(super) static SEGMENT_RECORDS_COMPACTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_records_compacted")
        .with_description("records removed by per-key compaction over segments")
        .build()
});

/// Batch frames re-encoded by the per-key compaction pass because they carried
/// dependent-block LZ4 (#253) — the drain progress signal for that repair, and
/// the one that tells when the damage is gone: it stops moving because every
/// affected prefix has been visited, not because the pass stopped running.
///
/// Deliberately separate from `SEGMENT_RECORDS_COMPACTED`: a repair removes no
/// record by construction, so the removal counter cannot show it and a flat
/// removal counter must not be read as "nothing to repair". Expected to reach a
/// bounded total (the frames written while the encoder bug was live) and then
/// stay flat forever; a fresh increment after that means an encoder regression.
pub(super) static SEGMENT_FRAMES_REPAIRED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_frames_repaired")
        .with_description("batch frames re-encoded from dependent-block LZ4 by compaction")
        .build()
});

/// Topics `Metadata` could not resolve but the topics index knows exist (#214).
///
/// Expected to be zero. Nonzero means a per-topic metadata read came back empty
/// for a topic that is there — the failure that previously reached clients as
/// `UNKNOWN_TOPIC_OR_PARTITION` and took source connectors into restart loops,
/// with no broker-side signal at all. This counter is that signal.
pub(super) static METADATA_UNRESOLVED_EXISTING: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_metadata_unresolved_existing_topic")
        .with_description("topics Metadata could not resolve although the index knows them")
        .build()
});

/// Per-topic metadata resolutions, labelled by the API path that asked
/// (`caller`) and where the answer came from (`source`: `index` or `object`)
/// (#387).
///
/// The object-store counters carry a `class="topic_metadata"` label but no
/// caller, so a 1,040/s revalidation plane could be attributed to a call site
/// only by arithmetic against the API mix. This says it directly, and it is also
/// how the fix is checked: `source="object"` is the population that still costs a
/// conditional GET, and after #387 it should be admin-rate — a `Metadata` or
/// `OffsetCommit` steady state that keeps showing `object` means the index is
/// being missed (aged out, or a topic it does not hold).
/// Per-topic metadata resolutions by `caller`, `source` and `index` (#387, #407).
///
/// `source` says whether the topics index answered or the topic's own object had
/// to be read. `index` says *why* — see [`IndexedTopic`] — and that is the pair
/// that matters, because the fleet falls through to the object 18.5 times a
/// second and **every one of those 404s**:
/// `tansu_topic_metadata_reads{source="object"}` and
/// `tansu_object_store_request_error{class="topic_metadata",reason="not_found"}`
/// are equal to three decimals, 1.6 M billed requests/day confirming that a
/// topic a client keeps asking for does not exist.
///
/// `index="fresh_miss"` is the share that could be answered negatively from the
/// index with no new staleness at all. `index="stale"` is the share the fallback
/// exists for (#28/#29). Skipping the fallback is only safe to the extent the
/// first dominates, and until this label there was no way to know.
pub(super) static TOPIC_METADATA_READS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_topic_metadata_reads")
        .with_description("per-topic metadata resolutions by caller, source and index outcome")
        .build()
});

/// Objects a [`TopicIndex`] refresh had to GET (`outcome="fetched"`) against
/// those it reused from the previous snapshot on an unchanged etag
/// (`outcome="reused"`) (#387).
///
/// The reuse is what makes the index cheap enough to serve every `Metadata`
/// answer from: one LIST per window, and a body read only for a topic whose
/// object actually changed. That rests entirely on the listing carrying an etag
/// per object — a store that omits it degrades the refresh to a GET per topic
/// per window, which at 15k topics is far worse than the per-topic reads this
/// replaces. Silent in every other signal, so it is counted here.
pub(super) static TOPIC_INDEX_REFRESH_OBJECTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_topic_index_refresh_objects")
        .with_description("objects a topic index refresh fetched against those it reused")
        .build()
});

/// Segments cached in this process's prefix index, across every prefix (#196).
pub(super) static PREFIX_INDEX_SEGMENTS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_index_segments")
        .with_description("segments held in the process-local prefix index")
        .build()
});

/// Sub-stream entries across every cached footer (#196) — the process-local
/// structure that scales with `segments × topics`, and the one big enough to
/// account for the broker's working-set growth. Paired with
/// `tansu_prefix_index_segments`: the ratio is the mean sub-streams per segment,
/// which is what makes the total interpretable rather than just large.
pub(super) static PREFIX_INDEX_SUBSTREAM_ENTRIES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_index_substream_entries")
        .with_description("sub-stream entries across every footer in the prefix index")
        .build()
});

/// Producer coordinates retained across every cached footer (#543), after
/// [`SubstreamEntry::retain_foldable_producers`] has pruned them.
///
/// The term this issue is about was ~847 MiB per replica and had to be measured
/// by sampling footers out of the bucket, because nothing in the process
/// reported it. Divided by `tansu_prefix_index_substream_entries` it is the
/// coordinates per entry — the number that used to move with the bucket's
/// segment-size distribution and now must not: pruning bounds it at
/// [`IDEMPOTENT_WINDOW`] per distinct producer id per entry, so a rise here is a
/// rise in producers per sub-stream and nothing else.
pub(super) static PREFIX_INDEX_PRODUCER_COORDS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_index_producer_coords")
        .with_description("producer coordinates retained across every footer in the prefix index")
        .build()
});

/// Distinct topic names held by the prefix index (#476 item 2b) — the
/// denominator the interning turned `tansu_prefix_index_substream_entries` into.
///
/// Entries per name is what the change bought: it was 1 by construction when
/// every entry carried its own copy, and it is now the mean number of sub-stream
/// entries sharing one allocation. It is also the only way to see the sweep
/// working — a name count that tracks the cluster's topics is right, one that
/// only ever rises means [`PrefixIndex::forget_unused_topic_names`] is not being
/// reached and the index is accumulating the names of deleted topics.
pub(super) static PREFIX_INDEX_TOPIC_NAMES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_index_topic_names")
        .with_description("distinct interned topic names across the prefix index")
        .build()
});

/// High-watermark resolutions where the persisted floor was **above** the
/// surviving segment tail, on a sub-stream that still holds segments (#290).
///
/// That is the state in which the broker advertises offsets no surviving segment
/// holds. A consumer parked in the gap reads empty on every poll, forever, with no
/// error on either side — `retention_can_orphan_offsets_below_the_advertised_watermark`
/// pins how ordinary retention reaches it.
///
/// **This counter does not say a fault occurred.** The same arithmetic is produced
/// by a peer replica having acked offsets this process never listed — segments
/// created *and* expired inside its blind window — where advertising the floor is
/// correct and lowering it would regress the log end below acknowledged offsets.
/// The two are byte-identical locally *and* in the bucket, since in both cases the
/// segments are gone. So this is a rate to characterise, not an alarm to wire, and
/// choosing between the candidate fixes needs its magnitude first.
///
/// Costs nothing to compute: both operands are already in hand at the fold. It is
/// deliberately not the shape of #292's detector, which paid a forced LIST and a
/// confirming read per empty fetch and measured ~10/min on a healthy fleet before
/// being removed in #314.
///
/// Labelled by prefix — bounded at tens — because "which prefix" is the first
/// question, and per-partition labels would not be bounded on a 14.7k-topic fleet.
/// The gap size goes in the log rather than a label: nine offsets below the tail is
/// a caught-up consumer, millions is a lost log, and that distinction wants a value
/// and not a series.
pub(super) static WATERMARK_ABOVE_SEGMENT_TAIL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_watermark_above_segment_tail")
        .with_description(
            "high watermark resolutions where the persisted floor exceeded the \
             surviving segment tail, by prefix",
        )
        .build()
});

/// Sub-streams whose dead gap was certified by the reconciliation pass rather
/// than by the expiry that created it (#290), by prefix.
///
/// A gap only becomes answerable once something writes `Watermark::served` for
/// it, and only an expiry did — so every gap that predates #343, and every gap
/// on a deployment whose `maintenance_interval` means expiry never runs, stayed
/// silent. This counts the retro-fit: a non-zero value on a prefix is that
/// prefix admitting it was in the #290 state and is now able to say so.
pub(super) static SERVED_END_CERTIFIED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_served_end_certified")
        .with_description(
            "sub-streams whose dead offset gap was certified by the reconciliation \
             pass, by prefix",
        )
        .build()
});

/// Segment objects a **whole-prefix listing** returned, by prefix (#66, #399) —
/// the signal that tells whether compaction is keeping `S` bounded (a counter
/// can't).
///
/// Recorded from the listing itself, which is the object store's own answer for
/// the prefix at that instant, and only when the listing covered the whole
/// prefix: the cold build and the reconciling pass
/// ([`DynoStore::PREFIX_INDEX_RECONCILE_INTERVAL`]) both do, an incremental
/// refresh from a `start_after` cursor sees only the tail and records nothing.
/// No request is issued for it — the listing was already being walked.
///
/// It used to be `PrefixIndex::segments.len()`, this process's cached footer
/// index, which is a different quantity and was wrong in both directions at
/// once: against a full bucket listing on 2026-08-28 the busiest prefix read 484
/// against **826** real objects while the prefix count read 1 256 against **228**,
/// because each replica indexes only the prefixes it has touched and the
/// incremental refresh is add-only below the tail. It is the gauge anyone reaches
/// for to decide whether compaction is healthy, and it filed #399 in one
/// direction and closed it in the other. What that quantity measures is now
/// `tansu_prefix_index_entries`, which says so in its name.
///
/// Every replica that lists a prefix reports it, so read `max by (prefix)` — the
/// freshest observation, not an aggregate. Summing across replicas counts the
/// same objects once per pod, as it always did.
pub(super) static SEGMENTS_LIVE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_segments_live")
        .with_description("segment objects a whole-prefix listing returned, by prefix")
        .build()
});

/// Segments **this replica has indexed** for a prefix after a maintenance tick
/// (#284), against `tansu_prefix_segments_live`'s count of what is really there.
///
/// The per-prefix half of `tansu_prefix_index_segments`, and what
/// `tansu_prefix_segments_live` used to hold. Read it per replica, never
/// aggregated: the incremental refresh is add-only below the tail, so an entry
/// for a segment a *peer* retired stays until a reconciling listing drops it, and
/// four maintainers reported 17 517, 14 374, 13 932 and 67 for the same prefix at
/// the same instant. The gap to `tansu_prefix_segments_live` on the same prefix
/// is this replica's index staleness — the thing #408 is about — rather than a
/// compaction backlog.
pub(super) static PREFIX_INDEX_ENTRIES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_index_entries")
        .with_description("segments this replica has indexed for a prefix, by prefix")
        .build()
});

/// Prefixes a maintenance tick met, by `outcome` (#399): `claimed` when this
/// replica took the work, `recent` when a peer's stamp was inside
/// `maintenance_recency`, `lost` when the lease acquire was fenced.
///
/// #140's remedies all shipped and the busiest prefixes still net-grew — 17 500
/// live segments against a 256 trigger — while the four maintainers ran at 12
/// millicores between them. Nothing said how many prefixes a tick actually
/// claimed, or whether the top-`S` ones were the ones being claimed. This is that
/// measurement, and the pairing that makes it readable: `claimed` against
/// `recent` says whether the recency window is throttling the fleet or doing
/// nothing.
pub(super) static MAINTENANCE_PREFIXES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_maintenance_prefixes")
        .with_description("prefixes a maintenance tick met, by outcome")
        .build()
});

/// Extra backlog sweeps a maintenance tick ran past its first pass (#399) — the
/// scheduling deficit made visible.
///
/// A tick used to be one pass over its claim, then idle until the next interval.
/// Measured in production at `maintenance_interval=10m`: compaction ran in a
/// burst of 1–2 minutes and then nothing for 8–9, a 10–20 % duty cycle, while
/// producers refilled the prefixes for the whole window. Zero here means the
/// first pass drained everything it claimed; a steady non-zero means the interval
/// was never the right place to decide how much work to do.
pub(super) static MAINTENANCE_SWEEPS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_maintenance_sweeps")
        .with_description("extra backlog sweeps past a maintenance tick's first pass")
        .build()
});

/// Compaction runs attempted, by `outcome` (#399): `merged`, `drained`,
/// `retry`, `error`.
///
/// The denominator `tansu_prefix_segment_compactions` never had: with it,
/// segments-merged-per-run is a division rather than a guess, which is what says
/// whether `prefix_compact_target_bytes` is the binding constraint on a run or
/// whether something else ends it first.
///
/// `retry` is the one to watch. A run that could not proceed — a peer deleted the
/// segments it selected — used to return `Ok(0)`, which the drain read as "this
/// prefix is drained" and stopped on, having merged nothing. Run selection picks
/// the *oldest* segments, which are exactly the ones a peer's compaction retires
/// first, and `tansu_prefix_segment_vanished_before_read` runs at 11/s on the
/// fleet.
pub(super) static SEGMENT_COMPACT_RUNS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_compact_runs")
        .with_description("compaction runs attempted, by outcome")
        .build()
});

/// Why a prefix's drain stopped, by `reason` (#399): `drained`, `runs`,
/// `retries`, `error`.
///
/// "Does a drain that starts on a 15 000-segment prefix finish it?" was
/// unanswerable: the drain logged nothing on the way out, so a prefix that
/// converged and a prefix that hit `MAX_RUNS_PER_PREFIX` looked identical from
/// outside. Anything other than `drained` means the backlog outlived the tick for
/// a reason worth naming.
pub(super) static PREFIX_DRAIN_STOPS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_drain_stops")
        .with_description("why a prefix's compaction drain stopped, by reason")
        .build()
});

/// Footer-index entries replaced by the object's own trailer after a short read
/// (#397) — the read path catching the in-memory index serving an entry that
/// does not belong to the object it was used against.
///
/// The trailer is self-describing precisely so a reader never has to derive a
/// region from anything else (#64/#60), and this counts the times that mattered.
/// Nonzero means the index is a cache that can be wrong, which is a different
/// fault from the object being wrong — the discriminator #397 asked for, answered
/// per occurrence rather than once by hand.
pub(super) static SEGMENT_INDEX_ENTRIES_CORRECTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_index_entries_corrected")
        .with_description("footer-index entries replaced by the object's own trailer")
        .build()
});

/// Segments quarantined by compaction because they hold a region no reader can
/// decode (#398) — first sight only, so this counts *distinct* bad objects
/// discovered, not re-reads of them.
///
/// The pairing with [`SEGMENTS_QUARANTINED`] is the point: a counter that moves
/// once and then stops, against a gauge that stays non-zero, is the steady state
/// this replaces — a permanent `ERROR` stream on a condition nothing in the
/// process will ever change. A counter that keeps moving means damage is still
/// being *created*, which is a different (and much worse) fact.
pub(super) static SEGMENT_QUARANTINES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_quarantines")
        .with_description("segments excluded from compaction as undecodable")
        .build()
});

/// Segments currently quarantined for a prefix (#398), by `prefix` — as
/// [`SEGMENTS_LIVE`] is labelled, and for the same reason.
pub(super) static SEGMENTS_QUARANTINED: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_segments_quarantined")
        .with_description("segments excluded from compaction as undecodable, by prefix")
        .build()
});

/// Prefix leases acquired or renewed (#59).
pub(super) static LEASE_ACQUIRES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_lease_acquires")
        .with_description("prefix single-writer lease acquisitions/renewals")
        .build()
});

/// Times this writer was fenced off a prefix lease (#59) — a nonzero rate means
/// contention/failover, so it is worth alerting on.
pub(super) static LEASE_FENCED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_lease_fenced")
        .with_description("prefix lease acquisitions lost to another writer (fenced)")
        .build()
});

/// Segment objects created at an assigned tail sequence, by `role` (#130) — the
/// denominator for the two counters below, so a conflict count reads as a rate
/// rather than an absolute.
pub(super) static SEGMENT_CREATES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_creates")
        .with_description("segment objects created at an assigned tail sequence")
        .build()
});

/// Tail-sequence create-CAS rounds lost to a concurrent writer of the same
/// prefix, by `role` (#130). Compaction claims the merged segment's name from the
/// *same* sequence namespace as live producers, so on a busy prefix it contends
/// with them — and each loss re-lists and re-uploads the whole merged payload.
pub(super) static SEGMENT_CREATE_CONFLICTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_create_conflicts")
        .with_description("segment tail-sequence create-CAS rounds lost to another writer")
        .build()
});

/// Payload bytes re-uploaded because a segment create-CAS was lost, by `role`
/// (#130). This is the write amplification a separate `compacted/` namespace would
/// remove: a merged payload is up to `prefix_compact_target_bytes`, and a losing
/// compactor PUTs all of it again every round. Measured before committing to that
/// split, whose correctness surface is large — the theorized worst case
/// (`MAX_ATTEMPTS × target_bytes` per pass) has never been observed, only derived.
pub(super) static SEGMENT_CREATE_BYTES_REWRITTEN: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_create_bytes_rewritten")
        .with_description("payload bytes re-uploaded after losing a segment create-CAS")
        .build()
});

/// Create-CAS rounds a leaseless flush lost to another writer of the same
/// prefix (#157) — `AlreadyExists`, or an ambiguous PUT resolved to a peer's
/// footer. A rate here is normal multi-writer arbitration; a rate approaching
/// `MAX_ATTEMPTS ×` the flush rate is the contention that exhausts a budget.
pub(super) static FLUSH_CAS_CONFLICTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_flush_cas_conflicts")
        .with_description("leaseless flush segment create-CAS rounds lost to another writer")
        .build()
});

/// Leaseless flushes that gave up their create-CAS budget (attempts or elapsed)
/// and returned a retriable error to the producer (#157). Every increment is a
/// failed produce round-trip, so this is the alerting signal for the
/// contention/wedge class.
///
/// Labelled by `prefix` and by `spent` (#401): the log line already said which
/// prefix and where the budget went, but the counter said neither, so a fleet
/// running 9 of these an hour could not attribute one without grepping ten
/// replicas' logs. `spent` is `elapsed` when the wall clock was actually spent
/// and `projected` when the loop declined to *start* an attempt the slowest
/// observed one said could not finish — on the fleet that projection ends about
/// a third of them, and the two want different fixes.
pub(super) static FLUSH_CAS_EXHAUSTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_flush_cas_exhausted")
        .with_description("leaseless flushes that exhausted their segment create-CAS budget")
        .build()
});

/// Conditional writes that lost the CAS to an object which was then deleted
/// before the winner's value could be read back (#431), by key `class`.
///
/// The condition was logged and not counted, so a fleet running ~17/h of them
/// across five of ten replicas was invisible on every dashboard and could only
/// be found by grepping. Each one used to end a connection with no response
/// written — see [`DynoStore::put`].
///
/// Expected to be small and non-zero: it is a genuine race between a CAS and a
/// delete, and both are things the group plane does continuously. A rate that
/// tracks `class="group_member"` is the session sweep meeting a rejoin; one on
/// `class="group_assignment"` is `delete_group_assignments_before` meeting a
/// `SyncGroup` for the generation it is sweeping.
pub(super) static CONDITIONAL_PUT_VANISHED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_conditional_put_vanished")
        .with_description("conditional writes whose lost-CAS re-read found the object deleted")
        .build()
});

/// Index entries dropped by a reconciling listing because their objects are gone
/// (#408), by `prefix` — segment footers and [`PrefixIndex::opaque`] names alike,
/// both being names this process would otherwise hold for ever.
///
/// The incremental refresh is add-only below the tail, so a replica's index
/// accrues an entry for every segment a *peer* retires. Shedding them one 404 at a
/// time does not converge — a retired segment is at the head, where nothing
/// reads — so this is the population that *only* a listing removes, and its rate
/// is now set by [`DynoStore::PREFIX_INDEX_RECONCILE_INTERVAL`] rather than by
/// how often a fetch trips over a ghost.
///
/// Read it against `tansu_prefix_index_segments`: entries dropped per window
/// should be about the segments the fleet retires per window, and the gauge
/// should track the objects in the bucket instead of climbing with uptime. A
/// prefix whose count keeps climbing while the gauge does too is one whose
/// segments are being retired faster than this window observes them.
pub(super) static PREFIX_INDEX_RECONCILED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_index_reconciled")
        .with_description("index entries dropped by a reconciling listing, by prefix")
        .build()
});

/// Segment objects whole-segment retention deleted, by prefix (#61, #509).
///
/// The retention half of [`SEGMENT_CREATES`]. Until #509 the count existed,
/// was threaded up three call frames, and died in a `debug!` that production's
/// `RUST_LOG=warn` discarded — so whether the object store was bounded could
/// only be inferred from successive bucket listings, and #476's two readings of
/// the same 18 h window projected working sets 10× apart.
///
/// Read it against [`SEGMENT_CREATES`] on the same window: creates minus expiries
/// is the bucket's net segment growth, and a bucket at steady state has them
/// equal. Labelled by prefix for the same reason [`SEGMENTS_LIVE`] is — a
/// fleet-wide sum cannot show that one prefix stopped expiring while the rest
/// carried on.
pub(super) static SEGMENTS_EXPIRED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segments_expired")
        .with_description("segment objects whole-segment retention deleted, by prefix")
        .build()
});

/// Sub-stream region bytes reclaimed by whole-segment retention, by prefix
/// (#509).
///
/// Summed from the `byte_len` of the expired segments' cached footer entries —
/// the same snapshot the expiry decision is made from, so no object read and no
/// new field on `CachedSegment` (which would grow the very index #476 is trying
/// to shrink). It is therefore the **data region** total and excludes each
/// object's footer, so it reads slightly under the bytes a listing shows going
/// away. That gap is the footers, not lost accounting.
pub(super) static EXPIRY_BYTES_RECLAIMED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_expiry_bytes_reclaimed")
        .with_description("sub-stream region bytes reclaimed by retention, by prefix")
        .build()
});

/// Prefix-ticks on which retention deleted nothing, by `reason` (#509).
///
/// Every variant is a *correct* outcome — this is not an error counter. It
/// exists because "retention deleted nothing" and "retention never ran" are
/// indistinguishable from [`SEGMENTS_EXPIRED`] alone, and that ambiguity is what
/// makes a wedged retention path look like a quiet one:
///
/// - `not_due` — the per-prefix oldest-retained hint proved nothing could be past
///   the threshold, so the tick skipped without a LIST or a lease round-trip
///   (#49). The overwhelmingly common case, and the one to watch: a prefix that
///   reports `not_due` forever while its segment count climbs has a hint that is
///   wrong, and [`OLDEST_RETAINED`] says which — recorded on this path too since
///   #544, without which that sentence was an instruction to read a series the
///   path did not emit.
/// - `nothing_expirable` — the prefix was scanned and every segment is either
///   young or blocked by the mid-log fence (#471), so age alone may not remove it.
/// - `no_threshold` — the prefix has no retention threshold at all, so no expiry
///   path runs for it (#544). Compact-only occupants (#175) and anything else
///   `segment_retention_thresholds` leaves out. By design, and the
///   reason this reason exists: without it, a prefix deliberately outside
///   retention and a prefix retention has silently stopped visiting are both
///   simply absent from every series here.
/// - `lease_held_elsewhere` — a peer maintainer holds the compaction lease and is
///   doing this prefix's expiry (#115). Expected on a multi-maintainer fleet;
///   sustained across *every* replica means no one is acquiring it.
///
/// The unit is prefix-ticks, so it is comparable with
/// [`MAINTENANCE_PREFIXES`] and not with a topic or segment count. Against
/// `outcome="claimed"` specifically: every claimed prefix reaches exactly one of
/// these reasons, an expiry, or an error.
pub(super) static EXPIRY_SKIPPED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_expiry_skipped")
        .with_description("prefix-ticks on which retention deleted nothing, by reason")
        .build()
});

/// Greatest record timestamp (ms) of the **oldest surviving** segment under a
/// prefix, after a retention scan (#509).
///
/// This is the number that has to cross `now - retention.ms` for the next expiry
/// to happen, which makes it the plateau signal: **while it rises, the retention
/// window is still filling; once it parks at roughly `retention.ms` behind now,
/// the prefix is at steady state and its contribution to the index has stopped
/// growing.** #476 needed exactly this and had to infer it from two bucket
/// listings a day apart.
///
/// Exported as an absolute epoch timestamp rather than an age on purpose: it is
/// recorded once per maintenance tick (10 m in production), so an age would read
/// up to a full interval stale and an operator could not tell. Compute the age in
/// the query — `time() * 1000 - tansu_prefix_oldest_retained_timestamp_ms` — and
/// it is correct whatever the record and scrape cadences are.
///
/// It is the max footer timestamp of that segment, i.e. its *newest* record, not
/// the oldest record under the prefix: retention is keyed on the newest record in
/// a segment so a shared segment is never dropped while any topic in it is live,
/// and this gauge reports the quantity the decision actually uses. Not recorded
/// when a scan leaves no survivors — there is then nothing retained to describe.
///
/// Recorded on **every** prefix-tick that has a retention threshold, from the
/// hint when the #49 fast path skips the scan and from the survivors when it does
/// not (#544). It was originally recorded only inside the scan, below that gate,
/// and the gate returns early precisely when the oldest survivor is *newer* than
/// the threshold — so the series was absent for exactly the prefixes still
/// filling, which is the inverse of the paragraph above. A prefix with no
/// threshold at all still reports nothing, and that is what
/// [`EXPIRY_SKIPPED`]`{reason="no_threshold"}` counts.
pub(super) static OLDEST_RETAINED: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_oldest_retained_timestamp_ms")
        .with_description("greatest record timestamp of the oldest surviving segment, by prefix")
        .build()
});

/// Topics exempt from time-based expiry because their `cleanup.policy` is
/// `compact` without `delete` (#175, #509).
///
/// By design: the latest value of a key must survive indefinitely, so a
/// compact-only topic's routed prefix gets no retention threshold at all and
/// `expire_prefix_segments` is never called for it. Recorded so that "this part
/// of the bucket can never expire" is a legible policy decision rather than
/// looking like retention failing to fire on it.
///
/// A gauge over topics, not prefixes: the exemption is decided per topic from its
/// config, and computing the routed prefixes of the topics being skipped would
/// add work to the threshold build to report a number nothing acts on.
pub(super) static RETENTION_EXEMPT_TOPICS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_retention_exempt_topics")
        .with_description("topics exempt from time-based expiry by cleanup.policy")
        .build()
});

/// Prefixes whose last topic has been deleted and whose segments are still being
/// reclaimed under a retired-prefix marker (#532).
///
/// The outstanding physical debt of every deleted topic, in prefixes: each one
/// holds segments no client can read and that only retention can remove. It
/// rises with topic deletions and falls back to zero as each prefix drains,
/// because the marker is dropped once the prefix holds no segments — so a
/// count that only ever rises says the reclaim is not running, which is the
/// state #532 found (27 899 segments surviving the deletion of every topic).
///
/// A gauge over prefixes rather than deleted topics: the prefix is the unit
/// retention acts on, and many topics of one connector retire onto the same
/// marker.
pub(super) static RETIRED_PREFIXES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_retired_prefixes")
        .with_description("prefixes whose deleted topics' segments are still being reclaimed")
        .build()
});

/// Member documents deleted because no generation named them (#486).
///
/// Expected to be non-zero while a fleet's abandoned member ids drain and near
/// zero after: it counts objects that could never be read again, so a rate that
/// stays high is a join path minting ids it does not register — which is what
/// produced 46 686 documents for 348 live members.
pub(super) static MEMBER_DOCUMENTS_RECLAIMED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_group_member_documents_reclaimed")
        .with_description("member documents deleted because no generation named them")
        .build()
});

/// Segment objects listed under a prefix whose footer could not be decoded
/// (#157). Expected to be zero: nonzero means something is squatting the
/// create-only segment namespace, and the arbiter is stepping over those names.
pub(super) static SEGMENT_FOOTER_UNDECODABLE: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_footer_undecodable")
        .with_description("listed segment objects carrying no decodable footer")
        .build()
});

/// Sub-stream regions whose bytes are not the batch frames their footer entry
/// claims (#386) — a `byte_start` that does not point at a frame header.
///
/// Counts the **attempt**, before the cause is known. It used to be documented as
/// "expected to be zero; nonzero is a data-integrity failure on the write side",
/// and that reading is what sent #395 and #397 after a write-side bug twice: the
/// fleet's population turned out to be intact objects read through an index entry
/// that belonged to a different segment (#432). Either cause produces this
/// counter, and each occurrence costs a partition a `CORRUPT_MESSAGE` answer that
/// a Kafka client retries at the same offset.
///
/// Read it against [`SEGMENT_INDEX_ENTRIES_CORRECTED`], which is the subset the
/// object's own trailer proved was an index fault
/// ([`DynoStore::resolve_corrupt_region`]). The difference between the two is the
/// write-side population — #395's husks — and only that difference is
/// unrecoverable. Paired with [`SEGMENT_REGION_TRUNCATED`], which is the same
/// symptom reached by an entry that over-states rather than under-states.
pub(super) static SEGMENT_REGION_CORRUPT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_regions_corrupt")
        .with_description("sub-stream regions that do not begin at a batch frame")
        .build()
});

/// Ranged region reads that came back short of the byte extent the footer claims
/// (#386): a damaged, truncated or partially-visible segment object.
///
/// Distinct from [`SEGMENT_REGION_CORRUPT`] on purpose — a short read is not
/// answered as corruption. Whole batches decode, the partial tail is ignored
/// exactly as the frame contract says, and a region yielding nothing that way
/// stays the bounded empty read of #290 rather than becoming an error. Which
/// makes it invisible without this counter, and it is one of the two candidate
/// causes #386 could not separate.
pub(super) static SEGMENT_REGION_TRUNCATED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_regions_truncated")
        .with_description("region reads returning fewer bytes than the footer extent")
        .build()
});

/// Every `segment` 404 this process takes, by the `caller` that took it (#408).
///
/// `class="segment",reason="not_found"` was the number #408 was filed on — 39.2/s
/// on the brokers — and its acceptance asked for an order of magnitude off it.
/// The plane could not be attributed, so that target folded together two
/// populations with opposite meanings:
///
/// - **stale-index 404s.** A segment a *peer* retired that this replica's
///   add-only index still names. The bug, and what #413's reconciling listing
///   removes: `caller="fetch"` / `"refresh"` / `"compaction"`.
/// - **404s that are the answer.** `probe_prefix_tail` proves the tail by reading
///   `cursor + 1` and *expecting* absence; `resolve_segment_create` probes a
///   sequence its own PUT may not have landed at. These are deliberate, they
///   scale with produce and reads rather than with staleness, and no fix reduces
///   them because reducing them would mean not asking.
///
/// So `sum by (caller)` is what says whether the acceptance is met, and
/// `sum(...)` over all callers is what the class counter was measuring. Kept
/// alongside [`SEGMENT_VANISHED_BEFORE_READ`] rather than replacing it: that one
/// means "the index named an object that is gone", which is a narrower claim than
/// this and the one #191/#274 reason about.
pub(super) static SEGMENT_ABSENT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_absent")
        .with_description("segment objects read and found absent, by caller")
        .build()
});

/// Segments this replica's index named that were gone by the time it read them
/// (#191): concurrent compaction (#66) merged them away, or retention (#61)
/// reclaimed them. Counted on the index refresh, the compaction fetch and — since
/// #399 — the read-path region GET, which is where most of it is.
///
/// A normal consequence of maintenance running against live readers, so nonzero is
/// expected. What is *not* normal is the rate the fleet runs at: ~11/s, against
/// 0.55 compaction runs/s that merge anything, because the incremental index
/// refresh is add-only below the tail so a stale entry is never reconciled away —
/// it is only ever discovered here. Read against `tansu_prefix_segment_compact_runs`
/// by outcome: the `retry` share is how much of the drain is spent walking an
/// index through the objects it names that no longer exist.
pub(super) static SEGMENT_VANISHED_BEFORE_READ: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_vanished_before_read")
        .with_description("segments deleted between the discovery listing and the footer read")
        .build()
});

/// Entries in the cluster-global `meta.json` producer table (#283).
///
/// Nothing prunes this table, and every `InitProducerId` appends to it — so every
/// connector restart mints an entry that is kept forever. The cost that matters is
/// not the bytes but the access pattern: `init_producer` round-trips the **whole**
/// object (GET, parse, mutate, CAS-PUT), so registration cost grows with the number
/// of producers the cluster has ever seen, and it degrades exactly when it hurts
/// most — the `InitProducerId` herd of a mass reconnect after an incident.
///
/// Recorded before any expiry policy exists, deliberately: the design decision (and
/// the transaction half of it, which #81's aborted-transaction retention constrains)
/// needs the growth rate and the current magnitude first. A gauge and not a counter
/// because the question is the level, and the level is what a prune would change.
pub(super) static META_PRODUCERS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_meta_producers")
        .with_description("producer entries in the cluster-global meta.json")
        .build()
});

/// Entries in the cluster-global `meta.json` transaction table (#283). Same shape
/// as [`META_PRODUCERS`], with the extra constraint that aborted transactions are
/// retained on purpose (#81), so this half needs a design decision rather than a
/// prune.
pub(super) static META_TRANSACTIONS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_meta_transactions")
        .with_description("transaction entries in the cluster-global meta.json")
        .build()
});

/// Serialised size of the cluster-global `meta.json` (#283) — the payload every
/// `InitProducerId` and every transaction state change moves twice. This is the
/// number the growth math is actually about; the two entry-count gauges above say
/// which table is responsible for it.
pub(super) static META_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_meta_bytes")
        .with_description("serialised size of the cluster-global meta.json")
        .build()
});

/// Listings issued, by `purpose` (#165). Pairs with the per-method request metric
/// [`Metron::instrument_listing`] restores: that one counts requests (pages), this
/// one attributes calls to the code that asked for them. A purpose whose rate
/// tracks the metered LIST rate is the one to optimise.
pub(super) static LIST_SCANS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_object_store_list_scans")
        .with_description("object store listings issued, by call site")
        .build()
});
