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

//! Offsets at the ends of a log: the high watermark, the log start, the
//! truncation floor (#176) and the served-end certification (#290) that keeps
//! a reader from waiting on a gap retention already emptied.

use super::*;

/// How far the persisted floor sits above the surviving segment tail, or `None`
/// when that is not the state (#290).
///
/// The whole subtlety is `tail == None`. A sub-stream with no segment at all is a
/// *drained* partition, where the floor is legitimately the only authority and #299
/// already makes it report a log that starts where it ends. The state worth
/// measuring is the **partial** one: records still served below the tail, offsets
/// advertised above it, and nothing in between for a consumer parked there.
///
/// Pure, so the distinction that matters can be asserted without standing up a
/// metrics reader.
fn floor_above_tail(tail: Option<i64>, watermark_floor: i64) -> Option<i64> {
    tail.filter(|tail| watermark_floor > *tail)
        .map(|tail| watermark_floor - tail)
}

impl ServedEnd {
    /// Whether this certification still describes the current floor, i.e. no
    /// uncertified writer has moved `watermark.high` since it was written.
    pub(super) fn certifies(&self, high: i64) -> bool {
        self.at_high == high
    }

    /// Whether `offset` falls in the gap this certification declares dead:
    /// at/above the surviving tail, below the floor the expiry raised.
    pub(super) fn gap_contains(&self, offset: i64) -> bool {
        offset >= self.end && offset < self.at_high
    }
}

impl OptiCon<Watermark> {
    pub(super) fn new(cluster: &str, topition: &Topition) -> Self {
        Self::path(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/watermark.json",
            cluster, topition.topic, topition.partition,
        ))
    }
}

impl DynoStore {
    pub(super) fn watermark(&self, topition: &Topition) -> Result<OptiCon<Watermark>> {
        self.topics
            .watermark(self.identity.cluster.as_str(), topition)
    }

    /// The truncation floor (#176) for `topition` from in-process caches only
    /// — the OptiCon watermark cache first (it is refreshed by the cold/slow
    /// watermark reads), else the [`TopicCaches`] truncation memo — **without
    /// any object-store request**. `None` when neither holds the partition.
    /// Callers on request-free paths treat `None` as "no floor known": for
    /// read-uncommitted `offset_stage_at` that degrades to the pre-truncation
    /// log start (self-correcting, like `cached_low`); for the segment-expiry
    /// loop the direction is mandatory — an unknown floor must defer reclaim,
    /// never force it.
    pub(super) fn cached_truncate(&self, topition: &Topition) -> Result<Option<i64>> {
        let from_watermark = self.topics.watermark_truncate(topition)?;

        if from_watermark.is_some() {
            return Ok(from_watermark);
        }

        self.topics.truncate_floor(topition)
    }

    /// The truncation floor (#176) for `topition`: the offset below which
    /// `DeleteRecords` has logically truncated this sub-stream, `0` when it
    /// never was.
    ///
    /// Zero object-store requests in steady state: served from
    /// [`Self::cached_truncate`] (the OptiCon watermark cache, populated
    /// wherever the cold path already reads `watermark.json`, or the memo).
    /// Only a fully-cold call — neither cache holds the partition — pays one
    /// `watermark.json` GET, and the result **including absence** (memoized
    /// as `0`) is retained, so a watermark-less partition is asked once per
    /// process, never per call — the #161 404-storm shape.
    ///
    /// Measured, after #194 questioned this claim and #203 made it checkable
    /// (0.7.0-beta.26, production fleet): the steady-state cost is **zero
    /// 404s**, and so is the cold cost. `class="watermark"` reads run at
    /// ~1,160/s through the cache and **not one of them** answers `not_found`,
    /// across a window spanning a rolling restart — so neither the "one 404 per
    /// process per partition" warm-up this comment described, nor the
    /// first-touch tail #194 hypothesised, is observable. The floor is cheaper
    /// than #186 claimed, not more expensive.
    ///
    /// #194's elevated `not_found` rate is real and persists (~34/s), but it is
    /// not this: 74% of it is the #112 tail probe (`class="segment"`, correlated
    /// 1:1 with `tansu_prefix_tail_probes`), which speculatively GETs the next
    /// sequence to avoid a listing and predates the floor by two releases. The
    /// attribution to #176 was coincident timing.
    ///
    /// Cross-replica staleness contract: the pod that served the
    /// `DeleteRecords` is exact immediately (`with_mut` leaves the written
    /// value in the OptiCon cache). A peer pod serves the floor it last
    /// observed, refreshed whenever its slow high-watermark path re-reads
    /// `watermark.json`: for a legacy/hybrid sub-stream that is every
    /// stale-hint slow poll (≈ the high-watermark hint TTL), but for a
    /// **pure-segment** sub-stream the watermark is re-read only when the
    /// certified seq-floor generation changes (segment expiry/compaction),
    /// on a cold start, or on this accessor's own first touch — there is
    /// **no TTL bound**, so on a quiet prefix a peer can honour a stale
    /// (lower) floor until restart. Accepted for a rare admin operation; a
    /// stale floor only ever under-hides (a peer serves records another pod
    /// already truncated), it never loses data.
    ///
    /// Release ordering: requires the whole fleet at ≥ 0.7.0-beta.23 — a
    /// pre-#182 pod would erase the floor on its next `watermark.json`
    /// round-trip (see [`Watermark::truncate`]).
    pub(super) async fn truncate_floor(&self, topition: &Topition) -> Result<i64> {
        if let Some(floor) = self.cached_truncate(topition)? {
            return Ok(floor);
        }

        // Fully cold: pay the watermark GET once. `with` serves the
        // `Default` (no floor -> 0) when the object is absent, and that is
        // memoized too, so absence costs one 404 per process per partition,
        // not one per call (#161).
        let floor = self
            .watermark(topition)?
            .with(&self.object_store, |watermark| {
                Ok(watermark.truncate.unwrap_or(0))
            })
            .await?;

        self.memo_truncate_floor(topition, floor)?;

        Ok(floor)
    }

    /// Max-fold `floor` into the [`TopicCaches`] truncation memo (the floor is
    /// monotonic, so a racing older resolution can never regress it).
    pub(super) fn memo_truncate_floor(&self, topition: &Topition, floor: i64) -> Result<()> {
        self.topics.memo_truncate_floor(topition, floor)
    }

    /// The log start offset for `topition` (#161).
    ///
    /// The log start comes from the footer index — the lowest surviving segment
    /// base, which is what `list_offsets` EARLIEST reports.
    ///
    /// It used to come from `watermark.low`, which only the legacy retention paths
    /// ever advanced: authoritatively silent for a segment-backed sub-stream, and
    /// usually absent entirely, yet read-committed `offset_stage` read it on
    /// **every poll** — ~1490 GET/s answered `404 NoSuchKey` at ~1,600
    /// subscriptions (#161), round trips that resolved nothing and are billable on
    /// a store that charges 4xx. The index is also *more* accurate: nothing
    /// advances that field after a segment expiry. Since #179 there is no legacy
    /// region left to own a log start, so this is unconditional.
    ///
    /// Either way the result is clamped to the truncation floor (#176):
    /// `DeleteRecords` hides segment-resident records without touching the
    /// shared segments, so the physical region start can sit below the
    /// logical log start.
    /// When NO segment survives, the log is empty — and an empty log starts
    /// where it ends, so the answer is `high_watermark` (#290).
    ///
    /// It used to be 0, which is a false statement about what the broker holds
    /// the moment the high watermark is above it, and the falsehood is exactly
    /// what made a damaged partition indistinguishable from a healthy one: a
    /// prefix advertising `LOG-START-OFFSET=0 / LOG-END-OFFSET=3024895` with
    /// nothing readable at any offset reported 3M of lag that no consumer could
    /// ever retire. Reporting the log end instead collapses that lag to zero,
    /// which is what makes the gap visible through ordinary metadata rather than
    /// by probing every offset by hand.
    ///
    /// A fully expired log lands in the same place, correctly: retention removes
    /// every segment, so its start becomes its end and it reports empty.
    pub(super) async fn log_start(&self, topition: &Topition, high_watermark: i64) -> Result<i64> {
        let start = self
            .segment_region_start(topition)
            .await?
            .unwrap_or(high_watermark);

        Ok(start.max(self.truncate_floor(topition).await?))
    }

    /// Optimistic-concurrency handle on the per-producer `producers/{id}.json`
    /// object holding `producer_id`'s idempotent sequence state.
    pub(super) fn producer(&self, producer_id: ProducerId) -> Result<OptiCon<ProducerDetail>> {
        self.clients
            .producer(self.identity.cluster.as_str(), producer_id)
    }

    /// Seed (or epoch-bump) the per-producer sequence object so the produce hot
    /// path can validate against it. Called from `init_producer` on the cold
    /// registration path; an absent epoch entry is what distinguishes a
    /// registered producer from `UnknownProducerId`.
    pub(super) async fn seed_producer(&self, response: &ProducerIdResponse) -> Result<()> {
        if response.error != ErrorCode::None || response.id < 0 {
            return Ok(());
        }

        let epoch = response.epoch;

        self.producer(response.id)?
            .with_mut(&self.object_store, |pd| {
                _ = pd.sequences.entry(epoch).or_default();
                Ok(())
            })
            .await
            .map(|_| ())
    }

    /// The cached next offset (== high watermark) hint for `topition`, if known
    /// to this process. `None` means the partition has not been read or written
    /// here yet and the tail must be listed. Ignores listing freshness — used for
    /// offset assignment (produce) and as a listing floor, where any known lower
    /// bound is safe (a `Create` conflict / tail listing reconciles it).
    pub(super) fn cached_high(&self, topition: &Topition) -> Result<Option<i64>> {
        self.topics.high(topition)
    }

    /// The cached next-offset hint for `topition` iff it was last reconciled
    /// against an authoritative tail listing within
    /// [`Self::HIGH_WATERMARK_HINT_TTL`]. `None` (cold, or stale beyond the TTL)
    /// means the high-watermark read path must LIST to pick up batches produced
    /// on another replica. Serving from a fresh hint is what takes the consumer
    /// Fetch hot path off the per-poll `ListObjectsV2` request (#40).
    pub(super) fn cached_high_fresh(&self, topition: &Topition) -> Result<Option<i64>> {
        self.topics
            .high_fresh(topition, self.tuning.watermark_hint_ttl)
    }

    /// Advance the cached next-offset hint for `topition` after a local produce.
    /// Monotonic: a slower task can never lower a value a faster one already
    /// published, so the hint only ever moves forward (offsets are never reused).
    /// Does **not** touch `listed_at`: a local produce reflects only this
    /// replica's writes, so the TTL clock that forces cross-replica reconciliation
    /// keeps running.
    pub(super) fn set_high(&self, topition: &Topition, high: i64) -> Result<()> {
        self.topics.set_high(topition, high)
    }

    /// Advance the hint after an authoritative tail *listing* and mark it fresh.
    /// The listing observed every batch at/after its floor — including other
    /// replicas' writes in that range — so it resets the [`Self::cached_high_fresh`]
    /// TTL clock.
    ///
    /// `as_of` is *when the underlying data was observed*, not `now`: a live
    /// listing passes the instant captured before the LIST; the prefix-coalesce
    /// read path passes the prefix index's `refreshed_at`, because that index is
    /// itself served from a cache up to one TTL old. Stamping `now` instead let
    /// cross-pod visibility staleness compound toward ~2×TTL — the index could be
    /// a TTL stale and then be treated as fresh for another full TTL (#91).
    pub(super) fn mark_listed(
        &self,
        topition: &Topition,
        high: i64,
        as_of: SystemTime,
    ) -> Result<()> {
        self.topics.mark_listed(topition, high, as_of)
    }

    /// The persisted `watermark.high`: a durable lower bound on the tail offset,
    /// used as a listing floor so a cold reader scans only forward (S3
    /// `start-after`) rather than the whole partition.
    ///
    /// This is the *assignment* floor and deliberately ignores the #290
    /// served-end certification: `leaseless_base` folds this value so a freed
    /// offset is never reused, and that must hold whether or not the offsets
    /// above the surviving tail are still fetchable.
    pub(super) async fn persisted_high(&self, topition: &Topition) -> Result<i64> {
        self.watermark(topition)?
            .with(&self.object_store, |watermark| {
                Ok(watermark.high.unwrap_or(0))
            })
            .await
    }

    /// The persisted `watermark.high` together with the served-end
    /// certification (#290), in the single GET the read path already pays.
    pub(super) async fn persisted_watermark_bounds(
        &self,
        topition: &Topition,
    ) -> Result<(i64, Option<ServedEnd>)> {
        self.watermark(topition)?
            .with(&self.object_store, |watermark| {
                Ok((watermark.high.unwrap_or(0), watermark.served))
            })
            .await
    }

    /// The cached `watermark.high` floor (and its #290 served-end
    /// certification, if any) for a prefix-coalesced sub-stream, valid only
    /// when it was read under the still-current certified seq `floor` (see
    /// [`TopicCaches`]). `None` means the caller must pay
    /// the `watermark.json` GET (once — the slow path caches it via
    /// [`Self::cache_coalesced_watermark`]).
    pub(super) fn cached_coalesced_watermark(
        &self,
        topition: &Topition,
        floor: u64,
    ) -> Result<Option<(i64, Option<ServedEnd>)>> {
        self.topics.coalesced_watermark(topition, floor)
    }

    /// Cache `high` and `served` (a just-read `watermark.json`) for `topition`
    /// under the certified seq `floor` that was current *at or before* the
    /// read. Pairing with an older floor is safe: any watermark advance after
    /// the read raises the floor above `floor`, invalidating this entry; an
    /// advance before the read is already contained in `high`.
    pub(super) fn cache_coalesced_watermark(
        &self,
        topition: &Topition,
        high: i64,
        served: Option<ServedEnd>,
        floor: u64,
    ) -> Result<()> {
        self.topics
            .cache_coalesced_watermark(topition, high, served, floor)
    }

    /// The stale-hint high watermark of a prefix-coalesced sub-stream served
    /// from the in-memory segment index alone — no per-partition object-store
    /// request. `None` means the index is not authoritative for this
    /// sub-stream and the caller must fall back to the `watermark.json` GET.
    ///
    /// Correctness (LATEST must equal the true high watermark exactly):
    ///
    /// - **The true high** is `max(tail across live segments, persisted
    ///   `watermark.high`)`: segments
    ///   are the offset-assignment authority, and the only way assigned
    ///   offsets leave them is retention/compaction, where
    ///   `expire_prefix_segments` persists each affected sub-stream's tail
    ///   into `watermark.high` write-ahead of the delete (retention advances
    ///   the log *start*, never lowers the log *end*).
    /// - **The index tail** covers every live segment after
    ///   [`Self::refresh_prefix_index`]: cold builds list the whole prefix,
    ///   and incremental listings can miss no live segment at/below the known
    ///   max (sequences are assigned by create-CAS at `folded max + 1`, so a
    ///   sequence below an observed one can never be created later). Ghost
    ///   entries (a peer's deletion never re-observed) only ever *equal* the
    ///   persisted watermark floor, never exceed the true high.
    /// - **The watermark floor** comes from the per-partition cache certified
    ///   by [`Self::certified_seq_floor`]: `watermark.high` of a coalesced
    ///   sub-stream only advances in an operation that then raises the seq
    ///   floor, so an unchanged certified floor proves the cached value is
    ///   current; any rise invalidates the cache and the slow path re-reads.
    /// - **No segments at all** is served the same way (#167): a sub-stream
    ///   that never produced, or was fully drained, has `watermark.json` as its
    ///   only authority — and that field is advanced by exactly one operation,
    ///   [`Self::expire_prefix_segments`], which raises the seq floor
    ///   immediately after. The certification argument above therefore does not
    ///   depend on the sub-stream owning a segment: an unchanged floor proves
    ///   the cached value current whether the tail is a segment or nothing at
    ///   all. A cold or floor-invalidated cache still declines to the slow path
    ///   below, so the first read still pays the GET.
    ///
    ///   This case was excluded for lake-sink topics, whose high advanced
    ///   without raising the floor. That engine went with the lakehouse (#96) —
    ///   nothing in this fork writes `watermark.high` outside segment expiry —
    ///   so the exclusion was charging one conditional GET per poll per drained
    ///   partition (~660/s, a third of the fleet's 304 plane) to protect an
    ///   authority that cannot move behind the floor.
    pub(super) async fn coalesced_high_from_index(
        &self,
        topition: &Topition,
    ) -> Result<Option<i64>> {
        let (prefix, substream) = self.routed_substream_of(topition).await?;
        self.refresh_prefix_index(&prefix).await?;

        // `None` for a sub-stream holding no segment: the persisted floor below is
        // then the whole answer, which is what the slow path would fold too. Kept
        // as an `Option` because "holds segments at all" is what separates the
        // state #290 is about from a drained partition (#299) — see
        // [`Self::note_floor_above_tail`].
        let tail = self
            .valid_substream_segments(&prefix, &substream, topition.partition())?
            .last()
            .map(FencedSegment::end);

        // Certified after the refresh above, so the floor covers every
        // watermark advance whose segment deletion that listing could have
        // reflected. One GET per prefix per listing generation, amortized
        // across every partition of the prefix — not per partition.
        let floor = self.certified_seq_floor(&prefix).await?;
        let Some((watermark_floor, _served)) = self.cached_coalesced_watermark(topition, floor)?
        else {
            return Ok(None);
        };

        self.note_floor_above_tail(&prefix, topition, tail, watermark_floor);

        // The same fold the slow path performs — `recover_substream_next_offset`
        // is `max(segment tail, persisted floor)` — so this serves the
        // value that path would have computed, without its per-partition GET.
        let high = tail
            .unwrap_or(0)
            .max(watermark_floor)
            .max(self.cached_high(topition)?.unwrap_or(0));

        // Anchor the hint to when the segment set was observed, not `now`
        // (#91), exactly as the slow path does.
        let as_of = self
            .prefix_index_refreshed_at(&prefix)
            .unwrap_or_else(SystemTime::now);
        self.mark_listed(topition, high, as_of)?;

        Ok(Some(high))
    }

    /// The `[end, at_high)` gap certified dead by the last segment expiry, when
    /// one exists for `topition` and still describes the current floor (#290).
    ///
    /// Served purely from in-process caches — the coalesced watermark cache
    /// that the high-watermark read populates — so an empty fetch pays no
    /// extra object request to consult it (deliberately unlike #292's
    /// confirming read, removed in #314). A cold cache answers `None`,
    /// degrading to today's empty response until the read path warms it.
    pub(super) async fn certified_dead_gap(
        &self,
        topition: &Topition,
    ) -> Result<Option<ServedEnd>> {
        let prefix = self.routed_prefix_of(topition).await?;
        let floor = self.certified_seq_floor(&prefix).await?;

        Ok(self
            .cached_coalesced_watermark(topition, floor)?
            .and_then(|(high, served)| {
                served.filter(|served| served.certifies(high) && served.end < served.at_high)
            }))
    }

    /// Record that the persisted floor sits above the surviving segment tail, when
    /// the sub-stream still holds segments (#290).
    ///
    /// `tail` is `None` for a sub-stream with no segment at all, which is *not* this
    /// state: a fully drained partition legitimately has the floor as its only
    /// authority, and #299 already makes it report a log that starts where it ends.
    /// The state worth counting is the partial one — records still served below the
    /// tail, offsets advertised above it, nothing in between.
    ///
    /// Free: both operands are already resolved by the caller. No request, no
    /// listing, no confirming read — deliberately unlike #292's detector, which paid
    /// for all three per empty fetch and was removed in #314 once the condition
    /// measured ~10/min on healthy data.
    ///
    /// Debug rather than warn, and no error code, because the condition does not
    /// imply damage: a peer replica that acked offsets this process never listed
    /// produces the same arithmetic, and there the floor is the correct answer. What
    /// is missing today is not an alarm but a magnitude — which of the candidate
    /// fixes is worth its cost depends on whether this fires once a week or
    /// constantly.
    pub(super) fn note_floor_above_tail(
        &self,
        prefix: &str,
        topition: &Topition,
        tail: Option<i64>,
        watermark_floor: i64,
    ) {
        if let Some(gap) = floor_above_tail(tail, watermark_floor) {
            WATERMARK_ABOVE_SEGMENT_TAIL.add(1, &[KeyValue::new("prefix", prefix.to_string())]);

            debug!(
                ?topition,
                tail, watermark_floor, gap, "advertising offsets no surviving segment holds (#290)"
            );
        }
    }

    /// The log end offset (high watermark) for `topition`.
    ///
    /// The authority is the immutable batch objects. The tail listing is floored
    /// at the best known lower bound — the in-memory hint, or the persisted
    /// `watermark.high` — so a *cold* reader (empty hint after a restart or on
    /// another replica) lists only the batches *after* that floor via S3
    /// `start-after`, instead of scanning the whole partition (the cold-LIST
    /// storm at scale). A read never writes that floor back: `watermark.high` is
    /// advanced only by [`Self::expire_prefix_segments`], write-ahead of the
    /// segment deletes it is about to perform — no read path CASes it (#13), and
    /// that single writer is what lets the floor-certified cache stand in for the
    /// object (see [`Self::coalesced_high_from_index`]).
    pub(super) async fn high_watermark(&self, topition: &Topition) -> Result<i64> {
        // Warm fast path: serve from the in-memory hint without ANY per-poll S3
        // request while it is fresh (reconciled against a listing within the TTL).
        // Every hint refresh (`mark_listed`) already folds in `from_watermark` —
        // see the `mark_listed` call sites below — so a fresh hint needs neither
        // the tail `ListObjectsV2` (#40) nor the `watermark.json` GET (#72). This
        // is what takes the consumer Fetch hot path off ~1 GET per poll per
        // partition. Bounded staleness (== the hint TTL): another replica's
        // just-produced batch is picked up on the next TTL-triggered listing
        // below.
        if let Some(hint) = self.cached_high_fresh(topition)? {
            return Ok(hint);
        }

        // Prefix-coalesced (#60): the tail offset lives in the segment footers,
        // not in a `records/` listing (there is none). The common case — a
        // pure-segment sub-stream whose watermark floor is certified — is
        // served entirely from the in-memory index with ZERO per-partition
        // object requests; that is what takes a wide `endOffsets(assignment)`
        // (the ~1500-partition ListOffsets that still timed out after being
        // parallelized) off the per-partition `watermark.json` conditional
        // GET. GCS-safe: no per-flush-mutated manifest is read.
        if let Some(high) = self.coalesced_high_from_index(topition).await? {
            return Ok(high);
        }

        // Index not authoritative for this sub-stream — a cold or
        // floor-invalidated watermark cache. Pay the `watermark.json` GET
        // and recover footer-only (#58), caching the watermark under the
        // certified floor read *before* it so the fast path serves the
        // next stale-hint resolution.
        let (prefix, substream) = self.routed_substream_of(topition).await?;
        let floor = self.certified_seq_floor(&prefix).await?;
        let (from_watermark, served) = self.persisted_watermark_bounds(topition).await?;

        // Unconditionally cacheable again (#179). The certification argument is
        // "`watermark.high` advances only in `expire_prefix_segments`, which raises
        // the seq floor immediately after", and that is true once more: legacy
        // retention — the second writer #241 had to gate against — went with the
        // layout it maintained, so there is no writer left that moves this value
        // without raising a floor.
        self.cache_coalesced_watermark(topition, from_watermark, served, floor)?;

        // Counted here too, or the measurement would be blind to exactly the reader
        // most likely to meet the state: a cold one, whose watermark cache the fast
        // path declined (#290).
        self.note_floor_above_tail(
            &prefix,
            topition,
            self.valid_substream_segments(&prefix, &substream, topition.partition())?
                .last()
                .map(FencedSegment::end),
            from_watermark,
        );

        let recovered = self
            .recover_substream_next_offset(topition, from_watermark)
            .await?;
        let high = recovered.max(self.cached_high(topition)?.unwrap_or(0));
        // Anchor to when the prefix index was actually listed, not `now`:
        // `recover_substream_next_offset` may have served a TTL-cached index,
        // so stamping `now` would let cross-pod staleness compound to ~2×TTL
        // (#91). Fall back to `now` only if the index has no timestamp (it was
        // just refreshed above, so this is the safe degenerate case).
        let as_of = self
            .prefix_index_refreshed_at(&prefix)
            .unwrap_or_else(SystemTime::now);
        self.mark_listed(topition, high, as_of)?;
        Ok(high)
    }
}

#[cfg(test)]
mod floor_above_tail_tests {
    use super::floor_above_tail;

    /// The state #290 is about: segments survive, and the floor advertises offsets
    /// above their tail. Two records at `[0, 2)` with a floor of 4 leaves offsets 2
    /// and 3 advertised and unreadable.
    #[test]
    fn a_floor_above_a_surviving_tail_is_the_gap() {
        assert_eq!(Some(2), floor_above_tail(Some(2), 4));
    }

    /// **Not** this state, and the distinction the counter exists to preserve: a
    /// sub-stream with no segment at all is a drained partition, whose floor is
    /// legitimately its only authority. #299 already reports it as a log that starts
    /// where it ends, and counting it here would bury the partial case in noise —
    /// drained partitions are common on a fleet with retention.
    #[test]
    fn a_drained_substream_is_not_the_gap() {
        assert_eq!(None, floor_above_tail(None, 3_024_895));
    }

    /// The ordinary case: the segments reach the floor, so nothing is advertised
    /// that cannot be served.
    #[test]
    fn a_tail_that_reaches_the_floor_is_not_the_gap() {
        assert_eq!(None, floor_above_tail(Some(4), 4));
        assert_eq!(None, floor_above_tail(Some(9), 4));
    }
}

#[cfg(test)]
mod served_end_tests {
    use super::ServedEnd;

    /// The honor condition (#290): the pair speaks only for the floor it was
    /// written with. Any other `high` — an older binary's expiry moved it
    /// without re-certifying — silences it.
    #[test]
    fn a_pair_certifies_exactly_its_own_floor() {
        let served = ServedEnd { end: 2, at_high: 4 };
        assert!(served.certifies(4));
        assert!(!served.certifies(9));
        assert!(!served.certifies(2));
    }

    /// The gap is `[end, at_high)`: `end` is the first destroyed offset — a
    /// consumer parked there waits for a record that can never come — and
    /// `at_high` is excluded because it is where the next record will be
    /// assigned (a peer may already be writing it).
    #[test]
    fn the_gap_is_the_surviving_tail_up_to_the_floor() {
        let served = ServedEnd { end: 2, at_high: 4 };
        assert!(!served.gap_contains(1));
        assert!(served.gap_contains(2));
        assert!(served.gap_contains(3));
        assert!(!served.gap_contains(4));
    }

    /// A certification with nothing destroyed above the survivors — the
    /// expiry deleted only lower segments, or none of this sub-stream's —
    /// declares an empty gap and can never error a fetch.
    #[test]
    fn survivors_reaching_the_floor_leave_no_gap() {
        let served = ServedEnd { end: 4, at_high: 4 };
        assert!(!served.gap_contains(3));
        assert!(!served.gap_contains(4));
    }
}
