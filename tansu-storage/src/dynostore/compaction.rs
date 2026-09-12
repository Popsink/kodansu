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

//! Compaction (#66): the size-merge that bounds the segment count, and the
//! per-key pass that enforces `cleanup.policy=compact` (#175).

use super::*;

impl CompactRun {
    pub(super) fn outcome(&self) -> &'static str {
        match self {
            Self::Merged(_) => "merged",
            Self::Drained => "drained",
            Self::Retry => "retry",
        }
    }
}

impl DynoStore {
    /// Compact a prefix's oldest segments into fewer, larger ones (#66) to bound
    /// the live segment count `S` (otherwise ≈ flush_rate × retention, unbounded)
    /// — keeping the footer index footprint and the per-fetch scan bounded.
    /// Returns the number of segments merged away.
    ///
    /// Coordinator-free and GCS-safe: only the single lease holder compacts, the
    /// merged segment is written as a new create-only object (the merged records
    /// are byte-identical to the originals, #64 contract preserved) carrying the
    /// max input epoch, and only then are the originals deleted — no object is
    /// ever mutated. During the write→delete window the merged and original
    /// segments overlap in offset, but they hold identical records and the read
    /// path's overlap resolver returns exactly one copy, so a concurrent fetch is
    /// correct; a fetch that GETs an original just as it is deleted retries off a
    /// refreshed index (see `fetch_prefix_coalesced`).
    pub(super) async fn compact_prefix_segments(&self, prefix: &str) -> Result<CompactRun> {
        if self.tuning.prefix_compact_min_segments == 0 {
            return Ok(CompactRun::Drained);
        }

        self.refresh_prefix_index(prefix).await?;

        // Snapshot (seq, epoch, last_modified, region bytes) for every cached
        // segment, ascending by seq (== ascending offset for a sub-stream).
        let mut segs: Vec<(u64, i64, i64, usize)> = {
            let index = self.prefixes.index()?;
            index
                .get(prefix)
                .map(|entry| {
                    entry
                        .segments
                        .iter()
                        .map(|(seq, cached)| {
                            let bytes: usize = cached
                                .footer
                                .entries
                                .iter()
                                .map(|e| e.byte_len as usize)
                                .sum();
                            (
                                *seq,
                                cached.footer.writer_epoch,
                                cached.last_modified_ms,
                                bytes,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default()
        };
        segs.sort_by_key(|(seq, ..)| *seq);

        // Segments a previous run proved undecodable (#398), and seams a
        // previous run's tiling check found (#399). Read before the walk below
        // so the locks are not held across it.
        let quarantined = self.quarantined_segments_of(prefix)?;
        let seams = self.compact_seams_of(prefix)?;

        // The offset spans a run would cover, needed only once this prefix is
        // known to have a hole in its tiling — a quarantined segment (#398) or a
        // learned seam (#399) — see [`RunCoverage`]. Skipped entirely otherwise:
        // a prefix with neither is contiguous by construction, and this clones a
        // `Vec<SubstreamEntry>` per segment on a path that walks tens of
        // thousands of them per drain.
        //
        // The coverage guard is what makes the seam memo *sufficient* rather
        // than merely helpful. A seam is a sequence, but the tiling breaks in
        // **offset** order, and the two disagree as soon as compaction runs: a
        // merged segment holds low offsets at a fresh high sequence, so a run
        // can meet the same hole from the other side in an arrangement no
        // per-sequence boundary describes. `RunCoverage` reasons in offsets —
        // a sub-stream's folded span must hold exactly as many records as it
        // spans — so once armed, selection cannot build a straddling run in any
        // sequence order, and the seam's own check below is just the cheap
        // fast path for the boundary already learned.
        //
        // The cost of arming it on a seamed prefix: coverage folds *raw* footer
        // entries, so a batch-aligned overlap the fenced merge could heal reads
        // as over-full and bounds the run instead. On a damaged prefix that
        // trade is taken deliberately — a smaller run beats a wedged one.
        let spans: BTreeMap<u64, Vec<SubstreamEntry>> =
            if quarantined.is_empty() && seams.is_empty() {
                BTreeMap::new()
            } else {
                let index = self.prefixes.index()?;
                index
                    .get(prefix)
                    .map(|entry| {
                        entry
                            .segments
                            .iter()
                            .map(|(seq, cached)| (*seq, cached.footer.entries.clone()))
                            .collect()
                    })
                    .unwrap_or_default()
            };

        // Only above the trigger, and never touch the hot (newest) tail.
        if segs.len() <= self.tuning.prefix_compact_min_segments {
            return Ok(CompactRun::Drained);
        }
        let eligible_end = segs
            .len()
            .saturating_sub(self.tuning.prefix_compact_keep_hot);
        if eligible_end < 2 {
            return Ok(CompactRun::Drained);
        }

        // Pick the OLDEST contiguous run of at least two segments to merge, up to
        // the target size — but skip any *leading* segment already at/above the
        // target (a prior merge, or a large folded-in backfill segment). Such a
        // segment is effectively done: re-merging it just rewrites ~target_bytes
        // to absorb one small neighbour (R=2 write amplification), and — worse —
        // if the next segment overflowed the target the run would collapse to
        // length one, `compact_prefix_segments` would return `Ok(0)`, and
        // `policy_compact_segments` would treat the prefix as drained while small
        // segments pile up behind the big one, so `S` grows unbounded until
        // retention (#114). A segment at/above the target also *bounds* a run
        // (never merged across), leaving it in place as its own segment. The
        // merged epoch is taken from the fenced view below, not from this raw
        // scan, so the segment epoch is ignored here.
        let (run, max_last_modified): (Vec<u64>, i64) = {
            let mut chosen: Vec<u64> = Vec::new();
            let mut chosen_last_modified = i64::MIN;
            let mut start = 0usize;

            while start < eligible_end {
                // A leading (or intervening) already-target-sized segment is a
                // boundary: leave it alone and seed the run after it. So is a
                // quarantined one (#398) — and it has to be a *boundary* rather
                // than a filtered-out element, because the merged segment carries
                // the base offset of its first region and concatenates the rest:
                // merging across the hole would shift every following record's
                // offset down into it, which is corruption where today there is
                // only a stalled drain.
                if segs[start].3 >= self.tuning.prefix_compact_target_bytes
                    || quarantined.contains(&segs[start].0)
                {
                    start += 1;
                    continue;
                }

                let mut bytes = 0usize;
                let mut end = start;
                let mut coverage = RunCoverage::default();
                while end < eligible_end
                    && segs[end].3 < self.tuning.prefix_compact_target_bytes
                    && !quarantined.contains(&segs[end].0)
                    // A seam (#399) ends the run *before* this segment but may
                    // begin the next one with it: unlike a quarantined segment,
                    // the segment itself is healthy — it is the offsets between
                    // it and its predecessor that are gone, so the two sides
                    // merge as their own runs and are never fused across it.
                    && (end == start || !seams.contains(&segs[end].0))
                    && (end == start || bytes + segs[end].3 <= self.tuning.prefix_compact_target_bytes)
                    // A hole in the prefix's offset tiling ends the run here
                    // (#398). Only ever consulted when something is quarantined,
                    // where `spans` is populated; empty means "no hole to cross".
                    && spans
                        .get(&segs[end].0)
                        .is_none_or(|entries| coverage.extend(entries))
                {
                    bytes += segs[end].3;
                    end += 1;
                }

                if end - start >= 2 {
                    chosen = segs[start..end].iter().map(|(seq, ..)| *seq).collect();
                    chosen_last_modified = segs[start..end]
                        .iter()
                        .map(|(_, _, last_modified, _)| *last_modified)
                        .max()
                        .unwrap_or(i64::MIN);
                    break;
                }

                // A lone small segment wedged between large ones: advance past it.
                start = end.max(start + 1);
            }

            (chosen, chosen_last_modified)
        };
        if run.len() < 2 {
            return Ok(CompactRun::Drained);
        }

        // Coordinate compactors with a *separate* compaction lease (#66 review):
        // compaction runs on the maintenance workers, which do not hold the
        // produce lease, so it must not require — or fence — the produce writer.
        // If another compactor holds this prefix, yield.
        if self.acquire_compaction_lease(prefix).await.is_err() {
            return Ok(CompactRun::Drained);
        }

        // Snapshot the run's footers; GET each run segment once.
        let footers: BTreeMap<u64, SegmentFooter> = {
            let index = self.prefixes.index()?;
            let entry = index.get(prefix);
            run.iter()
                .filter_map(|seq| {
                    entry
                        .and_then(|e| e.segments.get(seq))
                        .map(|cached| (*seq, cached.footer.clone()))
                })
                .collect()
        };

        // GET each run segment once; a ghost index entry yields the tick (#274).
        // Run selection picks the oldest segments, which are exactly the ones a
        // peer's compaction may already have deleted from under this replica's
        // add-only index.
        let Some(objects) = self
            .fetch_segment_objects(prefix, run.iter().copied())
            .await?
        else {
            // Not "drained" (#399): the run selected segments a peer had already
            // retired, and `fetch_segment_objects` has pruned them from the
            // index — so the *next* selection is over a different segment set and
            // the drain must take it rather than stopping here.
            return Ok(CompactRun::Retry);
        };

        // Merge the EPOCH-FENCED view (#66 review fix, critical): rebuild each
        // sub-stream from `decode_fenced_regions` (overlap-resolved, higher
        // epoch/sequence wins) restricted to the run — NOT the raw footer
        // entries. A zombie/dominated input is dropped there and an overlap's
        // duplicated head is clipped off (#461), never fused into the merged
        // segment, so compaction can't bake in duplicate/shifted offsets.
        // Identified as well as named (#442): two incarnations of one topic can
        // hold slices in the same run, and merging them as one sub-stream would
        // concatenate two logs' offsets into one region.
        let substream_keys: BTreeSet<(Substream, String, i32)> = footers
            .values()
            .flat_map(|footer| {
                footer
                    .entries
                    .iter()
                    .map(|e| (e.substream(), e.topic.to_string(), e.partition))
            })
            .collect();

        let mut substreams: Vec<SubstreamWrite> = Vec::new();
        let mut merged_epoch = i64::MIN;
        let mut seams: BTreeSet<u64> = BTreeSet::new();

        for (substream, topic, partition) in substream_keys {
            let in_run = self.decode_fenced_regions(prefix, &substream, partition, &objects)?;

            // Every run segment holding this sub-stream is superseded by a
            // segment outside the run — nothing to carry forward.
            let Some((_, base, _)) = in_run.first() else {
                continue;
            };
            let base = *base;

            // The merged region is read back by running offsets from `base`, so
            // the run's regions must tile `[base, ..)` exactly — one batch after
            // another, no gap and no overlap. `decode_fenced_regions` clips a
            // region overlapping the frontier to whole batches past it (#461),
            // and when the clip lands on a batch boundary the tiling holds and
            // the merge *heals* the overlap. It cannot hold when offsets between
            // two regions are simply **gone** (#461's holes — permanent by
            // definition), when the frontier falls inside a batch (batches are
            // opaque: splitting one is a re-encode this pass does not do), or
            // when the segment that set the frontier sits outside the run —
            // fusing any of these would shift every record above the seam onto
            // the wrong offset, and `retire_segments` at the end of this
            // function then deletes the originals they could still have been
            // read from.
            //
            // Refuse the run — but remember where it broke (#399). This used to
            // refuse alone, on the reasoning that a stalled drain is recoverable
            // where a deleted record is not. Both halves of that were right and
            // it still wedged: selection always picks the *oldest* eligible run
            // and a hole never closes, so the same straddling run was rebuilt,
            // fetched and refused every tick, forever — no compaction at all on
            // the damaged prefixes, including above the hole. Each break is a
            // **seam**: selection treats it as a run boundary from now on, so
            // the segments on either side merge as their own runs. Every seam is
            // collected before returning, not just the first, so the prefix
            // converges in one pass rather than one seam per pass.
            let mut expected = base;
            let mut tiles = true;
            for (seq, region_base, region) in &in_run {
                if *region_base != expected {
                    tiles = false;
                    _ = seams.insert(*seq);

                    error!(
                        prefix,
                        topic,
                        partition,
                        seq,
                        expected,
                        region_base,
                        "refusing to compact: the run's fenced regions do not tile \
                         contiguously, and merging them would shift offsets"
                    );

                    // Re-anchor past the break so a second seam in the same
                    // sub-stream is found in this pass too.
                    expected = *region_base;
                }

                expected += region
                    .iter()
                    .map(|batch| batch.last_offset_delta as i64 + 1)
                    .sum::<i64>();
            }

            // The run is already refused; this sub-stream's batches would only
            // be carried into a merge that will not happen.
            if !tiles {
                continue;
            }

            let mut batches = Vec::new();
            for (seq, _, region) in in_run {
                batches.extend(region);
                if let Some(footer) = footers.get(&seq) {
                    merged_epoch = merged_epoch.max(footer.writer_epoch);
                }
            }
            substreams.push(SubstreamWrite {
                topition: Topition::new(topic, partition),
                substream,
                base_offset: base,
                batches,
            });
        }

        if !seams.is_empty() {
            SEGMENT_COMPACT_REFUSED.add(1, &[KeyValue::new("reason", "run_not_contiguous")]);

            // `Retry` when something was learned: the drain loop re-selects at
            // once with the seams honoured, so both sides of each break merge in
            // *this* maintenance pass instead of one side per tick. `Drained`
            // when nothing was — the cap swallowed them, or a peer of this
            // store already knew them — because re-selecting would rebuild the
            // same run and refuse it again: the same spin guard
            // `quarantine_segment`'s `false` provides.
            return if self.memo_compact_seams(prefix, &seams)? {
                Ok(CompactRun::Retry)
            } else {
                Ok(CompactRun::Drained)
            };
        }

        // Write the merged segment (create-only, no produce-lease fencing), index
        // it (preserving the max input append time so retention isn't reset),
        // then delete ALL run segments — including any zombie/dominated ones,
        // whose data was intentionally excluded above.
        let new_seq = if substreams.is_empty() {
            None
        } else {
            // Carry the producer coordinates forward (#107). Re-encoding the
            // merged run as v3 re-derives each batch's producer coordinates —
            // flags included (#174) — from the (byte-identical) merged batches,
            // so log-based idempotent dedup (#88) still observes producers
            // whose batches were compacted — a retry of a compacted batch is
            // recognized as a duplicate and acked with its original offset
            // instead of being re-appended. A fresh per-segment nonce (#89) is
            // stamped as on any create.
            let nonce = rng().random::<u64>();
            let (payload, footer) = self.encode_segment_indexed(
                &substreams,
                merged_epoch.max(0),
                nonce,
                self.tuning.segment_format_version,
            )?;
            let seq = self
                .assign_and_create_segment(prefix, payload, nonce, SegmentCreateRole::Compaction)
                .await?;
            self.index_insert(prefix, seq, footer, max_last_modified)?;
            Some(seq)
        };

        // Retire the run: floor before delete, then prune (#77) — see
        // [`Self::retire_segments`]. Compaction usually adds a higher merged seq
        // so the listing max is unchanged, but when every run segment is
        // superseded no merged seq is written (`new_seq == None`) and deleting
        // the run *can* lower the listing max — freeing a run name for reuse
        // without the floor.
        _ = self.retire_segments(prefix, &run).await?;

        SEGMENT_COMPACTIONS.add(run.len() as u64, &[]);
        debug!(
            prefix,
            ?new_seq,
            merged = run.len(),
            "compacted prefix segments"
        );

        Ok(CompactRun::Merged(run.len() as u64))
    }

    /// Enforce `cleanup.policy=compact` over a compacted topic's dedicated
    /// segment prefix (#175): walking each sub-stream's batches newest first,
    /// drop every record whose key reappears later (and earlier duplicates
    /// within a batch), exactly as the legacy [`Self::compact_partition`] does
    /// over `records/` objects. Returns the number of records removed.
    ///
    /// Deliberately NOT part of [`Self::compact_prefix_segments`]'s run
    /// selection: its `min_segments` (256) / `keep_hot` (16) trigger exists to
    /// bound rewrite amplification on high-flush CDC prefixes and would simply
    /// never fire for a compacted topic holding a handful of segments — the
    /// topic would grow stale versions forever. This pass instead considers
    /// **all** of the prefix's segments every tick, with no size gate and no
    /// hot-tail exemption (the newest segment can hold within-batch
    /// duplicates), and relies on a **dirty-only rewrite guard** for cheap
    /// steady state: a segment is rewritten (create new seq + delete old, under
    /// the compaction lease) only when the transform removed at least one
    /// record — the segment analogue of legacy's `records > 0` in-place guard —
    /// so a clean prefix costs a bounded read walk and zero writes per tick.
    ///
    /// Offsets are load-bearing: a rewritten segment carries the SAME
    /// sub-stream `base_offset`s, and an emptied batch is kept as a header
    /// (records stripped, `last_offset_delta` preserved) rather than dropped,
    /// so the footer's `record_count` — accumulated from `last_offset_delta +
    /// 1` per batch — remains the sub-stream's exact offset span. Both
    /// [`Self::recover_substream_next_offset`] and the overlap resolver's
    /// `covered_to = base + record_count` depend on that: a shrunken span would
    /// admit a not-yet-deleted original alongside its rewrite (duplicates) and
    /// would regress the recovered tail (offset reuse). Kept batch headers also
    /// keep their `max_timestamp` (so `compact,delete` expiry stays
    /// conservative) and their producer coordinates (#88 dedup still observes
    /// producers whose batches were fully compacted).
    ///
    /// The one exception is a region the fenced view serves clipped (#461):
    /// its rewrite starts at the clip — `decode_fenced_regions` sheds the head
    /// batches a higher-priority segment already serves — so it covers the
    /// served tail rather than the original span. That is still resolver-safe
    /// on both sides of the write→delete window: while the original survives,
    /// it precedes the rewrite in base order and the rewrite is wholly inside
    /// what it and its frontier-setter cover (dropped); once the original is
    /// retired, the rewrite serves exactly the tail the original was serving.
    ///
    /// Memory: the prefix's segments are held decoded for the walk — bounded by
    /// the size merge's `prefix_compact_target_bytes` fold of the same prefix,
    /// and by O(distinct keys per partition) for `seen`, capped by
    /// `prefix_compact_seen_keys` (over the cap the partition is skipped for
    /// the tick — removal deferred, never corrupted).
    pub(super) async fn compact_prefix_per_key(&self, prefix: &str) -> Result<u64> {
        self.refresh_prefix_index(prefix).await?;

        // Segments a compaction pass has proved undecodable (#398). Excluded
        // outright here rather than treated as a run boundary: this pass rewrites
        // each segment in place under its own `base_offset`, so dropping one from
        // the walk shifts nothing — where the size merge would fuse across the
        // hole. Without the exclusion one bad object costs the prefix its per-key
        // pass on every tick, the same permanent stall the size merge had.
        let quarantined = self.quarantined_segments_of(prefix)?;

        // Snapshot `(writer_epoch, last_modified_ms)` per segment and the
        // sub-stream key set from the cached footers — no object requests.
        let mut segments_meta: BTreeMap<u64, (i64, i64)> = BTreeMap::new();
        let mut substream_keys: BTreeSet<(Substream, String, i32)> = BTreeSet::new();
        {
            let index = self.prefixes.index()?;
            if let Some(entry) = index.get(prefix) {
                for (seq, cached) in &entry.segments {
                    if quarantined.contains(seq) {
                        continue;
                    }

                    _ = segments_meta
                        .insert(*seq, (cached.footer.writer_epoch, cached.last_modified_ms));
                    for e in &cached.footer.entries {
                        _ = substream_keys.insert((
                            e.substream(),
                            e.topic.to_string(),
                            e.partition,
                        ));
                    }
                }
            }
        }

        if segments_meta.is_empty() {
            return Ok(0);
        }

        // Single writer per prefix: serialized against retention and the size
        // merge by the same compaction lease (#115). Taken before the read walk
        // so N maintainers do not duplicate the GETs; under the #126 claim the
        // term is already held and this acquire is free.
        if self.acquire_compaction_lease(prefix).await.is_err() {
            debug!(prefix, "yielding per-key compaction to the lease holder");
            return Ok(0);
        }

        // GET every segment of the prefix once; a ghost index entry yields the
        // tick, as for the size merge (#274).
        let Some(objects) = self
            .fetch_segment_objects(prefix, segments_meta.keys().copied())
            .await?
        else {
            return Ok(0);
        };

        // Per-key transform over the EPOCH-FENCED view (as the size merge): a
        // zombie/dominated region is dropped from any rewrite and an overlap's
        // duplicated head batches are clipped off (#461), never fused in.
        // `outputs[seq]` accumulates every fenced sub-stream's (possibly
        // compacted) region so a dirty segment can be rebuilt whole.
        let mut outputs: BTreeMap<u64, Vec<SubstreamWrite>> = BTreeMap::new();
        let mut dirty: BTreeSet<u64> = BTreeSet::new();
        let mut removed_total: u64 = 0;
        let mut repaired_total: u64 = 0;

        for (substream, topic, partition) in substream_keys {
            // Decode every fenced region once, then walk them newest first — a
            // key kept in a newer region supersedes older copies.
            let mut regions =
                self.decode_fenced_regions(prefix, &substream, partition, &objects)?;
            regions.reverse();

            let mut seen: BTreeSet<Bytes> = BTreeSet::new();
            let mut staged: Vec<(u64, i64, Vec<deflated::Batch>, u64, u64)> =
                Vec::with_capacity(regions.len());
            let mut aborted = false;

            'transform: for (seq, base, region) in &regions {
                // Newest batch first within the region too.
                let mut out: Vec<deflated::Batch> = Vec::with_capacity(region.len());
                let mut removed: u64 = 0;
                let mut repaired: u64 = 0;
                for batch in region.iter().rev() {
                    // Transaction markers and transactional data are exempt
                    // from the per-key transform (#174 release B routes them
                    // into segments; this pass predates that). A marker's
                    // "key" is its ControlBatch bytes — every commit marker
                    // shares it — so key-dedup would strip older markers,
                    // leaving a read-committed consumer's aborted ranges
                    // unbounded. And this cleaner is abort-unaware: letting an
                    // aborted (or still-open) transactional record's key into
                    // `seen` could remove the only *committed* copy of that
                    // key beneath it — data loss for read-committed readers.
                    // Carry them whole and withhold their keys from `seen`:
                    // conservative (committed transactional data is never
                    // compacted) but safe until the cleaner is
                    // transaction-aware.
                    if batch.is_control() || batch.is_transactional() {
                        out.push(batch.clone());
                        continue;
                    }

                    // A batch whose LZ4 frame has dependent blocks is durable damage
                    // (#253): no Kafka Java client can decode it, and nothing else
                    // will ever rewrite it — the per-key transform below only
                    // re-encodes a batch it removes records from, and an emptied
                    // remnant has no keys left to supersede. Repairing it here is
                    // what takes a partition from "one worker cannot start" back to
                    // readable, and it costs a rewrite of a segment that is being
                    // rewritten anyway whenever anything else in it is dirty.
                    let repair_frame = batch.has_dependent_lz4_blocks();

                    let compaction = inflated::Batch::try_from(batch)?.compact(&seen)?;
                    seen.extend(compaction.batch.keys());
                    if seen.len() > self.tuning.prefix_compact_seen_keys {
                        warn!(
                            prefix,
                            topic,
                            partition,
                            keys = seen.len(),
                            "per-key compaction seen set over cap; skipping this partition for the tick"
                        );
                        aborted = true;
                        break 'transform;
                    }
                    if compaction.records > 0 {
                        removed += compaction.records as u64;
                        out.push(deflated::Batch::try_from(compaction.batch)?);
                    } else if repair_frame {
                        // Nothing to compact, but the frame itself is unreadable:
                        // re-encode the batch unchanged. The encoder emits an
                        // independent-block frame now, so this is the repair.
                        repaired += 1;
                        out.push(deflated::Batch::try_from(compaction.batch)?);
                    } else {
                        // Untouched: carry the ORIGINAL batch, not a re-encode.
                        out.push(batch.clone());
                    }
                }
                out.reverse();
                staged.push((*seq, *base, out, removed, repaired));
            }

            if aborted {
                // Removal is deferred for this partition, but a rewrite dirtied
                // by a SIBLING partition still needs this sub-stream's content:
                // contribute the originals, untouched.
                for (seq, base, region) in regions {
                    outputs.entry(seq).or_default().push(SubstreamWrite {
                        topition: Topition::new(topic.clone(), partition),
                        substream: substream.clone(),
                        base_offset: base,
                        batches: region,
                    });
                }
                continue;
            }

            for (seq, base, out, removed, repaired) in staged {
                // Either reason dirties the segment: records removed, or a frame
                // repaired (#253). A repair changes no record, so it is not counted
                // as a removal — but it must still reach the object store, or the
                // damage stays durable.
                if removed > 0 || repaired > 0 {
                    _ = dirty.insert(seq);
                    removed_total += removed;

                    if repaired > 0 {
                        repaired_total += repaired;

                        // `warn`, not `info` (#259): re-encoding durable damage is
                        // not steady state. It is bounded by the frames written
                        // while the encoder bug was live, each occurrence is the
                        // recovery of a partition no Java client could read, and
                        // production runs at `warn` — at `info` this line was
                        // structurally unable to appear where it is needed.
                        warn!(
                            prefix,
                            topic,
                            partition,
                            repaired,
                            "re-encoded LZ4 frames with dependent blocks (#253)"
                        );
                    }
                }
                outputs.entry(seq).or_default().push(SubstreamWrite {
                    topition: Topition::new(topic.clone(), partition),
                    substream: substream.clone(),
                    base_offset: base,
                    batches: out,
                });
            }
        }

        // The dirty-only guard: a clean prefix ends here, having written and
        // deleted nothing — the steady state every tick after convergence.
        if dirty.is_empty() {
            return Ok(0);
        }

        // Rewrite each dirty segment as a new create-only object (create, then
        // delete — never mutate), carrying the original's writer epoch: with an
        // identical offset span the overlap resolver's same-epoch/higher-seq
        // tie-break makes the rewrite win during the write→delete window,
        // exactly as for a #66 merge. `index_insert` keeps the original append
        // time so `compact,delete` retention is not reset by the rewrite.
        for seq in &dirty {
            let Some(substreams) = outputs.get(seq) else {
                continue;
            };
            let (epoch, last_modified) = segments_meta.get(seq).copied().unwrap_or((0, i64::MIN));

            let nonce = rng().random::<u64>();
            let (payload, footer) = self.encode_segment_indexed(
                substreams,
                epoch.max(0),
                nonce,
                self.tuning.segment_format_version,
            )?;
            let new_seq = self
                .assign_and_create_segment(prefix, payload, nonce, SegmentCreateRole::Compaction)
                .await?;
            self.index_insert(prefix, new_seq, footer, last_modified)?;
        }

        // Retire the rewritten seqs: floor before delete, then prune (#77) — see
        // [`Self::retire_segments`].
        let retired: Vec<u64> = dirty.iter().copied().collect();
        _ = self.retire_segments(prefix, &retired).await?;

        SEGMENT_RECORDS_COMPACTED.add(removed_total, &[]);
        SEGMENT_FRAMES_REPAIRED.add(repaired_total, &[]);
        debug!(
            prefix,
            removed = removed_total,
            repaired = repaired_total,
            rewritten = dirty.len(),
            "per-key compacted prefix segments"
        );

        Ok(removed_total)
    }

    /// Compact segments across every coalesced prefix (#66).
    /// Read the compaction lease object for `prefix` without acquiring it, or
    /// `None` if absent (#126). Used by the maintenance claim to peek the
    /// recency stamp before deciding whether to work the prefix.
    pub(super) async fn read_compaction_lease(&self, prefix: &str) -> Result<Option<PrefixLease>> {
        let location = self.compaction_lease_location(prefix);
        match self.object_store.get(&location).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Ok(Some(serde_json::from_slice::<PrefixLease>(&bytes)?))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// The prefixes whose segments this replica should size-merge this tick
    /// (#66): every non-compacted topic's prefix — plus every segment-routed
    /// compacted topic's dedicated prefix (#175) — restricted to this tick's
    /// maintenance claim (#126). Empty when prefix coalescing or compaction is
    /// off. Paired with [`Self::drain_compact_prefix`] by
    /// [`Self::maintain_prefix_segments`].
    pub(super) async fn compactable_prefixes(
        &self,
        owned: Option<&BTreeSet<String>>,
    ) -> Result<Vec<String>> {
        if self.tuning.prefix_compact_min_segments == 0 {
            return Ok(Vec::new());
        }

        // Derive the prefixes from the topic metadata (#66 review fix), NOT just
        // the in-memory index: a dedicated maintenance worker never produces or
        // fetches, so its index is empty — it must discover prefixes from the
        // topics (as retention does). Union with any locally-indexed prefixes.
        let mut prefix_set: BTreeSet<String> = self.prefixes.index()?.keys().cloned().collect();
        for metadata in self.topics_index().await?.iter() {
            let compact = metadata
                .topic
                .configs
                .as_deref()
                .unwrap_or_default()
                .iter()
                .any(|config| {
                    config.name == "cleanup.policy"
                        && config
                            .value
                            .as_deref()
                            .is_some_and(|value| value.contains("compact"))
                });
            // Segment-routed compacted topics (#175) are size-merge candidates
            // too: the per-key pass leaves cleaned small segments and header
            // residues behind, and the byte-identical merge (#66) is what
            // bounds their count.
            for partition in 0..metadata.topic.num_partitions {
                _ = prefix_set.insert(self.routed_prefix(
                    &Topition::new(metadata.topic.name.clone(), partition),
                    compact,
                ));
            }
        }
        // Honour this tick's maintenance claim (#126): only compact prefixes this
        // replica owns. `None` = no sharding (every prefix), the single-maintainer
        // default and the standalone-test path.
        Ok(prefix_set
            .into_iter()
            .filter(|prefix| owned.is_none_or(|owned| owned.contains(prefix)))
            .collect())
    }

    /// The dedicated prefixes of segment-routed compacted topics (#175),
    /// restricted to this tick's maintenance claim — the set
    /// [`Self::maintain_prefix_segments`] runs [`Self::compact_prefix_per_key`]
    /// on every tick. Derived from the topic metadata like the other
    /// maintenance universes (a dedicated maintainer's in-memory index is
    /// empty). Empty unless compacted topics are segment-routed.
    pub(super) async fn per_key_compact_prefixes(
        &self,
        owned: Option<&BTreeSet<String>>,
    ) -> Result<BTreeSet<String>> {
        let mut prefixes = BTreeSet::new();
        for metadata in self.topics_index().await?.iter() {
            // Compacted only, and deliberately *not* the widened carry-over
            // predicate (#211): the per-key pass keeps one value per key, so
            // running it over a topic that merely retains forever would delete
            // records the operator asked to keep.
            if !Self::topic_configs_are_compacted(&metadata.topic) {
                continue;
            }
            for partition in 0..metadata.topic.num_partitions {
                _ =
                    prefixes.insert(self.routed_prefix(
                        &Topition::new(metadata.topic.name.clone(), partition),
                        true,
                    ));
            }
        }
        Ok(prefixes
            .into_iter()
            .filter(|prefix| owned.is_none_or(|owned| owned.contains(prefix)))
            .collect())
    }

    /// Drain one prefix to `<= prefix_compact_min_segments` (#66 review fix): a
    /// single run per tick cannot keep up with a high flush rate, so loop until
    /// compaction finds nothing more to merge. Each call re-lists, so `S`
    /// converges to the trigger threshold within the tick. Errors are logged and
    /// end this prefix's drain only — one bad prefix must never abort the others'
    /// maintenance (#140).
    ///
    /// An **undecodable segment is the exception** (#398). `Ok(0)` and `Err(_)`
    /// used to share one `break`, which conflated "there is nothing left to
    /// merge" with "this run died", and run selection picks the *oldest*
    /// mergeable segments — so a damaged old object was re-read and re-failed on
    /// every tick that reached it, for as long as it existed, having merged
    /// nothing for the prefix. `CorruptSegment` names the segment, so the run can
    /// skip it and the drain can carry on over what is readable: #274's fix for
    /// `NotFound`, one error variant later.
    ///
    /// Every other error still ends the drain. They are not attributable to one
    /// object, so there is nothing to exclude and nothing to make the next run
    /// different — retrying would be a hot loop against the object store.
    ///
    /// A run that could not *proceed* is the third case (#399), and it used to be
    /// the same `break` as well: `fetch_segment_objects` answering `None` — the
    /// index named segments a peer had already retired — came back as `Ok(0)`,
    /// which reads as "drained". Selection picks the oldest segments, which are
    /// exactly the ones a peer's compaction retires first, and
    /// `tansu_prefix_segment_vanished_before_read` runs at 11/s on the fleet
    /// while the busiest prefixes sit at 30–68× their trigger. The pruned index
    /// makes the next selection different, so the drain takes it.
    ///
    /// Why the drain stopped is reported (`tansu_prefix_drain_stops`), because
    /// "does a drain that starts on a 15 000-segment prefix finish it?" was
    /// otherwise unanswerable from outside.
    pub(super) async fn drain_compact_prefix(&self, prefix: &str) -> u64 {
        /// Bounds the drain loop so a pathological prefix can't monopolize a
        /// maintenance tick; far above the runs a real backlog needs.
        const MAX_RUNS_PER_PREFIX: usize = 4_096;

        /// Bounds the runs that could not proceed, so a listing that keeps
        /// naming segments that are gone cannot spin.
        ///
        /// High on purpose. Each such run prunes *every* gone segment its run
        /// named, so the ghosts are consumed monotonically and the ceiling is
        /// (ghost entries / segments per run) — on the fleet, an index carrying
        /// tens of thousands of stale entries against runs of ~22 segments. A
        /// low cap would leave the drain stopping short of the live segments for
        /// exactly the prefixes furthest over the trigger, which is the
        /// behaviour being fixed. The real bounds are `MAX_RUNS_PER_PREFIX`
        /// above and the broker's maintenance run timeout (#131).
        const MAX_RETRIES_PER_PREFIX: usize = 1_024;

        let mut compacted = 0;
        let mut retries = 0usize;
        let mut reason = "runs";

        for _ in 0..MAX_RUNS_PER_PREFIX {
            let outcome = self.compact_prefix_segments(prefix).await;

            SEGMENT_COMPACT_RUNS.add(
                1,
                &[KeyValue::new(
                    "outcome",
                    outcome.as_ref().map_or("error", CompactRun::outcome),
                )],
            );

            match outcome {
                Ok(CompactRun::Merged(n)) => compacted += n,

                Ok(CompactRun::Drained) => {
                    reason = "drained";
                    break;
                }

                Ok(CompactRun::Retry) => {
                    retries += 1;
                    if retries >= MAX_RETRIES_PER_PREFIX {
                        warn!(
                            prefix,
                            retries, "drain kept selecting segments that are gone"
                        );
                        reason = "retries";
                        break;
                    }
                }

                // Damage attributable to one object: exclude it and keep
                // draining. `quarantine_segment` returning `false` means the run
                // was *already* selected without it, so the failure is not the
                // one this skip list can route around — end the drain as any
                // other error would.
                Err(Error::CorruptSegment(region)) => {
                    if !self
                        .quarantine_segment(&region)
                        .inspect_err(|err| error!(?err, prefix))
                        .unwrap_or_default()
                    {
                        error!(?region, prefix, "compaction damage the quarantine misses");
                        reason = "error";
                        break;
                    }
                }

                Err(err) => {
                    error!(?err, prefix);
                    reason = "error";
                    break;
                }
            }
        }

        PREFIX_DRAIN_STOPS.add(1, &[KeyValue::new("reason", reason)]);

        // Report what this replica has indexed for the prefix, so runaway `S` is
        // observable even if the drain can't keep up.
        //
        // Labelled by prefix (#284). Recorded once per prefix per pass with no
        // attributes, last write won, so the gauge showed whichever prefix
        // happened to be drained last — never the one running away, which is the
        // only reason to look at it. Cardinality is tens of prefixes.
        //
        // This is the index's size, not the bucket's (#399): what is really under
        // the prefix is `tansu_prefix_segments_live`, recorded from the listing
        // that reconciled this index rather than from the index it produced.
        let live: Option<BTreeSet<u64>> = self.prefixes.try_index().and_then(|index| {
            index.get(prefix).map(|entry| {
                PREFIX_INDEX_ENTRIES.record(
                    entry.segments.len() as u64,
                    &[KeyValue::new("prefix", prefix.to_string())],
                );

                entry.segments.keys().copied().collect()
            })
        });

        // Retire quarantine entries whose objects are gone (#398), under no other
        // lock — the index one above is released by here. Only against an index
        // this process actually holds for the prefix: an absent entry is "not
        // known", not "no segments", and pruning against it would silently empty
        // the skip list on a store with compaction disabled.
        if let Some(live) = live {
            _ = self
                .prune_quarantine(prefix, &live)
                .inspect_err(|err| error!(?err, prefix));
            _ = self
                .prune_compact_seams(prefix, &live)
                .inspect_err(|err| error!(?err, prefix));
        }

        compacted
    }

    /// Run the per-key pass over `prefix`, excluding any segment it proves
    /// undecodable and retrying over the rest (#398).
    ///
    /// The pass reads *every* segment of the prefix, so one damaged object cost
    /// the whole prefix its key cleanup on every tick — the same permanent stall
    /// the size merge had, on the pass that matters more: a compacted topic with
    /// no per-key cleanup grows stale versions forever.
    ///
    /// Attempts are tightly bounded because each one re-GETs the prefix: a tick
    /// quarantines at most `MAX_ATTEMPTS - 1` new bad segments and defers the
    /// rest, which converges over ticks without turning one tick into a scan of
    /// the same prefix a hundred times over.
    pub(super) async fn drain_compact_prefix_per_key(&self, prefix: &str) {
        const MAX_ATTEMPTS: usize = 8;

        for _ in 0..MAX_ATTEMPTS {
            match self.compact_prefix_per_key(prefix).await {
                Ok(_) => return,

                Err(Error::CorruptSegment(region)) => {
                    if !self
                        .quarantine_segment(&region)
                        .inspect_err(|err| error!(?err, prefix))
                        .unwrap_or_default()
                    {
                        error!(?region, prefix, "per-key damage the quarantine misses");
                        return;
                    }
                }

                Err(err) => {
                    error!(?err, prefix);
                    return;
                }
            }
        }

        warn!(
            prefix,
            attempts = MAX_ATTEMPTS,
            "per-key compaction still finding undecodable segments; the rest wait for the next tick"
        );
    }
}
