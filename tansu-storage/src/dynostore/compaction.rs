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

use std::ops::Range;

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

/// What a break in a run's tiling actually is (#594), and so how loudly it is
/// reported — the `reason` on `tansu_prefix_segment_compact_refused`.
///
/// Every break refuses the run either way (#399, #461): the merge reads its
/// output back by running offsets from the first region, so any break shifts
/// the records above it. What differs is whether a record is gone. All
/// breaks used to be one `ERROR` line, ~58k a week on the EU fleet, and the
/// read-path checks in #594 found every conclusive sample readable — so the
/// one signal that would show a new #461 hole was buried under false alarms.
/// Now only [`Self::Missing`] is an error, and `reason="missing"` is the rate
/// to alert on.
///
/// Declared in ascending severity, so the worst break of a run is its `max`.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum TilingBreak {
    /// Every missing offset is served by a live segment outside the run: the
    /// common case, a merged segment's fresh high sequence holding the low
    /// offsets between two older run members.
    OutsideRun,

    /// The offsets nothing serves are all below the partition's log start —
    /// under its `DeleteRecords` floor, or in the aged head retention is about
    /// to delete.
    BelowLogStart,

    /// The partition's topic is `cleanup.policy=compact`, where a gap is
    /// expected rather than a loss.
    CompactedTopic,

    /// Not a gap: a region starts *below* where the previous one ended, the
    /// fenced view having clipped it inside a batch it cannot split. Nothing is
    /// lost, but the overlap it comes from is meant to be flat zero
    /// (`tansu_prefix_segment_overlap_clipped`).
    Straddle,

    /// Offsets above the log start that no live segment holds: #461.
    Missing,
}

impl TilingBreak {
    /// Classify the break where the run expected `expected` and its next region
    /// starts at `region_base`, given the `[from, to)` spans the live segments
    /// outside the run serve.
    pub(super) fn of(
        expected: i64,
        region_base: i64,
        outside: &[(i64, i64)],
        log_start: i64,
        compacted: bool,
    ) -> Self {
        if region_base < expected {
            return Self::Straddle;
        }

        match highest_unserved(expected..region_base, outside) {
            None => Self::OutsideRun,
            Some(offset) if offset < log_start => Self::BelowLogStart,
            Some(_) if compacted => Self::CompactedTopic,
            Some(_) => Self::Missing,
        }
    }

    pub(super) fn reason(self) -> &'static str {
        match self {
            Self::OutsideRun => "outside_run",
            Self::BelowLogStart => "below_log_start",
            Self::CompactedTopic => "compacted_topic",
            Self::Straddle => "straddle",
            Self::Missing => "missing",
        }
    }

    /// `warn` for a straddle because it was an `ERROR` before #594 and still
    /// marks the overlap shape #461 came from; production runs at `warn`.
    pub(super) fn level(self) -> tracing::Level {
        match self {
            Self::OutsideRun | Self::BelowLogStart | Self::CompactedTopic => tracing::Level::DEBUG,
            Self::Straddle => tracing::Level::WARN,
            Self::Missing => tracing::Level::ERROR,
        }
    }

    fn log(self, prefix: &str, topic: &str, partition: i32, seq: u64, gap: (i64, i64)) {
        let (expected, region_base) = gap;
        let reason = self.reason();

        macro_rules! refusal {
            ($level:ident) => {
                $level!(
                    prefix,
                    topic,
                    partition,
                    seq,
                    expected,
                    region_base,
                    reason,
                    "refusing to compact: the run's fenced regions do not tile \
                     contiguously, and merging them would shift offsets"
                )
            };
        }

        match self.level() {
            tracing::Level::ERROR => refusal!(error),
            tracing::Level::WARN => refusal!(warn),
            _ => refusal!(debug),
        }
    }
}

/// The highest offset in `range` that none of the `[from, to)` spans serve.
fn highest_unserved(range: Range<i64>, spans: &[(i64, i64)]) -> Option<i64> {
    let mut spans = spans.to_vec();
    spans.sort_unstable();

    let mut cursor = range.start;
    let mut highest = None;
    for (from, to) in spans {
        if cursor >= range.end {
            break;
        }

        if from > cursor {
            highest = Some(from.min(range.end) - 1);
        }

        cursor = cursor.max(to);
    }

    if cursor < range.end {
        highest = Some(range.end - 1);
    }

    highest
}

/// The offsets a candidate run's raw footer entries hold, per sub-stream, as
/// sorted disjoint `[from, to)` intervals (#594).
///
/// Unlike [`RunCoverage`] this tolerates overlap — a batch-aligned overlap is
/// one the fenced merge heals — and only answers whether the run holds every
/// offset between its lowest and its highest.
#[derive(Debug, Default)]
pub(super) struct RunUnion {
    spans: BTreeMap<SubstreamKey, Vec<(i64, i64)>>,
}

impl RunUnion {
    /// Fold in `[from, to)`, coalescing every interval it overlaps or abuts.
    pub(super) fn insert(&mut self, key: SubstreamKey, from: i64, to: i64) {
        if to <= from {
            return;
        }

        let spans = self.spans.entry(key).or_default();
        let (mut from, mut to) = (from, to);

        spans.retain(|&(held_from, held_to)| {
            if held_from <= to && held_to >= from {
                from = from.min(held_from);
                to = to.max(held_to);
                false
            } else {
                true
            }
        });

        let at = spans.partition_point(|&(held_from, _)| held_from < from);
        spans.insert(at, (from, to));
    }

    /// The union of `spans` with this union's intervals for the sub-streams
    /// `spans` hold, and only those.
    pub(super) fn joined(&self, spans: &[(SubstreamKey, i64, i64)]) -> Self {
        let mut trial = Self::default();

        for (key, ..) in spans {
            if trial.spans.contains_key(key) {
                continue;
            }

            if let Some(held) = self.spans.get(key) {
                _ = trial.spans.insert(key.clone(), held.clone());
            }
        }

        for (key, from, to) in spans {
            trial.insert(key.clone(), *from, *to);
        }

        trial
    }

    /// The union of `spans`.
    pub(super) fn of(spans: &[(SubstreamKey, i64, i64)]) -> Self {
        let mut union = Self::default();
        for (key, from, to) in spans {
            union.insert(key.clone(), *from, *to);
        }
        union
    }

    /// Whether folding `spans` in would leave every sub-stream one unbroken
    /// interval, on a union that is one now.
    pub(super) fn connects(&self, spans: &[(SubstreamKey, i64, i64)]) -> bool {
        self.joined(spans).is_connected()
    }

    /// Whether any of `spans` shares an offset with the union; abutting does
    /// not count.
    pub(super) fn overlaps(&self, spans: &[(SubstreamKey, i64, i64)]) -> bool {
        spans.iter().any(|(key, from, to)| {
            self.spans.get(key).is_some_and(|held| {
                held.iter()
                    .any(|&(held_from, held_to)| held_from < *to && *from < held_to)
            })
        })
    }

    /// Whether every sub-stream is one unbroken interval.
    pub(super) fn is_connected(&self) -> bool {
        self.spans.values().all(|spans| spans.len() <= 1)
    }

    /// Each sub-stream with a break, and the ranges between its intervals.
    pub(super) fn gaps(&self) -> Vec<(SubstreamKey, Vec<Range<i64>>)> {
        self.spans
            .iter()
            .filter(|(_, spans)| spans.len() > 1)
            .map(|(key, spans)| {
                (
                    key.clone(),
                    spans.windows(2).map(|pair| pair[0].1..pair[1].0).collect(),
                )
            })
            .collect()
    }
}

/// Each `(seq, expected, region_base)` where `in_run` stops tiling `[base, ..)`.
///
/// Re-anchored past each break, so a second break in the same sub-stream is
/// found in the same pass.
fn tiling_breaks(in_run: &[(u64, i64, Vec<deflated::Batch>)]) -> Vec<(u64, i64, i64)> {
    let mut breaks = Vec::new();
    let Some(&(_, mut expected, _)) = in_run.first() else {
        return breaks;
    };

    for (seq, region_base, region) in in_run {
        if *region_base != expected {
            breaks.push((*seq, expected, *region_base));
            expected = *region_base;
        }

        expected += region
            .iter()
            .map(|batch| batch.last_offset_delta as i64 + 1)
            .sum::<i64>();
    }

    breaks
}

/// One batch of a run's raw region, at the offsets its own entry gives it.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) struct BatchSpan {
    pub(super) from: i64,
    pub(super) to: i64,
    pub(super) seq: u64,
    pub(super) crc: u32,
}

/// The first two batches in `spans` that claim overlapping offsets without
/// being the same batch (#614): the same span and the same CRC is a copy, any
/// other overlap is two different records competing for one offset.
pub(super) fn first_divergence(mut spans: Vec<BatchSpan>) -> Option<(BatchSpan, BatchSpan)> {
    spans.sort_unstable_by_key(|span| (span.from, span.to, span.seq));

    let mut reach: Option<BatchSpan> = None;
    for span in spans {
        if let Some(held) = reach
            && span.from < held.to
        {
            if (span.from, span.to, span.crc) != (held.from, held.to, held.crc) {
                return Some((held, span));
            }

            continue;
        }

        if reach.is_none_or(|held| span.to > held.to) {
            reach = Some(span);
        }
    }

    None
}

/// `substreams` with every region over `cap` bytes split into consecutive
/// regions of at most `cap` bytes each (#610), at batch boundaries; `0` is no
/// cap.
///
/// A batch is opaque, so one larger than `cap` is a region of its own. The
/// pieces tile the original exactly — each starts where the previous one's
/// records end — so a reader of the fenced view sees the same offsets it did
/// from the single region, in smaller reads.
pub(super) fn capped_regions(substreams: Vec<SubstreamWrite>, cap: usize) -> Vec<SubstreamWrite> {
    if cap == 0 {
        return substreams;
    }

    let mut capped = Vec::with_capacity(substreams.len());

    for SubstreamWrite {
        topition,
        substream,
        base_offset,
        batches,
    } in substreams
    {
        let mut base = base_offset;
        let mut next = base_offset;
        let mut bytes = 0usize;
        let mut region = Vec::new();

        for batch in batches {
            let size = batch.wire_size();

            if !region.is_empty() && bytes + size > cap {
                capped.push(SubstreamWrite {
                    topition: topition.clone(),
                    substream: substream.clone(),
                    base_offset: base,
                    batches: std::mem::take(&mut region),
                });

                base = next;
                bytes = 0;
            }

            next += batch.last_offset_delta as i64 + 1;
            bytes += size;
            region.push(batch);
        }

        capped.push(SubstreamWrite {
            topition,
            substream,
            base_offset: base,
            batches: region,
        });
    }

    capped
}

/// The time retention ages `cached` by, as `expire_prefix_segments` does: its
/// newest record, or the object's append time when timestamps are unset.
fn retention_age_ms(cached: &CachedSegment) -> i64 {
    let newest = cached
        .footer
        .entries
        .iter()
        .map(|e| e.max_timestamp)
        .max()
        .unwrap_or(i64::MIN);

    if newest > 0 {
        newest
    } else {
        cached.last_modified_ms
    }
}

/// A segment's `[from, to)` offsets, one per footer entry.
type OffsetSpans = Vec<(SubstreamKey, i64, i64)>;

/// What bounds a run [`DynoStore::select_compaction_run`] builds.
struct RunBounds<'a> {
    target: usize,
    skips: usize,
    quarantined: &'a BTreeSet<u64>,
    seams: &'a BTreeSet<u64>,
    armed: bool,
}

/// Every offset `[from, to)` interval `entry`'s live segments hold for `key`,
/// sorted and coalesced.
fn served_spans(entry: &PrefixIndex, key: &SubstreamKey) -> Vec<(i64, i64)> {
    let mut spans: Vec<(i64, i64)> = entry
        .substream_entries(&key.0, key.1)
        .map(|(_, _, e)| (e.base_offset, e.base_offset + e.record_count))
        .collect();
    spans.sort_unstable();

    let mut coalesced: Vec<(i64, i64)> = Vec::with_capacity(spans.len());
    for (from, to) in spans {
        match coalesced.last_mut() {
            Some(last) if from <= last.1 => last.1 = last.1.max(to),
            _ => coalesced.push((from, to)),
        }
    }

    coalesced
}

/// Whether one of the sorted, coalesced `spans` holds all of `range`.
fn covers(spans: &[(i64, i64)], range: &Range<i64>) -> bool {
    let at = spans.partition_point(|&(from, _)| from <= range.start);
    at > 0 && spans[at - 1].1 >= range.end
}

impl RunBounds<'_> {
    /// The positions in `segs` of the run seeded by `segs[0]`, as
    /// [`DynoStore::select_compaction_run`] describes it.
    fn walk(
        &self,
        entry: &PrefixIndex,
        segs: &[(u64, i64, i64, usize)],
        served: &mut BTreeMap<SubstreamKey, Vec<(i64, i64)>>,
    ) -> Vec<usize> {
        let mut union = RunUnion::default();
        let mut stepped = RunUnion::default();
        let mut coverage = RunCoverage::default();
        let (mut bytes, mut skipped) = (0usize, 0usize);
        let mut run: Vec<usize> = Vec::new();
        let mut aside = Vec::new();

        for (at, &(seq, _, _, size)) in segs.iter().enumerate() {
            if self.quarantined.contains(&seq) || (!run.is_empty() && self.seams.contains(&seq)) {
                break;
            }

            let Some(cached) = entry.segments.get(&seq) else {
                break;
            };
            let entries = &cached.footer.entries;
            let spans: OffsetSpans = entries
                .iter()
                .map(|e| {
                    (
                        (e.substream(), e.partition),
                        e.base_offset,
                        e.base_offset + e.record_count,
                    )
                })
                .collect();

            if stepped.overlaps(&spans) {
                break;
            }

            let joined = union.joined(&spans);
            let fits = size < self.target && (run.is_empty() || bytes + size <= self.target);

            if joined.is_connected() {
                if !fits || (self.armed && !coverage.extend(entries)) {
                    break;
                }

                for (key, from, to) in spans {
                    union.insert(key, from, to);
                }
                bytes += size;
                run.push(at);
                continue;
            }

            if self.armed || union.overlaps(&spans) {
                break;
            }

            let hole = joined.gaps().into_iter().any(|(key, gaps)| {
                let held = served
                    .entry(key.clone())
                    .or_insert_with(|| served_spans(entry, &key));
                gaps.iter().any(|gap| !covers(held, gap))
            });

            if hole {
                if fits {
                    run.push(at);
                }
                return run;
            }

            if skipped >= self.skips {
                break;
            }

            for (key, from, to) in &spans {
                stepped.insert(key.clone(), *from, *to);
            }
            skipped += 1;
            aside.push((at, spans, size));
        }

        self.fold_back(&mut run, &mut union, bytes, aside);
        run.sort_unstable();
        run
    }

    /// Join each stepped-over segment that the run has since grown up to,
    /// where it still fits and overlaps no segment left stepped over: what a
    /// sequence-contiguous run would have merged.
    fn fold_back(
        &self,
        run: &mut Vec<usize>,
        union: &mut RunUnion,
        mut bytes: usize,
        mut aside: Vec<(usize, OffsetSpans, usize)>,
    ) {
        while let Some(i) = (0..aside.len()).find(|&i| {
            let (_, spans, size) = &aside[i];
            *size < self.target
                && bytes + size <= self.target
                && union.connects(spans)
                && !aside
                    .iter()
                    .enumerate()
                    .any(|(j, (_, other, _))| j != i && RunUnion::of(other).overlaps(spans))
        }) {
            let (at, spans, size) = aside.swap_remove(i);
            for (key, from, to) in spans {
                union.insert(key, from, to);
            }
            bytes += size;
            run.push(at);
        }
    }
}

/// A break in a run's tiling, as [`DynoStore::compact_prefix_segments`]
/// classified it.
#[derive(Debug)]
struct RunBreak {
    substream: Substream,
    topic: String,
    partition: i32,
    seq: u64,
    gap: (i64, i64),
    class: TilingBreak,
}

impl DynoStore {
    /// Segments one run may step over (DEV-692), see
    /// [`Self::select_compaction_run`]. Under sustained produce a merged
    /// segment lands among about a dozen producer segments, so a run of ~200
    /// steps over ~17; this leaves room for slower merges without letting one
    /// start walk the whole prefix.
    const COMPACT_RUN_SKIPS: usize = 64;

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
        //
        // Compaction claims are left out (DEV-675): they now outlive their
        // merge until the floor covers them, and a run holding one — two
        // segments in, a claim and a merged segment out — rewrote claims
        // without end. A claim holds no offsets, so skipping it joins nothing
        // that was not already contiguous, and it does not count towards the
        // trigger: it is swept by `retire_compacted`, not merged.
        let mut segs: Vec<(u64, i64, i64, usize)> = {
            let index = self.prefixes.index()?;
            index
                .get(prefix)
                .map(|entry| {
                    entry
                        .segments
                        .iter()
                        .filter(|(_, cached)| !Self::is_compaction_claim(&cached.footer))
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

        // Offset coverage is checked only once this prefix is known to have a
        // hole in its tiling — a quarantined segment (#398) or a learned seam
        // (#399) — see [`RunCoverage`]. A prefix with neither is kept off a
        // merged segment's offsets by the run's own union and by
        // `straddle_free_len` (#594).
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
        let armed = !quarantined.is_empty() || !seams.is_empty();

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

        // Pick the OLDEST run of at least two segments to merge, up to the
        // target size — see [`Self::select_compaction_run`] for what bounds a
        // run, what it may step over, and #114's guard against a drain that
        // looks finished while small segments pile up.
        let (run, max_last_modified) =
            self.select_compaction_run(prefix, &segs[..eligible_end], &quarantined, &seams, armed)?;
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

        // Before the fenced view picks a winner (#614): merging keeps one side of
        // an overlap and retires the other, which is only harmless when both
        // sides hold the same batches.
        if let Some(refused) =
            self.refuse_divergent_run(prefix, &substream_keys, &footers, &objects)?
        {
            return Ok(refused);
        }

        let mut substreams: Vec<SubstreamWrite> = Vec::new();
        let mut merged_epoch = i64::MIN;
        let mut seams: BTreeSet<u64> = BTreeSet::new();
        let mut found: Vec<RunBreak> = Vec::new();

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
            let breaks = tiling_breaks(&in_run);
            for &(seq, expected, region_base) in &breaks {
                _ = seams.insert(seq);

                let class = self
                    .classify_tiling_break(
                        prefix,
                        &substream,
                        &Topition::new(topic.as_str(), partition),
                        &run,
                        (expected, region_base),
                    )
                    .await;
                found.push(RunBreak {
                    substream: substream.clone(),
                    topic: topic.clone(),
                    partition,
                    seq,
                    gap: (expected, region_base),
                    class,
                });
            }

            // The run is already refused; this sub-stream's batches would only
            // be carried into a merge that will not happen.
            if !breaks.is_empty() {
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

        if found
            .iter()
            .any(|found| found.class == TilingBreak::Missing)
            && self
                .served_after_relisting(prefix, &run, &mut found)
                .await?
        {
            SEGMENT_COMPACT_REFUSED.add(1, &[KeyValue::new("reason", "stale_index")]);
            warn!(
                prefix,
                "offsets this replica's index had no segment for are served after relisting \
                 the prefix: selecting again rather than refusing"
            );
            return Ok(CompactRun::Retry);
        }

        for found in &found {
            found
                .class
                .log(prefix, &found.topic, found.partition, found.seq, found.gap);
        }

        if let Some(worst) = found.iter().map(|found| found.class).max() {
            SEGMENT_COMPACT_REFUSED.add(1, &[KeyValue::new("reason", worst.reason())]);

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
            let substreams = capped_regions(substreams, self.tuning.prefix_compact_region_bytes);
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
        // [`Self::retire_compacted`]. Compaction usually adds a higher merged seq
        // so the listing max is unchanged, but when every run segment is
        // superseded no merged seq is written (`new_seq == None`) and deleting
        // the run *can* lower the listing max — freeing a run name for reuse
        // without the floor.
        //
        // The merged segment's claim stays until the floor covers it (DEV-675):
        // retiring it here moved the floor to the tail on every merge.
        _ = self.retire_compacted(prefix, &run, false).await?;

        SEGMENT_COMPACTIONS.add(run.len() as u64, &[]);
        debug!(
            prefix,
            ?new_seq,
            merged = run.len(),
            "compacted prefix segments"
        );

        Ok(CompactRun::Merged(run.len() as u64))
    }

    /// Refuse a run in which two segments put **different** batches at the same
    /// offsets of one sub-stream (#614), quarantining both, or `None` when every
    /// overlap in the run is a copy of the same batches.
    ///
    /// The fenced view resolves an overlap by priority and the merge then
    /// retires the whole run, so whatever the losing side held is deleted, and
    /// the offsets it covered are served from the winner from then on. That is
    /// the healing #461 wants when the two sides are the same records — a merged
    /// segment against its originals, a duplicated write — and it is exactly
    /// the reported failure when they are not: on the use1 cluster a consumer
    /// was served one side at 12:46, the merge at 13:21 kept the other, and the
    /// partition has served different records at those offsets ever since, with
    /// nothing in the bucket left to show it.
    ///
    /// So the merge compares the run's raw regions, not the fenced view, batch
    /// by batch: an overlap whose batches agree on offsets and CRC is a copy and
    /// merges as before; anything else keeps both objects, out of every future
    /// run in this process, and says so at `error`.
    ///
    /// Only within one `writer_epoch`. A lower epoch losing to a higher one is
    /// the fence working — a zombie writer's region, never acknowledged — and
    /// dropping it is what the epoch is for. The reported overlap was two
    /// segments of the same leaseless era, where no rule says which side is
    /// right. Readers are unaffected — the
    /// fenced view still answers them — but the evidence survives for
    /// `tansu audit`, and no record is retired on the strength of a priority
    /// rule.
    pub(super) fn refuse_divergent_run(
        &self,
        prefix: &str,
        substream_keys: &BTreeSet<(Substream, String, i32)>,
        footers: &BTreeMap<u64, SegmentFooter>,
        objects: &BTreeMap<u64, Bytes>,
    ) -> Result<Option<CompactRun>> {
        let mut learned = false;
        let mut divergent = false;

        for (substream, topic, partition) in substream_keys {
            let mut spans: BTreeMap<i64, Vec<BatchSpan>> = BTreeMap::new();

            for (seq, footer) in footers {
                let Some(object) = objects.get(seq) else {
                    continue;
                };

                for entry in footer
                    .entries
                    .iter()
                    .filter(|entry| entry.partition == *partition && entry.is(substream))
                {
                    let start = entry.byte_start as usize;
                    let end = start + entry.byte_len as usize;

                    // A region past its object fails the merge in
                    // `decode_fenced_regions` (#397), with its own diagnosis.
                    if end > object.len() {
                        continue;
                    }

                    let mut running = entry.base_offset;
                    for batch in
                        self.decode_region(prefix, *seq, entry, object.slice(start..end))?
                    {
                        let from = running;
                        running += batch.last_offset_delta as i64 + 1;
                        spans
                            .entry(footer.writer_epoch)
                            .or_default()
                            .push(BatchSpan {
                                from,
                                to: running,
                                seq: *seq,
                                crc: batch.crc,
                            });
                    }
                }
            }

            let Some((kept, other)) = spans.into_values().find_map(first_divergence) else {
                continue;
            };

            divergent = true;

            error!(
                prefix,
                topic,
                partition,
                seq = kept.seq,
                offsets = ?(kept.from, kept.to),
                other_seq = other.seq,
                other_offsets = ?(other.from, other.to),
                "refusing to compact: two segments hold different batches at the same \
                 offsets, and merging would retire one of them"
            );

            for seq in [kept.seq, other.seq] {
                learned |= self.quarantine_seq(prefix, seq)?;
            }
        }

        if !divergent {
            return Ok(None);
        }

        SEGMENT_COMPACT_REFUSED.add(1, &[KeyValue::new("reason", "divergent_overlap")]);

        Ok(Some(if learned {
            CompactRun::Retry
        } else {
            CompactRun::Drained
        }))
    }

    /// Whether a [`TilingBreak::Missing`] among `breaks` was this replica's
    /// index rather than the bucket: relist the whole prefix and classify each
    /// missing break again, `true` when one of them is now served.
    ///
    /// A missing break is classified from the index alone, and the index can
    /// lack segments the bucket holds. On the GCS bucket (DEV-692) a maintainer
    /// that had run for an hour refused ~20 runs as `missing`, on every hot
    /// prefix, and each refusal left a seam that turned step-over selection off
    /// for that prefix. Every one of those ranges read back whole from a
    /// consumer, and a restarted maintainer, whose first listing is the whole
    /// prefix, met none in the hour after. A real hole survives the relisting
    /// and is refused, logged and learned as before (#461, #399); a stale index
    /// gets one listing and a fresh selection, and no seam. Breaks of other
    /// classes do not depend on what the index lacks, and are left as they were.
    async fn served_after_relisting(
        &self,
        prefix: &str,
        run: &[u64],
        breaks: &mut [RunBreak],
    ) -> Result<bool> {
        self.index_invalidate(prefix)?;
        self.refresh_prefix_index_forced(prefix).await?;

        let mut served = false;

        for missing in breaks
            .iter_mut()
            .filter(|found| found.class == TilingBreak::Missing)
        {
            missing.class = self
                .classify_tiling_break(
                    prefix,
                    &missing.substream,
                    &Topition::new(missing.topic.as_str(), missing.partition),
                    run,
                    missing.gap,
                )
                .await;

            served |= missing.class != TilingBreak::Missing;
        }

        Ok(served)
    }

    /// The run [`Self::compact_prefix_segments`] merges next, and its newest
    /// append time: the oldest run of at least two segments of `segs` (the
    /// eligible ones, ascending by sequence) whose bytes fit under the target,
    /// or an empty run when there is none.
    ///
    /// **What bounds a run.** A quarantined segment (#398) and a learned seam
    /// (#399) end it: merging across either would shift every following
    /// record's offset into a hole. So does a segment holding offsets the run
    /// already holds, or ones that would extend it but no longer fit the
    /// target. A segment already at or above the target is never merged (#114):
    /// re-merging it rewrites ~target bytes to absorb one small neighbour, and
    /// if the next segment overflowed, a run of one would read as a drained
    /// prefix while small segments pile up behind the big one. It cannot seed a
    /// run either; selection moves past it.
    ///
    /// **What a run may step over** (DEV-692). Sequence order is not offset
    /// order once compaction has run: a merged segment is created at the tail,
    /// a fresh high sequence, holding the low offsets of the segments it
    /// replaced. Under sustained produce one lands about every second, among
    /// the producer segments created meanwhile, so a sequence-contiguous run
    /// met one about every dozen segments, and `straddle_free_len` (#594) cut
    /// it there: its offsets are not adjacent to the run's. Runs of ~200 small
    /// segments came out as a median of 9 on the GCS bucket, a merge retired
    /// fewer segments than producers created meanwhile, and the live count grew
    /// without bound.
    ///
    /// So a segment whose offsets neither overlap the run's nor extend them is
    /// stepped over and left where it is, and the run continues past it, as
    /// long as live segments hold every offset between the two. A hole nothing
    /// serves is met as before: the segment past it joins and ends the run, so
    /// the merge refuses it at the tiling check and learns the seam (#461). A
    /// stepped-over segment the run later grows up to is joined after all. The
    /// run stays offset-contiguous: it joins a segment only if every
    /// sub-stream it holds stays one unbroken interval, and never one that
    /// overlaps a segment it stepped over. So no stepped-over segment lies
    /// inside the run's offsets, nothing outside the run serves offsets the
    /// merged segment straddles, and the merged segment, created above every
    /// stepped-over one, shadows none of their offsets. At most
    /// [`Self::COMPACT_RUN_SKIPS`] are stepped over per run, bounding the walk
    /// from each start. On a prefix with a hole in its tiling (`armed`),
    /// nothing is stepped over: [`RunCoverage`] alone decides, as before.
    pub(super) fn select_compaction_run(
        &self,
        prefix: &str,
        segs: &[(u64, i64, i64, usize)],
        quarantined: &BTreeSet<u64>,
        seams: &BTreeSet<u64>,
        armed: bool,
    ) -> Result<(Vec<u64>, i64)> {
        let bounds = RunBounds {
            target: self.tuning.prefix_compact_target_bytes,
            skips: if armed { 0 } else { Self::COMPACT_RUN_SKIPS },
            quarantined,
            seams,
            armed,
        };
        let mut served = BTreeMap::new();
        let mut start = 0usize;

        while start < segs.len() {
            let (seq, ..) = segs[start];
            if segs[start].3 >= bounds.target || quarantined.contains(&seq) {
                start += 1;
                continue;
            }

            let candidate = {
                let index = self.prefixes.index()?;
                let Some(entry) = index.get(prefix) else {
                    return Ok((Vec::new(), i64::MIN));
                };

                bounds.walk(entry, &segs[start..], &mut served)
            };

            if candidate.len() >= 2 {
                let candidate: Vec<usize> = candidate.into_iter().map(|at| start + at).collect();
                let seqs: Vec<u64> = candidate.iter().map(|at| segs[*at].0).collect();
                let take = self.straddle_free_len(prefix, &seqs)?;

                if take >= 2 {
                    let last_modified = candidate[..take]
                        .iter()
                        .map(|at| segs[*at].2)
                        .max()
                        .unwrap_or(i64::MIN);
                    return Ok((seqs[..take].to_vec(), last_modified));
                }
            }

            start += 1;
        }

        Ok((Vec::new(), i64::MIN))
    }

    /// How many of `run`'s leading segments selection may merge without
    /// straddling a segment outside it (#594): all of them, unless the run's
    /// offsets have a gap that live segments outside the run fully serve — then
    /// only the longest leading stretch whose offsets are unbroken.
    ///
    /// Sequence order is not offset order once compaction has run: a merged
    /// segment, or a per-key rewrite (#175), holds low offsets at a fresh high
    /// sequence, so a sequence-contiguous run can hold the offsets on both
    /// sides of one and not the offsets in it. The tiling check refuses such a
    /// run, but only after fetching it, and [`RunCoverage`] only stops it from
    /// being built once a refusal has taught selection a seam: every merged
    /// segment cost a refusal first. That was the `outside_run` class, the bulk
    /// of the refusals #594 counted.
    ///
    /// A gap nothing outside the run fully serves is *not* avoided here: that is
    /// a `below_log_start`, `compacted_topic` or `missing` break, and the merge
    /// must still meet it, refuse it, say so and learn its seam — avoiding it
    /// silently would hide the one break that is a lost record (#461). Nor does
    /// this weaken that refusal: it only ever shortens a run, and whatever run
    /// it lets through is checked exactly as before.
    ///
    /// A failed lookup of the outside segments leaves the run whole, which is
    /// the behaviour before #594. `run` is ascending, as selection builds it.
    pub(super) fn straddle_free_len(&self, prefix: &str, run: &[u64]) -> Result<usize> {
        let mut union = RunUnion::default();
        let mut connected = 0;

        {
            let index = self.prefixes.index()?;
            let Some(entry) = index.get(prefix) else {
                return Ok(run.len());
            };

            for (n, seq) in run.iter().enumerate() {
                if let Some(cached) = entry.segments.get(seq) {
                    for e in &cached.footer.entries {
                        union.insert(
                            (e.substream(), e.partition),
                            e.base_offset,
                            e.base_offset + e.record_count,
                        );
                    }
                }

                if union.is_connected() {
                    connected = n + 1;
                }
            }
        }

        if connected == run.len() {
            return Ok(run.len());
        }

        for ((substream, partition), gaps) in union.gaps() {
            let outside: Vec<(i64, i64)> = self
                .with_fenced_segments(prefix, &substream, partition, |fenced| {
                    fenced
                        .iter()
                        .filter(|fenced| run.binary_search(&fenced.seq).is_err())
                        .map(|fenced| (fenced.served_from, fenced.end()))
                        .collect()
                })
                .inspect_err(|err| warn!(?err, prefix))
                .unwrap_or_default();

            if gaps
                .into_iter()
                .any(|gap| highest_unserved(gap, &outside).is_none())
            {
                return Ok(connected);
            }
        }

        Ok(run.len())
    }

    /// Classify the break `gap = (expected, region_base)` in `run`'s tiling of
    /// `topition` (#594), from the cached index and what this process already
    /// knows of the partition.
    ///
    /// Infallible on purpose: a lookup that fails leaves the break looking
    /// *worse*, never better — no outside coverage, no log start, not
    /// compacted — so an outage cannot hide a real hole at `debug`.
    pub(super) async fn classify_tiling_break(
        &self,
        prefix: &str,
        substream: &Substream,
        topition: &Topition,
        run: &[u64],
        gap: (i64, i64),
    ) -> TilingBreak {
        let (expected, region_base) = gap;

        let outside: Vec<(i64, i64)> = self
            .valid_substream_segments(prefix, substream, topition.partition())
            .inspect_err(|err| warn!(?err, prefix))
            .unwrap_or_default()
            .into_iter()
            .filter(|fenced| !run.contains(&fenced.seq))
            .map(|fenced| (fenced.served_from, fenced.end()))
            .collect();

        let log_start = self
            .tiling_log_start(prefix, substream, topition)
            .await
            .inspect_err(|err| warn!(?err, prefix))
            .unwrap_or(i64::MIN);

        let compacted = self
            .topic_is_compacted(topition.topic())
            .await
            .inspect_err(|err| warn!(?err, prefix))
            .unwrap_or_default();

        TilingBreak::of(expected, region_base, &outside, log_start, compacted)
    }

    /// Where `topition`'s log will start once retention has run (#594): its
    /// `DeleteRecords` floor, or past the head of segments already older than
    /// the prefix's retention threshold, whichever is higher.
    ///
    /// The head and not every aged segment, because retention walks a
    /// sub-stream in offset order and stops at the first segment it keeps.
    /// That head is what the EU check in #594 found under 7 of 12 refusals:
    /// segments still indexed, still selected, already past retention.
    async fn tiling_log_start(
        &self,
        prefix: &str,
        substream: &Substream,
        topition: &Topition,
    ) -> Result<i64> {
        let floor = self.truncate_floor(topition).await?;

        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|since| since.as_millis() as i64)
            .unwrap_or_default();
        let owned = BTreeSet::from([prefix.to_owned()]);
        let Some(threshold) = self
            .segment_retention_thresholds(now_ms, Some(&owned))
            .await?
            .get(prefix)
            .copied()
        else {
            return Ok(floor);
        };

        let fenced = self.valid_substream_segments(prefix, substream, topition.partition())?;
        let index = self.prefixes.index()?;
        let segments = index.get(prefix).map(|entry| &entry.segments);

        let mut start = i64::MIN;
        for fenced in fenced {
            if segments
                .and_then(|segments| segments.get(&fenced.seq))
                .is_none_or(|cached| retention_age_ms(cached) >= threshold)
            {
                start = start.max(fenced.served_from);
                break;
            }

            start = fenced.end();
        }

        Ok(start.max(floor))
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
        let mut claims = Vec::with_capacity(dirty.len());
        for seq in &dirty {
            let Some(substreams) = outputs.remove(seq) else {
                continue;
            };
            let substreams = capped_regions(substreams, self.tuning.prefix_compact_region_bytes);
            let (epoch, last_modified) = segments_meta.get(seq).copied().unwrap_or((0, i64::MIN));

            let nonce = rng().random::<u64>();
            let (payload, footer) = self.encode_segment_indexed(
                &substreams,
                epoch.max(0),
                nonce,
                self.tuning.segment_format_version,
            )?;
            let new_seq = self
                .assign_and_create_segment(prefix, payload, nonce, SegmentCreateRole::Compaction)
                .await?;
            self.index_insert(prefix, new_seq, footer, last_modified)?;
            claims.push(Self::claim_of(new_seq));
        }

        // Retire the rewritten seqs and their claims (DEV-662): floor before
        // delete, then prune (#77) — see [`Self::retire_segments`].
        let retired: Vec<u64> = dirty.iter().copied().chain(claims).collect();
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

        /// Merges one call makes before handing its slot to the next prefix
        /// (DEV-662). A prefix under sustained produce never reports `Drained`:
        /// each run merges what arrived during the last one. Once merges
        /// started landing on the GCS bucket, the first four prefixes kept all
        /// of the sweep's slots for the whole tick and the other three were
        /// never compacted. Sliced, [`Self::sweep_backlogged_prefixes`] comes
        /// back to every backlogged prefix, largest first.
        const MAX_MERGES_PER_SLICE: usize = 32;

        self.sweep_staged(prefix).await;

        let mut compacted = 0;
        let mut retries = 0usize;
        let mut merges = 0usize;
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
                Ok(CompactRun::Merged(n)) => {
                    compacted += n;
                    merges += 1;
                    if merges >= MAX_MERGES_PER_SLICE {
                        reason = "slice";
                        break;
                    }
                }

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

        if reason == "drained" {
            self.sweep_compaction_claims(prefix).await;
        }

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

    /// Retire the claims a drained prefix still carries (DEV-675), with one
    /// floor write when any of them is above the floor.
    ///
    /// A merge leaves its claim behind until a later merge's floor raise covers
    /// it, so the claims of the last stretch of a drain have no later merge to
    /// do that. They hold no offsets and cost a reader nothing but a name in a
    /// listing, but on a prefix that drains and goes quiet nothing else would
    /// ever take them: retention ages them like any segment, and a topic with
    /// no retention keeps them for good. Under the compaction lease, like every
    /// other segment-mutating maintenance step (#115).
    async fn sweep_compaction_claims(&self, prefix: &str) {
        match self.settled_claims(prefix) {
            Ok((claims, _)) if !claims.is_empty() => {}
            Ok(_) => return,
            Err(err) => {
                error!(?err, prefix);
                return;
            }
        }

        if self.acquire_compaction_lease(prefix).await.is_err() {
            return;
        }

        _ = self
            .retire_compacted(prefix, &[], true)
            .await
            .inspect_err(|err| error!(?err, prefix, "sweeping compaction claims"));
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

#[cfg(test)]
mod tiling_break_tests {
    use std::ops::Range;

    use super::{RunUnion, TilingBreak, highest_unserved};
    use tracing::Level;

    /// The run expected offset 50 next and found 100.
    const GAP: (i64, i64) = (50, 100);

    fn of(outside: &[(i64, i64)], log_start: i64, compacted: bool) -> TilingBreak {
        TilingBreak::of(GAP.0, GAP.1, outside, log_start, compacted)
    }

    #[test]
    fn served_outside_the_run_is_outside_run_at_debug() {
        let class = of(&[(40, 70), (70, 120)], 0, false);
        assert_eq!(TilingBreak::OutsideRun, class);
        assert_eq!("outside_run", class.reason());
        assert_eq!(Level::DEBUG, class.level());
    }

    #[test]
    fn unserved_below_the_log_start_is_below_log_start_at_debug() {
        let class = of(&[(80, 100)], 80, false);
        assert_eq!(TilingBreak::BelowLogStart, class);
        assert_eq!("below_log_start", class.reason());
        assert_eq!(Level::DEBUG, class.level());
    }

    #[test]
    fn unserved_on_a_compacted_topic_is_compacted_topic_at_debug() {
        let class = of(&[], 0, true);
        assert_eq!(TilingBreak::CompactedTopic, class);
        assert_eq!("compacted_topic", class.reason());
        assert_eq!(Level::DEBUG, class.level());
    }

    /// A region starting below the expected offset is an overlap the fenced
    /// view clipped inside a batch, not a gap.
    #[test]
    fn a_region_below_the_frontier_is_a_straddle_at_warn() {
        let class = TilingBreak::of(100, 50, &[], 0, false);
        assert_eq!(TilingBreak::Straddle, class);
        assert_eq!("straddle", class.reason());
        assert_eq!(Level::WARN, class.level());
    }

    /// One unserved offset above the log start is a loss, whatever covers the
    /// rest — and the precedence above never outranks it on a delete topic.
    #[test]
    fn unserved_above_the_log_start_is_missing_at_error() {
        let class = of(&[(50, 98)], 98, false);
        assert_eq!(TilingBreak::Missing, class);
        assert_eq!("missing", class.reason());
        assert_eq!(Level::ERROR, class.level());
    }

    /// A run's metric carries its worst break.
    #[test]
    fn missing_is_the_worst_break() {
        assert_eq!(
            Some(TilingBreak::Missing),
            [
                TilingBreak::OutsideRun,
                TilingBreak::Missing,
                TilingBreak::Straddle,
                TilingBreak::BelowLogStart,
            ]
            .into_iter()
            .max()
        );
    }

    #[test]
    fn highest_unserved_finds_the_top_of_the_last_hole() {
        assert_eq!(Some(99), highest_unserved(50..100, &[]));
        assert_eq!(None, highest_unserved(50..100, &[(0, 60), (60, 100)]));
        assert_eq!(Some(69), highest_unserved(50..100, &[(70, 100), (0, 55)]));
        assert_eq!(Some(99), highest_unserved(50..100, &[(60, 70)]));
        assert_eq!(None, highest_unserved(50..100, &[(0, 200)]));
    }

    fn key() -> super::SubstreamKey {
        (super::Substream::Name("t".into()), 0)
    }

    /// A merged segment's low offsets arriving after the run's high ones (#594).
    #[test]
    fn a_union_is_broken_until_its_gap_is_filled() {
        let mut union = RunUnion::default();
        union.insert(key(), 100, 150);
        assert!(union.is_connected());

        union.insert(key(), 0, 50);
        assert!(!union.is_connected());
        let gap: Range<i64> = 50..100;
        assert_eq!(vec![(key(), vec![gap])], union.gaps());

        union.insert(key(), 50, 100);
        assert!(union.is_connected());
        assert!(union.gaps().is_empty());
    }

    /// An overlap is not a gap: the fenced merge heals a batch-aligned one.
    #[test]
    fn a_union_coalesces_overlap_and_ignores_empty_spans() {
        let mut union = RunUnion::default();
        union.insert(key(), 0, 60);
        union.insert(key(), 40, 100);
        union.insert(key(), 200, 200);
        assert!(union.is_connected());

        union.insert(key(), 300, 310);
        union.insert(key(), 150, 160);
        assert_eq!(vec![(key(), vec![100..150, 160..300])], union.gaps());

        union.insert(key(), 90, 400);
        assert!(union.is_connected());
    }

    fn other() -> super::SubstreamKey {
        (super::Substream::Name("u".into()), 0)
    }

    /// A merged segment's low offsets land among a run's high ones (DEV-692):
    /// selection steps over what neither overlaps nor extends the run.
    #[test]
    fn a_union_connects_what_extends_it_and_overlaps_only_shared_offsets() {
        let mut union = RunUnion::default();
        assert!(union.connects(&[(key(), 100, 150)]));
        union.insert(key(), 100, 150);

        assert!(union.connects(&[(key(), 150, 160)]));
        assert!(union.connects(&[(key(), 90, 100)]));
        assert!(union.connects(&[(key(), 120, 200)]));
        assert!(union.connects(&[(other(), 0, 10)]));
        assert!(!union.connects(&[(key(), 0, 50)]));
        assert!(!union.connects(&[(key(), 150, 160), (other(), 0, 10), (key(), 170, 180)]));
        assert!(union.connects(&[(key(), 160, 170), (key(), 150, 160)]));

        assert!(!union.overlaps(&[(key(), 150, 160)]));
        assert!(!union.overlaps(&[(key(), 0, 100)]));
        assert!(!union.overlaps(&[(other(), 100, 150)]));
        assert!(union.overlaps(&[(key(), 149, 151)]));
        assert!(union.overlaps(&[(key(), 0, 500)]));
    }

    #[test]
    fn covers_needs_one_span_holding_the_whole_range() {
        let spans = [(0, 50), (60, 100)];
        assert!(super::covers(&spans, &(10..50)));
        assert!(super::covers(&spans, &(60..60)));
        assert!(!super::covers(&spans, &(40..70)));
        assert!(!super::covers(&spans, &(100..110)));
        assert!(!super::covers(&[], &(0..1)));
    }
}
