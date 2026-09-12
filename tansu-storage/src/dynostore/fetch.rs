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

//! The read path: turning a fetch or a `ListOffsets` into ranged GETs against
//! the segments the index names (#60).

use super::*;

impl OffsetSpan {
    pub(super) fn is_contiguous(&self) -> bool {
        self.records == self.end - self.base
    }
}

impl RunCoverage {
    /// Fold `entries` in, or leave the coverage untouched and answer `false` if
    /// any sub-stream would stop being one unbroken interval.
    ///
    /// Overlapping spans are refused by the same arithmetic (records exceed the
    /// offsets spanned). Inside a run that cannot normally happen — the merge
    /// reads the epoch-fenced view, which resolves overlaps — but these entries
    /// come from the raw footers, so on a prefix holding a zombie region this
    /// ends the run early rather than merging it. Refusing is the safe
    /// direction, and it only applies to a prefix that already has a quarantined
    /// segment.
    pub(super) fn extend(&mut self, entries: &[SubstreamEntry]) -> bool {
        let mut folded: Vec<(SubstreamKey, OffsetSpan)> = Vec::with_capacity(entries.len());

        for entry in entries {
            let key = (entry.substream(), entry.partition);
            let end = entry.base_offset + entry.record_count;

            let span = match self.spans.get(&key) {
                Some(held) => OffsetSpan {
                    base: held.base.min(entry.base_offset),
                    end: held.end.max(end),
                    records: held.records + entry.record_count,
                },

                None => OffsetSpan {
                    base: entry.base_offset,
                    end,
                    records: entry.record_count,
                },
            };

            if !span.is_contiguous() {
                return false;
            }

            folded.push((key, span));
        }

        for (key, span) in folded {
            _ = self.spans.insert(key, span);
        }

        true
    }
}

impl DynoStore {
    /// GET each of `seqs` whole, or `None` when any of them had already been
    /// deleted.
    ///
    /// A 404 is a ghost index entry, not a failure (#274 — the compaction half
    /// of the race #191 fixed on the refresh path). The incremental refresh is
    /// add-only, so a replica that indexed this prefix before a peer compacted
    /// it holds entries below its own cursor permanently. Treating that as fatal
    /// aborted the pass *after* the claim had stamped `maintained_at_ms`, so
    /// peers skipped the prefix for the whole recency window and its segment
    /// count grew unbounded with nothing reporting it. Instead the vanished
    /// sequences are pruned — names are never reused (#77), so dropping one is
    /// permanent and correct — the prefix is invalidated so the next tick
    /// re-lists, and `None` tells the caller to yield this tick.
    ///
    /// Both compaction passes read their inputs this way (#286): whole objects
    /// beat per-sub-stream ranged GETs once a prefix holds more than one
    /// sub-stream, and every byte is about to be rewritten anyway.
    pub(super) async fn fetch_segment_objects(
        &self,
        prefix: &str,
        seqs: impl IntoIterator<Item = u64>,
    ) -> Result<Option<BTreeMap<u64, Bytes>>> {
        let mut objects: BTreeMap<u64, Bytes> = BTreeMap::new();
        let mut vanished: Vec<u64> = Vec::new();

        for seq in seqs {
            match self
                .object_store
                .get(&self.segment_location(prefix, seq))
                .await
            {
                Ok(result) => {
                    _ = objects.insert(seq, result.bytes().await.map_err(Error::from)?);
                }

                Err(object_store::Error::NotFound { .. }) => {
                    SEGMENT_VANISHED_BEFORE_READ.add(1, &[]);
                    SEGMENT_ABSENT.add(1, &[KeyValue::new("caller", "compaction")]);
                    debug!(
                        prefix,
                        seq, "segment deleted before compaction could read it; pruning"
                    );
                    vanished.push(seq);
                }

                Err(err) => {
                    error!(?err, prefix, seq);
                    return Err(Error::from(err));
                }
            }
        }

        if !vanished.is_empty() {
            self.index_prune(prefix, &vanished)?;
            self.index_invalidate(prefix)?;
            return Ok(None);
        }

        Ok(Some(objects))
    }

    /// Decode a sub-stream's epoch-fenced regions out of already-fetched segment
    /// objects, as `(seq, base_offset, batches)` ascending by sequence.
    ///
    /// The fenced view ([`Self::valid_substream_segments`]) is what both
    /// compaction passes transform (#286): overlap-resolved, higher
    /// epoch/sequence wins, so a zombie region is dropped here and never fused
    /// into a rewritten segment. The view spans the whole prefix, so it is
    /// narrowed to the segments the caller actually fetched — the size merge
    /// fetches its run, per-key rewrite fetches everything.
    ///
    /// A region whose recorded extent runs past its object **fails the run**
    /// (#397). It used to be skipped, on the reasoning that carrying its base
    /// forward without its records would mislabel the batches that follow it —
    /// but skipping mislabels them just as badly, and durably. The merged region
    /// is written with the base offset of its first region and read back by
    /// running offsets from there, so a dropped region in the middle of a run
    /// slides everything above it down into the gap, and `retire_segments` then
    /// deletes the original the records could still have been read from.
    ///
    /// Failing the run costs a merge; #398's quarantine then excludes the object
    /// and the drain proceeds over what is readable. Silently rewriting the
    /// prefix with shifted offsets costs the data.
    ///
    /// A **clipped** region ([`FencedSegment::is_clipped`], #461) contributes
    /// from its first batch not wholly below `served_from`: the leading batches
    /// duplicate offsets a higher-priority segment already serves, and fusing
    /// them into a rewrite would concatenate duplicate offsets. The returned
    /// base is that first kept batch's absolute offset — `served_from` when the
    /// frontier lands on a batch boundary, below it when a batch straddles the
    /// frontier (batches are opaque; the merge's contiguity check is what keeps
    /// a straddle from being fused mid-run).
    pub(super) fn decode_fenced_regions(
        &self,
        prefix: &str,
        substream: &Substream,
        partition: i32,
        objects: &BTreeMap<u64, Bytes>,
    ) -> Result<Vec<(u64, i64, Vec<deflated::Batch>)>> {
        let fenced = self.valid_substream_segments(prefix, substream, partition)?;
        let mut regions = Vec::with_capacity(fenced.len());

        for fenced in fenced {
            let Some(object) = objects.get(&fenced.seq) else {
                continue;
            };

            let start = fenced.entry.byte_start as usize;
            let end = start + fenced.entry.byte_len as usize;
            if end > object.len() {
                let available = object.slice(start.min(object.len())..);

                return Err(RegionRead {
                    prefix,
                    seq: fenced.seq,
                    entry: &fenced.entry,
                    encoded: &available,
                }
                .short_of_extent(format!(
                    "region extent runs {} bytes past a {}-byte object",
                    end - object.len(),
                    object.len(),
                )));
            }

            let mut batches =
                self.decode_region(prefix, fenced.seq, &fenced.entry, object.slice(start..end))?;

            let mut base = fenced.entry.base_offset;
            if fenced.is_clipped() {
                let mut dropped = 0;
                for batch in &batches {
                    let span = batch.last_offset_delta as i64 + 1;
                    if base + span > fenced.served_from {
                        break;
                    }
                    base += span;
                    dropped += 1;
                }
                drop(batches.drain(..dropped));
            }

            regions.push((fenced.seq, base, batches));
        }

        Ok(regions)
    }

    /// Fetch a topition's records out of shared prefix-coalesced segments (#60).
    /// Segments are located from the in-memory footer index (read-path #60 review
    /// fix — no `segments/` LIST and no per-segment footer GET per fetch), and
    /// only the segments overlapping `[offset, high_watermark)` are read, each
    /// with a single ranged GET of exactly the sub-stream's contiguous byte span
    /// — never the whole object, no cross-topic data. Absolute offsets come from
    /// the footer. Epoch-fenced (`valid_substream_segments`), so a stale-epoch
    /// (zombie) segment is skipped. A segment deleted by retention mid-fetch (a
    /// 404 on the data GET) is pruned and skipped. Bounded by `max_bytes` and by
    /// `max_wait` from `started_at`.
    /// Record one segment-data ranged GET and classify it against what this pod
    /// has recently read from the same object (#117). Measurement only: it never
    /// changes what is fetched, and a poisoned lock or a full trace degrades the
    /// numbers, never the read.
    pub(super) fn note_segment_data_read(
        &self,
        prefix: &str,
        seq: u64,
        byte_start: u64,
        byte_len: u64,
    ) {
        SEGMENT_DATA_GETS.add(1, &[]);
        SEGMENT_DATA_BYTES.add(byte_len, &[]);

        let Some(mut traces) = self.prefixes.traces() else {
            return;
        };

        let now = SystemTime::now();
        let fresh = |trace: &SegmentReadTrace| {
            now.duration_since(trace.last_read)
                .is_ok_and(|elapsed| elapsed < SEGMENT_READ_TRACE_TTL)
        };
        let range = (byte_start, byte_len);
        let key = (prefix.to_owned(), seq);

        match traces.get_mut(&key) {
            // Read again while still resident-ish: this is the repeat a cache
            // could have served — but only the block cache #117 proposes if the
            // span is identical.
            Some(trace) if fresh(trace) => {
                let overlap = if trace.ranges.contains(&range) {
                    "same_range"
                } else {
                    if trace.ranges.len() < SEGMENT_READ_TRACE_RANGES {
                        trace.ranges.push(range);
                    }
                    "other_range"
                };

                SEGMENT_DATA_GET_REPEATS.add(1, &[KeyValue::new("overlap", overlap)]);
                trace.last_read = now;
            }

            // Stale entry: too long ago to credit a cache with, so restart the
            // object's trace rather than count it as a repeat.
            Some(trace) => {
                trace.ranges.clear();
                trace.ranges.push(range);
                trace.last_read = now;
            }

            None => {
                if traces.len() >= SEGMENT_READ_TRACE_OBJECTS {
                    traces.retain(|_, trace| fresh(trace));

                    if traces.len() >= SEGMENT_READ_TRACE_OBJECTS {
                        traces.clear();
                    }
                }

                _ = traces.insert(
                    key,
                    SegmentReadTrace {
                        ranges: vec![range],
                        last_read: now,
                    },
                );
            }
        }
    }

    pub(super) async fn fetch_prefix_coalesced(
        &self,
        topition: &Topition,
        offset: i64,
        max_bytes: u32,
        high_watermark: i64,
        started_at: SystemTime,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let has_deadline_expired = || {
            started_at
                .elapsed()
                .map(|elapsed| max_wait.saturating_sub(elapsed).is_zero())
                .unwrap_or_default()
        };

        /// A segment can be deleted mid-fetch by compaction/retention; the merged
        /// segment covers the same offsets, so on a 404 we refresh the index and
        /// restart cleanly. Bounded so a genuinely missing object can't loop.
        const MAX_ATTEMPTS: usize = 3;

        /// Ranged region GETs in flight for one sub-stream (#426), matching the
        /// footer warm's `FOOTER_FETCH_CONCURRENCY` immediately above it and every
        /// other fan-out in the engine (`list_offsets`, `offset_commit`,
        /// `offset_fetch`, `metadata_fetch`, `list_state`, `describe`).
        const SEGMENT_READ_CONCURRENCY: usize = 32;

        let (prefix, substream) = self.routed_substream_of(topition).await?;

        for _ in 0..MAX_ATTEMPTS {
            self.refresh_prefix_index(&prefix).await?;
            let segments =
                self.valid_substream_segments(&prefix, &substream, topition.partition())?;

            // Plan the reads from the index before issuing any (#426).
            //
            // Every input to the decision is an index field: the offset skip and
            // the high-watermark stop come from `base_offset`/`record_count`, and
            // the byte budget consumes `entry.byte_len`, which is the cached
            // footer's claim and not the response's length. So the set of
            // segments this fetch will read is known before the first GET — which
            // is the whole reason the reads can be concurrent instead of a chain.
            //
            // This was the only fan-out in the engine that was not buffered. The
            // footer warm immediately above it is `buffered(32)` with a comment
            // explaining that a sequential footer-per-segment loop stalled
            // `list_offsets` past the client timeout; the data read below it had
            // the same shape and no buffering. Measured with injected per-GET
            // latency over 64 segments in one partition: 2 ms at 0 ms, 431 ms at
            // 5 ms, 1 417 ms at 20 ms — exactly linear, and that is the innermost
            // loop alone.
            let mut bytes = max_bytes as u64;
            let mut plan: Vec<FencedSegment> = Vec::new();

            for fenced in segments {
                // Segments are sorted by base offset; skip those ending at/before
                // the requested offset, stop once one starts at/past the HWM.
                if fenced.end() <= offset {
                    continue;
                }
                if fenced.entry.base_offset >= high_watermark {
                    break;
                }

                // The budget admits the entry that crosses it and stops *after*,
                // which is what the serial loop did: it read the region, pushed
                // its batches, and only then compared. Preserved exactly —
                // returning one region fewer per round trip is a behaviour change
                // a client would see.
                let byte_len = fenced.entry.byte_len;
                plan.push(fenced);

                if byte_len > bytes {
                    break;
                }
                bytes = bytes.saturating_sub(byte_len);
            }

            // `&str` rather than the `String`, so each read's future captures a
            // `Copy` borrow instead of moving the prefix the attempt loop reuses.
            let prefix = prefix.as_str();

            // Eagerly collected to pin lifetimes, the same idiom `Metadata` and
            // `ListOffsets` use in this file (#147): the futures are inert until
            // `buffered` polls them, so this allocates, it does not serialise.
            let reads = plan
                .iter()
                .map(|fenced| {
                    let seq = fenced.seq;
                    let entry = &fenced.entry;
                    let location = self.segment_location(prefix, seq);

                    async move {
                        self.note_segment_data_read(prefix, seq, entry.byte_start, entry.byte_len);

                        // One ranged GET of exactly this sub-stream's byte span.
                        match self
                            .object_store
                            .get_opts(
                                &location,
                                GetOptions {
                                    range: Some(GetRange::Bounded(
                                        entry.byte_start..entry.byte_start + entry.byte_len,
                                    )),
                                    ..Default::default()
                                },
                            )
                            .await
                        {
                            Ok(result) => {
                                let encoded = result.bytes().await.map_err(Error::from)?;

                                // Short of the extent the index claims (#397): the
                                // index entry and the object disagree, and only the
                                // object's own trailer says which is wrong. A
                                // correctable index is repaired and the fetch restarts
                                // off it — the whole entry, `base_offset` and
                                // `record_count` included, feeds the offset arithmetic
                                // below, so re-reading one region inline would mix a
                                // corrected extent with a stale span.
                                if (encoded.len() as u64) < entry.byte_len {
                                    self.resolve_short_region(
                                        prefix, seq, entry, &location, &encoded,
                                    )
                                    .await?;

                                    return Ok(RegionOutcome::Corrected);
                                }

                                // A full-length region that holds no frame is the
                                // *same* index/object disagreement, and the branch
                                // above cannot see it: an entry that under-states a
                                // healthy frame is served in full, so
                                // `read_len == byte_len` (#432). Ask the object's own
                                // trailer here too, and restart off a corrected entry
                                // — otherwise a wrong entry is a `CORRUPT_MESSAGE`
                                // that the client retries at the same offset, forever.
                                //
                                // Damage that survives the trailer still answers this
                                // partition `CORRUPT_MESSAGE` (#386) rather than
                                // propagating a bare integer-conversion failure that
                                // took the request, and the connection, with it.
                                match self.decode_region(prefix, seq, entry, encoded) {
                                    Ok(decoded) => Ok(RegionOutcome::Decoded(decoded)),

                                    Err(Error::CorruptSegment(corrupt)) => {
                                        self.resolve_corrupt_region(
                                            prefix, seq, entry, &location, corrupt,
                                        )
                                        .await?;

                                        Ok(RegionOutcome::Corrected)
                                    }

                                    Err(otherwise) => Err(otherwise),
                                }
                            }

                            // Deleted between locate and read (compaction #66 /
                            // retention #61). Reported rather than handled here: the
                            // prune, the reconciling listing and the restart are the
                            // caller's, so they happen once for the attempt instead of
                            // once per concurrent read that met the same stale prefix.
                            Err(object_store::Error::NotFound { .. }) => {
                                Ok(RegionOutcome::Vanished)
                            }

                            Err(error) => {
                                error!(?error, location = %location);
                                // Preserve the storage error so it is classified
                                // retriable rather than fatal `-1` (#6/#129).
                                Err(Error::from(error))
                            }
                        }
                    }
                })
                .collect::<Vec<_>>();

            let mut reads = futures::stream::iter(reads).buffered(SEGMENT_READ_CONCURRENCY);

            let mut batches = vec![];
            let mut restart = false;
            let mut spent = false;

            // What the response may still carry (#535).
            //
            // A second budget, over the same `max_bytes` the plan spent above,
            // because the two bound different things and only one of them was
            // bounded. The plan's budget picks which regions to *read*, in units
            // of `entry.byte_len` — the footer's claim over a whole sub-stream
            // region. This one picks which decoded batches to *return*, in the
            // units the client's `max_bytes` is written in.
            //
            // Without it, a response is `max_bytes` plus the whole of the region
            // that crossed the budget, and a region is sized by the *writer*:
            // `coalesce_bytes` (1 MiB by default, **64 MiB** on the production
            // fleet) and `message_max_bytes` (1 MiB by default, 10 MiB there),
            // against a `Fetch` budget that `FetchService` clamps to 5 MiB.
            // Measured on that fleet: ~14.7 MB shipped per response against the
            // 5 MiB clamp, 2.8-3.8x. That is not a rounding error a client can
            // absorb — it sizes its buffers off `fetch.max.bytes`, and a
            // single-threaded consumer that drains the socket only from inside
            // `poll()` spends the whole overshoot returning empty polls.
            //
            // Kafka's `minOneMessage` survives, as it must: a budget smaller
            // than the first batch still returns that batch, or a partition
            // holding one oversized record could never make progress and the
            // client would retry the same offset forever. So the bound is
            // `max_bytes` plus at most one batch — never `max_bytes` plus one
            // region.
            let mut budget = max_bytes as u64;

            // Consumed in input order, so the assembled batches are the serial
            // loop's. Stopping early drops the futures still in flight, which
            // cancels their reads — the same thing the serial `break` did, one
            // round trip earlier.
            for fenced in plan.iter() {
                // Checked before consuming, which is where the serial loop
                // checked it: before the work for this segment, not after. Tested
                // after the await instead, a single GET that spends the budget
                // would discard its own result and the fetch would answer empty.
                if has_deadline_expired() {
                    break;
                }

                let Some(outcome) = reads.next().await else {
                    break;
                };

                match outcome? {
                    RegionOutcome::Decoded(region) => {
                        let mut running = fenced.entry.base_offset;
                        for mut batch in region {
                            let span = batch.last_offset_delta as i64 + 1;
                            batch.base_offset = running;
                            running += span;

                            // The same skip against the *fetch offset* (#535).
                            //
                            // The plan drops a whole segment only when
                            // `fenced.end() <= offset`, so an admitted region
                            // routinely begins far below the position asked
                            // for, and every batch of it used to be returned.
                            // On an uncompacted log that costs little — the
                            // regions are small and the plan skips most of them
                            // — but a compacted prefix holds the sub-stream in
                            // one large region, and then the answer to a mid-log
                            // fetch is mostly records the client already has.
                            // Measured on a merged prefix: of 19 648 records
                            // returned for a fetch at offset 19 200, 19 200 were
                            // below it. With the byte bound above now spending
                            // the budget in order, those wasted records crowd
                            // out the ones asked for, so this is not a separate
                            // improvement — it is what keeps the bound from
                            // costing a mid-log consumer its throughput.
                            //
                            // Straddling batches are kept whole, for the reason
                            // the `served_from` comment below gives: a
                            // compressed batch beginning below the position
                            // still holds records above it, and a consumer drops
                            // what it has already seen.
                            if running <= offset {
                                continue;
                            }

                            // A batch wholly below `served_from` duplicates
                            // offsets the higher-priority segment before this
                            // one already served (#461): skip it. A batch
                            // straddling the frontier is kept whole — its tail
                            // is held by nothing else, and a consumer skips
                            // records below its position, exactly as for any
                            // compressed batch starting before the fetch
                            // offset.
                            if running <= fenced.served_from {
                                continue;
                            }

                            // Spent, and something to show for it: stop here
                            // rather than carry the rest of this region (#535).
                            //
                            // Tested before the push and not after, so the
                            // batch that crosses the budget is still returned —
                            // `max_bytes` is explicitly not an absolute maximum
                            // in Kafka, and a reader that stopped short of the
                            // crossing batch would make no progress on a
                            // partition whose next batch is larger than the
                            // budget. `batches.is_empty()` is what carries
                            // `minOneMessage`: an exhausted budget still admits
                            // the first batch of the response.
                            if budget == 0 && !batches.is_empty() {
                                spent = true;
                                break;
                            }

                            budget = budget.saturating_sub(batch.wire_size() as u64);
                            batches.push(batch);
                        }

                        // Whole regions still in flight behind this one are
                        // dropped unpolled, which cancels their GETs — the same
                        // thing the deadline break above does.
                        if spent {
                            break;
                        }
                    }

                    // Drop anything gathered so far, evict the stale seq
                    // (prune-on-404 — the add-only refresh never would), force a
                    // re-list to pick up the merged/surviving segments, and
                    // restart clean. The merged segment covers the same offsets
                    // and wins the overlap (higher seq), so no gap/duplicate.
                    RegionOutcome::Vanished => {
                        // Counted (#399): the same event the compaction path has
                        // always counted, and on the fleet the *bigger* half —
                        // 39 `segment` 404s/s on the brokers against 7 on the
                        // maintainers — while this side reported none of it,
                        // because the incremental index refresh is add-only and
                        // nothing here ever said how much of it was stale.
                        SEGMENT_VANISHED_BEFORE_READ.add(1, &[]);
                        SEGMENT_ABSENT.add(1, &[KeyValue::new("caller", "fetch")]);
                        self.index_prune(prefix, &[fenced.seq])?;

                        // Pruning the one sequence the 404 named leaves every
                        // other stale entry in place. The restart's refresh
                        // settles the whole prefix with one listing when its
                        // downward pass is due (#408) — the same rate limit this
                        // arm used to apply to a listing of its own, and no longer
                        // the only thing that triggers one: a retired segment sits
                        // at the head, where no consumer reads, so a fetch is the
                        // *last* thing to discover it.
                        self.index_invalidate(prefix)?;
                        restart = true;
                        break;
                    }

                    RegionOutcome::Corrected => {
                        restart = true;
                        break;
                    }
                }
            }

            if !restart {
                return Ok(batches);
            }
        }

        // Exhausted retries (persistent 404 churn): return what a final clean
        // pass can read rather than erroring.
        self.refresh_prefix_index(&prefix).await?;
        Ok(vec![])
    }

    /// The earliest (log-start) offset for a prefix-coalesced sub-stream (#60):
    /// the base offset of the oldest legacy `records/` object if any survive
    /// (they hold the lowest offsets until retention drains them, #60 hybrid),
    /// otherwise the base offset in the oldest segment (lowest sequence) that
    /// carries the sub-stream. `0` when neither exists yet. Clamped to the
    /// truncation floor (#176) in both arms: truncated records survive
    /// physically (in shared segments always; in the legacy region on a
    /// replica that has not observed the physical delete), so the oldest
    /// physical base can sit below the logical log start.
    pub(super) async fn coalesced_earliest_offset(&self, topition: &Topition) -> Result<i64> {
        // `truncate_floor` (not `cached_truncate`): EARLIEST does not pass
        // through the high-watermark slow path that warms the watermark
        // cache, so on a fresh process this is the read that resolves the
        // floor (once — absence is memoized, #161).
        let floor = self.truncate_floor(topition).await?;

        // The oldest segment's base for this sub-stream, from the index (#179: the
        // legacy region that used to hold lower offsets can no longer exist).
        //
        // With no segment the log is empty and EARLIEST is the log end, not 0
        // (#290) — see [`Self::log_start`] for why the 0 was worth removing.
        // This is the site a client actually reads: `LOG-START-OFFSET` comes
        // from ListOffsets EARLIEST, so it is where the false start offset
        // became visible as unretireable lag.
        let start = match self.segment_region_start(topition).await? {
            Some(base) => base,
            // Paid only when there is no segment, so a healthy EARLIEST keeps
            // its request profile.
            None => self.high_watermark(topition).await?,
        };

        Ok(start.max(floor))
    }

    /// The newest record timestamp for a PURE-segment sub-stream (#73), from the
    /// footer index — the tail segment's `max_timestamp`. Returns `None` (caller
    /// falls back to the legacy `records/` listing) when the sub-stream has no
    /// segment yet, when the footer carries no timestamp, OR when the topic is
    /// unconditional since #179: a legacy `records/` object could sit ABOVE the
    /// segment tail (the #58 seam, the #62 bypass), which made the footer's
    /// `max_timestamp` not the log's latest timestamp — no writer can create one,
    /// and no read path can serve one.
    pub(super) async fn coalesced_latest_timestamp(
        &self,
        topition: &Topition,
    ) -> Result<Option<SystemTime>> {
        let (prefix, substream) = self.routed_substream_of(topition).await?;
        self.refresh_prefix_index(&prefix).await?;
        Ok(self
            .valid_substream_segments(&prefix, &substream, topition.partition())?
            .last()
            .map(|fenced| fenced.entry.max_timestamp)
            .filter(|&ms| ms >= 0)
            .map(|ms| SystemTime::UNIX_EPOCH + Duration::from_millis(ms as u64)))
    }

    /// Resolve one partition's `ListOffsets` entry: the offset (and best-effort
    /// timestamp) for `offset_request`. `stable` maps a topition to its first
    /// unstable offset under read-committed isolation (empty for
    /// read-uncommitted). This is a single entry of [`Storage::list_offsets`],
    /// split out so a request's partitions can be resolved concurrently —
    /// `Ok(None)` means "no entry for this partition" (an unparseable batch
    /// object name), exactly the cases the sequential loop used to `continue`
    /// past without pushing a response.
    pub(super) async fn list_offset_response(
        &self,
        topition: &Topition,
        offset_request: &ListOffset,
        stable: &BTreeMap<Topition, Offset>,
    ) -> Result<Option<ListOffsetResponse>> {
        // LATEST is the log end offset — except under read-committed with an open
        // transaction, where it is the **last stable offset**: the first offset of
        // the earliest open transaction, which is what `stable` carries.
        //
        // That case used to be answered by walking the legacy `records/` listing
        // for the highest object below the stable bound (#179 deleted that scan,
        // and its final fallback answered 0 for a partition with no objects). The
        // value needs no scan and no approximation: it is the same fold
        // `offset_stage` performs, `stable.get(topition).unwrap_or(high_watermark)`,
        // so the two paths cannot disagree about the LSO.
        //
        // The log end itself comes from `high_watermark` (footer-aware): the
        // previous `last_modified` ordering was wrong under inter-replica clock
        // skew, and `max_base + 1` ignored multi-record batches.
        if *offset_request == ListOffset::Latest {
            let (offset, timestamp) = match stable.get(topition).copied() {
                // An open transaction bounds LATEST. No timestamp: the bound is a
                // transaction boundary, not a record this path has identified.
                Some(last_stable) => (last_stable, None),

                // Tail timestamp from the footer index (the newest segment's max
                // record timestamp) — the segment's record-time, closer to the SQL
                // backends' record `timestamp` than an object mtime ever was.
                None => (
                    self.high_watermark(topition).await?,
                    self.coalesced_latest_timestamp(topition).await?,
                ),
            };

            return Ok(Some(ListOffsetResponse {
                error_code: ErrorCode::None,
                offset: Some(offset),
                timestamp,
            }));
        }

        // Prefix-coalesced (#60): EARLIEST is the oldest segment's base
        // offset for this sub-stream, read from the footer index — no
        // `records/` listing (there is none). LATEST already went through
        // `high_watermark` above (footer-aware).
        if *offset_request == ListOffset::Earliest {
            let earliest = self.coalesced_earliest_offset(topition).await?;

            return Ok(Some(ListOffsetResponse {
                error_code: ErrorCode::None,
                offset: Some(earliest),
                timestamp: None,
            }));
        }

        // Prefix-coalesced TIMESTAMP / `offsetsForTimes` (#105): resolve from
        // the footer index — the earliest segment whose newest record
        // timestamp is at/after the target — instead of the legacy `records/`
        // scan that used to follow, which for a pure-segment topic found nothing
        // and wrongly returned offset 0. This is an in-memory scan of the warm
        // index (no per-segment I/O); `None` (→ -1 on the wire) when no record is
        // at or after the target, matching Kafka's "no offset" semantics.
        if let ListOffset::Timestamp(target) = offset_request {
            let (prefix, substream) = self.routed_substream_of(topition).await?;
            self.refresh_prefix_index(&prefix).await?;
            let target_ms = target
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_millis() as i64)
                .unwrap_or(0);

            let found = self
                .valid_substream_segments(&prefix, &substream, topition.partition())?
                .into_iter()
                .find(|fenced| {
                    fenced.entry.max_timestamp >= 0 && fenced.entry.max_timestamp >= target_ms
                });

            // A truncated segment survives physically (#176), so the located
            // base offset can sit below the truncation floor — clamp, exactly
            // as EARLIEST does. `served_from`, not `base_offset`: a clipped
            // entry's head belongs to the segment before it (#461), whose
            // records are all older than the target.
            let offset = match &found {
                Some(fenced) => Some(fenced.served_from.max(self.truncate_floor(topition).await?)),
                None => None,
            };

            return Ok(Some(ListOffsetResponse {
                error_code: ErrorCode::None,
                offset,
                timestamp: found.as_ref().map(|fenced| {
                    SystemTime::UNIX_EPOCH
                        + Duration::from_millis(fenced.entry.max_timestamp as u64)
                }),
            }));
        }

        // Nothing else to consult (#179). EARLIEST and LATEST returned above from
        // the footer index; a TIMESTAMP that resolved to no segment means no record
        // is at or after the target. The legacy `records/` scan that used to run
        // here — the last read-path listing of that layout, and the reason a
        // pure-segment topic answered offset 0 for a timestamp nobody had — is
        // gone with the layout it read.
        Ok(Some(ListOffsetResponse {
            error_code: ErrorCode::None,
            offset: None,
            ..Default::default()
        }))
    }
}
