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

//! Retention (#61): which segments are old enough to delete, the quarantine
//! (#398) and compaction seams (#399) that survive a delete, and the
//! retired-prefix obligations (#532) a deleted topic leaves behind.

use super::*;

/// The timestamp to report as a prefix's oldest retained segment, or `None` when
/// there is nothing truthful to report (#509).
///
/// Two rejections, and the second is the one that matters:
///
/// - **`None` survivors** — the scan expired everything, so no segment is
///   retained and there is no age to describe.
/// - **A non-positive timestamp** — `OLDEST_RETAINED` is a `u64` gauge, and the
///   value it is fed comes from a chain with two negative sentinels in it: an
///   empty footer's `max_timestamp` fold is `i64::MIN`, and the
///   `last_modified_ms` fallback is only positive for an object a store actually
///   stamped. Casting either to `u64` does not clamp, it *wraps* — `i64::MIN as
///   u64` is 9.2 × 10^18, which would export a prefix as retaining data from the
///   year 292 million and, worse, make `time() - gauge` hugely negative rather
///   than obviously broken. Dropping the sample leaves the series at its last
///   true reading, which is the same trade [`listed_population`] makes.
///
/// Pure, so the wrap is asserted without standing up a metrics reader.
fn retained_timestamp(oldest_ms: Option<i64>) -> Option<u64> {
    oldest_ms.filter(|ms| *ms > 0).map(|ms| ms as u64)
}

/// Whether a prefix might hold a segment older than `threshold_ms`, given the
/// per-prefix oldest-retained `hint` (#61, #49). An unknown hint must scan.
///
/// A comparison against the threshold rather than a sticky flag, which is what
/// makes the fast path survive a *tightened* `retention.ms`: lowering the setting
/// raises the threshold past a hint that had marked the prefix skippable, and the
/// prefix re-arms. A one-way ratchet would silently stop reclaiming instead, with
/// no error and no metric.
///
/// Pure, and taking the hint rather than reading it, so both callers — the gate
/// and the plateau gauge beside it (#544) — work from one lock acquisition.
pub(super) fn maybe_expirable(hint: Option<i64>, threshold_ms: i64) -> bool {
    hint.is_none_or(|oldest| oldest < threshold_ms)
}

impl DynoStore {
    /// The per-prefix oldest-retained hint (#61, the per-prefix analogue of
    /// [`Self::partition_maybe_expirable`]): the greatest record timestamp of the
    /// oldest segment the last scan left behind, or `None` when this process has
    /// never scanned the prefix and nothing can be concluded from it.
    ///
    /// Returns the hint rather than the `might this prefix hold something
    /// expirable?` predicate it used to (#544). The predicate threw the value
    /// away, and the value is the plateau signal — a caller that skips on it has
    /// the one number [`OLDEST_RETAINED`] wants and could not report it.
    pub(super) fn prefix_oldest_retained(&self, prefix: &str) -> Result<Option<i64>> {
        self.prefixes.oldest_retained(prefix)
    }

    /// Update the per-prefix oldest-retained hint after a segment scan (#61).
    pub(super) fn record_prefix_oldest_retained(
        &self,
        prefix: &str,
        oldest_ms: Option<i64>,
    ) -> Result<()> {
        self.prefixes.record_oldest_retained(prefix, oldest_ms)
    }

    /// Delete every segment under `prefix` whose records are all older than
    /// `threshold_ms` (#61). A segment is written atomically, so all its
    /// sub-streams share an append time; it is expirable only when the newest
    /// record across **every** sub-stream (the max footer timestamp, falling back
    /// to the object's append time when record timestamps are unset) is past the
    /// threshold — never dropping a segment while any topic in it is still live.
    /// A segment is also expirable, regardless of age, when every sub-stream
    /// slice in it ends at/below that sub-stream's truncation floor (#176) —
    /// best-effort, from in-process-cached floors only (an unknown floor
    /// defers, never forces, the reclaim). Deletes in bounded `DeleteObjects`
    /// chunks and refreshes the per-prefix skip hint from what survived.
    pub(super) async fn expire_prefix_segments(
        &self,
        prefix: &str,
        threshold_ms: i64,
    ) -> Result<u64> {
        self.refresh_prefix_index(prefix).await?;

        // Decide from the cached footers (no per-segment footer GET). A segment
        // is expirable when its newest record across every sub-stream (max
        // footer timestamp, or the object append time when record timestamps are
        // unset) is past the threshold — so a live topic never loses a shared
        // segment — OR when every sub-stream slice in it ends at/below that
        // sub-stream's truncation floor (#176, fully-truncated reclaim).
        //
        // Snapshot the decision inputs under the index lock, then evaluate
        // the floors OUTSIDE it: `cached_truncate` takes the `watermarks` /
        // `truncate_floors` locks, and nesting those under `prefix_index`
        // would set up a lock-order hazard for no benefit.
        let segments_snapshot: Vec<SegmentExpirySnapshot> = {
            let index = self.prefixes.index()?;

            index
                .get(prefix)
                .map(|entry| {
                    entry
                        .segments
                        .iter()
                        .map(|(seq, cached)| {
                            let newest = cached
                                .footer
                                .entries
                                .iter()
                                .map(|e| e.max_timestamp)
                                .max()
                                .unwrap_or(i64::MIN);
                            let age_ms = if newest > 0 {
                                newest
                            } else {
                                cached.last_modified_ms
                            };
                            let ends = cached
                                .footer
                                .entries
                                .iter()
                                .map(|e| {
                                    (
                                        e.substream(),
                                        e.topic.to_string(),
                                        e.partition,
                                        e.base_offset + e.record_count,
                                    )
                                })
                                .collect();

                            (*seq, age_ms, ends)
                        })
                        .collect()
                })
                .unwrap_or_default()
        };

        let mut aged: BTreeSet<u64> = BTreeSet::new();
        let mut truncated: BTreeSet<u64> = BTreeSet::new();

        for (seq, age_ms, ends) in &segments_snapshot {
            // Fully truncated (#176): every sub-stream slice ends at/below its
            // truncation floor, so the segment holds only logically-deleted
            // records — reclaimable regardless of age. Floors come from
            // in-process caches ONLY (`cached_truncate`: zero object requests
            // on this maintain path — re-reading N per-partition watermark
            // objects here is the request class this design exists to kill).
            // Reclaim is best-effort: an unknown floor makes the slice look
            // live and DEFERS reclaim — the safe direction — until this
            // process warms that partition's watermark (its own cold read, or
            // it served the DeleteRecords itself); age-based retention still
            // bounds the physical debt.
            let mut fully_truncated = !ends.is_empty();
            for (_, topic, partition, end) in ends {
                let tp = Topition::new(topic.as_str(), *partition);
                if self.cached_truncate(&tp)?.is_none_or(|floor| floor < *end) {
                    fully_truncated = false;
                    break;
                }
            }

            if fully_truncated {
                _ = truncated.insert(*seq);
            } else if *age_ms < threshold_ms {
                _ = aged.insert(*seq);
            }
        }

        // Age alone must not expire a segment out of the MIDDLE of a
        // sub-stream's offset space (the 08-31 loss). Retention is keyed on
        // record timestamps, and on a sub-stream whose offset order and
        // timestamp order disagree — a CDC backfill stamping source
        // timestamps, or any prefix #461's zombie era disordered — an all-old
        // segment can sit between two segments that survive. Kafka's
        // `deleteOldSegments` walks the log in offset order and stops at the
        // first segment it keeps; deleting past that point is what put the
        // fleet's 08-24 incident records into 621 fetch-verified mid-log holes
        // exactly seven days later, minute by minute as each segment's newest
        // record crossed the rolling threshold. #290 met the same shape at the
        // tail and certified the destroyed range so a parked fetch fails loud;
        // this stops the destruction itself.
        //
        // So: an aged segment expires only while every surviving segment of
        // each sub-stream it holds sits wholly ABOVE it — the maximal
        // expirable head prefix of the fenced (servable) view. A superseded
        // slice is not in that view and neither blocks nor is blocked: its
        // offsets are served by the superseding segment, which answers for
        // them itself. A retained segment is itself a blocker, so demotion
        // iterates to a fixpoint — one segment kept for a fresh co-tenant
        // shelters everything above it, exactly as it sheltered them before a
        // merge regrouped the slices (the #469-exposed half of the loss).
        //
        // The deliberate cost: mid-log expired records are retained until
        // everything below them expires too — physical debt on a disordered
        // sub-stream, bounded by its disorder horizon, and the price of never
        // deleting a record a fetch below the high watermark can still reach.
        // A fully-truncated segment (#176) is exempt: every offset it holds is
        // below a DeleteRecords floor, so nothing observable sits above it.
        if !aged.is_empty() {
            let substream_keys: BTreeSet<(Substream, i32)> = segments_snapshot
                .iter()
                .flat_map(|(_, _, ends)| {
                    ends.iter()
                        .map(|(substream, _, partition, _)| (substream.clone(), *partition))
                })
                .collect();

            // Each sub-stream's fenced order, resolved ONCE. The view is a
            // function of the cached index, which nothing here mutates — only
            // `aged` changes as the fixpoint runs — so re-deriving it per
            // iteration would re-take the `prefix_index` lock and re-scan every
            // segment of the prefix per sub-stream per pass. On the fleet's
            // worst prefix that is 25 779 segments against a few hundred
            // sub-streams, on the path this whole change exists to make safe.
            let mut orders: Vec<Vec<u64>> = Vec::with_capacity(substream_keys.len());
            for (substream, partition) in &substream_keys {
                orders.push(
                    self.valid_substream_segments(prefix, substream, *partition)?
                        .into_iter()
                        .map(|fenced| fenced.seq)
                        .collect(),
                );
            }

            loop {
                let mut demoted = false;

                for order in &orders {
                    let mut blocked = false;
                    for seq in order {
                        if blocked {
                            demoted |= aged.remove(seq);
                        } else if !aged.contains(seq) && !truncated.contains(seq) {
                            blocked = true;
                        }
                    }
                }

                if !demoted {
                    break;
                }
            }
        }

        let mut expirable: Vec<u64> = Vec::new();
        let mut affected: BTreeSet<(Substream, String, i32)> = BTreeSet::new();
        let mut surviving_oldest_ms: Option<i64> = None;

        for (seq, age_ms, ends) in &segments_snapshot {
            if aged.contains(seq) || truncated.contains(seq) {
                expirable.push(*seq);
                for (substream, topic, partition, _) in ends {
                    _ = affected.insert((substream.clone(), topic.clone(), *partition));
                }
            } else {
                surviving_oldest_ms =
                    Some(surviving_oldest_ms.map_or(*age_ms, |o: i64| o.min(*age_ms)));
            }
        }

        self.record_prefix_oldest_retained(prefix, surviving_oldest_ms)?;

        // The plateau signal (#509). Recorded here rather than inside
        // `record_prefix_oldest_retained`, which is also called with `None` from
        // the DeleteRecords path purely to *invalidate* the hint (#176) — that
        // call says "rescan this prefix", not "nothing is retained", and
        // exporting it would report a prefix as empty while it is full.
        if let Some(oldest_ms) = retained_timestamp(surviving_oldest_ms) {
            OLDEST_RETAINED.record(oldest_ms, &[KeyValue::new("prefix", prefix.to_string())]);
        }

        if expirable.is_empty() {
            EXPIRY_SKIPPED.add(1, &[KeyValue::new("reason", "nothing_expirable")]);
            return Ok(0);
        }

        // Serialize expiry per prefix with the maintenance (compaction) lease
        // (#115). Every replica runs `maintain`, so without a lease all N race
        // the same expiry: each duplicates the per-sub-stream watermark-floor CAS
        // and the seq-floor CAS, and — because the incremental index refresh
        // never observes deletions below its cached max — a replica that did not
        // perform the delete keeps stale entries and re-attempts `DeleteObjects`
        // on already-gone keys. The compaction lease already gates the only other
        // segment-mutating maintenance op, so reusing it makes retention
        // single-writer per prefix AND serializes it against compaction on the
        // same segments. A replica that does not hold the lease yields here; the
        // holder performs the floor writes, deletes, and prunes its own index.
        //
        // A non-holder still carries ghost index entries for the segments the
        // holder deleted (below its refresh watermark) until a cold rebuild —
        // read-side benign, since `fetch` retries off a refreshed index on a 404
        // — but it no longer drives redundant floor writes or deletes here.
        if self.acquire_compaction_lease(prefix).await.is_err() {
            debug!(prefix, "yielding segment expiry to the lease holder");
            EXPIRY_SKIPPED.add(1, &[KeyValue::new("reason", "lease_held_elsewhere")]);
            return Ok(0);
        }

        // Persist a durable offset floor for every sub-stream losing segments
        // BEFORE deleting (#61 review fix): if a drain removes a sub-stream's last
        // segment, cold recovery must not regress to 0 / reuse offsets. The floor
        // is the sub-stream's current tail; recovery folds it in via `max`.
        // Bounded to the affected sub-streams (a window's worth), on the maintain
        // path — not the produce hot path.
        //
        // The same CAS certifies what this expiry leaves servable (#290): the
        // tail of the sub-stream's *surviving* segments, paired with the floor
        // being written. Only this operation knows whether `floor > tail` means
        // "a peer acked offsets you have not listed" (advertise the floor) or
        // "the tail-holding segment is about to be deleted" (a fetch parked in
        // `[surviving tail, floor)` waits for records that can never come) —
        // writing that knowledge down is what lets the fetch path answer
        // `OffsetOutOfRange` instead of empty-forever. `None` survivors certify
        // at the floor itself: an empty log whose end is the floor, which #299
        // already reports as starting where it ends.
        // A slice of an incarnation that is no longer the topic of this name
        // (#442) still has to lose its segments to retention — that is the
        // physical debt this pass exists to reclaim — but it must not write a
        // watermark, because the watermark object is keyed by name and the topic
        // that name resolves to now is a *different* log. Advancing its `high` to
        // a dead incarnation's tail is the "recreated topic does not start at 0"
        // defect arriving by another route.
        //
        // Resolved once per distinct topic rather than once per sub-stream: a
        // deleted topic has no routing pin left, so its lookup is a 404 that
        // nothing memoizes, and `affected` is per *partition*.
        let mut current: BTreeMap<&String, Substream> = BTreeMap::new();
        for (_, topic, _) in &affected {
            if !current.contains_key(topic) {
                _ = current.insert(topic, self.current_substream_of(topic).await?);
            }
        }

        // Whether each affected name still resolves to a topic, memoized and
        // resolved lazily: only a sub-stream that keeps no surviving segment
        // asks, which is rare, and the answer costs a conditional GET.
        let mut live_topics: BTreeMap<&String, bool> = BTreeMap::new();

        let expirable_seqs: BTreeSet<u64> = expirable.iter().copied().collect();
        for (substream, topic, partition) in &affected {
            if current.get(topic) != Some(substream) {
                debug!(
                    ?substream,
                    topic, partition, "skipping the watermark of a retired incarnation"
                );
                continue;
            }

            let segments = self.valid_substream_segments(prefix, substream, *partition)?;
            let tail = segments.last().map(FencedSegment::end);
            let surviving = segments
                .iter()
                .filter(|fenced| !expirable_seqs.contains(&fenced.seq))
                .map(FencedSegment::end)
                .max();

            // The truncation tombstone of a DELETED topic whose last slice is
            // going has nothing left to hide, and keeping it now does harm
            // (#532). `delete_topic` rewrites `watermark.json` as a floor at the
            // deleted log end rather than deleting it, because the slices it
            // hides survive inside shared segments and a same-named successor
            // would otherwise find them by name (#246). Once retention has taken
            // the last of those segments, every record below the floor is
            // physically gone — and a successor, whose `create_topic` clears
            // `high` and folds its base from the segments (none), would start at
            // 0 *below* a floor of N and have its own first N records hidden by
            // it. So the tombstone goes with the records it was hiding, and the
            // name starts clean, exactly as an id-keyed recreation already does.
            //
            // Guarded on all three facts, because each one alone is not enough:
            // the sub-stream keeps no surviving segment, the identity is the
            // current one (checked above), and the topic really is gone —
            // resolved from its own object, not from the topic index, whose
            // bounded staleness would read a peer's 5-second-old create as a
            // deletion and drop a LIVE topic's floor and offset floor with it
            // (#241's offset reuse, arriving through the cleanup).
            //
            // This is also what makes `delete_topic`'s "one small object per
            // partition, kept indefinitely" finite: the tombstones drain with
            // the segments instead of accumulating for the life of the cluster.
            if surviving.is_none() && !self.is_live_topic(topic, &mut live_topics).await? {
                debug!(
                    ?substream,
                    topic, partition, "dropping a fully reclaimed topic's truncation tombstone"
                );

                self.watermark(&Topition::new(topic.clone(), *partition))?
                    .remove(&self.object_store)
                    .await?;
                self.topics.forget(topic);

                continue;
            }

            if let Some(tail) = tail {
                let tp = Topition::new(topic.clone(), *partition);
                _ = self
                    .watermark(&tp)?
                    .with_mut(&self.object_store, |watermark| {
                        let high = watermark.high.unwrap_or(0).max(tail);
                        watermark.high = Some(high);
                        watermark.served = Some(ServedEnd {
                            end: surviving.unwrap_or(high),
                            at_high: high,
                        });
                        Ok(())
                    })
                    .await
                    .inspect_err(|err| debug!(?err, ?tp));
            }
        }

        // Size what is about to go, while the index still names it:
        // `retire_segments` prunes these entries, so after it there is nothing
        // left to measure. Summing `byte_len` from the cached footers keeps
        // [`EXPIRY_BYTES_RECLAIMED`] free of object reads and of a size field on
        // `CachedSegment` — the index is the term #476 is trying to shrink, so
        // retention's own accounting must not grow it — and makes the figure a
        // data-region total, each object's footer excluded.
        //
        // Over `expirable` rather than the whole prefix: a scan that finds nothing
        // to expire is the common case (the mid-log fence, #471, holds old
        // segments back routinely), and the fleet's worst prefix carries 25 779
        // segments. This is O(expired), not O(prefix).
        let expired_bytes: u64 = {
            let index = self.prefixes.index()?;

            index
                .get(prefix)
                .map(|entry| {
                    expirable
                        .iter()
                        .filter_map(|seq| entry.segments.get(seq))
                        .flat_map(|cached| cached.footer.entries.iter())
                        .map(|e| e.byte_len)
                        .sum()
                })
                .unwrap_or_default()
        };

        // Floor before delete, then prune (#77) — see [`Self::retire_segments`].
        let deleted = self.retire_segments(prefix, &expirable).await?;

        // Report what retention removed (#509), after the delete rather than from
        // `expirable`, so a counter never claims a bucket shrank on a delete that
        // did not happen: `retire_segments` propagates a failed `DeleteObjects`
        // chunk, taking this whole function with it, so reaching here means the
        // set went in full and `deleted == expirable.len()`. There is deliberately
        // no partial-success branch — that shape cannot occur, and writing one
        // would only invent a case in which the bytes go uncounted.
        //
        // Bytes were measured above, before the prune took the entries away.
        if deleted > 0 {
            let prefix_label = [KeyValue::new("prefix", prefix.to_string())];

            SEGMENTS_EXPIRED.add(deleted, &prefix_label);
            EXPIRY_BYTES_RECLAIMED.add(expired_bytes, &prefix_label);
        }

        // This process's next-offset hints and cached watermark floors for the
        // affected sub-streams predate the delete: drop them so the next read
        // re-derives from the pruned index and the re-read watermark — which is
        // also what makes the served-end certification written above visible
        // locally without waiting out the hint TTL (#290). Peers converge on
        // their own: the seq-floor raise invalidates their cached watermark
        // floors, so their next read pays the one GET and sees the pair.
        self.topics.forget_hints(
            &affected
                .iter()
                .map(|(_, topic, partition)| Topition::new(topic.clone(), *partition))
                .collect::<Vec<_>>(),
        )?;

        Ok(deleted)
    }

    /// Whether `topic` still resolves to a topic, memoized in `live` for the
    /// caller's run (#532).
    ///
    /// Read from the topic's own object rather than from the topic index: the
    /// index has a bounded staleness by design, and the caller — retention,
    /// deciding whether to drop a truncation tombstone — reads a `false` here as
    /// permission to delete a durable floor. A peer's five-second-old create
    /// answered as a deletion would take a live topic's floor with it, which is
    /// #241's offset reuse arriving through the cleanup.
    pub(super) async fn is_live_topic<'topic>(
        &self,
        topic: &'topic String,
        live: &mut BTreeMap<&'topic String, bool>,
    ) -> Result<bool> {
        if let Some(&known) = live.get(topic) {
            return Ok(known);
        }

        let known = self
            .topic_metadata(&TopicId::Name(topic.clone()))
            .await?
            .is_some();
        _ = live.insert(topic, known);

        Ok(known)
    }

    /// The segments of `prefix` this process has proved undecodable (#398).
    ///
    /// Snapshotted rather than read under the lock by the caller: run selection
    /// walks the prefix's whole segment list, and holding a process-wide lock
    /// across that walk would serialise every prefix being maintained
    /// concurrently. The set is small by construction
    /// ([`Self::PREFIX_QUARANTINE_CAP`]).
    pub(super) fn quarantined_segments_of(&self, prefix: &str) -> Result<BTreeSet<u64>> {
        self.prefixes.quarantined_of(prefix)
    }

    /// Exclude `region`'s segment from this prefix's future compaction runs
    /// (#398). `true` when it was not already excluded — the first sight, and
    /// the only one that logs.
    ///
    /// `false` therefore means "quarantining this changes nothing", which is the
    /// signal the caller needs: the run that just failed was already selected
    /// with this segment excluded, so the failure is somewhere the skip list
    /// cannot reach and retrying the drain would spin.
    pub(super) fn quarantine_segment(&self, region: &CorruptRegion) -> Result<bool> {
        let held = {
            let mut quarantined = self.prefixes.quarantined()?;
            let seqs = quarantined.entry(region.prefix.clone()).or_default();

            if !seqs.contains(&region.seq) && seqs.len() >= Self::PREFIX_QUARANTINE_CAP {
                warn!(
                    prefix = region.prefix,
                    seq = region.seq,
                    cap = Self::PREFIX_QUARANTINE_CAP,
                    "quarantine cap reached; this prefix's drain still ends on this segment"
                );

                return Ok(false);
            }

            if !seqs.insert(region.seq) {
                return Ok(false);
            }

            seqs.len() as u64
        };

        SEGMENT_QUARANTINES.add(1, &[]);
        SEGMENTS_QUARANTINED.record(held, &[KeyValue::new("prefix", region.prefix.clone())]);

        // Once, at `warn` — the whole region detail, so the object is
        // identifiable — where this used to be an `ERROR` on every maintenance
        // tick for as long as the object existed (#398). The steady state is
        // `SEGMENTS_QUARANTINED`, not a log stream.
        warn!(
            ?region,
            "excluding an undecodable segment from this prefix's compaction"
        );

        Ok(true)
    }

    /// Drop quarantine entries naming segments `prefix` no longer holds (#398)
    /// and report what survives.
    ///
    /// The skip list must not outlive the objects in it: retention and the
    /// per-key rewrite retire segments the size merge never selects, and a
    /// sequence is never reused (#77), so an entry whose object is gone is dead
    /// weight — and a gauge that never falls would read as damage that was never
    /// cleared.
    pub(super) fn prune_quarantine(&self, prefix: &str, live: &BTreeSet<u64>) -> Result<()> {
        let held = {
            let mut quarantined = self.prefixes.quarantined()?;

            let Some(seqs) = quarantined.get_mut(prefix) else {
                return Ok(());
            };

            seqs.retain(|seq| live.contains(seq));
            let held = seqs.len() as u64;

            if held == 0 {
                _ = quarantined.remove(prefix);
            }

            held
        };

        SEGMENTS_QUARANTINED.record(held, &[KeyValue::new("prefix", prefix.to_string())]);

        Ok(())
    }

    /// The seams of `prefix` (#399) — see [`PrefixCaches`]. Snapshotted
    /// for the same reason [`Self::quarantined_segments_of`] is.
    pub(super) fn compact_seams_of(&self, prefix: &str) -> Result<BTreeSet<u64>> {
        self.prefixes.seams_of(prefix)
    }

    /// Record `seams` as run boundaries for `prefix` (#399), answering whether
    /// anything **new** was learned.
    ///
    /// `false` is the caller's stop signal, exactly as it is for
    /// [`Self::quarantine_segment`]: the run that just refused was selected with
    /// every one of these seams already honoured (or the cap swallowed them), so
    /// re-selecting would build the same run and refuse it again — retrying
    /// would spin.
    pub(super) fn memo_compact_seams(&self, prefix: &str, seams: &BTreeSet<u64>) -> Result<bool> {
        let mut learned = false;

        {
            let mut memo = self.prefixes.seams()?;
            let held = memo.entry(prefix.to_owned()).or_default();

            for seam in seams {
                if held.contains(seam) {
                    continue;
                }

                if held.len() >= Self::PREFIX_QUARANTINE_CAP {
                    warn!(
                        prefix,
                        seam,
                        cap = Self::PREFIX_QUARANTINE_CAP,
                        "seam cap reached; runs over this prefix may still meet \
                         the tiling refusal"
                    );

                    break;
                }

                _ = held.insert(*seam);
                learned = true;
            }
        }

        Ok(learned)
    }

    /// Drop seams naming segments `prefix` no longer holds (#399), as
    /// [`Self::prune_quarantine`] does for the quarantine and for the same
    /// reason: a sequence is never reused (#77), so a seam whose segment is gone
    /// bounds nothing and is dead weight.
    pub(super) fn prune_compact_seams(&self, prefix: &str, live: &BTreeSet<u64>) -> Result<()> {
        let mut memo = self.prefixes.seams()?;

        let Some(seams) = memo.get_mut(prefix) else {
            return Ok(());
        };

        seams.retain(|seam| live.contains(seam));

        if seams.is_empty() {
            _ = memo.remove(prefix);
        }

        Ok(())
    }

    /// The effective `retention.ms` of a topic's stored config: the configured
    /// value, `i64::MAX` for `-1` (retain forever — the threshold then floors to
    /// the log start, so nothing expires), or Kafka's 7-day default when the
    /// config is absent or unparseable.
    ///
    /// Shared so that the threshold a live topic contributes and the one its
    /// retired-prefix marker keeps after it is deleted (#532) cannot disagree:
    /// a marker that recorded a different number would either delete a
    /// prefix's segments earlier than the topic asked or keep them longer.
    pub(super) fn effective_retention_ms(topic: &CreatableTopic) -> i64 {
        match topic
            .configs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find(|config| config.name == "retention.ms")
            .and_then(|config| config.value.as_deref())
            .and_then(|value| i64::from_str(value).ok())
        {
            Some(ms) if ms < 0 => i64::MAX,
            Some(ms) => ms,
            None => Self::DEFAULT_RETENTION_MS,
        }
    }

    /// The cluster's retired-prefix markers (#532): prefix -> the effective
    /// `retention.ms` its stranded segments keep, in
    /// [`Self::effective_retention_ms`]'s convention.
    ///
    /// One LIST of `retired-prefixes/`, then a GET of only the markers whose
    /// etag this process has not already read — the same etag-delta shape as
    /// [`Self::refresh_topic_index`], and for the same reason: a marker is
    /// rewritten only when another topic on its prefix is deleted, so in steady
    /// state the LIST is the entire cost and both maintenance universes can
    /// afford to read the set every tick.
    ///
    /// Markers a peer has dropped leave the cache with the listing that no
    /// longer names them, so a drained prefix stops being claimed and stops
    /// being counted here without a restart.
    pub(super) async fn retired_prefixes(&self) -> Result<BTreeMap<String, i64>> {
        /// Matches [`Self::refresh_topic_index`]'s fan-out: the cold read of a
        /// fleet that has just deleted a thousand topics is otherwise a
        /// thousand sequential round-trips.
        const FETCH_EACH_CONCURRENCY: usize = 32;

        let listed = self
            .scan_delimited(
                Scan::RetiredPrefix,
                &Path::from(format!(
                    "clusters/{}/retired-prefixes/",
                    self.identity.cluster
                )),
            )
            .await?;

        let mut stale = Vec::new();
        let mut named: BTreeSet<String> = BTreeSet::new();

        {
            let cached = self.prefixes.retired()?;

            for object in &listed.objects {
                let Some(prefix) = object
                    .location
                    .filename()
                    .and_then(|file| file.strip_suffix(".json"))
                else {
                    continue;
                };

                _ = named.insert(prefix.to_owned());

                match cached.get(prefix) {
                    Some((etag, _)) if etag.is_some() && *etag == object.e_tag => {}
                    _ => stale.push((
                        prefix.to_owned(),
                        object.location.clone(),
                        object.e_tag.clone(),
                    )),
                }
            }
        }

        let object_store = &self.object_store;
        let fetched = futures::stream::iter(stale)
            .map(|(prefix, location, etag)| async move {
                let encoded = object_store.get(&location).await?.bytes().await?;
                let marker = serde_json::from_slice::<RetiredPrefix>(&encoded)?;
                Ok::<_, Error>((prefix, (etag, marker)))
            })
            .buffer_unordered(FETCH_EACH_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;

        let markers = {
            let mut cached = self.prefixes.retired()?;

            cached.retain(|prefix, _| named.contains(prefix));
            for (prefix, entry) in fetched {
                _ = cached.insert(prefix, entry);
            }

            cached
                .iter()
                .map(|(prefix, (_, marker))| (prefix.clone(), marker.retention_ms))
                .collect::<BTreeMap<String, i64>>()
        };

        RETIRED_PREFIXES.record(markers.len() as u64, &[]);

        Ok(markers)
    }

    /// Record that `prefix` now carries `topic`'s retention obligation after its
    /// deletion (#532), so retention keeps running on the segments the delete
    /// could not remove.
    ///
    /// A read-modify-write rather than a create: several topics retire onto one
    /// connector prefix, and the marker has to end up holding the longest of
    /// their retentions — a create-only write would pin whichever topic was
    /// deleted first and could then delete a later, longer-lived sibling's
    /// slices early.
    pub(super) async fn retire_prefix(
        &self,
        prefix: &str,
        topic: &str,
        retention_ms: i64,
    ) -> Result<()> {
        let now_ms = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX);

        // Nothing seeds the etag-delta cache from here: the marker is in the
        // listing the moment it is written, so every maintainer — including this
        // one — picks it up on its next refresh, and a hand-seeded entry would be
        // a second source of truth for a value that cache re-reads anyway.
        self.retired_prefix(prefix)
            .with_mut(&self.object_store, |marker| {
                marker.retire(topic, retention_ms, now_ms);
                Ok(())
            })
            .await
            .and(Ok(()))
    }

    /// Drop `prefix`'s retired marker once its segments are all gone (#532).
    /// `Ok(true)` when this call dropped it.
    ///
    /// Judged from the index entry the expiry pass just refreshed, never from
    /// the *absence* of one: a maintainer that has never indexed the prefix
    /// holds no entry at all, and reading that as "empty" would drop the only
    /// threshold that can ever reclaim a prefix still full of segments — the
    /// leak this marker exists to close, re-created by its own cleanup.
    ///
    /// Dropping it early is a leak, never a loss: the marker only ever *adds* a
    /// threshold, and new segments can appear under the prefix only from a live
    /// topic routed there, which supplies its own.
    pub(super) async fn drop_retired_prefix_if_drained(&self, prefix: &str) -> Result<bool> {
        let drained = self
            .prefixes
            .index()?
            .get(prefix)
            .is_some_and(|entry| entry.segments.is_empty());

        if !drained {
            return Ok(false);
        }

        self.retired_prefix(prefix)
            .remove(&self.object_store)
            .await?;

        self.prefixes.forget_retired_marker(prefix);

        debug!(prefix, "dropped a drained prefix's retired marker");

        Ok(true)
    }

    /// Whole-segment retention across all coalesced prefixes (#61). Groups the
    /// topics by connector prefix and expires each prefix's segments under one
    /// **uniform** retention — the longest `retention.ms` among the prefix's
    /// topics, so a per-topic override can never delete a shared segment early
    /// (heterogeneous per-topic retention is not honoured in coalesced mode, per
    /// the epic; the longest wins). Every non-compacted topic counts: Kafka's
    /// default `cleanup.policy` is `delete`, and a topic with no explicit policy
    /// still coalesces into the shared segments, so it must be included in the
    /// max — otherwise a sibling's shorter retention could delete its data (#61
    /// review fix). `retention.ms=-1` (retain forever) makes the whole prefix
    /// infinite. Compacted topics never reach segments (legacy path at produce)
    /// unless segment-routed (#175): a compact-only topic's dedicated prefix
    /// gets no threshold at all, `compact,delete` gets the topic's own.
    ///
    /// A prefix whose last topic has been *deleted* keeps its threshold from
    /// its retired-prefix marker (#532): the topics here are the live ones, and
    /// deriving the whole map from them alone meant a deleted topic's prefix
    /// dropped out of it permanently — no threshold, so
    /// [`Self::expire_prefix_segments`] was never called for it and its
    /// segments could not be reclaimed at any retention setting.
    ///
    /// Restricted to this tick's maintenance claim (#126), and paired with
    /// [`Self::expire_prefix_segments_if_due`] by
    /// [`Self::maintain_prefix_segments`].
    pub(super) async fn segment_retention_thresholds(
        &self,
        now_ms: i64,
        owned: Option<&BTreeSet<String>>,
    ) -> Result<BTreeMap<String, i64>> {
        let mut retention_by_prefix: BTreeMap<String, i64> = BTreeMap::new();
        let mut exempt_prefixes: BTreeSet<String> = BTreeSet::new();
        let mut exempt_topics = 0u64;

        for metadata in self.topics_index().await?.iter() {
            let configs = metadata.topic.configs.as_deref().unwrap_or_default();

            // Parse the policy once: `compact` decides both routing (#175) and
            // whether time-based expiry applies at all.
            let policy = configs
                .iter()
                .find(|config| config.name == "cleanup.policy")
                .and_then(|config| config.value.as_deref())
                .unwrap_or_default();
            let compact = policy.contains("compact");

            // Compact-only topics yield NO threshold: their prefix is never
            // time-expired — the latest value of a key must survive
            // indefinitely, and `expire_prefix_segments` is driven purely by
            // this map. `compact,delete` keeps a threshold from the topic's OWN
            // `retention.ms` (its routed prefix is dedicated, #175, so there is
            // no sibling max to fold): whole-segment expiry on the max footer
            // timestamp deleting old latest-values is exactly Kafka's
            // `compact,delete` semantics. Unrouted compacted topics have no
            // segments — skip, exactly as before.
            //
            // An ABSENT `cleanup.policy` falls through to a threshold, which is
            // Kafka's default (`delete`, 7 days) and is the contract the whole
            // engine follows. The legacy per-partition pass was the odd one out —
            // it read absent as retain-forever until #177 — and is gone (#179).
            if compact && !policy.contains("delete") {
                // Counted, not merely skipped (#509): a compact-only topic's
                // prefix never reaches `expire_prefix_segments` at all, so
                // without this the bytes it holds forever are invisible to every
                // retention metric and read as retention failing to fire.
                //
                // The prefix is also remembered, so that a retired marker on it
                // cannot hand it the threshold this branch is refusing (#532).
                // Reachable: a compacted topic's routed prefix is its own name
                // (#175), so a deleted topic that retired onto that name — a
                // same-named predecessor, or any topic whose connector prefix
                // is that name — leaves a marker sitting on it. One insert per
                // topic, not per partition: every partition of a compacted
                // topic routes to that one dedicated prefix.
                _ = exempt_prefixes.insert(
                    self.routed_prefix(&Topition::new(metadata.topic.name.clone(), 0), true),
                );
                exempt_topics += 1;
                continue;
            }

            // `retention.ms=-1` is retain-forever → treat as effectively infinite
            // (mapped to i64::MAX, so `now - retention` floors to the log start and
            // nothing expires). Absent → the 7-day default.
            //
            // Read through the shared helper, which is also what a retiring
            // topic records in its prefix's marker (#532).
            let retention_ms = Self::effective_retention_ms(&metadata.topic);

            for partition in 0..metadata.topic.num_partitions {
                let prefix = self.routed_prefix(
                    &Topition::new(metadata.topic.name.clone(), partition),
                    compact,
                );

                _ = retention_by_prefix
                    .entry(prefix)
                    .and_modify(|existing| {
                        if retention_ms > *existing {
                            *existing = retention_ms;
                        }
                    })
                    .or_insert(retention_ms);
            }
        }

        RETENTION_EXEMPT_TOPICS.record(exempt_topics, &[]);

        // Then the prefixes whose last topic is gone (#532). A live topic on the
        // prefix wins outright — its threshold is the one that must not lose
        // data, and a retired sibling's marker must never shorten it; the
        // marker only speaks for a prefix nothing live occupies. A compact-only
        // occupant wins the same way, by keeping the exemption above (#175): the
        // latest value of a key must survive indefinitely, and a marker cannot
        // overrule that.
        for (prefix, retention_ms) in self.retired_prefixes().await? {
            if retention_by_prefix.contains_key(&prefix) || exempt_prefixes.contains(&prefix) {
                continue;
            }

            _ = retention_by_prefix.insert(prefix, retention_ms);
        }

        Ok(retention_by_prefix
            .into_iter()
            // Honour this tick's maintenance claim (#126): only expire prefixes
            // this replica owns. `None` = no sharding (every prefix).
            .filter(|(prefix, _)| owned.is_none_or(|owned| owned.contains(prefix)))
            .map(|(prefix, retention_ms)| (prefix, now_ms.saturating_sub(retention_ms)))
            .collect())
    }

    /// Expire `prefix`'s segments older than `threshold_ms`, skipping the work
    /// entirely when the oldest-retained hint proves nothing can be past the
    /// threshold yet (#49) — no LIST, no lease round-trip.
    ///
    /// Both outcomes report [`OLDEST_RETAINED`], so the plateau signal's cadence
    /// is the prefix-tick and not "did this prefix expire something" (#544).
    pub(super) async fn expire_prefix_segments_if_due(
        &self,
        prefix: &str,
        threshold_ms: i64,
    ) -> Result<u64> {
        let hint = self.prefix_oldest_retained(prefix)?;

        if !maybe_expirable(hint, threshold_ms) {
            // The plateau signal on the fast path (#544). It used to live only
            // inside `expire_prefix_segments`, below this gate, which made the
            // gauge silent for exactly the prefixes whose retention window is
            // still filling: the gate returns here *because* the oldest survivor
            // is newer than the threshold, which is the rising phase the gauge
            // exists to show. 99 of 266 non-empty prefixes reported on the
            // production fleet at 1.0.0-alpha.18.
            //
            // Through the same `> 0` filter as the scan path, which is not
            // redundant here: the hint is whatever that scan stored, so a footer
            // fold that produced a sentinel is re-read rather than re-derived.
            if let Some(retained) = retained_timestamp(hint) {
                OLDEST_RETAINED.record(retained, &[KeyValue::new("prefix", prefix.to_string())]);
            }

            EXPIRY_SKIPPED.add(1, &[KeyValue::new("reason", "not_due")]);
            return Ok(0);
        }

        self.expire_prefix_segments(prefix, threshold_ms)
            .await
            .inspect_err(|err| error!(?err, prefix))
    }

    /// Certify the dead offset gap of every sub-stream in `prefix` whose
    /// advertised floor sits above the tail of the segments that are actually
    /// there — the retro-fit #343 could not do (#290).
    ///
    /// #343 made a fetch inside a certified-dead gap answer
    /// `OFFSET_OUT_OF_RANGE` instead of empty-forever, but only the expiry that
    /// performed the delete writes the certification. Every gap that already
    /// existed carries none, and a deployment whose `maintenance_interval` is a
    /// year will never run an expiry that touches one. Those partitions keep
    /// answering empty to a parked consumer, which is #290's original complaint
    /// verbatim.
    ///
    /// ## Why this is sound, and why the read path could not do it
    ///
    /// The read path cannot tell `floor > tail` caused by *retention deleting
    /// the tail-holder* (certify) from the same shape caused by *a peer acking
    /// offsets this process has not listed* (must not certify). That was
    /// established in #290 by implementing the read-side fix and watching
    /// `coalesced_latest_survives_peer_expiry_via_floor_certification` correctly
    /// reject it. Three things separate this pass from that attempt:
    ///
    /// 1. **The compaction lease**, so there is exactly one certifier per prefix
    ///    and it is serialized against expiry and compaction on the same
    ///    segments.
    /// 2. **A forced full listing**, so "not listed" is not a state this pass can
    ///    be in. On a strongly-consistent store the listing sees every completed
    ///    segment PUT, so a peer's segment is either present — and folds into the
    ///    tail, closing the gap — or it does not exist.
    /// 3. **The seq-floor fence.** `leaseless_base` folds the persisted floor
    ///    unconditionally (#287, #316), so a concurrent producer's next segment
    ///    starts at or above the floor — outside the gap being certified. The gap
    ///    cannot be filled after the listing, only appended past.
    ///
    /// The tail comes from [`Self::valid_substream_segments`], which is the same
    /// view the fetch path selects from. That is the point rather than an
    /// implementation detail: the certification says "this range is not
    /// servable", and deriving it from the predicate that decides what *is*
    /// servable is what makes `OFFSET_OUT_OF_RANGE` an honest answer rather than
    /// a guess about the bucket.
    ///
    /// A sub-stream with no segments at all is skipped: that is a drained
    /// partition, not a gap, and #299 already reports it as a log starting where
    /// it ends.
    ///
    /// Writing through the watermark CAS keeps #343's mixed-fleet guard: the pair
    /// is honored only while `at_high == high`, so a floor moved by anything that
    /// did not re-certify invalidates it rather than misleading.
    ///
    /// ## Cost
    ///
    /// Once per prefix per process ([`PrefixCaches`]) — a forced
    /// listing plus one conditional watermark GET per sub-stream, which answers
    /// 304 while the watermark is unchanged. Right as a one-shot after a deploy,
    /// wasteful every tick. A restart re-arms it, which is the correct default:
    /// a fresh process is exactly when a prefix may have picked up a gap under a
    /// binary that did not certify.
    pub(super) async fn certify_prefix_served_ends(&self, prefix: &str) -> Result<u64> {
        if self.prefixes.served_end_reconciled(prefix)? {
            return Ok(0);
        }

        if self.acquire_compaction_lease(prefix).await.is_err() {
            debug!(
                prefix,
                "yielding served-end reconciliation to the lease holder"
            );
            return Ok(0);
        }

        // Marked before the work, not after: a prefix whose reconciliation fails
        // must not retry every tick for the life of the process. The next restart
        // re-arms it, and #338's counter still reports the state meanwhile.
        self.prefixes.mark_served_end_reconciled(prefix)?;

        self.refresh_prefix_index_forced(prefix).await?;

        let substreams: BTreeSet<(Substream, String, i32)> = self
            .prefixes
            .index()?
            .get(prefix)
            .map(|index| {
                index
                    .segments
                    .values()
                    .flat_map(|cached| {
                        cached.footer.entries.iter().map(|entry| {
                            (entry.substream(), entry.topic.to_string(), entry.partition)
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut certified: Vec<Topition> = Vec::new();

        // The watermark object is keyed by name, so a slice left behind by an
        // incarnation this name no longer resolves to must not certify against it
        // (#442) — that would advertise a dead log's end as the live one's.
        // Resolved once per distinct topic; see `expire_prefix_segments`.
        let mut current: BTreeMap<&String, Substream> = BTreeMap::new();
        for (_, topic, _) in &substreams {
            if !current.contains_key(topic) {
                _ = current.insert(topic, self.current_substream_of(topic).await?);
            }
        }

        for (substream, topic, partition) in &substreams {
            if current.get(topic) != Some(substream) {
                continue;
            }

            let Some(tail) = self
                .valid_substream_segments(prefix, substream, *partition)?
                .last()
                .map(FencedSegment::end)
            else {
                continue;
            };

            let tp = Topition::new(topic.clone(), *partition);

            // Read before deciding, for two reasons. `with_mut` starts from the
            // in-process cached version, which on a cold replica — the one that
            // most needs this pass — is empty, so a closure that inspected
            // `watermark.high` there would see 0 and conclude there is no gap.
            // And a prefix that is already correct pays a conditional GET that
            // answers 304, rather than a PUT per sub-stream per restart.
            let Ok((high, served)) = self
                .watermark(&tp)?
                .with(&self.object_store, |watermark| {
                    Ok((watermark.high.unwrap_or(0), watermark.served))
                })
                .await
                .inspect_err(|err| debug!(?err, ?tp))
            else {
                continue;
            };

            if high <= tail
                || served.is_some_and(|served| served.certifies(high) && served.end == tail)
            {
                continue;
            }

            let wrote = self
                .watermark(&tp)?
                .with_mut(&self.object_store, |watermark| {
                    // Re-checked under the CAS: a floor raised between the read
                    // above and this write means a peer is producing, and the
                    // `end` computed from the old listing no longer pairs with
                    // it. Leaving it alone loses nothing — the next process to
                    // run this pass re-derives both together.
                    if watermark.high.unwrap_or(0) != high {
                        return Ok(false);
                    }

                    watermark.served = Some(ServedEnd {
                        end: tail,
                        at_high: high,
                    });

                    Ok(true)
                })
                .await
                .inspect_err(|err| debug!(?err, ?tp))
                .unwrap_or(false);

            if wrote {
                warn!(
                    prefix,
                    topic,
                    partition,
                    end = tail,
                    at_high = high,
                    "certified an offset gap dead: the advertised end sits above every \
                     segment present, so a consumer parked in the gap was reading empty \
                     forever and can now be told (#290)"
                );

                certified.push(tp);
            }
        }

        if certified.is_empty() {
            return Ok(0);
        }

        SERVED_END_CERTIFIED.add(
            certified.len() as u64,
            &[KeyValue::new("prefix", prefix.to_owned())],
        );

        // Both caches, and only the sub-streams actually certified — the same
        // invalidation `expire_prefix_segments` does after its own CAS, and for
        // the same reason. The next-offset hint has to go too, not just the
        // watermark floor: `high_watermark` is answered from the hint, so leaving
        // it would keep the coalesced watermark cache cold, and `certified_dead_gap`
        // reads exclusively from that cache (it pays no object request). The gap
        // would stay unanswerable on this replica until the hint aged out. Peers
        // converge on their own conditional GET.
        self.topics.forget_hints(&certified)?;

        Ok(certified.len() as u64)
    }

    /// Truncate `topition` below `before` by persisting the offset as the
    /// sub-stream's durable truncation floor (`watermark.truncate`, #176).
    /// Nothing is deleted physically: records live in segments, which are
    /// shared across sub-streams and immutable, so they are hidden at read
    /// time by the floor and reclaimed by [`Self::expire_prefix_segments`]
    /// once every sub-stream in a segment is past its floor. (Until #179 this
    /// also removed the per-partition legacy `records/` objects; that layout
    /// is gone.) `before` of `-1` means the log end offset — truncate
    /// everything.
    ///
    /// The floor is **monotonic**: max-folded against any existing floor
    /// inside the watermark CAS (`with_mut` re-applies the closure on
    /// conflict), so a later call with a lower offset cannot regress it — and
    /// the returned log start is the post-fold floor that actually holds, not
    /// the requested `before`.
    pub(super) async fn delete_records_before(
        &self,
        topition: &Topition,
        before: i64,
    ) -> Result<i64> {
        // The log end offset comes from the immutable batch objects (the
        // authority), not the write-behind `watermark` object, which is no
        // longer advanced on the produce hot path (#13).
        let high = self.high_watermark(topition).await?;

        let before = if before < 0 { high } else { before.min(high) };

        // Nothing physical is deleted here any more (#179): the per-partition
        // `records/` objects this used to remove cannot be created, and the
        // segment-resident records are hidden by the floor rather than rewritten,
        // because segments are shared across sub-streams.
        //
        // `truncate` is the whole floor (#180). This used to write `watermark.low`
        // in lockstep with it, for readers that predate the field (#176) and knew
        // only `low` — every one of those is gone, along with the legacy retention
        // that was the field's other writer. An existing object's historic `"low"`
        // is preserved rather than erased: it lands in the `rest` catch-all, so its
        // value survives — relocated after the named fields, which costs such an
        // object one rewrite the next time something else on it moves.
        //
        // The floor is max-folded so a later call with a lower offset cannot
        // regress it.
        let floor = self
            .watermark(topition)?
            .with_mut(&self.object_store, |watermark| {
                let floor = watermark
                    .truncate
                    .map_or(before, |truncate| truncate.max(before));
                watermark.truncate = Some(floor);
                Ok(floor)
            })
            .await?;

        self.memo_truncate_floor(topition, floor)?;

        // Wake the next maintenance pass for this prefix: the whole-segment
        // reclaim gate (`maybe_expirable`) is age-based, so without
        // dropping the hint a prefix whose segments are young but now fully
        // truncated (#176) would not be re-examined until age retention fired
        // anyway. Best-effort like the reclaim itself — a peer maintainer
        // keeps its own hint and converges via its normal age-based rescan.
        let prefix = self.routed_prefix_of(topition).await?;
        self.record_prefix_oldest_retained(&prefix, None)?;

        Ok(floor)
    }
}

#[cfg(test)]
mod retained_timestamp_tests {
    use super::retained_timestamp;

    /// The ordinary case: a surviving segment's greatest record timestamp is what
    /// has to cross `now - retention.ms` for the next expiry, so it is what the
    /// plateau signal reports.
    #[test]
    fn a_surviving_segment_reports_its_timestamp() {
        assert_eq!(
            Some(1_788_363_546_000),
            retained_timestamp(Some(1_788_363_546_000))
        );
    }

    /// A scan that expired everything retains nothing, so there is no age to
    /// describe — as distinct from describing it as zero, which would export the
    /// prefix as holding data from 1970 and read as infinitely stale.
    #[test]
    fn no_survivors_report_nothing() {
        assert_eq!(None, retained_timestamp(None));
    }

    /// The one that matters. `i64::MIN` reaches here from an empty footer's
    /// `max_timestamp` fold, and `as u64` **wraps** rather than clamping: the
    /// gauge would read 9 223 372 036 854 775 808 ms — the year 292 277 026 596 —
    /// and `time() * 1000 - gauge` would go hugely negative instead of visibly
    /// wrong. Rejected, leaving the last true reading in place.
    #[test]
    fn a_sentinel_timestamp_is_rejected_rather_than_wrapped() {
        assert_eq!(None, retained_timestamp(Some(i64::MIN)));
        assert_eq!(9_223_372_036_854_775_808, i64::MIN as u64);
    }

    /// The `last_modified_ms` fallback is only meaningful for an object a store
    /// stamped. Zero and negative are both "unstamped", and neither is a time.
    #[test]
    fn an_unstamped_object_is_rejected() {
        assert_eq!(None, retained_timestamp(Some(0)));
        assert_eq!(None, retained_timestamp(Some(-1)));
    }
}
