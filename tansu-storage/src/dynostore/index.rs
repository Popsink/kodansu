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

//! The per-prefix segment index: the in-memory [`PrefixIndex`] that answers
//! reads without listing, how it is refreshed, and the ranged tail probe
//! (#112) that follows a prefix without re-listing it.

use super::*;

/// The prefix population a completed listing is entitled to report, or `None`
/// when it saw too little of the prefix to have one (#399).
///
/// The whole subtlety is `whole_prefix == false`. An incremental refresh lists
/// from a `start_after` cursor, so it returns the handful of segments above the
/// index's watermark — a number that looks like a healthy prefix whatever the
/// prefix holds, and one that would overwrite a truthful reading taken minutes
/// earlier. Reporting nothing leaves the gauge at the last whole listing's
/// answer, which is stale but true, and `PREFIX_INDEX_RECONCILE_INTERVAL` bounds
/// how stale.
///
/// Pure, so the distinction that matters can be asserted without standing up a
/// metrics reader.
fn listed_population(discovered: usize, whole_prefix: bool) -> Option<u64> {
    whole_prefix.then_some(discovered as u64)
}

impl PrefixIndex {
    /// The highest sequence this process has *resolved* by a listing — decoded
    /// into `segments` or recorded as [`Self::opaque`]. Both the leaseless
    /// candidate derivation and the incremental-listing cursor use this, so an
    /// undecodable object neither wedges the arbiter (#157) nor is re-GET on
    /// every refresh. Committed in ascending order with the footers, so it stays
    /// a contiguous watermark even after a partial (cancelled) build (#105).
    pub(super) fn resolved_max(&self) -> Option<u64> {
        self.segments
            .keys()
            .next_back()
            .copied()
            .max(self.opaque.iter().next_back().copied())
    }

    /// Add (or replace) `seq`'s cached footer, keeping [`Self::by_substream`] in
    /// step (#492).
    ///
    /// A replace is real: the tail probe, the incremental listing and the
    /// winner-fold can all resolve the same sequence, and the footer they decode
    /// is identical (segments are immutable), but the entry *indices* are only
    /// guaranteed to match because it is the same object — so the old
    /// contribution is withdrawn first rather than assumed equal.
    ///
    /// The single chokepoint into `segments`, which is why the resident footer
    /// is pruned here (#543): every path that caches a footer — the writer's own
    /// flush, the incremental listing, the tail probe, the compaction insert —
    /// arrives through this call, so nothing can enter the index holding
    /// coordinates the fold would never read. Entry *count* and order are
    /// untouched, so the `(seq, position)` pairs below still address
    /// `footer.entries` directly.
    pub(super) fn insert_segment(&mut self, seq: u64, mut cached: CachedSegment) {
        self.unindex_segment(seq);

        for entry in &mut cached.footer.entries {
            entry.retain_foldable_producers();
            entry.topic = self.intern_topic(&entry.topic);
        }

        for (position, entry) in cached.footer.entries.iter().enumerate() {
            // A footer wide enough to overflow `u32` cannot be encoded — the
            // entry count is a `u32` on the wire — so the cast cannot lose.
            let at = (seq, position as u32);
            let seqs = self
                .by_substream
                .entry((entry.substream(), entry.partition))
                .or_default();

            // Ascending by sequence, which is how `valid_substream_segments`
            // wants them and how a refresh supplies them: the common case is a
            // push at the end, and an out-of-order fold binary-searches.
            match seqs.binary_search(&at) {
                Ok(_) => {}
                Err(position) => seqs.insert(position, at),
            }
        }

        _ = self.segments.insert(seq, cached);
    }

    /// Keep exactly the segments `keep` admits, keeping [`Self::by_substream`] in
    /// step. Returns how many were dropped.
    ///
    /// One compacting pass over the derived map rather than a withdrawal per
    /// dropped segment. Both bulk callers — #408's reconciling pass and
    /// `retire_segments`' prune — drop thousands at a time, and removing from
    /// the middle of a sub-stream's sequence list shifts its tail, so per-segment
    /// withdrawal is quadratic in the sub-stream's segment count. This is linear
    /// in the prefix, once, under a lock the request path also wants.
    pub(super) fn retain_segments(&mut self, keep: impl Fn(u64) -> bool) -> usize {
        let before = self.segments.len();
        self.segments.retain(|seq, _| keep(*seq));
        let dropped = before - self.segments.len();

        if dropped > 0 {
            self.by_substream.retain(|_, seqs| {
                seqs.retain(|(seq, _)| keep(*seq));
                !seqs.is_empty()
            });

            self.forget_unused_topic_names();
        }

        dropped
    }

    /// The one [`Arc<str>`] this index uses for `topic`, adopting `topic`'s own
    /// allocation the first time the name is seen (#476 item 2b).
    ///
    /// Called from [`Self::insert_segment`] and nowhere else. That is the
    /// property worth keeping: interning is only sound while every sharer is
    /// reachable from `segments`, because that is what
    /// [`Self::forget_unused_topic_names`] counts.
    pub(super) fn intern_topic(&mut self, topic: &Arc<str>) -> Arc<str> {
        if let Some(shared) = self.topic_names.get(topic) {
            return shared.clone();
        }

        _ = self.topic_names.insert(topic.clone());
        topic.clone()
    }

    /// Drop the interned names no cached footer names any more.
    ///
    /// The test is `Arc::strong_count == 1`: this set is the only other holder,
    /// so a count of one means every entry that named it has been dropped. A
    /// clone that escaped the index's lock inflates the count and keeps its name
    /// one sweep longer, which costs one name and converges — the failure this
    /// rules out is the opposite one, dropping a name an entry still points at,
    /// which cannot happen because that entry *is* a strong reference.
    ///
    /// Run from [`Self::retain_segments`], which is the only path that drops a
    /// cached footer, and only when it dropped one: it is O(topics in the
    /// prefix) and a prefix's topic count is three orders of magnitude below its
    /// entry count, so paying it per retained segment would cost more than the
    /// names are worth.
    pub(super) fn forget_unused_topic_names(&mut self) {
        self.topic_names.retain(|name| Arc::strong_count(name) > 1);
    }

    /// Withdraw `seq`'s contribution to [`Self::by_substream`]. Reads the footer
    /// still in `segments`, so it must run *before* the map is written.
    pub(super) fn unindex_segment(&mut self, seq: u64) {
        let Some(cached) = self.segments.get(&seq) else {
            return;
        };

        for (position, entry) in cached.footer.entries.iter().enumerate() {
            let key = (entry.substream(), entry.partition);
            let Some(seqs) = self.by_substream.get_mut(&key) else {
                continue;
            };

            if let Ok(at) = seqs.binary_search(&(seq, position as u32)) {
                _ = seqs.remove(at);
            }

            // A sub-stream with no segments left keeps no key: the map is sized
            // by the cluster's live partitions, not by everything it has ever
            // held.
            if seqs.is_empty() {
                _ = self.by_substream.remove(&key);
            }
        }
    }

    /// Every `(sequence, writer epoch, entry)` this prefix holds for one
    /// sub-stream, ascending by sequence — without touching a segment that does
    /// not hold it (#492).
    pub(super) fn substream_entries(
        &self,
        substream: &Substream,
        partition: i32,
    ) -> impl Iterator<Item = (u64, i64, &SubstreamEntry)> {
        self.by_substream
            .get(&(substream.clone(), partition))
            .into_iter()
            .flatten()
            .filter_map(move |(seq, position)| {
                let cached = self.segments.get(seq)?;
                let entry = cached.footer.entries.get(*position as usize)?;
                Some((*seq, cached.footer.writer_epoch, entry))
            })
    }
}

impl DynoStore {
    /// Refresh the in-memory [`PrefixIndex`] for `prefix` (read-path #60 review
    /// fix). Skips the listing while the cache is fresh (within
    /// [`Self::HIGH_WATERMARK_HINT_TTL`]); otherwise lists **incrementally**
    /// (`start_after` the highest known sequence) and reads only the footers of
    /// *new* segments, so steady-state cost is O(new), not O(total segments).
    /// Cheap for the writer (its own flushes already populated the cache).
    ///
    /// Once per [`Self::PREFIX_INDEX_RECONCILE_INTERVAL`] the listing is the
    /// whole prefix instead, and drops the entries it does not find (#408) — the
    /// only thing that removes an entry for a segment a *peer* retired.
    pub(super) async fn refresh_prefix_index(&self, prefix: &str) -> Result<()> {
        self.refresh_prefix_index_inner(prefix, false).await
    }

    /// Refresh the prefix index unconditionally, bypassing the TTL freshness
    /// gate (#86). The leaseless write path (`fold-before-claim`) must observe
    /// every live segment — including a peer replica's seconds-old write — before
    /// it derives offsets and claims a sequence, or two writers could stamp the
    /// same offset. The TTL'd [`Self::refresh_prefix_index`] is fine for the read
    /// path (bounded staleness), but not for the offset-assignment authority.
    pub(super) async fn refresh_prefix_index_forced(&self, prefix: &str) -> Result<()> {
        self.refresh_prefix_index_inner(prefix, true).await
    }

    /// Fold exactly one segment's footer into the prefix index, answering
    /// whether it landed (#401).
    ///
    /// This is the fold for a writer that already has *proof* the segment
    /// exists — a create that came back `AlreadyExists` — and so needs neither
    /// the tail probe's absence chain nor the always-fresh seq-floor read that
    /// makes the absence a proof ([`Self::probe_prefix_tail`]). One ranged
    /// footer GET, and `folded_max` advances by one.
    ///
    /// `false` means the caller should fall back to the full refresh: an
    /// unreadable or undecodable footer at a sequence a create says is occupied
    /// is exactly the `stalled` case the loop's own diagnostics exist for, and
    /// the LIST path is where that is resolved. So this can only be faster than
    /// a refresh, never a substitute for one that answers differently.
    ///
    /// Only `refreshed_at` is stamped, as the probe does: folding can add a
    /// segment but never reflect a peer's deletion, so the index generation —
    /// and with it the certified seq floor keeping the LATEST path off
    /// `watermark.json` — stays valid.
    pub(super) async fn fold_segment_footer(&self, prefix: &str, seq: u64) -> bool {
        let location = self.segment_location(prefix, seq);

        let result = match self
            .object_store
            .get_opts(
                &location,
                GetOptions {
                    range: Some(GetRange::Suffix(
                        SEGMENT_FOOTER_OVER_READ.max(SEGMENT_TRAILER_LEN) as u64,
                    )),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                if matches!(error, object_store::Error::NotFound { .. }) {
                    SEGMENT_ABSENT.add(1, &[KeyValue::new("caller", "fold")]);
                }

                debug!(?error, prefix, seq, "folding the winner's footer");
                return false;
            }
        };

        let last_modified_ms = result.meta.last_modified.timestamp_millis();

        let Ok(bytes) = result.bytes().await.inspect_err(|error| {
            debug!(?error, prefix, seq, "folding the winner's footer body");
        }) else {
            return false;
        };

        let Ok(Some(footer)) = Self::decode_segment_footer(&bytes).inspect_err(|error| {
            debug!(?error, prefix, seq, "decoding the winner's footer");
        }) else {
            return false;
        };

        let Some(mut index) = self.prefixes.try_index() else {
            return false;
        };

        let entry = index.entry(prefix.to_owned()).or_default();
        entry.insert_segment(
            seq,
            CachedSegment {
                footer,
                last_modified_ms,
            },
        );
        entry.refreshed_at = Some(SystemTime::now());

        true
    }

    /// What this process knows about `prefix`'s cached index before it decides
    /// what to list: whether it is within its freshness TTL, the
    /// incremental-listing watermark, and whether the downward pass is due.
    pub(super) fn prefix_index_freshness(&self, prefix: &str) -> Result<IndexFreshness> {
        let index = self.prefixes.index()?;
        Ok(match index.get(prefix) {
            Some(entry) => {
                let now = SystemTime::now();

                let fresh = entry.refreshed_at.is_some_and(|at| {
                    now.duration_since(at)
                        .is_ok_and(|elapsed| elapsed < self.tuning.watermark_hint_ttl)
                });

                // Due only for an index that holds something: a cold one has
                // nothing to drop, and its listing is already the whole prefix.
                let reconcile_due = (!entry.segments.is_empty() || !entry.opaque.is_empty())
                    && !entry.reconciled_at.is_some_and(|at| {
                        now.duration_since(at)
                            .is_ok_and(|elapsed| elapsed < Self::PREFIX_INDEX_RECONCILE_INTERVAL)
                    });

                IndexFreshness {
                    fresh,
                    cursor: entry.resolved_max(),
                    reconcile_due,
                }
            }
            None => IndexFreshness::default(),
        })
    }

    /// The per-prefix single-flight lock for the real index refresh and the
    /// certified seq-floor sync (see [`PrefixLocks`]).
    pub(super) fn prefix_read_sync_lock(
        &self,
        prefix: &str,
    ) -> Result<Arc<tokio::sync::Mutex<()>>> {
        self.prefix_locks.read_sync(prefix)
    }

    /// Follow a prefix's segment tail with ranged GETs instead of a
    /// `ListObjectsV2` (#112), proving there is nothing left to discover rather
    /// than guessing.
    ///
    /// **The proof.** A segment is only ever created at
    /// `max(known tail + 1, seq floor)` — by the leaseless arbiter
    /// ([`Self::tail_next_seq_folded`]), by the lease-mode writer and by compaction
    /// ([`Self::tail_next_seq`]). So created names are contiguous except where the
    /// durable floor jumps (#77, raised write-ahead of every delete) or where a
    /// name is occupied but unresolvable. Therefore, if `segments/{cursor + 1}` is
    /// **absent** and the floor read *after* observing that absence is
    /// `<= cursor + 1`, no segment can exist above `cursor`:
    ///
    /// - a segment at `S > cursor + 1` would have been created either at
    ///   `tail + 1` — which forces a segment at every seq down to `cursor + 1`,
    ///   contradicting the absence — or at a floor `>= S > cursor + 1`, which
    ///   (the floor being monotonic) our later read could not have seen as
    ///   `<= cursor + 1`;
    /// - an occupied-but-unresolvable name would answer the probe with an object,
    ///   not a 404.
    ///
    /// Object stores are read-after-write consistent, so the 404 is authoritative
    /// at that instant. Reading the floor *after* the absence is what makes the
    /// argument work, hence the ordering below. A segment created *after* both
    /// reads is not missed by this any more than by a LIST — that staleness window
    /// is the same one the TTL and the create-CAS already cover.
    ///
    /// Anything the proof does not cover returns [`TailProbe::Inconclusive`] and
    /// the caller LISTs: a cold index (no cursor), a floor ahead of the tail (which
    /// is exactly the "segments above our cursor were deleted" case), an
    /// unresolvable footer, an oversized footer, more new segments than the probe
    /// window, or any non-404 error.
    pub(super) async fn probe_prefix_tail(
        &self,
        prefix: &str,
        cursor: u64,
        path: &'static str,
    ) -> TailProbe {
        /// Consecutive new segments the probe will fold before deferring to a
        /// LIST. A reader this far behind is better served by one tier-1 request
        /// than by a growing chain of tier-2 ones.
        const PROBE_WINDOW: u64 = 4;

        let mut folded = 0;

        for seq in (cursor + 1)..=(cursor + PROBE_WINDOW) {
            let location = self.segment_location(prefix, seq);

            let (bytes, last_modified_ms) = match self
                .object_store
                .get_opts(
                    &location,
                    GetOptions {
                        range: Some(GetRange::Suffix(
                            SEGMENT_FOOTER_OVER_READ.max(SEGMENT_TRAILER_LEN) as u64,
                        )),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(result) => {
                    let last_modified_ms = result.meta.last_modified.timestamp_millis();
                    match result.bytes().await {
                        Ok(bytes) => (bytes, last_modified_ms),
                        Err(error) => {
                            debug!(?error, prefix, seq, "tail probe body");
                            return TailProbe::Inconclusive("probe_error");
                        }
                    }
                }

                // Absent: the tail is at `cursor` if the floor agrees. The floor is
                // read *after* the absence, and fresh — see the proof above and
                // [`Self::probe_seq_floor`].
                Err(object_store::Error::NotFound { .. }) => {
                    // Absence is what this read is *for* (#408): the probe asks
                    // whether `cursor + 1` exists and a 404 is the affirmative
                    // answer. Counted so the `segment` 404 plane can be split
                    // from the stale-index population it was conflated with.
                    SEGMENT_ABSENT.add(1, &[KeyValue::new("caller", "tail_probe")]);

                    let floor = match self.probe_seq_floor(prefix).await {
                        Ok(floor) => floor,
                        Err(error) => {
                            debug!(?error, prefix, seq, "tail probe floor");
                            return TailProbe::Inconclusive("probe_error");
                        }
                    };

                    if floor > seq {
                        // Names above our cursor were freed by retention or
                        // compaction, so absence proves nothing — LIST.
                        return TailProbe::Inconclusive("floor_ahead");
                    }

                    let outcome = if folded == 0 {
                        "up_to_date"
                    } else {
                        "extended"
                    };
                    PREFIX_TAIL_PROBES.add(
                        1,
                        &[
                            KeyValue::new("path", path),
                            KeyValue::new("outcome", outcome),
                        ],
                    );

                    // Only `refreshed_at` is stamped: unlike a listing, a probe can
                    // only *add* segments, never reflect a peer's deletion, so the
                    // index generation — and with it the certified seq floor that
                    // keeps the LATEST fast path off `watermark.json` — stays valid.
                    if let Some(mut index) = self.prefixes.try_index() {
                        index.entry(prefix.to_owned()).or_default().refreshed_at =
                            Some(SystemTime::now());
                    }

                    return TailProbe::Resolved;
                }

                Err(error) => {
                    debug!(?error, prefix, seq, "tail probe");
                    return TailProbe::Inconclusive("probe_error");
                }
            };

            // The over-read carries the footer for all but a pathologically wide
            // prefix; anything else is the LIST path's business.
            match Self::decode_segment_footer(&bytes) {
                Ok(Some(footer)) => {
                    if let Some(mut index) = self.prefixes.try_index() {
                        index.entry(prefix.to_owned()).or_default().insert_segment(
                            seq,
                            CachedSegment {
                                footer,
                                last_modified_ms,
                            },
                        );
                    }
                    folded += 1;
                }

                Ok(None) => return TailProbe::Inconclusive("undecodable"),
                Err(error) => {
                    debug!(?error, prefix, seq, "tail probe footer");
                    return TailProbe::Inconclusive("oversized_footer");
                }
            }
        }

        TailProbe::Inconclusive("window_exhausted")
    }

    pub(super) async fn refresh_prefix_index_inner(&self, prefix: &str, force: bool) -> Result<()> {
        // Lock-free fast path: a fresh index whose downward pass is not due is
        // served without touching the single-flight lock, so TTL-served readers
        // never contend.
        if !force {
            let freshness = self.prefix_index_freshness(prefix)?;
            if freshness.fresh && !freshness.reconcile_due {
                return Ok(());
            }
        }

        // Single-flight the real listing per prefix: concurrent stale readers
        // (a wide ListOffsets resolves 32 partitions at once) queue here and
        // re-check, so one LIST serves them all instead of N duplicates. The
        // reconciling pass queues in the same place and stamps the same clock, so
        // a prefix that comes due under a fetch storm lists once, not once per
        // waiter.
        let sync = self.prefix_read_sync_lock(prefix)?;
        let _guard = sync.lock().await;

        let freshness = self.prefix_index_freshness(prefix)?;
        if !force && freshness.fresh && !freshness.reconcile_due {
            return Ok(());
        }

        let start_after = freshness.cursor;

        // The downward pass (#408). Everything below is the ordinary refresh
        // except that the listing is the whole prefix and what it does not return
        // is dropped — one clock per prefix, so this is at most one full listing
        // per [`Self::PREFIX_INDEX_RECONCILE_INTERVAL`] however many refreshes
        // (read, forced, or a 404's restart) ask for one in the window.
        let reconcile = freshness.reconcile_due;

        // A cold build lists the whole prefix too, and so observes the same live
        // set the pass would: it stamps the clock (below) without pruning
        // anything, which costs a just-started process one listing rather than
        // two a TTL apart.
        let whole_prefix = reconcile || start_after.is_none();

        let path = if force { "forced" } else { "ttl" };

        // Try to follow the tail with ranged GETs instead of a LIST (#112): a
        // `ListObjectsV2` is a tier-1 request, ~12x the price of the tier-2 GET
        // that both proves the tail *and* returns the new segment's footer. Falls
        // through to the LIST whenever the proof does not hold.
        //
        // The probe is skipped outright when the pass is due: it can only ever
        // prove the *tail* has not moved, which is precisely the answer a stale
        // head keeps giving — `Resolved` is why a hot prefix reconciled 0.00014
        // entries/s while its index grew without bound.
        if reconcile {
            PREFIX_INDEX_LISTS.add(
                1,
                &[
                    KeyValue::new("path", path),
                    KeyValue::new("reason", "reconcile"),
                ],
            );
        } else if let Some(cursor) = start_after {
            match self.probe_prefix_tail(prefix, cursor, path).await {
                // The tail is proven (and anything new is folded): the index is
                // current, with no LIST issued.
                TailProbe::Resolved => return Ok(()),

                // The proof does not hold — fall through to the authoritative LIST.
                TailProbe::Inconclusive(reason) => {
                    PREFIX_INDEX_LISTS.add(
                        1,
                        &[KeyValue::new("path", path), KeyValue::new("reason", reason)],
                    );
                }
            }
        } else {
            PREFIX_INDEX_LISTS.add(
                1,
                &[KeyValue::new("path", path), KeyValue::new("reason", "cold")],
            );
        }

        let listing = self.segment_prefix(prefix);
        let mut stream = match start_after.filter(|_| !whole_prefix) {
            Some(seq) => self.scan_from(
                Scan::SegmentIndex,
                &listing,
                &self.segment_location(prefix, seq),
            ),
            None => self.scan(Scan::SegmentIndex, &listing),
        };

        let mut discovered: Vec<(u64, Path, i64)> = Vec::new();
        while let Some(meta) = stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, prefix))?
        {
            let Some(seq) = Self::segment_seq_of(&meta.location) else {
                continue;
            };
            discovered.push((seq, meta.location, meta.last_modified.timestamp_millis()));
        }

        // What is really under the prefix, from the store's own answer (#399).
        //
        // Only a whole-prefix listing may say so — see [`listed_population`].
        // Costs nothing, the listing was walked anyway, and it is the count the
        // index-derived gauge could never be: an index holds what a replica has
        // seen, not what a bucket holds.
        if let Some(population) = listed_population(discovered.len(), whole_prefix) {
            SEGMENTS_LIVE.record(population, &[KeyValue::new("prefix", prefix.to_string())]);
        }

        // The whole live set, kept only for the downward pass — a listing that
        // completed, so what it did not return below its own maximum is retired
        // (#408). Collected before the footers are consumed; an incremental
        // listing has no business pruning anything and builds nothing.
        let live: BTreeSet<u64> = if reconcile {
            discovered.iter().map(|(seq, _, _)| *seq).collect()
        } else {
            BTreeSet::new()
        };

        // Fetch not-yet-cached footers CONCURRENTLY and commit each to the index
        // as it arrives (#105). Two properties matter at scale — with compaction
        // disabled a prefix accrues thousands of segments:
        //
        // - **Concurrency.** A sequential footer-per-segment loop is O(#segments)
        //   round-trips; a cold `list_offsets` (LATEST via `high_watermark`,
        //   EARLIEST via `segment_region_start`) then blocked past the 60s client
        //   timeout, stalling the whole read path. `buffered` keeps up to
        //   `FOOTER_FETCH_CONCURRENCY` ranged GETs in flight (as the topic-index
        //   warm does), cutting wall-time ~N×.
        // - **Incremental, in-order commit.** Committing per footer rather than
        //   once at the end means a request the client abandons at its timeout
        //   (dropping this future) still leaves progress cached — so the index
        //   warms across attempts instead of restarting from zero (the
        //   "sustained, not decaying" stall). Ordered `buffered` preserves the
        //   ascending-sequence LIST order, so the committed set stays a
        //   contiguous prefix and the next refresh's `start_after` watermark is
        //   correct even after a partial build.
        const FOOTER_FETCH_CONCURRENCY: usize = 32;

        // Already resolved: decoded segments *and* the undecodable names (#157) —
        // re-GETting a footer that will not decode again costs a request per
        // refresh (per flush, on the forced path) and never makes progress.
        let cached: BTreeSet<u64> = self
            .prefixes
            .index()?
            .get(prefix)
            .map(|entry| {
                entry
                    .segments
                    .keys()
                    .copied()
                    .chain(entry.opaque.iter().copied())
                    .collect()
            })
            .unwrap_or_default();

        let mut footers = futures::stream::iter(
            discovered
                .into_iter()
                .filter(|(seq, _, _)| !cached.contains(seq)),
        )
        .map(|(seq, location, last_modified_ms)| async move {
            match self.read_segment_footer(&location).await {
                Ok(Some(footer)) => Ok((seq, last_modified_ms, FooterOutcome::Decoded(footer))),
                Ok(None) => Ok((seq, last_modified_ms, FooterOutcome::Undecodable)),
                // Gone between the LIST that discovered it and this GET:
                // concurrent compaction (#66) merged it away, or retention (#61)
                // reclaimed it. Not an integrity fault and not this reader's
                // problem — before #191 the `?` below turned it into a failed
                // index refresh and a raw `ObjectStore(NotFound)` escaping
                // `ListOffsets` all the way to the connection error path.
                Err(Error::ObjectStore(ref inner))
                    if matches!(**inner, object_store::Error::NotFound { .. }) =>
                {
                    Ok((seq, last_modified_ms, FooterOutcome::Vanished))
                }
                Err(error) => Err(error),
            }
        })
        .buffered(FOOTER_FETCH_CONCURRENCY);

        while let Some(result) = footers.next().await {
            let (seq, last_modified_ms, footer) = result?;
            let mut index = self.prefixes.index()?;
            let entry = index.entry(prefix.to_owned()).or_default();
            match footer {
                FooterOutcome::Decoded(footer) => {
                    entry.insert_segment(
                        seq,
                        CachedSegment {
                            footer,
                            last_modified_ms,
                        },
                    );
                }

                // The object holds a sequence but carries no decodable footer, so
                // it can never join the readable set — record the *name* as
                // resolved (#157) so the leaseless arbiter steps over it instead
                // of re-deriving an occupied candidate until its budget is gone,
                // and so the next refresh does not re-GET this footer.
                FooterOutcome::Undecodable => {
                    if entry.opaque.insert(seq) {
                        SEGMENT_FOOTER_UNDECODABLE.add(1, &[]);
                        warn!(
                            prefix,
                            seq,
                            "segment object has no decodable footer; stepping over the sequence"
                        );
                    }
                }

                // Deleted under us (#191). Same bookkeeping as an undecodable
                // name — resolved, unreadable, stepped over — but logged
                // separately: an operator seeing "no decodable footer" for an
                // object that maintenance simply reclaimed would go looking for
                // corruption that is not there. Sequences are never reused
                // (#77), so caching it as resolved is permanent and correct.
                FooterOutcome::Vanished => {
                    if entry.opaque.insert(seq) {
                        SEGMENT_VANISHED_BEFORE_READ.add(1, &[]);
                        SEGMENT_ABSENT.add(1, &[KeyValue::new("caller", "refresh")]);
                        debug!(
                            prefix,
                            seq, "segment deleted before its footer could be read; stepping over"
                        );
                    }
                }
            }
        }

        // Whole live set observed: stamp fresh so the TTL fast-path can serve.
        // The listing may reflect another replica's segment deletions (which an
        // incremental list can never re-observe), so bump the generation: the
        // certified seq floor must be re-read at least as recently as this
        // listing before the LATEST fast path may trust the index again.
        {
            let mut index = self.prefixes.index()?;
            let entry = index.entry(prefix.to_owned()).or_default();
            entry.refreshed_at = Some(SystemTime::now());
            entry.generation += 1;

            // The downward half (#408). **Only entries below the listing's own
            // maximum are dropped.** A segment created while the listing was
            // being walked may not appear in it, and pruning that would take
            // records out of this replica's view of the tail — where an
            // incremental refresh would never put them back. Below the maximum
            // the listing is authoritative: sequences are never reused (#77), so
            // an absent name is a retired one.
            //
            // Deliberately *not* derived from the seq floor. `retire_segments`
            // raises the floor to `max(retired) + 1`, so a batch retiring a
            // non-contiguous set leaves live segments below it; a floor read as a
            // liveness boundary would hide live records from readers, which is
            // worse than anything it saves. A listing is the only evidence taken
            // here.
            //
            // `opaque` is reconciled on the same terms. A name only enters it
            // because its object could not be decoded or had already vanished
            // (#157/#191), and it is held for ever so the leaseless arbiter steps
            // over an *occupied* name. A name the listing does not return is not
            // occupied, and it cannot be handed back out either: every delete
            // goes through `retire_segments`, which raises the floor past the
            // sequence write-ahead of removing it, and every candidate is
            // `max(tail + 1, floor)`.
            if reconcile && let Some(listed_max) = live.iter().next_back().copied() {
                let before = entry.opaque.len();

                let dropped_segments =
                    entry.retain_segments(|seq| seq >= listed_max || live.contains(&seq));
                entry
                    .opaque
                    .retain(|seq| *seq >= listed_max || live.contains(seq));

                let dropped = (dropped_segments + before.saturating_sub(entry.opaque.len())) as u64;

                if dropped > 0 {
                    PREFIX_INDEX_RECONCILED
                        .add(dropped, &[KeyValue::new("prefix", prefix.to_string())]);
                    debug!(prefix, dropped, listed_max, "reconciled the prefix index");
                }
            }

            // Stamped for the cold build as well as the pass, and stamped even
            // when the listing came back empty: an empty listing is not evidence
            // — it is also what a prefix whose objects are all above a raced page
            // boundary looks like — so it drops nothing, but a fetch storm must
            // not re-list on the strength of it either.
            if whole_prefix {
                entry.reconciled_at = Some(SystemTime::now());
            }

            // Size of the cached footers, process-wide (#196). The broker's
            // working set grows ~600 MiB fresh to ~1.4 GiB over 26h on an
            // unchanged workload, decelerating like a cache warming up rather
            // than leaking, and differing by ~450 MiB between pods of identical
            // age — a shape that points at something keyed by what each pod
            // served. This index is the only per-process structure large enough
            // to account for it: tens of prefixes, but each holding a whole
            // `SegmentFooter` per live segment, one `SubstreamEntry` per
            // `(topic, partition)` in that segment.
            //
            // Recorded here rather than on every mutation: this is the TTL-gated
            // path, so the O(segments) walk runs at most once per prefix per
            // `HIGH_WATERMARK_HINT_TTL`, and it is the point where the live set
            // has just been reconciled.
            let (segments, entries, coords, names) =
                index
                    .values()
                    .fold((0u64, 0u64, 0u64, 0u64), |(s, e, c, n), entry| {
                        (
                            s + entry.segments.len() as u64,
                            e + entry
                                .segments
                                .values()
                                .map(|cached| cached.footer.entries.len() as u64)
                                .sum::<u64>(),
                            c + entry
                                .segments
                                .values()
                                .flat_map(|cached| cached.footer.entries.iter())
                                .map(|entry| entry.producers.len() as u64)
                                .sum::<u64>(),
                            n + entry.topic_names.len() as u64,
                        )
                    });

            PREFIX_INDEX_SEGMENTS.record(segments, &[]);
            PREFIX_INDEX_SUBSTREAM_ENTRIES.record(entries, &[]);
            PREFIX_INDEX_PRODUCER_COORDS.record(coords, &[]);
            PREFIX_INDEX_TOPIC_NAMES.record(names, &[]);
        }

        // Every cache's occupancy, on the same schedule and for the same reason
        // as the four gauges above (#573): this walk is the one TTL-gated place
        // both deployments reach, and the maintenance tick that used to be the
        // sole recording site does not run on the serving fleet
        // (`?maintenance_interval=never`) — which is the half where the maps
        // actually fill. Outside the block above, not inside it: `prefix_index`
        // is one of the nineteen `len()`s, and the guard just dropped is that
        // map's lock.
        self.record_cache_occupancy();

        Ok(())
    }

    /// Record a freshly-written segment in the index (writer fast path): its
    /// footer is authoritative, so a following read on this node needs no
    /// listing/GET. `last_modified_ms` is the object's append time — `now` for a
    /// normal flush, but the max of the merged inputs for a compaction (#66) so
    /// compaction does not reset the retention clock of timestamp-less data.
    pub(super) fn index_insert(
        &self,
        prefix: &str,
        seq: u64,
        footer: SegmentFooter,
        last_modified_ms: i64,
    ) -> Result<()> {
        self.prefixes.index().map(|mut index| {
            let entry = index.entry(prefix.to_owned()).or_default();
            entry.insert_segment(
                seq,
                CachedSegment {
                    footer,
                    last_modified_ms,
                },
            );
            entry.refreshed_at = Some(SystemTime::now());
        })
    }

    /// When the cached prefix index for `prefix` was last reconciled by a
    /// listing, if this process has one. Used to anchor a high-watermark hint's
    /// freshness clock to when the *segment set* was observed rather than to
    /// `now` (#91): the read path serves the index from a cache that may itself
    /// be up to one TTL old, so stamping `mark_listed` with `now` let cross-pod
    /// staleness compound toward ~2×TTL.
    pub(super) fn prefix_index_refreshed_at(&self, prefix: &str) -> Option<SystemTime> {
        self.prefixes
            .try_index()
            .and_then(|index| index.get(prefix).and_then(|entry| entry.refreshed_at))
    }

    /// Force the next index access to re-list (bust the TTL) *and to reconcile* —
    /// used when a data GET 404s (a segment was compacted/expired out from under a
    /// reader, #66).
    ///
    /// Both callers are 404 sites, on the fetch and the compaction read, and a
    /// 404 is proof this index names an object that is gone. So it also lapses the
    /// reconcile window (#408): the entry the caller just pruned is one of an
    /// unknown number, and the next refresh should settle the whole prefix rather
    /// than wait out a window that was sized for a *scheduled* pass.
    ///
    /// That does not reopen the listing storm the window exists to stop. Every
    /// refresh for a prefix queues on one single-flight lock and re-reads the
    /// clock under it, so however many concurrent fetches meet the same ghost,
    /// one of them lists and the rest find the pass already done. A *second*
    /// listing therefore costs a second 404, which — the first listing being
    /// authoritative for the whole prefix at that instant — means something was
    /// retired since: new evidence, not the same evidence again.
    pub(super) fn index_invalidate(&self, prefix: &str) -> Result<()> {
        self.prefixes.index().map(|mut index| {
            if let Some(entry) = index.get_mut(prefix) {
                entry.refreshed_at = None;
                entry.reconciled_at = None;
            }
        })
    }

    /// Drop expired sequences from the index after a retention delete (#61).
    /// A prune removes tail knowledge from this process's view, so it also
    /// bumps the generation: the certified seq floor must be re-read before
    /// the LATEST fast path may trust the index again (see
    /// [`Self::certified_seq_floor`]).
    pub(super) fn index_prune(&self, prefix: &str, seqs: &[u64]) -> Result<()> {
        self.prefixes.index().map(|mut index| {
            if let Some(entry) = index.get_mut(prefix) {
                // Through `retain_segments`, not a `remove_segment` loop:
                // `retire_segments` prunes a whole retirement batch here, and
                // withdrawing one segment at a time from the derived map is
                // quadratic in a sub-stream's segment count (#492).
                let pruned: BTreeSet<u64> = seqs.iter().copied().collect();
                _ = entry.retain_segments(|seq| !pruned.contains(&seq));
                entry.generation += 1;
            }
        })
    }

    /// The epoch-fenced segments holding a sub-stream, sorted by base offset
    /// (#59 review fix). A segment is written atomically under a single-writer
    /// lease, so under normal operation a sub-stream's segments are disjoint
    /// and monotonic; a fenced/zombie writer is the only way two segments'
    /// offset ranges overlap. On overlap the higher `writer_epoch` wins: an
    /// entry wholly inside the range those winners cover is dropped — so a
    /// stale-epoch segment is ignored on read/recovery, exactly as the epic
    /// requires — and an entry that reaches **past** that range is served
    /// clipped to its tail ([`FencedSegment::served_from`], #461), because the
    /// tail is held by nothing else and dropping it loses those offsets
    /// everywhere this view is consumed: fetch, recovery, the high watermark,
    /// retention and both compaction passes.
    /// Operates on the cached index (no object requests).
    pub(super) fn valid_substream_segments(
        &self,
        prefix: &str,
        substream: &Substream,
        partition: i32,
    ) -> Result<Vec<FencedSegment>> {
        // Through `substream_entries` (#492), so the lock is held for this
        // sub-stream's own segments rather than for a scan of the whole prefix.
        let mut segs: Vec<(i64, u64, SubstreamEntry)> = self
            .prefixes
            .index()?
            .get(prefix)
            .map(|index| {
                index
                    .substream_entries(substream, partition)
                    .map(|(seq, writer_epoch, entry)| (writer_epoch, seq, entry.clone()))
                    .collect()
            })
            .unwrap_or_default();

        // Ascending base offset; on a tie prefer the higher epoch, then the
        // higher sequence, so the winner claims the overlapping range. The
        // higher-sequence tie-break matters when epochs are equal: a compacted
        // segment (#66) always has a higher sequence than the originals it
        // merged, so it wins the overlap during the write→delete window even
        // though it carries the same epoch — a reader replica with a lingering
        // deleted-original entry still selects the merged segment.
        segs.sort_by(|a, b| {
            a.2.base_offset
                .cmp(&b.2.base_offset)
                .then_with(|| b.0.cmp(&a.0))
                .then_with(|| b.1.cmp(&a.1))
        });

        let mut out: Vec<FencedSegment> = Vec::with_capacity(segs.len());
        let mut covered_to = i64::MIN;
        for (_epoch, seq, entry) in segs {
            let end = entry.base_offset + entry.record_count;

            // Wholly inside what an already-accepted (>= epoch) segment covers:
            // stale, drop it. This is the merged segment's originals — the case
            // the overlap rule exists for — and a zombie writer's duplicate of
            // records the winning epoch also holds.
            if end <= covered_to {
                continue;
            }

            // Overlaps the frontier and reaches past it: serve its TAIL,
            // `[covered_to, end)` (#461). The head duplicates offsets an
            // earlier segment already serves, but the tail is held by nothing
            // else — the reader's rule from `docs/virtual-topics-format.md`
            // ("drop any entry whose `base_offset` falls below the range
            // already covered") is written for resolving ONE offset, where the
            // higher-priority entry already answers it, and applied to
            // *coverage* it discards records: silently on the read path, and
            // durably on the compaction path, where `retire_segments` deletes
            // the run after the merge. The same error, from the same source,
            // was in the audit until #460.
            let served_from = if entry.base_offset < covered_to {
                SEGMENT_OVERLAP_CLIPPED.add(1, &[]);
                covered_to
            } else {
                entry.base_offset
            };

            covered_to = end;
            out.push(FencedSegment {
                seq,
                served_from,
                entry,
            });
        }
        Ok(out)
    }

    /// Recover a prefix-coalesced sub-stream's next offset (#58) when the
    /// in-memory counter is cold (fresh process / #59 failover). Takes the
    /// **max** of the epoch-fenced segment tail and the persisted floor. It used
    /// to fold a legacy `records/` tail too (the #58 seam, the #62 bypass), since
    /// a `{offset}.batch` object could sit above the segments and reusing an offset
    /// is unacceptable — nothing can create one since #179.
    /// Also folds in the persisted `watermark.high` floor so a fully
    /// retention-drained sub-stream never regresses to 0 (#61 review fix).
    pub(super) async fn recover_substream_next_offset(
        &self,
        topition: &Topition,
        persisted_floor: i64,
    ) -> Result<i64> {
        let (prefix, substream) = self.routed_substream_of(topition).await?;
        self.refresh_prefix_index(&prefix).await?;

        let segment_tail = self
            .valid_substream_segments(&prefix, &substream, topition.partition())?
            .last()
            .map(FencedSegment::end)
            .unwrap_or(0);

        // `persisted_floor` is supplied by the caller (the persisted
        // `watermark.high`) so this shares the single GET the read path already
        // issued instead of re-fetching it (#72). No legacy tail is folded any
        // more (#179): a `records/` object could sit above the segment tail (the
        // #58 seam, the #62 bypass), which is why this fold existed — no writer
        // can create one, and no read path can serve one.
        Ok(segment_tail.max(persisted_floor))
    }

    /// The lowest segment base offset for a sub-stream, from the index (#60): the
    /// start of the segment region. `None` when the
    /// sub-stream has no segment yet.
    pub(super) async fn segment_region_start(&self, topition: &Topition) -> Result<Option<i64>> {
        let (prefix, substream) = self.routed_substream_of(topition).await?;
        self.refresh_prefix_index(&prefix).await?;
        Ok(self
            .valid_substream_segments(&prefix, &substream, topition.partition())?
            .first()
            .map(|fenced| fenced.entry.base_offset))
    }
}

#[cfg(test)]
mod listed_population_tests {
    use super::listed_population;

    /// The cold build and the reconciling pass both list the whole prefix, so what
    /// came back *is* the prefix's population — the number #399 asked the gauge for
    /// and never got.
    #[test]
    fn a_whole_prefix_listing_reports_what_it_found() {
        assert_eq!(Some(826), listed_population(826, true));
    }

    /// An empty whole-prefix listing is still an answer: a prefix whose segments
    /// have all been retired holds none, and leaving the last non-zero reading in
    /// place would show a drained prefix as a backlog for ever.
    #[test]
    fn an_empty_whole_prefix_listing_reports_zero() {
        assert_eq!(Some(0), listed_population(0, true));
    }

    /// The one that matters: an incremental refresh returns the tail above its
    /// cursor. Three segments discovered on a prefix holding 826 is not a
    /// population, and recording it would say compaction is healthy on precisely
    /// the prefix that is running away.
    #[test]
    fn an_incremental_listing_has_no_population_to_report() {
        assert_eq!(None, listed_population(3, false));
    }
}

#[cfg(test)]
mod prefix_index_substream_tests {
    use super::{CachedSegment, PrefixIndex, SegmentFooter, Substream, SubstreamEntry};
    use std::{collections::HashMap, sync::Arc};
    use uuid::Uuid;

    fn entry(topic: &str, topic_id: Option<Uuid>, partition: i32, base: i64) -> SubstreamEntry {
        SubstreamEntry {
            topic: topic.into(),
            topic_id,
            partition,
            base_offset: base,
            record_count: 10,
            byte_start: 0,
            byte_len: 64,
            max_timestamp: 0,
            producers: Box::default(),
        }
    }

    fn segment(writer_epoch: i64, entries: Vec<SubstreamEntry>) -> CachedSegment {
        CachedSegment {
            footer: SegmentFooter {
                writer_epoch,
                nonce: 0,
                entries,
            },
            last_modified_ms: 0,
        }
    }

    /// What `by_substream` must equal at all times, derived the slow way: the
    /// scan `valid_substream_segments` used to do on every call.
    fn derived(index: &PrefixIndex) -> HashMap<(Substream, i32), Vec<(u64, u32)>> {
        let mut out: HashMap<(Substream, i32), Vec<(u64, u32)>> = HashMap::new();
        for (seq, cached) in &index.segments {
            for (position, entry) in cached.footer.entries.iter().enumerate() {
                out.entry((entry.substream(), entry.partition))
                    .or_default()
                    .push((*seq, position as u32));
            }
        }
        for seqs in out.values_mut() {
            seqs.sort_unstable();
        }
        out
    }

    fn assert_in_step(index: &PrefixIndex) {
        assert_eq!(derived(index), index.by_substream);
    }

    /// The derived map survives every mutation the refresh paths make — an
    /// insert, the *replace* the tail probe and the winner-fold both perform on
    /// a sequence already held, a prune, and #408's reconciling retain — because
    /// a map that drifts serves a sub-stream the wrong segments, silently.
    #[test]
    fn the_substream_map_tracks_every_mutation() {
        let id = Uuid::new_v4();
        let mut index = PrefixIndex::default();
        assert_in_step(&index);

        index.insert_segment(
            1,
            segment(
                7,
                vec![
                    entry("alpha", None, 0, 0),
                    entry("alpha", None, 1, 0),
                    entry("beta", Some(id), 0, 0),
                ],
            ),
        );
        index.insert_segment(2, segment(7, vec![entry("alpha", None, 0, 10)]));
        index.insert_segment(3, segment(7, vec![entry("beta", Some(id), 0, 10)]));
        assert_in_step(&index);

        // A replace withdraws the old footer's contribution first: the entry
        // *positions* are what the map holds, and assuming they are unchanged is
        // how a stale index would survive undetected.
        index.insert_segment(
            2,
            segment(
                9,
                vec![entry("beta", Some(id), 0, 10), entry("alpha", None, 0, 10)],
            ),
        );
        assert_in_step(&index);
        assert_eq!(
            index
                .substream_entries(&Substream::Name("alpha".into()), 0)
                .map(|(seq, epoch, _)| (seq, epoch))
                .collect::<Vec<_>>(),
            vec![(1, 7), (2, 9)]
        );

        assert_eq!(index.retain_segments(|seq| seq != 3), 1);
        assert_in_step(&index);

        assert_eq!(index.retain_segments(|seq| seq != 1), 1);
        assert_in_step(&index);

        // Nothing left for `alpha` partition 1, so nothing keyed for it: the map
        // is sized by live partitions, not by everything the prefix ever held.
        assert!(
            !index
                .by_substream
                .contains_key(&(Substream::Name("alpha".into()), 1))
        );
    }

    /// A name-keyed read must never pick up an id-keyed entry carrying the same
    /// name, or a recreated topic's records are served as its predecessor's
    /// (#442). The map keys on the same rule `SubstreamEntry::is` applies, so
    /// this holds by construction — assert it, because it is the one property
    /// the keying could get wrong.
    #[test]
    fn a_name_never_reaches_an_id_keyed_entry_of_the_same_name() {
        let id = Uuid::new_v4();
        let mut index = PrefixIndex::default();

        index.insert_segment(
            1,
            segment(
                1,
                vec![entry("shared", None, 0, 0), entry("shared", Some(id), 0, 0)],
            ),
        );

        assert_eq!(
            index
                .substream_entries(&Substream::Name("shared".into()), 0)
                .map(|(seq, _, e)| (seq, e.topic_id))
                .collect::<Vec<_>>(),
            vec![(1, None)]
        );
        assert_eq!(
            index
                .substream_entries(&Substream::Id(id), 0)
                .map(|(seq, _, e)| (seq, e.topic_id))
                .collect::<Vec<_>>(),
            vec![(1, Some(id))]
        );
    }

    /// The point of the map (#492): asking about a sub-stream the prefix does
    /// not hold costs a lookup, not a walk of every segment's every entry.
    #[test]
    fn an_absent_substream_touches_no_segment() {
        let mut index = PrefixIndex::default();
        for seq in 0..64 {
            index.insert_segment(
                seq,
                segment(1, vec![entry("held", None, 0, seq as i64 * 10)]),
            );
        }

        assert_eq!(
            index
                .substream_entries(&Substream::Name("never-written".into()), 0)
                .count(),
            0
        );
        assert!(
            !index
                .by_substream
                .contains_key(&(Substream::Name("never-written".into()), 0))
        );
    }

    /// The whole of #476 item 2b: a topic contributes one entry per segment it
    /// occupies, and all of them have to name it through one allocation.
    ///
    /// `Arc::ptr_eq` rather than `==` on purpose — the names compare equal
    /// whether or not anything is shared, which is exactly the state this
    /// replaces, so an equality assertion would pass on the unfixed tree.
    #[test]
    fn every_entry_naming_a_topic_shares_one_allocation() {
        let mut index = PrefixIndex::default();

        for seq in 0..8 {
            index.insert_segment(
                seq,
                segment(
                    1,
                    vec![
                        entry("alpha", None, 0, seq as i64 * 10),
                        entry("alpha", None, 1, seq as i64 * 10),
                        entry("beta", None, 0, seq as i64 * 10),
                    ],
                ),
            );
        }

        let names: Vec<&Arc<str>> = index
            .segments
            .values()
            .flat_map(|cached| cached.footer.entries.iter())
            .filter(|entry| &*entry.topic == "alpha")
            .map(|entry| &entry.topic)
            .collect();

        assert_eq!(names.len(), 16);
        assert!(names.iter().all(|name| Arc::ptr_eq(name, names[0])));
        assert_eq!(&**names[0], "alpha");

        // Two topics, 24 entries (#476): the count this index pays is its
        // topics, which is what `tansu_prefix_index_topic_names` reports.
        assert_eq!(index.topic_names.len(), 2);
    }

    /// A prefix outlives the topics routed into it, so the interner has to be a
    /// cache and not a ledger.
    #[test]
    fn a_topic_with_no_entries_left_keeps_no_name() {
        let mut index = PrefixIndex::default();

        index.insert_segment(1, segment(1, vec![entry("alpha", None, 0, 0)]));
        index.insert_segment(2, segment(1, vec![entry("beta", None, 0, 0)]));
        index.insert_segment(3, segment(1, vec![entry("alpha", None, 0, 10)]));

        assert_eq!(index.topic_names.len(), 2);

        // `beta`'s only segment goes (#476); `alpha` still has two.
        assert_eq!(index.retain_segments(|seq| seq != 2), 1);

        let mut held: Vec<&str> = index.topic_names.iter().map(|name| &**name).collect();
        held.sort_unstable();
        assert_eq!(held, vec!["alpha"]);

        assert_eq!(index.retain_segments(|_| false), 2);
        assert!(index.topic_names.is_empty());
    }
}
