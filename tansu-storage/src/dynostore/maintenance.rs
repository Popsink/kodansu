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

//! The maintenance tick: which prefixes this replica claims (#490), and what
//! it runs against each of them.

use super::*;

impl DynoStore {
    /// Record the size of the cluster-global `meta.json` and of the two tables
    /// inside it that nothing prunes (#283), returning `(producers, transactions,
    /// bytes)` so the numbers can be asserted without standing up a metrics
    /// reader.
    ///
    /// **Measurement, not a fix.** The producer table grows by one entry per
    /// `InitProducerId` — one per connector restart — and there is no `remove` or
    /// `retain` on it anywhere in the tree. Whether that needs an expiry policy
    /// *now* is a question about the production bucket's actual growth rate, and
    /// no amount of reading the code answers it; the transaction half additionally
    /// needs a decision, since #81 retains aborted transactions on purpose. So
    /// this attaches the growth math before the table is big enough to matter,
    /// which is the whole of what the issue asks for at this stage.
    ///
    /// On the maintenance tick, which is the right cadence for a level that moves
    /// with restarts rather than with traffic: at one conditional GET per tick it
    /// is a rounding error against the pass it runs in, and the cached etag makes
    /// most of those a `NotModified`. The serialisation is the same one
    /// `OptiCon::with_mut` performs on every producer registration, so its cost is
    /// already characterised.
    pub(super) async fn measure_meta(&self) -> Result<(u64, u64, u64)> {
        let measured = self
            .meta
            .with(&self.object_store, |meta| {
                Ok((
                    meta.producers.len() as u64,
                    meta.transactions.len() as u64,
                    serde_json::to_vec(meta)?.len() as u64,
                ))
            })
            .await?;

        let (producers, transactions, bytes) = measured;

        META_PRODUCERS.record(producers, &[]);
        META_TRANSACTIONS.record(transactions, &[]);
        META_BYTES.record(bytes, &[]);

        Ok(measured)
    }

    /// Claim this tick's maintenance work-set, stateless and coordinator-free
    /// (#126). Every maintainer enumerates the full prefix universe (the
    /// non-compacted topics' prefixes ∪ locally-indexed prefixes ∪ the retired
    /// prefixes of deleted topics, #532), shuffles it
    /// with a per-process seed so N replicas sweep in independent orders, and for
    /// each prefix:
    ///
    /// - **recency skip** — if the compaction lease shows *another* maintainer
    ///   maintained it within `maintenance_recency`, that peer has it, so skip
    ///   without a `segments/` LIST or any work. A lease this replica holds is
    ///   never skipped (DEV-683): the window exists to keep peers off a prefix,
    ///   and a prefix worked to the end of the last pass has its lease renewed
    ///   — re-stamped — moments before this tick, so skipping our own made a
    ///   lone maintainer claim only what its last pass had *not* worked;
    /// - **claim** — otherwise acquire the lease (which stamps `maintained_at_ms`
    ///   and caches the held term, so the two passes' inner acquires are free).
    ///   A win adds the prefix to the returned set; a fenced/lost claim means a
    ///   peer is on it right now, so skip.
    ///
    /// The returned set is the filter both maintenance passes honour. The
    /// per-prefix lease stays the correctness guard: a duplicate claim under a
    /// race is fenced, so this can only over-cover (one wasted lease GET) or
    /// under-cover (deferred to the next tick), never double-work or corrupt.
    /// Recency `0` disables the skip → every maintainer claims every prefix
    /// (single-maintainer behaviour).
    pub(super) async fn claim_maintenance_prefixes(&self, now_ms: i64) -> Result<BTreeSet<String>> {
        let mut universe: BTreeSet<String> = self.prefixes.index()?.keys().cloned().collect();
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
            // Compacted topics reach segments only when segment-routed (#175);
            // until then they hold no prefix to maintain. Routed, their
            // dedicated prefix joins the universe so the per-key pass runs
            // under the same claim as every other prefix's maintenance. Since
            // the routing flag was hardwired every compacted topic is routed, so
            // there is no unrouted case left to skip.
            for partition in 0..metadata.topic.num_partitions {
                _ = universe.insert(self.routed_prefix(
                    &Topition::new(metadata.topic.name.clone(), partition),
                    compact,
                ));
            }
        }

        // And the prefixes whose last topic has been deleted (#532): their
        // threshold comes from a retired marker rather than from a topic, and a
        // prefix outside this claim is filtered out of both maintenance passes —
        // so without them here the marker would never be acted on.
        universe.extend(self.retired_prefixes().await?.into_keys());

        let mut prefixes: Vec<String> = universe.into_iter().collect();
        let mut rng = SmallRng::seed_from_u64(self.identity.maintenance_seed ^ now_ms as u64);
        prefixes.shuffle(&mut rng);

        let recency_ms = self.tuning.maintenance_recency.as_millis() as i64;
        let mut owned = BTreeSet::new();
        for prefix in prefixes {
            if recency_ms > 0
                && let Some(lease) = self.read_compaction_lease(&prefix).await?
                && lease.holder != self.identity.writer_id
                && now_ms.saturating_sub(lease.maintained_at_ms) < recency_ms
            {
                MAINTENANCE_PREFIXES.add(1, &[KeyValue::new("outcome", "recent")]);
                continue;
            }
            match self.acquire_compaction_lease(&prefix).await {
                Ok(_) => {
                    MAINTENANCE_PREFIXES.add(1, &[KeyValue::new("outcome", "claimed")]);
                    _ = owned.insert(prefix);
                }
                Err(Error::Api(ErrorCode::NotLeaderOrFollower)) => {
                    MAINTENANCE_PREFIXES.add(1, &[KeyValue::new("outcome", "lost")]);
                }
                Err(err) => error!(?err, prefix, "maintenance claim"),
            }
        }
        Ok(owned)
    }

    /// Per-prefix segment maintenance — retention **then** compaction for each
    /// prefix, several prefixes at a time (#140). Replaces running the two as
    /// whole sequential passes, which had three compounding failure modes on a
    /// high-fan-out workload:
    ///
    /// - **Retention starved compaction.** `policy_delete` ran to completion
    ///   before `policy_compact_segments` started, so a large delete backlog
    ///   (~100k delete-ops after a restart) consumed the whole run budget and
    ///   compaction never executed — the bounded run (#131) cancelled the tick
    ///   first (observed: run-timeout fired 18×, zero completions). Interleaving
    ///   per prefix means a cancelled run has done *both* for a subset of
    ///   prefixes instead of *one* for none of them.
    /// - **Per-maintainer throughput was one prefix at a time.** A prefix is
    ///   compacted only by the replica holding its lease, so adding maintainer
    ///   pods cannot help a prefix that is already owned (3 → 8 replicas did not
    ///   drain the busiest prefixes). Concurrency *within* a maintainer is the
    ///   lever that does.
    /// - **Traversal order starved the prefixes that needed it most.** Ordered
    ///   largest-known-`S` first, so the prefixes furthest over the trigger are
    ///   drained before a timeout can cut the run, instead of by prefix name.
    ///
    /// Retention and compaction of one prefix stay strictly sequential: both
    /// mutate that prefix's segment set and are serialized by the same compaction
    /// lease (#115), so running them concurrently would make each yield to the
    /// other. Different prefixes hold different leases, so the fan-out is safe.
    /// A per-prefix error is logged and skips that prefix only.
    pub(super) async fn maintain_prefix_segments(
        &self,
        now_ms: i64,
        owned: Option<&BTreeSet<String>>,
    ) -> Result<(u64, u64)> {
        let concurrency = self.tuning.prefix_maintenance_concurrency.max(1);
        let deadline = tokio::time::Instant::now() + self.tuning.maintenance_pass_budget;

        let thresholds = self.segment_retention_thresholds(now_ms, owned).await?;

        // Which of the prefixes below are only being maintained because a
        // deleted topic left segments on them (#532), so a drained one can give
        // its marker up. Read here rather than returned by the threshold build
        // so that build keeps its signature and its single job; the set is
        // served from the same etag-delta cache, so this costs the listing and
        // no GET.
        let retired = self.retired_prefixes().await?;

        let compactable: BTreeSet<String> = self
            .compactable_prefixes(owned)
            .await?
            .into_iter()
            .collect();
        let per_key = self.per_key_compact_prefixes(owned).await?;

        // Drain frozen legacy regions before the per-prefix work (#175 release 2).
        // Sequential and separately budgeted rather than folded into the
        // concurrent per-prefix jobs below: the budget is per *tick*, and sharing
        // a counter across concurrent tasks would need a lock for no benefit —
        // the carry-over is a one-shot backlog, not steady-state work.

        // Largest known live-segment count first (free: it is what this process
        // already has cached, no extra request). A prefix this maintainer has
        // never indexed sorts last — it has no known backlog.
        let live_counts: BTreeMap<String, usize> = self
            .prefixes
            .index()?
            .iter()
            .map(|(prefix, entry)| (prefix.clone(), entry.segments.len()))
            .collect();

        let mut prefixes: Vec<String> = thresholds
            .keys()
            .chain(compactable.iter())
            .chain(per_key.iter())
            .cloned()
            .collect::<BTreeSet<String>>()
            .into_iter()
            .collect();
        prefixes.sort_by_key(|prefix| Reverse(live_counts.get(prefix).copied().unwrap_or(0)));

        let outcomes = futures::stream::iter(prefixes.into_iter().map(|prefix| {
            let thresholds = &thresholds;
            let compactable = &compactable;
            let per_key = &per_key;
            let retired = &retired;

            async move {
                let deleted = match thresholds.get(&prefix) {
                    Some(&threshold_ms) => self
                        .expire_prefix_segments_if_due(&prefix, threshold_ms)
                        .await
                        .unwrap_or(0),

                    // Retention was never asked about this prefix (#544). Its
                    // occupants are compact-only (#175) or otherwise outside
                    // `segment_retention_thresholds`, so no expiry path runs and
                    // none of the retention series mention it — which is the same
                    // silence a prefix retention has stopped visiting produces.
                    // Counted so the denominator of "prefixes retention should be
                    // covering" is `claimed` minus this, rather than a rate
                    // arithmetic over `tansu_maintenance_prefixes_total`.
                    None => {
                        EXPIRY_SKIPPED.add(1, &[KeyValue::new("reason", "no_threshold")]);
                        0
                    }
                };

                // Per-key compaction of a compacted topic's dedicated prefix
                // (#175), every tick, BEFORE the size merge so the merge folds
                // the cleaned small residues rather than stale versions. Its
                // count is records removed (reported via the counter), not
                // segments merged, so it is not folded into `compacted`. A
                // per-prefix error is logged and skips this prefix only, as
                // for the drain (#140).
                if per_key.contains(&prefix) {
                    self.drain_compact_prefix_per_key(&prefix).await;
                }

                let compacted = if compactable.contains(&prefix) {
                    self.drain_compact_prefix(&prefix).await
                } else {
                    0
                };

                // A retired prefix that has now given up every segment needs
                // no marker (#532): the threshold it carried has done its work,
                // and leaving it would keep the prefix in every maintainer's
                // claim universe — a lease GET per tick per replica, forever,
                // for a prefix with nothing in it. After the expiry above, which
                // is what refreshed the index this reads. A failure is this
                // prefix's alone: the next tick retries.
                if retired.contains_key(&prefix) {
                    _ = self
                        .drop_retired_prefix_if_drained(&prefix)
                        .await
                        .inspect_err(|err| error!(?err, prefix));
                }

                // Retro-certify gaps no expiry will reach (#290). Once per
                // prefix per process, and after the two passes above so it reads
                // the tail they left rather than the one they were about to
                // change. A failure is this prefix's alone, as for the others.
                _ = self
                    .certify_prefix_served_ends(&prefix)
                    .await
                    .inspect_err(|err| error!(?err, prefix));

                (deleted, compacted)
            }
        }))
        .buffer_unordered(concurrency)
        .fold((0, 0), |(deleted, compacted), (d, c)| async move {
            (deleted + d, compacted + c)
        })
        .await;

        // Then keep going while a claimed prefix is still over the trigger
        // (#399), instead of returning and idling out the rest of the interval.
        let (deleted, compacted) = outcomes;
        let swept = self
            .sweep_backlogged_prefixes(&compactable, concurrency, deadline)
            .await;

        Ok((deleted, compacted + swept))
    }

    /// Drain the claimed prefixes that are *still* over the compaction trigger,
    /// repeatedly, until `deadline` or until produce stops (#399, DEV-683).
    ///
    /// #140's acceptance was that the busiest prefixes converge toward
    /// `prefix_compact_min_segments` under sustained produce. Its four remedies
    /// shipped and they did not: production sits at 17 500 live segments against
    /// a 256 trigger, 30–68× over, having grown ~700 segments/hour through the
    /// remedies. #140 concluded the limit was one maintainer's merge throughput
    /// and added concurrency *within* a maintainer. But the four maintainers run
    /// at **12 millicores between them** with S3 GETs at 24 ms, and compaction
    /// fires in a burst of 1–2 minutes per 10-minute window and then nothing:
    /// a 10–20 % duty cycle. They are not throughput-bound, they are unscheduled.
    ///
    /// So the interval stops deciding how much work a tick does: the tick keeps
    /// sweeping while there is a backlog it owns. What ends it changed with
    /// DEV-683, after merges started landing on a GCS bucket:
    ///
    /// - **A wall-clock deadline, not "a sweep merged nothing".** Under
    ///   sustained produce every sweep merges something, so the pass outlived
    ///   the interval, the broker skipped the next tick as still in flight, and
    ///   the claim — which is what picks up a prefix the last pass did not hold —
    ///   ran once every few intervals. The deadline is the pass budget, ~0.9× the
    ///   interval, so the next tick always finds this pass finished. The broker's
    ///   bounded run (#131) stays the outer bound for a pass that wedges.
    /// - **The backlog is re-listed, not read from the cache.** A dedicated
    ///   maintainer produces nothing, so its index only moves when compaction
    ///   lists — and a prefix drained below the trigger was never listed again,
    ///   so it dropped out of the pass while it regrew. Each round lists the
    ///   prefixes still being watched (the incremental listing compaction itself
    ///   uses) before choosing the backlog.
    /// - **A round with nothing to merge waits for produce.** It used to end the
    ///   pass, which left every prefix to grow unattended for the rest of the
    ///   interval. A prefix stays watched while it is over the trigger or was
    ///   produced to within the last two seconds, judged by its segments' append
    ///   times rather than by whether a listing found something new: the round
    ///   after a drain can list a hot prefix before its next segment lands.
    ///   A backlogged prefix whose last drain merged nothing is not waited on
    ///   either: its segments cannot form a run, and a listing will not change
    ///   that, so it used to end the sweep and still does unless it is being
    ///   produced to. Anything else is let go, so a quiet universe ends the pass
    ///   at once and costs one listing per prefix per pass. Between rounds that merged
    ///   nothing the sweep sleeps a sixty-fourth of the budget, at most a
    ///   second, which bounds the listing rate on a prefix that is producing
    ///   but not yet over the trigger.
    ///
    /// The claim is deliberately **not** re-taken between sweeps. It is already
    /// held for these prefixes, and re-reading every lease per sweep would be
    /// O(prefixes) requests for nothing.
    ///
    /// Retention and the per-key pass are deliberately not repeated either. Both
    /// are once-per-tick work — retention is age-gated and the per-key pass GETs
    /// every segment of its prefix, so sweeping them would multiply the read cost
    /// of a clean prefix by the sweep count for no removal at all.
    pub(super) async fn sweep_backlogged_prefixes(
        &self,
        compactable: &BTreeSet<String>,
        concurrency: usize,
        deadline: tokio::time::Instant,
    ) -> u64 {
        /// How recently a prefix's newest segment must have been written for it
        /// to count as produced to, and so stay in the sweep below the trigger
        /// (DEV-683). Two seconds keeps any prefix producing more than one
        /// segment every two seconds — 300 a 10-minute interval, about the 256
        /// trigger — and lets a test that produced and then maintained finish
        /// two seconds later.
        const PRODUCING_WITHIN: Duration = Duration::from_secs(2);

        let pause = (self.tuning.maintenance_pass_budget / 64).min(PRODUCING_WITHIN / 2);
        let producing_within = PRODUCING_WITHIN.as_millis() as i64;

        let mut watched = compactable.clone();
        let mut stuck = BTreeSet::new();
        let mut compacted = 0;

        while !watched.is_empty() && tokio::time::Instant::now() < deadline {
            // A prefix's newest segment is as old as the listing that found it,
            // not as old as the slowest listing of the round. Judged after all
            // of them, one prefix listing in 3 s aged every other prefix past
            // `PRODUCING_WITHIN`: on the DEV-692 bucket `load-2` left the sweep
            // under ~12 segments a second and grew to 4 480 before the next
            // tick (#773 follow-up).
            let listed_from_ms = Self::now_ms();

            futures::stream::iter(watched.iter().cloned().map(|prefix| async move {
                _ = self
                    .refresh_prefix_index_forced(&prefix)
                    .await
                    .inspect_err(|err| warn!(?err, prefix, "listing a swept prefix"));
            }))
            .buffer_unordered(concurrency)
            .collect::<()>()
            .await;

            let newest = self.newest_produced_ms(&watched);
            let backlogged = self.backlogged_prefixes(&watched);

            watched.retain(|prefix| {
                (backlogged.contains(prefix) && !stuck.contains(prefix))
                    || newest.get(prefix).is_some_and(|newest| {
                        listed_from_ms.saturating_sub(*newest) < producing_within
                    })
            });

            let merged = if backlogged.is_empty() {
                0
            } else {
                MAINTENANCE_SWEEPS.add(1, &[]);
                let drained: Vec<(String, u64)> =
                    futures::stream::iter(backlogged.into_iter().map(|prefix| async move {
                        let merged = self.drain_compact_prefix(&prefix).await;
                        (prefix, merged)
                    }))
                    .buffer_unordered(concurrency)
                    .collect()
                    .await;

                for (prefix, merged) in &drained {
                    if *merged == 0 {
                        _ = stuck.insert(prefix.clone());
                    } else {
                        _ = stuck.remove(prefix);
                    }
                }
                drained.iter().map(|(_, merged)| merged).sum()
            };
            compacted += merged;

            if merged == 0 && !watched.is_empty() {
                tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + pause)).await;
            }
        }

        compacted
    }

    /// When each of `prefixes` was last produced to, as far as this process's
    /// index knows: the newest append time among its most recent segments
    /// (DEV-683).
    ///
    /// Compaction claims are left out, and a merged segment carries its
    /// inputs' newest append time, so nothing this maintainer writes reads as
    /// produce. Only the tail is looked at: produce lands there, and a prefix
    /// holds tens of thousands of entries on the fleet.
    fn newest_produced_ms(&self, prefixes: &BTreeSet<String>) -> BTreeMap<String, i64> {
        /// Segments read back from the tail; a merge in flight puts one claim
        /// and one merged segment there, so a handful is plenty.
        const TAIL: usize = 16;

        let Some(index) = self.prefixes.try_index() else {
            return BTreeMap::new();
        };

        prefixes
            .iter()
            .filter_map(|prefix| {
                index.get(prefix).and_then(|entry| {
                    entry
                        .segments
                        .values()
                        .rev()
                        .filter(|cached| !Self::is_compaction_claim(&cached.footer))
                        .take(TAIL)
                        .map(|cached| cached.last_modified_ms)
                        .max()
                        .map(|newest| (prefix.clone(), newest))
                })
            })
            .collect()
    }

    /// The claimed prefixes whose cached live-segment count is still over the
    /// compaction trigger, largest first (#399).
    ///
    /// Read from the in-memory index this process already holds — no request,
    /// so [`Self::sweep_backlogged_prefixes`] lists the prefixes first (DEV-683) —
    /// and gated on the same `min_segments` + `keep_hot` arithmetic
    /// `compact_prefix_segments` selects a run under, so a prefix that cannot
    /// yield a run is never swept for one.
    pub(super) fn backlogged_prefixes(&self, compactable: &BTreeSet<String>) -> Vec<String> {
        let Some(index) = self.prefixes.try_index() else {
            return Vec::new();
        };

        let mut backlogged: Vec<(usize, String)> = compactable
            .iter()
            .filter_map(|prefix| {
                index
                    .get(prefix)
                    .map(|entry| entry.segments.len())
                    .filter(|live| {
                        *live > self.tuning.prefix_compact_min_segments
                            && live.saturating_sub(self.tuning.prefix_compact_keep_hot) >= 2
                    })
                    .map(|live| (live, prefix.clone()))
            })
            .collect();

        // Largest known backlog first, as the tick's first pass orders itself
        // (#140): a timeout that cuts a sweep should cut the prefixes that needed
        // it least.
        backlogged.sort_by_key(|(live, _)| Reverse(*live));

        backlogged.into_iter().map(|(_, prefix)| prefix).collect()
    }
}
