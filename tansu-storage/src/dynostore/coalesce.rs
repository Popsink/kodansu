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

//! The write path: the per-prefix coalescing buffer (#50/#57), the
//! single-writer lease (#59) and the leaseless flush (#86) that replaced it.

use super::*;

impl DynoStore {
    /// The coalesce linger with per-flush random jitter (±20%, within the #91
    /// 10–25% guidance). Independent pods — and successive windows on one pod —
    /// draw uncorrelated flush instants instead of staying phase-aligned, so
    /// they stop racing the create of the *same* next segment name; on GCS that
    /// collision returns a 429 and burns a conflict-retry. Same desync trick as
    /// [`throttle_backoff`].
    pub(super) fn jittered_linger(&self) -> Duration {
        let base_ms = self
            .tuning
            .coalesce_linger
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let span = base_ms / 5; // ±20%
        let jitter = rng().random_range(0..=2 * span);
        Duration::from_millis(base_ms.saturating_sub(span) + jitter)
    }

    /// The per-prefix flush serialization lock (see [`PrefixLocks`]).
    pub(super) fn prefix_flush_lock(&self, prefix: &str) -> Result<Arc<tokio::sync::Mutex<()>>> {
        self.prefix_locks.flush(prefix)
    }

    /// The durable single-writer lease object for a connector prefix (#59).
    pub(super) fn lease_location(&self, prefix: &str) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/lease.json",
            self.identity.cluster, prefix,
        ))
    }

    /// The durable compaction lease object for a connector prefix (#66): a lease
    /// distinct from the produce lease, so compaction (which runs on the
    /// maintenance workers, not the producing broker) coordinates
    /// compactor-vs-compactor without needing — or fencing — the produce writer.
    pub(super) fn compaction_lease_location(&self, prefix: &str) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/compaction-lease.json",
            self.identity.cluster, prefix,
        ))
    }

    /// Acquire or renew the compaction lease for `prefix` (#66) — same fence as
    /// the produce lease but on a separate object/cache, so a maintenance worker
    /// can compact without holding (or fencing) the produce lease.
    pub(super) async fn acquire_compaction_lease(&self, prefix: &str) -> Result<i64> {
        let location = self.compaction_lease_location(prefix);
        self.acquire_or_renew_lease_at(prefix, &location).await
    }

    /// Generic lease acquire/renew against `location`, caching the held term in
    /// `cache` under `key` (#59/#66). The etag CAS is the fence; a live foreign
    /// lease or a lost CAS yields `NotLeaderOrFollower`. A held term is reused
    /// with no write while more than a third of it remains, keeping the object's
    /// mutation rate well under GCS's ~1/s cap (#13).
    pub(super) async fn acquire_or_renew_lease_at(
        &self,
        key: &str,
        location: &Path,
    ) -> Result<i64> {
        let now = SystemTime::now();
        let margin = self.tuning.prefix_lease_ttl / 3;

        // Fast path: comfortably within our term — no object mutation.
        if let Some(held) = self.prefixes.held_lease(key)?
            && held.expires_at > now + margin
        {
            return Ok(held.epoch);
        }

        // Read the current lease and its version (etag) to CAS against.
        let (current, version) = match self.object_store.get(location).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let lease = serde_json::from_slice::<PrefixLease>(&result.bytes().await?)?;
                (Some(lease), Some(version))
            }
            Err(object_store::Error::NotFound { .. }) => (None, None),
            Err(err) => return Err(err.into()),
        };

        // "Ours" iff the object's etag matches the one we last wrote — then this
        // is a renewal, not a takeover of a foreign live lease.
        let our_version = self.prefixes.held_lease(key)?.and_then(|held| held.version);
        let ours = matches!((&version, &our_version), (Some(v), Some(o)) if v.e_tag == o.e_tag);
        let expired = current
            .as_ref()
            .is_none_or(|lease| Self::now_ms() >= lease.expires_at_ms);

        // A live lease held by someone else — we are fenced. Drop any stale
        // cached term and yield.
        if !ours && !expired {
            if let Some(lease) = &current {
                debug!(key, holder = %lease.holder, epoch = lease.epoch, "lease held elsewhere");
            }
            self.prefixes.drop_lease(key);
            LEASE_FENCED.add(1, &[]);
            return Err(Error::Api(ErrorCode::NotLeaderOrFollower));
        }

        // Acquirable (unheld / expired / ours): bump epoch, CAS on the read
        // version so a concurrent acquirer loses.
        let epoch = current.as_ref().map(|lease| lease.epoch).unwrap_or(0) + 1;
        let lease = PrefixLease {
            epoch,
            holder: self.identity.writer_id.clone(),
            expires_at_ms: Self::now_ms() + self.tuning.prefix_lease_ttl.as_millis() as i64,
            // Stamp the acquire time (#126): for the compaction lease this marks
            // the prefix as maintained now, so a peer skips it for the recency
            // window. Harmless for the produce lease (never read).
            maintained_at_ms: Self::now_ms(),
        };
        let payload = PutPayload::from(Bytes::from(serde_json::to_vec(&lease)?));
        let mode = match &version {
            Some(version) => PutMode::Update(version.clone()),
            None => PutMode::Create,
        };

        match self
            .object_store
            .put_opts(
                location,
                payload,
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(result) => {
                let version = Some(UpdateVersion {
                    e_tag: result.e_tag,
                    version: result.version,
                });
                self.prefixes.hold_lease(
                    key,
                    HeldLease {
                        epoch,
                        expires_at: now + self.tuning.prefix_lease_ttl,
                        version,
                    },
                );
                LEASE_ACQUIRES.add(1, &[]);
                debug!(key, epoch, "lease acquired/renewed");
                Ok(epoch)
            }

            // Lost the CAS: another writer acquired concurrently. Fenced.
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. },
            ) => {
                debug!(key, "lease CAS lost — fenced");
                self.prefixes.drop_lease(key);
                LEASE_FENCED.add(1, &[]);
                Err(Error::Api(ErrorCode::NotLeaderOrFollower))
            }

            Err(err) => Err(err.into()),
        }
    }

    /// The effective (batch-count, byte) flush triggers for a prefix buffer
    /// (#90). A buffer that has ingested a backfill-class batch relaxes both to
    /// backfill floors — the byte trigger to [`Self::BACKFILL_COALESCE_BYTES`]
    /// and the count trigger past [`Self::COALESCE_MAX_RECORDS`] so it never
    /// fires first — leaving the record cap as the effective limiter. The
    /// folded-in snapshot (#90) then coalesces into a few large segments (~1 PUT
    /// per large batch, bounded `S`, #91) instead of one small segment per
    /// batch. Steady-state CDC keeps the tight `coalesce_batches` /
    /// `coalesce_bytes` for low latency. `max` never lowers an operator's
    /// URL-configured value (#54).
    pub(super) fn flush_thresholds(&self, backfill: bool) -> (usize, usize) {
        if backfill {
            (
                self.tuning
                    .coalesce_batches
                    .max(Self::COALESCE_MAX_RECORDS as usize),
                self.tuning
                    .coalesce_bytes
                    .max(Self::BACKFILL_COALESCE_BYTES),
            )
        } else {
            (self.tuning.coalesce_batches, self.tuning.coalesce_bytes)
        }
    }

    /// Buffer `deflated` for a prefix-coalesced flush and await its assigned
    /// offset (#57). Keyed by the topition's connector prefix, so one buffer
    /// accumulates batches across every topic under the prefix and flushes them
    /// into one shared segment object. (It replaced a per-partition buffer,
    /// deleted with the rest of #50 in #177.) The
    /// idempotent sequence and schema were already validated by `produce`.
    pub(super) async fn enqueue_prefix_coalesced(
        &self,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        // Routed, not `prefix_of` (#175): a compacted topic's batches buffer —
        // and flush — under its dedicated prefix. This is where the `#113` memo
        // is first consulted when the flag is on (`produce`'s eligibility gate
        // short-circuits past `topic_is_compacted` in that case), so the cost is
        // the same one conditional metadata GET per TTL — just paid here.
        let (prefix, substream) = self.routed_substream_of(topition).await?;
        let (ack, offset) = oneshot::channel();
        let span = deflated.last_offset_delta as i64 + 1;
        let size = size_of::<i64>() + size_of::<i32>() + deflated.batch_length.max(0) as usize;
        // A transaction marker flushes the buffer immediately (#174 release B):
        // `txn_end` writes markers sequentially per partition, so parking each
        // on the linger would cost an N-partition commit N × linger; flushing
        // now also shrinks the durable-but-unregistered window. Control-only —
        // transactional *data* batches coalesce normally, or a transactional
        // workload would degrade to one object per batch, the very cost the
        // coalescing exists to avoid. Cheap: commit/abort rate ≪ data rate.
        let control = deflated.is_control();

        enum Action {
            Flush(PrefixCoalesceBuffer),
            StartTimer,
            Wait,
        }

        let action = {
            let mut buffers = self
                .prefix_coalesce_buffers
                .lock()
                .map_err(Into::<Error>::into)?;
            let buffer = buffers.entry(prefix.clone()).or_default();

            let first = buffer.pending.is_empty();
            buffer.pending.push(PrefixPending {
                topition: topition.to_owned(),
                substream,
                batch: deflated,
                ack,
            });
            buffer.records += span;
            buffer.bytes += size;
            buffer.backfill |= span >= Self::PREFIX_BACKFILL_MIN_RECORDS;

            let (batches_threshold, bytes_threshold) = self.flush_thresholds(buffer.backfill);
            if buffer.pending.len() >= batches_threshold
                || buffer.bytes >= bytes_threshold
                || buffer.records >= Self::COALESCE_MAX_RECORDS
                || control
            {
                Action::Flush(std::mem::take(buffer))
            } else if first {
                Action::StartTimer
            } else {
                Action::Wait
            }
        };

        match action {
            Action::Flush(buffer) => self.flush_prefix_coalesced(&prefix, buffer).await,

            Action::StartTimer => {
                let store = self.clone();
                let prefix = prefix.clone();
                let linger = self.jittered_linger();

                _ = tokio::spawn(async move {
                    sleep(linger).await;

                    let buffer = store
                        .prefix_coalesce_buffers
                        .lock()
                        .ok()
                        .and_then(|mut buffers| buffers.remove(&prefix));

                    if let Some(buffer) = buffer.filter(|buffer| !buffer.pending.is_empty()) {
                        store.flush_prefix_coalesced(&prefix, buffer).await;
                    }
                });
            }

            Action::Wait => {}
        }

        offset
            .await
            .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
    }

    /// Flush a drained prefix buffer as one shared segment object and resolve
    /// each parked produce with its assigned offset (#57). Batches are grouped
    /// by topition; each sub-stream is assigned an independent base offset from
    /// its in-memory high-watermark hint (#58 assignment — the single-writer
    /// counter is authoritative, cold-start footer recovery is #58), and the
    /// whole set is written as one create-only segment via
    /// [`Self::assign_and_create_segment`]. On failure every parked producer
    /// gets the error and retries.
    pub(super) async fn flush_prefix_coalesced(&self, prefix: &str, buffer: PrefixCoalesceBuffer) {
        if buffer.pending.is_empty() {
            return;
        }

        // The leaseless seq-CAS arbiter (#86) is the only flush since #177: no
        // lease, no fencing epoch, any replica may append to any prefix.
        self.flush_prefix_coalesced_leaseless(prefix, buffer).await
    }

    /// Send `error` to every parked producer in a failed prefix flush (#57) so
    /// each retries; the unflushed batches are never acked.
    pub(super) fn fail_prefix_flush(buffer: PrefixCoalesceBuffer, error: Error, prefix: &str) {
        error!(?error, prefix, "prefix coalesced flush failed");
        for pending in buffer.pending {
            _ = pending.ack.send(Err(error.clone()));
        }
    }

    /// Leaseless prefix flush (#86): the create-only segment-sequence CAS is the
    /// offset arbiter, so any replica may append to any prefix — no lease, no
    /// fencing epoch, no cross-broker produce forwarding. Fold every live segment (bypassing the
    /// index TTL), derive each sub-stream's base from the folded tail, encode a v2
    /// segment and try to create it at the next free sequence. On a create
    /// conflict a peer won that sequence: fold its footer, re-derive the bases and
    /// re-encode, then retry the next sequence. Contiguity holds because a writer
    /// only ever targets `folded_max + 1`, and each conflict forces it to ingest
    /// the winner before re-deriving.
    ///
    /// An *ambiguous* PUT (our create may have landed before the response was
    /// lost) is disambiguated by the per-flush `nonce` written into the footer
    /// (#89): probe the object at `candidate` and adopt it iff it carries our
    /// nonce, rather than blind-retrying at the next sequence and double-writing
    /// the batch. A peer's footer (or none) means we did not win — fold and
    /// re-derive; a probe that itself errors leaves it unknown, so we fail for a
    /// client retry, which the log-based dedup (#88) makes safe.
    pub(super) async fn flush_prefix_coalesced_leaseless(
        &self,
        prefix: &str,
        buffer: PrefixCoalesceBuffer,
    ) {
        /// Bounds the conflict-correction loop; far above any real concurrency.
        const MAX_ATTEMPTS: usize = 64;

        // Wall-clock budget for the conflict-correction loop (#157), overridable
        // via `flush_max_elapsed`. A writer still losing the create-CAS after
        // this long is amplifying LIST+PUT against a contended prefix with no
        // sign of winning: yield to the producer's own retry (the terminal is
        // retriable, and log-based dedup #88 makes the replay safe) rather than
        // spend the rest of the budget on the bucket.
        let max_elapsed = self.tuning.flush_max_elapsed;

        // Per-partition FIFO across two concurrent local flushes of this prefix:
        // the seq-CAS is the cross-writer offset authority, but this lock still
        // keeps a single pod's buffer order == offset order.
        let flush_lock = match self.prefix_flush_lock(prefix) {
            Ok(lock) => lock,
            Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
        };
        let _flush_guard = flush_lock.lock().await;

        // Group pending by topition (arrival order preserved within a topition).
        let mut grouped: BTreeMap<Topition, Vec<usize>> = BTreeMap::new();
        for (index, pending) in buffer.pending.iter().enumerate() {
            grouped
                .entry(pending.topition.clone())
                .or_default()
                .push(index);
        }

        // Per-flush nonce, stamped into the footer (#89 self-recognition).
        let nonce = rng().random::<u64>();

        // Arm attribution for the exhaustion terminal (#157). Production runs at
        // warn level, where the per-attempt `debug!`s below are invisible, so the
        // give-up log must itself say which race was lost — and whether the
        // candidate ever moved: an advancing candidate is genuine multi-writer
        // contention, a stalled one is a sequence this writer cannot see as taken.
        let started = tokio::time::Instant::now();
        let mut conflicts = 0usize;
        let mut ambiguous_lost = 0usize;
        let mut stalled = 0usize;
        let mut last_candidate: Option<u64> = None;

        // Which of the two budget guards ended the loop, for the counter (#401).
        // `attempts` is the third: `MAX_ATTEMPTS` reached without either clock
        // guard firing, which no production sample has ever shown.
        let mut spent = "attempts";

        // Where the budget actually went (#192). `conflicts`/`ambiguous_lost`/
        // `stalled` say why the loop *retried*; without these, a flush starved by
        // one slow PUT and a flush losing races look identical in the log, which
        // is what made #192 read as contention when contention was near zero.
        let mut attempts_made = 0usize;
        let mut slowest_attempt = Duration::ZERO;

        // The sequence a peer's create was proved to hold, carried into the next
        // attempt so it can fold that footer instead of re-proving the tail
        // (#401). See the fold below.
        let mut won_by_peer: Option<u64> = None;
        let mut put_elapsed = Duration::ZERO;
        let mut put_bytes = 0u64;
        let mut backoff_elapsed = Duration::ZERO;

        for attempt in 0..MAX_ATTEMPTS {
            // Two departures from a plain `elapsed >= budget` (#192):
            //
            // - Never end the loop before `MIN_FLUSH_ATTEMPTS` real attempts.
            //   Surrendering rejects the produce, and with one attempt costing
            //   seconds a 10s budget otherwise gives up after a single lost race
            //   — yielding to a competitor that, at `conflicts == 1`, may not
            //   exist.
            // - Do not *start* an attempt the slowest observed attempt says
            //   cannot finish inside the budget. Checking only between attempts
            //   let a single attempt overshoot by ~90% (18.4s against 10s), so
            //   the budget bounded nothing. Deliberately not a timeout around the
            //   attempt: cancelling mid-PUT manufactures the ambiguous-create
            //   case the arms below exist to resolve.
            if attempts_made >= Self::MIN_FLUSH_ATTEMPTS {
                let elapsed = started.elapsed();
                if elapsed >= max_elapsed {
                    spent = "elapsed";
                    break;
                }
                if elapsed + slowest_attempt > max_elapsed {
                    spent = "projected";
                    break;
                }
            }

            let attempt_started = tokio::time::Instant::now();
            attempts_made += 1;

            // Fold-before-claim: observe every live segment so the candidate
            // sequence and the derived bases reflect all writers, not a stale
            // view.
            //
            // On a retry after a lost create the full refresh is more than the
            // situation needs (#401). The PUT already *proved* which sequence a
            // peer holds, so the only new information is that segment's footer:
            // folding it advances `folded_max` by one and the next candidate
            // follows, without the tail probe's absence chain or the
            // always-fresh seq-floor read that makes an absence a proof. One
            // ranged GET where the refresh costs three or four round trips —
            // and the fleet's exhaustion samples spend 88 % of a flush neither
            // in the PUT nor in the backoff, so the round trips are the budget.
            //
            // A fold that does not land falls through to the refresh, so the
            // worst case is today's cost, and the `stalled` diagnostics still
            // see a sequence this writer cannot resolve.
            let folded = match won_by_peer.take() {
                Some(seq) => self.fold_segment_footer(prefix, seq).await,
                None => false,
            };

            if !folded && let Err(error) = self.refresh_prefix_index_forced(prefix).await {
                return Self::fail_prefix_flush(buffer, error, prefix);
            }
            // Derive the candidate from the index the forced refresh just folded
            // — no second LIST per attempt (#91).
            let candidate = match self.tail_next_seq_folded(prefix).await {
                Ok(seq) => seq,
                Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
            };

            if last_candidate == Some(candidate) {
                stalled += 1;
            }
            last_candidate = Some(candidate);

            // Seed / read the leaseless era epoch stamped into this segment
            // (#92). Computed after the fold above, so it out-epochs every
            // pre-cutover lease-era segment; cached, so only the first flush of a
            // prefix pays the seeding round-trip.
            let era = match self.seed_era_epoch(prefix).await {
                Ok(era) => era,
                Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
            };

            // Re-derive each sub-stream's base from the folded index, and
            // classify each idempotent batch against the log-folded
            // `ProducerTable` (#88): only an in-order batch is admitted and
            // consumes a fresh offset; a batch already durable in the log is
            // acked with its original offset (duplicate) and not re-appended; a
            // gap or a fenced epoch is rejected. `outcomes[index]` is what the
            // parked producer is acked with — recomputed every attempt, so a
            // batch that raced in through a peer (folded on the conflict retry)
            // flips to a duplicate acked with the winner's offset, closing the
            // cross-pod dedup window.
            let mut substreams: Vec<SubstreamWrite> = Vec::with_capacity(grouped.len());
            let mut outcomes: Vec<Result<i64>> = vec![Ok(0); buffer.pending.len()];
            let mut advances: Vec<(Topition, i64)> = Vec::with_capacity(grouped.len());

            for (topition, indices) in &grouped {
                // Every batch grouped under one topition carries the same
                // identity — it comes from the topic's immutable pin (#442), not
                // from anything per-batch — so the group's is the first one's. A
                // group is never empty: `grouped` is built by pushing indices.
                let Some(substream) = indices
                    .first()
                    .map(|&index| buffer.pending[index].substream.clone())
                else {
                    continue;
                };

                let base = match self.leaseless_base(prefix, &substream, topition).await {
                    Ok(base) => base,
                    Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
                };

                // Working `ProducerTail`s for this sub-stream: seeded from the
                // fold, then advanced by each batch we admit ahead of another in
                // the same flush (the in-flight reservations).
                let mut tails: BTreeMap<i64, ProducerTail> = BTreeMap::new();

                let mut running = base;
                let mut batches = Vec::with_capacity(indices.len());
                for &index in indices {
                    // Copy the scalars first so no borrow of `buffer` is held
                    // across the fallible fold below.
                    let (producer_id, epoch, base_seq, last_offset_delta, is_idempotent) = {
                        let batch = &buffer.pending[index].batch;
                        (
                            batch.producer_id,
                            batch.producer_epoch,
                            batch.base_sequence,
                            batch.last_offset_delta,
                            batch.is_idempotent(),
                        )
                    };
                    let records = last_offset_delta as i64 + 1;

                    if is_idempotent {
                        let tail = match tails.entry(producer_id) {
                            Entry::Occupied(occupied) => occupied.into_mut(),
                            Entry::Vacant(vacant) => {
                                let folded = match self.producer_tail_folded(
                                    prefix,
                                    &substream,
                                    topition,
                                    producer_id,
                                ) {
                                    Ok(tail) => tail,
                                    Err(error) => {
                                        return Self::fail_prefix_flush(buffer, error, prefix);
                                    }
                                };
                                vacant.insert(folded)
                            }
                        };

                        match tail.classify(epoch, base_seq) {
                            IdempotentClass::Admit => {
                                let last_seq = base_seq.wrapping_add(last_offset_delta);
                                outcomes[index] = Ok(running);
                                tail.fold(epoch, base_seq, last_seq, running);
                                running += records;
                                batches.push(buffer.pending[index].batch.clone());
                            }
                            IdempotentClass::Duplicate(offset) => {
                                outcomes[index] = Ok(offset);
                            }
                            IdempotentClass::OutOfOrder => {
                                outcomes[index] =
                                    Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber));
                            }
                            IdempotentClass::Fenced => {
                                outcomes[index] = Err(Error::Api(ErrorCode::ProducerFenced));
                            }
                        }
                    } else {
                        outcomes[index] = Ok(running);
                        running += records;
                        batches.push(buffer.pending[index].batch.clone());
                    }
                }

                if !batches.is_empty() {
                    advances.push((topition.clone(), running));
                    substreams.push(SubstreamWrite {
                        topition: topition.clone(),
                        substream,
                        base_offset: base,
                        batches,
                    });
                }
            }

            // Every batch was a duplicate / rejected: ack the resolved outcomes
            // without writing an empty segment or burning a sequence.
            if substreams.is_empty() {
                return Self::ack_leaseless_outcomes(buffer, outcomes);
            }

            // Encode a v3 segment stamped with the leaseless era epoch (#92) and
            // try to create it at `candidate`.
            let (payload, footer) = match self.encode_segment_indexed(
                &substreams,
                era,
                nonce,
                self.tuning.segment_format_version,
            ) {
                Ok(encoded) => encoded,
                Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
            };

            let put_started = tokio::time::Instant::now();
            put_bytes += payload.content_length() as u64;
            let put_result = self
                .object_store
                .put_opts(
                    &self.segment_location(prefix, candidate),
                    payload,
                    PutOptions {
                        mode: PutMode::Create,
                        attributes: Attributes::new(),
                        ..Default::default()
                    },
                )
                .await;
            put_elapsed += put_started.elapsed();

            // Resolve the PUT, including the ambiguous case, through the one
            // definition shared with compaction (#286) — see
            // [`Self::resolve_segment_create`].
            match self
                .resolve_segment_create(prefix, candidate, nonce, put_result)
                .await
            {
                // Won the sequence — this create is the linearization point.
                SegmentCreate::Won => {
                    _ = self
                        .set_seq(prefix, candidate + 1)
                        .inspect_err(|err| debug!(?err));
                    return self
                        .finalize_prefix_flush_leaseless(
                            prefix, candidate, footer, buffer, outcomes, &advances,
                        )
                        .await;
                }

                // A peer took `candidate`: fold it and retry the next free
                // sequence with re-derived bases.
                SegmentCreate::Lost { ambiguous } => {
                    if ambiguous {
                        ambiguous_lost += 1;
                    } else {
                        conflicts += 1;
                    }

                    // A peer holds `candidate` — `resolve_segment_create` has
                    // established that for both arms, the ambiguous one by
                    // reading a footer that was not ours. So the next attempt
                    // folds that one segment rather than re-proving the tail
                    // (#401).
                    won_by_peer = Some(candidate);
                    slowest_attempt = slowest_attempt.max(attempt_started.elapsed());
                    FLUSH_CAS_CONFLICTS.add(1, &[]);
                    // Yield briefly, jittered (#157): N replicas flushing this
                    // prefix would otherwise re-LIST and re-PUT in lockstep, both
                    // amplifying requests on the busiest prefix and letting one
                    // writer lose every attempt of its budget to the same peers.
                    let backoff = cas_conflict_backoff(attempt);
                    backoff_elapsed += backoff;
                    sleep(backoff).await;
                    continue;
                }

                // The create did not land and cannot be claimed: fail for a
                // client retry, which log-based dedup (#88) makes safe, instead
                // of spinning the attempt budget against a throttling bucket.
                SegmentCreate::Failed(error) => {
                    return Self::fail_prefix_flush(buffer, error, prefix);
                }
            }
        }

        FLUSH_CAS_EXHAUSTED.add(
            1,
            &[
                KeyValue::new("prefix", prefix.to_string()),
                KeyValue::new("spent", spent),
            ],
        );
        // Arm-attributed at error level so production (warn) can tell the modes
        // apart without a debug bump (#157): `conflicts` = peers won the create,
        // `ambiguous_lost` = the PUT was ambiguous and a peer's footer was there,
        // `stalled` = the re-derived candidate did not move (a sequence taken by
        // an object this writer cannot resolve).
        //
        // The second group says where the *time* went (#192), which the first
        // cannot: compare `put_ms` against `elapsed_ms` to separate a flush
        // starved by slow PUTs from one losing races, and `slowest_attempt_ms`
        // against `budget_ms` to see whether the budget could ever have admitted
        // another attempt. `attempts` bounds both — the loop cannot iterate
        // without incrementing one of the three counters above, so a small
        // `attempts` with a large `elapsed_ms` is latency, not contention.
        error!(
            prefix,
            conflicts,
            ambiguous_lost,
            stalled,
            ?last_candidate,
            elapsed_ms = started.elapsed().as_millis(),
            attempts = attempts_made,
            put_ms = put_elapsed.as_millis(),
            put_bytes,
            backoff_ms = backoff_elapsed.as_millis(),
            slowest_attempt_ms = slowest_attempt.as_millis(),
            budget_ms = max_elapsed.as_millis(),
            spent,
            "leaseless flush exhausted retries"
        );
        // Retriable: exhaustion here is pure create-CAS contention (a transport
        // error fails fast retriably above), so tell the client to back off and
        // retry rather than dropping the batch on a fatal code (#6/#129).
        //
        // There is deliberately no fallback to a leased write here (#401's
        // direction 2): the produce lease was removed in #177 and the create-CAS
        // *is* the offset arbiter, so there is no other path to take. What makes
        // this terminal rarer is fitting more attempts inside the same budget,
        // which is what the winner-fold above does.
        Self::fail_prefix_flush(buffer, Error::Api(ErrorCode::KafkaStorageError), prefix)
    }

    /// The next offset for `topition` under the leaseless path (#86), derived from
    /// the already force-folded prefix index: the epoch-fenced segment tail, this
    /// process's hint, and the persisted floor, all three folded with `max` so an
    /// offset is never reused.
    /// `prefix` is the flush's (routed, #175) buffer key, threaded through
    /// rather than re-derived so the base is read from exactly the segment set
    /// the flush is about to append to.
    pub(super) async fn leaseless_base(
        &self,
        prefix: &str,
        substream: &Substream,
        topition: &Topition,
    ) -> Result<i64> {
        let segment_tail = self
            .valid_substream_segments(prefix, substream, topition.partition())?
            .last()
            .map(FencedSegment::end)
            .unwrap_or(0);
        let cached = self.cached_high(topition)?.unwrap_or(0);

        // Fold the persisted floor **unconditionally** (#287), matching
        // `recover_substream_next_offset` and `docs/design-multiwriter-segments.md`
        // step 2. It used to be folded only when `segment_tail.max(cached)` was
        // zero, on the reasoning that a non-zero tail already knows the log end.
        // It does not: `expire_prefix_segments` can reclaim a sub-stream's
        // *tail-holding* segment while a lower-offset one survives — a shared
        // segment kept alive by a hot sibling topic, or simply a batch whose
        // timestamp is older than its predecessor's. A replica that then rebuilds
        // its index from a listing sees a non-zero tail that under-reports the log
        // end, skipped the floor, and re-assigned acknowledged offsets. Silent
        // offset reuse: consumers see one offset carrying two different payloads.
        //
        // The floor is the only surviving record of those offsets, which is why
        // expiry writes it write-ahead of the delete.
        //
        // Cost: `persisted_high` goes through the cached `OptiCon<Watermark>`
        // handle, so this is a conditional GET that answers 304 while the
        // watermark is unchanged — which is almost always, since only expiry and
        // truncation move it. One revalidation round trip per flush, against a
        // flush that already does a LIST and at least one create-CAS PUT. It is
        // *not* the full GET per flush that the conditional appeared to be
        // avoiding, which is why no memo is needed here.
        //
        // A per-process memo was considered and rejected: it would be unsound in
        // exactly the case this fixes. The obvious memo keys off `cached_high`,
        // but that hint reflects only *this* replica's writes (see `set_high`),
        // so a warm replica whose peer produced the offsets that expiry then
        // reclaimed would hold a memo below the floor and reuse them anyway.
        //
        // The legacy tail is no longer folded (#179): it guarded against a
        // `records/` object sitting above the segment tail, which nothing can
        // create.
        let floor = self.persisted_high(topition).await.unwrap_or(0);

        Ok(segment_tail.max(cached).max(floor))
    }

    /// Build the folded [`ProducerTail`] for `(topition, producer_id)` from the
    /// cached prefix index (#88) — no object requests; the leaseless flush
    /// force-folds the index first. Coordinates fold in log order: segments
    /// ascending by base offset (epoch-deduped by [`Self::valid_substream_segments`]),
    /// and producers in offset order within each segment. Because this is a pure
    /// function of the folded footer set, two replicas that have observed the
    /// same segments derive an identical tail — the property that makes the
    /// dedup state converge across a connection migration.
    ///
    /// Transaction-marker (control) coordinates are skipped (#174): a marker
    /// carries `base_sequence = last_sequence = -1`, so folding it would set
    /// `next_sequence` to `seq_increment(-1) = 0` and mark the tail seen —
    /// misclassifying the producer's genuine next in-order data batch as
    /// `OutOfOrder`. Markers are placement metadata in the footer, never part
    /// of the idempotent sequence stream. Transactional *data* coordinates
    /// carry real sequences and fold normally — they are the dedup authority
    /// for those batches.
    pub(super) fn producer_tail_folded(
        &self,
        prefix: &str,
        substream: &Substream,
        topition: &Topition,
        producer_id: i64,
    ) -> Result<ProducerTail> {
        let mut tail = ProducerTail::default();
        for fenced in self.valid_substream_segments(prefix, substream, topition.partition())? {
            for pc in &fenced.entry.producers {
                if !pc.folds() {
                    continue;
                }
                if pc.producer_id == producer_id {
                    tail.fold(
                        pc.producer_epoch,
                        pc.base_sequence,
                        pc.last_sequence,
                        fenced.entry.base_offset + pc.offset_delta as i64,
                    );
                }
            }
        }
        Ok(tail)
    }

    /// Leaseless finalization (#88): like [`Self::finalize_prefix_flush`] but acks
    /// each parked producer with its *per-batch* idempotent outcome — the assigned
    /// offset (admitted), the original offset (duplicate), or an
    /// `OutOfOrderSequenceNumber` / `ProducerFenced` error — rather than one
    /// assigned offset for all. Called only after the segment PUT is durable.
    pub(super) async fn finalize_prefix_flush_leaseless(
        &self,
        prefix: &str,
        seq: u64,
        footer: SegmentFooter,
        buffer: PrefixCoalesceBuffer,
        outcomes: Vec<Result<i64>>,
        advances: &[(Topition, i64)],
    ) {
        _ = self
            .index_insert(prefix, seq, footer, Self::now_ms())
            .inspect_err(|err| debug!(?err));
        SEGMENT_FLUSHES.add(1, &[]);
        debug!(prefix, seq, "leaseless prefix segment flushed");

        // The write is durable — advance each admitted sub-stream's hint.
        for (topition, high) in advances {
            _ = self
                .set_high(topition, *high)
                .inspect_err(|err| debug!(?err));
        }

        Self::ack_leaseless_outcomes(buffer, outcomes);
    }

    /// Ack every parked producer in a leaseless flush with its classified
    /// idempotent outcome (#88): `Ok(offset)` for an admitted or duplicate batch,
    /// `Err(code)` for an out-of-order or fenced one.
    pub(super) fn ack_leaseless_outcomes(buffer: PrefixCoalesceBuffer, outcomes: Vec<Result<i64>>) {
        for (index, pending) in buffer.pending.into_iter().enumerate() {
            let outcome = outcomes
                .get(index)
                .cloned()
                .unwrap_or_else(|| Err(Error::Api(ErrorCode::UnknownServerError)));
            _ = pending.ack.send(outcome);
        }
    }
}
