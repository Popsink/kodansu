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

//! Segment objects: how they are named, the durable sequence floor (#77), the
//! era epoch (#92), and the create-only CAS that assigns the next sequence.

use super::*;

impl SegmentCreateRole {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Compaction => "compaction",
        }
    }
}

impl DynoStore {
    /// The segment sequence a listed object's name encodes, or `None` for a name
    /// that is not one.
    ///
    /// Shared by the incremental refresh and the reconciling pass (#408) so the
    /// two cannot disagree about which listed objects are segments — a
    /// reconciler that parsed one name differently from the refresher would drop
    /// live entries.
    ///
    /// It is also what would reject an ADLS Gen2 directory entry, and it has
    /// never had to. An undelimited `List Blobs` on a hierarchical-namespace
    /// account returns directories alongside blobs, but a `segments/` directory
    /// has no directory children — [`Self::segment_location`] appends
    /// `{seq:0>20}.seg` with no `/` — so the listing is all files. Observed on a
    /// real HNS account (#417), where the level *above* returns two directory
    /// entries and `segments/` returns none. `object_store` filters
    /// `ResourceType == "directory"` in any case; that filter is load-bearing
    /// only for a layout that nests below a listing prefix.
    pub(super) fn segment_seq_of(location: &Path) -> Option<u64> {
        let name = location.parts().next_back()?;
        let name = name.as_ref();

        if name.len() < 20 {
            return None;
        }

        u64::from_str(&name[0..20]).ok()
    }

    /// The `segments/` listing prefix for a connector prefix (#57).
    pub(super) fn segment_prefix(&self, prefix: &str) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/segments/",
            self.identity.cluster, prefix,
        ))
    }

    /// The object name of the `seq`-th segment under a connector prefix (#57).
    /// The zero-padded sequence makes the name monotonic and, written
    /// create-only, the ordering authority (as `{offset}.batch` is for #50).
    ///
    /// **The name must not contain a `/` below [`Self::segment_prefix`], and that
    /// is a portability constraint rather than a style choice (#421).** On an
    /// ADLS Gen2 account with hierarchical namespace enabled, `/` sorts
    /// **lowest** — below `-` (0x2D) and `.` (0x2E), which is the opposite of
    /// ASCII, and both of which a Kafka topic name admits. Observed on a real
    /// HNS account (#417): `a/b`, `a-b`, `a.b`, `a0b` comes back in exactly that
    /// order, where S3 and GCS return `a-b`, `a.b`, `a/b`, `a0b`.
    ///
    /// Today nothing is exposed to it. `segment_prefix` runs to `…/segments/`
    /// and this appends `{seq:0>20}.seg`, so every key a `scan_from` returns
    /// shares the full prefix and the remainder has no `/` in it — there is
    /// nothing for the rule to reorder. A layout that nested below the listing
    /// prefix and relied on ordering across the separator would be correct on
    /// S3 and silently wrong on ADLS, and `scan_from` is where that lands:
    /// [`Self::refresh_prefix_index_inner`] resumes from the highest sequence it
    /// knows, so a reordering means it resumes from the wrong place and misses
    /// segments rather than erroring.
    ///
    /// The same property is why an HNS `segments/` listing contains no directory
    /// entries — see [`Self::segment_seq_of`].
    pub(super) fn segment_location(&self, prefix: &str, seq: u64) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/segments/{:0>20}.seg",
            self.identity.cluster, prefix, seq,
        ))
    }

    /// The cached next segment sequence for `prefix`, if known to this process.
    pub(super) fn cached_seq(&self, prefix: &str) -> Result<Option<u64>> {
        self.prefixes.seq(prefix)
    }

    /// Advance the cached next-segment-sequence hint. Monotonic, like
    /// [`Self::set_high`]: a sequence is never reused.
    pub(super) fn set_seq(&self, prefix: &str, next: u64) -> Result<()> {
        self.prefixes.set_seq(prefix, next)
    }

    /// The durable sequence-floor object for `prefix` (#77).
    pub(super) fn seq_floor_location(&self, prefix: &str) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/seq-floor.json",
            self.identity.cluster, prefix,
        ))
    }

    /// Read the persisted next-sequence floor for `prefix` (#77); `0` when absent.
    pub(super) async fn read_seq_floor(&self, prefix: &str) -> Result<u64> {
        match self
            .object_store
            .get(&self.seq_floor_location(prefix))
            .await
        {
            Ok(result) => {
                Ok(serde_json::from_slice::<SeqFloor>(&result.bytes().await?)?.next_seq_floor)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    /// The persisted next-sequence floor for `prefix`, *certified against the
    /// current index generation*: served from the in-memory prefix index when
    /// it was read at-or-after the listing the current generation stands for,
    /// otherwise re-read with one GET (per prefix, not per partition) and
    /// cached under that generation.
    ///
    /// Why this certifies the LATEST fast path: the floor is raised
    /// write-ahead of *every* segment delete (segments are deleted only through
    /// [`Self::retire_segments`], which raises this floor first), and
    /// `expire_prefix_segments` persists each
    /// affected sub-stream's tail into `watermark.high` *before* that raise.
    /// So `watermark.high` of a prefix-coalesced sub-stream can only advance
    /// in an operation that subsequently raises this floor. A floor value read
    /// after our latest listing therefore covers every watermark advance whose
    /// segment deletion that listing could have reflected — if the floor has
    /// not risen since a `watermark.json` read, that read is still the current
    /// high floor, and no per-partition conditional GET is needed.
    ///
    /// Generation-checked commit: the GET is issued after capturing the
    /// generation, and the result is cached only if no listing/prune committed
    /// meanwhile — a stale read can never be certified against a newer view.
    /// The un-cached value is still returned: it is valid for the caller's own
    /// (older-generation) segment snapshot, whose tails a newer prune can only
    /// have kept or removed, never advanced.
    pub(super) async fn certified_seq_floor(&self, prefix: &str) -> Result<u64> {
        // Lock-free fast path: a floor already certified for the current
        // generation is served from memory.
        if let Some(floor) = self.cached_certified_seq_floor(prefix)? {
            return Ok(floor);
        }

        // Single-flight the sync per prefix (same lock as the index refresh):
        // concurrent stale readers re-check under the lock and are served by
        // the winner's GET instead of issuing N duplicates.
        let sync = self.prefix_read_sync_lock(prefix)?;
        let _guard = sync.lock().await;

        if let Some(floor) = self.cached_certified_seq_floor(prefix)? {
            return Ok(floor);
        }

        let generation = self
            .prefixes
            .index()?
            .get(prefix)
            .map(|entry| entry.generation)
            .unwrap_or_default();

        let floor = self.read_seq_floor(prefix).await?;

        {
            let mut index = self.prefixes.index()?;
            let entry = index.entry(prefix.to_owned()).or_default();
            // A prune can bump the generation without taking the single-flight
            // lock, so commit only if no such loss happened during the GET — a
            // stale read must never be certified against a newer view. The
            // value is still returned: it is valid for the caller's own
            // segment snapshot.
            if entry.generation == generation {
                entry.seq_floor = Some((floor, generation));
            }
        }

        Ok(floor)
    }

    /// Forget any certified seq floor cached for `prefix`, so the next
    /// [`Self::certified_seq_floor`] re-reads it.
    ///
    /// Called by [`Self::raise_seq_floor`] the moment the persisted floor moves.
    pub(super) fn invalidate_certified_seq_floor(&self, prefix: &str) -> Result<()> {
        self.prefixes.index().map(|mut index| {
            if let Some(entry) = index.get_mut(prefix) {
                entry.seq_floor = None;
            }
        })
    }

    /// The certified seq floor for `prefix` iff one is cached for the current
    /// index generation (see [`Self::certified_seq_floor`]).
    pub(super) fn cached_certified_seq_floor(&self, prefix: &str) -> Result<Option<u64>> {
        let index = self.prefixes.index()?;
        Ok(index.get(prefix).and_then(|entry| {
            entry
                .seq_floor
                .and_then(|(floor, at)| (at == entry.generation).then_some(floor))
        }))
    }

    /// Raise the persisted next-sequence floor for `prefix` to at least `floor`
    /// (#77). MUST be called write-ahead of deleting any segment, so a freed
    /// sequence name is never reused — [`Self::retire_segments`] is the one
    /// delete path and does exactly that. Max-fold CAS: a lost race means another
    /// worker wrote concurrently — re-read and, if the value already covers our
    /// floor, we are done; otherwise retry. Returns an error on persistent
    /// contention so the caller aborts the delete rather than break the invariant.
    pub(super) async fn raise_seq_floor(&self, prefix: &str, floor: u64) -> Result<()> {
        const MAX_ATTEMPTS: usize = 16;
        let location = self.seq_floor_location(prefix);

        for _ in 0..MAX_ATTEMPTS {
            let (current, version) = match self.object_store.get(&location).await {
                Ok(result) => {
                    let version = UpdateVersion {
                        e_tag: result.meta.e_tag.clone(),
                        version: result.meta.version.clone(),
                    };
                    let current =
                        serde_json::from_slice::<SeqFloor>(&result.bytes().await?)?.next_seq_floor;
                    (current, Some(version))
                }
                Err(object_store::Error::NotFound { .. }) => (0, None),
                Err(err) => return Err(err.into()),
            };

            if current >= floor {
                return Ok(());
            }

            let payload = PutPayload::from(Bytes::from(serde_json::to_vec(&SeqFloor {
                next_seq_floor: floor,
            })?));
            let mode = match &version {
                Some(version) => PutMode::Update(version.clone()),
                None => PutMode::Create,
            };

            match self
                .object_store
                .put_opts(
                    &location,
                    payload,
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => {
                    // The persisted floor moved, so any floor this process has
                    // certified is now stale. Drop it here rather than relying on
                    // the caller's later `index_prune` to bump the generation.
                    //
                    // Every raise call site does prune afterwards, but *afterwards*
                    // is the problem: between this PUT and that bump the generation
                    // is unchanged, so a concurrent `certified_seq_floor` would be
                    // served the pre-raise value. That is harmless for the LATEST
                    // fast path, which only compares watermarks, and not harmless
                    // for `tail_next_seq_folded` (#278), which picks the next
                    // sequence *name* from it — serving a floor from before the
                    // raise is how a just-freed name gets reused, the one thing #77
                    // forbids. Invalidating at the source closes the window without
                    // making anything depend on raise-then-prune ordering.
                    self.invalidate_certified_seq_floor(prefix)?;
                    return Ok(());
                }
                // Lost the CAS (create race or stale version) — re-read and retry.
                Err(object_store::Error::AlreadyExists { .. })
                | Err(object_store::Error::Precondition { .. }) => continue,
                Err(err) => return Err(err.into()),
            }
        }

        error!(prefix, floor, "seq floor CAS exhausted retries");
        // Retriable: contention exhaustion, not a permanent fault (#6/#129).
        Err(Error::Api(ErrorCode::KafkaStorageError))
    }

    /// The durable era-epoch object for `prefix` (#92).
    pub(super) fn era_location(&self, prefix: &str) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/era.json",
            self.identity.cluster, prefix,
        ))
    }

    /// The lease epoch currently recorded in `prefix`'s `lease.json` (#59); `0`
    /// when no lease object exists. Read-only — does not acquire or renew.
    pub(super) async fn read_lease_epoch(&self, prefix: &str) -> Result<i64> {
        match self.object_store.get(&self.lease_location(prefix)).await {
            Ok(result) => Ok(serde_json::from_slice::<PrefixLease>(&result.bytes().await?)?.epoch),
            Err(object_store::Error::NotFound { .. }) => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    /// The highest `writer_epoch` across `prefix`'s currently-cached segment
    /// footers; `0` when none are known. The leaseless flush force-refreshes the
    /// index (fold-before-claim) immediately before seeding, so at seed time this
    /// reflects every live pre-cutover segment.
    pub(super) fn max_footer_epoch(&self, prefix: &str) -> Result<i64> {
        Ok(self
            .prefixes
            .index()?
            .get(prefix)
            .map(|index| {
                index
                    .segments
                    .values()
                    .map(|cached| cached.footer.writer_epoch)
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0))
    }

    /// The leaseless era epoch for `prefix` (#92), seeding it on first use. The
    /// era is `max(lease epoch, max footer epoch) + 1` (never 0), so a leaseless
    /// segment strictly out-epochs every pre-cutover lease-era segment and a
    /// straggler can never win the overlap tie-break. Create-only and cached: the
    /// first writer to seed wins, and any peer racing the same prefix reads and
    /// adopts that value — so all replicas converge on one constant era. Called
    /// on the leaseless flush path *after* the forced index refresh, so
    /// `max_footer_epoch` sees every folded segment.
    pub(super) async fn seed_era_epoch(&self, prefix: &str) -> Result<i64> {
        if let Some(era) = self.prefixes.era(prefix)? {
            return Ok(era);
        }

        let location = self.era_location(prefix);

        // Already seeded durably (this process is cold, or a peer seeded first)?
        match self.object_store.get(&location).await {
            Ok(result) => {
                let era = serde_json::from_slice::<Era>(&result.bytes().await?)?.era_epoch;
                self.cache_era(prefix, era)?;
                return Ok(era);
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(err.into()),
        }

        let floor = self
            .read_lease_epoch(prefix)
            .await?
            .max(self.max_footer_epoch(prefix)?);
        let era = floor + 1;
        let payload = PutPayload::from(Bytes::from(serde_json::to_vec(&Era { era_epoch: era })?));

        match self
            .object_store
            .put_opts(
                &location,
                payload,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {
                self.cache_era(prefix, era)?;
                debug!(prefix, era, "leaseless era seeded");
                Ok(era)
            }
            // A peer seeded concurrently — adopt the durable value, not ours.
            Err(object_store::Error::AlreadyExists { .. }) => {
                let result = self.object_store.get(&location).await?;
                let era = serde_json::from_slice::<Era>(&result.bytes().await?)?.era_epoch;
                self.cache_era(prefix, era)?;
                Ok(era)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Cache a resolved era epoch (monotonic, like the other hints — the durable
    /// object is immutable, so the value can only ever be the same one).
    pub(super) fn cache_era(&self, prefix: &str, era: i64) -> Result<()> {
        self.prefixes.cache_era(prefix, era)
    }

    /// Roll `prefix` back to the lease regime (#92): rewrite `lease.json` with an
    /// epoch strictly above the seeded era, so a restarted old lease-holder — on
    /// its next acquire (which bumps `epoch + 1`) — stamps segments that
    /// out-epoch every leaseless-era segment and wins the overlap tie-break. Run
    /// per active prefix during the *reverse* quiesce-and-flip, after the
    /// leaseless fleet has drained and before old pods restart. Returns the lease
    /// epoch written.
    ///
    /// The write is a plain CAS against the current lease version (or a create),
    /// with an already-expired term so the first old pod re-acquires immediately.
    ///
    /// Operationally invoked (see `docs/migration-scos.md`); no in-tree caller
    /// until a migration CLI subcommand wires it, so allow it to stand unused.
    #[allow(dead_code)]
    pub(super) async fn rollback_prefix_to_lease(&self, prefix: &str) -> Result<i64> {
        let era = self.seed_era_epoch(prefix).await?;
        let epoch = era + 1;
        let location = self.lease_location(prefix);

        let version = match self.object_store.get(&location).await {
            Ok(result) => Some(UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            }),
            Err(object_store::Error::NotFound { .. }) => None,
            Err(err) => return Err(err.into()),
        };

        let lease = PrefixLease {
            epoch,
            holder: format!("{}-rollback", self.identity.writer_id),
            // Expired on purpose: the first restarted lease pod re-acquires at
            // once (bumping to `epoch + 1`), rather than waiting out a term.
            expires_at_ms: 0,
            maintained_at_ms: 0,
        };
        let payload = PutPayload::from(Bytes::from(serde_json::to_vec(&lease)?));
        let mode = match &version {
            Some(version) => PutMode::Update(version.clone()),
            None => PutMode::Create,
        };

        _ = self
            .object_store
            .put_opts(
                &location,
                payload,
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await?;
        debug!(prefix, epoch, era, "prefix rolled back to lease regime");
        Ok(epoch)
    }

    /// The next free segment sequence for `prefix`, read from the tail of the
    /// `segments/` listing (#57). Zero-padded names sort lexicographically by
    /// sequence, so the greatest listed name is the tail. Used to seed the hint
    /// cold and to resync after a `Create` conflict.
    pub(super) async fn tail_next_seq(&self, prefix: &str) -> Result<u64> {
        let listing = self.segment_prefix(prefix);
        let mut list_stream = self.scan(Scan::SegmentIndex, &listing);
        let mut max: Option<u64> = None;

        while let Some(meta) = list_stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, prefix))?
        {
            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };

            let name = name.as_ref();
            if name.len() < 20 {
                continue;
            }

            let Ok(seq) = u64::from_str(&name[0..20]) else {
                continue;
            };

            max = Some(max.map_or(seq, |m| m.max(seq)));
        }

        // Fold in the persisted floor (#77): a sequence name freed by
        // retention/compaction must never be reused, even when the surviving
        // listing's max has dropped below it (or the prefix listed empty).
        let floor = self.read_seq_floor(prefix).await?;
        Ok(max.map_or(0, |m| m + 1).max(floor))
    }

    /// The next free segment sequence for `prefix`, derived from the *already
    /// force-folded* in-memory index instead of a fresh LIST (#91). The leaseless
    /// flush (`fold-before-claim`) calls [`Self::refresh_prefix_index_forced`]
    /// immediately before this, so the cached segment set already reflects every
    /// live sequence — including a peer replica's seconds-old create. The full
    /// `tail_next_seq` LIST per conflict attempt was therefore redundant: it
    /// re-read what the forced refresh had just listed. Still folds the persisted
    /// seq floor (#77) so a name freed by retention/compaction is never reused.
    ///
    /// Derived from every sequence the listing *resolved* — decoded segments and
    /// undecodable objects alike ([`PrefixIndex::resolved_max`]) — because a
    /// candidate must be free in the **namespace**, not merely absent from the
    /// readable set: an occupied-but-undecodable name would otherwise be re-picked
    /// on every attempt and burn the whole create-CAS budget (#157). This matches
    /// the name-derived [`Self::tail_next_seq`] the leased/compaction path uses.
    /// Takes the floor through [`Self::certified_seq_floor`] rather than a second
    /// live GET, which is what made the steady-state flush read
    /// `seq-floor.json` twice milliseconds apart (#278).
    ///
    /// **Why one read still establishes #77.** The invariant is that a sequence
    /// name freed by retention or compaction is never reused, and it needs a
    /// floor observed *after* the tail. That ordering is preserved, in both
    /// cases and for different reasons:
    ///
    /// - **Probe resolved the tail.** There is exactly one path returning
    ///   [`TailProbe::Resolved`], and it is inside the branch that observed the
    ///   tail *absent* — which reads the floor live via
    ///   [`Self::probe_seq_floor`] and certifies it under the current
    ///   generation. So a cache hit here can only be that read: fresh, and
    ///   ordered after the absence it followed. Never an older caller's value,
    ///   because the probe overwrites it on the way out.
    /// - **Probe was inconclusive.** The forced refresh falls through to the
    ///   authoritative LIST, which bumps the generation. The cached floor is
    ///   then certified against a superseded view, so
    ///   [`Self::certified_seq_floor`] declines it and re-reads — the fallback,
    ///   taken because the generation moved rather than because anything assumed
    ///   it had not.
    ///
    /// A prune by this process also bumps the generation, and a floor raise by
    /// another replica is exactly what the live read on the inconclusive path
    /// catches. So the removed GET was redundant, not load-bearing: it could
    /// only ever re-read what the probe had just read under the same generation.
    pub(super) async fn tail_next_seq_folded(&self, prefix: &str) -> Result<u64> {
        let listed_max = self
            .prefixes
            .index()?
            .get(prefix)
            .and_then(PrefixIndex::resolved_max);
        let floor = self.certified_seq_floor(prefix).await?;
        Ok(listed_max.map_or(0, |m| m + 1).max(floor))
    }

    /// Resolve a create-only segment PUT at `candidate`, disambiguating an
    /// *ambiguous* result through the per-segment footer nonce (#89).
    ///
    /// `AlreadyExists` is unambiguous: a peer won the sequence. Any other error
    /// is ambiguous — the create may have landed durably before the response was
    /// lost — so the footer at `candidate` is probed and the object adopted iff
    /// it carries our nonce. Our nonce can only exist at a sequence our own PUT
    /// won, so a match is proof the create succeeded; blind-retrying at the next
    /// sequence would double-write the payload. A *peer's* footer means we lost
    /// the sequence exactly as in the `AlreadyExists` case and the transport
    /// error was moot. No footer at all means the create genuinely did not land
    /// (the store is read-after-write consistent, so a durable create would be
    /// visible) and a probe that itself errors leaves it unknown — both surface
    /// the storage error for a retry, which log-based dedup (#88) makes safe.
    ///
    /// One definition for the leaseless flush and for compaction (#286).
    /// Compaction used to treat every ambiguous PUT as a plain error, so a
    /// merged segment that had actually landed was retried as a failure and its
    /// whole payload re-uploaded — the #130 write amplification.
    pub(super) async fn resolve_segment_create(
        &self,
        prefix: &str,
        candidate: u64,
        nonce: u64,
        result: Result<PutResult, object_store::Error>,
    ) -> SegmentCreate {
        let error = match result {
            Ok(outcome) => {
                debug!(?outcome, prefix, candidate);
                return SegmentCreate::Won;
            }

            Err(object_store::Error::AlreadyExists { .. }) => {
                debug!(prefix, candidate, "segment seq taken, re-deriving");
                return SegmentCreate::Lost { ambiguous: false };
            }

            Err(error) => error,
        };

        match self
            .read_segment_footer(&self.segment_location(prefix, candidate))
            .await
        {
            Ok(Some(found)) if found.nonce == nonce => {
                debug!(prefix, candidate, "ambiguous PUT adopted via nonce");
                SegmentCreate::Won
            }

            Ok(Some(_)) => {
                debug!(prefix, candidate, ?error, "ambiguous PUT lost to peer");
                SegmentCreate::Lost { ambiguous: true }
            }

            Ok(None) => {
                debug!(
                    prefix,
                    candidate,
                    ?error,
                    "ambiguous PUT did not land, failing retriably"
                );
                SegmentCreate::Failed(error.into())
            }

            Err(probe_error) => {
                // A 404 here is the probe's answer, not a fault: the create did
                // not land, which is exactly what this read asks (#408). Counted
                // as its own caller so it is not mistaken for a stale index
                // entry — it scales with ambiguous PUTs, and no reconciliation
                // reduces it.
                if let Error::ObjectStore(ref inner) = probe_error
                    && matches!(**inner, object_store::Error::NotFound { .. })
                {
                    SEGMENT_ABSENT.add(1, &[KeyValue::new("caller", "create_probe")]);
                }

                debug!(
                    prefix,
                    candidate,
                    ?error,
                    ?probe_error,
                    "ambiguous PUT unresolved"
                );
                SegmentCreate::Failed(error.into())
            }
        }
    }

    /// Write `payload` — encoded with `nonce` in its footer — as the next
    /// create-only segment under `prefix`, and return its assigned sequence
    /// (#57). The create is the authority: on a lost sequence, fold and retry
    /// the next free one. There is no lease to re-validate since #177 — the
    /// leaseless arbiter (#86) makes the create-only CAS itself the arbiter, so
    /// a loser is never a fenced writer, only a slower one.
    ///
    /// The conflict protocol is the leaseless flush's, and deliberately so
    /// (#286): [`Self::resolve_segment_create`] for the ambiguous PUT, a
    /// jittered [`cas_conflict_backoff`] so N contenders do not resync in
    /// lockstep, and fold-before-claim off the in-memory index (#91) instead of
    /// a fresh `tail_next_seq` LIST per attempt.
    pub(super) async fn assign_and_create_segment(
        &self,
        prefix: &str,
        payload: PutPayload,
        nonce: u64,
        role: SegmentCreateRole,
    ) -> Result<u64> {
        /// Bounds the conflict-resync loop; far above any real contention.
        const MAX_ATTEMPTS: usize = 64;

        let attributes = [KeyValue::new("role", role.as_str())];
        let payload_len = payload.content_length() as u64;

        // Conflict accounting for the exhaustion terminal and for #130: how often
        // this role loses the shared tail-sequence race, and how many payload
        // bytes that costs in re-uploads.
        let mut conflicts = 0u64;

        // #77's invariant is that a sequence name freed by retention or
        // compaction is never reused, and it needs a floor observed *after* the
        // tail. The hint cannot supply one. `set_seq` only ever rises within
        // *this* process, and nothing else touches `segment_seqs` — so a peer's
        // `retire_segments`, which raises the durable floor write-ahead of the
        // delete and frees every name below it, is invisible here. A create at
        // such a name then **succeeds**: the create-only CAS proves the name is
        // unoccupied, which is not the same as fresh. Every replica still
        // caching the retired segment's footer under that name then serves it
        // against the reborn object — which is #432, and #77's own comment
        // predicted it verbatim.
        //
        // Read the floor live rather than through `certified_seq_floor`. That
        // cache is keyed on the index generation, and `index_insert` — the
        // writer fast path every create takes — does not bump it, so a peer's
        // raise can stay uncertified for as long as this process neither lists
        // nor prunes. A floor that is merely *usually* fresh does not establish
        // an invariant whose failure is a wrong offset.
        //
        // One GET per compaction create is what that costs, and this is not the
        // produce path: the leaseless flush derives every candidate from
        // `tail_next_seq_folded`, which has folded the floor all along. #116's
        // saving lives there and is untouched.
        let mut candidate = match self.cached_seq(prefix)? {
            Some(seq) => seq.max(self.read_seq_floor(prefix).await?),

            // Already folded: `tail_next_seq` reads the floor live, after its
            // listing.
            None => self.tail_next_seq(prefix).await?,
        };

        for attempt in 0..MAX_ATTEMPTS {
            let put_result = self
                .object_store
                .put_opts(
                    &self.segment_location(prefix, candidate),
                    payload.clone(),
                    PutOptions {
                        mode: PutMode::Create,
                        attributes: Attributes::new(),
                        ..Default::default()
                    },
                )
                .await;

            match self
                .resolve_segment_create(prefix, candidate, nonce, put_result)
                .await
            {
                SegmentCreate::Won => {
                    SEGMENT_CREATES.add(1, &attributes);
                    self.set_seq(prefix, candidate + 1)?;
                    return Ok(candidate);
                }

                SegmentCreate::Lost { .. } => {
                    // Every loss costs a re-upload of the whole payload into the
                    // same key prefix (#130).
                    conflicts += 1;
                    SEGMENT_CREATE_CONFLICTS.add(1, &attributes);
                    SEGMENT_CREATE_BYTES_REWRITTEN.add(payload_len, &attributes);

                    debug!(candidate, attempt, prefix, "segment seq taken, resyncing");

                    sleep(cas_conflict_backoff(attempt)).await;

                    // Fold-before-claim off the index the forced refresh just
                    // listed, rather than a second full LIST (#91).
                    self.refresh_prefix_index_forced(prefix).await?;
                    candidate = self.tail_next_seq_folded(prefix).await?;
                }

                SegmentCreate::Failed(error) => return Err(error),
            }
        }

        error!(
            prefix,
            candidate,
            role = role.as_str(),
            conflicts,
            payload_len,
            bytes_rewritten = conflicts * payload_len,
            "segment sequence assignment exhausted retries"
        );
        // Retriable: contention exhaustion, not a permanent fault (#6/#129).
        Err(Error::Api(ErrorCode::KafkaStorageError))
    }

    /// Read a segment's self-describing footer (#58/#64) with at most two ranged
    /// GETs of the object tail — never the record body: one `Suffix` GET of the
    /// fixed trailer to learn the footer length, then one `Suffix` GET of the
    /// footer + trailer. Returns `None` if the object carries no trailer (a
    /// legacy #50 object). This is the read primitive the fetch path (#60) also
    /// builds on.
    pub(super) async fn read_segment_footer(
        &self,
        location: &Path,
    ) -> Result<Option<SegmentFooter>> {
        // One speculative suffix GET covers the trailer and, for almost every
        // segment, the whole footer too (#112 follow-up) — halving the per-footer
        // GETs the read/refresh path pays on non-writer replicas. `decode_segment_footer`
        // reads the trailer from the end of the buffer and slices the footer just
        // before it, so leading record bytes in the over-read are ignored.
        let over_read = SEGMENT_FOOTER_OVER_READ.max(SEGMENT_TRAILER_LEN);
        let buffer = self
            .object_store
            .get_opts(
                location,
                GetOptions {
                    range: Some(GetRange::Suffix(over_read as u64)),
                    ..Default::default()
                },
            )
            .await?
            .bytes()
            .await?;

        if buffer.len() < SEGMENT_TRAILER_LEN {
            return Ok(None);
        }

        let trailer = &buffer[buffer.len() - SEGMENT_TRAILER_LEN..];
        let magic = u32::from_be_bytes(trailer[14..18].try_into()?);
        if magic != SEGMENT_MAGIC {
            return Ok(None);
        }

        let footer_len = u64::from_be_bytes(trailer[0..8].try_into()?) as usize;

        // Fast path: the over-read already holds the whole `[footer || trailer]`.
        if SEGMENT_TRAILER_LEN + footer_len <= buffer.len() {
            return Self::decode_segment_footer(&buffer);
        }

        // Rare: a footer larger than the over-read (a prefix with very many
        // sub-streams). Fetch exactly the `[footer || trailer]` suffix.
        let tail = self
            .object_store
            .get_opts(
                location,
                GetOptions {
                    range: Some(GetRange::Suffix((SEGMENT_TRAILER_LEN + footer_len) as u64)),
                    ..Default::default()
                },
            )
            .await?
            .bytes()
            .await?;

        Self::decode_segment_footer(&tail)
    }

    /// The seq floor for the tail proof (#112), **always read fresh** — the one
    /// place the certified cache ([`Self::certified_seq_floor`]) must not be used.
    /// A peer can raise the durable floor without bumping our index generation, so
    /// a certified value can be stale-low; everywhere else that only *understates*
    /// a watermark (delaying visibility, never corrupting), but here it would make
    /// `floor <= seq` hold when it does not, which is precisely the direction that
    /// would let the probe miss a segment created at a raised floor. The proof
    /// needs a floor read ordered *after* the observed absence, and only a live GET
    /// gives that.
    ///
    /// The fresh value is certified under the current generation on the way out, so
    /// the read also serves the LATEST fast path rather than being pure overhead.
    /// Inlined rather than calling `certified_seq_floor` because the probe already
    /// holds the per-prefix single-flight lock that method takes.
    pub(super) async fn probe_seq_floor(&self, prefix: &str) -> Result<u64> {
        let generation = self
            .prefixes
            .index()?
            .get(prefix)
            .map(|entry| entry.generation)
            .unwrap_or_default();

        let floor = self.read_seq_floor(prefix).await?;

        {
            let mut index = self.prefixes.index()?;
            let entry = index.entry(prefix.to_owned()).or_default();
            if entry.generation == generation {
                entry.seq_floor = Some((floor, generation));
            }
        }

        Ok(floor)
    }

    /// Retire `seqs` from `prefix`: raise the durable sequence floor past the
    /// highest of them, delete their objects, then prune them from the index.
    /// Returns the number of objects deleted.
    ///
    /// The floor write is **write-ahead of the delete** (#77): a freed sequence
    /// name must never be reused, or a peer caching the old footer would serve
    /// stale byte ranges against a reborn object. Deleting can lower the listing
    /// max, so without the persisted floor a later `tail_next_seq` would hand
    /// the freed name back out. On a floor-write error this returns before
    /// deleting anything — the caller retries on the next tick rather than break
    /// the invariant.
    ///
    /// This is the only place segment objects are deleted (#286): expiry,
    /// whole-segment compaction and per-key rewrite all retire through here, so
    /// a fourth delete path cannot get the ordering wrong by omission.
    pub(super) async fn retire_segments(&self, prefix: &str, seqs: &[u64]) -> Result<u64> {
        /// Segment objects deleted per bulk request — matches the S3
        /// `DeleteObjects` per-request key cap.
        const RETIRE_DELETE_CHUNK: usize = 1_000;

        let Some(max_seq) = seqs.iter().copied().max() else {
            return Ok(0);
        };

        self.raise_seq_floor(prefix, max_seq + 1).await?;

        let mut deleted: u64 = 0;
        let mut chunk: Vec<Path> = Vec::new();

        for seq in seqs {
            chunk.push(self.segment_location(prefix, *seq));

            if chunk.len() >= RETIRE_DELETE_CHUNK {
                deleted += chunk.len() as u64;
                self.delete_batches(std::mem::take(&mut chunk)).await?;
            }
        }

        if !chunk.is_empty() {
            deleted += chunk.len() as u64;
            self.delete_batches(chunk).await?;
        }

        self.index_prune(prefix, seqs)?;

        Ok(deleted)
    }
}
