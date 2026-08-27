// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! The create-only segment-sequence CAS as compaction drives it
//! (`assign_and_create_segment`), and in particular what it does with an
//! *ambiguous* PUT — one whose response was lost after the object may already
//! have landed durably.
//!
//! The leaseless flush has resolved that case since #89 by probing the footer
//! at the claimed sequence and adopting the object iff it carries the writer's
//! own nonce. Compaction ran a second, drifted copy of the same protocol that
//! treated every ambiguous PUT as a plain error: a merged segment that HAD
//! landed was retried as a failure and its whole payload re-uploaded, which is
//! the #130 write amplification. #286 made the two one definition
//! (`resolve_segment_create`); these tests pin the behaviour so it stays that
//! way.

use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    memory::InMemory, path::Path,
};
use tansu_sans_io::record::{Record, deflated, inflated};

use crate::{
    Result, Topition,
    dynostore::{DynoStore, SegmentCreateRole, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "connector-a";

/// Serves everything normally, but makes the first `ambiguous` segment creates
/// **land and then fail**: the payload is written to the inner store and a
/// transport-shaped error is returned anyway. That is exactly the case #89
/// exists for — the object is durable, the writer does not know it.
///
/// Counts every segment create attempt, so a test can assert the payload was
/// not re-uploaded.
struct AmbiguousSegmentCreates {
    inner: InMemory,
    remaining: AtomicUsize,
    attempts: Arc<AtomicUsize>,
}

impl AmbiguousSegmentCreates {
    fn new(ambiguous: usize, attempts: Arc<AtomicUsize>) -> Self {
        Self {
            inner: InMemory::new(),
            remaining: AtomicUsize::new(ambiguous),
            attempts,
        }
    }

    fn lost_response() -> object_store::Error {
        object_store::Error::Generic {
            store: "AmbiguousSegmentCreates",
            source: "connection reset by peer".into(),
        }
    }
}

impl Debug for AmbiguousSegmentCreates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmbiguousSegmentCreates").finish()
    }
}

impl Display for AmbiguousSegmentCreates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmbiguousSegmentCreates").finish()
    }
}

fn is_segment_create(location: &Path, options: &PutOptions) -> bool {
    options.mode == PutMode::Create && location.as_ref().ends_with(".seg")
}

#[async_trait]
impl ObjectStore for AmbiguousSegmentCreates {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if !is_segment_create(location, &options) {
            return self.inner.put_opts(location, payload, options).await;
        }

        _ = self.attempts.fetch_add(1, Ordering::SeqCst);

        let ambiguous = self
            .remaining
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();

        let outcome = self.inner.put_opts(location, payload, options).await;

        match (ambiguous, outcome) {
            // The create landed; the response did not come back.
            (true, Ok(_)) => Err(Self::lost_response()),
            (_, outcome) => outcome,
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> futures::stream::BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Fails every segment create with a transport-shaped error *without* writing
/// anything: the create genuinely did not land.
struct FailedSegmentCreates {
    inner: InMemory,
    attempts: Arc<AtomicUsize>,
}

impl FailedSegmentCreates {
    fn new(attempts: Arc<AtomicUsize>) -> Self {
        Self {
            inner: InMemory::new(),
            attempts,
        }
    }
}

impl Debug for FailedSegmentCreates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FailedSegmentCreates").finish()
    }
}

impl Display for FailedSegmentCreates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FailedSegmentCreates").finish()
    }
}

#[async_trait]
impl ObjectStore for FailedSegmentCreates {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if is_segment_create(location, &options) {
            _ = self.attempts.fetch_add(1, Ordering::SeqCst);
            return Err(AmbiguousSegmentCreates::lost_response());
        }

        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> futures::stream::BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A non-idempotent batch of `records` records.
fn batch(records: usize) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder();

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("record-{i}").as_bytes(),
        ))));
    }

    builder
        .last_offset_delta(records as i32 - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// A merged segment as compaction would encode one, plus its nonce.
fn merged(storage: &DynoStore, nonce: u64) -> Result<PutPayload> {
    let substreams = vec![(Topition::new("alpha", 0), 0, vec![batch(4)?])];
    storage
        .encode_segment_v3(&substreams, 0, nonce)
        .map(|(payload, _)| payload)
}

/// An ambiguous PUT whose object landed is **adopted** via the nonce: the
/// sequence it claimed is returned, and the payload is not written twice.
///
/// Before #286 compaction fell into `Err(err) => return Err(err.into())` here,
/// so a merged segment that was already durable came back as a failure. The
/// pass then retried from scratch on the next tick and re-uploaded the whole
/// merged payload — with the object it had actually written still sitting at
/// its sequence, to be resolved away by the overlap resolver.
#[tokio::test]
async fn an_ambiguous_create_that_landed_is_adopted_via_its_nonce() -> Result<()> {
    let _guard = init_tracing()?;

    let attempts = Arc::new(AtomicUsize::new(0));
    let storage = DynoStore::new(
        CLUSTER,
        NODE,
        AmbiguousSegmentCreates::new(1, attempts.clone()),
    );

    let nonce = 0x5EED_u64;
    let payload = merged(&storage, nonce)?;

    let seq = storage
        .assign_and_create_segment(PREFIX, payload, nonce, SegmentCreateRole::Compaction)
        .await?;

    assert_eq!(0, seq, "the ambiguous create won sequence 0");
    assert_eq!(
        1,
        attempts.load(Ordering::SeqCst),
        "an adopted create must not re-upload its payload (#130)"
    );

    // Exactly one segment object exists: the one the ambiguous PUT landed.
    assert!(
        storage
            .object_store
            .head(&storage.segment_location(PREFIX, 0))
            .await
            .is_ok()
    );

    Ok(())
}

/// A *peer's* object at the claimed sequence is not ours whatever the PUT said:
/// fold it in and claim the next sequence. The transport error was moot.
#[tokio::test]
async fn an_ambiguous_create_lost_to_a_peer_claims_the_next_sequence() -> Result<()> {
    let _guard = init_tracing()?;

    let attempts = Arc::new(AtomicUsize::new(0));
    let storage = DynoStore::new(
        CLUSTER,
        NODE,
        AmbiguousSegmentCreates::new(0, attempts.clone()),
    );

    // A peer already holds sequence 0, carrying its own (different) nonce.
    let peer = merged(&storage, 0xBEEF)?;
    _ = storage
        .object_store
        .put_opts(
            &storage.segment_location(PREFIX, 0),
            peer,
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await?;

    let nonce = 0xFEED_u64;
    let payload = merged(&storage, nonce)?;

    let seq = storage
        .assign_and_create_segment(PREFIX, payload, nonce, SegmentCreateRole::Compaction)
        .await?;

    assert_eq!(1, seq, "the peer holds 0, so the resync claims 1");

    Ok(())
}

/// A create that did not land is a storage error, not a silent success: nothing
/// carries our nonce, so there is nothing to adopt. The error is surfaced for a
/// retry rather than spun against the attempt budget.
#[tokio::test]
async fn an_ambiguous_create_that_did_not_land_is_an_error() -> Result<()> {
    let _guard = init_tracing()?;

    let attempts = Arc::new(AtomicUsize::new(0));
    let storage = DynoStore::new(CLUSTER, NODE, FailedSegmentCreates::new(attempts.clone()));

    let nonce = 0xC0FFEE_u64;
    let payload = merged(&storage, nonce)?;

    assert!(
        storage
            .assign_and_create_segment(PREFIX, payload, nonce, SegmentCreateRole::Compaction)
            .await
            .is_err()
    );

    assert_eq!(
        1,
        attempts.load(Ordering::SeqCst),
        "a create that did not land fails at once, it does not burn the budget"
    );

    Ok(())
}

/// A sequence freed by a peer's retire is never re-created, even when this
/// process's hint still points into the range (#432).
///
/// The hint is a plain `BTreeMap` that `set_seq` only ever raises, and nothing
/// else in the file touches `segment_seqs` — so it is monotonic *within a
/// process* and blind to every other one. A peer's `retire_segments` raises the
/// durable floor write-ahead of the delete (#77) and frees every name below it;
/// this process's hint still names one of them, and `PutMode::Create` there
/// **succeeds**, because the CAS proves the name is unoccupied and not that it
/// is fresh.
///
/// What that costs is not a lost write but a wrong read: every replica still
/// caching the retired segment's footer under that name serves it against the
/// reborn object. #77's own comment predicted the symptom verbatim, and #397's
/// discriminator found it in the bucket — an index entry claiming a 793-byte
/// region of a topic the object does not hold, over a healthy 10 220-byte frame
/// belonging to another.
#[tokio::test]
async fn a_create_never_targets_a_sequence_at_or_below_the_persisted_floor() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let storage = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let peer = DynoStore::new(CLUSTER, NODE, bucket.clone());

    // This replica wins 0, 1, 2, so its hint sits at 3.
    for nonce in 0..3u64 {
        assert_eq!(
            nonce,
            storage
                .assign_and_create_segment(
                    PREFIX,
                    merged(&storage, nonce)?,
                    nonce,
                    SegmentCreateRole::Compaction,
                )
                .await?
        );
    }
    assert_eq!(Some(3), storage.cached_seq(PREFIX)?);

    // A peer carries the prefix on to 5, which this replica never observes.
    for nonce in 3..6u64 {
        assert_eq!(
            nonce,
            peer.assign_and_create_segment(
                PREFIX,
                merged(&peer, nonce)?,
                nonce,
                SegmentCreateRole::Compaction,
            )
            .await?
        );
    }

    // The peer then merges the whole run away: floor write-ahead of the delete,
    // so 0..=5 are unoccupied names above which nothing may be created.
    assert_eq!(6, peer.retire_segments(PREFIX, &[0, 1, 2, 3, 4, 5]).await?);
    assert_eq!(6, storage.read_seq_floor(PREFIX).await?);

    // The hint is unmoved and now points *into* the freed range — the state the
    // fleet was in.
    assert_eq!(Some(3), storage.cached_seq(PREFIX)?);

    let seq = storage
        .assign_and_create_segment(
            PREFIX,
            merged(&storage, 6)?,
            6,
            SegmentCreateRole::Compaction,
        )
        .await?;

    assert_eq!(
        6, seq,
        "a create must land above the floor, not reuse the freed name its hint names"
    );

    // And nothing was written at the freed name, so no replica's cached footer
    // for it can alias onto a new object.
    assert!(
        bucket
            .head(&storage.segment_location(PREFIX, 3))
            .await
            .is_err(),
        "sequence 3 was freed and must stay free"
    );

    Ok(())
}

/// The floor is read live, because the certified one can be arbitrarily stale
/// against a peer (#432).
///
/// This is the test that justifies not taking `certified_seq_floor` here, which
/// is what the issue proposed. That cache is keyed on the prefix index's
/// generation, and `index_insert` — the writer fast path every create takes —
/// does not bump it. So a replica that creates and neither lists nor prunes
/// keeps whatever floor it last certified, however long ago, and a peer's raise
/// stays invisible to it for that whole time.
///
/// Certify a floor, let a peer raise it well past this process's hint, and the
/// next create must respect the raise rather than the certification.
#[tokio::test]
async fn a_peers_raise_is_respected_even_when_this_process_certified_an_older_floor() -> Result<()>
{
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let storage = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let peer = DynoStore::new(CLUSTER, NODE, bucket.clone());

    for nonce in 0..3u64 {
        _ = storage
            .assign_and_create_segment(
                PREFIX,
                merged(&storage, nonce)?,
                nonce,
                SegmentCreateRole::Compaction,
            )
            .await?;
    }
    assert_eq!(Some(3), storage.cached_seq(PREFIX)?);

    // Certify this process's view of the floor at its current value. Nothing
    // after this lists or prunes on this replica, so nothing invalidates it.
    assert_eq!(0, storage.certified_seq_floor(PREFIX).await?);
    assert_eq!(Some(0), storage.cached_certified_seq_floor(PREFIX)?);

    // The peer carries the prefix past this replica's hint and then retires the
    // whole run, raising the floor to 5.
    for nonce in 3..5u64 {
        _ = peer
            .assign_and_create_segment(
                PREFIX,
                merged(&peer, nonce)?,
                nonce,
                SegmentCreateRole::Compaction,
            )
            .await?;
    }
    assert_eq!(5, peer.retire_segments(PREFIX, &[0, 1, 2, 3, 4]).await?);

    // The stale certification survives the peer's raise — which is exactly why
    // the create cannot be built on it.
    assert_eq!(Some(0), storage.cached_certified_seq_floor(PREFIX)?);
    assert_eq!(5, storage.read_seq_floor(PREFIX).await?);

    let seq = storage
        .assign_and_create_segment(
            PREFIX,
            merged(&storage, 9)?,
            9,
            SegmentCreateRole::Compaction,
        )
        .await?;

    assert_eq!(
        5, seq,
        "a create must respect a peer's raise, not this process's certification"
    );

    Ok(())
}
