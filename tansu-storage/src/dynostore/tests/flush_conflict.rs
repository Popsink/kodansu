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

//! What a lost create-CAS costs the next attempt (#401).
//!
//! The leaseless flush returns `KafkaStorageError` to the producer when its
//! create-CAS budget runs out, ~9 times an hour in production — and at least one
//! first-party client turns one retriable send failure into a worker restart plus
//! duplicate records downstream (Popsink/data-plane#3621).
//!
//! 224 exhaustion samples off the fleet say what the budget went on, and it is
//! not what the issue assumed:
//!
//! | | |
//! |---|---|
//! | `attempts` | **3** in 212/224, 4 in 12 |
//! | `stalled` | **0** in all 224 |
//! | `ambiguous_lost` | **0** in all 224 |
//! | `put_ms` / `elapsed_ms` (median) | 1 343 / 11 494 — **12 %** |
//! | `backoff_ms` (median) | 11 |
//! | `slowest_attempt_ms` (median / max) | 4 992 / 13 143 |
//! | `budget_ms` | 10 000 |
//!
//! So the retry count is not a ceiling — 3 is `MIN_FLUSH_ATTEMPTS`, the *floor* —
//! and raising it would change nothing, because the 10s wall clock is already
//! spent. **88 % of a flush is neither the PUT nor the backoff**: it is the
//! fold-before-claim at the top of every attempt, in sequential object-store
//! round trips at the broker latencies of #409.
//!
//! A retry does not need that fold. The create came back `AlreadyExists`, which
//! is *proof* a peer holds that sequence — so the only new information is its
//! footer. Folding one footer advances `folded_max` by one and the next candidate
//! follows, with no absence chain and no always-fresh seq-floor read, because
//! there is no absence to prove.

use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage as _, Topition,
    dynostore::{CoalesceTuning, DynoStore},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

const PREFIX: &str = "org.env.conn";
const TOPIC: &str = "org.env.conn.table";

/// A second topic under the same prefix, so a peer's segment can hold a
/// sub-stream of its own: the fold has to work, but our own base offsets are
/// left alone and the assertions stay about the fold rather than about offset
/// re-derivation (which `prefix_coalesce.rs` already covers).
const PEER_TOPIC: &str = "org.env.conn.peer";

/// Counts segment-object reads and writes, and can hand one create to a "peer".
#[derive(Clone)]
struct PeerWins<O> {
    inner: O,
    segment_gets: Arc<AtomicU64>,
    segment_puts: Arc<AtomicU64>,
    /// Bytes to plant at the next create-CAS target, taking that sequence from
    /// under the flush exactly as a peer replica would. Armed once.
    peer_payload: Arc<Mutex<Option<Bytes>>>,
}

impl<O> Debug for PeerWins<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerWins").finish()
    }
}

impl<O> Display for PeerWins<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerWins").finish()
    }
}

impl<O> PeerWins<O> {
    fn segment_gets(&self) -> u64 {
        self.segment_gets.load(Relaxed)
    }

    fn segment_puts(&self) -> u64 {
        self.segment_puts.load(Relaxed)
    }

    fn reset(&self) {
        self.segment_gets.store(0, Relaxed);
        self.segment_puts.store(0, Relaxed);
    }

    /// Arm the peer: the next create-CAS of a segment loses.
    fn arm(&self, payload: Bytes) {
        _ = self
            .peer_payload
            .lock()
            .expect("peer payload")
            .replace(payload);
    }
}

fn is_segment(location: &Path) -> bool {
    location.as_ref().contains("/segments/")
}

#[async_trait]
impl<O> ObjectStore for PeerWins<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if is_segment(location) {
            _ = self.segment_puts.fetch_add(1, Relaxed);

            // The peer writes first and this create finds the sequence taken —
            // `AlreadyExists`, the unambiguous arm, which is what all 224
            // production samples are (`ambiguous_lost` is 0 in every one).
            //
            // Taken out of the lock before the await: a `MutexGuard` is not
            // `Send`, and `ObjectStore` futures are.
            let peer = if opts.mode == PutMode::Create {
                self.peer_payload.lock().expect("peer payload").take()
            } else {
                None
            };

            if let Some(peer) = peer {
                _ = self
                    .inner
                    .put_opts(location, PutPayload::from(peer), PutOptions::default())
                    .await?;

                return Err(object_store::Error::AlreadyExists {
                    path: location.to_string(),
                    source: "taken by a peer".into(),
                });
            }
        }

        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        if is_segment(location) {
            _ = self.segment_gets.fetch_add(1, Relaxed);
        }

        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list_with_offset(prefix, offset)
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
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// `coalesce_batches: Some(1)` so an awaited produce flushes rather than parking
/// on the linger — the op profile is what these measure, as in `scaling.rs`.
fn store() -> (DynoStore, PeerWins<InMemory>) {
    let peer = PeerWins {
        inner: InMemory::new(),
        segment_gets: Arc::new(AtomicU64::new(0)),
        segment_puts: Arc::new(AtomicU64::new(0)),
        peer_payload: Arc::new(Mutex::new(None)),
    };

    (
        DynoStore::new(CLUSTER, NODE, peer.clone()).coalesce_tuning(CoalesceTuning {
            coalesce_batches: Some(1),
            ..Default::default()
        }),
        peer,
    )
}

fn batch(value: &str) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::copy_from_slice(value.as_bytes()))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(store: &DynoStore, name: &str) -> Result<()> {
    _ = store
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

async fn fetch_from(store: &DynoStore, tp: &Topition, offset: i64) -> Result<Vec<deflated::Batch>> {
    store
        .fetch(
            tp,
            offset,
            0,
            100_000,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(200),
        )
        .await
}

/// A segment a peer replica would have written, holding its own sub-stream.
fn peer_segment(store: &DynoStore) -> Result<Bytes> {
    let (payload, _) = store.encode_segment_v3(
        &[(Topition::new(PEER_TOPIC, 0), 0, vec![batch("peer")?])],
        1,
        7,
    )?;

    Ok(Bytes::from(payload))
}

/// The fold, on its own: one ranged footer GET puts the segment in this
/// process's index.
#[tokio::test]
async fn folding_one_footer_costs_one_read() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let (store, peer) = store();
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    _ = store.produce(None, &tp, batch("v0")?).await?;

    peer.reset();
    assert!(store.fold_segment_footer(PREFIX, 0).await);
    assert_eq!(1, peer.segment_gets());
    assert_eq!(0, peer.segment_puts());

    Ok(())
}

/// And it declines rather than guessing. A sequence a create says is occupied
/// but whose footer cannot be read is the `stalled` case the flush's own
/// diagnostics exist for, so the fold answers `false` and the caller falls back
/// to the full refresh.
#[tokio::test]
async fn folding_declines_what_it_cannot_read() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let (store, peer) = store();
    create_topic(&store, TOPIC).await?;

    // Absent.
    assert!(!store.fold_segment_footer(PREFIX, 41).await);

    // Present and not a segment — something squatting the create-only namespace.
    _ = peer
        .put_opts(
            &Path::from(format!(
                "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{:0>20}.seg",
                42
            )),
            PutPayload::from_static(b"not a segment"),
            PutOptions::default(),
        )
        .await?;

    assert!(!store.fold_segment_footer(PREFIX, 42).await);

    Ok(())
}

/// The claim: a lost create costs the next attempt **one** segment read.
///
/// Measured as a difference against an uncontended flush, because the rest of a
/// flush (watermark writes, the finalize) is not what this changes. Before #401
/// the retry ran the whole fold-before-claim again — a tail probe from the new
/// cursor plus the always-fresh seq-floor read that makes an absence a proof —
/// so the same conflict cost three round trips where the situation offered one.
#[tokio::test]
async fn a_lost_create_costs_the_retry_one_read() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let (store, peer) = store();
    create_topic(&store, TOPIC).await?;
    create_topic(&store, PEER_TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    // Warm: the first flush of a prefix is a cold index, which is its own
    // profile and not the one under test.
    _ = store.produce(None, &tp, batch("warm")?).await?;

    peer.reset();
    _ = store.produce(None, &tp, batch("uncontended")?).await?;
    let uncontended_gets = peer.segment_gets();
    let uncontended_puts = peer.segment_puts();

    // Now a peer takes the sequence this flush is about to claim.
    let planted = peer_segment(&store)?;
    peer.reset();
    peer.arm(planted);
    _ = store.produce(None, &tp, batch("contended")?).await?;

    assert_eq!(
        uncontended_puts + 1,
        peer.segment_puts(),
        "the conflict costs exactly one extra create"
    );
    assert_eq!(
        uncontended_gets + 1,
        peer.segment_gets(),
        "the retry must fold the winner's footer and nothing else"
    );

    Ok(())
}

/// And it is still correct: the peer's segment is folded, our records land above
/// it, and every offset reads back as its own.
#[tokio::test]
async fn a_lost_create_still_lands_every_record_at_its_own_offset() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let (store, peer) = store();
    create_topic(&store, TOPIC).await?;
    create_topic(&store, PEER_TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);
    let peer_tp = Topition::new(PEER_TOPIC, 0);

    for i in 0..3 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }

    peer.arm(peer_segment(&store)?);
    _ = store
        .produce(None, &tp, batch("after the conflict")?)
        .await?;

    assert_eq!(
        vec![0, 1, 2, 3],
        fetch_from(&store, &tp, 0)
            .await?
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<i64>>()
    );

    // The peer's sub-stream is readable too — the fold took its footer, it was
    // not stepped over.
    assert_eq!(
        1,
        fetch_from(&store, &peer_tp, 0).await?.len(),
        "the folded peer segment is not readable"
    );

    Ok(())
}
