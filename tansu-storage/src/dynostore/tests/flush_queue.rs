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

//! The per-prefix flush queue (#590): a window that closes is handed to one
//! worker per prefix instead of being flushed by the admission that closed it.
//!
//! Three properties. The next window fills while the previous one's segment
//! PUT is in flight, which the inline flush could not do. Flushes start in the
//! order their windows closed, whether the size threshold or the linger timer
//! closed them. And a window queued behind one that fails is answered the way
//! #588 answers a batch admitted behind it.

use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use rand::{RngExt as _, rng};
use tansu_sans_io::{
    ErrorCode,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use tokio::{
    sync::Semaphore,
    time::{sleep, timeout},
};

use crate::{
    Ack, Error, Result, Storage, Topition,
    dynostore::{CoalesceTuning, DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const TOPIC: &str = "org.env.conn.queued";

/// Long enough that an admission the inline flush would have held is
/// unmistakably held, short enough to keep a failing run quick.
const PATIENCE: Duration = Duration::from_secs(5);

/// Two records, idempotent when `sequence` is given — so a producer's next
/// batch is two sequences on, and every batch two offsets on.
fn batch(sequence: Option<i32>) -> Result<deflated::Batch> {
    let builder = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"a"))))
        .record(Record::builder().value(Some(Bytes::from_static(b"b"))))
        .last_offset_delta(1);

    let builder = match sequence {
        Some(sequence) => builder
            .producer_id(1)
            .producer_epoch(0)
            .base_sequence(sequence),
        None => builder,
    };

    builder
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// A store whose windows close every `batches` batches, or after `linger`.
async fn store(
    bucket: Arc<Gated>,
    batches: usize,
    linger: Duration,
) -> Result<(DynoStore, Topition)> {
    let store = DynoStore::new(CLUSTER, NODE, bucket).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(batches),
        coalesce_linger: Some(linger),
        ..Default::default()
    });

    _ = store
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok((store, Topition::new(TOPIC, 0)))
}

/// What the gated store does with the first segment create.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum First {
    /// Hold it until released, then let it land.
    Held,
    /// Hold it until released, then fail it without writing anything.
    HeldThenFailed,
    /// Neither: every create is merely slow.
    Slow,
}

/// Segment creates held, failed or slowed on demand, so a window's PUT can be
/// kept in flight for as long as a test needs to look at the next one.
struct Gated {
    inner: InMemory,
    first: First,
    creates: AtomicUsize,
    release: Semaphore,
}

impl Gated {
    fn new(first: First) -> Arc<Self> {
        Arc::new(Self {
            inner: InMemory::new(),
            first,
            creates: AtomicUsize::new(0),
            release: Semaphore::new(0),
        })
    }

    fn creates(&self) -> usize {
        self.creates.load(Ordering::SeqCst)
    }

    /// Wait for the `n`th segment create to be in flight.
    async fn in_flight(&self, n: usize) {
        timeout(PATIENCE, async {
            while self.creates() < n {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the window never flushed");
    }

    fn release(&self) {
        self.release.add_permits(1);
    }

    async fn segments(&self) -> usize {
        use futures::TryStreamExt as _;

        self.inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .expect("list")
            .iter()
            .filter(|meta| meta.location.as_ref().ends_with(".seg"))
            .count()
    }
}

impl Debug for Gated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gated").finish()
    }
}

impl Display for Gated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gated").finish()
    }
}

#[async_trait]
impl ObjectStore for Gated {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if options.mode == PutMode::Create && location.as_ref().ends_with(".seg") {
            let create = self.creates.fetch_add(1, Ordering::SeqCst);

            match self.first {
                First::Held | First::HeldThenFailed if create == 0 => {
                    _ = self.release.acquire().await;

                    if self.first == First::HeldThenFailed {
                        return Err(object_store::Error::Generic {
                            store: "Gated",
                            source: "503 SlowDown".into(),
                        });
                    }
                }

                First::Slow => {
                    let delay = Duration::from_millis(rng().random_range(0..=3));
                    sleep(delay).await;
                }

                _ => {}
            }
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

/// Admit `batch` and fail the test if the admission does not return while
/// a segment PUT is held — which is what the inline flush did.
async fn admitted(store: &DynoStore, topition: &Topition, batch: deflated::Batch) -> Result<Ack> {
    timeout(PATIENCE, store.admit(None, topition, batch))
        .await
        .expect("admission waited for the segment PUT in flight")
}

async fn offsets(acks: Vec<Ack>) -> Result<Vec<i64>> {
    let mut offsets = Vec::with_capacity(acks.len());
    for ack in acks {
        offsets.push(timeout(PATIENCE, ack).await.expect("never acked")?);
    }
    Ok(offsets)
}

/// **The acceptance shape of #590.**
///
/// The batch that closes a window is admitted without waiting for the window's
/// PUT, so the next window fills — and closes — while that PUT is in flight.
/// With the inline flush the second admission below never returns until the
/// PUT is released.
#[tokio::test]
async fn the_next_window_fills_while_a_put_is_in_flight() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = Gated::new(First::Held);
    let (store, topition) = store(bucket.clone(), 2, Duration::from_secs(60)).await?;

    let mut acks = vec![admitted(&store, &topition, batch(Some(0))?).await?];
    acks.push(admitted(&store, &topition, batch(Some(2))?).await?);
    bucket.in_flight(1).await;

    acks.push(admitted(&store, &topition, batch(Some(4))?).await?);
    acks.push(admitted(&store, &topition, batch(Some(6))?).await?);
    assert_eq!(1, bucket.creates(), "the second window waits for the first");

    bucket.release();

    assert_eq!(vec![0, 2, 4, 6], offsets(acks).await?);
    assert_eq!(2, bucket.segments().await, "two windows, two PUTs");

    Ok(())
}

/// **The back-pressure bound.**
///
/// One closed window may wait behind the PUT in flight
/// ([`DynoStore::FLUSH_QUEUE_DEPTH`]); the admission that closes a second one
/// waits until the first queued window starts, so a slow PUT holds a bounded
/// number of windows rather than every request the connection can read.
#[tokio::test]
async fn admission_waits_once_a_window_is_queued_behind_the_put() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = Gated::new(First::Held);
    let (store, topition) = store(bucket.clone(), 1, Duration::from_secs(60)).await?;

    let first = admitted(&store, &topition, batch(Some(0))?).await?;
    bucket.in_flight(1).await;
    let queued = admitted(&store, &topition, batch(Some(2))?).await?;

    let behind = tokio::spawn({
        let store = store.clone();
        let topition = topition.clone();
        async move { store.admit(None, &topition, batch(Some(4))?).await }
    });

    sleep(Duration::from_millis(100)).await;
    assert!(!behind.is_finished(), "admitted past the queue bound");

    bucket.release();
    let behind = timeout(PATIENCE, behind)
        .await
        .expect("admission never resumed")
        .expect("join")?;

    assert_eq!(vec![0, 2, 4], offsets(vec![first, queued, behind]).await?);

    Ok(())
}

/// **Close order is flush order, threshold and timer alike.**
///
/// With the first PUT held: a window closed on size queues behind it, the
/// linger timer closes the next one, and a window closed on size again queues
/// behind both. Offsets are assigned when a flush starts, so a flush that
/// overtook another would hand its batches lower offsets than batches admitted
/// before them.
#[tokio::test]
async fn windows_closed_by_size_and_by_timer_flush_in_close_order() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = Gated::new(First::Held);
    let (store, topition) = store(bucket.clone(), 2, Duration::from_millis(20)).await?;

    let mut acks = vec![admitted(&store, &topition, batch(None)?).await?];
    acks.push(admitted(&store, &topition, batch(None)?).await?);
    bucket.in_flight(1).await;

    acks.push(admitted(&store, &topition, batch(None)?).await?);
    acks.push(admitted(&store, &topition, batch(None)?).await?);

    acks.push(admitted(&store, &topition, batch(None)?).await?);
    sleep(Duration::from_millis(200)).await;

    let last = tokio::spawn({
        let store = store.clone();
        let topition = topition.clone();
        async move {
            let mut acks = vec![store.admit(None, &topition, batch(None)?).await?];
            acks.push(store.admit(None, &topition, batch(None)?).await?);
            Ok::<_, Error>(acks)
        }
    });

    bucket.release();
    acks.extend(
        timeout(PATIENCE, last)
            .await
            .expect("stuck")
            .expect("join")?,
    );

    assert_eq!(vec![0, 2, 4, 6, 8, 10, 12], offsets(acks).await?);
    assert_eq!(4, bucket.segments().await, "size, size, timer, size");

    Ok(())
}

/// **The same property under a race.**
///
/// One admitter, a window of three batches and a 1 ms linger, with a jittered
/// pause between admissions and a jittered PUT: windows close on size and on
/// the timer in an unpredictable mix, on a multi-threaded runtime where the
/// timer task and the admitter run in parallel. Offsets must still follow
/// admission order exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_size_and_timer_windows_keep_admission_order() -> Result<()> {
    const BATCHES: i64 = 200;

    let _guard = init_tracing()?;

    let bucket = Gated::new(First::Slow);
    let (store, topition) = store(bucket.clone(), 3, Duration::from_millis(1)).await?;

    let mut acks = Vec::with_capacity(BATCHES as usize);
    for _ in 0..BATCHES {
        acks.push(store.admit(None, &topition, batch(None)?).await?);

        let pause = rng().random_range(0..=1_500);
        sleep(Duration::from_micros(pause)).await;
    }

    let expected = (0..BATCHES).map(|batch| batch * 2).collect::<Vec<_>>();
    assert_eq!(expected, offsets(acks).await?);

    let segments = bucket.segments().await;
    assert!(
        segments > (BATCHES as usize).div_ceil(3),
        "no window closed on the timer ({segments} segments)",
    );

    Ok(())
}

/// **A window queued behind one that fails.**
///
/// The first window's PUT fails while the second is already closed and queued.
/// The failure is the first window's alone (`fail_prefix_flush`); the second
/// flushes after it, and the producer table refuses its idempotent batch rather
/// than writing it past the gap (#88) — the outcome #588 gives a batch admitted
/// behind a failed window, now with the window already closed when the first
/// one fails. The client retries both, in order, and gets both.
#[tokio::test]
async fn a_window_queued_behind_a_failed_flush_is_refused_past_the_gap() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = Gated::new(First::HeldThenFailed);
    let (store, topition) = store(bucket.clone(), 1, Duration::from_secs(60)).await?;

    let first = admitted(&store, &topition, batch(Some(0))?).await?;
    bucket.in_flight(1).await;
    let second = admitted(&store, &topition, batch(Some(2))?).await?;
    assert_eq!(
        1,
        bucket.creates(),
        "the second window is queued, not started"
    );

    bucket.release();

    let first = timeout(PATIENCE, first).await.expect("never acked");
    let second = timeout(PATIENCE, second).await.expect("never acked");

    assert!(
        matches!(first, Err(Error::ObjectStore(_))),
        "the first window failed: {first:?}",
    );
    assert!(
        matches!(second, Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))),
        "written past the gap: {second:?}",
    );
    assert_eq!(0, bucket.segments().await, "nothing was written");

    assert_eq!(0, store.produce(None, &topition, batch(Some(0))?).await?);
    assert_eq!(2, store.produce(None, &topition, batch(Some(2))?).await?);

    Ok(())
}
