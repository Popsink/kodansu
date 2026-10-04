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

//! [`Storage::admit`]: a batch takes its place in the write order before it is
//! durable, so a connection can read its next request without waiting for the
//! window (#588).
//!
//! Two properties, and one hazard. Batches admitted one after another share a
//! window and take offsets in admission order. And a batch admitted behind a
//! window that then fails lands in the *next* window — which an idempotent
//! batch survives, because the producer table refuses it, and a batch without
//! a sequence does not. That last case is no reason to keep such a batch out
//! of the pipeline: its client sent the second request before the first was
//! answered, and an unpipelined connection would have reordered it the same.

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
use tansu_sans_io::{
    ErrorCode,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use tokio::{sync::Semaphore, time::timeout};

use crate::{
    Error, Result, Storage, Topition,
    dynostore::{DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const TOPIC: &str = "org.env.conn.pipelined";

/// Two records, idempotent when `sequence` is given — so a producer's next
/// batch is two sequences on.
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

async fn create_topic(store: &DynoStore) -> Result<Topition> {
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

    Ok(Topition::new(TOPIC, 0))
}

async fn segments(bucket: &dyn ObjectStore) -> usize {
    use futures::TryStreamExt as _;

    bucket
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .expect("list")
        .iter()
        .filter(|meta| meta.location.as_ref().ends_with(".seg"))
        .count()
}

/// **The acceptance shape of #588, at the engine.**
///
/// Five batches admitted one after another, none of them awaited: one window,
/// one segment PUT, offsets in the order they were admitted. Before #588 this
/// is what five *connections* got; one connection got five windows.
#[tokio::test]
async fn admitted_batches_share_one_window_in_admission_order() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let topition = create_topic(&store).await?;

    let mut acks = Vec::new();
    for sequence in (0..10).step_by(2) {
        acks.push(store.admit(None, &topition, batch(Some(sequence))?).await?);
    }

    let mut offsets = Vec::new();
    for ack in acks {
        offsets.push(ack.await?);
    }

    assert_eq!(vec![0, 2, 4, 6, 8], offsets);
    assert_eq!(1, segments(&bucket).await, "one window, one PUT");

    Ok(())
}

/// Holds the first segment create until released, then fails it without
/// writing anything: a window whose PUT is in flight, and then does not land.
struct HeldThenFailed {
    inner: InMemory,
    creates: AtomicUsize,
    release: Semaphore,
}

impl HeldThenFailed {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            creates: AtomicUsize::new(0),
            release: Semaphore::new(0),
        }
    }

    /// Wait for the first create to be in flight.
    async fn held(&self) {
        timeout(Duration::from_secs(10), async {
            while self.creates.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the first window never flushed");
    }
}

impl Debug for HeldThenFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeldThenFailed").finish()
    }
}

impl Display for HeldThenFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeldThenFailed").finish()
    }
}

#[async_trait]
impl ObjectStore for HeldThenFailed {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if options.mode == PutMode::Create
            && location.as_ref().ends_with(".seg")
            && self.creates.fetch_add(1, Ordering::SeqCst) == 0
        {
            _ = self.release.acquire().await;

            return Err(object_store::Error::Generic {
                store: "HeldThenFailed",
                source: "503 SlowDown".into(),
            });
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

/// Admit `first`, wait until its window's PUT is in flight, admit `second`
/// into the next window, then fail the first. What each was acked with.
async fn behind_a_failed_window(
    first: deflated::Batch,
    second: deflated::Batch,
) -> Result<(Result<i64>, Result<i64>, DynoStore, Topition)> {
    let bucket = Arc::new(HeldThenFailed::new());
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let topition = create_topic(&store).await?;

    let first = store.admit(None, &topition, first).await?;
    bucket.held().await;

    let second = store.admit(None, &topition, second).await?;
    bucket.release.add_permits(1);

    Ok((first.await, second.await, store, topition))
}

/// **The hazard pipelining has to answer, for an idempotent producer.**
///
/// Request 1's window fails; request 2 for the same partition was admitted
/// into the next one. The flush's producer table expects request 1's sequence
/// first, so request 2 is refused rather than written past the gap — and the
/// client, which retries both in order, ends up with both, in order.
#[tokio::test]
async fn an_idempotent_batch_behind_a_failed_window_is_refused() -> Result<()> {
    let _guard = init_tracing()?;

    let (first, second, store, topition) =
        behind_a_failed_window(batch(Some(0))?, batch(Some(2))?).await?;

    assert!(
        matches!(first, Err(Error::ObjectStore(_))),
        "the first window failed: {first:?}",
    );
    assert!(
        matches!(second, Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))),
        "written past the gap: {second:?}",
    );

    assert_eq!(0, store.produce(None, &topition, batch(Some(0))?).await?);
    assert_eq!(2, store.produce(None, &topition, batch(Some(2))?).await?);

    Ok(())
}

/// **The same hazard without a sequence.**
///
/// Nothing refuses the second batch, so it is written while the first is not —
/// and the first, retried, lands after it. Kafka's contract for a producer
/// with more than one request in flight and no idempotence, and the same
/// outcome a connection that reads one request at a time gives it: the second
/// request is read once the first has failed, and lands ahead of the retry.
#[tokio::test]
async fn a_batch_without_a_sequence_behind_a_failed_window_is_written_past_it() -> Result<()> {
    let _guard = init_tracing()?;

    let (first, second, store, topition) =
        behind_a_failed_window(batch(None)?, batch(None)?).await?;

    assert!(first.is_err(), "the first window failed: {first:?}");
    assert_eq!(0, second?, "the second took the first's place");

    assert_eq!(2, store.produce(None, &topition, batch(None)?).await?);

    Ok(())
}
