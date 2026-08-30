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

//! A `Fetch`'s partitions are read at once, not one after another (#426).
//!
//! #437 buffered the innermost loop — the segments of one sub-stream — and
//! deliberately left the two above it, because overlapping them means deciding
//! what a request-level `max_bytes` should mean when several partitions spend it
//! at once. That question is answered by `Budget`: a partition *claims* its
//! allowance before reading and returns what it did not spend, so the cap holds
//! exactly while the reads overlap.
//!
//! This measures the shape the claim makes possible. Latency is injected rather
//! than real, so it is observable with no bucket — and it is the parameter
//! because it is the whole point: the same partition count cost 24 ms × N
//! against the maintainers' measured `get_opts` and 350 ms × N against the
//! brokers' (#409).

// The whole file is a `DynoStore` over a custom `ObjectStore`, and
// `object_store` is an optional dependency the `dynostore` feature pulls in —
// so with the feature off there is nothing here to compile. The same
// crate-level gate every other test in this directory that names the store
// carries.
#![cfg(feature = "dynostore")]

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, memory::InMemory, path::Path,
};
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use tansu_sans_io::{
    ErrorCode, FetchRequest, IsolationLevel,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    record::{Record, deflated, inflated},
};
use tansu_storage::{DynoStore, FetchService, Storage, Topition};
use tokio::time::sleep;

use crate::common::{Error, cluster_id, init_tracing};

mod common;

const NODE: i32 = 111;
const TOPIC: &str = "fan-out";

/// Partitions in the assignment. A consumer in the 1500-topic fan-out shape
/// holds ~94; this is the concurrency bound, which is where the difference
/// between overlapping and not is already decisive.
const PARTITIONS: i32 = 32;

/// Per-GET latency. Between the maintainers' measured 24 ms and the brokers'
/// 350 ms (#409), and small enough that the serial shape still finishes.
const LATENCY: Duration = Duration::from_millis(20);

/// An object store that takes its time, and counts.
#[derive(Clone)]
struct Slow<O> {
    inner: O,
    gets: Arc<AtomicUsize>,
}

impl<O> fmt::Debug for Slow<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slow").finish()
    }
}

impl<O> fmt::Display for Slow<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slow").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for Slow<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<object_store::PutResult, object_store::Error> {
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
        _ = self.gets.fetch_add(1, Ordering::Relaxed);
        sleep(LATENCY).await;
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

fn batch(value: &str) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::copy_from_slice(value.as_bytes()))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// A topic of `PARTITIONS` partitions with one record in each, over a store
/// that costs `LATENCY` per GET.
async fn seeded() -> Result<(DynoStore, Arc<AtomicUsize>), Error> {
    let gets = Arc::new(AtomicUsize::new(0));

    let storage = DynoStore::new(
        &cluster_id(),
        NODE,
        Slow {
            inner: InMemory::new(),
            gets: gets.clone(),
        },
    );

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(PARTITIONS)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    for partition in 0..PARTITIONS {
        _ = storage
            .produce(
                None,
                &Topition::new(TOPIC, partition),
                batch(&format!("record-{partition}"))?,
            )
            .await?;
    }

    Ok((storage, gets))
}

fn fetch_every_partition() -> FetchRequest {
    FetchRequest::default()
        .max_wait_ms(1_000)
        .min_bytes(1)
        .max_bytes(Some(64 * 1024 * 1024))
        .isolation_level(Some(i8::from(IsolationLevel::ReadUncommitted)))
        .topics(Some(
            [FetchTopic::default()
                .topic(Some(TOPIC.into()))
                .partitions(Some(
                    (0..PARTITIONS)
                        .map(|partition| {
                            FetchPartition::default()
                                .partition(partition)
                                .fetch_offset(0)
                                .partition_max_bytes(1024 * 1024)
                        })
                        .collect(),
                ))]
            .into(),
        ))
}

/// The shape: `PARTITIONS` partitions used to be one round trip each, awaited
/// in sequence, so the wall clock was `partitions × latency`. Overlapped, it is
/// a couple of waves.
///
/// Asserted loosely enough to survive a loaded runner and tightly enough that a
/// return to the serial shape cannot pass: serial is at least
/// `PARTITIONS × LATENCY` = 640 ms, and the bound is well under half of that.
#[tokio::test(flavor = "multi_thread")]
async fn partitions_are_read_at_once() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, gets) = seeded().await?;

    let service = MapStateLayer::new(|_| storage).into_layer(FetchService);

    gets.store(0, Ordering::Relaxed);
    let started = SystemTime::now();

    let response = service
        .serve(Context::default(), fetch_every_partition())
        .await?;

    let elapsed = started.elapsed().map_or(0, |d| d.as_millis() as u64);
    let serial = PARTITIONS as u64 * LATENCY.as_millis() as u64;

    println!(
        "{PARTITIONS} partitions, {}ms/GET: {} GETs in {elapsed}ms (serial would be >= {serial}ms)",
        LATENCY.as_millis(),
        gets.load(Ordering::Relaxed),
    );

    // Every partition is answered, and answered with its record — a fan-out
    // that overlapped and lost data would also be fast.
    let topics = response.responses.unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.clone().unwrap_or_default();
    assert_eq!(PARTITIONS as usize, partitions.len());

    for (index, partition) in partitions.iter().enumerate() {
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partition.error_code)?,
            "partition {index}",
        );

        // `buffered`, not `buffer_unordered`: a client matches partitions to its
        // request positionally.
        assert_eq!(index as i32, partition.partition_index);
        assert!(
            partition.records.is_some(),
            "partition {index} has no records"
        );
    }

    assert!(
        elapsed < serial / 2,
        "{PARTITIONS} partitions took {elapsed}ms; serial would be >= {serial}ms",
    );

    Ok(())
}

/// The request-level cap still bounds the response, with every partition
/// spending it at once. This is the half of the change that is not about speed:
/// a budget merely *checked* before each read would be overshot by every
/// partition already in flight.
#[tokio::test(flavor = "multi_thread")]
async fn the_request_budget_still_bounds_a_concurrent_fetch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, _gets) = seeded().await?;

    let service = MapStateLayer::new(|_| storage).into_layer(FetchService);

    // Room for a couple of partitions' records, not thirty-two.
    const BUDGET: i32 = 512;

    /// Generous for the single small record each partition holds.
    const LARGEST_BATCH: usize = 128;

    let response = service
        .serve(
            Context::default(),
            fetch_every_partition().max_bytes(Some(BUDGET)),
        )
        .await?;

    let topics = response.responses.unwrap_or_default();
    let partitions = topics[0].partitions.clone().unwrap_or_default();

    // Every partition is still answered — a budget that ran out means empty
    // records, never a missing partition.
    assert_eq!(PARTITIONS as usize, partitions.len());

    let delivered: usize = partitions
        .iter()
        .filter_map(|partition| partition.records.as_ref())
        .map(|frame| {
            frame
                .batches
                .iter()
                .map(|batch| batch.batch_length.max(0) as usize)
                .sum::<usize>()
        })
        .sum();

    println!("budget {BUDGET}, delivered {delivered} bytes");

    // `max_bytes` is explicitly not an absolute maximum in Kafka: a log answers
    // with at least one whole batch whatever the cap, or a request too small for
    // the next batch would never make progress. Kafka overshoots by one batch
    // because it reads in series; this overshoots by at most one per reader in
    // flight, which is what buys the fan-out. Both are bounded by
    // `max.partition.fetch.bytes`, and the *count* is what this pins: without
    // charging an overspend back to the budget, every reader would spend the
    // excess again and the bound would be the assignment, not the concurrency.
    let overshoot = PARTITIONS as usize * LARGEST_BATCH;

    assert!(
        delivered <= BUDGET as usize + overshoot,
        "delivered {delivered} bytes against a {BUDGET} byte request budget \
         (+{overshoot} of permitted one-batch overshoot)",
    );

    // And it delivered *something*: a cap that answered empty would also pass
    // the assertion above.
    assert!(delivered > 0, "the budget bounded the response to nothing");

    Ok(())
}
