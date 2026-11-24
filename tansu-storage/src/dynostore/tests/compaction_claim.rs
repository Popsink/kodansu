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

//! Compaction lands merged segments against real request latencies while
//! leaseless producers on three replicas keep taking the tail (DEV-662).
//!
//! The first DEV-662 fix staged the merged payload and made each attempt a
//! constant-size create-only copy. On the real GCS bucket it still never won:
//! a prefix took a producer segment every ~80 ms, and one compaction attempt
//! took ~230 ms — the refresh that re-reads the tail, the backoff, then a
//! ~90 ms copy. The tail moves about three times between reading it and the
//! copy landing, so every attempt finds its name taken. `compaction_race.rs`
//! passes on that build only because its requests cost 2 ms.
//!
//! This store charges what the bucket charged: a LIST is 120 ms, a copy 90 ms,
//! a GET 15 ms and a small PUT 25 ms, each create-only precondition decided when
//! the request ends. The producers are the real leaseless flush, three
//! `DynoStore`s over one bucket, together creating a segment every ~80 ms.

use std::{
    fmt::{self, Debug, Display},
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use tokio::time::{Instant, sleep};

use crate::{
    LogStorage as _, MetadataStorage as _, PutRateLimiter, Result, Topition,
    dynostore::{CoalesceTuning, CompactRun, DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "org.env.load";
const TOPIC: &str = "org.env.load.tab";

const REPLICAS: i32 = 3;

const LIST: Duration = Duration::from_millis(120);
const COPY: Duration = Duration::from_millis(90);
const GET: Duration = Duration::from_millis(15);
const PUT: Duration = Duration::from_millis(25);
const DELETE: Duration = Duration::from_millis(15);

/// What each replica waits between two produces. With the flush's own round
/// trips, three replicas take a sequence every ~80 ms between them.
const PRODUCE_PAUSE: Duration = Duration::from_millis(150);

const RUN_FOR: Duration = Duration::from_secs(8);

const RECORDS: usize = 8;
const VALUE_BYTES: usize = 512;

const MIN_SEGMENTS: usize = 16;
const TARGET_BYTES: usize = 64 << 10;

/// An object store whose every request costs a round trip, deciding a
/// create-only precondition when the request ends.
#[derive(Clone)]
struct RoundTrips {
    inner: InMemory,
    floor_puts: Arc<AtomicU64>,
}

impl Debug for RoundTrips {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoundTrips").finish()
    }
}

impl Display for RoundTrips {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoundTrips").finish()
    }
}

impl RoundTrips {
    fn listed(
        listing: impl Future<Output = BoxStream<'static, Result<ObjectMeta, object_store::Error>>>
        + Send
        + 'static,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        futures::stream::once(async move {
            sleep(LIST).await;
            listing.await
        })
        .flatten()
        .boxed()
    }
}

#[async_trait]
impl ObjectStore for RoundTrips {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if location.filename() == Some("seq-floor.json") {
            _ = self.floor_puts.fetch_add(1, Ordering::Relaxed);
        }
        sleep(PUT).await;
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
        sleep(GET).await;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        let inner = self.inner.clone();

        futures::stream::once(async move {
            sleep(DELETE).await;
            inner.delete_stream(locations)
        })
        .flatten()
        .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        let (inner, prefix) = (self.inner.clone(), prefix.cloned());
        Self::listed(async move { inner.list(prefix.as_ref()) })
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        let (inner, prefix, offset) = (self.inner.clone(), prefix.cloned(), offset.clone());
        Self::listed(async move { inner.list_with_offset(prefix.as_ref(), &offset) })
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        sleep(LIST).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<(), object_store::Error> {
        sleep(COPY).await;
        self.inner.copy_opts(from, to, options).await
    }
}

fn batch() -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder();

    for _ in 0..RECORDS {
        builder =
            builder.record(Record::builder().value(Some(Bytes::from(vec![b'x'; VALUE_BYTES]))));
    }

    builder
        .last_offset_delta(RECORDS as i32 - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// One broker replica producing to its own partition until told to stop,
/// returning how many batches it had acknowledged.
async fn produce(replica: DynoStore, partition: i32, stop: Arc<AtomicBool>) -> Result<i64> {
    let (tp, batch) = (Topition::new(TOPIC, partition), batch()?);
    let mut acked = 0;

    while !stop.load(Ordering::Relaxed) {
        if replica.produce(None, &tp, batch.clone()).await.is_ok() {
            acked += 1;
        }

        sleep(PRODUCE_PAUSE).await;
    }

    Ok(acked)
}

/// Every record of `partition`, read back from offset 0 by a fresh replica.
async fn read_back(reader: &DynoStore, partition: i32) -> Result<i64> {
    let tp = Topition::new(TOPIC, partition);
    let mut offset = 0;

    loop {
        let batches = reader
            .fetch(
                &tp,
                offset,
                1,
                10 << 20,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(500),
            )
            .await?;

        let Some(last) = batches.last() else {
            return Ok(offset);
        };

        assert_eq!(
            offset, batches[0].base_offset,
            "partition {partition} is not contiguous at {offset}"
        );

        offset = last.base_offset + i64::from(last.last_offset_delta) + 1;
    }
}

fn replica(bucket: &InMemory) -> DynoStore {
    DynoStore::new(
        CLUSTER,
        NODE,
        RoundTrips {
            inner: bucket.clone(),
            floor_puts: Arc::default(),
        },
    )
    .coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        ..Default::default()
    })
}

#[tokio::test]
async fn compaction_lands_against_bucket_latencies() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();

    let maintainer = DynoStore::new(
        CLUSTER,
        NODE,
        RoundTrips {
            inner: bucket.clone(),
            floor_puts: Arc::default(),
        },
    )
    .coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(MIN_SEGMENTS),
        prefix_compact_keep_hot: Some(1),
        prefix_compact_target_bytes: Some(TARGET_BYTES),
        ..Default::default()
    });

    _ = maintainer
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(REPLICAS)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let stop = Arc::new(AtomicBool::new(false));

    let producers = (0..REPLICAS)
        .map(|partition| tokio::spawn(produce(replica(&bucket), partition, stop.clone())))
        .collect::<Vec<_>>();

    let started = Instant::now();
    let (mut merges, mut failed) = (0u64, 0u64);

    while started.elapsed() < RUN_FOR {
        maintainer.refresh_prefix_index_forced(PREFIX).await?;

        match maintainer.compact_prefix_segments(PREFIX).await {
            Ok(CompactRun::Merged(_)) => merges += 1,
            Ok(CompactRun::Drained | CompactRun::Retry) => sleep(Duration::from_millis(50)).await,
            Err(_) => failed += 1,
        }
    }

    stop.store(true, Ordering::Relaxed);

    let mut acked = Vec::new();
    for producer in producers {
        acked.push(producer.await.expect("a producer panicked")?);
    }

    let live = bucket
        .list(Some(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/"
        ))))
        .try_collect::<Vec<_>>()
        .await?
        .len();

    eprintln!(
        "acked={acked:?} live={live} merges={merges} failed_passes={failed} elapsed={:?}",
        started.elapsed()
    );

    assert!(
        merges >= 5,
        "{merges} merged segments landed in {RUN_FOR:?} of sustained produce"
    );

    let reader = replica(&bucket);
    for (partition, acked) in (0..REPLICAS).zip(acked) {
        assert_eq!(
            acked * RECORDS as i64,
            read_back(&reader, partition).await?,
            "partition {partition} lost or duplicated records"
        );
    }

    Ok(())
}

/// Records survive merges whose claims stay behind them until the floor
/// covers them (DEV-675), with the maintainer paced as `gs://` paces it and
/// draining through the maintenance path, claim sweep included.
#[tokio::test]
async fn drain_loses_nothing_while_claims_wait_for_the_floor() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let floor_puts = Arc::new(AtomicU64::new(0));

    let maintainer = DynoStore::new(
        CLUSTER,
        NODE,
        PutRateLimiter::new(
            RoundTrips {
                inner: bucket.clone(),
                floor_puts: floor_puts.clone(),
            },
            Duration::from_mins(5),
        )
        .with_rate_per_second(NonZeroU32::new(1)),
    )
    .object_write_interval(Some(Duration::from_secs(1)))
    .coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(MIN_SEGMENTS),
        prefix_compact_keep_hot: Some(1),
        prefix_compact_target_bytes: Some(TARGET_BYTES),
        ..Default::default()
    });

    _ = maintainer
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(REPLICAS)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let stop = Arc::new(AtomicBool::new(false));

    let producers = (0..REPLICAS)
        .map(|partition| tokio::spawn(produce(replica(&bucket), partition, stop.clone())))
        .collect::<Vec<_>>();

    let started = Instant::now();
    let mut merged = 0;

    while started.elapsed() < RUN_FOR {
        maintainer.refresh_prefix_index_forced(PREFIX).await?;

        match maintainer.drain_compact_prefix(PREFIX).await {
            0 => sleep(Duration::from_millis(50)).await,
            n => merged += n,
        }
    }

    stop.store(true, Ordering::Relaxed);

    let mut acked = Vec::new();
    for producer in producers {
        acked.push(producer.await.expect("a producer panicked")?);
    }

    eprintln!(
        "acked={acked:?} merged={merged} floor_puts={} elapsed={:?}",
        floor_puts.load(Ordering::Relaxed),
        started.elapsed()
    );

    assert!(
        merged >= 2 * MIN_SEGMENTS as u64,
        "{merged} segments merged in {RUN_FOR:?} of sustained produce"
    );

    let reader = replica(&bucket);
    for (partition, acked) in (0..REPLICAS).zip(acked) {
        assert_eq!(
            acked * RECORDS as i64,
            read_back(&reader, partition).await?,
            "partition {partition} lost or duplicated records"
        );
    }

    Ok(())
}
