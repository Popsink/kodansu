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

//! How many segments one maintainer merges per second against bucket
//! latencies (DEV-662).
//!
//! With the claim race solved, the GCS bucket showed one maintainer merging
//! ~22 segments/s against ~84/s produced over seven hot prefixes. This drains a
//! backlog of the same shape — seven prefixes, runs of ~22 segments — through
//! [`DynoStore::sweep_backlogged_prefixes`] over a store that charges the
//! bucket's round trips plus a transfer time, and counts what each merge asks
//! of the store.

use std::{
    collections::BTreeSet,
    fmt::{self, Debug, Display},
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
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
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use tokio::time::{Instant, sleep};

use crate::{
    LogStorage as _, MetadataStorage as _, PutRateLimiter, Result, Topition,
    dynostore::{CoalesceTuning, DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

const LIST: Duration = Duration::from_millis(120);
const COPY: Duration = Duration::from_millis(90);
const GET: Duration = Duration::from_millis(15);
const PUT: Duration = Duration::from_millis(25);
const DELETE: Duration = Duration::from_millis(15);

/// One stream's transfer rate, charged on top of a GET's or PUT's round trip.
const BYTES_PER_SEC: u64 = 100 << 20;

const RECORDS: usize = 64;
const VALUE_BYTES: usize = 512;

const MIN_SEGMENTS: usize = 16;

/// One segment's bytes; a run is `prefix_compact_target_bytes` over this.
const SEGMENT_BYTES: usize = RECORDS * VALUE_BYTES;

#[derive(Default, Debug)]
struct Counts {
    get: AtomicU64,
    put: AtomicU64,
    copy: AtomicU64,
    list: AtomicU64,
    delete: AtomicU64,
    getting: AtomicU64,
    most_getting: AtomicU64,
}

impl Counts {
    fn snapshot(&self) -> [u64; 5] {
        [
            self.get.load(Ordering::Relaxed),
            self.put.load(Ordering::Relaxed),
            self.copy.load(Ordering::Relaxed),
            self.list.load(Ordering::Relaxed),
            self.delete.load(Ordering::Relaxed),
        ]
    }
}

/// An object store charging a round trip per request and a transfer time per
/// byte, counting each kind of request.
#[derive(Clone)]
struct Bucket {
    inner: InMemory,
    counts: Arc<Counts>,
}

impl Debug for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bucket").finish()
    }
}

impl Display for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bucket").finish()
    }
}

fn transfer(bytes: usize) -> Duration {
    Duration::from_micros(bytes as u64 * 1_000_000 / BYTES_PER_SEC)
}

impl Bucket {
    fn listed(
        counts: Arc<Counts>,
        listing: impl Future<Output = BoxStream<'static, Result<ObjectMeta, object_store::Error>>>
        + Send
        + 'static,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        futures::stream::once(async move {
            _ = counts.list.fetch_add(1, Ordering::Relaxed);
            sleep(LIST).await;
            listing.await
        })
        .flatten()
        .boxed()
    }
}

#[async_trait]
impl ObjectStore for Bucket {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        _ = self.counts.put.fetch_add(1, Ordering::Relaxed);
        sleep(PUT + transfer(payload.content_length())).await;
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
        _ = self.counts.get.fetch_add(1, Ordering::Relaxed);
        let getting = self.counts.getting.fetch_add(1, Ordering::Relaxed) + 1;
        _ = self
            .counts
            .most_getting
            .fetch_max(getting, Ordering::Relaxed);
        let result = self.inner.get_opts(location, options).await;
        let bytes = result
            .as_ref()
            .map_or(0, |result| (result.range.end - result.range.start) as usize);
        sleep(GET + transfer(bytes)).await;
        _ = self.counts.getting.fetch_sub(1, Ordering::Relaxed);
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        let (inner, counts) = (self.inner.clone(), self.counts.clone());

        futures::stream::once(async move {
            _ = counts.delete.fetch_add(1, Ordering::Relaxed);
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
        Self::listed(
            self.counts.clone(),
            async move { inner.list(prefix.as_ref()) },
        )
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        let (inner, prefix, offset) = (self.inner.clone(), prefix.cloned(), offset.clone());
        Self::listed(self.counts.clone(), async move {
            inner.list_with_offset(prefix.as_ref(), &offset)
        })
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        _ = self.counts.list.fetch_add(1, Ordering::Relaxed);
        sleep(LIST).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<(), object_store::Error> {
        _ = self.counts.copy.fetch_add(1, Ordering::Relaxed);
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

fn prefix(i: usize) -> String {
    format!("org.env{i}.load")
}

/// `prefixes` prefixes of `backlog` one-batch segments each, written with no
/// latency.
async fn backlog(bucket: &InMemory, prefixes: usize, backlog: usize) -> Result<()> {
    let writer = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        ..Default::default()
    });

    let batch = batch()?;

    for i in 0..prefixes {
        let topic = format!("{}.tab", prefix(i));

        _ = writer
            .create_topic(
                CreatableTopic::default()
                    .name(topic.clone())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let tp = Topition::new(topic, 0);
        for _ in 0..backlog {
            _ = writer.produce(None, &tp, batch.clone()).await?;
        }
    }

    Ok(())
}

async fn live(bucket: &InMemory, prefix: &str) -> Result<usize> {
    Ok(bucket
        .list(Some(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{prefix}/segments/"
        ))))
        .try_collect::<Vec<_>>()
        .await?
        .len())
}

/// What draining `prefixes` backlogs of `segments` each did: segments merged
/// per second, and the most GETs that were ever in flight at once.
async fn drain(
    prefixes: usize,
    segments: usize,
    concurrency: usize,
    run: usize,
    paced: bool,
) -> Result<(f64, u64)> {
    let bucket = InMemory::new();
    backlog(&bucket, prefixes, segments).await?;

    let counts = Arc::new(Counts::default());

    let store = Bucket {
        inner: bucket.clone(),
        counts: counts.clone(),
    };

    // As the `gs` arm builds it: one write per object name per second.
    let maintainer = if paced {
        DynoStore::new(
            CLUSTER,
            NODE,
            PutRateLimiter::new(store, Duration::from_mins(5))
                .with_rate_per_second(NonZeroU32::new(1)),
        )
        .object_write_interval(Some(Duration::from_secs(1)))
    } else {
        DynoStore::new(CLUSTER, NODE, store)
    }
    .coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(MIN_SEGMENTS),
        prefix_compact_keep_hot: Some(1),
        prefix_compact_target_bytes: Some(run * SEGMENT_BYTES),
        ..Default::default()
    });

    let prefixes: BTreeSet<String> = (0..prefixes).map(prefix).collect();
    for prefix in &prefixes {
        maintainer.refresh_prefix_index_forced(prefix).await?;
    }

    counts.most_getting.store(0, Ordering::Relaxed);
    let before = counts.snapshot();
    let started = Instant::now();
    let merged = maintainer
        .sweep_backlogged_prefixes(&prefixes, concurrency)
        .await;
    let elapsed = started.elapsed();
    let after = counts.snapshot();

    let mut remaining = Vec::new();
    for prefix in &prefixes {
        remaining.push(live(&bucket, prefix).await?);
    }

    let runs = merged as f64 / run as f64;
    let per_run = |i: usize| (after[i] - before[i]) as f64 / runs;
    let rate = merged as f64 / elapsed.as_secs_f64();

    eprintln!(
        "paced={paced} concurrency={concurrency} run={run} merged={merged} elapsed={elapsed:?} rate={rate:.1}/s \
         per ~run: get={:.1} put={:.1} copy={:.1} list={:.1} delete={:.1} remaining={remaining:?}",
        per_run(0),
        per_run(1),
        per_run(2),
        per_run(3),
        per_run(4),
    );

    Ok((rate, counts.most_getting.load(Ordering::Relaxed)))
}

/// A merge reads its run's segments concurrently: one GET at a time made the
/// reads the largest term of a merge on the bucket.
#[tokio::test]
async fn merge_reads_its_run_concurrently() -> Result<()> {
    let _guard = init_tracing()?;

    let (rate, most_getting) = drain(1, 48, 1, 22, false).await?;

    assert!(rate > 0.0);
    assert!(
        most_getting >= 8,
        "{most_getting} GETs in flight at most, merging runs of 22 one prefix at a time"
    );

    Ok(())
}

/// Seven backlogged prefixes, the shape of the bucket validation, at the
/// default concurrency, one slot per prefix, and twice the run length — then
/// again with `gs://`'s one write per object per second, which every merge
/// meets at its prefix's sequence floor.
#[tokio::test]
#[ignore = "a measurement, not a check, and untraced: its debug log outgrows the disk (DEV-662)"]
async fn merge_throughput_against_bucket_latencies() -> Result<()> {
    for paced in [false, true] {
        for (concurrency, run) in [(4, 22), (8, 22), (4, 44)] {
            _ = drain(7, 240, concurrency, run, paced).await?;
        }
    }

    Ok(())
}

/// What one merge costs in CPU as the prefix's live segment count grows, with
/// no request latency at all.
async fn merge_cpu(live_segments: usize) -> Result<Duration> {
    let bucket = InMemory::new();

    let writer = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        ..Default::default()
    });

    let topic = format!("{}.tab", prefix(0));
    _ = writer
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let mut small = inflated::Batch::builder();
    small = small.record(Record::builder().value(Some(Bytes::from_static(b"x"))));
    let small = small
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(crate::Error::from)?;

    let tp = Topition::new(topic, 0);
    for _ in 0..live_segments {
        _ = writer.produce(None, &tp, small.clone()).await?;
    }

    let maintainer =
        DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
            prefix_compact_min_segments: Some(MIN_SEGMENTS),
            prefix_compact_keep_hot: Some(1),
            prefix_compact_target_bytes: Some(22 * 80),
            ..Default::default()
        });

    let prefix = prefix(0);
    maintainer.refresh_prefix_index_forced(&prefix).await?;

    const MERGES: u32 = 20;
    let started = Instant::now();
    for _ in 0..MERGES {
        _ = maintainer.compact_prefix_segments(&prefix).await?;
    }
    let per_merge = started.elapsed() / MERGES;

    eprintln!("live={live_segments} per_merge={per_merge:?}");
    Ok(per_merge)
}

#[tokio::test]
#[ignore = "a measurement, not a check, and untraced: its debug log outgrows the disk (DEV-662)"]
async fn merge_cpu_against_live_segments() -> Result<()> {
    for live in [1_000, 5_000, 20_000] {
        _ = merge_cpu(live).await?;
    }

    Ok(())
}
