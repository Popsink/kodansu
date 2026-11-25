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
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Debug, Display},
    num::NonZeroU32,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
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
    floor_put: AtomicU64,
    writes: Mutex<BTreeMap<Path, u64>>,
}

impl Counts {
    /// Count a PUT or a copy landing on `location`.
    fn wrote(&self, location: &Path) {
        if let Ok(mut writes) = self.writes.lock() {
            *writes.entry(location.clone()).or_default() += 1;
        }
    }

    /// The most writes any one object name took since the last call.
    fn take_most_writes(&self) -> u64 {
        self.writes
            .lock()
            .map(|mut writes| {
                std::mem::take(&mut *writes)
                    .into_values()
                    .max()
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }

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
        self.counts.wrote(location);
        if location.filename() == Some("seq-floor.json") {
            _ = self.counts.floor_put.fetch_add(1, Ordering::Relaxed);
        }
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
        self.counts.wrote(to);
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

/// How many segments `prefix` holds, having checked its sequence floor
/// against them: every name freed below the highest live one is under the
/// floor, so none can be handed out again (#77), and the floor is no further
/// ahead than the next name (DEV-675).
async fn live(bucket: &InMemory, prefix: &str) -> Result<usize> {
    let seqs: BTreeSet<u64> = bucket
        .list(Some(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{prefix}/segments/"
        ))))
        .try_collect::<Vec<_>>()
        .await?
        .iter()
        .filter_map(|meta| meta.location.filename()?.get(..20)?.parse().ok())
        .collect();

    let floor = match bucket
        .get(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{prefix}/seq-floor.json"
        )))
        .await
    {
        Ok(result) => {
            serde_json::from_slice::<serde_json::Value>(&result.bytes().await?)?["next_seq_floor"]
                .as_u64()
                .unwrap_or_default()
        }
        Err(object_store::Error::NotFound { .. }) => 0,
        Err(error) => return Err(error.into()),
    };

    let freed = seqs
        .last()
        .and_then(|max| (0..*max).rev().find(|seq| !seqs.contains(seq)));

    assert!(
        freed.is_none_or(|freed| freed < floor),
        "{prefix}: {freed:?} was freed under a seq floor of {floor}"
    );

    // The tail is never merged (`keep_hot`), so the highest live name is the
    // highest ever created: a floor past it + 1 would make writers skip names.
    assert!(
        seqs.last().is_none_or(|max| floor <= max + 1),
        "{prefix}: seq floor {floor} is ahead of the tail {:?}",
        seqs.last()
    );

    Ok(seqs.len())
}

/// What draining a backlog did.
#[derive(Clone, Copy, Debug)]
struct Drained {
    /// Segments merged per second.
    rate: f64,

    /// Segments merged away, over every prefix.
    merged: u64,

    /// Writes of `seq-floor.json`, over every prefix.
    floor_puts: u64,

    /// The most GETs that were ever in flight at once.
    most_getting: u64,

    /// The most writes any one object name took during the drain: `gs://`
    /// paces each name at one write a second.
    most_writes: u64,

    /// The most live segments any prefix was left with.
    most_remaining: usize,
}

/// Drain `prefixes` backlogs of `segments` each, merging runs of `run`
/// segments into a target of `run` segments plus `slack` bytes.
async fn drain(
    prefixes: usize,
    segments: usize,
    concurrency: usize,
    (run, slack): (usize, usize),
    paced: bool,
) -> Result<Drained> {
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
        prefix_compact_target_bytes: Some(run * SEGMENT_BYTES + slack),
        ..Default::default()
    });

    let prefixes: BTreeSet<String> = (0..prefixes).map(prefix).collect();
    for prefix in &prefixes {
        maintainer.refresh_prefix_index_forced(prefix).await?;
    }

    counts.most_getting.store(0, Ordering::Relaxed);
    let before = counts.snapshot();
    let floor_before = counts.floor_put.load(Ordering::Relaxed);
    _ = counts.take_most_writes();
    let started = Instant::now();
    let merged = maintainer
        .sweep_backlogged_prefixes(&prefixes, concurrency, started + Duration::from_mins(30))
        .await;
    let elapsed = started.elapsed();
    let after = counts.snapshot();
    let floor_puts = counts.floor_put.load(Ordering::Relaxed) - floor_before;
    let most_writes = counts.take_most_writes();

    let mut remaining = Vec::new();
    for prefix in &prefixes {
        remaining.push(live(&bucket, prefix).await?);
    }

    let runs = merged as f64 / run as f64;
    let per_run = |i: usize| (after[i] - before[i]) as f64 / runs;
    let rate = merged as f64 / elapsed.as_secs_f64();

    eprintln!(
        "paced={paced} concurrency={concurrency} run={run} merged={merged} elapsed={elapsed:?} rate={rate:.1}/s \
         floor_puts={floor_puts} per ~run: get={:.1} put={:.1} copy={:.1} list={:.1} delete={:.1} \
         remaining={remaining:?}",
        per_run(0),
        per_run(1),
        per_run(2),
        per_run(3),
        per_run(4),
    );

    Ok(Drained {
        rate,
        merged,
        floor_puts,
        most_getting: counts.most_getting.load(Ordering::Relaxed),
        most_writes,
        most_remaining: remaining.into_iter().max().unwrap_or_default(),
    })
}

/// A merge reads its run's segments concurrently: one GET at a time made the
/// reads the largest term of a merge on the bucket.
#[tokio::test]
async fn merge_reads_its_run_concurrently() -> Result<()> {
    let _guard = init_tracing()?;

    let drained = drain(1, 48, 1, (22, 0), false).await?;

    assert!(drained.rate > 0.0);
    assert!(
        drained.most_getting >= 8,
        "{} GETs in flight at most, merging runs of 22 one prefix at a time",
        drained.most_getting
    );

    Ok(())
}

/// A backlog pass moves the sequence floor a couple of times, not once per
/// merge (DEV-675): `gs://` paces `seq-floor.json` at one write a second, so a
/// write per merge capped every prefix at one merge a second.
#[tokio::test]
async fn seq_floor_moves_once_per_pass_not_per_merge() -> Result<()> {
    let _guard = init_tracing()?;

    const BACKLOG: usize = 240;
    const RUN: usize = 22;

    let drained = drain(1, BACKLOG, 1, (RUN, 0), false).await?;
    let merges = drained.merged / RUN as u64;

    assert!(
        merges >= 10,
        "{merges} merges of {BACKLOG} segments in runs of {RUN}"
    );
    assert!(
        drained.floor_puts <= 2,
        "{} seq-floor.json writes for {merges} merges",
        drained.floor_puts
    );
    assert!(
        drained.most_remaining <= MIN_SEGMENTS,
        "{} segments left, claims included",
        drained.most_remaining
    );

    Ok(())
}

/// A merge's claim stays behind it until the floor covers it, and must never
/// be selected into a run (DEV-675).
///
/// A target with room for a merged segment and an empty claim, but not for a
/// merged segment and one more producer segment, is the shape that undid the
/// naive version: every run was a merged segment and the claim behind it, two
/// in, a merged segment and a new claim out, until the drain ran out of runs.
/// On the bucket that was 30 191 segments "merged" out of 1 680.
#[tokio::test]
async fn claims_left_behind_are_never_merged() -> Result<()> {
    let _guard = init_tracing()?;

    const BACKLOG: usize = 120;
    const RUN: usize = 6;

    // Bounded: the naive version does not fail, it spins until the drain's
    // own bounds stop it, minutes later.
    let drained = tokio::time::timeout(
        Duration::from_secs(60),
        drain(1, BACKLOG, 1, (RUN, SEGMENT_BYTES / 2), false),
    )
    .await
    .expect("the drain re-merged claims without end")?;

    assert!(
        drained.merged <= BACKLOG as u64,
        "{} segments merged out of a backlog of {BACKLOG}",
        drained.merged
    );
    // Each merged segment is too big to take a neighbour, so it is all that
    // is left: one per run, and no claim beside it.
    assert!(
        drained.most_remaining <= BACKLOG / RUN,
        "{} segments left, claims included",
        drained.most_remaining
    );

    Ok(())
}

/// Seven backlogged prefixes, the shape of the bucket validation, at the
/// default concurrency, one slot per prefix, and twice the run length — then
/// again with `gs://`'s one write per object per second. Before DEV-675 every
/// merge met that pacing at its prefix's sequence floor, so a slot took ~1.2 s
/// per merge paced against ~0.1 s unpaced.
#[tokio::test]
#[ignore = "a measurement, not a check, and untraced: its debug log outgrows the disk (DEV-662)"]
async fn merge_throughput_against_bucket_latencies() -> Result<()> {
    for paced in [false, true] {
        for (concurrency, run) in [(4, 22), (8, 22), (4, 44)] {
            _ = drain(7, 240, concurrency, (run, 0), paced).await?;
        }
    }

    Ok(())
}

/// One slot's merges are no longer floored at a second each under `gs://`'s
/// pacing (DEV-675): paced, a backlog drains about as fast as unpaced.
///
/// A merge is floored when it writes one object name every merge, so that is
/// checked by count, whatever the runner's speed. The time bound is the floor
/// itself: a floored merge takes a second at least. It used to be 0.6 s,
/// against ~0.45 s on a workstation and 0.61 s on a CI runner (#768).
#[tokio::test]
async fn paced_merges_are_not_floored_at_a_second() -> Result<()> {
    let _guard = init_tracing()?;

    const BACKLOG: usize = 160;
    const RUN: usize = 22;

    let unpaced = drain(1, BACKLOG, 1, (RUN, 0), false).await?;
    let paced = drain(1, BACKLOG, 1, (RUN, 0), true).await?;

    let merges = paced.merged / RUN as u64;
    let per_merge = paced.merged as f64 / paced.rate / merges as f64;

    assert!(
        paced.most_writes * 2 <= merges,
        "one object written {} times over {merges} paced merges",
        paced.most_writes
    );
    assert!(
        per_merge < 1.0,
        "{per_merge:.2}s per paced merge over {merges} merges, {:.1}/s unpaced against {:.1}/s paced",
        unpaced.rate,
        paced.rate
    );
    assert!(
        paced.rate * 2.0 > unpaced.rate,
        "{:.1}/s paced against {:.1}/s unpaced",
        paced.rate,
        unpaced.rate
    );

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
