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

//! How many segments a merge retires under sustained produce (DEV-692).
//!
//! On the GCS bucket each hot prefix took ~12 producer segments of ~80 KB a
//! second and landed about one merge a second: the claim round trip plus the
//! copy. Merges took a median of 9 segments where the 16 MiB target allows
//! ~200, so the live count grew by ~50 a minute and never came back to the
//! trigger.
//!
//! The rate that matters is the ratio — segments produced per merge — so this
//! models it in steps rather than wall time: each step produces
//! [`PER_MERGE`] one-batch segments round-robin over three partitions, then
//! the maintainer refreshes and attempts one merge. The trigger and `keep_hot`
//! are the shipped defaults; the target is the shipped 16 MiB scaled to the
//! segment size, ~200 segments a run as on the bucket.

use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    LogStorage as _, MetadataStorage as _, Result, Topition,
    dynostore::{CoalesceTuning, CompactRun, DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "org.env.load";
const TOPIC: &str = "org.env.load.tab";
const PARTITIONS: i32 = 3;

const RECORDS: usize = 8;
const VALUE_BYTES: usize = 512;

/// One producer segment's region bytes, about: the bucket's ~80 KB scaled down.
const SEGMENT_BYTES: usize = RECORDS * VALUE_BYTES;

/// The shipped 16 MiB target over the bucket's ~80 KB segments.
const RUN: usize = 200;

/// Producer segments per merge attempt: ~12 created a second, ~1 merge a second.
const PER_MERGE: usize = 12;

/// The shipped trigger and hot tail.
const MIN_SEGMENTS: usize = DynoStore::PREFIX_COMPACT_MIN_SEGMENTS;
const KEEP_HOT: usize = DynoStore::PREFIX_COMPACT_KEEP_HOT;

/// An in-memory bucket counting the bytes every PUT carries.
#[derive(Clone)]
struct Counted {
    inner: InMemory,
    put_bytes: Arc<AtomicU64>,
}

impl Debug for Counted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Counted").finish()
    }
}

impl Display for Counted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Counted").finish()
    }
}

#[async_trait]
impl ObjectStore for Counted {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        _ = self
            .put_bytes
            .fetch_add(payload.content_length() as u64, Ordering::Relaxed);
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
        options: CopyOptions,
    ) -> Result<(), object_store::Error> {
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

/// Every record of `partition`, read back from offset 0.
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

/// What `steps` steps of sustained produce left behind.
#[derive(Debug)]
struct Sustained {
    /// Segments retired by each merge, in order.
    merged: Vec<u64>,

    /// Live segments, claims excluded, after each step.
    live: Vec<usize>,

    /// Bytes the merges wrote over bytes the producer wrote.
    amplification: f64,
}

impl Sustained {
    fn quantile(&self, q: f64) -> u64 {
        let mut sorted = self.merged.clone();
        sorted.sort_unstable();
        sorted
            .get(((sorted.len() as f64 - 1.0) * q).round() as usize)
            .copied()
            .unwrap_or_default()
    }
}

fn live(maintainer: &DynoStore) -> Result<usize> {
    let index = maintainer.prefixes.index()?;
    Ok(index.get(PREFIX).map_or(0, |entry| {
        entry
            .segments
            .values()
            .filter(|cached| !DynoStore::is_compaction_claim(&cached.footer))
            .count()
    }))
}

async fn sustain(backlog: usize, steps: usize) -> Result<Sustained> {
    let bucket = InMemory::new();
    let put_bytes = Arc::new(AtomicU64::new(0));
    let store = Counted {
        inner: bucket.clone(),
        put_bytes: put_bytes.clone(),
    };

    let producer = DynoStore::new(CLUSTER, NODE, store.clone()).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        ..Default::default()
    });

    let maintainer = DynoStore::new(CLUSTER, NODE, store).coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(MIN_SEGMENTS),
        prefix_compact_keep_hot: Some(KEEP_HOT),
        prefix_compact_target_bytes: Some(RUN * SEGMENT_BYTES),
        ..Default::default()
    });

    _ = producer
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

    let batch = batch()?;
    let (mut produced, mut rewritten) = (0u64, 0u64);
    let mut merged = Vec::new();
    let mut lives = Vec::new();
    let mut acked = [0i64; PARTITIONS as usize];

    for n in 0..backlog {
        let partition = (n % PARTITIONS as usize) as i32;
        _ = producer
            .produce(None, &Topition::new(TOPIC, partition), batch.clone())
            .await?;
        acked[partition as usize] += 1;
    }
    produced += put_bytes.load(Ordering::Relaxed);

    for step in 0..steps {
        let before = put_bytes.load(Ordering::Relaxed);
        for n in 0..PER_MERGE {
            let partition = ((backlog + step * PER_MERGE + n) % PARTITIONS as usize) as i32;
            _ = producer
                .produce(None, &Topition::new(TOPIC, partition), batch.clone())
                .await?;
            acked[partition as usize] += 1;
        }
        let between = put_bytes.load(Ordering::Relaxed);
        produced += between - before;

        maintainer.refresh_prefix_index_forced(PREFIX).await?;
        if let CompactRun::Merged(n) = maintainer.compact_prefix_segments(PREFIX).await? {
            merged.push(n);
        }
        rewritten += put_bytes.load(Ordering::Relaxed) - between;

        maintainer.refresh_prefix_index_forced(PREFIX).await?;
        lives.push(live(&maintainer)?);
    }

    let reader = DynoStore::new(CLUSTER, NODE, bucket.clone());
    for (partition, acked) in (0..PARTITIONS).zip(acked) {
        assert_eq!(
            acked * RECORDS as i64,
            read_back(&reader, partition).await?,
            "partition {partition} lost or duplicated records"
        );
    }

    let sustained = Sustained {
        merged,
        live: lives,
        amplification: rewritten as f64 / produced as f64,
    };

    eprintln!(
        "backlog={backlog} steps={steps} merges={} segments/merge p10={} p50={} p90={} max={} \
         amplification={:.2} live@{{25%,50%,75%,100%}}={:?}",
        sustained.merged.len(),
        sustained.quantile(0.1),
        sustained.quantile(0.5),
        sustained.quantile(0.9),
        sustained.quantile(1.0),
        sustained.amplification,
        [steps / 4, steps / 2, 3 * steps / 4, steps].map(|at| sustained
            .live
            .get(at.saturating_sub(1))
            .copied()
            .unwrap_or_default()),
    );

    Ok(sustained)
}

/// Under sustained produce a merge retires about as many segments as the
/// target holds, and the live count stays bounded near the trigger (DEV-692).
///
/// The backlog is the shape a maintenance interval leaves: once it is drained,
/// every merged segment lands among the producer segments created meanwhile.
/// Before DEV-692 each run stopped at the next one: a median of 12 segments a
/// merge, live segments growing from 319 to 1 078 over the steps, and 1.23
/// bytes rewritten per byte produced as small outputs were merged again.
#[tokio::test]
async fn sustained_produce_merges_full_runs() -> Result<()> {
    let _guard = init_tracing()?;

    const BACKLOG: usize = 2_000;
    const STEPS: usize = 300;

    let sustained = sustain(BACKLOG, STEPS).await?;

    let median = sustained.quantile(0.5);
    assert!(
        median as usize >= RUN / 2,
        "a median of {median} segments per merge, where the target holds {RUN}"
    );

    let most = sustained
        .live
        .iter()
        .skip(STEPS / 2)
        .copied()
        .max()
        .unwrap_or_default();
    assert!(
        most <= MIN_SEGMENTS + RUN + KEEP_HOT,
        "{most} live segments in the second half, against a trigger of {MIN_SEGMENTS}"
    );

    assert!(
        sustained.amplification < 1.1,
        "{:.2} bytes rewritten per byte produced",
        sustained.amplification
    );

    Ok(())
}
