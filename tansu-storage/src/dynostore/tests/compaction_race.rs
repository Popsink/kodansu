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

//! Compaction lands merged segments while producers keep taking the tail
//! sequence (DEV-662, #130).
//!
//! A merged segment is created at the next tail sequence, create-only, through
//! the producers' own CAS. Uploaded straight to that name, the create is decided
//! when the upload *finishes*, so it only wins if no producer takes the name in
//! the meantime. On the DEV-662 GCS run a 16 MiB merge took ~0.5 s to upload
//! while producers took a sequence every ~70 ms: all 64 attempts lost on every
//! pass, no merged segment ever landed, and a prefix reached 38 000 segments in
//! 46 minutes.
//!
//! The store here reproduces that shape at test scale: every PUT costs a
//! request latency plus a per-byte transfer time, and the create-only
//! precondition is evaluated when the transfer ends, as S3 and GCS do. A
//! server-side copy costs the request latency alone — it moves no bytes over
//! the client's link. A producer takes a sequence every few milliseconds, far
//! less than a merged upload takes.

use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tansu_sans_io::{
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use tokio::time::{Instant, sleep};

use crate::{
    MetadataStorage as _, Result, Topition,
    dynostore::{CoalesceTuning, CompactRun, DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "org.env.load";
const TOPIC: &str = "org.env.load.tab";

/// Records per producer segment, of [`VALUE_BYTES`] each: ~16 KiB a segment.
const RECORDS: usize = 16;
const VALUE_BYTES: usize = 1_024;

/// Round trip of any request, before its body is transferred.
const REQUEST: Duration = Duration::from_millis(2);

/// Transfer time per MiB of PUT body: a 1 MiB merge takes ~50 ms to land, ten
/// producer intervals.
const PER_MIB: Duration = Duration::from_millis(50);

/// How often the producer takes the next sequence.
const PRODUCE_EVERY: Duration = Duration::from_millis(5);

/// How long the producer runs.
const PRODUCE_FOR: Duration = Duration::from_secs(3);

const MIN_SEGMENTS: usize = 32;
const TARGET_BYTES: usize = 1 << 20;

/// An object store whose PUT latency grows with the payload, deciding a
/// create-only precondition when the transfer ends.
struct UploadLatency {
    inner: InMemory,
}

impl UploadLatency {
    fn new(inner: InMemory) -> Self {
        Self { inner }
    }

    fn transfer(bytes: usize) -> Duration {
        REQUEST + PER_MIB.mul_f64(bytes as f64 / (1 << 20) as f64)
    }
}

impl Debug for UploadLatency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadLatency").finish()
    }
}

impl Display for UploadLatency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadLatency").finish()
    }
}

#[async_trait]
impl ObjectStore for UploadLatency {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        sleep(Self::transfer(payload.content_length())).await;
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
        sleep(REQUEST).await;
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

fn segment_location(seq: u64) -> Path {
    Path::from(format!(
        "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{seq:0>20}.seg"
    ))
}

/// The leaseless arbiter reduced to its shape: claim the next sequence
/// create-only, step past one a peer holds, and advance the offset only on a
/// win. Returns how many segments it created.
async fn produce(writer: DynoStore, store: UploadLatency, stop: Arc<AtomicBool>) -> Result<u64> {
    let batch = batch()?;
    let (mut seq, mut base, mut created) = (0u64, 0i64, 0u64);

    while !stop.load(Ordering::Relaxed) {
        let (payload, _footer) = writer.encode_segment_v3(
            &[(Topition::new(TOPIC, 0), base, vec![batch.clone()])],
            1,
            seq,
        )?;

        match store
            .put_opts(
                &segment_location(seq),
                payload,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {
                base += RECORDS as i64;
                created += 1;
            }

            Err(object_store::Error::AlreadyExists { .. }) => {}

            Err(error) => return Err(error.into()),
        }

        seq += 1;
        sleep(PRODUCE_EVERY).await;
    }

    Ok(created)
}

async fn live_segments(bucket: &InMemory) -> Result<usize> {
    bucket
        .list(Some(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/"
        ))))
        .try_collect::<Vec<_>>()
        .await
        .map(|listed| listed.len())
        .map_err(Into::into)
}

#[tokio::test]
async fn compaction_lands_while_producers_take_the_tail() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();

    let maintainer = DynoStore::new(CLUSTER, NODE, UploadLatency::new(bucket.clone()))
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
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let stop = Arc::new(AtomicBool::new(false));
    let producer = tokio::spawn(produce(
        DynoStore::new(CLUSTER, NODE, bucket.clone()),
        UploadLatency::new(bucket.clone()),
        stop.clone(),
    ));

    let started = Instant::now();
    let (mut merges, mut merged, mut failed) = (0u64, 0u64, 0u64);

    while started.elapsed() < PRODUCE_FOR {
        maintainer.refresh_prefix_index_forced(PREFIX).await?;

        match maintainer.compact_prefix_segments(PREFIX).await {
            Ok(CompactRun::Merged(segments)) => {
                merges += 1;
                merged += segments;
            }

            Ok(CompactRun::Drained | CompactRun::Retry) => sleep(PRODUCE_EVERY).await,

            Err(_) => failed += 1,
        }
    }

    stop.store(true, Ordering::Relaxed);
    let produced = producer.await.expect("the producer panicked")?;
    let live = live_segments(&bucket).await?;

    let staged = bucket
        .list(Some(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{PREFIX}/staging/"
        ))))
        .count()
        .await;

    eprintln!(
        "produced={produced} live={live} merges={merges} merged={merged} failed_passes={failed} \
         elapsed={:?}",
        started.elapsed()
    );

    assert!(
        merges > 0,
        "no merged segment landed under sustained produce"
    );

    assert!(
        live <= 4 * MIN_SEGMENTS,
        "{live} live segments of {produced} produced: compaction fell behind"
    );

    assert_eq!(0, staged, "a staged payload outlived its merge");

    Ok(())
}
