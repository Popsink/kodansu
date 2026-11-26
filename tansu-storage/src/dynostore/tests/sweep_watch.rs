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

//! A prefix produced to stays in the backlog sweep however long another
//! prefix takes to list (DEV-692).
//!
//! On the GCS bucket, `load-2` was drained below the trigger at 11:44:03 and
//! then not visited until the next tick at 11:50:14, while producers kept
//! writing to it at ~12 segments a second: it grew to 4 480 segments. The
//! sweep keeps a prefix under the trigger only while its newest segment is
//! under two seconds old, and it judged that age after listing *every*
//! watched prefix, so one slow listing aged every other prefix's evidence
//! past the window.

use std::{
    collections::BTreeSet,
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
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
    LogStorage as _, MetadataStorage as _, Result, Topition,
    dynostore::{CoalesceTuning, DynoStore},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

const HOT: &str = "org.env.hot";
const SLOW: &str = "org.env.slow";

/// How long a listing of [`SLOW`] takes: past the sweep's two seconds.
const SLOW_LISTING: Duration = Duration::from_secs(3);

/// A producer segment on [`HOT`] this often.
const PRODUCE_EVERY: Duration = Duration::from_millis(100);

const MIN_SEGMENTS: usize = 16;
const PASS: Duration = Duration::from_secs(6);

/// A store whose listings of [`SLOW`] take [`SLOW_LISTING`].
#[derive(Clone)]
struct SlowListing {
    inner: InMemory,
}

impl Debug for SlowListing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlowListing").finish()
    }
}

impl Display for SlowListing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlowListing").finish()
    }
}

impl SlowListing {
    fn listed(
        prefix: Option<&Path>,
        listing: BoxStream<'static, Result<ObjectMeta, object_store::Error>>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        let slow = prefix.is_some_and(|prefix| prefix.as_ref().contains(&format!("/{SLOW}/")));

        futures::stream::once(async move {
            if slow {
                sleep(SLOW_LISTING).await;
            }
            listing
        })
        .flatten()
        .boxed()
    }
}

#[async_trait]
impl ObjectStore for SlowListing {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
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
        Self::listed(prefix, self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        Self::listed(prefix, self.inner.list_with_offset(prefix, offset))
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
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"x"))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

fn topic(prefix: &str) -> Topition {
    Topition::new(format!("{prefix}.t"), 0)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prefix_produced_to_stays_swept_behind_a_slow_listing() -> Result<()> {
    let bucket = InMemory::new();

    let writer = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        ..Default::default()
    });

    for prefix in [HOT, SLOW] {
        _ = writer
            .create_topic(
                CreatableTopic::default()
                    .name(topic(prefix).topic().into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;
    }

    let batch = batch()?;
    for _ in 0..4 {
        _ = writer.produce(None, &topic(SLOW), batch.clone()).await?;
    }

    let maintainer = DynoStore::new(
        CLUSTER,
        NODE,
        SlowListing {
            inner: bucket.clone(),
        },
    )
    .coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(MIN_SEGMENTS),
        prefix_compact_keep_hot: Some(1),
        ..Default::default()
    });

    let stop = Arc::new(AtomicBool::new(false));
    let producer = tokio::spawn({
        let (writer, stop, batch) = (writer.clone(), stop.clone(), batch.clone());
        async move {
            while !stop.load(Ordering::Relaxed) {
                _ = writer.produce(None, &topic(HOT), batch.clone()).await?;
                sleep(PRODUCE_EVERY).await;
            }
            Ok::<_, crate::Error>(())
        }
    });

    sleep(PRODUCE_EVERY * 4).await;

    let compactable = BTreeSet::from([HOT.to_owned(), SLOW.to_owned()]);
    let started = Instant::now();
    let merged = maintainer
        .sweep_backlogged_prefixes(&compactable, 2, started + PASS)
        .await;
    let swept_for = started.elapsed();

    stop.store(true, Ordering::Relaxed);
    producer.await.expect("the producer panicked")?;

    let hot = live(&bucket, HOT).await?;
    eprintln!("merged={merged} swept_for={swept_for:?} hot_live={hot}");

    assert!(
        merged > 0,
        "nothing merged on {HOT} in {swept_for:?} while it grew to {hot} segments"
    );
    assert!(
        hot <= 2 * MIN_SEGMENTS,
        "{HOT} grew to {hot} segments against a trigger of {MIN_SEGMENTS}"
    );

    Ok(())
}
