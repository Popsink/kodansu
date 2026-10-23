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

//! A refresh whose footer GET 404s re-lists before it trusts its listing
//! (Popsink/data-plane#5470).
//!
//! A cold build LISTs the prefix, then GETs each footer it does not hold. A peer
//! that merges a run between the two writes the merged segment at a sequence
//! above the listing and then deletes the originals, so every footer GET of the
//! run 404s. The refresh recorded those names as stepped over and stamped the
//! index fresh: it held neither the originals nor their replacement, and a
//! `Fetch` inside the run was served the next segment the index did hold — past
//! the requested offset, with nothing telling the consumer it had skipped.
//!
//! On use1 that was 64 records of one partition, while the brokers restarted
//! cold under a maintainer merging ~540 runs an hour. The bucket held them; the
//! producer's sequences had no gap.
//!
//! The merge is injected at the reader's first footer GET, which is held until
//! the merge, deletions included, has finished: the window the listing cannot
//! see, made deterministic.

use std::{
    fmt::{self, Debug, Display},
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use tokio::sync::Mutex;

use crate::{
    Result, Storage as _, Topition,
    dynostore::{CoalesceTuning, DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

const TOPIC: &str = "org.env.conn.table";

/// Segments produced: the merge folds all but the newest, which stays hot and
/// is the segment a reader with a hole was served instead.
const SEGMENTS: i64 = 4;

/// Runs a peer's merge the first time a footer is read through it, and holds
/// that read, and every read queued behind it, until the merge is done.
struct MergeOnFooterRead<O> {
    inner: O,
    peer: Arc<Mutex<Option<DynoStore>>>,
}

impl<O> Debug for MergeOnFooterRead<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MergeOnFooterRead").finish()
    }
}

impl<O> Display for MergeOnFooterRead<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MergeOnFooterRead").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for MergeOnFooterRead<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
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
        if location.as_ref().ends_with(".seg") && matches!(options.range, Some(GetRange::Suffix(_)))
        {
            let mut peer = self.peer.lock().await;
            if let Some(peer) = peer.take() {
                peer.maintain(SystemTime::now()).await.map_err(|error| {
                    object_store::Error::Generic {
                        store: "MergeOnFooterRead",
                        source: Box::new(error),
                    }
                })?;
            }
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

fn batch(n: i64) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from(format!("v{n}")))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// A reader whose cold build races a peer's merge of everything but the hot
/// segment serves every offset, each from a batch starting exactly there.
#[tokio::test]
async fn a_cold_build_racing_a_merge_serves_every_offset_once() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();

    let writer = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        prefix_compact_min_segments: Some(2),
        prefix_compact_keep_hot: Some(1),
        prefix_compact_target_bytes: Some(1 << 20),
        ..Default::default()
    });

    _ = writer
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

    let tp = Topition::new(TOPIC, 0);
    for n in 0..SEGMENTS {
        _ = writer.produce(None, &tp, batch(n)?).await?;
    }

    let peer = Arc::new(Mutex::new(Some(writer)));

    let reader = DynoStore::new(
        CLUSTER,
        NODE,
        MergeOnFooterRead {
            inner: bucket.clone(),
            peer: peer.clone(),
        },
    )
    .coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        ..Default::default()
    });

    let mut served = Vec::new();
    let mut offset = 0;

    while offset < SEGMENTS {
        let batches = reader
            .fetch(
                &tp,
                offset,
                1,
                100_000,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(200),
            )
            .await?;

        let first = batches
            .first()
            .unwrap_or_else(|| panic!("nothing served at offset {offset}"));
        assert_eq!(
            offset, first.base_offset,
            "a fetch at {offset} was served a batch starting at {}",
            first.base_offset
        );

        for batch in batches {
            served.push(batch.base_offset);
            offset = batch.base_offset + i64::from(batch.last_offset_delta) + 1;
        }
    }

    assert!(
        peer.lock().await.is_none(),
        "the merge must have raced the reader's build for this to test anything"
    );
    assert_eq!((0..SEGMENTS).collect::<Vec<_>>(), served);

    Ok(())
}
