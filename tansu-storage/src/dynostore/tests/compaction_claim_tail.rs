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

//! A compaction claim that starts behind the tail lands while producers take
//! names faster than it loses rounds (DEV-692).
//!
//! On the GCS bucket, after ~1h30 of sustained produce, a prefix took a name
//! every ~83 ms and a lost claim round lasted ~93 ms. The claim climbed one
//! name per lost round, so once it was behind the tail it fell further behind
//! with every round: each merge whose claim lost its first round lost all 256
//! and failed. Merges fell from 1 500 to 320 per 10 min and one prefix grew to
//! 16 912 segments.
//!
//! The store here plays those producers: the first claim finds [`AHEAD`] names
//! taken past it, the next ones lose the race for the name they ask for, and
//! every round the claim loses, producers take [`PER_ROUND`] more. A claim
//! that climbs one name a round never catches up; one that reads where the
//! tail is lands as soon as it wins a race.

use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{TryStreamExt as _, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tansu_sans_io::{
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    LogStorage as _, MetadataStorage as _, Result, Topition,
    dynostore::{CoalesceTuning, CompactRun, DynoStore, SEGMENT_FORMAT_VERSION_V3},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "org.env.load";
const TOPIC: &str = "org.env.load.tab";

/// Names producers took past the claim's first candidate.
const AHEAD: u64 = 40;

/// Names producers take during each round the claim loses.
const PER_ROUND: u64 = 2;

/// Rounds in which a producer takes the very name the claim asks for, as one
/// already uploading there would: the race at the tail, lost.
const RACED: u64 = 3;

const BACKLOG: usize = 32;

/// The maintainer's store: producers take names around its claims.
#[derive(Clone)]
struct Producers {
    inner: InMemory,
    segments: Path,
    taken: PutPayload,
    armed: Arc<AtomicBool>,
    lost: Arc<AtomicU64>,
}

impl Debug for Producers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Producers").finish()
    }
}

impl Display for Producers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Producers").finish()
    }
}

impl Producers {
    /// The highest sequence under `segments/`.
    async fn tail(&self) -> Result<u64, object_store::Error> {
        Ok(self
            .inner
            .list(Some(&self.segments))
            .try_collect::<Vec<_>>()
            .await?
            .iter()
            .filter_map(|meta| DynoStore::segment_seq_of(&meta.location))
            .max()
            .unwrap_or_default())
    }

    /// A producer segment at each name in `from..to` not already taken.
    async fn take(&self, from: u64, to: u64) -> Result<(), object_store::Error> {
        for seq in from..to {
            let location = self.segments.clone().join(format!("{seq:0>20}.seg"));

            match self
                .inner
                .put_opts(&location, self.taken.clone(), PutMode::Create.into())
                .await
            {
                Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
                Err(error) => return Err(error),
            }
        }

        Ok(())
    }
}

#[async_trait]
impl ObjectStore for Producers {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        let claim = matches!(options.mode, PutMode::Create)
            && location.prefix_matches(&self.segments)
            && self.armed.load(Ordering::Relaxed);

        if claim {
            let seq = DynoStore::segment_seq_of(location).expect("a segment name");
            let lost = self.lost.load(Ordering::Relaxed);

            if lost == 0 {
                self.take(seq, seq + AHEAD).await?;
            } else if lost < RACED {
                self.take(seq, seq + 1).await?;
            }
        }

        let put = self.inner.put_opts(location, payload, options).await;

        if claim && matches!(put, Err(object_store::Error::AlreadyExists { .. })) {
            _ = self.lost.fetch_add(1, Ordering::Relaxed);

            let tail = self.tail().await?;
            self.take(tail + 1, tail + 1 + PER_ROUND).await?;
        }

        put
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
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"x"))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

#[tokio::test]
async fn a_claim_behind_producers_that_outpace_its_rounds_lands() -> Result<()> {
    let bucket = InMemory::new();

    let writer = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
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
    let batch = batch()?;
    for _ in 0..BACKLOG {
        _ = writer.produce(None, &tp, batch.clone()).await?;
    }

    let (taken, _) = writer.encode_segment_indexed(&[], 0, 0, SEGMENT_FORMAT_VERSION_V3)?;
    let producers = Producers {
        inner: bucket.clone(),
        segments: writer.segment_prefix(PREFIX),
        taken,
        armed: Arc::default(),
        lost: Arc::default(),
    };

    let maintainer =
        DynoStore::new(CLUSTER, NODE, producers.clone()).coalesce_tuning(CoalesceTuning {
            prefix_compact_min_segments: Some(16),
            prefix_compact_keep_hot: Some(1),
            ..Default::default()
        });
    maintainer.refresh_prefix_index_forced(PREFIX).await?;

    producers.armed.store(true, Ordering::Relaxed);
    let merged = maintainer.compact_prefix_segments(PREFIX).await;
    let lost = producers.lost.load(Ordering::Relaxed);

    assert!(
        matches!(merged, Ok(CompactRun::Merged(_))),
        "{merged:?} after {lost} lost claim rounds"
    );
    assert!(lost <= RACED, "{lost} claim rounds lost behind producers");

    Ok(())
}

/// The name after a lost one is past every name taken since, and past the
/// lost one itself when nothing was.
#[tokio::test]
async fn the_next_name_after_a_lost_one_is_past_the_tail() -> Result<()> {
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());
    let (taken, _) = store.encode_segment_indexed(&[], 0, 0, SEGMENT_FORMAT_VERSION_V3)?;

    for seq in [3, 5, 9] {
        _ = store
            .object_store
            .put_opts(
                &store.segment_location(PREFIX, seq),
                taken.clone(),
                PutMode::Create.into(),
            )
            .await?;
    }

    assert_eq!(10, store.next_after_taken(PREFIX, 4).await?);
    assert_eq!(10, store.next_after_taken(PREFIX, 9).await?);
    assert_eq!(21, store.next_after_taken(PREFIX, 20).await?);

    Ok(())
}
