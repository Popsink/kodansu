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

//! How `OffsetCommit` reports a storage failure to the client (#275).

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use std::fmt::{self, Debug, Display};
use tansu_sans_io::{ErrorCode, create_topics_request::CreatableTopic};

use crate::{
    OffsetCommitRequest, Result, Storage, Topition,
    dynostore::{DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const TOPIC: &str = "committed";

/// Serves everything normally except a write under `.../offsets/`, which fails
/// the way S3 answers a throttle burst. Only the offset PUT fails, so the topic
/// metadata read ahead of it still succeeds and the commit reaches the write —
/// which is the path under test.
struct ThrottledOffsetWrites {
    inner: InMemory,
}

impl ThrottledOffsetWrites {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
        }
    }

    fn throttled() -> object_store::Error {
        object_store::Error::Generic {
            store: "ThrottledOffsetWrites",
            source: "503 SlowDown".into(),
        }
    }
}

impl Debug for ThrottledOffsetWrites {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThrottledOffsetWrites").finish()
    }
}

impl Display for ThrottledOffsetWrites {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThrottledOffsetWrites").finish()
    }
}

#[async_trait]
impl ObjectStore for ThrottledOffsetWrites {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if location.as_ref().contains("/offsets/") {
            return Err(Self::throttled());
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
        options: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn create_topic(storage: &DynoStore) -> Result<()> {
    _ = storage
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

    Ok(())
}

/// A transient object-store failure on the offset PUT must be reported with a
/// **retriable** code.
///
/// It used to collapse every failure mode — a 503 SlowDown, a timeout, a
/// transient 5xx — into `UnknownServerError`, which is non-retriable in Kafka
/// clients: `commitSync` throws instead of retrying, and a connector that
/// treats a commit failure as engine death restarts. A brief storage throttle
/// therefore surfaced as connector restarts rather than as a retry a moment
/// later (#275).
#[tokio::test]
async fn a_throttled_offset_commit_is_retriable() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, ThrottledOffsetWrites::new());
    create_topic(&storage).await?;

    let committed = storage
        .offset_commit(
            "group-a",
            None,
            &[(
                Topition::new(TOPIC, 0),
                OffsetCommitRequest::default().offset(5),
            )],
        )
        .await?;

    assert_eq!(1, committed.len());
    assert_eq!(Topition::new(TOPIC, 0), committed[0].0);
    assert_ne!(
        ErrorCode::UnknownServerError,
        committed[0].1,
        "a transient storage error must not be reported as fatal"
    );
    assert_eq!(
        ErrorCode::KafkaStorageError,
        committed[0].1,
        "the produce path's answer for the same condition"
    );

    Ok(())
}

/// The mapping is on the failure path only: a commit that lands still reports
/// success, and the topic-absent case keeps its own code.
#[tokio::test]
async fn a_healthy_offset_commit_is_unaffected() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());
    create_topic(&storage).await?;

    let committed = storage
        .offset_commit(
            "group-a",
            None,
            &[
                (
                    Topition::new(TOPIC, 0),
                    OffsetCommitRequest::default().offset(5),
                ),
                (
                    Topition::new("absent", 0),
                    OffsetCommitRequest::default().offset(5),
                ),
            ],
        )
        .await?;

    assert_eq!(
        vec![
            (Topition::new(TOPIC, 0), ErrorCode::None),
            (Topition::new("absent", 0), ErrorCode::UnknownTopicOrPartition),
        ],
        committed
    );

    Ok(())
}
