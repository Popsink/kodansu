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

use crate::common::{Error, init_tracing};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use std::sync::Arc;
use tansu_sans_io::{
    ErrorCode, MetadataRequest, MetadataResponse, metadata_request::MetadataRequestTopic,
};
use tansu_storage::{MetadataService, Storage, StorageContainer};
use url::Url;

mod common;

async fn storage(url: &str) -> Result<Arc<Box<dyn Storage>>, Error> {
    StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse(url)?)
        .build()
        .await
        .map_err(Into::into)
}

async fn metadata(
    storage: Arc<Box<dyn Storage>>,
    name: &str,
    allow_auto_topic_creation: bool,
) -> Result<MetadataResponse, Error> {
    MapStateLayer::new(|_| storage)
        .into_layer(MetadataService)
        .serve(
            Context::default(),
            MetadataRequest::default()
                .allow_auto_topic_creation(Some(allow_auto_topic_creation))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .topics(Some(
                    [MetadataRequestTopic::default().name(Some(name.into()))].into(),
                )),
        )
        .await
        .map_err(Into::into)
}

async fn metadata_many(
    storage: Arc<Box<dyn Storage>>,
    names: &[String],
    allow_auto_topic_creation: bool,
) -> Result<MetadataResponse, Error> {
    MapStateLayer::new(|_| storage)
        .into_layer(MetadataService)
        .serve(
            Context::default(),
            MetadataRequest::default()
                .allow_auto_topic_creation(Some(allow_auto_topic_creation))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .topics(Some(
                    names
                        .iter()
                        .map(|name| MetadataRequestTopic::default().name(Some(name.into())))
                        .collect(),
                )),
        )
        .await
        .map_err(Into::into)
}

/// A multi-topic metadata request returns exactly one entry per requested
/// topic, in REQUEST ORDER, each carrying that topic's own status — the
/// invariant the concurrent (`buffered`) per-topic fetch must preserve. The
/// request interleaves known and unknown topics and spans several fetch
/// concurrency windows, so an out-of-order or index-shifted result (the
/// failure mode a naive `buffer_unordered` or a mis-zipped parallel rewrite
/// would introduce) is caught.
#[tokio::test]
async fn multi_topic_metadata_preserves_order_and_per_topic_status() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage("memory://tansu/").await?;

    // Auto-create 100 known topics.
    for i in 0..100 {
        let _ = metadata(storage.clone(), &format!("known-{i:03}"), true).await?;
    }

    // known-000, missing-000, known-001, missing-001, ... (200 topics).
    let requested = (0..100)
        .flat_map(|i| [format!("known-{i:03}"), format!("missing-{i:03}")])
        .collect::<Vec<_>>();

    let response = metadata_many(storage.clone(), &requested, false).await?;
    let topics = response.topics.as_deref().unwrap_or_default();

    assert_eq!(requested.len(), topics.len());
    for (index, name) in requested.iter().enumerate() {
        assert_eq!(
            Some(name.as_str()),
            topics[index].name.as_deref(),
            "response out of request order at {index}"
        );
        let expected = if name.starts_with("known-") {
            ErrorCode::None
        } else {
            ErrorCode::UnknownTopicOrPartition
        };
        assert_eq!(
            i16::from(expected),
            topics[index].error_code,
            "wrong status for {name}"
        );
    }

    Ok(())
}

/// Default policy is enabled: a request that opts in auto-creates an unknown
/// topic with a single partition, and the response reflects it as existing.
#[tokio::test]
async fn auto_creates_unknown_topic_by_default() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let response = metadata(storage("memory://tansu/").await?, "abc", true).await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(Some("abc"), topics[0].name.as_deref());
    assert_eq!(i16::from(ErrorCode::None), topics[0].error_code);
    assert_eq!(1, topics[0].partitions.as_deref().unwrap_or_default().len());

    Ok(())
}

/// `auto_create_topics=false` on the storage URL disables it: an unknown topic
/// stays unknown.
#[tokio::test]
async fn disabled_leaves_topic_unknown() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let response = metadata(
        storage("memory://tansu/?auto_create_topics=false").await?,
        "abc",
        true,
    )
    .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(
        i16::from(ErrorCode::UnknownTopicOrPartition),
        topics[0].error_code
    );

    Ok(())
}

/// A request that does not opt in is never auto-created, even with the policy
/// enabled.
#[tokio::test]
async fn request_opt_out_leaves_topic_unknown() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let response = metadata(storage("memory://tansu/").await?, "abc", false).await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(
        i16::from(ErrorCode::UnknownTopicOrPartition),
        topics[0].error_code
    );

    Ok(())
}

/// The configured `num_partitions` is honoured for auto-created topics.
#[tokio::test]
async fn honours_configured_partition_count() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let response = metadata(
        storage("memory://tansu/?num_partitions=3").await?,
        "abc",
        true,
    )
    .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(i16::from(ErrorCode::None), topics[0].error_code);
    assert_eq!(3, topics[0].partitions.as_deref().unwrap_or_default().len());

    Ok(())
}

/// A second request for the same topic is a no-op (the topic already exists);
/// auto-create is idempotent.
#[tokio::test]
async fn idempotent_on_repeat() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage("memory://tansu/").await?;

    let first = metadata(storage.clone(), "abc", true).await?;
    assert_eq!(
        i16::from(ErrorCode::None),
        first.topics.as_deref().unwrap_or_default()[0].error_code
    );

    let second = metadata(storage, "abc", true).await?;
    assert_eq!(
        i16::from(ErrorCode::None),
        second.topics.as_deref().unwrap_or_default()[0].error_code
    );

    Ok(())
}
