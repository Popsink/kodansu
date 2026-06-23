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
