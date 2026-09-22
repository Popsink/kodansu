// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use crate::common::{Error, cluster_id, init_tracing, storage_url};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use std::sync::Arc;
use tansu_sans_io::{
    ConfigResource, DescribeConfigsRequest, DescribeConfigsResponse, ErrorCode,
    create_topics_request::CreatableTopic, describe_configs_request::DescribeConfigsResource,
};
use tansu_storage::{DescribeConfigsService, Storage, StorageContainer, TopicId};
use url::Url;

mod common;

async fn storage() -> Result<Arc<dyn Storage>, Error> {
    StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .build()
        .await
        .map_err(Into::into)
}

async fn describe(storage: Arc<dyn Storage>, name: &str) -> Result<DescribeConfigsResponse, Error> {
    MapStateLayer::new(|_| storage)
        .into_layer(DescribeConfigsService)
        .serve(
            Context::default(),
            DescribeConfigsRequest::default()
                .include_documentation(Some(false))
                .include_synonyms(Some(false))
                .resources(Some(
                    [DescribeConfigsResource::default()
                        .resource_name(name.into())
                        .resource_type(ConfigResource::Topic.into())
                        .configuration_keys(Some([].into()))]
                    .into(),
                )),
        )
        .await
        .map_err(Into::into)
}

/// A topic that exists is described, and the broker-level defaults it was
/// created with are what comes back.
#[tokio::test]
async fn an_existing_topic_is_described() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let name = "abcba";

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let response = describe(storage, name).await?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);
    assert!(!results[0].configs.as_deref().unwrap_or_default().is_empty());

    Ok(())
}

/// A topic that was never created is reported as unknown (#579).
///
/// It used to answer `NONE` with an empty config list, which makes
/// `DescribeConfigs` useless as an existence check — it says yes to every name
/// ever spelled — and, worse, says yes in exactly the way that reads as "the
/// topic is there and has no settings".
#[tokio::test]
async fn a_topic_that_does_not_exist_is_unknown() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let response = describe(storage().await?, "never-created").await?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(results[0].error_code)?
    );
    assert_eq!("never-created", results[0].resource_name.as_str());
    assert!(results[0].configs.as_deref().unwrap_or_default().is_empty());

    Ok(())
}

/// A deleted topic goes back to unknown: the answer tracks existence, not
/// whether the name was ever used.
#[tokio::test]
async fn a_deleted_topic_is_unknown_again() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let name = "transient";

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(
            describe(storage.clone(), name)
                .await?
                .results
                .unwrap_or_default()[0]
                .error_code
        )?
    );

    assert_eq!(
        ErrorCode::None,
        storage.delete_topic(&TopicId::Name(name.into())).await?
    );

    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(
            describe(storage, name).await?.results.unwrap_or_default()[0].error_code
        )?
    );

    Ok(())
}
