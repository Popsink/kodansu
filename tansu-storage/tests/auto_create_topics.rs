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

use crate::common::{Error, cluster_id, init_tracing, storage_url, storage_url_with_query};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use std::sync::Arc;
use tansu_sans_io::{
    ConfigResource, DescribeConfigsRequest, ErrorCode, MetadataRequest, MetadataResponse,
    describe_configs_request::DescribeConfigsResource, metadata_request::MetadataRequestTopic,
};
use tansu_storage::{
    DEFAULT_CLEANUP_POLICY, DEFAULT_RETENTION_MS, DescribeConfigsService, MetadataService, Storage,
    StorageContainer, TopicDefaults,
};
use url::Url;

mod common;

async fn storage(query: &str) -> Result<Arc<Box<dyn Storage>>, Error> {
    StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url_with_query(query)?)
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

    let storage = storage("").await?;

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

    let response = metadata(storage("").await?, "abc", true).await?;

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

    let response = metadata(storage("auto_create_topics=false").await?, "abc", true).await?;

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

    let response = metadata(storage("").await?, "abc", false).await?;

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

    let response = metadata(storage("num_partitions=3").await?, "abc", true).await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(i16::from(ErrorCode::None), topics[0].error_code);
    assert_eq!(3, topics[0].partitions.as_deref().unwrap_or_default().len());

    Ok(())
}

/// Auto-create applies the broker-level [`TopicDefaults`], so a topic materialised
/// by a `Metadata` request stores the same effective config as one created through
/// `CreateTopics` (#225).
///
/// It did not: the injection lived in `CreateTopicsService`, and `MetadataService`
/// builds its own `CreatableTopic` with an explicitly empty config list, so
/// `DEFAULT_CLEANUP_POLICY` / `DEFAULT_RETENTION` were silently dropped for every
/// auto-created topic. The topic then stored no config at all — invisible in
/// `DescribeConfigs`, and expiring on Kafka's fallback rather than the configured
/// default, for the same broker config. Any client relying on
/// `auto.create.topics.enable` got a different retention regime from the rest of
/// the cluster.
///
/// Note what this test does *not* do: it never mentions the defaults to the
/// service. They are configured on the store, and the service is the plain
/// `MetadataService` — the point of moving the injection into `create_topic` is
/// that a creation path cannot opt out of them, or forget them, at all.
#[tokio::test]
async fn auto_create_applies_broker_topic_defaults() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let name = "auto-defaults";

    let defaults = TopicDefaults {
        cleanup_policy: Some("delete".into()),
        retention_ms: 2_592_000_000,
    };

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .topic_defaults(defaults.clone())
        .build()
        .await?;

    let response = metadata(storage.clone(), name, true).await?;

    assert_eq!(
        i16::from(ErrorCode::None),
        response.topics.as_deref().unwrap_or_default()[0].error_code
    );

    let configs = describe(storage.clone(), name).await?;

    assert_eq!(
        Some("delete"),
        value(&configs, "cleanup.policy"),
        "the configured default policy must reach an auto-created topic"
    );
    assert_eq!(
        Some(defaults.retention_ms.to_string().as_str()),
        value(&configs, "retention.ms"),
        "the configured default retention must reach an auto-created topic"
    );

    Ok(())
}

/// The same path with the defaults left at their own defaults: Kafka's `delete` at
/// 7 days, stored and reported, rather than nothing at all.
#[tokio::test]
async fn auto_create_stores_kafka_defaults_when_unconfigured() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage("").await?;
    let name = "auto-kafka-defaults";

    _ = metadata(storage.clone(), name, true).await?;

    let configs = describe(storage.clone(), name).await?;

    assert_eq!(
        Some(DEFAULT_CLEANUP_POLICY),
        value(&configs, "cleanup.policy")
    );
    assert_eq!(
        Some(DEFAULT_RETENTION_MS.to_string().as_str()),
        value(&configs, "retention.ms")
    );

    Ok(())
}

/// The stored config of `name`, as `DescribeConfigs` reports it — which is what an
/// operator sees and what maintenance reads.
async fn describe(
    storage: Arc<Box<dyn Storage>>,
    name: &str,
) -> Result<Vec<(String, Option<String>)>, Error> {
    let response = MapStateLayer::new(|_| storage)
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
        .await?;

    Ok(response
        .results
        .unwrap_or_default()
        .into_iter()
        .flat_map(|result| result.configs.unwrap_or_default())
        .map(|config| (config.name, config.value))
        .collect())
}

fn value<'a>(configs: &'a [(String, Option<String>)], name: &str) -> Option<&'a str> {
    configs
        .iter()
        .find(|(key, _)| key == name)
        .and_then(|(_, value)| value.as_deref())
}

/// A second request for the same topic is a no-op (the topic already exists);
/// auto-create is idempotent.
#[tokio::test]
async fn idempotent_on_repeat() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage("").await?;

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
