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

use std::sync::Arc;

use crate::common::{Error, init_tracing};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use tansu_sans_io::{
    ConfigResource, CreateTopicsRequest, DescribeConfigsRequest,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    describe_configs_request::DescribeConfigsResource,
};
use tansu_storage::{
    CreateTopicsService, DEFAULT_CLEANUP_POLICY, DEFAULT_RETENTION_MS, DescribeConfigsService,
    Storage, StorageContainer, TopicDefaults,
};
use url::Url;

mod common;

type DynStorage = Arc<Box<dyn Storage>>;

/// A store carrying the broker-level `defaults`, which is where the injection now
/// lives: `create_topic` is the single choke point, so every creation path
/// inherits them (#225).
async fn storage(defaults: TopicDefaults) -> Result<DynStorage, Error> {
    StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://tansu/")?)
        .topic_defaults(defaults)
        .build()
        .await
        .map_err(Into::into)
}

/// Create `name` with the given client `configs` on a store built with the broker
/// `defaults`, then read the stored config back through `DescribeConfigs`.
async fn create_then_describe(
    storage: &DynStorage,
    name: &str,
    configs: Option<Vec<CreatableTopicConfig>>,
) -> Result<Vec<(String, Option<String>)>, Error> {
    let create = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
    };

    _ = create
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .validate_only(Some(false))
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(1)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(configs),
                ])),
        )
        .await?;

    let describe = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(DescribeConfigsService)
    };

    let response = describe
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

// A topic created with no config gets the Kafka defaults, visible via
// DescribeConfigs — so `maintain` (which reads the stored config) enforces
// retention by default.
#[tokio::test]
async fn no_config_gets_kafka_defaults() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage(TopicDefaults::default()).await?;

    let configs = create_then_describe(&storage, "no-config", None).await?;

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

// An empty client config list is treated the same as no config.
#[tokio::test]
async fn empty_config_gets_kafka_defaults() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage(TopicDefaults::default()).await?;

    let configs = create_then_describe(&storage, "empty-config", Some(vec![])).await?;

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

// A compact topic keeps its policy and is not given a retention.ms (retention
// only applies to delete-policy topics) — so internal compacted topics are
// left untouched.
#[tokio::test]
async fn compact_policy_is_preserved_without_retention() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage(TopicDefaults::default()).await?;

    let configs = create_then_describe(
        &storage,
        "compact",
        Some(vec![
            CreatableTopicConfig::default()
                .name("cleanup.policy".into())
                .value(Some("compact".into())),
        ]),
    )
    .await?;

    assert_eq!(Some("compact"), value(&configs, "cleanup.policy"));
    assert_eq!(None, value(&configs, "retention.ms"));

    Ok(())
}

// Explicit client values are never overwritten by the defaults.
#[tokio::test]
async fn explicit_config_is_not_overwritten() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage(TopicDefaults::default()).await?;

    let configs = create_then_describe(
        &storage,
        "explicit",
        Some(vec![
            CreatableTopicConfig::default()
                .name("cleanup.policy".into())
                .value(Some("delete".into())),
            CreatableTopicConfig::default()
                .name("retention.ms".into())
                .value(Some("1000".into())),
        ]),
    )
    .await?;

    assert_eq!(Some("delete"), value(&configs, "cleanup.policy"));
    assert_eq!(Some("1000"), value(&configs, "retention.ms"));

    Ok(())
}

// Opting out (no default cleanup.policy) stores nothing: a topic created with no
// config keeps no config.
//
// Note this controls what is *stored*, not what maintenance does. Since #177 an
// absent `cleanup.policy` reads as Kafka's default (`delete` at the default
// retention), so opting out no longer means "never expires" — it means the
// defaults are applied at maintenance time instead of frozen into the topic at
// creation. Retain-forever has one spelling: `retention.ms=-1`.
#[tokio::test]
async fn opt_out_injects_nothing() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let defaults = TopicDefaults {
        cleanup_policy: None,
        retention_ms: DEFAULT_RETENTION_MS,
    };

    let storage = storage(defaults).await?;

    let configs = create_then_describe(&storage, "opt-out", None).await?;

    assert_eq!(None, value(&configs, "cleanup.policy"));
    assert_eq!(None, value(&configs, "retention.ms"));

    Ok(())
}
