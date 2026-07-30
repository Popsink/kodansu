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

use common::{alphanumeric_string, register_broker};
use rama::{Context, Service};
use rand::{prelude::*, rng};
use tansu_broker::Result;
use tansu_sans_io::{
    ConfigResource, ConfigSource, DescribeConfigsRequest, DescribeConfigsResponse, ErrorCode,
    IncrementalAlterConfigsRequest, OpType,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    describe_configs_request::DescribeConfigsResource,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
};
use tansu_storage::{
    DEFAULT_CLEANUP_POLICY, DEFAULT_RETENTION_MS, DescribeConfigsService,
    IncrementalAlterConfigsService, Storage,
};
use tracing::debug;
use uuid::Uuid;
pub mod common;

pub async fn single_topic<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let cleanup_policy = "cleanup.policy";
    let compact = "compact";

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some(
        [CreatableTopicConfig::default()
            .name(cleanup_policy.into())
            .value(Some(compact.into()))]
        .into(),
    );

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;

    let resources = [DescribeConfigsResource::default()
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic_name.clone())
        .configuration_keys(None)];

    let include_synonyms = Some(false);
    let include_documentation = Some(false);

    let ctx = Context::with_state(sc);

    let results = DescribeConfigsService
        .serve(
            ctx,
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(include_synonyms)
                .resources(Some(resources.into())),
        )
        .await?;

    assert_eq!(
        results,
        DescribeConfigsResponse::default().results(Some(vec![
            DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some(ErrorCode::None.to_string()))
                .resource_type(ConfigResource::Topic.into())
                .resource_name(topic_name)
                .configs(Some(
                    [DescribeConfigsResourceResult::default()
                        .name(cleanup_policy.into())
                        .value(Some(compact.into()))
                        .read_only(false)
                        .is_default(None)
                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                        .is_sensitive(false)
                        .synonyms(Some([].into()))
                        .config_type(Some(ConfigResource::Topic.into()))
                        .documentation(Some("".into()))]
                    .into()
                ))
        ],))
    );

    debug!(?topic_id);
    Ok(())
}

/// The `DescribeConfigs` response for `topic_name` carrying exactly `rows`, in
/// order. Since #225 the engine injects the broker-level defaults at
/// `create_topic` — the single creation choke point — so a topic created with no
/// config still stores Kafka's `cleanup.policy=delete` and `retention.ms`, and
/// every expectation below carries the `retention.ms` an `AlterConfigs` on the
/// policy leaves untouched.
fn expected_configs(topic_name: &str, rows: &[(&str, &str)]) -> DescribeConfigsResponse {
    let none = ErrorCode::None;

    DescribeConfigsResponse::default().results(Some(vec![
        DescribeConfigsResult::default()
            .error_code(none.into())
            .error_message(Some(none.to_string()))
            .resource_type(ConfigResource::Topic.into())
            .resource_name(topic_name.into())
            .configs(Some(
                rows.iter()
                    .map(|(name, value)| {
                        DescribeConfigsResourceResult::default()
                            .name((*name).into())
                            .value(Some((*value).into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(ConfigSource::DefaultConfig.into()))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(ConfigResource::Topic.into()))
                            .documentation(Some("".into()))
                    })
                    .collect(),
            )),
    ]))
}

pub async fn alter_single_topic<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let cleanup_policy = "cleanup.policy";
    let compact = "compact";
    let delete = "delete";

    let num_partitions = 6;
    let replication_factor = 0;

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let resources = [DescribeConfigsResource::default()
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic_name.clone())
        .configuration_keys(None)];

    let include_synonyms = Some(false);
    let include_documentation = Some(false);

    let ctx = Context::with_state(sc);

    let results = DescribeConfigsService
        .serve(
            ctx.clone(),
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(include_synonyms)
                .resources(Some(resources.clone().into())),
        )
        .await?;

    let none = ErrorCode::None;

    // A topic created with no config stores the injected defaults (#225), rather
    // than nothing at all as it did when the injection lived in the CreateTopics
    // service and this direct `create_topic` call bypassed it.
    let retention_ms = DEFAULT_RETENTION_MS.to_string();
    assert_eq!(
        results,
        expected_configs(
            &topic_name,
            &[
                (cleanup_policy, DEFAULT_CLEANUP_POLICY),
                ("retention.ms", retention_ms.as_str()),
            ],
        )
    );

    let response = IncrementalAlterConfigsService
        .serve(
            ctx.clone(),
            IncrementalAlterConfigsRequest::default().resources(Some(
                [AlterConfigsResource::default()
                    .resource_type(ConfigResource::Topic.into())
                    .resource_name(topic_name.clone())
                    .configs(Some(vec![
                        AlterableConfig::default()
                            .name(cleanup_policy.into())
                            .config_operation(OpType::Set.into())
                            .value(Some(compact.into())),
                    ]))]
                .into(),
            )),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());
    assert_eq!(i16::from(none), responses[0].error_code);
    assert_eq!(i8::from(ConfigResource::Topic), responses[0].resource_type);
    assert_eq!(topic_name, responses[0].resource_name);

    let results = DescribeConfigsService
        .serve(
            ctx.clone(),
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(include_synonyms)
                .resources(Some(resources.clone().into())),
        )
        .await?;

    assert_eq!(
        results,
        expected_configs(
            &topic_name,
            &[
                (cleanup_policy, compact),
                ("retention.ms", retention_ms.as_str())
            ],
        )
    );

    let response = IncrementalAlterConfigsService
        .serve(
            ctx.clone(),
            IncrementalAlterConfigsRequest::default().resources(Some(
                [AlterConfigsResource::default()
                    .resource_type(ConfigResource::Topic.into())
                    .resource_name(topic_name.clone())
                    .configs(Some(vec![
                        AlterableConfig::default()
                            .name(cleanup_policy.into())
                            .config_operation(OpType::Set.into())
                            .value(Some(delete.into())),
                    ]))]
                .into(),
            )),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());
    assert_eq!(i16::from(none), responses[0].error_code);
    assert_eq!(i8::from(ConfigResource::Topic), responses[0].resource_type);
    assert_eq!(topic_name, responses[0].resource_name);

    let results = DescribeConfigsService
        .serve(
            ctx.clone(),
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(include_synonyms)
                .resources(Some(resources.clone().into())),
        )
        .await?;

    assert_eq!(
        results,
        expected_configs(
            &topic_name,
            &[
                (cleanup_policy, delete),
                ("retention.ms", retention_ms.as_str())
            ],
        )
    );

    let response = IncrementalAlterConfigsService
        .serve(
            ctx.clone(),
            IncrementalAlterConfigsRequest::default().resources(Some(
                [AlterConfigsResource::default()
                    .resource_type(ConfigResource::Topic.into())
                    .resource_name(topic_name.clone())
                    .configs(Some(vec![
                        AlterableConfig::default()
                            .name(cleanup_policy.into())
                            .config_operation(OpType::Delete.into())
                            .value(None),
                    ]))]
                .into(),
            )),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());
    assert_eq!(i16::from(none), responses[0].error_code);
    assert_eq!(i8::from(ConfigResource::Topic), responses[0].resource_type);
    assert_eq!(topic_name, responses[0].resource_name);

    let results = DescribeConfigsService
        .serve(
            ctx,
            DescribeConfigsRequest::default()
                .include_documentation(include_documentation)
                .include_synonyms(include_synonyms)
                .resources(Some(resources.into())),
        )
        .await?;

    // Deleting the policy leaves the injected retention behind: `AlterConfigs`
    // touches only the key it names.
    assert_eq!(
        results,
        expected_configs(&topic_name, &[("retention.ms", retention_ms.as_str())])
    );

    debug!(?topic_id);
    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use std::sync::Arc;

    use common::{StorageType, init_tracing};
    use url::Url;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String>,
        node: i32,
    ) -> Result<Arc<Box<dyn Storage>>> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
        )
        .await
    }

    #[tokio::test]
    async fn alter_single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::alter_single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
