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

// Every case here builds a `memory://` container, which needs the one storage
// engine this fork ships. The gate was on `mod in_memory` until #552 dissolved
// it; with the module gone it belongs to the file.
#![cfg(feature = "dynostore")]

//! A topic's configs through `DescribeConfigs`, as created and as
//! `IncrementalAlterConfigs` sets and deletes its `cleanup.policy`.

use rama::Context;
use rama::Service as _;
use rand::RngExt as _;
use tansu_sans_io::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use tansu_sans_io::describe_configs_request::DescribeConfigsResource;
use tansu_sans_io::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResult,
};
use tansu_sans_io::incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig};
use tansu_sans_io::{
    ConfigResource, ConfigSource, DescribeConfigsRequest, DescribeConfigsResponse,
};
use tansu_sans_io::{ErrorCode, IncrementalAlterConfigsRequest, OpType};
use tansu_storage::{DEFAULT_CLEANUP_POLICY, DEFAULT_RETENTION_MS};
use tansu_storage::{DescribeConfigsService, IncrementalAlterConfigsService};

pub mod common;

type Engine = std::sync::Arc<dyn tansu_storage::Storage>;

const CLEANUP_POLICY: &str = "cleanup.policy";
const RETENTION_MS: &str = "retention.ms";

/// `ConfigResource::Topic` as the wire carries it.
fn topic_kind() -> i8 {
    ConfigResource::Topic.into()
}

/// The `DescribeConfigs` response for `topic_name` carrying exactly `rows`, in
/// order. Since #225 the engine injects the broker-level defaults at
/// `create_topic` — the single creation choke point — so a topic created with no
/// config still stores Kafka's `cleanup.policy=delete` and `retention.ms`, and
/// every expectation below carries the `retention.ms` an `AlterConfigs` on the
/// policy leaves untouched.
fn expected_configs(topic_name: &str, rows: &[(&str, &str)]) -> DescribeConfigsResponse {
    let row = |&(name, value): &(&str, &str)| {
        let row = DescribeConfigsResourceResult::default().name(name.into());
        let row = row
            .value(Some(value.into()))
            .read_only(false)
            .is_default(None);
        let row = row.config_source(Some(ConfigSource::DefaultConfig.into()));
        let row = row.is_sensitive(false).synonyms(Some(vec![]));
        row.config_type(Some(ConfigResource::Topic.into()))
            .documentation(Some(String::new()))
    };
    let ok = ErrorCode::None;
    let result = DescribeConfigsResult::default().error_code(ok.into());
    let result = result.error_message(Some(ok.to_string()));
    let result = result.resource_type(topic_kind());
    let result = result.resource_name(topic_name.into());
    let result = result.configs(Some(rows.iter().map(row).collect()));
    DescribeConfigsResponse::default().results(Some(vec![result]))
}

/// A topic of six unreplicated partitions with a random name, on a fresh
/// `memory://` engine with one broker registered.
struct Topic {
    ctx: Context<Engine>,
    name: String,
}

impl Topic {
    async fn created(configs: Vec<CreatableTopicConfig>) -> tansu_broker::Result<Self> {
        let cluster = uuid::Uuid::now_v7();
        let node = rand::rng().random_range(0..i32::MAX);
        let listener = url::Url::parse("tcp://127.0.0.1/")?;
        let engine = common::storage_container(cluster, node, listener).await?;
        common::register_broker(cluster, node, engine.clone()).await?;

        let name = common::alphanumeric_string(15);
        tracing::debug!(topic_name = ?name);
        let topic = CreatableTopic::default()
            .name(name.clone())
            .num_partitions(6);
        let topic = topic.replication_factor(0).assignments(Some(vec![]));
        let creating = engine.create_topic(topic.configs(Some(configs)), false);
        let id = creating.await?;
        tracing::debug!(topic_id = ?id);
        Ok(Self {
            ctx: Context::with_state(engine),
            name,
        })
    }

    /// Every config of this topic, without synonyms or documentation.
    async fn described(&self) -> tansu_broker::Result<DescribeConfigsResponse> {
        let topic = DescribeConfigsResource::default().resource_name(self.name.clone());
        let topic = topic.resource_type(topic_kind()).configuration_keys(None);
        let request = DescribeConfigsRequest::default().resources(Some(vec![topic]));
        let request = request.include_documentation(Some(false));
        let request = request.include_synonyms(Some(false));
        let describing = DescribeConfigsService.serve(self.ctx.clone(), request);
        describing.await.map_err(Into::into)
    }

    /// Applies `operation` (with `value`) to this topic's `cleanup.policy`,
    /// and asserts that the one resource it names is answered without error.
    async fn alter_policy(
        &self,
        operation: OpType,
        value: Option<&str>,
    ) -> tansu_broker::Result<()> {
        let change = AlterableConfig::default().name(CLEANUP_POLICY.into());
        let change = change.config_operation(operation.into());
        let change = change.value(value.map(String::from));
        let topic = AlterConfigsResource::default().resource_name(self.name.clone());
        let topic = topic
            .resource_type(topic_kind())
            .configs(Some(vec![change]));
        let request = IncrementalAlterConfigsRequest::default().resources(Some(vec![topic]));
        let altering = IncrementalAlterConfigsService.serve(self.ctx.clone(), request);
        let answered = altering.await?.responses.unwrap_or_default();

        assert_eq!(1, answered.len());
        let error_code = i16::from(ErrorCode::None);
        let named = (answered[0].error_code, answered[0].resource_type);
        assert_eq!((error_code, topic_kind()), named);
        assert_eq!(self.name, answered[0].resource_name);
        Ok(())
    }
}

#[tokio::test]
async fn cleanup_policy_set_set_and_deleted() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let topic = Topic::created(vec![]).await?;
    let retention = DEFAULT_RETENTION_MS.to_string();
    let retained = (RETENTION_MS, retention.as_str());

    // A topic created with no config stores the injected defaults (#225), rather
    // than nothing at all as it did when the injection lived in the CreateTopics
    // service and this direct `create_topic` call bypassed it.
    let created = [(CLEANUP_POLICY, DEFAULT_CLEANUP_POLICY), retained];
    assert_eq!(
        expected_configs(&topic.name, &created),
        topic.described().await?
    );

    for policy in ["compact", "delete"] {
        topic.alter_policy(OpType::Set, Some(policy)).await?;
        let set = [(CLEANUP_POLICY, policy), retained];
        assert_eq!(
            expected_configs(&topic.name, &set),
            topic.described().await?
        );
    }

    // Deleting the policy leaves the injected retention behind: `AlterConfigs`
    // touches only the key it names.
    topic.alter_policy(OpType::Delete, None).await?;
    assert_eq!(
        expected_configs(&topic.name, &[retained]),
        topic.described().await?
    );
    Ok(())
}

#[tokio::test]
async fn a_topic_created_compact_describes_only_its_policy() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let compact = CreatableTopicConfig::default().name(CLEANUP_POLICY.into());
    let topic = Topic::created(vec![compact.value(Some("compact".into()))]).await?;
    let configured = [(CLEANUP_POLICY, "compact")];
    assert_eq!(
        expected_configs(&topic.name, &configured),
        topic.described().await?
    );
    Ok(())
}
