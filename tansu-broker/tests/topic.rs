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

//! Creating, describing and deleting topics straight on the storage engine,
//! with and without a config, by id and by name.

use rand::RngExt as _;
use tansu_sans_io::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use tansu_sans_io::describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic;
use tansu_sans_io::{ErrorCode, NULL_TOPIC_ID};
use tansu_storage::TopicId;

pub mod common;

type Engine = std::sync::Arc<dyn tansu_storage::Storage>;

const PARTITIONS: i32 = 6;

/// The page size every describe here asks for, larger than any answer.
const PARTITION_LIMIT: i32 = 32123;

/// A `memory://` engine with one broker registered, and that broker's id.
async fn registered() -> tansu_broker::Result<(Engine, i32)> {
    let cluster = uuid::Uuid::now_v7();
    let node = rand::rng().random_range(0..i32::MAX);
    let listener = url::Url::parse("tcp://127.0.0.1/")?;
    let engine = common::storage_container(cluster, node, listener).await?;
    common::register_broker(cluster, node, engine.clone()).await?;
    Ok((engine, node))
}

/// A topic with a random name, its six partitions held `replicas` times.
fn topic(replicas: i16, configs: Vec<CreatableTopicConfig>) -> CreatableTopic {
    let name = common::alphanumeric_string(15);
    tracing::debug!(topic_name = ?name);
    let topic = CreatableTopic::default()
        .name(name)
        .num_partitions(PARTITIONS);
    let topic = topic.replication_factor(replicas).assignments(Some(vec![]));
    topic.configs(Some(configs))
}

async fn create(engine: &Engine, topic: CreatableTopic) -> tansu_broker::Result<uuid::Uuid> {
    let creating = engine.create_topic(topic, false);
    let id = creating.await?;
    tracing::debug!(topic_id = ?id);
    Ok(id)
}

/// Asserts that `described` is `name`, uninternal and error-free, with six
/// partitions led by `node` at epoch 0 and replicated on it `replicas` times.
fn assert_described(
    described: &DescribeTopicPartitionsResponseTopic,
    name: &str,
    (node, replicas): (i32, i16),
) -> tansu_broker::Result<()> {
    assert_eq!(ErrorCode::None, ErrorCode::try_from(described.error_code)?);
    assert_eq!(
        (Some(name), false),
        (described.name.as_deref(), described.is_internal)
    );
    let partitions = described.partitions.as_deref().unwrap_or_default();
    assert_eq!(PARTITIONS as usize, partitions.len());

    let everywhere = Some(vec![node; replicas as usize]);
    for partition in partitions {
        let outcome = ErrorCode::try_from(partition.error_code)?;
        assert_eq!(ErrorCode::None, outcome);
        assert_eq!((node, 0), (partition.leader_id, partition.leader_epoch));
        assert_eq!(everywhere, partition.replica_nodes);
        assert_eq!(everywhere, partition.isr_nodes);
        let none = Some(vec![]);
        assert_eq!(none, partition.eligible_leader_replicas);
        assert_eq!(none, partition.last_known_elr);
        assert_eq!(none, partition.offline_replicas);
    }
    Ok(())
}

#[tokio::test]
async fn a_created_topic_deletes() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let (engine, _) = registered().await?;
    let id = create(&engine, topic(0, vec![])).await?;
    let created = TopicId::from(id);
    let deleting = engine.delete_topic(&created);
    assert_eq!(ErrorCode::None, deleting.await?);
    Ok(())
}

#[tokio::test]
async fn a_created_topic_describes_by_id() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let (engine, node) = registered().await?;
    let created = topic(3, vec![]);
    let name = created.name.clone();
    let id = create(&engine, created).await?;

    let wanted = [TopicId::from(id)];
    let describing = engine.describe_topic_partitions(Some(&wanted), PARTITION_LIMIT, None);
    let described = describing.await?;
    tracing::debug!(responses = ?described);

    assert_eq!(1, described.len());
    assert_described(&described[0], &name, (node, 3))?;
    assert_eq!(id.as_bytes(), &described[0].topic_id);
    Ok(())
}

#[tokio::test]
async fn a_created_topic_describes_by_name() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let (engine, node) = registered().await?;
    let created = topic(3, vec![]);
    let name = created.name.clone();
    let _id = create(&engine, created).await?;

    let wanted = [TopicId::from(name.clone())];
    let describing = engine.describe_topic_partitions(Some(&wanted), PARTITION_LIMIT, None);
    let described = describing.await?;

    assert_eq!(1, described.len());
    assert_described(&described[0], &name, (node, 3))
}

#[tokio::test]
async fn an_unknown_topic_describes_as_unknown() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let (engine, _) = registered().await?;
    let name = common::alphanumeric_string(15);
    tracing::debug!(topic_name = ?name);

    let wanted = [TopicId::from(name.clone())];
    let describing = engine.describe_topic_partitions(Some(&wanted), PARTITION_LIMIT, None);
    let described = describing.await?;

    assert_eq!(1, described.len());
    let unknown = &described[0];
    let outcome = ErrorCode::try_from(unknown.error_code)?;
    assert_eq!(ErrorCode::UnknownTopicOrPartition, outcome);
    assert_eq!(
        (Some(name.as_str()), false),
        (unknown.name.as_deref(), unknown.is_internal)
    );
    assert_eq!(&NULL_TOPIC_ID, &unknown.topic_id);
    assert_eq!(0, unknown.partitions.as_ref().map_or(0, Vec::len));
    Ok(())
}

#[tokio::test]
async fn a_topic_created_with_a_config_deletes() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let (engine, _) = registered().await?;
    let config = CreatableTopicConfig::default().name("xyz".into());
    let config = config.value(Some("12321".into()));
    let id = create(&engine, topic(0, vec![config])).await?;
    let created = TopicId::from(id);
    let deleting = engine.delete_topic(&created);
    assert_eq!(ErrorCode::None, deleting.await?);
    Ok(())
}
