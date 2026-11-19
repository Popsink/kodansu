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

//! What `Storage::metadata` answers for no list, an empty list, and topics
//! asked for by name and by id, known and unknown: always the one broker,
//! and the topics as asked.

use rand::RngExt as _;
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::{ErrorCode, NULL_TOPIC_ID};
use tansu_storage::{MetadataResponse, TopicId};

pub mod common;

type Engine = std::sync::Arc<dyn tansu_storage::Storage>;

const PARTITIONS: i32 = 6;
const REPLICAS: i16 = 3;

/// A `memory://` engine advertising `tcp://example.com:9092/`, with that
/// broker registered at a random node id.
struct Cluster {
    engine: Engine,
    node: i32,
    listener: url::Url,
}

impl Cluster {
    async fn registered() -> tansu_broker::Result<Self> {
        let cluster = uuid::Uuid::now_v7();
        let node = rand::rng().random_range(0..i32::MAX);
        let listener = url::Url::parse("tcp://example.com:9092/")?;
        let engine = common::storage_container(cluster, node, listener.clone()).await?;
        tracing::debug!(broker_id = node, advertised_listener = %listener);
        common::register_broker(cluster, node, engine.clone()).await?;
        Ok(Self {
            engine,
            node,
            listener,
        })
    }

    /// A randomly named topic of six partitions held three times, with its id.
    async fn created(&self) -> tansu_broker::Result<(String, uuid::Uuid)> {
        let name = common::alphanumeric_string(15);
        tracing::debug!(topic_name = ?name);
        let topic = CreatableTopic::default()
            .name(name.clone())
            .num_partitions(PARTITIONS);
        let topic = topic.replication_factor(REPLICAS).assignments(Some(vec![]));
        let creating = self.engine.create_topic(topic.configs(Some(vec![])), false);
        let id = creating.await?;
        tracing::debug!(topic_id = ?id);
        Ok((name, id))
    }

    /// Metadata for `topics`, asserting that it lists this broker alone.
    async fn metadata(&self, topics: Option<&[TopicId]>) -> tansu_broker::Result<MetadataResponse> {
        let answer = self.engine.metadata(topics).await?;
        tracing::debug!(metadata = ?answer);

        let brokers = answer.brokers();
        assert_eq!(1, brokers.len());
        let host = self.listener.host_str().unwrap_or("0.0.0.0");
        let port = i32::from(self.listener.port().unwrap_or(9092));
        let broker = &brokers[0];
        assert_eq!(
            (self.node, host, port),
            (broker.node_id, broker.host.as_str(), broker.port)
        );
        Ok(answer)
    }

    /// Asserts that `metadata` holds one topic, `name` at `id`, error-free, with six partitions
    /// each led by this broker at epoch 0 and held by it three times over.
    fn assert_found(&self, metadata: &MetadataResponse, name: &str, id: uuid::Uuid) {
        let [topic] = metadata.topics() else {
            panic!("one topic in {metadata:?}")
        };
        assert_eq!(i16::from(ErrorCode::None), topic.error_code);
        assert_eq!(Some(name), topic.name.as_deref());
        assert_eq!(Some(id.as_bytes()), topic.topic_id.as_ref());

        let partitions = topic.partitions.as_deref().unwrap_or_default();
        assert_eq!(PARTITIONS as usize, partitions.len());
        let everywhere = vec![self.node; REPLICAS as usize];
        for partition in partitions {
            tracing::debug!(?partition);
            assert_eq!(
                (self.node, Some(0)),
                (partition.leader_id, partition.leader_epoch)
            );
            assert!(
                partition
                    .offline_replicas
                    .as_deref()
                    .unwrap_or_default()
                    .is_empty()
            );
            assert_eq!(
                &everywhere[..],
                partition.replica_nodes.as_deref().unwrap_or_default()
            );
            assert_eq!(
                &everywhere[..],
                partition.isr_nodes.as_deref().unwrap_or_default()
            );
        }
    }
}

/// Asserts that `metadata` holds one topic, unknown, as the request named it.
fn assert_unknown(metadata: &MetadataResponse, name: Option<&str>, id: &[u8; 16]) {
    let [topic] = metadata.topics() else {
        panic!("one topic in {metadata:?}")
    };
    let unknown = i16::from(ErrorCode::UnknownTopicOrPartition);
    assert_eq!((unknown, name), (topic.error_code, topic.name.as_deref()));
    assert_eq!(Some(id), topic.topic_id.as_ref());
}

#[tokio::test]
async fn no_list_answers_every_topic() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let cluster = Cluster::registered().await?;
    let (name, id) = cluster.created().await?;

    let metadata = cluster.metadata(None).await?;
    cluster.assert_found(&metadata, &name, id);
    Ok(())
}

/// An empty list asks for the brokers and no topic at all (#593);
/// `no_list_answers_every_topic` is the request for every topic.
#[tokio::test]
async fn an_empty_list_answers_no_topic() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let cluster = Cluster::registered().await?;
    let _created = cluster.created().await?;

    let metadata = cluster.metadata(Some(&[])).await?;
    assert!(metadata.topics().is_empty());
    Ok(())
}

#[tokio::test]
async fn a_known_name_answers_its_topic() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let cluster = Cluster::registered().await?;
    let (name, id) = cluster.created().await?;

    let metadata = cluster
        .metadata(Some(&[TopicId::Name(name.clone())]))
        .await?;
    cluster.assert_found(&metadata, &name, id);
    Ok(())
}

#[tokio::test]
async fn an_unknown_name_answers_unknown_with_a_null_id() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let cluster = Cluster::registered().await?;
    let name = common::alphanumeric_string(15);
    tracing::debug!(topic_name = ?name);

    let metadata = cluster
        .metadata(Some(&[TopicId::Name(name.clone())]))
        .await?;
    assert_unknown(&metadata, Some(&name), &NULL_TOPIC_ID);
    Ok(())
}

#[tokio::test]
async fn a_known_id_answers_its_topic() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let cluster = Cluster::registered().await?;
    let (name, id) = cluster.created().await?;

    let metadata = cluster.metadata(Some(&[TopicId::Id(id)])).await?;
    cluster.assert_found(&metadata, &name, id);
    Ok(())
}

#[tokio::test]
async fn an_unknown_id_answers_unknown_without_a_name() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let cluster = Cluster::registered().await?;
    let id = uuid::Uuid::new_v4();

    let metadata = cluster.metadata(Some(&[TopicId::Id(id)])).await?;
    assert_unknown(&metadata, None, id.as_bytes());
    Ok(())
}
