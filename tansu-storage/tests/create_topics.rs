// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use tansu_sans_io::{
    CreateTopicsRequest, DescribeTopicPartitionsRequest, ErrorCode, NULL_TOPIC_ID,
    create_topics_request::CreatableTopic, describe_topic_partitions_request::TopicRequest,
};
use tansu_storage::{CreateTopicsService, DescribeTopicPartitionsService, StorageContainer};
use url::Url;

mod common;

#[tokio::test]
async fn create() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let node_id = 12321;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(node_id)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(CreateTopicsService);

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = service
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    Ok(())
}

#[tokio::test]
async fn create_with_default() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let node_id = 12321;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(node_id)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .build()
        .await?;

    let service = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
    };

    let name = "pqr";
    let num_partitions = -1;
    let replication_factor = -1;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = service
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(3), topics[0].num_partitions);
    assert_eq!(Some(1), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let service = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(DescribeTopicPartitionsService)
    };

    let response = service
        .serve(
            Context::default(),
            DescribeTopicPartitionsRequest::default()
                .topics(Some([TopicRequest::default().name(name.into())].into())),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(Some(name), topics[0].name.as_deref());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();

    assert_eq!(3, partitions.len());

    for (index, partition) in partitions.iter().enumerate() {
        assert_eq!(index as i32, partition.partition_index);
        assert_eq!(node_id, partition.leader_id);
        assert_eq!(0, partition.leader_epoch);
        assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
    }

    let offline_replicas = partitions[0]
        .offline_replicas
        .as_deref()
        .unwrap_or_default();
    assert!(offline_replicas.is_empty());

    let last_known_elr = partitions[0].last_known_elr.as_deref().unwrap_or_default();
    assert!(last_known_elr.is_empty());

    let eligible_leader_replicas = partitions[0]
        .eligible_leader_replicas
        .as_deref()
        .unwrap_or_default();
    assert!(eligible_leader_replicas.is_empty());

    let isr_nodes = partitions[0].isr_nodes.as_deref().unwrap_or_default();
    assert_eq!(1, isr_nodes.len());
    assert!(isr_nodes.iter().all(|isr_node| *isr_node == node_id));

    Ok(())
}

#[tokio::test]
async fn duplicate() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let node_id = 12321;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(node_id)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(CreateTopicsService);

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = service
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments.clone())
                        .configs(configs.clone()),
                ]))
                .validate_only(Some(false)),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let response = service
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(
        ErrorCode::TopicAlreadyExists,
        ErrorCode::try_from(topics[0].error_code)?
    );
    Ok(())
}

/// Everything below is #443: a creation Kafka refuses at the door was accepted
/// here, so the mistake travelled instead of dying at its cause.
mod refusals {
    use std::sync::Arc;

    use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
    use tansu_sans_io::{
        CreateTopicsRequest, ErrorCode, MetadataRequest, NULL_TOPIC_ID,
        create_topics_request::CreatableTopic, create_topics_response::CreatableTopicResult,
        metadata_request::MetadataRequestTopic,
    };
    use tansu_storage::{CreateTopicsService, MetadataService, Storage, StorageContainer};
    use url::Url;

    use crate::common::{Error, cluster_id, init_tracing, storage_url};

    async fn storage() -> Result<Arc<Box<dyn Storage>>, Error> {
        StorageContainer::builder()
            .cluster_id(cluster_id())
            .node_id(111)
            .advertised_listener(Url::parse("tcp://localhost:9092")?)
            .storage(storage_url()?)
            .build()
            .await
            .map_err(Into::into)
    }

    async fn create(
        storage: &Arc<Box<dyn Storage>>,
        name: &str,
        num_partitions: i32,
        validate_only: bool,
    ) -> Result<CreatableTopicResult, Error> {
        let service = {
            let storage = storage.clone();
            MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
        };

        let response = service
            .serve(
                Context::default(),
                CreateTopicsRequest::default()
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(num_partitions)
                            .replication_factor(1)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    ))
                    .validate_only(Some(validate_only)),
            )
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());

        Ok(topics[0].clone())
    }

    /// Whether the broker knows a topic by that name.
    async fn exists(storage: &Arc<Box<dyn Storage>>, name: &str) -> Result<bool, Error> {
        let service = {
            let storage = storage.clone();
            MapStateLayer::new(|_| storage).into_layer(MetadataService)
        };

        let response = service
            .serve(
                Context::default(),
                MetadataRequest::default()
                    .topics(Some(
                        [MetadataRequestTopic::default()
                            .name(Some(name.into()))
                            .topic_id(Some(NULL_TOPIC_ID))]
                        .into(),
                    ))
                    .allow_auto_topic_creation(Some(false))
                    .include_topic_authorized_operations(Some(false)),
            )
            .await?;

        let topics = response.topics.unwrap_or_default();

        Ok(topics
            .iter()
            .any(|topic| topic.name.as_deref() == Some(name) && topic.error_code == 0))
    }

    /// A topic name is not just a label: it is an object-store key component, a
    /// segment footer entry, a routing prefix and a metric label. An
    /// unrepresentable one breaks far from the client that chose it, and by
    /// then nothing points back — which is why Kafka kills it at creation.
    #[tokio::test]
    async fn a_name_outside_the_legal_pattern_is_refused() -> Result<(), Error> {
        let _guard = init_tracing()?;
        let storage = storage().await?;

        for name in [
            "some topic/bad!",
            "with space",
            "with/slash",
            ".",
            "..",
            "naïve",
        ] {
            let result = create(&storage, name, 1, false).await?;

            assert_eq!(
                ErrorCode::InvalidTopicException,
                ErrorCode::try_from(result.error_code)?,
                "{name} must be refused",
            );

            assert!(!exists(&storage, name).await?, "{name} must not exist");
        }

        Ok(())
    }

    /// And the names that are legal stay legal — the pattern has to admit the
    /// dotted connector names this fleet is built on, since they are also what
    /// the routing prefix is derived from.
    #[tokio::test]
    async fn a_legal_name_is_still_created() -> Result<(), Error> {
        let _guard = init_tracing()?;
        let storage = storage().await?;

        for name in ["abcba", "org.env.conn.tab_a", "with-dash_and.dot0123"] {
            let result = create(&storage, name, 1, false).await?;

            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(result.error_code)?,
                "{name} must be created",
            );
        }

        Ok(())
    }

    /// Zero partitions is undefined territory for every client library, and a
    /// topic that has none can never be produced to or consumed from.
    #[tokio::test]
    async fn a_topic_with_no_partitions_is_refused() -> Result<(), Error> {
        let _guard = init_tracing()?;
        let storage = storage().await?;

        let result = create(&storage, "no-partitions", 0, false).await?;

        assert_eq!(
            ErrorCode::InvalidPartitions,
            ErrorCode::try_from(result.error_code)?,
        );

        assert!(!exists(&storage, "no-partitions").await?);

        Ok(())
    }

    /// `-1` still means "use the broker default", and is resolved before it
    /// reaches the layer that refuses everything below 1 — so the sentinel must
    /// not have become an error along with the mistake it looks like.
    #[tokio::test]
    async fn the_default_partition_sentinel_still_resolves() -> Result<(), Error> {
        let _guard = init_tracing()?;
        let storage = storage().await?;

        let result = create(&storage, "default-partitions", -1, false).await?;

        assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);
        assert_eq!(Some(3), result.num_partitions);

        Ok(())
    }

    /// A dry run that provisions is worse than no dry run: a plan/apply
    /// provider or a CI validation job reports what it *would* do while having
    /// already done it.
    #[tokio::test]
    async fn validate_only_does_not_create() -> Result<(), Error> {
        let _guard = init_tracing()?;
        let storage = storage().await?;

        let result = create(&storage, "dry-run", 3, true).await?;

        assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);

        // No topic was created, so there is no id to name — which is what a
        // client reads back as `NULL_TOPIC_ID`.
        assert_eq!(Some(NULL_TOPIC_ID), result.topic_id);
        assert!(
            !exists(&storage, "dry-run").await?,
            "a dry run must not provision"
        );

        // And the real creation that follows it still works: the dry run left
        // nothing behind that would collide.
        let created = create(&storage, "dry-run", 3, false).await?;
        assert_eq!(ErrorCode::None, ErrorCode::try_from(created.error_code)?);
        assert_ne!(Some(NULL_TOPIC_ID), created.topic_id);
        assert!(exists(&storage, "dry-run").await?);

        Ok(())
    }

    /// A dry run still answers the question a plan is asking — "would this
    /// creation succeed?" — so an existing name is reported, as Kafka reports
    /// it.
    #[tokio::test]
    async fn validate_only_reports_a_name_already_taken() -> Result<(), Error> {
        let _guard = init_tracing()?;
        let storage = storage().await?;

        let created = create(&storage, "taken", 1, false).await?;
        assert_eq!(ErrorCode::None, ErrorCode::try_from(created.error_code)?);

        let dry_run = create(&storage, "taken", 1, true).await?;
        assert_eq!(
            ErrorCode::TopicAlreadyExists,
            ErrorCode::try_from(dry_run.error_code)?,
        );

        Ok(())
    }

    /// A dry run of a malformed creation is refused for being malformed, not
    /// waved through for being a dry run: the whole point of the plan is to
    /// find this out.
    #[tokio::test]
    async fn validate_only_still_refuses_what_it_would_refuse() -> Result<(), Error> {
        let _guard = init_tracing()?;
        let storage = storage().await?;

        let bad_name = create(&storage, "some topic/bad!", 1, true).await?;
        assert_eq!(
            ErrorCode::InvalidTopicException,
            ErrorCode::try_from(bad_name.error_code)?,
        );

        let no_partitions = create(&storage, "dry-no-partitions", 0, true).await?;
        assert_eq!(
            ErrorCode::InvalidPartitions,
            ErrorCode::try_from(no_partitions.error_code)?,
        );

        Ok(())
    }
}
