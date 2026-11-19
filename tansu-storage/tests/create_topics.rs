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

//! `CreateTopics`: what a creation answers, the broker defaults `-1` stands
//! for, a duplicate name, and the creations refused at the door.

use std::sync::Arc;

use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::create_topics_response::CreatableTopicResult;
use tansu_sans_io::describe_topic_partitions_request::TopicRequest;
use tansu_sans_io::describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic;
use tansu_sans_io::metadata_request::MetadataRequestTopic;
use tansu_sans_io::metadata_response::MetadataResponseTopic;
use tansu_sans_io::{
    CreateTopicsRequest, DescribeTopicPartitionsRequest, ErrorCode, MetadataRequest, NULL_TOPIC_ID,
};
use tansu_storage::{
    CreateTopicsService, DescribeTopicPartitionsService, MetadataService, Storage,
};

use crate::common::{Error, NODE_ID, init_tracing, serve, storage};

mod common;

/// What `CreateTopics` answers for one topic `name` with `partitions` and
/// `replicas`, as a dry run or not.
async fn create(
    storage: &Arc<dyn Storage>,
    name: &str,
    (partitions, replicas): (i32, i16),
    validate_only: bool,
) -> Result<CreatableTopicResult, Error> {
    let creatable = CreatableTopic::default()
        .name(name.to_owned())
        .num_partitions(partitions)
        .replication_factor(replicas)
        .assignments(Some(Vec::new()))
        .configs(Some(Vec::new()));
    let request = CreateTopicsRequest::default()
        .topics(Some(vec![creatable]))
        .validate_only(Some(validate_only));
    let response = serve(storage, CreateTopicsService, request).await?;
    let [topic] = <[_; 1]>::try_from(response.topics.unwrap_or_default()).expect("one topic");
    Ok(topic)
}

/// How a creation ended: its error, and whether it answered a topic id.
fn outcome(topic: &CreatableTopicResult) -> Result<(ErrorCode, bool), Error> {
    let error_code = ErrorCode::try_from(topic.error_code)?;
    Ok((error_code, topic.topic_id != Some(NULL_TOPIC_ID)))
}

/// What a creation answered it made: the name, partitions and replicas.
fn shape(topic: &CreatableTopicResult) -> (&str, Option<i32>, Option<i16>) {
    (
        topic.name.as_str(),
        topic.num_partitions,
        topic.replication_factor,
    )
}

/// Whether the broker knows a topic by that name.
async fn exists(storage: &Arc<dyn Storage>, name: &str) -> Result<bool, Error> {
    let wanted = MetadataRequestTopic::default()
        .name(Some(name.to_owned()))
        .topic_id(Some(NULL_TOPIC_ID));
    let request = MetadataRequest::default().topics(Some(vec![wanted]));
    let response = serve(storage, MetadataService, request).await?;
    let known = |topic: &MetadataResponseTopic| {
        topic.name.as_deref() == Some(name) && topic.error_code == 0
    };
    Ok(response.topics.unwrap_or_default().iter().any(known))
}

/// The one topic `DescribeTopicPartitions` answers for `name`.
async fn described(
    storage: &Arc<dyn Storage>,
    name: &str,
) -> Result<DescribeTopicPartitionsResponseTopic, Error> {
    let wanted = TopicRequest::default().name(name.to_owned());
    let request = DescribeTopicPartitionsRequest::default().topics(Some(vec![wanted]));
    let response = serve(storage, DescribeTopicPartitionsService, request).await?;
    let [topic] = <[_; 1]>::try_from(response.topics.unwrap_or_default()).expect("one topic");
    Ok(topic)
}

/// Everything below is #443: a creation Kafka refuses at the door was accepted
/// here, so the mistake travelled instead of dying at its cause.
mod refusals {
    use std::sync::Arc;

    use tansu_sans_io::create_topics_response::CreatableTopicResult;
    use tansu_sans_io::{ErrorCode, NULL_TOPIC_ID};
    use tansu_storage::Storage;

    use crate::common::{Error, init_tracing, storage};

    use super::exists;

    /// [`super::create`] with one replica.
    async fn create(
        storage: &Arc<dyn Storage>,
        name: &str,
        partitions: i32,
        validate_only: bool,
    ) -> Result<CreatableTopicResult, Error> {
        super::create(storage, name, (partitions, 1), validate_only).await
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
        let error_code = ErrorCode::try_from(dry_run.error_code)?;
        assert_eq!(ErrorCode::TopicAlreadyExists, error_code);

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

#[tokio::test]
async fn a_creation_answers_what_was_asked() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let created = create(&storage().await?, "pqr", (5, 3), false).await?;
    assert_eq!((ErrorCode::None, true), outcome(&created)?);
    assert_eq!(("pqr", Some(5), Some(3)), shape(&created));

    Ok(())
}

/// `-1` partitions and `-1` replicas are the broker defaults, 3 and 1, and the
/// partitions they make are led by this node with no other replica.
#[tokio::test]
async fn minus_one_takes_the_broker_defaults() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let created = create(&storage, "pqr", (-1, -1), false).await?;
    assert_eq!((ErrorCode::None, true), outcome(&created)?);
    assert_eq!(("pqr", Some(3), Some(1)), shape(&created));

    let topic = described(&storage, "pqr").await?;
    let error_code = ErrorCode::try_from(topic.error_code)?;
    assert_eq!(
        (Some("pqr"), ErrorCode::None),
        (topic.name.as_deref(), error_code)
    );

    let partitions = topic.partitions.unwrap_or_default();
    let led = partitions
        .iter()
        .map(|p| {
            let error_code = ErrorCode::try_from(p.error_code)?;
            Ok((p.partition_index, p.leader_id, p.leader_epoch, error_code))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let expected: Vec<_> = (0..3).map(|i| (i, NODE_ID, 0, ErrorCode::None)).collect();
    assert_eq!(expected, led, "three partitions, each led by this node");

    let first = &partitions[0];
    let empty = |replicas: &Option<Vec<i32>>| replicas.as_deref().unwrap_or_default().is_empty();
    assert!(empty(&first.offline_replicas));
    assert!(empty(&first.last_known_elr));
    assert!(empty(&first.eligible_leader_replicas));
    assert_eq!(Some(vec![NODE_ID]), first.isr_nodes);

    Ok(())
}

/// A second creation under the same name is refused, and answers no id.
#[tokio::test]
async fn a_duplicate_name_is_refused() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let first = create(&storage, "pqr", (5, 3), false).await?;
    let second = create(&storage, "pqr", (5, 3), false).await?;
    assert_eq!((ErrorCode::None, true), outcome(&first)?);
    assert_eq!((ErrorCode::TopicAlreadyExists, false), outcome(&second)?);
    assert_eq!(
        [("pqr", Some(5), Some(3)); 2],
        [shape(&first), shape(&second)]
    );

    Ok(())
}
