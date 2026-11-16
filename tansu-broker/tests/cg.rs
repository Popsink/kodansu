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

//! Committed offsets straight on the storage engine: committed, fetched, and
//! gone once the topic or the group is deleted; and what an unknown topic,
//! partition or group answers.

use std::slice::from_ref;

use rand::RngExt as _;
use tansu_sans_io::ErrorCode;
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_storage::{OffsetCommitRequest, Topition};

pub mod common;

type Engine = std::sync::Arc<dyn tansu_storage::Storage>;

const PARTITIONS: i32 = 6;

/// A `memory://` engine with one broker, at a random node id, registered.
async fn registered() -> tansu_broker::Result<Engine> {
    let cluster = uuid::Uuid::now_v7();
    let node = rand::rng().random_range(0..i32::MAX);
    let listener = url::Url::parse("tcp://127.0.0.1/")?;
    let engine = common::storage_container(cluster, node, listener).await?;
    common::register_broker(cluster, node, engine.clone()).await?;
    Ok(engine)
}

/// A random partition of a randomly named topic, which is created with
/// six partitions only when `create` is set.
async fn a_partition(engine: &Engine, create: bool) -> tansu_broker::Result<Topition> {
    let name = common::alphanumeric_string(15);
    tracing::debug!(topic_name = ?name);
    if create {
        let topic = CreatableTopic::default()
            .name(name.clone())
            .num_partitions(PARTITIONS);
        let topic = topic
            .replication_factor(0)
            .assignments(Some(vec![]))
            .configs(Some(vec![]));
        let creating = engine.create_topic(topic, false);
        let id = creating.await?;
        tracing::debug!(topic_id = ?id);
    }
    Ok(Topition::new(name, rand::rng().random_range(0..PARTITIONS)))
}

/// Commits a random offset for `group` on `topition`, returning it with the
/// one error code the commit answers.
async fn commit(
    engine: &Engine,
    group: &str,
    topition: &Topition,
) -> tansu_broker::Result<(i64, ErrorCode)> {
    let offset = rand::rng().random_range(0..i64::MAX);
    let at = OffsetCommitRequest::default().offset(offset);
    let request = [(topition.clone(), at)];
    let committing = engine.offset_commit(group, None, &request);
    let outcomes = committing.await?;
    assert_eq!(1, outcomes.len());
    Ok((offset, outcomes[0].1))
}

/// The offset `group` has on `topition`, as `OffsetFetch` answers it: -1
/// when there is none. The answer always names the partition.
async fn fetched(engine: &Engine, group: &str, topition: &Topition) -> tansu_broker::Result<i64> {
    let fetching = engine.offset_fetch(Some(group), from_ref(topition), None);
    let offsets = fetching.await?;
    let offset = offsets.get(topition).map(|committed| committed.offset);
    Ok(offset.expect("the fetched partition is always answered"))
}

/// Deletes `group` alone, returning the error code of its one answer.
async fn deleted_alone(engine: &Engine, group: &str) -> tansu_broker::Result<ErrorCode> {
    let wanted = [group.to_owned()];
    let answers = engine.delete_groups(Some(&wanted)).await?;
    let answered: Vec<_> = answers
        .iter()
        .map(|answer| answer.group_id.as_str())
        .collect();
    assert_eq!(vec![group], answered);
    ErrorCode::try_from(answers[0].error_code).map_err(Into::into)
}

#[tokio::test]
async fn a_committed_offset_fetches_back_and_lists_its_group() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let engine = registered().await?;
    let topition = a_partition(&engine, true).await?;
    let group = common::alphanumeric_string(15);

    let (offset, outcome) = commit(&engine, &group, &topition).await?;
    assert_eq!(ErrorCode::None, outcome);
    assert_eq!(offset, fetched(&engine, &group, &topition).await?);

    let by_group = engine.committed_offset_topitions(&group).await?;
    assert_eq!(
        Some(offset),
        by_group.get(&topition).map(|committed| committed.offset)
    );

    let listed = engine.list_groups(None).await?;
    let ids: Vec<_> = listed
        .iter()
        .map(|listing| listing.group_id.as_str())
        .collect();
    assert_eq!(vec![group.as_str()], ids);
    Ok(())
}

#[tokio::test]
async fn deleting_the_topic_forgets_its_committed_offsets() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let engine = registered().await?;
    let topition = a_partition(&engine, true).await?;
    let group = common::alphanumeric_string(15);

    let (offset, outcome) = commit(&engine, &group, &topition).await?;
    assert_eq!(ErrorCode::None, outcome);
    assert_eq!(offset, fetched(&engine, &group, &topition).await?);

    let topic = tansu_storage::TopicId::from(topition.topic().to_string());
    let deleting = engine.delete_topic(&topic);
    assert_eq!(ErrorCode::None, deleting.await?);
    assert_eq!(-1, fetched(&engine, &group, &topition).await?);
    Ok(())
}

#[tokio::test]
async fn deleting_the_group_forgets_its_committed_offsets() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let engine = registered().await?;
    let topition = a_partition(&engine, true).await?;
    let group = common::alphanumeric_string(15);

    let (offset, outcome) = commit(&engine, &group, &topition).await?;
    assert_eq!(ErrorCode::None, outcome);
    assert_eq!(offset, fetched(&engine, &group, &topition).await?);

    assert_eq!(ErrorCode::None, deleted_alone(&engine, &group).await?);
    assert_eq!(-1, fetched(&engine, &group, &topition).await?);
    Ok(())
}

#[tokio::test]
async fn deleting_an_unknown_group_is_group_id_not_found() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let engine = registered().await?;
    let group = common::alphanumeric_string(15);

    let outcome = deleted_alone(&engine, &group).await?;
    assert_eq!(ErrorCode::GroupIdNotFound, outcome);
    Ok(())
}

#[tokio::test]
async fn committing_to_an_unknown_topic_is_refused_and_lists_no_group() -> tansu_broker::Result<()>
{
    let _guard = common::init_tracing()?;
    let engine = registered().await?;
    let topition = a_partition(&engine, false).await?;
    let group = common::alphanumeric_string(15);

    let (_, outcome) = commit(&engine, &group, &topition).await?;
    assert_eq!(ErrorCode::UnknownTopicOrPartition, outcome);
    assert_eq!(-1, fetched(&engine, &group, &topition).await?);
    assert_eq!(0, engine.list_groups(None).await?.len());
    Ok(())
}

#[tokio::test]
async fn an_unknown_topic_fetches_no_offset_and_lists_no_group() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let engine = registered().await?;
    let topition = a_partition(&engine, false).await?;
    let group = common::alphanumeric_string(15);

    assert_eq!(-1, fetched(&engine, &group, &topition).await?);
    assert_eq!(0, engine.list_groups(None).await?.len());
    Ok(())
}

#[tokio::test]
async fn a_fresh_engine_lists_no_group() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let engine = registered().await?;
    assert_eq!(0, engine.list_groups(None).await?.len());
    Ok(())
}
