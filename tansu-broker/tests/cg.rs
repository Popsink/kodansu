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

// Every case here builds a `memory://` container, which needs the one storage
// engine this fork ships. The gate was on `mod in_memory` until #552 dissolved
// it; with the module gone it belongs to the file.
#![cfg(feature = "dynostore")]

use std::{slice, sync::Arc};

use common::{alphanumeric_string, init_tracing, register_broker};
use rand::{prelude::*, rng};
use tansu_broker::Result;
use tansu_sans_io::{ErrorCode, create_topics_request::CreatableTopic};
use tansu_storage::{OffsetCommitRequest, Storage, Topition};
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub mod common;

async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<Arc<dyn Storage>> {
    common::storage_container(cluster, node, Url::parse("tcp://127.0.0.1/")?).await
}

#[tokio::test]
async fn offset_commit() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);
    let sc = storage_container(cluster_id, broker_id).await?;

    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

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
    debug!(?topic_id);

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset = rng().random_range(0..i64::MAX);

    let commit = sc
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(offset),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    let co_tps = sc.committed_offset_topitions(&group_id).await?;
    assert!(co_tps.contains_key(&topition));
    assert_eq!(
        Some(offset),
        co_tps.get(&topition).map(|committed| committed.offset)
    );

    let groups = sc.list_groups(None).await?;
    assert_eq!(1, groups.len());
    assert_eq!(group_id, groups[0].group_id);

    Ok(())
}

#[tokio::test]
async fn topic_delete_cascade_to_offset_commit() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);
    let sc = storage_container(cluster_id, broker_id).await?;

    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

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
    debug!(?topic_id);

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset = rng().random_range(0..i64::MAX);

    let commit = sc
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(offset),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    assert_eq!(ErrorCode::None, sc.delete_topic(&topic_name.into()).await?);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(-1),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    Ok(())
}

#[tokio::test]
async fn consumer_group_delete_cascade_to_offset_commit() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);
    let sc = storage_container(cluster_id, broker_id).await?;

    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

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
    debug!(?topic_id);

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset = rng().random_range(0..i64::MAX);

    let commit = sc
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(offset),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    let deleted = sc.delete_groups(Some(slice::from_ref(&group_id))).await?;
    assert_eq!(1, deleted.len());
    assert_eq!(group_id, deleted[0].group_id);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(deleted[0].error_code)?);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(-1),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    Ok(())
}

#[tokio::test]
async fn delete_unknown_consumer_group() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);
    let sc = storage_container(cluster_id, broker_id).await?;

    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let group_id: String = alphanumeric_string(15);

    let deleted = sc.delete_groups(Some(slice::from_ref(&group_id))).await?;
    assert_eq!(1, deleted.len());
    assert_eq!(group_id, deleted[0].group_id);
    assert_eq!(
        ErrorCode::GroupIdNotFound,
        ErrorCode::try_from(deleted[0].error_code)?
    );

    Ok(())
}

#[tokio::test]
async fn offset_commit_unknown_topition() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);
    let sc = storage_container(cluster_id, broker_id).await?;

    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset = rng().random_range(0..i64::MAX);

    let commit = sc
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::UnknownTopicOrPartition, commit[0].1);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(-1),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    let groups = sc.list_groups(None).await?;
    assert_eq!(0, groups.len());

    Ok(())
}

#[tokio::test]
async fn offset_fetch_unknown_topition() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);
    let sc = storage_container(cluster_id, broker_id).await?;

    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let group_id: String = alphanumeric_string(15);

    let offset_fetch = sc
        .offset_fetch(Some(&group_id), slice::from_ref(&topition), None)
        .await?;
    assert!(offset_fetch.contains_key(&topition));
    assert_eq!(
        Some(-1),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    let groups = sc.list_groups(None).await?;
    assert_eq!(0, groups.len());

    Ok(())
}

#[tokio::test]
async fn list_groups_none() -> Result<()> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);
    let sc = storage_container(cluster_id, broker_id).await?;

    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let groups = sc.list_groups(None).await?;
    assert_eq!(0, groups.len());

    Ok(())
}
