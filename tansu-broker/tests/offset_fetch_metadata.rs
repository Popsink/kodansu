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

//! Commit metadata reaches the `OffsetFetch` response (#445).
//!
//! `tansu-storage` proves the value survives storage; this proves it survives
//! the response builder, which is where it was hardcoded — `Some("")` in the
//! v0–v7 shape and `None` in the v8+ one, regardless of what had been stored.
//!
//! Both shapes, because a client picks one by its negotiated version and the
//! two were wrong in different ways.

use std::sync::Arc;

use anyhow::Result;
use tansu_broker::coordinator::group::{Coordinator, administrator::Controller};
use tansu_sans_io::{
    Body, ErrorCode,
    create_topics_request::CreatableTopic,
    offset_commit_request::OffsetCommitRequestPartition,
    offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
    },
};
use tansu_storage::{OffsetCommitRequest, Storage, StorageContainer, Topition};
use url::Url;
use uuid::Uuid;

const TOPIC: &str = "commit-metadata";
const GROUP: &str = "checkpointing";
const CHECKPOINT: &str = "checkpoint-abc";

async fn storage_with_a_commit() -> Result<Arc<Box<dyn Storage>>> {
    let storage = StorageContainer::builder()
        .cluster_id(Uuid::now_v7().to_string())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://")?)
        .build()
        .await?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let committed = storage
        .offset_commit(
            GROUP,
            None,
            &[(
                Topition::new(TOPIC, 0),
                OffsetCommitRequest::try_from(
                    &OffsetCommitRequestPartition::default()
                        .partition_index(0)
                        .committed_offset(7)
                        .committed_leader_epoch(Some(-1))
                        .commit_timestamp(None)
                        .committed_metadata(Some(CHECKPOINT.into())),
                )?,
            )],
        )
        .await?;

    assert_eq!(ErrorCode::None, committed[0].1);

    Ok(storage)
}

/// The v8+ `groups` shape, which is what a modern client sends. The metadata
/// was hardcoded `None` here.
#[tokio::test]
async fn metadata_reaches_the_v8_response() -> Result<()> {
    let storage = storage_with_a_commit().await?;
    let coordinator = Controller::with_storage(storage)?;

    let Body::OffsetFetchResponse(response) = coordinator
        .offset_fetch(
            None,
            None,
            Some(&[OffsetFetchRequestGroup::default()
                .group_id(GROUP.into())
                .topics(Some(
                    [OffsetFetchRequestTopics::default()
                        .name(TOPIC.into())
                        .partition_indexes(Some([0].into()))]
                    .into(),
                ))]),
            Some(false),
        )
        .await?
    else {
        panic!("an OffsetFetch response")
    };

    let groups = response.groups.unwrap_or_default();
    assert_eq!(1, groups.len());

    let topics = groups[0].topics.clone().unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());

    assert_eq!(7, partitions[0].committed_offset);
    assert_eq!(Some(CHECKPOINT.to_owned()), partitions[0].metadata);

    Ok(())
}

/// The v0–v7 shape, where the metadata was hardcoded to the empty string —
/// which decodes as "committed, with no metadata" rather than as the checkpoint
/// that was stored.
#[tokio::test]
async fn metadata_reaches_the_legacy_response() -> Result<()> {
    let storage = storage_with_a_commit().await?;
    let coordinator = Controller::with_storage(storage)?;

    let Body::OffsetFetchResponse(response) = coordinator
        .offset_fetch(
            Some(GROUP),
            Some(&[OffsetFetchRequestTopic::default()
                .name(TOPIC.into())
                .partition_indexes(Some([0].into()))]),
            None,
            Some(false),
        )
        .await?
    else {
        panic!("an OffsetFetch response")
    };

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());

    assert_eq!(7, partitions[0].committed_offset);
    assert_eq!(Some(CHECKPOINT.to_owned()), partitions[0].metadata);

    Ok(())
}
