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

//! The consumer-group admin edges (#445).
//!
//! Group coordination itself is conformant — join, rebalance, offset
//! commit/fetch all match Kafka in differential testing. What was lax is the
//! admin surface around it, and every one of these was lax in the same
//! direction: an operation that should have been refused succeeded, or a
//! distinction Kafka draws was collapsed into the friendlier answer.
//!
//! That direction is what makes them worth fixing together. A refusal is a
//! safety property — an admin API that cannot be run by mistake — and a
//! distinction is what tooling reads.

use std::{collections::BTreeMap, sync::Arc};

use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use tansu_sans_io::{
    DescribeGroupsRequest, ErrorCode, create_topics_request::CreatableTopic,
    describe_groups_response::DescribedGroup, offset_commit_request::OffsetCommitRequestPartition,
};
use tansu_storage::{
    DescribeGroupsService, GenerationDoc, MemberDoc, MemberRef, OffsetCommitRequest, Storage,
    StorageContainer, Topition,
};
use url::Url;

use crate::common::{Error, cluster_id, init_tracing, storage_url};

mod common;

const TOPIC: &str = "group-admin";
const PARTITIONS: i32 = 2;

async fn storage() -> Result<Arc<Box<dyn Storage>>, Error> {
    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .build()
        .await?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(PARTITIONS)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(storage)
}

/// A group with one member in it, written the way `SyncGroup` leaves it: the
/// generation's member set is the authority, and a member document beside it.
async fn join(
    storage: &Arc<Box<dyn Storage>>,
    group_id: &str,
    member_id: &str,
) -> Result<(), Error> {
    _ = storage
        .write_group_member(
            group_id,
            member_id,
            MemberDoc {
                seq: 0,
                last_contact_ms: 1_000,
                session_timeout_ms: 45_000,
                ..Default::default()
            },
            None,
        )
        .await
        .map_err(|error| Error::Message(format!("{error:?}")))?;

    _ = storage
        .update_group_generation(
            group_id,
            GenerationDoc {
                generation_id: 1,
                session_timeout_ms: 45_000,
                members: BTreeMap::from([(member_id.to_owned(), MemberRef::default())]),
                ..Default::default()
            },
            None,
        )
        .await
        .map_err(|error| Error::Message(format!("{error:?}")))?;

    Ok(())
}

/// One group's `DescribeGroups` answer, through the service — the shape a
/// client reads, rather than the projection type behind it.
async fn describe(
    storage: &Arc<Box<dyn Storage>>,
    group_id: &str,
) -> Result<DescribedGroup, Error> {
    let service = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(DescribeGroupsService)
    };

    let response = service
        .serve(
            Context::default(),
            DescribeGroupsRequest::default()
                .groups(Some([group_id.into()].into()))
                .include_authorized_operations(Some(false)),
        )
        .await?;

    let groups = response.groups.unwrap_or_default();
    assert_eq!(1, groups.len());

    Ok(groups[0].clone())
}

fn commit_at(partition: i32, offset: i64) -> Result<(Topition, OffsetCommitRequest), Error> {
    Ok((
        Topition::new(TOPIC, partition),
        OffsetCommitRequest::try_from(
            &OffsetCommitRequestPartition::default()
                .partition_index(partition)
                .committed_offset(offset)
                .committed_leader_epoch(Some(-1))
                .commit_timestamp(None)
                .committed_metadata(None),
        )?,
    ))
}

/// Deleting the group of a live consumer takes its coordinator out from under
/// it mid-poll. Kafka simply refuses, and refusing is the whole safety
/// property: a cleanup script or an operator command cannot do this by mistake.
#[tokio::test]
async fn a_group_with_a_live_member_is_not_deleted() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let group_id = "live";

    join(&storage, group_id, "m-1").await?;

    let deleted = storage.delete_groups(Some(&[group_id.into()])).await?;
    assert_eq!(1, deleted.len());
    assert_eq!(
        ErrorCode::NonEmptyGroup,
        ErrorCode::try_from(deleted[0].error_code)?,
    );

    // And nothing was removed: a refusal that half-deleted the group would be
    // worse than the behaviour it replaced.
    let described = describe(&storage, group_id).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(described.error_code)?);
    assert_eq!(1, described.members.unwrap_or_default().len());

    Ok(())
}

/// A group whose consumers have all gone away is deletable on its own — the
/// refusal must not become a way to leak groups. Membership is the generation's
/// member set, which a session-timeout sweep retires.
#[tokio::test]
async fn a_group_whose_members_have_gone_is_deleted() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let group_id = "emptied";

    join(&storage, group_id, "m-1").await?;

    // The sweep's outcome: a generation with nobody in it.
    let (_, version) = storage
        .read_group_generation(group_id)
        .await?
        .expect("a generation");

    _ = storage
        .update_group_generation(
            group_id,
            GenerationDoc {
                seq: 1,
                generation_id: 2,
                session_timeout_ms: 45_000,
                ..Default::default()
            },
            Some(version),
        )
        .await
        .map_err(|error| Error::Message(format!("{error:?}")))?;

    let deleted = storage.delete_groups(Some(&[group_id.into()])).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(deleted[0].error_code)?);

    Ok(())
}

/// Deleting a group that never existed says so. It answered `NONE` because
/// existence was inferred from a delete — and a delete of an absent key
/// succeeds, on S3 and on `InMemory` alike, so the not-found branch was
/// unreachable.
#[tokio::test]
async fn deleting_a_group_that_never_existed_says_so() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;

    let deleted = storage
        .delete_groups(Some(&["never-existed".into()]))
        .await?;

    assert_eq!(1, deleted.len());
    assert_eq!(
        ErrorCode::GroupIdNotFound,
        ErrorCode::try_from(deleted[0].error_code)?,
    );

    Ok(())
}

/// **A group exists if anything of it does.** A client may commit for a group
/// id without ever joining, and such a group has no generation at all — but it
/// is one `kafka-consumer-groups --describe` lists and one `DeleteGroups` must
/// be able to reap. Answering "never existed" for it would leak its offsets
/// forever, which is the way the not-found fix breaks if it is drawn too
/// narrowly.
#[tokio::test]
async fn a_group_that_only_ever_committed_still_exists() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let group_id = "offsets-only";

    let committed = storage
        .offset_commit(group_id, None, &[commit_at(0, 7)?])
        .await?;
    assert_eq!(ErrorCode::None, committed[0].1);

    // Describing it finds an empty group, not a dead one.
    let described = describe(&storage, group_id).await?;
    assert_eq!("Empty", described.group_state.as_str());

    // And deleting it reaps it, rather than reporting it was never there.
    let deleted = storage.delete_groups(Some(&[group_id.into()])).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(deleted[0].error_code)?);

    Ok(())
}

/// Kafka reports `Empty` for a group that exists with nobody in it and `Dead`
/// for one it has never heard of. Collapsing the two leaves
/// cleanup-by-inactivity tooling unable to tell a group it should reap from one
/// it has already reaped, and monitoring unable to tell a group that went away
/// from one that is merely idle.
#[tokio::test]
async fn a_group_that_never_existed_is_dead_not_empty() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;

    let described = describe(&storage, "never-existed").await?;

    // `NONE` with the state `Dead`, as Kafka answers: not existing is an answer
    // about the group, not a failure to give one.
    assert_eq!(ErrorCode::None, ErrorCode::try_from(described.error_code)?);
    assert_eq!("Dead", described.group_state.as_str());

    Ok(())
}

/// And a group that does exist with nobody in it is still `Empty` — the two
/// answers are only useful because they are different, so both are asserted.
#[tokio::test]
async fn a_group_with_no_members_is_empty_not_dead() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let group_id = "idle";

    _ = storage
        .update_group_generation(
            group_id,
            GenerationDoc {
                generation_id: 1,
                session_timeout_ms: 45_000,
                ..Default::default()
            },
            None,
        )
        .await
        .map_err(|error| Error::Message(format!("{error:?}")))?;

    let described = describe(&storage, group_id).await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(described.error_code)?);
    assert_eq!("Empty", described.group_state.as_str());

    Ok(())
}

/// Committing partition 9 of a two-partition topic used to succeed, because the
/// existence test was whether the *topic* existed. A configuration typo landed
/// as a stored offset and surfaced much later as "the consumer restarted from
/// the wrong place" — pointing at the consumer rather than at the number that
/// was wrong.
#[tokio::test]
async fn a_commit_on_a_partition_that_does_not_exist_is_refused() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let group_id = "typo";

    for partition in [PARTITIONS, 9, i32::MAX] {
        let committed = storage
            .offset_commit(group_id, None, &[commit_at(partition, 7)?])
            .await?;

        assert_eq!(1, committed.len());
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            committed[0].1,
            "partition {partition} of a {PARTITIONS}-partition topic",
        );
    }

    // Nothing was stored for any of them.
    let fetched = storage
        .offset_fetch(Some(group_id), &[Topition::new(TOPIC, 9)], None)
        .await?;

    assert_eq!(
        Some(-1),
        fetched
            .get(&Topition::new(TOPIC, 9))
            .map(|committed| committed.offset)
    );

    Ok(())
}

/// Every partition the topic does have still commits — the refusal has to be
/// exactly the partitions that are missing, or it breaks every consumer.
#[tokio::test]
async fn a_commit_on_a_partition_that_exists_is_stored() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let group_id = "ordinary";

    for partition in 0..PARTITIONS {
        let committed = storage
            .offset_commit(group_id, None, &[commit_at(partition, 7)?])
            .await?;

        assert_eq!(ErrorCode::None, committed[0].1, "partition {partition}");
    }

    let topitions: Vec<Topition> = (0..PARTITIONS).map(|p| Topition::new(TOPIC, p)).collect();
    let fetched = storage
        .offset_fetch(Some(group_id), &topitions, None)
        .await?;

    for topition in &topitions {
        assert_eq!(
            Some(7),
            fetched.get(topition).map(|committed| committed.offset),
            "{topition:?}"
        );
    }

    Ok(())
}

/// A request mixing good and bad partitions answers per partition: the bad one
/// is refused and the good one is stored, rather than one typo failing a
/// consumer's whole commit.
#[tokio::test]
async fn a_mixed_commit_is_answered_per_partition() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let group_id = "mixed";

    let committed = storage
        .offset_commit(group_id, None, &[commit_at(0, 7)?, commit_at(9, 11)?])
        .await?;

    assert_eq!(2, committed.len());
    assert_eq!(ErrorCode::None, committed[0].1);
    assert_eq!(ErrorCode::UnknownTopicOrPartition, committed[1].1);

    let fetched = storage
        .offset_fetch(Some(group_id), &[Topition::new(TOPIC, 0)], None)
        .await?;

    assert_eq!(
        Some(7),
        fetched
            .get(&Topition::new(TOPIC, 0))
            .map(|committed| committed.offset)
    );

    Ok(())
}
