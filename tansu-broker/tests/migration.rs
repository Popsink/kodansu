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

//! A running classic group adopts `group.protocol=consumer` by a rolling
//! restart (#626).
//!
//! Three classic consumers form a group the classic way — join, a leader that
//! assigns, sync — and are then restarted one at a time with the consumer
//! protocol. The first converts the group; the classic members left carry on
//! inside the consumer group, told to rejoin when the assignment moves; and at
//! every step the group settles with each partition owned exactly once.
//!
//! The classic members encode their subscriptions and decode their
//! assignments in the embedded consumer protocol, as the Java client does,
//! because that is what the conversion reads and what the consumer group has
//! to answer in.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::UNIX_EPOCH,
};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use tansu_broker::coordinator::group::{Coordinator, OffsetCommit, administrator::Controller};
use tansu_sans_io::{
    Body, ConsumerGroupHeartbeatRequest, ErrorCode, OffsetCommitResponse,
    consumer::{
        ConsumerProtocolAssignment, ConsumerProtocolSubscription, MemberAssignment, MemberMetadata,
        TopicPartition,
    },
    consumer_group_heartbeat_request::TopicPartitions,
    create_topics_request::CreatableTopic,
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember,
    leave_group_request::MemberIdentity,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    sync_group_request::SyncGroupRequestAssignment,
};
use tansu_storage::{Storage, StorageContainer};
use tokio::task::JoinSet;
use url::Url;
use uuid::Uuid;

use crate::common::paused_clock;

pub mod common;

const GROUP: &str = "rolling";
const TOPIC: &str = "t";
const PARTITIONS: i32 = 6;
const RANGE: &str = "range";

/// Rounds of heartbeats a step is given to settle.
const ROUNDS: usize = 24;

struct Fixture {
    storage: Arc<dyn Storage>,
    coordinator: Controller<Arc<dyn Storage>>,
    topic_id: Uuid,
}

async fn fixture() -> Result<Fixture> {
    let storage: Arc<dyn Storage> = StorageContainer::builder()
        .cluster_id(Uuid::now_v7().to_string())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://")?)
        .build()
        .await?;

    let topic_id = storage
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

    Ok(Fixture {
        coordinator: Controller::with_storage(storage.clone())?.with_now(paused_clock),
        storage,
        topic_id,
    })
}

fn subscription(owned: &BTreeSet<i32>, generation: i32) -> Result<Bytes> {
    Bytes::try_from(
        &MemberMetadata::default().version(3).subscription(
            ConsumerProtocolSubscription::default()
                .topics([TOPIC.to_owned()])
                .owned_partitions(
                    [TopicPartition::default()
                        .topic(TOPIC)
                        .partitions(owned.iter().copied())]
                    .into_iter(),
                )
                .generation_id(Some(generation)),
        ),
    )
    .map_err(Into::into)
}

fn assignment(partitions: impl Iterator<Item = i32>) -> Result<Bytes> {
    Bytes::try_from(
        &MemberAssignment::default().version(3).assignment(
            ConsumerProtocolAssignment::default().assigned_partitions([TopicPartition::default()
                .topic(TOPIC)
                .partitions(partitions)]),
        ),
    )
    .map_err(Into::into)
}

fn decoded(assignment: &Bytes) -> Result<BTreeSet<i32>> {
    if assignment.is_empty() {
        return Ok(BTreeSet::new());
    }

    Ok(MemberAssignment::try_from(assignment.clone())?
        .assignment
        .assigned_partitions
        .into_iter()
        .filter(|assigned| assigned.topic == TOPIC)
        .flat_map(|assigned| assigned.partitions)
        .collect())
}

/// The range assignment a classic leader computes over `members`.
fn range(members: &[JoinGroupResponseMember]) -> Result<Vec<SyncGroupRequestAssignment>> {
    let member_ids = members
        .iter()
        .map(|member| member.member_id.clone())
        .collect::<BTreeSet<_>>();

    let count = i32::try_from(member_ids.len().max(1))?;
    let (base, extra) = (PARTITIONS / count, PARTITIONS % count);
    let mut next = 0;

    member_ids
        .into_iter()
        .zip(0..)
        .map(|(member_id, rank)| {
            let take = base + i32::from(rank < extra);
            let partitions = next..next + take;
            next += take;

            Ok(SyncGroupRequestAssignment::default()
                .member_id(member_id)
                .assignment(assignment(partitions)?))
        })
        .collect()
}

/// A classic consumer with the eager `range` protocol, the Java client's
/// default: it gives up everything it owns before it rejoins.
#[derive(Clone, Debug, Default)]
struct Classic {
    member_id: String,
    generation: i32,
    owned: BTreeSet<i32>,
}

impl Classic {
    /// Join and sync until it holds an assignment, as `poll()` does.
    async fn rejoin<C: Coordinator>(&mut self, coordinator: &C) -> Result<()> {
        self.owned.clear();

        for _ in 0..ROUNDS {
            let protocols = [JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(subscription(&self.owned, self.generation)?)];

            let Body::JoinGroupResponse(join) = coordinator
                .join(
                    Some("classic"),
                    GROUP,
                    45_000,
                    Some(300_000),
                    &self.member_id,
                    None,
                    "consumer",
                    Some(&protocols),
                    None,
                )
                .await?
            else {
                return Err(anyhow!("not a join response"));
            };

            match ErrorCode::try_from(join.error_code)? {
                ErrorCode::None => {}

                ErrorCode::MemberIdRequired => {
                    self.member_id = join.member_id;
                    continue;
                }

                ErrorCode::UnknownMemberId => {
                    self.member_id.clear();
                    continue;
                }

                ErrorCode::RebalanceInProgress => continue,
                otherwise => return Err(anyhow!("join: {otherwise:?}")),
            }

            self.member_id = join.member_id.clone();
            self.generation = join.generation_id;

            let assignments = if join.leader == join.member_id {
                range(join.members.as_deref().unwrap_or_default())?
            } else {
                vec![]
            };

            let Body::SyncGroupResponse(sync) = coordinator
                .sync(
                    GROUP,
                    self.generation,
                    &self.member_id,
                    None,
                    Some("consumer"),
                    join.protocol_name.as_deref(),
                    Some(&assignments),
                )
                .await?
            else {
                return Err(anyhow!("not a sync response"));
            };

            match ErrorCode::try_from(sync.error_code)? {
                ErrorCode::None => {
                    self.owned = decoded(&sync.assignment)?;
                    return Ok(());
                }

                ErrorCode::RebalanceInProgress | ErrorCode::IllegalGeneration => continue,
                otherwise => return Err(anyhow!("sync: {otherwise:?}")),
            }
        }

        Err(anyhow!("{} never synced", self.member_id))
    }

    async fn heartbeat<C: Coordinator>(&self, coordinator: &C) -> Result<ErrorCode> {
        let Body::HeartbeatResponse(heartbeat) = coordinator
            .heartbeat(GROUP, self.generation, &self.member_id, None)
            .await?
        else {
            return Err(anyhow!("not a heartbeat response"));
        };

        ErrorCode::try_from(heartbeat.error_code).map_err(Into::into)
    }

    async fn leave<C: Coordinator>(&self, coordinator: &C) -> Result<()> {
        let identity = [MemberIdentity::default()
            .member_id(self.member_id.clone())
            .group_instance_id(None)
            .reason(None)];

        _ = coordinator.leave(GROUP, None, Some(&identity)).await?;
        Ok(())
    }

    async fn commit<C: Coordinator>(&self, coordinator: &C) -> Result<ErrorCode> {
        commit(coordinator, &self.member_id, self.generation).await
    }
}

/// A consumer-protocol consumer, as the Java client drives one.
#[derive(Clone, Debug)]
struct Modern {
    member_id: String,
    epoch: i32,
    owned: BTreeSet<i32>,
}

impl Modern {
    fn new() -> Self {
        Self {
            member_id: Uuid::new_v4().to_string(),
            epoch: 0,
            owned: BTreeSet::new(),
        }
    }

    async fn beat(&mut self, fixture: &Fixture) -> Result<()> {
        let request = ConsumerGroupHeartbeatRequest::default()
            .group_id(GROUP.into())
            .member_id(self.member_id.clone())
            .member_epoch(self.epoch)
            .rebalance_timeout_ms(if self.epoch == 0 { 300_000 } else { -1 })
            .subscribed_topic_names((self.epoch == 0).then(|| vec![TOPIC.into()]))
            .topic_partitions(Some(vec![
                TopicPartitions::default()
                    .topic_id(fixture.topic_id.into_bytes())
                    .partitions(Some(self.owned.iter().copied().collect())),
            ]));

        let Body::ConsumerGroupHeartbeatResponse(response) = fixture
            .coordinator
            .consumer_group_heartbeat(Some("modern"), None, &request)
            .await?
        else {
            return Err(anyhow!("not a heartbeat response"));
        };

        if response.error_code != i16::from(ErrorCode::None) {
            return Err(anyhow!(
                "consumer heartbeat: {:?}",
                ErrorCode::try_from(response.error_code)
            ));
        }

        self.epoch = response.member_epoch;

        if let Some(assignment) = response.assignment {
            self.owned = assignment
                .topic_partitions
                .iter()
                .flatten()
                .flat_map(|topic| topic.partitions.iter().flatten().copied())
                .collect();
        }

        Ok(())
    }

    async fn leave(&self, fixture: &Fixture) -> Result<()> {
        let request = ConsumerGroupHeartbeatRequest::default()
            .group_id(GROUP.into())
            .member_id(self.member_id.clone())
            .member_epoch(-1);

        _ = fixture
            .coordinator
            .consumer_group_heartbeat(Some("modern"), None, &request)
            .await?;

        Ok(())
    }
}

async fn commit<C: Coordinator>(coordinator: &C, member_id: &str, epoch: i32) -> Result<ErrorCode> {
    let topics = [OffsetCommitRequestTopic::default()
        .name(TOPIC.into())
        .partitions(Some(vec![
            OffsetCommitRequestPartition::default()
                .partition_index(0)
                .committed_offset(1)
                .committed_leader_epoch(Some(-1))
                .commit_timestamp(None)
                .committed_metadata(None),
        ]))];

    let Body::OffsetCommitResponse(OffsetCommitResponse { topics, .. }) = coordinator
        .offset_commit(OffsetCommit {
            group_id: GROUP,
            generation_id_or_member_epoch: Some(epoch),
            member_id: Some(member_id),
            group_instance_id: None,
            retention_time_ms: None,
            topics: Some(&topics),
        })
        .await?
    else {
        return Err(anyhow!("not a commit response"));
    };

    ErrorCode::try_from(
        topics
            .unwrap_or_default()
            .first()
            .and_then(|topic| topic.partitions.as_ref()?.first().map(|p| p.error_code))
            .unwrap_or_default(),
    )
    .map_err(Into::into)
}

/// Every classic member forms the classic group together, as consumers
/// started at once do.
async fn form(fixture: &Fixture, classic: &mut [Classic]) -> Result<()> {
    let mut joining = JoinSet::new();

    for (index, member) in classic.iter().cloned().enumerate() {
        let coordinator = fixture.coordinator.clone();

        _ = joining.spawn(async move {
            let mut member = member;
            member.rejoin(&coordinator).await.map(|()| (index, member))
        });
    }

    while let Some(joined) = joining.join_next().await {
        let (index, member) = joined??;
        classic[index] = member;
    }

    Ok(())
}

/// Heartbeat everyone, rejoining the classic members told to, until a round
/// changes nothing and every partition is owned exactly once.
async fn settle(fixture: &Fixture, classic: &mut [Classic], modern: &mut [Modern]) -> Result<()> {
    for _ in 0..ROUNDS {
        let before = (
            classic
                .iter()
                .map(|member| member.owned.clone())
                .collect::<Vec<_>>(),
            modern
                .iter()
                .map(|member| (member.epoch, member.owned.clone()))
                .collect::<Vec<_>>(),
        );

        let mut quiet = true;

        for member in classic.iter_mut() {
            match member.heartbeat(&fixture.coordinator).await? {
                ErrorCode::None => {}

                ErrorCode::RebalanceInProgress
                | ErrorCode::IllegalGeneration
                | ErrorCode::UnknownMemberId => {
                    quiet = false;
                    member.rejoin(&fixture.coordinator).await?;
                }

                otherwise => return Err(anyhow!("classic heartbeat: {otherwise:?}")),
            }
        }

        for member in modern.iter_mut() {
            member.beat(fixture).await?;
        }

        let after = (
            classic
                .iter()
                .map(|member| member.owned.clone())
                .collect::<Vec<_>>(),
            modern
                .iter()
                .map(|member| (member.epoch, member.owned.clone()))
                .collect::<Vec<_>>(),
        );

        if quiet && before == after && owned_once(classic, modern) {
            return Ok(());
        }
    }

    Err(anyhow!(
        "the group did not settle: classic {classic:?}, modern {modern:?}"
    ))
}

/// Whether every partition is owned by exactly one member.
fn owned_once(classic: &[Classic], modern: &[Modern]) -> bool {
    let owned = classic
        .iter()
        .map(|member| &member.owned)
        .chain(modern.iter().map(|member| &member.owned))
        .flatten()
        .copied()
        .collect::<Vec<_>>();

    owned.len() == usize::try_from(PARTITIONS).unwrap_or_default()
        && owned.iter().collect::<BTreeSet<_>>().len() == owned.len()
}

/// The group as `ConsumerGroupDescribe` reports it — its state and each
/// member's type, `0` classic and `1` consumer — on the paused clock the
/// documents were written by: the storage service judges liveness by the wall
/// clock, which is years ahead of it.
async fn described(fixture: &Fixture) -> Result<(String, BTreeMap<String, Option<i8>>)> {
    let (group, _) = fixture
        .storage
        .read_consumer_group(GROUP)
        .await?
        .ok_or_else(|| anyhow!("no consumer group"))?;

    let stamps = fixture.storage.list_consumer_member_stamps(GROUP).await?;
    let now_ms = i64::try_from(paused_clock().duration_since(UNIX_EPOCH)?.as_millis())?;
    let described = group.with_contacts(&stamps).described(GROUP, now_ms);

    Ok((
        described.group_state,
        described
            .members
            .unwrap_or_default()
            .into_iter()
            .map(|member| (member.member_id, member.member_type))
            .collect(),
    ))
}

/// Three classic consumers, restarted one at a time with the consumer
/// protocol: the group converts on the first, keeps every partition owned
/// exactly once at every step, and ends with three consumer-protocol members.
#[tokio::test(start_paused = true)]
async fn a_rolling_restart_moves_a_classic_group_to_the_consumer_protocol() -> Result<()> {
    let fixture = fixture().await?;

    let mut classic = vec![Classic::default(); 3];
    form(&fixture, &mut classic).await?;
    assert!(
        owned_once(&classic, &[]),
        "the classic group formed: {classic:?}"
    );

    let mut modern = Vec::new();

    for step in 0..3 {
        let restarted = classic.remove(0);
        restarted.leave(&fixture.coordinator).await?;

        if !classic.is_empty() {
            settle(&fixture, &mut classic, &mut modern).await?;
        }

        modern.push(Modern::new());
        settle(&fixture, &mut classic, &mut modern).await?;

        let (generation, _) = fixture
            .storage
            .read_group_generation(GROUP)
            .await?
            .ok_or_else(|| anyhow!("no classic generation"))?;
        assert!(generation.is_converted(), "step {step}");

        let (state, members) = described(&fixture).await?;
        assert_eq!("Stable", state, "step {step}");
        assert_eq!(classic.len() + modern.len(), members.len(), "step {step}");

        for member in &classic {
            assert_eq!(
                Some(&Some(0)),
                members.get(&member.member_id),
                "step {step}"
            );
            assert_eq!(
                ErrorCode::None,
                member.commit(&fixture.coordinator).await?,
                "a classic member commits at its generation, step {step}"
            );
        }

        for member in &modern {
            assert_eq!(
                Some(&Some(1)),
                members.get(&member.member_id),
                "step {step}"
            );
            assert_eq!(
                ErrorCode::None,
                commit(&fixture.coordinator, &member.member_id, member.epoch).await?,
                "step {step}"
            );
        }
    }

    assert!(classic.is_empty());
    assert_eq!(3, modern.len());

    Ok(())
}

/// The way back, member by member: a consumer group serves a classic member
/// that joins it, so a rollback needs no conversion either (#626).
#[tokio::test(start_paused = true)]
async fn a_consumer_group_takes_classic_members_back() -> Result<()> {
    let fixture = fixture().await?;

    let mut modern = vec![Modern::new(), Modern::new()];
    let mut classic = Vec::new();
    settle(&fixture, &mut classic, &mut modern).await?;

    for _ in 0..2 {
        modern.remove(0).leave(&fixture).await?;

        let mut member = Classic::default();
        member.rejoin(&fixture.coordinator).await?;
        classic.push(member);

        settle(&fixture, &mut classic, &mut modern).await?;
    }

    assert!(modern.is_empty());

    let (state, members) = described(&fixture).await?;
    assert_eq!("Stable", state);
    assert_eq!(2, members.len());
    assert!(members.values().all(|member_type| *member_type == Some(0)));

    Ok(())
}

/// A conversion whose commit landed and whose `consumer.json` did not — the
/// replica that won the CAS stopped in between — is completed by the next
/// classic request any replica serves, from the snapshot the commit froze.
#[tokio::test(start_paused = true)]
async fn a_conversion_left_half_done_is_completed() -> Result<()> {
    let fixture = fixture().await?;

    let mut classic = vec![Classic::default(); 2];
    form(&fixture, &mut classic).await?;

    let (mut generation, version) = fixture
        .storage
        .read_group_generation(GROUP)
        .await?
        .ok_or_else(|| anyhow!("no classic generation"))?;

    generation.consumer_since_ms = Some(1);
    _ = fixture
        .storage
        .update_group_generation(GROUP, generation.bumped(), Some(version))
        .await
        .map_err(|error| anyhow!("{error:?}"))?;

    assert!(fixture.storage.read_consumer_group(GROUP).await?.is_none());

    assert_eq!(
        ErrorCode::None,
        classic[0].heartbeat(&fixture.coordinator).await?,
        "the snapshot carried the member at its generation"
    );

    let (state, members) = described(&fixture).await?;
    assert_eq!("Stable", state);
    assert_eq!(2, members.len());

    let mut modern = vec![Modern::new()];
    settle(&fixture, &mut classic, &mut modern).await?;

    Ok(())
}
