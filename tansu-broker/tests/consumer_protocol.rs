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

//! The KIP-848 consumer group protocol, `group.protocol=consumer` (#612).
//!
//! Driven through the coordinator and the storage services exactly as the
//! broker drives them, over `memory://`, and once over a real socket for what
//! only the route table can show: that `ConsumerGroupHeartbeat` is advertised.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::Ipv4Addr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use tansu_broker::{
    coordinator::group::{Coordinator, OffsetCommit, administrator::Controller},
    service::services,
};
use tansu_client::{
    BytesConnectionService, ConnectionManager, FrameConnectionLayer, FramePoolLayer, Pool,
};
use tansu_sans_io::{
    ApiKey as _, Body, ConsumerGroupDescribeRequest, ConsumerGroupHeartbeatRequest,
    ConsumerGroupHeartbeatResponse, DeleteGroupsRequest, DescribeGroupsRequest, ErrorCode, Frame,
    Header, ListGroupsRequest, OffsetCommitResponse, OffsetFetchResponse,
    consumer::{ConsumerProtocolSubscription, MemberMetadata},
    consumer_group_heartbeat_request::TopicPartitions,
    create_topics_request::CreatableTopic,
    join_group_request::JoinGroupRequestProtocol,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopics},
};
use tansu_service::{FrameBytesLayer, Peer, TcpContext};
use tansu_storage::{
    ConsumerGroupDescribeService, DeleteGroupsService, DescribeGroupsService, ListGroupsService,
    Storage, StorageContainer,
};
use tokio::{net::TcpListener, time::advance};
use url::Url;
use uuid::Uuid;

use crate::common::paused_clock;

pub mod common;

const TOPIC: &str = "t";
const PARTITIONS: i32 = 4;

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

    let coordinator = Controller::with_storage(storage.clone())?.with_now(paused_clock);

    Ok(Fixture {
        storage,
        coordinator,
        topic_id,
    })
}

/// One member of a consumer-protocol group, as the Java client drives it.
struct Member {
    group_id: String,
    member_id: String,
    epoch: i32,
    owned: BTreeSet<i32>,
}

impl Member {
    fn new(group_id: &str) -> Self {
        Self {
            group_id: group_id.into(),
            member_id: Uuid::new_v4().to_string(),
            epoch: 0,
            owned: BTreeSet::new(),
        }
    }

    fn owned_partitions(&self, topic_id: Uuid) -> Option<Vec<TopicPartitions>> {
        Some(vec![
            TopicPartitions::default()
                .topic_id(topic_id.into_bytes())
                .partitions(Some(self.owned.iter().copied().collect())),
        ])
    }

    /// One heartbeat, adopting whatever epoch and assignment it is answered —
    /// which is to say revoking at once whatever it is no longer assigned.
    async fn beat(&mut self, fixture: &Fixture) -> Result<ConsumerGroupHeartbeatResponse> {
        let request = ConsumerGroupHeartbeatRequest::default()
            .group_id(self.group_id.clone())
            .member_id(self.member_id.clone())
            .member_epoch(self.epoch)
            .rebalance_timeout_ms(if self.epoch == 0 { 300_000 } else { -1 })
            .subscribed_topic_names((self.epoch == 0).then(|| vec![TOPIC.into()]))
            .topic_partitions(self.owned_partitions(fixture.topic_id));

        let Body::ConsumerGroupHeartbeatResponse(response) = fixture
            .coordinator
            .consumer_group_heartbeat(Some("consumer-protocol"), Some("/10.0.0.1"), &request)
            .await?
        else {
            return Err(anyhow!("not a heartbeat response"));
        };

        if response.error_code == i16::from(ErrorCode::None) {
            self.epoch = response.member_epoch;

            if let Some(assignment) = response.assignment.as_ref() {
                self.owned = assignment
                    .topic_partitions
                    .iter()
                    .flatten()
                    .flat_map(|topic| topic.partitions.iter().flatten().copied())
                    .collect();
            }
        }

        Ok(response)
    }

    async fn leave(&mut self, fixture: &Fixture) -> Result<ErrorCode> {
        let request = ConsumerGroupHeartbeatRequest::default()
            .group_id(self.group_id.clone())
            .member_id(self.member_id.clone())
            .member_epoch(-1);

        let Body::ConsumerGroupHeartbeatResponse(response) = fixture
            .coordinator
            .consumer_group_heartbeat(None, None, &request)
            .await?
        else {
            return Err(anyhow!("not a heartbeat response"));
        };

        Ok(ErrorCode::try_from(response.error_code)?)
    }

    async fn commit(&self, fixture: &Fixture, epoch: i32) -> Result<ErrorCode> {
        let topics = [OffsetCommitRequestTopic::default()
            .name(TOPIC.into())
            .partitions(Some(vec![
                OffsetCommitRequestPartition::default()
                    .partition_index(0)
                    .committed_offset(1),
            ]))];

        let Body::OffsetCommitResponse(OffsetCommitResponse { topics, .. }) = fixture
            .coordinator
            .offset_commit(OffsetCommit {
                group_id: &self.group_id,
                generation_id_or_member_epoch: Some(epoch),
                member_id: Some(&self.member_id),
                group_instance_id: None,
                retention_time_ms: None,
                topics: Some(&topics),
            })
            .await?
        else {
            return Err(anyhow!("not an offset commit response"));
        };

        topics
            .into_iter()
            .flatten()
            .flat_map(|topic| topic.partitions.unwrap_or_default())
            .map(|partition| ErrorCode::try_from(partition.error_code).map_err(Into::into))
            .next()
            .ok_or(anyhow!("no partition answered"))?
    }
}

/// Heartbeat every member in turn until a round changes nothing.
async fn converge(fixture: &Fixture, members: &mut [Member]) -> Result<()> {
    for _ in 0..16 {
        let before = members
            .iter()
            .map(|member| (member.epoch, member.owned.clone()))
            .collect::<Vec<_>>();

        for member in members.iter_mut() {
            let response = member.beat(fixture).await?;
            assert_eq!(i16::from(ErrorCode::None), response.error_code);
        }

        if before
            == members
                .iter()
                .map(|member| (member.epoch, member.owned.clone()))
                .collect::<Vec<_>>()
        {
            return Ok(());
        }
    }

    Err(anyhow!("the group did not converge"))
}

fn disjoint_and_complete(members: &[Member]) {
    let mut seen = BTreeMap::new();

    for member in members {
        for partition in &member.owned {
            assert_eq!(
                None,
                seen.insert(*partition, member.member_id.clone()),
                "partition {partition} is owned twice"
            );
        }
    }

    assert_eq!(
        (0..PARTITIONS).collect::<BTreeSet<_>>(),
        seen.keys().copied().collect::<BTreeSet<_>>()
    );
}

#[tokio::test(start_paused = true)]
async fn one_member_joins_and_owns_every_partition() -> Result<()> {
    let fixture = fixture().await?;
    let mut member = Member::new("one");

    let joined = member.beat(&fixture).await?;

    assert_eq!(i16::from(ErrorCode::None), joined.error_code);
    assert_eq!(Some(member.member_id.clone()), joined.member_id);
    assert_eq!(1, joined.member_epoch);
    assert_eq!(5_000, joined.heartbeat_interval_ms);
    assert_eq!((0..PARTITIONS).collect::<BTreeSet<_>>(), member.owned);

    let steady = member.beat(&fixture).await?;
    assert_eq!(1, steady.member_epoch);
    assert_eq!(None, steady.assignment, "nothing changed, nothing is sent");

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_second_member_is_given_what_the_first_revokes() -> Result<()> {
    let fixture = fixture().await?;
    let mut members = [Member::new("two"), Member::new("two")];

    _ = members[0].beat(&fixture).await?;
    assert_eq!(PARTITIONS as usize, members[0].owned.len());

    let joined = members[1].beat(&fixture).await?;
    assert_eq!(2, joined.member_epoch, "the join moved the group epoch");
    assert!(
        members[1].owned.is_empty(),
        "nothing is handed over before the first member revokes it"
    );

    let revoking = members[0].beat(&fixture).await?;
    assert_eq!(
        1, revoking.member_epoch,
        "the epoch waits for the revocation"
    );
    assert_eq!(2, members[0].owned.len());

    let acknowledged = members[0].beat(&fixture).await?;
    assert_eq!(2, acknowledged.member_epoch);

    _ = members[1].beat(&fixture).await?;
    assert_eq!(2, members[1].owned.len());

    disjoint_and_complete(&members);

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_member_that_leaves_hands_its_partitions_on() -> Result<()> {
    let fixture = fixture().await?;
    let mut members = [Member::new("leave"), Member::new("leave")];

    converge(&fixture, &mut members).await?;
    disjoint_and_complete(&members);

    assert_eq!(ErrorCode::None, members[1].leave(&fixture).await?);

    let [first, _] = &mut members;
    let response = first.beat(&fixture).await?;

    assert_eq!(3, response.member_epoch);
    assert_eq!((0..PARTITIONS).collect::<BTreeSet<_>>(), first.owned);

    assert_eq!(
        ErrorCode::UnknownMemberId,
        members[1].leave(&fixture).await?,
        "a member that left is not a member"
    );

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_silent_member_is_fenced_after_its_session() -> Result<()> {
    let fixture = fixture().await?;
    let mut members = [Member::new("session"), Member::new("session")];

    converge(&fixture, &mut members).await?;

    for _ in 0..20 {
        advance(Duration::from_secs(5)).await;
        _ = members[0].beat(&fixture).await?;
    }

    assert_eq!((0..PARTITIONS).collect::<BTreeSet<_>>(), members[0].owned);

    let fenced = members[1].beat(&fixture).await?;
    assert_eq!(i16::from(ErrorCode::UnknownMemberId), fenced.error_code);

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_commit_is_fenced_by_the_member_epoch() -> Result<()> {
    let fixture = fixture().await?;
    let mut member = Member::new("commits");

    _ = member.beat(&fixture).await?;

    assert_eq!(
        ErrorCode::None,
        member.commit(&fixture, member.epoch).await?
    );
    assert_eq!(
        ErrorCode::StaleMemberEpoch,
        member.commit(&fixture, member.epoch - 1).await?
    );
    assert_eq!(
        ErrorCode::UnknownMemberId,
        Member::new("commits").commit(&fixture, 1).await?
    );

    let fetch = |epoch: i32| {
        vec![
            OffsetFetchRequestGroup::default()
                .group_id(member.group_id.clone())
                .member_id(Some(member.member_id.clone()))
                .member_epoch(Some(epoch))
                .topics(Some(vec![
                    OffsetFetchRequestTopics::default()
                        .name(TOPIC.into())
                        .partition_indexes(Some(vec![0])),
                ])),
        ]
    };

    let Body::OffsetFetchResponse(OffsetFetchResponse { groups, .. }) = fixture
        .coordinator
        .offset_fetch(None, None, Some(&fetch(member.epoch)), None)
        .await?
    else {
        return Err(anyhow!("not an offset fetch response"));
    };

    let group = groups.unwrap_or_default().remove(0);
    assert_eq!(i16::from(ErrorCode::None), group.error_code);
    assert_eq!(
        Some(1),
        group
            .topics
            .unwrap_or_default()
            .first()
            .and_then(|topic| topic.partitions.as_ref())
            .and_then(|partitions| partitions.first())
            .map(|partition| partition.committed_offset)
    );

    let Body::OffsetFetchResponse(OffsetFetchResponse { groups, .. }) = fixture
        .coordinator
        .offset_fetch(None, None, Some(&fetch(member.epoch + 1)), None)
        .await?
    else {
        return Err(anyhow!("not an offset fetch response"));
    };

    assert_eq!(
        i16::from(ErrorCode::StaleMemberEpoch),
        groups.unwrap_or_default()[0].error_code
    );

    Ok(())
}

#[tokio::test]
async fn a_consumer_group_is_described_listed_and_kept() -> Result<()> {
    let mut fixture = fixture().await?;

    // On the real clock (#612): the storage services judge liveness by it,
    // and the paused one is years behind.
    fixture.coordinator = Controller::with_storage(fixture.storage.clone())?;

    let mut members = [Member::new("described"), Member::new("described")];

    converge(&fixture, &mut members).await?;

    let storage = fixture.storage.clone();

    let described = MapStateLayer::new({
        let storage = storage.clone();
        move |_| storage
    })
    .into_layer(ConsumerGroupDescribeService)
    .serve(
        Context::default(),
        ConsumerGroupDescribeRequest::default()
            .group_ids(Some(vec!["described".into()]))
            .include_authorized_operations(false),
    )
    .await?;

    let group = described.groups.unwrap_or_default().remove(0);
    assert_eq!(0, group.error_code);
    assert_eq!("Stable", group.group_state);
    assert_eq!("uniform", group.assignor_name);
    assert_eq!(group.group_epoch, group.assignment_epoch);

    let members_described = group.members.unwrap_or_default();
    assert_eq!(2, members_described.len());

    for member in &members_described {
        assert_eq!(group.assignment_epoch, member.member_epoch);
        assert_eq!("consumer-protocol", member.client_id);
        assert_eq!("/10.0.0.1", member.client_host);
        assert_eq!(
            Some(TOPIC.to_owned()),
            member
                .assignment
                .topic_partitions
                .as_ref()
                .and_then(|topics| topics.first())
                .map(|topic| topic.topic_name.clone())
        );
    }

    let listed = MapStateLayer::new({
        let storage = storage.clone();
        move |_| storage
    })
    .into_layer(ListGroupsService)
    .serve(
        Context::default(),
        ListGroupsRequest::default().types_filter(Some(vec!["consumer".into()])),
    )
    .await?
    .groups
    .unwrap_or_default();

    assert_eq!(1, listed.len());
    assert_eq!(Some("consumer"), listed[0].group_type.as_deref());
    assert_eq!(Some("Stable"), listed[0].group_state.as_deref());

    let classic_only = MapStateLayer::new({
        let storage = storage.clone();
        move |_| storage
    })
    .into_layer(ListGroupsService)
    .serve(
        Context::default(),
        ListGroupsRequest::default().types_filter(Some(vec!["classic".into()])),
    )
    .await?
    .groups
    .unwrap_or_default();

    assert!(classic_only.is_empty());

    let classic = MapStateLayer::new({
        let storage = storage.clone();
        move |_| storage
    })
    .into_layer(DescribeGroupsService)
    .serve(
        Context::default(),
        DescribeGroupsRequest::default()
            .groups(Some(vec!["described".into()]))
            .include_authorized_operations(Some(false)),
    )
    .await?;

    assert_eq!(
        i16::from(ErrorCode::GroupIdNotFound),
        classic.groups.unwrap_or_default()[0].error_code,
        "the classic describe sends an AdminClient to ConsumerGroupDescribe"
    );

    let deleted = MapStateLayer::new({
        let storage = storage.clone();
        move |_| storage
    })
    .into_layer(DeleteGroupsService)
    .serve(
        Context::default(),
        DeleteGroupsRequest::default().groups_names(Some(vec!["described".into()])),
    )
    .await?;

    assert_eq!(
        i16::from(ErrorCode::NonEmptyGroup),
        deleted.results.unwrap_or_default()[0].error_code
    );

    Ok(())
}

/// A classic `JoinGroup`, as `register` drives it in the coordinator's own
/// tests: a first join to be handed a member id, a second to be admitted.
async fn classic_join(fixture: &Fixture, group_id: &str) -> Result<i16> {
    let metadata = Bytes::try_from(
        &MemberMetadata::default().version(3).subscription(
            ConsumerProtocolSubscription::default()
                .topics([TOPIC.to_owned()])
                .generation_id(None),
        ),
    )?;

    let protocols = [JoinGroupRequestProtocol::default()
        .name("range".into())
        .metadata(metadata)];

    let mut member_id = String::new();

    for _ in 0..2 {
        let Body::JoinGroupResponse(join) = fixture
            .coordinator
            .join(
                Some("classic"),
                group_id,
                45_000,
                Some(300_000),
                &member_id,
                None,
                "consumer",
                Some(&protocols),
                None,
            )
            .await?
        else {
            return Err(anyhow!("not a join response"));
        };

        if join.error_code != i16::from(ErrorCode::MemberIdRequired) {
            return Ok(join.error_code);
        }

        member_id = join.member_id;
    }

    Err(anyhow!("never admitted"))
}

#[tokio::test(start_paused = true)]
async fn a_group_does_not_mix_protocols() -> Result<()> {
    let fixture = fixture().await?;

    _ = Member::new("consumer-first").beat(&fixture).await?;

    assert_eq!(
        i16::from(ErrorCode::InconsistentGroupProtocol),
        classic_join(&fixture, "consumer-first").await?,
        "a classic member may not join a consumer-protocol group"
    );

    assert_eq!(
        i16::from(ErrorCode::None),
        classic_join(&fixture, "classic-first").await?
    );

    let refused = Member::new("classic-first").beat(&fixture).await?;
    assert_eq!(
        i16::from(ErrorCode::GroupIdNotFound),
        refused.error_code,
        "a consumer-protocol member may not join a classic group"
    );

    Ok(())
}

/// What only the route table can show: the API is advertised, and answered.
#[tokio::test]
async fn consumer_group_heartbeat_is_advertised_and_answered() -> Result<()> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    let fixture = fixture().await?;
    let served = fixture.storage.clone();

    _ = tokio::spawn(async move {
        while let Ok((stream, peer)) = listener.accept().await {
            let Ok(coordinator) = Controller::with_storage(served.clone()) else {
                return;
            };

            let Ok(service) = services(
                TcpContext::default().cluster_id(Some("tansu-612".into())),
                coordinator,
                served.clone(),
                None,
                None,
                None,
            ) else {
                return;
            };

            let mut ctx = Context::default();
            _ = ctx.insert(Peer(peer));

            _ = tokio::spawn(async move {
                _ = service.serve(ctx, stream).await;
            });
        }
    });

    let pool: Pool = ConnectionManager::builder(Url::parse(&format!("tcp://127.0.0.1:{port}"))?)
        .client_id(Some("consumer-protocol".into()))
        .max_size(Some(1))
        .build()
        .await?;

    let api_version = pool
        .manager()
        .api_version(ConsumerGroupHeartbeatRequest::KEY)?;

    let frame = Frame {
        size: 0,
        header: Header::Request {
            api_key: ConsumerGroupHeartbeatRequest::KEY,
            api_version,
            correlation_id: 0,
            client_id: Some("consumer-protocol".into()),
        },
        body: ConsumerGroupHeartbeatRequest::default()
            .group_id("wire".into())
            .member_id(Uuid::new_v4().to_string())
            .member_epoch(0)
            .rebalance_timeout_ms(300_000)
            .subscribed_topic_names(Some(vec![TOPIC.into()]))
            .topic_partitions(Some(vec![]))
            .into(),
    };

    let Body::ConsumerGroupHeartbeatResponse(response) = (
        FramePoolLayer::new(pool),
        FrameConnectionLayer,
        FrameBytesLayer,
    )
        .into_layer(BytesConnectionService)
        .serve(Context::default(), frame)
        .await?
        .body
    else {
        return Err(anyhow!("not a heartbeat response"));
    };

    assert_eq!(i16::from(ErrorCode::None), response.error_code);
    assert_eq!(1, response.member_epoch);
    assert_eq!(
        PARTITIONS as usize,
        response
            .assignment
            .and_then(|assignment| assignment.topic_partitions)
            .unwrap_or_default()
            .iter()
            .map(|topic| topic.partitions.as_ref().map_or(0, Vec::len))
            .sum::<usize>()
    );

    let member = fixture
        .storage
        .read_consumer_group("wire")
        .await?
        .and_then(|(group, _)| group.members.into_values().next())
        .ok_or_else(|| anyhow!("no member"))?;
    assert_eq!(
        "/127.0.0.1", member.client_host,
        "the peer, as Kafka prints it"
    );

    Ok(())
}

async fn create_topic(storage: &Arc<dyn Storage>, name: &str, partitions: i32) -> Result<Uuid> {
    storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await
        .map_err(Into::into)
}

/// Partitions by topic id, as a heartbeat assigned them.
fn assigned_by_topic(response: &ConsumerGroupHeartbeatResponse) -> BTreeMap<Uuid, Vec<i32>> {
    response
        .assignment
        .iter()
        .flat_map(|assignment| assignment.topic_partitions.iter().flatten())
        .map(|topic| {
            (
                Uuid::from_bytes(topic.topic_id),
                topic.partitions.clone().unwrap_or_default(),
            )
        })
        .collect()
}

/// A pattern subscription (#625) takes in every matching topic, including one
/// created after the member joined, and leaves the others alone.
#[tokio::test(start_paused = true)]
async fn a_pattern_subscription_follows_the_topics_it_matches() -> Result<()> {
    let mut fixture = fixture().await?;
    let eu = create_topic(&fixture.storage, "orders-eu", 2).await?;
    let member_id = Uuid::new_v4().to_string();

    let Body::ConsumerGroupHeartbeatResponse(joined) = fixture
        .coordinator
        .consumer_group_heartbeat(
            None,
            None,
            &ConsumerGroupHeartbeatRequest::default()
                .group_id("pattern".into())
                .member_id(member_id.clone())
                .member_epoch(0)
                .rebalance_timeout_ms(300_000)
                .subscribed_topic_names(Some(vec![]))
                .subscribed_topic_regex(Some("orders-.*".into()))
                .topic_partitions(Some(vec![])),
        )
        .await?
    else {
        return Err(anyhow!("not a heartbeat response"));
    };

    assert_eq!(i16::from(ErrorCode::None), joined.error_code);
    assert_eq!(
        BTreeMap::from([(eu, vec![0, 1])]),
        assigned_by_topic(&joined),
        "{TOPIC} does not match"
    );

    let us = create_topic(&fixture.storage, "orders-us", 3).await?;

    // A replica that has not resolved the group yet, as any other replica
    // would be, or this one once its refresh window has passed.
    fixture.coordinator = Controller::with_storage(fixture.storage.clone())?.with_now(paused_clock);

    let Body::ConsumerGroupHeartbeatResponse(refreshed) = fixture
        .coordinator
        .consumer_group_heartbeat(
            None,
            None,
            &ConsumerGroupHeartbeatRequest::default()
                .group_id("pattern".into())
                .member_id(member_id.clone())
                .member_epoch(joined.member_epoch)
                .topic_partitions(Some(vec![
                    TopicPartitions::default()
                        .topic_id(eu.into_bytes())
                        .partitions(Some(vec![0, 1])),
                ])),
        )
        .await?
    else {
        return Err(anyhow!("not a heartbeat response"));
    };

    assert_eq!(i16::from(ErrorCode::None), refreshed.error_code);
    assert_eq!(joined.member_epoch + 1, refreshed.member_epoch);
    assert_eq!(
        BTreeMap::from([(eu, vec![0, 1]), (us, vec![0, 1, 2])]),
        assigned_by_topic(&refreshed)
    );

    let Body::ConsumerGroupHeartbeatResponse(refused) = fixture
        .coordinator
        .consumer_group_heartbeat(
            None,
            None,
            &ConsumerGroupHeartbeatRequest::default()
                .group_id("pattern".into())
                .member_id(member_id)
                .member_epoch(refreshed.member_epoch)
                .subscribed_topic_regex(Some("orders-(".into())),
        )
        .await?
    else {
        return Err(anyhow!("not a heartbeat response"));
    };

    assert_eq!(
        i16::from(ErrorCode::InvalidRegularExpression),
        refused.error_code
    );

    Ok(())
}
