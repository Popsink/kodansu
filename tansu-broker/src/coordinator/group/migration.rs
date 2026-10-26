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

//! A classic group converted to a consumer group, and the classic members a
//! consumer group serves (#626).
//!
//! The way to adopt KIP-848 is a rolling restart: members flip to
//! `group.protocol=consumer` one at a time while the group keeps consuming. So
//! the first consumer-protocol member to join a classic group converts it
//! ([`from_classic`]), and every classic member carries on inside the consumer
//! group — served `JoinGroup` ([`join`]), `SyncGroup` ([`sync`]), `Heartbeat`
//! ([`heartbeat`]) and `LeaveGroup` ([`leave`]) from the consumer group's
//! state, assigned by its server-side assignor, until it comes back speaking
//! the consumer protocol. This follows Kafka 4.x's `GroupMetadataManager`:
//! `convertToConsumerGroup`, `classicGroupJoinToConsumerGroup` and its
//! siblings.
//!
//! A classic member cannot be told to revoke part of its assignment in a
//! heartbeat. It is told `REBALANCE_IN_PROGRESS` instead, rejoins reporting
//! what it owns, and its join does the reconciliation a consumer-protocol
//! heartbeat would: a partition is still handed on only once its owner has
//! given it up.
//!
//! Like [`super::consumer::apply`], everything here is a pure function of the group
//! as read, the request and the clock; the coordinator writes the result back
//! under the group document's etag.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use tansu_sans_io::{
    Body, ErrorCode, HeartbeatResponse, JoinGroupResponse, LeaveGroupResponse, SyncGroupResponse,
    consumer::{
        CONSUMER, ConsumerProtocolAssignment, MemberAssignment, MemberMetadata, TopicPartition,
    },
    join_group_request::JoinGroupRequestProtocol,
    leave_group_response::MemberResponse,
};
use tansu_storage::{
    AssignmentDoc, ClassicMember, ConsumerGroupDoc, ConsumerMember, GenerationDoc, MemberDoc,
    MemberState, Partitions, SubscribedTopic, flatten, grouped,
};
use uuid::Uuid;

use super::consumer::{
    Client, ConsumerGroupConfig, Liveness, Resolved, Transition, owned_by_others, reconcile,
    refresh_topics, retarget, sweep,
};

/// The newest version of the embedded consumer protocol's assignment, which a
/// member is answered at, or at the version of its subscription when older.
const ASSIGNMENT_VERSION: i16 = 3;

/// A classic `JoinGroup`, as a consumer group serves it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ClassicJoin<'a> {
    pub(crate) member_id: &'a str,
    pub(crate) instance_id: Option<&'a str>,
    pub(crate) session_timeout_ms: i32,
    pub(crate) rebalance_timeout_ms: Option<i32>,
    pub(crate) protocol_type: &'a str,
    pub(crate) protocols: &'a [JoinGroupRequestProtocol],
}

/// The topics a classic member's subscription names, or none when it does not
/// parse: what has to be resolved before [`from_classic`] or [`join`].
pub(crate) fn subscribed_topics(metadata: &Bytes) -> Vec<String> {
    MemberMetadata::try_from(metadata.clone())
        .map(|metadata| metadata.subscription.topics)
        .unwrap_or_default()
}

/// Topic-name partitions as ids, dropping topics that do not resolve.
fn by_id(partitions: &[TopicPartition], names: &BTreeMap<&str, Uuid>) -> Partitions {
    grouped(partitions.iter().flat_map(|topic| {
        names
            .get(topic.topic.as_str())
            .copied()
            .into_iter()
            .flat_map(move |topic_id| {
                topic
                    .partitions
                    .iter()
                    .map(move |partition| (topic_id, *partition))
            })
    }))
}

fn names_of(group: &ConsumerGroupDoc) -> BTreeMap<&str, Uuid> {
    group
        .topics
        .iter()
        .map(|(topic_id, topic)| (topic.name.as_str(), *topic_id))
        .collect()
}

fn sorted(topics: Vec<String>) -> Vec<String> {
    topics
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// The consumer group a classic group becomes (#626), Kafka's
/// `ConsumerGroup.fromClassicGroup`.
///
/// Every classic member is kept, at the classic generation as its epoch, with
/// what the generation's assignment gave it as both its current and target
/// assignment, so the conversion itself moves no partition. A member whose
/// document is gone or whose session has lapsed is left out, as the classic
/// sweep would have left it. A group mid-rebalance has no assignment for its
/// generation, and its members start with nothing — they are asked to rejoin
/// anyway, by the epoch the joining member is about to bump.
///
/// An assignment carrying user data is refused, as Kafka refuses it: a custom
/// assignor's user data has nowhere to go on the server side.
pub(crate) fn from_classic(
    generation: &GenerationDoc,
    members: &BTreeMap<String, MemberDoc>,
    assignment: Option<&AssignmentDoc>,
    resolved: &Resolved,
    held: Option<&ConsumerGroupDoc>,
    now_ms: i64,
) -> Result<ConsumerGroupDoc, ErrorCode> {
    let epoch = generation
        .generation_id
        .max(held.map_or(0, |held| held.group_epoch));

    let mut group = ConsumerGroupDoc {
        seq: held.map_or(0, |held| held.seq),
        group_epoch: epoch,
        assignment_epoch: epoch,
        created_ms: held.map_or(now_ms, |held| held.created_ms),
        classic_generation: Some(generation.generation_id),
        rest: held.map(|held| held.rest.clone()).unwrap_or_default(),
        ..Default::default()
    };

    group.topics = resolved
        .iter()
        .map(|(name, (topic_id, partitions))| {
            (
                *topic_id,
                SubscribedTopic {
                    name: name.clone(),
                    partitions: *partitions,
                },
            )
        })
        .collect();

    let names = names_of(&group);

    let assignment =
        assignment.filter(|assignment| assignment.generation_id == generation.generation_id);

    let protocol_name = generation.protocol_name.clone().unwrap_or_default();

    let mut converted = BTreeMap::new();

    for (member_id, member_ref) in &generation.members {
        let Some(member) = members
            .get(member_id)
            .filter(|member| !member.is_expired(now_ms))
        else {
            continue;
        };

        let metadata = MemberMetadata::try_from(member.join_response.metadata.clone())
            .map_err(|_| ErrorCode::GroupIdNotFound)?;

        let assigned = match assignment
            .and_then(|assignment| assignment.assignments.get(member_id))
            .filter(|assigned| !assigned.is_empty())
        {
            Some(assigned) => {
                let decoded = MemberAssignment::try_from(assigned.clone())
                    .map_err(|_| ErrorCode::GroupIdNotFound)?;

                if decoded
                    .assignment
                    .user_data
                    .as_ref()
                    .is_some_and(|user_data| !user_data.is_empty())
                {
                    return Err(ErrorCode::GroupIdNotFound);
                }

                by_id(&decoded.assignment.assigned_partitions, &names)
            }

            None => Partitions::new(),
        };

        _ = converted.insert(
            member_id.clone(),
            ConsumerMember {
                member_epoch: generation.generation_id,
                previous_member_epoch: generation.generation_id,
                state: MemberState::Stable,
                instance_id: member_ref.group_instance_id.clone(),
                rack_id: metadata.subscription.rack_id.clone(),
                subscribed_topic_names: sorted(metadata.subscription.topics.clone()),
                rebalance_timeout_ms: member
                    .rebalance_timeout_ms
                    .unwrap_or(member.session_timeout_ms),
                session_timeout_ms: member.session_timeout_ms,
                last_contact_ms: member.last_contact_ms,
                assigned: assigned.clone(),
                target: assigned,
                classic: Some(ClassicMember {
                    session_timeout_ms: member.session_timeout_ms,
                    protocol_name: protocol_name.clone(),
                    protocols: vec![protocol_name.clone()],
                    metadata: member.join_response.metadata.clone(),
                }),
                ..Default::default()
            },
        );
    }

    group.members = converted;
    Ok(group)
}

/// The member a classic request names: by its instance when it gives one, as
/// Kafka's `validateConsumerGroupMember` finds it.
fn named<'a>(
    group: &'a ConsumerGroupDoc,
    member_id: &str,
    instance_id: Option<&str>,
) -> Result<(&'a String, &'a ConsumerMember), ErrorCode> {
    match instance_id {
        Some(instance_id) => {
            let (id, member) = group
                .members
                .iter()
                .find(|(_, member)| member.instance_id.as_deref() == Some(instance_id))
                .ok_or(ErrorCode::UnknownMemberId)?;

            if id != member_id {
                return Err(ErrorCode::FencedInstanceId);
            }

            Ok((id, member))
        }

        None => group
            .members
            .get_key_value(member_id)
            .ok_or(ErrorCode::UnknownMemberId),
    }
}

/// The protocol a joining classic member is answered with: the first it
/// offers that every other classic member supports, Kafka's
/// `supportsClassicProtocols`.
fn agreed_protocol<'a>(
    group: &ConsumerGroupDoc,
    member_id: &str,
    protocols: &'a [JoinGroupRequestProtocol],
) -> Option<&'a JoinGroupRequestProtocol> {
    protocols.iter().find(|protocol| {
        group
            .members
            .iter()
            .filter(|(other, _)| *other != member_id)
            .filter_map(|(_, member)| member.classic.as_ref())
            .all(|classic| classic.protocols.contains(&protocol.name))
    })
}

/// Whether a member at the target epoch is still waiting for a partition
/// another member has not given up, Kafka's `waitingOnUnreleasedPartition`.
fn waiting_on_unreleased(
    group: &ConsumerGroupDoc,
    member_id: &str,
    member: &ConsumerMember,
) -> bool {
    let taken = owned_by_others(group, member_id);

    flatten(&member.target)
        .difference(&flatten(&member.assigned))
        .any(|partition| taken.contains(partition))
}

/// Settle a transition: stamp the member's contact when the group is written
/// anyway, and say whether its liveness is otherwise due a renewal.
fn settled<R>(
    before: &ConsumerGroupDoc,
    mut group: ConsumerGroupDoc,
    member_id: &str,
    liveness: &Liveness,
    now_ms: i64,
    response: R,
) -> Transition<R> {
    let changed = group != *before;

    let renew = match group.members.get_mut(member_id) {
        Some(member) if changed => {
            member.last_contact_ms = now_ms;
            false
        }

        Some(member) => {
            now_ms.saturating_sub(liveness.last_contact_ms(member_id, member))
                >= i64::from(member.session_timeout_ms / 2)
        }

        None => false,
    };

    Transition {
        group,
        changed,
        renew,
        member_id: member_id.to_owned(),
        response,
    }
}

/// A classic `JoinGroup` against a consumer group, Kafka's
/// `classicGroupJoinToConsumerGroup`.
///
/// Answered at once rather than after a join window: the consumer group's
/// assignment is the server's, so there is no leader to wait for, and the
/// member is told the epoch it has reached, which is the generation its
/// `SyncGroup` and `Heartbeat` must then carry.
pub(crate) fn join(
    held: Option<&ConsumerGroupDoc>,
    request: &ClassicJoin<'_>,
    client: Client<'_>,
    resolved: Option<&Resolved>,
    liveness: &Liveness,
    config: &ConsumerGroupConfig,
    now_ms: i64,
) -> Result<Transition<Body>, ErrorCode> {
    let before = held.cloned().unwrap_or_else(|| ConsumerGroupDoc {
        created_ms: now_ms,
        ..Default::default()
    });

    if request.protocol_type != CONSUMER {
        return Err(ErrorCode::InconsistentGroupProtocol);
    }

    let mut group = before.clone();
    let mut rebalance = sweep(&mut group, liveness, now_ms);

    let static_id = request.instance_id.and_then(|instance_id| {
        group
            .members
            .iter()
            .find(|(_, member)| member.instance_id.as_deref() == Some(instance_id))
            .map(|(member_id, _)| member_id.clone())
    });

    let member_id = match static_id {
        Some(held_id) if request.member_id.is_empty() || request.member_id == held_id => held_id,
        Some(_) => return Err(ErrorCode::FencedInstanceId),
        None if !request.member_id.is_empty() => request.member_id.to_owned(),
        None if request.instance_id.is_some() => Uuid::new_v4().to_string(),

        None => {
            let minted = if client.id.is_empty() {
                Uuid::new_v4().to_string()
            } else {
                format!("{}-{}", client.id, Uuid::new_v4())
            };

            let response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::MemberIdRequired.into())
                .generation_id(-1)
                .protocol_type(Some(CONSUMER.into()))
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(Some(false))
                .member_id(minted)
                .members(Some([].into()))
                .into();

            return Ok(Transition {
                group: before,
                changed: false,
                renew: false,
                member_id: String::new(),
                response,
            });
        }
    };

    let protocol = agreed_protocol(&group, &member_id, request.protocols)
        .ok_or(ErrorCode::InconsistentGroupProtocol)?;

    let subscription = MemberMetadata::try_from(protocol.metadata.clone())
        .map_err(|_| ErrorCode::InconsistentGroupProtocol)?;

    if !group.members.contains_key(&member_id) {
        if config
            .max_size
            .is_some_and(|max_size| group.members.len() >= max_size)
        {
            return Err(ErrorCode::GroupMaxSizeReached);
        }

        _ = group.members.insert(
            member_id.clone(),
            ConsumerMember {
                member_epoch: 0,
                previous_member_epoch: -1,
                ..Default::default()
            },
        );

        rebalance = true;
    }

    if let Some(resolved) = resolved {
        rebalance |= refresh_topics(&mut group, resolved);
    }

    let topics = sorted(subscription.subscription.topics.clone());

    if let Some(member) = group.members.get_mut(&member_id) {
        if member.subscribed_topic_names != topics {
            member.subscribed_topic_names = topics;
            rebalance = true;
        }

        if member.server_assignor.take().is_some() {
            rebalance = true;
        }

        member.instance_id = request.instance_id.map(str::to_owned);
        member.rack_id = subscription.subscription.rack_id.clone();
        member.rebalance_timeout_ms = request
            .rebalance_timeout_ms
            .unwrap_or(request.session_timeout_ms);
        member.session_timeout_ms = request.session_timeout_ms;
        client.id.clone_into(&mut member.client_id);

        if !client.host.is_empty() {
            client.host.clone_into(&mut member.client_host);
        }

        member.classic = Some(ClassicMember {
            session_timeout_ms: request.session_timeout_ms,
            protocol_name: protocol.name.clone(),
            protocols: request
                .protocols
                .iter()
                .map(|protocol| protocol.name.clone())
                .collect(),
            metadata: protocol.metadata.clone(),
        });
    }

    if rebalance {
        retarget(&mut group);
    }

    let owned = subscription
        .subscription
        .owned_partitions
        .as_deref()
        .map(|owned| by_id(owned, &names_of(&group)));

    reconcile(&mut group, &member_id, owned.as_ref(), now_ms);

    let member_epoch = group
        .members
        .get(&member_id)
        .map(|member| member.member_epoch)
        .ok_or(ErrorCode::UnknownMemberId)?;

    let response = JoinGroupResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(ErrorCode::None.into())
        .generation_id(member_epoch)
        .protocol_type(Some(CONSUMER.into()))
        .protocol_name(Some(protocol.name.clone()))
        .leader("".into())
        .skip_assignment(Some(false))
        .member_id(member_id.clone())
        .members(Some([].into()))
        .into();

    Ok(settled(
        &before, group, &member_id, liveness, now_ms, response,
    ))
}

/// What a classic member owns, encoded as its own client decodes an
/// assignment.
fn encoded_assignment(
    group: &ConsumerGroupDoc,
    member: &ConsumerMember,
) -> Result<Bytes, ErrorCode> {
    let version = member
        .classic
        .as_ref()
        .and_then(|classic| MemberMetadata::try_from(classic.metadata.clone()).ok())
        .map_or(0, |metadata| metadata.version)
        .clamp(0, ASSIGNMENT_VERSION);

    let assigned_partitions = member
        .assigned
        .iter()
        .filter_map(|(topic_id, partitions)| {
            group.topics.get(topic_id).map(|topic| TopicPartition {
                topic: topic.name.clone(),
                partitions: partitions.iter().copied().collect(),
            })
        })
        .collect::<Vec<_>>();

    Bytes::try_from(&MemberAssignment {
        version,
        assignment: ConsumerProtocolAssignment {
            assigned_partitions,
            user_data: None,
        },
    })
    .map_err(|_| ErrorCode::UnknownServerError)
}

/// The classic member a request names, checked as Kafka checks it before a
/// `SyncGroup` or a `Heartbeat`: known, speaking the classic protocol, and at
/// its own epoch.
fn validated<'a>(
    group: &'a ConsumerGroupDoc,
    member_id: &str,
    instance_id: Option<&str>,
    generation_id: i32,
) -> Result<&'a ConsumerMember, ErrorCode> {
    let (_, member) = named(group, member_id, instance_id)?;

    if member.classic.is_none() {
        return Err(ErrorCode::UnknownMemberId);
    }

    if member.member_epoch != generation_id {
        return Err(ErrorCode::IllegalGeneration);
    }

    Ok(member)
}

/// A classic `SyncGroup` against a consumer group, Kafka's
/// `classicGroupSyncToConsumerGroup`: the member's current assignment, unless
/// the group has moved past its epoch and it is not in the middle of revoking.
pub(crate) fn sync(
    held: Option<&ConsumerGroupDoc>,
    member_id: &str,
    instance_id: Option<&str>,
    generation_id: i32,
    protocol: (Option<&str>, Option<&str>),
    liveness: &Liveness,
    now_ms: i64,
) -> Result<Transition<Body>, ErrorCode> {
    let before = held.cloned().ok_or(ErrorCode::UnknownMemberId)?;
    let mut group = before.clone();

    if sweep(&mut group, liveness, now_ms) {
        retarget(&mut group);
    }

    let member = validated(&group, member_id, instance_id, generation_id)?;
    let classic = member.classic.as_ref().ok_or(ErrorCode::UnknownMemberId)?;

    let (protocol_type, protocol_name) = protocol;

    if protocol_type.is_some_and(|protocol_type| protocol_type != CONSUMER)
        || protocol_name.is_some_and(|protocol_name| protocol_name != classic.protocol_name)
    {
        return Err(ErrorCode::InconsistentGroupProtocol);
    }

    if group.group_epoch > member.member_epoch && member.state != MemberState::UnrevokedPartitions {
        return Err(ErrorCode::RebalanceInProgress);
    }

    let response = SyncGroupResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(ErrorCode::None.into())
        .protocol_type(Some(CONSUMER.into()))
        .protocol_name(Some(classic.protocol_name.clone()))
        .assignment(encoded_assignment(&group, member)?)
        .into();

    let member_id = member_id.to_owned();
    Ok(settled(
        &before, group, &member_id, liveness, now_ms, response,
    ))
}

/// A classic `Heartbeat` against a consumer group, Kafka's
/// `classicGroupHeartbeatToConsumerGroup`.
///
/// `REBALANCE_IN_PROGRESS` asks the member to rejoin: when the group has moved
/// past its epoch, when it has partitions to revoke, or when the partitions it
/// is waiting for have all been released. Liveness is renewed either way.
pub(crate) fn heartbeat(
    held: Option<&ConsumerGroupDoc>,
    member_id: &str,
    instance_id: Option<&str>,
    generation_id: i32,
    resolved: Option<&Resolved>,
    liveness: &Liveness,
    now_ms: i64,
) -> Result<Transition<Body>, ErrorCode> {
    let before = held.cloned().ok_or(ErrorCode::UnknownMemberId)?;
    let mut group = before.clone();

    let mut rebalance = sweep(&mut group, liveness, now_ms);

    if let Some(resolved) = resolved {
        rebalance |= refresh_topics(&mut group, resolved);
    }

    if rebalance {
        retarget(&mut group);
    }

    let member = validated(&group, member_id, instance_id, generation_id)?;

    let rejoin = member.member_epoch < group.group_epoch
        || member.state == MemberState::UnrevokedPartitions
        || (member.state == MemberState::UnreleasedPartitions
            && !waiting_on_unreleased(&group, member_id, member));

    let response = HeartbeatResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(
            if rejoin {
                ErrorCode::RebalanceInProgress
            } else {
                ErrorCode::None
            }
            .into(),
        )
        .into();

    let member_id = member_id.to_owned();
    Ok(settled(
        &before, group, &member_id, liveness, now_ms, response,
    ))
}

/// A classic `LeaveGroup` against a consumer group, Kafka's
/// `classicGroupLeaveToConsumerGroup`: each identity answered as sent, the
/// ones the group knows removed, and the rest assigned again.
pub(crate) fn leave(
    held: Option<&ConsumerGroupDoc>,
    departing: &[(String, Option<String>)],
    liveness: &Liveness,
    now_ms: i64,
) -> Result<Transition<Body>, ErrorCode> {
    let before = held.cloned().ok_or(ErrorCode::UnknownMemberId)?;
    let mut group = before.clone();
    let mut rebalance = sweep(&mut group, liveness, now_ms);

    let responses = departing
        .iter()
        .map(|(member_id, instance_id)| {
            let found = match instance_id.as_deref() {
                Some(instance_id) => group
                    .members
                    .iter()
                    .find(|(_, member)| member.instance_id.as_deref() == Some(instance_id))
                    .map(|(id, _)| id.clone())
                    .ok_or(ErrorCode::UnknownMemberId)
                    .and_then(|id| {
                        if member_id.is_empty() || *member_id == id {
                            Ok(id)
                        } else {
                            Err(ErrorCode::FencedInstanceId)
                        }
                    }),

                None => group
                    .members
                    .contains_key(member_id)
                    .then(|| member_id.clone())
                    .ok_or(ErrorCode::UnknownMemberId),
            };

            let error_code = match found {
                Ok(id) => {
                    _ = group.members.remove(&id);
                    rebalance = true;
                    ErrorCode::None
                }

                Err(error_code) => error_code,
            };

            MemberResponse::default()
                .member_id(member_id.clone())
                .group_instance_id(instance_id.clone())
                .error_code(error_code.into())
        })
        .collect::<Vec<_>>();

    if rebalance {
        retarget(&mut group);
    }

    let changed = group != before;

    Ok(Transition {
        group,
        changed,
        renew: false,
        member_id: String::new(),
        response: LeaveGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .members(Some(responses))
            .into(),
    })
}

#[cfg(test)]
mod tests {
    use tansu_sans_io::consumer::ConsumerProtocolSubscription;
    use tansu_sans_io::join_group_response::JoinGroupResponseMember;
    use tansu_storage::MemberRef;

    use super::*;

    const T: Uuid = Uuid::from_u128(7);
    const GENERATION: i32 = 4;

    fn resolved() -> Resolved {
        BTreeMap::from([("t".to_owned(), (T, 4))])
    }

    fn metadata(owned: &[i32]) -> Bytes {
        Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .topics(["t".to_owned()])
                    .owned_partitions(
                        [TopicPartition::default()
                            .topic("t")
                            .partitions(owned.iter().copied())]
                        .into_iter(),
                    ),
            ),
        )
        .expect("metadata")
    }

    fn encoded(partitions: &[i32], user_data: Option<Bytes>) -> Bytes {
        Bytes::try_from(&MemberAssignment {
            version: 3,
            assignment: ConsumerProtocolAssignment {
                assigned_partitions: vec![TopicPartition {
                    topic: "t".into(),
                    partitions: partitions.to_vec(),
                }],
                user_data,
            },
        })
        .expect("assignment")
    }

    fn member_doc(member_id: &str, last_contact_ms: i64) -> MemberDoc {
        MemberDoc {
            last_contact_ms,
            session_timeout_ms: 45_000,
            join_response: JoinGroupResponseMember::default()
                .member_id(member_id.into())
                .metadata(metadata(&[])),
            ..Default::default()
        }
    }

    /// A classic group of `a` and `b` at [`GENERATION`], `a` holding 0 and 1,
    /// `b` 2 and 3.
    fn classic(
        user_data: Option<Bytes>,
    ) -> (GenerationDoc, BTreeMap<String, MemberDoc>, AssignmentDoc) {
        let generation = GenerationDoc {
            generation_id: GENERATION,
            protocol_type: Some(CONSUMER.into()),
            protocol_name: Some("range".into()),
            leader: Some("a".into()),
            members: BTreeMap::from([
                ("a".to_owned(), MemberRef::default()),
                ("b".to_owned(), MemberRef::default()),
            ]),
            ..Default::default()
        };

        let members = BTreeMap::from([
            ("a".to_owned(), member_doc("a", 0)),
            ("b".to_owned(), member_doc("b", 0)),
        ]);

        let assignment = AssignmentDoc {
            generation_id: GENERATION,
            leader: "a".into(),
            protocol_type: CONSUMER.into(),
            protocol_name: "range".into(),
            assignments: BTreeMap::from([
                ("a".to_owned(), encoded(&[0, 1], user_data.clone())),
                ("b".to_owned(), encoded(&[2, 3], None)),
            ]),
            assigned_at_ms: 0,
        };

        (generation, members, assignment)
    }

    fn listed() -> Liveness {
        Liveness {
            contacts: BTreeMap::new(),
            listed: true,
        }
    }

    fn converted() -> ConsumerGroupDoc {
        let (generation, members, assignment) = classic(None);
        from_classic(
            &generation,
            &members,
            Some(&assignment),
            &resolved(),
            None,
            1_000,
        )
        .expect("converted")
    }

    const CLIENT: Client<'static> = Client { id: "c", host: "" };

    fn rejoining<'a>(
        member_id: &'a str,
        protocols: &'a [JoinGroupRequestProtocol],
    ) -> ClassicJoin<'a> {
        ClassicJoin {
            member_id,
            instance_id: None,
            session_timeout_ms: 45_000,
            rebalance_timeout_ms: Some(60_000),
            protocol_type: CONSUMER,
            protocols,
        }
    }

    #[test]
    fn a_classic_group_converts_without_moving_a_partition() {
        let group = converted();

        assert_eq!(GENERATION, group.group_epoch);
        assert_eq!(GENERATION, group.assignment_epoch);
        assert_eq!(Some(GENERATION), group.classic_generation);

        let a = &group.members["a"];
        assert_eq!(GENERATION, a.member_epoch);
        assert_eq!(grouped([(T, 0), (T, 1)]), a.assigned);
        assert_eq!(a.assigned, a.target);
        assert_eq!(
            Some("range"),
            a.classic.as_ref().map(|c| c.protocol_name.as_str())
        );
        assert_eq!(vec!["t".to_owned()], a.subscribed_topic_names);

        assert_eq!(grouped([(T, 2), (T, 3)]), group.members["b"].assigned);
    }

    #[test]
    fn a_lapsed_classic_member_is_not_carried_over() {
        let (generation, mut members, assignment) = classic(None);
        _ = members.insert("b".into(), member_doc("b", -1_000_000));

        let group = from_classic(
            &generation,
            &members,
            Some(&assignment),
            &resolved(),
            None,
            1_000,
        )
        .expect("converted");

        assert!(group.members.contains_key("a"));
        assert!(!group.members.contains_key("b"));
    }

    #[test]
    fn an_assignors_user_data_refuses_the_conversion() {
        let (generation, members, assignment) = classic(Some(Bytes::from_static(b"sticky")));

        assert_eq!(
            Err(ErrorCode::GroupIdNotFound),
            from_classic(
                &generation,
                &members,
                Some(&assignment),
                &resolved(),
                None,
                1_000
            )
        );
    }

    #[test]
    fn a_classic_member_is_told_to_rejoin_once_the_group_moves() -> Result<(), ErrorCode> {
        let mut group = converted();

        let steady = heartbeat(Some(&group), "a", None, GENERATION, None, &listed(), 1_000)?;
        assert_eq!(
            Body::from(
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(0)
            ),
            steady.response
        );

        assert_eq!(
            Some(ErrorCode::IllegalGeneration),
            heartbeat(
                Some(&group),
                "a",
                None,
                GENERATION - 1,
                None,
                &listed(),
                1_000
            )
            .err()
        );

        group.group_epoch += 1;

        let moved = heartbeat(Some(&group), "a", None, GENERATION, None, &listed(), 1_000)?;
        assert_eq!(
            Body::from(
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::RebalanceInProgress.into())
            ),
            moved.response
        );

        assert_eq!(
            Some(ErrorCode::RebalanceInProgress),
            sync(
                Some(&group),
                "a",
                None,
                GENERATION,
                (None, None),
                &listed(),
                1_000
            )
            .err(),
            "a member behind the group syncs only after it rejoins"
        );

        Ok(())
    }

    #[test]
    fn a_classic_member_revokes_before_its_partitions_move() -> Result<(), ErrorCode> {
        let owning = |owned: &[i32]| {
            vec![
                JoinGroupRequestProtocol::default()
                    .name("range".into())
                    .metadata(metadata(owned)),
            ]
        };

        let mut group = converted();
        _ = group.members.remove("b");
        retarget(&mut group);

        if let Some(a) = group.members.get_mut("a") {
            a.assigned = grouped([(T, 0), (T, 1), (T, 2), (T, 3)]);
            a.member_epoch = group.assignment_epoch;
        }

        _ = group.members.insert(
            "n".into(),
            ConsumerMember {
                subscribed_topic_names: vec!["t".into()],
                session_timeout_ms: 45_000,
                last_contact_ms: 1_000,
                ..Default::default()
            },
        );
        retarget(&mut group);

        let everything = owning(&[0, 1, 2, 3]);
        let still_owning = join(
            Some(&group),
            &rejoining("a", &everything),
            CLIENT,
            None,
            &listed(),
            &ConsumerGroupConfig::default(),
            1_000,
        )?;

        let a = &still_owning.group.members["a"];
        assert_eq!(MemberState::UnrevokedPartitions, a.state);
        assert!(!a.pending_revocation.is_empty());
        assert!(
            a.member_epoch < still_owning.group.group_epoch,
            "no epoch until revoked"
        );

        let Body::SyncGroupResponse(synced) = sync(
            Some(&still_owning.group),
            "a",
            None,
            a.member_epoch,
            (Some(CONSUMER), Some("range")),
            &listed(),
            1_000,
        )?
        .response
        else {
            panic!("not a sync response");
        };

        let kept = flatten(&a.assigned)
            .into_iter()
            .map(|(_, partition)| partition)
            .collect::<Vec<_>>();

        assert_eq!(
            Some(kept.clone()),
            MemberAssignment::try_from(synced.assignment)
                .ok()
                .and_then(|assigned| assigned.assignment.assigned_partitions.into_iter().next())
                .map(|assigned| assigned.partitions),
            "the sync hands back only what it keeps"
        );

        let revoked = owning(&kept);
        let rejoined = join(
            Some(&still_owning.group),
            &rejoining("a", &revoked),
            CLIENT,
            None,
            &listed(),
            &ConsumerGroupConfig::default(),
            1_000,
        )?;

        let a = &rejoined.group.members["a"];
        assert_eq!(MemberState::Stable, a.state);
        assert_eq!(rejoined.group.assignment_epoch, a.member_epoch);
        assert!(a.pending_revocation.is_empty());

        Ok(())
    }

    #[test]
    fn a_classic_member_must_share_a_protocol() {
        static OTHER: std::sync::LazyLock<Vec<JoinGroupRequestProtocol>> =
            std::sync::LazyLock::new(|| {
                vec![
                    JoinGroupRequestProtocol::default()
                        .name("roundrobin".into())
                        .metadata(metadata(&[])),
                ]
            });

        assert_eq!(
            Some(ErrorCode::InconsistentGroupProtocol),
            join(
                Some(&converted()),
                &rejoining("z", &OTHER),
                CLIENT,
                None,
                &listed(),
                &ConsumerGroupConfig::default(),
                1_000,
            )
            .err()
        );
    }

    #[test]
    fn a_classic_member_leaves_by_id_or_instance() -> Result<(), ErrorCode> {
        let group = converted();

        let left = leave(
            Some(&group),
            &[("a".into(), None), ("z".into(), None)],
            &listed(),
            1_000,
        )?;

        assert!(left.changed);
        assert!(!left.group.members.contains_key("a"));
        assert!(
            left.group.group_epoch > group.group_epoch,
            "the rest are assigned again"
        );

        let Body::LeaveGroupResponse(response) = left.response else {
            panic!("not a leave response");
        };

        assert_eq!(
            vec![0, i16::from(ErrorCode::UnknownMemberId)],
            response
                .members
                .unwrap_or_default()
                .iter()
                .map(|member| member.error_code)
                .collect::<Vec<_>>()
        );

        Ok(())
    }
}
