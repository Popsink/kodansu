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

//! The KIP-848 `ConsumerGroupHeartbeat` state machine (#612).
//!
//! [`apply`] is the whole of it: a pure function of the group document as
//! read, the request, the subscribed topics' metadata and the clock, answering
//! the document to write back and the response to send. The coordinator reads
//! [`ConsumerGroupDoc`], applies, and writes conditionally; a lost CAS is
//! re-read and re-applied. Nothing here is held between requests, which is
//! what lets any replica serve any heartbeat of any group.
//!
//! The reconciliation follows Kafka's `CurrentAssignmentBuilder`:
//!
//! - the **group epoch** moves on every change of membership, subscription or
//!   subscribed topic metadata, and the target assignment is recomputed at
//!   once, so the assignment epoch always catches up within the same write;
//! - a member whose current assignment holds partitions its target does not is
//!   told to revoke them first, keeping its epoch, and moves to the target
//!   epoch only once it has acknowledged (`UnrevokedPartitions`);
//! - a partition is handed to its new owner only once no other member owns it
//!   or has it pending revocation (`UnreleasedPartitions`), so no partition is
//!   ever owned by two members at once.

use std::collections::{BTreeMap, BTreeSet};

use tansu_sans_io::{
    ConsumerGroupHeartbeatRequest, ErrorCode,
    consumer_group_heartbeat_response::{
        Assignment, ConsumerGroupHeartbeatResponse, TopicPartitions,
    },
};
use tansu_storage::{
    CONSUMER_HEARTBEAT_INTERVAL_MS, CONSUMER_SESSION_TIMEOUT_MS, ConsumerGroupDoc, ConsumerMember,
    LEAVING_STATIC_MEMBER_EPOCH, MemberState, Partitions, SubscribedTopic, flatten, grouped,
};
use uuid::Uuid;

use super::assignor::{self, MemberSpec, UNIFORM};

/// The rebalance timeout of a member that joins without declaring one: the
/// Java client's default `max.poll.interval.ms`.
const DEFAULT_REBALANCE_TIMEOUT_MS: i32 = 300_000;

/// The subscribed topics that exist, by name: what a heartbeat resolved from
/// the topic metadata before applying.
pub(crate) type Resolved = BTreeMap<String, (Uuid, i32)>;

/// What [`apply`] decided.
#[derive(Clone, Debug)]
pub(crate) struct Transition {
    /// The group as it must be written back.
    pub(crate) group: ConsumerGroupDoc,

    /// Whether `group` differs from what was read, and so must be written.
    pub(crate) changed: bool,

    pub(crate) response: ConsumerGroupHeartbeatResponse,
}

/// The heartbeat answered with `error_code` alone.
pub(crate) fn error_response(error_code: ErrorCode) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse::default()
        .throttle_time_ms(0)
        .error_code(error_code.into())
        .error_message(Some(error_code.to_string()))
        .member_id(None)
        .member_epoch(-1)
        .heartbeat_interval_ms(0)
        .assignment(None)
}

/// Every topic name any member of `group` subscribes to, with `request`'s
/// subscription in place of its sender's: what [`apply`] needs resolved.
pub(crate) fn subscribed_names(
    group: Option<&ConsumerGroupDoc>,
    request: &ConsumerGroupHeartbeatRequest,
) -> BTreeSet<String> {
    group
        .into_iter()
        .flat_map(|group| group.members.iter())
        .filter(|(member_id, _)| {
            request.subscribed_topic_names.is_none() || **member_id != request.member_id
        })
        .flat_map(|(_, member)| member.subscribed_topic_names.iter().cloned())
        .chain(request.subscribed_topic_names.iter().flatten().cloned())
        .collect()
}

fn owned_from_request(request: &ConsumerGroupHeartbeatRequest) -> Option<Partitions> {
    request.topic_partitions.as_ref().map(|topics| {
        grouped(topics.iter().flat_map(|topic| {
            let topic_id = Uuid::from_bytes(topic.topic_id);
            topic
                .partitions
                .iter()
                .flatten()
                .map(move |partition| (topic_id, *partition))
        }))
    })
}

/// Drop every member whose session lapsed, or who was told to revoke and ran
/// out its rebalance timeout. Answers whether anyone was dropped.
fn sweep(group: &mut ConsumerGroupDoc, now_ms: i64) -> bool {
    let before = group.members.len();

    group
        .members
        .retain(|_, member| !member.is_expired(now_ms) && !member.is_revocation_overdue(now_ms));

    group.members.len() != before
}

/// The assignor most members ask for, or `uniform` when none asks.
fn preferred_assignor(group: &ConsumerGroupDoc) -> String {
    let mut votes: BTreeMap<&str, usize> = BTreeMap::new();

    for member in group.members.values() {
        if let Some(assignor) = member.server_assignor.as_deref() {
            *votes.entry(assignor).or_default() += 1;
        }
    }

    votes
        .into_iter()
        .max_by(|(a, a_votes), (b, b_votes)| a_votes.cmp(b_votes).then(b.cmp(a)))
        .map_or_else(|| UNIFORM.to_owned(), |(assignor, _)| assignor.to_owned())
}

/// Recompute every member's target at a new group epoch.
fn retarget(group: &mut ConsumerGroupDoc) {
    group.group_epoch = group.group_epoch.saturating_add(1);

    let by_name = group
        .topics
        .iter()
        .map(|(topic_id, topic)| (topic.name.as_str(), *topic_id))
        .collect::<BTreeMap<_, _>>();

    let specs = group
        .members
        .iter()
        .map(|(member_id, member)| {
            (
                member_id.clone(),
                MemberSpec {
                    subscribed: member
                        .subscribed_topic_names
                        .iter()
                        .filter_map(|name| by_name.get(name.as_str()).copied())
                        .collect(),
                    previous: member.target.clone(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    let topics = group
        .topics
        .iter()
        .map(|(topic_id, topic)| (*topic_id, topic.partitions))
        .collect();

    let assignor = preferred_assignor(group);
    let mut targets = assignor::assign(&assignor, &specs, &topics);

    for (member_id, member) in &mut group.members {
        member.target = targets.remove(member_id).unwrap_or_default();
    }

    group.assignor = Some(assignor);
    group.assignment_epoch = group.group_epoch;
}

/// Replace the subscribed topic metadata with `resolved`, answering whether it
/// moved — a topic created, deleted, recreated or repartitioned.
fn refresh_topics(group: &mut ConsumerGroupDoc, resolved: &Resolved) -> bool {
    let topics = resolved
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
        .collect::<BTreeMap<_, _>>();

    if topics == group.topics {
        false
    } else {
        group.topics = topics;
        true
    }
}

/// Every partition owned, or pending revocation, by a member other than
/// `member_id`.
fn owned_by_others(group: &ConsumerGroupDoc, member_id: &str) -> BTreeSet<(Uuid, i32)> {
    group
        .members
        .iter()
        .filter(|(other, _)| *other != member_id)
        .flat_map(|(_, member)| {
            flatten(&member.assigned)
                .into_iter()
                .chain(flatten(&member.pending_revocation))
        })
        .collect()
}

/// Whether the member, as far as its request says, still owns any of
/// `revoking`. A request that does not say is taken to still own them.
fn still_owns(owned: Option<&Partitions>, revoking: &Partitions) -> bool {
    owned.is_none_or(|owned| !flatten(owned).is_disjoint(&flatten(revoking)))
}

/// Kafka's `computeNextAssignment`: move `member` as far towards its target
/// as the other members' ownership allows.
fn next_assignment(
    member: &mut ConsumerMember,
    target_epoch: i32,
    taken: &BTreeSet<(Uuid, i32)>,
    owned: Option<&Partitions>,
    now_ms: i64,
) {
    let current = flatten(&member.assigned);
    let target = flatten(&member.target);

    let kept = current
        .intersection(&target)
        .copied()
        .collect::<BTreeSet<_>>();
    let revoking = grouped(current.difference(&target).copied());

    if !revoking.is_empty() && still_owns(owned, &revoking) {
        if member.state != MemberState::UnrevokedPartitions {
            member.revocation_deadline_ms =
                Some(now_ms.saturating_add(i64::from(member.rebalance_timeout_ms)));
        }

        member.state = MemberState::UnrevokedPartitions;
        member.assigned = grouped(kept);
        member.pending_revocation = revoking;
        return;
    }

    let (free, unreleased): (BTreeSet<_>, BTreeSet<_>) = target
        .difference(&current)
        .copied()
        .partition(|partition| !taken.contains(partition));

    if member.member_epoch != target_epoch {
        member.previous_member_epoch = member.member_epoch;
        member.member_epoch = target_epoch;
    }

    member.state = if unreleased.is_empty() {
        MemberState::Stable
    } else {
        MemberState::UnreleasedPartitions
    };

    member.assigned = grouped(kept.into_iter().chain(free));
    member.pending_revocation = Partitions::new();
    member.revocation_deadline_ms = None;
}

/// Kafka's `CurrentAssignmentBuilder.build` for the member that sent the
/// heartbeat.
fn reconcile(
    group: &mut ConsumerGroupDoc,
    member_id: &str,
    owned: Option<&Partitions>,
    now_ms: i64,
) {
    let taken = owned_by_others(group, member_id);
    let target_epoch = group.assignment_epoch;

    let Some(member) = group.members.get_mut(member_id) else {
        return;
    };

    let moves = match member.state {
        MemberState::Stable => member.member_epoch != target_epoch,
        MemberState::UnrevokedPartitions => !still_owns(owned, &member.pending_revocation),
        MemberState::UnreleasedPartitions => true,
    };

    if moves {
        next_assignment(member, target_epoch, &taken, owned, now_ms);
    }
}

fn new_member(request: &ConsumerGroupHeartbeatRequest, client_id: &str) -> ConsumerMember {
    ConsumerMember {
        member_epoch: 0,
        previous_member_epoch: -1,
        instance_id: request.instance_id.clone(),
        client_id: client_id.to_owned(),
        rebalance_timeout_ms: DEFAULT_REBALANCE_TIMEOUT_MS,
        session_timeout_ms: CONSUMER_SESSION_TIMEOUT_MS,
        ..Default::default()
    }
}

/// A member joining, or rejoining, with epoch 0. Answers the member id it
/// joins as and whether the group's composition changed.
fn join(
    group: &mut ConsumerGroupDoc,
    request: &ConsumerGroupHeartbeatRequest,
    client_id: &str,
) -> Result<(String, bool), ErrorCode> {
    let member_id = if request.member_id.is_empty() {
        Uuid::new_v4().to_string()
    } else {
        request.member_id.clone()
    };

    if let Some(instance_id) = request.instance_id.as_deref()
        && let Some(previous) = group
            .members
            .iter()
            .find(|(other, member)| {
                **other != member_id && member.instance_id.as_deref() == Some(instance_id)
            })
            .map(|(other, member)| (other.clone(), member.is_leaving_static()))
    {
        let (previous_id, left) = previous;

        if !left {
            return Err(ErrorCode::UnreleasedInstanceId);
        }

        let Some(mut replaced) = group.members.remove(&previous_id) else {
            return Err(ErrorCode::UnknownMemberId);
        };

        replaced.member_epoch = 0;
        replaced.previous_member_epoch = 0;
        replaced.state = MemberState::Stable;
        replaced.pending_revocation = Partitions::new();
        replaced.revocation_deadline_ms = None;
        replaced.client_id = client_id.to_owned();
        _ = group.members.insert(member_id.clone(), replaced);

        return Ok((member_id, false));
    }

    if let Some(member) = group.members.get_mut(&member_id) {
        member.member_epoch = 0;
        member.previous_member_epoch = 0;
        member.state = MemberState::Stable;
        member.assigned = Partitions::new();
        member.pending_revocation = Partitions::new();
        member.revocation_deadline_ms = None;
        return Ok((member_id, false));
    }

    if request.subscribed_topic_names.is_none() {
        return Err(ErrorCode::InvalidRequest);
    }

    _ = group
        .members
        .insert(member_id.clone(), new_member(request, client_id));

    Ok((member_id, true))
}

/// Kafka's `throwIfMemberEpochIsInvalid` for a member heartbeating with an
/// epoch above zero.
fn validate_epoch(
    group: &ConsumerGroupDoc,
    request: &ConsumerGroupHeartbeatRequest,
    owned: Option<&Partitions>,
) -> Result<(), ErrorCode> {
    let Some(member) = group.members.get(&request.member_id) else {
        return Err(ErrorCode::UnknownMemberId);
    };

    if request.member_epoch > member.member_epoch {
        return Err(ErrorCode::FencedMemberEpoch);
    }

    if request.member_epoch < member.member_epoch {
        let lost_response = request.member_epoch == member.previous_member_epoch
            && owned.is_some_and(|owned| flatten(owned).is_subset(&flatten(&member.assigned)));

        if !lost_response {
            return Err(ErrorCode::FencedMemberEpoch);
        }
    }

    Ok(())
}

/// Fold what the request says about its sender into the member, answering
/// whether its subscription or assignor moved.
fn update_member(
    member: &mut ConsumerMember,
    request: &ConsumerGroupHeartbeatRequest,
) -> Result<bool, ErrorCode> {
    let mut resubscribed = false;

    if let Some(rack_id) = request.rack_id.as_ref() {
        member.rack_id = Some(rack_id.clone());
    }

    if request.rebalance_timeout_ms > 0 {
        member.rebalance_timeout_ms = request.rebalance_timeout_ms;
    }

    if let Some(names) = request.subscribed_topic_names.as_ref() {
        let names = names
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        if names != member.subscribed_topic_names {
            member.subscribed_topic_names = names;
            resubscribed = true;
        }
    }

    if let Some(assignor) = request.server_assignor.as_deref() {
        if !assignor::is_supported(assignor) {
            return Err(ErrorCode::UnsupportedAssignor);
        }

        if member.server_assignor.as_deref() != Some(assignor) {
            member.server_assignor = Some(assignor.to_owned());
            resubscribed = true;
        }
    }

    Ok(resubscribed)
}

/// A member leaving: for good with epoch `-1`, or — a static member — with
/// `-2`, keeping its assignment for its session in case it comes back.
fn leave(
    group: &mut ConsumerGroupDoc,
    request: &ConsumerGroupHeartbeatRequest,
    now_ms: i64,
) -> Result<bool, ErrorCode> {
    let Some(member) = group.members.get_mut(&request.member_id) else {
        return Err(ErrorCode::UnknownMemberId);
    };

    if request.member_epoch == LEAVING_STATIC_MEMBER_EPOCH && member.instance_id.is_some() {
        member.member_epoch = LEAVING_STATIC_MEMBER_EPOCH;
        member.last_contact_ms = now_ms;
        return Ok(false);
    }

    _ = group.members.remove(&request.member_id);
    Ok(true)
}

fn response_partitions(partitions: &Partitions) -> Vec<TopicPartitions> {
    partitions
        .iter()
        .map(|(topic_id, partitions)| {
            TopicPartitions::default()
                .topic_id(topic_id.into_bytes())
                .partitions(Some(partitions.iter().copied().collect()))
        })
        .collect()
}

/// Apply one heartbeat to the group as read — `None` for a group with no
/// document yet — at `now_ms`.
///
/// `resolved` is the metadata of [`subscribed_names`], or `None` when the
/// caller did not refresh it for this request, in which case the topics the
/// group already holds stand.
pub(crate) fn apply(
    held: Option<&ConsumerGroupDoc>,
    request: &ConsumerGroupHeartbeatRequest,
    client_id: &str,
    resolved: Option<&Resolved>,
    now_ms: i64,
) -> Result<Transition, ErrorCode> {
    let before = held.cloned().unwrap_or_else(|| ConsumerGroupDoc {
        created_ms: now_ms,
        ..Default::default()
    });

    let mut group = before.clone();
    let owned = owned_from_request(request);

    let mut rebalance = sweep(&mut group, now_ms);

    let member_id = match request.member_epoch {
        -1 | LEAVING_STATIC_MEMBER_EPOCH => {
            rebalance |= leave(&mut group, request, now_ms)?;

            if rebalance {
                retarget(&mut group);
            }

            let changed = group != before;

            return Ok(Transition {
                group,
                changed,
                response: ConsumerGroupHeartbeatResponse::default()
                    .throttle_time_ms(0)
                    .error_code(ErrorCode::None.into())
                    .member_id(Some(request.member_id.clone()))
                    .member_epoch(request.member_epoch)
                    .heartbeat_interval_ms(0)
                    .assignment(None),
            });
        }

        0 => {
            let (member_id, joined) = join(&mut group, request, client_id)?;
            rebalance |= joined;
            member_id
        }

        _ => {
            validate_epoch(&group, request, owned.as_ref())?;
            request.member_id.clone()
        }
    };

    let previous_assigned = before
        .members
        .get(&member_id)
        .map(|member| member.assigned.clone());

    if let Some(member) = group.members.get_mut(&member_id) {
        rebalance |= update_member(member, request)?;
    }

    if let Some(resolved) = resolved {
        rebalance |= refresh_topics(&mut group, resolved);
    }

    if rebalance {
        retarget(&mut group);
    }

    reconcile(&mut group, &member_id, owned.as_ref(), now_ms);

    let Some(member) = group.members.get_mut(&member_id) else {
        return Err(ErrorCode::UnknownMemberId);
    };

    let renewal_due =
        now_ms.saturating_sub(member.last_contact_ms) >= i64::from(member.session_timeout_ms / 2);

    let send_assignment = request.member_epoch != member.member_epoch
        || previous_assigned.as_ref() != Some(&member.assigned)
        || owned
            .as_ref()
            .is_some_and(|owned| *owned != member.assigned);

    let response = ConsumerGroupHeartbeatResponse::default()
        .throttle_time_ms(0)
        .error_code(ErrorCode::None.into())
        .member_id(Some(member_id.clone()))
        .member_epoch(member.member_epoch)
        .heartbeat_interval_ms(CONSUMER_HEARTBEAT_INTERVAL_MS)
        .assignment(send_assignment.then(|| {
            Assignment::default().topic_partitions(Some(response_partitions(&member.assigned)))
        }));

    if (renewal_due || group != before)
        && let Some(member) = group.members.get_mut(&member_id)
    {
        member.last_contact_ms = now_ms;
    }

    let changed = group != before;

    Ok(Transition {
        group,
        changed,
        response,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tansu_sans_io::consumer_group_heartbeat_request;

    const T: Uuid = Uuid::from_u128(0x7);

    fn resolved(partitions: i32) -> Resolved {
        BTreeMap::from([("t".to_owned(), (T, partitions))])
    }

    fn joining(member_id: &str) -> ConsumerGroupHeartbeatRequest {
        ConsumerGroupHeartbeatRequest::default()
            .group_id("g".into())
            .member_id(member_id.into())
            .member_epoch(0)
            .rebalance_timeout_ms(60_000)
            .subscribed_topic_names(Some(vec!["t".into()]))
            .topic_partitions(Some(vec![]))
    }

    fn beating(
        member_id: &str,
        epoch: i32,
        owned: Option<&[i32]>,
    ) -> ConsumerGroupHeartbeatRequest {
        ConsumerGroupHeartbeatRequest::default()
            .group_id("g".into())
            .member_id(member_id.into())
            .member_epoch(epoch)
            .rebalance_timeout_ms(-1)
            .topic_partitions(owned.map(|owned| {
                vec![
                    consumer_group_heartbeat_request::TopicPartitions::default()
                        .topic_id(T.into_bytes())
                        .partitions(Some(owned.to_vec())),
                ]
            }))
    }

    fn assigned(response: &ConsumerGroupHeartbeatResponse) -> Option<BTreeSet<i32>> {
        response.assignment.as_ref().map(|assignment| {
            assignment
                .topic_partitions
                .iter()
                .flatten()
                .flat_map(|topic| topic.partitions.iter().flatten().copied())
                .collect()
        })
    }

    #[test]
    fn a_member_joins_and_is_given_every_partition() -> Result<(), ErrorCode> {
        let joined = apply(None, &joining("a"), "c", Some(&resolved(3)), 0)?;

        assert!(joined.changed);
        assert_eq!(1, joined.response.member_epoch);
        assert_eq!(Some(BTreeSet::from([0, 1, 2])), assigned(&joined.response));
        assert_eq!(1, joined.group.group_epoch);
        assert_eq!(1, joined.group.assignment_epoch);

        let steady = apply(
            Some(&joined.group),
            &beating("a", 1, None),
            "c",
            None,
            1_000,
        )?;

        assert!(!steady.changed, "a steady heartbeat writes nothing");
        assert_eq!(None, steady.response.assignment);

        Ok(())
    }

    #[test]
    fn a_partition_is_revoked_before_it_is_reassigned() -> Result<(), ErrorCode> {
        let a = apply(None, &joining("a"), "c", Some(&resolved(2)), 0)?;
        let b = apply(Some(&a.group), &joining("b"), "c", Some(&resolved(2)), 0)?;

        assert_eq!(2, b.group.group_epoch);
        assert_eq!(2, b.response.member_epoch);
        assert_eq!(
            Some(BTreeSet::new()),
            assigned(&b.response),
            "b's partition is still a's"
        );

        let a_revoking = apply(
            Some(&b.group),
            &beating("a", 1, Some(&[0, 1])),
            "c",
            None,
            0,
        )?;
        assert_eq!(
            1, a_revoking.response.member_epoch,
            "no epoch until revoked"
        );
        assert_eq!(1, assigned(&a_revoking.response).map_or(0, |p| p.len()));

        let kept = assigned(&a_revoking.response).unwrap_or_default();
        let given_up = [0, 1]
            .into_iter()
            .filter(|p| !kept.contains(p))
            .collect::<Vec<_>>();

        let b_waiting = apply(
            Some(&a_revoking.group),
            &beating("b", 2, Some(&[])),
            "c",
            None,
            0,
        )?;
        assert_eq!(None, assigned(&b_waiting.response));

        let owned = kept.iter().copied().collect::<Vec<_>>();
        let a_acked = apply(
            Some(&b_waiting.group),
            &beating("a", 1, Some(&owned)),
            "c",
            None,
            0,
        )?;
        assert_eq!(2, a_acked.response.member_epoch);

        let b_assigned = apply(
            Some(&a_acked.group),
            &beating("b", 2, Some(&[])),
            "c",
            None,
            0,
        )?;
        assert_eq!(
            Some(given_up.into_iter().collect()),
            assigned(&b_assigned.response)
        );

        Ok(())
    }

    #[test]
    fn epochs_are_fenced() -> Result<(), ErrorCode> {
        let a = apply(None, &joining("a"), "c", Some(&resolved(1)), 0)?;

        assert_eq!(
            Some(ErrorCode::FencedMemberEpoch),
            apply(Some(&a.group), &beating("a", 7, None), "c", None, 0).err()
        );

        assert_eq!(
            Some(ErrorCode::UnknownMemberId),
            apply(Some(&a.group), &beating("z", 1, None), "c", None, 0).err()
        );

        Ok(())
    }

    #[test]
    fn a_lapsed_member_is_swept() -> Result<(), ErrorCode> {
        let a = apply(None, &joining("a"), "c", Some(&resolved(2)), 0)?;
        let b = apply(Some(&a.group), &joining("b"), "c", Some(&resolved(2)), 0)?;

        let lapsed = i64::from(CONSUMER_SESSION_TIMEOUT_MS) * 2;

        let renewed = apply(
            Some(&b.group),
            &beating("b", 2, Some(&[])),
            "c",
            None,
            lapsed - i64::from(CONSUMER_SESSION_TIMEOUT_MS),
        )?;

        let swept = apply(
            Some(&renewed.group),
            &beating("b", 2, Some(&[])),
            "c",
            None,
            lapsed,
        )?;

        assert!(!swept.group.members.contains_key("a"));
        assert_eq!(3, swept.response.member_epoch);
        assert_eq!(Some(BTreeSet::from([0, 1])), assigned(&swept.response));

        Ok(())
    }

    #[test]
    fn a_static_member_is_replaced_only_after_it_leaves() -> Result<(), ErrorCode> {
        let static_join = |member_id: &str| joining(member_id).instance_id(Some("i-1".into()));

        let a = apply(None, &static_join("a"), "c", Some(&resolved(2)), 0)?;

        assert_eq!(
            Some(ErrorCode::UnreleasedInstanceId),
            apply(
                Some(&a.group),
                &static_join("b"),
                "c",
                Some(&resolved(2)),
                0
            )
            .err()
        );

        let left = apply(
            Some(&a.group),
            &beating("a", LEAVING_STATIC_MEMBER_EPOCH, None),
            "c",
            None,
            0,
        )?;
        assert_eq!(
            1, left.group.group_epoch,
            "a static leave is not a rebalance"
        );

        let b = apply(
            Some(&left.group),
            &static_join("b"),
            "c",
            Some(&resolved(2)),
            0,
        )?;
        assert_eq!(1, b.group.group_epoch);
        assert_eq!(Some(BTreeSet::from([0, 1])), assigned(&b.response));
        assert!(!b.group.members.contains_key("a"));

        Ok(())
    }
}
