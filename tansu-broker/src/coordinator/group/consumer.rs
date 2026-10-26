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

use regex::Regex;
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

/// The broker's policy for consumer-protocol groups (#628): Kafka's
/// `group.consumer.session.timeout.ms`, `group.consumer.heartbeat.interval.ms`
/// and `group.consumer.max.size`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsumerGroupConfig {
    /// The session every member is held to.
    pub session_timeout_ms: i32,

    /// How often every member is told to heartbeat.
    pub heartbeat_interval_ms: i32,

    /// The most members a group admits, `None` for no bound. A member joining
    /// past it is refused with `GROUP_MAX_SIZE_REACHED`.
    pub max_size: Option<usize>,
}

impl Default for ConsumerGroupConfig {
    fn default() -> Self {
        Self {
            session_timeout_ms: CONSUMER_SESSION_TIMEOUT_MS,
            heartbeat_interval_ms: CONSUMER_HEARTBEAT_INTERVAL_MS,
            max_size: None,
        }
    }
}

/// Who sent a heartbeat: what `ConsumerGroupDescribe` reports for its member.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Client<'a> {
    pub(crate) id: &'a str,

    /// The peer's address as Kafka prints it, `/10.0.0.1`.
    pub(crate) host: &'a str,
}

/// What a heartbeat knows of how recently each member was heard from (#627).
#[derive(Clone, Debug, Default)]
pub(crate) struct Liveness {
    /// The newest contact known per member, beyond what the document says:
    /// the stamps listed, and what this replica renewed itself.
    pub(crate) contacts: BTreeMap<String, i64>,

    /// Whether `contacts` holds the newest stamp of *every* member, from a
    /// listing. Only then can a member be judged lapsed: without it, a member
    /// another replica renewed would look as old as its document says.
    pub(crate) listed: bool,
}

impl Liveness {
    /// The newest contact known for `member_id`.
    fn last_contact_ms(&self, member_id: &str, member: &ConsumerMember) -> i64 {
        self.contacts
            .get(member_id)
            .map_or(member.last_contact_ms, |contact| {
                member.last_contact_ms.max(*contact)
            })
    }

    fn is_expired(&self, member_id: &str, member: &ConsumerMember, now_ms: i64) -> bool {
        now_ms.saturating_sub(self.last_contact_ms(member_id, member)) > member.lapses_after_ms()
    }
}

/// What [`apply`] decided.
#[derive(Clone, Debug)]
pub(crate) struct Transition {
    /// The group as it must be written back.
    pub(crate) group: ConsumerGroupDoc,

    /// Whether `group` differs from what was read, and so must be written.
    pub(crate) changed: bool,

    /// Whether the sender's liveness is due a renewal the document will not
    /// carry, because it is not being written (#627).
    pub(crate) renew: bool,

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

/// A subscription pattern as Kafka applies it (#625): matched against the
/// whole topic name, never a part of it.
///
/// Kafka compiles it with RE2J; the `regex` crate is the same RE2 dialect, so
/// a pattern one accepts the other matches alike. What RE2J has and this does
/// not (`\Q…\E` quoting) is refused as `INVALID_REGULAR_EXPRESSION` rather
/// than matched differently.
pub(crate) fn topic_pattern(regex: &str) -> Result<Regex, ErrorCode> {
    Regex::new(&format!("^(?:{regex})$")).map_err(|_| ErrorCode::InvalidRegularExpression)
}

/// What the members of a group subscribe to, by name and by pattern.
#[derive(Clone, Debug, Default)]
pub(crate) struct Subscriptions {
    pub(crate) names: BTreeSet<String>,
    pub(crate) patterns: Vec<Regex>,
}

impl Subscriptions {
    /// Whether any member subscribes to `name`.
    pub(crate) fn includes(&self, name: &str) -> bool {
        self.names.contains(name) || self.patterns.iter().any(|pattern| pattern.is_match(name))
    }
}

/// A regex in a request: `None` when it did not say, `Some(None)` when it
/// dropped its pattern (an empty string, as Kafka reads it).
fn requested_regex(request: &ConsumerGroupHeartbeatRequest) -> Option<Option<&str>> {
    request
        .subscribed_topic_regex
        .as_deref()
        .map(|regex| Some(regex).filter(|regex| !regex.is_empty()))
}

/// Every topic name and pattern any member of `group` subscribes to, with
/// `request`'s subscription in place of its sender's: what [`apply`] needs
/// resolved. A pattern that does not compile resolves nothing; [`apply`]
/// refuses the request that carries it.
pub(crate) fn subscriptions(
    group: Option<&ConsumerGroupDoc>,
    request: &ConsumerGroupHeartbeatRequest,
) -> Subscriptions {
    let others = group
        .into_iter()
        .flat_map(|group| group.members.iter())
        .filter(|(member_id, _)| **member_id != request.member_id)
        .map(|(_, member)| member);

    let sender = group.and_then(|group| group.members.get(&request.member_id));

    let names = others
        .clone()
        .chain(
            sender
                .filter(|_| request.subscribed_topic_names.is_none())
                .into_iter(),
        )
        .flat_map(|member| member.subscribed_topic_names.iter().cloned())
        .chain(request.subscribed_topic_names.iter().flatten().cloned())
        .collect();

    let sender_regex = requested_regex(request)
        .unwrap_or_else(|| sender.and_then(|member| member.subscribed_topic_regex.as_deref()));

    let patterns = others
        .filter_map(|member| member.subscribed_topic_regex.as_deref())
        .chain(sender_regex)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter_map(|regex| topic_pattern(regex).ok())
        .collect();

    Subscriptions { names, patterns }
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

/// Drop every member who was told to revoke and ran out its rebalance
/// timeout, and — when `liveness` was listed — every member whose session
/// lapsed. Answers whether anyone was dropped.
fn sweep(group: &mut ConsumerGroupDoc, liveness: &Liveness, now_ms: i64) -> bool {
    let before = group.members.len();

    group.members.retain(|member_id, member| {
        !member.is_revocation_overdue(now_ms)
            && !(liveness.listed && liveness.is_expired(member_id, member, now_ms))
    });

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
            let pattern = member
                .subscribed_topic_regex
                .as_deref()
                .and_then(|regex| topic_pattern(regex).ok());

            let by_pattern = by_name.iter().filter(|(name, _)| {
                pattern
                    .as_ref()
                    .is_some_and(|pattern| pattern.is_match(name))
            });

            (
                member_id.clone(),
                MemberSpec {
                    subscribed: member
                        .subscribed_topic_names
                        .iter()
                        .filter_map(|name| by_name.get(name.as_str()).copied())
                        .chain(by_pattern.map(|(_, topic_id)| *topic_id))
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

fn new_member(
    request: &ConsumerGroupHeartbeatRequest,
    client: Client<'_>,
    config: &ConsumerGroupConfig,
) -> ConsumerMember {
    ConsumerMember {
        member_epoch: 0,
        previous_member_epoch: -1,
        instance_id: request.instance_id.clone(),
        client_id: client.id.to_owned(),
        client_host: client.host.to_owned(),
        rebalance_timeout_ms: DEFAULT_REBALANCE_TIMEOUT_MS,
        session_timeout_ms: config.session_timeout_ms,
        ..Default::default()
    }
}

/// A member joining, or rejoining, with epoch 0. Answers the member id it
/// joins as and whether the group's composition changed.
fn join(
    group: &mut ConsumerGroupDoc,
    request: &ConsumerGroupHeartbeatRequest,
    client: Client<'_>,
    config: &ConsumerGroupConfig,
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
        replaced.client_id = client.id.to_owned();
        replaced.client_host = client.host.to_owned();
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

    if request.subscribed_topic_names.is_none() && requested_regex(request).flatten().is_none() {
        return Err(ErrorCode::InvalidRequest);
    }

    if config
        .max_size
        .is_some_and(|max_size| group.members.len() >= max_size)
    {
        return Err(ErrorCode::GroupMaxSizeReached);
    }

    _ = group
        .members
        .insert(member_id.clone(), new_member(request, client, config));

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
    client: Client<'_>,
    config: &ConsumerGroupConfig,
) -> Result<bool, ErrorCode> {
    let mut resubscribed = false;

    // Folded in on every heartbeat, so a broker restarted with another session
    // timeout, or a client that reconnected from elsewhere, is reflected
    // after one write rather than only on the member's next join (#628).
    member.session_timeout_ms = config.session_timeout_ms;

    if !client.host.is_empty() {
        client.host.clone_into(&mut member.client_host);
    }

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

    if let Some(regex) = requested_regex(request) {
        if let Some(regex) = regex {
            _ = topic_pattern(regex)?;
        }

        if member.subscribed_topic_regex.as_deref() != regex {
            member.subscribed_topic_regex = regex.map(str::to_owned);
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
/// `resolved` is the metadata of [`subscriptions`], or `None` when the
/// caller did not refresh it for this request, in which case the topics the
/// group already holds stand.
pub(crate) fn apply(
    held: Option<&ConsumerGroupDoc>,
    request: &ConsumerGroupHeartbeatRequest,
    client: Client<'_>,
    resolved: Option<&Resolved>,
    liveness: &Liveness,
    config: &ConsumerGroupConfig,
    now_ms: i64,
) -> Result<Transition, ErrorCode> {
    let before = held.cloned().unwrap_or_else(|| ConsumerGroupDoc {
        created_ms: now_ms,
        ..Default::default()
    });

    let mut group = before.clone();
    let owned = owned_from_request(request);

    let mut rebalance = sweep(&mut group, liveness, now_ms);

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
                renew: false,
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
            let (member_id, joined) = join(&mut group, request, client, config)?;
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
        rebalance |= update_member(member, request, client, config)?;
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

    let renewal_due = now_ms.saturating_sub(liveness.last_contact_ms(&member_id, member))
        >= i64::from(member.session_timeout_ms / 2);

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
        .heartbeat_interval_ms(config.heartbeat_interval_ms)
        .assignment(send_assignment.then(|| {
            Assignment::default().topic_partitions(Some(response_partitions(&member.assigned)))
        }));

    let changed = group != before;

    if changed && let Some(member) = group.members.get_mut(&member_id) {
        member.last_contact_ms = now_ms;
    }

    Ok(Transition {
        group,
        changed,
        renew: renewal_due && !changed,
        response,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tansu_sans_io::consumer_group_heartbeat_request;

    const T: Uuid = Uuid::from_u128(0x7);

    /// A listing that found no stamp: liveness is what the documents say.
    fn listed() -> Liveness {
        Liveness {
            contacts: BTreeMap::new(),
            listed: true,
        }
    }

    const CLIENT: Client<'static> = Client {
        id: "c",
        host: "/10.0.0.1",
    };

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
        let joined = apply(
            None,
            &joining("a"),
            CLIENT,
            Some(&resolved(3)),
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;

        assert!(joined.changed);
        assert_eq!(1, joined.response.member_epoch);
        assert_eq!(Some(BTreeSet::from([0, 1, 2])), assigned(&joined.response));
        assert_eq!(1, joined.group.group_epoch);
        assert_eq!(1, joined.group.assignment_epoch);

        let steady = apply(
            Some(&joined.group),
            &beating("a", 1, None),
            CLIENT,
            None,
            &listed(),
            &ConsumerGroupConfig::default(),
            1_000,
        )?;

        assert!(!steady.changed, "a steady heartbeat writes nothing");
        assert_eq!(None, steady.response.assignment);

        Ok(())
    }

    #[test]
    fn a_partition_is_revoked_before_it_is_reassigned() -> Result<(), ErrorCode> {
        let a = apply(
            None,
            &joining("a"),
            CLIENT,
            Some(&resolved(2)),
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;
        let b = apply(
            Some(&a.group),
            &joining("b"),
            CLIENT,
            Some(&resolved(2)),
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;

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
            CLIENT,
            None,
            &listed(),
            &ConsumerGroupConfig::default(),
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
            CLIENT,
            None,
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;
        assert_eq!(None, assigned(&b_waiting.response));

        let owned = kept.iter().copied().collect::<Vec<_>>();
        let a_acked = apply(
            Some(&b_waiting.group),
            &beating("a", 1, Some(&owned)),
            CLIENT,
            None,
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;
        assert_eq!(2, a_acked.response.member_epoch);

        let b_assigned = apply(
            Some(&a_acked.group),
            &beating("b", 2, Some(&[])),
            CLIENT,
            None,
            &listed(),
            &ConsumerGroupConfig::default(),
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
        let a = apply(
            None,
            &joining("a"),
            CLIENT,
            Some(&resolved(1)),
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;

        assert_eq!(
            Some(ErrorCode::FencedMemberEpoch),
            apply(
                Some(&a.group),
                &beating("a", 7, None),
                CLIENT,
                None,
                &listed(),
                &ConsumerGroupConfig::default(),
                0
            )
            .err()
        );

        assert_eq!(
            Some(ErrorCode::UnknownMemberId),
            apply(
                Some(&a.group),
                &beating("z", 1, None),
                CLIENT,
                None,
                &listed(),
                &ConsumerGroupConfig::default(),
                0
            )
            .err()
        );

        Ok(())
    }

    #[test]
    fn a_lapsed_member_is_swept() -> Result<(), ErrorCode> {
        let config = ConsumerGroupConfig::default();
        let a = apply(
            None,
            &joining("a"),
            CLIENT,
            Some(&resolved(2)),
            &listed(),
            &config,
            0,
        )?;
        let b = apply(
            Some(&a.group),
            &joining("b"),
            CLIENT,
            Some(&resolved(2)),
            &listed(),
            &config,
            0,
        )?;

        let lapsed = i64::from(CONSUMER_SESSION_TIMEOUT_MS) * 2;
        let renewal = lapsed - i64::from(CONSUMER_SESSION_TIMEOUT_MS);

        let renewed = apply(
            Some(&b.group),
            &beating("b", 2, Some(&[])),
            CLIENT,
            None,
            &listed(),
            &config,
            renewal,
        )?;

        assert!(renewed.renew, "b is due a renewal");
        assert!(
            !renewed.changed,
            "which is a stamp, not a write of the group (#627)"
        );

        let unlisted = apply(
            Some(&renewed.group),
            &beating("b", 2, Some(&[])),
            CLIENT,
            None,
            &Liveness::default(),
            &config,
            lapsed,
        )?;
        assert!(
            unlisted.group.members.contains_key("a"),
            "without a listing nobody can be judged lapsed"
        );

        let swept = apply(
            Some(&renewed.group),
            &beating("b", 2, Some(&[])),
            CLIENT,
            None,
            &Liveness {
                contacts: BTreeMap::from([("b".to_owned(), renewal)]),
                listed: true,
            },
            &config,
            lapsed,
        )?;

        assert!(!swept.group.members.contains_key("a"));
        assert!(swept.group.members.contains_key("b"), "b's stamp keeps it");
        assert_eq!(3, swept.response.member_epoch);
        assert_eq!(Some(BTreeSet::from([0, 1])), assigned(&swept.response));

        Ok(())
    }

    #[test]
    fn a_static_member_is_replaced_only_after_it_leaves() -> Result<(), ErrorCode> {
        let static_join = |member_id: &str| joining(member_id).instance_id(Some("i-1".into()));

        let a = apply(
            None,
            &static_join("a"),
            CLIENT,
            Some(&resolved(2)),
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;

        assert_eq!(
            Some(ErrorCode::UnreleasedInstanceId),
            apply(
                Some(&a.group),
                &static_join("b"),
                CLIENT,
                Some(&resolved(2)),
                &listed(),
                &ConsumerGroupConfig::default(),
                0
            )
            .err()
        );

        let left = apply(
            Some(&a.group),
            &beating("a", LEAVING_STATIC_MEMBER_EPOCH, None),
            CLIENT,
            None,
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;
        assert_eq!(
            1, left.group.group_epoch,
            "a static leave is not a rebalance"
        );

        let b = apply(
            Some(&left.group),
            &static_join("b"),
            CLIENT,
            Some(&resolved(2)),
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;
        assert_eq!(1, b.group.group_epoch);
        assert_eq!(Some(BTreeSet::from([0, 1])), assigned(&b.response));
        assert!(!b.group.members.contains_key("a"));

        Ok(())
    }

    #[test]
    fn a_member_is_described_from_the_host_it_heartbeats_from() -> Result<(), ErrorCode> {
        let a = apply(
            None,
            &joining("a"),
            CLIENT,
            Some(&resolved(1)),
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;
        assert_eq!("/10.0.0.1", a.group.members["a"].client_host);

        let moved = Client {
            id: "c",
            host: "/10.0.0.2",
        };

        let reconnected = apply(
            Some(&a.group),
            &beating("a", 1, None),
            moved,
            None,
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;
        assert!(reconnected.changed);
        assert_eq!("/10.0.0.2", reconnected.group.members["a"].client_host);

        Ok(())
    }

    #[test]
    fn a_member_past_the_maximum_size_is_refused() -> Result<(), ErrorCode> {
        let config = ConsumerGroupConfig {
            max_size: Some(1),
            ..Default::default()
        };

        let a = apply(
            None,
            &joining("a"),
            CLIENT,
            Some(&resolved(2)),
            &listed(),
            &config,
            0,
        )?;

        assert_eq!(
            Some(ErrorCode::GroupMaxSizeReached),
            apply(
                Some(&a.group),
                &joining("b"),
                CLIENT,
                Some(&resolved(2)),
                &listed(),
                &config,
                0
            )
            .err()
        );

        let rejoined = apply(
            Some(&a.group),
            &joining("a"),
            CLIENT,
            Some(&resolved(2)),
            &listed(),
            &config,
            0,
        )?;
        assert_eq!(
            1,
            rejoined.group.members.len(),
            "a member rejoining is not refused"
        );

        Ok(())
    }

    #[test]
    fn the_session_and_heartbeat_interval_are_the_brokers() -> Result<(), ErrorCode> {
        let config = ConsumerGroupConfig {
            session_timeout_ms: 10_000,
            heartbeat_interval_ms: 1_000,
            max_size: None,
        };

        let a = apply(
            None,
            &joining("a"),
            CLIENT,
            Some(&resolved(1)),
            &listed(),
            &config,
            0,
        )?;
        assert_eq!(1_000, a.response.heartbeat_interval_ms);
        assert_eq!(10_000, a.group.members["a"].session_timeout_ms);

        let lapsed = apply(
            Some(&a.group),
            &beating("a", 1, None),
            CLIENT,
            None,
            &listed(),
            &config,
            16_000,
        );
        assert_eq!(Some(ErrorCode::UnknownMemberId), lapsed.err());

        let shortened = apply(
            Some(&a.group),
            &beating("a", 1, None),
            CLIENT,
            None,
            &listed(),
            &ConsumerGroupConfig {
                session_timeout_ms: 6_000,
                ..config
            },
            1_000,
        )?;
        assert_eq!(6_000, shortened.group.members["a"].session_timeout_ms);

        Ok(())
    }

    fn by_pattern(member_id: &str, regex: &str) -> ConsumerGroupHeartbeatRequest {
        joining(member_id)
            .subscribed_topic_names(Some(vec![]))
            .subscribed_topic_regex(Some(regex.into()))
    }

    fn topics(topics: &[(&str, u128, i32)]) -> Resolved {
        topics
            .iter()
            .map(|(name, id, partitions)| ((*name).to_owned(), (Uuid::from_u128(*id), *partitions)))
            .collect()
    }

    fn assigned_topics(response: &ConsumerGroupHeartbeatResponse) -> BTreeMap<Uuid, usize> {
        response
            .assignment
            .iter()
            .flat_map(|assignment| assignment.topic_partitions.iter().flatten())
            .map(|topic| {
                (
                    Uuid::from_bytes(topic.topic_id),
                    topic.partitions.as_ref().map_or(0, Vec::len),
                )
            })
            .collect()
    }

    #[test]
    fn a_pattern_is_matched_against_the_whole_name() -> Result<(), ErrorCode> {
        let pattern = topic_pattern("orders-.*")?;

        assert!(pattern.is_match("orders-eu"));
        assert!(!pattern.is_match("old-orders-eu"), "a match is anchored");
        assert!(!topic_pattern("orders")?.is_match("orders-eu"));

        Ok(())
    }

    #[test]
    fn a_member_subscribed_by_pattern_is_assigned_every_matching_topic() -> Result<(), ErrorCode> {
        let resolved = topics(&[("orders-eu", 1, 2), ("orders-us", 2, 3), ("audit", 3, 4)]);

        let a = apply(
            None,
            &by_pattern("a", "orders-.*"),
            CLIENT,
            Some(&resolved),
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;

        assert_eq!(
            BTreeMap::from([(Uuid::from_u128(1), 2), (Uuid::from_u128(2), 3)]),
            assigned_topics(&a.response)
        );
        assert_eq!(
            Some("orders-.*"),
            a.group.members["a"].subscribed_topic_regex.as_deref()
        );

        let described = a.group.described("g", 0);
        assert_eq!(
            Some("orders-.*".to_owned()),
            described.members.unwrap_or_default()[0].subscribed_topic_regex
        );

        Ok(())
    }

    #[test]
    fn an_invalid_pattern_is_refused() {
        assert_eq!(
            Some(ErrorCode::InvalidRegularExpression),
            apply(
                None,
                &by_pattern("a", "orders-(.*"),
                CLIENT,
                Some(&Resolved::new()),
                &listed(),
                &ConsumerGroupConfig::default(),
                0
            )
            .err()
        );
    }

    #[test]
    fn a_topic_created_after_the_member_joined_is_assigned() -> Result<(), ErrorCode> {
        let before = topics(&[("orders-eu", 1, 2)]);
        let after = topics(&[("orders-eu", 1, 2), ("orders-us", 2, 3)]);

        let a = apply(
            None,
            &by_pattern("a", "orders-.*"),
            CLIENT,
            Some(&before),
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;
        assert_eq!(1, a.group.group_epoch);

        let refreshed = apply(
            Some(&a.group),
            &beating("a", 1, None),
            CLIENT,
            Some(&after),
            &listed(),
            &ConsumerGroupConfig::default(),
            1_000,
        )?;

        assert_eq!(2, refreshed.group.group_epoch);
        assert_eq!(2, refreshed.response.member_epoch);
        assert_eq!(
            BTreeMap::from([(Uuid::from_u128(1), 2), (Uuid::from_u128(2), 3)]),
            assigned_topics(&refreshed.response)
        );

        Ok(())
    }

    #[test]
    fn dropping_the_pattern_drops_its_topics() -> Result<(), ErrorCode> {
        let resolved = topics(&[("orders-eu", 1, 2), ("t", 7, 1)]);

        let a = apply(
            None,
            &by_pattern("a", "orders-.*").subscribed_topic_names(Some(vec!["t".into()])),
            CLIENT,
            Some(&resolved),
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;
        assert_eq!(2, assigned_topics(&a.response).len());

        let dropped = apply(
            Some(&a.group),
            &beating("a", 1, Some(&[0])).subscribed_topic_regex(Some(String::new())),
            CLIENT,
            Some(&topics(&[("t", 7, 1)])),
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;

        assert_eq!(None, dropped.group.members["a"].subscribed_topic_regex);
        assert_eq!(BTreeMap::from([(T, 1)]), assigned_topics(&dropped.response));

        Ok(())
    }

    #[test]
    fn subscriptions_take_in_every_members_pattern() -> Result<(), ErrorCode> {
        let a = apply(
            None,
            &by_pattern("a", "orders-.*"),
            CLIENT,
            Some(&Resolved::new()),
            &listed(),
            &ConsumerGroupConfig::default(),
            0,
        )?;

        let joined = subscriptions(Some(&a.group), &joining("b"));

        assert!(joined.includes("orders-eu"), "a's pattern");
        assert!(joined.includes("t"), "b's name");
        assert!(!joined.includes("audit"));

        let replaced = subscriptions(
            Some(&a.group),
            &beating("a", 1, None).subscribed_topic_regex(Some("audit".into())),
        );
        assert!(
            !replaced.includes("orders-eu"),
            "a's request replaces its pattern"
        );
        assert!(replaced.includes("audit"));

        Ok(())
    }
}
