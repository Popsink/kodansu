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

//! The KIP-848 consumer group object (#612).
//!
//! A group that speaks the `consumer` protocol (`group.protocol=consumer`) is
//! one object, `groups/consumers/{group}/consumer.json` → [`ConsumerGroupDoc`],
//! written under etag CAS by whichever replica serves the heartbeat — the same
//! discipline `generation.json` is held to (#359). A request reads it, applies
//! a pure function of the document, the request and the clock, and writes the
//! result back conditionally; a lost CAS is a full re-read and re-apply, so two
//! replicas serving the same group cannot both win and no replica holds group
//! state between requests.
//!
//! It is beside the classic group's objects rather than inside them, and a
//! group is one type for its whole life: the coordinator refuses a classic
//! `JoinGroup` against a consumer group with members, and a
//! `ConsumerGroupHeartbeat` against a classic group with members.
//!
//! # Why one object
//!
//! Every write a KIP-848 coordinator makes is a reconciliation step whose
//! correctness depends on what the *other* members own: a partition is handed
//! to its new owner only once its previous owner has acknowledged revoking it.
//! That is a read of the whole group, so splitting members into objects of
//! their own would turn each step into a multi-object read with no atomicity.
//! Liveness is the exception (#627). A member renews it at most once per
//! `session/2`, which on one object meant a write rate that grew with the
//! group — every one of them a CAS on the same etag. So a member renews with an
//! object of its own instead, `liveness/{member}-{stamp}`, which no other
//! member writes; the document is written only when membership, subscription
//! or assignment moves, and one listing of the stamps answers how recently
//! every member was heard from.
//!
//! # Schema evolution
//!
//! As [`GenerationDoc`](crate::GenerationDoc): every optional field is
//! `Option` + `skip_serializing_if`, and both structs carry a flattened `rest`
//! so a field a newer binary added survives a rewrite by an older one.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use tansu_sans_io::consumer_group_describe_response::{
    Assignment, DescribedGroup, Member, TopicPartitions,
};
use uuid::Uuid;

use crate::ConsumerGroupState;

/// The session a consumer-protocol member is held to unless the broker is
/// configured otherwise, Kafka's default `group.consumer.session.timeout.ms`.
pub const CONSUMER_SESSION_TIMEOUT_MS: i32 = 45_000;

/// How often a consumer-protocol member is told to heartbeat unless the broker
/// is configured otherwise, Kafka's default
/// `group.consumer.heartbeat.interval.ms`.
pub const CONSUMER_HEARTBEAT_INTERVAL_MS: i32 = 5_000;

/// `ConsumerGroupDescribe`'s `MemberType` for a member that speaks the
/// consumer protocol (KIP-1099), as opposed to `0` for a classic one.
const CONSUMER_MEMBER_TYPE: i8 = 1;

/// The epoch a static member sends, and is recorded with, when it leaves
/// intending to come back under the same `instance_id`.
pub const LEAVING_STATIC_MEMBER_EPOCH: i32 = -2;

/// Every liveness stamp a group's members have written, by member id (#627):
/// the epoch milliseconds of each renewal not yet cleaned up.
pub type ContactStamps = BTreeMap<String, BTreeSet<i64>>;

/// Partitions by topic id: what a member owns, is losing, or should own.
pub type Partitions = BTreeMap<Uuid, BTreeSet<i32>>;

/// Where a member is in reconciling its current assignment towards its target.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub enum MemberState {
    /// The member owns exactly what it was last told to.
    #[default]
    Stable,

    /// The member has been told to give partitions up and has not yet
    /// acknowledged it. Its epoch does not move until it does.
    UnrevokedPartitions,

    /// The member is at the target epoch but some of its target partitions are
    /// still owned by another member that has not revoked them yet.
    UnreleasedPartitions,
}

/// A subscribed topic as the target assignment was computed against it.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SubscribedTopic {
    pub name: String,
    pub partitions: i32,
}

/// One member of a consumer-protocol group.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ConsumerMember {
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub state: MemberState,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rack_id: Option<String>,

    #[serde(default)]
    pub client_id: String,

    #[serde(default)]
    pub client_host: String,

    pub subscribed_topic_names: Vec<String>,

    /// The pattern the member subscribes by (#625), RE2 syntax matched against
    /// the whole topic name, beside or instead of its topic names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribed_topic_regex: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_assignor: Option<String>,

    pub rebalance_timeout_ms: i32,
    pub session_timeout_ms: i32,

    /// Epoch milliseconds of the last contact this document recorded: when the
    /// member joined, or the document was last written while it heartbeated.
    ///
    /// Not renewed for its own sake any more (#627): a member renews its
    /// liveness with a stamp beside the document, at most once per
    /// `session/2`, and [`ConsumerGroupDoc::with_contacts`] folds the newest
    /// back in. What it lags the real last contact by is what
    /// [`Self::lapses_after_ms`] gives back.
    pub last_contact_ms: i64,

    /// When a member told to revoke partitions is fenced for not having done
    /// so: its rebalance timeout from the moment it was told.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revocation_deadline_ms: Option<i64>,

    /// What the member owns and may consume from.
    #[serde(default)]
    pub assigned: Partitions,

    /// What the member has been told to give up and has not acknowledged.
    #[serde(default)]
    pub pending_revocation: Partitions,

    /// What the member should own at the group's assignment epoch.
    #[serde(default)]
    pub target: Partitions,

    #[serde(flatten)]
    pub rest: BTreeMap<String, serde_json::Value>,
}

impl ConsumerMember {
    /// The silence this member is allowed: its session plus the renewal window,
    /// as [`MemberDoc::lapses_after_ms`](crate::MemberDoc::lapses_after_ms)
    /// and for the same reason.
    #[must_use]
    pub fn lapses_after_ms(&self) -> i64 {
        let session = i64::from(self.session_timeout_ms);
        session.saturating_add(session / 2)
    }

    /// Whether this member's session has lapsed at `now_ms`.
    #[must_use]
    pub fn is_expired(&self, now_ms: i64) -> bool {
        now_ms.saturating_sub(self.last_contact_ms) > self.lapses_after_ms()
    }

    /// Whether this member was told to revoke partitions and its rebalance
    /// timeout ran out before it acknowledged.
    #[must_use]
    pub fn is_revocation_overdue(&self, now_ms: i64) -> bool {
        self.state == MemberState::UnrevokedPartitions
            && self
                .revocation_deadline_ms
                .is_some_and(|deadline| now_ms > deadline)
    }

    /// Whether this is a static member that left intending to come back.
    #[must_use]
    pub fn is_leaving_static(&self) -> bool {
        self.member_epoch == LEAVING_STATIC_MEMBER_EPOCH
    }
}

/// A consumer-protocol group, `consumer.json`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ConsumerGroupDoc {
    /// ABA discriminator, incremented on every write. See
    /// [`MemberDoc::seq`](crate::MemberDoc::seq).
    pub seq: u64,

    /// Bumped on every change of membership, subscription, or subscribed
    /// topic metadata. Never regresses.
    pub group_epoch: i32,

    /// The group epoch the members' `target` assignments were computed at.
    pub assignment_epoch: i32,

    /// The server assignor the target assignment was computed with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignor: Option<String>,

    pub members: BTreeMap<String, ConsumerMember>,

    /// The subscribed topics the target assignment was computed against.
    #[serde(default)]
    pub topics: BTreeMap<Uuid, SubscribedTopic>,

    pub created_ms: i64,

    #[serde(flatten)]
    pub rest: BTreeMap<String, serde_json::Value>,
}

impl ConsumerGroupDoc {
    /// This document with its ABA discriminator advanced.
    #[must_use]
    pub fn bumped(mut self) -> Self {
        self.seq = self.seq.wrapping_add(1);
        self
    }

    /// This group with each member's last contact the later of the document's
    /// and its newest stamp in `stamps` (#627).
    ///
    /// The document's own `last_contact_ms` is only written when the document
    /// is, which a stable group no longer does; a member renews its liveness
    /// with a stamp of its own. Every reader that judges liveness — describe,
    /// list, delete — reads the group through this.
    #[must_use]
    pub fn with_contacts(mut self, stamps: &ContactStamps) -> Self {
        for (member_id, member) in &mut self.members {
            if let Some(latest) = stamps.get(member_id).and_then(|stamps| stamps.last()) {
                member.last_contact_ms = member.last_contact_ms.max(*latest);
            }
        }

        self
    }

    /// The members whose sessions have not lapsed at `now_ms`.
    pub fn live_members(&self, now_ms: i64) -> impl Iterator<Item = (&String, &ConsumerMember)> {
        self.members
            .iter()
            .filter(move |(_, member)| !member.is_expired(now_ms))
    }

    /// Whether the group has a member whose session has not lapsed.
    #[must_use]
    pub fn has_live_members(&self, now_ms: i64) -> bool {
        self.live_members(now_ms).next().is_some()
    }

    /// The group's state as Kafka names it for a consumer group, with members
    /// the clock has condemned left out — as the classic view does (#523).
    #[must_use]
    pub fn state(&self, now_ms: i64) -> ConsumerGroupState {
        if !self.has_live_members(now_ms) {
            ConsumerGroupState::Empty
        } else if self.group_epoch > self.assignment_epoch {
            ConsumerGroupState::Assigning
        } else if self.live_members(now_ms).any(|(_, member)| {
            member.state != MemberState::Stable
                || (member.member_epoch != self.assignment_epoch && !member.is_leaving_static())
        }) {
            ConsumerGroupState::Reconciling
        } else {
            ConsumerGroupState::Stable
        }
    }

    fn described_partitions(&self, partitions: &Partitions) -> Assignment {
        Assignment::default().topic_partitions(Some(
            partitions
                .iter()
                .map(|(topic_id, partitions)| {
                    TopicPartitions::default()
                        .topic_id(topic_id.into_bytes())
                        .topic_name(
                            self.topics
                                .get(topic_id)
                                .map(|topic| topic.name.clone())
                                .unwrap_or_default(),
                        )
                        .partitions(Some(partitions.iter().copied().collect()))
                })
                .collect(),
        ))
    }

    /// This group as `ConsumerGroupDescribe` reports it.
    #[must_use]
    pub fn described(&self, group_id: &str, now_ms: i64) -> DescribedGroup {
        let members = self
            .live_members(now_ms)
            .map(|(member_id, member)| {
                Member::default()
                    .member_id(member_id.clone())
                    .instance_id(member.instance_id.clone())
                    .rack_id(member.rack_id.clone())
                    .member_epoch(member.member_epoch)
                    .client_id(member.client_id.clone())
                    .client_host(member.client_host.clone())
                    .subscribed_topic_names(Some(member.subscribed_topic_names.clone()))
                    .subscribed_topic_regex(member.subscribed_topic_regex.clone())
                    .member_type(Some(CONSUMER_MEMBER_TYPE))
                    .assignment(self.described_partitions(&member.assigned))
                    .target_assignment(self.described_partitions(&member.target))
            })
            .collect();

        DescribedGroup::default()
            .error_code(0)
            .error_message(None)
            .group_id(group_id.into())
            .group_state(self.state(now_ms).to_string())
            .group_epoch(self.group_epoch)
            .assignment_epoch(self.assignment_epoch)
            .assignor_name(self.assignor.clone().unwrap_or_default())
            .members(Some(members))
            .authorized_operations(-2_147_483_648)
    }
}

/// Every partition of `partitions`, as `(topic, partition)` pairs.
pub fn flatten(partitions: &Partitions) -> BTreeSet<(Uuid, i32)> {
    partitions
        .iter()
        .flat_map(|(topic_id, partitions)| {
            partitions
                .iter()
                .map(move |partition| (*topic_id, *partition))
        })
        .collect()
}

/// `(topic, partition)` pairs grouped back by topic.
pub fn grouped<I>(pairs: I) -> Partitions
where
    I: IntoIterator<Item = (Uuid, i32)>,
{
    pairs
        .into_iter()
        .fold(Partitions::new(), |mut grouped, (topic_id, partition)| {
            _ = grouped.entry(topic_id).or_default().insert(partition);
            grouped
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(epoch: i32, last_contact_ms: i64) -> ConsumerMember {
        ConsumerMember {
            member_epoch: epoch,
            session_timeout_ms: CONSUMER_SESSION_TIMEOUT_MS,
            last_contact_ms,
            ..Default::default()
        }
    }

    #[test]
    fn a_consumer_group_round_trips_and_keeps_unmodelled_fields() -> serde_json::Result<()> {
        let topic = Uuid::from_u128(7);

        let mut doc = ConsumerGroupDoc {
            group_epoch: 3,
            assignment_epoch: 3,
            ..Default::default()
        };

        let mut m = member(3, 1_000);
        m.assigned = grouped([(topic, 0), (topic, 2)]);
        _ = doc.members.insert("m-1".into(), m);
        _ = doc.topics.insert(
            topic,
            SubscribedTopic {
                name: "t".into(),
                partitions: 3,
            },
        );

        let mut encoded = serde_json::to_value(&doc)?;
        encoded["future"] = serde_json::json!(true);

        let decoded: ConsumerGroupDoc = serde_json::from_value(encoded)?;
        assert_eq!(doc.members, decoded.members);
        assert_eq!(doc.topics, decoded.topics);
        assert_eq!(Some(&serde_json::json!(true)), decoded.rest.get("future"));

        Ok(())
    }

    #[test]
    fn the_state_follows_the_epochs_and_the_clock() {
        let mut doc = ConsumerGroupDoc {
            group_epoch: 2,
            assignment_epoch: 2,
            ..Default::default()
        };

        assert_eq!(ConsumerGroupState::Empty, doc.state(0));

        _ = doc.members.insert("m-1".into(), member(2, 0));
        assert_eq!(ConsumerGroupState::Stable, doc.state(0));

        _ = doc.members.insert("m-2".into(), member(1, 0));
        assert_eq!(ConsumerGroupState::Reconciling, doc.state(0));

        doc.group_epoch = 3;
        assert_eq!(ConsumerGroupState::Assigning, doc.state(0));

        let lapsed = i64::from(CONSUMER_SESSION_TIMEOUT_MS) * 2;
        assert_eq!(ConsumerGroupState::Empty, doc.state(lapsed));
    }

    #[test]
    fn flatten_and_grouped_are_inverse() {
        let partitions = grouped([
            (Uuid::from_u128(1), 0),
            (Uuid::from_u128(1), 1),
            (Uuid::from_u128(2), 4),
        ]);

        assert_eq!(partitions, grouped(flatten(&partitions)));
    }
}
