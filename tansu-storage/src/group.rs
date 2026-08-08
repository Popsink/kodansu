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

//! The decomposed consumer group objects (#359).
//!
//! Today a group is one object — `groups/consumers/{group}.json`, a
//! [`GroupDetail`](crate::GroupDetail) — carrying four things with incompatible
//! write regimes behind a single etag: per-member liveness (roughly one
//! write-worthy event per second per member), each member's subscription, the
//! group's generation and leader, and the leader's large assignment map.
//! Liveness churn structurally starves the assignment CAS, which is what group
//! forwarding and the per-group in-process lock exist to compensate for.
//!
//! This module holds the replacement, three objects under
//! `groups/consumers/{group}/`, each with a single write regime:
//!
//! | Object | Control | Written by |
//! |---|---|---|
//! | `members/{member_id}.json` → [`MemberDoc`] | CAS, one *logical* writer | the member itself, rate-limited to once per session/2 |
//! | `generation.json` → [`GenerationDoc`] | etag CAS, rare | composition changes only |
//! | `assignment/{generation_id:0>10}.json` → [`AssignmentDoc`] | create-only, immutable | once per generation, by the leader |
//!
//! Committed offsets are untouched by the split: they already live in their own
//! per-partition objects under `groups/consumers/{group}/offsets/`.
//!
//! # Schema evolution
//!
//! The two CAS'd documents are read-modify-written by whichever binary happens
//! to serve the request, so during a rolling deploy an older process rewrites
//! objects a newer one has just written. Both therefore carry a
//! `#[serde(flatten)]` catch-all (`rest`) so an unmodelled field round-trips
//! instead of being erased — the same mechanism, and the same reasoning, as
//! `Watermark` (#182). [`AssignmentDoc`] needs none: it is immutable, so no
//! process ever rewrites one.
//!
//! The rules a new field must follow, pinned by the guard tests below:
//!
//! - it is `Option` + `#[serde(default, skip_serializing_if = "Option::is_none")]`,
//!   so a fleet not using it does not have every object rewritten (and
//!   etag-churned) on first touch, and an object written before the field
//!   existed still parses;
//! - it is *not* `#[serde(default)]` if reading a wrong value silently would
//!   be worse than failing — `seq` and `generation_id` are load-bearing enough
//!   that a missing key must be an error, not a zero.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tansu_sans_io::join_group_response::JoinGroupResponseMember;

use crate::{ConsumerGroupState, Version};

/// A member's own object, `members/{member_id}.json`.
///
/// One logical writer — the member — so this is the object that absorbs
/// liveness churn without touching anything another request needs to CAS. It
/// is still written under CAS, because that one logical writer can have two
/// requests in flight on two replicas at once.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct MemberDoc {
    /// ABA discriminator: incremented on every rewrite.
    ///
    /// A **counter, not a clock**. On S3 the etag is the body's MD5, so a
    /// rewrite that happens to reproduce an earlier body reproduces its etag,
    /// and a CAS that should have been rejected succeeds. Two writes in the
    /// same millisecond, or a clock that pauses or steps backwards, can
    /// reproduce a body; a counter cannot.
    pub seq: u64,

    /// Epoch milliseconds, so the value is comparable across replicas that
    /// each have their own idea of monotonic time.
    pub last_contact_ms: i64,

    pub session_timeout_ms: i32,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebalance_timeout_ms: Option<i32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_instance_id: Option<String>,

    /// The subscription the member joined with, in the shape the group's
    /// leader is handed at `SyncGroup` and `DescribeGroups` reports.
    pub join_response: JoinGroupResponseMember,

    #[serde(flatten)]
    pub rest: BTreeMap<String, serde_json::Value>,
}

impl MemberDoc {
    /// This document with its ABA discriminator advanced, ready to be written
    /// back. Wrapping is deliberate: a saturating counter would stop
    /// discriminating after `u64::MAX` writes, which is the one thing this
    /// field exists to prevent. At the design's ceiling of one member write
    /// per 22.5s that is roughly 1.3e13 years.
    #[must_use]
    pub fn bumped(mut self) -> Self {
        self.seq = self.seq.wrapping_add(1);
        self
    }

    /// Whether this member's session has lapsed at `now_ms` — the whole of the
    /// dead-member sweep's verdict, as a pure function of the document and the
    /// clock, so that N replicas evaluating it concurrently reach identical
    /// conclusions and their racing CASes are interchangeable.
    ///
    /// Strictly greater: a member last heard from exactly `session_timeout_ms`
    /// ago is still live, matching the broker's existing comparison.
    #[must_use]
    pub fn is_expired(&self, now_ms: i64) -> bool {
        now_ms.saturating_sub(self.last_contact_ms) > i64::from(self.session_timeout_ms)
    }
}

/// A member as seen from [`GenerationDoc::members`].
///
/// The generation carries the member *set* — not the member documents — so a
/// fan-out over the group's members needs no LIST, and a static member's id
/// resolves without reading every document.
#[derive(Clone, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct MemberRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_instance_id: Option<String>,
}

/// The group's composition, `generation.json`.
///
/// The only object multiple writers contend on, and it changes only when the
/// group's composition does. There is deliberately **no `state` field**: the
/// externally-tagged `GroupState` made every new variant a format break, and
/// the state is derivable — see [`GenerationDoc::state`].
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct GenerationDoc {
    /// ABA discriminator, incremented on **every** CAS — including the ones
    /// that do not change the generation, such as healing an orphaned leader
    /// or stamping a sweep. See [`MemberDoc::seq`].
    pub seq: u64,

    /// Invariant: **never regresses and is never reused**, including across
    /// the group emptying and re-forming. `assignment/{generation_id}` is
    /// create-only, so a reused generation would adopt a dead generation's
    /// immutable assignment.
    ///
    /// Every CAS that changes `members` bumps this (a join adding a member, a
    /// leave, an eviction, a change of subscribed topics); CASes that do not
    /// bump only `seq`.
    pub generation_id: i32,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_name: Option<String>,

    /// `None` means the group has no leader yet — either it is forming, or a
    /// leader left and the next join heals it by electing itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader: Option<String>,

    pub members: BTreeMap<String, MemberRef>,

    /// When `members` last changed, and so the base of the join window: a
    /// replica declares the window quiesced from this value alone, which is
    /// why every replica reaches the same verdict.
    pub members_changed_at_ms: i64,

    /// When the group entered its current derived state. Persisted so that
    /// rebalance-stall reporting (#240) survives a broker restart instead of
    /// starting blind.
    pub state_since_ms: i64,

    /// When a dead-member sweep last ran, so that N replicas racing to sweep
    /// the same group cost one sweep per session/2 globally rather than one
    /// per replica.
    pub swept_at_ms: i64,

    pub session_timeout_ms: i32,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebalance_timeout_ms: Option<i32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_assignment: Option<bool>,

    pub inception_ms: i64,

    #[serde(flatten)]
    pub rest: BTreeMap<String, serde_json::Value>,
}

impl GenerationDoc {
    /// This document with its ABA discriminator advanced. See
    /// [`MemberDoc::bumped`].
    #[must_use]
    pub fn bumped(mut self) -> Self {
        self.seq = self.seq.wrapping_add(1);
        self
    }

    /// The state to report for this group, derived rather than stored.
    ///
    /// `assignment_present` is whether `assignment/{generation_id}` exists.
    /// Deriving `Stable` from the existence of an immutable object costs one
    /// CAS less per rebalance and cannot tear: existence is monotonic within a
    /// generation, and once observed it can be memoized forever.
    ///
    /// Mirrors `ConsumerGroupState::from(&GroupDetail)` exactly, so the
    /// decomposition is not a behaviour change for `DescribeGroups`.
    #[must_use]
    pub fn state(&self, assignment_present: bool) -> ConsumerGroupState {
        if self.members.is_empty() {
            ConsumerGroupState::Empty
        } else if self.leader.is_none() {
            ConsumerGroupState::Assigning
        } else if assignment_present {
            ConsumerGroupState::Stable
        } else {
            ConsumerGroupState::CompletingRebalance
        }
    }
}

/// A generation's assignment, `assignment/{generation_id:0>10}.json`.
///
/// Written create-only, exactly once, by the leader of that generation, and
/// never rewritten — which is what takes the starved write off the contended
/// etag entirely: there is no etag to lose a race on, only a key that one
/// writer wins. Immutability is also why this document needs no catch-all and
/// no `seq`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AssignmentDoc {
    /// Echo of the generation this assignment belongs to. Redundant with the
    /// object's key, and deliberately so: it pins document to generation when
    /// the two are compared.
    pub generation_id: i32,
    pub leader: String,
    pub protocol_type: String,
    pub protocol_name: String,
    pub assignments: BTreeMap<String, Bytes>,
    pub assigned_at_ms: i64,
}

/// What a create-only write of `assignment/{generation_id}` did.
///
/// `AlreadyExists` is not an error: one leader per generation is guaranteed by
/// the generation CAS, so the only writer that can find the object already
/// there is that same leader retrying. Adopting what is stored — rather than
/// failing, or overwriting — is what makes `SyncGroup` idempotent under retry.
#[derive(Clone, Debug, PartialEq)]
pub enum AssignmentOutcome {
    Created(Version),
    AlreadyExists(Box<AssignmentDoc>),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn join_response(member_id: &str) -> JoinGroupResponseMember {
        JoinGroupResponseMember::default()
            .member_id(member_id.into())
            .metadata(Bytes::from_static(b"ab"))
    }

    /// A member document with nothing optional set must serialise without the
    /// optional keys at all. `"rebalance_timeout_ms":null` in every object is
    /// not free: it is bytes on every write of every member of every group,
    /// and it makes any later default-value change invisible in the data.
    #[test]
    fn a_member_doc_has_no_keys_it_does_not_need() {
        assert_eq!(
            r#"{"seq":0,"last_contact_ms":1000,"session_timeout_ms":45000,"join_response":{"member_id":"m-1","group_instance_id":null,"metadata":[97,98]}}"#,
            serde_json::to_string(&MemberDoc {
                last_contact_ms: 1_000,
                session_timeout_ms: 45_000,
                join_response: join_response("m-1"),
                ..Default::default()
            })
            .unwrap()
        );

        assert_eq!(
            r#"{"seq":3,"last_contact_ms":1000,"session_timeout_ms":45000,"rebalance_timeout_ms":300000,"group_instance_id":"static-1","join_response":{"member_id":"m-1","group_instance_id":null,"metadata":[97,98]}}"#,
            serde_json::to_string(&MemberDoc {
                seq: 3,
                last_contact_ms: 1_000,
                session_timeout_ms: 45_000,
                rebalance_timeout_ms: Some(300_000),
                group_instance_id: Some("static-1".into()),
                join_response: join_response("m-1"),
                ..Default::default()
            })
            .unwrap()
        );
    }

    /// The same discipline for the contended object.
    #[test]
    fn a_generation_doc_has_no_keys_it_does_not_need() {
        assert_eq!(
            r#"{"seq":0,"generation_id":0,"members":{},"members_changed_at_ms":0,"state_since_ms":0,"swept_at_ms":0,"session_timeout_ms":45000,"inception_ms":0}"#,
            serde_json::to_string(&GenerationDoc {
                session_timeout_ms: 45_000,
                ..Default::default()
            })
            .unwrap()
        );

        assert_eq!(
            r#"{"seq":1,"generation_id":7,"protocol_type":"consumer","protocol_name":"range","leader":"m-1","members":{"m-1":{},"m-2":{"group_instance_id":"static-2"}},"members_changed_at_ms":5,"state_since_ms":5,"swept_at_ms":4,"session_timeout_ms":45000,"inception_ms":1}"#,
            serde_json::to_string(&GenerationDoc {
                seq: 1,
                generation_id: 7,
                protocol_type: Some("consumer".into()),
                protocol_name: Some("range".into()),
                leader: Some("m-1".into()),
                members: BTreeMap::from([
                    ("m-1".to_owned(), MemberRef::default()),
                    (
                        "m-2".to_owned(),
                        MemberRef {
                            group_instance_id: Some("static-2".into()),
                        },
                    ),
                ]),
                members_changed_at_ms: 5,
                state_since_ms: 5,
                swept_at_ms: 4,
                session_timeout_ms: 45_000,
                inception_ms: 1,
                ..Default::default()
            })
            .unwrap()
        );
    }

    /// The immutable document, pinned so the assignment blobs keep the
    /// encoding `GroupState::Formed` already gives them — a change here would
    /// silently make every previously written assignment unreadable, and there
    /// is no rewrite path for a create-only object.
    #[test]
    fn an_assignment_doc_encodes_its_blobs_as_byte_arrays() {
        assert_eq!(
            r#"{"generation_id":7,"leader":"m-1","protocol_type":"consumer","protocol_name":"range","assignments":{"m-1":[1,2],"m-2":[]},"assigned_at_ms":9}"#,
            serde_json::to_string(&AssignmentDoc {
                generation_id: 7,
                leader: "m-1".into(),
                protocol_type: "consumer".into(),
                protocol_name: "range".into(),
                assignments: BTreeMap::from([
                    ("m-1".to_owned(), Bytes::from_static(&[1, 2])),
                    ("m-2".to_owned(), Bytes::new()),
                ]),
                assigned_at_ms: 9,
            })
            .unwrap()
        );
    }

    /// Both CAS'd documents are read-modify-written by whichever binary serves
    /// the request, so a field an older process does not model must survive its
    /// rewrite. Without the catch-all a rolling deploy silently erases whatever
    /// the newer half of the fleet just wrote — the failure the `Watermark`
    /// catch-all (#182) exists to prevent, here for group membership.
    #[test]
    fn a_member_doc_preserves_fields_it_does_not_model() {
        let written_by_a_newer_binary = r#"{
            "seq": 4,
            "last_contact_ms": 1000,
            "session_timeout_ms": 45000,
            "join_response": {"member_id": "m-1", "group_instance_id": null, "metadata": [97, 98]},
            "client_host": "10.0.0.1",
            "future": {"nested": [1, 2, 3]}
        }"#;

        let doc: MemberDoc = serde_json::from_str(written_by_a_newer_binary).unwrap();
        assert_eq!(2, doc.rest.len());

        let round_tripped: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&doc.bumped()).unwrap()).unwrap();

        assert_eq!(Some(5), round_tripped["seq"].as_u64());
        assert_eq!(Some("10.0.0.1"), round_tripped["client_host"].as_str());
        assert_eq!(Some(3), round_tripped["future"]["nested"][2].as_i64());
    }

    #[test]
    fn a_generation_doc_preserves_fields_it_does_not_model() {
        let written_by_a_newer_binary = r#"{
            "seq": 4,
            "generation_id": 7,
            "leader": "m-1",
            "members": {"m-1": {}},
            "members_changed_at_ms": 5,
            "state_since_ms": 5,
            "swept_at_ms": 4,
            "session_timeout_ms": 45000,
            "inception_ms": 1,
            "assignor": "cooperative-sticky"
        }"#;

        let doc: GenerationDoc = serde_json::from_str(written_by_a_newer_binary).unwrap();
        assert_eq!(1, doc.rest.len());

        let round_tripped: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&doc.bumped()).unwrap()).unwrap();

        assert_eq!(Some(5), round_tripped["seq"].as_u64());
        assert_eq!(
            Some("cooperative-sticky"),
            round_tripped["assignor"].as_str()
        );
    }

    /// An object written before an optional field existed must still parse —
    /// which is what makes adding one a non-event. A missing `seq` or
    /// `generation_id` must not: defaulting either to zero would hand a CAS a
    /// discriminator it has already used, or resurrect a generation whose
    /// create-only assignment object exists. Failing the read is recoverable;
    /// succeeding with a wrong value is not.
    #[test]
    fn optional_fields_may_be_absent_and_load_bearing_ones_may_not() {
        let doc: MemberDoc = serde_json::from_str(
            r#"{"seq":1,"last_contact_ms":1000,"session_timeout_ms":45000,
                "join_response":{"member_id":"m-1","group_instance_id":null,"metadata":[97,98]}}"#,
        )
        .unwrap();
        assert_eq!(None, doc.rebalance_timeout_ms);
        assert_eq!(None, doc.group_instance_id);

        assert!(
            serde_json::from_str::<MemberDoc>(
                r#"{"last_contact_ms":1000,"session_timeout_ms":45000,
                    "join_response":{"member_id":"m-1","group_instance_id":null,"metadata":[97,98]}}"#,
            )
            .is_err()
        );

        assert!(
            serde_json::from_str::<GenerationDoc>(
                r#"{"seq":1,"members":{},"members_changed_at_ms":0,"state_since_ms":0,
                    "swept_at_ms":0,"session_timeout_ms":45000,"inception_ms":0}"#,
            )
            .is_err()
        );
    }

    /// The ABA hazard, at the type level: on S3 the etag is the body's MD5, so
    /// a CAS is only as safe as the body's uniqueness. A member renewing
    /// liveness twice within the same millisecond — or on a clock that stepped
    /// backwards — writes the same bytes twice unless something else differs.
    /// `seq` is that something.
    #[test]
    fn the_bytes_differ_on_every_rewrite_even_when_nothing_else_does() {
        let doc = MemberDoc {
            seq: 1,
            last_contact_ms: 1_000,
            session_timeout_ms: 45_000,
            join_response: join_response("m-1"),
            ..Default::default()
        };

        // Same content, same millisecond: identical bytes, and so an identical
        // etag — this is the hazard, not a nice-to-have.
        assert_eq!(
            serde_json::to_string(&doc).unwrap(),
            serde_json::to_string(&doc.clone()).unwrap()
        );

        // The discriminator is what breaks it.
        assert_ne!(
            serde_json::to_string(&doc).unwrap(),
            serde_json::to_string(&doc.clone().bumped()).unwrap()
        );

        let generation = GenerationDoc {
            seq: 1,
            session_timeout_ms: 45_000,
            ..Default::default()
        };

        assert_ne!(
            serde_json::to_string(&generation).unwrap(),
            serde_json::to_string(&generation.clone().bumped()).unwrap()
        );
    }

    /// The derived state must answer exactly what `GroupDetail` answers today,
    /// or the decomposition changes what every admin tool sees.
    #[test]
    fn the_state_is_derived_from_the_membership_the_leader_and_the_assignment() {
        let empty = GenerationDoc {
            session_timeout_ms: 45_000,
            ..Default::default()
        };
        assert_eq!(ConsumerGroupState::Empty, empty.state(false));
        // An assignment left behind by a generation the group has since
        // emptied does not make an empty group stable.
        assert_eq!(ConsumerGroupState::Empty, empty.state(true));

        let forming = GenerationDoc {
            members: BTreeMap::from([("m-1".to_owned(), MemberRef::default())]),
            ..empty.clone()
        };
        assert_eq!(ConsumerGroupState::Assigning, forming.state(false));

        let led = GenerationDoc {
            leader: Some("m-1".into()),
            ..forming.clone()
        };
        assert_eq!(ConsumerGroupState::CompletingRebalance, led.state(false));
        assert_eq!(ConsumerGroupState::Stable, led.state(true));
    }

    /// The sweep's verdict is a pure function of the document and the clock,
    /// which is what makes N replicas sweeping the same group concurrently
    /// safe: identical inputs, identical verdicts, and the first CAS to land
    /// makes the rest no-ops.
    #[test]
    fn a_lapsed_session_is_a_pure_function_of_the_clock() {
        let doc = MemberDoc {
            last_contact_ms: 1_000,
            session_timeout_ms: 45_000,
            join_response: join_response("m-1"),
            ..Default::default()
        };

        assert!(!doc.is_expired(1_000));
        // Exactly at the timeout is still live, matching today's comparison.
        assert!(!doc.is_expired(46_000));
        assert!(doc.is_expired(46_001));

        // A clock that steps backwards must not expire anyone: saturating
        // subtraction keeps the verdict at "live" rather than wrapping into a
        // large positive age.
        assert!(!doc.is_expired(0));
        assert!(!doc.is_expired(i64::MIN));
    }
}
