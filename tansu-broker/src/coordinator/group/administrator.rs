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

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::{Deref, Div as _, Mul},
    sync::{Arc, LazyLock, Mutex},
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use opentelemetry::{KeyValue, metrics::Counter};
use rand::{prelude::*, rng};
use tansu_sans_io::{
    Body, ErrorCode,
    consumer::{MemberAssignment, MemberMetadata},
    heartbeat_response::HeartbeatResponse,
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
    leave_group_request::MemberIdentity,
    leave_group_response::{LeaveGroupResponse, MemberResponse},
    offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    },
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
        OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
    sync_group_request::SyncGroupRequestAssignment,
    sync_group_response::SyncGroupResponse,
};
use tansu_storage::{
    GroupDetail, GroupMember, GroupState, OffsetCommitRequest, Storage, Topition, UpdateError,
    Version,
};
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::{Error, METER, Result};

use super::{Coordinator, OffsetCommit};

const PAUSE_MS: u128 = 3_000;

/// Join-window barrier: how long the persisted member set must stay unchanged
/// before a Forming group's leader is released from `Controller::join` with its
/// (now complete) member list. Kafka holds every JoinGroup — the leader above
/// all — inside a rebalance window (`InitialDelayedJoin` / `DelayedJoin`, with
/// `group.initial.rebalance.delay.ms` defaulting to 3s) so the leader computes
/// assignments for the full membership. This broker has no timer state, only
/// the shared group object, so the window is inferred instead: membership is
/// quiescent once the successive GET-first reads of the join loop have shown
/// the same member set for this long. Must exceed the CAS conflict backoff cap
/// (500ms + 50% jitter, see `cas_conflict_backoff`) plus the 1s long-poll
/// cadence, so a member mid-retry on another replica is not missed.
const JOIN_QUIESCENCE: Duration = Duration::from_secs(3);

/// After this many consecutive object-store CAS conflicts on a single group's
/// state object, log a warning: sustained conflicts mean several members of the
/// same group are landing on different replicas behind a shared endpoint (the
/// scenario mitigated client-side in #43).
const CAS_CONFLICT_WARN: u32 = 16;

/// Backoff before retrying a group-state write that lost the object-store CAS
/// race (another replica updated the same `{group}.json` first). Without it the
/// retry loop spins as fast as the store answers, hammering the shared object
/// and keeping racing replicas in lock-step. Exponential (5ms·2^n) capped at
/// 500ms, plus up to 50% jitter to desynchronise replicas. See #44.
fn cas_conflict_backoff(attempt: u32) -> Duration {
    let base_ms = 5u64.saturating_mul(1u64 << attempt.min(7)).min(500);
    let jitter = rng().random_range(0..=base_ms / 2);
    Duration::from_millis(base_ms + jitter)
}

/// Two persisted group projections represent the same rebalance state when they
/// agree on everything except each member's `last_contact` — the liveness
/// timestamp that `missed_heartbeat` refreshes to `now` on *every* dynamic-member
/// join/sync (`administrator.rs` `missed_heartbeat`). A waiting member long-polls
/// join/sync once a second; each poll bumps only its own `last_contact`, so an
/// unconditional PUT churns the single `{group}.json` etag ~once/sec/member. With
/// many members that churn structurally starves the one write that matters — the
/// leader's large SyncGroup assignment CAS — so the group never reaches `Stable`.
/// The join/sync loops use this to skip the PUT on a no-op poll (mirroring the
/// heartbeat GET-first skip in #111), leaving the etag still long enough for the
/// assignment write to land. `members` is a `BTreeMap`, so ordered `zip` is a
/// valid key/value comparison.
fn same_rebalance_state(a: &GroupDetail, b: &GroupDetail) -> bool {
    a.session_timeout_ms == b.session_timeout_ms
        && a.rebalance_timeout_ms == b.rebalance_timeout_ms
        && a.generation_id == b.generation_id
        && a.skip_assignment == b.skip_assignment
        && a.inception == b.inception
        && a.state == b.state
        && a.members.len() == b.members.len()
        && a.members
            .iter()
            .zip(b.members.iter())
            .all(|((ak, av), (bk, bv))| ak == bk && av.join_response == bv.join_response)
}

/// Whether `member_id`'s persisted `last_contact` (as seen in the just-read
/// projection `before`) is stale enough that a no-op poll must still persist a
/// refreshed timestamp, so a member waiting through a long rebalance is never
/// spuriously evicted by another replica. Bounded to half the session timeout —
/// well inside the eviction deadline — this fires at most once per
/// `session_timeout/2` per member, negligible next to the once-a-second churn it
/// replaces. An absent member or missing timestamp forces the write.
fn liveness_renewal_due(
    before: &GroupDetail,
    member_id: &str,
    now: SystemTime,
    session_timeout_ms: i32,
) -> bool {
    if member_id.is_empty() {
        return false;
    }
    match before
        .members
        .get(member_id)
        .and_then(|member| member.last_contact)
    {
        Some(last_contact) => {
            let elapsed = now.duration_since(last_contact).unwrap_or_default();
            let half = (u64::try_from(session_timeout_ms).unwrap_or(45_000)) / 2;
            elapsed >= Duration::from_millis(half)
        }
        None => true,
    }
}

static COORDINATOR_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_group_coordinator_requests")
        .with_description("consumer group coordinator requests")
        .build()
});

#[async_trait]
pub trait Group: Debug + Send {
    type JoinState;
    type SyncState;
    type HeartbeatState;
    type LeaveState;
    type OffsetCommitState;
    type OffsetFetchState;

    #[allow(clippy::too_many_arguments)]
    async fn join(
        self,
        now: SystemTime,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body);

    #[allow(clippy::too_many_arguments)]
    async fn sync(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body);

    async fn heartbeat(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body);

    async fn leave(
        self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body);

    #[allow(clippy::too_many_arguments)]
    async fn offset_commit(
        self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Body);

    async fn offset_fetch(
        self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body);
}

#[derive(Clone, Debug)]
pub enum Wrapper<O> {
    Forming(Inner<O, Forming>),
    Formed(Inner<O, Formed>),
}

impl<O> fmt::Display for Wrapper<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Wrapper::Forming(inner) => write!(f, "wrap: {}", inner),
            Wrapper::Formed(inner) => write!(f, "wrap: {}", inner),
        }
    }
}

impl<O> PartialEq for Wrapper<O> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Forming(sinner), Self::Forming(oinner)) => sinner == oinner,
            (Self::Formed(sinner), Self::Formed(oinner)) => sinner == oinner,
            _ => false,
        }
    }
}

impl<O> Hash for Wrapper<O>
where
    O: Storage,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Forming(inner) => {
                state.write_u8(3);
                inner.hash(state)
            }
            Self::Formed(inner) => {
                state.write_u8(5);
                inner.hash(state)
            }
        }
    }
}

impl<O> From<Inner<O, Forming>> for Wrapper<O>
where
    O: Storage,
{
    fn from(value: Inner<O, Forming>) -> Self {
        Self::Forming(value)
    }
}

impl<O> From<Inner<O, Formed>> for Wrapper<O>
where
    O: Storage,
{
    fn from(value: Inner<O, Formed>) -> Self {
        Self::Formed(value)
    }
}

impl<O> From<&Wrapper<O>> for GroupDetail
where
    O: Storage,
{
    fn from(value: &Wrapper<O>) -> Self {
        match value {
            Wrapper::Forming(Inner {
                session_timeout_ms,
                rebalance_timeout_ms,
                members,
                generation_id,
                state,
                skip_assignment,
                inception,
                ..
            }) => GroupDetail {
                session_timeout_ms: *session_timeout_ms,
                rebalance_timeout_ms: *rebalance_timeout_ms,
                members: members
                    .iter()
                    .map(|(id, member)| {
                        (
                            id.to_owned(),
                            GroupMember {
                                join_response: member.join_response.clone(),
                                last_contact: member.last_contact,
                            },
                        )
                    })
                    .collect(),
                generation_id: *generation_id,
                skip_assignment: *skip_assignment,
                inception: *inception,
                state: GroupState::Forming {
                    protocol_type: state.protocol_type.clone(),
                    protocol_name: state.protocol_name.clone(),
                    leader: state.leader.clone(),
                },
            },
            Wrapper::Formed(Inner {
                session_timeout_ms,
                rebalance_timeout_ms,
                members,
                generation_id,
                state,
                skip_assignment,
                inception,
                ..
            }) => GroupDetail {
                session_timeout_ms: *session_timeout_ms,
                rebalance_timeout_ms: *rebalance_timeout_ms,
                members: members
                    .iter()
                    .map(|(id, member)| {
                        (
                            id.to_owned(),
                            GroupMember {
                                join_response: member.join_response.clone(),
                                last_contact: member.last_contact,
                            },
                        )
                    })
                    .collect(),
                generation_id: *generation_id,
                skip_assignment: *skip_assignment,
                inception: *inception,
                state: GroupState::Formed {
                    protocol_type: state.protocol_type.clone(),
                    protocol_name: state.protocol_name.clone(),
                    leader: state.leader.clone(),
                    assignments: state.assignments.clone(),
                },
            },
        }
    }
}

impl<O> Wrapper<O>
where
    O: Storage,
{
    pub fn with_storage_group_detail(storage: O, gd: GroupDetail) -> Self {
        match gd.state {
            GroupState::Forming {
                protocol_type,
                protocol_name,
                mut leader,
            } => {
                if let Some(ref leader_id) = leader
                    && !gd
                        .members
                        .iter()
                        .any(|(member_id, _)| member_id == leader_id)
                {
                    _ = leader.take();
                }

                Self::Forming(Inner {
                    session_timeout_ms: gd.session_timeout_ms,
                    rebalance_timeout_ms: gd.rebalance_timeout_ms,
                    members: gd
                        .members
                        .iter()
                        .map(|(id, member)| {
                            (
                                id.to_owned(),
                                Member {
                                    join_response: member.join_response.clone(),
                                    last_contact: member.last_contact,
                                },
                            )
                        })
                        .collect(),
                    generation_id: gd.generation_id,
                    state: Forming {
                        protocol_type,
                        protocol_name,
                        leader,
                    },
                    storage,
                    skip_assignment: gd.skip_assignment,
                    inception: gd.inception,
                })
            }
            GroupState::Formed {
                protocol_type,
                protocol_name,
                leader,
                assignments,
            } => Self::Formed(Inner {
                session_timeout_ms: gd.session_timeout_ms,
                rebalance_timeout_ms: gd.rebalance_timeout_ms,
                members: gd
                    .members
                    .iter()
                    .map(|(id, member)| {
                        (
                            id.to_owned(),
                            Member {
                                join_response: member.join_response.clone(),
                                last_contact: member.last_contact,
                            },
                        )
                    })
                    .collect(),
                generation_id: gd.generation_id,
                state: Formed {
                    protocol_type,
                    protocol_name,
                    leader,
                    assignments,
                },
                storage,
                skip_assignment: gd.skip_assignment,
                inception: gd.inception,
            }),
        }
    }

    pub fn generation_id(&self) -> i32 {
        match self {
            Self::Forming(inner) => inner.generation_id,
            Self::Formed(inner) => inner.generation_id,
        }
    }

    pub fn inception(&self) -> SystemTime {
        match self {
            Self::Forming(inner) => inner.inception,
            Self::Formed(inner) => inner.inception,
        }
    }

    pub fn session_timeout_ms(&self) -> i32 {
        match self {
            Self::Forming(inner) => inner.session_timeout_ms,
            Self::Formed(inner) => inner.session_timeout_ms,
        }
    }

    pub fn rebalance_timeout_ms(&self) -> Option<i32> {
        match self {
            Self::Forming(inner) => inner.rebalance_timeout_ms,
            Self::Formed(inner) => inner.rebalance_timeout_ms,
        }
    }

    pub fn protocol_type(&self) -> Option<&str> {
        match self {
            Self::Forming(inner) => inner.state.protocol_type.as_deref(),
            Self::Formed(inner) => Some(inner.state.protocol_type.as_str()),
        }
    }

    pub fn protocol_name(&self) -> Option<&str> {
        match self {
            Self::Forming(inner) => inner.state.protocol_name.as_deref(),
            Self::Formed(inner) => Some(inner.state.protocol_name.as_str()),
        }
    }

    pub fn leader(&self) -> Option<&str> {
        match self {
            Self::Forming(inner) => inner.state.leader.as_deref(),
            Self::Formed(inner) => Some(inner.state.leader.as_str()),
        }
    }

    pub fn skip_assignment(&self) -> Option<&bool> {
        match self {
            Self::Forming(inner) => inner.skip_assignment.as_ref(),
            Self::Formed(inner) => inner.skip_assignment.as_ref(),
        }
    }

    fn members(&self) -> Vec<JoinGroupResponseMember> {
        match self {
            Self::Forming(inner) => inner
                .members
                .values()
                .cloned()
                .map(|member| member.join_response)
                .collect(),

            Self::Formed(inner) => inner
                .members
                .values()
                .cloned()
                .map(|member| member.join_response)
                .collect(),
        }
    }

    fn is_forming(&self) -> bool {
        matches!(self, Self::Forming(..))
    }

    fn assignments(&self) -> Option<BTreeMap<String, Bytes>> {
        match self {
            Self::Forming(..) => None,
            Self::Formed(inner) => Some(inner.state.assignments.clone()),
        }
    }

    #[instrument(skip(self, now))]
    fn missed_heartbeat(self, group_id: &str, member_id: &str, now: SystemTime) -> Self {
        match self {
            Self::Forming(mut inner) => {
                _ = inner.missed_heartbeat(group_id, member_id, now);
                Self::Forming(inner)
            }
            Self::Formed(mut inner) => {
                if inner.missed_heartbeat(group_id, member_id, now) {
                    info!("missed heartbeat in generation {}", inner.generation_id);

                    Self::Forming(Inner {
                        session_timeout_ms: inner.session_timeout_ms,
                        rebalance_timeout_ms: inner.rebalance_timeout_ms,
                        members: inner.members,
                        generation_id: inner.generation_id + 1,
                        state: Forming {
                            protocol_type: Some(inner.state.protocol_type),
                            protocol_name: Some(inner.state.protocol_name),
                            leader: None,
                        },
                        storage: inner.storage,
                        skip_assignment: inner.skip_assignment,
                        inception: inner.inception,
                    })
                } else {
                    Self::Formed(inner)
                }
            }
        }
    }

    fn is_leader(&self, body: &Body) -> bool {
        if let Body::JoinGroupResponse(response) = body
            && let JoinGroupResponse { member_id, .. } = response
        {
            self.leader().is_some_and(|leader| leader == member_id)
        } else {
            false
        }
    }

    #[instrument(skip(self), ret)]
    fn is_ok(&self, body: &Body) -> bool {
        match body {
            Body::SyncGroupResponse(SyncGroupResponse { error_code, .. })
            | Body::JoinGroupResponse(JoinGroupResponse { error_code, .. })
            | Body::HeartbeatResponse(HeartbeatResponse { error_code, .. }) => {
                *error_code == i16::from(ErrorCode::None)
            }

            otherwise => {
                warn!(?otherwise);
                false
            }
        }
    }

    #[instrument(skip(self), ret)]
    fn is_assigned(&self, body: &Body) -> bool {
        match body {
            Body::SyncGroupResponse(SyncGroupResponse {
                error_code,
                assignment,
                ..
            }) if *error_code == i16::from(ErrorCode::None) && !assignment.is_empty() => true,

            Body::JoinGroupResponse(JoinGroupResponse {
                member_id,
                error_code,
                ..
            }) if *error_code == i16::from(ErrorCode::None) => self
                .assignments()
                .is_some_and(|assignments| assignments.contains_key(member_id)),

            _ => false,
        }
    }

    #[instrument(skip(self), ret)]
    fn is_member_id_required(&self, body: &Body) -> bool {
        if let Body::JoinGroupResponse(response) = body
            && let JoinGroupResponse { error_code, .. } = response
            && *error_code == i16::from(ErrorCode::MemberIdRequired)
        {
            true
        } else {
            false
        }
    }
}

#[instrument(ret)]
fn set_error_code(body: Body, error_code: ErrorCode) -> Body {
    if let Body::SyncGroupResponse(mut sync) = body {
        sync.error_code = i16::from(error_code);
        sync.into()
    } else {
        unimplemented!("{body:?}")
    }
}

#[async_trait]
impl<O> Group for Wrapper<O>
where
    O: Storage,
{
    type JoinState = Wrapper<O>;
    type SyncState = Wrapper<O>;
    type HeartbeatState = Wrapper<O>;
    type LeaveState = Wrapper<O>;
    type OffsetCommitState = Wrapper<O>;
    type OffsetFetchState = Wrapper<O>;

    #[allow(clippy::too_many_arguments)]
    async fn join(
        self,
        now: SystemTime,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Self::Forming(inner) => {
                let (state, body) = inner
                    .join(
                        now,
                        client_id,
                        group_id,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocols,
                        reason,
                    )
                    .await;
                (state.into(), body)
            }

            Self::Formed(inner) => {
                let (state, body) = inner
                    .join(
                        now,
                        client_id,
                        group_id,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocols,
                        reason,
                    )
                    .await;
                (state, body)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn sync(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner
                    .sync(
                        now,
                        group_id,
                        generation_id,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocol_name,
                        assignments,
                    )
                    .await;
                (state, body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner
                    .sync(
                        now,
                        group_id,
                        generation_id,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocol_name,
                        assignments,
                    )
                    .await;
                (state.into(), body)
            }
        }
    }

    async fn leave(
        self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members).await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members).await;
                (state, body)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn offset_commit(self, now: SystemTime, detail: &OffsetCommit<'_>) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner.offset_commit(now, detail).await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner.offset_commit(now, detail).await;
                (state.into(), body)
            }
        }
    }

    async fn offset_fetch(
        self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner
                    .offset_fetch(now, group_id, topics, groups, require_stable)
                    .await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner
                    .offset_fetch(now, group_id, topics, groups, require_stable)
                    .await;
                (state.into(), body)
            }
        }
    }

    async fn heartbeat(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Wrapper<O>, Body) {
        debug!(
            ?now,
            ?group_id,
            ?generation_id,
            ?member_id,
            ?group_instance_id
        );

        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner
                    .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                    .await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner
                    .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                    .await;
                (state.into(), body)
            }
        }
    }
}

type WrapperMap<O> = Arc<Mutex<BTreeMap<String, (Wrapper<O>, Option<Version>)>>>;

#[derive(Clone, Debug)]
pub struct Controller<O> {
    storage: O,
    wrappers: WrapperMap<O>,
}

impl<O> Controller<O>
where
    O: Storage + Clone,
{
    pub fn with_storage(storage: O) -> Result<Self> {
        Ok(Self {
            storage,
            wrappers: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }
}

#[async_trait]
impl<O> Coordinator for Controller<O>
where
    O: Storage + Clone,
{
    #[instrument(skip(
        self,
        client_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        group_instance_id,
        protocol_type,
        protocols,
        reason
    ))]
    async fn join(
        &self,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> Result<Body> {
        debug!(
            ?client_id,
            ?session_timeout_ms,
            ?rebalance_timeout_ms,
            ?group_instance_id,
            ?protocol_type,
            protocols = ?protocols
                .map(|protocols| {
                    protocols
                        .iter()
                        .filter_map(|protocol| {
                            MemberMetadata::try_from(protocol.metadata.clone())
                                .ok()
                                .map(|metadata| (protocol.name.clone(), metadata.to_string()))
                        })
                        .collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_default(),
            ?reason,
        );

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join")]);

        let started_at = SystemTime::now();

        let mut iteration = 0;
        let mut cas_conflicts = 0u32;

        // Join-window barrier state (see `JOIN_QUIESCENCE`): the member set
        // last observed by this join call, and when it last changed. Inferred
        // purely from the per-iteration GET-first reads below — no on-disk
        // format change.
        let mut join_window_members: Option<BTreeSet<String>> = None;
        let mut join_window_changed_at = started_at;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_loop")]);

            let now = SystemTime::now();

            let (mut original, mut version) = self.wrappers.lock().map(|mut wrappers| {
                wrappers.remove(group_id).unwrap_or_else(|| {
                    debug!(?iteration, ?group_id);

                    let inner = Inner {
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        members: Default::default(),
                        generation_id: -1,
                        state: Forming::default(),
                        skip_assignment: Some(false),
                        storage: self.storage.clone(),
                        inception: SystemTime::now(),
                    };

                    (Wrapper::Forming(inner), None)
                })
            })?;

            // GET-first (#111 pattern, extended to join): observe a rebalance
            // started on another replica via a cheap read rather than learning it
            // only from a failed CAS, and evaluate this join against current
            // persisted state. `before` is that state — what an unconditional
            // loop iteration would otherwise rewrite with only a `last_contact`
            // bump. `persisted.is_some()` gates the no-op skip below: without a
            // persisted object there is nothing to be equal to, so the create
            // must go through.
            let persisted = self.storage.read_group(group_id).await?;
            if let Some((current, current_version)) = &persisted
                && version.as_ref() != Some(current_version)
            {
                original =
                    Wrapper::with_storage_group_detail(self.storage.clone(), current.clone());
                version = Some(current_version.clone());
            }

            let before = GroupDetail::from(&original);

            if group_instance_id.is_none() {
                original = original.missed_heartbeat(group_id, member_id, now);
            }

            debug!(%original, ?version, ?iteration);

            let original_members = original.members();

            let (updated, body) = original
                .join(
                    now,
                    client_id,
                    group_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    member_id,
                    group_instance_id,
                    protocol_type,
                    protocols,
                    reason,
                )
                .await;

            let is_stable = original_members == updated.members();

            debug!(%updated, ?version, iteration);

            let after = GroupDetail::from(&updated);

            // Join-window barrier: without it the leader returned as soon as
            // its own CAS landed, computed assignments for whatever partial
            // member list it had seen, and every member missing from that list
            // parked at "stable" with zero partitions — a multi-member group
            // never converged. Hold the leader (below) until the membership is
            // quiescent or the rebalance window closes.
            let members_now = after.members.keys().cloned().collect::<BTreeSet<_>>();
            if join_window_members.as_ref() != Some(&members_now) {
                join_window_members = Some(members_now);
                join_window_changed_at = now;
            }
            let membership_quiescent = now
                .duration_since(join_window_changed_at)
                .unwrap_or_default()
                >= JOIN_QUIESCENCE;

            // The rebalance window: `rebalance_timeout_ms` bounds how long the
            // leader may be held (the Java client allows JoinGroup responses up
            // to `rebalance_timeout + 5s`), falling back to the session timeout
            // when unset, as Kafka does for old protocol versions.
            let join_window_ms = u128::try_from(
                after
                    .rebalance_timeout_ms
                    .or(rebalance_timeout_ms)
                    .unwrap_or(after.session_timeout_ms),
            )
            .unwrap_or_default();

            let is_forming = updated.is_forming();

            // No-op long-poll skip: a member waiting through a rebalance re-joins
            // once a second, and each re-join changes only its own `last_contact`
            // (via `missed_heartbeat`). Persisting that is a CAS that churns the
            // `{group}.json` etag for nothing and, at scale, starves the leader's
            // assignment write so the group never stabilises. When the rebalance
            // state is otherwise unchanged and this member's liveness does not yet
            // need renewing, return without touching the object — keeping the etag
            // still long enough for the assignment CAS to land.
            if persisted.is_some()
                && same_rebalance_state(&before, &after)
                && !liveness_renewal_due(&before, member_id, now, before.session_timeout_ms)
            {
                COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_noop_skip")]);

                let elapsed_ms = started_at.elapsed().map(|duration| duration.as_millis())?;

                let is_leader = updated.is_leader(&body);
                let is_assigned = updated.is_assigned(&body);
                let is_member_id_required = updated.is_member_id_required(&body);
                let session_timeout_ms = updated.session_timeout_ms() as u128;

                _ = self
                    .wrappers
                    .lock()
                    .map(|mut wrappers| wrappers.insert(group_id.to_owned(), (updated, version)))?;

                if group_instance_id.is_some() && elapsed_ms < PAUSE_MS {
                    let pause = PAUSE_MS.saturating_sub(elapsed_ms);
                    COORDINATOR_REQUESTS
                        .add(1, &[KeyValue::new("method", "join_group_instance_pause")]);
                    sleep(Duration::from_millis(pause as u64)).await;

                    iteration += 1;
                    continue;
                } else if is_leader
                    && is_forming
                    && !membership_quiescent
                    && elapsed_ms < join_window_ms
                {
                    // Join-window barrier: the leader of a still-Forming group
                    // is held until membership is quiescent (or the window
                    // closes), so its join response carries the complete
                    // member list before it computes assignments.
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_window_hold")]);
                    sleep(Duration::from_secs(1)).await;

                    iteration += 1;
                    continue;
                } else if is_leader || is_assigned || is_member_id_required {
                    return Ok(body);
                } else if elapsed_ms < session_timeout_ms.div(2) {
                    COORDINATOR_REQUESTS
                        .add(1, &[KeyValue::new("method", "join_group_instance_pause")]);
                    sleep(Duration::from_secs(1)).await;

                    iteration += 1;
                    continue;
                } else {
                    return Ok(body);
                }
            }

            match self.storage.update_group(group_id, after, version).await {
                Ok(version) => {
                    let elapsed_ms = started_at.elapsed().map(|duration| duration.as_millis())?;

                    let is_leader = updated.is_leader(&body);
                    let is_assigned = updated.is_assigned(&body);
                    let is_member_id_required = updated.is_member_id_required(&body);
                    let is_ok = updated.is_ok(&body);

                    let session_timeout_ms = updated.session_timeout_ms() as u128;

                    debug!(
                        ?version,
                        iteration,
                        elapsed_ms,
                        ?is_forming,
                        ?is_leader,
                        ?is_assigned,
                        ?is_member_id_required,
                        ?is_ok,
                        is_stable
                    );

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(group_id.to_owned(), (updated, Some(version)))
                    })?;

                    if group_instance_id.is_some() && elapsed_ms < PAUSE_MS {
                        let pause = PAUSE_MS.saturating_sub(elapsed_ms);
                        debug!(pause);

                        COORDINATOR_REQUESTS
                            .add(1, &[KeyValue::new("method", "join_group_instance_pause")]);
                        sleep(Duration::from_millis(pause as u64)).await;

                        iteration += 1;
                        continue;
                    } else if is_leader
                        && is_forming
                        && !membership_quiescent
                        && elapsed_ms < join_window_ms
                    {
                        // Join-window barrier: see the no-op skip branch above.
                        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_window_hold")]);
                        sleep(Duration::from_secs(1)).await;

                        iteration += 1;
                        continue;
                    } else if is_leader || is_assigned || is_member_id_required {
                        return Ok(body);
                    } else if elapsed_ms < session_timeout_ms.div(2) {
                        COORDINATOR_REQUESTS
                            .add(1, &[KeyValue::new("method", "join_group_instance_pause")]);
                        sleep(Duration::from_secs(1)).await;

                        iteration += 1;
                        continue;
                    } else {
                        return Ok(body);
                    }
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?current, ?version, iteration);

                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_outdated")]);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(
                            group_id.to_owned(),
                            (
                                Wrapper::with_storage_group_detail(self.storage.clone(), *current),
                                Some(version),
                            ),
                        )
                    });

                    cas_conflicts += 1;
                    if cas_conflicts == CAS_CONFLICT_WARN {
                        warn!(
                            group_id,
                            cas_conflicts,
                            "join: repeated group-state CAS conflicts (concurrent members across replicas?)"
                        );
                    }
                    sleep(cas_conflict_backoff(cas_conflicts)).await;

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
            }
        }
    }

    #[instrument(skip(self, group_instance_id, protocol_type, protocol_name, assignments))]
    async fn sync(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body> {
        debug!(
            ?group_instance_id,
            ?protocol_type,
            ?protocol_name,
            assignments = ?assignments
                .map(|assignments| {
                    assignments
                        .iter()
                        .filter_map(|request| {
                            MemberAssignment::try_from(request.assignment.clone())
                                .ok()
                                .map(|member_assignment| {
                                    (request.member_id.clone(), member_assignment.to_string())
                                })
                        })
                        .collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_default(),
        );

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync")]);

        let started_at = SystemTime::now();
        let mut iteration = 0;
        let mut cas_conflicts = 0u32;

        if let Some(assignments) = assignments
            && !assignments.is_empty()
            && !has_unique_elements(
                assignments
                    .iter()
                    .map(|assignment| assignment.assignment.clone())
                    .filter_map(|assignment| MemberAssignment::try_from(assignment).ok())
                    .map(|ma| ma.assignment)
                    .map(|cpa| cpa.assigned_partitions)
                    .flat_map(|tp| tp.into_iter())
                    .map(|tp| (tp.topic, tp.partitions))
                    .flat_map(|(topic, partitions)| {
                        partitions
                            .into_iter()
                            .map(move |partition| (topic.clone(), partition))
                    }),
            )
        {
            warn!(
                group_id,
                generation_id,
                member_count = assignments.len(),
                non_unique_assignment = assignments
                    .iter()
                    .map(|assignment| (assignment.member_id.clone(), assignment.assignment.clone()))
                    .filter_map(|(member_id, assignment)| {
                        MemberAssignment::try_from(assignment)
                            .ok()
                            .map(|ma| (member_id, ma))
                    })
                    .map(|(member, ma)| format!("{member}: {ma}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync_loop")]);

            let now = SystemTime::now();

            let (mut original, mut version) = self.wrappers.lock().map(|mut wrappers| {
                wrappers
                    .remove(group_id)
                    .unwrap_or_else(|| (Wrapper::Forming(Inner::new(self.storage.clone())), None))
            })?;

            // GET-first (#111 pattern, extended to sync): see the join loop — a
            // waiting non-leader sync re-polls once a second and each poll bumps
            // only `last_contact`, so persisting it churns the etag and starves
            // the leader's assignment CAS. Read current state, then skip the PUT
            // below when nothing but `last_contact` would change.
            let persisted = self.storage.read_group(group_id).await?;
            if let Some((current, current_version)) = &persisted
                && version.as_ref() != Some(current_version)
            {
                original =
                    Wrapper::with_storage_group_detail(self.storage.clone(), current.clone());
                version = Some(current_version.clone());
            }

            let before = GroupDetail::from(&original);

            let original_members = original.members();

            debug!(?group_id, ?original, ?version, ?iteration);

            if group_instance_id.is_none() {
                original = original.missed_heartbeat(group_id, member_id, now);
            }

            let (updated, body) = original
                .sync(
                    now,
                    group_id,
                    generation_id,
                    member_id,
                    group_instance_id,
                    protocol_type,
                    protocol_name,
                    assignments,
                )
                .await;

            let is_stable = original_members == updated.members();

            debug!(group_id, %updated, ?version, iteration);

            let after = GroupDetail::from(&updated);

            // No-op long-poll skip (see the join loop for the full rationale): a
            // waiting sync that changes only `last_contact` must not rewrite the
            // group object. The leader's real Forming->Formed assignment sync
            // changes `state` + `assignments`, so `same_rebalance_state` is false
            // for it and it always takes the write path below.
            if persisted.is_some()
                && same_rebalance_state(&before, &after)
                && !liveness_renewal_due(&before, member_id, now, before.session_timeout_ms)
            {
                COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync_noop_skip")]);

                let elapsed_ms = started_at.elapsed().map(|duration| duration.as_millis())?;

                let is_forming = updated.is_forming();
                let is_assigned = updated.is_assigned(&body);
                let session_timeout_ms = updated.session_timeout_ms() as u128;

                _ = self
                    .wrappers
                    .lock()
                    .map(|mut wrappers| wrappers.insert(group_id.to_owned(), (updated, version)))?;

                if group_instance_id.is_some() && elapsed_ms < PAUSE_MS {
                    let pause = PAUSE_MS.saturating_sub(elapsed_ms);
                    COORDINATOR_REQUESTS
                        .add(1, &[KeyValue::new("method", "sync_group_instance_pause")]);
                    sleep(Duration::from_millis(pause as u64)).await;

                    iteration += 1;
                    continue;
                } else if is_forming || is_assigned {
                    return Ok(body);
                } else if elapsed_ms < session_timeout_ms.mul(8).div(10) {
                    COORDINATOR_REQUESTS
                        .add(1, &[KeyValue::new("method", "sync_group_instance_pause")]);
                    sleep(Duration::from_secs(1)).await;

                    iteration += 1;
                    continue;
                } else {
                    return Ok(set_error_code(body, ErrorCode::RebalanceInProgress));
                }
            }

            match self.storage.update_group(group_id, after, version).await {
                Ok(version) => {
                    let elapsed_ms = started_at.elapsed().map(|duration| duration.as_millis())?;

                    let is_forming = updated.is_forming();
                    let is_assigned = updated.is_assigned(&body);
                    let is_ok = updated.is_ok(&body);

                    debug!(
                        group_id,
                        ?version,
                        iteration,
                        elapsed_ms,
                        is_forming,
                        ?is_assigned,
                        ?is_ok,
                        is_stable
                    );

                    let session_timeout_ms = updated.session_timeout_ms() as u128;

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(group_id.to_owned(), (updated, Some(version)))
                    })?;

                    if group_instance_id.is_some() && elapsed_ms < PAUSE_MS {
                        let pause = PAUSE_MS.saturating_sub(elapsed_ms);
                        debug!(pause);

                        COORDINATOR_REQUESTS
                            .add(1, &[KeyValue::new("method", "sync_group_instance_pause")]);
                        sleep(Duration::from_millis(pause as u64)).await;

                        iteration += 1;
                        continue;
                    } else if is_forming || is_assigned {
                        return Ok(body);
                    } else if elapsed_ms < session_timeout_ms.mul(8).div(10) {
                        COORDINATOR_REQUESTS
                            .add(1, &[KeyValue::new("method", "sync_group_instance_pause")]);
                        sleep(Duration::from_secs(1)).await;

                        iteration += 1;
                        continue;
                    } else {
                        return Ok(set_error_code(body, ErrorCode::RebalanceInProgress));
                    }
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?group_id, ?current, ?version);
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync_outdated")]);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(
                            group_id.to_owned(),
                            (
                                Wrapper::with_storage_group_detail(self.storage.clone(), *current),
                                Some(version),
                            ),
                        )
                    })?;

                    cas_conflicts += 1;
                    if cas_conflicts == CAS_CONFLICT_WARN {
                        warn!(
                            group_id,
                            cas_conflicts,
                            "sync: repeated group-state CAS conflicts (concurrent members across replicas?)"
                        );
                    }
                    sleep(cas_conflict_backoff(cas_conflicts)).await;

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
            }
        }
    }

    #[instrument(skip(self, members))]
    async fn leave(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body> {
        debug!(?members);

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "leave")]);

        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "leave_loop")]);

            let (wrapper, version) = self.wrappers.lock().map(|mut wrappers| {
                wrappers
                    .remove(group_id)
                    .unwrap_or_else(|| (Wrapper::Forming(Inner::new(self.storage.clone())), None))
            })?;

            debug!(?group_id, ?wrapper, ?version, ?iteration);

            let now = SystemTime::now();
            let wrapper = wrapper.missed_heartbeat(group_id, member_id.unwrap_or_default(), now);

            let (wrapper, body) = wrapper.leave(now, group_id, member_id, members).await;
            debug!(group_id, ?wrapper, ?version, iteration,);

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    debug!(?group_id, ?version);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(group_id.to_owned(), (wrapper, Some(version)))
                    })?;

                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?group_id, ?current, ?version);
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "leave_outdated")]);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(
                            group_id.to_owned(),
                            (
                                Wrapper::with_storage_group_detail(self.storage.clone(), *current),
                                Some(version),
                            ),
                        )
                    })?;

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
            }
        }
    }

    #[instrument(skip(self, offset_commit), fields(group_id = offset_commit.group_id, generation_id = offset_commit.generation_id_or_member_epoch))]
    async fn offset_commit(&self, offset_commit: OffsetCommit<'_>) -> Result<Body> {
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_commit")]);

        let group_id = offset_commit.group_id;
        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_commit_loop")]);

            let (mut wrapper, mut version) = self.wrappers.lock().map(|mut wrappers| {
                wrappers
                    .remove(group_id)
                    .unwrap_or_else(|| (Wrapper::Forming(Inner::new(self.storage.clone())), None))
            })?;

            debug!(?group_id, ?wrapper, ?version, ?iteration);

            // GET-first (#111): observe a cross-replica change with a cheap read
            // and evaluate the commit against current state; committed offsets go
            // to their own per-topition objects inside `offset_commit` regardless.
            let persisted = self.storage.read_group(group_id).await?;
            if let Some((current, current_version)) = &persisted
                && version.as_ref() != Some(current_version)
            {
                wrapper = Wrapper::with_storage_group_detail(self.storage.clone(), current.clone());
                version = Some(current_version.clone());
            }

            let before = GroupDetail::from(&wrapper);

            let now = SystemTime::now();

            let (wrapper, body) = wrapper.offset_commit(now, &offset_commit).await;
            debug!(group_id, ?wrapper, ?version, iteration,);

            // Skip the redundant group-state PUT when the commit changed nothing
            // in `GroupDetail` (#111): the offsets were persisted to their own
            // objects, so re-writing the group object is pure overhead.
            if persisted.is_some() && GroupDetail::from(&wrapper) == before {
                _ = self
                    .wrappers
                    .lock()
                    .map(|mut wrappers| wrappers.insert(group_id.to_owned(), (wrapper, version)))?;
                return Ok(body);
            }

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    debug!(?group_id, ?version);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(group_id.to_owned(), (wrapper, Some(version)))
                    })?;

                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?group_id, ?current, ?version);
                    COORDINATOR_REQUESTS
                        .add(1, &[KeyValue::new("method", "offset_commit_outdated")]);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(
                            group_id.to_owned(),
                            (
                                Wrapper::with_storage_group_detail(self.storage.clone(), *current),
                                Some(version),
                            ),
                        )
                    })?;

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
            }
        }
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        debug!(?group_id, ?topics, ?groups, ?require_stable);

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_fetch")]);

        let wrapper = Wrapper::Forming(Inner::new(self.storage.clone()));

        let now = SystemTime::now();
        let (_wrapper, body) = wrapper
            .offset_fetch(now, group_id, topics, groups, require_stable)
            .await;
        Ok(body)
    }

    #[instrument(skip(self, group_instance_id))]
    async fn heartbeat(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body> {
        debug!(?group_id, ?generation_id, ?member_id, ?group_instance_id);
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "heartbeat")]);

        let mut iteration = 0;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "heartbeat_loop")]);

            let (mut wrapper, mut version) = self.wrappers.lock().map(|mut wrappers| {
                wrappers
                    .remove(group_id)
                    .unwrap_or_else(|| (Wrapper::Forming(Inner::new(self.storage.clone())), None))
            })?;

            debug!(?group_id, ?wrapper, ?version, ?iteration);

            // GET-first (#111): read the persisted group so a rebalance triggered
            // on another replica is observed with a cheap (tier-2) read rather
            // than an unconditional (tier-1) PUT. If it changed since we cached it
            // (a different version), adopt it so the heartbeat is evaluated
            // against current state — this is the cross-replica propagation the
            // unconditional PUT's `Outdated` path used to be the only source of.
            let persisted = self.storage.read_group(group_id).await?;
            if let Some((current, current_version)) = &persisted
                && version.as_ref() != Some(current_version)
            {
                wrapper = Wrapper::with_storage_group_detail(self.storage.clone(), current.clone());
                version = Some(current_version.clone());
            }

            // The persisted projection we are about to (maybe) rewrite.
            let before = GroupDetail::from(&wrapper);

            let now = SystemTime::now();

            if group_instance_id.is_none() {
                wrapper = wrapper.missed_heartbeat(group_id, member_id, now);
            }

            let (wrapper, body) = wrapper
                .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                .await;

            debug!(group_id, %wrapper, ?version, iteration,);

            // Skip the group-state PUT when nothing persistent changed (#111): the
            // object exists and already holds `before` (just read), so a
            // steady-state heartbeat — and a heartbeat that merely observed a
            // rebalance — writes zero tier-1 PUTs. Only a real membership /
            // generation change makes `before != after` and falls through to the
            // CAS below.
            if persisted.is_some() && GroupDetail::from(&wrapper) == before {
                _ = self
                    .wrappers
                    .lock()
                    .map(|mut wrappers| wrappers.insert(group_id.to_owned(), (wrapper, version)))?;
                return Ok(body);
            }

            match self
                .storage
                .update_group(group_id, GroupDetail::from(&wrapper), version)
                .await
            {
                Ok(version) => {
                    debug!(?group_id, ?version);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(group_id.to_owned(), (wrapper, Some(version)))
                    })?;

                    return Ok(body);
                }

                Err(UpdateError::Outdated { current, version }) => {
                    debug!(?group_id, ?current, ?version);
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "heartbeat_outdated")]);

                    _ = self.wrappers.lock().map(|mut wrappers| {
                        wrappers.insert(
                            group_id.to_owned(),
                            (
                                Wrapper::with_storage_group_detail(self.storage.clone(), *current),
                                Some(version),
                            ),
                        )
                    })?;

                    iteration += 1;
                    continue;
                }

                Err(UpdateError::Error(error)) => return Err(error.into()),

                Err(UpdateError::SerdeJson(error)) => return Err(error.into()),

                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from("missing e-tag")));
                }

                Err(UpdateError::Uuid(uuid)) => {
                    return Err(Error::Message(format!("uuid: {uuid}")));
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Forming {
    protocol_type: Option<String>,
    protocol_name: Option<String>,
    leader: Option<String>,
}

impl fmt::Display for Forming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Forming({}/{}/{})",
            self.protocol_type.as_deref().unwrap_or("?"),
            self.protocol_name.as_deref().unwrap_or("?"),
            self.leader.as_deref().unwrap_or("?")
        )
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Formed {
    protocol_type: String,
    protocol_name: String,
    leader: String,
    assignments: BTreeMap<String, Bytes>,
}

impl fmt::Display for Formed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Formed({}/{}/{}/[{}])",
            self.protocol_type,
            self.protocol_name,
            self.leader,
            self.assignments
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[derive(Clone, Debug)]
pub struct Inner<O, S> {
    session_timeout_ms: i32,
    rebalance_timeout_ms: Option<i32>,
    members: BTreeMap<String, Member>,
    generation_id: i32,
    state: S,
    storage: O,
    skip_assignment: Option<bool>,
    inception: SystemTime,
}

impl<O, S> fmt::Display for Inner<O, S>
where
    S: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "state: {}, generation: {}",
            self.state, self.generation_id
        )
    }
}

impl<O, S> PartialEq for Inner<O, S>
where
    S: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.session_timeout_ms == other.session_timeout_ms
            && self.rebalance_timeout_ms == other.rebalance_timeout_ms
            && self.members == other.members
            && self.generation_id == other.generation_id
            && self.state == other.state
            && self.skip_assignment == other.skip_assignment
            && self.inception == other.inception
    }
}

impl<O, S> Hash for Inner<O, S>
where
    S: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.session_timeout_ms.hash(state);
        self.rebalance_timeout_ms.hash(state);
        self.members.hash(state);
        self.generation_id.hash(state);
        self.state.hash(state);
        self.skip_assignment.hash(state);
        self.inception.hash(state);
    }
}

impl<O> Inner<O, PhantomData<Forming>>
where
    O: Storage,
{
    pub fn new(storage: O) -> Inner<O, Forming> {
        Inner {
            session_timeout_ms: Default::default(),
            rebalance_timeout_ms: Default::default(),
            members: Default::default(),
            generation_id: -1,
            state: Forming::default(),
            skip_assignment: Some(false),
            storage,
            inception: SystemTime::now(),
        }
    }
}

impl<O> Inner<O, Forming>
where
    O: Storage,
{
    #[instrument(skip(self, now))]
    fn missed_heartbeat(&mut self, group_id: &str, member_id: &str, now: SystemTime) -> bool {
        let original = self.members.len();

        if !member_id.is_empty() {
            _ = self
                .members
                .get_mut(member_id)
                .map(|member| member.last_contact.replace(now))
        }

        self.members.retain(|member_id, member| {
            member
                .last_contact
                .map(|last_contact| now.duration_since(last_contact).unwrap_or_default())
                .is_some_and(|duration| {
                    if duration.as_millis()
                        > u128::try_from(self.session_timeout_ms).unwrap_or(45_000)
                    {
                        if self
                            .state
                            .leader
                            .as_ref()
                            .is_some_and(|leader| leader == member_id)
                        {
                            info!(
                                "eviction of leader: {member_id}, in generation: {}, after {}ms",
                                self.generation_id,
                                duration.as_millis()
                            );

                            _ = self.state.leader.take();
                        } else {
                            info!(
                                "eviction of: {member_id}, in generation: {}, after {}ms",
                                self.generation_id,
                                duration.as_millis()
                            );
                        }

                        false
                    } else {
                        true
                    }
                })
        });

        original > self.members.len()
    }
}

impl<O> Inner<O, Formed>
where
    O: Storage,
{
    #[instrument(skip(self, now))]
    fn missed_heartbeat(&mut self, group_id: &str, member_id: &str, now: SystemTime) -> bool {
        let original = self.members.len();

        if !member_id.is_empty() {
            _ = self
                .members
                .get_mut(member_id)
                .map(|member| member.last_contact.replace(now))
        }

        self.members.retain(|member_id, member| {
            debug!(?member_id, ?member);

            member
                .last_contact
                .map(|last_contact| now.duration_since(last_contact).unwrap_or_default())
                .is_some_and(|duration| {
                    if duration.as_millis()
                        > u128::try_from(self.session_timeout_ms).unwrap_or(45_000)
                    {
                        info!(
                            "eviction of: {member_id}, in generation: {}, after {}ms",
                            self.generation_id,
                            duration.as_millis()
                        );

                        false
                    } else {
                        true
                    }
                })
        });

        original > self.members.len()
    }
}

impl<O, S> Inner<O, S>
where
    O: Storage,
    S: Debug,
{
    async fn fetch_offset(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        debug!(?group_id, ?topics, ?groups, ?require_stable);

        let topics = if let Some(topics) = topics {
            let topics: Vec<Topition> = topics
                .iter()
                .flat_map(|topic| {
                    topic
                        .partition_indexes
                        .as_ref()
                        .map(|partition_indexes| {
                            partition_indexes
                                .iter()
                                .map(|partition_index| {
                                    Topition::new(topic.name.clone(), *partition_index)
                                })
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .collect();

            self.storage
                .offset_fetch(group_id, topics.deref(), require_stable)
                .await
                .map(|offsets| {
                    offsets
                        .iter()
                        .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                            _ = topics.insert(topition.topic());
                            topics
                        })
                        .iter()
                        .map(|topic_name| {
                            OffsetFetchResponseTopic::default()
                                .name((*topic_name).into())
                                .partitions(Some(
                                    offsets
                                        .iter()
                                        .filter_map(|(topition, offset)| {
                                            if topition.topic() == *topic_name {
                                                Some(
                                                    OffsetFetchResponsePartition::default()
                                                        .partition_index(topition.partition())
                                                        .committed_offset(*offset)
                                                        .committed_leader_epoch(Some(-1))
                                                        .metadata(Some("".into()))
                                                        .error_code(ErrorCode::None.into()),
                                                )
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                ))
                        })
                        .collect()
                })
                .map(Some)?
        } else {
            None
        };

        let groups = if let Some(groups) = groups {
            let mut responses = vec![];

            for group in groups {
                debug!(?group);

                let response = if let Some(topics) = group.topics.as_ref().map(|topics| {
                    topics
                        .iter()
                        .flat_map(|topic| {
                            topic
                                .partition_indexes
                                .as_ref()
                                .map(|partition_indexes| {
                                    partition_indexes
                                        .iter()
                                        .map(|partition_index| {
                                            Topition::new(topic.name.clone(), *partition_index)
                                        })
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default()
                        })
                        .collect::<Vec<_>>()
                }) {
                    self.storage
                        .offset_fetch(
                            Some(group.group_id.as_str()),
                            topics.deref(),
                            require_stable,
                        )
                        .await
                        .inspect(|offsets| debug!(?offsets))
                        .inspect_err(|err| error!(?err, ?group))
                } else {
                    self.storage
                        .committed_offset_topitions(&group.group_id)
                        .await
                        .inspect(|offsets| debug!(?offsets))
                        .inspect_err(|err| error!(?err, ?group))
                }
                .map(|offsets| {
                    OffsetFetchResponseGroup::default()
                        .group_id(group.group_id.clone())
                        .topics(Some(
                            offsets
                                .iter()
                                .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                                    _ = topics.insert(topition.topic());
                                    topics
                                })
                                .iter()
                                .map(|topic_name| {
                                    OffsetFetchResponseTopics::default()
                                        .name((*topic_name).into())
                                        .partitions(Some(
                                            offsets
                                                .iter()
                                                .filter_map(|(topition, offset)| {
                                                    if topition.topic() == *topic_name {
                                                        Some(
                                                            OffsetFetchResponsePartitions::default(
                                                            )
                                                            .partition_index(topition.partition())
                                                            .committed_offset(*offset)
                                                            .committed_leader_epoch(-1)
                                                            .metadata(None)
                                                            .error_code(ErrorCode::None.into()),
                                                        )
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect(),
                                        ))
                                })
                                .collect(),
                        ))
                        .error_code(ErrorCode::None.into())
                })?;

                responses.push(response);
            }

            Some(responses)
        } else {
            None
        };

        Ok(OffsetFetchResponse::default()
            .throttle_time_ms(Some(0))
            .topics(topics)
            .error_code(Some(ErrorCode::None.into()))
            .groups(groups)
            .into())
    }

    async fn commit_offset(&mut self, detail: &OffsetCommit<'_>) -> Result<Body> {
        let retention_time_ms = detail.retention_time_ms.map_or(Ok(None), |ms| {
            u64::try_from(ms)
                .map(Duration::from_millis)
                .map_err(Error::from)
                .map(Some)
        })?;

        if let Some(topics) = detail.topics {
            let mut offsets = vec![];

            for topic in topics {
                if let Some(ref partitions) = topic.partitions {
                    for partition in partitions {
                        let topition = Topition::new(topic.name.clone(), partition.partition_index);
                        let offset = OffsetCommitRequest::try_from(partition)?;

                        offsets.push((topition, offset));
                    }
                }
            }

            self.storage
                .offset_commit(detail.group_id, retention_time_ms, offsets.deref())
                .await
                .map(|value| {
                    let topics = value
                        .iter()
                        .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                            _ = topics.insert(topition.topic());
                            topics
                        })
                        .iter()
                        .map(|topic_name| {
                            OffsetCommitResponseTopic::default()
                                .name((*topic_name).into())
                                .partitions(Some(
                                    value
                                        .iter()
                                        .filter_map(|(topition, error_code)| {
                                            if topition.topic() == *topic_name {
                                                Some(
                                                    OffsetCommitResponsePartition::default()
                                                        .partition_index(topition.partition())
                                                        .error_code(i16::from(*error_code)),
                                                )
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                ))
                        })
                        .collect();

                    OffsetCommitResponse::default()
                        .throttle_time_ms(Some(0))
                        .topics(Some(topics))
                        .into()
                })
                .inspect_err(|err| error!(?err))
                .map_err(Into::into)
        } else {
            Ok(OffsetCommitResponse::default()
                .throttle_time_ms(Some(0))
                .topics(detail.topics.map(|topics| {
                    topics
                        .as_ref()
                        .iter()
                        .map(|topic| {
                            OffsetCommitResponseTopic::default()
                                .name(topic.name.clone())
                                .partitions(topic.partitions.as_ref().map(|partitions| {
                                    partitions
                                        .iter()
                                        .map(|partition| {
                                            OffsetCommitResponsePartition::default()
                                                .partition_index(partition.partition_index)
                                                .error_code(ErrorCode::UnknownMemberId.into())
                                        })
                                        .collect()
                                }))
                        })
                        .collect()
                }))
                .into())
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Member {
    join_response: JoinGroupResponseMember,
    last_contact: Option<SystemTime>,
}

#[async_trait::async_trait]
impl<O> Group for Inner<O, Forming>
where
    O: Storage,
{
    type JoinState = Inner<O, Forming>;
    type SyncState = Wrapper<O>;
    type HeartbeatState = Inner<O, Forming>;
    type LeaveState = Inner<O, Forming>;
    type OffsetCommitState = Inner<O, Forming>;
    type OffsetFetchState = Inner<O, Forming>;

    async fn join(
        mut self,
        now: SystemTime,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body) {
        debug!(
            client_id,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            ?protocols,
            reason
        );

        let Some(protocols) = protocols else {
            debug!(join_outcome = ?ErrorCode::InvalidRequest);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::InvalidRequest.into())
                .generation_id(self.generation_id)
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id("".into())
                .members(Some([].into()));

            return (self, join_group_response.into());
        };

        let protocol = if let Some(protocol_name) = self.state.protocol_name.as_deref() {
            debug!(protocol_name);

            if let Some(protocol) = protocols
                .iter()
                .find(|protocol| protocol.name == protocol_name)
            {
                debug!(?protocol);

                protocol
            } else {
                debug!(join_outcome = ?ErrorCode::InconsistentGroupProtocol);

                let join_group_response = JoinGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::InconsistentGroupProtocol.into())
                    .generation_id(self.generation_id)
                    .protocol_type(Some(protocol_type.into()))
                    .protocol_name(self.state.protocol_name.clone())
                    .leader("".into())
                    .skip_assignment(self.skip_assignment)
                    .member_id("".into())
                    .members(Some([].into()));

                return (self, join_group_response.into());
            }
        } else {
            self.state.protocol_type = Some(protocol_type.to_owned());
            self.state.protocol_name = Some(protocols[0].name.as_str().to_owned());

            self.session_timeout_ms = session_timeout_ms;
            self.rebalance_timeout_ms = rebalance_timeout_ms;

            &protocols[0]
        };

        if member_id.is_empty() && group_instance_id.is_none() {
            let member_id = if let Some(client_id) = client_id {
                format!("{client_id}-{}", Uuid::new_v4())
            } else {
                format!("{}", Uuid::new_v4())
            };
            debug!(?member_id, join_outcome = ?ErrorCode::MemberIdRequired);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::MemberIdRequired.into())
                .generation_id(-1)
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id(member_id)
                .members(Some([].into()));

            // KIP-394: reply with the generated member id but leave the group
            // untouched — the member only registers (and the generation only
            // moves) when it re-joins with this id. Registering a phantom
            // member here would rebalance the group for a client that may
            // never come back.
            return (self, join_group_response.into());
        }

        let member_id = group_instance_id.map_or(member_id.to_owned(), |group_instance_id| {
            if member_id.is_empty() {
                if let Some((member_id, _)) = self.members.iter().find(|(_, member)| {
                    member.join_response.group_instance_id.as_deref() == Some(group_instance_id)
                }) {
                    member_id.into()
                } else {
                    format!("{group_instance_id}-{}", Uuid::new_v4())
                }
            } else {
                member_id.into()
            }
        });

        debug!(?member_id, ?self.members);

        if let Some(member) = self.members.get_mut(&member_id) {
            if member.join_response.metadata == protocol.metadata {
                debug!(
                    member_metadata = "existing",
                    member_id,
                    generation_id = self.generation_id
                );
                member.last_contact = Some(now);
            } else if group_instance_id.is_some() {
                debug!(
                    member_metadata = "soft_update",
                    member_id,
                    group_instance_id,
                    updated = ?protocol.metadata,
                    existing = ?member.join_response.metadata,
                    generation_id = self.generation_id
                );

                member.join_response.metadata = protocol.metadata.clone();
                member.last_contact = Some(now);
            } else {
                self.generation_id += 1;

                debug!(
                    member_metadata = "update",
                    member_id,
                    updated = ?protocol.metadata,
                    existing = ?member.join_response.metadata,
                    generation_id = self.generation_id
                );

                member.join_response.metadata = protocol.metadata.clone();
                member.last_contact = Some(now);
            }
        } else {
            self.generation_id += 1;

            debug!(
                member_metadata = "new",
                member_id,
                generation_id = self.generation_id
            );

            _ = self.members.insert(
                member_id.clone(),
                Member {
                    join_response: JoinGroupResponseMember::default()
                        .member_id(member_id.to_string())
                        .group_instance_id(group_instance_id.map(|s| s.to_owned()))
                        .metadata(protocol.metadata.clone()),
                    last_contact: Some(now),
                },
            );
        }

        debug!(?member_id, ?self.members);

        if self.state.leader.is_none() {
            info!(member_id, group_id, self.generation_id);

            _ = self.state.leader.replace(member_id.clone());
        }

        let join_group_response = JoinGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .generation_id(self.generation_id)
            .protocol_type(self.state.protocol_type.clone())
            .protocol_name(self.state.protocol_name.clone())
            .leader(
                self.state
                    .leader
                    .as_ref()
                    .map_or(String::from(""), |leader| leader.clone()),
            )
            .skip_assignment(self.skip_assignment)
            .members(Some(
                if self
                    .state
                    .leader
                    .as_ref()
                    .is_some_and(|leader| leader == member_id.as_str())
                {
                    self.members
                        .values()
                        .cloned()
                        .map(|member| member.join_response)
                        .collect()
                } else {
                    [].into()
                },
            ))
            .member_id(member_id);

        debug!(join_outcome = ?ErrorCode::None);

        (self, join_group_response.into())
    }

    async fn sync(
        mut self,
        _now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body) {
        debug!(
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            ?assignments
        );

        if !self.members.contains_key(member_id) {
            debug!(?self.members, sync_outcome = ?ErrorCode::UnknownMemberId);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::UnknownMemberId.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        debug!(?member_id);

        if generation_id > self.generation_id {
            debug!(self.generation_id, sync_outcome = ?ErrorCode::IllegalGeneration);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::IllegalGeneration.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        if generation_id < self.generation_id {
            debug!(self.generation_id, sync_outcome = ?ErrorCode::RebalanceInProgress);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        if self
            .state
            .leader
            .as_ref()
            .is_some_and(|leader_id| member_id != leader_id.as_str())
        {
            debug!(?self.state.leader, sync_outcome = ?ErrorCode::RebalanceInProgress);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        let Some(assignments) = assignments else {
            debug!(sync_outcome = ?ErrorCode::RebalanceInProgress);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        };

        let assignments = assignments
            .iter()
            .inspect(|assignment| {
                debug!(
                    member_id = assignment.member_id,
                    assignment = MemberAssignment::try_from(assignment.assignment.clone())
                        .map(|assignment| assignment.to_string())
                        .unwrap_or_default()
                )
            })
            .map(|assignment| (assignment.member_id.clone(), assignment.assignment.clone()))
            .collect::<BTreeMap<_, _>>();

        // A sync whose assignments do not cover the syncing member must not
        // form the group: answering `error=None` with an empty assignment
        // parks the client at "stable" with zero partitions. Rebalance instead
        // so the member re-joins and a complete assignment is computed.
        if !assignments.contains_key(member_id) {
            debug!(?self.members, sync_outcome = ?ErrorCode::RebalanceInProgress);

            let sync_group_response = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(self.state.protocol_type.clone())
                .protocol_name(self.state.protocol_name.clone())
                .assignment(Bytes::from_static(b""));

            return (self.into(), sync_group_response.into());
        }

        let sync_group_response = SyncGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .protocol_type(self.state.protocol_type.clone())
            .protocol_name(self.state.protocol_name.clone())
            .assignment(
                assignments
                    .get(member_id)
                    .cloned()
                    .unwrap_or(Bytes::from_static(b"")),
            );

        debug!(sync_outcome = ?ErrorCode::None, sync_assignment = assignments.contains_key(member_id));

        let state = Inner {
            session_timeout_ms: self.session_timeout_ms,
            rebalance_timeout_ms: self.rebalance_timeout_ms,

            members: self.members,
            generation_id: self.generation_id,
            state: Formed {
                protocol_name: self.state.protocol_name.expect("protocol_name"),
                protocol_type: self.state.protocol_type.expect("protocol_type"),
                leader: member_id.to_owned(),
                assignments,
            },
            storage: self.storage,
            skip_assignment: self.skip_assignment,
            inception: self.inception,
        };

        (state.into(), sync_group_response.into())
    }

    async fn heartbeat(
        mut self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body) {
        debug!(
            ?now,
            ?group_id,
            ?generation_id,
            ?member_id,
            ?group_instance_id
        );

        let _ = group_instance_id;

        if !self.members.contains_key(member_id) {
            debug!(?self.members);

            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::UnknownMemberId.into())
                    .into(),
            );
        }

        if generation_id > self.generation_id {
            debug!(?self.generation_id);

            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::IllegalGeneration.into())
                    .into(),
            );
        }

        _ = self
            .members
            .entry(member_id.to_owned())
            .and_modify(|member| _ = member.last_contact.replace(now));

        if self.missed_heartbeat(group_id, member_id, now) || (generation_id < self.generation_id) {
            debug!(self.generation_id);

            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::RebalanceInProgress.into())
                    .into(),
            );
        }

        let body = HeartbeatResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .into();

        (self, body)
    }

    async fn leave(
        mut self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body) {
        let _ = now;
        debug!(?group_id, member_id, ?members);

        let members = if let Some(member_id) = member_id {
            debug!(member_id);

            vec![
                MemberResponse::default()
                    .member_id(member_id.to_owned())
                    .group_instance_id(None)
                    .error_code({
                        if self.members.remove(member_id).is_some() {
                            ErrorCode::None.into()
                        } else {
                            ErrorCode::UnknownMemberId.into()
                        }
                    }),
            ]
        } else {
            members.map_or(vec![], |members| {
                members
                    .iter()
                    .map(|member| {
                        MemberResponse::default()
                            .member_id(member.member_id.clone())
                            .group_instance_id(member.group_instance_id.clone())
                            .error_code({
                                if self.members.remove(&member.member_id).is_some() {
                                    ErrorCode::None.into()
                                } else {
                                    ErrorCode::UnknownMemberId.into()
                                }
                            })
                    })
                    .collect::<Vec<MemberResponse>>()
            })
        };

        if members.iter().any(|member| {
            let error_code = i16::from(ErrorCode::None);

            member.error_code == error_code
        }) {
            self.generation_id += 1;
        }

        let body = LeaveGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .members(Some(members))
            .into();

        (self, body)
    }

    async fn offset_commit(
        mut self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Body) {
        let _ = now;
        debug!(?detail);

        match self.commit_offset(detail).await {
            Ok(body) => (self, body),
            Err(reason) => {
                debug!(?reason);
                (
                    self,
                    OffsetCommitResponse::default()
                        .throttle_time_ms(Some(0))
                        .topics(detail.topics.map(|topics| {
                            topics
                                .as_ref()
                                .iter()
                                .map(|topic| {
                                    OffsetCommitResponseTopic::default()
                                        .name(topic.name.clone())
                                        .partitions(topic.partitions.as_ref().map(|partitions| {
                                            partitions
                                                .iter()
                                                .map(|partition| {
                                                    OffsetCommitResponsePartition::default()
                                                        .partition_index(partition.partition_index)
                                                        .error_code(
                                                            ErrorCode::UnknownMemberId.into(),
                                                        )
                                                })
                                                .collect()
                                        }))
                                })
                                .collect()
                        }))
                        .into(),
                )
            }
        }
    }

    async fn offset_fetch(
        mut self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body) {
        let _ = now;
        debug!(group_id, ?topics, ?groups, ?require_stable);
        match self
            .fetch_offset(group_id, topics, groups, require_stable)
            .await
        {
            Ok(body) => (self, body),
            Err(error) => {
                debug!(?error);
                todo!()
            }
        }
    }
}

#[async_trait::async_trait]
impl<O> Group for Inner<O, Formed>
where
    O: Storage,
{
    type JoinState = Wrapper<O>;
    type SyncState = Inner<O, Formed>;
    type HeartbeatState = Inner<O, Formed>;
    type LeaveState = Wrapper<O>;
    type OffsetCommitState = Inner<O, Formed>;
    type OffsetFetchState = Inner<O, Formed>;

    async fn join(
        mut self,
        now: SystemTime,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> (Self::JoinState, Body) {
        debug!(
            client_id,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            ?protocols,
            reason
        );

        let Some(protocols) = protocols else {
            debug!(join_outcome = ?ErrorCode::InvalidRequest);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::InvalidRequest.into())
                .generation_id(self.generation_id)
                .protocol_type(Some(protocol_type.into()))
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id("".into())
                .members(Some([].into()));

            return (self.into(), join_group_response.into());
        };

        let Some(protocol) = protocols
            .iter()
            .find(|protocol| protocol.name == self.state.protocol_name)
        else {
            debug!(join_outcome = ?ErrorCode::InconsistentGroupProtocol);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::InconsistentGroupProtocol.into())
                .generation_id(self.generation_id)
                .protocol_type(Some(protocol_type.into()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id("".into())
                .members(Some([].into()));

            return (self.into(), join_group_response.into());
        };

        if member_id.is_empty() && group_instance_id.is_none() {
            let member_id = if let Some(client_id) = client_id {
                format!("{client_id}-{}", Uuid::new_v4())
            } else {
                format!("{}", Uuid::new_v4())
            };
            debug!(?member_id, join_outcome = ?ErrorCode::MemberIdRequired);

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::MemberIdRequired.into())
                .generation_id(-1)
                .protocol_type(None)
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(self.skip_assignment)
                .member_id(member_id)
                .members(Some([].into()));

            // KIP-394: reply with the generated member id but leave the group
            // untouched — no phantom member, no generation bump, and the
            // group stays Formed. The rebalance only starts when the member
            // re-joins with this id.
            return (self.into(), join_group_response.into());
        }

        let member_id = group_instance_id.map_or(member_id.to_owned(), |group_instance_id| {
            if member_id.is_empty() {
                if let Some((member_id, _)) = self.members.iter().find(|(_, member)| {
                    member.join_response.group_instance_id.as_deref() == Some(group_instance_id)
                }) {
                    member_id.into()
                } else {
                    format!("{group_instance_id}-{}", Uuid::new_v4())
                }
            } else {
                member_id.into()
            }
        });

        debug!(?member_id, ?self.members);

        match self.members.get_mut(&member_id) {
            Some(Member {
                join_response: JoinGroupResponseMember { metadata, .. },
                ..
            }) if *metadata == protocol.metadata => {
                debug!(
                    member_metadata = "existing",
                    member_id,
                    generation_id = self.generation_id
                );

                let state: Wrapper<O> = self.into();

                let body = {
                    let members = Some(
                        if state.leader().is_some_and(|leader| leader == member_id) {
                            state.members()
                        } else {
                            [].into()
                        },
                    );
                    let protocol_type = state.protocol_type().map(ToOwned::to_owned);
                    let protocol_name = state.protocol_name().map(ToOwned::to_owned);

                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(state.generation_id())
                        .protocol_type(protocol_type)
                        .protocol_name(protocol_name)
                        .leader(
                            state
                                .leader()
                                .map(|s| s.to_owned())
                                .unwrap_or("".to_owned()),
                        )
                        .skip_assignment(state.skip_assignment().map(ToOwned::to_owned))
                        .member_id(member_id)
                        .members(members)
                        .into()
                };

                debug!(join_outcome = ?ErrorCode::None);

                (state, body)
            }

            Some(Member {
                join_response: JoinGroupResponseMember { metadata, .. },
                ..
            }) => {
                debug!(
                    member_metadata = if group_instance_id.is_none() {"update"} else { "soft_update"},
                    member_id,
                    updated = ?protocol.metadata,
                    existing = ?metadata,
                );

                *metadata = protocol.metadata.clone();

                let state: Wrapper<O> = Inner {
                    generation_id: if group_instance_id.is_none() {
                        self.generation_id + 1
                    } else {
                        self.generation_id
                    },
                    session_timeout_ms: self.session_timeout_ms,
                    rebalance_timeout_ms: self.rebalance_timeout_ms,

                    members: self.members,
                    state: Forming {
                        protocol_type: Some(self.state.protocol_type),
                        protocol_name: Some(self.state.protocol_name),
                        leader: Some(self.state.leader),
                    },
                    storage: self.storage,
                    skip_assignment: self.skip_assignment,
                    inception: self.inception,
                }
                .into();

                let body = {
                    let members = Some(
                        if state.leader().is_some_and(|leader| leader == member_id) {
                            state.members()
                        } else {
                            [].into()
                        },
                    );
                    let protocol_type = state.protocol_type().map(|s| s.to_owned());
                    let protocol_name = state.protocol_name().map(|s| s.to_owned());

                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(state.generation_id())
                        .protocol_type(protocol_type)
                        .protocol_name(protocol_name)
                        .leader(
                            state
                                .leader()
                                .map(|s| s.to_owned())
                                .unwrap_or("".to_owned()),
                        )
                        .skip_assignment(self.skip_assignment)
                        .member_id(member_id)
                        .members(members)
                        .into()
                };

                debug!(join_outcome = ?ErrorCode::None);

                (state, body)
            }

            None => {
                debug!(
                    member_metadata = "new",
                    member_id,
                    generation_id = self.generation_id + 1
                );

                _ = self.members.insert(
                    member_id.clone(),
                    Member {
                        join_response: JoinGroupResponseMember::default()
                            .member_id(member_id.to_string())
                            .group_instance_id(group_instance_id.map(|s| s.to_owned()))
                            .metadata(protocol.metadata.clone()),
                        last_contact: Some(now),
                    },
                );

                let state: Wrapper<O> = Inner {
                    generation_id: self.generation_id + 1,
                    session_timeout_ms: self.session_timeout_ms,
                    rebalance_timeout_ms: self.rebalance_timeout_ms,

                    members: self.members,
                    state: Forming {
                        protocol_type: Some(self.state.protocol_type),
                        protocol_name: Some(self.state.protocol_name),
                        leader: Some(self.state.leader),
                    },
                    storage: self.storage,
                    skip_assignment: self.skip_assignment,
                    inception: self.inception,
                }
                .into();

                let body = {
                    let members = Some(
                        if state.leader().is_some_and(|leader| leader == member_id) {
                            state.members()
                        } else {
                            [].into()
                        },
                    );

                    let protocol_type = state.protocol_type().map(|s| s.to_owned());
                    let protocol_name = state.protocol_name().map(|s| s.to_owned());

                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(state.generation_id())
                        .protocol_type(protocol_type)
                        .protocol_name(protocol_name)
                        .leader(
                            state
                                .leader()
                                .map(|s| s.to_owned())
                                .unwrap_or("".to_owned()),
                        )
                        .skip_assignment(self.skip_assignment)
                        .member_id(member_id)
                        .members(members)
                        .into()
                };

                debug!(join_outcome = ?ErrorCode::None);

                (state, body)
            }
        }
    }

    async fn sync(
        mut self,
        _now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> (Self::SyncState, Body) {
        let _ = group_id;
        let _ = group_instance_id;
        let _ = protocol_type;
        let _ = protocol_name;
        let _ = assignments;

        if !self.members.contains_key(member_id) {
            debug!(sync_outcome = ?ErrorCode::UnknownMemberId);

            let body = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::UnknownMemberId.into())
                .protocol_type(Some(self.state.protocol_type.clone()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .assignment(Bytes::from_static(b""))
                .into();

            return (self, body);
        }

        debug!(?member_id);

        if generation_id > self.generation_id {
            debug!(sync_outcome = ?ErrorCode::IllegalGeneration);

            let body = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::IllegalGeneration.into())
                .protocol_type(Some(self.state.protocol_type.clone()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .assignment(Bytes::from_static(b""))
                .into();

            return (self, body);
        }

        if generation_id < self.generation_id {
            debug!(sync_outcome = ?ErrorCode::RebalanceInProgress);

            let body = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(Some(self.state.protocol_type.clone()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .assignment(Bytes::from_static(b""))
                .into();

            return (self, body);
        }

        // A member of the current generation that is missing from the leader's
        // assignment must re-join, not park on "stable" with zero partitions:
        // `error=None` plus an empty assignment reads as a valid (empty)
        // assignment to the client, which then sits idle forever.
        let Some(assignment) = self.state.assignments.get(member_id).cloned() else {
            debug!(?self.state.assignments, sync_outcome = ?ErrorCode::RebalanceInProgress);

            let body = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into())
                .protocol_type(Some(self.state.protocol_type.clone()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .assignment(Bytes::from_static(b""))
                .into();

            return (self, body);
        };

        let body = SyncGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .protocol_type(Some(self.state.protocol_type.clone()))
            .protocol_name(Some(self.state.protocol_name.clone()))
            .assignment(assignment)
            .into();

        debug!(sync_outcome = ?ErrorCode::None, sync_assignment = true);

        (self, body)
    }

    async fn heartbeat(
        mut self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body) {
        debug!(?group_id, ?generation_id, ?member_id, ?group_instance_id);

        if !self.members.contains_key(member_id) {
            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::UnknownMemberId.into())
                    .into(),
            );
        }

        if generation_id > self.generation_id {
            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::IllegalGeneration.into())
                    .into(),
            );
        }

        if self.missed_heartbeat(group_id, member_id, now) || (generation_id < self.generation_id) {
            return (
                self,
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::RebalanceInProgress.into())
                    .into(),
            );
        }

        _ = self
            .members
            .entry(member_id.to_owned())
            .and_modify(|member| _ = member.last_contact.replace(now));

        let body = HeartbeatResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .into();

        (self, body)
    }

    async fn leave(
        mut self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body) {
        let _ = now;
        let _ = group_id;

        let members = if let Some(member_id) = member_id {
            vec![
                MemberResponse::default()
                    .member_id(member_id.to_owned())
                    .group_instance_id(None)
                    .error_code({
                        if self.members.remove(member_id).is_some() {
                            ErrorCode::None.into()
                        } else {
                            ErrorCode::UnknownMemberId.into()
                        }
                    }),
            ]
        } else {
            members.map_or(vec![], |members| {
                members
                    .iter()
                    .map(|member| {
                        MemberResponse::default()
                            .member_id(member.member_id.clone())
                            .group_instance_id(member.group_instance_id.clone())
                            .error_code({
                                if self.members.remove(&member.member_id).is_some() {
                                    ErrorCode::None.into()
                                } else {
                                    ErrorCode::UnknownMemberId.into()
                                }
                            })
                    })
                    .collect::<Vec<MemberResponse>>()
            })
        };

        let state: Wrapper<O> = if members
            .iter()
            .any(|member| member.error_code == i16::from(ErrorCode::None))
        {
            let leader = if self.members.contains_key(&self.state.leader) {
                Some(self.state.leader)
            } else {
                None
            };

            Inner {
                generation_id: self.generation_id + 1,
                session_timeout_ms: self.session_timeout_ms,
                rebalance_timeout_ms: self.rebalance_timeout_ms,

                members: self.members,
                state: Forming {
                    protocol_type: Some(self.state.protocol_type),
                    protocol_name: Some(self.state.protocol_name),
                    leader,
                },
                storage: self.storage,
                skip_assignment: self.skip_assignment,
                inception: self.inception,
            }
            .into()
        } else {
            self.into()
        };

        let body = LeaveGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .members(Some(members))
            .into();

        (state, body)
    }

    async fn offset_commit(
        mut self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Body) {
        let _ = now;

        match self.commit_offset(detail).await {
            Ok(body) => (self, body),
            Err(reason) => {
                debug!(?reason);
                (
                    self,
                    OffsetCommitResponse::default()
                        .throttle_time_ms(Some(0))
                        .topics(detail.topics.map(|topics| {
                            topics
                                .as_ref()
                                .iter()
                                .map(|topic| {
                                    OffsetCommitResponseTopic::default()
                                        .name(topic.name.clone())
                                        .partitions(topic.partitions.as_ref().map(|partitions| {
                                            partitions
                                                .iter()
                                                .map(|partition| {
                                                    OffsetCommitResponsePartition::default()
                                                        .partition_index(partition.partition_index)
                                                        .error_code(
                                                            ErrorCode::UnknownMemberId.into(),
                                                        )
                                                })
                                                .collect()
                                        }))
                                })
                                .collect()
                        }))
                        .into(),
                )
            }
        }
    }

    async fn offset_fetch(
        mut self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body) {
        let _ = now;

        match self
            .fetch_offset(group_id, topics, groups, require_stable)
            .await
        {
            Ok(body) => (self, body),
            Err(error) => {
                debug!(?error);
                todo!()
            }
        }
    }
}

fn has_unique_elements<T>(iter: T) -> bool
where
    T: IntoIterator,
    T::Item: Eq + Hash,
{
    let mut uniq = HashSet::new();
    iter.into_iter().all(|x| uniq.insert(x))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use tansu_sans_io::{
        consumer::{
            Assignor, CONSUMER, ConsumerProtocolAssignment, ConsumerProtocolSubscription,
            TopicPartition,
        },
        offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    };
    use tansu_storage::StorageContainer;
    use tracing::subscriber::DefaultGuard;
    use url::Url;

    #[cfg(miri)]
    fn init_tracing() -> Result<()> {
        Ok(())
    }

    #[cfg(not(miri))]
    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        use tracing::Level;
        use tracing_subscriber::fmt::format::FmtSpan;

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_max_level(Level::DEBUG)
                .with_span_events(FmtSpan::ACTIVE)
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[test]
    fn cas_conflict_backoff_is_bounded() {
        for attempt in 0..32 {
            let backoff = cas_conflict_backoff(attempt);
            // Every retry pauses at least the 5ms base, and the exponential is
            // capped at 500ms with at most 50% jitter, so it never exceeds
            // 750ms regardless of how many conflicts have accrued.
            assert!(
                backoff >= Duration::from_millis(5),
                "attempt {attempt}: {backoff:?}"
            );
            assert!(
                backoff <= Duration::from_millis(750),
                "attempt {attempt}: {backoff:?}"
            );
        }
    }

    fn group_detail_with(
        members: &[(&str, Option<SystemTime>)],
        state: GroupState,
        generation_id: i32,
    ) -> GroupDetail {
        GroupDetail {
            session_timeout_ms: 45_000,
            rebalance_timeout_ms: Some(300_000),
            members: members
                .iter()
                .map(|(id, last_contact)| {
                    (
                        (*id).to_owned(),
                        GroupMember {
                            join_response: Default::default(),
                            last_contact: *last_contact,
                        },
                    )
                })
                .collect(),
            generation_id,
            skip_assignment: Some(false),
            inception: SystemTime::UNIX_EPOCH,
            state,
        }
    }

    #[test]
    fn same_rebalance_state_ignores_last_contact() {
        let t0 = SystemTime::UNIX_EPOCH;
        let t1 = t0 + Duration::from_secs(30);

        let forming = GroupState::Forming {
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            leader: Some("m1".into()),
        };

        // Same members, same state, same generation — only last_contact differs
        // (including present vs absent). This is the no-op poll we must skip.
        let a = group_detail_with(&[("m1", Some(t0)), ("m2", Some(t0))], forming.clone(), 3);
        let b = group_detail_with(&[("m1", Some(t1)), ("m2", None)], forming.clone(), 3);
        assert!(same_rebalance_state(&a, &b));

        // A bumped generation is a real rebalance — must not be skipped.
        let new_gen = group_detail_with(&[("m1", Some(t0)), ("m2", Some(t0))], forming.clone(), 4);
        assert!(!same_rebalance_state(&a, &new_gen));

        // A changed member set — must not be skipped.
        let fewer = group_detail_with(&[("m1", Some(t0))], forming.clone(), 3);
        assert!(!same_rebalance_state(&a, &fewer));

        // The Forming -> Formed assignment write (same members, same generation,
        // last_contact could even match) MUST be detected as a change, otherwise
        // the group would never stabilise. This is the write we protect.
        let formed = GroupState::Formed {
            protocol_type: "consumer".into(),
            protocol_name: "range".into(),
            leader: "m1".into(),
            assignments: [("m1".to_owned(), Bytes::from_static(b"assignment"))]
                .into_iter()
                .collect(),
        };
        let assigned = group_detail_with(&[("m1", Some(t0)), ("m2", Some(t0))], formed, 3);
        assert!(!same_rebalance_state(&a, &assigned));
    }

    #[test]
    fn liveness_renewal_due_threshold() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let state = GroupState::default();
        let session_timeout_ms = 30_000; // half == 15s

        // Just contacted -> not due.
        let fresh = group_detail_with(&[("m1", Some(now))], state.clone(), 1);
        assert!(!liveness_renewal_due(&fresh, "m1", now, session_timeout_ms));

        // Under half the session timeout -> still not due.
        let recent = group_detail_with(
            &[("m1", Some(now - Duration::from_secs(14)))],
            state.clone(),
            1,
        );
        assert!(!liveness_renewal_due(
            &recent,
            "m1",
            now,
            session_timeout_ms
        ));

        // Past half the session timeout -> a no-op poll must still refresh.
        let stale = group_detail_with(
            &[("m1", Some(now - Duration::from_secs(20)))],
            state.clone(),
            1,
        );
        assert!(liveness_renewal_due(&stale, "m1", now, session_timeout_ms));

        // Member not yet persisted, or persisted without a timestamp -> write.
        assert!(liveness_renewal_due(
            &fresh,
            "ghost",
            now,
            session_timeout_ms
        ));
        let no_ts = group_detail_with(&[("m1", None)], state, 1);
        assert!(liveness_renewal_due(&no_ts, "m1", now, session_timeout_ms));

        // Empty member id has nothing to renew.
        assert!(!liveness_renewal_due(&fresh, "", now, session_timeout_ms));
    }

    // End-to-end proof of the no-op skip: once a group is Formed and its member
    // assigned, a re-sync by that member (which only refreshes `last_contact`)
    // must not rewrite the group object. The persisted version (etag) staying
    // put is exactly what stops the rebalance-time churn that starves a large
    // group's assignment write. Without the skip this re-sync bumps the version.
    #[tokio::test]
    async fn noop_resync_does_not_bump_persisted_version() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "noop-resync-group";

        let storage = StorageContainer::builder()
            .cluster_id("test")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let s = Controller::with_storage(storage.clone())?;

        let range_meta = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .generation_id(Some(0))
                    .owned_partitions(
                        [TopicPartition::default().topic("a").partitions(0..3)].into_iter(),
                    ),
            ),
        )?;

        let protocols = [JoinGroupRequestProtocol::default()
            .name(Assignor::RANGE.into())
            .metadata(range_meta.clone())];

        // First join with an empty member id returns the assigned member id.
        let member_id = match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                CONSUMER,
                Some(&protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse { member_id, .. }) => member_id,
            otherwise => panic!("{otherwise:?}"),
        };

        // Second join with that id makes this the (single) leader.
        let generation_id = match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                &member_id,
                group_instance_id,
                CONSUMER,
                Some(&protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse { generation_id, .. }) => generation_id,
            otherwise => panic!("{otherwise:?}"),
        };

        // Sync with an assignment forms the group (Forming -> Formed).
        let assignment = Bytes::try_from(&MemberAssignment::default().assignment(
            ConsumerProtocolAssignment::default().assigned_partitions(
                [TopicPartition::default().topic("a").partitions(0..3)].into_iter(),
            ),
        ))?;
        let assignments = [SyncGroupRequestAssignment::default()
            .member_id(member_id.clone())
            .assignment(assignment.clone())];

        _ = s
            .sync(
                GROUP_ID,
                generation_id,
                &member_id,
                group_instance_id,
                Some(CONSUMER),
                Some(Assignor::RANGE),
                Some(&assignments[..]),
            )
            .await?;

        let (detail, v1) = storage
            .read_group(GROUP_ID)
            .await?
            .expect("group must be persisted after sync");
        assert!(
            matches!(detail.state, GroupState::Formed { .. }),
            "expected Formed, got {:?}",
            detail.state
        );

        // No-op re-sync by the assigned member: only `last_contact` would change,
        // so the object must not be rewritten and its version must be stable.
        _ = s
            .sync(
                GROUP_ID,
                generation_id,
                &member_id,
                group_instance_id,
                Some(CONSUMER),
                Some(Assignor::RANGE),
                Some(&assignments[..]),
            )
            .await?;

        let (_, v2) = storage
            .read_group(GROUP_ID)
            .await?
            .expect("group still persisted");

        assert_eq!(
            v1, v2,
            "a no-op re-sync must not rewrite the group object (etag must stay stable)"
        );

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn lifecycle() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        let cluster = "abc";
        let node = 12321;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "test-consumer-group";
        const TOPIC: &str = "test";

        let storage = StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let s = Controller::with_storage(storage)?;

        let first_member_range_meta = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .generation_id(Some(0))
                    .owned_partitions(
                        [TopicPartition::default().topic("a").partitions(0..3)].into_iter(),
                    ),
            ),
        )?;

        let first_member_sticky_meta = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .generation_id(Some(0))
                    .owned_partitions(
                        [TopicPartition::default().topic("b").partitions(0..3)].into_iter(),
                    ),
            ),
        )?;

        let protocols = [
            JoinGroupRequestProtocol::default()
                .name(Assignor::RANGE.into())
                .metadata(first_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(Assignor::COOPERATIVE_STICKY.into())
                .metadata(first_member_sticky_meta),
        ];

        let first_member_id = match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                CONSUMER,
                Some(&protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: -1,
                protocol_type: Some(protocol_type),
                protocol_name: Some(protocol_name),
                leader,
                skip_assignment: Some(false),
                members: Some(members),
                member_id,
                ..
            }) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert_eq!("consumer", protocol_type);
                assert_eq!("", protocol_name);
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(0, members.len());

                let join_response = s
                    .join(
                        Some(CLIENT_ID),
                        GROUP_ID,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        &member_id,
                        group_instance_id,
                        CONSUMER,
                        Some(&protocols[..]),
                        reason,
                    )
                    .await?;

                let join_response_expected = Body::from(
                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(0)
                        .protocol_type(Some(CONSUMER.into()))
                        .protocol_name(Some(Assignor::RANGE.into()))
                        .leader(member_id.clone())
                        .skip_assignment(Some(false))
                        .member_id(member_id.clone())
                        .members(Some(
                            [JoinGroupResponseMember::default()
                                .member_id(member_id.clone())
                                .group_instance_id(None)
                                .metadata(first_member_range_meta.clone())]
                            .into(),
                        )),
                );

                assert_eq!(join_response_expected, join_response);

                member_id
            }

            otherwise => panic!("{otherwise:?}"),
        };

        let first_member_assignment_01 = Bytes::try_from(&MemberAssignment::default().assignment(
            ConsumerProtocolAssignment::default().assigned_partitions(
                [TopicPartition::default().topic("x").partitions(3..6)].into_iter(),
            ),
        ))?;

        let assignments = [SyncGroupRequestAssignment::default()
            .member_id(first_member_id.clone())
            .assignment(first_member_assignment_01.clone())];

        assert_eq!(
            Body::from(
                SyncGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(0)
                    .protocol_type(Some(CONSUMER.into()))
                    .protocol_name(Some(Assignor::RANGE.into()))
                    .assignment(first_member_assignment_01)
            ),
            s.sync(
                GROUP_ID,
                0,
                &first_member_id,
                group_instance_id,
                Some(CONSUMER),
                Some(Assignor::RANGE),
                Some(&assignments[..]),
            )
            .await?
        );

        assert_eq!(
            Body::from(
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
            ),
            s.heartbeat(GROUP_ID, 0, &first_member_id, group_instance_id)
                .await?
        );

        let second_member_range_meta = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .generation_id(Some(0))
                    .owned_partitions(
                        [TopicPartition::default().topic("p").partitions(0..3)].into_iter(),
                    ),
            ),
        )?;

        let second_member_sticky_meta = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .generation_id(Some(0))
                    .owned_partitions(
                        [TopicPartition::default().topic("q").partitions(0..3)].into_iter(),
                    ),
            ),
        )?;

        let protocols = [
            JoinGroupRequestProtocol::default()
                .name(Assignor::RANGE.into())
                .metadata(second_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(Assignor::COOPERATIVE_STICKY.into())
                .metadata(second_member_sticky_meta.clone()),
        ];

        let second_member_id = match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                CONSUMER,
                Some(&protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: -1,
                protocol_type: None,
                protocol_name: Some(protocol_name),
                leader,
                skip_assignment: Some(false),
                members: Some(members),
                member_id,
                ..
            }) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert_eq!("", protocol_name);
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(0, members.len());

                let join_response = s
                    .join(
                        Some(CLIENT_ID),
                        GROUP_ID,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        &member_id,
                        group_instance_id,
                        CONSUMER,
                        Some(&protocols[..]),
                        reason,
                    )
                    .await?;

                let join_response_expected = Body::from(
                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        // The joining member observes the in-progress rebalance's
                        // generation (1): admitting it moved the group off the
                        // stable generation 0. It only advances to 2 once the
                        // leader re-joins with a new subscription further down.
                        .generation_id(1)
                        .protocol_type(Some(CONSUMER.into()))
                        .protocol_name(Some(Assignor::RANGE.into()))
                        .leader(first_member_id.clone())
                        .skip_assignment(Some(false))
                        .member_id(member_id.clone())
                        .members(Some([].into())),
                );

                assert_eq!(join_response_expected, join_response);

                member_id
            }

            otherwise => panic!("{otherwise:?}"),
        };

        assert_eq!(
            Body::from(
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(i16::from(ErrorCode::RebalanceInProgress))
            ),
            s.heartbeat(GROUP_ID, 0, &first_member_id, group_instance_id,)
                .await?
        );

        assert_eq!(
            Body::from(
                OffsetCommitResponse::default()
                    .throttle_time_ms(Some(0))
                    .topics(Some(
                        [OffsetCommitResponseTopic::default()
                            .name(TOPIC.into())
                            .partitions(Some(
                                (0..=2)
                                    .map(|partition_index| OffsetCommitResponsePartition::default()
                                        .partition_index(partition_index)
                                        .error_code(ErrorCode::UnknownTopicOrPartition.into()))
                                    .collect(),
                            )),]
                        .into()
                    ))
            ),
            s.offset_commit(OffsetCommit {
                group_id: GROUP_ID,
                generation_id_or_member_epoch: Some(0),
                member_id: Some(&first_member_id),
                group_instance_id,
                retention_time_ms: None,
                topics: Some(&[OffsetCommitRequestTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(
                        (0..=2)
                            .map(|partition_index| OffsetCommitRequestPartition::default()
                                .partition_index(partition_index)
                                .committed_offset(1)
                                .committed_leader_epoch(Some(0))
                                .commit_timestamp(None)
                                .committed_metadata(Some("".into())))
                            .collect(),
                    )),]),
            })
            .await?
        );

        {
            let first_member_range_meta = Bytes::try_from(
                &MemberMetadata::default().version(3).subscription(
                    ConsumerProtocolSubscription::default()
                        .generation_id(Some(0))
                        .owned_partitions(
                            [TopicPartition::default().topic("f").partitions(0..3)].into_iter(),
                        ),
                ),
            )?;

            let first_member_sticky_meta = Bytes::try_from(
                &MemberMetadata::default().version(3).subscription(
                    ConsumerProtocolSubscription::default()
                        .generation_id(Some(0))
                        .owned_partitions(
                            [TopicPartition::default().topic("g").partitions(0..3)].into_iter(),
                        ),
                ),
            )?;

            let protocols = [
                JoinGroupRequestProtocol::default()
                    .name(Assignor::RANGE.into())
                    .metadata(first_member_range_meta.clone()),
                JoinGroupRequestProtocol::default()
                    .name(Assignor::COOPERATIVE_STICKY.into())
                    .metadata(first_member_sticky_meta),
            ];

            match s
                .join(
                    Some(CLIENT_ID),
                    GROUP_ID,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &first_member_id,
                    group_instance_id,
                    CONSUMER,
                    Some(&protocols[..]),
                    reason,
                )
                .await?
            {
                Body::JoinGroupResponse(JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code,
                    generation_id,
                    protocol_type,
                    protocol_name,
                    leader,
                    skip_assignment: Some(false),
                    member_id,
                    members: Some(members),
                    ..
                }) => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);
                    assert_eq!(2, generation_id);
                    assert_eq!(Some(CONSUMER.into()), protocol_type);
                    assert_eq!(Some(Assignor::RANGE.into()), protocol_name);
                    assert_eq!(first_member_id, leader);
                    assert_eq!(first_member_id, member_id);

                    assert_eq!(
                        Some(first_member_range_meta),
                        members
                            .iter()
                            .find(|member| member.member_id == first_member_id)
                            .map(|member| member.metadata.clone())
                    );

                    assert_eq!(
                        Some(second_member_range_meta.clone()),
                        members
                            .iter()
                            .find(|member| member.member_id == second_member_id)
                            .map(|member| member.metadata.clone())
                    );
                }

                otherwise => panic!("{otherwise:?}"),
            }
        }

        {
            let protocols = [
                JoinGroupRequestProtocol::default()
                    .name(Assignor::RANGE.into())
                    .metadata(second_member_range_meta.clone()),
                JoinGroupRequestProtocol::default()
                    .name(Assignor::COOPERATIVE_STICKY.into())
                    .metadata(second_member_sticky_meta.clone()),
            ];

            match s
                .join(
                    Some(CLIENT_ID),
                    GROUP_ID,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &second_member_id,
                    group_instance_id,
                    CONSUMER,
                    Some(&protocols[..]),
                    reason,
                )
                .await?
            {
                Body::JoinGroupResponse(JoinGroupResponse {
                    throttle_time_ms: Some(0),
                    error_code,
                    generation_id,
                    protocol_type: Some(protocol_type),
                    protocol_name: Some(protocol_name),
                    leader,
                    skip_assignment: Some(false),
                    member_id,
                    members: Some(members),
                    ..
                }) => {
                    assert_eq!(i16::from(ErrorCode::None), error_code);
                    assert_eq!(2, generation_id);
                    assert_eq!(CONSUMER, protocol_type);
                    assert_eq!(Assignor::RANGE, protocol_name);
                    assert_eq!(first_member_id, leader);
                    assert_eq!(second_member_id, member_id);
                    assert_eq!(0, members.len());
                }

                otherwise => panic!("{otherwise:?}"),
            }
        }

        let second_member_assignment_02 =
            Bytes::try_from(&MemberAssignment::default().assignment(
                ConsumerProtocolAssignment::default().assigned_partitions(
                    [TopicPartition::default().topic("y").partitions(3..6)].into_iter(),
                ),
            ))?;

        {
            let first_member_assignment_02 =
                Bytes::try_from(&MemberAssignment::default().assignment(
                    ConsumerProtocolAssignment::default().assigned_partitions(
                        [TopicPartition::default().topic("z").partitions(0..3)].into_iter(),
                    ),
                ))?;

            let assignments = [
                SyncGroupRequestAssignment::default()
                    .member_id(first_member_id.clone())
                    .assignment(first_member_assignment_02.clone()),
                SyncGroupRequestAssignment::default()
                    .member_id(second_member_id.clone())
                    .assignment(second_member_assignment_02.clone()),
            ];

            assert_eq!(
                Body::from(
                    SyncGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .protocol_type(Some(CONSUMER.into()))
                        .protocol_name(Some(Assignor::RANGE.into()))
                        .assignment(first_member_assignment_02)
                ),
                s.sync(
                    GROUP_ID,
                    2,
                    &first_member_id,
                    group_instance_id,
                    Some(CONSUMER),
                    Some(Assignor::RANGE),
                    Some(&assignments[..]),
                )
                .await?
            );
        }

        assert_eq!(
            Body::from(
                SyncGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .protocol_type(Some(CONSUMER.into()))
                    .protocol_name(Some(Assignor::RANGE.into()))
                    .assignment(second_member_assignment_02)
            ),
            s.sync(
                GROUP_ID,
                2,
                &second_member_id,
                group_instance_id,
                Some(CONSUMER),
                Some(Assignor::RANGE),
                Some(&[]),
            )
            .await?
        );

        assert_eq!(
            Body::from(
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
            ),
            s.heartbeat(GROUP_ID, 2, &first_member_id, group_instance_id,)
                .await?
        );

        assert_eq!(
            Body::from(
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
            ),
            s.heartbeat(GROUP_ID, 2, &second_member_id, group_instance_id,)
                .await?
        );

        assert_eq!(
            Body::from(
                LeaveGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .members(Some(
                        [MemberResponse::default()
                            .member_id(first_member_id.clone())
                            .group_instance_id(None)
                            .error_code(ErrorCode::None.into())]
                        .into()
                    ))
            ),
            s.leave(
                GROUP_ID,
                None,
                Some(&[MemberIdentity::default()
                    .member_id(first_member_id.clone())
                    .group_instance_id(None)
                    .reason(Some("the consumer is being closed".into()))]),
            )
            .await?
        );

        assert_eq!(
            Body::from(
                HeartbeatResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::RebalanceInProgress.into())
            ),
            s.heartbeat(GROUP_ID, 2, &second_member_id, group_instance_id,)
                .await?
        );

        {
            let protocols = [
                JoinGroupRequestProtocol::default()
                    .name(Assignor::RANGE.into())
                    .metadata(second_member_range_meta.clone()),
                JoinGroupRequestProtocol::default()
                    .name(Assignor::COOPERATIVE_STICKY.into())
                    .metadata(second_member_sticky_meta.clone()),
            ];

            assert_eq!(
                Body::from(
                    JoinGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .generation_id(3)
                        .protocol_type(Some(CONSUMER.into()))
                        .protocol_name(Some(Assignor::RANGE.into()))
                        .leader(second_member_id.clone())
                        .skip_assignment(Some(false))
                        .member_id(second_member_id.clone())
                        .members(Some(
                            [JoinGroupResponseMember::default()
                                .member_id(second_member_id.clone())
                                .group_instance_id(None)
                                .metadata(second_member_range_meta.clone())]
                            .into()
                        ))
                ),
                s.join(
                    Some(CLIENT_ID),
                    GROUP_ID,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    &second_member_id,
                    group_instance_id,
                    CONSUMER,
                    Some(&protocols[..]),
                    reason,
                )
                .await?
            );
        }

        {
            let second_member_assignment_03 =
                Bytes::try_from(&MemberAssignment::default().assignment(
                    ConsumerProtocolAssignment::default().assigned_partitions(
                        [TopicPartition::default().topic("x").partitions(6..9)].into_iter(),
                    ),
                ))?;

            let assignments = [SyncGroupRequestAssignment::default()
                .member_id(second_member_id.clone())
                .assignment(second_member_assignment_03.clone())];

            assert_eq!(
                Body::from(
                    SyncGroupResponse::default()
                        .throttle_time_ms(Some(0))
                        .error_code(ErrorCode::None.into())
                        .protocol_type(Some(CONSUMER.into()))
                        .protocol_name(Some(Assignor::RANGE.into()))
                        .assignment(second_member_assignment_03)
                ),
                s.sync(
                    GROUP_ID,
                    3,
                    &second_member_id,
                    group_instance_id,
                    Some(CONSUMER),
                    Some(Assignor::RANGE),
                    Some(&assignments[..]),
                )
                .await?
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn rejoin() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 10_000;
        let rebalance_timeout_ms = Some(60_000);
        let group_instance_id = None;
        let reason = None;

        let cluster = "abc";
        let node = 12321;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "test-consumer-group";

        let storage = StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let s = Controller::with_storage(storage)?;

        let first_member_range_meta = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .generation_id(Some(0))
                    .owned_partitions(
                        [TopicPartition::default().topic("a").partitions(0..3)].into_iter(),
                    ),
            ),
        )?;

        let first_member_sticky_meta = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .generation_id(Some(0))
                    .owned_partitions(
                        [TopicPartition::default().topic("b").partitions(0..3)].into_iter(),
                    ),
            ),
        )?;

        let first_member_protocols = [
            JoinGroupRequestProtocol::default()
                .name(Assignor::RANGE.into())
                .metadata(first_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(Assignor::COOPERATIVE_STICKY.into())
                .metadata(first_member_sticky_meta),
        ];

        let first_member_id = match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                CONSUMER,
                Some(&first_member_protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: -1,
                leader,
                skip_assignment: Some(false),
                members: Some(members),
                member_id,
                ..
            }) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(0, members.len());

                assert_eq!(
                    Body::from(
                        JoinGroupResponse::default()
                            .throttle_time_ms(Some(0))
                            .error_code(ErrorCode::None.into())
                            .generation_id(0)
                            .protocol_type(Some(CONSUMER.into()))
                            .protocol_name(Some(Assignor::RANGE.into()))
                            .leader(member_id.clone())
                            .skip_assignment(Some(false))
                            .member_id(member_id.clone())
                            .members(Some(
                                [JoinGroupResponseMember::default()
                                    .member_id(member_id.clone())
                                    .group_instance_id(None)
                                    .metadata(first_member_range_meta.clone())]
                                .into()
                            ))
                    ),
                    s.join(
                        Some(CLIENT_ID),
                        GROUP_ID,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        &member_id,
                        group_instance_id,
                        CONSUMER,
                        Some(&first_member_protocols[..]),
                        reason,
                    )
                    .await?
                );

                member_id
            }

            otherwise => panic!("{otherwise:?}"),
        };

        let second_member_range_meta = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .generation_id(Some(0))
                    .owned_partitions(
                        [TopicPartition::default().topic("p").partitions(0..3)].into_iter(),
                    ),
            ),
        )?;

        let second_member_sticky_meta = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .generation_id(Some(0))
                    .owned_partitions(
                        [TopicPartition::default().topic("q").partitions(0..3)].into_iter(),
                    ),
            ),
        )?;

        let second_member_protocols = [
            JoinGroupRequestProtocol::default()
                .name(Assignor::RANGE.into())
                .metadata(second_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(Assignor::COOPERATIVE_STICKY.into())
                .metadata(second_member_sticky_meta),
        ];

        let second_member_id = match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                CONSUMER,
                Some(&second_member_protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: -1,
                leader,
                skip_assignment: Some(false),
                members: Some(members),
                member_id,
                ..
            }) => {
                assert_eq!(error_code, i16::from(ErrorCode::MemberIdRequired));
                assert!(leader.is_empty());
                assert!(member_id.starts_with(CLIENT_ID));
                assert_eq!(0, members.len());

                assert_eq!(
                    Body::from(
                        JoinGroupResponse::default()
                            .throttle_time_ms(Some(0))
                            .error_code(ErrorCode::None.into())
                            .generation_id(1)
                            .protocol_type(Some(CONSUMER.into()))
                            .protocol_name(Some(Assignor::RANGE.into()))
                            .leader(first_member_id.clone())
                            .skip_assignment(Some(false))
                            .member_id(member_id.clone())
                            .members(Some([].into()))
                    ),
                    s.join(
                        Some(CLIENT_ID),
                        GROUP_ID,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        &member_id,
                        group_instance_id,
                        CONSUMER,
                        Some(&second_member_protocols[..]),
                        reason,
                    )
                    .await?
                );

                member_id
            }

            otherwise => panic!("{otherwise:?}"),
        };

        match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                &first_member_id,
                group_instance_id,
                CONSUMER,
                Some(&first_member_protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse {
                throttle_time_ms: Some(0),
                error_code,
                generation_id: 1,
                protocol_type,
                protocol_name,
                leader,
                skip_assignment: Some(false),
                member_id,
                members: Some(members),
                ..
            }) => {
                assert_eq!(i16::from(ErrorCode::None), error_code);
                assert_eq!(Some(CONSUMER.into()), protocol_type);
                assert_eq!(Some(Assignor::RANGE.into()), protocol_name);
                assert_eq!(first_member_id.clone(), leader);
                assert_eq!(first_member_id.clone(), member_id);
                assert_eq!(2, members.len());
                assert!(
                    members.contains(
                        &JoinGroupResponseMember::default()
                            .member_id(second_member_id.clone())
                            .group_instance_id(None)
                            .metadata(second_member_range_meta.clone())
                    )
                );
                assert!(
                    members.contains(
                        &JoinGroupResponseMember::default()
                            .member_id(first_member_id.clone())
                            .group_instance_id(None)
                            .metadata(first_member_range_meta.clone())
                    )
                );
            }

            otherwise => panic!("{otherwise:?}"),
        }

        assert_eq!(
            Body::from(
                JoinGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .generation_id(1)
                    .protocol_type(Some(CONSUMER.into()))
                    .protocol_name(Some(Assignor::RANGE.into()))
                    .leader(first_member_id.clone())
                    .skip_assignment(Some(false))
                    .member_id(second_member_id.clone())
                    .members(Some([].into(),))
            ),
            s.join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                &second_member_id,
                group_instance_id,
                CONSUMER,
                Some(&second_member_protocols[..]),
                reason,
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn member_id_required_error_code_joins_group() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        let cluster = "abc";
        let node = 12321;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "test-consumer-group";
        const RANGE: &str = "range";
        const COOPERATIVE_STICKY: &str = "cooperative-sticky";

        const PROTOCOL_TYPE: &str = "consumer";

        let storage = StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let s = Wrapper::with_storage_group_detail(
            storage,
            GroupDetail {
                session_timeout_ms,
                rebalance_timeout_ms,
                state: GroupState::Forming {
                    protocol_type: Some(PROTOCOL_TYPE.into()),
                    protocol_name: Some(RANGE.into()),
                    leader: None,
                },
                ..Default::default()
            },
        );

        let now = SystemTime::now();

        let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
        let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

        let first_member_protocols = [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(first_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(first_member_sticky_meta),
        ];

        assert!(s.members().is_empty());

        match s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&first_member_protocols[..]),
                reason,
            )
            .await
        {
            (
                s,
                Body::JoinGroupResponse(JoinGroupResponse {
                    error_code,
                    generation_id,
                    leader,
                    member_id,
                    members,
                    ..
                }),
            ) => {
                assert_eq!(-1, generation_id);
                assert_eq!(i16::from(ErrorCode::MemberIdRequired), error_code);
                assert_eq!("", leader);
                assert_eq!(Some([].into()), members);
                assert!(member_id.starts_with(CLIENT_ID));
                // KIP-394: the MemberIdRequired reply parks the generated id —
                // the member only registers when it re-joins with that id.
                assert_eq!(0, s.members().len());
                assert_eq!(-1, s.generation_id());
            }

            otherwise => panic!("{otherwise:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn forming_leader_leaves_group() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        let cluster = "abc";
        let node = 12321;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "test-consumer-group";
        const RANGE: &str = "range";
        const COOPERATIVE_STICKY: &str = "cooperative-sticky";

        const PROTOCOL_TYPE: &str = "consumer";

        let storage = StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let s = Wrapper::with_storage_group_detail(
            storage,
            GroupDetail {
                session_timeout_ms,
                rebalance_timeout_ms,
                state: GroupState::Forming {
                    protocol_type: Some(PROTOCOL_TYPE.into()),
                    protocol_name: Some(RANGE.into()),
                    leader: None,
                },
                ..Default::default()
            },
        );

        let now = SystemTime::now();

        let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
        let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

        let first_member_protocols = [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(first_member_range_meta.clone()),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(first_member_sticky_meta),
        ];

        assert!(s.members().is_empty());

        let (s, member_id) = match s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&first_member_protocols[..]),
                reason,
            )
            .await
        {
            (
                s,
                Body::JoinGroupResponse(JoinGroupResponse {
                    error_code,
                    generation_id,
                    leader,
                    member_id,
                    members,
                    ..
                }),
            ) => {
                assert_eq!(-1, generation_id);
                assert_eq!(i16::from(ErrorCode::MemberIdRequired), error_code);
                assert_eq!("", leader);
                assert_eq!(Some([].into()), members);
                assert!(member_id.starts_with(CLIENT_ID));
                // KIP-394: the MemberIdRequired reply must not register the
                // member; it joins for real with the generated id below.
                assert_eq!(0, s.members().len());

                (s, member_id)
            }

            otherwise => panic!("{otherwise:?}"),
        };

        let (s, _) = s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id.as_str(),
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&first_member_protocols[..]),
                reason,
            )
            .await;

        assert_eq!(1, s.members().len());

        let assignments = [SyncGroupRequestAssignment::default()
            .member_id(member_id.clone())
            .assignment(Bytes::from_static(b"assignment_01"))];

        let (s, _) = s
            .sync(
                now,
                GROUP_ID,
                0,
                member_id.as_str(),
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments[..]),
            )
            .await;

        assert_eq!(Some(member_id.as_str()), s.leader());

        let (s, _) = s.leave(now, GROUP_ID, Some(member_id.as_str()), None).await;

        assert_eq!(None, s.leader());

        Ok(())
    }

    #[tokio::test]
    async fn sync_from_member_while_forming() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        let cluster = "abc";
        let node = 12321;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "test-consumer-group";
        const RANGE: &str = "range";
        const COOPERATIVE_STICKY: &str = "cooperative-sticky";

        const PROTOCOL_TYPE: &str = "consumer";

        let storage = StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let s = Wrapper::with_storage_group_detail(
            storage,
            GroupDetail {
                session_timeout_ms,
                rebalance_timeout_ms,
                state: GroupState::Forming {
                    protocol_type: Some(PROTOCOL_TYPE.into()),
                    protocol_name: Some(RANGE.into()),
                    leader: None,
                },
                ..Default::default()
            },
        );

        let now = SystemTime::now();

        let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
        let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

        let second_member_range_meta = Bytes::from_static(b"second_member_range_meta_01");
        let second_member_sticky_meta = Bytes::from_static(b"second_member_sticky_meta_01");

        assert!(s.members().is_empty());
        assert_eq!(None, s.leader());
        assert_eq!(None, s.assignments());

        let first_member_id = format!("{}-{}", CLIENT_ID, Uuid::new_v4());

        let (s, _) = s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                first_member_id.as_str(),
                group_instance_id,
                PROTOCOL_TYPE,
                Some(
                    &[
                        JoinGroupRequestProtocol::default()
                            .name(RANGE.into())
                            .metadata(first_member_range_meta.clone()),
                        JoinGroupRequestProtocol::default()
                            .name(COOPERATIVE_STICKY.into())
                            .metadata(first_member_sticky_meta),
                    ][..],
                ),
                reason,
            )
            .await;

        assert_eq!(0, s.generation_id());
        assert_eq!(1, s.members().len());
        assert!(
            s.members().contains(
                &JoinGroupResponseMember::default()
                    .member_id(first_member_id.clone())
                    .group_instance_id(None)
                    .metadata(first_member_range_meta.clone())
            )
        );
        assert_eq!(Some(first_member_id.as_str()), s.leader());

        let second_member_id = format!("{}-{}", CLIENT_ID, Uuid::new_v4());

        let (s, _) = s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                second_member_id.as_str(),
                group_instance_id,
                PROTOCOL_TYPE,
                Some(
                    &[
                        JoinGroupRequestProtocol::default()
                            .name(RANGE.into())
                            .metadata(second_member_range_meta.clone()),
                        JoinGroupRequestProtocol::default()
                            .name(COOPERATIVE_STICKY.into())
                            .metadata(second_member_sticky_meta),
                    ][..],
                ),
                reason,
            )
            .await;

        assert_eq!(1, s.generation_id());
        assert_eq!(2, s.members().len());
        assert_eq!(None, s.assignments());

        assert!(
            s.members().contains(
                &JoinGroupResponseMember::default()
                    .member_id(first_member_id.clone())
                    .group_instance_id(None)
                    .metadata(first_member_range_meta.clone())
            )
        );

        assert!(
            s.members().contains(
                &JoinGroupResponseMember::default()
                    .member_id(second_member_id.clone())
                    .group_instance_id(None)
                    .metadata(second_member_range_meta.clone())
            )
        );

        assert_eq!(Some(first_member_id.as_str()), s.leader());

        let (s, _) = s
            .sync(
                now,
                GROUP_ID,
                1,
                second_member_id.as_str(),
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&[]),
            )
            .await;

        assert_eq!(1, s.generation_id());
        assert_eq!(Some(first_member_id.as_str()), s.leader());
        assert_eq!(Some(first_member_id.as_str()), s.leader());
        assert_eq!(None, s.assignments());

        let assignments = [SyncGroupRequestAssignment::default()
            .member_id(first_member_id.clone())
            .assignment(Bytes::from_static(b"first_assignment_01"))];

        let (s, _) = s
            .sync(
                now,
                GROUP_ID,
                0,
                first_member_id.as_str(),
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments[..]),
            )
            .await;

        assert_eq!(1, s.generation_id());
        assert_eq!(Some(first_member_id.as_str()), s.leader());
        assert_eq!(None, s.assignments());

        let first_member_assignment = Bytes::from_static(b"first_assignment_02");
        let second_member_assignment = Bytes::from_static(b"second_assignment_02");

        let mut assignments = BTreeMap::new();
        _ = assignments.insert(first_member_id.clone(), first_member_assignment.clone());
        _ = assignments.insert(second_member_id.clone(), second_member_assignment.clone());

        let (s, _) = s
            .sync(
                now,
                GROUP_ID,
                1,
                first_member_id.as_str(),
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(
                    &assignments
                        .iter()
                        .map(|(member_id, assignment)| {
                            SyncGroupRequestAssignment::default()
                                .member_id(member_id.to_owned())
                                .assignment(assignment.to_owned())
                        })
                        .collect::<Vec<_>>()[..],
                ),
            )
            .await;

        assert_eq!(1, s.generation_id());
        assert_eq!(Some(first_member_id.as_str()), s.leader());
        assert_eq!(Some(first_member_id.as_str()), s.leader());

        assert_eq!(
            Some(first_member_assignment),
            s.assignments()
                .map(|assignments| assignments.get(first_member_id.as_str()).cloned())
                .unwrap()
        );

        assert_eq!(
            Some(second_member_assignment),
            s.assignments()
                .map(|assignments| assignments.get(second_member_id.as_str()).cloned())
                .unwrap()
        );

        Ok(())
    }

    /// Drive one consumer through the join/sync protocol against `controller`
    /// until it holds a non-empty assignment, the way a Kafka client would:
    /// first join (empty member id) → MemberIdRequired → re-join with the
    /// generated id; the leader syncs an assignment covering every member of
    /// its join response; RebalanceInProgress (or an empty assignment) means
    /// re-join the next round.
    async fn drive_member<O>(
        controller: Controller<O>,
        group_id: &'static str,
        index: usize,
    ) -> Result<(String, Bytes)>
    where
        O: Storage + Clone,
    {
        const SESSION_TIMEOUT_MS: i32 = 10_000;
        const REBALANCE_TIMEOUT_MS: Option<i32> = Some(15_000);

        let client_id = format!("member-{index:02}");

        let metadata = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .generation_id(Some(0))
                    .owned_partitions(
                        [TopicPartition::default().topic("t").partitions(0..3)].into_iter(),
                    ),
            ),
        )?;

        let protocols = [JoinGroupRequestProtocol::default()
            .name(Assignor::RANGE.into())
            .metadata(metadata)];

        let mut member_id = String::new();

        loop {
            let join = match controller
                .join(
                    Some(client_id.as_str()),
                    group_id,
                    SESSION_TIMEOUT_MS,
                    REBALANCE_TIMEOUT_MS,
                    member_id.as_str(),
                    None,
                    CONSUMER,
                    Some(&protocols[..]),
                    None,
                )
                .await?
            {
                Body::JoinGroupResponse(join) => join,
                otherwise => panic!("{otherwise:?}"),
            };

            if join.error_code == i16::from(ErrorCode::MemberIdRequired) {
                member_id = join.member_id.clone();
                continue;
            }

            assert_eq!(i16::from(ErrorCode::None), join.error_code);

            let assignments = if join.leader == join.member_id {
                join.members
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|member| {
                        SyncGroupRequestAssignment::default()
                            .member_id(member.member_id.clone())
                            .assignment(Bytes::from(format!("assignment-{}", member.member_id)))
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };

            let sync = match controller
                .sync(
                    group_id,
                    join.generation_id,
                    member_id.as_str(),
                    None,
                    Some(CONSUMER),
                    Some(Assignor::RANGE),
                    Some(&assignments[..]),
                )
                .await?
            {
                Body::SyncGroupResponse(sync) => sync,
                otherwise => panic!("{otherwise:?}"),
            };

            if sync.error_code == i16::from(ErrorCode::None) && !sync.assignment.is_empty() {
                return Ok((member_id, sync.assignment));
            }
        }
    }

    // Two `Controller`s sharing one `memory://` store model two broker
    // replicas behind a single load balancer: every join/sync races CAS
    // updates of the same `{group}.json` object. This is the scenario that
    // never converged before the join-window barrier — the leader's join
    // returned as soon as its own CAS landed, it computed assignments for a
    // partial member list, and everyone missing from that list parked at
    // "stable" with zero partitions.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_member_group_converges_across_replicas() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "convergence-group";
        const MEMBERS: usize = 8;

        let storage = StorageContainer::builder()
            .cluster_id("test")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let replicas = [
            Controller::with_storage(storage.clone())?,
            Controller::with_storage(storage.clone())?,
        ];

        let handles = (0..MEMBERS)
            .map(|index| {
                let controller = replicas[index % replicas.len()].clone();
                tokio::spawn(async move {
                    tokio::time::timeout(
                        Duration::from_secs(60),
                        drive_member(controller, GROUP_ID, index),
                    )
                    .await
                    .expect("member timed out before converging")
                })
            })
            .collect::<Vec<_>>();

        let mut assigned = BTreeMap::new();
        for handle in handles {
            let (member_id, assignment) = handle.await.expect("member task panicked")?;
            assert!(!assignment.is_empty());
            assert!(assigned.insert(member_id, assignment).is_none());
        }

        assert_eq!(MEMBERS, assigned.len());

        // The persisted group must be Formed with a non-empty assignment for
        // every one of the members, at a generation that is now stable.
        let (detail, version) = storage
            .read_group(GROUP_ID)
            .await?
            .expect("group must be persisted");
        let GroupState::Formed { assignments, .. } = detail.state else {
            panic!("expected Formed, got {:?}", detail.state);
        };
        for member_id in assigned.keys() {
            assert!(
                assignments
                    .get(member_id)
                    .is_some_and(|assignment| !assignment.is_empty()),
                "no assignment for {member_id}: {:?}",
                assignments.keys().collect::<Vec<_>>()
            );
        }

        let (again, version_again) = storage
            .read_group(GROUP_ID)
            .await?
            .expect("group still persisted");
        assert_eq!(detail.generation_id, again.generation_id);
        assert_eq!(version, version_again);

        Ok(())
    }

    // KIP-394 focused test: a first join with an empty member id replies
    // MemberIdRequired and parks the generated id — it must not register a
    // phantom member, bump the generation, or rewrite the persisted group.
    #[tokio::test]
    async fn member_id_required_join_leaves_persisted_group_unchanged() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "member-id-required-group";

        let storage = StorageContainer::builder()
            .cluster_id("test")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let s = Controller::with_storage(storage.clone())?;

        let range_meta = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .generation_id(Some(0))
                    .owned_partitions(
                        [TopicPartition::default().topic("a").partitions(0..3)].into_iter(),
                    ),
            ),
        )?;

        let protocols = [JoinGroupRequestProtocol::default()
            .name(Assignor::RANGE.into())
            .metadata(range_meta.clone())];

        // Establish a single-member Formed group.
        let member_id = match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                CONSUMER,
                Some(&protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse { member_id, .. }) => member_id,
            otherwise => panic!("{otherwise:?}"),
        };

        let generation_id = match s
            .join(
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                &member_id,
                group_instance_id,
                CONSUMER,
                Some(&protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse { generation_id, .. }) => generation_id,
            otherwise => panic!("{otherwise:?}"),
        };

        let assignments = [SyncGroupRequestAssignment::default()
            .member_id(member_id.clone())
            .assignment(Bytes::from_static(b"assignment_01"))];

        _ = s
            .sync(
                GROUP_ID,
                generation_id,
                &member_id,
                group_instance_id,
                Some(CONSUMER),
                Some(Assignor::RANGE),
                Some(&assignments[..]),
            )
            .await?;

        let (before, v1) = storage
            .read_group(GROUP_ID)
            .await?
            .expect("group must be persisted after sync");
        assert!(matches!(before.state, GroupState::Formed { .. }));

        // A second client's first join (empty member id) replies with a
        // generated id but must leave the persisted group untouched.
        match s
            .join(
                Some("other-client"),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                CONSUMER,
                Some(&protocols[..]),
                reason,
            )
            .await?
        {
            Body::JoinGroupResponse(JoinGroupResponse {
                error_code,
                member_id,
                ..
            }) => {
                assert_eq!(i16::from(ErrorCode::MemberIdRequired), error_code);
                assert!(member_id.starts_with("other-client"));
            }
            otherwise => panic!("{otherwise:?}"),
        }

        let (after, v2) = storage
            .read_group(GROUP_ID)
            .await?
            .expect("group still persisted");

        assert_eq!(before.generation_id, after.generation_id);
        assert_eq!(
            before.members.keys().collect::<Vec<_>>(),
            after.members.keys().collect::<Vec<_>>()
        );
        assert!(matches!(after.state, GroupState::Formed { .. }));
        assert_eq!(
            v1, v2,
            "a MemberIdRequired reply must not rewrite the group object"
        );

        Ok(())
    }

    // A member of the current generation that is missing from the assignments
    // map must be sent RebalanceInProgress so it re-joins — answering
    // `error=None` with an empty assignment parks the client at "stable" with
    // zero partitions.
    #[tokio::test]
    async fn sync_missing_from_assignments_is_rebalance_in_progress() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "missing-assignment-group";
        const RANGE: &str = "range";
        const PROTOCOL_TYPE: &str = "consumer";

        let now = SystemTime::now();

        let storage = StorageContainer::builder()
            .cluster_id("test")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        // Formed group where the assignments cover m1 but not m2.
        let formed = GroupState::Formed {
            protocol_type: PROTOCOL_TYPE.into(),
            protocol_name: RANGE.into(),
            leader: "m1".into(),
            assignments: [("m1".to_owned(), Bytes::from_static(b"assignment-m1"))]
                .into_iter()
                .collect(),
        };
        let s = Wrapper::with_storage_group_detail(
            storage.clone(),
            group_detail_with(&[("m1", Some(now)), ("m2", Some(now))], formed, 5),
        );

        let (s, body) = s
            .sync(
                now,
                GROUP_ID,
                5,
                "m2",
                None,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&[]),
            )
            .await;

        match body {
            Body::SyncGroupResponse(SyncGroupResponse {
                error_code,
                assignment,
                ..
            }) => {
                assert_eq!(i16::from(ErrorCode::RebalanceInProgress), error_code);
                assert!(assignment.is_empty());
            }
            otherwise => panic!("{otherwise:?}"),
        }

        // The group itself is untouched: still Formed at the same generation.
        assert!(matches!(s, Wrapper::Formed(_)));
        assert_eq!(5, s.generation_id());

        // While Forming, a leader sync whose assignments miss the syncing
        // member must not form the group either.
        let forming = GroupState::Forming {
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(RANGE.into()),
            leader: Some("m1".into()),
        };
        let s = Wrapper::with_storage_group_detail(
            storage,
            group_detail_with(&[("m1", Some(now)), ("m2", Some(now))], forming, 6),
        );

        let assignments = [SyncGroupRequestAssignment::default()
            .member_id("m2".into())
            .assignment(Bytes::from_static(b"assignment-m2"))];

        let (s, body) = s
            .sync(
                now,
                GROUP_ID,
                6,
                "m1",
                None,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments[..]),
            )
            .await;

        match body {
            Body::SyncGroupResponse(SyncGroupResponse {
                error_code,
                assignment,
                ..
            }) => {
                assert_eq!(i16::from(ErrorCode::RebalanceInProgress), error_code);
                assert!(assignment.is_empty());
            }
            otherwise => panic!("{otherwise:?}"),
        }

        assert!(s.is_forming());
        assert_eq!(6, s.generation_id());

        Ok(())
    }
}
