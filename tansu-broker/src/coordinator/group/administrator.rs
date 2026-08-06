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
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge},
};
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
    ConsumerGroupState, GroupDetail, GroupMember, GroupState, OffsetCommitRequest, Storage,
    Topition, UpdateError, Version,
};
use tokio::time::{Duration, Instant, sleep};
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

/// Whether two encoded member subscriptions request the SAME set of topics.
///
/// A cooperative consumer (kafka-clients, KIP-792) re-encodes its subscription
/// on every rejoin with a fresh `generationId` and `ownedPartitions` (and, for
/// the sticky assignor, `userData`), so the raw metadata bytes differ every
/// time even when the member still subscribes to exactly the same topics.
/// Bumping the group generation on such a no-op rejoin invalidates any
/// in-flight leader SyncGroup (its generation is now stale) and stretches — or,
/// with 16 members churning, prevents — convergence. Comparing the decoded
/// topic set instead of the raw bytes lets a known member's KIP-792-only rejoin
/// be treated as a no-op update.
///
/// Conservative on uncertainty: if either side cannot be decoded as a consumer
/// subscription, returns `false` so the caller keeps its safe,
/// rebalance-triggering path — an undecodable or genuinely changed subscription
/// never silently skips a rebalance.
fn same_subscription_topics(a: &Bytes, b: &Bytes) -> bool {
    match (
        MemberMetadata::try_from(a.clone()),
        MemberMetadata::try_from(b.clone()),
    ) {
        (Ok(a), Ok(b)) => {
            let mut at = a.subscription.topics;
            let mut bt = b.subscription.topics;
            at.sort();
            bt.sort();
            at == bt
        }
        _ => false,
    }
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

/// What a `join` or `sync` iteration does once this member's group state is
/// settled: wait and re-poll, or answer now.
///
/// Both APIs reach this decision twice per iteration — once on the no-op
/// long-poll skip and once after the state CAS — and the two copies differ only
/// in which of them wrote. Holding it in one place per API means a change to
/// the hold conditions cannot land on one branch and miss the other (#286),
/// and leaves the genuine protocol difference between `join` and `sync` stated
/// where it can be read rather than buried in a pasted `else if` chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LongPoll {
    /// Sleep for this long, then run another iteration. The label names the
    /// wait in `COORDINATOR_REQUESTS`.
    Wait(Duration, &'static str),

    /// Answer with the body this iteration produced, first overriding its error
    /// code when `Some`.
    Respond(Option<ErrorCode>),
}

/// `join`'s long-poll decision (see [`LongPoll`]).
///
/// A static member is paused out to [`PAUSE_MS`] so its rejoin does not spin.
/// The leader of a still-`Forming` group is then held by the join-window
/// barrier until membership is quiescent or the rebalance window closes, so it
/// assigns over the complete member list. Any member with something to act on —
/// leader, assigned, or told to retry under a broker-issued member id — is
/// answered immediately; anyone else waits up to **half** its session timeout
/// and is then answered anyway.
fn join_long_poll<O>(
    updated: &Wrapper<O>,
    body: &Body,
    group_instance_id: Option<&str>,
    elapsed_ms: u128,
    is_forming: bool,
    membership_quiescent: bool,
    join_window_ms: u128,
) -> LongPoll
where
    O: Storage,
{
    let is_leader = updated.is_leader(body);
    let is_assigned = updated.is_assigned(body);
    let is_member_id_required = updated.is_member_id_required(body);
    let is_ok = updated.is_ok(body);
    let session_timeout_ms = updated.session_timeout_ms() as u128;

    let decision = if group_instance_id.is_some() && elapsed_ms < PAUSE_MS {
        LongPoll::Wait(
            Duration::from_millis(PAUSE_MS.saturating_sub(elapsed_ms) as u64),
            "join_group_instance_pause",
        )
    } else if is_leader && is_forming && !membership_quiescent && elapsed_ms < join_window_ms {
        LongPoll::Wait(Duration::from_secs(1), "join_window_hold")
    } else if is_leader || is_assigned || is_member_id_required {
        LongPoll::Respond(None)
    } else if elapsed_ms < session_timeout_ms.div(2) {
        LongPoll::Wait(Duration::from_secs(1), "join_group_instance_pause")
    } else {
        LongPoll::Respond(None)
    };

    debug!(
        elapsed_ms,
        is_forming,
        is_leader,
        is_assigned,
        is_member_id_required,
        is_ok,
        membership_quiescent,
        join_window_ms,
        session_timeout_ms,
        ?decision,
    );

    decision
}

/// `sync`'s long-poll decision (see [`LongPoll`]).
///
/// The same static-member pause as [`join_long_poll`]. A member is answered as
/// soon as the group has fallen back to `Forming` (a fresh rebalance to follow)
/// or its assignment has arrived. Otherwise it waits up to **eight tenths** of
/// its session timeout — longer than `join`'s half, because a sync is waiting
/// on the leader's single assignment write — and is then terminated with
/// `RebalanceInProgress`, so the client rejoins instead of settling on an empty
/// assignment.
fn sync_long_poll<O>(
    updated: &Wrapper<O>,
    body: &Body,
    group_instance_id: Option<&str>,
    elapsed_ms: u128,
) -> LongPoll
where
    O: Storage,
{
    let is_forming = updated.is_forming();
    let is_assigned = updated.is_assigned(body);
    let is_ok = updated.is_ok(body);
    let session_timeout_ms = updated.session_timeout_ms() as u128;

    let decision = if group_instance_id.is_some() && elapsed_ms < PAUSE_MS {
        LongPoll::Wait(
            Duration::from_millis(PAUSE_MS.saturating_sub(elapsed_ms) as u64),
            "sync_group_instance_pause",
        )
    } else if is_forming || is_assigned {
        LongPoll::Respond(None)
    } else if elapsed_ms < session_timeout_ms.mul(8).div(10) {
        LongPoll::Wait(Duration::from_secs(1), "sync_group_instance_pause")
    } else {
        LongPoll::Respond(Some(ErrorCode::RebalanceInProgress))
    };

    debug!(
        elapsed_ms,
        is_forming,
        is_assigned,
        is_ok,
        session_timeout_ms,
        ?decision,
    );

    decision
}

/// Warn when the leader's assignment hands one partition to more than one
/// member — two consumers reading the same partition, which the group protocol
/// is supposed to make impossible.
///
/// Diagnostic only: the assignment is the leader's to make, so this reports
/// rather than rejects. Lifted out of the `sync` loop because it inspects the
/// request the caller sent, which does not change between iterations.
fn warn_on_overlapping_assignments(
    group_id: &str,
    generation_id: i32,
    assignments: Option<&[SyncGroupRequestAssignment]>,
) {
    let Some(assignments) = assignments else {
        return;
    };

    if assignments.is_empty() {
        return;
    }

    if has_unique_elements(
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
    ) {
        return;
    }

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

/// The `COORDINATOR_REQUESTS` labels one API's CAS loop emits, so the shared
/// driver can count per-API without building a string per iteration — every
/// member heartbeats every few seconds, so this is a hot path.
#[derive(Clone, Copy, Debug)]
struct CasLabels {
    /// One per loop iteration.
    iteration: &'static str,

    /// The no-op skip fired: the group object was left alone.
    noop_skip: &'static str,

    /// The CAS lost to a concurrent writer.
    outdated: &'static str,
}

/// The per-API half of [`Controller::run_group_cas_loop`].
///
/// The driver owns everything the five group-mutating APIs do identically — the
/// group lock, the `wrappers` cache, the GET-first reconcile, the CAS and its
/// five-arm `UpdateError` match, the conflict backoff — and an implementation
/// supplies only what genuinely differs.
///
/// That split is the whole point (#286). The sequence was written out five
/// times, so #111 and #256 each had to be applied five times, and #273 was the
/// heartbeat copy of a skip that join and sync had already moved on from: a
/// `GroupDetail` equality that could never hold, so the skip fired only on
/// errors and every member wrote a CAS per interval. With one definition a fix
/// reaches all five by construction, and what an API does differently is
/// stated in its implementation rather than buried in a pasted branch.
///
/// An implementation is built once per request and owns that request's
/// arguments and its across-iteration state, which is why the hooks take
/// `&mut self` and the driver holds none of it.
trait GroupCas<O>: Send
where
    O: Storage + Clone,
{
    const LABELS: CasLabels;

    /// Read `{group}.json` before evaluating the request (#111), so a change
    /// made on another replica is observed by a cheap read rather than learnt
    /// from a failed CAS. It is also what arms [`Self::skip`]: with no persisted
    /// object there is nothing to be equal to, so the create must go through.
    ///
    /// `leave` opts out. It is terminal — nothing to long-poll, no skip to arm —
    /// so the read would buy nothing the CAS does not already provide.
    const GET_FIRST: bool = true;

    /// Sleep between CAS conflicts. Only the long-polling APIs do: join and sync
    /// have every member of a group converging on one object, so an
    /// un-backed-off retry storm is real. The others answer in a single pass and
    /// retry immediately.
    const BACKOFF_ON_CONFLICT: bool = false;

    /// The `Wrapper` to start from when the in-process cache misses.
    fn seed(&self, storage: O) -> Wrapper<O> {
        Wrapper::Forming(Inner::new(storage))
    }

    /// Everything between the reconcile and the CAS: `missed_heartbeat` where
    /// the API wants it, then the delegation to `Wrapper`.
    fn apply(
        &mut self,
        group_id: &str,
        now: SystemTime,
        wrapper: Wrapper<O>,
    ) -> impl Future<Output = (Wrapper<O>, Body)> + Send;

    /// Report a group that has stopped making progress (#240), from the state
    /// this iteration read (`before`) or produced (`after`), per API.
    fn observe(
        &self,
        _controller: &Controller<O>,
        _group_id: &str,
        _before: &GroupDetail,
        _after: &GroupDetail,
        _now: SystemTime,
    ) {
    }

    /// Whether this iteration changed nothing worth a PUT (#111). Consulted only
    /// when the group object was actually read, so an API with
    /// [`Self::GET_FIRST`] off can never skip.
    fn skip(&self, _before: &GroupDetail, _after: &GroupDetail, _now: SystemTime) -> bool {
        false
    }

    /// What to do now that this iteration's state is settled — whether it was
    /// settled by the skip or by a landed CAS. The non-polling APIs answer
    /// immediately; join and sync defer to their long-poll decision.
    fn settled(
        &mut self,
        _updated: &Wrapper<O>,
        _body: &Body,
        _after: &GroupDetail,
        _now: SystemTime,
    ) -> LongPoll {
        LongPoll::Respond(None)
    }
}

static COORDINATOR_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_group_coordinator_requests")
        .with_description("consumer group coordinator requests")
        .build()
});

/// Rebalances observed still incomplete past [`stall_after`] (#240).
///
/// Deliberately unlabelled: this deployment has thousands of groups, so a
/// `group` label would be unbounded cardinality. The group's identity goes in
/// the accompanying `warn!`, which is what an operator reads once this fires.
static REBALANCE_STALLS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_group_rebalance_stalled")
        .with_description("rebalances still incomplete past the stall threshold")
        .build()
});

/// Per-group state evicted from this replica's in-process maps (#283) — the
/// numerator whose growth against `tansu_group_coordinator_cached` says whether
/// the sweep is keeping up with group churn.
static GROUPS_EVICTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_group_coordinator_evicted")
        .with_description("groups whose process-local coordinator state was evicted when idle")
        .build()
});

/// Groups whose state this replica still holds in memory after a sweep (#283).
///
/// A counter cannot answer the question this exists for: the maps grew
/// monotonically, so what matters is whether the *level* is flat under churn, not
/// how many entries were ever made. Unlabelled — thousands of groups, so a
/// `group` label would be unbounded cardinality.
static GROUPS_CACHED: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_group_coordinator_cached")
        .with_description("groups held in this replica's process-local coordinator maps")
        .build()
});

/// How long a group must go unserved before its process-local coordinator state
/// is evicted (#283).
///
/// Comfortably above every window a *live* group can be quiet for — the session
/// timeout, the rebalance timeout, the join long-poll — because the cost of
/// being wrong in that direction is not a stale answer (every path re-reads the
/// group object GET-first, so an evicted entry costs a cache miss) but pointless
/// re-reads on a busy group. Well below the pod lifetime, which is the window
/// the growth was previously bounded by.
///
/// Not tied to the storage engine's group-expiry threshold: this is not a
/// statement about when a group is dead, only about when this replica has
/// stopped hearing from one.
pub const GROUP_STATE_IDLE_AFTER: Duration = Duration::from_mins(30);

/// Floor for [`stall_after`] — the threshold used when a group declares no
/// rebalance timeout of its own, and the minimum applied to one that declares
/// an implausibly short value (#240).
///
/// Measured, after the first hour of this detector in production: large groups
/// (9-14 members) routinely complete a rebalance in **just over a minute** —
/// observed 60.1s, 60.4s, 62.9s, 67.2s, 81.2s, each recovering on its own. The
/// 60s this shipped with fired ~2 per minute fleet-wide on healthy groups, which
/// is how a signal becomes one operators learn to scroll past. 120s clears that
/// observed tail with margin and is still three orders of magnitude below the
/// hours the incident ran.
const REBALANCE_STALL_FLOOR: Duration = Duration::from_secs(120);

/// How long `group` may sit in `CompletingRebalance` before it is reported
/// (#240).
///
/// Taken from the group's **own** `rebalance_timeout_ms` where it declares one.
/// That is the interval its members told the coordinator to wait for a rejoin,
/// so still forming past it is anomalous by the group's own definition — and it
/// scales with the group instead of against it, which a constant cannot: the
/// same threshold cannot be right for a 2-member group and a 14-member one.
///
/// A group reaches this state when its members have joined and a leader is
/// elected but `SyncGroup` has not distributed the assignment. In the incident
/// this exists for it lasted **hours**, silently: the broker reported the group
/// as existing with all its members while the clients waited for an assignment
/// that never came, and the only symptom was "the worker consumes nothing".
fn stall_after(detail: &GroupDetail) -> Duration {
    detail
        .rebalance_timeout_ms
        .filter(|ms| *ms > 0)
        .map_or(REBALANCE_STALL_FLOOR, |ms| {
            Duration::from_millis(ms as u64).max(REBALANCE_STALL_FLOOR)
        })
}

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

/// Per-group in-process serialization locks. Now that group-coordination
/// requests are forwarded to a single deterministic owner replica (see
/// `coordinator::group::forward`), all of a group's members land on the same
/// `Controller`. Without in-process serialization their read-modify-write
/// cycles against the single `{group}.json` object interleave and thrash the
/// etag CAS — 16 members produced a permanent `cas_conflicts` storm and the
/// group never reached a stable generation. Holding a per-group async lock
/// across each read->CAS window turns those concurrent RMWs into a sequence of
/// single successful CAS writes. The lock is *released* before every long poll
/// / join-window sleep, so a held leader never blocks the very members it is
/// waiting for. The object-store etag CAS remains the cross-replica correctness
/// backstop (a forward timeout falls back to local processing on another
/// replica, which this in-process lock cannot serialize).
///
/// The map is keyed by group id, so it grows with the set of groups this replica
/// has *ever* served rather than the set it currently serves — a
/// group-per-restart or group-per-subscription naming pattern grew it without
/// bound. Swept by [`Controller::prune`] (#283); an entry whose `Arc` the map is
/// not the sole holder of is left alone, see there.
type GroupLocks = Arc<Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>>;

#[derive(Clone, Debug)]
pub struct Controller<O> {
    storage: O,
    wrappers: WrapperMap<O>,
    group_locks: GroupLocks,
    /// When each group was first seen mid-rebalance, and whether that has been
    /// reported yet (#240).
    ///
    /// Per-replica and in-process, deliberately: the alternative is a
    /// state-entry timestamp inside `GroupDetail`, which is a persisted-format
    /// change, and `inception` cannot stand in for one — it is set once at group
    /// creation and copied through every transition, so it measures the group's
    /// age, not how long it has been rebalancing. The cost of staying in memory
    /// is a blind window after a restart, which is exactly when a wedge is
    /// created; against a condition that lasted hours that is an acceptable
    /// trade, and it needs no rolling-deploy care.
    rebalance_stalls: Arc<Mutex<BTreeMap<String, (SystemTime, bool)>>>,

    /// When each group was last served by this replica — the touch stamp
    /// [`Self::prune`] evicts on (#283).
    ///
    /// Written by [`Self::group_lock`], which every request path that populates
    /// any of the maps above calls *before* it touches them. That ordering is
    /// what makes the sweep leak-free: see [`Self::prune`].
    ///
    /// On the MONOTONIC clock, unlike [`Self::rebalance_stalls`]: nothing here is
    /// persisted or compared against a stored timestamp, and a backwards NTP step
    /// would otherwise make `elapsed()` fail and freeze the sweep (#256).
    last_seen: Arc<Mutex<BTreeMap<String, Instant>>>,

    /// How long a group must go unserved before [`Self::prune`] evicts its
    /// state (#283). Lowered in tests so the sweep can be exercised without
    /// waiting one out.
    idle_after: Duration,
}

impl<O> Controller<O>
where
    O: Storage + Clone,
{
    pub fn with_storage(storage: O) -> Result<Self> {
        Ok(Self {
            storage,
            wrappers: Arc::new(Mutex::new(BTreeMap::new())),
            group_locks: Arc::new(Mutex::new(BTreeMap::new())),
            rebalance_stalls: Arc::new(Mutex::new(BTreeMap::new())),
            last_seen: Arc::new(Mutex::new(BTreeMap::new())),
            idle_after: GROUP_STATE_IDLE_AFTER,
        })
    }

    /// Override the idle window before [`Self::prune`] evicts a group's state
    /// (#283). Tests set it to zero to sweep without waiting.
    pub fn with_idle_after(self, idle_after: Duration) -> Self {
        Self { idle_after, ..self }
    }

    /// Report a group that has been mid-rebalance too long (#240).
    ///
    /// A group in `CompletingRebalance` — members joined, leader elected, no
    /// assignment distributed — past [`stall_after`] is always a bug:
    /// consumption has stopped and nothing else says so. The broker answers
    /// every request about the group normally, the clients wait, and the
    /// backlog sits behind it.
    ///
    /// Reported once per episode rather than per heartbeat: members heartbeat
    /// every few seconds, so an unguarded `warn!` here would emit thousands of
    /// lines for one stuck group and bury the signal it exists to raise. The
    /// entry clears as soon as the group leaves the state, so a group that
    /// stalls, recovers and stalls again is reported each time.
    fn observe_rebalance(&self, group_id: &str, detail: &GroupDetail, now: SystemTime) -> bool {
        let stalled = matches!(
            ConsumerGroupState::from(detail),
            ConsumerGroupState::CompletingRebalance
        );

        let Ok(mut stalls) = self.rebalance_stalls.lock() else {
            return false;
        };

        if !stalled {
            _ = stalls.remove(group_id);
            return false;
        }

        let threshold = stall_after(detail);
        let (since, reported) = stalls.entry(group_id.to_owned()).or_insert((now, false));
        let stalled_for = now.duration_since(*since).unwrap_or_default();

        if !*reported && stalled_for >= threshold {
            *reported = true;
            REBALANCE_STALLS.add(1, &[]);

            // `threshold_ms` is in the line on purpose: it is the group's own
            // declared rebalance timeout, so an operator can tell "this group is
            // stuck" from "this group declares an unusually tight timeout"
            // without going to read the config.
            warn!(
                group_id,
                members = detail.members.len(),
                stalled_for_ms = stalled_for.as_millis() as u64,
                threshold_ms = threshold.as_millis() as u64,
                "group has not completed its rebalance: members joined, no assignment distributed (#240)"
            );

            return true;
        }

        false
    }

    /// The serialization lock for `group_id`, created on first use. See
    /// [`GroupLocks`]. Callers hold the returned guard across a single
    /// read->CAS window and drop it before any long-poll / rebalance sleep.
    ///
    /// Also the single touch point for [`Self::last_seen`] (#283). Every request
    /// path that populates a per-group map calls this first, so one stamp here
    /// covers all of them — and covers them from *before* the wrapper is checked
    /// out, which is what [`Self::prune`] relies on.
    fn group_lock(&self, group_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        if let Ok(mut last_seen) = self.last_seen.lock() {
            _ = last_seen.insert(group_id.to_owned(), Instant::now());
        }

        self.group_locks
            .lock()
            .expect("group_locks mutex poisoned")
            .entry(group_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// The body of [`Coordinator::prune`] for this controller (#283) — see there
    /// for what it evicts and why that is safe.
    ///
    /// Inherent so the trait impl below is a one-liner and the reasoning lives
    /// next to the maps it reasons about.
    ///
    /// The four maps swept here were keyed by a client-controlled string and had
    /// no removal path at all — not even when the group was deleted from the
    /// object store — so a group-per-restart or group-per-subscription naming
    /// pattern grew them for the life of the pod. `wrappers` is the expensive one:
    /// it holds each group's full member-metadata byte blobs.
    ///
    /// **Idle-triggered rather than delete-triggered**, which is a deliberate
    /// choice and not an approximation of one. The two events the issue names —
    /// `DeleteGroups` and group expiry — both happen inside the storage engine,
    /// which has no handle on the broker's coordinator, so wiring either as a
    /// callback would invert the layering. Idleness needs no such handle and
    /// covers strictly more: a group nobody deleted and nothing expired, because
    /// its committed offsets are still there, but whose consumers are gone for
    /// good, is the shape the observed naming patterns actually produce.
    ///
    /// **Safe because none of this is authority.** Every request path re-reads
    /// `{group}.json` GET-first and adopts the persisted state whenever its cached
    /// version differs — and a freshly created wrapper has no version, so it
    /// always adopts. `inception` (the one field that could not be recomputed) is
    /// persisted in `GroupDetail`. So an eviction costs one object read on the
    /// group's next request and can change no answer.
    ///
    /// **`group_locks` is the exception, and needs the `Arc` count.** Dropping a
    /// lock another task holds does not free it — it means the *next* request
    /// creates a second lock for the same group, and two members then run their
    /// read->CAS windows concurrently, which is exactly the CAS thrash
    /// [`GroupLocks`] exists to prevent. `Arc::strong_count == 1` rules that out:
    /// the map is the only holder, so no request is between [`Self::group_lock`]
    /// and its end (`lock_owned` moves the `Arc` into the guard, and the join/sync
    /// loops keep their own clone for the whole call, so a live request always
    /// holds one).
    ///
    /// That check is also what makes the sweep **leak-free**, which it would not
    /// otherwise be: a request that has checked its wrapper *out* of `wrappers`
    /// and not yet put it back would be invisible to a sweep keyed on the map's
    /// contents, and the re-insert afterwards would strand an entry with no touch
    /// stamp — never a candidate again. Since the stamp is written before the
    /// checkout and the `Arc` is held across it, such a group is skipped whole
    /// (stamp included) and reconsidered on the next sweep.
    fn prune_idle_groups(&self) -> usize {
        let now = Instant::now();

        // Candidates decided from `last_seen` alone, then confirmed per group
        // under the lock below. Collected first so no two of these mutexes are
        // ever held at once.
        let Ok(idle) = self.last_seen.lock().map(|last_seen| {
            last_seen
                .iter()
                .filter(|(_, seen)| now.duration_since(**seen) >= self.idle_after)
                .map(|(group_id, _)| group_id.to_owned())
                .collect::<Vec<_>>()
        }) else {
            return 0;
        };

        let mut evicted = 0;

        for group_id in idle {
            // Serving now: leave every one of this group's entries, stamp
            // included, and reconsider it next sweep.
            let released = self.group_locks.lock().is_ok_and(|mut group_locks| {
                match group_locks.get(&group_id) {
                    Some(lock) if Arc::strong_count(lock) == 1 => {
                        _ = group_locks.remove(&group_id);
                        true
                    }

                    // No lock entry at all: nothing can be mid-request, since
                    // `group_lock` creates one before anything else happens.
                    None => true,

                    Some(_) => false,
                }
            });

            if !released {
                continue;
            }

            if let Ok(mut wrappers) = self.wrappers.lock() {
                _ = wrappers.remove(&group_id);
            }

            if let Ok(mut stalls) = self.rebalance_stalls.lock() {
                _ = stalls.remove(&group_id);
            }

            if let Ok(mut last_seen) = self.last_seen.lock() {
                _ = last_seen.remove(&group_id);
            }

            evicted += 1;
        }

        let cached = self
            .last_seen
            .lock()
            .map_or(0, |last_seen| last_seen.len() as u64);

        GROUPS_CACHED.record(cached, &[]);

        if evicted > 0 {
            GROUPS_EVICTED.add(evicted as u64, &[]);
            debug!(evicted, cached, "evicted idle group state");
        }

        evicted
    }

    /// Put a group's wrapper back in the in-process cache.
    ///
    /// A poisoned mutex propagates rather than being swallowed: the cache is not
    /// authority — every path re-reads `{group}.json` GET-first — but a poisoned
    /// lock means another task panicked mid-update, and answering as if nothing
    /// happened is how that becomes someone else's mystery.
    fn cache(&self, group_id: &str, wrapper: Wrapper<O>, version: Option<Version>) -> Result<()> {
        self.wrappers.lock().map(|mut wrappers| {
            _ = wrappers.insert(group_id.to_owned(), (wrapper, version));
        })?;

        Ok(())
    }

    /// The read-modify-CAS loop shared by `join`, `sync`, `leave`,
    /// `offset_commit` and `heartbeat` (#286). See [`GroupCas`] for the split
    /// between what lives here and what an API supplies.
    ///
    /// One iteration: take the group lock, check the wrapper out of the cache,
    /// reconcile it against the persisted object, delegate to the API, then
    /// either skip the write or CAS it. A conflict re-seeds the cache from the
    /// state that won and iterates; a long-poll decision sleeps and iterates.
    async fn run_group_cas_loop<C>(&self, group_id: &str, mut op: C) -> Result<Body>
    where
        C: GroupCas<O>,
    {
        let mut iteration = 0;
        let mut cas_conflicts = 0u32;

        // Cloned once for the whole call rather than per iteration, which
        // [`Self::prune`] depends on: it reads `Arc::strong_count` to decide a
        // group is not being served, so a request that sleeps between iterations
        // must keep a clone alive across the sleep or the sweep can drop the lock
        // out from under it and let a second one be created.
        let group_lock = self.group_lock(group_id);

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", C::LABELS.iteration)]);

            // Serialize this group's read->CAS window against other in-process
            // members (see `GroupLocks`). Released below before any sleep, so a
            // held leader does not block the members it is waiting for.
            let permit = group_lock.clone().lock_owned().await;

            let now = SystemTime::now();

            let (mut wrapper, mut version) = self
                .wrappers
                .lock()
                .map(|mut wrappers| wrappers.remove(group_id))?
                .unwrap_or_else(|| {
                    debug!(?iteration, ?group_id, "group cache miss");
                    (op.seed(self.storage.clone()), None)
                });

            // GET-first (#111): see [`GroupCas::GET_FIRST`].
            let persisted = if C::GET_FIRST {
                let persisted = self.storage.read_group(group_id).await?;

                if let Some((current, current_version)) = &persisted
                    && version.as_ref() != Some(current_version)
                {
                    wrapper =
                        Wrapper::with_storage_group_detail(self.storage.clone(), current.clone());
                    version = Some(current_version.clone());
                }

                persisted.is_some()
            } else {
                false
            };

            // The persisted projection we are about to (maybe) rewrite.
            let before = GroupDetail::from(&wrapper);

            debug!(?group_id, %wrapper, ?version, ?iteration);

            let (updated, body) = op.apply(group_id, now, wrapper).await;

            let after = GroupDetail::from(&updated);

            op.observe(self, group_id, &before, &after, now);

            debug!(?group_id, %updated, ?version, ?iteration);

            // Decided once and used by both arms below. The two used to be
            // separate copies of the same decision, differing only in which of
            // them had done the writing (#286).
            let decision = op.settled(&updated, &body, &after, now);

            if persisted && op.skip(&before, &after, now) {
                COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", C::LABELS.noop_skip)]);

                self.cache(group_id, updated, version)?;
            } else {
                match self.storage.update_group(group_id, after, version).await {
                    Ok(version) => {
                        debug!(?group_id, ?version, iteration, ?decision);

                        self.cache(group_id, updated, Some(version))?;
                    }

                    Err(UpdateError::Outdated { current, version }) => {
                        debug!(?group_id, ?current, ?version, iteration);
                        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", C::LABELS.outdated)]);

                        self.cache(
                            group_id,
                            Wrapper::with_storage_group_detail(self.storage.clone(), *current),
                            Some(version),
                        )?;

                        // Release before the backoff sleep. An in-process
                        // conflict is now rare (members serialize on the group
                        // lock); this path mainly catches cross-replica writes.
                        drop(permit);

                        cas_conflicts += 1;

                        if C::BACKOFF_ON_CONFLICT {
                            if cas_conflicts == CAS_CONFLICT_WARN {
                                warn!(
                                    group_id,
                                    cas_conflicts,
                                    method = C::LABELS.iteration,
                                    "repeated group-state CAS conflicts (concurrent members across replicas?)"
                                );
                            }

                            sleep(cas_conflict_backoff(cas_conflicts)).await;
                        }

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

            // The read->CAS window is closed either way; release before any
            // long-poll sleep.
            drop(permit);

            match decision {
                LongPoll::Respond(None) => return Ok(body),

                LongPoll::Respond(Some(error_code)) => return Ok(set_error_code(body, error_code)),

                LongPoll::Wait(pause, method) => {
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", method)]);
                    sleep(pause).await;

                    iteration += 1;
                }
            }
        }
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

        self.run_group_cas_loop(
            group_id,
            JoinCas {
                client_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id,
                group_instance_id,
                protocol_type,
                protocols,
                reason,

                // How long this call has been long-polling, on the MONOTONIC
                // clock (#256). Deliberately not the wall clock: `SystemTime`
                // makes `elapsed()` return `Err` after a backwards NTP step —
                // which `?` turned into an error response for a member that was
                // merely waiting — and it cannot be paused, so a test could only
                // buy determinism by waiting out the real duration. `SystemTime`
                // stays for everything persisted or compared against a stored
                // timestamp (`last_contact`, `inception`, and the join-window
                // barrier below).
                polling_since: Instant::now(),

                window_members: None,
                window_changed_at: SystemTime::now(),
            },
        )
        .await
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

        // Once per request, not once per iteration: this inspects the
        // assignments the caller sent, which do not change between retries.
        warn_on_overlapping_assignments(group_id, generation_id, assignments);

        self.run_group_cas_loop(
            group_id,
            SyncCas {
                generation_id,
                member_id,
                group_instance_id,
                protocol_type,
                protocol_name,
                assignments,

                // Monotonic, for the same reasons as `join`'s (#256). Nothing
                // here is compared against a stored timestamp, so this call
                // needs no wall clock at all.
                polling_since: Instant::now(),
            },
        )
        .await
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

        self.run_group_cas_loop(group_id, LeaveCas { member_id, members })
            .await
    }
    #[instrument(skip(self, offset_commit), fields(group_id = offset_commit.group_id, generation_id = offset_commit.generation_id_or_member_epoch))]
    async fn offset_commit(&self, offset_commit: OffsetCommit<'_>) -> Result<Body> {
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_commit")]);

        let group_id = offset_commit.group_id;

        self.run_group_cas_loop(group_id, OffsetCommitCas { offset_commit })
            .await
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

        self.run_group_cas_loop(
            group_id,
            HeartbeatCas {
                generation_id,
                member_id,
                group_instance_id,
            },
        )
        .await
    }

    fn prune(&self) -> usize {
        self.prune_idle_groups()
    }
}

/// `join`'s half of [`Controller::run_group_cas_loop`].
struct JoinCas<'a> {
    client_id: Option<&'a str>,
    session_timeout_ms: i32,
    rebalance_timeout_ms: Option<i32>,
    member_id: &'a str,
    group_instance_id: Option<&'a str>,
    protocol_type: &'a str,
    protocols: Option<&'a [JoinGroupRequestProtocol]>,
    reason: Option<&'a str>,

    /// When this call started long-polling, on the monotonic clock (#256).
    polling_since: Instant,

    /// Join-window barrier state (see `JOIN_QUIESCENCE`): the member set last
    /// observed by this join call, and when it last changed. Inferred purely
    /// from the per-iteration GET-first reads — no on-disk format change.
    window_members: Option<BTreeSet<String>>,
    window_changed_at: SystemTime,
}

impl<O> GroupCas<O> for JoinCas<'_>
where
    O: Storage + Clone,
{
    const LABELS: CasLabels = CasLabels {
        iteration: "join_loop",
        noop_skip: "join_noop_skip",
        outdated: "join_outdated",
    };

    // Many members converge on one group object during a rebalance, so an
    // un-backed-off retry storm here is real.
    const BACKOFF_ON_CONFLICT: bool = true;

    /// A join is the one request that can create a group, so its cache miss
    /// seeds from the timeouts the joining member declared rather than from
    /// `Inner::new`'s defaults.
    fn seed(&self, storage: O) -> Wrapper<O> {
        Wrapper::Forming(Inner {
            session_timeout_ms: self.session_timeout_ms,
            rebalance_timeout_ms: self.rebalance_timeout_ms,
            members: Default::default(),
            generation_id: -1,
            state: Forming::default(),
            skip_assignment: Some(false),
            storage,
            inception: SystemTime::now(),
        })
    }

    async fn apply(
        &mut self,
        group_id: &str,
        now: SystemTime,
        mut wrapper: Wrapper<O>,
    ) -> (Wrapper<O>, Body) {
        if self.group_instance_id.is_none() {
            wrapper = wrapper.missed_heartbeat(group_id, self.member_id, now);
        }

        let members_before = wrapper.members();

        let (updated, body) = wrapper
            .join(
                now,
                self.client_id,
                group_id,
                self.session_timeout_ms,
                self.rebalance_timeout_ms,
                self.member_id,
                self.group_instance_id,
                self.protocol_type,
                self.protocols,
                self.reason,
            )
            .await;

        debug!(is_stable = members_before == updated.members());

        (updated, body)
    }

    // Observed here as well as on the heartbeat path (#240).
    //
    // Heartbeat alone is blind to the failure this detector exists for.
    // `AbstractCoordinator$HeartbeatThread` disables itself while the consumer
    // has not joined — `if (state.hasNotJoinedGroup() || isFailed()) {
    // disable(); }` — so a group whose members are stuck *trying* to join sends
    // no heartbeat at all, and was sampled nowhere. Measured: a group 90+
    // minutes in `CompletingRebalance` with 16 members, all 16 heartbeat threads
    // parked in `wait()`, and not one line about it across ten broker replicas
    // over two hours.
    //
    // Join and sync keep arriving from exactly that consumer — every `poll()`
    // retries the join — so they are a live sampling source precisely when
    // heartbeats are not. `before` is the group as read this iteration, which is
    // the state being waited on.
    fn observe(
        &self,
        controller: &Controller<O>,
        group_id: &str,
        before: &GroupDetail,
        _after: &GroupDetail,
        now: SystemTime,
    ) {
        _ = controller.observe_rebalance(group_id, before, now);
    }

    /// A member waiting through a rebalance re-joins once a second, and each
    /// re-join changes only its own `last_contact` (via `missed_heartbeat`).
    /// Persisting that is a CAS that churns the `{group}.json` etag for nothing
    /// and, at scale, starves the leader's assignment write so the group never
    /// stabilises. When the rebalance state is otherwise unchanged and this
    /// member's liveness does not yet need renewing, answer without touching the
    /// object — keeping the etag still long enough for the assignment CAS to
    /// land.
    fn skip(&self, before: &GroupDetail, after: &GroupDetail, now: SystemTime) -> bool {
        same_rebalance_state(before, after)
            && !liveness_renewal_due(before, self.member_id, now, before.session_timeout_ms)
    }

    fn settled(
        &mut self,
        updated: &Wrapper<O>,
        body: &Body,
        after: &GroupDetail,
        now: SystemTime,
    ) -> LongPoll {
        // Join-window barrier: without it the leader returned as soon as its own
        // CAS landed, computed assignments for whatever partial member list it
        // had seen, and every member missing from that list parked at "stable"
        // with zero partitions — a multi-member group never converged. Hold the
        // leader until the membership is quiescent or the rebalance window
        // closes.
        let members_now = after.members.keys().cloned().collect::<BTreeSet<_>>();
        if self.window_members.as_ref() != Some(&members_now) {
            self.window_members = Some(members_now);
            self.window_changed_at = now;
        }

        let membership_quiescent = now
            .duration_since(self.window_changed_at)
            .unwrap_or_default()
            >= JOIN_QUIESCENCE;

        // The rebalance window: `rebalance_timeout_ms` bounds how long the leader
        // may be held (the Java client allows JoinGroup responses up to
        // `rebalance_timeout + 5s`), falling back to the session timeout when
        // unset, as Kafka does for old protocol versions.
        let join_window_ms = u128::try_from(
            after
                .rebalance_timeout_ms
                .or(self.rebalance_timeout_ms)
                .unwrap_or(after.session_timeout_ms),
        )
        .unwrap_or_default();

        join_long_poll(
            updated,
            body,
            self.group_instance_id,
            self.polling_since.elapsed().as_millis(),
            updated.is_forming(),
            membership_quiescent,
            join_window_ms,
        )
    }
}

/// `sync`'s half of [`Controller::run_group_cas_loop`].
struct SyncCas<'a> {
    generation_id: i32,
    member_id: &'a str,
    group_instance_id: Option<&'a str>,
    protocol_type: Option<&'a str>,
    protocol_name: Option<&'a str>,
    assignments: Option<&'a [SyncGroupRequestAssignment]>,

    /// When this call started long-polling, on the monotonic clock (#256).
    polling_since: Instant,
}

impl<O> GroupCas<O> for SyncCas<'_>
where
    O: Storage + Clone,
{
    const LABELS: CasLabels = CasLabels {
        iteration: "sync_loop",
        noop_skip: "sync_noop_skip",
        outdated: "sync_outdated",
    };

    const BACKOFF_ON_CONFLICT: bool = true;

    async fn apply(
        &mut self,
        group_id: &str,
        now: SystemTime,
        mut wrapper: Wrapper<O>,
    ) -> (Wrapper<O>, Body) {
        let members_before = wrapper.members();

        if self.group_instance_id.is_none() {
            wrapper = wrapper.missed_heartbeat(group_id, self.member_id, now);
        }

        let (updated, body) = wrapper
            .sync(
                now,
                group_id,
                self.generation_id,
                self.member_id,
                self.group_instance_id,
                self.protocol_type,
                self.protocol_name,
                self.assignments,
            )
            .await;

        debug!(is_stable = members_before == updated.members());

        (updated, body)
    }

    /// Sampled from the state this iteration read, for the same reason as
    /// [`JoinCas::observe`] — see there.
    fn observe(
        &self,
        controller: &Controller<O>,
        group_id: &str,
        before: &GroupDetail,
        _after: &GroupDetail,
        now: SystemTime,
    ) {
        _ = controller.observe_rebalance(group_id, before, now);
    }

    /// See [`JoinCas::skip`] for the rationale. The leader's real
    /// `Forming`->`Formed` assignment sync changes `state` + `assignments`, so
    /// `same_rebalance_state` is false for it and it always takes the write path.
    fn skip(&self, before: &GroupDetail, after: &GroupDetail, now: SystemTime) -> bool {
        same_rebalance_state(before, after)
            && !liveness_renewal_due(before, self.member_id, now, before.session_timeout_ms)
    }

    fn settled(
        &mut self,
        updated: &Wrapper<O>,
        body: &Body,
        _after: &GroupDetail,
        _now: SystemTime,
    ) -> LongPoll {
        sync_long_poll(
            updated,
            body,
            self.group_instance_id,
            self.polling_since.elapsed().as_millis(),
        )
    }
}

/// `leave`'s half of [`Controller::run_group_cas_loop`].
struct LeaveCas<'a> {
    member_id: Option<&'a str>,
    members: Option<&'a [MemberIdentity]>,
}

impl<O> GroupCas<O> for LeaveCas<'_>
where
    O: Storage + Clone,
{
    const LABELS: CasLabels = CasLabels {
        iteration: "leave_loop",
        // Never emitted: the skip is gated on the GET-first read, which this API
        // does not do. Named anyway so turning `GET_FIRST` on is a one-line
        // change rather than one that also has to invent a label.
        noop_skip: "leave_noop_skip",
        outdated: "leave_outdated",
    };

    // A leave is terminal: there is nothing to long-poll and no skip to arm, so
    // the read would buy nothing the CAS does not already give.
    const GET_FIRST: bool = false;

    async fn apply(
        &mut self,
        group_id: &str,
        now: SystemTime,
        wrapper: Wrapper<O>,
    ) -> (Wrapper<O>, Body) {
        // Unconditional, unlike join/sync/heartbeat: a static member's leave is
        // the one case where its liveness genuinely has to be reconciled.
        wrapper
            .missed_heartbeat(group_id, self.member_id.unwrap_or_default(), now)
            .leave(now, group_id, self.member_id, self.members)
            .await
    }
}

/// `offset_commit`'s half of [`Controller::run_group_cas_loop`].
struct OffsetCommitCas<'a> {
    offset_commit: OffsetCommit<'a>,
}

impl<O> GroupCas<O> for OffsetCommitCas<'_>
where
    O: Storage + Clone,
{
    const LABELS: CasLabels = CasLabels {
        iteration: "offset_commit_loop",
        noop_skip: "offset_commit_noop_skip",
        outdated: "offset_commit_outdated",
    };

    /// No `missed_heartbeat`: a commit is not a liveness signal, and the
    /// committed offsets go to their own per-topition objects inside
    /// `Wrapper::offset_commit` regardless of what happens to the group object.
    async fn apply(
        &mut self,
        _group_id: &str,
        now: SystemTime,
        wrapper: Wrapper<O>,
    ) -> (Wrapper<O>, Body) {
        wrapper.offset_commit(now, &self.offset_commit).await
    }

    /// Skip the redundant group-state PUT when the commit changed nothing in
    /// `GroupDetail` (#111): the offsets are already persisted to their own
    /// objects, so re-writing the group object is pure overhead.
    ///
    /// Strict equality is right here, and is *not* the #273 mistake: nothing on
    /// this path assigns `last_contact`, so equality is reachable.
    fn skip(&self, before: &GroupDetail, after: &GroupDetail, _now: SystemTime) -> bool {
        before == after
    }
}

/// `heartbeat`'s half of [`Controller::run_group_cas_loop`].
struct HeartbeatCas<'a> {
    generation_id: i32,
    member_id: &'a str,
    group_instance_id: Option<&'a str>,
}

impl<O> GroupCas<O> for HeartbeatCas<'_>
where
    O: Storage + Clone,
{
    const LABELS: CasLabels = CasLabels {
        iteration: "heartbeat_loop",
        noop_skip: "heartbeat_noop_skip",
        outdated: "heartbeat_outdated",
    };

    async fn apply(
        &mut self,
        group_id: &str,
        now: SystemTime,
        mut wrapper: Wrapper<O>,
    ) -> (Wrapper<O>, Body) {
        if self.group_instance_id.is_none() {
            wrapper = wrapper.missed_heartbeat(group_id, self.member_id, now);
        }

        wrapper
            .heartbeat(
                now,
                group_id,
                self.generation_id,
                self.member_id,
                self.group_instance_id,
            )
            .await
    }

    /// Every member heartbeats every few seconds, so this is the densest
    /// sampling of a group's state available — which is what makes it the right
    /// place to notice one that has stopped making progress (#240). Sampled from
    /// `after`, unlike join and sync: the heartbeat itself can be what moves the
    /// group, so the state worth reporting is the one it produced.
    fn observe(
        &self,
        controller: &Controller<O>,
        group_id: &str,
        _before: &GroupDetail,
        after: &GroupDetail,
        now: SystemTime,
    ) {
        _ = controller.observe_rebalance(group_id, after, now);
    }

    /// Skip the group-state PUT when nothing persistent changed (#111): the
    /// object exists and already holds `before` (just read), so a steady-state
    /// heartbeat — and a heartbeat that merely observed a rebalance — writes zero
    /// tier-1 PUTs. Only a real membership / generation change falls through to
    /// the CAS.
    ///
    /// This used to be strict equality on `GroupDetail`, which could never hold
    /// (#273): `GroupMember` derives `PartialEq` over `last_contact`, and the
    /// heartbeat path assigns it `now` before we get here — via
    /// `missed_heartbeat` and `Formed::heartbeat`. So `after != before` on every
    /// *successful* heartbeat and the skip fired only on errors, doing precisely
    /// what the comment said it did not: 1 GET + 1 CAS PUT per member per
    /// interval, ~100 PUT/s at 300 members. That the drifted copy is now the
    /// shared one is the point of #286.
    ///
    /// The second half is not optional. Constant heartbeat PUTs were what kept
    /// `{group}.json` mtimes fresh, and group expiry used to condemn a group on
    /// that mtime alone — so skipping without a liveness floor would have armed
    /// offset deletion for every stable group past the retention window. #272
    /// removed that coupling by making expiry consult committed-offset activity,
    /// which is why this is safe now and was not before. The renewal every
    /// `session_timeout/2` still preserves cross-replica member eviction.
    fn skip(&self, before: &GroupDetail, after: &GroupDetail, now: SystemTime) -> bool {
        same_rebalance_state(before, after)
            && !liveness_renewal_due(before, self.member_id, now, before.session_timeout_ms)
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
            } else if group_instance_id.is_some()
                || same_subscription_topics(&member.join_response.metadata, &protocol.metadata)
            {
                // The encoded metadata changed but the subscribed topic set did
                // not: a static member's soft update, or a cooperative
                // consumer's KIP-792-only rejoin (fresh generationId /
                // ownedPartitions / sticky userData, same topics). Record the
                // new metadata as the leader's sticky input WITHOUT bumping the
                // generation — bumping here would invalidate any in-flight
                // SyncGroup and, with many members re-joining, keep the group
                // from ever converging.
                debug!(
                    member_metadata = "soft_update",
                    member_id,
                    ?group_instance_id,
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

        // Heal an orphaned leader before considering a promotion: group state
        // written before the `Forming::leave` fix-up (or by any other path
        // that forgot the invariant) can carry a leader that is no longer a
        // member. Left in place it deadlocks the group — every live member is
        // told someone else is the leader, so nobody ever sends assignments —
        // and only an owner change would clear it, via the fix-up in
        // `with_storage_group_detail` (#240).
        if self
            .state
            .leader
            .as_ref()
            .is_some_and(|leader| !self.members.contains_key(leader))
        {
            warn!(
                group_id,
                orphaned_leader = self.state.leader.as_deref(),
                generation_id = self.generation_id,
                "leader is no longer a member; clearing so a live member is promoted (#240)"
            );

            _ = self.state.leader.take();
        }

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

        // The departing member may be the elected leader. `Formed::leave`
        // already re-checks this (leader no longer a member -> `None`);
        // without the same fix-up here, a leader that leaves while the group
        // is still forming stays behind as a phantom: `join` never promotes
        // anyone else (`state.leader.is_none()` never holds), every follower
        // sync is refused as not-leader, and the group freezes silently with
        // no writes and nothing to evict (#240).
        if self
            .state
            .leader
            .as_ref()
            .is_some_and(|leader| !self.members.contains_key(leader))
        {
            info!(
                group_id,
                departed_leader = self.state.leader.as_deref(),
                generation_id = self.generation_id,
                "leader left while forming; clearing so a live member is promoted (#240)"
            );

            _ = self.state.leader.take();
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

    /// #240: a group whose members never finish joining must still be reported.
    ///
    /// The detector was hooked only to the heartbeat path, on the reasoning that
    /// members heartbeat every few seconds so it is the densest sampling
    /// available. That is true of a group whose members are *in* the group, and
    /// false of the failure this exists for. `AbstractCoordinator$HeartbeatThread`
    /// disables itself while the consumer has not joined:
    ///
    /// ```java
    /// if (state.hasNotJoinedGroup() || isFailed()) { disable(); continue; }
    /// ```
    ///
    /// So a consumer stuck trying to join sends no heartbeat at all. Measured in
    /// production: a group 90+ minutes in `CompletingRebalance` with 16 members,
    /// every heartbeat thread parked in `wait()`, and not one line about it
    /// across ten replicas over two hours — the detector silent on precisely the
    /// case it was written for.
    ///
    /// `join` keeps arriving from that consumer (each `poll()` retries), so it is
    /// a live sampling source when heartbeat is not. This drives joins only, and
    /// never a heartbeat.
    #[tokio::test]
    async fn a_group_that_never_joins_is_still_observed() -> Result<()> {
        let _guard = init_tracing()?;

        let storage = StorageContainer::builder()
            .cluster_id("test")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let s = Controller::with_storage(storage.clone())?;

        const GROUP_ID: &str = "never-joins";
        const CONSUMER: &str = "consumer";
        const RANGE: &str = "range";

        // A group already mid-rebalance: members joined, leader elected, no
        // assignment distributed. This is what a stuck consumer keeps re-joining
        // into, and what `CompletingRebalance` maps from.
        let mut members = BTreeMap::new();
        _ = members.insert(
            String::from("member-1"),
            GroupMember {
                join_response: JoinGroupResponseMember::default().member_id("member-1".into()),
                last_contact: None,
            },
        );

        _ = storage
            .update_group(
                GROUP_ID,
                GroupDetail {
                    rebalance_timeout_ms: Some(300_000),
                    members,
                    generation_id: 1,
                    state: GroupState::Forming {
                        protocol_type: Some(String::from(CONSUMER)),
                        protocol_name: Some(String::from(RANGE)),
                        leader: Some(String::from("member-1")),
                    },
                    ..Default::default()
                },
                None,
            )
            .await;

        let protocols = [JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(encode_subscription(&["test"], None))];

        _ = s
            .join(
                Some("a-consumer"),
                GROUP_ID,
                45_000,
                Some(300_000),
                "",
                None,
                CONSUMER,
                Some(&protocols[..]),
                None,
            )
            .await?;

        // The join alone must have sampled the group. Reading the memo directly
        // rather than the log: what is under test is that the forming path
        // observes at all, which is the hook a refactor could silently drop —
        // and did, for the whole of beta.31 and beta.32.
        let sampled = s
            .rebalance_stalls
            .lock()
            .map(|stalls| stalls.contains_key(GROUP_ID))
            .unwrap_or_default();

        assert!(
            sampled,
            "a group mid-rebalance was not observed on the join path, so a consumer \
             that never joins — and therefore never heartbeats — is invisible",
        );

        Ok(())
    }

    /// #240: the stall threshold comes from the group, not from a constant.
    ///
    /// The first version of this detector used a flat 60s, on the assumption
    /// that a healthy rebalance takes seconds. Production contradicted that in
    /// under an hour: 9-14 member groups routinely completed in **just over a
    /// minute** (60.1s, 60.4s, 62.9s, 67.2s, 81.2s observed, each recovering),
    /// so the warning fired ~2 per minute on healthy groups — which is how a
    /// signal becomes one operators scroll past, the failure this was meant to
    /// prevent.
    ///
    /// A group's own `rebalance_timeout_ms` is the honest bound: it is what its
    /// members told the coordinator to wait, so exceeding it is anomalous by the
    /// group's own definition, and it scales with the group where a constant
    /// cannot.
    #[test]
    fn the_stall_threshold_follows_the_group() {
        let declared = |ms: Option<i32>| {
            stall_after(&GroupDetail {
                rebalance_timeout_ms: ms,
                ..Default::default()
            })
        };

        assert_eq!(
            Duration::from_secs(300),
            declared(Some(300_000)),
            "a declared timeout is the threshold",
        );

        assert_eq!(
            REBALANCE_STALL_FLOOR,
            declared(None),
            "a group that declares none gets the floor",
        );

        // Below the floor the floor wins, in both the implausible and the merely
        // tight case: the observed healthy tail is ~81s, so a threshold under
        // that reports groups doing nothing wrong.
        assert_eq!(REBALANCE_STALL_FLOOR, declared(Some(1)));
        assert_eq!(REBALANCE_STALL_FLOOR, declared(Some(60_000)));
        assert_eq!(REBALANCE_STALL_FLOOR, declared(Some(0)));
        assert_eq!(REBALANCE_STALL_FLOOR, declared(Some(-1)));

        assert!(
            REBALANCE_STALL_FLOOR > Duration::from_millis(81_203),
            "the floor must clear the slowest healthy rebalance measured in production",
        );
    }

    /// #240: a group that stops making progress mid-rebalance must say so.
    ///
    /// The incident this exists for ran for hours with no signal at all — the
    /// broker answered every request about the group normally while its members
    /// waited for an assignment that never arrived, and tens of millions of
    /// records sat behind eight such groups. A group in `CompletingRebalance`
    /// past a bounded time is always a bug; the only question was how to notice.
    ///
    /// Pins the three properties that make it useful rather than noisy: it does
    /// not fire early, it fires once per episode (members heartbeat every few
    /// seconds, so per-call reporting would bury itself), and a group that
    /// recovers and stalls again is reported again.
    #[tokio::test]
    async fn a_stalled_rebalance_is_reported_once_per_episode() -> Result<()> {
        let _guard = init_tracing()?;

        let storage = StorageContainer::builder()
            .cluster_id("test")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let s = Controller::with_storage(storage)?;

        const GROUP_ID: &str = "stalled-group";

        // Members joined, a leader elected, no assignment distributed: exactly
        // what `ConsumerGroupState` maps to `CompletingRebalance`.
        let mut members = BTreeMap::new();
        _ = members.insert(
            String::from("member-1"),
            GroupMember {
                join_response: JoinGroupResponseMember::default().member_id("member-1".into()),
                last_contact: None,
            },
        );

        // Declares its own rebalance timeout, so the threshold is derived from
        // the group rather than from a constant.
        const REBALANCE_TIMEOUT_MS: i32 = 300_000;
        let threshold = Duration::from_millis(REBALANCE_TIMEOUT_MS as u64);

        let stalling = GroupDetail {
            rebalance_timeout_ms: Some(REBALANCE_TIMEOUT_MS),
            members: members.clone(),
            state: GroupState::Forming {
                protocol_type: Some(String::from("consumer")),
                protocol_name: Some(String::from("range")),
                leader: Some(String::from("member-1")),
            },
            ..Default::default()
        };

        // The same group once the assignment lands.
        let progressed = GroupDetail {
            rebalance_timeout_ms: Some(REBALANCE_TIMEOUT_MS),
            members,
            state: GroupState::Formed {
                protocol_type: String::from("consumer"),
                protocol_name: String::from("range"),
                leader: String::from("member-1"),
                assignments: BTreeMap::new(),
            },
            ..Default::default()
        };

        let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

        assert!(
            !s.observe_rebalance(GROUP_ID, &stalling, t0),
            "not on first sight"
        );
        assert!(
            !s.observe_rebalance(GROUP_ID, &stalling, t0 + threshold - Duration::from_secs(1)),
            "not before the threshold",
        );

        assert!(
            s.observe_rebalance(GROUP_ID, &stalling, t0 + threshold),
            "reported once the threshold is reached",
        );
        assert!(
            !s.observe_rebalance(GROUP_ID, &stalling, t0 + threshold * 10),
            "and not again for the same episode",
        );

        // Recovery clears the episode ...
        assert!(!s.observe_rebalance(GROUP_ID, &progressed, t0 + threshold * 11));

        // ... so a fresh stall is a fresh episode, reported on its own clock.
        let t1 = t0 + threshold * 12;
        assert!(
            !s.observe_rebalance(GROUP_ID, &stalling, t1),
            "new episode starts quiet"
        );
        assert!(
            s.observe_rebalance(GROUP_ID, &stalling, t1 + threshold),
            "and is reported in its own right",
        );

        Ok(())
    }

    fn encode_subscription(topics: &[&str], generation_id: Option<i32>) -> Bytes {
        Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .topics(topics.iter().map(|topic| topic.to_string()))
                    .generation_id(generation_id),
            ),
        )
        .expect("encode member metadata")
    }

    // A cooperative consumer (KIP-792) re-encodes its subscription on every
    // rejoin with a fresh generationId, so the raw bytes differ while the topic
    // set is unchanged. `same_subscription_topics` must see through that churn
    // (else the group generation bumps on every rejoin and never converges),
    // yet still flag a genuine topic change and stay conservative on garbage.
    #[test]
    fn same_subscription_topics_ignores_kip792_churn() {
        let a = encode_subscription(&["t.a", "t.b"], Some(41));
        let b = encode_subscription(&["t.a", "t.b"], Some(42));
        assert_ne!(a, b, "raw metadata must differ (KIP-792 generationId)");
        assert!(same_subscription_topics(&a, &b), "same topics -> no-op");

        let c = encode_subscription(&["t.b", "t.a"], None);
        assert!(
            same_subscription_topics(&a, &c),
            "topic order must not matter"
        );

        let d = encode_subscription(&["t.a", "t.b", "t.c"], Some(42));
        assert!(
            !same_subscription_topics(&a, &d),
            "a real topic change must NOT be treated as a no-op"
        );

        assert!(
            !same_subscription_topics(&a, &Bytes::from_static(b"garbage")),
            "undecodable metadata must be conservatively treated as changed"
        );
    }

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

    /// #240: the leader leaving a still-Forming group must not stay behind as
    /// a phantom leader.
    ///
    /// `Formed::leave` re-checks the leader against the remaining members;
    /// `Forming::leave` did not. A leader that left mid-rebalance (a
    /// connector pod's graceful shutdown sends LeaveGroup) therefore kept
    /// `state.leader = Some(departed)`: `join` never promoted anyone else,
    /// every follower sync was refused as not-leader, no write ever happened,
    /// and the group froze silently — until the owning broker restarted and
    /// `with_storage_group_detail`'s fix-up cleared the orphan. Measured in
    /// production as a 16-member group held in `CompletingRebalance` for
    /// hours with zero broker log lines.
    #[tokio::test]
    async fn leave_of_leader_while_forming_clears_leader() -> Result<()> {
        let _guard = init_tracing()?;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "leader-leaves-while-forming";
        const RANGE: &str = "range";
        const PROTOCOL_TYPE: &str = "consumer";

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        let storage = StorageContainer::builder()
            .cluster_id("test")
            .node_id(111)
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

        let leader_id = format!("{CLIENT_ID}-{}", Uuid::new_v4());
        let follower_id = format!("{CLIENT_ID}-{}", Uuid::new_v4());

        let protocols = [JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"leader_meta"))];

        let (s, _) = s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                leader_id.as_str(),
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&protocols[..]),
                reason,
            )
            .await;

        let follower_protocols = [JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"follower_meta"))];

        let (s, _) = s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                follower_id.as_str(),
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&follower_protocols[..]),
                reason,
            )
            .await;

        assert_eq!(Some(leader_id.as_str()), s.leader());
        assert_eq!(2, s.members().len());

        // The leader leaves while the group is still forming.
        let (s, _) = s.leave(now, GROUP_ID, Some(leader_id.as_str()), None).await;

        assert_eq!(1, s.members().len());
        assert_eq!(
            None,
            s.leader(),
            "a departed leader must not remain elected"
        );

        // The next join from the surviving member must promote it...
        let (s, body) = s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                follower_id.as_str(),
                group_instance_id,
                PROTOCOL_TYPE,
                Some(&follower_protocols[..]),
                reason,
            )
            .await;

        assert_eq!(Some(follower_id.as_str()), s.leader());

        let Body::JoinGroupResponse(join_response) = body else {
            panic!("expected a join response");
        };
        assert_eq!(i16::from(ErrorCode::None), join_response.error_code);
        assert_eq!(follower_id, join_response.leader);

        // ...and its assignment sync must form the group.
        let generation_id = s.generation_id();
        let assignments = [SyncGroupRequestAssignment::default()
            .member_id(follower_id.clone())
            .assignment(Bytes::from_static(b"assignment"))];

        let (s, body) = s
            .sync(
                now,
                GROUP_ID,
                generation_id,
                follower_id.as_str(),
                group_instance_id,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments[..]),
            )
            .await;

        let Body::SyncGroupResponse(sync_response) = body else {
            panic!("expected a sync response");
        };
        assert_eq!(i16::from(ErrorCode::None), sync_response.error_code);
        assert!(!s.is_forming());

        Ok(())
    }

    /// #240: a persisted group can already carry an orphaned leader (written
    /// before the `Forming::leave` fix-up existed, or by a replica still
    /// running without it). A join must heal it — clear the orphan and
    /// promote a live member — rather than keep answering "the leader is
    /// someone else" to every live member forever.
    #[tokio::test]
    async fn join_heals_an_orphaned_leader() -> Result<()> {
        let _guard = init_tracing()?;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "orphaned-leader-group";
        const RANGE: &str = "range";
        const PROTOCOL_TYPE: &str = "consumer";

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);

        let storage = StorageContainer::builder()
            .cluster_id("test")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await?;

        let now = SystemTime::now();
        let member_id = format!("{CLIENT_ID}-{}", Uuid::new_v4());

        // Built directly rather than through `with_storage_group_detail`,
        // whose own fix-up would already clear the orphan: this is the shape
        // an owner replica holds in memory after the departed leader was
        // removed from `members` with `state.leader` left in place.
        let s = Wrapper::Forming(Inner {
            session_timeout_ms,
            rebalance_timeout_ms,
            members: BTreeMap::from([(
                member_id.clone(),
                Member {
                    join_response: JoinGroupResponseMember::default()
                        .member_id(member_id.clone())
                        .metadata(Bytes::from_static(b"member_meta")),
                    last_contact: Some(now),
                },
            )]),
            generation_id: 7,
            state: Forming {
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(RANGE.into()),
                leader: Some(format!("{CLIENT_ID}-{}", Uuid::new_v4())),
            },
            storage,
            skip_assignment: Some(false),
            inception: now,
        });

        let protocols = [JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"member_meta"))];

        let (s, body) = s
            .join(
                now,
                Some(CLIENT_ID),
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id.as_str(),
                None,
                PROTOCOL_TYPE,
                Some(&protocols[..]),
                None,
            )
            .await;

        assert_eq!(
            Some(member_id.as_str()),
            s.leader(),
            "the live member must be promoted over the orphan"
        );

        let Body::JoinGroupResponse(join_response) = body else {
            panic!("expected a join response");
        };
        assert_eq!(i16::from(ErrorCode::None), join_response.error_code);
        assert_eq!(member_id, join_response.leader);

        Ok(())
    }

    async fn memory_storage() -> Result<Arc<Box<dyn Storage>>> {
        StorageContainer::builder()
            .cluster_id("test")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await
            .map_err(Into::into)
    }

    /// Every per-group map entry this controller holds, as
    /// `(wrappers, group_locks, rebalance_stalls, last_seen)`.
    fn cached<O>(controller: &Controller<O>) -> (usize, usize, usize, usize) {
        (
            controller.wrappers.lock().unwrap().len(),
            controller.group_locks.lock().unwrap().len(),
            controller.rebalance_stalls.lock().unwrap().len(),
            controller.last_seen.lock().unwrap().len(),
        )
    }

    /// #283: none of the four per-group maps had a removal path — not even when
    /// the group was deleted from the object store — so a group-per-restart or
    /// group-per-subscription naming pattern grew them for the life of the pod.
    /// `wrappers` is the expensive one: it holds each group's full
    /// member-metadata byte blobs.
    ///
    /// Written against the *level*: the failure is monotonic growth, so what has
    /// to hold is that churning many uniquely-named groups returns to zero rather
    /// than climbing. A zero idle window makes every group a candidate on the
    /// sweep after it, which is what keeps this deterministic instead of timed.
    #[tokio::test]
    async fn group_churn_with_fresh_names_reaches_a_flat_steady_state() -> Result<()> {
        let _guard = init_tracing()?;

        let storage = memory_storage().await?;
        let s = Controller::with_storage(storage)?.with_idle_after(Duration::ZERO);

        const ROUNDS: usize = 16;
        const RANGE: &str = "range";
        const PROTOCOL_TYPE: &str = "consumer";

        let protocols = [JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(encode_subscription(&["test"], None))];

        for round in 0..ROUNDS {
            // A fresh name every round: a same-named re-join would reuse the
            // entries rather than add to them, so it would not show the growth.
            let group_id = format!("churn-{round}");

            _ = s
                .join(
                    Some("a-consumer"),
                    &group_id,
                    45_000,
                    Some(300_000),
                    "",
                    None,
                    PROTOCOL_TYPE,
                    Some(&protocols[..]),
                    None,
                )
                .await?;

            // The group is known now — otherwise the sweep below would be
            // asserting that nothing evicts nothing.
            let (wrappers, locks, _, last_seen) = cached(&s);
            assert_eq!(1, wrappers, "wrappers, round {round}");
            assert_eq!(1, locks, "group_locks, round {round}");
            assert_eq!(1, last_seen, "last_seen, round {round}");

            assert_eq!(1, s.prune(), "round {round}");
            assert_eq!((0, 0, 0, 0), cached(&s), "not flat after round {round}");
        }

        Ok(())
    }

    /// #283: the sweep must not drop a lock a request is holding.
    ///
    /// Dropping it does not free anything — it means the *next* request for that
    /// group creates a second lock, and two members then run their read->CAS
    /// windows concurrently against one `{group}.json`, which is the CAS thrash
    /// [`GroupLocks`] exists to prevent (#240). The `Arc` count is the guard, and
    /// it is also what makes the sweep leak-free: a request that has checked its
    /// wrapper out of `wrappers` and not yet put it back is invisible to a sweep
    /// keyed on the map's contents, so its stamp must survive too or the
    /// re-inserted entry would never be a candidate again.
    #[tokio::test]
    async fn a_group_being_served_is_not_evicted() -> Result<()> {
        let _guard = init_tracing()?;

        let storage = memory_storage().await?;
        let s = Controller::with_storage(storage)?.with_idle_after(Duration::ZERO);

        const GROUP_ID: &str = "in-flight";

        // Stand in for a request in flight: `group_lock` is what every path calls
        // first, and holding its `Arc` is exactly the state a live request is in.
        let held = s.group_lock(GROUP_ID);

        assert_eq!(0, s.prune());

        let (_, locks, _, last_seen) = cached(&s);
        assert_eq!(1, locks, "the held lock must survive");
        assert_eq!(
            1, last_seen,
            "the stamp must survive with it, or the entry becomes unpruneable"
        );

        // Request over.
        drop(held);

        assert_eq!(1, s.prune());
        assert_eq!((0, 0, 0, 0), cached(&s));

        Ok(())
    }

    /// #283: a group served within the idle window keeps its state — the sweep
    /// bounds growth, it does not throw away the cache it exists to be.
    #[tokio::test]
    async fn a_recently_served_group_is_kept() -> Result<()> {
        let _guard = init_tracing()?;

        let storage = memory_storage().await?;
        let s = Controller::with_storage(storage)?.with_idle_after(Duration::from_secs(3_600));

        _ = s.group_lock("recent");

        assert_eq!(0, s.prune());
        assert_eq!(1, cached(&s).1, "group_locks");

        Ok(())
    }

    /// #283: an evicted group is a cache miss and nothing more.
    ///
    /// Every request path reads `{group}.json` GET-first and adopts the persisted
    /// state whenever its cached version differs — and a freshly created wrapper
    /// has no version, so it always adopts. `inception` is persisted in
    /// `GroupDetail`, so it survives too. This drives a group to a generation,
    /// evicts it, and re-joins: the coordinator must answer from the object store
    /// as if it had never forgotten.
    #[tokio::test]
    async fn an_evicted_group_is_rebuilt_from_the_object_store() -> Result<()> {
        let _guard = init_tracing()?;

        let storage = memory_storage().await?;
        let s = Controller::with_storage(storage.clone())?.with_idle_after(Duration::ZERO);

        const GROUP_ID: &str = "rebuilt";
        const RANGE: &str = "range";
        const PROTOCOL_TYPE: &str = "consumer";

        let protocols = [JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(encode_subscription(&["test"], None))];

        let join = async || {
            s.join(
                Some("a-consumer"),
                GROUP_ID,
                45_000,
                Some(300_000),
                "",
                None,
                PROTOCOL_TYPE,
                Some(&protocols[..]),
                None,
            )
            .await
        };

        let Body::JoinGroupResponse(before) = join().await? else {
            panic!("expected a join response");
        };

        let persisted = storage
            .read_group(GROUP_ID)
            .await?
            .map(|(detail, _)| detail)
            .expect("the group must be persisted");

        assert_eq!(1, s.prune());
        assert_eq!((0, 0, 0, 0), cached(&s));

        let Body::JoinGroupResponse(after) = join().await? else {
            panic!("expected a join response");
        };

        // Same generation and same leader as before the eviction: the state came
        // back from the object, it was not re-formed from nothing.
        assert_eq!(before.generation_id, after.generation_id);
        assert_eq!(before.leader, after.leader);

        // `inception` is the one field a fresh `Inner` would invent rather than
        // recompute, so it is the one worth pinning.
        assert_eq!(
            persisted.inception,
            storage
                .read_group(GROUP_ID)
                .await?
                .map(|(detail, _)| detail.inception)
                .expect("the group must still be persisted"),
        );

        Ok(())
    }
}
