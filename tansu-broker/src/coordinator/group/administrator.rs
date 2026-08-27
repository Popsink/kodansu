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

//! The consumer group coordinator, over the decomposed group objects (#359).
//!
//! Every group-mutating API used to read-modify-write one object —
//! `groups/consumers/{group}.json` — behind one etag. That object carried
//! per-member liveness (roughly one write-worthy event per second per member),
//! each member's subscription, the group's generation and leader, and the
//! leader's assignment map, so liveness churn structurally starved the one
//! write that had to land. Group forwarding and the per-group in-process lock
//! exist to compensate for that, which is why replicas need pod identity.
//!
//! The state now lives in three objects with three write regimes — see
//! [`tansu_storage::MemberDoc`], [`tansu_storage::GenerationDoc`] and
//! [`tansu_storage::AssignmentDoc`] — and this module is what drives them:
//!
//! - a request holds **no** cached group state. It composes a [`GroupView`]
//!   from the store, answers from it, and forgets it: two fresh
//!   [`Controller`]s answer the same request identically, with no warm-up.
//! - the only object several members contend on is `generation.json`, and it
//!   only changes when the group's *composition* does. A member's liveness
//!   goes to its own document, at most once per session/2.
//! - the leader's assignment is written **create-only**, so the write that used
//!   to be starved has no etag to lose a race on: exactly one writer wins the
//!   key, and `Stable` is derived from that object's existence rather than
//!   stored.
//! - a lapsed member is reclaimed by a [sweep](Controller::sweep) whose verdict
//!   is a pure function of the member documents and the clock, so N replicas
//!   running it concurrently reach identical conclusions and cost one sweep per
//!   group per session/2 *globally*.
//!
//! A [`Controller`] holds **no per-group state at all** (#360). There is no
//! cache to warm, no lock keyed by a client-chosen name, and nothing to sweep,
//! so every replica is interchangeable: one can be added or removed under load
//! and the groups it was serving do not notice. That is the whole of what group
//! forwarding — a deterministic owner replica, discovered over headless-Service
//! DNS, reachable on a second listener — existed to buy, and it is why that
//! machinery could be deleted rather than reconfigured.
//!
//! Interchangeable also means *removable*, which is what the long polls have to
//! answer for: `join` and `sync` hold a request open for tens of seconds, and a
//! replica that goes away mid-poll used to take the connection with it. They
//! watch a [`CancellationToken`] now and answer early when the process is asked
//! to stop (#361) — an answer being the whole difference between a scale-in
//! event a client absorbs and one it reports as a broker that dropped it.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::{self, Debug},
    hash::Hash,
    ops::{Deref, Div as _, Mul},
    sync::{Arc, LazyLock, Mutex},
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use cached::stores::ExpiringSizedCache;
use futures::StreamExt as _;
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
    to_timestamp,
};
use tansu_service::Parked;
use tansu_storage::{
    AssignmentDoc, AssignmentOutcome, ConsumerGroupState, GenerationDoc, MemberDoc, MemberRef,
    OffsetCommitRequest, Storage, Topition, UpdateError, Version,
};
use tokio::time::{Duration, Instant, sleep};
use tokio_util::sync::CancellationToken;
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
/// assignments for the full membership. This broker has no timer state, so the
/// window is inferred instead — from `GenerationDoc::members_changed_at_ms`,
/// which is persisted precisely so that **every replica reaches the same
/// verdict**: the barrier used to be inferred from what one call had happened
/// to observe, which is not a property of the group. Must exceed the CAS
/// conflict backoff cap (500ms + 50% jitter, see `cas_conflict_backoff`) plus
/// the 1s long-poll cadence, so a member mid-retry on another replica is not
/// missed.
const JOIN_QUIESCENCE: Duration = Duration::from_secs(3);

/// After this many consecutive CAS conflicts on a single group's generation,
/// log a warning. Sustained conflicts now mean something is genuinely wrong:
/// the generation only changes when the group's composition does, so a healthy
/// group in steady state produces none at all.
const CAS_CONFLICT_WARN: u32 = 16;

/// How many times a non-polling API re-reads and re-applies after losing the
/// generation CAS before it gives up.
///
/// `join` and `sync` need no such bound — they long-poll, so a caller's own
/// deadline ends the call — but `leave` answers in one pass, and an unbounded
/// retry against a group being hammered by another replica would hold the
/// request open indefinitely rather than let the client retry.
const MAX_GENERATION_CAS_ATTEMPTS: u32 = 64;

/// The session timeout to assume when a group or a member declares none.
/// Kafka's own default, and the value this coordinator has always fallen back
/// to when reasoning about liveness.
const DEFAULT_SESSION_TIMEOUT_MS: i32 = 45_000;

/// Concurrency of the per-member document fan-outs (the leader's join response
/// and the sweep), matching the fan-outs in the storage engine.
const MEMBER_FETCH_CONCURRENCY: usize = 32;

/// Backoff before retrying a group write that lost the object-store CAS race
/// (another replica updated the same object first). Without it the retry loop
/// spins as fast as the store answers, hammering the shared object and keeping
/// racing replicas in lock-step. Exponential (5ms·2^n) capped at 500ms, plus up
/// to 50% jitter to desynchronise replicas. See #44.
fn cas_conflict_backoff(attempt: u32) -> Duration {
    let base_ms = 5u64.saturating_mul(1u64 << attempt.min(7)).min(500);
    let jitter = rng().random_range(0..=base_ms / 2);
    Duration::from_millis(base_ms + jitter)
}

/// `now` as epoch milliseconds.
///
/// Every timestamp this coordinator persists is in this form, because they are
/// read and compared by replicas that share no monotonic clock: the join
/// window, a member's session and the sweep stamp are all differences between
/// two epoch readings, one of which was taken on another machine.
fn epoch_ms(now: SystemTime) -> i64 {
    to_timestamp(&now).unwrap_or_default()
}

/// A declared session timeout, or the default when it is absent or nonsensical.
fn session_timeout_or_default(session_timeout_ms: i32) -> i32 {
    if session_timeout_ms > 0 {
        session_timeout_ms
    } else {
        DEFAULT_SESSION_TIMEOUT_MS
    }
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

/// Whether a member's liveness is stale enough that this request must persist a
/// refreshed timestamp.
///
/// Bounded to half the session timeout — well inside the eviction deadline — so
/// it fires at most once per `session_timeout/2` per member, which is the whole
/// of what the old once-a-second `last_contact` churn was buying. A member with
/// no document at all must write one: the sweep reads a missing document as an
/// expired session.
fn liveness_renewal_due(member: Option<&MemberDoc>, now_ms: i64, session_timeout_ms: i32) -> bool {
    let Some(member) = member else {
        return true;
    };

    now_ms.saturating_sub(member.last_contact_ms)
        >= i64::from(session_timeout_or_default(session_timeout_ms)) / 2
}

/// What a `join` or `sync` iteration does once this member's group state is
/// settled: wait and re-poll, or answer now.
///
/// Both APIs reach this decision once per iteration. Holding it in one place
/// per API means a change to the hold conditions cannot land on one branch and
/// miss the other (#286), and leaves the genuine protocol difference between
/// `join` and `sync` stated where it can be read.
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
fn join_long_poll(
    updated: &GroupView,
    body: &Body,
    group_instance_id: Option<&str>,
    elapsed_ms: u128,
    is_forming: bool,
    membership_quiescent: bool,
    join_window_ms: u128,
) -> LongPoll {
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
fn sync_long_poll(
    updated: &GroupView,
    body: &Body,
    group_instance_id: Option<&str>,
    elapsed_ms: u128,
) -> LongPoll {
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

/// How often one stalled group may be logged (#240/#360).
///
/// The dedup used to be "once per episode, cleared when the group recovers",
/// held in a map a periodic sweep emptied. Nothing sweeps anything now, so the
/// memo expires instead — and an hour is the interval that makes that a better
/// signal rather than merely a cheaper one: the incident this detector exists
/// for ran for *hours*, and a line emitted once and never again is one an
/// operator who came on shift after it can never see.
const REBALANCE_STALL_REPORT_EVERY: Duration = Duration::from_hours(1);

/// How long a group may sit in `CompletingRebalance` before it is reported
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
fn stall_after(rebalance_timeout_ms: Option<i32>) -> Duration {
    rebalance_timeout_ms
        .filter(|ms| *ms > 0)
        .map_or(REBALANCE_STALL_FLOOR, |ms| {
            Duration::from_millis(ms as u64).max(REBALANCE_STALL_FLOOR)
        })
}

/// Everything a request needs to know about a group, composed from the store
/// each time rather than cached (#359).
///
/// This is what replaced the `Wrapper`/`Inner` typestate and the map of them
/// this controller used to hold. The typestate encoded `Forming`/`Formed` as a
/// Rust type, which meant the coordinator's idea of a group's state had to be
/// *maintained*: adopted on a GET-first read, re-adopted on a lost CAS, rebuilt
/// on a cache miss, and reconciled with whatever another replica had done in
/// between. Here `Formed` is derived — from the existence of an immutable
/// object — so there is nothing to maintain and nothing to go stale: a request
/// is a pure function of the store.
#[derive(Clone, Debug, Default)]
struct GroupView {
    /// The group's composition. `Default` when `generation.json` is absent,
    /// which — together with a `None` [`Self::version`] — is how a group that
    /// does not exist reads.
    generation: GenerationDoc,

    /// The generation object's version, and so the CAS token for the next write
    /// of it. `None` means the object is not there, so the next write must
    /// create it.
    version: Option<Version>,

    /// `assignment/{generation_id}`, when the leader of that generation has
    /// written it. Immutable, so its presence is final for that generation.
    assignment: Option<AssignmentDoc>,
}

impl GroupView {
    /// Whether the group has a `generation.json` at all.
    fn exists(&self) -> bool {
        self.version.is_some()
    }

    fn generation_id(&self) -> i32 {
        self.generation.generation_id
    }

    fn leader(&self) -> Option<&str> {
        self.generation.leader.as_deref()
    }

    fn session_timeout_ms(&self) -> i32 {
        session_timeout_or_default(self.generation.session_timeout_ms)
    }

    /// The assignment in force, which is the one written **for this
    /// generation**. An assignment left behind by an earlier generation is not
    /// this group's assignment, and reading it as one is how a rebalance in
    /// progress reports as `Stable`.
    fn assignments(&self) -> Option<&BTreeMap<String, Bytes>> {
        self.assignment
            .as_ref()
            .filter(|assignment| assignment.generation_id == self.generation.generation_id)
            .map(|assignment| &assignment.assignments)
    }

    /// The group's state, derived exactly as `DescribeGroups` derives it, so
    /// the coordinator and every admin tool agree.
    fn state(&self) -> ConsumerGroupState {
        self.generation.state(self.assignments().is_some())
    }

    /// Whether the group is anything other than `Stable` — the successor to
    /// `Wrapper::Forming`, and derived rather than held.
    fn is_forming(&self) -> bool {
        self.generation.leader.is_none() || self.assignments().is_none()
    }

    fn is_leader(&self, body: &Body) -> bool {
        if let Body::JoinGroupResponse(JoinGroupResponse { member_id, .. }) = body {
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
        matches!(
            body,
            Body::JoinGroupResponse(JoinGroupResponse { error_code, .. })
                if *error_code == i16::from(ErrorCode::MemberIdRequired)
        )
    }

    /// This group's composition with its ABA discriminator advanced and its
    /// generation moved on, ready for the CAS that changes `members`.
    ///
    /// Every composition change goes through here, so the invariants that hold
    /// the layout together are stated once: a generation is **never reused**
    /// (`assignment/{gen}` is create-only, so reusing one would adopt a dead
    /// generation's immutable assignment), the join window is based on when
    /// membership last moved, and a state episode starts when the group's
    /// composition does.
    fn rebalanced(mut generation: GenerationDoc, now_ms: i64) -> GenerationDoc {
        generation.generation_id = generation.generation_id.saturating_add(1);
        generation.members_changed_at_ms = now_ms;
        generation.state_since_ms = now_ms;
        generation
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

#[derive(Clone)]
pub struct Controller<O> {
    storage: O,

    /// Which groups this replica has already logged a stalled rebalance for
    /// (#240) — the one thing in this struct keyed by a group id, and the
    /// reason it is a self-expiring cache rather than a map.
    ///
    /// Only the *dedup* is in memory. When the episode started is
    /// `GenerationDoc::state_since_ms`, persisted with the group, so a restart
    /// no longer starts a blind window at exactly the moment a wedge is most
    /// likely to have been created: a fresh `Controller` reports a group that
    /// has been stuck for an hour immediately, where it used to wait out the
    /// threshold again before saying anything.
    ///
    /// It expires rather than being swept, which is what let the sweep go with
    /// forwarding (#360): a map keyed by a client-chosen name and emptied by a
    /// periodic pass is state a replica has to be *kept alive* to maintain.
    /// Expiring also makes a long incident louder in the right way — a group
    /// stuck for hours is reported once an hour rather than once ever, so an
    /// operator who missed the first line still sees one.
    rebalance_stalls: Arc<Mutex<ExpiringSizedCache<String, ()>>>,

    /// Cancelled when this process has been asked to stop (#361).
    ///
    /// Only the long polls read it, and only to stop *waiting*: a cancelled
    /// poll answers with what it has, exactly as it would at its own deadline,
    /// rather than being abandoned. Nothing here refuses work — a request that
    /// arrives during the drain is served normally, because the connection it
    /// arrived on was accepted before the drain began and the client is owed a
    /// response either way.
    ///
    /// Defaults to a token nothing cancels, so a `Controller` built without one
    /// long-polls to its deadline as before.
    cancellation: CancellationToken,

    /// Where a request's wall-clock reading comes from (#359).
    ///
    /// **Sampling only.** Every value this produces is compared against
    /// another reading of the same clock — the join window, a member's session
    /// — never against something already persisted by another replica, which
    /// is why a test may move it without inventing a cross-replica time
    /// disagreement. Timestamps written into group state still come from
    /// `SystemTime::now()` directly.
    ///
    /// It exists because `tokio`'s paused clock advances `Instant` and
    /// `sleep`, and cannot touch `SystemTime`. A coordinator test therefore had
    /// to buy its determinism by waiting out real session timeouts: seven of
    /// the eight `new_cg` cases ran on the real clock, and the binary's floor
    /// was 58 seconds. With the reading injectable, a test derives it from the
    /// paused clock and the whole binary runs in the time it takes to schedule.
    now: fn() -> SystemTime,
}

/// Hand-written because the stall memo is a cache without a `Debug` impl, and
/// because there is nothing here worth printing: a `Controller` is its storage
/// handle and a clock.
impl<O> Debug for Controller<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Controller").finish_non_exhaustive()
    }
}

impl<O> Controller<O>
where
    O: Storage + Clone,
{
    pub fn with_storage(storage: O) -> Result<Self> {
        Ok(Self {
            storage,
            rebalance_stalls: Arc::new(Mutex::new(ExpiringSizedCache::new(
                REBALANCE_STALL_REPORT_EVERY,
            ))),
            cancellation: CancellationToken::new(),
            now: SystemTime::now,
        })
    }

    /// Watch `cancellation` for this process being asked to stop, so an
    /// in-flight long poll answers instead of being cut (#361).
    pub fn with_cancellation(self, cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            ..self
        }
    }

    /// Sleep out a long poll's wait, unless this process is stopping.
    ///
    /// `false` means the caller must answer now. The two are raced rather than
    /// checked before the sleep because the wait is up to a second and the
    /// drain is what is waiting on it: checking first would still leave a
    /// request holding the shutdown open for the length of one poll interval,
    /// per poll, for every member of every group this replica is serving.
    async fn waited(&self, pause: Duration, method: &'static str) -> bool {
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", method)]);

        // Waiting on a rebalance window or on another member, not on this
        // replica: a fleet full of these is idle, however many requests it is
        // holding open (#362).
        let _parked = Parked::enter();

        tokio::select! {
            () = sleep(pause) => true,

            () = self.cancellation.cancelled() => {
                debug!(method, "answering a long poll early: this replica is stopping");
                COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "long_poll_drained")]);
                false
            }
        }
    }

    /// Override where a request's wall-clock reading comes from (#359). See
    /// [`Self::now`] for what may and may not be moved this way.
    pub fn with_now(self, now: fn() -> SystemTime) -> Self {
        Self { now, ..self }
    }

    /// Report a group that has been mid-rebalance too long (#240).
    ///
    /// A group in `CompletingRebalance` — members joined, leader elected, no
    /// assignment distributed — past [`stall_after`] is always a bug:
    /// consumption has stopped and nothing else says so.
    ///
    /// Reported at most once per [`REBALANCE_STALL_REPORT_EVERY`] rather than
    /// per request: members heartbeat every few seconds, so an unguarded
    /// `warn!` here would emit thousands of lines for one stuck group and bury
    /// the signal it exists to raise. The episode's *start* is read from the
    /// group (`state_since_ms`), so only the "already said this" memo is
    /// per-replica — and it expires rather than being swept, which is what let
    /// this controller stop holding per-group state at all (#360).
    fn observe_rebalance(&self, group_id: &str, view: &GroupView, now: SystemTime) -> bool {
        let stalled = matches!(view.state(), ConsumerGroupState::CompletingRebalance);

        let threshold = stall_after(view.generation.rebalance_timeout_ms);
        let stalled_for = Duration::from_millis(
            epoch_ms(now)
                .saturating_sub(view.generation.state_since_ms)
                .max(0) as u64,
        );

        let Ok(mut stalls) = self.rebalance_stalls.lock() else {
            return false;
        };

        // Decided before the key is built, so the healthy path — which is every
        // request of every group that is not wedged — pays a comparison rather
        // than an allocation. The cache is keyed by `String` and has no
        // borrowed lookup, so touching it at all costs one.
        if !stalled {
            // A group that recovers is reported afresh if it stalls again,
            // which is why this is cleared rather than left to expire.
            if !stalls.is_empty() {
                _ = stalls.remove(&group_id.to_owned());
            }

            return false;
        }

        if stalled_for >= threshold && stalls.get(&group_id.to_owned()).is_none() {
            _ = stalls.insert_evict(group_id.to_owned(), (), true);
            REBALANCE_STALLS.add(1, &[]);

            // `threshold_ms` is in the line on purpose: it is the group's own
            // declared rebalance timeout, so an operator can tell "this group is
            // stuck" from "this group declares an unusually tight timeout"
            // without going to read the config.
            warn!(
                group_id,
                members = view.generation.members.len(),
                generation_id = view.generation.generation_id,
                stalled_for_ms = stalled_for.as_millis() as u64,
                threshold_ms = threshold.as_millis() as u64,
                "group has not completed its rebalance: members joined, no assignment distributed (#240)"
            );

            return true;
        }

        false
    }

    /// Compose a group from its decomposed objects.
    ///
    /// Two reads at most, and the second only when there can be something to
    /// find: with no leader there is no assignment, so probing for one would be
    /// a 404 on every read of a forming group.
    async fn read_view(&self, group_id: &str) -> Result<GroupView> {
        let Some((generation, version)) = self.storage.read_group_generation(group_id).await?
        else {
            return Ok(GroupView::default());
        };

        let assignment = if generation.leader.is_some() && generation.generation_id >= 0 {
            self.storage
                .read_group_assignment(group_id, generation.generation_id)
                .await?
        } else {
            None
        };

        Ok(GroupView {
            generation,
            version: Some(version),
            assignment,
        })
    }

    /// Account for a lost generation CAS and back off before the caller
    /// re-reads and re-applies.
    ///
    /// The retry is always a full re-read: a decision computed against the
    /// document that lost must never be replayed onto the one that won.
    async fn generation_conflict(&self, group_id: &str, method: &'static str, conflicts: &mut u32) {
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", method)]);

        *conflicts += 1;

        if *conflicts == CAS_CONFLICT_WARN {
            warn!(
                group_id,
                cas_conflicts = *conflicts,
                method,
                "repeated generation CAS conflicts"
            );
        }

        sleep(cas_conflict_backoff(*conflicts)).await;
    }

    /// Refresh this member's liveness, if it is due.
    ///
    /// This is the write that used to churn the shared `{group}.json` etag once
    /// a second per member and starve the leader's assignment CAS. It now goes
    /// to the member's own object, at most once per session/2, and contends
    /// with nothing.
    async fn renew_member(
        &self,
        group_id: &str,
        member_id: &str,
        session_timeout_ms: i32,
        now: SystemTime,
    ) -> Result<()> {
        if member_id.is_empty() {
            return Ok(());
        }

        let Some((member, version)) = self.storage.read_group_member(group_id, member_id).await?
        else {
            // Nothing to renew: a member with no document is one this request
            // is not registering either (a heartbeat from a stranger, a commit
            // from a simple consumer). The sweep reclaims the membership.
            return Ok(());
        };

        let now_ms = epoch_ms(now);

        if !liveness_renewal_due(Some(&member), now_ms, session_timeout_ms) {
            return Ok(());
        }

        let renewed = MemberDoc {
            last_contact_ms: now_ms,
            ..member
        }
        .bumped();

        match self
            .storage
            .write_group_member(group_id, member_id, renewed, Some(version))
            .await
        {
            Ok(_) => Ok(()),

            // Lost to this member's own other in-flight request, on this
            // replica or another. That write carried a reading at least as
            // fresh as this one, so there is nothing to redo.
            Err(UpdateError::Outdated { .. }) => Ok(()),

            // The document was reaped between this read and this write — the
            // sweep found the session lapsed. That is the same answer the
            // `read_group_member` guard above gives when the document is already
            // gone: there is nothing to renew, and the membership is the sweep's
            // to reclaim (#431).
            Err(UpdateError::Vanished) => Ok(()),

            Err(error) => Err(update_error(error)),
        }
    }

    /// The dead-member sweep, which replaces `missed_heartbeat` (#359).
    ///
    /// Every join, sync and heartbeat runs this, and it does something only when
    /// the group's own `swept_at_ms` says the last sweep was more than
    /// session/2 ago — so N replicas serving a group cost **one** sweep per
    /// group per session/2 between them, not one each.
    ///
    /// The verdict is a pure function of the member documents and the clock
    /// ([`MemberDoc::is_expired`]), which is what makes racing replicas safe:
    /// identical inputs give identical verdicts, so the first CAS to land is
    /// the one they were all trying to make and the losers see a fresh stamp
    /// and stop. A member the generation names but whose document is gone is
    /// expired — the document is written before the generation admits the
    /// member, so its absence cannot be a member that has not arrived yet.
    ///
    /// Returns the group as it now stands when it wrote, so the caller
    /// continues against the state it produced rather than the one it read.
    async fn sweep(
        &self,
        group_id: &str,
        view: &GroupView,
        now: SystemTime,
    ) -> Result<Option<GroupView>> {
        if !view.exists() || view.generation.members.is_empty() {
            return Ok(None);
        }

        let now_ms = epoch_ms(now);
        let due_after = i64::from(view.session_timeout_ms()) / 2;

        if now_ms.saturating_sub(view.generation.swept_at_ms) < due_after {
            return Ok(None);
        }

        // From the generation's member set, never a LIST: the set is
        // authoritative, and listing would put one on the request path.
        let expired = futures::stream::iter(view.generation.members.keys().cloned().map(
            |member_id| async move {
                let held = self
                    .storage
                    .read_group_member(group_id, &member_id)
                    .await
                    .inspect_err(|error| debug!(?error, group_id, member_id))
                    .ok()
                    .flatten();

                held.is_none_or(|(member, _)| member.is_expired(now_ms))
                    .then_some(member_id)
            },
        ))
        .buffered(MEMBER_FETCH_CONCURRENCY)
        .filter_map(|expired| async move { expired })
        .collect::<BTreeSet<_>>()
        .await;

        let mut next = view.generation.clone();
        next.swept_at_ms = now_ms;

        if !expired.is_empty() {
            for member_id in &expired {
                _ = next.members.remove(member_id);
            }

            // A membership change is a rebalance, and the leader is re-elected
            // by the first member to join: the same rule that heals an orphan,
            // rather than a second one that has to agree with it.
            next = GroupView::rebalanced(next, now_ms);
            next.leader = None;

            info!(
                group_id,
                generation_id = next.generation_id,
                evicted = ?expired,
                "evicting members whose sessions lapsed",
            );
        }

        match self
            .storage
            .update_group_generation(group_id, next.bumped(), view.version.clone())
            .await
        {
            Ok(_) => {
                // Best-effort: an orphaned document is harmless, since the
                // generation's member set is what is authoritative.
                for member_id in &expired {
                    _ = self
                        .storage
                        .delete_group_member(group_id, member_id)
                        .await
                        .inspect_err(|error| debug!(?error, group_id, member_id));
                }
            }

            // Another replica swept first, or the group moved. Either way its
            // verdict was computed from the same documents as ours — and a
            // generation document that has gone entirely says the same thing
            // more loudly (#431).
            Err(UpdateError::Outdated { .. } | UpdateError::Vanished) => (),

            Err(error) => return Err(update_error(error)),
        }

        self.read_view(group_id).await.map(Some)
    }

    /// The member list a leader's `JoinGroup` response carries.
    ///
    /// One document per member the generation names — no listing, and only on
    /// the response that needs it, which is one member's join per rebalance.
    /// A member named by the generation whose document has gone is reported
    /// with empty metadata rather than dropped: the generation is what says it
    /// is a member.
    async fn join_members(
        &self,
        group_id: &str,
        generation: &GenerationDoc,
    ) -> Vec<JoinGroupResponseMember> {
        futures::stream::iter(generation.members.clone().into_iter().map(
            |(member_id, held)| async move {
                self.storage
                    .read_group_member(group_id, &member_id)
                    .await
                    .inspect_err(|error| debug!(?error, group_id, member_id))
                    .ok()
                    .flatten()
                    .map_or_else(
                        || {
                            JoinGroupResponseMember::default()
                                .member_id(member_id.clone())
                                .group_instance_id(held.group_instance_id.clone())
                        },
                        |(member, _)| member.join_response,
                    )
            },
        ))
        .buffered(MEMBER_FETCH_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
    }

    async fn fetch_offset(
        &self,
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

    async fn commit_offset(&self, detail: &OffsetCommit<'_>) -> Result<Body> {
        let retention_time_ms = detail.retention_time_ms.map_or(Ok(None), |ms| {
            u64::try_from(ms)
                .map(Duration::from_millis)
                .map_err(Error::from)
                .map(Some)
        })?;

        let Some(topics) = detail.topics else {
            return Ok(offset_commit_error(detail, ErrorCode::UnknownMemberId));
        };

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
    }
}

/// Every partition of an `OffsetCommit` answered with the same error.
fn offset_commit_error(detail: &OffsetCommit<'_>, error_code: ErrorCode) -> Body {
    OffsetCommitResponse::default()
        .throttle_time_ms(Some(0))
        .topics(detail.topics.map(|topics| {
            topics
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
                                        .error_code(error_code.into())
                                })
                                .collect()
                        }))
                })
                .collect()
        }))
        .into()
}

/// A failed conditional write, as this crate's error.
///
/// `Outdated` never reaches here: every caller that can lose a CAS handles the
/// conflict itself, because re-reading and re-applying is the whole point.
fn update_error<T>(error: UpdateError<T>) -> Error {
    match error {
        UpdateError::Error(error) => error.into(),
        UpdateError::SerdeJson(error) => error.into(),
        UpdateError::Uuid(uuid) => Error::Message(format!("uuid: {uuid}")),
        UpdateError::MissingEtag => Error::Message(String::from("missing e-tag")),
        UpdateError::Outdated { .. } => Error::Message(String::from("outdated")),
        // Every caller that can meet one handles it before reaching here (#431).
        // Reaching this is a call site that forgot, so it is named rather than
        // folded into "outdated" — the two mean different things and the whole
        // point of the variant is that a caller must say which it is acting on.
        UpdateError::Vanished => Error::Message(String::from("vanished")),
    }
}

/// A `SyncGroup` answer carrying only an error.
fn sync_error(view: &GroupView, error_code: ErrorCode) -> Body {
    SyncGroupResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(error_code.into())
        .protocol_type(view.generation.protocol_type.clone())
        .protocol_name(view.generation.protocol_name.clone())
        .assignment(Bytes::from_static(b""))
        .into()
}

/// A `JoinGroup` answer carrying only an error, reported against the group as
/// it stands.
///
/// `protocol_type` falls back to the one the request declared: a group that
/// does not exist yet has none of its own, and a client that is told nothing
/// about the protocol cannot tell an unknown group from a disagreement over it.
fn join_error(
    view: &GroupView,
    error_code: ErrorCode,
    member_id: &str,
    protocol_type: &str,
) -> Body {
    JoinGroupResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(error_code.into())
        .generation_id(if view.exists() {
            view.generation_id()
        } else {
            -1
        })
        .protocol_type(
            view.generation
                .protocol_type
                .clone()
                .or_else(|| Some(protocol_type.to_owned())),
        )
        .protocol_name(Some(
            view.generation.protocol_name.clone().unwrap_or_default(),
        ))
        .leader("".into())
        .skip_assignment(view.generation.skip_assignment.or(Some(false)))
        .member_id(member_id.to_owned())
        .members(Some([].into()))
        .into()
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

        // How long this call has been long-polling, on the MONOTONIC clock
        // (#256). Deliberately not the wall clock: `SystemTime` makes
        // `elapsed()` return `Err` after a backwards NTP step — which `?`
        // turned into an error response for a member that was merely waiting —
        // and it cannot be paused, so a test could only buy determinism by
        // waiting out the real duration.
        let polling_since = Instant::now();
        let mut conflicts = 0u32;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_loop")]);

            let now = (self.now)();
            let now_ms = epoch_ms(now);

            let mut view = self.read_view(group_id).await?;

            if let Some(swept) = self.sweep(group_id, &view, now).await? {
                view = swept;
            }

            // Sampled here as well as on the heartbeat path (#240).
            //
            // Heartbeat alone is blind to the failure this detector exists for.
            // `AbstractCoordinator$HeartbeatThread` disables itself while the
            // consumer has not joined, so a group whose members are stuck
            // *trying* to join sends no heartbeat at all. Join keeps arriving
            // from exactly that consumer — every `poll()` retries it — so it is
            // a live sampling source precisely when heartbeats are not.
            _ = self.observe_rebalance(group_id, &view, now);

            let Some(protocols) = protocols.filter(|protocols| !protocols.is_empty()) else {
                debug!(join_outcome = ?ErrorCode::InvalidRequest);
                return Ok(join_error(
                    &view,
                    ErrorCode::InvalidRequest,
                    "",
                    protocol_type,
                ));
            };

            // The group's protocol, once it has one, is what every later member
            // must speak. The first member to join settles it, along with the
            // timeouts the group is run on.
            let protocol = match view.generation.protocol_name.as_deref() {
                Some(protocol_name) => {
                    let Some(protocol) = protocols
                        .iter()
                        .find(|protocol| protocol.name == protocol_name)
                    else {
                        debug!(join_outcome = ?ErrorCode::InconsistentGroupProtocol);
                        return Ok(join_error(
                            &view,
                            ErrorCode::InconsistentGroupProtocol,
                            "",
                            protocol_type,
                        ));
                    };

                    protocol
                }

                None => &protocols[0],
            };

            if member_id.is_empty() && group_instance_id.is_none() {
                // KIP-394: reply with a generated member id but leave the group
                // untouched — the member only registers (and the generation
                // only moves) when it re-joins with this id. Registering a
                // phantom member here would rebalance the group for a client
                // that may never come back. Zero object writes, and on this
                // path zero reads of anything but the generation.
                let minted = client_id.map_or_else(
                    || format!("{}", Uuid::new_v4()),
                    |client_id| format!("{client_id}-{}", Uuid::new_v4()),
                );

                debug!(?minted, join_outcome = ?ErrorCode::MemberIdRequired);

                let body = JoinGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::MemberIdRequired.into())
                    .generation_id(-1)
                    .protocol_type(
                        view.generation
                            .protocol_type
                            .clone()
                            .or_else(|| Some(protocol_type.to_owned())),
                    )
                    .protocol_name(Some("".into()))
                    .leader("".into())
                    .skip_assignment(view.generation.skip_assignment.or(Some(false)))
                    .member_id(minted)
                    .members(Some([].into()))
                    .into();

                return Ok(body);
            }

            // A static member reuses the id it already holds in the group, so
            // its restart is not a membership change.
            let member_id = group_instance_id.map_or_else(
                || member_id.to_owned(),
                |group_instance_id| {
                    if !member_id.is_empty() {
                        return member_id.to_owned();
                    }

                    view.generation
                        .members
                        .iter()
                        .find(|(_, held)| {
                            held.group_instance_id.as_deref() == Some(group_instance_id)
                        })
                        .map_or_else(
                            || format!("{group_instance_id}-{}", Uuid::new_v4()),
                            |(member_id, _)| member_id.to_owned(),
                        )
                },
            );

            let held = self
                .storage
                .read_group_member(group_id, member_id.as_str())
                .await?;

            let join_response = JoinGroupResponseMember::default()
                .member_id(member_id.clone())
                .group_instance_id(group_instance_id.map(ToOwned::to_owned))
                .metadata(protocol.metadata.clone());

            let mut next = view.generation.clone();
            let mut write = !view.exists();

            if next.protocol_name.is_none() {
                next.protocol_type = Some(protocol_type.to_owned());
                next.protocol_name = Some(protocol.name.clone());
                next.session_timeout_ms = session_timeout_ms;
                next.rebalance_timeout_ms = rebalance_timeout_ms;
                write = true;
            }

            if !view.exists() {
                next.inception_ms = now_ms;
                next.state_since_ms = now_ms;
                next.swept_at_ms = now_ms;
                next.skip_assignment = Some(false);

                // A brand-new group is born one below its first generation, so
                // the membership change below mints generation 0 — the same
                // arithmetic every later join does, rather than a special case
                // that has to agree with it.
                next.generation_id = -1;
            }

            let membership_changed = match view.generation.members.get(member_id.as_str()) {
                None => true,

                // The encoded metadata changed but the subscribed topic set did
                // not: a static member's soft update, or a cooperative
                // consumer's KIP-792-only rejoin (fresh generationId /
                // ownedPartitions / sticky userData, same topics). The new
                // metadata is recorded in the member's own document — which is
                // what the leader reads — WITHOUT bumping the generation.
                // Bumping here would invalidate any in-flight SyncGroup and,
                // with many members re-joining, keep the group from ever
                // converging.
                Some(_) if group_instance_id.is_some() => false,

                Some(_) => held.as_ref().is_none_or(|(member, _)| {
                    member.join_response.metadata != protocol.metadata
                        && !same_subscription_topics(
                            &member.join_response.metadata,
                            &protocol.metadata,
                        )
                }),
            };

            if membership_changed {
                _ = next.members.insert(
                    member_id.clone(),
                    MemberRef {
                        group_instance_id: group_instance_id.map(ToOwned::to_owned),
                    },
                );

                next = GroupView::rebalanced(next, now_ms);
                write = true;
            }

            // Heal an orphaned leader before considering a promotion: a leader
            // that is no longer a member deadlocks the group — every live
            // member is told someone else leads, so nobody ever sends
            // assignments (#240).
            if next
                .leader
                .as_ref()
                .is_some_and(|leader| !next.members.contains_key(leader))
            {
                warn!(
                    group_id,
                    orphaned_leader = next.leader.as_deref(),
                    generation_id = next.generation_id,
                    "leader is no longer a member; clearing so a live member is promoted (#240)"
                );

                _ = next.leader.take();
            }

            // First in elects itself. One rule covers the group's first
            // leader, the promotion after a leader leaves, and the healing
            // above; the CAS resolves the race, because a loser re-reads, sees
            // a leader and re-applies as an add.
            if next.leader.is_none() {
                info!(member_id, group_id, generation_id = next.generation_id);

                _ = next.leader.replace(member_id.clone());
                next.state_since_ms = now_ms;
                write = true;
            }

            // The member's own document, written **before** the generation
            // admits it: a member the generation names must always have one,
            // because the sweep reads a missing document as a lapsed session
            // and the leader's join response reads it for the subscription.
            let member = MemberDoc {
                seq: held.as_ref().map_or(0, |(member, _)| member.seq),
                last_contact_ms: now_ms,
                session_timeout_ms: next.session_timeout_ms,
                rebalance_timeout_ms: next.rebalance_timeout_ms,
                group_instance_id: group_instance_id.map(ToOwned::to_owned),
                join_response: join_response.clone(),
                rest: held
                    .as_ref()
                    .map(|(member, _)| member.rest.clone())
                    .unwrap_or_default(),
            };

            let member_changed = held
                .as_ref()
                .is_none_or(|(held, _)| held.join_response != join_response);

            if member_changed
                || liveness_renewal_due(
                    held.as_ref().map(|(member, _)| member),
                    now_ms,
                    next.session_timeout_ms,
                )
            {
                match self
                    .storage
                    .write_group_member(
                        group_id,
                        member_id.as_str(),
                        member.bumped(),
                        held.as_ref().map(|(_, version)| version.clone()),
                    )
                    .await
                {
                    Ok(_) => (),

                    // This member's other in-flight request won. Re-read
                    // everything rather than reason about which is newer —
                    // through the same backoff as a generation conflict, so a
                    // member whose two requests are chasing each other does not
                    // spin against the store.
                    // `Vanished` takes the same path: the loop re-reads, finds
                    // no held document, and writes with `None` — a create, which
                    // is what re-registering a member whose document was reaped
                    // mid-join means (#431).
                    Err(UpdateError::Outdated { .. } | UpdateError::Vanished) => {
                        self.generation_conflict(group_id, "join_member_outdated", &mut conflicts)
                            .await;
                        continue;
                    }

                    Err(error) => return Err(update_error(error)),
                }
            }

            if write {
                match self
                    .storage
                    .update_group_generation(group_id, next.clone().bumped(), view.version.clone())
                    .await
                {
                    Ok(version) => {
                        // A membership change mints a generation, and
                        // `assignment/{gen}` for it cannot exist yet.
                        let carried = (next.generation_id == view.generation_id())
                            .then_some(view.assignment)
                            .flatten();

                        view = GroupView {
                            generation: next.bumped(),
                            version: Some(version),
                            assignment: carried,
                        };
                    }

                    Err(UpdateError::Outdated { .. } | UpdateError::Vanished) => {
                        self.generation_conflict(group_id, "join_outdated", &mut conflicts)
                            .await;
                        continue;
                    }

                    Err(error) => return Err(update_error(error)),
                }
            } else {
                COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_noop_skip")]);
            }

            let is_leader = view.leader() == Some(member_id.as_str());

            // Built without the member list: the long-poll decision does not
            // need it, and a leader held by the join window would otherwise pay
            // a per-member read for every second it waits.
            let body = Body::from(
                JoinGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .generation_id(view.generation_id())
                    .protocol_type(view.generation.protocol_type.clone())
                    .protocol_name(view.generation.protocol_name.clone())
                    .leader(view.leader().unwrap_or_default().to_owned())
                    .skip_assignment(view.generation.skip_assignment)
                    .member_id(member_id.clone())
                    .members(Some([].into())),
            );

            // The join window is derived from the group, not from what this
            // call has happened to see: `members_changed_at_ms` is persisted, so
            // every replica reaches the same verdict about the same group at the
            // same instant — which is what makes the barrier hold when a
            // group's members are scattered across replicas.
            let membership_quiescent = Duration::from_millis(
                now_ms
                    .saturating_sub(view.generation.members_changed_at_ms)
                    .max(0) as u64,
            ) >= JOIN_QUIESCENCE;

            // `rebalance_timeout_ms` bounds how long the leader may be held
            // (the Java client allows JoinGroup responses up to
            // `rebalance_timeout + 5s`), falling back to the session timeout
            // when unset, as Kafka does for old protocol versions.
            let join_window_ms = u128::try_from(
                view.generation
                    .rebalance_timeout_ms
                    .or(rebalance_timeout_ms)
                    .unwrap_or(view.session_timeout_ms()),
            )
            .unwrap_or_default();

            let decision = join_long_poll(
                &view,
                &body,
                group_instance_id,
                polling_since.elapsed().as_millis(),
                view.is_forming(),
                membership_quiescent,
                join_window_ms,
            );

            match decision {
                // A cancelled wait falls through to the answer this iteration
                // already produced, which is exactly what the poll would have
                // returned at its own deadline (#361).
                LongPoll::Wait(pause, method) if self.waited(pause, method).await => continue,

                LongPoll::Wait(..) | LongPoll::Respond(_) => {
                    debug!(join_outcome = ?ErrorCode::None, generation_id = view.generation_id());

                    let Body::JoinGroupResponse(response) = body else {
                        unreachable!()
                    };

                    // Only the leader is handed the membership, and only now
                    // that it is being answered.
                    return Ok(response
                        .members(Some(if is_leader {
                            self.join_members(group_id, &view.generation).await
                        } else {
                            [].into()
                        }))
                        .into());
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

        // Once per request, not once per iteration: this inspects the
        // assignments the caller sent, which do not change between retries.
        warn_on_overlapping_assignments(group_id, generation_id, assignments);

        // Monotonic, for the same reasons as `join`'s (#256).
        let polling_since = Instant::now();

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync_loop")]);

            let now = (self.now)();
            let mut view = self.read_view(group_id).await?;

            if let Some(swept) = self.sweep(group_id, &view, now).await? {
                view = swept;
            }

            // Sampled from the state this iteration read, for the same reason
            // as `join`'s — see there.
            _ = self.observe_rebalance(group_id, &view, now);

            let (view, body) = self
                .sync_once(group_id, generation_id, member_id, view, assignments, now)
                .await?;

            self.renew_member(group_id, member_id, view.session_timeout_ms(), now)
                .await?;

            let decision = sync_long_poll(
                &view,
                &body,
                group_instance_id,
                polling_since.elapsed().as_millis(),
            );

            match decision {
                LongPoll::Respond(None) => return Ok(body),

                LongPoll::Respond(Some(error_code)) => return Ok(set_error_code(body, error_code)),

                LongPoll::Wait(pause, method) => {
                    if self.waited(pause, method).await {
                        continue;
                    }

                    // Stopping. A sync that has not been assigned yet is told
                    // to rebalance — the same answer it gets at its own
                    // deadline — so the member re-joins against a replica that
                    // is staying rather than reading a closed socket (#361).
                    return Ok(if view.is_assigned(&body) {
                        body
                    } else {
                        set_error_code(body, ErrorCode::RebalanceInProgress)
                    });
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

        // Who the request names, in the two shapes LeaveGroup has: the older
        // single-member form, and the batched one.
        let departing = member_id.map_or_else(
            || {
                members.map_or_else(Vec::new, |members| {
                    members
                        .iter()
                        .map(|member| (member.member_id.clone(), member.group_instance_id.clone()))
                        .collect()
                })
            },
            |member_id| vec![(member_id.to_owned(), None)],
        );

        let mut conflicts = 0u32;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "leave_loop")]);

            let now = (self.now)();
            let now_ms = epoch_ms(now);
            let view = self.read_view(group_id).await?;

            let mut next = view.generation.clone();
            let mut left = BTreeSet::new();

            let responses = departing
                .iter()
                .map(|(member_id, group_instance_id)| {
                    let known = next.members.remove(member_id).is_some();

                    if known {
                        _ = left.insert(member_id.clone());
                    }

                    MemberResponse::default()
                        .member_id(member_id.clone())
                        .group_instance_id(group_instance_id.clone())
                        .error_code(
                            if known {
                                ErrorCode::None
                            } else {
                                ErrorCode::UnknownMemberId
                            }
                            .into(),
                        )
                })
                .collect::<Vec<_>>();

            let body = Body::from(
                LeaveGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::None.into())
                    .members(Some(responses)),
            );

            if left.is_empty() {
                return Ok(body);
            }

            next = GroupView::rebalanced(next, now_ms);

            // The departing member may be the elected leader. Left in place it
            // freezes the group: `join` never promotes anyone else, and every
            // follower sync is refused as not-leader (#240).
            if next
                .leader
                .as_ref()
                .is_some_and(|leader| !next.members.contains_key(leader))
            {
                info!(
                    group_id,
                    departed_leader = next.leader.as_deref(),
                    generation_id = next.generation_id,
                    "leader left; clearing so a live member is promoted (#240)"
                );

                _ = next.leader.take();
            }

            match self
                .storage
                .update_group_generation(group_id, next.bumped(), view.version.clone())
                .await
            {
                Ok(_) => {
                    // Best-effort, and after the generation has stopped naming
                    // them: an orphaned document is harmless, a document still
                    // there for a member the generation names is not.
                    for member_id in &left {
                        _ = self
                            .storage
                            .delete_group_member(group_id, member_id)
                            .await
                            .inspect_err(|error| debug!(?error, group_id, member_id));
                    }

                    return Ok(body);
                }

                Err(UpdateError::Outdated { .. } | UpdateError::Vanished) => {
                    if conflicts >= MAX_GENERATION_CAS_ATTEMPTS {
                        return Err(Error::Api(ErrorCode::RebalanceInProgress));
                    }

                    self.generation_conflict(group_id, "leave_outdated", &mut conflicts)
                        .await;
                }

                Err(error) => return Err(update_error(error)),
            }
        }
    }

    #[instrument(skip(self, offset_commit), fields(group_id = offset_commit.group_id, generation_id = offset_commit.generation_id_or_member_epoch))]
    async fn offset_commit(&self, offset_commit: OffsetCommit<'_>) -> Result<Body> {
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_commit")]);

        let group_id = offset_commit.group_id;
        let member_id = offset_commit.member_id.unwrap_or_default();
        let generation_id = offset_commit.generation_id_or_member_epoch.unwrap_or(-1);

        let now = (self.now)();

        // Kafka's own rule: a commit that claims no generation, or names no
        // member, is a simple consumer managing its own offsets. It is fenced
        // by nothing and reads nothing about the group — which is also what
        // stops the commit path creating group state for a group that has
        // none (#272).
        if generation_id >= 0 && !member_id.is_empty() {
            let Some((generation, _)) = self.storage.read_group_generation(group_id).await? else {
                debug!(group_id, member_id, offset_commit_outcome = ?ErrorCode::UnknownMemberId);
                return Ok(offset_commit_error(
                    &offset_commit,
                    ErrorCode::UnknownMemberId,
                ));
            };

            if !generation.members.contains_key(member_id) {
                debug!(group_id, member_id, offset_commit_outcome = ?ErrorCode::UnknownMemberId);
                return Ok(offset_commit_error(
                    &offset_commit,
                    ErrorCode::UnknownMemberId,
                ));
            }

            if generation.generation_id != generation_id {
                debug!(
                    group_id,
                    member_id,
                    generation_id,
                    current = generation.generation_id,
                    offset_commit_outcome = ?ErrorCode::IllegalGeneration,
                );

                return Ok(offset_commit_error(
                    &offset_commit,
                    ErrorCode::IllegalGeneration,
                ));
            }

            // A member that commits is a member that is alive. Folding liveness
            // into the commit path — commits arrive every ~5s — makes most
            // heartbeat renewals no-ops, and it goes to the member's own
            // document rather than to the offsets object, so nothing about
            // group expiry changes.
            self.renew_member(group_id, member_id, generation.session_timeout_ms, now)
                .await?;
        }

        match self.commit_offset(&offset_commit).await {
            Ok(body) => Ok(body),

            Err(reason) => {
                debug!(?reason);
                Ok(offset_commit_error(
                    &offset_commit,
                    ErrorCode::UnknownMemberId,
                ))
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

        self.fetch_offset(group_id, topics, groups, require_stable)
            .await
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

        let now = (self.now)();
        let mut view = self.read_view(group_id).await?;

        if let Some(swept) = self.sweep(group_id, &view, now).await? {
            view = swept;
        }

        // Every member heartbeats every few seconds, so this is the densest
        // sampling of a group's state available (#240).
        _ = self.observe_rebalance(group_id, &view, now);

        let error_code = if !view.exists() || !view.generation.members.contains_key(member_id) {
            ErrorCode::UnknownMemberId
        } else if generation_id > view.generation_id() {
            ErrorCode::IllegalGeneration
        } else if generation_id < view.generation_id() {
            // Someone joined, left or was swept: the member's generation is
            // behind, so it must re-join.
            ErrorCode::RebalanceInProgress
        } else {
            // Steady state: two reads, and one member-document write per
            // session/2. No LIST, no shared CAS, no forwarding.
            self.renew_member(group_id, member_id, view.session_timeout_ms(), now)
                .await?;

            ErrorCode::None
        };

        debug!(?error_code, generation_id = view.generation_id());

        Ok(HeartbeatResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(error_code.into())
            .into())
    }
}

impl<O> Controller<O>
where
    O: Storage + Clone,
{
    /// One `SyncGroup` iteration against the group as read.
    ///
    /// Returns the group as it stands afterwards — which the leader's own
    /// create moves — so the long-poll decision is taken against what this
    /// iteration produced rather than what it started from.
    async fn sync_once(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        view: GroupView,
        assignments: Option<&[SyncGroupRequestAssignment]>,
        now: SystemTime,
    ) -> Result<(GroupView, Body)> {
        if !view.exists() || !view.generation.members.contains_key(member_id) {
            debug!(?member_id, sync_outcome = ?ErrorCode::UnknownMemberId);
            let body = sync_error(&view, ErrorCode::UnknownMemberId);
            return Ok((view, body));
        }

        if generation_id > view.generation_id() {
            debug!(current = view.generation_id(), sync_outcome = ?ErrorCode::IllegalGeneration);
            let body = sync_error(&view, ErrorCode::IllegalGeneration);
            return Ok((view, body));
        }

        if generation_id < view.generation_id() {
            debug!(current = view.generation_id(), sync_outcome = ?ErrorCode::RebalanceInProgress);
            let body = sync_error(&view, ErrorCode::RebalanceInProgress);
            return Ok((view, body));
        }

        // The assignment is immutable and create-only, so if it is there it is
        // the answer — for the leader retrying as much as for a follower.
        if let Some(assignments) = view.assignments() {
            // A member of this generation that the leader's assignment does not
            // cover must re-join, not park on "stable" with zero partitions:
            // `error=None` plus an empty assignment reads to the client as a
            // valid empty assignment, and it then sits idle forever.
            let Some(assignment) = assignments.get(member_id).cloned() else {
                debug!(sync_outcome = ?ErrorCode::RebalanceInProgress, "not in the assignment");
                let body = sync_error(&view, ErrorCode::RebalanceInProgress);
                return Ok((view, body));
            };

            debug!(sync_outcome = ?ErrorCode::None, sync_assignment = true);

            let body = SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::None.into())
                .protocol_type(view.generation.protocol_type.clone())
                .protocol_name(view.generation.protocol_name.clone())
                .assignment(assignment)
                .into();

            return Ok((view, body));
        }

        // No assignment for this generation yet, so only the leader has
        // anything to do here.
        if view.leader() != Some(member_id) {
            debug!(leader = ?view.leader(), sync_outcome = ?ErrorCode::RebalanceInProgress);
            let body = sync_error(&view, ErrorCode::RebalanceInProgress);
            return Ok((view, body));
        }

        let requested = assignments
            .unwrap_or_default()
            .iter()
            .map(|assignment| (assignment.member_id.clone(), assignment.assignment.clone()))
            .collect::<BTreeMap<_, _>>();

        // A sync whose assignments do not cover the syncing member must not
        // form the group, for the same reason as above.
        if !requested.contains_key(member_id) {
            debug!(sync_outcome = ?ErrorCode::RebalanceInProgress, "leader not in its own assignment");
            let body = sync_error(&view, ErrorCode::RebalanceInProgress);
            return Ok((view, body));
        }

        let assignment = AssignmentDoc {
            generation_id: view.generation_id(),
            leader: member_id.to_owned(),
            protocol_type: view
                .generation
                .protocol_type
                .clone()
                .unwrap_or_else(|| String::from("consumer")),
            protocol_name: view.generation.protocol_name.clone().unwrap_or_default(),
            assignments: requested,
            assigned_at_ms: epoch_ms(now),
        };

        // The write liveness churn used to starve, now with no etag to lose:
        // one key, one winner. Finding it already there is that same leader
        // retrying — one leader per generation is guaranteed by the generation
        // CAS — so what is stored is adopted rather than overwritten.
        let assignment = match self
            .storage
            .create_group_assignment(group_id, view.generation_id(), assignment.clone())
            .await?
        {
            AssignmentOutcome::Created(_) => assignment,
            AssignmentOutcome::AlreadyExists(stored) => *stored,
        };

        // Fence against a rebalance that started while the create was in
        // flight: the assignment just written is then for a generation the
        // group has left, and answering from it would hand out an assignment
        // nobody else will honour.
        let after = self.read_view(group_id).await?;

        if after.generation_id() != view.generation_id() {
            debug!(
                assigned = view.generation_id(),
                current = after.generation_id(),
                sync_outcome = ?ErrorCode::RebalanceInProgress,
            );

            let body = sync_error(&after, ErrorCode::RebalanceInProgress);
            return Ok((after, body));
        }

        // Housekeeping, not correctness: keep this generation and the one
        // before it, so a member still finishing its sync against N-1 finds it.
        // `delete_groups` remains the backstop.
        _ = self
            .storage
            .delete_group_assignments_before(group_id, view.generation_id().saturating_sub(1))
            .await
            .inspect_err(|error| debug!(?error, group_id));

        let body = SyncGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .protocol_type(after.generation.protocol_type.clone())
            .protocol_name(after.generation.protocol_name.clone())
            .assignment(
                assignment
                    .assignments
                    .get(member_id)
                    .cloned()
                    .unwrap_or_else(|| Bytes::from_static(b"")),
            )
            .into();

        debug!(sync_outcome = ?ErrorCode::None, generation_id = after.generation_id());

        Ok((after, body))
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
    use std::sync::OnceLock;
    use tansu_sans_io::{
        consumer::{CONSUMER, ConsumerProtocolSubscription},
        offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    };
    use tansu_storage::{LatencyIntroducingStorage, StorageContainer};
    use tokio::time::{advance, timeout};
    use tracing::subscriber::DefaultGuard;
    use url::Url;

    const CLIENT_ID: &str = "console-consumer";
    const RANGE: &str = "range";
    const SESSION_TIMEOUT_MS: i32 = 45_000;
    const REBALANCE_TIMEOUT_MS: Option<i32> = Some(300_000);

    /// A wall clock derived from `tokio`'s paused one, so that a coordinator
    /// comparing a persisted epoch reading against `now` moves with the test
    /// instead of standing still while `sleep` races ahead. See
    /// [`Controller::now`] for what may be moved this way.
    fn paused_clock() -> SystemTime {
        /// Far enough from the epoch that a duration subtracted from a reading
        /// stays representable.
        const ORIGIN: Duration = Duration::from_secs(1_700_000_000);

        static STARTED: OnceLock<Instant> = OnceLock::new();

        SystemTime::UNIX_EPOCH
            + ORIGIN
            + Instant::now().duration_since(*STARTED.get_or_init(Instant::now))
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

    /// The store every cost assertion runs over: it counts what was written
    /// without pretending to be a different engine.
    type Counted = LatencyIntroducingStorage<Arc<Box<dyn Storage>>>;

    async fn counted_storage() -> Result<Counted> {
        memory_storage()
            .await
            .map(|storage| LatencyIntroducingStorage::new(storage).with_latency(0..1))
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

    fn protocols(metadata: &Bytes) -> Vec<JoinGroupRequestProtocol> {
        vec![
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(metadata.clone()),
        ]
    }

    async fn join_group<O>(
        controller: &Controller<O>,
        group_id: &str,
        member_id: &str,
        metadata: &Bytes,
    ) -> Result<JoinGroupResponse>
    where
        O: Storage + Clone,
    {
        match controller
            .join(
                Some(CLIENT_ID),
                group_id,
                SESSION_TIMEOUT_MS,
                REBALANCE_TIMEOUT_MS,
                member_id,
                None,
                CONSUMER,
                Some(&protocols(metadata)[..]),
                None,
            )
            .await?
        {
            Body::JoinGroupResponse(join) => Ok(join),
            otherwise => panic!("{otherwise:?}"),
        }
    }

    /// The KIP-394 dance, as far as a registered member: mint an id, then join
    /// with it.
    async fn register<O>(
        controller: &Controller<O>,
        group_id: &str,
        metadata: &Bytes,
    ) -> Result<(String, JoinGroupResponse)>
    where
        O: Storage + Clone,
    {
        let minted = join_group(controller, group_id, "", metadata).await?;

        assert_eq!(
            i16::from(ErrorCode::MemberIdRequired),
            minted.error_code,
            "a first join must mint a member id"
        );

        let join = join_group(controller, group_id, &minted.member_id, metadata).await?;

        Ok((minted.member_id, join))
    }

    async fn sync_group<O>(
        controller: &Controller<O>,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        assignments: &[SyncGroupRequestAssignment],
    ) -> Result<SyncGroupResponse>
    where
        O: Storage + Clone,
    {
        match controller
            .sync(
                group_id,
                generation_id,
                member_id,
                None,
                Some(CONSUMER),
                Some(RANGE),
                Some(assignments),
            )
            .await?
        {
            Body::SyncGroupResponse(sync) => Ok(sync),
            otherwise => panic!("{otherwise:?}"),
        }
    }

    async fn heartbeat<O>(
        controller: &Controller<O>,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
    ) -> Result<HeartbeatResponse>
    where
        O: Storage + Clone,
    {
        match controller
            .heartbeat(group_id, generation_id, member_id, None)
            .await?
        {
            Body::HeartbeatResponse(heartbeat) => Ok(heartbeat),
            otherwise => panic!("{otherwise:?}"),
        }
    }

    async fn leave_group<O>(
        controller: &Controller<O>,
        group_id: &str,
        member_id: &str,
    ) -> Result<LeaveGroupResponse>
    where
        O: Storage + Clone,
    {
        match controller.leave(group_id, Some(member_id), None).await? {
            Body::LeaveGroupResponse(leave) => Ok(leave),
            otherwise => panic!("{otherwise:?}"),
        }
    }

    /// The group's composition as persisted, which is what every assertion
    /// about the layout reads.
    async fn generation_of<O>(storage: &O, group_id: &str) -> Result<GenerationDoc>
    where
        O: Storage,
    {
        storage
            .read_group_generation(group_id)
            .await
            .map(|held| held.expect("the group must have a generation").0)
            .map_err(Into::into)
    }

    async fn generation_version<O>(storage: &O, group_id: &str) -> Result<Version>
    where
        O: Storage,
    {
        storage
            .read_group_generation(group_id)
            .await
            .map(|held| held.expect("the group must have a generation").1)
            .map_err(Into::into)
    }

    fn assignment_of(member_id: &str) -> SyncGroupRequestAssignment {
        SyncGroupRequestAssignment::default()
            .member_id(member_id.to_owned())
            .assignment(Bytes::from(format!("assignment-{member_id}")))
    }

    /// Drive one consumer against `controller` until it holds a non-empty
    /// assignment, the way a Kafka client would: mint an id, join, and — if it
    /// is the leader — sync an assignment covering every member of its join
    /// response. `RebalanceInProgress` or an empty assignment means re-join.
    async fn drive_member<O>(
        controller: Controller<O>,
        group_id: &'static str,
        index: usize,
    ) -> Result<(String, Bytes)>
    where
        O: Storage + Clone,
    {
        let metadata = encode_subscription(&["t"], Some(index as i32));
        let mut member_id = String::new();

        loop {
            let join = join_group(&controller, group_id, member_id.as_str(), &metadata).await?;

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
                    .map(|member| assignment_of(&member.member_id))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };

            let sync = sync_group(
                &controller,
                group_id,
                join.generation_id,
                member_id.as_str(),
                &assignments[..],
            )
            .await?;

            if sync.error_code == i16::from(ErrorCode::None) && !sync.assignment.is_empty() {
                return Ok((member_id, sync.assignment));
            }
        }
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

    /// #240: the stall threshold comes from the group, not from a constant.
    ///
    /// The first version of this detector used a flat 60s, on the assumption
    /// that a healthy rebalance takes seconds. Production contradicted that in
    /// under an hour: 9-14 member groups routinely completed in **just over a
    /// minute** (60.1s, 60.4s, 62.9s, 67.2s, 81.2s observed, each recovering),
    /// so the warning fired ~2 per minute on healthy groups — which is how a
    /// signal becomes one operators scroll past.
    #[test]
    fn the_stall_threshold_follows_the_group() {
        assert_eq!(
            Duration::from_secs(300),
            stall_after(Some(300_000)),
            "a declared timeout is the threshold",
        );

        assert_eq!(
            REBALANCE_STALL_FLOOR,
            stall_after(None),
            "a group that declares none gets the floor",
        );

        // Below the floor the floor wins, in both the implausible and the merely
        // tight case: the observed healthy tail is ~81s, so a threshold under
        // that reports groups doing nothing wrong.
        assert_eq!(REBALANCE_STALL_FLOOR, stall_after(Some(1)));
        assert_eq!(REBALANCE_STALL_FLOOR, stall_after(Some(60_000)));
        assert_eq!(REBALANCE_STALL_FLOOR, stall_after(Some(0)));
        assert_eq!(REBALANCE_STALL_FLOOR, stall_after(Some(-1)));

        assert!(
            REBALANCE_STALL_FLOOR > Duration::from_millis(81_203),
            "the floor must clear the slowest healthy rebalance measured in production",
        );
    }

    #[test]
    fn liveness_renewal_is_due_at_half_the_session() {
        let member = |last_contact_ms| MemberDoc {
            last_contact_ms,
            session_timeout_ms: 30_000,
            ..Default::default()
        };

        // Half of 30s is 15s.
        assert!(!liveness_renewal_due(Some(&member(1_000)), 1_000, 30_000));
        assert!(!liveness_renewal_due(Some(&member(1_000)), 15_999, 30_000));
        assert!(liveness_renewal_due(Some(&member(1_000)), 16_000, 30_000));

        // A member with no document at all must write one: the sweep reads a
        // missing document as a lapsed session, so declining to write here
        // would evict a member that has just joined.
        assert!(liveness_renewal_due(None, 1_000, 30_000));

        // A group that declares no session timeout still has to renew on some
        // cadence, or nothing is ever written and everyone is swept.
        assert!(liveness_renewal_due(
            Some(&member(1_000)),
            1_000 + i64::from(DEFAULT_SESSION_TIMEOUT_MS) / 2,
            0,
        ));
    }

    /// The state a request derives must be the state `DescribeGroups` reports,
    /// or the coordinator and every admin tool disagree about the same group.
    #[test]
    fn a_view_derives_the_state_it_reports() {
        let empty = GroupView {
            generation: GenerationDoc {
                session_timeout_ms: SESSION_TIMEOUT_MS,
                ..Default::default()
            },
            version: Some(Version::default()),
            assignment: None,
        };
        assert_eq!(ConsumerGroupState::Empty, empty.state());
        assert!(empty.is_forming());

        let mut joined = empty.clone();
        joined.generation.members = BTreeMap::from([("m-1".to_owned(), MemberRef::default())]);
        assert_eq!(ConsumerGroupState::Assigning, joined.state());
        assert!(joined.is_forming());

        let mut led = joined.clone();
        led.generation.leader = Some("m-1".into());
        assert_eq!(ConsumerGroupState::CompletingRebalance, led.state());
        assert!(led.is_forming());

        let mut stable = led.clone();
        stable.assignment = Some(AssignmentDoc {
            generation_id: 0,
            leader: "m-1".into(),
            protocol_type: CONSUMER.into(),
            protocol_name: RANGE.into(),
            assignments: BTreeMap::from([("m-1".to_owned(), Bytes::from_static(b"a"))]),
            assigned_at_ms: 0,
        });
        assert_eq!(ConsumerGroupState::Stable, stable.state());
        assert!(!stable.is_forming());

        // An assignment left behind by an earlier generation is not this
        // generation's assignment. Reading it as one is how a rebalance in
        // progress reports as `Stable` and a client parks on a dead assignment.
        let mut moved_on = stable.clone();
        moved_on.generation.generation_id = 1;
        assert_eq!(
            ConsumerGroupState::CompletingRebalance,
            moved_on.state(),
            "an assignment from a dead generation must not make a group stable",
        );
        assert!(moved_on.is_forming());
    }

    /// #240: a group whose members never finish joining must still be reported.
    ///
    /// The detector was hooked only to the heartbeat path, on the reasoning that
    /// members heartbeat every few seconds so it is the densest sampling
    /// available. That is true of a group whose members are *in* the group, and
    /// false of the failure this exists for: `AbstractCoordinator$HeartbeatThread`
    /// disables itself while the consumer has not joined, so a consumer stuck
    /// trying to join sends no heartbeat at all. Measured in production as a
    /// group 90+ minutes in `CompletingRebalance` with 16 members and not one
    /// line about it across ten replicas.
    #[tokio::test(start_paused = true)]
    async fn a_group_that_never_joins_is_still_observed() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "never-joins";

        let storage = memory_storage().await?;
        let controller = Controller::with_storage(storage.clone())?.with_now(paused_clock);

        // A group already mid-rebalance and already past its own rebalance
        // timeout: members joined, leader elected, no assignment distributed,
        // for longer than the group itself said to wait. This is what a stuck
        // consumer keeps re-joining into, and what `CompletingRebalance` maps
        // from.
        let now_ms = epoch_ms(paused_clock());
        let stalled_since_ms = now_ms - i64::from(REBALANCE_TIMEOUT_MS.unwrap_or(300_000)) * 2;

        _ = storage
            .update_group_generation(
                GROUP_ID,
                GenerationDoc {
                    generation_id: 1,
                    protocol_type: Some(CONSUMER.into()),
                    protocol_name: Some(RANGE.into()),
                    leader: Some("member-1".into()),
                    members: BTreeMap::from([("member-1".to_owned(), MemberRef::default())]),
                    members_changed_at_ms: stalled_since_ms,
                    state_since_ms: stalled_since_ms,
                    swept_at_ms: now_ms,
                    session_timeout_ms: SESSION_TIMEOUT_MS,
                    rebalance_timeout_ms: REBALANCE_TIMEOUT_MS,
                    inception_ms: stalled_since_ms,
                    ..Default::default()
                },
                None,
            )
            .await
            .map_err(update_error)?;

        _ = join_group(
            &controller,
            GROUP_ID,
            "",
            &encode_subscription(&["t"], None),
        )
        .await?;

        // Reading the memo directly rather than the log: what is under test is
        // that the forming path observes at all, which is the hook a refactor
        // could silently drop — and did, for the whole of beta.31 and beta.32.
        let sampled = controller
            .rebalance_stalls
            .lock()
            .map(|stalls| stalls.get(&GROUP_ID.to_owned()).is_some())
            .unwrap_or_default();

        assert!(
            sampled,
            "a group mid-rebalance was not observed on the join path, so a consumer \
             that never joins — and therefore never heartbeats — is invisible",
        );

        Ok(())
    }

    /// #240: a group that stops making progress mid-rebalance must say so, once
    /// per episode — and must say so from the moment the *group* stalled, not
    /// from the moment this replica first looked at it.
    ///
    /// The episode's start used to be an in-memory stamp, so a restart began a
    /// blind window exactly when a wedge is most likely to have been created:
    /// the incident this exists for ran for hours across ten replicas, and a
    /// replica that came up mid-incident would have waited out the threshold
    /// again before saying anything. It is read from `state_since_ms` now, so a
    /// fresh `Controller` reports a group that has been stuck for an hour
    /// immediately.
    #[test]
    fn a_stalled_rebalance_is_reported_once_per_episode() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "stalled-group";
        const REBALANCE_TIMEOUT_MS: i32 = 300_000;

        let storage = StorageContainer::builder()
            .cluster_id("test")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .storage(Url::parse("memory://")?);

        let threshold = Duration::from_millis(REBALANCE_TIMEOUT_MS as u64);
        let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

        // Members joined, a leader elected, no assignment distributed: exactly
        // what `ConsumerGroupState` maps to `CompletingRebalance`.
        let stalling = |since: SystemTime| GroupView {
            generation: GenerationDoc {
                leader: Some("member-1".into()),
                members: BTreeMap::from([("member-1".to_owned(), MemberRef::default())]),
                state_since_ms: epoch_ms(since),
                session_timeout_ms: SESSION_TIMEOUT_MS,
                rebalance_timeout_ms: Some(REBALANCE_TIMEOUT_MS),
                ..Default::default()
            },
            version: Some(Version::default()),
            assignment: None,
        };

        // The same group once the assignment lands.
        let progressed = |since: SystemTime| GroupView {
            assignment: Some(AssignmentDoc {
                generation_id: 0,
                leader: "member-1".into(),
                protocol_type: CONSUMER.into(),
                protocol_name: RANGE.into(),
                assignments: BTreeMap::from([(
                    "member-1".to_owned(),
                    Bytes::from_static(b"assignment"),
                )]),
                assigned_at_ms: 0,
            }),
            ..stalling(since)
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let controller = runtime.block_on(async {
            storage
                .build()
                .await
                .map_err(Error::from)
                .and_then(Controller::with_storage)
        })?;

        assert!(
            !controller.observe_rebalance(GROUP_ID, &stalling(t0), t0),
            "not on first sight"
        );
        assert!(
            !controller.observe_rebalance(
                GROUP_ID,
                &stalling(t0),
                t0 + threshold - Duration::from_secs(1)
            ),
            "not before the threshold",
        );

        assert!(
            controller.observe_rebalance(GROUP_ID, &stalling(t0), t0 + threshold),
            "reported once the threshold is reached",
        );
        assert!(
            !controller.observe_rebalance(GROUP_ID, &stalling(t0), t0 + threshold * 10),
            "and not again for the same episode",
        );

        // Recovery clears the episode ...
        assert!(!controller.observe_rebalance(GROUP_ID, &progressed(t0), t0 + threshold * 11));

        // ... so a fresh stall is a fresh episode, on its own persisted clock.
        let t1 = t0 + threshold * 12;
        assert!(
            !controller.observe_rebalance(GROUP_ID, &stalling(t1), t1),
            "new episode starts quiet"
        );
        assert!(
            controller.observe_rebalance(GROUP_ID, &stalling(t1), t1 + threshold),
            "and is reported in its own right",
        );

        // The restart case, which used to be a blind window: a `Controller`
        // that has never seen this group reports a group that has been stalled
        // for longer than the threshold on its first look at it.
        let restarted = Controller {
            rebalance_stalls: Arc::new(Mutex::new(ExpiringSizedCache::new(
                REBALANCE_STALL_REPORT_EVERY,
            ))),
            ..controller.clone()
        };

        assert!(
            restarted.observe_rebalance(GROUP_ID, &stalling(t0), t0 + threshold * 20),
            "a fresh Controller must report a group that was already stalled",
        );

        Ok(())
    }

    /// KIP-394: a first join with an empty member id replies with a generated
    /// id and must leave the group entirely alone — no phantom member, no
    /// generation bump, no object written at all.
    #[tokio::test(start_paused = true)]
    async fn a_minted_member_id_writes_nothing() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "member-id-required-group";

        let storage = counted_storage().await?;
        let generation_updates = storage.generation_updates_handle();
        let member_puts = storage.member_puts_handle();

        let controller = Controller::with_storage(storage.clone())?.with_now(paused_clock);
        let metadata = encode_subscription(&["t"], None);

        let (member_id, join) = register(&controller, GROUP_ID, &metadata).await?;
        assert_eq!(i16::from(ErrorCode::None), join.error_code);

        _ = sync_group(
            &controller,
            GROUP_ID,
            join.generation_id,
            &member_id,
            &[assignment_of(&member_id)],
        )
        .await?;

        let before = generation_version(&storage, GROUP_ID).await?;
        let updates = generation_updates.load(std::sync::atomic::Ordering::Relaxed);
        let puts = member_puts.load(std::sync::atomic::Ordering::Relaxed);

        let minted = join_group(&controller, GROUP_ID, "", &metadata).await?;

        assert_eq!(i16::from(ErrorCode::MemberIdRequired), minted.error_code);
        assert!(minted.member_id.starts_with(CLIENT_ID));
        assert_eq!(-1, minted.generation_id);
        assert!(minted.leader.is_empty());
        assert_eq!(Some(vec![]), minted.members);

        assert_eq!(
            before,
            generation_version(&storage, GROUP_ID).await?,
            "a MemberIdRequired reply must not rewrite the generation"
        );
        assert_eq!(
            updates,
            generation_updates.load(std::sync::atomic::Ordering::Relaxed),
            "a MemberIdRequired reply must not even attempt a generation write"
        );
        assert_eq!(
            puts,
            member_puts.load(std::sync::atomic::Ordering::Relaxed),
            "a MemberIdRequired reply must not write a member document"
        );

        // And the member it minted is not in the group.
        assert_eq!(
            BTreeSet::from([member_id]),
            generation_of(&storage, GROUP_ID)
                .await?
                .members
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
        );

        Ok(())
    }

    /// The no-op poll, end to end. A member re-joining or re-syncing a settled
    /// group must rewrite nothing: not the generation — whose etag is what the
    /// leader's assignment write used to lose races on — and not its own
    /// document, until its liveness is actually due.
    #[tokio::test(start_paused = true)]
    async fn a_noop_poll_writes_nothing_until_liveness_is_due() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "noop-resync-group";

        let storage = counted_storage().await?;
        let generation_updates = storage.generation_updates_handle();
        let member_puts = storage.member_puts_handle();

        let controller = Controller::with_storage(storage.clone())?.with_now(paused_clock);
        let metadata = encode_subscription(&["t"], None);

        let (member_id, join) = register(&controller, GROUP_ID, &metadata).await?;

        let sync = sync_group(
            &controller,
            GROUP_ID,
            join.generation_id,
            &member_id,
            &[assignment_of(&member_id)],
        )
        .await?;
        assert_eq!(i16::from(ErrorCode::None), sync.error_code);

        let version = generation_version(&storage, GROUP_ID).await?;
        let updates = generation_updates.load(std::sync::atomic::Ordering::Relaxed);
        let puts = member_puts.load(std::sync::atomic::Ordering::Relaxed);

        // A settled member polling: re-join, re-sync, heartbeat. None of it
        // changes anything about the group, and the member was last heard from
        // a moment ago.
        for _ in 0..4 {
            let join = join_group(&controller, GROUP_ID, &member_id, &metadata).await?;
            assert_eq!(i16::from(ErrorCode::None), join.error_code);

            let sync = sync_group(
                &controller,
                GROUP_ID,
                join.generation_id,
                &member_id,
                &[assignment_of(&member_id)],
            )
            .await?;
            assert_eq!(i16::from(ErrorCode::None), sync.error_code);

            let beat = heartbeat(&controller, GROUP_ID, join.generation_id, &member_id).await?;
            assert_eq!(i16::from(ErrorCode::None), beat.error_code);
        }

        assert_eq!(
            version,
            generation_version(&storage, GROUP_ID).await?,
            "a no-op poll must leave the generation's etag alone"
        );
        assert_eq!(
            updates,
            generation_updates.load(std::sync::atomic::Ordering::Relaxed),
            "a no-op poll must not write the generation"
        );
        assert_eq!(
            puts,
            member_puts.load(std::sync::atomic::Ordering::Relaxed),
            "a no-op poll must not renew liveness that is not due"
        );

        // Past half the session, one write — and one only, however many polls
        // follow it. That cadence is the whole of what the old once-a-second
        // `last_contact` churn was buying.
        advance(Duration::from_millis(SESSION_TIMEOUT_MS as u64 / 2 + 1)).await;

        let before = generation_of(&storage, GROUP_ID).await?;

        for _ in 0..4 {
            let beat = heartbeat(&controller, GROUP_ID, join.generation_id, &member_id).await?;
            assert_eq!(i16::from(ErrorCode::None), beat.error_code);
        }

        assert_eq!(
            puts + 1,
            member_puts.load(std::sync::atomic::Ordering::Relaxed),
            "liveness must be renewed once per session/2, not once per request"
        );

        // The sweep comes due on the same cadence, so the generation is written
        // — once, by the first of these four heartbeats, and by that heartbeat
        // on behalf of the *group* rather than the member. What must not change
        // is the composition.
        assert_eq!(
            updates + 1,
            generation_updates.load(std::sync::atomic::Ordering::Relaxed),
            "only the sweep may write the generation in steady state"
        );

        let after = generation_of(&storage, GROUP_ID).await?;

        assert_eq!(before.generation_id, after.generation_id);
        assert_eq!(before.members, after.members);
        assert_eq!(before.leader, after.leader);
        assert!(
            after.swept_at_ms > before.swept_at_ms,
            "the write must be the sweep's stamp"
        );

        Ok(())
    }

    /// Two `Controller`s sharing one store model two broker replicas behind a
    /// load balancer: every join and sync of the same group races the others.
    #[tokio::test(start_paused = true)]
    async fn a_multi_member_group_converges_across_replicas() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "convergence-group";
        const MEMBERS: usize = 8;

        let storage = memory_storage().await?;

        let replicas = [
            Controller::with_storage(storage.clone())?.with_now(paused_clock),
            Controller::with_storage(storage.clone())?.with_now(paused_clock),
        ];

        let handles = (0..MEMBERS)
            .map(|index| {
                let controller = replicas[index % replicas.len()].clone();
                tokio::spawn(async move { drive_member(controller, GROUP_ID, index).await })
            })
            .collect::<Vec<_>>();

        let mut assigned = BTreeMap::new();

        for handle in handles {
            let (member_id, assignment) = handle.await.expect("member task panicked")?;
            assert!(!assignment.is_empty());
            assert!(assigned.insert(member_id, assignment).is_none());
        }

        assert_eq!(MEMBERS, assigned.len());

        // The persisted layout: the generation names exactly these members, and
        // the assignment of that generation covers all of them.
        let generation = generation_of(&storage, GROUP_ID).await?;
        let member_ids = assigned.keys().cloned().collect::<BTreeSet<_>>();

        assert_eq!(
            member_ids,
            generation.members.keys().cloned().collect::<BTreeSet<_>>()
        );

        let assignment = storage
            .read_group_assignment(GROUP_ID, generation.generation_id)
            .await?
            .expect("the generation must have an assignment");

        assert_eq!(
            member_ids,
            assignment
                .assignments
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
        );

        for member_id in &member_ids {
            assert!(
                assignment
                    .assignments
                    .get(member_id)
                    .is_some_and(|assigned| !assigned.is_empty()),
                "no assignment for {member_id}"
            );
        }

        // And it has settled: reading twice gives the same generation at the
        // same version.
        let version = generation_version(&storage, GROUP_ID).await?;
        assert_eq!(generation, generation_of(&storage, GROUP_ID).await?);
        assert_eq!(version, generation_version(&storage, GROUP_ID).await?);

        Ok(())
    }

    /// A request answers from the store and from nothing else.
    ///
    /// This is what replaces the eviction tests that pinned the per-group
    /// `Wrapper` cache: there is no cache to evict, so the property worth
    /// pinning is the one that made the cache deletable. Two `Controller`s that
    /// have never seen this group answer a mid-lifecycle request identically to
    /// each other and to the one that built it, with no warm-up.
    #[tokio::test(start_paused = true)]
    async fn a_request_is_a_pure_function_of_the_store() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "pure-function-group";

        let storage = memory_storage().await?;
        let built = Controller::with_storage(storage.clone())?.with_now(paused_clock);
        let metadata = encode_subscription(&["t"], None);

        // Mid-lifecycle on purpose: members joined, a leader elected, no
        // assignment distributed. That is the state the old typestate had to
        // *maintain* across a cache miss, a GET-first read and a lost CAS.
        let (leader_id, _) = register(&built, GROUP_ID, &metadata).await?;
        let follower = encode_subscription(&["t", "u"], None);
        let (follower_id, _) = register(&built, GROUP_ID, &follower).await?;

        assert_eq!(
            ConsumerGroupState::CompletingRebalance,
            built.read_view(GROUP_ID).await?.state()
        );

        let generation_id = generation_of(&storage, GROUP_ID).await?.generation_id;

        let fresh = || {
            Controller::with_storage(storage.clone())
                .expect("controller")
                .with_now(paused_clock)
        };

        // The leader's join carries the whole membership, which is the answer
        // that used to come from the cached wrapper.
        let a = join_group(&fresh(), GROUP_ID, &leader_id, &metadata).await?;
        let b = join_group(&fresh(), GROUP_ID, &leader_id, &metadata).await?;
        let c = join_group(&built, GROUP_ID, &leader_id, &metadata).await?;

        assert_eq!(a, b, "two fresh controllers must answer identically");
        assert_eq!(a, c, "and identically to the one that built the group");
        assert_eq!(2, a.members.as_deref().unwrap_or_default().len());

        // As must a follower's sync, and a heartbeat.
        assert_eq!(
            sync_group(&fresh(), GROUP_ID, generation_id, &follower_id, &[]).await?,
            sync_group(&fresh(), GROUP_ID, generation_id, &follower_id, &[]).await?,
        );

        assert_eq!(
            heartbeat(&fresh(), GROUP_ID, generation_id, &follower_id).await?,
            heartbeat(&fresh(), GROUP_ID, generation_id, &follower_id).await?,
        );

        Ok(())
    }

    /// The whole arc, at the `Coordinator` level: a group forms, admits a
    /// second member, re-forms, commits, loses its leader and re-forms again.
    ///
    /// Deliberately written against what a client observes — error codes,
    /// generations, who leads, who is assigned — and not against the objects
    /// underneath. That is what makes it the test the decomposition had to pass
    /// *unchanged*: it says nothing about how the state is stored, so it is
    /// evidence that the storage changed and the protocol did not.
    #[tokio::test(start_paused = true)]
    async fn lifecycle() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "lifecycle-group";
        const TOPIC: &str = "t";

        let storage = memory_storage().await?;
        let controller = Controller::with_storage(storage.clone())?.with_now(paused_clock);

        let first_meta = encode_subscription(&[TOPIC], None);
        let second_meta = encode_subscription(&[TOPIC, "u"], None);

        // One member: it forms the group and leads it.
        let (first, join) = register(&controller, GROUP_ID, &first_meta).await?;

        assert_eq!(i16::from(ErrorCode::None), join.error_code);
        assert_eq!(0, join.generation_id, "a group is born at generation 0");
        assert_eq!(first, join.leader);
        assert_eq!(Some(CONSUMER.into()), join.protocol_type);
        assert_eq!(Some(RANGE.into()), join.protocol_name);
        assert_eq!(
            vec![first.clone()],
            join.members
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|member| member.member_id.clone())
                .collect::<Vec<_>>(),
            "the leader is handed the membership"
        );

        let sync = sync_group(&controller, GROUP_ID, 0, &first, &[assignment_of(&first)]).await?;
        assert_eq!(i16::from(ErrorCode::None), sync.error_code);
        assert!(!sync.assignment.is_empty());
        assert_eq!(
            ConsumerGroupState::Stable,
            controller.read_view(GROUP_ID).await?.state()
        );

        assert_eq!(
            i16::from(ErrorCode::None),
            heartbeat(&controller, GROUP_ID, 0, &first)
                .await?
                .error_code
        );

        // A second member admits itself, which is a rebalance.
        let (second, join) = register(&controller, GROUP_ID, &second_meta).await?;

        assert_eq!(i16::from(ErrorCode::None), join.error_code);
        assert_eq!(1, join.generation_id);
        assert_eq!(first, join.leader, "admitting a member does not re-elect");
        assert_eq!(
            Some(vec![]),
            join.members,
            "a follower is handed no membership"
        );

        assert_eq!(
            i16::from(ErrorCode::RebalanceInProgress),
            heartbeat(&controller, GROUP_ID, 0, &first)
                .await?
                .error_code,
            "the leader's generation is behind now",
        );

        // The leader re-joins, sees both members, and assigns over them.
        let join = join_group(&controller, GROUP_ID, &first, &first_meta).await?;

        assert_eq!(1, join.generation_id);
        assert_eq!(first, join.leader);
        assert_eq!(
            BTreeSet::from([first.clone(), second.clone()]),
            join.members
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|member| member.member_id.clone())
                .collect::<BTreeSet<_>>(),
        );

        let sync = sync_group(
            &controller,
            GROUP_ID,
            1,
            &first,
            &[assignment_of(&first), assignment_of(&second)],
        )
        .await?;
        assert_eq!(i16::from(ErrorCode::None), sync.error_code);

        let follower = sync_group(&controller, GROUP_ID, 1, &second, &[]).await?;
        assert_eq!(i16::from(ErrorCode::None), follower.error_code);
        assert_eq!(
            Bytes::from(format!("assignment-{second}")),
            follower.assignment,
            "a follower is handed the slice the leader computed for it",
        );

        for member_id in [&first, &second] {
            assert_eq!(
                i16::from(ErrorCode::None),
                heartbeat(&controller, GROUP_ID, 1, member_id)
                    .await?
                    .error_code
            );
        }

        // A member of the current generation commits.
        let topics = [OffsetCommitRequestTopic::default()
            .name(TOPIC.into())
            .partitions(Some(vec![
                OffsetCommitRequestPartition::default()
                    .partition_index(0)
                    .committed_offset(1)
                    .committed_leader_epoch(Some(0))
                    .commit_timestamp(None)
                    .committed_metadata(Some("".into())),
            ]))];

        let Body::OffsetCommitResponse(committed) = controller
            .offset_commit(OffsetCommit {
                group_id: GROUP_ID,
                generation_id_or_member_epoch: Some(1),
                member_id: Some(second.as_str()),
                group_instance_id: None,
                retention_time_ms: None,
                topics: Some(&topics[..]),
            })
            .await?
        else {
            panic!("expecting an offset commit response");
        };

        assert!(
            committed
                .topics
                .unwrap_or_default()
                .iter()
                .flat_map(|topic| topic.partitions.clone().unwrap_or_default())
                .all(|partition| partition.error_code != i16::from(ErrorCode::UnknownMemberId)),
            "a member of the current generation must not be fenced",
        );

        // The leader leaves.
        let leave = leave_group(&controller, GROUP_ID, &first).await?;
        assert_eq!(i16::from(ErrorCode::None), leave.error_code);
        assert_eq!(
            vec![i16::from(ErrorCode::None)],
            leave
                .members
                .unwrap_or_default()
                .iter()
                .map(|member| member.error_code)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            i16::from(ErrorCode::RebalanceInProgress),
            heartbeat(&controller, GROUP_ID, 1, &second)
                .await?
                .error_code,
        );

        // The survivor is promoted and re-forms the group on its own.
        let join = join_group(&controller, GROUP_ID, &second, &second_meta).await?;

        assert_eq!(i16::from(ErrorCode::None), join.error_code);
        assert_eq!(2, join.generation_id);
        assert_eq!(second, join.leader, "the survivor is promoted");
        assert_eq!(
            vec![second.clone()],
            join.members
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|member| member.member_id.clone())
                .collect::<Vec<_>>(),
        );

        let sync = sync_group(&controller, GROUP_ID, 2, &second, &[assignment_of(&second)]).await?;
        assert_eq!(i16::from(ErrorCode::None), sync.error_code);
        assert_eq!(
            ConsumerGroupState::Stable,
            controller.read_view(GROUP_ID).await?.state()
        );

        Ok(())
    }

    /// A generation is never reused — including across the group emptying.
    ///
    /// `assignment/{generation}` is create-only and immutable, so a reused
    /// generation would adopt a dead generation's assignment: the members of
    /// the new generation would be handed partitions computed for a set of
    /// members that no longer exists, and the leader's create would silently
    /// find the object already there and succeed.
    #[tokio::test(start_paused = true)]
    async fn a_generation_is_never_reused() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "monotonic-generation";

        let storage = memory_storage().await?;
        let controller = Controller::with_storage(storage.clone())?.with_now(paused_clock);
        let metadata = encode_subscription(&["t"], None);

        let mut seen = Vec::new();

        for _ in 0..3 {
            let (member_id, join) = register(&controller, GROUP_ID, &metadata).await?;
            seen.push(join.generation_id);

            _ = sync_group(
                &controller,
                GROUP_ID,
                join.generation_id,
                &member_id,
                &[assignment_of(&member_id)],
            )
            .await?;

            let leave = leave_group(&controller, GROUP_ID, &member_id).await?;
            assert_eq!(i16::from(ErrorCode::None), leave.error_code);

            let generation = generation_of(&storage, GROUP_ID).await?;
            assert!(generation.members.is_empty(), "the group must be empty");
            assert_eq!(None, generation.leader, "and have no leader");

            seen.push(generation.generation_id);
        }

        assert!(
            seen.windows(2).all(|pair| pair[0] < pair[1]),
            "generations must strictly increase across the group emptying: {seen:?}",
        );

        Ok(())
    }

    /// #240: a leader that leaves must not stay behind as a phantom.
    ///
    /// Left in place, `leader = Some(departed)` freezes the group: `join` never
    /// promotes anyone else, every follower sync is refused as not-leader, no
    /// write ever happens, and there is nothing to evict. Measured in
    /// production as a 16-member group held in `CompletingRebalance` for hours
    /// with zero broker log lines.
    #[tokio::test(start_paused = true)]
    async fn a_departed_leader_is_replaced_by_the_next_to_join() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "leader-leaves";

        let storage = memory_storage().await?;
        let controller = Controller::with_storage(storage.clone())?.with_now(paused_clock);

        let leader_meta = encode_subscription(&["t"], None);
        let follower_meta = encode_subscription(&["t", "u"], None);

        let (leader_id, _) = register(&controller, GROUP_ID, &leader_meta).await?;
        let (follower_id, join) = register(&controller, GROUP_ID, &follower_meta).await?;

        assert_eq!(leader_id, join.leader);
        assert_eq!(2, generation_of(&storage, GROUP_ID).await?.members.len());

        _ = leave_group(&controller, GROUP_ID, &leader_id).await?;

        let generation = generation_of(&storage, GROUP_ID).await?;
        assert_eq!(
            BTreeSet::from([follower_id.clone()]),
            generation.members.keys().cloned().collect::<BTreeSet<_>>()
        );
        assert_eq!(
            None, generation.leader,
            "a departed leader must not remain elected"
        );

        // The next join promotes the survivor ...
        let join = join_group(&controller, GROUP_ID, &follower_id, &follower_meta).await?;
        assert_eq!(i16::from(ErrorCode::None), join.error_code);
        assert_eq!(follower_id, join.leader);

        // ... and its assignment forms the group.
        let sync = sync_group(
            &controller,
            GROUP_ID,
            join.generation_id,
            &follower_id,
            &[assignment_of(&follower_id)],
        )
        .await?;

        assert_eq!(i16::from(ErrorCode::None), sync.error_code);
        assert!(!controller.read_view(GROUP_ID).await?.is_forming());

        Ok(())
    }

    /// #240: a persisted group can already carry a leader that is not a member
    /// — written before the leave fix-up existed, or by a replica still running
    /// without it. A join must heal it rather than keep answering "the leader
    /// is someone else" to every live member forever.
    #[tokio::test(start_paused = true)]
    async fn a_join_heals_an_orphaned_leader() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "orphaned-leader-group";

        let storage = memory_storage().await?;
        let controller = Controller::with_storage(storage.clone())?.with_now(paused_clock);
        let metadata = encode_subscription(&["t"], None);

        let (member_id, _) = register(&controller, GROUP_ID, &metadata).await?;

        // Orphan the leader behind the coordinator's back, which is the shape a
        // replica running the old code could leave behind.
        let (generation, version) = storage
            .read_group_generation(GROUP_ID)
            .await?
            .expect("generation");

        _ = storage
            .update_group_generation(
                GROUP_ID,
                GenerationDoc {
                    leader: Some(String::from("a-leader-that-left")),
                    ..generation
                },
                Some(version),
            )
            .await
            .map_err(update_error)?;

        let join = join_group(&controller, GROUP_ID, &member_id, &metadata).await?;

        assert_eq!(i16::from(ErrorCode::None), join.error_code);
        assert_eq!(
            member_id, join.leader,
            "the live member must be promoted over the orphan"
        );
        assert_eq!(
            Some(member_id),
            generation_of(&storage, GROUP_ID).await?.leader
        );

        Ok(())
    }

    /// A member of the current generation that the assignment does not cover
    /// must be told to re-join, on both sides of the write: answering
    /// `error=None` with an empty assignment reads to the client as a valid
    /// empty assignment, and it then sits idle forever.
    #[tokio::test(start_paused = true)]
    async fn a_sync_that_misses_the_syncing_member_is_a_rebalance() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "missing-assignment-group";

        let storage = memory_storage().await?;
        let controller = Controller::with_storage(storage.clone())?.with_now(paused_clock);
        let metadata = encode_subscription(&["t"], None);

        let (leader_id, _) = register(&controller, GROUP_ID, &metadata).await?;
        let follower_meta = encode_subscription(&["t", "u"], None);
        let (follower_id, join) = register(&controller, GROUP_ID, &follower_meta).await?;

        assert_eq!(leader_id, join.leader);
        let generation_id = join.generation_id;

        // The leader's assignment covers the follower but not itself: the group
        // must not form on it.
        let sync = sync_group(
            &controller,
            GROUP_ID,
            generation_id,
            &leader_id,
            &[assignment_of(&follower_id)],
        )
        .await?;

        assert_eq!(i16::from(ErrorCode::RebalanceInProgress), sync.error_code);
        assert!(sync.assignment.is_empty());
        assert_eq!(
            None,
            storage
                .read_group_assignment(GROUP_ID, generation_id)
                .await?,
            "a refused sync must not have written an assignment"
        );

        // Now it covers itself but not the follower, so the group forms — and
        // the follower is sent back to re-join rather than parked on nothing.
        let sync = sync_group(
            &controller,
            GROUP_ID,
            generation_id,
            &leader_id,
            &[assignment_of(&leader_id)],
        )
        .await?;
        assert_eq!(i16::from(ErrorCode::None), sync.error_code);

        let sync = sync_group(&controller, GROUP_ID, generation_id, &follower_id, &[]).await?;

        assert_eq!(i16::from(ErrorCode::RebalanceInProgress), sync.error_code);
        assert!(sync.assignment.is_empty());
        assert_eq!(
            generation_id,
            generation_of(&storage, GROUP_ID).await?.generation_id,
            "a refused sync must not move the generation"
        );

        Ok(())
    }

    /// The leader's assignment is create-only, so a leader retrying its own
    /// sync — which is what a lost response or a re-drive looks like — must
    /// adopt what is stored rather than fail or overwrite it.
    #[tokio::test(start_paused = true)]
    async fn a_leader_retrying_its_sync_adopts_what_is_stored() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "retried-sync";

        let storage = memory_storage().await?;
        let controller = Controller::with_storage(storage.clone())?.with_now(paused_clock);
        let metadata = encode_subscription(&["t"], None);

        let (member_id, join) = register(&controller, GROUP_ID, &metadata).await?;

        let first = sync_group(
            &controller,
            GROUP_ID,
            join.generation_id,
            &member_id,
            &[assignment_of(&member_id)],
        )
        .await?;
        assert_eq!(i16::from(ErrorCode::None), first.error_code);

        // The same leader, same generation, a *different* assignment. The
        // stored one wins: it is what every other member has already been
        // handed, and two members holding assignments from different rounds of
        // the same generation is the overlap the protocol exists to prevent.
        let retried = sync_group(
            &controller,
            GROUP_ID,
            join.generation_id,
            &member_id,
            &[SyncGroupRequestAssignment::default()
                .member_id(member_id.clone())
                .assignment(Bytes::from_static(b"a-different-assignment"))],
        )
        .await?;

        assert_eq!(i16::from(ErrorCode::None), retried.error_code);
        assert_eq!(first.assignment, retried.assignment);

        Ok(())
    }

    /// The dead-member sweep, which replaces `missed_heartbeat`: a member that
    /// stops making requests is evicted, the generation moves on, and its
    /// document is reclaimed.
    ///
    /// Seeded rather than driven, so the only clock in the test is the one the
    /// sweep reads. Driving it through joins would make the verdict depend on
    /// how long a follower's long-poll happened to hold, which is a property of
    /// the join window and not of the sweep.
    #[tokio::test(start_paused = true)]
    async fn a_lapsed_member_is_swept_and_the_generation_moves_on() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "swept-group";
        const LEADER: &str = "m-1";
        const LAPSED: &str = "m-2";

        let storage = memory_storage().await?;
        let controller = Controller::with_storage(storage.clone())?.with_now(paused_clock);

        let now_ms = epoch_ms(paused_clock());
        let session = i64::from(SESSION_TIMEOUT_MS);

        let member = |member_id: &str, last_contact_ms: i64| MemberDoc {
            last_contact_ms,
            session_timeout_ms: SESSION_TIMEOUT_MS,
            rebalance_timeout_ms: REBALANCE_TIMEOUT_MS,
            join_response: JoinGroupResponseMember::default()
                .member_id(member_id.to_owned())
                .metadata(encode_subscription(&["t"], None)),
            ..Default::default()
        };

        for (member_id, last_contact_ms) in [(LEADER, now_ms), (LAPSED, now_ms - session - 1)] {
            _ = storage
                .write_group_member(
                    GROUP_ID,
                    member_id,
                    member(member_id, last_contact_ms),
                    None,
                )
                .await
                .map_err(update_error)?;
        }

        // Swept a full session ago, so the next request runs one.
        _ = storage
            .update_group_generation(
                GROUP_ID,
                GenerationDoc {
                    generation_id: 7,
                    protocol_type: Some(CONSUMER.into()),
                    protocol_name: Some(RANGE.into()),
                    leader: Some(LEADER.into()),
                    members: BTreeMap::from([
                        (LEADER.to_owned(), MemberRef::default()),
                        (LAPSED.to_owned(), MemberRef::default()),
                    ]),
                    members_changed_at_ms: now_ms - session,
                    state_since_ms: now_ms - session,
                    swept_at_ms: now_ms - session,
                    session_timeout_ms: SESSION_TIMEOUT_MS,
                    rebalance_timeout_ms: REBALANCE_TIMEOUT_MS,
                    inception_ms: now_ms - session,
                    ..Default::default()
                },
                None,
            )
            .await
            .map_err(update_error)?;

        let beat = heartbeat(&controller, GROUP_ID, 7, LEADER).await?;

        // The sweep evicted the lapsed member before this heartbeat was
        // answered, so the surviving member's own generation is now behind.
        assert_eq!(i16::from(ErrorCode::RebalanceInProgress), beat.error_code);

        let generation = generation_of(&storage, GROUP_ID).await?;

        assert_eq!(
            BTreeSet::from([LEADER.to_owned()]),
            generation.members.keys().cloned().collect::<BTreeSet<_>>(),
            "the lapsed member must be evicted"
        );
        assert!(
            generation.generation_id > 7,
            "an eviction is a rebalance, so the generation must move on"
        );
        assert_eq!(
            None, generation.leader,
            "the leader is re-elected by the first member to join"
        );
        assert_eq!(
            None,
            storage.read_group_member(GROUP_ID, LAPSED).await?,
            "the evicted member's document must be reclaimed"
        );
        assert!(
            storage.read_group_member(GROUP_ID, LEADER).await?.is_some(),
            "a live member's document must be left alone"
        );

        // The survivor rejoins and takes the group on.
        let join = join_group(
            &controller,
            GROUP_ID,
            LEADER,
            &encode_subscription(&["t"], None),
        )
        .await?;

        assert_eq!(i16::from(ErrorCode::None), join.error_code);
        assert_eq!(LEADER, join.leader);

        Ok(())
    }

    /// A member the generation names but whose document has gone is a lapsed
    /// session, not a member that has yet to arrive: the document is written
    /// *before* the generation admits the member, so its absence can only mean
    /// it was reclaimed.
    #[tokio::test(start_paused = true)]
    async fn a_member_with_no_document_is_swept() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "orphan-membership";

        let storage = memory_storage().await?;
        let controller = Controller::with_storage(storage.clone())?.with_now(paused_clock);
        let metadata = encode_subscription(&["t"], None);

        let (member_id, join) = register(&controller, GROUP_ID, &metadata).await?;

        _ = sync_group(
            &controller,
            GROUP_ID,
            join.generation_id,
            &member_id,
            &[assignment_of(&member_id)],
        )
        .await?;

        // Behind the coordinator's back, as a partial `delete_groups` or a
        // half-finished leave would leave it.
        storage.delete_group_member(GROUP_ID, &member_id).await?;

        let (generation, version) = storage
            .read_group_generation(GROUP_ID)
            .await?
            .expect("generation");

        _ = storage
            .update_group_generation(
                GROUP_ID,
                GenerationDoc {
                    swept_at_ms: generation.swept_at_ms - i64::from(SESSION_TIMEOUT_MS),
                    ..generation
                },
                Some(version),
            )
            .await
            .map_err(update_error)?;

        let beat = heartbeat(&controller, GROUP_ID, join.generation_id, &member_id).await?;

        assert_eq!(i16::from(ErrorCode::UnknownMemberId), beat.error_code);
        assert!(
            generation_of(&storage, GROUP_ID).await?.members.is_empty(),
            "a membership with no document must be reclaimed"
        );

        Ok(())
    }

    /// The sweep costs one write per group per session/2 across the fleet, not
    /// one per replica: its verdict is a pure function of the documents and the
    /// clock, and `swept_at_ms` is what stops every replica repeating it.
    #[tokio::test(start_paused = true)]
    async fn a_sweep_is_deduped_across_replicas() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "deduped-sweep";
        const REPLICAS: usize = 4;

        let storage = counted_storage().await?;
        let generation_updates = storage.generation_updates_handle();

        let replicas = (0..REPLICAS)
            .map(|_| {
                Controller::with_storage(storage.clone())
                    .expect("controller")
                    .with_now(paused_clock)
            })
            .collect::<Vec<_>>();

        let metadata = encode_subscription(&["t"], None);
        let (member_id, join) = register(&replicas[0], GROUP_ID, &metadata).await?;

        _ = sync_group(
            &replicas[0],
            GROUP_ID,
            join.generation_id,
            &member_id,
            &[assignment_of(&member_id)],
        )
        .await?;

        // Past the sweep interval, with nobody to evict: the group's own stamp
        // is what makes this one write rather than four.
        advance(Duration::from_millis(SESSION_TIMEOUT_MS as u64 / 2 + 1)).await;

        let before = generation_updates.load(std::sync::atomic::Ordering::Relaxed);

        for replica in &replicas {
            let beat = heartbeat(replica, GROUP_ID, join.generation_id, &member_id).await?;
            assert_eq!(i16::from(ErrorCode::None), beat.error_code);
        }

        assert_eq!(
            before + 1,
            generation_updates.load(std::sync::atomic::Ordering::Relaxed),
            "{REPLICAS} replicas serving one group must cost one sweep between them",
        );

        Ok(())
    }

    /// Kafka's fencing rules on the commit path (#359), which this broker did
    /// not apply at all: a commit claiming a generation the group has left, or
    /// naming a member the group does not know, is a zombie and must be
    /// refused. **Behaviour change** — a commit that used to be accepted now
    /// fails.
    #[tokio::test(start_paused = true)]
    async fn a_zombie_commit_is_fenced() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "fenced-commits";
        const TOPIC: &str = "t";

        let storage = memory_storage().await?;
        let controller = Controller::with_storage(storage.clone())?.with_now(paused_clock);
        let metadata = encode_subscription(&[TOPIC], None);

        let (member_id, join) = register(&controller, GROUP_ID, &metadata).await?;

        let commit = |generation_id: Option<i32>, member_id: Option<String>| {
            let controller = controller.clone();

            async move {
                let topics = [OffsetCommitRequestTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(vec![
                        OffsetCommitRequestPartition::default()
                            .partition_index(0)
                            .committed_offset(1)
                            .committed_leader_epoch(Some(0))
                            .commit_timestamp(None)
                            .committed_metadata(Some("".into())),
                    ]))];

                let body = controller
                    .offset_commit(OffsetCommit {
                        group_id: GROUP_ID,
                        generation_id_or_member_epoch: generation_id,
                        member_id: member_id.as_deref(),
                        group_instance_id: None,
                        retention_time_ms: None,
                        topics: Some(&topics[..]),
                    })
                    .await?;

                let Body::OffsetCommitResponse(response) = body else {
                    panic!("{body:?}");
                };

                Ok::<_, Error>(
                    response
                        .topics
                        .and_then(|topics| {
                            topics.first().and_then(|topic| {
                                topic
                                    .partitions
                                    .as_ref()
                                    .and_then(|partitions| partitions.first().cloned())
                            })
                        })
                        .map(|partition| partition.error_code)
                        .expect("a partition response"),
                )
            }
        };

        // A member of the current generation commits.
        assert_ne!(
            i16::from(ErrorCode::UnknownMemberId),
            commit(Some(join.generation_id), Some(member_id.clone())).await?,
        );

        // A generation the group has left.
        assert_eq!(
            i16::from(ErrorCode::IllegalGeneration),
            commit(Some(join.generation_id + 1), Some(member_id.clone())).await?,
            "a commit from a stale generation must be fenced",
        );

        // A member the group does not know.
        assert_eq!(
            i16::from(ErrorCode::UnknownMemberId),
            commit(Some(join.generation_id), Some(String::from("a-stranger"))).await?,
            "a commit from an unknown member must be fenced",
        );

        // The simple consumer, which manages its own offsets: Kafka's own
        // escape, and the reason this cannot simply reject everything it does
        // not recognise.
        assert_ne!(
            i16::from(ErrorCode::UnknownMemberId),
            commit(Some(-1), None).await?,
            "a simple consumer's commit must not be fenced",
        );

        // ... and it must read nothing about a group that does not exist.
        assert_ne!(
            i16::from(ErrorCode::UnknownMemberId),
            commit(Some(-1), None).await?,
        );

        Ok(())
    }

    /// #361: a long poll cut short by a shutdown must *answer*.
    ///
    /// `join` holds a member for up to half its session timeout waiting for
    /// something to act on. At a fixed replica count that only ever happened on
    /// deploy; under an autoscaler taking a replica away it is routine. A poll
    /// abandoned mid-flight is a closed socket to the client, which is
    /// indistinguishable from a network fault and costs a reconnect plus a
    /// coordinator round trip — the same neighbourhood as #289 and #300: a
    /// retriable condition must be answered, not dropped.
    ///
    /// What it answers with is what it would have answered at its own deadline,
    /// which is the point: no new client behaviour, and no generation bump, so
    /// a scale-in event causes no rebalance.
    #[tokio::test(start_paused = true)]
    async fn a_join_long_poll_answers_when_this_replica_is_stopping() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "draining-join";

        let storage = memory_storage().await?;
        let cancellation = CancellationToken::new();
        let controller = Controller::with_storage(storage.clone())?
            .with_now(paused_clock)
            .with_cancellation(cancellation.clone());

        let metadata = encode_subscription(&["t"], None);

        // A leader, so the group exists and the member below is a follower with
        // nothing to act on — the shape that long-polls.
        let (leader_id, _) = register(&controller, GROUP_ID, &metadata).await?;

        let generation_id = generation_of(&storage, GROUP_ID).await?.generation_id;

        let minted = join_group(&controller, GROUP_ID, "", &metadata).await?;
        assert_eq!(i16::from(ErrorCode::MemberIdRequired), minted.error_code);

        let follower = {
            let controller = controller.clone();
            let metadata = metadata.clone();
            let member_id = minted.member_id.clone();

            tokio::spawn(
                async move { join_group(&controller, GROUP_ID, &member_id, &metadata).await },
            )
        };

        // Let the follower reach its wait: it has joined, it is not the leader,
        // and no assignment exists, so `join_long_poll` holds it.
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }

        assert!(!follower.is_finished(), "the follower must be long-polling");

        cancellation.cancel();

        let answered = timeout(Duration::from_secs(5), follower)
            .await
            .expect("a cancelled long poll must answer rather than hang")
            .expect("the join task must not panic")?;

        assert_eq!(
            i16::from(ErrorCode::None),
            answered.error_code,
            "a drained poll answers what it had, not an error the client has to interpret",
        );
        assert_eq!(leader_id, answered.leader);

        // And it moved nothing: the group is at the generation the follower's
        // own join minted, and the shutdown added no rebalance of its own.
        assert_eq!(
            generation_id + 1,
            generation_of(&storage, GROUP_ID).await?.generation_id,
            "a drain must not cost a rebalance",
        );

        Ok(())
    }

    /// The same for `sync`, which waits longer — eight tenths of the session
    /// timeout — because it is waiting on the leader's single assignment write.
    ///
    /// A member with no assignment is told to rebalance, which is what it is
    /// told at its own deadline: the client re-joins, against a replica that is
    /// staying.
    #[tokio::test(start_paused = true)]
    async fn a_sync_long_poll_answers_when_this_replica_is_stopping() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "draining-sync";

        let storage = memory_storage().await?;
        let cancellation = CancellationToken::new();
        let controller = Controller::with_storage(storage.clone())?
            .with_now(paused_clock)
            .with_cancellation(cancellation.clone());

        let leader_meta = encode_subscription(&["t"], None);
        let follower_meta = encode_subscription(&["t", "u"], None);

        let (leader_id, _) = register(&controller, GROUP_ID, &leader_meta).await?;
        let (follower_id, join) = register(&controller, GROUP_ID, &follower_meta).await?;

        // The leader assigns itself and nobody else, so the follower is a
        // member of a `Stable` generation with no slice of its own: the state
        // `sync_long_poll` waits in rather than answering.
        let sync = sync_group(
            &controller,
            GROUP_ID,
            join.generation_id,
            &leader_id,
            &[assignment_of(&leader_id)],
        )
        .await?;
        assert_eq!(i16::from(ErrorCode::None), sync.error_code);

        let follower = {
            let controller = controller.clone();
            let member_id = follower_id.clone();
            let generation_id = join.generation_id;

            tokio::spawn(async move {
                sync_group(&controller, GROUP_ID, generation_id, &member_id, &[]).await
            })
        };

        for _ in 0..4 {
            tokio::task::yield_now().await;
        }

        assert!(!follower.is_finished(), "the follower must be long-polling");

        cancellation.cancel();

        let answered = timeout(Duration::from_secs(5), follower)
            .await
            .expect("a cancelled long poll must answer rather than hang")
            .expect("the sync task must not panic")?;

        assert_eq!(
            i16::from(ErrorCode::RebalanceInProgress),
            answered.error_code,
            "an unassigned sync is sent to re-join, as it is at its own deadline",
        );

        Ok(())
    }

    /// A `Controller` built without a token long-polls to its own deadline, so
    /// nothing that does not opt in changes behaviour.
    #[tokio::test(start_paused = true)]
    async fn a_controller_with_no_token_polls_to_its_deadline() -> Result<()> {
        let _guard = init_tracing()?;

        const GROUP_ID: &str = "no-token";

        let storage = memory_storage().await?;
        let controller = Controller::with_storage(storage)?.with_now(paused_clock);
        let metadata = encode_subscription(&["t"], None);

        let (_, _) = register(&controller, GROUP_ID, &metadata).await?;

        let minted = join_group(&controller, GROUP_ID, "", &metadata).await?;

        // Half the session timeout, which is what `join_long_poll` allows a
        // member with nothing to act on. Under the paused clock the sleeps are
        // free, so this is a bound on virtual time rather than on the run.
        let answered = join_group(&controller, GROUP_ID, &minted.member_id, &metadata).await?;

        assert_eq!(i16::from(ErrorCode::None), answered.error_code);

        Ok(())
    }

    /// #360: a `Controller` holds nothing keyed by a group id that a client can
    /// grow without bound, and nothing that has to be swept.
    ///
    /// This replaces the #283 eviction suite. Those tests pinned four maps and
    /// the periodic sweep that emptied them — state a replica had to be kept
    /// alive to maintain, and the reason a replica was not interchangeable. The
    /// property worth pinning now is that churning uniquely-named groups leaves
    /// nothing behind at all.
    #[tokio::test(start_paused = true)]
    async fn a_controller_holds_no_per_group_state() -> Result<()> {
        let _guard = init_tracing()?;

        const ROUNDS: usize = 32;

        let storage = memory_storage().await?;
        let controller = Controller::with_storage(storage)?.with_now(paused_clock);
        let metadata = encode_subscription(&["t"], None);

        for round in 0..ROUNDS {
            // A fresh name every round: a same-named re-join would reuse
            // whatever was held rather than add to it, so it would not show
            // growth.
            let group_id = format!("churn-{round}");

            let (member_id, join) = register(&controller, &group_id, &metadata).await?;

            _ = sync_group(
                &controller,
                &group_id,
                join.generation_id,
                &member_id,
                &[assignment_of(&member_id)],
            )
            .await?;

            assert_eq!(
                i16::from(ErrorCode::None),
                heartbeat(&controller, &group_id, join.generation_id, &member_id)
                    .await?
                    .error_code
            );

            _ = leave_group(&controller, &group_id, &member_id).await?;

            // The stall memo is the only map, and only a group observed
            // mid-rebalance past its threshold ever enters it. None of these
            // did.
            assert_eq!(
                0,
                controller.rebalance_stalls.lock().unwrap().len(),
                "round {round}",
            );
        }

        Ok(())
    }
}
