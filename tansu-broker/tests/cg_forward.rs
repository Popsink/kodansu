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

//! Convergence proof for forward-to-owner group coordination.
//!
//! The production topology, reproduced in-process: N stateless broker
//! replicas behind one load balancer share a single object store, so a
//! multi-member group's Join/Sync long-polls scatter across replicas and
//! thrash the one `{group}.json` object's etag CAS. The join-window barrier
//! converged small groups on few replicas but not 16 members on 10 replicas
//! in production. The structural fix routes every group API for a `group_id`
//! to its one rendezvous owner replica — the single-`Controller` regime the
//! `consumer_next_action_16c` test already proves out.
//!
//! The rig: 10 `Controller`s over ONE shared `memory://` store (one CAS
//! namespace), each behind its own per-replica store latency (deterministic
//! seeds — the object-store round trip is what makes the scatter thrash);
//! 10 fake pod IPs; a [`Forward`] shim dispatching a forwarded group API
//! straight into the owner's `Controller` (the in-process stand-in for the
//! internal-listener hop — what the production listener does after decoding
//! the frame, minus the socket). 16 consumers run the full KIP-394 dance,
//! each entering via a rotating ingress replica, exactly like production's
//! per-connection LB scatter.
//!
//! Proof and control share every constant: WITH forwarding the group must
//! fully converge; WITHOUT it (the pre-fix path, bare `Controller`s) the
//! same consumers must fail to converge inside the same bounded window.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, Ipv4Addr},
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use tansu_broker::{
    NODE_ID,
    coordinator::group::{
        Coordinator,
        administrator::Controller,
        forward::{Forward, ForwardingCoordinator, PeerRegistry, owner},
    },
};
use tansu_sans_io::{
    Body, ErrorCode, join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember, sync_group_request::SyncGroupRequestAssignment,
};
use tansu_storage::{GroupState, LatencyIntroducingStorage, Storage, StorageContainer};
use tokio::{task::JoinSet, time::timeout};
use tracing::debug;
use url::Url;

use crate::common::init_tracing;

pub mod common;

const GROUP_ID: &str = "g";
const PROTOCOL_TYPE: &str = "consumer";
const RANGE: &str = "range";

/// Production replica count: the deployment this reproduces runs 10
/// stateless brokers behind one load balancer.
const REPLICAS: usize = 10;

/// Production group size: the 16-member group that never converged.
const MEMBERS: usize = 16;

/// Partitions of the (single) subscribed topic; the leader's range
/// assignment must end up covering exactly these, uniquely.
const PARTITIONS: i32 = 32;

const SESSION_TIMEOUT_MS: i32 = 45_000;
const REBALANCE_TIMEOUT_MS: Option<i32> = Some(60_000);

/// Per-storage-operation latency injected under every replica, with a
/// deterministic per-replica seed: the object-store round trip is the
/// ingredient that turns scattered CAS writers into a thrashing herd, so
/// both the proof and the control run over it.
const STORE_LATENCY_MS: Range<u64> = 50..150;

/// Upper bound on join/sync rounds per member: with forwarding a member
/// needs ~3 (KIP-394 empty join, real join, sync); the bound keeps the
/// non-converging control from looping forever.
const MAX_ROUNDS: usize = 32;

/// Per-member deadline for the forwarded (must-converge) arrangement. Only
/// reached on failure — a converged member returns immediately — so it is
/// deliberately generous: convergence takes ~25s here, but a loaded CI
/// worker can double that.
const FORWARDED_DEADLINE: Duration = Duration::from_secs(120);

/// The bounded window inside which the scattered (control) arrangement must
/// fail to converge. Deliberately larger than the forwarded arrangement's
/// observed convergence time (~25s): given strictly more time than
/// forwarding needs, the pre-fix path still cannot form the group. Unlike
/// [`FORWARDED_DEADLINE`] this bound is always paid in wall clock — the
/// unconverging members run it out.
const SCATTERED_WINDOW: Duration = Duration::from_secs(60);

/// One shared store = one CAS namespace, exactly like N replicas writing the
/// same S3 bucket.
type SharedStorage = Arc<Box<dyn Storage>>;
type ReplicaStorage = LatencyIntroducingStorage<SharedStorage>;
type Replica = Controller<ReplicaStorage>;

/// The fake pod IP of replica `index`.
fn peer(index: usize) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(
        10,
        0,
        0,
        u8::try_from(index + 1).expect("replica index fits an IPv4 octet"),
    ))
}

async fn shared_storage() -> Result<SharedStorage> {
    StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .storage(Url::parse("memory://")?)
        .build()
        .await
        .map_err(Into::into)
}

/// The 10 replicas: independent `Controller`s (each with its own in-memory
/// wrapper cache, like a real pod) over the one shared store, each behind
/// deterministically-seeded store latency.
fn replicas(shared: &SharedStorage) -> Result<Vec<Replica>> {
    (0..REPLICAS)
        .map(|index| {
            Controller::with_storage(
                LatencyIntroducingStorage::new(shared.clone())
                    .with_seed(index as u64)
                    .with_latency(STORE_LATENCY_MS),
            )
            .map_err(Into::into)
        })
        .collect()
}

/// The in-process stand-in for the internal-listener hop: given the owner's
/// address, dispatch the forwarded group API straight into that replica's
/// local `Controller` — what the production 9093 listener does after
/// decoding the frame, minus the socket. Records which owners were dialled
/// so the test can assert the rendezvous routed ALL of the group's traffic
/// to ONE replica.
#[derive(Debug)]
struct InProcessForward {
    replicas: BTreeMap<IpAddr, Replica>,
    dialed: Mutex<BTreeSet<IpAddr>>,
    calls: AtomicU64,
}

impl InProcessForward {
    fn new(replicas: BTreeMap<IpAddr, Replica>) -> Arc<Self> {
        Arc::new(Self {
            replicas,
            dialed: Mutex::new(BTreeSet::new()),
            calls: AtomicU64::new(0),
        })
    }

    fn dialed(&self) -> BTreeSet<IpAddr> {
        self.dialed.lock().expect("dialed").clone()
    }

    fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Forward for InProcessForward {
    async fn call(
        &self,
        owner: IpAddr,
        _api_key: i16,
        client_id: Option<&str>,
        body: Body,
    ) -> tansu_broker::Result<Body> {
        _ = self.calls.fetch_add(1, Ordering::Relaxed);
        _ = self.dialed.lock().expect("dialed").insert(owner);

        let replica = self
            .replicas
            .get(&owner)
            .ok_or_else(|| tansu_broker::Error::Message(format!("no replica at {owner}")))?;

        match body {
            Body::JoinGroupRequest(join) => {
                replica
                    .join(
                        client_id,
                        &join.group_id,
                        join.session_timeout_ms,
                        join.rebalance_timeout_ms,
                        &join.member_id,
                        join.group_instance_id.as_deref(),
                        &join.protocol_type,
                        join.protocols.as_deref(),
                        join.reason.as_deref(),
                    )
                    .await
            }

            Body::SyncGroupRequest(sync) => {
                replica
                    .sync(
                        &sync.group_id,
                        sync.generation_id,
                        &sync.member_id,
                        sync.group_instance_id.as_deref(),
                        sync.protocol_type.as_deref(),
                        sync.protocol_name.as_deref(),
                        sync.assignments.as_deref(),
                    )
                    .await
            }

            Body::HeartbeatRequest(heartbeat) => {
                replica
                    .heartbeat(
                        &heartbeat.group_id,
                        heartbeat.generation_id,
                        &heartbeat.member_id,
                        heartbeat.group_instance_id.as_deref(),
                    )
                    .await
            }

            Body::LeaveGroupRequest(leave) => {
                replica
                    .leave(
                        &leave.group_id,
                        leave.member_id.as_deref(),
                        leave.members.as_deref(),
                    )
                    .await
            }

            otherwise => Err(tansu_broker::Error::Message(format!(
                "unexpected forwarded request: {otherwise:?}"
            ))),
        }
    }
}

/// The leader's range assignment over the member list its join returned:
/// members sorted by id, partitions `0..PARTITIONS` split into contiguous
/// runs, each member's partitions encoded as a comma-separated list so the
/// coverage assertions can decode them back.
fn range_assignments(members: &[JoinGroupResponseMember]) -> Vec<SyncGroupRequestAssignment> {
    let member_ids = members
        .iter()
        .map(|member| member.member_id.clone())
        .collect::<BTreeSet<_>>();

    let count = member_ids.len().max(1);
    let partitions = usize::try_from(PARTITIONS).expect("partition count");
    let base = partitions / count;
    let extra = partitions % count;

    let mut next = 0;

    member_ids
        .into_iter()
        .enumerate()
        .map(|(rank, member_id)| {
            let take = base + usize::from(rank < extra);
            let assigned = (next..next + take)
                .map(|partition| partition.to_string())
                .collect::<Vec<_>>()
                .join(",");
            next += take;

            SyncGroupRequestAssignment::default()
                .member_id(member_id)
                .assignment(Bytes::from(assigned))
        })
        .collect()
}

fn decode_partitions(assignment: &Bytes) -> Result<Vec<i32>> {
    std::str::from_utf8(assignment)?
        .split(',')
        .filter(|partition| !partition.is_empty())
        .map(|partition| partition.parse::<i32>().map_err(Into::into))
        .collect()
}

/// Drive one consumer through the full KIP-394 dance against its ingress
/// coordinator until it holds a non-empty assignment: empty-member-id join →
/// `MemberIdRequired` → real join with the generated id → sync (the leader
/// computes the range assignment over the member list its join returned) —
/// re-joining on `RebalanceInProgress` / an empty assignment, resetting its
/// id on `UnknownMemberId`, for at most [`MAX_ROUNDS`] rounds. `None` means
/// the member never converged inside the bound.
async fn drive_member<C>(ingress: C, index: usize) -> Result<Option<(String, Vec<i32>)>>
where
    C: Coordinator,
{
    let client_id = format!("member-{index:02}");

    let protocols = [JoinGroupRequestProtocol::default()
        .name(RANGE.into())
        .metadata(Bytes::from(format!("metadata-{index:02}")))];

    let mut member_id = String::new();

    for round in 0..MAX_ROUNDS {
        let join = match ingress
            .join(
                Some(client_id.as_str()),
                GROUP_ID,
                SESSION_TIMEOUT_MS,
                REBALANCE_TIMEOUT_MS,
                member_id.as_str(),
                None,
                PROTOCOL_TYPE,
                Some(&protocols[..]),
                None,
            )
            .await?
        {
            Body::JoinGroupResponse(join) => join,
            otherwise => return Err(anyhow!("expecting a join response: {otherwise:?}")),
        };

        match ErrorCode::try_from(join.error_code)? {
            ErrorCode::None => member_id = join.member_id.clone(),

            ErrorCode::MemberIdRequired => {
                member_id = join.member_id.clone();
                continue;
            }

            ErrorCode::UnknownMemberId => {
                member_id.clear();
                continue;
            }

            error_code => {
                debug!(client_id, round, ?error_code, "join retry");
                continue;
            }
        }

        let assignments = if join.leader == join.member_id {
            range_assignments(join.members.as_deref().unwrap_or_default())
        } else {
            Vec::new()
        };

        let sync = match ingress
            .sync(
                GROUP_ID,
                join.generation_id,
                member_id.as_str(),
                None,
                Some(PROTOCOL_TYPE),
                Some(RANGE),
                Some(&assignments[..]),
            )
            .await?
        {
            Body::SyncGroupResponse(sync) => sync,
            otherwise => return Err(anyhow!("expecting a sync response: {otherwise:?}")),
        };

        match ErrorCode::try_from(sync.error_code)? {
            ErrorCode::None if !sync.assignment.is_empty() => {
                return Ok(Some((member_id, decode_partitions(&sync.assignment)?)));
            }

            ErrorCode::UnknownMemberId => member_id.clear(),

            error_code => debug!(client_id, round, ?error_code, "sync retry"),
        }
    }

    Ok(None)
}

/// Run the 16 members concurrently, member `i` entering via ingress
/// `i % ingresses.len()` — the rotating scatter of production's
/// per-connection load balancing (16 members over 10 ingresses touches
/// every ingress). Each member is bounded by `deadline` on top of
/// [`MAX_ROUNDS`]; `None` at index `i` means member `i` did not converge.
async fn drive_members<C>(
    ingresses: &[C],
    deadline: Duration,
) -> Result<Vec<Option<(String, Vec<i32>)>>>
where
    C: Coordinator,
{
    let mut tasks = JoinSet::new();

    for index in 0..MEMBERS {
        let ingress = ingresses[index % ingresses.len()].clone();

        _ = tasks
            .spawn(async move { (index, timeout(deadline, drive_member(ingress, index)).await) });
    }

    let mut results = Vec::new();
    results.resize_with(MEMBERS, || None);

    while let Some(joined) = tasks.join_next().await {
        let (index, result) = joined.expect("member task panicked");

        results[index] = match result {
            Ok(converged) => converged?,
            Err(elapsed) => {
                debug!(index, %elapsed, "member timed out before converging");
                None
            }
        };
    }

    Ok(results)
}

/// Whether the persisted group is `Formed` covering exactly `member_ids`,
/// every assignment non-empty, their union covering partitions
/// `0..PARTITIONS` with no gaps and no duplicates, at a generation (and
/// object version) that is stable across two reads.
async fn persisted_group_converged(
    shared: &SharedStorage,
    member_ids: &BTreeSet<String>,
) -> Result<bool> {
    let Some((detail, version)) = shared.read_group(GROUP_ID).await? else {
        return Ok(false);
    };

    let GroupState::Formed { assignments, .. } = &detail.state else {
        return Ok(false);
    };

    if assignments.keys().cloned().collect::<BTreeSet<_>>() != *member_ids {
        return Ok(false);
    }

    let mut covered = BTreeSet::new();

    for assignment in assignments.values() {
        if assignment.is_empty() {
            return Ok(false);
        }

        for partition in decode_partitions(assignment)? {
            if !covered.insert(partition) {
                return Ok(false);
            }
        }
    }

    if covered != (0..PARTITIONS).collect::<BTreeSet<_>>() {
        return Ok(false);
    }

    let Some((again, version_again)) = shared.read_group(GROUP_ID).await? else {
        return Ok(false);
    };

    Ok(detail.generation_id == again.generation_id && version == version_again)
}

// The proof: with every replica wrapped in a `ForwardingCoordinator` whose
// registry knows all 10 peers, the rendezvous routes ALL of group `g`'s
// traffic to the ONE owner replica — the single-`Controller` regime — and
// the 16 members scattered across 10 ingresses fully converge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarded_sixteen_members_across_ten_replicas_converge() -> Result<()> {
    let _guard = init_tracing()?;

    let shared = shared_storage().await?;
    let replicas = replicas(&shared)?;
    let peers = (0..REPLICAS).map(peer).collect::<Vec<_>>();

    let forward = InProcessForward::new(
        peers
            .iter()
            .copied()
            .zip(replicas.iter().cloned())
            .collect(),
    );

    let ingresses = replicas
        .iter()
        .enumerate()
        .map(|(index, replica)| {
            let registry = Arc::new(PeerRegistry::new(
                peer(index),
                "unused.invalid",
                Duration::from_secs(5),
            ));
            registry.set_peers(peers.clone());

            ForwardingCoordinator::with_forward(replica.clone(), registry, forward.clone())
        })
        .collect::<Vec<_>>();

    let results = drive_members(&ingresses, FORWARDED_DEADLINE).await?;

    // every one of the 16 members ends with a non-empty assignment under a
    // distinct member id.
    let mut assigned = BTreeMap::new();

    for (index, result) in results.into_iter().enumerate() {
        let (member_id, partitions) =
            result.unwrap_or_else(|| panic!("member {index} did not converge"));

        assert!(
            !partitions.is_empty(),
            "member {index} converged with an empty assignment"
        );
        assert!(
            assigned.insert(member_id.clone(), partitions).is_none(),
            "duplicate member id {member_id}"
        );
    }

    assert_eq!(MEMBERS, assigned.len());

    // the group's traffic went to exactly ONE owner: every non-owner ingress
    // forwarded (9 of the 10 ingresses carry members, so the hop was
    // exercised), only the rendezvous owner was ever dialled, and no forward
    // fell back to scattered-local processing.
    let owner_ip = owner(GROUP_ID, peer(0), &peers);

    assert!(forward.calls() > 0);
    assert_eq!(BTreeSet::from([owner_ip]), forward.dialed());

    for ingress in &ingresses {
        assert_eq!(0, ingress.fallbacks());
    }

    // the persisted group is Formed with a non-empty assignment for all 16
    // members, the union of assignments covers the partitions uniquely, and
    // the generation (and object version) is stable.
    let member_ids = assigned.keys().cloned().collect::<BTreeSet<_>>();

    assert!(
        persisted_group_converged(&shared, &member_ids).await?,
        "persisted group is not fully converged: {:?}",
        shared.read_group(GROUP_ID).await?
    );

    Ok(())
}

// The control, proving the proof above is meaningful: the SAME 16 consumers,
// constants and shared-store latency, scattered across the same 10 replicas
// WITHOUT forwarding (direct-to-random-`Controller` — the pre-fix production
// path) must NOT converge inside the same bounded window: the scattered
// join/sync long-polls keep thrashing the single group object's CAS.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scattered_sixteen_members_across_ten_replicas_do_not_converge() -> Result<()> {
    let _guard = init_tracing()?;

    let shared = shared_storage().await?;
    let replicas = replicas(&shared)?;

    let results = drive_members(&replicas, SCATTERED_WINDOW).await?;

    let unassigned = results.iter().filter(|result| result.is_none()).count();

    let member_ids = results
        .iter()
        .flatten()
        .map(|(member_id, _)| member_id.clone())
        .collect::<BTreeSet<_>>();

    let converged = unassigned == 0 && persisted_group_converged(&shared, &member_ids).await?;

    assert!(
        !converged,
        "16 members scattered across 10 replicas converged WITHOUT forwarding: \
         the convergence proof no longer demonstrates anything"
    );

    debug!(
        unassigned,
        assigned = member_ids.len(),
        "scattered members did not converge"
    );

    Ok(())
}
