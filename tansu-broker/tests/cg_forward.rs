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
//! Both arrangements share every constant, and since #359 both must converge.
//! The control used to assert the opposite — that the scattered path thrashes
//! the group CAS — because all of a group's state lived behind one etag, so a
//! group without an owner could not stop its own members starving the write it
//! was waiting on. Decomposing that object is what removes the reason for the
//! owner: the scattered arrangement now converges *and* produces no
//! steady-state contention, which is the precondition #360 needs to delete
//! forwarding. Forwarding is still exercised here, and still has to work, until
//! it does.

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
use tansu_storage::{LatencyIntroducingStorage, Storage, StorageContainer};
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

/// Per-member deadline, in both arrangements. Only reached on failure — a
/// converged member returns immediately — so it is deliberately generous:
/// convergence takes ~25s here, but a loaded CI worker can double that.
const FORWARDED_DEADLINE: Duration = Duration::from_secs(120);

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

/// One replica's cost counters, kept together so an assertion names what it
/// read rather than indexing parallel vectors.
struct Counters {
    generation_updates: Arc<AtomicU64>,
    generation_cas_conflicts: Arc<AtomicU64>,
}

impl Counters {
    fn updates(counters: &[Self]) -> u64 {
        counters
            .iter()
            .map(|counters| counters.generation_updates.load(Ordering::Relaxed))
            .sum()
    }

    fn conflicts(counters: &[Self]) -> u64 {
        counters
            .iter()
            .map(|counters| counters.generation_cas_conflicts.load(Ordering::Relaxed))
            .sum()
    }
}

/// The 10 replicas: independent `Controller`s over the one shared store, each
/// behind deterministically-seeded store latency.
///
/// Also returns, aligned by index, that replica's counters for the object a
/// group's members can contend on — `generation.json` — so a test can assert
/// how much read-modify-write contention there actually was. Before #359 the
/// counters to read were the single group object's; that object is neither
/// written nor read any more, so counting it here would assert zero of nothing.
fn replicas(shared: &SharedStorage) -> Result<(Vec<Replica>, Vec<Counters>)> {
    let mut controllers = Vec::with_capacity(REPLICAS);
    let mut counters = Vec::with_capacity(REPLICAS);

    for index in 0..REPLICAS {
        let storage = LatencyIntroducingStorage::new(shared.clone())
            .with_seed(index as u64)
            .with_latency(STORE_LATENCY_MS);

        counters.push(Counters {
            generation_updates: storage.generation_updates_handle(),
            generation_cas_conflicts: storage.generation_cas_conflicts_handle(),
        });

        controllers.push(Controller::with_storage(storage)?);
    }

    Ok((controllers, counters))
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

/// Whether the persisted group is `Stable` over exactly `member_ids`, every
/// assignment non-empty, their union covering partitions `0..PARTITIONS` with
/// no gaps and no duplicates, at a generation (and object version) that is
/// stable across two reads.
///
/// Read from the decomposed objects rather than through a projection: what
/// this has to prove is that the *layout* converged — the generation names
/// exactly the members, and `assignment/{generation}` exists and covers them —
/// and a projection that fell back to the legacy object would report a group
/// that the new write path never wrote.
async fn persisted_group_converged(
    shared: &SharedStorage,
    member_ids: &BTreeSet<String>,
) -> Result<bool> {
    let Some((generation, version)) = shared.read_group_generation(GROUP_ID).await? else {
        return Ok(false);
    };

    if generation.members.keys().cloned().collect::<BTreeSet<_>>() != *member_ids {
        return Ok(false);
    }

    let Some(assignment) = shared
        .read_group_assignment(GROUP_ID, generation.generation_id)
        .await?
    else {
        return Ok(false);
    };

    if assignment
        .assignments
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>()
        != *member_ids
    {
        return Ok(false);
    }

    let mut covered = BTreeSet::new();

    for assigned in assignment.assignments.values() {
        if assigned.is_empty() {
            return Ok(false);
        }

        for partition in decode_partitions(assigned)? {
            if !covered.insert(partition) {
                return Ok(false);
            }
        }
    }

    if covered != (0..PARTITIONS).collect::<BTreeSet<_>>() {
        return Ok(false);
    }

    let Some((again, version_again)) = shared.read_group_generation(GROUP_ID).await? else {
        return Ok(false);
    };

    Ok(generation.generation_id == again.generation_id && version == version_again)
}

/// What the group looks like when a diagnostic wants to print it.
async fn persisted_group(shared: &SharedStorage) -> String {
    match shared.read_group_generation(GROUP_ID).await {
        Ok(Some((generation, _))) => format!("{generation:?}"),
        Ok(None) => String::from("no generation"),
        Err(error) => format!("{error:?}"),
    }
}

// The proof: with every replica wrapped in a `ForwardingCoordinator` whose
// registry knows all 10 peers, the rendezvous routes ALL of group `g`'s
// traffic to the ONE owner replica — the single-`Controller` regime — and
// the 16 members scattered across 10 ingresses fully converge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarded_sixteen_members_across_ten_replicas_converge() -> Result<()> {
    let _guard = init_tracing()?;

    let shared = shared_storage().await?;
    let (replicas, counters) = replicas(&shared)?;
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

    // Every member's group-coordination request goes to the SAME owner
    // replica, and that owner serializes each group's read->CAS window
    // in-process, so the 16 concurrent members never collide on the
    // generation: the owner observes ZERO conflicts. This used to be the whole
    // argument for having an owner at all; it is now one of two arrangements
    // that hold it, which is the point of the sibling test below.
    let owner_index = peers
        .iter()
        .position(|&candidate| candidate == owner_ip)
        .expect("owner is one of the peers");
    assert_eq!(
        0,
        counters[owner_index]
            .generation_cas_conflicts
            .load(Ordering::Relaxed),
        "owner replica saw in-process generation CAS conflicts despite \
         per-group serialization"
    );

    // the persisted group is Stable with a non-empty assignment for all 16
    // members, the union of assignments covers the partitions uniquely, and
    // the generation (and object version) is stable.
    let member_ids = assigned.keys().cloned().collect::<BTreeSet<_>>();

    assert!(
        persisted_group_converged(&shared, &member_ids).await?,
        "persisted group is not fully converged: {}",
        persisted_group(&shared).await
    );

    Ok(())
}

// The sibling, and what #359 exists to make true: the SAME 16 consumers,
// constants and shared-store latency, scattered across the same 10 replicas
// with NO owner and no forwarding, converge — and once converged, stop writing
// the one object they can contend on at all.
//
// This test asserted the opposite until now. It drove the same members through
// bare `Controller`s and required them to *thrash* the group CAS, because with
// every member's liveness, every subscription, the generation and the
// assignment behind one etag, that is what a group without an owner did: the
// once-a-second `last_contact` churn starved the leader's assignment write.
// Asserting the contention was the honest way to state that forwarding was
// load-bearing.
//
// It is the decomposition that removes the reason for the owner, so the
// assertions are what the decomposition claims:
//
//   1. all 16 members converge with no owner, no forwarding and no lock that
//      spans replicas;
//   2. **formation** costs a bounded number of generation CAS conflicts — the
//      members do race to add themselves, and the CAS is what resolves it, so
//      this is not zero and pretending otherwise would be pinning luck;
//   3. **steady state** costs nothing per member. A converged group's
//      heartbeats do not rewrite `generation.json` and do not contend on it;
//      the only write left is the sweep's stamp, which is one per group per
//      session/2 *across the whole fleet* rather than one per member per
//      heartbeat. That is the property that made forwarding necessary, and its
//      absence is what makes #360 possible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scattered_sixteen_members_across_ten_replicas_converge_without_conflicts() -> Result<()> {
    let _guard = init_tracing()?;

    /// Formation conflicts allowed, as a multiple of the member count.
    ///
    /// 16 members racing to add themselves to one document produce conflicts
    /// in the order of the member count, not its square: a loser re-reads,
    /// finds the winner's document and re-applies its own add, so every round
    /// of the race admits somebody and the backoff desynchronises the rest.
    /// Measured on this configuration: **56 conflicts over 72 attempts**, so
    /// 16 writes landed — exactly one per member, which is the floor for
    /// admitting 16 members one CAS at a time. The budget is what separates
    /// that from a regression *in kind* (a path that CASes per heartbeat, or a
    /// retry loop that never converges), not a pinned measurement.
    const FORMATION_CONFLICT_BUDGET: u64 = 6;

    /// Heartbeats per member in the steady-state window. Enough that a
    /// per-heartbeat write would be unmissable, and the whole window is well
    /// inside the session timeout, so no sweep is due inside it.
    const STEADY_HEARTBEATS: usize = 4;

    let shared = shared_storage().await?;
    let (replicas, counters) = replicas(&shared)?;

    let results = drive_members(&replicas, FORWARDED_DEADLINE).await?;

    let mut assigned = BTreeMap::new();

    for (index, result) in results.into_iter().enumerate() {
        let (member_id, partitions) =
            result.unwrap_or_else(|| panic!("member {index} did not converge without an owner"));

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

    let member_ids = assigned.keys().cloned().collect::<BTreeSet<_>>();

    assert!(
        persisted_group_converged(&shared, &member_ids).await?,
        "persisted group is not fully converged: {}",
        persisted_group(&shared).await
    );

    let formation_conflicts = Counters::conflicts(&counters);

    debug!(
        formation_conflicts,
        formation_updates = Counters::updates(&counters),
        "scattered members formed the group"
    );

    assert!(
        formation_conflicts <= FORMATION_CONFLICT_BUDGET * MEMBERS as u64,
        "forming the group scattered cost {formation_conflicts} generation CAS conflicts, \
         over the {} budgeted for {MEMBERS} members",
        FORMATION_CONFLICT_BUDGET * MEMBERS as u64,
    );

    // The steady-state window. Every member heartbeats, from the replica it
    // entered through, at the generation it converged at.
    let generation_id = shared
        .read_group_generation(GROUP_ID)
        .await?
        .map(|(generation, _)| generation.generation_id)
        .expect("the group must have a generation");

    let updates_before = Counters::updates(&counters);
    let conflicts_before = Counters::conflicts(&counters);

    for _ in 0..STEADY_HEARTBEATS {
        for (index, member_id) in member_ids.iter().enumerate() {
            let replica = &replicas[index % replicas.len()];

            match replica
                .heartbeat(GROUP_ID, generation_id, member_id.as_str(), None)
                .await?
            {
                Body::HeartbeatResponse(heartbeat) => assert_eq!(
                    i16::from(ErrorCode::None),
                    heartbeat.error_code,
                    "member {member_id} was not accepted at generation {generation_id}"
                ),

                otherwise => return Err(anyhow!("expecting a heartbeat response: {otherwise:?}")),
            }
        }
    }

    // At most the sweep's stamp: the window is shorter than session/2, so one
    // sweep can fall inside it and a second cannot. 64 heartbeats, one write —
    // and that write is the group's, not any member's.
    let steady_updates = Counters::updates(&counters) - updates_before;

    assert!(
        steady_updates <= 1,
        "{} heartbeats rewrote generation.json {steady_updates} times; only the \
         sweep stamp (one per group per session/2) may write in steady state",
        STEADY_HEARTBEATS * MEMBERS,
    );

    assert_eq!(
        conflicts_before,
        Counters::conflicts(&counters),
        "a converged group's heartbeats contended on generation.json",
    );

    assert_eq!(
        generation_id,
        shared
            .read_group_generation(GROUP_ID)
            .await?
            .map(|(generation, _)| generation.generation_id)
            .expect("the group must still have a generation"),
        "a converged group's heartbeats moved its generation",
    );

    Ok(())
}
