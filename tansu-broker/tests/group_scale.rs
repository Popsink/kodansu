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

//! The program exit criterion for #359, as a test rather than a rig.
//!
//! `cg_forward` proves one group of 16 members converges across 10 replicas.
//! What #359 has to be judged on is *many* groups at once — the shape that made
//! the 1500-topic deployment wedge — and on what each group costs to run, not
//! only on whether it converges. So this drives `GROUPS × MEMBERS` consumers
//! through the full KIP-394 dance over one shared store and asserts three
//! things:
//!
//! - every member of every group ends with a non-empty assignment;
//! - the group-state CAS conflict count is zero;
//! - each group's group-state PUTs stay inside a budget.
//!
//! **Today this runs the forwarded arrangement**, which is the one that
//! converges: it is the baseline the decomposition is measured against, and it
//! is why the harness lands before the flip rather than with it. Set
//! `TANSU_SCALE_FORWARDING=false` to run the same consumers scattered across
//! every replica with no owner — the arrangement #360 wants to make the only
//! one, and the arrangement that fails today. The failure is the point of the
//! switch: it is what the flip has to turn green.
//!
//! `#[ignore]` by default. It is minutes of wall clock at the default size and
//! it is not a regression gate; `just test-group-scale` runs it, and the size
//! comes from the environment so a laptop can run a tenth of production and CI
//! can run more:
//!
//! ```text
//! TANSU_SCALE_GROUPS=64 TANSU_SCALE_MEMBERS=16 TANSU_SCALE_REPLICAS=10 \
//!   TANSU_SCALE_FORWARDING=true just test-group-scale
//! ```

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    net::{IpAddr, Ipv4Addr},
    ops::Range,
    str::FromStr,
    sync::{
        Arc,
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
        forward::{Forward, ForwardingCoordinator, PeerRegistry},
    },
};
use tansu_sans_io::{
    Body, ErrorCode, join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember, sync_group_request::SyncGroupRequestAssignment,
};
use tansu_storage::{GroupState, LatencyIntroducingStorage, Storage, StorageContainer};
use tokio::{task::JoinSet, time::timeout};
use tracing::{debug, info};
use url::Url;

use crate::common::init_tracing;

pub mod common;

const PROTOCOL_TYPE: &str = "consumer";
const RANGE: &str = "range";

const SESSION_TIMEOUT_MS: i32 = 45_000;
const REBALANCE_TIMEOUT_MS: Option<i32> = Some(60_000);

/// Partitions per group's topic. Two per member at the default size, so an
/// assignment that silently collapsed onto one member would fail the coverage
/// check rather than merely look small.
const PARTITIONS_PER_MEMBER: i32 = 2;

/// Store latency under every replica. Deliberately an order of magnitude below
/// `cg_forward`'s 50..150ms: at 64 groups the question is how many operations a
/// group costs and whether they collide, not how slow one round trip is, and a
/// production-latency run of this size is half an hour of mostly sleeping.
const STORE_LATENCY_MS: Range<u64> = 2..8;

/// Upper bound on join/sync rounds per member. A converging member needs ~3
/// (KIP-394 empty join, real join, sync); the bound is what stops a
/// non-converging arrangement looping forever.
const MAX_ROUNDS: usize = 32;

type SharedStorage = Arc<Box<dyn Storage>>;
type ReplicaStorage = LatencyIntroducingStorage<SharedStorage>;
type Replica = Controller<ReplicaStorage>;

/// One replica's cost counters, kept together so an assertion names what it
/// read rather than indexing parallel vectors.
struct Counters {
    group_updates: Arc<AtomicU64>,
    cas_conflicts: Arc<AtomicU64>,
}

impl Counters {
    fn group_updates(&self) -> u64 {
        self.group_updates.load(Ordering::Relaxed)
    }

    fn cas_conflicts(&self) -> u64 {
        self.cas_conflicts.load(Ordering::Relaxed)
    }
}

/// A run's size, from the environment.
struct Scale {
    groups: usize,
    members: usize,
    replicas: usize,
    forwarding: bool,
    /// Group-state PUTs a single group may cost, formation included.
    ///
    /// Formation is inherently several CASes — one per member joining, plus the
    /// leader's assignment — so the budget is per member, not a constant. The
    /// forwarded arrangement measures 18 PUTs for a 16-member group (one per
    /// join, plus the leader's sync and the birth), so a budget of two per
    /// member leaves about 78% headroom while still catching a *regression in
    /// kind*: a path that writes group state per heartbeat, or a retry loop
    /// that rewrites it once per round, blows through it immediately.
    put_budget_per_member: u64,
    /// Per-member deadline. Only reached when a member never converges — a
    /// converged one returns immediately — so it is generous on purpose.
    deadline: Duration,
}

fn from_env<T>(name: &str, fallback: T) -> Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|err| anyhow!("{name}={value}: {err}")),
        Err(_) => Ok(fallback),
    }
}

impl Scale {
    fn from_env() -> Result<Self> {
        Ok(Self {
            groups: from_env("TANSU_SCALE_GROUPS", 64)?,
            members: from_env("TANSU_SCALE_MEMBERS", 16)?,
            replicas: from_env("TANSU_SCALE_REPLICAS", 10)?,
            forwarding: from_env("TANSU_SCALE_FORWARDING", true)?,
            put_budget_per_member: from_env("TANSU_SCALE_PUT_BUDGET_PER_MEMBER", 2)?,
            deadline: Duration::from_secs(from_env("TANSU_SCALE_DEADLINE_SECS", 120)?),
        })
    }

    fn partitions(&self) -> i32 {
        i32::try_from(self.members).expect("member count fits an i32") * PARTITIONS_PER_MEMBER
    }

    fn group_id(&self, index: usize) -> String {
        format!("scale-{index:04}")
    }
}

/// The fake pod IP of replica `index`.
fn peer(index: usize) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(
        10,
        0,
        u8::try_from(index / 256).expect("replica count fits two IPv4 octets"),
        u8::try_from(index % 256).expect("replica index fits an IPv4 octet"),
    ))
}

/// The in-process stand-in for the internal-listener hop, as `cg_forward`'s:
/// dispatch a forwarded group API straight into the owner's `Controller`.
#[derive(Debug)]
struct InProcessForward {
    replicas: BTreeMap<IpAddr, Replica>,
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

/// The leader's range assignment over the member list its join returned.
fn range_assignments(
    members: &[JoinGroupResponseMember],
    partitions: i32,
) -> Vec<SyncGroupRequestAssignment> {
    let member_ids = members
        .iter()
        .map(|member| member.member_id.clone())
        .collect::<BTreeSet<_>>();

    let count = member_ids.len().max(1);
    let partitions = usize::try_from(partitions).expect("partition count");
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

/// Drive one consumer through the full KIP-394 dance against its ingress until
/// it holds a non-empty assignment. `None` means it never converged inside
/// [`MAX_ROUNDS`].
async fn drive_member<C>(
    ingress: C,
    group_id: String,
    partitions: i32,
    index: usize,
) -> Result<Option<(String, Vec<i32>)>>
where
    C: Coordinator,
{
    let client_id = format!("{group_id}-member-{index:02}");

    let protocols = [JoinGroupRequestProtocol::default()
        .name(RANGE.into())
        .metadata(Bytes::from(format!("metadata-{index:02}")))];

    let mut member_id = String::new();

    for round in 0..MAX_ROUNDS {
        let join = match ingress
            .join(
                Some(client_id.as_str()),
                group_id.as_str(),
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
            range_assignments(join.members.as_deref().unwrap_or_default(), partitions)
        } else {
            Vec::new()
        };

        let sync = match ingress
            .sync(
                group_id.as_str(),
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

/// Whether the persisted group is `Formed` over exactly `member_ids`, every
/// assignment non-empty, their union covering `0..partitions` with no gaps and
/// no duplicates.
async fn persisted_group_converged(
    shared: &SharedStorage,
    group_id: &str,
    member_ids: &BTreeSet<String>,
    partitions: i32,
) -> Result<bool> {
    let Some((detail, _)) = shared.read_group(group_id).await? else {
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

    Ok(covered == (0..partitions).collect::<BTreeSet<_>>())
}

/// Run every group's members concurrently, member `m` of every group entering
/// through ingress `m % ingresses.len()` — the rotating scatter of
/// per-connection load balancing, so a group's members are spread across every
/// replica. Returns the converged member ids per group, and fails the run if
/// any member did not get there.
async fn drive_groups<C>(
    scale: &Scale,
    ingresses: &[C],
) -> Result<BTreeMap<usize, BTreeSet<String>>>
where
    C: Coordinator,
{
    let partitions = scale.partitions();
    let mut tasks = JoinSet::new();

    for group in 0..scale.groups {
        for member in 0..scale.members {
            let ingress = ingresses[member % ingresses.len()].clone();
            let group_id = scale.group_id(group);
            let deadline = scale.deadline;

            _ = tasks.spawn(async move {
                (
                    group,
                    member,
                    timeout(
                        deadline,
                        drive_member(ingress, group_id, partitions, member),
                    )
                    .await,
                )
            });
        }
    }

    let mut converged: BTreeMap<usize, BTreeSet<String>> = BTreeMap::new();
    let mut stragglers = Vec::new();

    while let Some(joined) = tasks.join_next().await {
        let (group, member, result) = joined.expect("member task panicked");

        match result {
            Ok(Ok(Some((member_id, assigned)))) if !assigned.is_empty() => {
                assert!(
                    converged
                        .entry(group)
                        .or_default()
                        .insert(member_id.clone()),
                    "duplicate member id {member_id} in group {group}"
                );
            }

            Ok(Ok(_)) => stragglers.push((group, member, String::from("no assignment"))),
            Ok(Err(err)) => stragglers.push((group, member, format!("{err}"))),
            Err(elapsed) => stragglers.push((group, member, format!("{elapsed}"))),
        }
    }

    assert!(
        stragglers.is_empty(),
        "{} of {} members did not converge: {:?}",
        stragglers.len(),
        scale.groups * scale.members,
        &stragglers[..stragglers.len().min(16)],
    );

    Ok(converged)
}

/// `GROUPS × MEMBERS` consumers across `REPLICAS`, all sharing one store.
///
/// The three assertions are convergence, zero group-state CAS conflicts, and a
/// per-group write budget — in that order of importance, and all three are what
/// #359 is judged on. The counters come from the store wrapper each replica
/// sits behind, so the budget is measured at the object store rather than
/// inferred from the coordinator's intentions.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale rig: minutes of wall clock, run with `just test-group-scale`"]
async fn groups_converge_within_their_write_budget() -> Result<()> {
    let _guard = init_tracing()?;

    let scale = Scale::from_env()?;
    let partitions = scale.partitions();

    info!(
        groups = scale.groups,
        members = scale.members,
        replicas = scale.replicas,
        forwarding = scale.forwarding,
        partitions,
        "group scale run"
    );

    let shared: SharedStorage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .storage(Url::parse("memory://")?)
        .build()
        .await?;

    let mut controllers = Vec::with_capacity(scale.replicas);
    let mut counters = Vec::with_capacity(scale.replicas);

    for index in 0..scale.replicas {
        let storage = LatencyIntroducingStorage::new(shared.clone())
            .with_seed(index as u64)
            .with_latency(STORE_LATENCY_MS);

        counters.push(Counters {
            group_updates: storage.group_updates_handle(),
            cas_conflicts: storage.cas_conflicts_handle(),
        });

        controllers.push(Controller::with_storage(storage)?);
    }

    let peers = (0..scale.replicas).map(peer).collect::<Vec<_>>();

    // Scattered is the arrangement with no owner: a member's requests land on
    // whichever replica the load balancer picked, and every replica drives the
    // group's state itself. Forwarded routes each group to its rendezvous owner
    // — today's shipped behaviour, and the baseline this measures against.
    //
    // `Coordinator` is not dyn-compatible, so the two arrangements cannot share
    // a boxed vector; they share the driver instead.
    let converged = if scale.forwarding {
        let forward = Arc::new(InProcessForward {
            replicas: peers
                .iter()
                .copied()
                .zip(controllers.iter().cloned())
                .collect(),
        });

        let ingresses = controllers
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

        drive_groups(&scale, &ingresses).await?
    } else {
        drive_groups(&scale, &controllers).await?
    };

    for (group, member_ids) in &converged {
        assert_eq!(scale.members, member_ids.len(), "group {group}");

        assert!(
            persisted_group_converged(&shared, &scale.group_id(*group), member_ids, partitions)
                .await?,
            "group {group} is not fully converged: {:?}",
            shared.read_group(&scale.group_id(*group)).await?
        );
    }

    let group_updates = counters.iter().map(Counters::group_updates).sum::<u64>();
    let cas_conflicts = counters.iter().map(Counters::cas_conflicts).sum::<u64>();

    let budget = scale.put_budget_per_member * scale.members as u64 * scale.groups as u64;

    info!(
        group_updates,
        cas_conflicts,
        budget,
        per_group = group_updates / scale.groups as u64,
        "group scale cost"
    );

    // Zero, not "few". A CAS conflict is a write the store rejected, and the
    // whole argument for routing a group to one owner — and, after #359, for
    // taking liveness off the contended object — is that a converging group
    // does not produce them.
    assert_eq!(
        0, cas_conflicts,
        "group-state CAS conflicts across {} replicas",
        scale.replicas
    );

    assert!(
        group_updates <= budget,
        "group-state PUTs {group_updates} exceed the budget {budget} \
         ({} groups × {} members × {})",
        scale.groups,
        scale.members,
        scale.put_budget_per_member,
    );

    Ok(())
}
