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
//! - once converged, a group's heartbeats cost **zero** generation CAS
//!   conflicts and no per-member write of `generation.json`;
//! - forming a group stays inside a per-member write budget.
//!
//! Every replica drives every group. There is no owner, no forwarding and no
//! configuration: that arrangement could not converge before #359 decomposed
//! the group object, it is what this file existed to switch between while the
//! decomposition landed, and since #360 it is the only one there is.
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
    ops::Range,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use tansu_broker::{
    NODE_ID,
    coordinator::group::{Coordinator, administrator::Controller},
};
use tansu_sans_io::{
    Body, ErrorCode, join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember, sync_group_request::SyncGroupRequestAssignment,
};
use tansu_storage::{LatencyIntroducingStorage, Storage, StorageContainer};
use tokio::{task::JoinSet, time::timeout};
use tracing::{debug, info};
use url::Url;

use crate::common::init_tracing;

pub mod common;

const PROTOCOL_TYPE: &str = "consumer";
const RANGE: &str = "range";

/// Deliberately far above a production session timeout, and above any run of
/// this rig.
///
/// The rig forms every group in one batch and only then heartbeats, where a
/// real deployment interleaves the two: a member that converged in the first
/// second of a 64-group run would otherwise have gone `SESSION_TIMEOUT_MS`
/// without contact by the time the last group formed, and the sweep would
/// evict it — correctly. That is an artifact of the rig's shape, not a
/// property of the coordinator, and buying determinism by making the window
/// wide is cheaper than interleaving two schedules to measure one of them.
const SESSION_TIMEOUT_MS: i32 = 600_000;

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

/// Heartbeats per member in the steady-state window. Enough that a
/// per-heartbeat write of `generation.json` would be unmissable.
const STEADY_HEARTBEATS: usize = 4;

/// Upper bound on join/sync rounds per member. A converging member needs ~3
/// (KIP-394 empty join, real join, sync); the bound is what stops a
/// non-converging arrangement looping forever.
const MAX_ROUNDS: usize = 32;

type SharedStorage = Arc<Box<dyn Storage>>;
type ReplicaStorage = LatencyIntroducingStorage<SharedStorage>;
type Replica = Controller<ReplicaStorage>;

/// One replica's cost counters, kept together so an assertion names what it
/// read rather than indexing parallel vectors.
///
/// The object to count is `generation.json`: since #359 it is the only one a
/// group's members can contend on, and `{group}.json` — what this used to
/// count — is not written at all.
struct Counters {
    generation_updates: Arc<AtomicU64>,
    generation_cas_conflicts: Arc<AtomicU64>,
    member_lists: Arc<AtomicU64>,
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

    fn lists(counters: &[Self]) -> u64 {
        counters
            .iter()
            .map(|counters| counters.member_lists.load(Ordering::Relaxed))
            .sum()
    }
}

/// A run's size, from the environment.
struct Scale {
    groups: usize,
    members: usize,
    replicas: usize,
    /// Writes of `generation.json` a single group may cost, formation
    /// included, and won or lost — a conditional PUT the store rejects is
    /// still a request it charged for.
    ///
    /// Formation is inherently one landed CAS per member joining, so the
    /// budget is per member rather than a constant. The rest is the race: 16
    /// members contending for those 16 slots lose the CAS and re-apply, and
    /// nothing serializes them any more — the per-group in-process lock went
    /// with the rest of the per-group state (#360), so members served by the
    /// same replica race exactly as members on different ones do.
    ///
    /// Measured at the default size: **5010 attempts for 1024 members**, 1024
    /// of them landing — 4.9 per member. Eight leaves ~40% headroom while
    /// still catching a *regression in kind*: a path that writes the
    /// generation per heartbeat, or a retry loop that rewrites it once per
    /// round, blows through it immediately.
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
            put_budget_per_member: from_env("TANSU_SCALE_PUT_BUDGET_PER_MEMBER", 8)?,
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

/// Whether the persisted group is `Stable` over exactly `member_ids`, every
/// assignment non-empty, their union covering `0..partitions` with no gaps and
/// no duplicates.
///
/// Read from the decomposed objects, not through a projection: what has to
/// hold is that the generation names exactly these members and that
/// `assignment/{generation}` exists and covers them.
async fn persisted_group_converged(
    shared: &SharedStorage,
    group_id: &str,
    member_ids: &BTreeSet<String>,
    partitions: i32,
) -> Result<bool> {
    let Some((generation, _)) = shared.read_group_generation(group_id).await? else {
        return Ok(false);
    };

    if generation.members.keys().cloned().collect::<BTreeSet<_>>() != *member_ids {
        return Ok(false);
    }

    let Some(assignment) = shared
        .read_group_assignment(group_id, generation.generation_id)
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
/// The assertions are convergence, a formation write budget, and a
/// **steady-state window that costs nothing** — in that order of importance,
/// and all of them are what #359 is judged on. The counters come from the store
/// wrapper each replica sits behind, so the cost is measured at the object
/// store rather than inferred from the coordinator's intentions.
///
/// Formation conflicts are budgeted rather than forbidden: `MEMBERS` members
/// racing to add themselves to one document is a race, and the CAS is what
/// resolves it. What must be exactly zero is the *steady state*, because that
/// is the regime a deployment actually lives in and the one that used to
/// require an owner.
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
            generation_updates: storage.generation_updates_handle(),
            generation_cas_conflicts: storage.generation_cas_conflicts_handle(),
            member_lists: storage.member_lists_handle(),
        });

        controllers.push(Controller::with_storage(storage)?);
    }

    // Every member enters through whichever replica the load balancer picked,
    // and every replica drives the group's state itself. There is nothing else
    // to arrange.
    let converged = drive_groups(&scale, &controllers).await?;

    for (group, member_ids) in &converged {
        assert_eq!(scale.members, member_ids.len(), "group {group}");

        assert!(
            persisted_group_converged(&shared, &scale.group_id(*group), member_ids, partitions)
                .await?,
            "group {group} is not fully converged: {:?}",
            shared
                .read_group_generation(&scale.group_id(*group))
                .await?
        );
    }

    let formation_updates = Counters::updates(&counters);
    let formation_conflicts = Counters::conflicts(&counters);

    let budget = scale.put_budget_per_member * scale.members as u64 * scale.groups as u64;

    info!(
        formation_updates,
        formation_conflicts,
        budget,
        per_group = formation_updates / scale.groups as u64,
        "group formation cost"
    );

    assert!(
        formation_updates <= budget,
        "forming {} groups cost {formation_updates} writes of generation.json, over the \
         budget {budget} ({} groups × {} members × {})",
        scale.groups,
        scale.groups,
        scale.members,
        scale.put_budget_per_member,
    );

    // The steady state, which is the regime that matters: every member of every
    // group heartbeats, several times over, from the replica it entered
    // through. Nothing here may touch `generation.json` — no write, so no
    // conflict — and nothing may LIST a group's member documents.
    let lists_before = Counters::lists(&counters);
    let conflicts_before = Counters::conflicts(&counters);
    let updates_before = Counters::updates(&counters);

    heartbeat_every_member(&scale, &controllers, &shared, &converged).await?;

    let steady_updates = Counters::updates(&counters) - updates_before;

    info!(
        steady_updates,
        steady_conflicts = Counters::conflicts(&counters) - conflicts_before,
        steady_lists = Counters::lists(&counters) - lists_before,
        "group steady-state cost"
    );

    // At most one sweep stamp per group may fall inside the window: the sweep
    // is deduped through the group's own `swept_at_ms`, so it costs one write
    // per group per session/2 across the whole fleet rather than one per
    // member per heartbeat.
    assert!(
        steady_updates <= scale.groups as u64,
        "{STEADY_HEARTBEATS} heartbeats per member wrote generation.json {steady_updates} \
         times across {} groups; only the sweep stamp may write in steady state",
        scale.groups,
    );

    // Zero, not "few". A CAS conflict is a write the store rejected, and the
    // whole argument for routing a group to one owner — and, after #359, for
    // taking liveness off the contended object — is that a *converged* group
    // does not produce them at any replica count.
    assert_eq!(
        conflicts_before,
        Counters::conflicts(&counters),
        "a converged group's heartbeats contended on generation.json across {} replicas",
        scale.replicas,
    );

    // The "no LIST on the request path" promise, stated as a count rather than
    // as a comment: the sweep and the leader's join both fan out over the
    // generation's member set, which needs no listing.
    assert_eq!(
        lists_before,
        Counters::lists(&counters),
        "a request path listed a group's member documents",
    );

    Ok(())
}

/// Heartbeats every member of every converged group, [`STEADY_HEARTBEATS`]
/// times, from the replica it entered through — and requires every one of them
/// to be accepted, so a window that measured nothing because the group had
/// fallen apart fails rather than passes.
async fn heartbeat_every_member(
    scale: &Scale,
    replicas: &[Replica],
    shared: &SharedStorage,
    converged: &BTreeMap<usize, BTreeSet<String>>,
) -> Result<()> {
    for (group, member_ids) in converged {
        let group_id = scale.group_id(*group);

        let generation_id = shared
            .read_group_generation(&group_id)
            .await?
            .map(|(generation, _)| generation.generation_id)
            .ok_or_else(|| anyhow!("group {group_id} has no generation"))?;

        for _ in 0..STEADY_HEARTBEATS {
            for (index, member_id) in member_ids.iter().enumerate() {
                let replica = &replicas[index % replicas.len()];

                match replica
                    .heartbeat(&group_id, generation_id, member_id.as_str(), None)
                    .await?
                {
                    Body::HeartbeatResponse(heartbeat) => assert_eq!(
                        i16::from(ErrorCode::None),
                        heartbeat.error_code,
                        "{member_id} was not accepted in {group_id} at generation {generation_id}",
                    ),

                    otherwise => {
                        return Err(anyhow!("expecting a heartbeat response: {otherwise:?}"));
                    }
                }
            }
        }
    }

    Ok(())
}
