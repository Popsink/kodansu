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

//! Consumer groups driven end to end through `ConsumerGroupLayer`: each member
//! has its own seeded latency, the coordinator runs over `memory://`, and the
//! whole exchange runs on tokio's paused clock.

use std::collections::{BTreeSet, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use rama::Service;
use rama::{Context, Layer as _};
use rand::{RngExt as _, SeedableRng as _, rngs::SmallRng};
use tansu_broker::NODE_ID;
use tansu_broker::coordinator::group::administrator::Controller;
use tansu_broker::service::coordinator::services;
use tansu_sans_io::consumer::{
    ConsumerProtocolAssignment, ConsumerProtocolSubscription, MemberAssignment,
};
use tansu_sans_io::metadata_response::{MetadataResponsePartition, MetadataResponseTopic};
use tansu_sans_io::{Body, ErrorCode, Frame, HeartbeatResponse, JoinGroupResponse};
use tansu_sans_io::{MetadataResponse, SyncGroupResponse};
use tansu_service::{BytesFrameLayer, ConsumerGroupLayer, ConsumerGroupService};
use tansu_service::{FrameBytesLayer, FrameRouteService, LatencyIntroducingLayer};
use tokio::sync::Notify;
use tokio::sync::mpsc::{self, Sender};
use tokio::task::{self, JoinSet};
use tokio::time::{self, sleep};
use tracing::debug;

use crate::common::{init_tracing, paused_clock, storage_container};

mod common;

const GROUP: &str = "g1";

const TOPIC: &str = "t";

/// The latency, in milliseconds, each request is delayed by in the one- and
/// two-member cases.
const LATENCY_MS: Range<u64> = 50..150;

type Route = FrameRouteService<(), tansu_broker::Error>;

/// The stack under a member's `ConsumerGroupLayer`: frames in, frames out.
trait Wire: Clone + Service<(), Frame, Response = Frame, Error = tansu_broker::Error> {}

impl<T> Wire for T where T: Clone + Service<(), Frame, Response = Frame, Error = tansu_broker::Error>
{}

/// A member as these tests drive it: handed its last answer (or nothing, to
/// start), it sends the request that answer implies and returns the next.
trait Member: Service<(), Option<Body>, Response = Body, Error: Into<anyhow::Error>> {}

impl<T> Member for T where T: Service<(), Option<Body>, Response = Body, Error: Into<anyhow::Error>> {}

/// Every group API, answered by a coordinator over `memory://` that reads
/// the paused clock.
async fn coordinator() -> Result<Route> {
    let listener = url::Url::parse("tcp://127.0.0.1:9092/")?;
    let storage = storage_container("tansu", NODE_ID, listener).await?;
    let controller = Controller::with_storage(storage)?.with_now(paused_clock);
    let routes = services(Route::builder(), controller)?;
    Ok(routes.build()?)
}

/// `TOPIC` with `partitions`, as a metadata response.
fn topic(partitions: Range<i32>) -> MetadataResponse {
    let partitions = partitions
        .map(|index| MetadataResponsePartition::default().partition_index(index))
        .collect();
    let topic = MetadataResponseTopic::default()
        .name(Some(TOPIC.into()))
        .partitions(Some(partitions));
    MetadataResponse::default().topics(Some(vec![topic]))
}

/// A member of `GROUP` subscribed to `TOPIC`, whose requests to `route` are
/// delayed by a latency drawn from `latency_ms` by a generator seeded with
/// `rng_seed`.
fn member(
    route: Route,
    rng_seed: u64,
    latency_ms: Range<u64>,
    metadata: MetadataResponse,
) -> ConsumerGroupService<impl Wire> {
    let group = ConsumerGroupLayer::new(GROUP, [TOPIC], metadata)
        .on_assignment(|group: String, assignment: MemberAssignment| debug!(group, %assignment));
    let delayed = LatencyIntroducingLayer::default()
        .with_seed(rng_seed)
        .with_latency_millis(latency_ms);
    (group, delayed, FrameBytesLayer, BytesFrameLayer::default()).into_layer(route)
}

async fn next(member: &impl Member, previous: Option<Body>) -> Result<Body> {
    let answering = member.serve(Context::default(), previous);
    answering.await.map_err(Into::into)
}

async fn join(member: &impl Member, previous: Option<Body>) -> Result<JoinGroupResponse> {
    match next(member, previous).await? {
        Body::JoinGroupResponse(joined) => Ok(joined),
        otherwise => Err(anyhow!("a JoinGroup answer expected, not {otherwise:?}")),
    }
}

async fn sync(member: &impl Member, previous: Option<Body>) -> Result<SyncGroupResponse> {
    match next(member, previous).await? {
        Body::SyncGroupResponse(synced) => Ok(synced),
        otherwise => Err(anyhow!("a SyncGroup answer expected, not {otherwise:?}")),
    }
}

async fn heartbeat(member: &impl Member, previous: Option<Body>) -> Result<HeartbeatResponse> {
    match next(member, previous).await? {
        Body::HeartbeatResponse(beat) => Ok(beat),
        otherwise => Err(anyhow!("a Heartbeat answer expected, not {otherwise:?}")),
    }
}

fn members(joined: &JoinGroupResponse) -> Option<usize> {
    joined.members.as_ref().map(Vec::len)
}

/// A first JoinGroup, without a member id: KIP-394 answers it with a
/// generated id and nothing else.
async fn first_join(member: &impl Member) -> Result<JoinGroupResponse> {
    let parked = join(member, None).await?;
    assert_eq!(i16::from(ErrorCode::MemberIdRequired), parked.error_code);
    assert_eq!(-1, parked.generation_id);
    assert!(parked.leader.is_empty());
    assert_eq!(Some(0), members(&parked));
    assert!(!parked.member_id.is_empty());
    Ok(parked)
}

/// Rejoins with the id `parked` handed out, then syncs and heartbeats as the
/// group's only member, and so its leader.
async fn lead_alone(member: &impl Member, parked: JoinGroupResponse) -> Result<()> {
    let id = parked.member_id.clone();
    let joined = join(member, Some(parked.into())).await?;
    assert_eq!(i16::from(ErrorCode::None), joined.error_code);
    assert_eq!(id, joined.member_id);
    assert_eq!(joined.leader, joined.member_id);
    assert_eq!(Some(1), members(&joined));

    let synced = sync(member, Some(joined.into())).await?;
    assert!(!synced.assignment.is_empty());
    assert_eq!(i16::from(ErrorCode::None), synced.error_code);

    let beat = heartbeat(member, Some(synced.into())).await?;
    assert_eq!(i16::from(ErrorCode::None), beat.error_code);
    Ok(())
}

/// The assignment of a member holding every partition of `TOPIC`.
fn everything(partitions: Range<i32>) -> MemberAssignment {
    let all = ConsumerProtocolAssignment::default().assigned_partitions([(TOPIC, partitions)]);
    let v3 = ConsumerProtocolSubscription::V3;
    MemberAssignment::default().version(v3).assignment(all)
}

/// Every partition assigned to one of `members`, once per member holding it.
fn assigned<'a, S: 'a>(
    members: impl IntoIterator<Item = &'a ConsumerGroupService<S>>,
) -> Result<Vec<(String, i32)>> {
    let mut held = Vec::new();
    for member in members {
        let topics = member.member_assignment()?.into_iter();
        for topic in topics.flat_map(|ours| ours.assignment.assigned_partitions) {
            for partition in topic.partitions {
                held.push((topic.topic.clone(), partition));
            }
        }
    }
    Ok(held)
}

/// Asserts that `held` is each of `partitions` exactly once.
fn assert_each_once(held: &[(String, i32)], partitions: usize) {
    assert_eq!(partitions, held.len(), "assigned: {held:?}");
    let distinct = held.iter().collect::<HashSet<_>>();
    assert_eq!(held.len(), distinct.len(), "assigned twice: {held:?}");
}

/// How one member takes part in a run.
struct Script {
    /// The member's position in its group, reported on `finished`.
    index: usize,
    rounds: usize,
    /// Whether this member cancels `done` once its rounds are up.
    ends_run: bool,
    finished: Sender<usize>,
    done: tokio_util::sync::CancellationToken,
}

impl Script {
    /// Answers whatever `member` asks `rounds` times, or until `done` is
    /// cancelled, then reports `index` on `finished`.
    async fn play(self, member: &impl Member) -> Result<()> {
        let mut previous = None;
        for _ in 0..self.rounds {
            if self.done.is_cancelled() {
                break;
            }
            previous = Some(next(member, previous).await?);
        }

        _ = self.finished.send(self.index).await;
        if self.ends_run && !self.done.is_cancelled() {
            self.done.cancel();
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn rejoin_after_the_session_of_the_parked_id_expires() -> Result<()> {
    let _guard = init_tracing()?;

    let c0 = member(coordinator().await?, 0, LATENCY_MS, topic(0..3));
    let parked = first_join(&c0).await?;

    let session = c0.session_timeout()?;
    task::yield_now().await;
    time::advance(session).await;
    task::yield_now().await;

    lead_alone(&c0, parked).await?;
    assert_eq!(Some(everything(0..3)), c0.member_assignment()?);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_lone_member_leads_and_is_assigned_everything() -> Result<()> {
    let _guard = init_tracing()?;

    let c0 = member(coordinator().await?, 0, LATENCY_MS, topic(0..3));
    let parked = first_join(&c0).await?;
    lead_alone(&c0, parked).await?;
    assert_eq!(Some(everything(0..3)), c0.member_assignment()?);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn two_members_split_the_partitions() -> Result<()> {
    let _guard = init_tracing()?;

    let route = coordinator().await?;
    let pair = [
        member(route.clone(), 0, LATENCY_MS, topic(0..3)),
        member(route, 1, LATENCY_MS, topic(0..3)),
    ];

    let (finished, _reports) = mpsc::channel(pair.len());
    let done = tokio_util::sync::CancellationToken::new();
    let mut running = JoinSet::new();
    for (id, c) in pair.iter().cloned().enumerate() {
        let (finished, done) = (finished.clone(), done.clone());
        let script = Script {
            index: id,
            rounds: 10,
            ends_run: true,
            finished,
            done,
        };
        _ = running.spawn(async move { script.play(&c).await });
    }
    for outcome in running.join_all().await {
        outcome?;
    }

    assert_each_once(&assigned(&pair)?, 3);
    Ok(())
}

/// Polls every second until the permanent members agree on one generation
/// and have each heartbeated in it a few times, and answers that generation.
async fn settled<S>(permanent: &[ConsumerGroupService<S>]) -> i32 {
    const POLL: Duration = Duration::from_secs(1);

    // Stability is "every permanent consumer has heartbeated a few times **in one
    // generation**", not "every permanent consumer has heartbeated a few times"
    // (#195).
    //
    // The heartbeat count alone says each consumer is alive; it says nothing about
    // whether they are alive in the *same* rebalance. A transient consumer
    // rejoining bumps the generation, and the assignments this test asserts on —
    // no partition twice, every partition once — are properties of one settled
    // generation. Sampled across two, they legitimately overlap or leave a gap,
    // which is the whole of this test's non-determinism: it failed under load and
    // passed on a re-run of the same commit.
    let mut settled_generation = None;

    loop {
        sleep(POLL).await;

        // One generation shared by every permanent consumer, or nothing.
        let generation = permanent
            .iter()
            .map(|member| member.generation().unwrap_or_default())
            .reduce(|acc, generation| acc.filter(|acc| Some(acc) == generation.as_ref()))
            .flatten();

        let heartbeating = permanent.iter().all(|member| {
            let beats = member.stable_heartbeat_count().unwrap_or_default();
            beats.is_some_and(|beats| beats > 2)
        });

        // Require the agreed generation to hold across two consecutive polls: a
        // rebalance that starts between the check and the cancel would otherwise
        // still be sampled in flight.
        if let Some(agreed) = generation.filter(|_| heartbeating)
            && generation == settled_generation
        {
            return agreed;
        }
        settled_generation = generation;
    }
}

/// `count` members over 32 partitions. The even ones are transient: each
/// leaves after between 5 and 10 rounds, drawn from its own seeded generator.
/// The odd ones are permanent, and once the group has settled they hold every
/// partition exactly once.
async fn churn(count: usize) -> Result<()> {
    const PARTITIONS: i32 = 32;
    const ROUNDS: Range<usize> = 5..10;

    let route = coordinator().await?;
    let group: Vec<_> = (0..count)
        .map(|id| member(route.clone(), id as u64, 10..150, topic(0..PARTITIONS)))
        .collect();
    let is_transient = |id: usize| id.is_multiple_of(2);

    let (finished, mut reports) = mpsc::channel(group.len());
    let done = tokio_util::sync::CancellationToken::new();
    let mut running = JoinSet::new();

    for (id, c) in group.iter().cloned().enumerate() {
        let rounds = match is_transient(id) {
            true => SmallRng::seed_from_u64(id as u64).random_range(ROUNDS),
            false => ROUNDS.end * 4,
        };
        let (finished, done) = (finished.clone(), done.clone());
        let script = Script {
            index: id,
            rounds,
            ends_run: rounds > ROUNDS.end,
            finished,
            done,
        };
        _ = running.spawn(async move { script.play(&c).await });
    }

    let mut transients = (0..count)
        .filter(|id| is_transient(*id))
        .collect::<BTreeSet<_>>();
    let permanent: Vec<_> = (0..count)
        .filter(|id| !is_transient(*id))
        .map(|id| group[id].clone())
        .collect();
    let wave_over = Arc::new(Notify::new());

    _ = running.spawn({
        let (wave_over, done) = (wave_over.clone(), done.clone());
        async move {
            while let Some(Some(id)) = done.run_until_cancelled(reports.recv()).await {
                let was_transient = transients.remove(&id);
                if was_transient && transients.is_empty() {
                    wave_over.notify_one();
                }
            }
            Ok(())
        }
    });

    _ = running.spawn({
        let (first, permanent) = (group[0].clone(), permanent.clone());
        async move {
            wave_over.notified().await;
            sleep(first.session_timeout()? * 2).await;
            let generation = settled(&permanent).await;
            debug!(generation);
            done.cancel();
            Ok(())
        }
    });

    for outcome in running.join_all().await {
        outcome?;
    }

    assert_each_once(&assigned(&permanent)?, PARTITIONS as usize);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn churn_of_08_members() -> Result<()> {
    let _guard = init_tracing()?;
    churn(8).await
}

#[tokio::test(start_paused = true)]
async fn churn_of_16_members() -> Result<()> {
    let _guard = init_tracing()?;
    churn(16).await
}

#[tokio::test(start_paused = true)]
async fn churn_of_24_members() -> Result<()> {
    let _guard = init_tracing()?;
    churn(24).await
}

#[tokio::test(start_paused = true)]
async fn churn_of_32_members() -> Result<()> {
    let _guard = init_tracing()?;
    churn(32).await
}

#[tokio::test(start_paused = true)]
async fn two_members_joining_in_one_window_share_a_generation() -> Result<()> {
    let _guard = init_tracing()?;

    let route = coordinator().await?;
    let c0 = member(route.clone(), 0, LATENCY_MS, topic(0..3));
    let c1 = member(route, 0, LATENCY_MS, topic(0..3));

    let c0_parked = first_join(&c0).await?;
    let c0_member_id = c0_parked.member_id.clone();
    let c1_parked = first_join(&c1).await?;
    let c1_member_id = c1_parked.member_id.clone();

    // KIP-394: the empty joins above only parked the generated member ids —
    // neither consumer is registered yet (previously each empty join
    // registered a phantom member and bumped the generation). For both
    // consumers to land in the same generation, their real joins must overlap
    // inside the same rebalance window: the join-window barrier holds the
    // leader's JoinGroup response until membership is quiescent, so it
    // carries the complete member list.
    //
    // c1: join with member id, in flight first so that c1 registers first
    // and becomes the leader
    //
    let c1_join = {
        let c1 = c1.clone();
        tokio::spawn(async move { join(&c1, Some(c1_parked.into())).await })
    };

    // give c1 a head start (well past the maximum introduced latency), then
    // join c0 while c1 is still held inside the rebalance window
    //
    sleep(Duration::from_millis(500)).await;

    let c0_join = {
        let c0 = c0.clone();
        tokio::spawn(async move { join(&c0, Some(c0_parked.into())).await })
    };

    let led = c1_join.await??;
    assert_eq!(i16::from(ErrorCode::None), led.error_code);
    assert_eq!(1, led.generation_id);
    assert_eq!(c1_member_id, led.leader);
    assert_eq!(Some(2), members(&led));
    assert_eq!(c1_member_id, led.member_id);

    let c1_synced = sync(&c1, Some(led.into())).await?;
    assert!(!c1_synced.assignment.is_empty());
    assert_eq!(i16::from(ErrorCode::None), c1_synced.error_code);

    // c0: join with member id — a follower of generation 1, released from
    // the join long-poll once the leader's assignment lands (or at the
    // long-poll cap; either way the response below is the same)
    //
    let followed = c0_join.await??;
    assert_eq!(i16::from(ErrorCode::None), followed.error_code);
    assert_eq!(1, followed.generation_id);
    assert_eq!(c1_member_id, followed.leader);
    assert_eq!(Some(0), members(&followed));
    assert_eq!(c0_member_id, followed.member_id);

    let c0_synced = sync(&c0, Some(followed.into())).await?;
    assert!(!c0_synced.assignment.is_empty());
    assert_eq!(i16::from(ErrorCode::None), c0_synced.error_code);

    let c0_beat = heartbeat(&c0, Some(c0_synced.into())).await?;
    assert_eq!(i16::from(ErrorCode::None), c0_beat.error_code);
    let c1_beat = heartbeat(&c1, Some(c1_synced.into())).await?;
    assert_eq!(i16::from(ErrorCode::None), c1_beat.error_code);

    assert_each_once(&assigned([&c0, &c1])?, 3);
    Ok(())
}
