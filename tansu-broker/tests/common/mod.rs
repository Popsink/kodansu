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

//! What the broker tests share: a log file per test, the `memory://` engine,
//! random names and bytes, broker registration, and the group protocol calls
//! a consumer makes, answered as typed responses.

#![allow(dead_code, reason = "each test binary uses part of this")]

use std::fs::File;
use std::sync::Arc;
use std::thread;

use rand::RngExt as _;
use rand::distr::{Alphanumeric, StandardUniform};
use tansu_broker::coordinator::group::Coordinator as _;
use tansu_broker::coordinator::group::administrator::Controller;
use tansu_sans_io::join_group_request::JoinGroupRequestProtocol;
use tansu_sans_io::join_group_response::JoinGroupResponseMember;
use tansu_sans_io::leave_group_request::MemberIdentity;
use tansu_sans_io::sync_group_request::SyncGroupRequestAssignment;
use tansu_sans_io::{Body, ErrorCode, HeartbeatResponse, JoinGroupResponse};
use tansu_sans_io::{LeaveGroupResponse, SyncGroupResponse};
use tansu_storage::{BrokerRegistrationRequest, StorageContainer};
use tracing::subscriber::DefaultGuard as Guard;

type Engine = Arc<dyn tansu_storage::Storage>;
type Outcome<T> = tansu_broker::Result<T>;

/// The client id every join here carries.
pub(crate) const CONSOLE_CLIENT: &str = "console-consumer";

/// The assignors every member offers, in the order it prefers them.
pub(crate) const RANGE_ASSIGNOR: &str = "range";
pub(crate) const STICKY_ASSIGNOR: &str = "cooperative-sticky";

/// The protocol type of a consumer group.
pub(crate) const CONSUMER_PROTOCOL: &str = "consumer";

/// Logs this test at `debug` for this binary, on top of `RUST_LOG`, into
/// `../logs/<package>/<crate>::<thread>.log`: one file per test.
pub(crate) fn init_tracing() -> Outcome<Guard> {
    let directive = format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?;
    let filter = tracing_subscriber::EnvFilter::from_default_env().add_directive(directive);

    let unnamed = || tansu_broker::Error::Message(String::from("unnamed thread"));
    let current = thread::current();
    let test = current.name().ok_or_else(unnamed)?;
    let (package, binary) = (env!("CARGO_PKG_NAME"), env!("CARGO_CRATE_NAME"));
    let log = File::create(format!("../logs/{package}/{binary}::{test}.log"))?;

    let subscriber = tracing_subscriber::fmt().with_level(true);
    let subscriber = subscriber.with_line_number(true).with_thread_names(false);
    let subscriber = subscriber
        .with_env_filter(filter)
        .with_writer(Arc::new(log));
    Ok(tracing::subscriber::set_default(subscriber.finish()))
}

/// The one storage every broker test runs against.
///
/// This took a `StorageType` and matched on it until #552. The enum had one
/// variant — the PostgreSQL, libSQL, SlateDB and Turso arms left with #96 —
/// and every call site passed `StorageType::InMemory`, so the parameter chose
/// between one thing.
pub(crate) async fn storage_container(
    cluster_id: impl Into<String>,
    node_id: i32,
    listener: url::Url,
) -> Outcome<Engine> {
    let builder = StorageContainer::builder()
        .cluster_id(cluster_id)
        .node_id(node_id);
    let builder = builder.advertised_listener(listener);
    let building = builder.storage(url::Url::parse("memory://")?).build();
    building.await.map_err(Into::into)
}

pub(crate) fn alphanumeric_string(len: usize) -> String {
    let letters = rand::rng().sample_iter(&Alphanumeric).take(len);
    letters.map(char::from).collect()
}

pub(crate) fn random_bytes(length: usize) -> bytes::Bytes {
    let octets = rand::rng().sample_iter(StandardUniform).take(length);
    octets.collect::<Vec<u8>>().into()
}

/// Registers broker `broker_id` of `cluster_id` with `engine`, under a fresh
/// incarnation.
pub(crate) async fn register_broker(
    cluster_id: impl Into<String>,
    broker_id: i32,
    engine: impl tansu_storage::Storage,
) -> Outcome<()> {
    let (incarnation, cluster, rack) = (uuid::Uuid::now_v7(), cluster_id.into(), None);
    let registration = BrokerRegistrationRequest {
        broker_id,
        cluster_id: cluster,
        incarnation_id: incarnation,
        rack,
    };
    let registering = engine.register_broker(registration);
    registering.await.map_err(Into::into)
}

/// `answer` as the response type the call it answers returns.
fn typed<T>(answer: tansu_broker::Result<Body>) -> Outcome<T>
where
    T: TryFrom<Body>,
    tansu_broker::Error: From<T::Error>,
{
    Ok(T::try_from(answer?)?)
}

#[allow(
    clippy::too_many_arguments,
    reason = "one per JoinGroup field a test sets"
)]
pub(crate) async fn join_group(
    coordinator: &mut Controller<Engine>,
    client: Option<&str>,
    group: &str,
    session_ms: i32,
    rebalance_ms: Option<i32>,
    member: &str,
    instance: Option<&str>,
    kind: &str,
    offered: Option<&[JoinGroupRequestProtocol]>,
    why: Option<&str>,
) -> Outcome<JoinGroupResponse> {
    let joining = coordinator.join(
        client,
        group,
        session_ms,
        rebalance_ms,
        member,
        instance,
        kind,
        offered,
        why,
    );
    typed(joining.await)
}

#[allow(
    clippy::too_many_arguments,
    reason = "one per SyncGroup field a test sets"
)]
pub(crate) async fn sync_group(
    coordinator: &mut Controller<Engine>,
    group: &str,
    generation_id: i32,
    member: &str,
    instance: Option<&str>,
    kind: &str,
    protocol: &str,
    handed: &[SyncGroupRequestAssignment],
) -> Outcome<SyncGroupResponse> {
    let (kind, protocol, handed) = (Some(kind), Some(protocol), Some(handed));
    let syncing = coordinator.sync(
        group,
        generation_id,
        member,
        instance,
        kind,
        protocol,
        handed,
    );
    typed(syncing.await)
}

pub(crate) async fn heartbeat(
    coordinator: &mut Controller<Engine>,
    group: &str,
    generation_id: i32,
    member: &str,
    instance: Option<&str>,
) -> Outcome<HeartbeatResponse> {
    let beating = coordinator.heartbeat(group, generation_id, member, instance);
    typed(beating.await)
}

/// Leaves `group` as `member`, as a consumer does when it is closed.
pub(crate) async fn leave(
    coordinator: &mut Controller<Engine>,
    group: &str,
    member: &str,
    instance: Option<&str>,
) -> Outcome<LeaveGroupResponse> {
    let leaving = MemberIdentity::default().member_id(member.into());
    let leaving = leaving.group_instance_id(instance.map(String::from));
    let leaving = leaving.reason(Some("the consumer is being closed".into()));
    let leavers = [leaving];
    let left = coordinator.leave(group, None, Some(&leavers));
    typed(left.await)
}

/// Where a join left a member: its id, its generation, the leader it was
/// told of (itself when it leads), the members a leader is told of, and the
/// protocols it offered, to offer again on a rejoin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Joined {
    pub(crate) id: String,
    pub(crate) generation: i32,
    pub(crate) leader: String,
    pub(crate) members: Vec<JoinGroupResponseMember>,
    pub(crate) protocols: Vec<JoinGroupRequestProtocol>,
}

impl Joined {
    pub(crate) fn leads(&self) -> bool {
        self.id == self.leader
    }
}

/// Joins `group` as the console consumer, offering `protocols` or else range
/// and cooperative-sticky with random metadata.
///
/// A dynamic member with no id is first refused with `MemberIdRequired` and
/// the id to come back with, and rejoins with it. Either way the answer is
/// asserted to be range under `consumer`, and a follower is told no members.
pub(crate) async fn join(
    coordinator: &mut Controller<Engine>,
    group: &str,
    member: Option<&str>,
    instance: Option<&str>,
    offers: Option<Vec<JoinGroupRequestProtocol>>,
    session_ms: i32,
    rebalance_ms: Option<i32>,
) -> Outcome<Joined> {
    let offer = |name: &str| {
        let protocol = JoinGroupRequestProtocol::default().name(name.into());
        protocol.metadata(random_bytes(15))
    };
    let offers = offers.unwrap_or_else(|| vec![offer(RANGE_ASSIGNOR), offer(STICKY_ASSIGNOR)]);
    let (client, id) = (Some(CONSOLE_CLIENT), member.unwrap_or_default());
    let joining = join_group(
        coordinator,
        client,
        group,
        session_ms,
        rebalance_ms,
        id,
        instance,
        CONSUMER_PROTOCOL,
        Some(&offers[..]),
        None,
    );
    let answer = joining.await?;
    let outcome = ErrorCode::try_from(answer.error_code)?;

    if member.is_none() && instance.is_none() {
        assert_eq!(ErrorCode::MemberIdRequired, outcome);
        assert_eq!(
            (Some(""), ""),
            (answer.protocol_name.as_deref(), answer.leader.as_str())
        );
        assert!(
            answer.member_id.starts_with(CONSOLE_CLIENT),
            "{}",
            answer.member_id
        );
        assert_eq!(0, answer.members.unwrap_or_default().len());
        let given = Some(answer.member_id.as_str());
        let again = join(
            coordinator,
            group,
            given,
            instance,
            Some(offers),
            session_ms,
            rebalance_ms,
        );
        return Box::pin(again).await;
    }

    assert_eq!(ErrorCode::None, outcome);
    let chosen = (
        answer.protocol_type.as_deref(),
        answer.protocol_name.as_deref(),
    );
    assert_eq!((Some(CONSUMER_PROTOCOL), Some(RANGE_ASSIGNOR)), chosen);
    let members = answer.members.unwrap_or_default();
    if answer.member_id != answer.leader {
        assert_eq!(0, members.len());
    }
    Ok(Joined {
        id: answer.member_id,
        generation: answer.generation_id,
        leader: answer.leader,
        members,
        protocols: offers,
    })
}

/// A wall-clock reading derived from `tokio`'s clock, for a test that runs
/// under `#[tokio::test(start_paused = true)]` (#359).
///
/// `tokio`'s paused clock advances `Instant` and `sleep` — by auto-advance when
/// every task is idle, and by `advance()` — but it cannot touch `SystemTime`.
/// A coordinator comparing a member's session against `SystemTime::now()`
/// therefore saw a frozen wall clock while its own long-poll raced ahead, so a
/// test could only buy determinism by waiting out the real duration. Handing
/// this to [`Controller::with_now`] makes the two agree, and the whole `new_cg`
/// binary drops from just under a minute to the time it takes to schedule.
///
/// The origin is captured on first use rather than being a constant: `Instant`
/// has no absolute zero, and `nextest` runs each test in its own process, so
/// "first use" is this test's start.
pub(crate) fn paused_clock() -> std::time::SystemTime {
    use std::sync::OnceLock;
    use std::time::{Duration, SystemTime};

    use tokio::time::Instant;

    /// Far enough from the epoch that a duration subtracted from a reading —
    /// a session timeout, a rebalance timeout — stays representable.
    const ORIGIN: Duration = Duration::from_secs(1_700_000_000);

    static STARTED: OnceLock<Instant> = OnceLock::new();

    let since_start = Instant::now().duration_since(*STARTED.get_or_init(Instant::now));
    SystemTime::UNIX_EPOCH + ORIGIN + since_start
}
