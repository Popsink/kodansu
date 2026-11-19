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

// Every case here builds a `memory://` container, which needs the one storage
// engine this fork ships. The gate was on `mod in_memory` until #552 dissolved
// it; with the module gone it belongs to the file.
#![cfg(feature = "dynostore")]
//! Dynamic group members, without a `group.instance.id`: a join without a
//! member id is told to come back with one, and two members form, rebalance,
//! lose a member and reform a group.

use common::{CONSOLE_CLIENT, CONSUMER_PROTOCOL, Joined, RANGE_ASSIGNOR, STICKY_ASSIGNOR};
use rand::RngExt as _;
use tansu_broker::coordinator::group::administrator::Controller;
use tansu_sans_io::ErrorCode;
use tansu_sans_io::join_group_request::JoinGroupRequestProtocol;
use tansu_sans_io::sync_group_request::SyncGroupRequestAssignment;

pub mod common;

type Engine = std::sync::Arc<dyn tansu_storage::Storage>;
type Bytes = bytes::Bytes;

/// The coordinator of a `memory://` engine with one broker registered.
async fn coordinator() -> tansu_broker::Result<Controller<Engine>> {
    let cluster = uuid::Uuid::now_v7();
    let node = rand::rng().random_range(0..i32::MAX);
    let listener = url::Url::parse("tcp://127.0.0.1/")?;
    let engine = common::storage_container(cluster, node, listener).await?;
    common::register_broker(cluster, node, engine.clone()).await?;
    Controller::with_storage(engine)
}

fn code(error_code: i16) -> tansu_broker::Result<ErrorCode> {
    ErrorCode::try_from(error_code).map_err(Into::into)
}

/// `assignment` handed to `member` by the leader.
fn handed(member: &Joined, assignment: &Bytes) -> SyncGroupRequestAssignment {
    let handed = SyncGroupRequestAssignment::default().member_id(member.id.clone());
    handed.assignment(assignment.clone())
}

/// A randomly named dynamic group on its own coordinator.
struct Group {
    controller: Controller<Engine>,
    id: String,
    session_ms: i32,
    rebalance_ms: Option<i32>,
}

impl Group {
    /// Joins as a new member, or rejoins as `member` with its protocols.
    async fn join(&mut self, member: Option<&Joined>) -> tansu_broker::Result<Joined> {
        let id = member.map(|member| member.id.as_str());
        let protocols = member.map(|member| member.protocols.clone());
        let joining = common::join(
            &mut self.controller,
            &self.id,
            id,
            None,
            protocols,
            self.session_ms,
            self.rebalance_ms,
        );
        let joined = joining.await?;
        tracing::debug!(?joined);
        Ok(joined)
    }

    /// Syncs `member` under range, asserting the protocol in the answer, and
    /// returns its error code and assignment.
    async fn sync(
        &mut self,
        member: &Joined,
        assignments: &[SyncGroupRequestAssignment],
    ) -> tansu_broker::Result<(ErrorCode, Bytes)> {
        let (generation, id) = (member.generation, member.id.as_str());
        let syncing = common::sync_group(
            &mut self.controller,
            &self.id,
            generation,
            id,
            None,
            CONSUMER_PROTOCOL,
            RANGE_ASSIGNOR,
            assignments,
        );
        let synced = syncing.await?;
        let protocol = (
            synced.protocol_type.as_deref(),
            synced.protocol_name.as_deref(),
        );
        assert_eq!((Some(CONSUMER_PROTOCOL), Some(RANGE_ASSIGNOR)), protocol);
        Ok((code(synced.error_code)?, synced.assignment))
    }

    async fn heartbeat(&mut self, member: &Joined) -> tansu_broker::Result<ErrorCode> {
        let (generation, id) = (member.generation, member.id.as_str());
        let beating = common::heartbeat(&mut self.controller, &self.id, generation, id, None);
        code(beating.await?.error_code)
    }

    async fn leave(&mut self, member: &Joined) -> tansu_broker::Result<ErrorCode> {
        let leaving = common::leave(&mut self.controller, &self.id, member.id.as_str(), None);
        code(leaving.await?.error_code)
    }
}

#[tokio::test]
async fn a_join_without_a_member_id_is_told_to_bring_one() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let mut controller = coordinator().await?;
    let group = common::alphanumeric_string(15);
    tracing::debug!(group_id = ?group);

    let offer = |name: &str, metadata| {
        let protocol = JoinGroupRequestProtocol::default().name(name.into());
        protocol.metadata(Bytes::from_static(metadata))
    };
    let range = offer(RANGE_ASSIGNOR, b"first_member_range_meta_01");
    let offers = [
        range,
        offer(STICKY_ASSIGNOR, b"first_member_sticky_meta_01"),
    ];
    let (client, offered) = (Some(CONSOLE_CLIENT), Some(&offers[..]));
    let (session_ms, rebalance_ms, consumer) = (45_000, Some(300_000), CONSUMER_PROTOCOL);
    let coordinating = &mut controller;
    let joining = common::join_group(
        coordinating,
        client,
        &group,
        session_ms,
        rebalance_ms,
        "",
        None,
        consumer,
        offered,
        None,
    );
    let refused = joining.await?;

    assert_eq!(ErrorCode::MemberIdRequired, code(refused.error_code)?);
    let protocol = (
        refused.protocol_type.as_deref(),
        refused.protocol_name.as_deref(),
    );
    assert_eq!((Some(CONSUMER_PROTOCOL), Some("")), protocol);
    assert_eq!("", refused.leader);
    assert!(
        refused.member_id.starts_with(CONSOLE_CLIENT),
        "{}",
        refused.member_id
    );
    assert_eq!(Some(0), refused.members.as_ref().map(Vec::len));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn two_members_form_rebalance_and_reform_after_a_leave() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;

    // Far beyond any plausible run of this test, deliberately (#195).
    //
    // At 45s it was within reach: this test takes 32-46s, and `missed_heartbeat`
    // evicts a member whose `last_contact` is older than the session timeout,
    // against the real clock. When the evicted member is the leader, the leader
    // slot is cleared and the next join elects the *other* member — so a slow run
    // silently swaps the roles these assertions are written around. That is why two
    // consecutive failures reported different assertions: whichever divergence the
    // eviction reached first.
    //
    // Session expiry is not what this test covers, so the fix is to put it out of
    // reach rather than to chase the wall clock.
    let mut group = Group {
        controller: coordinator().await?,
        id: common::alphanumeric_string(15),
        session_ms: 600_000,
        rebalance_ms: Some(900_000),
    };
    tracing::debug!(group_id = ?group.id);

    let first = group.join(None).await?;
    let alone = common::random_bytes(15);
    let formed = group.sync(&first, &[handed(&first, &alone)]).await?;
    assert_eq!((ErrorCode::None, alone.clone()), formed);
    assert_eq!(ErrorCode::None, group.heartbeat(&first).await?);

    let second = group.join(None).await?;
    let (told, _) = group.sync(&second, &[handed(&first, &alone)]).await?;
    assert_eq!(ErrorCode::RebalanceInProgress, told);
    let stale = group.heartbeat(&first).await?;
    assert_eq!(ErrorCode::RebalanceInProgress, stale);

    let second = group.join(Some(&second)).await?;
    assert_eq!(first.id, second.leader);
    let first = group.join(Some(&first)).await?;
    assert!(first.leads());
    assert_eq!(first.generation, second.generation);

    let (to_first, to_second) = (common::random_bytes(15), common::random_bytes(15));
    let shared = [handed(&first, &to_first), handed(&second, &to_second)];
    let leader_synced = group.sync(&first, &shared).await?;
    assert_eq!((ErrorCode::None, to_first), leader_synced);
    let follower_synced = group.sync(&second, &shared).await?;
    assert_eq!((ErrorCode::None, to_second), follower_synced);
    assert_eq!(ErrorCode::None, group.heartbeat(&first).await?);
    assert_eq!(ErrorCode::None, group.heartbeat(&second).await?);

    assert_eq!(ErrorCode::None, group.leave(&first).await?);
    let left_behind = group.heartbeat(&second).await?;
    assert_eq!(ErrorCode::RebalanceInProgress, left_behind);

    let second = group.join(Some(&second)).await?;
    assert!(second.leads());
    let alone = common::random_bytes(15);
    let reformed = group.sync(&second, &[handed(&second, &alone)]).await?;
    assert_eq!((ErrorCode::None, alone), reformed);
    assert_eq!(ErrorCode::None, group.heartbeat(&second).await?);
    Ok(())
}
