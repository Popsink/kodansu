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

//! A static member, one with a `group.instance.id`, joining without a member
//! id: it leads its own generation, and joining again changes nothing.

use common::{CLIENT_ID, COOPERATIVE_STICKY, PROTOCOL_TYPE, RANGE};
use rand::RngExt as _;
use tansu_broker::coordinator::group::administrator::Controller;
use tansu_sans_io::join_group_request::JoinGroupRequestProtocol;
use tansu_sans_io::join_group_response::JoinGroupResponse;

mod common;

type Engine = std::sync::Arc<dyn tansu_storage::Storage>;

/// A `memory://` engine with one broker, at a random node id, registered.
async fn registered() -> tansu_broker::Result<Engine> {
    let cluster = uuid::Uuid::now_v7();
    let node = rand::rng().random_range(0..i32::MAX);
    let listener = url::Url::parse("tcp://127.0.0.1/")?;
    let engine = common::storage_container(cluster, node, listener).await?;
    common::register_broker(cluster, node, engine.clone()).await?;
    Ok(engine)
}

/// Joins `group` as the static member `instance` with an empty member id,
/// offering range and then cooperative-sticky.
async fn join_static(
    controller: &mut Controller<Engine>,
    group: &str,
    instance: &str,
) -> tansu_broker::Result<JoinGroupResponse> {
    let offer = |name: &str, metadata| {
        let protocol = JoinGroupRequestProtocol::default().name(name.into());
        protocol.metadata(bytes::Bytes::from_static(metadata))
    };
    let range = offer(RANGE, b"first_member_range_meta_01");
    let offers = [
        range,
        offer(COOPERATIVE_STICKY, b"first_member_sticky_meta_01"),
    ];
    let (session_ms, rebalance_ms) = (45_000, Some(300_000));
    let (instance, consumer) = (Some(instance), PROTOCOL_TYPE);
    let (client, offered) = (Some(CLIENT_ID), Some(&offers[..]));
    let joining = common::join_group(
        controller,
        client,
        group,
        session_ms,
        rebalance_ms,
        "",
        instance,
        consumer,
        offered,
        None,
    );
    joining.await
}

/// What a static member alone in its group is told: it leads, under range,
/// with an id that starts with its instance id.
fn assert_leads_alone(joined: &JoinGroupResponse, instance: &str) -> tansu_broker::Result<()> {
    let outcome = tansu_sans_io::ErrorCode::try_from(joined.error_code)?;
    assert_eq!(tansu_sans_io::ErrorCode::None, outcome);
    let protocol = (
        joined.protocol_type.as_deref(),
        joined.protocol_name.as_deref(),
    );
    assert_eq!((Some(PROTOCOL_TYPE), Some(RANGE)), protocol);
    assert_eq!(joined.leader, joined.member_id);
    assert!(
        joined.member_id.starts_with(instance),
        "{}",
        joined.member_id
    );
    assert_eq!(Some(1), joined.members.as_ref().map(Vec::len));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_static_member_without_an_id_leads_alone() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let controller = Controller::with_storage(registered().await?)?;
    let mut controller = controller.with_now(common::paused_clock);

    let instance = common::alphanumeric_string(6);
    let group = common::alphanumeric_string(15);
    tracing::debug!(group_id = ?group);

    let joined = join_static(&mut controller, &group, &instance).await?;
    assert_leads_alone(&joined, &instance)
}

#[tokio::test(start_paused = true)]
async fn a_static_member_joining_again_keeps_its_id_and_generation() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let mut controller = Controller::with_storage(registered().await?)?;

    let instance = common::alphanumeric_string(6);
    let group = common::alphanumeric_string(15);
    tracing::debug!(group_id = ?group);

    let joined = join_static(&mut controller, &group, &instance).await?;
    assert_leads_alone(&joined, &instance)?;

    let again = join_static(&mut controller, &group, &instance).await?;
    let outcome = tansu_sans_io::ErrorCode::try_from(again.error_code)?;
    assert_eq!(tansu_sans_io::ErrorCode::None, outcome);
    let protocol = (
        again.protocol_type.as_deref(),
        again.protocol_name.as_deref(),
    );
    assert_eq!((Some(PROTOCOL_TYPE), Some(RANGE)), protocol);
    let identity = (&again.leader, &again.member_id, again.generation_id);
    assert_eq!(
        (&joined.leader, &joined.member_id, joined.generation_id),
        identity
    );
    assert_eq!(Some(1), again.members.as_ref().map(Vec::len));
    Ok(())
}
