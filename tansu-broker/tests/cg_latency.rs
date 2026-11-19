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

//! One member, driven by [`GroupConsumer`], through the group coordinator
//! behind seeded wire latency: join, sync, then the heartbeats of a group that
//! has settled, which must leave `generation.json` alone.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use anyhow::Result;
use rama::Context;
use rama::Layer as _;
use rama::Service as _;
use tansu_broker::NODE_ID;
use tansu_broker::coordinator::group::administrator::Controller;
use tansu_broker::service::coordinator::services;
use tansu_sans_io::consumer::DynConsumerAssignment;
use tansu_sans_io::consumer::GroupConsumer;
use tansu_sans_io::consumer::MemberAssignment;
use tansu_sans_io::metadata_response::MetadataResponsePartition;
use tansu_sans_io::metadata_response::MetadataResponseTopic;
use tansu_sans_io::{ErrorCode, HeartbeatRequest, JoinGroupRequest};
use tansu_sans_io::{MetadataResponse, SyncGroupRequest};
use tansu_service::{BytesFrameLayer, FrameBytesLayer, FrameRouteService};
use tansu_service::{LatencyIntroducingLayer, RequestFrameLayer};
use tansu_storage::LatencyIntroducingStorage;

pub mod common;

const GROUP: &str = "abc";
const TOPIC: &str = "pqr";

type Route = FrameRouteService<(), tansu_broker::Error>;

/// What the member is told the cluster holds: [`TOPIC`] in three partitions.
fn three_partitions() -> MetadataResponse {
    let partitions = (0..3).map(|i| MetadataResponsePartition::default().partition_index(i));
    let topic = MetadataResponseTopic::default().name(Some(TOPIC.into()));
    let topic = topic.partitions(Some(partitions.collect()));
    MetadataResponse::default().topics(Some(vec![topic]))
}

fn code(error_code: i16) -> Result<ErrorCode> {
    ErrorCode::try_from(error_code).map_err(Into::into)
}

#[tokio::test]
async fn a_settled_member_heartbeats_without_rewriting_its_generation() -> Result<()> {
    let _guard = common::init_tracing()?;

    let listener = "tcp://127.0.0.1:9092/".parse()?;
    let engine = common::storage_container("tansu", NODE_ID, listener).await?;

    // #111: this wrapper is here to count the writes of `generation.json`, the
    // one object a group's members contend on. Its own delay is next to none,
    // because the wire layer below already brings the latency.
    let storage = LatencyIntroducingStorage::new(engine).with_latency(0..1);
    let generation_writes = storage.generation_updates_handle();
    let groups = Controller::with_storage(storage)?;

    let routes = services(Route::builder(), groups)?;
    let framed = (FrameBytesLayer, BytesFrameLayer::default()).into_layer(routes.build()?);
    let wire = LatencyIntroducingLayer::default().with_seed(0);
    let wire = wire.with_latency_millis(50..150);
    let sut = (RequestFrameLayer, wire).into_layer(framed);
    let ctx = Context::default();

    let hook: Arc<DynConsumerAssignment> =
        Arc::new(|group, assignment| tracing::debug!(group, %assignment));
    let consumer = GroupConsumer::builder(GROUP).topics([TOPIC]);
    let consumer = consumer.metadata(three_partitions());
    let mut consumer = consumer.on_assignment(Some(hook)).build();

    let anonymous = JoinGroupRequest::try_from(consumer.next_action(None)?)?;
    assert_eq!("", anonymous.member_id);
    let refused = sut.serve(ctx.clone(), anonymous).await?;
    assert_eq!(ErrorCode::MemberIdRequired, code(refused.error_code)?);
    assert_ne!("", refused.member_id);

    let named = JoinGroupRequest::try_from(consumer.next_action(Some(refused.into()))?)?;
    assert_ne!("", named.member_id);
    let joined = sut.serve(ctx.clone(), named).await?;
    assert_eq!(ErrorCode::None, code(joined.error_code)?);
    assert_eq!(
        (0, &joined.member_id),
        (joined.generation_id, &joined.leader)
    );
    assert_eq!(Some(1), joined.members.as_ref().map(Vec::len));

    let leader_sync = SyncGroupRequest::try_from(consumer.next_action(Some(joined.into()))?)?;
    assert_eq!(
        (GROUP, 0),
        (leader_sync.group_id.as_str(), leader_sync.generation_id)
    );
    assert_eq!(Some(1), leader_sync.assignments.as_ref().map(Vec::len));
    let synced = sut.serve(ctx.clone(), leader_sync).await?;
    assert_eq!(ErrorCode::None, code(synced.error_code)?);

    let assigned = MemberAssignment::try_from(synced.assignment.clone())?;
    tracing::debug!(?assigned);
    assert_ne!(0, assigned.assignment.assigned_partitions.len());

    let beat = HeartbeatRequest::try_from(consumer.next_action(Some(synced.into()))?)?;
    assert_eq!(0, beat.generation_id);

    // #111's actual claim, false until #273 and structural since #359: a
    // steady-state heartbeat writes ZERO tier-1 PUTs on the contended object.
    //
    // The old skip compared `GroupDetail` for strict equality, and `GroupMember`
    // derives `PartialEq` over `last_contact`, which the heartbeat path assigned
    // `now` before the comparison. So it could never hold, and every heartbeat
    // cost 1 GET + 1 CAS PUT per member per interval — ~100 PUT/s at 300 members
    // fleet-wide, precisely what the code comment asserted it avoided. The
    // claim no longer rests on a skip being right: liveness is a different
    // object now, so a heartbeat has nothing to write here.
    //
    // The group is Stable and this member has just been assigned, so nothing
    // about the composition changes from here.
    let settled = generation_writes.load(Relaxed);

    for _ in 0..3 {
        let answer = sut.serve(ctx.clone(), beat.clone()).await?;
        assert_eq!(ErrorCode::None, code(answer.error_code)?);
    }

    assert_eq!(
        settled,
        generation_writes.load(Relaxed),
        "a steady-state heartbeat must not rewrite generation.json (#111/#273/#359)",
    );

    Ok(())
}
