// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use rama::Layer as _;
use rama::Service as _;
use rama::layer::MapErrLayer;
use rama::layer::MapStateLayer;
use tansu_sans_io::ApiKey as _;
use tansu_sans_io::ConsumerGroupHeartbeatRequest;
use tansu_sans_io::{HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest, SyncGroupRequest};
use tansu_sans_io::{OffsetCommitRequest, OffsetFetchRequest};
use tansu_service::FrameRouteBuilder;

use crate::Error;
use crate::broker::group::consumer_group_heartbeat::ConsumerGroupHeartbeatService;
use crate::broker::group::heartbeat::HeartbeatService;
use crate::broker::group::join::JoinGroupService;
use crate::broker::group::leave::LeaveGroupService;
use crate::broker::group::offset_commit::OffsetCommitService;
use crate::broker::group::offset_fetch::OffsetFetchService;
use crate::broker::group::sync::SyncGroupService;
use crate::coordinator::group::Coordinator;

/// Adds the route for `$request` to `$builder`: `$service`, with
/// `$coordinator` as its state and its errors mapped into the broker's [`Error`].
macro_rules! coordinated {
    ($builder:ident, $coordinator:ident, $request:ident => $service:ident) => {{
        let stateful = MapStateLayer::new(|_| $coordinator);
        let route = (MapErrLayer::new(Error::from), stateful).into_layer($service);
        let routed = $builder.with_route($request::KEY, route.boxed());
        routed.map_err(Error::from)
    }};
}

pub fn services<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    let builder = consumer_group_heartbeat(builder, coordinator.clone())?;
    let builder = heartbeat(builder, coordinator.clone())?;
    let builder = join_group(builder, coordinator.clone())?;
    let builder = leave_group(builder, coordinator.clone())?;
    let builder = offset_commit(builder, coordinator.clone())?;
    let builder = offset_fetch(builder, coordinator.clone())?;
    sync_group(builder, coordinator)
}

/// `ConsumerGroupHeartbeat`, and with it the advertisement that this broker
/// speaks the KIP-848 consumer group protocol (#612): `ApiVersions` is derived
/// from the routes registered here.
pub fn consumer_group_heartbeat<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    coordinated!(builder, coordinator, ConsumerGroupHeartbeatRequest => ConsumerGroupHeartbeatService)
}

pub fn heartbeat<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    coordinated!(builder, coordinator, HeartbeatRequest => HeartbeatService)
}

pub fn join_group<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    coordinated!(builder, coordinator, JoinGroupRequest => JoinGroupService)
}

pub fn leave_group<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    coordinated!(builder, coordinator, LeaveGroupRequest => LeaveGroupService)
}

pub fn offset_commit<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    coordinated!(builder, coordinator, OffsetCommitRequest => OffsetCommitService)
}

pub fn offset_fetch<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    coordinated!(builder, coordinator, OffsetFetchRequest => OffsetFetchService)
}

pub fn sync_group<C>(
    builder: FrameRouteBuilder<(), Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<(), Error>, Error>
where
    C: Coordinator,
{
    coordinated!(builder, coordinator, SyncGroupRequest => SyncGroupService)
}
