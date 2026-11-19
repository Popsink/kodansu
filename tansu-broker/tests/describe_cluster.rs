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

//! `DescribeCluster` served against a `memory://` engine with one broker
//! registered: that broker is the controller and the only broker listed.

use anyhow::Result;
use rama::Context;
use rama::Service as _;
use rand::RngExt as _;
use tansu_sans_io::describe_cluster_response::DescribeClusterBroker;
use tansu_sans_io::{DescribeClusterRequest, ErrorCode};
use tansu_storage::DescribeClusterService;

pub mod common;

/// `cluster_authorized_operations` when the broker reports none: `i32::MIN`,
/// Kafka's sentinel for "omitted".
const OPERATIONS_OMITTED: i32 = i32::MIN;

#[tokio::test]
async fn the_registered_broker_is_the_controller_and_the_only_broker() -> Result<()> {
    let _guard = common::init_tracing()?;

    let cluster = uuid::Uuid::now_v7();
    let node = rand::rng().random_range(0..i32::MAX);
    let listener = url::Url::parse("tcp://example.com:9092/")?;
    tracing::debug!(node, %listener);

    let storage = common::storage_container(cluster, node, listener.clone()).await?;
    common::register_broker(cluster, node, storage.clone()).await?;

    let request = DescribeClusterRequest::default().include_cluster_authorized_operations(true);
    let request = request.endpoint_type(Some(6));
    let described = DescribeClusterService.serve(Context::with_state(storage), request);
    let response = described.await?;

    let listed = DescribeClusterBroker::default().broker_id(node).rack(None);
    let listed = listed.host(listener.host_str().map(String::from).unwrap_or_default());
    let listed = listed.port(listener.port().map(i32::from).unwrap_or_default());

    assert_eq!(0, response.throttle_time_ms);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    assert_eq!(None, response.error_message);
    assert_eq!(node, response.controller_id);
    assert_eq!(OPERATIONS_OMITTED, response.cluster_authorized_operations);
    assert_eq!(Some(vec![listed]), response.brokers);

    Ok(())
}
