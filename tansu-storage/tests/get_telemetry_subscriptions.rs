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

use crate::common::{Error, cluster_id, init_tracing, storage_url};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use tansu_sans_io::{ErrorCode, GetTelemetrySubscriptionsRequest};
use tansu_storage::{GetTelemetrySubscriptionsService, StorageContainer};
use url::Url;

mod common;

#[tokio::test]
async fn req() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(storage_url()?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(GetTelemetrySubscriptionsService);

    let client_instance_id = [0; 16];

    let response = service
        .serve(
            Context::default(),
            GetTelemetrySubscriptionsRequest::default().client_instance_id(client_instance_id),
        )
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

    // "must be set to 0 on the first request" — so this one is assigned an id.
    assert_ne!([0; 16], response.client_instance_id);

    Ok(())
}

/// KIP-714's two halves of the same sentence (#410).
///
/// The message definition says `ClientInstanceId` is the "assigned client
/// instance id if ClientInstanceId was 0 in the request, **else 0**". A fresh
/// `Uuid::new_v4()` was minted on every request, so a client that identified
/// itself with the id this broker had given it was told, every five seconds,
/// that its id had changed — which is the one thing the field exists to prevent.
#[tokio::test]
async fn an_already_assigned_instance_id_is_not_reassigned() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(GetTelemetrySubscriptionsService);

    let assigned = service
        .serve(
            Context::default(),
            GetTelemetrySubscriptionsRequest::default().client_instance_id([0; 16]),
        )
        .await?
        .client_instance_id;

    assert_ne!([0; 16], assigned, "a zero id is assigned one");

    // The client comes back carrying it. It is already identified, so there is
    // nothing to assign.
    let returning = service
        .serve(
            Context::default(),
            GetTelemetrySubscriptionsRequest::default().client_instance_id(assigned),
        )
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(returning.error_code)?);
    assert_eq!(
        [0; 16], returning.client_instance_id,
        "an id the client already holds must not be reassigned",
    );

    Ok(())
}

/// The broker's own instruction is what sets the api_key 71 rate (#410).
///
/// `PushIntervalMs` is how long a client waits before asking again, and with
/// `requested_metrics` empty there is nothing for it to come back to except the
/// same answer. Hard-coded at 5 000 ms it produced **107.9 req/s — 20 % of all
/// Kafka API traffic** on the production fleet, second only to `Fetch` and 36x
/// the produce rate: ~540 clients each obeying a five-second back-off.
///
/// Pinned as a floor rather than an exact value so the number can be tuned, but
/// not back into seconds by accident.
#[tokio::test]
async fn a_client_with_no_subscription_is_told_to_wait_minutes_not_seconds() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(GetTelemetrySubscriptionsService);

    let response = service
        .serve(
            Context::default(),
            GetTelemetrySubscriptionsRequest::default().client_instance_id([0; 16]),
        )
        .await?;

    assert_eq!(
        Some(0),
        response.requested_metrics.as_ref().map(Vec::len),
        "no metrics are subscribed, so the interval is a back-off",
    );
    assert_eq!(0, response.subscription_id);

    assert!(
        response.push_interval_ms >= 60_000,
        "a client with nothing to push must not be told to come back in \
         {}ms — that cadence is the broker's own and it was 20% of all API \
         traffic",
        response.push_interval_ms,
    );

    Ok(())
}
