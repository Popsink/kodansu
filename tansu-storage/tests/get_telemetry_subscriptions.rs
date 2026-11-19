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

//! `GetTelemetrySubscriptions`: the instance id a client is given and keeps,
//! and how long it is told to wait.

use tansu_sans_io::{
    ErrorCode, GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse,
};
use tansu_storage::GetTelemetrySubscriptionsService;

use crate::common::{Error, init_tracing, serve, storage};

mod common;

/// The id a client presents before it has been given one.
const UNASSIGNED: [u8; 16] = [0; 16];

/// A client is answered with the id it presents (#515).
///
/// Two ways to get this wrong, and this fork has shipped both. Before #410 a
/// fresh `Uuid::new_v4()` was minted on every request, so a client that
/// identified itself with the id this broker had given it was told, every five
/// seconds, that its id had changed — the one thing the field exists to
/// prevent. #446 then implemented the message definition's "assigned client
/// instance id if ClientInstanceId was 0 in the request, **else 0**" verbatim,
/// and that sentence is not obeyable:
/// `ClientTelemetryUtils.validateClientInstanceId` rejects `ZERO_UUID` on
/// *every* response, so a returning client threw `IllegalArgumentException`
/// out of `poll()` — one per Java client per process, from `alpha.5` on.
///
/// This test asserted the zero. It reads as a conformance test and encoded the
/// misreading, so it would have passed forever. Both halves are pinned now:
/// the id is neither reassigned nor zeroed.
#[tokio::test]
async fn an_already_assigned_instance_id_is_not_reassigned() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let ask = |id| GetTelemetrySubscriptionsRequest::default().client_instance_id(id);

    let first = serve(&storage, GetTelemetrySubscriptionsService, ask(UNASSIGNED)).await?;
    let assigned = first.client_instance_id;
    assert_ne!(UNASSIGNED, assigned, "a zero id is assigned one");

    // The client comes back carrying it. It is already identified, so there is
    // nothing to assign.
    let returning = serve(&storage, GetTelemetrySubscriptionsService, ask(assigned)).await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(returning.error_code)?);
    assert_eq!(
        assigned, returning.client_instance_id,
        "an id the client already holds must be echoed, not reassigned",
    );
    assert_ne!(
        UNASSIGNED, returning.client_instance_id,
        "a zero id in a response is rejected by the reference client on sight",
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

    let request = GetTelemetrySubscriptionsRequest::default().client_instance_id(UNASSIGNED);
    let response = serve(&storage().await?, GetTelemetrySubscriptionsService, request).await?;

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

/// A client asking without an id is given one.
#[tokio::test]
async fn an_unassigned_client_is_given_an_instance_id() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let request = GetTelemetrySubscriptionsRequest::default().client_instance_id(UNASSIGNED);
    let GetTelemetrySubscriptionsResponse {
        error_code,
        client_instance_id,
        ..
    } = serve(&storage().await?, GetTelemetrySubscriptionsService, request).await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(error_code)?);
    // "must be set to 0 on the first request" — so this one is assigned an id.
    assert_ne!(UNASSIGNED, client_instance_id);

    Ok(())
}
