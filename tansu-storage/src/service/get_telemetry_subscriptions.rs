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

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, ErrorCode, GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse,
};
use tracing::instrument;
use uuid::Uuid;

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`GetTelemetrySubscriptionsRequest`] returning [`GetTelemetrySubscriptionsResponse`].
/// ```
/// use rama::{Context, Layer as _, Service, layer::MapStateLayer};
/// use tansu_sans_io::{ErrorCode, GetTelemetrySubscriptionsRequest};
/// use tansu_storage::{Error, GetTelemetrySubscriptionsService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
///
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
/// const NODE_ID: i32 = 111;
///
/// let storage = StorageContainer::builder()
///     .cluster_id("tansu")
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://tansu/")?)
///     .build()
///     .await?;
///
/// let service = MapStateLayer::new(|_| storage).into_layer(GetTelemetrySubscriptionsService);
///
/// let client_instance_id = [0; 16];
///
/// let response = service
///     .serve(
///         Context::default(),
///         GetTelemetrySubscriptionsRequest::default().client_instance_id(client_instance_id),
///     )
///     .await?;
///
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
/// # Ok(())
/// # }
/// ```
/// The `ClientInstanceId` a client sends before it has been assigned one.
///
/// It is only ever a *request* value. A response carrying it is rejected by the
/// reference client on sight — see the response's assignment below (#515).
const UNASSIGNED_CLIENT_INSTANCE_ID: [u8; 16] = [0; 16];

/// How long a client is told to wait before asking again when there is **no**
/// subscription for it (#410).
///
/// `PushIntervalMs` is documented as "the lowest configured interval in the
/// current subscription set". There is no subscription set here — nothing in
/// this fork configures one — so this is not a push cadence, it is a back-off:
/// how long until it is worth asking whether an *administrator* has created one.
///
/// It was hard-coded to 5 000 ms, and that is the whole of the fleet's api_key 71
/// traffic. Production, `1.0.0-alpha.4`: **107.9 req/s, 20 % of all Kafka API
/// traffic**, second only to `Fetch` and 36x the produce rate — which is
/// 107.9 x 5 s = ~540 clients each asking once per interval, exactly as told.
/// Every one of those is answered "still nothing", because `requested_metrics`
/// is empty and always will be.
///
/// Five minutes matches Apache Kafka's own default client-metrics interval and
/// takes the plane to ~1.8 req/s. What it costs is that a subscription created
/// while a client is waiting is picked up up to five minutes later — which would
/// matter if subscriptions existed.
///
/// Measured before changing it, so the change is not sold on the wrong number:
/// api_key 71 is **3 397 ms/s of ~320 000 ms/s** of total request time (~1 %) and
/// 48 bytes per response. It was never the 9.4 cores #400 could not account for.
/// The reason to fix it is that the cadence is the broker's own instruction, and
/// the instruction is wrong.
const NO_SUBSCRIPTION_PUSH_INTERVAL_MS: i32 = 300_000;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GetTelemetrySubscriptionsService;

impl ApiKey for GetTelemetrySubscriptionsService {
    const KEY: i16 = GetTelemetrySubscriptionsRequest::KEY;
}

impl<G> Service<G, GetTelemetrySubscriptionsRequest> for GetTelemetrySubscriptionsService
where
    G: Storage,
{
    type Response = GetTelemetrySubscriptionsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: GetTelemetrySubscriptionsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let _ = ctx;

        // The request's field is documented "must be set to 0 on the first
        // request", so a non-zero one is the id this broker already handed out
        // and the client is identifying itself with it. Mint one only in the
        // first case; echo it in the second. Either way the response carries a
        // non-zero id.
        //
        // The *response* field is documented "Assigned client instance id if
        // ClientInstanceId was 0 in the request, **else 0**", and that half of
        // the sentence cannot be obeyed (#515).
        // `ClientTelemetryUtils.validateClientInstanceId` rejects null *and*
        // `ZERO_UUID`, unconditionally, on every response — so a broker
        // answering 0 throws `IllegalArgumentException` out of the Java
        // client's `poll()`. `apache/kafka`'s own `ClientMetricsManager` never
        // answers 0 either: it filters `Uuid.ZERO_UUID` out of the request and
        // otherwise reuses what the request carried. The reference
        // implementation is the contract; the message definition is not.
        //
        // Before that, this minted a fresh `Uuid::new_v4()` on *every* request,
        // so a client was told its instance id had changed each time it asked —
        // which is the one thing the field exists to prevent.
        let client_instance_id = if req.client_instance_id == UNASSIGNED_CLIENT_INSTANCE_ID {
            *Uuid::new_v4().as_bytes()
        } else {
            req.client_instance_id
        };

        Ok(GetTelemetrySubscriptionsResponse::default()
            .throttle_time_ms(0)
            .error_code(ErrorCode::None.into())
            .client_instance_id(client_instance_id)
            .subscription_id(0)
            .accepted_compression_types(Some([0].into()))
            .push_interval_ms(NO_SUBSCRIPTION_PUSH_INTERVAL_MS)
            .telemetry_max_bytes(1_024)
            .delta_temporality(false)
            // "Empty array: No metrics subscribed." Nothing here configures a
            // subscription, so this is the only answer, and it is why the
            // interval above is a back-off rather than a push cadence.
            .requested_metrics(Some([].into())))
    }
}
