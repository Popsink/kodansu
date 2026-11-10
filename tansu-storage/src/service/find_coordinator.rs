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

//! `FindCoordinator`: every group and transaction is coordinated here.

use tansu_sans_io::find_coordinator_response::Coordinator;
use tansu_sans_io::{ErrorCode, FindCoordinatorRequest, FindCoordinatorResponse};

use super::handler::UNTHROTTLED;

storage_service! {
    /// Names this broker, at its advertised listener, as the coordinator for
    /// whatever the client asked about. Up to v3 the answer is the top-level
    /// node, host and port, for the one key in the request; from v4 it is one
    /// [`Coordinator`] per batched key. Both carry the same broker.
    ///
    /// `tests/find_coordinator.rs` serves a request end to end.
    FindCoordinatorService: FindCoordinatorRequest => FindCoordinatorResponse,
    |ctx, req| {
        let storage = ctx.state();
        let node = storage.node().await?;
        let listener = storage.advertised_listener().await?;
        let this = Coordinator::default()
            .node_id(node)
            .host(listener.host_str().unwrap_or("localhost").to_owned())
            .port(listener.port().map_or(9092, i32::from));

        let coordinators = req
            .coordinator_keys
            .map(|keys| keys.into_iter().map(|key| this.clone().key(key)).collect());
        let response = FindCoordinatorResponse::default()
            .throttle_time_ms(UNTHROTTLED)
            .error_code(Some(i16::from(ErrorCode::None)))
            .error_message(Some(String::from("NONE")))
            .node_id(Some(this.node_id))
            .host(Some(this.host))
            .port(Some(this.port))
            .coordinators(coordinators);
        Ok(response)
    }
}
