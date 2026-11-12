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

//! `ConsumerGroupHeartbeat` (api_key 68), the KIP-848 consumer group protocol
//! (#612).

use tansu_sans_io::acl::{Operation, Resource};
use tansu_sans_io::{ConsumerGroupHeartbeatRequest, ErrorCode};
use tansu_service::Peer;
use tansu_storage::authorized;

use super::answer;
use super::handler::{answered, respond};

group_service! {
    ConsumerGroupHeartbeatService: ConsumerGroupHeartbeatRequest,
    |ctx, frame| {
        let correlation_id = frame.correlation_id()?;
        let client_id = frame.client_id()?.map(str::to_owned);
        let request = ConsumerGroupHeartbeatRequest::try_from(frame.body)?;

        // As Kafka prints an `InetAddress`, which `kafka-consumer-groups`
        // shows as is in its HOST column (#628).
        let client_host = ctx.get::<Peer>().map(|Peer(addr)| format!("/{}", addr.ip()));

        // `READ` on the group, as for the classic `JoinGroup` and `Heartbeat`
        // (#363).
        if !authorized(&ctx, Resource::Group, &request.group_id, Operation::Read).await {
            let refused = answer::consumer_group_heartbeat(ErrorCode::GroupAuthorizationFailed);
            return Ok(respond(correlation_id, refused));
        }

        let beat = ctx
            .state_mut()
            .consumer_group_heartbeat(client_id.as_deref(), client_host.as_deref(), &request)
            .await;
        answered(beat, answer::consumer_group_heartbeat).map(|body| respond(correlation_id, body))
    }
}
