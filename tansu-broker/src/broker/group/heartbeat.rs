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

//! `Heartbeat`: a member of a classic group says it is still there.

use tansu_sans_io::acl::{Operation, Resource};
use tansu_sans_io::{ErrorCode, HeartbeatRequest};
use tansu_storage::authorized;

use super::answer;
use super::handler::{answered, respond};

group_service! {
    HeartbeatService: HeartbeatRequest,
    |ctx, frame| {
        let correlation_id = frame.correlation_id()?;
        let HeartbeatRequest {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            ..
        } = HeartbeatRequest::try_from(frame.body)?;

        // `READ` on the group, as Kafka requires (#363). Without it a principal
        // could join another tenant's group and be handed partitions of topics
        // it cannot read — refused at the fetch, but only after it had
        // disturbed the group's membership to find out.
        if !authorized(&ctx, Resource::Group, &group_id, Operation::Read).await {
            let refused = answer::heartbeat(ErrorCode::GroupAuthorizationFailed);
            return Ok(respond(correlation_id, refused));
        }

        let beat = ctx
            .state_mut()
            .heartbeat(&group_id, generation_id, &member_id, group_instance_id.as_deref())
            .await;
        answered(beat, answer::heartbeat).map(|body| respond(correlation_id, body))
    }
}
