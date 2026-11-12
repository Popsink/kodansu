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

//! `JoinGroup`: a member joins a classic group, or rejoins it.

use tansu_sans_io::acl::{Operation, Resource};
use tansu_sans_io::{ErrorCode, JoinGroupRequest};
use tansu_storage::authorized;

use super::answer;
use super::handler::{answered, respond};

group_service! {
    JoinGroupService: JoinGroupRequest,
    |ctx, frame| {
        let correlation_id = frame.correlation_id()?;
        let client_id = frame.client_id()?.map(str::to_owned);
        let JoinGroupRequest {
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
            reason,
            ..
        } = JoinGroupRequest::try_from(frame.body)?;

        // The member id is echoed back in every answer, because the client
        // matches the answer to its own attempt by it, whatever the outcome.
        let refused = |error_code| answer::join_group(error_code, &member_id);

        if !authorized(&ctx, Resource::Group, &group_id, Operation::Read).await {
            return Ok(respond(correlation_id, refused(ErrorCode::GroupAuthorizationFailed)));
        }

        let joined = ctx
            .state_mut()
            .join(
                client_id.as_deref(),
                &group_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                &member_id,
                group_instance_id.as_deref(),
                &protocol_type,
                protocols.as_deref(),
                reason.as_deref(),
            )
            .await;
        answered(joined, refused).map(|body| respond(correlation_id, body))
    }
}
