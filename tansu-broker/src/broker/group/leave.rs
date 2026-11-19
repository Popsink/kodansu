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

//! `LeaveGroup`: a member leaves a classic group.

use tansu_sans_io::acl::{Operation, Resource};
use tansu_sans_io::{ErrorCode, LeaveGroupRequest};
use tansu_storage::authorized;

use super::answer;
use super::handler::{answered, respond};

group_service! {
    LeaveGroupService: LeaveGroupRequest,
    |ctx, frame| {
        let correlation_id = frame.correlation_id()?;
        let LeaveGroupRequest { group_id, member_id, members, .. } =
            LeaveGroupRequest::try_from(frame.body)?;

        if !authorized(&ctx, Resource::Group, &group_id, Operation::Read).await {
            let refused = answer::leave_group(ErrorCode::GroupAuthorizationFailed);
            return Ok(respond(correlation_id, refused));
        }

        let left = ctx
            .state_mut()
            .leave(&group_id, member_id.as_deref(), members.as_deref())
            .await;
        answered(left, answer::leave_group).map(|body| respond(correlation_id, body))
    }
}
