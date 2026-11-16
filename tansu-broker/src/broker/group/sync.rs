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

//! `SyncGroup`: the leader hands out the assignment and every member collects
//! its own.

use tansu_sans_io::acl::{Operation, Resource};
use tansu_sans_io::{ErrorCode, SyncGroupRequest};
use tansu_storage::authorized;

use super::answer;
use super::handler::{answered, respond};

group_service! {
    SyncGroupService: SyncGroupRequest,
    |ctx, frame| {
        let correlation_id = frame.correlation_id()?;
        let SyncGroupRequest {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            assignments,
            ..
        } = SyncGroupRequest::try_from(frame.body)?;

        if !authorized(&ctx, Resource::Group, &group_id, Operation::Read).await {
            let refused = answer::sync_group(ErrorCode::GroupAuthorizationFailed);
            return Ok(respond(correlation_id, refused));
        }

        let coordinator = ctx.state_mut();
        let syncing = coordinator.sync(
            &group_id,
            generation_id,
            &member_id,
            group_instance_id.as_deref(),
            protocol_type.as_deref(),
            protocol_name.as_deref(),
            assignments.as_deref(),
        );
        let synced = syncing.await;
        answered(synced, answer::sync_group).map(|body| respond(correlation_id, body))
    }
}
