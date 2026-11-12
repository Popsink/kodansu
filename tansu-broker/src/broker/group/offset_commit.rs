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

//! `OffsetCommit`: a group commits the positions its members have consumed to.

use tansu_sans_io::acl::{Operation, Resource};
use tansu_sans_io::{ErrorCode, OffsetCommitRequest};
use tansu_storage::authorized;

use super::answer;
use super::handler::{answered, respond};
use crate::coordinator::group::OffsetCommit;

group_service! {
    OffsetCommitService: OffsetCommitRequest,
    |ctx, frame| {
        let correlation_id = frame.correlation_id()?;
        let OffsetCommitRequest {
            group_id,
            generation_id_or_member_epoch,
            member_id,
            group_instance_id,
            retention_time_ms,
            topics,
            ..
        } = OffsetCommitRequest::try_from(frame.body)?;

        let refused = |error_code| answer::offset_commit(error_code, topics.as_deref());

        if !authorized(&ctx, Resource::Group, &group_id, Operation::Read).await {
            return Ok(respond(correlation_id, refused(ErrorCode::GroupAuthorizationFailed)));
        }

        let commit = OffsetCommit {
            group_id: &group_id,
            generation_id_or_member_epoch,
            member_id: member_id.as_deref(),
            group_instance_id: group_instance_id.as_deref(),
            retention_time_ms: retention_time_ms.filter(|ms| !ms.is_negative()),
            topics: topics.as_deref(),
        };
        let committed = ctx.state_mut().offset_commit(commit).await;
        answered(committed, refused).map(|body| respond(correlation_id, body))
    }
}
