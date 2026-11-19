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

//! `OffsetFetch`: the positions a group has committed.

use tansu_sans_io::acl::{Operation, Resource};
use tansu_sans_io::{ErrorCode, OffsetFetchRequest};
use tansu_storage::authorized;

use super::answer;
use super::handler::respond;
use crate::Error;

group_service! {
    OffsetFetchService: OffsetFetchRequest,
    |ctx, frame| {
        let correlation_id = frame.correlation_id()?;
        let OffsetFetchRequest {
            group_id,
            topics,
            groups,
            require_stable,
            ..
        } = OffsetFetchRequest::try_from(frame.body)?;

        let refused = |error_code| answer::offset_fetch(error_code, groups.as_deref());

        // Every group the request names, in either shape: `group_id` below v8,
        // the `groups` array from v8. Refused as a whole rather than per group,
        // because the answer carries one code across all of them — a mixed
        // request would need a response shape that does not exist here, and
        // stamping the refusal on the allowed groups too would be worse than
        // refusing the call the client can retry one group at a time.
        let named = group_id
            .iter()
            .chain(groups.iter().flatten().map(|group| &group.group_id));
        for name in named {
            if !authorized(&ctx, Resource::Group, name, Operation::Describe).await {
                return Ok(respond(correlation_id, refused(ErrorCode::GroupAuthorizationFailed)));
            }
        }

        let fetched = ctx
            .state_mut()
            .offset_fetch(group_id.as_deref(), topics.as_deref(), groups.as_deref(), require_stable)
            .await;
        match fetched {
            Ok(body) => Ok(respond(correlation_id, body)),
            // Answered rather than ending the connection, as every group
            // service answers an `Error::Api` (#300) — and storage answers a
            // failed offsets read the same way (#607).
            Err(Error::Api(error_code) | Error::Storage(tansu_storage::Error::Api(error_code))) => {
                Ok(respond(correlation_id, refused(error_code)))
            }
            Err(otherwise) => Err(otherwise),
        }
    }
}
