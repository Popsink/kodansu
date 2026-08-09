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
use tansu_sans_io::{ApiKey, Frame, Header, OffsetFetchRequest};
use tracing::instrument;

use super::answer;
use tansu_sans_io::{
    ErrorCode,
    acl::{Operation, Resource},
};
use tansu_storage::authorized;

use crate::{Error, Result, coordinator::group::Coordinator};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OffsetFetchService;

impl ApiKey for OffsetFetchService {
    const KEY: i16 = OffsetFetchRequest::KEY;
}

impl<C> Service<C, Frame> for OffsetFetchService
where
    C: Coordinator,
{
    type Response = Frame;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(&self, mut ctx: Context<C>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;
        let offset_fetch = OffsetFetchRequest::try_from(req.body)?;

        // Every group the request names, in either shape: `group_id` below v8,
        // the `groups` array from v8. Refused as a whole rather than per group,
        // because the answer carries one code across all of them — a mixed
        // request would need a response shape that does not exist here, and
        // stamping the refusal on the allowed groups too would be worse than
        // refusing the call the client can retry one group at a time.
        let mut named = offset_fetch.group_id.iter().cloned().collect::<Vec<_>>();

        named.extend(
            offset_fetch
                .groups
                .iter()
                .flatten()
                .map(|group| group.group_id.clone()),
        );

        for group_id in &named {
            if !authorized(&ctx, Resource::Group, group_id, Operation::Describe).await {
                return Ok(Frame {
                    size: 0,
                    header: Header::Response { correlation_id },
                    body: answer::offset_fetch(
                        ErrorCode::GroupAuthorizationFailed,
                        offset_fetch.groups.as_deref(),
                    ),
                });
            }
        }

        let coordinator = ctx.state_mut();

        coordinator
            .offset_fetch(
                offset_fetch.group_id.as_deref(),
                offset_fetch.topics.as_deref(),
                offset_fetch.groups.as_deref(),
                offset_fetch.require_stable,
            )
            .await
            .or_else(|error| match error {
                // An `Error::Api` is an answer the broker chose, not a failure.
                // Answering it here is what stops it from ending the connection
                // with no response written — `early eof` to the caller (#300). See
                // [`super::answer`].
                Error::Api(error_code) => Ok(answer::offset_fetch(
                    error_code,
                    offset_fetch.groups.as_deref(),
                )),

                otherwise => Err(otherwise),
            })
            .map(|body| Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
    }
}
