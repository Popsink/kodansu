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
use tansu_sans_io::{ApiKey, Frame, Header, HeartbeatRequest};
use tracing::instrument;

use super::answer;
use tansu_sans_io::{
    ErrorCode,
    acl::{Operation, Resource},
};
use tansu_storage::authorized;

use crate::{Error, Result, coordinator::group::Coordinator};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HeartbeatService;

impl ApiKey for HeartbeatService {
    const KEY: i16 = HeartbeatRequest::KEY;
}

impl<C> Service<C, Frame> for HeartbeatService
where
    C: Coordinator,
{
    type Response = Frame;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(&self, mut ctx: Context<C>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;

        let req = HeartbeatRequest::try_from(req.body)?;

        // `READ` on the group, as Kafka requires (#363). Without it a principal
        // could join another tenant's group and be handed partitions of topics
        // it cannot read — refused at the fetch, but only after it had
        // disturbed the group's membership to find out.
        if !authorized(&ctx, Resource::Group, &req.group_id, Operation::Read).await {
            return Ok(Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body: answer::heartbeat(ErrorCode::GroupAuthorizationFailed),
            });
        }

        let coordinator = ctx.state_mut();

        coordinator
            .heartbeat(
                req.group_id.as_str(),
                req.generation_id,
                req.member_id.as_str(),
                req.group_instance_id.as_deref(),
            )
            .await
            .or_else(|error| match error {
                // An `Error::Api` is an answer the broker chose, not a failure.
                // Answering it here is what stops it from ending the connection
                // with no response written — `early eof` to the caller (#300). See
                // [`super::answer`].
                Error::Api(error_code) => Ok(answer::heartbeat(error_code)),

                otherwise => Err(otherwise),
            })
            .map(|body| Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
    }
}
