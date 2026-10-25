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

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, ConsumerGroupHeartbeatRequest, ErrorCode, Frame, Header,
    acl::{Operation, Resource},
};
use tansu_storage::authorized;
use tracing::instrument;

use super::answer;
use crate::{Error, Result, coordinator::group::Coordinator};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsumerGroupHeartbeatService;

impl ApiKey for ConsumerGroupHeartbeatService {
    const KEY: i16 = ConsumerGroupHeartbeatRequest::KEY;
}

impl<C> Service<C, Frame> for ConsumerGroupHeartbeatService
where
    C: Coordinator,
{
    type Response = Frame;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(&self, mut ctx: Context<C>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;
        let client_id = req
            .client_id()
            .map(|client_id| client_id.map(|client_id| client_id.to_owned()))?;

        let request = ConsumerGroupHeartbeatRequest::try_from(req.body)?;

        // `READ` on the group, as for the classic `JoinGroup` and `Heartbeat`
        // (#363).
        if !authorized(&ctx, Resource::Group, &request.group_id, Operation::Read).await {
            return Ok(Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body: answer::consumer_group_heartbeat(ErrorCode::GroupAuthorizationFailed),
            });
        }

        ctx.state_mut()
            .consumer_group_heartbeat(client_id.as_deref(), &request)
            .await
            .or_else(|error| match error {
                // An answer the broker chose, not a failure (#300).
                Error::Api(error_code) => Ok(answer::consumer_group_heartbeat(error_code)),
                otherwise => Err(otherwise),
            })
            .map(|body| Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
    }
}
