// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! `CreateAcls`, which used to answer success without doing anything (#363).

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, CreateAclsRequest, CreateAclsResponse, ErrorCode,
    create_acls_response::AclCreationResult,
};

use tansu_sans_io::acl::Operation;

use crate::{AclBinding, Error, Storage, authorized_cluster};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CreateAclsService;

impl ApiKey for CreateAclsService {
    const KEY: i16 = CreateAclsRequest::KEY;
}

impl<G> Service<G, CreateAclsRequest> for CreateAclsService
where
    G: Storage,
{
    type Response = CreateAclsResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: CreateAclsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let creations = req.creations.unwrap_or_default();

        // `ALTER` on the cluster, as Kafka requires — and the hole that made
        // every other rule provisional: an authenticated principal that can
        // delete the ACLs can grant itself anything, so enforcing everything
        // else while leaving this open enforces nothing (#363).
        if !authorized_cluster(&ctx, Operation::Alter).await {
            return Ok(CreateAclsResponse::default()
                .throttle_time_ms(0)
                .results(Some(
                    creations
                        .iter()
                        .map(|_| {
                            AclCreationResult::default()
                                .error_code(ErrorCode::ClusterAuthorizationFailed.into())
                                .error_message(None)
                        })
                        .collect(),
                )));
        }

        let bindings = creations
            .iter()
            .map(|creation| AclBinding {
                resource_type: creation.resource_type.into(),
                resource_name: creation.resource_name.clone(),
                // Absent below v1, where the pattern type did not exist and
                // every rule was literal. Defaulting to anything else would
                // silently change what an old client's rule selects.
                pattern: creation.resource_pattern_type.unwrap_or(3).into(),
                principal: creation.principal.clone(),
                host: creation.host.clone(),
                operation: creation.operation.into(),
                permission: creation.permission_type.into(),
            })
            .collect::<Vec<_>>();

        ctx.state()
            .create_acls(&bindings[..])
            .await
            .map(|outcomes| {
                CreateAclsResponse::default()
                    .throttle_time_ms(0)
                    .results(Some(
                        outcomes
                            .into_iter()
                            .map(|error_code| {
                                AclCreationResult::default()
                                    .error_code(error_code.into())
                                    .error_message(None)
                            })
                            .collect(),
                    ))
            })
            .or_else(|error| {
                // One error code per creation, in request order: a client
                // matches results to creations positionally, so a short list
                // is a client attributing the failure to the wrong rule.
                tracing::error!(?error, "could not create acls");

                Ok(CreateAclsResponse::default()
                    .throttle_time_ms(0)
                    .results(Some(
                        creations
                            .iter()
                            .map(|_| {
                                AclCreationResult::default()
                                    .error_code(ErrorCode::UnknownServerError.into())
                                    .error_message(Some("could not create acls".into()))
                            })
                            .collect(),
                    )))
            })
    }
}
