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

//! `DeleteAcls`, which was not implemented at all (#363).

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, DeleteAclsRequest, DeleteAclsResponse, ErrorCode,
    delete_acls_response::{DeleteAclsFilterResult, DeleteAclsMatchingAcl},
};

use tansu_sans_io::acl::Operation;

use crate::{AclFilter, Error, NO_AUTHORIZER, Storage, authorized_cluster, enforcing};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeleteAclsService;

impl ApiKey for DeleteAclsService {
    const KEY: i16 = DeleteAclsRequest::KEY;
}

impl<G> Service<G, DeleteAclsRequest> for DeleteAclsService
where
    G: Storage,
{
    type Response = DeleteAclsResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: DeleteAclsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let requested = req.filters.unwrap_or_default();

        // See `CreateAcls`: with nothing to enforce the rules, reporting them
        // deleted is as misleading as reporting them created (#578).
        if !enforcing(&ctx) {
            return Ok(refused(
                requested.len(),
                ErrorCode::SecurityDisabled,
                Some(NO_AUTHORIZER),
            ));
        }

        // See `CreateAcls`: a principal that can delete the rules can grant
        // itself anything (#363).
        if !authorized_cluster(&ctx, Operation::Alter).await {
            return Ok(refused(
                requested.len(),
                ErrorCode::ClusterAuthorizationFailed,
                None,
            ));
        }

        let filters = requested
            .iter()
            .map(|filter| AclFilter {
                resource_type: filter.resource_type_filter.into(),
                resource_name: filter.resource_name_filter.clone(),
                pattern: filter.pattern_type_filter.unwrap_or(3).into(),
                principal: filter.principal_filter.clone(),
                host: filter.host_filter.clone(),
                operation: filter.operation.into(),
                permission: filter.permission_type.into(),
            })
            .collect::<Vec<_>>();

        match ctx.state().delete_acls(&filters[..]).await {
            Ok(deleted) => Ok(DeleteAclsResponse::default()
                .throttle_time_ms(0)
                .filter_results(Some(
                    deleted
                        .into_iter()
                        .map(|bindings| {
                            DeleteAclsFilterResult::default()
                                .error_code(ErrorCode::None.into())
                                .error_message(None)
                                .matching_acls(Some(
                                    bindings
                                        .into_iter()
                                        .map(|binding| {
                                            DeleteAclsMatchingAcl::default()
                                                .error_code(ErrorCode::None.into())
                                                .error_message(None)
                                                .resource_type(binding.resource_type.into())
                                                .resource_name(binding.resource_name)
                                                .pattern_type(Some(binding.pattern.into()))
                                                .principal(binding.principal)
                                                .host(binding.host)
                                                .operation(binding.operation.into())
                                                .permission_type(binding.permission.into())
                                        })
                                        .collect(),
                                ))
                        })
                        .collect(),
                ))),

            Err(error) => {
                tracing::error!(?error, "could not delete acls");

                Ok(refused(
                    requested.len(),
                    ErrorCode::UnknownServerError,
                    Some("could not delete acls"),
                ))
            }
        }
    }
}

/// The same error against every filter the request carried, matching nothing.
///
/// One result per filter, in request order, for the same reason creations are:
/// a client matches them positionally.
fn refused(
    filters: usize,
    error_code: ErrorCode,
    error_message: Option<&str>,
) -> DeleteAclsResponse {
    DeleteAclsResponse::default()
        .throttle_time_ms(0)
        .filter_results(Some(
            (0..filters)
                .map(|_| {
                    DeleteAclsFilterResult::default()
                        .error_code(error_code.into())
                        .error_message(error_message.map(str::to_owned))
                        .matching_acls(Some([].into()))
                })
                .collect(),
        ))
}
