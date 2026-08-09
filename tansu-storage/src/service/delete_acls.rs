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

//! `DeleteAcls`, which was not implemented at all (#363).

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, DeleteAclsRequest, DeleteAclsResponse, ErrorCode,
    delete_acls_response::{DeleteAclsFilterResult, DeleteAclsMatchingAcl},
};

use crate::{AclFilter, Error, Storage};

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
                // One result per filter, in request order, for the same reason
                // creations are: a client matches them positionally.
                tracing::error!(?error, "could not delete acls");

                Ok(DeleteAclsResponse::default()
                    .throttle_time_ms(0)
                    .filter_results(Some(
                        requested
                            .iter()
                            .map(|_| {
                                DeleteAclsFilterResult::default()
                                    .error_code(ErrorCode::UnknownServerError.into())
                                    .error_message(Some("could not delete acls".into()))
                                    .matching_acls(Some([].into()))
                            })
                            .collect(),
                    )))
            }
        }
    }
}
