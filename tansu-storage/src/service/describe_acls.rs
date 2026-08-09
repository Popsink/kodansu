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

//! `DescribeAcls`, which used to answer an empty list whatever was stored
//! (#363).

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, DescribeAclsRequest, DescribeAclsResponse, ErrorCode,
    describe_acls_response::{AclDescription, DescribeAclsResource},
};

use crate::{AclBinding, AclFilter, Error, Storage};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DescribeAclsService;

impl ApiKey for DescribeAclsService {
    const KEY: i16 = DescribeAclsRequest::KEY;
}

impl<G> Service<G, DescribeAclsRequest> for DescribeAclsService
where
    G: Storage,
{
    type Response = DescribeAclsResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<G>,
        req: DescribeAclsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let filter = AclFilter {
            resource_type: req.resource_type_filter.into(),
            resource_name: req.resource_name_filter.clone(),
            pattern: req.pattern_type_filter.unwrap_or(3).into(),
            principal: req.principal_filter.clone(),
            host: req.host_filter.clone(),
            operation: req.operation.into(),
            permission: req.permission_type.into(),
        };

        match ctx.state().describe_acls(&filter).await {
            Ok(bindings) => Ok(DescribeAclsResponse::default()
                .throttle_time_ms(0)
                .error_code(ErrorCode::None.into())
                .error_message(None)
                .resources(Some(resources(bindings)))),

            Err(error) => {
                // Not knowing is not the same as "no rules", and on a
                // fail-closed broker those two answers are opposites: an
                // operator reading an empty list concludes nothing is
                // protected. Retriable is both true and actionable.
                tracing::error!(?error, "could not describe acls");

                Ok(DescribeAclsResponse::default()
                    .throttle_time_ms(0)
                    .error_code(ErrorCode::UnknownServerError.into())
                    .error_message(Some("could not describe acls".into()))
                    .resources(Some([].into())))
            }
        }
    }
}

/// Bindings grouped by the resource they are on, which is the shape the
/// response has: `kafka-acls.sh` prints one heading per resource with its rules
/// underneath, and a flat list would make it print one heading per rule.
fn resources(bindings: Vec<AclBinding>) -> Vec<DescribeAclsResource> {
    let mut resources: Vec<DescribeAclsResource> = vec![];

    for binding in bindings {
        let description = AclDescription::default()
            .principal(binding.principal.clone())
            .host(binding.host.clone())
            .operation(binding.operation.into())
            .permission_type(binding.permission.into());

        let resource_type = i8::from(binding.resource_type);
        let pattern_type = i8::from(binding.pattern);

        if let Some(resource) = resources.iter_mut().find(|resource| {
            resource.resource_type == resource_type
                && resource.resource_name == binding.resource_name
                && resource.pattern_type == Some(pattern_type)
        }) {
            let mut acls = resource.acls.take().unwrap_or_default();
            acls.push(description);
            resource.acls = Some(acls);
        } else {
            resources.push(
                DescribeAclsResource::default()
                    .resource_type(resource_type)
                    .resource_name(binding.resource_name)
                    .pattern_type(Some(pattern_type))
                    .acls(Some(vec![description])),
            );
        }
    }

    resources
}
