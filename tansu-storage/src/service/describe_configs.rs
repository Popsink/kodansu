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

//! `DescribeConfigs`: the configuration of each resource the client names.

use tansu_sans_io::{
    ConfigResource, DescribeConfigsRequest, DescribeConfigsResponse,
    describe_configs_request::DescribeConfigsResource,
};

storage_service! {
    /// Asks [`Storage::describe_config`](crate::Storage::describe_config) about
    /// each resource in turn, in request order. A resource that does not exist
    /// comes back with its own error code; only a storage failure fails the
    /// whole request.
    ///
    /// `tests/describe_configs.rs` covers a topic that exists and one that
    /// does not (#579).
    DescribeConfigsService: DescribeConfigsRequest => DescribeConfigsResponse,
    |ctx, req| {
        let storage = ctx.state();
        let mut results = Vec::new();
        for resource in req.resources.into_iter().flatten() {
            let DescribeConfigsResource {
                resource_name,
                resource_type,
                configuration_keys,
                ..
            } = resource;
            let kind = ConfigResource::from(resource_type);
            let keys = configuration_keys.as_deref();
            let described = storage.describe_config(&resource_name, kind, keys).await;
            results.push(described.inspect_err(|err| tracing::error!(?err))?);
        }
        let response = DescribeConfigsResponse::default().results(results.into());
        Ok(response)
    }
}
