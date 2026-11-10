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

//! `DescribeGroups`: state, protocol and members of classic consumer groups.

use tansu_sans_io::describe_groups_response::DescribedGroup;
use tansu_sans_io::{DescribeGroupsRequest, DescribeGroupsResponse};

storage_service! {
    /// Converts what [`Storage::describe_groups`](crate::Storage::describe_groups)
    /// reports into the wire shape, one entry per requested group. A group
    /// nobody created is `Dead`, not an error (#445).
    ///
    /// `tests/describe_groups.rs` describes a group that was never created.
    DescribeGroupsService: DescribeGroupsRequest => DescribeGroupsResponse,
    |ctx, req| {
        let DescribeGroupsRequest {
            groups,
            include_authorized_operations,
            ..
        } = req;
        let with_operations = include_authorized_operations.unwrap_or_default();
        let described = ctx
            .state()
            .describe_groups(groups.as_deref(), with_operations)
            .await?;
        let groups: Vec<_> = described.iter().map(DescribedGroup::from).collect();
        let no_throttle = Some(0);
        Ok(DescribeGroupsResponse::default()
            .throttle_time_ms(no_throttle)
            .groups(groups.into()))
    }
}
