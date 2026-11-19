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

//! `ListGroups`: the groups this principal may see, filtered by state and type.

use tansu_sans_io::acl::{Operation, Resource};
use tansu_sans_io::{ListGroupsRequest, ListGroupsResponse};

use super::handler::UNTHROTTLED;
use crate::authorized;

storage_service! {
    /// Lists the groups [`Storage::list_groups`](crate::Storage::list_groups)
    /// returns for the state filter, keeps those matching the type filter, and
    /// drops any the principal may not `Describe`.
    ///
    /// `tests/list_groups.rs` covers the filters, including an empty one.
    ListGroupsService: ListGroupsRequest => ListGroupsResponse,
    |ctx, req| {
        let ListGroupsRequest {
            states_filter,
            types_filter,
            ..
        } = req;
        let listed = ctx.state().list_groups(states_filter.as_deref()).await?;

        // Omitted rather than reported, unlike a named `Metadata` topic: this
        // request names nothing, so there is no client expectation to answer,
        // and an entry per refused group would enumerate the namespace as
        // thoroughly as returning them (#363). On a mutualised fleet the list
        // of groups is a list of the other tenants' workloads.
        let mut groups = Vec::with_capacity(listed.len());

        // KIP-848's `TypesFilter` (#612): as with states, an empty filter is
        // no filter, and Kafka matches the type names case-insensitively.
        let types = types_filter
            .filter(|types| !types.is_empty())
            .map(|types| {
                types
                    .iter()
                    .map(|group_type| group_type.to_lowercase())
                    .collect::<Vec<_>>()
            });

        for group in listed {
            if let Some(ref types) = types
                && !group
                    .group_type
                    .as_deref()
                    .is_some_and(|group_type| types.contains(&group_type.to_lowercase()))
            {
                continue;
            }

            let id = group.group_id.as_str();
            if authorized(&ctx, Resource::Group, id, Operation::Describe).await {
                groups.push(group);
            }
        }

        Ok(ListGroupsResponse::default()
            .throttle_time_ms(UNTHROTTLED)
            .groups(Some(groups)))
    }
}
