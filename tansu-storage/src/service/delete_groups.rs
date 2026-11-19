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

use rama::Context;
use rama::Service;
use tansu_sans_io::ApiKey;
use tansu_sans_io::ErrorCode;
use tansu_sans_io::acl::Operation;
use tansu_sans_io::acl::Resource;
use tansu_sans_io::delete_groups_response::DeletableGroupResult;
use tansu_sans_io::{DeleteGroupsRequest, DeleteGroupsResponse};

use crate::Storage;
use crate::authorized;
use crate::{Error, Result};

/// Deletes each named group the principal may `DELETE`, answering
/// [`ErrorCode::GroupAuthorizationFailed`] for the others and
/// [`ErrorCode::GroupIdNotFound`] for a group nothing created.
/// ```
/// use rama::layer::MapStateLayer;
/// use rama::{Context, Layer as _, Service as _};
/// use tansu_sans_io::DeleteGroupsRequest;
/// use tansu_sans_io::ErrorCode;
/// use tansu_storage::{DeleteGroupsService, StorageContainer};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), tansu_storage::Error> {
/// let listener = url::Url::parse("tcp://localhost:9092")?;
/// let engine = StorageContainer::builder().cluster_id("tansu").node_id(111);
/// let engine = engine.advertised_listener(listener);
/// let engine = engine.storage(url::Url::parse("memory://tansu/")?).build().await?;
/// let groups = MapStateLayer::new(|_| engine).into_layer(DeleteGroupsService);
///
/// let named = DeleteGroupsRequest::default().groups_names(Some(vec!["abcba".into()]));
/// let answered = groups.serve(Context::default(), named).await?;
/// let [only] = &answered.results.unwrap_or_default()[..] else {
///     panic!("one group asked, one result expected")
/// };
/// assert_eq!("abcba", only.group_id);
/// assert_eq!(ErrorCode::GroupIdNotFound, ErrorCode::try_from(only.error_code)?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeleteGroupsService;

impl ApiKey for DeleteGroupsService {
    const KEY: i16 = DeleteGroupsRequest::KEY;
}

impl<G> Service<G, DeleteGroupsRequest> for DeleteGroupsService
where
    G: Storage,
{
    type Response = DeleteGroupsResponse;
    type Error = Error;

    #[tracing::instrument(skip(state, request))]
    async fn serve(
        &self,
        state: Context<G>,
        request: DeleteGroupsRequest,
    ) -> Result<Self::Response, Self::Error> {
        // Per group, so a refusal on one does not fail the rest of the
        // request — and deleting a group is `DELETE` on it, not `READ` like
        // the coordination APIs (#363).
        let mut permitted = Vec::new();
        let mut denied = Vec::new();

        for name in request.groups_names.unwrap_or_default() {
            if authorized(&state, Resource::Group, &name, Operation::Delete).await {
                permitted.push(name);
                continue;
            }
            let code = i16::from(ErrorCode::GroupAuthorizationFailed);
            denied.push(
                DeletableGroupResult::default()
                    .group_id(name)
                    .error_code(code),
            );
        }

        let deleting = state.state().delete_groups(Some(permitted.as_slice()));
        let mut outcomes = deleting.await?;
        outcomes.extend(denied);

        let unthrottled = DeleteGroupsResponse::default().throttle_time_ms(0);
        Ok(unthrottled.results(Some(outcomes)))
    }
}

#[cfg(all(feature = "dynostore", test))]
mod tests {
    use rama::Context;
    use rama::Layer as _;
    use rama::Service as _;
    use rama::layer::MapStateLayer;
    use tansu_sans_io::DeleteGroupsRequest;
    use tansu_sans_io::ErrorCode;

    use crate::DeleteGroupsService;
    use crate::{Error, StorageContainer};

    #[tokio::test]
    async fn a_group_nothing_created_is_not_found() -> Result<(), Error> {
        let listener = url::Url::parse("tcp://localhost:9092")?;
        let engine = StorageContainer::builder().cluster_id("tansu").node_id(111);
        let engine = engine.advertised_listener(listener);
        let memory = url::Url::parse("memory://tansu/")?;
        let engine = engine.storage(memory).build().await?;
        let groups = MapStateLayer::new(|_| engine).into_layer(DeleteGroupsService);

        let named = DeleteGroupsRequest::default().groups_names(Some(vec!["abcba".into()]));
        let answered = groups.serve(Context::default(), named).await?;
        let results = answered.results.unwrap_or_default();
        let [only] = &results[..] else {
            panic!("one group asked, {} results", results.len())
        };
        assert_eq!("abcba", only.group_id);

        // Deleting a group that does not exist reports that it does not exist.
        // It answered `NONE` until #445, because existence was inferred from a
        // delete — and a delete of an absent key succeeds. Tooling that
        // distinguishes "idle group" from "nonexistent group" had nothing to read.
        let code = ErrorCode::try_from(only.error_code)?;
        assert_eq!(ErrorCode::GroupIdNotFound, code);
        Ok(())
    }
}
