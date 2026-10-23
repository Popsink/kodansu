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

use std::{collections::BTreeMap, time::SystemTime};

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, ConsumerGroupDescribeRequest,
    consumer_group_describe_response::{ConsumerGroupDescribeResponse, DescribedGroup},
    to_timestamp,
};
use tracing::{debug, instrument};

use crate::{Error, Result, Storage};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`ConsumerGroupDescribeRequest`] returning [`ConsumerGroupDescribeResponse`].
///
/// A KIP-848 group — one with a [`ConsumerGroupDoc`](crate::ConsumerGroupDoc)
/// — is reported from that document: members, epochs, current and target
/// assignments (#612). Every other group is answered
/// [`ErrorCode::GroupIdNotFound`](tansu_sans_io::ErrorCode::GroupIdNotFound),
/// the code that sends an AdminClient to
/// [`DescribeGroups`](crate::DescribeGroupsService), which is the path that
/// carries a classic group's membership.
/// ```
/// use rama::{Context, Layer, Service as _, layer::MapStateLayer};
/// use tansu_sans_io::{ConsumerGroupDescribeRequest, ErrorCode};
/// use tansu_storage::{ConsumerGroupDescribeService, Error, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let storage = StorageContainer::builder()
///     .cluster_id("tansu")
///     .node_id(111)
///     .advertised_listener(Url::parse("tcp://localhost:9092")?)
///     .storage(Url::parse("memory://tansu/")?)
///     .build()
///     .await?;
///
/// let service = MapStateLayer::new(|_| storage).into_layer(ConsumerGroupDescribeService);
///
/// let group_id = "abc";
///
/// let response = service
///     .serve(
///         Context::default(),
///         ConsumerGroupDescribeRequest::default()
///             .group_ids(Some([group_id.into()].into()))
///             .include_authorized_operations(false),
///     )
///     .await?;
///
/// let groups = response.groups.unwrap_or_default();
/// assert_eq!(1, groups.len());
/// assert_eq!(ErrorCode::GroupIdNotFound, ErrorCode::try_from(groups[0].error_code)?);
/// assert_eq!(group_id, groups[0].group_id.as_str());
/// assert_eq!("Unknown", groups[0].group_state.as_str());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsumerGroupDescribeService;

impl ApiKey for ConsumerGroupDescribeService {
    const KEY: i16 = ConsumerGroupDescribeRequest::KEY;
}

impl<G> Service<G, ConsumerGroupDescribeRequest> for ConsumerGroupDescribeService
where
    G: Storage,
{
    type Response = ConsumerGroupDescribeResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: ConsumerGroupDescribeRequest,
    ) -> Result<Self::Response, Self::Error> {
        let storage = ctx.state();
        let group_ids = req.group_ids.unwrap_or_default();
        let now_ms = to_timestamp(&SystemTime::now()).unwrap_or_default();

        let mut consumer = BTreeMap::new();
        let mut classic = Vec::new();

        for group_id in &group_ids {
            match storage.read_consumer_group(group_id).await? {
                Some((group, _)) => {
                    _ = consumer.insert(group_id.clone(), group.described(group_id, now_ms));
                }

                None => classic.push(group_id.clone()),
            }
        }

        let classic = if classic.is_empty() {
            BTreeMap::new()
        } else {
            storage
                .describe_groups(Some(&classic), req.include_authorized_operations)
                .await
                .inspect(|described| debug!(?described))?
                .iter()
                .map(DescribedGroup::from)
                .map(|described| (described.group_id.clone(), described))
                .collect()
        };

        let groups = group_ids
            .iter()
            .filter_map(|group_id| {
                consumer
                    .get(group_id)
                    .or_else(|| classic.get(group_id))
                    .cloned()
            })
            .collect();

        Ok(ConsumerGroupDescribeResponse::default()
            .throttle_time_ms(0)
            .groups(Some(groups)))
    }
}
