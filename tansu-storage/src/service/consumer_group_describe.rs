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

use std::collections::BTreeMap;
use std::time::SystemTime;

use rama::Context;
use rama::Service;
use tansu_sans_io::consumer_group_describe_response::ConsumerGroupDescribeResponse;
use tansu_sans_io::consumer_group_describe_response::DescribedGroup;
use tansu_sans_io::{ApiKey, ConsumerGroupDescribeRequest, to_timestamp};

use crate::Storage;
use crate::{Error, Result};

/// Answers `ConsumerGroupDescribe` from the [`Storage`] it is served with.
///
/// A KIP-848 group — one with a [`ConsumerGroupDoc`](crate::ConsumerGroupDoc)
/// — is reported from that document: members, epochs, current and target
/// assignments (#612). Every other group is answered
/// [`ErrorCode::GroupIdNotFound`](tansu_sans_io::ErrorCode::GroupIdNotFound),
/// the code that sends an AdminClient to
/// [`DescribeGroups`](crate::DescribeGroupsService), which is the path that
/// carries a classic group's membership.
/// ```
/// use rama::layer::MapStateLayer;
/// use rama::{Context, Layer as _, Service as _};
/// use tansu_sans_io::ConsumerGroupDescribeRequest;
/// use tansu_sans_io::ErrorCode;
/// use tansu_storage::{ConsumerGroupDescribeService, StorageContainer};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), tansu_storage::Error> {
/// let listener = url::Url::parse("tcp://localhost:9092")?;
/// let engine = StorageContainer::builder().cluster_id("tansu").node_id(111);
/// let engine = engine.advertised_listener(listener);
/// let engine = engine.storage(url::Url::parse("memory://tansu/")?).build().await?;
/// let describe = MapStateLayer::new(|_| engine).into_layer(ConsumerGroupDescribeService);
///
/// let request = ConsumerGroupDescribeRequest::default().group_ids(Some(vec!["abc".into()]));
/// let request = request.include_authorized_operations(false);
/// let described = describe.serve(Context::default(), request).await?;
///
/// let [unknown] = &described.groups.unwrap_or_default()[..] else {
///     panic!("one group")
/// };
/// assert_eq!(ErrorCode::GroupIdNotFound, ErrorCode::try_from(unknown.error_code)?);
/// assert_eq!(("abc", "Unknown"), (unknown.group_id.as_str(), unknown.group_state.as_str()));
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

    #[tracing::instrument(skip(state, request))]
    async fn serve(
        &self,
        state: Context<G>,
        request: ConsumerGroupDescribeRequest,
    ) -> Result<Self::Response, Self::Error> {
        let storage = state.state();
        let group_ids = request.group_ids.unwrap_or_default();
        let now_ms = to_timestamp(&SystemTime::now()).unwrap_or_default();

        let mut consumer = BTreeMap::new();
        let mut classic = Vec::new();

        for group_id in &group_ids {
            match storage.read_consumer_group(group_id).await? {
                Some((group, _)) => {
                    let stamps = storage.list_consumer_member_stamps(group_id).await?;
                    let described = group.with_contacts(&stamps).described(group_id, now_ms);

                    // A consumer group nobody is in may have been taken back
                    // by classic consumers since (#626), so ask the classic
                    // side too and let whichever has members answer.
                    if described.members.as_ref().is_none_or(Vec::is_empty) {
                        classic.push(group_id.clone());
                    }

                    _ = consumer.insert(group_id.clone(), described);
                }

                None => classic.push(group_id.clone()),
            }
        }

        let classic: BTreeMap<_, _> = if classic.is_empty() {
            BTreeMap::new()
        } else {
            let operations = request.include_authorized_operations;
            let describing = storage.describe_groups(Some(&classic), operations);
            let answered = describing.await?;
            tracing::debug!(described = ?answered);
            let keyed = |group: DescribedGroup| (group.group_id.clone(), group);
            let groups = answered.iter().map(DescribedGroup::from);
            groups.map(keyed).collect()
        };

        let has_members =
            |described: &&DescribedGroup| described.members.as_ref().is_some_and(|m| !m.is_empty());

        let groups = group_ids
            .iter()
            .filter_map(|group_id| {
                consumer
                    .get(group_id)
                    .filter(has_members)
                    .or_else(|| classic.get(group_id).filter(has_members))
                    .or_else(|| consumer.get(group_id))
                    .or_else(|| classic.get(group_id))
                    .cloned()
            })
            .collect();

        let unthrottled = ConsumerGroupDescribeResponse::default().throttle_time_ms(0);
        Ok(unthrottled.groups(Some(groups)))
    }
}
