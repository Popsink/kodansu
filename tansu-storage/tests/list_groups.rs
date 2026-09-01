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

use crate::common::{Error, cluster_id, init_tracing, storage_url};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use tansu_sans_io::{ErrorCode, ListGroupsRequest, create_topics_request::CreatableTopic};
use tansu_storage::{
    ListGroupsService, OffsetCommitRequest, Storage as _, StorageContainer, Topition,
};
use url::Url;

mod common;

#[tokio::test]
async fn req() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(storage_url()?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(ListGroupsService);

    let response = service
        .serve(
            Context::default(),
            ListGroupsRequest::default().states_filter(Some(["Empty".into()].into())),
        )
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    assert_eq!(Some([].into()), response.groups);

    Ok(())
}

/// A group that committed an offset and never joined is listed, in the state a
/// describe of it reports — and an empty `states_filter` is not a filter (#475).
///
/// The empty filter is the shape of a plain `listConsumerGroups()`: librdkafka
/// and the Java admin client both send the field with nothing in it when no
/// state was asked for, so reading it as "match nothing" answered every
/// unfiltered list with nothing at all.
#[tokio::test]
async fn a_committed_only_group_is_listed() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;
    const GROUP: &str = "committed-only";
    const TOPIC: &str = "pqr";

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(storage_url()?)
        .build()
        .await?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    for (_, error_code) in storage
        .offset_commit(
            GROUP,
            None,
            &[(
                Topition::new(TOPIC, 0),
                OffsetCommitRequest::default().offset(1_234),
            )],
        )
        .await?
    {
        assert_eq!(ErrorCode::None, error_code);
    }

    let service = MapStateLayer::new(|_| storage).into_layer(ListGroupsService);

    for states_filter in [None, Some(vec![]), Some(vec!["Empty".to_owned()])] {
        let response = service
            .serve(
                Context::default(),
                ListGroupsRequest::default().states_filter(states_filter.clone()),
            )
            .await?;

        assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

        assert_eq!(
            vec![(GROUP.to_owned(), Some("Empty".to_owned()))],
            response
                .groups
                .unwrap_or_default()
                .into_iter()
                .map(|group| (group.group_id, group.group_state))
                .collect::<Vec<_>>(),
            "states_filter: {states_filter:?}",
        );
    }

    Ok(())
}
