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

use crate::common::{Error, init_tracing};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use tansu_sans_io::{
    CreateTopicsRequest, DeleteGroupsRequest, ErrorCode, create_topics_request::CreatableTopic,
};
use tansu_storage::{
    CreateTopicsService, DeleteGroupsService, OffsetCommitRequest, Storage, StorageContainer,
    Topition,
};
use url::Url;

mod common;

#[tokio::test]
async fn delete_non_existent() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(DeleteGroupsService);

    let group_id = "abcba";

    let response = service
        .serve(
            Context::default(),
            DeleteGroupsRequest::default().groups_names(Some([group_id.into()].into())),
        )
        .await?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(group_id, results[0].group_id.as_str());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);

    Ok(())
}

/// A `DeleteGroups` carrying a group id that resolves to the root of the
/// consumer tree is refused, and the committed offsets of every other group
/// survive it (#277).
///
/// The id is interpolated into a deletion prefix, and [`Path`] drops empty
/// components on normalisation — so `""`, `"/"` and `"///"` do not narrow that
/// prefix at all. Before the guard, the scan behind this call enumerated every
/// group's state object and every committed offset in the cluster, and
/// `delete_stream` deleted them: total consumer-group loss from one admin call,
/// with every consumer resetting to its `auto.offset.reset` policy.
///
/// [`Path`]: object_store::path::Path
#[tokio::test]
async fn empty_group_id_is_refused_and_keeps_committed_offsets() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await?;

    let topic = "bystander";

    let created = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage)
            .into_layer(CreateTopicsService)
            .serve(
                Context::default(),
                CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(topic.into())
                            .num_partitions(1)
                            .replication_factor(1)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                    ]))
                    .validate_only(Some(false)),
            )
            .await?
    };
    assert_eq!(
        i16::from(ErrorCode::None),
        created.topics.unwrap_or_default()[0].error_code
    );

    // Two bystander groups with committed offsets: what an over-wide deletion
    // prefix takes with it.
    let bystanders = [("group-a", 5_i64), ("group-b", 15_i64)];
    for (group, offset) in bystanders {
        let committed = storage
            .offset_commit(
                group,
                None,
                &[(
                    Topition::new(topic, 0),
                    OffsetCommitRequest::default().offset(offset),
                )],
            )
            .await?;

        assert_eq!(
            vec![(Topition::new(topic, 0), ErrorCode::None)],
            committed,
            "{group} could not commit"
        );
    }

    let refused = ["", "/", "///"];
    let response = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage)
            .into_layer(DeleteGroupsService)
            .serve(
                Context::default(),
                DeleteGroupsRequest::default().groups_names(Some(
                    refused.iter().map(|group| (*group).into()).collect(),
                )),
            )
            .await?
    };

    let results = response.results.unwrap_or_default();
    assert_eq!(refused.len(), results.len());
    for (index, result) in results.iter().enumerate() {
        assert_eq!(refused[index], result.group_id.as_str());
        assert_eq!(
            ErrorCode::InvalidGroupId,
            ErrorCode::try_from(result.error_code)?,
            "{:?} must be refused",
            refused[index]
        );
    }

    // Every bystander still has its committed offset: a deleted one fetches -1.
    for (group, offset) in bystanders {
        let fetched = storage
            .offset_fetch(Some(group), &[Topition::new(topic, 0)], Some(false))
            .await?;

        assert_eq!(
            Some(&offset),
            fetched.get(&Topition::new(topic, 0)),
            "{group} lost its committed offset to a refused group id"
        );
    }

    Ok(())
}
