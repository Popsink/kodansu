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

//! `DeleteGroups` of a group that does not exist, and of ids that would
//! widen the deletion to every group.

use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::{CreateTopicsRequest, DeleteGroupsRequest, ErrorCode};
use tansu_storage::{CreateTopicsService, DeleteGroupsService, OffsetCommitRequest, Topition};

use crate::common::{Error, init_tracing, serve, storage};

mod common;

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

    let storage = storage().await?;

    let topic = "bystander";

    let bystander = CreatableTopic::default()
        .name(topic.to_owned())
        .num_partitions(1)
        .replication_factor(1)
        .assignments(Some(Vec::new()))
        .configs(Some(Vec::new()));
    let create = CreateTopicsRequest::default()
        .topics(Some(vec![bystander]))
        .validate_only(Some(false));
    let created = serve(&storage, CreateTopicsService, create).await?;
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
    let names = refused.iter().map(|group| (*group).to_owned()).collect();
    let delete = DeleteGroupsRequest::default().groups_names(Some(names));
    let response = serve(&storage, DeleteGroupsService, delete).await?;

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
            Some(offset),
            fetched
                .get(&Topition::new(topic, 0))
                .map(|committed| committed.offset),
            "{group} lost its committed offset to a refused group id"
        );
    }

    Ok(())
}

#[tokio::test]
async fn deleting_a_missing_group_reports_it_not_found() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let missing = "abcba";
    let request = DeleteGroupsRequest::default().groups_names(Some(vec![missing.to_owned()]));
    let response = serve(&storage().await?, DeleteGroupsService, request).await?;

    let [result] = response
        .results
        .unwrap_or_default()
        .try_into()
        .expect("one result");
    assert_eq!(missing, result.group_id);

    // This test's own name is the assertion: deleting a group that does not
    // exist reports that it does not exist (#445). It answered `NONE` until
    // then, because existence was inferred from a delete — and a delete of an
    // absent key succeeds, on S3 and on `InMemory` alike.
    assert_eq!(
        ErrorCode::GroupIdNotFound,
        ErrorCode::try_from(result.error_code)?
    );

    Ok(())
}
