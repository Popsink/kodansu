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

//! `ListGroups` with no groups, and with a group that only committed offsets.

use tansu_sans_io::{ErrorCode, ListGroupsRequest};
use tansu_storage::{ListGroupsService, OffsetCommitRequest, Topition};

use crate::common::{Error, init_tracing, serve, storage, topic};

mod common;

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

    const GROUP: &str = "committed-only";
    const TOPIC: &str = "pqr";

    let storage = storage().await?;
    topic(&storage, TOPIC).await?;

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

    for states_filter in [None, Some(vec![]), Some(vec!["Empty".to_owned()])] {
        let request = ListGroupsRequest::default().states_filter(states_filter.clone());
        let listed = serve(&storage, ListGroupsService, request).await?;
        assert_eq!(ErrorCode::None, ErrorCode::try_from(listed.error_code)?);

        assert_eq!(
            vec![(GROUP.to_owned(), Some("Empty".to_owned()))],
            listed
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

/// With no group in the store, a filtered list is empty and not an error.
#[tokio::test]
async fn no_group_matches_an_empty_store() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let empty_only = ListGroupsRequest::default().states_filter(Some(vec!["Empty".to_owned()]));
    let listed = serve(&storage().await?, ListGroupsService, empty_only).await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(listed.error_code)?);
    assert_eq!(Some(Vec::new()), listed.groups);

    Ok(())
}
