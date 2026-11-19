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

//! `DeleteTopics`: an unknown topic by name or by id, and a topic deleted and
//! then created again under the same name.

use std::sync::Arc;

use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::create_topics_response::CreatableTopicResult;
use tansu_sans_io::delete_topics_request::DeleteTopicState;
use tansu_sans_io::delete_topics_response::DeletableTopicResult;
use tansu_sans_io::{
    CreateTopicsRequest, DeleteTopicsRequest, DeleteTopicsResponse, ErrorCode, NULL_TOPIC_ID,
};
use tansu_storage::{CreateTopicsService, DeleteTopicsService, Storage};

use crate::common::{Error, init_tracing, serve, storage};

mod common;

const TOPIC: &str = "pqr";
const PARTITIONS: i32 = 5;
const REPLICAS: i16 = 3;

/// An id no topic in a fresh store has.
const UNKNOWN_ID: [u8; 16] = [7; 16];

/// The one result of a one-topic delete, once the answer is known to be
/// unthrottled.
fn only(response: DeleteTopicsResponse) -> DeletableTopicResult {
    let DeleteTopicsResponse {
        throttle_time_ms,
        responses,
        ..
    } = response;
    assert_eq!(Some(0), throttle_time_ms);
    let [result] = <[_; 1]>::try_from(responses.unwrap_or_default()).expect("one result");
    result
}

/// `result` as `DeleteTopics` reports `error_code` for it, message included.
fn reported(error_code: ErrorCode, result: DeletableTopicResult) -> DeletableTopicResult {
    let message = Some(error_code.to_string());
    result
        .error_code(i16::from(error_code))
        .error_message(message)
}

/// What `CreateTopics` answers for `TOPIC`.
async fn create(storage: &Arc<dyn Storage>) -> Result<CreatableTopicResult, Error> {
    let creatable = CreatableTopic::default()
        .name(TOPIC.to_owned())
        .num_partitions(PARTITIONS)
        .replication_factor(REPLICAS)
        .assignments(Some(Vec::new()))
        .configs(Some(Vec::new()));
    let request = CreateTopicsRequest::default().topics(Some(vec![creatable]));
    let response = serve(storage, CreateTopicsService, request).await?;
    let [topic] = <[_; 1]>::try_from(response.topics.unwrap_or_default()).expect("one topic");
    Ok(topic)
}

/// `TOPIC` was created as asked, with no config of its own.
fn assert_created(topic: &CreatableTopicResult) {
    let configs = topic.configs.as_ref().map(Vec::len);
    assert_eq!(
        (TOPIC, ErrorCode::None.into(), Some(0), Some(0)),
        (
            topic.name.as_str(),
            topic.error_code,
            topic.topic_config_error_code,
            configs
        )
    );
    assert_eq!(
        (Some(PARTITIONS), Some(REPLICAS)),
        (topic.num_partitions, topic.replication_factor)
    );
}

#[tokio::test]
async fn an_unknown_name_is_unknown() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let request = DeleteTopicsRequest::default().topic_names(Some(vec![TOPIC.to_owned()]));
    let response = serve(&storage().await?, DeleteTopicsService, request).await?;

    let named = DeletableTopicResult::default().name(Some(TOPIC.to_owned()));
    assert_eq!(
        reported(ErrorCode::UnknownTopicOrPartition, named),
        only(response)
    );

    Ok(())
}

#[tokio::test]
async fn an_unknown_id_is_unknown() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let by_id = DeleteTopicState::default().topic_id(UNKNOWN_ID);
    let request = DeleteTopicsRequest::default().topics(Some(vec![by_id]));
    let response = serve(&storage().await?, DeleteTopicsService, request).await?;

    let identified = DeletableTopicResult::default().topic_id(Some(UNKNOWN_ID));
    assert_eq!(
        reported(ErrorCode::UnknownTopicOrPartition, identified),
        only(response)
    );

    Ok(())
}

/// A deleted name is free again: creating it a second time succeeds as the
/// first time did.
#[tokio::test]
async fn a_deleted_name_can_be_created_again() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    assert_created(&create(&storage).await?);

    let by_name = DeleteTopicState::default()
        .name(Some(TOPIC.to_owned()))
        .topic_id(NULL_TOPIC_ID);
    let request = DeleteTopicsRequest::default().topics(Some(vec![by_name]));
    let response = serve(&storage, DeleteTopicsService, request).await?;
    let deleted = DeletableTopicResult::default()
        .name(Some(TOPIC.to_owned()))
        .topic_id(Some(NULL_TOPIC_ID));
    assert_eq!(reported(ErrorCode::None, deleted), only(response));

    assert_created(&create(&storage).await?);

    Ok(())
}
