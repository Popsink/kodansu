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

//! A topic through its whole life over the API: created, found by id and by
//! name, refused a second time, deleted, and unknown once deleted.

use std::sync::Arc;

use rand::distr::{Alphanumeric, SampleString as _};
use rand::{RngExt as _, rng};
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::metadata_request::MetadataRequestTopic;
use tansu_sans_io::metadata_response::MetadataResponseTopic;
use tansu_sans_io::{CreateTopicsRequest, DeleteTopicsRequest, ErrorCode, MetadataRequest};
use tansu_storage::{CreateTopicsService, DeleteTopicsService, MetadataService, Storage};

use crate::common::{Error, init_tracing, serve, storage};

mod common;

/// What `CreateTopics` answers for one topic: its error and its id.
async fn create(
    storage: &Arc<dyn Storage>,
    name: &str,
    partitions: i32,
    replicas: i16,
) -> Result<(ErrorCode, Option<[u8; 16]>), Error> {
    let creatable = CreatableTopic::default()
        .name(name.to_owned())
        .num_partitions(partitions)
        .replication_factor(replicas)
        .assignments(Some(Vec::new()))
        .configs(Some(Vec::new()));
    let request = CreateTopicsRequest::default().topics(Some(vec![creatable]));
    let response = serve(storage, CreateTopicsService, request).await?;
    let [topic] = <[_; 1]>::try_from(response.topics.unwrap_or_default()).expect("one topic");
    Ok((ErrorCode::try_from(topic.error_code)?, topic.topic_id))
}

/// The one topic `Metadata` answers for `wanted`.
async fn described(
    storage: &Arc<dyn Storage>,
    wanted: MetadataRequestTopic,
) -> Result<MetadataResponseTopic, Error> {
    let request = MetadataRequest::default().topics(Some(vec![wanted]));
    let response = serve(storage, MetadataService, request).await?;
    let [topic] = <[_; 1]>::try_from(response.topics.unwrap_or_default()).expect("one topic");
    Ok(topic)
}

/// What `DeleteTopics` answers for `name`.
async fn delete(storage: &Arc<dyn Storage>, name: &str) -> Result<ErrorCode, Error> {
    let request = DeleteTopicsRequest::default().topic_names(Some(vec![name.to_owned()]));
    let response = serve(storage, DeleteTopicsService, request).await?;
    let [result] = <[_; 1]>::try_from(response.responses.unwrap_or_default()).expect("one result");
    Ok(ErrorCode::try_from(result.error_code)?)
}

/// The name and the shape are random, so the lifecycle does not depend on any
/// one of them.
#[tokio::test]
async fn a_topic_from_creation_to_deletion() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let name = Alphanumeric.sample_string(&mut rng(), 15);
    let partitions = rng().random_range(1..64);
    let replicas = rng().random_range(0..64);

    let (_, id) = create(&storage, &name, partitions, replicas).await?;
    let id = id.expect("a created topic has an id");

    let by_id = MetadataRequestTopic::default().topic_id(Some(id));
    let by_name = MetadataRequestTopic::default().name(Some(name.clone()));
    for wanted in [by_id, by_name] {
        let topic = described(&storage, wanted).await?;
        assert_eq!(
            (ErrorCode::None, Some(name.as_str()), Some(id)),
            (
                ErrorCode::try_from(topic.error_code)?,
                topic.name.as_deref(),
                topic.topic_id
            )
        );
    }

    let (again, _) = create(&storage, &name, partitions, replicas).await?;
    assert_eq!(ErrorCode::TopicAlreadyExists, again);

    let deleted = delete(&storage, &name).await?;
    let again = delete(&storage, &name).await?;
    assert_eq!(
        (ErrorCode::None, ErrorCode::UnknownTopicOrPartition),
        (deleted, again)
    );

    Ok(())
}
