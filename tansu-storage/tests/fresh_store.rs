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

//! What the lookup APIs answer on a store nothing has written to: the one
//! broker is this node, and a group or topic nobody created is reported
//! missing in the way each API's clients expect.

use tansu_sans_io::describe_topic_partitions_request::TopicRequest;
use tansu_sans_io::{
    ConsumerGroupDescribeRequest, DescribeClusterRequest, DescribeGroupsRequest,
    DescribeTopicPartitionsRequest, EndpointType, ErrorCode, FindCoordinatorRequest,
    InitProducerIdRequest,
};
use tansu_storage::{
    ConsumerGroupDescribeService, DescribeClusterService, DescribeGroupsService,
    DescribeTopicPartitionsService, FindCoordinatorService, InitProducerIdService,
};

use crate::common::{Error, HOST, NODE_ID, PORT, init_tracing, serve, storage};

mod common;

#[tokio::test]
async fn the_only_broker_is_this_node() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let request = DescribeClusterRequest::default()
        .endpoint_type(Some(EndpointType::Broker.into()))
        .include_cluster_authorized_operations(false);
    let response = serve(&storage().await?, DescribeClusterService, request).await?;

    let [broker] = <[_; 1]>::try_from(response.brokers.unwrap_or_default()).expect("one broker");
    assert_eq!(
        (NODE_ID, HOST, PORT),
        (broker.broker_id, broker.host.as_str(), broker.port)
    );

    Ok(())
}

#[tokio::test]
async fn the_coordinator_is_this_node() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let request = FindCoordinatorRequest::default()
        .key(Some("abcba".to_owned()))
        .key_type(Some(0))
        .coordinator_keys(Some(vec!["xyzyx".to_owned()]));
    let response = serve(&storage().await?, FindCoordinatorService, request).await?;

    let error_code = response.error_code.map(ErrorCode::try_from).transpose()?;
    assert_eq!(
        (Some(ErrorCode::None), Some(NODE_ID), Some(HOST), Some(PORT)),
        (
            error_code,
            response.node_id,
            response.host.as_deref(),
            response.port
        )
    );

    Ok(())
}

/// Each call allocates the next producer id, starting from 1, at epoch 0 and
/// unthrottled.
#[tokio::test]
async fn each_call_allocates_the_next_producer_id() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let request = InitProducerIdRequest::default()
        .transactional_id(None)
        .transaction_timeout_ms(0)
        .producer_id(Some(-1))
        .producer_epoch(Some(-1));

    for expected in [1, 2] {
        let response = serve(&storage, InitProducerIdService, request.clone()).await?;
        let error_code = ErrorCode::try_from(response.error_code)?;
        assert_eq!(
            (0, ErrorCode::None, expected, 0),
            (
                response.throttle_time_ms,
                error_code,
                response.producer_id,
                response.producer_epoch
            )
        );
    }

    Ok(())
}

#[tokio::test]
async fn a_missing_group_is_dead() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let group_id = "abcba";
    let request = DescribeGroupsRequest::default()
        .groups(Some(vec![group_id.to_owned()]))
        .include_authorized_operations(Some(false));
    let response = serve(&storage().await?, DescribeGroupsService, request).await?;

    let [group] = <[_; 1]>::try_from(response.groups.unwrap_or_default()).expect("one group");
    assert_eq!(ErrorCode::None, ErrorCode::try_from(group.error_code)?);
    assert_eq!(group_id, group.group_id);
    // A group nothing has ever created is `Dead`, not `Empty` (#445): Kafka
    // reports `Empty` for a group that exists with nobody in it, and `Dead` for
    // one it has never heard of. Reporting the two alike left
    // cleanup-by-inactivity tooling unable to tell a group it should reap from
    // one it has already reaped.
    assert_eq!("Dead", group.group_state);

    Ok(())
}

#[tokio::test]
async fn a_missing_group_is_not_found() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let group_id = "abc";
    let request = ConsumerGroupDescribeRequest::default()
        .group_ids(Some(vec![group_id.to_owned()]))
        .include_authorized_operations(false);
    let response = serve(&storage().await?, ConsumerGroupDescribeService, request).await?;
    tracing::debug!(?response);

    // `GROUP_ID_NOT_FOUND` is what this API answers for anything it cannot
    // describe as a KIP-848 group — a group that does not exist, and equally a
    // classic group that does. Kafka answers the same for both, and the
    // AdminClient reads it as "ask `DescribeGroups` instead".
    let [group] = <[_; 1]>::try_from(response.groups.unwrap_or_default()).expect("one group");
    assert_eq!(
        ErrorCode::GroupIdNotFound,
        ErrorCode::try_from(group.error_code)?
    );
    assert_eq!(group_id, group.group_id);
    assert_eq!("Unknown", group.group_state);

    Ok(())
}

#[tokio::test]
async fn a_missing_topic_is_unknown() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let name = "abcba";
    let wanted = TopicRequest::default().name(name.to_owned());
    let request = DescribeTopicPartitionsRequest::default().topics(Some(vec![wanted]));
    let response = serve(&storage().await?, DescribeTopicPartitionsService, request).await?;

    let [topic] = <[_; 1]>::try_from(response.topics.unwrap_or_default()).expect("one topic");
    let error_code = ErrorCode::try_from(topic.error_code)?;
    assert_eq!(ErrorCode::UnknownTopicOrPartition, error_code);
    assert_eq!(Some(name), topic.name.as_deref());

    Ok(())
}
