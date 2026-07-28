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
use bytes::Bytes;
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use rand::{distr::Alphanumeric, prelude::*, rng};
use tansu_sans_io::{
    CreateTopicsRequest, DeleteTopicsRequest, ErrorCode, InitProducerIdRequest, IsolationLevel,
    ListOffset, ListOffsetsRequest, ProduceRequest, ProduceResponse,
    create_topics_request::CreatableTopic,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use tansu_storage::{
    CreateTopicsService, DeleteTopicsService, InitProducerIdService, ListOffsetsService,
    ProduceService, Storage as _, StorageContainer, Topition,
};
use tracing::debug;
use url::Url;
use uuid::Uuid;

mod common;

fn topic_data(
    topic: &str,
    index: i32,
    builder: inflated::Builder,
) -> Result<Option<Vec<TopicProduceData>>, Error> {
    builder
        .build()
        .and_then(deflated::Batch::try_from)
        .map(|deflated| {
            let partition_data =
                PartitionProduceData::default()
                    .index(index)
                    .records(Some(Frame {
                        batches: vec![deflated],
                    }));

            Some(vec![
                TopicProduceData::default()
                    .name(topic.into())
                    .partition_data(Some(vec![partition_data])),
            ])
        })
        .map_err(Into::into)
}

#[tokio::test]
async fn non_txn_idempotent_unknown_producer_id() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await?;

    let topic = "pqr";
    let index = 0;

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    let produce = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(ProduceService)
    };

    let response = produce
        .serve(
            Context::default(),
            ProduceRequest::default()
                .transactional_id(transactional_id)
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(Record::builder().value(Bytes::from_static(b"lorem").into()))
                        .producer_id(54345),
                )?),
        )
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::UnknownProducerId.into())
                            .base_offset(-1)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    Ok(())
}

#[tokio::test]
async fn non_txn_idempotent() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await?;

    let topic = "pqr";
    let index = 0;

    let init_producer_id = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(InitProducerIdService)
    };

    let produce = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(ProduceService)
    };

    let producer = init_producer_id
        .serve(
            Context::default(),
            InitProducerIdRequest::default()
                .transactional_id(None)
                .transaction_timeout_ms(0)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
        )
        .await?;

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    let response = produce
        .serve(
            Context::default(),
            ProduceRequest::default()
                .transactional_id(transactional_id.clone())
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()),
                        )
                        .producer_id(producer.producer_id),
                )?),
        )
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::None.into())
                            .base_offset(0)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    let response = produce
        .serve(
            Context::default(),
            ProduceRequest::default()
                .transactional_id(transactional_id.clone())
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"consectetur adipiscing elit").into()),
                        )
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"sed do eiusmod tempor").into()),
                        )
                        .base_sequence(1)
                        .last_offset_delta(1)
                        .producer_id(producer.producer_id),
                )?),
        )
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::None.into())
                            .base_offset(1)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    let response = produce
        .serve(
            Context::default(),
            ProduceRequest::default()
                .transactional_id(transactional_id.clone())
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"incididunt ut labore").into()),
                        )
                        .base_sequence(3)
                        .producer_id(producer.producer_id),
                )?),
        )
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::None.into())
                            .base_offset(3)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    Ok(())
}

#[tokio::test]
async fn non_txn_idempotent_duplicate_sequence() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await?;

    let topic = "pqr";
    let index = 0;

    let init_producer_id = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(InitProducerIdService)
    };

    let produce = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(ProduceService)
    };

    let producer = init_producer_id
        .serve(
            Context::default(),
            InitProducerIdRequest::default()
                .transactional_id(None)
                .transaction_timeout_ms(0)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
        )
        .await?;

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    let response = produce
        .serve(
            Context::default(),
            ProduceRequest::default()
                .transactional_id(transactional_id.clone())
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()),
                        )
                        .producer_id(producer.producer_id),
                )?),
        )
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::None.into())
                            .base_offset(0)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    let response = produce
        .serve(
            Context::default(),
            ProduceRequest::default()
                .transactional_id(transactional_id)
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()),
                        )
                        .producer_id(producer.producer_id),
                )?),
        )
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::DuplicateSequenceNumber.into())
                            .base_offset(-1)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    Ok(())
}

#[tokio::test]
async fn non_txn_idempotent_sequence_out_of_order() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await?;

    let init_producer_id = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(InitProducerIdService)
    };

    let produce = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(ProduceService)
    };

    let topic = "pqr";
    let index = 0;

    let producer = init_producer_id
        .serve(
            Context::default(),
            InitProducerIdRequest::default()
                .transactional_id(None)
                .transaction_timeout_ms(0)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
        )
        .await?;

    let transactional_id = None;
    let acks = 0;
    let timeout_ms = 0;

    let response = produce
        .serve(
            Context::default(),
            ProduceRequest::default()
                .transactional_id(transactional_id.clone())
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()),
                        )
                        .producer_id(producer.producer_id),
                )?),
        )
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::None.into())
                            .base_offset(0)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    let response = produce
        .serve(
            Context::default(),
            ProduceRequest::default()
                .transactional_id(transactional_id)
                .acks(acks)
                .timeout_ms(timeout_ms)
                .topic_data(topic_data(
                    topic,
                    index,
                    inflated::Batch::builder()
                        .record(
                            Record::builder()
                                .value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()),
                        )
                        .base_sequence(2)
                        .producer_id(producer.producer_id),
                )?),
        )
        .await?;

    assert_eq!(
        ProduceResponse::default()
            .responses(Some(vec![
                TopicProduceResponse::default()
                    .name(topic.into())
                    .partition_responses(Some(vec![
                        PartitionProduceResponse::default()
                            .index(index)
                            .error_code(ErrorCode::OutOfOrderSequenceNumber.into())
                            .base_offset(-1)
                            .log_append_time_ms(Some(-1))
                            .log_start_offset(Some(0))
                            .record_errors(Some(vec![]))
                            .error_message(None)
                            .current_leader(None)
                    ]))
            ]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None),
        response
    );

    Ok(())
}

#[tokio::test]
async fn list_offsets() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7().to_string();
    let node_id = rng().random_range(0..i32::MAX);

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id)
        .node_id(node_id)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await?;

    let create_topic = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(CreateTopicsService::default())
    };

    let delete_topic = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(DeleteTopicsService)
    };

    let list_offsets = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(ListOffsetsService)
    };

    let produce = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(ProduceService)
    };

    let name = &rng()
        .sample_iter(&Alphanumeric)
        .take(15)
        .map(char::from)
        .collect::<String>()[..];

    let num_partitions = rng().random_range(1..64);
    let replication_factor = rng().random_range(0..64);

    {
        let response = create_topic
            .serve(
                Context::default(),
                CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
            )
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    }

    let partition = rng().random_range(0..num_partitions);

    let before_produce_earliest = {
        let response = list_offsets
            .serve(
                Context::default(),
                ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .topics(Some(
                        [ListOffsetsTopic::default()
                            .name(name.into())
                            .partitions(Some(
                                [ListOffsetsPartition::default()
                                    .partition_index(partition)
                                    .timestamp(ListOffset::Earliest.try_into()?)]
                                .into(),
                            ))]
                        .into(),
                    )),
            )
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        partitions[0].offset
    };

    let before_produce_latest = {
        let response = list_offsets
            .serve(
                Context::default(),
                ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .topics(Some(
                        [ListOffsetsTopic::default()
                            .name(name.into())
                            .partitions(Some(
                                [ListOffsetsPartition::default()
                                    .partition_index(partition)
                                    .timestamp(ListOffset::Latest.try_into()?)]
                                .into(),
                            ))]
                        .into(),
                    )),
            )
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        partitions[0].offset
    };

    let offset = {
        let deflated = inflated::Batch::builder()
            .record(
                Record::builder().value(Bytes::from_static(b"Lorem ipsum dolor sit amet").into()),
            )
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        let response = produce
            .serve(
                Context::default(),
                ProduceRequest::default().topic_data(Some(
                    [TopicProduceData::default()
                        .name(name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(partition)
                                .records(Some(Frame {
                                    batches: vec![deflated],
                                }))]
                            .into(),
                        ))]
                    .into(),
                )),
            )
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        partitions[0].base_offset
    };

    assert_eq!(before_produce_latest, Some(offset));

    let after_produce_earliest = {
        let response = list_offsets
            .serve(
                Context::default(),
                ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .topics(Some(
                        [ListOffsetsTopic::default()
                            .name(name.into())
                            .partitions(Some(
                                [ListOffsetsPartition::default()
                                    .partition_index(partition)
                                    .timestamp(ListOffset::Earliest.try_into()?)]
                                .into(),
                            ))]
                        .into(),
                    )),
            )
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        partitions[0].offset
    };

    assert_eq!(before_produce_earliest, after_produce_earliest);

    let after_produce_latest = {
        let response = list_offsets
            .serve(
                Context::default(),
                ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .topics(Some(
                        [ListOffsetsTopic::default()
                            .name(name.into())
                            .partitions(Some(
                                [ListOffsetsPartition::default()
                                    .partition_index(partition)
                                    .timestamp(ListOffset::Latest.try_into()?)]
                                .into(),
                            ))]
                        .into(),
                    )),
            )
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        partitions[0].offset
    };

    assert_eq!(Some(offset + 1), after_produce_latest);

    let response = delete_topic
        .serve(
            Context::default(),
            DeleteTopicsRequest::default().topic_names(Some([name.into()].into())),
        )
        .await?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    Ok(())
}

/// A `coalesce_batches` threshold supplied in the **storage URL** reaches the
/// prefix-coalesced leaseless flush, not only the legacy produce buffer
/// (#54, #86, #171).
///
/// `coalesce_tuning` is unit-tested at the parser (`lib.rs`) and the leaseless
/// flush is tested with tuning injected through the *builder*, but nothing
/// joins the two: whether a value written in the URL an operator actually
/// deploys survives `StorageContainer::build` and reaches
/// `flush_prefix_coalesced_leaseless`. `coalesce::tuned_batch_count_flushes_
/// before_the_linger_elapses` pins that wiring for the legacy buffer only, and
/// #171 deletes it along with the buffer.
///
/// The linger is an hour out, so the tuned count trigger firing on the third
/// enqueue is the *only* way these produces can be acked inside the test. If
/// the URL value were dropped in transit the default (64) would apply and the
/// `join!` would park on the linger — so the test cannot pass for the wrong
/// reason.
#[tokio::test]
async fn url_coalesce_batches_reaches_the_leaseless_flush() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(Url::parse(
            "memory://tansu/\
             ?prefix_coalesce=true&prefix_leaseless=true\
             &coalesce_linger=1h&coalesce_batches=3",
        )?)
        .build()
        .await?;

    // Dotted name: `prefix_of` coalesces on the first three components.
    let topic = "org.env.conn.tuned";
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;
    let topition = Topition::new(topic, 0);

    let batch = || {
        inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from_static(b"a"))))
            .record(Record::builder().value(Some(Bytes::from_static(b"b"))))
            .last_offset_delta(1)
            .build()
            .and_then(deflated::Batch::try_from)
    };

    let (a, b, c) = tokio::join!(
        storage.produce(None, &topition, batch()?),
        storage.produce(None, &topition, batch()?),
        storage.produce(None, &topition, batch()?),
    );

    let mut offsets = vec![a?, b?, c?];
    offsets.sort_unstable();
    assert_eq!(
        vec![0, 2, 4],
        offsets,
        "the three batches tile one segment contiguously"
    );

    Ok(())
}

mod doctest_template {
    use bytes::Bytes;
    use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
    use tansu_sans_io::{
        CreateTopicsRequest, ErrorCode, ProduceRequest,
        create_topics_request::CreatableTopic,
        produce_request::{PartitionProduceData, TopicProduceData},
        record::{Record, deflated::Frame, inflated},
    };
    use tansu_storage::{CreateTopicsService, ProduceService, StorageContainer};
    use url::Url;

    use crate::common::{Error, init_tracing};

    #[tokio::test]
    async fn req() -> Result<(), Error> {
        let _guard = init_tracing()?;

        const CLUSTER_ID: &str = "tansu";
        const NODE_ID: i32 = 111;
        const HOST: &str = "localhost";
        const PORT: i32 = 9092;

        let storage = StorageContainer::builder()
            .cluster_id(CLUSTER_ID)
            .node_id(NODE_ID)
            .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
            .storage(Url::parse("memory://tansu/")?)
            .build()
            .await?;

        let create_topic = {
            let storage = storage.clone();
            MapStateLayer::new(|_| storage).into_layer(CreateTopicsService::default())
        };

        let name = "abcba";

        let response = create_topic
            .serve(
                Context::default(),
                CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(5)
                            .replication_factor(3)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                    ]))
                    .validate_only(Some(false)),
            )
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        let produce = {
            let storage = storage.clone();
            MapStateLayer::new(|_| storage).into_layer(ProduceService)
        };

        let partition = 0;

        let response = produce
            .serve(
                Context::default(),
                ProduceRequest::default().topic_data(Some(
                    [TopicProduceData::default()
                        .name(name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(partition)
                                .records(Some(Frame {
                                    batches: vec![
                                        inflated::Batch::builder()
                                            .record(
                                                Record::builder().value(
                                                    Bytes::from_static(
                                                        b"Lorem ipsum dolor sit amet",
                                                    )
                                                    .into(),
                                                ),
                                            )
                                            .build()
                                            .and_then(TryInto::try_into)?,
                                    ],
                                }))]
                            .into(),
                        ))]
                    .into(),
                )),
            )
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        Ok(())
    }
}
