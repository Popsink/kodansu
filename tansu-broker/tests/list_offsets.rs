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

// Every case here builds a `memory://` container, which needs the one storage
// engine this fork ships. The gate was on `mod in_memory` until #552 dissolved
// it; with the module gone it belongs to the file.
#![cfg(feature = "dynostore")]
//! `ListOffsets` for every `ListOffset` variant: through the broker over a log
//! of three records produced half a second apart, and straight on the engine
//! for a new topic and a single record.

use std::time::Duration;
use std::time::SystemTime;

use rama::Context;
use rama::Layer as _;
use rama::Service as _;
use rand::RngExt as _;
use tansu_broker::Error;
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use tansu_sans_io::list_offsets_response::ListOffsetsPartitionResponse;
use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};
use tansu_sans_io::record::{Record, deflated, inflated};
use tansu_sans_io::{Ack, CreateTopicsRequest, ErrorCode, IsolationLevel, ListOffset};
use tansu_sans_io::{ListOffsetsRequest, ProduceRequest, to_system_time, to_timestamp};
use tansu_service::{BytesFrameLayer, BytesFrameService, BytesLayer, BytesService};
use tansu_service::{FrameBytesLayer, FrameBytesService, FrameRouteService};
use tansu_service::{RequestFrameLayer, RequestFrameService};
use tansu_storage::{ListOffsetResponse, Topition};

pub mod common;

type Octets = bytes::Bytes;
type Engine = std::sync::Arc<dyn tansu_storage::Storage>;
type Route = FrameRouteService<(), Error>;
type Broker = RequestFrameService<FrameBytesService<BytesService<BytesFrameService<Route>>>>;

const PARTITIONS: i32 = 6;

/// The partition the broker case produces into.
const PRODUCED: i32 = 0;

const TIMEOUT_MS: i32 = 5_000;

/// What a partition with no records answers `EARLIEST` and `LATEST` with:
/// no error, offset 0, and the "unknown" timestamp.
fn empty() -> (i16, Option<i64>, Option<i64>) {
    (ErrorCode::None.into(), Some(0), Some(-1))
}

/// A `memory://` engine with one broker, at a random node id, registered.
async fn registered() -> tansu_broker::Result<Engine> {
    let cluster = uuid::Uuid::now_v7();
    let node = rand::rng().random_range(0..i32::MAX);
    let listener = url::Url::parse("tcp://127.0.0.1/")?;
    let engine = common::storage_container(cluster, node, listener).await?;
    common::register_broker(cluster, node, engine.clone()).await?;
    Ok(engine)
}

/// `engine`'s storage services behind the frame, bytes and request layers a
/// client's request goes through.
fn broker(engine: Engine) -> tansu_broker::Result<Broker> {
    let routes = tansu_broker::service::storage::services(Route::builder(), engine)?;
    tracing::debug!(builder = ?routes);
    let framed = (BytesLayer, BytesFrameLayer::default()).into_layer(routes.build()?);
    Ok((RequestFrameLayer, FrameBytesLayer).into_layer(framed))
}

/// A randomly named topic of six unreplicated partitions, straight on `engine`.
async fn created_on(engine: &Engine) -> tansu_broker::Result<String> {
    let name = common::alphanumeric_string(15);
    tracing::debug!(topic_name = ?name);
    let topic = CreatableTopic::default()
        .name(name.clone())
        .num_partitions(PARTITIONS);
    let topic = topic
        .replication_factor(0)
        .assignments(Some(vec![]))
        .configs(Some(vec![]));
    let creating = engine.create_topic(topic, false);
    let id = creating.await?;
    tracing::debug!(topic_id = ?id);
    Ok(name)
}

/// `offset` for every partition of `topic`, read uncommitted on `engine`.
async fn listed_on(
    engine: &Engine,
    topic: &str,
    offset: ListOffset,
) -> tansu_broker::Result<Vec<(Topition, ListOffsetResponse)>> {
    let wanted: Vec<_> = (0..PARTITIONS)
        .map(|i| (Topition::new(topic, i), offset))
        .collect();
    let listing = engine.list_offsets(IsolationLevel::ReadUncommitted, &wanted);
    let listed = listing.await?;
    tracing::debug!(response = ?listed);
    assert!(!listed.is_empty());
    Ok(listed)
}

/// `timestamp` for every partition of `topic` through `broker`, read
/// uncommitted, having checked the answer names the topic and all six.
async fn listed(
    broker: &Broker,
    topic: &str,
    timestamp: i64,
) -> tansu_broker::Result<Vec<ListOffsetsPartitionResponse>> {
    let partition = |index| {
        let partition = ListOffsetsPartition::default().partition_index(index);
        let partition = partition
            .max_num_offsets(Some(PARTITIONS))
            .timestamp(timestamp);
        partition.current_leader_epoch(Some(-1))
    };
    let asked = ListOffsetsTopic::default().name(topic.into());
    let asked = asked.partitions(Some((0..PARTITIONS).map(partition).collect()));
    let uncommitted = Some(IsolationLevel::ReadUncommitted.into());
    let request = ListOffsetsRequest::default()
        .isolation_level(uncommitted)
        .replica_id(-1);
    let listing = broker.serve(Context::default(), request.topics(Some(vec![asked])));
    let answer = listing.await?;

    let [answered] = answer.topics.as_deref().unwrap_or_default() else {
        panic!("one topic in {answer:?}")
    };
    assert_eq!(topic, answered.name);
    let partitions = answered.partitions.clone().unwrap_or_default();
    assert_eq!(PARTITIONS as usize, partitions.len());
    Ok(partitions)
}

fn triple(partition: &ListOffsetsPartitionResponse) -> (i16, Option<i64>, Option<i64>) {
    (partition.error_code, partition.offset, partition.timestamp)
}

/// Asserts that the produced partition answers no error, `offset`, and a real
/// record timestamp.
fn assert_found_at(partitions: &[ListOffsetsPartitionResponse], offset: i64) {
    let found = &partitions[0];
    let none = i16::from(ErrorCode::None);
    assert_eq!((none, Some(offset)), (found.error_code, found.offset));
    assert!(
        found.timestamp.is_some_and(|timestamp| timestamp > 0),
        "{found:?}"
    );
}

/// Asserts that every partition but the produced one answers a timestamp
/// query with no offset.
fn assert_none_elsewhere(partitions: &[ListOffsetsPartitionResponse]) {
    let none = i16::from(ErrorCode::None);
    for partition in &partitions[1..] {
        // No record at or after the target, because this partition has
        // none at all: `-1`, the "no such offset" sentinel, beside the
        // `-1` timestamp below (#444). It used to answer `0` here — a real
        // position meaning the very beginning — so a client seeking to it
        // replayed the whole partition.
        assert_eq!((none, Some(-1), Some(-1)), triple(partition));
    }
}

/// Produces `value` alone to [`PRODUCED`] through `broker`, returning the
/// offset it was given.
async fn produced(broker: &Broker, topic: &str, value: &'static [u8]) -> tansu_broker::Result<i64> {
    let record = Record::builder().value(Some(Octets::from_static(value)));
    let batches = vec![inflated::Batch::builder().record(record).build()?];
    let records = deflated::Frame::try_from(inflated::Frame { batches })?;

    let partition = PartitionProduceData::default()
        .index(PRODUCED)
        .records(Some(records));
    let data = TopicProduceData::default().name(topic.into());
    let data = data.partition_data(Some(vec![partition]));
    let request = ProduceRequest::default()
        .timeout_ms(TIMEOUT_MS)
        .acks(Ack::Leader.into());
    let producing = broker.serve(Context::default(), request.topic_data(Some(vec![data])));
    let answer = producing.await?;
    tracing::debug!("{answer:?}");

    let [answered] = answer.responses.as_deref().unwrap_or_default() else {
        panic!("one topic in {answer:?}")
    };
    assert_eq!(topic, answered.name);
    let [partition] = answered.partition_responses.as_deref().unwrap_or_default() else {
        panic!("one partition in {answer:?}")
    };
    let none = i16::from(ErrorCode::None);
    assert_eq!((PRODUCED, none), (partition.index, partition.error_code));
    Ok(partition.base_offset)
}

/// Waits half a second, so each record lands at a distinct millisecond, and
/// returns the time after.
async fn a_moment_later() -> SystemTime {
    tokio::time::sleep(Duration::from_millis(500)).await;
    SystemTime::now()
}

#[tokio::test]
async fn every_variant_over_three_records() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let serving = broker(registered().await?)?;
    let topic = common::alphanumeric_string(15);
    tracing::debug!(topic_name = ?topic);

    let created = CreatableTopic::default().name(topic.clone());
    let created = created.num_partitions(PARTITIONS).replication_factor(0);
    let created = created.configs(Some(vec![])).assignments(Some(vec![]));
    let request = CreateTopicsRequest::default().timeout_ms(TIMEOUT_MS);
    let request = request.validate_only(Some(false));
    let request = request.topics(Some(vec![created]));
    let creating = serving.serve(Context::default(), request);
    let answer = creating.await?;
    let [answered] = answer.topics.as_deref().unwrap_or_default() else {
        panic!("one topic in {answer:?}")
    };
    assert_eq!(i16::from(ErrorCode::None), answered.error_code);

    let (earliest, latest) = (
        ListOffset::Earliest.try_into()?,
        ListOffset::Latest.try_into()?,
    );
    for (phase, end) in [
        ("empty topic: latest", latest),
        ("empty topic: earliest", earliest),
    ] {
        tracing::debug!(phase);
        for partition in listed(&serving, &topic, end).await? {
            assert_eq!(empty(), triple(&partition));
        }
    }

    let first = a_moment_later().await;
    assert_eq!(0, produced(&serving, &topic, b"one").await?);
    let second = a_moment_later().await;
    assert_eq!(1, produced(&serving, &topic, b"two").await?);
    let third = a_moment_later().await;
    assert_eq!(2, produced(&serving, &topic, b"three").await?);

    tracing::debug!(phase = "topic: uncommitted earliest offset");
    let partitions = listed(&serving, &topic, earliest).await?;
    // EARLIEST carries no record timestamp (#177), so the wire value is the
    // Kafka "unknown" sentinel, -1. The legacy `records/` path reported the tail
    // object's mtime here instead — an object-store artifact, not a record
    // timestamp, and not something Kafka answers an offset query with.
    for partition in &partitions {
        assert_eq!(empty(), triple(partition));
    }

    tracing::debug!(phase = "topic: uncommitted latest offset");
    let partitions = listed(&serving, &topic, latest).await?;
    assert_found_at(&partitions, 3);
    for partition in &partitions[1..] {
        assert_eq!(empty(), triple(partition));
    }

    for (offset, at) in [(0, first), (1, second), (2, third)] {
        let timestamp = ListOffset::Timestamp(at).try_into()?;
        tracing::debug!(offset, timestamp);
        let partitions = listed(&serving, &topic, timestamp).await?;
        assert_found_at(&partitions, offset);
        assert_none_elsewhere(&partitions);
    }
    Ok(())
}

#[tokio::test]
async fn a_new_topic_has_nothing_to_seek_to() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let engine = registered().await?;
    let topic = created_on(&engine).await?;

    for end in [ListOffset::Latest, ListOffset::Earliest] {
        for (_, answer) in listed_on(&engine, &topic, end).await? {
            assert_eq!((Some(0), None), (answer.offset, answer.timestamp));
        }
    }

    let epoch = ListOffset::Timestamp(to_system_time(0)?);
    let answers = listed_on(&engine, &topic, epoch).await?;
    // An empty topic has no record at or after any timestamp, so
    // `offsetsForTimes` reports none rather than 0 (#177). The legacy `records/`
    // scan answered 0, pointing a seeking consumer at the start of an empty log
    // instead of telling it there was nothing to seek to.
    for (_, answer) in answers {
        assert_eq!((None, None), (answer.offset, answer.timestamp));
    }
    Ok(())
}

#[tokio::test]
async fn a_single_record_answers_each_variant() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let engine = registered().await?;
    let topic = created_on(&engine).await?;
    let topition = Topition::new(topic, rand::rng().random_range(0..PARTITIONS));

    let earlier = SystemTime::now();
    tracing::debug!(before = to_timestamp(&earlier)?);
    let value = Octets::copy_from_slice(common::alphanumeric_string(15).as_bytes());
    tokio::time::sleep(Duration::from_millis(500)).await;
    let batch = inflated::Batch::builder().record(Record::builder().value(Some(value)));
    let batch = deflated::Batch::try_from(batch.build()?)?;
    tracing::debug!(?batch);
    let producing = engine.produce(None, &topition, batch);
    assert_eq!(0, producing.await?);
    let later = SystemTime::now();
    tracing::debug!(after = to_timestamp(&later)?);

    let offset_of = async |end| -> tansu_broker::Result<Option<i64>> {
        let wanted = [(topition.clone(), end)];
        let listing = engine.list_offsets(IsolationLevel::ReadUncommitted, &wanted);
        let answers = listing.await?;
        tracing::debug!(responses = ?answers);
        assert_eq!(1, answers.len());
        Ok(answers[0].1.offset)
    };
    assert_eq!(Some(1), offset_of(ListOffset::Latest).await?);
    assert_eq!(Some(0), offset_of(ListOffset::Earliest).await?);
    assert_eq!(Some(0), offset_of(ListOffset::Timestamp(earlier)).await?);

    // A timestamp past every record has no offset at or after it, so the answer
    // is none — Kafka's `offsetsForTimes` contract (#177). The `before` case
    // above still resolves to 0, so the footer index is answering timestamp
    // queries; only the "nothing at or after" case changed.
    assert_eq!(None, offset_of(ListOffset::Timestamp(later)).await?);
    Ok(())
}
