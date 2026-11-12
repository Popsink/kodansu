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

//! `Produce`: idempotent sequencing for a producer without a transaction, the
//! offsets a produce moves, and the coalesced layout a storage URL selects.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rand::{RngExt as _, rng};
use tansu_sans_io::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};
use tansu_sans_io::produce_response::PartitionProduceResponse;
use tansu_sans_io::record::{Builder as RecordBuilder, Record, deflated, inflated};
use tansu_sans_io::{
    DeleteTopicsRequest, ErrorCode, InitProducerIdRequest, IsolationLevel, ListOffset,
    ListOffsetsRequest, ProduceRequest, ProduceResponse,
};
use tansu_storage::{
    DeleteTopicsService, InitProducerIdService, ListOffsetsService, ProduceService, Storage,
    Topition,
};

use crate::common::{
    Error, init_tracing, serve, storage, storage_with_query, topic, topic_partitioned,
};

mod common;

const TOPIC: &str = "pqr";

/// The producer id and epoch a client sends before it has been given either:
/// the only pair that allocates a producer without a transaction.
const UNASSIGNED: (i64, i16) = (-1, -1);

/// Produces the batch `builder` makes to `partition` of `name` through
/// `Produce`.
async fn produce(
    storage: &Arc<dyn Storage>,
    name: &str,
    partition: i32,
    batch: inflated::Builder,
) -> Result<ProduceResponse, Error> {
    let frame = deflated::Frame {
        batches: vec![deflate(batch)?],
    };
    let data = PartitionProduceData::default()
        .index(partition)
        .records(Some(frame));
    let topic = TopicProduceData::default()
        .name(name.to_owned())
        .partition_data(Some(vec![data]));
    let request = ProduceRequest::default().topic_data(Some(vec![topic]));
    serve(storage, ProduceService, request).await
}

/// The error and base offset of the one partition a produce to partition 0 of
/// `TOPIC` answers, once everything else in the answer is checked to be what
/// every such answer carries: unthrottled, no endpoints, no record errors, no
/// message, no leader change, no append time, and a log that starts at 0.
fn only(response: ProduceResponse) -> Result<(ErrorCode, i64), Error> {
    let ProduceResponse {
        responses,
        throttle_time_ms,
        node_endpoints,
        ..
    } = response;
    assert_eq!((Some(0), None), (throttle_time_ms, node_endpoints));

    let [topic] = <[_; 1]>::try_from(responses.unwrap_or_default()).expect("one topic");
    assert_eq!(TOPIC, topic.name);
    let [partition] =
        <[_; 1]>::try_from(topic.partition_responses.unwrap_or_default()).expect("one partition");

    assert_eq!(0, partition.index);
    let PartitionProduceResponse {
        error_code,
        base_offset,
        log_append_time_ms,
        log_start_offset,
        record_errors,
        error_message,
        current_leader,
        ..
    } = partition;
    assert_eq!((Some(-1), Some(0)), (log_append_time_ms, log_start_offset));
    assert_eq!(Some(vec![]), record_errors);
    assert_eq!((None, None), (error_message, current_leader));
    Ok((ErrorCode::try_from(error_code)?, base_offset))
}

/// A producer id from `InitProducerId`, without a transaction.
async fn init_producer(storage: &Arc<dyn Storage>) -> Result<i64, Error> {
    let (id, epoch) = UNASSIGNED;
    let request = InitProducerIdRequest::default()
        .producer_id(Some(id))
        .producer_epoch(Some(epoch));
    let response = serve(storage, InitProducerIdService, request).await?;
    Ok(response.producer_id)
}

/// A record carrying `value`.
fn record(value: &'static [u8]) -> RecordBuilder {
    Record::builder().value(Some(Bytes::from_static(value)))
}

/// A batch of one record per value, at offset deltas `0..values.len()`.
fn batch_of(values: &[&'static [u8]]) -> inflated::Builder {
    let records = values
        .iter()
        .fold(inflated::Batch::builder(), |batch, value| {
            batch.record(record(value))
        });
    records.last_offset_delta(values.len() as i32 - 1)
}

/// The batch `batch` builds, encoded as it goes on the wire.
fn deflate(batch: inflated::Builder) -> Result<deflated::Batch, Error> {
    Ok(batch.build().and_then(deflated::Batch::try_from)?)
}

/// [`batch_of`], from `producer` starting at `sequence`.
fn sequenced(producer: i64, sequence: i32, values: &[&'static [u8]]) -> inflated::Builder {
    batch_of(values)
        .base_sequence(sequence)
        .producer_id(producer)
}

/// What `ListOffsets` answers for `position` on `topition`, which must not be
/// an error.
async fn offset_at(
    storage: &Arc<dyn Storage>,
    topition: &Topition,
    position: ListOffset,
) -> Result<Option<i64>, Error> {
    let wanted = ListOffsetsPartition::default()
        .partition_index(topition.partition())
        .timestamp(position.try_into()?);
    let topic = ListOffsetsTopic::default()
        .name(topition.topic().to_owned())
        .partitions(Some(vec![wanted]));
    let request = ListOffsetsRequest::default().topics(Some(vec![topic]));
    let response = serve(storage, ListOffsetsService, request).await?;

    let [topic] = <[_; 1]>::try_from(response.topics.unwrap_or_default()).expect("one topic");
    let [answer] = <[_; 1]>::try_from(topic.partitions.unwrap_or_default()).expect("one partition");
    let error_code = ErrorCode::try_from(answer.error_code)?;
    assert_eq!(ErrorCode::None, error_code, "{position:?} on {topition:?}");
    Ok(answer.offset)
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

    let storage = storage_with_query(
        "prefix_coalesce=true&prefix_leaseless=true&coalesce_linger=1h&coalesce_batches=3",
    )
    .await?;

    // Dotted name: `prefix_of` coalesces on the first three components.
    let name = "org.env.conn.tuned";
    topic(&storage, name).await?;
    let topition = Topition::new(name, 0);

    let batch = || deflate(batch_of(&[b"a", b"b"]));

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

/// #177 acceptance: a store built from a **plain** storage URL — no query flags
/// at all — routes an eligible produce into a prefix-coalesced `segments/`
/// object and mints no `records/{offset:020}.batch` object at all.
///
/// This is the whole point of making segments mandatory: before #177 the same
/// URL produced the legacy layout, and segments were reachable only by opting
/// in with `?prefix_coalesce=true&prefix_leaseless=true`. The footer is v3
/// (#188), the format every reader in the fleet already decodes.
#[tokio::test]
async fn a_plain_url_produces_segments_and_no_legacy_object() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let name = "org.env.conn.plain";
    topic(&storage, name).await?;
    let topition = Topition::new(name, 0);

    let batch = deflate(batch_of(&[b"plain"]))?;

    assert_eq!(0, storage.produce(None, &topition, batch).await?);

    let latest = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await?;
    assert_eq!(
        Some(1),
        latest[0].1.offset,
        "the segment advanced the log end"
    );

    // Readable back through the same store, which is what actually proves the
    // segment was written and indexed rather than merely that no legacy object
    // appeared.
    let fetched = storage
        .fetch(
            &topition,
            0,
            0,
            8 * 1024 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(200),
        )
        .await?;
    assert_eq!(1, fetched.len());
    assert_eq!(0, fetched[0].base_offset);

    Ok(())
}

/// A producer with no coordinate in the log is admitted as new (#88), not
/// answered with `UnknownProducerId` — the per-pod registry that used to reject
/// it is gone. A deliberate Kafka divergence, recorded in
/// `dynostore::tests::idempotent::an_unknown_producer_is_admitted_as_new`.
#[tokio::test]
async fn non_txn_idempotent_unknown_producer_is_admitted() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let batch = sequenced(54345, 0, &[b"lorem"]);
    let answer = only(produce(&storage, TOPIC, 0, batch).await?)?;
    assert_eq!((ErrorCode::None, 0), answer);

    Ok(())
}

/// Each batch in sequence lands at the next offset: one record at 0, two at 1,
/// and one more at 3.
#[tokio::test]
async fn non_txn_sequenced_batches_land_at_consecutive_offsets() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let producer = init_producer(&storage).await?;

    let first = sequenced(producer, 0, &[b"Lorem ipsum dolor sit amet"]);
    let second = sequenced(
        producer,
        1,
        &[b"consectetur adipiscing elit", b"sed do eiusmod tempor"],
    );
    let third = sequenced(producer, 3, &[b"incididunt ut labore"]);

    let mut answers = Vec::new();
    for batch in [first, second, third] {
        answers.push(only(produce(&storage, TOPIC, 0, batch).await?)?);
    }
    let none = |offset| (ErrorCode::None, offset);
    assert_eq!(vec![none(0), none(1), none(3)], answers);

    Ok(())
}

/// A duplicate is acked with the offset the original landed at (#88), rather
/// than raising `DuplicateSequenceNumber`: a retry after an ack the client never
/// saw is idempotent, not fatal.
#[tokio::test]
async fn non_txn_idempotent_duplicate_is_acked_with_the_original_offset() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let producer = init_producer(&storage).await?;
    let batch = || sequenced(producer, 0, &[b"Lorem ipsum dolor sit amet"]);

    let original = only(produce(&storage, TOPIC, 0, batch()).await?)?;
    let retry = only(produce(&storage, TOPIC, 0, batch()).await?)?;
    assert_eq!([(ErrorCode::None, 0); 2], [original, retry]);

    Ok(())
}

/// A batch that skips a sequence number is refused, and takes no offset.
#[tokio::test]
async fn non_txn_idempotent_skipped_sequence_is_refused() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let producer = init_producer(&storage).await?;
    let batch = |sequence| sequenced(producer, sequence, &[b"Lorem ipsum dolor sit amet"]);

    let first = only(produce(&storage, TOPIC, 0, batch(0)).await?)?;
    let skipped = only(produce(&storage, TOPIC, 0, batch(2)).await?)?;
    assert_eq!((ErrorCode::None, 0), first);
    assert_eq!((ErrorCode::OutOfOrderSequenceNumber, -1), skipped);

    Ok(())
}

/// A produce lands at the partition's LATEST offset and moves it on by one,
/// and leaves EARLIEST where it was. The topic's name, its partition count and
/// the partition produced to are random.
#[tokio::test]
async fn a_produce_moves_latest_and_not_earliest() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let name = format!("produce-{:08x}", rng().random::<u32>());
    let partitions = rng().random_range(1..64);
    _ = topic_partitioned(&storage, &name, partitions).await?;
    let partition = rng().random_range(0..partitions);
    let topition = Topition::new(name.as_str(), partition);

    let earliest = offset_at(&storage, &topition, ListOffset::Earliest).await?;
    let latest = offset_at(&storage, &topition, ListOffset::Latest).await?;

    let batch = batch_of(&[b"Lorem ipsum dolor sit amet"]);
    let response = produce(&storage, &name, partition, batch).await?;
    let [data] = <[_; 1]>::try_from(response.responses.unwrap_or_default()).expect("one topic");
    let [landed] =
        <[_; 1]>::try_from(data.partition_responses.unwrap_or_default()).expect("one partition");
    let offset = landed.base_offset;
    assert_eq!(latest, Some(offset));

    let after = (
        offset_at(&storage, &topition, ListOffset::Earliest).await?,
        offset_at(&storage, &topition, ListOffset::Latest).await?,
    );
    assert_eq!((earliest, Some(offset + 1)), after);

    let request = DeleteTopicsRequest::default().topic_names(Some(vec![name]));
    let response = serve(&storage, DeleteTopicsService, request).await?;
    let [deleted] = <[_; 1]>::try_from(response.responses.unwrap_or_default()).expect("one topic");
    assert_eq!(ErrorCode::None, ErrorCode::try_from(deleted.error_code)?);

    Ok(())
}
