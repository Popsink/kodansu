// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use common::{alphanumeric_string, init_tracing, register_broker};
use rama::{Context, Layer as _, Service};
use rand::{prelude::*, rng};
use tansu_broker::{Error, Result, service::storage};
use tansu_sans_io::{
    Ack, CreateTopicsRequest, ErrorCode, FetchRequest, IsolationLevel, ListOffset,
    ListOffsetsRequest, NULL_TOPIC_ID, ProduceRequest,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    fetch_request::{FetchPartition, FetchTopic, ReplicaState},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Header, Record, deflated, inflated},
};
use tansu_service::{
    BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
    FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService,
};
use tansu_storage::Storage;
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub mod common;

const CLEANUP_POLICY: &str = "cleanup.policy";
const COMPACT: &str = "compact";
const DELETE: &str = "delete";
const RETENTION_MS: &str = "retention.ms";

/// Wider than the one partition these cases produce into, so every
/// `ListOffsets` answer carries five empty partitions alongside it: a
/// maintenance pass that reached a partition it had no records in shows up
/// there, and nowhere else.
const NUM_PARTITIONS: i32 = 6;

/// The partition every case produces into.
const PARTITION: i32 = 0;

const TIMEOUT_MS: i32 = 5_000;

/// Far above what three small records need, so a fetch never stops short of
/// the log's end — a truncated response and a compacted one are the same shape
/// to [`fetched`], and only one of them is what a case is asserting.
const MAX_BYTES: i32 = 64 * 1024;

/// One record header, on every record every case produces.
///
/// Headers are not what any case here is about, and no case varies them: they
/// are here so that the produce-maintain-fetch path a case walks carries one,
/// and [`fetched`] asserts it survived. `compact_only` was the only test in
/// the tree that put a header on the wire before #553, and it never looked at
/// it again.
const HEADER_KEY: &[u8] = b"x";
const HEADER_VALUE: &[u8] = b"y";

const ALPHA: &[u8] = b"alpha";
const BETA: &[u8] = b"beta";
const ONE: &[u8] = b"one";
const TWO: &[u8] = b"two";
const THREE: &[u8] = b"three";

/// One record as [`fetched`] yields it: key, value, and absolute offset.
type Fetched = (Option<Bytes>, Option<Bytes>, i64);

type Broker = RequestFrameService<
    FrameBytesService<BytesService<BytesFrameService<FrameRouteService<(), Error>>>>,
>;

fn broker<S>(storage: S) -> Result<Broker>
where
    S: Storage + Clone,
{
    storage::services(FrameRouteService::<(), Error>::builder(), storage)
        .inspect(|builder| debug!(?builder))
        .and_then(|builder| builder.build().map_err(Into::into))
        .map(|frame_route| {
            (
                RequestFrameLayer,
                FrameBytesLayer,
                BytesLayer,
                BytesFrameLayer::default(),
            )
                .into_layer(frame_route)
        })
}

async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<Arc<dyn Storage>> {
    common::storage_container(cluster, node, Url::parse("tcp://127.0.0.1/")?).await
}

/// A broker over its own `memory://` container, a topic name nothing else
/// uses, and that topic's id.
async fn broker_with_topic<V>(
    configs: &[(&str, V)],
) -> Result<(Arc<dyn Storage>, Broker, String, [u8; 16])>
where
    V: AsRef<str> + Debug,
{
    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);

    let sc = storage_container(cluster_id, broker_id).await?;
    register_broker(cluster_id, broker_id, sc.clone()).await?;

    let broker = broker(sc.clone())?;

    let topic_name = alphanumeric_string(15);
    debug!(?topic_name, ?configs);

    let topic_id = create_topic(&broker, &topic_name, configs).await?;

    Ok((sc, broker, topic_name, topic_id))
}

async fn create_topic<V>(broker: &Broker, name: &str, configs: &[(&str, V)]) -> Result<[u8; 16]>
where
    V: AsRef<str>,
{
    let response = broker
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .timeout_ms(TIMEOUT_MS)
                .validate_only(Some(false))
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(NUM_PARTITIONS)
                        .replication_factor(0)
                        .assignments(Some([].into()))
                        .configs(Some(
                            configs
                                .iter()
                                .map(|(name, value)| {
                                    CreatableTopicConfig::default()
                                        .name((*name).into())
                                        .value(Some(value.as_ref().into()))
                                })
                                .collect(),
                        )),
                ])),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(i16::from(ErrorCode::None), topics[0].error_code);

    Ok(topics[0].topic_id.unwrap_or(NULL_TOPIC_ID))
}

/// One keyed record through `Produce`, stamped `at`, answering the offset it
/// was assigned.
///
/// The stamp is the only handle these cases have on retention: whole-segment
/// expiry decides from the newest record timestamp a segment holds, so a record
/// stamped in the past is how a case straddles the threshold without waiting
/// for it.
async fn produce_keyed(
    broker: &Broker,
    name: &str,
    key: &'static [u8],
    value: &'static [u8],
    at: SystemTime,
) -> Result<i64> {
    let timestamp = at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Message(String::from("a timestamp before the epoch")))
        .and_then(|since| {
            i64::try_from(since.as_millis())
                .map_err(|_| Error::Message(String::from("a timestamp past i64 milliseconds")))
        })?;

    let frame = inflated::Batch::builder()
        .base_timestamp(timestamp)
        .max_timestamp(timestamp)
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(key)))
                .value(Some(Bytes::from_static(value)))
                .header(
                    Header::builder()
                        .key(Bytes::from_static(HEADER_KEY))
                        .value(Bytes::from_static(HEADER_VALUE)),
                ),
        )
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let response = broker
        .serve(
            Context::default(),
            ProduceRequest::default()
                .timeout_ms(TIMEOUT_MS)
                .acks(Ack::Leader.into())
                .topic_data(Some(vec![
                    TopicProduceData::default()
                        .name(name.into())
                        .partition_data(Some(vec![
                            PartitionProduceData::default()
                                .index(PARTITION)
                                .records(Some(frame)),
                        ])),
                ])),
        )
        .await
        .inspect(|response| debug!("{response:?}"))?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name);
    let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(PARTITION, partitions[0].index);
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);

    Ok(partitions[0].base_offset)
}

/// `(error code, offset, timestamp)` of the one partition with records, having
/// checked that every other partition came back empty at both ends.
async fn list_offset(
    broker: &Broker,
    name: &str,
    offset: ListOffset,
) -> Result<(i16, Option<i64>, Option<i64>)> {
    let timestamp = offset.try_into()?;

    let response = broker
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .replica_id(-1)
                .topics(Some(vec![
                    ListOffsetsTopic::default()
                        .name(name.into())
                        .partitions(Some(
                            (0..NUM_PARTITIONS)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(Some(NUM_PARTITIONS))
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(-1))
                                })
                                .collect::<Vec<_>>(),
                        )),
                ])),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(NUM_PARTITIONS as usize, partitions.len());

    for partition in &partitions[1..] {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    Ok((
        partitions[0].error_code,
        partitions[0].offset,
        partitions[0].timestamp,
    ))
}

/// `(error code, [(key, value, absolute offset)])` for a `Fetch` from
/// `fetch_offset`.
///
/// Every batch is flattened into one list, keyed on `base_offset +
/// offset_delta`, so the assertions hold however the storage layer chose to
/// group the records. A delta alone does not: it is relative to its own batch,
/// so the same three records read `0, 1, 2` under one coalesced batch and
/// `0, 0, 0` under one batch per produce request. The four bodies #552 deleted
/// asserted the deltas of the last batch only, which is why they read one
/// record where the log held three (#560).
async fn fetched(
    broker: &Broker,
    name: &str,
    topic_id: [u8; 16],
    fetch_offset: i64,
) -> Result<(i16, Vec<Fetched>)> {
    let response = broker
        .serve(
            Context::default(),
            FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(MAX_BYTES))
                .min_bytes(1)
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(PARTITION)
                                .current_leader_epoch(Some(-1))
                                .fetch_offset(fetch_offset)
                                .last_fetched_epoch(Some(-1))
                                .log_start_offset(Some(0))
                                .partition_max_bytes(MAX_BYTES),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
        )
        .await?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(PARTITION, partitions[0].partition_index);

    let mut records = vec![];

    for deflated in partitions[0]
        .records
        .iter()
        .flat_map(|frame| frame.batches.iter())
    {
        let batch = inflated::Batch::try_from(deflated)?;

        for record in &batch.records {
            assert_eq!(
                vec![Header {
                    key: Some(Bytes::from_static(HEADER_KEY)),
                    value: Some(Bytes::from_static(HEADER_VALUE)),
                }],
                record.headers,
            );

            records.push((
                record.key.clone(),
                record.value.clone(),
                batch.base_offset + i64::from(record.offset_delta),
            ));
        }
    }

    Ok((partitions[0].error_code, records))
}

/// The shape `fetched` answers, from `(key, value, absolute offset)` triples.
fn survivors(expected: &[(&'static [u8], &'static [u8], i64)]) -> (i16, Vec<Fetched>) {
    (
        i16::from(ErrorCode::None),
        expected
            .iter()
            .map(|(key, value, offset)| {
                (
                    Some(Bytes::from_static(key)),
                    Some(Bytes::from_static(value)),
                    *offset,
                )
            })
            .collect(),
    )
}

/// `(EARLIEST offset, LATEST offset)` for the one partition with records.
///
/// The three answers every case agrees on are asserted here rather than at
/// each call: both queries succeed, `EARLIEST` carries `-1` because it is an
/// offset lookup and not a timestamp one (#177), and `LATEST` carries a real
/// record timestamp. What is left is the pair of numbers the cases differ on.
///
/// An emptied log answers `LATEST` with `-1` too, so the one case that empties
/// a log asserts both queries itself.
async fn log_bounds(broker: &Broker, name: &str) -> Result<(Option<i64>, Option<i64>)> {
    let (error_code, earliest, timestamp) = list_offset(broker, name, ListOffset::Earliest).await?;
    assert_eq!(i16::from(ErrorCode::None), error_code);
    assert_eq!(Some(-1), timestamp);

    let (error_code, latest, timestamp) = list_offset(broker, name, ListOffset::Latest).await?;
    assert_eq!(i16::from(ErrorCode::None), error_code);
    assert!(timestamp.is_some_and(|timestamp| timestamp > 0));

    Ok((earliest, latest))
}

/// `cleanup.policy=compact,delete` at `retention`, as [`create_topic`] takes
/// its configs.
fn compact_delete(retention: Duration) -> [(&'static str, String); 2] {
    [
        (CLEANUP_POLICY, [COMPACT, DELETE].join(",")),
        (RETENTION_MS, retention.as_millis().to_string()),
    ]
}

/// `compact` alone: the per-key pass keeps the newest value of the key and
/// takes the two it supersedes.
///
/// This is the control the `compact,delete` cases are read against, and the
/// two `ListOffsets` pairs around the pass are what it adds to them:
/// compaction removes records without moving either end of the log. Kafka's
/// rule is that only retention and `DeleteRecords` advance the log start, so
/// `EARLIEST` stays at 0 over records that are no longer there, and `LATEST`
/// stays at 3.
///
/// It asserted `EARLIEST == 2` while the legacy in-place compactor did the
/// work: that rewrote the `records/` objects and advanced `watermark.low` with
/// them, so the log start followed the surviving record. The per-key pass over
/// segments does not, and is the conformant one, so the expectation moved
/// rather than the engine.
#[tokio::test]
async fn compact_only() -> Result<()> {
    let _guard = init_tracing()?;

    let (sc, broker, topic, topic_id) = broker_with_topic(&[(CLEANUP_POLICY, COMPACT)]).await?;

    let now = SystemTime::now();

    assert_eq!(0, produce_keyed(&broker, &topic, ALPHA, ONE, now).await?);
    assert_eq!(1, produce_keyed(&broker, &topic, ALPHA, TWO, now).await?);
    assert_eq!(2, produce_keyed(&broker, &topic, ALPHA, THREE, now).await?);

    assert_eq!((Some(0), Some(3)), log_bounds(&broker, &topic).await?);

    sc.maintain(now).await?;

    assert_eq!((Some(0), Some(3)), log_bounds(&broker, &topic).await?);

    assert_eq!(
        survivors(&[(ALPHA, THREE, 2)]),
        fetched(&broker, &topic, topic_id, 0).await?
    );

    Ok(())
}

/// `compact,delete` inside its retention window: the per-key pass keeps the
/// newest value of the key, and nothing is 30 minutes old so retention takes
/// nothing — the log still starts at 0 and still ends at 3.
///
/// This is `compact_delete_001`, which #552 deleted (#560). It had been
/// `#[ignore]`d since the SQL backends left in #96 and so had never asserted
/// anything about the object store, and it is written here against the object
/// store's answers rather than fixed up from theirs: compaction never
/// truncates, so only retention and `DeleteRecords` advance the log start,
/// which is Kafka's rule; `EARLIEST` is not a timestamp lookup, so it carries
/// the unknown sentinel rather than a positive value; and the survivor is
/// identified by its absolute offset rather than by a delta within whichever
/// batch happened to be last.
#[tokio::test]
async fn compact_delete_within_retention_keeps_the_latest_value() -> Result<()> {
    let _guard = init_tracing()?;

    let (sc, broker, topic, topic_id) =
        broker_with_topic(&compact_delete(Duration::from_mins(30))).await?;

    let now = SystemTime::now();

    assert_eq!(0, produce_keyed(&broker, &topic, ALPHA, ONE, now).await?);
    assert_eq!(1, produce_keyed(&broker, &topic, ALPHA, TWO, now).await?);
    assert_eq!(2, produce_keyed(&broker, &topic, ALPHA, THREE, now).await?);

    assert_eq!(
        survivors(&[(ALPHA, ONE, 0), (ALPHA, TWO, 1), (ALPHA, THREE, 2)]),
        fetched(&broker, &topic, topic_id, 0).await?
    );

    sc.maintain(now).await?;

    assert_eq!(
        survivors(&[(ALPHA, THREE, 2)]),
        fetched(&broker, &topic, topic_id, 0).await?
    );

    assert_eq!((Some(0), Some(3)), log_bounds(&broker, &topic).await?);

    Ok(())
}

/// One maintenance pass over records that straddle the retention threshold,
/// with a `compact`-only topic alongside as the control.
///
/// This is the distinction `compact,delete` exists for, and the one no test
/// reached end to end before (#560): retention on a compacted topic deletes a
/// key's newest — and only — value once it ages out, which `compact` alone
/// never does. `compact_only_prefix_has_no_retention_threshold` pins the
/// threshold derivation under it; this pins what a client sees.
///
/// Both topics get the same three records: `beta`'s only value stamped an hour
/// ago, then `alpha` twice at the present. One pass, and each policy takes
/// exactly one of them:
///
/// | | `compact,delete` | `compact` |
/// |---|---|---|
/// | `beta` at 0, aged out | retention takes it, log start moves to 1 | kept |
/// | `alpha` at 1, superseded | compaction takes it | compaction takes it |
/// | `alpha` at 2 | kept | kept |
///
/// The control is also what proves the pass reached it at all: `alpha`'s
/// superseded value is gone there too.
///
/// A fetch from 0 on the expiring topic is now below its log start, so this
/// reads from the `EARLIEST` the broker reports — what a consumer resetting to
/// earliest does.
#[tokio::test]
async fn compact_delete_expires_past_the_threshold_and_compacts_within_it() -> Result<()> {
    let _guard = init_tracing()?;

    let retention = Duration::from_mins(30);

    let (sc, broker, expiring, expiring_id) = broker_with_topic(&compact_delete(retention)).await?;
    let control = alphanumeric_string(15);
    let control_id = create_topic(&broker, &control, &[(CLEANUP_POLICY, COMPACT)]).await?;

    let now = SystemTime::now();
    let stale = now
        .checked_sub(retention + Duration::from_mins(30))
        .expect("an hour back");

    for (topic, id) in [(&expiring, expiring_id), (&control, control_id)] {
        assert_eq!(0, produce_keyed(&broker, topic, BETA, ONE, stale).await?);
        assert_eq!(1, produce_keyed(&broker, topic, ALPHA, TWO, now).await?);
        assert_eq!(2, produce_keyed(&broker, topic, ALPHA, THREE, now).await?);

        assert_eq!(
            survivors(&[(BETA, ONE, 0), (ALPHA, TWO, 1), (ALPHA, THREE, 2)]),
            fetched(&broker, topic, id, 0).await?
        );
    }

    sc.maintain(now).await?;

    assert_eq!(
        (i16::from(ErrorCode::None), Some(1), Some(-1)),
        list_offset(&broker, &expiring, ListOffset::Earliest).await?
    );

    assert_eq!(
        survivors(&[(ALPHA, THREE, 2)]),
        fetched(&broker, &expiring, expiring_id, 1).await?
    );

    assert_eq!(
        (i16::from(ErrorCode::None), Some(0), Some(-1)),
        list_offset(&broker, &control, ListOffset::Earliest).await?
    );

    assert_eq!(
        survivors(&[(BETA, ONE, 0), (ALPHA, THREE, 2)]),
        fetched(&broker, &control, control_id, 0).await?
    );

    Ok(())
}

/// A later pass, once everything is past retention, empties a `compact,delete`
/// log entirely.
///
/// This is `compact_delete_002`'s second maintenance. The object store answers
/// it differently from the backends #96 removed, deliberately: a partition that
/// says it begins three records before it ends while holding none of them is
/// indistinguishable from one whose segments were lost, and answering a
/// stranded consumer with silence is what left 77 of them polling for days.
/// So a consumer parked at 0 is told it is out of range, and one resetting to
/// the reported earliest reads an empty log rather than an error.
#[tokio::test]
async fn compact_delete_expires_the_whole_log() -> Result<()> {
    let _guard = init_tracing()?;

    let retention = Duration::from_mins(30);

    let (sc, broker, topic, topic_id) = broker_with_topic(&compact_delete(retention)).await?;

    let now = SystemTime::now();

    assert_eq!(0, produce_keyed(&broker, &topic, ALPHA, ONE, now).await?);
    assert_eq!(1, produce_keyed(&broker, &topic, BETA, TWO, now).await?);
    assert_eq!(2, produce_keyed(&broker, &topic, ALPHA, THREE, now).await?);

    sc.maintain(now).await?;

    assert_eq!(
        survivors(&[(BETA, TWO, 1), (ALPHA, THREE, 2)]),
        fetched(&broker, &topic, topic_id, 0).await?
    );

    let later = now
        .checked_add(retention + Duration::from_mins(30))
        .expect("an hour ahead");

    sc.maintain(later).await?;

    // An emptied log reports its start AT its end (#290), and a fetch from
    // below that start is out of range rather than empty (#337). Both readings
    // were 0 and "empty" on the backends #96 removed, and #560 restored this
    // case against the object store's rather than fixing up theirs.
    assert_eq!(
        (i16::from(ErrorCode::None), Some(3), Some(-1)),
        list_offset(&broker, &topic, ListOffset::Earliest).await?
    );

    assert_eq!(
        (i16::from(ErrorCode::None), Some(3), Some(-1)),
        list_offset(&broker, &topic, ListOffset::Latest).await?
    );

    let (error_code, records) = fetched(&broker, &topic, topic_id, 0).await?;
    assert_eq!(i16::from(ErrorCode::OffsetOutOfRange), error_code);
    assert_eq!(Vec::<Fetched>::new(), records);

    assert_eq!(survivors(&[]), fetched(&broker, &topic, topic_id, 3).await?);

    Ok(())
}
