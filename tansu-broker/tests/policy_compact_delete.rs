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

use std::fmt::Debug;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::init_tracing;
use rama::Context;
use rama::Layer as _;
use rama::Service as _;
use rand::RngExt as _;
use tansu_broker::Error;
use tansu_sans_io::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use tansu_sans_io::fetch_request::{FetchPartition, FetchTopic, ReplicaState};
use tansu_sans_io::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};
use tansu_sans_io::record::{Header, Record, deflated, inflated};
use tansu_sans_io::{Ack, CreateTopicsRequest, ErrorCode, FetchRequest, IsolationLevel};
use tansu_sans_io::{ListOffset, ListOffsetsRequest, NULL_TOPIC_ID, ProduceRequest};
use tansu_service::{BytesFrameLayer, BytesFrameService, BytesLayer, BytesService};
use tansu_service::{FrameBytesLayer, FrameBytesService, FrameRouteService};
use tansu_service::{RequestFrameLayer, RequestFrameService};

type Octets = bytes::Bytes;
type Engine = std::sync::Arc<dyn tansu_storage::Storage>;
type Route = FrameRouteService<(), Error>;

pub mod common;

const POLICY_KEY: &str = "cleanup.policy";
const COMPACT_POLICY: &str = "compact";
const DELETE_POLICY: &str = "delete";
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
type Fetched = (Option<Octets>, Option<Octets>, i64);

type Broker = RequestFrameService<FrameBytesService<BytesService<BytesFrameService<Route>>>>;

/// `engine`'s storage services behind the frame, bytes and request layers a
/// client's request goes through.
fn broker(engine: Engine) -> tansu_broker::Result<Broker> {
    let routes = tansu_broker::service::storage::services(Route::builder(), engine)?;
    tracing::debug!(builder = ?routes);
    let framed = (BytesLayer, BytesFrameLayer::default()).into_layer(routes.build()?);
    Ok((RequestFrameLayer, FrameBytesLayer).into_layer(framed))
}

/// A broker over its own `memory://` container, a topic name nothing else
/// uses, and that topic's id.
async fn broker_with_topic<V>(
    configs: &[(&str, V)],
) -> tansu_broker::Result<(Engine, Broker, String, [u8; 16])>
where
    V: AsRef<str> + Debug,
{
    let cluster = uuid::Uuid::now_v7();
    let node = rand::rng().random_range(0..i32::MAX);
    let listener = url::Url::parse("tcp://127.0.0.1/")?;
    let engine = common::storage_container(cluster, node, listener).await?;
    common::register_broker(cluster, node, engine.clone()).await?;
    let serving = broker(engine.clone())?;

    let name = common::alphanumeric_string(15);
    tracing::debug!(topic_name = ?name, ?configs);
    let id = create_topic(&serving, &name, configs).await?;
    Ok((engine, serving, name, id))
}

async fn create_topic<V>(
    broker: &Broker,
    name: &str,
    configs: &[(&str, V)],
) -> tansu_broker::Result<[u8; 16]>
where
    V: AsRef<str>,
{
    let config = |(key, value): &(&str, V)| {
        let config = CreatableTopicConfig::default().name((*key).into());
        config.value(Some(value.as_ref().into()))
    };
    let topic = CreatableTopic::default()
        .name(name.into())
        .num_partitions(NUM_PARTITIONS);
    let topic = topic.replication_factor(0).assignments(Some(vec![]));
    let topic = topic.configs(Some(configs.iter().map(config).collect()));
    let request = CreateTopicsRequest::default().topics(Some(vec![topic]));
    let request = request.timeout_ms(TIMEOUT_MS).validate_only(Some(false));
    let creating = broker.serve(Context::default(), request);
    let created = creating.await?;

    let [topic] = created.topics.as_deref().unwrap_or_default() else {
        panic!("one topic in {created:?}")
    };
    assert_eq!(i16::from(ErrorCode::None), topic.error_code);
    Ok(topic.topic_id.unwrap_or(NULL_TOPIC_ID))
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
) -> tansu_broker::Result<i64> {
    let since = at.duration_since(UNIX_EPOCH);
    let since = since.map_err(|_| Error::Message(String::from("a timestamp before the epoch")))?;
    let millis = i64::try_from(since.as_millis());
    let millis =
        millis.map_err(|_| Error::Message(String::from("a timestamp past i64 milliseconds")))?;

    let header = Header::builder().key(Octets::from_static(HEADER_KEY));
    let header = header.value(Octets::from_static(HEADER_VALUE));
    let record = Record::builder().key(Some(Octets::from_static(key)));
    let record = record
        .value(Some(Octets::from_static(value)))
        .header(header);
    let batch = inflated::Batch::builder()
        .base_timestamp(millis)
        .max_timestamp(millis);
    let batches = vec![batch.record(record).build()?];
    let records = deflated::Frame::try_from(inflated::Frame { batches })?;

    let partition = PartitionProduceData::default()
        .index(PARTITION)
        .records(Some(records));
    let topic = TopicProduceData::default().name(name.into());
    let topic = topic.partition_data(Some(vec![partition]));
    let request = ProduceRequest::default()
        .timeout_ms(TIMEOUT_MS)
        .acks(Ack::Leader.into());
    let producing = broker.serve(Context::default(), request.topic_data(Some(vec![topic])));
    let produced = producing.await?;
    tracing::debug!("{produced:?}");

    let [topic] = produced.responses.as_deref().unwrap_or_default() else {
        panic!("one topic in {produced:?}")
    };
    assert_eq!(name, topic.name);
    let [partition] = topic.partition_responses.as_deref().unwrap_or_default() else {
        panic!("one partition in {produced:?}")
    };
    let none = i16::from(ErrorCode::None);
    assert_eq!((PARTITION, none), (partition.index, partition.error_code));
    Ok(partition.base_offset)
}

/// `(error code, offset, timestamp)` of the one partition with records, having
/// checked that every other partition came back empty at both ends.
async fn list_offset(
    broker: &Broker,
    name: &str,
    offset: ListOffset,
) -> tansu_broker::Result<(i16, Option<i64>, Option<i64>)> {
    let timestamp = i64::try_from(offset)?;
    let partition = |index| {
        let partition = ListOffsetsPartition::default().partition_index(index);
        let partition = partition
            .max_num_offsets(Some(NUM_PARTITIONS))
            .timestamp(timestamp);
        partition.current_leader_epoch(Some(-1))
    };
    let topic = ListOffsetsTopic::default().name(name.into());
    let topic = topic.partitions(Some((0..NUM_PARTITIONS).map(partition).collect()));
    let uncommitted = Some(IsolationLevel::ReadUncommitted.into());
    let request = ListOffsetsRequest::default()
        .isolation_level(uncommitted)
        .replica_id(-1);
    let listing = broker.serve(Context::default(), request.topics(Some(vec![topic])));
    let listed = listing.await?;

    let [topic] = listed.topics.as_deref().unwrap_or_default() else {
        panic!("one topic in {listed:?}")
    };
    assert_eq!(name, topic.name);
    let partitions = topic.partitions.as_deref().unwrap_or_default();
    assert_eq!(NUM_PARTITIONS as usize, partitions.len());

    let empty = (i16::from(ErrorCode::None), Some(0), Some(-1));
    for others in &partitions[1..] {
        assert_eq!(empty, (others.error_code, others.offset, others.timestamp));
    }
    let first = &partitions[0];
    Ok((first.error_code, first.offset, first.timestamp))
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
) -> tansu_broker::Result<(i16, Vec<Fetched>)> {
    let partition = FetchPartition::default().partition(PARTITION);
    let partition = partition.fetch_offset(fetch_offset);
    let partition = partition.current_leader_epoch(Some(-1));
    let partition = partition.last_fetched_epoch(Some(-1));
    let partition = partition.log_start_offset(Some(0));
    let partition = partition.partition_max_bytes(MAX_BYTES);
    let topic = FetchTopic::default().topic(Some(name.into()));
    let topic = topic.topic_id(Some(topic_id));
    let topic = topic.partitions(Some(vec![partition]));

    let uncommitted = Some(IsolationLevel::ReadUncommitted.into());
    let follower = Some(ReplicaState::default());
    let request = FetchRequest::default().cluster_id(Some(String::new()));
    let request = request.replica_id(Some(-1)).replica_state(follower);
    let request = request.max_wait_ms(500).max_bytes(Some(MAX_BYTES));
    let request = request.min_bytes(1).isolation_level(uncommitted);
    let request = request.session_id(Some(-1)).session_epoch(Some(-1));
    let request = request.topics(Some(vec![topic]));
    let request = request.forgotten_topics_data(Some(vec![]));
    let fetching = broker.serve(Context::default(), request.rack_id(Some(String::new())));
    let response = fetching.await?;

    let [topic] = response.responses.as_deref().unwrap_or_default() else {
        panic!("one topic in {response:?}")
    };
    let [partition] = topic.partitions.as_deref().unwrap_or_default() else {
        panic!("one partition in {response:?}")
    };
    assert_eq!(PARTITION, partition.partition_index);

    let carried = Header {
        key: Some(Octets::from_static(HEADER_KEY)),
        value: Some(Octets::from_static(HEADER_VALUE)),
    };
    let mut records = vec![];
    for compressed in partition.records.iter().flat_map(|frame| &frame.batches) {
        let batch = inflated::Batch::try_from(compressed)?;
        for record in &batch.records {
            assert_eq!(std::slice::from_ref(&carried), &record.headers[..]);
            let offset = batch.base_offset + i64::from(record.offset_delta);
            records.push((record.key.clone(), record.value.clone(), offset));
        }
    }
    Ok((partition.error_code, records))
}

/// The shape `fetched` answers, from `(key, value, absolute offset)` triples.
fn survivors(expected: &[(&'static [u8], &'static [u8], i64)]) -> (i16, Vec<Fetched>) {
    (
        i16::from(ErrorCode::None),
        expected
            .iter()
            .map(|(key, value, offset)| {
                (
                    Some(Octets::from_static(key)),
                    Some(Octets::from_static(value)),
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
async fn log_bounds(
    broker: &Broker,
    name: &str,
) -> tansu_broker::Result<(Option<i64>, Option<i64>)> {
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
        (POLICY_KEY, [COMPACT_POLICY, DELETE_POLICY].join(",")),
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
async fn compact_only() -> tansu_broker::Result<()> {
    let _guard = init_tracing()?;

    let (sc, broker, topic, topic_id) = broker_with_topic(&[(POLICY_KEY, COMPACT_POLICY)]).await?;

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
async fn compact_delete_within_retention_keeps_the_latest_value() -> tansu_broker::Result<()> {
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
async fn compact_delete_expires_past_the_threshold_and_compacts_within_it()
-> tansu_broker::Result<()> {
    let _guard = init_tracing()?;

    let retention = Duration::from_mins(30);

    let (sc, broker, expiring, expiring_id) = broker_with_topic(&compact_delete(retention)).await?;
    let control = common::alphanumeric_string(15);
    let control_id = create_topic(&broker, &control, &[(POLICY_KEY, COMPACT_POLICY)]).await?;

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
async fn compact_delete_expires_the_whole_log() -> tansu_broker::Result<()> {
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
