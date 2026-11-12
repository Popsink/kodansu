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

//! The engine's `Fetch` against an object store where every round trip takes
//! [`LATENCY`]: how `max_wait`, `min_bytes` and `max_bytes` bound what one
//! fetch returns and how long it takes.

use std::time::{Duration, Instant};

use object_store::memory::InMemory;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use rama::{Context, Service};
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::fetch_request::{FetchPartition, FetchTopic};
use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};
use tansu_sans_io::record::{Record, deflated, inflated};
use tansu_sans_io::{CreateTopicsRequest, ErrorCode, FetchRequest, NULL_TOPIC_ID, ProduceRequest};

use crate::dynostore::{DynoStore, tests::init_tracing};
use crate::{CreateTopicsService, Error, FetchService, ProduceService};

/// One value per batch, in the order [`produce_all`] produces them. Their
/// lengths differ, so a `max_bytes` sized to the first one or two does not fit
/// a third by accident.
const VALUES: [&[u8]; 7] = [
    b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do",
    b"eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad",
    b"minim veniam, quis nostrud exercitation ullamco laboris nisi ut",
    b"aliquip ex ea commodo consequat. Duis aute irure dolor in",
    b"reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla",
    b"pariatur. Excepteur sint occaecat cupidatat non proident, sunt in",
    b"culpa qui officia deserunt mollit anim id est laborum.",
];

/// The time each put, get, copy and delimited listing takes before it reaches
/// the store. Deletes and plain listings are not delayed.
const LATENCY: Duration = Duration::from_millis(100);

const TOPIC: &str = "pqr";
const PARTITION: i32 = 0;

/// An engine over an in-memory store slowed by [`LATENCY`], with `TOPIC` created
/// through `CreateTopics` as five partitions of three replicas.
async fn engine() -> Result<DynoStore, Error> {
    let config = ThrottleConfig {
        wait_put_per_call: LATENCY,
        wait_get_per_call: LATENCY,
        wait_list_with_delimiter_per_call: LATENCY,
        ..ThrottleConfig::default()
    };
    let store = ThrottledStore::new(InMemory::new(), config);
    let engine =
        DynoStore::new("tansu", 12321, store).advertised_listener("tcp://localhost:9092".parse()?);

    let creatable = CreatableTopic::default()
        .name(TOPIC.to_owned())
        .num_partitions(5)
        .replication_factor(3)
        .assignments(Some(Vec::new()))
        .configs(Some(Vec::new()));
    let request = CreateTopicsRequest::default()
        .topics(Some(vec![creatable]))
        .validate_only(Some(false));
    let response = serve(&engine, CreateTopicsService, request).await?;

    let [created] = <[_; 1]>::try_from(response.topics.unwrap_or_default()).expect("one topic");
    assert_eq!(TOPIC, created.name);
    assert_ne!(Some(NULL_TOPIC_ID), created.topic_id);
    assert_eq!(
        (Some(5), Some(3)),
        (created.num_partitions, created.replication_factor)
    );
    assert_eq!(ErrorCode::None, ErrorCode::try_from(created.error_code)?);

    Ok(engine)
}

/// Sends `request` to `service` with `engine` as its state.
async fn serve<S: Service<DynoStore, Request, Error = Error>, Request>(
    engine: &DynoStore,
    service: S,
    request: Request,
) -> Result<S::Response, Error> {
    service
        .serve(Context::with_state(engine.clone()), request)
        .await
}

/// Produces each of [`VALUES`] to `PARTITION`, one single-record batch each.
async fn produce_all(engine: &DynoStore) -> Result<(), Error> {
    for value in VALUES {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(value.into())))
            .build()
            .and_then(deflated::Batch::try_from)?;
        let frame = deflated::Frame {
            batches: vec![batch],
        };
        let data = PartitionProduceData::default()
            .index(PARTITION)
            .records(Some(frame));
        let topic = TopicProduceData::default()
            .name(TOPIC.to_owned())
            .partition_data(Some(vec![data]));
        let request = ProduceRequest::default().topic_data(Some(vec![topic]));
        let response = serve(engine, ProduceService, request).await?;

        let [topic] =
            <[_; 1]>::try_from(response.responses.unwrap_or_default()).expect("one topic");
        let [produced] = <[_; 1]>::try_from(topic.partition_responses.unwrap_or_default())
            .expect("one partition");
        assert_eq!(ErrorCode::None, ErrorCode::try_from(produced.error_code)?);
    }
    Ok(())
}

/// How long one fetch of `PARTITION` from offset 0 took, and the value of the
/// one record in each batch it returned, in order.
async fn fetch(
    engine: &DynoStore,
    min_bytes: i32,
    max_bytes: usize,
    max_wait: Duration,
) -> Result<(Duration, Vec<Vec<u8>>), Error> {
    let wanted = FetchTopic::default()
        .topic(Some(TOPIC.to_owned()))
        .partitions(Some(vec![FetchPartition::default().partition(PARTITION)]));
    let request = FetchRequest::default()
        .topics(Some(vec![wanted]))
        .min_bytes(min_bytes)
        .max_bytes(Some(max_bytes as i32))
        .max_wait_ms(max_wait.as_millis() as i32);

    let started = Instant::now();
    let response = serve(engine, FetchService, request).await?;
    let elapsed = started.elapsed();

    let [topic] = <[_; 1]>::try_from(response.responses.unwrap_or_default()).expect("one topic");
    let [partition] =
        <[_; 1]>::try_from(topic.partitions.unwrap_or_default()).expect("one partition");
    assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);

    let batches = partition
        .records
        .map(|frame| frame.batches)
        .unwrap_or_default();
    let mut values = Vec::with_capacity(batches.len());
    for batch in &batches {
        let inflated = inflated::Batch::try_from(batch)?;
        let [record] = <[_; 1]>::try_from(inflated.records).expect("one record per batch");
        values.push(record.value.expect("a value").to_vec());
    }
    Ok((elapsed, values))
}

/// `elapsed` against the bounds a test sets, with every figure in the message.
fn assert_took(
    elapsed: Duration,
    max_wait: Duration,
    more_than: &[Duration],
    less_than: Option<Duration>,
) {
    let context = format!("elapsed: {elapsed:?}, latency: {LATENCY:?}, max wait: {max_wait:?}");
    for floor in more_than {
        assert!(elapsed > *floor, "{context}");
    }
    if let Some(ceiling) = less_than {
        assert!(elapsed < ceiling, "{context}");
    }
}

/// Up to `max_bytes` that holds exactly the first `n` of [`VALUES`].
fn bytes_of_first(n: usize) -> usize {
    VALUES[..n].iter().map(|value| value.len()).sum()
}

/// A fetch that can never be satisfied — one byte from an empty partition —
/// still answers cleanly when `max_wait` runs out, whether it is far longer
/// than a round trip or shorter.
#[tokio::test]
async fn an_empty_partition_answers_when_a_long_max_wait_runs_out() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let engine = engine().await?;
    _ = fetch(&engine, 0, 1, Duration::from_secs(5)).await?;

    Ok(())
}

#[tokio::test]
async fn an_empty_partition_answers_when_a_short_max_wait_runs_out() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let engine = engine().await?;
    _ = fetch(&engine, 0, 1, Duration::from_millis(50)).await?;

    Ok(())
}

/// With `min_bytes` 1 and a long `max_wait`, the first round trip returns the
/// whole partition, well before `max_wait`.
#[tokio::test]
async fn one_round_trip_returns_the_whole_partition_within_a_long_max_wait() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let engine = engine().await?;
    produce_all(&engine).await?;

    let max_wait = Duration::from_secs(5);
    let (elapsed, values) = fetch(&engine, 1, 52_428_800, max_wait).await?;
    assert_took(elapsed, max_wait, &[LATENCY], Some(max_wait));
    assert_eq!(VALUES.to_vec(), values);

    Ok(())
}

/// A `max_wait` of exactly one round trip expires as the first result lands.
#[tokio::test]
async fn a_max_wait_of_one_round_trip_returns_the_first_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let engine = engine().await?;
    produce_all(&engine).await?;

    let max_wait = LATENCY;
    let (elapsed, values) = fetch(&engine, 1, 52_428_800, max_wait).await?;
    assert_took(elapsed, max_wait, &[LATENCY, max_wait], None);
    assert_eq!(VALUES[..1].to_vec(), values);

    Ok(())
}

#[tokio::test]
async fn a_max_wait_of_two_round_trips_returns_the_whole_partition() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let engine = engine().await?;
    produce_all(&engine).await?;

    let max_wait = 2 * LATENCY;
    let (elapsed, values) = fetch(&engine, 1, 52_428_800, max_wait).await?;
    assert_took(elapsed, max_wait, &[LATENCY, max_wait], None);

    // Seven segments, one per produce, and a `max_bytes` far above their total —
    // so what a fetch returns here is decided by `max_wait` against the per-GET
    // latency, and nothing else.
    //
    // This asserted **2** when the region reads were serial: one round trip each,
    // so two fitted in `2 x LATENCY_INTRODUCED` and the other five were left for
    // the client to come back for. They are now issued concurrently (#426), so
    // one round trip covers all seven and the same `max_wait` returns the whole
    // sub-stream. The deadline still bounds the wall clock — the assertions above
    // are unchanged and still hold — it no longer bounds how much of the
    // partition one round trip can carry.
    //
    // `a_max_wait_of_one_round_trip_returns_the_first_batch` is the other half and is
    // unchanged at 1: a `max_wait` of exactly one round trip expires as the first
    // result lands, so the deadline check ends the loop after it. What moved is
    // the yield per round trip, not the meaning of `max_wait`.
    assert_eq!(VALUES.to_vec(), values);

    Ok(())
}

/// A `max_bytes` that holds one record returns that record alone, well before a
/// long `max_wait`.
#[tokio::test]
async fn max_bytes_for_one_record_returns_it_within_a_long_max_wait() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let engine = engine().await?;
    produce_all(&engine).await?;

    let max_wait = Duration::from_secs(5);
    let (elapsed, values) = fetch(&engine, 0, bytes_of_first(1), max_wait).await?;
    assert_took(elapsed, max_wait, &[LATENCY], Some(max_wait));
    assert_eq!(VALUES[..1].to_vec(), values);

    Ok(())
}

/// A `max_wait` shorter than one round trip still returns the record that
/// round trip brings.
#[tokio::test]
async fn max_bytes_for_one_record_returns_it_after_a_short_max_wait() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let engine = engine().await?;
    produce_all(&engine).await?;

    let max_wait = Duration::from_millis(50);
    let (elapsed, values) = fetch(&engine, 0, bytes_of_first(1), max_wait).await?;
    assert_took(elapsed, max_wait, &[max_wait, LATENCY], None);
    assert_eq!(VALUES[..1].to_vec(), values);

    Ok(())
}

#[tokio::test]
async fn max_bytes_for_two_records_returns_both_within_a_long_max_wait() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let engine = engine().await?;
    produce_all(&engine).await?;

    let max_wait = Duration::from_secs(5);
    let (elapsed, values) = fetch(&engine, 0, bytes_of_first(2), max_wait).await?;
    assert_took(elapsed, max_wait, &[LATENCY], Some(max_wait));
    assert_eq!(VALUES[..2].to_vec(), values);

    Ok(())
}

/// Room for two records, but a `max_wait` shorter than a round trip: the fetch
/// has to exceed `max_wait` to make progress at all, and returns after the
/// first.
#[tokio::test]
async fn max_bytes_for_two_records_returns_one_after_a_short_max_wait() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let engine = engine().await?;
    produce_all(&engine).await?;

    let max_wait = Duration::from_millis(50);
    let (elapsed, values) = fetch(&engine, 0, bytes_of_first(2), max_wait).await?;
    assert_took(elapsed, max_wait, &[LATENCY, max_wait], None);
    assert_eq!(VALUES[..1].to_vec(), values);

    Ok(())
}
