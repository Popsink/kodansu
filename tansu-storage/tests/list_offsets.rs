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

//! `ListOffsets`: the named positions, a consumer's whole assignment in one
//! request, and `offsetsForTimes`.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tansu_sans_io::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use tansu_sans_io::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};
use tansu_sans_io::record::{Record, deflated, inflated::Batch};
use tansu_sans_io::{ErrorCode, ListOffset, ListOffsetsRequest, ProduceRequest};
use tansu_storage::{ListOffsetsService, ProduceService, Storage};

use crate::common::{Error, init_tracing, serve, storage, storage_with_query, topic};

mod common;

/// The topics in a wide assignment.
const TOPICS: usize = 64;

/// The number of records produced to topic `i`: distinct per topic (mod a
/// small prime) so a response attributed to the wrong partition cannot
/// accidentally carry the right high watermark.
fn records_in(i: usize) -> i64 {
    (i % 5) as i64 + 1
}

/// One batch of `count` records valued `{prefix}-{n}`, at offset deltas
/// `0..count`.
fn batch_of(prefix: &str, count: i64) -> Result<deflated::Batch, Error> {
    let mut builder = Batch::builder().last_offset_delta(count as i32 - 1);
    for record in 0..count {
        let value = Bytes::from(format!("{prefix}-{record}"));
        builder = builder.record(
            Record::builder()
                .offset_delta(record as i32)
                .value(Some(value)),
        );
    }
    Ok(builder.build().and_then(deflated::Batch::try_from)?)
}

/// Produces `batch` to partition 0 of `name` through `Produce`, answering that
/// partition's error and base offset.
async fn produce(
    storage: &Arc<dyn Storage>,
    name: &str,
    batch: deflated::Batch,
) -> Result<(ErrorCode, i64), Error> {
    let frame = deflated::Frame {
        batches: vec![batch],
    };
    let partition = PartitionProduceData::default()
        .index(0)
        .records(Some(frame));
    let data = TopicProduceData::default()
        .name(name.to_owned())
        .partition_data(Some(vec![partition]));
    let request = ProduceRequest::default().topic_data(Some(vec![data]));
    let response = serve(storage, ProduceService, request).await?;

    let [data] = <[_; 1]>::try_from(response.responses.unwrap_or_default()).expect("one topic");
    let [partition] =
        <[_; 1]>::try_from(data.partition_responses.unwrap_or_default()).expect("one partition");
    Ok((
        ErrorCode::try_from(partition.error_code)?,
        partition.base_offset,
    ))
}

/// What one `ListOffsets` answers for partition 0 of each of `names`, all at
/// `timestamp`.
async fn offsets(
    storage: &Arc<dyn Storage>,
    names: impl IntoIterator<Item = String>,
    timestamp: i64,
) -> Result<Vec<ListOffsetsTopicResponse>, Error> {
    let wanted = names.into_iter().map(|name| {
        let partition = ListOffsetsPartition::default()
            .partition_index(0)
            .timestamp(timestamp);
        ListOffsetsTopic::default()
            .name(name)
            .partitions(Some(vec![partition]))
    });
    let request = ListOffsetsRequest::default().topics(Some(wanted.collect()));
    let response = serve(storage, ListOffsetsService, request).await?;
    Ok(response.topics.unwrap_or_default())
}

/// The one partition `ListOffsets` answers for `name` at `timestamp`.
async fn ask(
    storage: &Arc<dyn Storage>,
    name: &str,
    timestamp: i64,
) -> Result<ListOffsetsPartitionResponse, Error> {
    let topics = offsets(storage, [name.to_owned()], timestamp).await?;
    let [topic] = <[_; 1]>::try_from(topics).expect("one topic");
    let [partition] =
        <[_; 1]>::try_from(topic.partitions.unwrap_or_default()).expect("one partition");
    Ok(partition)
}

/// Creates `TOPICS` one-partition topics named by `name`, and produces
/// `records_in(i)` records to topic `i` in one batch.
async fn populate(storage: &Arc<dyn Storage>, name: impl Fn(usize) -> String) -> Result<(), Error> {
    for i in 0..TOPICS {
        let name = name(i);
        topic(storage, &name).await?;
        let produced = produce(storage, &name, batch_of(&name, records_in(i))?).await?;
        assert_eq!((ErrorCode::None, 0), produced, "{name}");
    }
    Ok(())
}

/// Asks EARLIEST and then LATEST for every topic of the assignment in one
/// request each, and checks that each topic gets its own offsets back.
async fn assert_own_offsets(
    storage: &Arc<dyn Storage>,
    name: impl Fn(usize) -> String,
    sweep: &str,
) -> Result<(), Error> {
    for position in [ListOffset::Earliest, ListOffset::Latest] {
        let topics = offsets(storage, (0..TOPICS).map(&name), position.try_into()?).await?;
        assert_eq!(TOPICS, topics.len(), "{sweep}: {position:?}");

        for i in 0..TOPICS {
            let name = name(i);
            let label = format!("{sweep}: {position:?}: {name}");
            let topic = topics
                .iter()
                .find(|topic| topic.name == name)
                .unwrap_or_else(|| panic!("{label}: no response"));
            let partitions = topic.partitions.as_deref().unwrap_or_default();
            let [partition] = partitions else {
                panic!("{label}: {} partitions", partitions.len())
            };

            let expected = match position {
                // The log start: nothing has been deleted, so 0 for every topic.
                ListOffset::Earliest => 0,
                // The high watermark: the record count produced to THIS topic.
                _ => records_in(i),
            };
            let error_code = ErrorCode::try_from(partition.error_code)?;
            assert_eq!(
                (0, ErrorCode::None, Some(expected)),
                (partition.partition_index, error_code, partition.offset),
                "{label}"
            );
        }
    }
    Ok(())
}

/// Milliseconds since the epoch, `ahead` from now.
fn millis_from_now(ahead: Duration) -> i64 {
    let then = SystemTime::now() + ahead;
    let since = then.duration_since(UNIX_EPOCH).expect("after the epoch");
    since.as_millis() as i64
}

/// EARLIEST on a partition nothing was produced to is offset 0, with no
/// timestamp.
#[tokio::test]
async fn earliest_on_an_empty_partition_is_zero() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let name = "abcba";
    let topics = offsets(
        &storage().await?,
        [name.to_owned()],
        ListOffset::Earliest.try_into()?,
    )
    .await?;

    let [topic] = <[_; 1]>::try_from(topics).expect("one topic");
    assert_eq!(name, topic.name);
    let [partition] =
        <[_; 1]>::try_from(topic.partitions.unwrap_or_default()).expect("one partition");
    let error_code = ErrorCode::try_from(partition.error_code)?;
    assert_eq!(
        (0, None, ErrorCode::None),
        (
            partition.partition_index,
            partition.old_style_offsets,
            error_code
        )
    );
    assert_eq!(
        (Some(-1), Some(0), Some(0)),
        (
            partition.timestamp,
            partition.offset,
            partition.leader_epoch
        )
    );

    Ok(())
}

/// A single `ListOffsets` carrying a consumer's whole assignment — the
/// `endOffsets(assignment)` shape that timed out at ~1500 partitions when each
/// partition was resolved with a sequential await (fix/listoffsets-scale). The
/// partitions are now resolved concurrently (bounded), so this pins down the
/// two properties concurrency must not disturb: every requested partition gets
/// a response, and each topic gets ITS OWN offsets back — EARLIEST == its
/// log-start, LATEST == its high watermark — with per-topic record counts made
/// distinct so any cross-partition mix-up of responses fails loudly.
#[tokio::test]
async fn wide_assignment_earliest_and_latest() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let name = |i: usize| format!("assignment-{i:04}");

    populate(&storage, name).await?;
    assert_own_offsets(&storage, name, "once").await
}

/// The wide-assignment shape again, under PREFIX COALESCING — the layout the
/// production `endOffsets(assignment)` timeout was observed on. Topics share
/// connector prefixes (dotted names), so LATEST/EARLIEST resolve from the
/// shared segment footer index rather than per-partition objects; the sweep is
/// repeated so the second pass exercises the warm, index-served path through
/// the full request stack. Both passes must return each topic's own exact
/// offsets: EARLIEST == its log start, LATEST == its high watermark (record
/// counts made distinct per topic so any cross-partition mix-up fails loudly).
#[tokio::test]
async fn wide_assignment_earliest_and_latest_prefix_coalesced() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const PREFIXES: usize = 4;

    let storage = storage_with_query("prefix_coalesce=true").await?;

    // Dotted names: `prefix_of` coalesces on the first three components, so
    // the 64 topics share PREFIXES connector prefixes.
    let name = |i: usize| format!("org.env.conn{}.assignment-{i:04}", i % PREFIXES);

    populate(&storage, name).await?;
    for sweep in ["cold", "warm"] {
        assert_own_offsets(&storage, name, sweep).await?;
    }

    Ok(())
}

/// `offsetsForTimes` with a timestamp no record reaches answers `-1`, not `0`
/// (#444).
///
/// The storage layer already says `None` for "no record at or after this time"
/// — the sentinel is what the wire mapping made of it. `0` is not a sentinel:
/// it is a valid position meaning the very beginning of the log, so
/// `offsets_for_times(now + 60s)` turned "resume from later" into "replay the
/// entire topic". A client that seeks to the returned offset re-reads
/// everything, at full-history scale, from a seek that looked innocent.
///
/// The `timestamp` field beside it already answered `-1`, which is what makes
/// this a mapping slip rather than a missing feature — and what made it
/// survive: a response carrying `timestamp: -1, offset: 0` looks half right.
#[tokio::test]
async fn a_timestamp_no_record_reaches_has_no_offset() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const TOPIC: &str = "offsets-for-times";
    const RECORDS: i64 = 5;

    let storage = storage().await?;
    topic(&storage, TOPIC).await?;
    for _ in 0..RECORDS {
        _ = produce(&storage, TOPIC, batch_of("record", 1)?).await?;
    }

    // An hour from now: no record is at or after it, and none ever will be in
    // this test.
    let future = millis_from_now(Duration::from_secs(3_600));

    let answered = ask(&storage, TOPIC, future).await?;
    let error_code = ErrorCode::try_from(answered.error_code)?;
    assert_eq!(
        (ErrorCode::None, Some(-1), Some(-1)),
        (error_code, answered.offset, answered.timestamp),
        "no matching record is an answer, not an error, and it has no offset",
    );

    // A timestamp every record is at or after still resolves to a real offset,
    // so the sentinel has not swallowed the working case.
    let matched = ask(&storage, TOPIC, 0).await?;
    let error_code = ErrorCode::try_from(matched.error_code)?;
    assert_eq!((ErrorCode::None, Some(0)), (error_code, matched.offset));

    // And the two named positions are unaffected: they never answer `None`, so
    // nothing about them goes through the sentinel.
    let earliest = ask(&storage, TOPIC, ListOffset::Earliest.try_into()?).await?;
    let latest = ask(&storage, TOPIC, ListOffset::Latest.try_into()?).await?;
    assert_eq!((Some(0), Some(RECORDS)), (earliest.offset, latest.offset));

    Ok(())
}

/// `offsetsForTimes` resolves to the first RECORD at or after the target, not
/// to the base offset of the batch holding it (#577).
///
/// Ten records with one second between them, produced in a single batch, and a
/// target halfway through: Kafka answers offset 5, this answered 0 — the whole
/// batch rewound, because the search stopped at the segment the footer index
/// named and took its base offset. One record per batch happened to answer
/// correctly, which is what made this look like a working timestamp index.
#[tokio::test]
async fn a_timestamp_resolves_inside_the_batch_holding_it() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const TOPIC: &str = "offsets-for-times-in-batch";
    const RECORDS: i32 = 10;

    let storage = storage().await?;
    topic(&storage, TOPIC).await?;

    // One second between records (#577), so a target between two of them is
    // unambiguous.
    let base_timestamp = millis_from_now(Duration::ZERO);
    let at = |record: i32| base_timestamp + i64::from(record) * 1_000;

    let mut builder = Batch::builder()
        .base_timestamp(base_timestamp)
        .max_timestamp(at(RECORDS - 1))
        .last_offset_delta(RECORDS - 1);
    for record in 0..RECORDS {
        let value = Bytes::from(format!("record-{record}"));
        builder = builder.record(
            Record::builder()
                .offset_delta(record)
                .timestamp_delta(i64::from(record) * 1_000)
                .value(Some(value)),
        );
    }
    _ = produce(
        &storage,
        TOPIC,
        builder.build().and_then(deflated::Batch::try_from)?,
    )
    .await?;

    let mid = ask(&storage, TOPIC, at(4) + 500).await?;
    let error_code = ErrorCode::try_from(mid.error_code)?;
    assert_eq!(
        (ErrorCode::None, Some(5), Some(at(5))),
        (error_code, mid.offset, mid.timestamp)
    );

    for record in 0..RECORDS {
        let answered = ask(&storage, TOPIC, at(record)).await?;
        let expected = (Some(i64::from(record)), Some(at(record)));
        assert_eq!(
            expected,
            (answered.offset, answered.timestamp),
            "at record {record}"
        );
    }

    let before = ask(&storage, TOPIC, base_timestamp - 1).await?;
    assert_eq!(
        (Some(0), Some(base_timestamp)),
        (before.offset, before.timestamp)
    );

    let after = ask(&storage, TOPIC, at(RECORDS - 1) + 1).await?;
    assert_eq!((Some(-1), Some(-1)), (after.offset, after.timestamp));

    Ok(())
}
