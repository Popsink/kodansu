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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::common::{Error, cluster_id, init_tracing, storage_url, storage_url_with_query};
use bytes::Bytes;
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use tansu_sans_io::{
    CreateTopicsRequest, ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest, ProduceRequest,
    create_topics_request::CreatableTopic,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    list_offsets_response::ListOffsetsPartitionResponse,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated::Batch},
};
use tansu_storage::{CreateTopicsService, ListOffsetsService, ProduceService, StorageContainer};
use url::Url;

mod common;

#[tokio::test]
async fn req() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(storage_url()?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(ListOffsetsService);

    let topic = "abcba";

    let response = service
        .serve(
            Context::default(),
            ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .replica_id(NODE_ID)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic.into())
                        .partitions(Some(
                            [ListOffsetsPartition::default()
                                .current_leader_epoch(Some(-1))
                                .max_num_offsets(Some(3))
                                .partition_index(0)
                                .timestamp(ListOffset::Earliest.try_into()?)]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic, topics[0].name);

    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(0, partitions[0].partition_index);
    assert!(partitions[0].old_style_offsets.is_none());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(Some(-1), partitions[0].timestamp);
    assert_eq!(Some(0), partitions[0].offset);
    assert_eq!(Some(0), partitions[0].leader_epoch);

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

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;
    const TOPICS: usize = 64;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(storage_url()?)
        .build()
        .await?;

    let create_topic = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
    };

    let produce = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(ProduceService)
    };

    let list_offsets = MapStateLayer::new(|_| storage).into_layer(ListOffsetsService);

    // The number of records produced to topic `i`: distinct per topic (mod a
    // small prime) so a response attributed to the wrong partition cannot
    // accidentally carry the right high watermark.
    let records_in = |i: usize| (i % 5) as i64 + 1;
    let topic_name = |i: usize| format!("assignment-{i:04}");

    for i in 0..TOPICS {
        let name = topic_name(i);

        let response = create_topic
            .serve(
                Context::default(),
                CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(name.clone())
                            .num_partitions(1)
                            .replication_factor(1)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
            )
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        let mut builder = Batch::builder().last_offset_delta(records_in(i) as i32 - 1);
        for record in 0..records_in(i) {
            builder = builder.record(
                Record::builder()
                    .offset_delta(record as i32)
                    .value(Bytes::from(format!("{name}-{record}").into_bytes()).into()),
            );
        }
        let deflated = builder.build().and_then(deflated::Batch::try_from)?;

        let response = produce
            .serve(
                Context::default(),
                ProduceRequest::default().topic_data(Some(
                    [TopicProduceData::default()
                        .name(name.clone())
                        .partition_data(Some(
                            [PartitionProduceData::default().index(0).records(Some(
                                deflated::Frame {
                                    batches: vec![deflated],
                                },
                            ))]
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
        assert_eq!(0, partitions[0].base_offset);
    }

    for offset_request in [ListOffset::Earliest, ListOffset::Latest] {
        let response = list_offsets
            .serve(
                Context::default(),
                ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .replica_id(-1)
                    .topics(Some(
                        (0..TOPICS)
                            .map(|i| {
                                Ok(ListOffsetsTopic::default().name(topic_name(i)).partitions(
                                    Some(
                                        [ListOffsetsPartition::default()
                                            .current_leader_epoch(Some(-1))
                                            .partition_index(0)
                                            .timestamp(offset_request.try_into()?)]
                                        .into(),
                                    ),
                                ))
                            })
                            .collect::<Result<Vec<_>, Error>>()?,
                    )),
            )
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(TOPICS, topics.len(), "{offset_request:?}");

        for i in 0..TOPICS {
            let name = topic_name(i);
            let topic = topics
                .iter()
                .find(|topic| topic.name == name)
                .unwrap_or_else(|| panic!("{offset_request:?}: no response for {name}"));

            let partitions = topic.partitions.as_deref().unwrap_or_default();
            assert_eq!(1, partitions.len(), "{offset_request:?}: {name}");
            assert_eq!(0, partitions[0].partition_index, "{name}");
            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(partitions[0].error_code)?,
                "{offset_request:?}: {name}"
            );

            let expected = match offset_request {
                // The log start: nothing has been deleted, so 0 for every topic.
                ListOffset::Earliest => 0,
                // The high watermark: the record count produced to THIS topic.
                _ => records_in(i),
            };
            assert_eq!(
                Some(expected),
                partitions[0].offset,
                "{offset_request:?}: {name}"
            );
        }
    }

    Ok(())
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

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;
    const TOPICS: usize = 64;
    const PREFIXES: usize = 4;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(storage_url_with_query("prefix_coalesce=true")?)
        .build()
        .await?;

    let create_topic = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
    };

    let produce = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(ProduceService)
    };

    let list_offsets = MapStateLayer::new(|_| storage).into_layer(ListOffsetsService);

    let records_in = |i: usize| (i % 5) as i64 + 1;
    // Dotted names: `prefix_of` coalesces on the first three components, so
    // the 64 topics share PREFIXES connector prefixes.
    let topic_name = |i: usize| format!("org.env.conn{}.assignment-{i:04}", i % PREFIXES);

    for i in 0..TOPICS {
        let name = topic_name(i);

        let response = create_topic
            .serve(
                Context::default(),
                CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(name.clone())
                            .num_partitions(1)
                            .replication_factor(1)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    )),
            )
            .await?;

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        let mut builder = Batch::builder().last_offset_delta(records_in(i) as i32 - 1);
        for record in 0..records_in(i) {
            builder = builder.record(
                Record::builder()
                    .offset_delta(record as i32)
                    .value(Bytes::from(format!("{name}-{record}").into_bytes()).into()),
            );
        }
        let deflated = builder.build().and_then(deflated::Batch::try_from)?;

        let response = produce
            .serve(
                Context::default(),
                ProduceRequest::default().topic_data(Some(
                    [TopicProduceData::default()
                        .name(name.clone())
                        .partition_data(Some(
                            [PartitionProduceData::default().index(0).records(Some(
                                deflated::Frame {
                                    batches: vec![deflated],
                                },
                            ))]
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
        assert_eq!(0, partitions[0].base_offset);
    }

    for sweep in ["cold", "warm"] {
        for offset_request in [ListOffset::Earliest, ListOffset::Latest] {
            let response = list_offsets
                .serve(
                    Context::default(),
                    ListOffsetsRequest::default()
                        .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                        .replica_id(-1)
                        .topics(Some(
                            (0..TOPICS)
                                .map(|i| {
                                    Ok(ListOffsetsTopic::default().name(topic_name(i)).partitions(
                                        Some(
                                            [ListOffsetsPartition::default()
                                                .current_leader_epoch(Some(-1))
                                                .partition_index(0)
                                                .timestamp(offset_request.try_into()?)]
                                            .into(),
                                        ),
                                    ))
                                })
                                .collect::<Result<Vec<_>, Error>>()?,
                        )),
                )
                .await?;

            let topics = response.topics.as_deref().unwrap_or_default();
            assert_eq!(TOPICS, topics.len(), "{sweep}: {offset_request:?}");

            for i in 0..TOPICS {
                let name = topic_name(i);
                let topic = topics
                    .iter()
                    .find(|topic| topic.name == name)
                    .unwrap_or_else(|| {
                        panic!("{sweep}: {offset_request:?}: no response for {name}")
                    });

                let partitions = topic.partitions.as_deref().unwrap_or_default();
                assert_eq!(1, partitions.len(), "{sweep}: {offset_request:?}: {name}");
                assert_eq!(0, partitions[0].partition_index, "{name}");
                assert_eq!(
                    ErrorCode::None,
                    ErrorCode::try_from(partitions[0].error_code)?,
                    "{sweep}: {offset_request:?}: {name}"
                );

                let expected = match offset_request {
                    ListOffset::Earliest => 0,
                    _ => records_in(i),
                };
                assert_eq!(
                    Some(expected),
                    partitions[0].offset,
                    "{sweep}: {offset_request:?}: {name}"
                );
            }
        }
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

    const NODE_ID: i32 = 111;
    const TOPIC: &str = "offsets-for-times";
    const RECORDS: i32 = 5;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(NODE_ID)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .build()
        .await?;

    _ = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
    }
    .serve(
        Context::default(),
        CreateTopicsRequest::default()
            .topics(Some(
                [CreatableTopic::default()
                    .name(TOPIC.into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into()))]
                .into(),
            ))
            .validate_only(Some(false)),
    )
    .await?;

    let produce = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(ProduceService)
    };

    for i in 0..RECORDS {
        let batch = Batch::builder()
            .record(Record::builder().value(Some(Bytes::copy_from_slice(
                format!("record-{i}").as_bytes(),
            ))))
            .build()
            .and_then(deflated::Batch::try_from)?;

        _ = produce
            .serve(
                Context::default(),
                ProduceRequest::default()
                    .acks(-1)
                    .timeout_ms(30_000)
                    .topic_data(Some(
                        [TopicProduceData::default()
                            .name(TOPIC.into())
                            .partition_data(Some(
                                [PartitionProduceData::default().index(0).records(Some(
                                    deflated::Frame {
                                        batches: [batch].into(),
                                    },
                                ))]
                                .into(),
                            ))]
                        .into(),
                    )),
            )
            .await?;
    }

    let list_offsets = MapStateLayer::new(|_| storage).into_layer(ListOffsetsService);

    let ask = async |timestamp: i64| -> Result<ListOffsetsPartitionResponse, Error> {
        let response = list_offsets
            .serve(
                Context::default(),
                ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .replica_id(NODE_ID)
                    .topics(Some(
                        [ListOffsetsTopic::default()
                            .name(TOPIC.into())
                            .partitions(Some(
                                [ListOffsetsPartition::default()
                                    .current_leader_epoch(Some(-1))
                                    .partition_index(0)
                                    .timestamp(timestamp)]
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

        Ok(partitions[0].clone())
    };

    // An hour from now: no record is at or after it, and none ever will be in
    // this test.
    let future = (SystemTime::now() + Duration::from_secs(3_600))
        .duration_since(UNIX_EPOCH)
        .expect("an hour from now is after the epoch")
        .as_millis() as i64;

    let answered = ask(future).await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(answered.error_code)?,
        "no matching record is an answer, not an error",
    );
    assert_eq!(Some(-1), answered.offset);
    assert_eq!(Some(-1), answered.timestamp);

    // A timestamp every record is at or after still resolves to a real offset,
    // so the sentinel has not swallowed the working case.
    let matched = ask(0).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(matched.error_code)?);
    assert_eq!(Some(0), matched.offset);

    // And the two named positions are unaffected: they never answer `None`, so
    // nothing about them goes through the sentinel.
    let earliest = ask(ListOffset::Earliest.try_into()?).await?;
    assert_eq!(Some(0), earliest.offset);

    let latest = ask(ListOffset::Latest.try_into()?).await?;
    assert_eq!(Some(i64::from(RECORDS)), latest.offset);

    Ok(())
}
