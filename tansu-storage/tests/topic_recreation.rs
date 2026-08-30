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

//! A topic deleted and recreated under the same name is a new log (#442).
//!
//! Reported as "watermarks (10, 13) where Kafka 4.1 answers (0, 3)". The cause
//! is the layout: a segment multiplexes many topics' records, is immutable, and
//! is reclaimed whole only once every sub-stream in it is past retention — so a
//! deleted topic's slices cannot be removed, and until #442 they were located by
//! `(topic, partition)` **name**, which a same-named successor also answers to.
//! Starting that successor at 0 would have laid its log directly on top of
//! records that are still there and still found by name, which is worse than the
//! offsets being wrong. What kept them apart was the truncation floor
//! `DeleteTopics` leaves behind (#246): the successor starts at the
//! predecessor's end.
//!
//! Footer v4 keys a sub-stream by the topic's **id** instead, pinned at creation
//! in `topic-routing/{name}.json`. A recreation is a different uuid, so it is a
//! different sub-stream: the predecessor's slices are unreachable by
//! construction rather than hidden by a floor, and the successor's log starts
//! where an empty log starts.
//!
//! The writer regime is a flag (`segment_format=4`), so both shapes are pinned
//! here — the v3 default must keep answering exactly what it answered before.
//!
//! The *migration* — one bucket, a writer regime moving under it — is pinned in
//! `dynostore::tests::substream_identity`, because it cannot be reached from
//! here: on `memory://` every `StorageContainer::build()` gets its own
//! `InMemory`, so two engines built here are two unrelated buckets.

use crate::common::{Error, cluster_id, init_tracing, storage_url_with_query};
use bytes::Bytes;
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use std::sync::Arc;
use tansu_sans_io::{
    CreateTopicsRequest, DeleteTopicsRequest, ErrorCode, IsolationLevel, ListOffset,
    ListOffsetsRequest, ProduceRequest,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use tansu_storage::{
    CreateTopicsService, DeleteTopicsService, FetchService, ListOffsetsService, ProduceService,
    Storage, StorageContainer,
};
use url::Url;

mod common;

const TOPIC: &str = "pqr";
const PARTITION: i32 = 0;

type Engine = Arc<Box<dyn Storage>>;

async fn storage(query: &str) -> Result<Engine, Error> {
    StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url_with_query(query)?)
        .build()
        .await
        .map_err(Into::into)
}

async fn create(storage: &Engine) -> Result<(), Error> {
    let storage = storage.clone();
    let create = MapStateLayer::new(move |_| storage.clone()).into_layer(CreateTopicsService);

    let response = create
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .validate_only(Some(false))
                .timeout_ms(0)
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(TOPIC.into())
                        .num_partitions(1)
                        .replication_factor(1)
                        .assignments(Some(vec![]))
                        .configs(Some(vec![])),
                ])),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(i16::from(ErrorCode::None), topics[0].error_code);

    Ok(())
}

async fn delete(storage: &Engine) -> Result<(), Error> {
    let storage = storage.clone();
    let delete = MapStateLayer::new(move |_| storage.clone()).into_layer(DeleteTopicsService);

    let response = delete
        .serve(
            Context::default(),
            DeleteTopicsRequest::default().topic_names(Some(vec![TOPIC.into()])),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());
    assert_eq!(i16::from(ErrorCode::None), responses[0].error_code);

    Ok(())
}

/// Produce `values` one batch at a time, so each lands at its own offset.
async fn produce(storage: &Engine, values: &[&'static str]) -> Result<(), Error> {
    let storage = storage.clone();
    let produce = MapStateLayer::new(move |_| storage.clone()).into_layer(ProduceService);

    for value in values {
        let deflated = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(value.as_bytes()).into()))
            .build()
            .and_then(deflated::Batch::try_from)?;

        let response = produce
            .serve(
                Context::default(),
                ProduceRequest::default()
                    .transactional_id(None)
                    .acks(1)
                    .timeout_ms(0)
                    .topic_data(Some(vec![
                        TopicProduceData::default()
                            .name(TOPIC.into())
                            .partition_data(Some(vec![
                                PartitionProduceData::default()
                                    .index(PARTITION)
                                    .records(Some(Frame {
                                        batches: vec![deflated],
                                    })),
                            ])),
                    ])),
            )
            .await?;

        let topics = response.responses.unwrap_or_default();
        let partitions = topics[0].partition_responses.clone().unwrap_or_default();
        assert_eq!(
            i16::from(ErrorCode::None),
            partitions[0].error_code,
            "producing {value}"
        );
    }

    Ok(())
}

/// `(earliest, latest)` — the pair `get_watermark_offsets` reports.
async fn watermarks(storage: &Engine) -> Result<(i64, i64), Error> {
    let storage = storage.clone();
    let list = MapStateLayer::new(move |_| storage.clone()).into_layer(ListOffsetsService);

    let mut bounds = Vec::with_capacity(2);

    for offset in [ListOffset::Earliest, ListOffset::Latest] {
        let response = list
            .serve(
                Context::default(),
                ListOffsetsRequest::default()
                    .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                    .topics(Some(vec![
                        ListOffsetsTopic::default()
                            .name(TOPIC.into())
                            .partitions(Some(vec![
                                ListOffsetsPartition::default()
                                    .partition_index(PARTITION)
                                    .timestamp(offset.try_into()?),
                            ])),
                    ])),
            )
            .await?;

        let topics = response.topics.unwrap_or_default();
        let partitions = topics[0].partitions.clone().unwrap_or_default();
        assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);

        bounds.push(partitions[0].offset.unwrap_or(-1));
    }

    Ok((bounds[0], bounds[1]))
}

/// Every record value the log serves from `offset`.
async fn fetch(storage: &Engine, offset: i64) -> Result<Vec<String>, Error> {
    let storage = storage.clone();
    let fetch = MapStateLayer::new(move |_| storage.clone()).into_layer(FetchService);

    let response = fetch
        .serve(
            Context::default(),
            tansu_sans_io::FetchRequest::default()
                .max_wait_ms(100)
                .min_bytes(1)
                .max_bytes(Some(1024 * 1024))
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(TOPIC.into()))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(PARTITION)
                                .fetch_offset(offset)
                                .partition_max_bytes(1024 * 1024),
                        ])),
                ])),
        )
        .await?;

    let mut values = vec![];

    for topic in response.responses.unwrap_or_default() {
        for partition in topic.partitions.unwrap_or_default() {
            for batch in partition
                .records
                .into_iter()
                .flat_map(|frame| frame.batches)
            {
                for record in inflated::Batch::try_from(batch)?.records {
                    values.push(
                        String::from_utf8_lossy(&record.value.unwrap_or_default()).into_owned(),
                    );
                }
            }
        }
    }

    Ok(values)
}

/// The reported shape, and what makes it a bug: the successor continues its
/// predecessor's offsets.
///
/// Pinned rather than merely acknowledged, because the fix is behind a flag: a
/// deployment that has not raised `segment_format` must behave exactly as it did,
/// including here. The floor doing the work is #246's truncation tombstone —
/// which is also why the successor's *records* are still its own, and why the
/// separate "silent old/new data mixing" the report predicted does not happen.
#[tokio::test]
async fn a_name_keyed_recreation_continues_the_deleted_log() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage("").await?;

    create(&storage).await?;
    produce(&storage, &["old-0", "old-1", "old-2"]).await?;
    assert_eq!((0, 3), watermarks(&storage).await?);

    delete(&storage).await?;
    create(&storage).await?;
    produce(&storage, &["new-0"]).await?;

    assert_eq!(
        (3, 4),
        watermarks(&storage).await?,
        "a name-keyed successor starts at the predecessor's end"
    );

    // It serves its own records and only its own — the predecessor's are below
    // the truncation floor.
    assert_eq!(vec!["new-0".to_owned()], fetch(&storage, 3).await?);

    Ok(())
}

/// The fix: with the v4 writer regime the successor is a new log, from 0.
#[tokio::test]
async fn an_id_keyed_recreation_starts_at_zero() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage("segment_format=4").await?;

    create(&storage).await?;
    produce(&storage, &["old-0", "old-1", "old-2"]).await?;
    assert_eq!((0, 3), watermarks(&storage).await?);
    assert_eq!(
        vec!["old-0".to_owned(), "old-1".to_owned(), "old-2".to_owned()],
        fetch(&storage, 0).await?
    );

    delete(&storage).await?;
    create(&storage).await?;
    produce(&storage, &["new-0"]).await?;

    assert_eq!(
        (0, 1),
        watermarks(&storage).await?,
        "a recreated topic is a brand-new log"
    );

    // And the predecessor's records are unreachable rather than merely hidden:
    // offset 0 belongs to the successor now, and there is no offset in the
    // successor's log at which the old records can appear.
    assert_eq!(vec!["new-0".to_owned()], fetch(&storage, 0).await?);

    Ok(())
}
