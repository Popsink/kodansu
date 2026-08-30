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

//! A `Produce`'s partitions are written at once, not one after another (#439).
//!
//! The partitions of a request are independent logs — nothing in the protocol
//! orders them against each other — but they were written sequentially, each
//! `produce` awaited to completion before the next began. A produce completes
//! only when its coalescing window has flushed, so an N-partition request cost
//! N × `coalesce_linger`.
//!
//! That is why the conformance bench (#439) found eight partitions *slower*
//! than one: "more batches = more requests" was the reading, and the truth was
//! that the request was paying eight flush windows end to end.
//!
//! The linger is the parameter here rather than injected latency, because the
//! linger is what the cost was: on `memory://` a segment PUT is microseconds, so
//! what a wide request spent was flush windows and nothing else.

use crate::common::{Error, cluster_id, init_tracing, storage_url_with_query};
use bytes::Bytes;
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tansu_sans_io::{
    CreateTopicsRequest, ErrorCode, ProduceRequest,
    create_topics_request::CreatableTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use tansu_storage::{CreateTopicsService, ProduceService, Storage, StorageContainer};
use url::Url;

mod common;

const HOST: &str = "localhost";
const PORT: i32 = 9092;
const NODE_ID: i32 = 111;

/// Long enough that N × linger is unmistakable against N × 0, and short enough
/// that a suite runs.
const LINGER: Duration = Duration::from_millis(250);

const PARTITIONS: i32 = 8;

async fn storage() -> Result<Arc<Box<dyn Storage>>, Error> {
    StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(storage_url_with_query(&format!(
            "coalesce_linger={}ms",
            LINGER.as_millis()
        ))?)
        .build()
        .await
        .map_err(Into::into)
}

async fn create(storage: Arc<Box<dyn Storage>>, topic: &str, partitions: i32) -> Result<(), Error> {
    let create = MapStateLayer::new(move |_| storage.clone()).into_layer(CreateTopicsService);

    _ = create
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .validate_only(Some(false))
                .timeout_ms(0)
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(topic.into())
                        .num_partitions(partitions)
                        .replication_factor(1)
                        .assignments(Some(vec![]))
                        .configs(Some(vec![])),
                ])),
        )
        .await?;

    Ok(())
}

fn entry(index: i32, value: &'static [u8]) -> Result<PartitionProduceData, Error> {
    inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(value).into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map(|deflated| {
            PartitionProduceData::default()
                .index(index)
                .records(Some(Frame {
                    batches: vec![deflated],
                }))
        })
        .map_err(Into::into)
}

/// A request naming eight partitions costs one flush window, not eight.
#[tokio::test]
async fn partitions_are_written_at_once() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let topic = "pqr";

    create(storage.clone(), topic, PARTITIONS).await?;

    let produce = MapStateLayer::new(move |_| storage.clone()).into_layer(ProduceService);

    let partition_data = (0..PARTITIONS)
        .map(|index| entry(index, b"lorem"))
        .collect::<Result<Vec<_>, Error>>()?;

    let started = Instant::now();

    let response = produce
        .serve(
            Context::default(),
            ProduceRequest::default()
                .transactional_id(None)
                .acks(1)
                .timeout_ms(0)
                .topic_data(Some(vec![
                    TopicProduceData::default()
                        .name(topic.into())
                        .partition_data(Some(partition_data)),
                ])),
        )
        .await?;

    let elapsed = started.elapsed();

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());

    let partitions = responses[0].partition_responses.clone().unwrap_or_default();
    assert_eq!(PARTITIONS as usize, partitions.len());

    for (position, partition) in partitions.iter().enumerate() {
        assert_eq!(position as i32, partition.index);
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(0, partition.base_offset);
    }

    // Two windows of headroom, not one: the linger is jittered ±20% (#91) and a
    // loaded CI box adds its own. Eight windows — what this cost before — is
    // nowhere near it.
    assert!(
        elapsed < LINGER * 2,
        "{PARTITIONS} partitions took {elapsed:?}; one linger is {LINGER:?}"
    );

    Ok(())
}

/// The answers come back in the positions the entries were asked in, across
/// topics as well as partitions — a client matches a partition response to its
/// request by position, so the fan-out completing in any order must not show.
#[tokio::test]
async fn every_entry_is_answered_in_its_own_position() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;

    create(storage.clone(), "abc", PARTITIONS).await?;
    create(storage.clone(), "def", PARTITIONS).await?;

    let produce = MapStateLayer::new(move |_| storage.clone()).into_layer(ProduceService);

    // Descending partition indexes, so a fan-out that answered in completion
    // order rather than request order would be visible as a reordering rather
    // than needing to be inferred.
    let descending = |topic: &str| -> Result<TopicProduceData, Error> {
        Ok(TopicProduceData::default()
            .name(topic.into())
            .partition_data(Some(
                (0..PARTITIONS)
                    .rev()
                    .map(|index| entry(index, b"lorem"))
                    .collect::<Result<Vec<_>, Error>>()?,
            )))
    };

    let response = produce
        .serve(
            Context::default(),
            ProduceRequest::default()
                .transactional_id(None)
                .acks(1)
                .timeout_ms(0)
                .topic_data(Some(vec![descending("abc")?, descending("def")?])),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(2, responses.len());

    for (name, topic) in ["abc", "def"].into_iter().zip(responses) {
        assert_eq!(name, topic.name);

        let partitions = topic.partition_responses.unwrap_or_default();
        assert_eq!(PARTITIONS as usize, partitions.len());

        for (position, partition) in partitions.iter().enumerate() {
            assert_eq!(PARTITIONS - 1 - position as i32, partition.index);
            assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        }
    }

    Ok(())
}

/// Two entries naming the *same* partition are two appends to one log, and they
/// keep the order the client wrote them in.
///
/// No Kafka client sends that shape, and the protocol does not say what it
/// means — but the fan-out is keyed by topition precisely so that a hand-rolled
/// one which does gets the ordering it had before rather than a race. The proof
/// is the offsets: the first entry is answered with 0 and the second with 1.
#[tokio::test]
async fn two_entries_for_one_partition_keep_their_order() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let topic = "pqr";

    create(storage.clone(), topic, 1).await?;

    let produce = MapStateLayer::new(move |_| storage.clone()).into_layer(ProduceService);

    let response = produce
        .serve(
            Context::default(),
            ProduceRequest::default()
                .transactional_id(None)
                .acks(1)
                .timeout_ms(0)
                .topic_data(Some(vec![
                    TopicProduceData::default()
                        .name(topic.into())
                        .partition_data(Some(vec![entry(0, b"first")?, entry(0, b"second")?])),
                ])),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    let partitions = responses[0].partition_responses.clone().unwrap_or_default();

    assert_eq!(2, partitions.len());
    assert_eq!(0, partitions[0].base_offset);
    assert_eq!(1, partitions[1].base_offset);

    Ok(())
}
