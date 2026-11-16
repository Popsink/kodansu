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

//! Fetching one partition through `FetchService`: empty, and after six
//! records from six idempotent producers.

use rama::Context;
use rama::Service as _;
use rand::RngExt as _;
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::fetch_request::{FetchPartition, FetchTopic};
use tansu_sans_io::record::{Record, deflated, inflated};
use tansu_sans_io::{ErrorCode, FetchRequest, FetchResponse, IsolationLevel, ListOffset};
use tansu_storage::Topition;

pub mod common;

type Engine = std::sync::Arc<dyn tansu_storage::Storage>;

const PARTITIONS: i32 = 6;

/// The byte budget of a fetch here, overall and per partition: 50 KiB.
const BUDGET: i32 = 50 * 1024;

/// A registered `memory://` engine and one random partition of a fresh
/// six-partition topic on it.
async fn a_partition() -> tansu_broker::Result<(Engine, Topition)> {
    let cluster = uuid::Uuid::now_v7();
    let node = rand::rng().random_range(0..i32::MAX);
    let listener = url::Url::parse("tcp://127.0.0.1/")?;
    let engine = common::storage_container(cluster, node, listener).await?;
    common::register_broker(cluster, node, engine.clone()).await?;

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

    let index = rand::rng().random_range(0..PARTITIONS);
    Ok((engine, Topition::new(name, index)))
}

/// The latest offset of `topition`, read uncommitted.
async fn latest(engine: &Engine, topition: &Topition) -> tansu_broker::Result<Option<i64>> {
    let wanted = [(topition.clone(), ListOffset::Latest)];
    let listing = engine.list_offsets(IsolationLevel::ReadUncommitted, &wanted);
    let listed = listing.await.inspect_err(|err| tracing::error!(?err))?;
    tracing::debug!(list_offsets = ?listed);
    assert_eq!(1, listed.len());
    Ok(listed[0].1.offset)
}

/// `topition` fetched from offset 0, read uncommitted, waiting up to 500ms
/// for a byte.
async fn fetched(engine: Engine, topition: &Topition) -> tansu_broker::Result<FetchResponse> {
    let partition = FetchPartition::default().partition(topition.partition());
    let partition = partition.current_leader_epoch(Some(-1)).fetch_offset(0);
    let partition = partition.log_start_offset(Some(-1));
    let partition = partition.last_fetched_epoch(Some(-1));
    let partition = partition.partition_max_bytes(BUDGET);
    let partition = partition.replica_directory_id(None);
    let topic = FetchTopic::default().topic(Some(topition.topic().to_string()));
    let topic = topic.topic_id(Some(tansu_sans_io::NULL_TOPIC_ID));
    let topic = topic.partitions(Some(vec![partition]));

    let uncommitted = i8::from(&IsolationLevel::ReadUncommitted);
    let request = FetchRequest::default().max_wait_ms(500).min_bytes(1);
    let request = request
        .max_bytes(Some(BUDGET))
        .isolation_level(Some(uncommitted));
    let request = request.topics(Some(vec![topic]));
    let fetching = tansu_storage::FetchService.serve(Context::with_state(engine), request);
    let response = fetching.await?;

    let outcome = response.error_code.map(ErrorCode::try_from).transpose()?;
    assert_eq!(Some(ErrorCode::None), outcome);
    Ok(response)
}

/// Every batch of every partition in `response`.
fn batches(response: &FetchResponse) -> impl Iterator<Item = &deflated::Batch> {
    let topics = response.responses.iter().flatten();
    let partitions = topics.flat_map(|topic| topic.partitions.iter().flatten());
    partitions.flat_map(|partition| partition.records.iter().flat_map(|frame| &frame.batches))
}

/// Every record counted by [`batches`].
fn records(response: &FetchResponse) -> i64 {
    batches(response)
        .map(|batch| i64::from(batch.record_count))
        .sum()
}

#[tokio::test]
async fn an_empty_partition_fetches_no_records() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let (engine, topition) = a_partition().await?;
    let fetch = fetched(engine, &topition).await?;

    // `.iter().flatten()` over an `Option`, not `.unwrap()`: an empty partition
    // answers `records: None`, and this body is the one case that has one. It
    // unwrapped because it had never run — its `mod in_memory` wrapper called
    // `simple_non_txn`, the neighbouring body, so `empty_topic` compiled and
    // was never executed (#552).
    for batch in batches(&fetch) {
        assert_eq!((-1, -1), (batch.producer_id, batch.producer_epoch));
    }
    assert_eq!(0, records(&fetch));
    Ok(())
}

#[tokio::test]
async fn six_idempotent_records_fetch_back() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let (engine, topition) = a_partition().await?;
    assert_eq!(Some(0), latest(&engine, &topition).await?);

    let produced = 6;
    let mut offsets = std::collections::BTreeMap::new();
    let mut total = 0;
    for n in 0..produced {
        let initialising = engine.init_producer(None, 10_000, Some(-1), Some(-1));
        let producer = initialising.await?;
        let value = bytes::Bytes::copy_from_slice(common::alphanumeric_string(15).as_bytes());
        total += value.len();

        let batch = inflated::Batch::builder().record(Record::builder().value(Some(value.clone())));
        let batch = batch.producer_id(producer.id);
        let batch = batch.producer_epoch(producer.epoch).build()?;
        let deflated = deflated::Batch::try_from(batch)?;
        tracing::debug!(n, bytes = total, batch = ?deflated);

        let producing = engine.produce(None, &topition, deflated);
        let offset = producing.await?;
        tracing::debug!(offset);
        assert_eq!(None, offsets.insert(offset, (producer, value)));
    }

    assert_eq!(Some(produced), latest(&engine, &topition).await?);
    assert_eq!(produced, records(&fetched(engine, &topition).await?));
    Ok(())
}
