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

//! Producing straight to the storage engine and fetching back: six records
//! from six idempotent producers, and one or three committed transactions of
//! six records each, with their end markers.

use std::collections::BTreeMap;
use std::time::Duration;

use rand::RngExt as _;
use tansu_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use tansu_sans_io::add_partitions_to_txn_response::AddPartitionsToTxnPartitionResult;
use tansu_sans_io::add_partitions_to_txn_response::AddPartitionsToTxnTopicResult;
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::record::{Record, deflated, inflated};
use tansu_sans_io::{BatchAttribute, ControlBatch, EndTransactionMarker, ErrorCode};
use tansu_storage::{ProducerIdResponse, Topition, TxnAddPartitionsRequest};

pub mod common;

type Engine = std::sync::Arc<dyn tansu_storage::Storage>;
type Octets = bytes::Bytes;

const PARTITIONS: i32 = 6;

/// Records per producer, or per transaction.
const RECORDS: i32 = 6;

/// What a produced record is expected to read back as, by offset.
type Expected = BTreeMap<i64, (ProducerIdResponse, Option<Octets>, Option<Octets>)>;

/// A registered `memory://` engine and a random partition of a fresh
/// six-partition topic on it.
async fn a_partition() -> tansu_broker::Result<(Engine, Topition)> {
    let cluster = uuid::Uuid::now_v7();
    let node = rand::rng().random_range(0..i32::MAX);
    let listener = url::Url::parse("tcp://127.0.0.1/")?;
    let engine = common::storage_container(cluster, node, listener).await?;
    common::register_broker(cluster, node, engine.clone()).await?;

    let name = common::alphanumeric_string(15);
    tracing::debug!(topic_name = ?name);
    let topic = CreatableTopic::default().name(name.clone());
    let topic = topic.num_partitions(PARTITIONS).replication_factor(0);
    let topic = topic.assignments(Some(vec![])).configs(Some(vec![]));
    let creating = engine.create_topic(topic, false);
    let created = creating.await;
    let id = created.inspect_err(|err| tracing::error!(?err, name))?;
    tracing::debug!(topic_id = ?id);
    Ok((
        engine,
        Topition::new(name, rand::rng().random_range(0..PARTITIONS)),
    ))
}

fn random_octets() -> Octets {
    Octets::copy_from_slice(common::alphanumeric_string(15).as_bytes())
}

/// Every batch of `topition` from `offset`, read uncommitted, inflated.
async fn fetched(
    engine: &Engine,
    topition: &Topition,
    offset: i64,
) -> tansu_broker::Result<Vec<inflated::Batch>> {
    let uncommitted = tansu_sans_io::IsolationLevel::ReadUncommitted;
    let wait = Duration::from_millis(500);
    let fetching = engine.fetch(topition, offset, 1, 50 * 1024, uncommitted, wait);
    let compressed = fetching.await?;
    let inflating = compressed.into_iter().map(inflated::Batch::try_from);
    let batches = inflating.collect::<Result<Vec<_>, _>>()?;
    tracing::debug!(?batches);
    Ok(batches)
}

fn record_count(batches: &[inflated::Batch]) -> usize {
    batches.iter().map(|batch| batch.records.len()).sum()
}

/// Every record in `batches` with its absolute offset.
fn placed(batches: &[inflated::Batch]) -> Vec<(i64, &Record)> {
    let mut placed = Vec::new();
    for batch in batches.iter() {
        let base = batch.base_offset;
        let absolute = |r: &'_ Record| base + i64::from(r.offset_delta);
        placed.extend(batch.records.iter().map(|r| (absolute(r), r)));
    }
    placed
}

/// Asserts that every record in `batches` reads back the key and value
/// `expected` holds at its absolute offset.
fn assert_as_produced(batches: &[inflated::Batch], expected: &Expected) {
    for (at, record) in placed(batches) {
        let wanted = expected
            .get(&at)
            .map(|(_, key, value)| (key.clone(), value.clone()));
        assert_eq!(wanted, Some((record.key(), record.value())), "at {at}");
    }
}

/// Commits `transactions` transactions of six keyed records each to
/// `topition`, one transactional producer apiece under one transactional
/// id, and returns what each offset should read back as, end markers
/// included.
async fn committed_transactions(
    engine: &Engine,
    topition: &Topition,
    transactions: i32,
) -> tansu_broker::Result<Expected> {
    let txn_id = common::alphanumeric_string(10);
    let (name, index) = (topition.topic().to_string(), topition.partition());
    let mut expected = Expected::new();
    let mut logged = 0;

    for round in 0..transactions {
        let initialising = engine.init_producer(Some(&txn_id), 10_000, Some(-1), Some(-1));
        let producer = initialising.await?;
        tracing::debug!(transaction = round, txn_producer = ?producer);

        let topic = AddPartitionsToTxnTopic::default().name(name.clone());
        let adding = engine.txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: txn_id.clone(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![topic.partitions(Some(vec![index]))],
        });
        let added = adding.await;
        let added = added.inspect_err(|err| tracing::error!(?err, name, index))?;
        let result = AddPartitionsToTxnPartitionResult::default().partition_index(index);
        let result = result.partition_error_code(ErrorCode::None.into());
        let topic = AddPartitionsToTxnTopicResult::default().name(name.clone());
        let topic = topic.results_by_partition(Some(vec![result]));
        assert_eq!([topic], added.zero_to_three());

        let transactional = BatchAttribute::default().transaction(true).into();
        for base_sequence in 0..RECORDS {
            let (key, value) = (random_octets(), random_octets());
            logged += key.len() + value.len();
            let record = Record::builder().key(Some(key.clone()));
            let record = record.value(Some(value.clone()));
            let built = inflated::Batch::builder().record(record);
            let built = built.attributes(transactional);
            let built = built.producer_id(producer.id);
            let built = built.producer_epoch(producer.epoch);
            let built = built.base_sequence(base_sequence).build()?;
            let batch = deflated::Batch::try_from(built)?;
            tracing::debug!(base_sequence, bytes = logged, ?batch);

            let producing = engine.produce(Some(&txn_id), topition, batch);
            let produced = producing.await;
            let offset = produced.inspect_err(|err| tracing::error!(?err, base_sequence))?;
            tracing::debug!(offset);
            let previous = expected.insert(offset, (producer, Some(key), Some(value)));
            assert_eq!(None, previous);
        }

        let ending = engine.txn_end(&txn_id, producer.id, producer.epoch, true);
        assert_eq!(ErrorCode::None, ending.await?);

        let marker_at = expected.last_key_value().map(|(offset, _)| offset + 1);
        let commit: Octets = ControlBatch::default().commit().try_into()?;
        let marker: Octets = EndTransactionMarker::default().try_into()?;
        let marked = (producer, Some(commit), Some(marker));
        let marker_at = marker_at.expect("a record before the marker");
        assert_eq!(None, expected.insert(marker_at, marked));
    }
    Ok(expected)
}

#[tokio::test]
async fn six_idempotent_records_fetch_back_unkeyed() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let (engine, topition) = a_partition().await?;

    let mut expected = Expected::new();
    let mut logged = 0;
    for n in 0..RECORDS {
        let initialising = engine.init_producer(None, 10_000, Some(-1), Some(-1));
        let producer = initialising.await?;
        let value = random_octets();
        logged += value.len();

        let record = Record::builder().value(Some(value.clone()));
        let built = inflated::Batch::builder().record(record);
        let built = built.producer_id(producer.id);
        let built = built.producer_epoch(producer.epoch).build()?;
        let batch = deflated::Batch::try_from(built)?;
        tracing::debug!(n, bytes = logged, ?batch);

        let producing = engine.produce(None, &topition, batch);
        let offset = producing.await?;
        tracing::debug!(?offset);
        assert_eq!(None, expected.insert(offset, (producer, None, Some(value))));
    }

    let first = *expected.first_key_value().expect("six records").0;
    let batches = fetched(&engine, &topition, first).await?;
    assert_eq!(Some(first), batches.first().map(|batch| batch.base_offset));
    assert_eq!(RECORDS as usize, record_count(&batches));
    assert_as_produced(&batches[..1], &expected);
    Ok(())
}

#[tokio::test]
async fn a_committed_transaction_fetches_back_with_its_marker() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let (engine, topition) = a_partition().await?;
    let expected = committed_transactions(&engine, &topition, 1).await?;

    let first = *expected.first_key_value().expect("one transaction").0;
    let batches = fetched(&engine, &topition, first).await?;
    assert_eq!((RECORDS + 1) as usize, record_count(&batches));

    for batch in &batches {
        for record in &batch.records {
            let at = batch.base_offset + i64::from(record.offset_delta);
            let producer = expected.get(&at).map(|(producer, _, _)| producer);
            let producer = producer.expect("every fetched record was produced");
            assert_eq!(
                (producer.id, producer.epoch),
                (batch.producer_id, batch.producer_epoch)
            );
        }
    }
    assert_as_produced(&batches, &expected);
    Ok(())
}

#[tokio::test]
async fn three_committed_transactions_fetch_back_with_their_markers() -> tansu_broker::Result<()> {
    let _guard = common::init_tracing()?;
    let (engine, topition) = a_partition().await?;
    let expected = committed_transactions(&engine, &topition, 3).await?;

    let first = *expected.first_key_value().expect("three transactions").0;
    let batches = fetched(&engine, &topition, first).await?;
    assert_eq!(Some(first), batches.first().map(|batch| batch.base_offset));
    assert_eq!(((RECORDS + 1) * 3) as usize, record_count(&batches));
    assert_as_produced(&batches, &expected);
    Ok(())
}
