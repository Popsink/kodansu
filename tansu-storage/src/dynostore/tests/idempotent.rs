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

//! Idempotent producer sequence state is held per producer in
//! `producers/{id}.json`, sharded off the cluster-global `meta` object so
//! `acks=all` producers no longer contend on one hot object (#13).
//!
//! The per-batch advance is applied in memory (the fast-path authority) and the
//! object is checkpointed lazily, at most once per debounce window, to halve the
//! steady-state S3 PUT cost of an idempotent workload (#48). The tradeoff is
//! that idempotency is now **per-replica between checkpoints**: within a replica
//! the exact `OutOfOrderSequenceNumber` / `DuplicateSequenceNumber` /
//! `ProducerFenced` semantics hold on every batch, but a producer that migrates
//! to another replica mid-window may not have its most recent advances deduped
//! there until the source replica's next checkpoint. Producers are expected to
//! stick to one replica; cross-replica dedup is eventually consistent, bounded
//! by the checkpoint window. These tests pin both halves of that contract.

use std::time::Duration;

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_sans_io::{
    BatchAttribute, ErrorCode, add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic, record::Record, record::deflated, record::inflated,
};

use crate::{
    Error, Result, Storage, Topition, TxnAddPartitionsRequest, dynostore::DynoStore,
    dynostore::tests::init_tracing,
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// Build an idempotent batch for `producer_id`/`epoch` starting at
/// `base_sequence` and carrying `records` records.
fn idempotent_batch(
    producer_id: i64,
    epoch: i16,
    base_sequence: i32,
    records: usize,
) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder()
        .producer_id(producer_id)
        .producer_epoch(epoch)
        .base_sequence(base_sequence)
        .last_offset_delta(records as i32 - 1);

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("record-{i}").as_bytes(),
        ))));
    }

    builder
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(storage: &DynoStore, name: &str) -> Result<()> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

async fn init_idempotent(storage: &DynoStore) -> Result<i64> {
    let response = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;
    assert_eq!(ErrorCode::None, response.error);
    Ok(response.id)
}

fn api_error(result: Result<i64>) -> ErrorCode {
    match result {
        Err(Error::Api(code)) => code,
        otherwise => panic!("expected Error::Api, got {otherwise:?}"),
    }
}

#[tokio::test]
async fn producers_validate_independently() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    let topic = "shared-partition";
    create_topic(&store, topic).await?;
    let topition = Topition::new(topic, 0);

    let p1 = init_idempotent(&store).await?;
    let p2 = init_idempotent(&store).await?;
    assert_ne!(p1, p2);

    // p1 writes two records (seq 0,1), p2 one record (seq 0); they interleave on
    // the same partition but each tracks its own sequence.
    assert_eq!(
        0,
        store
            .produce(None, &topition, idempotent_batch(p1, 0, 0, 2)?)
            .await?
    );
    assert_eq!(
        2,
        store
            .produce(None, &topition, idempotent_batch(p2, 0, 0, 1)?)
            .await?
    );
    assert_eq!(
        3,
        store
            .produce(None, &topition, idempotent_batch(p1, 0, 2, 1)?)
            .await?
    );

    // Re-sending p2's first batch is a duplicate for p2 — unaffected by p1's
    // progress on the same partition.
    assert_eq!(
        ErrorCode::DuplicateSequenceNumber,
        api_error(
            store
                .produce(None, &topition, idempotent_batch(p2, 0, 0, 1)?)
                .await
        )
    );

    // A gap for p1 is out-of-order; again independent of p2.
    assert_eq!(
        ErrorCode::OutOfOrderSequenceNumber,
        api_error(
            store
                .produce(None, &topition, idempotent_batch(p1, 0, 9, 1)?)
                .await
        )
    );

    Ok(())
}

#[tokio::test]
async fn idempotent_advance_is_local_until_checkpoint() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // Two stores over ONE bucket == two stateless replicas.
    let bucket = InMemory::new();
    let replica_a = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let replica_b = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "cross-replica-local";
    create_topic(&replica_a, topic).await?;
    let topition = Topition::new(topic, 0);

    // Registered on A (seeds producers/{id}.json in the shared bucket).
    let producer = init_idempotent(&replica_a).await?;

    // First batch on B advances B's in-memory sequence but is not yet
    // checkpointed to the shared object (below the debounce window, #48).
    assert_eq!(
        0,
        replica_b
            .produce(None, &topition, idempotent_batch(producer, 0, 0, 2)?)
            .await?
    );

    // A replays seq 0 before B's checkpoint. Because the advance is still local
    // to B, A does NOT see it and re-accepts the batch (at the next offset)
    // rather than rejecting it as a duplicate: idempotency is per-replica
    // between checkpoints, the accepted #48 tradeoff.
    assert_eq!(
        2,
        replica_a
            .produce(None, &topition, idempotent_batch(producer, 0, 0, 2)?)
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn idempotent_advance_visible_across_replicas_after_checkpoint() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // Stores over ONE bucket == stateless replicas.
    let bucket = InMemory::new();
    let replica_a = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "cross-replica-checkpoint";
    create_topic(&replica_a, topic).await?;
    let topition = Topition::new(topic, 0);

    let producer = init_idempotent(&replica_a).await?;

    // A produces seq 0 (in-memory only), then — after the debounce interval has
    // elapsed — a contiguous seq 2, which triggers A's lazy checkpoint and
    // persists the advance (through seq 3) to the shared object.
    assert_eq!(
        0,
        replica_a
            .produce(None, &topition, idempotent_batch(producer, 0, 0, 2)?)
            .await?
    );
    tokio::time::sleep(DynoStore::PRODUCER_CHECKPOINT_INTERVAL + Duration::from_millis(50)).await;
    assert_eq!(
        2,
        replica_a
            .produce(None, &topition, idempotent_batch(producer, 0, 2, 1)?)
            .await?
    );

    // A replica that is *cold* for this producer (the migration target) reads the
    // checkpointed object and correctly rejects a replay of the already-acked
    // sequence: cross-replica dedup is restored once the source replica
    // checkpoints. (A replica already holding the producer in memory validates
    // against its own authority and would not re-read — hence the eventual, not
    // immediate, cross-replica contract.)
    let replica_cold = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert_eq!(
        ErrorCode::DuplicateSequenceNumber,
        api_error(
            replica_cold
                .produce(None, &topition, idempotent_batch(producer, 0, 0, 2)?)
                .await
        )
    );

    Ok(())
}

#[tokio::test]
async fn unregistered_producer_is_rejected() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    let topic = "unregistered";
    create_topic(&store, topic).await?;
    let topition = Topition::new(topic, 0);

    // Never registered via InitProducerId.
    assert_eq!(
        ErrorCode::UnknownProducerId,
        api_error(
            store
                .produce(None, &topition, idempotent_batch(42, 0, 0, 1)?)
                .await
        )
    );

    Ok(())
}

#[tokio::test]
async fn stale_epoch_is_fenced() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    let topic = "fenced";
    create_topic(&store, topic).await?;
    let topition = Topition::new(topic, 0);

    // Registered at epoch 0.
    let producer = init_idempotent(&store).await?;

    // A batch claiming a different (here, higher) epoch than the one the
    // producer object knows about is fenced — the sharded object preserves the
    // exact ProducerFenced semantics of the old global-meta check.
    assert_eq!(
        ErrorCode::ProducerFenced,
        api_error(
            store
                .produce(None, &topition, idempotent_batch(producer, 1, 0, 1)?)
                .await
        )
    );

    // The correct epoch still works.
    assert_eq!(
        0,
        store
            .produce(None, &topition, idempotent_batch(producer, 0, 0, 1)?)
            .await?
    );

    Ok(())
}

/// A transactional batch for `producer_id`/`epoch` at `base_sequence`.
fn txn_batch(
    producer_id: i64,
    epoch: i16,
    base_sequence: i32,
    records: usize,
) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder()
        .attributes(BatchAttribute::default().transaction(true).into())
        .producer_id(producer_id)
        .producer_epoch(epoch)
        .base_sequence(base_sequence)
        .last_offset_delta(records as i32 - 1);

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("record-{i}").as_bytes(),
        ))));
    }

    builder
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// After a transaction is aborted, its produced range surfaces in `offset_stage`
/// as an aborted transaction (#81) — what a read-committed fetch needs to filter
/// out the aborted records. An open (not-yet-ended) transaction does not.
#[tokio::test]
async fn aborted_transaction_surfaces_in_offset_stage() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());
    let topic = "txn-topic";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);
    let txn = "txn-1";

    let producer = store
        .init_producer(Some(txn), 60_000, Some(-1), Some(-1))
        .await?;

    _ = store
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: txn.into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: [AddPartitionsToTxnTopic::default()
                .name(topic.into())
                .partitions(Some([0].into()))]
            .into(),
        })
        .await?;

    assert_eq!(
        0,
        store
            .produce(
                Some(txn),
                &tp,
                txn_batch(producer.id, producer.epoch, 0, 3)?
            )
            .await?
    );

    // Open transaction: nothing aborted yet.
    assert!(store.offset_stage(&tp).await?.aborted().is_empty());

    // Abort it.
    assert_eq!(
        ErrorCode::None,
        store
            .txn_end(txn, producer.id, producer.epoch, false)
            .await?
    );

    // The aborted transaction now surfaces as (producer_id, first_offset).
    let staged = store.offset_stage(&tp).await?;
    assert_eq!(vec![(producer.id, 0)], staged.aborted().to_vec());

    Ok(())
}
