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
//! `producers/{id}.json` and validated with a linearizable CAS, instead of
//! CASing the single cluster-global `meta` object on every `acks=all` batch
//! (#13). These tests check that the sharding keeps producers independent and
//! that the exact Kafka error codes survive — including across two stateless
//! replicas sharing one bucket.

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_sans_io::{
    ErrorCode, create_topics_request::CreatableTopic, record::Record, record::deflated,
    record::inflated,
};

use crate::{
    Error, Result, Storage, Topition, dynostore::DynoStore, dynostore::tests::init_tracing,
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
async fn idempotent_state_is_shared_across_replicas() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // Two stores over ONE bucket == two stateless replicas.
    let bucket = InMemory::new();
    let replica_a = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let replica_b = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "cross-replica";
    create_topic(&replica_a, topic).await?;
    let topition = Topition::new(topic, 0);

    // Registered on A (seeds producers/{id}.json in the shared bucket).
    let producer = init_idempotent(&replica_a).await?;

    // First batch produced on B: B has no in-memory producer state, so it reads
    // the sharded object A wrote.
    assert_eq!(
        0,
        replica_b
            .produce(None, &topition, idempotent_batch(producer, 0, 0, 2)?)
            .await?
    );

    // Replaying seq 0 on A must be rejected as a duplicate — A sees B's advance
    // through the shared object, not a per-replica counter.
    assert_eq!(
        ErrorCode::DuplicateSequenceNumber,
        api_error(
            replica_a
                .produce(None, &topition, idempotent_batch(producer, 0, 0, 2)?)
                .await
        )
    );

    // The contiguous continuation (seq 2) from B succeeds at offset 2.
    assert_eq!(
        2,
        replica_b
            .produce(None, &topition, idempotent_batch(producer, 0, 2, 1)?)
            .await?
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
