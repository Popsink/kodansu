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

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_sans_io::{
    BatchAttribute, ErrorCode, add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic, record::Record, record::deflated, record::inflated,
};

use crate::{
    Error, Result, Storage, Topition, TxnAddPartitionsRequest,
    dynostore::{CoalesceTuning, CompactRun, DynoStore, Substream, tests::init_tracing},
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

/// A store flushing on every enqueue, so an awaited produce returns without
/// parking on the coalesce linger — these tests are written as straight-line
/// sequential produces and each one's outcome is the assertion.
fn segment_store() -> DynoStore {
    DynoStore::new(CLUSTER, NODE, InMemory::new()).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        ..Default::default()
    })
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

    let store = segment_store();

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
    // progress on the same partition. The log-based authority (#88) acks it
    // with the offset the original landed at rather than raising
    // `DuplicateSequenceNumber`; that is the Kafka-conformant answer, and it is
    // what makes a retry after an ack the client never saw idempotent instead
    // of fatal.
    assert_eq!(
        2,
        store
            .produce(None, &topition, idempotent_batch(p2, 0, 0, 1)?)
            .await?,
        "a duplicate is acked with its original offset",
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

/// A producer with no coordinate in the log is admitted as new, at sequence 0
/// (#88) — it is not rejected with `UnknownProducerId`.
///
/// The per-pod `producers/{id}.json` registry that used to reject it is gone:
/// it diverged across a connection migration (#79) and mishandled i32 sequence
/// wraparound (#80), so #88 replaced it with coordinates folded from the log
/// itself. There is no registry left to miss, and a producer absent from the
/// log is indistinguishable from a genuinely new one.
///
/// This is a **deliberate divergence from Kafka**, which answers
/// `UNKNOWN_PRODUCER_ID` so the client knows to reset. It is pre-existing
/// rather than introduced here — every leaseless deployment has behaved this
/// way since #86 — and #177 only makes it the single behaviour. Tracked in
/// #198; pinned here so it is a decision on record rather than a silent gap.
#[tokio::test]
async fn an_unknown_producer_is_admitted_as_new() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = segment_store();

    let topic = "unregistered";
    create_topic(&store, topic).await?;
    let topition = Topition::new(topic, 0);

    // Never registered via InitProducerId.
    assert_eq!(
        0,
        store
            .produce(None, &topition, idempotent_batch(42, 0, 0, 1)?)
            .await?,
    );

    // And it is tracked from there like any other producer: the retry dedupes.
    assert_eq!(
        0,
        store
            .produce(None, &topition, idempotent_batch(42, 0, 0, 1)?)
            .await?,
    );

    Ok(())
}

/// Epoch fencing over the log-folded authority (#88): a **lower** epoch than
/// the one already in the log is fenced, and a **higher** one resets the
/// stream.
///
/// The registry this replaced fenced any epoch that differed from the
/// registered one, including a higher one. That was backwards: in Kafka a
/// higher epoch is a new incarnation of the producer and is what *does* the
/// fencing, while a lower epoch is the zombie to reject. `classify` folds the
/// epoch out of the log and gets this the Kafka way round.
#[tokio::test]
async fn a_lower_epoch_is_fenced_and_a_higher_one_resets() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = segment_store();

    let topic = "fenced";
    create_topic(&store, topic).await?;
    let topition = Topition::new(topic, 0);

    let producer = init_idempotent(&store).await?;

    // Epoch 1 writes first, so that is what the log carries.
    assert_eq!(
        0,
        store
            .produce(None, &topition, idempotent_batch(producer, 1, 0, 1)?)
            .await?
    );

    // A zombie still on epoch 0 is fenced.
    assert_eq!(
        ErrorCode::ProducerFenced,
        api_error(
            store
                .produce(None, &topition, idempotent_batch(producer, 0, 5, 1)?)
                .await
        )
    );

    // Epoch 2 is a new incarnation: the stream resets, so sequence 0 is in
    // order again even though epoch 1 had already reached sequence 1.
    assert_eq!(
        1,
        store
            .produce(None, &topition, idempotent_batch(producer, 2, 0, 1)?)
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

/// Compaction of a run of v2 segments must carry the idempotent producer
/// coordinates forward (#107): re-encoding the merged run as v2 re-derives them
/// from the (byte-identical) merged batches, so log-based dedup (#88) still
/// observes producers whose batches were compacted. Before the fix the merged
/// segment was written v1, dropping the coordinates, and a retry of a compacted
/// batch was silently re-appended as a duplicate record.
#[tokio::test]
async fn compaction_carries_producer_coordinates_forward() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const PREFIX: &str = "org.env.conn";
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new()).coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(2),
        // keep_hot = 0 so the whole run — including the retried batch — is
        // compactable; this is what exposes the dropped-coordinates bug.
        prefix_compact_keep_hot: Some(0),
        prefix_compact_target_bytes: Some(1 << 30),
        ..Default::default()
    });

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);
    let pid = init_idempotent(&store).await?;

    // Four idempotent batches, each flushed as its own segment (offsets 0..4).
    for seq in 0..4 {
        assert_eq!(
            seq as i64,
            store
                .produce(None, &tp, idempotent_batch(pid, 0, seq, 1)?)
                .await?
        );
    }

    // Compact the whole run into one segment.
    let merged = store.compact_prefix_segments(PREFIX).await?;
    assert!(
        matches!(merged, CompactRun::Merged(n) if n >= 2),
        "the run should have been compacted (got {merged:?})"
    );

    // The merged segment's footer still carries every batch's producer
    // coordinates (base sequences 0..4 for this producer) — not dropped.
    store.refresh_prefix_index(PREFIX).await?;
    let carried: Vec<i32> = store
        .valid_substream_segments(PREFIX, &Substream::Name(topic.into()), 0)?
        .into_iter()
        .flat_map(|fenced| fenced.entry.producers)
        .filter(|coord| coord.producer_id == pid)
        .map(|coord| coord.base_sequence)
        .collect();
    assert_eq!(
        vec![0, 1, 2, 3],
        carried,
        "compaction must carry the v2 producer coordinates forward (#107)"
    );

    // Behavioural acceptance: a retry of a compacted batch is recognized as a
    // duplicate and acked with its original offset (0), not re-appended.
    let retry = store
        .produce(None, &tp, idempotent_batch(pid, 0, 0, 1)?)
        .await?;
    assert_eq!(
        0, retry,
        "retry of a compacted batch must dedup to its offset"
    );

    Ok(())
}
