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

//! Offset assignment is done by *creating* immutable, create-only batch objects
//! whose names encode the base offset, rather than by updating a hot
//! per-partition `watermark` object on every batch (#13). These tests exercise
//! the correctness properties that move guards:
//!
//! - concurrent writers (tasks on one store, or two stores sharing one bucket —
//!   i.e. two stateless replicas) assign **contiguous, non-overlapping** offsets
//!   with no gaps and no losses;
//! - a `Create` conflict resyncs to the real next offset;
//! - the high watermark is derived from the immutable objects, so a cold replica
//!   reads it correctly without ever having written the partition.

use std::time::Duration;

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_sans_io::{
    IsolationLevel, ListOffset, create_topics_request::CreatableTopic, record::Record,
    record::deflated, record::inflated,
};

use crate::{
    Error, Result, Storage, Topition, dynostore::DynoStore, dynostore::tests::init_tracing,
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// Build a non-idempotent deflated batch carrying `records` records (so it
/// occupies `records` consecutive offsets, `last_offset_delta == records - 1`).
fn batch(records: usize) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder();

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("record-{i}").as_bytes(),
        ))));
    }

    // `build()` does not derive `last_offset_delta` from the record count — a
    // real Kafka producer sets it on the wire — so set it explicitly here. It
    // is what makes a batch occupy `records` consecutive offsets.
    builder
        .last_offset_delta(records as i32 - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(storage: &DynoStore, name: &str, partitions: i32) -> Result<()> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

/// Assert the (offset, record_count) pairs tile `[0, total)` exactly: sorted by
/// base offset they must be contiguous with no gaps and no overlaps, and every
/// base offset must be distinct.
fn assert_contiguous(mut assigned: Vec<(i64, usize)>, expected_total: i64) {
    assigned.sort_by_key(|(offset, _)| *offset);

    let mut next = 0i64;
    for (offset, count) in &assigned {
        assert_eq!(
            next, *offset,
            "non-contiguous offset assignment: expected {next}, got {offset} in {assigned:?}"
        );
        next += *count as i64;
    }

    assert_eq!(
        expected_total, next,
        "total records covered {next} != expected {expected_total} in {assigned:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_writer_offsets_are_contiguous() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // Two stores over ONE shared bucket == two stateless replicas (node 111).
    let bucket = InMemory::new();
    let replica_a = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let replica_b = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "concurrent";
    create_topic(&replica_a, topic, 1).await?;

    let topition = Topition::new(topic, 0);

    const PRODUCES: usize = 64;

    let mut handles = Vec::new();
    let mut expected_total = 0i64;

    for i in 0..PRODUCES {
        // Mix single- and multi-record batches so overlaps (which a base-only
        // view would miss) are exercised.
        let records = 1 + (i % 3);
        expected_total += records as i64;

        // Spread produces across both replicas so writers genuinely race the
        // same partition tail on different stores.
        let storage = if i % 2 == 0 {
            replica_a.clone()
        } else {
            replica_b.clone()
        };

        let topition = topition.clone();
        let deflated = batch(records)?;

        handles.push(tokio::spawn(async move {
            storage
                .produce(None, &topition, deflated)
                .await
                .map(|offset| (offset, records))
        }));
    }

    let mut assigned = Vec::with_capacity(PRODUCES);
    for handle in handles {
        assigned.push(handle.await.expect("task panicked")?);
    }

    // Every produce returned a distinct base offset.
    let distinct = assigned
        .iter()
        .map(|(offset, _)| *offset)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        PRODUCES,
        distinct.len(),
        "duplicate offsets in {assigned:?}"
    );

    // Offsets tile [0, expected_total) with no gaps/overlaps.
    assert_contiguous(assigned, expected_total);

    // A fresh, cold replica reads the high watermark purely from the immutable
    // batch objects (it never wrote this partition).
    let replica_c = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let stage = replica_c.offset_stage(&topition).await?;
    assert_eq!(expected_total, stage.high_watermark);

    Ok(())
}

#[tokio::test]
async fn conflict_resyncs_to_next_offset() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let replica_a = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let replica_b = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "resync";
    create_topic(&replica_a, topic, 1).await?;
    let topition = Topition::new(topic, 0);

    // B writes offset 0; its in-memory hint advances to 1.
    assert_eq!(0, replica_b.produce(None, &topition, batch(1)?).await?);

    // A (cold) derives the tail from listing -> writes offset 1; real tail is 2.
    assert_eq!(1, replica_a.produce(None, &topition, batch(1)?).await?);

    // B still believes the next offset is 1 (stale hint). Its Create at 1 must
    // conflict (A owns it), resync from the listing, and land on 2.
    assert_eq!(2, replica_b.produce(None, &topition, batch(1)?).await?);

    Ok(())
}

#[tokio::test]
async fn high_watermark_from_listing_on_cold_replica() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let writer = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "cold-read";
    create_topic(&writer, topic, 1).await?;
    let topition = Topition::new(topic, 0);

    // A multi-record batch (offsets 0..=2) then a single-record batch (3).
    assert_eq!(0, writer.produce(None, &topition, batch(3)?).await?);
    assert_eq!(3, writer.produce(None, &topition, batch(1)?).await?);

    // A reader that never touched this partition still reports the correct high
    // watermark (== log end offset == 4), proving it comes from the immutable
    // objects and not a per-replica cache.
    let reader = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let stage = reader.offset_stage(&topition).await?;
    assert_eq!(4, stage.high_watermark);

    let latest = reader
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await?;
    assert_eq!(1, latest.len());
    assert_eq!(Some(4), latest[0].1.offset);

    Ok(())
}

/// Fetch every batch of `topition` from `offset`, returning `(base_offset,
/// record_count)` for each — proving the create-only assignment yields records
/// that are actually fetchable at the offsets it handed back.
async fn fetch_offsets(storage: &DynoStore, topition: &Topition, offset: i64) -> Vec<(i64, usize)> {
    storage
        .fetch(
            topition,
            offset,
            0,
            10 * 1024 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_secs(5),
        )
        .await
        .expect("fetch")
        .iter()
        .map(|deflated| {
            let records = inflated::Batch::try_from(deflated)
                .expect("inflate")
                .records
                .len();
            (deflated.base_offset, records)
        })
        .collect()
}

#[tokio::test]
async fn produced_records_are_fetchable_at_assigned_offsets() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    let topic = "roundtrip";
    create_topic(&store, topic, 1).await?;
    let topition = Topition::new(topic, 0);

    // Mixed batch widths: offsets 0..=1, 2, 3..=5.
    assert_eq!(0, store.produce(None, &topition, batch(2)?).await?);
    assert_eq!(2, store.produce(None, &topition, batch(1)?).await?);
    assert_eq!(3, store.produce(None, &topition, batch(3)?).await?);

    // A full fetch returns the batches at exactly the assigned base offsets with
    // their original record counts — no gaps, the filename offset is authority.
    assert_eq!(
        vec![(0, 2), (2, 1), (3, 3)],
        fetch_offsets(&store, &topition, 0).await
    );

    // A mid-log fetch seeks to the requested offset.
    assert_eq!(
        vec![(2, 1), (3, 3)],
        fetch_offsets(&store, &topition, 2).await
    );

    // Fetching at the high watermark returns nothing.
    assert!(fetch_offsets(&store, &topition, 6).await.is_empty());

    Ok(())
}

#[tokio::test]
async fn partitions_track_offsets_independently() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    let topic = "multi-partition";
    create_topic(&store, topic, 3).await?;

    let p0 = Topition::new(topic, 0);
    let p1 = Topition::new(topic, 1);

    // Interleave produces across two partitions; each has its own offset space.
    assert_eq!(0, store.produce(None, &p0, batch(2)?).await?);
    assert_eq!(0, store.produce(None, &p1, batch(1)?).await?);
    assert_eq!(2, store.produce(None, &p0, batch(1)?).await?);
    assert_eq!(1, store.produce(None, &p1, batch(3)?).await?);

    assert_eq!(3, store.offset_stage(&p0).await?.high_watermark);
    assert_eq!(4, store.offset_stage(&p1).await?.high_watermark);

    // An untouched partition is empty.
    let p2 = Topition::new(topic, 2);
    assert_eq!(0, store.offset_stage(&p2).await?.high_watermark);

    Ok(())
}

#[tokio::test]
async fn delete_records_all_uses_listing_high_watermark() -> Result<(), Error> {
    let _guard = init_tracing()?;

    use tansu_sans_io::delete_records_request::DeleteRecordsPartition;
    use tansu_sans_io::delete_records_request::DeleteRecordsTopic;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    let topic = "delete-all";
    create_topic(&store, topic, 1).await?;
    let topition = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &topition, batch(2)?).await?);
    assert_eq!(2, store.produce(None, &topition, batch(3)?).await?);

    // `offset: -1` means "the log end offset" — it must come from the immutable
    // objects (high watermark 5), not the no-longer-advanced `watermark.high`.
    let results = store
        .delete_records(&[DeleteRecordsTopic::default()
            .name(topic.into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(-1),
            ]))])
        .await?;

    assert_eq!(1, results.len());
    let partitions = results[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(5, partitions[0].low_watermark);

    // Everything is gone; the next produce continues at the log end offset (5),
    // not back at 0.
    assert!(fetch_offsets(&store, &topition, 0).await.is_empty());
    assert_eq!(5, store.produce(None, &topition, batch(1)?).await?);

    Ok(())
}
