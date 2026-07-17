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

//! `delete`-policy retention (#49): the maintenance loop must skip the full
//! `records/` LIST of a partition whose oldest data is still within retention,
//! using the in-memory oldest-retained hint, while never skipping a partition
//! that has expirable data. These tests pin the guard and the hint refresh.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_sans_io::{
    create_topics_request::CreatableTopic, record::Record, record::deflated, record::inflated,
};

use crate::{
    Error, Result, Storage, Topition, dynostore::DynoStore, dynostore::tests::init_tracing,
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const HOUR_MS: i64 = 3_600_000;

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

fn batch(records: usize) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder();

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("record-{i}").as_bytes(),
        ))));
    }

    builder
        .last_offset_delta(records as i32 - 1)
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

#[tokio::test]
async fn absent_hint_forces_scan() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());
    let topition = Topition::new("never-scanned", 0);

    // With no hint yet, every threshold must be treated as possibly expirable so
    // the partition is scanned rather than silently skipped.
    assert!(store.partition_maybe_expirable(&topition, now_ms())?);

    Ok(())
}

#[tokio::test]
async fn scan_within_retention_populates_hint_and_skips_next_tick() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());
    let topic = "within-retention";
    create_topic(&store, topic).await?;
    let topition = Topition::new(topic, 0);

    // A single freshly written batch (last_modified ~= now).
    assert_eq!(0, store.produce(None, &topition, batch(3)?).await?);

    // A threshold an hour in the past: nothing is expirable.
    let threshold = now_ms() - HOUR_MS;
    assert_eq!(0, store.expire_partition(&topition, threshold).await?);

    // The scan recorded the oldest-retained hint, so the next tick with the same
    // (or older) threshold skips the LIST entirely.
    assert!(!store.partition_maybe_expirable(&topition, threshold)?);

    Ok(())
}

#[tokio::test]
async fn expiry_removes_old_batches_and_drops_hint() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());
    let topic = "past-retention";
    create_topic(&store, topic).await?;
    let topition = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &topition, batch(3)?).await?);

    // A threshold an hour in the future: the lone batch is past retention.
    let threshold = now_ms() + HOUR_MS;
    assert_eq!(1, store.expire_partition(&topition, threshold).await?);

    // No survivor remains, so the hint is dropped and the partition is scanned
    // again next tick (it may since have been produced to).
    assert!(store.partition_maybe_expirable(&topition, threshold)?);

    Ok(())
}

#[tokio::test]
async fn lowered_retention_rebascules_to_scan() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());
    let topic = "lowered-retention";
    create_topic(&store, topic).await?;
    let topition = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &topition, batch(3)?).await?);

    // First scan within a generous retention: nothing expirable, hint set to the
    // batch's ~now last_modified, next tick would skip.
    let generous = now_ms() - HOUR_MS;
    assert_eq!(0, store.expire_partition(&topition, generous).await?);
    assert!(!store.partition_maybe_expirable(&topition, generous)?);

    // Retention is then lowered (threshold rises past the hint): the guard must
    // flip back to scanning so the now-expirable data is not skipped.
    let tightened = now_ms() + HOUR_MS;
    assert!(store.partition_maybe_expirable(&topition, tightened)?);

    Ok(())
}
