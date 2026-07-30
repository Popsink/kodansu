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
use object_store::{ObjectStoreExt as _, memory::InMemory, path::Path};
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

/// Seed a legacy `records/{offset:020}.batch` object straight into the bucket.
///
/// Nothing writes this layout any more (#177/#178), but production buckets still
/// hold it and retention still drains it — which is what this file's #241 test is
/// about.
async fn seed_legacy_batch(
    store: &DynoStore,
    bucket: &InMemory,
    topition: &Topition,
    base_offset: i64,
    batch: deflated::Batch,
) -> Result<()> {
    let location = Path::from(format!(
        "clusters/{CLUSTER}/topics/{}/partitions/{:0>10}/records/{:0>20}.batch",
        topition.topic(),
        topition.partition(),
        base_offset,
    ));

    _ = bucket.put(&location, store.encode_frame(&[batch])?).await?;

    Ok(())
}

/// #241: draining a legacy partition to empty must leave the log END behind, not
/// just the log start.
///
/// `expire_partition` wrote only `watermark.low`, so once retention removed every
/// object nothing recorded where the log ended. `high_watermark` folds
/// `max(segment tail, legacy tail, persisted high)`, which is then `0` — the
/// `committed 171,958 / HWM 0` pairs #241 reports. The same fold is what
/// `leaseless_base` assigns from, so the next produce re-used offsets from `0`,
/// invisibly to every group whose committed position was above the new tail.
///
/// Asserted on a **cold** store, because that is the state that matters: the pod
/// that ran the drain still holds an in-process hint, so it cannot see the loss.
#[tokio::test]
async fn draining_a_legacy_partition_keeps_its_offset_floor() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.drained_legacy";
    create_topic(&store, topic).await?;
    let topition = Topition::new(topic, 0);

    // Two legacy batches of three records: the log ends at 6.
    seed_legacy_batch(&store, &bucket, &topition, 0, batch(3)?).await?;
    seed_legacy_batch(&store, &bucket, &topition, 3, batch(3)?).await?;
    assert_eq!(6, store.high_watermark(&topition).await?);

    // A threshold in the future expires everything, regardless of age.
    assert_eq!(
        2,
        store
            .expire_partition(&topition, now_ms() + HOUR_MS)
            .await?
    );

    let cold = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert_eq!(
        6,
        cold.high_watermark(&topition).await?,
        "a drained partition must not report its log end as 0"
    );

    // The consequence that matters: the next produce continues past the drained
    // region instead of re-using offsets from 0.
    assert_eq!(
        6,
        cold.produce(None, &topition, batch(1)?).await?,
        "offsets must never be re-used after a drain"
    );

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
