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

//! A compaction refusal says what kind of break refused it (#594).
//!
//! Every break in a run's tiling refuses the run, and every one used to be the
//! same `ERROR` line — ~58k a week on the EU fleet, while the read-path checks
//! on both fleets found every conclusive sample readable. These pin the class
//! [`DynoStore::classify_tiling_break`] gives each shape against a real store,
//! so the one class that means a lost record, `missing`, is the only `ERROR`.

use std::time::{Duration, SystemTime};

use object_store::{ObjectStoreExt as _, memory::InMemory, path::Path};
use tansu_sans_io::{
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
};

use crate::{
    Result, Storage as _, Topition,
    dynostore::{
        CoalesceTuning, CompactRun, DynoStore, Substream,
        compaction::TilingBreak,
        tests::{batch_stamped, init_tracing},
    },
};

use bytes::Bytes;

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "org.env.conn";
const TOPIC: &str = "org.env.conn.tab";

/// The run every test classifies: the two segments either side of the gap.
const RUN: [u64; 2] = [0, 1];

/// The gap between them: `[50, 100)` is in neither.
const GAP: (i64, i64) = (50, 100);

/// A non-idempotent batch occupying `records` offsets.
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

/// Every eligible segment but the newest, which stays hot: so a segment
/// written last sits outside any run the older ones form.
fn new_store(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(1),
        prefix_compact_keep_hot: Some(1),
        prefix_compact_target_bytes: Some(1 << 20),
        ..Default::default()
    })
}

async fn create_topic(store: &DynoStore, configs: &[(&str, &str)]) -> Result<()> {
    _ = store
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some(
                    configs
                        .iter()
                        .map(|(name, value)| {
                            CreatableTopicConfig::default()
                                .name((*name).into())
                                .value(Some((*value).into()))
                        })
                        .collect(),
                )),
            false,
        )
        .await?;

    Ok(())
}

/// Write segment `seq` out of band: one region for `TOPIC` partition 0 at
/// `base` — the only way to lay out a gap, since no healthy write path does.
async fn put_segment(
    store: &DynoStore,
    bucket: &InMemory,
    seq: u64,
    base: i64,
    batch: deflated::Batch,
) -> Result<()> {
    let (payload, _footer) =
        store.encode_segment_v3(&[(Topition::new(TOPIC, 0), base, vec![batch])], 1, seq)?;
    let path = Path::from(format!(
        "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{seq:0>20}.seg"
    ));
    _ = bucket.put(&path, payload).await?;

    Ok(())
}

/// `[0, 50)` in segment 0 and `[100, 150)` in segment 1: [`GAP`] between them.
async fn put_gap(store: &DynoStore, bucket: &InMemory) -> Result<()> {
    put_segment(store, bucket, 0, 0, batch(50)?).await?;
    put_segment(store, bucket, 1, 100, batch(50)?).await
}

async fn classify(store: &DynoStore) -> Result<TilingBreak> {
    store.refresh_prefix_index_forced(PREFIX).await?;

    Ok(store
        .classify_tiling_break(
            PREFIX,
            &Substream::Name(TOPIC.into()),
            &Topition::new(TOPIC, 0),
            &RUN,
            GAP,
        )
        .await)
}

/// The fleet's common case: a merged segment at a fresh *high* sequence holds
/// the low offsets between two older run members. Nothing is lost — the run is
/// still refused, but at `debug`.
///
/// This is the test that fails if every break were classed `missing`.
#[tokio::test]
async fn a_gap_served_outside_the_run_is_outside_run() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, &[]).await?;

    put_gap(&store, &bucket).await?;
    put_segment(&store, &bucket, 2, 50, batch(50)?).await?;

    assert_eq!(TilingBreak::OutsideRun, classify(&store).await?);
    assert_eq!(tracing::Level::DEBUG, TilingBreak::OutsideRun.level());

    // Selection really does build that run and refuse it.
    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Retry,
    ));

    Ok(())
}

/// Offsets a `DeleteRecords` floor already hides are not a loss.
#[tokio::test]
async fn a_gap_under_the_truncation_floor_is_below_log_start() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, &[]).await?;

    put_gap(&store, &bucket).await?;
    store.refresh_prefix_index_forced(PREFIX).await?;
    _ = store
        .delete_records_before(&Topition::new(TOPIC, 0), 120)
        .await?;

    assert_eq!(TilingBreak::BelowLogStart, classify(&store).await?);
    assert_eq!(tracing::Level::DEBUG, TilingBreak::BelowLogStart.level());

    Ok(())
}

/// The EU shape from #594: the segment below the gap is past retention, so the
/// log will start at the segment above it once retention runs.
#[tokio::test]
async fn a_gap_above_an_aged_head_is_below_log_start() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, &[("retention.ms", "3600000")]).await?;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|since| since.as_millis() as i64)
        .unwrap_or_default();
    let aged = now - Duration::from_secs(2 * 3_600).as_millis() as i64;

    put_segment(&store, &bucket, 0, 0, batch_stamped(&[aged; 50])?).await?;
    put_segment(&store, &bucket, 1, 100, batch_stamped(&[now; 50])?).await?;

    assert_eq!(TilingBreak::BelowLogStart, classify(&store).await?);

    Ok(())
}

/// A key-compacted topic's gaps are expected, not lost.
#[tokio::test]
async fn a_gap_on_a_compacted_topic_is_compacted_topic() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, &[("cleanup.policy", "compact")]).await?;

    put_gap(&store, &bucket).await?;

    assert_eq!(TilingBreak::CompactedTopic, classify(&store).await?);
    assert_eq!(tracing::Level::DEBUG, TilingBreak::CompactedTopic.level());

    Ok(())
}

/// #461: offsets above the log start that no live segment holds, on a
/// delete-policy topic. The one class that stays an `ERROR`.
#[tokio::test]
async fn a_gap_nothing_serves_is_missing() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, &[]).await?;

    put_gap(&store, &bucket).await?;

    assert_eq!(TilingBreak::Missing, classify(&store).await?);
    assert_eq!(tracing::Level::ERROR, TilingBreak::Missing.level());

    Ok(())
}
