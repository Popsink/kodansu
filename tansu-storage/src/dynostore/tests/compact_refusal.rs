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

use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime},
};

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
    put_segment_in(store, bucket, PREFIX, seq, base, batch).await
}

/// [`put_segment`] under `prefix`: a compacted topic's is its own name.
async fn put_segment_in(
    store: &DynoStore,
    bucket: &InMemory,
    prefix: &str,
    seq: u64,
    base: i64,
    batch: deflated::Batch,
) -> Result<()> {
    let (payload, _footer) =
        store.encode_segment_v3(&[(Topition::new(TOPIC, 0), base, vec![batch])], 1, seq)?;
    let path = Path::from(format!(
        "clusters/{CLUSTER}/prefixes/{prefix}/segments/{seq:0>20}.seg"
    ));
    _ = bucket.put(&path, payload).await?;

    Ok(())
}

/// A batch with one record per key, at consecutive offset deltas.
fn keyed(keys: &[&str]) -> Result<deflated::Batch> {
    keys.iter()
        .enumerate()
        .fold(inflated::Batch::builder(), |builder, (n, key)| {
            builder.record(
                Record::builder()
                    .offset_delta(n as i32)
                    .key(Some(Bytes::copy_from_slice(key.as_bytes())))
                    .value(Some(Bytes::copy_from_slice(
                        format!("value-{n}").as_bytes(),
                    ))),
            )
        })
        .last_offset_delta(keys.len() as i32 - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// The `(seq, from, to)` spans the fenced view of `TOPIC` serves in `prefix`.
fn served(store: &DynoStore, prefix: &str) -> Result<Vec<(u64, i64, i64)>> {
    Ok(store
        .valid_substream_segments(prefix, &Substream::Name(TOPIC.into()), 0)?
        .into_iter()
        .map(|fenced| (fenced.seq, fenced.served_from, fenced.end()))
        .collect())
}

/// Just the `[from, to)` of each span [`served`] returns.
fn offsets(spans: &[(u64, i64, i64)]) -> Vec<(i64, i64)> {
    spans.iter().map(|(_, from, to)| (*from, *to)).collect()
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

/// Selection does not build a run across a segment outside it (#594): the
/// fleet's `outside_run` shape, where segment 3 holds `[50, 100)` at a higher
/// sequence than the segments either side of it, and stays hot.
///
/// Until #594 selection built `[0, 1, 2]`, fetched it, refused it at the
/// tiling check and learned a seam — once for every merged segment. Now it
/// never builds it: `[0]` is left alone, and `[1, 2]`, which tile, merge.
#[tokio::test]
async fn selection_stops_a_run_before_a_segment_outside_it() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, &[]).await?;

    put_gap(&store, &bucket).await?;
    put_segment(&store, &bucket, 2, 150, batch(50)?).await?;
    put_segment(&store, &bucket, 3, 50, batch(50)?).await?;

    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Merged(2),
    ));
    assert!(store.compact_seams_of(PREFIX)?.is_empty());

    let served = served(&store, PREFIX)?;
    assert_eq!(vec![(0, 50), (50, 100), (100, 200)], offsets(&served));
    assert_eq!((0, 3), (served[0].0, served[1].0));

    Ok(())
}

/// A hole nothing serves is still met, refused and learned (#461): selection
/// only steers around a gap other segments fill, so `missing` keeps its
/// `ERROR` and its seam.
#[tokio::test]
async fn selection_still_meets_a_hole_nothing_serves() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, &[]).await?;

    put_gap(&store, &bucket).await?;
    put_segment(&store, &bucket, 2, 150, batch(50)?).await?;

    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Retry,
    ));
    assert_eq!(BTreeSet::from([1]), store.compact_seams_of(PREFIX)?);

    Ok(())
}

/// Key compaction does not open a gap in the tiling (#594), so a gap on a
/// compacted topic is not one it made, and fusing across it would shift
/// offsets exactly as on any other topic.
///
/// The per-key pass keeps every batch's offset span — a stripped batch keeps
/// its `last_offset_delta` (#175) — so after it removes a record the regions
/// still tile `[0, 8)`. What it does do is rewrite segment 1 at a fresh high
/// sequence, the shape of a merged segment, and selection steers around that
/// rather than refusing the run.
#[tokio::test]
async fn a_per_key_rewrite_keeps_the_tiling_whole() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, &[("cleanup.policy", "compact")]).await?;

    put_segment_in(&store, &bucket, TOPIC, 0, 0, keyed(&["x", "y"])?).await?;
    put_segment_in(&store, &bucket, TOPIC, 1, 2, keyed(&["a", "a"])?).await?;
    put_segment_in(&store, &bucket, TOPIC, 2, 4, keyed(&["c", "d"])?).await?;
    put_segment_in(&store, &bucket, TOPIC, 3, 6, keyed(&["e", "f"])?).await?;
    store.refresh_prefix_index_forced(TOPIC).await?;

    assert_eq!(1, store.compact_prefix_per_key(TOPIC).await?);

    let rewritten = served(&store, TOPIC)?;
    assert_eq!(vec![(0, 2), (2, 4), (4, 6), (6, 8)], offsets(&rewritten));
    assert!(rewritten[1].0 > 3);

    assert!(matches!(
        store.compact_prefix_segments(TOPIC).await?,
        CompactRun::Merged(2),
    ));
    assert!(store.compact_seams_of(TOPIC)?.is_empty());
    assert_eq!(
        vec![(0, 2), (2, 4), (4, 8)],
        offsets(&served(&store, TOPIC)?)
    );

    Ok(())
}
