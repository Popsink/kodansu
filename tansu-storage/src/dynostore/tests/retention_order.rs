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

//! Retention must only ever trim a sub-stream's **head** (#61, the 08-31 loss).
//!
//! `expire_prefix_segments` used to key the decision on record timestamps
//! alone: any segment whose newest record across every slice was past the
//! threshold was deleted, wherever its offsets sat. Kafka's contract is
//! narrower — `deleteOldSegments` walks a log in offset order and stops at the
//! first segment it must keep — and the difference is exactly a mid-log hole:
//! on a sub-stream whose offset order and timestamp order disagree (a CDC
//! backfill stamping source timestamps; every prefix #461's zombie era
//! disordered), the middle expires while both neighbours survive, and a fetch
//! skips the range with no error on either side.
//!
//! That is what destroyed the offsets probed on 2026-08-31: records sourced in
//! the 08-24 incident window crossed the 7-day threshold minute-by-minute a
//! week later, and every one of them sat mid-log. #290 had already met this
//! shape at the tail and answered with the served-end certification — loud,
//! but still destroyed. These tests pin the refusal instead: the records
//! survive until everything below them expires too.

use std::{collections::BTreeSet, time::Duration};

use futures::TryStreamExt as _;
use object_store::{ObjectStore as _, ObjectStoreExt as _, memory::InMemory, path::Path};
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Result, Storage as _, Topition,
    dynostore::{CoalesceTuning, CompactRun, DynoStore, tests::init_tracing},
};

use bytes::Bytes;

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "org.env.conn";
const TOPIC: &str = "org.env.conn.tab";
const SHELTERED: &str = "org.env.conn.tab2";

/// Milliseconds since the epoch, as the expiry threshold is expressed.
fn now_ms() -> i64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

/// A non-idempotent `records`-record batch whose records carry `timestamp` —
/// the source timestamp retention is keyed on, distinct from append order.
fn batch_at(records: usize, timestamp: i64) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder()
        .base_timestamp(timestamp)
        .max_timestamp(timestamp);

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

async fn create_topic(store: &DynoStore, name: &str) -> Result<()> {
    _ = store
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

fn segment_path(seq: u64) -> Path {
    Path::from(format!(
        "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{seq:0>20}.seg"
    ))
}

async fn segments(bucket: &InMemory) -> Vec<Path> {
    let mut paths = bucket
        .list(Some(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/"
        ))))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .expect("list segments");
    paths.sort();
    paths
}

/// Write segment `seq` out of band, one region per `(topic, base, batches)` —
/// the only way to build a sub-stream whose offset order and timestamp order
/// disagree, since a healthy produce assigns offsets in append order.
async fn put_segment(
    store: &DynoStore,
    bucket: &InMemory,
    seq: u64,
    substreams: Vec<(&'static str, i64, Vec<deflated::Batch>)>,
) -> Result<()> {
    let substreams: Vec<(Topition, i64, Vec<deflated::Batch>)> = substreams
        .into_iter()
        .map(|(topic, base, batches)| (Topition::new(topic, 0), base, batches))
        .collect();
    let (payload, _footer) = store.encode_segment_v3(&substreams, 1, seq)?;
    _ = bucket.put(&segment_path(seq), payload).await?;

    Ok(())
}

/// Every offset served by any fetched batch of `tp`, walking forward until a
/// fetch returns nothing new.
async fn readable_offsets(store: &DynoStore, tp: &Topition) -> Result<BTreeSet<i64>> {
    let mut out = BTreeSet::new();
    let mut off = 0i64;
    for _ in 0..64 {
        let batches = store
            .fetch(
                tp,
                off,
                0,
                1_000_000,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(200),
            )
            .await?;
        let mut advanced = false;
        for batch in batches {
            let end = batch.base_offset + batch.last_offset_delta as i64 + 1;
            out.extend(batch.base_offset..end);
            if end > off {
                off = end;
                advanced = true;
            }
        }
        if !advanced {
            break;
        }
    }
    Ok(out)
}

/// The minimal disorder: recent records at low offsets, expired records in the
/// middle, recent records above. The middle segment's records are past the
/// threshold, and it must survive anyway — nothing below it is being deleted,
/// so deleting it alone punches a hole a fetch skips with no error, which is
/// the 08-31 probe result (`fetch@12896322 -> 13058963..`, 162 641 offsets
/// gone) in one segment.
#[tokio::test]
async fn retention_only_ever_trims_a_substream_head() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    let recent = now_ms();
    let ancient = 1_000;

    put_segment(
        &store,
        &bucket,
        0,
        vec![(TOPIC, 0, vec![batch_at(100, recent)?])],
    )
    .await?;
    put_segment(
        &store,
        &bucket,
        1,
        vec![(TOPIC, 100, vec![batch_at(100, ancient)?])],
    )
    .await?;
    put_segment(
        &store,
        &bucket,
        2,
        vec![(TOPIC, 200, vec![batch_at(100, recent)?])],
    )
    .await?;

    store.refresh_prefix_index_forced(PREFIX).await?;
    let before = readable_offsets(&store, &tp).await?;
    assert_eq!(300, before.len());

    let deleted = store.expire_prefix_segments(PREFIX, recent - 1_000).await?;

    let after = readable_offsets(&store, &tp).await?;
    let missing: Vec<i64> = before.difference(&after).copied().collect();
    assert!(
        missing.is_empty(),
        "retention destroyed {} mid-log offsets {:?}..{:?} (deleted {deleted} segments)",
        missing.len(),
        missing.first(),
        missing.last(),
    );

    Ok(())
}

/// The head still trims: with the expired records at the bottom, retention
/// deletes exactly the maximal expired prefix and stops at the first segment
/// it must keep — offsets only ever vanish from the head, which is Kafka's
/// `deleteOldSegments` contract.
#[tokio::test]
async fn retention_still_trims_the_expired_head() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    let recent = now_ms();
    let ancient = 1_000;

    put_segment(
        &store,
        &bucket,
        0,
        vec![(TOPIC, 0, vec![batch_at(100, ancient)?])],
    )
    .await?;
    put_segment(
        &store,
        &bucket,
        1,
        vec![(TOPIC, 100, vec![batch_at(100, ancient)?])],
    )
    .await?;
    put_segment(
        &store,
        &bucket,
        2,
        vec![(TOPIC, 200, vec![batch_at(100, recent)?])],
    )
    .await?;

    store.refresh_prefix_index_forced(PREFIX).await?;
    assert_eq!(
        2,
        store.expire_prefix_segments(PREFIX, recent - 1_000).await?
    );
    assert_eq!(1, segments(&bucket).await.len());
    assert_eq!(
        (200..300).collect::<BTreeSet<i64>>(),
        readable_offsets(&store, &tp).await?,
    );

    Ok(())
}

/// The 08-31 chain in one prefix: compaction removes a retention shelter, and
/// the next expiry destroys the merged segment mid-log.
///
/// Pre-merge, the expired records share their segments with a co-tenant's
/// fresh slices, so per-segment retention holds everything — the state the
/// damaged fleet prefixes sat in while #462's refusal wedged compaction. The
/// merge is offset-correct: the co-tenant's slices are superseded by a segment
/// outside the run, so `compact_prefix_segments` rightly carries only the
/// expired sub-stream forward, and the merged segment's `max_timestamp` —
/// folded from the batches actually carried — is old. It was the shelter, not
/// the merge, that #469's unwedging removed at 1.4 M segments/hour; one tick
/// later, timestamp-only retention deleted the merged segment out of the
/// middle of the log.
#[tokio::test]
async fn a_merge_must_not_feed_mid_log_records_to_retention() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(1),
        prefix_compact_keep_hot: Some(2),
        prefix_compact_target_bytes: Some(2_000),
        ..Default::default()
    });
    create_topic(&store, TOPIC).await?;
    create_topic(&store, SHELTERED).await?;
    let tp = Topition::new(TOPIC, 0);
    let shel = Topition::new(SHELTERED, 0);

    let recent = now_ms();
    let ancient = 1_000;

    // Seq 0: above the merge target — bounds every run, survives as itself.
    put_segment(
        &store,
        &bucket,
        0,
        vec![(TOPIC, 0, vec![batch_at(200, recent)?])],
    )
    .await?;
    // Seqs 1-2: the run. Expired TOPIC records, sheltered by fresh SHELTERED
    // slices that seq 4 (hot tail) supersedes entirely.
    put_segment(
        &store,
        &bucket,
        1,
        vec![
            (TOPIC, 200, vec![batch_at(10, ancient)?]),
            (SHELTERED, 0, vec![batch_at(10, recent)?]),
        ],
    )
    .await?;
    put_segment(
        &store,
        &bucket,
        2,
        vec![
            (TOPIC, 210, vec![batch_at(10, ancient)?]),
            (SHELTERED, 10, vec![batch_at(10, recent)?]),
        ],
    )
    .await?;
    // Seqs 3-4: the hot tail `keep_hot = 2` protects from the merge.
    put_segment(
        &store,
        &bucket,
        3,
        vec![(TOPIC, 220, vec![batch_at(10, recent)?])],
    )
    .await?;
    put_segment(
        &store,
        &bucket,
        4,
        vec![(SHELTERED, 0, vec![batch_at(30, recent)?])],
    )
    .await?;

    store.refresh_prefix_index_forced(PREFIX).await?;
    let threshold = recent - 1_000;

    // The shelter holds while the slices share segments: nothing expires.
    assert_eq!(0, store.expire_prefix_segments(PREFIX, threshold).await?);

    let before_topic = readable_offsets(&store, &tp).await?;
    let before_shel = readable_offsets(&store, &shel).await?;
    assert_eq!(230, before_topic.len());
    assert_eq!(30, before_shel.len());

    // The merge: correct in offsets, and it strips the shelter — the SHELTERED
    // slices are superseded by seq 4 outside the run, so the merged segment
    // holds only the expired TOPIC records.
    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Merged(2),
    ));

    // The next maintenance tick's expiry.
    _ = store.expire_prefix_segments(PREFIX, threshold).await?;

    let after_topic = readable_offsets(&store, &tp).await?;
    let after_shel = readable_offsets(&store, &shel).await?;
    let missing: Vec<i64> = before_topic.difference(&after_topic).copied().collect();
    assert!(
        missing.is_empty(),
        "the merge fed {} mid-log offsets {:?}..{:?} to retention",
        missing.len(),
        missing.first(),
        missing.last(),
    );
    assert_eq!(before_shel, after_shel);

    Ok(())
}
