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

//! One undecodable segment must not cost its prefix compaction, forever (#398).
//!
//! `CorruptSegment` is deliberately fatal to a compaction run (#388) — it stops
//! one region's damage from being served as records. But run selection picks the
//! *oldest* mergeable segments and a damaged object is old, so `drain_compact_prefix`
//! re-read the same object on every tick that reached it and ended the prefix's
//! drain having merged nothing. Production ran that way for 17 hours at ~2
//! errors/hour on `1.0.0-alpha.3`, which is #274's "one error aborts the prefix's
//! pass" one error variant later.
//!
//! The damage predates #395's write-side invariant and nothing in the process
//! repairs it, so the fix is to route around the object: record it, exclude it
//! from run selection, keep draining what is readable.
//!
//! Excluding it punches a hole in the prefix's offset tiling, and that is the
//! second thing pinned here. The read path re-derives every record's offset by
//! running from the merged footer entry's `base_offset`, so a merge that spanned
//! the hole would slide every record above it down into the gap — silent offset
//! corruption where before there was only a stalled drain.

use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{
    ObjectStore as _, ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path,
};
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage as _, Topition,
    dynostore::{CoalesceTuning, DynoStore},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// The connector prefix `TOPIC` coalesces under (#236: first three components).
const PREFIX: &str = "org.env.conn";
const TOPIC: &str = "org.env.conn.table";

/// A compacted topic, prefix-routed under its own name (#175) — the shape of the
/// `*.connect.*-offsets` topics the per-key pass runs over, and the population
/// this damage is on.
const OFFSETS: &str = "org.env.conn.offsets";

/// A store whose compaction triggers on the smallest prefix a run can be picked
/// from, so a handful of produces is a whole backlog.
///
/// `keep_hot: 0` because the exempt newest segments would otherwise hold the
/// damaged one out of every run, which is the stall going away for the wrong
/// reason.
fn store(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(2),
        prefix_compact_keep_hot: Some(0),
        prefix_compact_target_bytes: Some(1 << 20),
        ..Default::default()
    })
}

fn batch(value: &str) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::copy_from_slice(value.as_bytes()))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

fn keyed_batch(key: &'static [u8], value: &'static [u8]) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(key)))
                .value(Some(Bytes::from_static(value))),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(store: &DynoStore, name: &str, configs: &[(&str, &str)]) -> Result<()> {
    let configs: Vec<CreatableTopicConfig> = configs
        .iter()
        .map(|(name, value)| {
            CreatableTopicConfig::default()
                .name((*name).to_owned())
                .value(Some((*value).to_owned()))
        })
        .collect();

    _ = store
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some(configs)),
            false,
        )
        .await?;

    Ok(())
}

fn segment_path(prefix: &str, seq: u64) -> Path {
    Path::from(format!(
        "clusters/{CLUSTER}/prefixes/{prefix}/segments/{seq:0>20}.seg"
    ))
}

async fn segments(bucket: &InMemory, prefix: &str) -> Vec<Path> {
    let mut paths = bucket
        .list(Some(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{prefix}/segments/"
        ))))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .expect("list segments");
    paths.sort();
    paths
}

/// Make segment `seq` of `prefix` undecodable, exactly as the fleet holds it:
/// the footer untouched, and the `batch_length` at the head of the first
/// region's frame declaring more bytes than the entry covers.
///
/// Production's signature is `read_len == byte_len` with `declared` over
/// `byte_len` — a whole object whose footer entry does not describe it (#395's
/// husk batch, written before the encoder refused it). The first region of a
/// single-sub-stream segment starts at byte 0, so the field is at bytes 8..12:
/// `base_offset (i64)` then `batch_length (i32)`.
async fn make_undecodable(bucket: &InMemory, prefix: &str, seq: u64) -> Result<Bytes> {
    let location = segment_path(prefix, seq);
    let mut bytes = bucket.get(&location).await?.bytes().await?.to_vec();

    let at = size_of::<i64>();
    bytes[at..at + size_of::<i32>()].copy_from_slice(&i32::MAX.to_be_bytes());

    let corrupted = Bytes::from(bytes);
    _ = bucket
        .put(&location, PutPayload::from(corrupted.clone()))
        .await?;

    Ok(corrupted)
}

async fn fetch_from(store: &DynoStore, tp: &Topition, offset: i64) -> Result<Vec<deflated::Batch>> {
    store
        .fetch(
            tp,
            offset,
            0,
            100_000,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(200),
        )
        .await
}

/// Six segments, the third of them undecodable. The drain used to merge nothing:
/// the run spans the damaged object, `CorruptSegment` propagates, and `Ok(0)`
/// and `Err(_)` shared a `break`. Now the object is excluded and the readable
/// segments on either side of it merge.
#[tokio::test]
async fn an_undecodable_segment_no_longer_ends_its_prefix_drain() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = store(&bucket);
    create_topic(&store, TOPIC, &[]).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..6 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }
    assert_eq!(6, segments(&bucket, PREFIX).await.len());

    let corrupted = make_undecodable(&bucket, PREFIX, 2).await?;

    // The prefix drains despite the damage, which is the whole claim.
    assert!(
        store.drain_compact_prefix(PREFIX).await > 0,
        "the drain merged nothing around the damaged segment"
    );

    // The damaged object is left exactly as it was — quarantine excludes it from
    // compaction, it does not rewrite or delete it.
    let live = segments(&bucket, PREFIX).await;
    let damaged = segment_path(PREFIX, 2);
    assert!(live.contains(&damaged), "{live:?}");
    assert_eq!(
        corrupted,
        bucket.get(&damaged).await?.bytes().await?,
        "the quarantined segment was rewritten"
    );

    // And it is what the process now knows to skip.
    assert_eq!(
        [2].into_iter().collect::<std::collections::BTreeSet<u64>>(),
        store.quarantined_segments_of(PREFIX)?
    );

    Ok(())
}

/// The offset hazard the exclusion creates, and the reason a quarantined segment
/// bounds a run rather than being filtered out of it.
///
/// Segment 2 holds offset 2. Merging the segments below it with the segments
/// above it would produce one region covering offsets `{0,1,3,4,5}` under a
/// `base_offset` of 0 — and the read path, which re-derives offsets by running
/// from that base, would serve the record written at offset 3 as offset 2.
#[tokio::test]
async fn a_merge_never_closes_the_hole_the_quarantine_opens() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = store(&bucket);
    create_topic(&store, TOPIC, &[]).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..6 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }
    _ = make_undecodable(&bucket, PREFIX, 2).await?;

    // Drain repeatedly: the first pass merges either side of the hole, and the
    // passes after it are where a run could pick up both merged segments at once
    // — the merged output takes a tail sequence, so sequence order stops being
    // offset order and only the coverage check stands between them.
    for _ in 0..4 {
        _ = store.drain_compact_prefix(PREFIX).await;
    }

    // Above the hole, every record still reads at the offset it was written at.
    let offsets: Vec<i64> = fetch_from(&store, &tp, 3)
        .await?
        .iter()
        .map(|batch| batch.base_offset)
        .collect();
    assert_eq!(vec![3, 4, 5], offsets);

    // Below it, likewise — and the read stops at the damage rather than
    // continuing over it.
    let offsets: Vec<i64> = fetch_from(&store, &tp, 0)
        .await
        .map(|batches| batches.iter().map(|batch| batch.base_offset).collect())
        .unwrap_or_default();
    assert!(
        offsets.iter().all(|offset| *offset < 2),
        "a read below the hole crossed it: {offsets:?}"
    );

    Ok(())
}

/// A prefix with no quarantined segment merges exactly as it did: the coverage
/// guard is not consulted, and the drain still collapses the whole backlog.
#[tokio::test]
async fn an_undamaged_prefix_still_drains_to_one_segment() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = store(&bucket);
    create_topic(&store, TOPIC, &[]).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..6 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }

    while store.drain_compact_prefix(PREFIX).await > 0 {}

    assert!(store.quarantined_segments_of(PREFIX)?.is_empty());
    assert_eq!(1, segments(&bucket, PREFIX).await.len());

    assert_eq!(
        vec![0, 1, 2, 3, 4, 5],
        fetch_from(&store, &tp, 0)
            .await?
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// The per-key pass reads *every* segment of a compacted topic's prefix, so one
/// damaged object cost the prefix its key cleanup on every tick — the worse half
/// of the same stall: a compacted topic with no cleanup grows stale versions
/// forever. It now quarantines and runs over the rest.
#[tokio::test]
async fn an_undecodable_segment_no_longer_costs_a_compacted_prefix_its_cleanup() -> Result<(), Error>
{
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = store(&bucket);
    create_topic(&store, OFFSETS, &[("cleanup.policy", "compact")]).await?;
    let tp = Topition::new(OFFSETS, 0);

    // One key, rewritten: every segment but the newest holds a superseded value
    // the pass has to remove.
    for _ in 0..4 {
        _ = store.produce(None, &tp, keyed_batch(b"k", b"v")?).await?;
    }

    let before = segments(&bucket, OFFSETS).await;
    assert_eq!(4, before.len());

    _ = make_undecodable(&bucket, OFFSETS, 1).await?;

    store.drain_compact_prefix_per_key(OFFSETS).await;

    assert_eq!(
        [1].into_iter().collect::<std::collections::BTreeSet<u64>>(),
        store.quarantined_segments_of(OFFSETS)?
    );

    // The readable segments were rewritten: a per-key rewrite creates a new
    // sequence and retires the old one, so the object set moved.
    let after = segments(&bucket, OFFSETS).await;
    assert_ne!(before, after, "no segment was cleaned");
    assert!(
        after.contains(&segment_path(OFFSETS, 1)),
        "the quarantined segment was rewritten: {after:?}"
    );

    Ok(())
}
