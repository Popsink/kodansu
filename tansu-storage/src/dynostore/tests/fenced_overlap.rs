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

//! A segment that overlaps the frontier and reaches **past** it holds offsets
//! nothing else holds (#461).
//!
//! [`DynoStore::valid_substream_segments`] is the answer to "what does this
//! sub-stream actually contain", and ten call sites depend on it: the read path,
//! offset recovery, the high watermark, retention, and — the one that made this
//! destructive — compaction, which merges the fenced view and then retires the
//! whole run through `retire_segments`.
//!
//! It used to apply the reader's overlap rule from
//! `docs/virtual-topics-format.md`: *drop any entry whose `base_offset` falls
//! below the range already covered*. That rule is right for what it is written
//! for — resolving **one offset**, where the higher-priority entry already
//! answers it. It is not right for deciding **what a sub-stream contains**: an
//! entry that overlaps the frontier and reaches past it holds a tail nothing
//! else holds, and dropping it hid those offsets from every caller — and let
//! compaction delete them. The same logical error was in
//! `tansu-storage/src/audit.rs` until #460, from the same source; its
//! `audit_partition` is the reference implementation of the clip these tests
//! now pin on the engine.

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
    dynostore::{
        CoalesceTuning, CompactRun, DynoStore, SegmentFooter, Substream, SubstreamEntry,
        tests::init_tracing,
    },
};

use bytes::Bytes;

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "org.env.conn";
const TOPIC: &str = "org.env.conn.tab";

/// A footer whose single sub-stream entry covers `[base, base + count)`.
fn footer(base: i64, count: i64) -> SegmentFooter {
    SegmentFooter {
        writer_epoch: 1,
        nonce: 0,
        entries: vec![SubstreamEntry {
            topic: TOPIC.into(),
            topic_id: None,
            partition: 0,
            base_offset: base,
            record_count: count,
            byte_start: 0,
            byte_len: 64,
            max_timestamp: 0,
            producers: Vec::new(),
        }],
    }
}

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

fn new_store(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(1),
        prefix_compact_keep_hot: Some(0),
        prefix_compact_target_bytes: Some(1 << 20),
        ..Default::default()
    })
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

/// Write segment `seq` out of band, holding one sub-stream region for `TOPIC`
/// partition 0 that starts at `base` and concatenates `batches` — the only way
/// to build the overlapping shape #461 found on the fleet, since no healthy
/// write path produces it.
async fn put_segment(
    store: &DynoStore,
    bucket: &InMemory,
    seq: u64,
    base: i64,
    batches: Vec<deflated::Batch>,
) -> Result<()> {
    let (payload, _footer) =
        store.encode_segment_v3(&[(Topition::new(TOPIC, 0), base, batches)], 1, seq)?;
    _ = bucket.put(&segment_path(seq), payload).await?;

    Ok(())
}

/// The `(base_offset, one past last offset)` span of every fetched batch.
async fn fetched_spans(store: &DynoStore, tp: &Topition, offset: i64) -> Result<Vec<(i64, i64)>> {
    Ok(store
        .fetch(
            tp,
            offset,
            0,
            1_000_000,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(200),
        )
        .await?
        .iter()
        .map(|batch| {
            (
                batch.base_offset,
                batch.base_offset + batch.last_offset_delta as i64 + 1,
            )
        })
        .collect())
}

/// The shape, and what it now serves.
///
/// Two segments: one covering `[0, 100)`, another covering `[50, 10_000)`. The
/// second starts inside the first and reaches 9 900 offsets past it. Nothing
/// else in the prefix holds `[100, 10_000)`.
///
/// The fenced view used to drop the second entirely — 9 900 offsets hidden from
/// every caller, and destroyed on the compaction path, which is #461's 162M
/// records. It is now served **clipped**: the head `[50, 100)` stays with the
/// first segment, the tail `[100, 10_000)` with the second, and the sub-stream's
/// tail is what the prefix actually holds.
#[tokio::test]
async fn a_segment_reaching_past_the_frontier_is_clipped_not_dropped() -> Result<()> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    store.index_insert(PREFIX, 1, footer(0, 100), 0)?;
    store.index_insert(PREFIX, 2, footer(50, 9_950), 0)?;

    let fenced = store.valid_substream_segments(PREFIX, &Substream::Name(TOPIC.into()), 0)?;

    assert_eq!(
        2,
        fenced.len(),
        "the reaching entry must survive the sweep: {fenced:?}",
    );

    assert_eq!(1, fenced[0].seq);
    assert!(!fenced[0].is_clipped());
    assert_eq!(0, fenced[0].served_from);

    assert_eq!(2, fenced[1].seq);
    assert!(fenced[1].is_clipped());
    assert_eq!(
        100, fenced[1].served_from,
        "the head `[50, 100)` belongs to seq 1; only the tail is this entry's",
    );

    // Every tail-derived value — recovery, the high watermark, leaseless offset
    // assignment — folds this same `end()`, so the sub-stream's end is what the
    // prefix holds, not 9 900 offsets short of it.
    assert_eq!(10_000, fenced.last().map(|f| f.end()).unwrap_or_default());

    Ok(())
}

/// The case the rule exists for still behaves: a merged segment supersedes the
/// originals it merged, and those contribute nothing. Dropping them is right —
/// they are wholly inside what the merge already covers — and a sweep that kept
/// (or clipped) them would double-count offsets into the merged segment and
/// stall every healthy compaction in the fleet.
#[tokio::test]
async fn a_segment_wholly_inside_the_frontier_is_still_dropped() -> Result<()> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    // Sequence 9 is the merge of 1 and 2; it wins the tie on the higher
    // sequence and covers everything they did.
    store.index_insert(PREFIX, 9, footer(0, 200), 0)?;
    store.index_insert(PREFIX, 1, footer(0, 100), 0)?;
    store.index_insert(PREFIX, 2, footer(100, 100), 0)?;

    let fenced = store.valid_substream_segments(PREFIX, &Substream::Name(TOPIC.into()), 0)?;

    assert_eq!(
        1,
        fenced.len(),
        "only the merged segment should survive: {fenced:?}",
    );
    assert_eq!(9, fenced[0].seq);
    assert!(
        !fenced[0].is_clipped(),
        "nothing here overlaps the frontier, so nothing may look clipped",
    );

    Ok(())
}

/// The read path across a batch-aligned overlap: the batches wholly below the
/// frontier are skipped after decoding — served once, by the segment that owns
/// them — and the tail past the frontier is served rather than silently
/// swallowed.
///
/// Seq 0 covers `[0, 100)` and seq 1 covers `[50, 200)`, with seq 1's first
/// batch spanning exactly the duplicated `[50, 100)`.
#[tokio::test]
async fn a_fetch_serves_the_clipped_tail_and_nothing_twice() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    put_segment(&store, &bucket, 0, 0, vec![batch(50)?, batch(50)?]).await?;
    put_segment(&store, &bucket, 1, 50, vec![batch(50)?, batch(100)?]).await?;

    // The high watermark folds the clipped tail: 200, not 100.
    assert_eq!(200, store.high_watermark(&tp).await?);

    // From 0: seq 0's two batches, then seq 1's second — its first duplicates
    // `[50, 100)` and is skipped after decoding. No offset twice, no gap.
    assert_eq!(
        vec![(0, 50), (50, 100), (100, 200)],
        fetched_spans(&store, &tp, 0).await?,
    );

    // From inside the tail: the duplicate batch is still skipped, and the batch
    // holding the requested offset is served (a consumer discards the records
    // below its position, as for any batch starting before the fetch offset).
    assert_eq!(vec![(100, 200)], fetched_spans(&store, &tp, 120).await?,);

    Ok(())
}

/// Compaction over a batch-aligned overlap **heals** it: the merged segment
/// tiles `[0, 200)` exactly once, the duplicated batch is not fused in (the
/// merge would otherwise concatenate duplicate offsets and shift everything
/// above them), and the originals are retired.
#[tokio::test]
async fn compaction_heals_a_batch_aligned_overlap() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    put_segment(&store, &bucket, 0, 0, vec![batch(50)?, batch(50)?]).await?;
    put_segment(&store, &bucket, 1, 50, vec![batch(50)?, batch(100)?]).await?;

    store.refresh_prefix_index_forced(PREFIX).await?;
    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Merged(2),
    ));

    // One surviving object, whose single entry tiles the sub-stream exactly.
    assert_eq!(1, segments(&bucket).await.len());

    let fenced = store.valid_substream_segments(PREFIX, &Substream::Name(TOPIC.into()), 0)?;
    assert_eq!(1, fenced.len(), "{fenced:?}");
    assert_eq!(0, fenced[0].entry.base_offset);
    assert_eq!(200, fenced[0].entry.record_count);
    assert!(!fenced[0].is_clipped());

    // And the merged segment reads back whole: every offset once.
    assert_eq!(
        vec![(0, 50), (50, 100), (100, 200)],
        fetched_spans(&store, &tp, 0).await?,
    );

    Ok(())
}

/// A frontier that falls **inside** a batch cannot be merged faithfully:
/// batches are opaque, so the straddling batch can be neither split nor fused
/// without shifting offsets. Compaction refuses the run — both objects survive
/// — while the read path still serves the whole span, straddling batch
/// included (its head duplicates `[50, 100)`, which a consumer discards by
/// position).
///
/// The refusal answers `Retry` since #399: the break is memoized as a seam, so
/// the immediate re-selection ends before it — a run of one on each side, and
/// the pass drains instead of rebuilding, re-fetching and re-refusing the same
/// straddling run on every tick for as long as the objects live.
#[tokio::test]
async fn compaction_refuses_a_frontier_inside_a_batch() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    put_segment(&store, &bucket, 0, 0, vec![batch(50)?, batch(50)?]).await?;
    // One 100-record batch spanning [50, 150): the frontier (100) is inside it.
    put_segment(&store, &bucket, 1, 50, vec![batch(100)?]).await?;

    store.refresh_prefix_index_forced(PREFIX).await?;
    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Retry,
    ));
    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Drained,
    ));
    assert_eq!(
        2,
        segments(&bucket).await.len(),
        "a refused run must retire nothing",
    );

    assert_eq!(150, store.high_watermark(&tp).await?);
    assert_eq!(
        vec![(0, 50), (50, 100), (50, 150)],
        fetched_spans(&store, &tp, 0).await?,
    );

    Ok(())
}

/// A #461 hole — offsets that exist in **no** segment — must not wedge the
/// prefix's compaction (#399).
///
/// It used to: run selection always picks the oldest eligible run and is blind
/// to holes it has not been told about, so it rebuilt the straddling run,
/// fetched it, and refused it on every tick — and since the missing records
/// are gone, "until the underlying overlap is resolved" meant *forever*.
/// Measured on the production fleet: 21 054 segments created per hour against
/// 142 merged away, with every sampled refusal being a hole.
///
/// Now the first refusal memoizes the seam and answers `Retry`; re-selection
/// treats the seam as a run boundary, and the segments on **both** sides merge
/// — in the same pass, which is what `Retry` is for.
#[tokio::test]
async fn a_hole_bounds_the_runs_instead_of_wedging_them() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    // [0, 100) in two segments, then a hole — [100, 200) exists nowhere — then
    // [200, 350) in three.
    put_segment(&store, &bucket, 0, 0, vec![batch(50)?]).await?;
    put_segment(&store, &bucket, 1, 50, vec![batch(50)?]).await?;
    put_segment(&store, &bucket, 2, 200, vec![batch(50)?]).await?;
    put_segment(&store, &bucket, 3, 250, vec![batch(50)?]).await?;
    put_segment(&store, &bucket, 4, 300, vec![batch(50)?]).await?;

    store.refresh_prefix_index_forced(PREFIX).await?;

    // Pass 1: the oldest run straddles the hole. Refused once — and the seam
    // (seq 2, the segment whose region no longer met the tiling) is learned.
    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Retry,
    ));

    // Pass 2: the run below the hole merges — the seam bounds it.
    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Merged(2),
    ));

    // Pass 3: the run above the hole merges — the segments the wedge used to
    // strand. Its merged predecessor holds the pre-hole offsets at a fresh
    // *high* sequence, so no per-sequence boundary describes this run; it is
    // the offset-space coverage guard, armed by the seam, that keeps the
    // straddler out of it.
    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Merged(3),
    ));

    // The two merged segments are all that is left, one per side of the hole.
    // Coverage keeps them apart without a refusal: the prefix is genuinely
    // drained, nothing re-fetched, nothing re-refused.
    assert!(matches!(
        store.compact_prefix_segments(PREFIX).await?,
        CompactRun::Drained,
    ));

    assert_eq!(2, segments(&bucket).await.len());

    // Both sides of the hole still read back whole, at their own offsets — one
    // fetch from 0 serves everything that survives, skipping the hole.
    assert_eq!(350, store.high_watermark(&tp).await?);
    assert_eq!(
        vec![(0, 50), (50, 100), (200, 250), (250, 300), (300, 350)],
        fetched_spans(&store, &tp, 0).await?,
    );
    assert_eq!(
        vec![(200, 250), (250, 300), (300, 350)],
        fetched_spans(&store, &tp, 200).await?,
    );

    Ok(())
}

/// A seam is a skip-list entry, not a verdict: pruning drops the ones whose
/// segments are gone (#399), exactly as the quarantine's prune does, because a
/// sequence is never reused and a seam naming a retired segment bounds nothing.
#[tokio::test]
async fn a_seam_is_pruned_with_its_segment() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);

    let seams = BTreeSet::from([2, 9]);
    assert!(store.memo_compact_seams(PREFIX, &seams)?);
    // Nothing new the second time: the caller reads that as "re-selecting would
    // rebuild the same refused run" and drains instead of spinning.
    assert!(!store.memo_compact_seams(PREFIX, &seams)?);

    store.prune_compact_seams(PREFIX, &BTreeSet::from([2]))?;
    assert_eq!(BTreeSet::from([2]), store.compact_seams_of(PREFIX)?);

    // The last seam gone removes the prefix's entry entirely, and the seams
    // become learnable again.
    store.prune_compact_seams(PREFIX, &BTreeSet::new())?;
    assert!(store.compact_seams_of(PREFIX)?.is_empty());
    assert!(store.memo_compact_seams(PREFIX, &seams)?);

    Ok(())
}

// ---------------------------------------------------------------------------
// The drain-preservation property (#388, #395, #403, #433, #434, #461, #465,
// #469): whatever shape a damaged prefix holds, a compaction drain must never
// make a previously readable offset unreadable. Each of those issues fixed one
// path and the class recurred; this pins the invariant itself, over generated
// shapes none of the pointwise tests thought of.
// ---------------------------------------------------------------------------

/// A fixed-sequence generator, so every seed is a reproducible shape: a
/// failure names the seed, and the seed rebuilds the prefix byte-for-byte.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// Every offset served by any fetched batch, walking from `from` until fetch
/// returns nothing new.
async fn readable_offsets(store: &DynoStore, tp: &Topition) -> Result<BTreeSet<i64>> {
    let mut out = BTreeSet::new();
    let mut off = 0i64;
    for _ in 0..64 {
        let spans = fetched_spans(store, tp, off).await?;
        let mut advanced = false;
        for (base, end) in spans {
            out.extend(base..end);
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

const TOPIC2: &str = "org.env.conn.tab2";
const TOPIC3: &str = "org.env.conn.tab3";

/// Write segment `seq` out of band holding one region per `(topic, base,
/// batches)` triple — the multi-sub-stream shape every production segment has.
async fn put_segment_multi(
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

/// One generated prefix: 1-3 co-tenant sub-streams whose offset walks hole,
/// overlap and back-track at random, sliced into segments whose sequence order
/// is shuffled against offset order — the #461-era shapes, which no healthy
/// write path produces — then drained to a fixed point as the maintenance
/// tick does, with the index entries a stale peer would still hold re-inserted
/// between ticks. Every offset readable before must be readable after.
async fn probe_seed(seed: u64) -> Result<()> {
    let mut rng = Lcg(seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1));

    let bucket = InMemory::new();
    let target = [1usize << 20, 4_000, 2_000][rng.below(3) as usize];
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(1),
        prefix_compact_keep_hot: Some(rng.below(2) as usize),
        prefix_compact_target_bytes: Some(target),
        ..Default::default()
    });
    let topics: Vec<&'static str> = match rng.below(3) {
        0 => vec![TOPIC],
        1 => vec![TOPIC, TOPIC2],
        _ => vec![TOPIC, TOPIC2, TOPIC3],
    };
    for topic in &topics {
        create_topic(&store, topic).await?;
    }

    let nsegs = 4 + rng.below(9) as usize;

    // Per topic, walk offset space with holes and overlaps, splitting the walk
    // into `nsegs` slices (a topic may skip a segment).
    let mut segments: Vec<Vec<(&'static str, i64, Vec<deflated::Batch>)>> = vec![Vec::new(); nsegs];
    for topic in &topics {
        let mut cur = 0i64;
        for slot in segments.iter_mut() {
            if rng.below(5) == 0 && topics.len() > 1 {
                continue; // this topic sits this segment out
            }
            match rng.below(4) {
                0 => cur += 10 + rng.below(200) as i64,       // hole
                1 => cur -= (rng.below(120) as i64).min(cur), // overlap backwards
                _ => {}
            }
            let mut batches = Vec::new();
            let nb = 1 + rng.below(3) as usize;
            let mut len = 0usize;
            for _ in 0..nb {
                let n = 10 + rng.below(90) as usize;
                batches.push(batch(n)?);
                len += n;
            }
            slot.push((topic, cur, batches));
            cur += len as i64;
        }
    }

    // Assign sequences in a random order.
    let mut seqs: Vec<u64> = (0..nsegs as u64).collect();
    for i in (1..seqs.len()).rev() {
        let j = rng.below(i as u64 + 1) as usize;
        seqs.swap(i, j);
    }
    for (i, substreams) in segments.into_iter().enumerate() {
        if substreams.is_empty() {
            continue;
        }
        put_segment_multi(&store, &bucket, seqs[i], substreams).await?;
    }

    store.refresh_prefix_index_forced(PREFIX).await?;
    let mut before = Vec::new();
    for topic in &topics {
        before.push(readable_offsets(&store, &Topition::new(*topic, 0)).await?);
    }

    // Snapshot the whole cached index: (seq, footer, last_modified) — a stale
    // peer replica's view of the prefix.
    let stale: Vec<(u64, SegmentFooter, i64)> = {
        let index = store.prefix_index.lock().unwrap();
        index
            .get(PREFIX)
            .map(|entry| {
                entry
                    .segments
                    .iter()
                    .map(|(seq, cached)| (*seq, cached.footer.clone(), cached.last_modified_ms))
                    .collect()
            })
            .unwrap_or_default()
    };

    // Drain as production does, over several maintenance ticks — and between
    // ticks, restore any retired segment's index entry: the add-only index of a
    // peer replica that never observed the deletes, now holding the lease.
    for _ in 0..4 {
        _ = store.drain_compact_prefix(PREFIX).await;

        let live: BTreeSet<u64> = {
            let index = store.prefix_index.lock().unwrap();
            index
                .get(PREFIX)
                .map(|entry| entry.segments.keys().copied().collect())
                .unwrap_or_default()
        };
        for (seq, footer, last_modified) in &stale {
            if !live.contains(seq) {
                store.index_insert(PREFIX, *seq, footer.clone(), *last_modified)?;
            }
        }
    }

    for (topic, before) in topics.iter().zip(before) {
        let after = readable_offsets(&store, &Topition::new(*topic, 0)).await?;
        let missing: Vec<i64> = before.difference(&after).copied().collect();
        assert!(
            missing.is_empty(),
            "seed {seed} topic {topic}: {} offsets destroyed, first..last {:?}..{:?}",
            missing.len(),
            missing.first(),
            missing.last(),
        );
    }

    Ok(())
}

#[tokio::test]
async fn a_drain_preserves_every_readable_offset() -> Result<()> {
    let _guard = init_tracing()?;
    for seed in 0..300u64 {
        probe_seed(seed).await?;
    }
    Ok(())
}
