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

use std::time::Duration;

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
        CoalesceTuning, CompactRun, DynoStore, SegmentFooter, SubstreamEntry, tests::init_tracing,
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

    let fenced = store.valid_substream_segments(PREFIX, TOPIC, 0)?;

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

    let fenced = store.valid_substream_segments(PREFIX, TOPIC, 0)?;

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

    let fenced = store.valid_substream_segments(PREFIX, TOPIC, 0)?;
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
