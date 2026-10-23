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

//! A broker's ghost originals against a capped merge (#616).
//!
//! A broker learns of a merge through the add-only incremental refresh: it
//! folds the merged segment in and keeps the originals the merge retired until
//! a fetch 404s on one or the reconcile drops it. Before #610 that was
//! harmless — the merge was one entry per sub-stream, every ghost sat wholly
//! inside it, and the fenced view dropped them. #610 writes the merge as
//! several consecutive capped regions cut at byte boundaries, not at the
//! originals', so a ghost straddles two of them: the first merged region ends
//! inside it, the second begins inside it.
//!
//! The greedy sweep met the ghost between the two, saw it reach past the
//! frontier, and served it **clipped** — counting
//! `tansu_substream_overlap_clipped` on every fenced read of every partition
//! carrying one (~554 000 an hour on use1 after the alpha.21 rollout, against
//! a bucket with no overlap at all) and planning reads from an object that was
//! already deleted, each one a 404 and a relist. The offsets were right; the
//! counter and the GETs were not.

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
        CoalesceTuning, CompactRun, DynoStore, SegmentFooter, Substream, SubstreamEntry,
        tests::init_tracing,
    },
};

use bytes::Bytes;

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "org.env.conn";
const TOPIC: &str = "org.env.conn.tab";

/// Records per batch, and batches per original segment.
const RECORDS: usize = 5;
const PER_SEGMENT: usize = 2;
const ORIGINALS: u64 = 3;

/// One past the last offset the sub-stream holds.
const END: i64 = (RECORDS * PER_SEGMENT) as i64 * ORIGINALS as i64;

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

/// A store whose merge caps a region at three batches: 15 offsets, so the
/// first merged region ends at 15, inside the 10-offset original `[10, 20)`.
fn new_store(bucket: &InMemory) -> Result<DynoStore> {
    let cap = 3 * batch(RECORDS)?.wire_size();

    Ok(
        DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
            prefix_compact_min_segments: Some(1),
            prefix_compact_keep_hot: Some(0),
            prefix_compact_target_bytes: Some(1 << 20),
            prefix_compact_region_bytes: Some(cap),
            ..Default::default()
        }),
    )
}

fn segment_path(seq: u64) -> Path {
    Path::from(format!(
        "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{seq:0>20}.seg"
    ))
}

async fn segments(bucket: &InMemory) -> Result<Vec<Path>> {
    let mut paths = bucket
        .list(Some(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/"
        ))))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await?;
    paths.sort();
    Ok(paths)
}

/// Every offset a consumer starting at 0 is delivered, in order: it fetches
/// from its position, drops the records of a batch below it, and advances past
/// the last batch.
async fn consumed(store: &DynoStore, tp: &Topition) -> Result<Vec<i64>> {
    let mut offsets = Vec::new();
    let mut position = 0;

    while position < END {
        let batches = store
            .fetch(
                tp,
                position,
                0,
                1_000_000,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(200),
            )
            .await?;

        assert!(!batches.is_empty(), "no progress at {position}");

        for batch in batches {
            let end = batch.base_offset + batch.last_offset_delta as i64 + 1;
            offsets.extend(position.max(batch.base_offset)..end);
            position = position.max(end);
        }
    }

    Ok(offsets)
}

/// The broker's view, after the maintainer's capped merge, while its index
/// still holds every original: the merged regions alone, none of them clipped,
/// and a consumer served every offset exactly once — both while the originals'
/// objects still exist (the merge's write→delete window) and after they are
/// gone.
#[tokio::test]
async fn ghost_originals_are_dropped_not_clipped_against_a_capped_merge() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let maintainer = new_store(&bucket)?;
    let broker = new_store(&bucket)?;
    let tp = Topition::new(TOPIC, 0);

    _ = maintainer
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let mut originals = Vec::new();
    for seq in 0..ORIGINALS {
        let base = (RECORDS * PER_SEGMENT) as i64 * seq as i64;
        let batches = (0..PER_SEGMENT)
            .map(|_| batch(RECORDS))
            .collect::<Result<Vec<_>>>()?;
        let (payload, _footer) =
            maintainer.encode_segment_v3(&[(tp.clone(), base, batches)], 1, seq)?;
        let bytes = Bytes::from(payload);
        _ = bucket.put(&segment_path(seq), bytes.clone().into()).await?;
        originals.push((seq, bytes));
    }

    broker.refresh_prefix_index_forced(PREFIX).await?;

    maintainer.refresh_prefix_index_forced(PREFIX).await?;
    assert!(matches!(
        maintainer.compact_prefix_segments(PREFIX).await?,
        CompactRun::Merged(ORIGINALS),
    ));
    assert_eq!(1, segments(&bucket).await?.len());

    // Within the reconcile interval this is the incremental refresh: it folds
    // the merged segment in and drops nothing.
    broker.refresh_prefix_index_forced(PREFIX).await?;

    let substream = Substream::Name(TOPIC.into());
    let merged = ORIGINALS;

    let fenced = broker.valid_substream_segments(PREFIX, &substream, 0)?;
    assert_eq!(
        vec![(merged, 0, 15), (merged, 15, END)],
        fenced
            .iter()
            .map(|fenced| (fenced.seq, fenced.served_from, fenced.end()))
            .collect::<Vec<_>>(),
        "only the merged regions may serve: {fenced:?}",
    );
    assert!(
        fenced.iter().all(|fenced| !fenced.is_clipped()),
        "a ghost wholly covered by the merge is not an overlap: {fenced:?}",
    );

    let every: Vec<i64> = (0..END).collect();

    for (seq, bytes) in &originals {
        _ = bucket
            .put(&segment_path(*seq), bytes.clone().into())
            .await?;
    }
    assert_eq!(every, consumed(&broker, &tp).await?, "originals present");

    for (seq, _) in &originals {
        bucket.delete(&segment_path(*seq)).await?;
    }
    assert_eq!(every, consumed(&broker, &tp).await?, "originals deleted");

    Ok(())
}

/// A footer of `(base, count)` sub-stream entries, in one segment.
fn footer(writer_epoch: i64, entries: &[(i64, i64)]) -> SegmentFooter {
    SegmentFooter {
        writer_epoch,
        nonce: 0,
        entries: entries
            .iter()
            .map(|&(base_offset, record_count)| SubstreamEntry {
                topic: TOPIC.into(),
                topic_id: None,
                partition: 0,
                base_offset,
                record_count,
                byte_start: 0,
                byte_len: 64,
                max_timestamp: 0,
                producers: Box::default(),
            })
            .collect(),
    }
}

/// A segment as `(seq, writer_epoch, entries)`.
type Segment<'a> = (u64, i64, &'a [(i64, i64)]);

/// `(seq, served_from, end)` of the fenced view over `segments`.
fn served(segments: &[Segment<'_>]) -> Result<Vec<(u64, i64, i64)>> {
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    for &(seq, epoch, entries) in segments {
        store.index_insert(PREFIX, seq, footer(epoch, entries), 0)?;
    }

    Ok(store
        .valid_substream_segments(PREFIX, &Substream::Name(TOPIC.into()), 0)?
        .iter()
        .map(|fenced| (fenced.seq, fenced.served_from, fenced.end()))
        .collect())
}

/// The drop is for an entry whose head *and* tail are held by entries that
/// outrank it, and nothing wider: an overlap the bucket really carries is still
/// served clipped, and still counted.
#[test]
fn only_an_outranked_straddle_is_dropped() -> Result<()> {
    // The ghost: seq 9's capped regions outrank seq 1 on both sides.
    assert_eq!(
        vec![(9, 0, 15), (9, 15, 30)],
        served(&[(1, 1, &[(10, 10)]), (9, 1, &[(0, 15), (15, 15)])])?,
    );

    // A higher writer epoch outranks a higher sequence.
    assert_eq!(
        vec![(1, 0, 15), (2, 15, 30)],
        served(&[(1, 2, &[(0, 15)]), (2, 2, &[(15, 15)]), (5, 1, &[(10, 10)])])?,
    );

    // Each entry outranks the one before it, so the frontier was set by a
    // lower rank: seq 2's head is held, but not by anything that outranks it.
    assert_eq!(
        vec![(1, 0, 100), (2, 100, 150), (3, 150, 200)],
        served(&[
            (1, 1, &[(0, 100)]),
            (2, 1, &[(50, 100)]),
            (3, 1, &[(100, 100)])
        ])?,
    );

    // The tail is held only by a lower rank: clipped, as before.
    assert_eq!(
        vec![(9, 0, 15), (5, 15, 20), (1, 20, 30)],
        served(&[(9, 1, &[(0, 15)]), (5, 1, &[(10, 10)]), (1, 1, &[(15, 15)])])?,
    );

    // A gap in what outranks it leaves offsets only the straddler holds.
    assert_eq!(
        vec![(9, 0, 15), (1, 15, 30), (9, 30, 40)],
        served(&[(1, 1, &[(10, 20)]), (9, 1, &[(0, 15), (20, 20)])])?,
    );

    Ok(())
}
