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
//! offset recovery, the high watermark, retention, and — the one that makes this
//! destructive — compaction, which merges the fenced view and then retires the
//! whole run through `retire_segments`.
//!
//! It applies the reader's overlap rule from `docs/virtual-topics-format.md`:
//! *drop any entry whose `base_offset` falls below the range already covered*.
//! That rule is right for what it is written for — resolving **one offset**,
//! where the higher-priority entry already answers it. It is not right for
//! deciding **what a run contains** before deleting the run.
//!
//! The same logical error was in `tansu-storage/src/audit.rs` until #460, from
//! the same source: the contract states the rule as a drop, because for a reader
//! that is all it needs to be.

use object_store::memory::InMemory;

use crate::{
    Result,
    dynostore::{DynoStore, SegmentFooter, SubstreamEntry, tests::init_tracing},
};

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

/// The shape, and what it costs.
///
/// Two segments: one covering `[0, 100)`, another covering `[50, 10_000)`. The
/// second starts inside the first and reaches 9 900 offsets past it. Nothing
/// else in the prefix holds `[100, 10_000)`.
///
/// The fenced view drops the second entirely — and that is still true, because
/// teaching ten callers to handle a partially-overlapping region is a change in
/// its own right. What is fixed is the *consequence*: compaction now asks first,
/// and refuses the run rather than merging `[0, 100)` and retiring both objects.
#[tokio::test]
async fn a_segment_reaching_past_the_frontier_is_seen_before_anything_is_deleted() -> Result<()> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    store.index_insert(PREFIX, 1, footer(0, 100), 0)?;
    store.index_insert(PREFIX, 2, footer(50, 9_950), 0)?;

    // The sweep itself still drops it: this is what compaction would have
    // merged, and it stops 9 900 offsets short of what the prefix holds.
    let fenced = store.valid_substream_segments(PREFIX, TOPIC, 0)?;
    let highest = fenced
        .iter()
        .map(|(_, entry)| entry.base_offset + entry.record_count)
        .max()
        .unwrap_or_default();

    assert_eq!(100, highest, "{fenced:?}");

    // And that is exactly what the guard reports, so the run is refused before
    // `retire_segments` can delete the object holding `[100, 10_000)`.
    assert_eq!(
        vec![2],
        store.substream_segments_reaching_past_frontier(PREFIX, TOPIC, 0)?,
    );

    Ok(())
}

/// The case the rule exists for still behaves, and the guard stays silent on it:
/// a merged segment supersedes the originals it merged, and those contribute
/// nothing. Dropping them is right — they are wholly inside what the merge
/// already covers — and a guard that fired here would refuse every healthy
/// compaction in the fleet, which is how this kind of check gets reverted.
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
    assert_eq!(9, fenced[0].0);

    assert!(
        store
            .substream_segments_reaching_past_frontier(PREFIX, TOPIC, 0)?
            .is_empty(),
        "a merged run must still compact",
    );

    Ok(())
}
