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

//! Why the busiest prefixes did not converge, after #140's remedies all shipped
//! (#399).
//!
//! #140 concluded the limit was one maintainer's merge throughput and added
//! concurrency *within* a maintainer. That shipped, and production still sat at
//! 17 500 indexed segments against a 256 trigger — with the four maintainers at
//! **12 millicores between them**, S3 GETs at 24 ms, and compaction firing for
//! 1–2 minutes per 10-minute window. Not throughput-bound. The two things that
//! were actually true:
//!
//! 1. **A run that could not proceed ended the prefix's drain.**
//!    `fetch_segment_objects` answering `None` — the index named segments a peer
//!    had already retired — came back as `Ok(0)`, and the drain reads `Ok(0)` as
//!    "this prefix is drained". Selection picks the *oldest* segments, which are
//!    exactly the ones a peer's compaction retires first, so the drain stopped on
//!    its first run, having merged nothing. Fleet rates:
//!    `tansu_prefix_segment_vanished_before_read` at ~11/s against 0.55
//!    compaction runs/s that merged anything.
//!
//! 2. **The index never reconciles away what it did not delete itself.** The
//!    incremental refresh is add-only below the tail, so those stale entries
//!    accumulate for the life of the process — four maintainers reported 17 517,
//!    14 374, 13 932 and **67** indexed segments for the same prefix at the same
//!    instant. Which is also why `tansu_prefix_segments_live` could not be read as
//!    a backlog.
//!
//! The two are one fix: a run that finds its segments gone prunes them and the
//! drain *takes the next run*, so a drain walks its index through the ghosts
//! instead of stopping at the first of them — and the index converges on the
//! truth at no extra request.

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{ObjectStore as _, memory::InMemory, path::Path};
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage as _, Topition,
    dynostore::{CoalesceTuning, DynoStore, SegmentFooter, SubstreamEntry},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

const PREFIX: &str = "org.env.conn";
const TOPIC: &str = "org.env.conn.table";

fn new_store(bucket: &InMemory) -> DynoStore {
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

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

/// A footer entry for a `TOPIC`-0 sub-stream, as a stale index entry carries it.
fn stale_footer(base: i64, count: i64) -> SegmentFooter {
    SegmentFooter {
        writer_epoch: 1,
        nonce: 0,
        entries: vec![SubstreamEntry {
            topic: TOPIC.to_owned(),
            partition: 0,
            base_offset: base,
            record_count: count,
            byte_start: 0,
            byte_len: 8,
            max_timestamp: 0,
            producers: Vec::new(),
        }],
    }
}

/// The sequences this process has indexed for `PREFIX`.
fn indexed(store: &DynoStore) -> Vec<u64> {
    store
        .prefix_index
        .lock()
        .expect("prefix index")
        .get(PREFIX)
        .map(|entry| entry.segments.keys().copied().collect())
        .unwrap_or_default()
}

/// Seed the fleet's shape: an index whose *oldest* entries name segments that a
/// peer has already retired, ahead of the live ones.
///
/// Built the way production got there — merge the originals away, then re-inject
/// their sequences as stale entries, then produce more. Run selection picks the
/// oldest first, so every one of these is met before a live segment is.
async fn with_ghost_entries(bucket: &InMemory, ghosts: u64, live: usize) -> Result<DynoStore> {
    let store = new_store(bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    for seq in 0..ghosts {
        _ = store.produce(None, &tp, batch(&format!("g{seq}"))?).await?;
    }
    while store.drain_compact_prefix(PREFIX).await > 0 {}

    for seq in 0..ghosts {
        store.index_insert(PREFIX, seq, stale_footer(seq as i64, 1), 0)?;
    }

    for i in 0..live {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }

    Ok(store)
}

/// The fix, at its smallest: one run whose segments are gone must not be read as
/// "this prefix has nothing left to merge".
#[tokio::test]
async fn a_run_of_gone_segments_does_not_end_the_drain() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = with_ghost_entries(&bucket, 4, 4).await?;

    let before = segments(&bucket).await.len();
    assert!(
        indexed(&store).len() > before,
        "the index must carry more than the bucket does for this to test anything"
    );

    // Before #399 this returned 0: the first run selected the four oldest
    // entries, all gone, and `Ok(0)` ended the drain.
    assert!(
        store.drain_compact_prefix(PREFIX).await > 0,
        "the drain merged nothing past the gone segments"
    );

    assert!(segments(&bucket).await.len() < before);

    Ok(())
}

/// And the index converges on the truth as it goes: every run that finds its
/// segments gone prunes them, so the drain walks the ghosts out rather than
/// stopping at the first of them.
///
/// This is what makes `tansu_prefix_segments_live` mean something again. Four
/// maintainers reporting 17 517, 14 374, 13 932 and 67 for one prefix were all
/// reporting how stale their own index was.
#[tokio::test]
async fn the_drain_prunes_the_index_down_to_what_the_bucket_holds() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = with_ghost_entries(&bucket, 6, 6).await?;

    let ghosts: Vec<u64> = (0..6).collect();
    assert!(
        ghosts.iter().all(|seq| indexed(&store).contains(seq)),
        "the ghosts are seeded: {:?}",
        indexed(&store)
    );

    // One drain, not a loop: the claim is that a single pass walks the ghosts out
    // rather than stopping at the first of them.
    _ = store.drain_compact_prefix(PREFIX).await;

    let live: Vec<u64> = segments(&bucket)
        .await
        .iter()
        .filter_map(|path| {
            path.parts()
                .next_back()
                .map(|name| name.as_ref().to_owned())
        })
        .filter_map(|name| name.get(0..20).and_then(|seq| seq.parse::<u64>().ok()))
        .collect();

    assert_eq!(live, indexed(&store), "index still carries gone segments");

    Ok(())
}

/// Nothing is lost on the way. The ghosts are entries, not records, and every
/// record produced is still readable at its own offset afterwards.
#[tokio::test]
async fn draining_through_gone_segments_keeps_every_record() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = with_ghost_entries(&bucket, 4, 4).await?;
    let tp = Topition::new(TOPIC, 0);

    let before: Vec<i64> = fetch_from(&store, &tp, 0)
        .await?
        .iter()
        .map(|batch| batch.base_offset)
        .collect();
    assert_eq!((0..8).collect::<Vec<i64>>(), before);

    _ = store.drain_compact_prefix(PREFIX).await;

    assert_eq!(
        before,
        fetch_from(&new_store(&bucket), &tp, 0)
            .await?
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<i64>>()
    );

    Ok(())
}

/// The sweep's work-set: only claimed prefixes still over the trigger, largest
/// first, and read from the index this process already holds — no request.
#[tokio::test]
async fn the_backlog_sweep_takes_the_prefixes_over_the_trigger_largest_first() -> Result<(), Error>
{
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);

    let big = "org.env.big";
    let small = "org.env.small";
    let under = "org.env.under";

    for (prefix, segments) in [(big, 9u64), (small, 5), (under, 2)] {
        for seq in 0..segments {
            store.index_insert(prefix, seq, stale_footer(seq as i64, 1), 0)?;
        }
    }

    let compactable = [big, small, under]
        .into_iter()
        .map(str::to_owned)
        .collect::<std::collections::BTreeSet<String>>();

    // `under` holds 2, which is not *over* `prefix_compact_min_segments`, so it
    // cannot yield a run and is never swept for one.
    assert_eq!(
        vec![big.to_owned(), small.to_owned()],
        store.backlogged_prefixes(&compactable)
    );

    // A prefix outside the claim is not swept however large it is: the lease
    // holder is another replica, and taking it here would only be fenced.
    assert_eq!(
        vec![small.to_owned()],
        store.backlogged_prefixes(&[small.to_owned()].into_iter().collect())
    );

    Ok(())
}

/// A tick keeps sweeping while it owns a backlog, rather than returning and
/// idling out the rest of the interval — but stops the moment a sweep merges
/// nothing, because then the prefix is waiting on produce and not on a tick.
#[tokio::test]
async fn a_tick_sweeps_until_the_backlog_stops_shrinking() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..8 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }
    assert_eq!(8, segments(&bucket).await.len());

    _ = store.maintain_prefix_segments(now_ms(), None).await?;

    // Drained, and a second pass has nothing to do — so a further sweep would
    // have merged nothing and the loop is not spinning on it.
    assert!(segments(&bucket).await.len() <= 2);
    assert!(
        store
            .backlogged_prefixes(&[PREFIX.to_owned()].into_iter().collect())
            .is_empty()
    );

    let (_, compacted) = store.maintain_prefix_segments(now_ms(), None).await?;
    assert_eq!(0, compacted);

    assert_eq!(
        (0..8).collect::<Vec<i64>>(),
        fetch_from(&store, &tp, 0)
            .await?
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<i64>>()
    );

    Ok(())
}
