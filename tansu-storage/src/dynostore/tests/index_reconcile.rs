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

//! An index that only ever grew, and the listing that shrinks it (#408).
//!
//! `refresh_prefix_index_inner` lists `start_after` the highest known sequence
//! and the tail probe only folds forward, so nothing removes an entry for a
//! segment a *peer* retired. It leaves by being read: a 404 on the fetch path
//! prunes that one sequence and restarts, bounded by `MAX_ATTEMPTS`. Production,
//! `1.0.0-alpha.3`: **39 `segment` 404s/s on the brokers** against 7 on the
//! maintainers, each one a billed GET plus a restarted fetch, and **4.94 M
//! indexed segments across ten replicas**.
//!
//! Two things this deliberately does not do:
//!
//! - **It does not prune from a seq floor.** `retire_segments` raises the floor
//!   to `max(retired) + 1`, so a batch retiring a non-contiguous set leaves live
//!   segments below the floor. Treating the floor as a liveness boundary would
//!   hide live records from readers — and because the refresh is add-only, an
//!   entry wrongly dropped never comes back.
//! - **It does not prune everything at or below a vanished sequence.** Same
//!   hazard from the other direction, and for the same reason it is unrecoverable.
//!
//! What is safe is a listing, which is authoritative for the range it covered.

use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{ObjectStore as _, ObjectStoreExt as _, memory::InMemory, path::Path};
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
        coalesce_batches: Some(1),
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

/// A footer entry as a stale index entry carries it.
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

/// Retire `seqs` from the bucket alone, so this replica's index keeps naming
/// them: the state a peer's compaction leaves behind, not the route to it.
async fn retired_by_a_peer(bucket: &InMemory, seqs: impl IntoIterator<Item = u64>) -> Result<()> {
    for seq in seqs {
        bucket
            .delete(&Path::from(format!(
                "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{seq:0>20}.seg"
            )))
            .await?;
    }

    Ok(())
}

/// One listing drops every entry whose object is gone, not one per 404.
#[tokio::test]
async fn a_listing_drops_every_entry_whose_object_is_gone() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..6 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }
    assert_eq!(6, segments(&bucket).await.len());
    assert_eq!(vec![0, 1, 2, 3, 4, 5], indexed(&store));

    // A peer merged the oldest three away. This replica's index still names them
    // and nothing in it can find that out.
    retired_by_a_peer(&bucket, 0..3).await?;
    assert_eq!(vec![0, 1, 2, 3, 4, 5], indexed(&store));

    assert_eq!(3, store.reconcile_prefix_index(PREFIX).await?);
    assert_eq!(vec![3, 4, 5], indexed(&store));

    Ok(())
}

/// The tail is never dropped. A segment created while the listing was walked may
/// not appear in it, and the add-only refresh would never put a wrongly-dropped
/// entry back — so entries at or above the listing's own maximum are left alone.
#[tokio::test]
async fn the_tail_above_the_listing_is_never_dropped() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    _ = store.produce(None, &tp, batch("v0")?).await?;

    // An entry above everything the listing can return — the shape of a create
    // that landed after the walk began.
    store.index_insert(PREFIX, 900, stale_footer(900, 1), 0)?;

    assert_eq!(0, store.reconcile_prefix_index(PREFIX).await?);
    assert_eq!(vec![0, 900], indexed(&store));

    Ok(())
}

/// Rate-limited per prefix: a fetch storm against a stale prefix must not buy one
/// tier-1 listing per fetch.
#[tokio::test]
async fn a_second_reconcile_inside_the_window_does_not_list() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..6 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }

    retired_by_a_peer(&bucket, 0..2).await?;
    assert_eq!(2, store.reconcile_prefix_index(PREFIX).await?);

    // More staleness accrues immediately, and the window has not lapsed: nothing
    // lists, and the entries wait for the next window rather than for the next
    // fetch.
    retired_by_a_peer(&bucket, 2..4).await?;
    assert_eq!(0, store.reconcile_prefix_index(PREFIX).await?);
    assert_eq!(vec![2, 3, 4, 5], indexed(&store));

    Ok(())
}

/// The fetch that produced the evidence serves records it could not serve before.
///
/// This is the difference the pass makes rather than a restatement of it. The 404
/// arm prunes one sequence and restarts, bounded by `MAX_ATTEMPTS = 3`, so a
/// prefix carrying more than three stale entries ahead of its live ones cannot be
/// read at all: three attempts, three 404s, three prunes, and an empty answer for
/// records that are sitting in the bucket. One listing settles the whole prefix
/// and the restart serves them.
#[tokio::test]
async fn a_fetch_reads_past_more_stale_entries_than_it_has_attempts() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..8 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }

    // Six retired by a peer, twice the fetch's attempt budget.
    retired_by_a_peer(&bucket, 0..6).await?;

    let offsets: Vec<i64> = fetch_from(&store, &tp, 0)
        .await?
        .iter()
        .map(|batch| batch.base_offset)
        .collect();

    assert_eq!(
        vec![6, 7],
        offsets,
        "the fetch answered empty: it spent its attempts one stale entry at a time"
    );
    assert_eq!(vec![6, 7], indexed(&store));

    Ok(())
}
