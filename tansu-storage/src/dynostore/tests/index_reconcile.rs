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
//! segment a *peer* retired. #413 made a listing do it and triggered that listing
//! from a fetch 404 — proof that an entry names an object that is gone. The proof
//! never arrives: **consumers read the tail and retirement takes the head**, so a
//! retired segment is one nothing will fetch. On `1.0.0-alpha.11`,
//! `tansu_prefix_segment_absent{caller="fetch"}` is 0.0007/s and
//! `tansu_prefix_index_reconciled` 0.00014 entries/s, while the index climbed
//! monotonically to **1.51 M segments across ten replicas against 17.6 k objects
//! in the bucket** — 62-73 % of the broker's live heap, naming objects that are
//! not there.
//!
//! So the pass is now **scheduled** rather than provoked: once per
//! [`DynoStore::PREFIX_INDEX_RECONCILE_INTERVAL`] a prefix's refresh lists the
//! whole prefix instead of its tail and drops what it does not find. A 404 still
//! lapses the window (`index_invalidate`), because it is still evidence — it is
//! just no longer the only thing that starts one.
//!
//! Three things this deliberately does not do:
//!
//! - **It does not prune from a seq floor.** `retire_segments` raises the floor
//!   to `max(retired) + 1`, so a batch retiring a non-contiguous set leaves live
//!   segments below the floor. Treating the floor as a liveness boundary would
//!   hide live records from readers — and because the refresh is add-only below
//!   the tail, an entry wrongly dropped never comes back.
//! - **It does not prune everything at or below a vanished sequence.** Same
//!   hazard from the other direction, and for the same reason it is unrecoverable.
//! - **It does not touch the tail.** Entries at or above the listing's own
//!   maximum are left alone, whatever the listing says about them.
//!
//! What is safe is a listing, which is authoritative for the range it covered.

use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{TryStreamExt as _, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore as _,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
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

fn new_store<O>(bucket: O) -> DynoStore
where
    O: object_store::ObjectStore,
{
    DynoStore::new(CLUSTER, NODE, bucket).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        ..Default::default()
    })
}

/// As [`new_store`], but with compaction thresholds a test can reach: the
/// default is 256 segments per prefix, so nothing is ever retired at this scale
/// and there would be no ghost to converge on.
fn new_maintaining_store(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
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

/// The sequences this process holds as resolved-but-unreadable (#157/#191) — the
/// index's other monotonic set.
fn opaque(store: &DynoStore) -> Vec<u64> {
    store
        .prefix_index
        .lock()
        .expect("prefix index")
        .get(PREFIX)
        .map(|entry| entry.opaque.iter().copied().collect())
        .unwrap_or_default()
}

/// Lapse the reconcile window, the way five minutes of uptime does. The interval
/// is a constant and a test cannot wait it out, so this is how a scheduled pass
/// coming due is expressed.
fn the_window_lapses(store: &DynoStore) {
    if let Some(entry) = store
        .prefix_index
        .lock()
        .expect("prefix index")
        .get_mut(PREFIX)
    {
        entry.reconciled_at = None;
    }
}

/// Lapse the index TTL alone, leaving the reconcile window intact — a refresh
/// that has real work to do, on a prefix whose scheduled pass is not due.
///
/// `index_invalidate` cannot stand in for this: a 404 lapses both clocks.
fn the_ttl_lapses(store: &DynoStore) {
    if let Some(entry) = store
        .prefix_index
        .lock()
        .expect("prefix index")
        .get_mut(PREFIX)
    {
        entry.refreshed_at = None;
    }
}

/// A footer entry as a stale index entry carries it.
fn stale_footer(base: i64, count: i64) -> SegmentFooter {
    SegmentFooter {
        writer_epoch: 1,
        nonce: 0,
        entries: vec![SubstreamEntry {
            topic: TOPIC.to_owned(),
            topic_id: None,
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

/// What the pass costs, and what it discovers: `.seg` GETs that answered
/// `NotFound`, whole-prefix listings, and `start_after` listings.
///
/// Counting the two listing shapes apart is the point — the reconciling pass
/// *replaces* a tail listing with a whole-prefix one at the same request count,
/// and the acceptance is that it does not add one per index TTL.
struct Tallies<O> {
    inner: O,
    absent: Arc<AtomicUsize>,
    full_lists: Arc<AtomicUsize>,
    tail_lists: Arc<AtomicUsize>,
}

impl Tallies<InMemory> {
    fn wrapping(bucket: &InMemory) -> Self {
        Self {
            inner: bucket.clone(),
            absent: Arc::new(AtomicUsize::new(0)),
            full_lists: Arc::new(AtomicUsize::new(0)),
            tail_lists: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// The counters, shared with the store the wrapper is handed to.
#[derive(Clone, Debug)]
struct Counts {
    absent: Arc<AtomicUsize>,
    full_lists: Arc<AtomicUsize>,
    tail_lists: Arc<AtomicUsize>,
}

impl Counts {
    fn of<O>(tallies: &Tallies<O>) -> Self {
        Self {
            absent: tallies.absent.clone(),
            full_lists: tallies.full_lists.clone(),
            tail_lists: tallies.tail_lists.clone(),
        }
    }

    fn absent(&self) -> usize {
        self.absent.load(Ordering::SeqCst)
    }

    fn full_lists(&self) -> usize {
        self.full_lists.load(Ordering::SeqCst)
    }

    fn tail_lists(&self) -> usize {
        self.tail_lists.load(Ordering::SeqCst)
    }

    fn reset(&self) {
        self.absent.store(0, Ordering::SeqCst);
        self.full_lists.store(0, Ordering::SeqCst);
        self.tail_lists.store(0, Ordering::SeqCst);
    }
}

/// Whether a listing is of a prefix's `segments/`, which is the only tier-1 plane
/// these tests are about.
fn of_segments(prefix: Option<&Path>) -> bool {
    prefix.is_some_and(|prefix| prefix.as_ref().ends_with("segments"))
}

impl<O> Debug for Tallies<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tallies").finish()
    }
}

impl<O> Display for Tallies<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tallies").finish()
    }
}

#[async_trait]
impl<O> object_store::ObjectStore for Tallies<O>
where
    O: object_store::ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        let outcome = self.inner.get_opts(location, options).await;

        if location.as_ref().ends_with(".seg")
            && matches!(outcome, Err(object_store::Error::NotFound { .. }))
        {
            _ = self.absent.fetch_add(1, Ordering::SeqCst);
        }

        outcome
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        if of_segments(prefix) {
            _ = self.full_lists.fetch_add(1, Ordering::SeqCst);
        }

        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        if of_segments(prefix) {
            _ = self.tail_lists.fetch_add(1, Ordering::SeqCst);
        }

        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// The refresh itself drops every entry whose object is gone — no 404 required,
/// which is the whole of #408 after #413.
#[tokio::test]
async fn a_scheduled_refresh_drops_every_entry_whose_object_is_gone() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(bucket.clone());
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..6 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }
    assert_eq!(6, segments(&bucket).await.len());
    assert_eq!(vec![0, 1, 2, 3, 4, 5], indexed(&store));

    // A peer merged the oldest three away. Nothing reads there — the consumers
    // are at the tail — so no fetch will ever 404 on them.
    retired_by_a_peer(&bucket, 0..3).await?;
    assert_eq!(vec![0, 1, 2, 3, 4, 5], indexed(&store));

    // A refresh inside the window is still the cheap incremental one, and still
    // add-only: the ghosts survive it.
    store.refresh_prefix_index(PREFIX).await?;
    assert_eq!(vec![0, 1, 2, 3, 4, 5], indexed(&store));

    // The window lapses and the next refresh — an ordinary read-path refresh,
    // asked for nothing but the tail — settles the whole prefix.
    the_window_lapses(&store);
    store.refresh_prefix_index(PREFIX).await?;
    assert_eq!(vec![3, 4, 5], indexed(&store));

    Ok(())
}

/// Acceptance criterion 2: the index tracks the objects in the bucket rather than
/// growing monotonically with uptime.
///
/// The fleet shape, on one bucket: a peer maintains the prefix (compaction merges
/// segments away and retires the inputs) while this replica only ever reads it.
/// Ten rounds of that used to leave a strictly growing index — 1.51 M entries
/// across ten replicas against 17.6 k objects. Here the reader's index is the
/// bucket's own segment set at the end of every round.
#[tokio::test]
async fn a_readers_index_tracks_the_bucket_not_its_uptime() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let maintainer = new_maintaining_store(&bucket);
    let reader = new_store(bucket.clone());

    create_topic(&maintainer, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    let mut peak = 0usize;

    for round in 0..10 {
        for i in 0..4 {
            _ = maintainer
                .produce(None, &tp, batch(&format!("r{round}v{i}"))?)
                .await?;
        }

        // The reader indexes what is there now, and reads it.
        the_ttl_lapses(&reader);
        the_window_lapses(&reader);
        assert!(!fetch_from(&reader, &tp, 0).await?.is_empty());

        // The peer merges the round's segments away and retires the inputs.
        while maintainer.drain_compact_prefix(PREFIX).await > 0 {}

        // Both clocks lapse, so the refresh does the same *upward* work either
        // way and the only thing under test is whether it also goes downwards.
        the_ttl_lapses(&reader);
        the_window_lapses(&reader);
        reader.refresh_prefix_index(PREFIX).await?;

        let live = segments(&bucket).await.len();
        peak = peak.max(indexed(&reader).len());

        assert_eq!(
            live,
            indexed(&reader).len(),
            "round {round}: the reader indexes {:?} against {live} objects",
            indexed(&reader),
        );
    }

    // And what it holds is bounded by the bucket, not by how long it has been up.
    assert!(
        peak <= segments(&bucket).await.len() + 4,
        "the index peaked at {peak} entries over ten rounds",
    );

    Ok(())
}

/// The tail is never dropped. A segment created while the listing was walked may
/// not appear in it, and the incremental refresh would never put a wrongly-dropped
/// entry back — so entries at or above the listing's own maximum are left alone.
#[tokio::test]
async fn the_tail_above_the_listing_is_never_dropped() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(bucket.clone());
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    _ = store.produce(None, &tp, batch("v0")?).await?;

    // An entry above everything the listing can return — the shape of a create
    // that landed after the walk began.
    store.index_insert(PREFIX, 900, stale_footer(900, 1), 0)?;

    the_window_lapses(&store);
    store.refresh_prefix_index(PREFIX).await?;
    assert_eq!(vec![0, 900], indexed(&store));

    Ok(())
}

/// Acceptance criterion 3: a pass that finds nothing gone drops nothing, and the
/// records stay readable.
///
/// The failure mode any shortcut here risks — a floor read as a liveness
/// boundary, a prune at-or-below a vanished sequence — is a read served short
/// from an index that dropped a live name. Only a listing's own absences are
/// taken as evidence, so a healthy prefix passes through untouched.
#[tokio::test]
async fn a_pass_that_finds_nothing_gone_drops_nothing() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(bucket.clone());
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..6 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }

    the_window_lapses(&store);
    store.refresh_prefix_index(PREFIX).await?;

    assert_eq!(vec![0, 1, 2, 3, 4, 5], indexed(&store));
    assert_eq!(6, fetch_from(&store, &tp, 0).await?.len());

    Ok(())
}

/// What it costs: one whole-prefix listing per window, and the refreshes inside
/// the window keep following the tail.
///
/// The read path cannot afford a reconciling listing per index TTL (5 s — 45/s
/// per replica on this fleet). It can afford one per prefix per five minutes, and
/// this pins that the window is what bounds it: the pass replaces a tail listing
/// with a whole-prefix one at the same request count, and adds nothing until the
/// window lapses again.
#[tokio::test]
async fn the_window_bounds_the_listings_the_pass_costs() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let tallies = Tallies::wrapping(&bucket);
    let counts = Counts::of(&tallies);
    let store = new_store(tallies);

    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..6 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }

    retired_by_a_peer(&bucket, 0..2).await?;
    the_window_lapses(&store);
    counts.reset();

    store.refresh_prefix_index(PREFIX).await?;
    assert_eq!(vec![2, 3, 4, 5], indexed(&store));
    assert_eq!(1, counts.full_lists(), "the pass is one listing");
    assert_eq!(0, counts.tail_lists());

    // More staleness accrues immediately, and the window has not lapsed: the
    // index is not re-listed, and the entries wait for the next window rather
    // than for a 404 that is never coming.
    retired_by_a_peer(&bucket, 2..4).await?;
    counts.reset();

    for _ in 0..8 {
        the_ttl_lapses(&store);
        store.refresh_prefix_index(PREFIX).await?;
    }

    assert_eq!(vec![2, 3, 4, 5], indexed(&store));
    assert_eq!(
        0,
        counts.full_lists(),
        "eight refreshes inside the window bought a whole-prefix listing",
    );

    Ok(())
}

/// A 404 still lapses the window, so the fetch that produced the evidence serves
/// records it could not serve before.
///
/// The 404 arm prunes one sequence and restarts, bounded by `MAX_ATTEMPTS = 3`,
/// so a prefix carrying more than three stale entries ahead of its live ones
/// cannot be read at all: three attempts, three 404s, three prunes, and an empty
/// answer for records that are sitting in the bucket. One listing settles the
/// whole prefix and the restart serves them — for **one** 404, not one per entry.
#[tokio::test]
async fn a_fetch_reads_past_more_stale_entries_than_it_has_attempts() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let tallies = Tallies::wrapping(&bucket);
    let counts = Counts::of(&tallies);
    let store = new_store(tallies);

    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..8 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }

    // Six retired by a peer, twice the fetch's attempt budget.
    retired_by_a_peer(&bucket, 0..6).await?;
    counts.reset();

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
    assert_eq!(
        1,
        counts.absent(),
        "one 404 settles the prefix; the other five entries left without being read",
    );

    Ok(())
}

/// The index's other monotonic set converges too: a name held only because its
/// object could not be read (#157/#191) is dropped once a listing says the object
/// is gone.
///
/// Held for ever, the set is a second unbounded structure — and the reason it was
/// held is to make the leaseless arbiter step over an **occupied** name. A name
/// the listing does not return is not occupied, and cannot be handed back out:
/// every delete goes through `retire_segments`, which raises the durable floor
/// past the sequence write-ahead of removing it (#77), and every candidate is
/// `max(tail + 1, floor)`.
#[tokio::test]
async fn an_unreadable_name_whose_object_is_gone_is_dropped() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(bucket.clone());
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    // Something is squatting sequence 0 with an object that carries no decodable
    // footer, so the arbiter must step over the name and the index must hold it.
    let squatter = Path::from(format!(
        "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{:0>20}.seg",
        0
    ));
    _ = bucket
        .put(&squatter, PutPayload::from_static(b"not a segment"))
        .await?;

    for i in 0..3 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }

    assert_eq!(vec![0], opaque(&store));
    assert_eq!(vec![1, 2, 3], indexed(&store));

    // The squatter is removed. Nothing reads the name — nothing can — so only a
    // listing can tell this index that it is free of it.
    bucket.delete(&squatter).await?;

    the_window_lapses(&store);
    store.refresh_prefix_index(PREFIX).await?;

    assert_eq!(Vec::<u64>::new(), opaque(&store));
    assert_eq!(vec![1, 2, 3], indexed(&store));

    Ok(())
}

/// A `segment` 404 is not evidence of a stale index, which is what #408's
/// acceptance assumed.
///
/// The issue's first criterion is *"`segment` `not_found` on the brokers drops by
/// at least an order of magnitude"*, on the reading that the plane is stale
/// entries being discovered one 404 at a time. Part of it is, and #413 removed
/// that part. The rest is `probe_prefix_tail` asking whether `cursor + 1` exists
/// — where **a 404 is the affirmative answer**, the cheap proof that the tail has
/// not moved, and the whole reason the read path does not LIST per fetch.
///
/// This pins it: a replica whose index is exactly right, reading a partition
/// nothing has retired, still takes segment 404s. No reconciliation reduces them,
/// because reducing them would mean not asking. Hence `caller` on
/// `tansu_prefix_segment_absent` — without it the acceptance is measured against
/// a floor it cannot reach.
#[tokio::test]
async fn a_segment_404_is_not_always_a_stale_index_entry() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let tallies = Tallies::wrapping(&bucket);
    let counts = Counts::of(&tallies);
    let storage = new_store(tallies);

    create_topic(&storage, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..4 {
        _ = storage.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }

    // One replica, one bucket, nothing retired: every entry this index holds
    // names an object that is there. Read it back so the index is warm and
    // provably accurate.
    let fetched = fetch_from(&storage, &tp, 0).await?;
    assert_eq!(4, fetched.len(), "the index is right and the read is whole");

    counts.reset();

    // Steady state: one more produce and the read that follows it. The flush's
    // fold-before-claim probes the tail before claiming a sequence, and the probe
    // proves it by reading `cursor + 1` and taking a 404. Nothing here is stale —
    // one replica, one bucket, nothing retired — so every one of these is the
    // `path="forced"` population the fleet runs at 10.6/s.
    _ = storage.produce(None, &tp, batch("v4")?).await?;
    assert_eq!(5, fetch_from(&storage, &tp, 0).await?.len());

    assert!(
        counts.absent() > 0,
        "a correct index still takes segment 404s — the tail probe's proof of \
         absence — so the `segment` 404 plane has a floor that reconciliation \
         cannot reach",
    );

    Ok(())
}
