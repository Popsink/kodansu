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

//! Scaling characterisation (Test A): asserts the *object-store op profile* of
//! the topic-metadata paths so the algorithmic complexity is pinned, not just
//! timed. Run under nextest like any test.

use std::{
    collections::BTreeSet,
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use std::time::{Duration, SystemTime};
use tansu_sans_io::{
    IsolationLevel, ListOffset,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage, Topition,
    dynostore::{CoalesceTuning, DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

#[derive(Debug, Default)]
struct Counters {
    put: AtomicU64,
    get: AtomicU64,
    /// Unbounded full-prefix `list` calls (the cold-scan cost).
    list_calls: AtomicU64,
    /// Bounded `list_with_offset` calls (S3 `start-after`, scans only forward).
    list_with_offset_calls: AtomicU64,
    listed_objects: AtomicU64,
    /// Requests whose path lies under a legacy `records/` prefix, whatever the
    /// method. #179's acceptance is that this reaches zero: once no writer can
    /// create a `records/` object, every read-path branch that asks "is there a
    /// legacy region, and where does it end?" is asking a question with one
    /// possible answer, and the requests spent asking it are pure residual cost.
    records_requests: AtomicU64,
}

impl Counters {
    fn reset(&self) {
        self.put.store(0, Relaxed);
        self.get.store(0, Relaxed);
        self.list_calls.store(0, Relaxed);
        self.list_with_offset_calls.store(0, Relaxed);
        self.listed_objects.store(0, Relaxed);
        self.records_requests.store(0, Relaxed);
    }

    /// Requests touching a legacy `records/` prefix since the last reset.
    /// Deliberately not folded into `report`'s tuple so the existing op profiles
    /// keep their shape.
    fn records_requests(&self) -> u64 {
        self.records_requests.load(Relaxed)
    }

    fn report(&self, label: &str) -> (u64, u64, u64, u64, u64) {
        let v = (
            self.put.load(Relaxed),
            self.get.load(Relaxed),
            self.list_calls.load(Relaxed),
            self.list_with_offset_calls.load(Relaxed),
            self.listed_objects.load(Relaxed),
        );
        eprintln!(
            "[{label}] put={} get={} list={} list_with_offset={} listed_objects={}",
            v.0, v.1, v.2, v.3, v.4
        );
        v
    }
}

/// `ObjectStore` decorator counting puts, gets, list calls and the number of
/// objects yielded by listings (the cost proxy for a full prefix scan).
#[derive(Clone)]
struct Counting<O> {
    inner: O,
    counters: Arc<Counters>,
}

impl<O> Debug for Counting<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Counting").finish()
    }
}

impl<O> Display for Counting<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Counting").finish()
    }
}

/// Does `path` address the legacy per-`(topic, partition)` `records/` layout?
fn is_records_path(path: Option<&Path>) -> bool {
    path.is_some_and(|path| {
        path.as_ref().contains("/records/") || path.as_ref().ends_with("/records")
    })
}

#[async_trait]
impl<O> ObjectStore for Counting<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        _ = self.counters.put.fetch_add(1, Relaxed);
        if is_records_path(Some(location)) {
            _ = self.counters.records_requests.fetch_add(1, Relaxed);
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        _ = self.counters.get.fetch_add(1, Relaxed);
        if is_records_path(Some(location)) {
            _ = self.counters.records_requests.fetch_add(1, Relaxed);
        }
        self.inner.get_opts(location, options).await
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
        _ = self.counters.list_calls.fetch_add(1, Relaxed);
        if is_records_path(prefix) {
            _ = self.counters.records_requests.fetch_add(1, Relaxed);
        }
        let counters = self.counters.clone();
        self.inner
            .list(prefix)
            .inspect(move |_| {
                _ = counters.listed_objects.fetch_add(1, Relaxed);
            })
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        _ = self.counters.list_with_offset_calls.fetch_add(1, Relaxed);
        if is_records_path(prefix) {
            _ = self.counters.records_requests.fetch_add(1, Relaxed);
        }
        let counters = self.counters.clone();
        self.inner
            .list_with_offset(prefix, offset)
            .inspect(move |_| {
                _ = counters.listed_objects.fetch_add(1, Relaxed);
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        _ = self.counters.list_calls.fetch_add(1, Relaxed);
        if is_records_path(prefix) {
            _ = self.counters.records_requests.fetch_add(1, Relaxed);
        }
        let result = self.inner.list_with_delimiter(prefix).await?;
        _ = self
            .counters
            .listed_objects
            .fetch_add(result.objects.len() as u64, Relaxed);
        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

fn store() -> (DynoStore, Arc<Counters>) {
    let counters = Arc::new(Counters::default());
    let store = Counting {
        inner: InMemory::new(),
        counters: counters.clone(),
    };
    (DynoStore::new(CLUSTER, NODE, store), counters)
}

/// The same counting store in prefix-coalesced segment mode under the leaseless
/// arbiter (#57/#86) — the layout the epic (#171) is making the only one.
///
/// `coalesce_batches: Some(1)` flushes on every enqueue, so an awaited produce
/// returns without parking on the linger. That keeps the op-profile tests
/// written as straight-line sequential produces, which is what they measure.
fn segment_store() -> (DynoStore, Arc<Counters>) {
    let counters = Arc::new(Counters::default());
    let store = Counting {
        inner: InMemory::new(),
        counters: counters.clone(),
    };
    (
        DynoStore::new(CLUSTER, NODE, store).coalesce_tuning(CoalesceTuning {
            coalesce_batches: Some(1),
            ..Default::default()
        }),
        counters,
    )
}

/// A segment-mode counting store over an EXISTING bucket, so a second store can
/// read the same objects with cold in-process memos — which is where the residual
/// `records/` probing actually shows up.
fn segment_store_on(bucket: InMemory) -> (DynoStore, Arc<Counters>) {
    let counters = Arc::new(Counters::default());
    let store = Counting {
        inner: bucket,
        counters: counters.clone(),
    };
    (
        DynoStore::new(CLUSTER, NODE, store).coalesce_tuning(CoalesceTuning {
            coalesce_batches: Some(1),
            ..Default::default()
        }),
        counters,
    )
}

async fn create(storage: &DynoStore, name: &str, partitions: i32) -> Result<()> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;
    Ok(())
}

/// Creating a topic costs the same whether the cluster has 1 or 1000 topics —
/// it writes only the topic's own objects, never a monolith that grows with the
/// topic count.
#[tokio::test]
async fn create_topic_is_constant_cost() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let (storage, counters) = store();

    // First create.
    create(&storage, "topic-00000", 1).await?;
    let first = counters.report("create #1");

    // 1000 more topics exist now.
    for n in 1..=1000 {
        create(&storage, &format!("topic-{n:05}"), 1).await?;
    }

    // Cost of the next create must equal the first (no per-existing-topic work).
    counters.reset();
    create(&storage, "topic-99999", 1).await?;
    let nth = counters.report("create #1002");

    assert_eq!(first, nth, "create cost must be independent of topic count");
    // And it never lists the cluster (no monolith read/scan on the create path).
    assert_eq!(0, nth.2, "create must not LIST");
    Ok(())
}

/// Repeated list-all metadata is served from the in-memory index: within the
/// TTL it hits the object store zero times, and a refresh is one LIST plus GETs
/// only for objects whose etag changed (zero on a stable topic set) — never a
/// GET per topic per request (the #29 OOM at scale).
#[tokio::test]
async fn list_all_metadata_is_cached() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let (storage, counters) = store();

    for n in 0..200 {
        create(&storage, &format!("topic-{n:05}"), 1).await?;
    }

    // Warm the index (first refresh: 1 LIST + a GET per topic).
    _ = storage.metadata(None).await?;
    let warm = counters.report("list-all warm (first refresh)");
    assert_eq!(1, warm.2, "first refresh is a single LIST");
    assert_eq!(200, warm.1, "first refresh GETs each topic object once");

    // Repeated calls within the TTL touch the object store zero times.
    counters.reset();
    for _ in 0..50 {
        _ = storage.metadata(None).await?;
    }
    let cached = counters.report("list-all x50 cached");
    assert_eq!(
        (0, 0, 0, 0, 0),
        cached,
        "cached list-all must not hit the object store"
    );
    Ok(())
}

fn batch(value: &[u8]) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(bytes::Bytes::copy_from_slice(value))))
        .last_offset_delta(0)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Steady-state (warm) produce assigns offsets from the in-memory hint and just
/// creates the batch object — no listing of the growing partition.
#[tokio::test]
async fn warm_produce_does_not_list() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let (storage, counters) = store();

    create(&storage, "hot", 1).await?;
    let topition = Topition::new("hot", 0);

    // First produce warms the offset hint.
    _ = storage.produce(None, &topition, batch(b"warmup")?).await?;

    counters.reset();
    const PRODUCES: usize = 50;
    for n in 0..PRODUCES {
        _ = storage
            .produce(None, &topition, batch(format!("m-{n}").as_bytes())?)
            .await?;
    }
    let produced = counters.report("50 warm produces");

    assert_eq!(
        0, produced.2,
        "warm produce must not full-list the partition"
    );
    assert_eq!(0, produced.3, "warm produce needs no tail scan");
    assert_eq!(
        PRODUCES as u64, produced.0,
        "one batch object create per produce"
    );
    Ok(())
}

/// A caught-up consumer long-polling an idle partition issues ZERO LISTs per
/// poll in steady state (#40): `high_watermark` is served from the warm hint
/// within its TTL (no tail `list_with_offset`), and the fetch seek is skipped
/// because `offset == high_watermark`. Only the first (cold) read lists.
#[tokio::test]
async fn caught_up_consumer_does_not_list_per_poll() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let (storage, counters) = store();

    create(&storage, "hot", 1).await?;
    let topition = Topition::new("hot", 0);
    const BATCHES: usize = 8;
    for n in 0..BATCHES {
        _ = storage
            .produce(None, &topition, batch(format!("m-{n}").as_bytes())?)
            .await?;
    }

    // Cold read once to reconcile the hint against a tail listing.
    assert_eq!(BATCHES as i64, storage.high_watermark(&topition).await?);

    // Steady state: 50 polls at the tail (offset == high watermark). No LISTs.
    counters.reset();
    const POLLS: usize = 50;
    for _ in 0..POLLS {
        let fetched = storage
            .fetch(
                &topition,
                BATCHES as i64,
                1,
                8 * 1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_secs(0),
            )
            .await?;
        assert!(fetched.is_empty(), "caught-up poll returns no batches");
    }
    let polled = counters.report("50 caught-up polls");

    assert_eq!(0, polled.2, "caught-up poll must not full-list");
    assert_eq!(
        0, polled.3,
        "caught-up poll must not list the tail (served from the warm hint)"
    );
    assert_eq!(0, polled.4, "caught-up poll lists no objects at all");

    Ok(())
}

/// The same zero-LIST idle-poll property on a **pure-segment** topic (#171).
///
/// `caught_up_consumer_does_not_list_per_poll` above pins it for the legacy
/// layout only, and the segment tail-probe tests
/// (`tail_probe_follows_a_peer_without_listing` and its floor-ahead twin) pin
/// the *writer* side. Nothing pins the reader side: what an idle consumer costs
/// per poll once segments are the only layout. Since #171 deletes the legacy
/// test with the layout it covers, leaving this unpinned would drop the
/// property silently — and an idle-consumer LIST is charged per poll per
/// topic-partition, which is exactly the tier-1 cost the epic exists to remove.
#[tokio::test]
async fn caught_up_consumer_does_not_list_per_poll_on_segments() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let (storage, counters) = segment_store();

    // Dotted name: `prefix_of` coalesces on the first three components.
    let topic = "org.env.conn.hot";
    create(&storage, topic, 1).await?;
    let topition = Topition::new(topic, 0);
    const BATCHES: usize = 8;
    for n in 0..BATCHES {
        _ = storage
            .produce(None, &topition, batch(format!("m-{n}").as_bytes())?)
            .await?;
    }

    // Cold read once to warm the prefix index from the footers.
    assert_eq!(BATCHES as i64, storage.high_watermark(&topition).await?);

    // Steady state: 50 polls at the tail (offset == high watermark). No LISTs.
    counters.reset();
    const POLLS: usize = 50;
    for _ in 0..POLLS {
        let fetched = storage
            .fetch(
                &topition,
                BATCHES as i64,
                1,
                8 * 1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_secs(0),
            )
            .await?;
        assert!(fetched.is_empty(), "caught-up poll returns no batches");
    }
    let polled = counters.report("50 caught-up polls (segments)");

    assert_eq!(0, polled.2, "caught-up poll must not full-list");
    assert_eq!(
        0, polled.3,
        "caught-up poll must not list the tail (served from the warm hint)"
    );
    assert_eq!(0, polled.4, "caught-up poll lists no objects at all");

    Ok(())
}

/// A caught-up consumer long-polling an idle partition serves the high watermark
/// from the fresh in-memory hint, issuing ZERO object-store requests per poll:
/// neither the tail listing (#40) nor the persisted `watermark.json` GET (#72).
/// Before #72 the `persisted_high` GET ran ahead of the fresh-hint check, so
/// every poll cost one GET; steady-state fetch cost is now nil.
#[tokio::test]
async fn warm_high_watermark_polls_hit_object_store_zero_times() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let (storage, counters) = store();

    create(&storage, "hot", 1).await?;
    let topition = Topition::new("hot", 0);
    for n in 0..4 {
        _ = storage
            .produce(None, &topition, batch(format!("m-{n}").as_bytes())?)
            .await?;
    }

    // One read reconciles against a listing and warms the hint (sets listed_at).
    assert_eq!(4, storage.high_watermark(&topition).await?);

    // Steady-state long-poll: every read within the TTL is served from the hint
    // with no object-store traffic at all.
    counters.reset();
    for _ in 0..50 {
        assert_eq!(4, storage.high_watermark(&topition).await?);
    }
    let warm = counters.report("50 warm high_watermark polls");

    assert_eq!(
        (0, 0, 0, 0, 0),
        warm,
        "a fresh-hint high watermark must issue no GET (#72) and no LIST (#40)"
    );

    Ok(())
}

/// The read-uncommitted fetch response resolves its offsets from
/// `offset_stage_at`, which for read-uncommitted serves the high watermark from
/// the fresh hint and the log start from the cached watermark — **never reading
/// the cluster-wide `meta.json` object** (#109). So a caught-up consumer
/// long-polling an idle partition issues ZERO object-store requests per poll for
/// its response metadata. Before #109 the fetch service always called the full,
/// meta-reading `offset_stage`, adding a `meta.json` GET (a single hot key, an
/// S3 request ceiling at consumer-fan-out scale) plus a `watermark.json` GET to
/// every poll.
#[tokio::test]
async fn warm_read_uncommitted_offset_stage_hits_object_store_zero_times() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let (storage, counters) = store();

    create(&storage, "hot", 1).await?;
    let topition = Topition::new("hot", 0);
    for n in 0..4 {
        _ = storage
            .produce(None, &topition, batch(format!("m-{n}").as_bytes())?)
            .await?;
    }

    // Warm the high-watermark hint once (a real fetch does this via the produce
    // and the first read); this is the only step that touches the store.
    let warmed = storage
        .offset_stage_at(&topition, IsolationLevel::ReadUncommitted)
        .await?;
    assert_eq!(4, warmed.high_watermark());

    // Steady-state long-poll: every response-offset resolution within the TTL is
    // served from memory with no object-store traffic.
    counters.reset();
    for _ in 0..50 {
        let stage = storage
            .offset_stage_at(&topition, IsolationLevel::ReadUncommitted)
            .await?;
        assert_eq!(4, stage.high_watermark());
        // Read-uncommitted: last-stable == high watermark, no aborted list.
        assert_eq!(4, stage.last_stable());
        assert!(stage.aborted().is_empty());
    }
    let warm = counters.report("50 warm read-uncommitted offset_stage_at polls");

    assert_eq!(
        (0, 0, 0, 0, 0),
        warm,
        "read-uncommitted offset_stage_at must read neither meta.json nor watermark.json on a warm poll (#109)"
    );

    Ok(())
}

/// Consistency guard: read-uncommitted `offset_stage_at` reports the same high
/// watermark as the full `offset_stage`, and the read-committed variant still
/// funnels through the full path (surfacing transaction state). This pins that
/// #109 only *dropped the meta read for read-uncommitted*, without changing the
/// observed offsets on a non-transactional workload.
#[tokio::test]
async fn read_uncommitted_offset_stage_matches_full_stage() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let (storage, _counters) = store();

    create(&storage, "hot", 1).await?;
    let topition = Topition::new("hot", 0);
    for n in 0..7 {
        _ = storage
            .produce(None, &topition, batch(format!("m-{n}").as_bytes())?)
            .await?;
    }

    let full = storage.offset_stage(&topition).await?;
    let uncommitted = storage
        .offset_stage_at(&topition, IsolationLevel::ReadUncommitted)
        .await?;
    let committed = storage
        .offset_stage_at(&topition, IsolationLevel::ReadCommitted)
        .await?;

    assert_eq!(full.high_watermark(), uncommitted.high_watermark());
    assert_eq!(full.log_start(), uncommitted.log_start());
    // No open/aborted transactions on this workload, so last-stable == high.
    assert_eq!(full.high_watermark(), uncommitted.last_stable());
    // Read-committed goes through the full path unchanged.
    assert_eq!(full, committed);

    Ok(())
}

/// Reading a segment footer takes ONE ranged GET, not two (#112 follow-up): the
/// over-read suffix already holds the trailer and the (small) footer, so a cold
/// index build on a non-writer replica pays one GET per segment footer instead
/// of two — halving the residual footer-GET plane. Correctness unchanged: the
/// footer decodes and the segment is located.
#[tokio::test]
async fn cold_prefix_index_reads_one_get_per_small_footer() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let writer = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: bucket.clone(),
            counters: Arc::new(Counters::default()),
        },
    );
    create(&writer, "org.env.conn.hot", 1).await?;
    let topition = Topition::new("org.env.conn.hot", 0);
    _ = writer.produce(None, &topition, batch(b"m")?).await?;

    // Fresh reader (cold index) locates the segment: one full LIST of the
    // segments prefix + exactly ONE GET for the single small footer.
    let reader_counters = Arc::new(Counters::default());
    let reader = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: bucket.clone(),
            counters: reader_counters.clone(),
        },
    );

    let region_start = reader.segment_region_start(&topition).await?;
    let reads = reader_counters.report("cold segment_region_start (1 segment)");

    assert_eq!(
        Some(0),
        region_start,
        "the footer decoded from the over-read locates the segment at base offset 0"
    );
    // Two GETs: the single small footer, plus the memoized `topic_is_compacted`
    // read that resolves which prefix this sub-stream lives under. That second
    // one is the price of routing being unconditional — a compacted topic gets a
    // dedicated prefix, so the resolution can no longer be skipped. It is paid
    // once per topic per memo TTL, and the produce gate already pays it, so
    // steady state is unchanged; it shows up here because this is a deliberately
    // cold reader.
    //
    // The footer itself is still one GET, which is what #112's follow-up bought
    // and what this test exists to pin.
    assert_eq!(
        2, reads.1,
        "one GET for the footer, one to resolve the prefix"
    );

    Ok(())
}

/// The compacted-`cleanup.policy` check `produce` makes on every batch is
/// memoized (#113): the first check reads the topic config, and repeated checks
/// within the TTL are served from memory — no per-batch conditional GET of the
/// `topic-metadata/<name>.json` object on the produce hot path.
#[tokio::test]
async fn topic_is_compacted_is_memoized() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let (storage, counters) = store();
    create(&storage, "t", 1).await?;

    // First check reads the topic config once.
    assert!(!storage.topic_is_compacted("t").await?);

    // Repeated checks within the TTL hit the memo — no object-store requests.
    counters.reset();
    for _ in 0..20 {
        assert!(!storage.topic_is_compacted("t").await?);
    }
    let reads = counters.report("20 topic_is_compacted checks");
    assert_eq!(
        (0, 0, 0, 0, 0),
        reads,
        "the compacted-policy check is memoized — no per-call object-store request (#113)"
    );

    Ok(())
}

/// A pure-segment topic's LATEST `list_offsets` (the consumer-lag path) issues
/// no `records/` LIST per poll (#113): the tail timestamp comes from the footer
/// index (or is skipped when absent), never the legacy per-topic listing.
#[tokio::test]
async fn latest_list_offsets_on_pure_segment_issues_no_records_list() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let counters = Arc::new(Counters::default());
    let storage = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: InMemory::new(),
            counters: counters.clone(),
        },
    );
    create(&storage, "org.env.conn.hot", 1).await?;
    let tp = Topition::new("org.env.conn.hot", 0);
    _ = storage.produce(None, &tp, batch(b"m")?).await?;

    // Warm the index + the has-legacy-records memo with one LATEST query.
    _ = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(tp.clone(), ListOffset::Latest)],
        )
        .await?;

    // Repeated LATEST polls issue no `records/` LIST on a pure-segment topic.
    counters.reset();
    for _ in 0..20 {
        _ = storage
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(tp.clone(), ListOffset::Latest)],
            )
            .await?;
    }
    let reads = counters.report("20 LATEST list_offsets (pure-segment)");
    assert_eq!(
        (0, 0),
        (reads.2, reads.3),
        "no records/ LIST per LATEST poll on a pure-segment topic (#113)"
    );

    Ok(())
}

/// `has_legacy_records`'s negative memo is held for the LONG window only for the
/// provably-safe segment-routed case (prefix-coalesced + owns a segment), and
/// for the SHORT window otherwise; a local legacy write flips it to present at
/// once (#110). Grouped into one test so the shared helpers stay local.
#[tokio::test]
async fn legacy_records_presence_memo_ttls_and_write_through() -> Result<(), Error> {
    let _guard = init_tracing()?;

    /// The memoized TTL for `topition`'s legacy-presence entry, if any.
    fn memo_ttl(storage: &DynoStore, topition: &Topition) -> Option<Duration> {
        storage
            .legacy_records_present
            .lock()
            .unwrap()
            .get(topition)
            .map(|(_, _, ttl)| *ttl)
    }

    let coalesce_store = || {
        let counters = Arc::new(Counters::default());
        let storage = DynoStore::new(
            CLUSTER,
            NODE,
            Counting {
                inner: InMemory::new(),
                counters: counters.clone(),
            },
        );
        (storage, counters)
    };

    // A segment-routed topition (prefix-coalesced, owns a segment) with no legacy
    // `records/` objects memoizes its negative result for the LONG window (#110),
    // so it is not re-listed every few seconds — the dominant residual
    // per-topition LIST the topic→prefix collapse had left behind. And a poll
    // within the window is served from memory (no LIST).
    {
        // Writer produces so the sub-stream owns a segment; a fresh reader (cold
        // memo, separate process) then observes the segment-routed empty
        // topition. (On the writer the flush's own `leaseless_base` warms the
        // memo before the segment exists, so a fresh reader is what exercises the
        // segment-present branch deterministically.)
        let bucket = InMemory::new();
        let writer = DynoStore::new(
            CLUSTER,
            NODE,
            Counting {
                inner: bucket.clone(),
                counters: Arc::new(Counters::default()),
            },
        );
        create(&writer, "org.env.conn.hot", 1).await?;
        let topition = Topition::new("org.env.conn.hot", 0);
        for n in 0..4 {
            _ = writer
                .produce(None, &topition, batch(format!("m-{n}").as_bytes())?)
                .await?;
        }

        let reader_counters = Arc::new(Counters::default());
        let reader = DynoStore::new(
            CLUSTER,
            NODE,
            Counting {
                inner: bucket.clone(),
                counters: reader_counters.clone(),
            },
        );

        assert!(!reader.has_legacy_records(&topition).await?);
        assert_eq!(
            Some(DynoStore::LEGACY_ABSENCE_TTL),
            memo_ttl(&reader, &topition),
            "a segment-routed empty topition memoizes its negative for the long window"
        );

        reader_counters.reset();
        for _ in 0..50 {
            assert!(!reader.has_legacy_records(&topition).await?);
        }
        let warm = reader_counters.report("50 warm has_legacy_records polls (segment-routed)");
        assert_eq!(
            (0, 0),
            (warm.2, warm.3),
            "a memoized negative is served from memory — no LIST per poll (#110)"
        );
    }

    // Contrast: a coalesced topition that is NOT yet segment-routed gets the
    // MIDDLE window (#166), not the seconds-long one this test originally
    // asserted. Confining the long window to already-segmented sub-streams left
    // every idle stream — and every partition with a cold memo after a restart —
    // re-listing `records/` per poll, which measured as the dominant tier-1
    // request source in production. It is still not given the full hour: this is
    // the case we have the least evidence about, and the failure it bounds (a
    // stream that reads as empty) is more visible than a lagging tail.
    {
        let (storage, _counters) = coalesce_store();
        create(&storage, "org.env.conn.cold", 1).await?;
        let topition = Topition::new("org.env.conn.cold", 0);

        assert!(!storage.has_legacy_records(&topition).await?);
        assert_eq!(
            Some(DynoStore::LEGACY_ABSENCE_UNSEGMENTED_TTL),
            memo_ttl(&storage, &topition),
            "a coalesced topition without a segment holds its negative for minutes"
        );
    }

    Ok(())
}

/// A wide stale-hint `ListOffsets` sweep over prefix-coalesced topics — the
/// `endOffsets(assignment)` shape over ~1500 mostly-idle CDC topics — costs
/// O(prefixes), not O(partitions), in object-store requests: one incremental
/// `segments/` LIST plus one `seq-floor.json` GET per prefix, and **zero**
/// per-partition `watermark.json` conditional GETs. Before this fast path the
/// stale-hint LATEST resolution paid one `watermark.json` round-trip per
/// partition (`persisted_high`), which at ~1500 partitions blew the client
/// timeout even after the resolution was parallelized 32-way.
#[tokio::test]
async fn coalesced_stale_hint_list_offsets_is_per_prefix_not_per_partition() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const PREFIXES: usize = 2;
    const TOPICS_PER_PREFIX: usize = 8;

    let counters = Arc::new(Counters::default());
    let storage = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: InMemory::new(),
            counters: counters.clone(),
        },
    );

    let topic_name = |p: usize, i: usize| format!("org.env.conn{p}.table{i:02}");
    // Distinct per topic so a response attributed to the wrong partition
    // cannot accidentally carry the right high watermark.
    let records_in = |p: usize, i: usize| ((p * TOPICS_PER_PREFIX + i) % 5) as i64 + 1;

    let mut latest = Vec::new();
    for p in 0..PREFIXES {
        for i in 0..TOPICS_PER_PREFIX {
            let name = topic_name(p, i);
            create(&storage, &name, 1).await?;
            let tp = Topition::new(name.as_str(), 0);
            for n in 0..records_in(p, i) {
                _ = storage
                    .produce(None, &tp, batch(format!("m-{n}").as_bytes())?)
                    .await?;
            }
            latest.push((tp, ListOffset::Latest));
        }
    }
    let earliest: Vec<_> = latest
        .iter()
        .map(|(tp, _)| (tp.clone(), ListOffset::Earliest))
        .collect();

    let expected_high = |tp: &Topition| {
        let topic = tp.topic();
        let p: usize = topic["org.env.conn".len()..topic.rfind('.').unwrap()]
            .parse()
            .unwrap();
        let i: usize = topic[topic.rfind("table").unwrap() + "table".len()..]
            .parse()
            .unwrap();
        records_in(p, i)
    };

    // First sweep: warms the per-partition watermark-floor cache (one
    // `watermark.json` GET each, exactly as before) and the per-prefix
    // certified seq floor, and asserts correctness cold.
    let responses = storage
        .list_offsets(IsolationLevel::ReadUncommitted, &latest)
        .await?;
    assert_eq!(latest.len(), responses.len());
    for (tp, response) in &responses {
        assert_eq!(
            Some(expected_high(tp)),
            response.offset,
            "cold LATEST: {tp:?}"
        );
    }

    // Age every hint and index clock past the TTL without sleeping: the next
    // sweep takes the stale-hint path, the one that used to pay the
    // per-partition GET.
    let age = |storage: &DynoStore| {
        let past = SystemTime::now() - Duration::from_secs(60);
        for hint in storage
            .next_offsets
            .lock()
            .expect("next_offsets lock")
            .values_mut()
        {
            hint.listed_at = Some(past);
        }
        for entry in storage
            .prefix_index
            .lock()
            .expect("prefix_index lock")
            .values_mut()
        {
            entry.refreshed_at = Some(past);
        }
    };
    age(&storage);

    counters.reset();
    let responses = storage
        .list_offsets(IsolationLevel::ReadUncommitted, &latest)
        .await?;
    assert_eq!(latest.len(), responses.len());
    for (tp, response) in &responses {
        assert_eq!(
            Some(expected_high(tp)),
            response.offset,
            "stale-hint LATEST: {tp:?}"
        );
    }
    let stale = counters.report("stale-hint LATEST sweep (16 partitions, 2 prefixes)");
    assert_eq!(0, stale.0, "no puts");
    assert_eq!(0, stale.2, "no full LIST");
    assert_eq!(
        0, stale.3,
        "no incremental segments LIST either: the tail probe proves the tail instead (#112)"
    );
    assert_eq!(
        2 * PREFIXES as u64,
        stale.1,
        "two tier-2 GETs per PREFIX — the tail probe and the seq-floor read that proves it — \
         replacing the tier-1 LIST, and still zero per-partition watermark.json GETs"
    );

    // EARLIEST over the whole assignment within the index TTL: served entirely
    // from the footer index (log start == oldest segment base), zero requests.
    counters.reset();
    let responses = storage
        .list_offsets(IsolationLevel::ReadUncommitted, &earliest)
        .await?;
    assert_eq!(earliest.len(), responses.len());
    for (tp, response) in &responses {
        assert_eq!(Some(0), response.offset, "EARLIEST: {tp:?}");
    }
    let warm = counters.report("EARLIEST sweep within TTL");
    assert_eq!(
        (0, 0, 0, 0, 0),
        warm,
        "EARLIEST must resolve from the in-memory segment index"
    );

    // Freshness is not traded away: a produce after the sweep advances LATEST
    // through the same fast path on the next stale-hint read.
    let hot = Topition::new(topic_name(0, 0).as_str(), 0);
    _ = storage.produce(None, &hot, batch(b"one-more")?).await?;
    age(&storage);
    let responses = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(hot.clone(), ListOffset::Latest)],
        )
        .await?;
    assert_eq!(
        Some(expected_high(&hot) + 1),
        responses[0].1.offset,
        "a post-sweep produce must be visible on the next stale-hint LATEST"
    );

    Ok(())
}

/// The floor-certified watermark cache never regresses LATEST below an offset
/// a peer replica acked and then expired. `watermark.high` of a coalesced
/// sub-stream only advances in `expire_prefix_segments`, which then raises the
/// prefix seq floor write-ahead of the delete — so a floor rise is exactly the
/// signal that a cached `watermark.json` value may be stale, and the fast path
/// steps aside for one re-GET. A floor that has NOT risen certifies the cache
/// and the stale-hint LATEST costs zero per-partition requests.
#[tokio::test]
async fn coalesced_latest_survives_peer_expiry_via_floor_certification() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let counters = Arc::new(Counters::default());
    let storage = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: bucket.clone(),
            counters: counters.clone(),
        },
    );

    let topic = "org.env.conn.expired";
    create(&storage, topic, 1).await?;
    let tp = Topition::new(topic, 0);
    for n in 0..4 {
        _ = storage
            .produce(None, &tp, batch(format!("m-{n}").as_bytes())?)
            .await?;
    }
    let prefix = storage.prefix_of(&tp);

    // Warm the caches (pays the one watermark GET), then age the clocks: the
    // stale-hint read serves LATEST from the index + certified cache with no
    // per-partition request — only the per-prefix LIST + floor GET remain.
    assert_eq!(4, storage.high_watermark(&tp).await?);
    let age = |storage: &DynoStore| {
        let past = SystemTime::now() - Duration::from_secs(60);
        for hint in storage
            .next_offsets
            .lock()
            .expect("next_offsets lock")
            .values_mut()
        {
            hint.listed_at = Some(past);
        }
        for entry in storage
            .prefix_index
            .lock()
            .expect("prefix_index lock")
            .values_mut()
        {
            entry.refreshed_at = Some(past);
        }
    };
    age(&storage);
    counters.reset();
    assert_eq!(4, storage.high_watermark(&tp).await?);
    let certified = counters.report("stale-hint LATEST, floor unchanged");
    assert_eq!(
        (0, 2, 0, 0, 0),
        certified,
        "floor unchanged: the refresh is served by the tail probe (#112) — one probe GET plus \
         the floor GET that proves it, no LIST and no watermark.json GET"
    );

    // A peer replica acked offsets this process never listed (segments created
    // and expired within its blind window), exactly as `expire_prefix_segments`
    // would leave the store: `watermark.high` persisted to the acked tail, then
    // the seq floor raised write-ahead of the delete.
    storage
        .watermark(&tp)?
        .with_mut(&storage.object_store, |watermark| {
            watermark.high = Some(100);
            Ok(())
        })
        .await?;
    storage.raise_seq_floor(&prefix, 1_000).await?;

    // The floor rise invalidates the cached watermark: the next stale-hint
    // read re-GETs `watermark.json` once and reports the peer-acked high —
    // LATEST never regresses below an acked offset.
    age(&storage);
    assert_eq!(
        100,
        storage.high_watermark(&tp).await?,
        "a floor rise must force the watermark re-read"
    );

    // A cold replica (empty caches, same bucket) converges to the same answer:
    // its index tail (4) never wins over the persisted floor (100).
    let cold = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: bucket.clone(),
            counters: Arc::new(Counters::default()),
        },
    );
    assert_eq!(
        100,
        cold.high_watermark(&tp).await?,
        "a cold replica must serve the peer-acked high, not the surviving segment tail"
    );

    Ok(())
}

/// A sub-stream holding **no segment** — never produced, or drained by retention
/// — resolves stale-hint LATEST from the floor-certified cache, at zero
/// per-partition object-store requests (#167).
///
/// Both cases used to be excluded: `coalesced_high_from_index` returned `None`
/// whenever the sub-stream owned no segment, so every poll paid its own
/// `watermark.json` round trip. Measured at ~660/s on the production fleet — a
/// third of a 304 plane that is ~90% of metered GETs — and the population only
/// grows, since retention moves partitions into it and nothing moves them out.
/// The exclusion was there for lake-sink topics, whose high advanced *without*
/// raising the seq floor; that engine went with the lakehouse (#96), leaving
/// `expire_prefix_segments` as the only writer of the field, and it raises the
/// floor write-ahead of its deletes.
///
/// Counted across the etag memo's window, which is what production polling
/// crosses: within one 5s window the memo answers the revalidation locally, so
/// the cost is one request either way and only wall-clock hours separate the two
/// versions. `age` therefore expires the memo alongside the hint and index
/// clocks, and each poll below is a fresh window: 8 polls used to cost 8
/// `watermark.json` requests, and now cost one.
#[tokio::test]
async fn segmentless_substream_stale_hint_latest_costs_no_per_partition_get() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let watermark_gets = Arc::new(AtomicU64::new(0));
    let counters = Arc::new(Counters::default());
    let storage = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: CountWatermarkGets {
                inner: InMemory::new(),
                gets: watermark_gets.clone(),
            },
            counters: counters.clone(),
        },
    );

    // Age every clock a poll consults — the offset hint, the prefix index, and the
    // metadata etag memo — so the next call takes the stale-hint path with nothing
    // memoized, which is the state a production poll arrives in once per window.
    let age = |storage: &DynoStore| {
        let past = SystemTime::now() - Duration::from_secs(60);
        for hint in storage
            .next_offsets
            .lock()
            .expect("next_offsets lock")
            .values_mut()
        {
            hint.listed_at = Some(past);
        }
        for entry in storage
            .prefix_index
            .lock()
            .expect("prefix_index lock")
            .values_mut()
        {
            entry.refreshed_at = Some(past);
        }
        storage.expire_metadata_etags();
    };

    // A topic created and never produced to — the shape of an idle CDC table whose
    // rows have not changed.
    let empty_topic = "org.env.empty.tab";
    create(&storage, empty_topic, 1).await?;
    let empty = Topition::new(empty_topic, 0);

    watermark_gets.store(0, Relaxed);
    for _ in 0..8 {
        age(&storage);
        assert_eq!(
            0,
            storage.high_watermark(&empty).await?,
            "an empty sub-stream's LATEST is 0"
        );
    }
    assert_eq!(
        1,
        watermark_gets.load(Relaxed),
        "8 stale-hint polls of a never-produced partition must cost ONE watermark.json \
         request, not one each (#167)"
    );

    // The drained case: produced, then retention took every segment the sub-stream
    // owned. `expire_prefix_segments` persists each affected tail into
    // `watermark.high`, raises the seq floor write-ahead, then deletes.
    // Two partitions so the remaining cost can be shown to be per *prefix*: that
    // is the whole claim, since a prefix carries thousands of partitions.
    let topic = "org.env.drained.tab";
    create(&storage, topic, 2).await?;
    let a = Topition::new(topic, 0);
    let b = Topition::new(topic, 1);
    for n in 0..4 {
        _ = storage
            .produce(None, &a, batch(format!("a-{n}").as_bytes())?)
            .await?;
    }
    for n in 0..2 {
        _ = storage
            .produce(None, &b, batch(format!("b-{n}").as_bytes())?)
            .await?;
    }
    let prefix = storage.prefix_of(&a);
    assert_eq!(4, storage.high_watermark(&a).await?);
    assert_eq!(2, storage.high_watermark(&b).await?);

    // A threshold in the future expires regardless of record age; one segment per
    // awaited produce, so every segment of the prefix goes.
    let threshold = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|since| since.as_millis() as i64 + 60_000)
        .expect("clock before the epoch");
    assert_eq!(6, storage.expire_prefix_segments(&prefix, threshold).await?);
    for tp in [&a, &b] {
        assert!(
            storage
                .valid_substream_segments(&prefix, topic, tp.partition())?
                .is_empty(),
            "{tp:?} must hold no segment for this to be the drained case"
        );
    }

    // The drain raised the floor, which invalidates the cached watermark: the first
    // stale-hint read of each partition after it re-reads that partition's object
    // once and reports the persisted high rather than regressing to 0. The
    // watermark cache is per partition, so this is one re-read each — bounded by
    // how often a floor moves, which is a maintenance event, not a poll.
    age(&storage);
    watermark_gets.store(0, Relaxed);
    assert_eq!(4, storage.high_watermark(&a).await?);
    assert_eq!(2, storage.high_watermark(&b).await?);
    assert_eq!(
        2,
        watermark_gets.load(Relaxed),
        "a floor rise must force exactly one watermark re-read per partition"
    );

    // Every stale-hint poll after that is served from the certified cache, and
    // LATEST never regresses to 0 — the persisted high is these sub-streams' only
    // authority.
    const WINDOWS: u64 = 8;
    watermark_gets.store(0, Relaxed);
    counters.reset();
    for _ in 0..WINDOWS {
        age(&storage);
        assert_eq!(4, storage.high_watermark(&a).await?);
        assert_eq!(2, storage.high_watermark(&b).await?);
    }
    assert_eq!(
        0,
        watermark_gets.load(Relaxed),
        "16 stale-hint polls of drained sub-streams must not GET watermark.json (#167)"
    );
    let polls = counters.report("8 windows x 2 drained partitions, stale-hint LATEST");
    assert_eq!(0, polls.0, "no puts");
    assert_eq!(
        (WINDOWS, WINDOWS),
        (polls.2, polls.1),
        "what remains is per prefix per window — one segments LIST and the seq-floor \
         GET that certifies it — and does not grow with the partition count"
    );

    // Freshness is not traded away. A produce lands above the drained floor and is
    // visible on the next stale-hint read …
    assert_eq!(4, storage.produce(None, &a, batch(b"after-drain")?).await?);
    age(&storage);
    assert_eq!(5, storage.high_watermark(&a).await?);

    // … and a peer's expiry — `watermark.high` advanced, then the floor raised —
    // still invalidates the cache and is picked up, never regressing LATEST.
    storage
        .watermark(&a)?
        .with_mut(&storage.object_store, |watermark| {
            watermark.high = Some(100);
            Ok(())
        })
        .await?;
    storage.raise_seq_floor(&prefix, 1_000).await?;
    age(&storage);
    assert_eq!(
        100,
        storage.high_watermark(&a).await?,
        "a peer-acked high must survive being served from the certified cache"
    );

    Ok(())
}

/// #165: every listing this engine issues must go through the attributed helpers
/// (`DynoStore::scan` / `scan_from` / `scan_delimited`), so the tier-1 plane can be
/// broken down by call site. The metric was blind for ~20 scan sites — reporting
/// 0.48 LIST/s against ~1,200/s metered — because `Metron` forwarded the two
/// streaming list methods uninstrumented, and every LIST reduction claimed from
/// that counter was therefore unverified.
///
/// A source-level guard is what catches the *next* direct call before it ships: an
/// operation test cannot, since an unattributed listing still returns the right
/// objects. Continuation lines are joined first — a builder-style
/// `self\n.object_store\n.list(..)` is the form that slipped through the first
/// conversion of this very issue, including the legacy probe #166 is about.
#[test]
fn every_listing_is_attributed_to_a_call_site() {
    let source = include_str!("../../dynostore.rs");

    // The attributed helpers, and the `Metron` wrapper that records the
    // per-method request metric, are the only places allowed to touch the raw
    // list methods.
    let allowed = BTreeSet::from([
        "scan",
        "scan_from",
        "scan_delimited",
        "list",
        "list_with_offset",
        "list_with_delimiter",
    ]);

    // Fold each `.method()` continuation onto the statement it belongs to, so a
    // call split across lines is seen as one.
    let mut statements: Vec<String> = Vec::new();
    for line in source.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with('.') && !statements.is_empty() {
            let last = statements.len() - 1;
            statements[last].push_str(trimmed);
        } else {
            statements.push(trimmed.to_owned());
        }
    }

    let mut enclosing = "<none>";
    let mut offenders = BTreeSet::new();

    for statement in &statements {
        if let Some((_, rest)) = statement.split_once("fn ")
            && let Some((name, _)) = rest.split_once('(')
        {
            enclosing = name;
        }

        let lists = statement.contains("object_store.list(")
            || statement.contains("object_store.list_with_offset(")
            || statement.contains("object_store.list_with_delimiter(");

        if lists && !allowed.contains(enclosing) {
            _ = offenders.insert(enclosing);
        }
    }

    assert!(
        offenders.is_empty(),
        "{offenders:?} list the object store directly — a listing that does not go through \
         DynoStore::scan/scan_from/scan_delimited is invisible to \
         tansu_object_store_list_scans (#165)"
    );
}

/// #166: a prefix-coalesced partition with no segment yet must not re-list
/// `records/` on the read path every few seconds. That short TTL applied to every
/// idle stream and to every partition whose memo was cold after a restart, which
/// made the legacy probe the dominant tier-1 request source in production
/// (~1,200 LIST/s metered, ~85% of the bill, stepping up on each consumer
/// restart). The write-through still has to win over the longer window, or a
/// legacy batch written here would be invisible to our own next read.
#[tokio::test]
async fn legacy_probe_is_not_relisted_for_a_segmentless_coalesced_partition() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let counters = Arc::new(Counters::default());
    let storage = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: InMemory::new(),
            counters: counters.clone(),
        },
    );

    let topic = "org.env.conn.idle";
    create(&storage, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    // No produce: the sub-stream owns no segment, which is the population that
    // used to re-list every `HIGH_WATERMARK_HINT_TTL`.
    counters.reset();
    assert!(!storage.has_legacy_records(&tp).await?);
    let first = counters.report("first legacy probe");
    assert!(
        first.2 >= 1,
        "the first probe lists records/ once: {first:?}"
    );

    // Repeated polls are served from the memo: no further listing at all.
    counters.reset();
    for _ in 0..8 {
        assert!(!storage.has_legacy_records(&tp).await?);
    }
    let repeated = counters.report("8 further legacy probes");
    assert_eq!(
        (0, 0, 0, 0, 0),
        repeated,
        "a segment-less coalesced partition must not re-list records/ per poll"
    );

    Ok(())
}

/// #166 companion: the graduated windows are ordered as intended — a segment-less
/// coalesced partition is held for minutes, a segment-routed one for the full
/// hour, and anything outside prefix coalescing keeps the seconds-long window
/// where a first legacy write is expected.
#[test]
fn legacy_absence_windows_are_graduated() {
    assert!(DynoStore::HIGH_WATERMARK_HINT_TTL < DynoStore::LEGACY_ABSENCE_UNSEGMENTED_TTL);
    assert!(DynoStore::LEGACY_ABSENCE_UNSEGMENTED_TTL < DynoStore::LEGACY_ABSENCE_TTL);
}

/// An `ObjectStore` counting GETs of the per-partition `watermark.json` object,
/// so a test can pin that a read path does not touch it (#161).
#[derive(Clone)]
struct CountWatermarkGets<O> {
    inner: O,
    gets: Arc<AtomicU64>,
}

impl<O> Debug for CountWatermarkGets<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountWatermarkGets").finish()
    }
}

impl<O> Display for CountWatermarkGets<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountWatermarkGets").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for CountWatermarkGets<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        if location.as_ref().ends_with("watermark.json") {
            _ = self.gets.fetch_add(1, Relaxed);
        }
        self.inner.get_opts(location, options).await
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
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
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
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// #161: read-committed polling of a pure-segment sub-stream must not GET
/// `watermark.json`. It is authoritatively silent about the log start there —
/// only the legacy `records/` retention paths ever advance `watermark.low` — and
/// polling it measured ~1490 GET/s returning `404 NoSuchKey` at ~1,600
/// subscriptions: round-trips that resolve nothing, add fetch latency, and are
/// billable on a store that charges 4xx. The reported offsets must not change.
#[tokio::test]
async fn read_committed_polling_does_not_get_watermark_json() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let gets = Arc::new(AtomicU64::new(0));
    let storage = DynoStore::new(
        CLUSTER,
        NODE,
        CountWatermarkGets {
            inner: InMemory::new(),
            gets: gets.clone(),
        },
    );

    let topic = "org.env.conn.committed";
    create(&storage, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    for n in 0..4 {
        _ = storage
            .produce(None, &tp, batch(format!("m-{n}").as_bytes())?)
            .await?;
    }

    // Read-uncommitted was already off this object (#109) — the baseline.
    let uncommitted = storage
        .offset_stage_at(&tp, IsolationLevel::ReadUncommitted)
        .await?;

    gets.store(0, Relaxed);

    for _ in 0..16 {
        let committed = storage
            .offset_stage_at(&tp, IsolationLevel::ReadCommitted)
            .await?;

        assert_eq!(4, committed.high_watermark);
        assert_eq!(4, committed.last_stable, "no open transaction");
        assert_eq!(
            uncommitted.log_start, committed.log_start,
            "the log start must not change with the isolation level"
        );
        assert!(committed.aborted.is_empty());
    }

    assert_eq!(
        0,
        gets.load(Relaxed),
        "16 read-committed polls must not GET watermark.json (#161)"
    );

    Ok(())
}

/// How much of a segment topic's read path is still spent asking about a legacy
/// `records/` region — the residual #179 removes.
///
/// Nothing writes that layout any more (#177/#178), so on a segment-only topic
/// every such request is asking a question with one possible answer. This pins the
/// cost of asking, for the whole read surface: `fetch`, `list_offsets` LATEST and
/// EARLIEST, and `offset_stage_at`.
///
/// The warm figure is the one that matters in production and is already zero — the
/// absence memo holds it there, and that assertion must survive #179 unchanged. The
/// cold figure is what #179 collects: **when that issue lands it becomes 0 too.**
/// Written as an equality rather than an upper bound so the number cannot drift up
/// unnoticed while the branch still exists.
#[tokio::test]
async fn a_segment_topic_read_path_still_probes_records_when_cold() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let topic = "org.env.conn.tab";
    let tp = Topition::new(topic, 0);

    {
        let (storage, _) = segment_store_on(bucket.clone());
        create(&storage, topic, 1).await?;
        for n in 0..3 {
            _ = storage
                .produce(None, &tp, batch(format!("m-{n}").as_bytes())?)
                .await?;
        }
    }

    // Cold: a fresh process over the same objects, every memo empty.
    let (storage, counters) = segment_store_on(bucket.clone());
    counters.reset();
    exercise_read_paths(&storage, &tp).await?;
    assert_eq!(
        1,
        counters.records_requests(),
        "cold read path spends this many requests on a legacy region that cannot exist; \
         #179 removes the branch and this becomes 0"
    );

    // Warm again on that same process: the memo holds, so repeating the whole read
    // surface costs nothing further. This assertion must survive #179 unchanged.
    counters.reset();
    exercise_read_paths(&storage, &tp).await?;
    assert_eq!(
        0,
        counters.records_requests(),
        "a warm read path must never re-probe records/"
    );

    Ok(())
}

/// Drive the whole read surface once: fetch, LATEST, EARLIEST, offset stage.
async fn exercise_read_paths(storage: &DynoStore, tp: &Topition) -> Result<()> {
    _ = storage
        .fetch(
            tp,
            0,
            1,
            64 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(500),
        )
        .await?;
    _ = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[
                (tp.clone(), ListOffset::Latest),
                (tp.clone(), ListOffset::Earliest),
            ],
        )
        .await?;
    _ = storage
        .offset_stage_at(tp, IsolationLevel::ReadUncommitted)
        .await?;
    Ok(())
}
