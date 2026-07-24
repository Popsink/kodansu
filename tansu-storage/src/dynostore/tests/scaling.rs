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
    dynostore::{DynoStore, tests::init_tracing},
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
}

impl Counters {
    fn reset(&self) {
        self.put.store(0, Relaxed);
        self.get.store(0, Relaxed);
        self.list_calls.store(0, Relaxed);
        self.list_with_offset_calls.store(0, Relaxed);
        self.listed_objects.store(0, Relaxed);
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

/// High-watermark never does a full prefix `list`: it always scans forward from
/// a floor via `list_with_offset` (S3 `start-after`). A cold reader whose floor
/// was persisted by an earlier reader scans *zero* batches — instead of the
/// whole partition (the cold-LIST storm at scale, the deferred #13 bottleneck).
#[tokio::test]
async fn cold_high_watermark_uses_persisted_floor() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();

    // Producer replica: create + produce many batches.
    let writer = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: bucket.clone(),
            counters: Arc::new(Counters::default()),
        },
    );
    create(&writer, "hot", 1).await?;
    let topition = Topition::new("hot", 0);
    const BATCHES: usize = 64;
    for n in 0..BATCHES {
        _ = writer
            .produce(None, &topition, batch(format!("value-{n}").as_bytes())?)
            .await?;
    }

    // First cold reader: no persisted floor yet, so it scans the batches once —
    // but via `list_with_offset`, never a full `list` — and persists the high.
    let first_counters = Arc::new(Counters::default());
    let first = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: bucket.clone(),
            counters: first_counters.clone(),
        },
    );
    assert_eq!(BATCHES as i64, first.high_watermark(&topition).await?);
    let first = first_counters.report("first cold high_watermark");
    assert_eq!(0, first.2, "must never do a full-prefix list");
    assert!(first.3 >= 1, "scans forward via list_with_offset");

    // Second cold reader (fresh process) now floors its scan at the persisted
    // watermark: it lists ZERO batch objects.
    let second_counters = Arc::new(Counters::default());
    let second = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: bucket.clone(),
            counters: second_counters.clone(),
        },
    );
    assert_eq!(BATCHES as i64, second.high_watermark(&topition).await?);
    let second = second_counters.report("second cold high_watermark (persisted floor)");
    assert_eq!(0, second.2, "must never do a full-prefix list");
    assert_eq!(
        0, second.4,
        "persisted floor must skip every batch object on a cold read"
    );

    Ok(())
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

/// A consumer fetch scans only from its offset forward (bounded `list_with_offset`,
/// never a full `list`) and reads only the batches in range — so fetch cost
/// tracks the bytes consumed, not the partition size.
#[tokio::test]
async fn consume_scans_only_from_offset() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let (storage, counters) = store();

    create(&storage, "hot", 1).await?;
    let topition = Topition::new("hot", 0);
    const BATCHES: usize = 64;
    for n in 0..BATCHES {
        _ = storage
            .produce(None, &topition, batch(format!("m-{n}").as_bytes())?)
            .await?;
    }

    // Consume the whole log from the start.
    counters.reset();
    let fetched = storage
        .fetch(
            &topition,
            0,
            1,
            8 * 1024 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_secs(5),
        )
        .await?;
    let consume = counters.report("fetch from offset 0");

    assert!(!fetched.is_empty(), "fetch returned batches");
    assert_eq!(0, consume.2, "fetch must not full-list the partition");
    assert!(
        consume.3 >= 1,
        "fetch scans the partition via bounded list_with_offset"
    );

    // Fetch from near the tail reads far fewer objects than from the start.
    counters.reset();
    _ = storage
        .fetch(
            &topition,
            (BATCHES - 2) as i64,
            1,
            8 * 1024 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_secs(5),
        )
        .await?;
    let tail = counters.report("fetch near tail");
    assert_eq!(0, tail.2, "fetch must not full-list the partition");
    assert!(
        tail.4 < consume.4,
        "fetching near the tail lists fewer objects than from the start"
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

/// A fresh reader (a consumer connecting to a replica that did not produce the
/// data) collapses repeated `high_watermark` reads to a single tail listing
/// within the TTL (#40): the first read lists the tail cold, the rest are served
/// from the now-warm hint. This is the path every read-uncommitted fetch,
/// `offset_stage`, and `list_offsets` LATEST funnels through.
#[tokio::test]
async fn repeated_high_watermark_on_fresh_reader_lists_once() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();

    // Producer replica writes the batches (separate process / hint).
    let writer = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: bucket.clone(),
            counters: Arc::new(Counters::default()),
        },
    );
    create(&writer, "hot", 1).await?;
    let topition = Topition::new("hot", 0);
    for n in 0..4 {
        _ = writer
            .produce(None, &topition, batch(format!("m-{n}").as_bytes())?)
            .await?;
    }

    // Fresh reader replica (empty hint) on the same bucket.
    let reader_counters = Arc::new(Counters::default());
    let reader = DynoStore::new(
        CLUSTER,
        NODE,
        Counting {
            inner: bucket.clone(),
            counters: reader_counters.clone(),
        },
    );

    for _ in 0..20 {
        assert_eq!(4, reader.high_watermark(&topition).await?);
    }
    let reads = reader_counters.report("20 high_watermark reads (fresh reader)");

    assert_eq!(0, reads.2, "high_watermark must not full-list");
    assert_eq!(
        1, reads.3,
        "first read lists the tail cold, the rest are served from the warm hint"
    );

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
    )
    .prefix_coalesce(true)
    .prefix_leaseless(true);
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
    )
    .prefix_coalesce(true)
    .prefix_leaseless(true);

    let region_start = reader.segment_region_start(&topition).await?;
    let reads = reader_counters.report("cold segment_region_start (1 segment)");

    assert_eq!(
        Some(0),
        region_start,
        "the footer decoded from the over-read locates the segment at base offset 0"
    );
    assert_eq!(
        1, reads.1,
        "one GET for the single small footer (was two before #112 follow-up)"
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
    )
    .prefix_coalesce(true)
    .prefix_leaseless(true);
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
        )
        .prefix_coalesce(true)
        .prefix_leaseless(true);
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
        )
        .prefix_coalesce(true)
        .prefix_leaseless(true);
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
        )
        .prefix_coalesce(true)
        .prefix_leaseless(true);

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

    // Contrast: a topition that is NOT yet segment-routed (no segment) keeps the
    // SHORT TTL, so a drain / first segment / legacy write is still picked up
    // within seconds — the long window is confined to the provably-safe case.
    {
        let (storage, _counters) = coalesce_store();
        create(&storage, "org.env.conn.cold", 1).await?;
        let topition = Topition::new("org.env.conn.cold", 0);

        assert!(!storage.has_legacy_records(&topition).await?);
        assert_eq!(
            Some(DynoStore::HIGH_WATERMARK_HINT_TTL),
            memo_ttl(&storage, &topition),
            "a topition without a segment keeps the short TTL"
        );
    }

    // A legacy `records/` write on this process flips the memo to `true` at once
    // (the write-through the produce path performs after an `assign_and_create`),
    // so a stale long-negative cannot hide a just-written legacy record: the next
    // read returns `true` from memory, no LIST.
    {
        let (storage, counters) = coalesce_store();
        create(&storage, "org.env.conn.hot", 1).await?;
        let topition = Topition::new("org.env.conn.hot", 0);
        for n in 0..4 {
            _ = storage
                .produce(None, &topition, batch(format!("m-{n}").as_bytes())?)
                .await?;
        }

        assert!(!storage.has_legacy_records(&topition).await?);
        storage.note_legacy_records_present(&topition)?;

        counters.reset();
        assert!(
            storage.has_legacy_records(&topition).await?,
            "a local legacy write must flip the memo to present"
        );
        let after = counters.report("has_legacy_records after write-through");
        assert_eq!(
            (0, 0),
            (after.2, after.3),
            "the flipped-present memo is served from memory, no LIST"
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
    )
    .prefix_coalesce(true)
    .prefix_leaseless(true);

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
        PREFIXES as u64, stale.3,
        "one single-flighted incremental segments LIST per PREFIX, not per partition"
    );
    assert_eq!(
        PREFIXES as u64, stale.1,
        "one seq-floor GET per PREFIX — and zero per-partition watermark.json GETs"
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
    )
    .prefix_coalesce(true)
    .prefix_leaseless(true);

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
        (0, 1, 0, 1, 0),
        certified,
        "floor unchanged: one per-prefix floor GET + one incremental LIST, no watermark.json GET"
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
    )
    .prefix_coalesce(true)
    .prefix_leaseless(true);
    assert_eq!(
        100,
        cold.high_watermark(&tp).await?,
        "a cold replica must serve the peer-acked high, not the surviving segment tail"
    );

    Ok(())
}
