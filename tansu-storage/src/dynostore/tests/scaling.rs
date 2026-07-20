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
use std::time::Duration;
use tansu_sans_io::{
    IsolationLevel,
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
