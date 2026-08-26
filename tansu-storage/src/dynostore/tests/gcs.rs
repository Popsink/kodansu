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

//! What GCS costs that S3 does not, on a laptop, with no bucket.
//!
//! Two unrelated things live here because one URL scheme is what they have in
//! common, and neither had a test.
//!
//! **The read path is latency-multiplied, and that is not GCS-specific.**
//! `fetch_is_one_serial_round_trip_per_segment` measures the shape: a fetch is
//! one ranged GET per segment, awaited in sequence, and the two loops above it
//! in `service::fetch` walk partitions and topics in sequence too. Wall clock
//! is therefore `segments x partitions x topics x per-GET latency`, and GCS is
//! where that first crosses a client timeout because its per-GET latency is
//! higher. Latency is injected rather than real, so the linearity is observable
//! without a store.
//!
//! **The write cap is GCS-specific and only reaches one object.** `memory://`
//! is `InMemory` with nothing in front of it, so every other test in this crate
//! measures the S3 shape; the `gs` arm of `StorageContainer::builder` wraps the
//! store in [`PutRateLimiter`] at one put per second per object. That wrapper
//! is a *local* throttle — it delays rather than sending — so what it costs is
//! observable here too. `produce_fan_out_under_the_per_object_cap` shows the
//! data plane never writes one key twice and so never meets the cap;
//! `group_formation_under_the_per_object_cap` shows `generation.json` does.
//!
//! What none of this observes is GCS itself: the 429 body, the generation
//! precondition, the per-bucket write ramp. Those need a real bucket.
//!
//! `#[ignore]` throughout: these are wall clock, not regression gates.
//! `just test-gcs` runs them.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use object_store::{ObjectStore, PutOptions, PutPayload, memory::InMemory, path::Path};
use tokio::task::JoinSet;

use crate::{
    MemberRef, Result, Storage, UpdateError,
    dynostore::{DynoStore, tests::init_tracing},
    gcs::limit::PutRateLimiter,
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// Exactly what `StorageContainer::builder`'s `gs` arm wraps the store in.
fn gcs_shaped<O>(inner: O) -> PutRateLimiter<O> {
    PutRateLimiter::new(inner, Duration::from_mins(5))
        .with_rate_per_second(NonZeroU32::new(1))
        .with_jitter(Some(Duration::from_millis(50)))
}

/// `rate_per_second` is not a knob. Four puts to one key at a configured 4/s
/// take ~3s, not ~0.75s: `rate_limit` asks `until_n_ready_with_jitter` for
/// `rate_per_second` cells out of a bucket that refills at `rate_per_second`
/// per second, so every setting resolves to one put per second.
#[ignore]
#[tokio::test]
async fn rate_per_second_is_not_a_knob() -> Result<()> {
    let _guard = init_tracing()?;

    let store = PutRateLimiter::new(InMemory::new(), Duration::from_mins(5))
        .with_rate_per_second(NonZeroU32::new(4));

    let location = Path::from("a");
    let started = SystemTime::now();

    for _ in 0..4 {
        _ = store
            .put_opts(
                &location,
                PutPayload::from(Bytes::from_static(b"x")),
                PutOptions::default(),
            )
            .await?;
    }

    let elapsed = started.elapsed().map_or(0, |d| d.as_millis() as u64);
    println!("4 puts at a configured 4/s took {elapsed}ms");

    Ok(())
}

/// One group of `MEMBERS`, every member racing to add itself to
/// `generation.json`, over a store shaped like GCS and over one that is not.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn group_formation_under_the_per_object_cap() -> Result<()> {
    let _guard = init_tracing()?;

    const MEMBERS: usize = 16;

    async fn form(storage: Arc<DynoStore>, label: &str) -> Result<()> {
        let attempts = Arc::new(AtomicU64::new(0));
        let started = SystemTime::now();
        let mut joins = JoinSet::new();

        for member in 0..MEMBERS {
            let storage = storage.clone();
            let attempts = attempts.clone();

            _ = joins.spawn(async move {
                let member_id = format!("m-{member}");

                loop {
                    let current = storage
                        .read_group_generation("g-1")
                        .await
                        .expect("read")
                        .map(|(doc, version)| (doc, Some(version)))
                        .unwrap_or_default();

                    let (mut doc, version) = current;

                    if doc.members.contains_key(&member_id) {
                        return;
                    }

                    doc.seq += 1;
                    doc.generation_id += 1;
                    _ = doc.members.insert(member_id.clone(), MemberRef::default());

                    _ = attempts.fetch_add(1, Ordering::Relaxed);

                    match storage.update_group_generation("g-1", doc, version).await {
                        Ok(_) => return,
                        Err(UpdateError::Outdated { .. }) => continue,
                        Err(err) => panic!("{err:?}"),
                    }
                }
            });
        }

        while let Some(joined) = joins.join_next().await {
            joined.expect("member");
        }

        let elapsed = started.elapsed().map_or(0, |d| d.as_millis() as u64);
        let attempts = attempts.load(Ordering::Relaxed);
        let landed = storage
            .read_group_generation("g-1")
            .await?
            .map_or(0, |(doc, _)| doc.members.len());

        println!(
            "{label}: {MEMBERS} members, {attempts} CAS attempts, {landed} landed, {elapsed}ms"
        );

        Ok(())
    }

    form(
        Arc::new(DynoStore::new(CLUSTER, NODE, InMemory::new())),
        "plain  ",
    )
    .await?;

    form(
        Arc::new(DynoStore::new(CLUSTER, NODE, gcs_shaped(InMemory::new()))),
        "gcs-cap",
    )
    .await?;

    Ok(())
}

/// Which object paths are written more than once — the ones the per-object cap
/// can actually reach — and what a topic fan-out costs under it.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn produce_fan_out_under_the_per_object_cap() -> Result<()> {
    use std::{collections::BTreeMap, fmt};

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions,
    };
    use tansu_sans_io::{
        create_topics_request::CreatableTopic,
        record::{Record, deflated, inflated},
    };

    use crate::Topition;

    const TOPICS: usize = 32;
    const PRODUCES: usize = 8;

    type Tally = Arc<std::sync::Mutex<BTreeMap<String, u64>>>;

    #[derive(Clone)]
    struct Tallying<O> {
        inner: O,
        puts: Tally,
    }

    impl<O> fmt::Debug for Tallying<O> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Tallying").finish()
        }
    }

    impl<O> fmt::Display for Tallying<O> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Tallying").finish()
        }
    }

    #[async_trait]
    impl<O> ObjectStore for Tallying<O>
    where
        O: ObjectStore,
    {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> std::result::Result<object_store::PutResult, object_store::Error> {
            if let Ok(mut puts) = self.puts.lock() {
                *puts.entry(location.to_string()).or_default() += 1;
            }

            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> std::result::Result<Box<dyn MultipartUpload>, object_store::Error> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> std::result::Result<GetResult, object_store::Error> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, std::result::Result<Path, object_store::Error>>,
        ) -> BoxStream<'static, std::result::Result<Path, object_store::Error>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, std::result::Result<ObjectMeta, object_store::Error>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> std::result::Result<ListResult, object_store::Error> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            opts: CopyOptions,
        ) -> std::result::Result<(), object_store::Error> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

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

    async fn run(storage: DynoStore, puts: Tally, label: &str) -> Result<()> {
        for topic in 0..TOPICS {
            _ = storage
                .create_topic(
                    CreatableTopic::default()
                        .name(format!("t-{topic}"))
                        .num_partitions(1)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some([].into())),
                    false,
                )
                .await?;
        }

        let started = SystemTime::now();

        for _ in 0..PRODUCES {
            for topic in 0..TOPICS {
                let topition = Topition::new(format!("t-{topic}"), 0);
                _ = storage.produce(None, &topition, batch(4)?).await?;
            }
        }

        let elapsed = started.elapsed().map_or(0, |d| d.as_millis() as u64);

        let tally = puts.lock().expect("puts").clone();
        let total: u64 = tally.values().sum();
        let repeated: Vec<_> = {
            let mut repeated: Vec<_> = tally
                .iter()
                .filter(|(_, n)| **n > 1)
                .map(|(k, n)| (*n, k.clone()))
                .collect();
            repeated.sort_by_key(|(n, _)| std::cmp::Reverse(*n));
            repeated.into_iter().take(5).collect()
        };

        println!(
            "{label}: {TOPICS} topics x {PRODUCES} produces, {total} puts over {} distinct keys, {elapsed}ms",
            tally.len()
        );

        for (n, key) in repeated {
            println!("{label}:   {n}x {key}");
        }

        Ok(())
    }

    let puts: Tally = Default::default();
    run(
        DynoStore::new(
            CLUSTER,
            NODE,
            Tallying {
                inner: InMemory::new(),
                puts: puts.clone(),
            },
        ),
        puts,
        "plain  ",
    )
    .await?;

    let puts: Tally = Default::default();
    run(
        DynoStore::new(
            CLUSTER,
            NODE,
            gcs_shaped(Tallying {
                inner: InMemory::new(),
                puts: puts.clone(),
            }),
        ),
        puts,
        "gcs-cap",
    )
    .await?;

    Ok(())
}

/// What a consumer pays to read a prefix that holds many segments.
///
/// `fetch_prefix_coalesced` walks the fenced sub-stream view with **one ranged
/// GET per segment, awaited in sequence** — the loop at `dynostore.rs:5595`.
/// The footer warm above it is `buffered(FOOTER_FETCH_CONCURRENCY)`; the data
/// read below it is not buffered at all, so a fetch spanning N segments is N
/// serial round trips and its wall clock is N x store latency.
///
/// Latency is the parameter because that is the whole point: the same N costs
/// 24ms x N against the maintainers' measured `get_opts` and 350ms x N against
/// the brokers' (#409). Injected rather than real so the shape is observable
/// without a bucket.
#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn fetch_is_one_serial_round_trip_per_segment() -> Result<()> {
    use std::{fmt, sync::atomic::AtomicUsize, time::Duration as StdDuration};

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions,
    };
    use tansu_sans_io::{
        IsolationLevel,
        create_topics_request::CreatableTopic,
        record::{Record, deflated, inflated},
    };
    use tokio::time::sleep;

    use crate::Topition;

    /// Segments in the prefix when the fetch runs. One produce per linger
    /// window is one segment, which is what a CDC fan-out workload produces.
    const SEGMENTS: usize = 64;

    #[derive(Clone, Default)]
    struct Tally {
        gets: Arc<AtomicUsize>,
        ranged_gets: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct Slow<O> {
        inner: O,
        latency: Option<StdDuration>,
        tally: Tally,
    }

    impl<O> fmt::Debug for Slow<O> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Slow").finish()
        }
    }

    impl<O> fmt::Display for Slow<O> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Slow").finish()
        }
    }

    #[async_trait]
    impl<O> ObjectStore for Slow<O>
    where
        O: ObjectStore,
    {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> std::result::Result<object_store::PutResult, object_store::Error> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> std::result::Result<Box<dyn MultipartUpload>, object_store::Error> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> std::result::Result<GetResult, object_store::Error> {
            _ = self.tally.gets.fetch_add(1, Ordering::Relaxed);

            if options.range.is_some() {
                _ = self.tally.ranged_gets.fetch_add(1, Ordering::Relaxed);
            }

            if let Some(latency) = self.latency {
                sleep(latency).await;
            }

            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, std::result::Result<Path, object_store::Error>>,
        ) -> BoxStream<'static, std::result::Result<Path, object_store::Error>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, std::result::Result<ObjectMeta, object_store::Error>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> std::result::Result<ListResult, object_store::Error> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            opts: CopyOptions,
        ) -> std::result::Result<(), object_store::Error> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

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

    async fn run(latency_ms: u64) -> Result<()> {
        let tally = Tally::default();
        let storage = DynoStore::new(
            CLUSTER,
            NODE,
            Slow {
                inner: InMemory::new(),
                latency: (latency_ms > 0).then(|| StdDuration::from_millis(latency_ms)),
                tally: tally.clone(),
            },
        );

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name("t".into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new("t", 0);

        // One produce per linger window is one segment.
        for _ in 0..SEGMENTS {
            _ = storage.produce(None, &topition, batch(4)?).await?;
        }

        // The consumer's read, from the start of the prefix, with room for
        // everything: max_bytes is what bounds the walk, and at CDC batch sizes
        // it bounds nothing.
        tally.gets.store(0, Ordering::Relaxed);
        tally.ranged_gets.store(0, Ordering::Relaxed);

        let started = SystemTime::now();
        let fetched = storage
            .fetch(
                &topition,
                0,
                1,
                64 * 1024 * 1024,
                IsolationLevel::ReadUncommitted,
                StdDuration::from_secs(300),
            )
            .await?;
        let elapsed = started.elapsed().map_or(0, |d| d.as_millis() as u64);

        println!(
            "latency={latency_ms:>3}ms: {SEGMENTS} segments -> {} batches, \
             {} GETs ({} ranged), {elapsed}ms",
            fetched.len(),
            tally.gets.load(Ordering::Relaxed),
            tally.ranged_gets.load(Ordering::Relaxed),
        );

        Ok(())
    }

    run(0).await?;
    run(5).await?;
    run(20).await?;

    Ok(())
}
