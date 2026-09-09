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

//! The delete fan-out on `gs://` is a number this fork chose (#518).
//!
//! `object_store` gives S3 a bulk `DeleteObjects` (1,000 keys per request, 20
//! requests in flight) and Azure the Blob Batch endpoint (256 × 20). For GCS it
//! issues one `DELETE` per object at a hardcoded `buffered(10)`
//! (`object_store-0.14.1/src/gcp/mod.rs:187`) — the same ten the trait's own doc
//! example uses, which is where it came from. Every delete this engine issues
//! inherited it: retention, compaction retiring the segments it just merged,
//! group and topic teardown, all seven `delete_stream` call sites in
//! `dynostore`.
//!
//! Inherited, and *unreachable*: there is no builder option on
//! `GoogleCloudStorage` for it, so the only place the number could be chosen is
//! the decorator the `gs` arm already wraps the store in. `PutRateLimiter` now
//! implements `delete_stream` by rebuilding the fan-out out of single-object
//! deletes at [`DEFAULT_DELETE_CONCURRENCY`], overridable per deployment with
//! `?delete_concurrency=`.
//!
//! Re-buffering what the inner store returns would not have worked, and that is
//! the whole reason the override looks the way it does — a stream already
//! narrowed to ten cannot be widened downstream of the narrowing. So these
//! tests observe **concurrency at the bottom of the chain**, not at the top: the
//! innermost store gauges how many deletes are in flight inside it and keeps the
//! maximum. `upstreams_ten_is_what_gcs_would_have_done` is the control, with
//! that same store unwrapped and its `delete_stream` a verbatim copy of
//! `gcp/mod.rs:187`, so the number the fix moves is measured and not asserted
//! from the changelog.
//!
//! What none of this observes is GCS: whether a wider wave is one the bucket's
//! object-write ramp actually accepts needs the real-bucket run #429 is still
//! about. The width is a stated number here, with the arithmetic in
//! [`DEFAULT_DELETE_CONCURRENCY`] and in `docs/gcs.md`; it is not a measured
//! one.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt as _};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use tokio::time::sleep;

use crate::{
    Result,
    dynostore::{Metron, metadata::Cache, tests::init_tracing},
    gcs::limit::{DEFAULT_DELETE_CONCURRENCY, PutRateLimiter},
};

const CLUSTER: &str = "tansu";

/// `object_store`'s GCS fan-out, verbatim (`gcp/mod.rs:187`).
const UPSTREAM_GCS_DELETE_CONCURRENCY: usize = 10;

/// Enough objects that every width under test is reached and refilled rather
/// than merely filled once.
const OBJECTS: usize = 40;

/// Long enough that every future admitted by a `buffered` wave has entered the
/// gauge before the first one leaves it, and short enough to run in the normal
/// suite. The whole file is ~0.5 s.
const DELETE_LATENCY: Duration = Duration::from_millis(25);

/// How many deletes were in flight inside the innermost store at once.
#[derive(Debug, Default)]
struct Gauge {
    in_flight: AtomicUsize,
    peak: AtomicUsize,
    completed: AtomicUsize,
}

impl Gauge {
    fn enter(&self) {
        _ = self
            .peak
            .fetch_max(self.in_flight.fetch_add(1, SeqCst) + 1, SeqCst);
    }

    fn leave(&self) {
        _ = self.in_flight.fetch_sub(1, SeqCst);
        _ = self.completed.fetch_add(1, SeqCst);
    }
}

/// The bottom of the chain: `InMemory` with a latency per delete and a gauge
/// around it.
///
/// Its `delete_stream` is `object_store`'s GCS implementation copied — one
/// delete per object, `buffered(10)` — so that composing a decorator over it
/// reproduces the shipped `gs` chain, and running it bare reproduces what GCS
/// does today.
#[derive(Clone)]
struct Latent {
    inner: Arc<InMemory>,
    gauge: Arc<Gauge>,
}

impl std::fmt::Debug for Latent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Latent").finish()
    }
}

impl std::fmt::Display for Latent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Latent").finish()
    }
}

#[async_trait]
impl ObjectStore for Latent {
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
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        let inner = self.inner.clone();
        let gauge = self.gauge.clone();

        locations
            .map(move |location| {
                let inner = inner.clone();
                let gauge = gauge.clone();

                async move {
                    let location = location?;

                    gauge.enter();
                    sleep(DELETE_LATENCY).await;
                    let deleted = inner.delete(&location).await;
                    gauge.leave();

                    deleted.map(|()| location)
                }
            })
            .buffered(UPSTREAM_GCS_DELETE_CONCURRENCY)
            .boxed()
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

/// [`OBJECTS`] keys to delete, and the gauge that will watch them go.
async fn latent() -> Result<(Latent, Arc<Gauge>)> {
    let gauge = Arc::new(Gauge::default());
    let latent = Latent {
        inner: Arc::new(InMemory::new()),
        gauge: gauge.clone(),
    };

    for key in doomed() {
        _ = latent
            .put_opts(
                &key,
                PutPayload::from(Bytes::from_static(b"x")),
                PutOptions::default(),
            )
            .await?;
    }

    Ok((latent, gauge))
}

fn doomed() -> Vec<Path> {
    (0..OBJECTS)
        .map(|n| Path::from(format!("clusters/{CLUSTER}/p/{n:0>20}.seg")))
        .collect()
}

/// Hand `store` every key and assert the width the innermost store saw.
///
/// Both halves matter: a fan-out that widens by dropping deletes on the floor
/// would pass the peak assertion alone.
async fn deletes_at(store: impl ObjectStore, gauge: Arc<Gauge>, expected: usize, name: &str) {
    let deleted = store
        .delete_stream(futures::stream::iter(doomed().into_iter().map(Ok)).boxed())
        .collect::<Vec<_>>()
        .await;

    assert_eq!(
        OBJECTS,
        deleted.iter().filter(|result| result.is_ok()).count(),
        "{name} must delete every key: {deleted:?}",
    );

    assert_eq!(
        OBJECTS,
        gauge.completed.load(SeqCst),
        "{name} must reach the store once per key",
    );

    assert_eq!(
        expected,
        gauge.peak.load(SeqCst),
        "{name} deleted {} at a time, not {expected}",
        gauge.peak.load(SeqCst),
    );
}

/// Exactly what `StorageContainer::builder`'s `gs` arm wraps the store in,
/// minus the width, which is what each test below sets.
fn gcs_shaped<O>(inner: O) -> PutRateLimiter<O> {
    PutRateLimiter::new(inner, Duration::from_mins(5))
        .with_rate_per_second(NonZeroU32::new(1))
        .with_jitter(Some(Duration::from_millis(50)))
}

/// The control: the number GCS deletes at today, measured rather than quoted.
///
/// Ten, from `gcp/mod.rs:187`, which is `Latent::delete_stream` copied. If this
/// test fails after an `object_store` bump, the copy has drifted from upstream
/// and the rest of the file is measuring the wrong baseline.
#[tokio::test]
async fn upstreams_ten_is_what_gcs_would_have_done() -> Result<()> {
    let _guard = init_tracing()?;

    let (latent, gauge) = latent().await?;

    deletes_at(
        latent,
        gauge,
        UPSTREAM_GCS_DELETE_CONCURRENCY,
        "GoogleCloudStorage",
    )
    .await;

    Ok(())
}

/// The `gs` arm as shipped: the width is this fork's default, not upstream's.
#[tokio::test]
async fn the_gs_arm_deletes_at_this_forks_stated_width() -> Result<()> {
    let _guard = init_tracing()?;

    let (latent, gauge) = latent().await?;

    assert_ne!(
        UPSTREAM_GCS_DELETE_CONCURRENCY,
        DEFAULT_DELETE_CONCURRENCY.get(),
        "a default equal to upstream's would make this file assert nothing",
    );

    deletes_at(
        gcs_shaped(latent),
        gauge,
        DEFAULT_DELETE_CONCURRENCY.get(),
        "PutRateLimiter",
    )
    .await;

    Ok(())
}

/// `?delete_concurrency=` is the width, and it is allowed to be *narrower* than
/// upstream's ten — a fleet on a cold bucket has more replicas than headroom.
#[tokio::test]
async fn a_configured_delete_concurrency_is_the_width() -> Result<()> {
    let _guard = init_tracing()?;

    const CONFIGURED: usize = 4;

    let (latent, gauge) = latent().await?;

    deletes_at(
        gcs_shaped(latent).with_delete_concurrency(NonZeroUsize::new(CONFIGURED)),
        gauge,
        CONFIGURED,
        "PutRateLimiter(delete_concurrency=4)",
    )
    .await;

    Ok(())
}

/// The whole `gs` chain, `Cache(Metron(PutRateLimiter(store)))`.
///
/// Both outer decorators forward `delete_stream` — `Metron` to count it, `Cache`
/// to evict what it deleted — and a forward that buffered or collected on the
/// way past would flatten the width back down without failing anything above.
#[tokio::test]
async fn the_gcs_chain_keeps_the_width() -> Result<()> {
    let _guard = init_tracing()?;

    let (latent, gauge) = latent().await?;

    deletes_at(
        Cache::new(
            Metron::new(gcs_shaped(latent), CLUSTER),
            Duration::from_millis(5_000),
        ),
        gauge,
        DEFAULT_DELETE_CONCURRENCY.get(),
        "Cache(Metron(PutRateLimiter(_)))",
    )
    .await;

    Ok(())
}
