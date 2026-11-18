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

use std::fmt::{Debug, Display};
use std::num::{NonZero, NonZeroUsize};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use cached::stores::ExpiringSizedCache;
use futures::stream::{BoxStream, StreamExt as _};
use governor::{DefaultDirectRateLimiter, Jitter, Quota};
use object_store::path::Path;
use object_store::{CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt as _, PutMultipartOptions};
use object_store::{PutOptions, PutPayload, PutResult};
use tracing::debug;

use crate::Result;

const DEFAULT_JITTER: Duration = Duration::from_millis(0);

/// Objects deleted concurrently on `gs://`, and the reason this decorator
/// implements [`ObjectStore::delete_stream`] at all (#518).
///
/// GCS has no bulk delete. `object_store` gives S3 `DeleteObjects` (1,000 keys
/// per request, 20 requests in flight) and Azure the Blob Batch endpoint (256 ×
/// 20), and for GCS it issues one `DELETE` per object at a hardcoded
/// `buffered(10)` (`object_store-0.14.1/src/gcp/mod.rs:187`) — the same 10 the
/// trait's own doc example uses. Every delete this engine issues, which is
/// retention, compaction retiring the segments it just merged, and group and
/// topic teardown, inherited that number without anyone choosing it.
///
/// Sixteen is chosen, and it is the number [`crate::dynostore`]'s `delete_each`
/// already picked for the identical shape: a per-key delete fan-out wide enough
/// to make progress and narrow enough not to re-create the request burst that
/// throttling punishes. On GCS *every* delete is that shape, so it takes that
/// width.
///
/// The arithmetic an operator needs before raising it, none of it measured
/// against a bucket:
///
/// - at a nominal 30 ms per `DELETE`, 16 in flight is ~530 deletes/s per
///   `delete_stream`;
/// - a maintainer runs up to `PREFIX_MAINTENANCE_CONCURRENCY` (4) prefixes at
///   once and each one deletes, so the per-replica ceiling is ~4× that;
/// - a bucket starts at ~1,000 **object writes**/s and deletes count against
///   that budget, and it ramps by redistribution rather than instantly.
///
/// So this is not a knob with 60× of headroom in it. What widening buys is a
/// shorter delete wave, not a higher sustained delete rate — the sustained rate
/// is whatever retention has to retire, which is set by the write rate. Raise it
/// with `?delete_concurrency=` when `tansu_maintenance_duration` approaches the
/// maintenance interval, or `tansu_prefix_drain_stops{reason!="drained"}` is
/// non-zero, and not before.
pub(crate) const DEFAULT_DELETE_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(16).unwrap();

#[derive(Clone)]
pub struct PutRateLimiter<O> {
    entries: Arc<Mutex<ExpiringSizedCache<Path, Arc<DefaultDirectRateLimiter>>>>,
    rate_per_second: Option<NonZero<u32>>,
    jitter: Option<Duration>,
    delete_concurrency: NonZeroUsize,
    // `Arc` rather than the store itself so `delete_stream` can hand an owned
    // handle to the `'static` stream it returns, the way every `ObjectStore`
    // implementation of that method does.
    object_store: Arc<O>,
}

impl<O> Debug for PutRateLimiter<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PutRateLimiter")
    }
}

impl<O> Display for PutRateLimiter<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PutRateLimiter")
    }
}

impl<O> PutRateLimiter<O> {
    pub fn new(object_store: O, ttl: Duration) -> Self {
        let cache = ExpiringSizedCache::new(ttl);
        PutRateLimiter {
            delete_concurrency: DEFAULT_DELETE_CONCURRENCY,
            entries: Arc::new(Mutex::new(cache)),
            jitter: None,
            object_store: Arc::new(object_store),
            rate_per_second: None,
        }
    }

    pub fn with_rate_per_second(self, rate_per_second: Option<NonZero<u32>>) -> Self {
        let mut limited = self;
        limited.rate_per_second = rate_per_second;
        limited
    }

    pub fn with_jitter(self, jitter: Option<Duration>) -> Self {
        let mut jittered = self;
        jittered.jitter = jitter;
        jittered
    }

    /// How many objects [`ObjectStore::delete_stream`] deletes at once, or
    /// `None` for [`DEFAULT_DELETE_CONCURRENCY`] (#518).
    ///
    /// `None` rather than a second constructor because that is how the storage
    /// URL arrives: an absent `?delete_concurrency=` and an unparseable one are
    /// the same thing to `StorageContainer::builder`, and both mean the stated
    /// default.
    pub fn with_delete_concurrency(self, delete_concurrency: Option<NonZeroUsize>) -> Self {
        Self {
            delete_concurrency: delete_concurrency.unwrap_or(DEFAULT_DELETE_CONCURRENCY),
            ..self
        }
    }

    fn rate_limiter(&self) -> Option<Arc<DefaultDirectRateLimiter>> {
        let quota = Quota::per_second(self.rate_per_second?);
        Some(Arc::new(governor::RateLimiter::direct(quota)))
    }

    /// The store this decorator wraps.
    fn inner(&self) -> &O {
        &self.object_store
    }

    #[tracing::instrument(skip_all, fields(location = %location))]
    fn location_rate_limiter(&self, location: &Path) -> Option<Arc<DefaultDirectRateLimiter>> {
        let mut entries = self.entries.lock().ok()?;
        let cached = entries.get(location).cloned();
        let limiter = match cached {
            Some(known) => known,
            None => self.rate_limiter()?,
        };
        let key = location.to_owned();
        _ = entries.insert_evict(key, Arc::clone(&limiter), true).ok()?;
        Some(limiter)
    }

    /// Wait for this location's budget to admit **one** put.
    ///
    /// One cell, not `rate_per_second` of them (#428). `Quota::per_second(n)`
    /// builds a bucket that holds `n` cells and refills at `n` per second, so
    /// asking for `n` cells made every put consume a full second of quota and
    /// every configured rate collapse to one put per second. Measured: four puts
    /// to one key at a configured 4/s took 3211 ms instead of ~750.
    ///
    /// It went unnoticed because the only caller hardcodes `1` — where the bug
    /// is invisible, `n == 1` either way — and it becomes live the moment
    /// someone reaches for the knob. Which is exactly what an operator reaches
    /// for when a consumer group cannot form under the cap (#427).
    ///
    /// `until_ready_with_jitter` rather than `until_n_ready_with_jitter(1)`:
    /// there is no count to get wrong, and no `InsufficientCapacity` to handle —
    /// a single cell cannot exceed a bucket that holds at least one.
    #[tracing::instrument(skip_all, fields(location = %location))]
    async fn rate_limit(&self, location: &Path) {
        let limiter = match self.rate_per_second {
            Some(_) => self.location_rate_limiter(location),
            None => None,
        };
        let Some(limiter) = limiter else {
            tracing::warn!("no_rate_limit");
            return;
        };

        let waiting_since = SystemTime::now();
        let jitter = Jitter::up_to(self.jitter.unwrap_or(DEFAULT_JITTER));
        limiter.until_ready_with_jitter(jitter).await;
        let took = waiting_since.elapsed();
        let waited = took.map_or(0, |took| took.as_millis() as u64);
        debug!(rate_limited_ms = waited);
    }
}

#[async_trait::async_trait]
impl<O> ObjectStore for PutRateLimiter<O>
where
    O: ObjectStore,
{
    #[tracing::instrument(skip_all, fields(location = %location))]
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        Self::rate_limit(self, location).await;
        self.inner().put_opts(location, payload, opts).await
    }

    #[tracing::instrument(skip_all, fields(location = %location))]
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        Self::rate_limit(self, location).await;
        self.inner().put_multipart_opts(location, opts).await
    }

    /// `debug`, and without `ret` (#428).
    ///
    /// `#[instrument]` with no level is `INFO`, and `ret` records the return
    /// value as an event at the span's level — so every GCS read emitted an
    /// `INFO` carrying the `Debug` of the whole `GetResult`: payload,
    /// `ObjectMeta`, range, attributes, extensions. On a read path that issues
    /// one GET per segment per partition per topic, and a fleet that runs at
    /// `RUST_LOG=info`.
    ///
    /// This decorator does not rate-limit reads at all — it only delegates — so
    /// it has nothing to say about a GET that `Metron`'s own instrumentation
    /// does not already say better.
    #[tracing::instrument(level = "debug", skip_all, fields(%location, if_none_match = options.if_none_match))]
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.inner().get_opts(location, options.clone()).await
    }

    /// Delete at a width this fork chose, not at `object_store`'s (#518).
    ///
    /// The inner store's own `delete_stream` is *not* called: on GCS it is a
    /// per-object `DELETE` at a hardcoded `buffered(10)`, and re-buffering the
    /// stream it returns cannot widen what it already narrowed. So the fan-out
    /// is rebuilt here out of single-object deletes —
    /// [`DEFAULT_DELETE_CONCURRENCY`] carries the width and the arithmetic
    /// behind it.
    ///
    /// `ObjectStoreExt::delete` bottoms out in the inner store's
    /// `delete_stream` with one location, which for GCS is exactly the one
    /// `DELETE` upstream would have issued. Same request, same per-location
    /// `Result`, same ordering — `buffered` and not `buffer_unordered`, because
    /// `Metron` counts what this stream yields and `dynostore::bulk_delete`
    /// `try_collect`s it.
    ///
    /// **This override is only correct because GCS has no bulk delete**, and
    /// this decorator is only ever built on the `gs` arm. Wrapping a store that
    /// *does* have one — S3's `DeleteObjects`, Azure's Blob Batch — would
    /// replace a thousand keys per request with a thousand requests. `gcs/`
    /// is where that stays true.
    ///
    /// Deletes are deliberately not rate-limited, unlike puts. The cap this
    /// decorator exists for is one write per second to the same *object name*,
    /// and the layout is create-only: a key is written once and deleted once,
    /// minutes to days later, so a delete never races a put to the same name.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        let object_store = self.object_store.clone();
        let delete_concurrency = self.delete_concurrency.get();

        debug!(delete_concurrency);

        locations
            .map(move |location| {
                let object_store = object_store.clone();

                async move {
                    let location = location?;
                    object_store.delete(&location).await?;
                    Ok(location)
                }
            })
            .buffered(delete_concurrency)
            .boxed()
    }

    #[tracing::instrument(skip_all, fields(prefix))]
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner().list(prefix)
    }

    /// Forward `list_with_offset` rather than inherit the default (#512).
    ///
    /// `list_with_offset` is one of the `ObjectStore` methods that ships a
    /// default body instead of a required one, and that default is not a
    /// delegation — it lists the whole prefix and filters client-side
    /// (`object_store-0.14.1/src/lib.rs:1253`). A decorator that omits it does
    /// not pass the call through, it *replaces* GCS's server-side `start-after`
    /// with a full listing. The impl still compiles, so nothing but this comment
    /// stands between the next reader and a whole-prefix LIST.
    ///
    /// The caller that pays is `refresh_prefix_index`'s incremental branch,
    /// whose whole point is to cost O(new) rather than O(total) — on GCS alone
    /// it was costing `ceil(N/1000)` class-A operations per refresh.
    ///
    /// `Cache` and `Metron` carry the same forward for the same reason
    /// (`b7c6846`); this decorator predates that fix and was not part of it.
    /// Reads are not rate-limited here at all, so there is nothing to add beyond
    /// the delegation.
    #[tracing::instrument(skip_all, fields(prefix, offset = %offset))]
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner().list_with_offset(prefix, offset)
    }

    #[tracing::instrument(skip_all, fields(prefix))]
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner().list_with_delimiter(prefix).await
    }

    #[tracing::instrument(skip_all, fields(from = %from, to = %to))]
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner().copy_opts(from, to, opts).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use object_store::path::Path;
    use object_store::{ObjectStore as _, PutOptions, PutPayload, memory::InMemory};

    use super::{PutRateLimiter, Result};

    /// Logs this test at `debug` for this crate, on top of `RUST_LOG`, into
    /// `../logs/<package>/<thread>.log`: one file per test.
    fn init_tracing() -> Result<tracing::subscriber::DefaultGuard> {
        let directive = format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?;
        let filter = tracing_subscriber::EnvFilter::from_default_env().add_directive(directive);

        let unnamed = || crate::Error::Message(String::from("unnamed thread"));
        let current = std::thread::current();
        let test = current.name().ok_or_else(unnamed)?;
        let log = std::fs::File::create(format!("../logs/{}/{test}.log", env!("CARGO_PKG_NAME")))?;

        let subscriber = tracing_subscriber::fmt().with_level(true);
        let subscriber = subscriber.with_line_number(true).with_thread_names(false);
        let subscriber = subscriber.with_env_filter(filter);
        let subscriber = subscriber.with_writer(std::sync::Arc::new(log));
        Ok(tracing::subscriber::set_default(subscriber.finish()))
    }

    /// Whole milliseconds since `start`, or none if the clock went back.
    fn millis_since(start: SystemTime) -> u64 {
        start.elapsed().map_or(0, |took| took.as_millis() as u64)
    }

    /// A rate limiter over memory, admitting `rate` puts a second per key.
    fn limited(rate: u32) -> PutRateLimiter<InMemory> {
        let limiter = PutRateLimiter::new(InMemory::new(), Duration::from_mins(5));
        limiter.with_rate_per_second(std::num::NonZeroU32::new(rate))
    }

    /// How long a put to `location` took, in milliseconds.
    async fn put(prl: &PutRateLimiter<InMemory>, location: &Path) -> Result<u64> {
        let start = SystemTime::now();
        let payload = PutPayload::from_static(b"12321");
        _ = prl
            .put_opts(location, payload, PutOptions::default())
            .await?;
        Ok(millis_since(start))
    }

    /// A configured rate of N admits N puts per second to one key (#428).
    ///
    /// It admitted **one**, whatever N was: each put asked for `rate_per_second`
    /// cells out of a bucket that refills at `rate_per_second` per second, so
    /// one put consumed a full second of quota. Four puts at a configured 4/s
    /// took 3211 ms instead of ~750.
    ///
    /// The test beside this one cannot see it — it only ever configures 1/s,
    /// where `n == 1` either way. That is why the defect survived having a test.
    ///
    /// The bucket starts full, so the first four puts are immediate and the
    /// fifth waits out one cell's worth of refill: 1/4 s. The 500 ms allowed
    /// for the burst is generous against a slow runner, and still an order of
    /// magnitude below the ~3 000 ms the defect produced.
    #[tokio::test]
    async fn a_configured_rate_admits_that_many_puts_per_second() -> Result<()> {
        let _guard = init_tracing()?;
        let rate = 4;
        let prl = limited(rate);
        let key = Path::from("a");

        let start = SystemTime::now();
        for _ in 0..rate {
            _ = put(&prl, &key).await?;
        }
        let burst = millis_since(start);
        assert!(
            burst < 500,
            "{rate} puts at a configured {rate}/s took {burst}ms; \
             the whole per-second quota is being spent on each one",
        );

        let fifth = put(&prl, &key).await?;
        assert!(
            fifth >= 150,
            "the {}th put must wait for its cell, waited {fifth}ms",
            rate + 1,
        );
        Ok(())
    }

    /// At one put a second, a second put to a key waits out the second, while
    /// a put to another key does not wait at all.
    #[tokio::test]
    async fn each_key_has_a_budget_of_its_own() -> Result<()> {
        let _guard = init_tracing()?;
        let full_second = 900;
        let prl = limited(1);
        let (a, b) = (Path::from("a"), Path::from("b"));

        let first = put(&prl, &a).await?;
        assert!(first < full_second, "{first}");
        let again = put(&prl, &a).await?;
        assert!(again >= full_second, "{again}");
        let other = put(&prl, &b).await?;
        assert!(other < full_second, "{other}");
        let back = put(&prl, &a).await?;
        assert!(back >= full_second, "{back}");
        Ok(())
    }
}
