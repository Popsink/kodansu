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

use std::{
    fmt::{Debug, Display},
    num::{NonZero, NonZeroUsize},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use cached::stores::ExpiringSizedCache;
use futures::stream::{BoxStream, StreamExt as _};
use governor::{DefaultDirectRateLimiter, Jitter, Quota, RateLimiter};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use tracing::{debug, instrument, warn};

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
        f.debug_struct("PutRateLimiter").finish()
    }
}

impl<O> Display for PutRateLimiter<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutRateLimiter").finish()
    }
}

impl<O> PutRateLimiter<O> {
    pub fn new(object_store: O, ttl: Duration) -> Self {
        Self {
            object_store: Arc::new(object_store),
            entries: Arc::new(Mutex::new(ExpiringSizedCache::new(ttl))),
            rate_per_second: Default::default(),
            jitter: Default::default(),
            delete_concurrency: DEFAULT_DELETE_CONCURRENCY,
        }
    }

    pub fn with_rate_per_second(self, rate_per_second: Option<NonZero<u32>>) -> Self {
        Self {
            rate_per_second,
            ..self
        }
    }

    pub fn with_jitter(self, jitter: Option<Duration>) -> Self {
        Self { jitter, ..self }
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
        self.rate_per_second
            .map(Quota::per_second)
            .map(RateLimiter::direct)
            .map(Arc::new)
    }

    #[instrument(skip_all, fields(location = %location))]
    fn location_rate_limiter(&self, location: &Path) -> Option<Arc<DefaultDirectRateLimiter>> {
        self.entries.lock().ok().and_then(|mut entries| {
            entries
                .get(location)
                .cloned()
                .or_else(|| self.rate_limiter())
                .and_then(|rate_limiter| {
                    entries
                        .insert_evict(location.to_owned(), rate_limiter.clone(), true)
                        .ok()
                        .map(|_| rate_limiter)
                })
        })
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
    #[instrument(skip_all, fields(location = %location))]
    async fn rate_limit(&self, location: &Path) {
        if self.rate_per_second.is_some()
            && let Some(rate_limiter) = self.location_rate_limiter(location)
        {
            let rate_limit_start = SystemTime::now();

            rate_limiter
                .until_ready_with_jitter(Jitter::up_to(self.jitter.unwrap_or(DEFAULT_JITTER)))
                .await;

            let rate_limited_ms = rate_limit_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64);
            debug!(rate_limited_ms);
        } else {
            warn!("no_rate_limit");
        }
    }
}

#[async_trait]
impl<O> ObjectStore for PutRateLimiter<O>
where
    O: ObjectStore,
{
    #[instrument(skip_all, fields(location = %location))]
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.rate_limit(location).await;
        self.object_store.put_opts(location, payload, opts).await
    }

    #[instrument(skip_all, fields(location = %location))]
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.rate_limit(location).await;
        self.object_store.put_multipart_opts(location, opts).await
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
    #[instrument(level = "debug", skip_all, fields(%location, if_none_match = options.if_none_match))]
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.object_store.get_opts(location, options.clone()).await
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

    #[instrument(skip_all, fields(prefix))]
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list(prefix)
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
    #[instrument(skip_all, fields(prefix, offset = %offset))]
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list_with_offset(prefix, offset)
    }

    #[instrument(skip_all, fields(prefix))]
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.object_store.list_with_delimiter(prefix).await
    }

    #[instrument(skip_all, fields(from = %from, to = %to))]
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.object_store.copy_opts(from, to, opts).await
    }
}

#[cfg(test)]
mod tests {

    use std::num::NonZeroU32;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::Error;

    use super::*;

    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(EnvFilter::from_default_env().add_directive(
                    format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
                ))
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    async fn put(prl: &PutRateLimiter<InMemory>, location: &Path) -> Result<u64> {
        let now = SystemTime::now();

        _ = prl
            .put_opts(
                location,
                PutPayload::from(Bytes::from_static(b"12321")),
                PutOptions::default(),
            )
            .await?;

        Ok(now
            .elapsed()
            .map_or(0, |duration| duration.as_millis() as u64))
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
    #[tokio::test]
    async fn a_configured_rate_admits_that_many_puts_per_second() -> Result<()> {
        let _guard = init_tracing()?;

        // The bucket starts full, so the first four puts are immediate and the
        // fifth waits out one cell's worth of refill: 1/4 s.
        const RATE: u32 = 4;

        let prl = PutRateLimiter::new(InMemory::new(), Duration::from_mins(5))
            .with_rate_per_second(NonZeroU32::new(RATE));

        let location = Path::from("a");

        let burst = {
            let now = SystemTime::now();

            for _ in 0..RATE {
                _ = put(&prl, &location).await?;
            }

            now.elapsed()
                .map_or(0, |duration| duration.as_millis() as u64)
        };

        // Generous against a slow runner, and still an order of magnitude below
        // the ~3 000 ms the defect produced.
        assert!(
            burst < 500,
            "{RATE} puts at a configured {RATE}/s took {burst}ms; \
             the whole per-second quota is being spent on each one",
        );

        // And the rate is still enforced: the next one waits for a refill.
        let fifth = put(&prl, &location).await?;

        assert!(
            fifth >= 150,
            "the {}th put must wait for its cell, waited {fifth}ms",
            RATE + 1,
        );

        Ok(())
    }

    #[tokio::test]
    async fn test() -> Result<()> {
        let _guard = init_tracing()?;

        const EXPECTED_DELAY: u64 = 900;

        let prl = PutRateLimiter::new(InMemory::new(), Duration::from_mins(5))
            .with_rate_per_second(NonZeroU32::new(1));

        let location = Path::from("a");

        let delay = {
            let now = SystemTime::now();
            _ = prl
                .put_opts(
                    &location,
                    PutPayload::from(Bytes::from_static(b"12321")),
                    PutOptions::default(),
                )
                .await?;

            now.elapsed()
                .map_or(0, |duration| duration.as_millis() as u64)
        };

        assert!(delay < EXPECTED_DELAY, "{delay}");

        let delay = {
            let now = SystemTime::now();
            _ = prl
                .put_opts(
                    &location,
                    PutPayload::from(Bytes::from_static(b"12321")),
                    PutOptions::default(),
                )
                .await?;

            now.elapsed()
                .map_or(0, |duration| duration.as_millis() as u64)
        };

        assert!(delay >= EXPECTED_DELAY, "{delay}");

        let location = Path::from("b");

        let delay = {
            let now = SystemTime::now();
            _ = prl
                .put_opts(
                    &location,
                    PutPayload::from(Bytes::from_static(b"12321")),
                    PutOptions::default(),
                )
                .await?;

            now.elapsed()
                .map_or(0, |duration| duration.as_millis() as u64)
        };

        assert!(delay < EXPECTED_DELAY, "{delay}");

        let location = Path::from("a");

        let delay = {
            let now = SystemTime::now();
            _ = prl
                .put_opts(
                    &location,
                    PutPayload::from(Bytes::from_static(b"12321")),
                    PutOptions::default(),
                )
                .await?;

            now.elapsed()
                .map_or(0, |duration| duration.as_millis() as u64)
        };

        assert!(delay >= EXPECTED_DELAY, "{delay}");

        Ok(())
    }
}
