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
    num::NonZero,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use cached::stores::ExpiringSizedCache;
use futures::stream::BoxStream;
use governor::{DefaultDirectRateLimiter, Jitter, Quota, RateLimiter};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use tracing::{debug, instrument, warn};

use crate::Result;

const DEFAULT_JITTER: Duration = Duration::from_millis(0);

#[derive(Clone)]
pub(crate) struct PutRateLimiter<O> {
    entries: Arc<Mutex<ExpiringSizedCache<Path, Arc<DefaultDirectRateLimiter>>>>,
    rate_per_second: Option<NonZero<u32>>,
    jitter: Option<Duration>,
    object_store: O,
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
    pub(crate) fn new(object_store: O, ttl: Duration) -> Self {
        Self {
            object_store,
            entries: Arc::new(Mutex::new(ExpiringSizedCache::new(ttl))),
            rate_per_second: Default::default(),
            jitter: Default::default(),
        }
    }

    pub(crate) fn with_rate_per_second(self, rate_per_second: Option<NonZero<u32>>) -> Self {
        Self {
            rate_per_second,
            ..self
        }
    }

    pub(crate) fn with_jitter(self, jitter: Option<Duration>) -> Self {
        Self { jitter, ..self }
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

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.object_store.delete_stream(locations)
    }

    #[instrument(skip_all, fields(prefix))]
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list(prefix)
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
