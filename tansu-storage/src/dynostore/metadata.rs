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
    ops::DerefMut,
    slice::from_ref,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use cached::stores::ExpiringSizedCache;
use futures::stream::{BoxStream, StreamExt};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UpdateVersion, path::Path,
};
use opentelemetry::{KeyValue, metrics::Counter};
use tracing::{debug, instrument};

use crate::{Error, dynostore::object_store_error_name};

use super::METER;

#[derive(Clone, Debug, Eq, PartialEq)]
struct CacheEntry {
    version: UpdateVersion,
}

impl From<GetOptions> for CacheEntry {
    fn from(value: GetOptions) -> Self {
        Self {
            version: UpdateVersion {
                e_tag: value.if_none_match,
                version: None,
            },
        }
    }
}

impl From<&PutResult> for CacheEntry {
    fn from(put_result: &PutResult) -> Self {
        debug!(?put_result);

        let e_tag = put_result.e_tag.clone();
        let version = put_result.version.clone();

        Self {
            version: UpdateVersion { e_tag, version },
        }
    }
}

impl From<&GetResult> for CacheEntry {
    fn from(get_result: &GetResult) -> Self {
        debug!(?get_result);

        let e_tag = get_result.meta.e_tag.clone();
        let version = get_result.meta.version.clone();

        Self {
            version: UpdateVersion { e_tag, version },
        }
    }
}

/// Coarse key class for the cache metrics (#167). A revalidation rate is only
/// actionable if it says *which* keys are buying "unchanged" from the object
/// store: the fix for a single hot cluster-wide key (collapse the concurrent
/// burst) is not the fix for thousands of per-partition keys (stop needing them
/// per partition), and the aggregate cannot tell them apart.
fn key_class(path: &Path) -> &'static str {
    let path = path.as_ref();

    if path.ends_with(".seg") {
        "segment"
    } else if path.ends_with(".batch") {
        "records"
    } else if path.ends_with("/meta.json") {
        "meta"
    } else if path.ends_with("/watermark.json") {
        "watermark"
    } else if path.ends_with("/seq-floor.json") {
        "seq_floor"
    } else if path.ends_with("/era.json") {
        "era"
    } else if path.ends_with("/lease.json") {
        "lease"
    } else if path.contains("/producers/") {
        "producer"
    } else if path.contains("/groups/") {
        "group"
    } else if path.contains("/topic-metadata/") || path.contains("/topic-ids/") {
        "topic_metadata"
    } else {
        "other"
    }
}

/// Single-flight stripes for revalidation (#167). Striped rather than per-key so
/// the map cannot grow with the key space — the object store holds one entry per
/// partition and per producer, and a lock per key read would be an unbounded map
/// on the read path. Two keys sharing a stripe serialise for one round trip,
/// which is what a revalidation costs anyway.
const REVALIDATION_STRIPES: usize = 256;

#[derive(Clone)]
pub(super) struct Cache<O> {
    entries: Arc<Mutex<ExpiringSizedCache<Path, CacheEntry>>>,
    /// Serialises concurrent revalidations of the same key so a burst of callers
    /// costs one conditional GET instead of one each (#167).
    revalidations: Arc<Vec<tokio::sync::Mutex<()>>>,
    object_store: O,
    retention: Duration,
}

impl<O> Debug for Cache<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache")
            .field("retention", &self.retention)
            .finish()
    }
}

impl<O> Display for Cache<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish()
    }
}

impl<O> Cache<O>
where
    O: ObjectStore,
{
    pub(super) fn new(object_store: O, retention: Duration) -> Self {
        let entries = Arc::new(Mutex::new(ExpiringSizedCache::new(retention)));

        Self {
            entries,
            revalidations: Arc::new(
                (0..REVALIDATION_STRIPES)
                    .map(|_| tokio::sync::Mutex::new(()))
                    .collect(),
            ),
            object_store,
            retention,
        }
    }

    /// The single-flight stripe guarding revalidations of `path`.
    fn revalidation_stripe(&self, path: &Path) -> &tokio::sync::Mutex<()> {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;

        for byte in path.as_ref().as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }

        &self.revalidations[(hash as usize) % REVALIDATION_STRIPES]
    }

    /// Whether the cached etag for `path` matches `presented` — i.e. whether a
    /// caller's conditional GET can be answered `NotModified` without a request.
    fn etag_matches(&self, path: &Path, presented: &str) -> bool {
        self.entries.lock().is_ok_and(|mut guard| {
            guard
                .deref_mut()
                .get(path)
                .and_then(|entry| entry.version.e_tag.as_deref())
                .is_some_and(|cached| cached == presented)
        })
    }

    #[cfg(test)]
    fn inner(&self) -> &O {
        &self.object_store
    }
}

static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_objectstore_cache_requests")
        .with_description("object_store cache requests")
        .build()
});

static ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_objectstore_cache_errors")
        .with_description("object_store cache errors")
        .build()
});

static OUTCOMES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_objectstore_cache_outcomes")
        .with_description("object_store cache outcomes")
        .build()
});

static ENTRIES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_objectstore_cache_entries")
        .with_description("object_store cache entries")
        .build()
});

#[async_trait]
impl<O> ObjectStore for Cache<O>
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
        let method = KeyValue::new("method", "put_opts");
        // Class the write as well as the read (#167 labelled `get_opts` only).
        // Without this there is no PUT-side `class="records"` series, so
        // "nothing writes the legacy layout any more" (#178's writer-absence
        // proof) can only be inferred from the absence of a signal that was
        // never emitted. With it, the claim is a query: the `records` class on
        // `put_opts` must be flat zero.
        let class = KeyValue::new("class", key_class(location));

        REQUESTS.add(1, &[method.clone(), class]);

        self.object_store
            .put_opts(location, payload, opts)
            .await
            .inspect(|put_result| {
                debug!(?put_result);
                if let Ok(mut guard) = self.entries.lock() {
                    debug!(cached = guard.len());

                    _ = guard.insert_evict(location.to_owned(), CacheEntry::from(put_result), true);
                }
            })
            .inspect_err(|error| {
                ERRORS.add(
                    1,
                    &[
                        method,
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }

    #[instrument(skip_all, fields(location = %location))]
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        debug!(%location, ?opts);

        let method = KeyValue::new("method", "put_multipart_opts");

        REQUESTS.add(1, from_ref(&method));

        self.object_store
            .put_multipart_opts(location, opts)
            .await
            .inspect_err(|error| {
                debug!(%location, ?error);

                ERRORS.add(
                    1,
                    &[
                        method,
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }

    #[instrument(skip_all, fields(%location, if_none_match = options.if_none_match), ret)]
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        debug!(?options);

        let method = KeyValue::new("method", "get_opts");
        let class = KeyValue::new("class", key_class(location));

        REQUESTS.add(1, &[method.clone(), class.clone()]);

        if let Ok(mut guard) = self.entries.lock() {
            if let Some(entry) = guard.deref_mut().get(location) {
                debug!(?entry);

                if let Some(ref cached_e_tag) = entry.version.e_tag {
                    debug!(cached_e_tag);

                    if let Some(ref presented) = options.if_none_match {
                        debug!(cached_e_tag, presented);

                        if cached_e_tag == presented {
                            let outcome = "hit";

                            OUTCOMES.add(1, &[method, class, KeyValue::new("outcome", outcome)]);
                            debug!(outcome);

                            return Err(object_store::Error::NotModified {
                                path: location.to_string(),
                                source: Box::new(Error::PhantomCached()),
                            });
                        } else {
                            let outcome = "no_match";
                            debug!(outcome);

                            OUTCOMES.add(
                                1,
                                &[
                                    method.clone(),
                                    class.clone(),
                                    KeyValue::new("outcome", outcome),
                                ],
                            );
                        }
                    } else {
                        let outcome = "miss";

                        debug!(outcome);
                        OUTCOMES.add(
                            1,
                            &[
                                method.clone(),
                                class.clone(),
                                KeyValue::new("outcome", outcome),
                            ],
                        );
                    }
                } else {
                    let outcome = "miss";

                    debug!(outcome);
                    OUTCOMES.add(
                        1,
                        &[
                            method.clone(),
                            class.clone(),
                            KeyValue::new("outcome", outcome),
                        ],
                    );
                }
            } else {
                let outcome = "miss";

                debug!(outcome);
                OUTCOMES.add(
                    1,
                    &[
                        method.clone(),
                        class.clone(),
                        KeyValue::new("outcome", outcome),
                    ],
                );
            }
        }

        // Single-flight the revalidation (#167): `OptiCon::with` carries no TTL of
        // its own — it revalidates on every call — so this etag memo is what turns
        // "every call" into "once per key per window". Concurrent callers arriving
        // after the entry expired each paid their own conditional GET, which the
        // store answers `304` for all of them. A wide `endOffsets(assignment)`
        // resolves 32 partitions at once, and every read-committed consumer reads
        // the same cluster `meta.json`, so that burst is the common case.
        //
        // Only a *revalidation* is serialised — a caller presenting an etag. A body
        // read (a ranged segment GET carries no `if_none_match`) must never queue
        // behind anything.
        let _revalidation = if options.if_none_match.is_some() {
            let guard = self.revalidation_stripe(location).lock().await;

            // The winner refreshed the memo while we waited: answer from it instead
            // of repeating its request. Same shape as the prefix-index
            // single-flight, and the staleness it admits is one round trip rather
            // than a window.
            if let Some(presented) = options.if_none_match.as_deref()
                && self.etag_matches(location, presented)
            {
                OUTCOMES.add(
                    1,
                    &[
                        method.clone(),
                        class.clone(),
                        KeyValue::new("outcome", "coalesced"),
                    ],
                );

                return Err(object_store::Error::NotModified {
                    path: location.to_string(),
                    source: Box::new(Error::PhantomCached()),
                });
            }

            Some(guard)
        } else {
            None
        };

        self.object_store
            .get_opts(location, options.clone())
            .await
            .inspect(|get_result| {
                let e_tag = get_result.meta.e_tag.clone();
                let version = get_result.meta.version.clone();

                if let Ok(mut guard) = self.entries.lock() {
                    debug!(e_tag, version);

                    let replacement = CacheEntry::from(get_result);

                    let outcome = match guard
                        .deref_mut()
                        .insert_evict(location.to_owned(), replacement.clone(), true)
                        .ok()
                        .flatten()
                    {
                        None => "add",

                        Some(existing) if existing == replacement => "existing",

                        Some(_) => "replace",
                    };

                    debug!(outcome);

                    ENTRIES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
                }
            })
            .inspect_err(|error| {
                debug!(%location, ?error);

                if matches!(error, object_store::Error::NotModified { .. }) {
                    if let Ok(mut guard) = self.entries.lock() {
                        let replacement = CacheEntry::from(options);

                        let outcome = match guard
                            .deref_mut()
                            .insert_evict(location.to_owned(), replacement.clone(), true)
                            .ok()
                            .flatten()
                        {
                            None => "add",

                            Some(existing) if existing == replacement => "existing",

                            Some(_) => "replace",
                        };

                        debug!(outcome);

                        ENTRIES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
                    }
                } else {
                    ERRORS.add(
                        1,
                        &[
                            method,
                            KeyValue::new("error", object_store_error_name(error)),
                        ],
                    );
                }
            })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        REQUESTS.add(1, &[KeyValue::new("method", "delete_stream")]);

        let entries = self.entries.clone();
        let inner = self.object_store.delete_stream(locations);

        Box::pin(inner.inspect(move |result| {
            if let Ok(location) = result
                && let Ok(mut guard) = entries.lock()
                && guard.deref_mut().remove(location).is_some()
            {
                ENTRIES.add(1, &[KeyValue::new("outcome", "delete")]);
            }
        }))
    }

    #[instrument(skip_all, fields(prefix))]
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix);
        REQUESTS.add(1, &[KeyValue::new("method", "list")]);
        self.object_store.list(prefix)
    }

    // Forward `list_with_offset` to the inner store instead of letting the
    // default downgrade it to a full `list`: tail-offset scans
    // (`DynoStore::tail_next_offset`) rely on S3 `start-after` to read only the
    // partition tail, not the whole records prefix.
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix, ?offset);
        REQUESTS.add(1, &[KeyValue::new("method", "list_with_offset")]);
        self.object_store.list_with_offset(prefix, offset)
    }

    #[instrument(skip_all, fields(prefix))]
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        debug!(?prefix);
        REQUESTS.add(1, &[KeyValue::new("method", "list_with_delimiter")]);
        self.object_store
            .list_with_delimiter(prefix)
            .await
            .inspect_err(|error| {
                debug!(?prefix, ?error);

                ERRORS.add(
                    1,
                    &[
                        KeyValue::new("method", "list_with_delimiter"),
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }

    #[instrument(skip_all, fields(from = %from, to = %to))]
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        debug!(%from, %to, ?opts);
        REQUESTS.add(1, &[KeyValue::new("method", "copy_opts")]);
        self.object_store
            .copy_opts(from, to, opts)
            .await
            .inspect_err(|error| {
                debug!(%from, %to, ?error);

                ERRORS.add(
                    1,
                    &[
                        KeyValue::new("method", "copy_opts"),
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::Result;
    use bytes::Bytes;
    use object_store::{ObjectStoreExt, memory::InMemory};
    use serde::{Deserialize, Serialize};
    use tokio::time::sleep;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;

    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X(i32);

    #[derive(Clone, Debug)]
    struct Counter<O> {
        put_opts: Arc<Mutex<u64>>,
        get_opts: Arc<Mutex<u64>>,
        object_store: O,
    }

    #[allow(dead_code)]
    impl<O> Counter<O> {
        fn new(object_store: O) -> Self {
            Self {
                put_opts: Default::default(),
                get_opts: Default::default(),
                object_store,
            }
        }

        fn put_opts(&self) -> Result<u64> {
            self.put_opts.lock().map(|guard| *guard).map_err(Into::into)
        }

        fn get_opts(&self) -> Result<u64> {
            self.get_opts.lock().map(|guard| *guard).map_err(Into::into)
        }
    }

    impl<O> Display for Counter<O> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Counter").finish()
        }
    }

    #[async_trait]
    impl<O> ObjectStore for Counter<O>
    where
        O: ObjectStore,
    {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult, object_store::Error> {
            if let Ok(mut guard) = self.put_opts.lock() {
                *guard += 1;
            }

            self.object_store.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
            self.object_store.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> Result<GetResult, object_store::Error> {
            if let Ok(mut guard) = self.get_opts.lock() {
                *guard += 1;
            }

            self.object_store.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path, object_store::Error>>,
        ) -> BoxStream<'static, Result<Path, object_store::Error>> {
            self.object_store.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
            self.object_store.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> Result<ListResult, object_store::Error> {
            self.object_store.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            opts: CopyOptions,
        ) -> Result<(), object_store::Error> {
            self.object_store.copy_opts(from, to, opts).await
        }
    }

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

    #[tokio::test]
    async fn get() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = Counter::new(InMemory::new());

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let duration = Duration::from_millis(100);

        let cache = Cache::new(object_store, duration);

        assert_eq!(0, cache.inner().get_opts()?);

        let metadata = cache.get(&path).await?;
        assert_eq!(1, cache.inner().get_opts()?);

        let options = GetOptions {
            if_none_match: metadata.meta.e_tag.clone(),
            ..Default::default()
        };

        assert!(matches!(
            cache.get_opts(&path, options).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(1, cache.inner().get_opts()?);

        Ok(())
    }

    #[tokio::test]
    async fn get_evict_get() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = Counter::new(InMemory::new());

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let duration = Duration::from_millis(100);
        let cache = Cache::new(object_store, duration);

        assert_eq!(0, cache.inner().get_opts()?);

        let metadata = cache.get(&path).await?;
        assert_eq!(1, cache.inner().get_opts()?);

        let options = GetOptions {
            if_none_match: metadata.meta.e_tag.clone(),
            ..Default::default()
        };

        assert!(matches!(
            cache.get_opts(&path, options.clone()).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(1, cache.inner().get_opts()?);

        sleep(duration).await;

        assert!(matches!(
            cache.get_opts(&path, options).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(2, cache.inner().get_opts()?);

        Ok(())
    }

    #[tokio::test]
    async fn put_get() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let duration = Duration::from_millis(5_000);
        let cache = Cache::new(Counter::new(InMemory::new()), duration);

        assert_eq!(0, cache.inner().put_opts()?);
        assert_eq!(0, cache.inner().get_opts()?);

        let put_result = cache
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;
        assert_eq!(1, cache.inner().put_opts()?);
        assert_eq!(0, cache.inner().get_opts()?);

        let options = GetOptions {
            if_none_match: put_result.e_tag,
            ..Default::default()
        };

        assert!(matches!(
            cache.get_opts(&path, options.clone()).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(0, cache.inner().get_opts()?);

        assert!(matches!(
            cache.get_opts(&path, options).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(0, cache.inner().get_opts()?);

        Ok(())
    }

    #[tokio::test]
    async fn put_delete_get() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let duration = Duration::from_millis(100);
        let cache = Cache::new(Counter::new(InMemory::new()), duration);

        assert_eq!(0, cache.inner().put_opts()?);
        assert_eq!(0, cache.inner().get_opts()?);

        let put_result = cache
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;
        assert_eq!(1, cache.inner().put_opts()?);
        assert_eq!(0, cache.inner().get_opts()?);

        cache.delete(&path).await?;

        let options = GetOptions {
            if_none_match: put_result.e_tag,
            ..Default::default()
        };

        assert!(matches!(
            cache.get_opts(&path, options.clone()).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert_eq!(1, cache.inner().get_opts()?);

        Ok(())
    }

    /// #167: `OptiCon::with` has no TTL of its own — it revalidates on every call —
    /// so after the etag memo expires, a burst of callers each paid their own
    /// conditional GET that the store answers `304`. Measured on the fleet:
    /// ~1,313 such round trips/s, ~83% of tier-2 volume. Concurrent revalidations
    /// of one key must collapse to a single request, which is what the read path
    /// generates: a wide `endOffsets(assignment)` resolves 32 partitions at once,
    /// and every read-committed consumer reads the same cluster `meta.json`.
    #[tokio::test]
    async fn concurrent_revalidations_of_one_key_cost_one_request() -> Result<()> {
        let _guard = init_tracing()?;

        let path = Path::from("/abc/meta.json");

        let cache = Cache::new(Counter::new(InMemory::new()), Duration::from_millis(100));

        let put_result = cache
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        // Let the memo expire, which is the state the burst arrives in: every
        // caller believes it must revalidate.
        sleep(Duration::from_millis(150)).await;
        assert_eq!(0, cache.inner().get_opts()?);

        let options = GetOptions {
            if_none_match: put_result.e_tag,
            ..Default::default()
        };

        let outcomes =
            futures::future::join_all((0..16).map(|_| cache.get_opts(&path, options.clone())))
                .await;

        // Every caller is told "unchanged" …
        for outcome in &outcomes {
            assert!(
                matches!(outcome, Err(object_store::Error::NotModified { .. })),
                "expected NotModified, got {outcome:?}"
            );
        }

        // … at the cost of one round trip, not sixteen.
        assert_eq!(
            1,
            cache.inner().get_opts()?,
            "concurrent revalidations must be single-flighted (#167)"
        );

        Ok(())
    }

    /// A body read must never queue behind a revalidation: a ranged segment GET
    /// carries no `if_none_match`, and serialising those would put the fetch path
    /// behind unrelated metadata traffic.
    #[tokio::test]
    async fn body_reads_are_not_single_flighted() -> Result<()> {
        let _guard = init_tracing()?;

        let path = Path::from("/abc/00000000000000000001.seg");
        let cache = Cache::new(Counter::new(InMemory::new()), Duration::from_millis(100));

        _ = cache
            .put(
                &path,
                PutPayload::from(Bytes::from_static(b"segment bytes")),
            )
            .await?;

        let reads =
            futures::future::join_all((0..8).map(|_| cache.get_opts(&path, GetOptions::default())))
                .await;

        for read in &reads {
            assert!(read.is_ok(), "body read failed: {read:?}");
        }

        assert_eq!(
            8,
            cache.inner().get_opts()?,
            "a body read is not a revalidation and must not be collapsed"
        );

        Ok(())
    }
}
