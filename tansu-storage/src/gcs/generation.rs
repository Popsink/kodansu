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

//! A store that conditions writes the way GCS does, so the engine's CAS
//! invariant can be tested without a bucket.
//!
//! S3 and Azure key a conditional update on the **etag**. GCS keys it on the
//! object's **generation**, and `object_store`'s GCS client reads that generation
//! out of `UpdateVersion::version` — not `e_tag`:
//!
//! ```text
//! PutMode::Update(v) => {
//!     let etag = v.version.as_ref().ok_or(Error::MissingVersion)?;
//!     builder.header(&VERSION_MATCH, etag)      // x-goog-if-generation-match
//! }
//! ```
//! (`object_store-0.14.1/src/gcp/client.rs:407`, and the local name `etag` there
//! is a misnomer.)
//!
//! So on GCS a conditional update whose `version` is `None` does not lose a
//! race — it does not happen at all. It returns `Generic { MissingVersion }`,
//! which is not `Precondition`, so no CAS loop in this crate retries it and no
//! caller recognises it.
//!
//! **Nothing else in the test suite can see that.** `InMemory` writes
//! `version: None` into every `PutResult` and every `ObjectMeta` it returns
//! (`object_store-0.14.1/src/memory.rs:221,248`) and conditions on the etag, so
//! on `memory://` every conditional update in the engine runs with the field
//! GCS requires left empty, and passes. The same is true of S3 and of Azure,
//! which ignores `version` entirely (`azure/client.rs:762` — see
//! [`crate::os`]). GCS is the only backend for which the field is load-bearing,
//! and it is the one backend with no coverage (#429).
//!
//! The invariant this store pins is therefore:
//!
//! > every `Version` handed to a conditional update must have come from a GET
//! > or from a PUT of that same object, never from a listing.
//!
//! It holds today — all four `PutMode::Update` sites in `dynostore` take their
//! version from a preceding `get` — but nothing states it and nothing enforces
//! it. A listing is where it would break: `object_store` builds a listed
//! `ObjectMeta` from the S3-shaped XML, which has no generation element at all,
//! and hard-codes `version: None` (`client/s3.rs:85`) — for GCS as well as for
//! S3. So [`crate::Version`] from a listing is a `Version` a GCS update cannot
//! use, and the type does not say so.
//!
//! What this store does NOT model: the per-object write cap (that is
//! [`super::limit::PutRateLimiter`]), 429 behaviour, the ramp, or anything about
//! the wire. It is one semantic difference, reproduced exactly.

#![cfg(test)]

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt as _, TryStreamExt as _};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use tokio::sync::Mutex;

use crate::Result;

/// GCS's own error text for a conditional update with no generation.
const MISSING_VERSION: &str = "Version required for conditional update";

/// Conditions `PutMode::Update` on the generation, as GCS does.
#[derive(Clone)]
pub(crate) struct GenerationConditioned<O> {
    inner: O,
    /// Current generation per live key, and the counter they are drawn from.
    /// Behind an async mutex because the check and the write have to be one
    /// step: on GCS the precondition is evaluated by the server.
    generations: Arc<Mutex<HashMap<Path, u64>>>,
    next: Arc<AtomicU64>,
    /// Conditional updates refused for want of a generation.
    missing_version: Arc<AtomicUsize>,
    /// Conditional updates that presented one and were evaluated.
    conditioned: Arc<AtomicUsize>,
}

impl<O> Debug for GenerationConditioned<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenerationConditioned").finish()
    }
}

impl<O> Display for GenerationConditioned<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenerationConditioned").finish()
    }
}

impl<O> GenerationConditioned<O> {
    pub(crate) fn new(inner: O) -> Self {
        Self {
            inner,
            generations: Arc::new(Mutex::new(HashMap::new())),
            next: Arc::new(AtomicU64::new(1_700_000_000_000_000)),
            missing_version: Arc::new(AtomicUsize::new(0)),
            conditioned: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// A handle to the refusal counter, shared with the store.
    pub(crate) fn refusals(&self) -> Refusals {
        Refusals {
            missing_version: self.missing_version.clone(),
            conditioned: self.conditioned.clone(),
        }
    }
}

/// Conditional updates this store refused because they carried no generation —
/// the ones GCS would answer `Generic { MissingVersion }` and nothing would
/// retry.
#[derive(Clone, Debug)]
pub(crate) struct Refusals {
    missing_version: Arc<AtomicUsize>,
    conditioned: Arc<AtomicUsize>,
}

impl Refusals {
    pub(crate) fn missing_version(&self) -> usize {
        self.missing_version.load(Ordering::SeqCst)
    }

    /// Conditional updates that presented a generation and were evaluated
    /// against the current one. The positive control: without it, a run that
    /// issued no conditional update at all would report the same zero refusals
    /// as one that issued a thousand good ones.
    pub(crate) fn conditioned(&self) -> usize {
        self.conditioned.load(Ordering::SeqCst)
    }

    pub(crate) fn reset(&self) {
        self.missing_version.store(0, Ordering::SeqCst);
        self.conditioned.store(0, Ordering::SeqCst);
    }
}

/// Strip the generation from a listed entry.
///
/// Not a simplification: `object_store` deserialises a GCS listing into the
/// S3-shaped `ListContents`, which carries no generation element, and the
/// conversion writes `version: None` (`client/s3.rs:85`). A listed `ObjectMeta`
/// on GCS therefore genuinely cannot be used for a conditional update — which is
/// the whole point of modelling it.
fn as_listed(mut meta: ObjectMeta) -> ObjectMeta {
    meta.version = None;
    meta
}

#[async_trait]
impl<O> ObjectStore for GenerationConditioned<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        let mut generations = self.generations.lock().await;

        // `PutMode::Update` is evaluated here rather than by the inner store,
        // because the condition GCS evaluates is not the one `InMemory` would.
        // The inner put is then unconditional, under this lock.
        let mode = match opts.mode {
            PutMode::Update(ref version) => {
                let Some(presented) = version.version.as_deref() else {
                    _ = self.missing_version.fetch_add(1, Ordering::SeqCst);

                    return Err(object_store::Error::Generic {
                        store: "GCS",
                        source: MISSING_VERSION.into(),
                    });
                };

                _ = self.conditioned.fetch_add(1, Ordering::SeqCst);

                let current = generations
                    .get(location)
                    .map(|generation| generation.to_string());

                if current.as_deref() != Some(presented) {
                    return Err(object_store::Error::Precondition {
                        path: location.to_string(),
                        source: format!(
                            "x-goog-if-generation-match validation failed. \
                             Expected = {presented} vs Actual = {}",
                            current.unwrap_or_else(|| String::from("0")),
                        )
                        .into(),
                    });
                }

                PutMode::Overwrite
            }

            // A create races on the key, which the inner store already resolves
            // — and its `AlreadyExists` is the class GCS produces too, since
            // `object_store` maps the `412` of a failed `x-goog-if-generation-match:
            // 0` onto it (`gcp/client.rs:413`).
            PutMode::Create => PutMode::Create,

            PutMode::Overwrite => PutMode::Overwrite,
        };

        let result = self
            .inner
            .put_opts(location, payload, PutOptions { mode, ..opts })
            .await?;

        let generation = self.next.fetch_add(1, Ordering::SeqCst);
        _ = generations.insert(location.to_owned(), generation);

        Ok(PutResult {
            version: Some(generation.to_string()),
            ..result
        })
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
        let generation = self.generations.lock().await.get(location).copied();

        let mut result = self.inner.get_opts(location, options).await?;
        result.meta.version = generation.map(|generation| generation.to_string());

        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        let generations = self.generations.clone();

        self.inner
            .delete_stream(locations)
            .then(move |deleted| {
                let generations = generations.clone();

                async move {
                    if let Ok(ref location) = deleted {
                        _ = generations.lock().await.remove(location);
                    }

                    deleted
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix).map_ok(as_listed).boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner
            .list_with_offset(prefix, offset)
            .map_ok(as_listed)
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        result.objects = result.objects.into_iter().map(as_listed).collect();

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
