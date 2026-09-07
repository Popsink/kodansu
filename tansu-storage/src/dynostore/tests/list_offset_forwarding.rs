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

//! Every `ObjectStore` wrapper in this crate forwards `list_with_offset` (#512).
//!
//! `list_with_offset` is one of the few `ObjectStore` methods with a default
//! body rather than a required one, and the default is not a delegation
//! (`object_store-0.14.1/src/lib.rs:1253`):
//!
//! ```text
//! fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> ... {
//!     let offset = offset.clone();
//!     self.list(prefix)
//!         .try_filter(move |f| futures_util::future::ready(f.location > offset))
//!         .boxed()
//! }
//! ```
//!
//! A wrapper that omits it therefore does not pass the call through — it
//! *replaces* the store's server-side offset (S3 and GCS `start-after`, Azure
//! `startFrom`) with a whole-prefix listing filtered in the client. The impl is
//! complete as far as the trait is concerned, so the compiler says nothing, the
//! results are identical, and the only thing that changes is the bill.
//!
//! **Three wrappers have now had this defect.** `b7c6846` fixed `Cache` and
//! `Metron`; `gcs::limit::PutRateLimiter` was in the tree at the time and was
//! not part of that pass, so it kept the downgrade for another two months
//! (#512). The op-profile guard in `scaling.rs` counts `list_with_offset` — but
//! over `InMemory` wrapped directly, so it could never see the wrapper that
//! still had the bug.
//!
//! This test therefore asserts the property against the wrappers *by name*,
//! one case per production decorator. Adding a fifth wrapper without a case
//! here is the only way left to reintroduce the defect quietly.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt as _};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};

use crate::{
    Result,
    azure::suffix::SuffixRange,
    dynostore::{Metron, metadata::Cache, tests::init_tracing},
    gcs::limit::PutRateLimiter,
};

const CLUSTER: &str = "tansu";

/// Which listing method reached the innermost store.
#[derive(Debug, Default)]
struct Calls {
    list: AtomicU64,
    list_with_offset: AtomicU64,
}

/// The bottom of the chain: records the method it was asked for, and nothing
/// else. `InMemory` implements the offset itself, so the *results* are the same
/// either way — which is exactly why only the method can be asserted on.
#[derive(Clone)]
struct Recording {
    inner: Arc<InMemory>,
    calls: Arc<Calls>,
}

impl std::fmt::Debug for Recording {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recording").finish()
    }
}

impl std::fmt::Display for Recording {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recording").finish()
    }
}

#[async_trait]
impl ObjectStore for Recording {
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
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        _ = self.calls.list.fetch_add(1, Relaxed);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        _ = self.calls.list_with_offset.fetch_add(1, Relaxed);
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

async fn recording() -> Result<(Recording, Arc<Calls>)> {
    let calls = Arc::new(Calls::default());
    let recording = Recording {
        inner: Arc::new(InMemory::new()),
        calls: calls.clone(),
    };

    // Three keys, so a whole-prefix listing and an offset listing differ in the
    // number of entries the *store* yields as well as in the method.
    for name in ["p/a", "p/b", "p/c"] {
        _ = recording
            .put_opts(
                &Path::from(name),
                PutPayload::from(Bytes::from_static(b"x")),
                PutOptions::default(),
            )
            .await?;
    }

    Ok((recording, calls))
}

/// Ask `wrapper` for an offset listing and assert the innermost store was asked
/// for one too.
async fn forwards(wrapper: impl ObjectStore, calls: Arc<Calls>, name: &str) {
    let listed = wrapper
        .list_with_offset(Some(&Path::from("p")), &Path::from("p/a"))
        .collect::<Vec<_>>()
        .await
        .len();

    assert_eq!(
        2, listed,
        "{name} must yield the entries after the offset, got {listed}",
    );

    assert_eq!(
        0,
        calls.list.load(Relaxed),
        "{name} downgraded list_with_offset to a whole-prefix list: it does not \
         override the method, so `object_store`'s default filtered client-side",
    );

    assert_eq!(
        1,
        calls.list_with_offset.load(Relaxed),
        "{name} did not forward list_with_offset to the inner store",
    );
}

/// `dynostore::metadata::Cache` — fixed by `b7c6846`.
#[tokio::test]
async fn cache_forwards_list_with_offset() -> Result<()> {
    let _guard = init_tracing()?;

    let (recording, calls) = recording().await?;
    forwards(
        Cache::new(recording, Duration::from_millis(5_000)),
        calls,
        "Cache",
    )
    .await;

    Ok(())
}

/// `dynostore::Metron` — fixed by `b7c6846`.
#[tokio::test]
async fn metron_forwards_list_with_offset() -> Result<()> {
    let _guard = init_tracing()?;

    let (recording, calls) = recording().await?;
    forwards(Metron::new(recording, CLUSTER), calls, "Metron").await;

    Ok(())
}

/// `gcs::limit::PutRateLimiter` — the one `b7c6846` missed (#512).
///
/// Built exactly as the `gs` arm of `StorageContainer::builder` builds it, so
/// the case is the shipped chain and not a convenient shape.
#[tokio::test]
async fn put_rate_limiter_forwards_list_with_offset() -> Result<()> {
    let _guard = init_tracing()?;

    let (recording, calls) = recording().await?;
    forwards(
        PutRateLimiter::new(recording, Duration::from_mins(5))
            .with_rate_per_second(NonZeroU32::new(1))
            .with_jitter(Some(Duration::from_millis(50))),
        calls,
        "PutRateLimiter",
    )
    .await;

    Ok(())
}

/// `azure::suffix::SuffixRange` — written with the forward already in it (#419),
/// because #512 was found while writing it.
#[tokio::test]
async fn suffix_range_forwards_list_with_offset() -> Result<()> {
    let _guard = init_tracing()?;

    let (recording, calls) = recording().await?;
    forwards(SuffixRange::new(recording), calls, "SuffixRange").await;

    Ok(())
}

/// The whole `gs` chain, `Cache(Metron(PutRateLimiter(store)))`, asserted end to
/// end: one omission anywhere in it is enough, and the per-wrapper cases above
/// cannot see a chain that is assembled wrongly.
#[tokio::test]
async fn the_gcs_chain_forwards_list_with_offset() -> Result<()> {
    let _guard = init_tracing()?;

    let (recording, calls) = recording().await?;

    forwards(
        Cache::new(
            Metron::new(
                PutRateLimiter::new(recording, Duration::from_mins(5))
                    .with_rate_per_second(NonZeroU32::new(1))
                    .with_jitter(Some(Duration::from_millis(50))),
                CLUSTER,
            ),
            Duration::from_millis(5_000),
        ),
        calls,
        "Cache(Metron(PutRateLimiter(_)))",
    )
    .await;

    Ok(())
}
