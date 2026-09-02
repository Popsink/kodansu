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

//! Azure has no suffix range GET, and the segment reader is built on one (#419).
//!
//! `object_store` does not merely pass a `Range: bytes=-N` through to Azure and
//! let it fail — it refuses the request client-side, before sending it
//! (`azure/client.rs:1177`). The coalesced-segment reader has no other shape:
//! the footer index lives at the *tail* of the object (#58/#64) and the read
//! path never learns the object's length first, so all four call sites are
//! suffix GETs — including `probe_prefix_tail`, where the 404 *is* the absence
//! proof and the reason the read path does not LIST per fetch (#411, #412).
//!
//! So this translates: `head` for the size, then the same bytes as a bounded
//! range. It is a decorator rather than a change to `dynostore` for the reason
//! `gcs/limit.rs` is one — the quirk belongs to one backend, and the arms above
//! it must keep issuing a single suffix GET.
//!
//! The cost is one extra request per footer read. `Get Blob Properties` and
//! `Get Blob` are the same Azure billing tier and a LIST is an order of
//! magnitude above both, so the translated probe stays comfortably cheaper than
//! the listing it exists to avoid. `tansu_azure_suffix_range_heads` is what
//! makes the deferred size-caching optimisation (RFC §3 option B) a decision
//! someone can take on a measurement rather than a hunch.

use std::{
    fmt::{Debug, Display},
    sync::LazyLock,
};

use async_trait::async_trait;
use futures::{
    StreamExt as _,
    stream::{self, BoxStream},
};
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use opentelemetry::metrics::Counter;
use tracing::instrument;

use crate::METER;

/// `Get Blob Properties` requests issued only to learn a size Azure will not let
/// us address from the end of the object.
///
/// This is the read amplification the Azure backend pays and the other two do
/// not, and the RFC defers caching it away until the bill says so. Exported so
/// that "if Azure read amplification shows up" is observable rather than
/// inferred: compare it against `tansu_prefix_index_lists` and the ratio is what
/// the translation buys.
static SUFFIX_RANGE_HEADS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_azure_suffix_range_heads")
        .with_description("size probes issued to translate a suffix range GET Azure cannot serve")
        .build()
});

/// Translates `GetRange::Suffix` into a bounded range for a store that cannot
/// serve one. Every other method delegates.
#[derive(Clone)]
pub(crate) struct SuffixRange<O> {
    object_store: O,
}

impl<O> SuffixRange<O> {
    pub(crate) fn new(object_store: O) -> Self {
        Self { object_store }
    }
}

/// Delegated to the inner store rather than printing this wrapper's own name:
/// `object_store` errors carry the store's `Display` as their `store` field, so
/// an opaque wrapper name there would cost the reader the one piece of
/// information the error was carrying.
impl<O> Debug for SuffixRange<O>
where
    O: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SuffixRange({:?})", self.object_store)
    }
}

impl<O> Display for SuffixRange<O>
where
    O: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SuffixRange({})", self.object_store)
    }
}

#[async_trait]
impl<O> ObjectStore for SuffixRange<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.object_store.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.object_store
            .put_multipart_opts(location, options)
            .await
    }

    /// `debug`, and without `ret`, for the reason `gcs/limit.rs` gives: this is
    /// on the per-segment-per-partition read path, and `ret` would put the
    /// `Debug` of a whole `GetResult` into the log at `INFO`.
    #[instrument(level = "debug", skip_all, fields(%location, range = ?options.range))]
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        let Some(GetRange::Suffix(suffix)) = options.range else {
            return self.object_store.get_opts(location, options).await;
        };

        SUFFIX_RANGE_HEADS.add(1, &[]);

        // The size — and, for the tail probe, the absence proof. A missing
        // object answers this `NotFound`, which is the same variant a suffix GET
        // of a missing object would have answered, and `fold_segment_footer` and
        // `probe_prefix_tail` both match on exactly that variant. So it is
        // propagated unchanged rather than wrapped: preserving it is the
        // correctness requirement here, not an optimisation.
        //
        // The caller's conditional headers are deliberately *not* sent on this
        // request. A `NotModified` answering a size probe would surface as the
        // answer to a range request the caller never had answered. They travel
        // on the ranged GET below, where they belong; `version` travels on both,
        // so the two requests address the same version of the object.
        let meta = self
            .object_store
            .get_opts(
                location,
                GetOptions {
                    head: true,
                    version: options.version.clone(),
                    ..Default::default()
                },
            )
            .await?
            .meta;

        let size = meta.size;

        // Reading the size and then the bytes is two requests, and nothing
        // holds a lock between them. That is sound *because of* the layout
        // rather than by luck: segments are written create-only and never
        // mutated (#57), so a segment's size is fixed the moment it exists. An
        // object replaced between the two requests would be a layout violation,
        // not a race this decorator has to defend against.
        //
        // Two degenerate cases `GetRange::Bounded` cannot express, both of which
        // a suffix GET answers without complaint:
        //
        // - a zero-length suffix is zero bytes, and `Bounded(n..n)` is
        //   `InvalidGetRange::Inconsistent`;
        // - an empty object, where the whole-object range is `Bounded(0..0)` and
        //   therefore the same error.
        //
        // No call site asks for either — every one reads a fixed-size trailer or
        // a footer whose length that trailer just gave it. They are handled
        // anyway because a decorator that turned a legal request into an error
        // on one backend and not the others would be precisely the silent
        // divergence this whole ticket exists to avoid.
        if suffix == 0 {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::empty().boxed()),
                range: size..size,
                attributes: Default::default(),
                extensions: Default::default(),
                meta,
            });
        }

        // `GetRange::Suffix(n).as_range(len)` is `len.saturating_sub(n)..len`
        // (`object_store::util`), so this is the same bytes by construction —
        // including when the suffix is longer than the object, which clamps to
        // the whole of it rather than failing.
        let range = (size > 0).then(|| GetRange::Bounded(size.saturating_sub(suffix)..size));

        self.object_store
            .get_opts(location, GetOptions { range, ..options })
            .await
    }

    /// Delegated so the inner store keeps whatever coalescing it does across
    /// ranges. None of these are suffix ranges — `get_ranges` takes bounded
    /// `Range<u64>`s — so there is nothing here to translate.
    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<bytes::Bytes>, object_store::Error> {
        self.object_store.get_ranges(location, ranges).await
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

    /// Forwarded, as `Metron` and `Cache` forward it, because the trait's default
    /// is not a delegation — it lists the whole prefix and filters client-side.
    ///
    /// Taking that default would silently turn `scan_from`'s O(new) tail refresh
    /// into an O(total) listing on Azure alone, which is the same class of defect
    /// as the one this module fixes and would be just as invisible from a green
    /// test run.
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list_with_offset(prefix, offset)
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
        options: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.object_store.copy_opts(from, to, options).await
    }
}

/// An `ObjectStore` that refuses suffix ranges the way `object_store`'s Azure
/// client does — client-side, before any request is sent.
///
/// This is how the translation is tested without an ADLS Gen2 account. It is not
/// a substitute for one: what it reproduces is the one behaviour taken verbatim
/// from `object_store`'s source, and nothing about hierarchical namespace,
/// throttling or `startFrom` (#417).
#[cfg(test)]
pub(crate) mod reject {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[derive(Clone)]
    pub(crate) struct SuffixRejecting<O> {
        inner: O,
        refused: Arc<AtomicUsize>,
        heads: Arc<AtomicUsize>,
        gets: Arc<AtomicUsize>,
        lists: Arc<AtomicUsize>,
    }

    /// The counters, shared with the store the wrapper is handed to.
    ///
    /// Listings are counted here as well as GETs because the failure this
    /// decorator prevents is not only a failed read: a suffix GET that cannot be
    /// served leaves `probe_prefix_tail` permanently `Inconclusive`, and the
    /// caller then LISTs. That is a cost regression with no failing read
    /// anywhere near it, so the listing count is the only thing that can see it.
    #[derive(Clone, Debug)]
    pub(crate) struct Requests {
        refused: Arc<AtomicUsize>,
        heads: Arc<AtomicUsize>,
        gets: Arc<AtomicUsize>,
        lists: Arc<AtomicUsize>,
    }

    impl<O> SuffixRejecting<O> {
        pub(crate) fn new(inner: O) -> Self {
            Self {
                inner,
                refused: Arc::new(AtomicUsize::new(0)),
                heads: Arc::new(AtomicUsize::new(0)),
                gets: Arc::new(AtomicUsize::new(0)),
                lists: Arc::new(AtomicUsize::new(0)),
            }
        }

        /// Only `segments/` listings are counted: the group and metadata planes
        /// list for their own reasons and would drown the signal.
        fn count_segment_listing(&self, prefix: Option<&Path>) {
            if prefix.is_some_and(|prefix| prefix.as_ref().ends_with("segments")) {
                _ = self.lists.fetch_add(1, Ordering::SeqCst);
            }
        }

        pub(crate) fn requests(&self) -> Requests {
            Requests {
                refused: self.refused.clone(),
                heads: self.heads.clone(),
                gets: self.gets.clone(),
                lists: self.lists.clone(),
            }
        }
    }

    impl Requests {
        /// Suffix ranges that reached the store and were refused. Any at all
        /// means the translation was bypassed.
        pub(crate) fn refused(&self) -> usize {
            self.refused.load(Ordering::SeqCst)
        }

        /// `Get Blob Properties` requests — the translation's extra cost.
        pub(crate) fn heads(&self) -> usize {
            self.heads.load(Ordering::SeqCst)
        }

        /// Requests that returned bytes.
        pub(crate) fn gets(&self) -> usize {
            self.gets.load(Ordering::SeqCst)
        }

        /// Listings of a prefix's `segments/`, of either shape — the expensive
        /// tier, and what a read falls back to when the tail cannot be proven.
        pub(crate) fn segment_lists(&self) -> usize {
            self.lists.load(Ordering::SeqCst)
        }

        pub(crate) fn reset(&self) {
            self.refused.store(0, Ordering::SeqCst);
            self.heads.store(0, Ordering::SeqCst);
            self.gets.store(0, Ordering::SeqCst);
            self.lists.store(0, Ordering::SeqCst);
        }
    }

    impl<O> Debug for SuffixRejecting<O> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SuffixRejecting").finish()
        }
    }

    impl<O> Display for SuffixRejecting<O> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SuffixRejecting").finish()
        }
    }

    #[async_trait]
    impl<O> ObjectStore for SuffixRejecting<O>
    where
        O: ObjectStore,
    {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> Result<PutResult, object_store::Error> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> Result<GetResult, object_store::Error> {
            // Verbatim from `object_store-0.14.1/src/azure/client.rs:1177`: the
            // request is refused here, not by Azure, so no amount of retrying
            // or endpoint configuration changes it.
            if let Some(GetRange::Suffix(_)) = options.range.as_ref() {
                _ = self.refused.fetch_add(1, Ordering::SeqCst);

                return Err(object_store::Error::NotSupported {
                    source: "Azure does not support suffix range requests".into(),
                });
            }

            if options.head {
                _ = self.heads.fetch_add(1, Ordering::SeqCst);
            } else {
                _ = self.gets.fetch_add(1, Ordering::SeqCst);
            }

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
            self.count_segment_listing(prefix);
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
            self.count_segment_listing(prefix);
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
            options: CopyOptions,
        ) -> Result<(), object_store::Error> {
            self.inner.copy_opts(from, to, options).await
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::{ObjectStoreExt as _, memory::InMemory};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::{Error, Result};

    use super::{reject::SuffixRejecting, *};

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

    const LOCATION: &str = "clusters/tansu/prefixes/org.env.conn/segments/00000000000000000001.seg";

    /// `bucket` holds `len` bytes at [`LOCATION`], distinguishable by position so
    /// a read of the wrong region cannot pass.
    async fn seeded(len: usize) -> Result<(InMemory, Bytes)> {
        let bucket = InMemory::new();
        let payload = Bytes::from_iter((0..len).map(|i| i as u8));

        _ = bucket
            .put(&Path::from(LOCATION), PutPayload::from(payload.clone()))
            .await?;

        Ok((bucket, payload))
    }

    async fn suffix_read<O>(store: &O, n: u64) -> Result<Bytes, object_store::Error>
    where
        O: ObjectStore,
    {
        store
            .get_opts(
                &Path::from(LOCATION),
                GetOptions {
                    range: Some(GetRange::Suffix(n)),
                    ..Default::default()
                },
            )
            .await?
            .bytes()
            .await
    }

    /// The translation returns the same bytes a suffix-capable store does.
    ///
    /// Differential rather than hand-computed: `InMemory` implements the suffix
    /// GET the way S3 and GCS do, so it is the oracle. Hand-computing the
    /// expected window would test this module against my arithmetic instead of
    /// against the semantics the other two backends actually have.
    #[tokio::test]
    async fn the_translation_returns_what_a_suffix_capable_store_returns() -> Result<()> {
        let _guard = init_tracing()?;

        // 64 KiB either side of the speculative footer over-read, plus the
        // degenerate sizes.
        for len in [0, 1, 17, 4_096, 65_535, 65_536, 65_537, 200_000] {
            for n in [1, 2, 17, 65_536, 1_000_000] {
                let (bucket, _) = seeded(len).await?;

                let expected = suffix_read(&bucket, n).await;

                let decorated = SuffixRange::new(SuffixRejecting::new(bucket));
                let actual = suffix_read(&decorated, n).await;

                assert_eq!(
                    expected.as_ref().ok(),
                    actual.as_ref().ok(),
                    "suffix {n} of a {len}-byte object: {expected:?} vs {actual:?}",
                );

                // Agreeing on an error would satisfy the comparison above
                // vacuously, so pin that a real object really is read.
                if len > 0 {
                    assert!(
                        actual.is_ok(),
                        "suffix {n} of a {len}-byte object: {actual:?}",
                    );
                }
            }
        }

        Ok(())
    }

    /// One `head` and one ranged GET, and no suffix range ever reaches the store.
    ///
    /// The request counts are the cost claim in #419 — one extra request per
    /// footer read, not two — and `refused` is what says the translation
    /// happened rather than the test having accidentally exercised a store that
    /// can serve a suffix.
    #[tokio::test]
    async fn a_suffix_read_costs_one_head_and_one_get() -> Result<()> {
        let _guard = init_tracing()?;

        let (bucket, payload) = seeded(4_096).await?;
        let rejecting = SuffixRejecting::new(bucket);
        let requests = rejecting.requests();
        let store = SuffixRange::new(rejecting);

        let read = suffix_read(&store, 64).await?;

        assert_eq!(payload.slice(4_096 - 64..), read);
        assert_eq!(0, requests.refused(), "a suffix range reached the store");
        assert_eq!(1, requests.heads());
        assert_eq!(1, requests.gets());

        Ok(())
    }

    /// A bounded range is untouched, and costs no `head`.
    ///
    /// This is the "no regression in request count" half of the acceptance: the
    /// decorator is only ever installed on the Azure arm, but a decorator that
    /// probed the size of every read would make the *Azure* read path twice as
    /// expensive as it needs to be.
    #[tokio::test]
    async fn a_bounded_range_is_passed_through_untouched() -> Result<()> {
        let _guard = init_tracing()?;

        let (bucket, payload) = seeded(4_096).await?;
        let rejecting = SuffixRejecting::new(bucket);
        let requests = rejecting.requests();
        let store = SuffixRange::new(rejecting);

        let read = store
            .get_opts(
                &Path::from(LOCATION),
                GetOptions {
                    range: Some(GetRange::Bounded(17..64)),
                    ..Default::default()
                },
            )
            .await?
            .bytes()
            .await?;

        assert_eq!(payload.slice(17..64), read);
        assert_eq!(0, requests.heads(), "a bounded range needs no size probe");
        assert_eq!(1, requests.gets());

        // And so is a whole-object GET.
        requests.reset();
        _ = store.get(&Path::from(LOCATION)).await?.bytes().await?;
        assert_eq!(0, requests.heads());
        assert_eq!(1, requests.gets());

        Ok(())
    }

    /// A 404 stays a 404 — the one thing the translation must not lose.
    ///
    /// `probe_prefix_tail` proves a prefix's tail by reading `cursor + 1` and
    /// taking `NotFound` as the affirmative answer, and `fold_segment_footer`
    /// matches the same variant. Both match on `object_store::Error::NotFound`
    /// specifically, so a `head` whose 404 came back wrapped — as a `Generic`, or
    /// as the `NotSupported` this decorator exists to avoid — would leave the
    /// probe permanently `Inconclusive` and every read falling back to a LIST.
    /// That is a *cost* regression with no failing assertion anywhere near it,
    /// which is why it is asserted on the variant and not on the read failing.
    #[tokio::test]
    async fn an_absent_object_answers_not_found_and_not_something_else() -> Result<()> {
        let _guard = init_tracing()?;

        let store = SuffixRange::new(SuffixRejecting::new(InMemory::new()));

        let outcome = suffix_read(&store, 65_536).await;

        assert!(
            matches!(outcome, Err(object_store::Error::NotFound { .. })),
            "the absence proof must survive the translation, got {outcome:?}",
        );

        Ok(())
    }

    /// A zero-length suffix reads nothing, and does not become an invalid range.
    ///
    /// `GetRange::Bounded` cannot express an empty window — `Bounded(n..n)` is
    /// `InvalidGetRange::Inconsistent` — so the naive translation turns a legal
    /// request into an error on Azure alone. No call site asks for it today;
    /// this is here so that the day one does, it is not an Azure-only failure
    /// found in production.
    #[tokio::test]
    async fn a_zero_length_suffix_reads_nothing() -> Result<()> {
        let _guard = init_tracing()?;

        let (bucket, _) = seeded(4_096).await?;
        let rejecting = SuffixRejecting::new(bucket);
        let requests = rejecting.requests();
        let store = SuffixRange::new(rejecting);

        assert!(suffix_read(&store, 0).await?.is_empty());

        // Answered from the size probe alone: there are no bytes to ask for.
        assert_eq!(1, requests.heads());
        assert_eq!(0, requests.gets());

        Ok(())
    }
}
