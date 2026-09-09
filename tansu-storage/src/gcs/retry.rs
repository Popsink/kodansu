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

//! Two retry budgets for `gs://`, because GCS has two rate limits (#519).
//!
//! They have opposite failure shapes, and one budget cannot be right for both.
//!
//! **Per object name**, Google documents *"maximum rate of writes to the same
//! object name: one write per second"*. Reaching it means one key is hot, and
//! retrying does not help: the object is the bottleneck, the write after the
//! retry contends for the same second, and [`super::limit::PutRateLimiter`] is
//! already pacing the caller. #13's symptom was a 30-second produce with the
//! `object_store` default budget backing off behind it, and the fix was to make
//! that a fast, visible failure — [`OBJECT_CAP_RETRY`].
//!
//! **Per bucket**, a bucket *"initially supports roughly 1,000 object writes per
//! second and 5,000 object reads per second and then scales as needed"*, where
//! scaling is Cloud Storage redistributing key ranges and *"typically takes on
//! the order of minutes"*. Above the guideline it answers a retryable **`429
//! rateLimitExceeded`**. That is S3's `503 SlowDown` in a different colour:
//! transient, fleet-wide, and cured by waiting — so it wants the `s3` arm's
//! long, gentle budget, [`BUCKET_RAMP_RETRY`], which exists for exactly that
//! (#5, #6). A cold bucket meeting a fleet that has just scaled out is what
//! #364's autoscaling produces by design.
//!
//! Both arrive as `429`, so the retry policy cannot tell them apart. What can is
//! the request: the per-object cap is reachable only by **writing one object
//! name twice**, and the layout says which keys those are.
//!
//! | request | budget | why |
//! |---|---|---|
//! | put of a `*.seg` / `*.batch` | ramp | create-only: a key is minted, written once, never rewritten |
//! | put of anything else | cap | mutable metadata — `meta.json`, `watermark.json`, `generation.json`, member documents, leases — is the same name written again |
//! | GET, any key | ramp | there is no per-object *read* cap; the read limit is the bucket's ~5,000/s and it ramps |
//! | LIST | ramp | ditto, and a listing addresses a prefix rather than a name |
//! | DELETE | ramp | deletes count against the bucket's object-write budget, but a key is deleted once — a retirement flood meets the bucket, not the name |
//!
//! So a produce, a fetch and a maintenance tick ride out a ramp, and a CAS still
//! fails fast — where failing fast is cheap, because the loop above the CAS
//! re-reads and retries at the layer that knows what it was trying to write.
//!
//! Two budgets means two clients: `RetryConfig` is captured by
//! `GoogleCloudStorageBuilder::build` and there is no per-request override. The
//! cost is a second connection pool and a second credential cache against the
//! same bucket. Everything else about the two is identical, so which one serves
//! a request changes only how long it is willing to wait.
//!
//! **Splitting reads from writes across two clients is safe, and it is worth
//! saying why**, because the invariant it looks like it could break is the one
//! #521 was about: *every `Version` handed to a conditional update came from a
//! GET or a PUT of that object*. Here the GET is served by the ramp client and
//! the conditional PUT by the cap one. A GCS generation is a property of the
//! object in the bucket, not of the connection that observed it, and both
//! clients address one bucket with one identity — so a version crosses the
//! split intact. `object_store` keeps no per-client state a precondition reads
//! from. What does stay on one client is a multipart upload, and it does so for
//! free: every part and the completion address the same location, so they route
//! the same way.
//!
//! Not observed against a bucket, like the rest of `gs://` (#429). What it rests
//! on is Google's two documented limits and `object_store`'s retryable set,
//! which is `5xx`, `429` and `408` and nothing else
//! (`client/retry.rs:407-411`). So neither budget can turn a lost race into a
//! wait: GCS answers both a lost `PutMode::Create` and a lost conditional
//! update with `412`, which is outside that set — the first re-labelled
//! `AlreadyExists` on the way out (`gcp/client.rs:414`) and the second left as
//! `Precondition`, and both resolved by a loop in [`crate::dynostore`] rather
//! than by a retry.

use std::{
    fmt::{Debug, Display},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    BackoffConfig, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RetryConfig, path::Path,
};

use crate::{Result, dynostore::is_immutable};

/// Ride out a bucket that is still redistributing: 32 retries over 300 s,
/// backing off 200 ms to 30 s.
///
/// The `s3` arm's numbers, and deliberately a copy of them rather than a shared
/// constant — the two arms are throttled by different things (`503 SlowDown` per
/// key prefix there, `429 rateLimitExceeded` per bucket here) and only happen to
/// want the same shape of patience. The `abfss` arm duplicates them too, with
/// the same reasoning written out at the call site.
///
/// 300 s is `object_store`'s own documented ceiling for `retry_timeout`: a
/// request is retried without renewing credentials, so a longer budget outlives
/// the token it was signed with.
///
/// Lengthening a budget is what #13 warned about — a long backoff that logs
/// nothing looks like a hang. It is not silent here: `object_store` emits
/// `"Encountered server error with status {}, backing off for {} seconds, retry
/// {} of {}"` at `INFO` on every attempt (`client/retry.rs:424`), and the fleet
/// runs at `RUST_LOG=info`.
pub(crate) const BUCKET_RAMP_RETRY: RetryConfig = RetryConfig {
    backoff: BackoffConfig {
        init_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(30),
        base: 2.0,
    },
    max_retries: 32,
    retry_timeout: Duration::from_secs(300),
};

/// Fail fast on a hot object name: 5 retries over 15 s, backing off 100 ms to
/// 3 s.
///
/// The budget the whole `gs` arm used to carry, kept for the case it was chosen
/// for. A `429` here means this one key is being written faster than one write
/// per second, and no amount of waiting inside `object_store` makes the key
/// faster — what resolves it is the caller giving up on this attempt so the CAS
/// loop above can re-read and re-apply, or `PutRateLimiter` pacing the next one.
///
/// It is also the budget the create-only layout's own conflicts run under, and
/// that costs nothing, because they are not retried at all: GCS answers a lost
/// `PutMode::Create` with `412`, which is outside `object_store`'s retryable
/// set, and `object_store` re-labels it `AlreadyExists` (`gcp/client.rs:414`)
/// for the offset-assignment loop in [`crate::dynostore`] to resolve.
pub(crate) const OBJECT_CAP_RETRY: RetryConfig = RetryConfig {
    backoff: BackoffConfig {
        init_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(3),
        base: 2.0,
    },
    max_retries: 5,
    retry_timeout: Duration::from_secs(15),
};

/// The relationship between the two budgets, at compile time.
///
/// The fix #519 asks for is emphatically *not* "lengthen the `gs` budget" —
/// that reinstates #13's 30-second produce. So the property that has to hold is
/// not either set of numbers but that the hot-key budget gives up sooner than
/// the ramp one, and an edit that relaxes [`OBJECT_CAP_RETRY`] to make a ramp
/// survivable should fail to build rather than fail in a bucket.
///
/// `as_millis` because `Duration`'s comparison operators are not `const`.
const _: () = {
    assert!(
        OBJECT_CAP_RETRY.max_retries < BUCKET_RAMP_RETRY.max_retries,
        "a hot object name must give up sooner than a ramping bucket"
    );
    assert!(
        OBJECT_CAP_RETRY.retry_timeout.as_millis() < BUCKET_RAMP_RETRY.retry_timeout.as_millis()
    );
    assert!(
        OBJECT_CAP_RETRY.backoff.max_backoff.as_millis()
            < BUCKET_RAMP_RETRY.backoff.max_backoff.as_millis()
    );
    // `object_store` documents five minutes as the ceiling for any budget: a
    // retry is issued without re-signing, so a longer one outlives the
    // credentials the first attempt carried.
    assert!(BUCKET_RAMP_RETRY.retry_timeout.as_millis() <= 300_000);
};

/// Routes each request to the client whose retry budget fits the throttle it can
/// meet (#519). See the module documentation for the table.
///
/// Deliberately not instrumented. The routing is a pure function of the path and
/// the method, `Metron` already labels every metered outcome with the key class
/// the decision is made from, and a span per request here would be a third one
/// on a path that has two.
#[derive(Clone)]
pub(crate) struct RetrySplit<O> {
    /// Long budget: everything that can only have met the bucket.
    bucket_ramp: Arc<O>,

    /// Short budget: a write to an object name that gets written again.
    object_cap: Arc<O>,
}

impl<O> Debug for RetrySplit<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetrySplit").finish()
    }
}

impl<O> Display for RetrySplit<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetrySplit").finish()
    }
}

impl<O> RetrySplit<O> {
    /// Two clients on one bucket, in budget order.
    ///
    /// The arguments have the same type and cannot be told apart by the
    /// compiler, so the names carry the contract:
    /// `bucket_ramp` is the [`BUCKET_RAMP_RETRY`] client and `object_cap` the
    /// [`OBJECT_CAP_RETRY`] one. `dynostore::tests::gcs_retry` asserts the
    /// mapping end to end, which is what would catch them swapped.
    pub(crate) fn new(bucket_ramp: O, object_cap: O) -> Self {
        Self {
            bucket_ramp: Arc::new(bucket_ramp),
            object_cap: Arc::new(object_cap),
        }
    }

    /// The client for a request that **writes** `location`.
    ///
    /// [`is_immutable`] is the layout's own answer to "is this key written
    /// once": it is what the metadata cache's etag memo is keyed on, so a new
    /// create-only object class becomes a data-plane write here and a
    /// non-memoized key there in one edit rather than two.
    fn writing(&self, location: &Path) -> &O {
        if is_immutable(location) {
            &self.bucket_ramp
        } else {
            &self.object_cap
        }
    }
}

#[async_trait]
impl<O> ObjectStore for RetrySplit<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.writing(location)
            .put_opts(location, payload, opts)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.writing(location)
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.bucket_ramp.get_opts(location, options).await
    }

    /// Whole-stream delegation, and not per-location routing.
    ///
    /// A delete cannot meet the per-object cap — the layout writes a key once
    /// and deletes it once, minutes to days later — so every delete belongs on
    /// the ramp budget whatever it is deleting, and there is nothing here to
    /// decide per key. Which matters more than it looks: routing per location
    /// would mean rebuilding the fan-out, and the fan-out is *chosen* one layer
    /// up ([`super::limit::DEFAULT_DELETE_CONCURRENCY`], #518). A second
    /// `buffered` below that one could only narrow it.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.bucket_ramp.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.bucket_ramp.list(prefix)
    }

    /// Forwarded rather than inherited, for the reason spelled out on
    /// [`super::limit::PutRateLimiter::list_with_offset`]: the trait's default
    /// body is not a delegation but a whole-prefix listing filtered
    /// client-side (#512).
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.bucket_ramp.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.bucket_ramp.list_with_delimiter(prefix).await
    }

    /// Routed on the destination, which is the object this request writes.
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.writing(to).copy_opts(from, to, opts).await
    }
}
