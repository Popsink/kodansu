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

pub(crate) mod generation;
// `pub(crate)`, with the one type other crates need re-exported from the crate
// root rather than the module opened up: see `PutRateLimiter` in `lib.rs`.
pub(crate) mod limit;
pub(crate) mod retry;

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};

use limit::PutRateLimiter;
use retry::{BUCKET_RAMP_RETRY, OBJECT_CAP_RETRY, RetrySplit};

/// The object store a `gs://` URL names: the one stack both the broker and
/// `tansu audit` build, so the two cannot drift apart.
///
/// Two clients on one bucket, because GCS has two rate limits and one retry
/// budget cannot be right for both (#519). The bucket ramps and answers a
/// retryable `429 rateLimitExceeded` above the guideline, which is `503
/// SlowDown` in a different colour and wants the `s3` arm's patience; a single
/// *object name* caps at ~1 write/s, and waiting inside `object_store` for that
/// is #13's 30-second produce. [`RetrySplit`] picks per request — see
/// `gcs/retry.rs` for the table and for what the second client costs.
///
/// Wrapped in [`PutRateLimiter`] at one put per second per key, the documented
/// per-object cap. GCS has no bulk delete, so the delete fan-out is this
/// decorator's and not `object_store`'s ten (#518); `delete_concurrency` is
/// the URL's `?delete_concurrency=`, `None` for the default.
///
/// A conditional update on GCS keys on the object's generation, not its etag:
/// that is a property of the client, carried through both decorators untouched,
/// and `dynostore::tests::gcs_generation` holds the engine to it.
pub(crate) fn object_store(
    bucket: &str,
    delete_concurrency: Option<NonZeroUsize>,
) -> object_store::Result<PutRateLimiter<RetrySplit<GoogleCloudStorage>>> {
    let client = |retry| {
        GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .with_retry(retry)
            .build()
    };

    let split = RetrySplit::new(client(BUCKET_RAMP_RETRY)?, client(OBJECT_CAP_RETRY)?);

    Ok(PutRateLimiter::new(split, Duration::from_mins(5))
        .with_rate_per_second(NonZeroU32::new(1))
        .with_jitter(Some(Duration::from_millis(50)))
        .with_delete_concurrency(delete_concurrency))
}
