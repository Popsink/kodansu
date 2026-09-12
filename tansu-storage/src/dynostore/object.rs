// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! Object-store primitives: the create-only PUT, the JSON `get`/`put` pair,
//! the attributed listings and the delete fan-out every other module is
//! written in terms of.

use super::*;

impl Scan {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::SegmentIndex => "segment_index",
            Self::Group => "group",
            Self::TopicMetadata => "topic_metadata",
            Self::AdminDelete => "admin_delete",
            Self::RetiredPrefix => "retired_prefix",
            Self::Ping => "ping",
        }
    }
}

impl DynoStore {
    /// Create-only PUT: `Ok(true)` if this call created the object, `Ok(false)`
    /// if it already existed.
    pub(super) async fn put_create(&self, path: &Path, payload: PutPayload) -> Result<bool> {
        match self
            .object_store
            .put_opts(
                path,
                payload,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// List `prefix`, attributing the call to the code that asked for it (#165).
    /// Every listing in this engine goes through here or [`Self::scan_from`], so
    /// the tier-1 plane can be broken down by purpose rather than guessed at.
    pub(super) fn scan(
        &self,
        purpose: Scan,
        prefix: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        LIST_SCANS.add(1, &[KeyValue::new("purpose", purpose.as_str())]);
        self.object_store.list(Some(prefix))
    }

    /// As [`Self::scan`], but resuming after `start_after` (S3 `start-after`) so
    /// only the tail beyond a known point is read.
    pub(super) fn scan_from(
        &self,
        purpose: Scan,
        prefix: &Path,
        start_after: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        LIST_SCANS.add(1, &[KeyValue::new("purpose", purpose.as_str())]);
        self.object_store
            .list_with_offset(Some(prefix), start_after)
    }

    /// As [`Self::scan`], but delimited (S3 `delimiter=/`, common prefixes only).
    pub(super) async fn scan_delimited(
        &self,
        purpose: Scan,
        prefix: &Path,
    ) -> Result<ListResult, object_store::Error> {
        LIST_SCANS.add(1, &[KeyValue::new("purpose", purpose.as_str())]);
        self.object_store.list_with_delimiter(Some(prefix)).await
    }

    /// Wall-clock milliseconds since the Unix epoch, for lease expiry (#59).
    pub(super) fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as i64)
            .unwrap_or(0)
    }

    /// Read `Option`, not `Result`: an object that is not there is an answer —
    /// the group has no generation, the member has no document — and only a
    /// store error is a failure.
    pub(super) fn absent_is_none<V>(result: Result<(V, Version)>) -> Result<Option<(V, Version)>> {
        match result {
            Ok(pair) => Ok(Some(pair)),
            Err(Error::ObjectStore(error))
                if matches!(error.as_ref(), object_store::Error::NotFound { .. }) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Delete the given batch object locations.
    ///
    /// S3 can throttle a multi-object `DeleteObjects` by returning **HTTP 200**
    /// with a top-level `<Error><Code>SlowDown</Code></Error>` body instead of
    /// the expected `<DeleteResult>`. The `object_store` S3 client only retries
    /// on the HTTP status, so a 200-with-error body slips past its retry loop
    /// and then fails XML deserialisation (`unknown variant `Code``), surfacing
    /// as a non-retryable error even though *nothing was deleted* (#5).
    ///
    /// We therefore retry the whole bulk delete ourselves, with backoff, on a
    /// detected throttle; once those retries are exhausted we fall back to
    /// per-key deletes, whose `503 SlowDown` is status-coded and so is retried
    /// by the store's own `RetryConfig`, side-stepping the bulk parse bug.
    pub(super) async fn delete_batches(&self, locations: Vec<Path>) -> Result<()> {
        /// Times to retry the whole bulk delete on a detected S3 throttle before
        /// falling back to per-key deletes.
        const MAX_BULK_THROTTLE_RETRIES: u32 = 5;

        if locations.is_empty() {
            return Ok(());
        }

        let mut attempt = 0u32;

        loop {
            match self.bulk_delete(locations.clone()).await {
                Ok(()) => return Ok(()),

                Err(err) if is_s3_throttle(&err) => {
                    if attempt >= MAX_BULK_THROTTLE_RETRIES {
                        warn!(
                            %err,
                            "bulk DeleteObjects still throttled after retries; falling back to per-key deletes"
                        );
                        return self.delete_each(locations).await;
                    }

                    let backoff = throttle_backoff(attempt);
                    warn!(%err, attempt, ?backoff, "S3 throttled DeleteObjects; backing off then retrying");
                    sleep(backoff).await;
                    attempt += 1;
                }

                Err(err) => return Err(err.into()),
            }
        }
    }

    /// Issue a single bulk `DeleteObjects` for `locations`.
    pub(super) async fn bulk_delete(
        &self,
        locations: Vec<Path>,
    ) -> Result<(), object_store::Error> {
        let stream = futures::stream::iter(locations.into_iter().map(Ok)).boxed();

        self.object_store
            .delete_stream(stream)
            .try_collect::<Vec<Path>>()
            .await
            .map(|_| ())
    }

    /// Delete each location individually, ignoring already-absent objects. Used
    /// as a throttle fallback: a single-object DELETE returns a real `503`
    /// status that the store's `RetryConfig` retries.
    pub(super) async fn delete_each(&self, locations: Vec<Path>) -> Result<()> {
        /// Bounded concurrency for the per-key fallback. A purely sequential
        /// pass would issue up to a full `DeleteObjects` batch (1000) of
        /// round-trips back-to-back; a small fan-out makes progress without
        /// re-creating the request burst that tripped the throttle.
        const DELETE_EACH_CONCURRENCY: usize = 16;

        let object_store = &self.object_store;

        futures::stream::iter(locations)
            .map(|location| async move {
                match object_store.delete(&location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(err) => Err(Error::from(err)),
                }
            })
            .buffer_unordered(DELETE_EACH_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await
            .map(|_| ())
    }

    pub(super) async fn get<V>(&self, location: &Path) -> Result<(V, Version)>
    where
        V: DeserializeOwned,
    {
        let get_result = self.object_store.get(location).await?;
        let meta = get_result.meta.clone();

        let payload = get_result
            .bytes()
            .await
            .map_err(Into::into)
            .and_then(|encoded| serde_json::from_reader(&encoded[..]).map_err(Error::from))?;

        Ok((payload, meta.into()))
    }

    pub(super) async fn put<V>(
        &self,
        location: &Path,
        value: V,
        attributes: Attributes,
        update_version: Option<UpdateVersion>,
    ) -> Result<PutResult, UpdateError<V>>
    where
        V: PartialEq + Serialize + DeserializeOwned + Debug,
    {
        debug!(%location, ?attributes, ?update_version, ?value);

        let options = PutOptions {
            mode: update_version.map_or(PutMode::Create, PutMode::Update),
            attributes,
            ..Default::default()
        };

        let payload = serde_json::to_vec(&value)
            .map(Bytes::from)
            .map(PutPayload::from)?;

        match self
            .object_store
            .put_opts(location, payload, options)
            .await
            .inspect_err(|error| debug!(%location, ?error))
        {
            Ok(put_result) => Ok(put_result),

            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => {
                // The re-read hands the caller the winner's value so the retry
                // can re-apply onto it. It can also find nothing: the object was
                // deleted between the failed CAS and this read — a member the
                // session sweep reaped, an assignment
                // `delete_group_assignments_before` swept — and *that is an
                // answer*, not a fault (#431).
                //
                // It used to be `?`, so the 404 propagated as a raw
                // `ObjectStore` error: not `Outdated`, so no retry loop absorbed
                // it, and `Severity::Failure` at the boundary, so the connection
                // ended with **no response written**. A Kafka client cannot
                // retry an error code it never received; it reconnects and
                // replays, which is the #219 wedge shape. Measured at ~17/h
                // across five of ten replicas on `1.0.0-alpha.4`, every one of
                // them a `JoinGroup`/`Heartbeat`-class call.
                let Some((current, version)) = Self::absent_is_none(self.get(location).await)
                    .inspect_err(|error| error!(%location, ?error))?
                else {
                    CONDITIONAL_PUT_VANISHED.add(1, &[KeyValue::new("class", key_class(location))]);
                    debug!(%location, ?value, "lost the CAS to an object that was then deleted");

                    return Err(UpdateError::Vanished);
                };

                debug!(%location, ?value, ?current);

                Err(UpdateError::Outdated {
                    current: Box::new(current),
                    version,
                })
            }

            Err(otherwise) => Err(otherwise.into()),
        }
    }
}
