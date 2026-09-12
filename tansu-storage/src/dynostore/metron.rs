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

//! The metering decorator every engine's object store is wrapped in, and the
//! classification of an `object_store::Error` it counts outcomes by.

use super::*;

/// Backoff (with jitter) before retrying a throttled bulk delete: 0.5s, 1s, 2s,
/// 4s, 8s … capped at 30s, plus up to 50% jitter to desynchronise replicas that
/// were throttled at the same instant.
pub(crate) fn throttle_backoff(attempt: u32) -> Duration {
    let base_ms = 500u64.saturating_mul(1 << attempt.min(6)).min(30_000);
    let jitter = rng().random_range(0..=base_ms / 2);
    Duration::from_millis(base_ms + jitter)
}

/// Backoff before re-deriving a segment sequence whose create-CAS was lost to a
/// peer writer (#157). N stateless replicas flush the same prefix concurrently;
/// without this every loser immediately re-LISTs and re-PUTs in lockstep, which
/// both amplifies requests on the busiest prefix and lets one writer lose its
/// whole attempt budget to the same peers. Deliberately short and heavily
/// jittered — 1ms, 2ms, 4ms, 8ms, 16ms (capped) plus up to 100% jitter, so the
/// whole 64-attempt budget adds well under a second of produce latency while the
/// jitter desynchronises the racers. Orders of magnitude below
/// [`throttle_backoff`]: this is arbitration between peers, not a throttled
/// bucket.
pub(crate) fn cas_conflict_backoff(attempt: usize) -> Duration {
    let base_ms = 1u64 << attempt.min(4);
    let jitter = rng().random_range(0..=base_ms);
    Duration::from_millis(base_ms + jitter)
}

/// True if `error` looks like an S3 request-rate throttle on a multi-object
/// delete — either a surfaced `SlowDown`, or the `object_store` deserialisation
/// failure that a `200`-with-`<Error>` throttle body produces (the response is
/// `<Error><Code>SlowDown</Code></Error>` rather than `<DeleteResult>`, so the
/// parser reports `unknown variant `Code``). See [`DynoStore::delete_batches`].
pub(crate) fn is_s3_throttle(error: &object_store::Error) -> bool {
    let mut current: Option<&dyn std::error::Error> = Some(error);

    while let Some(err) = current {
        let text = err.to_string();

        if text.contains("SlowDown")
            || text.contains("unknown variant `Code`")
            || text.contains("invalid DeleteObjects response")
        {
            return true;
        }

        current = err.source();
    }

    false
}

/// True if `error` is a `429 Too Many Requests` — on `gs://`, the only shape a
/// request-rate throttle takes (#519).
///
/// GCS throttles a bucket that is still redistributing key ranges, and a single
/// object name written more than once a second, and answers both with a
/// retryable `429 rateLimitExceeded`. Neither was distinguishable in
/// [`object_store_error_name`]: `object_store` maps every status it has no
/// variant for onto `Error::Generic` (`client/retry.rs:159-186`), so an
/// exhausted 429 arrived as `Generic { store: "GCS" }` and was labelled
/// `reason="otherwise"` — which is where #519's retry-budget split cannot be
/// validated from, since the two budgets are told apart by `class` on a
/// `reason="throttle"` series.
///
/// Text-matched, like [`is_s3_throttle`], and for the same reason and with the
/// same trade-off (#284). Two markers: the status line, because that is what
/// `object_store` formats into the error (`StatusCode`'s own `Display` is
/// `"429 Too Many Requests"`), and Google's reason string, which the response
/// body carries.
///
/// Deliberately *not* folded into [`is_s3_throttle`], which is not merely a
/// classifier: it also drives `delete_batches`' retry-the-bulk-delete-then-fall-
/// back-to-per-key loop, and on GCS both halves of that loop are already
/// per-object deletes, so there would be nothing to fall back to.
pub(crate) fn is_too_many_requests(error: &object_store::Error) -> bool {
    let mut current: Option<&dyn std::error::Error> = Some(error);

    while let Some(err) = current {
        let text = err.to_string();

        if text.contains("429 Too Many Requests") || text.contains("rateLimitExceeded") {
            return true;
        }

        current = err.source();
    }

    false
}

/// True if `error` looks like a request that ran out of time rather than one the
/// store answered.
///
/// `object_store` has no timeout variant — a connect, read or overall-deadline
/// expiry arrives as `Generic` wrapping the transport error — so this walks the
/// source chain for the text, as [`is_s3_throttle`] does. Same trade-off: coupled
/// to wording that could change under us, and the alternative is not
/// distinguishing a timeout at all (#284).
pub(crate) fn is_timeout(error: &object_store::Error) -> bool {
    let mut current: Option<&dyn std::error::Error> = Some(error);

    while let Some(err) = current {
        let text = err.to_string().to_ascii_lowercase();

        if text.contains("timed out")
            || text.contains("timeout")
            || text.contains("deadline has elapsed")
        {
            return true;
        }

        current = err.source();
    }

    false
}

/// The `reason` label for an object-store failure.
///
/// The interesting failures used to collapse into a single `otherwise` bucket, so
/// a 503 SlowDown, a DNS failure and a TLS reset were indistinguishable in metrics
/// — during an S3 event, the one label that would make the error counter alertable
/// was the one missing (#284). Throttles and timeouts are now named; the throttle
/// signal in particular existed only in logs, via `is_s3_throttle`.
///
/// The two text-matched arms are checked *after* the structured variants, so a
/// `NotFound` whose source happens to mention a timeout is still `not_found`. They
/// are guards on the fallthrough rather than a pre-match for the same reason.
pub(crate) fn object_store_error_name(error: &object_store::Error) -> &'static str {
    match error {
        object_store::Error::Precondition { .. } => "pre_condition",

        object_store::Error::AlreadyExists { .. } => "already_exists",

        object_store::Error::NotModified { .. } => "not_modified",

        object_store::Error::NotFound { .. } => "not_found",

        // Both shapes under one label: S3's `503 SlowDown` and GCS's `429
        // rateLimitExceeded` are the same event and no backend emits both, so
        // `reason="throttle"` stays the series it already was and now covers
        // `gs://` too. Which of GCS's two rate limits was hit is read off
        // `class`, not off `reason` — see `gcs/retry.rs`.
        throttled if is_s3_throttle(throttled) || is_too_many_requests(throttled) => "throttle",

        timed_out if is_timeout(timed_out) => "timeout",

        otherwise => {
            debug!(?otherwise);
            "otherwise"
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Metron<O> {
    request_duration: Histogram<u64>,

    /// Metered request outcomes that are not a body, labelled by method, reason
    /// and — for a request that addresses a key — key class (#167).
    ///
    /// The class is what makes the `not_modified` and `not_found` populations
    /// attributable *at the layer that bills*: this wrapper sits under the
    /// metadata cache, so it counts only the round trips that actually left the
    /// process. Without it, the 304 plane could only be inferred from
    /// `tansu_objectstore_cache_outcomes` misses by class — and that series
    /// conflates two populations, since a miss is recorded whenever no etag is
    /// memoized, whether the caller presented one (a revalidation, which the
    /// store answers `304`) or not (a body read, e.g. a ranged segment GET).
    /// That inference was right about `watermark` and `topic_metadata` and wrong
    /// about `segment`, which carries no `if_none_match` and is never revalidated
    /// at all.
    ///
    /// Deliberately not added to `request_duration`: a class label multiplies a
    /// bucketed histogram by the number of classes, and "which keys buy
    /// unchanged/absent" is a counting question.
    request_error: Counter<u64>,

    cluster: String,
    object_store: O,
}

impl<O> Display for Metron<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metron").finish()
    }
}

impl<O> Metron<O>
where
    O: ObjectStore,
{
    pub(crate) fn new(object_store: O, cluster: &str) -> Self {
        Self {
            cluster: cluster.into(),
            // Labelled by `class` as well as `method` (#409). The error counter
            // below has carried the key class since #203 and this had not, so
            // "the brokers are slow" — a 350 ms `get_opts` mean against the
            // maintainers' 24 ms on the same bucket — could not be narrowed to
            // "slow reading *what*". One label, and it separates a request
            // waiting on the wire from a plane with a pathological caller:
            // `list_with_delimiter` averages 4.5 s at 0.3 req/s on that fleet.
            request_duration: METER
                .u64_histogram("tansu_object_store_request_duration")
                .with_unit("ms")
                .with_description("The object store request latencies in milliseconds")
                .build(),
            request_error: METER
                .u64_counter("tansu_object_store_request_error")
                .with_description("The object store request errors")
                .build(),

            object_store,
        }
    }

    /// Make a listing stream visible to the request metrics (#165).
    ///
    /// The two streaming list methods were forwarded uninstrumented, so the
    /// per-method request metric reported ~0.5 LIST/s while the bucket meter
    /// showed ~1,200/s — the tier-1 plane that dominates the bill was invisible,
    /// and every LIST reduction claimed from this counter was unverified.
    ///
    /// A listing is not one request: the store pages it, returning at most
    /// [`LIST_PAGE_KEYS`] keys per `ListObjectsV2`, and the meter counts pages.
    /// So one sample is recorded for the call itself — including a listing that
    /// yields nothing, which is still a metered request, and is exactly the shape
    /// the legacy-records probe issues (#166) — and one more each time the objects
    /// streamed past cross a page boundary. That makes the sample *count* line up
    /// with the meter, which is the point of the metric.
    ///
    /// Only the page-boundary samples carry a real latency (time since the
    /// previous page); the call sample records `0`, since a stream's first request
    /// is still in flight when it is handed back. `delete_stream` already reports
    /// its per-object samples the same way.
    fn instrument_listing(
        &self,
        method: &'static str,
        class: &'static str,
        inner: BoxStream<'static, Result<ObjectMeta, object_store::Error>>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        /// Keys per `ListObjectsV2` response, i.e. per metered LIST request.
        const LIST_PAGE_KEYS: u64 = 1_000;

        let attributes = vec![
            KeyValue::new("method", method),
            KeyValue::new("cluster", self.cluster.clone()),
            KeyValue::new("class", class),
        ];
        let request_duration = self.request_duration.clone();
        let request_error = self.request_error.clone();

        request_duration.record(0, attributes.as_ref());

        let mut yielded = 0u64;
        let mut page_started = SystemTime::now();

        Box::pin(inner.inspect(move |result| match result {
            Ok(_) => {
                yielded += 1;

                if yielded.is_multiple_of(LIST_PAGE_KEYS) {
                    request_duration.record(
                        page_started
                            .elapsed()
                            .map_or(0, |elapsed| elapsed.as_millis() as u64),
                        attributes.as_ref(),
                    );
                    page_started = SystemTime::now();
                }
            }

            Err(err) => {
                debug!(?err, method);

                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", "prefix"),
                ];
                additional.extend(attributes.iter().cloned());
                request_error.add(1, &additional[..]);
            }
        }))
    }
}

#[async_trait]
impl<O> ObjectStore for Metron<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        debug!(%location, ?opts);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "put_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
            KeyValue::new("class", key_class(location)),
        ];

        self.object_store
            .put_opts(location, payload, opts.clone())
            .await
            .inspect(|put_result| {
                debug!(%location, etag = ?put_result.e_tag, version = ?put_result.version);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(%location, opts = ?opts, err = ?err);

                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", key_class(location)),
                ];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        debug!(%location, ?opts);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "put_multipart_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
            KeyValue::new("class", key_class(location)),
        ];

        self.object_store
            .put_multipart_opts(location, opts)
            .await
            .inspect(|_put_result| {
                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", key_class(location)),
                ];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    // `debug`, and without `ret` (#428): see the note on `Cache::get_opts`,
    // which sat above this one carrying the same annotation. Between them every
    // GET on every backend emitted two formatted `INFO` events.
    #[instrument(level = "debug", skip_all, fields(%location))]
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        debug!(?options);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "get_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
            KeyValue::new("class", key_class(location)),
        ];

        self.object_store
            .get_opts(location, options.clone())
            .await
            .inspect(|get_result| {
                debug!(meta = ?get_result.meta);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(?err);

                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", key_class(location)),
                ];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        debug!("delete_stream");

        let cluster = self.cluster.clone();
        let request_duration = self.request_duration.clone();
        let request_error = self.request_error.clone();

        let inner = self.object_store.delete_stream(locations);

        Box::pin(inner.inspect(move |result| {
            let attributes = vec![
                KeyValue::new("method", "delete_stream"),
                KeyValue::new("cluster", cluster.clone()),
            ];

            if let Err(err) = result {
                // The stream yields the key only on success, so a failed delete
                // is classed from the error when it carries a path — the
                // `DeleteObjects` per-key failures do.
                let class = match err {
                    object_store::Error::NotFound { path, .. } => key_class(&Path::from(&path[..])),
                    _ => "other",
                };

                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", class),
                ];
                additional.extend(attributes);
                request_error.add(1, &additional[..]);
            } else {
                request_duration.record(0, attributes.as_ref());
            }
        }))
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix);

        self.instrument_listing(
            "list",
            prefix.map_or("other", key_class),
            self.object_store.list(prefix),
        )
    }

    // Forward `list_with_offset` (S3 `start-after`) so a tail-offset scan reads
    // only the partition tail rather than the default full-`list` downgrade.
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix, ?offset);

        self.instrument_listing(
            "list_with_offset",
            prefix.map_or("other", key_class),
            self.object_store.list_with_offset(prefix, offset),
        )
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        debug!(?prefix);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "list_with_delimiter"),
            KeyValue::new("cluster", self.cluster.clone()),
            KeyValue::new("class", prefix.map_or("other", key_class)),
        ];

        if let Some(prefix) = prefix {
            attributes.push(KeyValue::new("prefix", prefix.to_string()));
        }

        self.object_store
            .list_with_delimiter(prefix)
            .await
            .inspect(|_list_result| {
                debug!(?prefix);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(?prefix, err = ?err);

                // A listing addresses a prefix, not a key: the same stand-in the
                // cache metrics use, so `sum by (class)` covers every series
                // rather than leaving listings in an unlabelled bucket.
                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", "prefix"),
                ];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        debug!(%from, %to, ?opts);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "copy_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
            KeyValue::new("class", key_class(to)),
        ];

        self.object_store
            .copy_opts(from, to, opts)
            .await
            .inspect(|_| {
                debug!(%from, %to);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(%from, %to, err = ?err);

                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", key_class(from)),
                ];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }
}

#[cfg(test)]
mod throttle_tests {
    use super::{
        Duration, cas_conflict_backoff, is_s3_throttle, is_too_many_requests,
        object_store_error_name, throttle_backoff,
    };

    fn generic_s3(source: &'static str) -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: source.into(),
        }
    }

    fn generic_gcs(source: &'static str) -> object_store::Error {
        object_store::Error::Generic {
            store: "GCS",
            source: source.into(),
        }
    }

    #[test]
    fn slowdown_503_body_is_throttle() {
        // A genuine 503 whose body is surfaced after retries are exhausted.
        assert!(is_s3_throttle(&generic_s3(
            "Status { status: 503, body: \"<Error><Code>SlowDown</Code></Error>\" }"
        )));
    }

    #[test]
    fn parse_error_from_200_throttle_body_is_throttle() {
        // S3 returned 200 with <Error><Code>SlowDown</Code></Error>; object_store
        // tried to parse it as <DeleteResult> and tripped on the <Code> element.
        assert!(is_s3_throttle(&generic_s3(
            "Got invalid DeleteObjects response: unknown variant `Code`, expected `Deleted` or `Error`"
        )));
    }

    /// A GCS throttle is a `429`, and it reaches the `throttle` label (#519).
    ///
    /// Both of GCS's rate limits answer this way — the per-bucket ramp and the
    /// per-object write cap — which is why the two retry budgets are told apart
    /// by the `class` label and not by `reason`.
    #[test]
    fn a_gcs_429_is_throttle() {
        // What an exhausted budget actually surfaces: `object_store` has no
        // variant for 429, so it is `Generic` wrapping the formatted status
        // line and body.
        let ramp = generic_gcs(
            "Server returned non-2xx status code: 429 Too Many Requests: \
             {\"error\":{\"code\":429,\"message\":\"The object exceeded its rate limit\",\
             \"errors\":[{\"reason\":\"rateLimitExceeded\"}]}}",
        );

        assert!(is_too_many_requests(&ramp));
        assert_eq!("throttle", object_store_error_name(&ramp));

        // The reason string on its own, for a 429 whose status line is not in
        // the text this walk can reach.
        assert!(is_too_many_requests(&generic_gcs(
            "Error performing PUT in 15s, after 5 retries - rateLimitExceeded"
        )));

        // And it is not the S3 predicate, which drives the bulk-delete
        // fall-back rather than the label.
        assert!(!is_s3_throttle(&ramp));
    }

    #[test]
    fn unrelated_errors_are_not_throttle() {
        assert!(!is_s3_throttle(&object_store::Error::NotFound {
            path: "x".into(),
            source: "missing".into(),
        }));
        assert!(!is_s3_throttle(&generic_s3("connection reset by peer")));

        // The 429 walk must not claim an unrelated error that merely mentions a
        // number, or a `412` a lost CAS produces — which `object_store` does
        // not retry and this must not relabel.
        assert!(!is_too_many_requests(&generic_gcs(
            "Server returned non-2xx status code: 412 Precondition Failed: "
        )));
        assert!(!is_too_many_requests(&generic_gcs(
            "Object at location x has size 429"
        )));
    }

    /// #284's acceptance: a throttle is distinguishable from a 404 and from a
    /// transport failure in the `reason` label, not just in the logs.
    #[test]
    fn throttle_and_timeout_have_their_own_reason() {
        assert_eq!(
            "throttle",
            object_store_error_name(&generic_s3(
                "Status { status: 503, body: \"<Error><Code>SlowDown</Code></Error>\" }"
            ))
        );

        assert_eq!(
            "timeout",
            object_store_error_name(&generic_s3("error sending request: operation timed out"))
        );

        // The two failures the report names as currently indistinguishable from a
        // throttle. A TLS reset stays in the fallthrough — naming it is not what
        // #284 asks for — but it must not be *mislabelled* as one of the two.
        assert_eq!(
            "not_found",
            object_store_error_name(&object_store::Error::NotFound {
                path: "x".into(),
                source: "missing".into(),
            })
        );
        assert_eq!(
            "otherwise",
            object_store_error_name(&generic_s3("connection reset by peer"))
        );
    }

    /// A structured variant wins over the text match, so an error the store
    /// actually answered is never relabelled by wording in its source chain.
    #[test]
    fn a_structured_variant_is_not_reclassified_by_its_source_text() {
        assert_eq!(
            "not_found",
            object_store_error_name(&object_store::Error::NotFound {
                path: "x".into(),
                source: "request timed out before the object was found".into(),
            })
        );
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        // attempt 0: base 500ms + up to 50% jitter => [500, 750]ms
        let first = throttle_backoff(0).as_millis();
        assert!((500..=750).contains(&first), "{first}");

        // large attempt: base capped at 30s + up to 50% jitter => [30s, 45s]
        let capped = throttle_backoff(20).as_millis();
        assert!((30_000..=45_000).contains(&capped), "{capped}");
    }

    /// #157: the create-CAS conflict yield must stay in the millisecond band —
    /// its job is to desynchronise peer writers, not to wait out a throttle — so
    /// the whole 64-attempt budget costs well under a second of produce latency.
    #[test]
    fn cas_conflict_backoff_is_short_capped_and_jittered() {
        // attempt 0: base 1ms + up to 100% jitter => [1, 2]ms
        let first = cas_conflict_backoff(0).as_millis();
        assert!((1..=2).contains(&first), "{first}");

        // capped from attempt 4 on: base 16ms + up to 100% jitter => [16, 32]ms
        for attempt in [4, 5, 64, 1_000] {
            let capped = cas_conflict_backoff(attempt).as_millis();
            assert!((16..=32).contains(&capped), "attempt {attempt}: {capped}");
        }

        // The whole budget, at the cap, stays sub-second.
        assert!(64 * cas_conflict_backoff(64) < Duration::from_secs(3));
    }
}
