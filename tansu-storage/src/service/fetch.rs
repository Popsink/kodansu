// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::time::{Duration, Instant};

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, ErrorCode, FetchRequest, FetchResponse, IsolationLevel, NULL_TOPIC_ID,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::{
        EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
    },
    metadata_response::MetadataResponseTopic,
    record::deflated::{Batch, Frame},
};
use tokio::time::{sleep, timeout};
use tracing::{debug, error, instrument, warn};

use crate::{Error, Result, Storage, Topition};

/// Small grace period added on top of `max_wait_ms` when wrapping storage
/// calls in a [`tokio::time::timeout`]. The grace lets a near-deadline call
/// finish if it is just about done, instead of forcing a cancel + retry.
const FETCH_DEADLINE_GRACE: Duration = Duration::from_millis(250);

/// A [`Service`] using [`Storage`] as [`Context`] taking [`FetchRequest`] returning [`FetchResponse`].
/// ```
/// use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
/// use tansu_sans_io::{
///     CreateTopicsRequest, ErrorCode, FetchRequest,
///     create_topics_request::CreatableTopic,
///     fetch_request::{FetchPartition, FetchTopic},
/// };
/// use tansu_storage::{CreateTopicsService, Error, FetchService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const CLUSTER_ID: &str = "tansu";
/// const NODE_ID: i32 = 111;
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
///
/// let storage = StorageContainer::builder()
///     .cluster_id(CLUSTER_ID)
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://tansu/")?)
///     .build()
///     .await?;
///
/// let create_topic = {
///     let storage = storage.clone();
///     MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
/// };
///
/// let name = "abcba";
///
/// let response = create_topic
///     .serve(
///         Context::default(),
///         CreateTopicsRequest::default()
///             .topics(Some(vec![
///                 CreatableTopic::default()
///                     .name(name.into())
///                     .num_partitions(5)
///                     .replication_factor(3)
///                     .assignments(Some([].into()))
///                     .configs(Some([].into())),
///             ]))
///             .validate_only(Some(false)),
///     )
///     .await?;
///
/// let topics = response.topics.unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
///
/// let fetch = {
///     let storage = storage.clone();
///     MapStateLayer::new(|_| storage).into_layer(FetchService)
/// };
///
/// let partition = 0;
///
/// let response = fetch
///     .serve(
///         Context::default(),
///         FetchRequest::default()
///             .topics(Some(
///                 [FetchTopic::default()
///                     .topic(Some(name.into()))
///                     .partitions(Some(
///                         [FetchPartition::default().partition(partition)].into(),
///                     ))]
///                 .into(),
///             ))
///             .max_bytes(Some(0))
///             .max_wait_ms(5_000),
///     )
///     .await?;
///
/// let topics = response.responses.as_deref().unwrap_or_default();
/// assert_eq!(1, topics.len());
/// let partitions = topics[0].partitions.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(partitions[0].error_code)?
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FetchService;

impl ApiKey for FetchService {
    const KEY: i16 = FetchRequest::KEY;
}

impl FetchService {
    #[allow(clippy::too_many_arguments)]
    async fn fetch_partition<G>(
        &self,
        ctx: Context<G>,
        deadline: Instant,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topic: &str,
        fetch_partition: &FetchPartition,
    ) -> Result<PartitionData>
    where
        G: Storage,
    {
        debug!(?min_bytes, ?max_bytes, ?isolation, ?fetch_partition);

        let partition_index = fetch_partition.partition;
        let tp = Topition::new(topic, partition_index);

        let mut batches = Vec::new();
        let mut offset = fetch_partition.fetch_offset;

        loop {
            if *max_bytes == 0 {
                break;
            }

            // Cooperative deadline check: don't start another storage call if
            // we have already overrun the client's max_wait_ms. We still emit
            // a (possibly empty) PartitionData below so the request always
            // gets a response.
            let now = Instant::now();
            if now >= deadline {
                debug!(?tp, ?offset, "fetch_partition deadline reached");
                break;
            }
            let remaining = deadline.saturating_duration_since(now) + FETCH_DEADLINE_GRACE;

            debug!(offset);

            let fetch_call = ctx
                .state()
                .fetch(&tp, offset, min_bytes, *max_bytes, isolation);

            let mut fetched = match timeout(remaining, fetch_call).await {
                Ok(Ok(r)) => {
                    debug!(?tp, ?offset, ?r);
                    r
                }
                Ok(Err(error)) => {
                    error!(?tp, ?error);
                    return Err(error);
                }
                Err(_elapsed) => {
                    warn!(?tp, ?offset, "storage fetch timed out, returning partial");
                    break;
                }
            };

            *max_bytes =
                u32::try_from(fetched.byte_size()).map(|bytes| max_bytes.saturating_sub(bytes))?;

            offset += fetched
                .iter()
                .map(|batch| batch.record_count as i64)
                .sum::<i64>();

            debug!(?offset, ?fetched);

            if fetched.is_empty() || fetched.first().is_some_and(|batch| batch.record_count == 0) {
                break;
            } else {
                batches.append(&mut fetched);
            }
        }

        // Always return offset_stage information even when we ran out of time
        // mid-fetch; clients use high_watermark/last_stable_offset to keep
        // their own offsets in sync. Fall back to safe defaults if even this
        // call exceeds the deadline so the response can still be sent.
        let now = Instant::now();
        let stage_remaining = deadline.saturating_duration_since(now) + FETCH_DEADLINE_GRACE;
        let offset_stage_call = ctx.state().offset_stage(&tp);
        let offset_stage = match timeout(stage_remaining, offset_stage_call).await {
            Ok(Ok(stage)) => Some(stage),
            Ok(Err(error)) => {
                error!(?error, ?tp);
                return Err(error);
            }
            Err(_elapsed) => {
                warn!(?tp, "offset_stage timed out, returning empty");
                None
            }
        };

        let (high_watermark, last_stable, log_start) = offset_stage
            .map(|s| (s.high_watermark(), s.last_stable(), s.log_start()))
            .unwrap_or((offset, offset, -1));

        Ok(PartitionData::default()
            .partition_index(partition_index)
            .error_code(ErrorCode::None.into())
            .high_watermark(high_watermark)
            .last_stable_offset(Some(last_stable))
            .log_start_offset(Some(log_start))
            .diverging_epoch(None)
            .current_leader(None)
            .snapshot_id(None)
            .aborted_transactions(Some([].into()))
            .preferred_read_replica(Some(-1))
            .records(if batches.is_empty() {
                None
            } else {
                Some(Frame { batches })
            }))
        .inspect(|r| debug!(?r))
    }

    fn unknown_topic_response(&self, fetch: &FetchTopic) -> Result<FetchableTopicResponse> {
        // For v13+ requests the topic name field is gone — clients match
        // responses to requests by topic_id. Echo back whatever id the client
        // sent so it can pair this error with its request; only fall back to
        // NULL_TOPIC_ID when the client itself didn't supply one.
        let topic_id = fetch.topic_id.or(Some(NULL_TOPIC_ID));

        Ok(FetchableTopicResponse::default()
            .topic(fetch.topic.clone())
            .topic_id(topic_id)
            .partitions(fetch.partitions.as_ref().map(|partitions| {
                partitions
                    .iter()
                    .map(|partition| {
                        PartitionData::default()
                            .partition_index(partition.partition)
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .high_watermark(0)
                            .last_stable_offset(Some(0))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(Some(
                                EpochEndOffset::default().epoch(-1).end_offset(-1),
                            ))
                            .current_leader(Some(
                                LeaderIdAndEpoch::default().leader_id(0).leader_epoch(0),
                            ))
                            .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None)
                    })
                    .collect()
            })))
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_topic<G>(
        &self,
        ctx: Context<G>,
        deadline: Instant,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        fetch: &FetchTopic,
    ) -> Result<FetchableTopicResponse>
    where
        G: Storage,
    {
        debug!(?min_bytes, ?isolation, ?fetch);

        // Bound the metadata lookup so the broker can never be wedged by a
        // stalled metadata backend before it even reaches the partitions.
        let metadata_remaining =
            deadline.saturating_duration_since(Instant::now()) + FETCH_DEADLINE_GRACE;
        let metadata = match timeout(
            metadata_remaining,
            ctx.state().metadata(Some(&[fetch.into()])),
        )
        .await
        {
            Ok(Ok(m)) => m,
            Ok(Err(error)) => return Err(error),
            Err(_elapsed) => {
                warn!(?fetch, "metadata lookup timed out during fetch");
                return self.unknown_topic_response(fetch);
            }
        };

        if let Some(MetadataResponseTopic {
            topic_id,
            name: Some(name),
            ..
        }) = metadata.topics().first()
        {
            let mut partitions = Vec::new();

            for fetch_partition in fetch.partitions.as_ref().unwrap_or(&Vec::new()) {
                let partition = self
                    .fetch_partition(
                        ctx.clone(),
                        deadline,
                        min_bytes,
                        max_bytes,
                        isolation,
                        name,
                        fetch_partition,
                    )
                    .await?;

                partitions.push(partition);
            }

            Ok(FetchableTopicResponse::default()
                .topic(fetch.topic.to_owned())
                .topic_id(topic_id.to_owned())
                .partitions(Some(partitions)))
        } else {
            self.unknown_topic_response(fetch)
        }
    }

    pub(crate) async fn fetch<G>(
        &self,
        ctx: Context<G>,
        max_wait: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topics: &[FetchTopic],
    ) -> Result<Vec<FetchableTopicResponse>>
    where
        G: Storage,
    {
        debug!(?max_wait, ?min_bytes, ?isolation, ?topics);

        if topics.is_empty() {
            return Ok(vec![]);
        }

        let start = Instant::now();
        let deadline = start + max_wait;

        // Do at least one fetch attempt, even when max_wait is zero. This is
        // the long-poll loop: keep retrying until either we accumulate
        // min_bytes of data or the deadline is reached. Crucially, we keep
        // each attempt's responses (the previous loop cleared them, which
        // could throw away good data when the inner storage call ran into
        // the deadline mid-iteration).
        let original_max_bytes = *max_bytes;
        let mut responses: Vec<FetchableTopicResponse> = Vec::with_capacity(topics.len());

        loop {
            // Reset per-attempt budget so a partial read on iteration N
            // doesn't leave iteration N+1 with no headroom.
            let mut attempt_max_bytes = original_max_bytes;
            let mut attempt: Vec<FetchableTopicResponse> = Vec::with_capacity(topics.len());

            for fetch in topics {
                let response = self
                    .fetch_topic(
                        ctx.clone(),
                        deadline,
                        min_bytes,
                        &mut attempt_max_bytes,
                        isolation,
                        fetch,
                    )
                    .await?;
                attempt.push(response);
            }

            let attempt_bytes = u32::try_from(attempt.byte_size()).unwrap_or(u32::MAX);

            // Keep the better of the two: any non-empty attempt replaces the
            // running best. If this attempt found nothing but a previous one
            // did, hold onto the previous data.
            if attempt_bytes > 0
                || responses.is_empty()
                || u32::try_from(responses.byte_size()).unwrap_or(0) == 0
            {
                responses = attempt;
                *max_bytes = attempt_max_bytes;
            }

            if attempt_bytes >= min_bytes {
                break;
            }

            let now = Instant::now();
            if now >= deadline {
                break;
            }

            // Long-poll: wait a fraction of the remaining time before
            // retrying. Cap the slice so a tiny remainder doesn't busy-loop.
            let remaining = deadline.saturating_duration_since(now);
            let sleep_for = if remaining.as_millis() >= 250 {
                remaining / 2
            } else {
                remaining
            };

            if sleep_for.is_zero() {
                break;
            }

            sleep(sleep_for).await;
        }

        Ok(responses)
    }
}

impl<G> Service<G, FetchRequest> for FetchService
where
    G: Storage,
{
    type Response = FetchResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: FetchRequest,
    ) -> Result<Self::Response, Self::Error> {
        let responses = Some(if let Some(topics) = req.topics {
            let isolation_level = req
                .isolation_level
                .map_or(Ok(IsolationLevel::ReadUncommitted), |isolation| {
                    IsolationLevel::try_from(isolation)
                })?;

            let max_wait_ms = u64::try_from(req.max_wait_ms).map(Duration::from_millis)?;

            let min_bytes = u32::try_from(req.min_bytes)?;

            const DEFAULT_MAX_BYTES: u32 = 5 * 1024 * 1024;

            let mut max_bytes = req.max_bytes.map_or(Ok(DEFAULT_MAX_BYTES), |max_bytes| {
                u32::try_from(max_bytes).map(|max_bytes| max_bytes.min(DEFAULT_MAX_BYTES))
            })?;

            self.fetch(
                ctx,
                max_wait_ms,
                min_bytes,
                &mut max_bytes,
                isolation_level,
                topics.as_ref(),
            )
            .await?
        } else {
            vec![]
        });

        Ok(FetchResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(Some(ErrorCode::None.into()))
            .session_id(Some(0))
            .node_endpoints(Some([].into()))
            .responses(responses))
        .inspect(|r| debug!(?r))
    }
}

trait ByteSize {
    fn byte_size(&self) -> u64;
}

impl<T> ByteSize for Vec<T>
where
    T: ByteSize,
{
    fn byte_size(&self) -> u64 {
        self.iter().map(|item| item.byte_size()).sum()
    }
}

impl<T> ByteSize for Option<T>
where
    T: ByteSize,
{
    fn byte_size(&self) -> u64 {
        self.as_ref().map_or(0, |some| some.byte_size())
    }
}

impl ByteSize for Batch {
    fn byte_size(&self) -> u64 {
        self.record_data.len() as u64
    }
}

impl ByteSize for Frame {
    fn byte_size(&self) -> u64 {
        self.batches.byte_size()
    }
}

impl ByteSize for PartitionData {
    fn byte_size(&self) -> u64 {
        self.records.byte_size()
    }
}

impl ByteSize for FetchableTopicResponse {
    fn byte_size(&self) -> u64 {
        self.partitions.byte_size()
    }
}
