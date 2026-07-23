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

use std::time::SystemTime;

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, ErrorCode, FetchRequest, FetchResponse, IsolationLevel,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::{
        AbortedTransaction, EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch,
        PartitionData, SnapshotId,
    },
    metadata_response::MetadataResponseTopic,
    record::deflated::{Batch, Frame},
};
use tokio::time::{Duration, Instant};
use tracing::{debug, error, instrument};

use crate::{Error, Result, Storage, Topition};

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
///     MapStateLayer::new(|_| storage).into_layer(CreateTopicsService::default())
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
    #[instrument(skip(self,ctx,min_bytes,isolation,fetch_partition), fields(partition = fetch_partition.partition))]
    async fn fetch_partition<G>(
        &self,
        ctx: &Context<G>,
        max_wait: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topic: &str,
        fetch_partition: &FetchPartition,
    ) -> Result<(PartitionData, i64)>
    where
        G: Storage,
    {
        let started_at = Instant::now();

        let partition_index = fetch_partition.partition;
        let tp = Topition::new(topic, partition_index);

        let mut batches = Vec::new();

        let mut offset = fetch_partition.fetch_offset;

        loop {
            if *max_bytes == 0 {
                break;
            }

            debug!(offset);

            let mut fetched = ctx
                .state()
                .fetch(
                    &tp,
                    offset,
                    min_bytes,
                    *max_bytes,
                    isolation,
                    max_wait.saturating_sub(started_at.elapsed()),
                )
                .await
                .inspect(|r| debug!(?tp, ?offset, ?r))
                .inspect_err(|error| error!(?tp, ?error))?;

            *max_bytes =
                u32::try_from(fetched.byte_size()).map(|bytes| max_bytes.saturating_sub(bytes))?;

            debug!(?offset, ?fetched, max_bytes);

            if fetched.is_empty() || fetched.first().is_some_and(|batch| batch.record_count == 0) {
                break;
            }

            if let Some(latest) = fetched
                .iter()
                .map(|batch| batch.base_offset + batch.record_count as i64)
                .max()
                .inspect(|latest| debug!(latest))
            {
                offset = latest;
            }

            batches.append(&mut fetched);
        }

        // Isolation-aware: read-uncommitted resolves its response offsets from
        // the in-memory high-watermark hint + cached log start, without reading
        // the cluster-wide `meta.json` object — keeping this hot path off the
        // single meta key's request ceiling (#109). Read-committed still reads
        // meta for the last-stable offset and aborted-transaction list.
        let offset_stage = ctx
            .state()
            .offset_stage_at(&tp, isolation)
            .await
            .inspect_err(|error| error!(?error, ?tp))?;

        // What the long-poll should watch this partition from: past this
        // offset means "new records for this fetch". Read-uncommitted watches
        // the offset the loop drained to. Read-committed drains only to the
        // last-stable offset, which lags the high watermark under an open
        // transaction — watching the drained offset there would
        // wake-and-refetch in a hot loop — so it watches the response high
        // watermark instead.
        let watch_from = if isolation == IsolationLevel::ReadUncommitted {
            offset
        } else {
            offset_stage.high_watermark()
        };

        Ok((
            PartitionData::default()
                .partition_index(partition_index)
                .error_code(ErrorCode::None.into())
                .high_watermark(offset_stage.high_watermark())
                .last_stable_offset(Some(offset_stage.last_stable()))
                .log_start_offset(Some(offset_stage.log_start()))
                .diverging_epoch(None)
                .current_leader(None)
                .snapshot_id(None)
                // Aborted transactions overlapping the log, so a read-committed
                // consumer filters out aborted records below the LSO (#81). Only
                // read-committed fetches receive the list (Kafka's contract); empty
                // for read-uncommitted and for non-transactional workloads.
                .aborted_transactions(Some(if isolation == IsolationLevel::ReadCommitted {
                    offset_stage
                        .aborted()
                        .iter()
                        .map(|(producer_id, first_offset)| {
                            AbortedTransaction::default()
                                .producer_id(*producer_id)
                                .first_offset(*first_offset)
                        })
                        .collect()
                } else {
                    Vec::new()
                }))
                .preferred_read_replica(Some(-1))
                .records(if batches.is_empty() {
                    None
                } else {
                    Some(Frame { batches })
                }),
            watch_from,
        ))
        .inspect(|(r, watch_from)| debug!(?r, watch_from, elapsed = ?started_at.elapsed()))
    }

    fn unknown_topic_response(&self, fetch: &FetchTopic) -> Result<FetchableTopicResponse> {
        Ok(FetchableTopicResponse::default()
            .topic(fetch.topic.clone())
            .topic_id(Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
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

    #[instrument(skip(self, ctx, isolation, topics))]
    pub(crate) async fn fetch<G>(
        &self,
        ctx: &Context<G>,
        max_wait: Duration,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topics: &[FetchTopic],
    ) -> Result<Vec<FetchableTopicResponse>>
    where
        G: Storage,
    {
        debug!(?isolation, ?topics);

        if topics.is_empty() {
            return Ok(vec![]);
        }

        // Resolve topic metadata ONCE for the whole request, not on every
        // long-poll iteration (#109). The name/id mapping is stable for the
        // fetch's lifetime, so re-reading `topic-metadata/<name>.json` each
        // iteration was pure per-poll S3 traffic. `known` is `None` for an
        // unknown topic (its error response is likewise stable).
        let mut resolved: Vec<ResolvedTopic<'_>> = Vec::with_capacity(topics.len());
        for fetch in topics.iter() {
            let metadata = ctx.state().metadata(Some(&[fetch.into()])).await?;

            let known = if let Some(MetadataResponseTopic {
                topic_id,
                name: Some(name),
                ..
            }) = metadata.topics().first()
            {
                Some((name.clone(), topic_id.to_owned()))
            } else {
                None
            };

            resolved.push(ResolvedTopic { fetch, known });
        }

        let started_at = SystemTime::now();
        let mut responses = vec![];
        let mut watch: Vec<(Topition, i64)> = vec![];
        let mut iteration = 0;
        let mut bytes = 0;

        while !max_wait.saturating_sub(started_at.elapsed()?).is_zero() && bytes <= min_bytes {
            debug!(?bytes, remaining = ?max_wait.saturating_sub(started_at.elapsed()?));

            responses.clear();
            watch.clear();

            let fetch_started_at = SystemTime::now();
            for topic in resolved.iter() {
                let fetch_response = if let Some((name, topic_id)) = topic.known.as_ref() {
                    let mut partitions = Vec::new();

                    for fetch_partition in topic.fetch.partitions.as_ref().unwrap_or(&Vec::new()) {
                        let remaining = max_wait.saturating_sub(started_at.elapsed()?);

                        let (partition, watch_from) = self
                            .fetch_partition(
                                ctx,
                                remaining,
                                min_bytes,
                                max_bytes,
                                isolation,
                                name,
                                fetch_partition,
                            )
                            .await?;

                        watch.push((
                            Topition::new(name.as_str(), fetch_partition.partition),
                            watch_from,
                        ));
                        partitions.push(partition);
                    }

                    FetchableTopicResponse::default()
                        .topic(topic.fetch.topic.to_owned())
                        .topic_id(topic_id.to_owned())
                        .partitions(Some(partitions))
                } else {
                    self.unknown_topic_response(topic.fetch)?
                };

                responses.push(fetch_response);
            }

            bytes += u32::try_from(responses.byte_size())?;

            let remaining = max_wait.saturating_sub(started_at.elapsed()?);

            debug!(?iteration, ?max_wait, ?remaining, ?bytes, ?min_bytes);

            if bytes > min_bytes {
                break;
            }

            {
                let fetch_elapsed = fetch_started_at.elapsed()?;

                // we have some data to return to the client,
                // we haven't met the minimum size requirement,
                // but we don't have enough (estimated) time remaining to do another round
                if !responses.is_empty() && remaining < fetch_elapsed {
                    debug!(responses.len = responses.len(), ?remaining, ?fetch_elapsed);
                    break;
                }
            }

            // Park until new records may be readable on a watched partition
            // (wake-on-write) rather than polling on a blind half-wait sleep;
            // a storage engine without a produce signal falls back to exactly
            // that sleep via the trait default. Waking also arms the
            // fetch-triggered flush, so a pending coalesce buffer flushes at
            // the fetch-flush floor instead of waiting out a wide linger.
            ctx.state().await_produce(&watch, remaining).await?;

            iteration += 1;
        }

        Ok(responses)
    }
}

/// A `FetchTopic` paired with its metadata resolved once per request (#109):
/// `known` is `Some((name, topic_id))` for a known topic, `None` for an unknown
/// one.
struct ResolvedTopic<'a> {
    fetch: &'a FetchTopic,
    known: Option<(String, Option<[u8; 16]>)>,
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
        let started_at = SystemTime::now();

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
                &ctx,
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
            // NodeEndpoints is a tagged field (tag 0) only valid from Fetch v16
            // (KIP-951). Emitting it (even as an empty list) on a response whose
            // negotiated version is < 16 makes clients fail to decode with
            // "Tag 0 is not valid for version <V>", killing e.g. Kafka Connect's
            // KafkaBasedLog work thread (#7). It carries no information for us
            // (no leader endpoints to advertise), so leave it unset.
            .node_endpoints(None)
            .responses(responses))
        .inspect(|r| debug!(?r, elapsed = ?started_at.elapsed().ok()))
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
