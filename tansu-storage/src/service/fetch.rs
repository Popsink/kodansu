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
    ApiKey, ErrorCode, FetchRequest, FetchResponse, IsolationLevel, NULL_TOPIC_ID,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::{
        AbortedTransaction, EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch,
        PartitionData, SnapshotId,
    },
    metadata_response::MetadataResponseTopic,
    record::deflated::{Batch, Frame},
};
use tokio::time::{Duration, Instant, sleep};
use tracing::{debug, error, instrument};

use crate::{Error, Parked, Result, Storage, Topition};

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
    ) -> Result<PartitionData>
    where
        G: Storage,
    {
        let started_at = Instant::now();

        let partition_index = fetch_partition.partition;
        let tp = Topition::new(topic, partition_index);

        let mut batches = Vec::new();

        // Set when storage reports this partition unservable (#290), so the
        // failure lands on this partition's response instead of the request's.
        let mut error_code = ErrorCode::None;

        let mut offset = fetch_partition.fetch_offset;

        loop {
            if *max_bytes == 0 {
                break;
            }

            debug!(offset);

            let fetched = ctx
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
                .inspect(|r| debug!(?tp, ?offset, ?r));

            // A partition the broker cannot serve fails *that partition* (#290).
            //
            // Propagating would fail the whole `Fetch`, and a fetch carries every
            // partition a consumer is assigned — which is the amplification the
            // issue is about: 168 damaged partitions stopped delivery on the 250
            // healthy ones sharing the same `poll()`. Answering per partition lets
            // the client see the error, apply its own policy, and keep consuming
            // everything else.
            let mut fetched = match fetched {
                Ok(fetched) => fetched,

                Err(Error::Api(code)) => {
                    error_code = code;
                    break;
                }

                Err(error) => {
                    error!(?tp, ?error);
                    return Err(error);
                }
            };

            *max_bytes =
                u32::try_from(fetched.byte_size()).map(|bytes| max_bytes.saturating_sub(bytes))?;

            debug!(?offset, ?fetched, max_bytes);

            if fetched.is_empty() {
                break;
            }

            // Advance by each batch's OFFSET SPAN, not by its record count, and
            // never treat a record-less batch as end-of-stream (#219).
            //
            // Per-key compaction over segments keeps a compacted-away batch as
            // a header — records stripped, `last_offset_delta` preserved — so
            // that the partition's offset space stays contiguous. So
            // `record_count == 0` is an ordinary interior state of a compacted
            // log, not a terminator, and `base_offset + record_count` stands
            // still on such a header. Breaking discarded the whole read
            // including the surviving records that follow, and a consumer
            // whose fetch offset had been compacted away (which is any fresh
            // `auto.offset.reset=earliest` consumer, since compaction does not
            // move the log start) never made progress.
            //
            // The headers stay in the response rather than being filtered out.
            // They cost only their own 61-byte header, they are what Kafka
            // itself returns for a compacted log, and they carry the span a
            // client needs to skip past them — filtering them would reintroduce
            // the stall for any position that resolves into a run of headers
            // with nothing surviving after it.
            let next = fetched
                .iter()
                .map(|batch| batch.base_offset + batch.last_offset_delta as i64 + 1)
                .max()
                .inspect(|next| debug!(next));

            batches.append(&mut fetched);

            match next {
                // Only keep reading while the offset actually moves forward: a
                // batch that spans nothing would otherwise re-read the same
                // objects forever.
                Some(next) if next > offset => offset = next,
                _ => break,
            }
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

        Ok(PartitionData::default()
            .partition_index(partition_index)
            .error_code(error_code.into())
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
            }))
        .inspect(|r| debug!(?r, elapsed = ?started_at.elapsed()))
    }

    fn unknown_topic_response(&self, fetch: &FetchTopic) -> Result<FetchableTopicResponse> {
        Ok(FetchableTopicResponse::default()
            .topic(fetch.topic.clone())
            .topic_id(Some(NULL_TOPIC_ID))
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
        let mut iteration = 0;
        let mut bytes = 0;

        while !max_wait.saturating_sub(started_at.elapsed()?).is_zero() && bytes <= min_bytes {
            debug!(?bytes, remaining = ?max_wait.saturating_sub(started_at.elapsed()?));

            responses.clear();

            let fetch_started_at = SystemTime::now();
            for topic in resolved.iter() {
                let fetch_response = if let Some((name, topic_id)) = topic.known.as_ref() {
                    let mut partitions = Vec::new();

                    for fetch_partition in topic.fetch.partitions.as_ref().unwrap_or(&Vec::new()) {
                        let remaining = max_wait.saturating_sub(started_at.elapsed()?);

                        let partition = self
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

                        partitions.push(partition);
                    }

                    FetchableTopicResponse::default()
                        .topic(topic.fetch.topic.to_owned())
                        // `topic_id` is not nullable from Fetch v13, where it
                        // replaces the name as the topic's identity. A resolved
                        // topic that carries none still has to occupy its 16
                        // bytes, so fall back to the nil uuid the way
                        // `unknown_topic_response` does — omitting it shifts the
                        // whole response under the client (#351).
                        .topic_id(Some(topic_id.unwrap_or(NULL_TOPIC_ID)))
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

            {
                // Waiting out `max.wait.ms` with nothing to return: the single
                // biggest reason requests-in-flight is a poor load proxy for a
                // broker, and what a fleet of idle consumers is made of (#362).
                let _parked = Parked::enter();

                sleep(remaining / 2).await;
            }

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

/// Wire size of a record batch's fixed header: `base_offset` (8) +
/// `batch_length` (4) + `partition_leader_epoch` (4) + `magic` (1) + `crc` (4) +
/// `attributes` (2) + `last_offset_delta` (4) + `base_timestamp` (8) +
/// `max_timestamp` (8) + `producer_id` (8) + `producer_epoch` (2) +
/// `base_sequence` (4) + `record_count` (4).
const BATCH_HEADER_BYTES: u64 = 61;

impl ByteSize for Batch {
    /// A batch costs its header plus its records — NOT its records alone.
    ///
    /// Charging only `record_data` made a record-less batch free, and per-key
    /// compaction leaves exactly those: emptied headers holding the offset space
    /// contiguous. Two things went wrong for a compacted partition (#228). The
    /// fetch walk decrements its `max_bytes` budget by this, so a run of headers
    /// never spent any of it and the walk accumulated every batch from the fetch
    /// offset to the high watermark — unbounded in memory and on the wire. And
    /// `fetch`'s outer loop compares the same total against `min_bytes`, so a
    /// header-only response looked like "no data" and it re-read the same objects
    /// until `max_wait_ms` expired instead of returning what it had.
    ///
    /// The header is real bytes on the wire and real bytes in memory, so charging
    /// it is both the bound and the honest accounting.
    fn byte_size(&self) -> u64 {
        BATCH_HEADER_BYTES + self.record_data.len() as u64
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
