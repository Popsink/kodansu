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

use std::{
    cmp::Ordering as Ordering2,
    sync::atomic::{AtomicU32, Ordering},
    time::SystemTime,
};

use futures::{StreamExt as _, TryStreamExt as _};
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

use tansu_sans_io::acl::{Operation, Resource};

use crate::{
    Error, OffsetStage, Parked, Result, Storage, Topition, authorized, storage_error_code,
};

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
/// Partitions read at once across the whole request (#426).
///
/// The same bound as every other fan-out in the engine — `FOOTER_FETCH_CONCURRENCY`,
/// `LIST_OFFSETS_CONCURRENCY`, `OFFSET_COMMIT_CONCURRENCY`, `DESCRIBE_CONCURRENCY` —
/// and for the same reason: it keeps one wide request inside the object store's
/// throttling envelope.
const FETCH_CONCURRENCY: usize = 32;

/// A `Fetch`'s request-level `max_bytes`, shared by every partition reading at
/// once (#426).
///
/// The partitions of a request used to be read one after another, threading the
/// remaining budget through as `&mut u32`. That is what Kafka does too — and
/// Kafka can afford it, because a local log read is sub-millisecond. Here every
/// partition read is a round trip to an object store, so a request's wall clock
/// was `partitions × topics × latency` and a consumer holding ~94 partitions
/// paid all of it in series.
///
/// Overlapping them is only safe if the cap still means something, which is the
/// question #437 left open. It is answered by **claiming rather than checking**:
/// a partition takes its allowance out of the budget *before* it reads and
/// hands back what it did not spend. A budget merely checked before the read
/// would be overshot by every partition already in flight; claimed, the total
/// delivered is bounded by `max_bytes` exactly, as it was in series.
///
/// # A claim is a share, not a reservation
///
/// The obvious claim — `min(partition_max_bytes, remaining)` — starves. The
/// request budget is clamped to 5 MiB and a client's `max.partition.fetch.bytes`
/// is 1 MiB by default, so the first five partitions would reserve the lot and
/// the remaining twenty-seven would be answered empty, having read nothing.
/// Measured exactly that way while writing this: 5 of 32 partitions returned
/// records.
///
/// So a claim is bounded by the budget's **share per concurrent reader** as well
/// as by the partition's own cap. The share is recomputed on every claim, so a
/// partition that finishes hands its slice back and the ones still reading grow
/// into it — and a partition needing more than one slice simply claims again on
/// the next turn of its loop, which is the loop it already had.
///
/// The floor of one byte is Kafka's `minOneMessage` in miniature: a budget that
/// is not yet exhausted must admit *somebody*, or a request whose remainder is
/// smaller than the number of readers would deliver nothing at all.
#[derive(Debug)]
struct Budget {
    remaining: AtomicU32,

    /// Readers that may be spending at once — `min(FETCH_CONCURRENCY, partitions
    /// in the request)`. The divisor of the share, and never zero.
    readers: u32,
}

impl Budget {
    fn new(max_bytes: u32, readers: usize) -> Self {
        Self {
            remaining: AtomicU32::new(max_bytes),
            readers: u32::try_from(readers).unwrap_or(u32::MAX).max(1),
        }
    }

    /// Take this reader's share, up to `want` bytes, or `None` when the budget
    /// is spent.
    ///
    /// `None` is the signal to stop reading, and it is *not* an error: the
    /// partition answers with whatever it already has, exactly as the
    /// `max_bytes == 0` break did in series.
    fn claim(&self, want: u32) -> Option<u32> {
        let mut claimed = 0;

        self.remaining
            .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                let share = (remaining / self.readers).max(1);
                claimed = want.min(share).min(remaining);
                (claimed > 0).then_some(remaining - claimed)
            })
            .ok()
            .map(|_| claimed)
    }

    /// Reconcile a claim against what the read actually cost.
    ///
    /// Usually it returns the unspent part. It can also *charge* an overspend:
    /// a partition asked for `claim` bytes and the log answers with at least one
    /// whole batch whatever the cap, so a small claim can come back with a
    /// larger batch. That is Kafka's rule too — `max_bytes` is explicitly not an
    /// absolute maximum, because a request that could never return the first
    /// batch would never make progress — and the excess has to come off the
    /// budget or the next reader spends it twice.
    fn settle(&self, claimed: u32, spent: u32) {
        match spent.cmp(&claimed) {
            Ordering2::Less => {
                _ = self.remaining.fetch_add(claimed - spent, Ordering::AcqRel);
            }

            Ordering2::Greater => {
                // Saturating: an overspend larger than what is left takes the
                // budget to zero and stops the readers behind it, which is the
                // answer — not a wrap to `u32::MAX`, which would uncap the
                // request entirely.
                _ = self
                    .remaining
                    .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                        Some(remaining.saturating_sub(spent - claimed))
                    });
            }

            Ordering2::Equal => {}
        }
    }

    #[cfg(test)]
    fn remaining(&self) -> u32 {
        self.remaining.load(Ordering::Acquire)
    }
}

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
        budget: &Budget,
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

        // A fetch position outside the log is answered `OFFSET_OUT_OF_RANGE`
        // (#444), and that error is every consumer's self-healing mechanism: it
        // is how a client detects a stale position — retention expiry, a
        // `DeleteRecords` truncation, a topic deleted and recreated (#442) — and
        // applies its `auto.offset.reset`. Without it the two ends fail
        // differently and both fail silently. A position *above* the log end
        // yields nothing and parks on the long poll, so the consumer polls
        // forever: connected, zero throughput, lag growing, nothing to alarm on.
        // A position *below* the log start is worse — the read resolves to the
        // oldest surviving records and serves them as though they were the ones
        // asked for, so a consumer resuming on a recreated topic silently reads
        // a different topic's successor from a position that no longer means
        // what it meant.
        //
        // Checked against the LOG, not the stable region, and so always at
        // read-uncommitted regardless of what this fetch asked for: an offset
        // above the last stable offset but below the log end is not out of
        // range, it is merely not yet visible to a read-committed consumer, and
        // refusing it would break every consumer of an open transaction. That
        // choice is also what makes the check free — the read-uncommitted stage
        // resolves from the in-memory hint and never reads `meta.json` (#109),
        // so no fetch pays a request for it.
        //
        // `fetch_offset == high_watermark` is IN range: it is what a caught-up
        // consumer sends on every poll.
        let log = match ctx
            .state()
            .offset_stage_at(&tp, IsolationLevel::ReadUncommitted)
            .await
        {
            Ok(log) => log,

            Err(error) => {
                error!(?error, ?tp);

                let error_code = match error {
                    Error::Api(code) => code,
                    ref error => storage_error_code(error),
                };

                return Ok(self.unservable_partition(partition_index, error_code));
            }
        };

        if offset < log.log_start() || offset > log.high_watermark() {
            debug!(
                ?tp,
                offset,
                log_start = log.log_start(),
                high_watermark = log.high_watermark(),
                "fetch offset is outside the log"
            );

            return Ok(self.out_of_range_partition(partition_index, &log));
        }

        // Kafka's own per-partition cap, which this path ignored: it passed the
        // whole remaining request budget to every partition. Honouring it is
        // what makes the partitions safe to overlap — each one can spend at most
        // what the client allotted it, so the claim below is bounded (#426).
        // Absent or nonsensical means *no* per-partition cap, which is what this
        // path did before it honoured one at all — not the tightest possible
        // cap. Clamping a missing value up to 1 would make a client that leaves
        // the field unset read one byte at a time.
        let partition_max_bytes = u32::try_from(fetch_partition.partition_max_bytes)
            .ok()
            .filter(|cap| *cap > 0)
            .unwrap_or(u32::MAX);

        // Claimed before each read and settled after, so the request-level cap
        // holds even though several partitions are spending it at once. A budget
        // merely *checked* before the read would be overshot by every partition
        // already in flight, and `None` — the budget is spent — is the signal to
        // stop, exactly as the `max_bytes == 0` break was in series.
        while let Some(claim) = budget.claim(partition_max_bytes) {
            debug!(offset, claim);

            let fetched = ctx
                .state()
                .fetch(
                    &tp,
                    offset,
                    min_bytes,
                    claim,
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
            //
            // That holds for EVERY error, not only `Error::Api` (#386). The
            // exception was doing the exact damage the rule exists to prevent: a
            // corrupt segment region raised a bare `TryFromIntError`, which is not
            // an `Api` code, so the whole request failed and the broker dropped the
            // connection with no response written. A client cannot act on a closed
            // socket — no code, no partition named, nothing to retry selectively —
            // so it reconnected and replayed the same fetch, which is how one
            // damaged region wedged a Connect worker reading its compacted offsets
            // topic from 0 (#219). No storage-layer error class gets to decide
            // whether a connection stays open; `storage_error_code` decides what
            // the client is told (`CORRUPT_MESSAGE` for damage,
            // `KAFKA_STORAGE_ERROR` for a transient store failure, #6/#275).
            let mut fetched = match fetched {
                Ok(fetched) => fetched,

                // Nothing was read, so nothing was spent: hand the whole claim
                // back before leaving, or a failing partition would take the
                // request's budget with it and starve the healthy ones sharing
                // the fan-out.
                Err(Error::Api(code)) => {
                    budget.settle(claim, 0);
                    error_code = code;
                    break;
                }

                Err(error) => {
                    budget.settle(claim, 0);
                    error!(?tp, ?error);
                    error_code = storage_error_code(&error);
                    break;
                }
            };

            let spent = u32::try_from(fetched.byte_size())?;
            budget.settle(claim, spent);

            debug!(?offset, ?fetched, claim, spent);

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
        // Per partition for the same reason the read above is (#386): a partition
        // whose offsets cannot be resolved is one partition's answer, and this is
        // the other place in this function where a storage error used to cost the
        // whole request its connection.
        let offset_stage = match ctx.state().offset_stage_at(&tp, isolation).await {
            Ok(offset_stage) => offset_stage,

            Err(error) => {
                error!(?error, ?tp);

                let error_code = match error {
                    Error::Api(code) => code,
                    ref error => storage_error_code(error),
                };

                return Ok(self.unservable_partition(partition_index, error_code));
            }
        };

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

    /// One partition answered with an error code and nothing else — the shape a
    /// client can act on when the broker cannot say what its offsets are (#386).
    ///
    /// The offsets are the ones `refused_topic_response` sends for a partition
    /// that has no readable state: a `high_watermark` of 0 with a
    /// `log_start_offset` of -1 claims nothing, so a client that looks past the
    /// error code cannot mistake it for an empty-but-healthy partition.
    /// A fetch position outside the log (#444).
    ///
    /// Distinct from [`Self::unservable_partition`], which reports zeroed
    /// bounds because it does not have any: this one carries the **real**
    /// `log_start_offset` and `high_watermark`, which is what a client resetting
    /// itself is looking at. Answering `OFFSET_OUT_OF_RANGE` with a log start of
    /// `-1` would tell a consumer its position is invalid and give it nothing to
    /// move to.
    fn out_of_range_partition(&self, partition_index: i32, log: &OffsetStage) -> PartitionData {
        PartitionData::default()
            .partition_index(partition_index)
            .error_code(ErrorCode::OffsetOutOfRange.into())
            .high_watermark(log.high_watermark())
            .last_stable_offset(Some(log.last_stable()))
            .log_start_offset(Some(log.log_start()))
            .diverging_epoch(None)
            .current_leader(None)
            .snapshot_id(None)
            .aborted_transactions(Some([].into()))
            .preferred_read_replica(Some(-1))
            .records(None)
    }

    fn unservable_partition(&self, partition_index: i32, error_code: ErrorCode) -> PartitionData {
        PartitionData::default()
            .partition_index(partition_index)
            .error_code(error_code.into())
            .high_watermark(0)
            .last_stable_offset(Some(0))
            .log_start_offset(Some(-1))
            .diverging_epoch(None)
            .current_leader(None)
            .snapshot_id(None)
            .aborted_transactions(Some([].into()))
            .preferred_read_replica(Some(-1))
            .records(None)
    }

    /// A topic this principal may not read, refused the way an unknown one is
    /// refused: per partition, because that is where a client reads an error
    /// code (#363).
    ///
    /// A *distinct* code from `UNKNOWN_TOPIC_OR_PARTITION`, deliberately.
    /// Answering "no such topic" would hide the topic's existence, which is
    /// the stronger property — and the one namespace isolation is for — but it
    /// would also tell a client with a genuine configuration error to go and
    /// create the topic. Kafka draws the line here, and drawing it elsewhere is
    /// a decision for the namespace work rather than a side effect of this.
    fn unauthorized_topic_response(&self, fetch: &FetchTopic) -> Result<FetchableTopicResponse> {
        self.refused_topic_response(fetch, ErrorCode::TopicAuthorizationFailed)
    }

    fn unknown_topic_response(&self, fetch: &FetchTopic) -> Result<FetchableTopicResponse> {
        self.refused_topic_response(fetch, ErrorCode::UnknownTopicOrPartition)
    }

    fn refused_topic_response(
        &self,
        fetch: &FetchTopic,
        error_code: ErrorCode,
    ) -> Result<FetchableTopicResponse> {
        Ok(FetchableTopicResponse::default()
            .topic(fetch.topic.clone())
            .topic_id(Some(NULL_TOPIC_ID))
            .partitions(fetch.partitions.as_ref().map(|partitions| {
                partitions
                    .iter()
                    .map(|partition| {
                        PartitionData::default()
                            .partition_index(partition.partition)
                            .error_code(error_code.into())
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
        max_bytes: u32,
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

            // Decided here, once, with the resolution — not inside the poll
            // loop below. A long poll iterates for as long as `max.wait.ms`,
            // and re-asking per iteration would make the authorization cost of
            // an idle consumer proportional to how long it waits (#363).
            //
            // Against the *resolved* name where there is one: from Fetch v13 a
            // client may name a topic by id alone, and the ACL is written on
            // the name.
            let allowed = authorized(
                ctx,
                Resource::Topic,
                known
                    .as_ref()
                    .map(|(name, _)| name.as_str())
                    .or(fetch.topic.as_deref())
                    .unwrap_or_default(),
                Operation::Read,
            )
            .await;

            resolved.push(ResolvedTopic {
                fetch,
                known,
                allowed,
            });
        }

        let started_at = SystemTime::now();
        let mut responses = vec![];
        let mut iteration = 0;
        let mut bytes = 0;

        while !max_wait.saturating_sub(started_at.elapsed()?).is_zero() && bytes <= min_bytes {
            debug!(?bytes, remaining = ?max_wait.saturating_sub(started_at.elapsed()?));

            responses.clear();

            let fetch_started_at = SystemTime::now();

            // Every partition of every topic in one flat fan-out, bounded once
            // (#426). Nested `buffered` calls would bound `topics × partitions`
            // rather than the request, and it is the request that meets the
            // object store's throttling envelope.
            //
            // `buffered`, not `buffer_unordered`: a client matches partitions to
            // its request positionally, so the order the answers come back in is
            // part of the response.
            // A fresh budget for a fresh response set, sized by how many
            // partitions will share it. The previous iteration's responses were
            // just cleared, so spending its budget on them and carrying the
            // remainder forward would make each long-poll pass read less than
            // the last and eventually nothing — it was threaded through as
            // `&mut` across iterations and never reset.
            let readers = resolved
                .iter()
                .filter(|topic| topic.allowed && topic.known.is_some())
                .map(|topic| topic.fetch.partitions.as_deref().unwrap_or_default().len())
                .sum::<usize>()
                .min(FETCH_CONCURRENCY);

            let budget = &Budget::new(max_bytes, readers);

            let reads = resolved
                .iter()
                .enumerate()
                .filter(|(_, topic)| topic.allowed && topic.known.is_some())
                .flat_map(|(index, topic)| {
                    topic
                        .fetch
                        .partitions
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(move |fetch_partition| (index, topic, fetch_partition))
                })
                .map(|(index, topic, fetch_partition)| {
                    let name = topic
                        .known
                        .as_ref()
                        .map(|(name, _)| name.as_str())
                        .unwrap_or_default();

                    async move {
                        let remaining = max_wait.saturating_sub(started_at.elapsed()?);

                        self.fetch_partition(
                            ctx,
                            remaining,
                            min_bytes,
                            budget,
                            isolation,
                            name,
                            fetch_partition,
                        )
                        .await
                        .map(|partition| (index, partition))
                    }
                })
                .collect::<Vec<_>>();

            let mut fetched: Vec<Vec<PartitionData>> = vec![Vec::new(); resolved.len()];

            {
                let mut reads = futures::stream::iter(reads).buffered(FETCH_CONCURRENCY);

                while let Some((index, partition)) = reads.try_next().await? {
                    fetched[index].push(partition);
                }
            }

            for (topic, partitions) in resolved.iter().zip(fetched) {
                let fetch_response = if !topic.allowed {
                    self.unauthorized_topic_response(topic.fetch)?
                } else if let Some((_, topic_id)) = topic.known.as_ref() {
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

    /// Whether this request's principal may read it (#363), decided once with
    /// the resolution rather than on every long-poll iteration.
    allowed: bool,
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

            let max_bytes = req.max_bytes.map_or(Ok(DEFAULT_MAX_BYTES), |max_bytes| {
                u32::try_from(max_bytes).map(|max_bytes| max_bytes.min(DEFAULT_MAX_BYTES))
            })?;

            self.fetch(
                &ctx,
                max_wait_ms,
                min_bytes,
                max_bytes,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole point of claiming rather than checking (#426): a budget that
    /// several partitions spend at once still bounds the response exactly.
    ///
    /// Checked-then-spent, every partition already in flight would see the full
    /// remaining budget and read against it, so `N` concurrent partitions could
    /// deliver `N ×` the cap. Claimed, the arithmetic is the same as it was in
    /// series.
    #[test]
    fn concurrent_claims_cannot_exceed_the_budget() {
        const BUDGET: u32 = 1_000;
        const PARTITION: u32 = 400;

        let budget = Budget::new(BUDGET, 1);

        // Three partitions want 400 each against a budget of 1 000. Two get
        // what they asked for; the third gets the remainder, not a refusal.
        assert_eq!(Some(PARTITION), budget.claim(PARTITION));
        assert_eq!(Some(PARTITION), budget.claim(PARTITION));
        assert_eq!(Some(BUDGET - 2 * PARTITION), budget.claim(PARTITION));

        // And a fourth gets nothing, which is the signal to stop reading.
        assert_eq!(None, budget.claim(PARTITION));
        assert_eq!(0, budget.remaining());
    }

    /// A partition that claimed more than it used hands the rest back, so a
    /// mostly-empty assignment does not starve the partitions that have data.
    #[test]
    fn an_unspent_claim_returns_to_the_budget() {
        let budget = Budget::new(1_000, 1);

        let claim = budget.claim(1_000).expect("the whole budget");
        assert_eq!(0, budget.remaining());

        budget.settle(claim, 10);

        assert_eq!(990, budget.remaining());
        assert_eq!(Some(990), budget.claim(u32::MAX));
    }

    /// A claim spent in full returns nothing, and a read that somehow reported
    /// more than it claimed does not credit the budget by underflowing.
    #[test]
    fn a_spent_claim_returns_nothing() {
        let budget = Budget::new(100, 1);

        let claim = budget.claim(100).expect("the whole budget");
        budget.settle(claim, 100);
        assert_eq!(0, budget.remaining());
    }

    /// A read that came back with more than it claimed charges the difference.
    ///
    /// The log answers with at least one whole batch whatever the cap — Kafka's
    /// rule, and the reason `max_bytes` is documented as not an absolute
    /// maximum — so a small claim can return a larger batch. Crediting only the
    /// unspent side would let the next reader spend the excess a second time.
    #[test]
    fn an_overspent_claim_is_charged_to_the_budget() {
        let budget = Budget::new(1_000, 1);

        let claim = budget.claim(10).expect("a small claim");
        assert_eq!(10, claim);

        // The batch was 110 bytes: 100 more than the claim.
        budget.settle(claim, 110);

        assert_eq!(890, budget.remaining());
    }

    /// And an overspend larger than the budget takes it to zero, stopping the
    /// readers behind it — never wrapping, which would uncap the request.
    #[test]
    fn an_overspend_cannot_wrap_the_budget() {
        let budget = Budget::new(100, 1);

        let claim = budget.claim(100).expect("the whole budget");
        budget.settle(claim, u32::MAX);

        assert_eq!(0, budget.remaining());
        assert_eq!(None, budget.claim(1));
    }

    /// A partition can never take the whole request's budget, which is Kafka's
    /// rule and was not honoured here at all: every partition used to be handed
    /// the entire remaining `max_bytes`.
    #[test]
    fn a_claim_is_bounded_by_what_the_partition_asked_for() {
        let budget = Budget::new(1_000, 1);

        assert_eq!(Some(10), budget.claim(10));
        assert_eq!(990, budget.remaining());
    }

    /// An exhausted budget refuses rather than returning zero: zero is a claim
    /// that would read nothing forever, and the caller reads `None` as "stop".
    #[test]
    fn an_exhausted_budget_refuses() {
        let budget = Budget::new(0, 1);

        assert_eq!(None, budget.claim(1));
        assert_eq!(None, budget.claim(u32::MAX));
    }
}
