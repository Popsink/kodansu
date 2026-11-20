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
    collections::BTreeMap,
    fmt::{self, Debug, Formatter},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use futures::StreamExt as _;
use rama::{Context, Service};
use tansu_sans_io::TimestampType;
use tansu_sans_io::acl::{Operation, Resource};
use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};
use tansu_sans_io::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use tansu_sans_io::{ApiKey, BatchAttribute, ErrorCode, ProduceRequest, ProduceResponse};
use tracing::{debug, error, instrument, warn};

use crate::{Ack, Error, Result, Storage, Topition, authorized, storage_error_code};

/// Serves a [`ProduceRequest`] by writing its batches to the [`Storage`] in its
/// [`Context`].
///
/// ```
/// use bytes::Bytes;
/// use rama::layer::MapStateLayer;
/// use rama::{Context, Layer as _, Service as _};
/// use tansu_sans_io::create_topics_request::CreatableTopic;
/// use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};
/// use tansu_sans_io::record::deflated::Frame;
/// use tansu_sans_io::record::{Record, inflated};
/// use tansu_sans_io::{CreateTopicsRequest, ErrorCode, ProduceRequest};
/// use tansu_storage::{CreateTopicsService, Error, ProduceService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let storage = StorageContainer::builder()
///     .cluster_id("tansu")
///     .node_id(111)
///     .advertised_listener(Url::parse("tcp://localhost:9092")?)
///     .storage(Url::parse("memory://tansu/")?)
///     .build()
///     .await?;
/// let state = |storage: &_| {
///     let storage = Clone::clone(storage);
///     MapStateLayer::new(move |_| storage)
/// };
///
/// let topic = CreatableTopic::default()
///     .name(String::from("abcba"))
///     .num_partitions(5)
///     .replication_factor(3)
///     .assignments(Some(Vec::new()))
///     .configs(Some(Vec::new()));
/// let create = CreateTopicsRequest::default()
///     .topics(Some(vec![topic]))
///     .validate_only(Some(false));
/// let created = state(&storage).into_layer(CreateTopicsService);
/// let topics = created.serve(Context::default(), create).await?.topics.unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
///
/// let value = Bytes::from_static(b"Lorem ipsum dolor sit amet");
/// let batch = inflated::Batch::builder()
///     .record(Record::builder().value(value.into()))
///     .build()
///     .and_then(TryInto::try_into)?;
/// let partition = PartitionProduceData::default()
///     .index(0)
///     .records(Some(Frame { batches: vec![batch] }));
/// let data = TopicProduceData::default()
///     .name(String::from("abcba"))
///     .partition_data(Some(vec![partition]));
/// let produce = ProduceRequest::default().topic_data(Some(vec![data]));
/// let producing = state(&storage).into_layer(ProduceService);
/// let answered = producing.serve(Context::default(), produce).await?;
///
/// let topics = answered.responses.unwrap_or_default();
/// assert_eq!(1, topics.len());
/// let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(partitions[0].error_code)?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceService;

/// Partitions written at once across the whole request (#439).
///
/// The same bound as every other fan-out in the engine — `FETCH_CONCURRENCY`,
/// `FOOTER_FETCH_CONCURRENCY`, `LIST_OFFSETS_CONCURRENCY`,
/// `OFFSET_COMMIT_CONCURRENCY` — and for the same reason: it keeps one wide
/// request inside the object store's throttling envelope.
const PRODUCE_CONCURRENCY: usize = 32;

impl ApiKey for ProduceService {
    const KEY: i16 = ProduceRequest::KEY;
}

/// Told when a produce request has nothing left to admit (#588).
///
/// Found in the request's [`Context`], put there by whoever can use it: the
/// connection loop reads the next request once this fires, rather than once
/// this one is acknowledged. Every batch of the request is then in its window,
/// so a batch the next request carries is written after all of them.
///
/// Absent, nothing changes: the request is served exactly as it would be with
/// the hook, and nobody is told.
#[derive(Clone)]
pub struct OnAdmitted(Arc<dyn Fn() + Send + Sync>);

impl OnAdmitted {
    pub fn new(admitted: impl Fn() + Send + Sync + 'static) -> Self {
        Self(Arc::new(admitted))
    }

    fn admitted(&self) {
        (self.0)()
    }
}

impl Debug for OnAdmitted {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(OnAdmitted)).finish()
    }
}

/// One partition entry once it has been admitted (#588).
enum Admitted {
    /// Nothing left to wait for: refused before reaching storage, or failed
    /// on the way.
    Answered(PartitionProduceResponse),

    /// The entry's last batch is in its window, and `ack` resolves when it is
    /// durable. `base_offset` is the first batch's, when there was more than one.
    Waiting {
        tp: Topition,
        index: i32,
        base_offset: Option<i64>,
        ack: Ack,
    },
}

impl ProduceService {
    fn error(&self, index: i32, error_code: ErrorCode) -> PartitionProduceResponse {
        partition_answer(index, error_code, -1, 0)
    }

    fn produced(&self, index: i32, base_offset: i64) -> PartitionProduceResponse {
        partition_answer(index, ErrorCode::None, base_offset, 0)
    }

    /// The answer for a batch storage refused, whether it refused to admit it
    /// or to acknowledge it.
    fn failed(&self, tp: &Topition, index: i32, error: Error) -> PartitionProduceResponse {
        // Expected idempotent-producer outcomes (a retried batch already
        // persisted) — routine, not a failure (#37).
        if error.is_expected_idempotent_outcome() {
            debug!(err = ?error);
        } else if let Error::Api(_) = &error {
            warn!(storage_api = ?error);
        } else {
            error!(otherwise = ?error);
        }

        match error {
            Error::Api(error_code) => {
                debug!(self = ?self, error_code = ?error_code);
                self.error(index, error_code)
            }

            otherwise => {
                // A transient storage failure (e.g. an S3 error under load) was
                // previously surfaced as UNKNOWN_SERVER_ERROR (-1), which
                // clients treat as fatal and drop the whole batch (#6). Map
                // storage errors to a *retriable* code so clients retry
                // instead, and log the underlying error with its
                // topic-partition so it isn't opaque.
                let error_code = storage_error_code(&otherwise);
                warn!(?otherwise, ?tp, ?error_code, "produce storage error");
                self.error(index, error_code)
            }
        }
    }

    /// Admit every batch of one partition entry, leaving the last one's ack
    /// outstanding.
    #[instrument(skip_all)]
    async fn admit<G>(
        &self,
        ctx: &Context<G>,
        transaction_id: Option<&str>,
        name: &str,
        partition: PartitionProduceData,
    ) -> Admitted
    where
        G: Storage,
    {
        let Some(records) = partition.records else {
            return Admitted::Answered(self.error(partition.index, ErrorCode::UnknownServerError));
        };

        let mut base_offset = None;
        let mut batches = records.batches.into_iter().peekable();

        while let Some(mut batch) = batches.next() {
            let tp = Topition::new(name, partition.index);

            // Refuse a record format this broker does not read, and do it
            // before the CRC gate below (#320).
            //
            // The order is the point. A pre-v2 MessageSet also fails that
            // gate — its checksum is a CRC-32 over a different range, not
            // the CRC-32C of a v2 payload — so legacy producers were
            // already being turned away, but as a side effect, and told
            // "your CRC is wrong" when the truth is that this broker does
            // not read their message format. Deciding here makes the
            // refusal deliberate, and keeps it from moving the next time
            // the CRC path is touched.
            //
            // UNSUPPORTED_FOR_MESSAGE_FORMAT (43) rather than
            // UNSUPPORTED_VERSION (35): the API version was understood, it
            // is the record format inside it that is not supported. Kafka
            // answers 43 for the same reason.
            if !batch.is_record_batch_v2() {
                warn!(
                    ?tp,
                    magic = batch.magic,
                    "rejecting a record format this broker does not read"
                );
                return Admitted::Answered(
                    self.error(partition.index, ErrorCode::UnsupportedForMessageFormat),
                );
            }

            // Refuse a batch whose CRC does not cover its payload, as
            // Kafka's LogValidator does, rather than storing it and
            // discovering the corruption on the read side (#271).
            //
            // This has to happen here, and before the LogAppendTime
            // rewrite below: the decoder logs a mismatch and carries on,
            // because it is shared with the path that reads bytes back
            // out of storage — and that rewrite mutates two CRC-covered
            // timestamps without recomputing the CRC, so a stored batch
            // legitimately carries a stale one. The reasoning is spelt
            // out at the mismatch in `deflated::Batch`'s `TryFrom<Bytes>`.
            match batch.crc_matches() {
                Ok(true) => (),

                Ok(false) => {
                    warn!(
                        ?tp,
                        crc = batch.crc,
                        record_count = batch.record_count,
                        "rejecting a batch whose crc does not match its payload"
                    );
                    return Admitted::Answered(
                        self.error(partition.index, ErrorCode::CorruptMessage),
                    );
                }

                // The batch cannot be re-encoded to check it, so it
                // cannot be stored either. Same answer.
                Err(err) => {
                    warn!(?err, ?tp, "cannot verify a batch crc");
                    return Admitted::Answered(
                        self.error(partition.index, ErrorCode::CorruptMessage),
                    );
                }
            }

            let attributes = BatchAttribute::try_from(batch.attributes);
            let appended_now = attributes
                .is_ok_and(|attributes| attributes.timestamp == TimestampType::LogAppendTime);
            if appended_now {
                let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH);
                let now = since_epoch.map_or(0, |elapsed| elapsed.as_millis() as i64);
                batch.base_timestamp = now;
                batch.max_timestamp = now;
            }

            let ack = match ctx.state().admit(transaction_id, &tp, batch).await {
                Ok(ack) => ack,
                Err(error) => return Admitted::Answered(self.failed(&tp, partition.index, error)),
            };

            if batches.peek().is_none() {
                return Admitted::Waiting {
                    tp,
                    index: partition.index,
                    base_offset,
                    ack,
                };
            }

            // A batch is admitted only once the one before it in the entry is
            // acknowledged, as it was before #588. Admitted together, a window
            // that filled between them could fail ahead of one that succeeds,
            // leaving a gap that nothing rejects for a producer without
            // sequence numbers.
            match ack.await {
                Ok(offset) => _ = base_offset.get_or_insert(offset),
                Err(error) => return Admitted::Answered(self.failed(&tp, partition.index, error)),
            }
        }

        Admitted::Answered(self.error(partition.index, ErrorCode::UnknownServerError))
    }

    /// Wait for an admitted entry's outstanding ack, if it has one.
    async fn complete(&self, admitted: Admitted) -> PartitionProduceResponse {
        match admitted {
            Admitted::Answered(response) => response,

            Admitted::Waiting {
                tp,
                index,
                base_offset,
                ack,
            } => match ack.await {
                Ok(offset) => self.produced(index, base_offset.unwrap_or(offset)),
                Err(error) => self.failed(&tp, index, error),
            },
        }
    }

    fn unauthorized(&self, index: i32) -> PartitionProduceResponse {
        partition_answer(index, ErrorCode::TopicAuthorizationFailed, -1, -1)
    }
}

/// The answer for partition `index`: `error_code` at `base_offset`, with no
/// record errors, no append time and the given log start.
fn partition_answer(
    index: i32,
    error_code: ErrorCode,
    base_offset: i64,
    log_start_offset: i64,
) -> PartitionProduceResponse {
    let code: i16 = error_code.into();
    PartitionProduceResponse::default()
        .index(index)
        .error_code(code)
        .base_offset(base_offset)
        .log_append_time_ms(Some(-1))
        .log_start_offset(Some(log_start_offset))
        .record_errors(Some(Vec::new()))
        .error_message(None)
        .current_leader(None)
}

impl<G> Service<G, ProduceRequest> for ProduceService
where
    G: Storage,
{
    type Response = ProduceResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: ProduceRequest,
    ) -> Result<Self::Response, Self::Error> {
        let topics: Vec<TopicProduceData> = req.topic_data.unwrap_or_default();
        let transactional_id = req.transactional_id;
        let transaction_id = transactional_id.as_deref();

        // Per topic, and refused per partition, because that is the shape the
        // response has: a client reads a partition's error code, and a blanket
        // failure at the top would tell it every partition of every topic in
        // the request had failed (#363).
        //
        // Resolved here, once per topic and before the fan-out below, rather
        // than inside it: the answer is the same for every partition of a
        // topic, and asking per partition would multiply an authorizer lookup
        // by the width of the request.
        let mut allowed = Vec::with_capacity(topics.len());
        for topic in topics.iter() {
            allowed.push(authorized(&ctx, Resource::Topic, &topic.name, Operation::Write).await);
        }

        // Every partition of every topic in one flat fan-out (#439). They were
        // written one after the other, each `produce` awaited to completion
        // before the next began — and because a produce completes only when its
        // coalescing window has flushed, an N-partition request cost N × linger.
        // That is why the conformance bench found eight partitions *slower* than
        // one: the partitions of a request are independent logs, and nothing in
        // the protocol orders them against each other, so waiting for one before
        // starting the next bought nothing and cost a whole flush window.
        //
        // Grouped by topition rather than fanned out entry by entry: a request
        // naming the same `(topic, partition)` twice is two appends to one log,
        // and those must still happen in the order the client wrote them —
        // exactly as the batches *within* one entry still do. Well-behaved
        // clients never send that shape; a hand-rolled one that does gets the
        // ordering it would have got before rather than a race.
        //
        // Bounded like every other fan-out in the engine, and for the same
        // reason: it keeps one wide request inside the object store's
        // throttling envelope.
        let mut grouped: BTreeMap<Topition, Vec<(usize, usize, PartitionProduceData)>> =
            BTreeMap::new();

        // Positional: a client matches a partition response to its request by
        // position, so each answer goes back into the slot its entry came from
        // however the fan-out completes.
        let mut responses: Vec<Vec<Option<PartitionProduceResponse>>> =
            Vec::with_capacity(topics.len());

        let mut names = Vec::with_capacity(topics.len());

        for (index, topic) in topics.into_iter().enumerate() {
            let partition_data = topic.partition_data.unwrap_or_default();

            responses.push(vec![None; partition_data.len()]);

            for (position, partition) in partition_data.into_iter().enumerate() {
                if allowed[index] {
                    grouped
                        .entry(Topition::new(topic.name.as_str(), partition.index))
                        .or_default()
                        .push((index, position, partition));
                } else {
                    responses[index][position] = Some(self.unauthorized(partition.index));
                }
            }

            names.push(topic.name);
        }

        // Admitted first, acknowledged second (#588). Once every batch of the
        // request is in its window, `OnAdmitted` says so, and a connection
        // that pipelines reads its next request — whose batches can then share
        // the windows this one is waiting on, instead of each request having a
        // window to itself.
        let admissions = grouped.into_iter().map(|(topition, entries)| {
            let ctx = &ctx;

            async move {
                let mut admitted = Vec::with_capacity(entries.len());
                let mut entries = entries.into_iter().peekable();

                while let Some((index, position, partition)) = entries.next() {
                    let entry = self
                        .admit(ctx, transaction_id, topition.topic(), partition)
                        .await;

                    // The same topition named twice is two appends to one log,
                    // so the second waits on the first exactly as the batches
                    // within one entry do (#439).
                    let entry = if entries.peek().is_some() {
                        Admitted::Answered(self.complete(entry).await)
                    } else {
                        entry
                    };

                    admitted.push((index, position, entry));
                }

                admitted
            }
        });

        let mut admitted = Vec::new();

        {
            let mut admissions =
                futures::stream::iter(admissions).buffer_unordered(PRODUCE_CONCURRENCY);

            while let Some(entries) = admissions.next().await {
                admitted.extend(entries);
            }
        }

        if let Some(on_admitted) = ctx.get::<OnAdmitted>() {
            on_admitted.admitted();
        }

        {
            let mut completions =
                futures::stream::iter(admitted.into_iter().map(
                    |(index, position, entry)| async move {
                        (index, position, self.complete(entry).await)
                    },
                ))
                .buffer_unordered(PRODUCE_CONCURRENCY);

            while let Some((index, position, response)) = completions.next().await {
                responses[index][position] = Some(response);
            }
        }

        let responses = names
            .into_iter()
            .zip(responses)
            .map(|(name, partitions)| {
                TopicProduceResponse::default()
                    .name(name)
                    .partition_responses(Some(
                        // Every slot was filled: each was created from an entry,
                        // and each entry was either answered by the fan-out or
                        // refused above it.
                        partitions.into_iter().flatten().collect(),
                    ))
            })
            .collect::<Vec<_>>();

        let answered = ProduceResponse::default()
            .throttle_time_ms(Some(0))
            .node_endpoints(None)
            .responses(Some(responses));
        Ok(answered)
    }
}

#[cfg(all(test, feature = "dynostore"))]
mod tests {
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use rama::Context;
    use tansu_sans_io::record::deflated::{self, Frame};
    use tansu_sans_io::record::{Record, inflated};
    use tansu_sans_io::{ErrorCode, InitProducerIdRequest};
    use tracing::subscriber::DefaultGuard;

    use super::*;
    use crate::Error;
    use crate::dynostore::DynoStore;
    use crate::service::init_producer_id::InitProducerIdService;

    #[test]
    fn object_store_errors_map_to_retriable_kafka_storage_error() {
        use std::sync::Arc;

        // A transient storage failure must surface as a retriable code so the
        // client retries instead of dropping the batch (#6).
        let object_store = Error::ObjectStore(Arc::new(object_store::Error::Generic {
            store: "S3",
            source: "503 SlowDown".into(),
        }));
        assert_eq!(
            ErrorCode::KafkaStorageError,
            storage_error_code(&object_store)
        );

        // Anything else stays UNKNOWN — a retry wouldn't fix it.
        assert_eq!(
            ErrorCode::UnknownServerError,
            storage_error_code(&Error::NoSuchOffset(0))
        );
    }

    #[test]
    fn expected_idempotent_outcomes_are_not_failures() {
        // Retried-after-disconnect batches surface as these two codes; a
        // well-behaved idempotent producer handles them, so they must not be
        // logged at error/warn (#37).
        assert!(Error::Api(ErrorCode::DuplicateSequenceNumber).is_expected_idempotent_outcome());
        assert!(Error::Api(ErrorCode::OutOfOrderSequenceNumber).is_expected_idempotent_outcome());

        // Any other Api error, and non-Api errors, remain genuine failures.
        assert!(!Error::Api(ErrorCode::ProducerFenced).is_expected_idempotent_outcome());
        assert!(!Error::Api(ErrorCode::UnknownServerError).is_expected_idempotent_outcome());
        assert!(!Error::NoSuchOffset(0).is_expected_idempotent_outcome());
    }

    /// Debug logs and active spans, to this test thread's own file under
    /// `logs/`.
    fn init_tracing() -> Result<DefaultGuard> {
        use std::fs::File;
        use std::sync::Arc;
        use std::thread;

        use tracing::Level;
        use tracing_subscriber::fmt::format::FmtSpan;

        let current = thread::current();
        let name = current
            .name()
            .ok_or(Error::Message(String::from("unnamed thread")))?;
        let path = format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"));
        let log = Arc::new(File::create(path)?);

        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_max_level(Level::DEBUG)
            .with_span_events(FmtSpan::ACTIVE)
            .with_writer(log)
            .finish();
        Ok(tracing::subscriber::set_default(subscriber))
    }

    /// One partition's worth of data for `topic`: the batch `builder` builds.
    fn topic_data(
        topic: &str,
        index: i32,
        builder: inflated::Builder,
    ) -> Result<Option<Vec<TopicProduceData>>> {
        let batch = deflated::Batch::try_from(builder.build()?)?;
        let records = Frame {
            batches: vec![batch],
        };
        let partition = PartitionProduceData::default()
            .index(index)
            .records(Some(records));
        let data = TopicProduceData::default()
            .name(topic.to_owned())
            .partition_data(Some(vec![partition]));
        Ok(Some(vec![data]))
    }

    /// A record holding `value`.
    fn record(value: &'static [u8]) -> tansu_sans_io::record::Builder {
        Record::builder().value(Bytes::from_static(value).into())
    }

    /// The answer `ProduceService` gives one partition of `topic`.
    fn answered(
        topic: &str,
        index: i32,
        error_code: ErrorCode,
        base_offset: i64,
    ) -> ProduceResponse {
        let partition = partition_answer(index, error_code, base_offset, 0);
        let topic = TopicProduceResponse::default()
            .name(topic.to_owned())
            .partition_responses(Some(vec![partition]));
        ProduceResponse::default()
            .responses(Some(vec![topic]))
            .throttle_time_ms(Some(0))
            .node_endpoints(None)
    }

    /// Produce `batch` to `topic` with no transaction, no acks and no timeout.
    async fn produce(
        ctx: &Context<DynoStore>,
        topic: &str,
        index: i32,
        batch: inflated::Builder,
    ) -> Result<ProduceResponse> {
        let request = ProduceRequest::default()
            .transactional_id(None)
            .acks(0)
            .timeout_ms(0)
            .topic_data(topic_data(topic, index, batch)?);
        ProduceService.serve(ctx.clone(), request).await
    }

    /// A new idempotent producer's id.
    async fn new_producer(ctx: &Context<DynoStore>) -> Result<i64> {
        let request = InitProducerIdRequest::default()
            .transactional_id(None)
            .transaction_timeout_ms(0)
            .producer_id(Some(-1))
            .producer_epoch(Some(-1));
        let answer = InitProducerIdService.serve(ctx.clone(), request).await?;
        Ok(answer.producer_id)
    }

    /// A producer with no coordinate in the log is admitted as new (#88), not
    /// answered with `UnknownProducerId`. See
    /// `dynostore::tests::idempotent::an_unknown_producer_is_admitted_as_new`
    /// for why the registry that used to reject it is gone, and for the
    /// deliberate Kafka divergence this records.
    #[tokio::test]
    async fn non_txn_idempotent_unknown_producer_is_admitted() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = "abc";
        let node = 12321;

        let topic = "pqr";
        let index = 0;

        let storage = DynoStore::new(cluster, node, InMemory::new());
        let ctx = Context::with_state(storage);

        assert_eq!(
            answered(topic, index, ErrorCode::None, 0),
            produce(
                &ctx,
                topic,
                index,
                inflated::Batch::builder()
                    .record(record(b"lorem"))
                    .producer_id(54345)
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = "abc";
        let node = 12321;
        let topic = "pqr";
        let index = 0;

        let storage = DynoStore::new(cluster, node, InMemory::new());
        let ctx = Context::with_state(storage);

        let producer_id = new_producer(&ctx).await?;

        assert_eq!(
            answered(topic, index, ErrorCode::None, 0),
            produce(
                &ctx,
                topic,
                index,
                inflated::Batch::builder()
                    .record(record(b"Lorem ipsum dolor sit amet"))
                    .producer_id(producer_id)
            )
            .await?
        );

        assert_eq!(
            answered(topic, index, ErrorCode::None, 1),
            produce(
                &ctx,
                topic,
                index,
                inflated::Batch::builder()
                    .record(record(b"consectetur adipiscing elit"))
                    .record(record(b"sed do eiusmod tempor"))
                    .base_sequence(1)
                    .last_offset_delta(1)
                    .producer_id(producer_id)
            )
            .await?
        );

        assert_eq!(
            answered(topic, index, ErrorCode::None, 3),
            produce(
                &ctx,
                topic,
                index,
                inflated::Batch::builder()
                    .record(record(b"incididunt ut labore"))
                    .base_sequence(3)
                    .producer_id(producer_id)
            )
            .await?
        );

        Ok(())
    }

    /// A duplicate is acked with the offset the original landed at (#88),
    /// rather than raising `DuplicateSequenceNumber` as the per-pod registry
    /// did. That is the Kafka-conformant answer: a retry after an ack the
    /// client never saw becomes idempotent instead of fatal.
    #[tokio::test]
    async fn non_txn_idempotent_duplicate_is_acked_with_the_original_offset() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = "abc";
        let node = 12321;
        let topic = "pqr";
        let index = 0;

        let storage = DynoStore::new(cluster, node, InMemory::new());
        let ctx = Context::with_state(storage);

        let producer_id = new_producer(&ctx).await?;

        assert_eq!(
            answered(topic, index, ErrorCode::None, 0),
            produce(
                &ctx,
                topic,
                index,
                inflated::Batch::builder()
                    .record(record(b"Lorem ipsum dolor sit amet"))
                    .producer_id(producer_id)
            )
            .await?
        );

        assert_eq!(
            answered(topic, index, ErrorCode::None, 0),
            produce(
                &ctx,
                topic,
                index,
                inflated::Batch::builder()
                    .record(record(b"Lorem ipsum dolor sit amet"))
                    .producer_id(producer_id)
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn non_txn_idempotent_sequence_out_of_order() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = "abc";
        let node = 12321;
        let topic = "pqr";
        let index = 0;

        let storage = DynoStore::new(cluster, node, InMemory::new());
        let ctx = Context::with_state(storage);

        let producer_id = new_producer(&ctx).await?;

        assert_eq!(
            answered(topic, index, ErrorCode::None, 0),
            produce(
                &ctx,
                topic,
                index,
                inflated::Batch::builder()
                    .record(record(b"Lorem ipsum dolor sit amet"))
                    .producer_id(producer_id)
            )
            .await?
        );

        assert_eq!(
            answered(topic, index, ErrorCode::OutOfOrderSequenceNumber, -1),
            produce(
                &ctx,
                topic,
                index,
                inflated::Batch::builder()
                    .record(record(b"Lorem ipsum dolor sit amet"))
                    .base_sequence(2)
                    .producer_id(producer_id)
            )
            .await?
        );

        Ok(())
    }

    /// A batch whose payload does not match its CRC is answered
    /// `CORRUPT_MESSAGE` (2) for that partition, and is not stored (#271).
    ///
    /// The alternative — rejecting while decoding the request — would fail the
    /// whole request body, and the broker ends a connection it cannot decode
    /// with no response at all, telling the producer nothing.
    #[tokio::test]
    async fn a_corrupt_batch_is_answered_corrupt_message() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "pqr";
        let index = 0;

        let storage = DynoStore::new("abc", 12321, InMemory::new());
        let ctx = Context::with_state(storage);

        let batch = inflated::Batch::builder()
            .record(record(b"lorem"))
            .build()
            .and_then(deflated::Batch::try_from)?;

        let corrupt = {
            let mut payload = Vec::from(&batch.record_data[..]);
            let last = payload.len() - 1;
            payload[last] ^= 0xff;

            deflated::Batch {
                record_data: Bytes::from(payload),
                ..batch
            }
        };

        // The precondition. Without it a pass could mean the batch was
        // rejected for some unrelated reason.
        assert!(
            !corrupt.crc_matches()?,
            "the batch must actually be corrupt"
        );

        let frame = Frame {
            batches: vec![corrupt],
        };

        let partition_data = PartitionProduceData::default()
            .index(index)
            .records(Some(frame));

        // Built inline rather than through the `topic_data` helper above: that
        // one goes via the builder, which recomputes the CRC and would undo the
        // corruption under test.
        let data = TopicProduceData::default()
            .name(topic.into())
            .partition_data(Some(vec![partition_data]));

        let response = ProduceService
            .serve(
                ctx,
                ProduceRequest::default()
                    .transactional_id(None)
                    .acks(0)
                    .timeout_ms(0)
                    .topic_data(Some(vec![data])),
            )
            .await?;

        assert_eq!(
            answered(topic, index, ErrorCode::CorruptMessage, -1),
            response
        );

        Ok(())
    }

    /// A pre-v2 MessageSet is answered `UNSUPPORTED_FOR_MESSAGE_FORMAT` (43),
    /// and is not stored (#320).
    ///
    /// It used to be refused as `CORRUPT_MESSAGE` (2) — accurately, in that it
    /// fails the CRC gate, but for the wrong reason: a legacy checksum is a
    /// CRC-32 over a different range, so it could never match. The producer was
    /// told its data was damaged when the truth is that this broker does not
    /// read its record format.
    #[tokio::test]
    async fn a_pre_v2_message_set_is_answered_unsupported_for_message_format() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "stu";
        let index = 0;

        let storage = DynoStore::new("abc", 12321, InMemory::new());
        let ctx = Context::with_state(storage.clone());

        // A magic-0 MessageSet captured from `sarama` on Produce v0, decoded
        // the way the broker decodes a request body. Built from bytes rather
        // than from the builder: no builder in this crate can produce a record
        // format it does not support.
        let legacy = deflated::Batch::try_from(Bytes::from_static(&[
            0, 0, 0, 0, 0, 0, 0, 0, // base offset
            0, 0, 0, 80, // message size
            14, 140, 97, 161, // legacy CRC-32
            0,   // magic
            0,   // attributes
            255, 255, 255, 255, // null key
            0, 0, 0, 66, // value length
            181, 164, 112, 10, 42, 24, 68, 168, 93, 201, 190, 85, 75, 81, 82, 227, 134, 137, 91,
            20, 86, 4, 92, 187, 141, 103, 65, 71, 241, 103, 73, 174, 19, 227, 180, 158, 176, 4, 27,
            78, 34, 140, 106, 1, 209, 63, 255, 52, 206, 164, 132, 184, 32, 34, 45, 24, 162, 18,
            187, 77, 19, 3, 161, 102, 20, 14,
        ]))?;

        // The precondition. A pass could otherwise mean the batch was turned
        // away for some unrelated reason.
        assert_eq!(0, legacy.magic);
        assert!(!legacy.is_record_batch_v2());

        let data = TopicProduceData::default()
            .name(topic.into())
            .partition_data(Some(vec![
                PartitionProduceData::default()
                    .index(index)
                    .records(Some(Frame {
                        batches: vec![legacy],
                    })),
            ]));

        assert_eq!(
            answered(topic, index, ErrorCode::UnsupportedForMessageFormat, -1),
            ProduceService
                .serve(
                    ctx,
                    ProduceRequest::default()
                        .transactional_id(None)
                        .acks(0)
                        .timeout_ms(0)
                        .topic_data(Some(vec![data])),
                )
                .await?
        );

        // And nothing reached the log.
        assert_eq!(
            0,
            storage
                .offset_stage(&Topition::new(topic, index))
                .await
                .map(|stage| stage.high_watermark)
                .unwrap_or_default(),
            "a refused batch must not advance the high watermark"
        );

        Ok(())
    }

    /// **The converse, on the path most at risk from the check.**
    ///
    /// A LogAppendTime batch is accepted, even though the rewrite the service
    /// applies to it leaves its CRC stale. That only holds because the check
    /// runs *before* the rewrite; moving it after would reject every
    /// LogAppendTime produce, and every other test here uses CreateTime, so
    /// nothing else would notice.
    #[tokio::test]
    async fn a_log_append_time_batch_is_accepted() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "pqr";
        let index = 0;

        let storage = DynoStore::new("abc", 12321, InMemory::new());
        let ctx = Context::with_state(storage);

        let attributes =
            i16::from(BatchAttribute::default().timestamp(TimestampType::LogAppendTime));

        // The precondition: this batch really does take the rewrite.
        assert_eq!(
            TimestampType::LogAppendTime,
            BatchAttribute::try_from(attributes)?.timestamp
        );

        assert_eq!(
            answered(topic, index, ErrorCode::None, 0),
            ProduceService
                .serve(
                    ctx,
                    ProduceRequest::default()
                        .transactional_id(None)
                        .acks(0)
                        .timeout_ms(0)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(record(b"lorem"))
                                .attributes(attributes)
                        )?)
                )
                .await?
        );

        Ok(())
    }
}
