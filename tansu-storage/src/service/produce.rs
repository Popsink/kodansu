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

use std::time::{SystemTime, UNIX_EPOCH};

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, BatchAttribute, ErrorCode, ProduceRequest, ProduceResponse, TimestampType,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};
use tracing::{debug, error, instrument, warn};

use tansu_sans_io::acl::{Operation, Resource};

use crate::{Error, Result, Storage, Topition, authorized, storage_error_code};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`ProduceRequest`] returning [`ProduceResponse`].
/// ```
/// use bytes::Bytes;
/// use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
/// use tansu_sans_io::{
///     CreateTopicsRequest, ErrorCode, ProduceRequest,
///     create_topics_request::CreatableTopic,
///     produce_request::{PartitionProduceData, TopicProduceData},
///     record::{Record, deflated::Frame, inflated},
/// };
/// use tansu_storage::{CreateTopicsService, Error, ProduceService, StorageContainer};
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
/// let produce = {
///     let storage = storage.clone();
///     MapStateLayer::new(|_| storage).into_layer(ProduceService)
/// };
///
/// let partition = 0;
///
/// let response = produce
///     .serve(
///         Context::default(),
///         ProduceRequest::default().topic_data(Some(
///             [TopicProduceData::default()
///                 .name(name.into())
///                 .partition_data(Some(
///                     [PartitionProduceData::default()
///                         .index(partition)
///                         .records(Some(Frame {
///                             batches: vec![
///                                 inflated::Batch::builder()
///                                     .record(
///                                         Record::builder().value(
///                                             Bytes::from_static(
///                                                 b"Lorem ipsum dolor sit amet",
///                                             )
///                                             .into(),
///                                         ),
///                                     )
///                                     .build()
///                                     .and_then(TryInto::try_into)?,
///                             ],
///                         }))]
///                     .into(),
///                 ))]
///             .into(),
///         )),
///     )
///     .await?;
///
/// let topics = response.responses.as_deref().unwrap_or_default();
/// assert_eq!(1, topics.len());
/// let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(partitions[0].error_code)?
/// );
/// # Ok(())
/// # }
/// ```

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceService;

impl ApiKey for ProduceService {
    const KEY: i16 = ProduceRequest::KEY;
}

impl ProduceService {
    fn error(&self, index: i32, error_code: ErrorCode) -> PartitionProduceResponse {
        PartitionProduceResponse::default()
            .index(index)
            .error_code(error_code.into())
            .base_offset(-1)
            .log_append_time_ms(Some(-1))
            .log_start_offset(Some(0))
            .record_errors(Some([].into()))
            .error_message(None)
            .current_leader(None)
    }

    #[instrument(skip_all)]
    async fn partition<G>(
        &self,
        ctx: &Context<G>,
        transaction_id: Option<&str>,
        name: &str,
        partition: PartitionProduceData,
    ) -> PartitionProduceResponse
    where
        G: Storage,
    {
        if let Some(records) = partition.records {
            let mut base_offset = None;

            for mut batch in records.batches {
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
                    return self.error(partition.index, ErrorCode::UnsupportedForMessageFormat);
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
                        return self.error(partition.index, ErrorCode::CorruptMessage);
                    }

                    // The batch cannot be re-encoded to check it, so it
                    // cannot be stored either. Same answer.
                    Err(err) => {
                        warn!(?err, ?tp, "cannot verify a batch crc");
                        return self.error(partition.index, ErrorCode::CorruptMessage);
                    }
                }

                if BatchAttribute::try_from(batch.attributes)
                    .map(|attributes| attributes.timestamp == TimestampType::LogAppendTime)
                    .unwrap_or_default()
                {
                    let base_timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|duration| duration.as_millis() as i64)
                        .unwrap_or_default();

                    batch.base_timestamp = base_timestamp;
                    batch.max_timestamp = base_timestamp;
                }

                match ctx
                    .state()
                    .produce(transaction_id, &tp, batch)
                    .await
                    .inspect_err(|err| match err {
                        // Expected idempotent-producer outcomes (a retried batch
                        // already persisted) — routine, not a failure (#37).
                        err if err.is_expected_idempotent_outcome() => {
                            debug!(?err)
                        }
                        storage_api @ Error::Api(_) => {
                            warn!(?storage_api)
                        }
                        otherwise => error!(?otherwise),
                    }) {
                    Ok(offset) => _ = base_offset.get_or_insert(offset),

                    Err(Error::Api(error_code)) => {
                        debug!(?self, ?error_code);
                        return self.error(partition.index, error_code);
                    }

                    Err(otherwise) => {
                        // A transient storage failure (e.g. an S3 error under
                        // load) was previously surfaced as UNKNOWN_SERVER_ERROR
                        // (-1), which clients treat as fatal and drop the whole
                        // batch (#6). Map storage errors to a *retriable* code
                        // so clients retry instead, and log the underlying error
                        // with its topic-partition so it isn't opaque.
                        let error_code = storage_error_code(&otherwise);
                        warn!(?otherwise, ?tp, ?error_code, "produce storage error");
                        return self.error(partition.index, error_code);
                    }
                }
            }

            if let Some(base_offset) = base_offset {
                PartitionProduceResponse::default()
                    .index(partition.index)
                    .error_code(ErrorCode::None.into())
                    .base_offset(base_offset)
                    .log_append_time_ms(Some(-1))
                    .log_start_offset(Some(0))
                    .record_errors(Some([].into()))
                    .error_message(None)
                    .current_leader(None)
            } else {
                self.error(partition.index, ErrorCode::UnknownServerError)
            }
        } else {
            self.error(partition.index, ErrorCode::UnknownServerError)
        }
    }

    #[instrument(skip_all)]
    async fn topic<G>(
        &self,
        ctx: &Context<G>,
        transaction_id: Option<&str>,
        topic: TopicProduceData,
    ) -> TopicProduceResponse
    where
        G: Storage,
    {
        // Per topic, and refused per partition, because that is the shape the
        // response has: a client reads a partition's error code, and a blanket
        // failure at the top would tell it every partition of every topic in
        // the request had failed (#363).
        let allowed = authorized(ctx, Resource::Topic, &topic.name, Operation::Write).await;

        let mut partitions = vec![];

        if let Some(partition_data) = topic.partition_data {
            for partition in partition_data {
                partitions.push(if allowed {
                    self.partition(ctx, transaction_id, &topic.name, partition)
                        .await
                } else {
                    PartitionProduceResponse::default()
                        .index(partition.index)
                        .error_code(ErrorCode::TopicAuthorizationFailed.into())
                        .base_offset(-1)
                        .log_append_time_ms(Some(-1))
                        .log_start_offset(Some(-1))
                        .record_errors(Some([].into()))
                        .error_message(None)
                        .current_leader(None)
                })
            }
        }

        TopicProduceResponse::default()
            .name(topic.name)
            .partition_responses(Some(partitions))
    }
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
        let mut responses = Vec::with_capacity(
            req.topic_data
                .as_ref()
                .map_or(0, |topic_data| topic_data.len()),
        );

        if let Some(topics) = req.topic_data {
            for topic in topics {
                responses.push(
                    self.topic(&ctx, req.transactional_id.as_deref(), topic)
                        .await,
                )
            }
        }

        Ok(ProduceResponse::default()
            .responses(Some(responses))
            .throttle_time_ms(Some(0))
            .node_endpoints(None))
    }
}

#[cfg(all(test, feature = "dynostore"))]
mod tests {
    use super::*;
    use crate::{Error, dynostore::DynoStore, service::init_producer_id::InitProducerIdService};
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use rama::Context;
    use tansu_sans_io::{
        ErrorCode, InitProducerIdRequest,
        record::{
            Record,
            deflated::{self, Frame},
            inflated,
        },
    };
    use tracing::subscriber::DefaultGuard;

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

    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        use tracing::Level;
        use tracing_subscriber::fmt::format::FmtSpan;

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_max_level(Level::DEBUG)
                .with_span_events(FmtSpan::ACTIVE)
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    fn topic_data(
        topic: &str,
        index: i32,
        builder: inflated::Builder,
    ) -> Result<Option<Vec<TopicProduceData>>> {
        builder
            .build()
            .and_then(deflated::Batch::try_from)
            .map(|deflated| {
                let partition_data =
                    PartitionProduceData::default()
                        .index(index)
                        .records(Some(Frame {
                            batches: vec![deflated],
                        }));

                Some(vec![
                    TopicProduceData::default()
                        .name(topic.into())
                        .partition_data(Some(vec![partition_data])),
                ])
            })
            .map_err(Into::into)
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

        let transactional_id = None;
        let acks = 0;
        let timeout_ms = 0;

        let storage = DynoStore::new(cluster, node, InMemory::new());
        let ctx = Context::with_state(storage);
        let service = ProduceService;

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(0)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            service
                .serve(
                    ctx,
                    ProduceRequest::default()
                        .transactional_id(transactional_id)
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(
                                    Record::builder().value(Bytes::from_static(b"lorem").into())
                                )
                                .producer_id(54345)
                        )?)
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

        let init_producer_id = InitProducerIdService;

        let producer = init_producer_id
            .serve(
                ctx.clone(),
                InitProducerIdRequest::default()
                    .transactional_id(None)
                    .transaction_timeout_ms(0)
                    .producer_id(Some(-1))
                    .producer_epoch(Some(-1)),
            )
            .await?;

        let request = ProduceService;

        let transactional_id = None;
        let acks = 0;
        let timeout_ms = 0;

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(0)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx.clone(),
                    ProduceRequest::default()
                        .transactional_id(transactional_id.clone())
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                ))
                                .producer_id(producer.producer_id)
                        )?)
                )
                .await?
        );

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(1)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx.clone(),
                    ProduceRequest::default()
                        .transactional_id(transactional_id.clone())
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(Record::builder().value(
                                    Bytes::from_static(b"consectetur adipiscing elit").into()
                                ))
                                .record(
                                    Record::builder()
                                        .value(Bytes::from_static(b"sed do eiusmod tempor").into())
                                )
                                .base_sequence(1)
                                .last_offset_delta(1)
                                .producer_id(producer.producer_id)
                        )?)
                )
                .await?
        );

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(3)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx,
                    ProduceRequest::default()
                        .transactional_id(transactional_id.clone())
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(
                                    Record::builder()
                                        .value(Bytes::from_static(b"incididunt ut labore").into())
                                )
                                .base_sequence(3)
                                .producer_id(producer.producer_id)
                        )?)
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

        let init_producer_id = InitProducerIdService;

        let producer = init_producer_id
            .serve(
                ctx.clone(),
                InitProducerIdRequest::default()
                    .transactional_id(None)
                    .transaction_timeout_ms(0)
                    .producer_id(Some(-1))
                    .producer_epoch(Some(-1)),
            )
            .await?;

        let request = ProduceService;

        let transactional_id = None;
        let acks = 0;
        let timeout_ms = 0;

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(0)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx.clone(),
                    ProduceRequest::default()
                        .transactional_id(transactional_id.clone())
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                ))
                                .producer_id(producer.producer_id)
                        )?)
                )
                .await?
        );

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(0)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx,
                    ProduceRequest::default()
                        .transactional_id(transactional_id)
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                ))
                                .producer_id(producer.producer_id)
                        )?)
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

        let init_producer_id = InitProducerIdService;

        let producer = init_producer_id
            .serve(
                ctx.clone(),
                InitProducerIdRequest::default()
                    .transactional_id(None)
                    .transaction_timeout_ms(0)
                    .producer_id(Some(-1))
                    .producer_epoch(Some(-1)),
            )
            .await?;

        let request = ProduceService;

        let transactional_id = None;
        let acks = 0;
        let timeout_ms = 0;

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(0)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx.clone(),
                    ProduceRequest::default()
                        .transactional_id(transactional_id.clone())
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                ))
                                .producer_id(producer.producer_id)
                        )?)
                )
                .await?
        );

        assert_eq!(
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::OutOfOrderSequenceNumber.into())
                                .base_offset(-1)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
            request
                .serve(
                    ctx,
                    ProduceRequest::default()
                        .transactional_id(transactional_id)
                        .acks(acks)
                        .timeout_ms(timeout_ms)
                        .topic_data(topic_data(
                            topic,
                            index,
                            inflated::Batch::builder()
                                .record(Record::builder().value(
                                    Bytes::from_static(b"Lorem ipsum dolor sit amet").into()
                                ))
                                .base_sequence(2)
                                .producer_id(producer.producer_id)
                        )?)
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
            .record(Record::builder().value(Bytes::from_static(b"lorem").into()))
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
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::CorruptMessage.into())
                                .base_offset(-1)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
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
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::UnsupportedForMessageFormat.into())
                                .base_offset(-1)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
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
            ProduceResponse::default()
                .responses(Some(vec![
                    TopicProduceResponse::default()
                        .name(topic.into())
                        .partition_responses(Some(vec![
                            PartitionProduceResponse::default()
                                .index(index)
                                .error_code(ErrorCode::None.into())
                                .base_offset(0)
                                .log_append_time_ms(Some(-1))
                                .log_start_offset(Some(0))
                                .record_errors(Some(vec![]))
                                .error_message(None)
                                .current_leader(None)
                        ]))
                ]))
                .throttle_time_ms(Some(0))
                .node_endpoints(None),
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
                                .record(
                                    Record::builder().value(Bytes::from_static(b"lorem").into())
                                )
                                .attributes(attributes)
                        )?)
                )
                .await?
        );

        Ok(())
    }
}
