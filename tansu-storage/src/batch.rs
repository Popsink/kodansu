// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram},
};
use tansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    record::{Record, deflated, inflated},
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tokio::time::sleep;
use tracing::{debug, instrument, warn};
use url::Url;
use uuid::Uuid;

use crate::delegate::storage_methods;
use crate::{
    AclBinding, AclFilter, AssignmentDoc, AssignmentOutcome, AutoTopicCreate,
    BrokerRegistrationRequest, CommittedOffset, Error, GenerationDoc, ListOffsetResponse, METER,
    MemberDoc, MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage,
    ProducerIdResponse, QuotaAlteration, QuotaEntity, QuotaFilterComponent, QuotaLimits, Quotas,
    Result, ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError, Version,
};

static BATCH_REQUESTS_LENGTH: LazyLock<Gauge<u64>> =
    LazyLock::new(|| METER.u64_gauge("batch_request_gauge").build());

static BATCH_RESPONSES_LENGTH: LazyLock<Gauge<u64>> =
    LazyLock::new(|| METER.u64_gauge("batch_response_gauge").build());

static BATCH_TICKET_POLL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_ticket_poll")
        .with_description("The number of ticket polls")
        .build()
});

static SEND_QUEUED_PRODUCED_RECORDS_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_send_queued_records")
        .with_description("The number of produced send queued records")
        .build()
});

static SEND_QUEUED_WAKE_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_send_queued_wake_event")
        .with_description("The number of wake events sent")
        .build()
});

static PRODUCE_REQUEST_MINIMUM_SIZE_TRIGGER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_produce_minimum_size_trigger")
        .with_description("The number of times the minimum size was a trigger")
        .build()
});

static PRODUCE_REQUEST_YOUR_TICKET_IS_READY: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_produce_your_ticket_is_ready")
        .with_description("The number of notifications that your ticket was ready while waiting")
        .build()
});

static PRODUCE_REQUEST_TIMEOUT_EXPIRED_TRIGGER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_produce_timeout_expired_trigger")
        .with_description("The number of times the timeout expiry was a trigger")
        .build()
});

static PRODUCE_REQUEST_QUEUED_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_produce_queued")
        .with_description("The number of produce requests queued")
        .build()
});

static PRODUCE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("batch_produce_duration")
        .with_unit("ms")
        .with_description("The batch produce latency in milliseconds")
        .build()
});

#[derive(Clone, Debug)]
struct Ticket<G> {
    id: Uuid,
    batcher: ProduceRequestBatcher<G>,
}

impl<G> Ticket<G> {
    fn new(batcher: ProduceRequestBatcher<G>) -> Self {
        Self {
            id: Uuid::now_v7(),
            batcher,
        }
    }
}

impl<G> AsRef<Uuid> for Ticket<G> {
    fn as_ref(&self) -> &Uuid {
        &self.id
    }
}

impl<G> Future for Ticket<G> {
    type Output = Result<i64, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut responses = self.batcher.responses.lock()?;

        match responses.remove(&self.id) {
            Some(BatchResponse::Response(response)) => {
                BATCH_TICKET_POLL.add(1, &[KeyValue::new("outcome", "ready")]);
                Poll::Ready(Ok(response))
            }
            Some(BatchResponse::Waker(_)) | None => {
                BATCH_TICKET_POLL.add(1, &[KeyValue::new("outcome", "pending")]);
                _ = responses.insert(self.id, BatchResponse::Waker(cx.waker().clone()));
                Poll::Pending
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TopitionProducerId {
    topition: Topition,
    producer_id: i64,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct BatchRequest {
    id: Uuid,
    batch: deflated::Batch,
}

#[derive(Clone, Debug)]
enum BatchResponse {
    Waker(Waker),
    Response(i64),
}

#[derive(Clone, Debug)]
pub(crate) struct ProduceRequestBatcher<G> {
    storage: G,
    maximum_delay: Option<Duration>,
    minimum_size: Option<usize>,

    requests: Arc<Mutex<BTreeMap<TopitionProducerId, Vec<BatchRequest>>>>,
    responses: Arc<Mutex<BTreeMap<Uuid, BatchResponse>>>,
}

impl<G> ProduceRequestBatcher<G> {
    fn update_metrics(&self) -> Result<()> {
        self.requests
            .lock()
            .map_err(Into::into)
            .map(|requests| requests.values().map(|queue| queue.len() as u64).sum())
            .map(|length| BATCH_REQUESTS_LENGTH.record(length, &[]))
            .and(
                self.responses
                    .lock()
                    .map_err(Into::into)
                    .map(|responses| responses.len() as u64)
                    .map(|length| BATCH_RESPONSES_LENGTH.record(length, &[])),
            )
    }
}

impl<G> ProduceRequestBatcher<G>
where
    G: Storage,
{
    pub(crate) fn new(storage: G) -> Self {
        Self {
            storage,
            minimum_size: Default::default(),
            maximum_delay: Default::default(),

            requests: Arc::new(Mutex::new(BTreeMap::new())),
            responses: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn with_minimum_size(self, minimum_size: Option<usize>) -> Self {
        Self {
            minimum_size,
            ..self
        }
    }

    pub(crate) fn with_maximum_delay(self, maximum_delay: Option<Duration>) -> Self {
        Self {
            maximum_delay,
            ..self
        }
    }

    #[instrument(skip(self, transaction_id, topition, producer_id))]
    async fn send_queued(
        &self,
        id: &Uuid,
        transaction_id: Option<&str>,
        topition: &Topition,
        producer_id: i64,
    ) -> Result<(), Error> {
        let Some(queued) = self.requests.lock().map(|mut requests| {
            BATCH_REQUESTS_LENGTH
                .record(requests.values().map(|queue| queue.len() as u64).sum(), &[]);

            requests.remove(&TopitionProducerId {
                topition: topition.to_owned(),
                producer_id,
            })
        })?
        else {
            BATCH_REQUESTS_LENGTH.record(0, &[]);

            return Ok(());
        };

        let owners = queued
            .iter()
            .map(|batch_request| batch_request.id)
            .collect::<BTreeSet<_>>();

        debug!(owners = owners.len());

        let attributes = [KeyValue::new("topic", topition.topic.clone())];

        if let Some(queued) = combine(queued.into_iter().map(|queued| queued.batch).collect())? {
            let record_count = (queued.last_offset_delta + 1) as u64;

            let offset = self
                .storage
                .produce(transaction_id, topition, queued)
                .await
                .inspect(|offset| debug!(offset))?;

            SEND_QUEUED_PRODUCED_RECORDS_COUNTER.add(record_count, &attributes);

            self.responses.lock().map(|mut responses| {
                for owner in owners {
                    if let Some(BatchResponse::Waker(waker)) =
                        responses.insert(owner, BatchResponse::Response(offset))
                    {
                        debug!(waking = %owner);
                        SEND_QUEUED_WAKE_COUNTER.add(1, &attributes);
                        waker.wake();
                    }
                }
            })?;
        }

        Ok(())
    }
}

/// The batching produce, and the reason it is an inherent method.
///
/// `#[instrument]` cannot be generated — a `macro_rules!` does not expand into
/// attribute position — so the `produce` generated below forwards here and the
/// span sits on this frame (#551).
impl<G> ProduceRequestBatcher<G>
where
    G: Storage + Clone,
{
    #[instrument(skip_all, fields(transaction_id, topic = topition.topic, partition = topition.partition))]
    async fn batched_produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        let Some(maximum_delay) = self.maximum_delay else {
            return self.storage.produce(transaction_id, topition, batch).await;
        };

        let start = SystemTime::now();

        let attributes = [KeyValue::new("topic", topition.topic.clone())];

        let producer_id = batch.producer_id;

        let topition_producer_id = TopitionProducerId {
            topition: topition.to_owned(),
            producer_id,
        };

        let ticket = self.requests.lock().map(|mut requests| {
            let ticket = Ticket::new(self.clone());

            let queue = requests.entry(topition_producer_id.clone()).or_default();

            queue.push(BatchRequest {
                id: ticket.id,
                batch,
            });

            PRODUCE_REQUEST_QUEUED_COUNTER.add(1, &attributes);
            debug!(queue_len = queue.len());

            ticket
        })?;

        debug!(ticket = %ticket.id);

        let mut iteration = -1;

        loop {
            self.update_metrics()?;

            let ticket = ticket.clone();
            let id = ticket.id;

            iteration += 1;

            let queued_bytes = self
                .requests
                .lock()
                .map(|requests| {
                    requests
                        .get(&topition_producer_id)
                        .map(|queue| {
                            queue
                                .iter()
                                .map(|batch_request| batch_request.batch.record_data.len())
                                .sum::<usize>()
                        })
                        .unwrap_or_default()
                })
                .inspect(|queued_bytes| debug!(queued_bytes))?;

            if self
                .minimum_size
                .inspect(|minimum_size| debug!(minimum_size, queued_bytes))
                .is_some_and(|minimum_size| queued_bytes > minimum_size)
            {
                PRODUCE_REQUEST_MINIMUM_SIZE_TRIGGER.add(1, &attributes);

                self.send_queued(&id, transaction_id, topition, producer_id)
                    .await?;
            }

            let patience = sleep(maximum_delay);

            tokio::select! {
                response = ticket  => {
                    let elapsed = start.elapsed().map_or(0, |duration| duration.as_millis() as u64);
                    debug!(ready = %id, elapsed, iteration);
                    PRODUCE_REQUEST_YOUR_TICKET_IS_READY.add(1, &attributes);
                    PRODUCE_DURATION.record(elapsed, &attributes);
                    self.update_metrics()?;
                    return response;
                }

                _ = patience => {
                    if iteration > 1 {
                        warn!(ticket = %id, iteration);
                    }

                    PRODUCE_REQUEST_TIMEOUT_EXPIRED_TRIGGER.add(1, &attributes);
                    self.send_queued(&id, transaction_id, topition, producer_id).await?;
                }
            }
        }
    }
}

/// One body per method: `produce` batches, the other fifty-one forward.
///
/// Invoked in expression position, which `#[async_trait]` can see through —
/// an item-position call inside the `impl` would still be unexpanded when
/// `async_trait` rewrites it (#551).
macro_rules! batcher_body {
    ($s:ident, produce, ($($arg:ident),*)) => {{
        $s.batched_produce($($arg),*).await
    }};

    ($s:ident, $name:ident, ($($arg:ident),*)) => {{
        $s.storage.$name($($arg),*).await
    }};
}

/// `Storage` for the produce batcher, generated from
/// [`storage_methods`](crate::delegate::storage_methods).
///
/// Fifty-one of the fifty-two are `self.storage.m(args).await`, written out by
/// hand until #551.
macro_rules! batcher_delegation {
    (
        $(fn $name:ident(&$s:ident $(, $arg:ident : $ty:ty)* $(,)?) -> $ret:ty;)*
        $(sync fn $sname:ident(&$ss:ident $(, $sarg:ident : $sty:ty)* $(,)?) -> $sret:ty;)*
    ) => {
        #[async_trait]
        impl<G> Storage for ProduceRequestBatcher<G>
        where
            G: Storage + Clone,
        {
            $(
                async fn $name(&$s $(, $arg: $ty)*) -> $ret {
                    batcher_body!($s, $name, ($($arg),*))
                }
            )*
            $(
                fn $sname(&$ss $(, $sarg: $sty)*) -> $sret {
                    $ss.storage.$sname($($sarg),*)
                }
            )*
        }
    };
}

storage_methods!(batcher_delegation);

#[instrument(skip_all)]
fn combine(batches: Vec<deflated::Batch>) -> Result<Option<deflated::Batch>> {
    debug!(len = batches.len());

    let mut i = batches.into_iter();

    let Some(first) = i.next() else {
        return Ok(None);
    };

    let mut sink = inflated::Batch::try_from(first)?;
    debug!(
        sink.base_offset,
        sink.last_offset_delta, sink.base_sequence, sink.max_timestamp
    );

    for batch in i {
        let batch = inflated::Batch::try_from(batch)?;

        debug!(
            sink.last_offset_delta,
            sink.max_timestamp, batch.base_offset, batch.last_offset_delta, batch.base_sequence
        );

        sink.records.append(
            &mut batch
                .records
                .into_iter()
                .map(|record| Record {
                    offset_delta: record.offset_delta + sink.last_offset_delta + 1,
                    timestamp_delta: record.timestamp_delta
                        + (sink.base_timestamp - batch.base_timestamp),
                    ..record
                })
                .collect::<Vec<_>>(),
        );

        sink.last_offset_delta += batch.last_offset_delta + 1;
        sink.max_timestamp = sink.max_timestamp.max(batch.max_timestamp);
    }

    debug!(
        sink.base_offset,
        sink.last_offset_delta, sink.base_sequence, sink.max_timestamp
    );

    deflated::Batch::try_from(sink)
        .map(Some)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use tansu_sans_io::{
        BatchAttribute,
        record::{Record, deflated, inflated},
    };
    use tokio::{task::yield_now, time::advance};

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct FlightRecorder {
        produced: Arc<Mutex<BTreeMap<Topition, Vec<deflated::Batch>>>>,
    }

    impl FlightRecorder {
        fn new() -> Self {
            Self {
                produced: Arc::new(Mutex::new(BTreeMap::new())),
            }
        }

        fn produced(&self, topition: &Topition) -> Result<Option<Vec<inflated::Batch>>> {
            self.produced
                .as_ref()
                .lock()
                .map_err(Into::into)
                .and_then(|produced| {
                    produced
                        .get(topition)
                        .map(|produced| {
                            produced
                                .iter()
                                .map(|deflated| {
                                    inflated::Batch::try_from(deflated).map_err(Into::into)
                                })
                                .collect::<Result<Vec<_>>>()
                        })
                        .transpose()
                })
        }
    }

    /// A stub: `produce` records, everything else is unreachable in these
    /// tests and says so.
    ///
    /// 346 lines of `unimplemented!()` until #551. `unused_variables` is
    /// allowed for the whole impl rather than `_`-prefixing fifty parameter
    /// names, because the names come from the shared signature list and the
    /// list has one reader that does use them.
    macro_rules! recorder_body {
        ($s:ident, produce, ($transaction_id:ident, $topition:ident, $batch:ident)) => {{
            $s.produced
                .lock()
                .map(|mut produced| {
                    _ = produced
                        .entry($topition.to_owned())
                        .or_default()
                        .push($batch);
                    0
                })
                .map_err(Into::into)
        }};

        ($s:ident, $name:ident, ($($arg:ident),*)) => {{ unimplemented!() }};
    }

    macro_rules! recorder_delegation {
        (
            $(fn $name:ident(&$s:ident $(, $arg:ident : $ty:ty)* $(,)?) -> $ret:ty;)*
            $(sync fn $sname:ident(&$ss:ident $(, $sarg:ident : $sty:ty)* $(,)?) -> $sret:ty;)*
        ) => {
            #[async_trait]
            #[allow(unused_variables)]
            impl Storage for FlightRecorder {
                $(
                    async fn $name(&$s $(, $arg: $ty)*) -> $ret {
                        recorder_body!($s, $name, ($($arg),*))
                    }
                )*
                $(
                    fn $sname(&$ss $(, $sarg: $sty)*) -> $sret {
                        unimplemented!()
                    }
                )*
            }
        };
    }

    storage_methods!(recorder_delegation);

    fn into_batch(
        attributes: i16,
        producer_id: i64,
        producer_epoch: i16,
        base_offset: i64,
        records: &[Bytes],
    ) -> Result<deflated::Batch> {
        let base_sequence = 0;

        let mut inflated = inflated::Batch::builder()
            .attributes(attributes)
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_offset(base_offset)
            .last_offset_delta(records.len() as i32 - 1)
            .base_sequence(base_sequence);

        for (offset_delta, value) in records.iter().enumerate() {
            inflated = inflated.record(
                Record::builder()
                    .value(value.clone().into())
                    .offset_delta(offset_delta as i32),
            );
        }

        inflated
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))
            .map_err(Into::into)
    }

    #[tokio::test(start_paused = true)]
    async fn single_produce_in_window() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::new();
        let storage =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_offset = 0;
        let attributes: i16 = BatchAttribute::default().into();

        let transaction_id = None;
        let abc0 = Topition::new("abc", 0);

        const A: Bytes = Bytes::from_static(b"a");
        const B: Bytes = Bytes::from_static(b"b");
        const C: Bytes = Bytes::from_static(b"c");

        let batch_a = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                storage
                    .produce(
                        transaction_id,
                        &abc0,
                        into_batch(
                            attributes,
                            producer_id,
                            producer_epoch,
                            base_offset,
                            &[A, B, C],
                        )?,
                    )
                    .await
            })
        };

        advance(ADVANCE_DELAY).await;
        yield_now().await;

        let response_a = batch_a
            .await
            .expect("join_handle")
            .inspect(|produce_response| debug!(?produce_response))?;
        assert_eq!(0, response_a);

        let sent = recorder.produced(&abc0)?.unwrap();
        assert_eq!(1, sent.len());
        assert_eq!(3, sent[0].records.len());
        assert_eq!(Some(A), sent[0].records[0].value());
        assert_eq!(Some(B), sent[0].records[1].value());
        assert_eq!(Some(C), sent[0].records[2].value());

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn two_produces_in_window() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::new();
        let storage =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_offset = 0;
        let attributes: i16 = BatchAttribute::default().into();

        let transaction_id = None;
        let abc0 = Topition::new("abc", 0);

        const A: Bytes = Bytes::from_static(b"a");
        const B: Bytes = Bytes::from_static(b"b");
        const C: Bytes = Bytes::from_static(b"c");

        let batch_a = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                storage
                    .produce(
                        transaction_id,
                        &abc0,
                        into_batch(
                            attributes,
                            producer_id,
                            producer_epoch,
                            base_offset,
                            &[A, B, C],
                        )?,
                    )
                    .await
            })
        };

        const D: Bytes = Bytes::from_static(b"d");
        const E: Bytes = Bytes::from_static(b"e");

        let batch_b = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                storage
                    .produce(
                        transaction_id,
                        &abc0,
                        into_batch(
                            attributes,
                            producer_id,
                            producer_epoch,
                            base_offset,
                            &[D, E],
                        )?,
                    )
                    .await
            })
        };

        advance(ADVANCE_DELAY).await;
        yield_now().await;

        let response_a = batch_a
            .await
            .expect("join_handle")
            .inspect(|produce_response| debug!(?produce_response))?;
        assert_eq!(0, response_a);

        let response_b = batch_b
            .await
            .expect("join_handle")
            .inspect(|produce_response| debug!(?produce_response))?;
        assert_eq!(0, response_b);

        let sent = recorder.produced(&abc0)?.unwrap();
        assert_eq!(1, sent.len());
        assert_eq!(5, sent[0].records.len());
        assert_eq!(Some(A), sent[0].records[0].value());
        assert_eq!(Some(B), sent[0].records[1].value());
        assert_eq!(Some(C), sent[0].records[2].value());
        assert_eq!(Some(D), sent[0].records[3].value());
        assert_eq!(Some(E), sent[0].records[4].value());

        Ok(())
    }

    #[test]
    fn combine_empty() -> Result<()> {
        assert_eq!(None, combine(vec![])?);
        Ok(())
    }

    fn into_batches(
        attributes: i16,
        producer_id: i64,
        producer_epoch: i16,
        base_offset: i64,
        batches: &[Vec<Bytes>],
    ) -> Result<Vec<deflated::Batch>> {
        let mut split = vec![];
        let mut base_sequence = 0;

        for batch in batches {
            let mut inflated = inflated::Batch::builder()
                .attributes(attributes)
                .producer_id(producer_id)
                .producer_epoch(producer_epoch)
                .base_offset(base_offset)
                .last_offset_delta(batch.len() as i32 - 1)
                .base_sequence(base_sequence);

            for (offset_delta, value) in batch.iter().enumerate() {
                inflated = inflated.record(
                    Record::builder()
                        .value(value.clone().into())
                        .offset_delta(offset_delta as i32),
                );
            }

            split.push(
                inflated
                    .build()
                    .and_then(TryInto::try_into)
                    .inspect(|deflated| debug!(?deflated))?,
            );

            base_sequence += batch.len() as i32;
        }

        Ok(split)
    }

    // A table of split/combine cases, each with its own assertion: the branch
    // count #555's nursery gate sees is the size of the table.
    #[allow(clippy::cognitive_complexity)]
    #[test]
    fn combine_batches() -> Result<()> {
        let batches = [
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
            ],
            vec![Bytes::from_static(b"f"), Bytes::from_static(b"g")],
            vec![Bytes::from_static(b"i")],
            vec![Bytes::from_static(b"j")],
            vec![Bytes::from_static(b"k")],
            vec![
                Bytes::from_static(b"p"),
                Bytes::from_static(b"q"),
                Bytes::from_static(b"r"),
                Bytes::from_static(b"s"),
            ],
        ];

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_offset = 0;
        let attributes: i16 = BatchAttribute::default().into();
        let base_sequence: i32 = 0;

        let combined = inflated::Batch::try_from(
            into_batches(
                attributes,
                producer_id,
                producer_epoch,
                base_offset,
                &batches[..],
            )
            .and_then(combine)?
            .expect("a batch"),
        )?;

        assert_eq!(combined.producer_id, producer_id);
        assert_eq!(combined.producer_epoch, producer_epoch);
        assert_eq!(combined.base_sequence, base_sequence);
        assert_eq!(combined.base_offset, base_offset);
        assert_eq!(combined.attributes, attributes);

        let index = 0;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[0][0].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 1;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[0][1].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 2;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[0][2].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 3;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[1][0].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 4;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[1][1].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 5;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[2][0].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 6;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[3][0].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 7;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[4][0].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 8;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[5][0].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 9;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[5][1].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 10;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[5][2].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 11;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[5][3].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        Ok(())
    }
}

/// The wrapper stack must answer exactly what the engine it wraps answers
/// (#273).
///
/// `Storage` has methods with default bodies, and `ProduceRequestBatcher` is
/// applied **unconditionally** in the `s3` and `gs` builder arms. So a method
/// this wrapper forgets to delegate is not a compile error — it silently becomes
/// the default, in production only. Two shipped optimisations were inert that
/// way for an unknown period, and the suite could not see it because the
/// `memory://` arm is unwrapped: in-memory tests exercised the optimised paths
/// that production never reached.
///
/// This is the regression that would have caught it, and it is deliberately
/// written as *parity with the wrapped engine* rather than as an assertion about
/// any particular value — a future defaulted method is caught by the same shape.
#[cfg(all(test, feature = "dynostore"))]
mod wrapper_parity {
    use bytes::Bytes;
    use object_store::memory::InMemory;

    use tansu_sans_io::{acl, resource};

    use super::*;
    use crate::dynostore::DynoStore;

    const CLUSTER: &str = "tansu";
    const NODE: i32 = 111;

    #[tokio::test]
    async fn the_batcher_answers_what_it_wraps() -> Result<(), Error> {
        let bucket = InMemory::new();

        // One bucket, two views of it: the engine, and the engine behind the
        // wrapper production always applies.
        let bare = DynoStore::new(CLUSTER, NODE, bucket.clone());
        let wrapped = ProduceRequestBatcher::new(DynoStore::new(CLUSTER, NODE, bucket.clone()));

        let topic = "parity";
        _ = bare
            .create_topic(
                CreatableTopic::default()
                    .name(topic.into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic, 0);

        for n in 0..3 {
            let batch = inflated::Batch::builder()
                .record(Record::builder().value(Some(Bytes::from(format!("m-{n}")))))
                .build()
                .and_then(deflated::Batch::try_from)?;

            _ = bare.produce(None, &topition, batch).await?;
        }

        // `offset_stage_at`: the default body redirects to `offset_stage`, which
        // reads the cluster-wide `meta.json` — a different implementation with a
        // separately derived `last_stable`, so this is parity of an answer, not
        // just of a cost.
        for isolation in [
            IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted,
        ] {
            assert_eq!(
                bare.offset_stage_at(&topition, isolation).await?,
                wrapped.offset_stage_at(&topition, isolation).await?,
                "offset_stage_at diverges through the wrapper at {isolation:?}",
            );
        }

        // The decomposed layout (#359). None of these has a default body, so
        // today the compiler is the guard — but that is exactly what was true
        // of the legacy `read_group` before a default was added to it, and the
        // parity shape is what survives someone adding one.
        let group = "decomposed";

        let member = MemberDoc {
            last_contact_ms: 1_000,
            session_timeout_ms: 45_000,
            ..Default::default()
        };

        _ = bare
            .write_group_member(group, "m-1", member.clone(), None)
            .await
            .expect("seed member document");

        assert_eq!(
            bare.read_group_member(group, "m-1").await?,
            wrapped.read_group_member(group, "m-1").await?,
            "read_group_member diverges through the wrapper",
        );

        assert_eq!(
            bare.list_group_members(group).await?,
            wrapped.list_group_members(group).await?,
            "list_group_members diverges through the wrapper",
        );

        _ = bare
            .update_group_generation(
                group,
                GenerationDoc {
                    generation_id: 7,
                    session_timeout_ms: 45_000,
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("seed generation");

        assert_eq!(
            bare.read_group_generation(group).await?,
            wrapped.read_group_generation(group).await?,
            "read_group_generation diverges through the wrapper",
        );

        let assignment = AssignmentDoc {
            generation_id: 7,
            leader: "m-1".into(),
            protocol_type: "consumer".into(),
            protocol_name: "range".into(),
            assignments: BTreeMap::from([("m-1".to_owned(), Bytes::from_static(&[1, 2]))]),
            assigned_at_ms: 9,
        };

        // Create-only: the wrapper must see the *loser*'s outcome, not a
        // silent success, or `SyncGroup` retries would look like new writes.
        assert!(matches!(
            bare.create_group_assignment(group, 7, assignment.clone())
                .await?,
            AssignmentOutcome::Created(_)
        ));

        assert_eq!(
            AssignmentOutcome::AlreadyExists(Box::new(assignment)),
            wrapped
                .create_group_assignment(group, 7, {
                    AssignmentDoc {
                        leader: "someone-else".into(),
                        ..bare
                            .read_group_assignment(group, 7)
                            .await?
                            .expect("assignment")
                    }
                })
                .await?,
            "create_group_assignment diverges through the wrapper: an existing \
             assignment must be adopted, never overwritten",
        );

        assert_eq!(
            bare.read_group_assignment(group, 7).await?,
            wrapped.read_group_assignment(group, 7).await?,
            "read_group_assignment diverges through the wrapper",
        );

        // The startup assertion. A wrapper that swallowed it would let a
        // binary start against a cluster in a layout it does not write, which
        // is the one thing this object exists to stop.
        bare.assert_group_schema().await?;
        wrapped.assert_group_schema().await?;

        // The ACLs (#363). A wrapper that swallowed `describe_acls` would
        // report a cluster as having no rules — which on a fail-closed broker
        // is not "no opinion", it is the most consequential answer there is,
        // and the shape the whole ACL API shipped in until now.
        let acl = AclBinding {
            resource_type: acl::Resource::Topic,
            resource_name: "parity".into(),
            pattern: resource::Pattern::Literal,
            principal: "User:alice".into(),
            host: crate::WILDCARD_HOST.into(),
            operation: acl::Operation::Read,
            permission: acl::Permission::Allow,
        };

        let everything = AclFilter {
            resource_type: acl::Resource::Any,
            pattern: resource::Pattern::Any,
            operation: acl::Operation::Any,
            permission: acl::Permission::Any,
            ..Default::default()
        };

        _ = bare.create_acls(&[acl]).await?;

        assert_eq!(
            bare.describe_acls(&everything).await?,
            wrapped.describe_acls(&everything).await?,
            "describe_acls diverges through the wrapper",
        );

        // Deleting *through the wrapper* must remove what a describe through it
        // reported, and the bare engine must then agree that it is gone.
        let seen = wrapped.describe_acls(&everything).await?;

        assert_eq!(
            vec![seen],
            wrapped
                .delete_acls(std::slice::from_ref(&everything))
                .await?,
            "delete_acls diverges through the wrapper",
        );

        assert!(
            bare.describe_acls(&everything).await?.is_empty(),
            "a delete through the wrapper must reach the engine",
        );

        Ok(())
    }
}
