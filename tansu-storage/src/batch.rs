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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram};
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::delete_groups_response::DeletableGroupResult;
use tansu_sans_io::delete_records_request::DeleteRecordsTopic;
use tansu_sans_io::delete_records_response::DeleteRecordsTopicResult;
use tansu_sans_io::describe_cluster_response::DescribeClusterBroker;
use tansu_sans_io::describe_configs_response::DescribeConfigsResult;
use tansu_sans_io::describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic;
use tansu_sans_io::incremental_alter_configs_request::AlterConfigsResource;
use tansu_sans_io::incremental_alter_configs_response::AlterConfigsResourceResponse;
use tansu_sans_io::list_groups_response::ListedGroup;
use tansu_sans_io::record::{Record, deflated, inflated};
use tansu_sans_io::txn_offset_commit_response::TxnOffsetCommitResponseTopic;
use tansu_sans_io::{ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism};
use tokio::time::sleep;
use tracing::{debug, instrument, warn};
use url::Url;
use uuid::Uuid;

use crate::delegate::storage_methods;
use crate::{
    Ack, AclBinding, AclFilter, AssignmentDoc, AssignmentOutcome, AutoTopicCreate,
    BrokerRegistrationRequest, CommittedOffset, ConsumerGroupDoc, ContactStamps, Error,
    GenerationDoc, ListOffsetResponse, METER, MemberDoc, MetadataResponse, NamedGroupDetail,
    OffsetCommitRequest, OffsetStage, ProducerIdResponse, QuotaAlteration, QuotaEntity,
    QuotaFilterComponent, QuotaLimits, Quotas, Result, ScramCredential, Storage, TopicId, Topition,
    TxnOffsetCommitRequest, UpdateError, Version,
};

/// A counter called `name`.
fn counter(name: &'static str, description: &'static str) -> Counter<u64> {
    let counter = METER.u64_counter(name);
    counter.with_description(description).build()
}

static BATCH_REQUESTS_LENGTH: LazyLock<Gauge<u64>> =
    LazyLock::new(|| METER.u64_gauge("batch_request_gauge").build());

static BATCH_RESPONSES_LENGTH: LazyLock<Gauge<u64>> =
    LazyLock::new(|| METER.u64_gauge("batch_response_gauge").build());

static BATCH_TICKET_POLL: LazyLock<Counter<u64>> =
    LazyLock::new(|| counter("batch_ticket_poll", "The number of ticket polls"));

static SEND_QUEUED_PRODUCED_RECORDS_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    let description = "The number of produced send queued records";
    counter("batch_send_queued_records", description)
});

static SEND_QUEUED_WAKE_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    counter(
        "batch_send_queued_wake_event",
        "The number of wake events sent",
    )
});

static PRODUCE_REQUEST_MINIMUM_SIZE_TRIGGER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    let description = "The number of times the minimum size was a trigger";
    counter("batch_produce_minimum_size_trigger", description)
});

static PRODUCE_REQUEST_YOUR_TICKET_IS_READY: LazyLock<Counter<u64>> = LazyLock::new(|| {
    let description = "The number of notifications that your ticket was ready while waiting";
    counter("batch_produce_your_ticket_is_ready", description)
});

static PRODUCE_REQUEST_TIMEOUT_EXPIRED_TRIGGER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    let description = "The number of times the timeout expiry was a trigger";
    counter("batch_produce_timeout_expired_trigger", description)
});

static PRODUCE_REQUEST_QUEUED_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    counter(
        "batch_produce_queued",
        "The number of produce requests queued",
    )
});

static PRODUCE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    let histogram = METER
        .u64_histogram("batch_produce_duration")
        .with_unit("ms");
    histogram
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
        let id = Uuid::now_v7();
        Ticket { id, batcher }
    }
}

impl<G> AsRef<Uuid> for Ticket<G> {
    fn as_ref(&self) -> &Uuid {
        let Ticket { id, .. } = self;
        id
    }
}

impl<G> Future for Ticket<G> {
    type Output = Result<i64, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let polled = |outcome: &'static str| {
            BATCH_TICKET_POLL.add(1, &[KeyValue::new("outcome", outcome)]);
        };
        let mut responses = self.batcher.responses.lock()?;
        if let Some(BatchResponse::Response(offset)) = responses.remove(&self.id) {
            polled("ready");
            return Poll::Ready(Ok(offset));
        }
        polled("pending");
        let waiting = BatchResponse::Waker(cx.waker().clone());
        _ = responses.insert(self.id, waiting);
        Poll::Pending
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
        let queued = self.requests.lock().map(|requests| {
            let length: u64 = requests.values().map(|queue| queue.len() as u64).sum();
            BATCH_REQUESTS_LENGTH.record(length, &[]);
        });
        let answered = self.responses.lock().map(|responses| {
            BATCH_RESPONSES_LENGTH.record(responses.len() as u64, &[]);
        });
        queued
            .map_err(Error::from)
            .and(answered.map_err(Error::from))
    }
}

impl<G> ProduceRequestBatcher<G>
where
    G: Storage,
{
    pub(crate) fn new(storage: G) -> Self {
        ProduceRequestBatcher {
            storage,
            maximum_delay: None,
            minimum_size: None,
            requests: Arc::default(),
            responses: Arc::default(),
        }
    }

    pub(crate) fn with_minimum_size(self, minimum_size: Option<usize>) -> Self {
        let mut batcher = self;
        batcher.minimum_size = minimum_size;
        batcher
    }

    pub(crate) fn with_maximum_delay(self, maximum_delay: Option<Duration>) -> Self {
        let mut batcher = self;
        batcher.maximum_delay = maximum_delay;
        batcher
    }

    #[instrument(skip(self, transaction_id, topition, producer_id))]
    async fn send_queued(
        &self,
        id: &Uuid,
        transaction_id: Option<&str>,
        topition: &Topition,
        producer_id: i64,
    ) -> Result<(), Error> {
        let key = TopitionProducerId {
            topition: topition.to_owned(),
            producer_id,
        };
        let removed = {
            let mut requests = self.requests.lock()?;
            let length: u64 = requests.values().map(|queue| queue.len() as u64).sum();
            BATCH_REQUESTS_LENGTH.record(length, &[]);
            requests.remove(&key)
        };
        let Some(queued) = removed else {
            BATCH_REQUESTS_LENGTH.record(0, &[]);
            return Ok(());
        };

        let owners: BTreeSet<Uuid> = queued.iter().map(|request| request.id).collect();
        debug!(owners = owners.len());
        let attributes = [KeyValue::new("topic", topition.topic.clone())];

        let batches = queued.into_iter().map(|request| request.batch).collect();
        let Some(combined) = combine(batches)? else {
            return Ok(());
        };
        let record_count = (combined.last_offset_delta + 1) as u64;
        let offset = self
            .storage
            .produce(transaction_id, topition, combined)
            .await?;
        debug!(offset = offset);
        SEND_QUEUED_PRODUCED_RECORDS_COUNTER.add(record_count, &attributes);

        let mut responses = self.responses.lock()?;
        for owner in owners {
            let previous = responses.insert(owner, BatchResponse::Response(offset));
            if let Some(BatchResponse::Waker(waker)) = previous {
                debug!(waking = %owner);
                SEND_QUEUED_WAKE_COUNTER.add(1, &attributes);
                waker.wake();
            }
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
        let key = TopitionProducerId {
            topition: topition.to_owned(),
            producer_id,
        };

        let ticket = {
            let mut requests = self.requests.lock()?;
            let ticket = Ticket::new(self.clone());
            let queue = requests.entry(key.clone()).or_default();
            queue.push(BatchRequest {
                id: ticket.id,
                batch,
            });
            PRODUCE_REQUEST_QUEUED_COUNTER.add(1, &attributes);
            debug!(queue_len = queue.len());
            ticket
        };
        debug!(ticket = %ticket.id);

        let mut round: i32 = 0;
        loop {
            self.update_metrics()?;
            let waiting = ticket.clone();
            let id = waiting.id;

            let queued_bytes: usize = {
                let requests = self.requests.lock()?;
                let queue = requests.get(&key);
                let sizes = queue
                    .into_iter()
                    .flatten()
                    .map(|request| request.batch.record_data.len());
                sizes.sum()
            };
            debug!(queued_bytes = queued_bytes);

            if let Some(minimum_size) = self.minimum_size {
                debug!(minimum_size = minimum_size, queued_bytes = queued_bytes);
                if queued_bytes > minimum_size {
                    PRODUCE_REQUEST_MINIMUM_SIZE_TRIGGER.add(1, &attributes);
                    self.send_queued(&id, transaction_id, topition, producer_id)
                        .await?;
                }
            }

            tokio::select! {
                response = waiting => {
                    let elapsed = start.elapsed().map_or(0, |took| took.as_millis() as u64);
                    debug!(ready = %id, elapsed = elapsed, iteration = round);
                    PRODUCE_REQUEST_YOUR_TICKET_IS_READY.add(1, &attributes);
                    PRODUCE_DURATION.record(elapsed, &attributes);
                    self.update_metrics()?;
                    return response;
                }

                () = sleep(maximum_delay) => {
                    if round > 1 {
                        warn!(ticket = %id, iteration = round);
                    }
                    PRODUCE_REQUEST_TIMEOUT_EXPIRED_TRIGGER.add(1, &attributes);
                    self.send_queued(&id, transaction_id, topition, producer_id).await?;
                }
            }
            round += 1;
        }
    }
}

/// Admission through the batcher (#588).
///
/// With no `maximum_delay` the batcher is a pass-through, and admission goes
/// straight to the engine underneath — which is every deployment that has not
/// asked for batching. With one, a batch's place in the write order is not
/// decided until its ticket is sent, so admitting is producing: the [`Ack`] is
/// already resolved, and a connection has nothing to pipeline behind it.
impl<G> ProduceRequestBatcher<G>
where
    G: Storage + Clone,
{
    async fn batched_admit(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<Ack> {
        if self.maximum_delay.is_none() {
            return self.storage.admit(transaction_id, topition, batch).await;
        }

        let produced = self.batched_produce(transaction_id, topition, batch).await;

        Ok(Box::pin(std::future::ready(produced)))
    }
}

/// One body per method: `produce` and `admit` batch, the other fifty-one
/// forward.
///
/// Invoked in expression position, which `#[async_trait]` can see through —
/// an item-position call inside the `impl` would still be unexpanded when
/// `async_trait` rewrites it (#551).
macro_rules! batcher_body {
    ($s:ident, produce, ($($arg:ident),*)) => {{
        $s.batched_produce($($arg),*).await
    }};

    ($s:ident, admit, ($($arg:ident),*)) => {{
        $s.batched_admit($($arg),*).await
    }};

    ($s:ident, $name:ident, ($($arg:ident),*)) => {{
        $s.storage.$name($($arg),*).await
    }};
}

/// `Storage` for the produce batcher, generated from
/// [`storage_methods`](crate::delegate::storage_methods).
///
/// Fifty-one of the fifty-three are `self.storage.m(args).await`, written out
/// by hand until #551.
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
    let mut batches = batches.into_iter();
    let Some(first) = batches.next() else {
        return Ok(None);
    };

    let mut sink = inflated::Batch::try_from(first)?;
    debug!(
        sink.base_offset = sink.base_offset,
        sink.last_offset_delta = sink.last_offset_delta,
        sink.base_sequence = sink.base_sequence,
        sink.max_timestamp = sink.max_timestamp
    );

    for next in batches {
        let batch = inflated::Batch::try_from(next)?;
        debug!(
            sink.last_offset_delta = sink.last_offset_delta,
            sink.max_timestamp = sink.max_timestamp,
            batch.base_offset = batch.base_offset,
            batch.last_offset_delta = batch.last_offset_delta,
            batch.base_sequence = batch.base_sequence
        );

        let offset_shift = sink.last_offset_delta + 1;
        let time_shift = sink.base_timestamp - batch.base_timestamp;
        let shifted = batch.records.into_iter().map(|record| Record {
            offset_delta: record.offset_delta + offset_shift,
            timestamp_delta: record.timestamp_delta + time_shift,
            ..record
        });
        sink.records.extend(shifted);

        sink.last_offset_delta += batch.last_offset_delta + 1;
        sink.max_timestamp = sink.max_timestamp.max(batch.max_timestamp);
    }

    debug!(
        sink.base_offset = sink.base_offset,
        sink.last_offset_delta = sink.last_offset_delta,
        sink.base_sequence = sink.base_sequence,
        sink.max_timestamp = sink.max_timestamp
    );
    Ok(Some(deflated::Batch::try_from(sink)?))
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use tansu_sans_io::BatchAttribute;
    use tansu_sans_io::record::{Record, deflated, inflated};
    use tokio::task::yield_now;
    use tokio::time::advance;

    use super::*;

    const A: Bytes = Bytes::from_static(b"a");
    const B: Bytes = Bytes::from_static(b"b");
    const C: Bytes = Bytes::from_static(b"c");
    const D: Bytes = Bytes::from_static(b"d");
    const E: Bytes = Bytes::from_static(b"e");

    #[derive(Clone, Debug, Default)]
    struct FlightRecorder {
        produced: Arc<Mutex<BTreeMap<Topition, Vec<deflated::Batch>>>>,
    }

    impl FlightRecorder {
        fn new() -> Self {
            let produced = Arc::default();
            FlightRecorder { produced }
        }

        fn produced(&self, topition: &Topition) -> Result<Option<Vec<inflated::Batch>>> {
            let held = self.produced.lock()?;
            let Some(batches) = held.get(topition) else {
                return Ok(None);
            };
            let mut inflated = Vec::with_capacity(batches.len());
            for batch in batches {
                inflated.push(inflated::Batch::try_from(batch)?);
            }
            Ok(Some(inflated))
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
        let values = [records.to_vec()];
        let mut split = into_batches(
            attributes,
            producer_id,
            producer_epoch,
            base_offset,
            &values,
        )?;
        Ok(split.remove(0))
    }

    /// Each of `groups` produced through a batcher with a one-second window,
    /// from its own task, before five seconds pass: the offset each was
    /// answered with, and the batches that reached storage.
    async fn produced_in_window(groups: &[&[Bytes]]) -> Result<(Vec<i64>, Vec<inflated::Batch>)> {
        let recorder = FlightRecorder::new();
        let window = Some(Duration::from_secs(1));
        let storage = ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(window);
        let abc0 = Topition::new("abc", 0);
        let attributes: i16 = BatchAttribute::default().into();

        let mut produces = Vec::new();
        for group in groups {
            let batch = into_batch(attributes, 54345, 32123, 0, group)?;
            let (storage, abc0) = (storage.clone(), abc0.clone());
            produces.push(tokio::spawn(async move {
                storage.produce(None, &abc0, batch).await
            }));
        }

        advance(Duration::from_secs(5)).await;
        yield_now().await;

        let mut offsets = Vec::new();
        for produce in produces {
            let offset = produce.await.expect("join_handle")?;
            debug!(produce_response = ?offset);
            offsets.push(offset);
        }
        let sent = recorder.produced(&abc0)?.unwrap_or_default();
        Ok((offsets, sent))
    }

    /// The values of `batch`'s records, in order.
    fn values(batch: &inflated::Batch) -> Vec<Option<Bytes>> {
        batch.records.iter().map(Record::value).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn single_produce_in_window() -> Result<()> {
        let (offsets, sent) = produced_in_window(&[&[A, B, C]]).await?;
        assert_eq!(vec![0], offsets);
        assert_eq!(1, sent.len());
        assert_eq!(vec![Some(A), Some(B), Some(C)], values(&sent[0]));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn two_produces_in_window() -> Result<()> {
        let (offsets, sent) = produced_in_window(&[&[A, B, C], &[D, E]]).await?;
        assert_eq!(vec![0, 0], offsets);
        assert_eq!(1, sent.len());
        let expected = vec![Some(A), Some(B), Some(C), Some(D), Some(E)];
        assert_eq!(expected, values(&sent[0]));
        Ok(())
    }

    #[test]
    fn combine_empty() -> Result<()> {
        assert_eq!(None, combine(Vec::new())?);
        Ok(())
    }

    fn into_batches(
        attributes: i16,
        producer_id: i64,
        producer_epoch: i16,
        base_offset: i64,
        batches: &[Vec<Bytes>],
    ) -> Result<Vec<deflated::Batch>> {
        let mut split = Vec::with_capacity(batches.len());
        let mut base_sequence = 0;
        for values in batches {
            let builder = inflated::Batch::builder()
                .attributes(attributes)
                .producer_id(producer_id)
                .producer_epoch(producer_epoch)
                .base_offset(base_offset)
                .last_offset_delta(values.len() as i32 - 1)
                .base_sequence(base_sequence);
            let records = values.iter().enumerate().map(|(offset_delta, value)| {
                let record = Record::builder().value(value.clone().into());
                record.offset_delta(offset_delta as i32)
            });
            let built = records
                .fold(builder, |builder, record| builder.record(record))
                .build()?;
            let deflated: deflated::Batch = built.try_into()?;
            debug!(deflated = ?deflated);
            split.push(deflated);
            base_sequence += values.len() as i32;
        }
        Ok(split)
    }

    #[test]
    fn combine_batches() -> Result<()> {
        let one = |value: &'static [u8]| Bytes::from_static(value);
        let batches = [
            vec![one(b"a"), one(b"b"), one(b"c")],
            vec![one(b"f"), one(b"g")],
            vec![one(b"i")],
            vec![one(b"j")],
            vec![one(b"k")],
            vec![one(b"p"), one(b"q"), one(b"r"), one(b"s")],
        ];
        let (producer_id, producer_epoch, base_offset) = (54345, 32123, 0);
        let attributes: i16 = BatchAttribute::default().into();

        let split = into_batches(
            attributes,
            producer_id,
            producer_epoch,
            base_offset,
            &batches,
        )?;
        let combined = inflated::Batch::try_from(combine(split)?.expect("a batch"))?;

        let header = (
            combined.producer_id,
            combined.producer_epoch,
            combined.base_sequence,
            combined.base_offset,
            combined.attributes,
        );
        assert_eq!(
            (producer_id, producer_epoch, 0, base_offset, attributes),
            header
        );

        for (index, value) in batches.iter().flatten().enumerate() {
            let record = &combined.records[index];
            assert_eq!(None, record.key, "{index}");
            assert_eq!(Some(value.clone()), record.value, "{index}");
            assert_eq!(index, record.offset_delta as usize);
        }
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
