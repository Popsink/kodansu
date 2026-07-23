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

use core::{
    fmt::{self, Debug, Display},
    result,
};
use std::{
    io,
    marker::PhantomData,
    num::{NonZero, NonZeroU32},
    ops::AddAssign,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex, PoisonError},
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use governor::{DefaultDirectRateLimiter, InsufficientCapacity, Jitter, Quota, RateLimiter};
use human_units::{
    FormatDuration,
    iec::{Byte, Prefix},
};
use nonzero_ext::nonzero;
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Meter},
};
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    metrics::{
        SdkMeterProvider, Temporality,
        data::{AggregatedMetrics, Histogram, Metric, MetricData, ResourceMetrics},
        exporter::PushMetricExporter,
    },
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use tansu_client::{Client, ConnectionManager};
use tansu_sans_io::{
    ByteSize as _, ErrorCode, FetchRequest, IsolationLevel, MetadataRequest, NULL_TOPIC_ID,
    ProduceRequest,
    fetch_request::{FetchPartition, FetchTopic},
    metadata_request::MetadataRequestTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use tokio::{
    signal::unix::{SignalKind, signal},
    task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument};
use url::Url;

pub type Result<T, E = Error> = result::Result<T, E>;

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    Client(#[from] tansu_client::Error),
    ExporterBuild(#[from] ExporterBuildError),
    InsufficientCapacity(#[from] InsufficientCapacity),
    Io(Arc<io::Error>),
    OtelSdk(#[from] OTelSdkError),
    Random(#[from] getrandom::Error),
    Poison,
    Protocol(#[from] tansu_sans_io::Error),
    UnknownHost(String),
    Url(#[from] url::ParseError),
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CancelKind {
    Interrupt,
    Terminate,
    Timeout,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Perf {
    broker: Url,
    topic: String,
    partition: i32,
    batch_size: u32,
    record_size: usize,
    per_second: Option<u32>,
    throughput: Option<u32>,
    producers: u32,
    duration: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct Builder<B, T> {
    broker: B,
    topic: T,
    partition: i32,
    batch_size: u32,
    record_size: usize,
    per_second: Option<u32>,
    throughput: Option<u32>,
    producers: u32,
    duration: Option<Duration>,
}

impl Default for Builder<PhantomData<Url>, PhantomData<String>> {
    fn default() -> Self {
        Self {
            broker: Default::default(),
            topic: Default::default(),
            partition: Default::default(),
            batch_size: 1,
            record_size: 1024,
            per_second: None,
            throughput: None,
            producers: 1,
            duration: None,
        }
    }
}

impl<B, T> Builder<B, T> {
    pub fn broker(self, broker: impl Into<Url>) -> Builder<Url, T> {
        Builder {
            broker: broker.into(),
            topic: self.topic,
            partition: self.partition,
            batch_size: self.batch_size,
            record_size: self.record_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
        }
    }

    pub fn topic(self, topic: impl Into<String>) -> Builder<B, String> {
        Builder {
            broker: self.broker,
            topic: topic.into(),
            partition: self.partition,
            batch_size: self.batch_size,
            record_size: self.record_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
        }
    }

    pub fn partition(self, partition: i32) -> Builder<B, T> {
        Self { partition, ..self }
    }

    pub fn batch_size(self, batch_size: u32) -> Self {
        Self { batch_size, ..self }
    }

    pub fn record_size(self, record_size: usize) -> Self {
        Self {
            record_size,
            ..self
        }
    }

    pub fn per_second(self, per_second: Option<u32>) -> Self {
        Self { per_second, ..self }
    }

    pub fn throughput(self, throughput: Option<u32>) -> Self {
        Self { throughput, ..self }
    }

    pub fn producers(self, producers: u32) -> Self {
        Self { producers, ..self }
    }

    pub fn duration(self, duration: Option<Duration>) -> Self {
        Self { duration, ..self }
    }
}

impl Builder<Url, String> {
    pub fn build(self) -> Perf {
        Perf {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            batch_size: self.batch_size,
            record_size: self.record_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
        }
    }
}

static RATE_LIMIT_DURATION: LazyLock<opentelemetry::metrics::Histogram<u64>> =
    LazyLock::new(|| {
        METER
            .u64_histogram("rate_limit_duration")
            .with_unit("ms")
            .with_description("Rate limit latencies in milliseconds")
            .build()
    });

static PRODUCE_RECORD_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("produce_record_count")
        .with_description("Produced record count")
        .build()
});

static PRODUCE_API_DURATION: LazyLock<opentelemetry::metrics::Histogram<u64>> =
    LazyLock::new(|| {
        METER
            .u64_histogram("produce_duration")
            .with_unit("ms")
            .with_description("Produce API latencies in milliseconds")
            .build()
    });

impl Perf {
    pub fn builder() -> Builder<PhantomData<Url>, PhantomData<String>> {
        Builder::default()
    }

    pub async fn main(self) -> Result<ErrorCode> {
        let token = CancellationToken::new();
        let meter_provider = install_meter_provider(token.clone());

        let rate_limiter = self
            .per_second
            .or(self.throughput)
            .inspect(|limit| debug!(?limit))
            .and_then(NonZeroU32::new)
            .map(Quota::per_second)
            .map(RateLimiter::direct)
            .map(Arc::new)
            .inspect(|rate_limiter| debug!(?rate_limiter));

        let record_data = {
            let mut data = vec![0u8; self.record_size];
            getrandom::fill(&mut data)?;
            Bytes::from(data)
        };

        let batch_size = NonZeroU32::new(self.batch_size)
            .inspect(|batch_size| debug!(batch_size = batch_size.get()))
            .unwrap_or(nonzero!(10u32));

        let mut set = JoinSet::new();

        let client = ConnectionManager::builder(self.broker)
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        for id in 0..self.producers {
            let producer = Producer {
                id,
                rate_limiter: rate_limiter.clone(),
                topic: self.topic.clone(),
                partition: self.partition,
                record_data: record_data.clone(),
                token: token.clone(),
                client: client.clone(),
                batch_size,
                throughput: self.throughput,
            };

            _ = set.spawn(async move {
                loop {
                    match producer.rate_limited().await {
                        Ok(false) | Err(_) => break,
                        _ => continue,
                    }
                }
            });
        }

        run_to_completion(token, set, self.duration, meter_provider).await
    }
}

/// Drive a fleet of already-spawned producer/consumer tasks to completion:
/// race the run duration (if any) against the task set finishing on its own
/// or an interrupt/terminate signal, then flush metrics and abort stragglers.
/// Shared by [`Perf::main`] and [`ConsumePerf::main`] so the process lifecycle
/// (signals, timeout grace period, final metrics flush) is identical for
/// produce and consume runs.
async fn run_to_completion(
    token: CancellationToken,
    mut set: JoinSet<()>,
    duration: Option<Duration>,
    meter_provider: SdkMeterProvider,
) -> Result<ErrorCode> {
    let mut interrupt_signal = signal(SignalKind::interrupt()).unwrap();
    debug!(?interrupt_signal);

    let mut terminate_signal = signal(SignalKind::terminate()).unwrap();
    debug!(?terminate_signal);

    let join_all = async {
        while !set.is_empty() {
            debug!(len = set.len());
            _ = set.join_next().await;
        }
    };

    let duration = duration
        .map(sleep)
        .map(Box::pin)
        .map(|pinned| pinned as Pin<Box<dyn Future<Output = ()>>>)
        .unwrap_or(Box::pin(std::future::pending()) as Pin<Box<dyn Future<Output = ()>>>);

    let cancellation = tokio::select! {

        timeout = duration => {
            debug!(?timeout);
            token.cancel();
            Some(CancelKind::Timeout)
        }

        completed = join_all => {
            debug!(?completed);
            None
        }

        interrupt = interrupt_signal.recv() => {
            debug!(?interrupt);
            Some(CancelKind::Interrupt)
        }

        terminate = terminate_signal.recv() => {
            debug!(?terminate);
            Some(CancelKind::Terminate)
        }

    };

    debug!(?cancellation);

    meter_provider
        .shutdown()
        .inspect(|shutdown| debug!(?shutdown))?;

    if let Some(CancelKind::Timeout) = cancellation {
        sleep(Duration::from_secs(5)).await;
    }

    debug!(abort = set.len());
    set.abort_all();

    while !set.is_empty() {
        _ = set.join_next().await;
    }

    Ok(ErrorCode::None)
}

/// Install the periodic metric exporter used for the live throughput/latency
/// summary line, tied to `token` so it prints a final line on cancellation.
fn install_meter_provider(token: CancellationToken) -> SdkMeterProvider {
    let exporter = MetricExporter::new(token);
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .build();
    global::set_meter_provider(meter_provider.clone());
    meter_provider
}

/// Continuously tail one partition with N independent fetchers, all starting
/// at the same offset — the fan-out shape the recent-write cache and
/// wake-on-write long-poll target (N consumer groups tailing one hot
/// partition). Each fetcher's long-poll `max_wait_ms` mirrors a real client's
/// poll loop, so an idle tail exercises the broker's wake-on-write path
/// (and, without it, would sit out the full `max_wait_ms` every empty poll).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsumePerf {
    broker: Url,
    topic: String,
    partition: i32,
    offset: i64,
    max_wait_ms: i32,
    max_bytes: i32,
    consumers: u32,
    duration: Option<Duration>,
}

impl ConsumePerf {
    /// `broker`/`topic` are mandatory (there's no meaningful default for
    /// either), so — unlike [`Perf`]'s two-phase [`Builder`] typestate, which
    /// exists to prevent building without them — they're plain constructor
    /// arguments: `ConsumePerf` has exactly one call site and it always
    /// supplies both up front, so the typestate ceremony bought nothing here.
    pub fn new(broker: impl Into<Url>, topic: impl Into<String>) -> Self {
        Self {
            broker: broker.into(),
            topic: topic.into(),
            partition: 0,
            offset: 0,
            max_wait_ms: 500,
            max_bytes: 1024 * 1024,
            consumers: 1,
            duration: None,
        }
    }

    pub fn partition(self, partition: i32) -> Self {
        Self { partition, ..self }
    }

    pub fn offset(self, offset: i64) -> Self {
        Self { offset, ..self }
    }

    pub fn max_wait_ms(self, max_wait_ms: i32) -> Self {
        Self {
            max_wait_ms,
            ..self
        }
    }

    pub fn max_bytes(self, max_bytes: i32) -> Self {
        Self { max_bytes, ..self }
    }

    pub fn consumers(self, consumers: u32) -> Self {
        Self { consumers, ..self }
    }

    pub fn duration(self, duration: Option<Duration>) -> Self {
        Self { duration, ..self }
    }
}

static CONSUME_RECORD_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("consume_record_count")
        .with_description("Consumed record count")
        .build()
});

static CONSUME_API_DURATION: LazyLock<opentelemetry::metrics::Histogram<u64>> =
    LazyLock::new(|| {
        METER
            .u64_histogram("consume_duration")
            .with_unit("ms")
            .with_description("Fetch API latencies in milliseconds")
            .build()
    });

impl ConsumePerf {
    pub async fn main(self) -> Result<ErrorCode> {
        let token = CancellationToken::new();
        let meter_provider = install_meter_provider(token.clone());

        let mut set = JoinSet::new();

        let client = ConnectionManager::builder(self.broker)
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        // At the negotiated (flexible) Fetch version a topic is identified by
        // its `topic_id` (KIP-516), not by name — `FetchTopic::topic` is only
        // consulted on old wire versions. Resolve it once via Metadata rather
        // than re-resolving per fetch call.
        let topic_id = client
            .call(
                MetadataRequest::default()
                    .allow_auto_topic_creation(Some(true))
                    .include_cluster_authorized_operations(Some(false))
                    .include_topic_authorized_operations(Some(false))
                    .topics(Some(
                        [MetadataRequestTopic::default()
                            .name(Some(self.topic.clone()))
                            .topic_id(Some(NULL_TOPIC_ID))]
                        .into(),
                    )),
            )
            .await?
            .topics
            .unwrap_or_default()
            .into_iter()
            .find(|topic| topic.name.as_deref() == Some(self.topic.as_str()))
            .and_then(|topic| topic.topic_id)
            .ok_or(Error::Api(ErrorCode::UnknownTopicOrPartition))?;

        for id in 0..self.consumers {
            let consumer = Consumer {
                id,
                topic_id,
                partition: self.partition,
                offset: self.offset,
                max_wait_ms: self.max_wait_ms,
                max_bytes: self.max_bytes,
                token: token.clone(),
                client: client.clone(),
            };

            _ = set.spawn(async move {
                _ = consumer.tail().await.inspect_err(|error| debug!(?error));
            });
        }

        run_to_completion(token, set, self.duration, meter_provider).await
    }
}

#[derive(Clone, Debug)]
struct Consumer {
    id: u32,
    topic_id: [u8; 16],
    partition: i32,
    offset: i64,
    max_wait_ms: i32,
    max_bytes: i32,
    token: CancellationToken,
    client: Client,
}

impl Consumer {
    #[instrument(skip_all, fields(offset))]
    async fn fetch(&self, offset: i64) -> Result<tansu_sans_io::FetchResponse> {
        let request = FetchRequest::default()
            .cluster_id(None)
            .replica_id(None)
            .replica_state(None)
            .max_wait_ms(self.max_wait_ms)
            .min_bytes(1)
            .max_bytes(Some(self.max_bytes))
            .isolation_level(Some(i8::from(IsolationLevel::ReadUncommitted)))
            .session_id(Some(-1))
            .session_epoch(Some(-1))
            .topics(Some(
                [FetchTopic::default()
                    .topic(None)
                    .topic_id(Some(self.topic_id))
                    .partitions(Some(
                        [FetchPartition::default()
                            .partition(self.partition)
                            .current_leader_epoch(Some(0))
                            .fetch_offset(offset)
                            .last_fetched_epoch(Some(-1))
                            .log_start_offset(Some(-1))
                            .partition_max_bytes(self.max_bytes)
                            .replica_directory_id(None)]
                        .into(),
                    ))]
                .into(),
            ))
            .forgotten_topics_data(Some([].into()))
            .rack_id(Some(String::new()));

        self.client.call(request).await.map_err(Into::into)
    }

    /// Long-poll `self.topic`/`self.partition` from `self.offset` onward until
    /// cancelled: each empty response means the broker held the request open
    /// for `max_wait_ms` waiting on new data (wake-on-write, if the broker has
    /// it, or the poll timing out, if it doesn't) — either way the loop just
    /// re-issues the fetch at the same offset.
    #[instrument(skip_all, fields(id = self.id))]
    async fn tail(&self) -> Result<()> {
        let attributes = [KeyValue::new("consumer", self.id.to_string())];
        let mut offset = self.offset;

        loop {
            if self.token.is_cancelled() {
                return Ok(());
            }

            let fetch_start = SystemTime::now();

            let response = tokio::select! {
                _ = self.token.cancelled() => return Ok(()),
                response = self.fetch(offset) => response?,
            };

            CONSUME_API_DURATION.record(
                fetch_start
                    .elapsed()
                    .inspect(|duration| debug!(fetch_duration_ms = duration.as_millis()))
                    .map_or(0, |duration| duration.as_millis() as u64),
                &attributes,
            );

            if let Some(code) = response.error_code
                && code != i16::from(ErrorCode::None)
            {
                return Err(ErrorCode::try_from(code)
                    .map(Error::Api)
                    .unwrap_or(Error::Api(ErrorCode::UnknownServerError)));
            }

            for topic_response in response.responses.into_iter().flatten() {
                for partition in topic_response.partitions.into_iter().flatten() {
                    if partition.error_code != i16::from(ErrorCode::None) {
                        return Err(ErrorCode::try_from(partition.error_code)
                            .map(Error::Api)
                            .unwrap_or(Error::Api(ErrorCode::UnknownServerError)));
                    }

                    for batch in partition
                        .records
                        .map(|frame| frame.batches)
                        .into_iter()
                        .flatten()
                    {
                        offset = offset.max(batch.base_offset + batch.last_offset_delta as i64 + 1);
                        CONSUME_RECORD_COUNT.add(batch.record_count as u64, &attributes);
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Producer {
    id: u32,
    rate_limiter: Option<Arc<DefaultDirectRateLimiter>>,
    topic: String,
    partition: i32,
    record_data: Bytes,
    token: CancellationToken,
    client: Client,
    batch_size: NonZero<u32>,
    throughput: Option<u32>,
}

impl Producer {
    #[instrument(skip_all, fields(record_data_len = self.record_data.len()))]
    fn frame(&self) -> Result<deflated::Frame> {
        let mut batch = inflated::Batch::builder();
        let offset_deltas = 0..(self.batch_size.get() as i32);

        for offset_delta in offset_deltas {
            batch = batch.record(
                Record::builder()
                    .value(Some(self.record_data.clone()))
                    .offset_delta(offset_delta),
            )
        }

        batch
            .last_offset_delta(self.batch_size.get() as i32)
            .build()
            .map(|batch| inflated::Frame {
                batches: vec![batch],
            })
            .and_then(deflated::Frame::try_from)
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn produce(&self, frame: deflated::Frame) -> Result<()> {
        let req = ProduceRequest::default().topic_data(Some(
            [TopicProduceData::default()
                .name(self.topic.clone())
                .partition_data(Some(
                    [PartitionProduceData::default()
                        .index(self.partition)
                        .records(Some(frame))]
                    .into(),
                ))]
            .into(),
        ));

        let response = self.client.call(req).await?;

        assert!(
            response
                .responses
                .unwrap_or_default()
                .into_iter()
                .all(|topic| {
                    topic
                        .partition_responses
                        .unwrap_or_default()
                        .iter()
                        .all(|partition| partition.error_code == i16::from(ErrorCode::None))
                })
        );

        Ok(())
    }

    #[instrument(skip_all, fields(id = self.id))]
    async fn rate_limited(&self) -> Result<bool> {
        let attributes = [KeyValue::new("producer", self.id.to_string())];

        let frame = self.frame()?;

        if let Some(ref rate_limiter) = self.rate_limiter {
            let rate_limit_start = SystemTime::now();

            let cells = self
                .throughput
                .and(
                    frame
                        .size_in_bytes()
                        .ok()
                        .and_then(|bytes| NonZeroU32::new(bytes as u32)),
                )
                .unwrap_or(self.batch_size);

            tokio::select! {
                cancelled = self.token.cancelled() => {
                    debug!(?cancelled);
                    return Ok(false)
                },

                Ok(_) = rate_limiter.until_n_ready_with_jitter(cells, Jitter::up_to(Duration::from_millis(10))) => {
                    RATE_LIMIT_DURATION.record(
                    rate_limit_start
                        .elapsed()
                        .inspect(|duration|debug!(rate_limit_duration_ms = duration.as_millis()))
                        .map_or(0, |duration| duration.as_millis() as u64),
                        &attributes)

                },
            }
        }

        let produce_start = SystemTime::now();

        tokio::select! {
            cancelled = self.token.cancelled() => {
                debug!(?cancelled);
                return Ok(false)
            },

            Ok(_) = self.produce(frame) => {
                PRODUCE_RECORD_COUNT.add(self.batch_size.get() as u64, &attributes);
                PRODUCE_API_DURATION.record(produce_start.elapsed().inspect(|duration|debug!(produce_duration_ms = duration.as_millis())).map_or(0, |duration| duration.as_millis() as u64), &attributes);
            },
        }

        Ok(!self.token.is_cancelled())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Observation {
    taken_at: SystemTime,
    bytes_sent: u64,
    record_count: u64,
}

impl AddAssign for Observation {
    fn add_assign(&mut self, rhs: Self) {
        self.taken_at = self.taken_at.max(rhs.taken_at);
        self.bytes_sent += rhs.bytes_sent;
        self.record_count += rhs.record_count;
    }
}

impl Default for Observation {
    fn default() -> Self {
        Self {
            taken_at: SystemTime::now(),
            bytes_sent: Default::default(),
            record_count: Default::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
struct Info {
    started_at: SystemTime,
    previous: Option<ObservationLatency>,
    current: ObservationLatency,
}

impl Info {
    fn new(started_at: SystemTime) -> Self {
        Self {
            started_at,
            current: Default::default(),
            previous: Default::default(),
        }
    }

    fn with_previous(self, previous: Option<ObservationLatency>) -> Self {
        Self { previous, ..self }
    }

    fn elapsed(&self) -> Duration {
        self.current
            .observation
            .taken_at
            .duration_since(
                self.previous
                    .map_or(self.started_at, |previous| previous.observation.taken_at),
            )
            .expect("duration")
    }

    fn bytes_sent(&self) -> u64 {
        self.current.observation.bytes_sent
            - self
                .previous
                .map(|previous| previous.observation.bytes_sent)
                .unwrap_or_default()
    }

    fn records_sent(&self) -> u64 {
        self.current.observation.record_count
            - self
                .previous
                .map(|previous| previous.observation.record_count)
                .unwrap_or_default()
    }

    /// Seconds elapsed since the previous observation, as a float: a metric
    /// export interval under 1s (a perfectly reasonable
    /// `OTEL_METRIC_EXPORT_INTERVAL` for a live-tailing perf run) previously
    /// truncated to whole seconds via `Duration::as_secs()`, which not only
    /// under/over-stated every rate by however far the tick landed from a
    /// second boundary, but made `bandwidth()`'s `checked_div` divide by zero
    /// and panic outright on any sub-second interval.
    fn elapsed_secs(&self) -> f64 {
        self.elapsed().as_secs_f64()
    }

    fn records_sent_per_second(&self) -> f64 {
        let elapsed = self.elapsed_secs();
        if elapsed > 0.0 {
            self.records_sent() as f64 / elapsed
        } else {
            0.0
        }
    }

    fn bandwidth(&self) -> Byte {
        let elapsed = self.elapsed_secs();
        let throughput = if elapsed > 0.0 {
            (self.bytes_sent() as f64 / elapsed) as u64
        } else {
            0
        };
        Byte::with_iec_prefix(throughput, Prefix::None)
    }
}

impl Display for Info {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A tick can legitimately have zero completed calls to report a
        // latency for (a burst of concurrent produces/fetches still in
        // flight when the periodic export fires, or a quiet idle period) —
        // that must render as "n/a", not panic the exporter thread and take
        // the whole run down with it (observed live against a 20ms-RTT
        // proxy: the very first export tick fired before any of 8
        // concurrently-launched consumers had completed a fetch).
        let min = self.current.latency.min.map_or_else(
            || "n/a".to_string(),
            |min| min.format_duration().to_string(),
        );

        let mean = self
            .current
            .latency
            .mean
            .map_or_else(|| "n/a".to_string(), |mean| format!("{mean:.1}ms"));

        let max = self.current.latency.max.map_or_else(
            || "n/a".to_string(),
            |max| max.format_duration().to_string(),
        );

        write!(
            f,
            "elapsed: {}, {} records sent, {:.1} records/s, ({}/s), latency: {min} min, {mean} avg, {max} max",
            self.elapsed().format_duration(),
            self.records_sent(),
            self.records_sent_per_second(),
            self.bandwidth().format_iec(),
        )
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
struct Latency {
    min: Option<Duration>,
    max: Option<Duration>,
    mean: Option<f64>,
}

impl From<&Histogram<u64>> for Latency {
    fn from(histogram: &Histogram<u64>) -> Self {
        let min = histogram
            .data_points()
            .filter_map(|dp| dp.min())
            .min()
            .map(Duration::from_millis);

        let max = histogram
            .data_points()
            .filter_map(|dp| dp.max())
            .max()
            .map(Duration::from_millis);

        let sum = histogram.data_points().map(|dp| dp.sum()).sum::<u64>() as f64;
        let count = histogram.data_points().map(|dp| dp.count()).sum::<u64>() as f64;

        // A tick with no completed calls yet (a burst of concurrent
        // fetches/produces still in flight when the first export fires, or
        // a quiet period between them) leaves the histogram with zero data
        // points: `min`/`max` above already read as `None` in that case via
        // `Iterator::min`/`max` on an empty sequence, so `mean` must match
        // rather than divide 0.0/0.0 into a silent `Some(NaN)`.
        let mean = (count > 0.0).then(|| sum / count);

        Self { min, max, mean }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
struct ObservationLatency {
    observation: Observation,
    latency: Latency,
}

#[derive(Debug)]
struct MetricExporter {
    started_at: SystemTime,
    temporality: Temporality,
    previous: Mutex<Option<ObservationLatency>>,
    cancellation: CancellationToken,
}

impl MetricExporter {
    fn new(cancellation: CancellationToken) -> Self {
        let started_at = SystemTime::now();
        Self {
            started_at,
            temporality: Default::default(),
            previous: Default::default(),
            cancellation,
        }
    }

    #[instrument(skip_all, fields(scope = scope.name(), metric = metric.name()))]
    fn info(&self, scope: &InstrumentationScope, metric: &Metric, info: &mut Info) {
        match (scope.name(), metric.name(), metric.data()) {
            // Wire bytes in either direction: produce mode is dominated by
            // `tcp_bytes_sent` (record payloads out), consume mode by
            // `tcp_bytes_received` (record payloads back). Both counters are
            // always present (every request has a response), so this sums
            // rather than overwrites — an assignment would have one
            // direction's small ack/request bytes nondeterministically
            // clobber the other's dominant value depending on metric
            // iteration order.
            (
                "tansu-client",
                "tcp_bytes_sent" | "tcp_bytes_received",
                AggregatedMetrics::U64(MetricData::Sum(sum)),
            ) => {
                for (point, data) in sum.data_points().enumerate() {
                    debug!(point, value = ?data.value());
                }

                info.current.observation.bytes_sent +=
                    sum.data_points().map(|sum| sum.value()).sum::<u64>();
            }

            (
                "tansu-perf",
                "produce_record_count" | "consume_record_count",
                AggregatedMetrics::U64(MetricData::Sum(sum)),
            ) => {
                for (point, data) in sum.data_points().enumerate() {
                    debug!(point, value = ?data.value());
                }

                info.current.observation.record_count =
                    sum.data_points().map(|sum| sum.value()).sum::<u64>();
            }

            (
                "tansu-perf",
                "produce_duration" | "consume_duration",
                AggregatedMetrics::U64(MetricData::Histogram(histogram)),
            ) => {
                info.current.latency = Latency::from(histogram);
            }

            _ => (),
        }
    }
}

impl PushMetricExporter for MetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let cancelled = self.cancellation.is_cancelled();

        if cancelled {
            if let Some(previous) = *self.previous.lock().expect("previous") {
                let mut info = Info::new(self.started_at);
                info.current = previous;

                println!("{}", info);
            }
        } else {
            let mut previous = self.previous.lock().expect("previous");

            let mut info = Info::new(self.started_at).with_previous(previous.take());

            for scope in metrics.scope_metrics() {
                debug!(scope = scope.scope().name());

                for metric in scope.metrics() {
                    debug!(scope = scope.scope().name(), metric = metric.name());

                    self.info(scope.scope(), metric, &mut info);
                }
            }

            println!("{info}");

            _ = previous.replace(info.current);
        }

        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    #[instrument]
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        self.temporality
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_assign_observation() {
        let now = SystemTime::now();
        let delta = Duration::from_secs(5);

        let previous = Observation {
            taken_at: now.checked_sub(delta).expect("previous"),
            bytes_sent: 32_123,
            record_count: 12_321,
        };

        let mut current = Observation {
            taken_at: now,
            bytes_sent: 43_234,
            record_count: 54_345,
        };

        current += previous;

        assert_eq!(now, current.taken_at);
        assert_eq!(75_357, current.bytes_sent);
        assert_eq!(66_666, current.record_count);
    }

    #[test]
    fn middle_observation() {
        let now = SystemTime::now();
        let elapsed = Duration::from_secs(4);

        let previous = {
            let observation = Observation {
                taken_at: now.checked_sub(elapsed).expect("previous"),
                bytes_sent: 43_234,
                record_count: 212,
            };

            ObservationLatency {
                observation,
                latency: Default::default(),
            }
        };

        let mut info = Info::new(now).with_previous(Some(previous));

        info.current = {
            let observation = Observation {
                taken_at: now,
                bytes_sent: 65_456,
                record_count: 656,
            };

            ObservationLatency {
                observation,
                latency: Default::default(),
            }
        };

        assert_eq!(elapsed, info.elapsed());
        assert_eq!(5_555, info.bandwidth().0);
        assert_eq!(111f64, info.records_sent_per_second());
    }

    #[test]
    fn last_or_first_observation() {
        let now = SystemTime::now();
        let elapsed = Duration::from_secs(4);

        let mut info = Info::new(now.checked_sub(elapsed).expect("elapsed"));

        info.current = {
            let observation = Observation {
                taken_at: now,
                bytes_sent: 65_456,
                record_count: 656,
            };

            ObservationLatency {
                observation,
                latency: Default::default(),
            }
        };

        assert_eq!(elapsed, info.elapsed());
        assert_eq!(16_364, info.bandwidth().0);
        assert_eq!(164f64, info.records_sent_per_second());
    }

    /// A sub-second gap between observations — an entirely reasonable
    /// `OTEL_METRIC_EXPORT_INTERVAL` for a live-tailing perf run — used to
    /// integer-truncate `elapsed().as_secs()` to zero, which not only
    /// distorted the reported rate but made `bandwidth()`'s `checked_div(0)`
    /// panic outright (observed live: a 250ms export interval crashed the
    /// exporter thread and killed the run mid-benchmark).
    #[test]
    fn sub_second_elapsed_neither_panics_nor_divides_by_zero() {
        let now = SystemTime::now();
        let elapsed = Duration::from_millis(250);

        let mut info = Info::new(now.checked_sub(elapsed).expect("elapsed"));

        info.current = {
            let observation = Observation {
                taken_at: now,
                bytes_sent: 10_000,
                record_count: 100,
            };

            ObservationLatency {
                observation,
                latency: Default::default(),
            }
        };

        assert_eq!(elapsed, info.elapsed());
        // 10_000 bytes / 0.25s = 40_000 bytes/s, 100 records / 0.25s = 400/s —
        // both exact, and computing them must not panic.
        assert_eq!(40_000, info.bandwidth().0);
        assert_eq!(400f64, info.records_sent_per_second());
    }

    /// Two observations landing on the identical instant (a spurious
    /// duplicate tick, or a clock with coarse resolution) must report zero
    /// rather than dividing by zero.
    #[test]
    fn zero_elapsed_reports_zero_not_a_panic() {
        let now = SystemTime::now();
        let mut info = Info::new(now);

        info.current = {
            let observation = Observation {
                taken_at: now,
                bytes_sent: 5_000,
                record_count: 50,
            };

            ObservationLatency {
                observation,
                latency: Default::default(),
            }
        };

        assert_eq!(Duration::ZERO, info.elapsed());
        assert_eq!(0, info.bandwidth().0);
        assert_eq!(0.0, info.records_sent_per_second());
    }

    /// A tick with zero completed calls (a burst of concurrent
    /// produces/fetches still in flight when the periodic export fires, or
    /// a quiet idle window) leaves `Latency` all-`None` — that must render
    /// as "n/a", not panic the exporter thread and take the whole run down
    /// with it (observed live: the first export tick against a 20ms-RTT
    /// proxy fired before any of 8 concurrently-launched consumers had
    /// completed a fetch).
    #[test]
    fn display_with_no_latency_samples_does_not_panic() {
        let now = SystemTime::now();
        let elapsed = Duration::from_millis(500);
        let mut info = Info::new(now.checked_sub(elapsed).expect("elapsed"));

        info.current = ObservationLatency {
            observation: Observation {
                taken_at: now,
                bytes_sent: 0,
                record_count: 0,
            },
            latency: Latency::default(),
        };

        let rendered = format!("{info}");
        assert!(rendered.contains("n/a min"), "{rendered}");
        assert!(rendered.contains("n/a avg"), "{rendered}");
        assert!(rendered.contains("n/a max"), "{rendered}");
    }
}
