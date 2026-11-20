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

//! A Kafka API client for Kodansu, built from [`rama`] layers over a pool of
//! broker connections.
//!
//! # Calling a broker with a [`Request`]
//!
//! ```no_run
//! use tansu_client::{Client, ConnectionManager, Error};
//! use tansu_sans_io::MetadataRequest;
//! use url::Url;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let broker = Url::parse("tcp://localhost:9092")?;
//! let pool = ConnectionManager::builder(broker)
//!     .client_id(Some(String::from(env!("CARGO_PKG_NAME"))))
//!     .build()
//!     .await?;
//! let client = Client::new(pool);
//!
//! let everything = MetadataRequest::default()
//!     .topics(Some(Vec::new()))
//!     .allow_auto_topic_creation(Some(false))
//!     .include_cluster_authorized_operations(Some(false))
//!     .include_topic_authorized_operations(Some(false));
//! let _metadata = client.call(everything).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Forwarding [`Frame`]s: composing [`Layer`]s
//!
//! The server half of [`tansu_service`] stacked on this crate's client half
//! makes a proxy: every frame read on `tcp://localhost:9092` is written to the
//! broker at `tcp://example.com:9092` over a pooled connection, and its answer
//! written back.
//!
//! ```no_run
//! use rama::{Context, Layer as _, Service as _};
//! use tansu_client::{BytesConnectionService, ConnectionManager, Error};
//! use tansu_client::{FrameConnectionLayer, FramePoolLayer};
//! use tansu_service::{BytesFrameLayer, FrameBytesLayer, TcpBytesLayer};
//! use tansu_service::{TcpContextLayer, TcpListenerLayer, host_port};
//! use tokio::net::TcpListener;
//! use tokio_util::sync::CancellationToken;
//! use url::Url;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let broker = Url::parse("tcp://example.com:9092")?;
//! let pool = ConnectionManager::builder(broker)
//!     .client_id(Some(String::from(env!("CARGO_PKG_NAME"))))
//!     .build()
//!     .await?;
//!
//! let local = host_port(Url::parse("tcp://localhost:9092")?).await?;
//! let listener = TcpListener::bind(local).await?;
//!
//! let reading = (
//!     TcpListenerLayer::new(CancellationToken::new()),
//!     TcpContextLayer::default(),
//!     TcpBytesLayer::<()>::default(),
//!     BytesFrameLayer::default(),
//! );
//! let writing = (FramePoolLayer::new(pool), FrameConnectionLayer, FrameBytesLayer);
//! let proxy = (reading, writing).into_layer(BytesConnectionService);
//!
//! proxy.serve(Context::default(), listener).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, PoisonError};
use std::time::SystemTime;
use std::{error, fmt, io};

use backoff::ExponentialBackoffBuilder;
use backoff::future::retry;
use bytes::Bytes;
use deadpool::managed::{self, BuildError, Object, PoolError};
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::{InstrumentationScope, KeyValue, global};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rama::{Context, Layer, Service};
use tansu_sans_io::{ApiKey, ApiVersionsRequest, Body, Frame, Header, Request, RootMessageMeta};
use tansu_service::{FrameBytesLayer, FrameBytesService, host_port};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::task::JoinError;
use tokio::time::Duration;
use tracing::{Instrument as _, debug};
use tracing_subscriber::filter::ParseError;
use url::Url;

mod consumer;

pub use self::consumer::{ConsumerGroupLayer, ConsumerGroupService};

/// What can go wrong talking to a broker.
#[derive(thiserror::Error, Clone, Debug)]
pub enum Error {
    DeadPoolBuild(#[from] BuildError),
    Io(Arc<io::Error>),
    Join(Arc<JoinError>),
    Message(String),
    ParseFilter(Arc<ParseError>),
    ParseUrl(#[from] url::ParseError),
    Poison,
    Pool(Arc<Box<dyn error::Error + Send + Sync>>),
    Protocol(#[from] tansu_sans_io::Error),
    Service(#[from] tansu_service::Error),
    UnknownApiKey(i16),
    UnknownHost(Url),
}

impl tansu_service::Classify for Error {
    fn severity(&self) -> tansu_service::Severity {
        // The peer went away. Ordinary for a pooled connection.
        let hung_up = [
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::ConnectionReset,
        ];
        match self {
            Error::Io(failed) if hung_up.contains(&failed.kind()) => {
                tansu_service::Severity::Expected
            }
            Error::Service(inner) => inner.severity(),
            _ => tansu_service::Severity::Failure,
        }
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Error::Poison
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{self:?}"))
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        let shared = Arc::new(value);
        Error::Join(shared)
    }
}

impl<E> From<PoolError<E>> for Error
where
    E: error::Error + Send + Sync + 'static,
{
    fn from(value: PoolError<E>) -> Self {
        let boxed: Box<dyn error::Error + Send + Sync> = Box::new(value);
        Error::Pool(Arc::new(boxed))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        let shared = Arc::new(value);
        Error::Io(shared)
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        let shared = Arc::new(value);
        Error::ParseFilter(shared)
    }
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    let scope = InstrumentationScope::builder(env!("CARGO_PKG_NAME"));
    let scope = scope.with_version(env!("CARGO_PKG_VERSION"));
    global::meter_with_scope(scope.with_schema_url(SCHEMA_URL).build())
});

/// Milliseconds since `started`, `0` when the clock went backwards.
fn elapsed_ms(started: SystemTime) -> u64 {
    let took = started.elapsed();
    took.map_or(0, |took| took.as_millis() as u64)
}

/// A TCP stream to a broker, and the [correlation
/// id][`Header#variant.Request.field.correlation_id`] of the next request on it.
#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    correlation_id: i32,
    /// `true` while a request/response round-trip is in flight on `stream`.
    /// Set before the request is written and cleared only once the full
    /// response frame has been read. If the driving future is cancelled (e.g.
    /// a forward deadline elapsing) between those points, the connection is
    /// dropped back to the pool with unread response bytes still queued in the
    /// socket; reusing it would decode that leftover as a bogus frame
    /// (`MessageMaxSizeExceeded` / "maximum tagged fields exceeded"). `recycle`
    /// discards any connection still flagged dirty. See [`ConnectionManager`].
    dirty: bool,
}

/// Opens [`Connection`]s to one broker, and knows the newest version of each
/// API that both it and this crate speak.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionManager {
    broker: Url,
    client_id: Option<String>,
    versions: BTreeMap<i16, i16>,
}

impl ConnectionManager {
    /// A [`Builder`] of a pool of connections to `broker`.
    pub fn builder(broker: Url) -> Builder {
        let (client_id, max_size) = (None, None);
        Builder {
            broker,
            client_id,
            max_size,
        }
    }

    /// The client id every request to the broker carries.
    pub fn client_id(&self) -> Option<String> {
        let ConnectionManager { client_id, .. } = self;
        client_id.clone()
    }

    /// The version of `api_key` requests are sent at.
    pub fn api_version(&self, api_key: i16) -> Result<i16, Error> {
        let negotiated = self.versions.get(&api_key).copied();
        negotiated.ok_or(Error::UnknownApiKey(api_key))
    }
}

const INITIAL_CONNECTION_TIMEOUT_MILLIS: u64 = 30_000;

impl managed::Manager for ConnectionManager {
    type Type = Connection;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        tracing::debug!(%self.broker);
        let attributes = [KeyValue::new("broker", self.broker.to_string())];
        let started = SystemTime::now();
        let address = host_port(self.broker.clone()).await?;

        let patience = Duration::from_millis(INITIAL_CONNECTION_TIMEOUT_MILLIS);
        let mut schedule = ExponentialBackoffBuilder::new();
        let schedule = schedule.with_max_elapsed_time(Some(patience)).build();
        let connected = retry(schedule, || async {
            let attempt = TcpStream::connect(address).await;
            match &attempt {
                Ok(_) => TCP_CONNECT_DURATION.record(elapsed_ms(started), &attributes),
                Err(err) => {
                    let elapsed = elapsed_ms(started);
                    debug!(broker = %self.broker, err = ?err, elapsed = elapsed);
                    TCP_CONNECT_ERRORS.add(1, &attributes);
                }
            }
            let stream = attempt?;
            Ok(Connection {
                stream,
                correlation_id: 0,
                dirty: false,
            })
        });
        connected.await.map_err(Into::into)
    }

    async fn recycle(
        &self,
        obj: &mut Self::Type,
        metrics: &managed::Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        let (correlation_id, dirty) = (obj.correlation_id, obj.dirty);
        let recycle_count = metrics.recycle_count;
        debug!(
            obj.correlation_id = correlation_id,
            obj.dirty = dirty,
            metrics.recycle_count = recycle_count
        );

        // A connection whose last round-trip did not complete (cancelled or
        // errored mid-frame) has an unaligned byte stream: its socket may still
        // hold part of a previous response. Reusing it would misframe the next
        // read. Discard it so deadpool creates a fresh connection instead.
        if !dirty {
            return Ok(());
        }
        let why = "connection left mid-frame by a cancelled or failed round-trip";
        Err(managed::RecycleError::message(why))
    }
}

/// A managed [`Pool`] of broker [`Connection`]s
pub type Pool = managed::Pool<ConnectionManager>;

/// Publish the size, capacity and waiters of `pool` on the pool gauges.
fn status_update(pool: &Pool) {
    let status = pool.status();
    let gauges = [
        (&*POOL_AVAILABLE, status.available),
        (&*POOL_CURRENT_SIZE, status.size),
        (&*POOL_MAX_SIZE, status.max_size),
        (&*POOL_WAITING, status.waiting),
    ];
    for (gauge, value) in gauges {
        gauge.record(value as u64, &[]);
    }
}

/// A connection out of `pool`, timing the wait for it.
async fn checkout(pool: &Pool) -> Result<Object<ConnectionManager>, PoolError<Error>> {
    let started = SystemTime::now();
    let connection = pool.get().await?;
    POOL_GET_DURATION.record(elapsed_ms(started), &[]);
    Ok(connection)
}

/// [Builds][`Builder#method.build`] a [`Pool`] of [`Connection`]s to a
/// [broker][`Builder#method.broker`].
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder {
    broker: Url,
    client_id: Option<String>,
    max_size: Option<usize>,
}

impl Builder {
    /// A builder of connections to `broker`, with no client id and the
    /// default pool size.
    pub fn broker(broker: Url) -> Self {
        ConnectionManager::builder(broker)
    }

    /// The client id every request to the broker carries.
    pub fn client_id(self, client_id: Option<String>) -> Self {
        let mut builder = self;
        builder.client_id = client_id;
        builder
    }

    /// Maximum number of pooled connections. Defaults to deadpool's
    /// CPU-derived sizing when not set, which can be very small on
    /// CPU-constrained deployments.
    pub fn max_size(self, max_size: Option<usize>) -> Self {
        let mut builder = self;
        builder.max_size = max_size;
        builder
    }

    /// Ask the broker which API versions it supports, over a one-off pool that
    /// knows only `ApiVersions` v0, and keep the newest version of each key it
    /// and this crate share.
    async fn bootstrap(&self) -> Result<BTreeMap<i16, i16>, Error> {
        let manager = ConnectionManager {
            broker: self.broker.clone(),
            client_id: self.client_id.clone(),
            versions: BTreeMap::from([(ApiVersionsRequest::KEY, 0)]),
        };
        let client = Pool::builder(manager).build().map(Client::new)?;

        let ours = RootMessageMeta::messages().requests();
        let request = ApiVersionsRequest::default()
            .client_software_name(Some(String::from(env!("CARGO_PKG_NAME"))))
            .client_software_version(Some(String::from(env!("CARGO_PKG_VERSION"))));
        let answer = client.call(request).await?;

        let mut versions = BTreeMap::new();
        for theirs in answer.api_keys.unwrap_or_default() {
            let Some(meta) = ours.get(&theirs.api_key) else {
                continue;
            };
            let range = meta.version.valid;
            if theirs.min_version < range.start {
                continue;
            }
            _ = versions.insert(theirs.api_key, theirs.max_version.min(range.end));
        }
        Ok(versions)
    }

    /// A [`Pool`] of connections speaking the API versions the broker
    /// supports.
    pub async fn build(self) -> Result<Pool, Error> {
        let versions = self.bootstrap().await?;
        let Builder {
            broker,
            client_id,
            max_size,
        } = self;
        let manager = ConnectionManager {
            broker,
            client_id,
            versions,
        };

        let mut pool = Pool::builder(manager);
        if let Some(limit) = max_size {
            pool = pool.max_size(limit);
        }
        pool.build().map_err(Into::into)
    }
}

/// A [`Layer`] of [`FramePoolService`]: the [`Pool`] becomes the [`Context`]
/// state of the [`Service`] it wraps.
#[derive(Clone, Debug)]
pub struct FramePoolLayer {
    pool: Pool,
}

impl FramePoolLayer {
    pub fn new(pool: Pool) -> Self {
        FramePoolLayer { pool }
    }
}

impl<S> Layer<S> for FramePoolLayer {
    type Service = FramePoolService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        let pool = self.pool.clone();
        FramePoolService { pool, inner }
    }
}

/// Serves a [`Frame`] with the [`Pool`] as the [`Context`] state of the inner
/// [`Service`].
#[derive(Clone, Debug)]
pub struct FramePoolService<S> {
    pool: Pool,
    inner: S,
}

impl<State, S> Service<State, Frame> for FramePoolService<S>
where
    S: Service<Pool, Frame, Response = Frame>,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let (pooled, _) = ctx.swap_state(self.pool.clone());
        self.inner.serve(pooled, req).await
    }
}

/// A [`Layer`] of [`RequestPoolService`]: the [`Pool`] becomes the
/// [`Context`] state of the [`Service`] it wraps.
#[derive(Clone, Debug)]
pub struct RequestPoolLayer {
    pool: Pool,
}

impl RequestPoolLayer {
    pub fn new(pool: Pool) -> Self {
        RequestPoolLayer { pool }
    }
}

impl<S> Layer<S> for RequestPoolLayer {
    type Service = RequestPoolService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        let pool = self.pool.clone();
        RequestPoolService { pool, inner }
    }
}

/// Serves a [`Request`] with the [`Pool`] as the [`Context`] state of the
/// inner [`Service`].
#[derive(Clone, Debug)]
pub struct RequestPoolService<S> {
    pool: Pool,
    inner: S,
}

impl<State, S, Q> Service<State, Q> for RequestPoolService<S>
where
    Q: Request,
    S: Service<Pool, Q>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    /// `req`, served by the inner service with the pool as its state.
    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        let (pooled, _) = ctx.swap_state(self.pool.clone());
        self.inner.serve(pooled, req).await
    }
}

/// Sends [`Request`]s to a broker over its [`Pool`] of [`Connection`]s.
#[derive(Clone, Debug)]
pub struct Client {
    service:
        RequestPoolService<RequestConnectionService<FrameBytesService<BytesConnectionService>>>,
}

impl Client {
    /// A client sending its requests over `pool`.
    pub fn new(pool: Pool) -> Self {
        let layers = (RequestPoolLayer::new(pool), RequestConnectionLayer);
        let service = (layers, FrameBytesLayer).into_layer(BytesConnectionService);
        Client { service }
    }

    /// The broker's answer to `req`, sent over a pooled connection.
    pub async fn call<Q>(&self, req: Q) -> Result<Q::Response, Error>
    where
        Q: Request,
        Error: From<<<Q as Request>::Response as TryFrom<Body>>::Error>,
    {
        let ctx = Context::default();
        self.service.serve(ctx, req).await
    }
}

/// A [`Layer`] of [`FrameConnectionService`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameConnectionLayer;

impl<S> Layer<S> for FrameConnectionLayer {
    type Service = FrameConnectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FrameConnectionService { inner }
    }
}

/// Serves a [`Frame`] over a [`Connection`] taken from the [`Pool`]: the
/// frame is renumbered with the connection's correlation id, and the inner
/// [`Service`] gets the connection as its [`Context`] state.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameConnectionService<S> {
    inner: S,
}

impl<S> Service<Pool, Frame> for FrameConnectionService<S>
where
    S: Service<Object<ConnectionManager>, Frame, Response = Frame>,
    S::Error: From<Error> + From<PoolError<Error>> + From<tansu_sans_io::Error>,
{
    type Response = Frame;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<Pool>, req: Frame) -> Result<Self::Response, Self::Error> {
        debug!(req = ?req);
        let api_key = req.api_key()?;
        let api_version = req.api_version()?;
        let client_id = req.client_id()?.map(|id| id.to_string());

        let pool = ctx.state();
        status_update(pool);
        let connection = checkout(pool).await?;

        let header = Header::Request {
            api_key,
            api_version,
            correlation_id: connection.correlation_id,
            client_id,
        };
        let Frame { body, .. } = req;
        let renumbered = Frame {
            size: 0,
            header,
            body,
        };

        let (connected, _) = ctx.swap_state(connection);
        self.inner.serve(connected, renumbered).await
    }
}

/// A [`Layer`] of [`RequestConnectionService`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestConnectionLayer;

impl<S> Layer<S> for RequestConnectionLayer {
    type Service = RequestConnectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestConnectionService { inner }
    }
}

/// Serves a [`Request`] over a [`Connection`] taken from the [`Pool`]: the
/// request goes in a [`Frame`] at the newest version the broker supports, and
/// the inner [`Service`] gets the connection as its [`Context`] state.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestConnectionService<S> {
    inner: S,
}

impl<Q, S> Service<Pool, Q> for RequestConnectionService<S>
where
    Q: Request,
    S: Service<Object<ConnectionManager>, Frame, Response = Frame>,
    S::Error: From<Error>
        + From<PoolError<Error>>
        + From<tansu_sans_io::Error>
        + From<<Q::Response as TryFrom<Body>>::Error>,
{
    type Response = Q::Response;
    type Error = S::Error;

    async fn serve(&self, ctx: Context<Pool>, req: Q) -> Result<Self::Response, Self::Error> {
        debug!(req = ?req);
        let pool = ctx.state();
        status_update(pool);

        let manager = pool.manager();
        let api_version = manager.api_version(Q::KEY)?;
        let client_id = manager.client_id();
        let connection = checkout(pool).await?;

        let header = Header::Request {
            api_key: Q::KEY,
            api_version,
            correlation_id: connection.correlation_id,
            client_id,
        };
        let framed = Frame {
            size: 0,
            header,
            body: req.into(),
        };

        let (connected, _) = ctx.swap_state(connection);
        let answer = self.inner.serve(connected, framed).await?;
        let response = Q::Response::try_from(answer.body)?;
        debug!(response = ?response);
        Ok(response)
    }
}

/// Writes an encoded frame to the [`Connection`] in its [`Context`], and
/// answers with the encoded frame the broker sends back.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesConnectionService;

impl BytesConnectionService {
    async fn write(
        &self,
        stream: &mut TcpStream,
        frame: Bytes,
        attributes: &[KeyValue],
    ) -> Result<(), Error> {
        debug!(frame = ?frame.as_ref());
        let started = SystemTime::now();

        let written = stream.write_all(&frame).await;
        if written.is_ok() {
            TCP_SEND_DURATION.record(elapsed_ms(started), attributes);
            TCP_BYTES_SENT.add(frame.len() as u64, attributes);
        } else {
            TCP_SEND_ERRORS.add(1, attributes);
        }
        written.map_err(Into::into)
    }

    async fn read(&self, stream: &mut TcpStream, attributes: &[KeyValue]) -> Result<Bytes, Error> {
        let started = SystemTime::now();

        let mut prefix = [0u8; 4];
        _ = stream.read_exact(&mut prefix).await?;
        let mut frame = vec![0u8; frame_length(prefix)];
        let (size, body) = frame.split_at_mut(prefix.len());
        size.copy_from_slice(&prefix);

        let filled = stream.read_exact(body).await;
        if filled.is_ok() {
            TCP_RECEIVE_DURATION.record(elapsed_ms(started), attributes);
            TCP_BYTES_RECEIVED.add(frame.len() as u64, attributes);
        } else {
            TCP_RECEIVE_ERRORS.add(1, attributes);
        }
        _ = filled?;

        let frame = Bytes::from(frame);
        debug!(frame = ?frame.as_ref());
        Ok(frame)
    }
}

impl Service<Object<ConnectionManager>, Bytes> for BytesConnectionService {
    type Response = Bytes;
    type Error = Error;

    async fn serve(
        &self,
        mut ctx: Context<Object<ConnectionManager>>,
        req: Bytes,
    ) -> Result<Self::Response, Self::Error> {
        let connection = ctx.state_mut();
        let local = connection.stream.local_addr()?;
        let peer = connection.stream.peer_addr()?;
        let attributes = [KeyValue::new("peer", peer.to_string())];
        let span = tracing::debug_span!("client", local = %local, peer = %peer);

        let round_trip = async move {
            // Mark the connection in-flight: cleared only after the full
            // response is read below. If this future is cancelled (a forward
            // deadline elapsing) or the write/read errors in between, the flag
            // stays set and `ConnectionManager::recycle` discards the
            // connection rather than handing its unaligned stream to the next
            // caller.
            connection.dirty = true;
            self.write(&mut connection.stream, req, &attributes).await?;
            connection.correlation_id += 1;
            let response = self.read(&mut connection.stream, &attributes).await?;
            connection.dirty = false;
            Ok(response)
        };
        round_trip.instrument(span).await
    }
}

/// The length of a whole frame: the 4-byte size `encoded`, and what it counts.
fn frame_length(encoded: [u8; 4]) -> usize {
    let counted = i32::from_be_bytes(encoded) as usize;
    counted + encoded.len()
}

/// A histogram of milliseconds called `name`.
fn millis(name: &'static str, description: &'static str) -> Histogram<u64> {
    let histogram = METER.u64_histogram(name).with_unit("ms");
    histogram.with_description(description).build()
}

/// A counter called `name`.
fn counter(name: &'static str, description: &'static str) -> Counter<u64> {
    let counter = METER.u64_counter(name);
    counter.with_description(description).build()
}

/// A gauge called `name`.
fn gauge(name: &'static str, description: &'static str) -> Gauge<u64> {
    let gauge = METER.u64_gauge(name);
    gauge.with_description(description).build()
}

static TCP_CONNECT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    millis(
        "tcp_connect_duration",
        "The TCP connect latencies in milliseconds",
    )
});

static TCP_CONNECT_ERRORS: LazyLock<Counter<u64>> =
    LazyLock::new(|| counter("tcp_connect_errors", "TCP connect errors"));

static TCP_SEND_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    millis(
        "tcp_send_duration",
        "The TCP send latencies in milliseconds",
    )
});

static TCP_SEND_ERRORS: LazyLock<Counter<u64>> =
    LazyLock::new(|| counter("tcp_send_errors", "TCP send errors"));

static TCP_RECEIVE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    millis(
        "tcp_receive_duration",
        "The TCP receive latencies in milliseconds",
    )
});

static TCP_RECEIVE_ERRORS: LazyLock<Counter<u64>> =
    LazyLock::new(|| counter("tcp_receive_errors", "TCP receive errors"));

static TCP_BYTES_SENT: LazyLock<Counter<u64>> =
    LazyLock::new(|| counter("tcp_bytes_sent", "TCP bytes sent"));

static TCP_BYTES_RECEIVED: LazyLock<Counter<u64>> =
    LazyLock::new(|| counter("tcp_bytes_received", "TCP bytes received"));

static POOL_GET_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    millis(
        "pool_get_duration",
        "The Pool Get latencies in milliseconds",
    )
});

static POOL_MAX_SIZE: LazyLock<Gauge<u64>> =
    LazyLock::new(|| gauge("pool_max_size", "The maximum size of the pool"));

static POOL_CURRENT_SIZE: LazyLock<Gauge<u64>> =
    LazyLock::new(|| gauge("pool_current_size", "The current size of the pool"));

static POOL_AVAILABLE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    gauge(
        "pool_available",
        "The number of available objects in the pool",
    )
});

static POOL_WAITING: LazyLock<Gauge<u64>> =
    LazyLock::new(|| gauge("pool_waiting", "The number of waiting objects in the pool"));

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::thread;

    use tansu_sans_io::{MetadataRequest, MetadataResponse};
    use tansu_service::{BytesFrameLayer, FrameRouteService, RequestLayer, ResponseService};
    use tansu_service::{TcpBytesLayer, TcpContextLayer, TcpListenerLayer};
    use tokio::net::TcpListener;
    use tokio::task::JoinSet;
    use tokio_util::sync::CancellationToken;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;

    /// Debug logs from this crate, written to this test thread's own file
    /// under `logs/`.
    fn init_tracing() -> Result<DefaultGuard, Error> {
        let directive = format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?;
        let filter = EnvFilter::from_default_env().add_directive(directive);

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
            .with_env_filter(filter)
            .with_writer(log)
            .finish();
        Ok(tracing::subscriber::set_default(subscriber))
    }

    /// A broker on `listener` that answers every `Metadata` request with an
    /// empty cluster `abc`, controlled by node 111, until `cancellation`.
    async fn server(cancellation: CancellationToken, listener: TcpListener) -> Result<(), Error> {
        let metadata = |_ctx: Context<()>, _req: MetadataRequest| {
            let cluster = MetadataResponse::default()
                .brokers(Some(Vec::new()))
                .topics(Some(Vec::new()))
                .cluster_id(Some(String::from("abc")));
            let answer = cluster
                .controller_id(Some(111))
                .throttle_time_ms(Some(0))
                .cluster_authorized_operations(Some(-1));
            Ok::<_, Error>(answer)
        };
        let route =
            RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(metadata));
        let routes = FrameRouteService::builder().with_service(route)?.build()?;

        let layers = (
            TcpListenerLayer::new(cancellation),
            TcpContextLayer::default(),
            TcpBytesLayer::default(),
            BytesFrameLayer::default(),
        );
        layers
            .into_layer(routes)
            .serve(Context::default(), listener)
            .await
    }

    #[tokio::test]
    async fn a_request_round_trips_over_tcp() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let stop = CancellationToken::new();
        let mut tasks = JoinSet::new();
        let _broker = tasks.spawn(server(stop.clone(), listener));

        let broker = Url::parse(&format!("tcp://{address}"))?;
        debug!(url = %broker);
        let pool = ConnectionManager::builder(broker)
            .client_id(Some(String::from(env!("CARGO_PKG_NAME"))))
            .build()
            .await?;
        debug!(pool = ?pool);
        let layers = (RequestPoolLayer::new(pool), RequestConnectionLayer);
        let client = (layers, FrameBytesLayer).into_layer(BytesConnectionService);

        let everything = MetadataRequest::default()
            .topics(Some(Vec::new()))
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false));
        let metadata = client.serve(Context::default(), everything).await?;

        let cluster = (metadata.cluster_id.as_deref(), metadata.controller_id);
        assert_eq!((Some("abc"), Some(111)), cluster);

        stop.cancel();
        let finished = tasks.join_all().await;
        debug!(joined = ?finished);
        Ok(())
    }

    // A round-trip cancelled mid-read (as a forward deadline elapsing does)
    // leaves the pooled connection's byte stream unaligned. It must be
    // discarded on recycle, not handed to the next caller — otherwise the next
    // read decodes leftover bytes as a bogus frame. Proven here by the server
    // seeing a SECOND accept: the poisoned connection was dropped and a fresh
    // one opened.
    #[tokio::test]
    async fn dirty_connection_is_discarded_after_cancelled_round_trip() {
        use std::sync::atomic::{AtomicU32, Ordering};

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let accepts = Arc::new(AtomicU32::new(0));

        {
            let accepts = accepts.clone();
            _ = tokio::spawn(async move {
                let mut n = 0u32;
                loop {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    _ = accepts.fetch_add(1, Ordering::SeqCst);
                    n += 1;
                    let first = n == 1;

                    // Handle each connection in its own task so a stalled
                    // handler never blocks the accept loop from accepting the
                    // fresh connection the fix must open.
                    _ = tokio::spawn(async move {
                        let mut buf = [0u8; 64];
                        _ = sock.read(&mut buf).await;

                        if first {
                            // Announce a 100-byte body but send only two bytes
                            // and withhold the rest: the client blocks in
                            // read_exact until its caller cancels the round-trip.
                            _ = sock.write_all(&[0, 0, 0, 100]).await;
                            _ = sock.write_all(&[1, 2]).await;
                        } else {
                            // A well-formed, empty-body frame (4-byte size
                            // prefix of zero) that the client reads cleanly.
                            _ = sock.write_all(&[0, 0, 0, 0]).await;
                        }
                        _ = sock.flush().await;
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    });
                }
            });
        }

        let pool: Pool = Pool::builder(ConnectionManager {
            broker: Url::parse(&format!("tcp://127.0.0.1:{port}")).unwrap(),
            client_id: None,
            versions: BTreeMap::new(),
        })
        .max_size(1)
        .build()
        .unwrap();

        let service = BytesConnectionService;
        let request = Bytes::from_static(&[0, 0, 0, 4, 9, 9, 9, 9]);

        // First round-trip: cancelled mid-read (server withholds the body).
        {
            let object = pool.get().await.unwrap();
            let (ctx, _) = Context::default().swap_state(object);
            let outcome = tokio::time::timeout(
                Duration::from_millis(300),
                service.serve(ctx, request.clone()),
            )
            .await;
            assert!(
                outcome.is_err(),
                "the round-trip should have been cancelled mid-read"
            );
        }

        // Second round-trip: the poisoned connection must be discarded on
        // recycle, forcing a fresh TCP connection that completes cleanly.
        {
            let object = pool.get().await.unwrap();
            let (ctx, _) = Context::default().swap_state(object);
            let response =
                tokio::time::timeout(Duration::from_secs(2), service.serve(ctx, request))
                    .await
                    .expect("the retry must not hang on a poisoned connection")
                    .expect("the retry must succeed on a fresh connection");
            assert_eq!(&response[..], &[0, 0, 0, 0]);
        }

        assert_eq!(
            2,
            accepts.load(Ordering::SeqCst),
            "the poisoned connection must be discarded and a fresh one opened"
        );
    }

    // The counterpart: a connection whose round-trip completed cleanly must be
    // reused, not reconnected. Three sequential round-trips over a max-size-1
    // pool must ride ONE connection (a single accept).
    #[tokio::test]
    async fn clean_connection_is_reused() {
        use std::sync::atomic::{AtomicU32, Ordering};

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let accepts = Arc::new(AtomicU32::new(0));

        {
            let accepts = accepts.clone();
            _ = tokio::spawn(async move {
                loop {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    _ = accepts.fetch_add(1, Ordering::SeqCst);
                    _ = tokio::spawn(async move {
                        let mut buf = [0u8; 64];
                        loop {
                            match sock.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => {
                                    _ = sock.write_all(&[0, 0, 0, 0]).await;
                                    _ = sock.flush().await;
                                }
                            }
                        }
                    });
                }
            });
        }

        let pool: Pool = Pool::builder(ConnectionManager {
            broker: Url::parse(&format!("tcp://127.0.0.1:{port}")).unwrap(),
            client_id: None,
            versions: BTreeMap::new(),
        })
        .max_size(1)
        .build()
        .unwrap();

        let service = BytesConnectionService;

        for _ in 0..3 {
            let object = pool.get().await.unwrap();
            let (ctx, _) = Context::default().swap_state(object);
            let response = service
                .serve(ctx, Bytes::from_static(&[0, 0, 0, 4, 9, 9, 9, 9]))
                .await
                .expect("a clean round-trip must succeed");
            assert_eq!(&response[..], &[0, 0, 0, 0]);
        }

        assert_eq!(
            1,
            accepts.load(Ordering::SeqCst),
            "a cleanly-returned connection must be reused, not reconnected"
        );
    }
}
