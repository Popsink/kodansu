// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
// Copyright ⓒ 2026 Popsink SAS
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

//! The [`rama`] layers and services every other crate composes into a Kafka
//! protocol stack.
//!
//! What moves through them is a [`Frame`], its [`Body`], or a typed
//! [request][`tansu_sans_io::Request`] and its
//! [response][`tansu_sans_io::Response`]. Three transports carry them:
//!
//! - TCP, served by [`TcpBytesLayer`] and initiated by [`BytesTcpService`];
//! - a [channel][`tokio::sync::mpsc`], with [`ChannelFrameLayer`] and
//!   [`ChannelFrameService`];
//! - [`Bytes`][`bytes::Bytes`] in process, through [`BytesLayer`], which is
//!   what protocol tests stack on.
//!
//! [`FrameRouteService`] hands each frame to the service registered for its
//! [API key][`Frame#method.api_key`], and answers
//! [`ApiVersionsRequest`][`tansu_sans_io::ApiVersionsRequest`] itself from the
//! version ranges of everything it routes.
//!
//! The example routes [`MetadataRequest`][`tansu_sans_io::MetadataRequest`] to
//! one handler, then wraps the route in a client half ([`RequestFrameLayer`],
//! [`FrameBytesLayer`]) and a server half ([`BytesFrameLayer`]) joined by
//! [`BytesLayer`]. A request served on the result is encoded, carried, decoded,
//! routed, answered and decoded again, so the
//! [`MetadataResponse`][`tansu_sans_io::MetadataResponse`] that comes back is
//! the handler's, and the
//! [`ApiVersionsResponse`][`tansu_sans_io::ApiVersionsResponse`] lists exactly
//! the two keys routed. A broker or a CLI client stacks the same way, with
//! [`TcpBytesLayer`] or [`BytesTcpService`] where [`BytesLayer`] is.
//!
//! ```
//! use rama::{Context, Layer as _, Service as _};
//! use tansu_sans_io::{ApiKey as _, ApiVersionsRequest};
//! use tansu_sans_io::{MetadataRequest, MetadataResponse};
//! use tansu_service::{BytesFrameLayer, BytesLayer, Error, FrameBytesLayer};
//! use tansu_service::{FrameRouteService, RequestFrameLayer, RequestLayer, ResponseService};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), tansu_service::Error> {
//! let metadata = ResponseService::new(|_, _| {
//!     let empty = MetadataResponse::default().brokers(Some(vec![])).topics(Some(vec![]));
//!     let named = empty.cluster_id(Some("tansu".into())).controller_id(Some(111));
//!     Ok(named.throttle_time_ms(Some(0)).cluster_authorized_operations(Some(-1)))
//! });
//! let routed = RequestLayer::<MetadataRequest>::new().into_layer(metadata);
//! let routes = FrameRouteService::<(), Error>::builder().with_service(routed)?;
//!
//! let halves = (RequestFrameLayer, FrameBytesLayer, BytesLayer, BytesFrameLayer::default());
//! let stack = halves.into_layer(routes.build()?);
//!
//! let asked = MetadataRequest::default().topics(Some(vec![]));
//! let asked = asked.allow_auto_topic_creation(Some(false));
//! let asked = asked.include_cluster_authorized_operations(Some(false));
//! let asked = asked.include_topic_authorized_operations(Some(false));
//! let answered = stack.serve(Context::default(), asked).await?;
//! assert_eq!(Some(String::from("tansu")), answered.cluster_id);
//!
//! let hello = ApiVersionsRequest::default().client_software_name(Some("abcba".into()));
//! let hello = hello.client_software_version(Some("1.2321".into()));
//! let versions = stack.serve(Context::default(), hello).await?;
//! let mut keys: Vec<i16> = versions.api_keys.iter().flatten().map(|key| key.api_key).collect();
//! keys.sort_unstable();
//! assert_eq!(vec![MetadataRequest::KEY, ApiVersionsRequest::KEY], keys);
//! # Ok::<(), tansu_service::Error>(())
//! # }
//! ```

use std::net::SocketAddr;
use std::ops::Range;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, SystemTime};
use std::{fmt, io};

use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};
use opentelemetry::{InstrumentationScope, KeyValue, global};
use opentelemetry_semantic_conventions as semconv;
use rama::Context;
use rama::{Layer, Service};
use rand::prelude::*;
use rand::rngs::SmallRng;
use tansu_sans_io::Body;
use tansu_sans_io::Frame;
use tokio::net::lookup_host;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio::time::sleep;
use tracing::debug;
use url::Url;

mod api;
mod channel;
mod consumer;
mod frame;
mod stream;

pub use self::api::ApiVersionsService;
pub use self::api::{FrameRouteBuilder, FrameRouteService};
pub use self::channel::{ChannelFrameLayer, ChannelFrameService, FrameChannelService};
pub use self::channel::{FrameReceiver, FrameSender, bounded_channel};
pub use self::consumer::{ConsumerGroupLayer, ConsumerGroupService};
pub use self::frame::{BodyRequestLayer, FrameBodyLayer, FrameRequestLayer, FrameService};
pub use self::frame::{BytesFrameLayer, BytesFrameService, FrameBytesLayer, FrameBytesService};
pub use self::frame::{FrameApiKeyMatcher, RequestApiKeyMatcher};
pub use self::frame::{RequestFrameLayer, RequestFrameService, RequestLayer, ResponseService};

/// A request parked in a long poll, counted for as long as this value lives
/// (#362).
///
/// RAII rather than a pair of calls because every parking site is a `select!`
/// or a `?` away from an early return, and a leaked increment does not decay:
/// it is an up-down counter, so one missed decrement is a permanent lie about
/// how loaded this replica is, and a scaler built on it never scales down
/// again.
///
/// Public because the group long polls live in the broker, not here.
#[derive(Debug)]
pub struct Parked;

impl Parked {
    /// Count this request as waiting until the returned value is dropped.
    #[must_use]
    pub fn enter() -> Self {
        REQUESTS_PARKED.add(1, &[]);
        Self
    }
}

impl Drop for Parked {
    fn drop(&mut self) {
        REQUESTS_PARKED.add(-1, &[]);
    }
}

pub use self::stream::{
    Admission, BytesLayer, BytesService, BytesTcpService, Keepalive, NoResponse, Peer,
    TcpBytesLayer, TcpBytesService, TcpContext, TcpContextLayer, TcpContextService,
    TcpListenerLayer, Throttle,
};

/// Requests answered with a non-zero `throttle_time_ms` (#384).
pub(crate) static THROTTLED_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    counter(
        "tansu_throttled_requests",
        "The number of API requests answered with a quota throttle",
    )
});

/// How long connections have spent muted between requests (#384).
///
/// Deliberately not a share of [`REQUEST_DURATION`], and deliberately not
/// counted in [`REQUESTS_IN_FLIGHT`]: the wait happens after a response is
/// written and before the next request is read, so a fleet throttling hard
/// reports `busy` at or near zero rather than looking saturated to the scaler
/// that reads it (#362). This counter is the only place the time shows up,
/// which is what makes "the fleet is deliberately refusing traffic" a thing an
/// operator can see rather than infer.
pub(crate) static THROTTLED_TIME: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_throttled_time")
        .with_unit("ms")
        .with_description("Time connections have spent muted by a quota, between requests")
        .build()
});

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    Auth(#[from] tansu_auth::Error),
    DuplicateRoute(i16),

    /// A frame prefix that is not a length: negative, so it announces nothing
    /// that can be read (see [`frame_length`]).
    FrameLength(i32),

    FrameTooBig(usize),
    Io(Arc<io::Error>),
    Join(Arc<JoinError>),
    Message(String),
    OneshotRecv(oneshot::error::RecvError),
    Poison,
    Parse(#[from] url::ParseError),
    Protocol(#[from] tansu_sans_io::Error),
    UnableToSend(Box<Frame>),
    UnknownHost(Url),
    UnknownServiceBody(Box<Body>),
    UnknownServiceFrame(Box<Frame>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{self:?}"))
    }
}

/// How loudly an error that reached a request boundary deserves to be logged.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Severity {
    /// A deliberate protocol answer, or ordinary transport noise. The broker is
    /// working; the client is expected to retry or has simply gone away.
    Expected,

    /// Unexpected, but handled.
    Unexpected,

    /// Everything else.
    Failure,
}

/// Classify an error by whether it is something going wrong.
///
/// Logging by whether a value is an `Err` puts the broker working correctly into
/// the error plane: a `NOT_COORDINATOR` answered on purpose — what #243 added, so
/// that a failed forward makes the client retry against the real owner instead of
/// splitting the group — is protocol flow control, and every rollout produces a
/// burst of them. The cost is not the lines but what they do to the ones that
/// matter (#289).
///
/// The decision cannot live in this crate. A layer here sees `S::Error` and has
/// no idea whether a variant means `NOT_COORDINATOR`; the error type does. So the
/// classification is a trait implemented next to each error type, and the
/// boundaries that log call it instead of each growing their own match — they had
/// already drifted apart, one logging connection errors at `debug` while another
/// logged the same class at `error`.
///
/// The default is [`Severity::Failure`]: a type that has not thought about this
/// keeps the loud behaviour, so adding a variant cannot quietly downgrade it.
pub trait Classify {
    fn severity(&self) -> Severity {
        Severity::Failure
    }
}

impl Classify for Error {
    fn severity(&self) -> Severity {
        match self {
            // The peer went away. Routine for a broker that clients connect to
            // and disconnect from continuously. `TimedOut` is how a peer that
            // vanished without a FIN dies once keepalive gives up on it (#605).
            Self::Io(io)
                if matches!(
                    io.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::TimedOut
                ) =>
            {
                Severity::Expected
            }

            // A client asked for more than the cap allows, or sent a prefix that
            // is not a length at all: its problem, but worth seeing.
            Self::FrameTooBig(_) | Self::FrameLength(_) => Severity::Unexpected,

            _ => Severity::Failure,
        }
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        let shared = Arc::new(value);
        Error::Join(shared)
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        let shared = Arc::new(value);
        Error::Io(shared)
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Error::Poison
    }
}

/// The whole frame's length — the four-byte prefix plus the payload it announces
/// — or `None` when the prefix is not a length at all.
///
/// Kafka writes the payload length as a signed `i32`, so a peer can send one that
/// is negative. `as usize` turned `-1` into `usize::MAX`, and the `+ 4` then
/// wrapped it back to `3`: a garbage prefix silently became a plausible tiny
/// frame and the connection went on to mis-frame everything after it. `try_from`
/// refuses instead, and the caller closes the connection — which is what a peer
/// that is not speaking the protocol has earned.
fn frame_length(encoded: [u8; 4]) -> Option<usize> {
    usize::try_from(i32::from_be_bytes(encoded))
        .ok()
        .map(|payload| payload + encoded.len())
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    let named = InstrumentationScope::builder(env!("CARGO_PKG_NAME"));
    let versioned = named.with_version(env!("CARGO_PKG_VERSION"));
    let scope = versioned.with_schema_url(semconv::SCHEMA_URL).build();
    global::meter_with_scope(scope)
});

/// A `u64` counter on [`METER`].
fn counter(name: &'static str, description: &'static str) -> Counter<u64> {
    let described = METER.u64_counter(name).with_description(description);
    described.build()
}

/// A `u64` histogram on [`METER`], in `unit`, bucketed at `boundaries`.
fn histogram(
    name: &'static str,
    unit: &'static str,
    description: &'static str,
    boundaries: &[f64],
) -> Histogram<u64> {
    let described = METER.u64_histogram(name).with_description(description);
    let bucketed = described
        .with_unit(unit)
        .with_boundaries(boundaries.to_vec());
    bucketed.build()
}

/// The first IPv4 address `url`'s host and port resolve to, the lookup's time
/// recorded in `dns_lookup_duration`, and [`Error::UnknownHost`] when either is
/// missing or nothing IPv4 comes back.
///
/// ```
/// use std::net::{Ipv4Addr, SocketAddr};
///
/// use tansu_service::host_port;
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), tansu_service::Error> {
/// let resolved = host_port(Url::parse("tcp://localhost:9092")?).await?;
/// assert_eq!(SocketAddr::from((Ipv4Addr::LOCALHOST, 9092)), resolved);
/// # Ok::<(), tansu_service::Error>(())
/// # }
/// ```
pub async fn host_port(url: Url) -> Result<SocketAddr, Error> {
    let (Some(host), Some(port)) = (url.host_str(), url.port()) else {
        return Err(Error::UnknownHost(url));
    };

    let attributes = [KeyValue::new("url", String::from(url.as_str()))];
    let asked = SystemTime::now();
    let resolved = lookup_host(format!("{host}:{port}")).await;
    if resolved.is_ok() {
        let took = asked.elapsed().map_or(0, |took| took.as_millis() as u64);
        DNS_LOOKUP_DURATION.record(took, &attributes);
    }

    let Some(first) = resolved?.find(SocketAddr::is_ipv4) else {
        return Err(Error::UnknownHost(url));
    };
    debug!(socket_addr = ?first);
    Ok(first)
}

/// Bucket boundaries for every duration histogram here, in milliseconds.
///
/// The OTel SDK's default ladder stops at a top finite bucket of 10 000 — of
/// anything, whatever its unit. For a broker that is 10 s, and every request
/// slower than that lands in `+Inf` together, which is where #535's 24 s
/// Fetches were: a p99 computed over that ladder cannot say whether the tail is
/// 11 s or 40 s, and #537 halving it would not have shown (#539).
///
/// Sixteen boundaries, the same count as the SDK default, so the memory and
/// export cost is unchanged. Dense to 1 s because that is where a healthy
/// broker sits, then decade-ish to a minute, which is past any timeout a client
/// in this fleet uses.
pub(crate) const DURATION_MS_BOUNDARIES: &[f64] = &[
    1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 20000.0,
    30000.0, 60000.0,
];

/// Bucket boundaries for every size histogram here, in bytes.
///
/// Same defect as the durations and worse: the default top finite bucket of
/// 10 000 is 10 kB, and 9% of 91.6 M responses measured in production exceeded
/// it, all of them in one `+Inf` bucket. #537 moved a response from 14.7 MB to
/// 5.36 MB and the broker's own metrics could not see it (#539).
///
/// Powers of four to 1 MiB, then powers of two to 64 MiB. The top matters:
/// the Fetch clamp defaults to 5 MiB and is a per-deployment key that has been
/// run at 16 MiB, so the interesting region is exactly the one the default
/// ladder collapses.
pub(crate) const SIZE_BYTES_BOUNDARIES: &[f64] = &[
    64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 2097152.0, 4194304.0,
    8388608.0, 16777216.0, 33554432.0, 67108864.0,
];

pub(crate) static DNS_LOOKUP_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    histogram(
        "dns_lookup_duration",
        "ms",
        "DNS lookup latencies",
        DURATION_MS_BOUNDARIES,
    )
});

pub(crate) static REQUEST_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    histogram(
        "tansu_request_size",
        "By",
        "The API request size in bytes",
        SIZE_BYTES_BOUNDARIES,
    )
});

pub(crate) static RESPONSE_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    histogram(
        "tansu_response_size",
        "By",
        "The API response size in bytes",
        SIZE_BYTES_BOUNDARIES,
    )
});

pub(crate) static REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    histogram(
        "tansu_request_duration",
        "ms",
        "The API request latencies in milliseconds",
        DURATION_MS_BOUNDARIES,
    )
});

/// How long the answer took to reach the socket, once the broker had it.
///
/// `tansu_request_duration` stamps when `inner.serve` returns, and `serve`
/// returns an already-encoded frame — so `write_all` plus `flush` is the one
/// part of answering a request that no histogram covered. #539 predicted it is
/// small; this is what says so, and it is the series that separates "the broker
/// was slow" from "the peer was not reading".
///
/// Recorded on the error path too. A write that fails after seconds of
/// backpressure is the interesting sample, and an `inspect`-shaped stamp would
/// drop exactly that one.
pub(crate) static RESPONSE_WRITE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    histogram(
        "tansu_response_write_duration",
        "ms",
        "Time from the encoded response in hand to flush returning",
        DURATION_MS_BOUNDARIES,
    )
});

/// Registers the two tokio runtime gauges, once per process.
///
/// `tansu_response_write_duration` on its own cannot say *why* a write was
/// slow: "the peer is not draining its socket" and "the runtime is saturated
/// and this continuation sat in a queue" produce the same number, and only the
/// second is a broker defect (#539). These two are what tell them apart —
/// a slow write with a shallow queue and idle workers is the peer.
///
/// Both are stable tokio API on a 64-bit target: `global_queue_depth` is
/// ungated and `worker_total_busy_duration` needs only `target_has_atomic =
/// "64"`, so neither asks for `--cfg tokio_unstable`.
///
/// The [`Handle`] is captured here rather than in the callback: the callback
/// runs on the exporter's thread, which is not inside the runtime, and
/// `Handle::current()` there would panic.
///
/// Call it from inside the runtime. Idempotent — a second call is the
/// `LazyLock` already forced.
pub fn register_runtime_gauges() {
    _ = LazyLock::force(&RUNTIME_GAUGES);
}

static RUNTIME_GAUGES: LazyLock<()> = LazyLock::new(|| {
    let Ok(handle) = Handle::try_current() else {
        // Not in a runtime, so there is nothing to observe and no `Handle` to
        // capture. The alternative is a panic in a metric registration, which
        // is not a thing that should be able to stop a broker.
        return;
    };

    let queue_depth = handle.clone();

    _ = METER
        .u64_observable_gauge("tansu_runtime_global_queue_depth")
        .with_description("Tasks waiting in the runtime's global queue")
        .with_callback(move |observer| {
            observer.observe(queue_depth.metrics().global_queue_depth() as u64, &[])
        })
        .build();

    _ = METER
        .u64_observable_gauge("tansu_runtime_worker_busy_ms")
        .with_unit("ms")
        .with_description("Total time the runtime's workers have spent busy")
        .with_callback(move |observer| {
            let metrics = handle.metrics();

            let busy = (0..metrics.num_workers())
                .map(|worker| metrics.worker_total_busy_duration(worker).as_millis() as u64)
                .sum();

            observer.observe(busy, &[])
        })
        .build();
});

pub(crate) static API_REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| counter("tansu_api_requests", "The number of API requests made"));

pub(crate) static API_ERRORS: LazyLock<Counter<u64>> =
    LazyLock::new(|| counter("tansu_api_errors", "The number of API errors"));

pub(crate) static BYTES_SENT: LazyLock<Counter<u64>> =
    LazyLock::new(|| counter("tansu_bytes_sent", "The number of bytes sent"));

/// Requests being served right now — working *or* parked (#362).
///
/// On its own this is the misleading half, and it is the number a scaler would
/// otherwise reach for: a Kafka broker holds requests open by design, so a
/// fleet doing nothing but waiting out `max.wait.ms` fetches reports the same
/// figure as one saturated with produce. Paired with [`REQUESTS_PARKED`] it
/// becomes the useful one, because the difference is the requests actually
/// making progress.
pub(crate) static REQUESTS_IN_FLIGHT: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    METER
        .i64_up_down_counter("tansu_requests_in_flight")
        .with_description("API requests being served, whether working or parked in a long poll")
        .build()
});

/// Requests parked in a long poll: waiting on a clock or on another client,
/// not on this replica (#362).
///
/// A fetch inside `max.wait.ms` with nothing to return, and a `JoinGroup` or
/// `SyncGroup` waiting out a rebalance window, are parked. A request waiting on
/// the object store is **not**: that is work in progress, and it is work a
/// second replica could be doing in parallel, which is what a scaler should be
/// reacting to.
///
/// Emitted from more than one crate — the fetch poll lives in the storage
/// engine, the group polls in the broker — so the same name arrives under two
/// instrumentation scopes. `sum()` over it, as the documented scaler
/// expression does across replicas anyway.
pub(crate) static REQUESTS_PARKED: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    METER
        .i64_up_down_counter("tansu_requests_parked")
        .with_description("API requests parked in a long poll, waiting rather than working")
        .build()
});

pub(crate) static BYTES_RECEIVED: LazyLock<Counter<u64>> =
    LazyLock::new(|| counter("tansu_bytes_received", "The number of bytes received"));

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct LatencyIntroducingLayer {
    seed: u64,
    latency: Range<u64>,
}

impl LatencyIntroducingLayer {
    pub fn with_seed(self, seed: u64) -> Self {
        let mut seeded = self;
        seeded.seed = seed;
        seeded
    }

    pub fn with_latency_millis(self, latency: Range<u64>) -> Self {
        let mut slowed = self;
        slowed.latency = latency;
        slowed
    }
}

impl<S> Layer<S> for LatencyIntroducingLayer {
    type Service = LatencyIntroducingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        let seeded = LatencyIntroducingService::new(inner).with_seed(self.seed);
        seeded.with_latency(self.latency.clone())
    }
}

#[derive(Clone)]
pub struct LatencyIntroducingService<S> {
    inner: S,
    rng: Arc<Mutex<SmallRng>>,
    latency: Range<u64>,
}

impl<S> fmt::Debug for LatencyIntroducingService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut shown = f.debug_struct("LatencyIntroducingService");
        _ = shown.field("latency", &self.latency);
        shown.finish_non_exhaustive()
    }
}

impl<S> LatencyIntroducingService<S> {
    pub fn new(inner: S) -> Self {
        let latency = 50..150;
        let rng = Arc::new(Mutex::new(SmallRng::seed_from_u64(0)));
        let wrapped = inner;
        LatencyIntroducingService {
            inner: wrapped,
            latency,
            rng,
        }
    }

    pub fn with_seed(self, seed: u64) -> Self {
        let mut reseeded = self;
        reseeded.rng = Arc::new(Mutex::new(SmallRng::seed_from_u64(seed)));
        reseeded
    }

    pub fn with_latency(self, latency: Range<u64>) -> Self {
        let mut slowed = self;
        slowed.latency = latency;
        slowed
    }

    #[tracing::instrument(skip_all)]
    async fn introduce_latency(&self) -> Result<(), Error> {
        let millis = {
            let mut rng = self.rng.lock()?;
            rng.random_range(self.latency.clone())
        };
        let pause = Duration::from_millis(millis);
        debug!(latency = ?pause);
        sleep(pause).await;
        Ok(())
    }
}

impl<S, Q, State> Service<State, Q> for LatencyIntroducingService<S>
where
    S: Service<State, Q>,
    Q: Send + 'static,
    S::Error: From<Error>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;

    type Error = S::Error;

    #[tracing::instrument(skip(ctx, req))]
    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        Self::introduce_latency(self).await?;
        let served = S::serve(&self.inner, ctx, req).await;
        Self::introduce_latency(self).await?;
        served
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_peer_reaped_by_keepalive_is_expected() {
        for kind in [
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::TimedOut,
        ] {
            assert_eq!(
                Severity::Expected,
                Error::from(io::Error::from(kind)).severity(),
                "{kind:?}"
            );
        }
    }
}
