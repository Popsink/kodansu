// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{
    error::{self},
    fmt::Debug,
    future::Future,
    io,
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use futures::{StreamExt as _, stream::FuturesOrdered};
use nanoid::nanoid;
use opentelemetry::KeyValue;
use rama::{Context, Layer, Service};
use tansu_sans_io::{ApiKey as _, ProduceRequest, RootMessageMeta};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore, SemaphorePermit, mpsc},
    task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, debug, error, info_span, instrument, warn};

use crate::{
    BYTES_RECEIVED, BYTES_SENT, Classify, Error, REQUEST_DURATION, REQUEST_SIZE,
    REQUESTS_IN_FLIGHT, RESPONSE_SIZE, RESPONSE_WRITE_DURATION, Severity, THROTTLED_REQUESTS,
    THROTTLED_TIME, frame_length, register_runtime_gauges,
};

/// A request being served, counted for as long as this value lives (#362).
///
/// RAII for the reason [`crate::Parked`] is: every step between reading a frame
/// and writing its response can return early, and a leaked increment on an
/// up-down counter is a permanent lie rather than a blip.
struct InFlight;

impl InFlight {
    fn enter() -> Self {
        REQUESTS_IN_FLIGHT.add(1, &[]);
        Self
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        REQUESTS_IN_FLIGHT.add(-1, &[]);
    }
}

/// What this connection owes before its next request is read (#384).
///
/// KIP-219 semantics, and the reason a quota is not simply a sleep in the
/// request path. A throttled request is *waiting*, not working: delaying it
/// before it is served would count it in [`REQUESTS_IN_FLIGHT`] and fold the
/// delay into [`REQUEST_DURATION`], which would report a fleet deliberately
/// refusing traffic as a fleet saturated by it — and #362's scaler would add
/// replicas to serve load this broker has just decided not to serve. So the
/// response is written immediately, carrying the delay in `throttle_time_ms`,
/// and the wait happens here: between requests, where nothing counts it.
///
/// A shared cell rather than a return value because the two ends are far apart.
/// The delay is decided by a layer that knows who is asking — deep inside the
/// stack, above the codec — and applied by the loop that reads frames, at the
/// very bottom of it. Nothing in between has any business carrying it.
///
/// A connection with no quota layer above it has no [`Throttle`] in its
/// context, and nothing writes one: the wait is unreachable rather than zero.
#[derive(Clone, Debug, Default)]
pub struct Throttle(Arc<AtomicU64>);

impl Throttle {
    /// Owe `delay` before the next request on this connection is read.
    ///
    /// The longest wins rather than accumulating: two dimensions of one request
    /// each asking for a wait are asking for the *same* wait, and one request
    /// must never be able to mute a connection for the sum of every limit it
    /// touched.
    pub fn owe(&self, delay: Duration) {
        _ = self.0.fetch_max(delay.as_millis() as u64, Ordering::AcqRel);
    }

    /// What is owed right now, without taking it.
    ///
    /// The layer that decides a throttle is not the code that applies it, so
    /// asserting that the two agree needs a way to look without draining —
    /// draining is what the connection loop does, exactly once.
    #[must_use]
    pub fn owed(&self) -> Duration {
        Duration::from_millis(self.0.load(Ordering::Acquire))
    }

    /// Take what is owed, leaving nothing.
    fn take(&self) -> Duration {
        Duration::from_millis(self.0.swap(0, Ordering::AcqRel))
    }
}

/// A request the Kafka protocol says gets **no response at all** (#440).
///
/// `Produce` with `acks=0` is the only one: the client does not register a
/// handler for it, does not wait for it, and considers the record delivered the
/// moment the request is written to the socket. A broker that answers it anyway
/// puts a frame on the wire that the client has no in-flight request for — the
/// correlation-id stream desynchronises, and a client meeting a correlation id
/// it never sent **drops the connection**. Everything still queued on it dies,
/// and because the delivery reports already fired as success, nothing anywhere
/// reports a loss. That is the shape of "345 of 2 000 persisted, no error".
///
/// A shared cell for the same reason [`Throttle`] is one: the fact is known
/// deep in the stack, by the layer that has decoded the request, and it is acted
/// on at the bottom, by the loop that writes frames. Nothing in between has any
/// business carrying it.
///
/// One per request, and taken exactly once, when that request's answer is
/// due. It used to be one per connection, which was sound only while the loop
/// served one request at a time; with produces pipelined (#588) a cell shared
/// between requests in flight together would silence whichever was answered
/// next.
#[derive(Clone, Debug, Default)]
pub struct NoResponse(Arc<AtomicBool>);

impl NoResponse {
    /// This request gets no response.
    pub fn set(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Whether the request asked for silence.
    fn take(&self) -> bool {
        self.0.swap(false, Ordering::AcqRel)
    }
}

/// That a request has nothing left to admit, so the next one may be read
/// (#588).
///
/// A connection used to read a request, wait for its answer, write it, and only
/// then read the next. For a produce, the answer waits on the coalescing window
/// and its segment PUT — so a producer's later requests sat unread in the
/// socket, and a window never held more than one request from any connection.
/// For a prefix with one writer, which is the common case, the linger was then
/// pure latency: there was never a second request to coalesce with.
///
/// One per request, in that request's context. Whoever knows the request is
/// admitted says so — for a produce, once every batch is in its window — and
/// the next frame is read without waiting for the answer. The answers are still
/// written in request order, because a Kafka client matches each one to its
/// oldest request in flight.
///
/// Signalling is a promise about **order**, not durability: nothing the next
/// request does may overtake this one. A request nobody signals for is
/// answered before the next is read, as every request used to be — its answer
/// is the other thing that fires this.
#[derive(Clone, Debug, Default)]
pub struct Admission(Arc<Notify>);

impl Admission {
    /// This request is admitted: the next may be read.
    pub fn admitted(&self) {
        self.0.notify_one();
    }

    /// Until the request is admitted or answered, whichever comes first. A
    /// signal sent before this is awaited is kept, not lost.
    async fn wait(&self) {
        self.0.notified().await
    }
}

/// A request read and not yet answered, apart from the answer itself.
struct Owed<'a> {
    /// From the frame read to its answer written (#362).
    _in_flight: InFlight,
    _slot: SemaphorePermit<'a>,
    attributes: Vec<KeyValue>,
    no_response: NoResponse,
    admission: Admission,
}

/// What a request owes its client, still being worked out.
type Answer<'a, E> = Pin<Box<dyn Future<Output = Result<Bytes, E>> + Send + 'a>>;

/// The address of the connected peer, put into the service [`Context`] by
/// whoever accepted the connection.
///
/// It used to be read back off the request — `TcpStream::peer_addr` — inside
/// [`TcpContextService`], and that one call is what pinned the whole service
/// stack to a bare [`TcpStream`]: a TLS stream cannot answer it, so TLS could
/// not be layered underneath (#358). Every accept site already has the address
/// in hand, so it carries it rather than the stream type having to answer for
/// it.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Peer(pub SocketAddr);

/// The API key at the head of a request frame, or `-1` (#410).
///
/// The frame carries its own four-byte size prefix, so the request header — and
/// the `api_key (i16)` that starts it — begins at byte 4.
///
/// Validated against the protocol's own request table before it becomes a label.
/// A SASL v0 handshake continues as opaque packets that are not Kafka frames at
/// all, so their first two bytes are not an API key; labelling three histograms
/// with whatever they happen to say would put unbounded cardinality on them.
/// `-1` is the same "not a known API" value the wire uses for an absent id.
fn frame_api_key(request: &Bytes) -> i64 {
    request
        .get(4..6)
        .and_then(|head| <[u8; 2]>::try_from(head).ok())
        .map(i16::from_be_bytes)
        .filter(|api_key| RootMessageMeta::messages().requests().contains_key(api_key))
        .map_or(-1, i64::from)
}

/// A [`Layer`] that listens for TCP connections
#[derive(Clone, Debug, Default)]
pub struct TcpListenerLayer {
    cancellation: CancellationToken,
}

impl TcpListenerLayer {
    pub fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }
}

impl<S> Layer<S> for TcpListenerLayer {
    type Service = TcpListenerService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            cancellation: self.cancellation.clone(),
            inner,
        }
    }
}

/// A [`Service`] that listens for TCP connections
#[derive(Clone, Default)]
pub struct TcpListenerService<S> {
    cancellation: CancellationToken,
    inner: S,
}

impl<S> Debug for TcpListenerService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TcpListenerService)).finish()
    }
}

impl<State, S> Service<State, TcpListener> for TcpListenerService<S>
where
    S: Service<State, TcpStream> + Clone,
    S::Response: Debug,
    S::Error: error::Error + Classify,
    State: Clone + Send + Sync + 'static,
{
    type Response = ();
    type Error = S::Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<State>,
        req: TcpListener,
    ) -> Result<Self::Response, Self::Error> {
        // The one place in this crate guaranteed to be inside the runtime and
        // reached once per process: the gauges capture a `Handle`, and their
        // callbacks run on the exporter's thread where `Handle::current()`
        // would panic (#539).
        register_runtime_gauges();

        let mut set = JoinSet::new();

        loop {
            tokio::select! {
                Ok((stream, addr)) = req.accept() => {
                    debug!(?req, ?stream, %addr);

                    let service = self.inner.clone();

                    let ctx = {
                        let mut ctx = ctx.clone();
                        _ = ctx.insert(Peer(addr));
                        ctx
                    };

                    let handle = set.spawn(async move {
                            match service.serve(ctx, stream).await {
                                // The connection ends here, and it ends because
                                // of this error — a fact the client experiences
                                // and nothing else records. Report it at the
                                // severity the error itself claims, rather than
                                // at `debug` for everything, which is what this
                                // boundary used to do while the broker's own
                                // accept loop logged the same class at `error`
                                // (#289).
                                Err(error) => match error.severity() {
                                    Severity::Expected => debug!(%addr, %error),
                                    Severity::Unexpected => warn!(%addr, %error),
                                    Severity::Failure => {
                                        error!(%addr, %error, "connection ended, no response written")
                                    }
                                },

                                Ok(response) => {
                                    debug!(%addr, ?response)
                                }
                        }
                    });

                    debug!(?handle);
                    continue;
                }

                v = set.join_next(), if !set.is_empty() => {
                    debug!(?v);
                }

                cancelled = self.cancellation.cancelled() => {
                    debug!(?cancelled);
                    break;
                }
            }
        }

        Ok(())
    }
}

/// A [context state][`Context#method.state`] state used by [`TcpContextLayer`] and [`TcpContextService`]
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct TcpContext {
    cluster_id: Option<String>,
    maximum_frame_size: Option<usize>,

    /// Cancelled when this process has been asked to stop (#361).
    ///
    /// Read only *between* requests, never during one: a connection is closed
    /// when it is sitting idle waiting for the next frame, and a request
    /// already being served runs to its response whatever the drain is doing.
    /// That split is the whole point — closing between requests is a
    /// reconnect, which every client handles; closing during one is the
    /// dropped socket #300 is about.
    ///
    /// Reading it here rather than in the accept loop is what keeps a scale-in
    /// fast. A Kafka client keeps its connections open and idle between polls,
    /// so a drain that waited for connections to *end* would wait out its whole
    /// grace period on every shutdown and then cut them anyway.
    ///
    /// The default is a token nothing cancels, so a stack built without one
    /// serves connections until the client goes away, as before.
    drain: CancellationToken,

    /// How many requests one connection may have read and not yet answered
    /// (#588): [`Self::PIPELINE_DEPTH`] unless an operator lowers it, and `1`
    /// is pipelining turned off.
    pipeline_depth: usize,
}

/// Armed, unlike every `Option` beside it.
///
/// The cap existed and was `None` — the guard that reads it is
/// `is_some_and(...)`, so the broker allocated whatever a client's four-byte
/// prefix claimed, before any validation and before a byte of the frame had
/// arrived (#477). Kafka has had `socket.request.max.bytes` since the beginning
/// and defaults it to 100 MiB; a broker without one is missing a limit, not
/// offering a feature.
///
/// It stays an `Option` so an operator can still turn it off deliberately, which
/// is different from never having turned it on.
impl Default for TcpContext {
    fn default() -> Self {
        Self {
            cluster_id: None,
            maximum_frame_size: Some(Self::MAXIMUM_FRAME_SIZE),
            drain: CancellationToken::default(),
            pipeline_depth: Self::PIPELINE_DEPTH,
        }
    }
}

impl TcpContext {
    /// Kafka's `socket.request.max.bytes` default, and ours.
    ///
    /// Comfortably above any frame this broker can legitimately be sent: the
    /// engine's own `message_max_bytes` defaults to Kafka's 1 048 588, and a
    /// deployment that raises it past this has to raise this too — hence
    /// `--socket-request-max-bytes`.
    pub const MAXIMUM_FRAME_SIZE: usize = 100 * 1024 * 1024;

    /// The most requests one connection may have read and not yet answered
    /// (#588), and the default.
    ///
    /// Kafka's own `max.in.flight.requests.per.connection` default, and the
    /// most an idempotent producer may set — Java and librdkafka both refuse
    /// more with `enable.idempotence` on. A producer without idempotence may
    /// set more, and is held to this: each slot is a whole request, up to
    /// `socket.request.max.bytes`, and five is what every producer can fill.
    ///
    /// It is also `IDEMPOTENT_WINDOW` in `tansu-storage`: the five batches per
    /// producer the duplicate check remembers. Kafka caps the client at five
    /// for that reason — a batch it resends is still in the window — so the two
    /// numbers move together or not at all.
    ///
    /// A client configured for more is not refused: its next request waits in
    /// the socket until an answer frees a slot.
    pub const PIPELINE_DEPTH: usize = 5;

    pub fn cluster_id(self, cluster_id: Option<String>) -> Self {
        Self { cluster_id, ..self }
    }

    pub fn maximum_frame_size(self, maximum_frame_size: Option<usize>) -> Self {
        Self {
            maximum_frame_size,
            ..self
        }
    }

    /// Watch `drain` for this process being asked to stop (#361).
    pub fn drain(self, drain: CancellationToken) -> Self {
        Self { drain, ..self }
    }

    /// Read at most `pipeline_depth` requests ahead of their answers (#588),
    /// held to `1..=`[`Self::PIPELINE_DEPTH`]: `1` turns pipelining off, and
    /// each slot above it is another whole request a connection may hold.
    pub fn pipeline_depth(self, pipeline_depth: usize) -> Self {
        Self {
            pipeline_depth: pipeline_depth.clamp(1, Self::PIPELINE_DEPTH),
            ..self
        }
    }
}

/// A [`Layer`] that injects the [`TcpContext`] into the service [`Context`] state
#[derive(Clone, Debug, Default)]
pub struct TcpContextLayer {
    state: TcpContext,
}

impl TcpContextLayer {
    pub fn new(state: TcpContext) -> Self {
        Self { state }
    }
}

impl<S> Layer<S> for TcpContextLayer {
    type Service = TcpContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            state: self.state.clone(),
        }
    }
}

/// A [`Service`] that requires the [`TcpContext`] as the service [`Context`] state
#[derive(Clone)]
pub struct TcpContextService<S> {
    inner: S,
    state: TcpContext,
}

impl<S> Debug for TcpContextService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TcpContextService)).finish()
    }
}

/// Generic over the stream, and not over [`TcpStream`] alone, so that the same
/// stack serves a TLS stream (#358). The peer address comes from [`Peer`] in the
/// context, which is what the stream used to be asked for — see [`Peer`].
impl<State, S, Stream> Service<State, Stream> for TcpContextService<S>
where
    S: Service<TcpContext, Stream>,
    State: Clone + Send + Sync + 'static,
    Stream: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    #[instrument(skip_all, fields(peer = ?ctx.get::<Peer>().map(|Peer(peer)| *peer)))]
    async fn serve(&self, ctx: Context<State>, req: Stream) -> Result<Self::Response, Self::Error> {
        let (ctx, _) = ctx.swap_state(self.state.clone());

        self.inner.serve(ctx, req).await
    }
}

/// Read the rest of a frame whose four-byte prefix has already been taken,
/// returning the whole frame — prefix included, which is what every layer above
/// expects to decode.
///
/// **The commitment grows with what has arrived, not with what was announced**
/// (#477). This used to be `vec![0u8; frame_length(size)]`: one zeroed
/// allocation of exactly the length a client claimed, made before a byte of it
/// had been read. With no cap armed that was unbounded, and with a cap armed it
/// is still the cap — a peer that announces the maximum and then dribbles holds
/// the whole thing, times however many connections it opens. Growing in chunks
/// makes the memory a function of the traffic instead of the claim.
///
/// The chunk is a compromise, not a tuning knob. A frame that fits in one — which
/// is every ordinary request — is allocated once at exactly its size and read in
/// one `read_exact`, precisely as before, so nothing is paid on the hot path.
/// Above that, each round grows the buffer geometrically, so the copying stays
/// amortised and the capacity stays within a factor of two of the bytes actually
/// received.
async fn read_frame<R, E>(req: &mut R, size: [u8; 4]) -> Result<Bytes, E>
where
    R: AsyncReadExt + Unpin,
    E: From<Error> + From<io::Error>,
{
    /// How much of an oversized frame is committed at a time.
    const CHUNK: usize = 64 * 1024;

    let Some(length) = frame_length(size) else {
        let announced = i32::from_be_bytes(size);

        warn!(
            announced,
            "frame prefix is not a length; closing the connection"
        );

        return Err(E::from(Error::FrameLength(announced)));
    };

    let mut request = Vec::with_capacity(length.min(CHUNK));
    request.extend_from_slice(&size[..]);

    while request.len() < length {
        let filled = request.len();
        let want = length.min(filled + CHUNK);

        // `resize` zeroes only the window about to be filled and `read_exact`
        // then overwrites it, so nothing is zeroed that is not immediately
        // written — unlike the whole announced length, which was.
        request.resize(want, 0);

        _ = req
            .read_exact(&mut request[filled..want])
            .await
            .inspect_err(|err| error!(?err))?;
    }

    BYTES_RECEIVED.add(request.len() as u64, &[]);

    Ok(Bytes::from(request))
}

/// A [`Service`] writing [`Bytes`] into a [`TcpStream`], responding with a length delimited frame of [`Bytes`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesTcpService;

impl Service<TcpStream, Bytes> for BytesTcpService {
    type Response = Bytes;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        mut ctx: Context<TcpStream>,
        req: Bytes,
    ) -> Result<Self::Response, Self::Error> {
        let stream = ctx.state_mut();

        stream.write_all(&req[..]).await?;
        BYTES_SENT.add(req.len() as u64, &[]);

        let mut size = [0u8; 4];
        _ = stream.read_exact(&mut size).await?;

        // The response direction gets the same cap as the request direction
        // (#477). This side has no [`TcpContext`] to read an operator's value
        // from — it is one request on a socket this service owns — so the
        // constant is the whole policy: a peer claiming more than a broker could
        // ever legitimately answer with is not answering, and reading it would be
        // taking its word for how much memory to commit.
        if frame_length(size).is_some_and(|length| length > TcpContext::MAXIMUM_FRAME_SIZE) {
            let length = frame_length(size).unwrap_or_default();

            warn!(
                length,
                maximum_frame_size = TcpContext::MAXIMUM_FRAME_SIZE,
                "rejecting an oversized response frame"
            );

            return Err(Error::FrameTooBig(length));
        }

        read_frame(stream, size).await
    }
}

/// A [`Layer`] receiving [`Bytes`] from a [`TcpStream`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpBytesLayer<State = ()> {
    _state: PhantomData<State>,
}

impl<S, State> Layer<S> for TcpBytesLayer<State> {
    type Service = TcpBytesService<S, State>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            _state: PhantomData,
        }
    }
}

/// A [`Service`] receiving [`Bytes`] from a [`TcpStream`], calling an inner [`Service`] and sending [`Bytes`] into the [`TcpStream`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpBytesService<S, State> {
    inner: S,
    _state: PhantomData<State>,
}

impl<S, State> Debug for TcpBytesService<S, State> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TcpBytesService)).finish()
    }
}

impl<S, State> TcpBytesService<S, State> {
    fn elapsed_millis(&self, start: SystemTime) -> u64 {
        start
            .elapsed()
            .map_or(0, |duration| duration.as_millis() as u64)
    }
}

impl<S, State> TcpBytesService<S, State>
where
    S: Service<State, Bytes, Response = Bytes>,
    S::Error: From<Error> + From<io::Error> + Debug,
    State: Clone + Default + Send + Sync + 'static,
{
    #[instrument(skip_all)]
    async fn wait<R>(
        &self,
        req: &mut R,
        maximum_frame_size: Option<usize>,
    ) -> Result<[u8; 4], S::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let mut size = [0u8; 4];

        _ = req
            .read_exact(&mut size)
            .await
            .inspect_err(|err| debug!(?err))?;

        let Some(length) = frame_length(size) else {
            let announced = i32::from_be_bytes(size);

            warn!(
                announced,
                "frame prefix is not a length; closing the connection"
            );

            return Err(Into::into(Error::FrameLength(announced)));
        };

        // Reject a frame LARGER than the cap (#244). The comparison used to run the
        // other way round, so the guard fired on every frame *smaller* than the
        // limit and passed everything above it: the cap did not cap, and it refused
        // ordinary traffic. Nothing set `maximum_frame_size`, so it was inert until
        // #477 gave [`TcpContext`] a default — and the first operator to arm it,
        // which is the natural response to a payload-size incident, would have taken
        // every connection down instead.
        //
        // Rejection ends the connection task, so the peer sees a close mid-request
        // rather than an error response — `early eof` on the client side. Hence the
        // `warn!`: it is the only place that says which limit was hit and by how
        // much, and without it the symptom is indistinguishable from a peer
        // disappearing.
        if maximum_frame_size.is_some_and(|maximum| length > maximum) {
            warn!(
                length,
                maximum_frame_size, "rejecting an oversized frame; closing the connection"
            );

            return Err(Into::into(Error::FrameTooBig(length)));
        }

        Ok(size)
    }

    #[instrument(skip_all)]
    async fn read<R>(&self, req: &mut R, size: [u8; 4]) -> Result<Bytes, S::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        read_frame(req, size).await
    }

    #[instrument(skip_all)]
    async fn process(
        &self,
        attributes: &[KeyValue],
        ctx: Context<TcpContext>,
        request: Bytes,
    ) -> Result<Bytes, S::Error> {
        REQUEST_SIZE.record(request.len() as u64, attributes);

        let (ctx, _) = ctx.swap_state(State::default());
        let request_start = SystemTime::now();

        // Deliberately does not log the error. It propagates, through `req` and
        // the `serve` loop, to the per-connection boundary that ends the
        // connection because of it — and that boundary logs it there. Logging
        // here as well put every error into the error plane twice, which is how
        // one `NOT_COORDINATOR` became two `ERROR` lines (#289).
        self.inner.serve(ctx, request).await.inspect(|_| {
            let elapsed_millis = self.elapsed_millis(request_start);

            REQUEST_DURATION.record(elapsed_millis, attributes);
        })
    }

    #[instrument(skip_all)]
    async fn write<W>(
        &self,
        req: &mut W,
        frame: Bytes,
        attributes: &[KeyValue],
    ) -> Result<(), S::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        // Recorded here rather than where the response was built, so a response
        // that is never written is never counted as one (#440): `acks=0` is
        // answered with silence, and a `RESPONSE_SIZE` that included those
        // would report bytes this broker did not send.
        RESPONSE_SIZE.record(frame.len() as u64, attributes);

        // Deliberately does not log. A write that fails here is almost always the
        // client having gone away mid-response — `BrokenPipe`, `ConnectionReset` —
        // which is routine for a broker clients connect to and drop continuously,
        // and it was logged at ERROR unconditionally.
        //
        // #289 removed the same duplication from `process` and reworked the
        // per-connection boundaries to classify, but missed this site. beta.36
        // found it in twenty minutes: with the retriable protocol answers gone from
        // the error plane, the one remaining unclassified emitter was the only
        // ERROR left standing — `err=Os { code: 32, kind: BrokenPipe }` from
        // exactly here.
        //
        // The error still propagates to the boundary that ends the connection, and
        // that boundary asks the error what it is worth ([`crate::Classify`]),
        // where a broken pipe is `Severity::Expected`. So dropping the log loses
        // nothing and stops asserting that a departing client is a fault.
        let mut w = BufWriter::new(req);
        let start = SystemTime::now();

        // Not `write_all(..).await?` followed by a stamp: `?` returns, and a
        // write that fails after seconds of backpressure is the sample worth
        // having (#539). Both arms record, so the histogram counts what the
        // socket cost whether or not the peer was still there.
        let written = async {
            w.write_all(&frame).await?;
            BYTES_SENT.add(frame.len() as u64, &[]);
            w.flush().await
        }
        .await;

        RESPONSE_WRITE_DURATION.record(self.elapsed_millis(start), attributes);

        written.map_err(Into::into)
    }
}

impl<S, State, Stream> Service<TcpContext, Stream> for TcpBytesService<S, State>
where
    S: Service<State, Bytes, Response = Bytes>,
    S::Error: From<Error> + From<io::Error> + Debug,
    State: Clone + Default + Send + Sync + 'static,
    Stream: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static,
{
    type Response = ();

    type Error = S::Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<TcpContext>,
        req: Stream,
    ) -> Result<Self::Response, Self::Error> {
        let attributes = {
            let state = ctx.state();

            let mut attributes = vec![];

            if let Some(cluster_id) = state.cluster_id.clone() {
                attributes.push(KeyValue::new("cluster_id", cluster_id))
            }

            attributes
        };

        let maximum_frame_size = ctx.state().maximum_frame_size;
        let drain = ctx.state().drain.clone();
        let depth = ctx.state().pipeline_depth;

        // One cell for the life of the connection, and the same one every
        // request writes into: the quota layer above finds it in the context,
        // and the reader below drains it before its next read (#384). Shared
        // rather than per request even with produces pipelined (#588), because
        // what it owes is the *connection's* silence: the longest delay any
        // answered request asked for, taken before the next frame is read.
        let throttle = Throttle::default();

        let ctx = {
            let mut ctx = ctx;
            _ = ctx.insert(throttle.clone());
            ctx
        };

        // Two halves, so a request can be read while an earlier one is still
        // being answered (#588). One task drives both, so nothing here outlives
        // the connection or needs to be `'static`.
        let slots = Semaphore::new(depth);
        let (mut reader, mut writer) = tokio::io::split(req);
        let (owe, mut owed) = mpsc::channel::<(Owed<'_>, Answer<'_, S::Error>)>(depth);

        // Cancelled by the answering half when it can no longer answer, so the
        // reading half stops reading requests nobody will answer (#588).
        let stop = CancellationToken::new();

        let reading = async {
            // Owned, so it is dropped when reading ends: that is what tells
            // the answering half nothing more is coming (#588).
            let owe = owe;

            loop {
                // Where a connection may be ended by the drain: between
                // requests. `biased` so the drain wins over a frame that has
                // already arrived — that request is retried on a connection to
                // a replica that is staying, where a cut mid-request could not
                // be (#361). Whatever has already been read is still answered:
                // the answering half runs until everything owed is written.
                let size = tokio::select! {
                    biased;

                    () = drain.cancelled() => {
                        debug!("closing an idle connection: this replica is stopping");
                        return Ok(());
                    }

                    size = self.wait(&mut reader, maximum_frame_size) => size?,
                };

                // Before the body is read, so what a connection holds is
                // bounded by the slots and not by them plus one (#588).
                let Ok(slot) = slots.acquire().await else {
                    return Ok(());
                };

                let request = self.read(&mut reader, size).await?;

                // Per-API, not just per-connection (#410), and derived here so
                // the response histograms carry it too (#539).
                let api_key = frame_api_key(&request);
                let labels = {
                    let mut labels = attributes.clone();
                    labels.push(KeyValue::new("api_key", api_key));
                    labels
                };

                // Anything but a produce is a barrier (#588): it is served only
                // once everything before it is answered, so every other API
                // sees exactly the connection it saw before pipelining — a
                // `Fetch` or an `EndTxn` behind a produce sees it acknowledged.
                if api_key != i64::from(ProduceRequest::KEY) {
                    _ = slots.acquire_many(depth as u32 - 1).await;
                }

                // From here to the response written: the span over which this
                // replica owes the client something, which is what "in flight"
                // has to mean for the difference against
                // `tansu_requests_parked` to be work (#362). After the barrier,
                // because a request waiting on it is not being served (#588).
                let in_flight = InFlight::enter();

                let no_response = NoResponse::default();
                let admission = Admission::default();

                let mut ctx = ctx.clone();
                _ = ctx.insert(no_response.clone());
                _ = ctx.insert(admission.clone());

                let answer: Answer<'_, S::Error> = {
                    let labels = labels.clone();
                    Box::pin(
                        async move { self.process(&labels, ctx, request).await }
                            .instrument(info_span!("answer", id = nanoid!())),
                    )
                };

                let owing = Owed {
                    _in_flight: in_flight,
                    _slot: slot,
                    attributes: labels,
                    no_response,
                    admission: admission.clone(),
                };

                if owe.send((owing, answer)).await.is_err() {
                    return Ok(());
                }

                // The next frame is read once this request is admitted, or
                // once it is answered — whichever comes first. A request that
                // never signals admission is answered first, which is every
                // request but a pipelined produce (#588).
                admission.wait().await;

                // The mute happens here, before the next read, where no counter
                // and no histogram is watching (#384).
                //
                // Deliberately *not* wrapped in `Parked` either: parked is
                // subtracted from in flight to get the fleet's `busy`, and
                // counting a wait that was never counted as in flight would
                // push that expression negative. The wait shows up in
                // `tansu_throttled_time` and nowhere else.
                let delay = throttle.take();

                if !delay.is_zero() {
                    debug!(?delay, "muting a connection over its quota");

                    THROTTLED_REQUESTS.add(1, &attributes[..]);
                    THROTTLED_TIME.add(delay.as_millis() as u64, &attributes[..]);

                    // The drain still wins: a replica that has been asked to
                    // stop must not hold a muted connection open for the
                    // throttle before noticing (#361).
                    tokio::select! {
                        biased;

                        () = drain.cancelled() => {
                            debug!("closing a throttled connection: this replica is stopping");
                            return Ok(());
                        }

                        () = sleep(delay) => {}
                    }
                }
            }
        };

        let reading = async { stop.run_until_cancelled(reading).await.unwrap_or(Ok(())) };

        let answering = async {
            let mut answers = FuturesOrdered::new();
            let mut open = true;
            let mut failed = None;

            loop {
                tokio::select! {
                    next = owed.recv(), if open && failed.is_none() => match next {
                        Some((owing, answer)) => {
                            answers.push_back(async move { (owing, answer.await) })
                        }

                        None => open = false,
                    },

                    // In request order, however they complete: a Kafka client
                    // matches each answer to its oldest request in flight (#588).
                    Some((owing, answer)) = answers.next(), if !answers.is_empty() => {
                        // Once one answer cannot be written, none after it
                        // can be: the client would match them to the wrong
                        // requests. They are still run to the end rather than
                        // dropped — a produce may be the one flushing its
                        // window, and cancelling a segment PUT mid-flight is
                        // the ambiguous create the flush exists to avoid (#89).
                        if failed.is_none() {
                            let written = match answer {
                                // `Produce` with `acks=0` is answered with
                                // silence, because that is what the protocol
                                // says and what the client is built for — see
                                // [`NoResponse`] (#440). The work still happened
                                // and is still counted; only the frame is withheld.
                                Ok(_) if owing.no_response.take() => {
                                    debug!("acks=0: the request is served and gets no response");
                                    Ok(())
                                }

                                Ok(frame) => self.write(&mut writer, frame, &owing.attributes).await,

                                // Deliberately not logged here: it ends the
                                // connection, and the boundary that ends it
                                // logs it (#289).
                                Err(error) => Err(error),
                            };

                            if let Err(error) = written {
                                failed = Some(error);
                                stop.cancel();
                            }
                        }

                        owing.admission.admitted();
                    }

                    else => break,
                }
            }

            failed.map_or(Ok(()), Err)
        };

        let (read, answered) = tokio::join!(reading, answering);

        answered.and(read)
    }
}

/// A [`Layer`] that handles and responds with [`Bytes`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesLayer;

impl<S> Layer<S> for BytesLayer {
    type Service = BytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that handles and responds with [`Bytes`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesService<S> {
    inner: S,
}

impl<S> Debug for BytesService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BytesService)).finish()
    }
}

impl<S, State> Service<State, Bytes> for BytesService<S>
where
    S: Service<State, Bytes, Response = Bytes>,
    State: Clone + Send + Sync + 'static,
{
    type Response = Bytes;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, ctx: Context<State>, req: Bytes) -> Result<Self::Response, Self::Error> {
        debug!(req = ?&req[..]);
        self.inner
            .serve(ctx, req)
            .await
            .inspect(|response| debug!(response = ?&response[..]))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        sync::Mutex,
        task::{Context as TaskContext, Poll},
    };

    use opentelemetry::global;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, InMemoryMetricExporterBuilder, PeriodicReader, SdkMeterProvider,
        Temporality,
        data::{AggregatedMetrics, MetricData},
    };
    use tokio::io::AsyncWrite;

    use super::*;
    use tansu_sans_io::{ApiKey, ApiVersionsRequest, FetchRequest, ProduceRequest};

    /// A socket that refuses every write: what a peer that has gone away looks
    /// like from inside [`TcpBytesService::write`].
    struct BrokenPipe;

    impl AsyncWrite for BrokenPipe {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// How many data points the histogram `name` exported carrying `api_key`.
    fn labelled_points(exporter: &InMemoryMetricExporter, name: &str, api_key: i64) -> usize {
        exporter
            .get_finished_metrics()
            .expect("metrics")
            .iter()
            .flat_map(|resource| {
                resource
                    .scope_metrics()
                    .flat_map(|scope| scope.metrics())
                    .filter(|metric| metric.name() == name)
                    .filter_map(|metric| match metric.data() {
                        AggregatedMetrics::U64(MetricData::Histogram(histogram)) => Some(
                            histogram
                                .data_points()
                                .filter(|point| {
                                    point.attributes().any(|attribute| {
                                        attribute.key.as_str() == "api_key"
                                            && attribute.value == opentelemetry::Value::I64(api_key)
                                    })
                                })
                                .count(),
                        ),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            })
            .sum()
    }

    /// **The reason the stamp is not an `inspect`.**
    ///
    /// `write_all` and `flush` both return through `?`, so a stamp hung off
    /// `inspect` records on the ok path only — and a write that fails after
    /// seconds of backpressure is the sample #539 wants. This asserts the
    /// failing write is timed, which is the arm an `inspect` would drop.
    #[tokio::test]
    async fn a_refused_write_is_still_timed() {
        let exporter = InMemoryMetricExporterBuilder::new()
            .with_temporality(Temporality::Delta)
            .build();

        let provider = SdkMeterProvider::builder()
            .with_reader(
                PeriodicReader::builder(exporter.clone())
                    .with_interval(Duration::from_millis(20))
                    .build(),
            )
            .build();

        global::set_meter_provider(provider.clone());

        let service: TcpBytesService<EchoBytes, ()> = TcpBytesService {
            inner: EchoBytes,
            _state: PhantomData,
        };

        let api_key = i64::from(ProduceRequest::KEY);
        let attributes = [KeyValue::new("api_key", api_key)];

        assert!(
            service
                .write(
                    &mut BrokenPipe,
                    Bytes::from_static(b"a response"),
                    &attributes
                )
                .await
                .is_err(),
            "a refused write has to propagate"
        );

        provider.force_flush().expect("flush");

        assert_eq!(
            1,
            labelled_points(&exporter, "tansu_response_write_duration", api_key),
            "the failing write was not timed"
        );
    }

    /// A frame is labelled by the API it actually is, and anything that is not a
    /// known request is not labelled with its first two bytes (#410).
    #[test]
    fn a_frame_is_labelled_by_its_api_key() {
        let framed = |api_key: i16| {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&0i32.to_be_bytes());
            bytes.extend_from_slice(&api_key.to_be_bytes());
            bytes.extend_from_slice(&0i16.to_be_bytes());
            Bytes::from(bytes)
        };

        for api_key in [
            ProduceRequest::KEY,
            FetchRequest::KEY,
            ApiVersionsRequest::KEY,
        ] {
            assert_eq!(i64::from(api_key), frame_api_key(&framed(api_key)));
        }

        // An opaque SASL v0 packet is not a Kafka frame, so its leading bytes
        // are not an API key. Labelling three histograms with whatever they say
        // would put unbounded cardinality on them.
        assert_eq!(-1, frame_api_key(&framed(31_337)));

        // And a frame too short to hold a header carries no key either.
        assert_eq!(-1, frame_api_key(&Bytes::from_static(&[0, 0, 0, 0, 1])));
    }

    /// #244: the cap rejects a frame larger than the limit and accepts one at it.
    ///
    /// The comparison was inverted, which made the guard fire on every frame
    /// *smaller* than the limit and pass everything above it. Both halves are
    /// asserted here, at the boundary, because either alone would still pass with
    /// the operator reversed: a limit that rejects nothing looks identical to a
    /// correct one until something oversized arrives.
    #[tokio::test]
    async fn oversized_frames_are_rejected_at_the_boundary() {
        // `frame_length` is the declared size plus its own 4 bytes, so a declared
        // 96 is a 100-byte frame.
        const DECLARED: i32 = 96;
        let size = DECLARED.to_be_bytes();
        let length = frame_length(size).expect("a length");
        assert_eq!(100, length);

        let service: TcpBytesService<EchoBytes, ()> = TcpBytesService {
            inner: EchoBytes,
            _state: PhantomData,
        };

        // No cap configured: every frame passes. Reachable only by an operator
        // asking for it since #477 armed the default.
        assert!(service.wait(&mut &size[..], None).await.is_ok());

        // At the limit, and one byte of headroom: accepted.
        assert!(service.wait(&mut &size[..], Some(length)).await.is_ok());
        assert!(service.wait(&mut &size[..], Some(length + 1)).await.is_ok());

        // One byte over: refused.
        assert!(
            service
                .wait(&mut &size[..], Some(length - 1))
                .await
                .is_err(),
            "a frame larger than the cap must be rejected"
        );
    }

    /// **The bug #477 closes: the cap was armed by nobody.**
    ///
    /// The guard reads `Option<usize>` through `is_some_and`, so a `None` is not
    /// "no limit configured yet", it is "no limit, ever" — and
    /// `TcpContext::default()` produced exactly that, on every listener, which is
    /// what let a four-byte prefix decide how much the broker allocated. The
    /// #244 test above proves the comparison is the right way round; only this
    /// one proves anybody is doing the comparing.
    #[test]
    fn the_default_context_arms_the_cap() {
        assert_eq!(
            Some(TcpContext::MAXIMUM_FRAME_SIZE),
            TcpContext::default().maximum_frame_size,
        );

        // Kafka's `socket.request.max.bytes`, so a client that works against a
        // Kafka broker works against this one.
        assert_eq!(104_857_600, TcpContext::MAXIMUM_FRAME_SIZE);

        // And an operator can still turn it off, which is a different thing from
        // it never having been on.
        assert_eq!(
            None,
            TcpContext::default()
                .maximum_frame_size(None)
                .maximum_frame_size
        );
    }

    /// A negative prefix is not a length, and it must not become one.
    ///
    /// `i32::from_be_bytes(..) as usize + 4` turned `-1` into `usize::MAX` and
    /// then wrapped it to **3**: a garbage prefix became a plausible tiny frame,
    /// the guard passed it because 3 is under any cap, and everything read after
    /// it on that connection was mis-framed. Both ends of the range are asserted
    /// because the wrap is silent in release builds.
    #[test]
    fn a_negative_frame_prefix_is_not_a_length() {
        for announced in [-1i32, -4, i32::MIN] {
            assert_eq!(
                None,
                frame_length(announced.to_be_bytes()),
                "{announced} must not be read as a length"
            );
        }

        // The boundary either side of it still is one.
        assert_eq!(Some(4), frame_length(0i32.to_be_bytes()));
        assert_eq!(
            Some(i32::MAX as usize + 4),
            frame_length(i32::MAX.to_be_bytes())
        );
    }

    /// A frame bigger than one chunk arrives whole (#477).
    ///
    /// `read_frame` grows the buffer as bytes arrive instead of allocating the
    /// announced length up front, which means the multi-round path is real code
    /// on any frame over 64 KiB — and an off-by-one in the window it reads into
    /// would corrupt a large `Produce` rather than fail it. So this asserts the
    /// bytes, not the length.
    #[tokio::test]
    async fn a_frame_larger_than_one_chunk_is_read_whole() {
        // Three chunks and a bit, and a payload whose every byte is a function of
        // its position, so a chunk read into the wrong window shows up.
        const PAYLOAD: usize = 3 * 64 * 1024 + 17;

        let payload = (0..PAYLOAD).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let framed = frame(&payload[..]);

        let mut size = [0u8; 4];
        size.copy_from_slice(&framed[..4]);

        let read = read_frame::<_, Error>(&mut &framed[4..], size)
            .await
            .expect("a whole frame");

        assert_eq!(framed.len(), read.len());
        assert_eq!(&framed[..], &read[..]);
    }

    /// A `Service<(), Bytes>` that satisfies `TcpBytesService`'s bounds. `wait`
    /// never reaches the inner service, so echoing is enough.
    #[derive(Clone, Debug, Default)]
    struct EchoBytes;

    impl Service<(), Bytes> for EchoBytes {
        type Response = Bytes;
        type Error = Error;

        async fn serve(&self, _ctx: Context<()>, req: Bytes) -> Result<Bytes, Error> {
            Ok(req)
        }
    }

    /// Echoes, and owes a throttle on the first request only — a client over
    /// its quota once.
    #[derive(Clone, Debug)]
    struct ThrottleOnce {
        delay: Duration,
        served: Arc<AtomicU64>,
    }

    impl Service<(), Bytes> for ThrottleOnce {
        type Response = Bytes;
        type Error = Error;

        async fn serve(&self, ctx: Context<()>, req: Bytes) -> Result<Bytes, Error> {
            if self.served.fetch_add(1, Ordering::AcqRel) == 0
                && let Some(throttle) = ctx.get::<Throttle>()
            {
                throttle.owe(self.delay);
            }

            Ok(req)
        }
    }

    /// A length-delimited frame of `payload`, as this loop reads them.
    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut framed = (payload.len() as i32).to_be_bytes().to_vec();
        framed.extend_from_slice(payload);
        framed
    }

    /// **KIP-219, and the whole reason a quota is not a sleep in the request
    /// path (#384).**
    ///
    /// A throttled request is answered *immediately* — the client gets its
    /// response with `throttle_time_ms` in it — and the connection is muted
    /// afterwards, so it is the *next* request that waits. Delaying before the
    /// response instead would put the wait inside `tansu_requests_in_flight`
    /// and inside `tansu_request_duration`, and #362's scaler would read a
    /// fleet deliberately refusing traffic as one saturated by it.
    ///
    /// Both halves are asserted, because either alone passes with the wait in
    /// the wrong place: an implementation that slept before serving would also
    /// deliver two responses five seconds apart.
    #[tokio::test(start_paused = true)]
    async fn a_throttle_is_answered_at_once_and_waited_for_between_requests() {
        const DELAY: Duration = Duration::from_secs(5);

        let service: TcpBytesService<ThrottleOnce, ()> = TcpBytesService {
            inner: ThrottleOnce {
                delay: DELAY,
                served: Arc::new(AtomicU64::new(0)),
            },
            _state: PhantomData,
        };

        let (mut client, server) = tokio::io::duplex(4096);

        let connection = tokio::spawn(async move {
            service
                .serve(Context::with_state(TcpContext::default()), server)
                .await
        });

        // Two requests, both in flight before either is answered — a pipelining
        // client, which is what makes "muted between requests" observable.
        client
            .write_all(&frame(b"first"))
            .await
            .expect("first request");
        client
            .write_all(&frame(b"second"))
            .await
            .expect("second request");

        let started = tokio::time::Instant::now();

        let mut first = [0u8; 9];
        _ = client
            .read_exact(&mut first)
            .await
            .expect("the first response");

        assert_eq!(
            Duration::ZERO,
            started.elapsed(),
            "a throttled request must be answered at once, not slept on",
        );
        assert_eq!(&frame(b"first")[..], &first[..]);

        let mut second = [0u8; 10];

        // Still muted: the second request has been sent and is sitting unread.
        assert!(
            tokio::time::timeout(Duration::from_millis(1), client.read_exact(&mut second))
                .await
                .is_err(),
            "the connection must stay muted for the throttle it answered",
        );

        _ = client
            .read_exact(&mut second)
            .await
            .expect("the second response");

        assert_eq!(&frame(b"second")[..], &second[..]);
        assert!(
            started.elapsed() >= DELAY,
            "the second request must wait out the throttle, not the first",
        );

        drop(client);
        _ = connection.await;
    }

    /// Echoes, and asks for silence on the first request only — an `acks=0`
    /// produce followed by anything else.
    #[derive(Clone, Debug)]
    struct SilentOnce {
        served: Arc<AtomicU64>,
    }

    impl Service<(), Bytes> for SilentOnce {
        type Response = Bytes;
        type Error = Error;

        async fn serve(&self, ctx: Context<()>, req: Bytes) -> Result<Bytes, Error> {
            if self.served.fetch_add(1, Ordering::AcqRel) == 0
                && let Some(no_response) = ctx.get::<NoResponse>()
            {
                no_response.set();
            }

            Ok(req)
        }
    }

    /// A request that asks for silence gets none of its bytes on the wire — and
    /// the *next* request is answered normally, in its own right (#440).
    ///
    /// The second half is what makes this a test rather than a tautology. A
    /// signal that leaked into the following request would mute a connection
    /// permanently after one `acks=0` produce, and a client would see its
    /// `Metadata` call hang rather than a correlation-id mismatch — a different
    /// bug, equally invisible from the broker's side.
    #[tokio::test]
    async fn a_request_that_asks_for_silence_is_not_answered() {
        let service: TcpBytesService<SilentOnce, ()> = TcpBytesService {
            inner: SilentOnce {
                served: Arc::new(AtomicU64::new(0)),
            },
            _state: PhantomData,
        };

        let (mut client, server) = tokio::io::duplex(4096);

        let connection = tokio::spawn(async move {
            service
                .serve(Context::with_state(TcpContext::default()), server)
                .await
        });

        client
            .write_all(&frame(b"silent"))
            .await
            .expect("first request");
        client
            .write_all(&frame(b"answered"))
            .await
            .expect("second request");

        // The only bytes on the wire are the second request's. Reading the
        // second response's length first is the assertion: if the first had been
        // answered, these four bytes would be *its* length.
        let mut answered = [0u8; 12];
        _ = client
            .read_exact(&mut answered)
            .await
            .expect("the second response");

        assert_eq!(&frame(b"answered")[..], &answered[..]);

        // And nothing else follows, so the first was withheld rather than
        // reordered behind the second.
        let mut trailing = [0u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), client.read_exact(&mut trailing))
                .await
                .is_err(),
            "a withheld response must not arrive later",
        );

        drop(client);
        _ = connection.await;
    }

    /// A connection nothing throttles is not delayed at all, which is every
    /// connection on a broker without `--authentication`.
    #[tokio::test(start_paused = true)]
    async fn an_unthrottled_connection_is_never_delayed() {
        let service: TcpBytesService<EchoBytes, ()> = TcpBytesService {
            inner: EchoBytes,
            _state: PhantomData,
        };

        let (mut client, server) = tokio::io::duplex(4096);

        let connection = tokio::spawn(async move {
            service
                .serve(Context::with_state(TcpContext::default()), server)
                .await
        });

        let started = tokio::time::Instant::now();

        for payload in [&b"first"[..], &b"second"[..]] {
            client.write_all(&frame(payload)).await.expect("request");

            let mut response = vec![0u8; payload.len() + 4];
            _ = client.read_exact(&mut response).await.expect("response");

            assert_eq!(frame(payload), response);
        }

        assert_eq!(Duration::ZERO, started.elapsed());

        drop(client);
        _ = connection.await;
    }

    /// A frame the loop takes for a `Produce` — the API key at the head of
    /// the header — followed by an id the fake service below reads back.
    fn produce(id: u8) -> Vec<u8> {
        frame(&[0, 0, id])
    }

    /// A frame the loop takes for a `Metadata`: anything but a produce.
    fn metadata(id: u8) -> Vec<u8> {
        frame(&[0, 3, id])
    }

    /// A produce path the tests can hold open (#588).
    ///
    /// A produce is admitted and then waits for `gate` before it is answered,
    /// which is a coalescing window that has not flushed yet. Once the gate
    /// opens, the higher the id the sooner it completes, so answers written in
    /// completion order would come out backwards. Anything that is not a
    /// produce is answered at once, recording how many produces were still
    /// outstanding when it was served.
    #[derive(Clone, Debug)]
    struct Pipelined {
        log: Arc<Mutex<Vec<(&'static str, u8)>>>,
        gate: Arc<tokio::sync::watch::Sender<bool>>,
        outstanding: Arc<AtomicU64>,
        slow_admission: Option<(u8, Duration)>,
        fails: Option<u8>,
    }

    impl Pipelined {
        fn new() -> Self {
            Self {
                log: Arc::default(),
                gate: Arc::new(tokio::sync::watch::channel(false).0),
                outstanding: Arc::default(),
                slow_admission: None,
                fails: None,
            }
        }

        fn note(&self, event: &'static str, id: u8) {
            self.log.lock().expect("log").push((event, id));
        }

        fn noted(&self, event: &'static str) -> Vec<u8> {
            self.log
                .lock()
                .expect("log")
                .iter()
                .filter(|(noted, _)| *noted == event)
                .map(|(_, id)| *id)
                .collect()
        }

        fn open(&self) {
            _ = self.gate.send_replace(true);
        }

        /// Wait for `event` to have been noted `count` times. Under a paused
        /// clock the timeout fires only once nothing else can run, so missing
        /// it means the loop never got there, not that it was slow.
        async fn until(&self, event: &'static str, count: usize) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while self.noted(event).len() < count {
                    sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{event} noted {:?}, expected {count}", self.noted(event)));
        }
    }

    impl Service<(), Bytes> for Pipelined {
        type Response = Bytes;
        type Error = Error;

        async fn serve(&self, ctx: Context<()>, req: Bytes) -> Result<Bytes, Error> {
            let id = req[6];
            self.note("started", id);

            if req[4..6] != [0, 0] {
                let outstanding = self.outstanding.load(Ordering::Acquire) as u8;
                self.note("outstanding when served", outstanding);
                return Ok(req);
            }

            if let Some((slow, delay)) = self.slow_admission
                && slow == id
            {
                sleep(delay).await;
            }

            _ = self.outstanding.fetch_add(1, Ordering::AcqRel);
            self.note("admitted", id);
            ctx.get::<Admission>().expect("an admission").admitted();

            if self.fails == Some(id) {
                _ = self.outstanding.fetch_sub(1, Ordering::AcqRel);
                return Err(Error::Message(format!("request {id} fails")));
            }

            _ = self.gate.subscribe().wait_for(|open| *open).await;
            sleep(Duration::from_millis(u64::from(10 - id))).await;

            _ = self.outstanding.fetch_sub(1, Ordering::AcqRel);
            self.note("completed", id);

            Ok(req)
        }
    }

    fn connect(
        inner: Pipelined,
        context: TcpContext,
    ) -> (
        tokio::io::DuplexStream,
        tokio::task::JoinHandle<Result<(), Error>>,
    ) {
        let service: TcpBytesService<Pipelined, ()> = TcpBytesService {
            inner,
            _state: PhantomData,
        };

        let (client, server) = tokio::io::duplex(4096);

        let connection =
            tokio::spawn(async move { service.serve(Context::with_state(context), server).await });

        (client, connection)
    }

    async fn answered(client: &mut tokio::io::DuplexStream, expected: Vec<u8>) {
        let mut response = vec![0u8; expected.len()];
        _ = client.read_exact(&mut response).await.expect("a response");
        assert_eq!(expected, response);
    }

    /// **The defect #588 is about: one produce per window per connection.**
    ///
    /// Five produces are sent and the first is held, as a window that has not
    /// flushed holds it. The loop that waited for an answer before reading the
    /// next request admits one and stops, so all five being admitted at once is
    /// the assertion. They then complete backwards, and are answered forwards,
    /// because a Kafka client matches each answer to its oldest request.
    #[tokio::test(start_paused = true)]
    async fn admitted_produces_are_read_ahead_and_answered_in_order() {
        let inner = Pipelined::new();
        let (mut client, connection) = connect(inner.clone(), TcpContext::default());

        for id in 1..=5 {
            client.write_all(&produce(id)).await.expect("request");
        }

        inner.until("admitted", 5).await;
        assert_eq!(vec![1, 2, 3, 4, 5], inner.noted("admitted"));
        assert!(inner.noted("completed").is_empty());

        inner.open();

        for id in 1..=5 {
            answered(&mut client, produce(id)).await;
        }

        assert_eq!(vec![5, 4, 3, 2, 1], inner.noted("completed"));

        drop(client);
        _ = connection.await;
    }

    /// **Ordered admission: the next request is not even started until this
    /// one is admitted** (#588).
    ///
    /// The storage engine can await a metadata read before it buffers a batch
    /// (`routed_substream_of`), so two produces admitted concurrently could
    /// buffer in either order — and buffer order is offset order. Request 1 is
    /// slow to admit here; were request 2 started before request 1 had been
    /// admitted, it would be admitted first and take the lower offset.
    #[tokio::test(start_paused = true)]
    async fn a_slow_admission_is_not_overtaken() {
        let inner = Pipelined {
            slow_admission: Some((1, Duration::from_millis(100))),
            ..Pipelined::new()
        };
        inner.open();

        let (mut client, connection) = connect(inner.clone(), TcpContext::default());

        client.write_all(&produce(1)).await.expect("request");
        client.write_all(&produce(2)).await.expect("request");

        answered(&mut client, produce(1)).await;
        answered(&mut client, produce(2)).await;

        assert_eq!(
            vec![
                ("started", 1),
                ("admitted", 1),
                ("started", 2),
                ("admitted", 2)
            ],
            inner
                .log
                .lock()
                .expect("log")
                .iter()
                .filter(|(event, _)| *event != "completed")
                .copied()
                .collect::<Vec<_>>(),
        );

        drop(client);
        _ = connection.await;
    }

    /// **Anything but a produce is a barrier** (#588): it is not served while a
    /// produce read before it is unanswered, so a `Fetch` or `EndTxn` behind a
    /// produce sees exactly what it saw before pipelining.
    #[tokio::test(start_paused = true)]
    async fn a_request_that_is_not_a_produce_waits_for_every_produce_before_it() {
        let inner = Pipelined::new();
        let (mut client, connection) = connect(inner.clone(), TcpContext::default());

        client.write_all(&produce(1)).await.expect("request");
        client.write_all(&produce(2)).await.expect("request");
        client.write_all(&metadata(3)).await.expect("request");

        inner.until("admitted", 2).await;
        sleep(Duration::from_millis(100)).await;

        assert!(
            !inner.noted("started").contains(&3),
            "served behind produces still in flight",
        );

        inner.open();

        answered(&mut client, produce(1)).await;
        answered(&mut client, produce(2)).await;
        answered(&mut client, metadata(3)).await;

        assert_eq!(vec![0], inner.noted("outstanding when served"));

        drop(client);
        _ = connection.await;
    }

    /// The broker bounds what one connection holds, whatever the client's
    /// `max.in.flight` (#588): past [`TcpContext::PIPELINE_DEPTH`], the next request waits
    /// in the socket until an answer frees a slot.
    #[tokio::test(start_paused = true)]
    async fn a_connection_holds_at_most_the_pipeline_depth() {
        let inner = Pipelined::new();
        let (mut client, connection) = connect(inner.clone(), TcpContext::default());

        for id in 1..=7 {
            client.write_all(&produce(id)).await.expect("request");
        }

        inner.until("admitted", TcpContext::PIPELINE_DEPTH).await;
        sleep(Duration::from_millis(100)).await;

        assert_eq!(TcpContext::PIPELINE_DEPTH, inner.noted("admitted").len());

        inner.open();

        for id in 1..=7 {
            answered(&mut client, produce(id)).await;
        }

        drop(client);
        _ = connection.await;
    }

    /// A drain ends a connection with nothing owed (#361), and with produces
    /// pipelined that means every one already read is answered first (#588).
    #[tokio::test(start_paused = true)]
    async fn a_drain_answers_everything_already_read() {
        let drain = CancellationToken::new();
        let inner = Pipelined::new();
        let (mut client, connection) =
            connect(inner.clone(), TcpContext::default().drain(drain.clone()));

        for id in 1..=3 {
            client.write_all(&produce(id)).await.expect("request");
        }

        inner.until("admitted", 3).await;

        drain.cancel();
        inner.open();

        for id in 1..=3 {
            answered(&mut client, produce(id)).await;
        }

        let mut trailing = [0u8; 1];
        assert_eq!(
            0,
            client.read(&mut trailing).await.expect("a close"),
            "the drain closes the connection once nothing is owed",
        );

        assert!(connection.await.expect("joined").is_ok());
    }

    /// **An answer that fails does not cancel the requests behind it** (#588).
    ///
    /// The connection ends — an answer after the failed one would be matched
    /// to the wrong request — but a produce already admitted runs to the end.
    /// It may be the one flushing its window, and cancelling a segment PUT
    /// mid-flight is the ambiguous create the flush exists to resolve.
    #[tokio::test(start_paused = true)]
    async fn a_failed_answer_drains_the_requests_behind_it() {
        let inner = Pipelined {
            fails: Some(2),
            ..Pipelined::new()
        };
        let (mut client, connection) = connect(inner.clone(), TcpContext::default());

        for id in 1..=3 {
            client.write_all(&produce(id)).await.expect("request");
        }

        inner.until("admitted", 3).await;
        inner.open();

        answered(&mut client, produce(1)).await;

        let mut trailing = [0u8; 1];
        assert_eq!(
            0,
            client.read(&mut trailing).await.expect("a close"),
            "nothing is answered after the failure",
        );

        assert!(connection.await.expect("joined").is_err());
        assert_eq!(vec![3, 1], inner.noted("completed"));
    }

    /// **A request waiting on the barrier is not in flight** (#588, #362).
    ///
    /// `tansu_requests_in_flight` is what the scaler reads as work. A
    /// `Metadata` queued behind two produces held in their window is waiting,
    /// not being served, so the gauge says two — the produces — and not three.
    #[tokio::test(start_paused = true)]
    async fn a_request_waiting_on_the_barrier_is_not_in_flight() {
        let exporter = InMemoryMetricExporterBuilder::new()
            .with_temporality(Temporality::Cumulative)
            .build();

        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();

        global::set_meter_provider(provider.clone());

        let in_flight = || {
            provider.force_flush().expect("flush");

            exporter
                .get_finished_metrics()
                .expect("metrics")
                .iter()
                .flat_map(|resource| {
                    resource
                        .scope_metrics()
                        .flat_map(|scope| scope.metrics())
                        .filter(|metric| metric.name() == "tansu_requests_in_flight")
                        .filter_map(|metric| match metric.data() {
                            AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                                Some(sum.data_points().map(|point| point.value()).sum::<i64>())
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                })
                .next_back()
                .unwrap_or_default()
        };

        let inner = Pipelined::new();
        let (mut client, connection) = connect(inner.clone(), TcpContext::default());

        client.write_all(&produce(1)).await.expect("request");
        client.write_all(&produce(2)).await.expect("request");
        client.write_all(&metadata(3)).await.expect("request");

        inner.until("admitted", 2).await;
        sleep(Duration::from_millis(100)).await;

        assert_eq!(2, in_flight(), "the request behind the barrier is counted");

        inner.open();

        answered(&mut client, produce(1)).await;
        answered(&mut client, produce(2)).await;
        answered(&mut client, metadata(3)).await;

        assert_eq!(0, in_flight());

        drop(client);
        _ = connection.await;
    }

    /// **A depth of one is pipelining turned off** (#588): with the first
    /// produce held in its window, the second is not read, exactly as every
    /// request was served before.
    #[tokio::test(start_paused = true)]
    async fn a_depth_of_one_reads_nothing_ahead() {
        let inner = Pipelined::new();
        let (mut client, connection) =
            connect(inner.clone(), TcpContext::default().pipeline_depth(1));

        for id in 1..=3 {
            client.write_all(&produce(id)).await.expect("request");
        }

        inner.until("admitted", 1).await;
        sleep(Duration::from_millis(100)).await;

        assert_eq!(vec![1], inner.noted("started"), "read ahead at depth one");

        inner.open();

        for id in 1..=3 {
            answered(&mut client, produce(id)).await;
        }

        drop(client);
        _ = connection.await;
    }

    /// The depth is held to `1..=`[`TcpContext::PIPELINE_DEPTH`] (#588): zero
    /// would read nothing at all, and the ceiling bounds what one connection
    /// holds.
    #[test]
    fn the_depth_is_held_to_what_pipelining_can_use() {
        assert_eq!(
            TcpContext::PIPELINE_DEPTH,
            TcpContext::default().pipeline_depth
        );

        for (asked, held) in [(0, 1), (1, 1), (3, 3), (5, 5), (64, 5)] {
            assert_eq!(
                held,
                TcpContext::default().pipeline_depth(asked).pipeline_depth,
                "asked for {asked}"
            );
        }
    }
}
