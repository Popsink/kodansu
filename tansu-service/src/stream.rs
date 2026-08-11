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
    error::{self},
    fmt::Debug,
    io,
    marker::PhantomData,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use nanoid::nanoid;
use opentelemetry::KeyValue;
use rama::{Context, Layer, Service};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, warn};

use crate::{
    BYTES_RECEIVED, BYTES_SENT, Classify, Error, REQUEST_DURATION, REQUEST_SIZE,
    REQUESTS_IN_FLIGHT, RESPONSE_SIZE, Severity, THROTTLED_REQUESTS, THROTTLED_TIME, frame_length,
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
#[derive(Clone, Debug, Default)]
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
}

impl TcpContext {
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

        let mut buffer: Vec<u8> = vec![0u8; frame_length(size)];
        buffer[0..size.len()].copy_from_slice(&size[..]);
        _ = stream.read_exact(&mut buffer[4..]).await?;
        BYTES_RECEIVED.add(buffer.len() as u64, &[]);

        Ok(Bytes::from(buffer))
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

        let length = frame_length(size);

        // Reject a frame LARGER than the cap (#244). The comparison used to run the
        // other way round, so the guard fired on every frame *smaller* than the
        // limit and passed everything above it: the cap did not cap, and it refused
        // ordinary traffic. Nothing set `maximum_frame_size`, so it was inert — but
        // the first operator to arm it, which is the natural response to a
        // payload-size incident, would have taken every connection down instead.
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
        let mut request: Vec<u8> = vec![0u8; frame_length(size)];

        request[0..size.len()].copy_from_slice(&size[..]);

        _ = req
            .read_exact(&mut request[4..])
            .await
            .inspect_err(|err| error!(?err))?;
        BYTES_RECEIVED.add(request.len() as u64, &[]);

        Ok(Bytes::from(request))
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
        self.inner.serve(ctx, request).await.inspect(|response| {
            RESPONSE_SIZE.record(response.len() as u64, attributes);

            let elapsed_millis = self.elapsed_millis(request_start);

            REQUEST_DURATION.record(elapsed_millis, attributes);
        })
    }

    #[instrument(skip_all)]
    async fn write<W>(&self, req: &mut W, frame: Bytes) -> Result<(), S::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
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
        w.write_all(&frame).await?;
        BYTES_SENT.add(frame.len() as u64, &[]);
        w.flush().await.map_err(Into::into)
    }

    /// Everything a request owes its caller once its first four bytes have
    /// been read: the body, the answer, and the answer written back.
    ///
    /// Split from the wait above it so the drain has somewhere to interrupt
    /// that is not *inside* a request (#361).
    #[instrument(skip_all, fields(id = nanoid!()))]
    async fn answer<R>(
        &self,
        req: &mut R,
        size: [u8; 4],
        attributes: &[KeyValue],
        ctx: Context<TcpContext>,
    ) -> Result<(), S::Error>
    where
        R: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        // From here to the response written: the span over which this replica
        // owes the client something, which is what "in flight" has to mean for
        // the difference against `tansu_requests_parked` to be work (#362).
        let _in_flight = InFlight::enter();

        let request = self.read(req, size).await?;
        let response = self.process(attributes, ctx, request).await?;
        self.write(req, response).await
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
        mut req: Stream,
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

        // One cell for the life of the connection, and the same one every
        // request writes into: the quota layer above finds it in the context,
        // and this loop drains it below (#384).
        let throttle = Throttle::default();

        let ctx = {
            let mut ctx = ctx;
            _ = ctx.insert(throttle.clone());
            ctx
        };

        loop {
            // The only place a connection may be ended by the drain: between
            // requests, with nothing owed to the client. `biased` so the drain
            // wins over a frame that has already arrived — that request is
            // retried on a connection to a replica that is staying, where a
            // cut mid-request could not be (#361).
            let size = tokio::select! {
                biased;

                () = drain.cancelled() => {
                    debug!("closing an idle connection: this replica is stopping");
                    return Ok(());
                }

                size = self.wait(&mut req, maximum_frame_size) => size?,
            };

            self.answer(&mut req, size, &attributes[..], ctx.clone())
                .await?;

            // The response is already written, and this request is no longer in
            // flight — `answer` dropped its guard on the way out. What is left
            // is the mute, and it happens here, where no counter and no
            // histogram is watching (#384).
            //
            // Deliberately *not* wrapped in `Parked` either: parked is
            // subtracted from in flight to get the fleet's `busy`, and counting
            // a wait that was never counted as in flight would push that
            // expression negative. The wait shows up in `tansu_throttled_time`
            // and nowhere else.
            let delay = throttle.take();

            if !delay.is_zero() {
                debug!(?delay, "muting a connection over its quota");

                THROTTLED_REQUESTS.add(1, &attributes[..]);
                THROTTLED_TIME.add(delay.as_millis() as u64, &attributes[..]);

                // The drain still wins: a replica that has been asked to stop
                // must not hold a muted connection open for the throttle before
                // noticing (#361).
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
    use super::*;

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
        let length = frame_length(size);
        assert_eq!(100, length);

        let service: TcpBytesService<EchoBytes, ()> = TcpBytesService {
            inner: EchoBytes,
            _state: PhantomData,
        };

        // No cap configured: every frame passes, which is the behaviour of every
        // deployment today.
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
}
