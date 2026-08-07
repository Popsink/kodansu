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

pub mod group;

use crate::{
    CancelKind, METER, Result,
    coordinator::group::{
        Coordinator,
        administrator::Controller,
        forward::{DEFAULT_INTERNAL_PORT, GroupCoordinator, PEER_REFRESH_INTERVAL, PeerRegistry},
    },
    otel,
    service::services,
};
use console::Term;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use opentelemetry::metrics::Counter;
use rama::{Context, Service};
use rsasl::config::SASLConfig;
use rustls::ServerConfig;
use std::{
    future::{Future, pending},
    io,
    marker::PhantomData,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    str::FromStr,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tansu_sans_io::{ErrorCode, RootMessageMeta};
use tansu_service::{Classify as _, Peer, Severity};
use tansu_storage::{
    ArcDynStorage, BrokerRegistrationRequest, Storage, StorageContainer, TopicDefaults,
};
use tokio::{
    net::{TcpListener, TcpStream},
    signal::unix::{SignalKind, signal},
    task::JoinSet,
    time::{self, Instant, MissedTickBehavior, sleep},
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Level, debug, error, span, warn};
use url::Url;
use uuid::Uuid;

/// Maintenance passes that failed at the top, by reason.
///
/// A pass that fails before it starts — the topics-index refresh, the claim —
/// means **no retention and no compaction on this replica** until a later tick
/// succeeds. That was logged at `debug!`, and production runs at
/// `RUST_LOG=warn`, so a fleet could stop compacting entirely and the only
/// symptom would be growth (#284). A counter makes it alertable rather than
/// something a person has to notice in logs.
static MAINTENANCE_FAILURES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_maintenance_failures")
        .with_description("maintenance passes that failed at the top")
        .build()
});

/// Releases the maintenance in-flight flag on drop — on normal return, timeout
/// cancellation, or panic unwind — so a run can never leave it stuck `true` and
/// disable maintenance pod-wide until restart (#8, #131).
struct InFlightGuard(Arc<AtomicBool>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Run a maintenance `pass` while holding the in-flight guard, bounded by
/// `timeout`. On timeout the pass future is dropped (cancelled) and the guard is
/// released as this returns, so a *hung* run — e.g. one wedged in S3
/// `DeleteObjects` retry loops during a throttling storm — cannot hold the flag
/// forever and disable compaction/retention pod-wide until the pod is restarted
/// (#131). The overlap guard (#8) prevents *concurrent* runs; this is the escape
/// hatch for a *stuck* one. Cancellation is safe: maintenance is idempotent and
/// resumes on the next tick.
async fn run_bounded_maintenance<F>(in_flight: Arc<AtomicBool>, timeout: Duration, pass: F)
where
    F: Future<Output = ()>,
{
    let _guard = InFlightGuard(in_flight);
    if time::timeout(timeout, pass).await.is_err() {
        warn!(
            ?timeout,
            "maintenance run exceeded its time budget; cancelling so the next tick retries \
             (a run wedged in S3 retries would otherwise disable maintenance until restart)"
        );
    }
}

/// How often the coordinator's idle group state is evicted (#283).
///
/// Fixed, and independent of `maintenance_interval`: this is a bound on memory,
/// not a storage cadence, and it has to hold on a fleet that runs no storage
/// maintenance at all.
const GROUP_PRUNE_INTERVAL: Duration = Duration::from_mins(10);

/// How often this replica runs storage maintenance — retention and compaction.
///
/// `Never` is a first-class answer rather than a very large period. A serving
/// broker fleet that leaves both to a dedicated maintainer used to say so with
/// `maintenance_interval=8760h`, which is not the same statement: it schedules a
/// full maintenance pass on every replica at once, a year out, and relies on
/// pods being replaced before it lands. It also made "never" depend on where the
/// first tick happened to fall, which is exactly the coupling the wall-clock
/// anchoring below removed everywhere else.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Maintenance {
    Every(Duration),
    Never,
}

impl Default for Maintenance {
    fn default() -> Self {
        Self::Every(Duration::from_mins(10))
    }
}

/// Read a `maintenance_interval` query value: `never` disables maintenance, and
/// anything else is a humantime duration.
///
/// `None` means the value was not usable and the caller should fall back to the
/// default. A silent fallback is how `maintenance_interval=nevr` becomes a
/// ten-minute full maintenance pass on a fleet that asked for none, so the
/// caller warns; the parse only reports.
///
/// Zero is rejected rather than honoured: `interval_at` panics on a zero period,
/// so a `0s` here used to take the broker down at startup, and "as often as
/// possible" is not a cadence this loop can offer anyway.
fn parse_maintenance(value: &str) -> Option<Maintenance> {
    let value = value.trim();

    if value.eq_ignore_ascii_case("never") {
        return Some(Maintenance::Never);
    }

    value
        .parse::<humantime::Duration>()
        .ok()
        .map(Into::into)
        .filter(|period| *period > Duration::ZERO)
        .map(Maintenance::Every)
}

/// How long from `now` until the next wall-clock-aligned maintenance tick — the
/// next instant whose milliseconds since the Unix epoch divide evenly by
/// `period`.
///
/// The point is that the answer does not depend on `now` beyond where it falls
/// in the period, so two processes asking at different moments still land on the
/// same tick. That is what makes replicas share a schedule without coordinating,
/// and what keeps a pod that started 20s after its peers from trailing them
/// forever (see the call site).
///
/// Returns a delay in `(0, period]`: exactly on a boundary it waits a whole
/// period rather than firing immediately, so a restart loop cannot turn into a
/// maintenance loop.
fn until_next_aligned_tick(now: SystemTime, period: Duration) -> Duration {
    let period_ms = period.as_millis();
    if period_ms == 0 {
        return period;
    }

    let now_ms = now
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let remaining = period_ms - (now_ms % period_ms);

    // `remaining <= period_ms`, and a `Duration` this came from is expressible,
    // so the fallback is unreachable rather than a policy.
    u64::try_from(remaining).map_or(period, Duration::from_millis)
}

/// The next maintenance tick, or a future that never completes when maintenance
/// is disabled.
///
/// Same shape as `accept_on` below: the `select!` arm stays in the loop and is
/// inert, rather than the loop having two versions.
async fn maintenance_tick(interval: Option<&mut time::Interval>) {
    match interval {
        Some(interval) => _ = interval.tick().await,
        None => pending::<()>().await,
    }
}

#[derive(Clone, Debug)]
pub struct Broker<G, S> {
    node_id: i32,
    cluster_id: String,
    incarnation_id: Uuid,
    listener: Url,
    advertised_listener: Url,
    storage: S,
    groups: G,

    // The receive side of the forward-to-owner hop: when group forwarding is
    // enabled, `internal_listener` is bound alongside the public listener and
    // its connections are served with `internal_groups` — always the plain
    // local coordinator, never the forwarding wrapper — so a forwarded frame
    // is processed locally by construction and can never be forwarded again
    // (the structural one-hop guarantee). Both are `None` when forwarding is
    // off: no extra listener, bit-for-bit today's behaviour.
    internal_listener: Option<Url>,
    internal_groups: Option<G>,

    sasl_config: Option<Arc<SASLConfig>>,
    tls_server_config: Option<Arc<ServerConfig>>,
    silent: bool,
    maintenance: Maintenance,

    cancellation: CancellationToken,
}

impl<G, S> Broker<G, S>
where
    G: Coordinator,
    S: Storage + Clone + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: i32,
        cluster_id: &str,
        listener: Url,
        advertised_listener: Url,
        storage: S,
        groups: G,
        incarnation_id: Uuid,
    ) -> Self {
        Self {
            node_id,
            cluster_id: cluster_id.to_owned(),
            incarnation_id,
            listener,
            advertised_listener,
            storage,
            groups,

            internal_listener: None,
            internal_groups: None,

            sasl_config: None,
            tls_server_config: None,

            silent: false,

            maintenance: Maintenance::default(),

            cancellation: CancellationToken::new(),
        }
    }

    pub fn builder() -> PhantomBuilder {
        Builder::default()
    }

    pub async fn main(mut self, started: Instant) -> Result<ErrorCode> {
        {
            let root_meta = RootMessageMeta::messages();
            debug!(
                messages = root_meta
                    .requests()
                    .values()
                    .map(|meta| meta.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        let mut set = JoinSet::new();

        let mut interrupt_signal = signal(SignalKind::interrupt()).unwrap();
        debug!(?interrupt_signal);

        let mut terminate_signal = signal(SignalKind::terminate()).unwrap();
        debug!(?terminate_signal);

        let silent = self.silent;

        let token = self.cancellation.clone();

        _ = set.spawn(async move {
            self.serve(started)
                .await
                .inspect_err(|err| error!(?err))
                .unwrap();
        });

        let kind = tokio::select! {
            v = set.join_next() => {
                debug!(?v);
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

        if let Some(kind) = kind {
            token.cancel();

            let cleanup = async {
                while !set.is_empty() {
                    debug!(len = set.len());

                    _ = set.join_next().await;
                }
            };

            let patience = sleep(Duration::from(kind));

            tokio::select! {
                v = cleanup => {
                    debug!(?v)
                }

                _ = patience => {
                    debug!(aborting = set.len());
                    set.abort_all();

                    while !set.is_empty() {
                        _ = set.join_next().await;
                    }
                }
            }

            if !silent {
                let stdout = Term::stdout();

                if stdout.is_term() {
                    _ = stdout.clear_screen().ok();
                }
            }
        }

        Ok(ErrorCode::None)
    }

    pub async fn serve(&mut self, started: Instant) -> Result<()> {
        self.register().await?;
        self.listen(started).await
    }

    pub async fn register(&mut self) -> Result<()> {
        self.storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: self.node_id,
                cluster_id: self.cluster_id.clone(),
                incarnation_id: self.incarnation_id,
                rack: None,
            })
            .await
            .map_err(Into::into)
    }

    pub async fn listen(&self, started: Instant) -> Result<()> {
        debug!(%self.listener, %self.advertised_listener);

        let listener = bind(&self.listener, 9092)
            .await
            .inspect_err(|err| error!(?err, %self.advertised_listener))?;

        // The receive side of the forward-to-owner hop. Bound only when group
        // forwarding is enabled (both fields are populated by the builder in
        // that case alone); its connections are served with the plain local
        // coordinator — never the forwarding wrapper — so a forwarded frame is
        // processed locally by construction and cannot be forwarded again,
        // even when peer views are skewed mid-rollout (the structural one-hop
        // guarantee).
        let internal = match (&self.internal_listener, &self.internal_groups) {
            (Some(internal_listener), Some(internal_groups)) => Some((
                bind(internal_listener, DEFAULT_INTERNAL_PORT).await?,
                internal_groups.clone(),
            )),

            _ => None,
        };

        // Upper bound on a single maintenance run so a *hung* pass cannot hold the
        // in-flight guard forever and disable maintenance pod-wide until restart
        // (#131). A generous multiple of the interval, clamped so a short interval
        // still leaves a busy pass room to finish. A wedge is unbounded, so any
        // finite bound catches it; cancellation is safe because maintenance is
        // idempotent and resumes on the next tick.
        //
        // The floor is 30 min because the previous 10 min was cutting *legitimate*
        // runs: at a 2 min interval it bounded a run to 12 min, and a maintainer
        // draining a real backlog (~100k retention deletes plus thousands of
        // segments to merge on the busiest prefixes) was cancelled mid-drain,
        // every tick, so the hottest prefixes never converged (#140). A genuine
        // wedge is unbounded and is still caught 30 min in — far below the
        // multi-hour stall #131 was created to prevent.
        let maintenance_run_timeout = match self.maintenance {
            Maintenance::Every(period) => period
                .saturating_mul(6)
                .clamp(Duration::from_mins(30), Duration::from_mins(60)),

            // Never read — nothing spawns a run — but the bound is a `Duration`,
            // not an `Option<Duration>`, and the floor is the honest stand-in.
            Maintenance::Never => Duration::from_mins(30),
        };

        // Every replica ticks on the same wall-clock schedule, so the tick a
        // pod lands on does not depend on when that pod started.
        //
        // This used to offset the first tick by a node-derived fraction of the
        // period — a golden-ratio spread meant to keep N replicas off each
        // other's schedule, back when a shared startup meant they would all
        // scan+delete the same prefixes at once (#8). Two things happened to
        // it. `NODE_ID` is the constant 111 (`crate::NODE_ID`), so the spread
        // has always given every replica the *same* fraction and desynchronised
        // nothing; and #126 replaced the property it was protecting — the claim
        // now takes a per-prefix lease and skips anything a peer stamped within
        // `maintenance_recency`, so duplicated work is prevented by the claim,
        // not by the clock.
        //
        // What was left was an offset measured from each process's own start,
        // which turned pod start skew into tick skew. Because the claim stamps
        // every lease up front, a replica trailing the leader by less than the
        // recency window finds the whole universe already stamped and skips all
        // of it — every tick, for the life of the process. Measured in
        // production: of four maintainers, the two that started in the same
        // second shared the work and the two that started 20s later did nothing
        // for six hours but pay one lease GET per prefix per tick.
        //
        // Anchoring to the wall clock removes the skew rather than the sharing:
        // co-ticking replicas contend on the leases and split the universe
        // between them, which is what the two working replicas already did.
        //
        // `None` under `maintenance_interval=never`: the arm below stays in the
        // loop and never fires, rather than there being two versions of the loop.
        let mut interval = match self.maintenance {
            Maintenance::Every(period) => {
                let mut interval = time::interval_at(
                    Instant::now() + until_next_aligned_tick(SystemTime::now(), period),
                    period,
                );

                // Don't let a stalled/slow pass burst-fire catch-up ticks the
                // moment it returns; the overlap guard below already skips while
                // a run is in flight, and skipping missed ticks keeps maintenance
                // from compounding.
                interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

                Some(interval)
            }

            Maintenance::Never => None,
        };

        // Evicting idle coordinator group state (#283) is on its own clock, and
        // deliberately not on the maintenance one it used to share.
        //
        // It rode the maintenance tick because that was the only periodic thing
        // in this loop, which quietly tied a *memory* bound to a *storage*
        // cadence — so the serving fleet, the one whose group maps grow and the
        // one configured to leave storage maintenance to a dedicated maintainer,
        // was the fleet that never evicted anything. Production ran ten brokers
        // that way, and the eviction #283 added had never run on any of them.
        //
        // Start-relative rather than wall-clock-aligned, unlike maintenance
        // above: this walks in-memory maps, issues no request and contends with
        // no peer, so there is nothing for replicas to agree on and staggering
        // them is if anything the better default.
        let mut prune_interval =
            time::interval_at(Instant::now() + GROUP_PRUNE_INTERVAL, GROUP_PRUNE_INTERVAL);
        prune_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Guard against overlapping `maintain` runs: under S3 throttling a pass
        // can take longer than the interval, and spawning a second concurrent
        // run compounds the memory + S3 pressure (#8).
        let maintenance_in_flight = Arc::new(AtomicBool::new(false));

        let mut set = JoinSet::new();

        let m = MultiProgress::new();

        let spinner_style = ProgressStyle::with_template("{prefix:.bold.dim} {spinner} {msg}")
            .unwrap()
            .tick_chars("⠁⠂⠄⡀⢀⠠⠐");

        let ls = if self.silent {
            None
        } else {
            println!("ready in {}ms", started.elapsed().as_millis(),);

            let ls = m.add(ProgressBar::new_spinner());
            ls.set_style(spinner_style.clone());

            if let Ok(local_addr) = listener.local_addr() {
                ls.set_prefix(format!("[{local_addr:?}]"));
            }

            ls.set_message("listening for connection...");

            Some(ls)
        };

        // The client listener speaks TLS whenever a certificate is configured,
        // and this is the acceptor that makes it so. It used to be bound to
        // `_acceptor` and dropped on the spot, so `--cert`/`--key` were
        // accepted, the certificate was parsed, and every connection was still
        // plaintext — including the SASL PLAIN password and the SCRAM exchange
        // an operator had every reason to believe were encrypted (#358).
        //
        // Applied to the existing listener rather than bound as a second,
        // TLS-only one: there is a single advertised listener URL, so a second
        // port would be one no client is ever told about while the port they all
        // use stayed in the clear — which is the state being fixed.
        let acceptor = self.tls_server_config.clone().map(TlsAcceptor::from);

        let mut connections = 0;

        loop {
            connections += 1;

            if let Some(ref ls) = ls {
                ls.tick();
            }

            tokio::select! {
                Ok((stream, addr)) = listener.accept() => {
                    self.spawn_connection(
                        &mut set,
                        &m,
                        &spinner_style,
                        connections,
                        self.groups.clone(),
                        stream,
                        addr,
                        acceptor.clone(),
                    )?;

                    continue;
                }

                // A forwarded group request from a peer replica: served with
                // the plain local coordinator, so it is processed here and
                // never re-forwarded (one hop by construction). This arm is
                // inert — `accept_on(None)` never yields — unless forwarding
                // is enabled; both accept loops share this `select!`, so they
                // run concurrently and shut down together on cancellation.
                Ok((stream, addr)) = accept_on(internal.as_ref().map(|(listener, _)| listener)) => {
                    if let Some((_, internal_groups)) = &internal {
                        self.spawn_connection(
                            &mut set,
                            &m,
                            &spinner_style,
                            connections,
                            internal_groups.clone(),
                            stream,
                            addr,
                            // Plaintext, deliberately, even when the client
                            // listener is TLS. Nothing a client authenticates
                            // with crosses this listener: it carries forwarded
                            // group frames between replicas of one cluster, on
                            // a port reachable only through the headless
                            // Service, and #360 removes the listener outright.
                            // Encrypting it needs a peer trust store plus a
                            // client config on the forward side — a real cost,
                            // spent on a listener with a deletion date.
                            // Revisit only if #360 is abandoned.
                            None,
                        )?;
                    }

                    continue;
                }

                _ = prune_interval.tick() => {
                    // Evict the coordinator's per-group state for groups this
                    // replica has stopped serving (#283). Its own arm, on its own
                    // interval: it walks in-memory maps and issues no request, so
                    // it must not be skipped just because a storage pass is still
                    // draining — that is precisely the loaded pod whose memory
                    // this bounds — nor because this fleet runs no storage
                    // maintenance, which is how it came to never run at all.
                    //
                    // `groups` alone covers `internal_groups` too: both are built
                    // from one `Controller` whose clone shares the maps, and the
                    // internal coordinator is always the plain local one, so it
                    // holds nothing of its own to sweep.
                    let evicted = self.groups.prune();
                    if evicted > 0 {
                        debug!(evicted, "pruned idle coordinator group state");
                    }

                    continue;
                }

                _ = maintenance_tick(interval.as_mut()) => {
                    // Skip this tick if the previous maintenance run is still in
                    // flight, rather than spawning a second concurrent run (#8).
                    if maintenance_in_flight
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        warn!("skipping maintenance tick: previous run still in flight");
                    } else {
                        let storage = self.storage.clone();
                        let in_flight = maintenance_in_flight.clone();

                        let handle = set.spawn(async move {
                            let span = span!(Level::DEBUG, "maintenance");

                            // Bounded so a hung run releases the guard and the next
                            // tick can retry, rather than wedging maintenance
                            // pod-wide until restart (#131).
                            run_bounded_maintenance(in_flight, maintenance_run_timeout, async move {
                                // A failure here is not cosmetic: it means this
                                // replica did no retention and no compaction this
                                // tick. `warn!` rather than `debug!` so it is
                                // visible at the level production runs at, plus a
                                // counter so it does not depend on someone reading
                                // logs (#284).
                                _ = storage
                                    .maintain(SystemTime::now())
                                    .await
                                    .inspect(|maintain| debug!(?maintain))
                                    .inspect_err(|err| {
                                        // Unlabelled deliberately: `tansu_storage::Error`
                                        // has no bounded name helper, and inventing a
                                        // taxonomy here to fill a label would be a
                                        // second one to keep in step. The counter
                                        // carries the alert, the `warn!` below carries
                                        // the detail.
                                        MAINTENANCE_FAILURES.add(1, &[]);
                                        warn!(
                                            ?err,
                                            "maintenance pass failed: no retention or compaction \
                                             on this replica this tick"
                                        )
                                    })
                                    .ok();
                            }).instrument(span).await
                        });

                        debug!(?handle);
                    }
                }

                v = set.join_next(), if !set.is_empty() => {
                    debug!(?v);
                }

                message = self.cancellation.cancelled() => {
                    debug!(?message);
                    break;
                }
            }
        }

        while !set.is_empty() {
            debug!(len = set.len());

            _ = set.join_next().await;
        }

        Ok(())
    }

    /// Serve one accepted connection on `set`, with `groups` as the group
    /// coordinator for its service stack.
    ///
    /// Shared by both accept arms of [`Broker::listen`]: the public listener
    /// serves with the broker's own coordinator (which may forward to the
    /// group's owner), the internal listener with the plain local one. The
    /// stacks are otherwise identical — same storage, SASL and topic
    /// defaults.
    ///
    /// `acceptor` decides the transport: `Some` wraps the stream in TLS before
    /// the Kafka stack sees a byte, `None` serves it as it arrived. The two
    /// accept arms pass different values (#358), which is the whole of the
    /// per-listener transport policy.
    #[allow(clippy::too_many_arguments)]
    fn spawn_connection(
        &self,
        set: &mut JoinSet<()>,
        m: &MultiProgress,
        spinner_style: &ProgressStyle,
        connections: i32,
        groups: G,
        stream: TcpStream,
        addr: SocketAddr,
        acceptor: Option<TlsAcceptor>,
    ) -> Result<()> {
        let mut c = Context::default();

        // What `TcpContextService` used to read off the stream with
        // `peer_addr()` — the call that made the service stack `TcpStream`-only
        // and so kept TLS out of it. The accept loop knows the address already.
        _ = c.insert(Peer(addr));

        let pb = if self.silent {
            None
        } else {
            let pb = m.add(ProgressBar::new_spinner());
            pb.set_style(spinner_style.clone());
            pb.set_prefix(format!("[{connections}/{addr:?}]"));
            pb.set_message("connected");
            pb.tick();

            _ = c.insert(pb.clone());
            Some(pb)
        };

        stream.set_nodelay(true)?;

        let service = services(
            self.cluster_id.as_str(),
            groups,
            self.storage.clone(),
            self.sasl_config.clone(),
        )?;

        let handle = set.spawn(async move {
            match acceptor {
                // The handshake runs here, in the connection's own task, and not
                // in `listen`'s accept loop: a peer that opens a socket and then
                // says nothing must cost itself, not everything queued behind
                // `accept`.
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(stream) => report_connection(service.serve(c, stream).await),

                    // Where a plaintext client on a TLS listener arrives, and
                    // the one event #358's silence left no trace of at all.
                    // Reported with the address because the handshake failed, so
                    // no per-connection span was ever entered to carry it.
                    Err(err) => warn!(%addr, ?err, "TLS handshake failed; refusing the connection"),
                },

                None => report_connection(service.serve(c, stream).await),
            }

            if let Some(ref pb) = pb {
                pb.finish_and_clear();
            }
        });

        debug!(?handle);

        Ok(())
    }
}

/// Report how one connection ended.
///
/// Two separate facts, and this used to conflate them into one `error!` (#289).
///
/// *Which* error occurred is classified by the error itself, so a deliberately
/// retriable answer does not fill the error plane on every rollout.
///
/// *That the connection is being abandoned with no response written* is
/// reported whatever the error was, because it is the part the client
/// experiences and nothing else records it. From the caller's side an abandoned
/// connection is indistinguishable from the peer closing mid-frame — which is
/// exactly the `early eof` shape of #300, and this line is the only evidence
/// that a request-level error is what caused it. Downgrading the error code
/// must not take that with it.
///
/// #300 removed the largest population that reached here: `Error::Api` on a
/// group API, which the coordinator means as a protocol *answer*
/// (`NOT_COORDINATOR` from a failed forward, #243) and which was costing a
/// connection instead of being written back. `broker::group::answer` converts
/// those now, so what still arrives here is an error the broker has no answer
/// for — which is what makes this line worth reading.
///
/// A failed TLS handshake never reaches here: it is reported at the accept site,
/// because no request was read and there is nothing further to say about the
/// connection (#358).
fn report_connection(served: Result<()>) {
    match served {
        Err(error) => match error.severity() {
            Severity::Expected => debug!(?error),

            Severity::Unexpected => {
                warn!(?error, "connection ended, no response written")
            }

            Severity::Failure => {
                error!(?error, "connection ended, no response written")
            }
        },

        Ok(response) => {
            debug!(?response)
        }
    }
}

/// Bind a TCP listener on `url`, falling back to the unspecified address
/// and/or `default_port` when the URL leaves them out.
async fn bind(url: &Url, default_port: u16) -> Result<TcpListener> {
    TcpListener::bind(url.host().map_or_else(
        || {
            SocketAddr::from((
                IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                url.port().unwrap_or(default_port),
            ))
        },
        |host| {
            let port = url.port().unwrap_or(default_port);
            debug!(?host, port);

            match host {
                url::Host::Domain(domain) => SocketAddr::from_str(&format!("{domain}:{port}"))
                    .unwrap_or(SocketAddr::from((IpAddr::V6(Ipv6Addr::UNSPECIFIED), port))),
                url::Host::Ipv4(ipv4_addr) => SocketAddr::from((IpAddr::V4(ipv4_addr), port)),
                url::Host::Ipv6(ipv6_addr) => SocketAddr::from((IpAddr::V6(ipv6_addr), port)),
            }
        },
    ))
    .await
    .inspect(|listener| debug!(%url, listener = ?listener.local_addr().ok()))
    .inspect_err(|err| error!(?err, %url))
    .map_err(Into::into)
}

/// Accept on `listener` when one is bound; an absent listener never yields,
/// so the `select!` arm it feeds is simply inert (used for the internal
/// listener, which only exists when group forwarding is enabled).
async fn accept_on(listener: Option<&TcpListener>) -> io::Result<(TcpStream, SocketAddr)> {
    match listener {
        Some(listener) => listener.accept().await,
        None => pending().await,
    }
}

#[derive(Clone, Debug, Default)]
pub struct Builder<N, C, I, A, S, L> {
    node_id: N,
    cluster_id: C,
    incarnation_id: I,
    advertised_listener: A,
    storage: S,
    listener: L,
    otlp_endpoint_url: Option<Url>,
    authentication: bool,
    tls_server_config: Option<ServerConfig>,
    silent: bool,
    maintenance: Maintenance,
    topic_defaults: TopicDefaults,
    group_forwarding: bool,
    group_forward_peer_dns: Option<String>,
    internal_listener_url: Option<Url>,
    pod_ip: Option<IpAddr>,

    cancellation: CancellationToken,
}

type PhantomBuilder = Builder<
    PhantomData<i32>,
    PhantomData<String>,
    PhantomData<Uuid>,
    PhantomData<Url>,
    PhantomData<Url>,
    PhantomData<Url>,
>;

impl<N, C, I, A, S, L> Builder<N, C, I, A, S, L> {
    const MAINTENANCE_INTERVAL: &str = "maintenance_interval";

    pub fn node_id(self, node_id: i32) -> Builder<i32, C, I, A, S, L> {
        Builder {
            node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance: self.maintenance,
            topic_defaults: self.topic_defaults,
            group_forwarding: self.group_forwarding,
            group_forward_peer_dns: self.group_forward_peer_dns,
            internal_listener_url: self.internal_listener_url,
            pod_ip: self.pod_ip,

            cancellation: self.cancellation,
        }
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> Builder<N, String, I, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: cluster_id.into(),
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance: self.maintenance,
            topic_defaults: self.topic_defaults,
            group_forwarding: self.group_forwarding,
            group_forward_peer_dns: self.group_forward_peer_dns,
            internal_listener_url: self.internal_listener_url,
            pod_ip: self.pod_ip,

            cancellation: self.cancellation,
        }
    }

    pub fn incarnation_id(self, incarnation_id: impl Into<Uuid>) -> Builder<N, C, Uuid, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: incarnation_id.into(),
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance: self.maintenance,
            topic_defaults: self.topic_defaults,
            group_forwarding: self.group_forwarding,
            group_forward_peer_dns: self.group_forward_peer_dns,
            internal_listener_url: self.internal_listener_url,
            pod_ip: self.pod_ip,

            cancellation: self.cancellation,
        }
    }

    pub fn advertised_listener(
        self,
        advertised_listener: impl Into<Url>,
    ) -> Builder<N, C, I, Url, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: advertised_listener.into(),
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance: self.maintenance,
            topic_defaults: self.topic_defaults,
            group_forwarding: self.group_forwarding,
            group_forward_peer_dns: self.group_forward_peer_dns,
            internal_listener_url: self.internal_listener_url,
            pod_ip: self.pod_ip,

            cancellation: self.cancellation,
        }
    }

    pub fn storage(self, mut storage: Url) -> Builder<N, C, I, A, Url, L> {
        // A value that does not parse is worth a `warn!` rather than a silent
        // fall back to the ten-minute default: on a serving fleet that asked for
        // `never`, a typo is the difference between no maintenance and a full
        // pass on every replica, and nothing downstream would say so.
        let maintenance = storage
            .query_pairs()
            .find(|(k, _)| k == Self::MAINTENANCE_INTERVAL)
            .map_or_else(Maintenance::default, |(_, v)| {
                parse_maintenance(&v).unwrap_or_else(|| {
                    let default = Maintenance::default();
                    warn!(
                        value = %v,
                        ?default,
                        "unusable maintenance_interval, using the default \
                         (a duration such as `10m`, or `never`)"
                    );
                    default
                })
            });

        let pairs = storage
            .query_pairs()
            .filter_map(|(k, v)| {
                if k == Self::MAINTENANCE_INTERVAL {
                    None
                } else {
                    Some((k.to_string(), v.to_string()))
                }
            })
            .collect::<Vec<_>>();

        if pairs.is_empty() {
            storage.set_query(None);
        } else {
            _ = storage.query_pairs_mut().clear().extend_pairs(pairs);
        }

        debug!(?maintenance, %storage);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance,
            topic_defaults: self.topic_defaults,
            group_forwarding: self.group_forwarding,
            group_forward_peer_dns: self.group_forward_peer_dns,
            internal_listener_url: self.internal_listener_url,
            pod_ip: self.pod_ip,

            cancellation: self.cancellation,
        }
    }

    pub fn listener(self, listener: Url) -> Builder<N, C, I, A, S, Url> {
        debug!(%listener);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            maintenance: self.maintenance,
            topic_defaults: self.topic_defaults,
            group_forwarding: self.group_forwarding,
            group_forward_peer_dns: self.group_forward_peer_dns,
            internal_listener_url: self.internal_listener_url,
            pod_ip: self.pod_ip,

            cancellation: self.cancellation,
        }
    }

    pub fn otlp_endpoint_url(self, otlp_endpoint_url: Option<Url>) -> Self {
        Self {
            otlp_endpoint_url,
            ..self
        }
    }

    pub fn authentication(self, authentication: bool) -> Self {
        Self {
            authentication,
            ..self
        }
    }

    pub fn tls_server_config(self, tls_server_config: Option<ServerConfig>) -> Self {
        Self {
            tls_server_config,
            ..self
        }
    }
    pub fn silent(self, silent: bool) -> Self {
        Self { silent, ..self }
    }

    pub fn topic_defaults(self, topic_defaults: TopicDefaults) -> Self {
        Self {
            topic_defaults,
            ..self
        }
    }

    /// Enable forward-to-owner group coordination (default: off, pure-local).
    pub fn group_forwarding(self, group_forwarding: bool) -> Self {
        Self {
            group_forwarding,
            ..self
        }
    }

    /// Headless-Service hostname whose A/AAAA records are the peer replicas
    /// eligible to own consumer groups.
    pub fn group_forward_peer_dns(self, group_forward_peer_dns: Option<String>) -> Self {
        Self {
            group_forward_peer_dns,
            ..self
        }
    }

    /// URL of the internal (broker-to-broker) listener; forwarded group
    /// requests are sent to each owner at this URL's port.
    pub fn internal_listener_url(self, internal_listener_url: Option<Url>) -> Self {
        Self {
            internal_listener_url,
            ..self
        }
    }

    /// This replica's own address (in Kubernetes, the pod IP from the
    /// Downward API), used to recognise itself in the peer set.
    pub fn pod_ip(self, pod_ip: Option<IpAddr>) -> Self {
        Self { pod_ip, ..self }
    }
}

impl Builder<i32, String, Uuid, Url, Url, Url> {
    pub async fn build(self) -> Result<Broker<GroupCoordinator<ArcDynStorage>, ArcDynStorage>> {
        if let Some(otlp_endpoint_url) = self
            .otlp_endpoint_url
            .clone()
            .inspect(|otlp_endpoint_url| debug!(%otlp_endpoint_url))
        {
            otel::metric_exporter(otlp_endpoint_url)?;
        }

        let storage = StorageContainer::builder()
            .cluster_id(self.cluster_id.clone())
            .node_id(self.node_id)
            .advertised_listener(self.advertised_listener.clone())
            .storage(self.storage.clone())
            .topic_defaults(self.topic_defaults.clone())
            .silent(self.silent)
            .build()
            .await
            .map(|storage| Arc::new(storage) as ArcDynStorage)?;

        let controller = Controller::with_storage(storage.clone())?;

        // Local (pure-local group coordination, bit-for-bit today's
        // behaviour) unless forwarding is explicitly enabled AND the peer
        // discovery inputs are both present; anything less degrades to
        // Local, never to a broken half-configuration. Only the
        // fully-configured forwarding arm produces the internal listener —
        // the receive side of the forward hop — and its coordinator is
        // always the plain local one, so a forwarded frame is served
        // locally by construction (one hop, no forwarding loop possible).
        let (groups, internal_listener, internal_groups) = match (
            self.group_forwarding,
            self.group_forward_peer_dns.as_deref(),
            self.pod_ip,
        ) {
            (true, Some(peer_dns), Some(pod_ip)) => {
                let internal_listener = match self.internal_listener_url {
                    Some(internal_listener_url) => internal_listener_url,
                    None => Url::parse(&format!("tcp://0.0.0.0:{DEFAULT_INTERNAL_PORT}"))?,
                };

                let internal_port = internal_listener.port().unwrap_or(DEFAULT_INTERNAL_PORT);

                debug!(%peer_dns, %pod_ip, internal_port, "group forwarding enabled");

                let registry = Arc::new(PeerRegistry::new(pod_ip, peer_dns, PEER_REFRESH_INTERVAL));
                _ = registry.clone().spawn_refresh();

                // Both coordinators are built from the same `Controller`
                // (whose clone shares the in-memory group cache), so
                // owner-local calls arriving on the public listener and
                // forwarded calls arriving on the internal listener see the
                // same group state.
                (
                    GroupCoordinator::forwarding(controller.clone(), registry, internal_port),
                    Some(internal_listener),
                    Some(GroupCoordinator::local(controller)),
                )
            }

            (true, peer_dns, pod_ip) => {
                warn!(
                    ?peer_dns,
                    ?pod_ip,
                    "group forwarding enabled without peer dns and pod ip; \
                     using local group coordination"
                );

                (GroupCoordinator::local(controller), None, None)
            }

            (false, _, _) => (GroupCoordinator::local(controller), None, None),
        };

        let sasl_config = if self.authentication {
            tansu_auth::configuration(storage.clone()).map(Some)?
        } else {
            None
        };

        Ok(Broker {
            node_id: self.node_id,
            cluster_id: self.cluster_id.clone(),
            incarnation_id: self.incarnation_id,
            listener: self.listener,
            advertised_listener: self.advertised_listener,
            storage,
            groups,
            internal_listener,
            internal_groups,
            sasl_config,
            tls_server_config: self.tls_server_config.map(Arc::new),

            silent: self.silent,
            maintenance: self.maintenance,
            cancellation: self.cancellation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;

    // A run that never completes (modelling a pass wedged in S3 retry loops)
    // must be cancelled at the timeout and release the in-flight guard, so the
    // next tick can retry — not stay `true` and disable maintenance pod-wide
    // until restart (#131). Paused time makes `timeout` fire in virtual time.
    #[tokio::test(start_paused = true)]
    async fn bounded_maintenance_cancels_a_hung_pass_and_releases_the_guard() {
        let in_flight = Arc::new(AtomicBool::new(true));

        run_bounded_maintenance(in_flight.clone(), Duration::from_secs(60), pending::<()>()).await;

        assert!(
            !in_flight.load(Ordering::Acquire),
            "guard must be released after a timed-out run"
        );
    }

    // The normal path: the pass runs to completion and the guard is released.
    #[tokio::test]
    async fn bounded_maintenance_runs_the_pass_and_releases_the_guard() {
        let in_flight = Arc::new(AtomicBool::new(true));
        let ran = Arc::new(AtomicBool::new(false));

        let ran_in_pass = ran.clone();
        run_bounded_maintenance(in_flight.clone(), Duration::from_secs(60), async move {
            ran_in_pass.store(true, Ordering::Release);
        })
        .await;

        assert!(ran.load(Ordering::Acquire), "the pass must have run");
        assert!(
            !in_flight.load(Ordering::Acquire),
            "guard must be released after the run completes"
        );
    }

    mod maintenance_schedule {
        use super::*;

        #[test]
        fn never_is_a_schedule_rather_than_a_very_long_period() {
            for value in ["never", "NEVER", "  never  "] {
                assert_eq!(
                    parse_maintenance(value),
                    Some(Maintenance::Never),
                    "{value}"
                );
            }
        }

        #[test]
        fn a_duration_is_a_cadence() {
            assert_eq!(
                parse_maintenance("10m"),
                Some(Maintenance::Every(Duration::from_mins(10)))
            );
            assert_eq!(
                parse_maintenance("8760h"),
                Some(Maintenance::Every(Duration::from_hours(8760)))
            );
        }

        // The value that used to panic the broker at startup: `interval_at`
        // requires a non-zero period, and "as often as possible" is not a cadence
        // this loop can offer.
        #[test]
        fn a_zero_interval_is_rejected_rather_than_scheduled() {
            assert_eq!(parse_maintenance("0s"), None);
            assert_eq!(parse_maintenance("0ms"), None);
        }

        // Reported, not guessed at: the caller warns and uses the default, which
        // is the behaviour a typo on a fleet that asked for `never` needs.
        #[test]
        fn an_unusable_value_is_not_a_schedule() {
            for value in ["nevr", "", "-1", "10 parsecs"] {
                assert_eq!(parse_maintenance(value), None, "{value}");
            }
        }

        // The storage URL is where the setting comes from, and the key is
        // consumed here — the storage engine must not see it.
        #[test]
        fn the_storage_url_carries_the_schedule_and_loses_the_key() {
            let built = PhantomBuilder::default().storage(
                Url::parse("s3://bucket/?maintenance_interval=never&coalesce_linger=1s").unwrap(),
            );

            assert_eq!(built.maintenance, Maintenance::Never);
            assert_eq!(built.storage.as_str(), "s3://bucket/?coalesce_linger=1s");
        }

        #[test]
        fn an_absent_key_leaves_the_default() {
            let built = PhantomBuilder::default().storage(Url::parse("s3://bucket/").unwrap());

            assert_eq!(built.maintenance, Maintenance::default());
        }

        // `Never` has to be inert for the whole life of the process, not merely
        // slow: the `select!` arm stays in the loop and this is what keeps it
        // from ever being taken.
        #[tokio::test(start_paused = true)]
        async fn a_disabled_schedule_never_ticks() {
            assert!(
                time::timeout(Duration::from_hours(24 * 365), maintenance_tick(None))
                    .await
                    .is_err()
            );
        }

        #[tokio::test(start_paused = true)]
        async fn an_enabled_schedule_ticks() {
            let mut interval = time::interval_at(
                Instant::now() + Duration::from_mins(10),
                Duration::from_mins(10),
            );

            assert!(
                time::timeout(
                    Duration::from_mins(11),
                    maintenance_tick(Some(&mut interval))
                )
                .await
                .is_ok()
            );
        }
    }

    mod aligned_tick {
        use super::*;

        fn at(unix_ms: u64) -> SystemTime {
            UNIX_EPOCH + Duration::from_millis(unix_ms)
        }

        // The property the fleet depends on: two pods that started at different
        // moments must land on the same tick. This is the regression — the old
        // schedule measured its first tick from each process's own start, so a
        // replica launched 20s after its peers stayed 20s behind them forever
        // and the claim's recency skip left it with nothing to do, every tick.
        #[test]
        fn replicas_that_start_apart_still_land_on_the_same_tick() {
            let period = Duration::from_mins(10);

            let leader_started = 1_000_000_000_000;
            let trailer_started = leader_started + 20_000;

            assert_eq!(
                at(leader_started) + until_next_aligned_tick(at(leader_started), period),
                at(trailer_started) + until_next_aligned_tick(at(trailer_started), period),
            );
        }

        // Two pods a *whole period* apart land on different ticks, of course —
        // but still on the same grid, which is the same property stated where it
        // is easy to get wrong.
        #[test]
        fn every_tick_falls_on_the_period_grid() {
            let period = Duration::from_mins(10);

            for start in [0, 1, 199_999, 1_000_000_000_123, 1_755_000_000_000u64] {
                let tick =
                    Duration::from_millis(start) + until_next_aligned_tick(at(start), period);

                assert_eq!(
                    tick.as_millis() % period.as_millis(),
                    0,
                    "tick for start={start} is off the grid"
                );
            }
        }

        // Exactly on a boundary, wait a whole period rather than firing
        // immediately: a crash-looping pod would otherwise run a full
        // maintenance pass on every start.
        #[test]
        fn a_start_on_the_boundary_waits_a_whole_period() {
            let period = Duration::from_mins(10);

            assert_eq!(until_next_aligned_tick(at(600_000), period), period);
        }

        // The "compaction disabled on serving brokers" configuration is a period
        // of a year, which must not overflow the millisecond arithmetic or
        // saturate to something short.
        #[test]
        fn a_year_long_period_is_expressible() {
            let period = Duration::from_hours(8760);
            let delay = until_next_aligned_tick(at(1_755_000_000_000), period);

            assert!(delay > Duration::ZERO && delay <= period, "{delay:?}");
        }

        // Degenerate rather than reachable — no configuration produces it — but
        // the modulo below it would divide by zero.
        #[test]
        fn a_zero_period_does_not_divide_by_zero() {
            assert_eq!(
                until_next_aligned_tick(at(1_755_000_000_000), Duration::ZERO),
                Duration::ZERO
            );
        }
    }

    /// Building a broker needs the in-memory object store, which — like the
    /// existing `tests/cg*.rs` in-memory suites — is only built
    /// workspace-wide with `--all-features` (the `dynostore` feature of
    /// `tansu-storage` is enabled through the `tansu` crate).
    #[cfg(feature = "dynostore")]
    mod group_coordination {
        use super::*;
        use crate::coordinator::group::forward::{Forward, FrameForwarder};
        use bytes::Bytes;
        use tansu_sans_io::{
            ApiKey as _, Body, JoinGroupRequest, join_group_request::JoinGroupRequestProtocol,
        };

        fn builder() -> Builder<i32, String, Uuid, Url, Url, Url> {
            Broker::<GroupCoordinator<ArcDynStorage>, ArcDynStorage>::builder()
                .node_id(111)
                .cluster_id(Uuid::now_v7().to_string())
                .incarnation_id(Uuid::now_v7())
                .advertised_listener(Url::parse("tcp://localhost:9092").unwrap())
                .storage(Url::parse("memory://").unwrap())
                .listener(Url::parse("tcp://localhost:9092").unwrap())
                .silent(true)
        }

        // The default is pure-local group coordination: without the
        // forwarding flag the broker is built with `GroupCoordinator::Local`,
        // bit-for-bit today's behaviour — and no internal listener.
        #[tokio::test]
        async fn build_defaults_to_local_group_coordination() -> Result<()> {
            let broker = builder().build().await?;

            assert!(matches!(broker.groups, GroupCoordinator::Local(_)));
            assert!(broker.internal_listener.is_none());
            assert!(broker.internal_groups.is_none());

            Ok(())
        }

        // Forwarding requires the flag AND both discovery inputs. Only then
        // is the internal listener constructed, and its coordinator is the
        // plain local one — never the forwarding wrapper — so a forwarded
        // frame is served locally by construction (the one-hop guarantee).
        #[tokio::test]
        async fn build_with_forwarding_flag_and_discovery_is_forwarding() -> Result<()> {
            let broker = builder()
                .group_forwarding(true)
                .group_forward_peer_dns(Some("peers.example.invalid".into()))
                .pod_ip(Some(IpAddr::from_str("10.0.0.1")?))
                .build()
                .await?;

            assert!(matches!(broker.groups, GroupCoordinator::Forwarding(_)));
            assert!(matches!(
                broker.internal_listener.as_ref().and_then(Url::port),
                Some(DEFAULT_INTERNAL_PORT)
            ));
            assert!(matches!(
                broker.internal_groups,
                Some(GroupCoordinator::Local(_))
            ));

            Ok(())
        }

        // A half-configuration (flag set, discovery inputs missing) degrades
        // to Local rather than to a broken forwarding setup — and binds no
        // internal listener.
        #[tokio::test]
        async fn build_with_forwarding_flag_but_no_discovery_is_local() -> Result<()> {
            let broker = builder().group_forwarding(true).build().await?;

            assert!(matches!(broker.groups, GroupCoordinator::Local(_)));
            assert!(broker.internal_listener.is_none());
            assert!(broker.internal_groups.is_none());

            Ok(())
        }

        fn free_port() -> Result<u16> {
            std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .and_then(|listener| listener.local_addr())
                .map(|local_addr| local_addr.port())
                .map_err(Into::into)
        }

        // The receive side of the forward hop, end to end: with forwarding
        // enabled the broker binds the internal listener, and a frame-level
        // JoinGroup delivered to it — by the production `FrameForwarder`,
        // exactly what a peer replica sends to the group's owner — is served
        // locally. The KIP-394 member-id-required response proves both that
        // the local `Controller` processed the join and that the member's
        // own client id survived the hop (the member id is derived from it).
        #[tokio::test]
        async fn internal_listener_serves_a_forwarded_group_request_locally() -> Result<()> {
            let main_port = free_port()?;
            let internal_port = free_port()?;

            let mut broker = builder()
                .listener(Url::parse(&format!("tcp://127.0.0.1:{main_port}"))?)
                .group_forwarding(true)
                // resolution failure leaves the peer set empty; discovery is
                // not what this test exercises — the internal listener is
                // dialled directly below.
                .group_forward_peer_dns(Some("peers.example.invalid".into()))
                .pod_ip(Some(IpAddr::from_str("127.0.0.1")?))
                .internal_listener_url(Some(Url::parse(&format!(
                    "tcp://127.0.0.1:{internal_port}"
                ))?))
                .build()
                .await?;

            assert!(matches!(broker.groups, GroupCoordinator::Forwarding(_)));

            let serving = tokio::spawn(async move { broker.serve(Instant::now()).await });

            let forwarder = FrameForwarder::new(internal_port);
            let owner = IpAddr::from_str("127.0.0.1")?;

            let request = JoinGroupRequest::default()
                .group_id("pr3-internal-listener".into())
                .session_timeout_ms(45_000)
                .rebalance_timeout_ms(Some(60_000))
                .member_id("".into())
                .protocol_type("consumer".into())
                .protocols(Some(vec![
                    JoinGroupRequestProtocol::default()
                        .name("range".into())
                        .metadata(Bytes::from_static(b"pr3_meta")),
                ]));

            // retry until the spawned broker is accepting on the internal
            // listener.
            let mut response = None;

            for _ in 0..50 {
                match forwarder
                    .call(
                        owner,
                        JoinGroupRequest::KEY,
                        Some("pr3-member"),
                        request.clone().into(),
                    )
                    .await
                {
                    Ok(body) => {
                        response = Some(body);
                        break;
                    }

                    Err(_) => sleep(Duration::from_millis(100)).await,
                }
            }

            serving.abort();

            match response {
                Some(Body::JoinGroupResponse(join)) => {
                    assert_eq!(
                        ErrorCode::MemberIdRequired,
                        ErrorCode::try_from(join.error_code)?
                    );

                    assert!(
                        join.member_id.starts_with("pr3-member-"),
                        "member id must be derived from the forwarded client id: {}",
                        join.member_id
                    );

                    Ok(())
                }

                otherwise => panic!("expected a join group response, got: {otherwise:?}"),
            }
        }
    }
}
