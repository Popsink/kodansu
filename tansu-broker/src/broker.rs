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

pub mod group;

use std::future::{Future, pending};
use std::io;
use std::marker::PhantomData;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::str::FromStr as _;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use console::Term;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use opentelemetry::metrics::{Counter, Histogram};
use rama::{Context, Service as _};
use rsasl::config::SASLConfig;
use rustls::ServerConfig;
use tansu_sans_io::{ErrorCode, RootMessageMeta};
use tansu_service::{Classify as _, Keepalive, Peer, Severity, TcpContext};
use tansu_storage::{ArcDynStorage, Authorizer, BrokerRegistrationRequest, QuotaEnforcer};
use tansu_storage::{QuotaLimits, Storage, StorageContainer, TopicDefaults};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinSet;
use tokio::time::{self, Instant, MissedTickBehavior, sleep};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, debug, error, warn};
use url::Url;
use uuid::Uuid;

use crate::coordinator::group::administrator::Controller;
use crate::coordinator::group::{ConsumerGroupConfig, Coordinator};
use crate::service::services;
use crate::{CancelKind, METER, Result, otel};

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

/// Wall clock of one maintenance pass (#399), against which
/// `maintenance_interval` is the thing to compare it to.
///
/// #140's remedies shipped and the busiest prefixes still net-grew, and the first
/// question — is a tick finishing early? — could only be answered indirectly, by
/// watching `tansu_prefix_segment_compactions` go quiet between bursts. It did:
/// 1–2 minutes of merging per 10-minute window at
/// `maintenance_interval=10m`, a 10–20 % duty cycle, with the maintainers at 12
/// millicores between them. This makes the answer a measurement rather than an
/// inference, and it is the number to read the sweep counter against: a pass that
/// now fills its interval is one whose backlog the interval was never going to
/// clear.
static MAINTENANCE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_maintenance_duration")
        .with_unit("ms")
        .with_description("wall clock of one maintenance pass in milliseconds")
        .build()
});

/// Whether a maintenance pass is running on this replica, and whether a tick
/// came while it was.
///
/// One run at a time (#8), but a tick that finds a run in flight is **owed**
/// rather than dropped. It used to be skipped, and with the interval's
/// `MissedTickBehavior::Skip` that cost a whole interval of compaction: on the
/// GCS bucket a 10 min pass ended 4 s after the next tick, nothing merged for
/// the next 10 min, and every hot prefix took ~3 600 more segments (DEV-662).
/// An owed tick is run as soon as the pass ends, so an overrun costs the
/// overrun. Ticks owed during one pass coalesce into one pass.
#[derive(Clone, Debug, Default)]
struct MaintenanceSlot(Arc<AtomicU8>);

impl MaintenanceSlot {
    const IDLE: u8 = 0;
    const RUNNING: u8 = 1;
    const OWED: u8 = 2;

    /// Move the slot on by `next`, returning the state it was in.
    fn step(&self, next: impl Fn(u8) -> u8) -> u8 {
        let mut state = self.0.load(Ordering::Acquire);

        loop {
            match self.0.compare_exchange_weak(
                state,
                next(state),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(was) => return was,
                Err(now) => state = now,
            }
        }
    }

    /// Take the slot for a tick: `true` when the caller is to start a run,
    /// `false` when one is running and now owes a pass.
    fn claim(&self) -> bool {
        self.step(|state| {
            if state == Self::IDLE {
                Self::RUNNING
            } else {
                Self::OWED
            }
        }) == Self::IDLE
    }

    /// End a pass: `true` when a tick came during it and the run is to go on
    /// with another pass, `false` when the slot is free again.
    fn release(&self) -> bool {
        self.step(|state| {
            if state == Self::OWED {
                Self::RUNNING
            } else {
                Self::IDLE
            }
        }) == Self::OWED
    }

    #[cfg(test)]
    fn is_idle(&self) -> bool {
        self.0.load(Ordering::Acquire) == Self::IDLE
    }
}

/// Frees the slot if a run ends other than by [`MaintenanceSlot::release`]:
/// cancelled with its task, or unwinding from a panic, so a run can never
/// leave it taken and disable maintenance pod-wide until restart (#8, #131).
struct SlotGuard(Option<MaintenanceSlot>);

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if let Some(slot) = self.0.take() {
            slot.0.store(MaintenanceSlot::IDLE, Ordering::Release);
        }
    }
}

/// Run maintenance passes in the slot a tick claimed, each bounded by
/// `timeout`, until no tick is owed, then free the slot.
///
/// On timeout a pass future is dropped (cancelled), so a *hung* pass — e.g.
/// one wedged in S3 `DeleteObjects` retry loops during a throttling storm —
/// cannot hold the slot forever and disable compaction and retention
/// pod-wide until the pod is restarted (#131). The slot (#8) prevents
/// *concurrent* runs; this is the escape hatch for a *stuck* one.
/// Cancellation is safe: maintenance is idempotent and resumes on the next
/// pass.
async fn run_bounded_maintenance<F, P>(slot: MaintenanceSlot, timeout: Duration, mut pass: P)
where
    P: FnMut() -> F,
    F: Future<Output = ()>,
{
    let mut guard = SlotGuard(Some(slot.clone()));

    loop {
        if time::timeout(timeout, pass()).await.is_err() {
            warn!(
                ?timeout,
                "maintenance run exceeded its time budget; cancelling so the next tick retries \
                 (a run wedged in S3 retries would otherwise disable maintenance until restart)"
            );
        }

        if !slot.release() {
            break;
        }

        warn!("a maintenance tick came during the last pass; running the pass it owes now");
    }

    guard.0 = None;
}

/// How long the drain waits for in-flight requests to answer before the
/// process stops anyway (#361).
///
/// It does not have to cover a long poll: `join` and `sync` watch the same
/// cancellation token and answer as soon as it fires, and a `Fetch` finishes
/// inside its own `max.wait.ms`. What it covers is the tail — a slow object
/// store on the last read of a request that had already started — so it is
/// sized for a round trip under load, not for a poll window.
///
/// **`terminationGracePeriodSeconds` must exceed this**, or the kernel sends
/// `SIGKILL` while the drain is still running and the drain is decorative. See
/// the deployment notes in the README.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Accept on `listener` while one is bound. Once the drain has dropped it this
/// never yields, so the arm it feeds goes quiet rather than having to be
/// removed from the `select!`.
async fn accept_on(listener: Option<&TcpListener>) -> io::Result<(TcpStream, SocketAddr)> {
    match listener {
        Some(listener) => listener.accept().await,
        None => pending().await,
    }
}

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

/// What reaps a connection whose client has gone quiet or gone away (#605).
///
/// Both armed by default, as the frame cap is (#477): a broker built without
/// asking gets Kafka's `connections.max.idle.ms` and a keepalive that probes
/// within a minute, rather than holding a vanished client's socket until it
/// restarts.
#[derive(Clone, Copy, Debug)]
struct Liveness {
    /// See [`TcpContext::CONNECTIONS_MAX_IDLE`]; `None` never closes an idle
    /// connection.
    connections_max_idle: Option<Duration>,

    /// See [`Keepalive`]; `None` leaves `SO_KEEPALIVE` off.
    keepalive: Option<Keepalive>,
}

impl Default for Liveness {
    fn default() -> Self {
        Self {
            connections_max_idle: Some(TcpContext::CONNECTIONS_MAX_IDLE),
            keepalive: Some(Keepalive::default()),
        }
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

    sasl_config: Option<Arc<SASLConfig>>,

    /// The ACL decision, when this broker enforces one (#363).
    ///
    /// `None` without `--authentication`: there are no principals, so there is
    /// nothing to evaluate, and enforcing would refuse every request on every
    /// deployment that has never turned authentication on.
    authorizer: Option<Authorizer>,

    /// The quota decision, when this broker enforces one (#384).
    ///
    /// `None` on the same switch as `authorizer`, and for the same reason:
    /// without a principal there is nothing to write a limit against.
    enforcer: Option<QuotaEnforcer>,

    tls_server_config: Option<Arc<ServerConfig>>,
    silent: bool,

    /// The largest request frame this listener will read, `None` to accept any
    /// (#477). Kafka's `socket.request.max.bytes`.
    maximum_frame_size: Option<usize>,

    /// Requests a connection may read ahead of their answers (#588), `None`
    /// for [`TcpContext::PIPELINE_DEPTH`].
    pipeline_depth: Option<usize>,

    liveness: Liveness,

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
        Broker {
            node_id,
            cluster_id: cluster_id.to_owned(),
            incarnation_id,
            listener,
            advertised_listener,
            storage,
            groups,

            sasl_config: None,
            authorizer: None,
            enforcer: None,
            tls_server_config: None,

            silent: false,

            // Armed, as `TcpContext::default()` is: a `Broker` built directly
            // rather than through the builder gets the same cap (#477).
            maximum_frame_size: Some(TcpContext::MAXIMUM_FRAME_SIZE),
            pipeline_depth: None,
            liveness: Liveness::default(),

            maintenance: Maintenance::default(),

            cancellation: CancellationToken::new(),
        }
    }

    pub fn builder() -> PhantomBuilder {
        PhantomBuilder::default()
    }

    pub async fn main(mut self, started: Instant) -> Result<ErrorCode> {
        let requests = RootMessageMeta::messages().requests();
        let names: Vec<&str> = requests.values().map(|meta| meta.name).collect();
        debug!(messages = names.join(", "));

        let mut tasks = JoinSet::new();
        let mut interrupts = signal(SignalKind::interrupt()).unwrap();
        debug!(interrupt_signal = ?interrupts);
        let mut terminations = signal(SignalKind::terminate()).unwrap();
        debug!(terminate_signal = ?terminations);

        let silent = self.silent;
        let token = self.cancellation.clone();
        _ = tasks.spawn(async move {
            let served = self.serve(started).await;
            if let Err(err) = &served {
                error!(err = ?err);
            }
            served.unwrap();
        });

        let kind = tokio::select! {
            finished = tasks.join_next() => {
                debug!(v = ?finished);
                None
            }
            interrupt = interrupts.recv() => {
                debug!(interrupt = ?interrupt);
                Some(CancelKind::Interrupt)
            }
            terminate = terminations.recv() => {
                debug!(terminate = ?terminate);
                Some(CancelKind::Terminate)
            }
        };
        let Some(kind) = kind else {
            return Ok(ErrorCode::None);
        };

        token.cancel();
        let joined = async {
            loop {
                let len = tasks.len();
                if len == 0 {
                    break;
                }
                debug!(len = len);
                _ = tasks.join_next().await;
            }
        };
        tokio::select! {
            finished = joined => debug!(v = ?finished),
            () = sleep(Duration::from(kind)) => {
                debug!(aborting = tasks.len());
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
            }
        }

        let stdout = Term::stdout();
        if !silent && stdout.is_term() {
            _ = stdout.clear_screen().ok();
        }
        Ok(ErrorCode::None)
    }

    pub async fn serve(&mut self, started: Instant) -> Result<()> {
        // Before anything is registered or served: this binary writes the
        // decomposed consumer group layout (#359), and a cluster holding
        // another one must stop it rather than have both written into it. See
        // `tansu_storage::GroupSchema` for why this is an assertion and never
        // a converter, and `docs/migration-groups.md` for the cutover it
        // guards.
        self.storage.assert_group_schema().await?;

        self.register().await?;
        self.listen(started).await
    }

    pub async fn register(&mut self) -> Result<()> {
        let registration = BrokerRegistrationRequest {
            broker_id: self.node_id,
            cluster_id: self.cluster_id.clone(),
            incarnation_id: self.incarnation_id,
            rack: None,
        };
        self.storage.register_broker(registration).await?;
        Ok(())
    }

    pub async fn listen(&self, started: Instant) -> Result<()> {
        tracing::debug!(%self.listener, %self.advertised_listener);

        // `Option` so the drain below can drop it. A listener left bound while
        // nothing accepts is worse than one that is gone: the kernel keeps
        // completing handshakes into the backlog, so a client connecting to a
        // stopping replica waits for a response that will never be read
        // instead of being refused and moving on (#361).
        let bound = bind(&self.listener, 9092).await;
        if let Err(err) = &bound {
            error!(err = ?err, advertised_listener = %self.advertised_listener);
        }
        let mut listener = Some(bound?);

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

        // Guard against overlapping `maintain` runs: under S3 throttling a pass
        // can take longer than the interval, and spawning a second concurrent
        // run compounds the memory + S3 pressure (#8).
        let maintenance = MaintenanceSlot::default();

        let mut set = JoinSet::new();
        let m = MultiProgress::new();
        let spinner_style = spinner_style();

        let ls = (!self.silent).then(|| {
            let ready_ms = started.elapsed().as_millis();
            println!("ready in {ready_ms}ms");
            let listening = m.add(ProgressBar::new_spinner());
            listening.set_style(spinner_style.clone());
            if let Some(Ok(local_addr)) = listener.as_ref().map(TcpListener::local_addr) {
                listening.set_prefix(format!("[{local_addr:?}]"));
            }
            listening.set_message("listening for connection...");
            listening
        });

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
            if let Some(listening) = &ls {
                listening.tick();
            }

            tokio::select! {
                Ok((stream, addr)) = accept_on(listener.as_ref()) => {
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

                _ = maintenance_tick(interval.as_mut()) => {
                    // A tick that finds a run in flight is owed to it rather than
                    // spawning a second concurrent run (#8): see `MaintenanceSlot`.
                    if !maintenance.claim() {
                        warn!(
                            "maintenance tick came while the previous run is in flight; \
                             it runs when that one ends"
                        );
                    } else {
                        let maintained = self.storage.clone();
                        let slot = maintenance.clone();

                        let spawned = set.spawn(async move {
                            let span = tracing::debug_span!("maintenance");

                            // Bounded so a hung run frees the slot and the next
                            // tick can retry, rather than wedging maintenance
                            // pod-wide until restart (#131).
                            run_bounded_maintenance(slot, maintenance_run_timeout, || {
                                let maintained = maintained.clone();
                                async move {
                                let started = Instant::now();

                                // A failure here is not cosmetic: it means this
                                // replica did no retention and no compaction this
                                // tick. `warn!` rather than `debug!` so it is
                                // visible at the level production runs at, plus a
                                // counter so it does not depend on someone reading
                                // logs (#284).
                                _ = maintained
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

                                // Recorded for a failed pass too: a pass that
                                // died at the top is a pass that did nothing for
                                // the rest of the interval, and the duration is
                                // how that reads next to the sweep counter (#399).
                                MAINTENANCE_DURATION
                                    .record(started.elapsed().as_millis() as u64, &[]);
                                }
                            }).instrument(span).await
                        });

                        debug!(handle = ?spawned);
                    }
                }

                finished = set.join_next(), if !set.is_empty() => {
                    debug!(v = ?finished);
                }

                stopped = self.cancellation.cancelled() => {
                    debug!(message = ?stopped);
                    break;
                }
            }
        }

        // Stop accepting, and stop *appearing* to accept: dropping the listener
        // closes the socket, so the load balancer's health check fails, new
        // connections are refused rather than queued, and traffic moves to the
        // replicas that are staying (#361).
        drop(listener.take());

        // Drain. Every in-flight request finishes and writes its response —
        // the group long polls answer early because they watch the same token
        // (`Controller::with_cancellation`), and a fetch finishes inside its
        // own `max.wait.ms`. Bounded, because a client that holds a connection
        // open and sends nothing would otherwise hold the shutdown open with
        // it; the bound is what `terminationGracePeriodSeconds` has to exceed.
        let draining = Instant::now();

        let drain = async {
            loop {
                let in_flight = set.len();
                if in_flight == 0 {
                    break;
                }
                debug!(in_flight = in_flight);
                _ = set.join_next().await;
            }
        };

        tokio::select! {
            () = drain => debug!(drained_in_ms = draining.elapsed().as_millis() as u64),

            () = sleep(DRAIN_TIMEOUT) => {
                // Not `debug!`: a drain that ran out of time cut somebody's
                // request, which is the thing this exists to prevent, and it is
                // invisible from the client side (a closed socket looks like a
                // network fault).
                warn!(
                    in_flight = set.len(),
                    drain_timeout_ms = DRAIN_TIMEOUT.as_millis() as u64,
                    "drain timed out with requests still in flight; they will be cut"
                );
            }
        }

        Ok(())
    }

    /// Serve one accepted connection on `set`, with `groups` as the group
    /// coordinator for its service stack.
    ///
    /// `acceptor` decides the transport: `Some` wraps the stream in TLS before
    /// the Kafka stack sees a byte, `None` serves it as it arrived (#358).
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
        let mut ctx = Context::default();

        // What `TcpContextService` used to read off the stream with
        // `peer_addr()` — the call that made the service stack `TcpStream`-only
        // and so kept TLS out of it. The accept loop knows the address already.
        _ = ctx.insert(Peer(addr));

        let pb = (!self.silent).then(|| {
            let connected = m.add(ProgressBar::new_spinner());
            connected.set_style(spinner_style.clone());
            connected.set_prefix(format!("[{connections}/{addr:?}]"));
            connected.set_message("connected");
            connected.tick();
            connected
        });
        if let Some(connected) = &pb {
            _ = ctx.insert(connected.clone());
        }

        TcpStream::set_nodelay(&stream, true)?;

        // Warned and served without, not `?`: an error here ends the accept
        // loop, and a peer that resets between `accept` and this call must not
        // take every other connection with it (#605).
        if let Some(keepalive) = self.liveness.keepalive
            && let Err(err) = keepalive.apply(&stream)
        {
            warn!(%addr, ?err, "could not turn TCP keepalive on; serving the connection without it");
        }

        let service = services(
            self.pipeline_depth
                .map_or_else(TcpContext::default, |depth| {
                    TcpContext::default().pipeline_depth(depth)
                })
                .cluster_id(Some(self.cluster_id.clone()))
                .maximum_frame_size(self.maximum_frame_size)
                .connections_max_idle(self.liveness.connections_max_idle)
                // So this connection is closed the next time it is idle between
                // requests, rather than held open until the client goes away and
                // the drain runs out of patience (#361).
                .drain(self.cancellation.clone()),
            groups,
            S::clone(&self.storage),
            self.sasl_config.clone(),
            self.authorizer.clone(),
            self.enforcer.clone(),
        )?;

        let spawned = set.spawn(async move {
            match acceptor {
                // The handshake runs here, in the connection's own task, and not
                // in `listen`'s accept loop: a peer that opens a socket and then
                // says nothing must cost itself, not everything queued behind
                // `accept`.
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(stream) => report_connection(service.serve(ctx, stream).await),

                    // Where a plaintext client on a TLS listener arrives, and
                    // the one event #358's silence left no trace of at all.
                    // Reported with the address because the handshake failed, so
                    // no per-connection span was ever entered to carry it.
                    Err(err) => warn!(%addr, ?err, "TLS handshake failed; refusing the connection"),
                },

                None => report_connection(service.serve(ctx, stream).await),
            }

            if let Some(connected) = &pb {
                connected.finish_and_clear();
            }
        });

        debug!(handle = ?spawned);

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

        Ok(()) => debug!(response = ?()),
    }
}

/// Bind a TCP listener on `url`, falling back to the unspecified address
/// and/or `default_port` when the URL leaves them out.
async fn bind(url: &Url, default_port: u16) -> Result<TcpListener> {
    let port = url.port().unwrap_or(default_port);
    let unspecified = SocketAddr::from((Ipv6Addr::UNSPECIFIED, port));
    let address = match url.host() {
        None => unspecified,
        Some(host) => {
            debug!(host = ?host, port = port);
            match host {
                url::Host::Domain(domain) => {
                    let named = SocketAddr::from_str(&format!("{domain}:{port}"));
                    named.unwrap_or(unspecified)
                }
                url::Host::Ipv4(v4) => SocketAddr::from((IpAddr::V4(v4), port)),
                url::Host::Ipv6(v6) => SocketAddr::from((IpAddr::V6(v6), port)),
            }
        }
    };

    let bound = TcpListener::bind(address).await;
    match &bound {
        Ok(listening) => debug!(url = %url, listener = ?listening.local_addr().ok()),
        Err(err) => error!(err = ?err, url = %url),
    }
    bound.map_err(Into::into)
}

/// The style of every spinner the listener draws.
fn spinner_style() -> ProgressStyle {
    let template = ProgressStyle::with_template("{prefix:.bold.dim} {spinner} {msg}");
    template.unwrap().tick_chars("⠁⠂⠄⡀⢀⠠⠐")
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

    /// Principals allowed everything without consulting a rule (#363).
    ///
    /// Without at least one, a cluster with no ACLs can never be given any:
    /// a fail-closed broker denies `CreateAcls` like everything else.
    super_users: Vec<String>,

    /// The limits a principal gets when the cluster's quotas name neither it
    /// nor a default (#384), so that a broker with no control plane in front of
    /// it is still protected by something.
    quota_defaults: QuotaLimits,

    /// How many replicas a configured quota is shared between (#384).
    ///
    /// `1` enforces every configured limit on every replica, which is what
    /// Apache Kafka does. Higher reads a configured limit as a fleet-wide one
    /// and divides it — approximate, and wrong while the fleet is mid-scale;
    /// see `docs/quotas.md`.
    ///
    /// A count, deliberately not a peer set: it does not name a replica, ask
    /// for a hostname, or need any replica to be reachable from any other,
    /// which is what #360 removed and what must not come back.
    quota_fleet_size: u32,

    /// The largest request frame the listener will read (#477), `None` to accept
    /// any. Defaults to Kafka's `socket.request.max.bytes`.
    maximum_frame_size: Option<usize>,

    /// Requests a connection may read ahead of their answers (#588). `None`,
    /// which is what `Default` gives, is [`TcpContext::PIPELINE_DEPTH`] — an
    /// `Option` because a derived `0` would turn pipelining off for every
    /// broker built without asking.
    pipeline_depth: Option<usize>,

    /// The idle deadline and keepalive (#605), armed by `Default`.
    liveness: Liveness,

    /// The policy consumer-protocol groups are held to (#628), Kafka's
    /// defaults unless asked otherwise.
    consumer_group_config: ConsumerGroupConfig,

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
    /// This builder with its six typestate fields mapped through `retype`,
    /// every other field carried over as it is.
    fn retyped<N2, C2, I2, A2, S2, L2>(
        self,
        retype: impl FnOnce(N, C, I, A, S, L) -> (N2, C2, I2, A2, S2, L2),
    ) -> Builder<N2, C2, I2, A2, S2, L2> {
        let Builder {
            node_id,
            cluster_id,
            incarnation_id,
            advertised_listener,
            storage,
            listener,
            otlp_endpoint_url,
            authentication,
            tls_server_config,
            silent,
            maintenance,
            topic_defaults,
            super_users,
            quota_defaults,
            quota_fleet_size,
            maximum_frame_size,
            pipeline_depth,
            liveness,
            consumer_group_config,
            cancellation,
        } = self;
        let (node_id, cluster_id, incarnation_id, advertised_listener, storage, listener) = retype(
            node_id,
            cluster_id,
            incarnation_id,
            advertised_listener,
            storage,
            listener,
        );
        Builder {
            node_id,
            cluster_id,
            incarnation_id,
            advertised_listener,
            storage,
            listener,
            otlp_endpoint_url,
            authentication,
            tls_server_config,
            silent,
            maintenance,
            topic_defaults,
            super_users,
            quota_defaults,
            quota_fleet_size,
            maximum_frame_size,
            pipeline_depth,
            liveness,
            consumer_group_config,
            cancellation,
        }
    }
}

impl<N, C, I, A, S, L> Builder<N, C, I, A, S, L> {
    const MAINTENANCE_INTERVAL: &str = "maintenance_interval";
    const MAINTENANCE_PASS_BUDGET: &str = "maintenance_pass_budget";

    pub fn node_id(self, node_id: i32) -> Builder<i32, C, I, A, S, L> {
        self.retyped(
            |_, cluster_id, incarnation_id, advertised_listener, storage, listener| {
                (
                    node_id,
                    cluster_id,
                    incarnation_id,
                    advertised_listener,
                    storage,
                    listener,
                )
            },
        )
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> Builder<N, String, I, A, S, L> {
        self.retyped(
            |node_id, _, incarnation_id, advertised_listener, storage, listener| {
                (
                    node_id,
                    cluster_id.into(),
                    incarnation_id,
                    advertised_listener,
                    storage,
                    listener,
                )
            },
        )
    }

    pub fn incarnation_id(self, incarnation_id: impl Into<Uuid>) -> Builder<N, C, Uuid, A, S, L> {
        self.retyped(
            |node_id, cluster_id, _, advertised_listener, storage, listener| {
                (
                    node_id,
                    cluster_id,
                    incarnation_id.into(),
                    advertised_listener,
                    storage,
                    listener,
                )
            },
        )
    }

    pub fn advertised_listener(
        self,
        advertised_listener: impl Into<Url>,
    ) -> Builder<N, C, I, Url, S, L> {
        self.retyped(
            |node_id, cluster_id, incarnation_id, _, storage, listener| {
                (
                    node_id,
                    cluster_id,
                    incarnation_id,
                    advertised_listener.into(),
                    storage,
                    listener,
                )
            },
        )
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

        let mut kept: Vec<(String, String)> = storage
            .query_pairs()
            .filter(|(key, _)| key != Self::MAINTENANCE_INTERVAL)
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();

        // The storage engine sweeps its backlog for a pass budget and cannot see
        // the interval, which is consumed above (DEV-683). 0.9× leaves the pass
        // finished when the next tick fires, so that tick re-claims every prefix
        // rather than being skipped as still in flight. A budget the URL names
        // wins.
        if let Maintenance::Every(period) = maintenance
            && !kept
                .iter()
                .any(|(key, _)| key == Self::MAINTENANCE_PASS_BUDGET)
        {
            kept.push((
                Self::MAINTENANCE_PASS_BUDGET.to_owned(),
                format!("{}ms", (period * 9 / 10).as_millis()),
            ));
        }
        if kept.is_empty() {
            storage.set_query(None);
        } else {
            _ = storage.query_pairs_mut().clear().extend_pairs(kept);
        }

        debug!(?maintenance, %storage);

        let mut retyped = self.retyped(
            |node_id, cluster_id, incarnation_id, advertised_listener, _, listener| {
                (
                    node_id,
                    cluster_id,
                    incarnation_id,
                    advertised_listener,
                    storage,
                    listener,
                )
            },
        );
        retyped.maintenance = maintenance;
        retyped
    }

    pub fn listener(self, listener: Url) -> Builder<N, C, I, A, S, Url> {
        tracing::debug!(%listener);

        self.retyped(
            |node_id, cluster_id, incarnation_id, advertised_listener, storage, _| {
                (
                    node_id,
                    cluster_id,
                    incarnation_id,
                    advertised_listener,
                    storage,
                    listener,
                )
            },
        )
    }

    pub fn otlp_endpoint_url(self, otlp_endpoint_url: Option<Url>) -> Self {
        let mut builder = self;
        builder.otlp_endpoint_url = otlp_endpoint_url;
        builder
    }

    pub fn authentication(self, authentication: bool) -> Self {
        let mut builder = self;
        builder.authentication = authentication;
        builder
    }

    pub fn tls_server_config(self, tls_server_config: Option<ServerConfig>) -> Self {
        let mut builder = self;
        builder.tls_server_config = tls_server_config;
        builder
    }
    pub fn silent(self, silent: bool) -> Self {
        let mut builder = self;
        builder.silent = silent;
        builder
    }

    /// Principals allowed everything without consulting an ACL (#363), in
    /// Kafka's `User:name` spelling.
    pub fn super_users(self, super_users: Vec<String>) -> Self {
        let mut builder = self;
        builder.super_users = super_users;
        builder
    }

    /// The quota a principal gets when the cluster's own quotas name neither it
    /// nor a default (#384).
    pub fn quota_defaults(self, quota_defaults: QuotaLimits) -> Self {
        let mut builder = self;
        builder.quota_defaults = quota_defaults;
        builder
    }

    /// How many replicas a configured quota is shared between (#384). `1`
    /// enforces each configured limit on every replica, as Apache Kafka does.
    /// Cap the request frames this listener will read, `None` to accept any
    /// (#477).
    ///
    /// `None` is an operator deliberately turning the cap off, which is not the
    /// same thing as the cap never having been armed — which is what it was
    /// until #477.
    pub fn maximum_frame_size(self, maximum_frame_size: Option<usize>) -> Self {
        let mut builder = self;
        builder.maximum_frame_size = maximum_frame_size;
        builder
    }

    /// Read at most `pipeline_depth` requests on a connection ahead of their
    /// answers (#588); `1` turns pipelining off. See
    /// [`TcpContext::PIPELINE_DEPTH`] for the default and
    /// [`TcpContext::MAXIMUM_PIPELINE_DEPTH`] for what raising it costs.
    pub fn pipeline_depth(self, pipeline_depth: usize) -> Self {
        let mut builder = self;
        builder.pipeline_depth = Some(pipeline_depth);
        builder
    }

    /// Close a connection that has owed nothing and read nothing for
    /// `connections_max_idle` (#605), Kafka's `connections.max.idle.ms`;
    /// `None` never does. See [`TcpContext::CONNECTIONS_MAX_IDLE`].
    pub fn connections_max_idle(self, connections_max_idle: Option<Duration>) -> Self {
        let mut builder = self;
        builder.liveness.connections_max_idle = connections_max_idle;
        builder
    }

    /// TCP keepalive on every accepted connection (#605); `None` leaves it
    /// off. See [`Keepalive`].
    pub fn keepalive(self, keepalive: Option<Keepalive>) -> Self {
        let mut builder = self;
        builder.liveness.keepalive = keepalive;
        builder
    }

    /// Hold consumer-protocol groups to `consumer_group_config` (#628).
    pub fn consumer_group_config(self, consumer_group_config: ConsumerGroupConfig) -> Self {
        let mut builder = self;
        builder.consumer_group_config = consumer_group_config;
        builder
    }

    pub fn quota_fleet_size(self, quota_fleet_size: u32) -> Self {
        let mut builder = self;
        builder.quota_fleet_size = quota_fleet_size;
        builder
    }

    /// The token that asks this broker to stop and drain (#361).
    ///
    /// `Broker::main` cancels its own on a signal; a caller that drives
    /// `serve` directly — a test, or an embedder — needs a handle on the same
    /// token to ask for the drain at all.
    pub fn cancellation(self, cancellation: CancellationToken) -> Self {
        let mut builder = self;
        builder.cancellation = cancellation;
        builder
    }

    pub fn topic_defaults(self, topic_defaults: TopicDefaults) -> Self {
        let mut builder = self;
        builder.topic_defaults = topic_defaults;
        builder
    }
}

impl Builder<i32, String, Uuid, Url, Url, Url> {
    pub async fn build(self) -> Result<Broker<Controller<ArcDynStorage>, ArcDynStorage>> {
        if let Some(endpoint) = self.otlp_endpoint_url.clone() {
            debug!(otlp_endpoint_url = %endpoint);
            otel::metric_exporter(endpoint)?;
        }

        let container = StorageContainer::builder()
            .cluster_id(self.cluster_id.clone())
            .node_id(self.node_id)
            .advertised_listener(self.advertised_listener.clone())
            .storage(self.storage.clone());
        let built = container
            .topic_defaults(self.topic_defaults.clone())
            .silent(self.silent)
            .build()
            .await?;
        let storage: ArcDynStorage = Arc::new(built);

        // One coordinator, and nothing to choose between: a group is coordinated
        // by whichever replica the request arrived at (#360). There is no owner
        // to elect, no peer set to discover, no second listener to bind — the
        // decomposed group objects (#359) made a group's writes independent, so
        // routing them to one replica bought nothing that the CAS does not.
        let groups = Controller::with_storage(storage.clone())?
            .with_consumer_group_config(self.consumer_group_config)
            // So an in-flight `JoinGroup` or `SyncGroup` answers when this
            // replica is asked to stop, rather than being cut mid-poll (#361).
            .with_cancellation(self.cancellation.clone());

        let sasl_config = match self.authentication {
            true => Some(tansu_auth::configuration(Arc::clone(&storage))?),
            false => None,
        };

        // Enforcement follows authentication: without it there are no
        // principals, so there is nothing to evaluate and every request on
        // every existing deployment would be refused (#363).
        let authorizer = self
            .authentication
            .then(|| Authorizer::new(storage.clone(), self.super_users.clone()));

        // The same switch, for the same reason: a quota is written against a
        // principal, and without authentication there are none (#384). A broker
        // that has never turned authentication on behaves exactly as it did.
        let enforcer = self.authentication.then(|| {
            QuotaEnforcer::new(storage.clone())
                .with_defaults(self.quota_defaults)
                .with_replicas(self.quota_fleet_size.max(1))
        });

        let tls_server_config = self.tls_server_config.map(Arc::new);
        Ok(Broker {
            cluster_id: self.cluster_id.clone(),
            node_id: self.node_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            listener: self.listener,
            groups,
            storage,
            authorizer,
            enforcer,
            sasl_config,
            tls_server_config,

            silent: self.silent,
            maintenance: self.maintenance,
            maximum_frame_size: self.maximum_frame_size,
            pipeline_depth: self.pipeline_depth,
            liveness: self.liveness,
            cancellation: self.cancellation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;

    // A run that never completes (modelling a pass wedged in S3 retry loops)
    // must be cancelled at the timeout and free the slot, so the next tick can
    // retry — not stay taken and disable maintenance pod-wide until restart
    // (#131). Paused time makes `timeout` fire in virtual time.
    #[tokio::test(start_paused = true)]
    async fn bounded_maintenance_cancels_a_hung_pass_and_frees_the_slot() {
        let slot = MaintenanceSlot::default();
        assert!(slot.claim());

        run_bounded_maintenance(slot.clone(), Duration::from_secs(60), pending::<()>).await;

        assert!(
            slot.is_idle(),
            "the slot must be free after a timed-out run"
        );
    }

    // The normal path: the pass runs to completion and the slot is freed.
    #[tokio::test]
    async fn bounded_maintenance_runs_the_pass_and_frees_the_slot() {
        let slot = MaintenanceSlot::default();
        assert!(slot.claim());
        let passes = Arc::new(AtomicU8::new(0));

        let counted = passes.clone();
        run_bounded_maintenance(slot.clone(), Duration::from_secs(60), || {
            _ = counted.fetch_add(1, Ordering::AcqRel);
            std::future::ready(())
        })
        .await;

        assert_eq!(
            1,
            passes.load(Ordering::Acquire),
            "the pass must have run once"
        );
        assert!(
            slot.is_idle(),
            "the slot must be free after the run completes"
        );
    }

    /// A tick during a pass is run when the pass ends, not skipped
    /// (DEV-662): skipping it lost a whole interval of compaction on the GCS
    /// bucket. Two ticks during one pass owe one pass, not two.
    #[tokio::test]
    async fn a_tick_during_a_pass_is_run_when_it_ends() {
        let slot = MaintenanceSlot::default();
        assert!(slot.claim());
        let passes = Arc::new(AtomicU8::new(0));

        let (ticking, counted) = (slot.clone(), passes.clone());
        run_bounded_maintenance(slot.clone(), Duration::from_secs(60), || {
            if counted.fetch_add(1, Ordering::AcqRel) == 0 {
                assert!(!ticking.claim(), "a run is in flight");
                assert!(!ticking.claim(), "a run is in flight");
            } else {
                assert!(!ticking.is_idle(), "the owed pass holds the slot");
            }
            std::future::ready(())
        })
        .await;

        assert_eq!(2, passes.load(Ordering::Acquire));
        assert!(slot.is_idle());
        assert!(slot.claim(), "a tick after the run starts one");
    }

    /// A run whose task is dropped mid-pass, as at shutdown, or that panics,
    /// frees the slot (#8, #131).
    #[tokio::test]
    async fn a_run_that_dies_frees_the_slot() {
        let slot = MaintenanceSlot::default();
        assert!(slot.claim());

        let task = tokio::spawn(run_bounded_maintenance(
            slot.clone(),
            Duration::from_secs(60),
            || -> std::future::Ready<()> { panic!("a pass panicked") },
        ));
        assert!(task.await.is_err_and(|err| err.is_panic()));
        assert!(slot.is_idle(), "a panicked run frees the slot");

        assert!(slot.claim());
        let task = tokio::spawn(run_bounded_maintenance(
            slot.clone(),
            Duration::from_secs(60),
            pending::<()>,
        ));
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.is_err_and(|err| err.is_cancelled()));
        assert!(slot.is_idle(), "a cancelled run frees the slot");
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

        // The engine cannot see the interval, so it is handed a pass budget just
        // under it (DEV-683) — unless the URL already names one.
        #[test]
        fn the_interval_sets_the_pass_budget_unless_the_url_does() {
            let built = PhantomBuilder::default()
                .storage(Url::parse("s3://bucket/?maintenance_interval=2m").unwrap());
            assert_eq!(
                built.storage.as_str(),
                "s3://bucket/?maintenance_pass_budget=108000ms"
            );

            let built = PhantomBuilder::default().storage(
                Url::parse("s3://bucket/?maintenance_interval=2m&maintenance_pass_budget=30s")
                    .unwrap(),
            );
            assert_eq!(
                built.storage.as_str(),
                "s3://bucket/?maintenance_pass_budget=30s"
            );
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
}
