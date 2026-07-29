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

//! Foundations for in-broker forward-to-owner group coordination.
//!
//! Consumer-group state is a single object per group mutated via etag CAS.
//! With N stateless replicas behind one load balancer, a group's Join/Sync
//! long-polls scatter across replicas and thrash that object's CAS, so
//! membership never quiesces. The structural fix is *soft ownership*: every
//! replica deterministically maps each `group_id` to exactly one owner
//! replica and forwards the six group APIs there, so the whole membership
//! state machine for a group runs on a single [`Controller`] — the regime
//! already proven to converge by the single-controller tests.
//!
//! Ownership is *soft* because it is an optimisation, never a correctness
//! requirement: every write remains conditional on the object's etag, so two
//! replicas transiently believing they own the same group (DNS skew during a
//! rolling restart) degrades at worst to today's CAS-retry behaviour, never
//! to corruption.
//!
//! This module provides:
//!
//! * [`fnv1a_64`] — an inline FNV-1a 64 hash. [`std::hash::DefaultHasher`]
//!   is deliberately not used: SipHash keys are unstable across processes
//!   and Rust versions, while the owner decision must agree across pods and
//!   across a rolling upgrade. FNV-1a is fully specified, trivially inlined,
//!   and stable forever.
//! * [`owner`] — rendezvous (highest-random-weight) hashing over the peer
//!   set. Rendezvous beats mod-N: removing one peer reassigns only that
//!   peer's groups (~1/N of the total); every other group keeps its owner.
//! * [`PeerRegistry`] — the peer set, discovered by polling the DNS A/AAAA
//!   records of a headless-Service hostname, plus this replica's own
//!   identity (its pod IP).
//! * [`ForwardingCoordinator`] — a [`Coordinator`] wrapper that forwards
//!   each group API call to the owner replica's internal listener (the
//!   forward hop is [`FrameForwarder`]), or processes it locally when this
//!   replica is the owner. Any forward failure falls back to local
//!   processing — the etag-CAS path — so every failure mode degrades to
//!   today's behaviour, never to an outage or corruption.
//! * [`GroupCoordinator`] — the enum the broker is actually built with
//!   (mirroring the `StorageContainer` enum-dispatch idiom), keeping
//!   `Broker<GroupCoordinator<_>, _>` a single concrete type whether
//!   forwarding is enabled or not. The default is [`GroupCoordinator::Local`],
//!   bit-for-bit today's behaviour.
//!
//! When the peer set is empty — DNS not yet resolved, resolution failing, or
//! discovery not configured — [`owner`] returns the local replica, i.e. the
//! broker processes the request itself. That fallback is bit-for-bit today's
//! local CAS behaviour, so every discovery failure mode degrades to the
//! status quo rather than to an outage.

use super::{Coordinator, OffsetCommit, administrator::Controller};
use crate::{Error, METER, Result};
use async_trait::async_trait;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge},
};
use rama::{Context, Layer as _, Service as _};
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    net::IpAddr,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tansu_client::{
    BytesConnectionService, ConnectionManager, FrameConnectionLayer, FramePoolLayer, Pool,
};
use tansu_sans_io::{
    ApiKey as _, Body, ErrorCode, Frame, Header, HeartbeatRequest, JoinGroupRequest,
    LeaveGroupRequest, OffsetCommitRequest, OffsetFetchRequest, SyncGroupRequest,
    join_group_request::JoinGroupRequestProtocol,
    leave_group_request::MemberIdentity,
    offset_commit_request::OffsetCommitRequestTopic,
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    sync_group_request::SyncGroupRequestAssignment,
};
use tansu_service::FrameBytesLayer;
use tansu_storage::Storage;
use tokio::task::JoinHandle;
use tracing::{debug, warn};
use url::Url;

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 64-bit hash of `bytes` folded into an existing `state`.
///
/// Exposed as a fold so a rendezvous score can hash `group_id ‖ peer`
/// without allocating a concatenated buffer.
fn fnv1a_64_fold(state: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(state, |hash, octet| {
        (hash ^ u64::from(*octet)).wrapping_mul(FNV_PRIME)
    })
}

/// FNV-1a 64-bit hash.
///
/// Chosen over [`std::hash::DefaultHasher`] because the result must be
/// identical on every pod and across rolling upgrades: SipHash is keyed per
/// process and unspecified across Rust versions, whereas FNV-1a is a fixed
/// public algorithm. Distribution quality is more than sufficient for
/// rendezvous hashing over tens of peers.
pub fn fnv1a_64(bytes: &[u8]) -> u64 {
    fnv1a_64_fold(FNV_OFFSET_BASIS, bytes)
}

/// The splitmix64 finalizer (Steele, Lea & Flood; also murmur-style
/// `fmix64`): a fixed, fully-specified bijection on `u64` used to avalanche
/// the rendezvous score.
///
/// Needed because FNV-1a diffuses trailing bytes weakly: rendezvous peers
/// are pod IPs that typically differ only in their last octet — exactly the
/// bytes folded in last — and without a finalizer one peer can capture a
/// large multiple of its fair share of groups. Like FNV-1a itself the
/// finalizer uses only fixed public constants, so scores stay identical
/// across pods and rolling upgrades.
fn mix64(hash: u64) -> u64 {
    let z = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The rendezvous score of `peer` for `group_id`:
/// `mix64(fnv1a_64(group_id ‖ peer))` where the peer is folded in as its raw
/// address octets.
fn rendezvous_score(group_id: &str, peer: &IpAddr) -> u64 {
    let group = fnv1a_64_fold(FNV_OFFSET_BASIS, group_id.as_bytes());

    mix64(match peer {
        IpAddr::V4(v4) => fnv1a_64_fold(group, &v4.octets()),
        IpAddr::V6(v6) => fnv1a_64_fold(group, &v6.octets()),
    })
}

/// The owner of `group_id` under rendezvous hashing: the candidate maximising
/// `mix64(fnv1a_64(group_id ‖ candidate))`, with ties broken by [`IpAddr`]
/// ordering so every replica agrees regardless of the order peers were
/// discovered in.
///
/// `self_ip` is ALWAYS one of the candidates, even when it is absent from
/// `peers`. A replica can always coordinate a group itself, and forcing self
/// into the candidate set removes a startup/rollout asymmetry: in the window
/// where the headless Service has published this pod to its *peers'* DNS but
/// this pod's own resolver has not yet listed itself, a self-excluding view
/// would forward this replica's own groups elsewhere while peers route those
/// same groups back to it — transient double-ownership. Including self makes
/// this replica agree with its peers about the groups it owns. (An empty
/// `peers` slice therefore still yields `self_ip` = local, today's behaviour.)
pub fn owner(group_id: &str, self_ip: IpAddr, peers: &[IpAddr]) -> IpAddr {
    peers
        .iter()
        .copied()
        .chain(std::iter::once(self_ip))
        .max_by_key(|candidate| (rendezvous_score(group_id, candidate), *candidate))
        .unwrap_or(self_ip)
}

/// The set of replicas eligible to own groups, refreshed from DNS.
///
/// Peers are the A/AAAA records of a headless-Service hostname (Kubernetes
/// publishes one record per *ready* pod, so ownership only lands on ready
/// replicas). `self_ip` is this replica's pod IP; it is used both for the
/// [`PeerRegistry::is_local`] decision and as the fallback owner when the
/// peer set is empty.
///
/// The registry itself performs no forwarding — it only answers "who owns
/// this group?" and "is that me?".
#[derive(Debug)]
pub struct PeerRegistry {
    self_ip: IpAddr,
    peer_dns: String,
    interval: Duration,
    peers: Mutex<Arc<Vec<IpAddr>>>,
}

impl PeerRegistry {
    /// A registry that will resolve `peer_dns` (a headless-Service
    /// hostname) every `interval`, starting with an empty peer set (all
    /// groups local until the first successful refresh).
    pub fn new(self_ip: IpAddr, peer_dns: impl Into<String>, interval: Duration) -> Self {
        Self {
            self_ip,
            peer_dns: peer_dns.into(),
            interval,
            peers: Mutex::new(Arc::new(Vec::new())),
        }
    }

    /// This replica's own address.
    pub fn self_ip(&self) -> IpAddr {
        self.self_ip
    }

    /// A snapshot of the current peer set.
    pub fn peers(&self) -> Arc<Vec<IpAddr>> {
        self.peers
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Replace the peer set. Peers are sorted and deduplicated; order does
    /// not affect ownership (see [`owner`]), this just keeps snapshots
    /// canonical for logging and comparison.
    pub fn set_peers(&self, mut peers: Vec<IpAddr>) {
        peers.sort_unstable();
        peers.dedup();

        GROUP_FORWARD_PEERS.record(peers.len() as u64, &[]);

        if let Ok(mut guard) = self.peers.lock() {
            *guard = Arc::new(peers);
        }
    }

    /// Resolve the configured DNS name and replace the peer set with the
    /// addresses found, returning how many there are.
    ///
    /// On resolution failure the peer set is cleared: an unresolvable
    /// headless service means the peer view cannot be trusted, and an empty
    /// set degrades every decision to "local", i.e. today's behaviour.
    pub async fn refresh(&self) -> Result<usize> {
        match tokio::net::lookup_host((self.peer_dns.as_str(), 0u16)).await {
            Ok(addresses) => {
                let peers = addresses.map(|address| address.ip()).collect::<Vec<_>>();

                debug!(peer_dns = %self.peer_dns, ?peers, "refreshed group forwarding peers");

                let n = peers.len();
                self.set_peers(peers);
                Ok(n)
            }

            Err(error) => {
                warn!(
                    peer_dns = %self.peer_dns,
                    %error,
                    "peer DNS resolution failed; falling back to local group coordination"
                );

                self.set_peers(Vec::new());
                Err(error.into())
            }
        }
    }

    /// The owner of `group_id` given the current peer snapshot.
    pub fn owner(&self, group_id: &str) -> IpAddr {
        owner(group_id, self.self_ip, &self.peers())
    }

    /// Whether this replica should coordinate `group_id` itself (it is the
    /// owner, or no peers are known).
    pub fn is_local(&self, group_id: &str) -> bool {
        self.owner(group_id) == self.self_ip
    }

    /// Spawn a background task refreshing the peer set every `interval`
    /// (the first refresh happens immediately). Resolution failures are
    /// logged by [`PeerRegistry::refresh`] and retried on the next tick;
    /// the task only ends when the registry is dropped by every other
    /// holder.
    pub fn spawn_refresh(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                _ = interval.tick().await;

                _ = self.refresh().await;

                if Arc::strong_count(&self) == 1 {
                    debug!(peer_dns = %self.peer_dns, "peer registry dropped; refresh ending");
                    break;
                }
            }
        })
    }
}

/// Default port of the internal (broker-to-broker) listener that forwarded
/// group requests are sent to.
pub const DEFAULT_INTERNAL_PORT: u16 = 9093;

/// Default interval between peer-set DNS refreshes. CoreDNS publishes
/// headless-Service records with a ~5s TTL, so polling faster buys nothing.
pub const PEER_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// The client id stamped on forwarded frames for the five group APIs whose
/// [`Coordinator`] method does not receive the caller's client id. Purely
/// cosmetic there: only `join` derives state (the member id) from the client
/// id, and `join` forwards the caller's own client id.
const FORWARD_CLIENT_ID: &str = "tansu-fwd";

/// Explicit upper bound on each per-owner connection pool. The deadpool
/// default is CPU-derived and far too small under fractional-CPU pod limits,
/// while a forwarded `join` long-polls up to the whole join window and pins
/// its pooled connection for the duration.
const POOL_MAX_SIZE: usize = 64;

/// Slack added to the join window (the rebalance timeout) when bounding a
/// forwarded `join`: the owner may legitimately hold the request for the
/// whole window, so only the excess indicates a dead owner.
const JOIN_TIMEOUT_SLACK: Duration = Duration::from_secs(10);

/// Upper bound on forwarded non-join calls. These are sub-second on a
/// healthy owner (heartbeat, commit); the bound guards against silent owner
/// death (node gone, no RST) where the TCP read would hang forever.
const CALL_TIMEOUT: Duration = Duration::from_secs(15);

static GROUP_FORWARD_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_group_forward_total")
        .with_description(
            "group API ownership decisions by api and outcome \
             (forwarded, local_owner or fallback)",
        )
        .build()
});

static GROUP_FORWARD_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_group_forward_errors")
        .with_description("forward-to-owner failures by kind")
        .build()
});

static GROUP_FORWARD_PEERS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_group_forward_peers")
        .with_description("replicas eligible to own consumer groups")
        .build()
});

/// The forward hop: deliver one group API request [`Body`] to the owner
/// replica and return the response [`Body`].
///
/// A trait so [`ForwardingCoordinator`] is unit-testable without a live
/// socket; the production implementation is [`FrameForwarder`].
#[async_trait]
pub trait Forward: Debug + Send + Sync + 'static {
    /// Deliver `body` (a group API request) to `owner`, preserving
    /// `client_id`, and return the owner's response body.
    async fn call(
        &self,
        owner: IpAddr,
        api_key: i16,
        client_id: Option<&str>,
        body: Body,
    ) -> Result<Body>;

    /// Drop any per-owner resources for addresses no longer in `owners`
    /// (peers that left the set during a rolling restart).
    fn retain(&self, _owners: &[IpAddr]) {}
}

/// The production [`Forward`]: frame-level forwarding over `tansu-client`'s
/// pooled proxy stack, one bounded connection pool per owner address.
///
/// The hop is frame level — `FramePoolLayer` → `FrameConnectionLayer` →
/// `FrameBytesLayer` → `BytesConnectionService` — rather than the typed
/// `Client::call`, because the typed path stamps the *pool's* client id on
/// every request while `member_id` is derived from the caller's client id on
/// join. The frame path takes the client id from the frame itself, so a
/// single pool per owner serves every member.
#[derive(Debug)]
pub struct FrameForwarder {
    internal_port: u16,
    pools: Mutex<HashMap<IpAddr, Pool>>,
}

impl FrameForwarder {
    pub fn new(internal_port: u16) -> Self {
        Self {
            internal_port,
            pools: Mutex::new(HashMap::new()),
        }
    }

    /// The pool for `owner`, created lazily (pool creation bootstraps the
    /// supported API versions from the owner, handling a mixed-version fleet
    /// during rolling upgrades).
    async fn pool(&self, owner: IpAddr) -> Result<Pool> {
        if let Some(pool) = self.pools.lock()?.get(&owner) {
            return Ok(pool.clone());
        }

        let broker = Url::parse(&match owner {
            IpAddr::V4(_) => format!("tcp://{owner}:{}", self.internal_port),
            IpAddr::V6(_) => format!("tcp://[{owner}]:{}", self.internal_port),
        })?;

        let pool = ConnectionManager::builder(broker)
            .client_id(Some(FORWARD_CLIENT_ID.into()))
            .max_size(Some(POOL_MAX_SIZE))
            .build()
            .await?;

        // two callers may race the bootstrap; first insert wins, the loser's
        // pool is dropped.
        Ok(self
            .pools
            .lock()
            .map(|mut pools| pools.entry(owner).or_insert(pool).clone())?)
    }

    /// The version to forward at: the negotiated maximum, except for
    /// `OffsetFetch` in its pre-v8 shape (`group_id` + `topics` fields),
    /// which only encodes at v7 and below — v8 restructured the request and
    /// response around a `groups` array, so forwarding a legacy-shaped call
    /// at v8+ would silently drop the group and return an empty response.
    fn api_version(pool: &Pool, api_key: i16, body: &Body) -> Result<i16> {
        let negotiated = pool.manager().api_version(api_key)?;

        if let Body::OffsetFetchRequest(request) = body
            && request.group_id.is_some()
        {
            return Ok(negotiated.min(7));
        }

        Ok(negotiated)
    }
}

#[async_trait]
impl Forward for FrameForwarder {
    async fn call(
        &self,
        owner: IpAddr,
        api_key: i16,
        client_id: Option<&str>,
        body: Body,
    ) -> Result<Body> {
        let pool = self.pool(owner).await?;
        let api_version = Self::api_version(&pool, api_key, &body)?;

        let frame = Frame {
            size: 0,
            header: Header::Request {
                api_key,
                api_version,
                // swapped for the pooled connection's own correlation id by
                // FrameConnectionService.
                correlation_id: 0,
                client_id: client_id.map(ToOwned::to_owned),
            },
            body,
        };

        let stack = (
            FramePoolLayer::new(pool),
            FrameConnectionLayer,
            FrameBytesLayer,
        )
            .into_layer(BytesConnectionService);

        stack
            .serve(Context::default(), frame)
            .await
            .map(|response| response.body)
            .map_err(Into::into)
    }

    fn retain(&self, owners: &[IpAddr]) {
        if let Ok(mut pools) = self.pools.lock() {
            pools.retain(|owner, _| owners.contains(owner));
        }
    }
}

/// A [`Coordinator`] that routes each group API call to the deterministic
/// owner of its `group_id`: processed locally when this replica is the
/// owner, forwarded verbatim to the owner's internal listener otherwise.
///
/// Correctness never depends on the routing: any forward failure — connect
/// error, pool exhaustion, protocol error, deadline — falls back to local
/// processing, which is exactly today's GET-first + etag-CAS path, so
/// transient double-ownership (peer-view skew during a rolling restart)
/// degrades at worst to today's CAS-retry behaviour. Persistent fallback is
/// surfaced by the `tansu_group_forward_total{outcome="fallback"}` counter:
/// if it climbs, forwarding is silently off and the CAS thrash it prevents
/// is back.
#[derive(Clone, Debug)]
pub struct ForwardingCoordinator<C> {
    local: C,
    peers: Arc<PeerRegistry>,
    forward: Arc<dyn Forward>,
    join_timeout_slack: Duration,
    call_timeout: Duration,
    /// Last rebalance window each group's members asked for, learned from the
    /// `join` this replica forwarded (#190). `sync` has no such parameter in
    /// its own signature, yet it is the request that carries the whole
    /// assignment — so without this it was the biggest payload on the smallest
    /// deadline.
    rebalance_windows: Arc<Mutex<BTreeMap<String, Duration>>>,
    fallbacks: Arc<AtomicU64>,
}

/// What is being forwarded — the parts that describe the request rather than
/// where it goes or how long it may take.
struct Forwardee<'a> {
    api: &'static str,
    api_key: i16,
    client_id: Option<&'a str>,
    /// Reported on a timeout so the deadline can be sized against the payload
    /// it was too small for (#190). `None` where the request is small and
    /// fixed-shape.
    payload_bytes: Option<usize>,
    /// Whether serving this API locally would write group state. Decides what a
    /// live-but-slow owner means: for a mutating API it means "do not race it",
    /// for a read-only one the local answer is free.
    mutating: bool,
}

/// What the caller should do with a forward attempt (#190).
///
/// The decision turns on distinguishing the two failure shapes. Peers come from
/// a headless-Service DNS lookup, which publishes only *ready* pods, so an owner
/// this replica can name is an owner that is up — and ownership moves on its own
/// when it is not. A **timeout** therefore means the owner accepted the request
/// and is still working on it. A **transport error** is the genuinely ambiguous
/// case: the owner may have gone between the DNS refresh and the call.
///
/// For a state-mutating API those are opposite situations. Processing locally
/// against a live owner puts a second writer on one group document — #78's
/// hazard applied to group state, and the split-brain behind #190's permanent
/// rebalance churn. Processing locally when the owner has actually gone is the
/// backstop that keeps the group serviceable.
#[derive(Debug)]
enum Forwarded {
    /// The owner answered.
    Response(Body),
    /// Process locally: this replica owns the group, or the forward failed in a
    /// way that makes the local answer the right one.
    Local,
    /// Tell the client to retry against the real owner, which is up and still
    /// working on this group.
    Retry,
}

impl<C> ForwardingCoordinator<C>
where
    C: Coordinator,
{
    /// A coordinator forwarding to each owner's `internal_port` over
    /// [`FrameForwarder`].
    pub fn new(local: C, peers: Arc<PeerRegistry>, internal_port: u16) -> Self {
        Self::with_forward(local, peers, Arc::new(FrameForwarder::new(internal_port)))
    }

    /// A coordinator with an injected forward hop (tests stub this seam).
    pub fn with_forward(local: C, peers: Arc<PeerRegistry>, forward: Arc<dyn Forward>) -> Self {
        Self {
            local,
            peers,
            forward,
            join_timeout_slack: JOIN_TIMEOUT_SLACK,
            call_timeout: CALL_TIMEOUT,
            rebalance_windows: Arc::new(Mutex::new(BTreeMap::new())),
            fallbacks: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Override the flat forward deadline (#190). A deployment whose pooled
    /// groups are large enough to need longer than the default can raise it
    /// without a rebuild.
    pub fn with_call_timeout(self, call_timeout: Duration) -> Self {
        Self {
            call_timeout,
            ..self
        }
    }

    /// How many forwards have fallen back to local processing. Mirrors the
    /// `outcome="fallback"` counter for in-process observation.
    pub fn fallbacks(&self) -> u64 {
        self.fallbacks.load(Ordering::Relaxed)
    }

    /// Remember what rebalance window this group's members granted themselves
    /// (#190), so `sync` can spend it rather than a flat constant.
    fn note_rebalance_window(&self, group_id: &str, window: Duration) {
        if let Ok(mut windows) = self.rebalance_windows.lock() {
            _ = windows.insert(group_id.to_owned(), window);
        }
    }

    /// The deadline for forwarding this group's `sync`.
    ///
    /// A member that granted itself `rebalance_timeout_ms` for the whole
    /// rebalance should let the owner use it: `sync` carries members ×
    /// subscribed topics, and at the scale in #190 the owner legitimately needs
    /// longer than the flat 15s that applied before. Falls back to
    /// `call_timeout` for a group whose join this replica never saw, and never
    /// goes below it — this only ever lengthens the deadline.
    fn sync_deadline(&self, group_id: &str) -> Duration {
        self.rebalance_windows
            .lock()
            .ok()
            .and_then(|windows| windows.get(group_id).copied())
            .map_or(self.call_timeout, |window| window.max(self.call_timeout))
    }

    /// Forward `body` to the owner of `group_id` if that owner is another
    /// replica, bounded by `deadline`.
    ///
    /// See [`Forwarded`] for why the caller must treat a timeout and a transport
    /// error differently rather than both meaning "process locally".
    ///
    /// `payload_bytes` is carried only so the timeout warning can report how
    /// big the request was: a deadline that is too small is impossible to size
    /// without knowing what it was too small *for* (#190).
    async fn forwarded(
        &self,
        what: Forwardee<'_>,
        group_id: &str,
        body: Body,
        deadline: Duration,
    ) -> Forwarded {
        let Forwardee {
            api,
            api_key,
            client_id,
            payload_bytes,
            mutating,
        } = what;

        let owner = self.peers.owner(group_id);

        if owner == self.peers.self_ip() {
            GROUP_FORWARD_TOTAL.add(
                1,
                &[
                    KeyValue::new("api", api),
                    KeyValue::new("outcome", "local_owner"),
                ],
            );

            return Forwarded::Local;
        }

        self.forward.retain(&self.peers.peers());

        let (kind, outcome, decision) = match tokio::time::timeout(
            deadline,
            self.forward.call(owner, api_key, client_id, body),
        )
        .await
        {
            Ok(Ok(response)) => {
                GROUP_FORWARD_TOTAL.add(
                    1,
                    &[
                        KeyValue::new("api", api),
                        KeyValue::new("outcome", "forwarded"),
                    ],
                );

                return Forwarded::Response(response);
            }

            // The owner may already be gone: local processing is the backstop.
            Ok(Err(error)) => {
                warn!(api, group_id, %owner, %error, "group forward failed; processing locally");
                ("forward", "fallback", Forwarded::Local)
            }

            // The owner is up and still working. Racing it is only a problem if
            // serving locally would write.
            Err(_elapsed) if mutating => {
                warn!(
                    api,
                    group_id,
                    %owner,
                    ?deadline,
                    payload_bytes,
                    "group forward timed out; owner is up, telling the client to retry"
                );
                ("timeout", "owner_slow", Forwarded::Retry)
            }

            Err(_elapsed) => {
                warn!(api, group_id, %owner, ?deadline, "group forward timed out; processing locally");
                ("timeout", "fallback", Forwarded::Local)
            }
        };

        GROUP_FORWARD_ERRORS.add(1, &[KeyValue::new("kind", kind)]);
        GROUP_FORWARD_TOTAL.add(
            1,
            &[KeyValue::new("api", api), KeyValue::new("outcome", outcome)],
        );

        if matches!(decision, Forwarded::Local) {
            _ = self.fallbacks.fetch_add(1, Ordering::Relaxed);
        }

        decision
    }
}

#[async_trait]
impl<C> Coordinator for ForwardingCoordinator<C>
where
    C: Coordinator,
{
    #[allow(clippy::too_many_arguments)]
    async fn join(
        &self,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> Result<Body> {
        // the owner may hold the join for the whole join window.
        let deadline = Duration::from_millis(
            u64::try_from(rebalance_timeout_ms.unwrap_or(session_timeout_ms)).unwrap_or_default(),
        ) + self.join_timeout_slack;

        // The same window the owner may spend on this group's `sync` (#190).
        self.note_rebalance_window(group_id, deadline);

        let request = JoinGroupRequest::default()
            .group_id(group_id.into())
            .session_timeout_ms(session_timeout_ms)
            .rebalance_timeout_ms(rebalance_timeout_ms)
            .member_id(member_id.into())
            .group_instance_id(group_instance_id.map(ToOwned::to_owned))
            .protocol_type(protocol_type.into())
            .protocols(protocols.map(<[JoinGroupRequestProtocol]>::to_vec))
            .reason(reason.map(ToOwned::to_owned));

        // join forwards the caller's own client id: the owner derives the
        // member id from it.
        match self
            .forwarded(
                Forwardee {
                    api: "join",
                    api_key: JoinGroupRequest::KEY,
                    client_id,
                    payload_bytes: None,
                    mutating: true,
                },
                group_id,
                request.into(),
                deadline,
            )
            .await
        {
            Forwarded::Response(response) => return Ok(response),
            Forwarded::Retry => return Err(Error::Api(ErrorCode::NotCoordinator)),
            Forwarded::Local => {}
        }

        self.local
            .join(
                client_id,
                group_id,
                session_timeout_ms,
                rebalance_timeout_ms,
                member_id,
                group_instance_id,
                protocol_type,
                protocols,
                reason,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn sync(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body> {
        // Reported on a timeout so the deadline can be sized against what it
        // was too small for, rather than guessed (#190).
        let assignment_bytes = assignments.map_or(0, |assignments| {
            assignments
                .iter()
                .map(|assignment| assignment.assignment.len())
                .sum()
        });

        let request = SyncGroupRequest::default()
            .group_id(group_id.into())
            .generation_id(generation_id)
            .member_id(member_id.into())
            .group_instance_id(group_instance_id.map(ToOwned::to_owned))
            .protocol_type(protocol_type.map(ToOwned::to_owned))
            .protocol_name(protocol_name.map(ToOwned::to_owned))
            .assignments(assignments.map(<[SyncGroupRequestAssignment]>::to_vec));

        match self
            .forwarded(
                Forwardee {
                    api: "sync",
                    api_key: SyncGroupRequest::KEY,
                    client_id: Some(FORWARD_CLIENT_ID),
                    payload_bytes: Some(assignment_bytes),
                    mutating: true,
                },
                group_id,
                request.into(),
                self.sync_deadline(group_id),
            )
            .await
        {
            Forwarded::Response(response) => return Ok(response),
            Forwarded::Retry => return Err(Error::Api(ErrorCode::NotCoordinator)),
            Forwarded::Local => {}
        }

        self.local
            .sync(
                group_id,
                generation_id,
                member_id,
                group_instance_id,
                protocol_type,
                protocol_name,
                assignments,
            )
            .await
    }

    async fn heartbeat(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body> {
        let request = HeartbeatRequest::default()
            .group_id(group_id.into())
            .generation_id(generation_id)
            .member_id(member_id.into())
            .group_instance_id(group_instance_id.map(ToOwned::to_owned));

        // Heartbeat keeps the local fallback on both failure shapes: it is
        // small, frequent, and a missed one costs a member its session, which is
        // worse than the write it makes.
        if let Forwarded::Response(response) = self
            .forwarded(
                Forwardee {
                    api: "heartbeat",
                    api_key: HeartbeatRequest::KEY,
                    client_id: Some(FORWARD_CLIENT_ID),
                    payload_bytes: None,
                    mutating: false,
                },
                group_id,
                request.into(),
                self.call_timeout,
            )
            .await
        {
            return Ok(response);
        }

        self.local
            .heartbeat(group_id, generation_id, member_id, group_instance_id)
            .await
    }

    async fn leave(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body> {
        // the forward encodes at the negotiated (v3+) version whose only
        // member field is `members`; a pre-v3 caller supplies `member_id`
        // alone, so lift it into `members` to survive the version hop.
        let forwarded_members = members.map(<[MemberIdentity]>::to_vec).or_else(|| {
            member_id.map(|member_id| vec![MemberIdentity::default().member_id(member_id.into())])
        });

        let request = LeaveGroupRequest::default()
            .group_id(group_id.into())
            .member_id(member_id.map(ToOwned::to_owned))
            .members(forwarded_members);

        match self
            .forwarded(
                Forwardee {
                    api: "leave",
                    api_key: LeaveGroupRequest::KEY,
                    client_id: Some(FORWARD_CLIENT_ID),
                    payload_bytes: None,
                    mutating: true,
                },
                group_id,
                request.into(),
                self.call_timeout,
            )
            .await
        {
            Forwarded::Response(response) => return Ok(response),
            Forwarded::Retry => return Err(Error::Api(ErrorCode::NotCoordinator)),
            Forwarded::Local => {}
        }

        self.local.leave(group_id, member_id, members).await
    }

    async fn offset_commit(&self, detail: OffsetCommit<'_>) -> Result<Body> {
        // retention_time_ms (v2-4 only) is not encodable at the negotiated
        // version; the local coordinator ignores it, so nothing is lost.
        let request = OffsetCommitRequest::default()
            .group_id(detail.group_id.into())
            .generation_id_or_member_epoch(detail.generation_id_or_member_epoch)
            .member_id(detail.member_id.map(ToOwned::to_owned))
            .group_instance_id(detail.group_instance_id.map(ToOwned::to_owned))
            .retention_time_ms(detail.retention_time_ms)
            .topics(detail.topics.map(<[OffsetCommitRequestTopic]>::to_vec));

        match self
            .forwarded(
                Forwardee {
                    api: "offset_commit",
                    api_key: OffsetCommitRequest::KEY,
                    client_id: Some(FORWARD_CLIENT_ID),
                    payload_bytes: None,
                    mutating: true,
                },
                detail.group_id,
                request.into(),
                self.call_timeout,
            )
            .await
        {
            Forwarded::Response(response) => return Ok(response),
            // Committing locally against a live owner is how an offset write is
            // lost to a CAS conflict (#190) — the very symptom that issue
            // reports. Retry against the real owner instead.
            Forwarded::Retry => return Err(Error::Api(ErrorCode::NotCoordinator)),
            Forwarded::Local => {}
        }

        self.local.offset_commit(detail).await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        // ownership is per group: forward only when the request names one
        // group (the pre-v8 `group_id` field, or a single-entry v8+ `groups`
        // array). A multi-group fetch spans owners, so process it locally —
        // offset_fetch is read-only, correctness does not need the owner.
        let target = match (group_id, groups) {
            (Some(group_id), None) => Some(group_id),
            (None, Some([group])) => Some(group.group_id.as_str()),
            _ => None,
        };

        if let Some(target) = target {
            let request = OffsetFetchRequest::default()
                .group_id(group_id.map(ToOwned::to_owned))
                .topics(topics.map(<[OffsetFetchRequestTopic]>::to_vec))
                .groups(groups.map(<[OffsetFetchRequestGroup]>::to_vec))
                .require_stable(require_stable);

            // Read-only, so serving it locally cannot create a second writer:
            // the local fallback stays for both failure shapes.
            if let Forwarded::Response(response) = self
                .forwarded(
                    Forwardee {
                        api: "offset_fetch",
                        api_key: OffsetFetchRequest::KEY,
                        client_id: Some(FORWARD_CLIENT_ID),
                        payload_bytes: None,
                        mutating: false,
                    },
                    target,
                    request.into(),
                    self.call_timeout,
                )
                .await
            {
                return Ok(response);
            }
        }

        self.local
            .offset_fetch(group_id, topics, groups, require_stable)
            .await
    }
}

/// The group coordinator the broker is built with: pure-local (today's
/// behaviour, the default) or forward-to-owner. Enum dispatch — mirroring
/// `StorageContainer` — keeps `Broker<GroupCoordinator<O>, O>` one concrete
/// type so nothing downstream of `Builder::build` changes with the flag.
#[derive(Clone, Debug)]
pub enum GroupCoordinator<O> {
    Local(Controller<O>),
    Forwarding(ForwardingCoordinator<Controller<O>>),
}

impl<O> GroupCoordinator<O>
where
    O: Storage + Clone,
{
    /// Today's behaviour: every group API call is processed by the local
    /// [`Controller`].
    pub fn local(controller: Controller<O>) -> Self {
        Self::Local(controller)
    }

    /// Forward-to-owner coordination over `registry`, dialling each owner's
    /// `internal_port`.
    pub fn forwarding(
        controller: Controller<O>,
        registry: Arc<PeerRegistry>,
        internal_port: u16,
    ) -> Self {
        Self::Forwarding(ForwardingCoordinator::new(
            controller,
            registry,
            internal_port,
        ))
    }
}

#[async_trait]
impl<O> Coordinator for GroupCoordinator<O>
where
    O: Storage + Clone,
{
    #[allow(clippy::too_many_arguments)]
    async fn join(
        &self,
        client_id: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> Result<Body> {
        match self {
            Self::Local(controller) => {
                controller
                    .join(
                        client_id,
                        group_id,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocols,
                        reason,
                    )
                    .await
            }

            Self::Forwarding(forwarding) => {
                forwarding
                    .join(
                        client_id,
                        group_id,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocols,
                        reason,
                    )
                    .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn sync(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body> {
        match self {
            Self::Local(controller) => {
                controller
                    .sync(
                        group_id,
                        generation_id,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocol_name,
                        assignments,
                    )
                    .await
            }

            Self::Forwarding(forwarding) => {
                forwarding
                    .sync(
                        group_id,
                        generation_id,
                        member_id,
                        group_instance_id,
                        protocol_type,
                        protocol_name,
                        assignments,
                    )
                    .await
            }
        }
    }

    async fn heartbeat(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body> {
        match self {
            Self::Local(controller) => {
                controller
                    .heartbeat(group_id, generation_id, member_id, group_instance_id)
                    .await
            }

            Self::Forwarding(forwarding) => {
                forwarding
                    .heartbeat(group_id, generation_id, member_id, group_instance_id)
                    .await
            }
        }
    }

    async fn leave(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body> {
        match self {
            Self::Local(controller) => controller.leave(group_id, member_id, members).await,
            Self::Forwarding(forwarding) => forwarding.leave(group_id, member_id, members).await,
        }
    }

    async fn offset_commit(&self, detail: OffsetCommit<'_>) -> Result<Body> {
        match self {
            Self::Local(controller) => controller.offset_commit(detail).await,
            Self::Forwarding(forwarding) => forwarding.offset_commit(detail).await,
        }
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        match self {
            Self::Local(controller) => {
                controller
                    .offset_fetch(group_id, topics, groups, require_stable)
                    .await
            }

            Self::Forwarding(forwarding) => {
                forwarding
                    .offset_fetch(group_id, topics, groups, require_stable)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{SeedableRng, prelude::*};
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn peer(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, last))
    }

    fn group_ids(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("group-{i}")).collect()
    }

    #[test]
    fn fnv1a_64_golden_vectors() {
        // Published FNV-1a 64 test vectors. These lock the hash across
        // refactors, Rust versions and pods: if any of these change, owner
        // placement changes fleet-wide during a rolling upgrade.
        assert_eq!(0xcbf2_9ce4_8422_2325, fnv1a_64(b""));
        assert_eq!(0xaf63_dc4c_8601_ec8c, fnv1a_64(b"a"));
        assert_eq!(0x8594_4171_f739_67e8, fnv1a_64(b"foobar"));
        assert_eq!(0x980f_eb29_c5a2_796c, fnv1a_64(b"tansu"));
    }

    #[test]
    fn rendezvous_score_golden_vectors() {
        // Locks the full score pipeline (FNV-1a fold + splitmix64
        // finalizer): a change here reshuffles group ownership fleet-wide.
        assert_eq!(0x8897_4c77_96b5_efe1, rendezvous_score("group-0", &peer(1)));
        assert_eq!(
            0xd76a_768b_aea3_2195,
            rendezvous_score("consumers", &IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)))
        );
    }

    #[test]
    fn owner_of_empty_peer_set_is_self() {
        let self_ip = peer(99);

        assert_eq!(self_ip, owner("some-group", self_ip, &[]));
        assert_eq!(self_ip, owner("", self_ip, &[]));
    }

    #[test]
    fn owner_is_deterministic_under_peer_order() {
        let peers = (1..=10).map(peer).collect::<Vec<_>>();
        let self_ip = peer(200);
        let mut rng = StdRng::seed_from_u64(0x0074_616e_7375);

        for group_id in group_ids(250) {
            let expected = owner(&group_id, self_ip, &peers);

            for _ in 0..5 {
                let mut shuffled = peers.clone();
                shuffled.shuffle(&mut rng);

                assert_eq!(expected, owner(&group_id, self_ip, &shuffled));
            }
        }
    }

    #[test]
    fn owner_handles_ipv6_peers() {
        let peers = (1..=4)
            .map(|i| IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, i)))
            .collect::<Vec<_>>();
        let self_ip = peer(200);

        for group_id in group_ids(50) {
            let elected = owner(&group_id, self_ip, &peers);
            // self_ip is always a candidate, so the winner may be self even
            // when it is absent from `peers`.
            assert!(elected == self_ip || peers.contains(&elected));
        }
    }

    #[test]
    fn rendezvous_removal_moves_only_the_removed_peers_groups() {
        let peers = (1..=10).map(peer).collect::<Vec<_>>();
        // self is one of the peers here: this test is about peer
        // redistribution, not self-candidacy (see
        // `owner_considers_self_even_when_absent_from_peers`).
        let self_ip = peer(1);
        let removed = peer(4);
        let survivors = peers
            .iter()
            .copied()
            .filter(|p| *p != removed)
            .collect::<Vec<_>>();

        let groups = group_ids(2_000);

        let before = groups
            .iter()
            .map(|g| (g.clone(), owner(g, self_ip, &peers)))
            .collect::<BTreeMap<_, _>>();

        let after = groups
            .iter()
            .map(|g| (g.clone(), owner(g, self_ip, &survivors)))
            .collect::<BTreeMap<_, _>>();

        let mut moved = 0;

        for group in &groups {
            if before[group] == removed {
                moved += 1;
                assert_ne!(removed, after[group]);
                assert!(survivors.contains(&after[group]));
            } else {
                // rendezvous invariant: every group not owned by the removed
                // peer keeps its owner.
                assert_eq!(before[group], after[group]);
            }
        }

        // the removed peer owned ~1/10 of the groups; loose bounds guard
        // against a degenerate hash distribution.
        assert!(
            (100..=350).contains(&moved),
            "expected ~200 of 2000 groups to move, got {moved}"
        );
    }

    #[test]
    fn owners_are_reasonably_distributed() {
        let peers = (1..=10).map(peer).collect::<Vec<_>>();
        // self is one of the peers here (this test measures peer distribution).
        let self_ip = peer(1);
        let mut per_owner = BTreeMap::<IpAddr, usize>::new();

        for group_id in group_ids(2_000) {
            *per_owner
                .entry(owner(&group_id, self_ip, &peers))
                .or_default() += 1;
        }

        assert_eq!(10, per_owner.len(), "every peer should own some groups");

        for (owner, n) in per_owner {
            assert!(
                (100..=350).contains(&n),
                "peer {owner} owns {n} of 2000 groups; expected ~200"
            );
        }
    }

    #[test]
    fn owner_considers_self_even_when_absent_from_peers() {
        // Startup/rollout window: the headless Service has published this pod
        // to its peers' DNS, but this pod's own resolver has not yet listed
        // itself, so `peers` excludes `self_ip`. Self must still win the groups
        // that rendezvous to it — otherwise this replica forwards its own
        // groups away while its peers route them back to it (double-ownership).
        let self_ip = peer(200); // deliberately NOT in `peers`
        let peers = (1..=10).map(peer).collect::<Vec<_>>();
        assert!(!peers.contains(&self_ip));

        let self_owned = group_ids(2_000)
            .into_iter()
            .filter(|g| owner(g, self_ip, &peers) == self_ip)
            .count();

        // self is a genuine 11th candidate: it must win a roughly fair share,
        // never zero (the pre-fix bug) and never all.
        assert!(
            (100..=320).contains(&self_owned),
            "self owns {self_owned} of 2000 groups; expected ~182 (1/11)"
        );
    }

    #[test]
    fn registry_owner_and_is_local_agree() {
        let self_ip = peer(3);
        let registry = PeerRegistry::new(self_ip, "unused.invalid", Duration::from_secs(5));

        // empty peer set: everything is local.
        assert!(registry.peers().is_empty());
        assert!(registry.is_local("some-group"));
        assert_eq!(self_ip, registry.owner("some-group"));

        registry.set_peers((1..=10).map(peer).collect());
        assert_eq!(10, registry.peers().len());

        let mut local = 0;

        for group_id in group_ids(500) {
            let elected = registry.owner(&group_id);

            assert!(registry.peers().contains(&elected));
            assert_eq!(elected == self_ip, registry.is_local(&group_id));

            if registry.is_local(&group_id) {
                local += 1;
            }
        }

        assert!(
            (10..=120).contains(&local),
            "self owns {local} of 500 groups; expected ~50"
        );
    }

    #[test]
    fn set_peers_sorts_and_deduplicates() {
        let registry = PeerRegistry::new(peer(1), "unused.invalid", Duration::from_secs(5));

        registry.set_peers(vec![peer(3), peer(1), peer(3), peer(2)]);

        assert_eq!(vec![peer(1), peer(2), peer(3)], *registry.peers());
    }

    #[tokio::test]
    async fn refresh_resolves_localhost() -> Result<()> {
        let registry = PeerRegistry::new(peer(1), "localhost", Duration::from_secs(5));

        let n = registry.refresh().await?;

        assert!(n >= 1);
        assert!(registry.peers().iter().all(|ip| ip.is_loopback()));

        Ok(())
    }

    #[tokio::test]
    async fn refresh_failure_clears_peers() {
        // RFC 2606 reserves .invalid: resolution must fail.
        let registry = PeerRegistry::new(
            peer(1),
            "peer-discovery.does-not-exist.invalid",
            Duration::from_secs(5),
        );

        registry.set_peers((1..=3).map(peer).collect());
        assert_eq!(3, registry.peers().len());

        assert!(registry.refresh().await.is_err());

        assert!(registry.peers().is_empty());
        assert!(registry.is_local("any-group"));
    }

    /// Coordinator-level tests need the in-memory object store, which — like
    /// the existing `tests/cg*.rs` in-memory suites — is only built
    /// workspace-wide with `--all-features` (the `dynostore` feature of
    /// `tansu-storage` is enabled through the `tansu` crate).
    #[cfg(feature = "dynostore")]
    mod coordination {
        use super::*;
        use crate::Error;
        use bytes::Bytes;
        use tansu_sans_io::{
            ErrorCode, HeartbeatResponse, JoinGroupResponse, LeaveGroupResponse,
            OffsetCommitResponse, OffsetFetchResponse, SyncGroupResponse,
            create_topics_request::CreatableTopic,
            offset_commit_request::OffsetCommitRequestPartition,
        };
        use tansu_storage::{BrokerRegistrationRequest, StorageContainer};
        use uuid::Uuid;

        const CLIENT_ID: &str = "console-consumer";
        const PROTOCOL_TYPE: &str = "consumer";
        const RANGE: &str = "range";
        const SESSION_TIMEOUT_MS: i32 = 45_000;
        const REBALANCE_TIMEOUT_MS: Option<i32> = Some(300_000);

        async fn storage_container() -> Result<Arc<Box<dyn Storage>>> {
            let cluster_id = Uuid::now_v7().to_string();

            let storage = StorageContainer::builder()
                .cluster_id(cluster_id.clone())
                .node_id(111)
                .advertised_listener(Url::parse("tcp://localhost:9092")?)
                .storage(Url::parse("memory://")?)
                .build()
                .await?;

            storage
                .register_broker(BrokerRegistrationRequest {
                    broker_id: 111,
                    cluster_id,
                    incarnation_id: Uuid::now_v7(),
                    rack: None,
                })
                .await?;

            Ok(storage)
        }

        async fn join_as_leader<C>(coordinator: &C, group_id: &str) -> Result<JoinGroupResponse>
        where
            C: Coordinator,
        {
            let protocols = [JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(Bytes::from_static(b"range_meta_01"))];

            // a dynamic join without a member id is rejected with the member id
            // to rejoin with.
            let required = JoinGroupResponse::try_from(
                coordinator
                    .join(
                        Some(CLIENT_ID),
                        group_id,
                        SESSION_TIMEOUT_MS,
                        REBALANCE_TIMEOUT_MS,
                        "",
                        None,
                        PROTOCOL_TYPE,
                        Some(&protocols[..]),
                        None,
                    )
                    .await?,
            )?;

            assert_eq!(
                ErrorCode::MemberIdRequired,
                ErrorCode::try_from(required.error_code)?
            );
            assert!(required.member_id.starts_with(CLIENT_ID));

            let joined = JoinGroupResponse::try_from(
                coordinator
                    .join(
                        Some(CLIENT_ID),
                        group_id,
                        SESSION_TIMEOUT_MS,
                        REBALANCE_TIMEOUT_MS,
                        required.member_id.as_str(),
                        None,
                        PROTOCOL_TYPE,
                        Some(&protocols[..]),
                        None,
                    )
                    .await?,
            )?;

            assert_eq!(ErrorCode::None, ErrorCode::try_from(joined.error_code)?);
            assert_eq!(joined.member_id, joined.leader);

            Ok(joined)
        }

        // The Local variant is today's behaviour: the full single-member group
        // lifecycle — all 6 Coordinator APIs — delegates to the wrapped
        // Controller unchanged.
        #[tokio::test]
        async fn local_group_coordinator_runs_the_full_lifecycle() -> Result<()> {
            let storage = storage_container().await?;

            let topic = alphanumeric(15);
            _ = storage
                .create_topic(
                    CreatableTopic::default()
                        .name(topic.clone())
                        .num_partitions(1)
                        .replication_factor(0)
                        .assignments(Some([].into()))
                        .configs(Some([].into())),
                    false,
                )
                .await?;

            let coordinator = GroupCoordinator::local(Controller::with_storage(storage)?);

            let group_id = alphanumeric(15);

            // join
            let joined = join_as_leader(&coordinator, group_id.as_str()).await?;
            let member_id = joined.member_id.clone();
            let generation_id = joined.generation_id;

            // sync, as leader, with an assignment
            let assignment = Bytes::from_static(b"assignment_01");

            let synced = SyncGroupResponse::try_from(
                coordinator
                    .sync(
                        group_id.as_str(),
                        generation_id,
                        member_id.as_str(),
                        None,
                        Some(PROTOCOL_TYPE),
                        Some(RANGE),
                        Some(&[SyncGroupRequestAssignment::default()
                            .member_id(member_id.clone())
                            .assignment(assignment.clone())]),
                    )
                    .await?,
            )?;

            assert_eq!(ErrorCode::None, ErrorCode::try_from(synced.error_code)?);
            assert_eq!(assignment, synced.assignment);

            // heartbeat
            let heartbeat = HeartbeatResponse::try_from(
                coordinator
                    .heartbeat(group_id.as_str(), generation_id, member_id.as_str(), None)
                    .await?,
            )?;

            assert_eq!(ErrorCode::None, ErrorCode::try_from(heartbeat.error_code)?);

            // offset commit
            let committed_offset = 32123;

            let committed = OffsetCommitResponse::try_from(
                coordinator
                    .offset_commit(OffsetCommit {
                        group_id: group_id.as_str(),
                        generation_id_or_member_epoch: Some(generation_id),
                        member_id: Some(member_id.as_str()),
                        group_instance_id: None,
                        retention_time_ms: None,
                        topics: Some(&[OffsetCommitRequestTopic::default()
                            .name(topic.clone())
                            .partitions(Some(vec![
                                OffsetCommitRequestPartition::default()
                                    .partition_index(0)
                                    .committed_offset(committed_offset),
                            ]))]),
                    })
                    .await?,
            )?;

            let committed_partitions = committed
                .topics
                .into_iter()
                .flatten()
                .flat_map(|topic| topic.partitions.into_iter().flatten())
                .collect::<Vec<_>>();
            assert_eq!(1, committed_partitions.len());
            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(committed_partitions[0].error_code)?
            );

            // offset fetch reads the commit back
            let fetched = OffsetFetchResponse::try_from(
                coordinator
                    .offset_fetch(
                        Some(group_id.as_str()),
                        Some(&[OffsetFetchRequestTopic::default()
                            .name(topic.clone())
                            .partition_indexes(Some(vec![0]))]),
                        None,
                        Some(false),
                    )
                    .await?,
            )?;

            let fetched_partitions = fetched
                .topics
                .into_iter()
                .flatten()
                .flat_map(|topic| topic.partitions.into_iter().flatten())
                .collect::<Vec<_>>();
            assert_eq!(1, fetched_partitions.len());
            assert_eq!(committed_offset, fetched_partitions[0].committed_offset);

            // leave
            let left = LeaveGroupResponse::try_from(
                coordinator
                    .leave(
                        group_id.as_str(),
                        None,
                        Some(&[MemberIdentity::default()
                            .member_id(member_id.clone())
                            .group_instance_id(None)
                            .reason(Some("the consumer is being closed".into()))]),
                    )
                    .await?,
            )?;

            assert_eq!(ErrorCode::None, ErrorCode::try_from(left.error_code)?);

            Ok(())
        }

        fn alphanumeric(length: usize) -> String {
            rand::rng()
                .sample_iter(&rand::distr::Alphanumeric)
                .take(length)
                .map(char::from)
                .collect()
        }

        #[derive(Debug)]
        enum StubBehaviour {
            Respond(Body),
            Fail,
            Hang,
        }

        /// A [`Forward`] stub: records calls and responds, fails or hangs.
        #[derive(Debug)]
        struct StubForward {
            calls: AtomicU64,
            behaviour: StubBehaviour,
        }

        impl StubForward {
            fn new(behaviour: StubBehaviour) -> Arc<Self> {
                Arc::new(Self {
                    calls: AtomicU64::new(0),
                    behaviour,
                })
            }

            fn calls(&self) -> u64 {
                self.calls.load(Ordering::Relaxed)
            }
        }

        #[async_trait]
        impl Forward for StubForward {
            async fn call(
                &self,
                _owner: IpAddr,
                _api_key: i16,
                _client_id: Option<&str>,
                _body: Body,
            ) -> Result<Body> {
                _ = self.calls.fetch_add(1, Ordering::Relaxed);

                match &self.behaviour {
                    StubBehaviour::Respond(body) => Ok(body.clone()),
                    StubBehaviour::Fail => Err(Error::Message("stubbed forward failure".into())),
                    StubBehaviour::Hang => {
                        std::future::pending::<()>().await;
                        unreachable!()
                    }
                }
            }
        }

        /// A registry whose only peer is this replica: every group is local.
        fn registry_owning_everything(self_ip: IpAddr) -> Arc<PeerRegistry> {
            let registry = Arc::new(PeerRegistry::new(
                self_ip,
                "unused.invalid",
                Duration::from_secs(5),
            ));
            registry.set_peers(vec![self_ip]);
            registry
        }

        /// A registry whose only peer is *another* replica: every group is
        /// owned by that peer.
        fn registry_owning_nothing(self_ip: IpAddr, owner: IpAddr) -> Arc<PeerRegistry> {
            let registry = Arc::new(PeerRegistry::new(
                self_ip,
                "unused.invalid",
                Duration::from_secs(5),
            ));
            registry.set_peers(vec![owner]);
            registry
        }

        // Owner is self: the request is processed by the wrapped Controller and
        // the forward hop is never attempted.
        #[tokio::test]
        async fn forwarding_processes_locally_when_owner_is_self() -> Result<()> {
            let storage = storage_container().await?;
            let stub = StubForward::new(StubBehaviour::Fail);

            let coordinator = ForwardingCoordinator::with_forward(
                Controller::with_storage(storage)?,
                registry_owning_everything(peer(1)),
                stub.clone(),
            );

            let group_id = alphanumeric(15);
            _ = join_as_leader(&coordinator, group_id.as_str()).await?;

            assert_eq!(0, stub.calls());
            assert_eq!(0, coordinator.fallbacks());

            Ok(())
        }

        // Owner is a peer: the request is forwarded and the owner's response is
        // relayed verbatim, without touching the local Controller.
        #[tokio::test]
        async fn forwarding_relays_the_owners_response_when_owner_is_a_peer() -> Result<()> {
            let storage = storage_container().await?;

            let canned = HeartbeatResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::RebalanceInProgress.into());
            let stub = StubForward::new(StubBehaviour::Respond(canned.clone().into()));

            let coordinator = ForwardingCoordinator::with_forward(
                Controller::with_storage(storage)?,
                registry_owning_nothing(peer(1), peer(2)),
                stub.clone(),
            );

            // a heartbeat for a group the local Controller has never seen: only
            // the stubbed owner can produce RebalanceInProgress.
            let response = HeartbeatResponse::try_from(
                coordinator
                    .heartbeat("some-group", 0, "some-member", None)
                    .await?,
            )?;

            assert_eq!(
                ErrorCode::RebalanceInProgress,
                ErrorCode::try_from(response.error_code)?
            );
            assert_eq!(1, stub.calls());
            assert_eq!(0, coordinator.fallbacks());

            Ok(())
        }

        // A failed forward falls back to local processing (the etag-CAS
        // correctness backstop) and the fallback tally increments.
        #[tokio::test]
        async fn forwarding_failure_falls_back_to_local() -> Result<()> {
            let storage = storage_container().await?;
            let stub = StubForward::new(StubBehaviour::Fail);

            let owner_ip = peer(2);
            let registry = registry_owning_nothing(peer(1), owner_ip);

            // self is always an owner candidate, so a random group could be
            // self-owned (processed locally, never forwarded). Pick one the
            // registry routes to the peer, so the forward-then-fail path is
            // exercised deterministically.
            let group_id = (0u32..)
                .map(|i| format!("fallback-group-{i}"))
                .find(|group_id| registry.owner(group_id) == owner_ip)
                .expect("a peer-owned group exists");

            let coordinator = ForwardingCoordinator::with_forward(
                Controller::with_storage(storage)?,
                registry,
                stub.clone(),
            );

            // despite the owner being unreachable, the join is served by the
            // local Controller exactly as today.
            _ = join_as_leader(&coordinator, group_id.as_str()).await?;

            // one forward attempt per join call (member-id-required + rejoin).
            assert_eq!(2, stub.calls());
            assert_eq!(2, coordinator.fallbacks());

            Ok(())
        }

        /// #190: a `sync` whose owner is up but slow must not be served
        /// locally.
        ///
        /// The owner comes from a headless-Service DNS lookup, which lists only
        /// ready pods, so a named owner is a live one and a timeout means it is
        /// still working on this same generation. Serving the request here too
        /// puts a second writer on one group document: the group-state CAS then
        /// conflicts, and the members that lose are told
        /// `REBALANCE_IN_PROGRESS` on every commit cycle — the permanent
        /// rebalance churn #190 reports, with the dropped offset commits that
        /// come with it.
        ///
        /// The client is told `NOT_COORDINATOR` instead, which is retriable and
        /// sends it back to the real owner. `fallbacks` stays zero because
        /// nothing fell back.
        #[tokio::test(start_paused = true)]
        async fn slow_owner_does_not_get_a_second_writer_on_sync() -> Result<()> {
            let storage = storage_container().await?;
            let stub = StubForward::new(StubBehaviour::Hang);

            let owner_ip = peer(2);
            let registry = registry_owning_nothing(peer(1), owner_ip);
            let group_id = (0u32..)
                .map(|i| format!("slow-owner-group-{i}"))
                .find(|group_id| registry.owner(group_id) == owner_ip)
                .expect("a peer-owned group exists");

            let coordinator = ForwardingCoordinator::with_forward(
                Controller::with_storage(storage)?,
                registry,
                stub.clone(),
            );

            let error = coordinator
                .sync(group_id.as_str(), 0, "some-member", None, None, None, None)
                .await
                .expect_err("a live-but-slow owner must not be raced");

            assert!(
                matches!(error, Error::Api(ErrorCode::NotCoordinator)),
                "expected a retriable NOT_COORDINATOR, got {error:?}",
            );
            assert_eq!(1, stub.calls());
            assert_eq!(
                0,
                coordinator.fallbacks(),
                "nothing fell back: the request was refused, not processed twice",
            );

            Ok(())
        }

        /// A transport failure is the opposite case and keeps the backstop
        /// (#190): the owner may have gone between the DNS refresh and the
        /// call, so refusing would strand the group.
        #[tokio::test]
        async fn unreachable_owner_still_falls_back_on_sync() -> Result<()> {
            let storage = storage_container().await?;
            let stub = StubForward::new(StubBehaviour::Fail);

            let owner_ip = peer(2);
            let registry = registry_owning_nothing(peer(1), owner_ip);
            let group_id = (0u32..)
                .map(|i| format!("gone-owner-group-{i}"))
                .find(|group_id| registry.owner(group_id) == owner_ip)
                .expect("a peer-owned group exists");

            let coordinator = ForwardingCoordinator::with_forward(
                Controller::with_storage(storage)?,
                registry,
                stub.clone(),
            );

            // Served by the local Controller, exactly as before.
            _ = coordinator
                .sync(group_id.as_str(), 0, "some-member", None, None, None, None)
                .await?;

            assert_eq!(1, stub.calls());
            assert_eq!(1, coordinator.fallbacks());

            Ok(())
        }

        /// #190: `sync` spends the rebalance window its members granted
        /// themselves, not a flat constant.
        ///
        /// `sync` carries the whole assignment — members × subscribed topics —
        /// so it is the largest group request, and before this it ran on the
        /// smallest deadline. The window is learned from the `join` this
        /// replica forwarded, since `sync`'s own signature does not carry it.
        #[tokio::test]
        async fn sync_spends_the_rebalance_window_the_members_asked_for() -> Result<()> {
            let storage = storage_container().await?;
            let stub = StubForward::new(StubBehaviour::Fail);

            let owner_ip = peer(2);
            let registry = registry_owning_nothing(peer(1), owner_ip);
            let group_id = (0u32..)
                .map(|i| format!("window-group-{i}"))
                .find(|group_id| registry.owner(group_id) == owner_ip)
                .expect("a peer-owned group exists");

            let coordinator = ForwardingCoordinator::with_forward(
                Controller::with_storage(storage)?,
                registry,
                stub.clone(),
            );

            // Unknown group: the flat timeout, which is what applied to every
            // sync before this.
            assert_eq!(
                CALL_TIMEOUT,
                coordinator.sync_deadline(group_id.as_str()),
                "an unseen group falls back to the flat deadline",
            );

            _ = join_as_leader(&coordinator, group_id.as_str()).await?;

            let expected = Duration::from_millis(
                u64::try_from(REBALANCE_TIMEOUT_MS.unwrap_or(SESSION_TIMEOUT_MS))
                    .unwrap_or_default(),
            ) + JOIN_TIMEOUT_SLACK;

            assert_eq!(
                expected,
                coordinator.sync_deadline(group_id.as_str()),
                "after a join, sync gets the window the members granted",
            );
            assert!(
                coordinator.sync_deadline(group_id.as_str()) > CALL_TIMEOUT,
                "which is longer than the flat deadline that timed out in #190",
            );

            Ok(())
        }

        // A forward that hangs (owner died without RST) is bounded by the call
        // timeout and falls back to local processing.
        #[tokio::test(start_paused = true)]
        async fn forwarding_timeout_falls_back_to_local() -> Result<()> {
            let storage = storage_container().await?;
            let stub = StubForward::new(StubBehaviour::Hang);

            let coordinator = ForwardingCoordinator::with_forward(
                Controller::with_storage(storage)?,
                registry_owning_nothing(peer(1), peer(2)),
                stub.clone(),
            );

            // heartbeat for an unknown group: the local fallback answers with
            // today's error, proving the deadline fired and local processing
            // took over.
            let response = HeartbeatResponse::try_from(
                coordinator
                    .heartbeat("some-group", 0, "some-member", None)
                    .await?,
            )?;

            assert_ne!(
                ErrorCode::RebalanceInProgress,
                ErrorCode::try_from(response.error_code)?
            );
            assert_eq!(1, stub.calls());
            assert_eq!(1, coordinator.fallbacks());

            Ok(())
        }
    }
}
