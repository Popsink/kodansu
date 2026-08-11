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

//! Deciding how long a principal waits, cheaply enough to decide it per
//! request (#384).
//!
//! The same shape as [`crate::Authorizer`], and for the same reason: the
//! configuration lives in one object in the object store, and a produce of a
//! thousand records must not cost a thousand reads of it. A snapshot is held
//! for a short TTL, so a limit an operator applies takes effect across the
//! fleet within that window without anything being told about it.
//!
//! **The accounting is per replica and in memory.** A fleet-wide limit would
//! need shared accounting, which on this broker means a read and a write of the
//! object store on the hot path that exists to avoid exactly that. So the
//! counters are local, and a fleet-wide intent is expressed by declaring how
//! many replicas there are and dividing ([`QuotaLimits::divided_between`]).
//! That is approximate, and it is wrong while the fleet is mid-scale;
//! `docs/quotas.md` says so and says what it costs.
//!
//! **Failing open, unlike authorization.** A replica that cannot read the ACLs
//! denies, because denying cannot leak. A replica that cannot read the quotas
//! falls back to the limits its own command line configured, because the risk
//! of not throttling for a few seconds is a bill and the risk of throttling
//! everybody because a GET timed out is an outage.

use std::{
    collections::HashMap,
    fmt::{self, Debug},
    sync::{Arc, Mutex},
};

use tokio::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::{ArcDynStorage, QuotaLimits, Quotas};

/// How long a snapshot of the quotas is served before it is re-read.
///
/// The same window [`crate::ACL_SNAPSHOT_TTL`] uses, for the same reason: short
/// enough that an operator applying a limit sees it take effect while they are
/// still watching, long enough that a busy topic costs nothing.
pub const QUOTA_SNAPSHOT_TTL: Duration = Duration::from_secs(5);

/// The longest throttle a single response may ask a client to wait.
///
/// A throttle is served to the client and then applied *between* requests
/// (KIP-219), so a client's next request sits unread for this long. Beyond a
/// client's `request.timeout.ms` — 30 seconds by default — that stops looking
/// like backpressure and starts looking like a dead broker. Kafka caps its own
/// throttle at the quota window for the same reason.
///
/// Capping the answer does not uncap the limit: the debt that could not be
/// repaid in one throttle stays on the bucket and is charged for on the next
/// request, so a client over its quota keeps being throttled until it is not.
pub const MAX_THROTTLE: Duration = Duration::from_secs(10);

/// How much unused rate a principal may carry forward.
///
/// One second of it. A quota with no burst throttles the first request of every
/// idle client, because a request is a lump and a rate is not; a quota with a
/// large one lets a client that has been quiet all day spend the whole day's
/// allowance at once, which on an object store is the request spike the limit
/// exists to prevent.
const BURST: Duration = Duration::from_secs(1);

/// How much debt a principal may accumulate.
///
/// Debt is what makes the sustained rate converge on the limit when a single
/// request is larger than one throttle can repay. Unbounded, a client that
/// blasted once and stopped would go on being throttled long after; bounded
/// here, the catch-up is at most this long.
const MAX_DEBT: Duration = Duration::from_secs(60);

/// How often the per-principal accounting is swept.
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

/// How long a principal with nothing outstanding is remembered.
const PRUNE_IDLE: Duration = Duration::from_secs(300);

/// What one request costs the principal that made it.
///
/// Produce and fetch bytes are counted from the record batches, not from the
/// frame: that is what Kafka's own quotas measure, it is what an operator sizes
/// a limit against, and it does not make a client pay for this broker's
/// framing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Charge {
    pub produced_bytes: u64,
    pub fetched_bytes: u64,
    pub requests: u32,
}

impl Charge {
    /// One request, costing nothing but itself.
    #[must_use]
    pub fn request() -> Self {
        Self {
            requests: 1,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn produced(self, bytes: u64) -> Self {
        Self {
            produced_bytes: self.produced_bytes + bytes,
            ..self
        }
    }

    #[must_use]
    pub fn fetched(self, bytes: u64) -> Self {
        Self {
            fetched_bytes: self.fetched_bytes + bytes,
            ..self
        }
    }
}

/// One dimension's accounting for one principal.
///
/// A token bucket rather than Kafka's sampled-window rate. Both converge a
/// sustained rate on the limit; this one is O(1) state, has no window boundary
/// for a client to synchronise with, and answers "how long until this is within
/// the limit again" directly, which is exactly the number `throttle_time_ms`
/// carries.
#[derive(Clone, Copy, Debug)]
struct Bucket {
    /// Unspent allowance, in whatever the dimension counts. Negative is debt.
    available: f64,
    at: Instant,
}

impl Bucket {
    /// A principal seen for the first time starts with a full burst.
    ///
    /// Expressed as a bucket that was last charged a burst ago rather than as
    /// an opening balance, because the balance is in bytes and the rate is not
    /// known here — the limit is read per request, and it can change under a
    /// bucket that already exists.
    ///
    /// Without it the first request of every idle client is throttled, which is
    /// every client: a Kafka consumer polls, waits, and polls again.
    fn new(now: Instant) -> Self {
        Self {
            available: 0.0,
            at: now.checked_sub(BURST).unwrap_or(now),
        }
    }

    /// Charge `cost` against a limit of `rate` per second, answering how long
    /// the principal must wait before it is within its limit again.
    fn charge(&mut self, now: Instant, rate: f64, cost: f64) -> Duration {
        if cost <= 0.0 {
            return Duration::ZERO;
        }

        if rate <= 0.0 {
            // A limit of zero permits nothing, and dividing by it would answer
            // an infinite wait. The longest throttle is the honest answer: the
            // client is told to stop, repeatedly.
            return MAX_THROTTLE;
        }

        let burst = rate * BURST.as_secs_f64();
        let elapsed = now.saturating_duration_since(self.at).as_secs_f64();

        self.at = now;
        self.available = (self.available + elapsed * rate).min(burst) - cost;
        self.available = self.available.max(-rate * MAX_DEBT.as_secs_f64());

        if self.available >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64((-self.available / rate).min(MAX_THROTTLE.as_secs_f64()))
        }
    }

    /// Whether this bucket has nothing outstanding and can be forgotten.
    fn settled(&self) -> bool {
        self.available >= 0.0
    }
}

/// Every dimension's accounting for one principal.
#[derive(Clone, Copy, Debug)]
struct Buckets {
    producer: Bucket,
    consumer: Bucket,
    request: Bucket,
    seen: Instant,
}

impl Buckets {
    fn new(now: Instant) -> Self {
        Self {
            producer: Bucket::new(now),
            consumer: Bucket::new(now),
            request: Bucket::new(now),
            seen: now,
        }
    }

    fn charge(&mut self, now: Instant, limits: QuotaLimits, charge: Charge) -> Duration {
        self.seen = now;

        // The longest of the three, not their sum: they are three separate
        // limits, and waiting out the longest satisfies all of them. Summing
        // would throttle a request that violated one limit as though it had
        // violated every one.
        [
            limits.producer_byte_rate.map(|rate| {
                self.producer
                    .charge(now, rate, charge.produced_bytes as f64)
            }),
            limits
                .consumer_byte_rate
                .map(|rate| self.consumer.charge(now, rate, charge.fetched_bytes as f64)),
            limits
                .request_rate
                .map(|rate| self.request.charge(now, rate, f64::from(charge.requests))),
        ]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or_default()
    }

    fn forgettable(&self, now: Instant) -> bool {
        self.producer.settled()
            && self.consumer.settled()
            && self.request.settled()
            && now.saturating_duration_since(self.seen) > PRUNE_IDLE
    }
}

/// The per-principal accounting, and when it was last swept.
#[derive(Debug)]
struct Accounting {
    principals: HashMap<String, Buckets>,
    pruned: Instant,
}

/// The cluster's quotas as last read, and when.
type Snapshot = Arc<Mutex<Option<(Arc<Quotas>, Instant)>>>;

/// The quotas of a cluster, cached, and the accounting taken against them.
#[derive(Clone)]
pub struct QuotaEnforcer {
    storage: ArcDynStorage,

    /// The limits a principal gets when neither it nor the cluster default
    /// names one — the broker's own command line.
    ///
    /// So that a broker started with no control plane in front of it is still
    /// protected by something. A fleet whose quotas have never been configured
    /// is the one most likely to be surprised by a bill.
    defaults: QuotaLimits,

    /// How many replicas the configured limits are shared between.
    replicas: u32,

    snapshot: Snapshot,
    accounting: Arc<Mutex<Accounting>>,
    ttl: Duration,
}

impl Debug for QuotaEnforcer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuotaEnforcer")
            .field("defaults", &self.defaults)
            .field("replicas", &self.replicas)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl QuotaEnforcer {
    #[must_use]
    pub fn new(storage: ArcDynStorage) -> Self {
        Self {
            storage,
            defaults: QuotaLimits::default(),
            replicas: 1,
            snapshot: Arc::new(Mutex::new(None)),
            accounting: Arc::new(Mutex::new(Accounting {
                principals: HashMap::new(),
                pruned: Instant::now(),
            })),
            ttl: QUOTA_SNAPSHOT_TTL,
        }
    }

    /// The limits applied to a principal the cluster's quotas do not cover.
    #[must_use]
    pub fn with_defaults(self, defaults: QuotaLimits) -> Self {
        Self { defaults, ..self }
    }

    /// How many replicas a configured limit is shared between.
    ///
    /// `1` — the default — enforces every configured limit on every replica,
    /// which is what Apache Kafka does and what an operator moving from it
    /// expects. Anything higher reads the configured limit as a fleet-wide one.
    #[must_use]
    pub fn with_replicas(self, replicas: u32) -> Self {
        Self { replicas, ..self }
    }

    /// Override how long a snapshot is served. Tests set it to zero to see a
    /// limit take effect without waiting one out.
    #[must_use]
    pub fn with_ttl(self, ttl: Duration) -> Self {
        Self { ttl, ..self }
    }

    /// The limits in force for `principal`, in `User:name` form, on this
    /// replica.
    pub async fn limits_for(&self, principal: &str) -> QuotaLimits {
        self.quotas()
            .await
            .for_principal(principal)
            .or(self.defaults)
            .divided_between(self.replicas)
    }

    /// Charge a request to `principal`, answering how long its connection owes
    /// before the next one is read.
    ///
    /// The answer is a delay to be applied *between* requests, never inside
    /// one: a throttled request that slept in the request path would be counted
    /// in flight and folded into `tansu_request_duration`, which would report a
    /// fleet deliberately refusing traffic as a fleet saturated by it and have
    /// #362's scaler add replicas to serve load this broker has just decided
    /// not to serve.
    pub async fn throttle(&self, principal: &str, charge: Charge) -> Duration {
        let limits = self.limits_for(principal).await;

        if limits.is_empty() {
            // Nothing configured anywhere: no accounting, and in particular no
            // entry in the map. A fleet with no quotas must not grow one row
            // per principal for nothing.
            return Duration::ZERO;
        }

        let now = Instant::now();

        let Ok(mut accounting) = self.accounting.lock() else {
            warn!(principal, "quota accounting is poisoned; not throttling");
            return Duration::ZERO;
        };

        let throttle = accounting
            .principals
            .entry(principal.to_owned())
            .or_insert_with(|| Buckets::new(now))
            .charge(now, limits, charge);

        if now.saturating_duration_since(accounting.pruned) > PRUNE_INTERVAL {
            accounting.pruned = now;
            accounting
                .principals
                .retain(|_, buckets| !buckets.forgettable(now));
        }

        if !throttle.is_zero() {
            debug!(principal, ?charge, ?throttle, ?limits);
        }

        throttle
    }

    /// The current snapshot, re-read when it has aged out.
    ///
    /// A failed re-read serves the stale snapshot, and an empty one when there
    /// has never been a snapshot at all — which leaves the broker's own
    /// defaults in force. See the module docs for why this fails open where
    /// authorization fails closed.
    async fn quotas(&self) -> Arc<Quotas> {
        if let Some(fresh) = self.cached(true) {
            return fresh;
        }

        match self.storage.client_quotas().await {
            Ok(quotas) => {
                let quotas = Arc::new(quotas);

                if let Ok(mut snapshot) = self.snapshot.lock() {
                    *snapshot = Some((quotas.clone(), Instant::now()));
                }

                quotas
            }

            Err(error) => {
                warn!(
                    ?error,
                    "could not refresh the client quotas; serving the last snapshot"
                );

                self.cached(false).unwrap_or_default()
            }
        }
    }

    fn cached(&self, fresh_only: bool) -> Option<Arc<Quotas>> {
        self.snapshot.lock().ok().and_then(|snapshot| {
            snapshot.as_ref().and_then(|(quotas, read_at)| {
                (!fresh_only || read_at.elapsed() < self.ttl).then(|| quotas.clone())
            })
        })
    }
}

#[cfg(all(test, feature = "dynostore"))]
mod tests {
    use object_store::memory::InMemory;

    use super::*;
    use crate::{
        PRODUCER_BYTE_RATE, QuotaAlteration, QuotaEntity, QuotaOp, REQUEST_RATE, Storage,
        dynostore::DynoStore,
    };

    const ALICE: &str = "User:alice";

    fn enforcer() -> QuotaEnforcer {
        QuotaEnforcer::new(Arc::new(
            Box::new(DynoStore::new("tansu", 111, InMemory::new())) as Box<dyn Storage>,
        ))
    }

    fn set(entity: QuotaEntity, key: &str, value: f64) -> QuotaAlteration {
        QuotaAlteration {
            entity,
            ops: vec![QuotaOp {
                key: key.into(),
                value,
                remove: false,
            }],
        }
    }

    /// A cluster with no quotas throttles nobody: a broker without
    /// `--authentication`, and every deployment that has never configured one,
    /// must behave exactly as it does today.
    #[tokio::test]
    async fn nothing_configured_throttles_nobody() {
        let enforcer = enforcer();

        assert_eq!(
            Duration::ZERO,
            enforcer
                .throttle(ALICE, Charge::request().produced(1_000_000))
                .await
        );
    }

    /// The acceptance criterion: a principal over its produce byte-rate is
    /// given a non-zero throttle, and its **sustained** rate converges on the
    /// limit.
    ///
    /// Convergence is the part worth asserting. A limiter that answers a
    /// non-zero throttle is easy; one that answers the *right* throttle is what
    /// makes the observed rate equal the configured one, and a bucket that
    /// forgot its debt would pass the first assertion and fail this.
    #[tokio::test(start_paused = true)]
    async fn a_producer_over_its_limit_converges_on_it() {
        const RATE: f64 = 1024.0;
        const BATCH: u64 = 4096;

        let enforcer = enforcer().with_ttl(Duration::ZERO);

        _ = enforcer
            .storage
            .alter_client_quotas(
                &[set(
                    QuotaEntity::User("alice".into()),
                    PRODUCER_BYTE_RATE,
                    RATE,
                )],
                false,
            )
            .await
            .expect("alter");

        // The burst is spent first, so the very first request of an idle
        // principal is not throttled — a rate is not a lump and a request is.
        assert_eq!(
            Duration::ZERO,
            enforcer
                .throttle(ALICE, Charge::request().produced(BATCH / 8))
                .await,
        );

        let start = Instant::now();
        let mut produced = 0u64;

        // Produce as fast as the throttle allows, for a simulated minute.
        while Instant::now().saturating_duration_since(start) < Duration::from_secs(60) {
            let throttle = enforcer
                .throttle(ALICE, Charge::request().produced(BATCH))
                .await;

            produced += BATCH;
            tokio::time::sleep(throttle).await;
        }

        let elapsed = Instant::now()
            .saturating_duration_since(start)
            .as_secs_f64();
        let observed = produced as f64 / elapsed;

        assert!(
            (observed - RATE).abs() < RATE * 0.1,
            "a sustained rate of {observed} B/s must converge on the configured {RATE} B/s",
        );
    }

    /// A throttle is bounded, so that a client's next request is not left
    /// unread past its own request timeout — and the debt that could not be
    /// repaid inside that bound is still owed.
    #[tokio::test(start_paused = true)]
    async fn a_throttle_is_capped_and_the_debt_is_not_forgotten() {
        let enforcer = enforcer().with_ttl(Duration::ZERO);

        _ = enforcer
            .storage
            .alter_client_quotas(
                &[set(
                    QuotaEntity::User("alice".into()),
                    PRODUCER_BYTE_RATE,
                    1.0,
                )],
                false,
            )
            .await
            .expect("alter");

        assert_eq!(
            MAX_THROTTLE,
            enforcer
                .throttle(ALICE, Charge::request().produced(1_000_000))
                .await,
            "a single request far over the limit is capped at the longest throttle",
        );

        // Waiting the throttle out does not clear a debt that large: the next
        // request is throttled again, which is what makes the cap safe.
        tokio::time::sleep(MAX_THROTTLE).await;

        assert!(
            !enforcer
                .throttle(ALICE, Charge::request().produced(1))
                .await
                .is_zero(),
            "the debt a capped throttle could not repay must still be owed",
        );
    }

    /// The dimensions are separate limits: a produce quota must not throttle a
    /// consumer, and a request-rate quota applies to APIs that move no bytes at
    /// all.
    #[tokio::test(start_paused = true)]
    async fn each_dimension_is_its_own_limit() {
        let enforcer = enforcer().with_ttl(Duration::ZERO);

        _ = enforcer
            .storage
            .alter_client_quotas(
                &[set(QuotaEntity::User("alice".into()), REQUEST_RATE, 1.0)],
                false,
            )
            .await
            .expect("alter");

        // Nothing limits bytes, so a large fetch is free…
        assert_eq!(
            Duration::ZERO,
            enforcer
                .throttle(
                    ALICE,
                    Charge {
                        requests: 0,
                        ..Charge::default()
                    }
                    .fetched(1_000_000)
                )
                .await,
        );

        // …but the requests themselves are counted, and a metadata storm is
        // exactly the shape of load a byte-rate quota cannot see.
        for _ in 0..4 {
            _ = enforcer.throttle(ALICE, Charge::request()).await;
        }

        assert!(
            !enforcer.throttle(ALICE, Charge::request()).await.is_zero(),
            "a request-rate quota must throttle requests that carry no records",
        );
    }

    /// The broker's own default applies to a principal the cluster's quotas do
    /// not name, so a broker with no control plane in front of it is still
    /// protected by something.
    #[tokio::test(start_paused = true)]
    async fn the_brokers_own_default_applies_when_nothing_names_the_principal() {
        let enforcer = enforcer().with_defaults(QuotaLimits {
            producer_byte_rate: Some(1.0),
            ..QuotaLimits::default()
        });

        assert!(
            !enforcer
                .throttle(ALICE, Charge::request().produced(1_000_000))
                .await
                .is_zero(),
        );
    }

    /// A configured quota takes precedence over the broker's default: the
    /// control plane, when there is one, is the authority.
    #[tokio::test(start_paused = true)]
    async fn a_configured_quota_overrides_the_brokers_default() {
        let enforcer = enforcer()
            .with_ttl(Duration::ZERO)
            .with_defaults(QuotaLimits {
                producer_byte_rate: Some(1.0),
                ..QuotaLimits::default()
            });

        _ = enforcer
            .storage
            .alter_client_quotas(
                &[set(
                    QuotaEntity::User("alice".into()),
                    PRODUCER_BYTE_RATE,
                    1_000_000.0,
                )],
                false,
            )
            .await
            .expect("alter");

        assert_eq!(
            Some(1_000_000.0),
            enforcer.limits_for(ALICE).await.producer_byte_rate,
        );
    }

    /// A declared fleet size divides a configured limit, so that what an
    /// operator writes is what the fleet as a whole is allowed rather than what
    /// each replica is.
    #[tokio::test(start_paused = true)]
    async fn a_declared_fleet_divides_a_configured_limit() {
        let enforcer = enforcer()
            .with_ttl(Duration::ZERO)
            .with_replicas(4)
            .with_defaults(QuotaLimits {
                producer_byte_rate: Some(1000.0),
                ..QuotaLimits::default()
            });

        assert_eq!(
            Some(250.0),
            enforcer.limits_for(ALICE).await.producer_byte_rate,
        );
    }
}
