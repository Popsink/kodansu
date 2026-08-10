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

//! Client quotas: how much a principal may ask of this broker (#384).
//!
//! #363 gave a mutualised fleet an authorization boundary. Authorization says
//! *whether* a principal may write; nothing said *how much*. On a broker whose
//! cost is object-store requests, the second half is the one that decides the
//! bill, and the request rate against the object store was a property of who
//! happened to be connected rather than of anything anybody configured.
//!
//! This is the model and its storage. [`crate::QuotaEnforcer`] is the decision
//! taken against it, and the broker's quota layer is where that decision is
//! applied.
//!
//! ## What a quota is written against
//!
//! The `user` entity of KIP-546, and only that one. Kafka also has `client-id`
//! and `ip`; neither is a boundary here. A client id is chosen by the client
//! and can be changed by it, and an address is the load balancer's, not the
//! tenant's. The principal is the thing #363 already made the fleet's unit of
//! isolation, so it is the thing a limit is written against.
//!
//! A quota is stored against the **bare** user name — `alice` — not the
//! `User:alice` an ACL is written against. That is not a choice: it is what the
//! wire carries. `kafka-configs.sh --entity-type users --entity-name alice`
//! sends `alice`, and an operator reading `DescribeClientQuotas` back expects
//! the same string. The translation happens once, in [`Quotas::for_principal`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Bytes per second a principal may produce, summed over the record batches it
/// sends. Kafka's own spelling, so `kafka-configs.sh` and `rpk` configure it
/// with no bespoke tooling.
pub const PRODUCER_BYTE_RATE: &str = "producer_byte_rate";

/// Bytes per second a principal may fetch, summed over the record batches it is
/// answered with. Kafka's own spelling.
pub const CONSUMER_BYTE_RATE: &str = "consumer_byte_rate";

/// Requests per second a principal may make, of any API.
///
/// **Not** Kafka's `request_percentage`, deliberately. That one is a share of a
/// request-handler thread's time, and this broker has no such pool to divide:
/// what a request costs here is object-store operations, which track the count
/// of requests and not the time they took. Answering `request_percentage` with
/// something that measured requests would be the worse mistake — a stub that
/// reports success is what #363 found and what #381 found again.
pub const REQUEST_RATE: &str = "request_rate";

/// The quota configuration keys this broker enforces. Anything else is refused
/// at `AlterClientQuotas` rather than stored and ignored.
pub const QUOTA_KEYS: [&str; 3] = [PRODUCER_BYTE_RATE, CONSUMER_BYTE_RATE, REQUEST_RATE];

/// The KIP-546 entity type a quota on a principal is written against.
pub const USER_ENTITY: &str = "user";

/// Who a quota applies to.
///
/// `Default` is the entity `kafka-configs.sh --entity-type users
/// --entity-default` writes: the limit every principal gets unless one names
/// it. On a mutualised fleet that is the one that matters, because it applies
/// to the tenant nobody has got round to configuring.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum QuotaEntity {
    /// Every principal without one of its own.
    Default,

    /// One principal, by the bare name the wire carries — `alice`, not
    /// `User:alice`.
    User(String),
}

impl QuotaEntity {
    /// The name this entity is described back with: `None` is how the wire
    /// spells the default entity.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Default => None,
            Self::User(user) => Some(user.as_str()),
        }
    }
}

/// The limits of one entity.
///
/// Three `Option`s rather than a map, because "not configured" and "configured
/// to zero" are different answers and a map of validated keys is a map that
/// still has to be validated at every read. An unset dimension is not enforced
/// at all — a produce quota alone must not throttle a consumer.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, PartialOrd, Serialize)]
pub struct QuotaLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_byte_rate: Option<f64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_byte_rate: Option<f64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_rate: Option<f64>,
}

impl QuotaLimits {
    /// Whether nothing at all is configured. An entity that reaches this state
    /// is removed rather than stored empty, so `DescribeClientQuotas` does not
    /// report an entity with no quotas as though it had been given one.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.producer_byte_rate.is_none()
            && self.consumer_byte_rate.is_none()
            && self.request_rate.is_none()
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<f64> {
        match key {
            PRODUCER_BYTE_RATE => self.producer_byte_rate,
            CONSUMER_BYTE_RATE => self.consumer_byte_rate,
            REQUEST_RATE => self.request_rate,
            _ => None,
        }
    }

    /// Set or clear one key, refusing a key this broker does not enforce.
    ///
    /// # Errors
    ///
    /// The key is not one of [`QUOTA_KEYS`], or the value is not a limit that
    /// can be enforced — negative, or not a number.
    pub fn set(&mut self, key: &str, value: Option<f64>) -> Result<(), QuotaKeyError> {
        if let Some(value) = value
            && (!value.is_finite() || value < 0.0)
        {
            return Err(QuotaKeyError::Value(key.to_owned(), value));
        }

        let field = match key {
            PRODUCER_BYTE_RATE => &mut self.producer_byte_rate,
            CONSUMER_BYTE_RATE => &mut self.consumer_byte_rate,
            REQUEST_RATE => &mut self.request_rate,
            unknown => return Err(QuotaKeyError::Unknown(unknown.to_owned())),
        };

        *field = value;

        Ok(())
    }

    /// Every configured key, in the order [`QUOTA_KEYS`] names them, so that
    /// two brokers describe the same entity identically.
    #[must_use]
    pub fn configured(&self) -> Vec<(&'static str, f64)> {
        QUOTA_KEYS
            .into_iter()
            .filter_map(|key| self.get(key).map(|value| (key, value)))
            .collect()
    }

    /// This entity's limits, falling back to `fallback` per key.
    ///
    /// Per key rather than per entity: a principal given only a
    /// `producer_byte_rate` of its own must keep the cluster default's
    /// `consumer_byte_rate`, not lose it by having been named at all.
    #[must_use]
    pub fn or(self, fallback: Self) -> Self {
        Self {
            producer_byte_rate: self.producer_byte_rate.or(fallback.producer_byte_rate),
            consumer_byte_rate: self.consumer_byte_rate.or(fallback.consumer_byte_rate),
            request_rate: self.request_rate.or(fallback.request_rate),
        }
    }

    /// These limits divided between `replicas`.
    ///
    /// A limit enforced independently on every replica of an autoscaled fleet
    /// is multiplied by the replica count, and the multiplier moves on its own.
    /// There is no membership to count — a replica of this broker does not know
    /// its peers exist, which is the whole of #360 — so the count is declared
    /// by whoever operates the fleet. See `docs/quotas.md` for what that costs.
    #[must_use]
    pub fn divided_between(self, replicas: u32) -> Self {
        let replicas = f64::from(replicas.max(1));

        Self {
            producer_byte_rate: self.producer_byte_rate.map(|rate| rate / replicas),
            consumer_byte_rate: self.consumer_byte_rate.map(|rate| rate / replicas),
            request_rate: self.request_rate.map(|rate| rate / replicas),
        }
    }
}

/// A key or value `AlterClientQuotas` cannot store.
#[derive(Clone, Debug, PartialEq)]
pub enum QuotaKeyError {
    Unknown(String),
    Value(String, f64),
}

impl std::fmt::Display for QuotaKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown(key) => write!(
                f,
                "unknown quota configuration key {key:?}; this broker enforces {}",
                QUOTA_KEYS.join(", ")
            ),

            Self::Value(key, value) => write!(f, "{value} is not a limit for {key:?}"),
        }
    }
}

impl std::error::Error for QuotaKeyError {}

/// Every quota in the cluster.
///
/// One document, for the reason the ACLs are one document: the request path
/// needs the whole of it to answer any question, so a key per entity would put
/// a LIST on the hot path — the one place on an object-store broker that can
/// least afford one.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Quotas {
    /// The limits applied to a principal no entry names.
    #[serde(default)]
    pub default: QuotaLimits,

    /// By the bare user name the wire carries.
    #[serde(default)]
    pub users: BTreeMap<String, QuotaLimits>,
}

impl Quotas {
    /// The limits that apply to an authorization principal — `User:alice`, the
    /// form [`crate::Requester`] carries.
    ///
    /// The strip is the whole reason this takes a principal rather than a user
    /// name. Two namespaces that look alike: SASL and the ACLs speak
    /// `User:alice`, `kafka-configs.sh` and this document speak `alice`.
    /// Looking the qualified form up in a map keyed by the bare one finds
    /// nothing, and a quota that matches nothing is a quota that is not
    /// enforced — the failure #363 shipped once already, in the other
    /// direction.
    #[must_use]
    pub fn for_principal(&self, principal: &str) -> QuotaLimits {
        self.for_user(user_of(principal))
    }

    /// The limits that apply to a bare user name.
    #[must_use]
    pub fn for_user(&self, user: &str) -> QuotaLimits {
        self.users
            .get(user)
            .copied()
            .unwrap_or_default()
            .or(self.default)
    }

    /// The limits of one entity as stored, with no fallback.
    #[must_use]
    pub fn get(&self, entity: &QuotaEntity) -> Option<QuotaLimits> {
        match entity {
            QuotaEntity::Default => Some(self.default).filter(|limits| !limits.is_empty()),
            QuotaEntity::User(user) => self.users.get(user).copied(),
        }
    }

    /// Apply one alteration, answering what could not be applied.
    ///
    /// All of an entry's operations or none of them: KIP-546 says an entry is
    /// atomic, and half-applying one would leave an operator with a limit they
    /// did not ask for and were told had failed.
    ///
    /// # Errors
    ///
    /// Any operation names a key this broker does not enforce, or a value that
    /// is not a limit.
    pub fn alter(&mut self, alteration: &QuotaAlteration) -> Result<(), QuotaKeyError> {
        let mut limits = self.get(&alteration.entity).unwrap_or_default();

        for op in &alteration.ops {
            limits.set(&op.key, (!op.remove).then_some(op.value))?;
        }

        match &alteration.entity {
            QuotaEntity::Default => self.default = limits,

            QuotaEntity::User(user) => {
                // An entity left with nothing configured is removed rather than
                // stored empty, so that describing it answers "no quota" rather
                // than an entity with an empty value list — which reads, to
                // both an operator and a client, as though something had been
                // set.
                if limits.is_empty() {
                    _ = self.users.remove(user);
                } else {
                    _ = self.users.insert(user.clone(), limits);
                }
            }
        }

        Ok(())
    }

    /// Every entity `components` selects, with the limits stored against it.
    ///
    /// Stored, not effective: an operator describing `alice` is asking what
    /// they configured for `alice`, and folding the cluster default in would
    /// report a limit that no `--alter` wrote and no `--delete-config` can
    /// remove.
    #[must_use]
    pub fn matching(
        &self,
        components: &[QuotaFilterComponent],
        strict: bool,
    ) -> Vec<(QuotaEntity, QuotaLimits)> {
        // `strict` excludes entities whose types the filter does not mention.
        // With `user` the only type this broker knows, a strict filter that
        // does not mention it selects nothing at all.
        let mentions_user = components
            .iter()
            .any(|component| component.entity_type == USER_ENTITY);

        if strict && !mentions_user {
            return vec![];
        }

        // A component naming a type this broker does not know cannot be
        // satisfied by anything stored, so it selects nothing — rather than
        // being ignored, which would answer an `ip` filter with every user's
        // quotas.
        if components
            .iter()
            .any(|component| component.entity_type != USER_ENTITY)
        {
            return vec![];
        }

        let mut selected = vec![];

        let wants_default = components.is_empty()
            || components
                .iter()
                .any(|component| matches!(component.matches, QuotaMatch::Default));

        let wants_named = |user: &str| {
            components.is_empty()
                || components.iter().any(|component| match &component.matches {
                    QuotaMatch::Exact(name) => name == user,
                    QuotaMatch::Any => true,
                    QuotaMatch::Default => false,
                })
        };

        if wants_default && !self.default.is_empty() {
            selected.push((QuotaEntity::Default, self.default));
        }

        selected.extend(
            self.users
                .iter()
                .filter(|(user, _)| wants_named(user))
                .map(|(user, limits)| (QuotaEntity::User(user.clone()), *limits)),
        );

        selected
    }
}

/// The bare user name inside an authorization principal.
///
/// `User:alice` is `alice`; anything without the prefix is itself, because a
/// broker without `--authentication` has no principals to strip and a test that
/// hands over a bare name should get the same answer.
#[must_use]
pub fn user_of(principal: &str) -> &str {
    principal.strip_prefix("User:").unwrap_or(principal)
}

/// One `AlterClientQuotas` entry, already parsed into something storable.
///
/// The wire's entity is a list of `(type, name)` pairs and can name types this
/// broker does not have; turning that into a [`QuotaEntity`] is the service's
/// job, so that a refusal can echo back the entity the client actually sent.
#[derive(Clone, Debug, PartialEq)]
pub struct QuotaAlteration {
    pub entity: QuotaEntity,
    pub ops: Vec<QuotaOp>,
}

/// One key of one entry, set or removed.
#[derive(Clone, Debug, PartialEq)]
pub struct QuotaOp {
    pub key: String,
    pub value: f64,
    pub remove: bool,
}

/// How a `DescribeClientQuotas` component selects a name.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum QuotaMatch {
    /// `0`: this name exactly.
    Exact(String),

    /// `1`: the default entity, the one with no name.
    Default,

    /// `2`: any *named* entity, which is not the default one.
    #[default]
    Any,
}

/// One `DescribeClientQuotas` filter component.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QuotaFilterComponent {
    pub entity_type: String,
    pub matches: QuotaMatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(producer: Option<f64>, consumer: Option<f64>) -> QuotaLimits {
        QuotaLimits {
            producer_byte_rate: producer,
            consumer_byte_rate: consumer,
            request_rate: None,
        }
    }

    /// The invariant this whole file rests on, and the one #363 got wrong in
    /// the other direction: what SASL authenticates is `User:alice`, and what
    /// `kafka-configs.sh` writes a quota against is `alice`.
    ///
    /// Looking the qualified form up in this map finds nothing, and a quota
    /// that matches nothing is one that is never enforced — which looks exactly
    /// like a broker under its limits.
    #[test]
    fn a_quota_is_written_against_the_bare_user_the_wire_carries() {
        assert_eq!("alice", user_of("User:alice"));
        assert_eq!("alice", user_of("alice"));

        let quotas = Quotas {
            default: QuotaLimits::default(),
            users: [("alice".into(), limits(Some(1024.0), None))]
                .into_iter()
                .collect(),
        };

        assert_eq!(
            Some(1024.0),
            quotas.for_principal("User:alice").producer_byte_rate,
            "the principal the request path carries must find the quota an operator wrote",
        );
    }

    /// A named principal keeps the cluster default for every key it was not
    /// given one of. The alternative — most-specific-entity-wins — silently
    /// unlimits a consumer the moment somebody gives its producer a limit.
    #[test]
    fn a_named_entity_falls_back_to_the_default_per_key() {
        let quotas = Quotas {
            default: limits(Some(1000.0), Some(2000.0)),
            users: [("alice".into(), limits(Some(50.0), None))]
                .into_iter()
                .collect(),
        };

        let alice = quotas.for_principal("User:alice");
        assert_eq!(Some(50.0), alice.producer_byte_rate);
        assert_eq!(Some(2000.0), alice.consumer_byte_rate);

        let bob = quotas.for_principal("User:bob");
        assert_eq!(Some(1000.0), bob.producer_byte_rate);
        assert_eq!(Some(2000.0), bob.consumer_byte_rate);
    }

    /// A principal nothing names and a cluster with no default is not
    /// throttled. Off by default, as with authorization.
    #[test]
    fn nothing_configured_is_no_limit() {
        assert!(Quotas::default().for_principal("User:alice").is_empty());
    }

    #[test]
    fn an_unknown_key_is_refused_rather_than_stored() {
        let mut limits = QuotaLimits::default();

        assert_eq!(
            Err(QuotaKeyError::Unknown("request_percentage".into())),
            limits.set("request_percentage", Some(200.0)),
            "a key this broker cannot enforce must be refused, not accepted and ignored",
        );

        assert!(limits.is_empty());
    }

    #[test]
    fn a_negative_or_infinite_rate_is_not_a_limit() {
        let mut limits = QuotaLimits::default();

        assert!(limits.set(PRODUCER_BYTE_RATE, Some(-1.0)).is_err());
        assert!(limits.set(PRODUCER_BYTE_RATE, Some(f64::INFINITY)).is_err());
        assert!(limits.set(PRODUCER_BYTE_RATE, Some(f64::NAN)).is_err());
        assert!(limits.set(PRODUCER_BYTE_RATE, Some(0.0)).is_ok());
    }

    #[test]
    fn an_entry_is_applied_whole_or_not_at_all() {
        let mut quotas = Quotas::default();

        assert!(
            quotas
                .alter(&QuotaAlteration {
                    entity: QuotaEntity::User("alice".into()),
                    ops: vec![
                        QuotaOp {
                            key: PRODUCER_BYTE_RATE.into(),
                            value: 1024.0,
                            remove: false,
                        },
                        QuotaOp {
                            key: "nonsense".into(),
                            value: 1.0,
                            remove: false,
                        },
                    ],
                })
                .is_err()
        );

        assert_eq!(
            None,
            quotas.get(&QuotaEntity::User("alice".into())),
            "a rejected entry must leave nothing behind",
        );
    }

    /// Removing the last key removes the entity. An entity stored with an empty
    /// value list is described back as though it had been given a quota.
    #[test]
    fn removing_the_last_key_removes_the_entity() {
        let mut quotas = Quotas::default();
        let alice = QuotaEntity::User("alice".into());

        quotas
            .alter(&QuotaAlteration {
                entity: alice.clone(),
                ops: vec![QuotaOp {
                    key: PRODUCER_BYTE_RATE.into(),
                    value: 1024.0,
                    remove: false,
                }],
            })
            .expect("set");

        assert!(quotas.get(&alice).is_some());

        quotas
            .alter(&QuotaAlteration {
                entity: alice.clone(),
                ops: vec![QuotaOp {
                    key: PRODUCER_BYTE_RATE.into(),
                    value: 0.0,
                    remove: true,
                }],
            })
            .expect("remove");

        assert_eq!(None, quotas.get(&alice));
        assert!(quotas.matching(&[], false).is_empty());
    }

    #[test]
    fn a_filter_selects_by_name_by_default_and_by_any() {
        let quotas = Quotas {
            default: limits(Some(1000.0), None),
            users: [
                ("alice".into(), limits(Some(50.0), None)),
                ("bob".into(), limits(Some(60.0), None)),
            ]
            .into_iter()
            .collect(),
        };

        let user = |matches| QuotaFilterComponent {
            entity_type: USER_ENTITY.into(),
            matches,
        };

        assert_eq!(
            vec![(QuotaEntity::User("alice".into()), limits(Some(50.0), None))],
            quotas.matching(&[user(QuotaMatch::Exact("alice".into()))], false),
        );

        assert_eq!(
            vec![(QuotaEntity::Default, limits(Some(1000.0), None))],
            quotas.matching(&[user(QuotaMatch::Default)], false),
        );

        // `Any` is "any *specified* name", which is not the default entity.
        assert_eq!(2, quotas.matching(&[user(QuotaMatch::Any)], false).len());

        // No components at all: everything, including the default entity.
        assert_eq!(3, quotas.matching(&[], false).len());

        // Strict, with nothing naming `user`: nothing.
        assert!(quotas.matching(&[], true).is_empty());

        // A type this broker does not have selects nothing rather than
        // everything.
        assert!(
            quotas
                .matching(
                    &[QuotaFilterComponent {
                        entity_type: "ip".into(),
                        matches: QuotaMatch::Any,
                    }],
                    false,
                )
                .is_empty()
        );
    }

    #[test]
    fn a_fleet_wide_limit_is_divided_between_its_replicas() {
        let fleet = limits(Some(1000.0), Some(2000.0));

        assert_eq!(fleet, fleet.divided_between(1));
        assert_eq!(
            limits(Some(250.0), Some(500.0)),
            fleet.divided_between(4),
            "a limit enforced on every replica is multiplied by the replica count",
        );

        // Zero replicas is not a fleet; it must not divide by zero and hand out
        // an infinite limit.
        assert_eq!(fleet, fleet.divided_between(0));
    }
}
