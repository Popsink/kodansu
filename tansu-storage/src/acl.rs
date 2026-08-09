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

//! Access control lists (#363).
//!
//! Authorization did not exist: the ACL APIs were routed and answered success
//! without doing anything, so `kafka-acls.sh` appeared to work while any
//! authenticated principal could produce to, consume from and delete any topic
//! in the cluster. A stub that reports success is worse than an absent API —
//! an operator who applies ACLs is told they took effect.
//!
//! This is the native Kafka model, deliberately, and not something adjacent to
//! it: resource type, resource name, pattern type, principal, host, operation,
//! permission. Standard means `kafka-acls.sh` and every operator tool work
//! unchanged, and it means `PREFIXED` is already exactly what scopes a
//! principal to one tenant's `tenant-a.` namespace without the broker needing
//! a notion of a tenant at all.
//!
//! The model, its storage, and the decision taken against it. Where that
//! decision is *applied* is the request path; see [`crate::Authorizer`] for the
//! cache that makes asking it cheap.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use tansu_sans_io::{
    acl::{Operation, Permission, Resource},
    resource::Pattern,
};

/// The principal a rule matching anyone is written against.
pub const WILDCARD_PRINCIPAL: &str = "User:*";

/// The host a rule matching anywhere is written against. Kafka's own spelling,
/// and the default `kafka-acls.sh` writes when `--allow-host` is not given.
pub const WILDCARD_HOST: &str = "*";

/// The resource name a `LITERAL` rule matching every resource of its type is
/// written against.
pub const WILDCARD_RESOURCE: &str = "*";

/// One ACL: a principal, on a host, may or may not perform an operation
/// against the resources a pattern selects.
///
/// Stored as its own document rather than as an index, and readable as one: an
/// operator debugging an authorization failure at 3am reads this object, and
/// `resource_type: "topic"` is worth more there than the `2` the wire carries.
/// The enums serialise by name for that reason.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AclBinding {
    pub resource_type: Resource,

    /// The topic, group, transactional id or user the pattern selects, or
    /// [`WILDCARD_RESOURCE`] for all of them.
    pub resource_name: String,

    pub pattern: Pattern,

    /// `User:alice`, or [`WILDCARD_PRINCIPAL`].
    ///
    /// Carried as the whole `Type:name` string rather than split, because that
    /// is what the wire carries, what `kafka-acls.sh` prints, and what an
    /// operator compares against. Splitting it would mean re-joining it
    /// everywhere it is shown.
    pub principal: String,

    /// The client address the rule applies from, or [`WILDCARD_HOST`].
    pub host: String,

    pub operation: Operation,

    pub permission: Permission,
}

impl AclBinding {
    /// Whether this binding selects `name` for a resource of `resource_type`.
    ///
    /// Not an authorization decision — that needs the principal, the host and
    /// the operation as well — only the half of it that the *pattern* decides,
    /// which is the half worth having on its own: a `PREFIXED` rule is the
    /// whole mechanism by which one tenant cannot see another's topics, and it
    /// is the part that is easy to get subtly wrong.
    #[must_use]
    pub fn selects(&self, resource_type: Resource, name: &str) -> bool {
        if self.resource_type != resource_type {
            return false;
        }

        match self.pattern {
            Pattern::Literal => {
                self.resource_name == name || self.resource_name == WILDCARD_RESOURCE
            }

            Pattern::Prefixed => name.starts_with(self.resource_name.as_str()),

            // `MATCH` is a *filter* pattern — it means "literal, prefixed, or
            // wildcard rules that would select this name" — so it has meaning
            // when describing or deleting and none when stored. `ANY` and
            // `UNKNOWN` likewise. A stored binding carrying one selects
            // nothing rather than everything: the failure of guessing wrong
            // here is a rule that grants more than it says.
            Pattern::Match | Pattern::Any | Pattern::Unknown => false,
        }
    }

    /// Whether this binding applies to `principal`.
    #[must_use]
    pub fn applies_to(&self, principal: &str) -> bool {
        self.principal == principal || self.principal == WILDCARD_PRINCIPAL
    }

    /// Whether this binding applies at `host`.
    #[must_use]
    pub fn applies_at(&self, host: &str) -> bool {
        self.host == host || self.host == WILDCARD_HOST
    }

    /// Whether this binding speaks to `operation`, exactly.
    ///
    /// `ALL` covers everything, which is the only implication resolved here.
    /// The rest of Kafka's table — `READ` implies `DESCRIBE` and so on — is
    /// [`implied_by`], and it is deliberately *not* here: implication holds for
    /// grants and not for denials, so a rule cannot answer it without knowing
    /// which of the two it is being asked about.
    #[must_use]
    pub fn covers(&self, operation: Operation) -> bool {
        self.operation == Operation::All || self.operation == operation
    }

    /// Whether this binding **grants** `operation`, following Kafka's
    /// implication table.
    #[must_use]
    pub fn grants(&self, operation: Operation) -> bool {
        self.covers(operation) || implied_by(operation).contains(&self.operation)
    }
}

/// The operations that, when granted, also grant `operation` (Kafka's
/// implication table).
///
/// Asymmetric on purpose, and this is the part that is easy to get wrong:
/// **implication holds for `ALLOW` and not for `DENY`.** Granting `READ`
/// grants `DESCRIBE`, because a client that may read a topic must be able to
/// see it exists. Denying `READ` does *not* deny `DESCRIBE` — a principal can
/// be told a topic exists while being refused its contents, and reading the
/// table symmetrically would silently widen every `DENY` an operator wrote.
fn implied_by(operation: Operation) -> &'static [Operation] {
    match operation {
        Operation::Describe => &[
            Operation::Read,
            Operation::Write,
            Operation::Delete,
            Operation::Alter,
        ],

        Operation::DescribeConfigs => &[Operation::AlterConfigs],

        _ => &[],
    }
}

/// What a `DescribeAcls` or `DeleteAcls` selects.
///
/// A separate type from [`AclBinding`], and not the same one with optional
/// fields, because the two mean opposite things about an absent value: a
/// binding with no principal is not a rule, and a filter with no principal
/// matches every principal. Conflating them is how a delete removes more than
/// it was asked to.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AclFilter {
    /// [`Resource::Any`] matches every type.
    pub resource_type: Resource,

    /// `None` matches every name.
    pub resource_name: Option<String>,

    /// [`Pattern::Any`] matches every pattern; [`Pattern::Match`] matches the
    /// rules that would select `resource_name`.
    pub pattern: Pattern,

    /// `None` matches every principal.
    pub principal: Option<String>,

    /// `None` matches every host.
    pub host: Option<String>,

    /// [`Operation::Any`] matches every operation.
    pub operation: Operation,

    /// [`Permission::Any`] matches both.
    pub permission: Permission,
}

impl AclFilter {
    /// The filter that selects everything.
    ///
    /// `Default` cannot be it: the derived default is `UNKNOWN` for every
    /// enum, which selects nothing. Spelling "everything" out is what stops a
    /// caller reaching for `Default::default()` and quietly getting the
    /// opposite.
    #[must_use]
    pub fn any() -> Self {
        Self {
            resource_type: Resource::Any,
            resource_name: None,
            pattern: Pattern::Any,
            principal: None,
            host: None,
            operation: Operation::Any,
            permission: Permission::Any,
        }
    }

    /// Whether `binding` is selected by this filter.
    #[must_use]
    pub fn matches(&self, binding: &AclBinding) -> bool {
        if self.resource_type != Resource::Any && self.resource_type != binding.resource_type {
            return false;
        }

        if self.operation != Operation::Any && self.operation != binding.operation {
            return false;
        }

        if self.permission != Permission::Any && self.permission != binding.permission {
            return false;
        }

        if self
            .principal
            .as_deref()
            .is_some_and(|principal| principal != binding.principal)
        {
            return false;
        }

        if self
            .host
            .as_deref()
            .is_some_and(|host| host != binding.host)
        {
            return false;
        }

        match self.pattern {
            Pattern::Any => self.name_matches_any(binding),

            // "Rules that would select this name", which is what makes
            // `kafka-acls.sh --list --topic foo` show the `PREFIXED` rule on
            // `fo` as well as the literal one on `foo` — the question an
            // operator is actually asking when a topic is unexpectedly denied.
            Pattern::Match => self
                .resource_name
                .as_deref()
                .is_none_or(|name| binding.selects(binding.resource_type, name)),

            pattern if pattern == binding.pattern => self.name_matches_any(binding),

            _ => false,
        }
    }

    /// The name half of a filter that is not asking about pattern semantics:
    /// absent matches everything, present matches exactly.
    fn name_matches_any(&self, binding: &AclBinding) -> bool {
        self.resource_name
            .as_deref()
            .is_none_or(|name| name == binding.resource_name)
    }
}

/// Every ACL in a cluster, as one object.
///
/// One object rather than one per rule, and a set rather than a list. Both
/// follow from how it is read: the request path needs *all* of them to answer
/// any question, so paying one conditional GET for the lot and caching it
/// under its etag is the whole design — a per-rule keyspace would mean a LIST
/// on the authorization path, which is the one place that can least afford it.
///
/// A `BTreeSet` because a duplicate rule is not a second rule, and because the
/// serialised order then does not depend on the order they were created in:
/// two clusters given the same ACLs hold byte-identical objects, which is what
/// makes the etag meaningful.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Acls {
    pub bindings: BTreeSet<AclBinding>,
}

impl Acls {
    /// Every binding this filter selects.
    pub fn matching<'a>(&'a self, filter: &'a AclFilter) -> impl Iterator<Item = &'a AclBinding> {
        self.bindings
            .iter()
            .filter(move |binding| filter.matches(binding))
    }

    /// Whether `principal`, connecting from `host`, may perform `operation`
    /// against the named resource.
    ///
    /// Kafka's evaluation order, and the order matters at every step:
    ///
    /// 1. **`DENY` wins.** Any matching denial ends it, whatever else is
    ///    written, so an operator can carve an exception out of a broad grant
    ///    and be sure it holds.
    /// 2. **then `ALLOW`**, following the implication table — a grant of `READ`
    ///    answers a question about `DESCRIBE`.
    /// 3. **otherwise deny.** No rule is not permission. On a mutualised fleet
    ///    the alternative is that every resource nobody has written a rule
    ///    about is readable by every tenant, which is the state this whole
    ///    issue is about.
    ///
    /// Super users are not consulted here: they never reach this function.
    /// See [`crate::Authorizer`].
    #[must_use]
    pub fn allows(
        &self,
        principal: &str,
        host: &str,
        resource_type: Resource,
        resource_name: &str,
        operation: Operation,
    ) -> bool {
        let applicable = || {
            self.bindings.iter().filter(move |binding| {
                binding.applies_to(principal)
                    && binding.applies_at(host)
                    && binding.selects(resource_type, resource_name)
            })
        };

        // Exact, not implied: denying `READ` must not deny `DESCRIBE`.
        if applicable()
            .any(|binding| binding.permission == Permission::Deny && binding.covers(operation))
        {
            return false;
        }

        applicable()
            .any(|binding| binding.permission == Permission::Allow && binding.grants(operation))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(resource_name: &str, pattern: Pattern) -> AclBinding {
        AclBinding {
            resource_type: Resource::Topic,
            resource_name: resource_name.into(),
            pattern,
            principal: "User:alice".into(),
            host: WILDCARD_HOST.into(),
            operation: Operation::Read,
            permission: Permission::Allow,
        }
    }

    /// The pattern that makes multi-tenancy work without the broker knowing
    /// what a tenant is: `PREFIXED` on `tenant-a.` selects that tenant's topics
    /// and nobody else's.
    #[test]
    fn a_prefixed_rule_selects_its_prefix_and_nothing_else() {
        let rule = binding("tenant-a.", Pattern::Prefixed);

        assert!(rule.selects(Resource::Topic, "tenant-a.orders"));
        assert!(rule.selects(Resource::Topic, "tenant-a."));

        assert!(!rule.selects(Resource::Topic, "tenant-b.orders"));
        assert!(
            !rule.selects(Resource::Topic, "tenant-ab.orders"),
            "a prefix is a prefix of the *name*, and `tenant-a` is not a \
             delimiter — but `tenant-a.` is, which is why the rule carries the dot",
        );

        // The resource type is part of the selection: a group called
        // `tenant-a.workers` is not selected by a topic rule.
        assert!(!rule.selects(Resource::Group, "tenant-a.orders"));
    }

    #[test]
    fn a_literal_rule_selects_one_name_or_every_name() {
        let one = binding("orders", Pattern::Literal);

        assert!(one.selects(Resource::Topic, "orders"));
        assert!(!one.selects(Resource::Topic, "orders.dlq"));
        assert!(!one.selects(Resource::Topic, "order"));

        let every = binding(WILDCARD_RESOURCE, Pattern::Literal);

        assert!(every.selects(Resource::Topic, "orders"));
        assert!(every.selects(Resource::Topic, "anything"));
    }

    /// A stored rule carrying a filter-only pattern selects nothing.
    ///
    /// `ANY` and `MATCH` mean something when *asking* and nothing when stored,
    /// so a binding that somehow carries one — a client sending a filter where
    /// a rule belongs, an object written by a future version — must be inert.
    /// Guessing the other way round is a rule that grants more than it says.
    #[test]
    fn a_filter_only_pattern_grants_nothing() {
        for pattern in [Pattern::Any, Pattern::Match, Pattern::Unknown] {
            assert!(
                !binding("orders", pattern).selects(Resource::Topic, "orders"),
                "{pattern:?} must not select when stored as a rule",
            );
        }
    }

    #[test]
    fn a_wildcard_principal_and_host_apply_to_everyone_everywhere() {
        let named = AclBinding {
            principal: "User:alice".into(),
            host: "10.0.0.1".into(),
            ..binding("orders", Pattern::Literal)
        };

        assert!(named.applies_to("User:alice"));
        assert!(!named.applies_to("User:bob"));
        assert!(named.applies_at("10.0.0.1"));
        assert!(!named.applies_at("10.0.0.2"));

        let anyone = AclBinding {
            principal: WILDCARD_PRINCIPAL.into(),
            host: WILDCARD_HOST.into(),
            ..binding("orders", Pattern::Literal)
        };

        assert!(anyone.applies_to("User:bob"));
        assert!(anyone.applies_at("10.0.0.2"));
    }

    /// `ALL` is the one implication a binding resolves; the rest belong with
    /// the question being asked.
    #[test]
    fn all_covers_every_operation() {
        let read = binding("orders", Pattern::Literal);
        assert!(read.covers(Operation::Read));
        assert!(!read.covers(Operation::Write));

        let all = AclBinding {
            operation: Operation::All,
            ..read
        };
        assert!(all.covers(Operation::Read));
        assert!(all.covers(Operation::Write));
        assert!(all.covers(Operation::Delete));
    }

    /// An absent filter field matches everything, which is the difference
    /// between a filter and a rule — and the difference between deleting what
    /// was asked for and deleting the cluster's authorization.
    #[test]
    fn an_absent_filter_field_matches_everything() {
        let acls = Acls {
            bindings: BTreeSet::from([
                binding("orders", Pattern::Literal),
                AclBinding {
                    principal: "User:bob".into(),
                    ..binding("payments", Pattern::Literal)
                },
            ]),
        };

        let everything = AclFilter {
            resource_type: Resource::Any,
            pattern: Pattern::Any,
            operation: Operation::Any,
            permission: Permission::Any,
            ..Default::default()
        };

        assert_eq!(2, acls.matching(&everything).count());

        let alice = AclFilter {
            principal: Some("User:alice".into()),
            ..everything.clone()
        };

        assert_eq!(1, acls.matching(&alice).count());

        let payments = AclFilter {
            resource_name: Some("payments".into()),
            ..everything
        };

        assert_eq!(1, acls.matching(&payments).count());
    }

    /// `MATCH` asks "which rules would select this name", which is what an
    /// operator wants when a topic is unexpectedly denied: the literal rule
    /// *and* the prefixed one that also covers it.
    #[test]
    fn match_finds_every_rule_that_would_select_a_name() {
        let acls = Acls {
            bindings: BTreeSet::from([
                binding("tenant-a.orders", Pattern::Literal),
                binding("tenant-a.", Pattern::Prefixed),
                binding("tenant-b.", Pattern::Prefixed),
            ]),
        };

        let selecting = AclFilter {
            resource_type: Resource::Topic,
            resource_name: Some("tenant-a.orders".into()),
            pattern: Pattern::Match,
            operation: Operation::Any,
            permission: Permission::Any,
            ..Default::default()
        };

        assert_eq!(2, acls.matching(&selecting).count());
    }

    /// The object an operator reads at 3am. Names, not the wire's integers.
    #[test]
    fn the_stored_object_is_readable() {
        assert_eq!(
            r#"{"bindings":[{"resource_type":"topic","resource_name":"tenant-a.","pattern":"prefixed","principal":"User:alice","host":"*","operation":"read","permission":"allow"}]}"#,
            serde_json::to_string(&Acls {
                bindings: BTreeSet::from([binding("tenant-a.", Pattern::Prefixed)]),
            })
            .unwrap()
        );
    }

    /// A duplicate rule is not a second rule, and the order rules were created
    /// in must not reach the bytes: two clusters given the same ACLs hold the
    /// same object, which is what makes its etag mean anything.
    #[test]
    fn the_same_acls_are_the_same_object_however_they_arrived() {
        let one = Acls {
            bindings: BTreeSet::from([
                binding("b", Pattern::Literal),
                binding("a", Pattern::Literal),
                binding("a", Pattern::Literal),
            ]),
        };

        let other = Acls {
            bindings: BTreeSet::from([
                binding("a", Pattern::Literal),
                binding("b", Pattern::Literal),
            ]),
        };

        assert_eq!(2, one.bindings.len(), "a duplicate is not a second rule");
        assert_eq!(
            serde_json::to_string(&one).unwrap(),
            serde_json::to_string(&other).unwrap(),
        );
    }

    /// No rule is not permission.
    ///
    /// On a mutualised fleet the alternative is that every resource nobody has
    /// written a rule about is readable by every tenant, which is the state
    /// #363 is about.
    #[test]
    fn nothing_is_allowed_by_default() {
        let none = Acls::default();

        assert!(!none.allows(
            "User:alice",
            "10.0.0.1",
            Resource::Topic,
            "orders",
            Operation::Read
        ));
    }

    /// The rule that makes a mutualised fleet safe: one tenant's prefix grants
    /// nothing outside it.
    #[test]
    fn a_prefixed_grant_does_not_reach_another_prefix() {
        let acls = Acls {
            bindings: BTreeSet::from([binding("tenant-a.", Pattern::Prefixed)]),
        };

        assert!(acls.allows(
            "User:alice",
            "10.0.0.1",
            Resource::Topic,
            "tenant-a.orders",
            Operation::Read,
        ));

        assert!(!acls.allows(
            "User:alice",
            "10.0.0.1",
            Resource::Topic,
            "tenant-b.orders",
            Operation::Read,
        ));

        // Another principal gets nothing from it.
        assert!(!acls.allows(
            "User:bob",
            "10.0.0.1",
            Resource::Topic,
            "tenant-a.orders",
            Operation::Read,
        ));
    }

    /// A denial ends it, whatever else is written — which is what lets an
    /// operator carve an exception out of a broad grant and be sure it holds.
    #[test]
    fn deny_beats_allow_however_broad_the_grant() {
        let acls = Acls {
            bindings: BTreeSet::from([
                AclBinding {
                    resource_name: WILDCARD_RESOURCE.into(),
                    operation: Operation::All,
                    ..binding(WILDCARD_RESOURCE, Pattern::Literal)
                },
                AclBinding {
                    permission: Permission::Deny,
                    ..binding("secrets", Pattern::Literal)
                },
            ]),
        };

        assert!(acls.allows(
            "User:alice",
            "10.0.0.1",
            Resource::Topic,
            "orders",
            Operation::Read
        ));

        assert!(
            !acls.allows(
                "User:alice",
                "10.0.0.1",
                Resource::Topic,
                "secrets",
                Operation::Read
            ),
            "a denial must not be outvoted by a wildcard grant",
        );
    }

    /// Implication is asymmetric, and this is the half that is easy to get
    /// wrong: reading the table symmetrically would silently widen every
    /// `DENY` an operator wrote.
    #[test]
    fn implication_grants_but_never_denies() {
        let granted = Acls {
            bindings: BTreeSet::from([binding("orders", Pattern::Literal)]),
        };

        assert!(
            granted.allows(
                "User:alice",
                "10.0.0.1",
                Resource::Topic,
                "orders",
                Operation::Describe
            ),
            "a client allowed to read a topic must be able to see it exists",
        );

        assert!(!granted.allows(
            "User:alice",
            "10.0.0.1",
            Resource::Topic,
            "orders",
            Operation::Write
        ));

        // Denying READ and separately allowing DESCRIBE leaves DESCRIBE
        // allowed: the denial speaks to READ alone.
        let denied = Acls {
            bindings: BTreeSet::from([
                AclBinding {
                    permission: Permission::Deny,
                    ..binding("orders", Pattern::Literal)
                },
                AclBinding {
                    operation: Operation::Describe,
                    ..binding("orders", Pattern::Literal)
                },
            ]),
        };

        assert!(
            denied.allows(
                "User:alice",
                "10.0.0.1",
                Resource::Topic,
                "orders",
                Operation::Describe
            ),
            "denying READ must not deny DESCRIBE",
        );

        assert!(!denied.allows(
            "User:alice",
            "10.0.0.1",
            Resource::Topic,
            "orders",
            Operation::Read
        ));

        assert!(
            Acls {
                bindings: BTreeSet::from([AclBinding {
                    operation: Operation::AlterConfigs,
                    ..binding("orders", Pattern::Literal)
                }]),
            }
            .allows(
                "User:alice",
                "10.0.0.1",
                Resource::Topic,
                "orders",
                Operation::DescribeConfigs
            ),
        );
    }

    /// A host-scoped rule applies where it says and nowhere else.
    #[test]
    fn a_host_scoped_grant_is_scoped_to_its_host() {
        let acls = Acls {
            bindings: BTreeSet::from([AclBinding {
                host: "10.0.0.1".into(),
                ..binding("orders", Pattern::Literal)
            }]),
        };

        assert!(acls.allows(
            "User:alice",
            "10.0.0.1",
            Resource::Topic,
            "orders",
            Operation::Read
        ));

        assert!(!acls.allows(
            "User:alice",
            "10.0.0.2",
            Resource::Topic,
            "orders",
            Operation::Read
        ));
    }
}
