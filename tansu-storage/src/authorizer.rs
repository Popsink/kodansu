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

//! Asking the ACLs a question, cheaply enough to do it per request (#363).
//!
//! The rules live in one object and the decision is a pure function of them
//! ([`Acls::allows`]). What this adds is the part that makes asking viable on
//! the hot path: a snapshot held for a short TTL, so a produce of a thousand
//! records costs no object-store round trip to authorize, and a rule an
//! operator applies takes effect across the fleet within that window without
//! anything being told about it.
//!
//! **Absence is the disabled state.** A request path with no `Authorizer` in
//! its context authorizes nothing, which is what a broker started without
//! `--authentication` does: there are no principals, so there is nothing to
//! evaluate and denying would refuse every request on every existing
//! deployment. The broker inserts one exactly when authentication is on.

use std::{
    collections::BTreeSet,
    fmt::{self, Debug},
    sync::{Arc, Mutex},
};

use tansu_sans_io::acl::{Operation, Resource};
use tokio::time::{Duration, Instant};
use tracing::{debug, error, warn};

use crate::{AclFilter, Acls, ArcDynStorage};

/// How long a snapshot of the ACLs is served before it is re-read.
///
/// The same five seconds the object-store metadata cache holds an etag for,
/// and for the same reason: it is short enough that an operator applying a
/// rule sees it take effect while they are still watching, and long enough
/// that the authorization of a busy topic costs nothing. The re-read itself is
/// a conditional GET, so a fleet whose ACLs are not changing pays a 304 per
/// replica per window.
pub const ACL_SNAPSHOT_TTL: Duration = Duration::from_secs(5);

/// The cluster's ACLs as last read, and when.
///
/// `Option` because "never read" and "read and empty" are different answers:
/// the first denies and says so, the second is a cluster with no rules.
type Snapshot = Arc<Mutex<Option<(Arc<Acls>, Instant)>>>;

/// The ACLs of a cluster, cached, and the principals that bypass them.
#[derive(Clone)]
pub struct Authorizer {
    storage: ArcDynStorage,

    /// Principals allowed everything without consulting a rule.
    ///
    /// Without these the first ACL could never be written: a fail-closed
    /// broker with no rules denies `CreateAcls` like everything else, and the
    /// cluster is bricked with no way in. Kafka has the same escape and for
    /// the same reason.
    super_users: Arc<BTreeSet<String>>,

    snapshot: Snapshot,

    ttl: Duration,
}

impl Debug for Authorizer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Authorizer")
            .field("super_users", &self.super_users)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl Authorizer {
    pub fn new(storage: ArcDynStorage, super_users: impl IntoIterator<Item = String>) -> Self {
        let super_users = super_users.into_iter().collect::<BTreeSet<_>>();

        if super_users.is_empty() {
            // Not an error — a cluster can be run entirely on rules an earlier
            // super user wrote — but it is the configuration that cannot
            // recover from having no rules, and finding that out is otherwise
            // a `CreateAcls` that is denied for reasons nobody can see.
            warn!(
                "no super users configured: if this cluster has no ACLs, nothing can write the \
                 first one"
            );
        }

        Self {
            storage,
            super_users: Arc::new(super_users),
            snapshot: Arc::new(Mutex::new(None)),
            ttl: ACL_SNAPSHOT_TTL,
        }
    }

    /// Override how long a snapshot is served. Tests set it to zero to see a
    /// rule take effect without waiting one out.
    #[must_use]
    pub fn with_ttl(self, ttl: Duration) -> Self {
        Self { ttl, ..self }
    }

    /// Whether `principal` is allowed everything without consulting a rule.
    #[must_use]
    pub fn is_super_user(&self, principal: &str) -> bool {
        self.super_users.contains(principal)
    }

    /// Whether `principal`, connecting from `host`, may perform `operation` on
    /// the named resource.
    pub async fn allows(
        &self,
        principal: &str,
        host: &str,
        resource_type: Resource,
        resource_name: &str,
        operation: Operation,
    ) -> bool {
        if self.is_super_user(principal) {
            return true;
        }

        let Some(acls) = self.acls().await else {
            // No snapshot and the read failed: this replica does not know what
            // the rules are. Denying is the only answer that cannot leak, and
            // it is loud rather than silent because a fleet that cannot read
            // its ACLs refuses everything, which is an outage and should look
            // like one.
            error!(
                principal,
                host,
                ?resource_type,
                resource_name,
                ?operation,
                "denying: this replica could not read the cluster's ACLs"
            );

            return false;
        };

        let allowed = acls.allows(principal, host, resource_type, resource_name, operation);

        debug!(
            principal,
            host,
            ?resource_type,
            resource_name,
            ?operation,
            allowed
        );

        allowed
    }

    /// The current snapshot, re-read when it has aged out.
    ///
    /// A failed re-read serves the **stale** snapshot rather than denying: the
    /// rules change on an operator's timescale and an object store hiccups on
    /// its own, so refusing the fleet because a GET timed out trades a
    /// correctness risk that does not exist for an outage that does. `None` —
    /// and a denial — only when there has never been a snapshot at all.
    async fn acls(&self) -> Option<Arc<Acls>> {
        if let Some(fresh) = self.cached(true) {
            return Some(fresh);
        }

        match self.storage.describe_acls(&AclFilter::any()).await {
            Ok(bindings) => {
                let acls = Arc::new(Acls {
                    bindings: bindings.into_iter().collect(),
                });

                if let Ok(mut snapshot) = self.snapshot.lock() {
                    *snapshot = Some((acls.clone(), Instant::now()));
                }

                Some(acls)
            }

            Err(error) => {
                warn!(
                    ?error,
                    "could not refresh the acls; serving the last snapshot"
                );

                self.cached(false)
            }
        }
    }

    /// The held snapshot, optionally only when it is still inside the TTL.
    fn cached(&self, fresh_only: bool) -> Option<Arc<Acls>> {
        self.snapshot.lock().ok().and_then(|snapshot| {
            snapshot.as_ref().and_then(|(acls, read_at)| {
                (!fresh_only || read_at.elapsed() < self.ttl).then(|| acls.clone())
            })
        })
    }
}

#[cfg(all(test, feature = "dynostore"))]
mod tests {
    use object_store::memory::InMemory;
    use tansu_sans_io::{acl::Permission, resource::Pattern};

    use super::*;
    use crate::{AclBinding, Storage, WILDCARD_HOST, dynostore::DynoStore};

    const HOST: &str = "10.0.0.1";

    fn authorizer(super_users: &[&str]) -> Authorizer {
        Authorizer::new(
            Arc::new(Box::new(DynoStore::new("tansu", 111, InMemory::new())) as Box<dyn Storage>),
            super_users.iter().map(|user| (*user).to_owned()),
        )
    }

    fn allow_read(resource_name: &str, principal: &str) -> AclBinding {
        AclBinding {
            resource_type: Resource::Topic,
            resource_name: resource_name.into(),
            pattern: Pattern::Prefixed,
            principal: principal.into(),
            host: WILDCARD_HOST.into(),
            operation: Operation::Read,
            permission: Permission::Allow,
        }
    }

    /// A cluster with no rules refuses everything — and that is exactly why a
    /// super user has to exist, because otherwise the first `CreateAcls` is
    /// refused too and the cluster can never be given any.
    #[tokio::test]
    async fn no_rules_refuses_everyone_except_a_super_user() {
        let authorizer = authorizer(&["User:admin"]);

        assert!(
            !authorizer
                .allows(
                    "User:alice",
                    HOST,
                    Resource::Topic,
                    "orders",
                    Operation::Read
                )
                .await
        );

        assert!(
            authorizer
                .allows(
                    "User:admin",
                    HOST,
                    Resource::Cluster,
                    "kafka-cluster",
                    Operation::Alter
                )
                .await,
            "a super user must be able to write the first ACL into an empty cluster",
        );
    }

    /// A rule an operator applies takes effect, and it takes effect on a
    /// replica that has already answered questions — which is what the TTL is
    /// for, and what a cache without one would get wrong.
    #[tokio::test]
    async fn a_new_rule_takes_effect_when_the_snapshot_ages_out() {
        let authorizer = authorizer(&[]).with_ttl(Duration::ZERO);

        assert!(
            !authorizer
                .allows(
                    "User:alice",
                    HOST,
                    Resource::Topic,
                    "tenant-a.orders",
                    Operation::Read
                )
                .await,
            "nothing is granted before a rule exists",
        );

        _ = authorizer
            .storage
            .create_acls(&[allow_read("tenant-a.", "User:alice")])
            .await
            .expect("create");

        assert!(
            authorizer
                .allows(
                    "User:alice",
                    HOST,
                    Resource::Topic,
                    "tenant-a.orders",
                    Operation::Read
                )
                .await,
            "an applied rule must take effect once the snapshot is re-read",
        );

        assert!(
            !authorizer
                .allows(
                    "User:alice",
                    HOST,
                    Resource::Topic,
                    "tenant-b.orders",
                    Operation::Read
                )
                .await,
            "and must not reach outside its prefix",
        );
    }

    /// Inside the TTL the answer comes from the snapshot, which is the whole
    /// reason this type exists: a produce of a thousand records must not cost
    /// a thousand object-store reads.
    #[tokio::test]
    async fn a_held_snapshot_answers_without_re_reading() {
        let authorizer = authorizer(&[]);

        _ = authorizer
            .storage
            .create_acls(&[allow_read("tenant-a.", "User:alice")])
            .await
            .expect("create");

        assert!(
            authorizer
                .allows(
                    "User:alice",
                    HOST,
                    Resource::Topic,
                    "tenant-a.orders",
                    Operation::Read
                )
                .await
        );

        // Removed behind the snapshot's back. Inside the TTL the held answer
        // stands — a bounded staleness that is stated rather than discovered.
        _ = authorizer
            .storage
            .delete_acls(&[AclFilter::any()])
            .await
            .expect("delete");

        assert!(
            authorizer
                .allows(
                    "User:alice",
                    HOST,
                    Resource::Topic,
                    "tenant-a.orders",
                    Operation::Read
                )
                .await,
            "inside the TTL the snapshot answers, without a read",
        );
    }
}
