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

//! Saying who is asking, so the storage engine can decide (#363).
//!
//! Three crates each hold one part of the answer and none of them can hold all
//! three. `tansu-auth` knows the SASL session but depends on `tansu-storage`,
//! so it cannot be depended on by it. `tansu-service` knows the peer address
//! and depends on neither. `tansu-storage` holds the rules and the decision and
//! must not learn how to authenticate anybody.
//!
//! This crate depends on all three, which makes it the only place the
//! translation can happen: read the authenticated id off the session, read the
//! address off the connection, and put the pair where the decision can find it.

use std::net::SocketAddr;

use rama::{Context, Layer, Service};
use tansu_auth::Authentication;
use tansu_service::Peer;
use tansu_storage::{Authorizer, Requester};

/// The principal type a SASL-authenticated client is given.
///
/// Kafka's principals are `Type:name`, and SASL only ever produces `User`. The
/// other types in Kafka's model come from an authorizer plugin, which this
/// broker does not have — so this is a constant rather than a configuration,
/// and naming it is what keeps `User:alice` from being spelled by hand in two
/// places that could drift.
const PRINCIPAL_TYPE: &str = "User";

/// Puts the [`Requester`] — and the [`Authorizer`], when there is one — into
/// every request's context.
#[derive(Clone, Debug, Default)]
pub struct RequesterLayer {
    /// `None` on a broker without `--authentication`: there are no principals,
    /// so there is nothing to authorize, and inserting an authorizer would
    /// refuse every request on every deployment that has never turned
    /// authentication on.
    authorizer: Option<Authorizer>,
}

impl RequesterLayer {
    #[must_use]
    pub fn new(authorizer: Option<Authorizer>) -> Self {
        Self { authorizer }
    }
}

impl<S> Layer<S> for RequesterLayer {
    type Service = RequesterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            authorizer: self.authorizer.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RequesterService<S> {
    inner: S,
    authorizer: Option<Authorizer>,
}

impl<S, State, Request> Service<State, Request> for RequesterService<S>
where
    S: Service<State, Request>,
    State: Clone + Send + Sync + 'static,
    Request: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    async fn serve(
        &self,
        mut ctx: Context<State>,
        req: Request,
    ) -> Result<Self::Response, Self::Error> {
        // Read here rather than when the connection was accepted: SASL happens
        // *on* the connection, so at accept time there is no principal yet.
        // `Authentication` is a handle onto the session and is re-read per
        // request, which is also what makes re-authentication (KIP-368) visible
        // without anything else being told.
        //
        // Qualified with its type on the way through, because an authentication
        // identity and an authorization principal are not the same string. SASL
        // yields a bare `alice`; every rule ever written says `User:alice` —
        // that is what the wire carries, what `kafka-acls.sh` prints, what
        // `--super-users` is documented to take, and what `User:*` has to match.
        // Comparing the bare name against any of them matches nothing, so a
        // cluster with authentication on denies everything to everyone,
        // including the super user who is the only way to write the first rule.
        let principal = ctx
            .get::<Authentication>()
            .and_then(Authentication::principal)
            .map(|auth_id| principal_of(&auth_id));

        let host = ctx
            .get::<Peer>()
            .map(|Peer(addr)| host_of(*addr))
            .unwrap_or_default();

        _ = ctx.insert(Requester { principal, host });

        if let Some(authorizer) = self.authorizer.clone() {
            _ = ctx.insert(authorizer);
        }

        self.inner.serve(ctx, req).await
    }
}

/// The principal an ACL is written against, from the id SASL authenticated.
///
/// The translation between two namespaces that look alike and are not: SASL's
/// `alice` and authorization's `User:alice`. Nothing downstream can bridge them
/// — a rule carries the qualified form because that is what the wire carries,
/// and `--super-users` takes it for the same reason.
fn principal_of(auth_id: &str) -> String {
    format!("{PRINCIPAL_TYPE}:{auth_id}")
}

/// The address an ACL's `host` is written against.
///
/// The address alone, without the ephemeral port: an operator writes
/// `--allow-host 10.0.0.1`, and a rule that had to name a port would match one
/// connection and never the next.
fn host_of(addr: SocketAddr) -> String {
    addr.ip().to_string()
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use tansu_sans_io::{
        acl::{Operation, Permission, Resource},
        resource::Pattern,
    };
    use tansu_storage::{AclBinding, WILDCARD_HOST, WILDCARD_PRINCIPAL};

    use super::*;

    /// The port is ephemeral and an ACL is written against an address, so
    /// carrying it would mean a rule that matches one connection and never the
    /// next.
    #[test]
    fn a_host_is_an_address_without_its_port() {
        assert_eq!(
            "10.0.0.1",
            host_of(SocketAddr::from((
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                54321
            ))),
        );

        assert_eq!(
            "::1",
            host_of(SocketAddr::from((IpAddr::V6(Ipv6Addr::LOCALHOST), 54321))),
        );
    }

    fn rule_for(principal: &str) -> AclBinding {
        AclBinding {
            resource_type: Resource::Topic,
            resource_name: "orders".into(),
            pattern: Pattern::Literal,
            principal: principal.into(),
            host: WILDCARD_HOST.into(),
            operation: Operation::Read,
            permission: Permission::Allow,
        }
    }

    /// The invariant the whole of #363 rests on: what SASL authenticates and
    /// what a rule is written against must be the *same string*.
    ///
    /// They are two namespaces that look alike. SASL yields `alice`; every rule
    /// on the wire, every `kafka-acls.sh` line and every `--super-users` entry
    /// says `User:alice`. Handing the bare id to the decision matches no rule
    /// naming anybody, and no super user — so a broker with authentication on
    /// denies everything to everyone, permanently, including the super user who
    /// is the only way to write the first rule.
    ///
    /// Every unit test of the rules themselves passed throughout, because they
    /// all fed the qualified form that production never produced.
    #[test]
    fn an_authenticated_id_becomes_the_principal_a_rule_is_written_against() {
        let principal = principal_of("alice");

        assert_eq!("User:alice", principal);
        assert!(rule_for("User:alice").applies_to(&principal));
        assert!(rule_for(WILDCARD_PRINCIPAL).applies_to(&principal));

        // What the bare id did, and what this exists to keep it from doing
        // again. Only a rule that names somebody: `User:*` matches whatever it
        // is handed, which is why a cluster whose only rule was a wildcard
        // looked like it worked.
        assert!(!rule_for("User:alice").applies_to("alice"));
    }

    /// `--super-users` is documented as `User:admin`, and is compared against
    /// this same string. A bare id makes the escape hatch unreachable.
    #[test]
    fn a_super_user_is_named_the_way_the_option_documents_it() {
        assert_eq!("User:admin", principal_of("admin"));
    }
}
