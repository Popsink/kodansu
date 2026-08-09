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
        let principal = ctx
            .get::<Authentication>()
            .and_then(Authentication::principal);

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
}
