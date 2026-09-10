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

//! A broker that answers, for the subcommands that only exist to talk to one.
//!
//! `tansu user` and `tansu topic` are shells over a request and a response,
//! and what can break in a shell is the delegation: a subcommand that builds
//! the wrong request, reads the wrong field, or stops sending one at all. All
//! three still type-check, and none of them is visible to a test that stops at
//! the request (#556). So the tests that matter drive the real `main` against
//! an in-process broker over `memory://` and assert the effect on it.
//!
//! The same shape as `tansu-topic`'s own suite, which is where the pattern
//! comes from.

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};

use tansu_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use tansu_storage::ArcDynStorage;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{Error, Result};

/// A port nothing else is on. Bound and released rather than guessed, which is
/// what makes a parallel test run deterministic.
fn free_port() -> Result<u16> {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .map_err(|error| Error::Box(Box::new(error)))
}

/// A broker serving `memory://` on a free port, and the URL to reach it.
///
/// The [`CancellationToken`] is returned rather than dropped: cancelling it is
/// what stops the accept loop, and a test that forgets leaves the task behind.
pub(crate) async fn broker() -> Result<(Url, CancellationToken)> {
    let port = free_port()?;
    let listener = Url::parse(&format!("tcp://{}:{port}", Ipv4Addr::LOCALHOST))?;
    let cancellation = CancellationToken::new();

    let mut broker = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(Uuid::now_v7().to_string())
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(listener.clone())
        .storage(Url::parse("memory://")?)
        .listener(listener.clone())
        .cancellation(cancellation.clone())
        .silent(true)
        .build()
        .await?;

    let serving = cancellation.clone();
    _ = tokio::spawn(async move {
        _ = broker.serve(Instant::now()).await;
        drop(serving);
    });

    Ok((listener, cancellation))
}
