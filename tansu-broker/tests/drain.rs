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

//! A replica asked to stop drains rather than dropping what it is holding
//! (#361).
//!
//! The broker holds requests open by design — `Fetch` waits out `max.wait.ms`,
//! `JoinGroup` and `SyncGroup` long-poll across the rebalance window. At a fixed
//! replica count losing those on shutdown happened on deploy and was mostly
//! tolerated. Under an autoscaler it happens every time the fleet scales in,
//! which is routine rather than exceptional, and a cut poll reaches the client
//! as a closed socket — indistinguishable from a network fault.
//!
//! The long polls answering early is covered where they live, against the
//! coordinator. What needs a real socket is the half above them: the listener
//! closing, so a client reaching a stopping replica is *refused* rather than
//! left waiting in a backlog nothing will accept, and the accept loop waiting
//! for its connections before it returns.

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener},
    time::Duration,
};

use anyhow::{Result, anyhow};
use tansu_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use tansu_sans_io::{ApiKey as _, ApiVersionsRequest, Frame, Header};
use tansu_storage::ArcDynStorage;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::common::init_tracing;

pub mod common;

/// A port nothing else is on. Bound and released rather than guessed, which is
/// what makes a parallel test run deterministic.
fn free_port() -> Result<u16> {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .map_err(Into::into)
}

/// A connection the broker has actually **served** one request on, returned
/// still open.
///
/// A bare `TcpStream::connect` is not enough and quietly makes this test prove
/// nothing: the kernel completes the handshake from the listen backlog, so the
/// connect succeeds whether or not the accept loop ever reached it, and the
/// drain then has no connection to wait for. A round trip is what says a
/// connection task exists — and, once it has answered, that the task is back in
/// its read waiting for a frame that will never come, which is the state every
/// idle client connection is in.
async fn served_connection(addr: SocketAddr, within: Duration) -> Result<TcpStream> {
    let deadline = Instant::now() + within;

    // v0, so the body is empty and this needs no version negotiation to build.
    let request = Frame::request(
        Header::Request {
            api_key: ApiVersionsRequest::KEY,
            api_version: 0,
            correlation_id: 0,
            client_id: Some("drain".into()),
        },
        ApiVersionsRequest::default().into(),
    )?;

    loop {
        match TcpStream::connect(addr).await {
            Ok(mut stream) => {
                stream.write_all(&request).await?;

                let mut size = [0u8; 4];
                _ = stream.read_exact(&mut size).await?;

                let mut response = vec![0u8; u32::from_be_bytes(size) as usize];
                _ = stream.read_exact(&mut response).await?;

                return Ok(stream);
            }

            Err(error) if Instant::now() >= deadline => {
                return Err(anyhow!("{addr} never accepted: {error}"));
            }

            Err(_) => sleep(Duration::from_millis(20)).await,
        }
    }
}

/// Cancelling a broker closes its listener and returns from `serve`.
///
/// Both halves matter and they fail differently. A listener left bound while
/// nothing accepts is worse than one that is gone: the kernel completes the
/// handshake into the backlog, so a client connecting to a stopping replica
/// waits for a response nobody will ever read, where a refused connect is an
/// error it acts on immediately. And a `serve` that does not return means the
/// process is killed by its grace period rather than stopping on its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_stopping_broker_closes_its_listener_and_returns() -> Result<()> {
    let _guard = init_tracing()?;

    let port = free_port()?;
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let cancellation = CancellationToken::new();

    let mut broker = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(Uuid::now_v7().to_string())
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(Url::parse(&format!("tcp://127.0.0.1:{port}"))?)
        .storage(Url::parse("memory://")?)
        .listener(Url::parse(&format!("tcp://127.0.0.1:{port}"))?)
        .cancellation(cancellation.clone())
        .silent(true)
        .build()
        .await?;

    let serving = tokio::spawn(async move { broker.serve(Instant::now()).await });

    // Served, and then left **open and idle** — which is how a Kafka client
    // keeps its connections between polls, and the case that makes the
    // difference here. A drain that waited for connections to *end* would wait
    // out its whole grace period on every shutdown, on connections that owed
    // nobody anything, and then cut them regardless.
    let mut connected = served_connection(addr, Duration::from_secs(10)).await?;

    cancellation.cancel();

    // What the client sees: a clean close between requests, which every Kafka
    // client handles by reconnecting — and which it can only see because the
    // connection was idle. A closed socket *during* a request is the one this
    // must never become (#300).
    let mut byte = [0u8; 1];

    let closed = timeout(Duration::from_secs(5), connected.read(&mut byte))
        .await
        .map_err(|elapsed| {
            anyhow!("an idle connection was not closed by the drain: {elapsed}")
        })??;

    assert_eq!(
        0, closed,
        "the drain must close an idle connection, not write to it"
    );

    // `serve` returns on its own, and *promptly*: well inside the drain's own
    // timeout, which is what says the idle connection was closed rather than
    // waited out. Without that, this passes only by running the grace period
    // down — so the bound is the assertion, not the completion.
    timeout(Duration::from_secs(5), serving)
        .await
        .map_err(|elapsed| anyhow!("serve did not return promptly after cancellation: {elapsed}"))?
        .map_err(|joined| anyhow!("the serve task panicked: {joined}"))??;

    // And the socket is gone, so the load balancer's health check fails and a
    // client that arrives anyway is refused rather than queued.
    let refused = timeout(Duration::from_secs(5), async {
        loop {
            if TcpStream::connect(addr).await.is_err() {
                return;
            }

            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    assert!(
        refused.is_ok(),
        "a stopped broker must refuse connections, not queue them in a backlog nobody accepts",
    );

    Ok(())
}
