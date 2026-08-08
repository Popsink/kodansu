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

//! What a scaled-up replica costs the client that lands on it (#362).
//!
//! Scaling out is only transparent if a fresh replica answers inside the
//! client's `request.timeout.ms` — 30 s by default. That budget is very
//! probably comfortable, which is exactly why it is worth measuring rather than
//! assuming: it is the number that decides whether a KEDA scale-out is a
//! non-event or a wave of client-side timeouts, and nothing else in the tree
//! records it.
//!
//! Measured from `build` — the storage container, the schema assertion, the
//! coordinator — through `serve` to a client's `ApiVersions` and `Metadata`
//! answered over a real socket. That is the whole of what a replica does before
//! it is useful, minus the container image pull, which is Kubernetes' half of
//! the budget and not this test's to measure.

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener},
    time::Duration,
};

use anyhow::{Result, anyhow};
use tansu_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use tansu_sans_io::{ApiKey as _, Body, Frame, Header, MetadataRequest};
use tansu_storage::ArcDynStorage;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::info;
use url::Url;
use uuid::Uuid;

use crate::common::init_tracing;

pub mod common;

/// The client-side budget this has to fit inside: `request.timeout.ms`, whose
/// Kafka default is 30 s.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The fraction of that budget a cold start may take before this test says so.
///
/// A tenth, deliberately generous against a measurement that runs on shared CI
/// hardware: the point is to catch a cold start that has become *seconds*, not
/// to pin milliseconds. Anything approaching this is a change in kind — a
/// listing on the startup path, a synchronous warm-up — and worth a look.
const BUDGET: Duration = Duration::from_secs(3);

fn free_port() -> Result<u16> {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .map_err(Into::into)
}

/// One request over a fresh connection, returning when its response is decoded.
async fn round_trip(addr: SocketAddr, api_key: i16, api_version: i16, body: Body) -> Result<()> {
    let request = Frame::request(
        Header::Request {
            api_key,
            api_version,
            correlation_id: 0,
            client_id: Some("cold-start".into()),
        },
        body,
    )?;

    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(&request).await?;

    let mut size = [0u8; 4];
    _ = stream.read_exact(&mut size).await?;

    let mut response = vec![0u8; u32::from_be_bytes(size) as usize];
    _ = stream.read_exact(&mut response).await?;

    Ok(())
}

/// A replica that has just been scaled up answers a client well inside its
/// timeout.
#[tokio::test(flavor = "multi_thread")]
async fn a_scaled_up_replica_answers_inside_the_client_timeout() -> Result<()> {
    let _guard = init_tracing()?;

    let port = free_port()?;
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let cancellation = CancellationToken::new();

    // The clock starts where the process would: nothing has been built yet.
    let started = Instant::now();

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

    let built = started.elapsed();

    let serving = tokio::spawn(async move { broker.serve(Instant::now()).await });

    // `Metadata` is what a client asks first and what it cannot proceed
    // without: it carries the broker list and the topic assignment, so a
    // replica that answers it is a replica the client can use.
    let answered = timeout(REQUEST_TIMEOUT, async {
        loop {
            if round_trip(
                addr,
                MetadataRequest::KEY,
                0,
                MetadataRequest::default().topics(Some(vec![])).into(),
            )
            .await
            .is_ok()
            {
                return started.elapsed();
            }

            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|elapsed| {
        anyhow!("a cold replica did not answer inside {REQUEST_TIMEOUT:?}: {elapsed}")
    })?;

    info!(
        built_ms = built.as_millis() as u64,
        answered_ms = answered.as_millis() as u64,
        budget_ms = BUDGET.as_millis() as u64,
        client_timeout_ms = REQUEST_TIMEOUT.as_millis() as u64,
        "cold start"
    );

    assert!(
        answered < BUDGET,
        "a cold start took {answered:?}, over the {BUDGET:?} this test allows of the \
         client's {REQUEST_TIMEOUT:?} — a scale-out is only transparent while it fits",
    );

    cancellation.cancel();
    _ = timeout(Duration::from_secs(10), serving).await??;

    Ok(())
}
