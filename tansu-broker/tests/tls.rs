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

//! The client listener is TLS when, and only when, a certificate is configured
//! (#358).
//!
//! The negative test is the one that matters. `--cert`/`--key` were parsed, a
//! `ServerConfig` was built, threaded through every builder stage, turned into a
//! `TlsAcceptor` — and dropped on the next line. Every connection stayed
//! plaintext and nothing said so, which is a failure mode no positive test can
//! see: a TLS listener that quietly also serves plaintext answers a TLS client
//! perfectly well.
//!
//! Driven through `Broker::listen`, deliberately, and not through a hand-rolled
//! accept loop: the defect was in the wiring between the accepted socket and the
//! service stack, so a test that does its own accepting would have passed
//! throughout.

use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use common::init_tracing;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{PrivateKeyDer, ServerName},
};
use tansu_broker::{NODE_ID, broker::Broker, coordinator::group::forward::GroupCoordinator};
use tansu_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, Body, ErrorCode, Frame, Header,
};
use tansu_storage::ArcDynStorage;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Instant, sleep, timeout},
};
use tokio_rustls::TlsConnector;
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub mod common;

/// The SAN of the throwaway certificate, and the SNI the TLS client sends. A DNS
/// name rather than the loopback address: the socket is reached at 127.0.0.1 and
/// the certificate is verified against the name, which is how a real deployment
/// behind a Service name works.
const SERVER_NAME: &str = "tansu.test";

const API_VERSION: i16 = 3;

/// Longer than any of this ever takes, and finite.
///
/// A broker that is not wrapping the stream reads a TLS `ClientHello` as a Kafka
/// frame, and the first four bytes of one declare a length of ~370MB: the read
/// blocks for bytes that will never arrive. Nothing above this caps a test — the
/// `ci` nextest profile names slow tests but terminates none — so a regression to
/// plaintext would hang the job rather than fail it.
const PATIENCE: Duration = Duration::from_secs(15);

/// A self-signed certificate and the client configuration that trusts it.
///
/// Generated per test rather than checked in: no key material in the tree, and
/// nothing to expire.
fn certified() -> Result<(ServerConfig, ClientConfig)> {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec![SERVER_NAME.to_owned()])?;

    let server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            PrivateKeyDer::Pkcs8(signing_key.serialize_der().into()),
        )?;

    let mut roots = RootCertStore::empty();
    roots.add(cert.der().clone())?;

    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok((server, client))
}

/// A port nothing is listening on: bound to learn which one, then released.
///
/// `Broker::listen` binds the URL it was configured with and does not report
/// where it landed, so the port has to be named up front. The gap between the
/// release here and the bind there is a race in principle; in practice the
/// kernel does not hand the same ephemeral port straight back out.
async fn free_port() -> Result<u16> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// A broker serving on an ephemeral loopback port, TLS when `tls` is `Some`.
async fn listening(tls: Option<ServerConfig>) -> Result<u16> {
    let port = free_port().await?;

    let broker = Broker::<GroupCoordinator<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(Uuid::now_v7().to_string())
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(Url::parse(&format!("tcp://{SERVER_NAME}:{port}"))?)
        .storage(Url::parse("memory://tansu/")?)
        .listener(Url::parse(&format!("tcp://127.0.0.1:{port}"))?)
        .tls_server_config(tls)
        .silent(true)
        .build()
        .await?;

    _ = tokio::spawn(async move { broker.listen(Instant::now()).await });

    Ok(port)
}

/// Connect once the broker's own listener is up — `listening` returns before the
/// spawned task has bound.
async fn connect(port: u16) -> Result<TcpStream> {
    for _ in 0..250 {
        match TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await {
            Ok(stream) => return Ok(stream),
            Err(err) => debug!(?err, port, "waiting for the listener"),
        }

        sleep(Duration::from_millis(20)).await;
    }

    Err(anyhow!("nothing listening on {port}"))
}

/// One `ApiVersions` round trip over `stream`, whatever the transport.
///
/// `ApiVersions` because it is answered from the message metadata alone: no
/// broker registration, no storage, nothing that could fail for a reason other
/// than the transport under test.
async fn api_versions<S>(stream: &mut S) -> Result<ApiVersionsResponse>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let request = Frame::request(
        Header::Request {
            api_key: ApiVersionsRequest::KEY,
            api_version: API_VERSION,
            correlation_id: 358,
            client_id: Some("tansu-358".into()),
        },
        Body::ApiVersionsRequest(
            ApiVersionsRequest::default()
                .client_software_name(Some("tansu-tls-test".into()))
                .client_software_version(Some("0".into())),
        ),
    )?;

    stream.write_all(&request).await?;
    stream.flush().await?;

    let mut size = [0u8; 4];
    _ = stream.read_exact(&mut size).await?;

    // The reply to a plaintext request on a TLS listener is a TLS alert record,
    // and its first four bytes read as a declared frame length of ~350MB. Refuse
    // it rather than allocate it: a length no broker would ever answer with is
    // already the answer this test is looking for.
    const MAXIMUM: usize = 1 << 20;
    let length = i32::from_be_bytes(size) as usize + size.len();
    if length > MAXIMUM {
        return Err(anyhow!("{length} is not a Kafka frame length"));
    }

    let mut frame = vec![0u8; length];
    frame[..size.len()].copy_from_slice(&size[..]);
    _ = stream.read_exact(&mut frame[size.len()..]).await?;

    Frame::response_from_bytes(Bytes::from(frame), ApiVersionsResponse::KEY, API_VERSION)
        .and_then(|frame| ApiVersionsResponse::try_from(frame.body))
        .map_err(Into::into)
}

/// The point of the ticket: with a certificate configured, a plaintext client
/// gets no answer.
///
/// Before the fix this returned a perfectly good `ApiVersionsResponse`, on a
/// listener the operator had configured for TLS, with nothing anywhere saying
/// the transport was in the clear.
#[tokio::test]
async fn a_plaintext_client_is_refused_on_a_tls_listener() -> Result<()> {
    let _guard = init_tracing()?;

    let (server, _) = certified()?;
    let port = listening(Some(server)).await?;

    let mut stream = connect(port).await?;

    let answered = timeout(PATIENCE, api_versions(&mut stream)).await;
    debug!(?answered);

    // Elapsed counts as refused — a listener that writes nothing back has refused
    // — but in practice rustls answers the ClientHello that never was with an
    // alert record and closes, so this lands in the `Ok(Err(_))` arm.
    assert!(
        !matches!(answered, Ok(Ok(_))),
        "a plaintext client must not be answered on a TLS listener: {answered:?}"
    );

    Ok(())
}

/// The other half: a TLS client is served normally.
#[tokio::test]
async fn a_tls_client_is_served_on_a_tls_listener() -> Result<()> {
    let _guard = init_tracing()?;

    let (server, client) = certified()?;
    let port = listening(Some(server)).await?;

    let response = timeout(PATIENCE, async move {
        let mut stream = TlsConnector::from(Arc::new(client))
            .connect(
                ServerName::try_from(SERVER_NAME)?.to_owned(),
                connect(port).await?,
            )
            .await?;

        api_versions(&mut stream).await
    })
    .await??;

    debug!(?response);

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    assert!(
        response
            .api_keys
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|api_key| api_key.api_key == ApiVersionsRequest::KEY),
        "a TLS client must get the real API map back"
    );

    Ok(())
}

/// Without a certificate, nothing changes: the listener is plaintext, which is
/// what every deployment that sets neither `--cert` nor `--key` gets.
#[tokio::test]
async fn a_plaintext_client_is_served_when_no_certificate_is_configured() -> Result<()> {
    let _guard = init_tracing()?;

    let port = listening(None).await?;
    let mut stream = connect(port).await?;

    let response = timeout(PATIENCE, api_versions(&mut stream)).await??;
    debug!(?response);

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

    Ok(())
}
