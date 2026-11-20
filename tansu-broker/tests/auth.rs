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

//! SASL/SCRAM over the broker's framing: `SaslHandshake`, then
//! `SaslAuthenticate` until the client is satisfied, against credentials held
//! in a `memory://` engine.

use std::mem;

use bytes::{BufMut as _, Bytes, BytesMut};
use rama::Service as _;
use rama::{Context, Layer as _};
use rsasl::config::SASLConfig;
use rsasl::mechanisms::scram::tools::{derive_keys, hash_password};
use rsasl::prelude::{Mechname, SASLClient, Session, SessionError, State};
use sha2::digest::core_api::BlockSizeUser;
use sha2::digest::{Digest, FixedOutputReset};
use sha2::{Sha256, Sha512};
use tansu_broker::service::{auth, storage};
use tansu_broker::{Error, NODE_ID, Result};
use tansu_sans_io::Error as ProtocolError;
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::{ApiKey, Body, CreateTopicsRequest, ErrorCode, Frame, Header};
use tansu_sans_io::{SaslAuthenticateRequest, SaslAuthenticateResponse};
use tansu_sans_io::{SaslHandshakeRequest, SaslHandshakeResponse, ScramMechanism};
use tansu_service::FrameRouteService;
use tansu_service::{BytesFrameLayer, BytesFrameService};
use tansu_storage::ScramCredential;

use crate::common::{init_tracing, storage_container};

mod common;

/// The one principal these tests authenticate as.
const USER: &str = "alice";

/// `USER`'s password, from which every stored credential here is derived.
const SECRET: &str = "secret";

const CLIENT: &str = "rdkafka";

const ITERATIONS: i32 = 8192;

type SaslBroker = BytesFrameService<FrameRouteService<(), Error>>;

/// A digest SCRAM can be run over: SHA-256 and SHA-512 here.
trait ScramDigest: Digest + BlockSizeUser + FixedOutputReset + Clone + Sync {}

impl<D> ScramDigest for D where D: Digest + BlockSizeUser + FixedOutputReset + Clone + Sync {}

/// `which` mechanism, and the keys a SCRAM server checks a client's proof against,
/// derived with digest `D` from `SECRET` over `salt`.
fn stored<D: ScramDigest>(
    which: ScramMechanism,
    salt: &'static [u8],
) -> (ScramMechanism, ScramCredential) {
    let mut salted = Default::default();
    hash_password::<D>(SECRET.as_bytes(), ITERATIONS as u32, salt, &mut salted);
    let (client_key, server_key) = derive_keys::<D>(&salted);
    let derived = ScramCredential {
        salt: Bytes::from_static(salt),
        iterations: ITERATIONS,
        stored_key: Bytes::copy_from_slice(&D::digest(client_key)),
        server_key: Bytes::copy_from_slice(&server_key),
    };
    (which, derived)
}

fn scram_256() -> (ScramMechanism, ScramCredential) {
    stored::<Sha256>(ScramMechanism::Scram256, b"k52tay1vvtia5e6lc37gn3f3h")
}

fn scram_512() -> (ScramMechanism, ScramCredential) {
    stored::<Sha512>(ScramMechanism::Scram512, b"3kg0jllwpec5yvd2rc4xzfnab")
}

/// A broker over a fresh `memory://` engine, with SASL enabled and `USER`
/// holding a credential for each `(mechanism, credential)` given.
async fn broker(credentials: Vec<(ScramMechanism, ScramCredential)>) -> Result<SaslBroker> {
    let listener = url::Url::parse("tcp://127.0.0.1:9092/")?;
    let engine = storage_container("tansu", NODE_ID, listener).await?;
    for (mechanism, credential) in credentials {
        let stored = engine.upsert_user_scram_credential(USER, mechanism, credential);
        stored.await?;
    }

    let sasl = tansu_auth::configuration(engine.clone())?;
    let routes =
        storage::services(FrameRouteService::builder(), engine).and_then(auth::services)?;
    let framing = BytesFrameLayer::default().with_sasl_config(Some(sasl));
    Ok(framing.into_layer(routes.build()?))
}

fn mechanism(name: &str) -> Option<&Mechname> {
    Mechname::parse(name.as_bytes()).ok()
}

/// A SCRAM client session for `USER` with `password`, over whichever of the
/// mechanisms the broker `offered` it prefers. SCRAM is client-first.
fn client(offered: &SaslHandshakeResponse, password: &str) -> Session {
    let mechanisms = offered.mechanisms.as_deref().unwrap_or_default();
    let names: Vec<_> = mechanisms.iter().filter_map(|m| mechanism(m)).collect();
    let config = SASLConfig::with_credentials(None, USER.into(), password.into());
    let sasl = SASLClient::new(config.expect("a SASL client config from a user and password"));
    let session = sasl
        .start_suggested(&names)
        .expect("a mechanism both sides support");
    assert!(session.are_we_first(), "a SCRAM client speaks first");
    session
}

/// One connection's worth of requests at `api_version`, numbered from
/// `correlation_id`.
struct Connection {
    broker: SaslBroker,
    api_version: i16,
    correlation_id: i32,
}

impl Connection {
    fn new(broker: SaslBroker, api_version: i16) -> Self {
        Self {
            broker,
            api_version,
            correlation_id: 0,
        }
    }

    async fn send(&mut self, api_key: i16, body: Body) -> Result<Bytes> {
        let header = Header::Request {
            api_key,
            api_version: self.api_version,
            correlation_id: self.correlation_id,
            client_id: Some(CLIENT.to_owned()),
        };
        self.correlation_id += 1;
        let request = Frame::request(header, body)?;
        self.broker.serve(Context::default(), request).await
    }

    async fn handshake(&mut self, mechanism: &str) -> Result<SaslHandshakeResponse> {
        let request = SaslHandshakeRequest::default().mechanism(mechanism.into());
        let response = self.send(SaslHandshakeRequest::KEY, request.into()).await?;
        let key = SaslHandshakeResponse::KEY;
        let frame = Frame::response_from_bytes(response, key, self.api_version)?;
        Ok(SaslHandshakeResponse::try_from(frame.body)?)
    }

    async fn authenticate(&mut self, auth_bytes: Bytes) -> Result<SaslAuthenticateResponse> {
        let request = SaslAuthenticateRequest::default().auth_bytes(auth_bytes);
        let sending = self.send(SaslAuthenticateRequest::KEY, request.into());
        let response = sending.await?;
        let key = SaslAuthenticateResponse::KEY;
        let frame = Frame::response_from_bytes(response, key, self.api_version)?;
        Ok(SaslAuthenticateResponse::try_from(frame.body)?)
    }

    /// Runs the SCRAM exchange as `USER` with `password` over the mechanisms
    /// `offered`, carrying each client message in a `SaslAuthenticate` that
    /// must succeed. Answers how the client's session ended: `Ok` once it has
    /// verified the server.
    async fn scram(
        &mut self,
        offered: &SaslHandshakeResponse,
        password: &str,
    ) -> Result<Result<(), SessionError>> {
        let mut session = client(offered, password);
        let mut input: Option<Bytes> = None;
        let mut token = Vec::new();

        let ended = loop {
            match session.step(input.as_deref(), &mut token) {
                Ok(State::Running) => {
                    let message = Bytes::from(mem::take(&mut token));
                    let answer = self.authenticate(message).await?;
                    assert_eq!(ErrorCode::None, ErrorCode::try_from(answer.error_code)?);
                    input = Some(answer.auth_bytes);
                }
                Ok(State::Finished(_)) => break Ok(()),
                Err(refused) => break Err(refused),
            }
        };
        Ok(ended)
    }
}

#[tokio::test]
async fn scram_sha_256_over_v1_framing() -> Result<()> {
    let _guard = init_tracing()?;

    let mut connection = Connection::new(broker(vec![scram_256()]).await?, 1);
    let offered = connection.handshake("SCRAM-SHA-256").await?;
    assert!(connection.scram(&offered, SECRET).await?.is_ok());
    Ok(())
}

/// A client may authenticate again on a connection it has already
/// authenticated (KIP-368): Java's kafka-clients do so when their session
/// lifetime runs out, and so does Kafka Connect on Strimzi.
///
/// The broker used to answer the second `SaslHandshake` with
/// `UnsupportedSaslMechanism`: the connection's authentication stage only ever
/// moved forward, from server to session to finished, and a finished stage had
/// no way back to accepting a handshake.
#[tokio::test]
async fn scram_sha_256_again_on_the_same_connection() -> Result<()> {
    let _guard = init_tracing()?;

    let mut connection = Connection::new(broker(vec![scram_256()]).await?, 1);
    for attempt in ["first", "second"] {
        let offered = connection.handshake("SCRAM-SHA-256").await?;
        let accepted = ErrorCode::try_from(offered.error_code)?;
        assert_eq!(ErrorCode::None, accepted, "the {attempt} handshake");
        assert!(connection.scram(&offered, SECRET).await?.is_ok());
    }
    Ok(())
}

#[tokio::test]
async fn scram_sha_512_over_v1_framing() -> Result<()> {
    let _guard = init_tracing()?;

    let mut connection = Connection::new(broker(vec![scram_512()]).await?, 1);
    let offered = connection.handshake("SCRAM-SHA-512").await?;
    assert!(connection.scram(&offered, SECRET).await?.is_ok());
    Ok(())
}

#[tokio::test]
async fn scram_sha_512_with_the_wrong_password_is_refused() -> Result<()> {
    let _guard = init_tracing()?;

    let mut connection = Connection::new(broker(vec![scram_512()]).await?, 1);
    let offered = connection.handshake("SCRAM-SHA-512").await?;
    let ended = connection.scram(&offered, "not_secret").await?;
    assert!(
        matches!(ended, Err(SessionError::MechanismError(_))),
        "the client rejects the server's final message: {ended:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_request_before_authenticating_is_refused() -> Result<()> {
    let _guard = init_tracing()?;

    let mut connection = Connection::new(broker(vec![]).await?, 7);
    connection.correlation_id = 1;
    let mut topic = CreatableTopic::default();
    topic.name = "abc".into();
    topic.num_partitions = 6;
    topic.replication_factor = 1;
    topic.assignments = Some(vec![]);
    topic.configs = Some(vec![]);
    let mut request = CreateTopicsRequest::default();
    request.topics = Some(vec![topic]);
    request.timeout_ms = 30_000;
    request.validate_only = Some(false);

    let refused = connection
        .send(CreateTopicsRequest::KEY, request.into())
        .await;
    let is_refused = matches!(
        refused,
        Err(Error::KafkaProtocol(ProtocolError::NotAuthenticated))
    );
    assert!(is_refused, "{refused:?}");
    Ok(())
}

/// Version 0 of `SaslHandshake` hands the rest of the exchange to raw SASL
/// tokens, each a 4-byte big-endian length and its bytes, with no Kafka
/// framing around them.
#[tokio::test]
async fn scram_sha_256_over_v0_raw_tokens() -> Result<()> {
    let _guard = init_tracing()?;

    let mut connection = Connection::new(broker(vec![scram_256()]).await?, 0);
    connection.correlation_id = 1;
    let offered = connection.handshake("SCRAM-SHA-256").await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(offered.error_code)?);

    let mut session = client(&offered, SECRET);
    let mut input: Option<Bytes> = None;
    let mut token = Vec::new();

    while let State::Running = session
        .step(input.as_deref(), &mut token)
        .expect("a SCRAM step")
    {
        let mut framed = BytesMut::new();
        framed.put_i32(i32::try_from(token.len())?);
        framed.put_slice(&mem::take(&mut token));

        let answer = connection
            .broker
            .serve(Context::default(), framed.freeze())
            .await?;
        input = Some(answer.slice(4..));
    }
    Ok(())
}
