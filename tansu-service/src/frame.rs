// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use bytes::{BufMut as _, Bytes, BytesMut};
use indicatif::ProgressBar;
use opentelemetry::KeyValue;
use rama::context::Extensions;
use rama::matcher::Matcher;
use rama::service::BoxService;
use rama::{Context, Layer, Service};
use rsasl::config::SASLConfig;
use tansu_auth::Authentication;
use tansu_sans_io::{ApiKey, ApiVersionsRequest, Body, Frame, Header, Request};
use tansu_sans_io::{RequestApiVersion, Response, RootMessageMeta, SaslAuthenticateRequest};
use tansu_sans_io::{SaslAuthenticateResponse, SaslHandshakeRequest};
use tokio::task::spawn_blocking;
use tracing::{debug, error};

use crate::{API_ERRORS, API_REQUESTS};

/// Matches a [`Request`] whose [API key][`ApiKey`] is this one.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestApiKeyMatcher(pub i16);

impl<State, Q> Matcher<State, Q> for RequestApiKeyMatcher
where
    Q: Request,
    State: Clone + Debug,
{
    fn matches(&self, ext: Option<&mut Extensions>, ctx: &Context<State>, req: &Q) -> bool {
        debug!(ext = ?ext, ctx = ?ctx, req = ?req);
        let RequestApiKeyMatcher(wanted) = *self;
        wanted == Q::KEY
    }
}

/// Matches a [`Frame`] whose [API key][`Frame#method.api_key`] is this one.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameApiKeyMatcher(pub i16);

impl<State> Matcher<State, Frame> for FrameApiKeyMatcher
where
    State: Clone + Debug,
{
    fn matches(&self, ext: Option<&mut Extensions>, ctx: &Context<State>, req: &Frame) -> bool {
        let (_, _) = (ext, ctx);
        let FrameApiKeyMatcher(wanted) = *self;
        matches!(req.api_key(), Ok(key) if key == wanted)
    }
}

/// The [`Layer`] of `RequestService`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> RequestLayer<Q> {
    pub fn new() -> Self {
        let request = PhantomData;
        RequestLayer { request }
    }
}

impl<S, Q> Layer<S> for RequestLayer<Q> {
    type Service = RequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        let request = PhantomData;
        RequestService { inner, request }
    }
}

/// Serves a typed [`Request`] with a typed [`Response`], logging both.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q> Debug for RequestService<S, Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RequestService")
    }
}

impl<State, S, Q> Service<State, Q> for RequestService<S, Q>
where
    S: Service<State, Q>,
    Q: Request,
    S::Error: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<State, Q>>::Error>,
    S::Response: Response,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    #[tracing::instrument(skip(ctx, req))]
    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        debug!(req = ?req);
        let answered = self.inner.serve(ctx, req).await;
        if let Ok(response) = &answered {
            debug!(response = ?response);
        }
        answered
    }
}

impl<S, Q> ApiKey for RequestService<S, Q>
where
    Q: Request,
{
    const KEY: i16 = Q::KEY;
}

/// The [`Layer`] of `FrameRequestService`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameRequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> FrameRequestLayer<Q> {
    pub fn new() -> Self {
        let request = PhantomData;
        FrameRequestLayer { request }
    }
}

impl<S, Q> Layer<S> for FrameRequestLayer<Q> {
    type Service = FrameRequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        let request = PhantomData;
        FrameRequestService { inner, request }
    }
}

/// Serves a [`Frame`] by decoding its body as a [`Request`], answering in a
/// response frame with the same correlation id.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameRequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q> Debug for FrameRequestService<S, Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FrameRequestService")
    }
}

impl<S, Q, State> Service<State, Frame> for FrameRequestService<S, Q>
where
    S: Service<State, Q>,
    S::Response: Response,
    S::Error: From<tansu_sans_io::Error>,
    Q: Request + TryFrom<Body>,
    <Q as TryFrom<Body>>::Error: Into<S::Error>,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = S::Error;

    #[tracing::instrument(skip(ctx, req))]
    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;
        let Frame { body, .. } = req;
        let request = Q::try_from(body).map_err(Into::into)?;

        let response = self.inner.serve(ctx, request).await?;
        let header = Header::Response { correlation_id };
        Ok(Frame {
            size: 0,
            header,
            body: response.into(),
        })
    }
}

impl<S, Q, State> Matcher<State, Frame> for FrameRequestService<S, Q>
where
    S: Clone + Send + Sync + 'static,
    Q: Request,
    State: Clone + Debug,
{
    fn matches(&self, ext: Option<&mut Extensions>, ctx: &Context<State>, req: &Frame) -> bool {
        debug!(ext = ?ext, ctx = ?ctx, req = ?req);
        matches!(req.api_key(), Ok(key) if key == Q::KEY)
    }
}

/// The [`Layer`] of [`BytesFrameService`], optionally authenticating with SASL.
#[derive(Clone, Debug, Default)]
pub struct BytesFrameLayer {
    sasl_config: Option<Arc<SASLConfig>>,
}

impl BytesFrameLayer {
    pub fn with_sasl_config(self, sasl_config: Option<Arc<SASLConfig>>) -> Self {
        let mut configured = self;
        configured.sasl_config = sasl_config;
        configured
    }
}

impl<S> Layer<S> for BytesFrameLayer {
    type Service = BytesFrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        let af = self.sasl_config.clone().map(|config| {
            let authentication = Authentication::server(config);
            AuthenticationFrame {
                authentication,
                v0: Arc::default(),
            }
        });
        BytesFrameService { inner, af }
    }
}

#[derive(Clone)]
struct AuthenticationFrame {
    authentication: Authentication,
    v0: Arc<Mutex<Option<bool>>>,
}

impl AuthenticationFrame {
    fn is_authenticated(&self) -> bool {
        let AuthenticationFrame { authentication, .. } = self;
        authentication.is_authenticated()
    }
}

/// Serves an encoded request frame with an encoded response frame, enforcing
/// SASL authentication when the layer was given a configuration.
#[derive(Clone, Default)]
pub struct BytesFrameService<S> {
    inner: S,
    af: Option<AuthenticationFrame>,
}

impl<S> Debug for BytesFrameService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BytesFrameService")
    }
}

impl<S> BytesFrameService<S> {
    fn is_authenticated(&self, api_key: i16) -> bool {
        let Some(af) = &self.af else {
            return true;
        };
        let unauthenticated = [
            SaslHandshakeRequest::KEY,
            SaslAuthenticateRequest::KEY,
            ApiVersionsRequest::KEY,
        ];
        af.authentication.is_authenticated() || unauthenticated.contains(&api_key)
    }

    /// Whether a v0 SASL handshake is under way.
    ///
    /// After a v0 `SaslHandshakeRequest` the client and the broker exchange the
    /// mechanism's tokens as opaque, length-prefixed packets with no Kafka
    /// header around them. From v1 on, the tokens travel inside
    /// `SaslAuthenticate` requests and responses instead, and the final
    /// response's error code says whether authentication succeeded.
    fn handshake_v0(&self) -> bool {
        let Some(af) = &self.af else {
            return false;
        };
        let Ok(v0) = af.v0.lock() else {
            return false;
        };
        debug!(v0 = ?v0);
        v0.unwrap_or_default()
    }

    /// Mark the packets after a v0 `SaslHandshakeRequest` as opaque tokens.
    fn begin_handshake_v0(&self, api_key: i16, api_version: i16) {
        let Some(af) = &self.af else {
            return;
        };
        if api_key != SaslHandshakeRequest::KEY || api_version != 0 {
            return;
        }
        if let Ok(mut v0) = af.v0.lock() {
            *v0 = Some(true);
        }
    }

    /// Once authenticated, the opaque token just answered was the last one of a
    /// v0 handshake: the packets after it are framed again.
    fn end_handshake_v0(&self) {
        let Some(af) = &self.af else {
            return;
        };
        if !af.is_authenticated() {
            return;
        }
        if let Ok(mut v0) = af.v0.lock() {
            _ = v0.take();
        }
    }
}

impl<S, State> Service<State, Bytes> for BytesFrameService<S>
where
    S: Service<State, Frame, Response = Frame>,
    State: Clone + Send + Sync + 'static,
    S::Error: From<tansu_sans_io::Error> + From<tokio::task::JoinError> + Debug,
{
    type Response = Bytes;
    type Error = S::Error;

    #[tracing::instrument(skip(ctx, req))]
    async fn serve(
        &self,
        mut ctx: Context<State>,
        req: Bytes,
    ) -> Result<Self::Response, Self::Error> {
        let sasl_handshake_v0 = self.handshake_v0();
        debug!(request = ?req.as_ref(), sasl_handshake_v0 = sasl_handshake_v0);

        let req = if sasl_handshake_v0 {
            opaque_token(&req)
        } else {
            let decoded = spawn_blocking(|| Frame::request_from_bytes(req)).await??;
            debug!(request = ?decoded);
            decoded
        };

        let api_key = req.api_key()?;
        let authorised = self.is_authenticated(api_key);
        if !authorised {
            return Err(tansu_sans_io::Error::NotAuthenticated.into());
        }
        let api_version = req.api_version()?;
        let correlation_id = req.correlation_id()?;

        if let Some(progress) = ctx.get::<ProgressBar>() {
            let name = req.api_name();
            progress.set_message(format!("{name} v{api_version}/{correlation_id}"));
            progress.tick();
        }

        let attributes = [
            KeyValue::new("api_key", i64::from(api_key)),
            KeyValue::new("api_version", i64::from(api_version)),
        ];

        if let Some(af) = &self.af {
            let previous = ctx.insert(af.authentication.clone());
            assert!(previous.is_none());
        }
        _ = ctx.insert(RequestApiVersion(api_version));
        let answer = self.inner.serve(ctx, req).await?;
        debug!(response = ?answer);
        let Frame { body, .. } = answer;

        if sasl_handshake_v0 {
            self.end_handshake_v0();
            let token = SaslAuthenticateResponse::try_from(body);
            return token
                .and_then(|response| length_prefixed(response.auth_bytes))
                .map_err(Into::into);
        }

        self.begin_handshake_v0(api_key, api_version);
        let header = Header::Response { correlation_id };
        let encoded =
            spawn_blocking(move || Frame::response(header, body, api_key, api_version)).await?;
        match &encoded {
            Ok(response) => {
                debug!(response = ?response.as_ref());
                API_REQUESTS.add(1, &attributes);
            }
            Err(err) => {
                error!(api_key = api_key, api_version = api_version, err = ?err);
                API_ERRORS.add(1, &attributes);
            }
        }
        encoded.map_err(Into::into)
    }
}

/// The opaque v0 SASL token in `packet`, after its 4-byte length, as the
/// `SaslAuthenticate` v0 request the services downstream understand.
fn opaque_token(packet: &Bytes) -> Frame {
    let header = Header::Request {
        api_key: SaslAuthenticateRequest::KEY,
        api_version: 0,
        correlation_id: 0,
        client_id: None,
    };
    let request = SaslAuthenticateRequest::default().auth_bytes(packet.slice(4..));
    Frame {
        size: 0,
        header,
        body: Body::SaslAuthenticateRequest(request),
    }
}

/// `token` behind its length as a big-endian `i32`: a v0 SASL packet.
fn length_prefixed(token: Bytes) -> Result<Bytes, tansu_sans_io::Error> {
    let length = i32::try_from(token.len())?;
    let mut packet = BytesMut::new();
    packet.put_slice(&length.to_be_bytes());
    packet.put(token);
    Ok(packet.freeze())
}

/// The [`Layer`] of [`FrameBytesService`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBytesLayer;

impl<S> Layer<S> for FrameBytesLayer {
    type Service = FrameBytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FrameBytesService { inner }
    }
}

/// Serves a [`Frame`] by encoding it, and decodes the encoded answer the inner
/// service gives back.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBytesService<S> {
    inner: S,
}

impl<S> Debug for FrameBytesService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FrameBytesService")
    }
}

impl<S, State> Service<State, Frame> for FrameBytesService<S>
where
    S: Service<State, Bytes, Response = Bytes>,
    S::Error: From<tansu_sans_io::Error>,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = S::Error;

    #[tracing::instrument(skip(ctx, req), fields(api_key = req.api_key()?, api_version = req.api_version()?, correlation_id = req.correlation_id()?))]
    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        debug!(req = ?req);
        let (api_key, api_version) = (req.api_key()?, req.api_version()?);
        let Frame { header, body, .. } = req;
        let encoded = Frame::request(header, body)?;

        let answer = self.inner.serve(ctx, encoded).await?;
        let response = Frame::response_from_bytes(answer, api_key, api_version)?;
        debug!(response = ?response);
        Ok(response)
    }
}

/// The [`Layer`] of `FrameBodyService`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBodyLayer;

impl<S> Layer<S> for FrameBodyLayer {
    type Service = FrameBodyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FrameBodyService { inner }
    }
}

/// Serves a [`Frame`] with the [`Body`] the inner service answers its body
/// with, under the same correlation id.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBodyService<S> {
    inner: S,
}

impl<S, State> Service<State, Frame> for FrameBodyService<S>
where
    S: Service<State, Body, Response = Body>,
    S::Error: From<tansu_sans_io::Error>,
    State: Send + Sync + 'static,
{
    type Response = Frame;

    type Error = S::Error;

    #[tracing::instrument(skip_all, fields(api_key = req.api_key()?, api_version = req.api_version()?, correlation_id = req.correlation_id()?))]
    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let correlation_id = req.correlation_id()?;
        let Frame { body: request, .. } = req;
        let body = self.inner.serve(ctx, request).await?;
        let header = Header::Response { correlation_id };
        Ok(Frame {
            size: 0,
            header,
            body,
        })
    }
}

/// The [`Layer`] of `BodyRequestService`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyRequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> BodyRequestLayer<Q> {
    pub fn new() -> Self {
        let request = PhantomData;
        BodyRequestLayer { request }
    }
}

impl<S, Q> Layer<S> for BodyRequestLayer<Q> {
    type Service = BodyRequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        let request = PhantomData;
        BodyRequestService { inner, request }
    }
}

/// Serves a [`Body`] by decoding it as a [`Request`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyRequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q> ApiKey for BodyRequestService<S, Q>
where
    Q: Request,
{
    const KEY: i16 = Q::KEY;
}

impl<S, State, Q> Service<State, Body> for BodyRequestService<S, Q>
where
    S: Service<State, Q>,
    Q: Request,
    S::Error: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<State, Q>>::Error>,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    type Response = Body;
    type Error = S::Error;

    #[tracing::instrument(skip_all)]
    async fn serve(&self, ctx: Context<State>, req: Body) -> Result<Self::Response, Self::Error> {
        let request = Q::try_from(req)?;
        let response = self.inner.serve(ctx, request).await?;
        Ok(response.into())
    }
}

/// The [`Layer`] of [`RequestFrameService`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestFrameLayer;

impl<S> Layer<S> for RequestFrameLayer {
    type Service = RequestFrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestFrameService { inner }
    }
}

/// Serves a [`Request`] by sending it in a [`Frame`] at the newest version this
/// crate knows, and decoding the [`Response`] from the answer.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestFrameService<S> {
    inner: S,
}

impl<S, State, Q> Service<State, Q> for RequestFrameService<S>
where
    Q: Request,
    S: Service<State, Frame, Response = Frame>,
    S::Error: From<<<Q as Request>::Response as TryFrom<Body>>::Error>,
    State: Send + Sync + 'static,
{
    type Response = Q::Response;
    type Error = S::Error;

    #[tracing::instrument(skip_all)]
    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        debug!(req = ?req);

        let requests = RootMessageMeta::messages().requests();
        let newest = requests
            .get(&Q::KEY)
            .map_or(0, |meta| meta.version.valid().end);
        let header = Header::Request {
            api_key: Q::KEY,
            api_version: newest,
            correlation_id: 0,
            client_id: Some(env!("CARGO_CRATE_NAME").to_owned()),
        };
        let request = Frame {
            size: 0,
            header,
            body: req.into(),
        };

        let answer = self.inner.serve(ctx, request).await?;
        let response = Q::Response::try_from(answer.body)?;
        debug!(response = ?response);
        Ok(response)
    }
}

impl<S, State, Q, E> From<RequestService<S, Q>> for BoxService<State, Body, Body, E>
where
    S: Service<State, Q, Error = E>,
    Q: Request,
    <S as Service<State, Q>>::Response: Response,
    E: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<State, Q>>::Error>,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    fn from(value: RequestService<S, Q>) -> Self {
        let layer = BodyRequestLayer::<Q>::new();
        layer.into_layer(value).boxed()
    }
}

impl<S, State, Q, E> From<RequestService<S, Q>> for BoxService<State, Frame, Frame, E>
where
    S: Service<State, Q, Error = E>,
    Q: Request,
    <S as Service<State, Q>>::Response: Response,
    E: From<tansu_sans_io::Error>
        + From<<Q as TryFrom<Body>>::Error>
        + From<<S as Service<State, Q>>::Error>,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    fn from(value: RequestService<S, Q>) -> Self {
        let layers = (FrameBodyLayer, BodyRequestLayer::<Q>::new());
        layers.into_layer(value).boxed()
    }
}

impl<S, State, Q, E> From<BodyRequestService<S, Q>> for BoxService<State, Frame, Frame, E>
where
    S: Service<State, Q, Error = E>,
    Q: Request,
    E: From<tansu_sans_io::Error>
        + From<<Q as TryFrom<Body>>::Error>
        + From<<S as Service<State, Q>>::Error>,
    Body: From<<S as Service<State, Q>>::Response>,
    State: Send + Sync + 'static,
{
    fn from(value: BodyRequestService<S, Q>) -> Self {
        let service = FrameBodyLayer.into_layer(value);
        service.boxed()
    }
}

/// Serves a [`Frame`] with whatever the closure answers it with.
#[derive(Clone, Copy, Debug, Hash)]
pub struct FrameService<F> {
    response: F,
}

impl<State, E, F> Service<State, Frame> for FrameService<F>
where
    F: Fn(Context<State>, Frame) -> Result<Frame, E> + Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    type Response = Frame;
    type Error = E;

    #[tracing::instrument(skip_all)]
    async fn serve(&self, ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let respond = &self.response;
        respond(ctx, req)
    }
}

impl<F> FrameService<F> {
    pub fn new<State, E>(response: F) -> Self
    where
        F: Fn(Context<State>, Frame) -> Result<Frame, E> + Clone,
        E: Send + Sync + 'static,
    {
        FrameService { response }
    }
}

/// Serves a [`Request`] with whatever [`Response`] the closure answers it with.
#[derive(Clone, Copy, Hash)]
pub struct ResponseService<F> {
    response: F,
}

impl<F> Debug for ResponseService<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ResponseService")
    }
}

impl<State, Q, E, F> Service<State, Q> for ResponseService<F>
where
    F: Fn(Context<State>, Q) -> Result<Q::Response, E> + Clone + Send + Sync + 'static,
    Q: Request,
    E: Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    type Response = Q::Response;
    type Error = E;

    #[tracing::instrument(skip(ctx, req))]
    async fn serve(&self, ctx: Context<State>, req: Q) -> Result<Self::Response, Self::Error> {
        let respond = &self.response;
        respond(ctx, req)
    }
}

impl<F> ResponseService<F> {
    pub fn new<State, Q, E>(response: F) -> Self
    where
        F: Fn(Context<State>, Q) -> Result<Q::Response, E> + Clone,
        Q: Request,
        E: Send + Sync + 'static,
    {
        ResponseService { response }
    }
}
