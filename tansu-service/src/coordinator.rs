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

//! Broker→coordinator RPC (Milestone 2, #18).
//!
//! A stateless front (the proxy) writes record-batch bytes to object storage
//! itself, and uses this small RPC to ask the single-writer coordinator to
//! assign offsets and record the offset→object metadata — so record bytes never
//! transit the coordinator.
//!
//! The wire transport reuses the broker's generic byte framing
//! ([`crate::TcpBytesService`]: a 4-byte big-endian length prefix); only the
//! *body* changes — a postcard-encoded [`Request`]/[`Response`] instead of a
//! Kafka [`tansu_sans_io::Frame`]. [`PostcardFrameLayer`] is the body codec.

use std::{fmt, marker::PhantomData};

use bytes::{BufMut as _, Bytes, BytesMut};
use rama::{Context, Layer, Service};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::Error;

/// Encode `value` as a length-delimited postcard frame: a 4-byte big-endian
/// body length followed by the postcard body. This is the wire format the
/// broker's [`crate::TcpBytesService`] expects (it reads the 4-byte prefix to
/// size the frame and writes a service response verbatim), so client and server
/// share it. Mirrors the Kafka `fix_length` prefix.
pub fn encode_frame<T: Serialize>(value: &T) -> Result<Bytes, Error> {
    let body = postcard::to_stdvec(value)?;
    let mut framed = BytesMut::with_capacity(4 + body.len());
    framed.put_i32(body.len() as i32);
    framed.extend_from_slice(&body);
    Ok(framed.freeze())
}

/// Decode a value from a length-delimited postcard frame (`[4-byte len][body]`),
/// the inverse of [`encode_frame`]. The 4-byte prefix is skipped; the body is
/// postcard-decoded.
pub fn decode_frame<T: for<'de> Deserialize<'de>>(framed: &[u8]) -> Result<T, Error> {
    let body = framed.get(4..).ok_or(Error::FrameTooBig(framed.len()))?;
    postcard::from_bytes(body).map_err(Into::into)
}

/// A coordinator RPC request. Topic is carried by name (the proxy speaks Kafka
/// names; the coordinator resolves to its topic id).
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum Request {
    /// Assign `count` offsets and record a pending reservation that holds the
    /// visible high watermark until [`Request::Confirm`]; `deadline_ms` is the
    /// wall-clock past which the reservation may be gap-filled.
    Reserve {
        topic: String,
        partition: i32,
        count: i64,
        deadline_ms: i64,
    },
    /// Confirm a reserved batch whose bytes are now durable in object storage.
    Confirm {
        topic: String,
        partition: i32,
        base: i64,
        byte_size: u64,
    },
}

/// A coordinator RPC response.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum Response {
    /// Offsets `[base, base + count)` are reserved for the caller, which must
    /// write the batch bytes to `object_path` (the coordinator owns the key
    /// layout) before [`Request::Confirm`].
    Reserved { base: i64, object_path: String },
    /// The reserved batch is recorded; its offsets are now confirmed.
    Confirmed,
    /// The request failed; carries the Kafka error-code value.
    Error { code: i16 },
}

/// A [`Layer`] adapting a typed request/response [`Service`] to the wire: it
/// decodes an inbound [`Bytes`] body into `Req` (postcard), calls the inner
/// service, and encodes its `Resp` back to [`Bytes`]. Pairs with the broker's
/// length-prefix framing, exactly as [`crate::BytesFrameLayer`] does for Kafka.
pub struct PostcardFrameLayer<Req, Resp> {
    _marker: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> PostcardFrameLayer<Req, Resp> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<Req, Resp> Default for PostcardFrameLayer<Req, Resp> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Req, Resp> Clone for PostcardFrameLayer<Req, Resp> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<S, Req, Resp> Layer<S> for PostcardFrameLayer<Req, Resp> {
    type Service = PostcardFrameService<S, Req, Resp>;

    fn layer(&self, inner: S) -> Self::Service {
        PostcardFrameService {
            inner,
            _marker: PhantomData,
        }
    }
}

/// The [`Service`] produced by [`PostcardFrameLayer`].
pub struct PostcardFrameService<S, Req, Resp> {
    inner: S,
    _marker: PhantomData<fn(Req) -> Resp>,
}

impl<S, Req, Resp> Clone for PostcardFrameService<S, Req, Resp>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, Req, Resp> fmt::Debug for PostcardFrameService<S, Req, Resp> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(PostcardFrameService)).finish()
    }
}

impl<S, State, Req, Resp> Service<State, Bytes> for PostcardFrameService<S, Req, Resp>
where
    S: Service<State, Req, Response = Resp>,
    S::Error: From<Error>,
    State: Clone + Send + Sync + 'static,
    Req: for<'de> Deserialize<'de> + Send + Sync + 'static,
    Resp: Serialize + Send + Sync + 'static,
{
    type Response = Bytes;
    type Error = S::Error;

    #[instrument(skip(self, ctx, req))]
    async fn serve(&self, ctx: Context<State>, req: Bytes) -> Result<Self::Response, Self::Error> {
        // `req` arrives from TcpBytesService as `[4-byte len][body]`; the
        // response must carry the same prefix (TcpBytesService writes it
        // verbatim). encode_frame/decode_frame own that framing.
        let request: Req = decode_frame(&req).map_err(Error::from)?;
        let response = self.inner.serve(ctx, request).await?;
        let framed = encode_frame(&response).map_err(Error::from)?;
        debug!(response_bytes = framed.len());
        Ok(framed)
    }
}

/// deadpool manager for pooled TCP connections to the coordinator's RPC port.
#[derive(Clone, Debug)]
struct ConnectionManager {
    addr: String,
}

impl deadpool::managed::Manager for ConnectionManager {
    type Type = tokio::net::TcpStream;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let stream = tokio::net::TcpStream::connect(&self.addr).await?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    async fn recycle(
        &self,
        _conn: &mut Self::Type,
        _metrics: &deadpool::managed::Metrics,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        Ok(())
    }
}

/// Pooled client for the broker→coordinator RPC.
///
/// reserve/confirm are on the produce hot path (one call per coalesced batch),
/// so connections are pooled rather than dialed per call. A backend API-error
/// rejection ([`Response::Error`]) surfaces as [`Error::Coordinator`]; the proxy
/// maps it to the right Kafka error for the client.
#[derive(Clone)]
pub struct Client {
    pool: deadpool::managed::Pool<ConnectionManager>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Client)).finish()
    }
}

impl Client {
    /// Connect to the coordinator RPC endpoint (`host:port`).
    pub fn new(addr: impl Into<String>) -> Result<Self, Error> {
        let pool = deadpool::managed::Pool::builder(ConnectionManager { addr: addr.into() })
            .build()
            .map_err(|e| Error::Message(e.to_string()))?;
        Ok(Self { pool })
    }

    async fn call(&self, request: Request) -> Result<Response, Error> {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| Error::Message(e.to_string()))?;

        let framed = encode_frame(&request)?;
        conn.write_all(&framed).await?;
        conn.flush().await?;

        let mut len = [0u8; 4];
        conn.read_exact(&mut len).await?;
        let mut body = vec![0u8; i32::from_be_bytes(len) as usize];
        conn.read_exact(&mut body).await?;
        postcard::from_bytes(&body).map_err(Into::into)
    }

    /// Reserve `count` offsets for `topic`/`partition`; returns the assigned base
    /// offset and the object-store path to write the batch bytes to.
    pub async fn reserve(
        &self,
        topic: impl Into<String>,
        partition: i32,
        count: i64,
        deadline_ms: i64,
    ) -> Result<(i64, String), Error> {
        match self
            .call(Request::Reserve {
                topic: topic.into(),
                partition,
                count,
                deadline_ms,
            })
            .await?
        {
            Response::Reserved { base, object_path } => Ok((base, object_path)),
            Response::Error { code } => Err(Error::Coordinator(code)),
            Response::Confirmed => Err(Error::Message("unexpected Confirmed for reserve".into())),
        }
    }

    /// Confirm a reserved batch now durable in object storage.
    pub async fn confirm(
        &self,
        topic: impl Into<String>,
        partition: i32,
        base: i64,
        byte_size: u64,
    ) -> Result<(), Error> {
        match self
            .call(Request::Confirm {
                topic: topic.into(),
                partition,
                base,
                byte_size,
            })
            .await?
        {
            Response::Confirmed => Ok(()),
            Response::Error { code } => Err(Error::Coordinator(code)),
            Response::Reserved { .. } => {
                Err(Error::Message("unexpected Reserved for confirm".into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rama::service::service_fn;

    #[tokio::test]
    async fn round_trips_reserve_through_the_postcard_codec() -> Result<(), Error> {
        // inner service: typed Request -> typed Response
        let inner = service_fn(async |_ctx: Context<()>, req: Request| match req {
            Request::Reserve { count, .. } => Ok::<_, Error>(Response::Reserved {
                base: count,
                object_path: "p".into(),
            }),
            Request::Confirm { .. } => Ok(Response::Confirmed),
        });

        let svc = PostcardFrameLayer::<Request, Response>::new().into_layer(inner);

        // the service consumes/produces length-prefixed frames (what
        // TcpBytesService hands it / writes verbatim).
        let wire = encode_frame(&Request::Reserve {
            topic: "t".into(),
            partition: 0,
            count: 7,
            deadline_ms: 0,
        })?;

        let out = svc.serve(Context::default(), wire).await?;
        let decoded: Response = decode_frame(&out)?;
        assert_eq!(
            decoded,
            Response::Reserved {
                base: 7,
                object_path: "p".into()
            }
        );
        Ok(())
    }

    // End-to-end over a real TCP socket: the full server stack
    // (TcpBytes → PostcardFrame → handler) served on a loopback listener, hit by
    // the pooled Client. Proves the wire framing agrees in both directions.
    #[tokio::test]
    async fn client_server_round_trip_over_tcp() -> Result<(), Error> {
        use crate::{TcpBytesLayer, TcpContext, TcpContextLayer};
        use rama::Context;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        // TcpBytesService hands its inner service `Context<()>` (it swaps to its
        // own State::default()), so the handler is Service<(), Request>.
        let handler = service_fn(async |_ctx: Context<()>, req: Request| match req {
            Request::Reserve { count, .. } => Ok::<_, Error>(Response::Reserved {
                base: count - 1,
                object_path: "p".into(),
            }),
            Request::Confirm { .. } => Ok(Response::Confirmed),
        });
        let server = (
            TcpContextLayer::new(TcpContext::default()),
            TcpBytesLayer::<()>::default(),
            PostcardFrameLayer::<Request, Response>::default(),
        )
            .into_layer(handler);

        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _ = server.serve(Context::default(), stream).await;
            }
        });

        let client = Client::new(addr.to_string())?;
        let (base, object_path) = client.reserve("t", 3, 10, 0).await?;
        assert_eq!((base, object_path.as_str()), (9, "p"));
        assert!(matches!(client.confirm("t", 3, 9, 64).await, Ok(())));
        Ok(())
    }
}
