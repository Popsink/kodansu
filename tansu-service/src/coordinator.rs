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

use bytes::Bytes;
use rama::{Context, Layer, Service};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::Error;

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
    /// Offsets `[base, base + count)` are reserved for the caller.
    Reserved { base: i64 },
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
        let request: Req = postcard::from_bytes(&req).map_err(Error::from)?;
        let response = self.inner.serve(ctx, request).await?;
        let encoded = postcard::to_stdvec(&response).map_err(Error::from)?;
        debug!(response_bytes = encoded.len());
        Ok(Bytes::from(encoded))
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
            Request::Reserve { count, .. } => Ok::<_, Error>(Response::Reserved { base: count }),
            Request::Confirm { .. } => Ok(Response::Confirmed),
        });

        let svc = PostcardFrameLayer::<Request, Response>::new().into_layer(inner);

        let wire = Bytes::from(postcard::to_stdvec(&Request::Reserve {
            topic: "t".into(),
            partition: 0,
            count: 7,
            deadline_ms: 0,
        })?);

        let out = svc.serve(Context::default(), wire).await?;
        let decoded: Response = postcard::from_bytes(&out)?;
        assert_eq!(decoded, Response::Reserved { base: 7 });
        Ok(())
    }
}
