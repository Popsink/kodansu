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

//! Coordinator RPC server (Milestone 2, #18).
//!
//! The server side of the broker→coordinator RPC: a [`Service`] that maps a
//! [`Request`] onto the [`Storage`] reserve/confirm primitives, plus a
//! [`services`] builder that wraps it in the same byte transport the Kafka
//! listener uses (`TcpContext` → length-prefix bytes → postcard body).
//!
//! A reserve/confirm that the backend rejects with an API error code is
//! returned as [`Response::Error`] (an expected, per-request outcome the client
//! acts on); transport/internal failures propagate as [`Error`].

use std::fmt;

use rama::{Context, Layer, Service};
use tansu_service::{
    TcpBytesLayer, TcpContext, TcpContextLayer,
    coordinator::{PostcardFrameLayer, Request, Response},
};
use tansu_storage::{Error as StorageError, Storage, Topition};
use tracing::{debug, instrument};

use crate::Error;

/// Maps coordinator [`Request`]s onto [`Storage`] offset primitives.
#[derive(Clone)]
pub struct CoordinatorRpcService<S> {
    storage: S,
}

impl<S> CoordinatorRpcService<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S> fmt::Debug for CoordinatorRpcService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(CoordinatorRpcService)).finish()
    }
}

impl<S, State> Service<State, Request> for CoordinatorRpcService<S>
where
    S: Storage,
    State: Clone + Send + Sync + 'static,
{
    type Response = Response;
    type Error = Error;

    #[instrument(skip(self, _ctx))]
    async fn serve(
        &self,
        _ctx: Context<State>,
        request: Request,
    ) -> Result<Self::Response, Self::Error> {
        debug!(?request);
        match request {
            Request::Reserve {
                topic,
                partition,
                count,
                deadline_ms,
            } => {
                let tp = Topition::new(topic, partition);
                match self.storage.reserve(&tp, count, deadline_ms).await {
                    Ok(base) => {
                        // resolve the S3 key here (coordinator-side), so the
                        // front writes to an opaque path and never needs the
                        // topic id, cluster id, or key layout.
                        let object_path = self.storage.record_object_path(&tp, base).await?;
                        Ok(Response::Reserved { base, object_path })
                    }
                    Err(StorageError::Api(code)) => Ok(Response::Error { code: code.into() }),
                    Err(error) => Err(error.into()),
                }
            }

            Request::Confirm {
                topic,
                partition,
                base,
                byte_size,
            } => {
                let tp = Topition::new(topic, partition);
                match self.storage.confirm(&tp, base, byte_size).await {
                    Ok(()) => Ok(Response::Confirmed),
                    Err(StorageError::Api(code)) => Ok(Response::Error { code: code.into() }),
                    Err(error) => Err(error.into()),
                }
            }
        }
    }
}

/// Build the coordinator RPC service stack served on the coordinator's RPC
/// listener: `TcpContext` → length-prefix bytes → postcard body → handler.
/// Mirrors the Kafka [`crate::service::services`] composition, swapping the
/// Kafka frame codec for the postcard one.
pub fn services<S>(
    cluster_id: &str,
    storage: S,
) -> impl Service<(), tokio::net::TcpStream, Response = (), Error = Error>
where
    S: Storage + Clone,
{
    (
        TcpContextLayer::new(TcpContext::default().cluster_id(Some(cluster_id.into()))),
        TcpBytesLayer::<()>::default(),
        PostcardFrameLayer::<Request, Response>::default(),
    )
        .into_layer(CoordinatorRpcService::new(storage))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tansu_sans_io::{ErrorCode, create_topics_request::CreatableTopic};
    use tansu_storage::{ArcDynStorage, StorageContainer};
    use url::Url;

    async fn hybrid_storage() -> ArcDynStorage {
        StorageContainer::builder()
            .cluster_id("rpc-test")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://localhost:9092").unwrap())
            .schema_registry(None)
            .storage(Url::parse("hybrid://memory").unwrap())
            .build()
            .await
            .expect("hybrid storage")
    }

    #[tokio::test]
    async fn reserve_then_confirm_through_handler() -> Result<(), Error> {
        let storage = hybrid_storage().await;
        let _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name("rpc".into())
                    .num_partitions(1)
                    .replication_factor(1),
                false,
            )
            .await?;

        let handler = CoordinatorRpcService::new(storage);
        let ctx = Context::default();

        let reserved = handler
            .serve(
                ctx.clone(),
                Request::Reserve {
                    topic: "rpc".into(),
                    partition: 0,
                    count: 5,
                    deadline_ms: 0,
                },
            )
            .await?;
        assert!(matches!(
            reserved,
            Response::Reserved { base: 0, ref object_path }
                if object_path.ends_with("/records/00000000000000000000.batch")
        ));

        let confirmed = handler
            .serve(
                ctx,
                Request::Confirm {
                    topic: "rpc".into(),
                    partition: 0,
                    base: 0,
                    byte_size: 128,
                },
            )
            .await?;
        assert_eq!(confirmed, Response::Confirmed);
        Ok(())
    }

    #[tokio::test]
    async fn unknown_topic_is_an_error_response_not_a_failure() -> Result<(), Error> {
        let storage = hybrid_storage().await;
        let handler = CoordinatorRpcService::new(storage);

        let response = handler
            .serve(
                Context::default(),
                Request::Reserve {
                    topic: "missing".into(),
                    partition: 0,
                    count: 1,
                    deadline_ms: 0,
                },
            )
            .await?;

        assert_eq!(
            response,
            Response::Error {
                code: ErrorCode::UnknownTopicOrPartition.into()
            }
        );
        Ok(())
    }
}
