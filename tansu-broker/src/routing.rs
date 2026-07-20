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

//! Prefix-owner produce forwarding (#70). The storage layer decides which broker
//! owns a connector prefix (pure `prefix_owner_node`), but the network hop to
//! that peer uses `tansu-client`, which storage cannot depend on (it would cycle
//! via tansu-client → service → auth → storage). So the wire-forward is this
//! [`tansu_storage::PrefixRouter`] implementation, injected into the storage
//! builder by the broker binary.

use std::{collections::HashMap, sync::Mutex, time::Duration};

use async_trait::async_trait;
use tansu_client::{Client, ConnectionManager, Pool};
use tansu_sans_io::{
    ErrorCode, ProduceRequest,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::deflated,
};
use tansu_storage::{Error, PrefixRouter, Result, Topition};
use tokio::time::timeout;
use tracing::error;
use url::Url;

/// Forwards produce to the owning broker over `tansu-client`, pooling one
/// connection pool per owner address.
#[derive(Debug, Default)]
pub struct ClientRouter {
    pools: Mutex<HashMap<String, Pool>>,
}

impl ClientRouter {
    /// Wall-clock deadline for a whole forwarded produce — pool build (which can
    /// otherwise spin in `tansu-client`'s ~30 s connect backoff against a dead
    /// owner) AND the request/response round trip. On elapse the forward returns
    /// a retriable error so the client retries, rather than parking a produce
    /// task (and its pooled connection) indefinitely behind a wedged owner.
    const FORWARD_TIMEOUT: Duration = Duration::from_secs(3);

    pub fn new() -> Self {
        Self::default()
    }

    /// A `tansu-client` pool to `owner`, built lazily and cached by address.
    async fn pool(&self, owner: &Url) -> Result<Pool> {
        let key = owner.to_string();

        if let Some(pool) = self
            .pools
            .lock()
            .map_err(|_| Error::Api(ErrorCode::NotLeaderOrFollower))?
            .get(&key)
            .cloned()
        {
            return Ok(pool);
        }

        let pool = ConnectionManager::builder(owner.clone())
            .client_id(Some("tansu-prefix-forward".into()))
            .build()
            .await
            .map_err(|err| {
                error!(?err, %owner, "building forward pool");
                Error::Api(ErrorCode::NotLeaderOrFollower)
            })?;

        Ok(self
            .pools
            .lock()
            .map_err(|_| Error::Api(ErrorCode::NotLeaderOrFollower))?
            .entry(key)
            .or_insert(pool)
            .clone())
    }
}

#[async_trait]
impl PrefixRouter for ClientRouter {
    async fn forward(
        &self,
        owner: &Url,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        // Bound the whole hop (pool build + round trip) so a wedged owner yields
        // a retriable error instead of hanging produce indefinitely (#70 review).
        match timeout(
            Self::FORWARD_TIMEOUT,
            self.forward_inner(owner, topition, batch),
        )
        .await
        {
            Ok(result) => result,
            Err(_elapsed) => {
                error!(%owner, "forward to prefix owner timed out");
                Err(Error::Api(ErrorCode::NotLeaderOrFollower))
            }
        }
    }
}

impl ClientRouter {
    async fn forward_inner(
        &self,
        owner: &Url,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        let pool = self.pool(owner).await?;

        let request = ProduceRequest::default()
            .transactional_id(None)
            .acks(-1)
            .timeout_ms(Self::FORWARD_TIMEOUT.as_millis() as i32)
            .topic_data(Some(vec![
                TopicProduceData::default()
                    .name(topition.topic().to_owned())
                    .partition_data(Some(vec![
                        PartitionProduceData::default()
                            .index(topition.partition())
                            .records(Some(deflated::Frame {
                                batches: vec![batch],
                            })),
                    ])),
            ]));

        let response = Client::new(pool).call(request).await.map_err(|err| {
            error!(?err, %owner, "forward to prefix owner failed");
            Error::Api(ErrorCode::NotLeaderOrFollower)
        })?;

        let partition = response
            .responses
            .as_deref()
            .unwrap_or_default()
            .first()
            .and_then(|topic| {
                topic
                    .partition_responses
                    .as_deref()
                    .unwrap_or_default()
                    .first()
            })
            .cloned();

        match partition {
            // Kafka error code 0 == no error.
            Some(partition) if partition.error_code == 0 => {
                if partition.base_offset < 0 {
                    return Err(Error::Api(ErrorCode::NotLeaderOrFollower));
                }
                Ok(partition.base_offset)
            }
            // Relay the owner's error. An unparseable code (version skew on a
            // rolling upgrade) maps to a RETRIABLE error, never the fatal
            // UnknownServerError (-1) that would make the client drop the batch.
            Some(partition) => Err(Error::Api(
                ErrorCode::try_from(partition.error_code).unwrap_or(ErrorCode::NotLeaderOrFollower),
            )),
            None => Err(Error::Api(ErrorCode::NotLeaderOrFollower)),
        }
    }
}
