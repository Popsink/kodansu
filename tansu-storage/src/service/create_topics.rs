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

use rama::Context;
use rama::Service;
use tansu_sans_io::NULL_TOPIC_ID;
use tansu_sans_io::acl::Operation;
use tansu_sans_io::create_topics_response::CreatableTopicResult;
use tansu_sans_io::{ApiKey, ErrorCode};
use tansu_sans_io::{CreateTopicsRequest, CreateTopicsResponse};

use crate::Storage;
use crate::authorized_cluster;
use crate::{Error, Result};

/// What `NumPartitions` and `ReplicationFactor` carry when there is no topic to
/// report them for — the defaults `CreateTopicsResponse.json` gives the two
/// fields, which are `int32`/`int16` and **not** nullable from v5 onwards.
const UNKNOWN_PARTITIONS: i32 = -1;
const UNKNOWN_REPLICATION_FACTOR: i16 = -1;

/// The part of a topic's result every outcome shares: its name, its id, and the code.
fn result_for(name: String, id: [u8; 16], code: ErrorCode) -> CreatableTopicResult {
    let row = CreatableTopicResult::default()
        .name(name)
        .topic_id(Some(id));
    row.error_code(i16::from(code))
}

/// Creates the topics a request names, given `CREATE` on the cluster, with a
/// partition count of 3 and a replication factor of 1 where the request asks
/// for the broker's default (`-1`).
///
/// Broker-level [`crate::TopicDefaults`] are applied by the engine, inside
/// [`crate::MetadataStorage::create_topic`], not here: this is one of two paths that create a
/// topic (the other is auto-create, in [`super::MetadataService`]), and a topic's
/// effective config must not depend on which one it came through (#225).
///
/// ```
/// use rama::layer::MapStateLayer;
/// use rama::{Context, Layer as _, Service as _};
/// use tansu_sans_io::create_topics_request::CreatableTopic;
/// use tansu_storage::{CreateTopicsService, StorageContainer};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), tansu_storage::Error> {
/// let listener = url::Url::parse("tcp://localhost:9092")?;
/// let engine = StorageContainer::builder().cluster_id("tansu").node_id(111);
/// let engine = engine.advertised_listener(listener);
/// let engine = engine.storage(url::Url::parse("memory://tansu/")?).build().await?;
/// let creating = MapStateLayer::new(|_| engine).into_layer(CreateTopicsService);
///
/// let wanted = CreatableTopic::default().name(String::from("abcba"));
/// let wanted = wanted.num_partitions(1).replication_factor(3);
/// let wanted = wanted.assignments(Some(vec![])).configs(Some(vec![]));
/// let asked = tansu_sans_io::CreateTopicsRequest::default().topics(Some(vec![wanted]));
/// let answered = creating.serve(Context::default(), asked.validate_only(Some(false))).await?;
///
/// let [created] = &answered.topics.unwrap_or_default()[..] else {
///     panic!("one topic asked, one result expected")
/// };
/// assert_eq!("abcba", created.name);
/// assert_ne!(Some(tansu_sans_io::NULL_TOPIC_ID), created.topic_id);
/// let shape = (created.num_partitions, created.replication_factor);
/// assert_eq!((Some(1), Some(3)), shape);
/// assert_eq!(0, created.error_code, "NONE");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CreateTopicsService;

impl ApiKey for CreateTopicsService {
    const KEY: i16 = CreateTopicsRequest::KEY;
}

impl<G> Service<G, CreateTopicsRequest> for CreateTopicsService
where
    G: Storage,
{
    type Response = CreateTopicsResponse;
    type Error = Error;

    #[tracing::instrument(skip(state, request))]
    async fn serve(
        &self,
        state: Context<G>,
        request: CreateTopicsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let validate_only = request.validate_only == Some(true);
        let mut outcomes = Vec::new();

        // `CREATE` on the cluster, as Kafka requires: the topic does not exist
        // yet, so there is nothing for a topic rule to select. A tenant scoped
        // to `tenant-a.` therefore cannot create topics at all — creating them
        // is an operator's job on a mutualised fleet, and a rule on a name
        // nobody has taken yet would be a rule on the whole namespace (#363).
        let allowed = authorized_cluster(&state, Operation::Create).await;

        for mut wanted in request.topics.into_iter().flatten() {
            let name = wanted.name.clone();

            if !allowed {
                let code = ErrorCode::ClusterAuthorizationFailed;
                let refusal = result_for(name, NULL_TOPIC_ID, code).error_message(None);
                // `-1`, the schema's own default, and not `None`:
                // `NumPartitions` and `ReplicationFactor` are not
                // nullable from v5, so a null here does not encode. The
                // client cannot parse the response it gets, so a
                // refusal that should have been immediate and final
                // arrives as a request timeout instead — which reads as
                // a broker that has stopped answering rather than one
                // that said no (#363).
                let refusal = refusal.num_partitions(Some(UNKNOWN_PARTITIONS));
                let refusal = refusal.replication_factor(Some(UNKNOWN_REPLICATION_FACTOR));
                outcomes.push(refusal.topic_config_error_code(None).configs(None));
                continue;
            }

            if wanted.num_partitions == -1 {
                wanted.num_partitions = 3;
            }
            if wanted.replication_factor == -1 {
                wanted.replication_factor = 1;
            }
            let shape = (Some(wanted.num_partitions), Some(wanted.replication_factor));

            let creating = state.state().create_topic(wanted, validate_only);
            let outcome = match creating.await {
                Ok(id) => {
                    tracing::debug!(topic_id = ?id);
                    let row = result_for(name, id.into_bytes(), ErrorCode::None);
                    let accepted = Some(i16::from(ErrorCode::None));
                    let row = row.error_message(None).topic_config_error_code(accepted);
                    row.num_partitions(shape.0).replication_factor(shape.1)
                }

                Err(Error::Api(code)) => {
                    let row = result_for(name, NULL_TOPIC_ID, code);
                    let row = row.error_message(Some(code.to_string()));
                    let row = row.topic_config_error_code(None);
                    row.num_partitions(shape.0).replication_factor(shape.1)
                }

                Err(error) => {
                    tracing::debug!(?error);
                    let row = result_for(name, NULL_TOPIC_ID, ErrorCode::UnknownServerError);
                    let row = row.error_message(None).topic_config_error_code(None);
                    // As above: not nullable, so the failure has to be
                    // reported with the schema's default rather than
                    // with nothing.
                    let row = row.num_partitions(Some(UNKNOWN_PARTITIONS));
                    row.replication_factor(Some(UNKNOWN_REPLICATION_FACTOR))
                }
            };
            outcomes.push(outcome.configs(Some(Vec::new())));
        }

        let answer = CreateTopicsResponse::default().topics(Some(outcomes));
        Ok(answer.throttle_time_ms(Some(0)))
    }
}
