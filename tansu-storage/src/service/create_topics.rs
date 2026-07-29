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

use rama::{Context, Service};
use tansu_sans_io::{
    ApiKey, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, NULL_TOPIC_ID,
    create_topics_request::CreatableTopicConfig, create_topics_response::CreatableTopicResult,
};
use tracing::{debug, instrument};

use crate::{Error, Result, Storage};

/// The Apache Kafka default `retention.ms` (7 days).
pub const DEFAULT_RETENTION_MS: i64 = 604_800_000;

/// The Apache Kafka default `cleanup.policy`.
pub const DEFAULT_CLEANUP_POLICY: &str = "delete";

/// Broker-level topic config defaults applied by [`CreateTopicsService`] when a
/// client creates a topic without an explicit value.
///
/// Mirrors Apache Kafka, where `cleanup.policy` defaults to `delete` and
/// `retention.ms` to 7 days, so retention is enforced even when the client sends
/// no topic config.
///
/// Setting [`cleanup_policy`](Self::cleanup_policy) to `None` (or an empty
/// string) opts out of *injecting* a stored policy. It does **not** give the
/// topic infinite retention, which is what this said before #223: the engine
/// reads an absent `cleanup.policy` as Kafka's default, `delete`, and applies the
/// 7-day `retention.ms` fallback, so opting out of the injection produces a topic
/// that expires at 7 days with nothing recorded to explain why.
///
/// Retain-forever has exactly one spelling: `retention.ms=-1`, which both expiry
/// paths map to "never". It can be set per topic with `(Incremental)AlterConfigs`;
/// there is no broker-level default that expresses it (#224).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TopicDefaults {
    /// Default `cleanup.policy`; `None`/empty means "do not inject a stored
    /// policy" — the engine still reads absent as `delete`.
    pub cleanup_policy: Option<String>,

    /// Default `retention.ms`, injected only for `delete`-policy topics. `-1`
    /// means retain forever.
    pub retention_ms: i64,
}

impl Default for TopicDefaults {
    fn default() -> Self {
        Self {
            cleanup_policy: Some(DEFAULT_CLEANUP_POLICY.into()),
            retention_ms: DEFAULT_RETENTION_MS,
        }
    }
}

impl TopicDefaults {
    /// Inject the defaults into `configs` for any key the client omitted.
    ///
    /// `cleanup.policy` is only added when a default is configured; `retention.ms`
    /// is only added when the effective `cleanup.policy` contains `delete` (a
    /// `compact` topic keeps no retention), so internal compacted topics are left
    /// untouched.
    fn apply(&self, configs: &mut Vec<CreatableTopicConfig>) {
        let default_policy = self
            .cleanup_policy
            .as_deref()
            .filter(|policy| !policy.is_empty());

        if let Some(policy) = default_policy
            && !configs.iter().any(|config| config.name == "cleanup.policy")
        {
            configs.push(
                CreatableTopicConfig::default()
                    .name("cleanup.policy".into())
                    .value(Some(policy.to_owned())),
            );
        }

        let policy_is_delete = configs
            .iter()
            .find(|config| config.name == "cleanup.policy")
            .and_then(|config| config.value.as_deref())
            .is_some_and(|policy| policy.contains("delete"));

        if policy_is_delete && !configs.iter().any(|config| config.name == "retention.ms") {
            configs.push(
                CreatableTopicConfig::default()
                    .name("retention.ms".into())
                    .value(Some(self.retention_ms.to_string())),
            );
        }
    }
}

/// A [`Service`] using [`Storage`] as [`Context`] taking [`CreateTopicsRequest`] returning [`CreateTopicsResponse`].
/// ```
/// use rama::{Context, Layer, Service as _, layer::MapStateLayer};
/// use tansu_sans_io::{NULL_TOPIC_ID, CreateTopicsRequest,
///     create_topics_request::CreatableTopic, ErrorCode};
/// use tansu_storage::{CreateTopicsService, Error, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let storage = StorageContainer::builder()
///     .cluster_id("tansu")
///     .node_id(111)
///     .advertised_listener(Url::parse("tcp://localhost:9092")?)
///     .storage(Url::parse("memory://tansu/")?)
///     .build()
///     .await?;
///
/// let service = MapStateLayer::new(|_| storage).into_layer(CreateTopicsService::default());
///
/// let name = "abcba";
///
/// let response = service
///     .serve(
///         Context::default(),
///         CreateTopicsRequest::default()
///             .topics(Some(vec![
///                 CreatableTopic::default()
///                     .name(name.into())
///                     .num_partitions(1)
///                     .replication_factor(3)
///                     .assignments(Some([].into()))
///                     .configs(Some([].into())),
///             ]))
///             .validate_only(Some(false)),
///     )
///     .await?;
///
/// let topics = response.topics.unwrap_or_default();
///
/// assert_eq!(1, topics.len());
/// assert_eq!(name, topics[0].name.as_str());
/// assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
/// assert_eq!(Some(1), topics[0].num_partitions);
/// assert_eq!(Some(3), topics[0].replication_factor);
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CreateTopicsService {
    defaults: TopicDefaults,
}

impl CreateTopicsService {
    /// A service applying the given broker-level [`TopicDefaults`] at create time.
    pub fn new(defaults: TopicDefaults) -> Self {
        Self { defaults }
    }
}

impl ApiKey for CreateTopicsService {
    const KEY: i16 = CreateTopicsRequest::KEY;
}

impl<G> Service<G, CreateTopicsRequest> for CreateTopicsService
where
    G: Storage,
{
    type Response = CreateTopicsResponse;
    type Error = Error;

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: CreateTopicsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let mut topics = vec![];

        for mut topic in req.topics.unwrap_or_default() {
            let name = topic.name.clone();

            let num_partitions = Some(match topic.num_partitions {
                -1 => {
                    topic.num_partitions = 3;
                    topic.num_partitions
                }
                otherwise => otherwise,
            });

            let replication_factor = Some(match topic.replication_factor {
                -1 => {
                    topic.replication_factor = 1;
                    topic.replication_factor
                }
                otherwise => otherwise,
            });

            self.defaults
                .apply(topic.configs.get_or_insert_with(Vec::new));

            match ctx
                .state()
                .create_topic(topic, req.validate_only.unwrap_or_default())
                .await
            {
                Ok(topic_id) => {
                    debug!(?topic_id);

                    topics.push(
                        CreatableTopicResult::default()
                            .name(name)
                            .topic_id(Some(topic_id.into_bytes()))
                            .error_code(ErrorCode::None.into())
                            .error_message(None)
                            .topic_config_error_code(Some(ErrorCode::None.into()))
                            .num_partitions(num_partitions)
                            .replication_factor(replication_factor)
                            .configs(Some([].into())),
                    );
                }

                Err(Error::Api(error_code)) => topics.push(
                    CreatableTopicResult::default()
                        .name(name)
                        .topic_id(Some(NULL_TOPIC_ID))
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .topic_config_error_code(None)
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .configs(Some([].into())),
                ),

                Err(error) => {
                    debug!(?error);

                    topics.push(
                        CreatableTopicResult::default()
                            .name(name)
                            .topic_id(Some(NULL_TOPIC_ID))
                            .error_code(ErrorCode::UnknownServerError.into())
                            .error_message(None)
                            .topic_config_error_code(None)
                            .num_partitions(None)
                            .replication_factor(None)
                            .configs(Some([].into())),
                    )
                }
            }
        }

        Ok(CreateTopicsResponse::default()
            .topics(Some(topics))
            .throttle_time_ms(Some(0)))
    }
}
