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
use tansu_sans_io::acl::{Operation, Resource};
use tansu_sans_io::delete_topics_response::DeletableTopicResult;
use tansu_sans_io::{ApiKey, DeleteTopicsRequest, DeleteTopicsResponse, ErrorCode};

use crate::{Error, Result};
use crate::{Storage, authorized, enforcing};

/// Deletes each topic a `DeleteTopics` request names, by id or by name,
/// answering one result per topic: refused where the principal may not
/// delete it, otherwise whatever [`Storage::delete_topic`] answered.
/// ```
/// use rama::layer::MapStateLayer;
/// use rama::{Context, Layer as _, Service as _};
/// use tansu_sans_io::ErrorCode;
/// use tansu_sans_io::delete_topics_response::DeletableTopicResult;
/// use tansu_sans_io::{DeleteTopicsRequest, DeleteTopicsResponse};
/// use tansu_storage::{DeleteTopicsService, StorageContainer};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), tansu_storage::Error> {
/// let listener = url::Url::parse("tcp://localhost:9092")?;
/// let engine = StorageContainer::builder().cluster_id("tansu").node_id(111);
/// let engine = engine.advertised_listener(listener);
/// let engine = engine.storage(url::Url::parse("memory://tansu/")?).build().await?;
/// let delete = MapStateLayer::new(|_| engine).into_layer(DeleteTopicsService);
///
/// let request = DeleteTopicsRequest::default().topic_names(Some(vec!["pqr".into()]));
/// let answered = delete.serve(Context::default(), request).await?;
///
/// let unknown = ErrorCode::UnknownTopicOrPartition;
/// let result = DeletableTopicResult::default().name(Some("pqr".into()));
/// let result = result.error_code(unknown.into()).error_message(Some(unknown.to_string()));
/// let expected = DeleteTopicsResponse::default().throttle_time_ms(Some(0));
/// assert_eq!(expected.responses(Some(vec![result])), answered);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeleteTopicsService;

impl ApiKey for DeleteTopicsService {
    const KEY: i16 = DeleteTopicsRequest::KEY;
}

impl<G> Service<G, DeleteTopicsRequest> for DeleteTopicsService
where
    G: Storage,
{
    type Response = DeleteTopicsResponse;
    type Error = Error;

    #[tracing::instrument(skip(state, request))]
    async fn serve(
        &self,
        state: Context<G>,
        request: DeleteTopicsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let answered = |name, topic_id, code: ErrorCode| {
            let result = DeletableTopicResult::default()
                .name(name)
                .topic_id(topic_id);
            result
                .error_code(i16::from(code))
                .error_message(Some(code.to_string()))
        };
        let refused = ErrorCode::TopicAuthorizationFailed;
        let mut results = vec![];

        for topic in request.topics.unwrap_or_default() {
            // Per topic, because that is where the response carries a code —
            // and a delete refused for one topic must not fail the rest of the
            // request (#363).
            //
            // A topic named by id alone cannot be authorized: an ACL is written
            // on a name, and resolving the id to one would itself be a read
            // this principal may not be allowed. Refusing is the answer that
            // cannot leak — but only where authorization is on at all, or a
            // broker without `--authentication` would stop serving a request
            // it has always served.
            let outcome = match topic.name.as_deref() {
                Some(name)
                    if !authorized(&state, Resource::Topic, name, Operation::Delete).await =>
                {
                    refused
                }
                None if enforcing(&state) => refused,
                _ => {
                    let target = crate::TopicId::from(topic.clone());
                    state.state().delete_topic(&target).await?
                }
            };
            results.push(answered(topic.name.clone(), Some(topic.topic_id), outcome));
        }

        for name in request.topic_names.unwrap_or_default() {
            let allowed = authorized(&state, Resource::Topic, &name, Operation::Delete).await;
            let outcome = if allowed {
                let target = crate::TopicId::from(name.clone());
                state.state().delete_topic(&target).await?
            } else {
                refused
            };
            results.push(answered(Some(name), None, outcome));
        }

        let unthrottled = DeleteTopicsResponse::default().throttle_time_ms(Some(0));
        Ok(unthrottled.responses(Some(results)))
    }
}
