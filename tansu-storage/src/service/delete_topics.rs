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
    ApiKey, DeleteTopicsRequest, DeleteTopicsResponse, ErrorCode,
    delete_topics_response::DeletableTopicResult,
};
use tracing::instrument;

use tansu_sans_io::acl::{Operation, Resource};

use crate::{Error, Result, Storage, authorized, enforcing};

/// A [`Service`] using [`Storage`] as [`Context`] taking [`DeleteTopicsRequest`] returning [`DeleteTopicsResponse`].
/// ```
/// use rama::{Context, Layer, Service as _, layer::MapStateLayer};
/// use tansu_sans_io::{DeleteTopicsRequest, DeleteTopicsResponse,
///     delete_topics_response::DeletableTopicResult, ErrorCode};
/// use tansu_storage::{DeleteTopicsService, Error, StorageContainer};
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
/// let service = MapStateLayer::new(|_| storage).into_layer(DeleteTopicsService);
///
/// let topic = "pqr";
///
/// let error_code = ErrorCode::UnknownTopicOrPartition;
///
/// assert_eq!(
///     DeleteTopicsResponse::default()
///         .throttle_time_ms(Some(0))
///         .responses(Some(vec![
///             DeletableTopicResult::default()
///                 .error_code(error_code.into())
///                 .error_message(Some(error_code.to_string()))
///                 .name(Some(topic.into())),
///         ])),
///     service
///         .serve(
///             Context::default(),
///             DeleteTopicsRequest::default().topic_names(Some(vec![topic.into()]))
///         )
///         .await?
/// );
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

    #[instrument(skip(ctx, req))]
    async fn serve(
        &self,
        ctx: Context<G>,
        req: DeleteTopicsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let mut responses = vec![];

        for topic in req.topics.unwrap_or_default() {
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
            let error_code = match topic.name.as_deref() {
                Some(name) if !authorized(&ctx, Resource::Topic, name, Operation::Delete).await => {
                    ErrorCode::TopicAuthorizationFailed
                }

                None if enforcing(&ctx) => ErrorCode::TopicAuthorizationFailed,

                _ => ctx.state().delete_topic(&topic.clone().into()).await?,
            };

            responses.push(
                DeletableTopicResult::default()
                    .name(topic.name.clone())
                    .topic_id(Some(topic.topic_id))
                    .error_code(i16::from(error_code))
                    .error_message(Some(error_code.to_string())),
            );
        }

        for topic in req.topic_names.unwrap_or_default() {
            let error_code = if !authorized(&ctx, Resource::Topic, &topic, Operation::Delete).await
            {
                ErrorCode::TopicAuthorizationFailed
            } else {
                ctx.state().delete_topic(&topic.clone().into()).await?
            };

            responses.push(
                DeletableTopicResult::default()
                    .name(Some(topic))
                    .topic_id(None)
                    .error_code(i16::from(error_code))
                    .error_message(Some(error_code.to_string())),
            );
        }

        Ok(DeleteTopicsResponse::default()
            .throttle_time_ms(Some(0))
            .responses(Some(responses)))
    }
}
