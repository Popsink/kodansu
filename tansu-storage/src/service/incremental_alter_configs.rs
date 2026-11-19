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
use tansu_sans_io::ApiKey;
use tansu_sans_io::{IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse};

use crate::Storage;
use crate::{Error, Result};

/// Hands each resource of the request to [`Storage::incremental_alter_resource`],
/// answering with what the engine reports for each, in request order.
/// ```
/// use rama::layer::MapStateLayer;
/// use rama::{Context, Layer as _, Service as _};
/// use tansu_sans_io::create_topics_request::CreatableTopic;
/// use tansu_sans_io::describe_configs_request::DescribeConfigsResource;
/// use tansu_sans_io::incremental_alter_configs_request::AlterConfigsResource;
/// use tansu_sans_io::incremental_alter_configs_request::AlterableConfig;
/// use tansu_sans_io::{ConfigResource, DescribeConfigsRequest, OpType};
/// use tansu_storage::{CreateTopicsService, DescribeConfigsService};
/// use tansu_storage::{IncrementalAlterConfigsService, StorageContainer};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), tansu_storage::Error> {
/// let listener = url::Url::parse("tcp://localhost:9092")?;
/// let engine = StorageContainer::builder().cluster_id("tansu").node_id(111);
/// let engine = engine.advertised_listener(listener);
/// let engine = engine.storage(url::Url::parse("memory://tansu/")?).build().await?;
/// let (for_create, for_describe) = (engine.clone(), engine.clone());
/// let creating = MapStateLayer::new(move |_| for_create).into_layer(CreateTopicsService);
/// let describing = MapStateLayer::new(move |_| for_describe).into_layer(DescribeConfigsService);
/// let altering = MapStateLayer::new(move |_| engine).into_layer(IncrementalAlterConfigsService);
///
/// let topic = CreatableTopic::default().name(String::from("abcba"));
/// let topic = topic.num_partitions(3).replication_factor(1);
/// let create = tansu_sans_io::CreateTopicsRequest::default().topics(Some(vec![topic]));
/// let created = creating.serve(Context::default(), create).await?;
/// assert_eq!(0, created.topics.unwrap_or_default()[0].error_code, "NONE");
///
/// let asking = || {
///     let key = DescribeConfigsResource::default().resource_name(String::from("abcba"));
///     let key = key.resource_type(i8::from(ConfigResource::Topic));
///     let key = key.configuration_keys(Some(vec![String::from("x.y.z")]));
///     let ask = DescribeConfigsRequest::default().include_documentation(Some(false));
///     ask.include_synonyms(Some(false)).resources(Some(vec![key]))
/// };
/// let value_of_xyz = |described: tansu_sans_io::DescribeConfigsResponse| {
///     let [resource] = &described.results.unwrap_or_default()[..] else {
///         panic!("one resource asked, one described")
///     };
///     assert_eq!("abcba", resource.resource_name);
///     let configs = resource.configs.as_deref().unwrap_or_default();
///     let found = configs.iter().find(|config| config.name == "x.y.z");
///     found.and_then(|config| config.value.clone())
/// };
///
/// // The topic carries the injected broker defaults (cleanup.policy/retention.ms),
/// // but the not-yet-altered key must be absent.
/// let before = describing.serve(Context::default(), asking()).await?;
/// assert_eq!(None, value_of_xyz(before));
///
/// let set = AlterableConfig::default().config_operation(i8::from(OpType::Set));
/// let set = set.name(String::from("x.y.z")).value(Some(String::from("pqr")));
/// let resource = AlterConfigsResource::default().resource_name(String::from("abcba"));
/// let resource = resource.resource_type(i8::from(ConfigResource::Topic));
/// let resource = resource.configs(Some(vec![set]));
/// let alter = tansu_sans_io::IncrementalAlterConfigsRequest::default();
/// let _altered = altering.serve(Context::default(), alter.resources(Some(vec![resource]))).await?;
///
/// let after = describing.serve(Context::default(), asking()).await?;
/// assert_eq!(Some(String::from("pqr")), value_of_xyz(after));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IncrementalAlterConfigsService;

impl ApiKey for IncrementalAlterConfigsService {
    const KEY: i16 = IncrementalAlterConfigsRequest::KEY;
}

impl<G> Service<G, IncrementalAlterConfigsRequest> for IncrementalAlterConfigsService
where
    G: Storage,
{
    type Response = IncrementalAlterConfigsResponse;
    type Error = Error;

    #[tracing::instrument(skip(state, request))]
    async fn serve(
        &self,
        state: Context<G>,
        request: IncrementalAlterConfigsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let engine = state.state();
        let mut altered = Vec::new();
        for resource in request.resources.into_iter().flatten() {
            let altering = engine.incremental_alter_resource(resource);
            altered.push(altering.await?);
        }
        let unthrottled = IncrementalAlterConfigsResponse::default().throttle_time_ms(0);
        Ok(unthrottled.responses(Some(altered)))
    }
}
