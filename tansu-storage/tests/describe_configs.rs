// Copyright ⓒ 2026 Popsink SAS
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

//! `DescribeConfigs` against topics that exist, never existed, and were
//! deleted.

use std::sync::Arc;

use tansu_sans_io::describe_configs_request::DescribeConfigsResource;
use tansu_sans_io::{ConfigResource, DescribeConfigsRequest, DescribeConfigsResponse, ErrorCode};
use tansu_storage::{DescribeConfigsService, Storage, TopicId};

use crate::common::{Error, init_tracing, serve, storage, topic};

mod common;

/// Every config of topic `name`.
async fn describe(
    storage: &Arc<dyn Storage>,
    name: &str,
) -> Result<DescribeConfigsResponse, Error> {
    let resource = DescribeConfigsResource::default()
        .resource_type(i8::from(ConfigResource::Topic))
        .resource_name(name.to_owned())
        .configuration_keys(Some(Vec::new()));
    let request = DescribeConfigsRequest::default().resources(Some(vec![resource]));
    serve(storage, DescribeConfigsService, request).await
}

/// The error code of the one result a single-resource describe returns.
fn outcome(response: &DescribeConfigsResponse) -> Result<ErrorCode, Error> {
    let results = response.results.as_deref().unwrap_or_default();
    assert_eq!(1, results.len());
    Ok(ErrorCode::try_from(results[0].error_code)?)
}

/// A topic that exists is described, and the broker-level defaults it was
/// created with are what comes back.
#[tokio::test]
async fn an_existing_topic_is_described() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let name = "abcba";

    topic(&storage, name).await?;

    let response = describe(&storage, name).await?;
    assert_eq!(ErrorCode::None, outcome(&response)?);
    let configs = response.results.unwrap_or_default().remove(0).configs;
    assert!(!configs.unwrap_or_default().is_empty());

    Ok(())
}

/// A topic that was never created is reported as unknown (#579).
///
/// It used to answer `NONE` with an empty config list, which makes
/// `DescribeConfigs` useless as an existence check — it says yes to every name
/// ever spelled — and, worse, says yes in exactly the way that reads as "the
/// topic is there and has no settings".
#[tokio::test]
async fn a_topic_that_does_not_exist_is_unknown() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let response = describe(&storage().await?, "never-created").await?;
    assert_eq!(ErrorCode::UnknownTopicOrPartition, outcome(&response)?);
    let result = response.results.unwrap_or_default().remove(0);
    assert_eq!("never-created", result.resource_name);
    assert!(result.configs.unwrap_or_default().is_empty());

    Ok(())
}

/// A deleted topic goes back to unknown: the answer tracks existence, not
/// whether the name was ever used.
#[tokio::test]
async fn a_deleted_topic_is_unknown_again() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let name = "transient";

    topic(&storage, name).await?;

    assert_eq!(ErrorCode::None, outcome(&describe(&storage, name).await?)?);

    assert_eq!(
        ErrorCode::None,
        storage.delete_topic(&TopicId::Name(name.into())).await?
    );

    let after = describe(&storage, name).await?;
    assert_eq!(ErrorCode::UnknownTopicOrPartition, outcome(&after)?);

    Ok(())
}
