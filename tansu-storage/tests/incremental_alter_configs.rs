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

//! `IncrementalAlterConfigs`: a topic config that was never set is absent, and
//! once set reads back through `DescribeConfigs`.

use std::sync::Arc;

use tansu_sans_io::describe_configs_request::DescribeConfigsResource;
use tansu_sans_io::describe_configs_response::DescribeConfigsResult;
use tansu_sans_io::incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig};
use tansu_sans_io::{
    ConfigResource, DescribeConfigsRequest, IncrementalAlterConfigsRequest, OpType,
};
use tansu_storage::{DescribeConfigsService, IncrementalAlterConfigsService, Storage};

use crate::common::{Error, init_tracing, serve, storage, topic_partitioned};

mod common;

const TOPIC: &str = "abcba";
const KEY: &str = "x.y.z";

/// `TOPIC` named as a config resource, the same way in every request.
fn topic_resource() -> (String, i8) {
    (TOPIC.to_owned(), ConfigResource::Topic.into())
}

/// What `DescribeConfigs` answers for `KEY` on `TOPIC`.
async fn described(storage: &Arc<dyn Storage>) -> Result<DescribeConfigsResult, Error> {
    let (name, kind) = topic_resource();
    let resource = DescribeConfigsResource::default()
        .resource_name(name)
        .resource_type(kind)
        .configuration_keys(Some(vec![KEY.to_owned()]));
    let request = DescribeConfigsRequest::default().resources(Some(vec![resource]));
    let response = serve(storage, DescribeConfigsService, request).await?;
    let [result] = <[_; 1]>::try_from(response.results.unwrap_or_default()).expect("one result");
    Ok(result)
}

/// Applies `operation` to `KEY` on `TOPIC`, with `value` for a `Set`.
async fn alter(
    storage: &Arc<dyn Storage>,
    operation: OpType,
    value: Option<&str>,
) -> Result<(), Error> {
    let change = AlterableConfig::default()
        .config_operation(operation.into())
        .name(KEY.to_owned())
        .value(value.map(str::to_owned));
    let (name, kind) = topic_resource();
    let resource = AlterConfigsResource::default()
        .resource_name(name)
        .resource_type(kind)
        .configs(Some(vec![change]));
    let request = IncrementalAlterConfigsRequest::default().resources(Some(vec![resource]));
    _ = serve(storage, IncrementalAlterConfigsService, request).await?;
    Ok(())
}

/// The value `KEY` reads back as, if it is there at all.
fn value_of(result: &DescribeConfigsResult) -> Option<Option<&str>> {
    let configs = result.configs.as_deref().unwrap_or_default();
    let config = configs.iter().find(|config| config.name == KEY)?;
    Some(config.value.as_deref())
}

#[tokio::test]
async fn a_set_config_reads_back_its_value() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    _ = topic_partitioned(&storage, TOPIC, 3).await?;

    // The topic carries the injected broker defaults (cleanup.policy/retention.ms),
    // but the not-yet-altered key must be absent.
    let before = described(&storage).await?;
    assert_eq!(
        None,
        value_of(&before),
        "unset config `{KEY}` must be absent before it is altered"
    );

    alter(&storage, OpType::Set, Some("pqr")).await?;

    let after = described(&storage).await?;
    assert_eq!(TOPIC, after.resource_name);
    assert_eq!(
        Some(Some("pqr")),
        value_of(&after),
        "altered config `{KEY}` must read back its new value"
    );

    Ok(())
}

/// A deleted config is absent again: `x.y.z` has no broker default to fall back
/// to.
#[tokio::test]
async fn a_deleted_config_is_absent_again() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    _ = topic_partitioned(&storage, TOPIC, 3).await?;

    alter(&storage, OpType::Set, Some("pqr")).await?;
    alter(&storage, OpType::Delete, None).await?;

    let after = described(&storage).await?;
    assert_eq!(
        None,
        value_of(&after),
        "deleted config `{KEY}` must be absent"
    );

    Ok(())
}
