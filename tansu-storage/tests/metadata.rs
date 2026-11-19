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

//! `Metadata`: the broker list, and which topics a request's list asks for at
//! each version.

use std::sync::Arc;

use rama::Service as _;
use tansu_sans_io::metadata_request::MetadataRequestTopic;
use tansu_sans_io::{MetadataRequest, MetadataResponse, RequestApiVersion};
use tansu_storage::{MetadataService, Storage};

use crate::common::{Error, HOST, NODE_ID, PORT, init_tracing, serve, storage, topic_partitioned};

mod common;

/// Serves `request` as a frame decoded at `version` would be — or with no
/// version at all, as an in-process client asks.
async fn metadata_at(
    storage: Arc<dyn Storage>,
    version: Option<i16>,
    request: MetadataRequest,
) -> Result<MetadataResponse, Error> {
    let mut ctx = rama::Context::with_state(storage);
    if let Some(version) = version {
        _ = ctx.insert(RequestApiVersion(version));
    }
    Ok(MetadataService.serve(ctx, request).await?)
}

/// The names in a `Metadata` answer to `topics`, asked at `version`, on a store
/// holding the one topic `orders`.
async fn topics_answered(
    version: Option<i16>,
    topics: Option<Vec<MetadataRequestTopic>>,
) -> Result<(usize, Vec<Option<String>>), Error> {
    let storage = storage().await?;
    _ = topic_partitioned(&storage, "orders", 3).await?;

    let request = MetadataRequest::default().topics(topics);
    let MetadataResponse {
        brokers, topics, ..
    } = metadata_at(storage, version, request).await?;

    let names = topics.unwrap_or_default().into_iter().map(|t| t.name);
    Ok((brokers.map_or(0, |b| b.len()), names.collect()))
}

/// The one broker is this node, with no rack.
#[tokio::test]
async fn the_only_broker_is_this_node() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let request = MetadataRequest::default().topics(Some(Vec::new()));
    let response = serve(&storage().await?, MetadataService, request).await?;

    let [broker] = <[_; 1]>::try_from(response.brokers.unwrap_or_default()).expect("one broker");
    assert_eq!(
        (NODE_ID, HOST, PORT, None),
        (
            broker.node_id,
            broker.host.as_str(),
            broker.port,
            broker.rack
        )
    );

    Ok(())
}

/// From v1 an empty list asks for the brokers alone (#593). A client sends it at
/// bootstrap and from `describeCluster`, and answering it with every topic made
/// `Metadata` 87 % of the bytes a production fleet sent.
#[tokio::test]
async fn an_empty_list_at_v1_and_later_asks_for_no_topic() -> Result<(), Error> {
    let _guard = init_tracing()?;

    assert_eq!((1, vec![]), topics_answered(Some(12), Some(vec![])).await?);
    assert_eq!((1, vec![]), topics_answered(Some(1), Some(vec![])).await?);

    Ok(())
}

/// A request built in process carries no version, and keeps the meaning of the
/// version its client encodes at, the latest.
#[tokio::test]
async fn an_empty_list_without_a_version_asks_for_no_topic() -> Result<(), Error> {
    let _guard = init_tracing()?;

    assert_eq!((1, vec![]), topics_answered(None, Some(vec![])).await?);

    Ok(())
}

/// `null` is still every topic.
#[tokio::test]
async fn a_null_list_asks_for_every_topic() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let every = (1, vec![Some("orders".to_owned())]);

    assert_eq!(every, topics_answered(Some(12), None).await?);
    assert_eq!(every, topics_answered(None, None).await?);

    Ok(())
}

/// At v0 the list is not nullable, so `[]` is the only way to ask for every
/// topic, and it still does.
#[tokio::test]
async fn an_empty_list_at_v0_asks_for_every_topic() -> Result<(), Error> {
    let _guard = init_tracing()?;

    assert_eq!(
        (1, vec![Some("orders".to_owned())]),
        topics_answered(Some(0), Some(vec![])).await?
    );

    Ok(())
}

/// A topic asked for by id is answered by id, with its name, although the Java
/// client sends `name` at its non-null default `""` beside the id (#625). That
/// is how a KIP-848 consumer names the topics it was assigned.
#[tokio::test]
async fn a_topic_asked_for_by_id_is_answered_with_its_name() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    let id = topic_partitioned(&storage, "orders", 3).await?.into_bytes();

    let by_id = MetadataRequestTopic::default()
        .topic_id(Some(id))
        .name(Some(String::new()));
    let request = MetadataRequest::default().topics(Some(vec![by_id]));
    let response = metadata_at(storage, Some(12), request).await?;

    let [topic] = <[_; 1]>::try_from(response.topics.unwrap_or_default()).expect("one topic");
    let partitions = topic.partitions.map_or(0, |p| p.len());
    assert_eq!(
        (0, Some("orders"), Some(id), 3),
        (
            topic.error_code,
            topic.name.as_deref(),
            topic.topic_id,
            partitions
        )
    );

    Ok(())
}
