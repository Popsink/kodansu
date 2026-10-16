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

use crate::common::{Error, cluster_id, init_tracing, storage_url};
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use tansu_sans_io::{
    MetadataRequest, RequestApiVersion, create_topics_request::CreatableTopic,
    metadata_request::MetadataRequestTopic,
};
use tansu_storage::{MetadataService, Storage as _, StorageContainer};
use url::Url;

mod common;

#[tokio::test]
async fn req() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const HOST: &str = "localhost";
    const PORT: i32 = 9092;
    const NODE_ID: i32 = 111;

    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(storage_url()?)
        .build()
        .await?;

    let service = MapStateLayer::new(|_| storage).into_layer(MetadataService);

    let response = service
        .serve(
            Context::default(),
            MetadataRequest::default()
                .allow_auto_topic_creation(Some(false))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .topics(Some([].into())),
        )
        .await?;

    let brokers = response.brokers.as_deref().unwrap_or_default();
    assert_eq!(1, brokers.len());
    assert_eq!(HOST, brokers[0].host);
    assert_eq!(PORT, brokers[0].port);
    assert_eq!(NODE_ID, brokers[0].node_id);
    assert!(brokers[0].rack.is_none());

    Ok(())
}

/// The names in a `Metadata` answer to `topics`, asked at `version` — or with
/// no version at all, as an in-process client asks.
async fn topics_answered(
    version: Option<i16>,
    topics: Option<Vec<MetadataRequestTopic>>,
) -> Result<(usize, Vec<Option<String>>), Error> {
    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .build()
        .await?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("orders".into())
                .num_partitions(3)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let mut ctx = Context::default();

    if let Some(version) = version {
        _ = ctx.insert(RequestApiVersion(version));
    }

    let response = MapStateLayer::new(|_| storage)
        .into_layer(MetadataService)
        .serve(ctx, MetadataRequest::default().topics(topics))
        .await?;

    Ok((
        response.brokers.unwrap_or_default().len(),
        response
            .topics
            .unwrap_or_default()
            .into_iter()
            .map(|topic| topic.name)
            .collect(),
    ))
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
