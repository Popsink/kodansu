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

//! Kafka's `message.max.bytes` (#443).
//!
//! Without a cap an unbounded payload reaches the write path, and an
//! application built against a broker with no limit breaks the day it is
//! pointed at a stock Kafka that has one. The default matches Kafka's exactly
//! for that reason: what fits here fits there.

use std::sync::Arc;

use bytes::Bytes;
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use tansu_sans_io::{
    ErrorCode, ProduceRequest,
    create_topics_request::CreatableTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use tansu_storage::{ProduceService, Storage, StorageContainer};
use url::Url;

use crate::common::{Error, cluster_id, init_tracing, storage_url_with_query};

mod common;

const TOPIC: &str = "message-max-bytes";
const PARTITION: i32 = 0;

async fn storage_from(query: &str) -> Result<Arc<Box<dyn Storage>>, Error> {
    let storage = StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url_with_query(query)?)
        .build()
        .await?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(storage)
}

/// A single-record batch carrying `value_bytes` of payload.
fn batch_of(value_bytes: usize) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from(vec![7u8; value_bytes]))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// The error code the produce path answers for a batch of `value_bytes`.
async fn produce(storage: &Arc<Box<dyn Storage>>, value_bytes: usize) -> Result<ErrorCode, Error> {
    let service = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(ProduceService)
    };

    let response = service
        .serve(
            Context::default(),
            ProduceRequest::default()
                .acks(-1)
                .timeout_ms(30_000)
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(TOPIC.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(PARTITION)
                                .records(Some(deflated::Frame {
                                    batches: [batch_of(value_bytes)?].into(),
                                }))]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());

    let partitions = responses[0].partition_responses.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());

    ErrorCode::try_from(partitions[0].error_code).map_err(Into::into)
}

/// The report's own case: a 2 MiB message against a broker whose default is
/// Kafka's 1 MiB. It was accepted and delivered.
#[tokio::test]
async fn a_batch_over_the_default_is_refused() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage_from("").await?;

    assert_eq!(
        ErrorCode::MessageTooLarge,
        produce(&storage, 2 * 1024 * 1024).await?,
    );

    Ok(())
}

/// And an ordinary batch still lands. The cap has to be invisible to everything
/// a Kafka client sends by default — `max.request.size` is 1 MiB client-side,
/// so nothing reaches the limit without having been configured past it.
#[tokio::test]
async fn a_batch_under_the_default_is_accepted() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage_from("").await?;

    for value_bytes in [0, 1, 1024, 512 * 1024] {
        assert_eq!(
            ErrorCode::None,
            produce(&storage, value_bytes).await?,
            "{value_bytes} bytes must be accepted",
        );
    }

    Ok(())
}

/// A deployment that genuinely sends larger batches raises the limit rather
/// than losing them — the escape hatch that makes the conformant default safe
/// to adopt.
#[tokio::test]
async fn the_limit_is_raised_from_the_storage_url() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage_from("message_max_bytes=8m").await?;

    assert_eq!(ErrorCode::None, produce(&storage, 2 * 1024 * 1024).await?);

    Ok(())
}

/// And lowered, which is what makes the boundary testable at all without moving
/// megabytes around.
#[tokio::test]
async fn the_limit_is_lowered_from_the_storage_url() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage_from("message_max_bytes=4k").await?;

    assert_eq!(ErrorCode::None, produce(&storage, 1024).await?);
    assert_eq!(
        ErrorCode::MessageTooLarge,
        produce(&storage, 8 * 1024).await?,
    );

    Ok(())
}

/// An unparseable value keeps the default rather than becoming something else.
/// A size limit that silently changed is worse than one that was ignored: the
/// operator believes a number that is not in force.
#[tokio::test]
async fn an_unparseable_limit_keeps_the_default() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage_from("message_max_bytes=not-a-size").await?;

    assert_eq!(ErrorCode::None, produce(&storage, 512 * 1024).await?);
    assert_eq!(
        ErrorCode::MessageTooLarge,
        produce(&storage, 2 * 1024 * 1024).await?,
    );

    Ok(())
}
