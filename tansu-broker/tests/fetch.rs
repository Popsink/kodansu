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

use std::collections::BTreeMap;

use bytes::Bytes;
use common::{StorageType, alphanumeric_string, init_tracing, register_broker};
use rama::{Context, Service};
use rand::{prelude::*, rng};
use tansu_broker::Result;
use tansu_sans_io::{
    ErrorCode, FetchRequest, IsolationLevel, ListOffset, NULL_TOPIC_ID,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    record::{Record, inflated},
};
use tansu_storage::{FetchService, ListOffsetResponse, Storage, StorageContainer, Topition};
use tracing::{debug, error};
use url::Url;
use uuid::Uuid;

pub mod common;

pub async fn empty_topic<C>(cluster_id: Uuid, broker_id: i32, sc: StorageContainer) -> Result<()>
where
    C: Into<String>,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let record_count = 0;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(vec![
            FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None),
        ]))];

    let ctx = Context::with_state(sc);

    let fetch = FetchService
        .serve(
            ctx,
            FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
        )
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    for response in fetch.responses.as_ref().unwrap_or(&vec![]) {
        for partition in response.partitions.as_ref().unwrap_or(&vec![]) {
            for batch in &partition.records.as_ref().unwrap().batches {
                assert_eq!(-1, batch.producer_id);
                assert_eq!(-1, batch.producer_epoch);
            }
        }
    }

    assert_eq!(
        record_count,
        fetch
            .responses
            .unwrap_or_default()
            .iter()
            .map(|response| {
                response
                    .partitions
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(|partition| {
                        partition
                            .records
                            .as_ref()
                            .unwrap()
                            .batches
                            .iter()
                            .map(|batch| batch.record_count as i64)
                            .sum::<i64>()
                    })
                    .sum::<i64>()
            })
            .sum::<i64>()
    );

    Ok(())
}

pub async fn simple_non_txn<C>(cluster_id: C, broker_id: i32, sc: StorageContainer) -> Result<()>
where
    C: Into<String>,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let list_offsets = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await
        .inspect_err(|err| error!(?err))
        .inspect(|list_offsets| debug!(?list_offsets))?;

    assert_eq!(1, list_offsets.len());

    assert!(matches!(
        list_offsets[0].1,
        ListOffsetResponse {
            offset: Some(0),
            ..
        }
    ));

    let record_count = 6;

    let mut offset_producer = BTreeMap::new();
    let mut bytes = 0;

    for n in 0..record_count {
        let producer = sc
            .init_producer(None, transaction_timeout_ms, Some(-1), Some(-1))
            .await?;

        let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
        bytes += value.len();

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(value.clone().into()))
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        debug!(n, bytes, ?batch);

        let offset = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?;

        debug!(offset);

        assert_eq!(None, offset_producer.insert(offset, (producer, value)));
    }

    let list_offsets = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await
        .inspect_err(|err| error!(?err))
        .inspect(|list_offsets| debug!(?list_offsets))?;

    assert_eq!(1, list_offsets.len());

    assert!(matches!(
        list_offsets[0].1,
        ListOffsetResponse {
            offset: Some(records),
            ..
            } if records == record_count
    ));

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(vec![
            FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None),
        ]))];

    let ctx = Context::with_state(sc);

    let fetch = FetchService
        .serve(
            ctx,
            FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
        )
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    assert_eq!(
        record_count,
        fetch
            .responses
            .unwrap_or_default()
            .iter()
            .map(|response| {
                response
                    .partitions
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(|partition| {
                        partition
                            .records
                            .as_ref()
                            .unwrap()
                            .batches
                            .iter()
                            .map(|batch| batch.record_count as i64)
                            .sum::<i64>()
                    })
                    .sum::<i64>()
            })
            .sum::<i64>()
    );

    Ok(())
}

/// Reproduce the FETCH v13+/v17 path used by librdkafka and the JVM 4.x
/// client: the request only carries `topic_id`, and `topic` (the name) is
/// absent. Regression test for tansu-io/tansu#668 — Tansu used to accept
/// the request without ever returning a response when the topic was
/// resolved by id only and the storage layer was slow to drain.
pub async fn fetch_by_topic_id<C>(cluster_id: C, broker_id: i32, sc: StorageContainer) -> Result<()>
where
    C: Into<String>,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    let num_partitions = 1;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let topition = Topition::new(topic_name.clone(), 0);

    let transaction_timeout_ms = 10_000;
    let record_count = 4;
    let mut bytes = 0;

    for _ in 0..record_count {
        let producer = sc
            .init_producer(None, transaction_timeout_ms, Some(-1), Some(-1))
            .await?;

        let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
        bytes += value.len();

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(value.clone().into()))
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .build()
            .and_then(TryInto::try_into)?;

        let _offset = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?;
    }

    debug!(record_count, bytes);

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;

    // v13+/v17 style: only topic_id, no topic name. This mirrors what
    // librdkafka 2.x and the Kafka 4.x JVM client send, which was the
    // path that hung in the issue.
    let topics = [FetchTopic::default()
        .topic(None)
        .topic_id(Some(topic_id.into_bytes()))
        .partitions(Some(vec![
            FetchPartition::default()
                .partition(0)
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None),
        ]))];

    let ctx = Context::with_state(sc);

    // The request must complete well before max_wait_ms + a generous slack.
    // The previous implementation could spin past max_wait_ms when storage
    // was slow because the inner loop didn't honour the deadline.
    let started = std::time::Instant::now();
    let fetch = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        FetchService.serve(
            ctx,
            FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
        ),
    )
    .await
    .map_err(|_| {
        tansu_broker::Error::Storage(tansu_storage::Error::Message(
            "FetchService.serve did not return within 5s — issue #668 regressed".into(),
        ))
    })??;
    debug!(elapsed_ms = ?started.elapsed().as_millis());

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    let returned_records: i64 = fetch
        .responses
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|response| {
            response
                .partitions
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .filter_map(|partition| partition.records.as_ref())
                .flat_map(|frame| frame.batches.iter())
                .map(|batch| batch.record_count as i64)
                .sum::<i64>()
        })
        .sum();

    assert_eq!(record_count, returned_records);

    Ok(())
}

/// Companion to [`fetch_by_topic_id`]: even with no produced data, an empty
/// topic referenced by id alone must still get a response within
/// `max_wait_ms` plus reasonable slack — never a hang.
pub async fn fetch_by_topic_id_empty<C>(
    cluster_id: C,
    broker_id: i32,
    sc: StorageContainer,
) -> Result<()>
where
    C: Into<String>,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let max_wait_ms = 200;
    let topics = [FetchTopic::default()
        .topic(None)
        .topic_id(Some(topic_id.into_bytes()))
        .partitions(Some(vec![
            FetchPartition::default()
                .partition(0)
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None),
        ]))];

    let ctx = Context::with_state(sc);

    let started = std::time::Instant::now();
    let fetch = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        FetchService.serve(
            ctx,
            FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(1)
                .max_bytes(Some(50 * 1024))
                .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
                .topics(Some(topics.into())),
        ),
    )
    .await
    .map_err(|_| {
        tansu_broker::Error::Storage(tansu_storage::Error::Message(
            "FetchService.serve did not return within 5s for empty topic — issue #668 regressed"
                .into(),
        ))
    })??;
    debug!(elapsed_ms = ?started.elapsed().as_millis());

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    let responses = fetch.responses.as_deref().unwrap_or_default();
    assert_eq!(1, responses.len());
    let response = &responses[0];
    assert_eq!(Some(topic_id.into_bytes()), response.topic_id);

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn fetch_by_topic_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::fetch_by_topic_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn fetch_by_topic_id_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::fetch_by_topic_id_empty(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn fetch_by_topic_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::fetch_by_topic_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn fetch_by_topic_id_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::fetch_by_topic_id_empty(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::Lite,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn fetch_by_topic_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::fetch_by_topic_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn fetch_by_topic_id_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::fetch_by_topic_id_empty(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;

    async fn storage_container(cluster: impl Into<String>, node: i32) -> Result<StorageContainer> {
        common::storage_container(
            StorageType::SlateDb,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn fetch_by_topic_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::fetch_by_topic_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn fetch_by_topic_id_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::fetch_by_topic_id_empty(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
