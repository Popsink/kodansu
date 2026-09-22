// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use crate::common::{Error, cluster_id, init_tracing, storage_url};
use bytes::Bytes;
use rama::{Context, Layer as _, Service, layer::MapStateLayer};
use std::sync::Arc;
use tansu_sans_io::{
    DeleteRecordsRequest, DeleteRecordsResponse, ErrorCode,
    create_topics_request::CreatableTopic,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    record::{Record, deflated, inflated},
};
use tansu_storage::{DeleteRecordsService, Storage, StorageContainer, Topition};
use tracing::debug;
use url::Url;

mod common;

const PARTITION: i32 = 0;

async fn storage() -> Result<Arc<dyn Storage>, Error> {
    StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .build()
        .await
        .map_err(Into::into)
}

/// A topic with `records` single-record batches on partition 0, so the high
/// watermark is exactly `records`.
async fn topic_with(storage: &Arc<dyn Storage>, name: &str, records: i32) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(name, PARTITION);

    for _ in 0..records {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from_static(b"v"))))
            .build()
            .and_then(deflated::Batch::try_from)?;

        _ = storage.produce(None, &topition, batch).await?;
    }

    Ok(())
}

async fn delete_records(
    storage: Arc<dyn Storage>,
    name: &str,
    offset: i64,
) -> Result<DeleteRecordsResponse, Error> {
    MapStateLayer::new(|_| storage)
        .into_layer(DeleteRecordsService)
        .serve(
            Context::default(),
            DeleteRecordsRequest::default().topics(Some(
                [DeleteRecordsTopic::default()
                    .name(name.into())
                    .partitions(Some(
                        [DeleteRecordsPartition::default()
                            .offset(offset)
                            .partition_index(PARTITION)]
                        .into(),
                    ))]
                .into(),
            )),
        )
        .await
        .inspect(|response| debug!(?response))
        .map_err(Into::into)
}

/// The one partition result of a single-partition request.
fn only(response: DeleteRecordsResponse) -> Result<(ErrorCode, i64), Error> {
    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());

    Ok((
        ErrorCode::try_from(partitions[0].error_code)?,
        partitions[0].low_watermark,
    ))
}

/// A truncation inside the log moves the floor to exactly the offset asked for.
#[tokio::test]
async fn an_offset_inside_the_log_truncates_to_it() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    topic_with(&storage, "inside", 10).await?;

    assert_eq!(
        (ErrorCode::None, 4),
        only(delete_records(storage, "inside", 4).await?)?
    );

    Ok(())
}

/// The high watermark itself is in range — it is the offset that deletes the
/// whole log, and the boundary the refusal below is one past.
#[tokio::test]
async fn the_high_watermark_is_in_range() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    topic_with(&storage, "boundary", 10).await?;

    assert_eq!(
        (ErrorCode::None, 10),
        only(delete_records(storage, "boundary", 10).await?)?
    );

    Ok(())
}

/// `-1` is Kafka's "everything" sentinel and stays one: it resolves to the log
/// end offset rather than falling into the negative refusal.
#[tokio::test]
async fn minus_one_deletes_the_whole_log() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    topic_with(&storage, "everything", 10).await?;

    assert_eq!(
        (ErrorCode::None, 10),
        only(delete_records(storage, "everything", -1).await?)?
    );

    Ok(())
}

/// An offset past the high watermark is refused rather than clamped (#579).
///
/// It used to be clamped, so `delete_records(9999)` on a ten-record partition
/// answered `ok` — having deleted all ten. A typo, or an offset taken from the
/// wrong partition, destroyed the log and reported success.
#[tokio::test]
async fn an_offset_beyond_the_high_watermark_is_refused() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    topic_with(&storage, "beyond", 10).await?;

    assert_eq!(
        (ErrorCode::OffsetOutOfRange, -1),
        only(delete_records(storage.clone(), "beyond", 9999).await?)?
    );

    // And the refusal is a refusal (#579): nothing was truncated, so the whole
    // log is still deletable afterwards.
    assert_eq!(
        (ErrorCode::None, 10),
        only(delete_records(storage, "beyond", 10).await?)?
    );

    Ok(())
}

/// One past the high watermark is out of range too — the off-by-one that a
/// clamp hides is the likeliest way to reach this at all.
#[tokio::test]
async fn one_past_the_high_watermark_is_refused() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    topic_with(&storage, "one-past", 10).await?;

    assert_eq!(
        (ErrorCode::OffsetOutOfRange, -1),
        only(delete_records(storage, "one-past", 11).await?)?
    );

    Ok(())
}

/// A negative offset other than `-1` is out of range, as it is in Kafka's
/// `Partition.deleteRecordsOnLeader`. It used to be read as the everything
/// sentinel, so `delete_records(-2)` deleted the log.
#[tokio::test]
async fn a_negative_offset_that_is_not_the_sentinel_is_refused() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;
    topic_with(&storage, "negative", 10).await?;

    assert_eq!(
        (ErrorCode::OffsetOutOfRange, -1),
        only(delete_records(storage.clone(), "negative", -2).await?)?
    );

    assert_eq!(
        (ErrorCode::None, 10),
        only(delete_records(storage, "negative", 10).await?)?
    );

    Ok(())
}

/// A topic that was never created has no records to delete at any offset above
/// zero, so the same range check answers it.
#[tokio::test]
async fn delete_non_existent_records() -> Result<(), Error> {
    let _guard = init_tracing()?;

    assert_eq!(
        (ErrorCode::OffsetOutOfRange, -1),
        only(delete_records(storage().await?, "abcba", 32123).await?)?
    );

    Ok(())
}
