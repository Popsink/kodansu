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

//! A replica whose view of the log end lags another's does not answer
//! `OFFSET_OUT_OF_RANGE` to a consumer the other replica served.
//!
//! On euw3 at 08:41 UTC on 2026-10-23, as `alpha.21` rolled out, several consumer
//! groups replayed their whole topic from offset 0 with no rebalance: one
//! group's committed offset was 5 735 693 of a 5 795 451 log, and it read
//! 4.85 M records again. A consumer reconnecting to a freshly started pod asks
//! for the position another pod served it; the new pod's high watermark is its
//! own index's, which can lag, and `offset > high_watermark` was answered
//! `OFFSET_OUT_OF_RANGE` (#444). librdkafka applies `auto.offset.reset` to that
//! answer, so `earliest` turned one stale view into a full replay.

use std::time::Duration;

use bytes::Bytes;
use object_store::memory::InMemory;
use rama::{Context, Service as _};
use tansu_sans_io::{
    ErrorCode, FetchRequest, IsolationLevel,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    record::{Record, deflated, inflated},
};

use crate::{
    Result, Storage as _, Topition,
    dynostore::{CoalesceTuning, DynoStore, tests::init_tracing},
    service::FetchService,
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const TOPIC: &str = "org.env.conn.table";

/// A replica that keeps its view of the log end for an hour, as a pod does
/// between its index refreshes.
fn replica(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        coalesce_linger: Some(Duration::from_millis(1)),
        coalesce_batches: Some(1),
        watermark_hint_ttl: Some(Duration::from_secs(3600)),
        ..Default::default()
    })
}

fn batch(records: i32) -> Result<deflated::Batch> {
    (0..records)
        .fold(inflated::Batch::builder(), |builder, i| {
            builder.record(
                Record::builder()
                    .offset_delta(i)
                    .value(Some(Bytes::from(format!("record-{i}")))),
            )
        })
        .last_offset_delta(records - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// The error code and the first served offset of a Fetch at `offset` through
/// the service, which is where `OFFSET_OUT_OF_RANGE` is decided.
async fn fetch(store: &DynoStore, offset: i64) -> Result<(ErrorCode, Option<i64>)> {
    let response = FetchService
        .serve(
            Context::with_state(store.clone()),
            FetchRequest::default()
                .max_wait_ms(500)
                .min_bytes(1)
                .max_bytes(Some(1 << 20))
                .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(TOPIC.to_owned()))
                        .partitions(Some(
                            [FetchPartition::default()
                                .partition(0)
                                .fetch_offset(offset)
                                .partition_max_bytes(1 << 20)]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    let partition = response
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| topic.partitions.unwrap_or_default())
        .next()
        .expect("one partition");

    Ok((
        ErrorCode::try_from(partition.error_code)?,
        partition
            .records
            .and_then(|frame| frame.batches.first().map(|batch| batch.base_offset)),
    ))
}

async fn writer_ahead_of_reader() -> Result<(DynoStore, DynoStore)> {
    let bucket = InMemory::new();
    let writer = replica(&bucket);
    let reader = replica(&bucket);

    _ = writer
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

    let tp = Topition::new(TOPIC, 0);
    _ = writer.produce(None, &tp, batch(10)?).await?;

    // The reader looks once and settles on a log end of 10 for the hour.
    assert_eq!((ErrorCode::None, Some(0)), fetch(&reader, 0).await?);
    assert_eq!(10, reader.high_watermark(&tp).await?);

    _ = writer.produce(None, &tp, batch(10)?).await?;
    assert_eq!(20, writer.high_watermark(&tp).await?);

    Ok((writer, reader))
}

/// A consumer the writer served up to 15 reconnects to the lagging reader and
/// is served from 15, not told its position is outside the log.
#[tokio::test]
async fn a_lagging_view_refreshes_before_answering_out_of_range() -> Result<()> {
    let _guard = init_tracing()?;

    let (writer, reader) = writer_ahead_of_reader().await?;
    assert_eq!((ErrorCode::None, Some(10)), fetch(&writer, 15).await?);

    assert_eq!(
        (ErrorCode::None, Some(10)),
        fetch(&reader, 15).await?,
        "a position past this replica's stale log end is not out of range \
         until a listing says so"
    );

    Ok(())
}

/// A position past the log end the bucket holds is still out of range, which
/// is the self-healing #444 exists for.
#[tokio::test]
async fn a_position_past_the_real_log_end_is_still_out_of_range() -> Result<()> {
    let _guard = init_tracing()?;

    let (_writer, reader) = writer_ahead_of_reader().await?;

    assert_eq!(
        ErrorCode::OffsetOutOfRange,
        fetch(&reader, 100).await?.0,
        "a listing confirmed it"
    );

    Ok(())
}
