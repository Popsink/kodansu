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

mod common;

mod doctest_template {
    use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
    use tansu_sans_io::{
        CreateTopicsRequest, ErrorCode, FetchRequest,
        create_topics_request::CreatableTopic,
        fetch_request::{FetchPartition, FetchTopic},
    };
    use tansu_storage::{CreateTopicsService, FetchService, StorageContainer};
    use url::Url;

    use crate::common::{Error, cluster_id, init_tracing, storage_url};

    #[tokio::test]
    async fn req() -> Result<(), Error> {
        let _guard = init_tracing()?;
        const NODE_ID: i32 = 111;
        const HOST: &str = "localhost";
        const PORT: i32 = 9092;

        let storage = StorageContainer::builder()
            .cluster_id(cluster_id())
            .node_id(NODE_ID)
            .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
            .storage(storage_url()?)
            .build()
            .await?;

        let create_topic = {
            let storage = storage.clone();
            MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
        };

        let name = "abcba";

        let response = create_topic
            .serve(
                Context::default(),
                CreateTopicsRequest::default()
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(name.into())
                            .num_partitions(5)
                            .replication_factor(3)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                    ]))
                    .validate_only(Some(false)),
            )
            .await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        let fetch = {
            let storage = storage.clone();
            MapStateLayer::new(|_| storage).into_layer(FetchService)
        };

        let partition = 0;

        let response = fetch
            .serve(
                Context::default(),
                FetchRequest::default()
                    .topics(Some(
                        [FetchTopic::default()
                            .topic(Some(name.into()))
                            .partitions(Some(
                                [FetchPartition::default().partition(partition)].into(),
                            ))]
                        .into(),
                    ))
                    .max_bytes(Some(0))
                    .max_wait_ms(5_000),
            )
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        let partitions = topics[0].partitions.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );

        Ok(())
    }
}

/// Exercises the `start-after` based fetch on the `dynostore` (`memory://`) engine: fetch must
/// return the contiguous run of batches at or after the requested offset, bounded by `max_bytes`,
/// without depending on the partition's total history.
mod start_after {
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;
    use tansu_sans_io::{
        IsolationLevel,
        record::{Record, deflated, inflated},
    };
    use tansu_storage::{Storage, StorageContainer, Topition};
    use url::Url;

    use crate::common::{Error, cluster_id, init_tracing, storage_url};
    const NODE_ID: i32 = 111;
    const MAX_WAIT: Duration = Duration::from_secs(5);

    async fn storage_container() -> Result<Arc<Box<dyn Storage>>, Error> {
        StorageContainer::builder()
            .cluster_id(cluster_id())
            .node_id(NODE_ID)
            .advertised_listener(Url::parse("tcp://localhost:9092")?)
            .storage(storage_url()?)
            .build()
            .await
            .map_err(Into::into)
    }

    fn single_record_batch(value: &'static [u8]) -> Result<deflated::Batch, Error> {
        inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(value).into()))
            .build()
            .and_then(deflated::Batch::try_from)
            .map_err(Into::into)
    }

    /// Produce `count` single-record batches; each occupies one offset, so base offsets are
    /// `0..count` and the high watermark ends at `count`.
    async fn produce_batches(
        storage: &Arc<Box<dyn Storage>>,
        topition: &Topition,
        count: i64,
    ) -> Result<(), Error> {
        for offset in 0..count {
            let assigned = storage
                .produce(None, topition, single_record_batch(b"lorem ipsum")?)
                .await?;
            assert_eq!(offset, assigned);
        }

        Ok(())
    }

    #[tokio::test]
    async fn fetch_from_middle_returns_contiguous_tail() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = storage_container().await?;
        let topition = Topition::new("abcba", 0);

        produce_batches(&storage, &topition, 10).await?;

        let batches = storage
            .fetch(
                &topition,
                4,
                0,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                MAX_WAIT,
            )
            .await?;

        let base_offsets = batches.iter().map(|b| b.base_offset).collect::<Vec<_>>();
        assert_eq!(vec![4, 5, 6, 7, 8, 9], base_offsets);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_from_start_returns_all() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = storage_container().await?;
        let topition = Topition::new("abcba", 0);

        produce_batches(&storage, &topition, 10).await?;

        let batches = storage
            .fetch(
                &topition,
                0,
                0,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                MAX_WAIT,
            )
            .await?;

        let base_offsets = batches.iter().map(|b| b.base_offset).collect::<Vec<_>>();
        assert_eq!((0..10).collect::<Vec<_>>(), base_offsets);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_is_bounded_by_max_bytes() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = storage_container().await?;
        let topition = Topition::new("abcba", 0);

        produce_batches(&storage, &topition, 10).await?;

        // A single byte budget always yields at least one batch (Kafka semantics) but never the
        // whole partition.
        let batches = storage
            .fetch(
                &topition,
                0,
                0,
                1,
                IsolationLevel::ReadUncommitted,
                MAX_WAIT,
            )
            .await?;

        assert_eq!(1, batches.len());
        assert_eq!(0, batches[0].base_offset);

        Ok(())
    }

    #[tokio::test]
    async fn fetch_at_or_beyond_high_watermark_is_empty() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = storage_container().await?;
        let topition = Topition::new("abcba", 0);

        produce_batches(&storage, &topition, 10).await?;

        for offset in [10, 11, 100] {
            let batches = storage
                .fetch(
                    &topition,
                    offset,
                    0,
                    1024 * 1024,
                    IsolationLevel::ReadUncommitted,
                    MAX_WAIT,
                )
                .await?;
            assert!(batches.is_empty(), "offset {offset} should be empty");
        }

        Ok(())
    }
}

/// A fetch position outside the log is answered `OFFSET_OUT_OF_RANGE` (#444).
///
/// That error is every consumer's self-healing mechanism: it is how a client
/// detects that its position no longer means what it meant — retention expiry,
/// a `DeleteRecords` truncation, a topic deleted and recreated (#442) — and
/// applies its `auto.offset.reset`. Without it the two ends of the log fail
/// differently and both fail silently: a position above the end yields nothing
/// and parks on the long poll, so the consumer polls forever with lag growing
/// and nothing to alarm on; a position below the start resolves to the oldest
/// surviving records and serves them as though they were the ones asked for.
///
/// Through `FetchService`, because that is where the check belongs and where it
/// is: `Storage::fetch` answering an empty read for a position past the end is
/// correct at its own level, and the storage-level tests above still assert it.
mod out_of_range {
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;
    use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
    use tansu_sans_io::{
        ErrorCode, FetchRequest, IsolationLevel,
        create_topics_request::CreatableTopic,
        fetch_request::{FetchPartition, FetchTopic},
        fetch_response::PartitionData,
        record::{Record, deflated, inflated},
    };
    use tansu_storage::{FetchService, Storage, StorageContainer, TopicId, Topition};
    use url::Url;

    use crate::common::{Error, cluster_id, init_tracing, storage_url};

    const NODE_ID: i32 = 111;
    const TOPIC: &str = "out-of-range";
    const PARTITION: i32 = 0;

    async fn storage_container() -> Result<Arc<Box<dyn Storage>>, Error> {
        StorageContainer::builder()
            .cluster_id(cluster_id())
            .node_id(NODE_ID)
            .advertised_listener(Url::parse("tcp://localhost:9092")?)
            .storage(storage_url()?)
            .build()
            .await
            .map_err(Into::into)
    }

    async fn create_topic(storage: &Arc<Box<dyn Storage>>) -> Result<(), Error> {
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

        Ok(())
    }

    async fn produce(storage: &Arc<Box<dyn Storage>>, count: i64) -> Result<(), Error> {
        let topition = Topition::new(TOPIC, PARTITION);

        for _ in 0..count {
            _ = storage
                .produce(
                    None,
                    &topition,
                    inflated::Batch::builder()
                        .record(Record::builder().value(Bytes::from_static(b"lorem ipsum").into()))
                        .build()
                        .and_then(deflated::Batch::try_from)?,
                )
                .await?;
        }

        Ok(())
    }

    /// One partition's answer to a fetch at `offset`.
    async fn fetch_at(
        storage: &Arc<Box<dyn Storage>>,
        offset: i64,
    ) -> Result<PartitionData, Error> {
        let service = {
            let storage = storage.clone();
            MapStateLayer::new(|_| storage).into_layer(FetchService)
        };

        let response = service
            .serve(
                Context::default(),
                FetchRequest::default()
                    // Short, not zero: the poll loop is `while remaining > 0`,
                    // so a `max_wait_ms` of 0 returns before it has looked at
                    // any partition at all. An out-of-range position is refused
                    // on the first iteration and breaks out immediately, so
                    // none of these tests actually waits.
                    .max_wait_ms(100)
                    .min_bytes(0)
                    .max_bytes(Some(1024 * 1024))
                    .isolation_level(Some(i8::from(IsolationLevel::ReadUncommitted)))
                    .topics(Some(
                        [FetchTopic::default()
                            .topic(Some(TOPIC.into()))
                            .partitions(Some(
                                [FetchPartition::default()
                                    .partition(PARTITION)
                                    .fetch_offset(offset)
                                    .partition_max_bytes(1024 * 1024)]
                                .into(),
                            ))]
                        .into(),
                    )),
            )
            .await?;

        let responses = response.responses.unwrap_or_default();
        assert_eq!(1, responses.len());

        let partitions = responses[0].partitions.clone().unwrap_or_default();
        assert_eq!(1, partitions.len());

        Ok(partitions[0].clone())
    }

    /// The symptom the incident report describes: a consumer at a position past
    /// the end polls with no data and no error, forever. Connected, zero
    /// throughput, lag growing — the hardest incident to diagnose.
    #[tokio::test]
    async fn a_fetch_beyond_the_log_end_is_out_of_range() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = storage_container().await?;
        create_topic(&storage).await?;
        produce(&storage, 10).await?;

        for offset in [11, 100, 9999] {
            let partition = fetch_at(&storage, offset).await?;

            assert_eq!(
                ErrorCode::OffsetOutOfRange,
                ErrorCode::try_from(partition.error_code)?,
                "offset {offset} is past the log end",
            );

            // The bounds a client resets from have to be the real ones: an
            // `OFFSET_OUT_OF_RANGE` carrying a log start of -1 would tell a
            // consumer its position is invalid and give it nowhere to move to.
            assert_eq!(10, partition.high_watermark);
            assert_eq!(Some(0), partition.log_start_offset);
        }

        Ok(())
    }

    /// The other half, and the one that matters more: `fetch_offset ==
    /// high_watermark` is what every caught-up consumer sends on every poll.
    /// Refusing it would fail the whole fleet, so the boundary is asserted from
    /// both sides.
    #[tokio::test]
    async fn a_fetch_at_the_log_end_is_in_range() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = storage_container().await?;
        create_topic(&storage).await?;
        produce(&storage, 10).await?;

        for offset in [0, 4, 9, 10] {
            let partition = fetch_at(&storage, offset).await?;

            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(partition.error_code)?,
                "offset {offset} is inside the log",
            );
        }

        Ok(())
    }

    /// A topic deleted and recreated keeps its predecessor's offsets — the old
    /// records live in shared, immutable segments keyed by `(topic, partition)`
    /// name, so the successor's log starts at the old log end rather than at 0
    /// (#442). The truncation floor hides the old records, which is what stops
    /// them being served as the new topic's; what was left was a consumer
    /// holding a position from before the delete being answered with the
    /// *successor's* records as though nothing had happened.
    ///
    /// Now it is told its position is gone, which is the one signal that makes
    /// the recreation visible to it.
    #[tokio::test]
    async fn a_fetch_below_the_log_start_is_out_of_range() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = storage_container().await?;

        create_topic(&storage).await?;
        produce(&storage, 10).await?;

        _ = storage.delete_topic(&TopicId::Name(TOPIC.into())).await?;

        create_topic(&storage).await?;
        produce(&storage, 3).await?;

        // The successor's log is [10, 13): its start is the predecessor's end.
        let caught_up = fetch_at(&storage, 10).await?;
        assert_eq!(ErrorCode::None, ErrorCode::try_from(caught_up.error_code)?);
        assert_eq!(13, caught_up.high_watermark);
        assert_eq!(Some(10), caught_up.log_start_offset);

        // A consumer resuming from where it was before the delete.
        let stale = fetch_at(&storage, 5).await?;
        assert_eq!(
            ErrorCode::OffsetOutOfRange,
            ErrorCode::try_from(stale.error_code)?,
            "a position from before the recreation is gone, not merely behind",
        );
        assert_eq!(Some(10), stale.log_start_offset);

        Ok(())
    }

    /// A partition that has never been written to has an empty log whose start
    /// is its end, so offset 0 is the only position in range — and it is the one
    /// every fresh consumer starts from.
    #[tokio::test]
    async fn a_fetch_at_zero_on_an_empty_log_is_in_range() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = storage_container().await?;
        create_topic(&storage).await?;

        let partition = fetch_at(&storage, 0).await?;
        assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);

        let past_the_end = fetch_at(&storage, 1).await?;
        assert_eq!(
            ErrorCode::OffsetOutOfRange,
            ErrorCode::try_from(past_the_end.error_code)?,
        );

        Ok(())
    }

    const _: Duration = Duration::from_secs(1);
}
