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

//! `cleanup.policy=delete` (retention) for the dynostore (object store) backend.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::common::{Error, init_tracing};
use bytes::Bytes;
use tansu_sans_io::{
    ErrorCode, IsolationLevel,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
};
use tansu_storage::{Storage, StorageContainer, Topition};
use url::Url;

mod common;

type Sc = Arc<Box<dyn Storage>>;

async fn memory_storage() -> Result<Sc, Error> {
    StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(Url::parse("memory://tansu/")?)
        .build()
        .await
        .map_err(Into::into)
}

fn batch(value: &'static [u8]) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(value))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(
    storage: &Sc,
    name: &str,
    configs: Vec<CreatableTopicConfig>,
) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some(configs)),
            false,
        )
        .await?;

    Ok(())
}

async fn fetch_len(storage: &Sc, topition: &Topition) -> Result<usize, Error> {
    storage
        .fetch(
            topition,
            0,
            1,
            64 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(500),
        )
        .await
        .map(|batches| batches.len())
        .map_err(Into::into)
}

async fn produce_three(storage: &Sc, topition: &Topition) -> Result<(), Error> {
    for value in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        _ = storage.produce(None, topition, batch(value)?).await?;
    }

    Ok(())
}

#[tokio::test]
async fn retention_ms_expires_old_records() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = memory_storage().await?;

    let retention = Duration::from_mins(30);

    create_topic(
        &storage,
        "retained",
        vec![
            CreatableTopicConfig::default()
                .name("cleanup.policy".into())
                .value(Some("delete".into())),
            CreatableTopicConfig::default()
                .name("retention.ms".into())
                .value(Some(retention.as_millis().to_string())),
        ],
    )
    .await?;

    let topition = Topition::new("retained", 0);

    produce_three(&storage, &topition).await?;

    // the three batches are present
    assert_eq!(3, fetch_len(&storage, &topition).await?);

    // a maintenance pass now keeps everything: nothing is older than 30m
    storage.maintain(SystemTime::now()).await?;
    assert_eq!(3, fetch_len(&storage, &topition).await?);

    // a maintenance pass an hour into the future ages every batch out
    let later = SystemTime::now()
        .checked_add(retention + Duration::from_mins(30))
        .expect("an hour ahead");
    storage.maintain(later).await?;

    // Offset 0 is below the start of a log that is now empty, so it is out of
    // range rather than empty (#337). Answering empty here is what left consumers
    // polling a stranded partition forever: 77 of them on one connector, which
    // delivered nothing for days because `poll()` covers a whole assignment.
    //
    // Safe to say so because #299 already reports `log_start == log_end`, so a
    // consumer resetting to earliest lands at the end and skips nothing available.
    assert!(matches!(
        fetch_len(&storage, &topition).await,
        Err(Error::Storage(tansu_storage::Error::Api(
            ErrorCode::OffsetOutOfRange
        )))
    ));

    // the partition reports an empty log — its start IS its end (#290). This
    // used to assert 0, which said the log began three records before it ended
    // while holding none of them: three records of lag no consumer could ever
    // retire, and the same false statement a partition whose segments were lost
    // makes. An empty log and a damaged one were indistinguishable through
    // ordinary metadata precisely because of it.
    let stage = storage.offset_stage(&topition).await?;
    assert_eq!(stage.high_watermark(), stage.log_start());
    assert_eq!(3, stage.log_start());

    Ok(())
}

#[tokio::test]
async fn default_retention_keeps_recent_records() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = memory_storage().await?;

    // cleanup.policy=delete with no retention.ms falls back to the 7 day default
    create_topic(
        &storage,
        "defaulted",
        vec![
            CreatableTopicConfig::default()
                .name("cleanup.policy".into())
                .value(Some("delete".into())),
        ],
    )
    .await?;

    let topition = Topition::new("defaulted", 0);

    produce_three(&storage, &topition).await?;

    assert_eq!(3, fetch_len(&storage, &topition).await?);

    // an hour into the future is still well inside the 7 day window
    let later = SystemTime::now()
        .checked_add(Duration::from_hours(1))
        .expect("an hour ahead");
    storage.maintain(later).await?;

    assert_eq!(3, fetch_len(&storage, &topition).await?);

    // eight days into the future is past the default retention
    let later = SystemTime::now()
        .checked_add(Duration::from_hours(8 * 24))
        .expect("eight days ahead");
    storage.maintain(later).await?;

    // Offset 0 is below the start of a log that is now empty, so it is out of
    // range rather than empty (#337). Answering empty here is what left consumers
    // polling a stranded partition forever: 77 of them on one connector, which
    // delivered nothing for days because `poll()` covers a whole assignment.
    //
    // Safe to say so because #299 already reports `log_start == log_end`, so a
    // consumer resetting to earliest lands at the end and skips nothing available.
    assert!(matches!(
        fetch_len(&storage, &topition).await,
        Err(Error::Storage(tansu_storage::Error::Api(
            ErrorCode::OffsetOutOfRange
        )))
    ));

    Ok(())
}

/// An absent `cleanup.policy` is Kafka's default — `delete` at the default
/// retention — so maintenance expires the records (#177).
///
/// This test previously asserted the opposite, that a policy-less topic is
/// retained forever. That promise was only ever kept by the legacy
/// `records/` path: `segment_retention_thresholds` has always applied the
/// 7-day default to a policy-less topic, so on any segment-backed deployment
/// the data expired regardless. #177 makes segments the only layout, so the
/// two readings were reconciled in favour of the segment one, which is also
/// the Kafka-conformant one. Retain-forever is still expressible, and now has
/// exactly one spelling: `retention.ms=-1`, covered by
/// `retention_ms_minus_one_retains_forever` below.
#[tokio::test]
async fn without_cleanup_policy_kafka_defaults_apply() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = memory_storage().await?;

    // No cleanup.policy at all: the Kafka default (`delete`) applies.
    create_topic(&storage, "no-policy", vec![]).await?;

    let topition = Topition::new("no-policy", 0);

    produce_three(&storage, &topition).await?;

    assert_eq!(3, fetch_len(&storage, &topition).await?);

    let later = SystemTime::now()
        .checked_add(Duration::from_hours(100 * 24))
        .expect("a hundred days ahead");
    storage.maintain(later).await?;

    // Expired to nothing, so offset 0 is below the start of an empty log and is
    // out of range rather than empty (#337).
    assert!(
        matches!(
            fetch_len(&storage, &topition).await,
            Err(Error::Storage(tansu_storage::Error::Api(
                ErrorCode::OffsetOutOfRange
            )))
        ),
        "a policy-less topic expires at the default retention, as Kafka does",
    );

    Ok(())
}

/// `retention.ms=-1` is the one spelling of retain-forever, and it holds even
/// with no `cleanup.policy` (where the default `delete` would otherwise apply).
#[tokio::test]
async fn retention_ms_minus_one_retains_forever() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = memory_storage().await?;

    create_topic(
        &storage,
        "forever",
        vec![
            CreatableTopicConfig::default()
                .name("retention.ms".into())
                .value(Some("-1".into())),
        ],
    )
    .await?;

    let topition = Topition::new("forever", 0);

    produce_three(&storage, &topition).await?;

    assert_eq!(3, fetch_len(&storage, &topition).await?);

    let later = SystemTime::now()
        .checked_add(Duration::from_hours(100 * 24))
        .expect("a hundred days ahead");
    storage.maintain(later).await?;

    assert_eq!(3, fetch_len(&storage, &topition).await?);

    Ok(())
}
