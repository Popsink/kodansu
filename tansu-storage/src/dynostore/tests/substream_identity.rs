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

//! Migrating a bucket onto id-keyed sub-streams (#442).
//!
//! `tests/topic_recreation.rs` pins the *behaviour* — a recreated topic starts at
//! 0 under `segment_format=4`, and continues its predecessor's offsets without
//! it. What it cannot reach is the state in between, because on `memory://`
//! every `StorageContainer::build()` gets its own `InMemory`: two writer regimes
//! over **one** bucket. That is the migration, and it is the half that cannot be
//! papered over.
//!
//! Records written before the flip live in segments that carry no `topic_id`, so
//! a broker that resolved every topic by id after it would read every
//! pre-existing topic as empty — a fleet-wide, silent read outage on the way *in*
//! to the fix. The identity is therefore pinned per topic at creation
//! (`topic-routing/{name}.json`), never derived from the writer regime, so a
//! topic's answer does not change under a flag it was not created with.

use std::time::Duration;

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_sans_io::{
    IsolationLevel, ListOffset,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    ListOffsetResponse, Result, Storage, TopicId, Topition,
    dynostore::{
        CoalesceTuning, DynoStore, SEGMENT_FORMAT_VERSION_V4, Substream, tests::init_tracing,
    },
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const TOPIC: &str = "org.env.conn.tab";

/// A store over `bucket`, writing `version` footers.
fn store(bucket: &InMemory, version: u16) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        segment_format_version: Some(version),
        ..Default::default()
    })
}

fn batch(value: &'static [u8]) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(value))))
        .last_offset_delta(0)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create(storage: &DynoStore) -> Result<()> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some(vec![]))
                .configs(Some(vec![])),
            false,
        )
        .await?;

    Ok(())
}

/// `(earliest, latest)`.
async fn watermarks(storage: &DynoStore, topition: &Topition) -> Result<(i64, i64)> {
    let answers = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[
                (topition.clone(), ListOffset::Earliest),
                (topition.clone(), ListOffset::Latest),
            ],
        )
        .await?;

    let offset = |response: &ListOffsetResponse| response.offset().unwrap_or(-1);

    Ok((offset(&answers[0].1), offset(&answers[1].1)))
}

/// Every record value the log serves from `offset`.
async fn values(storage: &DynoStore, topition: &Topition, offset: i64) -> Result<Vec<String>> {
    let batches = storage
        .fetch(
            topition,
            offset,
            1,
            1024 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(100),
        )
        .await?;

    let mut values = vec![];

    for batch in batches {
        for record in inflated::Batch::try_from(batch)?.records {
            values.push(String::from_utf8_lossy(&record.value.unwrap_or_default()).into_owned());
        }
    }

    Ok(values)
}

/// A topic created before the flip keeps its records, and keeps taking writes,
/// once the writer moves to v4 — name-keyed for its whole life.
#[tokio::test]
async fn a_pre_flip_topic_survives_the_writer_moving_to_v4() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let topition = Topition::new(TOPIC, 0);

    {
        let before = store(&bucket, crate::dynostore::SEGMENT_FORMAT_VERSION_V3);

        create(&before).await?;
        _ = before.produce(None, &topition, batch(b"old-0")?).await?;
        _ = before.produce(None, &topition, batch(b"old-1")?).await?;

        assert_eq!((0, 2), watermarks(&before, &topition).await?);
    }

    // The same bucket, a v4 writer, and a store with none of the first one's
    // caches — which is what a rolled pod is.
    let after = store(&bucket, SEGMENT_FORMAT_VERSION_V4);

    assert_eq!(
        (0, 2),
        watermarks(&after, &topition).await?,
        "a pre-flip topic must not read as empty once the writer moves to v4"
    );
    assert_eq!(
        vec!["old-0".to_owned(), "old-1".to_owned()],
        values(&after, &topition, 0).await?
    );

    // It keeps taking writes, still keyed by name — into v4 segments, whose
    // entries carry the nil uuid for it.
    _ = after.produce(None, &topition, batch(b"new-0")?).await?;

    assert_eq!((0, 3), watermarks(&after, &topition).await?);
    assert_eq!(
        vec!["old-0".to_owned(), "old-1".to_owned(), "new-0".to_owned()],
        values(&after, &topition, 0).await?
    );

    Ok(())
}

/// A topic created **after** the flip is pinned to its id, and the two
/// incarnations of one name coexist in the same prefix without meeting.
///
/// The predecessor's slices are still physically in the shared segments — that is
/// the constraint the whole design is built around — so this asserts the thing
/// that makes restarting at 0 safe rather than reckless: the successor's fenced
/// view holds only its own entries, at its own offsets, with the predecessor's
/// nowhere in it.
#[tokio::test]
async fn two_incarnations_share_a_prefix_without_meeting() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let storage = store(&bucket, SEGMENT_FORMAT_VERSION_V4);
    let topition = Topition::new(TOPIC, 0);

    let first = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some(vec![]))
                .configs(Some(vec![])),
            false,
        )
        .await?;

    _ = storage.produce(None, &topition, batch(b"old-0")?).await?;
    _ = storage.produce(None, &topition, batch(b"old-1")?).await?;
    _ = storage.produce(None, &topition, batch(b"old-2")?).await?;

    assert_eq!((0, 3), watermarks(&storage, &topition).await?);

    _ = storage.delete_topic(&TopicId::Name(TOPIC.into())).await?;
    create(&storage).await?;
    _ = storage.produce(None, &topition, batch(b"new-0")?).await?;

    assert_eq!(
        (0, 1),
        watermarks(&storage, &topition).await?,
        "a recreated topic is a brand-new log"
    );
    assert_eq!(
        vec!["new-0".to_owned()],
        values(&storage, &topition, 0).await?
    );

    // The predecessor's records are still there — the segments were never
    // rewritten — and reachable by asking for its identity, which nothing on the
    // Kafka surface can do. That is what "unreachable by construction" means
    // here, as opposed to "hidden by a floor".
    let prefix = storage.routed_prefix_of(&topition).await?;
    storage.refresh_prefix_index_forced(&prefix).await?;

    let predecessor = storage.valid_substream_segments(&prefix, &Substream::Id(first), 0)?;

    assert_eq!(
        3,
        predecessor
            .iter()
            .map(|fenced| fenced.entry.record_count)
            .sum::<i64>(),
        "the predecessor's slices must still be in the shared segments"
    );

    assert_eq!(
        0,
        storage
            .valid_substream_segments(&prefix, &Substream::Name(TOPIC.into()), 0)?
            .len(),
        "nothing may answer to the name once the topic is keyed by id"
    );

    Ok(())
}

/// Deleting an id-keyed topic clears the truncation floor its successor would
/// otherwise inherit.
///
/// The floor (#246) exists to hide a predecessor's slices from a same-named
/// successor that would otherwise find them. Keyed by id there is nothing to
/// hide — and keeping the floor would be worse than pointless, because it clamps
/// a genuinely empty log to a dead incarnation's end. That is the reported defect
/// arriving by another route, so it is asserted directly rather than only through
/// the watermark it would move.
#[tokio::test]
async fn recreating_an_id_keyed_topic_clears_the_inherited_floor() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let storage = store(&bucket, SEGMENT_FORMAT_VERSION_V4);
    let topition = Topition::new(TOPIC, 0);

    create(&storage).await?;
    _ = storage.produce(None, &topition, batch(b"old-0")?).await?;
    _ = storage.produce(None, &topition, batch(b"old-1")?).await?;

    _ = storage.delete_topic(&TopicId::Name(TOPIC.into())).await?;

    // `delete_topic` truncates every partition to its log end, as it does for a
    // name-keyed topic. Read through a store with no caches, so this is the
    // durable `watermark.json` and not a memo.
    assert_eq!(
        2,
        store(&bucket, SEGMENT_FORMAT_VERSION_V4)
            .truncate_floor(&topition)
            .await?
    );

    create(&storage).await?;

    assert_eq!(
        0,
        store(&bucket, SEGMENT_FORMAT_VERSION_V4)
            .truncate_floor(&topition)
            .await?,
        "an id-keyed successor must not inherit its predecessor's floor"
    );

    Ok(())
}

/// The v3 default is unchanged: a topic created without the flag is name-keyed,
/// and its footer entries carry no id at all.
#[tokio::test]
async fn the_default_writer_regime_pins_nothing() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let storage = store(&bucket, crate::dynostore::SEGMENT_FORMAT_VERSION_V3);
    let topition = Topition::new(TOPIC, 0);

    create(&storage).await?;
    _ = storage.produce(None, &topition, batch(b"old-0")?).await?;

    let (prefix, substream) = storage.routed_substream_of(&topition).await?;

    assert_eq!(Substream::Name(TOPIC.to_owned()), substream);

    storage.refresh_prefix_index_forced(&prefix).await?;

    assert_eq!(
        1,
        storage
            .valid_substream_segments(&prefix, &substream, 0)?
            .len()
    );

    Ok(())
}
