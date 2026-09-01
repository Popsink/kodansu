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

//! The `watermark_hint_ttl` deployment knob (#500): the freshness window of the
//! in-memory high-watermark view is per-store configuration, not a compile-time
//! constant, so a fleet whose watermark readers are periodic diagnostics can
//! align the window with their cadence.

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_sans_io::{
    IsolationLevel, ListOffset, create_topics_request::CreatableTopic, record::Record,
    record::deflated, record::inflated,
};

use crate::{Error, Result, Storage, Topition};

use super::super::{CoalesceTuning, DynoStore, tests::init_tracing};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

fn batch(records: usize) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder();

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("record-{i}").as_bytes(),
        ))));
    }

    builder
        .last_offset_delta(records as i32 - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(storage: &DynoStore, name: &str, partitions: i32) -> Result<()> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;
    Ok(())
}

async fn latest(storage: &DynoStore, topition: &Topition) -> Result<Option<i64>> {
    storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await
        .map(|responses| responses.first().and_then(|(_, response)| response.offset))
}

/// Rewind the whole in-memory watermark view — `topition`'s hint and every
/// prefix-index refresh stamp — by `age`, standing in for wall-clock passage
/// without the test sleeping it. Both layers must age together: a fresh prefix
/// index would answer for a stale hint and hide the window under test.
fn age_watermark_view(storage: &DynoStore, topition: &Topition, age: Duration) -> Result<()> {
    let then = SystemTime::now() - age;

    storage
        .next_offsets
        .lock()
        .map_err(Into::<Error>::into)
        .map(|mut locked| {
            if let Some(hint) = locked.get_mut(topition) {
                hint.listed_at = hint.listed_at.map(|_| then);
            }
        })?;

    storage
        .prefix_index
        .lock()
        .map_err(Into::<Error>::into)
        .map(|mut locked| {
            for entry in locked.values_mut() {
                entry.refreshed_at = entry.refreshed_at.map(|_| then);
            }
        })
}

/// A zero TTL declares every hint stale, so each `ListOffsets` re-derives the
/// tail and a peer's produce is visible immediately. Under the compile-time
/// default this same immediate re-read is served from the seconds-old hint —
/// which is what proves the knob reached [`DynoStore::cached_high_fresh`]
/// rather than merely parsing.
#[tokio::test]
async fn zero_ttl_reads_through_to_a_peers_produce() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let producer = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let reader = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        watermark_hint_ttl: Some(Duration::ZERO),
        ..CoalesceTuning::default()
    });

    let topic = "zero-ttl";
    create_topic(&producer, topic, 1).await?;
    let topition = Topition::new(topic, 0);

    assert_eq!(0, producer.produce(None, &topition, batch(1)?).await?);
    assert_eq!(Some(1), latest(&reader, &topition).await?);

    // The reader's hint is milliseconds old; only a zero TTL refuses to serve it.
    assert_eq!(1, producer.produce(None, &topition, batch(1)?).await?);
    assert_eq!(Some(2), latest(&reader, &topition).await?);

    Ok(())
}

/// A widened TTL keeps serving the hint at an age the default would have
/// re-listed at: a reader whose hint is ten minutes old still answers from
/// memory under a one-hour window, under-reporting a peer's produce — the
/// documented bounded-staleness trade the knob exposes.
#[tokio::test]
async fn widened_ttl_serves_the_aged_hint_from_memory() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let producer = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let reader = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        watermark_hint_ttl: Some(Duration::from_secs(3600)),
        ..CoalesceTuning::default()
    });

    let topic = "wide-ttl";
    create_topic(&producer, topic, 1).await?;
    let topition = Topition::new(topic, 0);

    assert_eq!(0, producer.produce(None, &topition, batch(1)?).await?);
    assert_eq!(Some(1), latest(&reader, &topition).await?);

    // Ten minutes pass (the default 5s window is long spent), and a peer produces.
    age_watermark_view(&reader, &topition, Duration::from_secs(600))?;
    assert_eq!(1, producer.produce(None, &topition, batch(1)?).await?);

    // Still inside the widened window: served from memory, peer produce unseen.
    assert_eq!(Some(1), latest(&reader, &topition).await?);

    Ok(())
}
