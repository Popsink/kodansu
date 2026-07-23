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

//! Wake-on-write long-poll and fetch-triggered flush (phase 2 of
//! `docs/design-recent-write-cache.md`): `await_produce` parks until a watched
//! topition's high-watermark hint advances — woken by the produce path's
//! notify instead of a polling sleep — and a parked waiter collapses a wide
//! `coalesce_linger` down to the fetch-flush floor, so consumer latency is
//! demand-bounded while unconsumed buffers keep the wide window. The timing
//! assertions are deliberately coarse (an order of magnitude apart from the
//! configured waits) so scheduler jitter cannot flake them.

use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage, Topition,
    dynostore::{CoalesceTuning, DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// A non-idempotent batch of `records` records (occupies `records` offsets).
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

async fn create_topic(storage: &DynoStore, name: &str) -> Result<()> {
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

    Ok(())
}

async fn fetch_from(store: &DynoStore, tp: &Topition, offset: i64) -> Result<Vec<deflated::Batch>> {
    store
        .fetch(
            tp,
            offset,
            0,
            100_000,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(200),
        )
        .await
}

#[tokio::test]
async fn await_wakes_on_produce() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());
    let topic = "wake";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    let producer = {
        let store = store.clone();
        let tp = tp.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            store.produce(None, &tp, batch(2)?).await
        })
    };

    // Parked with a 30s budget, the waiter must return on the produce notify
    // (~100ms in), not the deadline.
    let parked_at = Instant::now();
    store
        .await_produce(&[(tp.clone(), 0)], Duration::from_secs(30))
        .await?;
    assert!(parked_at.elapsed() < Duration::from_secs(3));

    assert_eq!(
        0,
        producer
            .await
            .map_err(|error| Error::Message(error.to_string()))??
    );
    assert_eq!(1, fetch_from(&store, &tp, 0).await?.len());

    Ok(())
}

#[tokio::test]
async fn await_returns_immediately_when_already_ahead() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());
    let topic = "ahead";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &tp, batch(2)?).await?);

    // The records the caller has not read yet are already durable: the hint
    // check short-circuits the park entirely.
    let parked_at = Instant::now();
    store
        .await_produce(&[(tp.clone(), 0)], Duration::from_secs(30))
        .await?;
    assert!(parked_at.elapsed() < Duration::from_secs(3));

    Ok(())
}

#[tokio::test]
async fn await_times_out_on_a_quiet_topic() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());
    let topic = "quiet";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    let parked_at = Instant::now();
    store
        .await_produce(&[(tp.clone(), 0)], Duration::from_millis(250))
        .await?;

    let elapsed = parked_at.elapsed();
    assert!(elapsed >= Duration::from_millis(200), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");

    Ok(())
}

#[tokio::test]
async fn parked_fetch_collapses_a_wide_linger() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // A 30s linger: without the fetch-triggered flush, the parked produce
    // below would not resolve (and the waiter not wake) within this test.
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new())
        .produce_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            coalesce_linger: Some(Duration::from_secs(30)),
            ..Default::default()
        });

    let topic = "collapsed";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    let producer = {
        let store = store.clone();
        let tp = tp.clone();
        tokio::spawn(async move { store.produce(None, &tp, batch(2)?).await })
    };
    // Let the produce park in the coalesce buffer before the fetch waits.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let parked_at = Instant::now();
    store
        .await_produce(&[(tp.clone(), 0)], Duration::from_secs(30))
        .await?;
    assert!(parked_at.elapsed() < Duration::from_secs(5));

    assert_eq!(
        0,
        producer
            .await
            .map_err(|error| Error::Message(error.to_string()))??
    );
    assert_eq!(1, fetch_from(&store, &tp, 0).await?.len());

    Ok(())
}

#[tokio::test]
async fn parked_fetch_collapses_a_wide_prefix_linger() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new())
        .prefix_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            coalesce_linger: Some(Duration::from_secs(30)),
            ..Default::default()
        });

    let topic = "org.env.conn.schema.table";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    let producer = {
        let store = store.clone();
        let tp = tp.clone();
        tokio::spawn(async move { store.produce(None, &tp, batch(2)?).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let parked_at = Instant::now();
    store
        .await_produce(&[(tp.clone(), 0)], Duration::from_secs(30))
        .await?;
    assert!(parked_at.elapsed() < Duration::from_secs(5));

    assert_eq!(
        0,
        producer
            .await
            .map_err(|error| Error::Message(error.to_string()))??
    );
    assert_eq!(1, fetch_from(&store, &tp, 0).await?.len());

    Ok(())
}
