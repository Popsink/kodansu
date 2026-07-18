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

//! Server-side produce coalescing (#50): with the flag on, batches produced
//! within a linger window are buffered and flushed as one `records/` object,
//! cutting the PUT (and matching fetch GET) count, while offsets stay
//! contiguous and a fetch into the middle of a coalesced object still returns
//! it. With the flag off (the default) every batch is its own object.

use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{ObjectStore, memory::InMemory, path::Path};
use tansu_sans_io::{
    IsolationLevel, create_topics_request::CreatableTopic, record::Record, record::deflated,
    record::inflated,
};

use crate::{
    Error, Result, Storage, Topition, dynostore::CoalesceTuning, dynostore::DynoStore,
    dynostore::tests::init_tracing,
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

/// Number of `records/` objects for a partition-0 topic in the raw bucket.
async fn object_count(bucket: &InMemory, topic: &str) -> usize {
    let prefix = Path::from(format!(
        "clusters/{CLUSTER}/topics/{topic}/partitions/{:0>10}/records/",
        0
    ));

    bucket
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .expect("list")
        .len()
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
async fn coalesces_a_window_into_one_object_with_contiguous_offsets() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).produce_coalesce(true);

    let topic = "coalesced";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    // Produce three 2-record batches concurrently so they land in one linger
    // window and flush together (the first enqueue starts the timer; the others
    // join the same buffer, and all three park until the single flush resolves).
    let (a, b, c) = tokio::join!(
        store.produce(None, &tp, batch(2)?),
        store.produce(None, &tp, batch(2)?),
        store.produce(None, &tp, batch(2)?),
    );

    let mut offsets = vec![a?, b?, c?];
    offsets.sort_unstable();
    assert_eq!(vec![0, 2, 4], offsets);

    // One object holds all three batches.
    assert_eq!(1, object_count(&bucket, topic).await);

    // ...and a fetch reads every batch back, offsets tiling [0, 6).
    let batches = fetch_from(&store, &tp, 0).await?;
    assert_eq!(
        vec![0, 2, 4],
        batches.iter().map(|b| b.base_offset).collect::<Vec<_>>()
    );

    Ok(())
}

#[tokio::test]
async fn fetch_into_the_middle_of_a_coalesced_object_returns_it() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).produce_coalesce(true);

    let topic = "mid-frame";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    let (a, b, c) = tokio::join!(
        store.produce(None, &tp, batch(2)?),
        store.produce(None, &tp, batch(2)?),
        store.produce(None, &tp, batch(2)?),
    );
    _ = (a?, b?, c?);
    assert_eq!(1, object_count(&bucket, topic).await);

    // Offset 3 sits inside the single coalesced object [0, 6). The seek must not
    // skip it: the whole containing object is returned (base offsets 0, 2, 4),
    // so a consumer resuming at a sub-batch boundary loses nothing.
    let batches = fetch_from(&store, &tp, 3).await?;
    assert_eq!(
        vec![0, 2, 4],
        batches.iter().map(|b| b.base_offset).collect::<Vec<_>>()
    );

    Ok(())
}

#[tokio::test]
async fn flag_off_writes_one_object_per_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    // Coalescing is opt-in: the default store keeps one object per batch.
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "uncoalesced";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &tp, batch(2)?).await?);
    assert_eq!(2, store.produce(None, &tp, batch(2)?).await?);
    assert_eq!(4, store.produce(None, &tp, batch(2)?).await?);

    assert_eq!(3, object_count(&bucket, topic).await);

    Ok(())
}

#[tokio::test]
async fn tuned_batch_count_flushes_before_the_linger_elapses() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    // Linger set an hour out: the ONLY way these produces can be acked within the
    // test is the tuned batch-count trigger firing on the third enqueue. If the
    // `coalesce_batches` override were ignored (default 64), the join! below would
    // park on the linger and never complete — so a passing test proves the
    // storage-URL threshold is actually applied (#54).
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .produce_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            coalesce_linger: Some(Duration::from_secs(3600)),
            coalesce_batches: Some(3),
            ..Default::default()
        });

    let topic = "tuned-count";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    let (a, b, c) = tokio::join!(
        store.produce(None, &tp, batch(2)?),
        store.produce(None, &tp, batch(2)?),
        store.produce(None, &tp, batch(2)?),
    );

    let mut offsets = vec![a?, b?, c?];
    offsets.sort_unstable();
    assert_eq!(vec![0, 2, 4], offsets);

    // The three batches flushed as one object on the count trigger.
    assert_eq!(1, object_count(&bucket, topic).await);

    Ok(())
}

#[tokio::test]
async fn coalesced_offsets_have_no_gaps_across_many_producers() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).produce_coalesce(true);

    let topic = "tiling";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    // Five concurrent batches of varying size flushed as one run.
    let sizes = [1usize, 3, 2, 4, 1];
    let (a, b, c, d, e) = tokio::join!(
        store.produce(None, &tp, batch(sizes[0])?),
        store.produce(None, &tp, batch(sizes[1])?),
        store.produce(None, &tp, batch(sizes[2])?),
        store.produce(None, &tp, batch(sizes[3])?),
        store.produce(None, &tp, batch(sizes[4])?),
    );

    // Every produce is acked with a distinct base offset, and they coalesce into
    // one object.
    let mut assigned = [a?, b?, c?, d?, e?];
    assigned.sort_unstable();
    assert_eq!(0, assigned[0]);
    assert_eq!(1, object_count(&bucket, topic).await);

    // The fetched batches tile [0, total) exactly — contiguous, no gaps, no
    // overlaps — regardless of the order they flushed in.
    let total: i64 = sizes.iter().map(|size| *size as i64).sum();
    let batches = fetch_from(&store, &tp, 0).await?;

    let mut next = 0i64;
    for batch in &batches {
        assert_eq!(next, batch.base_offset);
        next += batch.last_offset_delta as i64 + 1;
    }
    assert_eq!(total, next);

    Ok(())
}
