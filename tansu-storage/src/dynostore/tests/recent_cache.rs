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

//! Recent-write cache (`docs/design-recent-write-cache.md`): with
//! `recent_cache_bytes` set, the bytes a produce just wrote are served to a
//! fetch from memory instead of an object-store GET. The tests prove byte
//! provenance by overwriting the durable objects with garbage after the
//! produce: any real data a fetch still returns can only have come from the
//! cache, while a store with the cache disabled (the default) reads the
//! garbage and returns nothing.

use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
use tansu_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage, TopicId, Topition,
    dynostore::{CoalesceTuning, DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// A store with the recent-write cache enabled at a generous budget.
fn cached(store: DynoStore) -> DynoStore {
    store.coalesce_tuning(CoalesceTuning {
        recent_cache_bytes: Some(1 << 20),
        ..Default::default()
    })
}

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

/// Overwrite every object under `prefix` in the raw bucket with bytes that
/// decode to no batches, returning how many were corrupted. The names (and so
/// listings) are unchanged — only the payloads a GET would return are gone.
async fn corrupt_all(bucket: &InMemory, prefix: &Path) -> usize {
    let locations: Vec<Path> = bucket
        .list(Some(prefix))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .expect("list");

    for location in &locations {
        _ = bucket
            .put(location, PutPayload::from_static(b"garbage"))
            .await
            .expect("put");
    }

    locations.len()
}

fn records_prefix(topic: &str) -> Path {
    Path::from(format!(
        "clusters/{CLUSTER}/topics/{topic}/partitions/{:0>10}/records/",
        0
    ))
}

#[tokio::test]
async fn disabled_by_default_data_reads_hit_the_bucket() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    // No tuning: recent_cache_bytes defaults to 0 (disabled).
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "uncached";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &tp, batch(2)?).await?);
    assert_eq!(1, corrupt_all(&bucket, &records_prefix(topic)).await);

    // The fetch reads the garbage from the bucket and decodes no batches —
    // proving the disabled store pays the GET (shipped behaviour, unchanged).
    assert!(fetch_from(&store, &tp, 0).await?.is_empty());

    Ok(())
}

#[tokio::test]
async fn serves_recently_produced_bytes_from_memory() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = cached(DynoStore::new(CLUSTER, NODE, bucket.clone()));

    let topic = "tail";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &tp, batch(2)?).await?);
    assert_eq!(2, store.produce(None, &tp, batch(2)?).await?);
    assert_eq!(4, store.produce(None, &tp, batch(2)?).await?);

    // Every durable object is garbage now; only the cache holds real bytes.
    assert_eq!(3, corrupt_all(&bucket, &records_prefix(topic)).await);

    let batches = fetch_from(&store, &tp, 0).await?;
    assert_eq!(
        vec![0, 2, 4],
        batches.iter().map(|b| b.base_offset).collect::<Vec<_>>()
    );

    Ok(())
}

#[tokio::test]
async fn coalesced_flush_populates_the_cache() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = cached(DynoStore::new(CLUSTER, NODE, bucket.clone()).produce_coalesce(true));

    let topic = "coalesced-cached";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    // One linger window: three batches flush as one object (#50).
    let (a, b, c) = tokio::join!(
        store.produce(None, &tp, batch(2)?),
        store.produce(None, &tp, batch(2)?),
        store.produce(None, &tp, batch(2)?),
    );
    _ = (a?, b?, c?);

    assert_eq!(1, corrupt_all(&bucket, &records_prefix(topic)).await);

    // A mid-frame fetch is served whole from the cached coalesced object.
    let batches = fetch_from(&store, &tp, 3).await?;
    assert_eq!(
        vec![0, 2, 4],
        batches.iter().map(|b| b.base_offset).collect::<Vec<_>>()
    );

    Ok(())
}

#[tokio::test]
async fn prefix_segment_sub_stream_served_from_memory() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = cached(DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true));

    let topic = "org.env.conn.schema.table";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &tp, batch(2)?).await?);
    assert_eq!(2, store.produce(None, &tp, batch(2)?).await?);

    let segments = Path::from(format!(
        "clusters/{CLUSTER}/prefixes/org.env.conn/segments/"
    ));
    assert_eq!(2, corrupt_all(&bucket, &segments).await);

    // The footer index (in memory) locates the sub-streams and the cache
    // serves each byte span — no ranged GET touches the corrupted objects.
    let batches = fetch_from(&store, &tp, 0).await?;
    assert_eq!(
        vec![0, 2],
        batches.iter().map(|b| b.base_offset).collect::<Vec<_>>()
    );

    Ok(())
}

#[tokio::test]
async fn topic_delete_purges_the_cached_names() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = cached(DynoStore::new(CLUSTER, NODE, bucket.clone()));

    let topic = "reborn";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &tp, batch(2)?).await?);
    assert!(
        store
            .recent_writes
            .lock()
            .map(|cache| cache.len() == 1)
            .map_err(Into::<Error>::into)?
    );

    // Deleting the topic frees its `records/` names for a re-created topic
    // (offsets restart at 0) — the cached bytes must go with them.
    assert_eq!(
        tansu_sans_io::ErrorCode::None,
        store.delete_topic(&TopicId::Name(topic.into())).await?
    );
    assert!(
        store
            .recent_writes
            .lock()
            .map(|cache| cache.len() == 0)
            .map_err(Into::<Error>::into)?
    );

    Ok(())
}
