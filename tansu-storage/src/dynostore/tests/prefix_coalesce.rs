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

//! Prefix-coalesced "virtual topics" produce (#56/#57): with prefix mode on,
//! batches produced within a linger window across *every topic under a
//! connector prefix* are flushed as one shared segment object — collapsing PUTs
//! from ~`(topics × flushes)` to ~`(connectors × flushes)` — while each
//! `(topic, partition)` sub-stream keeps its own independent offset sequence.

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use tansu_sans_io::{
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage, Topition,
    dynostore::{DynoStore, SegmentFooter},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "org.env.conn";

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

/// The segment objects under a connector prefix.
async fn segments(bucket: &InMemory) -> Vec<Path> {
    let listing = Path::from(format!("clusters/{CLUSTER}/prefixes/{PREFIX}/segments/"));
    bucket
        .list(Some(&listing))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .expect("list segments")
}

async fn footer_of(bucket: &InMemory, store: &DynoStore, location: &Path) -> SegmentFooter {
    let bytes = bucket
        .get(location)
        .await
        .expect("get segment")
        .bytes()
        .await
        .expect("segment bytes");

    store
        .decode_segment_footer(&bytes)
        .expect("decode footer")
        .expect("segment carries a footer")
}

/// Batches produced across two topics of the same prefix in one window land in
/// one shared segment, and each topic gets its own offset sequence from 0.
#[tokio::test]
async fn one_window_across_topics_is_one_segment() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);

    let topic_a = "org.env.conn.tab_a";
    let topic_b = "org.env.conn.tab_b";
    create_topic(&store, topic_a).await?;
    create_topic(&store, topic_b).await?;

    let a = Topition::new(topic_a, 0);
    let b = Topition::new(topic_b, 0);

    // Two 2-record batches per topic, produced concurrently so they share one
    // linger window and flush as a single segment.
    let (a0, a1, b0, b1) = tokio::join!(
        store.produce(None, &a, batch(2)?),
        store.produce(None, &a, batch(2)?),
        store.produce(None, &b, batch(2)?),
        store.produce(None, &b, batch(2)?),
    );

    // Each topic's two batches occupy offsets {0, 2}, independently.
    let mut a_offsets = vec![a0?, a1?];
    a_offsets.sort_unstable();
    assert_eq!(vec![0, 2], a_offsets);

    let mut b_offsets = vec![b0?, b1?];
    b_offsets.sort_unstable();
    assert_eq!(vec![0, 2], b_offsets);

    // One PUT for the whole window: a single shared segment, no per-topic
    // `records/` objects.
    let segments = segments(&bucket).await;
    assert_eq!(1, segments.len(), "expected exactly one segment PUT");

    let footer = footer_of(&bucket, &store, &segments[0]).await;
    assert_eq!(2, footer.entries.len());

    let ea = footer.get(topic_a, 0).expect("tab_a entry");
    assert_eq!(0, ea.base_offset);
    assert_eq!(4, ea.record_count);

    let eb = footer.get(topic_b, 0).expect("tab_b entry");
    assert_eq!(0, eb.base_offset);
    assert_eq!(4, eb.record_count);

    Ok(())
}

/// A second window continues each sub-stream's offsets past the first segment,
/// and writes a second segment (monotonic sequence).
#[tokio::test]
async fn a_later_window_continues_offsets_in_a_new_segment() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);

    let topic_a = "org.env.conn.tab_a";
    create_topic(&store, topic_a).await?;
    let a = Topition::new(topic_a, 0);

    // First window: one 3-record batch -> offset 0.
    let first = store.produce(None, &a, batch(3)?).await?;
    assert_eq!(0, first);

    // Second window: continues at 3.
    let second = store.produce(None, &a, batch(2)?).await?;
    assert_eq!(3, second);

    let segments = segments(&bucket).await;
    assert_eq!(2, segments.len(), "one segment per window");

    // Sequence is monotonic and zero-padded.
    let names: Vec<String> = segments
        .iter()
        .map(|p| p.parts().next_back().unwrap().as_ref().to_owned())
        .collect();
    assert!(names.contains(&"00000000000000000000.seg".to_owned()));
    assert!(names.contains(&"00000000000000000001.seg".to_owned()));

    Ok(())
}

/// With prefix mode off, produce is byte-for-byte the legacy per-partition
/// layout: no segment objects, records land under `topics/.../records/`.
#[tokio::test]
async fn prefix_mode_off_uses_legacy_layout() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic_a = "org.env.conn.tab_a";
    create_topic(&store, topic_a).await?;
    let a = Topition::new(topic_a, 0);

    let offset = store.produce(None, &a, batch(2)?).await?;
    assert_eq!(0, offset);

    assert!(
        segments(&bucket).await.is_empty(),
        "no segments in legacy mode"
    );

    let records = Path::from(format!(
        "clusters/{CLUSTER}/topics/{topic_a}/partitions/{:0>10}/records/",
        0
    ));
    let count = bucket
        .list(Some(&records))
        .try_collect::<Vec<_>>()
        .await
        .expect("list records")
        .len();
    assert_eq!(1, count, "one legacy batch object");

    Ok(())
}

/// After a cold restart — a fresh process on the same bucket, so the in-memory
/// offset counter is empty — each sub-stream resumes at the exact next offset,
/// recovered from the tail segment footer (#58): no gap, no reuse.
#[tokio::test]
async fn cold_restart_recovers_offsets_from_the_footer() -> Result<(), Error> {
    let bucket = InMemory::new();
    let topic_a = "org.env.conn.tab_a";
    let a = Topition::new(topic_a, 0);

    // First process: two windows -> offsets 0 then 3 (5 records total).
    {
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
        create_topic(&store, topic_a).await?;
        assert_eq!(0, store.produce(None, &a, batch(3)?).await?);
        assert_eq!(3, store.produce(None, &a, batch(2)?).await?);
    }

    // A fresh process on the same bucket: empty in-memory counters. The next
    // produce must continue at 5, recovered from the tail segment footer, and
    // land in a third segment (sequence recovered from the tail listing).
    let restarted = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    let resumed = restarted.produce(None, &a, batch(1)?).await?;
    assert_eq!(5, resumed, "resume past the footer end offset, no reuse");
    assert_eq!(3, segments(&bucket).await.len());

    Ok(())
}
