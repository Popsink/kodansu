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

use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use tansu_sans_io::{
    IsolationLevel, ListOffset,
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

/// A `records`-record batch stamped with `max_timestamp` (for retention #61).
fn batch_at(records: usize, max_timestamp: i64) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder();

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("record-{i}").as_bytes(),
        ))));
    }

    builder
        .last_offset_delta(records as i32 - 1)
        .base_timestamp(max_timestamp)
        .max_timestamp(max_timestamp)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
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
/// recovered from the tail segment footer (#58): no gap, no reuse. The new
/// process takes over only once the previous writer's lease (#59) has lapsed
/// (lease fencing bounds failover to one lease term after an unclean stop).
#[tokio::test]
async fn cold_restart_recovers_offsets_from_the_footer() -> Result<(), Error> {
    let ttl = Duration::from_millis(150);
    let bucket = InMemory::new();
    let topic_a = "org.env.conn.tab_a";
    let a = Topition::new(topic_a, 0);

    // First process: two windows -> offsets 0 then 3 (5 records total).
    {
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
            .prefix_coalesce(true)
            .prefix_lease_ttl(ttl);
        create_topic(&store, topic_a).await?;
        assert_eq!(0, store.produce(None, &a, batch(3)?).await?);
        assert_eq!(3, store.produce(None, &a, batch(2)?).await?);
    }

    // The lease lapses, so a takeover is allowed.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // A fresh process on the same bucket: empty in-memory counters. The next
    // produce must continue at 5, recovered from the tail segment footer, and
    // land in a third segment (sequence recovered from the tail listing).
    let restarted = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    let resumed = restarted.produce(None, &a, batch(1)?).await?;
    assert_eq!(5, resumed, "resume past the footer end offset, no reuse");
    assert_eq!(3, segments(&bucket).await.len());

    Ok(())
}

/// Two writers contending for the same prefix: exactly one acquires the lease
/// and appends; the other is fenced (NotLeaderOrFollower) and its produce fails
/// (#59) — at most one writer per prefix, no coordinator.
#[tokio::test]
async fn two_writers_contend_one_is_fenced() -> Result<(), Error> {
    let bucket = InMemory::new();
    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    let store1 = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    create_topic(&store1, topic).await?;
    let store2 = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);

    let (r1, r2) = tokio::join!(
        store1.produce(None, &a, batch(1)?),
        store2.produce(None, &a, batch(1)?),
    );

    let winners = [r1.is_ok(), r2.is_ok()]
        .into_iter()
        .filter(|ok| *ok)
        .count();
    assert_eq!(
        1, winners,
        "exactly one writer appends, the other is fenced"
    );
    assert_eq!(1, segments(&bucket).await.len(), "one segment written");

    Ok(())
}

/// After a lease expires and a new writer takes it over (bumping the epoch), the
/// old writer is a zombie: its next append is fenced by the etag CAS and never
/// reaches storage (#59).
#[tokio::test]
async fn a_zombie_writer_is_fenced_after_takeover() -> Result<(), Error> {
    let ttl = Duration::from_millis(80);
    let bucket = InMemory::new();
    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    let old = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_lease_ttl(ttl);
    create_topic(&old, topic).await?;
    assert_eq!(0, old.produce(None, &a, batch(1)?).await?);

    // The lease lapses.
    tokio::time::sleep(Duration::from_millis(160)).await;

    // A new writer takes over (epoch bumped), recovers offset 1 from the footer.
    let new = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_lease_ttl(ttl);
    assert_eq!(1, new.produce(None, &a, batch(1)?).await?);

    // The old writer, now holding a stale lease, is fenced and cannot append.
    let zombie = old.produce(None, &a, batch(1)?).await;
    assert!(zombie.is_err(), "fenced zombie must not append");
    assert_eq!(
        2,
        segments(&bucket).await.len(),
        "only old+new segments, no zombie"
    );

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

/// A consumer of one topic reads exactly its own records out of a shared
/// segment via a ranged GET — correct offsets, no cross-topic data (#60).
#[tokio::test]
async fn fetch_reads_only_its_topition_from_a_shared_segment() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);

    let topic_a = "org.env.conn.tab_a";
    let topic_b = "org.env.conn.tab_b";
    create_topic(&store, topic_a).await?;
    create_topic(&store, topic_b).await?;
    let a = Topition::new(topic_a, 0);
    let b = Topition::new(topic_b, 0);

    // One window, one shared segment: A=5 records, B=4 records.
    let (ra, rb) = tokio::join!(
        store.produce(None, &a, batch(5)?),
        store.produce(None, &b, batch(4)?),
    );
    _ = ra?;
    _ = rb?;
    assert_eq!(1, segments(&bucket).await.len());

    // A reads its 5 records from offset 0 — only A's bytes.
    let fa = fetch_from(&store, &a, 0).await?;
    let a_records: i64 = fa
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(5, a_records, "A reads exactly its own records");
    assert_eq!(0, fa[0].base_offset);

    // B reads its 4 records from offset 0 — only B's bytes, independent offsets.
    let fb = fetch_from(&store, &b, 0).await?;
    let b_records: i64 = fb
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(4, b_records, "B reads exactly its own records");
    assert_eq!(0, fb[0].base_offset);

    Ok(())
}

/// A fresh reader process (empty in-memory hint) fetches from segments: it
/// recovers the high watermark footer-only and returns the records (#60/#58).
#[tokio::test]
async fn a_fresh_reader_fetches_from_segments() -> Result<(), Error> {
    let bucket = InMemory::new();
    let topic_a = "org.env.conn.tab_a";
    let a = Topition::new(topic_a, 0);

    {
        let writer = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
        create_topic(&writer, topic_a).await?;
        assert_eq!(0, writer.produce(None, &a, batch(3)?).await?);
    }

    // Fresh reader: no cached hint, no lease — read path only.
    let reader = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);

    let latest = reader
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(a.clone(), ListOffset::Latest)],
        )
        .await?;
    assert_eq!(Some(3), latest[0].1.offset, "high watermark recovered");

    let earliest = reader
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(a.clone(), ListOffset::Earliest)],
        )
        .await?;
    assert_eq!(Some(0), earliest[0].1.offset, "log start from footer");

    let fetched = fetch_from(&reader, &a, 0).await?;
    let records: i64 = fetched
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(3, records, "fresh reader returns the records");

    Ok(())
}

/// Seam continuity (#58): a topic with existing legacy `records/{offset}.batch`
/// data flipped to segment mode continues its offset sequence unbroken — the
/// first segment offset equals the legacy tail, no gap/overlap/off-by-one.
#[tokio::test]
async fn first_segment_continues_from_the_legacy_tail() -> Result<(), Error> {
    let bucket = InMemory::new();
    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    // Legacy phase (prefix mode off): per-batch records/ objects, tail -> 5.
    {
        let legacy = DynoStore::new(CLUSTER, NODE, bucket.clone());
        create_topic(&legacy, topic).await?;
        assert_eq!(0, legacy.produce(None, &a, batch(3)?).await?);
        assert_eq!(3, legacy.produce(None, &a, batch(2)?).await?);
    }

    // Cutover to segment mode: the first segment must start at the legacy tail.
    let seg = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    let first = seg.produce(None, &a, batch(4)?).await?;
    assert_eq!(
        5, first,
        "first segment offset == legacy tail, no gap/overlap"
    );

    Ok(())
}

/// Hybrid reads (#60): a topic with `[0, C)` legacy objects and `[C, ∞)`
/// segments serves both layouts, a single fetch spanning `C` stitches them with
/// a continuous offset sequence, and earliest/high_watermark span both layouts.
#[tokio::test]
async fn hybrid_fetch_stitches_legacy_and_segment() -> Result<(), Error> {
    let bucket = InMemory::new();
    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    // Legacy [0, 5): batch(3)@0, batch(2)@3.
    {
        let legacy = DynoStore::new(CLUSTER, NODE, bucket.clone());
        create_topic(&legacy, topic).await?;
        assert_eq!(0, legacy.produce(None, &a, batch(3)?).await?);
        assert_eq!(3, legacy.produce(None, &a, batch(2)?).await?);
    }

    // Segment [5, 9): batch(4)@5 (continues from the seam).
    let seg = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    assert_eq!(5, seg.produce(None, &a, batch(4)?).await?);

    // A single fetch from 0 stitches legacy + segment: 9 records, base offsets
    // 0, 3, 5, contiguous across the seam at C=5, no gap/duplicate/reorder.
    let fetched = fetch_from(&seg, &a, 0).await?;
    let bases: Vec<i64> = fetched.iter().map(|batch| batch.base_offset).collect();
    assert_eq!(vec![0, 3, 5], bases, "legacy then segment, continuous");
    let total: i64 = fetched
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(9, total, "all records across the seam");

    // earliest follows the legacy objects; high watermark spans both layouts.
    let earliest = seg
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(a.clone(), ListOffset::Earliest)],
        )
        .await?;
    assert_eq!(Some(0), earliest[0].1.offset, "earliest from legacy");

    let latest = seg
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(a.clone(), ListOffset::Latest)],
        )
        .await?;
    assert_eq!(Some(9), latest[0].1.offset, "high watermark spans the seam");

    Ok(())
}

/// Whole-segment retention (#61): a segment all of whose records are past the
/// threshold is deleted; a segment with any recent record survives.
#[tokio::test]
async fn expires_aged_segments_keeps_recent_ones() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    let recent = now_ms();
    // Two windows -> two segments: one ancient, one recent.
    _ = store.produce(None, &a, batch_at(2, 1_000)?).await?;
    _ = store.produce(None, &a, batch_at(2, recent)?).await?;
    assert_eq!(2, segments(&bucket).await.len());

    // Threshold just below the recent record: the ancient segment expires, the
    // recent one survives.
    let deleted = store.expire_prefix_segments(PREFIX, recent - 1_000).await?;
    assert_eq!(1, deleted);
    assert_eq!(1, segments(&bucket).await.len());

    Ok(())
}

/// A shared segment is kept while *any* of its sub-streams is still live (#61):
/// whole-segment expiry never drops a segment a live topic still needs.
#[tokio::test]
async fn keeps_a_segment_while_any_substream_is_live() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);

    let topic_a = "org.env.conn.tab_a";
    let topic_b = "org.env.conn.tab_b";
    create_topic(&store, topic_a).await?;
    create_topic(&store, topic_b).await?;
    let a = Topition::new(topic_a, 0);
    let b = Topition::new(topic_b, 0);

    let recent = now_ms();
    // One window, one segment: A ancient, B recent.
    let (ra, rb) = tokio::join!(
        store.produce(None, &a, batch_at(1, 1_000)?),
        store.produce(None, &b, batch_at(1, recent)?),
    );
    _ = ra?;
    _ = rb?;
    assert_eq!(1, segments(&bucket).await.len());

    // Even though A's records are ancient, B is live, so the shared segment stays.
    let deleted = store.expire_prefix_segments(PREFIX, recent - 1_000).await?;
    assert_eq!(0, deleted);
    assert_eq!(1, segments(&bucket).await.len());

    Ok(())
}

/// A large backfill batch bypasses the segment buffer and takes the legacy
/// per-object path (#62): already S3-efficient, and parallel (no lease).
#[tokio::test]
async fn a_large_backfill_batch_bypasses_coalescing() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    // 1000 records >= the backfill threshold -> bypass to a records/ object.
    let offset = store.produce(None, &a, batch(1_000)?).await?;
    assert_eq!(0, offset);

    assert!(
        segments(&bucket).await.is_empty(),
        "backfill must not write a segment"
    );

    let records = Path::from(format!(
        "clusters/{CLUSTER}/topics/{topic}/partitions/{:0>10}/records/",
        0
    ));
    let count = bucket
        .list(Some(&records))
        .try_collect::<Vec<_>>()
        .await
        .expect("list records")
        .len();
    assert_eq!(1, count, "one backfill batch object");

    Ok(())
}

/// Snapshot → streaming: a backfill (legacy objects) followed by CDC (segments)
/// keeps one continuous offset sequence with no gap/duplicate, and a fetch
/// stitches both (#62 handoff over #58 seam / #60 hybrid).
#[tokio::test]
async fn backfill_then_cdc_is_continuous() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    // Backfill: one bulk batch of 1000 records -> legacy object, offsets [0, 1000).
    assert_eq!(0, store.produce(None, &a, batch(1_000)?).await?);

    // CDC steady state resumes: a small batch coalesces into a segment, and it
    // must continue at 1000 (no gap/overlap at the snapshot→streaming seam).
    let cdc = store.produce(None, &a, batch(3)?).await?;
    assert_eq!(1000, cdc, "streaming continues from the backfill tail");
    assert_eq!(1, segments(&bucket).await.len());

    // A fetch from 0 stitches backfill + CDC: 1003 records, continuous.
    let fetched = fetch_from(&store, &a, 0).await?;
    let total: i64 = fetched
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(1003, total);
    assert_eq!(0, fetched.first().map(|b| b.base_offset).unwrap());

    Ok(())
}

/// `prefix_owner_node` is a deterministic, order-independent pure assignment and
/// removing a non-owner leaves the owner unchanged (rendezvous-hash stability).
#[test]
fn prefix_owner_is_deterministic_and_stable() {
    use crate::dynostore::prefix_owner_node;

    assert_eq!(None, prefix_owner_node("x", &[]));
    assert_eq!(Some(111), prefix_owner_node("x", &[111]));

    let nodes = [1, 2, 3, 4];
    let owner = prefix_owner_node("org.env.conn", &nodes).expect("owner");
    // Order-independent.
    assert_eq!(
        Some(owner),
        prefix_owner_node("org.env.conn", &[4, 3, 2, 1])
    );

    // Removing a non-owner node keeps the owner.
    let non_owner = nodes.into_iter().find(|n| *n != owner).unwrap();
    let pruned: Vec<i32> = nodes.into_iter().filter(|n| *n != non_owner).collect();
    assert_eq!(Some(owner), prefix_owner_node("org.env.conn", &pruned));
}
