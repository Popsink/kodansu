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

use std::{
    fmt::{self, Debug, Display},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{TryStreamExt as _, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    memory::InMemory, path::Path,
};
use tansu_sans_io::{
    ErrorCode, IsolationLevel, ListOffset,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage, Topition,
    dynostore::{CoalesceTuning, DynoStore, Era, PrefixLease, SegmentFooter, SubstreamEntry},
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
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
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

async fn create_topic_with_configs(
    storage: &DynoStore,
    name: &str,
    configs: &[(&str, &str)],
) -> Result<()> {
    let configs: Vec<CreatableTopicConfig> = configs
        .iter()
        .map(|(k, v)| {
            CreatableTopicConfig::default()
                .name((*k).to_owned())
                .value(Some((*v).to_owned()))
        })
        .collect();
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some(configs)),
            false,
        )
        .await?;
    Ok(())
}

fn segment_path(seq: u64) -> Path {
    Path::from(format!(
        "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{seq:0>20}.seg"
    ))
}

/// A SubstreamEntry for a topic-0 sub-stream (test scaffolding).
fn entry(topic: &str, base: i64, count: i64) -> SubstreamEntry {
    SubstreamEntry {
        topic: topic.to_owned(),
        partition: 0,
        base_offset: base,
        record_count: count,
        byte_start: 0,
        byte_len: 8,
        max_timestamp: 0,
        producers: Vec::new(),
    }
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

/// The legacy per-`(topic, partition)` `records/` objects for partition 0.
async fn legacy_records(bucket: &InMemory, topic: &str) -> Vec<Path> {
    let listing = Path::from(format!(
        "clusters/{CLUSTER}/topics/{topic}/partitions/{:0>10}/records/",
        0
    ));
    bucket
        .list(Some(&listing))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .expect("list records")
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

/// Write a pre-cutover `lease.json` for `prefix` with a chosen epoch (expired),
/// standing in for the lease-era state the #92 migration seeds the era above.
async fn write_lease(bucket: &InMemory, store: &DynoStore, prefix: &str, epoch: i64) {
    let payload = PutPayload::from(Bytes::from(
        serde_json::to_vec(&PrefixLease {
            epoch,
            holder: "old".to_owned(),
            expires_at_ms: 0,
            maintained_at_ms: 0,
        })
        .expect("serialize lease"),
    ));
    _ = bucket
        .put(&store.lease_location(prefix), payload)
        .await
        .expect("write lease");
}

/// The durable seeded era epoch for `prefix`, or `None` if not yet seeded.
async fn read_era(bucket: &InMemory, store: &DynoStore, prefix: &str) -> Option<i64> {
    match bucket.get(&store.era_location(prefix)).await {
        Ok(result) => Some(
            serde_json::from_slice::<Era>(&result.bytes().await.expect("era bytes"))
                .expect("decode era")
                .era_epoch,
        ),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(err) => panic!("read era: {err}"),
    }
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

/// `ListOffsets` LATEST serves the tail *timestamp* for a pure-segment topic
/// from the footer's `max_timestamp` (#73), not from a per-topic `records/`
/// listing (there is none). The offset already comes from `high_watermark`.
#[tokio::test]
async fn list_offsets_latest_timestamp_from_footer() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    const TS: i64 = 1_700_000_000_000;
    assert_eq!(0, store.produce(None, &a, batch_at(2, TS)?).await?);

    let latest = store
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(a.clone(), ListOffset::Latest)],
        )
        .await?;

    assert_eq!(Some(2), latest[0].1.offset, "high watermark");
    assert_eq!(
        Some(SystemTime::UNIX_EPOCH + Duration::from_millis(TS as u64)),
        latest[0].1.timestamp,
        "tail timestamp derived from the footer max_timestamp",
    );

    Ok(())
}

/// Retention (#71): a pure-segment partition has an empty legacy `records/`
/// prefix, so the maintainer must not re-LIST it every tick. The real scan path
/// (`expire_partition`) records a time-bounded skip; subsequent ticks skip it;
/// the skip self-heals once its TTL lapses so a legacy object written afterwards
/// (possibly by another process) is still scanned and expired.
#[tokio::test]
async fn pure_segment_partition_retention_skips_rescan() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    // Produce lands in a segment; the legacy `records/` prefix stays empty.
    assert_eq!(0, store.produce(None, &a, batch(1)?).await?);

    // A threshold far in the future would expire anything present.
    let threshold = now_ms() + 1_000_000;

    // Cold: no skip recorded yet, so the partition must be scanned once.
    assert!(
        store.partition_maybe_expirable(&a, threshold)?,
        "cold partition must be scanned",
    );

    // The real maintenance scan finds an empty `records/`; under prefix
    // coalescing it records the time-bounded skip (not a permanent sentinel).
    _ = store.expire_partition(&a, threshold).await?;

    // Subsequent maintenance ticks within the TTL skip the empty per-topic LIST.
    assert!(
        !store.partition_maybe_expirable(&a, threshold)?,
        "pure-segment partition must be skipped while the skip is fresh",
    );

    // Simulate the TTL lapsing (backdate the recorded instant beyond the TTL):
    // the skip self-heals so retention scans the partition again — which is how a
    // legacy object written meanwhile (even by another process that never touched
    // this in-memory map) is eventually expired.
    {
        let mut skip = store.retention_empty_skip.lock().expect("skip lock");
        let stale = SystemTime::now() - Duration::from_secs(2 * 60 * 60);
        _ = skip.insert(a.clone(), stale);
    }
    assert!(
        store.partition_maybe_expirable(&a, threshold)?,
        "a lapsed skip must re-arm the retention scan (self-heal)",
    );

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

/// Many concurrent produces to one prefix, with a tiny flush threshold forcing
/// overlapping flush windows, must yield a gap-free, duplicate-free offset
/// sequence — the per-prefix flush lock serializes offset assignment (#1 fix).
#[tokio::test]
async fn concurrent_flushes_assign_unique_offsets() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            coalesce_batches: Some(2),
            ..Default::default()
        });

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    // 40 single-record batches produced concurrently: with a 2-batch flush
    // threshold this drains many overlapping windows.
    const N: usize = 40;
    let mut batches = Vec::with_capacity(N);
    for _ in 0..N {
        batches.push(batch(1)?);
    }
    let results =
        futures::future::join_all(batches.into_iter().map(|b| store.produce(None, &a, b))).await;

    let mut offsets = results.into_iter().collect::<Result<Vec<i64>>>()?;
    offsets.sort_unstable();
    assert_eq!(
        (0..N as i64).collect::<Vec<_>>(),
        offsets,
        "exactly one of each offset 0..N — no gap, no duplicate"
    );

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

/// Segment expiry is single-writer per prefix (#115): every replica runs
/// `maintain`, so without a lease all N would race the same deletes + floor
/// writes. Reusing the compaction lease makes a non-holder yield rather than
/// re-run the expiry against already-gone keys. Two stores share a bucket; the
/// lease holder expires, the other yields with the segments untouched.
#[tokio::test]
async fn segment_expiry_yields_to_the_lease_holder() -> Result<(), Error> {
    let bucket = InMemory::new();
    let holder = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    let other = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);

    let topic = "org.env.conn.tab_a";
    create_topic(&holder, topic).await?;
    let a = Topition::new(topic, 0);

    // Two ancient segments, both past any positive retention threshold.
    _ = holder.produce(None, &a, batch_at(1, 1_000)?).await?;
    _ = holder.produce(None, &a, batch_at(1, 2_000)?).await?;
    assert_eq!(2, segments(&bucket).await.len());

    // The holder takes the maintenance (compaction) lease; `other` is fenced.
    assert!(holder.acquire_compaction_lease(PREFIX).await.is_ok());

    // A non-holder must yield: no deletes, segments untouched.
    let threshold = now_ms();
    assert_eq!(0, other.expire_prefix_segments(PREFIX, threshold).await?);
    assert_eq!(
        2,
        segments(&bucket).await.len(),
        "a non-holder must not delete segments (#115)"
    );

    // The lease holder performs the expiry.
    assert_eq!(2, holder.expire_prefix_segments(PREFIX, threshold).await?);
    assert_eq!(0, segments(&bucket).await.len());

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

/// A dedicated maintenance worker (a fresh process that never produced, so its
/// in-memory index is cold) discovers the prefix from the topic metadata and
/// compacts it (#66 review fix) — it does not depend on a warm local index.
#[tokio::test]
async fn maintainer_with_cold_index_compacts() -> Result<(), Error> {
    let tuning = CoalesceTuning {
        prefix_compact_min_segments: Some(2),
        prefix_compact_keep_hot: Some(0),
        prefix_compact_target_bytes: Some(1 << 30),
        ..Default::default()
    };
    let bucket = InMemory::new();
    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    // A producer writes four segments, then goes away.
    {
        let producer = DynoStore::new(CLUSTER, NODE, bucket.clone())
            .prefix_coalesce(true)
            .coalesce_tuning(tuning);
        create_topic(&producer, topic).await?;
        for _ in 0..4 {
            _ = producer.produce(None, &a, batch(1)?).await?;
        }
        assert_eq!(4, segments(&bucket).await.len());
    }

    // A fresh maintainer (cold index) runs the compaction pass.
    let maintainer = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .coalesce_tuning(tuning);
    assert!(
        maintainer.policy_compact_segments(None).await? > 0,
        "maintainer discovered the prefix and compacted"
    );
    assert!(segments(&bucket).await.len() < 4);

    Ok(())
}

/// Compaction merges the epoch-fenced view, NOT raw footers (#69 review fix,
/// critical): a zombie/overlapping segment in the run is dropped, never fused —
/// so the merged segment doesn't duplicate records.
#[tokio::test]
async fn compaction_drops_zombie_overlap_input() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            prefix_compact_min_segments: Some(1),
            prefix_compact_keep_hot: Some(0),
            prefix_compact_target_bytes: Some(1 << 30),
            ..Default::default()
        });
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    // Legit segment (epoch 2) and a zombie (epoch 1) both covering [0,3).
    let (legit, legit_footer) = store.encode_segment(&[(a.clone(), 0, vec![batch(3)?])], 2)?;
    _ = bucket
        .put(&segment_path(0), legit)
        .await
        .expect("put legit");
    store.index_insert(PREFIX, 0, legit_footer, 100)?;

    let (zombie, zombie_footer) = store.encode_segment(&[(a.clone(), 0, vec![batch(3)?])], 1)?;
    _ = bucket
        .put(&segment_path(1), zombie)
        .await
        .expect("put zombie");
    store.index_insert(PREFIX, 1, zombie_footer, 100)?;

    // The fence already hides the zombie: 3 records, not 6.
    let before: i64 = fetch_from(&store, &a, 0)
        .await?
        .iter()
        .map(|b| b.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(3, before);

    // Compaction removes both run segments and merges only the fenced view.
    assert_eq!(2, store.compact_prefix_segments(PREFIX).await?);

    // Still 3 records — the zombie was not fused into the merged segment.
    let after: i64 = fetch_from(&store, &a, 0)
        .await?
        .iter()
        .map(|b| b.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(3, after, "zombie not fused → no duplicate");

    Ok(())
}

/// A reader whose index still points at an original that compaction deleted must
/// still read the data — the merged segment wins the overlap (higher seq) and is
/// served instead (#69 review fix, no empty-result data loss).
#[tokio::test]
async fn stale_index_entry_reads_via_merged() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            prefix_compact_min_segments: Some(1),
            prefix_compact_keep_hot: Some(0),
            prefix_compact_target_bytes: Some(1 << 30),
            ..Default::default()
        });
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    // Two segments [0,2) then [2,4); compact into one merged, originals deleted.
    _ = store.produce(None, &a, batch(2)?).await?;
    _ = store.produce(None, &a, batch(2)?).await?;
    assert_eq!(2, store.compact_prefix_segments(PREFIX).await?);

    // Re-inject a now-deleted original (seq 0) as a stale index entry.
    store.index_insert(
        PREFIX,
        0,
        SegmentFooter {
            writer_epoch: 1,
            nonce: 0,
            entries: vec![entry(topic, 0, 2)],
        },
        0,
    )?;

    // The merged segment (higher seq) wins the overlap, so the stale seq is
    // ignored and the read returns all 4 records.
    let records: i64 = fetch_from(&store, &a, 0)
        .await?
        .iter()
        .map(|b| b.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(4, records);

    Ok(())
}

/// Draining: one maintenance pass compacts a prefix down to <= min_segments,
/// regardless of how fast segments accrued (#69 review fix).
#[tokio::test]
async fn compaction_drains_to_min_segments() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            prefix_compact_min_segments: Some(2),
            prefix_compact_keep_hot: Some(0),
            prefix_compact_target_bytes: Some(4096),
            ..Default::default()
        });
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    for _ in 0..8 {
        _ = store.produce(None, &a, batch(1)?).await?;
    }
    assert_eq!(8, segments(&bucket).await.len());

    _ = store.policy_compact_segments(None).await?;
    assert!(
        segments(&bucket).await.len() <= 2,
        "drained to <= min_segments in one pass"
    );

    // All 8 records still readable, in order.
    let bases: Vec<i64> = fetch_from(&store, &a, 0)
        .await?
        .iter()
        .map(|b| b.base_offset)
        .collect();
    assert_eq!((0..8).collect::<Vec<_>>(), bases);

    Ok(())
}

/// Compaction (#66) merges old segments into fewer, and reads are byte-for-byte
/// unchanged (same offsets, no gap/dup); produce continues past the merge.
#[tokio::test]
async fn compaction_merges_segments_and_reads_are_unchanged() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            prefix_compact_min_segments: Some(2),
            prefix_compact_keep_hot: Some(0),
            prefix_compact_target_bytes: Some(1 << 30),
            ..Default::default()
        });
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    // Four windows -> four segments, offsets 0..4.
    for _ in 0..4 {
        _ = store.produce(None, &a, batch(1)?).await?;
    }
    assert_eq!(4, segments(&bucket).await.len());

    let before: Vec<i64> = fetch_from(&store, &a, 0)
        .await?
        .iter()
        .map(|b| b.base_offset)
        .collect();
    assert_eq!(vec![0, 1, 2, 3], before);

    // Compact: the four merge into one.
    let merged = store.compact_prefix_segments(PREFIX).await?;
    assert_eq!(4, merged);
    assert_eq!(1, segments(&bucket).await.len());

    // Reads are identical after the merge.
    let after: Vec<i64> = fetch_from(&store, &a, 0)
        .await?
        .iter()
        .map(|b| b.base_offset)
        .collect();
    assert_eq!(before, after);

    // Produce continues from the tail — no gap.
    assert_eq!(4, store.produce(None, &a, batch(1)?).await?);

    Ok(())
}

/// Compaction is a no-op below the trigger threshold (#66).
#[tokio::test]
async fn compaction_below_threshold_is_a_noop() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            prefix_compact_min_segments: Some(10),
            ..Default::default()
        });
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    for _ in 0..3 {
        _ = store.produce(None, &a, batch(1)?).await?;
    }
    assert_eq!(3, segments(&bucket).await.len());

    assert_eq!(0, store.compact_prefix_segments(PREFIX).await?);
    assert_eq!(
        3,
        segments(&bucket).await.len(),
        "below threshold: untouched"
    );

    Ok(())
}

/// A large oldest segment (a prior merge, or a folded-in backfill segment) must
/// not head-of-line-block compaction of the small segments behind it (#114).
/// Before the fix the run seeded at the big segment; adding the next overflowed
/// the target, so the run collapsed to length one, `compact_prefix_segments`
/// returned `Ok(0)`, and `policy_compact_segments` treated the prefix as drained
/// while the small segments piled up — `S` unbounded until retention. Now the
/// big segment is left in place as a boundary and the small run behind it is
/// merged.
#[tokio::test]
async fn large_oldest_segment_does_not_stall_compaction() -> Result<(), Error> {
    let bucket = InMemory::new();
    // Leaseless so every produce routes through the segment path (the
    // non-leaseless backfill bypass would send the big batch to legacy
    // `records/` instead of making it the large oldest *segment* under test).
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true)
        .coalesce_tuning(CoalesceTuning {
            prefix_compact_min_segments: Some(2),
            prefix_compact_keep_hot: Some(0),
            prefix_compact_target_bytes: Some(2048),
            ..Default::default()
        });
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    // Oldest segment is large (well above target_bytes): a big batch.
    _ = store.produce(None, &a, batch(1_000)?).await?;
    // Six small segments behind it (each far below target_bytes).
    for _ in 0..6 {
        _ = store.produce(None, &a, batch(1)?).await?;
    }
    assert_eq!(7, segments(&bucket).await.len());

    let before: Vec<i64> = fetch_from(&store, &a, 0)
        .await?
        .iter()
        .map(|b| b.base_offset)
        .collect();

    // Without the fix this stalls at 7 (the run collapses to length one). With
    // it, the small run behind the big segment merges and the prefix drains to
    // <= min_segments.
    _ = store.policy_compact_segments(None).await?;
    let after_count = segments(&bucket).await.len();
    assert!(
        after_count <= 2,
        "small segments behind a large one must still compact (S = {after_count})"
    );

    // Reads unchanged — offsets and order preserved across the merge.
    let after: Vec<i64> = fetch_from(&store, &a, 0)
        .await?
        .iter()
        .map(|b| b.base_offset)
        .collect();
    assert_eq!(before, after);

    Ok(())
}

/// Epoch fencing on read (#59 review fix): when two segments' offset ranges
/// overlap (only a fenced/zombie writer produces that), the higher writer_epoch
/// wins and the stale one is dropped; non-overlapping legitimate history is
/// kept.
#[tokio::test]
async fn epoch_fencing_drops_stale_overlapping_segment() -> Result<(), Error> {
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new()).prefix_coalesce(true);
    let topic = "org.env.conn.tab_a";

    let entry = |base: i64, count: i64| SubstreamEntry {
        topic: topic.to_owned(),
        partition: 0,
        base_offset: base,
        record_count: count,
        byte_start: 0,
        byte_len: 8,
        max_timestamp: 0,
        producers: Vec::new(),
    };
    let footer = |epoch: i64, base: i64, count: i64| SegmentFooter {
        writer_epoch: epoch,
        nonce: 0,
        entries: vec![entry(base, count)],
    };

    // seq0 epoch1 [0,10) — legit history.
    store.index_insert(PREFIX, 0, footer(1, 0, 10), 0)?;
    // seq1 epoch2 [10,20) — the new writer after a takeover (contiguous, kept).
    store.index_insert(PREFIX, 1, footer(2, 10, 10), 0)?;
    // seq2 epoch1 [10,20) — a zombie overlapping seq1 with the OLD epoch.
    store.index_insert(PREFIX, 2, footer(1, 10, 10), 0)?;

    let valid = store.valid_substream_segments(PREFIX, topic, 0)?;
    let seqs: Vec<u64> = valid.iter().map(|(seq, _)| *seq).collect();
    assert_eq!(vec![0, 1], seqs, "zombie seq2 dropped, higher epoch wins");

    Ok(())
}

/// A large batch arriving AFTER the sub-stream is segmented must coalesce, not
/// bypass to legacy (#62 review fix) — otherwise it would write records/ above
/// segments and break the seam.
#[tokio::test]
async fn large_batch_after_segmentation_coalesces() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    // A small batch segments the sub-stream, then a large one arrives.
    assert_eq!(0, store.produce(None, &a, batch(1)?).await?);
    let big = store.produce(None, &a, batch(1_000)?).await?;
    assert_eq!(1, big, "continues in a segment, no offset break");

    let records = Path::from(format!(
        "clusters/{CLUSTER}/topics/{topic}/partitions/{:0>10}/records/",
        0
    ));
    let legacy = bucket
        .list(Some(&records))
        .try_collect::<Vec<_>>()
        .await
        .expect("list records")
        .len();
    assert_eq!(0, legacy, "no legacy object written after segmentation");
    assert_eq!(2, segments(&bucket).await.len());

    Ok(())
}

/// A backfill batch (large, before any segment) takes the legacy per-object path
/// under prefix coalescing; with the dual-authority guard (#78) it holds the
/// per-prefix flush lock, so it serializes with coalesced flushes of the same
/// prefix. Offsets stay contiguous across the legacy→segment seam (no
/// duplicate/overlapping offset), and the guarded legacy write does not deadlock
/// against the coalesced flush lock.
#[tokio::test]
async fn backfill_then_coalesce_offsets_are_contiguous() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    // Large first batch (>= PREFIX_BACKFILL_MIN_RECORDS, no segment yet) → the
    // legacy `records/{offset}.batch` create path (`assign_and_create`), now
    // guarded by the per-prefix flush lock.
    assert_eq!(0, store.produce(None, &a, batch(1_000)?).await?);

    // Smaller follow-ups coalesce into segments, continuing from the legacy tail.
    assert_eq!(1_000, store.produce(None, &a, batch(2)?).await?);
    assert_eq!(1_002, store.produce(None, &a, batch(3)?).await?);

    // The hybrid read stitches legacy [0,1000) + segments [1000,1005) with no
    // gap or duplicate.
    let fetched = fetch_from(&store, &a, 0).await?;
    let total: i64 = fetched
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(1_005, total, "contiguous across the legacy→segment seam");

    Ok(())
}

/// After retention drains every segment, a restart must not reuse offsets: the
/// per-sub-stream floor persisted before deletion keeps the next offset (#61
/// review fix).
#[tokio::test]
async fn full_drain_then_restart_keeps_offset() -> Result<(), Error> {
    let bucket = InMemory::new();
    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    {
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
            .prefix_coalesce(true)
            .prefix_lease_ttl(Duration::from_millis(80));
        create_topic(&store, topic).await?;
        assert_eq!(0, store.produce(None, &a, batch_at(1, 1_000)?).await?);
        assert_eq!(1, store.produce(None, &a, batch_at(1, 2_000)?).await?);

        // Expire everything (threshold far in the future) → both segments gone.
        let deleted = store.expire_prefix_segments(PREFIX, now_ms()).await?;
        assert_eq!(2, deleted);
        assert!(segments(&bucket).await.is_empty());
    }

    // Let the previous holder's lease lapse so the restart can take over.
    tokio::time::sleep(Duration::from_millis(160)).await;

    // Fresh process: no in-memory state, no segments, legacy drained. The next
    // offset must still be 2 (recovered from the persisted floor), not 0.
    let restarted = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    let resumed = restarted.produce(None, &a, batch(1)?).await?;
    assert_eq!(2, resumed, "no offset reuse after a full retention drain");

    Ok(())
}

/// A segment sequence *name* freed by full retention must not be reused (#77).
/// After every segment expires, a fresh writer must continue at the persisted
/// sequence floor, never the freed seq 0 — otherwise a peer (or an external
/// S3-direct reader) still caching the old seq-0 footer would serve its stale
/// byte ranges against a reborn object.
#[tokio::test]
async fn seq_name_not_reused_after_full_expiry() -> Result<(), Error> {
    let bucket = InMemory::new();
    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    {
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
            .prefix_coalesce(true)
            .prefix_lease_ttl(Duration::from_millis(80));
        create_topic(&store, topic).await?;
        assert_eq!(0, store.produce(None, &a, batch_at(1, 1_000)?).await?);
        assert_eq!(1, store.produce(None, &a, batch_at(1, 2_000)?).await?);

        // Segments live at seq 0 and 1.
        let mut seqs = segments(&bucket).await;
        seqs.sort();
        assert_eq!(vec![segment_path(0), segment_path(1)], seqs);

        // Expire everything → both segment objects gone, seq floor raised to 2.
        assert_eq!(2, store.expire_prefix_segments(PREFIX, now_ms()).await?);
        assert!(segments(&bucket).await.is_empty());
    }

    tokio::time::sleep(Duration::from_millis(160)).await;

    // Fresh process: cold state, all segments gone. The next segment must be
    // written at seq 2 (the floor), never at the freed seq 0.
    let restarted = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    _ = restarted.produce(None, &a, batch(1)?).await?;

    let seqs = segments(&bucket).await;
    assert!(
        !seqs.contains(&segment_path(0)),
        "freed seq 0 name must not be reused",
    );
    assert_eq!(vec![segment_path(2)], seqs);

    Ok(())
}

/// A byte-budget-limited hybrid fetch must not jump into segments and skip the
/// unserved legacy tail — it returns only the legacy prefix, contiguous (#60
/// review fix).
#[tokio::test]
async fn hybrid_fetch_budget_limited_does_not_skip_legacy() -> Result<(), Error> {
    let bucket = InMemory::new();
    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    // Legacy [0,5): two batches. Then a segment at [5,9).
    {
        let legacy = DynoStore::new(CLUSTER, NODE, bucket.clone());
        create_topic(&legacy, topic).await?;
        assert_eq!(0, legacy.produce(None, &a, batch(3)?).await?);
        assert_eq!(3, legacy.produce(None, &a, batch(2)?).await?);
    }
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    assert_eq!(5, store.produce(None, &a, batch(4)?).await?);

    // max_bytes=1 stops the legacy read after the first batch (base 0). The fetch
    // must NOT append the segment [5,9) — that would skip legacy [3,5).
    let fetched = store
        .fetch(
            &a,
            0,
            0,
            1,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(200),
        )
        .await?;
    assert!(
        fetched.iter().all(|b| b.base_offset < 5),
        "no segment data before the legacy tail is served"
    );
    assert_eq!(Some(0), fetched.first().map(|b| b.base_offset));

    Ok(())
}

/// `retention.ms=-1` (retain forever) must keep every segment, not delete them
/// all (#61 review fix for the -1 parse).
#[tokio::test]
async fn retention_forever_keeps_segments() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    let topic = "org.env.conn.tab_a";
    create_topic_with_configs(
        &store,
        topic,
        &[("cleanup.policy", "delete"), ("retention.ms", "-1")],
    )
    .await?;
    let a = Topition::new(topic, 0);

    // An ancient segment that a positive retention would delete.
    _ = store.produce(None, &a, batch_at(2, 1_000)?).await?;
    assert_eq!(1, segments(&bucket).await.len());

    let deleted = store.policy_delete(SystemTime::now(), None).await?;
    assert_eq!(0, deleted, "retain-forever deletes nothing");
    assert_eq!(1, segments(&bucket).await.len());

    Ok(())
}

/// Leaseless (#86): with no lease, two replicas append to the SAME sub-stream by
/// alternating, and the fold-before-claim step makes each observe the other's
/// segment before deriving its base — so offsets stay dense and contiguous with
/// no reuse. Segments are written v2 and read back correctly.
#[tokio::test]
async fn leaseless_alternating_writers_stay_contiguous() -> Result<(), Error> {
    let bucket = InMemory::new();
    let mk = || {
        DynoStore::new(CLUSTER, NODE, bucket.clone())
            .prefix_coalesce(true)
            .prefix_leaseless(true)
    };
    let a_store = mk();
    let b_store = mk();
    let topic = "org.env.conn.tab_a";
    create_topic(&a_store, topic).await?;
    let a = Topition::new(topic, 0);

    // A → B → A → B, each awaited: every writer folds the other's latest segment
    // before it claims its own, so the next offset is always the true tail.
    assert_eq!(0, a_store.produce(None, &a, batch(1)?).await?);
    assert_eq!(1, b_store.produce(None, &a, batch(1)?).await?);
    assert_eq!(2, a_store.produce(None, &a, batch(1)?).await?);
    assert_eq!(3, b_store.produce(None, &a, batch(1)?).await?);

    // A fresh reader (cold index) recovers all four records footer-only.
    let reader = mk();
    let fetched = fetch_from(&reader, &a, 0).await?;
    let total: i64 = fetched
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(4, total, "four records, contiguous across two writers");

    Ok(())
}

/// Leaseless (#86): two replicas producing to the SAME sub-stream *concurrently*
/// exercise the seq-CAS conflict-correction loop — a create conflict makes the
/// loser fold the winner and retry the next sequence with a re-derived base. The
/// four records must land at four distinct, dense offsets (no reuse, no gap).
#[tokio::test]
async fn leaseless_concurrent_writers_no_reuse_or_gap() -> Result<(), Error> {
    let bucket = InMemory::new();
    let mk = || {
        DynoStore::new(CLUSTER, NODE, bucket.clone())
            .prefix_coalesce(true)
            .prefix_leaseless(true)
    };
    let a_store = mk();
    let b_store = mk();
    let topic = "org.env.conn.tab_a";
    create_topic(&a_store, topic).await?;
    let a = Topition::new(topic, 0);

    let (o1, o2, o3, o4) = tokio::join!(
        a_store.produce(None, &a, batch(1)?),
        b_store.produce(None, &a, batch(1)?),
        a_store.produce(None, &a, batch(1)?),
        b_store.produce(None, &a, batch(1)?),
    );

    let mut offsets = vec![o1?, o2?, o3?, o4?];
    offsets.sort_unstable();
    assert_eq!(
        vec![0, 1, 2, 3],
        offsets,
        "four concurrent produces → four distinct contiguous offsets",
    );

    let reader = mk();
    let fetched = fetch_from(&reader, &a, 0).await?;
    let total: i64 = fetched
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(4, total, "no record lost or duplicated under contention");

    Ok(())
}

/// An idempotent batch for `producer_id`/`epoch` at `base_sequence`.
fn idempotent_batch(
    producer_id: i64,
    epoch: i16,
    base_sequence: i32,
    records: usize,
) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder()
        .producer_id(producer_id)
        .producer_epoch(epoch)
        .base_sequence(base_sequence)
        .last_offset_delta(records as i32 - 1);

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("record-{i}").as_bytes(),
        ))));
    }

    builder
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

fn api_error(result: Result<i64>) -> ErrorCode {
    match result {
        Err(Error::Api(code)) => code,
        otherwise => panic!("expected Error::Api, got {otherwise:?}"),
    }
}

/// Leaseless idempotent dedup is folded from the log, not from
/// `producers/{id}.json` (#88): an in-order batch is admitted, a retried batch is
/// acked with its *original* offset without being re-appended, and the producer
/// object is never consulted or written on the segment path (its per-pod,
/// advance-before-durable view is exactly what the fold replaces, #79). This
/// asserts the demotion directly: no `producers/{id}.json` object exists, yet
/// dedup is exact.
#[tokio::test]
async fn leaseless_idempotent_dedup_is_log_based() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true);
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    let pid = 42;
    // In order: seq 0 (2 records → offsets 0,1), then seq 2 (offset 2).
    assert_eq!(
        0,
        store
            .produce(None, &tp, idempotent_batch(pid, 0, 0, 2)?)
            .await?
    );
    assert_eq!(
        2,
        store
            .produce(None, &tp, idempotent_batch(pid, 0, 2, 1)?)
            .await?
    );

    // Retries of both batches are acked with their original offsets, not
    // re-appended.
    assert_eq!(
        0,
        store
            .produce(None, &tp, idempotent_batch(pid, 0, 0, 2)?)
            .await?
    );
    assert_eq!(
        2,
        store
            .produce(None, &tp, idempotent_batch(pid, 0, 2, 1)?)
            .await?
    );

    // The next in-order batch continues densely at offset 3.
    assert_eq!(
        3,
        store
            .produce(None, &tp, idempotent_batch(pid, 0, 3, 1)?)
            .await?
    );

    // The log holds exactly the four distinct records — the two duplicates added
    // nothing.
    let fetched = fetch_from(&store, &tp, 0).await?;
    let total: i64 = fetched
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(4, total, "duplicates must not append records");

    // Dedup came from the folded footers, so the producer object was never
    // written on this path.
    let producer_object = Path::from(format!("clusters/{CLUSTER}/producers/{pid}.json"));
    assert!(
        bucket.head(&producer_object).await.is_err(),
        "leaseless idempotent produce must not write producers/{{id}}.json"
    );

    Ok(())
}

/// The dedup state converges across a connection migration (#88): a producer
/// whose earlier batches were written by one replica continues on a *fresh*
/// replica with no local producer state. The fresh replica folds the log and
/// derives the correct expected sequence — so the continuation is admitted (no
/// false `OutOfOrderSequenceNumber`) and a retry of an earlier batch is still
/// recognised as a duplicate and acked with its original offset. This is the
/// window the lazy `producers/{id}.json` checkpoint (#48) left open.
#[tokio::test]
async fn leaseless_dedup_survives_pod_migration() -> Result<(), Error> {
    let bucket = InMemory::new();
    let mk = || {
        DynoStore::new(CLUSTER, NODE, bucket.clone())
            .prefix_coalesce(true)
            .prefix_leaseless(true)
    };
    let a_store = mk();
    let topic = "org.env.conn.tab_a";
    create_topic(&a_store, topic).await?;
    let tp = Topition::new(topic, 0);

    let pid = 7;
    // Replica A writes seq 0 and 1.
    assert_eq!(
        0,
        a_store
            .produce(None, &tp, idempotent_batch(pid, 0, 0, 1)?)
            .await?
    );
    assert_eq!(
        1,
        a_store
            .produce(None, &tp, idempotent_batch(pid, 0, 1, 1)?)
            .await?
    );

    // The producer migrates to a brand-new replica B (cold: no in-memory
    // producer state, no checkpoint from A). Folding A's segments gives the
    // right expected sequence, so seq 2 is admitted contiguously.
    let b_store = mk();
    assert_eq!(
        2,
        b_store
            .produce(None, &tp, idempotent_batch(pid, 0, 2, 1)?)
            .await?
    );

    // A retry of seq 1 on B is deduped from A's log, acked with the original
    // offset — no false out-of-order, no re-append.
    assert_eq!(
        1,
        b_store
            .produce(None, &tp, idempotent_batch(pid, 0, 1, 1)?)
            .await?
    );

    let reader = mk();
    let fetched = fetch_from(&reader, &tp, 0).await?;
    let total: i64 = fetched
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(3, total, "three distinct records across the migration");

    Ok(())
}

/// A genuine sequence gap is still rejected on the leaseless path (#88): after
/// the log-fold, an out-of-order batch (`base_sequence` ahead of the expected
/// next) returns `OutOfOrderSequenceNumber` — the fold makes the classification
/// exact, so this is a real gap, not a stale-view artifact.
#[tokio::test]
async fn leaseless_out_of_order_sequence_is_rejected() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true);
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    let pid = 99;
    assert_eq!(
        0,
        store
            .produce(None, &tp, idempotent_batch(pid, 0, 0, 1)?)
            .await?
    );

    // seq 3 skips 1 and 2 — a gap.
    assert_eq!(
        ErrorCode::OutOfOrderSequenceNumber,
        api_error(
            store
                .produce(None, &tp, idempotent_batch(pid, 0, 3, 1)?)
                .await
        )
    );

    // The rejected batch appended nothing.
    let fetched = fetch_from(&store, &tp, 0).await?;
    let total: i64 = fetched
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(1, total, "the out-of-order batch must not append");

    Ok(())
}

/// An `ObjectStore` that makes the *first* create-PUT of a `.seg` object
/// ambiguous: the write lands durably in the inner store, then the call returns
/// a transport error as if the response were lost. Every other call (later seg
/// PUTs, gets, lists, the topic-metadata `.json` writes) delegates unchanged.
/// Drives the #89 adoption path.
#[derive(Clone)]
struct AmbiguousSegmentPut<O> {
    inner: O,
    armed: Arc<AtomicBool>,
}

impl<O> AmbiguousSegmentPut<O> {
    fn new(inner: O) -> Self {
        Self {
            inner,
            armed: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl<O> Debug for AmbiguousSegmentPut<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmbiguousSegmentPut").finish()
    }
}

impl<O> Display for AmbiguousSegmentPut<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmbiguousSegmentPut").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for AmbiguousSegmentPut<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        let segment_create =
            matches!(opts.mode, PutMode::Create) && location.as_ref().ends_with(".seg");
        if segment_create && self.armed.swap(false, Relaxed) {
            // Land the write, then simulate a lost response.
            _ = self.inner.put_opts(location, payload, opts).await?;
            return Err(object_store::Error::Generic {
                store: "ambiguous",
                source: "simulated lost PUT response".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// #90: under the leaseless arbiter a backfill-class batch on a fresh sub-stream
/// folds into the segment path — the single offset authority — instead of the
/// #62 legacy per-topic bypass. No `records/` object is written (so the per-topic
/// LIST/GET residuals, #75, never engage), and offsets stay dense and readable.
#[tokio::test]
async fn leaseless_backfill_folds_into_segments() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true);
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    // A backfill-class batch (>= PREFIX_BACKFILL_MIN_RECORDS) on a fresh
    // sub-stream: the leaseless path routes it through the segment buffer.
    assert_eq!(0, store.produce(None, &tp, batch(1000)?).await?);

    assert_eq!(
        vec![segment_path(0)],
        segments(&bucket).await,
        "backfill landed as a segment"
    );
    assert!(
        legacy_records(&bucket, topic).await.is_empty(),
        "leaseless backfill must not write a legacy records/ object"
    );

    let fetched = fetch_from(&store, &tp, 0).await?;
    let total: i64 = fetched
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(
        1000, total,
        "all backfill records are readable from the segment"
    );

    Ok(())
}

/// #90 gating: on the pre-SCOS *lease* path the #62 backfill bypass is preserved
/// (single-broker only — a multi-replica lease deployment hits the #70 fence
/// storm), so a backfill-class batch on a fresh sub-stream still takes the
/// legacy per-topic create path — no segment.
#[tokio::test]
async fn lease_backfill_keeps_legacy_bypass() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &tp, batch(1000)?).await?);

    assert!(
        segments(&bucket).await.is_empty(),
        "the lease-path bypass writes no segment"
    );
    assert_eq!(
        1,
        legacy_records(&bucket, topic).await.len(),
        "the lease-path bypass writes a legacy records/ object"
    );

    Ok(())
}

/// #90 adaptive coalescing: a buffer that has seen a backfill-class batch relaxes
/// its flush triggers to backfill floors (byte floor >= 32 MiB, batch count past
/// the record cap so it never fires first), leaving the record cap as the
/// limiter — so a folded-in snapshot coalesces into a few large segments rather
/// than one per batch. Steady state keeps the tight configured triggers.
#[test]
fn backfill_relaxes_flush_thresholds() {
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    assert_eq!(
        (DynoStore::COALESCE_BATCHES, DynoStore::COALESCE_BYTES),
        store.flush_thresholds(false),
        "steady state uses the configured triggers"
    );

    let (batches, bytes) = store.flush_thresholds(true);
    assert!(
        bytes >= DynoStore::BACKFILL_COALESCE_BYTES,
        "backfill raises the byte floor to >= 32 MiB"
    );
    assert!(
        batches as i64 >= DynoStore::COALESCE_MAX_RECORDS,
        "backfill count trigger never fires before the record cap"
    );
}

/// #89: an ambiguous create-PUT — our segment landed durably but the response
/// was lost — is adopted via the footer nonce rather than blind-retried at the
/// next sequence. The produce still succeeds with the assigned offset, and the
/// log holds the batch exactly once: a single segment at seq 0, no double-write
/// at seq 1.
#[tokio::test]
async fn leaseless_ambiguous_put_is_adopted_via_nonce() -> Result<(), Error> {
    let inner = InMemory::new();
    let fault = AmbiguousSegmentPut::new(inner.clone());
    let armed = fault.armed.clone();
    let store = DynoStore::new(CLUSTER, NODE, fault)
        .prefix_coalesce(true)
        .prefix_leaseless(true);
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    // The flush's first seg PUT lands then errors; the ambiguous arm probes the
    // footer at the candidate, matches our nonce, and adopts the create.
    let offset = store
        .produce(None, &tp, idempotent_batch(11, 0, 0, 3)?)
        .await?;
    assert_eq!(0, offset, "adopted create keeps the assigned base offset");

    // Guard against a false positive: the ambiguous fault must actually have
    // fired (a normal, no-error flush would also leave one segment).
    assert!(
        !armed.load(Relaxed),
        "the ambiguous-PUT fault never triggered — test would not exercise #89"
    );

    // Exactly one segment — the create was adopted at seq 0, not repeated at 1.
    let segs = segments(&inner).await;
    assert_eq!(
        vec![segment_path(0)],
        segs,
        "ambiguous PUT must be adopted, not re-written at the next sequence"
    );

    // And the log holds the three records exactly once.
    let fetched = fetch_from(&store, &tp, 0).await?;
    let total: i64 = fetched
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(3, total, "no double-write: the batch is present once");

    Ok(())
}

/// #92: the first leaseless flush of a prefix seeds a durable era epoch of
/// `max(lease epoch, max footer epoch) + 1` (never 0) and stamps it as the
/// segment's `writer_epoch`. With a pre-cutover lease at epoch 5, the leaseless
/// segment carries epoch 6 — strictly above the lease era, so a straggler can
/// never win the overlap tie-break and erase acked data.
#[tokio::test]
async fn leaseless_segment_stamps_seeded_era_above_lease() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true);
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    // Pre-cutover lease-era state.
    write_lease(&bucket, &store, PREFIX, 5).await;

    assert_eq!(0, store.produce(None, &tp, batch(3)?).await?);

    let footer = footer_of(&bucket, &store, &segment_path(0)).await;
    assert_eq!(
        6, footer.writer_epoch,
        "era = max(lease 5, footer 0) + 1, stamped into the segment"
    );
    assert_eq!(
        Some(6),
        read_era(&bucket, &store, PREFIX).await,
        "era is persisted durably"
    );

    Ok(())
}

/// #92: a brand-new prefix (no lease, no segments) seeds era 1 — never 0, so a
/// leaseless segment always out-epochs a legacy `writer_epoch: 0` footer — and
/// the seeded value is stable and shared: a second call and a fresh process
/// (cold cache) both read the same durable era.
#[tokio::test]
async fn era_seed_defaults_to_one_and_is_stable() -> Result<(), Error> {
    let bucket = InMemory::new();
    let a = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true);

    assert_eq!(
        1,
        a.seed_era_epoch(PREFIX).await?,
        "fresh prefix seeds era 1"
    );
    assert_eq!(
        1,
        a.seed_era_epoch(PREFIX).await?,
        "stable within a process"
    );

    // A fresh process (cold in-memory cache) reads the durable value, not a
    // re-seed — the era is a constant for the whole leaseless regime.
    let b = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true);
    assert_eq!(1, b.seed_era_epoch(PREFIX).await?, "shared across replicas");

    Ok(())
}

/// #92 rollback: rewriting `lease.json` above the seeded era lets a restarted
/// lease-holder's next acquire (epoch + 1) out-epoch every leaseless-era
/// segment. After seeding era 6 (lease 5), rollback writes a lease epoch of 7.
#[tokio::test]
async fn rollback_rewrites_lease_above_era() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true);

    write_lease(&bucket, &store, PREFIX, 5).await;
    assert_eq!(6, store.seed_era_epoch(PREFIX).await?);

    let rolled = store.rollback_prefix_to_lease(PREFIX).await?;
    assert_eq!(7, rolled, "lease rewritten to era + 1");
    assert_eq!(
        7,
        store.read_lease_epoch(PREFIX).await?,
        "durable lease epoch now exceeds the era"
    );

    Ok(())
}

fn at_ms(ms: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(ms)
}

/// #105: `offsetsForTimes` (ListOffset::Timestamp) on a pure-segment topic is
/// resolved from the footer index — the earliest segment whose newest record
/// timestamp is at/after the target — instead of scanning the (empty) legacy
/// `records/` prefix and wrongly returning offset 0. A target past every segment
/// returns no offset (`None` → -1), per Kafka semantics.
#[tokio::test]
async fn list_offsets_by_timestamp_resolves_from_segments() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true);
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    // Three single-record batches, each its own segment (one flush per produce),
    // with strictly increasing timestamps → segments at offsets 0,1,2.
    assert_eq!(0, store.produce(None, &tp, batch_at(1, 1000)?).await?);
    assert_eq!(1, store.produce(None, &tp, batch_at(1, 2000)?).await?);
    assert_eq!(2, store.produce(None, &tp, batch_at(1, 3000)?).await?);

    let ask = |ts: u64| {
        let store = &store;
        let tp = tp.clone();
        async move {
            store
                .list_offsets(
                    IsolationLevel::ReadUncommitted,
                    &[(tp, ListOffset::Timestamp(at_ms(ts)))],
                )
                .await
        }
    };

    // Before everything → first segment (offset 0).
    assert_eq!(Some(0), ask(500).await?[0].1.offset);
    // Between seg0 and seg1 → first segment at/after 1500 is seg1 (offset 1).
    assert_eq!(Some(1), ask(1500).await?[0].1.offset);
    // Exactly the tail timestamp → the tail segment (offset 2).
    assert_eq!(Some(2), ask(3000).await?[0].1.offset);
    // After every record → no offset (regression guard: must NOT be 0).
    assert_eq!(None, ask(4000).await?[0].1.offset);

    Ok(())
}

/// An `ObjectStore` that fails the footer GET of any segment whose sequence is
/// `>= fail_from_seq` (an `AtomicU64`, raisable to disarm). Drives the #105
/// incremental-commit test: a cold index build that errors partway must still
/// have committed the footers it read before the error.
#[derive(Clone)]
struct FailSegmentFooterFrom<O> {
    inner: O,
    fail_from_seq: Arc<AtomicU64>,
}

impl<O> Debug for FailSegmentFooterFrom<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FailSegmentFooterFrom").finish()
    }
}

impl<O> Display for FailSegmentFooterFrom<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FailSegmentFooterFrom").finish()
    }
}

fn seg_seq_of(location: &Path) -> Option<u64> {
    let name = location.parts().next_back()?;
    let name = name.as_ref();
    name.strip_suffix(".seg")
        .filter(|seq| seq.len() >= 20)
        .and_then(|seq| u64::from_str(&seq[0..20]).ok())
}

#[async_trait]
impl<O> ObjectStore for FailSegmentFooterFrom<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        if let Some(seq) = seg_seq_of(location)
            && seq >= self.fail_from_seq.load(Relaxed)
        {
            return Err(object_store::Error::Generic {
                store: "fail-footer",
                source: format!("injected footer failure at seq {seq}").into(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// #105: a cold prefix-index build commits footers **incrementally**, so a build
/// that errors partway (a client abandoning its request, modelled here as a
/// footer GET failure) still caches the footers it read — the index warms across
/// attempts instead of restarting from zero (the "sustained, not decaying"
/// stall). The retry only reads the segments still missing.
#[tokio::test]
async fn prefix_index_cold_build_commits_incrementally() -> Result<(), Error> {
    let inner = InMemory::new();

    // Writer populates 6 segments (0..=5) under one prefix.
    {
        let writer = DynoStore::new(CLUSTER, NODE, inner.clone())
            .prefix_coalesce(true)
            .prefix_leaseless(true);
        let topic = "org.env.conn.tab_a";
        create_topic(&writer, topic).await?;
        let tp = Topition::new(topic, 0);
        for _ in 0..6 {
            _ = writer.produce(None, &tp, batch(1)?).await?;
        }
        assert_eq!(
            6,
            segments(&inner).await.len(),
            "writer laid down 6 segments"
        );
    }

    // A fresh (cold-index) reader whose footer GETs fail from seq 3 onward.
    let fail_from_seq = Arc::new(AtomicU64::new(3));
    let reader = DynoStore::new(
        CLUSTER,
        NODE,
        FailSegmentFooterFrom {
            inner: inner.clone(),
            fail_from_seq: fail_from_seq.clone(),
        },
    )
    .prefix_coalesce(true)
    .prefix_leaseless(true);

    let cached = |store: &DynoStore| {
        store
            .prefix_index
            .lock()
            .unwrap()
            .get(PREFIX)
            .map(|entry| entry.segments.len())
            .unwrap_or(0)
    };

    // The cold build errors at seq 3 — but seq 0,1,2 were read and committed.
    assert!(
        reader.refresh_prefix_index(PREFIX).await.is_err(),
        "cold build should surface the injected footer failure"
    );
    assert_eq!(
        3,
        cached(&reader),
        "footers read before the failure are committed (incremental, not all-or-nothing)"
    );

    // Disarm the fault; the retry reads only the still-missing seq 3,4,5.
    fail_from_seq.store(u64::MAX, Relaxed);
    reader.refresh_prefix_index(PREFIX).await?;
    assert_eq!(
        6,
        cached(&reader),
        "retry completes the index — warms across attempts"
    );

    Ok(())
}

/// Stateless maintenance scheduling (#126): a maintainer that claims a prefix
/// stamps the compaction lease's `maintained_at_ms`; within the recency window
/// it (and any peer) skips the prefix, and past the window it is claimed again —
/// no per-replica identity, no membership, coordinator-free.
#[tokio::test]
async fn maintenance_claim_recency_skips_then_reclaims() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true)
        .coalesce_tuning(CoalesceTuning {
            maintenance_recency: Some(Duration::from_secs(60)),
            ..Default::default()
        });
    create_topic(&store, "org.env.conn.tab_a").await?;

    let t = now_ms();
    let first = store.claim_maintenance_prefixes(t).await?;
    assert!(first.contains(PREFIX), "first claim wins the prefix");

    let again = store.claim_maintenance_prefixes(t).await?;
    assert!(
        !again.contains(PREFIX),
        "a prefix maintained within the recency window is skipped"
    );

    let later = store.claim_maintenance_prefixes(t + 120_000).await?;
    assert!(
        later.contains(PREFIX),
        "past the recency window the prefix is claimed again"
    );

    Ok(())
}

/// Cross-replica: a peer maintainer skips a prefix another replica maintained
/// within the window, learned purely from the durable lease stamp — no shared
/// membership, no coordination (#126). This is how N stateless maintainers
/// partition the work without duplicating it.
#[tokio::test]
async fn maintenance_claim_peer_skips_recently_maintained_prefix() -> Result<(), Error> {
    let bucket = InMemory::new();
    let scheduler = |bucket: InMemory| {
        DynoStore::new(CLUSTER, NODE, bucket)
            .prefix_coalesce(true)
            .prefix_leaseless(true)
            .coalesce_tuning(CoalesceTuning {
                maintenance_recency: Some(Duration::from_secs(60)),
                ..Default::default()
            })
    };
    let a = scheduler(bucket.clone());
    let b = scheduler(bucket.clone());
    create_topic(&a, "org.env.conn.tab_a").await?;

    let t = now_ms();
    assert!(
        a.claim_maintenance_prefixes(t).await?.contains(PREFIX),
        "A claims the prefix"
    );
    assert!(
        !b.claim_maintenance_prefixes(t).await?.contains(PREFIX),
        "B skips it — A maintained it within the window (durable stamp, no coordination)"
    );

    Ok(())
}

/// An `ObjectStore` that, while `armed`, fails every *segment* create-CAS
/// (`PutMode::Create` on a `*.seg` key) with `AlreadyExists` — modelling a hot
/// prefix whose tail sequence is perpetually contended (peers winning it, or S3
/// throttling slowing our PUTs) so a leaseless flush burns its whole retry
/// budget. Every other operation, and non-segment PUTs (e.g. `era.json`),
/// delegate to the inner store.
#[derive(Clone)]
struct ContendSegmentCreate<O> {
    inner: O,
    armed: Arc<AtomicBool>,
}

impl<O> Debug for ContendSegmentCreate<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContendSegmentCreate").finish()
    }
}

impl<O> Display for ContendSegmentCreate<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContendSegmentCreate").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for ContendSegmentCreate<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if self.armed.load(Relaxed)
            && matches!(opts.mode, PutMode::Create)
            && seg_seq_of(location).is_some()
        {
            return Err(object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: "injected create-CAS contention".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// #129: a leaseless flush that exhausts its create-CAS retry budget (a hot
/// prefix under perpetual sequence contention / S3 throttling) must fail with a
/// **retriable** code, so the client backs off and retries — not the fatal
/// `UnknownServerError` (-1) that makes clients drop the batch and drove the
/// downstream connector OOM loop. The condition is transient and self-heals.
#[tokio::test]
async fn leaseless_flush_exhaustion_is_retriable_not_fatal() -> Result<(), Error> {
    let inner = InMemory::new();
    let armed = Arc::new(AtomicBool::new(false));
    let store = DynoStore::new(
        CLUSTER,
        NODE,
        ContendSegmentCreate {
            inner: inner.clone(),
            armed: armed.clone(),
        },
    )
    .prefix_coalesce(true)
    .prefix_leaseless(true);
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    // Baseline: a normal produce succeeds and lays down a segment.
    _ = store.produce(None, &tp, batch(1)?).await?;

    // Arm perpetual contention on the segment tail → the flush burns all its
    // create-CAS attempts and hits the exhaustion terminal.
    armed.store(true, Relaxed);
    let error = store
        .produce(None, &tp, batch(1)?)
        .await
        .expect_err("flush must fail when the sequence is perpetually contended");

    // The fix: exhaustion is retriable, not fatal `-1`.
    assert!(
        matches!(error, Error::Api(ErrorCode::KafkaStorageError)),
        "exhaustion must be a retriable KafkaStorageError, got {error:?}"
    );
    assert!(
        !matches!(error, Error::Api(ErrorCode::UnknownServerError)),
        "must not be the fatal UnknownServerError that makes clients drop the batch"
    );

    // Self-heals: once contention clears, produce succeeds again.
    armed.store(false, Relaxed);
    _ = store.produce(None, &tp, batch(1)?).await?;

    Ok(())
}

/// #130 mitigation: the shipped default merged-segment target is 16 MiB, not a
/// larger size. While compaction writes into the producer tail create-CAS
/// namespace, a larger target multiplies the S3 write amplification and request
/// pressure of a lost create race, so the default is kept modest. A deployment
/// can still raise it via `prefix_compact_target_bytes`. Guards the default
/// against an accidental bump.
#[tokio::test]
async fn default_compaction_target_bytes_is_modest() {
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new()).prefix_coalesce(true);
    assert_eq!(16 << 20, store.prefix_compact_target_bytes);
}
