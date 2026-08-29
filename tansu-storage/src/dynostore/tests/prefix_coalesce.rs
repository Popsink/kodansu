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
    collections::BTreeMap,
    fmt::{self, Debug, Display},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    memory::InMemory, path::Path,
};
use tansu_sans_io::{
    BatchAttribute, ErrorCode, IsolationLevel, ListOffset,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage, TopicId, Topition, TxnAddPartitionsRequest,
    dynostore::{
        CoalesceTuning, CompactRun, DynoStore, Era, PrefixLease, SegmentFooter, ServedEnd,
        SubstreamEntry, TxnProduceOffset,
    },
    storage_error_code,
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

/// The live segments of an arbitrary connector prefix (for the multi-prefix
/// maintenance tests, which span more than [`PREFIX`]).
async fn segments_of(bucket: &InMemory, prefix: &str) -> Vec<Path> {
    let listing = Path::from(format!("clusters/{CLUSTER}/prefixes/{prefix}/segments/"));
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

async fn footer_of(bucket: &InMemory, location: &Path) -> SegmentFooter {
    let bytes = bucket
        .get(location)
        .await
        .expect("get segment")
        .bytes()
        .await
        .expect("segment bytes");

    DynoStore::decode_segment_footer(&bytes)
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

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

    let footer = footer_of(&bucket, &segments[0]).await;
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

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
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_lease_ttl(ttl);
        create_topic(&store, topic_a).await?;
        assert_eq!(0, store.produce(None, &a, batch(3)?).await?);
        assert_eq!(3, store.produce(None, &a, batch(2)?).await?);
    }

    // The lease lapses, so a takeover is allowed.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // A fresh process on the same bucket: empty in-memory counters. The next
    // produce must continue at 5, recovered from the tail segment footer, and
    // land in a third segment (sequence recovered from the tail listing).
    let restarted = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let resumed = restarted.produce(None, &a, batch(1)?).await?;
    assert_eq!(5, resumed, "resume past the footer end offset, no reuse");
    assert_eq!(3, segments(&bucket).await.len());

    Ok(())
}

/// #246: a topic re-created under the same name must not inherit its
/// predecessor's records from a shared segment.
///
/// `delete_topic` cannot remove a sub-stream's slices inside a shared segment —
/// a segment multiplexes many topics, is immutable, and is reclaimed whole only
/// once every sub-stream in it is past retention (#61). Slices are located by
/// `(topic, partition)` NAME in the footer, so a successor used to find its
/// predecessor's slices, fold its offsets from them, and serve those records as
/// its own: a `DeleteTopics` that reads as "the data is gone" left it readable,
/// silently, for as long as a segment holding a slice survived.
///
/// The sibling topic here is load-bearing: it keeps the shared segment alive
/// after the delete, which is exactly the window the issue describes.
#[tokio::test]
async fn a_recreated_topic_inherits_nothing_from_a_shared_segment() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic_a = "org.env.conn.tab_a";
    let topic_b = "org.env.conn.tab_b";
    create_topic(&store, topic_a).await?;
    create_topic(&store, topic_b).await?;

    let a = Topition::new(topic_a, 0);
    let b = Topition::new(topic_b, 0);

    // One shared segment holding both topics' slices.
    let (pa, pb) = tokio::join!(
        store.produce(None, &a, batch(3)?),
        store.produce(None, &b, batch(2)?),
    );
    _ = pa?;
    _ = pb?;

    assert_eq!(1, segments(&bucket).await.len(), "one shared segment");

    assert_eq!(
        ErrorCode::None,
        store.delete_topic(&TopicId::Name(topic_a.into())).await?,
    );

    // The segment survives the delete: the sibling still needs it.
    assert_eq!(
        1,
        segments(&bucket).await.len(),
        "segment kept by the sibling"
    );

    // Re-create under the same name, and read it with a store that has no
    // in-process state from before the delete — a successor on another replica,
    // or this one after a restart.
    create_topic(&store, topic_a).await?;
    let successor = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let fetched = fetch_from(&successor, &a, 0).await?;
    let records: i64 = fetched
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(0, records, "a successor must inherit no records");

    let earliest = successor
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(a.clone(), ListOffset::Earliest)],
        )
        .await?;
    assert_eq!(
        Some(3),
        earliest[0].1.offset,
        "log start is the predecessor's end, not 0: the records are hidden, not gone",
    );

    // The sibling is untouched — the tombstone is per sub-stream, and the shared
    // segment was never rewritten.
    let sibling = fetch_from(&successor, &b, 0).await?;
    let sibling_records: i64 = sibling
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum();
    assert_eq!(2, sibling_records, "the sibling keeps its records");

    Ok(())
}

/// A fetch whose region comes back short of its extent returns, bounded, and
/// answers `CORRUPT_MESSAGE` — never `OffsetOutOfRange` (#302, #397).
///
/// This test has been through both wrong answers. It first asserted
/// `OffsetOutOfRange`; that shipped, fired on 61 healthy topics in 3 minutes and
/// reset live consumers through `auto.offset.reset`. It then asserted empty and
/// error-free, which is what #397 came back on: the truncated arm returned the
/// batches it could decode and dropped the rest of the region, and a consumer of
/// `*.connect.ibmi-offsets` served part of an offset map with nothing in its own
/// logs to say so is a connector resuming from holes.
///
/// The evidence that settles which is right is that
/// `tansu_prefix_segment_regions_truncated` was flat zero across a whole
/// pre-`alpha.2` week and only moved when known-damaged objects were re-read.
/// This is not a condition a healthy fleet produces, so it is not one to absorb
/// silently. And segments are immutable and created atomically, so a ranged GET
/// returning fewer bytes than the extent means the entry and the object
/// disagree — which the reader now resolves against the object's own trailer
/// before answering (`resolve_short_region`). Here the trailer agrees with the
/// index, so the object really is short and there is nowhere else to look.
///
/// `CORRUPT_MESSAGE` is what keeps #302 fixed: it is per-partition damage
/// (#388), and it is not the error code `auto.offset.reset` acts on. What it is
/// not is silence.
///
/// The *neighbouring* state — a floor above the surviving segment tail — is
/// counted (`tansu_watermark_above_segment_tail`, #338) and, once certified
/// (`Watermark::served`) either by the expiry that created it or by
/// `certify_prefix_served_ends` reconciling one it never saw, its fetches
/// answer `OffsetOutOfRange` (#290). That is a different state from this one:
/// there the index does not cover the offset, here it claims to and cannot
/// deliver.
///
/// The property from #290 still holds and is still pinned: the fetch
/// **returns**. Its complaint was a fetch that never completed at all, which let
/// 25.6M records of advertised backlog sit unreadable with no signal anywhere
/// while `poll()` starved 250 healthy partitions on the same consumer.
#[tokio::test]
async fn a_fetch_whose_region_is_short_of_its_extent_answers_corrupt() -> Result<(), Error> {
    /// Once armed, truncates every segment record read to fewer bytes than a
    /// batch header needs.
    ///
    /// `decode_frame` wants `size_of::<i64>() + size_of::<i32>()` bytes before
    /// it will consider a batch at all, so a shorter region decodes to no
    /// batches and the read comes back empty — while the index goes on claiming
    /// the offsets. That is the pairing under test: metadata saying the records
    /// are here, against a read that produces none of them, which is what #290's
    /// "present but unreachable" looks like from the read path.
    ///
    /// Footer reads use a suffix range and are left alone, so the index is built
    /// normally and keeps its claim.
    #[derive(Clone)]
    struct TruncateRecordReads<O> {
        inner: O,
        armed: Arc<AtomicBool>,
    }

    impl<O> Debug for TruncateRecordReads<O> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("TruncateRecordReads").finish()
        }
    }

    impl<O> Display for TruncateRecordReads<O> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("TruncateRecordReads").finish()
        }
    }

    #[async_trait]
    impl<O> ObjectStore for TruncateRecordReads<O>
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
            // Only bounded record-range reads of a segment are touched. Footer
            // reads use a suffix range, so the index is built normally.
            if let Some(GetRange::Bounded(ref range)) = options.range
                && self.armed.load(Relaxed)
                && location.as_ref().contains("/segments/")
            {
                // Short of a batch header, and non-empty so the range stays
                // valid: `decode_frame` stops before the first batch.
                let mut options = options.clone();
                options.range = Some(GetRange::Bounded(range.start..range.start + 4));
                return self.inner.get_opts(location, options).await;
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

    let bucket = InMemory::new();
    let armed = Arc::new(AtomicBool::new(false));

    let store = DynoStore::new(
        CLUSTER,
        NODE,
        TruncateRecordReads {
            inner: bucket.clone(),
            armed: armed.clone(),
        },
    );

    let topic = "org.env.conn.tab_orphaned";
    create_topic(&store, topic).await?;

    let tp = Topition::new(topic, 0);

    // Real records in a real segment, with a real index entry claiming [0, 4).
    assert_eq!(0, store.produce(None, &tp, batch(4)?).await?);
    assert_eq!(4, store.high_watermark(&tp).await?);
    assert_eq!(
        4,
        record_count(&fetch_from(&store, &tp, 0).await?),
        "the partition is healthy before the region stops producing",
    );

    // The bytes behind the index entry stop yielding batches. The entry itself
    // is untouched, so the broker goes on advertising [0, 4).
    armed.store(true, Relaxed);

    // Head and tail, as the report probed them.
    for offset in [0, 3] {
        let outcome =
            tokio::time::timeout(Duration::from_secs(30), fetch_from(&store, &tp, offset))
                .await
                .map_err(|_| {
                    Error::Message(format!(
                        "fetch at offset {offset} never returned: a consumer here polls forever"
                    ))
                })?;

        let error = outcome.expect_err("a region short of its extent is damage");

        assert_eq!(
            ErrorCode::CorruptMessage,
            storage_error_code(&error),
            "offset {offset} must answer CORRUPT_MESSAGE — damage on this partition \
             alone, and not the code auto.offset.reset acts on (#302); got {error:?}",
        );
    }

    Ok(())
}

/// A caught-up consumer is never told its offset is out of range (#302).
///
/// **This is the test whose absence let the bug reach a cluster.** Its sibling
/// above pinned that a simulated damaged region produced `OffsetOutOfRange`;
/// nothing pinned the converse, so an answer that fired on healthy partitions
/// looked correct all the way to production, where it reset consumers on 61
/// topics in 3 minutes at 9 to 21859 offsets below the high watermark.
///
/// The tail is where it happened, so the tail is what this walks: the last
/// offset, and every offset in the log, on a partition with nothing wrong with
/// it.
#[tokio::test]
async fn a_caught_up_consumer_is_never_told_it_is_out_of_range() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.tab_healthy";
    create_topic(&store, topic).await?;

    let tp = Topition::new(topic, 0);

    // Two flushes, so the sub-stream spans more than one segment and the tail
    // segment is not also the first.
    assert_eq!(0, store.produce(None, &tp, batch(4)?).await?);
    assert_eq!(4, store.produce(None, &tp, batch(3)?).await?);

    let high_watermark = store.high_watermark(&tp).await?;
    assert_eq!(7, high_watermark);

    // Every offset in the log, tail included. None may error, and each must
    // reach the tail.
    for offset in 0..high_watermark {
        let fetched = fetch_from(&store, &tp, offset).await.map_err(|err| {
            Error::Message(format!(
                "offset {offset} of {high_watermark} is servable, but the fetch errored: \
                 {err:?} — this is what resets a healthy consumer"
            ))
        })?;

        assert!(
            !fetched.is_empty(),
            "offset {offset} of {high_watermark} must serve records",
        );

        // Whole batches are returned, so the answer may begin *before* the
        // requested offset — the consumer trims to its own position, as it does
        // against Kafka. Asserting an exact record count here would pin
        // batch-splitting the broker deliberately does not do. What matters is
        // that the tail is reachable from every offset.
        let last_offset = fetched
            .iter()
            .map(|batch| batch.base_offset + batch.last_offset_delta as i64)
            .max()
            .expect("a non-empty answer has a last offset");

        assert_eq!(
            high_watermark - 1,
            last_offset,
            "offset {offset} must reach the tail",
        );
    }

    // And the tail itself: at the high watermark there is nothing yet, which is
    // an empty answer, never an error.
    assert!(
        fetch_from(&store, &tp, high_watermark).await?.is_empty(),
        "a consumer sitting exactly at the tail has caught up, not run out of range",
    );

    Ok(())
}

/// A fully expired log answers `OffsetOutOfRange` below its end (#337).
///
/// **This reverses what this test asserted, deliberately.** It used to demand
/// empty, on the reasoning that "a consumer of an aged-out partition that gets an
/// out-of-range error resets off a log that is merely old". Two things retire that
/// reasoning:
///
/// - **#299 changed the premise.** A drained partition now reports
///   `log_start == log_end`, so `auto.offset.reset=earliest` moves the consumer to
///   the log start, which *is* the end. Nothing available is skipped, because
///   nothing is available. The old fear assumed `log_start` still lied as `0`,
///   which is what made a reset look like data loss.
/// - **Production measured the alternative.** Answering empty reads to a consumer
///   as "caught up, nothing new", so it polls again, forever. On one connector that
///   stranded 77 partitions, 15 of 16 members holding at least one, and delivered
///   zero records for days — because `poll()` covers the whole assignment, so one
///   such partition freezes every healthy partition beside it.
///
/// What still holds is the guard that actually protects against the #303 incident:
/// `a_caught_up_consumer_is_never_told_it_is_out_of_range`, which passes unchanged.
/// A consumer *at* the end is caught up, not out of range — that distinction is what
/// reset 61 topics when it was got wrong, and it is untouched here.
///
/// Narrow on purpose: the answer is keyed on there being **no segment at all**, not
/// on the offset being below `log_start`. With segments present a low offset is
/// already served the records above it, and the truncation floor is deliberately a
/// skip rather than an error (#176).
#[tokio::test]
async fn a_fully_expired_log_is_out_of_range_below_its_end() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.tab_expired";
    create_topic(&store, topic).await?;

    let tp = Topition::new(topic, 0);

    // Metadata says records exist; no segment was ever written, so none does.
    // This is the shape retention leaves behind once everything has expired.
    _ = bucket
        .put_opts(
            &Path::from(format!(
                "clusters/{CLUSTER}/topics/{topic}/partitions/{:0>10}/watermark.json",
                0
            )),
            PutPayload::from_static(br#"{"high":3024895}"#),
            PutOptions::default(),
        )
        .await?;

    let reader = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert_eq!(3024895, reader.high_watermark(&tp).await?);

    // The log start is the log end, so there is no live position below it. That is
    // the property that makes the reset harmless.
    assert_eq!(
        3024895,
        reader.log_start(&tp, 3024895).await?,
        "an expired log starts where it ends (#299)"
    );

    // The committed offset of a group that stopped before the data expired — the
    // production shape, `committed < log_start`.
    assert!(
        matches!(
            fetch_from(&reader, &tp, 868_514).await,
            Err(Error::Api(ErrorCode::OffsetOutOfRange))
        ),
        "a committed offset below the start of an empty log must be told so, not \
         answered empty forever (#337)"
    );

    // And from the bottom, which is where a reset-to-earliest lands a client that
    // never committed.
    assert!(matches!(
        fetch_from(&reader, &tp, 0).await,
        Err(Error::Api(ErrorCode::OffsetOutOfRange))
    ));

    // At the end there is nothing to report: this consumer has caught up with an
    // empty log, which is the case #303's incident was about.
    assert!(
        fetch_from(&reader, &tp, 3024895).await?.is_empty(),
        "at the end is caught up, not out of range"
    );

    Ok(())
}

/// A partition with no surviving segment reports a log that starts where it
/// ends, through both offset paths (#290).
///
/// This is the other half of the report, and the half a detector cannot reach:
/// production advertised
/// `LOG-START-OFFSET=0 / LOG-END-OFFSET=3024895` on a prefix that served
/// nothing at any offset, so every lag computation downstream inherited 3M of
/// backlog that no consumer could retire. The start offset was the false
/// statement — the broker does not hold a record at 0 — and correcting it is
/// what makes the gap visible through ordinary metadata instead of by probing
/// offsets by hand.
///
/// It is also what tells an empty log from a damaged one at all: before this,
/// both said "starts at 0, ends at N" while holding nothing.
#[tokio::test]
async fn a_partition_with_no_segment_starts_where_it_ends() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.tab_unbacked";
    create_topic(&store, topic).await?;

    let tp = Topition::new(topic, 0);

    _ = bucket
        .put_opts(
            &Path::from(format!(
                "clusters/{CLUSTER}/topics/{topic}/partitions/{:0>10}/watermark.json",
                0
            )),
            PutPayload::from_static(br#"{"high":3024895}"#),
            PutOptions::default(),
        )
        .await?;

    let reader = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert_eq!(3024895, reader.high_watermark(&tp).await?);

    // ListOffsets EARLIEST — the `LOG-START-OFFSET` a client actually prints,
    // and where the 3M of phantom lag came from.
    assert_eq!(
        3024895,
        earliest(&reader, &tp).await?,
        "an empty log's earliest offset is its end, not 0",
    );

    // Both offset-stage paths agree: read-uncommitted (the consumer hot path,
    // index-derived) and the transaction-aware one.
    let uncommitted = reader
        .offset_stage_at(&tp, IsolationLevel::ReadUncommitted)
        .await?;
    assert_eq!(uncommitted.high_watermark(), uncommitted.log_start());

    let committed = reader
        .offset_stage_at(&tp, IsolationLevel::ReadCommitted)
        .await?;
    assert_eq!(committed.high_watermark(), committed.log_start());

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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

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
        let writer = DynoStore::new(CLUSTER, NODE, bucket.clone());
        create_topic(&writer, topic_a).await?;
        assert_eq!(0, writer.produce(None, &a, batch(3)?).await?);
    }

    // Fresh reader: no cached hint, no lease — read path only.
    let reader = DynoStore::new(CLUSTER, NODE, bucket.clone());

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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
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

/// Many concurrent produces to one prefix, with a tiny flush threshold forcing
/// overlapping flush windows, must yield a gap-free, duplicate-free offset
/// sequence — the per-prefix flush lock serializes offset assignment (#1 fix).
#[tokio::test]
async fn concurrent_flushes_assign_unique_offsets() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

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

/// **#290, named.** Ordinary retention can leave a sub-stream advertising
/// offsets that no surviving segment holds, and a consumer parked in that gap
/// reads empty on every poll, forever, with no error on either side.
///
/// No damage is simulated here. Every step is the retention path working as
/// designed:
///
/// 1. `expire_prefix_segments` persists `watermark.high = max(current, tail)`
///    for each sub-stream losing segments, **write-ahead of the delete**, where
///    `tail` is computed from the *pre-delete* segment set
///    (`dynostore.rs:5690-5706`). That floor exists so cold recovery cannot
///    regress to 0 and reuse offsets (#61 review fix).
/// 2. Retention is keyed on **record timestamps**, not segment order, so the
///    segment holding the tail can expire while a lower one survives. Below,
///    the recent batch is produced first and the ancient batch second — which
///    is what a CDC backfill stamping source timestamps does routinely.
/// 3. `high_watermark` is `tail.max(watermark_floor).max(hint)`
///    (`dynostore.rs:2518-2520`), so the floor wins and the partition still
///    advertises the pre-delete tail.
/// 4. A fetch in `[surviving tail, floor)` passes the `offset < high_watermark`
///    gate (`:7975`), then every surviving entry is skipped by
///    `base_offset + record_count <= offset` (`:4195`), and the read returns
///    `Ok(vec![])` (`:4260`).
///
/// **This is why the #292 detector could never have seen it.** That check
/// demanded a covering index entry — `base <= offset < base + record_count` —
/// and in this state no entry covers the offset. The population it did fire on
/// (about ten a minute, #314) is therefore disjoint from this one: it required
/// the entry to exist, and here the entry is precisely what is missing.
///
/// #299 fixed the neighbouring case where *every* segment is gone, by reporting
/// a log that starts where it ends. It does not reach this one, because the log
/// is not empty — records at `[0, surviving tail)` are still there and still
/// served.
///
/// **Why the obvious fix does not work, established by trying it.** Splitting the
/// fold — log end from the segment tail, next-offset-to-assign from the floor —
/// makes this partition advertise 2 and closes the gap. It also breaks
/// `scaling::coalesced_latest_survives_peer_expiry_via_floor_certification`, and
/// that test is right: a peer replica can ack offsets this process never listed
/// (segments created *and* expired inside its blind window), leaving exactly
/// `floor > tail` with segments present. Ignoring the floor there regresses the
/// log end below offsets already acknowledged to a producer — worse than the wedge
/// it removes.
///
/// The two states are byte-identical locally: `floor > tail`, segments present,
/// either because a peer wrote offsets we have not seen or because retention
/// deleted the ones we had. A forced LIST would separate them, which is what the
/// #292 detector paid for and what the request-profile tests in `scaling` forbid on
/// this path.
///
/// So this is #290's point 2, demonstrated rather than argued: the read path has no
/// information that tells the two apart — which is why the fix is the durable
/// served-end certification (`Watermark::served`), written by the expiry itself
/// in the same CAS that raises the floor. Only that operation knows whether
/// `floor > tail` means "a peer acked offsets you have not listed" or "the
/// tail-holding segment is gone", and it is the single writer of both values.
///
/// With the certification in place the advertised end deliberately does NOT
/// move — the floor is the log end in Kafka's sense, the next offset to be
/// assigned, and lowering it under offsets a peer may have acked is the
/// regression `scaling::coalesced_latest_survives_peer_expiry_via_floor_certification`
/// exists to forbid. What changes is the fetch: a poll that found nothing,
/// whose offset lies inside the certified-dead gap, answers
/// `OFFSET_OUT_OF_RANGE` instead of empty — bounded, loud, and recoverable via
/// `auto.offset.reset`, exactly the #337 discipline extended from the empty
/// log to the mid-log gap.
#[tokio::test]
async fn retention_can_orphan_offsets_below_the_advertised_watermark() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    let recent = now_ms();
    let ancient = 1_000;

    // Segment 0 holds offsets [0, 2) with *recent* records; segment 1 holds
    // [2, 4) with *ancient* ones. Produced in that order deliberately: record
    // time need not follow segment order.
    _ = store.produce(None, &a, batch_at(2, recent)?).await?;
    _ = store.produce(None, &a, batch_at(2, ancient)?).await?;
    assert_eq!(2, segments(&bucket).await.len());
    assert_eq!(4, store.high_watermark(&a).await?);

    // Retention with a threshold between the two: the *tail-holding* segment is
    // the one that expires, and the lower one survives.
    let deleted = store.expire_prefix_segments(PREFIX, recent - 1_000).await?;
    assert_eq!(1, deleted, "exactly the ancient, tail-holding segment");
    assert_eq!(1, segments(&bucket).await.len());

    // The advertised end offset has not moved: the durable floor, written
    // write-ahead of the delete, still says 4. That is deliberate — the floor
    // is where the next record will be assigned, and regressing it under
    // offsets a peer may have acked is worse than the gap (see the doc above).
    assert_eq!(
        4,
        store.high_watermark(&a).await?,
        "the write-ahead floor keeps advertising the pre-delete tail"
    );

    // The expiry certified what it left behind: offsets [2, 4) are dead, by
    // the one writer that could know.
    assert_eq!(
        (4, Some(ServedEnd { end: 2, at_high: 4 })),
        store.persisted_watermark_bounds(&a).await?,
        "expiry must certify the surviving tail alongside the floor it raised"
    );

    // Offsets 0 and 1 are still served: the log is not empty, which is why
    // #299's "starts where it ends" never reached this case.
    assert!(
        !fetch_from(&store, &a, 0).await?.is_empty(),
        "the surviving segment still serves its own offsets"
    );

    // Offsets 2 and 3 are advertised and destroyed — and the fetch now says
    // so, instead of answering empty on every poll forever (#290). This is
    // what un-parks a consumer committed inside the gap: `auto.offset.reset`
    // moves it to a live position, and `none` fails loudly.
    for offset in [2, 3] {
        assert!(
            matches!(
                fetch_from(&store, &a, offset).await,
                Err(Error::Api(ErrorCode::OffsetOutOfRange))
            ),
            "offset {offset} is in the certified-dead gap and must answer OFFSET_OUT_OF_RANGE"
        );
    }

    // Fetching at the surviving tail itself is not an error: that is the
    // caught-up position for a consumer of what remains.
    assert!(fetch_from(&store, &a, 4).await?.is_empty());

    // A cold reader reaches the same answers, so this is a property of the
    // store and not of one process's memory: same advertised end, and the
    // same error once its read path has warmed the watermark cache.
    let cold = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert_eq!(4, cold.high_watermark(&a).await?);
    assert!(
        matches!(
            fetch_from(&cold, &a, 2).await,
            Err(Error::Api(ErrorCode::OffsetOutOfRange))
        ),
        "a cold replica must refuse the certified-dead gap too"
    );

    Ok(())
}

/// The honor condition that keeps the certified-gap error safe on a mixed
/// fleet (#290): a floor moved by a writer that did not re-certify — an older
/// binary's expiry round-trips `served` untouched through the catch-all while
/// raising `high` — invalidates the pair, and a fetch in the gap falls back
/// to answering empty. Erring on a stale pair would be the #292 failure over
/// again: resetting live consumers off a claim that no longer describes the
/// store.
#[tokio::test]
async fn a_stale_served_end_is_ignored_not_misread() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    let recent = now_ms();
    let ancient = 1_000;
    _ = store.produce(None, &a, batch_at(2, recent)?).await?;
    _ = store.produce(None, &a, batch_at(2, ancient)?).await?;

    // A certified expiry first: `served == {end: 2, at_high: 4}`.
    assert_eq!(
        1,
        store.expire_prefix_segments(PREFIX, recent - 1_000).await?
    );
    assert_eq!(
        (4, Some(ServedEnd { end: 2, at_high: 4 })),
        store.persisted_watermark_bounds(&a).await?
    );

    // An "old binary" raises the floor without re-certifying — exactly what a
    // pre-#290 `expire_prefix_segments` does to this object: `high` moves,
    // `served` rides along untouched.
    store
        .watermark(&a)?
        .with_mut(&store.object_store, |watermark| {
            watermark.high = Some(9);
            Ok(())
        })
        .await?;

    // A cold reader advertises the raised floor, and the stale pair no longer
    // certifies it: a fetch in the old gap answers empty — bounded, no error —
    // because nothing can say whether those offsets are destroyed or merely
    // unlisted here.
    let cold = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert_eq!(9, cold.high_watermark(&a).await?);
    assert!(
        fetch_from(&cold, &a, 2).await?.is_empty(),
        "a stale certification must degrade to empty, never to an error"
    );

    Ok(())
}

/// Lowering `retention.ms` must re-arm a prefix the oldest-retained hint had
/// marked skippable (#61, #49) — the segment analogue of
/// `retention.rs::lowered_retention_rebascules_to_scan`, which pins this only
/// for the legacy per-partition path.
///
/// The hint exists so a tick that cannot possibly find anything expirable skips
/// the LIST entirely. If it were a one-way ratchet, a prefix once judged "all
/// records newer than the threshold" would stay skipped for as long as it kept
/// being asked the same question — and *tightening* `retention.ms` would
/// silently stop reclaiming, with no error and no metric. The guard is a
/// comparison against the threshold, not a sticky flag, and this is what says
/// so.
#[tokio::test]
async fn lowered_retention_rescans_a_skipped_prefix() -> Result<(), Error> {
    const HOUR_MS: i64 = 60 * 60 * 1_000;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    let written = now_ms();
    assert_eq!(0, store.produce(None, &a, batch_at(2, written)?).await?);
    assert_eq!(1, segments(&bucket).await.len());

    // A generous retention: the record is newer than the threshold, so the scan
    // reclaims nothing — and records the oldest-retained hint on its way out.
    let generous = written - HOUR_MS;
    assert_eq!(
        0,
        store
            .expire_prefix_segments_if_due(PREFIX, generous)
            .await?,
    );

    // The hint now short-circuits the next tick at that same threshold.
    assert!(
        !store.prefix_maybe_expirable(PREFIX, generous)?,
        "a prefix whose oldest record is newer than the threshold must be skipped",
    );

    // Retention is lowered, so the threshold rises past the hint. The guard has
    // to flip back to scanning — and the scan must actually reclaim, not merely
    // be permitted to run.
    let tightened = written + HOUR_MS;
    assert!(
        store.prefix_maybe_expirable(PREFIX, tightened)?,
        "a threshold past the hint must re-arm the scan",
    );
    assert_eq!(
        1,
        store
            .expire_prefix_segments_if_due(PREFIX, tightened)
            .await?,
    );
    assert!(
        segments(&bucket).await.is_empty(),
        "the re-armed scan must delete the now-expirable segment",
    );

    Ok(())
}

/// A shared segment is kept while *any* of its sub-streams is still live (#61):
/// whole-segment expiry never drops a segment a live topic still needs.
#[tokio::test]
async fn keeps_a_segment_while_any_substream_is_live() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

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
    let holder = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let other = DynoStore::new(CLUSTER, NODE, bucket.clone());

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

/// Snapshot → streaming: a backfill (legacy objects) followed by CDC (segments)
/// keeps one continuous offset sequence with no gap/duplicate, and a fetch
/// stitches both (#62 handoff over #58 seam / #60 hybrid).
#[tokio::test]
async fn backfill_then_cdc_is_continuous() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        ..Default::default()
    });

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    // Backfill: one bulk batch of 1000 records, offsets [0, 1000). Since #177
    // it goes through the segment CAS like everything else — the #62 bypass
    // that sent it to a legacy `records/` object is gone (#90 widened the
    // byte threshold instead, so a bulk batch still flushes as ~its own
    // segment and keeps the 1-PUT parity the bypass gave).
    assert_eq!(0, store.produce(None, &a, batch(1_000)?).await?);

    // CDC steady state resumes: the small batch must continue at 1000 (no
    // gap/overlap at the snapshot→streaming seam). That seam is the property
    // this test exists for, and it is unchanged by both sides now being
    // segments.
    let cdc = store.produce(None, &a, batch(3)?).await?;
    assert_eq!(1000, cdc, "streaming continues from the backfill tail");
    assert!(
        legacy_records(&bucket, topic).await.is_empty(),
        "backfill no longer mints a legacy object",
    );

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
        let producer = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(tuning);
        create_topic(&producer, topic).await?;
        for _ in 0..4 {
            _ = producer.produce(None, &a, batch(1)?).await?;
        }
        assert_eq!(4, segments(&bucket).await.len());
    }

    // A fresh maintainer (cold index) runs the compaction pass.
    let maintainer = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(tuning);
    assert!(
        maintainer.maintain_prefix_segments(now_ms(), None).await?.1 > 0,
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
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
    assert_eq!(
        CompactRun::Merged(2),
        store.compact_prefix_segments(PREFIX).await?
    );

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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
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
    assert_eq!(
        CompactRun::Merged(2),
        store.compact_prefix_segments(PREFIX).await?
    );

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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
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

    _ = store.maintain_prefix_segments(now_ms(), None).await?;
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
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
    assert_eq!(CompactRun::Merged(4), merged);
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
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

    assert_eq!(
        CompactRun::Drained,
        store.compact_prefix_segments(PREFIX).await?
    );
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
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
    _ = store.maintain_prefix_segments(now_ms(), None).await?;
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
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
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
    let restarted = DynoStore::new(CLUSTER, NODE, bucket.clone());
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
    let restarted = DynoStore::new(CLUSTER, NODE, bucket.clone());
    _ = restarted.produce(None, &a, batch(1)?).await?;

    let seqs = segments(&bucket).await;
    assert!(
        !seqs.contains(&segment_path(0)),
        "freed seq 0 name must not be reused",
    );
    assert_eq!(vec![segment_path(2)], seqs);

    Ok(())
}

/// `retention.ms=-1` (retain forever) must keep every segment, not delete them
/// all (#61 review fix for the -1 parse).
#[tokio::test]
async fn retention_forever_keeps_segments() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
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

    let (deleted, _) = store.maintain_prefix_segments(now_ms(), None).await?;
    assert_eq!(0, deleted, "retain-forever deletes nothing");
    assert_eq!(1, segments(&bucket).await.len());

    Ok(())
}

/// Leaseless (#86): with no lease, two replicas append to the SAME sub-stream by
/// alternating, and the fold-before-claim step makes each observe the other's
/// segment before deriving its base — so offsets stay dense and contiguous with
/// no reuse. Segments are written v3 and read back correctly.
#[tokio::test]
async fn leaseless_alternating_writers_stay_contiguous() -> Result<(), Error> {
    let bucket = InMemory::new();
    let mk = || DynoStore::new(CLUSTER, NODE, bucket.clone());
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
    let mk = || DynoStore::new(CLUSTER, NODE, bucket.clone());
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
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
    let mk = || DynoStore::new(CLUSTER, NODE, bucket.clone());
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
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
    let store = DynoStore::new(CLUSTER, NODE, fault);
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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    // Pre-cutover lease-era state.
    write_lease(&bucket, &store, PREFIX, 5).await;

    assert_eq!(0, store.produce(None, &tp, batch(3)?).await?);

    let footer = footer_of(&bucket, &segment_path(0)).await;
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
    let a = DynoStore::new(CLUSTER, NODE, bucket.clone());

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
    let b = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert_eq!(1, b.seed_era_epoch(PREFIX).await?, "shared across replicas");

    Ok(())
}

/// #92 rollback: rewriting `lease.json` above the seeded era lets a restarted
/// lease-holder's next acquire (epoch + 1) out-epoch every leaseless-era
/// segment. After seeding era 6 (lease 5), rollback writes a lease epoch of 7.
#[tokio::test]
async fn rollback_rewrites_lease_above_era() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
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
        let writer = DynoStore::new(CLUSTER, NODE, inner.clone());
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
    );

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
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
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
        DynoStore::new(CLUSTER, NODE, bucket).coalesce_tuning(CoalesceTuning {
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
    );
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

/// A store that still *lists* a segment but answers its GET with `NotFound` —
/// the state of the world between a discovery listing and the footer read when
/// compaction (#66) or retention (#61) reclaims the object in between (#191).
#[derive(Clone)]
struct VanishOnGet<O> {
    inner: O,
    vanished: Path,
}

impl<O> Debug for VanishOnGet<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VanishOnGet").finish()
    }
}

impl<O> Display for VanishOnGet<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VanishOnGet").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for VanishOnGet<O>
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
        if *location == self.vanished {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "reclaimed between the listing and this GET".into(),
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

/// #191: a segment a discovery listing found, but which is gone by the time its
/// footer is read, must not fail the read.
///
/// Compaction (#66) merges segments away and retention (#61) reclaims them, both
/// concurrently with readers, so a LIST can legitimately name an object a
/// following GET no longer finds. Before this the `?` in the refresh loop turned
/// that race into a failed index refresh, and the raw `ObjectStore(NotFound)`
/// escaped `ListOffsets` through the connection error path — three layers logged
/// it, none handled it, and one reclaimed segment made a topic's offsets
/// unresolvable while its producer was still healthy.
///
/// The store here lists both segments and 404s the GET of one, which is exactly
/// the window the race opens.
#[tokio::test]
async fn a_segment_reclaimed_before_its_footer_is_read_does_not_fail_the_read() -> Result<(), Error>
{
    let bucket = InMemory::new();
    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    // A writer lays down two segments: [0,2) then [2,5).
    {
        let writer =
            DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
                coalesce_batches: Some(1),
                ..Default::default()
            });
        create_topic(&writer, topic).await?;
        assert_eq!(0, writer.produce(None, &a, batch(2)?).await?);
        assert_eq!(2, writer.produce(None, &a, batch(3)?).await?);
    }

    let live = segments(&bucket).await;
    assert_eq!(2, live.len());
    let vanished = live.last().expect("a newest segment").clone();

    // Cold reader whose index has never seen either sequence: it lists both and
    // GETs both footers, and one of those GETs 404s.
    let reader = DynoStore::new(
        CLUSTER,
        NODE,
        VanishOnGet {
            inner: bucket.clone(),
            vanished,
        },
    );

    // The whole point: these resolve instead of propagating `NotFound`.
    assert_eq!(
        0,
        earliest(&reader, &a).await?,
        "the surviving segment still answers EARLIEST",
    );

    let latest = reader
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(a.clone(), ListOffset::Latest)],
        )
        .await?;
    assert!(
        latest[0].1.offset.is_some(),
        "LATEST must resolve, got {:?}",
        latest[0].1,
    );

    // And the surviving segment's records are served.
    let fetched = fetch_from(&reader, &a, 0).await?;
    assert_eq!(2, record_count(&fetched), "the surviving segment is served");

    Ok(())
}

/// A create-CAS contender that also makes each segment PUT *slow*, and counts
/// the attempts, so a latency-bound flush can be told apart from a
/// contention-bound one (#192).
#[derive(Clone)]
struct SlowContendSegmentCreate<O> {
    inner: O,
    delay: Duration,
    attempts: Arc<AtomicU64>,
}

impl<O> Debug for SlowContendSegmentCreate<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlowContendSegmentCreate").finish()
    }
}

impl<O> Display for SlowContendSegmentCreate<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlowContendSegmentCreate").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for SlowContendSegmentCreate<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if matches!(opts.mode, PutMode::Create) && seg_seq_of(location).is_some() {
            _ = self.attempts.fetch_add(1, Relaxed);
            tokio::time::sleep(self.delay).await;
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

/// The wall-clock budget never ends the flush before `MIN_FLUSH_ATTEMPTS` real
/// attempts (#192).
///
/// Before this, the loop checked `elapsed >= budget` at the top of every
/// iteration, so a budget already spent by *one* slow PUT ended the flush after
/// a single attempt — surrendering to a competitor that, at `conflicts == 1`,
/// need not exist. Production saw exactly that: exhaustion reported at one or
/// two conflicts with `stalled=0`, i.e. no contention to yield to, while the
/// produce was rejected. A rejected produce is retriable, but the clients here
/// treat it as an engine failure and restart the whole connector.
///
/// The budget is 1 ms against a 20 ms PUT, so it is exhausted during the first
/// attempt and every subsequent check fails. Attempts must still reach the floor.
#[tokio::test]
async fn flush_budget_never_surrenders_before_the_attempt_floor() -> Result<(), Error> {
    let attempts = Arc::new(AtomicU64::new(0));
    let store = DynoStore::new(
        CLUSTER,
        NODE,
        SlowContendSegmentCreate {
            inner: InMemory::new(),
            delay: Duration::from_millis(20),
            attempts: attempts.clone(),
        },
    )
    .coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        flush_max_elapsed: Some(Duration::from_millis(1)),
        ..Default::default()
    });

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    let error = store
        .produce(None, &tp, batch(1)?)
        .await
        .expect_err("perpetual contention must exhaust the flush");
    assert!(
        matches!(error, Error::Api(ErrorCode::KafkaStorageError)),
        "exhaustion stays retriable, got {error:?}"
    );

    let made = attempts.load(Relaxed);
    assert_eq!(
        3, made,
        "the floor must be honoured exactly: fewer means the clock cut it short, \
         more means the clock stopped bounding it",
    );

    Ok(())
}

/// A generous budget lets the same flush keep going past the floor, so the floor
/// is a minimum rather than a cap (#192): the clock still governs, it just no
/// longer fires before the loop has learned anything.
#[tokio::test]
async fn flush_budget_admits_more_attempts_when_it_can_afford_them() -> Result<(), Error> {
    let attempts = Arc::new(AtomicU64::new(0));
    let store = DynoStore::new(
        CLUSTER,
        NODE,
        SlowContendSegmentCreate {
            inner: InMemory::new(),
            delay: Duration::from_millis(5),
            attempts: attempts.clone(),
        },
    )
    .coalesce_tuning(CoalesceTuning {
        coalesce_batches: Some(1),
        flush_max_elapsed: Some(Duration::from_millis(400)),
        ..Default::default()
    });

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    _ = store
        .produce(None, &tp, batch(1)?)
        .await
        .expect_err("perpetual contention must exhaust the flush");

    let made = attempts.load(Relaxed);
    assert!(
        made > 3,
        "a budget that can afford more attempts must make them, got {made}",
    );

    Ok(())
}

/// #157: an object squatting a segment sequence with **no decodable footer**
/// (shorter than the trailer, or a tail whose magic is not `TSEG`) must not wedge
/// the leaseless arbiter. The candidate is derived from the *resolved* sequences —
/// decoded segments and undecodable names alike — so the writer steps over the
/// squatter. Deriving it from the readable set alone re-picks the same occupied
/// sequence on every attempt, burning the whole create-CAS budget on every flush,
/// on every replica, at any produce rate — the deterministic livelock behind the
/// "leaseless flush exhausted retries" spam on a low-rate prefix.
#[tokio::test]
async fn leaseless_steps_over_footerless_segment_object() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    // A first produce lays down segment 0, so the next free sequence is 1.
    assert_eq!(0, store.produce(None, &tp, batch(1)?).await?);

    // Squat sequence 1 with an object that carries no `TSEG` trailer — the shape
    // a foreign/truncated write leaves in the create-only namespace.
    _ = bucket
        .put(
            &Path::from(format!(
                "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{:0>20}.seg",
                1
            )),
            PutPayload::from_static(b"not a segment"),
        )
        .await?;

    // The flush must step over sequence 1 and keep the sub-stream contiguous
    // rather than exhaust its budget on an occupied candidate.
    assert_eq!(1, store.produce(None, &tp, batch(1)?).await?);
    assert_eq!(2, store.produce(None, &tp, batch(1)?).await?);

    // And the records are readable across the skipped sequence.
    let fetched = fetch_from(&store, &tp, 0).await?;
    assert!(
        !fetched.is_empty(),
        "records written around the squatted sequence must still be served"
    );

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
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());
    assert_eq!(16 << 20, store.prefix_compact_target_bytes);
}

/// One footer entry as an external reader coded from the doc would recover it:
/// `(topic, partition, base_offset, record_count, byte_start, byte_len,
/// pcoord_count)`.
type DocEntry = (String, i32, i64, i64, u64, u64, usize);

/// Decode a segment object's footer using **only** what
/// `docs/virtual-topics-format.md` states — the external-reader contract, with no
/// access to the storage crate's own decoder. Returns
/// `(version, entries, trailing_footer_bytes)`; a non-zero `trailing_footer_bytes`
/// means the documented layout left the cursor short of the footer's end, i.e. a
/// reader built from the doc would desync (#138).
fn decode_per_the_doc(object: &[u8]) -> (u16, Vec<DocEntry>, usize) {
    fn u16_at(b: &[u8], at: usize) -> u16 {
        u16::from_be_bytes(b[at..at + 2].try_into().expect("u16"))
    }
    fn u32_at(b: &[u8], at: usize) -> u32 {
        u32::from_be_bytes(b[at..at + 4].try_into().expect("u32"))
    }
    fn u64_at(b: &[u8], at: usize) -> u64 {
        u64::from_be_bytes(b[at..at + 8].try_into().expect("u64"))
    }
    fn i64_at(b: &[u8], at: usize) -> i64 {
        i64::from_be_bytes(b[at..at + 8].try_into().expect("i64"))
    }

    // Trailer: fixed 18 bytes at the very end.
    let trailer = &object[object.len() - 18..];
    let footer_len = u64_at(trailer, 0) as usize;
    let entry_count = u32_at(trailer, 8) as usize;
    let version = u16_at(trailer, 12);
    assert_eq!(0x5453_4547, u32_at(trailer, 14), "TSEG magic");

    // Footer: the `footer_len` bytes immediately before the trailer.
    let footer_end = object.len() - 18;
    let footer = &object[footer_end - footer_len..footer_end];

    // Header: writer_epoch (i64), plus nonce (u64) at v2.
    let mut at = 8;
    if version >= 2 {
        at += 8;
    }

    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let topic_len = u16_at(footer, at) as usize;
        at += 2;
        let topic = String::from_utf8(footer[at..at + topic_len].to_vec()).expect("utf8 topic");
        at += topic_len;
        let partition = u32_at(footer, at) as i32;
        at += 4;
        let base_offset = i64_at(footer, at);
        at += 8;
        let record_count = i64_at(footer, at);
        at += 8;
        let byte_start = u64_at(footer, at);
        at += 8;
        let byte_len = u64_at(footer, at);
        at += 8;
        let _max_timestamp = i64_at(footer, at);
        at += 8;

        // v2+ ONLY: pcoord_count (u16) then that many producer coordinates —
        // 22 bytes each at v2, 23 at v3 (the extra `flags` byte; the exact
        // stride trap the format doc calls out).
        let mut pcoord_count = 0;
        if version >= 2 {
            pcoord_count = u16_at(footer, at) as usize;
            at += 2;
            at += pcoord_count * (8 + 2 + 4 + 4 + 4 + usize::from(version >= 3));
        }

        entries.push((
            topic,
            partition,
            base_offset,
            record_count,
            byte_start,
            byte_len,
            pcoord_count,
        ));
    }

    (version, entries, footer.len() - at)
}

/// #138: a reader implemented from `docs/virtual-topics-format.md` must decode a
/// segment this build actually writes. The doc used to describe footer v1 and the
/// `lease.json` regime while the leaseless writer emitted v2, so a reader coded
/// from it desynced its cursor on every production segment — the failure this
/// pins. The decoder above is deliberately independent of the storage crate's own
/// footer code: it is what an external S3-direct reader (kotatsu#82) would write.
#[tokio::test]
async fn documented_layout_decodes_a_segment_this_build_writes() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic_a = "org.env.conn.tab_a";
    let topic_b = "org.env.conn.tab_b";
    create_topic(&store, topic_a).await?;
    create_topic(&store, topic_b).await?;
    let a = Topition::new(topic_a, 0);
    let b = Topition::new(topic_b, 0);

    // Two sub-streams in one segment (produced concurrently so they share one
    // linger window), one of them idempotent so the `producers` block is
    // non-empty — the block a v1-shaped reader skips.
    let (produced_a, produced_b) = tokio::join!(
        store.produce(None, &a, idempotent_batch(4_242, 0, 0, 2)?),
        store.produce(None, &b, batch(3)?),
    );
    assert_eq!(0, produced_a?);
    assert_eq!(0, produced_b?);

    let locations = segments(&bucket).await;
    assert_eq!(1, locations.len(), "one shared segment");
    let object = bucket
        .get(&locations[0])
        .await
        .expect("get segment")
        .bytes()
        .await
        .expect("segment bytes");

    let (version, entries, trailing) = decode_per_the_doc(&object);

    assert_eq!(
        3, version,
        "the leaseless writer emits footer v3 (#174 release B)"
    );
    assert_eq!(
        0, trailing,
        "the documented layout must consume the footer exactly — {trailing} bytes left over \
         means an external reader desyncs"
    );

    let by_topic: BTreeMap<&str, &DocEntry> = entries.iter().map(|e| (e.0.as_str(), e)).collect();
    assert_eq!(2, by_topic.len(), "both sub-streams indexed: {entries:?}");

    let entry_a = by_topic.get(topic_a).expect("tab_a entry");
    assert_eq!((0, 0, 2), (entry_a.1, entry_a.2, entry_a.3));
    assert!(entry_a.6 > 0, "idempotent sub-stream carries producers");

    let entry_b = by_topic.get(topic_b).expect("tab_b entry");
    assert_eq!((0, 0, 3), (entry_b.1, entry_b.2, entry_b.3));
    assert_eq!(0, entry_b.6, "non-idempotent sub-stream carries none");

    // The documented byte extents address real records: each region must be a
    // batch concatenation, and the regions must not overlap.
    for entry in &entries {
        let region = &object[entry.4 as usize..(entry.4 + entry.5) as usize];
        assert!(!region.is_empty(), "region bytes present for {}", entry.0);
    }
    assert!(
        entry_a.4 + entry_a.5 <= entry_b.4 || entry_b.4 + entry_b.5 <= entry_a.4,
        "sub-stream regions must not overlap: {entries:?}"
    );

    Ok(())
}

/// An `ObjectStore` that records the order in which segment objects are created
/// and `segments/` prefixes listed, and fails every delete of keys under one
/// chosen prefix — enough to observe the per-prefix maintenance driver's ordering
/// and error isolation (#140). Everything else delegates to the inner store.
#[derive(Clone)]
struct MaintenanceProbe<O> {
    inner: O,
    observed: Arc<Mutex<Vec<String>>>,
    fail_delete_under: Option<String>,
}

impl<O> MaintenanceProbe<O> {
    fn note(&self, path: &Path) {
        if let Ok(mut observed) = self.observed.lock() {
            observed.push(path.to_string());
        }
    }

    fn note_listing(&self, prefix: Option<&Path>) {
        if let Some(prefix) = prefix
            && prefix.as_ref().contains("/segments")
        {
            self.note(prefix);
        }
    }
}

impl<O> Debug for MaintenanceProbe<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaintenanceProbe").finish()
    }
}

impl<O> Display for MaintenanceProbe<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaintenanceProbe").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for MaintenanceProbe<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if seg_seq_of(location).is_some() {
            self.note(location);
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
        let Some(under) = self.fail_delete_under.clone() else {
            return self.inner.delete_stream(locations);
        };

        // A matching key never reaches the inner store, so it survives — and the
        // error surfaces to the caller exactly as a failed `DeleteObjects` would.
        self.inner.delete_stream(
            locations
                .map(move |location| match location {
                    Ok(path) if path.as_ref().contains(under.as_str()) => {
                        Err(object_store::Error::Generic {
                            store: "S3",
                            source: "injected delete failure".into(),
                        })
                    }
                    other => other,
                })
                .boxed(),
        )
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.note_listing(prefix);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.note_listing(prefix);
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

/// #140: retention and compaction are interleaved **per prefix** — one
/// maintenance pass expires a prefix's aged segments *and then* merges what
/// survives. Asserting `deleted` also pins the order: were compaction to run
/// first, it would merge the aged segments into a fresh-stamped one and nothing
/// would expire at all.
#[tokio::test]
async fn maintenance_expires_then_compacts_each_prefix() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(2),
        prefix_compact_keep_hot: Some(0),
        prefix_compact_target_bytes: Some(4096),
        ..Default::default()
    });
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    // Four ancient segments (past the default 7-day retention) …
    for _ in 0..4 {
        _ = store.produce(None, &tp, batch_at(1, 1_000)?).await?;
    }
    // … then four current ones, which retention must keep and compaction merge.
    for _ in 0..4 {
        _ = store.produce(None, &tp, batch(1)?).await?;
    }
    assert_eq!(8, segments(&bucket).await.len());

    let (deleted, compacted) = store.maintain_prefix_segments(now_ms(), None).await?;

    assert_eq!(4, deleted, "the aged segments expired in this same pass");
    assert!(compacted > 0, "and the survivors were merged");
    assert!(
        segments(&bucket).await.len() <= 2,
        "prefix converged to <= min_segments"
    );

    Ok(())
}

/// #140: one prefix whose retention fails must not cost every other prefix its
/// compaction. Retention used to be a whole pass ahead of compaction that
/// propagated the first per-prefix error, so a single failing prefix aborted the
/// tick before compaction ran at all — on any prefix.
#[tokio::test]
async fn a_failing_prefix_does_not_stop_the_others() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(
        CLUSTER,
        NODE,
        MaintenanceProbe {
            inner: bucket.clone(),
            observed: Arc::new(Mutex::new(Vec::new())),
            fail_delete_under: Some("prefixes/org.env.aaa/".into()),
        },
    )
    .coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(2),
        prefix_compact_keep_hot: Some(0),
        prefix_compact_target_bytes: Some(4096),
        ..Default::default()
    });

    // Prefix `aaa`: aged segments whose expiry delete is injected to fail.
    let doomed = "org.env.aaa.tab_a";
    create_topic(&store, doomed).await?;
    let doomed_tp = Topition::new(doomed, 0);
    for _ in 0..2 {
        _ = store.produce(None, &doomed_tp, batch_at(1, 1_000)?).await?;
    }

    // Prefix `zzz`: current segments over the compaction trigger.
    let healthy = "org.env.zzz.tab_a";
    create_topic(&store, healthy).await?;
    let healthy_tp = Topition::new(healthy, 0);
    for _ in 0..8 {
        _ = store.produce(None, &healthy_tp, batch(1)?).await?;
    }

    let (deleted, compacted) = store.maintain_prefix_segments(now_ms(), None).await?;

    assert_eq!(0, deleted, "the injected delete failure expired nothing");
    assert_eq!(
        2,
        segments_of(&bucket, "org.env.aaa").await.len(),
        "the failing prefix kept its segments"
    );
    assert!(compacted > 0, "the healthy prefix was still compacted");
    assert!(
        segments_of(&bucket, "org.env.zzz").await.len() <= 2,
        "and drained to <= min_segments despite the other prefix failing"
    );

    Ok(())
}

/// #140: the prefix with the largest known backlog is maintained first, so a run
/// cut short by the maintenance timeout has drained the prefixes furthest over
/// the trigger — not whichever ones sort first by name.
#[tokio::test]
async fn maintenance_visits_the_largest_prefix_first() -> Result<(), Error> {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let bucket = InMemory::new();
    let store = DynoStore::new(
        CLUSTER,
        NODE,
        MaintenanceProbe {
            inner: bucket.clone(),
            observed: observed.clone(),
            fail_delete_under: None,
        },
    )
    .coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(2),
        prefix_compact_keep_hot: Some(0),
        prefix_compact_target_bytes: Some(4096),
        ..Default::default()
    });

    // `aaa` sorts first by name but holds the smaller backlog; `zzz` sorts last
    // and holds the larger one. Both are over the compaction trigger, so both
    // will write a merged segment — the order of those writes is the observation.
    let small = "org.env.aaa.tab_a";
    create_topic(&store, small).await?;
    let small_tp = Topition::new(small, 0);
    for _ in 0..3 {
        _ = store.produce(None, &small_tp, batch(1)?).await?;
    }

    let large = "org.env.zzz.tab_a";
    create_topic(&store, large).await?;
    let large_tp = Topition::new(large, 0);
    for _ in 0..8 {
        _ = store.produce(None, &large_tp, batch(1)?).await?;
    }

    observed.lock().expect("observed").clear();
    let (_, compacted) = store.maintain_prefix_segments(now_ms(), None).await?;
    assert!(compacted > 0, "both prefixes were over the trigger");

    let observed = observed.lock().expect("observed").clone();
    let first_large = observed
        .iter()
        .position(|path| path.contains("org.env.zzz"));
    let first_small = observed
        .iter()
        .position(|path| path.contains("org.env.aaa"));

    assert!(
        matches!((first_large, first_small), (Some(large), Some(small)) if large < small),
        "the larger prefix must be maintained first: {observed:?}"
    );

    Ok(())
}

/// #130: compaction claims the merged segment's name from the **same** tail
/// sequence namespace as live producers, so a producer can take the sequence the
/// compactor targeted. It must then resync to the next free sequence and re-PUT
/// the merged payload — the write amplification `tansu_prefix_segment_create_*`
/// now measures, and the reason a separate `compacted/` namespace is on the table.
/// Squatting the compactor's target sequence models the lost race.
#[tokio::test]
async fn compaction_resyncs_when_its_target_sequence_is_taken() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(2),
        prefix_compact_keep_hot: Some(0),
        prefix_compact_target_bytes: Some(4096),
        ..Default::default()
    });
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    for _ in 0..4 {
        _ = store.produce(None, &tp, batch(1)?).await?;
    }
    let before = segments(&bucket).await;
    assert_eq!(4, before.len());

    let offsets: Vec<i64> = fetch_from(&store, &tp, 0)
        .await?
        .iter()
        .map(|batch| batch.base_offset)
        .collect();

    // A producer wins sequence 4 — the tail the compactor is about to claim.
    let taken = Path::from(format!(
        "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{:0>20}.seg",
        4
    ));
    _ = bucket
        .put(&taken, PutPayload::from_static(b"taken by a producer"))
        .await?;

    // Compaction loses that create, resyncs past it, and still merges.
    assert!(
        store.drain_compact_prefix(PREFIX).await > 0,
        "compaction merged despite losing its target sequence"
    );

    let after = segments(&bucket).await;
    assert!(
        after.contains(&taken),
        "the producer's segment is untouched: {after:?}"
    );
    assert!(
        after.len() < before.len() + 1,
        "the merged run replaced its originals: {after:?}"
    );

    // Reads are unchanged across the resynced merge.
    assert_eq!(
        offsets,
        fetch_from(&store, &tp, 0)
            .await?
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// #117 measurement: consumers of *different* topics under one connector prefix
/// read **disjoint** byte ranges of the same shared segment object. That is the
/// duplication a `(prefix, seq, range)`-keyed block cache — the design proposed in
/// #117 — cannot serve at all: the ranges never match. Only caching the object
/// would collapse these into one GET. Pins the read pattern the metric classifies
/// as `other_range`.
#[tokio::test]
async fn co_prefix_consumers_read_disjoint_ranges_of_one_segment() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic_a = "org.env.conn.tab_a";
    let topic_b = "org.env.conn.tab_b";
    create_topic(&store, topic_a).await?;
    create_topic(&store, topic_b).await?;
    let a = Topition::new(topic_a, 0);
    let b = Topition::new(topic_b, 0);

    // Both sub-streams in one segment (one shared linger window).
    let (produced_a, produced_b) = tokio::join!(
        store.produce(None, &a, batch(2)?),
        store.produce(None, &b, batch(2)?),
    );
    assert_eq!(0, produced_a?);
    assert_eq!(0, produced_b?);
    assert_eq!(1, segments(&bucket).await.len());

    assert!(!fetch_from(&store, &a, 0).await?.is_empty());
    assert!(!fetch_from(&store, &b, 0).await?.is_empty());

    let traces = store.segment_reads.lock().expect("segment_reads").clone();
    let trace = traces
        .get(&(PREFIX.to_owned(), 0))
        .expect("both consumers read segment 0");

    assert_eq!(
        2,
        trace.ranges.len(),
        "two disjoint ranges of one object, not one shared range: {:?}",
        trace.ranges
    );
    let (first_start, first_len) = trace.ranges[0];
    let (second_start, second_len) = trace.ranges[1];
    assert!(
        first_start + first_len <= second_start || second_start + second_len <= first_start,
        "ranges must not overlap: {:?}",
        trace.ranges
    );

    Ok(())
}

/// #117 measurement, the other half: one consumer re-reading its own sub-stream
/// asks for the **identical** span, which is what the proposed block cache would
/// serve. Classified `same_range`, so it is not conflated with the co-prefix
/// pattern above — the split between the two is what decides the design.
#[tokio::test]
async fn re_reading_one_substream_repeats_the_same_range() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    _ = store.produce(None, &tp, batch(2)?).await?;

    assert!(!fetch_from(&store, &tp, 0).await?.is_empty());
    assert!(!fetch_from(&store, &tp, 0).await?.is_empty());

    let traces = store.segment_reads.lock().expect("segment_reads").clone();
    let trace = traces
        .get(&(PREFIX.to_owned(), 0))
        .expect("the sub-stream was read");

    assert_eq!(
        1,
        trace.ranges.len(),
        "a re-read of the same sub-stream is the same span: {:?}",
        trace.ranges
    );

    Ok(())
}

/// The #117 trace is a measurement device on the fetch hot path, so it must stay
/// bounded whatever the read pattern: an unbounded map keyed by every segment ever
/// read would turn an observability aid into a leak.
#[tokio::test]
async fn segment_read_trace_stays_bounded() {
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());

    for seq in 0..(4 * 1_024u64) {
        store.note_segment_data_read(PREFIX, seq, 0, 64);
    }

    let traced = store.segment_reads.lock().expect("segment_reads").len();
    assert!(traced <= 1_024, "trace grew to {traced} objects");
}

/// An `ObjectStore` that counts `segments/` listings, so a test can assert a
/// refresh was served **without** a `ListObjectsV2` (#112).
#[derive(Clone)]
struct CountSegmentLists<O> {
    inner: O,
    lists: Arc<AtomicU64>,
}

impl<O> CountSegmentLists<O> {
    fn note(&self, prefix: Option<&Path>) {
        if prefix.is_some_and(|prefix| prefix.as_ref().contains("/segments")) {
            _ = self.lists.fetch_add(1, Relaxed);
        }
    }
}

impl<O> Debug for CountSegmentLists<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountSegmentLists").finish()
    }
}

impl<O> Display for CountSegmentLists<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountSegmentLists").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for CountSegmentLists<O>
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
        self.note(prefix);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.note(prefix);
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

/// #112: a reader following the tail of a prefix another replica is writing must
/// not issue a `ListObjectsV2` per refresh. The tail probe folds the peer's new
/// segment from the same ranged GET that discovers it, and proves there is nothing
/// beyond it — so a caught-up refresh costs two tier-2 GETs instead of a tier-1
/// LIST, and a refresh that finds a new segment costs no more than the footer GET
/// it would have paid anyway.
#[tokio::test]
async fn tail_probe_follows_a_peer_without_listing() -> Result<(), Error> {
    let bucket = InMemory::new();
    let lists = Arc::new(AtomicU64::new(0));

    let writer = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let reader = DynoStore::new(
        CLUSTER,
        NODE,
        CountSegmentLists {
            inner: bucket.clone(),
            lists: lists.clone(),
        },
    );

    let topic = "org.env.conn.tab_a";
    create_topic(&writer, topic).await?;
    let tp = Topition::new(topic, 0);

    _ = writer.produce(None, &tp, batch(1)?).await?;

    // Cold index: one listing, unavoidable (there is no cursor to probe from).
    reader.refresh_prefix_index_forced(PREFIX).await?;
    let after_cold = lists.load(Relaxed);
    assert_eq!(1, after_cold, "the cold build lists once");

    // Nothing new: proven by probe, no listing.
    reader.refresh_prefix_index_forced(PREFIX).await?;
    assert_eq!(
        after_cold,
        lists.load(Relaxed),
        "a caught-up refresh must not list"
    );

    // A peer appends: folded from the probe GET, still no listing.
    _ = writer.produce(None, &tp, batch(1)?).await?;
    reader.refresh_prefix_index_forced(PREFIX).await?;
    assert_eq!(
        after_cold,
        lists.load(Relaxed),
        "folding a peer's new segment must not list"
    );

    assert_eq!(
        Some(1),
        reader
            .prefix_index
            .lock()
            .expect("prefix_index")
            .get(PREFIX)
            .and_then(|index| index.segments.keys().next_back().copied()),
        "the peer's segment 1 is in the index"
    );

    // And the reader serves the peer's records from it.
    assert_eq!(
        vec![0, 1],
        fetch_from(&reader, &tp, 0)
            .await?
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// #112: the probe's proof rests on the seq floor not being ahead of the absent
/// sequence. A floor above it means names past our cursor were freed by retention
/// or compaction, so a 404 there proves nothing — the refresh must fall back to a
/// listing rather than conclude the tail is where it last saw it.
#[tokio::test]
async fn tail_probe_defers_to_a_listing_when_the_floor_is_ahead() -> Result<(), Error> {
    let bucket = InMemory::new();
    let lists = Arc::new(AtomicU64::new(0));
    let store = DynoStore::new(
        CLUSTER,
        NODE,
        CountSegmentLists {
            inner: bucket.clone(),
            lists: lists.clone(),
        },
    );

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);
    _ = store.produce(None, &tp, batch(1)?).await?;

    store.refresh_prefix_index_forced(PREFIX).await?;
    let baseline = lists.load(Relaxed);

    // Sanity: with the floor at the tail the probe answers on its own.
    store.refresh_prefix_index_forced(PREFIX).await?;
    assert_eq!(baseline, lists.load(Relaxed));

    // A drain elsewhere raised the floor past the tail: absence is no longer proof.
    store.raise_seq_floor(PREFIX, 9).await?;
    store.refresh_prefix_index_forced(PREFIX).await?;
    assert_eq!(
        baseline + 1,
        lists.load(Relaxed),
        "a floor ahead of the cursor must force a listing"
    );

    Ok(())
}

/// Truncate `tp` below `offset` via the `Storage::delete_records` API,
/// returning the reported low watermark (#176).
async fn delete_before(store: &DynoStore, tp: &Topition, offset: i64) -> Result<i64> {
    let responses = store
        .delete_records(&[DeleteRecordsTopic::default()
            .name(tp.topic().into())
            .partitions(Some(vec![
                DeleteRecordsPartition::default()
                    .partition_index(tp.partition())
                    .offset(offset),
            ]))])
        .await?;

    let partition = responses
        .first()
        .and_then(|topic| topic.partitions.as_deref())
        .and_then(|partitions| partitions.first())
        .expect("delete_records partition result")
        .clone();

    assert_eq!(i16::from(ErrorCode::None), partition.error_code);

    Ok(partition.low_watermark)
}

/// The EARLIEST list-offsets entry for `tp`.
async fn earliest(store: &DynoStore, tp: &Topition) -> Result<i64> {
    store
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(tp.clone(), ListOffset::Earliest)],
        )
        .await
        .map(|responses| responses[0].1.offset.expect("earliest offset"))
}

/// The record count carried by `batches`.
fn record_count(batches: &[deflated::Batch]) -> i64 {
    batches
        .iter()
        .map(|batch| batch.last_offset_delta as i64 + 1)
        .sum()
}

/// DeleteRecords on a pure-segment sub-stream advances the log start (#176):
/// the response, EARLIEST, both offset-stage isolation paths, and fetch all
/// honour the truncation floor — while the shared segment objects are
/// physically untouched (truncation is logical).
#[tokio::test]
async fn delete_records_pure_segment_advances_log_start() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    // Two windows -> two segments: [0,3) and [3,5).
    assert_eq!(0, store.produce(None, &a, batch(3)?).await?);
    assert_eq!(3, store.produce(None, &a, batch(2)?).await?);
    assert_eq!(2, segments(&bucket).await.len());

    assert_eq!(3, delete_before(&store, &a, 3).await?);

    // Physical layout untouched: segments are shared, truncation is logical.
    assert_eq!(2, segments(&bucket).await.len());

    assert_eq!(3, earliest(&store, &a).await?);

    // Read-uncommitted (cache-served, zero requests warm) and read-committed
    // (log_start over the footer index) agree on the log start.
    let ru = store
        .offset_stage_at(&a, IsolationLevel::ReadUncommitted)
        .await?;
    assert_eq!(3, ru.log_start());
    assert_eq!(5, ru.high_watermark());

    let rc = store
        .offset_stage_at(&a, IsolationLevel::ReadCommitted)
        .await?;
    assert_eq!(3, rc.log_start());

    // A fetch from 0 is clamped to the floor: the fully-truncated first
    // segment is skipped whole, and no returned batch ends at/below it.
    let fetched = fetch_from(&store, &a, 0).await?;
    assert_eq!(Some(3), fetched.first().map(|batch| batch.base_offset));
    assert_eq!(2, record_count(&fetched));
    assert!(
        fetched
            .iter()
            .all(|batch| batch.base_offset + batch.last_offset_delta as i64 + 1 > 3),
        "no batch entirely below the floor is served"
    );

    Ok(())
}

/// DeleteRecords is per sub-stream (#176): truncating topic A must not move
/// topic B's log start or records, even when both live in the same shared
/// segment — and the shared segment survives while B is live.
#[tokio::test]
async fn delete_records_shared_segment_isolates_substreams() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic_a = "org.env.conn.tab_a";
    let topic_b = "org.env.conn.tab_b";
    create_topic(&store, topic_a).await?;
    create_topic(&store, topic_b).await?;
    let a = Topition::new(topic_a, 0);
    let b = Topition::new(topic_b, 0);

    // One window, one shared segment: A=[0,2), B=[0,2).
    let (ra, rb) = tokio::join!(
        store.produce(None, &a, batch(2)?),
        store.produce(None, &b, batch(2)?),
    );
    _ = ra?;
    _ = rb?;
    assert_eq!(1, segments(&bucket).await.len());

    // Truncate all of A.
    assert_eq!(2, delete_before(&store, &a, -1).await?);

    // A is empty to readers; the shared segment object survives for B.
    assert_eq!(1, segments(&bucket).await.len());
    assert_eq!(2, earliest(&store, &a).await?);
    assert!(fetch_from(&store, &a, 0).await?.is_empty());

    // B is untouched.
    assert_eq!(0, earliest(&store, &b).await?);
    let fb = fetch_from(&store, &b, 0).await?;
    assert_eq!(Some(0), fb.first().map(|batch| batch.base_offset));
    assert_eq!(2, record_count(&fb));

    Ok(())
}

/// The truncation floor is durable (#176): a fresh process on the same bucket
/// — cold caches, nothing in memory — serves the truncated log start on
/// EARLIEST (the accessor's own cold `watermark.json` read resolves it),
/// offset-stage, and fetch.
#[tokio::test]
async fn delete_records_floor_survives_restart() -> Result<(), Error> {
    let bucket = InMemory::new();
    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    {
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
        create_topic(&store, topic).await?;
        assert_eq!(0, store.produce(None, &a, batch(3)?).await?);
        assert_eq!(3, store.produce(None, &a, batch(2)?).await?);
        assert_eq!(3, delete_before(&store, &a, 3).await?);
    }

    // Fresh process. EARLIEST first, before anything else warms the
    // watermark cache: this pins the floor accessor's cold read.
    let restarted = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert_eq!(3, earliest(&restarted, &a).await?);

    let stage = restarted
        .offset_stage_at(&a, IsolationLevel::ReadCommitted)
        .await?;
    assert_eq!(3, stage.log_start());
    assert_eq!(5, stage.high_watermark());

    let fetched = fetch_from(&restarted, &a, 0).await?;
    assert_eq!(Some(3), fetched.first().map(|batch| batch.base_offset));
    assert_eq!(2, record_count(&fetched));

    Ok(())
}

/// The truncation floor is monotonic (#176): a second DeleteRecords with a
/// LOWER offset must not regress the log start, and — because the fold is a
/// `max` under the watermark CAS — the response must report the floor that
/// actually holds, not echo the requested offset.
#[tokio::test]
async fn delete_records_is_monotonic() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &a, batch(3)?).await?);
    assert_eq!(3, store.produce(None, &a, batch(2)?).await?);

    assert_eq!(3, delete_before(&store, &a, 3).await?);

    // Lower offset: the post-fold floor (3) is reported, nothing regresses.
    assert_eq!(
        3,
        delete_before(&store, &a, 1).await?,
        "the response is the held floor, not the requested offset"
    );
    assert_eq!(3, earliest(&store, &a).await?);
    assert_eq!(
        3,
        store
            .offset_stage_at(&a, IsolationLevel::ReadUncommitted)
            .await?
            .log_start()
    );

    // A higher offset still advances it.
    assert_eq!(4, delete_before(&store, &a, 4).await?);
    assert_eq!(4, earliest(&store, &a).await?);

    Ok(())
}

/// DeleteRecords of the whole log (`offset = -1`, #176): EARLIEST == the high
/// watermark, fetch is empty, and the next produce continues at the old end —
/// offsets are never reused.
#[tokio::test]
async fn delete_records_all_then_produce() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &a, batch(3)?).await?);

    assert_eq!(3, delete_before(&store, &a, -1).await?);

    let stage = store
        .offset_stage_at(&a, IsolationLevel::ReadUncommitted)
        .await?;
    assert_eq!(3, stage.log_start());
    assert_eq!(3, stage.high_watermark());
    assert_eq!(3, earliest(&store, &a).await?);
    assert!(fetch_from(&store, &a, 0).await?.is_empty());

    // The next produce continues at the old end, and only it is served.
    assert_eq!(3, store.produce(None, &a, batch(2)?).await?);
    assert_eq!(3, earliest(&store, &a).await?);
    let fetched = fetch_from(&store, &a, 0).await?;
    assert_eq!(Some(3), fetched.first().map(|batch| batch.base_offset));
    assert_eq!(2, record_count(&fetched));

    Ok(())
}

/// A segment whose every sub-stream slice ends at/below its truncation floor
/// is reclaimed by segment expiry regardless of age (#176); a partially
/// truncated one is not; and the freed offsets and sequence names are never
/// reused after the reclaim (#77).
#[tokio::test]
async fn fully_truncated_segment_expires() -> Result<(), Error> {
    let bucket = InMemory::new();
    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    {
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
            .prefix_lease_ttl(Duration::from_millis(80));
        create_topic(&store, topic).await?;

        let recent = now_ms();
        // Two recent segments: [0,2) at seq 0 and [2,4) at seq 1 — age-based
        // retention alone (threshold below the record age) must reclaim
        // neither.
        assert_eq!(0, store.produce(None, &a, batch_at(2, recent)?).await?);
        assert_eq!(2, store.produce(None, &a, batch_at(2, recent)?).await?);
        assert_eq!(0, store.expire_prefix_segments(PREFIX, 1_000).await?);
        assert_eq!(2, segments(&bucket).await.len());

        // Floor 3: seq 0 (ends at 2) is fully truncated, seq 1 (ends at 4)
        // only partially — expiry reclaims exactly the fully-truncated one.
        assert_eq!(3, delete_before(&store, &a, 3).await?);
        assert_eq!(1, store.expire_prefix_segments(PREFIX, 1_000).await?);
        assert_eq!(vec![segment_path(1)], segments(&bucket).await);

        // Records above the floor are still served after the reclaim
        // (batch-granular: the surviving batch starts below the floor).
        let fetched = fetch_from(&store, &a, 0).await?;
        assert_eq!(Some(2), fetched.first().map(|batch| batch.base_offset));
        assert_eq!(2, record_count(&fetched));

        // Truncate the rest: the last segment becomes reclaimable too.
        assert_eq!(4, delete_before(&store, &a, -1).await?);
        assert_eq!(1, store.expire_prefix_segments(PREFIX, 1_000).await?);
        assert!(segments(&bucket).await.is_empty());
    }

    // Let the writer's lease lapse so the restart can take over.
    tokio::time::sleep(Duration::from_millis(160)).await;

    // Fresh process on the fully-drained prefix: the next produce resumes at
    // offset 4 (the persisted watermark floor) in a segment named by the
    // raised sequence floor (seq 2) — never a freed offset or seq name.
    let restarted = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert_eq!(4, restarted.produce(None, &a, batch(1)?).await?);
    assert_eq!(vec![segment_path(2)], segments(&bucket).await);

    Ok(())
}

/// Truncation-driven reclaim is best-effort and floor-warmth-gated (#176): a
/// maintainer that does not know a sub-stream's floor must DEFER the reclaim
/// (a missing floor never means "reclaim anyway"), and performs it once a
/// read has warmed the floor from `watermark.json`.
#[tokio::test]
async fn unknown_floor_defers_reclaim_until_warmed() -> Result<(), Error> {
    let bucket = InMemory::new();
    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    {
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
        create_topic(&store, topic).await?;
        let recent = now_ms();
        assert_eq!(0, store.produce(None, &a, batch_at(2, recent)?).await?);
        assert_eq!(2, delete_before(&store, &a, -1).await?);
    }

    // A fresh maintainer holds no floor for the partition: the fully
    // truncated (but recent) segment must survive its pass.
    let maintainer = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert_eq!(0, maintainer.expire_prefix_segments(PREFIX, 1_000).await?);
    assert_eq!(1, segments(&bucket).await.len());

    // Any read that warms the watermark (here EARLIEST) resolves the floor;
    // the next maintenance pass reclaims.
    assert_eq!(2, earliest(&maintainer, &a).await?);
    assert_eq!(1, maintainer.expire_prefix_segments(PREFIX, 1_000).await?);
    assert!(segments(&bucket).await.is_empty());

    Ok(())
}

/// A transactional data batch for `producer_id`/`epoch` at `base_sequence`.
fn txn_batch(
    producer_id: i64,
    epoch: i16,
    base_sequence: i32,
    records: usize,
) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder()
        .attributes(BatchAttribute::default().transaction(true).into())
        .producer_id(producer_id)
        .producer_epoch(epoch)
        .base_sequence(base_sequence)
        .last_offset_delta(records as i32 - 1);

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("txn-record-{i}").as_bytes(),
        ))));
    }

    builder
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Register a transactional producer and add `topics`' partition 0 to the
/// transaction; returns `(producer_id, producer_epoch)`.
async fn begin_txn(store: &DynoStore, txn: &str, topics: &[&str]) -> Result<(i64, i16)> {
    let producer = store
        .init_producer(Some(txn), 60_000, Some(-1), Some(-1))
        .await?;
    assert_eq!(ErrorCode::None, producer.error);

    _ = store
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: txn.into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: topics
                .iter()
                .map(|topic| {
                    AddPartitionsToTxnTopic::default()
                        .name((*topic).into())
                        .partitions(Some([0].into()))
                })
                .collect(),
        })
        .await?;

    Ok((producer.id, producer.epoch))
}

/// The transaction's registered produce range for `tp` at `epoch`, straight
/// from `meta.json` — the ledger `txn_end` and `offset_stage` read.
async fn produced_range(
    store: &DynoStore,
    txn: &str,
    epoch: i16,
    tp: &Topition,
) -> Result<Option<TxnProduceOffset>> {
    store
        .meta
        .with(&store.object_store, |meta| {
            Ok(meta
                .transactions
                .get(txn)
                .and_then(|transaction| transaction.epochs.get(&epoch))
                .and_then(|detail| detail.produces.get(tp.topic()))
                .and_then(|partitions| partitions.get(&tp.partition()))
                .copied()
                .flatten())
        })
        .await
}

/// #174 release B: transactional data batches coalesce into the shared
/// segment like any other batch (no legacy per-batch objects), the commit
/// marker lands in a segment too, the v3 footer indexes both with
/// attribute-derived flags, and after the commit the last stable offset
/// catches up to the high watermark.
#[tokio::test]
async fn txn_commit_marker_lands_in_segment() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic_a = "org.env.conn.tab_a";
    let topic_b = "org.env.conn.tab_b";
    create_topic(&store, topic_a).await?;
    create_topic(&store, topic_b).await?;
    let a = Topition::new(topic_a, 0);
    let b = Topition::new(topic_b, 0);

    let txn = "txn-commit";
    let (pid, epoch) = begin_txn(&store, txn, &[topic_a, topic_b]).await?;

    // Transactional data on two topitions of one prefix, produced concurrently
    // so they share one linger window: ONE shared segment, no legacy objects.
    let (produced_a, produced_b) = tokio::join!(
        store.produce(Some(txn), &a, txn_batch(pid, epoch, 0, 2)?),
        store.produce(Some(txn), &b, txn_batch(pid, epoch, 0, 3)?),
    );
    assert_eq!(0, produced_a?);
    assert_eq!(0, produced_b?);
    assert_eq!(
        1,
        segments(&bucket).await.len(),
        "transactional data coalesces into one shared segment"
    );
    assert!(legacy_records(&bucket, topic_a).await.is_empty());
    assert!(legacy_records(&bucket, topic_b).await.is_empty());

    // Commit: each partition's marker is an ordinary batch in an ordinary
    // (immediately flushed) segment — still no legacy objects.
    assert_eq!(ErrorCode::None, store.txn_end(txn, pid, epoch, true).await?);
    assert!(legacy_records(&bucket, topic_a).await.is_empty());
    assert!(legacy_records(&bucket, topic_b).await.is_empty());
    let locations = segments(&bucket).await;
    assert_eq!(
        3,
        locations.len(),
        "one data segment + one immediately-flushed segment per marker"
    );

    // Every segment is stamped v3 and decodes exactly by the documented
    // 23-byte-coordinate stride; the coordinates carry the attribute-derived
    // flags: 0b01 per transactional data batch, 0b11 per marker with the -1
    // sequences a marker carries.
    let mut data_coords = 0;
    let mut marker_coords = 0;
    for location in &locations {
        let object = bucket
            .get(location)
            .await
            .expect("get segment")
            .bytes()
            .await
            .expect("segment bytes");
        let (version, _, trailing) = decode_per_the_doc(&object);
        assert_eq!(3, version, "the leaseless writer stamps v3 unconditionally");
        assert_eq!(0, trailing, "documented v3 stride must consume the footer");

        let footer = footer_of(&bucket, location).await;
        for coord in footer.entries.iter().flat_map(|entry| &entry.producers) {
            assert_eq!(pid, coord.producer_id);
            match coord.flags {
                0b01 => {
                    assert_eq!(0, coord.base_sequence);
                    data_coords += 1;
                }
                0b11 => {
                    assert_eq!(-1, coord.base_sequence);
                    assert_eq!(-1, coord.last_sequence);
                    marker_coords += 1;
                }
                flags => panic!("unexpected coordinate flags {flags:#04b}"),
            }
        }
    }
    assert_eq!(2, data_coords, "one 0b01 coordinate per data batch");
    assert_eq!(2, marker_coords, "one 0b11 coordinate per marker");

    // Fetch delivers the marker as a batch at the expected offset with the
    // control+transaction attributes intact (the client filters it; it still
    // occupies one offset).
    let fetched = fetch_from(&store, &a, 0).await?;
    assert_eq!(2, fetched.len());
    assert_eq!(0, fetched[0].base_offset);
    assert!(!fetched[0].is_control());
    let marker = &fetched[1];
    assert_eq!(2, marker.base_offset);
    assert!(marker.is_control() && marker.is_transactional());
    assert_eq!(1, marker.record_count);

    // Committed: the last stable offset equals the high watermark on both
    // topitions, and nothing is aborted.
    for (tp, high) in [(&a, 3), (&b, 4)] {
        let stage = store.offset_stage(tp).await?;
        assert_eq!(high, stage.high_watermark());
        assert_eq!(high, stage.last_stable());
        assert!(stage.aborted().is_empty());
    }

    Ok(())
}

/// The read-committed contract over shared segments (#174 release B): an
/// aborted transaction interleaved with committed data from another producer
/// in the SAME segment must leave a read-committed consumer seeing only the
/// committed records. The broker never body-filters — it serves the batches
/// byte-preserved and bounds the consumer with the last stable offset and the
/// aborted-transaction list, both pure `meta.json` functions (`offset_stage`);
/// no footer read and no extra request decide abortedness. This test plays
/// the client's role exactly: stop at the LSO, drop aborted producers' ranges,
/// skip markers.
#[tokio::test]
async fn read_committed_sees_only_committed_records_from_shared_segment() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    let txn = "txn-abort";
    let (pid, epoch) = begin_txn(&store, txn, &[topic]).await?;

    // One linger window, one shared segment: a transactional batch (2 records,
    // will be aborted) interleaved with a plain committed batch (2 records)
    // from another producer. Enqueue order inside the window is not
    // deterministic — derive both offsets from the acks.
    let (produced_txn, produced_plain) = tokio::join!(
        store.produce(Some(txn), &a, txn_batch(pid, epoch, 0, 2)?),
        store.produce(None, &a, batch(2)?),
    );
    let (txn_offset, plain_offset) = (produced_txn?, produced_plain?);
    assert_eq!(1, segments(&bucket).await.len(), "one shared segment");

    // While the transaction is open the LSO pins at its first offset: a
    // read-committed fetch never serves the open range.
    let open = store.offset_stage(&a).await?;
    assert_eq!(4, open.high_watermark());
    assert_eq!(txn_offset, open.last_stable());
    assert!(open.aborted().is_empty());

    // Abort. The marker occupies offset 4; nothing is open anymore, so the
    // LSO catches up to the high watermark and the aborted range surfaces as
    // (producer_id, first_offset).
    assert_eq!(
        ErrorCode::None,
        store.txn_end(txn, pid, epoch, false).await?
    );
    let stage = store
        .offset_stage_at(&a, IsolationLevel::ReadCommitted)
        .await?;
    assert_eq!(5, stage.high_watermark());
    assert_eq!(5, stage.last_stable());
    assert_eq!(vec![(pid, txn_offset)], stage.aborted().to_vec());

    // The broker delivers everything below the LSO — aborted data and the
    // marker included, byte-preserved (attributes and producer fields intact).
    let fetched = store
        .fetch(
            &a,
            0,
            0,
            100_000,
            IsolationLevel::ReadCommitted,
            Duration::from_millis(200),
        )
        .await?;
    assert_eq!(3, fetched.len(), "plain data, aborted data, abort marker");

    // Kafka's client-side read-committed filter: stop at the LSO; a marker
    // closes its producer's aborted range and is never counted as data; a
    // transactional batch from a producer with an open aborted range at/after
    // first_offset is dropped.
    let mut aborted: BTreeMap<i64, i64> = stage.aborted().iter().copied().collect();
    let mut visible = Vec::new();
    for batch in &fetched {
        if batch.base_offset >= stage.last_stable() {
            break;
        }
        if batch.is_control() {
            _ = aborted.remove(&batch.producer_id);
            continue;
        }
        if batch.is_transactional()
            && aborted
                .get(&batch.producer_id)
                .is_some_and(|first| batch.base_offset >= *first)
        {
            continue;
        }
        visible.push(batch);
    }

    // Only the committed plain batch survives.
    assert_eq!(1, visible.len());
    assert_eq!(plain_offset, visible[0].base_offset);
    assert_eq!(2, visible[0].record_count);
    assert!(!visible[0].is_transactional());

    Ok(())
}

/// The same produce/abort script on a legacy-mode store and a segment-mode
/// store yields identical `offset_stage` output (#174 release B): moving the
/// bytes into segments must not change the LSO/aborted derivation, which is a
/// pure `meta.json` function of the registered ranges.
#[tokio::test]
async fn txn_abort_lso_and_aborted_match_legacy() -> Result<(), Error> {
    async fn script(store: &DynoStore) -> Result<(i64, [i64; 3], [i64; 3], Vec<(i64, i64)>)> {
        let topic = "org.env.conn.tab_a";
        create_topic(store, topic).await?;
        let tp = Topition::new(topic, 0);

        let txn = "txn-parity";
        let (pid, epoch) = begin_txn(store, txn, &[topic]).await?;

        assert_eq!(
            0,
            store
                .produce(Some(txn), &tp, txn_batch(pid, epoch, 0, 3)?)
                .await?
        );

        let open = store.offset_stage(&tp).await?;
        assert!(open.aborted().is_empty());

        assert_eq!(
            ErrorCode::None,
            store.txn_end(txn, pid, epoch, false).await?
        );
        let after = store.offset_stage(&tp).await?;

        Ok((
            pid,
            [open.last_stable(), open.high_watermark(), open.log_start()],
            [
                after.last_stable(),
                after.high_watermark(),
                after.log_start(),
            ],
            after.aborted().to_vec(),
        ))
    }

    let legacy = DynoStore::new(CLUSTER, NODE, InMemory::new());
    let segment = DynoStore::new(CLUSTER, NODE, InMemory::new());

    let (legacy_pid, legacy_open, legacy_after, legacy_aborted) = script(&legacy).await?;
    let (segment_pid, segment_open, segment_after, segment_aborted) = script(&segment).await?;

    assert_eq!(legacy_open, segment_open, "open-transaction stage differs");
    assert_eq!(legacy_after, segment_after, "post-abort stage differs");
    assert_eq!(vec![(legacy_pid, 0)], legacy_aborted);
    assert_eq!(vec![(segment_pid, 0)], segment_aborted);

    Ok(())
}

/// Behavioural guard for the control-coordinate fold filter: after a commit
/// marker lands in a segment, the SAME producer's next in-order batch is
/// still admitted. Without the filter, `producer_tail_folded` would fold the
/// marker's `-1` sequences (`next_sequence = 0`) and reject the batch as
/// `OutOfOrderSequenceNumber`.
#[tokio::test]
async fn control_coordinate_not_folded_into_producer_tail() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    let txn = "txn-fold";
    let (pid, epoch) = begin_txn(&store, txn, &[topic]).await?;

    // Sequences 0..=1 committed; the marker occupies offset 2.
    assert_eq!(
        0,
        store
            .produce(Some(txn), &a, txn_batch(pid, epoch, 0, 2)?)
            .await?
    );
    assert_eq!(ErrorCode::None, store.txn_end(txn, pid, epoch, true).await?);

    // The producer's genuine next in-order batch (sequence 2) must be
    // admitted at the next offset — the marker's coordinate never folds.
    assert_eq!(
        3,
        store
            .produce(None, &a, idempotent_batch(pid, epoch, 2, 1)?)
            .await?
    );

    Ok(())
}

/// A control batch flushes the coalesce buffer immediately (#174 release B) —
/// `txn_end` writes markers sequentially per partition, so parking each on
/// the linger would cost an N-partition commit N × linger — while a
/// transactional DATA batch must NOT: it coalesces like any other batch, or a
/// transactional workload would degrade to one object per batch.
#[tokio::test]
async fn control_batch_triggers_immediate_flush() -> Result<(), Error> {
    let bucket = InMemory::new();
    // A linger far beyond the test's patience: only a non-linger trigger can
    // flush. Data flushes on the 2-batch count; a marker must flush by itself.
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        coalesce_linger: Some(Duration::from_secs(30)),
        coalesce_batches: Some(2),
        ..Default::default()
    });

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    let txn = "txn-flush";
    let (pid, epoch) = begin_txn(&store, txn, &[topic]).await?;

    // A lone transactional data batch parks in the buffer: no per-batch flush.
    let parked = {
        let store = store.clone();
        let a = a.clone();
        let batch = txn_batch(pid, epoch, 0, 1)?;
        tokio::spawn(async move { store.produce(Some(txn), &a, batch).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        segments(&bucket).await.is_empty(),
        "a transactional data batch must not force a per-batch flush"
    );

    // The second batch trips the count threshold; both resolve.
    assert_eq!(
        1,
        store
            .produce(Some(txn), &a, txn_batch(pid, epoch, 1, 1)?)
            .await?
    );
    assert_eq!(0, parked.await.expect("join")?);
    assert_eq!(1, segments(&bucket).await.len());

    // The commit's marker must flush immediately — well inside the 30s
    // linger, which is the only other trigger left.
    let ended = tokio::time::timeout(Duration::from_secs(5), store.txn_end(txn, pid, epoch, true))
        .await
        .expect("txn_end must not park the marker on the linger")?;
    assert_eq!(ErrorCode::None, ended);
    assert_eq!(2, segments(&bucket).await.len());

    Ok(())
}

/// A retried transactional data batch is acked `Duplicate` with its original
/// offset, and its re-registration must not widen the transaction's produced
/// range (#174 release B): `offset_start` never moves, `offset_end` only ever
/// grows with genuinely new produces (and finally the marker).
#[tokio::test]
async fn transactional_duplicate_reregistration_is_idempotent() -> Result<(), Error> {
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "org.env.conn.tab_a";
    create_topic(&store, topic).await?;
    let a = Topition::new(topic, 0);

    let txn = "txn-dup";
    let (pid, epoch) = begin_txn(&store, txn, &[topic]).await?;

    assert_eq!(
        0,
        store
            .produce(Some(txn), &a, txn_batch(pid, epoch, 0, 2)?)
            .await?
    );
    assert_eq!(
        Some(TxnProduceOffset {
            offset_start: 0,
            offset_end: 1,
        }),
        produced_range(&store, txn, epoch, &a).await?
    );

    // The retry dedups against the folded footer coordinates: acked with the
    // ORIGINAL offset, and the registered range is untouched.
    assert_eq!(
        0,
        store
            .produce(Some(txn), &a, txn_batch(pid, epoch, 0, 2)?)
            .await?
    );
    assert_eq!(
        Some(TxnProduceOffset {
            offset_start: 0,
            offset_end: 1,
        }),
        produced_range(&store, txn, epoch, &a).await?
    );

    // A genuinely new produce extends the range...
    assert_eq!(
        2,
        store
            .produce(Some(txn), &a, txn_batch(pid, epoch, 2, 1)?)
            .await?
    );
    assert_eq!(
        Some(TxnProduceOffset {
            offset_start: 0,
            offset_end: 2,
        }),
        produced_range(&store, txn, epoch, &a).await?
    );

    // ...and committing drops the range entirely: `txn_end` clears a
    // *committed* transaction's produces (only an ABORTED one keeps them, so a
    // read-committed fetch can still report its aborted offsets, #81). So the
    // marker's own offset extending the range is not observable here — that
    // property is asserted on the abort path, in
    // `txn_abort_lso_and_aborted_match_legacy`, where the range survives.
    // Segment routing does not change this: it is the same `meta.transactions`
    // bookkeeping as the legacy path.
    assert_eq!(ErrorCode::None, store.txn_end(txn, pid, epoch, true).await?);
    assert_eq!(None, produced_range(&store, txn, epoch, &a).await?);

    let stage = store.offset_stage(&a).await?;
    assert_eq!(4, stage.high_watermark());
    assert_eq!(4, stage.last_stable());

    Ok(())
}

/// A gap left by an expiry that never certified it is certified by the
/// reconciliation pass, and the fetch inside it goes from empty-forever to
/// `OFFSET_OUT_OF_RANGE` (#290).
///
/// This is the population #343 could not reach. Only the expiry that performed a
/// delete writes `Watermark::served`, so every gap that predates it carries
/// none — and the deployment that reported #290 runs `maintenance_interval` at a
/// year, so no organic expiry will ever come along to certify one. Those
/// partitions keep answering empty to a parked consumer, which is #290's
/// original complaint verbatim.
///
/// The pre-#343 state is reproduced by stripping the pair the expiry wrote,
/// rather than by simulating damage: what is left is exactly the bytes an older
/// binary's expiry leaves behind.
#[tokio::test]
async fn an_uncertified_gap_is_certified_by_the_reconciliation_pass() -> Result<(), Error> {
    let ttl = Duration::from_millis(80);
    let bucket = InMemory::new();

    let topic = "org.env.conn.tab_a";
    let a = Topition::new(topic, 0);

    let recent = now_ms();
    let ancient = 1_000;

    // The process that creates the gap, then forgets to say so — an expiry
    // running an older binary.
    {
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_lease_ttl(ttl);
        create_topic(&store, topic).await?;

        _ = store.produce(None, &a, batch_at(2, recent)?).await?;
        _ = store.produce(None, &a, batch_at(2, ancient)?).await?;
        assert_eq!(4, store.high_watermark(&a).await?);

        assert_eq!(
            1,
            store.expire_prefix_segments(PREFIX, recent - 1_000).await?
        );

        store
            .watermark(&a)?
            .with_mut(&bucket, |watermark| {
                watermark.served = None;
                Ok(())
            })
            .await?;
    }

    // The lease lapses, so the next process may take it over.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // A fresh process: nothing here is answered out of the memory of the store
    // that did the expiry.
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_lease_ttl(ttl);

    assert_eq!(
        (4, None),
        store.persisted_watermark_bounds(&a).await?,
        "the gap must start out uncertified, as every existing one is"
    );

    assert_eq!(4, store.high_watermark(&a).await?);
    assert!(
        fetch_from(&store, &a, 2).await?.is_empty(),
        "an uncertified gap answers empty, with no error on either side — #290"
    );

    assert_eq!(
        1,
        store.certify_prefix_served_ends(PREFIX).await?,
        "the pass must certify the one sub-stream whose floor sits above its tail"
    );

    assert_eq!(
        (4, Some(ServedEnd { end: 2, at_high: 4 })),
        store.persisted_watermark_bounds(&a).await?,
        "the reconciliation must write the same pair the expiry would have"
    );

    // The advertised end does not move: the floor is the next offset to assign,
    // and lowering it under offsets a peer may have acked is the regression
    // `scaling` forbids. What changes is the answer inside the gap.
    assert_eq!(4, store.high_watermark(&a).await?);

    for offset in [2, 3] {
        assert!(
            matches!(
                fetch_from(&store, &a, offset).await,
                Err(Error::Api(ErrorCode::OffsetOutOfRange))
            ),
            "offset {offset} is now certified dead and must answer OFFSET_OUT_OF_RANGE"
        );
    }

    // The surviving records are untouched: this certifies a gap, it does not
    // condemn a log.
    assert!(!fetch_from(&store, &a, 0).await?.is_empty());
    assert!(fetch_from(&store, &a, 4).await?.is_empty());

    // Once per prefix per process, so the same store does no further work.
    assert_eq!(0, store.certify_prefix_served_ends(PREFIX).await?);

    // And a cold replica reaches the same answer without running the pass at
    // all, so the certification is a property of the bucket rather than of the
    // process that wrote it.
    let cold = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_lease_ttl(ttl);
    assert_eq!(4, cold.high_watermark(&a).await?);
    assert!(
        matches!(
            fetch_from(&cold, &a, 2).await,
            Err(Error::Api(ErrorCode::OffsetOutOfRange))
        ),
        "a cold replica must refuse the certified-dead gap too"
    );

    // A third process running the pass over an already-certified prefix writes
    // nothing, so a fleet restart does not cost a PUT per sub-stream per pod.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        0,
        DynoStore::new(CLUSTER, NODE, bucket.clone())
            .prefix_lease_ttl(ttl)
            .certify_prefix_served_ends(PREFIX)
            .await?,
        "an already-certified prefix must not be rewritten"
    );

    Ok(())
}

/// The reconciliation certifies nothing on a healthy prefix, and nothing on a
/// fully drained partition (#290).
///
/// Both halves matter. A pass that certified a healthy sub-stream would make a
/// live partition answer `OFFSET_OUT_OF_RANGE` — the #303 failure, which reset
/// consumers on 61 topics. A drained partition legitimately has the floor as its
/// only authority, and #299 already reports it as a log starting where it ends;
/// certifying `[0, floor)` there would be a second, redundant answer to a
/// question already answered.
#[tokio::test]
async fn the_reconciliation_leaves_a_healthy_or_drained_partition_alone() -> Result<(), Error> {
    let ttl = Duration::from_millis(80);
    let bucket = InMemory::new();

    let healthy = "org.env.conn.tab_a";
    let drained = "org.env.conn.tab_b";
    let a = Topition::new(healthy, 0);
    let b = Topition::new(drained, 0);

    let recent = now_ms();
    let ancient = 1_000;

    {
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_lease_ttl(ttl);
        create_topic(&store, healthy).await?;
        create_topic(&store, drained).await?;

        // `a` keeps everything; `b` loses its only segment, so it has no tail.
        _ = store.produce(None, &a, batch_at(2, recent)?).await?;
        _ = store.produce(None, &b, batch_at(2, ancient)?).await?;

        assert_eq!(
            1,
            store.expire_prefix_segments(PREFIX, recent - 1_000).await?
        );
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_lease_ttl(ttl);

    assert_eq!(
        0,
        store.certify_prefix_served_ends(PREFIX).await?,
        "neither a healthy nor a drained sub-stream is a gap"
    );

    assert!(
        !fetch_from(&store, &a, 0).await?.is_empty(),
        "the healthy partition still serves its records"
    );

    Ok(())
}
