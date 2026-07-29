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

//! Compacted topics over segments (#175): behind the `compacted_segments`
//! flag, a compacted topic is prefix-coalesced into segments under its OWN
//! dedicated prefix (the full topic name) instead of `prefix_of`'s
//! first-three-components connector prefix, is excluded from whole-segment
//! time expiry unless its policy also contains `delete`, and is per-key
//! compacted by `compact_prefix_per_key`. With the flag off, routing must be
//! byte-identical to a build without this code: the flag ships dark to a
//! mixed-version fleet, and any off-state drift is a #78-class
//! dual-offset-authority hazard.

use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{ObjectStore as _, ObjectStoreExt as _, memory::InMemory, path::Path};
use rama::{Context, Service as _};
use tansu_sans_io::{
    FetchRequest, IsolationLevel, ListOffset,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    fetch_request::{FetchPartition, FetchTopic},
    record::{Record, deflated, inflated},
};

use crate::{Error, FetchService, Result, Storage as _, Topition, dynostore::DynoStore};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// Segment mode with compacted-topic routing on (#175).
fn routed_store(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone()).compacted_segments(true)
}

/// Segment mode with the routing flag OFF — today's shipped behaviour.
fn unrouted_store(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone())
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

fn keyed_batch(key: &'static [u8], value: &'static [u8]) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(key)))
                .value(Some(Bytes::from_static(value))),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// The live segment objects under a prefix.
async fn segments_of(bucket: &InMemory, prefix: &str) -> Vec<Path> {
    let listing = Path::from(format!("clusters/{CLUSTER}/prefixes/{prefix}/segments/"));
    bucket
        .list(Some(&listing))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .expect("list segments")
}

/// Seed a legacy `records/{offset:020}.batch` object directly into the bucket.
///
/// A *compacted* topic can be seeded by producing through a routing-off store,
/// because the `topic_is_compacted` gate sends it to `records/`. A
/// **non-compacted** one cannot: it is segment-routed from its first write
/// whatever the flags say. So the retain-forever cases below — which are not
/// compacted — have to write the legacy object themselves. Nothing writes this
/// layout any more (#177); the objects still exist in production buckets.
async fn seed_legacy_batch(
    store: &DynoStore,
    bucket: &InMemory,
    topition: &Topition,
    base_offset: i64,
    batch: deflated::Batch,
) -> Result<()> {
    let location = Path::from(format!(
        "clusters/{CLUSTER}/topics/{}/partitions/{:0>10}/records/{:0>20}.batch",
        topition.topic(),
        topition.partition(),
        base_offset,
    ));
    _ = bucket.put(&location, store.encode_frame(&[batch])?).await?;
    Ok(())
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

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

/// A compacted dotted topic's produce lands in a segment under its OWN prefix
/// — not the connector prefix shared with its CDC siblings, and not legacy
/// `records/`. Without this, the topic's segments would share objects whose
/// whole-segment expiry (#61) runs at the SIBLINGS' retention, deleting
/// old-but-latest keys.
#[tokio::test]
async fn dotted_compacted_topic_routes_to_its_own_prefix() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = routed_store(&bucket);

    create_topic_with_configs(
        &store,
        "org.env.conn.config",
        &[("cleanup.policy", "compact")],
    )
    .await?;
    let tp = Topition::new("org.env.conn.config", 0);

    let offset = store.produce(None, &tp, keyed_batch(b"k", b"v")?).await?;
    assert_eq!(0, offset);

    assert_eq!(1, segments_of(&bucket, "org.env.conn.config").await.len());
    assert!(segments_of(&bucket, "org.env.conn").await.is_empty());
    assert!(
        legacy_records(&bucket, "org.env.conn.config")
            .await
            .is_empty()
    );

    Ok(())
}

/// A compact-only prefix yields NO retention threshold, while `compact,delete`
/// keeps one from the topic's OWN `retention.ms`. Without the exclusion,
/// whole-segment time expiry would delete a compacted topic's only copy of a
/// key once it is older than the retention window — the invariant compaction
/// exists to provide.
#[tokio::test]
async fn compact_only_prefix_has_no_retention_threshold() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = routed_store(&bucket);

    create_topic_with_configs(
        &store,
        "org.env.conn.config",
        &[("cleanup.policy", "compact")],
    )
    .await?;
    create_topic_with_configs(
        &store,
        "org.env.conn.status",
        &[
            ("cleanup.policy", "compact,delete"),
            ("retention.ms", "60000"),
        ],
    )
    .await?;
    create_topic_with_configs(&store, "org.env.conn.table", &[]).await?;

    let now = now_ms();
    let thresholds = store.segment_retention_thresholds(now, None).await?;

    // Compact-only: no entry, so `expire_prefix_segments` can never reach it.
    assert!(!thresholds.contains_key("org.env.conn.config"));

    // `compact,delete`: its own retention on its own dedicated prefix.
    assert_eq!(Some(&(now - 60_000)), thresholds.get("org.env.conn.status"));

    // The delete-policy sibling keeps the shared prefix's threshold, as today.
    assert!(thresholds.contains_key("org.env.conn"));

    Ok(())
}

/// The dedicated prefixes of compacted topics join the maintenance claim
/// universe and the per-key set once routed. Without this, no maintainer would
/// ever hold their lease and the per-key pass would never run — compaction
/// silently off for exactly the topics the flag exists for.
#[tokio::test]
async fn per_key_set_holds_the_dedicated_prefixes() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = routed_store(&bucket);

    create_topic_with_configs(
        &store,
        "org.env.conn.config",
        &[("cleanup.policy", "compact")],
    )
    .await?;
    create_topic_with_configs(&store, "org.env.conn.table", &[]).await?;

    let per_key = store.per_key_compact_prefixes(None).await?;
    assert_eq!(BTreeSet::from(["org.env.conn.config".to_owned()]), per_key);

    Ok(())
}

/// After a per-key merge the offset span survives a COLD restart: emptied
/// batches kept as headers keep footer `record_count` equal to the sub-stream
/// span, so recovery reads the same tail and the next produce continues from
/// it. Without this, a restarted broker would re-mint offsets the compacted-
/// away records used to hold — offset reuse, the unrecoverable corruption.
#[tokio::test]
async fn span_and_tail_survive_per_key_merge_across_restart() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let tp = Topition::new("org.env.conn.config", 0);

    {
        let store = routed_store(&bucket);
        create_topic_with_configs(
            &store,
            "org.env.conn.config",
            &[("cleanup.policy", "compact")],
        )
        .await?;

        for value in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            _ = store
                .produce(None, &tp, keyed_batch(b"alpha", value)?)
                .await?;
        }
        assert_eq!(3, store.high_watermark(&tp).await?);

        store.maintain(SystemTime::now()).await?;

        // The rewrite happened: the two superseded copies are gone on a fetch
        // from 0, at unchanged offsets.
        let batches = store
            .fetch(
                &tp,
                0,
                1,
                64 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(500),
            )
            .await?;
        let survivors: i64 = batches
            .iter()
            .map(|batch| i64::from(batch.record_count))
            .sum();
        assert_eq!(1, survivors);
    }

    // A fresh process (cold caches) recovers the SAME tail from the footers...
    let restarted = routed_store(&bucket);
    assert_eq!(3, restarted.high_watermark(&tp).await?);

    let responses = restarted
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[
                (tp.clone(), ListOffset::Earliest),
                (tp.clone(), ListOffset::Latest),
            ],
        )
        .await?;
    assert_eq!(Some(0), responses[0].1.offset);
    assert_eq!(Some(3), responses[1].1.offset);

    // ...and continues producing from it, never reusing a compacted offset.
    let next = restarted
        .produce(None, &tp, keyed_batch(b"alpha", b"four")?)
        .await?;
    assert_eq!(3, next);

    Ok(())
}

/// Fetch `tp` from `from` **through [`FetchService`]**, with an explicit byte
/// budget, and return the batches the client would see.
///
/// Going through the service rather than `DynoStore::fetch` is the point: both
/// #219 and #228 lived entirely in the service's walk over what storage handed
/// it, so a storage-level assertion cannot see either.
async fn service_fetch(
    store: &DynoStore,
    tp: &Topition,
    from: i64,
    max_bytes: i32,
) -> Result<Vec<deflated::Batch>> {
    let response = FetchService
        .serve(
            Context::with_state(store.clone()),
            FetchRequest::default()
                .max_wait_ms(500)
                .min_bytes(1)
                .max_bytes(Some(max_bytes))
                .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(tp.topic().to_owned()))
                        .partitions(Some(
                            [FetchPartition::default()
                                .partition(tp.partition())
                                .fetch_offset(from)
                                .partition_max_bytes(max_bytes)]
                            .into(),
                        ))]
                    .into(),
                )),
        )
        .await?;

    Ok(response
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| topic.partitions.unwrap_or_default())
        .filter_map(|partition| partition.records)
        .flat_map(|frame| frame.batches)
        .collect())
}

/// A consumer reading a compacted partition **through the Fetch service** from
/// an offset whose batch was compacted away still receives the surviving
/// records (#219).
///
/// The storage layer was always right here — it returns the emptied headers and
/// the survivor together. [`FetchService`] discarded the whole read because its
/// loop treated a leading `record_count == 0` batch as end-of-stream, and
/// advanced by `record_count` (0 on a header) rather than by the batch's offset
/// span. Since compaction does not move the log start, offset 0 stays valid and
/// resolves to a header — so a fresh `auto.offset.reset=earliest` consumer read
/// nothing at all, forever, while the high watermark sat above it.
///
/// This has to go through the service, not `DynoStore::fetch`: the defect was
/// entirely in the service's walk over what storage handed it, so a
/// storage-level assertion cannot see it.
#[tokio::test]
async fn fetch_service_serves_survivors_past_compacted_headers() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let tp = Topition::new("org.env.conn.config", 0);
    let store = routed_store(&bucket);

    create_topic_with_configs(
        &store,
        "org.env.conn.config",
        &[("cleanup.policy", "compact")],
    )
    .await?;

    // One key, three writes: compaction keeps the last and empties offsets 0
    // and 1 down to headers.
    for value in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        _ = store
            .produce(None, &tp, keyed_batch(b"alpha", value)?)
            .await?;
    }
    store.maintain(SystemTime::now()).await?;

    // From 0 — the compacted-away offset a fresh consumer resolves to. The
    // survivor comes back; before the fix this was empty.
    let batches = service_fetch(&store, &tp, 0, 64 * 1024).await?;
    assert_eq!(
        1,
        batches
            .iter()
            .map(|batch| i64::from(batch.record_count))
            .sum::<i64>()
    );
    let survivor = batches
        .iter()
        .find(|batch| batch.record_count > 0)
        .expect("survivor");
    assert_eq!(2, survivor.base_offset);

    // The emptied headers are carried in the response rather than filtered out,
    // each still spanning the offset it stands for, so a client can skip past
    // them: every offset in `[0, 3)` is accounted for exactly once.
    let mut spanned: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch.base_offset..=batch.base_offset + i64::from(batch.last_offset_delta)
        })
        .collect();
    spanned.sort_unstable();
    assert_eq!(vec![0, 1, 2], spanned);

    // And a consumer already positioned past the headers is unaffected.
    let batches = service_fetch(&store, &tp, 2, 64 * 1024).await?;
    assert_eq!(1, batches.len());
    assert_eq!(2, batches[0].base_offset);
    assert_eq!(1, batches[0].record_count);

    // At the high watermark there is nothing to serve and the walk terminates.
    assert!(service_fetch(&store, &tp, 3, 64 * 1024).await?.is_empty());

    Ok(())
}

/// A fetch over a heavily-compacted partition is bounded by the client's byte
/// budget instead of walking to the high watermark (#228).
///
/// Emptied batch headers cost no `record_data`, so while `ByteSize for Batch`
/// charged records alone a run of them spent **none** of `max_bytes`: the walk
/// introduced by #219 accumulated every batch from the fetch offset to the high
/// watermark, unbounded in memory and on the wire. Two production brokers grew
/// past 1.4 GiB within three hours of the beta.28 deploy on this.
///
/// The budget must bound the walk, and what comes back must still let a consumer
/// advance — a bounded response that skipped offsets would be a silent gap.
#[tokio::test]
async fn a_tight_byte_budget_bounds_the_walk_over_compacted_headers() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    const RECORDS: i64 = 64;

    let bucket = InMemory::new();
    let tp = Topition::new("org.env.conn.config", 0);
    let store = routed_store(&bucket);

    create_topic_with_configs(
        &store,
        "org.env.conn.config",
        &[("cleanup.policy", "compact")],
    )
    .await?;

    // One key, many writes: compaction keeps the last and leaves RECORDS-1
    // emptied headers behind it.
    for _ in 0..RECORDS {
        _ = store
            .produce(None, &tp, keyed_batch(b"alpha", b"v")?)
            .await?;
    }
    store.maintain(SystemTime::now()).await?;
    assert_eq!(RECORDS, store.high_watermark(&tp).await?);

    // A generous budget reaches the survivor at the tail.
    let generous = service_fetch(&store, &tp, 0, 64 * 1024).await?;
    assert_eq!(
        1,
        generous
            .iter()
            .map(|batch| i64::from(batch.record_count))
            .sum::<i64>(),
        "the survivor must be served when the budget allows the whole span"
    );

    // A tight budget stops early rather than accumulating the whole span. 512
    // bytes buys ~8 batch headers; without the fix this returned all 64.
    let tight = service_fetch(&store, &tp, 0, 512).await?;
    assert!(
        !tight.is_empty(),
        "a bounded walk must still return something to advance on"
    );
    assert!(
        (tight.len() as i64) < RECORDS,
        "the walk must be bounded by max_bytes, got all {RECORDS} batches"
    );

    // Whatever came back starts at the requested offset and is contiguous, so the
    // consumer can resume from the last batch's end and lose nothing.
    assert_eq!(0, tight[0].base_offset);
    let mut expected = 0;
    for batch in &tight {
        assert_eq!(
            expected, batch.base_offset,
            "a bounded response must not skip offsets"
        );
        expected = batch.base_offset + i64::from(batch.last_offset_delta) + 1;
    }

    // Resuming where the bounded response ended eventually reaches the survivor,
    // so the consumer makes progress rather than stalling.
    let mut from = expected;
    let mut guard = 0;
    let mut saw_survivor = false;
    while from < RECORDS && guard < RECORDS {
        let next = service_fetch(&store, &tp, from, 512).await?;
        if next.is_empty() {
            break;
        }
        assert_eq!(from, next[0].base_offset, "resume must not skip offsets");
        if next.iter().any(|batch| batch.record_count > 0) {
            saw_survivor = true;
        }
        from = next
            .last()
            .map(|batch| batch.base_offset + i64::from(batch.last_offset_delta) + 1)
            .expect("non-empty");
        guard += 1;
    }
    assert!(
        saw_survivor,
        "stepping through with a tight budget must still reach the survivor"
    );

    Ok(())
}

/// Carry-over drains the legacy region **one chunk at a time**, and every
/// intermediate state must still be fully readable (#175 release 2).
///
/// This is the test that matters. The legacy region is the hybrid seam `[0, C)`
/// and [`DynoStore::fetch`] reads a *gap* in it as "compacted away" — it skips
/// silently rather than erroring. So a carry-over that removed objects from the
/// middle would make live keys invisible with no signal, which for a connector
/// offsets topic means re-snapshotting its source. Carrying tail-first and
/// deleting descending is what keeps every intermediate region a contiguous
/// `[0, x)`; driving the drain with a budget of one object per call exercises
/// exactly those intermediate states.
#[tokio::test]
async fn carryover_drains_legacy_chunk_by_chunk_with_no_hole() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let topic = "org.env.conn.offsets";
    let tp = Topition::new(topic, 0);

    // Pre-release-2 state: a compacted topic whose data sits in legacy
    // `records/`, one object per batch, and no segment anywhere.
    {
        let store = unrouted_store(&bucket);
        create_topic_with_configs(&store, topic, &[("cleanup.policy", "compact")]).await?;
        // Written directly: routing is unconditional now, so producing would
        // land in segments rather than the legacy region this test drains.
        for (offset, (key, value)) in [
            (b"k1".as_slice(), b"v1".as_slice()),
            (b"k2".as_slice(), b"v2".as_slice()),
            (b"k3".as_slice(), b"v3".as_slice()),
            (b"k4".as_slice(), b"v4".as_slice()),
        ]
        .into_iter()
        .enumerate()
        {
            seed_legacy_batch(
                &store,
                &bucket,
                &tp,
                offset as i64,
                keyed_batch(key, value)?,
            )
            .await?;
        }
        assert_eq!(4, legacy_records(&bucket, topic).await.len());
        assert!(segments_of(&bucket, topic).await.is_empty());
    }

    let store = routed_store(&bucket).compacted_carryover(true);
    assert_eq!(4, store.high_watermark(&tp).await?);

    async fn readable(store: &DynoStore, tp: &Topition) -> Result<i64> {
        Ok(store
            .fetch(
                tp,
                0,
                1,
                64 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(500),
            )
            .await?
            .iter()
            .map(|batch| i64::from(batch.record_count))
            .sum())
    }

    // Every record is readable before the drain starts.
    assert_eq!(4, readable(&store, &tp).await?);

    // One object per call: each iteration is an intermediate state a crash could
    // leave behind, and each must still expose all four records.
    for expected_legacy in [3usize, 2, 1, 0] {
        let mut budget = 1usize;
        let retired = store.carry_over_legacy(&tp, &mut budget).await?;
        assert_eq!(1, retired, "one object per budgeted call");
        assert_eq!(
            expected_legacy,
            legacy_records(&bucket, topic).await.len(),
            "legacy region must shrink from the tail only"
        );
        assert_eq!(
            4,
            readable(&store, &tp).await?,
            "a partially drained region must not hide a single record"
        );
    }

    // Fully drained, and the data now lives under the topic's dedicated prefix.
    assert!(legacy_records(&bucket, topic).await.is_empty());
    assert!(!segments_of(&bucket, topic).await.is_empty());
    assert_eq!(4, store.high_watermark(&tp).await?);

    // A cold process reads the same log from the footers alone.
    let restarted = routed_store(&bucket).compacted_carryover(true);
    assert_eq!(4, readable(&restarted, &tp).await?);
    assert_eq!(4, restarted.high_watermark(&tp).await?);

    Ok(())
}

/// The carry-over re-encodes **verbatim** and never computes latest-per-key
/// itself (#175 release 2). Once routing is on, a segment write may already
/// supersede a legacy key; folding the legacy region alone would elect the stale
/// value and resurrect it. Carrying as-is leaves release 1's per-key pass to
/// decide with the full picture.
#[tokio::test]
async fn carryover_does_not_resurrect_a_superseded_key() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let topic = "org.env.conn.state";
    let tp = Topition::new(topic, 0);

    // `stale` lands in the legacy region...
    {
        let store = unrouted_store(&bucket);
        create_topic_with_configs(&store, topic, &[("cleanup.policy", "compact")]).await?;
        seed_legacy_batch(&store, &bucket, &tp, 0, keyed_batch(b"key", b"stale")?).await?;
        assert_eq!(1, legacy_records(&bucket, topic).await.len());
    }

    // ...and `fresh` supersedes it in a segment, after the routing flip.
    let store = routed_store(&bucket).compacted_carryover(true);
    _ = store
        .produce(None, &tp, keyed_batch(b"key", b"fresh")?)
        .await?;

    // Drive the carry through its real entry point. Calling `carry_over_legacy`
    // directly would take the compaction lease *outside* the #126 claim, so the
    // next claim would skip this prefix as recently maintained and the per-key
    // pass would never run — a sequence production never performs, since the
    // carry-over runs inside `maintain` after the claim.
    store.maintain(SystemTime::now()).await?;
    assert!(
        legacy_records(&bucket, topic).await.is_empty(),
        "maintenance must drain the legacy region"
    );
    assert_eq!(
        2,
        segments_of(&bucket, topic).await.len(),
        "the carried region is its own segment beside the post-flip one"
    );

    // One tick suffices: the #126 claim is computed *before* the carry-over runs,
    // so the prefix is already owned when the per-key pass reaches it later in the
    // same tick. Nothing is left for a second pass to remove.
    assert_eq!(
        0,
        store.compact_prefix_per_key(topic).await?,
        "the carried copy was already elected against within the same tick"
    );

    let batches = store
        .fetch(
            &tp,
            0,
            1,
            64 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(500),
        )
        .await?;
    let values: Vec<Bytes> = batches
        .iter()
        .filter_map(|batch| inflated::Batch::try_from(batch).ok())
        .flat_map(|batch| batch.records)
        .filter_map(|record| record.value)
        .collect();
    assert_eq!(
        vec![Bytes::from_static(b"fresh")],
        values,
        "the surviving value must be the post-flip one, not the carried legacy copy"
    );

    Ok(())
}

/// #211: a **retain-forever** topic's legacy region is carried over too, not
/// just a compacted one's.
///
/// The production case, reproduced: a schema-history topic with
/// `cleanup.policy=delete` and `retention.ms` at `i64::MAX`, holding legacy
/// objects and *no segment at all*. The original predicate selected on
/// `compact`, so it skipped this topic entirely — and #179, which deletes the
/// legacy read paths, would then have made it read as **empty**. A sweep of the
/// production bucket found five such topics, one of them exactly this shape.
///
/// A schema history is what a connector needs to decode its own journal, so
/// losing it is not losing replayable history — it is losing the ability to read
/// what remains. `retention.ms` at infinity is an operator saying so.
#[tokio::test]
async fn carryover_drains_a_retain_forever_topic() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let topic = "org.env.conn.schema";
    let tp = Topition::new(topic, 0);

    // Pre-cutover: everything in legacy `records/`, no segment anywhere — the
    // shape that would read as empty after #179.
    {
        let store = unrouted_store(&bucket);
        create_topic_with_configs(
            &store,
            topic,
            &[
                ("cleanup.policy", "delete"),
                ("retention.ms", &i64::MAX.to_string()),
            ],
        )
        .await?;
        seed_legacy_batch(&store, &bucket, &tp, 0, keyed_batch(b"s1", b"ddl-1")?).await?;
        seed_legacy_batch(&store, &bucket, &tp, 1, keyed_batch(b"s2", b"ddl-2")?).await?;
    }
    assert_eq!(2, legacy_records(&bucket, topic).await.len());
    // Not compacted, so its segments would live under the *connector* prefix,
    // not a dedicated one — and there are none yet.
    assert!(segments_of(&bucket, "org.env.conn").await.is_empty());

    // Carry-over on. The topic is not compacted, so before #211 this drained
    // nothing at all.
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .compacted_segments(true)
        .compacted_carryover(true);

    store.maintain(SystemTime::now()).await?;

    assert!(
        legacy_records(&bucket, topic).await.is_empty(),
        "the legacy region must be drained, not abandoned",
    );
    assert!(
        !segments_of(&bucket, "org.env.conn").await.is_empty(),
        "its content must have landed in segments under the connector prefix",
    );

    // The whole point: still readable, from the segments alone.
    let readable: i64 = store
        .fetch(
            &tp,
            0,
            1,
            64 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(500),
        )
        .await?
        .iter()
        .map(|batch| i64::from(batch.record_count))
        .sum();
    assert_eq!(2, readable, "both schema records survive the carry-over");
    assert_eq!(2, store.high_watermark(&tp).await?);

    Ok(())
}

/// `retention.ms=-1` is the documented spelling of retain-forever and selects
/// the same way as `i64::MAX` (#211) — production carries the latter, the docs
/// say the former, and both mean expiry can never fire.
#[tokio::test]
async fn carryover_treats_minus_one_retention_as_retain_forever() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let topic = "org.env.conn.history";
    let tp = Topition::new(topic, 0);

    {
        let store = unrouted_store(&bucket);
        create_topic_with_configs(&store, topic, &[("retention.ms", "-1")]).await?;
        seed_legacy_batch(&store, &bucket, &tp, 0, keyed_batch(b"h1", b"v1")?).await?;
    }
    assert_eq!(1, legacy_records(&bucket, topic).await.len());

    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .compacted_segments(true)
        .compacted_carryover(true);
    store.maintain(SystemTime::now()).await?;

    assert!(legacy_records(&bucket, topic).await.is_empty());

    Ok(())
}

/// An ordinary topic is still abandoned in place — the epic's decision (2)
/// (#211 widens the carve-out, it does not remove it).
///
/// Without this, "carry everything" would pass the two tests above while
/// re-encoding the 3,000-odd hybrid seams the epic deliberately writes off, and
/// the widening would be indistinguishable from abandoning the decision.
#[tokio::test]
async fn carryover_still_leaves_an_ordinary_topic_alone() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let topic = "org.env.conn.ordinary";
    let tp = Topition::new(topic, 0);

    {
        let store = unrouted_store(&bucket);
        create_topic_with_configs(
            &store,
            topic,
            &[("cleanup.policy", "delete"), ("retention.ms", "604800000")],
        )
        .await?;
        seed_legacy_batch(&store, &bucket, &tp, 0, keyed_batch(b"o1", b"v1")?).await?;
    }
    let before = legacy_records(&bucket, topic).await;
    assert_eq!(1, before.len());

    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .compacted_segments(true)
        .compacted_carryover(true);
    store.maintain(SystemTime::now()).await?;

    assert_eq!(
        before,
        legacy_records(&bucket, topic).await,
        "a week's retention is abandonable history: decision (2) still applies",
    );

    Ok(())
}
