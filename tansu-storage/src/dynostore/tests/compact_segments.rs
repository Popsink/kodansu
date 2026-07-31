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

//! Compacted topics over segments (#175): a compacted topic is prefix-coalesced
//! into segments under its OWN dedicated prefix (the full topic name) instead of
//! `prefix_of`'s first-three-components connector prefix, is excluded from
//! whole-segment time expiry unless its policy also contains `delete`, and is
//! per-key compacted by `compact_prefix_per_key`. Its legacy `records/` region is
//! drained into that prefix by the carry-over.
//!
//! This shipped behind `compacted_segments` / `compacted_carryover` so it could
//! roll out dark to a mixed-version fleet — any off-state drift being a #78-class
//! dual-offset-authority hazard. Both are hardwired on since #222, so the tests
//! below no longer have an off state to pin.

use std::{
    collections::BTreeSet,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{ObjectStore as _, ObjectStoreExt as _, memory::InMemory, path::Path};
use rama::{Context, Service as _};
use tansu_sans_io::{
    Compression, ErrorCode, FetchRequest, IsolationLevel, ListOffset, OpType,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    fetch_request::{FetchPartition, FetchTopic},
    incremental_alter_configs_request::AlterableConfig,
    record::{Record, deflated, inflated},
};

use crate::{Error, FetchService, Result, Storage as _, TopicId, Topition, dynostore::DynoStore};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// A store over `bucket`. Compacted-topic routing and carry-over are no longer
/// switchable (#222 hardwired both on), so there is exactly one mode: a compacted
/// topic is segment-routed under its own dedicated prefix and its legacy region
/// is drained. This used to be a `routed_store` / `unrouted_store` pair, kept
/// distinct while the flags shipped dark.
fn new_store(bucket: &InMemory) -> DynoStore {
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
    let store = new_store(&bucket);

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
    let store = new_store(&bucket);

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
    let store = new_store(&bucket);

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
        let store = new_store(&bucket);
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
    let restarted = new_store(&bucket);
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
    let store = new_store(&bucket);

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
    let store = new_store(&bucket);

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

/// An `AlterConfigs` that turns compaction ON for a live topic does NOT move where
/// its records are written (#236). The routing prefix is pinned at creation, so the
/// topic keeps the prefix its existing segments are under.
///
/// This is the dual-offset-authority window (#78) closing. While routing was
/// derived from `cleanup.policy`, the verdict was memoized per pod for
/// `HIGH_WATERMARK_HINT_TTL`, so for a few seconds after such an `AlterConfigs` one
/// pod routed a batch to the dedicated prefix while a peer still routed the same
/// `(topic, partition)` to the connector prefix — two create-CAS namespaces
/// assigning offsets for one partition. Pinned, there is no window at all, and the
/// value becomes permanently cacheable, which is where the ~$38/day goes.
#[tokio::test]
async fn alter_configs_does_not_move_pinned_routing() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);

    let topic = "org.env.conn.live";
    create_topic_with_configs(&store, topic, &[("cleanup.policy", "delete")]).await?;
    let tp = Topition::new(topic, 0);

    assert_eq!(
        0,
        store.produce(None, &tp, keyed_batch(b"k", b"v1")?).await?
    );
    assert_eq!(1, segments_of(&bucket, "org.env.conn").await.len());
    assert!(segments_of(&bucket, topic).await.is_empty());

    // Turn compaction on: the operation that used to move the routing.
    store
        .topic_meta(topic)?
        .with_mut(&store.object_store, |metadata| {
            metadata.alter_configs(&[AlterableConfig::default()
                .name("cleanup.policy".into())
                .config_operation(OpType::Set.into())
                .value(Some("compact".into()))])
        })
        .await?;

    // A pod with a cold memo is the interesting one: it reads the pin, not the
    // config it would otherwise find changed.
    let peer = new_store(&bucket);
    assert_eq!(
        "org.env.conn",
        peer.routed_prefix_of(&tp).await?,
        "a pinned topic's routing must not follow cleanup.policy"
    );

    assert_eq!(
        1,
        store.produce(None, &tp, keyed_batch(b"k", b"v2")?).await?
    );
    assert_eq!(
        2,
        segments_of(&bucket, "org.env.conn").await.len(),
        "the second produce must land in the same prefix as the first — one segment each, \
         since an awaited produce flushes"
    );
    assert!(
        segments_of(&bucket, topic).await.is_empty(),
        "no dedicated-prefix segment may appear for a topic pinned to the connector prefix"
    );

    Ok(())
}

/// A topic created before pinning existed has no pin, and the fallback must
/// reproduce **exactly** the old derivation before pinning it (#236) — for both
/// classes. Getting this wrong is not a cost regression: new records would land
/// under a prefix the topic's existing segments are not under, and what is already
/// written would become unreachable.
#[tokio::test]
async fn an_unpinned_topic_keeps_the_routing_it_already_has() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();

    for (topic, configs, expected) in [
        (
            "org.env.conn.plain",
            &[("cleanup.policy", "delete")][..],
            "org.env.conn",
        ),
        (
            "org.env.conn.state",
            &[("cleanup.policy", "compact")][..],
            "org.env.conn.state",
        ),
    ] {
        let store = new_store(&bucket);
        create_topic_with_configs(&store, topic, configs).await?;

        // Erase the pin: this is what a pre-#236 topic looks like in a production
        // bucket — metadata object, segments, no `topic-routing/` object.
        bucket.delete(&store.topic_routing_path(topic)).await?;

        let cold = new_store(&bucket);
        let tp = Topition::new(topic, 0);
        assert_eq!(
            expected,
            cold.routed_prefix_of(&tp).await?,
            "the fallback must reproduce the pre-pin derivation for {topic}"
        );

        // … and it pins that answer, so this is the last derivation anyone pays for.
        assert_eq!(
            Some(expected.to_owned()),
            cold.read_routing_pin(topic).await?,
            "the derived answer must be pinned for {topic}"
        );
    }

    Ok(())
}

/// Two pods that race the lazy pin converge on one value: create-only means the
/// first writer wins and the loser adopts the winner's prefix (#236).
///
/// Without that, a permanent per-pod cache would turn the old bounded window into a
/// permanent split — two offset authorities for one partition, for as long as both
/// pods live.
#[tokio::test]
async fn a_lazily_pinned_prefix_converges_across_pods() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let topic = "org.env.conn.race";

    let seed = new_store(&bucket);
    create_topic_with_configs(&seed, topic, &[("cleanup.policy", "delete")]).await?;
    bucket.delete(&seed.topic_routing_path(topic)).await?;

    // One pod pins the answer it derives; the other arrives after and must adopt it
    // rather than keeping its own.
    let winner = new_store(&bucket);
    let loser = new_store(&bucket);
    let tp = Topition::new(topic, 0);

    let first = winner.routed_prefix_of(&tp).await?;
    let second = loser.routed_prefix_of(&tp).await?;

    assert_eq!(first, second, "pods must agree on a lazily pinned prefix");
    assert_eq!(Some(first), loser.read_routing_pin(topic).await?);

    Ok(())
}

/// A re-created topic does not inherit its predecessor's routing: the pin is
/// deleted with the topic, and `create_topic` overwrites rather than writing
/// create-only, so even a torn delete cannot leave a stale pin in force (#236).
#[tokio::test]
async fn a_recreated_topic_is_pinned_afresh() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);

    let topic = "org.env.conn.reborn";
    create_topic_with_configs(&store, topic, &[("cleanup.policy", "compact")]).await?;
    assert_eq!(Some(topic.to_owned()), store.read_routing_pin(topic).await?);

    assert_eq!(
        ErrorCode::None,
        store.delete_topic(&TopicId::Name(topic.into())).await?
    );
    assert_eq!(None, store.read_routing_pin(topic).await?);

    create_topic_with_configs(&store, topic, &[("cleanup.policy", "delete")]).await?;
    assert_eq!(
        Some("org.env.conn".to_owned()),
        store.read_routing_pin(topic).await?,
        "a re-created topic is pinned from its own config"
    );
    assert_eq!(
        "org.env.conn",
        store.routed_prefix_of(&Topition::new(topic, 0)).await?,
        "the in-process memo must not survive the delete"
    );

    Ok(())
}

/// #253: the per-key pass repairs a batch whose LZ4 frame has dependent blocks,
/// even when it has nothing to compact.
///
/// Every LZ4 frame this broker wrote before the encoder was corrected has linked
/// blocks, which `KafkaLZ4BlockInputStream` rejects in its constructor — so a Java
/// worker dies on the partition while a Go client reads it fine. The fix to the
/// encoder stops new damage; it does nothing for what is already durable, and
/// nothing else would: this pass re-encodes a batch only when it *removes* records
/// from it, and an emptied compaction remnant has no keys left to supersede. The
/// batch blocking a production worker is exactly such a remnant.
///
/// The frame here is built with a linked-block encoder on purpose. The corrected
/// encoder cannot produce one any more, so a test that used our own writer would
/// assert nothing.
#[tokio::test]
async fn per_key_pass_repairs_dependent_lz4_frames() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);

    let topic = "org.env.conn.frames";
    create_topic_with_configs(&store, topic, &[("cleanup.policy", "compact")]).await?;
    let tp = Topition::new(topic, 0);

    // A batch carrying a linked-block frame, as a pre-fix broker wrote it.
    let mut linked = inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(b"k")))
                .value(Some(Bytes::from_static(b"v"))),
        )
        .attributes(Compression::Lz4.into())
        .build()
        .and_then(deflated::Batch::try_from)?;
    linked.record_data = dependent_lz4(&linked.record_data)?;

    assert!(
        linked.has_dependent_lz4_blocks(),
        "the seeded batch must carry the damage this test repairs"
    );

    _ = store.produce(None, &tp, linked).await?;

    // Nothing to compact — one key, one record — so only the frame can dirty it.
    assert_eq!(0, store.compact_prefix_per_key(topic).await?);

    let repaired = store
        .fetch(
            &tp,
            0,
            1,
            64 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(500),
        )
        .await?;

    assert_eq!(1, repaired.len(), "the record must survive the repair");
    assert!(
        !repaired[0].has_dependent_lz4_blocks(),
        "the rewritten batch must carry an independent-block frame"
    );

    let values: Vec<Bytes> = repaired
        .iter()
        .filter_map(|batch| inflated::Batch::try_from(batch).ok())
        .flat_map(|batch| batch.records)
        .filter_map(|record| record.value)
        .collect();
    assert_eq!(vec![Bytes::from_static(b"v")], values);

    Ok(())
}

/// Re-compress `record_data` as an LZ4 frame with **linked** blocks — what the
/// `lz4` crate emitted by default before #253, and what no Kafka Java client can
/// read.
fn dependent_lz4(independent: &Bytes) -> Result<Bytes, Error> {
    use std::io::Write as _;

    let records = lz4::Decoder::new(&independent[..])
        .and_then(|mut decoder| {
            let mut plain = Vec::new();
            std::io::copy(&mut decoder, &mut plain).map(|_| plain)
        })
        .map_err(Error::from)?;

    let mut encoder = lz4::EncoderBuilder::new()
        .block_mode(lz4::BlockMode::Linked)
        .build(Vec::new())
        .map_err(Error::from)?;
    encoder.write_all(&records[..]).map_err(Error::from)?;
    let (buffer, result) = encoder.finish();
    result.map_err(Error::from)?;

    Ok(Bytes::from(buffer))
}
