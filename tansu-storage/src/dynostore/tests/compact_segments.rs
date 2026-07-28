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
use object_store::{ObjectStore as _, memory::InMemory, path::Path};
use tansu_sans_io::{
    IsolationLevel, ListOffset,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
};

use crate::{Error, Result, Storage as _, Topition, dynostore::DynoStore};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// Segment mode with compacted-topic routing on (#175).
fn routed_store(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true)
        .compacted_segments(true)
}

/// Segment mode with the routing flag OFF — today's shipped behaviour.
fn unrouted_store(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true)
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

/// With the flag OFF, routing is byte-identical to today for every policy —
/// and with it ON, a non-compacted topic is untouched. Without this, deploying
/// the (dark) flag to one pod of a fleet would already split the prefix the
/// pods resolve for the same topic: two offset authorities for one sub-stream,
/// the #78 corruption.
#[tokio::test]
async fn flag_off_routing_is_byte_identical() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = unrouted_store(&bucket);

    create_topic_with_configs(
        &store,
        "org.env.conn.config",
        &[("cleanup.policy", "compact")],
    )
    .await?;
    let compacted = Topition::new("org.env.conn.config", 0);

    // Flag off: a dotted compacted topic still resolves the SHARED connector
    // prefix, exactly as `prefix_of` always has.
    assert_eq!(
        store.prefix_of(&compacted),
        store.routed_prefix_of(&compacted).await?
    );
    assert_eq!("org.env.conn", store.routed_prefix_of(&compacted).await?);

    // Flag on: a NON-compacted topic keeps the shared prefix — the override
    // exists for compacted topics only.
    let routed = routed_store(&bucket);
    create_topic_with_configs(&routed, "org.env.conn.table", &[]).await?;
    let plain = Topition::new("org.env.conn.table", 0);
    assert_eq!("org.env.conn", routed.routed_prefix_of(&plain).await?);

    // Flag on, compacted: the dedicated prefix is the full topic name.
    assert_eq!(
        "org.env.conn.config",
        routed.routed_prefix_of(&compacted).await?
    );

    // A dotless topic is its own prefix under every combination.
    create_topic_with_configs(&routed, "kv", &[("cleanup.policy", "compact")]).await?;
    let dotless = Topition::new("kv", 0);
    assert_eq!("kv", routed.routed_prefix_of(&dotless).await?);
    assert_eq!("kv", store.routed_prefix_of(&dotless).await?);

    Ok(())
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

    // Flag off: every compacted topic is skipped — no threshold under either
    // its own name or the shared prefix beyond the sibling's.
    let unrouted = unrouted_store(&bucket);
    let thresholds = unrouted.segment_retention_thresholds(now, None).await?;
    assert!(!thresholds.contains_key("org.env.conn.config"));
    assert!(!thresholds.contains_key("org.env.conn.status"));
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

    // Flag off: empty — the pass must not exist for an unrouted fleet.
    let unrouted = unrouted_store(&bucket);
    assert!(unrouted.per_key_compact_prefixes(None).await?.is_empty());

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
        for (key, value) in [
            (b"k1".as_slice(), b"v1".as_slice()),
            (b"k2".as_slice(), b"v2".as_slice()),
            (b"k3".as_slice(), b"v3".as_slice()),
            (b"k4".as_slice(), b"v4".as_slice()),
        ] {
            _ = store.produce(None, &tp, keyed_batch(key, value)?).await?;
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
        _ = store
            .produce(None, &tp, keyed_batch(b"key", b"stale")?)
            .await?;
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

/// With the carry-over flag off, a routed compacted topic's legacy region is
/// left strictly alone — the routing flip and the drain are two config deploys,
/// and the first must not start the second.
#[tokio::test]
async fn carryover_off_leaves_the_legacy_region_untouched() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let topic = "org.env.conn.configs";
    let tp = Topition::new(topic, 0);

    {
        let store = unrouted_store(&bucket);
        create_topic_with_configs(&store, topic, &[("cleanup.policy", "compact")]).await?;
        _ = store.produce(None, &tp, keyed_batch(b"a", b"1")?).await?;
        _ = store.produce(None, &tp, keyed_batch(b"b", b"2")?).await?;
    }
    let before = legacy_records(&bucket, topic).await;
    assert_eq!(2, before.len());

    // Routing on, carry-over off: maintenance must not retire a single object.
    let store = routed_store(&bucket);
    store.maintain(SystemTime::now()).await?;
    assert_eq!(before, legacy_records(&bucket, topic).await);

    // And the explicit call is a no-op too, not merely unreached.
    let mut budget = 8usize;
    assert_eq!(0, store.carry_over_legacy(&tp, &mut budget).await?);
    assert_eq!(8, budget, "an inert pass must not spend the tick's budget");

    Ok(())
}
