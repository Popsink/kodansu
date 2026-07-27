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
