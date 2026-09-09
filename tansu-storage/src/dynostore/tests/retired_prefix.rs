// Copyright ⓒ 2026 Popsink SAS
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

//! A deleted topic's prefix must keep a retention threshold (#532).
//!
//! `delete_topic` cannot delete a coalesced segment — it multiplexes many
//! topics and is immutable — so it writes a truncation tombstone per partition
//! and leaves the bytes to retention (#61/#246). Retention never came: every
//! maintenance universe was derived from `topic-metadata/`, so deleting the
//! last topic of a prefix removed the only thing that gave the prefix a
//! threshold, `expire_prefix_segments` was never called for it again, and its
//! segments survived at any retention setting. Measured on a real ADLS Gen2
//! account: 27 899 of 27 899 `.seg` objects and 2.75 GiB survived the deletion
//! of all 1 000 topics, invisible to every retention metric because the prefix
//! was never processed.
//!
//! These tests pin the marker that closes it, and the things it must not do:
//! shorten a live sibling's retention, hand a threshold to a compact-only
//! prefix, or be dropped by a maintainer that has not looked at the prefix. The
//! last one covers what the reclaim then has to tidy after itself — the
//! truncation tombstone, which hides records that no longer exist and would hide
//! a successor's own if it outlived them.

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{ObjectStore as _, ObjectStoreExt as _, memory::InMemory, path::Path};
use tansu_sans_io::{
    IsolationLevel, ListOffset,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage as _, TopicId, Topition,
    dynostore::{DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// Milliseconds since the epoch, as the expiry threshold is expressed.
fn now_ms() -> i64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

/// A batch whose records carry `timestamp` — the source timestamp retention is
/// keyed on. `1_000` is 1970, i.e. past every finite retention.
fn batch_at(timestamp: i64) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .base_timestamp(timestamp)
        .max_timestamp(timestamp)
        .record(Record::builder().value(Some(Bytes::from_static(b"record"))))
        .last_offset_delta(0)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(store: &DynoStore, name: &str, configs: &[(&str, &str)]) -> Result<()> {
    _ = store
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some(
                    configs
                        .iter()
                        .map(|(name, value)| {
                            CreatableTopicConfig::default()
                                .name((*name).into())
                                .value(Some((*value).into()))
                        })
                        .collect(),
                )),
            false,
        )
        .await?;

    Ok(())
}

/// An ancient record in `topic`, flushed into a segment of its routed prefix.
async fn produce_ancient(store: &DynoStore, topic: &str) -> Result<()> {
    _ = store
        .produce(None, &Topition::new(topic, 0), batch_at(1_000)?)
        .await?;

    Ok(())
}

async fn segments(bucket: &InMemory, prefix: &str) -> Vec<Path> {
    let mut paths = bucket
        .list(Some(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{prefix}/segments/"
        ))))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .expect("list segments");
    paths.sort();
    paths
}

async fn marker(bucket: &InMemory, prefix: &str) -> Option<Bytes> {
    match bucket
        .get(&Path::from(format!(
            "clusters/{CLUSTER}/retired-prefixes/{prefix}.json"
        )))
        .await
    {
        Ok(result) => Some(result.bytes().await.expect("marker bytes")),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(otherwise) => panic!("{otherwise:?}"),
    }
}

async fn watermark(bucket: &InMemory, topic: &str) -> Option<Bytes> {
    match bucket
        .get(&Path::from(format!(
            "clusters/{CLUSTER}/topics/{topic}/partitions/{:0>10}/watermark.json",
            0
        )))
        .await
    {
        Ok(result) => Some(result.bytes().await.expect("watermark bytes")),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(otherwise) => panic!("{otherwise:?}"),
    }
}

/// The worst case the account measured: a topic name with fewer components than
/// the prefix depth, so the topic is the only occupant of its prefix and its
/// deletion takes the whole threshold with it.
///
/// The delete leaving the segment behind is by design; what has to hold is that
/// the next maintenance tick past the retention window reclaims it, and that the
/// reclaim is reported (the returned count is the value
/// `tansu_prefix_segments_expired` is incremented by, so a prefix that is never
/// processed cannot report it).
#[tokio::test]
async fn deleting_the_only_topic_leaves_its_segments_expirable() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let topic = "orders";

    create_topic(&store, topic, &[]).await?;
    produce_ancient(&store, topic).await?;
    assert_eq!(1, segments(&bucket, topic).await.len());

    assert_eq!(
        tansu_sans_io::ErrorCode::None,
        store.delete_topic(&TopicId::from(topic)).await?
    );

    // Deliberately unchanged by the delete itself: the segment is immutable and
    // shared by construction, so only retention can remove it.
    assert_eq!(1, segments(&bucket, topic).await.len());
    assert!(marker(&bucket, topic).await.is_some());

    let (deleted, _) = store.maintain_prefix_segments(now_ms(), None).await?;

    assert_eq!(
        1, deleted,
        "the retired prefix must be expired, not skipped"
    );
    assert!(segments(&bucket, topic).await.is_empty());

    // And the marker goes with the last segment: it has done its work, and
    // leaving it would keep the prefix in every maintainer's claim forever.
    assert!(marker(&bucket, topic).await.is_none());

    Ok(())
}

/// Without the marker, nothing ever revisits the prefix: this is the same
/// deletion driven by a maintainer that never held the marker set, pinning that
/// the topic-derived universes alone still reclaim nothing. It is the control
/// for the test above.
#[tokio::test]
async fn a_prefix_with_no_marker_and_no_topic_is_never_expired() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let topic = "orders";

    create_topic(&store, topic, &[]).await?;
    produce_ancient(&store, topic).await?;
    assert_eq!(
        tansu_sans_io::ErrorCode::None,
        store.delete_topic(&TopicId::from(topic)).await?
    );

    // Take the marker away, leaving exactly the layout #532 measured.
    bucket
        .delete(&Path::from(format!(
            "clusters/{CLUSTER}/retired-prefixes/{topic}.json"
        )))
        .await?;

    // A maintainer with no in-memory prefix index either — a dedicated
    // maintenance pod, or any replica after a restart, which is where the
    // stranding became permanent.
    let maintainer = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let (deleted, _) = maintainer.maintain_prefix_segments(now_ms(), None).await?;

    assert_eq!(0, deleted);
    assert_eq!(1, segments(&bucket, topic).await.len());

    Ok(())
}

/// A live topic on the prefix decides the threshold, and a retired sibling's
/// marker cannot shorten it — the segment they share holds the live topic's
/// records too.
#[tokio::test]
async fn a_retired_marker_cannot_shorten_a_live_siblings_retention() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let prefix = "org.env.conn";
    let live = "org.env.conn.kept";
    let retired = "org.env.conn.gone";

    create_topic(&store, live, &[("retention.ms", "-1")]).await?;
    create_topic(&store, retired, &[("retention.ms", "1")]).await?;

    produce_ancient(&store, live).await?;
    produce_ancient(&store, retired).await?;
    let before = segments(&bucket, prefix).await;
    assert!(!before.is_empty());

    assert_eq!(
        tansu_sans_io::ErrorCode::None,
        store.delete_topic(&TopicId::from(retired)).await?
    );

    // The marker is written whatever else is on the prefix — whether a sibling
    // survives is not knowable at delete time without a race — so what has to
    // hold is that it loses to the live topic.
    assert!(marker(&bucket, prefix).await.is_some());

    let (deleted, _) = store.maintain_prefix_segments(now_ms(), None).await?;

    assert_eq!(0, deleted, "retain-forever must still hold the prefix");
    assert_eq!(before, segments(&bucket, prefix).await);

    // Still holding segments, so the marker stays: the last sibling's deletion
    // is what it is being kept for.
    assert!(marker(&bucket, prefix).await.is_some());

    Ok(())
}

/// A compact-only topic's prefix is exempt from time-based expiry (#175): the
/// latest value of a key must survive indefinitely. A marker left on that name
/// by a deleted predecessor must not overrule it.
///
/// Reachable because a compacted topic's routed prefix is its own name, so any
/// deleted topic that retired onto that name leaves a marker sitting exactly
/// there.
#[tokio::test]
async fn a_compact_only_prefix_gains_no_threshold_from_a_marker() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let topic = "ledger";

    // A `delete` incarnation, deleted: a marker on prefix `ledger`.
    create_topic(&store, topic, &[("cleanup.policy", "delete")]).await?;
    produce_ancient(&store, topic).await?;
    assert_eq!(
        tansu_sans_io::ErrorCode::None,
        store.delete_topic(&TopicId::from(topic)).await?
    );
    assert!(marker(&bucket, topic).await.is_some());

    // Recreated compact-only, routed to that same prefix, and holding a record
    // any finite retention would delete.
    create_topic(&store, topic, &[("cleanup.policy", "compact")]).await?;
    produce_ancient(&store, topic).await?;
    let before = segments(&bucket, topic).await;
    assert!(!before.is_empty());

    let (deleted, _) = store.maintain_prefix_segments(now_ms(), None).await?;

    assert_eq!(0, deleted, "a compact-only prefix must never time-expire");
    assert_eq!(before, segments(&bucket, topic).await);

    Ok(())
}

/// Two topics retiring onto one prefix: the marker keeps the **longest** of
/// their retentions, for the reason the live fold does (#61) — the segment is
/// shared, so the shortest retention on it must never be the one that deletes
/// it. Whichever order they are deleted in.
#[tokio::test]
async fn a_markers_retention_is_the_longest_retired_onto_it() -> Result<(), Error> {
    let _guard = init_tracing()?;

    for order in [
        ["org.env.conn.a", "org.env.conn.b"],
        ["org.env.conn.b", "org.env.conn.a"],
    ] {
        let bucket = InMemory::new();
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
        let prefix = "org.env.conn";

        create_topic(&store, "org.env.conn.a", &[("retention.ms", "1")]).await?;
        create_topic(&store, "org.env.conn.b", &[("retention.ms", "-1")]).await?;

        produce_ancient(&store, "org.env.conn.a").await?;
        produce_ancient(&store, "org.env.conn.b").await?;
        let before = segments(&bucket, prefix).await;
        assert!(!before.is_empty());

        for topic in order {
            assert_eq!(
                tansu_sans_io::ErrorCode::None,
                store.delete_topic(&TopicId::from(topic)).await?
            );
        }

        let (deleted, _) = store.maintain_prefix_segments(now_ms(), None).await?;

        assert_eq!(
            0, deleted,
            "the retain-forever topic's obligation must survive the other's, deleted {order:?}"
        );
        assert_eq!(before, segments(&bucket, prefix).await);
    }

    Ok(())
}

/// The marker may only be dropped on the strength of an index entry that says
/// the prefix is empty — never on the absence of one.
///
/// A maintainer that has never indexed the prefix holds no entry at all, and
/// reading that as "empty" would drop the only threshold that can ever reclaim a
/// prefix still full of segments: #532's leak, re-created by its own cleanup,
/// and unrecoverable because the topic metadata is already gone.
#[tokio::test]
async fn a_cold_maintainer_does_not_drop_a_full_prefixs_marker() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let topic = "orders";

    {
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
        create_topic(&store, topic, &[]).await?;
        produce_ancient(&store, topic).await?;
        assert_eq!(
            tansu_sans_io::ErrorCode::None,
            store.delete_topic(&TopicId::from(topic)).await?
        );
    }

    let cold = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert!(!cold.drop_retired_prefix_if_drained(topic).await?);
    assert!(marker(&bucket, topic).await.is_some());

    // And the prefix is still reclaimable through it.
    let (deleted, _) = cold.maintain_prefix_segments(now_ms(), None).await?;
    assert_eq!(1, deleted);
    assert!(marker(&bucket, topic).await.is_none());

    Ok(())
}

/// Reclaiming a retired prefix must leave the topic **name** clean (#246/#532).
///
/// `delete_topic` rewrites each partition's `watermark.json` as a truncation
/// floor at the deleted log end rather than removing it, because the records it
/// hides survive inside shared segments and a same-named successor would
/// otherwise find them by name. This reclaim is the first thing that ever ran
/// retention on a last-occupant prefix, so it is the first thing that can take
/// those records away — and a floor left behind then hides the *successor's*
/// own records instead: its `create_topic` clears `high` and folds its base from
/// the segments, of which there are none, so it starts at 0 underneath a floor
/// of 1 and its first record is unreadable.
#[tokio::test]
async fn a_reclaimed_prefix_leaves_the_topic_name_clean() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let topic = "orders";
    let tp = Topition::new(topic, 0);

    create_topic(&store, topic, &[]).await?;
    assert_eq!(0, store.produce(None, &tp, batch_at(1_000)?).await?);
    assert_eq!(
        tansu_sans_io::ErrorCode::None,
        store.delete_topic(&TopicId::from(topic)).await?
    );

    // The tombstone the delete left, hiding the slice it could not remove.
    assert!(watermark(&bucket, topic).await.is_some());

    let (deleted, _) = store.maintain_prefix_segments(now_ms(), None).await?;
    assert_eq!(1, deleted);
    assert!(segments(&bucket, topic).await.is_empty());

    // Gone with the records it was hiding — the other half of the cost
    // `delete_topic` accepted "indefinitely".
    assert!(watermark(&bucket, topic).await.is_none());

    // A fresh process, so nothing below is answered from a warm hint.
    let restarted = DynoStore::new(CLUSTER, NODE, bucket.clone());
    create_topic(&restarted, topic, &[]).await?;

    assert_eq!(0, restarted.produce(None, &tp, batch_at(now_ms())?).await?);

    // And the record is readable, which is what a surviving floor would have
    // taken away: the log starts at 0 and ends at 1.
    assert_eq!(
        vec![Some(0), Some(1)],
        restarted
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[
                    (tp.clone(), ListOffset::Earliest),
                    (tp.clone(), ListOffset::Latest),
                ],
            )
            .await?
            .into_iter()
            .map(|(_, response)| response.offset)
            .collect::<Vec<_>>()
    );

    let fetched = restarted
        .fetch(
            &tp,
            0,
            0,
            1_000_000,
            IsolationLevel::ReadUncommitted,
            std::time::Duration::from_millis(200),
        )
        .await?;

    assert_eq!(
        1,
        fetched.iter().map(|batch| batch.record_count).sum::<u32>()
    );

    Ok(())
}
