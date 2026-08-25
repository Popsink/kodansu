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

//! One object per group for committed offsets (#406, #111).
//!
//! The layout this replaces is one object per `(group, topic, partition)`,
//! written with an unconditional overwrite: a commit over `t` partitions cost `t`
//! billed PUTs, and an `OffsetFetch(all)` cost a LIST plus a GET each. On the
//! production fleet consumer-group writes are **67 % of the whole PUT plane** —
//! $10.59/day, the largest single line item on the request bill — and #111 asked
//! for exactly this, with the acceptance *"a commit over `t` topitions issues
//! O(1) PUTs, not `1 + t`"*. That half was never landed and the issue was closed.
//!
//! The migration is lazy on purpose. The per-partition objects are neither folded
//! in bulk nor deleted:
//!
//! - a **read** prefers the one object and falls back per key, so a group that
//!   has not committed since the upgrade reads exactly what it did before;
//! - the **topition-set discovery** unions the two, because a group whose commits
//!   only ever landed in the new object has nothing under `offsets/` to list, and
//!   an `OffsetFetch(all)` that took either source alone would answer empty for
//!   one of them — a consumer resuming from nothing;
//! - nothing reads O(partitions) objects on the commit path, which is the path
//!   the change exists to make cheaper.
//!
//! It also bounds what a rollback costs: an older binary reads only the
//! per-partition objects, so it resumes from the last offset committed before the
//! upgrade rather than from nothing.

use bytes::Bytes;
use object_store::{ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path};
use tansu_sans_io::{ErrorCode, create_topics_request::CreatableTopic};

use crate::{
    Error, OffsetCommitRequest, Result, Storage as _, TopicId, Topition,
    dynostore::{DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const GROUP: &str = "group-a";
const TOPIC: &str = "committed";

async fn create_topic(store: &DynoStore, name: &str, partitions: i32) -> Result<()> {
    _ = store
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

fn offsets_json(group: &str) -> Path {
    Path::from(format!(
        "clusters/{CLUSTER}/groups/consumers/{group}/offsets.json"
    ))
}

/// Write a committed offset the way the pre-#406 layout did: one object per
/// `(group, topic, partition)`, through the bucket so no in-memory handle knows
/// about it. This is the state an upgrade inherits, not a route to it.
async fn commit_the_old_way(
    bucket: &InMemory,
    group: &str,
    topition: &Topition,
    offset: i64,
) -> Result<()> {
    let payload = serde_json::to_vec(&OffsetCommitRequest::default().offset(offset))?;

    _ = bucket
        .put(
            &Path::from(format!(
                "clusters/{CLUSTER}/groups/consumers/{group}/offsets/{}/partitions/{:0>10}.json",
                topition.topic(),
                topition.partition(),
            )),
            PutPayload::from(Bytes::from(payload)),
        )
        .await?;

    Ok(())
}

/// A commit lands in one object, and reads back through it.
#[tokio::test]
async fn a_commit_lands_in_one_object() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    create_topic(&store, TOPIC, 4).await?;

    let offsets: Vec<(Topition, OffsetCommitRequest)> = (0..4)
        .map(|partition| {
            (
                Topition::new(TOPIC, partition),
                OffsetCommitRequest::default().offset(i64::from(partition) + 10),
            )
        })
        .collect();

    for (_, error_code) in store.offset_commit(GROUP, None, &offsets).await? {
        assert_eq!(ErrorCode::None, error_code);
    }

    assert!(bucket.get(&offsets_json(GROUP)).await.is_ok());

    let fetched = store
        .offset_fetch(
            Some(GROUP),
            &(0..4).map(|p| Topition::new(TOPIC, p)).collect::<Vec<_>>(),
            Some(false),
        )
        .await?;

    for partition in 0..4 {
        assert_eq!(
            Some(&(i64::from(partition) + 10)),
            fetched.get(&Topition::new(TOPIC, partition))
        );
    }

    Ok(())
}

/// A group that has not committed since the upgrade reads exactly what it did
/// before: the one object does not hold its offsets, so each falls back to its
/// own.
#[tokio::test]
async fn the_old_layout_still_reads_when_the_new_object_does_not_hold_it() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    create_topic(&store, TOPIC, 2).await?;

    commit_the_old_way(&bucket, GROUP, &Topition::new(TOPIC, 0), 41).await?;
    commit_the_old_way(&bucket, GROUP, &Topition::new(TOPIC, 1), 42).await?;

    let fetched = store
        .offset_fetch(
            Some(GROUP),
            &[Topition::new(TOPIC, 0), Topition::new(TOPIC, 1)],
            Some(false),
        )
        .await?;

    assert_eq!(Some(&41), fetched.get(&Topition::new(TOPIC, 0)));
    assert_eq!(Some(&42), fetched.get(&Topition::new(TOPIC, 1)));

    // And a commit for one of them moves that key into the new object without
    // disturbing the other, which is the whole of the migration.
    _ = store
        .offset_commit(
            GROUP,
            None,
            &[(
                Topition::new(TOPIC, 0),
                OffsetCommitRequest::default().offset(99),
            )],
        )
        .await?;

    let fetched = store
        .offset_fetch(
            Some(GROUP),
            &[Topition::new(TOPIC, 0), Topition::new(TOPIC, 1)],
            Some(false),
        )
        .await?;

    assert_eq!(Some(&99), fetched.get(&Topition::new(TOPIC, 0)));
    assert_eq!(Some(&42), fetched.get(&Topition::new(TOPIC, 1)));

    Ok(())
}

/// `OffsetFetch(all)` unions the two layouts. Taking either alone answers empty
/// for a group that lives in the other, which is a consumer resuming from
/// nothing.
#[tokio::test]
async fn the_all_topics_form_unions_both_layouts() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    create_topic(&store, TOPIC, 1).await?;
    create_topic(&store, "legacy", 1).await?;

    // One key each side of the upgrade.
    commit_the_old_way(&bucket, GROUP, &Topition::new("legacy", 0), 7).await?;
    _ = store
        .offset_commit(
            GROUP,
            None,
            &[(
                Topition::new(TOPIC, 0),
                OffsetCommitRequest::default().offset(11),
            )],
        )
        .await?;

    let committed = store.committed_offset_topitions(GROUP).await?;

    assert_eq!(2, committed.len(), "{committed:?}");
    assert_eq!(Some(&7), committed.get(&Topition::new("legacy", 0)));
    assert_eq!(Some(&11), committed.get(&Topition::new(TOPIC, 0)));

    Ok(())
}

/// Deleting a topic takes its committed offsets with it, in **both** layouts.
///
/// A committed offset that outlives its topic is served against the recreated
/// one, which is #241's shape: 70 topics reporting a committed offset above a
/// high watermark of 0. Sweeping only the per-partition objects would leave the
/// new layout holding exactly that.
#[tokio::test]
async fn deleting_a_topic_drops_its_offsets_from_the_group_object() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    create_topic(&store, TOPIC, 1).await?;
    create_topic(&store, "survivor", 1).await?;

    _ = store
        .offset_commit(
            GROUP,
            None,
            &[
                (
                    Topition::new(TOPIC, 0),
                    OffsetCommitRequest::default().offset(11),
                ),
                (
                    Topition::new("survivor", 0),
                    OffsetCommitRequest::default().offset(22),
                ),
            ],
        )
        .await?;

    _ = store.delete_topic(&TopicId::Name(TOPIC.into())).await?;

    let committed = store.committed_offset_topitions(GROUP).await?;

    assert_eq!(
        None,
        committed.get(&Topition::new(TOPIC, 0)),
        "a deleted topic's committed offset survived: {committed:?}"
    );
    assert_eq!(Some(&22), committed.get(&Topition::new("survivor", 0)));

    Ok(())
}

/// Two replicas committing for the same group fold onto each other rather than
/// over each other.
///
/// The old layout raced per object with last-write-wins per partition, so a
/// commit could interleave with another and leave a group's offsets taken from
/// two different requests. One object under a CAS makes the loser re-apply its
/// fold onto the winner's value, so both commits survive.
#[tokio::test]
async fn two_replicas_committing_the_same_group_do_not_lose_each_other() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let a = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let b = DynoStore::new(CLUSTER, NODE, bucket.clone());
    create_topic(&a, TOPIC, 2).await?;

    // Both replicas read the same (absent) state before either writes: the
    // interleaving the CAS has to survive.
    _ = a
        .offset_commit(
            GROUP,
            None,
            &[(
                Topition::new(TOPIC, 0),
                OffsetCommitRequest::default().offset(5),
            )],
        )
        .await?;

    for (_, error_code) in b
        .offset_commit(
            GROUP,
            None,
            &[(
                Topition::new(TOPIC, 1),
                OffsetCommitRequest::default().offset(6),
            )],
        )
        .await?
    {
        assert_eq!(ErrorCode::None, error_code, "the losing writer's commit");
    }

    let committed = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .committed_offset_topitions(GROUP)
        .await?;

    assert_eq!(Some(&5), committed.get(&Topition::new(TOPIC, 0)));
    assert_eq!(Some(&6), committed.get(&Topition::new(TOPIC, 1)));

    Ok(())
}
