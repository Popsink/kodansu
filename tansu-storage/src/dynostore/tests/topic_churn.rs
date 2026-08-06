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

//! `delete_topic` must leave nothing behind in this process's per-topic maps
//! (#283).
//!
//! It used to invalidate three of them — the topic-id pointer, the routing pin
//! and the topic index — and leave six. Those six were cleared only by a
//! same-named `create_topic` or by a process restart, so under create/delete
//! churn with **fresh** names nothing cleared them at all: `topic_metas` and
//! `watermarks` each hold a cached JSON value per entry, so the growth was real
//! memory, monotonic for the life of the pod.
//!
//! Both tests below are written against the *level*, not against a delta: the
//! failure being guarded is monotonic growth, so what has to hold is that a
//! churn loop returns to where it started however many times it runs.

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_sans_io::{
    ListOffset,
    create_topics_request::CreatableTopic,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    record::Record,
    record::deflated,
    record::inflated,
};

use crate::{
    Error, Result, Storage, TopicId, Topition,
    dynostore::{DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PARTITIONS: i32 = 3;

fn batch() -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"record"))))
        .last_offset_delta(0)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create(storage: &DynoStore, name: &str, compacted: bool) -> Result<()> {
    let configs = if compacted {
        Some(
            [
                tansu_sans_io::create_topics_request::CreatableTopicConfig::default()
                    .name("cleanup.policy".into())
                    .value(Some("compact".into())),
            ]
            .into(),
        )
    } else {
        Some([].into())
    };

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(PARTITIONS)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(configs),
            false,
        )
        .await?;

    Ok(())
}

/// Drive a topic through the paths that populate the per-topic maps, as a client
/// would: a by-name Metadata lookup (`topic_metas`), a produce (`next_offsets`,
/// `watermarks`) and a LATEST/EARLIEST offset read
/// (`coalesced_watermark_floors`, `truncate_floors`).
///
/// `compacted_topics` is not reachable from here and is asserted empty by the
/// callers. Since the routing pin (#236) it is only consulted for a topic created
/// before pinning existed, so on a store that created its own topics the memo
/// stays empty — it is swept anyway, because a fleet upgraded into #236 still has
/// pre-pin topics.
async fn exercise(storage: &DynoStore, name: &str) -> Result<()> {
    _ = storage.metadata(Some(&[TopicId::from(name)])).await?;

    for partition in 0..PARTITIONS {
        let topition = Topition::new(name, partition);

        _ = storage.produce(None, &topition, batch()?).await?;

        _ = storage
            .list_offsets(
                tansu_sans_io::IsolationLevel::ReadUncommitted,
                &[
                    (topition.clone(), ListOffset::Latest),
                    (topition.clone(), ListOffset::Earliest),
                ],
            )
            .await?;

        _ = storage.offset_stage(&topition).await?;
    }

    Ok(())
}

/// Every per-topic map entry this store holds, as
/// `(topic_metas, watermarks, next_offsets, coalesced_watermark_floors,
/// truncate_floors, compacted_topics)`.
///
/// Reads the fields directly rather than through a helper on `DynoStore`: the
/// point of the test is that *no* entry survives, and a helper that reported the
/// counts could be written to agree with the eviction it is meant to check.
fn cached(storage: &DynoStore) -> (usize, usize, usize, usize, usize, usize) {
    (
        storage.topic_metas.lock().unwrap().len(),
        storage.watermarks.lock().unwrap().len(),
        storage.next_offsets.lock().unwrap().len(),
        storage.coalesced_watermark_floors.lock().unwrap().len(),
        storage.truncate_floors.lock().unwrap().len(),
        storage.compacted_topics.lock().unwrap().len(),
    )
}

/// Deleting a topic leaves no entry behind in any per-topic map — the acceptance
/// criterion, on one topic, so a failure names the map rather than a count.
#[tokio::test]
async fn delete_topic_leaves_no_per_topic_cache_entry() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    assert_eq!((0, 0, 0, 0, 0, 0), cached(&storage));

    create(&storage, "leaves-nothing", false).await?;
    exercise(&storage, "leaves-nothing").await?;

    // The test is only meaningful if the maps were actually populated: assert
    // every one of them holds something before the delete, so an `exercise` that
    // stops reaching a path fails here rather than passing vacuously below.
    //
    // `truncate_floors` is asserted empty on purpose: the creating replica's
    // watermark `OptiCon` is warm, and `truncate_floor` serves from that in
    // preference to the memo, so on this store the memo is never reached. The
    // peer-replica test below is where it gets populated.
    let (metas, watermarks, next, floors, truncates, compacted) = cached(&storage);
    assert_eq!(1, metas, "topic_metas");
    assert_eq!(PARTITIONS as usize, watermarks, "watermarks");
    assert_eq!(PARTITIONS as usize, next, "next_offsets");
    assert_eq!(PARTITIONS as usize, floors, "coalesced_watermark_floors");
    assert_eq!(0, truncates, "truncate_floors");
    assert_eq!(0, compacted, "compacted_topics");

    _ = storage
        .delete_topic(&TopicId::from("leaves-nothing"))
        .await?;

    assert_eq!((0, 0, 0, 0, 0, 0), cached(&storage));

    Ok(())
}

/// A replica that only ever *read* the topic must converge too (#283).
///
/// `delete_topic` evicts the caches of the replica that served it, and nothing
/// else — eviction is process-local. A stateless fleet puts every topic through
/// every replica, so without the maintenance sweep nine pods in ten keep their
/// entries for a deleted topic and the growth stays monotonic, just slower. This
/// pins the sweep: the peer holds entries, the topic is deleted elsewhere, the
/// peer's next maintenance tick clears them.
#[tokio::test]
async fn a_peer_replica_converges_on_the_maintenance_tick() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // Two stores over one bucket == two stateless replicas.
    let bucket = InMemory::new();
    let owner = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let peer = DynoStore::new(CLUSTER, NODE, bucket.clone());

    create(&owner, "peer-converges", false).await?;
    exercise(&owner, "peer-converges").await?;

    // The peer reads the topic without ever having created it.
    exercise(&peer, "peer-converges").await?;

    // `truncate_floors` is memoized by whoever serves a `DeleteRecords`, so have
    // the peer serve one — that is the state in which a replica that never owned
    // the topic ends up holding a floor for it.
    _ = peer
        .delete_records(&[DeleteRecordsTopic::default()
            .name("peer-converges".into())
            .partitions(Some(
                (0..PARTITIONS)
                    .map(|partition| {
                        DeleteRecordsPartition::default()
                            .partition_index(partition)
                            .offset(0)
                    })
                    .collect(),
            ))])
        .await?;

    let (metas, watermarks, next, floors, truncates, compacted) = cached(&peer);
    assert_eq!(1, metas, "topic_metas");
    assert_eq!(PARTITIONS as usize, watermarks, "watermarks");
    assert_eq!(PARTITIONS as usize, next, "next_offsets");
    assert_eq!(PARTITIONS as usize, floors, "coalesced_watermark_floors");
    assert_eq!(PARTITIONS as usize, truncates, "truncate_floors");
    assert_eq!(0, compacted, "compacted_topics");

    _ = owner.delete_topic(&TopicId::from("peer-converges")).await?;

    // Deleted on the owner, and the peer still holds every entry: this is the
    // gap the sweep closes, so assert it is there before asserting it closes.
    assert_ne!((0, 0, 0, 0, 0, 0), cached(&peer));

    assert_eq!(1, peer.evict_deleted_topic_caches().await?);
    assert_eq!((0, 0, 0, 0, 0, 0), cached(&peer));

    // Idempotent, and it does not evict a live topic: re-create, read it on the
    // peer, sweep again, and the entries must survive.
    create(&owner, "peer-converges", false).await?;
    exercise(&peer, "peer-converges").await?;

    assert_eq!(0, peer.evict_deleted_topic_caches().await?);
    assert_ne!((0, 0, 0, 0, 0, 0), cached(&peer));

    Ok(())
}

/// A successful listing that finds no topics *is* an empty cluster, and the
/// sweep evicts on it — but a listing that **failed** must not read as one, or a
/// transient store error would drop the whole fleet's caches at once. The
/// distinction is carried by `?` on the refresh; this pins the empty-cluster half
/// of it, which is the half that could be mistaken for a bug.
#[tokio::test]
async fn an_empty_cluster_evicts_everything() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let owner = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let peer = DynoStore::new(CLUSTER, NODE, bucket.clone());

    create(&owner, "only-topic", false).await?;
    exercise(&peer, "only-topic").await?;

    _ = owner.delete_topic(&TopicId::from("only-topic")).await?;

    assert_eq!(1, peer.evict_deleted_topic_caches().await?);
    assert_eq!((0, 0, 0, 0, 0, 0), cached(&peer));

    Ok(())
}

/// Create/delete many **uniquely named** topics — the shape the growth was
/// observed under, since a same-named re-create was the only thing that used to
/// clear these maps — and the steady state must be flat, not merely
/// sub-linear.
///
/// Compacted topics are mixed in because they route to their own dedicated
/// prefix (#175), which is per-topic where every other prefix is shared; the
/// prefix-keyed maps are deliberately *not* evicted here (a prefix can outlive
/// any one of its topics), so this pins that the per-topic maps are flat
/// regardless.
#[tokio::test]
async fn topic_churn_with_fresh_names_reaches_a_flat_steady_state() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    const ROUNDS: usize = 16;

    for round in 0..ROUNDS {
        let name = format!("churn-{round}");

        create(&storage, &name, round % 2 == 0).await?;
        exercise(&storage, &name).await?;

        _ = storage.delete_topic(&TopicId::from(name.as_str())).await?;

        assert_eq!(
            (0, 0, 0, 0, 0, 0),
            cached(&storage),
            "per-topic maps not flat after round {round}"
        );
    }

    Ok(())
}
