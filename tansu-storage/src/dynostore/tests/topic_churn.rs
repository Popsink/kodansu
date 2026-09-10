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

//! `delete_topic` must leave nothing behind in this process's per-topic maps
//! (#283), on the replica that served it **and** on every replica that did not
//! (#554).
//!
//! It used to invalidate three of them — the topic-id pointer, the routing pin
//! and the topic index — and leave six. Those six were cleared only by a
//! same-named `create_topic` or by a process restart, so under create/delete
//! churn with **fresh** names nothing cleared them at all: `topic_metas` and
//! `watermarks` each hold a cached JSON value per entry, so the growth was real
//! memory, monotonic for the life of the pod.
//!
//! #283 then fixed the six and swept only those six on peers, so the id pointer
//! and the routing pin stayed on every replica that had not served the delete —
//! for the life of the pod, since both are cached without a TTL on the grounds
//! that they are immutable for a topic's lifetime. The routing pin is the
//! authority for which prefix and which sub-stream identity (#442) a topic's
//! records are written under, so a peer holding a dead incarnation's pin wrote a
//! same-named successor's records under the dead identity. Which is why the
//! assertions below are over *every* map the group holds, named by the group
//! rather than by this file.
//!
//! All three tests are written against the *level*, not against a delta: the
//! failure being guarded is monotonic growth, so what has to hold is that a
//! churn loop returns to where it started however many times it runs.

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_sans_io::{
    ConfigResource, ListOffset,
    create_topics_request::CreatableTopic,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    record::Record,
    record::deflated,
    record::inflated,
};

use uuid::Uuid;

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

async fn create(storage: &DynoStore, name: &str, compacted: bool) -> Result<Uuid> {
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

    storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(PARTITIONS)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(configs),
            false,
        )
        .await
}

/// Drive a topic through the paths that populate the per-topic maps, as a client
/// would: a by-name Metadata lookup, a `DescribeConfigs` (`topic_metas`), a
/// produce (`next_offsets`, `watermarks`) and a LATEST/EARLIEST offset read
/// (`coalesced_watermark_floors`, `truncate_floors`).
///
/// `DescribeConfigs` is what reaches `topic_metas` on a replica that never created
/// the topic. The by-name Metadata lookup used to, and since #387 does not: it is
/// answered from the topic index, so it allocates no per-topic `OptiCon` handle at
/// all. That is a reduction in exactly the growth this module guards, but it would
/// make the assertions below pass vacuously — hence a path that still reads the
/// topic's own object, which `describe_config` deliberately remains (a stale
/// `cleanup.policy` there is a permanently mis-pinned routing prefix).
///
/// The by-**id** Metadata lookup is what reaches `topic_ids`: a client that knows
/// a topic's id (every modern one, past `Metadata` v10) asks by id, and the
/// answer is memoized permanently because the mapping is immutable for the
/// topic's lifetime. `routing_prefixes` is reached by the produce below, and
/// memoized permanently for the same reason. Both are therefore only ever
/// dropped by a delete, which is what makes a delete that misses them permanent
/// (#554).
///
/// `compacted_topics` is not reachable from here and is asserted empty by the
/// callers. Since the routing pin (#236) it is only consulted for a topic created
/// before pinning existed, so on a store that created its own topics the memo
/// stays empty — it is swept anyway, because a fleet upgraded into #236 still has
/// pre-pin topics.
async fn exercise(storage: &DynoStore, name: &str, id: Uuid) -> Result<()> {
    _ = storage.metadata(Some(&[TopicId::from(name)])).await?;
    _ = storage.metadata(Some(&[TopicId::Id(id)])).await?;

    _ = storage
        .describe_config(name, ConfigResource::Topic, None)
        .await?;

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

/// Every map in the topic-scoped cache group, as `(name, entries)`.
///
/// Read as the group's own inventory rather than as a list of fields this file
/// names: the acceptance criterion is that *no* map keeps an entry, so a test
/// that enumerates the maps it checks stops covering the next map added — which
/// is exactly how `topic_ids` and `routing_prefixes` went unswept for a release
/// (#554). Adding a map to `TopicCaches` now widens these assertions by
/// construction, and a map the eviction does not reach fails them.
fn cached(storage: &DynoStore) -> [(&'static str, usize); 8] {
    storage.topics.occupancy()
}

/// The maps holding anything at all, so a failure names them rather than
/// reporting a count.
fn held(storage: &DynoStore) -> Vec<(&'static str, usize)> {
    cached(storage)
        .into_iter()
        .filter(|(_, entries)| *entries > 0)
        .collect()
}

/// Deleting a topic leaves no entry behind in any per-topic map — the acceptance
/// criterion, on one topic, so a failure names the map rather than a count.
#[tokio::test]
async fn delete_topic_leaves_no_per_topic_cache_entry() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(CLUSTER, NODE, InMemory::new());

    assert_eq!(Vec::<(&str, usize)>::new(), held(&storage));

    let id = create(&storage, "leaves-nothing", false).await?;
    exercise(&storage, "leaves-nothing", id).await?;

    // The test is only meaningful if the maps were actually populated: assert
    // every one of them holds something before the delete, so an `exercise` that
    // stops reaching a path fails here rather than passing vacuously below.
    //
    // `truncate_floors` is asserted empty on purpose: the creating replica's
    // watermark `OptiCon` is warm, and `truncate_floor` serves from that in
    // preference to the memo, so on this store the memo is never reached. The
    // peer-replica test below is where it gets populated.
    assert_eq!(
        [
            ("topic_metas", 1),
            ("topic_ids", 1),
            ("routing_prefixes", 1),
            ("compacted_topics", 0),
            ("watermarks", PARTITIONS as usize),
            ("next_offsets", PARTITIONS as usize),
            ("coalesced_watermark_floors", PARTITIONS as usize),
            ("truncate_floors", 0),
        ],
        cached(&storage)
    );

    _ = storage
        .delete_topic(&TopicId::from("leaves-nothing"))
        .await?;

    assert_eq!(Vec::<(&str, usize)>::new(), held(&storage));

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

    let id = create(&owner, "peer-converges", false).await?;
    exercise(&owner, "peer-converges", id).await?;

    // The peer reads the topic without ever having created it.
    exercise(&peer, "peer-converges", id).await?;

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

    assert_eq!(
        [
            ("topic_metas", 1),
            ("topic_ids", 1),
            ("routing_prefixes", 1),
            ("compacted_topics", 0),
            ("watermarks", PARTITIONS as usize),
            ("next_offsets", PARTITIONS as usize),
            ("coalesced_watermark_floors", PARTITIONS as usize),
            ("truncate_floors", PARTITIONS as usize),
        ],
        cached(&peer)
    );

    _ = owner.delete_topic(&TopicId::from("peer-converges")).await?;

    // Deleted on the owner, and the peer still holds every entry: this is the
    // gap the sweep closes, so assert it is there before asserting it closes.
    assert_ne!(Vec::<(&str, usize)>::new(), held(&peer));

    assert_eq!(1, peer.evict_deleted_topic_caches().await?);
    assert_eq!(Vec::<(&str, usize)>::new(), held(&peer));

    // Idempotent, and it does not evict a live topic: re-create, read it on the
    // peer, sweep again, and the entries must survive.
    let id = create(&owner, "peer-converges", false).await?;
    exercise(&peer, "peer-converges", id).await?;

    assert_eq!(0, peer.evict_deleted_topic_caches().await?);
    assert_ne!(Vec::<(&str, usize)>::new(), held(&peer));

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

    let id = create(&owner, "only-topic", false).await?;
    exercise(&peer, "only-topic", id).await?;

    _ = owner.delete_topic(&TopicId::from("only-topic")).await?;

    assert_eq!(1, peer.evict_deleted_topic_caches().await?);
    assert_eq!(Vec::<(&str, usize)>::new(), held(&peer));

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

        let id = create(&storage, &name, round % 2 == 0).await?;
        exercise(&storage, &name, id).await?;

        _ = storage.delete_topic(&TopicId::from(name.as_str())).await?;

        assert_eq!(
            Vec::<(&str, usize)>::new(),
            held(&storage),
            "per-topic maps not flat after round {round}"
        );
    }

    Ok(())
}
