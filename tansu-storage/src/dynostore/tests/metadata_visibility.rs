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

use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use std::{
    collections::BTreeMap,
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
};
use tansu_sans_io::{ErrorCode, create_topics_request::CreatableTopic};
use uuid::Uuid;

use crate::{
    Error, Result, Storage, TopicId,
    dynostore::{DynoStore, TopicMetadata, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

async fn create_topic(storage: &DynoStore, name: &str, partitions: i32) -> Result<Uuid> {
    storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await
}

/// Regression for #28: a topic created on one replica must be visible on
/// another sharing the same bucket, with no stale-read window.
///
/// On the old monolithic `meta.json`, replica B's read of the (still absent)
/// topic primed its 5s metadata cache; a create on A then stayed invisible on B
/// until that cache expired, so `create-then-produce` against a fresh topic
/// failed with `UnknownTopicOrPartition`. Per-topic objects fix this: B holds
/// no cached etag for a topic it only saw as absent, so the conditional GET
/// cannot be short-circuited to a stale `NotModified`.
/// An object store that answers `NotFound` for one key, once, then behaves.
///
/// Models the shape behind #214: a single spurious 404 against a live topic's
/// metadata object. `OptiCon::refresh` clears its cached value on any
/// `NotFound`, so one such answer is enough to make `topic_metadata` return
/// `Ok(None)` for a topic that exists.
///
/// `armed` gates the injection so a test can warm the topic index *through* this
/// store before the fault is live (#387): the point of that fix is that a topic
/// the index holds is never read from its own object, and proving it means
/// asserting the fault is never reached.
///
/// `list_fails` injects one failing `list_with_delimiter` on top, which is what
/// makes the #214 witness reachable deterministically: the index refresh fails,
/// the topic falls back to its own (404-ing) object, and the witness — a second
/// refresh, which succeeds — is what recognises the topic as live.
#[derive(Clone)]
struct NotFoundOnce<O> {
    inner: O,
    key: Path,
    armed: Arc<AtomicBool>,
    fired: Arc<AtomicBool>,
    list_fails: Arc<AtomicBool>,
}

impl<O> NotFoundOnce<O> {
    /// Injecting nothing yet: reads go straight through.
    fn disarmed(inner: O, key: Path) -> Self {
        Self {
            inner,
            key,
            armed: Arc::new(AtomicBool::new(false)),
            fired: Arc::new(AtomicBool::new(false)),
            list_fails: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Injecting from now on: the next read of `key` answers `NotFound`.
    fn armed(inner: O, key: Path) -> Self {
        let injecting = Self::disarmed(inner, key);
        injecting.arm();
        injecting
    }

    fn arm(&self) {
        self.armed.store(true, Relaxed);
    }

    /// Also fail the next `list_with_delimiter`, so the index cannot answer.
    fn failing_one_listing(self) -> Self {
        self.list_fails.store(true, Relaxed);
        self
    }

    /// Whether the injected 404 was ever reached.
    fn fired(&self) -> bool {
        self.fired.load(Relaxed)
    }
}

impl<O> Debug for NotFoundOnce<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotFoundOnce").finish()
    }
}

impl<O> Display for NotFoundOnce<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotFoundOnce").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for NotFoundOnce<O>
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
        if *location == self.key && self.armed.load(Relaxed) && !self.fired.swap(true, Relaxed) {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "injected spurious 404".into(),
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
        if self.list_fails.swap(false, Relaxed) {
            return Err(object_store::Error::Generic {
                store: "NotFoundOnce",
                source: "injected listing failure".into(),
            });
        }
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

/// #214, as #387 leaves it: a live topic is answered **from the topic index**, so
/// the per-object read that used to report it absent is never made.
///
/// One spurious 404 is enough to empty `OptiCon`'s cached value, so
/// `topic_metadata` returns `Ok(None)` for a topic that plainly exists. Reporting
/// that as `UNKNOWN_TOPIC_OR_PARTITION` is the worst available answer: a client
/// cannot tell it from a deleted topic, so it refreshes metadata until
/// `max.block.ms` expires and then fails the batch. Production saw eight such
/// topics and six of twenty-four source connectors in restart loops.
///
/// #214 answered that with a retriable error code; #387 removes the read that
/// produces it. The index is built from a LIST of `topic-metadata/`, and once it
/// holds a topic, `Metadata` answers from it — so the fault below is not survived,
/// it is not reached, which is what `fired()` asserts. The retriable answer is
/// still there for the fallback population, pinned by
/// `an_unresolvable_existing_topic_is_retriable_not_absent`.
#[tokio::test]
async fn a_topic_the_index_holds_is_never_read_from_its_own_object() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let topic = "actively-produced";

    let seeder = DynoStore::new(CLUSTER, NODE, bucket.clone());
    _ = create_topic(&seeder, topic, 1).await?;

    // A replica reading through an injectable fault, not yet armed: warming its
    // index has to be allowed to read the object once.
    let faulty = NotFoundOnce::disarmed(
        bucket.clone(),
        Path::from(format!("clusters/{CLUSTER}/topic-metadata/{topic}.json")),
    );
    let store = DynoStore::new(CLUSTER, NODE, faulty.clone());

    _ = store.metadata(None).await?;

    // From here, any read of that object answers 404 — as production's did.
    faulty.arm();

    let response = store.metadata(Some(&[TopicId::Name(topic.into())])).await?;

    assert_eq!(1, response.topics.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(response.topics[0].error_code)?,
        "a topic the index holds must resolve",
    );
    assert_eq!(
        Some(1),
        response.topics[0]
            .partitions
            .as_ref()
            .map(|partitions| partitions.len()),
        "and it must be described, not answered empty",
    );
    assert!(
        !faulty.fired(),
        "the topic's own object must not be read at all (#387)",
    );

    Ok(())
}

/// #214: a topic the index could not answer for, whose own object then cannot be
/// resolved either, is answered **retriably** rather than reported absent.
///
/// This is the fallback population #387 leaves behind — a name the index does not
/// hold, here because its refresh failed. The read of the topic's own object then
/// takes the spurious 404, and the witness is a *second* index refresh, which
/// succeeds: it comes from a LIST of `topic-metadata/`, not from the per-object
/// read that just came back empty, so `LeaderNotAvailable` is both true and
/// retriable.
///
/// The interleaving is injected here and racy in production — a peer's create, a
/// maintenance sweep, or another request refreshing after an invalidation between
/// the lookup that missed and the read that came back empty — which is why the
/// witness stays.
#[tokio::test]
async fn an_unresolvable_existing_topic_is_retriable_not_absent() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let topic = "actively-produced";

    let seeder = DynoStore::new(CLUSTER, NODE, bucket.clone());
    _ = create_topic(&seeder, topic, 1).await?;

    // A replica whose index refresh fails once, and whose very first read of that
    // object then gets a spurious 404.
    let store = DynoStore::new(
        CLUSTER,
        NODE,
        NotFoundOnce::armed(
            bucket.clone(),
            Path::from(format!("clusters/{CLUSTER}/topic-metadata/{topic}.json")),
        )
        .failing_one_listing(),
    );

    let response = store.metadata(Some(&[TopicId::Name(topic.into())])).await?;

    assert_eq!(1, response.topics.len());

    let code = ErrorCode::try_from(response.topics[0].error_code)?;
    assert_ne!(
        ErrorCode::UnknownTopicOrPartition,
        code,
        "an existing topic must never be reported absent",
    );
    assert_eq!(
        ErrorCode::LeaderNotAvailable,
        code,
        "and the answer must be one the client retries",
    );

    Ok(())
}

#[tokio::test]
async fn topic_created_on_one_replica_is_visible_on_another() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // Two stores over ONE shared bucket == two stateless replicas (node 111).
    let bucket = InMemory::new();
    let replica_a = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let replica_b = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "freshly-created";

    // B looks the topic up *before* it exists (the read that used to poison the
    // monolithic cache).
    assert!(
        replica_b
            .topic_metadata(&TopicId::Name(topic.into()))
            .await?
            .is_none()
    );

    // Create on A.
    let id = create_topic(&replica_a, topic, 1).await?;

    // B must see it immediately, by name...
    let by_name = replica_b
        .topic_metadata(&TopicId::Name(topic.into()))
        .await?;
    assert_eq!(
        Some(topic),
        by_name
            .as_ref()
            .map(|metadata| metadata.topic.name.as_str())
    );
    assert_eq!(Some(id), by_name.as_ref().map(|metadata| metadata.id));

    // ...and by id, which exercises the `topic-ids/{uuid}.json` pointer across
    // replicas.
    let by_id = replica_b.topic_metadata(&TopicId::Id(id)).await?;
    assert_eq!(Some(id), by_id.map(|metadata| metadata.id));

    Ok(())
}

/// #28 survives #387: a topic created on another replica is visible **through
/// `Metadata`** at once, not after a topic-index window.
///
/// `Metadata` is now answered from the index (#387), whose snapshot may be up to
/// `TOPIC_INDEX_TTL` old — so on its own that would put a 30s hole exactly where
/// #28 needs none, and `create-then-produce` against a fresh topic would fail
/// with `UNKNOWN_TOPIC_OR_PARTITION` again. It does not, because a name the index
/// does not hold falls back to the topic's own object. That asymmetry is the whole
/// contract of the window: it delays *changes to* and *removals of* topics the
/// index already lists, never the appearance of a new one.
///
/// Warming B's index first is load-bearing: an empty index would miss the topic
/// for the trivial reason that it holds nothing, and prove nothing.
#[tokio::test]
async fn a_topic_created_on_a_peer_resolves_through_metadata_before_the_index_refreshes()
-> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let replica_a = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let replica_b = DynoStore::new(CLUSTER, NODE, bucket.clone());

    _ = create_topic(&replica_a, "already-there", 1).await?;

    // B's index is now fresh, and holds one topic.
    _ = replica_b.metadata(None).await?;

    let topic = "created-on-a-peer";
    let id = create_topic(&replica_a, topic, 3).await?;

    // B's snapshot cannot know it, and B must answer anyway.
    let response = replica_b
        .metadata(Some(&[TopicId::Name(topic.into())]))
        .await?;

    assert_eq!(1, response.topics.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(response.topics[0].error_code)?,
    );
    assert_eq!(Some(id.into_bytes()), response.topics[0].topic_id);
    assert_eq!(
        Some(3),
        response.topics[0]
            .partitions
            .as_ref()
            .map(|partitions| partitions.len()),
    );

    // And by id, which resolves through the `topic-ids/{uuid}.json` pointer before
    // reaching the index.
    let by_id = replica_b.metadata(Some(&[TopicId::Id(id)])).await?;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(by_id.topics[0].error_code)?,
    );

    Ok(())
}

/// `delete_topic` must remove the per-topic metadata object *and* its
/// `topic-ids/{uuid}.json` pointer from the bucket, so the topic is gone for a
/// replica that never cached it (and locally, the deleting replica's cache is
/// cleared too). A replica that had already cached the topic may still serve it
/// until its short metadata cache expires — that bounded staleness is the
/// pre-existing cache contract and is out of scope here.
#[tokio::test]
async fn delete_topic_removes_metadata_object_and_id_pointer() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let replica_a = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let topic = "transient";
    let id = create_topic(&replica_a, topic, 1).await?;

    _ = replica_a.delete_topic(&TopicId::Name(topic.into())).await?;

    // The deleting replica's own cache is cleared.
    assert!(
        replica_a
            .topic_metadata(&TopicId::Name(topic.into()))
            .await?
            .is_none()
    );

    // A replica that only ever sees the bucket post-delete finds nothing, by
    // name or by id (the metadata object and the id pointer are both gone).
    let replica_b = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert!(
        replica_b
            .topic_metadata(&TopicId::Name(topic.into()))
            .await?
            .is_none()
    );
    assert!(replica_b.topic_metadata(&TopicId::Id(id)).await?.is_none());

    Ok(())
}

/// An object store whose `delete_stream` fails for any location under a given
/// substring, modelling #251: the data deletion in `delete_topic` not completing
/// (an error, a throttle, a pod restart, a client timeout).
#[derive(Clone)]
struct DeleteFailsUnder<O> {
    inner: O,
    fragment: String,
}

impl<O> Debug for DeleteFailsUnder<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeleteFailsUnder").finish()
    }
}

impl<O> Display for DeleteFailsUnder<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeleteFailsUnder").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for DeleteFailsUnder<O>
where
    O: ObjectStore + Clone,
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
        let fragment = self.fragment.clone();

        self.inner.delete_stream(
            locations
                .map(move |item| match item {
                    Ok(location) if location.as_ref().contains(fragment.as_str()) => {
                        Err(object_store::Error::Generic {
                            store: "DeleteFailsUnder",
                            source: "injected delete failure".into(),
                        })
                    }
                    otherwise => otherwise,
                })
                .boxed(),
        )
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

/// #251: a `delete_topic` whose data deletion fails must leave the topic
/// **existing**.
///
/// The metadata object is the only handle on a topic's data — maintenance
/// discovers work by listing `topic-metadata/`, so a topic without one is never
/// revisited by anything, ever. Removing it before the data therefore turned any
/// mid-delete failure into permanently unreachable objects: a production audit
/// found 878,065 of them under two deleted topics, paid for indefinitely.
///
/// With the data deleted first, the same failure leaves a topic that still
/// exists with some of its data gone — visible, and recoverable by re-issuing
/// `DeleteTopics`. This asserts that recoverability, which is the whole property:
/// under the old ordering the topic would be gone from both replicas here while
/// its objects stayed behind.
#[tokio::test]
async fn a_failed_delete_leaves_the_topic_recoverable() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let topic = "stranded";

    let inner = InMemory::new();
    let bucket = DeleteFailsUnder {
        inner: inner.clone(),
        fragment: format!("/offsets/{topic}/"),
    };

    let replica_a = DynoStore::new(CLUSTER, NODE, bucket.clone());
    let id = create_topic(&replica_a, topic, 2).await?;

    // Give the delete something to remove outright: a group's committed offsets
    // for this topic. Since #246 the watermark objects under `topics/{name}/`
    // are rewritten as truncation tombstones rather than deleted, so they are no
    // longer a failure point — the offsets are. Seeded directly: what is under
    // test is the delete ordering, not how an offset is committed.
    for partition in 0..2 {
        _ = inner
            .put_opts(
                &Path::from(format!(
                    "clusters/{CLUSTER}/groups/consumers/a-group/offsets/{topic}/partitions/{partition:0>10}.json"
                )),
                PutPayload::from_static(br#"{"offset":0}"#),
                PutOptions::default(),
            )
            .await?;
    }

    // The delete must report the failure rather than swallow it.
    assert!(
        replica_a
            .delete_topic(&TopicId::Name(topic.into()))
            .await
            .is_err()
    );

    // ... and the topic must still be there, by name and by id, for the deleting
    // replica and for one that only ever sees the bucket.
    assert!(
        replica_a
            .topic_metadata(&TopicId::Name(topic.into()))
            .await?
            .is_some()
    );

    let replica_b = DynoStore::new(CLUSTER, NODE, bucket.clone());
    assert!(
        replica_b
            .topic_metadata(&TopicId::Name(topic.into()))
            .await?
            .is_some()
    );
    assert!(replica_b.topic_metadata(&TopicId::Id(id)).await?.is_some());

    Ok(())
}

/// A cluster written by an older tansu carries its topics inside the monolithic
/// `meta.json`. The one-shot backfill (run from `register_broker`) must
/// decompose them into per-topic objects + id pointers, idempotently.
#[tokio::test]
async fn legacy_meta_json_is_backfilled_into_per_topic_objects() -> Result<(), Error> {
    let _guard = init_tracing()?;

    #[derive(serde::Serialize)]
    struct LegacyMetaWrite {
        topics: BTreeMap<String, TopicMetadata>,
    }

    let bucket = InMemory::new();

    // Seed a legacy monolithic meta.json with one embedded topic.
    let id = Uuid::now_v7();
    let legacy = LegacyMetaWrite {
        topics: BTreeMap::from([(
            "legacy".to_string(),
            TopicMetadata {
                id,
                topic: CreatableTopic::default()
                    .name("legacy".into())
                    .num_partitions(3)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
            },
        )]),
    };
    _ = bucket
        .put_opts(
            &Path::from(format!("clusters/{CLUSTER}/meta.json")),
            PutPayload::from(serde_json::to_vec(&legacy)?),
            Default::default(),
        )
        .await?;

    let replica = DynoStore::new(CLUSTER, NODE, bucket.clone());

    // Not yet decomposed: the per-topic object does not exist.
    assert!(
        replica
            .topic_metadata(&TopicId::Name("legacy".into()))
            .await?
            .is_none()
    );

    // Run the backfill (idempotent — run twice).
    replica.migrate_legacy_topic_metadata().await?;
    replica.migrate_legacy_topic_metadata().await?;

    // Now visible by name and by id, with the original id preserved.
    let by_name = replica
        .topic_metadata(&TopicId::Name("legacy".into()))
        .await?;
    assert_eq!(Some(id), by_name.as_ref().map(|metadata| metadata.id));
    assert_eq!(
        Some(3),
        by_name.map(|metadata| metadata.topic.num_partitions)
    );
    assert_eq!(
        Some(id),
        replica
            .topic_metadata(&TopicId::Id(id))
            .await?
            .map(|metadata| metadata.id)
    );

    Ok(())
}

/// The list-all metadata path is served from the in-memory topic index (not a
/// per-request sweep of every per-topic object — the #29 OOM at scale). A local
/// create/delete invalidates the index, so list-all reflects the change at once.
#[tokio::test]
async fn list_all_reflects_create_and_delete_via_index() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let replica = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let names = |response: &crate::MetadataResponse| {
        response
            .topics()
            .iter()
            .filter_map(|topic| topic.name.clone())
            .collect::<std::collections::BTreeSet<_>>()
    };

    _ = create_topic(&replica, "alpha", 1).await?;
    _ = create_topic(&replica, "beta", 1).await?;

    let listed = names(&replica.metadata(None).await?);
    assert!(
        listed.contains("alpha") && listed.contains("beta"),
        "{listed:?}"
    );

    _ = replica.delete_topic(&TopicId::Name("alpha".into())).await?;

    let listed = names(&replica.metadata(None).await?);
    assert!(
        !listed.contains("alpha"),
        "alpha should be gone: {listed:?}"
    );
    assert!(listed.contains("beta"), "beta should remain: {listed:?}");

    Ok(())
}

/// The backfill is one-shot: once its marker is written, a later boot does not
/// re-scan `meta.json`. (Re-scanning every boot — re-loading the whole legacy
/// object and re-attempting a create per topic — is the O(topics) startup
/// cost/memory spike that crash-looped a large production cluster at its memory
/// limit.) A topic appended to the legacy object after the first run is not
/// picked up, proving the scan does not run again.
#[tokio::test]
async fn migration_is_one_shot() -> Result<(), Error> {
    let _guard = init_tracing()?;

    #[derive(serde::Serialize)]
    struct LegacyMetaWrite {
        topics: BTreeMap<String, TopicMetadata>,
    }

    fn legacy_topic(name: &str) -> (String, TopicMetadata) {
        (
            name.to_string(),
            TopicMetadata {
                id: Uuid::now_v7(),
                topic: CreatableTopic::default()
                    .name(name.into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
            },
        )
    }

    let bucket = InMemory::new();
    let meta = Path::from(format!("clusters/{CLUSTER}/meta.json"));

    // Legacy state with a single topic, then migrate.
    _ = bucket
        .put_opts(
            &meta,
            PutPayload::from(serde_json::to_vec(&LegacyMetaWrite {
                topics: BTreeMap::from([legacy_topic("a")]),
            })?),
            Default::default(),
        )
        .await?;

    let replica = DynoStore::new(CLUSTER, NODE, bucket.clone());
    replica.migrate_legacy_topic_metadata().await?;
    assert!(
        replica
            .topic_metadata(&TopicId::Name("a".into()))
            .await?
            .is_some()
    );

    // The legacy object later grows a second topic. A fresh replica's migration
    // must take the marker fast-path and NOT backfill it.
    _ = bucket
        .put_opts(
            &meta,
            PutPayload::from(serde_json::to_vec(&LegacyMetaWrite {
                topics: BTreeMap::from([legacy_topic("a"), legacy_topic("b")]),
            })?),
            Default::default(),
        )
        .await?;

    let replica = DynoStore::new(CLUSTER, NODE, bucket.clone());
    replica.migrate_legacy_topic_metadata().await?;
    assert!(
        replica
            .topic_metadata(&TopicId::Name("b".into()))
            .await?
            .is_none(),
        "marker should make migration one-shot; b must not be backfilled"
    );

    Ok(())
}
