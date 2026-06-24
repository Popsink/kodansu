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

use object_store::{ObjectStore as _, PutPayload, memory::InMemory, path::Path};
use std::collections::BTreeMap;
use tansu_sans_io::create_topics_request::CreatableTopic;
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
