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

use std::sync::Arc;

use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use tansu_sans_io::{CreateTopicsRequest, ErrorCode, create_topics_request::CreatableTopic};
use tansu_storage::{
    CreateTopicsService, OffsetCommitRequest, Storage, StorageContainer, Topition,
};
use url::Url;

use crate::common::{Error, cluster_id, init_tracing, storage_url};

mod common;

async fn storage() -> Result<Arc<Box<dyn Storage>>, Error> {
    StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092")?)
        .storage(storage_url()?)
        .build()
        .await
        .map_err(Into::into)
}

/// Committing then fetching offsets for many partitions at once must preserve
/// per-topition order and status, and round-trip each committed offset — the
/// invariant the concurrent (`buffered`) commit/fetch loops must keep. The
/// count spans several fetch-concurrency windows, and an unknown topic is
/// interleaved to check per-topition error handling and fail-open on absence.
#[tokio::test]
async fn concurrent_offset_commit_fetch_round_trip() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = storage().await?;

    const PARTITIONS: i32 = 40;
    let group = "g-round-trip";
    let known = "known-topic";

    // Create the topic the offsets belong to.
    let create = {
        let storage = storage.clone();
        MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
    };
    let created = create
        .serve(
            Context::default(),
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(known.into())
                        .num_partitions(PARTITIONS)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some([].into())),
                ]))
                .validate_only(Some(false)),
        )
        .await?;
    assert_eq!(
        i16::from(ErrorCode::None),
        created.topics.unwrap_or_default()[0].error_code
    );

    // Commit offset partition*10 for each known partition, then one partition
    // of a topic that does not exist (last, to check order + status).
    let mut offsets = (0..PARTITIONS)
        .map(|p| {
            (
                Topition::new(known, p),
                OffsetCommitRequest::default().offset(i64::from(p) * 10),
            )
        })
        .collect::<Vec<_>>();
    offsets.push((
        Topition::new("missing-topic", 0),
        OffsetCommitRequest::default().offset(999),
    ));

    let committed = storage.offset_commit(group, None, &offsets).await?;

    // One entry per request, in request order, with per-topition status.
    assert_eq!(offsets.len(), committed.len());
    for (index, (topition, code)) in committed.iter().enumerate() {
        assert_eq!(
            &offsets[index].0, topition,
            "commit out of order at {index}"
        );
        let expected = if topition.topic() == known {
            ErrorCode::None
        } else {
            ErrorCode::UnknownTopicOrPartition
        };
        assert_eq!(expected, *code, "bad status at {index}");
    }

    // Fetch back all known partitions plus one never-committed partition.
    let mut fetch = (0..PARTITIONS)
        .map(|p| Topition::new(known, p))
        .collect::<Vec<_>>();
    fetch.push(Topition::new(known, PARTITIONS + 1)); // never committed -> -1

    let fetched = storage
        .offset_fetch(Some(group), &fetch, Some(false))
        .await?;

    for p in 0..PARTITIONS {
        assert_eq!(
            Some(i64::from(p) * 10),
            fetched
                .get(&Topition::new(known, p))
                .map(|committed| committed.offset),
            "wrong committed offset for partition {p}"
        );
    }
    assert_eq!(
        Some(-1),
        fetched
            .get(&Topition::new(known, PARTITIONS + 1))
            .map(|committed| committed.offset),
        "never-committed partition must fetch -1"
    );

    Ok(())
}

/// Commit metadata survives the round trip (#445).
///
/// It was accepted and stored all along, and dropped by the **return type** of
/// the read: `offset_fetch` answered `BTreeMap<Topition, i64>`, so no response
/// builder could have carried it however it was written. Nothing reported the
/// loss at write time — the commit succeeded — and frameworks that keep their
/// restore point in commit metadata (Streams-style checkpointing, recovery
/// tools) found out during a recovery, which is the worst moment there is.
mod metadata {
    use std::sync::Arc;

    use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
    use tansu_sans_io::{
        CreateTopicsRequest, ErrorCode, create_topics_request::CreatableTopic,
        offset_commit_request::OffsetCommitRequestPartition,
    };
    use tansu_storage::{
        CreateTopicsService, OffsetCommitRequest, Storage, StorageContainer, Topition,
    };
    use url::Url;

    use crate::common::{Error, cluster_id, init_tracing, storage_url};

    const TOPIC: &str = "commit-metadata";
    const GROUP: &str = "checkpointing";
    const CHECKPOINT: &str = "checkpoint-abc";

    async fn storage() -> Result<Arc<Box<dyn Storage>>, Error> {
        let storage = StorageContainer::builder()
            .cluster_id(cluster_id())
            .node_id(111)
            .advertised_listener(Url::parse("tcp://localhost:9092")?)
            .storage(storage_url()?)
            .build()
            .await?;

        let create = {
            let storage = storage.clone();
            MapStateLayer::new(|_| storage).into_layer(CreateTopicsService)
        };

        let created = create
            .serve(
                Context::default(),
                CreateTopicsRequest::default()
                    .topics(Some(
                        [CreatableTopic::default()
                            .name(TOPIC.into())
                            .num_partitions(1)
                            .replication_factor(1)
                            .assignments(Some([].into()))
                            .configs(Some([].into()))]
                        .into(),
                    ))
                    .validate_only(Some(false)),
            )
            .await?;

        let topics = created.topics.unwrap_or_default();
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        Ok(storage)
    }

    async fn commit(storage: &Arc<Box<dyn Storage>>, metadata: Option<&str>) -> Result<(), Error> {
        let committed = storage
            .offset_commit(
                GROUP,
                None,
                &[(
                    Topition::new(TOPIC, 0),
                    OffsetCommitRequest::try_from(
                        &OffsetCommitRequestPartition::default()
                            .partition_index(0)
                            .committed_offset(7)
                            .committed_leader_epoch(Some(-1))
                            .commit_timestamp(None)
                            .committed_metadata(metadata.map(ToOwned::to_owned)),
                    )?,
                )],
            )
            .await?;

        assert_eq!(ErrorCode::None, committed[0].1);

        Ok(())
    }

    /// Through `Storage`, where the value now survives at all.
    #[tokio::test]
    async fn metadata_committed_is_metadata_fetched() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = storage().await?;
        commit(&storage, Some(CHECKPOINT)).await?;

        let fetched = storage
            .offset_fetch(Some(GROUP), &[Topition::new(TOPIC, 0)], None)
            .await?;

        let committed = fetched
            .get(&Topition::new(TOPIC, 0))
            .expect("the partition was committed");

        assert_eq!(7, committed.offset);
        assert_eq!(Some(CHECKPOINT.to_owned()), committed.metadata);

        Ok(())
    }

    /// A commit that carried no metadata still reads back as none, rather than
    /// as an empty string: a framework that stores `""` deliberately and one
    /// that stores nothing are different, and the round trip has to keep them
    /// apart at the storage layer.
    #[tokio::test]
    async fn no_metadata_committed_is_no_metadata_fetched() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = storage().await?;
        commit(&storage, None).await?;

        let fetched = storage
            .offset_fetch(Some(GROUP), &[Topition::new(TOPIC, 0)], None)
            .await?;

        assert_eq!(
            None,
            fetched
                .get(&Topition::new(TOPIC, 0))
                .and_then(|committed| committed.metadata.clone())
        );

        Ok(())
    }

    /// A partition nothing has committed still answers `-1` with no metadata —
    /// the sentinel is unchanged by carrying a second field beside it.
    #[tokio::test]
    async fn an_uncommitted_partition_is_still_minus_one() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let storage = storage().await?;

        let fetched = storage
            .offset_fetch(Some(GROUP), &[Topition::new(TOPIC, 0)], None)
            .await?;

        let committed = fetched.get(&Topition::new(TOPIC, 0)).expect("an answer");

        assert_eq!(-1, committed.offset);
        assert_eq!(None, committed.metadata);

        Ok(())
    }
}
