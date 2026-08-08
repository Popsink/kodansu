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

//! The `null://` engine.
//!
//! It is the sink `just broker-null`, `samply-null` and `flamegraph-null` point
//! a broker at to measure everything *except* storage, and it had no test at all
//! — 0% of the file was covered by the suite. That matters more than a discard
//! sink sounds like it should: a perf or flamegraph run against `null://` dies
//! wherever the first unimplemented method is, and it dies mid-measurement,
//! looking like a broker bug rather than a missing arm in the sink.
//!
//! So what is asserted here is the contract the profiling runs depend on: the
//! read paths answer from the topics that were created, the write paths accept
//! and discard, and the three methods that genuinely cannot work without storage
//! say so with `FeatureNotEnabled` rather than panicking.

use std::{collections::BTreeMap, time::Duration};

use bytes::Bytes;
use tansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    record::{Record, deflated, inflated},
    txn_offset_commit_request::TxnOffsetCommitRequestTopic,
};
use tansu_storage::{
    BrokerRegistrationRequest, Error, GenerationDoc, NamedGroupDetail, OffsetCommitRequest,
    ScramCredential, Storage, StorageContainer, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError,
};
use url::Url;
use uuid::Uuid;

use crate::common::init_tracing;

mod common;

const CLUSTER: &str = "tansu";
const HOST: &str = "localhost";
const NODE_ID: i32 = 111;
const PORT: u16 = 9092;

type Result<T = (), E = Box<dyn std::error::Error>> = std::result::Result<T, E>;

async fn null_storage() -> Result<std::sync::Arc<Box<dyn Storage>>> {
    StorageContainer::builder()
        .cluster_id(CLUSTER)
        .node_id(NODE_ID)
        .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
        .storage(Url::parse("null://sink/")?)
        .silent(true)
        .build()
        .await
        .map_err(Into::into)
}

fn topic(name: &str, partitions: i32, replication_factor: i16) -> CreatableTopic {
    CreatableTopic::default()
        .name(name.into())
        .num_partitions(partitions)
        .replication_factor(replication_factor)
}

/// The identity a broker announces at startup has to survive the round trip, or
/// clients are handed an address that resolves to nothing.
#[tokio::test]
async fn broker_identity_is_the_configured_one() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    assert_eq!(CLUSTER, storage.cluster_id().await?);
    assert_eq!(NODE_ID, storage.node().await?);
    assert_eq!(
        Url::parse(&format!("tcp://{HOST}:{PORT}"))?,
        storage.advertised_listener().await?
    );

    storage.ping().await?;

    storage
        .register_broker(BrokerRegistrationRequest {
            broker_id: NODE_ID,
            cluster_id: CLUSTER.into(),
            incarnation_id: Uuid::now_v7(),
            rack: None,
        })
        .await?;

    let brokers = storage.brokers().await?;
    assert_eq!(1, brokers.len());
    assert_eq!(NODE_ID, brokers[0].broker_id);
    assert_eq!(HOST, brokers[0].host.as_str());
    assert_eq!(i32::from(PORT), brokers[0].port);

    Ok(())
}

/// The sink discards records, but it does not discard topics: a producer asks
/// for metadata before it can address a partition, so the topics created have to
/// come back with the partition count they were created with.
#[tokio::test]
async fn created_topics_are_visible_in_metadata() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    assert!(storage.metadata(None).await?.topics().is_empty());

    _ = storage.create_topic(topic("abc", 3, 2), false).await?;
    _ = storage.create_topic(topic("def", 1, 1), false).await?;

    let metadata = storage.metadata(None).await?;

    assert_eq!(Some(CLUSTER), metadata.cluster());
    assert_eq!(Some(NODE_ID), metadata.controller());

    let topics = metadata.topics();
    assert_eq!(2, topics.len());

    let abc = topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some("abc"))
        .expect("abc");

    let partitions = abc.partitions.as_ref().expect("partitions");
    assert_eq!(3, partitions.len());
    assert_eq!(
        vec![0, 1, 2],
        partitions
            .iter()
            .map(|partition| partition.partition_index)
            .collect::<Vec<_>>()
    );
    // Replication factor 2 on a single node means the same node listed twice:
    // the sink has no second broker to name.
    assert_eq!(Some(vec![NODE_ID; 2]), partitions[0].replica_nodes.clone());
    assert!(
        partitions
            .iter()
            .all(|partition| partition.leader_id == NODE_ID)
    );

    // describe_topic_partitions reads the same registry, and drifting apart from
    // metadata is exactly the kind of thing nothing would have caught.
    let described = storage.describe_topic_partitions(None, 100, None).await?;
    assert_eq!(2, described.len());
    assert_eq!(
        3,
        described
            .iter()
            .find(|topic| topic.name.as_deref() == Some("abc"))
            .and_then(|topic| topic.partitions.as_ref())
            .map(|partitions| partitions.len())
            .expect("abc partitions")
    );

    Ok(())
}

/// Creating the same topic twice is `TOPIC_ALREADY_EXISTS`, not a second entry —
/// otherwise metadata grows a duplicate and a client picks whichever it saw
/// first.
#[tokio::test]
async fn creating_a_topic_twice_is_rejected() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    _ = storage.create_topic(topic("abc", 3, 1), false).await?;

    assert!(matches!(
        storage.create_topic(topic("abc", 3, 1), false).await,
        Err(Error::Api(ErrorCode::TopicAlreadyExists))
    ));

    assert_eq!(1, storage.metadata(None).await?.topics().len());

    Ok(())
}

/// Produce accepts and discards; fetch returns nothing; the offsets a consumer
/// asks about are answered without error. A profiling run drives this loop
/// millions of times and must not see an error code from any of it.
#[tokio::test]
async fn produce_is_accepted_and_fetch_returns_nothing() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    _ = storage.create_topic(topic("abc", 1, 1), false).await?;

    let topition = Topition::new("abc", 0);

    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(b"lorem").into()))
        .build()
        .and_then(deflated::Batch::try_from)?;

    // The sink answers with an offset rather than an error: a producer that is
    // told its write failed stops producing, which ends the measurement.
    assert_eq!(6, storage.produce(None, &topition, batch).await?);

    let batches = storage
        .fetch(
            &topition,
            0,
            1,
            1024 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(1),
        )
        .await?;
    assert!(batches.is_empty());

    let offsets = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await?;
    assert_eq!(1, offsets.len());
    assert_eq!(topition, offsets[0].0);
    assert_eq!(ErrorCode::None, offsets[0].1.error_code);
    assert_eq!(Some(0), offsets[0].1.offset);

    let stage = storage.offset_stage(&topition).await?;
    assert_eq!(0, stage.high_watermark());
    assert_eq!(0, stage.log_start());

    let fetched = storage
        .offset_fetch(Some("g1"), std::slice::from_ref(&topition), None)
        .await?;
    assert_eq!(BTreeMap::from([(topition.clone(), 0)]), fetched);

    // A commit is acknowledged per partition, and nothing is retained: the
    // committed set stays empty however many commits arrive.
    assert_eq!(
        vec![(topition.clone(), ErrorCode::None)],
        storage
            .offset_commit(
                "g1",
                None,
                &[(topition, OffsetCommitRequest::default().offset(0))],
            )
            .await?
    );
    assert!(storage.committed_offset_topitions("g1").await?.is_empty());

    Ok(())
}

/// A group's composition is the one thing the sink does keep, because the
/// coordinator read-modify-writes it under a version and treats a mismatch as a
/// lost race. A store that handed out versions it then failed to recognise
/// would put the coordinator in a retry loop it can never leave.
#[tokio::test]
async fn a_group_update_round_trips_its_version() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    let generation = |generation_id| GenerationDoc {
        generation_id,
        session_timeout_ms: 45_000,
        ..Default::default()
    };

    // `UpdateError` deliberately does not implement `Display`, so `?` cannot
    // convert it — every call here unwraps its own outcome.
    let first = storage
        .update_group_generation("g1", generation(1), None)
        .await
        .expect("create");

    let second = storage
        .update_group_generation("g1", generation(2), Some(first.clone()))
        .await
        .expect("update");
    assert_ne!(first, second);

    // Replaying the version that has just been superseded is the lost race, and
    // it has to come back as `Outdated` carrying what is actually stored — the
    // coordinator merges onto `current` and retries.
    match storage
        .update_group_generation("g1", generation(1), Some(first))
        .await
    {
        Err(UpdateError::Outdated { current, version }) => {
            assert_eq!(2, current.generation_id);
            assert_eq!(second, version);
        }
        otherwise => panic!("expected Outdated, got {otherwise:?}"),
    }

    // A group nobody has described is not a group: `describe_groups` answers
    // per-name so a caller asking about a stale id gets an error code, not a
    // truncated list it has to re-align by index.
    assert_eq!(
        vec![
            NamedGroupDetail::error_code("g1".into(), ErrorCode::GroupIdNotFound),
            NamedGroupDetail::error_code("g2".into(), ErrorCode::GroupIdNotFound),
        ],
        storage
            .describe_groups(Some(&["g1".into(), "g2".into()]), false)
            .await?
    );

    let deleted = storage.delete_groups(Some(&["g1".into()])).await?;
    assert_eq!(1, deleted.len());
    assert_eq!("g1", deleted[0].group_id.as_str());
    assert_eq!(i16::from(ErrorCode::None), deleted[0].error_code);

    Ok(())
}

/// The transaction methods are all no-ops, but the shape of what they return is
/// not: a response is matched back to its request positionally, so a sink that
/// answers a two-topic request with one result desynchronises the client.
#[tokio::test]
async fn transactions_are_acknowledged_shape_intact() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    let producer = storage
        .init_producer(Some("txn-1"), 30_000, None, None)
        .await?;
    assert_eq!(ErrorCode::None, producer.error);

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_add_offsets("txn-1", producer.id, producer.epoch, "g1")
            .await?
    );

    // One result per topic, in the order asked. Two topics in, two out.
    let added = storage
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: "txn-1".into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default().name("abc".into()),
                AddPartitionsToTxnTopic::default().name("def".into()),
            ],
        })
        .await?;

    let TxnAddPartitionsResponse::VersionZeroToThree(results) = added else {
        panic!("a v0-3 request must not be answered with a v4+ response");
    };
    assert_eq!(
        vec!["abc", "def"],
        results
            .iter()
            .map(|result| result.name.as_str())
            .collect::<Vec<_>>()
    );

    assert_eq!(
        vec!["abc"],
        storage
            .txn_offset_commit(TxnOffsetCommitRequest {
                transaction_id: "txn-1".into(),
                group_id: "g1".into(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                generation_id: None,
                member_id: None,
                group_instance_id: None,
                topics: vec![TxnOffsetCommitRequestTopic::default().name("abc".into())],
            })
            .await?
            .iter()
            .map(|topic| topic.name.as_str())
            .collect::<Vec<_>>()
    );

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end("txn-1", producer.id, producer.epoch, true)
            .await?
    );

    Ok(())
}

/// Configuration is described rather than stored, and `delete_topic` succeeds
/// because there is nothing to fail at. Both are on the admin path a perf run
/// crosses on the way in.
#[tokio::test]
async fn admin_paths_answer_without_storage() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    _ = storage.create_topic(topic("abc", 1, 1), false).await?;

    let described = storage
        .describe_config("abc", ConfigResource::Topic, None)
        .await?;
    assert_eq!("abc", described.resource_name.as_str());
    assert_eq!(i16::from(ErrorCode::None), described.error_code);

    // An alter is echoed back rather than stored, but the resource it echoes has
    // to be the one asked about — a client matches the response to its request
    // by name and type.
    let altered = storage
        .incremental_alter_resource(
            AlterConfigsResource::default()
                .resource_name("abc".into())
                .resource_type(ConfigResource::Topic.into()),
        )
        .await?;
    assert_eq!("abc", altered.resource_name.as_str());
    assert_eq!(i8::from(ConfigResource::Topic), altered.resource_type);
    assert_eq!(i16::from(ErrorCode::None), altered.error_code);

    assert_eq!(
        ErrorCode::None,
        storage.delete_topic(&TopicId::Name("abc".into())).await?
    );

    assert!(storage.list_groups(None).await?.is_empty());

    storage.maintain(std::time::SystemTime::now()).await?;

    Ok(())
}

/// The methods that cannot be faked. Answering `Ok` here would be worse than
/// erroring: `tansu user create` against a `null://` broker would report success
/// and store nothing.
#[tokio::test]
async fn credentials_and_record_deletion_report_feature_not_enabled() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    assert!(matches!(
        storage.delete_records(&[]).await,
        Err(Error::FeatureNotEnabled { .. })
    ));

    assert!(matches!(
        storage
            .user_scram_credential("alice", ScramMechanism::Scram512)
            .await,
        Err(Error::FeatureNotEnabled { .. })
    ));

    assert!(matches!(
        storage
            .delete_user_scram_credential("alice", ScramMechanism::Scram512)
            .await,
        Err(Error::FeatureNotEnabled { .. })
    ));

    assert!(matches!(
        storage
            .upsert_user_scram_credential(
                "alice",
                ScramMechanism::Scram512,
                ScramCredential::default(),
            )
            .await,
        Err(Error::FeatureNotEnabled { .. })
    ));

    Ok(())
}
