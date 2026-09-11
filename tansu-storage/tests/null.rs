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
//!
//! The four document families below — members, generations, assignments, ACLs
//! and quotas — are the part of the sink that is not a discard at all. It holds
//! them in `BTreeMap`s behind the same CAS contract the object store gives, and
//! the reason to assert that here rather than trust it is that a profiling run
//! against `null://` drives the *whole* coordinator: a version this engine hands
//! out and then fails to recognise puts the coordinator in a retry loop, and the
//! flamegraph shows the retry rather than the work (#556's fifth milestone).

use std::{collections::BTreeMap, time::Duration};

use bytes::Bytes;
use tansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
    acl::{Operation, Permission, Resource},
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    join_group_response::JoinGroupResponseMember,
    record::{Record, deflated, inflated},
    resource::Pattern,
    txn_offset_commit_request::TxnOffsetCommitRequestTopic,
};
use tansu_storage::{
    AclBinding, AclFilter, AssignmentDoc, AssignmentOutcome, AutoTopicCreate,
    BrokerRegistrationRequest, CommittedOffset, DEFAULT_FETCH_MAX_BYTES, Error, GenerationDoc,
    MemberDoc, NamedGroupDetail, OffsetCommitRequest, PRODUCER_BYTE_RATE, QuotaAlteration,
    QuotaEntity, QuotaFilterComponent, QuotaLimits, QuotaMatch, QuotaOp, ScramCredential, Storage,
    StorageContainer, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, USER_ENTITY, UpdateError, Version, WILDCARD_HOST,
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

async fn null_storage() -> Result<std::sync::Arc<dyn Storage>> {
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

fn member(member_id: &str, last_contact_ms: i64) -> MemberDoc {
    MemberDoc {
        last_contact_ms,
        session_timeout_ms: 45_000,
        join_response: JoinGroupResponseMember::default().member_id(member_id.into()),
        ..Default::default()
    }
}

fn assignment(generation_id: i32) -> AssignmentDoc {
    AssignmentDoc {
        generation_id,
        leader: "m1".into(),
        protocol_type: "consumer".into(),
        protocol_name: "range".into(),
        assignments: BTreeMap::from([("m1".to_owned(), Bytes::from_static(b"ab"))]),
        assigned_at_ms: 1_000,
    }
}

fn binding(operation: Operation) -> AclBinding {
    AclBinding {
        resource_type: Resource::Topic,
        resource_name: "abc".into(),
        pattern: Pattern::Literal,
        principal: "User:alice".into(),
        host: WILDCARD_HOST.into(),
        operation,
        permission: Permission::Allow,
    }
}

fn alteration(user: &str, key: &str, value: f64) -> QuotaAlteration {
    QuotaAlteration {
        entity: QuotaEntity::User(user.into()),
        ops: vec![QuotaOp {
            key: key.into(),
            value,
            remove: false,
        }],
    }
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
    assert_eq!(
        BTreeMap::from([(topition.clone(), CommittedOffset::new(0, None))]),
        fetched
    );

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

/// A member document is CAS'd on the version it was read at, and the sink has
/// to recognise the versions it hands out.
///
/// The coordinator renews a member's liveness by read-modify-write under the
/// held version, and reads a mismatch as "another replica got there first". An
/// engine whose versions never match makes every renewal a lost race, and a
/// profiling run then measures the retry.
#[tokio::test]
async fn a_member_document_cas_matches_the_version_it_handed_out() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    let first = storage
        .write_group_member("g1", "m1", member("m1", 1_000), None)
        .await
        .expect("create");

    assert_eq!(
        Some((member("m1", 1_000), first.clone())),
        storage.read_group_member("g1", "m1").await?
    );

    let second = storage
        .write_group_member("g1", "m1", member("m1", 2_000), Some(first.clone()))
        .await
        .expect("renew");
    assert_ne!(first, second);

    match storage
        .write_group_member("g1", "m1", member("m1", 3_000), Some(first))
        .await
    {
        Err(UpdateError::Outdated { current, version }) => {
            assert_eq!(2_000, current.last_contact_ms);
            assert_eq!(second, version);
        }
        otherwise => panic!("expected Outdated, got {otherwise:?}"),
    }

    // A CAS against a member that is not there cannot be `Outdated`: there is
    // no `current` to merge onto, so the caller has to be told its identity is
    // gone rather than handed a document to retry against.
    match storage
        .write_group_member("g1", "ghost", member("ghost", 1_000), Some(second))
        .await
    {
        Err(UpdateError::Error(Error::Api(ErrorCode::UnknownMemberId))) => (),
        otherwise => panic!("expected UnknownMemberId, got {otherwise:?}"),
    }

    Ok(())
}

/// Every read of a group's members is scoped to that group, and a delete
/// removes one document rather than the group.
///
/// The documents are keyed `(group, member)` in one map, so "the members of g1"
/// is a filter and not a lookup — which is exactly the shape that answers with
/// another group's members if the filter is wrong.
#[tokio::test]
async fn a_group_sees_its_own_member_documents_and_no_others() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    for (group, member_id, last_contact_ms) in [
        ("g1", "m1", 1_000),
        ("g1", "m2", 2_000),
        ("g2", "m3", 3_000),
    ] {
        _ = storage
            .write_group_member(group, member_id, member(member_id, last_contact_ms), None)
            .await
            .expect(member_id);
    }

    assert_eq!(
        vec!["m1", "m2"],
        storage
            .list_group_members("g1")
            .await?
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );

    // The cheap listing batch admission elects from (#427): the same member
    // set, carrying each document's own stamp rather than the document.
    assert_eq!(
        BTreeMap::from([("m1".to_owned(), 1_000), ("m2".to_owned(), 2_000)]),
        storage.list_group_member_stamps("g1").await?
    );

    storage.delete_group_member("g1", "m1").await?;

    assert_eq!(None, storage.read_group_member("g1", "m1").await?);
    assert_eq!(
        vec!["m2"],
        storage
            .list_group_members("g1")
            .await?
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        vec!["m3"],
        storage
            .list_group_members("g2")
            .await?
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// A generation CAS against a group that does not exist is an error, not a
/// lost race.
///
/// The distinction is the whole of the caller's retry: an `Outdated` says "read
/// me again and merge", and there is nothing to read. Returning one for a group
/// that was never written would send the coordinator round a loop whose next
/// read answers `None`.
#[tokio::test]
async fn a_generation_cas_against_no_group_is_not_a_lost_race() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    assert_eq!(None, storage.read_group_generation("g1").await?);

    match storage
        .update_group_generation("g1", GenerationDoc::default(), Some(Version::default()))
        .await
    {
        Err(UpdateError::Error(Error::Api(ErrorCode::GroupIdNotFound))) => (),
        otherwise => panic!("expected GroupIdNotFound, got {otherwise:?}"),
    }

    let written = storage
        .update_group_generation(
            "g1",
            GenerationDoc {
                generation_id: 1,
                session_timeout_ms: 45_000,
                ..Default::default()
            },
            None,
        )
        .await
        .expect("create");

    let (held, version) = storage.read_group_generation("g1").await?.expect("g1");
    assert_eq!(1, held.generation_id);
    assert_eq!(written, version);

    Ok(())
}

/// An assignment is create-only, and retired by generation rather than by age.
///
/// Create-only is what makes a generation's assignment safe to read without a
/// version: two replicas racing to publish the same generation's assignment
/// must not produce two different partition maps, so the loser is handed the
/// winner's document and syncs from that.
#[tokio::test]
async fn an_assignment_is_written_once_and_retired_by_generation() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    assert!(matches!(
        storage
            .create_group_assignment("g1", 1, assignment(1))
            .await?,
        AssignmentOutcome::Created(_)
    ));

    match storage
        .create_group_assignment("g1", 1, assignment(9))
        .await?
    {
        AssignmentOutcome::AlreadyExists(current) => assert_eq!(assignment(1), *current),
        otherwise => panic!("expected AlreadyExists, got {otherwise:?}"),
    }

    assert_eq!(
        Some(assignment(1)),
        storage.read_group_assignment("g1", 1).await?
    );
    assert_eq!(None, storage.read_group_assignment("g1", 2).await?);

    for (group, generation_id) in [("g1", 2), ("g1", 3), ("g2", 1)] {
        _ = storage
            .create_group_assignment(group, generation_id, assignment(generation_id))
            .await?;
    }

    // Retiring below generation 3 leaves generation 3 and every other group's
    // assignments where they were: the key is `(group, generation)`, so a
    // retirement that ignored the group would empty the cluster.
    assert_eq!(2, storage.delete_group_assignments_before("g1", 3).await?);
    assert_eq!(None, storage.read_group_assignment("g1", 2).await?);
    assert_eq!(
        Some(assignment(3)),
        storage.read_group_assignment("g1", 3).await?
    );
    assert_eq!(
        Some(assignment(1)),
        storage.read_group_assignment("g2", 1).await?
    );

    Ok(())
}

/// What an operator applies to the sink is what it describes back, and a
/// narrowed delete removes only what it selected.
///
/// `kafka-acls.sh` against a `null://` broker is a plausible way to rehearse a
/// rule set, and #363 is the precedent for why that has to be asserted rather
/// than assumed: create and describe answered success without touching
/// anything, so the tool appeared to work.
#[tokio::test]
async fn acls_applied_to_the_sink_are_the_acls_it_describes() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    let read = binding(Operation::Read);
    let write = binding(Operation::Write);

    assert_eq!(
        vec![ErrorCode::None; 2],
        storage.create_acls(&[read.clone(), write.clone()]).await?
    );

    // Creating the same binding twice is not two bindings: they are held in a
    // set, keyed by everything that makes a rule a rule.
    assert_eq!(
        vec![ErrorCode::None],
        storage.create_acls(std::slice::from_ref(&read)).await?
    );

    assert_eq!(
        vec![read.clone(), write.clone()],
        storage.describe_acls(&AclFilter::any()).await?
    );

    let narrowed = AclFilter {
        operation: Operation::Read,
        ..AclFilter::any()
    };

    assert_eq!(vec![read.clone()], storage.describe_acls(&narrowed).await?);

    assert_eq!(
        vec![vec![read]],
        storage.delete_acls(std::slice::from_ref(&narrowed)).await?
    );

    assert_eq!(vec![write], storage.describe_acls(&AclFilter::any()).await?);
    assert!(storage.delete_acls(std::slice::from_ref(&narrowed)).await?[0].is_empty());

    Ok(())
}

/// A quota alteration is validated against a copy, so a `validate_only` call
/// and a refused key both leave the document as they found it.
///
/// The refusal matters more than it looks: `alter_client_quotas` answers one
/// error code per alteration, and a partially-applied batch would leave the
/// stored document in a state no client asked for and no describe explains.
#[tokio::test]
async fn a_quota_is_validated_against_a_copy_before_it_is_stored() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    assert_eq!(
        vec![ErrorCode::None],
        storage
            .alter_client_quotas(&[alteration("alice", PRODUCER_BYTE_RATE, 1_024.0)], true)
            .await?
    );
    assert!(storage.client_quotas().await?.users.is_empty());

    assert_eq!(
        vec![ErrorCode::None],
        storage
            .alter_client_quotas(&[alteration("alice", PRODUCER_BYTE_RATE, 1_024.0)], false)
            .await?
    );

    let alice = QuotaLimits {
        producer_byte_rate: Some(1_024.0),
        ..Default::default()
    };

    assert_eq!(
        BTreeMap::from([("alice".to_owned(), alice)]),
        storage.client_quotas().await?.users
    );

    assert_eq!(
        vec![ErrorCode::InvalidConfig],
        storage
            .alter_client_quotas(&[alteration("bob", "no.such.rate", 1.0)], false)
            .await?
    );
    assert!(!storage.client_quotas().await?.users.contains_key("bob"));

    assert_eq!(
        vec![(QuotaEntity::User("alice".into()), alice)],
        storage
            .describe_client_quotas(
                &[QuotaFilterComponent {
                    entity_type: USER_ENTITY.into(),
                    matches: QuotaMatch::Exact("alice".into()),
                }],
                true,
            )
            .await?
    );

    // A strict filter that names no entity type this broker knows selects
    // nothing, rather than everything.
    assert!(storage.describe_client_quotas(&[], true).await?.is_empty());

    // Removing the only key configured removes the entity rather than storing
    // it empty, so a later describe says "no quota" and not "a quota with no
    // values".
    assert_eq!(
        vec![ErrorCode::None],
        storage
            .alter_client_quotas(
                &[QuotaAlteration {
                    entity: QuotaEntity::User("alice".into()),
                    ops: vec![QuotaOp {
                        key: PRODUCER_BYTE_RATE.into(),
                        value: 0.0,
                        remove: true,
                    }],
                }],
                false,
            )
            .await?
    );
    assert!(storage.client_quotas().await?.users.is_empty());

    Ok(())
}

/// The three methods #551 stated rather than inherited answer what the trait
/// defaults answered.
///
/// A default is a method a wrapper can forget without the compiler noticing
/// (#273), which is why they were removed — and stating them means they can now
/// drift from what they replaced, which is why this asserts the values rather
/// than that they return at all.
#[tokio::test]
async fn the_stated_answers_are_the_ones_the_trait_defaults_gave() -> Result {
    let _guard = init_tracing()?;

    let storage = null_storage().await?;

    assert_eq!(
        AutoTopicCreate::default(),
        storage.auto_create_topic_config()
    );
    assert_eq!(DEFAULT_FETCH_MAX_BYTES, storage.fetch_max_bytes());

    let topition = Topition::new("abc", 0);

    assert_eq!(
        storage.offset_stage(&topition).await?,
        storage
            .offset_stage_at(&topition, IsolationLevel::ReadCommitted)
            .await?
    );

    Ok(())
}
