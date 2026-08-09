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

//! The breach, closed: one tenant cannot read or write another's topics (#363).
//!
//! Until now `#363`'s impact statement was literally true — "any tenant can
//! read and delete every other tenant's topics" — because the ACL APIs stored
//! nothing and nothing consulted them. This drives produce and fetch through
//! the real services with a real `Authorizer` in the context, which is the
//! arrangement a broker with `--authentication` builds.
//!
//! Written against `PREFIXED` rules on `tenant-a.` and `tenant-b.`, because
//! that is the whole multi-tenancy mechanism: no notion of a tenant exists
//! anywhere in the broker, only a prefix in a rule.

use std::sync::Arc;

use bytes::Bytes;
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use tansu_sans_io::{
    CreateAclsRequest, CreateTopicsRequest, DeleteAclsRequest, DeleteTopicsRequest,
    DescribeAclsRequest, ErrorCode, FetchRequest, IsolationLevel, NULL_TOPIC_ID, ProduceRequest,
    acl::{Operation, Permission, Resource},
    create_acls_request::AclCreation,
    create_topics_request::CreatableTopic,
    delete_acls_request::DeleteAclsFilter,
    delete_topics_request::DeleteTopicState,
    fetch_request::{FetchPartition, FetchTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
    resource::Pattern,
};
use tansu_storage::{
    AclBinding, AclFilter, Authorizer, CLUSTER_RESOURCE, CreateAclsService, CreateTopicsService,
    DeleteAclsService, DeleteTopicsService, DescribeAclsService, Error, FetchService,
    ProduceService, Requester, Storage, StorageContainer, WILDCARD_HOST,
};
use url::Url;

const ALICE: &str = "User:alice";
const BOB: &str = "User:bob";
const HOST: &str = "10.0.0.1";

async fn storage() -> Result<Arc<Box<dyn Storage>>, Error> {
    StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .storage(Url::parse("memory://")?)
        .build()
        .await
}

/// The context a broker with `--authentication` builds for a request: who is
/// asking, and the decision to ask.
fn asking(storage: &Arc<Box<dyn Storage>>, principal: &str) -> Context<Arc<Box<dyn Storage>>> {
    let mut ctx = Context::default();

    _ = ctx.insert(Requester {
        principal: Some(principal.to_owned()),
        host: HOST.into(),
    });

    // Zero TTL so a rule applied by the test is seen by the next request; the
    // window itself is `authorizer::tests`' subject, not this file's.
    _ = ctx.insert(
        Authorizer::new(storage.clone(), [] as [String; 0]).with_ttl(std::time::Duration::ZERO),
    );

    ctx.map_state(|()| storage.clone())
}

async fn create_topic(storage: &Arc<Box<dyn Storage>>, name: &str) -> Result<(), Error> {
    let service = MapStateLayer::new({
        let storage = storage.clone();
        move |_| storage.clone()
    })
    .into_layer(CreateTopicsService);

    _ = service
        .serve(
            Context::default(),
            CreateTopicsRequest::default().topics(Some(vec![
                CreatableTopic::default()
                    .name(name.into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
            ])),
        )
        .await?;

    Ok(())
}

fn allow(resource_name: &str, principal: &str, operation: Operation) -> AclBinding {
    AclBinding {
        resource_type: Resource::Topic,
        resource_name: resource_name.into(),
        pattern: Pattern::Prefixed,
        principal: principal.into(),
        host: WILDCARD_HOST.into(),
        operation,
        permission: Permission::Allow,
    }
}

/// The error code every partition of `topic` came back with.
async fn produce(
    storage: &Arc<Box<dyn Storage>>,
    principal: &str,
    topic: &str,
) -> Result<Vec<i16>, Error> {
    let batch = deflated::Frame {
        batches: vec![
            inflated::Batch::builder()
                .record(Record::builder().value(Some(Bytes::from_static(b"m"))))
                .build()
                .and_then(deflated::Batch::try_from)?,
        ],
    };

    let response = ProduceService
        .serve(
            asking(storage, principal),
            ProduceRequest::default()
                .acks(-1)
                .timeout_ms(1_000)
                .topic_data(Some(vec![
                    TopicProduceData::default()
                        .name(topic.into())
                        .partition_data(Some(vec![
                            PartitionProduceData::default()
                                .index(0)
                                .records(Some(batch)),
                        ])),
                ])),
        )
        .await?;

    Ok(response
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| topic.partition_responses.unwrap_or_default())
        .map(|partition| partition.error_code)
        .collect())
}

async fn fetch(
    storage: &Arc<Box<dyn Storage>>,
    principal: &str,
    topic: &str,
) -> Result<Vec<i16>, Error> {
    let response = FetchService
        .serve(
            asking(storage, principal),
            FetchRequest::default()
                // Small but non-zero: the wait loop is a `while`, so a zero
                // window runs no iteration at all and answers with no topics —
                // which would make every assertion below vacuous rather than
                // fast.
                .max_wait_ms(100)
                .min_bytes(1)
                .max_bytes(Some(1024 * 1024))
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic.into()))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(0)
                                .fetch_offset(0)
                                .partition_max_bytes(1024 * 1024),
                        ])),
                ])),
        )
        .await?;

    Ok(response
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| topic.partitions.unwrap_or_default())
        .map(|partition| partition.error_code)
        .collect())
}

/// The headline of #363, as a test: one tenant's principal cannot touch
/// another tenant's topic, and nothing about the broker knows what a tenant is
/// — only that a rule carries a prefix.
#[tokio::test]
async fn a_prefix_rule_keeps_one_tenant_out_of_another() -> Result<(), Error> {
    let storage = storage().await?;

    create_topic(&storage, "tenant-a.orders").await?;
    create_topic(&storage, "tenant-b.orders").await?;

    // Before any rule, a fail-closed broker refuses everyone. This is the
    // assertion that would have failed on every previous build, in the other
    // direction: everything was allowed.
    assert_eq!(
        vec![i16::from(ErrorCode::TopicAuthorizationFailed)],
        produce(&storage, ALICE, "tenant-a.orders").await?,
        "no rule is not permission",
    );

    _ = storage
        .create_acls(&[
            allow("tenant-a.", ALICE, Operation::Write),
            allow("tenant-a.", ALICE, Operation::Read),
            allow("tenant-b.", BOB, Operation::Write),
            allow("tenant-b.", BOB, Operation::Read),
        ])
        .await?;

    assert_eq!(
        vec![i16::from(ErrorCode::None)],
        produce(&storage, ALICE, "tenant-a.orders").await?,
        "a principal must be able to write inside its own prefix",
    );

    assert_eq!(
        vec![i16::from(ErrorCode::TopicAuthorizationFailed)],
        produce(&storage, ALICE, "tenant-b.orders").await?,
        "and must not be able to write outside it",
    );

    assert_eq!(
        vec![i16::from(ErrorCode::None)],
        fetch(&storage, ALICE, "tenant-a.orders").await?,
    );

    assert_eq!(
        vec![i16::from(ErrorCode::TopicAuthorizationFailed)],
        fetch(&storage, ALICE, "tenant-b.orders").await?,
        "reading another tenant's topic is the breach this closes",
    );

    // And the other way round, so the rules are not accidentally symmetric in
    // whoever happens to be first.
    assert_eq!(
        vec![i16::from(ErrorCode::None)],
        fetch(&storage, BOB, "tenant-b.orders").await?,
    );

    assert_eq!(
        vec![i16::from(ErrorCode::TopicAuthorizationFailed)],
        fetch(&storage, BOB, "tenant-a.orders").await?,
    );

    Ok(())
}

/// A grant of `WRITE` does not confer `READ`.
///
/// The implication table runs one way — a grant implies `DESCRIBE`, nothing
/// else — and a table read too generously is how an ACL silently means more
/// than it says.
#[tokio::test]
async fn writing_does_not_confer_reading() -> Result<(), Error> {
    let storage = storage().await?;

    create_topic(&storage, "tenant-a.orders").await?;

    _ = storage
        .create_acls(&[allow("tenant-a.", ALICE, Operation::Write)])
        .await?;

    assert_eq!(
        vec![i16::from(ErrorCode::None)],
        produce(&storage, ALICE, "tenant-a.orders").await?,
    );

    assert_eq!(
        vec![i16::from(ErrorCode::TopicAuthorizationFailed)],
        fetch(&storage, ALICE, "tenant-a.orders").await?,
    );

    Ok(())
}

/// With no `Authorizer` in the context — a broker without `--authentication` —
/// nothing is authorized and nothing changes.
///
/// Every other test in this repository runs in that arrangement, so this pins
/// the property they all depend on rather than leaving it implied.
#[tokio::test]
async fn without_an_authorizer_nothing_is_refused() -> Result<(), Error> {
    let storage = storage().await?;

    create_topic(&storage, "tenant-a.orders").await?;

    // Deliberately hostile: a rule exists that would deny, and no authorizer
    // to consult it.
    _ = storage
        .create_acls(&[allow("tenant-a.", BOB, Operation::Write)])
        .await?;

    let response = ProduceService
        .serve(
            Context::default().map_state(|()| storage.clone()),
            ProduceRequest::default()
                .acks(-1)
                .timeout_ms(1_000)
                .topic_data(Some(vec![
                    TopicProduceData::default()
                        .name("tenant-a.orders".into())
                        .partition_data(Some(vec![
                            PartitionProduceData::default().index(0).records(Some(
                                deflated::Frame {
                                    batches: vec![
                                        inflated::Batch::builder()
                                            .record(
                                                Record::builder()
                                                    .value(Some(Bytes::from_static(b"m"))),
                                            )
                                            .build()
                                            .and_then(deflated::Batch::try_from)?,
                                    ],
                                },
                            )),
                        ])),
                ])),
        )
        .await?;

    assert_eq!(
        vec![i16::from(ErrorCode::None)],
        response
            .responses
            .unwrap_or_default()
            .into_iter()
            .flat_map(|topic| topic.partition_responses.unwrap_or_default())
            .map(|partition| partition.error_code)
            .collect::<Vec<_>>(),
    );

    Ok(())
}

/// A principal that can delete the ACLs can grant itself anything, so
/// enforcing everything else while leaving the ACL APIs open enforces nothing.
///
/// This was the last hole of consequence: after #375 a tenant could no longer
/// read another's *data*, but any authenticated principal could still wipe the
/// rules that said so.
#[tokio::test]
async fn only_a_cluster_admin_may_read_or_change_the_acls() -> Result<(), Error> {
    let storage = storage().await?;

    // Alice may read her own prefix, and nothing else — in particular, nothing
    // about the cluster.
    _ = storage
        .create_acls(&[allow("tenant-a.", ALICE, Operation::Read)])
        .await?;

    let creation = || {
        AclCreation::default()
            .resource_type(Resource::Topic.into())
            .resource_name("tenant-b.".into())
            .resource_pattern_type(Some(i8::from(Pattern::Prefixed)))
            .principal(ALICE.into())
            .host(WILDCARD_HOST.into())
            .operation(Operation::Read.into())
            .permission_type(Permission::Allow.into())
    };

    // Granting herself another tenant's prefix.
    let created = CreateAclsService
        .serve(
            asking(&storage, ALICE),
            CreateAclsRequest::default().creations(Some(vec![creation()])),
        )
        .await?;

    assert_eq!(
        vec![i16::from(ErrorCode::ClusterAuthorizationFailed)],
        created
            .results
            .unwrap_or_default()
            .into_iter()
            .map(|result| result.error_code)
            .collect::<Vec<_>>(),
    );

    // Reading the rules tells her what every other principal may do, which on
    // a mutualised fleet names the other tenants.
    let described = DescribeAclsService
        .serve(
            asking(&storage, ALICE),
            DescribeAclsRequest::default()
                .resource_type_filter(Resource::Any.into())
                .resource_name_filter(None)
                .pattern_type_filter(Some(i8::from(Pattern::Any)))
                .principal_filter(None)
                .host_filter(None)
                .operation(Operation::Any.into())
                .permission_type(Permission::Any.into()),
        )
        .await?;

    assert_eq!(
        i16::from(ErrorCode::ClusterAuthorizationFailed),
        described.error_code,
    );
    assert!(described.resources.unwrap_or_default().is_empty());

    // And wiping them, which is the one that would undo everything else.
    let deleted = DeleteAclsService
        .serve(
            asking(&storage, ALICE),
            DeleteAclsRequest::default().filters(Some(vec![
                DeleteAclsFilter::default()
                    .resource_type_filter(Resource::Any.into())
                    .resource_name_filter(None)
                    .pattern_type_filter(Some(i8::from(Pattern::Any)))
                    .principal_filter(None)
                    .host_filter(None)
                    .operation(Operation::Any.into())
                    .permission_type(Permission::Any.into()),
            ])),
        )
        .await?;

    assert_eq!(
        vec![i16::from(ErrorCode::ClusterAuthorizationFailed)],
        deleted
            .filter_results
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|result| result.error_code)
            .collect::<Vec<_>>(),
    );

    assert!(
        deleted
            .filter_results
            .unwrap_or_default()
            .into_iter()
            .all(|result| result.matching_acls.unwrap_or_default().is_empty()),
        "a refused delete must not report what it would have removed",
    );

    // Her own rule is still there.
    assert_eq!(1, storage.describe_acls(&AclFilter::any()).await?.len());

    // An `ALTER` on the cluster is what makes an operator an operator.
    _ = storage
        .create_acls(&[AclBinding {
            resource_type: Resource::Cluster,
            resource_name: CLUSTER_RESOURCE.into(),
            pattern: Pattern::Literal,
            principal: BOB.into(),
            host: WILDCARD_HOST.into(),
            operation: Operation::Alter,
            permission: Permission::Allow,
        }])
        .await?;

    let created = CreateAclsService
        .serve(
            asking(&storage, BOB),
            CreateAclsRequest::default().creations(Some(vec![creation()])),
        )
        .await?;

    assert_eq!(
        vec![i16::from(ErrorCode::None)],
        created
            .results
            .unwrap_or_default()
            .into_iter()
            .map(|result| result.error_code)
            .collect::<Vec<_>>(),
    );

    Ok(())
}

/// Deleting a topic is refused per topic, so one refusal does not fail the
/// rest of the request.
#[tokio::test]
async fn deleting_a_topic_needs_a_rule_on_that_topic() -> Result<(), Error> {
    let storage = storage().await?;

    create_topic(&storage, "tenant-a.orders").await?;
    create_topic(&storage, "tenant-b.orders").await?;

    _ = storage
        .create_acls(&[allow("tenant-a.", ALICE, Operation::Delete)])
        .await?;

    let response = DeleteTopicsService
        .serve(
            asking(&storage, ALICE),
            DeleteTopicsRequest::default().topics(Some(vec![
                DeleteTopicState::default()
                    .name(Some("tenant-a.orders".into()))
                    .topic_id(NULL_TOPIC_ID),
                DeleteTopicState::default()
                    .name(Some("tenant-b.orders".into()))
                    .topic_id(NULL_TOPIC_ID),
            ])),
        )
        .await?;

    assert_eq!(
        vec![
            (
                Some("tenant-a.orders".to_owned()),
                i16::from(ErrorCode::None)
            ),
            (
                Some("tenant-b.orders".to_owned()),
                i16::from(ErrorCode::TopicAuthorizationFailed)
            ),
        ],
        response
            .responses
            .unwrap_or_default()
            .into_iter()
            .map(|result| (result.name, result.error_code))
            .collect::<Vec<_>>(),
        "a refusal on one topic must not fail the other",
    );

    Ok(())
}

/// Creating a topic is a cluster operation, because the topic does not exist
/// yet and there is nothing for a topic rule to select.
#[tokio::test]
async fn creating_a_topic_needs_the_cluster() -> Result<(), Error> {
    let storage = storage().await?;

    // A prefix grant is not enough, and that is the point: a rule on a name
    // nobody has taken yet would be a rule on the whole namespace.
    _ = storage
        .create_acls(&[allow("tenant-a.", ALICE, Operation::Create)])
        .await?;

    let response = CreateTopicsService
        .serve(
            asking(&storage, ALICE),
            CreateTopicsRequest::default().topics(Some(vec![
                CreatableTopic::default()
                    .name("tenant-a.new".into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
            ])),
        )
        .await?;

    assert_eq!(
        vec![i16::from(ErrorCode::ClusterAuthorizationFailed)],
        response
            .topics
            .unwrap_or_default()
            .into_iter()
            .map(|topic| topic.error_code)
            .collect::<Vec<_>>(),
    );

    Ok(())
}
