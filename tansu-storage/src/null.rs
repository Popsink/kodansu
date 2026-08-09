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

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use tansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, ScramMechanism,
    add_partitions_to_txn_response::{AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult},
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::deflated::Batch,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tracing::instrument;
use url::Url;
use uuid::Uuid;

use crate::{
    AclBinding, AclFilter, Acls, AssignmentDoc, AssignmentOutcome, BrokerRegistrationRequest,
    Error, GenerationDoc, GroupDetailResponse, ListOffsetResponse, MemberDoc, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result,
    ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version,
};

/// A stored document with the version identifying it, as the object store
/// hands the pair back.
type Held<T> = (T, Version);

/// Member documents keyed `(group, member)`, as their object keys are.
type MemberDocs = BTreeMap<(String, String), Held<MemberDoc>>;

/// Generation documents keyed by group.
type GenerationDocs = BTreeMap<String, Held<GenerationDoc>>;

/// Assignment documents keyed `(group, generation)`. No version: they are
/// immutable, so there is nothing to condition a later write on.
type AssignmentDocs = BTreeMap<(String, i32), AssignmentDoc>;

#[derive(Clone, Debug)]
pub(crate) struct Engine {
    cluster: String,
    node: i32,
    advertised_listener: Url,

    topics: Arc<Mutex<Vec<CreatableTopic>>>,

    /// The decomposed group layout (#359), keyed as the object store keys it:
    /// `(group, member)`, `group`, `(group, generation)`.
    acls: Arc<Mutex<Acls>>,
    members: Arc<Mutex<MemberDocs>>,
    generations: Arc<Mutex<GenerationDocs>>,
    assignments: Arc<Mutex<AssignmentDocs>>,
}

impl Engine {
    pub(crate) fn new(cluster: String, node: i32, advertised_listener: Url) -> Self {
        Self {
            cluster,
            node,
            advertised_listener,
            topics: Arc::new(Mutex::new(Vec::new())),
            acls: Arc::new(Mutex::new(Acls::default())),
            members: Arc::new(Mutex::new(BTreeMap::new())),
            generations: Arc::new(Mutex::new(BTreeMap::new())),
            assignments: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// A version distinct from every other, as an object store's etag is.
    fn next_version() -> Version {
        let id = Uuid::now_v7();

        Version {
            e_tag: Some(id.to_string()),
            version: Some(id.to_string()),
        }
    }
}

const FEATURE: &str = "storage";
const MESSAGE: &str = "storage has not been defined";

#[async_trait]
impl Storage for Engine {
    #[instrument(skip_all)]
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        Ok(())
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        self.topics
            .lock()
            .map_err(Into::into)
            .and_then(|mut topics| {
                if topics.iter().any(|existing| existing.name == topic.name) {
                    Err(Error::Api(ErrorCode::TopicAlreadyExists))
                } else {
                    topics.push(topic);
                    Ok(Uuid::now_v7())
                }
            })
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        _topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        Ok(AlterConfigsResourceResponse::default()
            .error_code(ErrorCode::None.into())
            .error_message(Some(ErrorCode::None.to_string()))
            .resource_name(resource.resource_name)
            .resource_type(resource.resource_type))
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        _transaction_id: Option<&str>,
        _topition: &Topition,
        _deflated: Batch,
    ) -> Result<i64> {
        Ok(6)
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        _topition: &Topition,
        _offset: i64,
        _min_bytes: u32,
        _max_bytes: u32,
        _isolation_level: IsolationLevel,
        _max_wait: Duration,
    ) -> Result<Vec<Batch>> {
        Ok([].into())
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
        Ok(OffsetStage::default())
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        _group: &str,
        _retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        Ok(offsets
            .iter()
            .map(|(topition, _)| (topition.to_owned(), ErrorCode::None))
            .collect())
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(&self, _group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        Ok(BTreeMap::new())
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        _group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        Ok(topics
            .iter()
            .map(|topition| (topition.to_owned(), 0))
            .collect())
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        _isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        Ok(offsets
            .iter()
            .map(|(topition, _)| {
                (
                    topition.to_owned(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        timestamp: None,
                        offset: Some(0),
                    },
                )
            })
            .collect())
    }

    #[instrument(skip_all)]
    async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let node_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        self.topics
            .lock()
            .map(|topics| {
                topics
                    .iter()
                    .map(|topic| {
                        MetadataResponseTopic::default()
                            .error_code(ErrorCode::None.into())
                            .is_internal(Some(false))
                            .name(Some(topic.name.clone()))
                            .partitions(Some(
                                (0..topic.num_partitions)
                                    .map(|partition_index| {
                                        MetadataResponsePartition::default()
                                            .leader_id(self.node)
                                            .leader_epoch(Some(-1))
                                            .partition_index(partition_index)
                                            .error_code(ErrorCode::None.into())
                                            .offline_replicas(Some([].into()))
                                            .replica_nodes(Some(vec![
                                                self.node;
                                                topic.replication_factor
                                                    as usize
                                            ]))
                                            .isr_nodes(Some(vec![
                                                self.node;
                                                topic.replication_factor as usize
                                            ]))
                                    })
                                    .collect(),
                            ))
                            .topic_id(Some(NULL_TOPIC_ID))
                            .topic_authorized_operations(Some(i32::MIN))
                    })
                    .collect()
            })
            .map(|topics| MetadataResponse {
                cluster: Some(self.cluster.clone()),
                controller: Some(self.node),
                brokers: [MetadataResponseBroker::default()
                    .node_id(node_id)
                    .host(host)
                    .port(port)
                    .rack(rack)]
                .into(),
                topics,
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        Ok(DescribeConfigsResult::default()
            .configs(Some([].into()))
            .resource_name(name.to_string())
            .resource_type(resource.into())
            .error_code(ErrorCode::None.into())
            .error_message(Some(ErrorCode::None.to_string())))
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        _topics: Option<&[TopicId]>,
        _partition_limit: i32,
        _cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.topics.lock().map_err(Into::into).map(|existing| {
            existing
                .iter()
                .map(|existing| {
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::None.into())
                        .name(Some(existing.name.clone()))
                        .partitions(Some(
                            (0..existing.num_partitions)
                                .map(|partition_index| {
                                    DescribeTopicPartitionsResponsePartition::default()
                                        .leader_id(self.node)
                                        .partition_index(partition_index)
                                        .isr_nodes(Some(vec![
                                            self.node;
                                            existing.replication_factor as usize
                                        ]))
                                })
                                .collect(),
                        ))
                })
                .collect()
        })
    }

    #[instrument(skip_all)]
    async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        Ok([].into())
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        Ok(group_ids
            .unwrap_or_default()
            .iter()
            .map(|group_id| {
                DeletableGroupResult::default()
                    .error_code(ErrorCode::None.into())
                    .group_id(group_id.to_owned())
            })
            .collect())
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        Ok(group_ids
            .unwrap_or_default()
            .iter()
            .map(|name| NamedGroupDetail {
                name: name.to_owned(),
                response: GroupDetailResponse::ErrorCode(ErrorCode::GroupIdNotFound),
            })
            .collect())
    }

    #[instrument(skip_all)]
    async fn write_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        member: MemberDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<MemberDoc>> {
        let key = (group_id.to_owned(), member_id.to_owned());

        self.members
            .lock()
            .map_err(|err| UpdateError::Error(err.into()))
            .and_then(|mut members| match members.get(&key) {
                Some((current, held)) if Some(held) != version.as_ref() => {
                    Err(UpdateError::Outdated {
                        current: Box::new(current.clone()),
                        version: held.clone(),
                    })
                }

                // An etag CAS against an object that is not there: the store
                // reports a failed precondition, and there is no `current` to
                // hand back, so it cannot be an `Outdated`.
                None if version.is_some() => {
                    Err(UpdateError::Error(Error::Api(ErrorCode::UnknownMemberId)))
                }

                _ => {
                    let version = Self::next_version();
                    _ = members.insert(key, (member, version.clone()));
                    Ok(version)
                }
            })
    }

    #[instrument(skip_all)]
    async fn read_group_member(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> Result<Option<(MemberDoc, Version)>> {
        let key = (group_id.to_owned(), member_id.to_owned());

        self.members
            .lock()
            .map_err(Into::into)
            .map(|members| members.get(&key).cloned())
    }

    #[instrument(skip_all)]
    async fn delete_group_member(&self, group_id: &str, member_id: &str) -> Result<()> {
        let key = (group_id.to_owned(), member_id.to_owned());

        self.members.lock().map_err(Into::into).map(|mut members| {
            _ = members.remove(&key);
        })
    }

    #[instrument(skip_all)]
    async fn list_group_members(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<String, (MemberDoc, Version)>> {
        self.members.lock().map_err(Into::into).map(|members| {
            members
                .iter()
                .filter(|((group, _), _)| group == group_id)
                .map(|((_, member_id), held)| (member_id.clone(), held.clone()))
                .collect()
        })
    }

    /// In memory for the life of the process, like everything else this engine
    /// keeps.
    #[instrument(skip_all)]
    async fn create_acls(&self, bindings: &[AclBinding]) -> Result<Vec<ErrorCode>> {
        self.acls.lock().map_err(Into::into).map(|mut acls| {
            for binding in bindings {
                _ = acls.bindings.insert(binding.clone());
            }

            vec![ErrorCode::None; bindings.len()]
        })
    }

    #[instrument(skip_all)]
    async fn describe_acls(&self, filter: &AclFilter) -> Result<Vec<AclBinding>> {
        self.acls
            .lock()
            .map_err(Into::into)
            .map(|acls| acls.matching(filter).cloned().collect())
    }

    #[instrument(skip_all)]
    async fn delete_acls(&self, filters: &[AclFilter]) -> Result<Vec<Vec<AclBinding>>> {
        self.acls.lock().map_err(Into::into).map(|mut acls| {
            filters
                .iter()
                .map(|filter| {
                    let selected = acls.matching(filter).cloned().collect::<Vec<_>>();

                    for binding in &selected {
                        _ = acls.bindings.remove(binding);
                    }

                    selected
                })
                .collect()
        })
    }

    /// The Null engine holds every group in memory for the life of the
    /// process, so there is no cluster to have written a layout and nothing to
    /// disagree with.
    #[instrument(skip_all)]
    async fn assert_group_schema(&self) -> Result<()> {
        Ok(())
    }

    #[instrument(skip_all)]
    async fn read_group_generation(
        &self,
        group_id: &str,
    ) -> Result<Option<(GenerationDoc, Version)>> {
        self.generations
            .lock()
            .map_err(Into::into)
            .map(|generations| generations.get(group_id).cloned())
    }

    #[instrument(skip_all)]
    async fn update_group_generation(
        &self,
        group_id: &str,
        generation: GenerationDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GenerationDoc>> {
        self.generations
            .lock()
            .map_err(|err| UpdateError::Error(err.into()))
            .and_then(|mut generations| match generations.get(group_id) {
                Some((current, held)) if Some(held) != version.as_ref() => {
                    Err(UpdateError::Outdated {
                        current: Box::new(current.clone()),
                        version: held.clone(),
                    })
                }

                None if version.is_some() => {
                    Err(UpdateError::Error(Error::Api(ErrorCode::GroupIdNotFound)))
                }

                _ => {
                    let version = Self::next_version();
                    _ = generations.insert(group_id.to_owned(), (generation, version.clone()));
                    Ok(version)
                }
            })
    }

    #[instrument(skip_all)]
    async fn create_group_assignment(
        &self,
        group_id: &str,
        generation_id: i32,
        assignment: AssignmentDoc,
    ) -> Result<AssignmentOutcome> {
        let key = (group_id.to_owned(), generation_id);

        self.assignments
            .lock()
            .map_err(Into::into)
            .map(|mut assignments| match assignments.get(&key) {
                Some(current) => AssignmentOutcome::AlreadyExists(Box::new(current.clone())),

                None => {
                    let version = Self::next_version();
                    _ = assignments.insert(key, assignment);
                    AssignmentOutcome::Created(version)
                }
            })
    }

    #[instrument(skip_all)]
    async fn read_group_assignment(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Result<Option<AssignmentDoc>> {
        let key = (group_id.to_owned(), generation_id);

        self.assignments
            .lock()
            .map_err(Into::into)
            .map(|assignments| assignments.get(&key).cloned())
    }

    #[instrument(skip_all)]
    async fn delete_group_assignments_before(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Result<u64> {
        self.assignments
            .lock()
            .map_err(Into::into)
            .map(|mut assignments| {
                let held = assignments.len();

                assignments.retain(|(group, generation), _| {
                    group != group_id || *generation >= generation_id
                });

                (held - assignments.len()) as u64
            })
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        _transaction_id: Option<&str>,
        _transaction_timeout_ms: i32,
        _producer_id: Option<i64>,
        _producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        Ok(ProducerIdResponse {
            error: ErrorCode::None,
            id: 6,
            epoch: 6,
        })
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        Ok(match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree { topics, .. } => {
                TxnAddPartitionsResponse::VersionZeroToThree(
                    topics
                        .iter()
                        .map(|topic| {
                            AddPartitionsToTxnTopicResult::default()
                                .name(topic.name.clone())
                                .results_by_partition(Some([].into()))
                        })
                        .collect(),
                )
            }
            TxnAddPartitionsRequest::VersionFourPlus { transactions } => {
                TxnAddPartitionsResponse::VersionFourPlus(
                    transactions
                        .iter()
                        .map(|_| AddPartitionsToTxnResult::default())
                        .collect(),
                )
            }
        })
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        Ok(offsets
            .topics
            .iter()
            .map(|topic| TxnOffsetCommitResponseTopic::default().name(topic.name.clone()))
            .collect())
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _committed: bool,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    #[instrument(skip_all)]
    async fn maintain(&self, _now: SystemTime) -> Result<()> {
        Ok(())
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    #[instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        Ok(())
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }

    #[instrument(ret)]
    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Err(Error::FeatureNotEnabled {
            feature: FEATURE.into(),
            message: MESSAGE.into(),
        })
    }
}
