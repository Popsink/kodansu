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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tansu_sans_io::add_partitions_to_txn_response::AddPartitionsToTxnResult;
use tansu_sans_io::add_partitions_to_txn_response::AddPartitionsToTxnTopicResult;
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::delete_groups_response::DeletableGroupResult;
use tansu_sans_io::delete_records_request::DeleteRecordsTopic;
use tansu_sans_io::delete_records_response::DeleteRecordsTopicResult;
use tansu_sans_io::describe_cluster_response::DescribeClusterBroker;
use tansu_sans_io::describe_configs_response::DescribeConfigsResult;
use tansu_sans_io::describe_topic_partitions_response::DescribeTopicPartitionsResponsePartition;
use tansu_sans_io::describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic;
use tansu_sans_io::incremental_alter_configs_request::AlterConfigsResource;
use tansu_sans_io::incremental_alter_configs_response::AlterConfigsResourceResponse;
use tansu_sans_io::list_groups_response::ListedGroup;
use tansu_sans_io::metadata_response::MetadataResponseBroker;
use tansu_sans_io::metadata_response::{MetadataResponsePartition, MetadataResponseTopic};
use tansu_sans_io::record::deflated::Batch;
use tansu_sans_io::txn_offset_commit_response::TxnOffsetCommitResponseTopic;
use tansu_sans_io::{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse};
use tansu_sans_io::{ConfigResource, ErrorCode, IsolationLevel, ListOffset};
use tansu_sans_io::{NULL_TOPIC_ID, ScramMechanism};
use url::Url;
use uuid::Uuid;

use crate::{
    Ack, AclBinding, AclFilter, Acls, AssignmentDoc, AssignmentOutcome, AutoTopicCreate,
    BrokerRegistrationRequest, CommittedOffset, ConsumerGroupDoc, ContactStamps,
    DEFAULT_FETCH_MAX_BYTES, Error, GenerationDoc, GroupDetailResponse, ListOffsetResponse,
    MemberDoc, MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage,
    ProducerIdResponse, QuotaAlteration, QuotaEntity, QuotaFilterComponent, QuotaLimits, Quotas,
    Result, ScramCredential, Storage, TopicId, Topition, TxnOffsetCommitRequest, UpdateError,
    Version,
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

/// KIP-848 consumer group documents keyed by group (#612).
type ConsumerGroupDocs = BTreeMap<String, Held<ConsumerGroupDoc>>;

/// KIP-848 liveness stamps keyed by group (#627).
type ConsumerStamps = BTreeMap<String, ContactStamps>;

#[derive(Clone, Debug)]
pub(crate) struct Engine {
    cluster: String,
    node: i32,
    advertised_listener: Url,

    topics: Arc<Mutex<Vec<CreatableTopic>>>,

    /// The decomposed group layout (#359), keyed as the object store keys it:
    /// `(group, member)`, `group`, `(group, generation)`.
    acls: Arc<Mutex<Acls>>,
    quotas: Arc<Mutex<Quotas>>,
    members: Arc<Mutex<MemberDocs>>,
    generations: Arc<Mutex<GenerationDocs>>,
    assignments: Arc<Mutex<AssignmentDocs>>,
    consumer_groups: Arc<Mutex<ConsumerGroupDocs>>,
    consumer_stamps: Arc<Mutex<ConsumerStamps>>,
}

impl Engine {
    pub(crate) fn new(cluster: String, node: i32, advertised_listener: Url) -> Self {
        Self {
            cluster,
            node,
            advertised_listener,
            topics: Arc::new(Mutex::new(Vec::new())),
            acls: Arc::new(Mutex::new(Acls::default())),
            quotas: Arc::new(Mutex::new(Quotas::default())),
            members: Arc::new(Mutex::new(BTreeMap::new())),
            generations: Arc::new(Mutex::new(BTreeMap::new())),
            assignments: Arc::new(Mutex::new(BTreeMap::new())),
            consumer_groups: Arc::new(Mutex::new(BTreeMap::new())),
            consumer_stamps: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// This node's advertised host and port, `0.0.0.0` and 9092 where the
    /// listener leaves either out.
    fn host_and_port(&self) -> (String, i32) {
        let host = self.advertised_listener.host_str().unwrap_or("0.0.0.0");
        let port = self.advertised_listener.port().unwrap_or(9092);
        (host.to_owned(), i32::from(port))
    }

    /// This node's id, once per replica: the only replica there is.
    fn replicas(&self, factor: i16) -> Vec<i32> {
        vec![self.node; factor as usize]
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

/// The answer of the methods that cannot work without storage.
fn undefined<T>() -> Result<T> {
    let (feature, message) = (String::from(FEATURE), String::from(MESSAGE));
    Err(Error::FeatureNotEnabled { feature, message })
}

#[async_trait::async_trait]
impl Storage for Engine {
    #[tracing::instrument(skip_all)]
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let (host, port) = self.host_and_port();
        let this_node = DescribeClusterBroker::default()
            .broker_id(self.node)
            .host(host);
        Ok(vec![this_node.port(port).rack(None)])
    }

    #[tracing::instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        let mut created = self.topics.lock()?;
        if created.iter().any(|existing| existing.name == topic.name) {
            return Err(Error::Api(ErrorCode::TopicAlreadyExists));
        }
        created.push(topic);
        Ok(Uuid::now_v7())
    }

    #[tracing::instrument(skip_all)]
    async fn delete_records(
        &self,
        _topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        undefined()
    }

    #[tracing::instrument(skip_all)]
    async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    #[tracing::instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let none = ErrorCode::None;
        let altered = AlterConfigsResourceResponse::default().error_code(i16::from(none));
        let altered = altered.error_message(Some(none.to_string()));
        let altered = altered.resource_name(resource.resource_name);
        Ok(altered.resource_type(resource.resource_type))
    }

    #[tracing::instrument(skip_all)]
    async fn produce(
        &self,
        _transaction_id: Option<&str>,
        _topition: &Topition,
        _deflated: Batch,
    ) -> Result<i64> {
        Ok(6)
    }

    /// Nothing to wait for, so nothing to pipeline: the [`Ack`] is already
    /// resolved, with the offset [`Self::produce`] answers.
    #[tracing::instrument(skip_all)]
    async fn admit(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: Batch,
    ) -> Result<Ack> {
        let produced = self.produce(transaction_id, topition, deflated).await;

        Ok(Box::pin(std::future::ready(produced)))
    }

    #[tracing::instrument(skip_all)]
    async fn fetch(
        &self,
        _topition: &Topition,
        _offset: i64,
        _min_bytes: u32,
        _max_bytes: u32,
        _isolation_level: IsolationLevel,
        _max_wait: Duration,
    ) -> Result<Vec<Batch>> {
        Ok(Vec::new())
    }

    #[tracing::instrument(skip_all)]
    async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
        Ok(OffsetStage::default())
    }

    /// The three below are stated rather than inherited: #551 removed their
    /// trait defaults, because a default is a method a wrapper can forget
    /// without the compiler noticing (#273). The engine that stores nothing
    /// takes the same answers the defaults gave, and now says so.
    fn auto_create_topic_config(&self) -> AutoTopicCreate {
        AutoTopicCreate::default()
    }

    fn fetch_max_bytes(&self) -> u32 {
        DEFAULT_FETCH_MAX_BYTES
    }

    async fn offset_stage_at(
        &self,
        topition: &Topition,
        _isolation: IsolationLevel,
    ) -> Result<OffsetStage> {
        self.offset_stage(topition).await
    }

    async fn offset_stage_refreshed(&self, topition: &Topition) -> Result<OffsetStage> {
        self.offset_stage(topition).await
    }

    #[tracing::instrument(skip_all)]
    async fn offset_commit(
        &self,
        _group: &str,
        _retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let accepted = |(at, _): &(Topition, OffsetCommitRequest)| (at.clone(), ErrorCode::None);
        Ok(offsets.iter().map(accepted).collect())
    }

    #[tracing::instrument(skip_all)]
    async fn committed_offset_topitions(
        &self,
        _group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        Ok(BTreeMap::new())
    }

    #[tracing::instrument(skip_all)]
    async fn offset_fetch(
        &self,
        _group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let at_zero = |at: &Topition| (at.clone(), CommittedOffset::new(0, None));
        Ok(topics.iter().map(at_zero).collect())
    }

    #[tracing::instrument(skip_all)]
    async fn list_offsets(
        &self,
        _isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let start = ListOffsetResponse {
            offset: Some(0),
            ..ListOffsetResponse::default()
        };
        let listed = |(at, _): &(Topition, ListOffset)| (at.clone(), start);
        Ok(offsets.iter().map(listed).collect())
    }

    #[tracing::instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let asks_for_none = topics.is_some_and(<[TopicId]>::is_empty);
        let (host, port) = self.host_and_port();
        let created = self.topics.lock()?;

        let partition = |index, factor| {
            let led = MetadataResponsePartition::default().leader_id(self.node);
            let led = led.leader_epoch(Some(-1)).partition_index(index);
            let led = led
                .error_code(i16::from(ErrorCode::None))
                .offline_replicas(Some(vec![]));
            let led = led.replica_nodes(Some(self.replicas(factor)));
            led.isr_nodes(Some(self.replicas(factor)))
        };
        let describe = |topic: &CreatableTopic| {
            let indexes = 0..topic.num_partitions;
            let partitions = indexes.map(|index| partition(index, topic.replication_factor));
            let named = MetadataResponseTopic::default().error_code(i16::from(ErrorCode::None));
            let named = named
                .is_internal(Some(false))
                .name(Some(topic.name.clone()));
            let named = named
                .partitions(Some(partitions.collect()))
                .topic_id(Some(NULL_TOPIC_ID));
            named.topic_authorized_operations(Some(i32::MIN))
        };
        let listed = created
            .iter()
            .filter(|_| !asks_for_none)
            .map(describe)
            .collect();

        let this_node = MetadataResponseBroker::default()
            .node_id(self.node)
            .host(host);
        let brokers = vec![this_node.port(port).rack(None)];
        let (cluster, controller) = (Some(self.cluster.clone()), Some(self.node));
        Ok(MetadataResponse {
            topics: listed,
            brokers,
            controller,
            cluster,
        })
    }

    #[tracing::instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let none = ErrorCode::None;
        let described = DescribeConfigsResult::default().configs(Some(vec![]));
        let described = described
            .resource_name(name.to_owned())
            .resource_type(i8::from(resource));
        Ok(described
            .error_code(i16::from(none))
            .error_message(Some(none.to_string())))
    }

    #[tracing::instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        _topics: Option<&[TopicId]>,
        _partition_limit: i32,
        _cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let created = self.topics.lock()?;
        let describe = |topic: &CreatableTopic| {
            let in_sync = self.replicas(topic.replication_factor);
            let partition = |index| {
                let led = DescribeTopicPartitionsResponsePartition::default().leader_id(self.node);
                led.partition_index(index).isr_nodes(Some(in_sync.clone()))
            };
            let partitions = (0..topic.num_partitions).map(partition).collect();
            let named = DescribeTopicPartitionsResponseTopic::default();
            let named = named.error_code(i16::from(ErrorCode::None));
            named
                .name(Some(topic.name.clone()))
                .partitions(Some(partitions))
        };
        Ok(created.iter().map(describe).collect())
    }

    #[tracing::instrument(skip_all)]
    async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        Ok(Vec::new())
    }

    #[tracing::instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let deleted = |group: &String| {
            let result = DeletableGroupResult::default().error_code(i16::from(ErrorCode::None));
            result.group_id(group.clone())
        };
        Ok(group_ids.unwrap_or_default().iter().map(deleted).collect())
    }

    #[tracing::instrument(skip_all)]
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

    #[tracing::instrument(skip_all)]
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

    #[tracing::instrument(skip_all)]
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

    #[tracing::instrument(skip_all)]
    async fn delete_group_member(&self, group_id: &str, member_id: &str) -> Result<()> {
        let key = (group_id.to_owned(), member_id.to_owned());

        self.members.lock().map_err(Into::into).map(|mut members| {
            _ = members.remove(&key);
        })
    }

    #[tracing::instrument(skip_all)]
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

    async fn list_group_member_stamps(&self, group_id: &str) -> Result<BTreeMap<String, i64>> {
        // The document's own stamp, where the object store answers with the
        // object's `last_modified`. They are the same reading taken by two
        // clocks (#427), and this engine holds documents rather than objects.
        self.members.lock().map_err(Into::into).map(|members| {
            members
                .iter()
                .filter(|((group, _), _)| group == group_id)
                .map(|((_, member_id), (member, _))| (member_id.clone(), member.last_contact_ms))
                .collect()
        })
    }

    /// In memory for the life of the process, like everything else this engine
    /// keeps.
    #[tracing::instrument(skip_all)]
    async fn create_acls(&self, bindings: &[AclBinding]) -> Result<Vec<ErrorCode>> {
        self.acls.lock().map_err(Into::into).map(|mut acls| {
            for binding in bindings {
                _ = acls.bindings.insert(binding.clone());
            }

            vec![ErrorCode::None; bindings.len()]
        })
    }

    #[tracing::instrument(skip_all)]
    async fn describe_acls(&self, filter: &AclFilter) -> Result<Vec<AclBinding>> {
        self.acls
            .lock()
            .map_err(Into::into)
            .map(|acls| acls.matching(filter).cloned().collect())
    }

    #[tracing::instrument(skip_all)]
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

    #[tracing::instrument(skip_all)]
    async fn alter_client_quotas(
        &self,
        alterations: &[QuotaAlteration],
        validate_only: bool,
    ) -> Result<Vec<ErrorCode>> {
        self.quotas.lock().map_err(Into::into).map(|mut quotas| {
            // Validated against a copy, so that `validate_only` and a refused
            // key both leave the document as they found it.
            let mut proposed = quotas.clone();

            let outcomes = alterations
                .iter()
                .map(|alteration| match proposed.alter(alteration) {
                    Ok(()) => ErrorCode::None,
                    Err(_) => ErrorCode::InvalidConfig,
                })
                .collect::<Vec<_>>();

            if !validate_only {
                *quotas = proposed;
            }

            outcomes
        })
    }

    #[tracing::instrument(skip_all)]
    async fn describe_client_quotas(
        &self,
        components: &[QuotaFilterComponent],
        strict: bool,
    ) -> Result<Vec<(QuotaEntity, QuotaLimits)>> {
        self.quotas
            .lock()
            .map_err(Into::into)
            .map(|quotas| quotas.matching(components, strict))
    }

    #[tracing::instrument(skip_all)]
    async fn client_quotas(&self) -> Result<Quotas> {
        self.quotas.lock().map_err(Into::into).map(|quotas| {
            let quotas: &Quotas = &quotas;
            quotas.clone()
        })
    }

    /// The Null engine holds every group in memory for the life of the
    /// process, so there is no cluster to have written a layout and nothing to
    /// disagree with.
    #[tracing::instrument(skip_all)]
    async fn assert_group_schema(&self) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn read_group_generation(
        &self,
        group_id: &str,
    ) -> Result<Option<(GenerationDoc, Version)>> {
        self.generations
            .lock()
            .map_err(Into::into)
            .map(|generations| generations.get(group_id).cloned())
    }

    #[tracing::instrument(skip_all)]
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

    #[tracing::instrument(skip_all)]
    async fn read_consumer_group(
        &self,
        group_id: &str,
    ) -> Result<Option<(ConsumerGroupDoc, Version)>> {
        self.consumer_groups
            .lock()
            .map_err(Into::into)
            .map(|groups| groups.get(group_id).cloned())
    }

    #[tracing::instrument(skip_all)]
    async fn renew_consumer_member(
        &self,
        group_id: &str,
        member_id: &str,
        last_contact_ms: i64,
    ) -> Result<()> {
        self.consumer_stamps
            .lock()
            .map_err(Into::into)
            .map(|mut stamps| {
                _ = stamps
                    .entry(group_id.to_owned())
                    .or_default()
                    .entry(member_id.to_owned())
                    .or_default()
                    .insert(last_contact_ms);
            })
    }

    #[tracing::instrument(skip_all)]
    async fn list_consumer_member_stamps(&self, group_id: &str) -> Result<ContactStamps> {
        self.consumer_stamps
            .lock()
            .map_err(Into::into)
            .map(|stamps| stamps.get(group_id).cloned().unwrap_or_default())
    }

    #[tracing::instrument(skip_all)]
    async fn delete_consumer_member_stamps(
        &self,
        group_id: &str,
        stamps: &[(String, i64)],
    ) -> Result<()> {
        self.consumer_stamps
            .lock()
            .map_err(Into::into)
            .map(|mut held| {
                if let Some(group) = held.get_mut(group_id) {
                    for (member_id, stamp) in stamps {
                        if let Some(member) = group.get_mut(member_id) {
                            _ = member.remove(stamp);
                        }
                    }

                    group.retain(|_, member| !member.is_empty());
                }
            })
    }

    #[tracing::instrument(skip_all)]
    async fn update_consumer_group(
        &self,
        group_id: &str,
        group: ConsumerGroupDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<ConsumerGroupDoc>> {
        self.consumer_groups
            .lock()
            .map_err(|err| UpdateError::Error(err.into()))
            .and_then(|mut groups| match groups.get(group_id) {
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
                    _ = groups.insert(group_id.to_owned(), (group, version.clone()));
                    Ok(version)
                }
            })
    }

    #[tracing::instrument(skip_all)]
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

    #[tracing::instrument(skip_all)]
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

    #[tracing::instrument(skip_all)]
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

    #[tracing::instrument(skip_all)]
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

    #[tracing::instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    #[tracing::instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: AddPartitionsToTxnRequest,
    ) -> Result<AddPartitionsToTxnResponse> {
        match partitions {
            AddPartitionsToTxnRequest {
                transactions: None,
                v_3_and_below_transactional_id: Some(_),
                v_3_and_below_producer_id: Some(_),
                v_3_and_below_producer_epoch: Some(_),
                v_3_and_below_topics: Some(topics),
                ..
            } => {
                let answered = topics.iter().map(|topic| {
                    AddPartitionsToTxnTopicResult::default()
                        .name(topic.name.clone())
                        .results_by_partition(Some(Vec::new()))
                });
                Ok(crate::added_by_topic(answered.collect()))
            }
            AddPartitionsToTxnRequest {
                transactions: Some(transactions),
                v_3_and_below_transactional_id: None,
                v_3_and_below_producer_id: None,
                v_3_and_below_producer_epoch: None,
                v_3_and_below_topics: None,
                ..
            } => {
                let answered = transactions
                    .iter()
                    .map(|_| AddPartitionsToTxnResult::default());
                Ok(crate::added_by_transaction(answered.collect()))
            }
            unexpected => {
                let boxed = Box::new(unexpected);
                Err(Error::UnexpectedAddPartitionsToTxnRequest(boxed))
            }
        }
    }

    #[tracing::instrument(skip_all)]
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

    #[tracing::instrument(skip_all)]
    async fn txn_end(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _committed: bool,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    #[tracing::instrument(skip_all)]
    async fn maintain(&self, _now: SystemTime) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    #[tracing::instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    #[tracing::instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    #[tracing::instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        undefined()
    }

    #[tracing::instrument(ret)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        undefined()
    }

    #[tracing::instrument(ret)]
    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        undefined()
    }
}
