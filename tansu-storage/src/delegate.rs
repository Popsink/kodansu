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

//! The [`Storage`](crate::Storage) method set, stated once for delegation.
//!
//! Five types implement `Storage` by forwarding it: [`Arc`](std::sync::Arc),
//! [`Box`](std::boxed::Box),
//! [`LatencyIntroducingStorage`](crate::latency::LatencyIntroducingStorage),
//! [`ProduceRequestBatcher`](crate::batch::ProduceRequestBatcher), and the test
//! `FlightRecorder`. Until #551 each wrote all fifty methods out by hand —
//! ~2 170 lines whose bodies were `self.as_ref().m(args).await`,
//! `self.introduce_latency().await?; self.storage.m(args).await`, or
//! `unimplemented!()`.
//!
//! # Why one list rather than five copies
//!
//! #273 is this code one failure earlier. Two methods with **default bodies**
//! — `offset_stage_at` and `read_group` — were simply absent from
//! `ProduceRequestBatcher` and `LatencyIntroducingStorage`, so the trait
//! defaults absorbed them and #109 and #111 were inert in every object-store
//! deployment.
//!
//! The default body is the whole mechanism of that bug, and it is worth being
//! precise about it: a trait method **without** one already fails to compile
//! when a wrapper omits it, so forty-eight of the fifty were never at risk.
//! #551 closes the remaining two by removing their defaults (#273's option 1)
//! and generating the forwarding from here (its option 2), so the list is both
//! the only place the set is stated and the only place it can be forgotten.
//!
//! # How an expander is written
//!
//! [`storage_methods`] takes the name of a macro and hands it the fifty
//! signatures. That macro emits the whole `#[async_trait] impl` in one
//! expansion — it has to, because `#[async_trait]` rewrites the impl it is
//! attached to and cannot see through a macro call sitting inside one.
//!
//! Per-method bodies come from a second macro invoked in *expression*
//! position, which `async_trait` does see through. `self` is threaded as
//! `$s:ident` from the signature list, so the receiver and the body share one
//! hygiene context.
//!
//! An override that needs an **attribute** — `#[instrument]` on `produce` in
//! two of the five — cannot be generated this way, because a `macro_rules!`
//! cannot expand into attribute position. Those bodies live in an inherent
//! method that carries the attribute, and the generated trait method forwards
//! to it.

/// The fifty-two [`Storage`](crate::Storage) methods, handed to `$expand`.
///
/// The signatures are stated as `fn name(&self, arg: Ty) -> Ret;` — no
/// `async`, because every expander adds it, and the receiver is a real `self`
/// token so an expander can pass it into a body macro.
///
/// The two methods that are genuinely not `async` come last and are marked
/// `sync fn`, so an expander takes them in a second repetition group. Keep
/// them there: `macro_rules!` matches the two groups in order.
macro_rules! storage_methods {
    ($expand:ident) => {
        $expand! {
            fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()>;
            fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid>;
            fn incremental_alter_resource(&self, resource: AlterConfigsResource) -> Result<AlterConfigsResourceResponse>;
            fn delete_records(&self, topics: &[DeleteRecordsTopic]) -> Result<Vec<DeleteRecordsTopicResult>>;
            fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode>;
            fn brokers(&self) -> Result<Vec<DescribeClusterBroker>>;
            fn produce(&self, transaction_id: Option<&str>, topition: &Topition, batch: deflated::Batch) -> Result<i64>;
            fn fetch(&self, topition: &'_ Topition, offset: i64, min_bytes: u32, max_bytes: u32, isolation: IsolationLevel, max_wait: Duration) -> Result<Vec<deflated::Batch>>;
            fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage>;
            fn offset_stage_at(&self, topition: &Topition, isolation: IsolationLevel) -> Result<OffsetStage>;
            fn list_offsets(&self, isolation_level: IsolationLevel, offsets: &[(Topition, ListOffset)]) -> Result<Vec<(Topition, ListOffsetResponse)>>;
            fn offset_commit(&self, group_id: &str, retention_time_ms: Option<Duration>, offsets: &[(Topition, OffsetCommitRequest)]) -> Result<Vec<(Topition, ErrorCode)>>;
            fn offset_fetch(&self, group_id: Option<&str>, topics: &[Topition], require_stable: Option<bool>) -> Result<BTreeMap<Topition, CommittedOffset>>;
            fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, CommittedOffset>>;
            fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse>;
            fn upsert_user_scram_credential(&self, user: &str, mechanism: ScramMechanism, credential: ScramCredential) -> Result<()>;
            fn delete_user_scram_credential(&self, user: &str, mechanism: ScramMechanism) -> Result<()>;
            fn user_scram_credential(&self, user: &str, mechanism: ScramMechanism) -> Result<Option<ScramCredential>>;
            fn describe_config(&self, name: &str, resource: ConfigResource, keys: Option<&[String]>) -> Result<DescribeConfigsResult>;
            fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>>;
            fn delete_groups(&self, group_ids: Option<&[String]>) -> Result<Vec<DeletableGroupResult>>;
            fn describe_groups(&self, group_ids: Option<&[String]>, include_authorized_operations: bool) -> Result<Vec<NamedGroupDetail>>;
            fn describe_topic_partitions(&self, topics: Option<&[TopicId]>, partition_limit: i32, cursor: Option<Topition>) -> Result<Vec<DescribeTopicPartitionsResponseTopic>>;
            fn write_group_member(&self, group_id: &str, member_id: &str, member: MemberDoc, version: Option<Version>) -> Result<Version, UpdateError<MemberDoc>>;
            fn read_group_member(&self, group_id: &str, member_id: &str) -> Result<Option<(MemberDoc, Version)>>;
            fn delete_group_member(&self, group_id: &str, member_id: &str) -> Result<()>;
            fn list_group_members(&self, group_id: &str) -> Result<BTreeMap<String, (MemberDoc, Version)>>;
            fn list_group_member_stamps(&self, group_id: &str) -> Result<BTreeMap<String, i64>>;
            fn read_group_generation(&self, group_id: &str) -> Result<Option<(GenerationDoc, Version)>>;
            fn update_group_generation(&self, group_id: &str, generation: GenerationDoc, version: Option<Version>) -> Result<Version, UpdateError<GenerationDoc>>;
            fn create_group_assignment(&self, group_id: &str, generation_id: i32, assignment: AssignmentDoc) -> Result<AssignmentOutcome>;
            fn read_group_assignment(&self, group_id: &str, generation_id: i32) -> Result<Option<AssignmentDoc>>;
            fn delete_group_assignments_before(&self, group_id: &str, generation_id: i32) -> Result<u64>;
            fn create_acls(&self, bindings: &[AclBinding]) -> Result<Vec<ErrorCode>>;
            fn describe_acls(&self, filter: &AclFilter) -> Result<Vec<AclBinding>>;
            fn delete_acls(&self, filters: &[AclFilter]) -> Result<Vec<Vec<AclBinding>>>;
            fn alter_client_quotas(&self, alterations: &[QuotaAlteration], validate_only: bool) -> Result<Vec<ErrorCode>>;
            fn describe_client_quotas(&self, components: &[QuotaFilterComponent], strict: bool) -> Result<Vec<(QuotaEntity, QuotaLimits)>>;
            fn client_quotas(&self) -> Result<Quotas>;
            fn assert_group_schema(&self) -> Result<()>;
            fn init_producer(&self, transaction_id: Option<&str>, transaction_timeout_ms: i32, producer_id: Option<i64>, producer_epoch: Option<i16>) -> Result<ProducerIdResponse>;
            fn txn_add_offsets(&self, transaction_id: &str, producer_id: i64, producer_epoch: i16, group_id: &str) -> Result<ErrorCode>;
            fn txn_add_partitions(&self, partitions: TxnAddPartitionsRequest) -> Result<TxnAddPartitionsResponse>;
            fn txn_offset_commit(&self, offsets: TxnOffsetCommitRequest) -> Result<Vec<TxnOffsetCommitResponseTopic>>;
            fn txn_end(&self, transaction_id: &str, producer_id: i64, producer_epoch: i16, committed: bool) -> Result<ErrorCode>;
            fn maintain(&self, _now: SystemTime) -> Result<()>;
            fn cluster_id(&self) -> Result<String>;
            fn node(&self) -> Result<i32>;
            fn advertised_listener(&self) -> Result<Url>;
            fn ping(&self) -> Result<()>;

            // The two that are not `async`, last and marked, so an expander can
            // take them in a second repetition group. Both had default bodies
            // until #551 — `fetch_max_bytes`'s in particular is #547's
            // per-deployment clamp, which every wrapper has to forward or the
            // key is inert behind it, exactly the #273 shape.
            sync fn auto_create_topic_config(&self) -> AutoTopicCreate;
            sync fn fetch_max_bytes(&self) -> u32;
        }
    };
}

pub(crate) use storage_methods;
