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
//
//! Tansu Storage Abstraction
//!
//! [`StorageContainer::builder`] selects a [`Storage`] from a URL scheme:
//! `memory://` for tests and ephemeral use, or an object store —
//! [S3](https://en.wikipedia.org/wiki/Amazon_S3) and Google Cloud Storage.
//!
//! The PostgreSQL, libSQL and Turso backends this used to advertise were removed
//! in #96; their examples went with this doc's rewrite (#279).
//!
//! ## Memory
//!
//! ```
//! # use tansu_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("tansu")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("memory://tansu/")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## S3
//!
//! ```no_run
//! # use tansu_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("tansu")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("s3://tansu/")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!

use async_trait::async_trait;
use bytes::Bytes;

use console::Emoji;
#[cfg(feature = "dynostore")]
use dynostore::CoalesceTuning;
// Gated like the `mod dynostore` it names, and like the `CoalesceTuning` import
// directly above. Without this the crate does not build without its own optional
// feature: `cargo check -p tansu-storage --no-default-features` fails on an
// unresolved `dynostore`, and so does building any *other* crate alone with
// `--all-features`, which resolves this one to its non-default set.
#[cfg(feature = "dynostore")]
pub use dynostore::DynoStore;

use indicatif::{ProgressBar, ProgressStyle};
#[cfg(feature = "dynostore")]
use object_store::memory::InMemory;

#[cfg(feature = "dynostore")]
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
#[cfg(feature = "dynostore")]
use object_store::{BackoffConfig, RetryConfig};

use opentelemetry::{
    InstrumentationScope, global,
    metrics::{Meter, UpDownCounter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;

use governor::InsufficientCapacity;
use serde::{Deserialize, Serialize};
use std::{
    array::TryFromSliceError,
    collections::BTreeMap,
    fmt::{self, Debug, Display, Formatter},
    io,
    marker::PhantomData,
    num::{ParseIntError, TryFromIntError},
    result,
    str::FromStr,
    sync::{Arc, LazyLock, PoisonError},
    time::{Duration, SystemTime, SystemTimeError},
};
use tansu_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, ScramMechanism,
    add_partitions_to_txn_request::{
        AddPartitionsToTxnRequest, AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
    },
    add_partitions_to_txn_response::{AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult},
    consumer_group_describe_response,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    delete_topics_request::DeleteTopicState,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_groups_response,
    describe_topic_partitions_request::{Cursor, TopicRequest},
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    fetch_request::FetchTopic,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    join_group_response::JoinGroupResponseMember,
    list_groups_response::ListedGroup,
    metadata_request::MetadataRequestTopic,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
    offset_commit_request::OffsetCommitRequestPartition,
    record::deflated,
    to_system_time, to_timestamp,
    txn_offset_commit_request::TxnOffsetCommitRequestTopic,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tracing::debug;
use tracing_subscriber::filter::ParseError;
use url::Url;
use uuid::Uuid;

#[cfg(feature = "dynostore")]
use tracing::warn;

#[cfg(feature = "dynostore")]
mod dynostore;

#[cfg(feature = "dynostore")]
mod audit;

#[cfg(feature = "dynostore")]
pub use audit::{Audit, AuditReport, Bracket, Gap, PartitionAudit, SegmentFault, TopicAudit};

mod acl;
mod authorizer;
mod batch;
mod enforcer;
mod group;
mod latency;
mod null;
mod quota;
mod service;
mod validation;

pub use acl::{
    AclBinding, AclFilter, Acls, CLUSTER_RESOURCE, WILDCARD_HOST, WILDCARD_PRINCIPAL,
    WILDCARD_RESOURCE,
};

pub use authorizer::{
    ACL_SNAPSHOT_TTL, Authorizer, Requester, authorized, authorized_cluster, enforcing,
};

pub use enforcer::{Charge, MAX_THROTTLE, QUOTA_SNAPSHOT_TTL, QuotaEnforcer};

pub use quota::{
    CONSUMER_BYTE_RATE, PRODUCER_BYTE_RATE, QUOTA_KEYS, QuotaAlteration, QuotaEntity,
    QuotaFilterComponent, QuotaKeyError, QuotaLimits, QuotaMatch, QuotaOp, Quotas, REQUEST_RATE,
    USER_ENTITY, user_of,
};

pub use group::{
    AssignmentDoc, AssignmentOutcome, GROUP_SCHEMA_VERSION, GenerationDoc, GroupSchema, MemberDoc,
    MemberRef,
};

pub use latency::LatencyIntroducingStorage;

pub use service::{
    AlterClientQuotasService, AlterUserScramCredentialsService, ConsumerGroupDescribeService,
    CreateAclsService, CreateTopicsService, DeleteAclsService, DeleteGroupsService,
    DeleteRecordsService, DeleteTopicsService, DescribeAclsService, DescribeClientQuotasService,
    DescribeClusterService, DescribeConfigsService, DescribeGroupsService,
    DescribeTopicPartitionsService, DescribeUserScramCredentialsService, FetchService,
    FindCoordinatorService, GetTelemetrySubscriptionsService, IncrementalAlterConfigsService,
    InitProducerIdService, ListGroupsService, ListOffsetsService,
    ListPartitionReassignmentsService, MetadataService, ProduceService, TxnAddOffsetsService,
    TxnAddPartitionService, TxnEndService, TxnOffsetCommitService,
};

#[cfg(feature = "dynostore")]
mod gcs;

#[cfg(feature = "dynostore")]
mod os;

/// Everything a read knew about a segment region that would not decode as batch
/// frames (#386), so the *next* occurrence says which cause it is instead of
/// only that it happened.
///
/// The failure used to reach the log as a bare `TryFromIntError` — no prefix, no
/// sequence, no byte range, not even which of the two candidate causes it was:
/// a footer entry that disagrees with its payload (a `byte_start` that does not
/// point at a frame, so `read_len == byte_len` and the head is garbage), or a
/// damaged/partially-visible object (a ranged GET that came back short, so
/// `read_len < byte_len`). Those want opposite fixes — the first is a write-side
/// integrity bug in the segment rewrite, the second is read-side repair — and
/// nothing in the error told them apart.
///
/// `head` is the hex of the bytes at `at`, where a `base_offset (i64) +
/// batch_length (i32)` frame header was expected; `declared` is the length those
/// bytes decoded to, when a length was read at all.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorruptRegion {
    /// Coalesced prefix the segment belongs to.
    pub prefix: String,
    /// Segment sequence within the prefix.
    pub seq: u64,
    pub topic: String,
    pub partition: i32,
    /// The footer entry's claims about the region.
    pub base_offset: i64,
    pub byte_start: u64,
    pub byte_len: u64,
    /// Bytes the read actually returned; short of `byte_len` means a torn or
    /// partially-visible object rather than a divergent footer.
    pub read_len: usize,
    /// Byte offset *within the region* where the frame scan stopped.
    pub at: usize,
    /// The `batch_length` read at `at`, when one was read.
    pub declared: Option<i32>,
    /// Hex of the frame header bytes at `at`.
    pub head: String,
    /// Why the scan stopped there.
    pub detail: String,
}

/// A batch refused at the segment encoder because its `batch_length` field does
/// not describe the bytes it serialises to (#393).
///
/// Writing it would produce exactly the region [`CorruptRegion`] reports: a
/// footer entry covering the bytes actually written, and a frame at
/// `byte_start` declaring more of them than exist. Refusing it here is the
/// write-side half of that issue — the footer cannot claim what the encoder
/// never accepted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DivergentBatch {
    pub topic: String,
    pub partition: i32,
    /// Offset the sub-stream assigns this batch's region.
    pub base_offset: i64,
    /// Index of the batch within the sub-stream, so a run of batches can be
    /// told apart.
    pub index: usize,
    /// The `batch_length` the batch carries.
    pub declared: i32,
    /// The `batch_length` its own bytes imply.
    pub encoded: i32,
    /// `magic`, because the one shape known to reach here is the pre-v2 husk
    /// (`magic != 2`) the decoder returns for a MessageSet it cannot represent.
    pub magic: i8,
    pub record_data_len: usize,
}

/// Storage Errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    Api(ErrorCode),

    /// A segment region whose bytes are not the batch frames its footer entry
    /// claims (#386). Answered to the client as `CORRUPT_MESSAGE` for that
    /// partition — see [`storage_error_code`].
    CorruptSegment(Box<CorruptRegion>),

    /// A batch whose declared length diverges from its bytes, refused before it
    /// could be written (#393). Answered as `CORRUPT_MESSAGE`: the batch is
    /// unusable and no retry of the same bytes changes that.
    DivergentBatch(Box<DivergentBatch>),

    FeatureNotEnabled {
        feature: String,
        message: String,
    },

    InsufficientCapacity(#[from] InsufficientCapacity),
    Io(Arc<io::Error>),

    Message(String),
    NoSuchOffset(i64),

    #[cfg(feature = "dynostore")]
    ObjectStore(Arc<object_store::Error>),

    ParseFilter(Arc<ParseError>),
    ParseInt(#[from] ParseIntError),
    PhantomCached(),
    Poison,

    SansIo(#[from] tansu_sans_io::Error),

    SerdeJson(Arc<serde_json::Error>),

    SystemTime(#[from] SystemTimeError),

    TryFromInt(#[from] TryFromIntError),
    TryFromSlice(#[from] TryFromSliceError),

    UnsupportedStorageUrl(Url),
    UnexpectedAddPartitionsToTxnRequest(Box<AddPartitionsToTxnRequest>),
    Url(#[from] url::ParseError),
    UnknownTxnState(String),

    Uuid(#[from] uuid::Error),

    UnableToSend,
    OneshotRecv,
}

impl Error {
    /// Expected idempotent-producer protocol outcomes handled by a well-behaved
    /// client on its own (e.g. a retried-after-disconnect batch that tansu had
    /// already persisted). These are the normal Kafka idempotent-producer
    /// contract, not broker failures, so they must not be logged at error/warn
    /// (#37).
    pub(crate) fn is_expected_idempotent_outcome(&self) -> bool {
        matches!(
            self,
            Error::Api(ErrorCode::DuplicateSequenceNumber | ErrorCode::OutOfOrderSequenceNumber)
        )
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

#[cfg(feature = "dynostore")]
impl From<Arc<object_store::Error>> for Error {
    fn from(value: Arc<object_store::Error>) -> Self {
        Self::ObjectStore(value)
    }
}

#[cfg(feature = "dynostore")]
impl From<object_store::Error> for Error {
    fn from(value: object_store::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

/// Map an unexpected (non-`Api`) error to the Kafka error code returned to the
/// client.
///
/// Transient storage failures (e.g. an S3 error under load) are mapped to
/// [`ErrorCode::KafkaStorageError`], which clients treat as **retriable** —
/// instead of [`ErrorCode::UnknownServerError`] (-1), which is fatal and makes
/// clients drop the whole batch (#6). Anything else stays UNKNOWN, since it
/// signals a genuine bug rather than something a retry would fix.
///
/// This lives at the crate root because the distinction is not specific to one
/// API: produce got it right first, and every path that reports a storage
/// failure to a client owes the client the same answer. `OffsetCommit` did not,
/// and a throttle burst therefore restarted connectors instead of being retried
/// a moment later (#275).
pub(crate) fn storage_error_code(error: &Error) -> ErrorCode {
    match error {
        #[cfg(feature = "dynostore")]
        Error::ObjectStore(_) => ErrorCode::KafkaStorageError,

        // Damage in a stored segment region (#386), which is exactly what
        // `CORRUPT_MESSAGE` means: the bytes for these offsets are unreadable, so
        // no retry against this broker will change the answer and the client is
        // told so rather than being handed a dropped socket to guess from.
        Error::CorruptSegment(_) => ErrorCode::CorruptMessage,

        // Refused before the write (#393). Same answer as reading the damage
        // would give, for the same reason: these bytes cannot be stored or
        // served, and re-sending them will not help.
        Error::DivergentBatch(_) => ErrorCode::CorruptMessage,

        _ => ErrorCode::UnknownServerError,
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

impl From<Arc<serde_json::Error>> for Error {
    fn from(value: Arc<serde_json::Error>) -> Self {
        Self::SerdeJson(value)
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

/// Topic Partition (topition)
///
/// A topic partition pair.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Topition {
    topic: String,
    partition: i32,
}

impl Topition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        let topic = topic.into();
        Self { topic, partition }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }
}

impl From<Cursor> for Topition {
    fn from(value: Cursor) -> Self {
        Self {
            topic: value.topic_name,
            partition: value.partition_index,
        }
    }
}

impl FromStr for Topition {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        i32::from_str(&s[s.len() - 10..])
            .map(|partition| {
                let topic = String::from(&s[..s.len() - 11]);

                Self { topic, partition }
            })
            .map_err(Into::into)
    }
}

/// Topic Partition Offset
///
/// A topic partition with an offset.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopitionOffset {
    topition: Topition,
    offset: i64,
}

impl TopitionOffset {
    pub fn new(topition: Topition, offset: i64) -> Self {
        Self { topition, offset }
    }

    pub fn topition(&self) -> &Topition {
        &self.topition
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListOffsetResponse {
    pub error_code: ErrorCode,
    pub timestamp: Option<SystemTime>,
    pub offset: Option<i64>,
}

impl Default for ListOffsetResponse {
    fn default() -> Self {
        Self {
            error_code: ErrorCode::None,
            timestamp: None,
            offset: None,
        }
    }
}

impl ListOffsetResponse {
    pub fn offset(&self) -> Option<i64> {
        self.offset
    }

    pub fn timestamp(&self) -> Result<Option<i64>> {
        self.timestamp.map_or(Ok(None), |system_time| {
            to_timestamp(&system_time).map(Some).map_err(Into::into)
        })
    }

    pub fn error_code(&self) -> ErrorCode {
        self.error_code
    }
}

/// Offset Commit Request
///
/// A structure representing an [`tansu_sans_io::OffsetCommitRequestPartition](OffsetCommitRequestPartition).
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct OffsetCommitRequest {
    offset: i64,
    leader_epoch: Option<i32>,
    timestamp: Option<SystemTime>,
    metadata: Option<String>,
}

impl OffsetCommitRequest {
    pub fn offset(self, offset: i64) -> Self {
        Self { offset, ..self }
    }
}

impl TryFrom<&OffsetCommitRequestPartition> for OffsetCommitRequest {
    type Error = Error;

    fn try_from(value: &OffsetCommitRequestPartition) -> Result<Self, Self::Error> {
        value
            .commit_timestamp
            .map_or(Ok(None), |commit_timestamp| {
                to_system_time(commit_timestamp)
                    .map(Some)
                    .map_err(Into::into)
            })
            .map(|timestamp| Self {
                offset: value.committed_offset,
                leader_epoch: value.committed_leader_epoch,
                timestamp,
                metadata: value.committed_metadata.clone(),
            })
    }
}

/// A committed offset as a group reads it back (#445).
///
/// `offset` alone is what `offset_fetch` used to answer, and the metadata a
/// client had committed beside it was dropped on the way out — accepted and
/// stored, then projected away by the return type, so nothing could report the
/// loss at write time. Frameworks that keep their restore point in commit
/// metadata (Streams-style checkpointing, recovery tools) found out during a
/// recovery, which is the worst moment there is.
///
/// `-1` with no metadata is the "nothing committed" answer, unchanged.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CommittedOffset {
    pub offset: i64,
    pub metadata: Option<String>,
}

impl CommittedOffset {
    /// No offset committed for this partition.
    pub const NONE: Self = Self {
        offset: -1,
        metadata: None,
    };

    pub fn new(offset: i64, metadata: Option<String>) -> Self {
        Self { offset, metadata }
    }
}

impl From<&OffsetCommitRequest> for CommittedOffset {
    fn from(value: &OffsetCommitRequest) -> Self {
        Self {
            offset: value.offset,
            metadata: value.metadata.clone(),
        }
    }
}

/// Topic Id
///
/// An enumeration of either the name or UUID of a topic.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TopicId {
    Name(String),
    Id(Uuid),
}

impl FromStr for TopicId {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        Ok(Self::Name(s.into()))
    }
}

impl From<&str> for TopicId {
    fn from(value: &str) -> Self {
        Self::Name(value.to_owned())
    }
}

impl From<String> for TopicId {
    fn from(value: String) -> Self {
        Self::Name(value)
    }
}

impl From<Uuid> for TopicId {
    fn from(value: Uuid) -> Self {
        Self::Id(value)
    }
}

impl From<[u8; 16]> for TopicId {
    fn from(value: [u8; 16]) -> Self {
        Self::Id(Uuid::from_bytes(value))
    }
}

impl From<&TopicId> for [u8; 16] {
    fn from(value: &TopicId) -> Self {
        match value {
            TopicId::Id(id) => id.into_bytes(),
            TopicId::Name(_) => NULL_TOPIC_ID,
        }
    }
}

impl From<&FetchTopic> for TopicId {
    fn from(value: &FetchTopic) -> Self {
        if let Some(ref name) = value.topic {
            Self::Name(name.into())
        } else if let Some(ref id) = value.topic_id {
            Self::Id(Uuid::from_bytes(*id))
        } else {
            panic!("neither name nor uuid")
        }
    }
}

impl From<&MetadataRequestTopic> for TopicId {
    fn from(value: &MetadataRequestTopic) -> Self {
        if let Some(ref name) = value.name {
            Self::Name(name.into())
        } else if let Some(ref id) = value.topic_id {
            Self::Id(Uuid::from_bytes(*id))
        } else {
            panic!("neither name nor uuid")
        }
    }
}

impl From<DeleteTopicState> for TopicId {
    fn from(value: DeleteTopicState) -> Self {
        match value {
            DeleteTopicState {
                name: Some(name),
                topic_id,
                ..
            } if topic_id == NULL_TOPIC_ID => name.into(),

            DeleteTopicState { topic_id, .. } => topic_id.into(),
        }
    }
}

impl From<&TopicRequest> for TopicId {
    fn from(value: &TopicRequest) -> Self {
        value.name.to_owned().into()
    }
}

impl From<&Topition> for TopicId {
    fn from(value: &Topition) -> Self {
        value.topic.to_owned().into()
    }
}

/// Broker Registration Request
///
/// A broker will register with storage using this structure.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BrokerRegistrationRequest {
    pub broker_id: i32,
    pub cluster_id: String,
    pub incarnation_id: Uuid,
    pub rack: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct MetadataResponse {
    cluster: Option<String>,
    controller: Option<i32>,
    brokers: Vec<MetadataResponseBroker>,
    topics: Vec<MetadataResponseTopic>,
}

impl MetadataResponse {
    pub fn cluster(&self) -> Option<&str> {
        self.cluster.as_deref()
    }

    pub fn controller(&self) -> Option<i32> {
        self.controller
    }

    pub fn brokers(&self) -> &[MetadataResponseBroker] {
        self.brokers.as_ref()
    }

    pub fn topics(&self) -> &[MetadataResponseTopic] {
        self.topics.as_ref()
    }
}

/// Offset Stage
///
/// An offset stage structure represents the `last_stable`, `high_watermark` and `log_start` offsets.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct OffsetStage {
    last_stable: i64,
    high_watermark: i64,
    log_start: i64,
    /// Aborted transactions whose data is still in the log for this partition
    /// (#81), as `(producer_id, first_offset)` sorted by first offset — what a
    /// read-committed consumer needs to filter out aborted records below the
    /// last-stable offset. Empty for backends that do not surface it (unchanged
    /// behaviour) and for non-transactional workloads.
    aborted: Vec<(i64, i64)>,
}

impl OffsetStage {
    pub fn last_stable(&self) -> i64 {
        self.last_stable
    }

    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    pub fn log_start(&self) -> i64 {
        self.log_start
    }

    /// Aborted transactions `(producer_id, first_offset)` for a read-committed
    /// fetch response (#81).
    pub fn aborted(&self) -> &[(i64, i64)] {
        &self.aborted
    }
}

/// Broker policy for auto-creating a topic referenced by a Metadata request
/// (Kafka `auto.create.topics.enable` / `num.partitions` /
/// `default.replication.factor`). A topic is auto-created only when both this
/// `enable` flag and the request's `allow_auto_topic_creation` are set.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AutoTopicCreate {
    pub enable: bool,
    pub num_partitions: i32,
    pub replication_factor: i16,
}

impl Default for AutoTopicCreate {
    fn default() -> Self {
        Self {
            enable: true,
            num_partitions: 1,
            replication_factor: 1,
        }
    }
}

/// The Apache Kafka default `retention.ms` (7 days).
pub const DEFAULT_RETENTION_MS: i64 = 604_800_000;

/// The Apache Kafka default `cleanup.policy`.
pub const DEFAULT_CLEANUP_POLICY: &str = "delete";

/// Broker-level topic config defaults, applied by the engine when a topic is
/// created without an explicit value.
///
/// Mirrors Apache Kafka, where `cleanup.policy` defaults to `delete` and
/// `retention.ms` to 7 days, so retention is enforced even when the client sends
/// no topic config.
///
/// Applied inside [`Storage::create_topic`] rather than in the `CreateTopics`
/// service, so a topic's effective config cannot depend on which API created it
/// (#225): the auto-create path builds its own `CreatableTopic` and used to bypass
/// the injection entirely, storing no config at all.
///
/// Setting [`cleanup_policy`](Self::cleanup_policy) to `None` (or an empty
/// string) opts out of *injecting* a stored policy. It does **not** give the
/// topic infinite retention, which is what this said before #223: the engine
/// reads an absent `cleanup.policy` as Kafka's default, `delete`, and applies the
/// 7-day `retention.ms` fallback, so opting out of the injection produces a topic
/// that expires at 7 days with nothing recorded to explain why.
///
/// Retain-forever has exactly one spelling: `retention.ms=-1`, which both expiry
/// paths map to "never". It can be set per topic with `(Incremental)AlterConfigs`;
/// there is no broker-level default that expresses it (#224).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TopicDefaults {
    /// Default `cleanup.policy`; `None`/empty means "do not inject a stored
    /// policy" — the engine still reads absent as `delete`.
    pub cleanup_policy: Option<String>,

    /// Default `retention.ms`, injected only for `delete`-policy topics. `-1`
    /// means retain forever.
    pub retention_ms: i64,
}

impl Default for TopicDefaults {
    fn default() -> Self {
        Self {
            cleanup_policy: Some(DEFAULT_CLEANUP_POLICY.into()),
            retention_ms: DEFAULT_RETENTION_MS,
        }
    }
}

impl TopicDefaults {
    /// Inject the defaults into `configs` for any key the caller omitted.
    ///
    /// `cleanup.policy` is only added when a default is configured; `retention.ms`
    /// is only added when the effective `cleanup.policy` contains `delete` (a
    /// `compact` topic keeps no retention), so internal compacted topics are left
    /// untouched. Idempotent — an already-present key is never overwritten — so
    /// applying it at the single creation choke point is safe even if a caller
    /// above has already filled a value in.
    pub(crate) fn apply(&self, configs: &mut Vec<CreatableTopicConfig>) {
        let default_policy = self
            .cleanup_policy
            .as_deref()
            .filter(|policy| !policy.is_empty());

        if let Some(policy) = default_policy
            && !configs.iter().any(|config| config.name == "cleanup.policy")
        {
            configs.push(
                CreatableTopicConfig::default()
                    .name("cleanup.policy".into())
                    .value(Some(policy.to_owned())),
            );
        }

        let policy_is_delete = configs
            .iter()
            .find(|config| config.name == "cleanup.policy")
            .and_then(|config| config.value.as_deref())
            .is_some_and(|policy| policy.contains("delete"));

        if policy_is_delete && !configs.iter().any(|config| config.name == "retention.ms") {
            configs.push(
                CreatableTopicConfig::default()
                    .name("retention.ms".into())
                    .value(Some(self.retention_ms.to_string())),
            );
        }
    }
}

/// Group Member
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupMember {
    pub join_response: JoinGroupResponseMember,
    pub last_contact: Option<SystemTime>,
}

/// Group State
///
/// A group is either in the process of [`GroupState::Forming`] or has [`GroupState::Formed`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum GroupState {
    Forming {
        protocol_type: Option<String>,
        protocol_name: Option<String>,
        leader: Option<String>,
    },

    Formed {
        protocol_type: String,
        protocol_name: String,
        leader: String,
        assignments: BTreeMap<String, Bytes>,
    },
}

impl GroupState {
    pub fn protocol_type(&self) -> Option<String> {
        match self {
            Self::Forming { protocol_type, .. } => protocol_type.clone(),
            Self::Formed { protocol_type, .. } => Some(protocol_type.clone()),
        }
    }

    pub fn protocol_name(&self) -> Option<String> {
        match self {
            Self::Forming { protocol_name, .. } => protocol_name.clone(),
            Self::Formed { protocol_name, .. } => Some(protocol_name.clone()),
        }
    }

    pub fn leader(&self) -> Option<String> {
        match self {
            Self::Forming { leader, .. } => leader.clone(),
            Self::Formed { leader, .. } => Some(leader.clone()),
        }
    }

    pub fn assignments(&self) -> BTreeMap<String, Bytes> {
        match self {
            Self::Forming { .. } => BTreeMap::new(),
            Self::Formed { assignments, .. } => assignments.clone(),
        }
    }
}

impl Default for GroupState {
    fn default() -> Self {
        Self::Forming {
            protocol_type: None,
            protocol_name: Some("".into()),
            leader: None,
        }
    }
}

/// Consumer Group State
///
/// A helper type for conversion into [`consumer_group_describe_response::DescribedGroup`].
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum ConsumerGroupState {
    Unknown,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
    Empty,
    Assigning,
    Reconciling,
}

impl Display for ConsumerGroupState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("Unknown"),
            Self::PreparingRebalance => f.write_str("PreparingRebalance"),
            Self::CompletingRebalance => f.write_str("CompletingRebalance"),
            Self::Stable => f.write_str("Stable"),
            Self::Dead => f.write_str("Dead"),
            Self::Empty => f.write_str("Empty"),
            Self::Assigning => f.write_str("Assigning"),
            Self::Reconciling => f.write_str("Reconciling"),
        }
    }
}

/// Group Detail
///
/// A helper type that can be easily converted into [`consumer_group_describe_response::DescribedGroup`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupDetail {
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: Option<i32>,
    pub members: BTreeMap<String, GroupMember>,
    pub generation_id: i32,
    pub skip_assignment: Option<bool>,
    pub inception: SystemTime,
    pub state: GroupState,
}

impl Default for GroupDetail {
    fn default() -> Self {
        Self {
            session_timeout_ms: 45_000,
            rebalance_timeout_ms: None,
            members: BTreeMap::new(),
            generation_id: -1,
            skip_assignment: Some(false),
            inception: SystemTime::now(),
            state: GroupState::default(),
        }
    }
}

impl From<&GroupDetail> for ConsumerGroupState {
    fn from(value: &GroupDetail) -> Self {
        match value {
            GroupDetail { members, .. } if members.is_empty() => Self::Empty,

            GroupDetail {
                state: GroupState::Forming { leader: None, .. },
                ..
            } => Self::Assigning,

            GroupDetail {
                state: GroupState::Formed { .. },
                ..
            } => Self::Stable,

            // Members have joined and a leader is elected, but `SyncGroup` has
            // not distributed the assignment yet (#215). Kafka calls that
            // `CompletingRebalance`; reporting `Unknown` made an
            // actively-consuming group that rebalances often look broken, and
            // indistinguishable from a genuinely unknown one.
            //
            // This was the *only* way to reach `Unknown` from a `GroupDetail`,
            // which the compiler now confirms: with this arm present the
            // catch-all is unreachable. A `GroupDetail` is a group the broker
            // holds state for, so it never had an unknown state to report — it
            // had a state with no mapping.
            GroupDetail {
                state:
                    GroupState::Forming {
                        leader: Some(_), ..
                    },
                ..
            } => Self::CompletingRebalance,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum GroupDetailResponse {
    ErrorCode(ErrorCode),
    Found(GroupDetail),

    /// The group does not exist (#445).
    ///
    /// Distinct from `Found` with no members, and the distinction is the point:
    /// Kafka answers `Dead` for a group it has never heard of and `Empty` for
    /// one that exists with nobody in it. Collapsing the two leaves
    /// cleanup-by-inactivity tooling unable to tell a group it should reap from
    /// one it has already reaped, and monitoring unable to tell a group that
    /// went away from one that is merely idle.
    ///
    /// Not an `ErrorCode`: Kafka reports this with `NONE` and the state
    /// `Dead`, because not existing is an answer about the group rather than a
    /// failure to answer.
    Dead,
}

/// NamedGroupDetail
///
/// A helper type that can be easily converted into [`consumer_group_describe_response::DescribedGroup`]
/// or [`describe_groups_response::DescribedGroup`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NamedGroupDetail {
    name: String,
    response: GroupDetailResponse,
}

impl NamedGroupDetail {
    pub fn error_code(name: String, error_code: ErrorCode) -> Self {
        Self {
            name,
            response: GroupDetailResponse::ErrorCode(error_code),
        }
    }

    pub fn found(name: String, found: GroupDetail) -> Self {
        Self {
            name,
            response: GroupDetailResponse::Found(found),
        }
    }

    /// A group the broker holds no state for at all (#445).
    pub fn dead(name: String) -> Self {
        Self {
            name,
            response: GroupDetailResponse::Dead,
        }
    }
}

impl From<&NamedGroupDetail> for consumer_group_describe_response::DescribedGroup {
    fn from(value: &NamedGroupDetail) -> Self {
        let NamedGroupDetail { name, response } = value;

        // Every group this engine holds is a *classic* group, and
        // `ConsumerGroupDescribe` describes KIP-848 groups only. Kafka answers
        // `GROUP_ID_NOT_FOUND` for a classic group here, and that code is not a
        // failure to the caller — it is the signal the AdminClient waits for
        // before retrying on `DescribeGroups` ("failed because the group is not
        // a new consumer group"). So a found group is reported not-found on
        // this API, and there is nothing left to describe on it.
        //
        // Answering `NONE` with an empty member list instead — which is what
        // this did until the pre-cut rehearsal caught it — is taken at face
        // value: the client never falls back, so a healthy group reports zero
        // members, zero assigned partitions and therefore zero lag. That is the
        // whole of #215's symptom, still live for every 4.x client, on the one
        // API a modern AdminClient tries first.
        //
        // Reporting a group here for real means decoding the classic protocol's
        // opaque assignment blob into KIP-848's structured form. Until this
        // engine speaks that protocol there is no honest `NONE` to give.
        let error_code = match response {
            // A dead group is not a KIP-848 group either, and this API answers
            // for those alone — so it is reported the same way a live classic
            // group is, and for the same reason.
            GroupDetailResponse::Found(_) | GroupDetailResponse::Dead => ErrorCode::GroupIdNotFound,
            GroupDetailResponse::ErrorCode(error_code) => *error_code,
        };

        Self::default()
            .error_code(error_code.into())
            .error_message(Some(error_code.to_string()))
            .group_id(name.into())
            .group_state("Unknown".into())
            .group_epoch(-1)
            .assignment_epoch(-1)
            .assignor_name("".into())
            .members(Some([].into()))
            .authorized_operations(-1)
    }
}

impl From<&NamedGroupDetail> for describe_groups_response::DescribedGroup {
    fn from(value: &NamedGroupDetail) -> Self {
        match value {
            NamedGroupDetail {
                name,
                response: GroupDetailResponse::Found(group_detail),
            } => {
                let group_state = ConsumerGroupState::from(group_detail).to_string();

                // The assignment every client and admin tool decodes to learn
                // which partitions a group holds (#215). It is persisted — it is
                // what `SyncGroup` distributed — and was simply never surfaced:
                // an empty buffer here means `rpk`, `kafka-consumer-groups.sh`
                // and the AdminClient all derive an empty partition set, never
                // issue `OffsetFetch`, and report `TOTAL-LAG 0` for every group
                // whether it is healthy or wedged.
                let assignments = match group_detail.state {
                    GroupState::Formed {
                        ref assignments, ..
                    } => Some(assignments),
                    GroupState::Forming { .. } => None,
                };

                let members = group_detail
                    .members
                    .iter()
                    .map(|(member_id, member)| {
                        describe_groups_response::DescribedGroupMember::default()
                            .member_id(member_id.into())
                            .group_instance_id(member.join_response.group_instance_id.clone())
                            // Not persisted in the group state, so it cannot be
                            // reported without widening `GroupMember` — see the
                            // note on #215.
                            .client_id("".into())
                            .client_host("".into())
                            // The subscription the member joined with.
                            .member_metadata(member.join_response.metadata.clone())
                            .member_assignment(
                                assignments
                                    .and_then(|assignments| assignments.get(member_id))
                                    .cloned()
                                    .unwrap_or_default(),
                            )
                    })
                    .collect::<Vec<_>>();

                Self::default()
                    .error_code(ErrorCode::None.into())
                    .group_id(name.clone())
                    .group_state(group_state)
                    .protocol_type(group_detail.state.protocol_type().unwrap_or_default())
                    .protocol_data(group_detail.state.protocol_name().unwrap_or_default())
                    .members(Some(members))
                    .authorized_operations(Some(-1))
            }

            // `NONE` with the state `Dead`, as Kafka answers: the broker knows
            // the group does not exist, which is an answer about the group and
            // not a failure to give one (#445).
            NamedGroupDetail {
                name,
                response: GroupDetailResponse::Dead,
            } => Self::default()
                .error_code(ErrorCode::None.into())
                .group_id(name.clone())
                .group_state(ConsumerGroupState::Dead.to_string())
                .protocol_type("".into())
                .protocol_data("".into())
                .members(Some(vec![]))
                .authorized_operations(Some(-1)),

            NamedGroupDetail {
                name,
                response: GroupDetailResponse::ErrorCode(error_code),
            } => Self::default()
                .error_code((*error_code).into())
                .group_id(name.clone())
                .group_state("Unknown".into())
                .protocol_type("".into())
                .protocol_data("".into())
                .members(Some(vec![]))
                .authorized_operations(Some(-1)),
        }
    }
}

/// Version representing an `e_tag` and `version` used in conditional writes to an object store.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Version {
    e_tag: Option<String>,
    version: Option<String>,
}

impl From<&Uuid> for Version {
    fn from(value: &Uuid) -> Self {
        Self {
            e_tag: Some(value.to_string()),
            version: None,
        }
    }
}

/// Producer Id Response
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ProducerIdResponse {
    pub error: ErrorCode,
    pub id: i64,
    pub epoch: i16,
}

impl Default for ProducerIdResponse {
    fn default() -> Self {
        Self {
            error: ErrorCode::None,
            id: 1,
            epoch: 0,
        }
    }
}

/// For protocol versions 0..=3 using [`AddPartitionsToTxnTopic`],
/// thereafter using [`AddPartitionsToTxnTransaction`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TxnAddPartitionsRequest {
    VersionZeroToThree {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        topics: Vec<AddPartitionsToTxnTopic>,
    },

    VersionFourPlus {
        transactions: Vec<AddPartitionsToTxnTransaction>,
    },
}

impl TryFrom<AddPartitionsToTxnRequest> for TxnAddPartitionsRequest {
    type Error = Error;

    fn try_from(value: AddPartitionsToTxnRequest) -> result::Result<Self, Self::Error> {
        match value {
            AddPartitionsToTxnRequest {
                transactions: None,
                v_3_and_below_transactional_id: Some(transactional_id),
                v_3_and_below_producer_id: Some(producer_id),
                v_3_and_below_producer_epoch: Some(producer_epoch),
                v_3_and_below_topics: Some(topics),
                ..
            } => Ok(Self::VersionZeroToThree {
                transaction_id: transactional_id,
                producer_id,
                producer_epoch,
                topics,
            }),

            AddPartitionsToTxnRequest {
                transactions: Some(transactions),
                v_3_and_below_transactional_id: None,
                v_3_and_below_producer_id: None,
                v_3_and_below_producer_epoch: None,
                v_3_and_below_topics: None,
                ..
            } => Ok(Self::VersionFourPlus { transactions }),

            unexpected => Err(Error::UnexpectedAddPartitionsToTxnRequest(Box::new(
                unexpected,
            ))),
        }
    }
}

/// Transaction Add Partitions Response
///
/// For protocol versions 0..=3 using `AddPartitionsToTxnTopic`, thereafter using `AddPartitionsToTxnTransaction`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TxnAddPartitionsResponse {
    VersionZeroToThree(Vec<AddPartitionsToTxnTopicResult>),
    VersionFourPlus(Vec<AddPartitionsToTxnResult>),
}

impl TxnAddPartitionsResponse {
    pub fn zero_to_three(&self) -> &[AddPartitionsToTxnTopicResult] {
        match self {
            Self::VersionZeroToThree(result) => result.as_slice(),
            Self::VersionFourPlus(_) => &[][..],
        }
    }
}

/// Transaction Offset Commit Request
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TxnOffsetCommitRequest {
    pub transaction_id: String,
    pub group_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub generation_id: Option<i32>,
    pub member_id: Option<String>,
    pub group_instance_id: Option<String>,
    pub topics: Vec<TxnOffsetCommitRequestTopic>,
}

/// Transaction State
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TxnState {
    Begin,
    PrepareCommit,
    PrepareAbort,
    Committed,
    Aborted,
}

impl TxnState {
    pub fn is_prepared(&self) -> bool {
        match self {
            Self::PrepareAbort | Self::PrepareCommit => true,
            _otherwise => false,
        }
    }
}

impl FromStr for TxnState {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ABORTED" => Ok(TxnState::Aborted),
            "BEGIN" => Ok(TxnState::Begin),
            "COMMITTED" => Ok(TxnState::Committed),
            "PREPARE_ABORT" => Ok(TxnState::PrepareAbort),
            "PREPARE_COMMIT" => Ok(TxnState::PrepareCommit),
            otherwise => Err(Error::UnknownTxnState(otherwise.to_owned())),
        }
    }
}

impl TryFrom<String> for TxnState {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<TxnState> for String {
    fn from(value: TxnState) -> Self {
        match value {
            TxnState::Begin => "BEGIN".into(),
            TxnState::PrepareCommit => "PREPARE_COMMIT".into(),
            TxnState::PrepareAbort => "PREPARE_ABORT".into(),
            TxnState::Committed => "COMMITTED".into(),
            TxnState::Aborted => "ABORTED".into(),
        }
    }
}

/// Storage
///
/// The Core storage abstraction. All storage engines implement this type.
#[async_trait]
pub trait Storage: Debug + Send + Sync + 'static {
    /// On startup a broker will register with storage.
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()>;

    /// Create a topic on this storage.
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid>;

    /// Incrementally alter a resource on this storage.
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse>;

    /// Delete records on this storage.
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>>;

    /// Delete a topic from this storage.
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode>;

    /// Query the brokers registered with this storage.
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>>;

    /// Produce a deflated batch to this storage.
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64>;

    /// Fetch deflated batches from storage.
    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>>;

    /// Query the offset stage for a topic partition.
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage>;

    /// Offset stage for a fetch *response* at the given isolation level.
    ///
    /// Read-uncommitted (the common consumer case) needs no transaction state:
    /// the last-stable offset equals the high watermark and the
    /// aborted-transaction list is empty. A backend may therefore serve it
    /// without reading the cluster-wide `meta.json` object, keeping the consumer
    /// fetch hot path off that single hot key — which at consumer-fan-out scale
    /// is both an S3 request ceiling (503 SlowDown) and ~2 extra round-trips per
    /// fetch (#109). Read-committed falls through to the full, transaction-aware
    /// [`Self::offset_stage`]. The default delegates for backends that do not
    /// distinguish the two.
    async fn offset_stage_at(
        &self,
        topition: &Topition,
        isolation: IsolationLevel,
    ) -> Result<OffsetStage> {
        let _ = isolation;
        self.offset_stage(topition).await
    }

    /// Query the offsets for one or more topic partitions.
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>>;

    /// Commit offsets for one or more topic partitions in a consumer group.
    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>>;

    /// Fetch committed offsets for one or more topic partitions in a consumer group.
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>>;

    /// Fetch all committed offsets in a consumer group.
    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>>;

    /// Query broker and topic metadata.
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse>;

    /// The broker's auto-topic-creation policy. Defaults to enabled with a
    /// single partition and replication factor of one; backends carrying a
    /// configured value override this.
    fn auto_create_topic_config(&self) -> AutoTopicCreate {
        AutoTopicCreate::default()
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()>;

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()>;

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>>;

    /// Query the configuration of a resource in this storage.
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult>;

    /// Query available groups optionally with a state filter.
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>>;

    /// Delete one or more groups from storage.
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>>;

    /// Describe the groups found in this storage.
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>>;

    /// Describe the topic partitions found in this storage.
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>>;

    /// Write a member's own document, `members/{member_id}.json` (#359).
    ///
    /// `version` is `None` to create and `Some` to CAS. A member has one
    /// *logical* writer — itself — but
    /// that writer can have two requests in flight on two replicas at once, and
    /// the CAS is what stops the later-arriving stale one (a heartbeat carrying
    /// the subscription as it was before a join changed it) from clobbering the
    /// newer.
    ///
    /// **Required, not defaulted.** Every method here is: a default body is
    /// silently satisfied by a wrapper that forgets to delegate, which is how
    /// the legacy `read_group` shipped inert in every object-store deployment
    /// for two releases (#273) — its `Ok(None)` default was the scar, and it
    /// went with the object it read.
    async fn write_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        member: MemberDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<MemberDoc>>;

    /// A member's document and its version, or `None` when the member has none.
    async fn read_group_member(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> Result<Option<(MemberDoc, Version)>>;

    /// Remove a member's document. Best-effort by contract: an orphaned
    /// document is harmless, because the generation's member set is what is
    /// authoritative, and the sweep reclaims it.
    async fn delete_group_member(&self, group_id: &str, member_id: &str) -> Result<()>;

    /// Every member document of a group, by member id.
    ///
    /// The one LIST on the group path, and deliberately not on the request
    /// path: joins and sweeps fan out over the generation's member set instead,
    /// which needs no listing. This exists for the paths that must discover
    /// documents the generation does not name — reconciliation and the tests
    /// that assert what was written.
    async fn list_group_members(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<String, (MemberDoc, Version)>>;

    /// A group's composition and its version, or `None` when the group has no
    /// generation object — which is how a group that does not exist, and a
    /// group that exists only in the legacy layout, both read.
    async fn read_group_generation(
        &self,
        group_id: &str,
    ) -> Result<Option<(GenerationDoc, Version)>>;

    /// Conditionally write a group's composition, `generation.json`.
    ///
    /// `version` is `None` to create — which is how a group is born, and why
    /// two replicas racing to create the same group cannot both win — and
    /// `Some` to CAS.
    async fn update_group_generation(
        &self,
        group_id: &str,
        generation: GenerationDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GenerationDoc>>;

    /// Write a generation's assignment, create-only.
    ///
    /// The write that liveness churn used to starve, now with no etag to lose:
    /// there is no read-modify-write, only a key that exactly one writer wins.
    /// See [`AssignmentOutcome`] for why finding it already there is a success.
    async fn create_group_assignment(
        &self,
        group_id: &str,
        generation_id: i32,
        assignment: AssignmentDoc,
    ) -> Result<AssignmentOutcome>;

    /// A generation's assignment, or `None` when the leader has not written it
    /// yet. Immutable once present, so a caller may memoize it forever.
    async fn read_group_assignment(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Result<Option<AssignmentDoc>>;

    /// Drop the assignments of generations below `generation_id`, returning how
    /// many were removed.
    ///
    /// Housekeeping, not correctness: assignment objects are immutable and
    /// per-generation, so they accumulate one per rebalance until something
    /// removes them. `delete_groups` remains the backstop.
    async fn delete_group_assignments_before(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Result<u64>;

    /// Create each ACL, answering one error code per creation **in request
    /// order** (#363).
    ///
    /// Creating a rule that already exists is success: `kafka-acls.sh` is run
    /// from configuration management, so re-applying the same file must not
    /// start reporting failures on the second run.
    async fn create_acls(&self, bindings: &[AclBinding]) -> Result<Vec<ErrorCode>>;

    /// Every ACL `filter` selects.
    async fn describe_acls(&self, filter: &AclFilter) -> Result<Vec<AclBinding>>;

    /// Delete every ACL each filter selects, answering what each one removed,
    /// in request order.
    ///
    /// The removed bindings rather than a count, because that is what
    /// `DeleteAcls` reports back and what an operator reads to confirm they
    /// deleted what they meant to.
    async fn delete_acls(&self, filters: &[AclFilter]) -> Result<Vec<Vec<AclBinding>>>;

    /// Apply each quota alteration, answering one error code per alteration
    /// **in request order** (#384).
    ///
    /// An entry is applied whole or not at all, as KIP-546 requires, and a key
    /// this broker does not enforce fails its entry rather than being stored
    /// and ignored — a stub that reports success is what #363 found for ACLs
    /// and #381 found again for credentials.
    ///
    /// `validate_only` checks every entry against the current document and
    /// writes nothing.
    async fn alter_client_quotas(
        &self,
        alterations: &[QuotaAlteration],
        validate_only: bool,
    ) -> Result<Vec<ErrorCode>>;

    /// Every quota entity `components` selects, with the limits stored against
    /// it.
    async fn describe_client_quotas(
        &self,
        components: &[QuotaFilterComponent],
        strict: bool,
    ) -> Result<Vec<(QuotaEntity, QuotaLimits)>>;

    /// The cluster's quotas, whole.
    ///
    /// What [`QuotaEnforcer`] snapshots. A filter would do, but the enforcer
    /// wants all of them to answer any question, and saying so here is what
    /// keeps the hot path from being expressed as a describe that matches
    /// everything.
    async fn client_quotas(&self) -> Result<Quotas>;

    /// Assert that the group layout this binary writes is the one the cluster
    /// already holds, claiming it when the cluster holds none (#359).
    ///
    /// Absent means a cluster that has never run a binary that cares, so the
    /// version is claimed with a create-only write — two replicas starting
    /// together race on the key and the loser reads what the winner claimed.
    /// A different version is a refusal to start: see [`GroupSchema`] for why
    /// a converter would be all cost and no benefit, and why a mixed fleet is
    /// the hazard worth a guard rail.
    async fn assert_group_schema(&self) -> Result<()>;

    /// Initialise a transactional or idempotent producer in this storage.
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse>;

    /// Add offsets to a transaction for a producer.
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode>;

    /// Add partitions to a transaction.
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse>;

    /// Commit an offset within a transaction.
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>>;

    /// Commit or abort a running transaction.
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode>;

    /// Run periodic maintenance on this storage.
    async fn maintain(&self, _now: SystemTime) -> Result<()> {
        Ok(())
    }

    async fn cluster_id(&self) -> Result<String>;

    async fn node(&self) -> Result<i32>;

    async fn advertised_listener(&self) -> Result<Url>;

    async fn ping(&self) -> Result<()>;
}

// The existence of this function makes the compiler catch if the Storage
// trait is "object-safe" or not.
fn _assert_trait_object(_s: &dyn Storage) {}

#[async_trait]
impl<T> Storage for Arc<T>
where
    T: Storage + ?Sized,
{
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.as_ref().register_broker(broker_registration).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.as_ref().create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.as_ref().incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.as_ref().delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.as_ref().delete_topic(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.as_ref().brokers().await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        self.as_ref().produce(transaction_id, topition, batch).await
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        self.as_ref()
            .fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.as_ref().offset_stage(topition).await
    }

    async fn offset_stage_at(
        &self,
        topition: &Topition,
        isolation: IsolationLevel,
    ) -> Result<OffsetStage> {
        self.as_ref().offset_stage_at(topition, isolation).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.as_ref().list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.as_ref()
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        self.as_ref()
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        self.as_ref().committed_offset_topitions(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.as_ref().metadata(topics).await
    }

    fn auto_create_topic_config(&self) -> AutoTopicCreate {
        self.as_ref().auto_create_topic_config()
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.as_ref()
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.as_ref()
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.as_ref().user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.as_ref().describe_config(name, resource, keys).await
    }

    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        self.as_ref().list_groups(states_filter).await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.as_ref().delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.as_ref()
            .describe_groups(group_ids, include_authorized_operations)
            .await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.as_ref()
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn write_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        member: MemberDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<MemberDoc>> {
        self.as_ref()
            .write_group_member(group_id, member_id, member, version)
            .await
    }

    async fn read_group_member(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> Result<Option<(MemberDoc, Version)>> {
        self.as_ref().read_group_member(group_id, member_id).await
    }

    async fn delete_group_member(&self, group_id: &str, member_id: &str) -> Result<()> {
        self.as_ref().delete_group_member(group_id, member_id).await
    }

    async fn list_group_members(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<String, (MemberDoc, Version)>> {
        self.as_ref().list_group_members(group_id).await
    }

    async fn read_group_generation(
        &self,
        group_id: &str,
    ) -> Result<Option<(GenerationDoc, Version)>> {
        self.as_ref().read_group_generation(group_id).await
    }

    async fn update_group_generation(
        &self,
        group_id: &str,
        generation: GenerationDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GenerationDoc>> {
        self.as_ref()
            .update_group_generation(group_id, generation, version)
            .await
    }

    async fn create_group_assignment(
        &self,
        group_id: &str,
        generation_id: i32,
        assignment: AssignmentDoc,
    ) -> Result<AssignmentOutcome> {
        self.as_ref()
            .create_group_assignment(group_id, generation_id, assignment)
            .await
    }

    async fn read_group_assignment(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Result<Option<AssignmentDoc>> {
        self.as_ref()
            .read_group_assignment(group_id, generation_id)
            .await
    }

    async fn delete_group_assignments_before(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Result<u64> {
        self.as_ref()
            .delete_group_assignments_before(group_id, generation_id)
            .await
    }

    async fn create_acls(&self, bindings: &[AclBinding]) -> Result<Vec<ErrorCode>> {
        self.as_ref().create_acls(bindings).await
    }

    async fn describe_acls(&self, filter: &AclFilter) -> Result<Vec<AclBinding>> {
        self.as_ref().describe_acls(filter).await
    }

    async fn delete_acls(&self, filters: &[AclFilter]) -> Result<Vec<Vec<AclBinding>>> {
        self.as_ref().delete_acls(filters).await
    }

    async fn alter_client_quotas(
        &self,
        alterations: &[QuotaAlteration],
        validate_only: bool,
    ) -> Result<Vec<ErrorCode>> {
        self.as_ref()
            .alter_client_quotas(alterations, validate_only)
            .await
    }

    async fn describe_client_quotas(
        &self,
        components: &[QuotaFilterComponent],
        strict: bool,
    ) -> Result<Vec<(QuotaEntity, QuotaLimits)>> {
        self.as_ref()
            .describe_client_quotas(components, strict)
            .await
    }

    async fn client_quotas(&self) -> Result<Quotas> {
        self.as_ref().client_quotas().await
    }

    async fn assert_group_schema(&self) -> Result<()> {
        self.as_ref().assert_group_schema().await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.as_ref()
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        self.as_ref()
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.as_ref().txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.as_ref().txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.as_ref()
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.as_ref().maintain(now).await
    }

    async fn cluster_id(&self) -> Result<String> {
        self.as_ref().cluster_id().await
    }

    async fn node(&self) -> Result<i32> {
        self.as_ref().node().await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.as_ref().advertised_listener().await
    }

    async fn ping(&self) -> Result<()> {
        self.as_ref().ping().await
    }
}

#[async_trait]
impl<T> Storage for Box<T>
where
    T: Storage + ?Sized,
{
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.as_ref().register_broker(broker_registration).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.as_ref().create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.as_ref().incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.as_ref().delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.as_ref().delete_topic(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.as_ref().brokers().await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        self.as_ref().produce(transaction_id, topition, batch).await
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        self.as_ref()
            .fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.as_ref().offset_stage(topition).await
    }

    async fn offset_stage_at(
        &self,
        topition: &Topition,
        isolation: IsolationLevel,
    ) -> Result<OffsetStage> {
        self.as_ref().offset_stage_at(topition, isolation).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.as_ref().list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.as_ref()
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        self.as_ref()
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        self.as_ref().committed_offset_topitions(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.as_ref().metadata(topics).await
    }

    fn auto_create_topic_config(&self) -> AutoTopicCreate {
        self.as_ref().auto_create_topic_config()
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.as_ref()
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.as_ref()
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.as_ref().user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.as_ref().describe_config(name, resource, keys).await
    }

    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        self.as_ref().list_groups(states_filter).await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.as_ref().delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.as_ref()
            .describe_groups(group_ids, include_authorized_operations)
            .await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.as_ref()
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn write_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        member: MemberDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<MemberDoc>> {
        self.as_ref()
            .write_group_member(group_id, member_id, member, version)
            .await
    }

    async fn read_group_member(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> Result<Option<(MemberDoc, Version)>> {
        self.as_ref().read_group_member(group_id, member_id).await
    }

    async fn delete_group_member(&self, group_id: &str, member_id: &str) -> Result<()> {
        self.as_ref().delete_group_member(group_id, member_id).await
    }

    async fn list_group_members(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<String, (MemberDoc, Version)>> {
        self.as_ref().list_group_members(group_id).await
    }

    async fn read_group_generation(
        &self,
        group_id: &str,
    ) -> Result<Option<(GenerationDoc, Version)>> {
        self.as_ref().read_group_generation(group_id).await
    }

    async fn update_group_generation(
        &self,
        group_id: &str,
        generation: GenerationDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GenerationDoc>> {
        self.as_ref()
            .update_group_generation(group_id, generation, version)
            .await
    }

    async fn create_group_assignment(
        &self,
        group_id: &str,
        generation_id: i32,
        assignment: AssignmentDoc,
    ) -> Result<AssignmentOutcome> {
        self.as_ref()
            .create_group_assignment(group_id, generation_id, assignment)
            .await
    }

    async fn read_group_assignment(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Result<Option<AssignmentDoc>> {
        self.as_ref()
            .read_group_assignment(group_id, generation_id)
            .await
    }

    async fn delete_group_assignments_before(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Result<u64> {
        self.as_ref()
            .delete_group_assignments_before(group_id, generation_id)
            .await
    }

    async fn create_acls(&self, bindings: &[AclBinding]) -> Result<Vec<ErrorCode>> {
        self.as_ref().create_acls(bindings).await
    }

    async fn describe_acls(&self, filter: &AclFilter) -> Result<Vec<AclBinding>> {
        self.as_ref().describe_acls(filter).await
    }

    async fn delete_acls(&self, filters: &[AclFilter]) -> Result<Vec<Vec<AclBinding>>> {
        self.as_ref().delete_acls(filters).await
    }

    async fn alter_client_quotas(
        &self,
        alterations: &[QuotaAlteration],
        validate_only: bool,
    ) -> Result<Vec<ErrorCode>> {
        self.as_ref()
            .alter_client_quotas(alterations, validate_only)
            .await
    }

    async fn describe_client_quotas(
        &self,
        components: &[QuotaFilterComponent],
        strict: bool,
    ) -> Result<Vec<(QuotaEntity, QuotaLimits)>> {
        self.as_ref()
            .describe_client_quotas(components, strict)
            .await
    }

    async fn client_quotas(&self) -> Result<Quotas> {
        self.as_ref().client_quotas().await
    }

    async fn assert_group_schema(&self) -> Result<()> {
        self.as_ref().assert_group_schema().await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.as_ref()
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        self.as_ref()
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.as_ref().txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.as_ref().txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.as_ref()
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.as_ref().maintain(now).await
    }

    async fn cluster_id(&self) -> Result<String> {
        self.as_ref().cluster_id().await
    }

    async fn node(&self) -> Result<i32> {
        self.as_ref().node().await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.as_ref().advertised_listener().await
    }

    async fn ping(&self) -> Result<()> {
        self.as_ref().ping().await
    }
}

pub type DynStorage = dyn Storage;
pub type ArcDynStorage = Arc<DynStorage>;

/// Conditional Update Errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum UpdateError<T> {
    Error(#[from] Error),

    MissingEtag,

    Outdated {
        current: Box<T>,
        version: Version,
    },

    /// The conditional write lost, and the object it lost to was **gone** by the
    /// time the winner's value was read back (#431).
    ///
    /// Distinct from [`Self::Outdated`] on purpose, and the distinction is not
    /// cosmetic. `Outdated` says *"a peer holds a different value, here it is"*;
    /// this says *"there is no value"*, and a caller that acts on the two the
    /// same way is wrong in at least one place. `assert_group_schema` is the
    /// proof: folding this into `Outdated` with a defaulted document would hand
    /// it `GroupSchema { version: 0 }` and it would **refuse the cluster's group
    /// layout** over a document nobody wrote.
    ///
    /// For a CAS retry loop the answer is almost always the same as `Outdated` —
    /// re-read and re-apply, which now takes `PutMode::Create` — but it has to
    /// be said at each call site rather than assumed, which is why this is a
    /// variant and not a defaulted value.
    Vanished,

    SerdeJson(Arc<serde_json::Error>),

    Uuid(#[from] uuid::Error),
}

#[cfg(feature = "dynostore")]
impl<T> From<object_store::Error> for UpdateError<T> {
    fn from(value: object_store::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

impl<T> From<serde_json::Error> for UpdateError<T> {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeJson(Arc::new(value))
    }
}

/// The entry point to [`Builder`], and nothing else (#279).
///
/// This was an enum of `Null` / `DynoStore` with a hand-written ~750-line
/// `Storage` impl mirroring the whole trait. Neither variant was ever
/// constructed: `build()` returns the concrete engine behind
/// `Arc<Box<dyn Storage>>`, so the impl was unreachable and its per-method
/// request/error counters were never emitted — while two Grafana panels queried
/// them and read a flat zero, which is how an operator concluded there was no
/// storage traffic on a broker serving ~14.7k topics.
///
/// Deleted rather than made real. Making it real would have kept a full-trait
/// impl that has to be updated by hand on every trait change, which is exactly
/// the defect #273 found: two wrappers silently failed to delegate two methods
/// and two shipped optimisations were inert in production for an unknown period.
/// `dead_code` cannot fire on a reachable-looking impl.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageContainer;

impl StorageContainer {
    pub fn builder() -> PhantomBuilder {
        PhantomBuilder::default()
    }
}

/// A [`StorageContainer`] builder
#[derive(Clone, Debug, Default)]
pub struct Builder<N, C, A, S> {
    node_id: N,
    cluster_id: C,
    advertised_listener: A,
    storage: S,
    silent: bool,

    /// Broker-level topic config defaults, handed to the engine so they are
    /// applied at the single creation choke point (#225).
    topic_defaults: TopicDefaults,
}

type PhantomBuilder =
    Builder<PhantomData<i32>, PhantomData<String>, PhantomData<Url>, PhantomData<Url>>;

impl<N, C, A, S> Builder<N, C, A, S> {
    pub fn node_id(self, node_id: i32) -> Builder<i32, C, A, S> {
        Builder {
            node_id,
            cluster_id: self.cluster_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            silent: self.silent,
            topic_defaults: self.topic_defaults,
        }
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> Builder<N, String, A, S> {
        Builder {
            node_id: self.node_id,
            cluster_id: cluster_id.into(),
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            silent: self.silent,
            topic_defaults: self.topic_defaults,
        }
    }

    pub fn advertised_listener(self, advertised_listener: impl Into<Url>) -> Builder<N, C, Url, S> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            advertised_listener: advertised_listener.into(),
            storage: self.storage,
            silent: self.silent,
            topic_defaults: self.topic_defaults,
        }
    }

    pub fn storage(self, storage: Url) -> Builder<N, C, A, Url> {
        debug!(%storage);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            advertised_listener: self.advertised_listener,
            storage,
            silent: self.silent,
            topic_defaults: self.topic_defaults,
        }
    }

    pub fn silent(self, silent: bool) -> Self {
        Self { silent, ..self }
    }

    /// The broker-level topic config defaults the engine injects into every topic
    /// it creates (#225).
    pub fn topic_defaults(self, topic_defaults: TopicDefaults) -> Self {
        Self {
            topic_defaults,
            ..self
        }
    }
}

/// The largest record batch this broker accepts, from the storage URL query
/// string (`?message_max_bytes=1MiB`), falling back to Kafka's own default
/// (#443).
///
/// A key here rather than a broker CLI flag, alongside `auto_create_topics` and
/// the `coalesce_*` keys, because that is where this deployment's engine
/// behaviour is already configured — and because the limit is enforced by the
/// engine, at the single write choke point, rather than by the protocol layer.
///
/// An unparseable value keeps the default and says so: a size limit that
/// silently became something else is worse than one that was ignored.
#[cfg(feature = "dynostore")]
fn message_max_bytes(storage: &Url) -> usize {
    storage
        .query_pairs()
        .find(|(k, _)| k == "message_max_bytes")
        .and_then(|(_, v)| {
            human_units::Size::from_str(v.as_ref())
                .map(|size| size.0)
                .inspect_err(|err| warn!(%storage, value = v.as_ref(), ?err))
                .ok()
                .and_then(|size| usize::try_from(size).ok())
        })
        .unwrap_or(DynoStore::MESSAGE_MAX_BYTES)
}

/// Parse the auto-topic-creation policy from the storage URL query string
/// (`?auto_create_topics=false&num_partitions=3&default_replication_factor=2`),
/// falling back to [`AutoTopicCreate::default`] for any absent or unparseable key.
#[cfg(feature = "dynostore")]
fn auto_topic_create(storage: &Url) -> AutoTopicCreate {
    let mut config = AutoTopicCreate::default();

    for (key, value) in storage.query_pairs() {
        match key.as_ref() {
            "auto_create_topics" => {
                if let Ok(enable) = value.parse() {
                    config.enable = enable;
                }
            }
            "num_partitions" => {
                if let Ok(num_partitions) = value.parse() {
                    config.num_partitions = num_partitions;
                }
            }
            "default_replication_factor" => {
                if let Ok(replication_factor) = value.parse() {
                    config.replication_factor = replication_factor;
                }
            }
            _ => {}
        }
    }

    config
}

/// Warn once, at store build, for storage-URL keys that selected the pre-#177
/// layout.
///
/// Prefix-coalesced segments under the leaseless arbiter are the only layout
/// now, so `prefix_coalesce`, `prefix_leaseless` and `produce_coalesce` no
/// longer select anything. They are *ignored*, not rejected: production URLs
/// pass them explicitly, and failing the build on an unknown key would turn a
/// no-op config line into a failed rollout. The warning exists so an operator
/// finds out from a log line rather than from a behaviour change that never
/// comes.
#[cfg(feature = "dynostore")]
fn warn_deprecated_layout_flags(storage: &Url) {
    const DEPRECATED: [&str; 7] = [
        "prefix_coalesce",
        "prefix_leaseless",
        "produce_coalesce",
        "compacted_segments",
        "compacted_carryover",
        // Inert since #178, parsed-but-ignored until #227: the per-pod
        // `producers/{id}.json` debounce they tuned (#48) died with the legacy
        // write path, and idempotent dedup is now the segment flush's folded
        // `ProducerTable` (#88), which has no debounce to tune.
        "producer_checkpoint_interval",
        "producer_checkpoint_batches",
    ];

    let present: Vec<&str> = DEPRECATED
        .into_iter()
        .filter(|key| storage.query_pairs().any(|(k, _)| k == *key))
        .collect();

    if !present.is_empty() {
        warn!(
            keys = ?present,
            "ignoring deprecated storage URL keys: prefix-coalesced segments are the only layout (#177)"
        );
    }
}

#[cfg(feature = "dynostore")]
fn coalesce_tuning(storage: &Url) -> CoalesceTuning {
    let mut tuning = CoalesceTuning::default();

    for (key, value) in storage.query_pairs() {
        let value = value.as_ref();
        match key.as_ref() {
            "coalesce_linger" => {
                tuning.coalesce_linger = human_units::Duration::from_str(value)
                    .map(|duration| duration.0)
                    .inspect_err(
                        |err| warn!(storage = %storage, key = "coalesce_linger", value, ?err),
                    )
                    .ok();
            }
            "coalesce_batches" => {
                tuning.coalesce_batches = value
                    .parse()
                    .inspect_err(
                        |err| warn!(storage = %storage, key = "coalesce_batches", value, ?err),
                    )
                    .ok();
            }
            "coalesce_bytes" => {
                tuning.coalesce_bytes = human_units::Size::from_str(value)
                    .map(|size| size.0)
                    .inspect_err(
                        |err| warn!(storage = %storage, key = "coalesce_bytes", value, ?err),
                    )
                    .ok()
                    .and_then(|size| usize::try_from(size).ok());
            }
            "prefix_compact_min_segments" => {
                tuning.prefix_compact_min_segments = value
                    .parse()
                    .inspect_err(|err| warn!(storage = %storage, key = "prefix_compact_min_segments", value, ?err))
                    .ok();
            }
            "prefix_compact_target_bytes" => {
                tuning.prefix_compact_target_bytes = human_units::Size::from_str(value)
                    .map(|size| size.0)
                    .inspect_err(|err| warn!(storage = %storage, key = "prefix_compact_target_bytes", value, ?err))
                    .ok()
                    .and_then(|size| usize::try_from(size).ok());
            }
            "prefix_compact_keep_hot" => {
                tuning.prefix_compact_keep_hot = value
                    .parse()
                    .inspect_err(|err| warn!(storage = %storage, key = "prefix_compact_keep_hot", value, ?err))
                    .ok();
            }
            "prefix_compact_seen_keys" => {
                tuning.prefix_compact_seen_keys = value
                    .parse()
                    .inspect_err(|err| warn!(storage = %storage, key = "prefix_compact_seen_keys", value, ?err))
                    .ok();
            }
            "maintenance_recency" => {
                tuning.maintenance_recency = human_units::Duration::from_str(value)
                    .map(|duration| duration.0)
                    .inspect_err(
                        |err| warn!(storage = %storage, key = "maintenance_recency", value, ?err),
                    )
                    .ok();
            }
            "flush_max_elapsed" => {
                tuning.flush_max_elapsed = human_units::Duration::from_str(value)
                    .map(|duration| duration.0)
                    .inspect_err(
                        |err| warn!(storage = %storage, key = "flush_max_elapsed", value, ?err),
                    )
                    .ok();
            }
            _ => {}
        }
    }

    debug!(?tuning);
    tuning
}

impl Builder<i32, String, Url, Url> {
    pub async fn build(self) -> Result<Arc<Box<dyn Storage>>> {
        let storage = match self.storage.scheme() {
            #[cfg(feature = "dynostore")]
            "s3" => {
                use crate::batch::ProduceRequestBatcher;

                let bucket_name = self.storage.host_str().unwrap_or("tansu");

                let minimum_size = self.storage.query_pairs().find_map(|(k, v)| {
                    if k == "batch_min_size" {
                        human_units::Size::from_str(v.as_ref())
                            .map(|size| size.0)
                            .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                            .ok()
                            .and_then(|size| usize::try_from(size).ok())
                    } else {
                        None
                    }
                });

                let maximum_delay = self.storage.query_pairs().find_map(|(k, v)| {
                    if k == "batch_max_delay" {
                        human_units::Duration::from_str(v.as_ref())
                            .map(|duration| duration.0)
                            .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                            .ok()
                    } else {
                        None
                    }
                });

                warn_deprecated_layout_flags(&self.storage);

                // Compacted topics segment-routed to dedicated prefixes with a
                // per-key compaction pass (#175). Off by default; flipped only
                debug!(?minimum_size, ?maximum_delay);

                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    // S3 enforces a per-prefix request-rate limit (~3,500
                    // mutating req/prefix/sec). A hot topic-partition with many
                    // tiny batch objects, plus the periodic `maintain` delete
                    // flood, trips `503 SlowDown`; the object_store default of
                    // 10 retries over 180s isn't enough and deletes/produces are
                    // dropped (#5, #6). Give throttles a longer, gentler ceiling
                    // so they ride out a SlowDown burst instead of failing.
                    .with_retry(RetryConfig {
                        backoff: BackoffConfig {
                            init_backoff: Duration::from_millis(200),
                            max_backoff: Duration::from_secs(30),
                            base: 2.0,
                        },
                        max_retries: 32,
                        retry_timeout: Duration::from_secs(300),
                    })
                    .build()
                    .map(|object_store| {
                        DynoStore::new(self.cluster_id.as_str(), self.node_id, object_store)
                            .advertised_listener(self.advertised_listener.clone())
                            .auto_create(auto_topic_create(&self.storage))
                            .topic_defaults(self.topic_defaults.clone())
                            .coalesce_tuning(coalesce_tuning(&self.storage))
                            .message_max_bytes(message_max_bytes(&self.storage))
                    })
                    .map(|storage| {
                        ProduceRequestBatcher::new(storage)
                            .with_minimum_size(minimum_size)
                            .with_maximum_delay(maximum_delay)
                    })
                    .map(|storage| Box::new(storage) as Box<dyn Storage>)
                    .map(Arc::new)
                    .map_err(Into::into)
            }

            #[cfg(feature = "dynostore")]
            "gs" => {
                use std::num::NonZeroU32;

                use object_store::gcp::GoogleCloudStorageBuilder;

                use crate::{batch::ProduceRequestBatcher, gcs::limit::PutRateLimiter};

                let bucket_name = self.storage.host_str().unwrap_or("tansu");

                let minimum_size = self.storage.query_pairs().find_map(|(k, v)| {
                    if k == "batch_min_size" {
                        human_units::Size::from_str(v.as_ref())
                            .map(|size| size.0)
                            .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                            .ok()
                            .and_then(|size| usize::try_from(size).ok())
                    } else {
                        None
                    }
                });

                let maximum_delay = self.storage.query_pairs().find_map(|(k, v)| {
                    if k == "batch_max_delay" {
                        human_units::Duration::from_str(v.as_ref())
                            .map(|duration| duration.0)
                            .inspect_err(|err| warn!(storage = %self.storage, v = v.as_ref(), ?err))
                            .ok()
                    } else {
                        None
                    }
                });

                warn_deprecated_layout_flags(&self.storage);

                GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket_name)
                    // GCS caps updates to a single object at ~1 write/second; an
                    // over-the-cap conditional update returns `429 rateLimitExceeded`.
                    // The object_store default RetryConfig (10 retries over 180s) backs
                    // off *silently*, which is exactly the "30s produce latency with zero
                    // log lines" reported in #13. Bound the budget so a throttled object
                    // fails fast instead of hanging tens of seconds. `PutMode::Create`
                    // conflicts are NOT retried here — they surface as `AlreadyExists`
                    // and are resolved by the dynostore offset-assignment loop.
                    .with_retry(RetryConfig {
                        backoff: BackoffConfig {
                            init_backoff: Duration::from_millis(100),
                            max_backoff: Duration::from_secs(3),
                            base: 2.0,
                        },
                        max_retries: 5,
                        retry_timeout: Duration::from_secs(15),
                    })
                    .build()
                    .map(|object_store| {
                        PutRateLimiter::new(object_store, Duration::from_mins(5))
                            .with_rate_per_second(NonZeroU32::new(1))
                            .with_jitter(Some(Duration::from_millis(50)))
                    })
                    .map(|object_store| {
                        DynoStore::new(self.cluster_id.as_str(), self.node_id, object_store)
                            .advertised_listener(self.advertised_listener.clone())
                            .auto_create(auto_topic_create(&self.storage))
                            .topic_defaults(self.topic_defaults.clone())
                            .coalesce_tuning(coalesce_tuning(&self.storage))
                            .message_max_bytes(message_max_bytes(&self.storage))
                    })
                    .map(|storage| {
                        ProduceRequestBatcher::new(storage)
                            .with_minimum_size(minimum_size)
                            .with_maximum_delay(maximum_delay)
                    })
                    .map(|storage| Box::new(storage) as Box<dyn Storage>)
                    .map(Arc::new)
                    .map_err(Into::into)
            }

            #[cfg(feature = "dynostore")]
            "memory" => Ok(
                DynoStore::new(self.cluster_id.as_str(), self.node_id, InMemory::new())
                    .advertised_listener(self.advertised_listener.clone())
                    .auto_create(auto_topic_create(&self.storage))
                    .topic_defaults(self.topic_defaults.clone())
                    .coalesce_tuning(coalesce_tuning(&self.storage))
                    .message_max_bytes(message_max_bytes(&self.storage)),
            )
            .map(|storage| Box::new(storage) as Box<dyn Storage>)
            .map(Arc::new),

            "null" => Ok(null::Engine::new(
                self.cluster_id.clone(),
                self.node_id,
                self.advertised_listener.clone(),
            ))
            .map(|storage| Box::new(storage) as Box<dyn Storage>)
            .map(Arc::new),

            _unsupported => Err(Error::UnsupportedStorageUrl(self.storage.clone())),
        }?;

        let pb = if self.silent {
            None
        } else {
            let pb = ProgressBar::new(1);
            pb.set_style(
                ProgressStyle::with_template("[{elapsed}] {bar:40.cyan/blue} {msg}")
                    .unwrap()
                    .progress_chars("##-"),
            );

            pb.set_message("connecting to storage");

            Some(pb)
        };

        storage.ping().await?;

        if let Some(pb) = pb {
            pb.inc(1);
            pb.finish_with_message(format!("{} connected to storage", Emoji("✅", ""),));
        }

        Ok(storage)
    }
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

/// Requests parked in a long poll: waiting on a clock, not on this replica
/// (#362).
///
/// The same instrument as `tansu-service`'s, deliberately: the two places a
/// request parks are in two crates that do not depend on each other — the fetch
/// poll is here, the group polls are in the broker. OpenTelemetry keys an
/// instrument by its scope as well as its name, so both arrive and neither
/// overwrites the other; the documented scaler expression `sum()`s over them,
/// which it would do across replicas anyway.
///
/// Adding a crate dependency to share one counter is the alternative, and it is
/// the worse trade: this crate is the storage engine and knows nothing about
/// serving requests.
pub(crate) static REQUESTS_PARKED: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    METER
        .i64_up_down_counter("tansu_requests_parked")
        .with_description("API requests parked in a long poll, waiting rather than working")
        .build()
});

/// A request parked in a long poll, counted for as long as this value lives.
///
/// RAII, for the reason every counter of this shape is: an up-down counter that
/// leaks an increment does not decay, so one missed decrement is a permanent
/// lie about how loaded this replica is and a scaler built on it never scales
/// down again.
#[derive(Debug)]
pub(crate) struct Parked;

impl Parked {
    pub(crate) fn enter() -> Self {
        REQUESTS_PARKED.add(1, &[]);
        Self
    }
}

impl Drop for Parked {
    fn drop(&mut self) {
        REQUESTS_PARKED.add(-1, &[]);
    }
}

/// What a SCRAM handshake is checked against.
///
/// Never the password, and never anything a password can be recovered from:
/// PBKDF2 over the salt gives a salted password, `stored_key` is a further hash
/// of the client key derived from it, and `server_key` is what lets the client
/// verify the broker in turn. The client proves it knows the password without
/// sending it, and this document is what the broker needs to check that proof.
///
/// It is still the whole of a principal's authentication: anyone who can read
/// it can impersonate the user to a broker. It is written under the cluster
/// prefix in the object store, so **the bucket's own access control is what
/// keeps it**, exactly as it is for the committed offsets beside it.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ScramCredential {
    pub salt: Bytes,
    pub iterations: i32,
    pub stored_key: Bytes,
    pub server_key: Bytes,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn formed_group_with_assignment(
        member_id: &str,
        subscription: &[u8],
        assignment: &[u8],
    ) -> GroupDetail {
        GroupDetail {
            generation_id: 7,
            members: [(
                member_id.to_owned(),
                GroupMember {
                    join_response: JoinGroupResponseMember::default()
                        .member_id(member_id.into())
                        .group_instance_id(Some("inst-1".into()))
                        .metadata(Bytes::copy_from_slice(subscription)),
                    last_contact: None,
                },
            )]
            .into(),
            state: GroupState::Formed {
                protocol_type: "consumer".into(),
                protocol_name: "range".into(),
                leader: member_id.to_owned(),
                assignments: [(member_id.to_owned(), Bytes::copy_from_slice(assignment))].into(),
            },
            ..Default::default()
        }
    }

    /// #215: `DescribeGroups` reports the assignment the coordinator handed out.
    ///
    /// It was a zero-length buffer for every member. `rpk`,
    /// `kafka-consumer-groups.sh` and the AdminClient all derive a group's
    /// partition set from that field and then issue `OffsetFetch` for it, so an
    /// empty assignment means committed offsets are never read and lag computes
    /// to 0 — for every group, healthy or wedged. A monitoring stack built on it
    /// showed all-green through a multi-day consumption outage.
    #[test]
    fn describe_groups_reports_the_assignment_and_subscription() {
        let described = describe_groups_response::DescribedGroup::from(&NamedGroupDetail::found(
            "g1".into(),
            formed_group_with_assignment("m-1", b"subscription-bytes", b"assignment-bytes"),
        ));

        let members = described.members.expect("members");
        assert_eq!(1, members.len());

        assert_eq!(
            Bytes::from_static(b"assignment-bytes"),
            members[0].member_assignment,
            "the assignment must be surfaced, not an empty buffer",
        );
        assert_eq!(
            Bytes::from_static(b"subscription-bytes"),
            members[0].member_metadata,
            "and so must the subscription the member joined with",
        );
        assert_eq!(Some("inst-1".to_owned()), members[0].group_instance_id);
        assert_eq!("Stable", described.group_state.as_str());
        assert_eq!("range", described.protocol_data.as_str());
    }

    /// A group mid-rebalance with a leader elected is `CompletingRebalance`, not
    /// `Unknown` (#215).
    ///
    /// That state was the only route to `Unknown`, so an actively-consuming group
    /// that rebalances often was reported as though the broker knew nothing about
    /// it — indistinguishable from a group that is genuinely gone.
    #[test]
    fn a_group_awaiting_sync_group_is_completing_rebalance() {
        let awaiting_sync = GroupDetail {
            members: [("m-1".to_owned(), GroupMember::default())].into(),
            state: GroupState::Forming {
                protocol_type: Some("consumer".into()),
                protocol_name: Some("range".into()),
                leader: Some("m-1".into()),
            },
            ..Default::default()
        };

        assert_eq!(
            ConsumerGroupState::CompletingRebalance,
            ConsumerGroupState::from(&awaiting_sync),
        );

        // The two neighbouring states are unchanged.
        let no_leader = GroupDetail {
            members: [("m-1".to_owned(), GroupMember::default())].into(),
            state: GroupState::Forming {
                protocol_type: None,
                protocol_name: None,
                leader: None,
            },
            ..Default::default()
        };
        assert_eq!(
            ConsumerGroupState::Assigning,
            ConsumerGroupState::from(&no_leader),
        );
        assert_eq!(
            ConsumerGroupState::Empty,
            ConsumerGroupState::from(&GroupDetail::default()),
        );
    }

    /// A classic group is reported `GROUP_ID_NOT_FOUND` on the KIP-848 API, so
    /// that an AdminClient falls back to `DescribeGroups` — which is the path
    /// that carries the members and their assignments. Answering `NONE` with an
    /// empty member list is believed, and reports a healthy group as empty.
    #[test]
    fn consumer_group_describe_sends_a_classic_group_to_the_classic_api() {
        let described =
            consumer_group_describe_response::DescribedGroup::from(&NamedGroupDetail::found(
                "g1".into(),
                formed_group_with_assignment("m-1", b"sub", b"assign"),
            ));

        assert_eq!(i16::from(ErrorCode::GroupIdNotFound), described.error_code);
        assert_eq!("g1", described.group_id);
    }

    /// The classic path is the one that must carry the membership, precisely
    /// because the KIP-848 path above sends every client to it.
    #[test]
    fn describe_groups_carries_the_members_and_their_assignments() {
        let described = describe_groups_response::DescribedGroup::from(&NamedGroupDetail::found(
            "g1".into(),
            formed_group_with_assignment("m-1", b"sub", b"assign"),
        ));

        assert_eq!(i16::from(ErrorCode::None), described.error_code);

        let members = described.members.unwrap_or_default();
        assert_eq!(1, members.len());
        assert_eq!("m-1", members[0].member_id);
        assert_eq!(b"assign".to_vec(), members[0].member_assignment);
    }

    #[test]
    fn topition_from_str() -> Result<()> {
        let topition = Topition::from_str("qwerty-2147483647")?;
        assert_eq!("qwerty", topition.topic());
        assert_eq!(i32::MAX, topition.partition());
        Ok(())
    }

    #[test]
    fn topic_with_dashes_in_name() -> Result<()> {
        let topition = Topition::from_str("test-topic-0000000-eFC79C8-2147483647")?;
        assert_eq!("test-topic-0000000-eFC79C8", topition.topic());
        assert_eq!(i32::MAX, topition.partition());
        Ok(())
    }

    /// The pre-#177 layout keys are inert, not rejected: production URLs pass
    /// them explicitly, so failing the build on one would turn a no-op config
    /// line into a failed rollout. They are warned about once at store build.
    /// `memory://` with no host is the URL the broker's in-memory tests use;
    /// it must resolve to the in-memory store rather than falling through to
    /// `UnsupportedStorageUrl`.
    #[cfg(feature = "dynostore")]
    #[tokio::test]
    async fn hostless_memory_url_builds() -> Result<()> {
        let storage = StorageContainer::builder()
            .cluster_id("tansu")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://localhost:9092")?)
            .storage(Url::parse("memory://")?)
            .build()
            .await;
        assert!(
            storage.is_ok(),
            "hostless memory:// must build: {storage:?}"
        );
        Ok(())
    }

    #[cfg(feature = "dynostore")]
    #[tokio::test]
    async fn deprecated_layout_flags_still_build() -> Result<()> {
        let storage = StorageContainer::builder()
            .cluster_id("tansu")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://localhost:9092")?)
            .storage(Url::parse(
                "memory://tansu/\
                 ?prefix_coalesce=true&prefix_leaseless=true&produce_coalesce=false",
            )?)
            .build()
            .await;

        assert!(storage.is_ok(), "deprecated keys must not fail the build");
        Ok(())
    }

    /// #227: the removed producer-checkpoint keys must not fail a build either.
    ///
    /// They were parsed-but-ignored from #178, so a deployment carrying them is
    /// expected to exist. Dropping the parse arms without adding them to the
    /// deprecated list would turn a no-op config line into a failed rollout, which
    /// is the trade #177 and #222 already made for the layout flags.
    #[cfg(feature = "dynostore")]
    #[tokio::test]
    async fn removed_producer_checkpoint_keys_still_build() -> Result<()> {
        let storage = StorageContainer::builder()
            .cluster_id("tansu")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://localhost:9092")?)
            .storage(Url::parse(
                "memory://tansu/?producer_checkpoint_interval=5s&producer_checkpoint_batches=256",
            )?)
            .build()
            .await;

        assert!(
            storage.is_ok(),
            "a URL carrying the removed keys must still start"
        );
        Ok(())
    }

    #[cfg(feature = "dynostore")]
    #[test]
    fn coalesce_tuning_parses_compaction_keys() -> Result<()> {
        let tuning = coalesce_tuning(&Url::parse(
            "s3://tansu/?prefix_compact_min_segments=512&prefix_compact_target_bytes=128m&prefix_compact_keep_hot=32",
        )?);
        assert_eq!(Some(512), tuning.prefix_compact_min_segments);
        assert_eq!(Some(128 * 1024 * 1024), tuning.prefix_compact_target_bytes);
        assert_eq!(Some(32), tuning.prefix_compact_keep_hot);
        // Absent keys stay None (compile-time defaults).
        let empty = coalesce_tuning(&Url::parse("s3://tansu/")?);
        assert_eq!(None, empty.prefix_compact_min_segments);
        Ok(())
    }

    #[cfg(feature = "dynostore")]
    #[test]
    fn coalesce_tuning_absent_keys_keep_defaults() -> Result<()> {
        let tuning = coalesce_tuning(&Url::parse("s3://tansu/?produce_coalesce=true")?);
        assert_eq!(None, tuning.coalesce_linger);
        assert_eq!(None, tuning.coalesce_batches);
        assert_eq!(None, tuning.coalesce_bytes);
        Ok(())
    }

    #[cfg(feature = "dynostore")]
    #[test]
    fn coalesce_tuning_parses_all_keys() -> Result<()> {
        let tuning = coalesce_tuning(&Url::parse(
            "s3://tansu/?coalesce_linger=300ms&coalesce_batches=128&coalesce_bytes=4M",
        )?);
        assert_eq!(Some(Duration::from_millis(300)), tuning.coalesce_linger);
        assert_eq!(Some(128), tuning.coalesce_batches);
        assert_eq!(Some(4 << 20), tuning.coalesce_bytes);
        Ok(())
    }

    #[cfg(feature = "dynostore")]
    #[test]
    fn coalesce_tuning_unparseable_value_is_ignored() -> Result<()> {
        let tuning = coalesce_tuning(&Url::parse(
            "s3://tansu/?coalesce_linger=not-a-duration&coalesce_batches=lots",
        )?);
        assert_eq!(None, tuning.coalesce_linger);
        assert_eq!(None, tuning.coalesce_batches);
        Ok(())
    }
}
