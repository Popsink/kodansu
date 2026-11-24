// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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
//
//! Where Kodansu keeps its data, behind one [`Storage`] trait.
//!
//! [`StorageContainer::builder`] picks the engine from the storage URL's
//! scheme: `memory://` for tests and anything ephemeral, `s3://` for
//! [S3](https://en.wikipedia.org/wiki/Amazon_S3) or MinIO, and `gs://` for
//! Google Cloud Storage.
//!
//! The PostgreSQL, libSQL and Turso backends this used to advertise were removed
//! in #96; their examples went with this doc's rewrite (#279).
//!
//! ## Memory
//!
//! ```
//! use tansu_storage::{Error, StorageContainer};
//! use url::Url;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let listener = Url::parse("tcp://localhost:9092")?;
//! let _storage = StorageContainer::builder()
//!     .cluster_id("tansu")
//!     .node_id(111)
//!     .advertised_listener(listener)
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
//! use tansu_storage::{Error, StorageContainer};
//! use url::Url;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let listener = Url::parse("tcp://localhost:9092")?;
//! let _storage = StorageContainer::builder()
//!     .cluster_id("tansu")
//!     .node_id(111)
//!     .advertised_listener(listener)
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
use dynostore::{CoalesceTuning, SEGMENT_FORMAT_WRITABLE};
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

use std::array::TryFromSliceError;
use std::collections::BTreeMap;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::num::{ParseIntError, TryFromIntError};
use std::pin::Pin;
use std::result;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, PoisonError};
use std::time::{Duration, SystemTime, SystemTimeError};

use governor::InsufficientCapacity;
use opentelemetry::metrics::{Counter, Meter, UpDownCounter};
use opentelemetry::{InstrumentationScope, global};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use serde::{Deserialize, Serialize};
use tansu_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnRequest;
use tansu_sans_io::add_partitions_to_txn_response::{
    AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult,
};
use tansu_sans_io::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use tansu_sans_io::delete_groups_response::DeletableGroupResult;
use tansu_sans_io::delete_records_request::DeleteRecordsTopic;
use tansu_sans_io::delete_records_response::DeleteRecordsTopicResult;
use tansu_sans_io::delete_topics_request::DeleteTopicState;
use tansu_sans_io::describe_cluster_response::DescribeClusterBroker;
use tansu_sans_io::describe_configs_response::DescribeConfigsResult;
use tansu_sans_io::describe_topic_partitions_request::{Cursor, TopicRequest};
use tansu_sans_io::describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic;
use tansu_sans_io::fetch_request::FetchTopic;
use tansu_sans_io::incremental_alter_configs_request::AlterConfigsResource;
use tansu_sans_io::incremental_alter_configs_response::AlterConfigsResourceResponse;
use tansu_sans_io::join_group_response::JoinGroupResponseMember;
use tansu_sans_io::list_groups_response::ListedGroup;
use tansu_sans_io::metadata_request::MetadataRequestTopic;
use tansu_sans_io::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};
use tansu_sans_io::offset_commit_request::OffsetCommitRequestPartition;
use tansu_sans_io::record::deflated;
use tansu_sans_io::txn_offset_commit_request::TxnOffsetCommitRequestTopic;
use tansu_sans_io::txn_offset_commit_response::TxnOffsetCommitResponseTopic;
use tansu_sans_io::{AddPartitionsToTxnResponse, ConfigResource, ErrorCode, IsolationLevel};
use tansu_sans_io::{ListOffset, NULL_TOPIC_ID};
use tansu_sans_io::{ScramMechanism, consumer_group_describe_response, describe_groups_response};
use tansu_sans_io::{to_system_time, to_timestamp};
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

use crate::delegate::storage_methods;
mod backend;
mod batch;
mod consumer_group;
mod delegate;
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

// `pub` so that the `conditional_put` conformance target — the third builder,
// and an integration test rather than a module — routes through the same table
// as the broker and the audit (#531).
pub use backend::Backend;

pub use authorizer::{
    ACL_SNAPSHOT_TTL, Authorizer, NO_AUTHORIZER, NO_AUTHORIZER_DESCRIBE, Requester, authorized,
    authorized_cluster, enforcing,
};

pub use enforcer::{Charge, MAX_THROTTLE, QUOTA_SNAPSHOT_TTL, QuotaEnforcer};

pub use quota::{
    CONSUMER_BYTE_RATE, PRODUCER_BYTE_RATE, QUOTA_KEYS, QuotaAlteration, QuotaEntity,
    QuotaFilterComponent, QuotaKeyError, QuotaLimits, QuotaMatch, QuotaOp, Quotas, REQUEST_RATE,
    USER_ENTITY, user_of,
};

pub use consumer_group::{
    CONSUMER_HEARTBEAT_INTERVAL_MS, CONSUMER_SESSION_TIMEOUT_MS, ClassicMember, ConsumerGroupDoc,
    ConsumerMember, ContactStamps, LEAVING_STATIC_MEMBER_EPOCH, MemberState, Partitions,
    SubscribedTopic, flatten, grouped,
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
    ListPartitionReassignmentsService, MetadataService, OnAdmitted, ProduceService,
    TxnAddOffsetsService, TxnAddPartitionService, TxnEndService, TxnOffsetCommitService,
};

#[cfg(feature = "dynostore")]
mod azure;

#[cfg(feature = "dynostore")]
mod gcs;

// The `gs` arm's per-object write cap, exported for the same reason
// `LatencyIntroducingStorage` is: it is a *local* delay, so composing it over
// `InMemory` reproduces GCS's one-write-per-second-per-object exactly, and the
// test that has to see it is the group formation one in `tansu-broker` (#427).
#[cfg(feature = "dynostore")]
pub use gcs::limit::PutRateLimiter;

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

    /// The object store behind the storage URL does not honour the
    /// conditional writes the broker is built on (#597), so the store is not
    /// built. `reason` names the header that was ignored or the error the probe
    /// got instead, and how to proceed — see `DynoStore::conditional_put_probed`.
    ///
    /// Fatal on purpose. A store that ignores `If-None-Match: *` or `If-Match`
    /// does not error, it overwrites: produce, fetch and `ping()` all work, and
    /// the damage is duplicated offsets and lost group state under concurrency.
    ConditionalPutRefused {
        scheme: String,
        reason: String,
    },

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

    /// The coalescing prefix shape on the storage URL (`prefix_depth` /
    /// `prefix_separator`) is not the one this cluster is sealed at (#464), so
    /// the store is not built. Both shapes are named because either one could be
    /// the mistake: the config may be wrong, or the bucket may be the wrong
    /// bucket.
    ///
    /// Fatal on purpose. Running with a shape the cluster's segments are not
    /// under leaves produce and fetch correct — they read the per-topic routing
    /// pin (#236) — while retention and compaction sweep prefixes that hold
    /// nothing, reporting themselves clean forever.
    PrefixShapeSealed {
        sealed: String,
        configured: String,
    },

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

/// By id whenever the id is not the null one, as Kafka resolves it (#625).
///
/// From v10 a topic may be asked for by id, and the Java client does exactly
/// that to name the topics of a KIP-848 assignment it has no metadata for. It
/// sends the id with `name` at its non-null default, `""`, so reading the name
/// first asked for a topic called `""` and answered `INVALID_TOPIC_EXCEPTION`:
/// a pattern-subscribed consumer could never resolve what it was assigned.
impl From<&MetadataRequestTopic> for TopicId {
    fn from(value: &MetadataRequestTopic) -> Self {
        match (value.topic_id, value.name.as_deref()) {
            (Some(id), _) if id != NULL_TOPIC_ID => Self::Id(Uuid::from_bytes(id)),
            (_, Some(name)) => Self::Name(name.into()),
            (Some(id), None) => Self::Id(Uuid::from_bytes(id)),
            (None, None) => panic!("neither name nor uuid"),
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

/// Upper bound on one `Fetch` response, before a deployment overrides it (#539).
///
/// 5 MiB is the value that has always shipped, and it is a *clamp*: a client
/// asking for `fetch.max.bytes=16m` is silently answered with 5 MiB. #535 and
/// #537 measured what a response over this bound costs — the bound itself was
/// never chosen against a measurement, which is why #539 made it tunable rather
/// than moving it.
pub const DEFAULT_FETCH_MAX_BYTES: u32 = 5 * 1024 * 1024;

/// Broker-level topic config defaults, applied by the engine when a topic is
/// created without an explicit value.
///
/// Mirrors Apache Kafka, where `cleanup.policy` defaults to `delete` and
/// `retention.ms` to 7 days, so retention is enforced even when the client sends
/// no topic config.
///
/// Applied inside [`MetadataStorage::create_topic`] rather than in the `CreateTopics`
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
            GroupState::Formed { protocol_type, .. } => Some(String::clone(protocol_type)),
            GroupState::Forming { protocol_type, .. } => Option::clone(protocol_type),
        }
    }

    pub fn protocol_name(&self) -> Option<String> {
        match self {
            GroupState::Formed { protocol_name, .. } => Some(String::clone(protocol_name)),
            GroupState::Forming { protocol_name, .. } => Option::clone(protocol_name),
        }
    }

    pub fn leader(&self) -> Option<String> {
        match self {
            GroupState::Formed { leader, .. } => Some(String::clone(leader)),
            GroupState::Forming { leader, .. } => Option::clone(leader),
        }
    }

    pub fn assignments(&self) -> BTreeMap<String, Bytes> {
        let GroupState::Formed { assignments, .. } = self else {
            return BTreeMap::new();
        };
        assignments.clone()
    }
}

impl Default for GroupState {
    fn default() -> Self {
        let protocol_name = Some(String::new());
        GroupState::Forming {
            protocol_type: None,
            protocol_name,
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
        let name = match self {
            ConsumerGroupState::Unknown => "Unknown",
            ConsumerGroupState::PreparingRebalance => "PreparingRebalance",
            ConsumerGroupState::CompletingRebalance => "CompletingRebalance",
            ConsumerGroupState::Stable => "Stable",
            ConsumerGroupState::Dead => "Dead",
            ConsumerGroupState::Empty => "Empty",
            ConsumerGroupState::Assigning => "Assigning",
            ConsumerGroupState::Reconciling => "Reconciling",
        };
        f.write_str(name)
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
        let inception = SystemTime::now();
        GroupDetail {
            session_timeout_ms: 45_000,
            rebalance_timeout_ms: None,
            members: BTreeMap::default(),
            generation_id: -1,
            skip_assignment: Some(false),
            inception,
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

        let code: i16 = error_code.into();
        let message = error_code.to_string();
        Self::default()
            .group_id(name.to_owned())
            .error_code(code)
            .error_message(Some(message))
            .group_state(String::from("Unknown"))
            .group_epoch(-1)
            .assignment_epoch(-1)
            .assignor_name(String::new())
            .members(Some(Vec::new()))
            .authorized_operations(-1)
    }
}

/// A `DescribeGroups` answer for `name` in `state` with no members and no
/// protocol: what the broker says about a group it holds no state for.
fn memberless(
    name: &str,
    error_code: ErrorCode,
    state: String,
) -> describe_groups_response::DescribedGroup {
    let code: i16 = error_code.into();
    describe_groups_response::DescribedGroup::default()
        .group_id(name.to_owned())
        .error_code(code)
        .group_state(state)
        .protocol_type(String::new())
        .protocol_data(String::new())
        .members(Some(Vec::new()))
        .authorized_operations(Some(-1))
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
            } => memberless(name, ErrorCode::None, ConsumerGroupState::Dead.to_string()),

            NamedGroupDetail {
                name,
                response: GroupDetailResponse::ErrorCode(error_code),
            } => memberless(name, *error_code, String::from("Unknown")),
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

/// A below-v4 `AddPartitionsToTxn` answer: results by topic, and an empty
/// list by transaction, as the protocol has a client read the field of its
/// own shape.
pub(crate) fn added_by_topic(
    results: Vec<AddPartitionsToTxnTopicResult>,
) -> AddPartitionsToTxnResponse {
    AddPartitionsToTxnResponse::default()
        .error_code(Some(i16::from(ErrorCode::None)))
        .results_by_transaction(Some(Vec::new()))
        .results_by_topic_v_3_and_below(Some(results))
}

/// A v4+ `AddPartitionsToTxn` answer: results by transaction, and an empty
/// list by topic.
pub(crate) fn added_by_transaction(
    results: Vec<AddPartitionsToTxnResult>,
) -> AddPartitionsToTxnResponse {
    AddPartitionsToTxnResponse::default()
        .error_code(Some(i16::from(ErrorCode::None)))
        .results_by_transaction(Some(results))
        .results_by_topic_v_3_and_below(Some(Vec::new()))
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
        matches!(self, TxnState::PrepareAbort | TxnState::PrepareCommit)
    }
}

impl FromStr for TxnState {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let state = match s {
            "BEGIN" => TxnState::Begin,
            "PREPARE_COMMIT" => TxnState::PrepareCommit,
            "PREPARE_ABORT" => TxnState::PrepareAbort,
            "COMMITTED" => TxnState::Committed,
            "ABORTED" => TxnState::Aborted,
            unknown => return Err(Error::UnknownTxnState(String::from(unknown))),
        };
        Ok(state)
    }
}

impl TryFrom<String> for TxnState {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<TxnState> for String {
    fn from(value: TxnState) -> Self {
        let spelled = match value {
            TxnState::Begin => "BEGIN",
            TxnState::PrepareCommit => "PREPARE_COMMIT",
            TxnState::PrepareAbort => "PREPARE_ABORT",
            TxnState::Committed => "COMMITTED",
            TxnState::Aborted => "ABORTED",
        };
        String::from(spelled)
    }
}

/// A batch [admitted](LogStorage::admit) to storage, resolving to its base offset
/// once it is durable (#588).
///
/// `'static` and owned, so whoever admitted the batch can go and admit the next
/// one — from the same connection — while this one waits for its window.
pub type Ack = Pin<Box<dyn Future<Output = Result<i64>> + Send + 'static>>;

/// The cluster, its brokers and its topics: registration, metadata, topic
/// lifecycle and configuration, and the engine's own housekeeping.
#[async_trait]
pub trait MetadataStorage: Debug + Send + Sync + 'static {
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

    /// Query broker and topic metadata.
    ///
    /// `None` asks for every topic and `Some` for exactly the topics listed, so
    /// an empty list answers with the brokers and no topic at all (#593). The
    /// wire's v0 `[]`, which means every topic, is resolved to `None` before it
    /// gets here.
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse>;

    /// The broker's auto-topic-creation policy.
    ///
    /// Had a default of [`AutoTopicCreate::default`] until #551, alongside
    /// [`LogStorage::fetch_max_bytes`]. Both are read *through* the wrapper
    /// stack, so a wrapper that inherits the default answers for a backend
    /// that was configured otherwise — the #273 shape, and the one place it
    /// would have cost most, because `fetch_max_bytes` is #547's
    /// per-deployment clamp. Every wrapper forwarded both by hand and none had
    /// to; now none can forget.
    fn auto_create_topic_config(&self) -> AutoTopicCreate;

    /// Query the configuration of a resource in this storage.
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult>;

    /// Describe the topic partitions found in this storage.
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>>;

    /// Run periodic maintenance on this storage.
    ///
    /// This had a default body of `Ok(())`, for backends with nothing to do
    /// periodically. Removed with `offset_stage_at`'s and for the same reason
    /// (#273, #551): a defaulted method is one a wrapper can forget without
    /// the compiler noticing. A backend with no maintenance writes the
    /// `Ok(())`.
    async fn maintain(&self, now: SystemTime) -> Result<()>;

    async fn cluster_id(&self) -> Result<String>;

    async fn node(&self) -> Result<i32>;

    async fn advertised_listener(&self) -> Result<Url>;

    async fn ping(&self) -> Result<()>;
}

/// The log: producing and fetching batches, the offsets that bound a
/// partition, and the offsets consumers commit against it.
#[async_trait]
pub trait LogStorage: Debug + Send + Sync + 'static {
    /// Produce a deflated batch to this storage.
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64>;

    /// Admit a deflated batch to this storage, without waiting for it to be
    /// durable (#588).
    ///
    /// Returns once the batch holds its place in the write order: a batch
    /// admitted after this one returns is written after it. The [`Ack`] then
    /// resolves to what [`Self::produce`] would have returned.
    ///
    /// That ordering is the whole contract, and it is what lets a connection
    /// pipeline its produces — the next request is read once this one is
    /// admitted, not once it is acknowledged. An engine with nothing to admit
    /// into may return an [`Ack`] that is already resolved, which is correct
    /// and gives a connection nothing to pipeline.
    async fn admit(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<Ack>;

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
    /// [`Self::offset_stage`].
    ///
    /// This had a default body delegating to [`Self::offset_stage`], for
    /// backends that do not distinguish the two. That default is what made
    /// #273 possible — two wrappers never stated the method and silently
    /// inherited it, which left #109 inert in every deployment — so #551
    /// removed it. A backend that does not distinguish them says so by writing
    /// the one-line delegation itself.
    async fn offset_stage_at(
        &self,
        topition: &Topition,
        isolation: IsolationLevel,
    ) -> Result<OffsetStage>;

    /// The read-uncommitted [`OffsetStage`] after this replica has caught up
    /// with the store, for a decision a lagging view must not make.
    ///
    /// [`Self::offset_stage_at`] answers from whatever this replica last saw,
    /// which on a stateless fleet can trail what another replica already served:
    /// a pod that has just started, or one inside its index TTL. That is fine
    /// for reporting offsets and wrong for refusing one. A Fetch whose position
    /// lies past the stage's log end was answered `OFFSET_OUT_OF_RANGE` (#444),
    /// and a consumer with `auto.offset.reset=earliest` replays its whole topic
    /// on that answer — which is what a broker rollout did to several groups on
    /// euw3 (Popsink/data-plane#5470). The Fetch service asks this before it
    /// says so.
    ///
    /// A backend whose view cannot lag says so by delegating to
    /// [`Self::offset_stage_at`]; there is no default body, for #551's reason.
    async fn offset_stage_refreshed(&self, topition: &Topition) -> Result<OffsetStage>;

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

    /// Upper bound on one `Fetch` response; the request's `max_bytes` is
    /// clamped to it. A backend with no configured value answers
    /// [`DEFAULT_FETCH_MAX_BYTES`].
    fn fetch_max_bytes(&self) -> u32;
}

/// Consumer groups: the classic protocol's members, generations and
/// assignments, and the consumer protocol's group documents.
#[async_trait]
pub trait GroupStorage: Debug + Send + Sync + 'static {
    /// Query available groups optionally with a state filter.
    ///
    /// The filter is the wire field, with the wire's meaning: `None` is a
    /// client too old to have one, and `Some([])` is a client that has one and
    /// asked for no filtering. **Both mean every group** — an empty filter is
    /// the absence of a filter, not a filter nothing matches, and reading it
    /// the other way answers a plain `listConsumerGroups()` with nothing at all
    /// (#475).
    ///
    /// Every listed group reports its state, filtered or not: the response
    /// carries one from `ListGroups` v4 on and admin tooling filters on it
    /// client side.
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

    /// Every member id a group holds a document for, with the epoch
    /// milliseconds that document was last written.
    ///
    /// One LIST and **no GETs**: the stamp is the listed object's
    /// `last_modified`, and a member document is written precisely when its
    /// `last_contact_ms` moves, so the listing answers "how recently was this
    /// member heard from" without reading a byte of any document.
    ///
    /// That is what batch admission needs (#427). A member the generation does
    /// not name yet, whose document is fresh, is a member mid-join; the lowest
    /// id among them does the one CAS that admits them all, and every replica
    /// elects the same one because they read the same listing. Only the member
    /// that goes on to admit reads documents, and only the ones it is admitting
    /// — the `group_instance_id` the generation has to record lives nowhere
    /// else.
    ///
    /// Contrast [`Self::list_group_members`], which reads every document: that
    /// one is for reconciliation, this one is cheap enough to be reached from a
    /// request — and only from the admission path, which a member the
    /// generation already names never takes.
    async fn list_group_member_stamps(&self, group_id: &str) -> Result<BTreeMap<String, i64>>;

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

    /// A KIP-848 consumer group's document and its version, or `None` when the
    /// group has none — which is how a classic group and a group that does
    /// not exist both read (#612).
    async fn read_consumer_group(
        &self,
        group_id: &str,
    ) -> Result<Option<(ConsumerGroupDoc, Version)>>;

    /// Conditionally write a KIP-848 consumer group's document (#612).
    ///
    /// `version` is `None` to create — so two replicas racing to create the
    /// same group cannot both win — and `Some` to CAS, exactly as
    /// [`Self::update_group_generation`].
    async fn update_consumer_group(
        &self,
        group_id: &str,
        group: ConsumerGroupDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<ConsumerGroupDoc>>;

    /// Record that a KIP-848 member was heard from at `last_contact_ms` (#627).
    ///
    /// A new object for every renewal, keyed by the member and the stamp, so no
    /// two members — and no two renewals — ever write the same key: there is
    /// no etag to contend on, which is the point. The stamps a renewal
    /// supersedes are removed by [`Self::delete_consumer_member_stamps`].
    async fn renew_consumer_member(
        &self,
        group_id: &str,
        member_id: &str,
        last_contact_ms: i64,
    ) -> Result<()>;

    /// Every liveness stamp of a KIP-848 group, by member (#627).
    ///
    /// One LIST and no GETs: the stamp is in the key. The broker's clock wrote
    /// it, not the store's, so it is compared with the clock that judges it.
    async fn list_consumer_member_stamps(&self, group_id: &str) -> Result<ContactStamps>;

    /// Remove liveness stamps: superseded ones, and those of members the group
    /// no longer has (#627). Best-effort by contract — a stamp left behind is
    /// one more key in the next listing, never a wrong answer, because only the
    /// newest stamp of a member the document names is ever read.
    async fn delete_consumer_member_stamps(
        &self,
        group_id: &str,
        stamps: &[(String, i64)],
    ) -> Result<()>;

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
}

/// Transactions and idempotent producers.
#[async_trait]
pub trait TxnStorage: Debug + Send + Sync + 'static {
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
        partitions: AddPartitionsToTxnRequest,
    ) -> Result<AddPartitionsToTxnResponse>;

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
}

/// Who may do what, and how much: SCRAM credentials, ACLs and client quotas.
#[async_trait]
pub trait PrincipalStorage: Debug + Send + Sync + 'static {
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
}

/// Storage
///
/// The core storage abstraction, as the five domain traits it is made of
/// (Popsink/data-plane#5884). Every engine implements all five, and so is a
/// `Storage`; code that needs only one domain can ask for that trait alone.
pub trait Storage:
    MetadataStorage + LogStorage + GroupStorage + TxnStorage + PrincipalStorage
{
}

impl<T> Storage for T where
    T: MetadataStorage + LogStorage + GroupStorage + TxnStorage + PrincipalStorage
{
}

// The existence of this function makes the compiler catch if the Storage
// trait is "object-safe" or not.
fn _assert_trait_object(_s: &dyn Storage) {}

/// `Storage` for `Arc<T>` and `Box<T>`, generated from
/// [`storage_methods`](crate::delegate::storage_methods).
///
/// Fifty methods each, every body `self.as_ref().m(args).await`, written out
/// by hand until #551. The two expanders are separate macros rather than one
/// parameterised by the type, because `#[async_trait]` has to be attached to a
/// complete `impl` in a single expansion — see `delegate.rs`.
macro_rules! arc_delegation {
    ($(
        $trait:ident {
            $(fn $name:ident(&$s:ident $(, $arg:ident : $ty:ty)* $(,)?) -> $ret:ty;)*
            $(sync fn $sname:ident(&$ss:ident $(, $sarg:ident : $sty:ty)* $(,)?) -> $sret:ty;)*
        }
    )*) => {$(
        #[async_trait]
        impl<T> $trait for Arc<T>
        where
            T: Storage + ?Sized,
        {
            $(
                async fn $name(&$s $(, $arg: $ty)*) -> $ret {
                    $s.as_ref().$name($($arg),*).await
                }
            )*
            $(
                fn $sname(&$ss $(, $sarg: $sty)*) -> $sret {
                    $ss.as_ref().$sname($($sarg),*)
                }
            )*
        }
    )*};
}

macro_rules! box_delegation {
    ($(
        $trait:ident {
            $(fn $name:ident(&$s:ident $(, $arg:ident : $ty:ty)* $(,)?) -> $ret:ty;)*
            $(sync fn $sname:ident(&$ss:ident $(, $sarg:ident : $sty:ty)* $(,)?) -> $sret:ty;)*
        }
    )*) => {$(
        #[async_trait]
        impl<T> $trait for Box<T>
        where
            T: Storage + ?Sized,
        {
            $(
                async fn $name(&$s $(, $arg: $ty)*) -> $ret {
                    $s.as_ref().$name($($arg),*).await
                }
            )*
            $(
                fn $sname(&$ss $(, $sarg: $sty)*) -> $sret {
                    $ss.as_ref().$sname($($sarg),*)
                }
            )*
        }
    )*};
}

storage_methods!(arc_delegation);
storage_methods!(box_delegation);

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
/// `Arc<dyn Storage>`, so the impl was unreachable and its per-method
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
/// string (`?message_max_bytes=1m`), falling back to Kafka's own default (#443).
///
/// The suffixes are `human_units`' IEC ones — `1m` is one MiB — and it does *not*
/// accept `1MiB`, which this comment used to say. Following it got the default
/// back, silently, which is the failure the paragraph below is about.
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

/// `?batch_min_size=` — the smallest produce batch
/// [`ProduceRequestBatcher`](crate::batch::ProduceRequestBatcher) will flush,
/// or `None` for its own default. Unparseable values warn and fall back rather
/// than failing the build: a bad tuning knob is not worth refusing to start
/// over.
#[cfg(feature = "dynostore")]
fn batch_minimum_size(storage: &Url) -> Option<usize> {
    storage.query_pairs().find_map(|(k, v)| {
        if k == "batch_min_size" {
            human_units::Size::from_str(v.as_ref())
                .map(|size| size.0)
                .inspect_err(|err| warn!(%storage, v = v.as_ref(), ?err))
                .ok()
                .and_then(|size| usize::try_from(size).ok())
        } else {
            None
        }
    })
}

/// `?batch_max_delay=` — how long the produce batcher will hold a batch short of
/// [`batch_minimum_size`] before flushing it anyway. As above, `None` means the
/// batcher's default.
#[cfg(feature = "dynostore")]
fn batch_maximum_delay(storage: &Url) -> Option<Duration> {
    storage.query_pairs().find_map(|(k, v)| {
        if k == "batch_max_delay" {
            human_units::Duration::from_str(v.as_ref())
                .map(|duration| duration.0)
                .inspect_err(|err| warn!(%storage, v = v.as_ref(), ?err))
                .ok()
        } else {
            None
        }
    })
}

/// `?delete_concurrency=` — how many objects the `gs` arm deletes at once,
/// or `None` for [`PutRateLimiter`]'s stated default (#518).
///
/// **The one storage-URL key only one arm reads.** Parsing it is
/// scheme-independent like everything else here — see
/// `storage_url_query_parameters_are_scheme_independent` — but the value only
/// reaches `gcs::limit::PutRateLimiter`, which is built on the `gs` arm and
/// nowhere else, because GCS is the one backend `object_store` gives no bulk
/// delete: S3 and Azure delete a batch per request and have no per-object
/// fan-out to widen. On any other scheme it is accepted and ignored, as every
/// key an arm does not read already is.
///
/// Zero is rejected rather than clamped: a fan-out of none is a `delete_stream`
/// that never yields, and an operator who typed it meant something else.
/// Unparseable values warn and fall back, as everywhere else here.
#[cfg(feature = "dynostore")]
fn delete_concurrency(storage: &Url) -> Option<std::num::NonZeroUsize> {
    storage.query_pairs().find_map(|(k, v)| {
        if k == "delete_concurrency" {
            v.parse::<std::num::NonZeroUsize>()
                .inspect_err(
                    |err| warn!(%storage, key = "delete_concurrency", v = v.as_ref(), ?err),
                )
                .ok()
        } else {
            None
        }
    })
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
            "fetch_max_bytes" => {
                tuning.fetch_max_bytes = human_units::Size::from_str(value)
                    .map(|size| size.0)
                    .inspect_err(
                        |err| warn!(storage = %storage, key = "fetch_max_bytes", value, ?err),
                    )
                    .ok()
                    .and_then(|size| u32::try_from(size).ok());
            }
            "fetch_read_concurrency" => {
                tuning.fetch_read_concurrency = value
                    .parse()
                    .inspect_err(
                        |err| warn!(storage = %storage, key = "fetch_read_concurrency", value, ?err),
                    )
                    .ok();
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
            "prefix_compact_region_bytes" => {
                tuning.prefix_compact_region_bytes = human_units::Size::from_str(value)
                    .map(|size| size.0)
                    .inspect_err(|err| warn!(storage = %storage, key = "prefix_compact_region_bytes", value, ?err))
                    .ok()
                    .and_then(|size| usize::try_from(size).ok());
            }
            "prefix_compact_keep_hot" => {
                tuning.prefix_compact_keep_hot = value
                    .parse()
                    .inspect_err(|err| warn!(storage = %storage, key = "prefix_compact_keep_hot", value, ?err))
                    .ok();
            }
            "prefix_maintenance_concurrency" => {
                tuning.prefix_maintenance_concurrency = value
                    .parse()
                    .inspect_err(|err| warn!(storage = %storage, key = "prefix_maintenance_concurrency", value, ?err))
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
            "maintenance_pass_budget" => {
                tuning.maintenance_pass_budget = human_units::Duration::from_str(value)
                    .map(|duration| duration.0)
                    .inspect_err(
                        |err| warn!(storage = %storage, key = "maintenance_pass_budget", value, ?err),
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
            "watermark_hint_ttl" => {
                tuning.watermark_hint_ttl = human_units::Duration::from_str(value)
                    .map(|duration| duration.0)
                    .inspect_err(
                        |err| warn!(storage = %storage, key = "watermark_hint_ttl", value, ?err),
                    )
                    .ok();
            }
            // The shape of the coalescing prefix (#464). Not a threshold like
            // everything above: it decides *where* records live, is sealed per
            // cluster at store build (`DynoStore::sealed_prefix_shape`), and
            // cannot be moved on a populated bucket. An unparseable value is
            // ignored with a warning as usual, which leaves the shape at the
            // default — and if the bucket is sealed at something else, the seal
            // turns that typo into a failed build rather than a silently
            // different layout.
            "prefix_depth" => {
                tuning.prefix_depth = value
                    .parse()
                    .inspect_err(|err| warn!(storage = %storage, key = "prefix_depth", value, ?err))
                    .ok();
            }
            // Rejected empty rather than accepted: splitting on "" yields one
            // component per character, which is not a prefix shape anyone asked
            // for and would seal the cluster into it.
            "prefix_separator" => {
                tuning.prefix_separator = if value.is_empty() {
                    warn!(
                        storage = %storage,
                        key = "prefix_separator",
                        "ignoring an empty prefix separator"
                    );
                    None
                } else {
                    Some(value.to_owned())
                };
            }
            // The one key here that changes what is *written* rather than how
            // often (#442), and the only one whose effect does not reverse when
            // it is unset again — see the engine's `segment_format_version`. An
            // unparseable or unknown version is ignored with a warning like
            // every other key, which leaves the writer at its default: a typo
            // must not be able to stamp segments with a version no reader knows.
            "segment_format" => {
                tuning.segment_format_version = value
                    .parse::<u16>()
                    .inspect_err(
                        |err| warn!(storage = %storage, key = "segment_format", value, ?err),
                    )
                    .ok()
                    .filter(|version| SEGMENT_FORMAT_WRITABLE.contains(version))
                    .or_else(|| {
                        warn!(
                            storage = %storage,
                            key = "segment_format",
                            value,
                            writable = ?SEGMENT_FORMAT_WRITABLE,
                            "ignoring an unwritable segment format version"
                        );
                        None
                    });
            }
            _ => {}
        }
    }

    debug!(?tuning);
    tuning
}

impl Builder<i32, String, Url, Url> {
    /// An object-store engine over `object_store`, with this builder's
    /// identity and the knobs its storage URL carries.
    #[cfg(feature = "dynostore")]
    fn engine(&self, object_store: impl object_store::ObjectStore) -> DynoStore {
        let engine = DynoStore::new(self.cluster_id.as_str(), self.node_id, object_store);
        engine
            .advertised_listener(self.advertised_listener.clone())
            .auto_create(auto_topic_create(&self.storage))
            .topic_defaults(self.topic_defaults.clone())
            .coalesce_tuning(coalesce_tuning(&self.storage))
            .message_max_bytes(message_max_bytes(&self.storage))
    }

    /// `engine` probed for conditional puts, sealed to its prefix shape, and
    /// behind a produce batcher with the URL's minimum size and delay.
    #[cfg(feature = "dynostore")]
    async fn served(
        &self,
        engine: DynoStore,
        minimum_size: Option<usize>,
        maximum_delay: Option<Duration>,
    ) -> Result<Arc<dyn Storage>> {
        let probed = engine.conditional_put_probed(&self.storage).await?;
        let sealed = probed.sealed_prefix_shape().await?;
        let batched = batch::ProduceRequestBatcher::new(sealed)
            .with_minimum_size(minimum_size)
            .with_maximum_delay(maximum_delay);
        Ok(Arc::new(batched))
    }

    pub async fn build(self) -> Result<Arc<dyn Storage>> {
        // Routed through [`Backend`] rather than matching `scheme()` here, so
        // that a backend added to one builder cannot be missing from the other
        // two: this match is exhaustive over the enum (#531).
        let storage = match Backend::try_from_url(&self.storage)? {
            #[cfg(feature = "dynostore")]
            Backend::S3 => {
                let bucket_name = self.storage.host_str().unwrap_or("tansu");

                let minimum_size = batch_minimum_size(&self.storage);
                let maximum_delay = batch_maximum_delay(&self.storage);

                warn_deprecated_layout_flags(&self.storage);

                // Compacted topics segment-routed to dedicated prefixes with a
                // per-key compaction pass (#175). Off by default; flipped only
                debug!(?minimum_size, ?maximum_delay);

                let engine = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    // S3 enforces a per-prefix request-rate limit (~3,500
                    // mutating req/prefix/sec). A hot topic-partition with many
                    // tiny batch objects, plus the periodic `maintain` delete
                    // flood, trips `503 SlowDown`; the object_store default of
                    // 10 retries over 180s isn't enough and deletes/produces are
                    // dropped (#5, #6). Give throttles a longer, gentler ceiling
                    // so they ride out a SlowDown burst instead of failing.
                    //
                    // `gs`'s data plane takes these same numbers, via
                    // `gcs::retry::BUCKET_RAMP_RETRY`, for the same reason
                    // wearing a different status code (#519). Duplicated rather
                    // than shared: the two arms are throttled by different
                    // things and only happen to want the same patience.
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
                    .map(|object_store| self.engine(object_store))
                    .map_err(Error::from)?;
                self.served(engine, minimum_size, maximum_delay).await
            }

            #[cfg(feature = "dynostore")]
            Backend::Google => {
                let bucket_name = self.storage.host_str().unwrap_or("tansu");

                let minimum_size = batch_minimum_size(&self.storage);
                let maximum_delay = batch_maximum_delay(&self.storage);

                warn_deprecated_layout_flags(&self.storage);

                let engine = gcs::object_store(bucket_name, delete_concurrency(&self.storage))
                    .map(|object_store| {
                        self.engine(object_store)
                            .object_write_interval(Some(gcs::OBJECT_WRITE_INTERVAL))
                    })
                    .map_err(Error::from)?;
                self.served(engine, minimum_size, maximum_delay).await
            }

            // Azure Data Lake Storage Gen2, hierarchical namespace on (#418).
            //
            // `abfss://<container>@<account>.dfs.core.windows.net/` is canonical:
            // it is what an ADLS Gen2 user already writes, and it carries the
            // account. `az://<container>/` is the short form, with the account
            // from `AZURE_STORAGE_ACCOUNT_NAME`.
            //
            // The whole URL goes to `with_url` rather than being taken apart
            // here the way the `s3` arm takes its bucket from `host_str()`,
            // because the container is the URL's *username* in the canonical
            // form and its host in the short one — and `object_store`'s parser
            // already tells the two apart. That parser reads scheme, host and
            // username only, so every scheme-independent query parameter
            // (`batch_min_size`, `coalesce_*`, …) passes through untouched.
            //
            // `object_store` speaks the *blob* endpoint against an ADLS Gen2
            // account, so a `dfs` host in the URL is resolved to
            // `<account>.blob.core.windows.net`. A tenant whose network team
            // provisioned a private endpoint for the `dfs` sub-resource only —
            // the usual default for a data-lake account — will not reach us.
            //
            // Azure has no suffix range GET and the segment footer lives at
            // the tail of the object, so the store is wrapped in `SuffixRange`,
            // which translates each one into a `head` plus a bounded range
            // (#419). Without it every fetch fails and `probe_prefix_tail`
            // degrades to a permanent LIST fallback.
            #[cfg(feature = "dynostore")]
            Backend::Azure => {
                use object_store::azure::MicrosoftAzureBuilder;

                use crate::azure::suffix::SuffixRange;

                let minimum_size = batch_minimum_size(&self.storage);
                let maximum_delay = batch_maximum_delay(&self.storage);

                warn_deprecated_layout_flags(&self.storage);

                debug!(?minimum_size, ?maximum_delay);

                // Credentials come from the environment and nowhere else:
                // `from_env` already covers account key, client secret, MSI and
                // workload identity federation (`AZURE_FEDERATED_TOKEN_FILE`).
                // The last is the AKS path, and the direct analogue of IRSA in
                // the serverless roadmap (#364).
                let engine = MicrosoftAzureBuilder::from_env()
                    .with_url(self.storage.as_str())
                    // Azure throttles at the *account* and storage-partition
                    // level (`503 ServerBusy`, `500 OperationTimedOut`), not per
                    // object. That is S3's failure shape, not GCS's, so this
                    // takes the `s3` arm's long, gentle budget for every request
                    // and deliberately not the fail-fast one — which exists
                    // only because a GCS per-object throttle cannot be won by
                    // waiting (#13), and which on the `gs` arm is now one of
                    // two budgets picked per request (#519). There is nothing
                    // to pick between here: Azure has one throttle.
                    .with_retry(RetryConfig {
                        backoff: BackoffConfig {
                            init_backoff: Duration::from_millis(200),
                            max_backoff: Duration::from_secs(30),
                            base: 2.0,
                        },
                        max_retries: 32,
                        retry_timeout: Duration::from_secs(300),
                    })
                    // Two things this arm deliberately does *not* do, both of
                    // which the arms above it do:
                    //
                    // No `PutRateLimiter`. `gcs/limit.rs` is a workaround for
                    // GCS capping a single object at ~1 write/s (#13); Azure's
                    // per-blob ceiling is ~500 req/s. Wrapping the store here
                    // would be cargo-culting a fix for a limit that does not
                    // exist, and it would cost a real per-put delay.
                    //
                    // No `with_conditional_put`. Azure implements natively what
                    // S3 needs `S3ConditionalPut::ETagMatch` to opt into, and
                    // create-only puts plus etag CAS are what this layout is
                    // built on.
                    .build()
                    .map(SuffixRange::new)
                    .map(|object_store| self.engine(object_store))
                    .map_err(Error::from)?;
                self.served(engine, minimum_size, maximum_delay).await
            }

            #[cfg(feature = "dynostore")]
            Backend::Memory => {
                let sealed = self.engine(InMemory::new()).sealed_prefix_shape().await?;
                Ok(Arc::new(sealed) as Arc<dyn Storage>)
            }

            Backend::Null => {
                let listener = self.advertised_listener.clone();
                let engine = null::Engine::new(self.cluster_id.clone(), self.node_id, listener);
                Ok(Arc::new(engine) as Arc<dyn Storage>)
            }

            // `file:///path/to/copy` is the offline audit's, and only the
            // audit's: a deployment already past the damage cannot be measured
            // by starting a broker on a copy of its bucket.
            Backend::Local => Err(Error::UnsupportedStorageUrl(self.storage.clone())),

            // Without `dynostore` there is no object store to build, and the
            // arms above are compiled out — the schemes still route, they just
            // have nothing to route to.
            #[cfg(not(feature = "dynostore"))]
            Backend::S3 | Backend::Google | Backend::Azure | Backend::Memory => {
                Err(Error::UnsupportedStorageUrl(self.storage.clone()))
            }
        }?;

        let progress = (!self.silent).then(|| {
            let style = ProgressStyle::with_template("[{elapsed}] {bar:40.cyan/blue} {msg}");
            let connecting = ProgressBar::new(1);
            connecting.set_style(style.unwrap().progress_chars("##-"));
            connecting.set_message("connecting to storage");
            connecting
        });

        storage.ping().await?;

        if let Some(connected) = progress {
            connected.inc(1);
            let done = Emoji("✅", "");
            connected.finish_with_message(format!("{done} connected to storage"));
        }
        Ok(storage)
    }
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    let scope = InstrumentationScope::builder(env!("CARGO_PKG_NAME"));
    let scope = scope.with_version(env!("CARGO_PKG_VERSION"));
    global::meter_with_scope(scope.with_schema_url(SCHEMA_URL).build())
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

/// Fetch positions found outside this replica's view of the log, by what a
/// forced listing then said (Popsink/data-plane#5470): `confirmed` was
/// answered `OFFSET_OUT_OF_RANGE`, `refuted` was a lagging view and was served,
/// `unresolved` could not be checked and was answered empty.
///
/// `OFFSET_OUT_OF_RANGE` is the answer a consumer turns into a reset, and a
/// reset to `earliest` is a whole-topic replay, so every one of them is worth
/// seeing. A non-zero `refuted` rate is the measure of how often a lagging pod
/// would have replayed a consumer before this check existed.
pub(crate) static FETCH_OUT_OF_RANGE: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_fetch_out_of_range")
        .with_description("fetch positions outside a replica's view of the log, by outcome")
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

    /// #418: the Azure schemes we accept, and the Azure-adjacent ones we do not.
    ///
    /// `object_store`'s own parser accepts `adl://`, `azure://` and
    /// `https://<account>.blob.core.windows.net/` as well, and this arm
    /// deliberately does not: `abfss://` is what an ADLS Gen2 user writes and
    /// `az://` is the short form, and every extra alias is another spelling a
    /// deployment can drift onto. `wasbs://` is the legacy Blob scheme and is
    /// the one someone migrating is most likely to try — it must fail loudly
    /// rather than be quietly treated as Gen2.
    ///
    /// This asserts the *routing*, not a working store: building an Azure store
    /// needs an account, and `--storage-engine` reaching `ping()` is asserted
    /// against a real ADLS Gen2 account (#417), not here.
    #[cfg(feature = "dynostore")]
    #[tokio::test]
    async fn azure_adjacent_schemes_are_unsupported() -> Result<()> {
        for url in [
            "wasbs://tansu@acct.blob.core.windows.net/",
            "wasb://tansu@acct.blob.core.windows.net/",
            "adl://tansu/",
            "azure://tansu/",
            "https://acct.blob.core.windows.net/tansu",
        ] {
            let storage = StorageContainer::builder()
                .cluster_id("tansu")
                .node_id(111)
                .advertised_listener(Url::parse("tcp://localhost:9092")?)
                .storage(Url::parse(url)?)
                .build()
                .await;

            assert!(
                matches!(storage, Err(Error::UnsupportedStorageUrl(_))),
                "{url} must not resolve to a storage engine, got {storage:?}",
            );
        }

        Ok(())
    }

    /// #418: every query parameter is scheme-independent, including on Azure.
    ///
    /// The tuning knobs are read off the URL by free functions that never look
    /// at the scheme, so this is a property of the parsing rather than of the
    /// arm — which is exactly why it is worth pinning. The `s3` and `gs` arms
    /// each carried their own copy of the two batch readers before this ticket;
    /// a third copy is how one arm silently stops honouring a knob.
    #[cfg(feature = "dynostore")]
    #[test]
    fn storage_url_query_parameters_are_scheme_independent() -> Result<()> {
        let url = Url::parse(
            "abfss://tansu@acct.dfs.core.windows.net/\
             ?batch_min_size=32k&batch_max_delay=250ms\
             &message_max_bytes=8m\
             &auto_create_topics=false\
             &prefix_compact_min_segments=512",
        )?;

        assert_eq!(Some(32 * 1_024), batch_minimum_size(&url));
        assert_eq!(Some(Duration::from_millis(250)), batch_maximum_delay(&url));
        assert_eq!(8 * 1_024 * 1_024, message_max_bytes(&url));
        assert!(!auto_topic_create(&url).enable);
        assert_eq!(Some(512), coalesce_tuning(&url).prefix_compact_min_segments,);

        Ok(())
    }

    #[cfg(feature = "dynostore")]
    #[test]
    fn coalesce_tuning_parses_compaction_keys() -> Result<()> {
        let tuning = coalesce_tuning(&Url::parse(
            "s3://tansu/?prefix_compact_min_segments=512&prefix_compact_target_bytes=128m&prefix_compact_keep_hot=32&prefix_compact_region_bytes=4m&prefix_maintenance_concurrency=8",
        )?);
        assert_eq!(Some(8), tuning.prefix_maintenance_concurrency);
        assert_eq!(Some(512), tuning.prefix_compact_min_segments);
        assert_eq!(Some(128 * 1024 * 1024), tuning.prefix_compact_target_bytes);
        assert_eq!(Some(32), tuning.prefix_compact_keep_hot);
        assert_eq!(Some(4 * 1024 * 1024), tuning.prefix_compact_region_bytes);
        // Absent keys stay None (compile-time defaults).
        let empty = coalesce_tuning(&Url::parse("s3://tansu/")?);
        assert_eq!(None, empty.prefix_compact_min_segments);
        assert_eq!(None, empty.prefix_compact_region_bytes);
        Ok(())
    }

    /// `?delete_concurrency=` — the GCS delete fan-out (#518).
    ///
    /// `None` for absent, unparseable *and* zero, all three of which mean
    /// `PutRateLimiter`'s stated default rather than a fan-out of none.
    #[cfg(feature = "dynostore")]
    #[test]
    fn delete_concurrency_parses() -> Result<()> {
        use std::num::NonZeroUsize;

        assert_eq!(
            NonZeroUsize::new(64),
            delete_concurrency(&Url::parse("gs://tansu/?delete_concurrency=64")?)
        );

        assert_eq!(None, delete_concurrency(&Url::parse("gs://tansu/")?));

        assert_eq!(
            None,
            delete_concurrency(&Url::parse("gs://tansu/?delete_concurrency=wide")?)
        );

        assert_eq!(
            None,
            delete_concurrency(&Url::parse("gs://tansu/?delete_concurrency=0")?)
        );

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

    /// The `Fetch` response bound (#539): parsed as a human size, absent or
    /// unparseable keeps [`DEFAULT_FETCH_MAX_BYTES`], and a value past `u32`
    /// is refused rather than truncated — `4G` narrowed to `u32` would land on
    /// `0` and cap every fetch at nothing.
    #[cfg(feature = "dynostore")]
    #[test]
    fn fetch_max_bytes_parses() -> Result<()> {
        assert_eq!(
            Some(16 << 20),
            coalesce_tuning(&Url::parse("s3://tansu/?fetch_max_bytes=16M")?).fetch_max_bytes
        );

        assert_eq!(
            None,
            coalesce_tuning(&Url::parse("s3://tansu/")?).fetch_max_bytes
        );

        assert_eq!(
            None,
            coalesce_tuning(&Url::parse("s3://tansu/?fetch_max_bytes=big")?).fetch_max_bytes
        );

        assert_eq!(
            None,
            coalesce_tuning(&Url::parse("s3://tansu/?fetch_max_bytes=4G")?).fetch_max_bytes
        );

        Ok(())
    }

    /// The region read fan-out (#587): absent, unparseable or zero keeps
    /// [`dynostore::DynoStore::FETCH_READ_CONCURRENCY`] — zero refused rather
    /// than taken, because `buffered(0)` never polls a read and the fetch
    /// would hang until the client's timeout.
    #[cfg(feature = "dynostore")]
    #[test]
    fn fetch_read_concurrency_parses() -> Result<()> {
        let parse = |url: &str| -> Result<Option<usize>> {
            Ok(coalesce_tuning(&Url::parse(url)?)
                .fetch_read_concurrency
                .map(std::num::NonZeroUsize::get))
        };

        assert_eq!(Some(128), parse("s3://tansu/?fetch_read_concurrency=128")?);
        assert_eq!(None, parse("s3://tansu/")?);
        assert_eq!(None, parse("s3://tansu/?fetch_read_concurrency=lots")?);
        assert_eq!(None, parse("s3://tansu/?fetch_read_concurrency=0")?);
        assert_eq!(None, parse("s3://tansu/?fetch_read_concurrency=-1")?);

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

    /// The watermark-view freshness window (#500): parsed as a human duration,
    /// absent or unparseable keeps the compile-time default.
    #[cfg(feature = "dynostore")]
    #[test]
    fn coalesce_tuning_parses_watermark_hint_ttl() -> Result<()> {
        let tuning = coalesce_tuning(&Url::parse("s3://tansu/?watermark_hint_ttl=30s")?);
        assert_eq!(Some(Duration::from_secs(30)), tuning.watermark_hint_ttl);

        let absent = coalesce_tuning(&Url::parse("s3://tansu/")?);
        assert_eq!(None, absent.watermark_hint_ttl);

        let unparseable = coalesce_tuning(&Url::parse("s3://tansu/?watermark_hint_ttl=whenever")?);
        assert_eq!(None, unparseable.watermark_hint_ttl);
        Ok(())
    }

    /// The prefix shape keys (#464), including what "absent" has to mean: the
    /// pre-#464 derivation, so a storage URL that has never carried them keeps
    /// its exact object layout.
    #[cfg(feature = "dynostore")]
    #[test]
    fn coalesce_tuning_parses_the_prefix_shape() -> Result<()> {
        let parse = |query: &str| -> Result<(Option<usize>, Option<String>)> {
            let tuning = coalesce_tuning(&Url::parse(&format!("s3://tansu/?{query}"))?);
            Ok((tuning.prefix_depth, tuning.prefix_separator))
        };

        assert_eq!((None, None), parse("")?);
        assert_eq!((Some(2), None), parse("prefix_depth=2")?);
        assert_eq!((Some(0), None), parse("prefix_depth=0")?);
        assert_eq!((None, Some("_".into())), parse("prefix_separator=_")?);
        assert_eq!(
            (Some(4), Some("-".into())),
            parse("prefix_depth=4&prefix_separator=-")?
        );

        // A typo leaves the shape at the default — and the per-cluster seal then
        // turns that into a failed build rather than a different layout.
        assert_eq!((None, None), parse("prefix_depth=three")?);
        assert_eq!((None, None), parse("prefix_depth=-1")?);
        assert_eq!((None, None), parse("prefix_depth=")?);

        // An empty separator would split a name into one component per
        // character; ignored, like any other unusable value.
        assert_eq!((None, None), parse("prefix_separator=")?);

        Ok(())
    }

    /// `segment_format` accepts only a version this build can *write* (#442).
    ///
    /// Every other tuning key trades cost against latency and a typo costs a
    /// default. This one decides the bytes that go into immutable objects, and a
    /// version no reader knows is a hard `decode_segment_footer` error — a
    /// partition-wide read outage, on a shared segment, for every topic in the
    /// prefix. So an unrecognised value leaves the writer where it was rather
    /// than being passed through.
    #[cfg(feature = "dynostore")]
    #[test]
    fn segment_format_accepts_only_a_writable_version() -> Result<()> {
        let parse = |query: &str| -> Result<Option<u16>> {
            Ok(
                coalesce_tuning(&Url::parse(&format!("s3://tansu/?{query}"))?)
                    .segment_format_version,
            )
        };

        assert_eq!(None, parse("")?, "absent leaves the writer at its default");
        assert_eq!(Some(3), parse("segment_format=3")?);
        assert_eq!(Some(4), parse("segment_format=4")?);

        // Readable, but nothing may be asked to emit them again.
        assert_eq!(None, parse("segment_format=1")?);
        assert_eq!(None, parse("segment_format=2")?);

        // Ahead of this build, and not a number at all.
        assert_eq!(None, parse("segment_format=5")?);
        assert_eq!(None, parse("segment_format=v4")?);
        assert_eq!(None, parse("segment_format=")?);

        Ok(())
    }
}
