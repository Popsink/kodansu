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

//! Dynamic Object Storage engine (S3, memory, ...)

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    fmt::{Debug, Display},
    str::FromStr,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{self, AtomicU64},
    },
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    StreamExt,
    stream::{BoxStream, TryStreamExt},
};
use metadata::Cache;
use object_store::{
    Attribute, AttributeValue, Attributes, CopyOptions, DynObjectStore, GetOptions, GetRange,
    GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UpdateVersion, path::Path,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram},
};
use opticon::OptiCon;
use rand::{prelude::*, rng};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tansu_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, OpType, ScramMechanism,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{Record, deflated, inflated},
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use tansu_schema::{
    Registry,
    lake::{House, LakeHouse as _},
};
use tokio::sync::oneshot;
use tokio::time::Duration;
use tracing::{debug, error, info, instrument, warn};
use url::Url;
use uuid::Uuid;

mod metadata;
mod opticon;

#[cfg(test)]
mod tests;

use crate::{
    AutoTopicCreate, BrokerRegistrationRequest, Error, GroupDetail, ListOffsetResponse, METER,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
    Result, ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState, UpdateError, Version,
};

const APPLICATION_JSON: &str = "application/json";

#[derive(Clone, Debug)]
pub struct DynoStore {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    schemas: Option<Registry>,
    lake: Option<House>,
    watermarks: Arc<Mutex<BTreeMap<Topition, OptiCon<Watermark>>>>,
    meta: OptiCon<Meta>,

    /// Per-topic optimistic-concurrency handle on
    /// `topic-metadata/{name}.json`, the authoritative record of a topic's id
    /// and config. Decomposing topic metadata out of the cluster-global
    /// `meta.json` removes the create-time CAS contention on that monolith and,
    /// crucially, makes a freshly created topic immediately visible to every
    /// replica: a replica that has never read the topic holds no cached etag,
    /// so the conditional GET cannot be short-circuited to a stale
    /// `NotModified` (the cross-replica create-then-produce race, #28).
    topic_metas: Arc<Mutex<BTreeMap<Topic, OptiCon<TopicMetadata>>>>,

    /// In-memory, etag-delta-refreshed index of all topics, serving the list-all
    /// metadata path and the cleanup policies from memory. Without it, list-all
    /// swept every per-topic object (a GET each) on every request — the #29
    /// regression that OOM-crash-looped prod under metadata load. A refresh
    /// LISTs the `topic-metadata/` prefix once and GETs only the objects whose
    /// etag changed, so it scales to tens of thousands of topics.
    topic_index: Arc<Mutex<TopicIndex>>,

    /// Single-flight guard: only one task refreshes [`Self::topic_index`] at a
    /// time; concurrent list-all callers await it rather than each re-listing.
    topic_index_refresh: Arc<tokio::sync::Mutex<()>>,

    /// Cache of the `topic-ids/{uuid}.json` pointer (topic-id -> name), immutable
    /// for a topic's lifetime, so a by-id lookup avoids an uncached object GET;
    /// invalidated on delete.
    topic_ids: Arc<Mutex<BTreeMap<Uuid, Topic>>>,

    /// Broker auto-topic-creation policy (Kafka `auto.create.topics.enable` /
    /// `num.partitions` / `default.replication.factor`), consulted by the
    /// Metadata handler.
    auto_create: AutoTopicCreate,

    /// Per-partition cache of the next offset to assign (== the high watermark).
    ///
    /// This is a *hint*, not the authority: the authority is the set of
    /// immutable, create-only batch objects whose names encode their base
    /// offset. The hint lets the common produce path skip a tail listing, and
    /// is reconciled against the listing on a `Create` conflict or a cold read
    /// (see [`DynoStore::refresh_high`]). Keeping offset assignment off a single
    /// mutable `watermark` object is what takes the produce hot path off the
    /// GCS per-object update-rate cap (#13).
    next_offsets: Arc<Mutex<BTreeMap<Topition, OffsetHint>>>,

    /// Per-producer optimistic-concurrency handle on `producers/{id}.json`,
    /// holding that producer's idempotent sequence state. Sharding the sequence
    /// CAS per producer (instead of CASing the single cluster-global `meta`
    /// object on every idempotent batch) removes the cross-producer contention
    /// that serialised every `acks=all`/Debezium producer on GCS (#13). The
    /// linearizable CAS is kept, so the exact `OutOfOrderSequenceNumber` /
    /// `DuplicateSequenceNumber` / `ProducerFenced` semantics are preserved.
    producers: Arc<Mutex<BTreeMap<ProducerId, OptiCon<ProducerDetail>>>>,

    /// Per-producer debounce state for the lazy `producers/{id}.json` checkpoint
    /// (#48). The in-memory sequence is advanced on every idempotent batch; the
    /// durable object is written at most once per
    /// [`Self::PRODUCER_CHECKPOINT_BATCHES`] batches or
    /// [`Self::PRODUCER_CHECKPOINT_INTERVAL`], whichever comes first (plus the
    /// immediate seed/epoch-bump write via [`Self::seed_producer`]). On an
    /// unclean crash the persisted sequence may lag the acked one by at most one
    /// such window (graceful-shutdown flush is intentionally not wired — a stale
    /// checkpoint can only re-accept a bounded tail on replay, never lose data,
    /// since the object never moves backwards).
    producer_checkpoints: Arc<Mutex<BTreeMap<ProducerId, ProducerCheckpoint>>>,

    /// Per-partition `last_modified` (ms) of the *oldest* surviving batch object,
    /// as observed at the last `delete`-policy scan (#49). Lets the maintenance
    /// loop skip the full `records/` LIST of a partition whose oldest data is
    /// still newer than the retention threshold — the dominant wasted Tier1 cost
    /// once `delete` is the default policy. Absent means "unknown, must scan";
    /// the value is a *lower bound* on the true oldest timestamp (produce only
    /// adds newer objects, expiry/compaction only makes the oldest newer), so a
    /// stale hint can at worst force an unnecessary scan, never skip a partition
    /// that has expirable data. In-memory only: rebuilt from the first scan
    /// after a restart, and independent per maintain node.
    oldest_retained: Arc<Mutex<BTreeMap<Topition, i64>>>,

    /// Pure-segment retention skip (#71): the instant a maintenance scan last
    /// found a partition's legacy `records/` prefix empty under prefix coalescing.
    /// While this is within [`Self::RETENTION_EMPTY_SKIP_TTL`] the maintenance loop
    /// skips the empty per-topic LIST entirely (segment retention is handled
    /// per-prefix by `expire_prefix_segments`), instead of re-LISTing every tick
    /// forever. It SELF-HEALS: once the TTL lapses the partition is scanned again,
    /// so a legacy write that lands afterwards — even from another process (the
    /// broker and the dedicated `maintain` worker do not share this map) — is
    /// still expired within one TTL. That bounds worst-case over-retention to the
    /// TTL while cutting the empty-LIST rate to ~one per TTL. In-memory only.
    retention_empty_skip: Arc<Mutex<BTreeMap<Topition, SystemTime>>>,

    /// Per-*prefix* analogue of [`Self::oldest_retained`] for whole-segment
    /// retention (#61): the oldest surviving segment's age (ms) observed at the
    /// last scan, letting the maintenance loop skip the `segments/` LIST of a
    /// prefix whose oldest segment is still within retention. Same lower-bound
    /// soundness as the per-partition hint. In-memory only.
    oldest_retained_prefix: Arc<Mutex<BTreeMap<String, i64>>>,

    /// When set, the produce path buffers batches per partition and flushes them
    /// as one coalesced `records/` object per linger window (#50), cutting the
    /// PUT (and matching fetch GET) count on small-batch workloads. Off by
    /// default — each batch is its own object, exactly as before. Transactional,
    /// lake-sink and compacted topics always bypass the buffer.
    produce_coalesce: bool,

    /// Per-partition coalescing buffer (#50), used only when
    /// [`Self::produce_coalesce`] is set. Holds batches awaiting a flush plus the
    /// one-shot ack channels their produce calls are parked on; the map is
    /// drained (never held across an await) on a threshold or linger-timer flush.
    coalesce: Arc<Mutex<BTreeMap<Topition, CoalesceBuffer>>>,

    /// When set, eligible batches are coalesced per *connector prefix* rather
    /// than per `(topic, partition)`: one shared, immutable segment object holds
    /// interleaved batches from every topition under the prefix produced in one
    /// flush window (the #56 "virtual topics" write path, #57). This collapses
    /// PUTs from ~`(topics × flushes)` to ~`(connectors × flushes)`. Off by
    /// default; takes precedence over [`Self::produce_coalesce`] for eligible
    /// batches when both are set. The URL flag that drives it is wired by #63.
    prefix_coalesce: bool,

    /// Per-prefix coalescing buffer (#57), used only when
    /// [`Self::prefix_coalesce`] is set. Like [`Self::coalesce`] but keyed by
    /// prefix, so one buffer accumulates `PrefixPending` batches across many
    /// topitions; drained (never held across an await) on a threshold or linger
    /// flush into one create-only segment object.
    prefix_coalesce_buffers: Arc<Mutex<BTreeMap<String, PrefixCoalesceBuffer>>>,

    /// Per-prefix next segment sequence hint (#57). The segment object name
    /// `prefixes/{prefix}/segments/{seq:020}.seg` is monotonic and create-only,
    /// so — exactly as the `{offset}.batch` name is the offset authority for the
    /// legacy layout — the segment sequence is the ordering authority for the
    /// coalesced layout. A `Create` conflict resyncs the hint from the tail of
    /// the segment listing (single-writer per prefix, #59, makes conflicts a
    /// failover edge case rather than the steady state).
    segment_seqs: Arc<Mutex<BTreeMap<String, u64>>>,

    /// Per-prefix async flush lock: serializes `flush_prefix_coalesced` for a
    /// given prefix so a window's `cached_high` read → segment PUT → `set_high`
    /// is atomic. Without it two overlapping flushes (a threshold flush and a
    /// linger-timer flush) could both read the same base offset before either
    /// advanced the hint, writing two segments at the same offsets. The segment
    /// `Create` only guards the *sequence* name, not offsets, so this lock — not
    /// the create-race — is the per-prefix offset authority.
    prefix_flush_locks: Arc<Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>>,

    /// Per-prefix in-memory segment-footer index (read-path #60 review fix). See
    /// [`PrefixIndex`]: caches immutable footers so fetch/high-watermark/earliest/
    /// retention resolve without a per-call `segments/` LIST or per-segment footer
    /// GET.
    prefix_index: Arc<Mutex<BTreeMap<String, PrefixIndex>>>,

    /// Per-prefix single-writer leases this process currently holds (#59). The
    /// durable side is `prefixes/{prefix}/lease.json`; this caches the held
    /// epoch, its etag (for the next CAS) and its expiry (the no-write fast
    /// path). Consulted before every prefix flush to fence a stale writer.
    prefix_leases: Arc<Mutex<BTreeMap<String, HeldLease>>>,

    /// Per-prefix compaction leases this process holds (#66) — the maintenance
    /// side of the single-writer fence, kept separate from `prefix_leases` so
    /// compaction on a maintenance worker never touches the produce lease.
    compaction_leases: Arc<Mutex<BTreeMap<String, HeldLease>>>,

    /// Whether a topition still has legacy `records/` objects, cached (with the
    /// check time) to keep the prefix-coalesced read path off a per-fetch
    /// `records/` LIST (#60). A topic flipped to segment mode mid-life is a
    /// *hybrid*: `[0, C)` legacy objects, `[C, ∞)` segments; its fetch must serve
    /// both. A greenfield prefix has no legacy objects and must not pay that
    /// LIST. Re-checked at most once per [`Self::HIGH_WATERMARK_HINT_TTL`] so a
    /// `true` flips to `false` once retention drains the legacy region, and a
    /// `false` flips back if a txn/control batch (which always takes the legacy
    /// path) re-creates one — otherwise served from memory (no per-fetch LIST).
    legacy_records_present: Arc<Mutex<BTreeMap<Topition, (bool, SystemTime)>>>,

    /// This writer's identity, recorded in the lease `holder` field (#59) for
    /// observability. Unique per process instance so two brokers (or two test
    /// stores) are distinguishable.
    writer_id: String,

    /// Prefix-lease term length (#59). A held lease is reused without a write
    /// while more than a third of the term remains, so renewal happens ~once per
    /// `2/3 · ttl` — kept well above GCS's ~1/s/object mutation cap (#13) and
    /// never tied to the flush cadence. Defaults to [`Self::PREFIX_LEASE_TTL`];
    /// lowered in tests to exercise failover.
    prefix_lease_ttl: Duration,

    /// Runtime coalescing (#50) / producer-checkpoint (#48) flush thresholds
    /// (#54). Each is seeded from its compile-time default
    /// ([`Self::COALESCE_LINGER`], [`Self::COALESCE_BATCHES`],
    /// [`Self::COALESCE_BYTES`], [`Self::PRODUCER_CHECKPOINT_INTERVAL`],
    /// [`Self::PRODUCER_CHECKPOINT_BATCHES`]) and overridable per deployment via
    /// [`Self::coalesce_tuning`], so a high-topic-fan-out workload can widen the
    /// linger / checkpoint windows from the storage URL without recompiling.
    coalesce_linger: Duration,
    coalesce_batches: usize,
    coalesce_bytes: usize,
    producer_checkpoint_interval: Duration,
    producer_checkpoint_batches: u64,

    /// Segment-compaction thresholds (#66), each seeded from its compile-time
    /// default and overridable per deployment via [`Self::coalesce_tuning`].
    /// `prefix_compact_min_segments == 0` disables compaction.
    prefix_compact_min_segments: usize,
    prefix_compact_target_bytes: usize,
    prefix_compact_keep_hot: usize,

    object_store: Arc<DynObjectStore>,
}

/// A batch parked in the coalescing buffer (#50) with the one-shot the producing
/// `produce` call awaits its assigned base offset on.
#[derive(Debug)]
struct Pending {
    batch: deflated::Batch,
    ack: oneshot::Sender<Result<i64>>,
}

/// Per-partition accumulator for coalesced produce (#50): batches awaiting a
/// flush plus the running offset span and byte size used for the flush triggers.
#[derive(Debug, Default)]
struct CoalesceBuffer {
    pending: Vec<Pending>,
    records: i64,
    bytes: usize,
}

/// A batch parked in the prefix coalescing buffer (#57), carrying the topition
/// it belongs to (a prefix buffer multiplexes many) alongside the one-shot the
/// producing `produce` call awaits its assigned offset on.
#[derive(Debug)]
struct PrefixPending {
    topition: Topition,
    batch: deflated::Batch,
    ack: oneshot::Sender<Result<i64>>,
}

/// Per-prefix accumulator for prefix-coalesced produce (#57): batches from every
/// topition under the prefix awaiting a flush, plus the running record and byte
/// counts used for the flush triggers. Flushed as one shared segment object.
#[derive(Debug, Default)]
struct PrefixCoalesceBuffer {
    pending: Vec<PrefixPending>,
    records: i64,
    bytes: usize,
}

/// The durable per-prefix single-writer lease (#59), stored at
/// `clusters/{cluster}/prefixes/{prefix}/lease.json`. Its etag is the fence: a
/// writer takes/renews the lease with a conditional PUT, and a stale holder's
/// CAS fails `Precondition`, so at most one writer is live per prefix — with no
/// external coordinator. `epoch` is bumped on every (re)acquire and stamped into
/// each segment; `expires_at_ms` bounds how long a crashed holder blocks a
/// takeover. The object is CAS-**mutated**, so renewal stays well under GCS's
/// ~1/s/object mutation cap (#13): it renews once per lease term, never per
/// flush.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct PrefixLease {
    epoch: i64,
    holder: String,
    expires_at_ms: i64,
}

/// In-memory index of a connector prefix's segments (read-path #60 review fix):
/// segment footers are immutable, so once read they are cached by sequence
/// forever. Locate (fetch), high-watermark, earliest and retention all resolve
/// from this cache instead of LISTing `segments/` and GETting every footer on
/// each call. The writer populates it on flush and prunes it on retention, so on
/// the (single-node) write path reads cost zero object requests; a reader on a
/// cold cache (or another broker) refreshes it with a TTL'd **incremental** list
/// (`start_after` the highest known sequence), fetching only *new* footers — so
/// steady-state refresh is O(new), not O(total segments). Deleted (expired)
/// segments are pruned lazily when a data GET 404s.
#[derive(Clone, Debug)]
struct CachedSegment {
    footer: SegmentFooter,
    /// The segment object's append time (ms), used as the retention age when a
    /// sub-stream's record timestamps are unset. The writer stamps ~`now` on
    /// insert; a refresh takes the listing's `last_modified`.
    last_modified_ms: i64,
}

#[derive(Clone, Debug, Default)]
struct PrefixIndex {
    /// `seq -> segment` for every segment known to this process. Immutable
    /// content, so an entry never needs re-reading.
    segments: BTreeMap<u64, CachedSegment>,
    /// When the live segment set was last reconciled by a listing; gates the
    /// TTL so a hot prefix lists at most once per [`DynoStore::HIGH_WATERMARK_HINT_TTL`].
    refreshed_at: Option<SystemTime>,
}

/// A prefix lease this process currently holds (#59): the in-memory side of
/// [`PrefixLease`]. `version` is the etag the next renewal CASes against;
/// `expires_at` gates the no-write fast path so a live term is reused without
/// touching the object.
#[derive(Clone, Debug)]
struct HeldLease {
    epoch: i64,
    expires_at: SystemTime,
    version: Option<UpdateVersion>,
}

/// Debounce accounting for one producer's lazy `producers/{id}.json` checkpoint
/// (see [`DynoStore::producer_checkpoints`], #48).
#[derive(Clone, Copy, Debug)]
struct ProducerCheckpoint {
    batches_since_flush: u64,
    last_flush: SystemTime,
}

/// Per-deployment overrides for the produce-coalescing (#50) and
/// producer-checkpoint (#48) flush thresholds (#54), applied via
/// [`DynoStore::coalesce_tuning`]. A `None` field keeps that trigger's
/// compile-time default, so omitting every key reproduces the shipped
/// behaviour. Populated from the storage URL query string; see the storage
/// tuning docs for the fan-out tradeoff these expose.
#[derive(Clone, Copy, Debug, Default)]
pub struct CoalesceTuning {
    pub coalesce_linger: Option<Duration>,
    pub coalesce_batches: Option<usize>,
    pub coalesce_bytes: Option<usize>,
    pub producer_checkpoint_interval: Option<Duration>,
    pub producer_checkpoint_batches: Option<u64>,
    pub prefix_compact_min_segments: Option<usize>,
    pub prefix_compact_target_bytes: Option<usize>,
    pub prefix_compact_keep_hot: Option<usize>,
}

/// Process-wide counter making each [`DynoStore`]'s `writer_id` unique (#59), so
/// two stores in one process (or two brokers) are distinguishable holders in a
/// prefix lease.
static WRITER_INSTANCE: AtomicU64 = AtomicU64::new(0);

/// Segments written (one create-only PUT per flush window per prefix, #57).
static SEGMENT_FLUSHES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_flushes")
        .with_description("prefix-coalesced segment objects written")
        .build()
});

/// Segments merged away by compaction (#66) — bounds live segment count.
static SEGMENT_COMPACTIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_compactions")
        .with_description("segments merged away by prefix compaction")
        .build()
});

/// Live segment count per prefix after a maintenance tick (#66) — the signal
/// that tells whether compaction is keeping `S` bounded (a counter can't).
static SEGMENTS_LIVE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_segments_live")
        .with_description("live segments for a prefix after maintenance")
        .build()
});

/// Prefix leases acquired or renewed (#59).
static LEASE_ACQUIRES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_lease_acquires")
        .with_description("prefix single-writer lease acquisitions/renewals")
        .build()
});

/// Times this writer was fenced off a prefix lease (#59) — a nonzero rate means
/// contention/failover, so it is worth alerting on.
static LEASE_FENCED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_lease_fenced")
        .with_description("prefix lease acquisitions lost to another writer (fenced)")
        .build()
});

/// Deterministic owner node for a connector prefix (#59): rendezvous
/// (highest-random-weight) hashing over the broker set — a pure function with no
/// shared state, so every broker computes the same owner, and adding/removing a
/// broker moves only ~1/N of prefixes. A multi-broker deployment routes a
/// prefix's produce to this node to avoid lease contention; the lease
/// (`acquire_or_renew_lease`) is the actual single-writer *guarantee*, so
/// assignment is an optimization, not the fence. This single-node broker
/// (node 111) trivially owns every prefix, so the routing layer that would
/// consult this is out of scope here. Orthogonal to consumer-group coordination
/// (still node 111). `None` iff `nodes` is empty.
#[allow(dead_code)]
fn prefix_owner_node(prefix: &str, nodes: &[i32]) -> Option<i32> {
    fn weight(prefix: &str, node: i32) -> u64 {
        // FNV-1a over the prefix bytes then the node, mixing both so the winner
        // is stable per (prefix, node-set) and independent of node ordering.
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in prefix.as_bytes().iter().chain(node.to_be_bytes().iter()) {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }

    nodes
        .iter()
        .copied()
        .max_by_key(|node| weight(prefix, *node))
}

/// Magic trailer word marking a prefix-coalesced multi-topic segment object
/// (#64), distinguishing a `.seg` from a legacy single-topic coalesced object
/// (#50), which carries no trailer. ASCII `TSEG`.
const SEGMENT_MAGIC: u32 = 0x5453_4547;

/// On-disk version of the segment frame + footer format (#64). Version `0` is
/// the implicit legacy single-topic layout (a bare batch concatenation with no
/// trailer, produced by #50); `1` is the first self-describing multi-topic
/// segment. Versioned so future footer fields stay forward-compatible.
const SEGMENT_FORMAT_VERSION: u16 = 1;

/// Fixed-size trailer at the very end of every multi-topic segment (#64):
/// `footer_len (u64) + entry_count (u32) + version (u16) + magic (u32)`. A
/// reader recovers the index with one ranged GET of the last
/// [`SEGMENT_TRAILER_LEN`] bytes, then a second ranged GET of the `footer_len`
/// bytes immediately preceding it — never downloading the record body.
const SEGMENT_TRAILER_LEN: usize =
    size_of::<u64>() + size_of::<u32>() + size_of::<u16>() + size_of::<u32>();

/// One `(topic, partition)` sub-stream's self-describing entry in a segment
/// footer (#64): where its batches live in the shared object and what offset
/// span they cover. This is what the fetch path (#60) and cold-start offset
/// recovery (#58) read, instead of deriving offsets from the object filename
/// (the legacy `{offset}.batch` authority).
#[derive(Clone, Debug, Eq, PartialEq)]
struct SubstreamEntry {
    topic: String,
    partition: i32,
    /// Absolute base offset of this sub-stream's first record in the segment.
    base_offset: i64,
    /// Offsets this sub-stream occupies
    /// (`last_offset == base_offset + record_count - 1`).
    record_count: i64,
    /// Byte offset of this sub-stream's contiguous region within the segment.
    byte_start: u64,
    /// Byte length of that region (its batches, wire-encoded and concatenated).
    byte_len: u64,
    /// Greatest record timestamp in the sub-stream, read by per-prefix
    /// whole-segment retention (#61) to decide expiry without a body read.
    max_timestamp: i64,
}

/// The self-describing footer index of a prefix-coalesced segment (#64): one
/// [`SubstreamEntry`] per `(topic, partition)` multiplexed into the shared
/// object, plus the epoch of the writer that produced it (#59). Serialized at
/// the segment tail ahead of the [`SEGMENT_TRAILER_LEN`] trailer and treated as
/// the published external-reader contract (kotatsu#82).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SegmentFooter {
    /// The lease epoch of the writer that produced this segment (#59). `0` when
    /// prefix leasing is not in effect. Stamped so a stale-epoch segment (from a
    /// fenced writer) is identifiable on read/recovery.
    writer_epoch: i64,
    entries: Vec<SubstreamEntry>,
}

impl SegmentFooter {
    /// The entry for a `(topic, partition)` sub-stream, if it is present in this
    /// segment. `None` means the segment holds no records for that topition.
    fn get(&self, topic: &str, partition: i32) -> Option<&SubstreamEntry> {
        self.entries
            .iter()
            .find(|entry| entry.topic == topic && entry.partition == partition)
    }
}

type Group = String;
type Offset = i64;
type Partition = i32;
type ProducerEpoch = i16;
type ProducerId = i64;
type Sequence = i32;
type Topic = String;

/// Per-partition next-offset hint (see [`DynoStore::next_offsets`]).
///
/// `next` is the cached next offset to assign (== the high watermark) and is the
/// authority for offset *assignment* only as a starting candidate — the true
/// authority is the immutable batch objects. `listed_at` records when `next` was
/// last reconciled against an authoritative tail *listing* (not merely advanced
/// by a local produce): the high-watermark read path serves from `next` without
/// listing while `listed_at` is within [`DynoStore::HIGH_WATERMARK_HINT_TTL`],
/// so a batch produced on *another* replica becomes visible within that bound.
#[derive(Clone, Copy, Debug, Default)]
struct OffsetHint {
    next: i64,
    listed_at: Option<SystemTime>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Meta {
    producers: BTreeMap<ProducerId, ProducerDetail>,
    transactions: BTreeMap<String, Txn>,
}

impl OptiCon<Meta> {
    fn new(cluster: &str) -> Self {
        Self::path(format!("clusters/{cluster}/meta.json"))
    }
}

impl Meta {
    fn produced(
        &self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<BTreeMap<Topition, TxnProduceOffset>> {
        let Some(txn) = self.transactions.get(transaction_id) else {
            return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
        };

        if txn.producer != producer_id {
            return Err(Error::Api(ErrorCode::UnknownProducerId));
        }

        let Some(txn_detail) = txn.epochs.get(&producer_epoch) else {
            return Err(Error::Api(ErrorCode::ProducerFenced));
        };

        let mut produced = BTreeMap::new();

        for (topic, partitions) in txn_detail.produces.iter() {
            for (partition, offset_range) in partitions.iter() {
                let Some(offset_range) = offset_range else {
                    continue;
                };

                let tp = Topition::new(topic.to_owned(), *partition);
                assert_eq!(None, produced.insert(tp, *offset_range));
            }
        }

        Ok(produced)
    }

    fn overlapping_transactions(
        &self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<Vec<TxnId>> {
        let candidates = self.produced(transaction_id, producer_id, producer_epoch)?;

        let mut overlapping = Vec::new();

        'candidates: for (candidate_id, txn) in self.transactions.iter() {
            for (epoch, txn_detail) in txn.epochs.iter() {
                if transaction_id == candidate_id
                    && producer_id == txn.producer
                    && producer_epoch == *epoch
                {
                    continue;
                }

                let Some(state) = txn_detail.state else {
                    continue;
                };

                for (topic, partitions) in txn_detail.produces.iter() {
                    for (partition, offset_range) in partitions.iter() {
                        let Some(offset_range) = offset_range else {
                            continue;
                        };

                        let tp = Topition::new(topic.to_owned(), *partition);

                        if let Some(candidate) = candidates.get(&tp)
                            && offset_range.offset_start < candidate.offset_end
                        {
                            overlapping.push(TxnId {
                                transaction: candidate_id.to_owned(),
                                producer_id: txn.producer,
                                producer_epoch: *epoch,
                                state,
                            });

                            continue 'candidates;
                        }
                    }
                }
            }
        }

        Ok(overlapping)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct ProducerDetail {
    sequences: BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, Sequence>>>,
}

impl ProducerDetail {
    /// Fold `other` into `self` by taking the element-wise maximum sequence per
    /// (epoch, topic, partition). Idempotent sequences are monotonic, so a
    /// max-merge reconciles a lagging or concurrently-written checkpoint without
    /// ever lowering an already-acked sequence. Used as the `reconcile` closure
    /// of [`OptiCon::checkpoint`] on the lazy `producers/{id}.json` flush (#48).
    fn reconcile(&mut self, other: &ProducerDetail) {
        for (epoch, topics) in &other.sequences {
            let dst_topics = self.sequences.entry(*epoch).or_default();
            for (topic, partitions) in topics {
                let dst_partitions = dst_topics.entry(topic.clone()).or_default();
                for (partition, sequence) in partitions {
                    let dst = dst_partitions.entry(*partition).or_default();
                    *dst = (*dst).max(*sequence);
                }
            }
        }
    }
}

impl OptiCon<ProducerDetail> {
    fn new(cluster: &str, producer_id: ProducerId) -> Self {
        Self::path(format!("clusters/{cluster}/producers/{producer_id}.json"))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnId {
    transaction: String,
    producer_id: ProducerId,
    producer_epoch: ProducerEpoch,
    state: TxnState,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Txn {
    producer: ProducerId,
    epochs: BTreeMap<ProducerEpoch, TxnDetail>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnDetail {
    transaction_timeout_ms: i32,
    started_at: Option<SystemTime>,
    state: Option<TxnState>,
    produces: BTreeMap<Topic, BTreeMap<Partition, Option<TxnProduceOffset>>>,
    offsets: BTreeMap<Group, BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>>,
}

impl From<&TxnDetail> for BTreeMap<Topition, Offset> {
    fn from(value: &TxnDetail) -> Self {
        let mut result = BTreeMap::new();

        for (topic, partitions) in value.produces.iter() {
            for (partition, offset_range) in partitions.iter() {
                let Some(offset_range) = offset_range else {
                    continue;
                };

                let tp = Topition::new(topic.to_owned(), *partition);
                assert_eq!(None, result.insert(tp, offset_range.offset_start));
            }
        }

        result
    }
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
struct TxnProduceOffset {
    offset_start: Offset,
    offset_end: Offset,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnCommitOffset {
    committed_offset: Offset,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TopicMetadata {
    id: Uuid,
    topic: CreatableTopic,
}

impl TopicMetadata {
    /// Apply an incremental config change in place (used under the per-topic
    /// `OptiCon::with_mut` CAS). Mirrors Kafka `IncrementalAlterConfigs`
    /// Set/Delete semantics on `topic.configs`.
    fn alter_configs(&mut self, changes: &[AlterableConfig]) -> Result<()> {
        let mut configuration = self
            .topic
            .configs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .fold(BTreeMap::new(), |mut acc, item| {
                _ = acc.insert(item.name.as_str(), item.value.as_deref());
                acc
            });

        for change in changes {
            match OpType::try_from(change.config_operation)? {
                OpType::Set => {
                    _ = configuration.insert(change.name.as_str(), change.value.as_deref());
                }
                OpType::Delete => {
                    _ = configuration.remove(change.name.as_str());
                }
                OpType::Append => todo!(),
                OpType::Subtract => todo!(),
            }
        }

        _ = self.topic.configs.replace(configuration.into_iter().fold(
            Vec::new(),
            |mut acc, (key, value)| {
                acc.push(
                    CreatableTopicConfig::default()
                        .name(key.to_owned())
                        .value(value.map(|value| value.to_owned())),
                );
                acc
            },
        ));

        Ok(())
    }
}

impl OptiCon<TopicMetadata> {
    fn new(cluster: &str, name: &str) -> Self {
        Self::path(format!("clusters/{cluster}/topic-metadata/{name}.json"))
    }
}

/// Pointer object at `topic-ids/{uuid}.json` mapping a topic's id back to its
/// name, so a metadata lookup by topic-id can resolve to the per-topic
/// `topic-metadata/{name}.json` object. Written create-only alongside the
/// topic in [`DynoStore::create_topic`].
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TopicIdRef {
    name: Topic,
}

/// In-memory topic index (see [`DynoStore::topic_index`]). `entries` maps a
/// topic name to its last-seen object etag and decoded metadata, used to skip
/// re-GETting unchanged objects on refresh; `snapshot` is the shared, ready-to-
/// serve list reused by every list-all caller between refreshes.
#[derive(Debug, Default)]
struct TopicIndex {
    entries: BTreeMap<Topic, (Option<String>, TopicMetadata)>,
    snapshot: Arc<Vec<TopicMetadata>>,
    refreshed_at: Option<SystemTime>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Watermark {
    low: Option<i64>,
    high: Option<i64>,
}

impl OptiCon<Watermark> {
    fn new(cluster: &str, topition: &Topition) -> Self {
        Self::path(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/watermark.json",
            cluster, topition.topic, topition.partition,
        ))
    }
}

fn json_content_type() -> Attributes {
    let mut attributes = Attributes::new();
    _ = attributes.insert(
        Attribute::ContentType,
        AttributeValue::from(APPLICATION_JSON),
    );
    attributes
}

impl DynoStore {
    pub fn new(cluster: &str, node: i32, object_store: impl ObjectStore) -> Self {
        Self {
            cluster: cluster.into(),
            node,
            advertised_listener: Url::parse("tcp://127.0.0.1/").unwrap(),
            schemas: None,

            lake: None,

            watermarks: Arc::new(Mutex::new(BTreeMap::new())),
            next_offsets: Arc::new(Mutex::new(BTreeMap::new())),
            producers: Arc::new(Mutex::new(BTreeMap::new())),
            producer_checkpoints: Arc::new(Mutex::new(BTreeMap::new())),
            oldest_retained: Arc::new(Mutex::new(BTreeMap::new())),
            retention_empty_skip: Arc::new(Mutex::new(BTreeMap::new())),
            oldest_retained_prefix: Arc::new(Mutex::new(BTreeMap::new())),
            produce_coalesce: false,
            coalesce: Arc::new(Mutex::new(BTreeMap::new())),
            prefix_coalesce: false,
            prefix_coalesce_buffers: Arc::new(Mutex::new(BTreeMap::new())),
            segment_seqs: Arc::new(Mutex::new(BTreeMap::new())),
            prefix_flush_locks: Arc::new(Mutex::new(BTreeMap::new())),
            prefix_index: Arc::new(Mutex::new(BTreeMap::new())),
            prefix_leases: Arc::new(Mutex::new(BTreeMap::new())),
            compaction_leases: Arc::new(Mutex::new(BTreeMap::new())),
            legacy_records_present: Arc::new(Mutex::new(BTreeMap::new())),
            writer_id: format!(
                "{node}-{}",
                WRITER_INSTANCE.fetch_add(1, atomic::Ordering::Relaxed)
            ),
            prefix_lease_ttl: Self::PREFIX_LEASE_TTL,
            coalesce_linger: Self::COALESCE_LINGER,
            coalesce_batches: Self::COALESCE_BATCHES,
            coalesce_bytes: Self::COALESCE_BYTES,
            producer_checkpoint_interval: Self::PRODUCER_CHECKPOINT_INTERVAL,
            producer_checkpoint_batches: Self::PRODUCER_CHECKPOINT_BATCHES,
            prefix_compact_min_segments: Self::PREFIX_COMPACT_MIN_SEGMENTS,
            prefix_compact_target_bytes: Self::PREFIX_COMPACT_TARGET_BYTES,
            prefix_compact_keep_hot: Self::PREFIX_COMPACT_KEEP_HOT,
            topic_metas: Arc::new(Mutex::new(BTreeMap::new())),
            topic_index: Arc::new(Mutex::new(TopicIndex::default())),
            topic_index_refresh: Arc::new(tokio::sync::Mutex::new(())),
            topic_ids: Arc::new(Mutex::new(BTreeMap::new())),
            auto_create: AutoTopicCreate::default(),
            meta: OptiCon::<Meta>::new(cluster),
            object_store: Arc::new(Cache::new(
                Metron::new(object_store, cluster),
                Duration::from_millis(5_000),
            )),
        }
    }

    pub fn advertised_listener(self, advertised_listener: Url) -> Self {
        Self {
            advertised_listener,
            ..self
        }
    }

    pub fn schemas(self, schemas: Option<Registry>) -> Self {
        Self { schemas, ..self }
    }

    pub fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }

    pub fn auto_create(self, auto_create: AutoTopicCreate) -> Self {
        Self {
            auto_create,
            ..self
        }
    }

    /// Enable server-side produce coalescing (#50): buffer batches per partition
    /// and flush them as one `records/` object per linger window. Off by default.
    pub fn produce_coalesce(self, produce_coalesce: bool) -> Self {
        Self {
            produce_coalesce,
            ..self
        }
    }

    /// Enable prefix-coalesced "virtual topics" produce (#56/#57): buffer
    /// eligible batches per connector prefix and flush them as one shared,
    /// immutable segment object per linger window, collapsing the PUT count from
    /// ~`(topics × flushes)` to ~`(connectors × flushes)`. Off by default; takes
    /// precedence over [`Self::produce_coalesce`] for eligible batches. The URL
    /// flag driving this is wired by #63.
    pub fn prefix_coalesce(self, prefix_coalesce: bool) -> Self {
        Self {
            prefix_coalesce,
            ..self
        }
    }

    /// Override the prefix single-writer lease term (#59). Kept above ~1s in
    /// production so lease renewal stays under GCS's per-object mutation cap
    /// (#13); lowered only in tests to exercise failover/fencing quickly.
    pub fn prefix_lease_ttl(self, prefix_lease_ttl: Duration) -> Self {
        Self {
            prefix_lease_ttl,
            ..self
        }
    }

    /// Override the coalescing (#50) / producer-checkpoint (#48) flush
    /// thresholds (#54). Each `None` in `tuning` leaves that trigger at its
    /// current value (the compile-time default), so an all-default `tuning` is a
    /// no-op and reproduces the shipped behaviour.
    pub fn coalesce_tuning(self, tuning: CoalesceTuning) -> Self {
        Self {
            coalesce_linger: tuning.coalesce_linger.unwrap_or(self.coalesce_linger),
            coalesce_batches: tuning.coalesce_batches.unwrap_or(self.coalesce_batches),
            coalesce_bytes: tuning.coalesce_bytes.unwrap_or(self.coalesce_bytes),
            producer_checkpoint_interval: tuning
                .producer_checkpoint_interval
                .unwrap_or(self.producer_checkpoint_interval),
            producer_checkpoint_batches: tuning
                .producer_checkpoint_batches
                .unwrap_or(self.producer_checkpoint_batches),
            prefix_compact_min_segments: tuning
                .prefix_compact_min_segments
                .unwrap_or(self.prefix_compact_min_segments),
            prefix_compact_target_bytes: tuning
                .prefix_compact_target_bytes
                .unwrap_or(self.prefix_compact_target_bytes),
            prefix_compact_keep_hot: tuning
                .prefix_compact_keep_hot
                .unwrap_or(self.prefix_compact_keep_hot),
            ..self
        }
    }

    /// Optimistic-concurrency handle on a topic's `topic-metadata/{name}.json`.
    fn topic_meta(&self, name: &str) -> Result<OptiCon<TopicMetadata>> {
        self.topic_metas
            .lock()
            .map(|mut locked| {
                locked
                    .entry(name.to_owned())
                    .or_insert_with(|| OptiCon::<TopicMetadata>::new(self.cluster.as_str(), name))
                    .to_owned()
            })
            .map_err(Into::into)
    }

    fn topic_id_path(&self, id: &Uuid) -> Path {
        Path::from(format!("clusters/{}/topic-ids/{}.json", self.cluster, id))
    }

    fn topic_metadata_path(&self, name: &str) -> Path {
        Path::from(format!(
            "clusters/{}/topic-metadata/{}.json",
            self.cluster, name
        ))
    }

    /// Marker object recording that the one-shot legacy-metadata backfill has
    /// run. Kept outside the `topic-metadata/` prefix so it is never returned by
    /// [`Self::all_topics`]'s listing.
    fn topic_metadata_migration_marker(&self) -> Path {
        Path::from(format!(
            "clusters/{}/.migrations/topic-metadata",
            self.cluster
        ))
    }

    /// Create-only PUT: `Ok(true)` if this call created the object, `Ok(false)`
    /// if it already existed.
    async fn put_create(&self, path: &Path, payload: PutPayload) -> Result<bool> {
        match self
            .object_store
            .put_opts(
                path,
                payload,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// Resolve a topic-id to its name: in-memory cache first, then the
    /// `topic-ids/{uuid}.json` pointer (result cached). The mapping is immutable
    /// for a topic's lifetime, so the cache is safe until delete.
    async fn topic_name_by_id(&self, id: &Uuid) -> Result<Option<Topic>> {
        if let Ok(cache) = self.topic_ids.lock()
            && let Some(name) = cache.get(id)
        {
            return Ok(Some(name.clone()));
        }

        match self.object_store.get(&self.topic_id_path(id)).await {
            Ok(get_result) => {
                let encoded = get_result.bytes().await?;
                let name = serde_json::from_slice::<TopicIdRef>(&encoded)?.name;
                if let Ok(mut cache) = self.topic_ids.lock() {
                    _ = cache.insert(*id, name.clone());
                }
                Ok(Some(name))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// Drop a cached topic-id -> name mapping (on delete).
    fn invalidate_topic_id(&self, id: &Uuid) {
        if let Ok(mut cache) = self.topic_ids.lock() {
            _ = cache.remove(id);
        }
    }

    /// Every topic's metadata, by listing the `topic-metadata/` prefix and
    /// reading each object. Used by the list-all Metadata path and the cleanup
    /// policies; not on the produce/fetch hot path. The prefix holds only the
    /// per-topic metadata objects (topic *data* lives under `topics/`), so the
    /// listing does not scan record objects.
    /// How long a [`TopicIndex`] snapshot is served before a refresh. Bounds the
    /// cross-replica staleness of the list-all metadata view (a topic created on
    /// another replica appears within this window); the by-name path is always
    /// fresh via the per-topic object.
    const TOPIC_INDEX_TTL: Duration = Duration::from_secs(5);

    /// How long a per-partition high-watermark hint is served from memory before
    /// the read path re-lists the tail (see [`OffsetHint`] / [`Self::cached_high_fresh`]).
    /// Bounds the cross-replica staleness of the read-uncommitted high watermark:
    /// a batch produced on another replica becomes visible within this window,
    /// while a caught-up consumer long-polling an idle partition issues no
    /// `ListObjectsV2` per poll in steady state (#40). Read-committed is
    /// unaffected in semantics — a stale (lower) high watermark can only *delay*
    /// visibility, never expose unstable offsets.
    const HIGH_WATERMARK_HINT_TTL: Duration = Duration::from_secs(5);

    /// How long a pure-segment partition is skipped by retention after a scan
    /// found its legacy `records/` prefix empty (#71), before it is re-scanned
    /// once. This is the worst-case over-retention of a legacy object later
    /// written to such a partition by another process (see
    /// [`Self::retention_empty_skip`]); one hour is negligible against the
    /// multi-day `retention.ms` typical of CDC while cutting the empty per-topic
    /// LIST rate on the maintainer by roughly this window over the maintenance
    /// interval.
    const RETENTION_EMPTY_SKIP_TTL: Duration = Duration::from_secs(60 * 60);

    /// Default debounce window for persisting a producer's `producers/{id}.json`
    /// (#48): the durable object is checkpointed after at most this many
    /// idempotent batches have advanced its in-memory sequence. Overridable per
    /// deployment via `producer_checkpoint_batches` (#54).
    const PRODUCER_CHECKPOINT_BATCHES: u64 = 64;

    /// Time-based companion to [`Self::PRODUCER_CHECKPOINT_BATCHES`]: a producer
    /// that keeps advancing below the batch threshold is still checkpointed at
    /// least this often, bounding the unclean-crash replay window. Overridable
    /// via `producer_checkpoint_interval` (#54).
    const PRODUCER_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(250);

    /// Default coalescing flush triggers (#50), whichever is reached first:
    /// linger time, batch count, byte size, or offset span. `COALESCE_LINGER`,
    /// `COALESCE_BATCHES` and `COALESCE_BYTES` are overridable per deployment via
    /// `coalesce_linger` / `coalesce_batches` / `coalesce_bytes` (#54). The span
    /// cap ([`Self::COALESCE_MAX_RECORDS`]) stays a fixed safety bound: it also
    /// bounds how far back [`Self::fetch`] must probe to find the object
    /// containing a mid-frame offset.
    const COALESCE_LINGER: Duration = Duration::from_millis(50);
    const COALESCE_BATCHES: usize = 64;
    const COALESCE_BYTES: usize = 1 << 20;
    const COALESCE_MAX_RECORDS: i64 = 100_000;

    /// Default prefix-lease term (#59). Renewal happens once ~`2/3 · ttl`
    /// remains, i.e. every ~7s — well under GCS's ~1/s/object mutation cap (#13)
    /// — while a crashed holder blocks a takeover for at most one term.
    const PREFIX_LEASE_TTL: Duration = Duration::from_secs(10);

    /// A batch this large bypasses the prefix segment buffer and takes the legacy
    /// per-object create path (#62 backfill). CDC steady-state batches fan a
    /// handful of events per topition (well below this) and coalesce into
    /// segments; a snapshot's bulk batches are already S3-efficient alone and
    /// take the parallel create path. Between the two regimes by a wide margin,
    /// so the exact value only shifts a throughput/PUT tradeoff, never
    /// correctness.
    const PREFIX_BACKFILL_MIN_RECORDS: i64 = 1_000;

    /// Compact a prefix's segments once it holds more than this many live ones
    /// (#66), bounding `S` (segments per prefix ≈ flush_rate × retention, which
    /// is otherwise unbounded) so the footer index footprint and per-fetch scan
    /// stay bounded. `0` disables compaction.
    const PREFIX_COMPACT_MIN_SEGMENTS: usize = 256;

    /// Target byte size of a merged segment (#66): the oldest eligible run is
    /// merged until it reaches this, then written as one create-only object.
    const PREFIX_COMPACT_TARGET_BYTES: usize = 64 << 20;

    /// Newest segments never compacted (#66): leaves the actively-produced tail
    /// alone so compaction never races the current write point.
    const PREFIX_COMPACT_KEEP_HOT: usize = 16;

    /// All topics, served from the in-memory [`TopicIndex`]. Returns the shared
    /// snapshot if fresh; otherwise refreshes it (single-flight) by LISTing the
    /// `topic-metadata/` prefix and GETting only the objects whose etag changed.
    /// Used by the list-all metadata path and the cleanup policies — never on
    /// the produce/fetch hot path.
    async fn topics_index(&self) -> Result<Arc<Vec<TopicMetadata>>> {
        if let Some(snapshot) = self.fresh_topic_index()? {
            return Ok(snapshot);
        }

        // Stale or empty: one task refreshes, the rest await and reuse it.
        let _guard = self.topic_index_refresh.lock().await;
        if let Some(snapshot) = self.fresh_topic_index()? {
            return Ok(snapshot);
        }
        self.refresh_topic_index().await
    }

    /// The cached snapshot iff it was refreshed within [`Self::TOPIC_INDEX_TTL`].
    fn fresh_topic_index(&self) -> Result<Option<Arc<Vec<TopicMetadata>>>> {
        let index = self.topic_index.lock()?;
        let fresh = index.refreshed_at.is_some_and(|at| {
            SystemTime::now()
                .duration_since(at)
                .is_ok_and(|elapsed| elapsed < Self::TOPIC_INDEX_TTL)
        });
        Ok(fresh.then(|| index.snapshot.clone()))
    }

    /// Rebuild the index: LIST the prefix once, reuse cached entries whose etag
    /// is unchanged, GET only the new/changed objects, and drop deleted ones.
    async fn refresh_topic_index(&self) -> Result<Arc<Vec<TopicMetadata>>> {
        let prefix = Path::from(format!("clusters/{}/topic-metadata/", self.cluster));
        let listed = self.object_store.list_with_delimiter(Some(&prefix)).await?;

        let mut entries: BTreeMap<Topic, (Option<String>, TopicMetadata)> = BTreeMap::new();
        let mut stale = Vec::new();

        {
            let index = self.topic_index.lock()?;
            for object in &listed.objects {
                let Some(name) = object
                    .location
                    .filename()
                    .and_then(|file| file.strip_suffix(".json"))
                else {
                    continue;
                };

                match index.entries.get(name) {
                    Some(cached @ (etag, _)) if etag.is_some() && *etag == object.e_tag => {
                        _ = entries.insert(name.to_owned(), cached.clone());
                    }
                    _ => stale.push((
                        name.to_owned(),
                        object.location.clone(),
                        object.e_tag.clone(),
                    )),
                }
            }
        }

        // GET only the new/changed objects (no lock held), with a bounded
        // fan-out. The cold build — every topic stale on the first refresh —
        // is otherwise O(topics) *sequential* round-trips: ~6s for 5k objects
        // on local minio and tens of seconds against real S3, which (now that
        // the warm-up runs this before the listener opens) would stretch boot
        // unacceptably at 15k topics. A small concurrency keeps it to a few
        // seconds without re-creating a request burst.
        const FETCH_EACH_CONCURRENCY: usize = 32;

        let object_store = &self.object_store;

        let fetched = futures::stream::iter(stale)
            .map(|(name, location, etag)| async move {
                let encoded = object_store.get(&location).await?.bytes().await?;
                let metadata = serde_json::from_slice::<TopicMetadata>(&encoded)?;
                Ok::<_, Error>((name, (etag, metadata)))
            })
            .buffer_unordered(FETCH_EACH_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;

        for (name, value) in fetched {
            _ = entries.insert(name, value);
        }

        let snapshot = Arc::new(
            entries
                .values()
                .map(|(_, metadata)| metadata.clone())
                .collect::<Vec<_>>(),
        );

        {
            let mut index = self.topic_index.lock()?;
            index.entries = entries;
            index.snapshot = snapshot.clone();
            index.refreshed_at = Some(SystemTime::now());
        }

        Ok(snapshot)
    }

    /// Force the next [`Self::topics_index`] to refresh (after a local create or
    /// delete), so the change is reflected without waiting out the TTL.
    fn invalidate_topic_index(&self) {
        if let Ok(mut index) = self.topic_index.lock() {
            index.refreshed_at = None;
        }
    }

    /// Best-effort warm-up of the topic index at boot, run from
    /// [`Storage::register_broker`] which completes *before* the broker binds
    /// its listener. Without it, the first list-all metadata request pays the
    /// full cold build — LIST the `topic-metadata/` prefix and GET every object
    /// — which at 15k topics is several seconds and can exceed a Kafka client's
    /// first metadata timeout. Paying it here keeps the port closed (so the pod
    /// is not yet ready for traffic) until the index is hot.
    ///
    /// Best-effort by design: a transient object-store error at boot must not
    /// stop the broker starting. On failure the index stays empty and the lazy
    /// [`Self::topics_index`] path rebuilds it on the first request.
    async fn warm_topic_index(&self) {
        let started = SystemTime::now();
        match self.refresh_topic_index().await {
            Ok(snapshot) => info!(
                topics = snapshot.len(),
                elapsed_ms = started.elapsed().ok().map(|elapsed| elapsed.as_millis()),
                "topic index warmed"
            ),
            Err(err) => warn!(
                ?err,
                "topic index warm-up failed; building lazily on first request"
            ),
        }
    }

    /// One-shot, idempotent backfill from the legacy monolithic `meta.json`
    /// (which embedded a `topics` map) to per-topic `topic-metadata/{name}.json`
    /// objects plus their `topic-ids/{uuid}.json` pointers.
    ///
    /// Safe to run on every boot and from every replica concurrently: each
    /// object is written create-only, so an already migrated (or freshly
    /// created) topic is skipped. A cluster with no legacy `meta.json`, or whose
    /// topics are already decomposed, is a no-op. The legacy `topics` bytes are
    /// left in `meta.json` untouched — they are dead data the current `Meta`
    /// deserialiser ignores.
    async fn migrate_legacy_topic_metadata(&self) -> Result<()> {
        let marker = self.topic_metadata_migration_marker();

        // Fast path: a prior boot already backfilled. Without this, every
        // restart re-loads `meta.json` and re-attempts a create per topic — a
        // ~O(topics) startup cost (and memory spike) that, on a large cluster,
        // is what tipped the broker over its memory limit and crash-looped it.
        match self.object_store.head(&marker).await {
            Ok(_) => return Ok(()),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(otherwise) => return Err(otherwise.into()),
        }

        #[derive(Deserialize)]
        struct LegacyMeta {
            #[serde(default)]
            topics: BTreeMap<Topic, TopicMetadata>,
        }

        let path = Path::from(format!("clusters/{}/meta.json", self.cluster));

        let legacy = match self.object_store.get(&path).await {
            Ok(get_result) => {
                let encoded = get_result.bytes().await?;
                serde_json::from_slice::<LegacyMeta>(&encoded)?
            }
            Err(object_store::Error::NotFound { .. }) => LegacyMeta {
                topics: BTreeMap::new(),
            },
            Err(otherwise) => return Err(otherwise.into()),
        };

        let mut migrated = 0u64;

        // Write per-topic objects directly (create-only), NOT through the cached
        // `topic_meta` handle: the per-topic `OptiCon` cache would otherwise
        // retain every migrated topic, making migration memory scale with topic
        // count. Consume by value so the legacy map shrinks as we go.
        for (name, metadata) in legacy.topics {
            let id = metadata.id;

            let payload = serde_json::to_vec(&metadata)
                .map(Bytes::from)
                .map(PutPayload::from)?;
            if self
                .put_create(&self.topic_metadata_path(&name), payload)
                .await?
            {
                migrated += 1;
            }

            let pointer = serde_json::to_vec(&TopicIdRef { name })
                .map(Bytes::from)
                .map(PutPayload::from)?;
            _ = self.put_create(&self.topic_id_path(&id), pointer).await?;
        }

        if migrated > 0 {
            info!(
                cluster = %self.cluster,
                migrated,
                "backfilled legacy topic metadata into per-topic objects"
            );
        }

        // Record completion so every subsequent boot takes the fast path above.
        _ = self
            .put_create(&marker, PutPayload::from(Bytes::new()))
            .await?;

        Ok(())
    }

    async fn topic_metadata(&self, topic: &TopicId) -> Result<Option<TopicMetadata>> {
        debug!(?topic);

        match topic {
            TopicId::Name(name) => self.topic_meta(name)?.get_opt(&self.object_store).await,
            TopicId::Id(id) => match self.topic_name_by_id(id).await? {
                Some(name) => self.topic_meta(&name)?.get_opt(&self.object_store).await,
                None => Ok(None),
            },
        }
    }

    fn records_prefix(&self, topition: &Topition) -> Path {
        Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/",
            self.cluster, topition.topic, topition.partition,
        ))
    }

    fn batch_location(&self, topition: &Topition, offset: i64) -> Path {
        Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}.batch",
            self.cluster, topition.topic, topition.partition, offset,
        ))
    }

    fn watermark(&self, topition: &Topition) -> Result<OptiCon<Watermark>> {
        self.watermarks
            .lock()
            .map(|mut locked| {
                locked
                    .entry(topition.to_owned())
                    .or_insert_with(|| OptiCon::<Watermark>::new(self.cluster.as_str(), topition))
                    .to_owned()
            })
            .map_err(Into::into)
    }

    /// Optimistic-concurrency handle on the per-producer `producers/{id}.json`
    /// object holding `producer_id`'s idempotent sequence state.
    fn producer(&self, producer_id: ProducerId) -> Result<OptiCon<ProducerDetail>> {
        self.producers
            .lock()
            .map(|mut locked| {
                locked
                    .entry(producer_id)
                    .or_insert_with(|| {
                        OptiCon::<ProducerDetail>::new(self.cluster.as_str(), producer_id)
                    })
                    .to_owned()
            })
            .map_err(Into::into)
    }

    /// Seed (or epoch-bump) the per-producer sequence object so the produce hot
    /// path can validate against it. Called from `init_producer` on the cold
    /// registration path; an absent epoch entry is what distinguishes a
    /// registered producer from `UnknownProducerId`.
    async fn seed_producer(&self, response: &ProducerIdResponse) -> Result<()> {
        if response.error != ErrorCode::None || response.id < 0 {
            return Ok(());
        }

        let epoch = response.epoch;

        self.producer(response.id)?
            .with_mut(&self.object_store, |pd| {
                _ = pd.sequences.entry(epoch).or_default();
                Ok(())
            })
            .await
            .map(|_| ())
    }

    /// Validate and advance a producer's idempotent sequence for `topition`.
    ///
    /// The sequence check/advance happens in memory only (the fast-path
    /// authority); the durable `producers/{id}.json` object is persisted lazily
    /// via [`OptiCon::checkpoint`] once the per-producer debounce window
    /// ([`Self::PRODUCER_CHECKPOINT_BATCHES`] / [`Self::PRODUCER_CHECKPOINT_INTERVAL`])
    /// elapses, so an all-idempotent workload issues ~1 PUT per batch instead of
    /// two (#48). The `OutOfOrderSequenceNumber` / `DuplicateSequenceNumber` /
    /// `ProducerFenced` / `UnknownProducerId` outcomes are unchanged from the
    /// former per-batch `with_mut` (the seed/epoch-bump write stays immediate).
    async fn advance_idempotent_sequence(
        &self,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        topition: &Topition,
        base_sequence: Sequence,
        last_offset_delta: i32,
    ) -> Result<()> {
        let producer = self.producer(producer_id)?;

        producer
            .mutate_cached(&self.object_store, |pd| {
                let Some(mut current) = pd.sequences.last_entry() else {
                    // An empty/absent producer object means the id was never
                    // registered (InitProducerId seeds it).
                    debug!(producer_id, ?pd);
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                };

                if current.key() != &producer_epoch {
                    debug!(current = ?current.key(), producer_epoch);
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                }

                let sequences = current.get_mut();
                debug!(?sequences);

                match sequences
                    .entry(topition.topic.clone())
                    .or_default()
                    .entry(topition.partition)
                    .or_default()
                {
                    sequence if *sequence < base_sequence => {
                        debug!(?sequence, base_sequence);
                        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
                    }

                    sequence if *sequence > base_sequence => {
                        debug!(?sequence, base_sequence);
                        Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
                    }

                    sequence => {
                        debug!(?sequence, delta = last_offset_delta + 1);
                        *sequence += last_offset_delta + 1;
                        Ok(())
                    }
                }
            })
            .await?;

        // The advance succeeded; persist it only when the debounce window is due.
        if self.producer_checkpoint_due(producer_id)? {
            producer
                .checkpoint(&self.object_store, ProducerDetail::reconcile)
                .await?;
            self.producer_checkpoint_reset(producer_id)?;
        }

        Ok(())
    }

    /// Record one more advance for `producer_id` and report whether its durable
    /// checkpoint is now due (batch-count or interval threshold reached). Seeds
    /// the entry on first use.
    fn producer_checkpoint_due(&self, producer_id: ProducerId) -> Result<bool> {
        let now = SystemTime::now();

        self.producer_checkpoints
            .lock()
            .map_err(Into::into)
            .map(|mut locked| {
                let entry = locked.entry(producer_id).or_insert(ProducerCheckpoint {
                    batches_since_flush: 0,
                    last_flush: now,
                });
                entry.batches_since_flush += 1;

                entry.batches_since_flush >= self.producer_checkpoint_batches
                    || now
                        .duration_since(entry.last_flush)
                        .is_ok_and(|elapsed| elapsed >= self.producer_checkpoint_interval)
            })
    }

    /// Reset `producer_id`'s debounce accounting after a successful checkpoint.
    fn producer_checkpoint_reset(&self, producer_id: ProducerId) -> Result<()> {
        self.producer_checkpoints
            .lock()
            .map_err(Into::into)
            .map(|mut locked| {
                _ = locked.insert(
                    producer_id,
                    ProducerCheckpoint {
                        batches_since_flush: 0,
                        last_flush: SystemTime::now(),
                    },
                );
            })
    }

    /// The cached next offset (== high watermark) hint for `topition`, if known
    /// to this process. `None` means the partition has not been read or written
    /// here yet and the tail must be listed. Ignores listing freshness — used for
    /// offset assignment (produce) and as a listing floor, where any known lower
    /// bound is safe (a `Create` conflict / tail listing reconciles it).
    fn cached_high(&self, topition: &Topition) -> Result<Option<i64>> {
        self.next_offsets
            .lock()
            .map(|locked| locked.get(topition).map(|hint| hint.next))
            .map_err(Into::into)
    }

    /// The cached next-offset hint for `topition` iff it was last reconciled
    /// against an authoritative tail listing within
    /// [`Self::HIGH_WATERMARK_HINT_TTL`]. `None` (cold, or stale beyond the TTL)
    /// means the high-watermark read path must LIST to pick up batches produced
    /// on another replica. Serving from a fresh hint is what takes the consumer
    /// Fetch hot path off the per-poll `ListObjectsV2` request (#40).
    fn cached_high_fresh(&self, topition: &Topition) -> Result<Option<i64>> {
        self.next_offsets
            .lock()
            .map(|locked| {
                locked.get(topition).and_then(|hint| {
                    let fresh = hint.listed_at.is_some_and(|at| {
                        SystemTime::now()
                            .duration_since(at)
                            .is_ok_and(|elapsed| elapsed < Self::HIGH_WATERMARK_HINT_TTL)
                    });
                    fresh.then_some(hint.next)
                })
            })
            .map_err(Into::into)
    }

    /// Advance the cached next-offset hint for `topition` after a local produce.
    /// Monotonic: a slower task can never lower a value a faster one already
    /// published, so the hint only ever moves forward (offsets are never reused).
    /// Does **not** touch `listed_at`: a local produce reflects only this
    /// replica's writes, so the TTL clock that forces cross-replica reconciliation
    /// keeps running.
    fn set_high(&self, topition: &Topition, high: i64) -> Result<()> {
        self.next_offsets
            .lock()
            .map(|mut locked| {
                let entry = locked.entry(topition.to_owned()).or_default();
                entry.next = entry.next.max(high);
            })
            .map_err(Into::into)
    }

    /// Advance the hint after an authoritative tail *listing* and mark it fresh.
    /// The listing observed every batch at/after its floor — including other
    /// replicas' writes in that range — so it resets the [`Self::cached_high_fresh`]
    /// TTL clock.
    fn mark_listed(&self, topition: &Topition, high: i64) -> Result<()> {
        self.next_offsets
            .lock()
            .map(|mut locked| {
                let entry = locked.entry(topition.to_owned()).or_default();
                entry.next = entry.next.max(high);
                entry.listed_at = Some(SystemTime::now());
            })
            .map_err(Into::into)
    }

    /// Read the base offset and record count of the partition's tail batch,
    /// scanning only objects at or after `from` (a cached floor). Returns the
    /// next offset past the tail (`base + last_offset_delta + 1`), or `from`
    /// (defaulting to `0`) when no batch exists at/after the floor.
    ///
    /// The authority for the high watermark is the immutable batch objects, not
    /// any mutable `watermark` object — so this is correct across replicas: a
    /// batch another replica created is visible here as soon as it is listed
    /// (GCS object listing is strongly consistent).
    async fn tail_next_offset(&self, topition: &Topition, from: Option<i64>) -> Result<i64> {
        let prefix = self.records_prefix(topition);

        let mut max: Option<(i64, Path)> = None;

        let mut list_stream = match from {
            Some(floor) => {
                // Batch names are zero-padded offsets, so this strict prefix of
                // `{floor:0>20}.batch` yields base offsets >= floor (same trick
                // as `fetch`), bounding the scan to the contended tail region.
                let start_after = Path::from(format!(
                    "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}",
                    self.cluster, topition.topic, topition.partition, floor,
                ));

                self.object_store
                    .list_with_offset(Some(&prefix), &start_after)
            }
            None => self.object_store.list(Some(&prefix)),
        };

        while let Some(meta) = list_stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, ?topition))?
        {
            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };

            let Ok(offset) = i64::from_str(&name.as_ref()[0..20]) else {
                continue;
            };

            if max.as_ref().is_none_or(|(m, _)| offset > *m) {
                max = Some((offset, meta.location));
            }
        }

        let Some((base, location)) = max else {
            return Ok(from.unwrap_or(0));
        };

        let encoded = self
            .object_store
            .get(&location)
            .await
            .inspect_err(|err| error!(?err, ?topition, base))?
            .bytes()
            .await
            .inspect_err(|err| error!(?err, ?topition, base))?;

        // A coalesced object holds several contiguous batches (#50); the next
        // offset past the tail is the object's base plus the sum of every
        // sub-batch's offset span. A single-batch object reduces to the previous
        // `base + last_offset_delta + 1`.
        let span: i64 = self
            .decode_frame(encoded)?
            .iter()
            .map(|batch| batch.last_offset_delta as i64 + 1)
            .sum();

        Ok(base + span)
    }

    /// The persisted `watermark.high`: a durable lower bound on the tail offset,
    /// used as a listing floor so a cold reader scans only forward (S3
    /// `start-after`) rather than the whole partition.
    async fn persisted_high(&self, topition: &Topition) -> Result<i64> {
        self.watermark(topition)?
            .with(&self.object_store, |watermark| {
                Ok(watermark.high.unwrap_or(0))
            })
            .await
    }

    /// Reconcile the cached high-watermark hint for `topition` against the
    /// partition's batch objects and return the authoritative next offset.
    async fn refresh_high(&self, topition: &Topition) -> Result<i64> {
        // Cold (no in-memory hint): floor at the persisted watermark so we scan
        // only the tail, not the whole partition. Still correct for offset
        // assignment — listing from a floor at/below the true tail still finds
        // the true tail.
        let floor = match self.cached_high(topition)? {
            Some(hint) => hint,
            None => self.persisted_high(topition).await?,
        };
        let high = self.tail_next_offset(topition, Some(floor)).await?;
        self.mark_listed(topition, high)?;
        Ok(high)
    }

    /// The log end offset (high watermark) for `topition`.
    ///
    /// The authority is the immutable batch objects. The tail listing is floored
    /// at the best known lower bound — the in-memory hint, or the persisted
    /// `watermark.high` — so a *cold* reader (empty hint after a restart or on
    /// another replica) lists only the batches *after* that floor via S3
    /// `start-after`, instead of scanning the whole partition (the cold-LIST
    /// storm at scale). On a cold read the computed high is persisted back to
    /// `watermark.high`, keeping the floor current for the next cold reader; this
    /// runs once per process per partition, not on the produce hot path, so it
    /// does not reintroduce the #13 hot-object write. Lake-sink topics
    /// (`tansu.lake.sink`) write no batch objects and carry the offset in
    /// `watermark.high`, which the `max` below preserves.
    async fn high_watermark(&self, topition: &Topition) -> Result<i64> {
        // Warm fast path: serve from the in-memory hint without ANY per-poll S3
        // request while it is fresh (reconciled against a listing within the TTL).
        // Every hint refresh (`mark_listed`) already folds in `from_watermark` —
        // including lake-sink topics' authoritative high (they write no batch
        // objects) — see the `mark_listed` call sites below, so a fresh hint needs
        // neither the tail `ListObjectsV2` (#40) nor the `watermark.json` GET
        // (#72). This is what takes the consumer Fetch hot path off ~1 GET per
        // poll per partition. Bounded staleness (== the hint TTL): another
        // replica's just-produced batch, or a lake-sink high bump in the last TTL
        // window, is picked up on the next TTL-triggered listing below.
        if let Some(hint) = self.cached_high_fresh(topition)? {
            return Ok(hint);
        }

        // Cold/stale hint: read the persisted watermark as the listing floor (and
        // to fold in lake-sink topics' authoritative high). Only on this slow
        // path, not on every poll.
        let from_watermark = self.persisted_high(topition).await?;

        // Prefix-coalesced (#60): the tail offset lives in the segment footers,
        // not in a `records/` listing (there is none). Recover it footer-only
        // (#58) and refresh the hint so the hot path serves from memory next
        // time. GCS-safe: no per-flush-mutated manifest is read.
        if self.prefix_coalesce {
            let recovered = self
                .recover_substream_next_offset(topition, from_watermark)
                .await?;
            let high = recovered.max(self.cached_high(topition)?.unwrap_or(0));
            self.mark_listed(topition, high)?;
            return Ok(high);
        }

        let cached = self.cached_high(topition)?;
        let was_cold = cached.is_none();
        let floor = cached.unwrap_or(0).max(from_watermark);

        let from_objects = self.tail_next_offset(topition, Some(floor)).await?;
        self.mark_listed(topition, from_objects)?;

        let high = from_objects.max(from_watermark);

        // Cold read: refresh the persisted floor so the next cold reader scans
        // only forward from here. Skipped for lake-sink topics (no batch objects
        // → `high == from_watermark`) so we never clobber their authoritative
        // watermark.
        if was_cold && high > from_watermark {
            self.watermark(topition)?
                .with_mut(&self.object_store, |watermark| {
                    watermark.high = Some(watermark.high.unwrap_or(0).max(high));
                    Ok(())
                })
                .await?;
        }

        Ok(high)
    }

    /// Assign the next offset to `deflated` and persist it as an immutable,
    /// create-only batch object whose name encodes the base offset.
    ///
    /// The `PutMode::Create` *is* the offset-assignment authority: two writers
    /// (tasks on one replica, or different replicas) racing the same offset both
    /// attempt the create, exactly one wins and the loser gets `AlreadyExists`
    /// (GCS guards this with `x-goog-if-generation-match: 0`), resyncs from the
    /// partition tail and retries at the next free offset. This keeps the
    /// produce hot path on *creating distinct new objects* (high create rate is
    /// fine on GCS) instead of *updating one hot `watermark` object* (capped at
    /// ~1/s), which is the #13 collapse. Contiguity holds because the resync
    /// reads the real tail, never a speculative reservation — no gaps, no
    /// overlaps.
    async fn assign_and_create(
        &self,
        topition: &Topition,
        record_count: i64,
        payload: PutPayload,
    ) -> Result<i64> {
        /// Bounds the conflict-resync loop so a pathologically hot partition
        /// fails fast instead of spinning; far above any real contention.
        const MAX_ATTEMPTS: usize = 64;

        // Under prefix coalescing, a segment-routed topic's ineligible batches
        // (transactional / control / backfill) still take this legacy
        // `records/{offset}.batch` path, and assign offsets from the SAME
        // per-(topic,partition) `cached_high` that a coalesced flush of the same
        // prefix reads under the per-prefix flush lock. Without holding that lock
        // here, a legacy write and a flush can both stamp the same offset — a
        // legacy object and a segment record at one offset, which the two
        // create-only namespaces cannot detect across each other (#78). Take the
        // lock so the two offset authorities serialize. No re-entrancy: the flush
        // path holds this lock and calls `assign_and_create_segment`, never this
        // function. `None` on the non-coalesce path leaves behaviour unchanged.
        let _flush_guard = if self.prefix_coalesce {
            Some(
                self.prefix_flush_lock(&self.prefix_of(topition))?
                    .lock_owned()
                    .await,
            )
        } else {
            None
        };

        let mut candidate = match self.cached_high(topition)? {
            Some(hint) => hint,
            None => self.refresh_high(topition).await?,
        };

        for attempt in 0..MAX_ATTEMPTS {
            match self
                .object_store
                .put_opts(
                    &self.batch_location(topition, candidate),
                    payload.clone(),
                    PutOptions {
                        mode: PutMode::Create,
                        attributes: Attributes::new(),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(outcome) => {
                    debug!(?outcome, candidate, ?topition);
                    self.set_high(topition, candidate + record_count)?;
                    return Ok(candidate);
                }

                Err(object_store::Error::AlreadyExists { .. }) => {
                    debug!(
                        candidate,
                        attempt,
                        ?topition,
                        "offset taken, resyncing tail"
                    );
                    candidate = self.tail_next_offset(topition, Some(candidate)).await?;
                    self.mark_listed(topition, candidate)?;
                }

                Err(err) => return Err(err.into()),
            }
        }

        error!(?topition, candidate, "offset assignment exhausted retries");
        Err(Error::Api(ErrorCode::UnknownServerError))
    }

    /// Buffer `deflated` for a coalesced flush and await its assigned base offset
    /// (#50). The idempotent sequence and schema were already validated by
    /// `produce`, so only eligible (non-txn, non-control, non-compacted) batches
    /// reach here. The produce call parks on the returned one-shot until the
    /// batch's run is durably written, so an unflushed batch is never acked
    /// (crash-safe: the client retries).
    async fn enqueue_coalesced(
        &self,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let (ack, offset) = oneshot::channel();
        let span = deflated.last_offset_delta as i64 + 1;
        let size = size_of::<i64>() + size_of::<i32>() + deflated.batch_length.max(0) as usize;

        enum Action {
            Flush(CoalesceBuffer),
            StartTimer,
            Wait,
        }

        let action = {
            let mut buffers = self.coalesce.lock().map_err(Into::<Error>::into)?;
            let buffer = buffers.entry(topition.to_owned()).or_default();

            let first = buffer.pending.is_empty();
            buffer.pending.push(Pending {
                batch: deflated,
                ack,
            });
            buffer.records += span;
            buffer.bytes += size;

            if buffer.pending.len() >= self.coalesce_batches
                || buffer.bytes >= self.coalesce_bytes
                || buffer.records >= Self::COALESCE_MAX_RECORDS
            {
                Action::Flush(std::mem::take(buffer))
            } else if first {
                Action::StartTimer
            } else {
                Action::Wait
            }
        };

        match action {
            Action::Flush(buffer) => self.flush_coalesced(topition, buffer).await,

            Action::StartTimer => {
                let store = self.clone();
                let topition = topition.to_owned();
                let linger = self.coalesce_linger;

                _ = tokio::spawn(async move {
                    tokio::time::sleep(linger).await;

                    let buffer = store
                        .coalesce
                        .lock()
                        .ok()
                        .and_then(|mut buffers| buffers.remove(&topition));

                    if let Some(buffer) = buffer.filter(|buffer| !buffer.pending.is_empty()) {
                        store.flush_coalesced(&topition, buffer).await;
                    }
                });
            }

            Action::Wait => {}
        }

        offset
            .await
            .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
    }

    /// Flush a drained coalescing buffer as one `records/` object and resolve
    /// each parked produce with its assigned base offset (#50). One
    /// `assign_and_create` covers the whole run, so offset assignment stays the
    /// create-only, single-writer authority it is for a lone batch; on failure
    /// every parked producer gets the error and retries.
    async fn flush_coalesced(&self, topition: &Topition, buffer: CoalesceBuffer) {
        if buffer.pending.is_empty() {
            return;
        }

        let batches: Vec<deflated::Batch> = buffer
            .pending
            .iter()
            .map(|pending| pending.batch.clone())
            .collect();
        let total: i64 = batches
            .iter()
            .map(|batch| batch.last_offset_delta as i64 + 1)
            .sum();

        let base = match async {
            let payload = self.encode_frame(&batches)?;
            self.assign_and_create(topition, total, payload).await
        }
        .await
        {
            Ok(base) => base,
            Err(error) => {
                error!(?error, ?topition, "coalesced flush failed");
                for pending in buffer.pending {
                    _ = pending.ack.send(Err(error.clone()));
                }
                return;
            }
        };

        // Mirror the immediate path's lake sink, per sub-batch, with offsets
        // running from the frame base. Only fetched when a lake is configured.
        let config = if self.lake.is_some() {
            self.describe_config(topition.topic(), ConfigResource::Topic, None)
                .await
                .inspect_err(|err| debug!(?err))
                .ok()
        } else {
            None
        };

        let mut running = base;
        for pending in buffer.pending {
            let offset = running;
            running += pending.batch.last_offset_delta as i64 + 1;

            if let (Some(lake), Some(config)) = (self.lake.as_ref(), config.as_ref())
                && let Ok(inflated) = inflated::Batch::try_from(&pending.batch)
            {
                _ = lake
                    .store(
                        topition.topic(),
                        topition.partition(),
                        offset,
                        &inflated,
                        config.clone(),
                    )
                    .await
                    .inspect_err(|err| debug!(?err));
            }

            _ = pending.ack.send(Ok(offset));
        }
    }

    /// The connector prefix a topition's records coalesce under (#57). Popsink
    /// topics are `org.env.conn.<schema>.<table>`; the prefix is the connector
    /// unit `org.env.conn` (the first three dotted components) — the
    /// tenant/retention/isolation boundary the epic coalesces on. A topic with
    /// fewer than three components is its own prefix. A configurable
    /// `prefix_map` override (custom topic-glob → prefix) is not yet implemented;
    /// tracked as a follow-up. This derivation is the only mapping today.
    fn prefix_of(&self, topition: &Topition) -> String {
        let topic = topition.topic();
        let mut parts = topic.split('.');
        let mut prefix = String::new();

        for i in 0..3 {
            match parts.next() {
                Some(part) => {
                    if i > 0 {
                        prefix.push('.');
                    }
                    prefix.push_str(part);
                }
                None => return topic.to_owned(),
            }
        }

        prefix
    }

    /// The `segments/` listing prefix for a connector prefix (#57).
    fn segment_prefix(&self, prefix: &str) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/segments/",
            self.cluster, prefix,
        ))
    }

    /// The object name of the `seq`-th segment under a connector prefix (#57).
    /// The zero-padded sequence makes the name monotonic and, written
    /// create-only, the ordering authority (as `{offset}.batch` is for #50).
    fn segment_location(&self, prefix: &str, seq: u64) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/segments/{:0>20}.seg",
            self.cluster, prefix, seq,
        ))
    }

    /// The cached next segment sequence for `prefix`, if known to this process.
    fn cached_seq(&self, prefix: &str) -> Result<Option<u64>> {
        self.segment_seqs
            .lock()
            .map(|locked| locked.get(prefix).copied())
            .map_err(Into::into)
    }

    /// Advance the cached next-segment-sequence hint. Monotonic, like
    /// [`Self::set_high`]: a sequence is never reused.
    fn set_seq(&self, prefix: &str, next: u64) -> Result<()> {
        self.segment_seqs
            .lock()
            .map(|mut locked| {
                let entry = locked.entry(prefix.to_owned()).or_default();
                *entry = (*entry).max(next);
            })
            .map_err(Into::into)
    }

    /// The next free segment sequence for `prefix`, read from the tail of the
    /// `segments/` listing (#57). Zero-padded names sort lexicographically by
    /// sequence, so the greatest listed name is the tail. Used to seed the hint
    /// cold and to resync after a `Create` conflict.
    async fn tail_next_seq(&self, prefix: &str) -> Result<u64> {
        let listing = self.segment_prefix(prefix);
        let mut list_stream = self.object_store.list(Some(&listing));
        let mut max: Option<u64> = None;

        while let Some(meta) = list_stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, prefix))?
        {
            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };

            let name = name.as_ref();
            if name.len() < 20 {
                continue;
            }

            let Ok(seq) = u64::from_str(&name[0..20]) else {
                continue;
            };

            max = Some(max.map_or(seq, |m| m.max(seq)));
        }

        Ok(max.map_or(0, |m| m + 1))
    }

    /// Write `payload` as the next create-only segment under `prefix` and return
    /// its assigned sequence (#57). The create is the authority: a conflicting
    /// sequence (a racing writer during a #59 failover) resyncs from the tail
    /// and retries. Single-writer per prefix makes the conflict path an edge
    /// case, not the steady state.
    ///
    /// `fence_on_conflict` controls the produce vs. compaction contract: the
    /// produce path passes `true` — a seq conflict means a lease takeover, so it
    /// re-validates the lease and aborts if fenced (#59). Compaction passes
    /// `false` — it does not hold the produce lease and a conflict just means the
    /// producer grabbed that tail seq, so it simply resyncs to the next free one.
    async fn assign_and_create_segment(
        &self,
        prefix: &str,
        payload: PutPayload,
        fence_on_conflict: bool,
    ) -> Result<u64> {
        /// Bounds the conflict-resync loop; far above any real contention.
        const MAX_ATTEMPTS: usize = 64;

        let mut candidate = match self.cached_seq(prefix)? {
            Some(seq) => seq,
            None => self.tail_next_seq(prefix).await?,
        };

        for attempt in 0..MAX_ATTEMPTS {
            match self
                .object_store
                .put_opts(
                    &self.segment_location(prefix, candidate),
                    payload.clone(),
                    PutOptions {
                        mode: PutMode::Create,
                        attributes: Attributes::new(),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(outcome) => {
                    debug!(?outcome, candidate, prefix);
                    self.set_seq(prefix, candidate + 1)?;
                    return Ok(candidate);
                }

                Err(object_store::Error::AlreadyExists { .. }) => {
                    // A seq conflict means another writer wrote this sequence.
                    // With per-prefix flush serialization (#1) that can only be a
                    // *different* broker — i.e. a lease takeover. Re-validate the
                    // lease before resyncing (#59 review fix): if we've been
                    // fenced, abort now instead of appending the next sequence
                    // with stale offsets (which would split-brain the log). A
                    // still-valid holder resyncs and retries. Compaction
                    // (fence_on_conflict=false) is not the produce writer, so a
                    // conflict is just the producer taking that seq — resync only.
                    if fence_on_conflict {
                        _ = self.acquire_or_renew_lease(prefix).await?;
                    }
                    debug!(candidate, attempt, prefix, "segment seq taken, resyncing");
                    candidate = self.tail_next_seq(prefix).await?;
                }

                Err(err) => return Err(err.into()),
            }
        }

        error!(
            prefix,
            candidate, "segment sequence assignment exhausted retries"
        );
        Err(Error::Api(ErrorCode::UnknownServerError))
    }

    /// Read a segment's self-describing footer (#58/#64) with at most two ranged
    /// GETs of the object tail — never the record body: one `Suffix` GET of the
    /// fixed trailer to learn the footer length, then one `Suffix` GET of the
    /// footer + trailer. Returns `None` if the object carries no trailer (a
    /// legacy #50 object). This is the read primitive the fetch path (#60) also
    /// builds on.
    async fn read_segment_footer(&self, location: &Path) -> Result<Option<SegmentFooter>> {
        let trailer = self
            .object_store
            .get_opts(
                location,
                GetOptions {
                    range: Some(GetRange::Suffix(SEGMENT_TRAILER_LEN as u64)),
                    ..Default::default()
                },
            )
            .await?
            .bytes()
            .await?;

        if trailer.len() < SEGMENT_TRAILER_LEN {
            return Ok(None);
        }

        let magic = u32::from_be_bytes(trailer[14..18].try_into()?);
        if magic != SEGMENT_MAGIC {
            return Ok(None);
        }

        let footer_len = u64::from_be_bytes(trailer[0..8].try_into()?) as usize;

        let tail = self
            .object_store
            .get_opts(
                location,
                GetOptions {
                    range: Some(GetRange::Suffix((SEGMENT_TRAILER_LEN + footer_len) as u64)),
                    ..Default::default()
                },
            )
            .await?
            .bytes()
            .await?;

        self.decode_segment_footer(&tail)
    }

    /// Refresh the in-memory [`PrefixIndex`] for `prefix` (read-path #60 review
    /// fix). Skips the listing while the cache is fresh (within
    /// [`Self::HIGH_WATERMARK_HINT_TTL`]); otherwise lists **incrementally**
    /// (`start_after` the highest known sequence) and reads only the footers of
    /// *new* segments, so steady-state cost is O(new), not O(total segments).
    /// Cheap for the writer (its own flushes already populated the cache).
    async fn refresh_prefix_index(&self, prefix: &str) -> Result<()> {
        let (fresh, start_after) = {
            let index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            match index.get(prefix) {
                Some(entry) => {
                    let fresh = entry.refreshed_at.is_some_and(|at| {
                        SystemTime::now()
                            .duration_since(at)
                            .is_ok_and(|elapsed| elapsed < Self::HIGH_WATERMARK_HINT_TTL)
                    });
                    (fresh, entry.segments.keys().next_back().copied())
                }
                None => (false, None),
            }
        };

        if fresh {
            return Ok(());
        }

        let listing = self.segment_prefix(prefix);
        let mut stream = match start_after {
            Some(seq) => self
                .object_store
                .list_with_offset(Some(&listing), &self.segment_location(prefix, seq)),
            None => self.object_store.list(Some(&listing)),
        };

        let mut discovered: Vec<(u64, Path, i64)> = Vec::new();
        while let Some(meta) = stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, prefix))?
        {
            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };
            let name = name.as_ref();
            if name.len() < 20 {
                continue;
            }
            let Ok(seq) = u64::from_str(&name[0..20]) else {
                continue;
            };
            discovered.push((seq, meta.location, meta.last_modified.timestamp_millis()));
        }

        // Read footers only for sequences not already cached.
        let mut fetched: Vec<(u64, CachedSegment)> = Vec::new();
        for (seq, location, last_modified_ms) in discovered {
            let cached = self
                .prefix_index
                .lock()
                .map(|index| {
                    index
                        .get(prefix)
                        .is_some_and(|entry| entry.segments.contains_key(&seq))
                })
                .unwrap_or(false);
            if cached {
                continue;
            }
            if let Some(footer) = self.read_segment_footer(&location).await? {
                fetched.push((
                    seq,
                    CachedSegment {
                        footer,
                        last_modified_ms,
                    },
                ));
            }
        }

        let mut index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
        let entry = index.entry(prefix.to_owned()).or_default();
        for (seq, segment) in fetched {
            _ = entry.segments.insert(seq, segment);
        }
        entry.refreshed_at = Some(SystemTime::now());
        Ok(())
    }

    /// Record a freshly-written segment in the index (writer fast path): its
    /// footer is authoritative, so a following read on this node needs no
    /// listing/GET. `last_modified_ms` is the object's append time — `now` for a
    /// normal flush, but the max of the merged inputs for a compaction (#66) so
    /// compaction does not reset the retention clock of timestamp-less data.
    fn index_insert(
        &self,
        prefix: &str,
        seq: u64,
        footer: SegmentFooter,
        last_modified_ms: i64,
    ) -> Result<()> {
        self.prefix_index
            .lock()
            .map_err(Into::into)
            .map(|mut index| {
                let entry = index.entry(prefix.to_owned()).or_default();
                _ = entry.segments.insert(
                    seq,
                    CachedSegment {
                        footer,
                        last_modified_ms,
                    },
                );
                entry.refreshed_at = Some(SystemTime::now());
            })
    }

    /// Force the next index access to re-list (bust the TTL) — used when a data
    /// GET 404s (a segment was compacted/expired out from under a reader, #66).
    fn index_invalidate(&self, prefix: &str) -> Result<()> {
        self.prefix_index
            .lock()
            .map_err(Into::into)
            .map(|mut index| {
                if let Some(entry) = index.get_mut(prefix) {
                    entry.refreshed_at = None;
                }
            })
    }

    /// Drop expired sequences from the index after a retention delete (#61).
    fn index_prune(&self, prefix: &str, seqs: &[u64]) -> Result<()> {
        self.prefix_index
            .lock()
            .map_err(Into::into)
            .map(|mut index| {
                if let Some(entry) = index.get_mut(prefix) {
                    for seq in seqs {
                        _ = entry.segments.remove(seq);
                    }
                }
            })
    }

    /// The epoch-fenced segments holding a sub-stream, as `(seq, entry)` sorted
    /// by base offset (#59 review fix). A segment is written atomically under a
    /// single-writer lease, so under normal operation a sub-stream's segments are
    /// disjoint and monotonic; a fenced/zombie writer is the only way two
    /// segments' offset ranges overlap. On overlap the higher `writer_epoch`
    /// wins and the stale (lower-epoch) segment is dropped — so a stale-epoch
    /// segment is ignored on read/recovery, exactly as the epic requires.
    /// Operates on the cached index (no object requests).
    fn valid_substream_segments(
        &self,
        prefix: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Vec<(u64, SubstreamEntry)>> {
        let mut segs: Vec<(i64, u64, SubstreamEntry)> = self
            .prefix_index
            .lock()
            .map_err(Into::<Error>::into)?
            .get(prefix)
            .map(|index| {
                index
                    .segments
                    .iter()
                    .filter_map(|(seq, cached)| {
                        cached
                            .footer
                            .get(topic, partition)
                            .map(|entry| (cached.footer.writer_epoch, *seq, entry.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Ascending base offset; on a tie prefer the higher epoch, then the
        // higher sequence, so the winner claims the overlapping range. The
        // higher-sequence tie-break matters when epochs are equal: a compacted
        // segment (#66) always has a higher sequence than the originals it
        // merged, so it wins the overlap during the write→delete window even
        // though it carries the same epoch — a reader replica with a lingering
        // deleted-original entry still selects the merged segment.
        segs.sort_by(|a, b| {
            a.2.base_offset
                .cmp(&b.2.base_offset)
                .then_with(|| b.0.cmp(&a.0))
                .then_with(|| b.1.cmp(&a.1))
        });

        let mut out: Vec<(u64, SubstreamEntry)> = Vec::with_capacity(segs.len());
        let mut covered_to = i64::MIN;
        for (_epoch, seq, entry) in segs {
            if entry.base_offset < covered_to {
                // Overlaps an already-accepted (>= epoch) segment: stale, drop it.
                continue;
            }
            covered_to = covered_to.max(entry.base_offset + entry.record_count);
            out.push((seq, entry));
        }
        Ok(out)
    }

    /// Recover a prefix-coalesced sub-stream's next offset (#58) when the
    /// in-memory counter is cold (fresh process / #59 failover). Takes the
    /// **max** of the epoch-fenced segment tail and the legacy `records/` tail
    /// (#58 seam / #62 backfill bypass): a topic can have pre-cutover or bypassed
    /// `{offset}.batch` objects above or below segments, and reusing an offset is
    /// unacceptable — so the true next offset is the highest either source knows.
    /// Also folds in the persisted `watermark.high` floor so a fully
    /// retention-drained sub-stream never regresses to 0 (#61 review fix).
    async fn recover_substream_next_offset(
        &self,
        topition: &Topition,
        persisted_floor: i64,
    ) -> Result<i64> {
        let prefix = self.prefix_of(topition);
        self.refresh_prefix_index(&prefix).await?;

        let segment_tail = self
            .valid_substream_segments(&prefix, topition.topic(), topition.partition())?
            .last()
            .map(|(_, entry)| entry.base_offset + entry.record_count)
            .unwrap_or(0);

        // Legacy tail only when the sub-stream still has legacy `records/` objects
        // (#73): a pure-segment sub-stream has an empty prefix, so the
        // unconditional per-call LIST here was a residual on the recurring
        // stale-hint refresh path. Gated on the memoized `has_legacy_records` (the
        // flag the fetch path warms). The writer/flush path reaches this only when
        // the in-memory counter is genuinely cold (never set on this process),
        // where the memo is also cold and does a real probe — so offset recovery
        // stays authoritative and never regresses. On the read path any memo
        // staleness is folded away by the caller's `.max(cached_high)` and bounded
        // by the hint TTL, consistent with #40/#72.
        let legacy_tail = if self.has_legacy_records(topition).await? {
            self.tail_next_offset(topition, None).await?
        } else {
            0
        };

        // `persisted_floor` is supplied by the caller (the persisted
        // `watermark.high`) so this shares the single GET the read path already
        // issued instead of re-fetching it (#72).
        Ok(segment_tail.max(legacy_tail).max(persisted_floor))
    }

    /// Fetch a topition's records out of shared prefix-coalesced segments (#60).
    /// Segments are located from the in-memory footer index (read-path #60 review
    /// fix — no `segments/` LIST and no per-segment footer GET per fetch), and
    /// only the segments overlapping `[offset, high_watermark)` are read, each
    /// with a single ranged GET of exactly the sub-stream's contiguous byte span
    /// — never the whole object, no cross-topic data. Absolute offsets come from
    /// the footer. Epoch-fenced (`valid_substream_segments`), so a stale-epoch
    /// (zombie) segment is skipped. A segment deleted by retention mid-fetch (a
    /// 404 on the data GET) is pruned and skipped. Bounded by `max_bytes` and by
    /// `max_wait` from `started_at`.
    async fn fetch_prefix_coalesced(
        &self,
        topition: &Topition,
        offset: i64,
        max_bytes: u32,
        high_watermark: i64,
        started_at: SystemTime,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let has_deadline_expired = || {
            started_at
                .elapsed()
                .map(|elapsed| max_wait.saturating_sub(elapsed).is_zero())
                .unwrap_or_default()
        };

        /// A segment can be deleted mid-fetch by compaction/retention; the merged
        /// segment covers the same offsets, so on a 404 we refresh the index and
        /// restart cleanly. Bounded so a genuinely missing object can't loop.
        const MAX_ATTEMPTS: usize = 3;

        let prefix = self.prefix_of(topition);

        for _ in 0..MAX_ATTEMPTS {
            self.refresh_prefix_index(&prefix).await?;
            let segments =
                self.valid_substream_segments(&prefix, topition.topic(), topition.partition())?;

            let mut batches = vec![];
            let mut bytes = max_bytes as u64;
            let mut restart = false;

            for (seq, entry) in segments {
                if has_deadline_expired() {
                    break;
                }

                // Segments are sorted by base offset; skip those ending at/before
                // the requested offset, stop once one starts at/past the HWM.
                if entry.base_offset + entry.record_count <= offset {
                    continue;
                }
                if entry.base_offset >= high_watermark {
                    break;
                }

                // One ranged GET of exactly this sub-stream's byte span.
                let location = self.segment_location(&prefix, seq);
                let region = match self
                    .object_store
                    .get_opts(
                        &location,
                        GetOptions {
                            range: Some(GetRange::Bounded(
                                entry.byte_start..entry.byte_start + entry.byte_len,
                            )),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(result) => result
                        .bytes()
                        .await
                        .map_err(|_| Error::Api(ErrorCode::UnknownServerError))
                        .and_then(|encoded| self.decode_frame(encoded))?,

                    // Deleted between locate and read (compaction #66 / retention
                    // #61): drop anything gathered so far, evict the stale seq
                    // (prune-on-404 — the add-only refresh never would), force a
                    // re-list to pick up the merged/surviving segments, and
                    // restart clean. The merged segment covers the same offsets
                    // and wins the overlap (higher seq), so no gap/duplicate.
                    Err(object_store::Error::NotFound { .. }) => {
                        self.index_prune(&prefix, &[seq])?;
                        self.index_invalidate(&prefix)?;
                        restart = true;
                        break;
                    }

                    Err(error) => {
                        error!(?error, location = %location);
                        return Err(Error::Api(ErrorCode::UnknownServerError));
                    }
                };

                let mut running = entry.base_offset;
                for mut batch in region {
                    let span = batch.last_offset_delta as i64 + 1;
                    batch.base_offset = running;
                    running += span;
                    batches.push(batch);
                }

                if entry.byte_len > bytes {
                    break;
                } else {
                    bytes = bytes.saturating_sub(entry.byte_len);
                }
            }

            if !restart {
                return Ok(batches);
            }
        }

        // Exhausted retries (persistent 404 churn): return what a final clean
        // pass can read rather than erroring.
        self.refresh_prefix_index(&prefix).await?;
        Ok(vec![])
    }

    /// The lowest segment base offset for a sub-stream, from the index (#60): the
    /// start of the segment region (`C` in the hybrid layout). `None` when the
    /// sub-stream has no segment yet.
    async fn segment_region_start(&self, topition: &Topition) -> Result<Option<i64>> {
        let prefix = self.prefix_of(topition);
        self.refresh_prefix_index(&prefix).await?;
        Ok(self
            .valid_substream_segments(&prefix, topition.topic(), topition.partition())?
            .first()
            .map(|(_, entry)| entry.base_offset))
    }

    /// Whether `topition` still has legacy `records/` objects (#60 hybrid),
    /// memoized with a TTL so the common case pays no per-fetch LIST while a
    /// drain (`true`→`false`) or a txn/control write (`false`→`true`) is still
    /// picked up within [`Self::HIGH_WATERMARK_HINT_TTL`].
    async fn has_legacy_records(&self, topition: &Topition) -> Result<bool> {
        if let Some((present, checked_at)) = self
            .legacy_records_present
            .lock()
            .map_err(Into::<Error>::into)?
            .get(topition)
            .copied()
            && checked_at
                .elapsed()
                .is_ok_and(|elapsed| elapsed < Self::HIGH_WATERMARK_HINT_TTL)
        {
            return Ok(present);
        }

        let prefix = Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/",
            self.cluster, topition.topic, topition.partition
        ));
        let present = self
            .object_store
            .list(Some(&prefix))
            .next()
            .await
            .transpose()
            .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
            .is_some();

        _ = self.legacy_records_present.lock().map(|mut cache| {
            _ = cache.insert(topition.to_owned(), (present, SystemTime::now()));
        });

        Ok(present)
    }

    /// Fetch a topition's records from the legacy per-`(topic, partition)`
    /// `records/{offset}.batch` objects (the pre-#57 layout). Extracted from
    /// [`Storage::fetch`] so the prefix-coalesced path can serve the legacy
    /// region of a hybrid topic (#60 coexistence) before continuing into
    /// segments. Seeks with `start-after` (offsets are filenames) and reads up to
    /// `max_bytes`, bounded by `max_wait` from `started_at`.
    async fn fetch_legacy_records(
        &self,
        topition: &Topition,
        offset: i64,
        max_bytes: u32,
        high_watermark: i64,
        started_at: SystemTime,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let has_deadline_expired = || {
            started_at
                .elapsed()
                .map(|elapsed| max_wait.saturating_sub(elapsed).is_zero())
                .unwrap_or_default()
        };

        let mut batches = vec![];

        let prefix = Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/",
            self.cluster, topition.topic, topition.partition
        ));

        // Under #50 coalescing `offset` may fall inside a multi-batch object;
        // start from the object that contains it so the seek does not skip it.
        let start_offset = if self.produce_coalesce {
            self.coalesce_fetch_floor(topition, offset)
                .await?
                .unwrap_or(offset)
        } else {
            offset
        };

        let start_after = Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}",
            self.cluster, topition.topic, topition.partition, start_offset,
        ));

        let mut list_stream = self
            .object_store
            .list_with_offset(Some(&prefix), &start_after);

        let mut bytes = max_bytes as u64;

        while let Some(meta) = list_stream
            .next()
            .await
            .inspect(|meta| debug!(?meta))
            .transpose()
            .inspect_err(|error| error!(?error, ?topition, ?offset, ?max_bytes))
            .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
            && !has_deadline_expired()
        {
            let Some(file_name) = meta.location.parts().next_back() else {
                continue;
            };

            let file_name = file_name.as_ref();
            if file_name.len() < 20 {
                continue;
            }
            let Ok(base_offset) = i64::from_str(&file_name[0..20]) else {
                continue;
            };
            debug!(base_offset);

            if base_offset >= high_watermark {
                break;
            }

            let size = meta.size;

            let frame = self
                .object_store
                .get(&meta.location)
                .await
                .inspect_err(|error| error!(?error, ?topition, ?offset, ?max_bytes))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
                .bytes()
                .await
                .inspect_err(|error| error!(?error, location = %meta.location))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))
                .and_then(|encoded| self.decode_frame(encoded))?;

            let mut running = base_offset;
            for mut batch in frame {
                let span = batch.last_offset_delta as i64 + 1;
                batch.base_offset = running;
                running += span;
                batches.push(batch);
            }

            if size > bytes {
                break;
            } else {
                bytes = bytes.saturating_sub(size);
            }
        }

        Ok(batches)
    }

    /// The earliest (log-start) offset for a prefix-coalesced sub-stream (#60):
    /// the base offset of the oldest legacy `records/` object if any survive
    /// (they hold the lowest offsets until retention drains them, #60 hybrid),
    /// otherwise the base offset in the oldest segment (lowest sequence) that
    /// carries the sub-stream. `0` when neither exists yet.
    async fn coalesced_earliest_offset(&self, topition: &Topition) -> Result<i64> {
        // Hybrid topic: the legacy region holds the lowest offsets (below the #58
        // seam), so the smallest base offset present in `records/` is the log
        // start (listing is ascending, so the first parseable name is the min).
        // Gated on the memoized hybrid check (#73) so a pure-segment topic — the
        // common case under prefix coalescing — pays NO per-call `records/` LIST;
        // `has_legacy_records` is the same TTL-cached flag the fetch path warms.
        if self.has_legacy_records(topition).await? {
            let records = Path::from(format!(
                "clusters/{}/topics/{}/partitions/{:0>10}/records/",
                self.cluster, topition.topic, topition.partition,
            ));
            let mut stream = self.object_store.list(Some(&records));
            while let Some(meta) = stream
                .next()
                .await
                .transpose()
                .inspect_err(|error| error!(?error, ?topition))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
            {
                if let Some(name) = meta.location.parts().next_back()
                    && name.as_ref().len() >= 20
                    && let Ok(base) = i64::from_str(&name.as_ref()[0..20])
                {
                    return Ok(base);
                }
            }
        }

        // Otherwise the oldest segment's base for this sub-stream, from the index.
        Ok(self.segment_region_start(topition).await?.unwrap_or(0))
    }

    /// The newest record timestamp for a PURE-segment sub-stream (#73), from the
    /// footer index — the tail segment's `max_timestamp`. Returns `None` (caller
    /// falls back to the legacy `records/` listing) when the sub-stream has no
    /// segment yet, when the footer carries no timestamp, OR when the topic is
    /// hybrid: a topic under prefix coalescing can hold legacy `records/` objects
    /// (transactional/control/compacted/backfill batches are never coalesced) at
    /// offsets ABOVE the segment tail, so the footer's max_timestamp is NOT the
    /// log's latest timestamp there. Gated on the memoized `has_legacy_records`
    /// (like `coalesced_earliest_offset`) so the common pure-segment case pays no
    /// per-topic LIST while the hybrid case stays correct.
    async fn coalesced_latest_timestamp(&self, topition: &Topition) -> Result<Option<SystemTime>> {
        if self.has_legacy_records(topition).await? {
            return Ok(None);
        }

        let prefix = self.prefix_of(topition);
        self.refresh_prefix_index(&prefix).await?;
        Ok(self
            .valid_substream_segments(&prefix, topition.topic(), topition.partition())?
            .last()
            .map(|(_, entry)| entry.max_timestamp)
            .filter(|&ms| ms >= 0)
            .map(|ms| SystemTime::UNIX_EPOCH + Duration::from_millis(ms as u64)))
    }

    /// The per-prefix flush serialization lock (see [`Self::prefix_flush_locks`]).
    fn prefix_flush_lock(&self, prefix: &str) -> Result<Arc<tokio::sync::Mutex<()>>> {
        self.prefix_flush_locks
            .lock()
            .map_err(Into::into)
            .map(|mut locks| locks.entry(prefix.to_owned()).or_default().clone())
    }

    /// The durable single-writer lease object for a connector prefix (#59).
    fn lease_location(&self, prefix: &str) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/lease.json",
            self.cluster, prefix,
        ))
    }

    /// The durable compaction lease object for a connector prefix (#66): a lease
    /// distinct from the produce lease, so compaction (which runs on the
    /// maintenance workers, not the producing broker) coordinates
    /// compactor-vs-compactor without needing — or fencing — the produce writer.
    fn compaction_lease_location(&self, prefix: &str) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/compaction-lease.json",
            self.cluster, prefix,
        ))
    }

    /// Wall-clock milliseconds since the Unix epoch, for lease expiry (#59).
    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as i64)
            .unwrap_or(0)
    }

    /// Acquire or renew this writer's single-writer lease on `prefix` and return
    /// the held epoch (#59). The etag CAS is the fence: if another writer holds a
    /// live lease, or wins the acquire race, this call returns
    /// `NotLeaderOrFollower` (retriable) — so at most one writer appends per
    /// prefix, with no external coordinator.
    ///
    /// Note: this broker is single-node (node 111), so the fenced writer *is*
    /// this process and the retry lands back here after the stale lease lapses.
    /// A true multi-broker deployment would need a routing layer to send the
    /// retried produce to the `prefix_owner_node` holder; that layer does not
    /// exist yet, so prefix coalescing is intended for single-broker deployments
    /// until it does.
    ///
    /// A held term is reused with no write while more than a third of it
    /// remains, so the lease object is mutated ~once per `2/3 · ttl`, never per
    /// flush — keeping it under GCS's ~1/s/object mutation cap (#13).
    async fn acquire_or_renew_lease(&self, prefix: &str) -> Result<i64> {
        let location = self.lease_location(prefix);
        self.acquire_or_renew_lease_at(prefix, &location, &self.prefix_leases)
            .await
    }

    /// Acquire or renew the compaction lease for `prefix` (#66) — same fence as
    /// the produce lease but on a separate object/cache, so a maintenance worker
    /// can compact without holding (or fencing) the produce lease.
    async fn acquire_compaction_lease(&self, prefix: &str) -> Result<i64> {
        let location = self.compaction_lease_location(prefix);
        self.acquire_or_renew_lease_at(prefix, &location, &self.compaction_leases)
            .await
    }

    /// Generic lease acquire/renew against `location`, caching the held term in
    /// `cache` under `key` (#59/#66). The etag CAS is the fence; a live foreign
    /// lease or a lost CAS yields `NotLeaderOrFollower`. A held term is reused
    /// with no write while more than a third of it remains, keeping the object's
    /// mutation rate well under GCS's ~1/s cap (#13).
    async fn acquire_or_renew_lease_at(
        &self,
        key: &str,
        location: &Path,
        cache: &Arc<Mutex<BTreeMap<String, HeldLease>>>,
    ) -> Result<i64> {
        let now = SystemTime::now();
        let margin = self.prefix_lease_ttl / 3;

        // Fast path: comfortably within our term — no object mutation.
        if let Some(held) = cache.lock().map_err(Into::<Error>::into)?.get(key)
            && held.expires_at > now + margin
        {
            return Ok(held.epoch);
        }

        // Read the current lease and its version (etag) to CAS against.
        let (current, version) = match self.object_store.get(location).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let lease = serde_json::from_slice::<PrefixLease>(&result.bytes().await?)?;
                (Some(lease), Some(version))
            }
            Err(object_store::Error::NotFound { .. }) => (None, None),
            Err(err) => return Err(err.into()),
        };

        // "Ours" iff the object's etag matches the one we last wrote — then this
        // is a renewal, not a takeover of a foreign live lease.
        let our_version = cache
            .lock()
            .map_err(Into::<Error>::into)?
            .get(key)
            .and_then(|held| held.version.clone());
        let ours = matches!((&version, &our_version), (Some(v), Some(o)) if v.e_tag == o.e_tag);
        let expired = current
            .as_ref()
            .is_none_or(|lease| Self::now_ms() >= lease.expires_at_ms);

        // A live lease held by someone else — we are fenced. Drop any stale
        // cached term and yield.
        if !ours && !expired {
            if let Some(lease) = &current {
                debug!(key, holder = %lease.holder, epoch = lease.epoch, "lease held elsewhere");
            }
            _ = cache.lock().map(|mut leases| leases.remove(key));
            LEASE_FENCED.add(1, &[]);
            return Err(Error::Api(ErrorCode::NotLeaderOrFollower));
        }

        // Acquirable (unheld / expired / ours): bump epoch, CAS on the read
        // version so a concurrent acquirer loses.
        let epoch = current.as_ref().map(|lease| lease.epoch).unwrap_or(0) + 1;
        let lease = PrefixLease {
            epoch,
            holder: self.writer_id.clone(),
            expires_at_ms: Self::now_ms() + self.prefix_lease_ttl.as_millis() as i64,
        };
        let payload = PutPayload::from(Bytes::from(serde_json::to_vec(&lease)?));
        let mode = match &version {
            Some(version) => PutMode::Update(version.clone()),
            None => PutMode::Create,
        };

        match self
            .object_store
            .put_opts(
                location,
                payload,
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(result) => {
                let version = Some(UpdateVersion {
                    e_tag: result.e_tag,
                    version: result.version,
                });
                _ = cache.lock().map(|mut leases| {
                    _ = leases.insert(
                        key.to_owned(),
                        HeldLease {
                            epoch,
                            expires_at: now + self.prefix_lease_ttl,
                            version,
                        },
                    );
                });
                LEASE_ACQUIRES.add(1, &[]);
                debug!(key, epoch, "lease acquired/renewed");
                Ok(epoch)
            }

            // Lost the CAS: another writer acquired concurrently. Fenced.
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. },
            ) => {
                debug!(key, "lease CAS lost — fenced");
                _ = cache.lock().map(|mut leases| leases.remove(key));
                LEASE_FENCED.add(1, &[]);
                Err(Error::Api(ErrorCode::NotLeaderOrFollower))
            }

            Err(err) => Err(err.into()),
        }
    }

    /// Buffer `deflated` for a prefix-coalesced flush and await its assigned
    /// offset (#57). Like [`Self::enqueue_coalesced`] but keyed by the topition's
    /// connector prefix, so one buffer accumulates batches across every topic
    /// under the prefix and flushes them into one shared segment object. The
    /// idempotent sequence and schema were already validated by `produce`.
    async fn enqueue_prefix_coalesced(
        &self,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let prefix = self.prefix_of(topition);
        let (ack, offset) = oneshot::channel();
        let span = deflated.last_offset_delta as i64 + 1;
        let size = size_of::<i64>() + size_of::<i32>() + deflated.batch_length.max(0) as usize;

        enum Action {
            Flush(PrefixCoalesceBuffer),
            StartTimer,
            Wait,
        }

        let action = {
            let mut buffers = self
                .prefix_coalesce_buffers
                .lock()
                .map_err(Into::<Error>::into)?;
            let buffer = buffers.entry(prefix.clone()).or_default();

            let first = buffer.pending.is_empty();
            buffer.pending.push(PrefixPending {
                topition: topition.to_owned(),
                batch: deflated,
                ack,
            });
            buffer.records += span;
            buffer.bytes += size;

            if buffer.pending.len() >= self.coalesce_batches
                || buffer.bytes >= self.coalesce_bytes
                || buffer.records >= Self::COALESCE_MAX_RECORDS
            {
                Action::Flush(std::mem::take(buffer))
            } else if first {
                Action::StartTimer
            } else {
                Action::Wait
            }
        };

        match action {
            Action::Flush(buffer) => self.flush_prefix_coalesced(&prefix, buffer).await,

            Action::StartTimer => {
                let store = self.clone();
                let prefix = prefix.clone();
                let linger = self.coalesce_linger;

                _ = tokio::spawn(async move {
                    tokio::time::sleep(linger).await;

                    let buffer = store
                        .prefix_coalesce_buffers
                        .lock()
                        .ok()
                        .and_then(|mut buffers| buffers.remove(&prefix));

                    if let Some(buffer) = buffer.filter(|buffer| !buffer.pending.is_empty()) {
                        store.flush_prefix_coalesced(&prefix, buffer).await;
                    }
                });
            }

            Action::Wait => {}
        }

        offset
            .await
            .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
    }

    /// Flush a drained prefix buffer as one shared segment object and resolve
    /// each parked produce with its assigned offset (#57). Batches are grouped
    /// by topition; each sub-stream is assigned an independent base offset from
    /// its in-memory high-watermark hint (#58 assignment — the single-writer
    /// counter is authoritative, cold-start footer recovery is #58), and the
    /// whole set is written as one create-only segment via
    /// [`Self::assign_and_create_segment`]. On failure every parked producer
    /// gets the error and retries.
    async fn flush_prefix_coalesced(&self, prefix: &str, buffer: PrefixCoalesceBuffer) {
        if buffer.pending.is_empty() {
            return;
        }

        // Serialize flushes for this prefix so offset assignment (read
        // `cached_high`) → segment PUT → `set_high` is atomic (#1 fix): two
        // overlapping windows must not both read the same base offset. Held
        // across the whole flush; per-prefix, so distinct prefixes still flush
        // concurrently.
        let flush_lock = match self.prefix_flush_lock(prefix) {
            Ok(lock) => lock,
            Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
        };
        let _flush_guard = flush_lock.lock().await;

        // Fence first (#59): acquire/renew the single-writer lease before writing.
        // A fenced writer fails here and never appends a segment; the parked
        // producers get NotLeaderOrFollower and retry (routed to the live
        // writer). On a fresh acquire this precedes offset recovery below, so a
        // new writer recovers the tail before it writes (the handoff order).
        let epoch = match self.acquire_or_renew_lease(prefix).await {
            Ok(epoch) => epoch,
            Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
        };

        // Group pending batches by topition (arrival order preserved within a
        // topition). BTreeMap gives a deterministic per-segment sub-stream order.
        let mut grouped: BTreeMap<Topition, Vec<usize>> = BTreeMap::new();
        for (index, pending) in buffer.pending.iter().enumerate() {
            grouped
                .entry(pending.topition.clone())
                .or_default()
                .push(index);
        }

        // Resolve each sub-stream's base offset and the absolute offset of every
        // batch within it. `assigned[index]` is the offset the pending at that
        // index will be acked with; `advances` is the post-flush high per
        // topition.
        let mut substreams: Vec<(Topition, i64, Vec<deflated::Batch>)> =
            Vec::with_capacity(grouped.len());
        let mut assigned = vec![0i64; buffer.pending.len()];
        let mut advances: Vec<(Topition, i64)> = Vec::with_capacity(grouped.len());

        for (topition, indices) in &grouped {
            let base = match self.cached_high(topition) {
                Ok(Some(hint)) => hint,
                // Cold counter (fresh process / #59 failover): recover the next
                // offset as max(segment footer tail, legacy records/ tail,
                // persisted floor) so a seam/backfill/drained sub-stream never
                // reuses or regresses an offset (#58/#61 review fixes).
                Ok(None) => {
                    // Cold writer path: fetch the persisted floor once and hand it
                    // to recovery (which no longer fetches it itself, #72).
                    let persisted = self.persisted_high(topition).await.unwrap_or(0);
                    match self
                        .recover_substream_next_offset(topition, persisted)
                        .await
                    {
                        Ok(high) => {
                            _ = self
                                .set_high(topition, high)
                                .inspect_err(|err| debug!(?err));
                            high
                        }
                        Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
                    }
                }
                Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
            };

            let mut running = base;
            let mut batches = Vec::with_capacity(indices.len());
            for &index in indices {
                assigned[index] = running;
                let batch = &buffer.pending[index].batch;
                running += batch.last_offset_delta as i64 + 1;
                batches.push(batch.clone());
            }

            substreams.push((topition.clone(), base, batches));
            advances.push((topition.clone(), running));
        }

        let (seq, footer) = match async {
            let (payload, footer) = self.encode_segment(&substreams, epoch)?;
            let seq = self
                .assign_and_create_segment(prefix, payload, true)
                .await?;
            Ok::<_, Error>((seq, footer))
        }
        .await
        {
            Ok(result) => result,
            Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
        };

        // Populate the in-memory footer index so reads on this node need no
        // listing/GET to see the segment we just wrote (read-path #60 fix).
        _ = self
            .index_insert(prefix, seq, footer, Self::now_ms())
            .inspect_err(|err| debug!(?err));
        SEGMENT_FLUSHES.add(1, &[]);

        debug!(
            prefix,
            seq,
            substreams = substreams.len(),
            "prefix segment flushed"
        );

        // The write succeeded and is durable — advance each sub-stream's hint.
        for (topition, high) in &advances {
            _ = self
                .set_high(topition, *high)
                .inspect_err(|err| debug!(?err));
        }

        // Mirror the immediate path's lake sink per sub-batch. Config is fetched
        // once per topic and only when a lake is configured.
        let mut configs: BTreeMap<String, Option<_>> = BTreeMap::new();
        for (index, pending) in buffer.pending.into_iter().enumerate() {
            let offset = assigned[index];
            let topition = &pending.topition;

            if self.lake.is_some() {
                let config = match configs.get(topition.topic()) {
                    Some(config) => config.clone(),
                    None => {
                        let config = self
                            .describe_config(topition.topic(), ConfigResource::Topic, None)
                            .await
                            .inspect_err(|err| debug!(?err))
                            .ok();
                        _ = configs.insert(topition.topic().to_owned(), config.clone());
                        config
                    }
                };

                if let (Some(lake), Some(config)) = (self.lake.as_ref(), config.as_ref())
                    && let Ok(inflated) = inflated::Batch::try_from(&pending.batch)
                {
                    _ = lake
                        .store(
                            topition.topic(),
                            topition.partition(),
                            offset,
                            &inflated,
                            config.clone(),
                        )
                        .await
                        .inspect_err(|err| debug!(?err));
                }
            }

            _ = pending.ack.send(Ok(offset));
        }
    }

    /// Send `error` to every parked producer in a failed prefix flush (#57) so
    /// each retries; the unflushed batches are never acked.
    fn fail_prefix_flush(buffer: PrefixCoalesceBuffer, error: Error, prefix: &str) {
        error!(?error, prefix, "prefix coalesced flush failed");
        for pending in buffer.pending {
            _ = pending.ack.send(Err(error.clone()));
        }
    }

    /// The base offset of the object that *contains* `offset` — the greatest
    /// base `<= offset` — or `None` when no such object exists (offset is at or
    /// before the log start). Used only under coalescing (#50): a consumer that
    /// resumes at a sub-batch boundary inside a coalesced object must be served
    /// the whole containing object, not have it skipped by the `base >= offset`
    /// seek. Bounded by [`Self::COALESCE_MAX_RECORDS`] — the per-object span cap
    /// — so the probe reads only the tail region, never the whole partition.
    async fn coalesce_fetch_floor(&self, topition: &Topition, offset: i64) -> Result<Option<i64>> {
        let prefix = self.records_prefix(topition);
        let floor = offset.saturating_sub(Self::COALESCE_MAX_RECORDS).max(0);
        let start_after = Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}",
            self.cluster, topition.topic, topition.partition, floor,
        ));

        let mut list_stream = self
            .object_store
            .list_with_offset(Some(&prefix), &start_after);
        let mut container = None;

        while let Some(meta) = list_stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, ?topition))?
        {
            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };
            let Ok(base) = i64::from_str(&name.as_ref()[0..20]) else {
                continue;
            };

            if base <= offset {
                container = Some(base);
            } else {
                break;
            }
        }

        Ok(container)
    }

    /// Enforce the `delete` cleanup policy: for every topic configured with
    /// `cleanup.policy` containing `delete`, drop the batches whose records are
    /// older than `retention.ms` (defaulting to 7 days, matching the SQL
    /// backends). Returns the number of batches removed.
    #[instrument(skip(self), ret)]
    /// Expire consumer groups whose state object under
    /// `clusters/{cluster}/groups/consumers/` has not been modified within
    /// [`GROUP_RETENTION`]. A live group rewrites its state object on every
    /// join, heartbeat and offset commit, so an object left untouched for the
    /// whole window has had no member activity for that long and is treated as
    /// dead. Age is read from the `last_modified` of the delimiter listing, so
    /// candidate selection needs no per-group GET.
    ///
    /// Deletions are capped at [`GROUP_EXPIRE_CHUNK`] per tick so a large
    /// accumulated backlog (e.g. groups leaked by a one-group-per-subscription
    /// client model) drains gradually rather than issuing tens of thousands of
    /// deletes at once — the concentrated object-store pressure that degraded
    /// the broker in #8. See #45.
    async fn expire_groups(&self, now: SystemTime) -> Result<u64> {
        /// Kafka's default `offsets.retention.minutes` is 7 days; match it.
        const GROUP_RETENTION: Duration = Duration::from_hours(7 * 24);
        /// Maximum number of groups expired per maintenance tick.
        const GROUP_EXPIRE_CHUNK: usize = 1_000;

        let now_ms = i64::try_from(
            now.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX);
        let threshold_ms = now_ms.saturating_sub(GROUP_RETENTION.as_millis() as i64);

        let prefix = Path::from(format!("clusters/{}/groups/consumers/", self.cluster));

        let listed = self
            .object_store
            .list_with_delimiter(Some(&prefix))
            .await
            .inspect_err(|err| error!(?err, cluster = self.cluster))?;

        // The `{group}.json` files directly under the prefix are the group
        // state objects; the `{group}/` common prefixes hold the per-partition
        // committed offsets and are removed alongside them by `delete_groups`.
        let mut stale: Vec<String> = Vec::new();
        for meta in listed.objects {
            if meta.last_modified.timestamp_millis() >= threshold_ms {
                continue;
            }

            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };

            if let Some(group_id) = name.as_ref().strip_suffix(".json") {
                stale.push(group_id.to_owned());

                if stale.len() >= GROUP_EXPIRE_CHUNK {
                    break;
                }
            }
        }

        if stale.is_empty() {
            return Ok(0);
        }

        let capped = stale.len() >= GROUP_EXPIRE_CHUNK;
        let expired = stale.len() as u64;

        // `delete_groups` removes each state object and every committed-offset
        // object under the group prefix, logging the per-group outcome.
        _ = self.delete_groups(Some(&stale)).await?;

        if capped {
            info!(
                expired,
                cluster = self.cluster,
                "expire_groups hit the per-tick cap; more stale groups remain for the next tick"
            );
        } else {
            debug!(expired, cluster = self.cluster, "expire_groups");
        }

        Ok(expired)
    }

    async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        const DEFAULT_RETENTION: Duration = Duration::from_hours(7 * 24);

        let now_ms = i64::try_from(
            now.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX);

        let topics = self
            .topics_index()
            .await?
            .iter()
            .filter_map(|metadata| {
                let configs = metadata.topic.configs.as_deref().unwrap_or_default();

                let delete = configs.iter().any(|config| {
                    config.name == "cleanup.policy"
                        && config
                            .value
                            .as_deref()
                            .is_some_and(|value| value.contains("delete"))
                });

                if !delete {
                    return None;
                }

                // `retention.ms=-1` is retain-forever → i64::MAX (nothing expires),
                // not `now+1` which would delete everything.
                let retention_ms = match configs
                    .iter()
                    .find(|config| config.name == "retention.ms")
                    .and_then(|config| config.value.as_deref())
                    .and_then(|value| i64::from_str(value).ok())
                {
                    Some(ms) if ms < 0 => i64::MAX,
                    Some(ms) => ms,
                    None => DEFAULT_RETENTION.as_millis() as i64,
                };

                Some((
                    metadata.topic.name.clone(),
                    metadata.topic.num_partitions,
                    retention_ms,
                ))
            })
            .collect::<Vec<_>>();

        let mut deleted = 0;

        for (topic, num_partitions, retention_ms) in topics {
            let threshold_ms = now_ms.saturating_sub(retention_ms);

            for partition in 0..num_partitions {
                let topition = Topition::new(topic.clone(), partition);

                // Skip the full `records/` LIST when the oldest-retained hint
                // proves nothing can be past the retention threshold yet (#49).
                if !self.partition_maybe_expirable(&topition, threshold_ms)? {
                    continue;
                }

                deleted += self
                    .expire_partition(&topition, threshold_ms)
                    .await
                    .inspect_err(|err| error!(?err, ?topition))?;
            }
        }

        // Prefix-coalesced data lives in shared segments, not `records/` (the
        // per-partition loop above only drains a hybrid topic's legacy region);
        // expire whole segments per prefix (#61).
        if self.prefix_coalesce {
            deleted += self.policy_delete_segments(now_ms).await?;
        }

        Ok(deleted)
    }

    /// Whether `topition` might hold data older than `threshold_ms`, cheaply,
    /// without a LIST. Returns `false` (safe to skip the full LIST) when either
    /// (a) a maintenance scan found the legacy `records/` prefix empty within
    /// [`Self::RETENTION_EMPTY_SKIP_TTL`] (#71 — a pure-segment partition), or
    /// (b) the oldest-retained hint's known oldest-surviving timestamp is at or
    /// after the threshold (#49). Returns `true` (must scan) otherwise, including
    /// when nothing is known.
    ///
    /// Both signals are lower bounds and time-bounded, so `false` is always sound:
    /// the empty-skip self-heals after the TTL (re-scanning picks up any legacy
    /// object written meanwhile, even by another process), and the oldest-retained
    /// hint only ever under-states the oldest. Neither can hide expirable data for
    /// longer than the TTL / until the next scan.
    fn partition_maybe_expirable(&self, topition: &Topition, threshold_ms: i64) -> Result<bool> {
        if self
            .retention_empty_skip
            .lock()
            .map_err(Into::<Error>::into)?
            .get(topition)
            .and_then(|at| SystemTime::now().duration_since(*at).ok())
            .is_some_and(|elapsed| elapsed < Self::RETENTION_EMPTY_SKIP_TTL)
        {
            return Ok(false);
        }

        self.oldest_retained
            .lock()
            .map_err(Into::into)
            .map(|locked| {
                locked
                    .get(topition)
                    .is_none_or(|oldest| *oldest < threshold_ms)
            })
    }

    /// Update the oldest-retained hint for `topition` after a scan: `Some(ms)`
    /// records the oldest surviving batch's `last_modified`; `None` (no surviving
    /// object) drops the entry.
    fn record_oldest_retained(&self, topition: &Topition, oldest_ms: Option<i64>) -> Result<()> {
        self.oldest_retained
            .lock()
            .map_err(Into::into)
            .map(|mut locked| match oldest_ms {
                Some(ms) => {
                    _ = locked.insert(topition.to_owned(), ms);
                }
                None => {
                    _ = locked.remove(topition);
                }
            })
    }

    /// Record that a maintenance scan found `topition`'s legacy `records/` prefix
    /// empty under prefix coalescing (#71), so it is skipped for
    /// [`Self::RETENTION_EMPTY_SKIP_TTL`] before being re-scanned once. See
    /// [`Self::retention_empty_skip`].
    fn record_retention_empty_skip(&self, topition: &Topition) -> Result<()> {
        self.retention_empty_skip
            .lock()
            .map_err(Into::into)
            .map(|mut locked| {
                _ = locked.insert(topition.to_owned(), SystemTime::now());
            })
    }

    /// Whether a prefix might hold a segment older than `threshold_ms`, from the
    /// per-prefix oldest-retained hint (#61, the per-prefix analogue of
    /// [`Self::partition_maybe_expirable`]). `true` (must scan) when unknown.
    fn prefix_maybe_expirable(&self, prefix: &str, threshold_ms: i64) -> Result<bool> {
        self.oldest_retained_prefix
            .lock()
            .map_err(Into::into)
            .map(|locked| {
                locked
                    .get(prefix)
                    .is_none_or(|oldest| *oldest < threshold_ms)
            })
    }

    /// Update the per-prefix oldest-retained hint after a segment scan (#61).
    fn record_prefix_oldest_retained(&self, prefix: &str, oldest_ms: Option<i64>) -> Result<()> {
        self.oldest_retained_prefix
            .lock()
            .map_err(Into::into)
            .map(|mut locked| match oldest_ms {
                Some(ms) => {
                    _ = locked.insert(prefix.to_owned(), ms);
                }
                None => {
                    _ = locked.remove(prefix);
                }
            })
    }

    /// Delete every segment under `prefix` whose records are all older than
    /// `threshold_ms` (#61). A segment is written atomically, so all its
    /// sub-streams share an append time; it is expirable only when the newest
    /// record across **every** sub-stream (the max footer timestamp, falling back
    /// to the object's append time when record timestamps are unset) is past the
    /// threshold — never dropping a segment while any topic in it is still live.
    /// Deletes in bounded `DeleteObjects` chunks and refreshes the per-prefix
    /// skip hint from what survived.
    async fn expire_prefix_segments(&self, prefix: &str, threshold_ms: i64) -> Result<u64> {
        /// Matches the S3 `DeleteObjects` per-request key cap.
        const EXPIRE_DELETE_CHUNK: usize = 1_000;

        self.refresh_prefix_index(prefix).await?;

        // Decide from the cached footers (no per-segment footer GET). A segment
        // is expirable only when its newest record across every sub-stream (max
        // footer timestamp, or the object append time when record timestamps are
        // unset) is past the threshold — so a live topic never loses a shared
        // segment.
        let (expirable, affected, surviving_oldest_ms): (
            Vec<u64>,
            BTreeSet<(String, i32)>,
            Option<i64>,
        ) = {
            let index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            let mut expirable = Vec::new();
            let mut affected = BTreeSet::new();
            let mut surviving: Option<i64> = None;

            if let Some(entry) = index.get(prefix) {
                for (seq, cached) in &entry.segments {
                    let newest = cached
                        .footer
                        .entries
                        .iter()
                        .map(|e| e.max_timestamp)
                        .max()
                        .unwrap_or(i64::MIN);
                    let age_ms = if newest > 0 {
                        newest
                    } else {
                        cached.last_modified_ms
                    };

                    if age_ms < threshold_ms {
                        expirable.push(*seq);
                        for e in &cached.footer.entries {
                            _ = affected.insert((e.topic.clone(), e.partition));
                        }
                    } else {
                        surviving = Some(surviving.map_or(age_ms, |o: i64| o.min(age_ms)));
                    }
                }
            }
            (expirable, affected, surviving)
        };

        self.record_prefix_oldest_retained(prefix, surviving_oldest_ms)?;

        if expirable.is_empty() {
            return Ok(0);
        }

        // Persist a durable offset floor for every sub-stream losing segments
        // BEFORE deleting (#61 review fix): if a drain removes a sub-stream's last
        // segment, cold recovery must not regress to 0 / reuse offsets. The floor
        // is the sub-stream's current tail; recovery folds it in via `max`.
        // Bounded to the affected sub-streams (a window's worth), on the maintain
        // path — not the produce hot path.
        for (topic, partition) in &affected {
            let tail = self
                .valid_substream_segments(prefix, topic, *partition)?
                .last()
                .map(|(_, e)| e.base_offset + e.record_count);
            if let Some(tail) = tail {
                let tp = Topition::new(topic.clone(), *partition);
                _ = self
                    .watermark(&tp)?
                    .with_mut(&self.object_store, |watermark| {
                        watermark.high = Some(watermark.high.unwrap_or(0).max(tail));
                        Ok(())
                    })
                    .await
                    .inspect_err(|err| debug!(?err, ?tp));
            }
        }

        // Delete the expired segment objects in bounded chunks.
        let mut deleted: u64 = 0;
        let mut chunk: Vec<Path> = Vec::new();
        for seq in &expirable {
            chunk.push(self.segment_location(prefix, *seq));
            if chunk.len() >= EXPIRE_DELETE_CHUNK {
                deleted += chunk.len() as u64;
                self.delete_batches(std::mem::take(&mut chunk)).await?;
            }
        }
        if !chunk.is_empty() {
            deleted += chunk.len() as u64;
            self.delete_batches(chunk).await?;
        }

        self.index_prune(prefix, &expirable)?;

        Ok(deleted)
    }

    /// Compact a prefix's oldest segments into fewer, larger ones (#66) to bound
    /// the live segment count `S` (otherwise ≈ flush_rate × retention, unbounded)
    /// — keeping the footer index footprint and the per-fetch scan bounded.
    /// Returns the number of segments merged away.
    ///
    /// Coordinator-free and GCS-safe: only the single lease holder compacts, the
    /// merged segment is written as a new create-only object (the merged records
    /// are byte-identical to the originals, #64 contract preserved) carrying the
    /// max input epoch, and only then are the originals deleted — no object is
    /// ever mutated. During the write→delete window the merged and original
    /// segments overlap in offset, but they hold identical records and the read
    /// path's overlap resolver returns exactly one copy, so a concurrent fetch is
    /// correct; a fetch that GETs an original just as it is deleted retries off a
    /// refreshed index (see `fetch_prefix_coalesced`).
    async fn compact_prefix_segments(&self, prefix: &str) -> Result<u64> {
        if self.prefix_compact_min_segments == 0 {
            return Ok(0);
        }

        self.refresh_prefix_index(prefix).await?;

        // Snapshot (seq, epoch, last_modified, region bytes) for every cached
        // segment, ascending by seq (== ascending offset for a sub-stream).
        let mut segs: Vec<(u64, i64, i64, usize)> = {
            let index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            index
                .get(prefix)
                .map(|entry| {
                    entry
                        .segments
                        .iter()
                        .map(|(seq, cached)| {
                            let bytes: usize = cached
                                .footer
                                .entries
                                .iter()
                                .map(|e| e.byte_len as usize)
                                .sum();
                            (
                                *seq,
                                cached.footer.writer_epoch,
                                cached.last_modified_ms,
                                bytes,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default()
        };
        segs.sort_by_key(|(seq, ..)| *seq);

        // Only above the trigger, and never touch the hot (newest) tail.
        if segs.len() <= self.prefix_compact_min_segments {
            return Ok(0);
        }
        let eligible_end = segs.len().saturating_sub(self.prefix_compact_keep_hot);
        if eligible_end < 2 {
            return Ok(0);
        }

        // Pick a run from the oldest, up to the target size (at least 2). The
        // merged epoch is taken from the fenced view below, not from this raw
        // scan, so the segment epoch is ignored here.
        let mut run: Vec<u64> = Vec::new();
        let mut bytes = 0usize;
        let mut max_last_modified = i64::MIN;
        for (seq, _epoch, last_modified, size) in &segs[..eligible_end] {
            if !run.is_empty() && bytes + size > self.prefix_compact_target_bytes {
                break;
            }
            run.push(*seq);
            bytes += size;
            max_last_modified = max_last_modified.max(*last_modified);
        }
        if run.len() < 2 {
            return Ok(0);
        }

        // Coordinate compactors with a *separate* compaction lease (#66 review):
        // compaction runs on the maintenance workers, which do not hold the
        // produce lease, so it must not require — or fence — the produce writer.
        // If another compactor holds this prefix, yield.
        if self.acquire_compaction_lease(prefix).await.is_err() {
            return Ok(0);
        }

        // Snapshot the run's footers; GET each run segment once.
        let footers: BTreeMap<u64, SegmentFooter> = {
            let index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            let entry = index.get(prefix);
            run.iter()
                .filter_map(|seq| {
                    entry
                        .and_then(|e| e.segments.get(seq))
                        .map(|cached| (*seq, cached.footer.clone()))
                })
                .collect()
        };

        let mut objects: BTreeMap<u64, Bytes> = BTreeMap::new();
        for seq in &run {
            let object = self
                .object_store
                .get(&self.segment_location(prefix, *seq))
                .await
                .inspect_err(|err| error!(?err, prefix, seq))?
                .bytes()
                .await
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?;
            _ = objects.insert(*seq, object);
        }

        // Merge the EPOCH-FENCED view (#66 review fix, critical): rebuild each
        // sub-stream from `valid_substream_segments` (overlap-resolved, higher
        // epoch/sequence wins) restricted to the run — NOT the raw footer
        // entries. A zombie/overlap input is dropped here, never fused into the
        // merged segment, so compaction can't bake in duplicate/shifted offsets.
        let run_set: BTreeSet<u64> = run.iter().copied().collect();
        let substream_keys: BTreeSet<(String, i32)> = footers
            .values()
            .flat_map(|footer| {
                footer
                    .entries
                    .iter()
                    .map(|e| (e.topic.clone(), e.partition))
            })
            .collect();

        let mut substreams: Vec<(Topition, i64, Vec<deflated::Batch>)> = Vec::new();
        let mut merged_epoch = i64::MIN;

        for (topic, partition) in substream_keys {
            let in_run: Vec<(u64, SubstreamEntry)> = self
                .valid_substream_segments(prefix, &topic, partition)?
                .into_iter()
                .filter(|(seq, _)| run_set.contains(seq))
                .collect();
            if in_run.is_empty() {
                // Every run segment holding this sub-stream is superseded by a
                // segment outside the run — nothing to carry forward.
                continue;
            }

            let base = in_run[0].1.base_offset;
            let mut batches = Vec::new();
            for (seq, entry) in &in_run {
                let Some(object) = objects.get(seq) else {
                    continue;
                };
                let start = entry.byte_start as usize;
                let end = start + entry.byte_len as usize;
                if end > object.len() {
                    continue;
                }
                batches.extend(self.decode_frame(object.slice(start..end))?);
                if let Some(footer) = footers.get(seq) {
                    merged_epoch = merged_epoch.max(footer.writer_epoch);
                }
            }
            substreams.push((Topition::new(topic, partition), base, batches));
        }

        // Write the merged segment (create-only, no produce-lease fencing), index
        // it (preserving the max input append time so retention isn't reset),
        // then delete ALL run segments — including any zombie/dominated ones,
        // whose data was intentionally excluded above.
        let new_seq = if substreams.is_empty() {
            None
        } else {
            let (payload, footer) = self.encode_segment(&substreams, merged_epoch.max(0))?;
            let seq = self
                .assign_and_create_segment(prefix, payload, false)
                .await?;
            self.index_insert(prefix, seq, footer, max_last_modified)?;
            Some(seq)
        };

        let locations: Vec<Path> = run
            .iter()
            .map(|seq| self.segment_location(prefix, *seq))
            .collect();
        self.delete_batches(locations).await?;
        self.index_prune(prefix, &run)?;

        SEGMENT_COMPACTIONS.add(run.len() as u64, &[]);
        debug!(
            prefix,
            ?new_seq,
            merged = run.len(),
            "compacted prefix segments"
        );

        Ok(run.len() as u64)
    }

    /// Compact segments across every coalesced prefix (#66).
    async fn policy_compact_segments(&self) -> Result<u64> {
        if !self.prefix_coalesce || self.prefix_compact_min_segments == 0 {
            return Ok(0);
        }

        // Derive the prefixes from the topic metadata (#66 review fix), NOT just
        // the in-memory index: a dedicated maintenance worker never produces or
        // fetches, so its index is empty — it must discover prefixes from the
        // topics (as retention does). Union with any locally-indexed prefixes.
        let mut prefix_set: BTreeSet<String> = self
            .prefix_index
            .lock()
            .map_err(Into::<Error>::into)?
            .keys()
            .cloned()
            .collect();
        for metadata in self.topics_index().await?.iter() {
            let compact = metadata
                .topic
                .configs
                .as_deref()
                .unwrap_or_default()
                .iter()
                .any(|config| {
                    config.name == "cleanup.policy"
                        && config
                            .value
                            .as_deref()
                            .is_some_and(|value| value.contains("compact"))
                });
            if compact {
                continue;
            }
            for partition in 0..metadata.topic.num_partitions {
                _ = prefix_set
                    .insert(self.prefix_of(&Topition::new(metadata.topic.name.clone(), partition)));
            }
        }
        let prefixes: Vec<String> = prefix_set.into_iter().collect();

        /// Bounds the drain loop so a pathological prefix can't monopolize a
        /// maintenance tick; far above the runs a real backlog needs.
        const MAX_RUNS_PER_PREFIX: usize = 4_096;

        let mut compacted = 0;
        for prefix in prefixes {
            // Drain the prefix to <= min_segments in one tick (#66 review fix):
            // a single run per tick can't keep up with a high flush rate, so loop
            // until compaction finds nothing more to merge. Each call re-lists,
            // so `S` converges to the trigger threshold within the tick.
            for _ in 0..MAX_RUNS_PER_PREFIX {
                match self
                    .compact_prefix_segments(&prefix)
                    .await
                    .inspect_err(|err| error!(?err, prefix))
                {
                    Ok(0) | Err(_) => break,
                    Ok(n) => compacted += n,
                }
            }

            // Report the live segment count so runaway `S` is observable even if
            // the drain can't keep up.
            if let Ok(index) = self.prefix_index.lock()
                && let Some(entry) = index.get(&prefix)
            {
                SEGMENTS_LIVE.record(entry.segments.len() as u64, &[]);
            }
        }
        Ok(compacted)
    }

    /// Whole-segment retention across all coalesced prefixes (#61). Groups the
    /// topics by connector prefix and expires each prefix's segments under one
    /// **uniform** retention — the longest `retention.ms` among the prefix's
    /// topics, so a per-topic override can never delete a shared segment early
    /// (heterogeneous per-topic retention is not honoured in coalesced mode, per
    /// the epic; the longest wins). Every non-compacted topic counts: Kafka's
    /// default `cleanup.policy` is `delete`, and a topic with no explicit policy
    /// still coalesces into the shared segments, so it must be included in the
    /// max — otherwise a sibling's shorter retention could delete its data (#61
    /// review fix). `retention.ms=-1` (retain forever) makes the whole prefix
    /// infinite. Compacted topics never reach segments (legacy path at produce).
    async fn policy_delete_segments(&self, now_ms: i64) -> Result<u64> {
        const DEFAULT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

        let mut retention_by_prefix: BTreeMap<String, i64> = BTreeMap::new();

        for metadata in self.topics_index().await?.iter() {
            let configs = metadata.topic.configs.as_deref().unwrap_or_default();

            let compact = configs.iter().any(|config| {
                config.name == "cleanup.policy"
                    && config
                        .value
                        .as_deref()
                        .is_some_and(|value| value.contains("compact"))
            });

            // Compacted topics stay on the legacy path — never in segments.
            if compact {
                continue;
            }

            // `retention.ms=-1` is retain-forever → treat as effectively infinite
            // (mapped to i64::MAX, so `now - retention` floors to the log start and
            // nothing expires). Absent → the 7-day default.
            let retention_ms = match configs
                .iter()
                .find(|config| config.name == "retention.ms")
                .and_then(|config| config.value.as_deref())
                .and_then(|value| i64::from_str(value).ok())
            {
                Some(ms) if ms < 0 => i64::MAX,
                Some(ms) => ms,
                None => DEFAULT_RETENTION_MS,
            };

            for partition in 0..metadata.topic.num_partitions {
                let prefix = self.prefix_of(&Topition::new(metadata.topic.name.clone(), partition));

                _ = retention_by_prefix
                    .entry(prefix)
                    .and_modify(|existing| {
                        if retention_ms > *existing {
                            *existing = retention_ms;
                        }
                    })
                    .or_insert(retention_ms);
            }
        }

        let mut deleted = 0;

        for (prefix, retention_ms) in retention_by_prefix {
            let threshold_ms = now_ms.saturating_sub(retention_ms);

            if !self.prefix_maybe_expirable(&prefix, threshold_ms)? {
                continue;
            }

            deleted += self
                .expire_prefix_segments(&prefix, threshold_ms)
                .await
                .inspect_err(|err| error!(?err, prefix))?;
        }

        Ok(deleted)
    }

    /// List the batch files of `topition` as a map of base offset to its object
    /// metadata (sorted ascending by offset).
    async fn list_batch_offsets(&self, topition: &Topition) -> Result<BTreeMap<i64, ObjectMeta>> {
        let prefix = self.records_prefix(topition);

        let mut offsets = BTreeMap::new();
        let mut list_stream = self.object_store.list(Some(&prefix));

        while let Some(meta) = list_stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, ?topition))?
        {
            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };

            let Ok(offset) = i64::from_str(&name.as_ref()[0..20]) else {
                continue;
            };

            _ = offsets.insert(offset, meta);
        }

        Ok(offsets)
    }

    /// List the base offsets of `topition`'s batch files, sorted ascending.
    ///
    /// Unlike [`Self::list_batch_offsets`] this only retains the `i64` offsets
    /// (not the full [`ObjectMeta`] of every object), so the transient
    /// allocation stays small on hot partitions with many batch objects (#8).
    async fn list_batch_offset_keys(&self, topition: &Topition) -> Result<Vec<i64>> {
        let prefix = self.records_prefix(topition);

        let mut offsets = Vec::new();
        let mut list_stream = self.object_store.list(Some(&prefix));

        while let Some(meta) = list_stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, ?topition))?
        {
            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };

            let Ok(offset) = i64::from_str(&name.as_ref()[0..20]) else {
                continue;
            };

            offsets.push(offset);
        }

        offsets.sort_unstable();

        Ok(offsets)
    }

    /// Set the log start offset (`watermark.low`) to `low`.
    async fn advance_low_watermark(&self, topition: &Topition, low: Option<i64>) -> Result<()> {
        self.watermark(topition)?
            .with_mut(&self.object_store, |watermark| {
                watermark.low = low;
                Ok(())
            })
            .await
    }

    /// Delete the given batch object locations.
    ///
    /// S3 can throttle a multi-object `DeleteObjects` by returning **HTTP 200**
    /// with a top-level `<Error><Code>SlowDown</Code></Error>` body instead of
    /// the expected `<DeleteResult>`. The `object_store` S3 client only retries
    /// on the HTTP status, so a 200-with-error body slips past its retry loop
    /// and then fails XML deserialisation (`unknown variant `Code``), surfacing
    /// as a non-retryable error even though *nothing was deleted* (#5).
    ///
    /// We therefore retry the whole bulk delete ourselves, with backoff, on a
    /// detected throttle; once those retries are exhausted we fall back to
    /// per-key deletes, whose `503 SlowDown` is status-coded and so is retried
    /// by the store's own `RetryConfig`, side-stepping the bulk parse bug.
    async fn delete_batches(&self, locations: Vec<Path>) -> Result<()> {
        /// Times to retry the whole bulk delete on a detected S3 throttle before
        /// falling back to per-key deletes.
        const MAX_BULK_THROTTLE_RETRIES: u32 = 5;

        if locations.is_empty() {
            return Ok(());
        }

        let mut attempt = 0u32;

        loop {
            match self.bulk_delete(locations.clone()).await {
                Ok(()) => return Ok(()),

                Err(err) if is_s3_throttle(&err) => {
                    if attempt >= MAX_BULK_THROTTLE_RETRIES {
                        warn!(
                            %err,
                            "bulk DeleteObjects still throttled after retries; falling back to per-key deletes"
                        );
                        return self.delete_each(locations).await;
                    }

                    let backoff = throttle_backoff(attempt);
                    warn!(%err, attempt, ?backoff, "S3 throttled DeleteObjects; backing off then retrying");
                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                }

                Err(err) => return Err(err.into()),
            }
        }
    }

    /// Issue a single bulk `DeleteObjects` for `locations`.
    async fn bulk_delete(&self, locations: Vec<Path>) -> Result<(), object_store::Error> {
        let stream = futures::stream::iter(locations.into_iter().map(Ok)).boxed();

        self.object_store
            .delete_stream(stream)
            .try_collect::<Vec<Path>>()
            .await
            .map(|_| ())
    }

    /// Delete each location individually, ignoring already-absent objects. Used
    /// as a throttle fallback: a single-object DELETE returns a real `503`
    /// status that the store's `RetryConfig` retries.
    async fn delete_each(&self, locations: Vec<Path>) -> Result<()> {
        /// Bounded concurrency for the per-key fallback. A purely sequential
        /// pass would issue up to a full `DeleteObjects` batch (1000) of
        /// round-trips back-to-back; a small fan-out makes progress without
        /// re-creating the request burst that tripped the throttle.
        const DELETE_EACH_CONCURRENCY: usize = 16;

        let object_store = &self.object_store;

        futures::stream::iter(locations)
            .map(|location| async move {
                match object_store.delete(&location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(err) => Err(Error::from(err)),
                }
            })
            .buffer_unordered(DELETE_EACH_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await
            .map(|_| ())
    }

    /// Delete every batch of `topition` written before `now - retention_ms`, then
    /// advance the log start offset (`watermark.low`) to the oldest surviving
    /// batch. Age is taken from the object's `last_modified` (the append time),
    /// streamed from the object store so no per-batch GET is required.
    ///
    /// The listing is consumed as a stream and expired locations are deleted in
    /// bounded chunks (`EXPIRE_DELETE_CHUNK`) rather than materialising the whole
    /// partition's object metadata in memory first — a hot partition can hold a
    /// very large number of tiny batch objects, and collecting them all into a
    /// map drives the per-tick memory spike seen on every broker pod (#8).
    async fn expire_partition(&self, topition: &Topition, threshold_ms: i64) -> Result<u64> {
        /// Maximum number of expired locations buffered before a delete is
        /// flushed. Matches the S3 `DeleteObjects` per-request key cap.
        const EXPIRE_DELETE_CHUNK: usize = 1_000;

        let prefix = self.records_prefix(topition);
        let mut list_stream = self.object_store.list(Some(&prefix));

        let mut surviving_low: Option<i64> = None;
        let mut surviving_oldest_ms: Option<i64> = None;
        let mut chunk: Vec<Path> = Vec::new();
        let mut deleted: u64 = 0;

        while let Some(meta) = list_stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, ?topition))?
        {
            let Some(name) = meta.location.parts().next_back() else {
                continue;
            };

            let Ok(offset) = i64::from_str(&name.as_ref()[0..20]) else {
                continue;
            };

            let last_modified_ms = meta.last_modified.timestamp_millis();

            if last_modified_ms < threshold_ms {
                chunk.push(meta.location);

                if chunk.len() >= EXPIRE_DELETE_CHUNK {
                    deleted += chunk.len() as u64;
                    self.delete_batches(std::mem::take(&mut chunk)).await?;
                }
            } else {
                if surviving_low.is_none_or(|low| offset < low) {
                    surviving_low = Some(offset);
                }
                surviving_oldest_ms =
                    Some(surviving_oldest_ms.map_or(last_modified_ms, |m| m.min(last_modified_ms)));
            }
        }

        if !chunk.is_empty() {
            deleted += chunk.len() as u64;
            self.delete_batches(chunk).await?;
        }

        // Refresh the skip hint from what this scan observed — even when nothing
        // was deleted, so a partition well within retention is skipped next tick
        // (#49).
        self.record_oldest_retained(topition, surviving_oldest_ms)?;

        // #71: under prefix coalescing a pure-segment partition has an empty
        // legacy `records/` prefix, so this scan found nothing every tick — a
        // per-topic LIST that would otherwise never go away (segment retention is
        // handled per-prefix by `expire_prefix_segments`, #61). Record a
        // time-bounded skip so subsequent ticks skip the empty LIST, self-healing
        // after `RETENTION_EMPTY_SKIP_TTL` so a legacy object written afterwards —
        // possibly by another process, since the broker and the dedicated
        // `maintain` worker do not share this map — is still expired within one
        // TTL. The non-coalesce path is unchanged: ordinary writes go to
        // `records/` there, so an empty partition must keep being re-scanned.
        if self.prefix_coalesce && surviving_oldest_ms.is_none() {
            self.record_retention_empty_skip(topition)?;
        }

        if deleted == 0 {
            return Ok(0);
        }

        self.advance_low_watermark(topition, surviving_low).await?;

        Ok(deleted)
    }

    /// Delete every batch of `topition` whose base offset is below `before` and
    /// set the log start offset to `before`. `before` of `-1` means the log end
    /// offset (delete everything). Returns the new log start offset.
    async fn delete_records_before(&self, topition: &Topition, before: i64) -> Result<i64> {
        // The log end offset comes from the immutable batch objects (the
        // authority), not the write-behind `watermark` object, which is no
        // longer advanced on the produce hot path (#13).
        let high = self.high_watermark(topition).await?;

        let before = if before < 0 { high } else { before.min(high) };

        let removed = self
            .list_batch_offsets(topition)
            .await?
            .into_iter()
            .filter(|(offset, _)| *offset < before)
            .map(|(_, meta)| meta.location)
            .collect::<Vec<_>>();

        self.delete_batches(removed).await?;
        self.advance_low_watermark(topition, Some(before)).await?;

        Ok(before)
    }

    /// Enforce the `compact` cleanup policy: for every topic configured with
    /// `cleanup.policy` containing `compact`, keep only the latest record per key
    /// (by offset), dropping the earlier versions. Surviving offsets are
    /// preserved. Returns the number of records removed.
    #[instrument(skip(self), ret)]
    async fn policy_compact(&self) -> Result<u64> {
        let topics = self
            .topics_index()
            .await?
            .iter()
            .filter_map(|metadata| {
                let configs = metadata.topic.configs.as_deref().unwrap_or_default();

                let compact = configs.iter().any(|config| {
                    config.name == "cleanup.policy"
                        && config
                            .value
                            .as_deref()
                            .is_some_and(|value| value.contains("compact"))
                });

                compact.then(|| (metadata.topic.name.clone(), metadata.topic.num_partitions))
            })
            .collect::<Vec<_>>();

        let mut compacted = 0;

        for (topic, num_partitions) in topics {
            for partition in 0..num_partitions {
                let topition = Topition::new(topic.clone(), partition);

                compacted += self
                    .compact_partition(&topition)
                    .await
                    .inspect_err(|err| error!(?err, ?topition))?;
            }
        }

        Ok(compacted)
    }

    /// Compact a single partition: walking the batches newest first, drop every
    /// record whose key reappears in a more recent batch (and earlier duplicates
    /// within a batch). Emptied batch files are removed, partially compacted ones
    /// are rewritten in place (preserving base offset and record offsets).
    async fn compact_partition(&self, topition: &Topition) -> Result<u64> {
        let offsets = self.list_batch_offset_keys(topition).await?;

        if offsets.is_empty() {
            return Ok(0);
        }

        let mut seen: BTreeSet<Bytes> = BTreeSet::new();
        let mut removed = vec![];
        let mut surviving_low: Option<i64> = None;
        let mut compacted = 0;

        // newest to oldest: a key kept in a newer batch supersedes older ones
        for offset in offsets.into_iter().rev() {
            let location = self.batch_location(topition, offset);

            let deflated = match self.object_store.get(&location).await {
                Ok(get_result) => self.decode(get_result.bytes().await?)?,
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(err) => return Err(err.into()),
            };

            let inflated::Compaction { batch, records } =
                inflated::Batch::try_from(&deflated)?.compact(&seen)?;

            compacted += records;
            seen.extend(batch.keys());

            if batch.records.is_empty() {
                removed.push(location);
            } else {
                if records > 0 {
                    let payload = self.encode(deflated::Batch::try_from(batch)?)?;

                    _ = self
                        .object_store
                        .put_opts(
                            &location,
                            payload,
                            PutOptions {
                                mode: PutMode::Overwrite,
                                attributes: Attributes::new(),
                                ..Default::default()
                            },
                        )
                        .await
                        .inspect_err(|err| error!(?err, ?topition, offset))?;
                }

                if surviving_low.is_none_or(|low| offset < low) {
                    surviving_low = Some(offset);
                }
            }
        }

        if !removed.is_empty() {
            self.delete_batches(removed).await?;
            self.advance_low_watermark(topition, surviving_low).await?;
        }

        Ok(compacted as u64)
    }

    fn encode(&self, deflated: deflated::Batch) -> Result<PutPayload> {
        Ok(PutPayload::from(Bytes::from(deflated)))
    }

    fn decode(&self, encoded: Bytes) -> Result<deflated::Batch> {
        debug!(encoded = ?&encoded[..]);
        deflated::Batch::try_from(encoded)
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    /// Decode a stored `records/` object into its constituent batches.
    ///
    /// An object written by the coalescing produce path (#50) holds several
    /// Kafka record batches concatenated; a legacy or non-coalesced object holds
    /// exactly one. The wire layout is identical either way — each batch is
    /// `base_offset (i64) + batch_length (i32) + batch_length bytes` — so this
    /// handles both, and a single-batch object decodes to a one-element vec that
    /// is byte-for-byte what [`Self::decode`] would return. Trailing bytes that
    /// do not form a whole batch are ignored (mirroring `Batch::try_from`).
    fn decode_frame(&self, encoded: Bytes) -> Result<Vec<deflated::Batch>> {
        // base_offset (i64) + batch_length (i32) precede the `batch_length` body.
        const PREFIX: usize = size_of::<i64>() + size_of::<i32>();

        let mut batches = Vec::new();
        let mut remaining = encoded;

        while remaining.len() >= PREFIX {
            let mut length = [0u8; size_of::<i32>()];
            length.copy_from_slice(&remaining[size_of::<i64>()..PREFIX]);
            let batch_length = usize::try_from(i32::from_be_bytes(length))?;

            let total = PREFIX + batch_length;
            if total > remaining.len() {
                break;
            }

            batches.push(self.decode(remaining.slice(0..total))?);
            remaining = remaining.slice(total..);
        }

        Ok(batches)
    }

    /// Serialize a run of contiguous batches into one `records/` object payload
    /// (the coalescing produce write, #50). The batches are concatenated in wire
    /// order; a single-batch slice is byte-identical to [`Self::encode`], so a
    /// coalesced object and a legacy object are read back the same way by
    /// [`Self::decode_frame`].
    fn encode_frame(&self, batches: &[deflated::Batch]) -> Result<PutPayload> {
        let mut buf = Vec::new();
        for batch in batches {
            buf.extend_from_slice(&Bytes::from(batch.clone()));
        }
        Ok(PutPayload::from(Bytes::from(buf)))
    }

    /// Serialize many `(topic, partition)` sub-streams into one shared,
    /// prefix-coalesced segment object (#64) — the write produced by #57. Each
    /// sub-stream's batches are concatenated contiguously (byte-compatible with
    /// [`Self::encode_frame`], so a region decodes with [`Self::decode_frame`]);
    /// the regions are laid end to end; then a self-describing [`SegmentFooter`]
    /// and a fixed [`SEGMENT_TRAILER_LEN`] trailer are appended. A reader
    /// locates any sub-stream by footer lookup + a ranged GET of its byte span
    /// (#60) rather than deriving offsets from the filename. Each element is
    /// `(topition, base_offset, batches)`, where `base_offset` is the absolute
    /// offset already assigned to the sub-stream's first record (#58).
    /// `writer_epoch` is the producing writer's lease epoch (#59), stamped into
    /// the footer so a fenced writer's segment is identifiable. Empty
    /// sub-streams are skipped. Returns the payload and the footer, which the
    /// writer keeps as the segment's in-memory index.
    fn encode_segment(
        &self,
        substreams: &[(Topition, i64, Vec<deflated::Batch>)],
        writer_epoch: i64,
    ) -> Result<(PutPayload, SegmentFooter)> {
        let mut body = Vec::new();
        let mut entries = Vec::with_capacity(substreams.len());

        for (topition, base_offset, batches) in substreams {
            if batches.is_empty() {
                continue;
            }

            let byte_start = body.len() as u64;
            let mut record_count = 0i64;
            let mut max_timestamp = i64::MIN;

            for batch in batches {
                body.extend_from_slice(&Bytes::from(batch.clone()));
                record_count += batch.last_offset_delta as i64 + 1;
                max_timestamp = max_timestamp.max(batch.max_timestamp);
            }

            entries.push(SubstreamEntry {
                topic: topition.topic().to_owned(),
                partition: topition.partition(),
                base_offset: *base_offset,
                record_count,
                byte_start,
                byte_len: body.len() as u64 - byte_start,
                max_timestamp,
            });
        }

        let footer = SegmentFooter {
            writer_epoch,
            entries,
        };
        let footer_bytes = Self::encode_footer(&footer);

        body.extend_from_slice(&footer_bytes);
        body.extend_from_slice(&(footer_bytes.len() as u64).to_be_bytes());
        body.extend_from_slice(&(footer.entries.len() as u32).to_be_bytes());
        body.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_be_bytes());
        body.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());

        Ok((PutPayload::from(Bytes::from(body)), footer))
    }

    /// Serialize a [`SegmentFooter`] index (#64/#59). Header: `writer_epoch
    /// (i64)`. Then each entry: `topic_len (u16) + topic (utf8) +
    /// partition (i32) + base_offset (i64) + record_count (i64) +
    /// byte_start (u64) + byte_len (u64) + max_timestamp (i64)`, all big-endian.
    /// Paired with [`Self::decode_footer`].
    fn encode_footer(footer: &SegmentFooter) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&footer.writer_epoch.to_be_bytes());
        for entry in &footer.entries {
            let topic = entry.topic.as_bytes();
            buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
            buf.extend_from_slice(topic);
            buf.extend_from_slice(&entry.partition.to_be_bytes());
            buf.extend_from_slice(&entry.base_offset.to_be_bytes());
            buf.extend_from_slice(&entry.record_count.to_be_bytes());
            buf.extend_from_slice(&entry.byte_start.to_be_bytes());
            buf.extend_from_slice(&entry.byte_len.to_be_bytes());
            buf.extend_from_slice(&entry.max_timestamp.to_be_bytes());
        }
        buf
    }

    /// Parse a [`SegmentFooter`] from `footer_bytes`, the `footer_len` bytes that
    /// precede the trailer (#64). Inverse of [`Self::encode_footer`]; a
    /// truncated or malformed footer is a corrupt segment, not a legacy object.
    fn decode_footer(footer_bytes: &[u8], entry_count: usize) -> Result<SegmentFooter> {
        let mut entries = Vec::with_capacity(entry_count);
        let mut cursor = footer_bytes;

        fn take<'a>(cursor: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
            if cursor.len() < n {
                return Err(Error::Message(String::from("truncated segment footer")));
            }
            let (head, tail) = cursor.split_at(n);
            *cursor = tail;
            Ok(head)
        }

        let writer_epoch = i64::from_be_bytes(take(&mut cursor, 8)?.try_into()?);

        for _ in 0..entry_count {
            let topic_len = u16::from_be_bytes(take(&mut cursor, 2)?.try_into()?) as usize;
            let topic = String::from_utf8(take(&mut cursor, topic_len)?.to_vec())
                .map_err(|e| Error::Message(e.to_string()))?;
            let partition = i32::from_be_bytes(take(&mut cursor, 4)?.try_into()?);
            let base_offset = i64::from_be_bytes(take(&mut cursor, 8)?.try_into()?);
            let record_count = i64::from_be_bytes(take(&mut cursor, 8)?.try_into()?);
            let byte_start = u64::from_be_bytes(take(&mut cursor, 8)?.try_into()?);
            let byte_len = u64::from_be_bytes(take(&mut cursor, 8)?.try_into()?);
            let max_timestamp = i64::from_be_bytes(take(&mut cursor, 8)?.try_into()?);

            entries.push(SubstreamEntry {
                topic,
                partition,
                base_offset,
                record_count,
                byte_start,
                byte_len,
                max_timestamp,
            });
        }

        Ok(SegmentFooter {
            writer_epoch,
            entries,
        })
    }

    /// Recover the [`SegmentFooter`] of a segment given its tail bytes (#64):
    /// `tail` must include at least the footer and the [`SEGMENT_TRAILER_LEN`]
    /// trailer (in practice the last N bytes fetched by a ranged GET, or the
    /// whole object). Returns `Ok(None)` when the trailer magic is absent — the
    /// object is a legacy single-topic coalesced object (#50, the v0 case) and
    /// must be read as a bare batch concatenation via [`Self::decode_frame`].
    fn decode_segment_footer(&self, tail: &[u8]) -> Result<Option<SegmentFooter>> {
        if tail.len() < SEGMENT_TRAILER_LEN {
            return Ok(None);
        }

        let trailer = &tail[tail.len() - SEGMENT_TRAILER_LEN..];
        let magic = u32::from_be_bytes(trailer[14..18].try_into()?);
        if magic != SEGMENT_MAGIC {
            return Ok(None);
        }

        let footer_len = u64::from_be_bytes(trailer[0..8].try_into()?) as usize;
        let entry_count = u32::from_be_bytes(trailer[8..12].try_into()?) as usize;
        let version = u16::from_be_bytes(trailer[12..14].try_into()?);
        if version != SEGMENT_FORMAT_VERSION {
            return Err(Error::Message(format!(
                "unsupported segment format version {version}"
            )));
        }

        let footer_end = tail.len() - SEGMENT_TRAILER_LEN;
        let footer_start = footer_end
            .checked_sub(footer_len)
            .ok_or_else(|| Error::Message(String::from("segment footer length exceeds tail")))?;

        Self::decode_footer(&tail[footer_start..footer_end], entry_count).map(Some)
    }

    async fn get<V>(&self, location: &Path) -> Result<(V, Version)>
    where
        V: DeserializeOwned,
    {
        let get_result = self.object_store.get(location).await?;
        let meta = get_result.meta.clone();

        let payload = get_result
            .bytes()
            .await
            .map_err(Into::into)
            .and_then(|encoded| serde_json::from_reader(&encoded[..]).map_err(Error::from))?;

        Ok((payload, meta.into()))
    }

    async fn put<V>(
        &self,
        location: &Path,
        value: V,
        attributes: Attributes,
        update_version: Option<UpdateVersion>,
    ) -> Result<PutResult, UpdateError<V>>
    where
        V: PartialEq + Serialize + DeserializeOwned + Debug,
    {
        debug!(%location, ?attributes, ?update_version, ?value);

        let options = PutOptions {
            mode: update_version.map_or(PutMode::Create, PutMode::Update),
            attributes,
            ..Default::default()
        };

        let payload = serde_json::to_vec(&value)
            .map(Bytes::from)
            .map(PutPayload::from)?;

        match self
            .object_store
            .put_opts(location, payload, options)
            .await
            .inspect_err(|error| debug!(%location, ?error))
        {
            Ok(put_result) => Ok(put_result),

            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => {
                let (current, version) = self
                    .get(location)
                    .await
                    .inspect_err(|error| error!(%location, ?error))?;

                debug!(%location, ?value, ?current);

                Err(UpdateError::Outdated {
                    current: Box::new(current),
                    version,
                })
            }

            Err(otherwise) => Err(otherwise.into()),
        }
    }

    fn txn_offset_commit_response_error(
        offsets: &TxnOffsetCommitRequest,
        error_code: ErrorCode,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let mut responses = vec![];

        for topic in &offsets.topics {
            let mut partition_responses = vec![];

            if let Some(partitions) = topic.partitions.as_deref() {
                for partition in partitions {
                    partition_responses.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(error_code.into()),
                    );
                }
            }

            responses.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name.to_string())
                    .partitions(Some(partition_responses)),
            );
        }

        Ok(responses)
    }
}

#[async_trait]
impl Storage for DynoStore {
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        // Idempotent, concurrency-safe backfill of any legacy monolithic topic
        // metadata into per-topic objects on first boot of this version.
        self.migrate_legacy_topic_metadata().await?;

        // Warm the in-memory topic index now, while the listener is still
        // closed, so the first client's list-all metadata request hits a hot
        // cache instead of paying the cold build (LIST + GET-per-topic).
        self.warm_topic_index().await;

        Ok(())
    }

    fn auto_create_topic_config(&self) -> AutoTopicCreate {
        self.auto_create
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let _ = resource;

        match ConfigResource::from(resource.resource_type) {
            ConfigResource::Group => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::ClientMetric => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::BrokerLogger => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Broker => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Topic => {
                let handle = self.topic_meta(resource.resource_name.as_str())?;

                // Only mutate an existing topic: `with_mut` would otherwise
                // create one from `Default` (Kafka alter on an unknown topic is
                // a no-op here, matching the previous behaviour).
                if handle.get_opt(&self.object_store).await?.is_some() {
                    handle
                        .with_mut(&self.object_store, |topic_metadata| {
                            topic_metadata
                                .alter_configs(resource.configs.as_deref().unwrap_or_default())
                        })
                        .await?;
                }

                Ok(AlterConfigsResourceResponse::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some("".into()))
                    .resource_type(resource.resource_type)
                    .resource_name(resource.resource_name))
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }

    #[instrument(skip_all, fields(topic = %topic.name))]
    async fn create_topic(&self, topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        let id = Uuid::now_v7();
        debug!(%id);

        // Create-only PUT of the per-topic object. A losing creator (another
        // replica racing the same name) gets `false` here and returns
        // `TopicAlreadyExists` without overwriting the winner's object.
        let created = self
            .topic_meta(topic.name.as_str())?
            .create(
                &self.object_store,
                TopicMetadata {
                    id,
                    topic: topic.clone(),
                },
            )
            .await?;

        if !created {
            return Err(Error::Api(ErrorCode::TopicAlreadyExists));
        }

        // id -> name pointer so a lookup by topic-id can resolve to the named
        // object. Written after the topic object so only the winning creator
        // (whose id is the topic's id) ever writes it.
        _ = self
            .object_store
            .put_opts(
                &self.topic_id_path(&id),
                serde_json::to_vec(&TopicIdRef {
                    name: topic.name.clone(),
                })
                .map(Bytes::from)
                .map(PutPayload::from)?,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;

        for partition in 0..topic.num_partitions {
            let topition = Topition::new(topic.name.as_str(), partition);

            let watermark = self.watermarks.lock().map(|mut locked| {
                locked
                    .entry(topition.to_owned())
                    .or_insert(OptiCon::<Watermark>::new(self.cluster.as_str(), &topition))
                    .to_owned()
            })?;

            watermark
                .with_mut(&self.object_store, |watermark| {
                    _ = watermark.high.take();
                    _ = watermark.low.take();

                    Ok(())
                })
                .await?;

            // Drop any stale next-offset hint (e.g. a topic of the same
            // name was previously deleted) so the fresh, empty partition
            // re-derives offset 0 from listing.
            _ = self
                .next_offsets
                .lock()
                .map(|mut locked| locked.remove(&topition))?;
        }

        // Reflect the new topic in this replica's list-all view at once.
        self.invalidate_topic_index();

        Ok(id)
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(cluster = self.cluster, ?topics);

        let mut responses = vec![];

        for topic in topics {
            let mut partition_responses = vec![];

            if let Some(ref partitions) = topic.partitions {
                for partition in partitions {
                    let topition = Topition::new(topic.name.clone(), partition.partition_index);

                    let partition_result = match self
                        .delete_records_before(&topition, partition.offset)
                        .await
                    {
                        Ok(low_watermark) => DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(low_watermark)
                            .error_code(ErrorCode::None.into()),

                        Err(err) => {
                            error!(?err, ?topition);

                            DeleteRecordsPartitionResult::default()
                                .partition_index(partition.partition_index)
                                .low_watermark(0)
                                .error_code(ErrorCode::UnknownServerError.into())
                        }
                    };

                    partition_responses.push(partition_result);
                }
            }

            responses.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }

        Ok(responses)
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        if let Some(metadata) = self.topic_metadata(topic).await? {
            // Remove the per-topic metadata object and its id -> name pointer.
            self.topic_meta(metadata.topic.name.as_str())?
                .remove(&self.object_store)
                .await?;

            self.invalidate_topic_id(&metadata.id);
            self.invalidate_topic_index();

            match self
                .object_store
                .delete(&self.topic_id_path(&metadata.id))
                .await
            {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(otherwise) => return Err(otherwise.into()),
            }

            let prefix = Path::from(format!(
                "clusters/{}/topics/{}/",
                self.cluster, metadata.topic.name,
            ));

            let locations = self
                .object_store
                .list(Some(&prefix))
                .map_ok(|m| m.location)
                .boxed();

            _ = self
                .object_store
                .delete_stream(locations)
                .try_collect::<Vec<Path>>()
                .await?;

            let prefix = Path::from(format!("clusters/{}/groups/consumers/", self.cluster));

            let topic_name = metadata.topic.name.clone();
            let prefix_clone = prefix.clone();
            let locations = self
                .object_store
                .list(Some(&prefix))
                .filter_map(move |m| {
                    let prefix = prefix_clone.clone();
                    let topic_name = topic_name.clone();
                    async move {
                        m.map_or(None, |m| {
                            debug!(?m.location);

                            m.location.prefix_match(&prefix).and_then(|mut i| {
                                // skip over the consumer group name
                                _ = i.next();

                                let sub = Path::from_iter(i);
                                debug!(?sub);

                                if sub.prefix_matches(&Path::from(format!(
                                    "offsets/{}/partitions/",
                                    topic_name
                                ))) {
                                    Some(Ok(m.location.clone()))
                                } else {
                                    None
                                }
                            })
                        })
                    }
                })
                .boxed();

            _ = self
                .object_store
                .delete_stream(locations)
                .try_collect::<Vec<Path>>()
                .await?;
            Ok(ErrorCode::None)
        } else {
            Ok(ErrorCode::UnknownTopicOrPartition)
        }
    }

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

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let config = self
            .describe_config(topition.topic(), ConfigResource::Topic, None)
            .await
            .inspect_err(|err| debug!(?err))?;

        if self.lake.is_some()
            && config
                .configs
                .as_ref()
                .map(|configs| {
                    configs
                        .iter()
                        .inspect(|config| debug!(?config))
                        .any(|config| {
                            config.name.as_str() == "tansu.lake.sink"
                                && config
                                    .value
                                    .as_deref()
                                    .and_then(|value| bool::from_str(value).ok())
                                    .unwrap_or(false)
                        })
                })
                .unwrap_or(false)
        {
            // Get watermark to calculate proper offset for lake sink
            let watermark = self.watermarks.lock().map(|mut locked| {
                locked
                    .entry(topition.to_owned())
                    .or_insert_with(|| OptiCon::<Watermark>::new(self.cluster.as_str(), topition))
                    .to_owned()
            })?;

            let offset = watermark
                .with_mut(&self.object_store, |watermark| {
                    debug!(?watermark);

                    let offset = watermark.high.unwrap_or_default();
                    watermark.high = watermark.high.map_or_else(
                        || Some(deflated.last_offset_delta as i64 + 1i64),
                        |high| Some(high + deflated.last_offset_delta as i64 + 1i64),
                    );

                    debug!(?watermark);

                    Ok(offset)
                })
                .await
                .inspect(|offset| debug!(offset, transaction_id, ?topition))
                .inspect_err(|err| error!(?err, transaction_id, ?topition))?;

            // Advance the in-memory hint so a read on THIS broker reflects the
            // lake-sink write immediately (#72). The lake-sink branch is the only
            // producer path that writes no batch object, so without this the
            // fresh-hint fast path would serve a stale high watermark until the
            // hint TTL expired — a read-your-writes regression the pre-#72 per-poll
            // GET masked.
            self.set_high(topition, offset + deflated.last_offset_delta as i64 + 1)?;

            if let Some(ref registry) = self.schemas {
                let batch_attribute = BatchAttribute::try_from(deflated.attributes)
                    .inspect(|batch_attribute| debug!(?batch_attribute))
                    .inspect_err(|err| debug!(?err))?;

                if !batch_attribute.control {
                    let inflated = inflated::Batch::try_from(&deflated)
                        .inspect(|inflated| debug!(?inflated))
                        .inspect_err(|err| debug!(?err))?;

                    registry
                        .validate(topition.topic(), &inflated)
                        .await
                        .inspect(|validation| debug!(?validation))
                        .inspect_err(|err| debug!(?err))?;

                    if let Some(ref lake) = self.lake {
                        lake.store(
                            topition.topic(),
                            topition.partition(),
                            offset,
                            &inflated,
                            config,
                        )
                        .await
                        .inspect(|store| debug!(?store))
                        .inspect_err(|err| debug!(?err))?;
                    }
                }
            }

            Ok(offset)
        } else {
            if deflated.is_idempotent() {
                // Validate (and advance) the idempotent sequence on the producer's
                // own `producers/{id}.json` object rather than the cluster-global
                // `meta` object, so distinct producers no longer contend on one
                // hot object on GCS (#13). The advance is applied in memory (the
                // fast-path authority) and the object is checkpointed lazily, so
                // an idempotent batch no longer costs a second S3 PUT (#48); the
                // exact OutOfOrder/Duplicate/Fenced semantics are unchanged.
                self.advance_idempotent_sequence(
                    deflated.producer_id,
                    deflated.producer_epoch,
                    topition,
                    deflated.base_sequence,
                    deflated.last_offset_delta,
                )
                .await
                .inspect(|outcome| debug!(transaction_id, ?topition, ?outcome))
                // `DuplicateSequenceNumber` / `OutOfOrderSequenceNumber` are the
                // expected idempotent-producer outcomes for a retried batch, not
                // broker failures — log them at debug like the CAS itself, and
                // reserve error! for genuinely unexpected Api errors (#37).
                .inspect_err(|err| {
                    if err.is_expected_idempotent_outcome() {
                        debug!(?err, transaction_id, ?topition);
                    } else {
                        error!(?err, transaction_id, ?topition);
                    }
                })?;
            }

            if let Some(ref registry) = self.schemas {
                let batch_attribute = BatchAttribute::try_from(deflated.attributes)
                    .inspect_err(|err| debug!(?err))?;

                if !batch_attribute.control {
                    let inflated =
                        inflated::Batch::try_from(&deflated).inspect_err(|err| debug!(?err))?;

                    registry
                        .validate(topition.topic(), &inflated)
                        .await
                        .inspect_err(|err| debug!(?err))?;
                }
            }

            let attributes =
                BatchAttribute::try_from(deflated.attributes).inspect_err(|err| debug!(?err))?;

            // Server-side coalescing: buffer the batch and flush a run as one
            // object per linger window — per connector prefix into a shared
            // segment (#57) when prefix mode is on, else per partition (#50).
            // Transactional, control and compacted batches bypass both paths
            // (offset/txn-marker/compaction semantics must stay one batch per
            // object, and a compacted topic can't share a whole-segment-expiry
            // segment, #61); the idempotent sequence and schema were already
            // validated above, so a duplicate is rejected before it is buffered.
            let coalesce_eligible = transaction_id.is_none()
                && !attributes.transaction
                && !attributes.control
                && !config
                    .configs
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .any(|config| {
                        config.name == "cleanup.policy"
                            && config
                                .value
                                .as_deref()
                                .is_some_and(|value| value.contains("compact"))
                    });

            if coalesce_eligible {
                if self.prefix_coalesce {
                    // Backfill high-throughput bypass (#62): a snapshot streams a
                    // whole table to one partition in large batches that are
                    // already S3-efficient on their own (~thousands of events per
                    // PUT) and want the parallelism single-writer segments cap. A
                    // large batch takes the legacy per-(topic,partition) create
                    // path below (lock-free/parallel, no lease) — BUT only while
                    // the sub-stream has no segment yet, so legacy offsets stay
                    // strictly below segment offsets (the #58 seam the hybrid read
                    // path depends on). A large batch arriving *after* segmentation
                    // (e.g. a mid-life add-table snapshot or lag catch-up) must
                    // coalesce, or it would write legacy objects above segments and
                    // break fetch ordering / cold recovery (#60 review fix).
                    let records = deflated.last_offset_delta as i64 + 1;
                    let bypass_backfill = records >= Self::PREFIX_BACKFILL_MIN_RECORDS
                        && self.segment_region_start(topition).await?.is_none();
                    if !bypass_backfill {
                        return self.enqueue_prefix_coalesced(topition, deflated).await;
                    }
                } else if self.produce_coalesce {
                    return self.enqueue_coalesced(topition, deflated).await;
                }
            }

            // Assign the offset by *creating* the immutable batch object (its
            // name encodes the base offset), rather than by updating a hot
            // per-partition `watermark` object on every batch. The create is the
            // authority; the watermark stays a write-behind cache only. This is
            // the #13 fix: the produce hot path no longer hammers a single
            // object capped at ~1 write/s on GCS.
            let payload = self
                .encode(deflated.clone())
                .inspect_err(|err| debug!(?err))?;

            let offset = self
                .assign_and_create(topition, deflated.last_offset_delta as i64 + 1, payload)
                .await
                .inspect(|offset| debug!(offset, transaction_id, ?topition))
                .inspect_err(|err| error!(?err, transaction_id, ?topition))?;

            if !attributes.control
                && let Some(ref lake) = self.lake
            {
                let inflated =
                    inflated::Batch::try_from(&deflated).inspect_err(|err| debug!(?err))?;

                lake.store(
                    topition.topic(),
                    topition.partition(),
                    offset,
                    &inflated,
                    config,
                )
                .await
                .inspect(|store| debug!(?store))
                .inspect_err(|err| debug!(?err))?;
            }

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        if let Some(transaction) = meta.transactions.get_mut(transaction_id) {
                            debug!(?transaction);

                            if let Some(txn_detail) =
                                transaction.epochs.get_mut(&deflated.producer_epoch)
                            {
                                debug!(?txn_detail);

                                let offset_end = offset + deflated.last_offset_delta as i64;

                                _ = txn_detail
                                    .produces
                                    .entry(topition.topic.clone())
                                    .or_default()
                                    .entry(topition.partition)
                                    .and_modify(|entry| {
                                        let range = entry.get_or_insert(TxnProduceOffset {
                                            offset_start: offset,
                                            offset_end,
                                        });

                                        if offset_end > range.offset_end {
                                            range.offset_end = offset_end;
                                        }
                                    })
                                    .or_insert(Some(TxnProduceOffset {
                                        offset_start: offset,
                                        offset_end,
                                    }));
                            }
                        }

                        Ok(())
                    })
                    .await
                    .inspect(|outcome| debug!(?outcome, transaction_id, ?topition))
                    .inspect_err(|err| error!(?err, transaction_id, ?topition))?;
            }

            Ok(offset)
        }
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        _min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let started_at = SystemTime::now();

        // Read-committed needs the last-stable offset, which `offset_stage`
        // derives from the cluster `meta.json` transactions. Read-uncommitted
        // (the common case) only needs the high watermark, derived purely from
        // the immutable batch objects — so go straight to `high_watermark` and
        // keep the hot fetch path off the meta object entirely.
        let high_watermark = if isolation_level == IsolationLevel::ReadCommitted {
            self.offset_stage(topition).await?.last_stable
        } else {
            self.high_watermark(topition).await?
        };

        debug!(high_watermark);

        let mut batches = vec![];

        if offset < high_watermark {
            if self.prefix_coalesce {
                // Prefix-coalesced (#60): records live in shared segments, located
                // by footer index and read with a ranged GET of exactly the
                // topition's byte span — no cross-topic download. A *hybrid* topic
                // (flipped to segment mode mid-life) also has legacy `records/`
                // objects for `[0, C)`; serve those first, then segments from `C`.
                // Legacy offsets are all below segment offsets (the #58 seam), so
                // legacy-then-segment preserves order and a single fetch with
                // budget to spare stitches across the seam.
                let mut from = offset;
                let hybrid = self.has_legacy_records(topition).await?;

                if hybrid {
                    batches = self
                        .fetch_legacy_records(
                            topition,
                            offset,
                            max_bytes,
                            high_watermark,
                            started_at,
                            max_wait,
                        )
                        .await?;
                    from = batches
                        .last()
                        .map(|batch| batch.base_offset + batch.last_offset_delta as i64 + 1)
                        .unwrap_or(offset);
                }

                // Enter the segment region only once the legacy region is
                // consumed up to the seam C (the lowest segment base). If the
                // legacy read was byte-budget-limited and stopped below C, do NOT
                // jump into segments — that would skip the un-served legacy range
                // [from, C) (#60 review fix). The consumer re-fetches from `from`
                // and continues. Non-hybrid topics have no such gap.
                let seam = self.segment_region_start(topition).await?;
                let blocked_by_legacy = hybrid && seam.is_some_and(|c| from < c);

                let consumed: u64 = batches
                    .iter()
                    .map(|batch| batch.batch_length.max(0) as u64)
                    .sum();
                let remaining = (max_bytes as u64).saturating_sub(consumed);

                if !blocked_by_legacy && from < high_watermark && remaining > 0 {
                    let segment = self
                        .fetch_prefix_coalesced(
                            topition,
                            from,
                            remaining.min(u32::MAX as u64) as u32,
                            high_watermark,
                            started_at,
                            max_wait,
                        )
                        .await?;
                    batches.extend(segment);
                }
            } else {
                batches = self
                    .fetch_legacy_records(
                        topition,
                        offset,
                        max_bytes,
                        high_watermark,
                        started_at,
                        max_wait,
                    )
                    .await?;
            }
        }

        Ok(batches)
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let stable = self
            .meta
            .with(&self.object_store, |meta| {
                Ok(meta
                    .transactions
                    .values()
                    .flat_map(|txn| {
                        debug!(?txn);

                        txn.epochs
                            .values()
                            .filter(|detail| {
                                detail.state.is_some_and(|state| {
                                    state != TxnState::Committed && state != TxnState::Aborted
                                })
                            })
                            .map(BTreeMap::<Topition, Offset>::from)
                            .collect::<Vec<_>>()
                    })
                    .reduce(|mut acc, e| {
                        debug!(?acc, ?e);

                        for (topition, offset_start) in e.iter() {
                            _ = acc
                                .entry(topition.to_owned())
                                .and_modify(|existing_offset_start| {
                                    if *existing_offset_start > *offset_start {
                                        *existing_offset_start = *offset_start
                                    }
                                })
                                .or_insert(*offset_start);
                        }

                        acc
                    })
                    .unwrap_or(BTreeMap::new()))
            })
            .await?;

        debug!(?stable);

        // The high watermark is derived from the immutable batch objects (the
        // authority), not from the mutable `watermark` object — so it is correct
        // across replicas even though offset assignment no longer CASes the
        // watermark on every produce (#13). The `watermark` object is consulted
        // only for the log start offset (`low`), advanced on the cold
        // maintain/expire path (and for lake-sink topics' high, folded into
        // `high_watermark`).
        let high_watermark = self.high_watermark(topition).await?;

        let watermark = self.watermarks.lock().map(|mut locked| {
            locked
                .entry(topition.to_owned())
                .or_insert(OptiCon::<Watermark>::new(self.cluster.as_str(), topition))
                .to_owned()
        })?;

        watermark
            .with(&self.object_store, |watermark| {
                debug!(?watermark);
                let log_start = watermark.low.unwrap_or(0);
                let last_stable = stable.get(topition).copied().unwrap_or(high_watermark);

                Ok(OffsetStage {
                    last_stable,
                    high_watermark,
                    log_start,
                })
            })
            .await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let stable = if isolation_level == IsolationLevel::ReadCommitted {
            self.meta
                .with(&self.object_store, |meta| {
                    Ok(meta
                        .transactions
                        .values()
                        .flat_map(|txn| {
                            txn.epochs
                                .values()
                                .filter(|detail| {
                                    detail.state.is_some_and(|state| {
                                        state != TxnState::Committed && state != TxnState::Aborted
                                    })
                                })
                                .map(BTreeMap::<Topition, Offset>::from)
                                .collect::<Vec<_>>()
                        })
                        .reduce(|mut acc, e| {
                            debug!(?acc, ?e);
                            for (topition, offset_start) in e.iter() {
                                _ = acc
                                    .entry(topition.to_owned())
                                    .and_modify(|existing_offset_start| {
                                        if *existing_offset_start > *offset_start {
                                            *existing_offset_start = *offset_start
                                        }
                                    })
                                    .or_insert(*offset_start);
                            }

                            acc
                        })
                        .unwrap_or(BTreeMap::new()))
                })
                .await?
        } else {
            BTreeMap::new()
        };

        let mut responses = vec![];

        for (topition, offset_request) in offsets {
            // LATEST (outside an open read-committed transaction) is the log end
            // offset == high watermark, derived from the immutable batch objects
            // (max base offset + that batch's record count). The previous
            // `last_modified` ordering was wrong under inter-replica clock skew
            // and `max_base + 1` ignored multi-record batches; `high_watermark`
            // is correct on both counts. `None` (→ -1 on the wire) only for an
            // empty log.
            if *offset_request == ListOffset::Latest && !stable.contains_key(topition) {
                let high = self.high_watermark(topition).await?;

                // Tail timestamp. For a PURE-segment topic (#73) read it from the
                // footer index (the newest segment's max record timestamp) rather
                // than a per-topic `records/` LIST. This is the segment's
                // record-time (closer to the SQL backends' record `timestamp` than
                // the legacy object mtime), but the two sources differ, so the
                // fallback is used whenever a segment isn't the authoritative tail:
                // `coalesced_latest_timestamp` returns `None` for a non-coalesced
                // or hybrid topic (a legacy object may sit above the segments), and
                // we then take the legacy tail's mtime as before.
                let timestamp = match if self.prefix_coalesce {
                    self.coalesced_latest_timestamp(topition).await?
                } else {
                    None
                } {
                    Some(timestamp) => Some(timestamp),
                    None => self
                        .list_batch_offsets(topition)
                        .await?
                        .last_key_value()
                        .map(|(_, meta)| meta.last_modified.into()),
                };

                responses.push((
                    topition.to_owned(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(high),
                        timestamp,
                    },
                ));

                continue;
            }

            // Prefix-coalesced (#60): EARLIEST is the oldest segment's base
            // offset for this sub-stream, read from the footer index — no
            // `records/` listing (there is none). LATEST already went through
            // `high_watermark` above (footer-aware).
            if self.prefix_coalesce && *offset_request == ListOffset::Earliest {
                let earliest = self.coalesced_earliest_offset(topition).await?;

                responses.push((
                    topition.to_owned(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(earliest),
                        timestamp: None,
                    },
                ));

                continue;
            }

            let location = Path::from(format!(
                "clusters/{}/topics/{}/partitions/{:0>10}/records",
                self.cluster, topition.topic, topition.partition,
            ));

            let mut list_stream = self.object_store.list(Some(&location));

            let mut candidate: Option<ObjectMeta> = None;

            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
            {
                if let Some(last) = stable.get(topition)
                    && offset_request == &ListOffset::Latest
                {
                    let Some(found_offset) = candidate
                        .as_ref()
                        .and_then(|found| found.location.parts().next_back())
                        .and_then(|offset| i64::from_str(&offset.as_ref()[0..20]).ok())
                    else {
                        continue;
                    };

                    let Some(meta_offset) = meta
                        .location
                        .parts()
                        .next_back()
                        .and_then(|offset| i64::from_str(&offset.as_ref()[0..20]).ok())
                    else {
                        continue;
                    };

                    if meta_offset >= *last && found_offset > meta_offset {
                        _ = candidate.replace(meta);
                    }
                } else {
                    // Selection orders on the immutable base offset encoded in
                    // the object key, never on `last_modified`. Compaction
                    // rewrites surviving batches in place with
                    // `PutMode::Overwrite` (`compact_partition`), which bumps
                    // `last_modified`; ordering on it inverted the mtime↔offset
                    // relation and made EARLIEST/TIMESTAMP return an offset past
                    // retained data, so consumers silently skipped it (see #26).
                    // LATEST was already moved off mtime onto `high_watermark`.
                    let Some(meta_offset) = meta
                        .location
                        .parts()
                        .next_back()
                        .and_then(|offset| i64::from_str(&offset.as_ref()[0..20]).ok())
                    else {
                        continue;
                    };

                    let found_offset = candidate.as_ref().and_then(|found| {
                        found
                            .location
                            .parts()
                            .next_back()
                            .and_then(|offset| i64::from_str(&offset.as_ref()[0..20]).ok())
                    });

                    match offset_request {
                        // EARLIEST: the smallest base offset present == the log
                        // start (matches the SQL backends' `min(offset_id)`).
                        ListOffset::Earliest
                            if found_offset.is_none_or(|found| meta_offset < found) =>
                        {
                            _ = candidate.replace(meta);
                        }

                        ListOffset::Latest
                            if found_offset.is_none_or(|found| meta_offset > found) =>
                        {
                            _ = candidate.replace(meta);
                        }

                        // TIMESTAMP: earliest offset whose batch is not older than
                        // the target. The `last_modified` predicate is the batch's
                        // write time (best effort — a fully correct answer needs
                        // per-record timestamps), but the candidate is chosen by
                        // smallest base offset so the result stays monotonic in
                        // offset rather than in mtime.
                        ListOffset::Timestamp(system_time)
                            if SystemTime::from(meta.last_modified) > *system_time
                                && found_offset.is_none_or(|found| meta_offset < found) =>
                        {
                            _ = candidate.replace(meta);
                        }
                        _ => continue,
                    }
                }
            }

            debug!(?candidate);

            if let Some(ref found) = candidate {
                let Some(offset) = found.location.parts().next_back() else {
                    continue;
                };

                let offset = i64::from_str(&offset.as_ref()[0..20])?;
                debug!(offset);

                responses.push((
                    topition.to_owned(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(match offset_request {
                            ListOffset::Latest => offset + 1,
                            _ => offset,
                        }),
                        timestamp: Some(found.last_modified.into()),
                    },
                ))
            } else {
                responses.push((
                    topition.to_owned(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(0),
                        ..Default::default()
                    },
                ))
            }
        }

        Ok(responses)
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        _retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let mut responses = vec![];

        for (topition, offset_commit) in offsets {
            if self
                .topic_metadata(&TopicId::from(topition))
                .await?
                .is_some()
            {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}/offsets/{}/partitions/{:0>10}.json",
                    self.cluster, group_id, topition.topic, topition.partition,
                ));

                let payload = serde_json::to_vec(&offset_commit)
                    .map(Bytes::from)
                    .map(PutPayload::from)?;

                let options = PutOptions {
                    mode: PutMode::Overwrite,
                    attributes: json_content_type(),
                    ..Default::default()
                };

                let error_code = self
                    .object_store
                    .put_opts(&location, payload, options)
                    .await
                    .inspect_err(|err| error!(?err))
                    .inspect(|outcome| debug!(?outcome))
                    .map_or(ErrorCode::UnknownServerError, |_| ErrorCode::None);

                responses.push((topition.to_owned(), error_code));
            } else {
                responses.push((topition.to_owned(), ErrorCode::UnknownTopicOrPartition));
            }
        }

        Ok(responses)
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let mut topitions = vec![];

        {
            let location = Path::from(format!(
                "clusters/{}/groups/consumers/{}/offsets/",
                self.cluster, group_id,
            ));

            let mut list_stream = self.object_store.list(Some(&location));

            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error))
                .map_err(|_| Error::Api(ErrorCode::UnknownServerError))?
            {
                debug!(?meta);
                let Some(topic): Option<String> = meta
                    .location
                    .parts()
                    .nth(6)
                    .inspect(|topic| debug!(?topic))
                    .map(|topic| topic.as_ref().into())
                else {
                    continue;
                };

                let Some(partition) = meta
                    .location
                    .parts()
                    .nth(8)
                    .inspect(|partition| debug!(?partition))
                    .map(|partition| i32::from_str(&partition.as_ref()[0..10]))
                    .transpose()?
                else {
                    continue;
                };

                debug!(topic, partition);

                topitions.push(Topition::new(topic, partition));
            }
        }

        self.offset_fetch(Some(group_id), topitions.as_ref(), Some(false))
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, i64>> {
        let mut responses = BTreeMap::new();

        if let Some(group_id) = group_id {
            for topition in topics {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}/offsets/{}/partitions/{:0>10}.json",
                    self.cluster, group_id, topition.topic, topition.partition,
                ));

                let offset = match self.object_store.get(&location).await {
                    Ok(get_result) => get_result
                        .bytes()
                        .await
                        .map_err(Error::from)
                        .and_then(|encoded| {
                            serde_json::from_slice::<OffsetCommitRequest>(&encoded[..])
                                .map_err(Error::from)
                        })
                        .map(|commit| commit.offset)
                        .inspect_err(|error| error!(?error, ?group_id, ?topition))
                        .map_err(|_| Error::Api(ErrorCode::UnknownServerError)),

                    Err(object_store::Error::NotFound { .. }) => Ok(-1),

                    Err(error) => {
                        error!(?error, ?group_id, ?topition);
                        Err(Error::Api(ErrorCode::UnknownServerError))
                    }
                }?;

                _ = responses.insert(topition.to_owned(), offset);
            }
        }

        Ok(responses)
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let brokers = vec![
            MetadataResponseBroker::default()
                .node_id(self.node)
                .host(
                    self.advertised_listener
                        .host_str()
                        .unwrap_or("0.0.0.0")
                        .into(),
                )
                .port(self.advertised_listener.port().unwrap_or(9092).into())
                .rack(None),
        ];

        let responses = match topics {
            Some(topics) if !topics.is_empty() => {
                let mut responses = vec![];

                for topic in topics {
                    let response = match self
                        .topic_metadata(topic)
                        .await
                        .inspect_err(|error| error!(?error))
                    {
                        Ok(Some(topic_metadata)) => {
                            let name = Some(topic_metadata.topic.name.to_owned());
                            let error_code = ErrorCode::None.into();
                            let topic_id = Some(topic_metadata.id.into_bytes());
                            let is_internal = Some(false);
                            let partitions = topic_metadata.topic.num_partitions;
                            let replication_factor = topic_metadata.topic.replication_factor;

                            debug!(
                                ?error_code,
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            let mut rng = rng();
                            let mut broker_ids: Vec<_> =
                                brokers.iter().map(|broker| broker.node_id).collect();
                            broker_ids.shuffle(&mut rng);

                            let mut brokers = broker_ids.into_iter().cycle();

                            let partitions = Some(
                                (0..partitions)
                                    .map(|partition_index| {
                                        let leader_id = brokers.next().expect("cycling");

                                        let replica_nodes = Some(
                                            (0..replication_factor)
                                                .map(|_replica| brokers.next().expect("cycling"))
                                                .collect(),
                                        );
                                        let isr_nodes = replica_nodes.clone();

                                        MetadataResponsePartition::default()
                                            .error_code(error_code)
                                            .partition_index(partition_index)
                                            .leader_id(leader_id)
                                            .leader_epoch(Some(0))
                                            .replica_nodes(replica_nodes)
                                            .isr_nodes(isr_nodes)
                                            .offline_replicas(Some([].into()))
                                    })
                                    .collect(),
                            );

                            MetadataResponseTopic::default()
                                .error_code(error_code)
                                .name(name)
                                .topic_id(topic_id)
                                .is_internal(is_internal)
                                .partitions(partitions)
                                .topic_authorized_operations(Some(-2147483648))
                        }

                        Ok(None) => MetadataResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => None,
                            })
                            .topic_id(Some(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            }))
                            .is_internal(Some(false))
                            .partitions(Some([].into()))
                            .topic_authorized_operations(Some(-2147483648)),

                        Err(_) => MetadataResponseTopic::default()
                            .error_code(ErrorCode::UnknownServerError.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => Some("".into()),
                            })
                            .topic_id(Some(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            }))
                            .is_internal(Some(false))
                            .partitions(Some([].into()))
                            .topic_authorized_operations(Some(-2147483648)),
                    };

                    responses.push(response);
                }

                responses
            }

            _ => {
                let mut responses = vec![];

                for topic_metadata in self.topics_index().await?.iter() {
                    debug!(?topic_metadata);

                    let name = Some(topic_metadata.topic.name.clone());
                    let error_code = ErrorCode::None.into();
                    let topic_id = Some(topic_metadata.id.into_bytes());
                    let is_internal = Some(false);
                    let partitions = topic_metadata.topic.num_partitions;
                    let replication_factor = topic_metadata.topic.replication_factor;

                    debug!(
                        ?error_code,
                        ?topic_id,
                        ?name,
                        ?is_internal,
                        ?partitions,
                        ?replication_factor
                    );

                    let mut rng = rng();
                    let mut broker_ids: Vec<_> =
                        brokers.iter().map(|broker| broker.node_id).collect();
                    broker_ids.shuffle(&mut rng);

                    let mut brokers = broker_ids.into_iter().cycle();

                    let partitions = Some(
                        (0..partitions)
                            .map(|partition_index| {
                                let leader_id = brokers.next().expect("cycling");

                                let replica_nodes = Some(
                                    (0..replication_factor)
                                        .map(|_replica| brokers.next().expect("cycling"))
                                        .collect(),
                                );
                                let isr_nodes = replica_nodes.clone();

                                MetadataResponsePartition::default()
                                    .error_code(error_code)
                                    .partition_index(partition_index)
                                    .leader_id(leader_id)
                                    .leader_epoch(Some(0))
                                    .replica_nodes(replica_nodes)
                                    .isr_nodes(isr_nodes)
                                    .offline_replicas(Some([].into()))
                            })
                            .collect(),
                    );

                    responses.push(
                        MetadataResponseTopic::default()
                            .error_code(error_code)
                            .name(name)
                            .topic_id(topic_id)
                            .is_internal(is_internal)
                            .partitions(partitions)
                            .topic_authorized_operations(Some(-2147483648)),
                    );
                }

                responses
            }
        };

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: responses,
        })
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        match resource {
            ConfigResource::Topic => match self.topic_metadata(&TopicId::Name(name.into())).await {
                Ok(Some(topic_metadata)) => {
                    let error_code = ErrorCode::None;

                    Ok(DescribeConfigsResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(i8::from(resource))
                        .resource_name(name.into())
                        .configs(topic_metadata.topic.configs.map(|configs| {
                            configs
                                .iter()
                                .map(|config| {
                                    DescribeConfigsResourceResult::default()
                                        .name(config.name.clone())
                                        .value(config.value.clone())
                                        .read_only(false)
                                        .is_default(None)
                                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                                        .is_sensitive(false)
                                        .synonyms(Some([].into()))
                                        .config_type(Some(ConfigType::String.into()))
                                        .documentation(Some("".into()))
                                })
                                .collect()
                        })))
                }

                Ok(None) => Ok(DescribeConfigsResult::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some(ErrorCode::None.to_string()))
                    .resource_type(i8::from(resource))
                    .resource_name(name.into())
                    .configs(Some(vec![]))),

                Err(_) => todo!(),
            },

            _ => Ok(DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some(ErrorCode::None.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some(vec![]))),
        }
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let _ = (partition_limit, cursor);

        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            match self
                .topic_metadata(topic)
                .await
                .inspect_err(|error| error!(?error))
            {
                Ok(Some(topic_metadata)) => responses.push(
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::None.into())
                        .name(Some(topic_metadata.topic.name))
                        .topic_id(topic_metadata.id.into_bytes())
                        .is_internal(false)
                        .partitions(Some(
                            (0..topic_metadata.topic.num_partitions)
                                .map(|partition_index| {
                                    DescribeTopicPartitionsResponsePartition::default()
                                        .error_code(ErrorCode::None.into())
                                        .partition_index(partition_index)
                                        .leader_id(self.node)
                                        .leader_epoch(0)
                                        .replica_nodes(Some(vec![
                                            self.node;
                                            topic_metadata.topic.replication_factor
                                                as usize
                                        ]))
                                        .isr_nodes(Some(vec![
                                            self.node;
                                            topic_metadata.topic.replication_factor
                                                as usize
                                        ]))
                                        .eligible_leader_replicas(Some(vec![]))
                                        .last_known_elr(Some(vec![]))
                                        .offline_replicas(Some(vec![]))
                                })
                                .collect(),
                        ))
                        .topic_authorized_operations(-2147483648),
                ),

                Ok(None) => responses.push(
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                        .name(match topic {
                            TopicId::Name(name) => Some(name.into()),
                            TopicId::Id(_) => None,
                        })
                        .topic_id(match topic {
                            TopicId::Name(_) => NULL_TOPIC_ID,
                            TopicId::Id(id) => id.into_bytes(),
                        })
                        .is_internal(false)
                        .partitions(Some([].into()))
                        .topic_authorized_operations(-2147483648),
                ),

                Err(_) => responses.push(
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::UnknownServerError.into())
                        .name(match topic {
                            TopicId::Name(name) => Some(name.into()),
                            TopicId::Id(_) => None,
                        })
                        .topic_id(match topic {
                            TopicId::Name(_) => NULL_TOPIC_ID,
                            TopicId::Id(id) => id.into_bytes(),
                        })
                        .is_internal(false)
                        .partitions(Some([].into()))
                        .topic_authorized_operations(-2147483648),
                ),
            }
        }

        Ok(responses)
    }

    async fn list_groups(&self, _states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        let location = Path::from(format!("clusters/{}/groups/consumers/", self.cluster,));
        let list_result = self
            .object_store
            .list_with_delimiter(Some(&location))
            .await
            .inspect(|list_result| debug!(?list_result))
            .inspect_err(|error| error!(?error, cluster = self.cluster))?;

        let mut listed_groups = vec![];

        for prefix in list_result.common_prefixes {
            if let Some(group_id) = prefix.parts().next_back() {
                listed_groups.push(
                    ListedGroup::default()
                        .group_id(group_id.as_ref().into())
                        .protocol_type("consumer".into())
                        .group_state(Some("Unknown".into()))
                        .group_type(Some("classic".into())),
                );
            }
        }

        Ok(listed_groups)
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}.json",
                    self.cluster, group_id,
                ));

                let had_group_state = self
                    .object_store
                    .delete(&location)
                    .await
                    .inspect(|outcome| debug!(group_id, ?outcome))
                    .inspect_err(|err| error!(group_id, ?err))
                    .is_ok();

                debug!(group_id, had_group_state);

                let prefix = Path::from(format!(
                    "clusters/{}/groups/consumers/{}",
                    self.cluster, group_id,
                ));

                let locations = self
                    .object_store
                    .list(Some(&prefix))
                    .map_ok(|m| m.location)
                    .boxed();

                let deleted_committed_offsets = self
                    .object_store
                    .delete_stream(locations)
                    .try_collect::<Vec<Path>>()
                    .await?;

                debug!(group_id, ?deleted_committed_offsets);

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(
                            if had_group_state || !deleted_committed_offsets.is_empty() {
                                ErrorCode::None
                            } else {
                                ErrorCode::GroupIdNotFound
                            }
                            .into(),
                        ),
                );
            }
        }

        Ok(results)
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let mut results = vec![];
        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}.json",
                    self.cluster, group_id,
                ));

                match self
                    .get::<GroupDetail>(&location)
                    .await
                    .inspect(|o| debug!(?o, group_id))
                    .inspect_err(|err| error!(?err, group_id))
                {
                    Ok((group_detail, _)) => {
                        results.push(NamedGroupDetail::found(group_id.into(), group_detail));
                    }

                    Err(Error::ObjectStore(error)) => match error.as_ref() {
                        object_store::Error::NotFound { .. } => {
                            results.push(NamedGroupDetail::found(
                                group_id.into(),
                                GroupDetail::default(),
                            ));
                        }

                        _otherwise => {
                            results.push(NamedGroupDetail::found(
                                group_id.into(),
                                GroupDetail::default(),
                            ));
                        }
                    },

                    Err(_) => {
                        results.push(NamedGroupDetail::error_code(
                            group_id.into(),
                            ErrorCode::UnknownServerError,
                        ));
                    }
                }
            }
        }

        Ok(results)
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let location = Path::from(format!(
            "clusters/{}/groups/consumers/{}.json",
            self.cluster, group_id,
        ));

        self.put(
            &location,
            detail,
            json_content_type(),
            version.map(Into::into),
        )
        .await
        .map(Into::into)
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        #[derive(Clone, Debug)]
        enum InitProducer {
            Completed(ProducerIdResponse),
            NeedToRollback {
                producer_id: i64,
                producer_epoch: i16,
            },
        }

        if let Some(transaction_id) = transaction_id {
            match self
                .meta
                .with_mut(&self.object_store, |meta| {
                    debug!(?meta);
                    match (producer_id, producer_epoch) {
                        (Some(-1), Some(-1)) => {
                            match meta.transactions.entry(transaction_id.to_string()) {
                                Entry::Vacant(vacant) => {
                                    let id = meta
                                        .producers
                                        .last_key_value()
                                        .map_or(1.into(), |(k, _v)| k + 1);

                                    let mut pd = ProducerDetail::default();
                                    assert_eq!(None, pd.sequences.insert(0, BTreeMap::new()));
                                    assert_eq!(None, meta.producers.insert(id, pd));

                                    let mut epochs = BTreeMap::new();
                                    assert_eq!(
                                        None,
                                        epochs.insert(
                                            0,
                                            TxnDetail {
                                                transaction_timeout_ms,
                                                ..Default::default()
                                            },
                                        )
                                    );

                                    _ = vacant.insert(Txn {
                                        producer: id,
                                        epochs,
                                    });

                                    Ok(InitProducer::Completed(ProducerIdResponse {
                                        id,
                                        epoch: 0,
                                        error: ErrorCode::None,
                                    }))
                                }

                                Entry::Occupied(mut occupied) => {
                                    if let Some((current_epoch, txn_detail)) =
                                        occupied.get().epochs.last_key_value()
                                    {
                                        if txn_detail.state == Some(TxnState::Begin) {
                                            Ok(InitProducer::NeedToRollback {
                                                producer_id: occupied.get().producer,
                                                producer_epoch: *current_epoch,
                                            })
                                        } else {
                                            let id = occupied.get().producer;
                                            let epoch = current_epoch + 1;

                                            _ = meta.producers.entry(id).and_modify(|pd| {
                                                assert_eq!(
                                                    None,
                                                    pd.sequences.insert(epoch, BTreeMap::new())
                                                );
                                            });

                                            assert_eq!(
                                                None,
                                                occupied.get_mut().epochs.insert(
                                                    epoch,
                                                    TxnDetail {
                                                        transaction_timeout_ms,
                                                        ..Default::default()
                                                    }
                                                )
                                            );

                                            Ok(InitProducer::Completed(ProducerIdResponse {
                                                id,
                                                epoch,
                                                error: ErrorCode::None,
                                            }))
                                        }
                                    } else {
                                        todo!()
                                    }
                                }
                            }
                        }

                        (producer, epoch) => {
                            error!(?producer, ?epoch);
                            Ok(InitProducer::Completed(ProducerIdResponse {
                                id: -1,
                                epoch: -1,
                                error: ErrorCode::UnknownServerError,
                            }))
                        }
                    }
                })
                .await?
            {
                InitProducer::Completed(completed) => {
                    self.seed_producer(&completed).await?;
                    Ok(completed)
                }
                InitProducer::NeedToRollback {
                    producer_id: rollback_producer_id,
                    producer_epoch: rollback_producer_epoch,
                } => {
                    let error_code = self
                        .txn_end(
                            transaction_id,
                            rollback_producer_id,
                            rollback_producer_epoch,
                            false,
                        )
                        .await?;

                    debug!(?rollback_producer_id, ?rollback_producer_epoch, ?error_code);

                    if error_code == ErrorCode::None {
                        return self
                            .init_producer(
                                Some(transaction_id),
                                transaction_timeout_ms,
                                producer_id,
                                producer_epoch,
                            )
                            .await;
                    } else {
                        Ok(ProducerIdResponse {
                            id: -1,
                            epoch: -1,
                            error: ErrorCode::UnknownServerError,
                        })
                    }
                }
            }
        } else {
            let response = self
                .meta
                .with_mut(&self.object_store, |meta| {
                    debug!(?meta);
                    match (producer_id, producer_epoch) {
                        (Some(-1), Some(-1)) => {
                            let producer = meta
                                .producers
                                .last_key_value()
                                .map_or(1.into(), |(k, _v)| k + 1);

                            let epoch = 0;
                            let mut pd = ProducerDetail::default();
                            assert_eq!(None, pd.sequences.insert(epoch, BTreeMap::new()));
                            debug!(?producer, ?pd);
                            assert_eq!(None, meta.producers.insert(producer, pd));

                            Ok(ProducerIdResponse {
                                id: producer,
                                epoch,
                                ..Default::default()
                            })
                        }

                        (producer, epoch) => {
                            error!(?producer, ?epoch);
                            Ok(ProducerIdResponse {
                                id: -1,
                                epoch: -1,
                                error: ErrorCode::UnknownServerError,
                            })
                        }
                    }
                })
                .await
                .inspect(|response| debug!(?response))?;

            self.seed_producer(&response).await?;

            Ok(response)
        }
    }

    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                ref topics,
            } => {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        let Some(transaction) = meta.transactions.get_mut(&transaction_id) else {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(
                                                ErrorCode::TransactionalIdNotFound.into(),
                                            ),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        };

                        if transaction.producer != producer_id {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(
                                                ErrorCode::UnknownProducerId.into(),
                                            ),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        }

                        let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(ErrorCode::ProducerFenced.into()),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        };

                        if &producer_epoch != current_epoch.key() {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(ErrorCode::ProducerFenced.into()),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        }

                        let txn_detail = current_epoch.get_mut();

                        let mut results = vec![];

                        for topic in topics {
                            let mut results_by_partition = vec![];

                            for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                _ = txn_detail
                                    .produces
                                    .entry(topic.name.clone())
                                    .or_default()
                                    .entry(*partition_index)
                                    .or_default();

                                results_by_partition.push(
                                    AddPartitionsToTxnPartitionResult::default()
                                        .partition_index(*partition_index)
                                        .partition_error_code(i16::from(ErrorCode::None)),
                                );
                            }

                            results.push(
                                AddPartitionsToTxnTopicResult::default()
                                    .name(topic.name.clone())
                                    .results_by_partition(Some(results_by_partition)),
                            )
                        }

                        txn_detail.started_at = Some(SystemTime::now());
                        txn_detail.state = Some(TxnState::Begin);

                        Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
                    })
                    .await
            }

            TxnAddPartitionsRequest::VersionFourPlus { .. } => {
                todo!()
            }
        }
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.meta
            .with_mut(&self.object_store, |meta| {
                let Some(transaction) = meta.transactions.get_mut(&offsets.transaction_id) else {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::TransactionalIdNotFound,
                    );
                };

                if transaction.producer != offsets.producer_id {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::UnknownProducerId,
                    );
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::ProducerFenced,
                    );
                };

                if &offsets.producer_epoch != current_epoch.key() {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::ProducerFenced,
                    );
                }

                let txn_detail = current_epoch.get_mut();

                let mut responses = vec![];

                for topic in &offsets.topics {
                    let mut partition_responses = vec![];

                    if let Some(partitions) = topic.partitions.as_deref() {
                        for partition in partitions {
                            _ = txn_detail
                                .offsets
                                .entry(offsets.group_id.clone())
                                .or_default()
                                .entry(topic.name.clone())
                                .or_default()
                                .insert(
                                    partition.partition_index,
                                    TxnCommitOffset {
                                        committed_offset: partition.committed_offset,
                                        leader_epoch: partition.committed_leader_epoch,
                                        metadata: partition.committed_metadata.clone(),
                                    },
                                );

                            partition_responses.push(
                                TxnOffsetCommitResponsePartition::default()
                                    .partition_index(partition.partition_index)
                                    .error_code(ErrorCode::None.into()),
                            );
                        }
                    }

                    responses.push(
                        TxnOffsetCommitResponseTopic::default()
                            .name(topic.name.to_string())
                            .partitions(Some(partition_responses)),
                    );
                }

                Ok(responses)
            })
            .await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let produced = self
            .meta
            .with_mut(&self.object_store, |meta| {
                debug!(transactions = ?meta.transactions);

                let Some(transaction) = meta.transactions.get_mut(transaction_id) else {
                    return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
                };

                if transaction.producer != producer_id {
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                };

                if &producer_epoch != current_epoch.key() {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                }

                let txn_detail = current_epoch.get_mut();

                let mut produced = vec![];

                if txn_detail.state == Some(TxnState::Begin) {
                    assert_eq!(
                        Some(TxnState::Begin),
                        txn_detail.state.replace(if committed {
                            TxnState::PrepareCommit
                        } else {
                            TxnState::PrepareAbort
                        })
                    );

                    for (topic, partitions) in &txn_detail.produces {
                        for (partition, offset_range) in partitions {
                            debug!(?topic, partition, ?offset_range);

                            if offset_range.is_some() {
                                produced.push(Topition::new(topic.to_owned(), *partition));
                            }
                        }
                    }
                }

                Ok(produced)
            })
            .await
            .inspect(|produced| debug!(?produced))
            .inspect_err(|err| error!(?err))?;

        for topition in produced {
            debug!(?topition);

            let control_batch: Bytes = if committed {
                ControlBatch::default().commit().try_into()?
            } else {
                ControlBatch::default().abort().try_into()?
            };

            let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

            let batch = inflated::Batch::builder()
                .record(
                    Record::builder()
                        .key(control_batch.into())
                        .value(end_transaction_marker.into()),
                )
                .attributes(
                    BatchAttribute::default()
                        .control(true)
                        .transaction(true)
                        .into(),
                )
                .producer_id(producer_id)
                .producer_epoch(producer_epoch)
                .base_sequence(-1)
                .build()
                .and_then(TryInto::try_into)
                .inspect(|deflated| debug!(?deflated))?;

            _ = self
                .produce(Some(transaction_id), &topition, batch)
                .await
                .inspect(|offset| {
                    debug!(
                        offset,
                        ?topition,
                        producer_id,
                        producer_epoch,
                        transaction_id,
                        committed,
                    )
                })
                .inspect_err(|err| {
                    error!(
                        ?err,
                        ?topition,
                        producer_id,
                        producer_epoch,
                        transaction_id,
                        committed,
                    )
                })?;
        }

        let offsets_to_commit = self
            .meta
            .with_mut(&self.object_store, |meta| {
                debug!(transactions = ?meta.transactions);

                let Some(transaction) = meta.transactions.get_mut(transaction_id) else {
                    return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
                };

                if transaction.producer != producer_id {
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                }

                let Some(current_epoch) = transaction.epochs.last_entry() else {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                };

                if &producer_epoch != current_epoch.key() {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                }

                let mut overlaps =
                    meta.overlapping_transactions(transaction_id, producer_id, producer_epoch)?;
                debug!(?overlaps);

                let mut offsets_to_commit: BTreeMap<
                    Group,
                    BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>,
                > = BTreeMap::new();

                if overlaps.iter().all(|txn_id| txn_id.state.is_prepared()) {
                    let txn_ids = {
                        overlaps.push(TxnId {
                            transaction: transaction_id.into(),
                            producer_id,
                            producer_epoch,
                            state: if committed {
                                TxnState::PrepareCommit
                            } else {
                                TxnState::PrepareAbort
                            },
                        });

                        overlaps
                    };

                    for txn_id in txn_ids {
                        debug!(?txn_id);

                        if let Some(txn) = meta.transactions.get_mut(txn_id.transaction.as_str())
                            && let Some(txn_detail) = txn.epochs.get_mut(&txn_id.producer_epoch)
                        {
                            debug!(?txn_detail);

                            match txn_detail.state {
                                None | Some(TxnState::PrepareCommit) => {
                                    _ = txn_detail.state.replace(TxnState::Committed);
                                }

                                Some(TxnState::PrepareAbort) => {
                                    _ = txn_detail.state.replace(TxnState::Aborted);
                                }

                                otherwise => {
                                    warn!(
                                        transaction = txn_id.transaction,
                                        producer = txn_id.producer_id,
                                        epoch = txn_id.producer_epoch,
                                        ?otherwise,
                                    );

                                    continue;
                                }
                            }

                            if txn_id.state == TxnState::PrepareCommit {
                                for (group, topics) in txn_detail.offsets.iter() {
                                    for (topic, partitions) in topics.iter() {
                                        for (partition, committed_offset) in partitions {
                                            _ = offsets_to_commit
                                                .entry(group.to_owned())
                                                .or_default()
                                                .entry(topic.to_owned())
                                                .or_default()
                                                .insert(*partition, committed_offset.to_owned());
                                        }
                                    }
                                }
                            }

                            txn_detail.produces.clear();
                            txn_detail.offsets.clear();
                            _ = txn_detail.started_at.take();
                        }
                    }
                }

                Ok(offsets_to_commit)
            })
            .await
            .inspect(|outcome| debug!(?outcome))
            .inspect_err(|err| error!(?err))?;

        debug!(?offsets_to_commit);

        for (group, topics) in offsets_to_commit.iter() {
            let mut offsets = vec![];

            for (topic, partitions) in topics.iter() {
                for (partition, txn_co) in partitions {
                    let tp = Topition::new(topic.to_owned(), *partition);
                    let ocr = OffsetCommitRequest {
                        offset: txn_co.committed_offset,
                        leader_epoch: txn_co.leader_epoch,
                        timestamp: None,
                        metadata: txn_co.metadata.clone(),
                    };

                    offsets.push((tp, ocr));
                }
            }

            _ = self.offset_commit(group, None, &offsets[..]).await?;
        }

        Ok(ErrorCode::None)
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        let deleted = self.policy_delete(now).await?;
        let compacted = self.policy_compact().await?;
        // Bound the live segment count per prefix (#66): merge old segments after
        // retention has pruned the expired ones.
        let compacted_segments = self.policy_compact_segments().await?;
        let expired_groups = self.expire_groups(now).await?;
        debug!(deleted, compacted, compacted_segments, expired_groups);

        if let Some(ref lake) = self.lake {
            return lake
                .maintain()
                .await
                .inspect(|maintain| debug!(?maintain))
                .inspect_err(|err| debug!(?err))
                .map_err(Into::into);
        }

        Ok(())
    }

    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        Ok(())
    }

    async fn upsert_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Ok(())
    }

    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Ok(None)
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        // Verify connectivity by listing objects at the root
        let _ = self.object_store.list(Some(&Path::from("/"))).next().await;
        Ok(())
    }
}

/// Backoff (with jitter) before retrying a throttled bulk delete: 0.5s, 1s, 2s,
/// 4s, 8s … capped at 30s, plus up to 50% jitter to desynchronise replicas that
/// were throttled at the same instant.
fn throttle_backoff(attempt: u32) -> Duration {
    let base_ms = 500u64.saturating_mul(1 << attempt.min(6)).min(30_000);
    let jitter = rng().random_range(0..=base_ms / 2);
    Duration::from_millis(base_ms + jitter)
}

/// True if `error` looks like an S3 request-rate throttle on a multi-object
/// delete — either a surfaced `SlowDown`, or the `object_store` deserialisation
/// failure that a `200`-with-`<Error>` throttle body produces (the response is
/// `<Error><Code>SlowDown</Code></Error>` rather than `<DeleteResult>`, so the
/// parser reports `unknown variant `Code``). See [`DynoStore::delete_batches`].
fn is_s3_throttle(error: &object_store::Error) -> bool {
    let mut current: Option<&dyn std::error::Error> = Some(error);

    while let Some(err) = current {
        let text = err.to_string();

        if text.contains("SlowDown")
            || text.contains("unknown variant `Code`")
            || text.contains("invalid DeleteObjects response")
        {
            return true;
        }

        current = err.source();
    }

    false
}

fn object_store_error_name(error: &object_store::Error) -> &'static str {
    match error {
        object_store::Error::Precondition { .. } => "pre_condition",

        object_store::Error::AlreadyExists { .. } => "already_exists",

        object_store::Error::NotModified { .. } => "not_modified",

        object_store::Error::NotFound { .. } => "not_found",

        otherwise => {
            debug!(?otherwise);
            "otherwise"
        }
    }
}

#[derive(Debug, Clone)]
struct Metron<O> {
    request_duration: Histogram<u64>,
    request_error: Counter<u64>,

    cluster: String,
    object_store: O,
}

impl<O> Display for Metron<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metron").finish()
    }
}

impl<O> Metron<O>
where
    O: ObjectStore,
{
    fn new(object_store: O, cluster: &str) -> Self {
        Self {
            cluster: cluster.into(),
            request_duration: METER
                .u64_histogram("tansu_object_store_request_duration")
                .with_unit("ms")
                .with_description("The object store request latencies in milliseconds")
                .build(),
            request_error: METER
                .u64_counter("tansu_object_store_request_error")
                .with_description("The object store request errors")
                .build(),

            object_store,
        }
    }
}

#[async_trait]
impl<O> ObjectStore for Metron<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        debug!(%location, ?opts);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "put_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .put_opts(location, payload, opts.clone())
            .await
            .inspect(|put_result| {
                debug!(%location, etag = ?put_result.e_tag, version = ?put_result.version);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(%location, opts = ?opts, err = ?err);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        debug!(%location, ?opts);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "put_multipart_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .put_multipart_opts(location, opts)
            .await
            .inspect(|_put_result| {
                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    #[instrument(skip_all, fields(%location), ret)]
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        debug!(?options);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "get_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .get_opts(location, options.clone())
            .await
            .inspect(|get_result| {
                debug!(meta = ?get_result.meta);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(?err);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        debug!("delete_stream");

        let cluster = self.cluster.clone();
        let request_duration = self.request_duration.clone();
        let request_error = self.request_error.clone();

        let inner = self.object_store.delete_stream(locations);

        Box::pin(inner.inspect(move |result| {
            let attributes = vec![
                KeyValue::new("method", "delete_stream"),
                KeyValue::new("cluster", cluster.clone()),
            ];

            if let Err(err) = result {
                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.extend(attributes);
                request_error.add(1, &additional[..]);
            } else {
                request_duration.record(0, attributes.as_ref());
            }
        }))
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix);

        self.object_store.list(prefix)
    }

    // Forward `list_with_offset` (S3 `start-after`) so a tail-offset scan reads
    // only the partition tail rather than the default full-`list` downgrade.
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix, ?offset);

        self.object_store.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        debug!(?prefix);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "list_with_delimiter"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        if let Some(prefix) = prefix {
            attributes.push(KeyValue::new("prefix", prefix.to_string()));
        }

        self.object_store
            .list_with_delimiter(prefix)
            .await
            .inspect(|_list_result| {
                debug!(?prefix);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(?prefix, err = ?err);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        debug!(%from, %to, ?opts);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "copy_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .copy_opts(from, to, opts)
            .await
            .inspect(|_| {
                debug!(%from, %to);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(%from, %to, err = ?err);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }
}

#[cfg(test)]
mod throttle_tests {
    use super::{is_s3_throttle, throttle_backoff};

    fn generic_s3(source: &'static str) -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: source.into(),
        }
    }

    #[test]
    fn slowdown_503_body_is_throttle() {
        // A genuine 503 whose body is surfaced after retries are exhausted.
        assert!(is_s3_throttle(&generic_s3(
            "Status { status: 503, body: \"<Error><Code>SlowDown</Code></Error>\" }"
        )));
    }

    #[test]
    fn parse_error_from_200_throttle_body_is_throttle() {
        // S3 returned 200 with <Error><Code>SlowDown</Code></Error>; object_store
        // tried to parse it as <DeleteResult> and tripped on the <Code> element.
        assert!(is_s3_throttle(&generic_s3(
            "Got invalid DeleteObjects response: unknown variant `Code`, expected `Deleted` or `Error`"
        )));
    }

    #[test]
    fn unrelated_errors_are_not_throttle() {
        assert!(!is_s3_throttle(&object_store::Error::NotFound {
            path: "x".into(),
            source: "missing".into(),
        }));
        assert!(!is_s3_throttle(&generic_s3("connection reset by peer")));
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        // attempt 0: base 500ms + up to 50% jitter => [500, 750]ms
        let first = throttle_backoff(0).as_millis();
        assert!((500..=750).contains(&first), "{first}");

        // large attempt: base capped at 30s + up to 50% jitter => [30s, 45s]
        let capped = throttle_backoff(20).as_millis();
        assert!((30_000..=45_000).contains(&capped), "{capped}");
    }
}
