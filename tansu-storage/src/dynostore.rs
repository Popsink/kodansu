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
    cmp::Reverse,
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

/// Per-topition memo of legacy-`records/` presence: `(present, checked_at, ttl)`
/// with a per-entry TTL (#110). Aliased to keep the field type readable.
type LegacyRecordsMemo = BTreeMap<Topition, (bool, SystemTime, Duration)>;

/// One cached segment's expiry-decision inputs (#61/#176): `(seq, age_ms,
/// [(topic, partition, end_offset)])`, snapshotted under the `prefix_index`
/// lock so the truncation floors can be evaluated outside it.
type SegmentExpirySnapshot = (u64, i64, Vec<(String, i32, i64)>);

#[derive(Clone, Debug)]
pub struct DynoStore {
    cluster: String,
    node: i32,
    advertised_listener: Url,
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

    /// Per-partition cache of the persisted `watermark.high` floor for
    /// prefix-coalesced sub-streams, paired with the certified seq floor under
    /// which it was read: `topition -> (watermark.high, certified floor)`. An
    /// entry is valid only while [`Self::certified_seq_floor`] still returns
    /// the same floor — for coalesced sub-streams `watermark.high` only ever
    /// advances in an operation that then raises that floor (see
    /// `certified_seq_floor`), so an unchanged floor certifies the cached
    /// value. This is what takes the stale-hint LATEST path
    /// ([`Self::coalesced_high_from_index`]) off the per-partition
    /// `watermark.json` conditional GET: a wide `endOffsets(assignment)` costs
    /// O(prefixes), not O(partitions), in object-store round-trips.
    coalesced_watermark_floors: Arc<Mutex<BTreeMap<Topition, (i64, u64)>>>,

    /// Per-partition memo of the resolved truncation floor (#176), including
    /// the **absence** of one (memoized as `0`). Read paths that do not pass
    /// through the watermark slow path (EARLIEST on a fresh process, the
    /// fetch clamp) resolve the floor via [`Self::truncate_floor`], which
    /// pays at most one `watermark.json` GET per process per partition and
    /// then serves from here — never a per-call 404 on floor-less partitions
    /// (the #161 pathology). Entries are max-folded (the floor is monotonic)
    /// and evicted on `create_topic` so a re-created topic does not inherit
    /// a dead incarnation's floor. The OptiCon watermark cache, when
    /// populated, takes precedence over this memo: it is refreshed by the
    /// cold/slow watermark reads, while the memo is not.
    truncate_floors: Arc<Mutex<BTreeMap<Topition, i64>>>,

    /// Per-prefix single-flight for the stale-index refresh and the certified
    /// seq-floor sync. A wide ListOffsets resolves its partitions concurrently
    /// (32-way), so without this every stale same-prefix partition in flight
    /// would issue its own duplicate `segments/` LIST and `seq-floor.json`
    /// GET — re-inflating the per-prefix amortized cost back toward
    /// per-partition. Losers of the race re-check under the lock and are
    /// served by the winner's work. Fresh (TTL-served) reads never touch this
    /// lock.
    prefix_read_sync_locks: Arc<Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>>,

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

    /// Per-prefix coalescing buffer (#57) — the only produce buffer since #177.
    /// Keyed by prefix, so one buffer accumulates `PrefixPending` batches across
    /// many topitions; drained (never held across an await) on a threshold or
    /// linger flush into one create-only segment object.
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

    /// Measurement-only trace of which segment objects this pod has recently read
    /// record bytes from, and over which byte ranges (#117). Not a cache — it
    /// holds no data and nothing reads through it; it exists to answer the one
    /// question that decides #117's design: when a segment object is read more
    /// than once on a pod, is it the *same* range (what a `(prefix, seq, range)`
    /// block cache would serve) or a *different* one (co-prefix sub-streams
    /// reading disjoint slices of the same object, which only a whole-object cache
    /// would serve)? See [`SegmentReadTrace`].
    segment_reads: Arc<Mutex<BTreeMap<(String, u64), SegmentReadTrace>>>,

    /// Per-prefix compaction leases this process holds (#66) — the maintenance
    /// side of the single-writer fence. The produce lease is gone with #177;
    /// this is the only remaining lease.
    compaction_leases: Arc<Mutex<BTreeMap<String, HeldLease>>>,

    /// Per-prefix leaseless *era* epoch (#92), the durable side being
    /// `prefixes/{prefix}/era.json`. Seeded on the first leaseless flush of a
    /// prefix as `max(lease.json epoch, max footer epoch) + 1` (never 0) and
    /// stamped as a constant `writer_epoch` into every leaseless segment, so a
    /// straggler from the pre-cutover lease era can never win the overlap
    /// tie-break in [`Self::valid_substream_segments`] and erase acked data. This
    /// caches it so the seeding object is read once per process per prefix.
    era_epochs: Arc<Mutex<BTreeMap<String, i64>>>,

    /// Whether a topition still has legacy `records/` objects, cached (with the
    /// check time) to keep the prefix-coalesced read path off a per-fetch
    /// `records/` LIST (#60). A topic flipped to segment mode mid-life is a
    /// *hybrid*: `[0, C)` legacy objects, `[C, ∞)` segments; its fetch must serve
    /// both. A greenfield prefix has no legacy objects and must not pay that
    /// LIST. Each entry carries its own TTL (`checked_at`, `ttl`): a `true`
    /// result — or a `false` for a topition not yet proven segment-routed —
    /// expires after [`Self::HIGH_WATERMARK_HINT_TTL`] so a drain
    /// (`true`→`false`) or a fresh legacy write (`false`→`true`) is picked up
    /// within seconds; a `false` for a segment-routed topition is held for
    /// [`Self::LEGACY_ABSENCE_TTL`] (its legacy seam only ever drains), cutting
    /// the dominant per-topition LIST rate (#109/#110). A legacy write on this
    /// process flips the entry to `true` immediately via
    /// [`Self::note_legacy_records_present`].
    legacy_records_present: Arc<Mutex<LegacyRecordsMemo>>,

    /// Per-topic memo of whether `cleanup.policy` is `compact`, with a check
    /// time (#113). `produce` consults this on every batch to decide the
    /// coalesce route; the topic config changes only on `AlterConfigs` (rare), so
    /// a short TTL keeps the produce hot path off a per-batch conditional GET of
    /// `topic-metadata/<name>.json` while still picking up a policy change within
    /// the window.
    compacted_topics: Arc<Mutex<BTreeMap<String, (bool, SystemTime)>>>,

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

    /// Bound on the per-key pass's `seen` key set for one partition (#175). The
    /// set is O(distinct keys per partition) — identical to the legacy
    /// compactor's — but a pathological keyspace could balloon a maintainer's
    /// memory; past the cap the pass aborts that partition for this tick
    /// (removing nothing — never corrupting), rather than growing unbounded. A
    /// Kafka-style dirty map is the follow-up if a real workload hits this.
    prefix_compact_seen_keys: usize,

    /// Recency window for stateless maintenance scheduling (#126): a prefix
    /// whose compaction lease was last acquired within this window is skipped by
    /// other maintainers (they neither LIST nor re-work it). Set to ~0.9× the
    /// `maintenance_interval` so every prefix is still maintained ~once per
    /// interval by exactly one replica. `0` disables the skip (every maintainer
    /// works every prefix — the single-maintainer default behaviour).
    maintenance_recency: Duration,

    /// Wall-clock budget for the leaseless prefix flush's conflict-correction
    /// loop (#157/#192). The loop yields to a competing writer rather than
    /// amplifying LIST+PUT against a contended prefix — but only once it has
    /// made [`Self::MIN_FLUSH_ATTEMPTS`] real attempts, because surrendering
    /// rejects the produce and a rejected produce costs a connector restart
    /// downstream. Overridable via `flush_max_elapsed`.
    flush_max_elapsed: Duration,

    /// Per-process random seed for the maintenance traversal shuffle (#126), so
    /// N stateless maintainers sweep the prefix set in independent orders and
    /// partition the work by first-arrival rather than all starting at prefix 0.
    maintenance_seed: u64,

    object_store: Arc<DynObjectStore>,
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
    /// Set once any buffered batch is backfill-class (span ≥
    /// [`DynoStore::PREFIX_BACKFILL_MIN_RECORDS`]): relaxes the flush triggers to
    /// backfill floors so a folded-in snapshot (#90) coalesces into a few large
    /// segments instead of one per batch. Reset with the buffer on flush.
    backfill: bool,
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
    /// Wall-clock ms of the last acquire (#126). For the compaction lease this is
    /// the last time a maintainer *claimed* the prefix, so a peer can skip a
    /// recently-maintained prefix without doing its work (the recency window).
    /// `#[serde(default)]` = 0 for old objects and for the produce lease (which
    /// never reads it). Distinct from `expires_at_ms` so the lease TTL (fencing /
    /// crash-takeover) stays decoupled from the maintenance recency window.
    #[serde(default)]
    maintained_at_ms: i64,
}

/// Durable lower bound on the next segment sequence for a prefix (#77). Segment
/// names are a create-only, monotonic sequence, but a *name* can be freed by
/// retention/compaction while a peer (another replica, or an external S3-direct
/// reader) still caches the old footer for that sequence — reusing the name would
/// then serve the old byte ranges against a new object. Persisting a floor,
/// raised write-ahead of every delete, guarantees a freed name is never reused.
/// CAS-mutated at most once per maintenance tick per prefix that deleted
/// something — never on the produce hot path — so it stays well under GCS's
/// ~1/s/object mutation cap.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct SeqFloor {
    next_seq_floor: u64,
}

/// Durable per-prefix leaseless *era* marker (#92): the `writer_epoch` every
/// leaseless segment of the prefix carries. Seeded once, at the migration
/// cutover, as `max(lease epoch, max footer epoch) + 1` (never 0) so leaseless
/// writes strictly out-epoch every pre-cutover lease-era segment — a mixed fleet
/// is otherwise corrupt (a straggler's lease-era epoch would win the overlap
/// tie-break and erase acked data). Create-only and immutable: a constant era
/// for the whole leaseless regime, read once per process per prefix.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct Era {
    era_epoch: i64,
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
    /// Monotonic token bumped whenever this process's view of the segment set
    /// may have *lost* a segment's tail knowledge: a committed real listing
    /// (which can reflect another replica's deletions) or a prune. The
    /// certified seq floor below is valid only for the generation it was read
    /// under — see [`DynoStore::certified_seq_floor`] for the ordering
    /// argument.
    generation: u64,
    /// The persisted next-sequence floor (#77) as last read *after* the
    /// listing committed under `generation` (`(floor, generation)`), if
    /// synced. The floor is raised write-ahead of every segment delete, so a
    /// floor read ordered after a listing certifies every deletion that
    /// listing could have observed; that is what lets the ListOffsets LATEST
    /// fast path skip the per-partition `watermark.json` GET.
    seq_floor: Option<(u64, u64)>,
    /// Sequences whose segment object was listed but carries **no decodable
    /// footer** (`read_segment_footer` → `None`: shorter than the trailer, or a
    /// tail whose magic is not `TSEG`). They can never enter `segments`, yet
    /// they *own* their name in the create-only namespace, so the leaseless
    /// arbiter must still step over them (#157): deriving the candidate from
    /// `segments` alone re-picks an occupied sequence on every attempt, so the
    /// create-CAS budget is burned deterministically — on every replica, at any
    /// produce rate — and the prefix wedges until retention raises the floor
    /// past it. Kept out of `segments` so no fetch/high-watermark/retention path
    /// can ever see a segment it cannot decode; kept here (rather than
    /// discarded) so both the arbiter and the incremental listing cursor treat
    /// the name as resolved, which also stops every forced refresh re-GETting
    /// its footer.
    ///
    /// Expected to stay empty: a nonzero
    /// `tansu_prefix_segment_footer_undecodable` means a foreign or truncated
    /// object is squatting the segment namespace.
    opaque: BTreeSet<u64>,
}

impl PrefixIndex {
    /// The highest sequence this process has *resolved* by a listing — decoded
    /// into `segments` or recorded as [`Self::opaque`]. Both the leaseless
    /// candidate derivation and the incremental-listing cursor use this, so an
    /// undecodable object neither wedges the arbiter (#157) nor is re-GET on
    /// every refresh. Committed in ascending order with the footers, so it stays
    /// a contiguous watermark even after a partial (cancelled) build (#105).
    fn resolved_max(&self) -> Option<u64> {
        self.segments
            .keys()
            .next_back()
            .copied()
            .max(self.opaque.iter().next_back().copied())
    }
}

/// Outcome of following a prefix's segment tail with ranged GETs instead of a
/// `ListObjectsV2` (#112). See [`DynoStore::probe_prefix_tail`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TailProbe {
    /// The tail is proven and anything new is folded — no LIST needed.
    Resolved,

    /// The proof does not hold; the caller must LIST. Carries why, for the
    /// fallback-rate metric.
    Inconclusive(&'static str),
}

/// `segments/` listings issued to refresh a prefix index (#112), by `path`
/// (`forced` = the produce-path fold, `ttl` = a read-path refresh) and `reason`
/// (why the cheaper tail probe could not answer). Tier-1 requests, ~12x the price
/// of the GET the probe replaces them with, so the ratio of this to
/// [`PREFIX_TAIL_PROBES`] is the saving.
static PREFIX_INDEX_LISTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_index_lists")
        .with_description("segments/ listings issued to refresh a prefix index")
        .build()
});

/// Prefix-index refreshes answered by the tail probe instead of a listing (#112),
/// by `path` and `outcome` (`up_to_date` = nothing new was there, `extended` = new
/// segments were folded from their probe GETs).
static PREFIX_TAIL_PROBES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_tail_probes")
        .with_description("prefix-index refreshes served by a tail probe, not a listing")
        .build()
});

/// What one pod has recently read out of a single segment object (#117):
/// the distinct `(byte_start, byte_len)` ranges it fetched record bytes over, and
/// when it last did. Measurement only — no record bytes are held here.
///
/// The distinct-range list is capped ([`SEGMENT_READ_TRACE_RANGES`]): past the cap
/// a range that is not already listed still counts as a different-range repeat, it
/// just stops being remembered, which can only *under*-count the same-range class.
/// That is the conservative direction: it never invents evidence for the cheaper
/// design.
#[derive(Clone, Debug)]
struct SegmentReadTrace {
    ranges: Vec<(u64, u64)>,
    last_read: SystemTime,
}

/// Distinct byte ranges remembered per segment object (#117).
const SEGMENT_READ_TRACE_RANGES: usize = 8;

/// Segment objects traced at once (#117). A read of an untraced object past this
/// cap prunes entries older than [`SEGMENT_READ_TRACE_TTL`] first, and clears the
/// trace outright if that frees nothing — a measurement device must never be the
/// thing that grows without bound.
const SEGMENT_READ_TRACE_OBJECTS: usize = 1_024;

/// How long a segment read stays interesting for overlap accounting (#117).
/// Bounds what "read more than once" means: a cache only collapses reads close
/// enough together to still be resident, so counting a repeat hours later would
/// overstate what any cache could serve.
const SEGMENT_READ_TRACE_TTL: Duration = Duration::from_secs(60);

/// Ranged GETs of segment *record* bytes on the fetch path (#117) — the
/// tier-2 requests a consumer-side cache would target. Footer GETs are already
/// served from the in-memory index and are not counted here.
static SEGMENT_DATA_GETS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_segment_data_gets")
        .with_description("ranged GETs of segment record bytes served to consumers")
        .build()
});

/// Bytes requested by those GETs (#117), so the request/byte trade of caching
/// whole objects instead of ranges can be costed rather than guessed.
static SEGMENT_DATA_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_segment_data_bytes")
        .with_description("bytes requested by ranged GETs of segment record bytes")
        .build()
});

/// Segment-data GETs of an object this pod already read within
/// [`SEGMENT_READ_TRACE_TTL`] (#117), labelled `overlap`:
///
/// - `same_range` — the identical span was fetched again: what the block cache
///   proposed in #117 would have served.
/// - `other_range` — a different span of the same object: co-prefix sub-streams
///   reading disjoint slices, which a range-keyed cache **cannot** serve and only
///   a whole-object cache could.
///
/// The split between these two is the number #117's design hinges on; a low total
/// closes the issue instead.
static SEGMENT_DATA_GET_REPEATS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_segment_data_get_repeats")
        .with_description("segment-data GETs of a recently-read object, by range overlap")
        .build()
});

/// Who is claiming a segment's tail sequence (#130). Both roles create into the
/// *same* `segments/{seq}` namespace and therefore contend with each other, but
/// they react differently to losing the race, and their contention is worth
/// telling apart in the metrics: the compactor's share is what a separate
/// `compacted/` namespace would remove.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SegmentCreateRole {
    /// Segment compaction (#66). Since #177 this is the only role that creates
    /// a segment out of band: the produce path goes through the leaseless
    /// arbiter (#86), which resyncs on conflict and has no lease to fence.
    Compaction,
}

impl SegmentCreateRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Compaction => "compaction",
        }
    }
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
    /// Per-partition `seen` key-set cap for the per-key compaction pass (#175).
    pub prefix_compact_seen_keys: Option<usize>,
    /// Maintenance recency window (#126); set to ~0.9× `maintenance_interval`.
    pub maintenance_recency: Option<Duration>,
    /// Wall-clock budget for the leaseless flush's conflict-correction loop
    /// (#192). Hard-coded at 10s before that issue, which is too small to admit
    /// a useful number of attempts once one attempt costs seconds.
    pub flush_max_elapsed: Option<Duration>,
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

/// Records removed by the per-key compaction pass over a compacted topic's
/// dedicated prefix (#175) — the signal that key-based cleanup is actually
/// reclaiming superseded values, which `SEGMENT_COMPACTIONS` (a byte-identical
/// merge) cannot show.
static SEGMENT_RECORDS_COMPACTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_records_compacted")
        .with_description("records removed by per-key compaction over segments")
        .build()
});

/// Legacy `records/` objects retired by the carry-over (#175 release 2) — the
/// drain progress signal, and the one that tells when the backlog is gone: it
/// stops moving because every compacted topic is drained, not because the pass
/// stopped running.
/// Topics `Metadata` could not resolve but the topics index knows exist (#214).
///
/// Expected to be zero. Nonzero means a per-topic metadata read came back empty
/// for a topic that is there — the failure that previously reached clients as
/// `UNKNOWN_TOPIC_OR_PARTITION` and took source connectors into restart loops,
/// with no broker-side signal at all. This counter is that signal.
static METADATA_UNRESOLVED_EXISTING: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_metadata_unresolved_existing_topic")
        .with_description("topics Metadata could not resolve although the index knows them")
        .build()
});

static SEGMENT_CARRYOVER_OBJECTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_carryover_objects")
        .with_description("legacy records/ objects carried into segments and deleted")
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

/// Segment objects created at an assigned tail sequence, by `role` (#130) — the
/// denominator for the two counters below, so a conflict count reads as a rate
/// rather than an absolute.
static SEGMENT_CREATES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_creates")
        .with_description("segment objects created at an assigned tail sequence")
        .build()
});

/// Tail-sequence create-CAS rounds lost to a concurrent writer of the same
/// prefix, by `role` (#130). Compaction claims the merged segment's name from the
/// *same* sequence namespace as live producers, so on a busy prefix it contends
/// with them — and each loss re-lists and re-uploads the whole merged payload.
static SEGMENT_CREATE_CONFLICTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_create_conflicts")
        .with_description("segment tail-sequence create-CAS rounds lost to another writer")
        .build()
});

/// Payload bytes re-uploaded because a segment create-CAS was lost, by `role`
/// (#130). This is the write amplification a separate `compacted/` namespace would
/// remove: a merged payload is up to `prefix_compact_target_bytes`, and a losing
/// compactor PUTs all of it again every round. Measured before committing to that
/// split, whose correctness surface is large — the theorized worst case
/// (`MAX_ATTEMPTS × target_bytes` per pass) has never been observed, only derived.
static SEGMENT_CREATE_BYTES_REWRITTEN: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_create_bytes_rewritten")
        .with_description("payload bytes re-uploaded after losing a segment create-CAS")
        .build()
});

/// Create-CAS rounds a leaseless flush lost to another writer of the same
/// prefix (#157) — `AlreadyExists`, or an ambiguous PUT resolved to a peer's
/// footer. A rate here is normal multi-writer arbitration; a rate approaching
/// `MAX_ATTEMPTS ×` the flush rate is the contention that exhausts a budget.
static FLUSH_CAS_CONFLICTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_flush_cas_conflicts")
        .with_description("leaseless flush segment create-CAS rounds lost to another writer")
        .build()
});

/// Leaseless flushes that gave up their create-CAS budget (attempts or elapsed)
/// and returned a retriable error to the producer (#157). Every increment is a
/// failed produce round-trip, so this is the alerting signal for the
/// contention/wedge class.
static FLUSH_CAS_EXHAUSTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_flush_cas_exhausted")
        .with_description("leaseless flushes that exhausted their segment create-CAS budget")
        .build()
});

/// Segment objects listed under a prefix whose footer could not be decoded
/// (#157). Expected to be zero: nonzero means something is squatting the
/// create-only segment namespace, and the arbiter is stepping over those names.
static SEGMENT_FOOTER_UNDECODABLE: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_footer_undecodable")
        .with_description("listed segment objects carrying no decodable footer")
        .build()
});

/// Segments discovered by a listing that were gone by the time their footer was
/// read (#191): concurrent compaction (#66) merged them away, or retention (#61)
/// reclaimed them. A normal consequence of maintenance running against live
/// readers, so nonzero is expected — but it is worth a series, because it used to
/// abort the whole index refresh and surface as a raw `NotFound` on `ListOffsets`.
static SEGMENT_VANISHED_BEFORE_READ: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_vanished_before_read")
        .with_description("segments deleted between the discovery listing and the footer read")
        .build()
});

/// Why a listing was issued (#165). A per-method LIST total says the tier-1
/// plane is large; it does not say which of the ~20 scan sites is spending it,
/// which is the question an aggregate cannot answer and the one that matters when
/// LIST is ~85% of the bill (#166).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scan {
    /// Refreshing a prefix's segment index, or deriving its tail sequence. The
    /// read path and maintenance (compaction, segment retention) both discover
    /// segments through that one refresh, so they share this purpose.
    SegmentIndex,
    /// Retention/expiry scanning what is past its threshold.
    Retention,
    /// Probing whether a partition has legacy `records/` objects.
    LegacyProbe,
    /// Reading the legacy `records/` region: offsets, batches, fetch.
    LegacyRecords,
    /// Resolving `list_offsets` for a partition.
    ListOffsets,
    /// Consumer-group bookkeeping.
    Group,
    /// The topic-metadata index refresh.
    TopicMetadata,
    /// Deleting a topic or a consumer group.
    AdminDelete,
    /// Connectivity check.
    Ping,
}

impl Scan {
    fn as_str(self) -> &'static str {
        match self {
            Self::SegmentIndex => "segment_index",
            Self::Retention => "retention",
            Self::LegacyProbe => "legacy_probe",
            Self::LegacyRecords => "legacy_records",
            Self::ListOffsets => "list_offsets",
            Self::Group => "group",
            Self::TopicMetadata => "topic_metadata",
            Self::AdminDelete => "admin_delete",
            Self::Ping => "ping",
        }
    }
}

/// Listings issued, by `purpose` (#165). Pairs with the per-method request metric
/// [`Metron::instrument_listing`] restores: that one counts requests (pages), this
/// one attributes calls to the code that asked for them. A purpose whose rate
/// tracks the metered LIST rate is the one to optimise.
static LIST_SCANS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_object_store_list_scans")
        .with_description("object store listings issued, by call site")
        .build()
});

/// Magic trailer word marking a prefix-coalesced multi-topic segment object
/// (#64), distinguishing a `.seg` from a legacy single-topic coalesced object
/// (#50), which carries no trailer. ASCII `TSEG`.
const SEGMENT_MAGIC: u32 = 0x5453_4547;

/// On-disk version of the segment frame + footer format (#64). Version `0` is
/// the implicit legacy single-topic layout (a bare batch concatenation with no
/// trailer, produced by #50); `1` is the first self-describing multi-topic
/// segment; `2` (#87) adds a per-flush footer nonce and per-batch producer
/// coordinates for log-based idempotent dedup (#88). Versioned so footer fields
/// stay forward-compatible.
///
/// This is the version the **lease-mode** writer (#59) emits. Readers accept
/// `1`, `2` and `3` (see [`Self::decode_segment_footer`]); the version is
/// chosen by the write path, not by a global switch — see
/// [`SEGMENT_FORMAT_VERSION_V2`] and [`SEGMENT_FORMAT_VERSION_V3`].
const SEGMENT_FORMAT_VERSION: u16 = 1;

/// Segment format version carrying the v2 footer additions (#87): a per-flush
/// nonce and per-(idempotent-)batch producer coordinates.
///
/// Emitted by the **leaseless** writer (#86) and by compaction on a leaseless
/// prefix **until release B of #174**, when the leaseless write version became
/// [`SEGMENT_FORMAT_VERSION_V3`]. No writer emits v2 anymore, but v2 segments
/// remain in the buckets until compaction/retention turns them over, so
/// external S3-direct readers must keep decoding it
/// (`docs/virtual-topics-format.md`, kotatsu#82). Not gated on bumping
/// [`SEGMENT_FORMAT_VERSION`]: the versions coexist per prefix according to
/// the writer regime, and all stay readable.
const SEGMENT_FORMAT_VERSION_V2: u16 = 2;

/// Segment format version adding a per-coordinate `flags: u8` (#174): bit 0 =
/// transactional, bit 1 = control, bits 2-7 written 0 and ignored on read. The
/// flags let the footer index transactional data batches and transaction
/// markers (control batches), which release B of #174 routes into segments.
///
/// **What every leaseless write emits** ([`Self::encode_segment_v3`]: the
/// leaseless flush, merge compaction, and the per-key compaction rewrite) —
/// unconditionally: the version follows the writer regime, never the
/// segment's content. Shipped reader-first, in two releases, like the
/// watermark field-erasure fix (#182): [`Self::decode_segment_footer`]
/// hard-errors on an unknown version, and that error propagates through the
/// index refresh into fetch — a broker meeting a segment version it does not
/// know suffers a partition-wide read outage, not a graceful skip. Nor is the
/// blast radius confined to this fleet — external S3-direct readers
/// (kotatsu#82) decode these segments with the same version rejection, which
/// the contract requires of them (`docs/virtual-topics-format.md`). So every
/// reader — internal and external, broker and maintain deployments alike —
/// had to accept v3 (release A, beta.23; kotatsu#87, chart 0.9.0) before this
/// writer flip could land.
const SEGMENT_FORMAT_VERSION_V3: u16 = 3;

/// Fixed-size trailer at the very end of every multi-topic segment (#64):
/// `footer_len (u64) + entry_count (u32) + version (u16) + magic (u32)`. A
/// reader recovers the index with one ranged GET of a suffix that, for almost
/// every segment, already covers the whole footer (see
/// [`SEGMENT_FOOTER_OVER_READ`]); only a footer larger than the over-read needs
/// a second exact GET — never downloading the record body.
const SEGMENT_TRAILER_LEN: usize =
    size_of::<u64>() + size_of::<u32>() + size_of::<u16>() + size_of::<u32>();

/// Speculative suffix size for reading a segment footer in a single ranged GET
/// (#112 follow-up). The trailer + footer of the overwhelming majority of
/// segments fit within this, so one over-reading GET replaces the previous
/// read-trailer-then-read-footer two-GET dance — halving the footer GETs the
/// read/refresh path pays on every non-writer replica. A footer larger than
/// this (a prefix with very many sub-streams) falls back to a second exact GET.
/// Footers are immutable, so the over-read is always self-consistent; the extra
/// bytes are in-region and cost nothing per request.
const SEGMENT_FOOTER_OVER_READ: usize = 64 * 1024;

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
    /// Producer coordinates of the idempotent/transactional batches in this
    /// sub-stream's region, in region (offset) order (#87, footer v2). Empty in a
    /// v1 footer and for non-idempotent batches. Consumed by log-based idempotent
    /// dedup (#88) so duplicate detection derives from the durable log rather than
    /// a lazily-checkpointed `producers/{id}.json`.
    producers: Vec<ProducerCoord>,
}

/// [`ProducerCoord::flags`] bit 0 (#174): the batch is transactional
/// (wire-batch attribute bit 4). Derived from the batch attributes by the v3
/// writer ([`DynoStore::encode_segment_v3`]); transactional *data*
/// coordinates carry real sequences and fold into the [`ProducerTail`] like
/// any idempotent batch.
const FLAG_TRANSACTIONAL: u8 = 0b01;

/// [`ProducerCoord::flags`] bit 1 (#174): the batch is a control batch — a
/// transaction marker (wire-batch attribute bit 5). A marker coordinate is
/// placement metadata, not an idempotent sequence: it carries
/// `base_sequence = last_sequence = -1` and MUST NOT fold into a
/// [`ProducerTail`] (see [`DynoStore::producer_tail_folded`]).
const FLAG_CONTROL: u8 = 0b10;

/// One idempotent/transactional batch's producer coordinates as carried in a
/// v2 segment footer (#87), plus a `flags` byte at v3 (#174). `offset_delta`
/// is the batch's base offset *relative to its sub-stream's* `base_offset` (so
/// it survives the offset re-derivation on a conflict-correction re-encode);
/// `last_sequence` is `base_sequence + (record_count - 1)`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProducerCoord {
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    last_sequence: i32,
    offset_delta: u32,
    /// Batch classification flags, carried on disk from footer v3 (#174):
    /// bit 0 = transactional ([`FLAG_TRANSACTIONAL`], wire-batch attribute
    /// bit 4), bit 1 = control ([`FLAG_CONTROL`], attribute bit 5, a
    /// transaction marker); bits 2-7 are written 0 and ignored on read.
    /// Always `0` when decoded from a v1/v2 footer — those layouts carry no
    /// flags byte. Derived from the batch attribute bits by the v3 writer
    /// ([`DynoStore::encode_segment_v3`]), so every re-encode — conflict
    /// correction, merge compaction, the per-key rewrite — carries flags
    /// forward for free.
    flags: u8,
}

/// Kafka's per-producer duplicate window: the last five batches are retained so
/// a retried (duplicate) batch is acked with its *original* offset rather than
/// re-appended.
const IDEMPOTENT_WINDOW: usize = 5;

/// The idempotent-dedup outcome for one batch, classified against the folded
/// [`ProducerTail`] (#88).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IdempotentClass {
    /// In order (`base_sequence == expected`): assign a fresh offset and append.
    Admit,
    /// A retried batch already in the log: ack with its original base offset and
    /// do not re-append (Kafka's duplicate-with-offset).
    Duplicate(i64),
    /// A gap, or a stale sequence too old to verify: `OutOfOrderSequenceNumber`.
    OutOfOrder,
    /// A batch from a fenced (lower) producer epoch: `ProducerFenced`.
    Fenced,
}

/// Per-`(producer_id, topition)` idempotent state folded from the segment
/// footers' producer coordinates (#88) — i.e. from the log itself, so every
/// replica that has folded the same segment set derives the same tail. This
/// replaces the per-pod `producers/{id}.json` view (which diverges across a
/// connection migration and advances *before* the batch is durable, #79) as the
/// dedup authority on the leaseless path. Folding is a pure function of the
/// footer set; classification reads it plus the current flush's in-flight
/// reservations.
#[derive(Clone, Debug, Default)]
struct ProducerTail {
    /// Highest producer epoch folded so far. A lower-epoch batch is fenced; a
    /// higher-epoch batch resets the expected sequence to 0.
    epoch: i16,
    /// Whether any coordinate has been folded at `epoch` — distinguishes a brand
    /// new producer (which must start at sequence 0) from one sitting at a
    /// wrapped 0.
    seen: bool,
    /// The next in-order sequence: Kafka's wrapping increment of the last folded
    /// `last_sequence`. Meaningful only when `seen`.
    next_sequence: i32,
    /// The last <= [`IDEMPOTENT_WINDOW`] folded batches at `epoch`, oldest first,
    /// as `(base_sequence, base_offset)` — the duplicate lookup window.
    window: Vec<(i32, i64)>,
}

impl ProducerTail {
    /// The sequence a next in-order batch must carry (0 for an unseen producer).
    fn expected(&self) -> i32 {
        if self.seen { self.next_sequence } else { 0 }
    }

    /// Fold one batch's coordinate (in log order) into the tail.
    fn fold(&mut self, epoch: i16, base_sequence: i32, last_sequence: i32, base_offset: i64) {
        if epoch < self.epoch {
            return; // a stale, fenced writer's coordinate — ignore it
        }
        if epoch > self.epoch {
            self.window.clear(); // the new epoch resets the stream
        }
        self.epoch = epoch;
        self.seen = true;
        self.next_sequence = Self::seq_increment(last_sequence);
        if self.window.len() == IDEMPOTENT_WINDOW {
            _ = self.window.remove(0);
        }
        self.window.push((base_sequence, base_offset));
    }

    /// Classify a batch's `(epoch, base_sequence)` against the folded tail.
    fn classify(&self, epoch: i16, base_sequence: i32) -> IdempotentClass {
        if epoch < self.epoch {
            return IdempotentClass::Fenced;
        }
        // A higher epoch resets the stream: only sequence 0 is in order, and the
        // prior epoch's duplicate window no longer applies.
        let (expected, fresh_epoch) = if epoch > self.epoch {
            (0, true)
        } else {
            (self.expected(), false)
        };
        if base_sequence == expected {
            IdempotentClass::Admit
        } else if !fresh_epoch
            && let Some((_, offset)) = self.window.iter().find(|(seq, _)| *seq == base_sequence)
        {
            IdempotentClass::Duplicate(*offset)
        } else {
            IdempotentClass::OutOfOrder
        }
    }

    /// Kafka's `DefaultRecordBatch` sequence increment: wraps at `i32::MAX` back
    /// to 0 (sequences stay non-negative), keeping the dedup arithmetic
    /// wraparound-safe (#80).
    fn seq_increment(sequence: i32) -> i32 {
        if sequence == i32::MAX {
            0
        } else {
            sequence + 1
        }
    }
}

/// What resolving one discovered segment's footer produced during an index
/// refresh.
///
/// Three states rather than `Option`, because "the object is not readable" and
/// "the object is not there" want different logs: the first is #157's squatter
/// and should be zero, the second is a benign race with maintenance (#191). Both
/// mark the sequence resolved so the arbiter steps over it.
#[derive(Debug)]
enum FooterOutcome {
    Decoded(SegmentFooter),
    Undecodable,
    Vanished,
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
    /// Per-flush nonce (#87, footer v2): lets a writer recognise its own segment
    /// after an ambiguous PUT (create succeeded but the response was lost) and
    /// adopt it instead of re-writing the batch at the next sequence (#89). `0` in
    /// a v1 footer.
    nonce: u64,
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

/// The per-topition durable watermark object (`watermark.json`).
///
/// `rest` is a catch-all for fields this binary does not model: the object is
/// round-tripped through [`OptiCon::with_mut`] on the maintainer path every
/// maintenance interval ([`DynoStore::expire_prefix_segments`] persists `high`,
/// [`DynoStore::advance_low_watermark`] persists `low`), so without it a
/// rolling deploy would let an old process silently erase any field a newer
/// one had just written — for the truncation floor (`truncate`, #176) that
/// means resurrecting records a user deleted. An empty map flattens to no
/// bytes at all, so existing objects are not rewritten on first touch; the
/// guard test `watermark_with_mut_preserves_unknown_fields` pins both
/// properties.
///
/// (`Hash`/`Ord`/`PartialOrd` were dropped with the catch-all —
/// [`serde_json::Value`] does not implement them — which is free: the only
/// consumer is the [`OptiCon`] payload bound, `Clone + Debug + Default +
/// DeserializeOwned + PartialEq + Serialize`.)
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct Watermark {
    low: Option<i64>,
    high: Option<i64>,
    /// Truncation floor (#176): the offset below which `DeleteRecords` has
    /// logically truncated this sub-stream (records survive physically in
    /// shared segments; read paths hide them below this floor). Monotonic:
    /// only ever max-folded under the watermark CAS
    /// ([`DynoStore::delete_records_before`]).
    ///
    /// The `skip_serializing_if` is load-bearing, not stylistic: a floor-less
    /// watermark must keep its pre-#176 byte layout (`{"low":…,"high":…}`) —
    /// the #182 guard test pins that byte identity, and emitting
    /// `"truncate":null` would rewrite (and etag-churn) every watermark
    /// object across a fleet that has no floors.
    ///
    /// Release ordering: writing this field requires the whole fleet at
    /// ≥ 0.7.0-beta.23 — a pre-#182 binary does not model unknown watermark
    /// fields and would erase the floor on its next `watermark.json`
    /// maintenance round-trip, silently un-deleting records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    truncate: Option<i64>,
    #[serde(flatten)]
    rest: BTreeMap<String, serde_json::Value>,
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
            watermarks: Arc::new(Mutex::new(BTreeMap::new())),
            next_offsets: Arc::new(Mutex::new(BTreeMap::new())),
            coalesced_watermark_floors: Arc::new(Mutex::new(BTreeMap::new())),
            truncate_floors: Arc::new(Mutex::new(BTreeMap::new())),
            prefix_read_sync_locks: Arc::new(Mutex::new(BTreeMap::new())),
            producers: Arc::new(Mutex::new(BTreeMap::new())),
            producer_checkpoints: Arc::new(Mutex::new(BTreeMap::new())),
            oldest_retained: Arc::new(Mutex::new(BTreeMap::new())),
            retention_empty_skip: Arc::new(Mutex::new(BTreeMap::new())),
            oldest_retained_prefix: Arc::new(Mutex::new(BTreeMap::new())),
            prefix_coalesce_buffers: Arc::new(Mutex::new(BTreeMap::new())),
            segment_seqs: Arc::new(Mutex::new(BTreeMap::new())),
            prefix_flush_locks: Arc::new(Mutex::new(BTreeMap::new())),
            prefix_index: Arc::new(Mutex::new(BTreeMap::new())),
            segment_reads: Arc::new(Mutex::new(BTreeMap::new())),
            compaction_leases: Arc::new(Mutex::new(BTreeMap::new())),
            era_epochs: Arc::new(Mutex::new(BTreeMap::new())),
            legacy_records_present: Arc::new(Mutex::new(BTreeMap::new())),
            compacted_topics: Arc::new(Mutex::new(BTreeMap::new())),
            // Per-process random component so two ReplicaSet pods are actually
            // distinguishable (#126): `node` is always 111 and `WRITER_INSTANCE`
            // is a per-process counter, so without entropy every pod's first
            // store is "111-0" — harmless for the lease (the etag is the fence)
            // but it makes the lease `holder` useless for forensics and would
            // break any identity-derived scheme.
            writer_id: format!(
                "{node}-{:016x}-{}",
                rng().random::<u64>(),
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
            prefix_compact_seen_keys: Self::PREFIX_COMPACT_SEEN_KEYS,
            maintenance_recency: Self::MAINTENANCE_RECENCY,
            flush_max_elapsed: Self::FLUSH_MAX_ELAPSED,
            maintenance_seed: rng().random::<u64>(),
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

    pub fn auto_create(self, auto_create: AutoTopicCreate) -> Self {
        Self {
            auto_create,
            ..self
        }
    }

    /// No-op: compacted topics are always segment-routed to their own dedicated
    /// prefix (#175), and the per-key pass there is what enforces
    /// `cleanup.policy=compact`. Retained so the existing call sites still
    /// compile.
    pub fn compacted_segments(self, _compacted_segments: bool) -> Self {
        self
    }

    /// No-op: the legacy carry-over always runs, which makes it the standing
    /// reconciliation for any straggler rather than a one-shot migration
    /// (#175 release 2, widened by #211). Retained so the existing call sites
    /// still compile.
    pub fn compacted_carryover(self, _compacted_carryover: bool) -> Self {
        self
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
            prefix_compact_seen_keys: tuning
                .prefix_compact_seen_keys
                .unwrap_or(self.prefix_compact_seen_keys),
            maintenance_recency: tuning
                .maintenance_recency
                .unwrap_or(self.maintenance_recency),
            flush_max_elapsed: tuning.flush_max_elapsed.unwrap_or(self.flush_max_elapsed),
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

    /// How long a *negative* `has_legacy_records` result is served from memory
    /// for a topition that is provably segment-routed (prefix-coalesced and
    /// already owning at least one segment), before the `records/` prefix is
    /// re-listed once (#109/#110). Such a topition's only legacy objects are the
    /// drained `[0, C)` hybrid seam, which retention only shrinks and the
    /// segment write path never re-creates — so "no legacy records" is stable
    /// for hours, not seconds, and re-listing it every
    /// [`Self::HIGH_WATERMARK_HINT_TTL`] was the dominant residual per-topition
    /// LIST cost. A *positive* result keeps the short TTL so a drain
    /// (`true`→`false`) is still picked up within seconds, and any legacy write
    /// on this process flips the entry back to `true` immediately via
    /// [`Self::note_legacy_records_present`]. The only cross-process
    /// `false`→`true` source is a txn/control/compacted batch written to a
    /// segment-routed topic on another replica — which the single-authority CDC
    /// model does not produce; the bound here is that worst case (≤ this window),
    /// symmetric to [`Self::RETENTION_EMPTY_SKIP_TTL`].
    const LEGACY_ABSENCE_TTL: Duration = Duration::from_secs(60 * 60);

    /// How long a *negative* `has_legacy_records` result is served for a
    /// prefix-coalesced topition that does **not** yet own a segment (#166).
    ///
    /// [`Self::LEGACY_ABSENCE_TTL`] was made conditional on already owning a
    /// segment, so everything else — every idle stream not yet flushed into a
    /// prefix segment, and every partition whose memo is cold after a restart —
    /// fell back to [`Self::HIGH_WATERMARK_HINT_TTL`] and re-listed `records/`
    /// every few seconds on the read path. At a large subscription count that was
    /// the dominant tier-1 request source: LIST metered ~1,200/s, ~85% of the
    /// bill, and it stepped up exactly when consumer targets restarted.
    ///
    /// A segment-less coalesced sub-stream has no more reason to hold legacy
    /// objects than a segment-routed one — the segment write path never creates
    /// them either — so the short window bought very little. It is not given the
    /// full hour, though: for a stream we have never seen a segment for, the
    /// "only a txn/control/backfill batch on another replica can flip this"
    /// argument behind the hour is the least evidenced, and the failure it bounds
    /// is more visible (a stream that reads as *empty*, rather than a tail that
    /// lags). Minutes cut the LIST rate ~60x while keeping that worst case short;
    /// a legacy write on this process still flips the memo at once
    /// ([`Self::note_legacy_records_present`]).
    ///
    /// The remaining cross-replica window is inherent to a per-process memo. The
    /// durable fix is to seal the legacy region per prefix so an invalidation is
    /// visible to every replica (#166), which also retires the per-partition probe
    /// entirely.
    const LEGACY_ABSENCE_UNSEGMENTED_TTL: Duration = Duration::from_secs(5 * 60);

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

    /// Byte floor for a flush buffer that has ingested a backfill-class batch
    /// (span ≥ [`Self::PREFIX_BACKFILL_MIN_RECORDS`]) once the #62 bypass is
    /// folded into the segment path (#90). A snapshot's large batches must
    /// coalesce into a few big segments rather than one small segment per batch,
    /// which would blow up the live segment count `S` (#91) and defeat the
    /// ~1-PUT-per-large-batch parity the bypass gave. Well above the
    /// steady-state [`Self::COALESCE_BYTES`], so only a backfill widens the
    /// window; the [`Self::COALESCE_MAX_RECORDS`] cap still bounds a segment.
    const BACKFILL_COALESCE_BYTES: usize = 32 << 20;

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
    ///
    /// 16 MiB rather than a larger target because the merged create currently
    /// shares the producer tail create-CAS namespace (#130): a bigger merged PUT
    /// spends longer in flight, loses the create race to producers more often,
    /// and re-uploads its whole payload on each retry — so an oversized target
    /// multiplies the S3 write amplification and request pressure that feeds
    /// `503 SlowDown`. A smaller target keeps each merged PUT short (less
    /// re-upload per lost race, a narrower conflict window) while compaction
    /// still bounds the live segment count, which is driven by
    /// [`Self::PREFIX_COMPACT_MIN_SEGMENTS`] (a count trigger, independent of the
    /// target), not by this size. Overridable via `prefix_compact_target_bytes`.
    const PREFIX_COMPACT_TARGET_BYTES: usize = 16 << 20;

    /// Newest segments never compacted (#66): leaves the actively-produced tail
    /// alone so compaction never races the current write point.
    const PREFIX_COMPACT_KEEP_HOT: usize = 16;

    /// Default maintenance recency window (#126): ~0.9× the default
    /// `maintenance_interval` (10 min), so a prefix maintained by one replica is
    /// skipped by peers for just under an interval and every prefix is still
    /// maintained ~once per interval. Override with `maintenance_recency` to
    /// match a non-default interval.
    const MAINTENANCE_RECENCY: Duration = Duration::from_secs(9 * 60);

    /// Default wall-clock budget for the leaseless flush loop (#157/#192).
    ///
    /// Unchanged from the value #157 introduced, but it is now a floor on
    /// *attempts* rather than a hard deadline: see `MIN_FLUSH_ATTEMPTS`. A
    /// budget this size admits only two or three attempts once a flush's
    /// segment PUT costs seconds, which is why #192 saw exhaustion at one
    /// conflict.
    const FLUSH_MAX_ELAPSED: Duration = Duration::from_secs(10);

    /// Attempts the leaseless flush always makes before the clock may end it
    /// (#192).
    ///
    /// The budget exists to stop amplifying LIST+PUT against a prefix this
    /// writer keeps losing. Applied from the first attempt it does something
    /// else: it converts a *slow* bucket into a rejected produce, and the
    /// clients here treat a retriable rejection as an engine failure and
    /// restart the whole connector. Three attempts is enough to distinguish
    /// "losing a race" from "one slow PUT" while still bounding the work.
    const MIN_FLUSH_ATTEMPTS: usize = 3;

    /// Default cap on the per-key pass's `seen` key set per partition (#175).
    /// Far above any real compacted topic here (connector config/status/offsets
    /// topics hold hundreds of keys); a partition exceeding it skips removal
    /// for the tick instead of ballooning maintainer memory.
    const PREFIX_COMPACT_SEEN_KEYS: usize = 1_000_000;

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
        let listed = self.scan_delimited(Scan::TopicMetadata, &prefix).await?;

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

    /// The cached log-start (`watermark.low`) for `topition` **without any
    /// object-store request**, or `None` when this process has never read the
    /// watermark object. Serving `log_start` from here keeps a warm,
    /// read-uncommitted fetch off the per-poll `watermark.json` GET (#109). A
    /// missing (or stale-low) value reports 0 — the correct pre-retention log
    /// start; a consumer that fetches from it self-corrects to the true
    /// earliest, and the value is refreshed whenever the cold high-watermark
    /// path reads the watermark object.
    fn cached_low(&self, topition: &Topition) -> Result<Option<i64>> {
        Ok(self.watermark(topition)?.cached().and_then(|w| w.low))
    }

    /// The truncation floor (#176) for `topition` from in-process caches only
    /// — the OptiCon watermark cache first (it is refreshed by the cold/slow
    /// watermark reads), else the [`Self::truncate_floors`] memo — **without
    /// any object-store request**. `None` when neither holds the partition.
    /// Callers on request-free paths treat `None` as "no floor known": for
    /// read-uncommitted `offset_stage_at` that degrades to the pre-truncation
    /// log start (self-correcting, like `cached_low`); for the segment-expiry
    /// loop the direction is mandatory — an unknown floor must defer reclaim,
    /// never force it.
    fn cached_truncate(&self, topition: &Topition) -> Result<Option<i64>> {
        // Deliberately non-inserting (unlike `self.watermark(...)`) so the
        // expiry loop's sweep over every footer entry does not populate an
        // OptiCon handle per sub-stream it will never serve.
        let from_watermark = self
            .watermarks
            .lock()
            .map(|locked| {
                locked
                    .get(topition)
                    .and_then(|watermark| watermark.cached())
                    .map(|watermark| watermark.truncate.unwrap_or(0))
            })
            .map_err(Into::<Error>::into)?;

        if from_watermark.is_some() {
            return Ok(from_watermark);
        }

        self.truncate_floors
            .lock()
            .map(|locked| locked.get(topition).copied())
            .map_err(Into::into)
    }

    /// The truncation floor (#176) for `topition`: the offset below which
    /// `DeleteRecords` has logically truncated this sub-stream, `0` when it
    /// never was.
    ///
    /// Zero object-store requests in steady state: served from
    /// [`Self::cached_truncate`] (the OptiCon watermark cache, populated
    /// wherever the cold path already reads `watermark.json`, or the memo).
    /// Only a fully-cold call — neither cache holds the partition — pays one
    /// `watermark.json` GET, and the result **including absence** (memoized
    /// as `0`) is retained, so a watermark-less partition is asked once per
    /// process, never per call — the #161 404-storm shape.
    ///
    /// Measured, after #194 questioned this claim and #203 made it checkable
    /// (0.7.0-beta.26, production fleet): the steady-state cost is **zero
    /// 404s**, and so is the cold cost. `class="watermark"` reads run at
    /// ~1,160/s through the cache and **not one of them** answers `not_found`,
    /// across a window spanning a rolling restart — so neither the "one 404 per
    /// process per partition" warm-up this comment described, nor the
    /// first-touch tail #194 hypothesised, is observable. The floor is cheaper
    /// than #186 claimed, not more expensive.
    ///
    /// #194's elevated `not_found` rate is real and persists (~34/s), but it is
    /// not this: 74% of it is the #112 tail probe (`class="segment"`, correlated
    /// 1:1 with `tansu_prefix_tail_probes`), which speculatively GETs the next
    /// sequence to avoid a listing and predates the floor by two releases. The
    /// attribution to #176 was coincident timing.
    ///
    /// Cross-replica staleness contract: the pod that served the
    /// `DeleteRecords` is exact immediately (`with_mut` leaves the written
    /// value in the OptiCon cache). A peer pod serves the floor it last
    /// observed, refreshed whenever its slow high-watermark path re-reads
    /// `watermark.json`: for a legacy/hybrid sub-stream that is every
    /// stale-hint slow poll (≈ the high-watermark hint TTL), but for a
    /// **pure-segment** sub-stream the watermark is re-read only when the
    /// certified seq-floor generation changes (segment expiry/compaction),
    /// on a cold start, or on this accessor's own first touch — there is
    /// **no TTL bound**, so on a quiet prefix a peer can honour a stale
    /// (lower) floor until restart. Accepted for a rare admin operation; a
    /// stale floor only ever under-hides (a peer serves records another pod
    /// already truncated), it never loses data.
    ///
    /// Release ordering: requires the whole fleet at ≥ 0.7.0-beta.23 — a
    /// pre-#182 pod would erase the floor on its next `watermark.json`
    /// round-trip (see [`Watermark::truncate`]).
    async fn truncate_floor(&self, topition: &Topition) -> Result<i64> {
        if let Some(floor) = self.cached_truncate(topition)? {
            return Ok(floor);
        }

        // Fully cold: pay the watermark GET once. `with` serves the
        // `Default` (no floor -> 0) when the object is absent, and that is
        // memoized too, so absence costs one 404 per process per partition,
        // not one per call (#161).
        let floor = self
            .watermark(topition)?
            .with(&self.object_store, |watermark| {
                Ok(watermark.truncate.unwrap_or(0))
            })
            .await?;

        self.memo_truncate_floor(topition, floor)?;

        Ok(floor)
    }

    /// Max-fold `floor` into the [`Self::truncate_floors`] memo (the floor is
    /// monotonic, so a racing older resolution can never regress it).
    fn memo_truncate_floor(&self, topition: &Topition, floor: i64) -> Result<()> {
        self.truncate_floors
            .lock()
            .map(|mut locked| {
                let entry = locked.entry(topition.to_owned()).or_insert(floor);
                *entry = (*entry).max(floor);
            })
            .map_err(Into::into)
    }

    /// The log start offset for `topition` (#161).
    ///
    /// `watermark.low` is advanced **only** by the legacy `records/` retention
    /// paths ([`Self::expire_partition`] and the compaction drain); segment
    /// retention never writes it. So for a pure-segment sub-stream the object is
    /// authoritatively silent about the log start — and usually absent entirely —
    /// yet read-committed `offset_stage` used to read it on **every poll**, which
    /// measured as ~1490 GET/s returning `404 NoSuchKey` at ~1,600 subscriptions:
    /// round-trips that resolve nothing, add fetch latency, and are billable on a
    /// store that charges 4xx (GCS). Read-uncommitted already avoids it (#109);
    /// this closes the read-committed and cold-path gap.
    ///
    /// For a pure-segment sub-stream the log start comes from the footer index —
    /// the lowest surviving segment base, which is what `list_offsets` EARLIEST
    /// already reports and is *more* accurate than `watermark.low`, since nothing
    /// advances that field after a segment expiry. A hybrid or legacy sub-stream
    /// still reads the object: there the legacy region owns the log start, and
    /// retention does record it.
    ///
    /// Either way the result is clamped to the truncation floor (#176):
    /// `DeleteRecords` hides segment-resident records without touching the
    /// shared segments, so the physical region start can sit below the
    /// logical log start.
    async fn log_start(&self, topition: &Topition) -> Result<i64> {
        if !self.has_legacy_records(topition).await? {
            let start = self.segment_region_start(topition).await?.unwrap_or(0);
            return Ok(start.max(self.truncate_floor(topition).await?));
        }

        self.watermark(topition)?
            .with(&self.object_store, |watermark| {
                debug!(?watermark);
                Ok(watermark
                    .low
                    .unwrap_or(0)
                    .max(watermark.truncate.unwrap_or(0)))
            })
            .await
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
    ///
    /// `as_of` is *when the underlying data was observed*, not `now`: a live
    /// listing passes the instant captured before the LIST; the prefix-coalesce
    /// read path passes the prefix index's `refreshed_at`, because that index is
    /// itself served from a cache up to one TTL old. Stamping `now` instead let
    /// cross-pod visibility staleness compound toward ~2×TTL — the index could be
    /// a TTL stale and then be treated as fresh for another full TTL (#91).
    fn mark_listed(&self, topition: &Topition, high: i64, as_of: SystemTime) -> Result<()> {
        self.next_offsets
            .lock()
            .map(|mut locked| {
                let entry = locked.entry(topition.to_owned()).or_default();
                entry.next = entry.next.max(high);
                entry.listed_at = Some(as_of);
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

                self.scan_from(Scan::LegacyRecords, &prefix, &start_after)
            }
            None => self.scan(Scan::LegacyRecords, &prefix),
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

    /// The cached `watermark.high` floor for a prefix-coalesced sub-stream,
    /// valid only when it was read under the still-current certified seq
    /// `floor` (see [`Self::coalesced_watermark_floors`]). `None` means the
    /// caller must pay the `watermark.json` GET (once — the slow path caches
    /// it via [`Self::cache_coalesced_watermark`]).
    fn cached_coalesced_watermark(&self, topition: &Topition, floor: u64) -> Result<Option<i64>> {
        self.coalesced_watermark_floors
            .lock()
            .map(|locked| {
                locked
                    .get(topition)
                    .and_then(|(high, at)| (*at == floor).then_some(*high))
            })
            .map_err(Into::into)
    }

    /// Cache `high` (a just-read `watermark.high`) for `topition` under the
    /// certified seq `floor` that was current *at or before* the read. Pairing
    /// with an older floor is safe: any watermark advance after the read
    /// raises the floor above `floor`, invalidating this entry; an advance
    /// before the read is already contained in `high`.
    fn cache_coalesced_watermark(&self, topition: &Topition, high: i64, floor: u64) -> Result<()> {
        self.coalesced_watermark_floors
            .lock()
            .map(|mut locked| {
                _ = locked.insert(topition.to_owned(), (high, floor));
            })
            .map_err(Into::into)
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
        // Anchor freshness to before the listing (#91): the LIST reflects state
        // no newer than this instant, so the fresh window never over-claims.
        let listed_at = SystemTime::now();
        let high = self.tail_next_offset(topition, Some(floor)).await?;
        self.mark_listed(topition, high, listed_at)?;
        Ok(high)
    }

    /// The stale-hint high watermark of a prefix-coalesced sub-stream served
    /// from the in-memory segment index alone — no per-partition object-store
    /// request. `None` means the index is not authoritative for this
    /// sub-stream and the caller must fall back to the `watermark.json` GET.
    ///
    /// Correctness (LATEST must equal the true high watermark exactly):
    ///
    /// - **The true high** is `max(tail across live segments + legacy
    ///   `records/` objects, persisted `watermark.high`)`: segments/batches
    ///   are the offset-assignment authority, and the only way assigned
    ///   offsets leave them is retention/compaction, where
    ///   `expire_prefix_segments` persists each affected sub-stream's tail
    ///   into `watermark.high` write-ahead of the delete (retention advances
    ///   the log *start*, never lowers the log *end*).
    /// - **Legacy objects** can hold offsets above the segment tail (#58 seam
    ///   / #62 backfill bypass), so a hybrid sub-stream
    ///   ([`Self::has_legacy_records`], same memo the slow path uses) is not
    ///   served here.
    /// - **The index tail** covers every live segment after
    ///   [`Self::refresh_prefix_index`]: cold builds list the whole prefix,
    ///   and incremental listings can miss no live segment at/below the known
    ///   max (sequences are assigned by create-CAS at `folded max + 1`, so a
    ///   sequence below an observed one can never be created later). Ghost
    ///   entries (a peer's deletion never re-observed) only ever *equal* the
    ///   persisted watermark floor, never exceed the true high.
    /// - **The watermark floor** comes from the per-partition cache certified
    ///   by [`Self::certified_seq_floor`]: `watermark.high` of a coalesced
    ///   sub-stream only advances in an operation that then raises the seq
    ///   floor, so an unchanged certified floor proves the cached value is
    ///   current; any rise invalidates the cache and the slow path re-reads.
    /// - **No segments at all** is not served: an empty sub-stream is
    ///   indistinguishable from a fully drained or lake-sink one, whose only
    ///   authority is `watermark.json` — and a lake-sink high advances
    ///   *without* raising the seq floor, so the floor-certified cache must
    ///   not vouch for it. Those keep today's per-partition GET.
    async fn coalesced_high_from_index(&self, topition: &Topition) -> Result<Option<i64>> {
        if self.has_legacy_records(topition).await? {
            return Ok(None);
        }

        let prefix = self.routed_prefix_of(topition).await?;
        self.refresh_prefix_index(&prefix).await?;

        let Some(tail) = self
            .valid_substream_segments(&prefix, topition.topic(), topition.partition())?
            .last()
            .map(|(_, entry)| entry.base_offset + entry.record_count)
        else {
            return Ok(None);
        };

        // Certified after the refresh above, so the floor covers every
        // watermark advance whose segment deletion that listing could have
        // reflected. One GET per prefix per listing generation, amortized
        // across every partition of the prefix — not per partition.
        let floor = self.certified_seq_floor(&prefix).await?;
        let Some(watermark_floor) = self.cached_coalesced_watermark(topition, floor)? else {
            return Ok(None);
        };

        let high = tail
            .max(watermark_floor)
            .max(self.cached_high(topition)?.unwrap_or(0));

        // Anchor the hint to when the segment set was observed, not `now`
        // (#91), exactly as the slow path does.
        let as_of = self
            .prefix_index_refreshed_at(&prefix)
            .unwrap_or_else(SystemTime::now);
        self.mark_listed(topition, high, as_of)?;

        Ok(Some(high))
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

        // Prefix-coalesced (#60): the tail offset lives in the segment footers,
        // not in a `records/` listing (there is none). The common case — a
        // pure-segment sub-stream whose watermark floor is certified — is
        // served entirely from the in-memory index with ZERO per-partition
        // object requests; that is what takes a wide `endOffsets(assignment)`
        // (the ~1500-partition ListOffsets that still timed out after being
        // parallelized) off the per-partition `watermark.json` conditional
        // GET. GCS-safe: no per-flush-mutated manifest is read.
        if let Some(high) = self.coalesced_high_from_index(topition).await? {
            return Ok(high);
        }

        // Index not authoritative for this sub-stream — hybrid (legacy
        // `records/` objects may sit above the segments), no segment at
        // all (never produced, fully retention-drained, or a lake-sink
        // topic whose offset lives only in `watermark.high`), or a cold /
        // floor-invalidated watermark cache. Pay the `watermark.json` GET
        // and recover footer-only (#58), caching the watermark under the
        // certified floor read *before* it so the fast path serves the
        // next stale-hint resolution.
        let prefix = self.routed_prefix_of(topition).await?;
        let floor = self.certified_seq_floor(&prefix).await?;
        let from_watermark = self.persisted_high(topition).await?;
        self.cache_coalesced_watermark(topition, from_watermark, floor)?;

        let recovered = self
            .recover_substream_next_offset(topition, from_watermark)
            .await?;
        let high = recovered.max(self.cached_high(topition)?.unwrap_or(0));
        // Anchor to when the prefix index was actually listed, not `now`:
        // `recover_substream_next_offset` may have served a TTL-cached index,
        // so stamping `now` would let cross-pod staleness compound to ~2×TTL
        // (#91). Fall back to `now` only if the index has no timestamp (it was
        // just refreshed above, so this is the safe degenerate case).
        let as_of = self
            .prefix_index_refreshed_at(&prefix)
            .unwrap_or_else(SystemTime::now);
        self.mark_listed(topition, high, as_of)?;
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
        // function.
        let _flush_guard = self
            .prefix_flush_lock(&self.routed_prefix_of(topition).await?)?
            .lock_owned()
            .await;

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
                    // A legacy `records/` object now exists for this topition:
                    // flip the presence memo to `true` so a following read on
                    // this process does not serve a stale "no legacy" from the
                    // long negative window (#110).
                    self.note_legacy_records_present(topition)?;
                    return Ok(candidate);
                }

                Err(object_store::Error::AlreadyExists { .. }) => {
                    debug!(
                        candidate,
                        attempt,
                        ?topition,
                        "offset taken, resyncing tail"
                    );
                    let listed_at = SystemTime::now();
                    candidate = self.tail_next_offset(topition, Some(candidate)).await?;
                    self.mark_listed(topition, candidate, listed_at)?;
                }

                Err(err) => return Err(err.into()),
            }
        }

        error!(?topition, candidate, "offset assignment exhausted retries");
        // Retriable: exhaustion is contention/backpressure, not a permanent
        // fault — a fatal code would make the client drop the batch (#6/#129).
        Err(Error::Api(ErrorCode::KafkaStorageError))
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

    /// Whether this topic's legacy `records/` region must be carried into
    /// segments rather than abandoned in place (#211).
    ///
    /// Two classes, for the same reason expressed two ways.
    ///
    /// A **compacted** topic's surviving latest-value-per-key *is* its state
    /// (#175), so abandoning its legacy region loses the state itself.
    ///
    /// A **retain-forever** topic is an operator saying the same thing
    /// explicitly. The case that forced this: schema-history topics carrying
    /// `cleanup.policy=delete` with `retention.ms` at `i64::MAX`. They are not
    /// compacted, so the original predicate skipped them — yet a schema history
    /// is what a connector needs to decode its own journal, so losing it is not
    /// losing replayable history, it is losing the ability to read what remains.
    /// A production sweep found five such topics still holding legacy objects,
    /// one of them with no segment at all, which #179 would have made read as
    /// empty.
    ///
    /// Everything else is genuinely abandonable — the epic's decision (2) —
    /// and is left alone.
    fn legacy_region_must_survive(topic: &CreatableTopic) -> bool {
        let configs = topic.configs.as_deref().unwrap_or_default();

        let retain_forever = configs.iter().any(|config| {
            config.name == "retention.ms"
                && config
                    .value
                    .as_deref()
                    .and_then(|value| i64::from_str(value).ok())
                    .is_some_and(|ms| !(0..Self::RETAIN_FOREVER_MS).contains(&ms))
        });

        Self::topic_configs_are_compacted(topic) || retain_forever
    }

    /// Whether `cleanup.policy` names `compact`, read straight off the stored
    /// config. Shared so the carry-over's selection and its prefix resolution
    /// cannot disagree about what "compacted" means (#211).
    fn topic_configs_are_compacted(topic: &CreatableTopic) -> bool {
        topic
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
            })
    }

    /// The prefix a topition's records are segment-routed under, given a
    /// `compacted` verdict the caller already holds (#175): a compacted topic's
    /// dedicated prefix is its **full topic name**, so its segments never share
    /// an object with a sibling topic whose whole-segment retention (#61) would
    /// delete the compacted topic's old-but-latest keys. Everything else — and
    /// everything, when the flag is off — is [`Self::prefix_of`], byte-identical
    /// to today.
    fn routed_prefix(&self, topition: &Topition, compacted: bool) -> String {
        if compacted {
            topition.topic().to_owned()
        } else {
            self.prefix_of(topition)
        }
    }

    /// [`Self::routed_prefix`] when the caller has no `compacted` verdict in
    /// hand. `prefix_of` is synchronous and on many paths, but every segment
    /// routing consumer is an async fn — so rather than guessing from a cold
    /// cache (a wrong guess would route a compacted topic's segments into the
    /// shared prefix, where a sibling's retention could delete them), this
    /// awaits the memoized [`Self::topic_is_compacted`] (#113): a warm memo is
    /// one map lookup, a cold one is the same conditional metadata GET the
    /// produce gate already pays once per TTL. Topics whose connector prefix
    /// already equals their name (≤ 3 dotted components) skip even that — the
    /// override cannot change their routing.
    async fn routed_prefix_of(&self, topition: &Topition) -> Result<String> {
        let prefix = self.prefix_of(topition);
        if prefix == topition.topic() {
            return Ok(prefix);
        }

        if self.topic_is_compacted(topition.topic()).await? {
            Ok(topition.topic().to_owned())
        } else {
            Ok(prefix)
        }
    }

    /// List `prefix`, attributing the call to the code that asked for it (#165).
    /// Every listing in this engine goes through here or [`Self::scan_from`], so
    /// the tier-1 plane can be broken down by purpose rather than guessed at.
    fn scan(
        &self,
        purpose: Scan,
        prefix: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        LIST_SCANS.add(1, &[KeyValue::new("purpose", purpose.as_str())]);
        self.object_store.list(Some(prefix))
    }

    /// As [`Self::scan`], but resuming after `start_after` (S3 `start-after`) so
    /// only the tail beyond a known point is read.
    fn scan_from(
        &self,
        purpose: Scan,
        prefix: &Path,
        start_after: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        LIST_SCANS.add(1, &[KeyValue::new("purpose", purpose.as_str())]);
        self.object_store
            .list_with_offset(Some(prefix), start_after)
    }

    /// As [`Self::scan`], but delimited (S3 `delimiter=/`, common prefixes only).
    async fn scan_delimited(
        &self,
        purpose: Scan,
        prefix: &Path,
    ) -> Result<ListResult, object_store::Error> {
        LIST_SCANS.add(1, &[KeyValue::new("purpose", purpose.as_str())]);
        self.object_store.list_with_delimiter(Some(prefix)).await
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

    /// The coalesce linger with per-flush random jitter (±20%, within the #91
    /// 10–25% guidance). Independent pods — and successive windows on one pod —
    /// draw uncorrelated flush instants instead of staying phase-aligned, so
    /// they stop racing the create of the *same* next segment name; on GCS that
    /// collision returns a 429 and burns a conflict-retry. Same desync trick as
    /// [`throttle_backoff`].
    fn jittered_linger(&self) -> Duration {
        let base_ms = self.coalesce_linger.as_millis().min(u128::from(u64::MAX)) as u64;
        let span = base_ms / 5; // ±20%
        let jitter = rng().random_range(0..=2 * span);
        Duration::from_millis(base_ms.saturating_sub(span) + jitter)
    }

    /// The durable sequence-floor object for `prefix` (#77).
    fn seq_floor_location(&self, prefix: &str) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/seq-floor.json",
            self.cluster, prefix,
        ))
    }

    /// Read the persisted next-sequence floor for `prefix` (#77); `0` when absent.
    async fn read_seq_floor(&self, prefix: &str) -> Result<u64> {
        match self
            .object_store
            .get(&self.seq_floor_location(prefix))
            .await
        {
            Ok(result) => {
                Ok(serde_json::from_slice::<SeqFloor>(&result.bytes().await?)?.next_seq_floor)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    /// The persisted next-sequence floor for `prefix`, *certified against the
    /// current index generation*: served from the in-memory prefix index when
    /// it was read at-or-after the listing the current generation stands for,
    /// otherwise re-read with one GET (per prefix, not per partition) and
    /// cached under that generation.
    ///
    /// Why this certifies the LATEST fast path: the floor is raised
    /// write-ahead of *every* segment delete
    /// ([`Self::raise_seq_floor`] call sites in `expire_prefix_segments` and
    /// `compact_prefix_segments`), and `expire_prefix_segments` persists each
    /// affected sub-stream's tail into `watermark.high` *before* that raise.
    /// So `watermark.high` of a prefix-coalesced sub-stream can only advance
    /// in an operation that subsequently raises this floor. A floor value read
    /// after our latest listing therefore covers every watermark advance whose
    /// segment deletion that listing could have reflected — if the floor has
    /// not risen since a `watermark.json` read, that read is still the current
    /// high floor, and no per-partition conditional GET is needed.
    ///
    /// Generation-checked commit: the GET is issued after capturing the
    /// generation, and the result is cached only if no listing/prune committed
    /// meanwhile — a stale read can never be certified against a newer view.
    /// The un-cached value is still returned: it is valid for the caller's own
    /// (older-generation) segment snapshot, whose tails a newer prune can only
    /// have kept or removed, never advanced.
    async fn certified_seq_floor(&self, prefix: &str) -> Result<u64> {
        // Lock-free fast path: a floor already certified for the current
        // generation is served from memory.
        if let Some(floor) = self.cached_certified_seq_floor(prefix)? {
            return Ok(floor);
        }

        // Single-flight the sync per prefix (same lock as the index refresh):
        // concurrent stale readers re-check under the lock and are served by
        // the winner's GET instead of issuing N duplicates.
        let sync = self.prefix_read_sync_lock(prefix)?;
        let _guard = sync.lock().await;

        if let Some(floor) = self.cached_certified_seq_floor(prefix)? {
            return Ok(floor);
        }

        let generation = self
            .prefix_index
            .lock()
            .map_err(Into::<Error>::into)?
            .get(prefix)
            .map(|entry| entry.generation)
            .unwrap_or_default();

        let floor = self.read_seq_floor(prefix).await?;

        {
            let mut index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            let entry = index.entry(prefix.to_owned()).or_default();
            // A prune can bump the generation without taking the single-flight
            // lock, so commit only if no such loss happened during the GET — a
            // stale read must never be certified against a newer view. The
            // value is still returned: it is valid for the caller's own
            // segment snapshot.
            if entry.generation == generation {
                entry.seq_floor = Some((floor, generation));
            }
        }

        Ok(floor)
    }

    /// The certified seq floor for `prefix` iff one is cached for the current
    /// index generation (see [`Self::certified_seq_floor`]).
    fn cached_certified_seq_floor(&self, prefix: &str) -> Result<Option<u64>> {
        let index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
        Ok(index.get(prefix).and_then(|entry| {
            entry
                .seq_floor
                .and_then(|(floor, at)| (at == entry.generation).then_some(floor))
        }))
    }

    /// Raise the persisted next-sequence floor for `prefix` to at least `floor`
    /// (#77). MUST be called write-ahead of deleting any segment, so a freed
    /// sequence name is never reused. Max-fold CAS: a lost race means another
    /// worker wrote concurrently — re-read and, if the value already covers our
    /// floor, we are done; otherwise retry. Returns an error on persistent
    /// contention so the caller aborts the delete rather than break the invariant.
    async fn raise_seq_floor(&self, prefix: &str, floor: u64) -> Result<()> {
        const MAX_ATTEMPTS: usize = 16;
        let location = self.seq_floor_location(prefix);

        for _ in 0..MAX_ATTEMPTS {
            let (current, version) = match self.object_store.get(&location).await {
                Ok(result) => {
                    let version = UpdateVersion {
                        e_tag: result.meta.e_tag.clone(),
                        version: result.meta.version.clone(),
                    };
                    let current =
                        serde_json::from_slice::<SeqFloor>(&result.bytes().await?)?.next_seq_floor;
                    (current, Some(version))
                }
                Err(object_store::Error::NotFound { .. }) => (0, None),
                Err(err) => return Err(err.into()),
            };

            if current >= floor {
                return Ok(());
            }

            let payload = PutPayload::from(Bytes::from(serde_json::to_vec(&SeqFloor {
                next_seq_floor: floor,
            })?));
            let mode = match &version {
                Some(version) => PutMode::Update(version.clone()),
                None => PutMode::Create,
            };

            match self
                .object_store
                .put_opts(
                    &location,
                    payload,
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => return Ok(()),
                // Lost the CAS (create race or stale version) — re-read and retry.
                Err(object_store::Error::AlreadyExists { .. })
                | Err(object_store::Error::Precondition { .. }) => continue,
                Err(err) => return Err(err.into()),
            }
        }

        error!(prefix, floor, "seq floor CAS exhausted retries");
        // Retriable: contention exhaustion, not a permanent fault (#6/#129).
        Err(Error::Api(ErrorCode::KafkaStorageError))
    }

    /// The durable era-epoch object for `prefix` (#92).
    fn era_location(&self, prefix: &str) -> Path {
        Path::from(format!(
            "clusters/{}/prefixes/{}/era.json",
            self.cluster, prefix,
        ))
    }

    /// The lease epoch currently recorded in `prefix`'s `lease.json` (#59); `0`
    /// when no lease object exists. Read-only — does not acquire or renew.
    async fn read_lease_epoch(&self, prefix: &str) -> Result<i64> {
        match self.object_store.get(&self.lease_location(prefix)).await {
            Ok(result) => Ok(serde_json::from_slice::<PrefixLease>(&result.bytes().await?)?.epoch),
            Err(object_store::Error::NotFound { .. }) => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    /// The highest `writer_epoch` across `prefix`'s currently-cached segment
    /// footers; `0` when none are known. The leaseless flush force-refreshes the
    /// index (fold-before-claim) immediately before seeding, so at seed time this
    /// reflects every live pre-cutover segment.
    fn max_footer_epoch(&self, prefix: &str) -> Result<i64> {
        Ok(self
            .prefix_index
            .lock()
            .map_err(Into::<Error>::into)?
            .get(prefix)
            .map(|index| {
                index
                    .segments
                    .values()
                    .map(|cached| cached.footer.writer_epoch)
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0))
    }

    /// The leaseless era epoch for `prefix` (#92), seeding it on first use. The
    /// era is `max(lease epoch, max footer epoch) + 1` (never 0), so a leaseless
    /// segment strictly out-epochs every pre-cutover lease-era segment and a
    /// straggler can never win the overlap tie-break. Create-only and cached: the
    /// first writer to seed wins, and any peer racing the same prefix reads and
    /// adopts that value — so all replicas converge on one constant era. Called
    /// on the leaseless flush path *after* the forced index refresh, so
    /// `max_footer_epoch` sees every folded segment.
    async fn seed_era_epoch(&self, prefix: &str) -> Result<i64> {
        if let Some(era) = self
            .era_epochs
            .lock()
            .map_err(Into::<Error>::into)?
            .get(prefix)
            .copied()
        {
            return Ok(era);
        }

        let location = self.era_location(prefix);

        // Already seeded durably (this process is cold, or a peer seeded first)?
        match self.object_store.get(&location).await {
            Ok(result) => {
                let era = serde_json::from_slice::<Era>(&result.bytes().await?)?.era_epoch;
                self.cache_era(prefix, era)?;
                return Ok(era);
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(err.into()),
        }

        let floor = self
            .read_lease_epoch(prefix)
            .await?
            .max(self.max_footer_epoch(prefix)?);
        let era = floor + 1;
        let payload = PutPayload::from(Bytes::from(serde_json::to_vec(&Era { era_epoch: era })?));

        match self
            .object_store
            .put_opts(
                &location,
                payload,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {
                self.cache_era(prefix, era)?;
                debug!(prefix, era, "leaseless era seeded");
                Ok(era)
            }
            // A peer seeded concurrently — adopt the durable value, not ours.
            Err(object_store::Error::AlreadyExists { .. }) => {
                let result = self.object_store.get(&location).await?;
                let era = serde_json::from_slice::<Era>(&result.bytes().await?)?.era_epoch;
                self.cache_era(prefix, era)?;
                Ok(era)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Cache a resolved era epoch (monotonic, like the other hints — the durable
    /// object is immutable, so the value can only ever be the same one).
    fn cache_era(&self, prefix: &str, era: i64) -> Result<()> {
        self.era_epochs
            .lock()
            .map(|mut cache| {
                let entry = cache.entry(prefix.to_owned()).or_default();
                *entry = (*entry).max(era);
            })
            .map_err(Into::into)
    }

    /// Roll `prefix` back to the lease regime (#92): rewrite `lease.json` with an
    /// epoch strictly above the seeded era, so a restarted old lease-holder — on
    /// its next acquire (which bumps `epoch + 1`) — stamps segments that
    /// out-epoch every leaseless-era segment and wins the overlap tie-break. Run
    /// per active prefix during the *reverse* quiesce-and-flip, after the
    /// leaseless fleet has drained and before old pods restart. Returns the lease
    /// epoch written.
    ///
    /// The write is a plain CAS against the current lease version (or a create),
    /// with an already-expired term so the first old pod re-acquires immediately.
    ///
    /// Operationally invoked (see `docs/migration-scos.md`); no in-tree caller
    /// until a migration CLI subcommand wires it, so allow it to stand unused.
    #[allow(dead_code)]
    async fn rollback_prefix_to_lease(&self, prefix: &str) -> Result<i64> {
        let era = self.seed_era_epoch(prefix).await?;
        let epoch = era + 1;
        let location = self.lease_location(prefix);

        let version = match self.object_store.get(&location).await {
            Ok(result) => Some(UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            }),
            Err(object_store::Error::NotFound { .. }) => None,
            Err(err) => return Err(err.into()),
        };

        let lease = PrefixLease {
            epoch,
            holder: format!("{}-rollback", self.writer_id),
            // Expired on purpose: the first restarted lease pod re-acquires at
            // once (bumping to `epoch + 1`), rather than waiting out a term.
            expires_at_ms: 0,
            maintained_at_ms: 0,
        };
        let payload = PutPayload::from(Bytes::from(serde_json::to_vec(&lease)?));
        let mode = match &version {
            Some(version) => PutMode::Update(version.clone()),
            None => PutMode::Create,
        };

        _ = self
            .object_store
            .put_opts(
                &location,
                payload,
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await?;
        debug!(prefix, epoch, era, "prefix rolled back to lease regime");
        Ok(epoch)
    }

    /// The next free segment sequence for `prefix`, read from the tail of the
    /// `segments/` listing (#57). Zero-padded names sort lexicographically by
    /// sequence, so the greatest listed name is the tail. Used to seed the hint
    /// cold and to resync after a `Create` conflict.
    async fn tail_next_seq(&self, prefix: &str) -> Result<u64> {
        let listing = self.segment_prefix(prefix);
        let mut list_stream = self.scan(Scan::SegmentIndex, &listing);
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

        // Fold in the persisted floor (#77): a sequence name freed by
        // retention/compaction must never be reused, even when the surviving
        // listing's max has dropped below it (or the prefix listed empty).
        let floor = self.read_seq_floor(prefix).await?;
        Ok(max.map_or(0, |m| m + 1).max(floor))
    }

    /// The next free segment sequence for `prefix`, derived from the *already
    /// force-folded* in-memory index instead of a fresh LIST (#91). The leaseless
    /// flush (`fold-before-claim`) calls [`Self::refresh_prefix_index_forced`]
    /// immediately before this, so the cached segment set already reflects every
    /// live sequence — including a peer replica's seconds-old create. The full
    /// `tail_next_seq` LIST per conflict attempt was therefore redundant: it
    /// re-read what the forced refresh had just listed. Still folds the persisted
    /// seq floor (#77) so a name freed by retention/compaction is never reused.
    ///
    /// Derived from every sequence the listing *resolved* — decoded segments and
    /// undecodable objects alike ([`PrefixIndex::resolved_max`]) — because a
    /// candidate must be free in the **namespace**, not merely absent from the
    /// readable set: an occupied-but-undecodable name would otherwise be re-picked
    /// on every attempt and burn the whole create-CAS budget (#157). This matches
    /// the name-derived [`Self::tail_next_seq`] the leased/compaction path uses.
    async fn tail_next_seq_folded(&self, prefix: &str) -> Result<u64> {
        let listed_max = self
            .prefix_index
            .lock()
            .map_err(Into::<Error>::into)?
            .get(prefix)
            .and_then(PrefixIndex::resolved_max);
        let floor = self.read_seq_floor(prefix).await?;
        Ok(listed_max.map_or(0, |m| m + 1).max(floor))
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
        role: SegmentCreateRole,
    ) -> Result<u64> {
        /// Bounds the conflict-resync loop; far above any real contention.
        const MAX_ATTEMPTS: usize = 64;

        let attributes = [KeyValue::new("role", role.as_str())];
        let payload_len = payload.content_length() as u64;

        // Conflict accounting for the exhaustion terminal and for #130: how often
        // this role loses the shared tail-sequence race, and how many payload
        // bytes that costs in re-uploads.
        let mut conflicts = 0u64;

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
                    SEGMENT_CREATES.add(1, &attributes);
                    self.set_seq(prefix, candidate + 1)?;
                    return Ok(candidate);
                }

                Err(object_store::Error::AlreadyExists { .. }) => {
                    // A seq conflict just means another writer took that
                    // sequence: resync and retry. There is no lease to
                    // re-validate since #177 — the leaseless arbiter (#86) makes
                    // the create-only CAS itself the arbiter, so a loser is
                    // never a fenced writer, only a slower one.
                    // Every loss costs a re-LIST and a re-upload of the whole
                    // payload into the same key prefix (#130).
                    conflicts += 1;
                    SEGMENT_CREATE_CONFLICTS.add(1, &attributes);
                    SEGMENT_CREATE_BYTES_REWRITTEN.add(payload_len, &attributes);

                    debug!(candidate, attempt, prefix, "segment seq taken, resyncing");
                    candidate = self.tail_next_seq(prefix).await?;
                }

                Err(err) => return Err(err.into()),
            }
        }

        error!(
            prefix,
            candidate,
            role = role.as_str(),
            conflicts,
            payload_len,
            bytes_rewritten = conflicts * payload_len,
            "segment sequence assignment exhausted retries"
        );
        // Retriable: contention exhaustion, not a permanent fault (#6/#129).
        Err(Error::Api(ErrorCode::KafkaStorageError))
    }

    /// Read a segment's self-describing footer (#58/#64) with at most two ranged
    /// GETs of the object tail — never the record body: one `Suffix` GET of the
    /// fixed trailer to learn the footer length, then one `Suffix` GET of the
    /// footer + trailer. Returns `None` if the object carries no trailer (a
    /// legacy #50 object). This is the read primitive the fetch path (#60) also
    /// builds on.
    async fn read_segment_footer(&self, location: &Path) -> Result<Option<SegmentFooter>> {
        // One speculative suffix GET covers the trailer and, for almost every
        // segment, the whole footer too (#112 follow-up) — halving the per-footer
        // GETs the read/refresh path pays on non-writer replicas. `decode_segment_footer`
        // reads the trailer from the end of the buffer and slices the footer just
        // before it, so leading record bytes in the over-read are ignored.
        let over_read = SEGMENT_FOOTER_OVER_READ.max(SEGMENT_TRAILER_LEN);
        let buffer = self
            .object_store
            .get_opts(
                location,
                GetOptions {
                    range: Some(GetRange::Suffix(over_read as u64)),
                    ..Default::default()
                },
            )
            .await?
            .bytes()
            .await?;

        if buffer.len() < SEGMENT_TRAILER_LEN {
            return Ok(None);
        }

        let trailer = &buffer[buffer.len() - SEGMENT_TRAILER_LEN..];
        let magic = u32::from_be_bytes(trailer[14..18].try_into()?);
        if magic != SEGMENT_MAGIC {
            return Ok(None);
        }

        let footer_len = u64::from_be_bytes(trailer[0..8].try_into()?) as usize;

        // Fast path: the over-read already holds the whole `[footer || trailer]`.
        if SEGMENT_TRAILER_LEN + footer_len <= buffer.len() {
            return self.decode_segment_footer(&buffer);
        }

        // Rare: a footer larger than the over-read (a prefix with very many
        // sub-streams). Fetch exactly the `[footer || trailer]` suffix.
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
        self.refresh_prefix_index_inner(prefix, false).await
    }

    /// Refresh the prefix index unconditionally, bypassing the TTL freshness
    /// gate (#86). The leaseless write path (`fold-before-claim`) must observe
    /// every live segment — including a peer replica's seconds-old write — before
    /// it derives offsets and claims a sequence, or two writers could stamp the
    /// same offset. The TTL'd [`Self::refresh_prefix_index`] is fine for the read
    /// path (bounded staleness), but not for the offset-assignment authority.
    async fn refresh_prefix_index_forced(&self, prefix: &str) -> Result<()> {
        self.refresh_prefix_index_inner(prefix, true).await
    }

    /// Whether the cached prefix index is within its freshness TTL, plus the
    /// incremental-listing watermark (highest known sequence).
    fn prefix_index_freshness(&self, prefix: &str) -> Result<(bool, Option<u64>)> {
        let index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
        Ok(match index.get(prefix) {
            Some(entry) => {
                let fresh = entry.refreshed_at.is_some_and(|at| {
                    SystemTime::now()
                        .duration_since(at)
                        .is_ok_and(|elapsed| elapsed < Self::HIGH_WATERMARK_HINT_TTL)
                });
                (fresh, entry.resolved_max())
            }
            None => (false, None),
        })
    }

    /// The per-prefix single-flight lock for the real index refresh and the
    /// certified seq-floor sync (see [`Self::prefix_read_sync_locks`]).
    fn prefix_read_sync_lock(&self, prefix: &str) -> Result<Arc<tokio::sync::Mutex<()>>> {
        self.prefix_read_sync_locks
            .lock()
            .map_err(Into::into)
            .map(|mut locks| locks.entry(prefix.to_owned()).or_default().clone())
    }

    /// Follow a prefix's segment tail with ranged GETs instead of a
    /// `ListObjectsV2` (#112), proving there is nothing left to discover rather
    /// than guessing.
    ///
    /// **The proof.** A segment is only ever created at
    /// `max(known tail + 1, seq floor)` — by the leaseless arbiter
    /// ([`Self::tail_next_seq_folded`]), by the lease-mode writer and by compaction
    /// ([`Self::tail_next_seq`]). So created names are contiguous except where the
    /// durable floor jumps (#77, raised write-ahead of every delete) or where a
    /// name is occupied but unresolvable. Therefore, if `segments/{cursor + 1}` is
    /// **absent** and the floor read *after* observing that absence is
    /// `<= cursor + 1`, no segment can exist above `cursor`:
    ///
    /// - a segment at `S > cursor + 1` would have been created either at
    ///   `tail + 1` — which forces a segment at every seq down to `cursor + 1`,
    ///   contradicting the absence — or at a floor `>= S > cursor + 1`, which
    ///   (the floor being monotonic) our later read could not have seen as
    ///   `<= cursor + 1`;
    /// - an occupied-but-unresolvable name would answer the probe with an object,
    ///   not a 404.
    ///
    /// Object stores are read-after-write consistent, so the 404 is authoritative
    /// at that instant. Reading the floor *after* the absence is what makes the
    /// argument work, hence the ordering below. A segment created *after* both
    /// reads is not missed by this any more than by a LIST — that staleness window
    /// is the same one the TTL and the create-CAS already cover.
    ///
    /// Anything the proof does not cover returns [`TailProbe::Inconclusive`] and
    /// the caller LISTs: a cold index (no cursor), a floor ahead of the tail (which
    /// is exactly the "segments above our cursor were deleted" case), an
    /// unresolvable footer, an oversized footer, more new segments than the probe
    /// window, or any non-404 error.
    async fn probe_prefix_tail(&self, prefix: &str, cursor: u64, path: &'static str) -> TailProbe {
        /// Consecutive new segments the probe will fold before deferring to a
        /// LIST. A reader this far behind is better served by one tier-1 request
        /// than by a growing chain of tier-2 ones.
        const PROBE_WINDOW: u64 = 4;

        let mut folded = 0;

        for seq in (cursor + 1)..=(cursor + PROBE_WINDOW) {
            let location = self.segment_location(prefix, seq);

            let (bytes, last_modified_ms) = match self
                .object_store
                .get_opts(
                    &location,
                    GetOptions {
                        range: Some(GetRange::Suffix(
                            SEGMENT_FOOTER_OVER_READ.max(SEGMENT_TRAILER_LEN) as u64,
                        )),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(result) => {
                    let last_modified_ms = result.meta.last_modified.timestamp_millis();
                    match result.bytes().await {
                        Ok(bytes) => (bytes, last_modified_ms),
                        Err(error) => {
                            debug!(?error, prefix, seq, "tail probe body");
                            return TailProbe::Inconclusive("probe_error");
                        }
                    }
                }

                // Absent: the tail is at `cursor` if the floor agrees. The floor is
                // read *after* the absence, and fresh — see the proof above and
                // [`Self::probe_seq_floor`].
                Err(object_store::Error::NotFound { .. }) => {
                    let floor = match self.probe_seq_floor(prefix).await {
                        Ok(floor) => floor,
                        Err(error) => {
                            debug!(?error, prefix, seq, "tail probe floor");
                            return TailProbe::Inconclusive("probe_error");
                        }
                    };

                    if floor > seq {
                        // Names above our cursor were freed by retention or
                        // compaction, so absence proves nothing — LIST.
                        return TailProbe::Inconclusive("floor_ahead");
                    }

                    let outcome = if folded == 0 {
                        "up_to_date"
                    } else {
                        "extended"
                    };
                    PREFIX_TAIL_PROBES.add(
                        1,
                        &[
                            KeyValue::new("path", path),
                            KeyValue::new("outcome", outcome),
                        ],
                    );

                    // Only `refreshed_at` is stamped: unlike a listing, a probe can
                    // only *add* segments, never reflect a peer's deletion, so the
                    // index generation — and with it the certified seq floor that
                    // keeps the LATEST fast path off `watermark.json` — stays valid.
                    if let Ok(mut index) = self.prefix_index.lock() {
                        index.entry(prefix.to_owned()).or_default().refreshed_at =
                            Some(SystemTime::now());
                    }

                    return TailProbe::Resolved;
                }

                Err(error) => {
                    debug!(?error, prefix, seq, "tail probe");
                    return TailProbe::Inconclusive("probe_error");
                }
            };

            // The over-read carries the footer for all but a pathologically wide
            // prefix; anything else is the LIST path's business.
            match self.decode_segment_footer(&bytes) {
                Ok(Some(footer)) => {
                    if let Ok(mut index) = self.prefix_index.lock() {
                        _ = index.entry(prefix.to_owned()).or_default().segments.insert(
                            seq,
                            CachedSegment {
                                footer,
                                last_modified_ms,
                            },
                        );
                    }
                    folded += 1;
                }

                Ok(None) => return TailProbe::Inconclusive("undecodable"),
                Err(error) => {
                    debug!(?error, prefix, seq, "tail probe footer");
                    return TailProbe::Inconclusive("oversized_footer");
                }
            }
        }

        TailProbe::Inconclusive("window_exhausted")
    }

    /// The seq floor for the tail proof (#112), **always read fresh** — the one
    /// place the certified cache ([`Self::certified_seq_floor`]) must not be used.
    /// A peer can raise the durable floor without bumping our index generation, so
    /// a certified value can be stale-low; everywhere else that only *understates*
    /// a watermark (delaying visibility, never corrupting), but here it would make
    /// `floor <= seq` hold when it does not, which is precisely the direction that
    /// would let the probe miss a segment created at a raised floor. The proof
    /// needs a floor read ordered *after* the observed absence, and only a live GET
    /// gives that.
    ///
    /// The fresh value is certified under the current generation on the way out, so
    /// the read also serves the LATEST fast path rather than being pure overhead.
    /// Inlined rather than calling `certified_seq_floor` because the probe already
    /// holds the per-prefix single-flight lock that method takes.
    async fn probe_seq_floor(&self, prefix: &str) -> Result<u64> {
        let generation = self
            .prefix_index
            .lock()
            .map_err(Into::<Error>::into)?
            .get(prefix)
            .map(|entry| entry.generation)
            .unwrap_or_default();

        let floor = self.read_seq_floor(prefix).await?;

        {
            let mut index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            let entry = index.entry(prefix.to_owned()).or_default();
            if entry.generation == generation {
                entry.seq_floor = Some((floor, generation));
            }
        }

        Ok(floor)
    }

    async fn refresh_prefix_index_inner(&self, prefix: &str, force: bool) -> Result<()> {
        // Lock-free fast path: a fresh index is served without touching the
        // single-flight lock, so TTL-served readers never contend.
        if !force && self.prefix_index_freshness(prefix)?.0 {
            return Ok(());
        }

        // Single-flight the real listing per prefix: concurrent stale readers
        // (a wide ListOffsets resolves 32 partitions at once) queue here and
        // re-check, so one LIST serves them all instead of N duplicates.
        let sync = self.prefix_read_sync_lock(prefix)?;
        let _guard = sync.lock().await;

        let (fresh, start_after) = self.prefix_index_freshness(prefix)?;
        if !force && fresh {
            return Ok(());
        }

        let path = if force { "forced" } else { "ttl" };

        // Try to follow the tail with ranged GETs instead of a LIST (#112): a
        // `ListObjectsV2` is a tier-1 request, ~12x the price of the tier-2 GET
        // that both proves the tail *and* returns the new segment's footer. Falls
        // through to the LIST whenever the proof does not hold.
        if let Some(cursor) = start_after {
            match self.probe_prefix_tail(prefix, cursor, path).await {
                // The tail is proven (and anything new is folded): the index is
                // current, with no LIST issued.
                TailProbe::Resolved => return Ok(()),

                // The proof does not hold — fall through to the authoritative LIST.
                TailProbe::Inconclusive(reason) => {
                    PREFIX_INDEX_LISTS.add(
                        1,
                        &[KeyValue::new("path", path), KeyValue::new("reason", reason)],
                    );
                }
            }
        } else {
            PREFIX_INDEX_LISTS.add(
                1,
                &[KeyValue::new("path", path), KeyValue::new("reason", "cold")],
            );
        }

        let listing = self.segment_prefix(prefix);
        let mut stream = match start_after {
            Some(seq) => self.scan_from(
                Scan::SegmentIndex,
                &listing,
                &self.segment_location(prefix, seq),
            ),
            None => self.scan(Scan::SegmentIndex, &listing),
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

        // Fetch not-yet-cached footers CONCURRENTLY and commit each to the index
        // as it arrives (#105). Two properties matter at scale — with compaction
        // disabled a prefix accrues thousands of segments:
        //
        // - **Concurrency.** A sequential footer-per-segment loop is O(#segments)
        //   round-trips; a cold `list_offsets` (LATEST via `high_watermark`,
        //   EARLIEST via `segment_region_start`) then blocked past the 60s client
        //   timeout, stalling the whole read path. `buffered` keeps up to
        //   `FOOTER_FETCH_CONCURRENCY` ranged GETs in flight (as the topic-index
        //   warm does), cutting wall-time ~N×.
        // - **Incremental, in-order commit.** Committing per footer rather than
        //   once at the end means a request the client abandons at its timeout
        //   (dropping this future) still leaves progress cached — so the index
        //   warms across attempts instead of restarting from zero (the
        //   "sustained, not decaying" stall). Ordered `buffered` preserves the
        //   ascending-sequence LIST order, so the committed set stays a
        //   contiguous prefix and the next refresh's `start_after` watermark is
        //   correct even after a partial build.
        const FOOTER_FETCH_CONCURRENCY: usize = 32;

        // Already resolved: decoded segments *and* the undecodable names (#157) —
        // re-GETting a footer that will not decode again costs a request per
        // refresh (per flush, on the forced path) and never makes progress.
        let cached: BTreeSet<u64> = self
            .prefix_index
            .lock()
            .map_err(Into::<Error>::into)?
            .get(prefix)
            .map(|entry| {
                entry
                    .segments
                    .keys()
                    .copied()
                    .chain(entry.opaque.iter().copied())
                    .collect()
            })
            .unwrap_or_default();

        let mut footers = futures::stream::iter(
            discovered
                .into_iter()
                .filter(|(seq, _, _)| !cached.contains(seq)),
        )
        .map(|(seq, location, last_modified_ms)| async move {
            match self.read_segment_footer(&location).await {
                Ok(Some(footer)) => Ok((seq, last_modified_ms, FooterOutcome::Decoded(footer))),
                Ok(None) => Ok((seq, last_modified_ms, FooterOutcome::Undecodable)),
                // Gone between the LIST that discovered it and this GET:
                // concurrent compaction (#66) merged it away, or retention (#61)
                // reclaimed it. Not an integrity fault and not this reader's
                // problem — before #191 the `?` below turned it into a failed
                // index refresh and a raw `ObjectStore(NotFound)` escaping
                // `ListOffsets` all the way to the connection error path.
                Err(Error::ObjectStore(ref inner))
                    if matches!(**inner, object_store::Error::NotFound { .. }) =>
                {
                    Ok((seq, last_modified_ms, FooterOutcome::Vanished))
                }
                Err(error) => Err(error),
            }
        })
        .buffered(FOOTER_FETCH_CONCURRENCY);

        while let Some(result) = footers.next().await {
            let (seq, last_modified_ms, footer) = result?;
            let mut index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            let entry = index.entry(prefix.to_owned()).or_default();
            match footer {
                FooterOutcome::Decoded(footer) => {
                    _ = entry.segments.insert(
                        seq,
                        CachedSegment {
                            footer,
                            last_modified_ms,
                        },
                    );
                }

                // The object holds a sequence but carries no decodable footer, so
                // it can never join the readable set — record the *name* as
                // resolved (#157) so the leaseless arbiter steps over it instead
                // of re-deriving an occupied candidate until its budget is gone,
                // and so the next refresh does not re-GET this footer.
                FooterOutcome::Undecodable => {
                    if entry.opaque.insert(seq) {
                        SEGMENT_FOOTER_UNDECODABLE.add(1, &[]);
                        warn!(
                            prefix,
                            seq,
                            "segment object has no decodable footer; stepping over the sequence"
                        );
                    }
                }

                // Deleted under us (#191). Same bookkeeping as an undecodable
                // name — resolved, unreadable, stepped over — but logged
                // separately: an operator seeing "no decodable footer" for an
                // object that maintenance simply reclaimed would go looking for
                // corruption that is not there. Sequences are never reused
                // (#77), so caching it as resolved is permanent and correct.
                FooterOutcome::Vanished => {
                    if entry.opaque.insert(seq) {
                        SEGMENT_VANISHED_BEFORE_READ.add(1, &[]);
                        debug!(
                            prefix,
                            seq, "segment deleted before its footer could be read; stepping over"
                        );
                    }
                }
            }
        }

        // Whole live set observed: stamp fresh so the TTL fast-path can serve.
        // The listing may reflect another replica's segment deletions (which an
        // incremental list can never re-observe), so bump the generation: the
        // certified seq floor must be re-read at least as recently as this
        // listing before the LATEST fast path may trust the index again.
        {
            let mut index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            let entry = index.entry(prefix.to_owned()).or_default();
            entry.refreshed_at = Some(SystemTime::now());
            entry.generation += 1;
        }
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

    /// When the cached prefix index for `prefix` was last reconciled by a
    /// listing, if this process has one. Used to anchor a high-watermark hint's
    /// freshness clock to when the *segment set* was observed rather than to
    /// `now` (#91): the read path serves the index from a cache that may itself
    /// be up to one TTL old, so stamping `mark_listed` with `now` let cross-pod
    /// staleness compound toward ~2×TTL.
    fn prefix_index_refreshed_at(&self, prefix: &str) -> Option<SystemTime> {
        self.prefix_index
            .lock()
            .ok()
            .and_then(|index| index.get(prefix).and_then(|entry| entry.refreshed_at))
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
    /// A prune removes tail knowledge from this process's view, so it also
    /// bumps the generation: the certified seq floor must be re-read before
    /// the LATEST fast path may trust the index again (see
    /// [`Self::certified_seq_floor`]).
    fn index_prune(&self, prefix: &str, seqs: &[u64]) -> Result<()> {
        self.prefix_index
            .lock()
            .map_err(Into::into)
            .map(|mut index| {
                if let Some(entry) = index.get_mut(prefix) {
                    for seq in seqs {
                        _ = entry.segments.remove(seq);
                    }
                    entry.generation += 1;
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
        let prefix = self.routed_prefix_of(topition).await?;
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
    /// Record one segment-data ranged GET and classify it against what this pod
    /// has recently read from the same object (#117). Measurement only: it never
    /// changes what is fetched, and a poisoned lock or a full trace degrades the
    /// numbers, never the read.
    fn note_segment_data_read(&self, prefix: &str, seq: u64, byte_start: u64, byte_len: u64) {
        SEGMENT_DATA_GETS.add(1, &[]);
        SEGMENT_DATA_BYTES.add(byte_len, &[]);

        let Ok(mut traces) = self.segment_reads.lock() else {
            return;
        };

        let now = SystemTime::now();
        let fresh = |trace: &SegmentReadTrace| {
            now.duration_since(trace.last_read)
                .is_ok_and(|elapsed| elapsed < SEGMENT_READ_TRACE_TTL)
        };
        let range = (byte_start, byte_len);
        let key = (prefix.to_owned(), seq);

        match traces.get_mut(&key) {
            // Read again while still resident-ish: this is the repeat a cache
            // could have served — but only the block cache #117 proposes if the
            // span is identical.
            Some(trace) if fresh(trace) => {
                let overlap = if trace.ranges.contains(&range) {
                    "same_range"
                } else {
                    if trace.ranges.len() < SEGMENT_READ_TRACE_RANGES {
                        trace.ranges.push(range);
                    }
                    "other_range"
                };

                SEGMENT_DATA_GET_REPEATS.add(1, &[KeyValue::new("overlap", overlap)]);
                trace.last_read = now;
            }

            // Stale entry: too long ago to credit a cache with, so restart the
            // object's trace rather than count it as a repeat.
            Some(trace) => {
                trace.ranges.clear();
                trace.ranges.push(range);
                trace.last_read = now;
            }

            None => {
                if traces.len() >= SEGMENT_READ_TRACE_OBJECTS {
                    traces.retain(|_, trace| fresh(trace));

                    if traces.len() >= SEGMENT_READ_TRACE_OBJECTS {
                        traces.clear();
                    }
                }

                _ = traces.insert(
                    key,
                    SegmentReadTrace {
                        ranges: vec![range],
                        last_read: now,
                    },
                );
            }
        }
    }

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

        let prefix = self.routed_prefix_of(topition).await?;

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
                self.note_segment_data_read(&prefix, seq, entry.byte_start, entry.byte_len);
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
                        .map_err(Error::from)
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
                        // Preserve the storage error so it is classified
                        // retriable rather than fatal `-1` (#6/#129).
                        return Err(Error::from(error));
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
        let prefix = self.routed_prefix_of(topition).await?;
        self.refresh_prefix_index(&prefix).await?;
        Ok(self
            .valid_substream_segments(&prefix, topition.topic(), topition.partition())?
            .first()
            .map(|(_, entry)| entry.base_offset))
    }

    /// Whether `topition` still has legacy `records/` objects (#60 hybrid),
    /// memoized with a per-entry TTL so the common case pays no per-fetch LIST.
    /// A `true` result (and a `false` for a not-yet-segment-routed topition) is
    /// held only [`Self::HIGH_WATERMARK_HINT_TTL`] so a drain or a fresh legacy
    /// write is picked up within seconds; a `false` for a segment-routed
    /// topition — whose only legacy is the monotonically-draining `[0, C)` seam
    /// — is held [`Self::LEGACY_ABSENCE_TTL`], removing the dominant residual
    /// per-topition LIST (#109/#110). A legacy write on this process flips the
    /// entry to `true` at once ([`Self::note_legacy_records_present`]).
    async fn has_legacy_records(&self, topition: &Topition) -> Result<bool> {
        if let Some((present, checked_at, ttl)) = self
            .legacy_records_present
            .lock()
            .map_err(Into::<Error>::into)?
            .get(topition)
            .copied()
            && checked_at.elapsed().is_ok_and(|elapsed| elapsed < ttl)
        {
            return Ok(present);
        }

        let prefix = Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/",
            self.cluster, topition.topic, topition.partition
        ));
        let present = self
            .scan(Scan::LegacyProbe, &prefix)
            .next()
            .await
            .transpose()
            .map_err(Error::from)?
            .is_some();

        // A negative result is stable for a topition that is provably
        // segment-routed (prefix-coalesced and already owning a segment): its
        // only legacy objects are the `[0, C)` seam, which retention only
        // shrinks and the segment write path never re-creates. Hold that for the
        // long window.
        //
        // A coalesced topition with no segment yet gets the middle window (#166):
        // it has no more reason to hold legacy objects, but it is the case we have
        // the least evidence about, so its staleness is bounded in minutes rather
        // than the hour. Re-listing it every few seconds — which is what it used
        // to do, for every idle stream and after every restart — was the dominant
        // tier-1 request source in production.
        //
        // A positive result keeps the short TTL: there a drain
        // (`true`->`false`) is expected, and must be seen within seconds.
        let ttl = if present {
            Self::HIGH_WATERMARK_HINT_TTL
        } else if self.segment_region_start(topition).await?.is_some() {
            Self::LEGACY_ABSENCE_TTL
        } else {
            Self::LEGACY_ABSENCE_UNSEGMENTED_TTL
        };

        _ = self.legacy_records_present.lock().map(|mut cache| {
            _ = cache.insert(topition.to_owned(), (present, SystemTime::now(), ttl));
        });

        Ok(present)
    }

    /// Whether `topic`'s `cleanup.policy` is `compact`, memoized for
    /// [`Self::HIGH_WATERMARK_HINT_TTL`] (#113). `produce` needs this on every
    /// batch to choose the coalesce route; the value changes only on a rare
    /// `AlterConfigs`, so serving it from memory keeps the produce hot path off a
    /// per-batch conditional GET of the `topic-metadata/<name>.json` object. A
    /// policy change is observed within the TTL.
    async fn topic_is_compacted(&self, topic: &str) -> Result<bool> {
        if let Some((compacted, checked_at)) = self
            .compacted_topics
            .lock()
            .map_err(Into::<Error>::into)?
            .get(topic)
            .copied()
            && checked_at
                .elapsed()
                .is_ok_and(|elapsed| elapsed < Self::HIGH_WATERMARK_HINT_TTL)
        {
            return Ok(compacted);
        }

        let compacted = self
            .describe_config(topic, ConfigResource::Topic, None)
            .await
            .inspect_err(|err| debug!(?err))?
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

        _ = self.compacted_topics.lock().map(|mut cache| {
            _ = cache.insert(topic.to_owned(), (compacted, SystemTime::now()));
        });

        Ok(compacted)
    }

    /// Record that a legacy `records/` object was just written for `topition` on
    /// this process, flipping the memoized presence to `true` at once so a
    /// following read does not serve a stale "no legacy" from the long negative
    /// window ([`Self::LEGACY_ABSENCE_TTL`]). Called on the legacy create path
    /// (txn/control/compacted/backfill/non-coalesced batches); the segment write
    /// path does not touch `records/` and so does not call this.
    fn note_legacy_records_present(&self, topition: &Topition) -> Result<()> {
        self.legacy_records_present
            .lock()
            .map_err(Into::into)
            .map(|mut cache| {
                _ = cache.insert(
                    topition.to_owned(),
                    (true, SystemTime::now(), Self::HIGH_WATERMARK_HINT_TTL),
                );
            })
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

        // Historic #50 multi-batch objects are still in production buckets, so
        // `offset` may fall inside one; start from the object that contains it
        // so the seek does not skip it. Probed unconditionally since #177: the
        // writer is gone, the objects are not.
        let start_offset = self
            .coalesce_fetch_floor(topition, offset)
            .await?
            .unwrap_or(offset);

        let start_after = Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records/{:0>20}",
            self.cluster, topition.topic, topition.partition, start_offset,
        ));

        let mut list_stream = self.scan_from(Scan::LegacyRecords, &prefix, &start_after);

        let mut bytes = max_bytes as u64;

        while let Some(meta) = list_stream
            .next()
            .await
            .inspect(|meta| debug!(?meta))
            .transpose()
            .inspect_err(|error| error!(?error, ?topition, ?offset, ?max_bytes))
            .map_err(Error::from)?
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
                .map_err(Error::from)?
                .bytes()
                .await
                .inspect_err(|error| error!(?error, location = %meta.location))
                .map_err(Error::from)
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
    /// carries the sub-stream. `0` when neither exists yet. Clamped to the
    /// truncation floor (#176) in both arms: truncated records survive
    /// physically (in shared segments always; in the legacy region on a
    /// replica that has not observed the physical delete), so the oldest
    /// physical base can sit below the logical log start.
    async fn coalesced_earliest_offset(&self, topition: &Topition) -> Result<i64> {
        // `truncate_floor` (not `cached_truncate`): EARLIEST does not pass
        // through the high-watermark slow path that warms the watermark
        // cache, so on a fresh process this is the read that resolves the
        // floor (once — absence is memoized, #161).
        let floor = self.truncate_floor(topition).await?;

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
            let mut stream = self.scan(Scan::LegacyProbe, &records);
            while let Some(meta) = stream
                .next()
                .await
                .transpose()
                .inspect_err(|error| error!(?error, ?topition))
                .map_err(Error::from)?
            {
                if let Some(name) = meta.location.parts().next_back()
                    && name.as_ref().len() >= 20
                    && let Ok(base) = i64::from_str(&name.as_ref()[0..20])
                {
                    return Ok(base.max(floor));
                }
            }
        }

        // Otherwise the oldest segment's base for this sub-stream, from the index.
        Ok(self
            .segment_region_start(topition)
            .await?
            .unwrap_or(0)
            .max(floor))
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

        let prefix = self.routed_prefix_of(topition).await?;
        self.refresh_prefix_index(&prefix).await?;
        Ok(self
            .valid_substream_segments(&prefix, topition.topic(), topition.partition())?
            .last()
            .map(|(_, entry)| entry.max_timestamp)
            .filter(|&ms| ms >= 0)
            .map(|ms| SystemTime::UNIX_EPOCH + Duration::from_millis(ms as u64)))
    }

    /// Resolve one partition's `ListOffsets` entry: the offset (and best-effort
    /// timestamp) for `offset_request`. `stable` maps a topition to its first
    /// unstable offset under read-committed isolation (empty for
    /// read-uncommitted). This is a single entry of [`Storage::list_offsets`],
    /// split out so a request's partitions can be resolved concurrently —
    /// `Ok(None)` means "no entry for this partition" (an unparseable batch
    /// object name), exactly the cases the sequential loop used to `continue`
    /// past without pushing a response.
    async fn list_offset_response(
        &self,
        topition: &Topition,
        offset_request: &ListOffset,
        stable: &BTreeMap<Topition, Offset>,
    ) -> Result<Option<ListOffsetResponse>> {
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
            let timestamp = match self.coalesced_latest_timestamp(topition).await? {
                Some(timestamp) => Some(timestamp),
                // Only the legacy `records/` tail can carry a timestamp when
                // the segment index has none. For a pure-segment coalesced
                // topic there are no legacy objects, so that LIST would scan
                // an empty prefix and return nothing (#113) — skip it (the
                // memo is cheap, #110) and report no timestamp. A hybrid topic
                // still lists for the legacy tail mtime.
                None if !self.has_legacy_records(topition).await? => None,
                None => self
                    .list_batch_offsets(topition)
                    .await?
                    .last_key_value()
                    .map(|(_, meta)| meta.last_modified.into()),
            };

            return Ok(Some(ListOffsetResponse {
                error_code: ErrorCode::None,
                offset: Some(high),
                timestamp,
            }));
        }

        // Prefix-coalesced (#60): EARLIEST is the oldest segment's base
        // offset for this sub-stream, read from the footer index — no
        // `records/` listing (there is none). LATEST already went through
        // `high_watermark` above (footer-aware).
        if *offset_request == ListOffset::Earliest {
            let earliest = self.coalesced_earliest_offset(topition).await?;

            return Ok(Some(ListOffsetResponse {
                error_code: ErrorCode::None,
                offset: Some(earliest),
                timestamp: None,
            }));
        }

        // Prefix-coalesced TIMESTAMP / `offsetsForTimes` (#105): resolve from
        // the footer index — the earliest segment whose newest record
        // timestamp is at/after the target — instead of the legacy `records/`
        // scan below, which for a pure-segment topic finds nothing and wrongly
        // returned offset 0. This is an in-memory scan of the warm index (no
        // per-segment I/O); `None` (→ -1 on the wire) when no record is at or
        // after the target, matching Kafka's "no offset" semantics. A hybrid
        // topic (legacy objects above the segment tail) falls through to the
        // records/ path so those batches are still considered.
        if let ListOffset::Timestamp(target) = offset_request
            && !self.has_legacy_records(topition).await?
        {
            let prefix = self.routed_prefix_of(topition).await?;
            self.refresh_prefix_index(&prefix).await?;
            let target_ms = target
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_millis() as i64)
                .unwrap_or(0);

            let found = self
                .valid_substream_segments(&prefix, topition.topic(), topition.partition())?
                .into_iter()
                .find(|(_, entry)| entry.max_timestamp >= 0 && entry.max_timestamp >= target_ms);

            // A truncated segment survives physically (#176), so the located
            // base offset can sit below the truncation floor — clamp, exactly
            // as EARLIEST does.
            let offset = match &found {
                Some((_, entry)) => {
                    Some(entry.base_offset.max(self.truncate_floor(topition).await?))
                }
                None => None,
            };

            return Ok(Some(ListOffsetResponse {
                error_code: ErrorCode::None,
                offset,
                timestamp: found.as_ref().map(|(_, entry)| {
                    SystemTime::UNIX_EPOCH + Duration::from_millis(entry.max_timestamp as u64)
                }),
            }));
        }

        let location = Path::from(format!(
            "clusters/{}/topics/{}/partitions/{:0>10}/records",
            self.cluster, topition.topic, topition.partition,
        ));

        let mut list_stream = self.scan(Scan::ListOffsets, &location);

        let mut candidate: Option<ObjectMeta> = None;

        while let Some(meta) = list_stream
            .next()
            .await
            .inspect(|meta| debug!(?meta))
            .transpose()
            .inspect_err(|error| error!(?error))
            .map_err(Error::from)?
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

                    ListOffset::Latest if found_offset.is_none_or(|found| meta_offset > found) => {
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
                return Ok(None);
            };

            let offset = i64::from_str(&offset.as_ref()[0..20])?;
            debug!(offset);

            Ok(Some(ListOffsetResponse {
                error_code: ErrorCode::None,
                offset: Some(match offset_request {
                    ListOffset::Latest => offset + 1,
                    _ => offset,
                }),
                timestamp: Some(found.last_modified.into()),
            }))
        } else {
            Ok(Some(ListOffsetResponse {
                error_code: ErrorCode::None,
                offset: Some(0),
                ..Default::default()
            }))
        }
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
            // Stamp the acquire time (#126): for the compaction lease this marks
            // the prefix as maintained now, so a peer skips it for the recency
            // window. Harmless for the produce lease (never read).
            maintained_at_ms: Self::now_ms(),
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

    /// The effective (batch-count, byte) flush triggers for a prefix buffer
    /// (#90). A buffer that has ingested a backfill-class batch relaxes both to
    /// backfill floors — the byte trigger to [`Self::BACKFILL_COALESCE_BYTES`]
    /// and the count trigger past [`Self::COALESCE_MAX_RECORDS`] so it never
    /// fires first — leaving the record cap as the effective limiter. The
    /// folded-in snapshot (#90) then coalesces into a few large segments (~1 PUT
    /// per large batch, bounded `S`, #91) instead of one small segment per
    /// batch. Steady-state CDC keeps the tight `coalesce_batches` /
    /// `coalesce_bytes` for low latency. `max` never lowers an operator's
    /// URL-configured value (#54).
    fn flush_thresholds(&self, backfill: bool) -> (usize, usize) {
        if backfill {
            (
                self.coalesce_batches
                    .max(Self::COALESCE_MAX_RECORDS as usize),
                self.coalesce_bytes.max(Self::BACKFILL_COALESCE_BYTES),
            )
        } else {
            (self.coalesce_batches, self.coalesce_bytes)
        }
    }

    /// Buffer `deflated` for a prefix-coalesced flush and await its assigned
    /// offset (#57). Keyed by the topition's connector prefix, so one buffer
    /// accumulates batches across every topic under the prefix and flushes them
    /// into one shared segment object. (It replaced a per-partition buffer,
    /// deleted with the rest of #50 in #177.) The
    /// idempotent sequence and schema were already validated by `produce`.
    async fn enqueue_prefix_coalesced(
        &self,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        // Routed, not `prefix_of` (#175): a compacted topic's batches buffer —
        // and flush — under its dedicated prefix. This is where the `#113` memo
        // is first consulted when the flag is on (`produce`'s eligibility gate
        // short-circuits past `topic_is_compacted` in that case), so the cost is
        // the same one conditional metadata GET per TTL — just paid here.
        let prefix = self.routed_prefix_of(topition).await?;
        let (ack, offset) = oneshot::channel();
        let span = deflated.last_offset_delta as i64 + 1;
        let size = size_of::<i64>() + size_of::<i32>() + deflated.batch_length.max(0) as usize;
        // A transaction marker flushes the buffer immediately (#174 release B):
        // `txn_end` writes markers sequentially per partition, so parking each
        // on the linger would cost an N-partition commit N × linger; flushing
        // now also shrinks the durable-but-unregistered window. Control-only —
        // transactional *data* batches coalesce normally, or a transactional
        // workload would degrade to one object per batch, the very cost the
        // coalescing exists to avoid. Cheap: commit/abort rate ≪ data rate.
        let control = deflated.is_control();

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
            buffer.backfill |= span >= Self::PREFIX_BACKFILL_MIN_RECORDS;

            let (batches_threshold, bytes_threshold) = self.flush_thresholds(buffer.backfill);
            if buffer.pending.len() >= batches_threshold
                || buffer.bytes >= bytes_threshold
                || buffer.records >= Self::COALESCE_MAX_RECORDS
                || control
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
                let linger = self.jittered_linger();

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

        // The leaseless seq-CAS arbiter (#86) is the only flush since #177: no
        // lease, no fencing epoch, any replica may append to any prefix.
        self.flush_prefix_coalesced_leaseless(prefix, buffer).await
    }

    /// Send `error` to every parked producer in a failed prefix flush (#57) so
    /// each retries; the unflushed batches are never acked.
    fn fail_prefix_flush(buffer: PrefixCoalesceBuffer, error: Error, prefix: &str) {
        error!(?error, prefix, "prefix coalesced flush failed");
        for pending in buffer.pending {
            _ = pending.ack.send(Err(error.clone()));
        }
    }

    /// Leaseless prefix flush (#86): the create-only segment-sequence CAS is the
    /// offset arbiter, so any replica may append to any prefix — no lease, no
    /// fencing epoch, no cross-broker produce forwarding. Fold every live segment (bypassing the
    /// index TTL), derive each sub-stream's base from the folded tail, encode a v2
    /// segment and try to create it at the next free sequence. On a create
    /// conflict a peer won that sequence: fold its footer, re-derive the bases and
    /// re-encode, then retry the next sequence. Contiguity holds because a writer
    /// only ever targets `folded_max + 1`, and each conflict forces it to ingest
    /// the winner before re-deriving.
    ///
    /// An *ambiguous* PUT (our create may have landed before the response was
    /// lost) is disambiguated by the per-flush `nonce` written into the footer
    /// (#89): probe the object at `candidate` and adopt it iff it carries our
    /// nonce, rather than blind-retrying at the next sequence and double-writing
    /// the batch. A peer's footer (or none) means we did not win — fold and
    /// re-derive; a probe that itself errors leaves it unknown, so we fail for a
    /// client retry, which the log-based dedup (#88) makes safe.
    async fn flush_prefix_coalesced_leaseless(&self, prefix: &str, buffer: PrefixCoalesceBuffer) {
        /// Bounds the conflict-correction loop; far above any real concurrency.
        const MAX_ATTEMPTS: usize = 64;

        // Wall-clock budget for the conflict-correction loop (#157), overridable
        // via `flush_max_elapsed`. A writer still losing the create-CAS after
        // this long is amplifying LIST+PUT against a contended prefix with no
        // sign of winning: yield to the producer's own retry (the terminal is
        // retriable, and log-based dedup #88 makes the replay safe) rather than
        // spend the rest of the budget on the bucket.
        let max_elapsed = self.flush_max_elapsed;

        // Per-partition FIFO across two concurrent local flushes of this prefix:
        // the seq-CAS is the cross-writer offset authority, but this lock still
        // keeps a single pod's buffer order == offset order.
        let flush_lock = match self.prefix_flush_lock(prefix) {
            Ok(lock) => lock,
            Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
        };
        let _flush_guard = flush_lock.lock().await;

        // Group pending by topition (arrival order preserved within a topition).
        let mut grouped: BTreeMap<Topition, Vec<usize>> = BTreeMap::new();
        for (index, pending) in buffer.pending.iter().enumerate() {
            grouped
                .entry(pending.topition.clone())
                .or_default()
                .push(index);
        }

        // Per-flush nonce, stamped into the footer (#89 self-recognition).
        let nonce = rng().random::<u64>();

        // Arm attribution for the exhaustion terminal (#157). Production runs at
        // warn level, where the per-attempt `debug!`s below are invisible, so the
        // give-up log must itself say which race was lost — and whether the
        // candidate ever moved: an advancing candidate is genuine multi-writer
        // contention, a stalled one is a sequence this writer cannot see as taken.
        let started = tokio::time::Instant::now();
        let mut conflicts = 0usize;
        let mut ambiguous_lost = 0usize;
        let mut stalled = 0usize;
        let mut last_candidate: Option<u64> = None;

        // Where the budget actually went (#192). `conflicts`/`ambiguous_lost`/
        // `stalled` say why the loop *retried*; without these, a flush starved by
        // one slow PUT and a flush losing races look identical in the log, which
        // is what made #192 read as contention when contention was near zero.
        let mut attempts_made = 0usize;
        let mut slowest_attempt = Duration::ZERO;
        let mut put_elapsed = Duration::ZERO;
        let mut put_bytes = 0u64;
        let mut backoff_elapsed = Duration::ZERO;

        for attempt in 0..MAX_ATTEMPTS {
            // Two departures from a plain `elapsed >= budget` (#192):
            //
            // - Never end the loop before `MIN_FLUSH_ATTEMPTS` real attempts.
            //   Surrendering rejects the produce, and with one attempt costing
            //   seconds a 10s budget otherwise gives up after a single lost race
            //   — yielding to a competitor that, at `conflicts == 1`, may not
            //   exist.
            // - Do not *start* an attempt the slowest observed attempt says
            //   cannot finish inside the budget. Checking only between attempts
            //   let a single attempt overshoot by ~90% (18.4s against 10s), so
            //   the budget bounded nothing. Deliberately not a timeout around the
            //   attempt: cancelling mid-PUT manufactures the ambiguous-create
            //   case the arms below exist to resolve.
            if attempts_made >= Self::MIN_FLUSH_ATTEMPTS {
                let elapsed = started.elapsed();
                if elapsed >= max_elapsed || elapsed + slowest_attempt > max_elapsed {
                    break;
                }
            }

            let attempt_started = tokio::time::Instant::now();
            attempts_made += 1;

            // Fold-before-claim: observe every live segment so the candidate
            // sequence and the derived bases reflect all writers, not a stale view.
            if let Err(error) = self.refresh_prefix_index_forced(prefix).await {
                return Self::fail_prefix_flush(buffer, error, prefix);
            }
            // Derive the candidate from the index the forced refresh just folded
            // — no second LIST per attempt (#91).
            let candidate = match self.tail_next_seq_folded(prefix).await {
                Ok(seq) => seq,
                Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
            };

            if last_candidate == Some(candidate) {
                stalled += 1;
            }
            last_candidate = Some(candidate);

            // Seed / read the leaseless era epoch stamped into this segment
            // (#92). Computed after the fold above, so it out-epochs every
            // pre-cutover lease-era segment; cached, so only the first flush of a
            // prefix pays the seeding round-trip.
            let era = match self.seed_era_epoch(prefix).await {
                Ok(era) => era,
                Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
            };

            // Re-derive each sub-stream's base from the folded index, and
            // classify each idempotent batch against the log-folded
            // `ProducerTable` (#88): only an in-order batch is admitted and
            // consumes a fresh offset; a batch already durable in the log is
            // acked with its original offset (duplicate) and not re-appended; a
            // gap or a fenced epoch is rejected. `outcomes[index]` is what the
            // parked producer is acked with — recomputed every attempt, so a
            // batch that raced in through a peer (folded on the conflict retry)
            // flips to a duplicate acked with the winner's offset, closing the
            // cross-pod dedup window.
            let mut substreams: Vec<(Topition, i64, Vec<deflated::Batch>)> =
                Vec::with_capacity(grouped.len());
            let mut outcomes: Vec<Result<i64>> = vec![Ok(0); buffer.pending.len()];
            let mut advances: Vec<(Topition, i64)> = Vec::with_capacity(grouped.len());

            for (topition, indices) in &grouped {
                let base = match self.leaseless_base(prefix, topition).await {
                    Ok(base) => base,
                    Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
                };

                // Working `ProducerTail`s for this sub-stream: seeded from the
                // fold, then advanced by each batch we admit ahead of another in
                // the same flush (the in-flight reservations).
                let mut tails: BTreeMap<i64, ProducerTail> = BTreeMap::new();

                let mut running = base;
                let mut batches = Vec::with_capacity(indices.len());
                for &index in indices {
                    // Copy the scalars first so no borrow of `buffer` is held
                    // across the fallible fold below.
                    let (producer_id, epoch, base_seq, last_offset_delta, is_idempotent) = {
                        let batch = &buffer.pending[index].batch;
                        (
                            batch.producer_id,
                            batch.producer_epoch,
                            batch.base_sequence,
                            batch.last_offset_delta,
                            batch.is_idempotent(),
                        )
                    };
                    let records = last_offset_delta as i64 + 1;

                    if is_idempotent {
                        let tail = match tails.entry(producer_id) {
                            Entry::Occupied(occupied) => occupied.into_mut(),
                            Entry::Vacant(vacant) => {
                                let folded = match self.producer_tail_folded(
                                    prefix,
                                    topition,
                                    producer_id,
                                ) {
                                    Ok(tail) => tail,
                                    Err(error) => {
                                        return Self::fail_prefix_flush(buffer, error, prefix);
                                    }
                                };
                                vacant.insert(folded)
                            }
                        };

                        match tail.classify(epoch, base_seq) {
                            IdempotentClass::Admit => {
                                let last_seq = base_seq.wrapping_add(last_offset_delta);
                                outcomes[index] = Ok(running);
                                tail.fold(epoch, base_seq, last_seq, running);
                                running += records;
                                batches.push(buffer.pending[index].batch.clone());
                            }
                            IdempotentClass::Duplicate(offset) => {
                                outcomes[index] = Ok(offset);
                            }
                            IdempotentClass::OutOfOrder => {
                                outcomes[index] =
                                    Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber));
                            }
                            IdempotentClass::Fenced => {
                                outcomes[index] = Err(Error::Api(ErrorCode::ProducerFenced));
                            }
                        }
                    } else {
                        outcomes[index] = Ok(running);
                        running += records;
                        batches.push(buffer.pending[index].batch.clone());
                    }
                }

                if !batches.is_empty() {
                    advances.push((topition.clone(), running));
                    substreams.push((topition.clone(), base, batches));
                }
            }

            // Every batch was a duplicate / rejected: ack the resolved outcomes
            // without writing an empty segment or burning a sequence.
            if substreams.is_empty() {
                return Self::ack_leaseless_outcomes(buffer, outcomes);
            }

            // Encode a v3 segment stamped with the leaseless era epoch (#92) and
            // try to create it at `candidate`.
            let (payload, footer) = match self.encode_segment_v3(&substreams, era, nonce) {
                Ok(encoded) => encoded,
                Err(error) => return Self::fail_prefix_flush(buffer, error, prefix),
            };

            let put_started = tokio::time::Instant::now();
            put_bytes += payload.content_length() as u64;
            let put_result = self
                .object_store
                .put_opts(
                    &self.segment_location(prefix, candidate),
                    payload,
                    PutOptions {
                        mode: PutMode::Create,
                        attributes: Attributes::new(),
                        ..Default::default()
                    },
                )
                .await;
            put_elapsed += put_started.elapsed();

            match put_result {
                // Won the sequence — this create is the linearization point.
                Ok(_) => {
                    _ = self
                        .set_seq(prefix, candidate + 1)
                        .inspect_err(|err| debug!(?err));
                    return self
                        .finalize_prefix_flush_leaseless(
                            prefix, candidate, footer, buffer, outcomes, &advances,
                        )
                        .await;
                }

                // A peer took `candidate`: fold it and retry the next free
                // sequence with re-derived bases.
                Err(object_store::Error::AlreadyExists { .. }) => {
                    debug!(prefix, candidate, attempt, "segment seq taken, re-deriving");
                    conflicts += 1;
                    slowest_attempt = slowest_attempt.max(attempt_started.elapsed());
                    FLUSH_CAS_CONFLICTS.add(1, &[]);
                    // Yield briefly, jittered (#157): N replicas flushing this
                    // prefix would otherwise re-LIST and re-PUT in lockstep, both
                    // amplifying requests on the busiest prefix and letting one
                    // writer lose every attempt of its budget to the same peers.
                    let backoff = cas_conflict_backoff(attempt);
                    backoff_elapsed += backoff;
                    tokio::time::sleep(backoff).await;
                    continue;
                }

                // Ambiguous PUT (#89): the create may have landed durably before
                // the transport error. Probe the footer at `candidate` and adopt
                // it iff the nonce is ours — a blind retry at the next sequence
                // would double-write the batch. Our nonce can only exist at a
                // sequence our PUT actually won, so a match is proof the create
                // succeeded. A peer's footer or none → we did not win, fold and
                // re-derive. A probe error leaves it genuinely unknown → fail for
                // a client retry (log-based dedup, #88, dedups the replay).
                Err(error) => {
                    match self
                        .read_segment_footer(&self.segment_location(prefix, candidate))
                        .await
                    {
                        Ok(Some(found)) if found.nonce == nonce => {
                            debug!(
                                prefix,
                                candidate, attempt, "ambiguous PUT adopted via nonce"
                            );
                            _ = self
                                .set_seq(prefix, candidate + 1)
                                .inspect_err(|err| debug!(?err));
                            return self
                                .finalize_prefix_flush_leaseless(
                                    prefix, candidate, footer, buffer, outcomes, &advances,
                                )
                                .await;
                        }
                        // A *peer* created `candidate` while our PUT was
                        // failing: we lost the sequence exactly as in the
                        // AlreadyExists arm (the transport error was moot). Fold
                        // it and retry the next sequence — cheap, not a fault.
                        Ok(Some(_)) => {
                            debug!(
                                prefix,
                                candidate,
                                attempt,
                                ?error,
                                "ambiguous PUT lost to peer, re-deriving"
                            );
                            ambiguous_lost += 1;
                            slowest_attempt = slowest_attempt.max(attempt_started.elapsed());
                            FLUSH_CAS_CONFLICTS.add(1, &[]);
                            let backoff = cas_conflict_backoff(attempt);
                            backoff_elapsed += backoff;
                            tokio::time::sleep(backoff).await;
                            continue;
                        }
                        // Nothing landed at `candidate` (S3 is read-after-write
                        // consistent, so a durable create would be visible): our
                        // PUT genuinely failed — a transient S3 throttle /
                        // transport error, not a lost race. Surface the storage
                        // error so it is classified *retriable* (#6/#129) and the
                        // client backs off and retries (log-based dedup #88 makes
                        // the replay safe), instead of spinning the attempt budget
                        // against a throttling bucket toward a fatal terminal.
                        Ok(None) => {
                            debug!(
                                prefix,
                                candidate,
                                attempt,
                                ?error,
                                "ambiguous PUT did not land, failing retriably"
                            );
                            return Self::fail_prefix_flush(buffer, error.into(), prefix);
                        }
                        Err(probe_error) => {
                            debug!(
                                prefix,
                                candidate,
                                ?error,
                                ?probe_error,
                                "ambiguous PUT unresolved"
                            );
                            return Self::fail_prefix_flush(buffer, error.into(), prefix);
                        }
                    }
                }
            }
        }

        FLUSH_CAS_EXHAUSTED.add(1, &[]);
        // Arm-attributed at error level so production (warn) can tell the modes
        // apart without a debug bump (#157): `conflicts` = peers won the create,
        // `ambiguous_lost` = the PUT was ambiguous and a peer's footer was there,
        // `stalled` = the re-derived candidate did not move (a sequence taken by
        // an object this writer cannot resolve).
        //
        // The second group says where the *time* went (#192), which the first
        // cannot: compare `put_ms` against `elapsed_ms` to separate a flush
        // starved by slow PUTs from one losing races, and `slowest_attempt_ms`
        // against `budget_ms` to see whether the budget could ever have admitted
        // another attempt. `attempts` bounds both — the loop cannot iterate
        // without incrementing one of the three counters above, so a small
        // `attempts` with a large `elapsed_ms` is latency, not contention.
        error!(
            prefix,
            conflicts,
            ambiguous_lost,
            stalled,
            ?last_candidate,
            elapsed_ms = started.elapsed().as_millis(),
            attempts = attempts_made,
            put_ms = put_elapsed.as_millis(),
            put_bytes,
            backoff_ms = backoff_elapsed.as_millis(),
            slowest_attempt_ms = slowest_attempt.as_millis(),
            budget_ms = max_elapsed.as_millis(),
            "leaseless flush exhausted retries"
        );
        // Retriable: exhaustion here is pure create-CAS contention (a transport
        // error fails fast retriably above), so tell the client to back off and
        // retry rather than dropping the batch on a fatal code (#6/#129).
        Self::fail_prefix_flush(buffer, Error::Api(ErrorCode::KafkaStorageError), prefix)
    }

    /// The next offset for `topition` under the leaseless path (#86), derived from
    /// the already force-folded prefix index: the epoch-fenced segment tail folded
    /// with this process's hint, and — only for a cold/drained sub-stream — the
    /// legacy `records/` tail and persisted floor, so an offset is never reused.
    /// `prefix` is the flush's (routed, #175) buffer key, threaded through
    /// rather than re-derived so the base is read from exactly the segment set
    /// the flush is about to append to.
    async fn leaseless_base(&self, prefix: &str, topition: &Topition) -> Result<i64> {
        let segment_tail = self
            .valid_substream_segments(prefix, topition.topic(), topition.partition())?
            .last()
            .map(|(_, entry)| entry.base_offset + entry.record_count)
            .unwrap_or(0);
        let cached = self.cached_high(topition)?.unwrap_or(0);

        // Legacy objects can sit above segments until single-authority routing
        // (#78) folds them in, so always consider the legacy tail — cheap
        // (memoized, and 0 for a pure-segment sub-stream). The persisted floor
        // only matters when nothing else is known (a fully retention-drained
        // sub-stream), so it costs a GET only then.
        let legacy = if self.has_legacy_records(topition).await? {
            self.tail_next_offset(topition, None).await?
        } else {
            0
        };
        let base = segment_tail.max(cached).max(legacy);
        if base > 0 {
            Ok(base)
        } else {
            Ok(self.persisted_high(topition).await.unwrap_or(0))
        }
    }

    /// Build the folded [`ProducerTail`] for `(topition, producer_id)` from the
    /// cached prefix index (#88) — no object requests; the leaseless flush
    /// force-folds the index first. Coordinates fold in log order: segments
    /// ascending by base offset (epoch-deduped by [`Self::valid_substream_segments`]),
    /// and producers in offset order within each segment. Because this is a pure
    /// function of the folded footer set, two replicas that have observed the
    /// same segments derive an identical tail — the property that makes the
    /// dedup state converge across a connection migration.
    ///
    /// Transaction-marker (control) coordinates are skipped (#174): a marker
    /// carries `base_sequence = last_sequence = -1`, so folding it would set
    /// `next_sequence` to `seq_increment(-1) = 0` and mark the tail seen —
    /// misclassifying the producer's genuine next in-order data batch as
    /// `OutOfOrder`. Markers are placement metadata in the footer, never part
    /// of the idempotent sequence stream. Transactional *data* coordinates
    /// carry real sequences and fold normally — they are the dedup authority
    /// for those batches.
    fn producer_tail_folded(
        &self,
        prefix: &str,
        topition: &Topition,
        producer_id: i64,
    ) -> Result<ProducerTail> {
        let mut tail = ProducerTail::default();
        for (_seq, entry) in
            self.valid_substream_segments(prefix, topition.topic(), topition.partition())?
        {
            for pc in &entry.producers {
                // Belt and braces: `base_sequence == -1` also catches any
                // future non-sequenced coordinate that lacks the flag (a v2
                // footer decodes with `flags == 0`).
                if pc.flags & FLAG_CONTROL != 0 || pc.base_sequence == -1 {
                    continue;
                }
                if pc.producer_id == producer_id {
                    tail.fold(
                        pc.producer_epoch,
                        pc.base_sequence,
                        pc.last_sequence,
                        entry.base_offset + pc.offset_delta as i64,
                    );
                }
            }
        }
        Ok(tail)
    }

    /// Leaseless finalization (#88): like [`Self::finalize_prefix_flush`] but acks
    /// each parked producer with its *per-batch* idempotent outcome — the assigned
    /// offset (admitted), the original offset (duplicate), or an
    /// `OutOfOrderSequenceNumber` / `ProducerFenced` error — rather than one
    /// assigned offset for all. Called only after the segment PUT is durable.
    async fn finalize_prefix_flush_leaseless(
        &self,
        prefix: &str,
        seq: u64,
        footer: SegmentFooter,
        buffer: PrefixCoalesceBuffer,
        outcomes: Vec<Result<i64>>,
        advances: &[(Topition, i64)],
    ) {
        _ = self
            .index_insert(prefix, seq, footer, Self::now_ms())
            .inspect_err(|err| debug!(?err));
        SEGMENT_FLUSHES.add(1, &[]);
        debug!(prefix, seq, "leaseless prefix segment flushed");

        // The write is durable — advance each admitted sub-stream's hint.
        for (topition, high) in advances {
            _ = self
                .set_high(topition, *high)
                .inspect_err(|err| debug!(?err));
        }

        Self::ack_leaseless_outcomes(buffer, outcomes);
    }

    /// Ack every parked producer in a leaseless flush with its classified
    /// idempotent outcome (#88): `Ok(offset)` for an admitted or duplicate batch,
    /// `Err(code)` for an out-of-order or fenced one.
    fn ack_leaseless_outcomes(buffer: PrefixCoalesceBuffer, outcomes: Vec<Result<i64>>) {
        for (index, pending) in buffer.pending.into_iter().enumerate() {
            let outcome = outcomes
                .get(index)
                .cloned()
                .unwrap_or_else(|| Err(Error::Api(ErrorCode::UnknownServerError)));
            _ = pending.ack.send(outcome);
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

        let mut list_stream = self.scan_from(Scan::LegacyRecords, &prefix, &start_after);
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
            .scan_delimited(Scan::Group, &prefix)
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

    /// Retention over the legacy per-partition `records/` objects (#49/#71).
    /// Prefix-coalesced data lives in shared segments, not `records/` (this only
    /// drains a hybrid topic's legacy region); the segment half is driven per
    /// prefix, interleaved with compaction under one lease, by
    /// [`Self::maintain_prefix_segments`] (#140) — it used to be a whole pass
    /// appended here, which had to complete before compaction could start.
    async fn policy_delete_records(&self, now: SystemTime) -> Result<u64> {
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

                // An ABSENT `cleanup.policy` means Kafka's default, `delete`
                // — so it expires. This path used to read absent as
                // retain-forever, which made it the only place in the engine
                // that did: `segment_retention_thresholds` has always applied
                // the 7-day default to a policy-less topic, so on a
                // segment-backed deployment (all of production) the data was
                // expiring while this path claimed it would not. #177 makes
                // segments the only layout, so the segment reading is the one
                // that survives; aligning here removes the divergence rather
                // than preserving a promise the engine was not keeping.
                //
                // A policy that is present and compact-only still yields no
                // time expiry — the latest value of a key must survive.
                let expires = match configs
                    .iter()
                    .find(|config| config.name == "cleanup.policy")
                    .and_then(|config| config.value.as_deref())
                {
                    Some(policy) => policy.contains("delete"),
                    None => true,
                };

                if !expires {
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
    /// A segment is also expirable, regardless of age, when every sub-stream
    /// slice in it ends at/below that sub-stream's truncation floor (#176) —
    /// best-effort, from in-process-cached floors only (an unknown floor
    /// defers, never forces, the reclaim). Deletes in bounded `DeleteObjects`
    /// chunks and refreshes the per-prefix skip hint from what survived.
    async fn expire_prefix_segments(&self, prefix: &str, threshold_ms: i64) -> Result<u64> {
        /// Matches the S3 `DeleteObjects` per-request key cap.
        const EXPIRE_DELETE_CHUNK: usize = 1_000;

        self.refresh_prefix_index(prefix).await?;

        // Decide from the cached footers (no per-segment footer GET). A segment
        // is expirable when its newest record across every sub-stream (max
        // footer timestamp, or the object append time when record timestamps are
        // unset) is past the threshold — so a live topic never loses a shared
        // segment — OR when every sub-stream slice in it ends at/below that
        // sub-stream's truncation floor (#176, fully-truncated reclaim).
        //
        // Snapshot the decision inputs under the index lock, then evaluate
        // the floors OUTSIDE it: `cached_truncate` takes the `watermarks` /
        // `truncate_floors` locks, and nesting those under `prefix_index`
        // would set up a lock-order hazard for no benefit.
        let segments_snapshot: Vec<SegmentExpirySnapshot> = {
            let index = self.prefix_index.lock().map_err(Into::<Error>::into)?;

            index
                .get(prefix)
                .map(|entry| {
                    entry
                        .segments
                        .iter()
                        .map(|(seq, cached)| {
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
                            let ends = cached
                                .footer
                                .entries
                                .iter()
                                .map(|e| {
                                    (e.topic.clone(), e.partition, e.base_offset + e.record_count)
                                })
                                .collect();

                            (*seq, age_ms, ends)
                        })
                        .collect()
                })
                .unwrap_or_default()
        };

        let mut expirable: Vec<u64> = Vec::new();
        let mut affected: BTreeSet<(String, i32)> = BTreeSet::new();
        let mut surviving_oldest_ms: Option<i64> = None;

        for (seq, age_ms, ends) in &segments_snapshot {
            // Fully truncated (#176): every sub-stream slice ends at/below its
            // truncation floor, so the segment holds only logically-deleted
            // records — reclaimable regardless of age. Floors come from
            // in-process caches ONLY (`cached_truncate`: zero object requests
            // on this maintain path — re-reading N per-partition watermark
            // objects here is the request class this design exists to kill).
            // Reclaim is best-effort: an unknown floor makes the slice look
            // live and DEFERS reclaim — the safe direction — until this
            // process warms that partition's watermark (its own cold read, or
            // it served the DeleteRecords itself); age-based retention still
            // bounds the physical debt.
            let mut fully_truncated = !ends.is_empty();
            for (topic, partition, end) in ends {
                let tp = Topition::new(topic.as_str(), *partition);
                if self.cached_truncate(&tp)?.is_none_or(|floor| floor < *end) {
                    fully_truncated = false;
                    break;
                }
            }

            if *age_ms < threshold_ms || fully_truncated {
                expirable.push(*seq);
                for (topic, partition, _) in ends {
                    _ = affected.insert((topic.clone(), *partition));
                }
            } else {
                surviving_oldest_ms =
                    Some(surviving_oldest_ms.map_or(*age_ms, |o: i64| o.min(*age_ms)));
            }
        }

        self.record_prefix_oldest_retained(prefix, surviving_oldest_ms)?;

        if expirable.is_empty() {
            return Ok(0);
        }

        // Serialize expiry per prefix with the maintenance (compaction) lease
        // (#115). Every replica runs `maintain`, so without a lease all N race
        // the same expiry: each duplicates the per-sub-stream watermark-floor CAS
        // and the seq-floor CAS, and — because the incremental index refresh
        // never observes deletions below its cached max — a replica that did not
        // perform the delete keeps stale entries and re-attempts `DeleteObjects`
        // on already-gone keys. The compaction lease already gates the only other
        // segment-mutating maintenance op, so reusing it makes retention
        // single-writer per prefix AND serializes it against compaction on the
        // same segments. A replica that does not hold the lease yields here; the
        // holder performs the floor writes, deletes, and prunes its own index.
        //
        // A non-holder still carries ghost index entries for the segments the
        // holder deleted (below its refresh watermark) until a cold rebuild —
        // read-side benign, since `fetch` retries off a refreshed index on a 404
        // — but it no longer drives redundant floor writes or deletes here.
        if self.acquire_compaction_lease(prefix).await.is_err() {
            debug!(prefix, "yielding segment expiry to the lease holder");
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

        // Raise the durable sequence floor past every seq we are about to delete,
        // write-ahead of the delete (#77): a freed sequence name must never be
        // reused, or a peer caching the old footer would serve stale byte ranges
        // against a reborn object. On error, abort before deleting (retry next
        // tick) rather than break the invariant.
        if let Some(max_expirable) = expirable.iter().copied().max() {
            self.raise_seq_floor(prefix, max_expirable + 1).await?;
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

        // Pick the OLDEST contiguous run of at least two segments to merge, up to
        // the target size — but skip any *leading* segment already at/above the
        // target (a prior merge, or a large folded-in backfill segment). Such a
        // segment is effectively done: re-merging it just rewrites ~target_bytes
        // to absorb one small neighbour (R=2 write amplification), and — worse —
        // if the next segment overflowed the target the run would collapse to
        // length one, `compact_prefix_segments` would return `Ok(0)`, and
        // `policy_compact_segments` would treat the prefix as drained while small
        // segments pile up behind the big one, so `S` grows unbounded until
        // retention (#114). A segment at/above the target also *bounds* a run
        // (never merged across), leaving it in place as its own segment. The
        // merged epoch is taken from the fenced view below, not from this raw
        // scan, so the segment epoch is ignored here.
        let (run, max_last_modified): (Vec<u64>, i64) = {
            let mut chosen: Vec<u64> = Vec::new();
            let mut chosen_last_modified = i64::MIN;
            let mut start = 0usize;

            while start < eligible_end {
                // A leading (or intervening) already-target-sized segment is a
                // boundary: leave it alone and seed the run after it.
                if segs[start].3 >= self.prefix_compact_target_bytes {
                    start += 1;
                    continue;
                }

                let mut bytes = 0usize;
                let mut end = start;
                while end < eligible_end
                    && segs[end].3 < self.prefix_compact_target_bytes
                    && (end == start || bytes + segs[end].3 <= self.prefix_compact_target_bytes)
                {
                    bytes += segs[end].3;
                    end += 1;
                }

                if end - start >= 2 {
                    chosen = segs[start..end].iter().map(|(seq, ..)| *seq).collect();
                    chosen_last_modified = segs[start..end]
                        .iter()
                        .map(|(_, _, last_modified, _)| *last_modified)
                        .max()
                        .unwrap_or(i64::MIN);
                    break;
                }

                // A lone small segment wedged between large ones: advance past it.
                start = end.max(start + 1);
            }

            (chosen, chosen_last_modified)
        };
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
                .map_err(Error::from)?;
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
            // Carry the producer coordinates forward (#107). Re-encoding the
            // merged run as v3 re-derives each batch's producer coordinates —
            // flags included (#174) — from the (byte-identical) merged batches,
            // so log-based idempotent dedup (#88) still observes producers
            // whose batches were compacted — a retry of a compacted batch is
            // recognized as a duplicate and acked with its original offset
            // instead of being re-appended. A fresh per-segment nonce (#89) is
            // stamped as on any create.
            let (payload, footer) = {
                let nonce = rng().random::<u64>();
                self.encode_segment_v3(&substreams, merged_epoch.max(0), nonce)?
            };
            let seq = self
                .assign_and_create_segment(prefix, payload, SegmentCreateRole::Compaction)
                .await?;
            self.index_insert(prefix, seq, footer, max_last_modified)?;
            Some(seq)
        };

        // Raise the durable sequence floor past every run seq, write-ahead of the
        // delete (#77). Compaction usually adds a higher merged seq so the listing
        // max is unchanged, but when every run segment is superseded no merged seq
        // is written (`new_seq == None`) and deleting the run *can* lower the
        // listing max — freeing a run name for reuse without the floor.
        if let Some(max_run) = run.iter().copied().max() {
            self.raise_seq_floor(prefix, max_run + 1).await?;
        }

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

    /// Enforce `cleanup.policy=compact` over a compacted topic's dedicated
    /// segment prefix (#175): walking each sub-stream's batches newest first,
    /// drop every record whose key reappears later (and earlier duplicates
    /// within a batch), exactly as the legacy [`Self::compact_partition`] does
    /// over `records/` objects. Returns the number of records removed.
    ///
    /// Deliberately NOT part of [`Self::compact_prefix_segments`]'s run
    /// selection: its `min_segments` (256) / `keep_hot` (16) trigger exists to
    /// bound rewrite amplification on high-flush CDC prefixes and would simply
    /// never fire for a compacted topic holding a handful of segments — the
    /// topic would grow stale versions forever. This pass instead considers
    /// **all** of the prefix's segments every tick, with no size gate and no
    /// hot-tail exemption (the newest segment can hold within-batch
    /// duplicates), and relies on a **dirty-only rewrite guard** for cheap
    /// steady state: a segment is rewritten (create new seq + delete old, under
    /// the compaction lease) only when the transform removed at least one
    /// record — the segment analogue of legacy's `records > 0` in-place guard —
    /// so a clean prefix costs a bounded read walk and zero writes per tick.
    ///
    /// Offsets are load-bearing: a rewritten segment carries the SAME
    /// sub-stream `base_offset`s, and an emptied batch is kept as a header
    /// (records stripped, `last_offset_delta` preserved) rather than dropped,
    /// so the footer's `record_count` — accumulated from `last_offset_delta +
    /// 1` per batch — remains the sub-stream's exact offset span. Both
    /// [`Self::recover_substream_next_offset`] and the overlap resolver's
    /// `covered_to = base + record_count` depend on that: a shrunken span would
    /// admit a not-yet-deleted original alongside its rewrite (duplicates) and
    /// would regress the recovered tail (offset reuse). Kept batch headers also
    /// keep their `max_timestamp` (so `compact,delete` expiry stays
    /// conservative) and their producer coordinates (#88 dedup still observes
    /// producers whose batches were fully compacted).
    ///
    /// Memory: the prefix's segments are held decoded for the walk — bounded by
    /// the size merge's `prefix_compact_target_bytes` fold of the same prefix,
    /// and by O(distinct keys per partition) for `seen`, capped by
    /// `prefix_compact_seen_keys` (over the cap the partition is skipped for
    /// the tick — removal deferred, never corrupted).
    async fn compact_prefix_per_key(&self, prefix: &str) -> Result<u64> {
        self.refresh_prefix_index(prefix).await?;

        // Snapshot `(writer_epoch, last_modified_ms)` per segment and the
        // sub-stream key set from the cached footers — no object requests.
        let mut segments_meta: BTreeMap<u64, (i64, i64)> = BTreeMap::new();
        let mut substream_keys: BTreeSet<(String, i32)> = BTreeSet::new();
        {
            let index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            if let Some(entry) = index.get(prefix) {
                for (seq, cached) in &entry.segments {
                    _ = segments_meta
                        .insert(*seq, (cached.footer.writer_epoch, cached.last_modified_ms));
                    for e in &cached.footer.entries {
                        _ = substream_keys.insert((e.topic.clone(), e.partition));
                    }
                }
            }
        }

        if segments_meta.is_empty() {
            return Ok(0);
        }

        // Single writer per prefix: serialized against retention and the size
        // merge by the same compaction lease (#115). Taken before the read walk
        // so N maintainers do not duplicate the GETs; under the #126 claim the
        // term is already held and this acquire is free.
        if self.acquire_compaction_lease(prefix).await.is_err() {
            debug!(prefix, "yielding per-key compaction to the lease holder");
            return Ok(0);
        }

        // GET each segment once; every byte belongs to the compacted topic
        // (dedicated prefix), so whole objects beat per-sub-stream ranged GETs
        // once the topic has more than one partition.
        let mut objects: BTreeMap<u64, Bytes> = BTreeMap::new();
        for seq in segments_meta.keys() {
            let object = self
                .object_store
                .get(&self.segment_location(prefix, *seq))
                .await
                .inspect_err(|err| error!(?err, prefix, seq))?
                .bytes()
                .await
                .map_err(Error::from)?;
            _ = objects.insert(*seq, object);
        }

        // Per-key transform over the EPOCH-FENCED view (as the size merge): a
        // zombie/overlap region is dropped from any rewrite, never fused in.
        // `outputs[seq]` accumulates every fenced sub-stream's (possibly
        // compacted) region so a dirty segment can be rebuilt whole.
        let mut outputs: BTreeMap<u64, Vec<(Topition, i64, Vec<deflated::Batch>)>> =
            BTreeMap::new();
        let mut dirty: BTreeSet<u64> = BTreeSet::new();
        let mut removed_total: u64 = 0;

        for (topic, partition) in substream_keys {
            let fenced = self.valid_substream_segments(prefix, &topic, partition)?;

            // Decode every fenced region once, newest first — a key kept in a
            // newer region supersedes older copies.
            let mut regions: Vec<(u64, i64, Vec<deflated::Batch>)> =
                Vec::with_capacity(fenced.len());
            for (seq, entry) in fenced.iter().rev() {
                let Some(object) = objects.get(seq) else {
                    continue;
                };
                let start = entry.byte_start as usize;
                let end = start + entry.byte_len as usize;
                if end > object.len() {
                    continue;
                }
                regions.push((
                    *seq,
                    entry.base_offset,
                    self.decode_frame(object.slice(start..end))?,
                ));
            }

            let mut seen: BTreeSet<Bytes> = BTreeSet::new();
            let mut staged: Vec<(u64, i64, Vec<deflated::Batch>, u64)> =
                Vec::with_capacity(regions.len());
            let mut aborted = false;

            'transform: for (seq, base, region) in &regions {
                // Newest batch first within the region too.
                let mut out: Vec<deflated::Batch> = Vec::with_capacity(region.len());
                let mut removed: u64 = 0;
                for batch in region.iter().rev() {
                    // Transaction markers and transactional data are exempt
                    // from the per-key transform (#174 release B routes them
                    // into segments; this pass predates that). A marker's
                    // "key" is its ControlBatch bytes — every commit marker
                    // shares it — so key-dedup would strip older markers,
                    // leaving a read-committed consumer's aborted ranges
                    // unbounded. And this cleaner is abort-unaware: letting an
                    // aborted (or still-open) transactional record's key into
                    // `seen` could remove the only *committed* copy of that
                    // key beneath it — data loss for read-committed readers.
                    // Carry them whole and withhold their keys from `seen`:
                    // conservative (committed transactional data is never
                    // compacted) but safe until the cleaner is
                    // transaction-aware.
                    if batch.is_control() || batch.is_transactional() {
                        out.push(batch.clone());
                        continue;
                    }
                    let compaction = inflated::Batch::try_from(batch)?.compact(&seen)?;
                    seen.extend(compaction.batch.keys());
                    if seen.len() > self.prefix_compact_seen_keys {
                        warn!(
                            prefix,
                            topic,
                            partition,
                            keys = seen.len(),
                            "per-key compaction seen set over cap; skipping this partition for the tick"
                        );
                        aborted = true;
                        break 'transform;
                    }
                    if compaction.records > 0 {
                        removed += compaction.records as u64;
                        out.push(deflated::Batch::try_from(compaction.batch)?);
                    } else {
                        // Untouched: carry the ORIGINAL batch, not a re-encode.
                        out.push(batch.clone());
                    }
                }
                out.reverse();
                staged.push((*seq, *base, out, removed));
            }

            if aborted {
                // Removal is deferred for this partition, but a rewrite dirtied
                // by a SIBLING partition still needs this sub-stream's content:
                // contribute the originals, untouched.
                for (seq, base, region) in regions {
                    outputs.entry(seq).or_default().push((
                        Topition::new(topic.clone(), partition),
                        base,
                        region,
                    ));
                }
                continue;
            }

            for (seq, base, out, removed) in staged {
                if removed > 0 {
                    _ = dirty.insert(seq);
                    removed_total += removed;
                }
                outputs.entry(seq).or_default().push((
                    Topition::new(topic.clone(), partition),
                    base,
                    out,
                ));
            }
        }

        // The dirty-only guard: a clean prefix ends here, having written and
        // deleted nothing — the steady state every tick after convergence.
        if dirty.is_empty() {
            return Ok(0);
        }

        // Rewrite each dirty segment as a new create-only object (create, then
        // delete — never mutate), carrying the original's writer epoch: with an
        // identical offset span the overlap resolver's same-epoch/higher-seq
        // tie-break makes the rewrite win during the write→delete window,
        // exactly as for a #66 merge. `index_insert` keeps the original append
        // time so `compact,delete` retention is not reset by the rewrite.
        for seq in &dirty {
            let Some(substreams) = outputs.get(seq) else {
                continue;
            };
            let (epoch, last_modified) = segments_meta.get(seq).copied().unwrap_or((0, i64::MIN));

            let (payload, footer) = {
                let nonce = rng().random::<u64>();
                self.encode_segment_v3(substreams, epoch.max(0), nonce)?
            };
            let new_seq = self
                .assign_and_create_segment(prefix, payload, SegmentCreateRole::Compaction)
                .await?;
            self.index_insert(prefix, new_seq, footer, last_modified)?;
        }

        // Raise the durable sequence floor past every rewritten seq, write-ahead
        // of the delete (#77): a freed sequence name must never be reused.
        if let Some(max_dirty) = dirty.iter().copied().max() {
            self.raise_seq_floor(prefix, max_dirty + 1).await?;
        }

        let locations: Vec<Path> = dirty
            .iter()
            .map(|seq| self.segment_location(prefix, *seq))
            .collect();
        self.delete_batches(locations).await?;
        let pruned: Vec<u64> = dirty.iter().copied().collect();
        self.index_prune(prefix, &pruned)?;

        SEGMENT_RECORDS_COMPACTED.add(removed_total, &[]);
        debug!(
            prefix,
            removed = removed_total,
            rewritten = dirty.len(),
            "per-key compacted prefix segments"
        );

        Ok(removed_total)
    }

    /// Bytes of legacy batches folded into one carried segment (#175 release 2).
    /// Matches the size merge's target so a carried object is no larger than one
    /// the compactor would produce, and bounds the decoded working set.
    const CARRYOVER_CHUNK_BYTES: usize = 16 * 1024 * 1024;

    /// Drain one segment-routed compacted topic-partition's legacy `records/`
    /// region into segments under its dedicated prefix (#175 release 2), so the
    /// latest-value-per-key state survives the removal of the legacy read paths
    /// (#179). Returns the number of legacy objects retired; decrements `budget`
    /// so one maintenance tick cannot spend an unbounded number of requests.
    ///
    /// **Tail-first, and that is the safety property.** The legacy region is the
    /// hybrid seam `[0, C)`: the read path serves `records/` below `C` and
    /// segments at/above it ([`Self::segment_region_start`]). Carrying the
    /// **highest** offsets first makes the new segment's base the new `C`, so the
    /// surviving legacy region is still `[0, C')` — contiguous from zero. Leftover
    /// objects inside the carried span sit at/above `C'` and are simply ignored.
    /// Carrying the head first would instead leave a gap in the middle of the
    /// legacy region, and [`Self::fetch`] reads a gap as "compacted away" — it
    /// would silently skip live keys rather than error, which for a connector
    /// offsets topic means re-snapshotting its source. Same reason the deletes
    /// below run **descending and serially** instead of through
    /// [`Self::delete_batches`]: every intermediate state must still be a
    /// contiguous `[0, x)`.
    ///
    /// **Verbatim, not compacted.** The batches are re-encoded exactly as they
    /// were. Computing latest-per-key here would be wrong, not merely wasteful:
    /// once routing is on, segment writes may already supersede a legacy key, so
    /// folding the legacy region alone could resurrect a stale value. Release 1's
    /// [`Self::compact_prefix_per_key`] cleans the carried segment on a later
    /// tick, with the full picture.
    ///
    /// **Create before delete, and only what is covered.** The legacy objects go
    /// only after the carried segment is durable *and* the fenced footer proves it
    /// spans them. This is the single path that can lose data, so it is the one
    /// with an assertion rather than a comment. `raise_seq_floor` is deliberately
    /// NOT called: it protects freed *segment sequence names* (#77), and a legacy
    /// delete frees none.
    ///
    /// Idempotent: a re-run after a crash between create and delete finds the span
    /// already covered and takes the delete-only path, so it neither duplicates
    /// nor strands.
    async fn carry_over_legacy(&self, topition: &Topition, budget: &mut usize) -> Result<u64> {
        if *budget == 0 {
            return Ok(0);
        }

        // Memoized (#166): a drained partition costs no request per tick.
        if !self.has_legacy_records(topition).await? {
            return Ok(0);
        }

        let legacy = self.list_batch_offsets(topition).await?;
        if legacy.is_empty() {
            return Ok(0);
        }

        let prefix = self.routed_prefix_of(topition).await?;

        // Serialize against the per-key pass and the size merge over the same
        // prefix, and against peer maintainers, with the existing lease (#115).
        if self.acquire_compaction_lease(&prefix).await.is_err() {
            debug!(prefix, ?topition, "yielding carry-over to the lease holder");
            return Ok(0);
        }

        // Tail-first chunk: highest offsets, bounded by bytes and by the tick's
        // remaining object budget.
        let mut chunk: Vec<(i64, ObjectMeta)> = Vec::new();
        let mut bytes = 0usize;
        for (offset, meta) in legacy.iter().rev() {
            if !chunk.is_empty()
                && (bytes + meta.size as usize > Self::CARRYOVER_CHUNK_BYTES
                    || chunk.len() >= *budget)
            {
                break;
            }
            bytes += meta.size as usize;
            chunk.push((*offset, meta.clone()));
        }
        chunk.reverse();

        let Some(&(base_offset, _)) = chunk.first() else {
            return Ok(0);
        };

        // Decode the chunk in offset order; the batches are carried untouched.
        let mut batches: Vec<deflated::Batch> = Vec::with_capacity(chunk.len());
        for (offset, meta) in &chunk {
            let bytes = self
                .object_store
                .get(&meta.location)
                .await
                .inspect_err(|err| error!(?err, ?topition, offset))?
                .bytes()
                .await
                .map_err(Error::from)?;
            batches.extend(self.decode_frame(bytes)?);
        }

        let span: i64 = batches
            .iter()
            .map(|batch| batch.last_offset_delta as i64 + 1)
            .sum();

        let substreams = vec![(topition.to_owned(), base_offset, batches)];
        // The era epoch matters: `valid_substream_segments` fences by epoch, so a
        // carried segment stamped below the prefix's current era would be
        // invisible — the coverage gate below would (correctly) refuse to delete,
        // and the carry could never complete. Read it the same way the leaseless
        // flush does.
        let (payload, footer) = {
            let era = self.seed_era_epoch(&prefix).await?;
            self.encode_segment_v3(&substreams, era.max(0), rng().random::<u64>())?
        };

        let seq = self
            .assign_and_create_segment(&prefix, payload, SegmentCreateRole::Compaction)
            .await?;

        // `now`, not the legacy objects' age: a carried segment must not be
        // instantly expirable by a `compact,delete` prefix's threshold.
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as i64)
            .unwrap_or(0);
        self.index_insert(&prefix, seq, footer, now_ms)?;

        // Coverage gate: the fenced view must show a segment spanning the whole
        // chunk before a single legacy object is removed. A failure here leaves
        // the carried segment in place and the legacy region intact — the next
        // tick retries, and the overlap resolver keeps exactly one copy readable.
        let covered = self
            .valid_substream_segments(&prefix, topition.topic(), topition.partition())?
            .iter()
            .any(|(_, entry)| {
                entry.base_offset <= base_offset
                    && entry.base_offset + entry.record_count >= base_offset + span
            });
        if !covered {
            error!(
                ?topition,
                prefix,
                seq,
                base_offset,
                span,
                "carried segment does not cover the chunk; deferring deletes"
            );
            return Ok(0);
        }

        // Descending and serial: every intermediate state stays a contiguous
        // `[0, x)` legacy region.
        let mut retired = 0u64;
        for (offset, meta) in chunk.iter().rev() {
            self.object_store
                .delete(&meta.location)
                .await
                .inspect_err(|err| error!(?err, ?topition, offset))?;
            retired += 1;
            *budget = budget.saturating_sub(1);
        }

        // No memo invalidation needed: a positive `has_legacy_records` entry is
        // held only briefly, so a fully drained partition re-probes on its own and
        // then memoizes the absence for the long TTL (#166).
        SEGMENT_CARRYOVER_OBJECTS.add(retired, &[]);
        debug!(
            ?topition,
            prefix, seq, base_offset, span, retired, "carried legacy region into a segment"
        );

        Ok(retired)
    }

    /// Compact segments across every coalesced prefix (#66).
    /// Read the compaction lease object for `prefix` without acquiring it, or
    /// `None` if absent (#126). Used by the maintenance claim to peek the
    /// recency stamp before deciding whether to work the prefix.
    async fn read_compaction_lease(&self, prefix: &str) -> Result<Option<PrefixLease>> {
        let location = self.compaction_lease_location(prefix);
        match self.object_store.get(&location).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Ok(Some(serde_json::from_slice::<PrefixLease>(&bytes)?))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Claim this tick's maintenance work-set, stateless and coordinator-free
    /// (#126). Every maintainer enumerates the full prefix universe (the
    /// non-compacted topics' prefixes ∪ locally-indexed prefixes), shuffles it
    /// with a per-process seed so N replicas sweep in independent orders, and for
    /// each prefix:
    ///
    /// - **recency skip** — if the compaction lease shows it was maintained
    ///   within `maintenance_recency`, a peer (or this replica) just did it, so
    ///   skip without a `segments/` LIST or any work;
    /// - **claim** — otherwise acquire the lease (which stamps `maintained_at_ms`
    ///   and caches the held term, so the two passes' inner acquires are free).
    ///   A win adds the prefix to the returned set; a fenced/lost claim means a
    ///   peer is on it right now, so skip.
    ///
    /// The returned set is the filter both maintenance passes honour. The
    /// per-prefix lease stays the correctness guard: a duplicate claim under a
    /// race is fenced, so this can only over-cover (one wasted lease GET) or
    /// under-cover (deferred to the next tick), never double-work or corrupt.
    /// Recency `0` disables the skip → every maintainer claims every prefix
    /// (single-maintainer behaviour).
    async fn claim_maintenance_prefixes(&self, now_ms: i64) -> Result<BTreeSet<String>> {
        let mut universe: BTreeSet<String> = self
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
            // Compacted topics reach segments only when segment-routed (#175);
            // until then they hold no prefix to maintain. Routed, their
            // dedicated prefix joins the universe so the per-key pass runs
            // under the same claim as every other prefix's maintenance. Since
            // the routing flag was hardwired every compacted topic is routed, so
            // there is no unrouted case left to skip.
            for partition in 0..metadata.topic.num_partitions {
                _ = universe.insert(self.routed_prefix(
                    &Topition::new(metadata.topic.name.clone(), partition),
                    compact,
                ));
            }
        }

        let mut prefixes: Vec<String> = universe.into_iter().collect();
        let mut rng = SmallRng::seed_from_u64(self.maintenance_seed ^ now_ms as u64);
        prefixes.shuffle(&mut rng);

        let recency_ms = self.maintenance_recency.as_millis() as i64;
        let mut owned = BTreeSet::new();
        for prefix in prefixes {
            if recency_ms > 0
                && let Some(lease) = self.read_compaction_lease(&prefix).await?
                && now_ms.saturating_sub(lease.maintained_at_ms) < recency_ms
            {
                continue;
            }
            match self.acquire_compaction_lease(&prefix).await {
                Ok(_) => {
                    _ = owned.insert(prefix);
                }
                Err(Error::Api(ErrorCode::NotLeaderOrFollower)) => {}
                Err(err) => error!(?err, prefix, "maintenance claim"),
            }
        }
        Ok(owned)
    }

    /// The prefixes whose segments this replica should size-merge this tick
    /// (#66): every non-compacted topic's prefix — plus every segment-routed
    /// compacted topic's dedicated prefix (#175) — restricted to this tick's
    /// maintenance claim (#126). Empty when prefix coalescing or compaction is
    /// off. Paired with [`Self::drain_compact_prefix`] by
    /// [`Self::maintain_prefix_segments`].
    async fn compactable_prefixes(&self, owned: Option<&BTreeSet<String>>) -> Result<Vec<String>> {
        if self.prefix_compact_min_segments == 0 {
            return Ok(Vec::new());
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
            // Segment-routed compacted topics (#175) are size-merge candidates
            // too: the per-key pass leaves cleaned small segments and header
            // residues behind, and the byte-identical merge (#66) is what
            // bounds their count.
            for partition in 0..metadata.topic.num_partitions {
                _ = prefix_set.insert(self.routed_prefix(
                    &Topition::new(metadata.topic.name.clone(), partition),
                    compact,
                ));
            }
        }
        // Honour this tick's maintenance claim (#126): only compact prefixes this
        // replica owns. `None` = no sharding (every prefix), the single-maintainer
        // default and the standalone-test path.
        Ok(prefix_set
            .into_iter()
            .filter(|prefix| owned.is_none_or(|owned| owned.contains(prefix)))
            .collect())
    }

    /// The dedicated prefixes of segment-routed compacted topics (#175),
    /// restricted to this tick's maintenance claim — the set
    /// [`Self::maintain_prefix_segments`] runs [`Self::compact_prefix_per_key`]
    /// on every tick. Derived from the topic metadata like the other
    /// maintenance universes (a dedicated maintainer's in-memory index is
    /// empty). Empty unless compacted topics are segment-routed.
    async fn per_key_compact_prefixes(
        &self,
        owned: Option<&BTreeSet<String>>,
    ) -> Result<BTreeSet<String>> {
        let mut prefixes = BTreeSet::new();
        for metadata in self.topics_index().await?.iter() {
            // Compacted only, and deliberately *not* the widened carry-over
            // predicate (#211): the per-key pass keeps one value per key, so
            // running it over a topic that merely retains forever would delete
            // records the operator asked to keep.
            if !Self::topic_configs_are_compacted(&metadata.topic) {
                continue;
            }
            for partition in 0..metadata.topic.num_partitions {
                _ =
                    prefixes.insert(self.routed_prefix(
                        &Topition::new(metadata.topic.name.clone(), partition),
                        true,
                    ));
            }
        }
        Ok(prefixes
            .into_iter()
            .filter(|prefix| owned.is_none_or(|owned| owned.contains(prefix)))
            .collect())
    }

    /// Legacy `records/` objects one maintenance tick may retire across all
    /// compacted topics (#175 release 2). Budgeted by **objects**, not by topics:
    /// the production backlog spans 1 to 898 objects per topic, so "N topics per
    /// tick" bounds nothing. At this budget the measured ~3k-object backlog drains
    /// in one or two ticks while a tick's added request cost stays comparable to
    /// the retention pass it runs beside.
    const CARRYOVER_OBJECT_BUDGET: usize = 2_048;

    /// `retention.ms` at or beyond which time-based expiry can never fire, so
    /// the topic is retain-forever in practice (#211).
    ///
    /// A century. `-1` is the documented spelling, but production carries
    /// `i64::MAX`, and `segment_retention_thresholds` uses `saturating_sub`, so
    /// any retention past the current epoch floors the threshold at zero and
    /// nothing ever expires. Testing against a fixed century rather than
    /// `now_ms` keeps the predicate time-independent.
    const RETAIN_FOREVER_MS: i64 = 100 * 365 * 24 * 60 * 60 * 1_000;

    /// Run [`Self::carry_over_legacy`] across every topic-partition this replica
    /// owns whose legacy region must survive (#175 release 2, widened by #211),
    /// under one shared object budget. Returns the objects retired.
    ///
    /// See [`Self::legacy_region_must_survive`] for which topics those are: the
    /// selection is not "compacted" but "compacted or retain-forever".
    ///
    /// Sequential on purpose: the budget is per tick, so concurrent workers would
    /// have to share it under a lock, and this is one-shot backlog drainage rather
    /// than steady-state work — there is nothing to parallelise for. Per-partition
    /// errors are logged and skip that partition only, as elsewhere in maintenance
    /// (#140): one unreadable object must not strand every other topic's drain.
    async fn carry_over_legacy_topitions(&self, owned: Option<&BTreeSet<String>>) -> Result<u64> {
        let mut budget = Self::CARRYOVER_OBJECT_BUDGET;
        let mut retired = 0u64;

        for metadata in self.topics_index().await?.iter() {
            if !Self::legacy_region_must_survive(&metadata.topic) {
                continue;
            }

            // Which prefix this topic's segments live under, and therefore which
            // maintenance claim covers it. Only a *compacted* topic gets a
            // dedicated prefix (#175); a retain-forever one (#211) shares its
            // connector prefix like any other topic, so assuming `true` below
            // would look for a claim on a prefix that never exists and skip
            // every topic the widened predicate just selected.
            let compacted = Self::topic_configs_are_compacted(&metadata.topic);

            for partition in 0..metadata.topic.num_partitions {
                if budget == 0 {
                    debug!(retired, "carry-over budget spent for this tick");
                    return Ok(retired);
                }

                let topition = Topition::new(metadata.topic.name.clone(), partition);

                // Honour the maintenance claim (#126) on the topic's dedicated
                // prefix, so N maintainers split the backlog instead of racing on
                // the same partition's compaction lease.
                if let Some(owned) = owned
                    && !owned.contains(&self.routed_prefix(&topition, compacted))
                {
                    continue;
                }

                match self.carry_over_legacy(&topition, &mut budget).await {
                    Ok(count) => retired += count,
                    Err(err) => error!(?err, ?topition, "legacy carry-over"),
                }
            }
        }

        Ok(retired)
    }

    /// Drain one prefix to `<= prefix_compact_min_segments` (#66 review fix): a
    /// single run per tick cannot keep up with a high flush rate, so loop until
    /// compaction finds nothing more to merge. Each call re-lists, so `S`
    /// converges to the trigger threshold within the tick. Errors are logged and
    /// end this prefix's drain only — one bad prefix must never abort the others'
    /// maintenance (#140).
    async fn drain_compact_prefix(&self, prefix: &str) -> u64 {
        /// Bounds the drain loop so a pathological prefix can't monopolize a
        /// maintenance tick; far above the runs a real backlog needs.
        const MAX_RUNS_PER_PREFIX: usize = 4_096;

        let mut compacted = 0;

        for _ in 0..MAX_RUNS_PER_PREFIX {
            match self
                .compact_prefix_segments(prefix)
                .await
                .inspect_err(|err| error!(?err, prefix))
            {
                Ok(0) | Err(_) => break,
                Ok(n) => compacted += n,
            }
        }

        // Report the live segment count so runaway `S` is observable even if the
        // drain can't keep up.
        if let Ok(index) = self.prefix_index.lock()
            && let Some(entry) = index.get(prefix)
        {
            SEGMENTS_LIVE.record(entry.segments.len() as u64, &[]);
        }

        compacted
    }

    /// Per-prefix segment maintenance — retention **then** compaction for each
    /// prefix, several prefixes at a time (#140). Replaces running the two as
    /// whole sequential passes, which had three compounding failure modes on a
    /// high-fan-out workload:
    ///
    /// - **Retention starved compaction.** `policy_delete` ran to completion
    ///   before `policy_compact_segments` started, so a large delete backlog
    ///   (~100k delete-ops after a restart) consumed the whole run budget and
    ///   compaction never executed — the bounded run (#131) cancelled the tick
    ///   first (observed: run-timeout fired 18×, zero completions). Interleaving
    ///   per prefix means a cancelled run has done *both* for a subset of
    ///   prefixes instead of *one* for none of them.
    /// - **Per-maintainer throughput was one prefix at a time.** A prefix is
    ///   compacted only by the replica holding its lease, so adding maintainer
    ///   pods cannot help a prefix that is already owned (3 → 8 replicas did not
    ///   drain the busiest prefixes). Concurrency *within* a maintainer is the
    ///   lever that does.
    /// - **Traversal order starved the prefixes that needed it most.** Ordered
    ///   largest-known-`S` first, so the prefixes furthest over the trigger are
    ///   drained before a timeout can cut the run, instead of by prefix name.
    ///
    /// Retention and compaction of one prefix stay strictly sequential: both
    /// mutate that prefix's segment set and are serialized by the same compaction
    /// lease (#115), so running them concurrently would make each yield to the
    /// other. Different prefixes hold different leases, so the fan-out is safe.
    /// A per-prefix error is logged and skips that prefix only.
    async fn maintain_prefix_segments(
        &self,
        now_ms: i64,
        owned: Option<&BTreeSet<String>>,
    ) -> Result<(u64, u64)> {
        /// Prefixes maintained concurrently per maintainer. Deliberately small:
        /// each in-flight prefix can hold a merged payload of up to
        /// `prefix_compact_target_bytes` (16 MiB) plus the segments being merged,
        /// and a maintainer runs in a modest memory budget. Raises per-maintainer
        /// drain throughput ~K× without more pods.
        const PREFIX_MAINTENANCE_CONCURRENCY: usize = 4;

        let thresholds = self.segment_retention_thresholds(now_ms, owned).await?;
        let compactable: BTreeSet<String> = self
            .compactable_prefixes(owned)
            .await?
            .into_iter()
            .collect();
        let per_key = self.per_key_compact_prefixes(owned).await?;

        // Drain frozen legacy regions before the per-prefix work (#175 release 2).
        // Sequential and separately budgeted rather than folded into the
        // concurrent per-prefix jobs below: the budget is per *tick*, and sharing
        // a counter across concurrent tasks would need a lock for no benefit —
        // the carry-over is a one-shot backlog, not steady-state work.
        _ = self
            .carry_over_legacy_topitions(owned)
            .await
            .inspect_err(|err| error!(?err, "legacy carry-over"));

        // Largest known live-segment count first (free: it is what this process
        // already has cached, no extra request). A prefix this maintainer has
        // never indexed sorts last — it has no known backlog.
        let live_counts: BTreeMap<String, usize> = self
            .prefix_index
            .lock()
            .map_err(Into::<Error>::into)?
            .iter()
            .map(|(prefix, entry)| (prefix.clone(), entry.segments.len()))
            .collect();

        let mut prefixes: Vec<String> = thresholds
            .keys()
            .chain(compactable.iter())
            .chain(per_key.iter())
            .cloned()
            .collect::<BTreeSet<String>>()
            .into_iter()
            .collect();
        prefixes.sort_by_key(|prefix| Reverse(live_counts.get(prefix).copied().unwrap_or(0)));

        let outcomes = futures::stream::iter(prefixes.into_iter().map(|prefix| {
            let thresholds = &thresholds;
            let compactable = &compactable;
            let per_key = &per_key;

            async move {
                let deleted = match thresholds.get(&prefix) {
                    Some(&threshold_ms) => self
                        .expire_prefix_segments_if_due(&prefix, threshold_ms)
                        .await
                        .unwrap_or(0),
                    None => 0,
                };

                // Per-key compaction of a compacted topic's dedicated prefix
                // (#175), every tick, BEFORE the size merge so the merge folds
                // the cleaned small residues rather than stale versions. Its
                // count is records removed (reported via the counter), not
                // segments merged, so it is not folded into `compacted`. A
                // per-prefix error is logged and skips this prefix only, as
                // for the drain (#140).
                if per_key.contains(&prefix) {
                    _ = self
                        .compact_prefix_per_key(&prefix)
                        .await
                        .inspect_err(|err| error!(?err, prefix));
                }

                let compacted = if compactable.contains(&prefix) {
                    self.drain_compact_prefix(&prefix).await
                } else {
                    0
                };

                (deleted, compacted)
            }
        }))
        .buffer_unordered(PREFIX_MAINTENANCE_CONCURRENCY)
        .fold((0, 0), |(deleted, compacted), (d, c)| async move {
            (deleted + d, compacted + c)
        })
        .await;

        Ok(outcomes)
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
    /// infinite. Compacted topics never reach segments (legacy path at produce)
    /// unless segment-routed (#175): a compact-only topic's dedicated prefix
    /// gets no threshold at all, `compact,delete` gets the topic's own.
    ///
    /// Restricted to this tick's maintenance claim (#126), and paired with
    /// [`Self::expire_prefix_segments_if_due`] by
    /// [`Self::maintain_prefix_segments`].
    async fn segment_retention_thresholds(
        &self,
        now_ms: i64,
        owned: Option<&BTreeSet<String>>,
    ) -> Result<BTreeMap<String, i64>> {
        const DEFAULT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

        let mut retention_by_prefix: BTreeMap<String, i64> = BTreeMap::new();

        for metadata in self.topics_index().await?.iter() {
            let configs = metadata.topic.configs.as_deref().unwrap_or_default();

            // Parse the policy once: `compact` decides both routing (#175) and
            // whether time-based expiry applies at all.
            let policy = configs
                .iter()
                .find(|config| config.name == "cleanup.policy")
                .and_then(|config| config.value.as_deref())
                .unwrap_or_default();
            let compact = policy.contains("compact");

            // Compact-only topics yield NO threshold: their prefix is never
            // time-expired — the latest value of a key must survive
            // indefinitely, and `expire_prefix_segments` is driven purely by
            // this map. `compact,delete` keeps a threshold from the topic's OWN
            // `retention.ms` (its routed prefix is dedicated, #175, so there is
            // no sibling max to fold): whole-segment expiry on the max footer
            // timestamp deleting old latest-values is exactly Kafka's
            // `compact,delete` semantics. Unrouted compacted topics have no
            // segments — skip, exactly as before.
            //
            // An ABSENT `cleanup.policy` falls through to a threshold, which is
            // Kafka's default (`delete`, 7 days) and is the contract the whole
            // engine now follows — see `policy_delete_records`, which was the
            // odd one out until #177 and treated absent as retain-forever.
            if compact && !policy.contains("delete") {
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
                let prefix = self.routed_prefix(
                    &Topition::new(metadata.topic.name.clone(), partition),
                    compact,
                );

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

        Ok(retention_by_prefix
            .into_iter()
            // Honour this tick's maintenance claim (#126): only expire prefixes
            // this replica owns. `None` = no sharding (every prefix).
            .filter(|(prefix, _)| owned.is_none_or(|owned| owned.contains(prefix)))
            .map(|(prefix, retention_ms)| (prefix, now_ms.saturating_sub(retention_ms)))
            .collect())
    }

    /// Expire `prefix`'s segments older than `threshold_ms`, skipping the work
    /// entirely when the oldest-retained hint proves nothing can be past the
    /// threshold yet (#49) — no LIST, no lease round-trip.
    async fn expire_prefix_segments_if_due(&self, prefix: &str, threshold_ms: i64) -> Result<u64> {
        if !self.prefix_maybe_expirable(prefix, threshold_ms)? {
            return Ok(0);
        }

        self.expire_prefix_segments(prefix, threshold_ms)
            .await
            .inspect_err(|err| error!(?err, prefix))
    }

    /// List the batch files of `topition` as a map of base offset to its object
    /// metadata (sorted ascending by offset).
    async fn list_batch_offsets(&self, topition: &Topition) -> Result<BTreeMap<i64, ObjectMeta>> {
        let prefix = self.records_prefix(topition);

        let mut offsets = BTreeMap::new();
        let mut list_stream = self.scan(Scan::LegacyRecords, &prefix);

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
        let mut list_stream = self.scan(Scan::Retention, &prefix);

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
        // TTL.
        if surviving_oldest_ms.is_none() {
            self.record_retention_empty_skip(topition)?;
        }

        if deleted == 0 {
            return Ok(0);
        }

        self.advance_low_watermark(topition, surviving_low).await?;

        Ok(deleted)
    }

    /// Truncate `topition` below `before`: physically delete the legacy
    /// `records/` batch objects whose base offset is below `before` (they are
    /// per-partition objects, safe to remove), and persist the offset as the
    /// sub-stream's durable truncation floor (`watermark.truncate`, #176).
    /// Segment-resident records are NOT rewritten — segments are shared
    /// across sub-streams — they are hidden at read time by the floor and
    /// reclaimed by [`Self::expire_prefix_segments`] once every sub-stream in
    /// a segment is past its floor. `before` of `-1` means the log end offset
    /// (delete everything).
    ///
    /// The floor is **monotonic**: max-folded against any existing floor
    /// inside the watermark CAS (`with_mut` re-applies the closure on
    /// conflict), so a later call with a lower offset cannot regress it — and
    /// the returned log start is the post-fold floor that actually holds, not
    /// the requested `before`.
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

        // One CAS carries both the truncation floor and the legacy log start
        // (`low`, kept in lockstep for pre-floor readers), replacing the
        // previous separate `advance_low_watermark` PUT. Both are max-folded:
        // `low` may already sit above `before` (legacy retention advances it
        // independently) and must not regress.
        let floor = self
            .watermark(topition)?
            .with_mut(&self.object_store, |watermark| {
                let floor = watermark
                    .truncate
                    .map_or(before, |truncate| truncate.max(before));
                watermark.truncate = Some(floor);
                watermark.low = Some(watermark.low.unwrap_or(0).max(floor));
                Ok(floor)
            })
            .await?;

        self.memo_truncate_floor(topition, floor)?;

        // Wake the next maintenance pass for this prefix: the whole-segment
        // reclaim gate (`prefix_maybe_expirable`) is age-based, so without
        // dropping the hint a prefix whose segments are young but now fully
        // truncated (#176) would not be re-examined until age retention fired
        // anyway. Best-effort like the reclaim itself — a peer maintainer
        // keeps its own hint and converges via its normal age-based rescan.
        let prefix = self.routed_prefix_of(topition).await?;
        self.record_prefix_oldest_retained(&prefix, None)?;

        Ok(floor)
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
    #[cfg(test)]
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
    #[cfg(test)]
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
                // Populated when the writer emits v2 (#88); empty on the current
                // v1 write path.
                producers: Vec::new(),
            });
        }

        let footer = SegmentFooter {
            writer_epoch,
            nonce: 0,
            entries,
        };
        let footer_bytes = Self::encode_footer(&footer, SEGMENT_FORMAT_VERSION);

        body.extend_from_slice(&footer_bytes);
        body.extend_from_slice(&(footer_bytes.len() as u64).to_be_bytes());
        body.extend_from_slice(&(footer.entries.len() as u32).to_be_bytes());
        body.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_be_bytes());
        body.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());

        Ok((PutPayload::from(Bytes::from(body)), footer))
    }

    /// Like [`Self::encode_segment`] but emits a **v3** footer (#87/#174): a
    /// per-flush `nonce` plus, per sub-stream, the producer coordinates — with
    /// an attribute-derived `flags` byte — of its idempotent, transactional
    /// and control batches (in region/offset order). Used by every leaseless
    /// write path (#86): the flush, merge compaction (#66) and the per-key
    /// compaction rewrite (#175). The coordinates back log-based idempotent
    /// dedup (#88), the nonce backs ambiguous-PUT adoption (#89), and the
    /// flags make transaction markers and transactional data locatable from
    /// the footer alone (#174) — placement metadata, not commit authority:
    /// LSO/aborted derivation stays a pure `meta.json` function. `offset_delta`
    /// is the batch's offset within its sub-stream so it survives the
    /// conflict-correction re-encode. v3 is stamped unconditionally — the
    /// version follows the writer regime, never the segment's content.
    fn encode_segment_v3(
        &self,
        substreams: &[(Topition, i64, Vec<deflated::Batch>)],
        writer_epoch: i64,
        nonce: u64,
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
            let mut producers = Vec::new();

            for batch in batches {
                // Offset of this batch within the sub-stream, before it is added.
                let offset_delta = record_count as u32;
                body.extend_from_slice(&Bytes::from(batch.clone()));
                // v3 emission rule (#174): a coordinate per idempotent,
                // transactional or control batch. A transaction marker is not
                // idempotent (`base_sequence == -1`) yet must be indexed: its
                // coordinate carries its real producer_id/epoch, the -1
                // sequences, and `flags = 0b11` — and is never folded into
                // the producer tail (see `producer_tail_folded`).
                if batch.is_idempotent() || batch.is_transactional() || batch.is_control() {
                    let mut flags = 0u8;
                    if batch.is_transactional() {
                        flags |= FLAG_TRANSACTIONAL;
                    }
                    if batch.is_control() {
                        flags |= FLAG_CONTROL;
                    }
                    producers.push(ProducerCoord {
                        producer_id: batch.producer_id,
                        producer_epoch: batch.producer_epoch,
                        base_sequence: batch.base_sequence,
                        last_sequence: batch.base_sequence.wrapping_add(batch.last_offset_delta),
                        offset_delta,
                        flags,
                    });
                }
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
                producers,
            });
        }

        let footer = SegmentFooter {
            writer_epoch,
            nonce,
            entries,
        };
        let footer_bytes = Self::encode_footer(&footer, SEGMENT_FORMAT_VERSION_V3);

        body.extend_from_slice(&footer_bytes);
        body.extend_from_slice(&(footer_bytes.len() as u64).to_be_bytes());
        body.extend_from_slice(&(footer.entries.len() as u32).to_be_bytes());
        body.extend_from_slice(&SEGMENT_FORMAT_VERSION_V3.to_be_bytes());
        body.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());

        Ok((PutPayload::from(Bytes::from(body)), footer))
    }

    /// Serialize a [`SegmentFooter`] index (#64/#59). Header: `writer_epoch
    /// (i64)`, plus `nonce (u64)` at v2. Then each entry: `topic_len (u16) +
    /// topic (utf8) + partition (i32) + base_offset (i64) + record_count (i64) +
    /// byte_start (u64) + byte_len (u64) + max_timestamp (i64)`, plus at v2
    /// `pcoord_count (u16)` and that many `producer_id (i64) + producer_epoch
    /// (i16) + base_sequence (i32) + last_sequence (i32) + offset_delta (u32)`,
    /// plus at v3 a per-coordinate `flags (u8)` (#174) — all big-endian. Asked
    /// for v2, this MUST keep emitting the exact pre-v3 bytes (`flags` is
    /// dropped, not zero-filled): deployed readers, internal and S3-direct
    /// external, decode v2 by that byte layout. Paired with
    /// [`Self::decode_footer`]; the external contract is
    /// `docs/virtual-topics-format.md`.
    fn encode_footer(footer: &SegmentFooter, version: u16) -> Vec<u8> {
        let v2 = version >= SEGMENT_FORMAT_VERSION_V2;
        let v3 = version >= SEGMENT_FORMAT_VERSION_V3;
        let mut buf = Vec::new();
        buf.extend_from_slice(&footer.writer_epoch.to_be_bytes());
        if v2 {
            buf.extend_from_slice(&footer.nonce.to_be_bytes());
        }
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
            if v2 {
                buf.extend_from_slice(&(entry.producers.len() as u16).to_be_bytes());
                for pc in &entry.producers {
                    buf.extend_from_slice(&pc.producer_id.to_be_bytes());
                    buf.extend_from_slice(&pc.producer_epoch.to_be_bytes());
                    buf.extend_from_slice(&pc.base_sequence.to_be_bytes());
                    buf.extend_from_slice(&pc.last_sequence.to_be_bytes());
                    buf.extend_from_slice(&pc.offset_delta.to_be_bytes());
                    if v3 {
                        buf.push(pc.flags);
                    }
                }
            }
        }
        buf
    }

    /// Parse a [`SegmentFooter`] from `footer_bytes`, the `footer_len` bytes that
    /// precede the trailer (#64). Inverse of [`Self::encode_footer`]; a
    /// truncated or malformed footer is a corrupt segment, not a legacy object.
    fn decode_footer(
        footer_bytes: &[u8],
        entry_count: usize,
        version: u16,
    ) -> Result<SegmentFooter> {
        let v2 = version >= SEGMENT_FORMAT_VERSION_V2;
        let v3 = version >= SEGMENT_FORMAT_VERSION_V3;
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
        let nonce = if v2 {
            u64::from_be_bytes(take(&mut cursor, 8)?.try_into()?)
        } else {
            0
        };

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

            let producers = if v2 {
                let pcoord_count = u16::from_be_bytes(take(&mut cursor, 2)?.try_into()?) as usize;
                let mut producers = Vec::with_capacity(pcoord_count);
                for _ in 0..pcoord_count {
                    producers.push(ProducerCoord {
                        producer_id: i64::from_be_bytes(take(&mut cursor, 8)?.try_into()?),
                        producer_epoch: i16::from_be_bytes(take(&mut cursor, 2)?.try_into()?),
                        base_sequence: i32::from_be_bytes(take(&mut cursor, 4)?.try_into()?),
                        last_sequence: i32::from_be_bytes(take(&mut cursor, 4)?.try_into()?),
                        offset_delta: u32::from_be_bytes(take(&mut cursor, 4)?.try_into()?),
                        // v3 appends one flags byte per coordinate (#174); a
                        // v1/v2 footer has no such byte and decodes to 0, the
                        // exact value pre-v3 code observed.
                        flags: if v3 { take(&mut cursor, 1)?[0] } else { 0 },
                    });
                }
                producers
            } else {
                Vec::new()
            };

            entries.push(SubstreamEntry {
                topic,
                partition,
                base_offset,
                record_count,
                byte_start,
                byte_len,
                max_timestamp,
                producers,
            });
        }

        Ok(SegmentFooter {
            writer_epoch,
            nonce,
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
        // v3 is accepted one release before anything writes it (#174): this
        // rejection is a hard error that propagates through the index refresh
        // into fetch, so a reader that lacks a version suffers a partition-wide
        // read outage the moment a writer emits it — see
        // [`SEGMENT_FORMAT_VERSION_V3`]. Rejecting every *other* version stays:
        // it is the external contract's MUST (`docs/virtual-topics-format.md`),
        // and guessing at an unknown layout would mis-decode, not degrade.
        if version != SEGMENT_FORMAT_VERSION
            && version != SEGMENT_FORMAT_VERSION_V2
            && version != SEGMENT_FORMAT_VERSION_V3
        {
            return Err(Error::Message(format!(
                "unsupported segment format version {version}"
            )));
        }

        let footer_end = tail.len() - SEGMENT_TRAILER_LEN;
        let footer_start = footer_end
            .checked_sub(footer_len)
            .ok_or_else(|| Error::Message(String::from("segment footer length exceeds tail")))?;

        Self::decode_footer(&tail[footer_start..footer_end], entry_count, version).map(Some)
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
                    _ = watermark.truncate.take();

                    Ok(())
                })
                .await?;

            // Drop any stale next-offset hint (e.g. a topic of the same
            // name was previously deleted) so the fresh, empty partition
            // re-derives offset 0 from listing. The cached watermark floor
            // must go with it: the prefix's seq floor is unrelated to topic
            // lifecycle, so it alone would never invalidate a floor cached
            // for the deleted incarnation. Same for the truncation-floor
            // memo (#176): a re-created topic must not inherit a dead
            // incarnation's floor.
            _ = self
                .next_offsets
                .lock()
                .map(|mut locked| locked.remove(&topition))?;
            _ = self
                .coalesced_watermark_floors
                .lock()
                .map(|mut locked| locked.remove(&topition))?;
            _ = self
                .truncate_floors
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
                .scan(Scan::AdminDelete, &prefix)
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
                .scan(Scan::AdminDelete, &prefix)
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
        {
            let attributes =
                BatchAttribute::try_from(deflated.attributes).inspect_err(|err| debug!(?err))?;

            // Server-side coalescing: buffer the batch and flush a run as one
            // object per linger window — per connector prefix into a shared
            // segment (#57) when prefix mode is on, else per partition (#50).
            // Transactional and control batches route into segments on the
            // leaseless prefix path since release B of #174 (footer v3 indexes
            // them; `meta.json` stays the commit authority); they still bypass
            // the lease-mode prefix path and the per-partition coalescer,
            // neither of which has a footer/transaction story. Compacted
            // batches bypass both paths — a compacted topic can't share a
            // whole-segment-expiry segment (#61) — UNLESS compacted topics are
            // segment-routed (#175), which sidesteps that objection by giving
            // each compacted topic a dedicated prefix (`routed_prefix`); with
            // the flag off, the gate is exactly today's. The compacted-policy
            // check is memoized (#113) so a steady-state produce does not read
            // the topic metadata object per batch; with the flag on the `||`
            // short-circuits it away entirely here, and `routed_prefix_of`
            // consults the same memo at enqueue instead.
            // Every topic is segment-routed since the compacted flags were
            // hardwired: a compacted topic under its own dedicated prefix, all
            // others under their connector prefix. Nothing is ineligible, so
            // nothing reaches the legacy `records/` writer — which is what makes
            // it deletable here (#178).
            let coalesce_eligible = true;

            // Will this batch be buffered into a prefix-coalesced segment (#57)?
            //
            // Since #177 the answer is "whenever it is eligible": the segment
            // CAS is the *single* offset authority, so everything must go
            // through it — a legacy `records/` object would be a second
            // authority the two create-only name spaces can't reconcile (#78)
            // and would reintroduce the per-topic LIST/GET the epic removes
            // (#75). That includes transactional and control batches (#174
            // release B) and bulk backfill (#90): a large backfill batch trips
            // the raised byte threshold (see `flush_thresholds`) and flushes as
            // ~its own segment, keeping the 1-PUT parity the old #62 bypass
            // gave. The flush is strictly one segment at a time under the
            // per-prefix lock, so folding backfill in never pipelines sequences
            // (a failed N after N+1 landed would mint a gap).
            let prefix_buffer_route = coalesce_eligible;

            if deflated.is_idempotent() {
                // Under the leaseless arbiter (#86) the segment flush owns
                // idempotent dedup: it folds the log's producer coordinates into
                // a `ProducerTable` (#88) — a cross-pod-convergent authority that
                // also cannot advance before the batch is durable. So the per-pod
                // `producers/{id}.json` gate is *skipped* for batches taking that
                // route, since it diverges across a connection migration (#79)
                // and mishandles i32 sequence wraparound (#80). Transactional
                // data batches carry real sequences and shift to the same folded
                // authority now that they take the route (#174 release B);
                // control markers are not idempotent and never reach this block.
                // Every other path (non-leaseless, compacted-unrouted, backfill
                // bypass, legacy create) keeps the gate: validate (and advance)
                // the idempotent sequence on the producer's own
                // `producers/{id}.json` object rather than the cluster-global
                // `meta` object, so distinct producers no longer contend on one
                // hot object on GCS (#13); the advance is applied in memory and
                // checkpointed lazily so an idempotent batch costs ~1 PUT (#48).
                if !prefix_buffer_route {
                    self.advance_idempotent_sequence(
                        deflated.producer_id,
                        deflated.producer_epoch,
                        topition,
                        deflated.base_sequence,
                        deflated.last_offset_delta,
                    )
                    .await
                    .inspect(|outcome| debug!(transaction_id, ?topition, ?outcome))
                    // `DuplicateSequenceNumber` / `OutOfOrderSequenceNumber` are
                    // the expected idempotent-producer outcomes for a retried
                    // batch, not broker failures — log them at debug like the CAS
                    // itself, and reserve error! for genuinely unexpected Api
                    // errors (#37).
                    .inspect_err(|err| {
                        if err.is_expected_idempotent_outcome() {
                            debug!(?err, transaction_id, ?topition);
                        } else {
                            error!(?err, transaction_id, ?topition);
                        }
                    })?;
                }
            }

            // Captured before `deflated` is moved into the prefix buffer; the
            // transaction registration below needs both.
            let last_offset_delta = deflated.last_offset_delta;
            let producer_epoch = deflated.producer_epoch;

            let offset = if prefix_buffer_route {
                // Buffered into a prefix-coalesced segment (#57/#174): the
                // offset resolves only once the segment PUT is durable and the
                // parked producer is acked, so the transaction registration
                // below still runs durable-then-register, exactly like the
                // legacy arm. Deliberately NOT an early return: a transactional
                // produce that skipped the registration would leave its range
                // out of `meta.transactions`, `txn_end` would find nothing to
                // mark, and a read-committed consumer would read aborted data
                // as committed (the #81 bug class).
                self.enqueue_prefix_coalesced(topition, deflated).await?
            } else {
                // Assign the offset by *creating* the immutable batch object
                // (its name encodes the base offset), rather than by updating a
                // hot per-partition `watermark` object on every batch. The
                // create is the authority; the watermark stays a write-behind
                // cache only. This is the #13 fix: the produce hot path no
                // longer hammers a single object capped at ~1 write/s on GCS.
                let payload = self
                    .encode(deflated.clone())
                    .inspect_err(|err| debug!(?err))?;

                self.assign_and_create(topition, last_offset_delta as i64 + 1, payload)
                    .await
                    .inspect(|offset| debug!(offset, transaction_id, ?topition))
                    .inspect_err(|err| error!(?err, transaction_id, ?topition))?
            };

            // Register the produced range on the open transaction. Covers the
            // end-transaction marker too (`txn_end` produces it with the
            // transaction id and the transactional attribute), extending
            // `offset_end` over the marker's offset. Idempotent under retries:
            // a leaseless `Duplicate` ack returns the *original* offset, and
            // the `and_modify` below only ever widens `offset_end`.
            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        if let Some(transaction) = meta.transactions.get_mut(transaction_id) {
                            debug!(?transaction);

                            if let Some(txn_detail) = transaction.epochs.get_mut(&producer_epoch) {
                                debug!(?txn_detail);

                                let offset_end = offset + last_offset_delta as i64;

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
            // Prefix-coalesced (#60): records live in shared segments, located
            // by footer index and read with a ranged GET of exactly the
            // topition's byte span — no cross-topic download. A *hybrid* topic
            // (flipped to segment mode mid-life) also has legacy `records/`
            // objects for `[0, C)`; serve those first, then segments from `C`.
            // Legacy offsets are all below segment offsets (the #58 seam), so
            // legacy-then-segment preserves order and a single fetch with
            // budget to spare stitches across the seam.
            //
            // Nothing below the truncation floor is served (#176):
            // truncated records survive physically in shared segments, so
            // the floor is enforced by clamping the requested offset —
            // skip, not error, and batch-granular by construction via the
            // whole-batch skip in `fetch_prefix_coalesced`. Served from
            // in-process caches on a warm poll (`truncate_floor` memoizes
            // absence, so floor-less partitions add no request, #161).
            // The non-coalesced branch below is untouched: there
            // DeleteRecords deletes the per-partition objects physically.
            let offset = offset.max(self.truncate_floor(topition).await?);
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
        }

        Ok(batches)
    }

    async fn offset_stage_at(
        &self,
        topition: &Topition,
        isolation: IsolationLevel,
    ) -> Result<OffsetStage> {
        // Read-committed needs the last-stable offset and the aborted-transaction
        // list, both derived from the cluster `meta.json` object — take the full,
        // transaction-aware path (a fresh meta read, unchanged semantics).
        if isolation == IsolationLevel::ReadCommitted {
            return self.offset_stage(topition).await;
        }

        // Read-uncommitted (the common consumer case): no transaction state is
        // needed. The last-stable offset is the high watermark and there are no
        // aborted transactions to surface, so `meta.json` — the single, hot,
        // cluster-wide key — is never read. The high watermark comes from the
        // in-memory hint (#40) and the log start from the cached watermark
        // (#109) clamped to the cached truncation floor (#176) — both
        // request-free — so a warm, caught-up consumer resolves its
        // fetch-response offsets with zero object-store requests, off the
        // meta-object throttle ceiling entirely.
        let high_watermark = self.high_watermark(topition).await?;
        let log_start = self
            .cached_low(topition)?
            .unwrap_or(0)
            .max(self.cached_truncate(topition)?.unwrap_or(0));

        Ok(OffsetStage {
            last_stable: high_watermark,
            high_watermark,
            log_start,
            aborted: Vec::new(),
        })
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let (stable, aborted_raw) = self
            .meta
            .with(&self.object_store, |meta| {
                let stable = meta
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
                    .unwrap_or(BTreeMap::new());

                // Aborted transactions that produced to `topition` (#81), as
                // `(producer_id, first_offset, last_offset)` — read-committed
                // consumers use these to drop aborted records below the LSO. The
                // abort state is retained in `meta.transactions` (`txn_end` sets
                // `Aborted`, never prunes), so this is a pure meta read.
                let mut aborted_raw: Vec<(i64, i64, i64)> = Vec::new();
                for txn in meta.transactions.values() {
                    for detail in txn.epochs.values() {
                        if detail.state != Some(TxnState::Aborted) {
                            continue;
                        }
                        if let Some(partitions) = detail.produces.get(&topition.topic)
                            && let Some(Some(range)) = partitions.get(&topition.partition)
                        {
                            aborted_raw.push((txn.producer, range.offset_start, range.offset_end));
                        }
                    }
                }

                Ok((stable, aborted_raw))
            })
            .await?;

        debug!(?stable, ?aborted_raw);

        // The high watermark is derived from the immutable batch objects (the
        // authority), not from the mutable `watermark` object — so it is correct
        // across replicas even though offset assignment no longer CASes the
        // watermark on every produce (#13). The `watermark` object is consulted
        // only for the log start offset (`low`), advanced on the cold
        // maintain/expire path (and for lake-sink topics' high, folded into
        // `high_watermark`).
        let high_watermark = self.high_watermark(topition).await?;

        let log_start = self.log_start(topition).await?;
        let last_stable = stable.get(topition).copied().unwrap_or(high_watermark);

        // Keep aborted transactions whose records are still in the log (last
        // offset at/after the log start), as `(producer_id, first_offset)` sorted
        // by first offset (#81).
        let mut aborted: Vec<(i64, i64)> = aborted_raw
            .iter()
            .filter(|(_, _, offset_end)| *offset_end >= log_start)
            .map(|(producer_id, offset_start, _)| (*producer_id, *offset_start))
            .collect();
        aborted.sort_by_key(|(_, first_offset)| *first_offset);

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
            aborted,
        })
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

        // Resolve the partitions CONCURRENTLY (bounded) instead of awaiting
        // each in turn. A single ListOffsets can carry a consumer's whole
        // assignment — `endOffsets` over ~1500 partitions. On the warm
        // prefix-coalesced path a partition now costs ZERO per-partition
        // object-store round-trips (LATEST and EARLIEST are served from the
        // segment index, `coalesced_high_from_index` /
        // `coalesced_earliest_offset`; only per-prefix amortized requests
        // remain), but the cold, hybrid and non-coalesced paths still pay at
        // least one round-trip each (the `watermark.json` GET in
        // `persisted_high`, or a `records/` LIST). The sequential loop was
        // O(partitions × RTT) there and blew past the Kafka client's request
        // timeout at scale, so the consumer could never resolve its
        // positions. Bounded concurrency issues exactly the same
        // per-partition reads (and returns the same answers) while making
        // wall-time O(partitions / concurrency); `buffered` — not
        // `buffer_unordered` — preserves request order in the response, as
        // the loop did. These per-partition paths already run concurrently
        // across independent client requests, so no new interleaving is
        // introduced. The bound matches `FOOTER_FETCH_CONCURRENCY`, keeping a
        // wide request within the object store's throttling envelope.
        const LIST_OFFSETS_CONCURRENCY: usize = 32;

        let stable = &stable;

        // Eagerly collected: a lazily-mapped iterator of async blocks inside
        // `stream::iter` trips a higher-ranked lifetime inference failure
        // ("implementation of `FnOnce` is not general enough") under
        // `async_trait`; the Vec pins every future to the one concrete
        // lifetime of this call. The futures are inert until polled by
        // `buffered`, so this allocates, it does not serialize.
        let resolutions = offsets
            .iter()
            .map(|(topition, offset_request)| async move {
                self.list_offset_response(topition, offset_request, stable)
                    .await
                    .map(|response| response.map(|response| (topition.to_owned(), response)))
            })
            .collect::<Vec<_>>();

        futures::stream::iter(resolutions)
            .buffered(LIST_OFFSETS_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await
            .map(|responses| responses.into_iter().flatten().collect())
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        _retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        // Commit each partition's offset concurrently (bounded): the same
        // O(N) -> O(N / concurrency) scaling fix as ListOffsets (#147) and
        // Metadata (#154). A large formed group commits offsets for
        // hundreds-to-thousands of partitions at once, and two serial
        // object-store round trips per partition (a metadata GET plus the
        // offset PUT) blew past the client's commit/rebalance timeout at scale,
        // stalling every rebalance. `try_collect` keeps the fail-fast semantics
        // of the `?` below (a metadata read error aborts the commit); the
        // per-partition PUT error is still reported as `UnknownServerError` in
        // the response, and `buffered` preserves response order.
        const OFFSET_COMMIT_CONCURRENCY: usize = 32;

        // Eagerly collected to pin lifetimes under `async_trait` (see #147).
        let commits = offsets
            .iter()
            .map(|(topition, offset_commit)| async move {
                let error_code = if self
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

                    self.object_store
                        .put_opts(&location, payload, options)
                        .await
                        .inspect_err(|err| error!(?err))
                        .inspect(|outcome| debug!(?outcome))
                        .map_or(ErrorCode::UnknownServerError, |_| ErrorCode::None)
                } else {
                    ErrorCode::UnknownTopicOrPartition
                };

                Ok::<_, Error>((topition.to_owned(), error_code))
            })
            .collect::<Vec<_>>();

        let responses = futures::stream::iter(commits)
            .buffered(OFFSET_COMMIT_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(responses)
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        let mut topitions = vec![];

        {
            let location = Path::from(format!(
                "clusters/{}/groups/consumers/{}/offsets/",
                self.cluster, group_id,
            ));

            let mut list_stream = self.scan(Scan::Group, &location);

            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error))
                .map_err(Error::from)?
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
            // Fetch each partition's committed offset concurrently (bounded):
            // the same O(N) -> O(N / concurrency) scaling fix as ListOffsets
            // (#147) and Metadata (#154). A large formed group (and the
            // rebalance-callback `committed()` lookups) reads offsets for
            // hundreds-to-thousands of partitions at once; a serial GET per
            // partition blew past the client timeout at scale. `try_collect`
            // preserves the fail-fast semantics of the `?` below (a transient
            // store error stays retriable, not a fatal `-1`, #6/#129).
            const OFFSET_FETCH_CONCURRENCY: usize = 32;

            // Eagerly collected to pin lifetimes under `async_trait` (see #147).
            let fetches = topics
                .iter()
                .map(|topition| async move {
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
                            .inspect_err(|error| error!(?error, ?group_id, ?topition)),

                        Err(object_store::Error::NotFound { .. }) => Ok(-1),

                        Err(error) => {
                            error!(?error, ?group_id, ?topition);
                            // Preserve the storage error so a transient S3
                            // failure is retriable, not fatal `-1` (#6/#129).
                            Err(Error::from(error))
                        }
                    }?;

                    Ok::<_, Error>((topition.to_owned(), offset))
                })
                .collect::<Vec<_>>();

            responses = futures::stream::iter(fetches)
                .buffered(OFFSET_FETCH_CONCURRENCY)
                .try_collect()
                .await?;
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
                // Fetch each topic's metadata concurrently. `topic_metadata`'s
                // object-store GET is the only await here; a serial loop is
                // O(topics × RTT) and, on a high-latency store, blew past the
                // client's metadata/request timeout at scale — so a consumer
                // group leader resolving the union of its members' subscriptions
                // (hundreds–thousands of topics) never completed the fetch,
                // never sent SyncGroup, and the group stayed `Forming` with zero
                // partitions assigned. Bounded concurrency issues exactly the
                // same per-topic reads and returns the same answers, in
                // O(topics / concurrency) wall time. `buffered` preserves
                // response order; collecting `Result`s (not `try_collect`) keeps
                // the per-topic error handling below intact. Same scaling fix as
                // ListOffsets (#147), same concurrency bound.
                const METADATA_FETCH_CONCURRENCY: usize = 32;

                // Eagerly collected to pin every future to this call's lifetime
                // under `async_trait` (see the identical note on ListOffsets);
                // the futures are inert until polled by `buffered`, so this
                // allocates, it does not serialize.
                let fetches = topics
                    .iter()
                    .map(|topic| async move { self.topic_metadata(topic).await })
                    .collect::<Vec<_>>();

                let fetched = futures::stream::iter(fetches)
                    .buffered(METADATA_FETCH_CONCURRENCY)
                    .collect::<Vec<_>>()
                    .await;

                // A per-topic read that resolved to nothing is NOT proof the
                // topic is gone (#214).
                //
                // `topic_metadata` reads one object through the `OptiCon` cache,
                // and `OptiCon::refresh` *clears* its cached value on any
                // `NotFound` — real, transient or spurious alike. So a single
                // 404 against a live topic's object turns into `Ok(None)`, and
                // reporting that as `UnknownTopicOrPartition` is the worst
                // available answer: the client cannot tell it from a deleted
                // topic, so it refreshes metadata until `max.block.ms` expires
                // and fails the batch. Production saw eight topics answered
                // absent minutes after being written, taking six of twenty-four
                // source connectors into restart loops.
                //
                // The topics index is an independent witness: it is built from a
                // LIST of the `topic-metadata/` prefix, not from the per-object
                // reads that just came back empty. If it knows the topic, the
                // read failed to resolve rather than the topic being absent, and
                // the honest answer is a retriable one.
                let unresolved: BTreeSet<&str> = topics
                    .iter()
                    .zip(&fetched)
                    .filter(|(_, result)| matches!(result, Ok(None)))
                    .filter_map(|(topic, _)| match topic {
                        TopicId::Name(name) => Some(name.as_str()),
                        TopicId::Id(_) => None,
                    })
                    .collect();

                let existing_but_unresolved: BTreeSet<String> = if unresolved.is_empty() {
                    BTreeSet::new()
                } else {
                    let known = self
                        .topics_index()
                        .await
                        .inspect_err(|error| warn!(?error, "topics index for absent-topic check"))
                        .unwrap_or_default();

                    let existing: BTreeSet<String> = known
                        .iter()
                        .map(|metadata| metadata.topic.name.clone())
                        .filter(|name| unresolved.contains(name.as_str()))
                        .collect();

                    for name in &existing {
                        METADATA_UNRESOLVED_EXISTING.add(1, &[]);
                        warn!(
                            topic = name.as_str(),
                            "metadata could not resolve a topic the index knows; \
                             answering retriably rather than reporting it absent"
                        );
                    }

                    existing
                };

                topics
                    .iter()
                    .zip(fetched)
                    .map(
                        |(topic, result)| match result.inspect_err(|error| error!(?error)) {
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
                                                    .map(|_replica| {
                                                        brokers.next().expect("cycling")
                                                    })
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
                                .error_code(
                                    // Retriable when the index says the topic is
                                    // there: the client backs off and retries
                                    // instead of spinning on an assertion of
                                    // absence it cannot question (#214).
                                    if matches!(topic, TopicId::Name(name) if existing_but_unresolved.contains(name))
                                    {
                                        ErrorCode::LeaderNotAvailable.into()
                                    } else {
                                        ErrorCode::UnknownTopicOrPartition.into()
                                    },
                                )
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
                        },
                    )
                    .collect()
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
            .scan_delimited(Scan::Group, &location)
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
                    .scan(Scan::AdminDelete, &prefix)
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

    async fn read_group(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        let location = Path::from(format!(
            "clusters/{}/groups/consumers/{}.json",
            self.cluster, group_id,
        ));

        match self.get::<GroupDetail>(&location).await {
            Ok(pair) => Ok(Some(pair)),
            Err(Error::ObjectStore(error))
                if matches!(error.as_ref(), object_store::Error::NotFound { .. }) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
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

                            // Keep an ABORTED transaction's produce ranges so a
                            // read-committed fetch can report its aborted offsets
                            // (#81) — the txn_detail is retained in
                            // `meta.transactions` regardless, only its produces
                            // were being dropped. A committed transaction's ranges
                            // are no longer needed. Consumer-offset commit state is
                            // cleared either way.
                            if txn_detail.state != Some(TxnState::Aborted) {
                                txn_detail.produces.clear();
                            }
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
        let now_ms = i64::try_from(
            now.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX);

        // Claim this tick's prefix-segment maintenance work-set once (#126),
        // stateless and coordinator-free: N maintainer replicas partition the
        // prefixes by first-arrival on the per-prefix lease + a recency stamp,
        // so retention and compaction of a prefix run under one claim (one
        // discovery LIST, not one per pass per replica). `None` when prefix
        // coalescing is off = today's every-prefix behaviour.
        let owned = self.claim_maintenance_prefixes(now_ms).await?;
        let owned = Some(&owned);

        let deleted = self.policy_delete_records(now).await?;

        // Retention and segment compaction per prefix, interleaved and
        // concurrent (#140) — not two whole sequential passes, where a delete
        // backlog consumed the bounded run (#131) before compaction ever ran.
        let (deleted_segments, compacted_segments) =
            self.maintain_prefix_segments(now_ms, owned).await?;
        let expired_groups = self.expire_groups(now).await?;
        debug!(
            deleted,
            deleted_segments, compacted_segments, expired_groups
        );

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
        let _ = self.scan(Scan::Ping, &Path::from("/")).next().await;
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

/// Backoff before re-deriving a segment sequence whose create-CAS was lost to a
/// peer writer (#157). N stateless replicas flush the same prefix concurrently;
/// without this every loser immediately re-LISTs and re-PUTs in lockstep, which
/// both amplifies requests on the busiest prefix and lets one writer lose its
/// whole attempt budget to the same peers. Deliberately short and heavily
/// jittered — 1ms, 2ms, 4ms, 8ms, 16ms (capped) plus up to 100% jitter, so the
/// whole 64-attempt budget adds well under a second of produce latency while the
/// jitter desynchronises the racers. Orders of magnitude below
/// [`throttle_backoff`]: this is arbitration between peers, not a throttled
/// bucket.
fn cas_conflict_backoff(attempt: usize) -> Duration {
    let base_ms = 1u64 << attempt.min(4);
    let jitter = rng().random_range(0..=base_ms);
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

    /// Make a listing stream visible to the request metrics (#165).
    ///
    /// The two streaming list methods were forwarded uninstrumented, so the
    /// per-method request metric reported ~0.5 LIST/s while the bucket meter
    /// showed ~1,200/s — the tier-1 plane that dominates the bill was invisible,
    /// and every LIST reduction claimed from this counter was unverified.
    ///
    /// A listing is not one request: the store pages it, returning at most
    /// [`LIST_PAGE_KEYS`] keys per `ListObjectsV2`, and the meter counts pages.
    /// So one sample is recorded for the call itself — including a listing that
    /// yields nothing, which is still a metered request, and is exactly the shape
    /// the legacy-records probe issues (#166) — and one more each time the objects
    /// streamed past cross a page boundary. That makes the sample *count* line up
    /// with the meter, which is the point of the metric.
    ///
    /// Only the page-boundary samples carry a real latency (time since the
    /// previous page); the call sample records `0`, since a stream's first request
    /// is still in flight when it is handed back. `delete_stream` already reports
    /// its per-object samples the same way.
    fn instrument_listing(
        &self,
        method: &'static str,
        inner: BoxStream<'static, Result<ObjectMeta, object_store::Error>>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        /// Keys per `ListObjectsV2` response, i.e. per metered LIST request.
        const LIST_PAGE_KEYS: u64 = 1_000;

        let attributes = vec![
            KeyValue::new("method", method),
            KeyValue::new("cluster", self.cluster.clone()),
        ];
        let request_duration = self.request_duration.clone();
        let request_error = self.request_error.clone();

        request_duration.record(0, attributes.as_ref());

        let mut yielded = 0u64;
        let mut page_started = SystemTime::now();

        Box::pin(inner.inspect(move |result| match result {
            Ok(_) => {
                yielded += 1;

                if yielded.is_multiple_of(LIST_PAGE_KEYS) {
                    request_duration.record(
                        page_started
                            .elapsed()
                            .map_or(0, |elapsed| elapsed.as_millis() as u64),
                        attributes.as_ref(),
                    );
                    page_started = SystemTime::now();
                }
            }

            Err(err) => {
                debug!(?err, method);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.extend(attributes.iter().cloned());
                request_error.add(1, &additional[..]);
            }
        }))
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

        self.instrument_listing("list", self.object_store.list(prefix))
    }

    // Forward `list_with_offset` (S3 `start-after`) so a tail-offset scan reads
    // only the partition tail rather than the default full-`list` downgrade.
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix, ?offset);

        self.instrument_listing(
            "list_with_offset",
            self.object_store.list_with_offset(prefix, offset),
        )
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
    use super::{Duration, cas_conflict_backoff, is_s3_throttle, throttle_backoff};

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

    /// #157: the create-CAS conflict yield must stay in the millisecond band —
    /// its job is to desynchronise peer writers, not to wait out a throttle — so
    /// the whole 64-attempt budget costs well under a second of produce latency.
    #[test]
    fn cas_conflict_backoff_is_short_capped_and_jittered() {
        // attempt 0: base 1ms + up to 100% jitter => [1, 2]ms
        let first = cas_conflict_backoff(0).as_millis();
        assert!((1..=2).contains(&first), "{first}");

        // capped from attempt 4 on: base 16ms + up to 100% jitter => [16, 32]ms
        for attempt in [4, 5, 64, 1_000] {
            let capped = cas_conflict_backoff(attempt).as_millis();
            assert!((16..=32).contains(&capped), "attempt {attempt}: {capped}");
        }

        // The whole budget, at the cap, stays sub-second.
        assert!(64 * cas_conflict_backoff(64) < Duration::from_secs(3));
    }
}
