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
    fmt::{Debug, Display, Write as _},
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
use metadata::{Cache, key_class};
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
    join_group_response::JoinGroupResponseMember,
    list_groups_response::ListedGroup,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{Record, deflated, inflated},
    to_system_time,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, instrument, warn};
use url::Url;
use uuid::Uuid;

mod metadata;
mod opticon;

#[cfg(test)]
mod tests;

use crate::{
    AclBinding, AclFilter, Acls, AssignmentDoc, AssignmentOutcome, AutoTopicCreate,
    BrokerRegistrationRequest, ConsumerGroupState, CorruptRegion, DivergentBatch, Error,
    GROUP_SCHEMA_VERSION, GenerationDoc, GroupDetail, GroupMember, GroupSchema, GroupState,
    ListOffsetResponse, METER, MemberDoc, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
    OffsetStage, ProducerIdResponse, QuotaAlteration, QuotaEntity, QuotaFilterComponent,
    QuotaLimits, Quotas, Result, ScramCredential, Storage, TopicDefaults, TopicId, Topition,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState,
    UpdateError, Version, storage_error_code,
};

const APPLICATION_JSON: &str = "application/json";

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

    /// Cache of the pinned routing prefix (`topic-routing/{name}.json`), the
    /// prefix a topic's records coalesce under (#236).
    ///
    /// Held **without a TTL**, for the same reason [`Self::topic_ids`] is: the
    /// pinned value is immutable for a topic's lifetime, so there is no staleness
    /// argument to make. That is the whole point of pinning it. Before, routing
    /// was re-derived from `cleanup.policy` — mutable config — so the value could
    /// only be memoized for seconds, and the per-topic conditional GET that
    /// refreshed it was 57% of the fleet's 304 plane (~$38/day). Invalidated on
    /// delete, exactly like the id pointer.
    routing_prefixes: Arc<Mutex<BTreeMap<Topic, String>>>,

    /// Broker auto-topic-creation policy (Kafka `auto.create.topics.enable` /
    /// `num.partitions` / `default.replication.factor`), consulted by the
    /// Metadata handler.
    auto_create: AutoTopicCreate,

    /// Broker-level topic config defaults, injected into every topic this engine
    /// creates. Held here rather than in the `CreateTopics` service so the
    /// injection sits at the single creation choke point and cannot be bypassed by
    /// a caller that builds its own `CreatableTopic` — which is exactly how the
    /// auto-create path silently dropped it (#225).
    topic_defaults: TopicDefaults,

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
    coalesced_watermark_floors: Arc<Mutex<BTreeMap<Topition, CachedWatermark>>>,

    /// Prefixes this process has already run the served-end reconciliation over
    /// (#290). See [`DynoStore::certify_prefix_served_ends`].
    ///
    /// Once per prefix per process, not once per tick: the pass costs a forced
    /// listing plus one conditional watermark GET per sub-stream, which is fine
    /// as a one-shot after a deploy and wasteful every tick. A restart re-arms
    /// it, which is the right default — a fresh process is exactly when a prefix
    /// may have picked up a gap under a binary that did not certify.
    served_end_reconciled: Arc<Mutex<BTreeSet<String>>>,

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

    /// Per-group optimistic-concurrency handle on `offsets.json` (#406), so the
    /// conditional GET a commit pays is served from a memoized etag rather than a
    /// body read on every commit.
    group_offsets: Arc<Mutex<BTreeMap<String, OptiCon<GroupOffsets>>>>,

    /// Segments a compaction pass has proved undecodable, per prefix (#398).
    ///
    /// A region that arrives whole and holds no frame is damage no code path in
    /// this process can undo: `CorruptSegment` is deliberately fatal to the
    /// compaction run (#388), so without this the run selection picks the same
    /// object every tick — it selects the *oldest* mergeable segments, and a
    /// damaged one is old — and the prefix's drain dies on byte 0 of it forever.
    /// Observed in production for 17 hours at ~2 errors/hour, which is #274's
    /// "one error aborts the prefix's pass" one error variant later.
    ///
    /// In memory and per process on purpose: it is a *skip list*, not a verdict
    /// about the object. A restart re-reads the segment once and re-quarantines
    /// it if it is still bad, which is exactly the behaviour wanted the day a
    /// repair path lands — nothing durable has to be un-said.
    ///
    /// A quarantined sequence bounds a merge run rather than being filtered out
    /// of it: the merged segment carries the base offset of its first region and
    /// concatenates the rest, so merging *across* a hole would shift every
    /// following record's offset down into it. Bounded by
    /// [`Self::PREFIX_QUARANTINE_CAP`] per prefix, and pruned against the index
    /// as segments retire.
    quarantined_segments: Arc<Mutex<BTreeMap<String, BTreeSet<u64>>>>,

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

    /// The same cache as `object_store`, typed, so a test can expire its etag
    /// memo instead of sleeping through the window (#167).
    #[cfg(test)]
    metadata_etags: Arc<dyn metadata::ExpireCachedEtags>,
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

    /// When a listing last reconciled this index *downwards* — dropped entries
    /// whose objects are gone (#408).
    ///
    /// Distinct from `refreshed_at`, which the add-only refresh and the tail
    /// probe both stamp: neither of those can remove an entry, so a replica's
    /// index grows monotonically with every segment a *peer* retires and only
    /// ever shrinks when this replica reads a 404 for one, an entry at a time.
    /// This is the clock that bounds how often the downward pass is paid for.
    reconciled_at: Option<SystemTime>,
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

/// Per-sub-stream offset coverage of a candidate merge run (#398) — the
/// guard that stops a merge from closing a hole.
///
/// The read path re-derives every record's offset by running from the
/// merged footer entry's `base_offset`
/// ([`DynoStore::fetch_prefix_coalesced`]), so a merged region must cover one
/// unbroken offset interval per sub-stream. Normally it does: the segments
/// of a prefix tile the offset axis with no gaps, so any run of them is
/// contiguous whatever order it was selected in. Quarantining a segment
/// (see [`DynoStore::quarantined_segments`]) punches a hole in that tiling, and
/// merging *across* the hole would slide every record above it down into
/// the gap — silent offset corruption, which is far worse than the stalled
/// drain being fixed.
///
/// Contiguity is checked with running totals rather than by sorting: a set
/// of non-overlapping spans is one interval exactly when the records it
/// holds equal the offsets it spans.
/// The outcome of one compaction run over a prefix (#399).
///
/// [`DynoStore::compact_prefix_segments`] used to answer `u64`, and the drain
/// read `Ok(0)` as "this prefix has nothing left to merge". It is also what a run
/// that could not proceed *at all* answered — the index named segments a peer had
/// already retired — so the drain stopped there having merged nothing, and run
/// selection picks the oldest segments, which are exactly the ones a peer's
/// compaction retires first. `tansu_prefix_segment_vanished_before_read` runs at
/// 11/s on the fleet, against busiest prefixes sitting at 30–68× their trigger.
///
/// Naming the three cases is what lets the drain continue over a run it could
/// not use, and stop on the one case that means it is finished.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum CompactRun {
    /// Segments merged away by this run.
    Merged(u64),

    /// Nothing left to merge under the current thresholds — or another compactor
    /// holds the prefix, which is the same thing for this replica this tick.
    Drained,

    /// This run could not proceed, and the next selection will differ: the index
    /// named segments that are gone, and they have been pruned from it.
    Retry,
}

impl CompactRun {
    fn outcome(&self) -> &'static str {
        match self {
            Self::Merged(_) => "merged",
            Self::Drained => "drained",
            Self::Retry => "retry",
        }
    }
}

/// Every committed offset of one consumer group, in one object (#406, #111).
///
/// The layout this replaces is one object per `(group, topic, partition)`,
/// written with an unconditional overwrite. So a commit over `t` partitions cost
/// `t` billed PUTs and an `OffsetFetch(all)` cost a LIST plus a GET each — and on
/// the production fleet consumer-group writes are **67 % of the whole PUT
/// plane**, $10.59/day, the largest single line item on the request bill.
///
/// #111 asked for exactly this and its acceptance — "a commit over `t` topitions
/// issues O(1) PUTs, not `1 + t`" — was never met; the issue was closed on its
/// other half.
///
/// The per-partition objects are **not** migrated in bulk and **not** deleted.
/// Reads fall back to them per key ([`DynoStore::offset_fetch`]) and the
/// topition-set discovery unions them in
/// ([`DynoStore::committed_offset_topitions`]), so this object accumulates as
/// commits happen with no fold-everything pass on a path whose whole purpose is
/// to stop reading O(partitions) objects. Leaving them also bounds what a
/// rollback costs: an older binary reads only the per-partition objects, so it
/// resumes from the last offset committed before the upgrade rather than from
/// nothing.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct GroupOffsets {
    /// `topic -> partition -> commit`.
    #[serde(default)]
    committed: BTreeMap<String, BTreeMap<i32, OffsetCommitRequest>>,

    /// Fields a future version adds, preserved through this version's rewrites —
    /// the same catch-all the watermark document carries (#182), for the same
    /// reason: a reader that drops what it does not understand turns every
    /// round-trip through an older binary into silent field erasure.
    #[serde(flatten)]
    rest: BTreeMap<String, serde_json::Value>,
}

impl GroupOffsets {
    fn get(&self, topition: &Topition) -> Option<&OffsetCommitRequest> {
        self.committed
            .get(topition.topic())
            .and_then(|partitions| partitions.get(&topition.partition()))
    }

    fn insert(&mut self, topition: &Topition, commit: OffsetCommitRequest) {
        _ = self
            .committed
            .entry(topition.topic().to_owned())
            .or_default()
            .insert(topition.partition(), commit);
    }

    /// Drop every partition of `topic`, answering whether anything went. Called
    /// when a topic is deleted: a committed offset that outlives its topic is
    /// served against the recreated one, which is #241's shape.
    fn remove_topic(&mut self, topic: &str) -> bool {
        self.committed.remove(topic).is_some()
    }

    fn topitions(&self) -> impl Iterator<Item = Topition> + '_ {
        self.committed.iter().flat_map(|(topic, partitions)| {
            partitions
                .keys()
                .map(move |partition| Topition::new(topic.clone(), *partition))
        })
    }
}

/// Why a describe-path metadata lookup did or did not come from the topics index
/// (#407).
///
/// [`DynoStore::indexed_topic_metadata`] answered `Option`, which collapsed two
/// facts that want different things. The fleet falls through to the object 18.5
/// times a second and **every one of those 404s** —
/// `tansu_topic_metadata_reads{source="object"}` and
/// `class="topic_metadata",reason="not_found"` are equal to three decimals — so
/// the question is whether the fallback is ever the thing that finds a topic, or
/// whether it is 1.6 M/day of confirming absence.
///
/// It cannot be answered from the request counters, because a miss on a *fresh*
/// index and a miss because there was no usable index at all are the same
/// `source="object"`. Only the first is provably pointless.
enum IndexedTopic {
    /// The index holds it, and this is the whole of #387 working.
    Hit(TopicMetadata),

    /// The index is inside its TTL and does not hold this topic.
    ///
    /// The object read still follows, and #407 proposed skipping it here on the
    /// grounds that a fresh index is authoritative for absence. **It is not, and
    /// the codebase already says so**:
    /// `a_topic_created_on_a_peer_resolves_through_metadata_before_the_index_refreshes`
    /// pins #28's contract — a topic created on another replica is visible
    /// through `Metadata` *at once*, and it is this fallback that makes it so.
    /// The index window delays changes to and removals of topics it already
    /// lists; it must never delay the appearance of a new one. Skipping here
    /// would put a `TOPIC_INDEX_TTL` hole exactly where #28 needs none.
    ///
    /// So this arm costs one GET and keeps it.
    FreshMiss,

    /// A by-id lookup whose `topic-ids/{uuid}.json` pointer does not resolve.
    ///
    /// Split out of `FreshMiss` (#407), which folded the two on the reasoning
    /// that they were "the same answer for the same reason". They are not the
    /// same *cost*. The pointer lookup is the only way to turn an id into a
    /// name, [`DynoStore::topic_metadata`] does exactly the same lookup, and
    /// `topic_name_by_id` caches only positives — so falling through spent a
    /// second GET on the same key, in the same request, microseconds later, and
    /// could not answer differently for any reason a caller could observe.
    ///
    /// Measured: a by-id miss cost **2** GETs of one `topic-ids/{uuid}.json`
    /// against a by-name miss's 1. Unlike `FreshMiss` there is no contract here
    /// to trade — this is not the index's window, it is the same uncached
    /// function called twice.
    UnknownId,

    /// No usable index: outside its TTL, or never built. This is the case the
    /// fallback exists for (#28/#29 — a topic created since the last refresh must
    /// be visible immediately), and the one that must keep reading the object.
    Stale,
}

impl IndexedTopic {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Hit(_) => "hit",
            Self::FreshMiss => "fresh_miss",
            Self::UnknownId => "unknown_id",
            Self::Stale => "stale",
        }
    }
}

/// A sub-stream inside a merge run.
type SubstreamKey = (String, i32);

/// One sub-stream's offset coverage within a candidate run: the lowest base
/// offset seen, the highest end, and the records held between them. The spans
/// are one unbroken interval exactly when the records equal the offsets spanned.
#[derive(Copy, Clone, Debug)]
struct OffsetSpan {
    base: i64,
    end: i64,
    records: i64,
}

impl OffsetSpan {
    fn is_contiguous(&self) -> bool {
        self.records == self.end - self.base
    }
}

#[derive(Debug, Default)]
struct RunCoverage {
    spans: BTreeMap<SubstreamKey, OffsetSpan>,
}

impl RunCoverage {
    /// Fold `entries` in, or leave the coverage untouched and answer `false` if
    /// any sub-stream would stop being one unbroken interval.
    ///
    /// Overlapping spans are refused by the same arithmetic (records exceed the
    /// offsets spanned). Inside a run that cannot normally happen — the merge
    /// reads the epoch-fenced view, which resolves overlaps — but these entries
    /// come from the raw footers, so on a prefix holding a zombie region this
    /// ends the run early rather than merging it. Refusing is the safe
    /// direction, and it only applies to a prefix that already has a quarantined
    /// segment.
    fn extend(&mut self, entries: &[SubstreamEntry]) -> bool {
        let mut folded: Vec<(SubstreamKey, OffsetSpan)> = Vec::with_capacity(entries.len());

        for entry in entries {
            let key = (entry.topic.clone(), entry.partition);
            let end = entry.base_offset + entry.record_count;

            let span = match self.spans.get(&key) {
                Some(held) => OffsetSpan {
                    base: held.base.min(entry.base_offset),
                    end: held.end.max(end),
                    records: held.records + entry.record_count,
                },

                None => OffsetSpan {
                    base: entry.base_offset,
                    end,
                    records: entry.record_count,
                },
            };

            if !span.is_contiguous() {
                return false;
            }

            folded.push((key, span));
        }

        for (key, span) in folded {
            _ = self.spans.insert(key, span);
        }

        true
    }
}

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

/// What a create-only segment PUT actually achieved, once an *ambiguous* result
/// has been resolved (#89) — see [`DynoStore::resolve_segment_create`].
#[derive(Debug)]
enum SegmentCreate {
    /// The object at the claimed sequence is ours: either the PUT returned
    /// success, or it failed ambiguously and the footer there carries our nonce.
    Won,

    /// A peer holds the sequence. Fold it in and retry the next free one.
    /// `ambiguous` separates a plain `AlreadyExists` from a PUT that errored and
    /// only *then* turned out to have been beaten to the sequence — the two cost
    /// the same but say different things about what is going wrong.
    Lost { ambiguous: bool },

    /// The create did not land and cannot be claimed. Carries the storage error,
    /// which is classified retriable (#6/#129).
    Failed(Error),
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

/// Per-deployment overrides for the coalescing flush thresholds (#54), applied
/// via [`DynoStore::coalesce_tuning`]. A `None` field keeps that trigger's
/// compile-time default, so omitting every key reproduces the shipped
/// behaviour. Populated from the storage URL query string; see the storage
/// tuning docs for the fan-out tradeoff these expose.
#[derive(Clone, Copy, Debug, Default)]
pub struct CoalesceTuning {
    pub coalesce_linger: Option<Duration>,
    pub coalesce_batches: Option<usize>,
    pub coalesce_bytes: Option<usize>,
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

/// Batch frames re-encoded by the per-key compaction pass because they carried
/// dependent-block LZ4 (#253) — the drain progress signal for that repair, and
/// the one that tells when the damage is gone: it stops moving because every
/// affected prefix has been visited, not because the pass stopped running.
///
/// Deliberately separate from `SEGMENT_RECORDS_COMPACTED`: a repair removes no
/// record by construction, so the removal counter cannot show it and a flat
/// removal counter must not be read as "nothing to repair". Expected to reach a
/// bounded total (the frames written while the encoder bug was live) and then
/// stay flat forever; a fresh increment after that means an encoder regression.
static SEGMENT_FRAMES_REPAIRED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_frames_repaired")
        .with_description("batch frames re-encoded from dependent-block LZ4 by compaction")
        .build()
});

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

/// Per-topic metadata resolutions, labelled by the API path that asked
/// (`caller`) and where the answer came from (`source`: `index` or `object`)
/// (#387).
///
/// The object-store counters carry a `class="topic_metadata"` label but no
/// caller, so a 1,040/s revalidation plane could be attributed to a call site
/// only by arithmetic against the API mix. This says it directly, and it is also
/// how the fix is checked: `source="object"` is the population that still costs a
/// conditional GET, and after #387 it should be admin-rate — a `Metadata` or
/// `OffsetCommit` steady state that keeps showing `object` means the index is
/// being missed (aged out, or a topic it does not hold).
/// Per-topic metadata resolutions by `caller`, `source` and `index` (#387, #407).
///
/// `source` says whether the topics index answered or the topic's own object had
/// to be read. `index` says *why* — see [`IndexedTopic`] — and that is the pair
/// that matters, because the fleet falls through to the object 18.5 times a
/// second and **every one of those 404s**:
/// `tansu_topic_metadata_reads{source="object"}` and
/// `tansu_object_store_request_error{class="topic_metadata",reason="not_found"}`
/// are equal to three decimals, 1.6 M billed requests/day confirming that a
/// topic a client keeps asking for does not exist.
///
/// `index="fresh_miss"` is the share that could be answered negatively from the
/// index with no new staleness at all. `index="stale"` is the share the fallback
/// exists for (#28/#29). Skipping the fallback is only safe to the extent the
/// first dominates, and until this label there was no way to know.
static TOPIC_METADATA_READS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_topic_metadata_reads")
        .with_description("per-topic metadata resolutions by caller, source and index outcome")
        .build()
});

/// Objects a [`TopicIndex`] refresh had to GET (`outcome="fetched"`) against
/// those it reused from the previous snapshot on an unchanged etag
/// (`outcome="reused"`) (#387).
///
/// The reuse is what makes the index cheap enough to serve every `Metadata`
/// answer from: one LIST per window, and a body read only for a topic whose
/// object actually changed. That rests entirely on the listing carrying an etag
/// per object — a store that omits it degrades the refresh to a GET per topic
/// per window, which at 15k topics is far worse than the per-topic reads this
/// replaces. Silent in every other signal, so it is counted here.
static TOPIC_INDEX_REFRESH_OBJECTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_topic_index_refresh_objects")
        .with_description("objects a topic index refresh fetched against those it reused")
        .build()
});

/// Segments cached in this process's prefix index, across every prefix (#196).
static PREFIX_INDEX_SEGMENTS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_index_segments")
        .with_description("segments held in the process-local prefix index")
        .build()
});

/// Sub-stream entries across every cached footer (#196) — the process-local
/// structure that scales with `segments × topics`, and the one big enough to
/// account for the broker's working-set growth. Paired with
/// `tansu_prefix_index_segments`: the ratio is the mean sub-streams per segment,
/// which is what makes the total interpretable rather than just large.
static PREFIX_INDEX_SUBSTREAM_ENTRIES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_index_substream_entries")
        .with_description("sub-stream entries across every footer in the prefix index")
        .build()
});

/// How far the persisted floor sits above the surviving segment tail, or `None`
/// when that is not the state (#290).
///
/// The whole subtlety is `tail == None`. A sub-stream with no segment at all is a
/// *drained* partition, where the floor is legitimately the only authority and #299
/// already makes it report a log that starts where it ends. The state worth
/// measuring is the **partial** one: records still served below the tail, offsets
/// advertised above it, and nothing in between for a consumer parked there.
///
/// Pure, so the distinction that matters can be asserted without standing up a
/// metrics reader.
fn floor_above_tail(tail: Option<i64>, watermark_floor: i64) -> Option<i64> {
    tail.filter(|tail| watermark_floor > *tail)
        .map(|tail| watermark_floor - tail)
}

/// High-watermark resolutions where the persisted floor was **above** the
/// surviving segment tail, on a sub-stream that still holds segments (#290).
///
/// That is the state in which the broker advertises offsets no surviving segment
/// holds. A consumer parked in the gap reads empty on every poll, forever, with no
/// error on either side — `retention_can_orphan_offsets_below_the_advertised_watermark`
/// pins how ordinary retention reaches it.
///
/// **This counter does not say a fault occurred.** The same arithmetic is produced
/// by a peer replica having acked offsets this process never listed — segments
/// created *and* expired inside its blind window — where advertising the floor is
/// correct and lowering it would regress the log end below acknowledged offsets.
/// The two are byte-identical locally *and* in the bucket, since in both cases the
/// segments are gone. So this is a rate to characterise, not an alarm to wire, and
/// choosing between the candidate fixes needs its magnitude first.
///
/// Costs nothing to compute: both operands are already in hand at the fold. It is
/// deliberately not the shape of #292's detector, which paid a forced LIST and a
/// confirming read per empty fetch and measured ~10/min on a healthy fleet before
/// being removed in #314.
///
/// Labelled by prefix — bounded at tens — because "which prefix" is the first
/// question, and per-partition labels would not be bounded on a 14.7k-topic fleet.
/// The gap size goes in the log rather than a label: nine offsets below the tail is
/// a caught-up consumer, millions is a lost log, and that distinction wants a value
/// and not a series.
static WATERMARK_ABOVE_SEGMENT_TAIL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_watermark_above_segment_tail")
        .with_description(
            "high watermark resolutions where the persisted floor exceeded the \
             surviving segment tail, by prefix",
        )
        .build()
});

/// Sub-streams whose dead gap was certified by the reconciliation pass rather
/// than by the expiry that created it (#290), by prefix.
///
/// A gap only becomes answerable once something writes `Watermark::served` for
/// it, and only an expiry did — so every gap that predates #343, and every gap
/// on a deployment whose `maintenance_interval` means expiry never runs, stayed
/// silent. This counts the retro-fit: a non-zero value on a prefix is that
/// prefix admitting it was in the #290 state and is now able to say so.
static SERVED_END_CERTIFIED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_served_end_certified")
        .with_description(
            "sub-streams whose dead offset gap was certified by the reconciliation \
             pass, by prefix",
        )
        .build()
});

/// Segments **this replica has indexed** for a prefix after a maintenance tick
/// (#66) — the signal that tells whether compaction is keeping `S` bounded (a
/// counter can't).
///
/// Carries a `prefix` attribute: without one the last write of a pass won and the
/// gauge reported an arbitrary prefix rather than the growing one (#284).
///
/// Read it per replica, not aggregated (#399). It is `PrefixIndex::segments.len()`
/// — the size of this process's cached footer index — and the incremental refresh
/// is add-only below the tail, so an entry for a segment a *peer* retired stays
/// until this replica reads a 404 for it. Four maintainers reported 17 517,
/// 14 374, 13 932 and 67 for the same prefix at the same instant, and none of
/// those is the number of objects in the bucket. `max()` over the fleet is the
/// staleness of the worst index, not a backlog. What makes the gauge converge on
/// the truth is the drain pruning what it finds gone
/// ([`CompactRun::Retry`]) rather than stopping on the first of it.
static SEGMENTS_LIVE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_segments_live")
        .with_description("segments this replica has indexed for a prefix, by prefix")
        .build()
});

/// Prefixes a maintenance tick met, by `outcome` (#399): `claimed` when this
/// replica took the work, `recent` when a peer's stamp was inside
/// `maintenance_recency`, `lost` when the lease acquire was fenced.
///
/// #140's remedies all shipped and the busiest prefixes still net-grew — 17 500
/// live segments against a 256 trigger — while the four maintainers ran at 12
/// millicores between them. Nothing said how many prefixes a tick actually
/// claimed, or whether the top-`S` ones were the ones being claimed. This is that
/// measurement, and the pairing that makes it readable: `claimed` against
/// `recent` says whether the recency window is throttling the fleet or doing
/// nothing.
static MAINTENANCE_PREFIXES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_maintenance_prefixes")
        .with_description("prefixes a maintenance tick met, by outcome")
        .build()
});

/// Extra backlog sweeps a maintenance tick ran past its first pass (#399) — the
/// scheduling deficit made visible.
///
/// A tick used to be one pass over its claim, then idle until the next interval.
/// Measured in production at `maintenance_interval=10m`: compaction ran in a
/// burst of 1–2 minutes and then nothing for 8–9, a 10–20 % duty cycle, while
/// producers refilled the prefixes for the whole window. Zero here means the
/// first pass drained everything it claimed; a steady non-zero means the interval
/// was never the right place to decide how much work to do.
static MAINTENANCE_SWEEPS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_maintenance_sweeps")
        .with_description("extra backlog sweeps past a maintenance tick's first pass")
        .build()
});

/// Compaction runs attempted, by `outcome` (#399): `merged`, `drained`,
/// `retry`, `error`.
///
/// The denominator `tansu_prefix_segment_compactions` never had: with it,
/// segments-merged-per-run is a division rather than a guess, which is what says
/// whether `prefix_compact_target_bytes` is the binding constraint on a run or
/// whether something else ends it first.
///
/// `retry` is the one to watch. A run that could not proceed — a peer deleted the
/// segments it selected — used to return `Ok(0)`, which the drain read as "this
/// prefix is drained" and stopped on, having merged nothing. Run selection picks
/// the *oldest* segments, which are exactly the ones a peer's compaction retires
/// first, and `tansu_prefix_segment_vanished_before_read` runs at 11/s on the
/// fleet.
static SEGMENT_COMPACT_RUNS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_compact_runs")
        .with_description("compaction runs attempted, by outcome")
        .build()
});

/// Why a prefix's drain stopped, by `reason` (#399): `drained`, `runs`,
/// `retries`, `error`.
///
/// "Does a drain that starts on a 15 000-segment prefix finish it?" was
/// unanswerable: the drain logged nothing on the way out, so a prefix that
/// converged and a prefix that hit `MAX_RUNS_PER_PREFIX` looked identical from
/// outside. Anything other than `drained` means the backlog outlived the tick for
/// a reason worth naming.
static PREFIX_DRAIN_STOPS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_drain_stops")
        .with_description("why a prefix's compaction drain stopped, by reason")
        .build()
});

/// Footer-index entries replaced by the object's own trailer after a short read
/// (#397) — the read path catching the in-memory index serving an entry that
/// does not belong to the object it was used against.
///
/// The trailer is self-describing precisely so a reader never has to derive a
/// region from anything else (#64/#60), and this counts the times that mattered.
/// Nonzero means the index is a cache that can be wrong, which is a different
/// fault from the object being wrong — the discriminator #397 asked for, answered
/// per occurrence rather than once by hand.
static SEGMENT_INDEX_ENTRIES_CORRECTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_index_entries_corrected")
        .with_description("footer-index entries replaced by the object's own trailer")
        .build()
});

/// Segments quarantined by compaction because they hold a region no reader can
/// decode (#398) — first sight only, so this counts *distinct* bad objects
/// discovered, not re-reads of them.
///
/// The pairing with [`SEGMENTS_QUARANTINED`] is the point: a counter that moves
/// once and then stops, against a gauge that stays non-zero, is the steady state
/// this replaces — a permanent `ERROR` stream on a condition nothing in the
/// process will ever change. A counter that keeps moving means damage is still
/// being *created*, which is a different (and much worse) fact.
static SEGMENT_QUARANTINES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_quarantines")
        .with_description("segments excluded from compaction as undecodable")
        .build()
});

/// Segments currently quarantined for a prefix (#398), by `prefix` — as
/// [`SEGMENTS_LIVE`] is labelled, and for the same reason.
static SEGMENTS_QUARANTINED: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_prefix_segments_quarantined")
        .with_description("segments excluded from compaction as undecodable, by prefix")
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
///
/// Labelled by `prefix` and by `spent` (#401): the log line already said which
/// prefix and where the budget went, but the counter said neither, so a fleet
/// running 9 of these an hour could not attribute one without grepping ten
/// replicas' logs. `spent` is `elapsed` when the wall clock was actually spent
/// and `projected` when the loop declined to *start* an attempt the slowest
/// observed one said could not finish — on the fleet that projection ends about
/// a third of them, and the two want different fixes.
static FLUSH_CAS_EXHAUSTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_flush_cas_exhausted")
        .with_description("leaseless flushes that exhausted their segment create-CAS budget")
        .build()
});

/// Conditional writes that lost the CAS to an object which was then deleted
/// before the winner's value could be read back (#431), by key `class`.
///
/// The condition was logged and not counted, so a fleet running ~17/h of them
/// across five of ten replicas was invisible on every dashboard and could only
/// be found by grepping. Each one used to end a connection with no response
/// written — see [`DynoStore::put`].
///
/// Expected to be small and non-zero: it is a genuine race between a CAS and a
/// delete, and both are things the group plane does continuously. A rate that
/// tracks `class="group_member"` is the session sweep meeting a rejoin; one on
/// `class="group_assignment"` is `delete_group_assignments_before` meeting a
/// `SyncGroup` for the generation it is sweeping.
static CONDITIONAL_PUT_VANISHED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_conditional_put_vanished")
        .with_description("conditional writes whose lost-CAS re-read found the object deleted")
        .build()
});

/// Index entries dropped by a reconciling listing because their objects are gone
/// (#408), by `prefix`.
///
/// The incremental refresh is add-only below the tail, so a replica's index
/// accrues an entry for every segment a *peer* retires and sheds them one 404 at
/// a time: 39 `segment` 404s/s on the brokers, each one a billed GET and a
/// restarted fetch, against 4.94 M indexed segments across ten of them. This
/// counts what a listing removes that no 404 had reached yet.
///
/// Expected to move in bursts on the busiest prefixes and to fall as compaction
/// converges (#399). A prefix whose count keeps climbing is one whose segments
/// are being retired faster than this replica reads them.
static PREFIX_INDEX_RECONCILED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_index_reconciled")
        .with_description("index entries dropped by a reconciling listing, by prefix")
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

/// Sub-stream regions whose bytes are not the batch frames their footer entry
/// claims (#386) — a `byte_start` that does not point at a frame header.
///
/// Counts the **attempt**, before the cause is known. It used to be documented as
/// "expected to be zero; nonzero is a data-integrity failure on the write side",
/// and that reading is what sent #395 and #397 after a write-side bug twice: the
/// fleet's population turned out to be intact objects read through an index entry
/// that belonged to a different segment (#432). Either cause produces this
/// counter, and each occurrence costs a partition a `CORRUPT_MESSAGE` answer that
/// a Kafka client retries at the same offset.
///
/// Read it against [`SEGMENT_INDEX_ENTRIES_CORRECTED`], which is the subset the
/// object's own trailer proved was an index fault
/// ([`DynoStore::resolve_corrupt_region`]). The difference between the two is the
/// write-side population — #395's husks — and only that difference is
/// unrecoverable. Paired with [`SEGMENT_REGION_TRUNCATED`], which is the same
/// symptom reached by an entry that over-states rather than under-states.
static SEGMENT_REGION_CORRUPT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_regions_corrupt")
        .with_description("sub-stream regions that do not begin at a batch frame")
        .build()
});

/// Ranged region reads that came back short of the byte extent the footer claims
/// (#386): a damaged, truncated or partially-visible segment object.
///
/// Distinct from [`SEGMENT_REGION_CORRUPT`] on purpose — a short read is not
/// answered as corruption. Whole batches decode, the partial tail is ignored
/// exactly as the frame contract says, and a region yielding nothing that way
/// stays the bounded empty read of #290 rather than becoming an error. Which
/// makes it invisible without this counter, and it is one of the two candidate
/// causes #386 could not separate.
static SEGMENT_REGION_TRUNCATED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_regions_truncated")
        .with_description("region reads returning fewer bytes than the footer extent")
        .build()
});

/// Every `segment` 404 this process takes, by the `caller` that took it (#408).
///
/// `class="segment",reason="not_found"` was the number #408 was filed on — 39.2/s
/// on the brokers — and its acceptance asked for an order of magnitude off it.
/// The plane could not be attributed, so that target folded together two
/// populations with opposite meanings:
///
/// - **stale-index 404s.** A segment a *peer* retired that this replica's
///   add-only index still names. The bug, and what #413's reconciling listing
///   removes: `caller="fetch"` / `"refresh"` / `"compaction"`.
/// - **404s that are the answer.** `probe_prefix_tail` proves the tail by reading
///   `cursor + 1` and *expecting* absence; `resolve_segment_create` probes a
///   sequence its own PUT may not have landed at. These are deliberate, they
///   scale with produce and reads rather than with staleness, and no fix reduces
///   them because reducing them would mean not asking.
///
/// So `sum by (caller)` is what says whether the acceptance is met, and
/// `sum(...)` over all callers is what the class counter was measuring. Kept
/// alongside [`SEGMENT_VANISHED_BEFORE_READ`] rather than replacing it: that one
/// means "the index named an object that is gone", which is a narrower claim than
/// this and the one #191/#274 reason about.
static SEGMENT_ABSENT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_absent")
        .with_description("segment objects read and found absent, by caller")
        .build()
});

/// Segments this replica's index named that were gone by the time it read them
/// (#191): concurrent compaction (#66) merged them away, or retention (#61)
/// reclaimed them. Counted on the index refresh, the compaction fetch and — since
/// #399 — the read-path region GET, which is where most of it is.
///
/// A normal consequence of maintenance running against live readers, so nonzero is
/// expected. What is *not* normal is the rate the fleet runs at: ~11/s, against
/// 0.55 compaction runs/s that merge anything, because the incremental index
/// refresh is add-only below the tail so a stale entry is never reconciled away —
/// it is only ever discovered here. Read against `tansu_prefix_segment_compact_runs`
/// by outcome: the `retry` share is how much of the drain is spent walking an
/// index through the objects it names that no longer exist.
static SEGMENT_VANISHED_BEFORE_READ: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_prefix_segment_vanished_before_read")
        .with_description("segments deleted between the discovery listing and the footer read")
        .build()
});

/// Topics this process holds a metadata handle for, after a maintenance sweep
/// (#283).
///
/// The level, not a count of evictions: the failure being watched is monotonic
/// growth, so what says the fix is working is that this tracks the cluster's live
/// topic count instead of climbing past it. Divergence is the signal that
/// something populates a per-topic map by a path the sweep does not reach.
static TOPIC_CACHE_TOPICS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_topic_cache_topics")
        .with_description("topics held in this process's topic-metadata cache")
        .build()
});

/// Partitions this process holds a watermark handle for, after a maintenance
/// sweep (#283) — the partition-scale companion to [`TOPIC_CACHE_TOPICS`], and
/// the larger of the two by the partition count, so it is the one that shows up
/// first in RSS.
static TOPIC_CACHE_PARTITIONS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_topic_cache_partitions")
        .with_description("partitions held in this process's watermark cache")
        .build()
});

/// The topic names a per-topic cache holds entries for that are **not** in
/// `live` (#283), where `topic_of` projects the map's key onto its topic name —
/// identity for the name-keyed caches, [`Topition::topic`] for the
/// partition-keyed ones.
///
/// Generic so the six maps, whose keys and values are all different types, are
/// swept by one rule rather than by six copies of it. A poisoned lock yields
/// nothing: the sweep is opportunistic, and the next tick retries.
fn dead_keys<K, V, F>(
    cache: &Arc<Mutex<BTreeMap<K, V>>>,
    topic_of: F,
    live: &BTreeSet<Topic>,
) -> BTreeSet<Topic>
where
    F: Fn(&K) -> &str,
{
    cache.lock().map_or_else(
        |_| BTreeSet::new(),
        |cache| {
            cache
                .keys()
                .map(topic_of)
                .filter(|topic| !live.contains(*topic))
                .map(ToOwned::to_owned)
                .collect()
        },
    )
}

/// Entries in the cluster-global `meta.json` producer table (#283).
///
/// Nothing prunes this table, and every `InitProducerId` appends to it — so every
/// connector restart mints an entry that is kept forever. The cost that matters is
/// not the bytes but the access pattern: `init_producer` round-trips the **whole**
/// object (GET, parse, mutate, CAS-PUT), so registration cost grows with the number
/// of producers the cluster has ever seen, and it degrades exactly when it hurts
/// most — the `InitProducerId` herd of a mass reconnect after an incident.
///
/// Recorded before any expiry policy exists, deliberately: the design decision (and
/// the transaction half of it, which #81's aborted-transaction retention constrains)
/// needs the growth rate and the current magnitude first. A gauge and not a counter
/// because the question is the level, and the level is what a prune would change.
static META_PRODUCERS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_meta_producers")
        .with_description("producer entries in the cluster-global meta.json")
        .build()
});

/// Entries in the cluster-global `meta.json` transaction table (#283). Same shape
/// as [`META_PRODUCERS`], with the extra constraint that aborted transactions are
/// retained on purpose (#81), so this half needs a design decision rather than a
/// prune.
static META_TRANSACTIONS: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_meta_transactions")
        .with_description("transaction entries in the cluster-global meta.json")
        .build()
});

/// Serialised size of the cluster-global `meta.json` (#283) — the payload every
/// `InitProducerId` and every transaction state change moves twice. This is the
/// number the growth math is actually about; the two entry-count gauges above say
/// which table is responsible for it.
static META_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_meta_bytes")
        .with_description("serialised size of the cluster-global meta.json")
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
/// (#64), distinguishing a `.seg` from a foreign or truncated object
/// (#50), which carries no trailer. ASCII `TSEG`.
pub(crate) const SEGMENT_MAGIC: u32 = 0x5453_4547;

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
pub(crate) const SEGMENT_TRAILER_LEN: usize =
    size_of::<u64>() + size_of::<u32>() + size_of::<u16>() + size_of::<u32>();

/// Speculative suffix size for reading a segment footer in a single ranged GET
/// (#112 follow-up). The trailer + footer of the overwhelming majority of
/// segments fit within this, so one over-reading GET replaces the previous
/// read-trailer-then-read-footer two-GET dance — halving the footer GETs the
/// read/refresh path pays on every non-writer replica. A footer larger than
/// this (a prefix with very many sub-streams) falls back to a second exact GET.
/// Footers are immutable, so the over-read is always self-consistent; the extra
/// bytes are in-region and cost nothing per request.
pub(crate) const SEGMENT_FOOTER_OVER_READ: usize = 64 * 1024;

/// One `(topic, partition)` sub-stream's self-describing entry in a segment
/// footer (#64): where its batches live in the shared object and what offset
/// span they cover. This is what the fetch path (#60) and cold-start offset
/// recovery (#58) read, instead of deriving offsets from the object filename
/// (the legacy `{offset}.batch` authority).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SubstreamEntry {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    /// Absolute base offset of this sub-stream's first record in the segment.
    pub(crate) base_offset: i64,
    /// Offsets this sub-stream occupies
    /// (`last_offset == base_offset + record_count - 1`).
    pub(crate) record_count: i64,
    /// Byte offset of this sub-stream's contiguous region within the segment.
    pub(crate) byte_start: u64,
    /// Byte length of that region (its batches, wire-encoded and concatenated).
    pub(crate) byte_len: u64,
    /// Greatest record timestamp in the sub-stream, read by per-prefix
    /// whole-segment retention (#61) to decide expiry without a body read.
    pub(crate) max_timestamp: i64,
    /// Producer coordinates of the idempotent/transactional batches in this
    /// sub-stream's region, in region (offset) order (#87, footer v2). Empty in a
    /// v1 footer and for non-idempotent batches. Consumed by log-based idempotent
    /// dedup (#88) so duplicate detection derives from the durable log rather than
    /// a lazily-checkpointed `producers/{id}.json`.
    producers: Vec<ProducerCoord>,
}

/// Why a [`DynoStore::decode_frame`] scan stopped (#386).
///
/// The bytes cannot say whether stopping early is benign, so the scan reports
/// where and why and leaves the verdict to [`DynoStore::decode_region`], which
/// holds the footer entry the bytes were read for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameTail {
    /// Every byte was consumed by whole batches.
    Exhausted,

    /// Fewer bytes remain than a frame header needs, so no length was read.
    Short { at: usize, remaining: usize },

    /// A `batch_length` that no frame can carry: negative, or running past the
    /// bytes that remain.
    Malformed { at: usize, declared: i32 },
}

impl FrameTail {
    /// Byte offset within the region where the scan stopped.
    fn at(&self) -> usize {
        match self {
            Self::Exhausted => 0,
            Self::Short { at, .. } | Self::Malformed { at, .. } => *at,
        }
    }

    /// The `batch_length` read where the scan stopped, if one was read.
    fn declared(&self) -> Option<i32> {
        match self {
            Self::Malformed { declared, .. } => Some(*declared),
            _ => None,
        }
    }
}

/// What one buffered region read produced (#426).
///
/// The serial loop could `break` out of the middle of an iteration; a buffered
/// stage cannot, so each read reports its outcome and the caller interprets them
/// in input order. `buffered` preserves that order, and the offset arithmetic is
/// per-segment (`running = entry.base_offset`), so the regions are independent
/// and the assembled result is byte-identical to the serial shape.
enum RegionOutcome {
    /// The region decoded. Its batches still carry the encoded base offsets; the
    /// caller re-bases them from the entry, as it always did.
    Decoded(Vec<deflated::Batch>),

    /// The object was gone by the time it was read (compaction #66 / retention
    /// #61). The caller prunes, reconciles and restarts.
    Vanished,

    /// The index entry and the object disagreed, and the object's own trailer
    /// has replaced the cached footer (#397, #432). The caller restarts off the
    /// corrected entry rather than mixing a corrected extent with a stale span.
    Corrected,
}

/// A footer entry paired with the bytes read for it (#386): what
/// [`DynoStore::decode_region`] needs to classify a frame scan that stopped
/// early, and to name the segment in the error if it was damage.
struct RegionRead<'a> {
    prefix: &'a str,
    seq: u64,
    entry: &'a SubstreamEntry,
    encoded: &'a Bytes,
}

impl RegionRead<'_> {
    /// Frame header bytes reported in a diagnostic.
    const HEAD: usize = size_of::<i64>() + size_of::<i32>();

    /// Whether the read came back short of the extent the footer claims — a torn
    /// or partially-visible object rather than a footer that disagrees with its
    /// payload. The whole discrimination #386 asked for rests on this, so it is
    /// one named predicate and not an inline comparison.
    fn truncated(&self) -> bool {
        (self.encoded.len() as u64) < self.entry.byte_len
    }

    /// The diagnostic for this read, with the scan's stopping point folded in.
    fn region(&self, at: usize, declared: Option<i32>, detail: String) -> CorruptRegion {
        let head = self
            .encoded
            .get(at..)
            .unwrap_or_default()
            .iter()
            .take(Self::HEAD)
            .fold(String::new(), |mut head, byte| {
                let _ = write!(head, "{byte:02x}");
                head
            });

        CorruptRegion {
            prefix: self.prefix.to_owned(),
            seq: self.seq,
            topic: self.entry.topic.clone(),
            partition: self.entry.partition,
            base_offset: self.entry.base_offset,
            byte_start: self.entry.byte_start,
            byte_len: self.entry.byte_len,
            read_len: self.encoded.len(),
            at,
            declared,
            head,
            detail,
        }
    }

    /// Report the region as damaged, counted and logged with everything needed to
    /// tell the two causes apart on the next occurrence.
    fn corrupt(&self, at: usize, declared: Option<i32>, detail: String) -> Error {
        let region = self.region(at, declared, detail);

        SEGMENT_REGION_CORRUPT.add(1, &[]);
        error!(?region, "segment region does not begin at a batch frame");

        Error::CorruptSegment(Box::new(region))
    }

    /// Report the read as short of the extent its footer entry claims (#397).
    ///
    /// The same `CorruptRegion` payload as [`Self::corrupt`], under its own
    /// counter, because the two say different things about *what* is wrong: a
    /// full-length read that holds no frame means the entry does not describe
    /// the region, while a short read means the entry claims bytes the object
    /// does not have at all.
    fn short_of_extent(&self, detail: String) -> Error {
        let region = self.region(self.encoded.len(), None, detail);

        SEGMENT_REGION_TRUNCATED.add(1, &[]);
        error!(?region, "segment region read short of its footer extent");

        Error::CorruptSegment(Box::new(region))
    }
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
pub(crate) struct ProducerCoord {
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
pub(crate) struct SegmentFooter {
    /// The lease epoch of the writer that produced this segment (#59). `0` when
    /// prefix leasing is not in effect. Stamped so a stale-epoch segment (from a
    /// fenced writer) is identifiable on read/recovery.
    pub(crate) writer_epoch: i64,
    /// Per-flush nonce (#87, footer v2): lets a writer recognise its own segment
    /// after an ambiguous PUT (create succeeded but the response was lost) and
    /// adopt it instead of re-writing the batch at the next sequence (#89). `0` in
    /// a v1 footer.
    nonce: u64,
    pub(crate) entries: Vec<SubstreamEntry>,
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
    /// The last-stable-offset floor contributed by each still-open transaction,
    /// as the minimum `offset_start` per topition over every epoch detail that
    /// is neither `Committed` nor `Aborted`.
    ///
    /// A read-committed consumer may not see past the first offset written by a
    /// transaction that has not yet resolved. Both `offset_stage` and
    /// `list_offsets` answer with this floor, and the two must agree: this is
    /// the single definition, so a change to the open-transaction predicate
    /// (transaction-state pruning, a new `TxnState`) reaches `Fetch` and
    /// `ListOffsets` together (#286).
    fn open_transaction_floors(&self) -> BTreeMap<Topition, Offset> {
        let mut floors = BTreeMap::new();

        for txn in self.transactions.values() {
            debug!(?txn);

            for detail in txn.epochs.values().filter(|detail| {
                detail
                    .state
                    .is_some_and(|state| state != TxnState::Committed && state != TxnState::Aborted)
            }) {
                for (topition, offset_start) in BTreeMap::<Topition, Offset>::from(detail) {
                    _ = floors
                        .entry(topition)
                        .and_modify(|existing: &mut Offset| {
                            if *existing > offset_start {
                                *existing = offset_start
                            }
                        })
                        .or_insert(offset_start);
                }
            }
        }

        debug!(?floors);

        floors
    }

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
                // Kafka's list-valued APPEND/SUBTRACT are not implemented.
                // `config_operation` is a wire field, so a client picks it —
                // panicking the request task on one was remote input deciding
                // broker liveness (#276). Refuse the operation instead.
                OpType::Append | OpType::Subtract => {
                    error!(
                        config = change.name,
                        operation = change.config_operation,
                        "IncrementalAlterConfigs APPEND/SUBTRACT are not supported"
                    );

                    return Err(Error::Api(ErrorCode::InvalidConfig));
                }
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

/// The pinned routing prefix at `topic-routing/{name}.json`: the prefix a topic's
/// records coalesce under, decided **once, at creation**, and never re-derived
/// (#236).
///
/// Written create-only alongside the topic, and immutable for its lifetime — which
/// is what lets every reader cache it permanently, with no TTL and no staleness
/// argument, exactly as `topic-ids/{uuid}.json` already is.
///
/// It replaces a derivation from `cleanup.policy`, and that is a correctness fix
/// as much as a cost one. The prefix selects the create-CAS namespace a batch's
/// offsets are assigned from, so while it was derived from mutable config, an
/// `AlterConfigs` setting `cleanup.policy=compact` on a live topic opened a window
/// where one pod routed to the dedicated prefix and a peer, holding a staler
/// verdict, still routed to the connector prefix — two offset authorities for the
/// same `(topic, partition)`, the #78 class that #177/#178 made impossible
/// everywhere else. Pinned, `cleanup.policy` can change freely without moving
/// where records live.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TopicRouting {
    prefix: String,
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
/// round-tripped through [`OptiCon::with_mut`] ([`DynoStore::expire_prefix_segments`]
/// persists `high`, [`DynoStore::delete_records_before`] the truncation floor), so
/// without it a rolling deploy would let an old process silently erase any field a
/// newer one had just written — and it is what preserves the historic `"low"` of an
/// object written before #180 dropped that field — for the truncation floor (`truncate`, #176) that
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
    /// What the last segment expiry left servable (#290): `end` is the tail of
    /// the segments that survived it, `at_high` the assignment floor (`high`)
    /// written by the same CAS. Read paths honor the pair only while
    /// `at_high == high`: a floor moved by a writer that did not re-certify —
    /// an older binary's expiry, which round-trips the pair untouched through
    /// `rest` — invalidates it, falling back to pre-#290 behaviour (a fetch in
    /// the gap answers empty rather than `OffsetOutOfRange`). Same byte-layout
    /// discipline as `truncate`: absent until an expiry writes it, so existing
    /// watermark objects are not rewritten on first touch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    served: Option<ServedEnd>,
    #[serde(flatten)]
    rest: BTreeMap<String, serde_json::Value>,
}

/// The `{end, at_high}` pair certifying what a segment expiry left servable
/// (#290). A single nested object so the two halves can never be split by a
/// partial rewrite: either both round-trip, or the pair is dropped whole.
///
/// `floor > segment tail` is locally ambiguous between two states that demand
/// opposite treatment: a peer may have acked offsets this process never
/// listed (the floor is the log end; regressing under acked offsets re-reads
/// or reuses them), or retention may have deleted the tail-holding segment
/// while a lower one survived (the floor advertises offsets no segment holds,
/// and a consumer parked on them polls empty forever — the #290 wedge). Only
/// the expiry that performed the delete knows which, and this is it saying
/// so: every offset in `[end, at_high)` was destroyed by that expiry, and the
/// seq-floor fence (#77, #316) forbids assigning new offsets below `at_high`,
/// so nothing can ever appear in the gap again.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ServedEnd {
    end: i64,
    at_high: i64,
}

impl ServedEnd {
    /// Whether this certification still describes the current floor, i.e. no
    /// uncertified writer has moved `watermark.high` since it was written.
    fn certifies(&self, high: i64) -> bool {
        self.at_high == high
    }

    /// Whether `offset` falls in the gap this certification declares dead:
    /// at/above the surviving tail, below the floor the expiry raised.
    fn gap_contains(&self, offset: i64) -> bool {
        offset >= self.end && offset < self.at_high
    }
}

/// A `watermark.json` read cached for a prefix-coalesced sub-stream: the
/// assignment floor (`high`), its served-end certification (#290), and the
/// certified seq floor the read was performed under — the pairing that makes
/// the cache valid (see [`DynoStore::cached_coalesced_watermark`]).
#[derive(Clone, Copy, Debug)]
struct CachedWatermark {
    high: i64,
    served: Option<ServedEnd>,
    seq_floor: u64,
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
        let cache = Arc::new(Cache::new(
            Metron::new(object_store, cluster),
            Duration::from_millis(5_000),
        ));

        Self {
            cluster: cluster.into(),
            node,
            advertised_listener: Url::parse("tcp://127.0.0.1/").unwrap(),
            watermarks: Arc::new(Mutex::new(BTreeMap::new())),
            next_offsets: Arc::new(Mutex::new(BTreeMap::new())),
            coalesced_watermark_floors: Arc::new(Mutex::new(BTreeMap::new())),
            served_end_reconciled: Arc::new(Mutex::new(BTreeSet::new())),
            truncate_floors: Arc::new(Mutex::new(BTreeMap::new())),
            prefix_read_sync_locks: Arc::new(Mutex::new(BTreeMap::new())),
            producers: Arc::new(Mutex::new(BTreeMap::new())),
            oldest_retained_prefix: Arc::new(Mutex::new(BTreeMap::new())),
            prefix_coalesce_buffers: Arc::new(Mutex::new(BTreeMap::new())),
            segment_seqs: Arc::new(Mutex::new(BTreeMap::new())),
            prefix_flush_locks: Arc::new(Mutex::new(BTreeMap::new())),
            prefix_index: Arc::new(Mutex::new(BTreeMap::new())),
            segment_reads: Arc::new(Mutex::new(BTreeMap::new())),
            group_offsets: Arc::new(Mutex::new(BTreeMap::new())),
            quarantined_segments: Arc::new(Mutex::new(BTreeMap::new())),
            compaction_leases: Arc::new(Mutex::new(BTreeMap::new())),
            era_epochs: Arc::new(Mutex::new(BTreeMap::new())),
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
            routing_prefixes: Arc::new(Mutex::new(BTreeMap::new())),
            auto_create: AutoTopicCreate::default(),
            topic_defaults: TopicDefaults::default(),
            meta: OptiCon::<Meta>::new(cluster),
            #[cfg(test)]
            metadata_etags: cache.clone(),
            object_store: cache,
        }
    }

    /// Expire the metadata cache's etag memo, as its 5s window does — so an
    /// op-profile test can count revalidations across windows without sleeping
    /// through them (#167).
    #[cfg(test)]
    fn expire_metadata_etags(&self) {
        self.metadata_etags.expire_cached_etags();
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

    /// The broker-level topic config defaults this engine injects into every topic
    /// it creates (#225).
    pub fn topic_defaults(self, topic_defaults: TopicDefaults) -> Self {
        Self {
            topic_defaults,
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

    /// The pinned routing prefix object for `name` (see [`TopicRouting`]).
    ///
    /// A prefix of its own, not a field of `topic-metadata/{name}.json`: that
    /// object carries genuinely mutable config, so a permanent cache of it would
    /// be wrong, and a cache of one field of it would be a discipline someone has
    /// to maintain. A separate immutable object makes it structural. It also keeps
    /// the pin out of [`Self::all_topics`]'s listing of `topic-metadata/`.
    fn topic_routing_path(&self, name: &str) -> Path {
        Path::from(format!(
            "clusters/{}/topic-routing/{}.json",
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

    /// How long a [`TopicIndex`] snapshot is served before a refresh.
    ///
    /// Since #387 this bounds the staleness of **every** `Metadata` answer, not
    /// just the list-all view: the by-name path is served from this index too, so
    /// a change made on another replica — a create, a delete, an `AlterConfigs`
    /// or a `CreatePartitions` — becomes visible here within one window. (A
    /// create is the exception that stays immediate: a name the index does not
    /// hold falls back to the topic's own object, which resolves it at once. The
    /// window is a delay on *changes to* and *removals of* topics the index
    /// already lists.)
    ///
    /// 30s rather than the 5s it was while the by-name path read one object per
    /// topic. That 5s was not a freshness requirement — it was #167's blocker,
    /// where a stale `cleanup.policy` verdict meant two pods routing one
    /// partition to different prefixes. #236/#242 pinned the routing prefix in an
    /// immutable object, so what is left here is ordinary bounded staleness, and
    /// every Kafka client already caches metadata for `metadata.max.age.ms`
    /// (default 5 minutes) — an order of magnitude looser than this.
    ///
    /// What the window costs is the LIST that refreshes it, once per window per
    /// replica, against a plane that used to cost one conditional GET per topic
    /// per window per replica: ~1,040 revalidations/s and 63% of the remaining S3
    /// request bill on the production fleet (#387). Shortening it back does not
    /// re-break correctness, it re-buys that bill — which is why
    /// `warm_metadata_by_name_costs_no_per_topic_get` pins the request count
    /// rather than the number.
    const TOPIC_INDEX_TTL: Duration = Duration::from_secs(30);

    /// How long a per-partition high-watermark hint is served from memory before
    /// the read path re-lists the tail (see [`OffsetHint`] / [`Self::cached_high_fresh`]).
    /// Bounds the cross-replica staleness of the read-uncommitted high watermark:
    /// a batch produced on another replica becomes visible within this window,
    /// while a caught-up consumer long-polling an idle partition issues no
    /// `ListObjectsV2` per poll in steady state (#40). Read-committed is
    /// unaffected in semantics — a stale (lower) high watermark can only *delay*
    /// visibility, never expose unstable offsets.
    const HIGH_WATERMARK_HINT_TTL: Duration = Duration::from_secs(5);

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

    /// Newest segments never compacted (#66): the actively-produced tail is left
    /// out of the merge, so compaction never rewrites a segment a producer is
    /// still appending behind.
    ///
    /// This does **not** keep compaction out of the producers' race, which an
    /// earlier wording of this comment claimed. `keep_hot` constrains which
    /// segments are *merged*; the merged object is still *created* at the next
    /// tail sequence, in the same `PutMode::Create` namespace produce uses, so
    /// the compactor remains a contender for it (#130) — and a costly one, since
    /// a lost claim re-uploads the whole merged payload. That is the shape of a
    /// production incident, not a theoretical concern: a large compaction PUT
    /// losing repeatedly against a hot prefix has exhausted the claim budget and
    /// taken produce down with it.
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

    /// Cap on [`Self::quarantined_segments`] per prefix (#398).
    ///
    /// The set is one `u64` per known-bad object and production holds ~900 of
    /// them across the fleet, so this is far above the incidence it exists for.
    /// Past the cap a further bad segment is logged and *not* recorded, which
    /// restores the pre-#398 behaviour for it — the drain ends on that run — in
    /// preference to letting a pathological prefix grow the set without bound.
    const PREFIX_QUARANTINE_CAP: usize = 4_096;

    /// How often one prefix's index may be reconciled downwards by a listing
    /// (#408).
    ///
    /// The pass is triggered by a 404 — proof that this replica's index names an
    /// object that is gone — so the rate is bounded by how many *distinct*
    /// prefixes are proved stale, not by the 404 rate. Without a gate a fetch
    /// storm against one stale prefix would buy a tier-1 listing per fetch.
    ///
    /// Five minutes is chosen against the maintenance interval rather than the
    /// index TTL (5s, far too short to amortise a listing): compaction retires
    /// segments in bursts one tick apart, so re-listing much faster than that
    /// pays for a staleness that has not accrued yet.
    const PREFIX_INDEX_RECONCILE_INTERVAL: Duration = Duration::from_secs(300);

    /// All topics, served from the in-memory [`TopicIndex`]. Returns the shared
    /// snapshot if fresh; otherwise refreshes it (single-flight) by LISTing the
    /// `topic-metadata/` prefix and GETting only the objects whose etag changed.
    /// Used by the list-all metadata path, the by-name metadata path (#387) and
    /// the cleanup policies — never on the produce/fetch hot path.
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

        TOPIC_INDEX_REFRESH_OBJECTS
            .add(fetched.len() as u64, &[KeyValue::new("outcome", "fetched")]);
        TOPIC_INDEX_REFRESH_OBJECTS
            .add(entries.len() as u64, &[KeyValue::new("outcome", "reused")]);

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

    /// A topic's metadata read from its own `topic-metadata/{name}.json`: one
    /// conditional GET, always fresh.
    ///
    /// The authority, and the only thing that may decide a routing derivation or
    /// a lifecycle operation. Read paths that only *describe* a topic should go
    /// through [`Self::described_topic_metadata`] instead — this costs a request
    /// per topic per etag-memo window, which is the ~1,040/s plane #387 removed.
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

    /// A topic's metadata from the in-memory [`TopicIndex`], at **zero**
    /// object-store requests, or `None` when the index cannot answer.
    ///
    /// `None` is not a claim of absence. It means the index does not hold the name
    /// — because the topic does not exist, or because it was created after the
    /// snapshot was taken — or that the snapshot has aged past
    /// [`Self::TOPIC_INDEX_TTL`]. Every caller therefore falls back to the topic's
    /// own object, which is what keeps a freshly created topic immediately
    /// resolvable (#28).
    ///
    /// Deliberately does **not** refresh: the caller refreshes once for a whole
    /// request (see [`Self::refresh_index_for_described_reads`]). A per-topic
    /// refresh here would serialise behind the index's single-flight lock — so a
    /// `Metadata` naming 1,500 topics against a failing object store would attempt
    /// 1,500 sequential LISTs rather than one.
    ///
    /// A topic-id is resolved to a name through the permanently-cached
    /// `topic-ids/{uuid}.json` pointer rather than by scanning the snapshot: the
    /// mapping is immutable for a topic's lifetime, so it costs one GET per id per
    /// process, where a scan would be O(topics) per lookup — 15k comparisons per
    /// topic on a by-id `Metadata`.
    async fn indexed_topic_metadata(&self, topic: &TopicId) -> Result<IndexedTopic> {
        if self.fresh_topic_index()?.is_none() {
            return Ok(IndexedTopic::Stale);
        }

        let name = match topic {
            TopicId::Name(name) => name.clone(),
            TopicId::Id(id) => match self.topic_name_by_id(id).await? {
                Some(name) => name,
                None => return Ok(IndexedTopic::UnknownId),
            },
        };

        Ok(self
            .topic_index
            .lock()?
            .entries
            .get(name.as_str())
            .map(|(_, metadata)| IndexedTopic::Hit(metadata.clone()))
            .unwrap_or(IndexedTopic::FreshMiss))
    }

    /// A topic's metadata for a path that only *describes* it: served from the
    /// [`TopicIndex`] when the index holds it, falling back to the topic's own
    /// object otherwise (#387).
    ///
    /// This is the whole of #387. `Metadata` maps every requested topic to a
    /// per-topic read 32-way concurrently, and consumers here subscribe to
    /// hundreds of topics each, so one client refreshing metadata was ~100
    /// conditional GETs — 2,081 lookups/s against 21.5 `Metadata` requests/s on
    /// the production fleet, of which ~1,040/s reached S3 as a `304`, ~$38/day and
    /// 63% of the remaining request bill. The index answers all of them from one
    /// LIST per window per replica: a cost that does not scale with the topic
    /// count, the same reasoning as #112's per-prefix manifest.
    ///
    /// Not for anything that must be fresh. In particular `describe_config` stays
    /// on [`Self::topic_metadata`], because [`Self::topic_is_compacted`] reads
    /// through it to derive a routing prefix for a pre-#236 topic, and that
    /// derivation is pinned permanently — a stale verdict there is unreachable
    /// data, not a stale answer. Lifecycle operations (`create_topic`,
    /// `delete_topic`, `AlterConfigs`) likewise read the object.
    ///
    /// `caller` labels [`TOPIC_METADATA_READS`], so the residual `source="object"`
    /// population is attributable per call site rather than inferred from the API
    /// mix.
    async fn described_topic_metadata(
        &self,
        topic: &TopicId,
        caller: &'static str,
    ) -> Result<Option<TopicMetadata>> {
        let indexed = self
            .indexed_topic_metadata(topic)
            .await
            .inspect_err(|error| warn!(?error, caller, ?topic, "indexed topic metadata"))
            // A failed index read is not a usable index, which is exactly the
            // case the object fallback exists for.
            .unwrap_or(IndexedTopic::Stale);

        let source = if matches!(indexed, IndexedTopic::Hit(_)) {
            "index"
        } else {
            "object"
        };

        // `index` alongside `source` (#407): a `fresh_miss` is a fallback that can
        // only confirm the topic does not exist, and a `stale` is the fallback
        // doing the job it was added for. `source="object"` alone cannot tell
        // them apart, so it could not say whether skipping the fallback on a
        // fresh index would cost any visibility at all.
        TOPIC_METADATA_READS.add(
            1,
            &[
                KeyValue::new("caller", caller),
                KeyValue::new("source", source),
                KeyValue::new("index", indexed.as_str()),
            ],
        );

        match indexed {
            IndexedTopic::Hit(metadata) => Ok(Some(metadata)),

            // The pointer has already been read and did not resolve.
            // `topic_metadata` would read the same key again, through the same
            // positives-only cache, and return `Ok(None)` — so this is the one
            // arm where the fallback is provably a no-op rather than a
            // visibility guarantee (#407).
            IndexedTopic::UnknownId => Ok(None),

            // Both keep the fallback, and for different reasons: `Stale` because
            // there is no usable index at all, `FreshMiss` because a fresh index
            // is *not* authoritative for absence — see [`IndexedTopic`].
            IndexedTopic::FreshMiss | IndexedTopic::Stale => self.topic_metadata(topic).await,
        }
    }

    /// Refresh the [`TopicIndex`] once for a request that is about to resolve
    /// topics through [`Self::described_topic_metadata`].
    ///
    /// Best-effort: a failed refresh is not a failed request. Every topic then
    /// misses the index and falls back to its own object, which is exactly the
    /// pre-#387 behaviour — so a LIST that cannot be served degrades the cost, not
    /// the answers.
    async fn refresh_index_for_described_reads(&self, caller: &'static str) {
        if let Err(error) = self.topics_index().await {
            warn!(
                ?error,
                caller, "topics index unavailable; falling back to per-topic reads"
            );
        }
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
    /// The log start comes from the footer index — the lowest surviving segment
    /// base, which is what `list_offsets` EARLIEST reports.
    ///
    /// It used to come from `watermark.low`, which only the legacy retention paths
    /// ever advanced: authoritatively silent for a segment-backed sub-stream, and
    /// usually absent entirely, yet read-committed `offset_stage` read it on
    /// **every poll** — ~1490 GET/s answered `404 NoSuchKey` at ~1,600
    /// subscriptions (#161), round trips that resolved nothing and are billable on
    /// a store that charges 4xx. The index is also *more* accurate: nothing
    /// advances that field after a segment expiry. Since #179 there is no legacy
    /// region left to own a log start, so this is unconditional.
    ///
    /// Either way the result is clamped to the truncation floor (#176):
    /// `DeleteRecords` hides segment-resident records without touching the
    /// shared segments, so the physical region start can sit below the
    /// logical log start.
    /// When NO segment survives, the log is empty — and an empty log starts
    /// where it ends, so the answer is `high_watermark` (#290).
    ///
    /// It used to be 0, which is a false statement about what the broker holds
    /// the moment the high watermark is above it, and the falsehood is exactly
    /// what made a damaged partition indistinguishable from a healthy one: a
    /// prefix advertising `LOG-START-OFFSET=0 / LOG-END-OFFSET=3024895` with
    /// nothing readable at any offset reported 3M of lag that no consumer could
    /// ever retire. Reporting the log end instead collapses that lag to zero,
    /// which is what makes the gap visible through ordinary metadata rather than
    /// by probing every offset by hand.
    ///
    /// A fully expired log lands in the same place, correctly: retention removes
    /// every segment, so its start becomes its end and it reports empty.
    async fn log_start(&self, topition: &Topition, high_watermark: i64) -> Result<i64> {
        let start = self
            .segment_region_start(topition)
            .await?
            .unwrap_or(high_watermark);

        Ok(start.max(self.truncate_floor(topition).await?))
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

    /// The persisted `watermark.high`: a durable lower bound on the tail offset,
    /// used as a listing floor so a cold reader scans only forward (S3
    /// `start-after`) rather than the whole partition.
    ///
    /// This is the *assignment* floor and deliberately ignores the #290
    /// served-end certification: `leaseless_base` folds this value so a freed
    /// offset is never reused, and that must hold whether or not the offsets
    /// above the surviving tail are still fetchable.
    async fn persisted_high(&self, topition: &Topition) -> Result<i64> {
        self.watermark(topition)?
            .with(&self.object_store, |watermark| {
                Ok(watermark.high.unwrap_or(0))
            })
            .await
    }

    /// The persisted `watermark.high` together with the served-end
    /// certification (#290), in the single GET the read path already pays.
    async fn persisted_watermark_bounds(
        &self,
        topition: &Topition,
    ) -> Result<(i64, Option<ServedEnd>)> {
        self.watermark(topition)?
            .with(&self.object_store, |watermark| {
                Ok((watermark.high.unwrap_or(0), watermark.served))
            })
            .await
    }

    /// The cached `watermark.high` floor (and its #290 served-end
    /// certification, if any) for a prefix-coalesced sub-stream, valid only
    /// when it was read under the still-current certified seq `floor` (see
    /// [`Self::coalesced_watermark_floors`]). `None` means the caller must pay
    /// the `watermark.json` GET (once — the slow path caches it via
    /// [`Self::cache_coalesced_watermark`]).
    fn cached_coalesced_watermark(
        &self,
        topition: &Topition,
        floor: u64,
    ) -> Result<Option<(i64, Option<ServedEnd>)>> {
        self.coalesced_watermark_floors
            .lock()
            .map(|locked| {
                locked.get(topition).and_then(|cached| {
                    (cached.seq_floor == floor).then_some((cached.high, cached.served))
                })
            })
            .map_err(Into::into)
    }

    /// Cache `high` and `served` (a just-read `watermark.json`) for `topition`
    /// under the certified seq `floor` that was current *at or before* the
    /// read. Pairing with an older floor is safe: any watermark advance after
    /// the read raises the floor above `floor`, invalidating this entry; an
    /// advance before the read is already contained in `high`.
    fn cache_coalesced_watermark(
        &self,
        topition: &Topition,
        high: i64,
        served: Option<ServedEnd>,
        floor: u64,
    ) -> Result<()> {
        self.coalesced_watermark_floors
            .lock()
            .map(|mut locked| {
                _ = locked.insert(
                    topition.to_owned(),
                    CachedWatermark {
                        high,
                        served,
                        seq_floor: floor,
                    },
                );
            })
            .map_err(Into::into)
    }

    /// The stale-hint high watermark of a prefix-coalesced sub-stream served
    /// from the in-memory segment index alone — no per-partition object-store
    /// request. `None` means the index is not authoritative for this
    /// sub-stream and the caller must fall back to the `watermark.json` GET.
    ///
    /// Correctness (LATEST must equal the true high watermark exactly):
    ///
    /// - **The true high** is `max(tail across live segments, persisted
    ///   `watermark.high`)`: segments
    ///   are the offset-assignment authority, and the only way assigned
    ///   offsets leave them is retention/compaction, where
    ///   `expire_prefix_segments` persists each affected sub-stream's tail
    ///   into `watermark.high` write-ahead of the delete (retention advances
    ///   the log *start*, never lowers the log *end*).
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
    /// - **No segments at all** is served the same way (#167): a sub-stream
    ///   that never produced, or was fully drained, has `watermark.json` as its
    ///   only authority — and that field is advanced by exactly one operation,
    ///   [`Self::expire_prefix_segments`], which raises the seq floor
    ///   immediately after. The certification argument above therefore does not
    ///   depend on the sub-stream owning a segment: an unchanged floor proves
    ///   the cached value current whether the tail is a segment or nothing at
    ///   all. A cold or floor-invalidated cache still declines to the slow path
    ///   below, so the first read still pays the GET.
    ///
    ///   This case was excluded for lake-sink topics, whose high advanced
    ///   without raising the floor. That engine went with the lakehouse (#96) —
    ///   nothing in this fork writes `watermark.high` outside segment expiry —
    ///   so the exclusion was charging one conditional GET per poll per drained
    ///   partition (~660/s, a third of the fleet's 304 plane) to protect an
    ///   authority that cannot move behind the floor.
    async fn coalesced_high_from_index(&self, topition: &Topition) -> Result<Option<i64>> {
        let prefix = self.routed_prefix_of(topition).await?;
        self.refresh_prefix_index(&prefix).await?;

        // `None` for a sub-stream holding no segment: the persisted floor below is
        // then the whole answer, which is what the slow path would fold too. Kept
        // as an `Option` because "holds segments at all" is what separates the
        // state #290 is about from a drained partition (#299) — see
        // [`Self::note_floor_above_tail`].
        let tail = self
            .valid_substream_segments(&prefix, topition.topic(), topition.partition())?
            .last()
            .map(|(_, entry)| entry.base_offset + entry.record_count);

        // Certified after the refresh above, so the floor covers every
        // watermark advance whose segment deletion that listing could have
        // reflected. One GET per prefix per listing generation, amortized
        // across every partition of the prefix — not per partition.
        let floor = self.certified_seq_floor(&prefix).await?;
        let Some((watermark_floor, _served)) = self.cached_coalesced_watermark(topition, floor)?
        else {
            return Ok(None);
        };

        self.note_floor_above_tail(&prefix, topition, tail, watermark_floor);

        // The same fold the slow path performs — `recover_substream_next_offset`
        // is `max(segment tail, persisted floor)` — so this serves the
        // value that path would have computed, without its per-partition GET.
        let high = tail
            .unwrap_or(0)
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

    /// The `[end, at_high)` gap certified dead by the last segment expiry, when
    /// one exists for `topition` and still describes the current floor (#290).
    ///
    /// Served purely from in-process caches — the coalesced watermark cache
    /// that the high-watermark read populates — so an empty fetch pays no
    /// extra object request to consult it (deliberately unlike #292's
    /// confirming read, removed in #314). A cold cache answers `None`,
    /// degrading to today's empty response until the read path warms it.
    async fn certified_dead_gap(&self, topition: &Topition) -> Result<Option<ServedEnd>> {
        let prefix = self.routed_prefix_of(topition).await?;
        let floor = self.certified_seq_floor(&prefix).await?;

        Ok(self
            .cached_coalesced_watermark(topition, floor)?
            .and_then(|(high, served)| {
                served.filter(|served| served.certifies(high) && served.end < served.at_high)
            }))
    }

    /// Record that the persisted floor sits above the surviving segment tail, when
    /// the sub-stream still holds segments (#290).
    ///
    /// `tail` is `None` for a sub-stream with no segment at all, which is *not* this
    /// state: a fully drained partition legitimately has the floor as its only
    /// authority, and #299 already makes it report a log that starts where it ends.
    /// The state worth counting is the partial one — records still served below the
    /// tail, offsets advertised above it, nothing in between.
    ///
    /// Free: both operands are already resolved by the caller. No request, no
    /// listing, no confirming read — deliberately unlike #292's detector, which paid
    /// for all three per empty fetch and was removed in #314 once the condition
    /// measured ~10/min on healthy data.
    ///
    /// Debug rather than warn, and no error code, because the condition does not
    /// imply damage: a peer replica that acked offsets this process never listed
    /// produces the same arithmetic, and there the floor is the correct answer. What
    /// is missing today is not an alarm but a magnitude — which of the candidate
    /// fixes is worth its cost depends on whether this fires once a week or
    /// constantly.
    fn note_floor_above_tail(
        &self,
        prefix: &str,
        topition: &Topition,
        tail: Option<i64>,
        watermark_floor: i64,
    ) {
        if let Some(gap) = floor_above_tail(tail, watermark_floor) {
            WATERMARK_ABOVE_SEGMENT_TAIL.add(1, &[KeyValue::new("prefix", prefix.to_string())]);

            debug!(
                ?topition,
                tail, watermark_floor, gap, "advertising offsets no surviving segment holds (#290)"
            );
        }
    }

    /// The log end offset (high watermark) for `topition`.
    ///
    /// The authority is the immutable batch objects. The tail listing is floored
    /// at the best known lower bound — the in-memory hint, or the persisted
    /// `watermark.high` — so a *cold* reader (empty hint after a restart or on
    /// another replica) lists only the batches *after* that floor via S3
    /// `start-after`, instead of scanning the whole partition (the cold-LIST
    /// storm at scale). A read never writes that floor back: `watermark.high` is
    /// advanced only by [`Self::expire_prefix_segments`], write-ahead of the
    /// segment deletes it is about to perform — no read path CASes it (#13), and
    /// that single writer is what lets the floor-certified cache stand in for the
    /// object (see [`Self::coalesced_high_from_index`]).
    async fn high_watermark(&self, topition: &Topition) -> Result<i64> {
        // Warm fast path: serve from the in-memory hint without ANY per-poll S3
        // request while it is fresh (reconciled against a listing within the TTL).
        // Every hint refresh (`mark_listed`) already folds in `from_watermark` —
        // see the `mark_listed` call sites below — so a fresh hint needs neither
        // the tail `ListObjectsV2` (#40) nor the `watermark.json` GET (#72). This
        // is what takes the consumer Fetch hot path off ~1 GET per poll per
        // partition. Bounded staleness (== the hint TTL): another replica's
        // just-produced batch is picked up on the next TTL-triggered listing
        // below.
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

        // Index not authoritative for this sub-stream — a cold or
        // floor-invalidated watermark cache. Pay the `watermark.json` GET
        // and recover footer-only (#58), caching the watermark under the
        // certified floor read *before* it so the fast path serves the
        // next stale-hint resolution.
        let prefix = self.routed_prefix_of(topition).await?;
        let floor = self.certified_seq_floor(&prefix).await?;
        let (from_watermark, served) = self.persisted_watermark_bounds(topition).await?;

        // Unconditionally cacheable again (#179). The certification argument is
        // "`watermark.high` advances only in `expire_prefix_segments`, which raises
        // the seq floor immediately after", and that is true once more: legacy
        // retention — the second writer #241 had to gate against — went with the
        // layout it maintained, so there is no writer left that moves this value
        // without raising a floor.
        self.cache_coalesced_watermark(topition, from_watermark, served, floor)?;

        // Counted here too, or the measurement would be blind to exactly the reader
        // most likely to meet the state: a cold one, whose watermark cache the fast
        // path declined (#290).
        self.note_floor_above_tail(
            &prefix,
            topition,
            self.valid_substream_segments(&prefix, topition.topic(), topition.partition())?
                .last()
                .map(|(_, entry)| entry.base_offset + entry.record_count),
            from_watermark,
        );

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

    /// The prefix `topition`'s records are segment-routed under: the **pinned**
    /// value (#236), read once per process and then served from memory forever.
    ///
    /// Three steps, in cost order:
    ///
    /// 1. Topics whose connector prefix already equals their own name (fewer than
    ///    three dotted components) need nothing at all — both routings agree, so
    ///    there is no decision to pin and no request to make.
    /// 2. The permanent memo. Sound because the pin is immutable, which is the
    ///    property that makes this whole path free; see [`TopicRouting`].
    /// 3. Otherwise read the pin. A topic created before pinning existed has none,
    ///    so the fallback **reproduces exactly today's derivation** —
    ///    [`Self::prefix_of`] plus the [`Self::topic_is_compacted`] verdict — and
    ///    pins that answer, create-only. Reproducing it is not a nicety: a
    ///    different answer would route the topic's new records to a prefix its
    ///    existing segments are not under, which is not a cost regression but data
    ///    becoming unreachable.
    ///
    /// The lazy pin is create-only so peers converge: whoever writes first wins,
    /// and a loser adopts the winner's value rather than keeping its own. Without
    /// that, two pods that derived different answers — possible for exactly as long
    /// as the old 5s window was open, if an `AlterConfigs` lands between their
    /// reads — would each cache their own permanently, turning a bounded window
    /// into a permanent split. The pin is the tie-breaker.
    async fn routed_prefix_of(&self, topition: &Topition) -> Result<String> {
        let topic = topition.topic();

        let prefix = self.prefix_of(topition);
        if prefix == topic {
            return Ok(prefix);
        }

        if let Some(pinned) = self
            .routing_prefixes
            .lock()
            .map_err(Into::<Error>::into)?
            .get(topic)
            .cloned()
        {
            return Ok(pinned);
        }

        let pinned = match self.read_routing_pin(topic).await? {
            Some(pinned) => pinned,

            // Pre-#236 topic: derive as before, then pin it so this is the last
            // time anyone derives it.
            None => {
                let derived = self.routed_prefix(topition, self.topic_is_compacted(topic).await?);

                if self
                    .put_create(
                        &self.topic_routing_path(topic),
                        serde_json::to_vec(&TopicRouting {
                            prefix: derived.clone(),
                        })
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                    )
                    .await?
                {
                    derived
                } else {
                    // A peer pinned it first: adopt its value, whatever we derived.
                    self.read_routing_pin(topic).await?.unwrap_or(derived)
                }
            }
        };

        _ = self
            .routing_prefixes
            .lock()
            .map(|mut locked| locked.insert(topic.to_owned(), pinned.clone()));

        Ok(pinned)
    }

    /// The pinned prefix for `topic`, or `None` when the object does not exist (a
    /// topic created before #236). One GET, uncached — the caller memoizes.
    async fn read_routing_pin(&self, topic: &str) -> Result<Option<String>> {
        match self.object_store.get(&self.topic_routing_path(topic)).await {
            Ok(get_result) => get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(|encoded| {
                    serde_json::from_slice::<TopicRouting>(&encoded).map_err(Into::into)
                })
                .map(|routing| Some(routing.prefix)),

            Err(object_store::Error::NotFound { .. }) => Ok(None),

            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// Drop a cached pinned prefix (on delete), so a topic re-created under the
    /// same name cannot inherit a dead incarnation's routing.
    fn invalidate_routing_prefix(&self, topic: &str) {
        if let Ok(mut cache) = self.routing_prefixes.lock() {
            _ = cache.remove(topic);
        }
    }

    /// Drop every remaining process-local cache entry keyed by `topic` or by one
    /// of its topitions (#283).
    ///
    /// Called for a topic whose metadata object is gone — by `delete_topic` for
    /// the one it just deleted, and by [`Self::evict_deleted_topic_caches`] for one
    /// a peer replica deleted.
    ///
    /// `delete_topic` used to invalidate three caches — the id pointer, the
    /// routing pin and the topic index — and leave six behind. Those six kept
    /// their entries until a same-named `create_topic` cleared them or the
    /// process restarted, so under create/delete churn with **fresh** names
    /// nothing ever cleared them: the growth was monotonic for the life of the
    /// pod. `topic_metas` and `watermarks` each hold a cached JSON value per
    /// entry, which makes it real memory rather than a few bytes of key.
    ///
    /// Every one of these is a cache or a hint whose authority is in the object
    /// store, so dropping an entry can only cost a re-read:
    ///
    /// - `topic_metas` — the per-topic `OptiCon`. Dropping it is also what makes a
    ///   same-named successor behave like a topic this replica has never read,
    ///   which is the state #28 needs for a fresh create to be immediately visible
    ///   (a retained handle holds a cached etag that can short-circuit the
    ///   conditional GET to a stale `NotModified`).
    /// - `next_offsets` — a hint, reconciled against the segment listing on a
    ///   cold read or a create conflict. Reached **only** because the topic is
    ///   gone: this is offset-authority state for a live topic, so a size- or
    ///   age-triggered eviction here would be a correctness bug, not a cache miss.
    /// - `coalesced_watermark_floors` — certified by [`Self::certified_seq_floor`],
    ///   which is unrelated to topic lifecycle, so nothing else would ever
    ///   invalidate a floor cached for the deleted incarnation.
    /// - `truncate_floors` / `watermarks` — re-read from the `watermark.json` that
    ///   `delete_topic` rewrites as the truncation tombstone (#246). The floor that
    ///   hides the topic's slices inside shared segments lives in that object, not
    ///   in these maps, so dropping the memo cannot resurrect anything: the next
    ///   reader re-reads the same floor.
    /// - `compacted_topics` — a TTL'd memo of `cleanup.policy`.
    ///
    /// Prefix-keyed state is deliberately untouched. A prefix is shared between
    /// topics (`a.b.c` and `a.b.c.d` route to the same one), and neither caller can
    /// tell whether the deleted topic was its last member without a scan — so
    /// evicting `segment_seqs` or a flush lock here could put a second sequence
    /// authority on a prefix a sibling topic is still producing to. A compacted
    /// topic's prefix *is* per-topic (#175), so that state does still grow with
    /// compacted-topic churn; establishing exclusivity cheaply is a separate
    /// change.
    fn invalidate_topic_caches(&self, topic: &str) {
        if let Ok(mut cache) = self.topic_metas.lock() {
            _ = cache.remove(topic);
        }

        if let Ok(mut cache) = self.compacted_topics.lock() {
            _ = cache.remove(topic);
        }

        if let Ok(mut cache) = self.watermarks.lock() {
            cache.retain(|topition, _| topition.topic() != topic);
        }

        if let Ok(mut cache) = self.next_offsets.lock() {
            cache.retain(|topition, _| topition.topic() != topic);
        }

        if let Ok(mut cache) = self.coalesced_watermark_floors.lock() {
            cache.retain(|topition, _| topition.topic() != topic);
        }

        if let Ok(mut cache) = self.truncate_floors.lock() {
            cache.retain(|topition, _| topition.topic() != topic);
        }
    }

    /// Drop the per-topic caches of every topic that no longer exists,
    /// returning how many topics were evicted (#283).
    ///
    /// [`Self::invalidate_topic_caches`] fixes only the replica that served the
    /// `DeleteTopics`. Eviction is process-local and a stateless fleet puts every
    /// topic through every replica, so on a ten-pod deployment nine pods keep
    /// their entries for a deleted topic — the growth is still monotonic, just at
    /// nine tenths of the rate. This is the half that converges the peers.
    ///
    /// Reconciled against the topic index rather than against a clock, because
    /// "this topic is gone" is a fact about the bucket and an idle window is only
    /// a guess at one. The index is exactly the right authority: it is rebuilt
    /// from a single LIST of `topic-metadata/`, drops deleted topics by
    /// construction, and is already maintained for the list-all metadata path —
    /// so the sweep costs one listing per maintenance tick and no per-topic
    /// requests.
    ///
    /// A refresh that **fails** propagates rather than evicting: a listing that
    /// did not happen says nothing about what exists, and treating it as "no
    /// topics" would drop the whole fleet's caches at once. An empty listing that
    /// succeeded is a cluster with no topics, and evicting is then correct.
    ///
    /// This is the one trigger that touches [`Self::next_offsets`] without a local
    /// delete, so it is worth being explicit that it is not the size-triggered
    /// eviction that map must never have: the criterion is the topic's *absence
    /// from the bucket*, never memory pressure or age, so a live topic cannot be
    /// selected however hot or cold its partitions are. The only way to reach a
    /// live topic here is a listing that omits an object that exists, which is not
    /// a state either object store produces.
    async fn evict_deleted_topic_caches(&self) -> Result<usize> {
        // Force the listing: a snapshot up to `TOPIC_INDEX_TTL` old is fine for
        // answering Metadata and is not fine for deciding what to forget.
        self.invalidate_topic_index();

        let live = self
            .topics_index()
            .await?
            .iter()
            .map(|metadata| metadata.topic.name.clone())
            .collect::<BTreeSet<_>>();

        // Whose entries to drop, decided once across every map, so the six
        // cannot end up disagreeing about which topics are gone.
        let mut evicted = BTreeSet::new();

        evicted.extend(dead_keys(&self.topic_metas, |topic| topic, &live));
        evicted.extend(dead_keys(&self.compacted_topics, |topic| topic, &live));
        evicted.extend(dead_keys(&self.watermarks, Topition::topic, &live));
        evicted.extend(dead_keys(&self.next_offsets, Topition::topic, &live));
        evicted.extend(dead_keys(
            &self.coalesced_watermark_floors,
            Topition::topic,
            &live,
        ));
        evicted.extend(dead_keys(&self.truncate_floors, Topition::topic, &live));

        for topic in &evicted {
            self.invalidate_topic_caches(topic);
        }

        // Read from the maps after the eviction rather than derived from `live`:
        // these must report what is actually held, so a map populated by a path
        // this sweep does not reach shows up as divergence from the cluster's topic
        // count instead of being papered over. Both are a `len()`, so the gauges
        // cost nothing even at 14.7k topics.
        let topics = self.topic_metas.lock().map_or(0, |cache| cache.len());
        let partitions = self.watermarks.lock().map_or(0, |cache| cache.len());

        TOPIC_CACHE_TOPICS.record(topics as u64, &[]);
        TOPIC_CACHE_PARTITIONS.record(partitions as u64, &[]);

        if !evicted.is_empty() {
            debug!(
                evicted = evicted.len(),
                live = live.len(),
                topics,
                partitions,
                cluster = self.cluster,
                "evicted per-topic caches of deleted topics"
            );
        }

        Ok(evicted.len())
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

    /// The segment sequence a listed object's name encodes, or `None` for a name
    /// that is not one.
    ///
    /// Shared by the incremental refresh and the reconciling pass (#408) so the
    /// two cannot disagree about which listed objects are segments — a
    /// reconciler that parsed one name differently from the refresher would drop
    /// live entries.
    fn segment_seq_of(location: &Path) -> Option<u64> {
        let name = location.parts().next_back()?;
        let name = name.as_ref();

        if name.len() < 20 {
            return None;
        }

        u64::from_str(&name[0..20]).ok()
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
    /// write-ahead of *every* segment delete (segments are deleted only through
    /// [`Self::retire_segments`], which raises this floor first), and
    /// `expire_prefix_segments` persists each
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

    /// Forget any certified seq floor cached for `prefix`, so the next
    /// [`Self::certified_seq_floor`] re-reads it.
    ///
    /// Called by [`Self::raise_seq_floor`] the moment the persisted floor moves.
    fn invalidate_certified_seq_floor(&self, prefix: &str) -> Result<()> {
        self.prefix_index
            .lock()
            .map_err(Into::into)
            .map(|mut index| {
                if let Some(entry) = index.get_mut(prefix) {
                    entry.seq_floor = None;
                }
            })
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
    /// sequence name is never reused — [`Self::retire_segments`] is the one
    /// delete path and does exactly that. Max-fold CAS: a lost race means another
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
                Ok(_) => {
                    // The persisted floor moved, so any floor this process has
                    // certified is now stale. Drop it here rather than relying on
                    // the caller's later `index_prune` to bump the generation.
                    //
                    // Every raise call site does prune afterwards, but *afterwards*
                    // is the problem: between this PUT and that bump the generation
                    // is unchanged, so a concurrent `certified_seq_floor` would be
                    // served the pre-raise value. That is harmless for the LATEST
                    // fast path, which only compares watermarks, and not harmless
                    // for `tail_next_seq_folded` (#278), which picks the next
                    // sequence *name* from it — serving a floor from before the
                    // raise is how a just-freed name gets reused, the one thing #77
                    // forbids. Invalidating at the source closes the window without
                    // making anything depend on raise-then-prune ordering.
                    self.invalidate_certified_seq_floor(prefix)?;
                    return Ok(());
                }
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
    /// Takes the floor through [`Self::certified_seq_floor`] rather than a second
    /// live GET, which is what made the steady-state flush read
    /// `seq-floor.json` twice milliseconds apart (#278).
    ///
    /// **Why one read still establishes #77.** The invariant is that a sequence
    /// name freed by retention or compaction is never reused, and it needs a
    /// floor observed *after* the tail. That ordering is preserved, in both
    /// cases and for different reasons:
    ///
    /// - **Probe resolved the tail.** There is exactly one path returning
    ///   [`TailProbe::Resolved`], and it is inside the branch that observed the
    ///   tail *absent* — which reads the floor live via
    ///   [`Self::probe_seq_floor`] and certifies it under the current
    ///   generation. So a cache hit here can only be that read: fresh, and
    ///   ordered after the absence it followed. Never an older caller's value,
    ///   because the probe overwrites it on the way out.
    /// - **Probe was inconclusive.** The forced refresh falls through to the
    ///   authoritative LIST, which bumps the generation. The cached floor is
    ///   then certified against a superseded view, so
    ///   [`Self::certified_seq_floor`] declines it and re-reads — the fallback,
    ///   taken because the generation moved rather than because anything assumed
    ///   it had not.
    ///
    /// A prune by this process also bumps the generation, and a floor raise by
    /// another replica is exactly what the live read on the inconclusive path
    /// catches. So the removed GET was redundant, not load-bearing: it could
    /// only ever re-read what the probe had just read under the same generation.
    async fn tail_next_seq_folded(&self, prefix: &str) -> Result<u64> {
        let listed_max = self
            .prefix_index
            .lock()
            .map_err(Into::<Error>::into)?
            .get(prefix)
            .and_then(PrefixIndex::resolved_max);
        let floor = self.certified_seq_floor(prefix).await?;
        Ok(listed_max.map_or(0, |m| m + 1).max(floor))
    }

    /// Resolve a create-only segment PUT at `candidate`, disambiguating an
    /// *ambiguous* result through the per-segment footer nonce (#89).
    ///
    /// `AlreadyExists` is unambiguous: a peer won the sequence. Any other error
    /// is ambiguous — the create may have landed durably before the response was
    /// lost — so the footer at `candidate` is probed and the object adopted iff
    /// it carries our nonce. Our nonce can only exist at a sequence our own PUT
    /// won, so a match is proof the create succeeded; blind-retrying at the next
    /// sequence would double-write the payload. A *peer's* footer means we lost
    /// the sequence exactly as in the `AlreadyExists` case and the transport
    /// error was moot. No footer at all means the create genuinely did not land
    /// (the store is read-after-write consistent, so a durable create would be
    /// visible) and a probe that itself errors leaves it unknown — both surface
    /// the storage error for a retry, which log-based dedup (#88) makes safe.
    ///
    /// One definition for the leaseless flush and for compaction (#286).
    /// Compaction used to treat every ambiguous PUT as a plain error, so a
    /// merged segment that had actually landed was retried as a failure and its
    /// whole payload re-uploaded — the #130 write amplification.
    async fn resolve_segment_create(
        &self,
        prefix: &str,
        candidate: u64,
        nonce: u64,
        result: Result<PutResult, object_store::Error>,
    ) -> SegmentCreate {
        let error = match result {
            Ok(outcome) => {
                debug!(?outcome, prefix, candidate);
                return SegmentCreate::Won;
            }

            Err(object_store::Error::AlreadyExists { .. }) => {
                debug!(prefix, candidate, "segment seq taken, re-deriving");
                return SegmentCreate::Lost { ambiguous: false };
            }

            Err(error) => error,
        };

        match self
            .read_segment_footer(&self.segment_location(prefix, candidate))
            .await
        {
            Ok(Some(found)) if found.nonce == nonce => {
                debug!(prefix, candidate, "ambiguous PUT adopted via nonce");
                SegmentCreate::Won
            }

            Ok(Some(_)) => {
                debug!(prefix, candidate, ?error, "ambiguous PUT lost to peer");
                SegmentCreate::Lost { ambiguous: true }
            }

            Ok(None) => {
                debug!(
                    prefix,
                    candidate,
                    ?error,
                    "ambiguous PUT did not land, failing retriably"
                );
                SegmentCreate::Failed(error.into())
            }

            Err(probe_error) => {
                // A 404 here is the probe's answer, not a fault: the create did
                // not land, which is exactly what this read asks (#408). Counted
                // as its own caller so it is not mistaken for a stale index
                // entry — it scales with ambiguous PUTs, and no reconciliation
                // reduces it.
                if let Error::ObjectStore(ref inner) = probe_error
                    && matches!(**inner, object_store::Error::NotFound { .. })
                {
                    SEGMENT_ABSENT.add(1, &[KeyValue::new("caller", "create_probe")]);
                }

                debug!(
                    prefix,
                    candidate,
                    ?error,
                    ?probe_error,
                    "ambiguous PUT unresolved"
                );
                SegmentCreate::Failed(error.into())
            }
        }
    }

    /// Write `payload` — encoded with `nonce` in its footer — as the next
    /// create-only segment under `prefix`, and return its assigned sequence
    /// (#57). The create is the authority: on a lost sequence, fold and retry
    /// the next free one. There is no lease to re-validate since #177 — the
    /// leaseless arbiter (#86) makes the create-only CAS itself the arbiter, so
    /// a loser is never a fenced writer, only a slower one.
    ///
    /// The conflict protocol is the leaseless flush's, and deliberately so
    /// (#286): [`Self::resolve_segment_create`] for the ambiguous PUT, a
    /// jittered [`cas_conflict_backoff`] so N contenders do not resync in
    /// lockstep, and fold-before-claim off the in-memory index (#91) instead of
    /// a fresh `tail_next_seq` LIST per attempt.
    async fn assign_and_create_segment(
        &self,
        prefix: &str,
        payload: PutPayload,
        nonce: u64,
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

        // #77's invariant is that a sequence name freed by retention or
        // compaction is never reused, and it needs a floor observed *after* the
        // tail. The hint cannot supply one. `set_seq` only ever rises within
        // *this* process, and nothing else touches `segment_seqs` — so a peer's
        // `retire_segments`, which raises the durable floor write-ahead of the
        // delete and frees every name below it, is invisible here. A create at
        // such a name then **succeeds**: the create-only CAS proves the name is
        // unoccupied, which is not the same as fresh. Every replica still
        // caching the retired segment's footer under that name then serves it
        // against the reborn object — which is #432, and #77's own comment
        // predicted it verbatim.
        //
        // Read the floor live rather than through `certified_seq_floor`. That
        // cache is keyed on the index generation, and `index_insert` — the
        // writer fast path every create takes — does not bump it, so a peer's
        // raise can stay uncertified for as long as this process neither lists
        // nor prunes. A floor that is merely *usually* fresh does not establish
        // an invariant whose failure is a wrong offset.
        //
        // One GET per compaction create is what that costs, and this is not the
        // produce path: the leaseless flush derives every candidate from
        // `tail_next_seq_folded`, which has folded the floor all along. #116's
        // saving lives there and is untouched.
        let mut candidate = match self.cached_seq(prefix)? {
            Some(seq) => seq.max(self.read_seq_floor(prefix).await?),

            // Already folded: `tail_next_seq` reads the floor live, after its
            // listing.
            None => self.tail_next_seq(prefix).await?,
        };

        for attempt in 0..MAX_ATTEMPTS {
            let put_result = self
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
                .await;

            match self
                .resolve_segment_create(prefix, candidate, nonce, put_result)
                .await
            {
                SegmentCreate::Won => {
                    SEGMENT_CREATES.add(1, &attributes);
                    self.set_seq(prefix, candidate + 1)?;
                    return Ok(candidate);
                }

                SegmentCreate::Lost { .. } => {
                    // Every loss costs a re-upload of the whole payload into the
                    // same key prefix (#130).
                    conflicts += 1;
                    SEGMENT_CREATE_CONFLICTS.add(1, &attributes);
                    SEGMENT_CREATE_BYTES_REWRITTEN.add(payload_len, &attributes);

                    debug!(candidate, attempt, prefix, "segment seq taken, resyncing");

                    sleep(cas_conflict_backoff(attempt)).await;

                    // Fold-before-claim off the index the forced refresh just
                    // listed, rather than a second full LIST (#91).
                    self.refresh_prefix_index_forced(prefix).await?;
                    candidate = self.tail_next_seq_folded(prefix).await?;
                }

                SegmentCreate::Failed(error) => return Err(error),
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
            return Self::decode_segment_footer(&buffer);
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

        Self::decode_segment_footer(&tail)
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

    /// Reconcile `prefix`'s cached index *downwards* against a listing, dropping
    /// entries whose objects are gone (#408). Answers how many it dropped.
    ///
    /// Nothing else can do this. `refresh_prefix_index_inner` lists
    /// `start_after` the highest known sequence and the tail probe only ever
    /// folds forward, so an entry for a segment a *peer* retired survives until
    /// this replica happens to read a 404 for it — and the read path prunes one
    /// per fetch, then restarts, bounded by `MAX_ATTEMPTS`. On the fleet that is
    /// 39 `segment` 404s/s on the brokers, each a billed GET and a restarted
    /// fetch, against 4.94 M indexed segments across ten of them.
    ///
    /// **Called from evidence, not from a timer.** A 404 is proof this prefix's
    /// index is stale, and only then is a listing worth its tier-1 price; a
    /// timer would pay it for every prefix whether or not anything had been
    /// retired. Then rate-limited per prefix
    /// ([`Self::PREFIX_INDEX_RECONCILE_INTERVAL`]), because one fetch storm
    /// against a stale prefix would otherwise buy one listing per fetch.
    ///
    /// **Only entries below the listing's own maximum are dropped.** A segment
    /// created while the listing was being walked may not appear in it, and
    /// pruning that would take records out of this replica's view of the tail —
    /// where the add-only refresh would never put them back. Below the maximum
    /// the listing is authoritative: sequences are never reused (#77), so an
    /// absent name is a retired one.
    ///
    /// The generation is bumped exactly as [`Self::index_prune`] does: dropping
    /// entries removes tail knowledge, so the certified seq floor must be
    /// re-read before the LATEST fast path may trust the index again.
    async fn reconcile_prefix_index(&self, prefix: &str) -> Result<u64> {
        // Due, and worth listing at all: an index this process holds nothing for
        // has nothing to drop.
        {
            let index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            let Some(entry) = index.get(prefix) else {
                return Ok(0);
            };

            if entry.segments.is_empty() {
                return Ok(0);
            }

            if entry.reconciled_at.is_some_and(|at| {
                SystemTime::now()
                    .duration_since(at)
                    .is_ok_and(|elapsed| elapsed < Self::PREFIX_INDEX_RECONCILE_INTERVAL)
            }) {
                return Ok(0);
            }
        }

        let listing = self.segment_prefix(prefix);
        let mut stream = self.scan(Scan::SegmentIndex, &listing);
        let mut live: BTreeSet<u64> = BTreeSet::new();

        while let Some(meta) = stream
            .next()
            .await
            .transpose()
            .inspect_err(|err| debug!(?err, prefix, "reconciling the prefix index"))?
        {
            if let Some(seq) = Self::segment_seq_of(&meta.location) {
                _ = live.insert(seq);
            }
        }

        let Some(listed_max) = live.iter().next_back().copied() else {
            // An empty listing is not evidence: it is also what a prefix whose
            // objects are all above a raced page boundary looks like. Stamp the
            // clock so a fetch storm does not re-list, and drop nothing.
            let mut index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            if let Some(entry) = index.get_mut(prefix) {
                entry.reconciled_at = Some(SystemTime::now());
            }

            return Ok(0);
        };

        let dropped = {
            let mut index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            let Some(entry) = index.get_mut(prefix) else {
                return Ok(0);
            };

            let before = entry.segments.len();
            entry
                .segments
                .retain(|seq, _| *seq >= listed_max || live.contains(seq));
            let dropped = before.saturating_sub(entry.segments.len()) as u64;

            if dropped > 0 {
                entry.generation += 1;
            }
            entry.reconciled_at = Some(SystemTime::now());

            dropped
        };

        if dropped > 0 {
            PREFIX_INDEX_RECONCILED.add(dropped, &[KeyValue::new("prefix", prefix.to_string())]);
            debug!(prefix, dropped, listed_max, "reconciled the prefix index");
        }

        Ok(dropped)
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

    /// Fold exactly one segment's footer into the prefix index, answering
    /// whether it landed (#401).
    ///
    /// This is the fold for a writer that already has *proof* the segment
    /// exists — a create that came back `AlreadyExists` — and so needs neither
    /// the tail probe's absence chain nor the always-fresh seq-floor read that
    /// makes the absence a proof ([`Self::probe_prefix_tail`]). One ranged
    /// footer GET, and `folded_max` advances by one.
    ///
    /// `false` means the caller should fall back to the full refresh: an
    /// unreadable or undecodable footer at a sequence a create says is occupied
    /// is exactly the `stalled` case the loop's own diagnostics exist for, and
    /// the LIST path is where that is resolved. So this can only be faster than
    /// a refresh, never a substitute for one that answers differently.
    ///
    /// Only `refreshed_at` is stamped, as the probe does: folding can add a
    /// segment but never reflect a peer's deletion, so the index generation —
    /// and with it the certified seq floor keeping the LATEST path off
    /// `watermark.json` — stays valid.
    async fn fold_segment_footer(&self, prefix: &str, seq: u64) -> bool {
        let location = self.segment_location(prefix, seq);

        let result = match self
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
            Ok(result) => result,
            Err(error) => {
                if matches!(error, object_store::Error::NotFound { .. }) {
                    SEGMENT_ABSENT.add(1, &[KeyValue::new("caller", "fold")]);
                }

                debug!(?error, prefix, seq, "folding the winner's footer");
                return false;
            }
        };

        let last_modified_ms = result.meta.last_modified.timestamp_millis();

        let Ok(bytes) = result.bytes().await.inspect_err(|error| {
            debug!(?error, prefix, seq, "folding the winner's footer body");
        }) else {
            return false;
        };

        let Ok(Some(footer)) = Self::decode_segment_footer(&bytes).inspect_err(|error| {
            debug!(?error, prefix, seq, "decoding the winner's footer");
        }) else {
            return false;
        };

        let Ok(mut index) = self.prefix_index.lock() else {
            return false;
        };

        let entry = index.entry(prefix.to_owned()).or_default();
        _ = entry.segments.insert(
            seq,
            CachedSegment {
                footer,
                last_modified_ms,
            },
        );
        entry.refreshed_at = Some(SystemTime::now());

        true
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
                    // Absence is what this read is *for* (#408): the probe asks
                    // whether `cursor + 1` exists and a 404 is the affirmative
                    // answer. Counted so the `segment` 404 plane can be split
                    // from the stale-index population it was conflated with.
                    SEGMENT_ABSENT.add(1, &[KeyValue::new("caller", "tail_probe")]);

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
            match Self::decode_segment_footer(&bytes) {
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
            let Some(seq) = Self::segment_seq_of(&meta.location) else {
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
                        SEGMENT_ABSENT.add(1, &[KeyValue::new("caller", "refresh")]);
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

            // Size of the cached footers, process-wide (#196). The broker's
            // working set grows ~600 MiB fresh to ~1.4 GiB over 26h on an
            // unchanged workload, decelerating like a cache warming up rather
            // than leaking, and differing by ~450 MiB between pods of identical
            // age — a shape that points at something keyed by what each pod
            // served. This index is the only per-process structure large enough
            // to account for it: tens of prefixes, but each holding a whole
            // `SegmentFooter` per live segment, one `SubstreamEntry` per
            // `(topic, partition)` in that segment.
            //
            // Recorded here rather than on every mutation: this is the TTL-gated
            // path, so the O(segments) walk runs at most once per prefix per
            // `HIGH_WATERMARK_HINT_TTL`, and it is the point where the live set
            // has just been reconciled.
            let (segments, entries) = index.values().fold((0u64, 0u64), |(s, e), entry| {
                (
                    s + entry.segments.len() as u64,
                    e + entry
                        .segments
                        .values()
                        .map(|cached| cached.footer.entries.len() as u64)
                        .sum::<u64>(),
                )
            });

            PREFIX_INDEX_SEGMENTS.record(segments, &[]);
            PREFIX_INDEX_SUBSTREAM_ENTRIES.record(entries, &[]);
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

    /// GET each of `seqs` whole, or `None` when any of them had already been
    /// deleted.
    ///
    /// A 404 is a ghost index entry, not a failure (#274 — the compaction half
    /// of the race #191 fixed on the refresh path). The incremental refresh is
    /// add-only, so a replica that indexed this prefix before a peer compacted
    /// it holds entries below its own cursor permanently. Treating that as fatal
    /// aborted the pass *after* the claim had stamped `maintained_at_ms`, so
    /// peers skipped the prefix for the whole recency window and its segment
    /// count grew unbounded with nothing reporting it. Instead the vanished
    /// sequences are pruned — names are never reused (#77), so dropping one is
    /// permanent and correct — the prefix is invalidated so the next tick
    /// re-lists, and `None` tells the caller to yield this tick.
    ///
    /// Both compaction passes read their inputs this way (#286): whole objects
    /// beat per-sub-stream ranged GETs once a prefix holds more than one
    /// sub-stream, and every byte is about to be rewritten anyway.
    async fn fetch_segment_objects(
        &self,
        prefix: &str,
        seqs: impl IntoIterator<Item = u64>,
    ) -> Result<Option<BTreeMap<u64, Bytes>>> {
        let mut objects: BTreeMap<u64, Bytes> = BTreeMap::new();
        let mut vanished: Vec<u64> = Vec::new();

        for seq in seqs {
            match self
                .object_store
                .get(&self.segment_location(prefix, seq))
                .await
            {
                Ok(result) => {
                    _ = objects.insert(seq, result.bytes().await.map_err(Error::from)?);
                }

                Err(object_store::Error::NotFound { .. }) => {
                    SEGMENT_VANISHED_BEFORE_READ.add(1, &[]);
                    SEGMENT_ABSENT.add(1, &[KeyValue::new("caller", "compaction")]);
                    debug!(
                        prefix,
                        seq, "segment deleted before compaction could read it; pruning"
                    );
                    vanished.push(seq);
                }

                Err(err) => {
                    error!(?err, prefix, seq);
                    return Err(Error::from(err));
                }
            }
        }

        if !vanished.is_empty() {
            self.index_prune(prefix, &vanished)?;
            self.index_invalidate(prefix)?;
            return Ok(None);
        }

        Ok(Some(objects))
    }

    /// Decode a sub-stream's epoch-fenced regions out of already-fetched segment
    /// objects, as `(seq, base_offset, batches)` ascending by sequence.
    ///
    /// The fenced view ([`Self::valid_substream_segments`]) is what both
    /// compaction passes transform (#286): overlap-resolved, higher
    /// epoch/sequence wins, so a zombie region is dropped here and never fused
    /// into a rewritten segment. The view spans the whole prefix, so it is
    /// narrowed to the segments the caller actually fetched — the size merge
    /// fetches its run, per-key rewrite fetches everything.
    ///
    /// A region whose recorded extent runs past its object **fails the run**
    /// (#397). It used to be skipped, on the reasoning that carrying its base
    /// forward without its records would mislabel the batches that follow it —
    /// but skipping mislabels them just as badly, and durably. The merged region
    /// is written with the base offset of its first region and read back by
    /// running offsets from there, so a dropped region in the middle of a run
    /// slides everything above it down into the gap, and `retire_segments` then
    /// deletes the original the records could still have been read from.
    ///
    /// Failing the run costs a merge; #398's quarantine then excludes the object
    /// and the drain proceeds over what is readable. Silently rewriting the
    /// prefix with shifted offsets costs the data.
    fn decode_fenced_regions(
        &self,
        prefix: &str,
        topic: &str,
        partition: i32,
        objects: &BTreeMap<u64, Bytes>,
    ) -> Result<Vec<(u64, i64, Vec<deflated::Batch>)>> {
        let fenced = self.valid_substream_segments(prefix, topic, partition)?;
        let mut regions = Vec::with_capacity(fenced.len());

        for (seq, entry) in fenced {
            let Some(object) = objects.get(&seq) else {
                continue;
            };

            let start = entry.byte_start as usize;
            let end = start + entry.byte_len as usize;
            if end > object.len() {
                let available = object.slice(start.min(object.len())..);

                return Err(RegionRead {
                    prefix,
                    seq,
                    entry: &entry,
                    encoded: &available,
                }
                .short_of_extent(format!(
                    "region extent runs {} bytes past a {}-byte object",
                    end - object.len(),
                    object.len(),
                )));
            }

            regions.push((
                seq,
                entry.base_offset,
                self.decode_region(prefix, seq, &entry, object.slice(start..end))?,
            ));
        }

        Ok(regions)
    }

    /// Retire `seqs` from `prefix`: raise the durable sequence floor past the
    /// highest of them, delete their objects, then prune them from the index.
    /// Returns the number of objects deleted.
    ///
    /// The floor write is **write-ahead of the delete** (#77): a freed sequence
    /// name must never be reused, or a peer caching the old footer would serve
    /// stale byte ranges against a reborn object. Deleting can lower the listing
    /// max, so without the persisted floor a later `tail_next_seq` would hand
    /// the freed name back out. On a floor-write error this returns before
    /// deleting anything — the caller retries on the next tick rather than break
    /// the invariant.
    ///
    /// This is the only place segment objects are deleted (#286): expiry,
    /// whole-segment compaction and per-key rewrite all retire through here, so
    /// a fourth delete path cannot get the ordering wrong by omission.
    async fn retire_segments(&self, prefix: &str, seqs: &[u64]) -> Result<u64> {
        /// Segment objects deleted per bulk request — matches the S3
        /// `DeleteObjects` per-request key cap.
        const RETIRE_DELETE_CHUNK: usize = 1_000;

        let Some(max_seq) = seqs.iter().copied().max() else {
            return Ok(0);
        };

        self.raise_seq_floor(prefix, max_seq + 1).await?;

        let mut deleted: u64 = 0;
        let mut chunk: Vec<Path> = Vec::new();

        for seq in seqs {
            chunk.push(self.segment_location(prefix, *seq));

            if chunk.len() >= RETIRE_DELETE_CHUNK {
                deleted += chunk.len() as u64;
                self.delete_batches(std::mem::take(&mut chunk)).await?;
            }
        }

        if !chunk.is_empty() {
            deleted += chunk.len() as u64;
            self.delete_batches(chunk).await?;
        }

        self.index_prune(prefix, seqs)?;

        Ok(deleted)
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
    /// **max** of the epoch-fenced segment tail and the persisted floor. It used
    /// to fold a legacy `records/` tail too (the #58 seam, the #62 bypass), since
    /// a `{offset}.batch` object could sit above the segments and reusing an offset
    /// is unacceptable — nothing can create one since #179.
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

        // `persisted_floor` is supplied by the caller (the persisted
        // `watermark.high`) so this shares the single GET the read path already
        // issued instead of re-fetching it (#72). No legacy tail is folded any
        // more (#179): a `records/` object could sit above the segment tail (the
        // #58 seam, the #62 bypass), which is why this fold existed — no writer
        // can create one, and no read path can serve one.
        Ok(segment_tail.max(persisted_floor))
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

        /// Ranged region GETs in flight for one sub-stream (#426), matching the
        /// footer warm's `FOOTER_FETCH_CONCURRENCY` immediately above it and every
        /// other fan-out in the engine (`list_offsets`, `offset_commit`,
        /// `offset_fetch`, `metadata_fetch`, `list_state`, `describe`).
        const SEGMENT_READ_CONCURRENCY: usize = 32;

        let prefix = self.routed_prefix_of(topition).await?;

        for _ in 0..MAX_ATTEMPTS {
            self.refresh_prefix_index(&prefix).await?;
            let segments =
                self.valid_substream_segments(&prefix, topition.topic(), topition.partition())?;

            // Plan the reads from the index before issuing any (#426).
            //
            // Every input to the decision is an index field: the offset skip and
            // the high-watermark stop come from `base_offset`/`record_count`, and
            // the byte budget consumes `entry.byte_len`, which is the cached
            // footer's claim and not the response's length. So the set of
            // segments this fetch will read is known before the first GET — which
            // is the whole reason the reads can be concurrent instead of a chain.
            //
            // This was the only fan-out in the engine that was not buffered. The
            // footer warm immediately above it is `buffered(32)` with a comment
            // explaining that a sequential footer-per-segment loop stalled
            // `list_offsets` past the client timeout; the data read below it had
            // the same shape and no buffering. Measured with injected per-GET
            // latency over 64 segments in one partition: 2 ms at 0 ms, 431 ms at
            // 5 ms, 1 417 ms at 20 ms — exactly linear, and that is the innermost
            // loop alone.
            let mut bytes = max_bytes as u64;
            let mut plan: Vec<(u64, SubstreamEntry)> = Vec::new();

            for (seq, entry) in segments {
                // Segments are sorted by base offset; skip those ending at/before
                // the requested offset, stop once one starts at/past the HWM.
                if entry.base_offset + entry.record_count <= offset {
                    continue;
                }
                if entry.base_offset >= high_watermark {
                    break;
                }

                // The budget admits the entry that crosses it and stops *after*,
                // which is what the serial loop did: it read the region, pushed
                // its batches, and only then compared. Preserved exactly —
                // returning one region fewer per round trip is a behaviour change
                // a client would see.
                let byte_len = entry.byte_len;
                plan.push((seq, entry));

                if byte_len > bytes {
                    break;
                }
                bytes = bytes.saturating_sub(byte_len);
            }

            // `&str` rather than the `String`, so each read's future captures a
            // `Copy` borrow instead of moving the prefix the attempt loop reuses.
            let prefix = prefix.as_str();

            // Eagerly collected to pin lifetimes, the same idiom `Metadata` and
            // `ListOffsets` use in this file (#147): the futures are inert until
            // `buffered` polls them, so this allocates, it does not serialise.
            let reads = plan
                .iter()
                .map(|(seq, entry)| {
                    let location = self.segment_location(prefix, *seq);

                    async move {
                        self.note_segment_data_read(prefix, *seq, entry.byte_start, entry.byte_len);

                        // One ranged GET of exactly this sub-stream's byte span.
                        match self
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
                            Ok(result) => {
                                let encoded = result.bytes().await.map_err(Error::from)?;

                                // Short of the extent the index claims (#397): the
                                // index entry and the object disagree, and only the
                                // object's own trailer says which is wrong. A
                                // correctable index is repaired and the fetch restarts
                                // off it — the whole entry, `base_offset` and
                                // `record_count` included, feeds the offset arithmetic
                                // below, so re-reading one region inline would mix a
                                // corrected extent with a stale span.
                                if (encoded.len() as u64) < entry.byte_len {
                                    self.resolve_short_region(
                                        prefix, *seq, entry, &location, &encoded,
                                    )
                                    .await?;

                                    return Ok(RegionOutcome::Corrected);
                                }

                                // A full-length region that holds no frame is the
                                // *same* index/object disagreement, and the branch
                                // above cannot see it: an entry that under-states a
                                // healthy frame is served in full, so
                                // `read_len == byte_len` (#432). Ask the object's own
                                // trailer here too, and restart off a corrected entry
                                // — otherwise a wrong entry is a `CORRUPT_MESSAGE`
                                // that the client retries at the same offset, forever.
                                //
                                // Damage that survives the trailer still answers this
                                // partition `CORRUPT_MESSAGE` (#386) rather than
                                // propagating a bare integer-conversion failure that
                                // took the request, and the connection, with it.
                                match self.decode_region(prefix, *seq, entry, encoded) {
                                    Ok(decoded) => Ok(RegionOutcome::Decoded(decoded)),

                                    Err(Error::CorruptSegment(corrupt)) => {
                                        self.resolve_corrupt_region(
                                            prefix, *seq, entry, &location, corrupt,
                                        )
                                        .await?;

                                        Ok(RegionOutcome::Corrected)
                                    }

                                    Err(otherwise) => Err(otherwise),
                                }
                            }

                            // Deleted between locate and read (compaction #66 /
                            // retention #61). Reported rather than handled here: the
                            // prune, the reconciling listing and the restart are the
                            // caller's, so they happen once for the attempt instead of
                            // once per concurrent read that met the same stale prefix.
                            Err(object_store::Error::NotFound { .. }) => {
                                Ok(RegionOutcome::Vanished)
                            }

                            Err(error) => {
                                error!(?error, location = %location);
                                // Preserve the storage error so it is classified
                                // retriable rather than fatal `-1` (#6/#129).
                                Err(Error::from(error))
                            }
                        }
                    }
                })
                .collect::<Vec<_>>();

            let mut reads = futures::stream::iter(reads).buffered(SEGMENT_READ_CONCURRENCY);

            let mut batches = vec![];
            let mut restart = false;

            // Consumed in input order, so the assembled batches are the serial
            // loop's. Stopping early drops the futures still in flight, which
            // cancels their reads — the same thing the serial `break` did, one
            // round trip earlier.
            for (seq, entry) in plan.iter() {
                // Checked before consuming, which is where the serial loop
                // checked it: before the work for this segment, not after. Tested
                // after the await instead, a single GET that spends the budget
                // would discard its own result and the fetch would answer empty.
                if has_deadline_expired() {
                    break;
                }

                let Some(outcome) = reads.next().await else {
                    break;
                };

                match outcome? {
                    RegionOutcome::Decoded(region) => {
                        let mut running = entry.base_offset;
                        for mut batch in region {
                            let span = batch.last_offset_delta as i64 + 1;
                            batch.base_offset = running;
                            running += span;
                            batches.push(batch);
                        }
                    }

                    // Drop anything gathered so far, evict the stale seq
                    // (prune-on-404 — the add-only refresh never would), force a
                    // re-list to pick up the merged/surviving segments, and
                    // restart clean. The merged segment covers the same offsets
                    // and wins the overlap (higher seq), so no gap/duplicate.
                    RegionOutcome::Vanished => {
                        // Counted (#399): the same event the compaction path has
                        // always counted, and on the fleet the *bigger* half —
                        // 39 `segment` 404s/s on the brokers against 7 on the
                        // maintainers — while this side reported none of it,
                        // because the incremental index refresh is add-only and
                        // nothing here ever said how much of it was stale.
                        SEGMENT_VANISHED_BEFORE_READ.add(1, &[]);
                        SEGMENT_ABSENT.add(1, &[KeyValue::new("caller", "fetch")]);
                        self.index_prune(prefix, &[*seq])?;

                        // This 404 is proof the index is stale, and pruning the
                        // one sequence it named leaves every other stale entry in
                        // place — to be found by another 404, another billed GET
                        // and another restarted fetch (#408). A listing settles
                        // the whole prefix at once, so take it here where there is
                        // evidence for it, rate-limited per prefix.
                        //
                        // Not `?`: a listing that fails must not cost the fetch
                        // the restart it was already going to make. The next 404
                        // tries again.
                        _ = self
                            .reconcile_prefix_index(prefix)
                            .await
                            .inspect_err(|err| debug!(?err, prefix, seq));

                        self.index_invalidate(prefix)?;
                        restart = true;
                        break;
                    }

                    RegionOutcome::Corrected => {
                        restart = true;
                        break;
                    }
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
    /// start of the segment region. `None` when the
    /// sub-stream has no segment yet.
    async fn segment_region_start(&self, topition: &Topition) -> Result<Option<i64>> {
        let prefix = self.routed_prefix_of(topition).await?;
        self.refresh_prefix_index(&prefix).await?;
        Ok(self
            .valid_substream_segments(&prefix, topition.topic(), topition.partition())?
            .first()
            .map(|(_, entry)| entry.base_offset))
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

        // The oldest segment's base for this sub-stream, from the index (#179: the
        // legacy region that used to hold lower offsets can no longer exist).
        //
        // With no segment the log is empty and EARLIEST is the log end, not 0
        // (#290) — see [`Self::log_start`] for why the 0 was worth removing.
        // This is the site a client actually reads: `LOG-START-OFFSET` comes
        // from ListOffsets EARLIEST, so it is where the false start offset
        // became visible as unretireable lag.
        let start = match self.segment_region_start(topition).await? {
            Some(base) => base,
            // Paid only when there is no segment, so a healthy EARLIEST keeps
            // its request profile.
            None => self.high_watermark(topition).await?,
        };

        Ok(start.max(floor))
    }

    /// The newest record timestamp for a PURE-segment sub-stream (#73), from the
    /// footer index — the tail segment's `max_timestamp`. Returns `None` (caller
    /// falls back to the legacy `records/` listing) when the sub-stream has no
    /// segment yet, when the footer carries no timestamp, OR when the topic is
    /// unconditional since #179: a legacy `records/` object could sit ABOVE the
    /// segment tail (the #58 seam, the #62 bypass), which made the footer's
    /// `max_timestamp` not the log's latest timestamp — no writer can create one,
    /// and no read path can serve one.
    async fn coalesced_latest_timestamp(&self, topition: &Topition) -> Result<Option<SystemTime>> {
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
        // LATEST is the log end offset — except under read-committed with an open
        // transaction, where it is the **last stable offset**: the first offset of
        // the earliest open transaction, which is what `stable` carries.
        //
        // That case used to be answered by walking the legacy `records/` listing
        // for the highest object below the stable bound (#179 deleted that scan,
        // and its final fallback answered 0 for a partition with no objects). The
        // value needs no scan and no approximation: it is the same fold
        // `offset_stage` performs, `stable.get(topition).unwrap_or(high_watermark)`,
        // so the two paths cannot disagree about the LSO.
        //
        // The log end itself comes from `high_watermark` (footer-aware): the
        // previous `last_modified` ordering was wrong under inter-replica clock
        // skew, and `max_base + 1` ignored multi-record batches.
        if *offset_request == ListOffset::Latest {
            let (offset, timestamp) = match stable.get(topition).copied() {
                // An open transaction bounds LATEST. No timestamp: the bound is a
                // transaction boundary, not a record this path has identified.
                Some(last_stable) => (last_stable, None),

                // Tail timestamp from the footer index (the newest segment's max
                // record timestamp) — the segment's record-time, closer to the SQL
                // backends' record `timestamp` than an object mtime ever was.
                None => (
                    self.high_watermark(topition).await?,
                    self.coalesced_latest_timestamp(topition).await?,
                ),
            };

            return Ok(Some(ListOffsetResponse {
                error_code: ErrorCode::None,
                offset: Some(offset),
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
        // scan that used to follow, which for a pure-segment topic found nothing
        // and wrongly returned offset 0. This is an in-memory scan of the warm
        // index (no per-segment I/O); `None` (→ -1 on the wire) when no record is
        // at or after the target, matching Kafka's "no offset" semantics.
        if let ListOffset::Timestamp(target) = offset_request {
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

        // Nothing else to consult (#179). EARLIEST and LATEST returned above from
        // the footer index; a TIMESTAMP that resolved to no segment means no record
        // is at or after the target. The legacy `records/` scan that used to run
        // here — the last read-path listing of that layout, and the reason a
        // pure-segment topic answered offset 0 for a timestamp nobody had — is
        // gone with the layout it read.
        Ok(Some(ListOffsetResponse {
            error_code: ErrorCode::None,
            offset: None,
            ..Default::default()
        }))
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
                    sleep(linger).await;

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

        // Which of the two budget guards ended the loop, for the counter (#401).
        // `attempts` is the third: `MAX_ATTEMPTS` reached without either clock
        // guard firing, which no production sample has ever shown.
        let mut spent = "attempts";

        // Where the budget actually went (#192). `conflicts`/`ambiguous_lost`/
        // `stalled` say why the loop *retried*; without these, a flush starved by
        // one slow PUT and a flush losing races look identical in the log, which
        // is what made #192 read as contention when contention was near zero.
        let mut attempts_made = 0usize;
        let mut slowest_attempt = Duration::ZERO;

        // The sequence a peer's create was proved to hold, carried into the next
        // attempt so it can fold that footer instead of re-proving the tail
        // (#401). See the fold below.
        let mut won_by_peer: Option<u64> = None;
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
                if elapsed >= max_elapsed {
                    spent = "elapsed";
                    break;
                }
                if elapsed + slowest_attempt > max_elapsed {
                    spent = "projected";
                    break;
                }
            }

            let attempt_started = tokio::time::Instant::now();
            attempts_made += 1;

            // Fold-before-claim: observe every live segment so the candidate
            // sequence and the derived bases reflect all writers, not a stale
            // view.
            //
            // On a retry after a lost create the full refresh is more than the
            // situation needs (#401). The PUT already *proved* which sequence a
            // peer holds, so the only new information is that segment's footer:
            // folding it advances `folded_max` by one and the next candidate
            // follows, without the tail probe's absence chain or the
            // always-fresh seq-floor read that makes an absence a proof. One
            // ranged GET where the refresh costs three or four round trips —
            // and the fleet's exhaustion samples spend 88 % of a flush neither
            // in the PUT nor in the backoff, so the round trips are the budget.
            //
            // A fold that does not land falls through to the refresh, so the
            // worst case is today's cost, and the `stalled` diagnostics still
            // see a sequence this writer cannot resolve.
            let folded = match won_by_peer.take() {
                Some(seq) => self.fold_segment_footer(prefix, seq).await,
                None => false,
            };

            if !folded && let Err(error) = self.refresh_prefix_index_forced(prefix).await {
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

            // Resolve the PUT, including the ambiguous case, through the one
            // definition shared with compaction (#286) — see
            // [`Self::resolve_segment_create`].
            match self
                .resolve_segment_create(prefix, candidate, nonce, put_result)
                .await
            {
                // Won the sequence — this create is the linearization point.
                SegmentCreate::Won => {
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
                SegmentCreate::Lost { ambiguous } => {
                    if ambiguous {
                        ambiguous_lost += 1;
                    } else {
                        conflicts += 1;
                    }

                    // A peer holds `candidate` — `resolve_segment_create` has
                    // established that for both arms, the ambiguous one by
                    // reading a footer that was not ours. So the next attempt
                    // folds that one segment rather than re-proving the tail
                    // (#401).
                    won_by_peer = Some(candidate);
                    slowest_attempt = slowest_attempt.max(attempt_started.elapsed());
                    FLUSH_CAS_CONFLICTS.add(1, &[]);
                    // Yield briefly, jittered (#157): N replicas flushing this
                    // prefix would otherwise re-LIST and re-PUT in lockstep, both
                    // amplifying requests on the busiest prefix and letting one
                    // writer lose every attempt of its budget to the same peers.
                    let backoff = cas_conflict_backoff(attempt);
                    backoff_elapsed += backoff;
                    sleep(backoff).await;
                    continue;
                }

                // The create did not land and cannot be claimed: fail for a
                // client retry, which log-based dedup (#88) makes safe, instead
                // of spinning the attempt budget against a throttling bucket.
                SegmentCreate::Failed(error) => {
                    return Self::fail_prefix_flush(buffer, error, prefix);
                }
            }
        }

        FLUSH_CAS_EXHAUSTED.add(
            1,
            &[
                KeyValue::new("prefix", prefix.to_string()),
                KeyValue::new("spent", spent),
            ],
        );
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
            spent,
            "leaseless flush exhausted retries"
        );
        // Retriable: exhaustion here is pure create-CAS contention (a transport
        // error fails fast retriably above), so tell the client to back off and
        // retry rather than dropping the batch on a fatal code (#6/#129).
        //
        // There is deliberately no fallback to a leased write here (#401's
        // direction 2): the produce lease was removed in #177 and the create-CAS
        // *is* the offset arbiter, so there is no other path to take. What makes
        // this terminal rarer is fitting more attempts inside the same budget,
        // which is what the winner-fold above does.
        Self::fail_prefix_flush(buffer, Error::Api(ErrorCode::KafkaStorageError), prefix)
    }

    /// The next offset for `topition` under the leaseless path (#86), derived from
    /// the already force-folded prefix index: the epoch-fenced segment tail, this
    /// process's hint, and the persisted floor, all three folded with `max` so an
    /// offset is never reused.
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

        // Fold the persisted floor **unconditionally** (#287), matching
        // `recover_substream_next_offset` and `docs/design-multiwriter-segments.md`
        // step 2. It used to be folded only when `segment_tail.max(cached)` was
        // zero, on the reasoning that a non-zero tail already knows the log end.
        // It does not: `expire_prefix_segments` can reclaim a sub-stream's
        // *tail-holding* segment while a lower-offset one survives — a shared
        // segment kept alive by a hot sibling topic, or simply a batch whose
        // timestamp is older than its predecessor's. A replica that then rebuilds
        // its index from a listing sees a non-zero tail that under-reports the log
        // end, skipped the floor, and re-assigned acknowledged offsets. Silent
        // offset reuse: consumers see one offset carrying two different payloads.
        //
        // The floor is the only surviving record of those offsets, which is why
        // expiry writes it write-ahead of the delete.
        //
        // Cost: `persisted_high` goes through the cached `OptiCon<Watermark>`
        // handle, so this is a conditional GET that answers 304 while the
        // watermark is unchanged — which is almost always, since only expiry and
        // truncation move it. One revalidation round trip per flush, against a
        // flush that already does a LIST and at least one create-CAS PUT. It is
        // *not* the full GET per flush that the conditional appeared to be
        // avoiding, which is why no memo is needed here.
        //
        // A per-process memo was considered and rejected: it would be unsound in
        // exactly the case this fixes. The obvious memo keys off `cached_high`,
        // but that hint reflects only *this* replica's writes (see `set_high`),
        // so a warm replica whose peer produced the offsets that expiry then
        // reclaimed would hold a memo below the floor and reuse them anyway.
        //
        // The legacy tail is no longer folded (#179): it guarded against a
        // `records/` object sitting above the segment tail, which nothing can
        // create.
        let floor = self.persisted_high(topition).await.unwrap_or(0);

        Ok(segment_tail.max(cached).max(floor))
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

    /// The root of the consumer tree: every group's state object and every
    /// committed offset in the cluster lives under this prefix.
    fn groups_root(&self) -> Path {
        Path::from(format!("clusters/{}/groups/consumers/", self.cluster))
    }

    /// The prefix holding everything owned by `group_id`, or `None` when
    /// `group_id` contributes no path component of its own.
    ///
    /// [`Path`] drops empty components on normalisation, so an empty group id —
    /// or one made only of delimiters — does not narrow the prefix at all: it
    /// collapses onto [`Self::groups_root`]. Handing that to a `delete_stream`
    /// deletes every group and every committed offset in the cluster (#277), so
    /// this returns the widening case as `None` rather than a prefix that
    /// silently means "everything".
    ///
    /// The check is structural — built prefix against root — rather than
    /// `group_id.is_empty()`, because normalisation is what does the widening:
    /// `""`, `"/"` and `"///"` all produce the same root.
    fn group_prefix(&self, group_id: &str) -> Option<Path> {
        let prefix = Path::from(format!(
            "clusters/{}/groups/consumers/{}",
            self.cluster, group_id,
        ));

        (prefix != self.groups_root()).then_some(prefix)
    }

    /// The optimistic-concurrency handle on a group's `offsets.json` (#406), or
    /// `None` for the widening group id [`Self::group_prefix`] refuses (#277).
    ///
    /// Memoized per group so the conditional GET a commit pays is answered from
    /// the etag memo. It sits *beside* the `offsets/` prefix rather than under it,
    /// so every listing that walks that prefix — the topition-set discovery, and
    /// `delete_topic`'s per-topic sweep — is unchanged by its existence.
    fn group_offsets(&self, group_id: &str) -> Result<Option<OptiCon<GroupOffsets>>> {
        let Some(prefix) = self.group_prefix(group_id) else {
            return Ok(None);
        };

        Ok(Some(
            self.group_offsets
                .lock()
                .map_err(Into::<Error>::into)?
                .entry(group_id.to_owned())
                .or_insert_with(|| OptiCon::path(Path::from(format!("{prefix}/offsets.json"))))
                .clone(),
        ))
    }

    /// `members/` under a group's prefix, or `None` for the widening group id
    /// [`Self::group_prefix`] refuses (#277).
    fn group_members_prefix(&self, group_id: &str) -> Option<Path> {
        self.group_prefix(group_id)
            .map(|prefix| Path::from(format!("{prefix}/members")))
    }

    /// A member's own document (#359). `None` when either id contributes no
    /// path component: a member id that normalises away would otherwise write
    /// the group's `members` prefix itself as an object.
    fn group_member_location(&self, group_id: &str, member_id: &str) -> Option<Path> {
        let prefix = self.group_members_prefix(group_id)?;
        let location = Path::from(format!("{prefix}/{member_id}.json"));

        (location != Path::from(format!("{prefix}/.json"))).then_some(location)
    }

    /// A group's composition document (#359).
    fn group_generation_location(&self, group_id: &str) -> Option<Path> {
        self.group_prefix(group_id)
            .map(|prefix| Path::from(format!("{prefix}/generation.json")))
    }

    /// `assignment/` under a group's prefix (#359).
    fn group_assignments_prefix(&self, group_id: &str) -> Option<Path> {
        self.group_prefix(group_id)
            .map(|prefix| Path::from(format!("{prefix}/assignment")))
    }

    /// A generation's immutable assignment (#359). Zero-padded so the listing
    /// is in generation order, as `{seq}.seg` is for segments.
    ///
    /// `None` for a negative generation: zero-padding one yields `00000000-1`,
    /// which neither sorts nor parses back, so the housekeeping sweep could
    /// never remove it. Generations are minted from zero upward, so this is a
    /// guard against a caller bug rather than a reachable state.
    fn group_assignment_location(&self, group_id: &str, generation_id: i32) -> Option<Path> {
        if generation_id < 0 {
            return None;
        }

        self.group_assignments_prefix(group_id)
            .map(|prefix| Path::from(format!("{prefix}/{generation_id:0>10}.json")))
    }

    /// A group's decomposed objects, composed back into the [`GroupDetail`]
    /// every reader already projects from (#359).
    ///
    /// `None` when the group has no `generation.json`, which is how a group
    /// that does not exist and a group still in the legacy layout both read —
    /// the caller falls back to `{group}.json`.
    ///
    /// Deliberately **not** a trait method: the composition is a property of
    /// this layout, not of storage, and putting it on the trait would oblige
    /// every engine to reproduce a fan-out it has no objects for.
    ///
    /// Torn reads are answered, never failed. Between reading the generation
    /// and reading the member documents a member can leave, and between
    /// observing a leader and reading the assignment a rebalance can start.
    /// So a member the generation names whose document is gone is reported
    /// with empty metadata rather than dropped — it *is* a member, the
    /// generation is what says so — and a generation with a leader but no
    /// assignment reports `CompletingRebalance`, which is true, rather than a
    /// phantom `Stable`.
    async fn group_view(&self, group_id: &str) -> Result<Option<GroupDetail>> {
        /// As `describe_groups`' own fan-out: one round trip per member, and a
        /// group is tens of members.
        const MEMBER_FETCH_CONCURRENCY: usize = 32;

        let Some((generation, _)) = self.read_group_generation(group_id).await? else {
            return Ok(None);
        };

        let assignment = self
            .read_group_assignment(group_id, generation.generation_id)
            .await?;

        // From the generation's member set, not from a listing: the set is
        // authoritative, and a LIST here would put one on the describe path for
        // every group an admin client asks about.
        let members = futures::stream::iter(generation.members.keys().cloned().map(|member_id| {
            let generation = &generation;

            async move {
                let held = self
                    .read_group_member(group_id, &member_id)
                    .await
                    .inspect_err(|err| debug!(?err, group_id, member_id))
                    .ok()
                    .flatten();

                let join_response = held
                    .as_ref()
                    .map(|(doc, _)| doc.join_response.clone())
                    .unwrap_or_else(|| {
                        JoinGroupResponseMember::default()
                            .member_id(member_id.clone())
                            .group_instance_id(
                                generation
                                    .members
                                    .get(&member_id)
                                    .and_then(|held| held.group_instance_id.clone()),
                            )
                    });

                let last_contact = held
                    .as_ref()
                    .and_then(|(doc, _)| to_system_time(doc.last_contact_ms).ok());

                (
                    member_id,
                    GroupMember {
                        join_response,
                        last_contact,
                    },
                )
            }
        }))
        .buffered(MEMBER_FETCH_CONCURRENCY)
        .collect::<BTreeMap<_, _>>()
        .await;

        let state = match (generation.leader.clone(), assignment) {
            (Some(leader), Some(assignment))
                if assignment.generation_id == generation.generation_id =>
            {
                GroupState::Formed {
                    protocol_type: assignment.protocol_type,
                    protocol_name: assignment.protocol_name,
                    leader,
                    assignments: assignment.assignments,
                }
            }

            (leader, _) => GroupState::Forming {
                protocol_type: generation.protocol_type.clone(),
                protocol_name: generation.protocol_name.clone(),
                leader,
            },
        };

        Ok(Some(GroupDetail {
            session_timeout_ms: generation.session_timeout_ms,
            rebalance_timeout_ms: generation.rebalance_timeout_ms,
            members,
            generation_id: generation.generation_id,
            skip_assignment: generation.skip_assignment,
            inception: to_system_time(generation.inception_ms).unwrap_or(SystemTime::UNIX_EPOCH),
            state,
        }))
    }

    /// Every ACL in the cluster, as one object (#363).
    ///
    /// One object rather than one per rule because of how it is read: the
    /// request path needs all of them to answer any question, so a per-rule
    /// keyspace would put a LIST on the authorization path — the one place
    /// that can least afford one.
    fn acls_location(&self) -> Path {
        Path::from(format!("clusters/{}/acls.json", self.cluster))
    }

    /// Every client quota in the cluster, as one object (#384).
    ///
    /// The same choice as `acls.json` above and for the same reason: the
    /// request path holds a snapshot of all of them to answer any question, so
    /// a key per entity would put a LIST behind the refresh of the one cache
    /// that has to be cheap.
    fn quotas_location(&self) -> Path {
        Path::from(format!("clusters/{}/quotas.json", self.cluster))
    }

    /// One object per principal per mechanism.
    ///
    /// The opposite choice to `acls.json` above, and for the opposite reason:
    /// the ACLs are read all-at-once to answer any question, whereas a handshake
    /// knows exactly whose credential it wants and never needs another's. One
    /// key per user means the handshake is a GET of a known key rather than a
    /// LIST, and two administrators changing two passwords do not contend.
    ///
    /// The mechanism is in the key because SCRAM-SHA-256 and SCRAM-SHA-512
    /// derive different keys from the same password: they are two credentials
    /// for one user, and a client picks which to present.
    fn user_scram_credential_location(&self, user: &str, mechanism: ScramMechanism) -> Path {
        // `Path::from` percent-encodes what it must, so a user name with a
        // slash in it stays one path segment rather than silently becoming two.
        Path::from(format!(
            "clusters/{}/users/{user}/{}.json",
            self.cluster,
            match mechanism {
                ScramMechanism::Scram256 => "scram-sha-256",
                ScramMechanism::Scram512 => "scram-sha-512",
            }
        ))
    }

    /// The cluster's ACLs and the version to CAS the next write against.
    ///
    /// A cluster that has never had an ACL applied has no object, which reads
    /// as an empty set rather than as an error: "no rules" is a state, and on a
    /// fail-closed broker it is the *most* consequential one, so it must not
    /// depend on somebody having written the object first.
    async fn read_acls(&self) -> Result<(Acls, Option<Version>)> {
        Ok(
            match Self::absent_is_none(self.get::<Acls>(&self.acls_location()).await)? {
                Some((acls, version)) => (acls, Some(version)),
                None => (Acls::default(), None),
            },
        )
    }

    /// Read-modify-CAS the ACL object.
    ///
    /// `apply` is re-run from scratch on every lost race, against the document
    /// that won — never replayed onto the one that lost. Two operators
    /// applying different rules at the same moment both land; the alternative,
    /// last-writer-wins, silently drops one of them.
    async fn update_acls<F>(&self, mut apply: F) -> Result<()>
    where
        F: FnMut(&mut Acls),
    {
        /// Generous: ACL writes are administrative and rare, so a conflict
        /// means two operators at once rather than sustained contention, and
        /// giving up on one is worse than trying again.
        const ATTEMPTS: u32 = 16;

        for attempt in 0..ATTEMPTS {
            let (mut acls, version) = self.read_acls().await?;

            apply(&mut acls);

            match self
                .put(
                    &self.acls_location(),
                    acls,
                    json_content_type(),
                    version.map(Into::into),
                )
                .await
            {
                Ok(_) => return Ok(()),

                // `Vanished` is the same instruction as `Outdated` here and for
                // the same reason: this is a read-modify-write loop, so the next
                // attempt re-reads. It finds the object absent, `version` is
                // `None`, and the put becomes a `PutMode::Create` — which is
                // exactly what re-applying onto "there is no value" means (#431).
                Err(UpdateError::Outdated { .. } | UpdateError::Vanished) => {
                    debug!(attempt, cluster = self.cluster, "acl update lost the CAS");
                    sleep(Duration::from_millis(5 * u64::from(1 + attempt))).await;
                }

                Err(UpdateError::Error(error)) => return Err(error),
                Err(UpdateError::SerdeJson(error)) => return Err(Error::SerdeJson(error)),
                Err(UpdateError::Uuid(error)) => return Err(Error::Uuid(error)),
                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from(
                        "acl update reported a missing etag",
                    )));
                }
            }
        }

        Err(Error::Message(format!(
            "could not write the acls of cluster {} in {ATTEMPTS} attempts",
            self.cluster,
        )))
    }

    /// The cluster's quotas and the version to CAS the next write against.
    ///
    /// A cluster that has never had a quota applied has no object, which reads
    /// as no quotas rather than as an error — and on a broker that fails open,
    /// "no quotas" has to be a state a fresh cluster can be in without anybody
    /// having written the object first.
    async fn read_quotas(&self) -> Result<(Quotas, Option<Version>)> {
        Ok(
            match Self::absent_is_none(self.get::<Quotas>(&self.quotas_location()).await)? {
                Some((quotas, version)) => (quotas, Some(version)),
                None => (Quotas::default(), None),
            },
        )
    }

    /// Read-modify-CAS the quota object.
    ///
    /// `apply` is re-run from scratch against the document that won every lost
    /// race, never replayed onto the one that lost — the reasoning is
    /// [`Self::update_acls`]'s, and so is the attempt count: quota writes are
    /// administrative and rare, so a conflict means two operators at once.
    async fn update_quotas<F>(&self, mut apply: F) -> Result<()>
    where
        F: FnMut(&mut Quotas),
    {
        const ATTEMPTS: u32 = 16;

        for attempt in 0..ATTEMPTS {
            let (mut quotas, version) = self.read_quotas().await?;

            apply(&mut quotas);

            match self
                .put(
                    &self.quotas_location(),
                    quotas,
                    json_content_type(),
                    version.map(Into::into),
                )
                .await
            {
                Ok(_) => return Ok(()),

                // `Vanished` is the same instruction as `Outdated` here and for
                // the same reason: this is a read-modify-write loop, so the next
                // attempt re-reads. It finds the object absent, `version` is
                // `None`, and the put becomes a `PutMode::Create` — which is
                // exactly what re-applying onto "there is no value" means (#431).
                Err(UpdateError::Outdated { .. } | UpdateError::Vanished) => {
                    debug!(attempt, cluster = self.cluster, "quota update lost the CAS");
                    sleep(Duration::from_millis(5 * u64::from(1 + attempt))).await;
                }

                Err(UpdateError::Error(error)) => return Err(error),
                Err(UpdateError::SerdeJson(error)) => return Err(Error::SerdeJson(error)),
                Err(UpdateError::Uuid(error)) => return Err(Error::Uuid(error)),
                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from(
                        "quota update reported a missing etag",
                    )));
                }
            }
        }

        Err(Error::Message(format!(
            "could not write the client quotas of cluster {} in {ATTEMPTS} attempts",
            self.cluster,
        )))
    }

    /// Read `Option`, not `Result`: an object that is not there is an answer —
    /// the group has no generation, the member has no document — and only a
    /// store error is a failure.
    fn absent_is_none<V>(result: Result<(V, Version)>) -> Result<Option<(V, Version)>> {
        match result {
            Ok(pair) => Ok(Some(pair)),
            Err(Error::ObjectStore(error))
                if matches!(error.as_ref(), object_store::Error::NotFound { .. }) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Which group owns an object under [`Self::groups_root`], or `None` for
    /// one that belongs to no group.
    ///
    /// Layout-agnostic on purpose (#359): a group owns `{group}.json` directly
    /// under the root — the legacy state object — *and* everything under
    /// `{group}/`: its committed offsets, and now its member documents, its
    /// generation and its assignments. Keying on the *prefix* rather than on
    /// any one object is what lets expiry outlive the layout.
    ///
    /// An id that normalises to nothing (a stray object named exactly `.json`)
    /// is refused here rather than passed to `delete_groups`, which would
    /// otherwise report `InvalidGroupId` once per maintenance tick, forever.
    fn group_of(root: &Path, location: &Path) -> Option<String> {
        let mut parts = location.prefix_match(root)?;
        let first = parts.next()?;

        let group_id = if parts.next().is_some() {
            first.as_ref().to_owned()
        } else {
            first.as_ref().strip_suffix(".json")?.to_owned()
        };

        (!group_id.is_empty()).then_some(group_id)
    }

    /// Enforce the `delete` cleanup policy: for every topic configured with
    /// `cleanup.policy` containing `delete`, drop the batches whose records are
    /// older than `retention.ms` (defaulting to 7 days, matching the SQL
    /// backends). Returns the number of batches removed.
    #[instrument(skip(self), ret)]
    /// Expire consumer groups with no activity anywhere under their prefix
    /// within [`GROUP_RETENTION`].
    ///
    /// One streaming listing of `clusters/{cluster}/groups/consumers/`, folded
    /// into the most recent `last_modified` per group. A group is condemned
    /// when *nothing* it owns has been touched inside the window — its state
    /// object, its committed offsets, its member documents, its generation.
    ///
    /// **The signal is the whole prefix, not one object (#272, #359).** The
    /// legacy state object's mtime freezes once a group stops rewriting it,
    /// which a commit-only consumer does after its first commit: age on that
    /// object alone said "abandoned" about a consumer that was still
    /// committing, and expiry then deleted the group state *and every committed
    /// offset under it*. The decomposed layout (#359) has no `{group}.json` at
    /// all, so a rule keyed on that object would have stopped condemning
    /// anything — the same reasoning, failing the other way, into an unbounded
    /// leak. Folding the prefix answers both, and is why the rule survives the
    /// layout change without a flag: a member's liveness write is now activity,
    /// which is what it always should have been.
    ///
    /// The failure direction is deliberate: every way this can be wrong keeps a
    /// group that would have been deleted. Expiry reclaiming late is #45's
    /// complaint; expiry reclaiming a live consumer's offsets is data loss.
    ///
    /// Cost is one listing page per thousand objects under the consumer tree,
    /// per tick — where the previous shape paid one delimited listing plus one
    /// listing *per candidate*, and a candidate was any group old enough to be
    /// considered.
    ///
    /// Deletions are capped at [`GROUP_EXPIRE_CHUNK`] per tick so a large
    /// accumulated backlog (e.g. groups leaked by a one-group-per-subscription
    /// client model) drains gradually rather than issuing tens of thousands of
    /// deletes at once — the concentrated object-store pressure that degraded
    /// the broker in #8. See #45. The oldest go first, so a backlog drains in
    /// the order it accumulated rather than in listing order.
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

        let root = self.groups_root();

        // Folded as the listing streams: one entry per group, not one per
        // object, so the tick's memory is the group count however many objects
        // each group owns.
        let mut latest: BTreeMap<String, i64> = BTreeMap::new();
        let mut listing = self.scan(Scan::Group, &root);

        while let Some(meta) = listing
            .next()
            .await
            .transpose()
            .inspect_err(|err| error!(?err, cluster = self.cluster))?
        {
            let Some(group_id) = Self::group_of(&root, &meta.location) else {
                continue;
            };

            let modified = meta.last_modified.timestamp_millis();

            _ = latest
                .entry(group_id)
                .and_modify(|held| *held = (*held).max(modified))
                .or_insert(modified);
        }

        let mut stale = latest
            .into_iter()
            .filter(|(_, latest_ms)| *latest_ms < threshold_ms)
            .collect::<Vec<_>>();

        if stale.is_empty() {
            return Ok(0);
        }

        // Oldest first, so a backlog drains in the order it accumulated.
        stale.sort_by_key(|(_, latest_ms)| *latest_ms);

        let capped = stale.len() > GROUP_EXPIRE_CHUNK;
        stale.truncate(GROUP_EXPIRE_CHUNK);

        let stale = stale
            .into_iter()
            .map(|(group_id, _)| group_id)
            .collect::<Vec<_>>();

        let expired = stale.len() as u64;

        // `delete_groups` removes each state object and every object under the
        // group prefix, logging the per-group outcome.
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

    /// Record the size of the cluster-global `meta.json` and of the two tables
    /// inside it that nothing prunes (#283), returning `(producers, transactions,
    /// bytes)` so the numbers can be asserted without standing up a metrics
    /// reader.
    ///
    /// **Measurement, not a fix.** The producer table grows by one entry per
    /// `InitProducerId` — one per connector restart — and there is no `remove` or
    /// `retain` on it anywhere in the tree. Whether that needs an expiry policy
    /// *now* is a question about the production bucket's actual growth rate, and
    /// no amount of reading the code answers it; the transaction half additionally
    /// needs a decision, since #81 retains aborted transactions on purpose. So
    /// this attaches the growth math before the table is big enough to matter,
    /// which is the whole of what the issue asks for at this stage.
    ///
    /// On the maintenance tick, which is the right cadence for a level that moves
    /// with restarts rather than with traffic: at one conditional GET per tick it
    /// is a rounding error against the pass it runs in, and the cached etag makes
    /// most of those a `NotModified`. The serialisation is the same one
    /// `OptiCon::with_mut` performs on every producer registration, so its cost is
    /// already characterised.
    async fn measure_meta(&self) -> Result<(u64, u64, u64)> {
        let measured = self
            .meta
            .with(&self.object_store, |meta| {
                Ok((
                    meta.producers.len() as u64,
                    meta.transactions.len() as u64,
                    serde_json::to_vec(meta)?.len() as u64,
                ))
            })
            .await?;

        let (producers, transactions, bytes) = measured;

        META_PRODUCERS.record(producers, &[]);
        META_TRANSACTIONS.record(transactions, &[]);
        META_BYTES.record(bytes, &[]);

        Ok(measured)
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
        //
        // The same CAS certifies what this expiry leaves servable (#290): the
        // tail of the sub-stream's *surviving* segments, paired with the floor
        // being written. Only this operation knows whether `floor > tail` means
        // "a peer acked offsets you have not listed" (advertise the floor) or
        // "the tail-holding segment is about to be deleted" (a fetch parked in
        // `[surviving tail, floor)` waits for records that can never come) —
        // writing that knowledge down is what lets the fetch path answer
        // `OffsetOutOfRange` instead of empty-forever. `None` survivors certify
        // at the floor itself: an empty log whose end is the floor, which #299
        // already reports as starting where it ends.
        let expirable_seqs: BTreeSet<u64> = expirable.iter().copied().collect();
        for (topic, partition) in &affected {
            let segments = self.valid_substream_segments(prefix, topic, *partition)?;
            let tail = segments.last().map(|(_, e)| e.base_offset + e.record_count);
            let surviving = segments
                .iter()
                .filter(|(seq, _)| !expirable_seqs.contains(seq))
                .map(|(_, e)| e.base_offset + e.record_count)
                .max();
            if let Some(tail) = tail {
                let tp = Topition::new(topic.clone(), *partition);
                _ = self
                    .watermark(&tp)?
                    .with_mut(&self.object_store, |watermark| {
                        let high = watermark.high.unwrap_or(0).max(tail);
                        watermark.high = Some(high);
                        watermark.served = Some(ServedEnd {
                            end: surviving.unwrap_or(high),
                            at_high: high,
                        });
                        Ok(())
                    })
                    .await
                    .inspect_err(|err| debug!(?err, ?tp));
            }
        }

        // Floor before delete, then prune (#77) — see [`Self::retire_segments`].
        let deleted = self.retire_segments(prefix, &expirable).await?;

        // This process's next-offset hints and cached watermark floors for the
        // affected sub-streams predate the delete: drop them so the next read
        // re-derives from the pruned index and the re-read watermark — which is
        // also what makes the served-end certification written above visible
        // locally without waiting out the hint TTL (#290). Peers converge on
        // their own: the seq-floor raise invalidates their cached watermark
        // floors, so their next read pays the one GET and sees the pair.
        self.next_offsets.lock().map(|mut locked| {
            for (topic, partition) in &affected {
                _ = locked.remove(&Topition::new(topic.clone(), *partition));
            }
        })?;
        self.coalesced_watermark_floors.lock().map(|mut locked| {
            for (topic, partition) in &affected {
                _ = locked.remove(&Topition::new(topic.clone(), *partition));
            }
        })?;

        Ok(deleted)
    }

    /// The segments of `prefix` this process has proved undecodable (#398).
    ///
    /// Snapshotted rather than read under the lock by the caller: run selection
    /// walks the prefix's whole segment list, and holding a process-wide lock
    /// across that walk would serialise every prefix being maintained
    /// concurrently. The set is small by construction
    /// ([`Self::PREFIX_QUARANTINE_CAP`]).
    fn quarantined_segments_of(&self, prefix: &str) -> Result<BTreeSet<u64>> {
        Ok(self
            .quarantined_segments
            .lock()
            .map_err(Into::<Error>::into)?
            .get(prefix)
            .cloned()
            .unwrap_or_default())
    }

    /// Exclude `region`'s segment from this prefix's future compaction runs
    /// (#398). `true` when it was not already excluded — the first sight, and
    /// the only one that logs.
    ///
    /// `false` therefore means "quarantining this changes nothing", which is the
    /// signal the caller needs: the run that just failed was already selected
    /// with this segment excluded, so the failure is somewhere the skip list
    /// cannot reach and retrying the drain would spin.
    fn quarantine_segment(&self, region: &CorruptRegion) -> Result<bool> {
        let held = {
            let mut quarantined = self
                .quarantined_segments
                .lock()
                .map_err(Into::<Error>::into)?;
            let seqs = quarantined.entry(region.prefix.clone()).or_default();

            if !seqs.contains(&region.seq) && seqs.len() >= Self::PREFIX_QUARANTINE_CAP {
                warn!(
                    prefix = region.prefix,
                    seq = region.seq,
                    cap = Self::PREFIX_QUARANTINE_CAP,
                    "quarantine cap reached; this prefix's drain still ends on this segment"
                );

                return Ok(false);
            }

            if !seqs.insert(region.seq) {
                return Ok(false);
            }

            seqs.len() as u64
        };

        SEGMENT_QUARANTINES.add(1, &[]);
        SEGMENTS_QUARANTINED.record(held, &[KeyValue::new("prefix", region.prefix.clone())]);

        // Once, at `warn` — the whole region detail, so the object is
        // identifiable — where this used to be an `ERROR` on every maintenance
        // tick for as long as the object existed (#398). The steady state is
        // `SEGMENTS_QUARANTINED`, not a log stream.
        warn!(
            ?region,
            "excluding an undecodable segment from this prefix's compaction"
        );

        Ok(true)
    }

    /// Drop quarantine entries naming segments `prefix` no longer holds (#398)
    /// and report what survives.
    ///
    /// The skip list must not outlive the objects in it: retention and the
    /// per-key rewrite retire segments the size merge never selects, and a
    /// sequence is never reused (#77), so an entry whose object is gone is dead
    /// weight — and a gauge that never falls would read as damage that was never
    /// cleared.
    fn prune_quarantine(&self, prefix: &str, live: &BTreeSet<u64>) -> Result<()> {
        let held = {
            let mut quarantined = self
                .quarantined_segments
                .lock()
                .map_err(Into::<Error>::into)?;

            let Some(seqs) = quarantined.get_mut(prefix) else {
                return Ok(());
            };

            seqs.retain(|seq| live.contains(seq));
            let held = seqs.len() as u64;

            if held == 0 {
                _ = quarantined.remove(prefix);
            }

            held
        };

        SEGMENTS_QUARANTINED.record(held, &[KeyValue::new("prefix", prefix.to_string())]);

        Ok(())
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
    async fn compact_prefix_segments(&self, prefix: &str) -> Result<CompactRun> {
        if self.prefix_compact_min_segments == 0 {
            return Ok(CompactRun::Drained);
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

        // Segments a previous run proved undecodable (#398). Read before the
        // walk below so the lock is not held across it.
        let quarantined = self.quarantined_segments_of(prefix)?;

        // The offset spans a run would cover, needed only once this prefix has a
        // hole in its tiling — see [`RunCoverage`]. Skipped entirely otherwise:
        // a prefix with no quarantined segment is contiguous by construction, and
        // this clones a `Vec<SubstreamEntry>` per segment on a path that walks
        // tens of thousands of them per drain.
        let spans: BTreeMap<u64, Vec<SubstreamEntry>> = if quarantined.is_empty() {
            BTreeMap::new()
        } else {
            let index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            index
                .get(prefix)
                .map(|entry| {
                    entry
                        .segments
                        .iter()
                        .map(|(seq, cached)| (*seq, cached.footer.entries.clone()))
                        .collect()
                })
                .unwrap_or_default()
        };

        // Only above the trigger, and never touch the hot (newest) tail.
        if segs.len() <= self.prefix_compact_min_segments {
            return Ok(CompactRun::Drained);
        }
        let eligible_end = segs.len().saturating_sub(self.prefix_compact_keep_hot);
        if eligible_end < 2 {
            return Ok(CompactRun::Drained);
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
                // boundary: leave it alone and seed the run after it. So is a
                // quarantined one (#398) — and it has to be a *boundary* rather
                // than a filtered-out element, because the merged segment carries
                // the base offset of its first region and concatenates the rest:
                // merging across the hole would shift every following record's
                // offset down into it, which is corruption where today there is
                // only a stalled drain.
                if segs[start].3 >= self.prefix_compact_target_bytes
                    || quarantined.contains(&segs[start].0)
                {
                    start += 1;
                    continue;
                }

                let mut bytes = 0usize;
                let mut end = start;
                let mut coverage = RunCoverage::default();
                while end < eligible_end
                    && segs[end].3 < self.prefix_compact_target_bytes
                    && !quarantined.contains(&segs[end].0)
                    && (end == start || bytes + segs[end].3 <= self.prefix_compact_target_bytes)
                    // A hole in the prefix's offset tiling ends the run here
                    // (#398). Only ever consulted when something is quarantined,
                    // where `spans` is populated; empty means "no hole to cross".
                    && spans
                        .get(&segs[end].0)
                        .is_none_or(|entries| coverage.extend(entries))
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
            return Ok(CompactRun::Drained);
        }

        // Coordinate compactors with a *separate* compaction lease (#66 review):
        // compaction runs on the maintenance workers, which do not hold the
        // produce lease, so it must not require — or fence — the produce writer.
        // If another compactor holds this prefix, yield.
        if self.acquire_compaction_lease(prefix).await.is_err() {
            return Ok(CompactRun::Drained);
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

        // GET each run segment once; a ghost index entry yields the tick (#274).
        // Run selection picks the oldest segments, which are exactly the ones a
        // peer's compaction may already have deleted from under this replica's
        // add-only index.
        let Some(objects) = self
            .fetch_segment_objects(prefix, run.iter().copied())
            .await?
        else {
            // Not "drained" (#399): the run selected segments a peer had already
            // retired, and `fetch_segment_objects` has pruned them from the
            // index — so the *next* selection is over a different segment set and
            // the drain must take it rather than stopping here.
            return Ok(CompactRun::Retry);
        };

        // Merge the EPOCH-FENCED view (#66 review fix, critical): rebuild each
        // sub-stream from `decode_fenced_regions` (overlap-resolved, higher
        // epoch/sequence wins) restricted to the run — NOT the raw footer
        // entries. A zombie/overlap input is dropped there, never fused into the
        // merged segment, so compaction can't bake in duplicate/shifted offsets.
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
            let in_run = self.decode_fenced_regions(prefix, &topic, partition, &objects)?;

            // Every run segment holding this sub-stream is superseded by a
            // segment outside the run — nothing to carry forward.
            let Some((_, base, _)) = in_run.first() else {
                continue;
            };
            let base = *base;

            let mut batches = Vec::new();
            for (seq, _, region) in in_run {
                batches.extend(region);
                if let Some(footer) = footers.get(&seq) {
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
            let nonce = rng().random::<u64>();
            let (payload, footer) =
                self.encode_segment_v3(&substreams, merged_epoch.max(0), nonce)?;
            let seq = self
                .assign_and_create_segment(prefix, payload, nonce, SegmentCreateRole::Compaction)
                .await?;
            self.index_insert(prefix, seq, footer, max_last_modified)?;
            Some(seq)
        };

        // Retire the run: floor before delete, then prune (#77) — see
        // [`Self::retire_segments`]. Compaction usually adds a higher merged seq
        // so the listing max is unchanged, but when every run segment is
        // superseded no merged seq is written (`new_seq == None`) and deleting
        // the run *can* lower the listing max — freeing a run name for reuse
        // without the floor.
        _ = self.retire_segments(prefix, &run).await?;

        SEGMENT_COMPACTIONS.add(run.len() as u64, &[]);
        debug!(
            prefix,
            ?new_seq,
            merged = run.len(),
            "compacted prefix segments"
        );

        Ok(CompactRun::Merged(run.len() as u64))
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

        // Segments a compaction pass has proved undecodable (#398). Excluded
        // outright here rather than treated as a run boundary: this pass rewrites
        // each segment in place under its own `base_offset`, so dropping one from
        // the walk shifts nothing — where the size merge would fuse across the
        // hole. Without the exclusion one bad object costs the prefix its per-key
        // pass on every tick, the same permanent stall the size merge had.
        let quarantined = self.quarantined_segments_of(prefix)?;

        // Snapshot `(writer_epoch, last_modified_ms)` per segment and the
        // sub-stream key set from the cached footers — no object requests.
        let mut segments_meta: BTreeMap<u64, (i64, i64)> = BTreeMap::new();
        let mut substream_keys: BTreeSet<(String, i32)> = BTreeSet::new();
        {
            let index = self.prefix_index.lock().map_err(Into::<Error>::into)?;
            if let Some(entry) = index.get(prefix) {
                for (seq, cached) in &entry.segments {
                    if quarantined.contains(seq) {
                        continue;
                    }

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

        // GET every segment of the prefix once; a ghost index entry yields the
        // tick, as for the size merge (#274).
        let Some(objects) = self
            .fetch_segment_objects(prefix, segments_meta.keys().copied())
            .await?
        else {
            return Ok(0);
        };

        // Per-key transform over the EPOCH-FENCED view (as the size merge): a
        // zombie/overlap region is dropped from any rewrite, never fused in.
        // `outputs[seq]` accumulates every fenced sub-stream's (possibly
        // compacted) region so a dirty segment can be rebuilt whole.
        let mut outputs: BTreeMap<u64, Vec<(Topition, i64, Vec<deflated::Batch>)>> =
            BTreeMap::new();
        let mut dirty: BTreeSet<u64> = BTreeSet::new();
        let mut removed_total: u64 = 0;
        let mut repaired_total: u64 = 0;

        for (topic, partition) in substream_keys {
            // Decode every fenced region once, then walk them newest first — a
            // key kept in a newer region supersedes older copies.
            let mut regions = self.decode_fenced_regions(prefix, &topic, partition, &objects)?;
            regions.reverse();

            let mut seen: BTreeSet<Bytes> = BTreeSet::new();
            let mut staged: Vec<(u64, i64, Vec<deflated::Batch>, u64, u64)> =
                Vec::with_capacity(regions.len());
            let mut aborted = false;

            'transform: for (seq, base, region) in &regions {
                // Newest batch first within the region too.
                let mut out: Vec<deflated::Batch> = Vec::with_capacity(region.len());
                let mut removed: u64 = 0;
                let mut repaired: u64 = 0;
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

                    // A batch whose LZ4 frame has dependent blocks is durable damage
                    // (#253): no Kafka Java client can decode it, and nothing else
                    // will ever rewrite it — the per-key transform below only
                    // re-encodes a batch it removes records from, and an emptied
                    // remnant has no keys left to supersede. Repairing it here is
                    // what takes a partition from "one worker cannot start" back to
                    // readable, and it costs a rewrite of a segment that is being
                    // rewritten anyway whenever anything else in it is dirty.
                    let repair_frame = batch.has_dependent_lz4_blocks();

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
                    } else if repair_frame {
                        // Nothing to compact, but the frame itself is unreadable:
                        // re-encode the batch unchanged. The encoder emits an
                        // independent-block frame now, so this is the repair.
                        repaired += 1;
                        out.push(deflated::Batch::try_from(compaction.batch)?);
                    } else {
                        // Untouched: carry the ORIGINAL batch, not a re-encode.
                        out.push(batch.clone());
                    }
                }
                out.reverse();
                staged.push((*seq, *base, out, removed, repaired));
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

            for (seq, base, out, removed, repaired) in staged {
                // Either reason dirties the segment: records removed, or a frame
                // repaired (#253). A repair changes no record, so it is not counted
                // as a removal — but it must still reach the object store, or the
                // damage stays durable.
                if removed > 0 || repaired > 0 {
                    _ = dirty.insert(seq);
                    removed_total += removed;

                    if repaired > 0 {
                        repaired_total += repaired;

                        // `warn`, not `info` (#259): re-encoding durable damage is
                        // not steady state. It is bounded by the frames written
                        // while the encoder bug was live, each occurrence is the
                        // recovery of a partition no Java client could read, and
                        // production runs at `warn` — at `info` this line was
                        // structurally unable to appear where it is needed.
                        warn!(
                            prefix,
                            topic,
                            partition,
                            repaired,
                            "re-encoded LZ4 frames with dependent blocks (#253)"
                        );
                    }
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

            let nonce = rng().random::<u64>();
            let (payload, footer) = self.encode_segment_v3(substreams, epoch.max(0), nonce)?;
            let new_seq = self
                .assign_and_create_segment(prefix, payload, nonce, SegmentCreateRole::Compaction)
                .await?;
            self.index_insert(prefix, new_seq, footer, last_modified)?;
        }

        // Retire the rewritten seqs: floor before delete, then prune (#77) — see
        // [`Self::retire_segments`].
        let retired: Vec<u64> = dirty.iter().copied().collect();
        _ = self.retire_segments(prefix, &retired).await?;

        SEGMENT_RECORDS_COMPACTED.add(removed_total, &[]);
        SEGMENT_FRAMES_REPAIRED.add(repaired_total, &[]);
        debug!(
            prefix,
            removed = removed_total,
            repaired = repaired_total,
            rewritten = dirty.len(),
            "per-key compacted prefix segments"
        );

        Ok(removed_total)
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
                MAINTENANCE_PREFIXES.add(1, &[KeyValue::new("outcome", "recent")]);
                continue;
            }
            match self.acquire_compaction_lease(&prefix).await {
                Ok(_) => {
                    MAINTENANCE_PREFIXES.add(1, &[KeyValue::new("outcome", "claimed")]);
                    _ = owned.insert(prefix);
                }
                Err(Error::Api(ErrorCode::NotLeaderOrFollower)) => {
                    MAINTENANCE_PREFIXES.add(1, &[KeyValue::new("outcome", "lost")]);
                }
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

    /// Drain one prefix to `<= prefix_compact_min_segments` (#66 review fix): a
    /// single run per tick cannot keep up with a high flush rate, so loop until
    /// compaction finds nothing more to merge. Each call re-lists, so `S`
    /// converges to the trigger threshold within the tick. Errors are logged and
    /// end this prefix's drain only — one bad prefix must never abort the others'
    /// maintenance (#140).
    ///
    /// An **undecodable segment is the exception** (#398). `Ok(0)` and `Err(_)`
    /// used to share one `break`, which conflated "there is nothing left to
    /// merge" with "this run died", and run selection picks the *oldest*
    /// mergeable segments — so a damaged old object was re-read and re-failed on
    /// every tick that reached it, for as long as it existed, having merged
    /// nothing for the prefix. `CorruptSegment` names the segment, so the run can
    /// skip it and the drain can carry on over what is readable: #274's fix for
    /// `NotFound`, one error variant later.
    ///
    /// Every other error still ends the drain. They are not attributable to one
    /// object, so there is nothing to exclude and nothing to make the next run
    /// different — retrying would be a hot loop against the object store.
    ///
    /// A run that could not *proceed* is the third case (#399), and it used to be
    /// the same `break` as well: `fetch_segment_objects` answering `None` — the
    /// index named segments a peer had already retired — came back as `Ok(0)`,
    /// which reads as "drained". Selection picks the oldest segments, which are
    /// exactly the ones a peer's compaction retires first, and
    /// `tansu_prefix_segment_vanished_before_read` runs at 11/s on the fleet
    /// while the busiest prefixes sit at 30–68× their trigger. The pruned index
    /// makes the next selection different, so the drain takes it.
    ///
    /// Why the drain stopped is reported (`tansu_prefix_drain_stops`), because
    /// "does a drain that starts on a 15 000-segment prefix finish it?" was
    /// otherwise unanswerable from outside.
    async fn drain_compact_prefix(&self, prefix: &str) -> u64 {
        /// Bounds the drain loop so a pathological prefix can't monopolize a
        /// maintenance tick; far above the runs a real backlog needs.
        const MAX_RUNS_PER_PREFIX: usize = 4_096;

        /// Bounds the runs that could not proceed, so a listing that keeps
        /// naming segments that are gone cannot spin.
        ///
        /// High on purpose. Each such run prunes *every* gone segment its run
        /// named, so the ghosts are consumed monotonically and the ceiling is
        /// (ghost entries / segments per run) — on the fleet, an index carrying
        /// tens of thousands of stale entries against runs of ~22 segments. A
        /// low cap would leave the drain stopping short of the live segments for
        /// exactly the prefixes furthest over the trigger, which is the
        /// behaviour being fixed. The real bounds are `MAX_RUNS_PER_PREFIX`
        /// above and the broker's maintenance run timeout (#131).
        const MAX_RETRIES_PER_PREFIX: usize = 1_024;

        let mut compacted = 0;
        let mut retries = 0usize;
        let mut reason = "runs";

        for _ in 0..MAX_RUNS_PER_PREFIX {
            let outcome = self.compact_prefix_segments(prefix).await;

            SEGMENT_COMPACT_RUNS.add(
                1,
                &[KeyValue::new(
                    "outcome",
                    outcome.as_ref().map_or("error", CompactRun::outcome),
                )],
            );

            match outcome {
                Ok(CompactRun::Merged(n)) => compacted += n,

                Ok(CompactRun::Drained) => {
                    reason = "drained";
                    break;
                }

                Ok(CompactRun::Retry) => {
                    retries += 1;
                    if retries >= MAX_RETRIES_PER_PREFIX {
                        warn!(
                            prefix,
                            retries, "drain kept selecting segments that are gone"
                        );
                        reason = "retries";
                        break;
                    }
                }

                // Damage attributable to one object: exclude it and keep
                // draining. `quarantine_segment` returning `false` means the run
                // was *already* selected without it, so the failure is not the
                // one this skip list can route around — end the drain as any
                // other error would.
                Err(Error::CorruptSegment(region)) => {
                    if !self
                        .quarantine_segment(&region)
                        .inspect_err(|err| error!(?err, prefix))
                        .unwrap_or_default()
                    {
                        error!(?region, prefix, "compaction damage the quarantine misses");
                        reason = "error";
                        break;
                    }
                }

                Err(err) => {
                    error!(?err, prefix);
                    reason = "error";
                    break;
                }
            }
        }

        PREFIX_DRAIN_STOPS.add(1, &[KeyValue::new("reason", reason)]);

        // Report the live segment count so runaway `S` is observable even if the
        // drain can't keep up.
        //
        // Labelled by prefix (#284). Recorded once per prefix per pass with no
        // attributes, last write won, so the gauge showed whichever prefix
        // happened to be drained last — never the one running away, which is the
        // only reason to look at it. Cardinality is tens of prefixes.
        let live: Option<BTreeSet<u64>> = self.prefix_index.lock().ok().and_then(|index| {
            index.get(prefix).map(|entry| {
                SEGMENTS_LIVE.record(
                    entry.segments.len() as u64,
                    &[KeyValue::new("prefix", prefix.to_string())],
                );

                entry.segments.keys().copied().collect()
            })
        });

        // Retire quarantine entries whose objects are gone (#398), under no other
        // lock — the index one above is released by here. Only against an index
        // this process actually holds for the prefix: an absent entry is "not
        // known", not "no segments", and pruning against it would silently empty
        // the skip list on a store with compaction disabled.
        if let Some(live) = live {
            _ = self
                .prune_quarantine(prefix, &live)
                .inspect_err(|err| error!(?err, prefix));
        }

        compacted
    }

    /// Run the per-key pass over `prefix`, excluding any segment it proves
    /// undecodable and retrying over the rest (#398).
    ///
    /// The pass reads *every* segment of the prefix, so one damaged object cost
    /// the whole prefix its key cleanup on every tick — the same permanent stall
    /// the size merge had, on the pass that matters more: a compacted topic with
    /// no per-key cleanup grows stale versions forever.
    ///
    /// Attempts are tightly bounded because each one re-GETs the prefix: a tick
    /// quarantines at most `MAX_ATTEMPTS - 1` new bad segments and defers the
    /// rest, which converges over ticks without turning one tick into a scan of
    /// the same prefix a hundred times over.
    async fn drain_compact_prefix_per_key(&self, prefix: &str) {
        const MAX_ATTEMPTS: usize = 8;

        for _ in 0..MAX_ATTEMPTS {
            match self.compact_prefix_per_key(prefix).await {
                Ok(_) => return,

                Err(Error::CorruptSegment(region)) => {
                    if !self
                        .quarantine_segment(&region)
                        .inspect_err(|err| error!(?err, prefix))
                        .unwrap_or_default()
                    {
                        error!(?region, prefix, "per-key damage the quarantine misses");
                        return;
                    }
                }

                Err(err) => {
                    error!(?err, prefix);
                    return;
                }
            }
        }

        warn!(
            prefix,
            attempts = MAX_ATTEMPTS,
            "per-key compaction still finding undecodable segments; the rest wait for the next tick"
        );
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
                    self.drain_compact_prefix_per_key(&prefix).await;
                }

                let compacted = if compactable.contains(&prefix) {
                    self.drain_compact_prefix(&prefix).await
                } else {
                    0
                };

                // Retro-certify gaps no expiry will reach (#290). Once per
                // prefix per process, and after the two passes above so it reads
                // the tail they left rather than the one they were about to
                // change. A failure is this prefix's alone, as for the others.
                _ = self
                    .certify_prefix_served_ends(&prefix)
                    .await
                    .inspect_err(|err| error!(?err, prefix));

                (deleted, compacted)
            }
        }))
        .buffer_unordered(PREFIX_MAINTENANCE_CONCURRENCY)
        .fold((0, 0), |(deleted, compacted), (d, c)| async move {
            (deleted + d, compacted + c)
        })
        .await;

        // Then keep going while a claimed prefix is still over the trigger
        // (#399), instead of returning and idling out the rest of the interval.
        let (deleted, compacted) = outcomes;
        let swept = self
            .sweep_backlogged_prefixes(&compactable, PREFIX_MAINTENANCE_CONCURRENCY)
            .await;

        Ok((deleted, compacted + swept))
    }

    /// Drain the claimed prefixes that are *still* over the compaction trigger,
    /// repeatedly, until the backlog stops shrinking (#399).
    ///
    /// #140's acceptance was that the busiest prefixes converge toward
    /// `prefix_compact_min_segments` under sustained produce. Its four remedies
    /// shipped and they did not: production sits at 17 500 live segments against
    /// a 256 trigger, 30–68× over, having grown ~700 segments/hour through the
    /// remedies. #140 concluded the limit was one maintainer's merge throughput
    /// and added concurrency *within* a maintainer. But the four maintainers run
    /// at **12 millicores between them** with S3 GETs at 24 ms, and compaction
    /// fires in a burst of 1–2 minutes per 10-minute window and then nothing:
    /// a 10–20 % duty cycle. They are not throughput-bound, they are unscheduled.
    ///
    /// So the interval stops deciding how much work a tick does. The tick keeps
    /// sweeping while there is a backlog it owns, and stops when a sweep merges
    /// nothing — which means the prefix is waiting on produce, not on scheduling.
    /// The outer bound stays where it was: the broker's bounded maintenance run
    /// (#131) cancels a tick that overruns, and its in-flight guard stops a slow
    /// tick from being joined by the next one.
    ///
    /// The claim is deliberately **not** re-taken between sweeps. It is already
    /// held for these prefixes, re-reading every lease per sweep would be
    /// O(prefixes) requests for nothing, and `maintenance_recency` (110 s in
    /// production, against a 600 s interval) would skip this replica's own claim
    /// back at it.
    ///
    /// Retention and the per-key pass are deliberately not repeated either. Both
    /// are once-per-tick work — retention is age-gated and the per-key pass GETs
    /// every segment of its prefix, so sweeping them would multiply the read cost
    /// of a clean prefix by the sweep count for no removal at all.
    async fn sweep_backlogged_prefixes(
        &self,
        compactable: &BTreeSet<String>,
        concurrency: usize,
    ) -> u64 {
        /// Bounds the sweeps so one tick cannot become unbounded even if the
        /// broker's run timeout were disabled. Each sweep that does nothing ends
        /// the loop anyway, so this is a backstop, not a budget.
        const MAX_SWEEPS: usize = 64;

        let mut compacted = 0;

        for _ in 0..MAX_SWEEPS {
            let backlogged = self.backlogged_prefixes(compactable);
            if backlogged.is_empty() {
                break;
            }

            let merged = futures::stream::iter(
                backlogged
                    .into_iter()
                    .map(|prefix| async move { self.drain_compact_prefix(&prefix).await }),
            )
            .buffer_unordered(concurrency)
            .fold(0u64, |merged, n| async move { merged + n })
            .await;

            MAINTENANCE_SWEEPS.add(1, &[]);
            compacted += merged;

            // A sweep that merged nothing is a prefix waiting for produce, not
            // for a tick. Sweeping again would re-list every backlogged prefix
            // for the same answer.
            if merged == 0 {
                break;
            }
        }

        compacted
    }

    /// The claimed prefixes whose cached live-segment count is still over the
    /// compaction trigger, largest first (#399).
    ///
    /// Read from the in-memory index this process already holds — no request —
    /// and gated on the same `min_segments` + `keep_hot` arithmetic
    /// `compact_prefix_segments` selects a run under, so a prefix that cannot
    /// yield a run is never swept for one.
    fn backlogged_prefixes(&self, compactable: &BTreeSet<String>) -> Vec<String> {
        let Ok(index) = self.prefix_index.lock() else {
            return Vec::new();
        };

        let mut backlogged: Vec<(usize, String)> = compactable
            .iter()
            .filter_map(|prefix| {
                index
                    .get(prefix)
                    .map(|entry| entry.segments.len())
                    .filter(|live| {
                        *live > self.prefix_compact_min_segments
                            && live.saturating_sub(self.prefix_compact_keep_hot) >= 2
                    })
                    .map(|live| (live, prefix.clone()))
            })
            .collect();

        // Largest known backlog first, as the tick's first pass orders itself
        // (#140): a timeout that cuts a sweep should cut the prefixes that needed
        // it least.
        backlogged.sort_by_key(|(live, _)| Reverse(*live));

        backlogged.into_iter().map(|(_, prefix)| prefix).collect()
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
            // engine follows. The legacy per-partition pass was the odd one out —
            // it read absent as retain-forever until #177 — and is gone (#179).
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

    /// Certify the dead offset gap of every sub-stream in `prefix` whose
    /// advertised floor sits above the tail of the segments that are actually
    /// there — the retro-fit #343 could not do (#290).
    ///
    /// #343 made a fetch inside a certified-dead gap answer
    /// `OFFSET_OUT_OF_RANGE` instead of empty-forever, but only the expiry that
    /// performed the delete writes the certification. Every gap that already
    /// existed carries none, and a deployment whose `maintenance_interval` is a
    /// year will never run an expiry that touches one. Those partitions keep
    /// answering empty to a parked consumer, which is #290's original complaint
    /// verbatim.
    ///
    /// ## Why this is sound, and why the read path could not do it
    ///
    /// The read path cannot tell `floor > tail` caused by *retention deleting
    /// the tail-holder* (certify) from the same shape caused by *a peer acking
    /// offsets this process has not listed* (must not certify). That was
    /// established in #290 by implementing the read-side fix and watching
    /// `coalesced_latest_survives_peer_expiry_via_floor_certification` correctly
    /// reject it. Three things separate this pass from that attempt:
    ///
    /// 1. **The compaction lease**, so there is exactly one certifier per prefix
    ///    and it is serialized against expiry and compaction on the same
    ///    segments.
    /// 2. **A forced full listing**, so "not listed" is not a state this pass can
    ///    be in. On a strongly-consistent store the listing sees every completed
    ///    segment PUT, so a peer's segment is either present — and folds into the
    ///    tail, closing the gap — or it does not exist.
    /// 3. **The seq-floor fence.** `leaseless_base` folds the persisted floor
    ///    unconditionally (#287, #316), so a concurrent producer's next segment
    ///    starts at or above the floor — outside the gap being certified. The gap
    ///    cannot be filled after the listing, only appended past.
    ///
    /// The tail comes from [`Self::valid_substream_segments`], which is the same
    /// view the fetch path selects from. That is the point rather than an
    /// implementation detail: the certification says "this range is not
    /// servable", and deriving it from the predicate that decides what *is*
    /// servable is what makes `OFFSET_OUT_OF_RANGE` an honest answer rather than
    /// a guess about the bucket.
    ///
    /// A sub-stream with no segments at all is skipped: that is a drained
    /// partition, not a gap, and #299 already reports it as a log starting where
    /// it ends.
    ///
    /// Writing through the watermark CAS keeps #343's mixed-fleet guard: the pair
    /// is honored only while `at_high == high`, so a floor moved by anything that
    /// did not re-certify invalidates it rather than misleading.
    ///
    /// ## Cost
    ///
    /// Once per prefix per process ([`Self::served_end_reconciled`]) — a forced
    /// listing plus one conditional watermark GET per sub-stream, which answers
    /// 304 while the watermark is unchanged. Right as a one-shot after a deploy,
    /// wasteful every tick. A restart re-arms it, which is the correct default:
    /// a fresh process is exactly when a prefix may have picked up a gap under a
    /// binary that did not certify.
    async fn certify_prefix_served_ends(&self, prefix: &str) -> Result<u64> {
        if self
            .served_end_reconciled
            .lock()
            .map_err(Into::<Error>::into)?
            .contains(prefix)
        {
            return Ok(0);
        }

        if self.acquire_compaction_lease(prefix).await.is_err() {
            debug!(
                prefix,
                "yielding served-end reconciliation to the lease holder"
            );
            return Ok(0);
        }

        // Marked before the work, not after: a prefix whose reconciliation fails
        // must not retry every tick for the life of the process. The next restart
        // re-arms it, and #338's counter still reports the state meanwhile.
        _ = self
            .served_end_reconciled
            .lock()
            .map_err(Into::<Error>::into)?
            .insert(prefix.to_owned());

        self.refresh_prefix_index_forced(prefix).await?;

        let substreams: BTreeSet<(String, i32)> = self
            .prefix_index
            .lock()
            .map_err(Into::<Error>::into)?
            .get(prefix)
            .map(|index| {
                index
                    .segments
                    .values()
                    .flat_map(|cached| {
                        cached
                            .footer
                            .entries
                            .iter()
                            .map(|entry| (entry.topic.clone(), entry.partition))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut certified: Vec<Topition> = Vec::new();

        for (topic, partition) in &substreams {
            let Some(tail) = self
                .valid_substream_segments(prefix, topic, *partition)?
                .last()
                .map(|(_, entry)| entry.base_offset + entry.record_count)
            else {
                continue;
            };

            let tp = Topition::new(topic.clone(), *partition);

            // Read before deciding, for two reasons. `with_mut` starts from the
            // in-process cached version, which on a cold replica — the one that
            // most needs this pass — is empty, so a closure that inspected
            // `watermark.high` there would see 0 and conclude there is no gap.
            // And a prefix that is already correct pays a conditional GET that
            // answers 304, rather than a PUT per sub-stream per restart.
            let Ok((high, served)) = self
                .watermark(&tp)?
                .with(&self.object_store, |watermark| {
                    Ok((watermark.high.unwrap_or(0), watermark.served))
                })
                .await
                .inspect_err(|err| debug!(?err, ?tp))
            else {
                continue;
            };

            if high <= tail
                || served.is_some_and(|served| served.certifies(high) && served.end == tail)
            {
                continue;
            }

            let wrote = self
                .watermark(&tp)?
                .with_mut(&self.object_store, |watermark| {
                    // Re-checked under the CAS: a floor raised between the read
                    // above and this write means a peer is producing, and the
                    // `end` computed from the old listing no longer pairs with
                    // it. Leaving it alone loses nothing — the next process to
                    // run this pass re-derives both together.
                    if watermark.high.unwrap_or(0) != high {
                        return Ok(false);
                    }

                    watermark.served = Some(ServedEnd {
                        end: tail,
                        at_high: high,
                    });

                    Ok(true)
                })
                .await
                .inspect_err(|err| debug!(?err, ?tp))
                .unwrap_or(false);

            if wrote {
                warn!(
                    prefix,
                    topic,
                    partition,
                    end = tail,
                    at_high = high,
                    "certified an offset gap dead: the advertised end sits above every \
                     segment present, so a consumer parked in the gap was reading empty \
                     forever and can now be told (#290)"
                );

                certified.push(tp);
            }
        }

        if certified.is_empty() {
            return Ok(0);
        }

        SERVED_END_CERTIFIED.add(
            certified.len() as u64,
            &[KeyValue::new("prefix", prefix.to_owned())],
        );

        // Both caches, and only the sub-streams actually certified — the same
        // invalidation `expire_prefix_segments` does after its own CAS, and for
        // the same reason. The next-offset hint has to go too, not just the
        // watermark floor: `high_watermark` is answered from the hint, so leaving
        // it would keep the coalesced watermark cache cold, and `certified_dead_gap`
        // reads exclusively from that cache (it pays no object request). The gap
        // would stay unanswerable on this replica until the hint aged out. Peers
        // converge on their own conditional GET.
        self.next_offsets.lock().map(|mut locked| {
            for tp in &certified {
                _ = locked.remove(tp);
            }
        })?;
        self.coalesced_watermark_floors.lock().map(|mut locked| {
            for tp in &certified {
                _ = locked.remove(tp);
            }
        })?;

        Ok(certified.len() as u64)
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
                    sleep(backoff).await;
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

    /// Truncate `topition` below `before` by persisting the offset as the
    /// sub-stream's durable truncation floor (`watermark.truncate`, #176).
    /// Nothing is deleted physically: records live in segments, which are
    /// shared across sub-streams and immutable, so they are hidden at read
    /// time by the floor and reclaimed by [`Self::expire_prefix_segments`]
    /// once every sub-stream in a segment is past its floor. (Until #179 this
    /// also removed the per-partition legacy `records/` objects; that layout
    /// is gone.) `before` of `-1` means the log end offset — truncate
    /// everything.
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

        // Nothing physical is deleted here any more (#179): the per-partition
        // `records/` objects this used to remove cannot be created, and the
        // segment-resident records are hidden by the floor rather than rewritten,
        // because segments are shared across sub-streams.
        //
        // `truncate` is the whole floor (#180). This used to write `watermark.low`
        // in lockstep with it, for readers that predate the field (#176) and knew
        // only `low` — every one of those is gone, along with the legacy retention
        // that was the field's other writer. An existing object's historic `"low"`
        // is preserved rather than erased: it lands in the `rest` catch-all, so its
        // value survives — relocated after the named fields, which costs such an
        // object one rewrite the next time something else on it moves.
        //
        // The floor is max-folded so a later call with a lower offset cannot
        // regress it.
        let floor = self
            .watermark(topition)?
            .with_mut(&self.object_store, |watermark| {
                let floor = watermark
                    .truncate
                    .map_or(before, |truncate| truncate.max(before));
                watermark.truncate = Some(floor);
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

    /// Decide which of the footer index and the object is wrong when a region
    /// read comes back short of the extent the index claims (#397), and repair
    /// the index if it is the one at fault.
    ///
    /// Segments are immutable and created atomically, so a ranged GET cannot
    /// return fewer bytes than the object holds over that range. A short read
    /// therefore says the entry the read was issued from claims bytes past the
    /// end of the object — and `encode_segment_v3` measures `byte_len` from the
    /// bytes it just appended, so it cannot over-claim for the payload it built.
    /// Two things can produce the pairing, and they are distinguishable by one
    /// suffix GET:
    ///
    /// - **the index is wrong.** The reader locates a region from the in-memory
    ///   prefix index, not from the object's trailer. An entry that describes a
    ///   different payload — a rewrite's, an adopted create's — is a *cache*
    ///   fault, and the trailer is the authority. Re-index the segment from the
    ///   trailer and let the caller retry: `Ok(())`.
    /// - **the object is wrong.** The trailer says exactly what the index said,
    ///   and the object is still short of it. Nothing in the bucket can serve
    ///   these offsets, so the read is answered `CORRUPT_MESSAGE` rather than
    ///   returning part of a region and calling it the whole thing.
    ///
    /// This is why the short read is no longer served as a silently truncated
    /// region. #395 said it did not repair the regions already on the fleet; this
    /// makes the ones caused by a stale entry read whole, and makes the rest say
    /// so.
    async fn resolve_short_region(
        &self,
        prefix: &str,
        seq: u64,
        entry: &SubstreamEntry,
        location: &Path,
        encoded: &Bytes,
    ) -> Result<()> {
        let read = RegionRead {
            prefix,
            seq,
            entry,
            encoded,
        };

        let Some(footer) = self.read_segment_footer(location).await? else {
            return Err(read.short_of_extent(
                "object carries no segment trailer to resolve the region against".to_owned(),
            ));
        };

        let Some(own) = footer.get(&entry.topic, entry.partition) else {
            return Err(read.short_of_extent(format!(
                "object's own footer holds no {}-{} region",
                entry.topic, entry.partition
            )));
        };

        if own.byte_start == entry.byte_start && own.byte_len == entry.byte_len {
            return Err(read.short_of_extent(format!(
                "object's own footer claims the same {} bytes at {} and the object is short of it",
                entry.byte_len, entry.byte_start
            )));
        }

        // The index served an entry that does not belong to this object.
        self.adopt_segment_trailer(prefix, seq, entry, footer)
    }

    /// Resolve a **full-length** region that holds no whole batch against the
    /// object's own trailer (#432), the way [`Self::resolve_short_region`]
    /// already resolves a short one.
    ///
    /// #403 built that mechanism on the short-read arm alone, because the
    /// population it was aimed at *over*-stated the region: the ranged GET came
    /// back short, and `read_len < byte_len` was the tell. The discriminator run
    /// on #397 then found the other half of the same fault — an entry that
    /// **under**-states a healthy frame. The GET returns those bytes in full, so
    /// `read_len == byte_len`, the short arm never fires, and the frame decoder
    /// correctly reports that the truncated span holds no whole batch. Same
    /// index/object disagreement, opposite sign, and the arm that could ask the
    /// authority was the one that never saw it.
    ///
    /// The verdict is the same discrimination against the same authority:
    ///
    /// - **the index is wrong.** The trailer describes this object differently —
    ///   a different extent for this sub-stream, or no region for it at all.
    ///   Re-index the segment from the trailer and let the caller retry:
    ///   `Ok(())`.
    /// - **the object is wrong.** The trailer claims exactly what the index
    ///   claimed and the bytes still hold no frame — #395's husk population. The
    ///   original verdict stands and the read is answered `CORRUPT_MESSAGE`.
    ///
    /// Why this arm matters more than the short one: a `CORRUPT_MESSAGE` is
    /// retried by a Kafka client **at the same offset**, so a partition served
    /// through a wrong entry never advances. Its only exits were compaction
    /// merging the object away or retention expiring it, and the second skips
    /// every record in between. Measured on `1.0.0-alpha.4`: five partitions
    /// across four replicas re-reading one segment each at ~100/minute, against
    /// 41 285 corrupt reads on the brokers in 25.5 h.
    async fn resolve_corrupt_region(
        &self,
        prefix: &str,
        seq: u64,
        entry: &SubstreamEntry,
        location: &Path,
        corrupt: Box<CorruptRegion>,
    ) -> Result<()> {
        // No trailer is no authority: the original verdict is the only one
        // available, and it is the honest one.
        let Some(footer) = self.read_segment_footer(location).await? else {
            return Err(Error::CorruptSegment(corrupt));
        };

        // A trailer holding no region for this sub-stream at all is the loudest
        // form of the fault — the entry belongs to another object outright, which
        // is what the #397 discriminator found (40 sub-streams in the object, and
        // the index named a 41st). An entry present but at a different extent is
        // the same fault seen through a partial overlap.
        let disagrees = footer
            .get(&entry.topic, entry.partition)
            .is_none_or(|own| own.byte_start != entry.byte_start || own.byte_len != entry.byte_len);

        if !disagrees {
            return Err(Error::CorruptSegment(corrupt));
        }

        self.adopt_segment_trailer(prefix, seq, entry, footer)
    }

    /// Replace this segment's cached footer with the object's own trailer, after
    /// one of the resolvers above has established that the index entry the read
    /// was issued from does not belong to this object (#397, #432).
    ///
    /// Replaces the **whole** cached footer, not just this sub-stream's entry: if
    /// one entry came from another payload they all did, and a footer half from
    /// each would be a third thing that describes nothing.
    fn adopt_segment_trailer(
        &self,
        prefix: &str,
        seq: u64,
        entry: &SubstreamEntry,
        footer: SegmentFooter,
    ) -> Result<()> {
        // Read out before the footer is moved into the index.
        let trailer = footer.get(&entry.topic, entry.partition).map(|own| {
            (
                own.byte_start,
                own.byte_len,
                own.base_offset,
                own.record_count,
            )
        });

        // The append time is preserved from the cached segment where there is one,
        // because whole-segment retention (#61) decides expiry on it and a `0`
        // here would read as "ancient" and delete a live segment.
        let last_modified_ms = self
            .prefix_index
            .lock()
            .map_err(Into::<Error>::into)?
            .get(prefix)
            .and_then(|index| index.segments.get(&seq))
            .map(|cached| cached.last_modified_ms)
            .unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|since| since.as_millis() as i64)
                    .unwrap_or_default()
            });

        SEGMENT_INDEX_ENTRIES_CORRECTED.add(1, &[]);
        warn!(
            prefix,
            seq,
            topic = entry.topic,
            partition = entry.partition,
            indexed = ?(entry.byte_start, entry.byte_len, entry.base_offset, entry.record_count),
            ?trailer,
            "footer index entry did not belong to this object; taking the object's own trailer"
        );

        self.index_insert(prefix, seq, footer, last_modified_ms)
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
    /// do not form a whole batch are ignored (mirroring `Batch::try_from`) — the
    /// returned [`FrameTail`] says why the scan stopped, which is what lets a
    /// caller holding the footer entry tell an ignorable tail from a region that
    /// never began at a frame (#386).
    ///
    /// Both malformed-length cases now stop the scan and report, where a negative
    /// length used to `?` out as a bare `TryFromIntError`: the guard was
    /// asymmetric, and the arm that raised was the one carrying no diagnostic.
    /// Reading a length is not the place to decide severity — a length can be
    /// unusable because the region is a truncated read (benign) or because it
    /// starts in the wrong place (damage), and only [`Self::decode_region`] can
    /// see which.
    fn decode_frame(&self, encoded: Bytes) -> Result<(Vec<deflated::Batch>, FrameTail)> {
        // base_offset (i64) + batch_length (i32) precede the `batch_length` body.
        const PREFIX: usize = size_of::<i64>() + size_of::<i32>();

        let mut batches = Vec::new();
        let mut remaining = encoded;
        let mut at = 0usize;

        let tail = loop {
            if remaining.len() < PREFIX {
                break if remaining.is_empty() {
                    FrameTail::Exhausted
                } else {
                    FrameTail::Short {
                        at,
                        remaining: remaining.len(),
                    }
                };
            }

            let mut length = [0u8; size_of::<i32>()];
            length.copy_from_slice(&remaining[size_of::<i64>()..PREFIX]);
            let declared = i32::from_be_bytes(length);

            // A `batch_length` is a byte count: negative is not a short frame, it
            // is not a frame. Same outcome as one that overruns what is left.
            let Ok(batch_length) = usize::try_from(declared) else {
                break FrameTail::Malformed { at, declared };
            };

            let total = PREFIX + batch_length;
            if total > remaining.len() {
                break FrameTail::Malformed { at, declared };
            }

            batches.push(self.decode(remaining.slice(0..total))?);
            remaining = remaining.slice(total..);
            at += total;
        };

        Ok((batches, tail))
    }

    /// Decode the bytes read for footer `entry` into that sub-stream's batches,
    /// attributing damage to the segment it came from (#386).
    ///
    /// [`Self::decode_frame`] knows only bytes, so on its own it cannot tell the
    /// documented ignorable tail from a region that is not where the footer says
    /// it is. Here the entry is in hand, and the discriminator is whether the read
    /// returned the whole extent the footer claims:
    ///
    /// - **short read** — the entry claims bytes the object does not hold.
    ///   Answered as damage, counted ([`SEGMENT_REGION_TRUNCATED`]).
    /// - **full-length read that yields no batch** — the region arrived whole and
    ///   still holds no frame, so `byte_start` does not point at one: the footer
    ///   and the payload disagree. That is damage, and it is answered as such.
    ///
    /// The short read used to return the batches it managed to decode and drop the
    /// rest of the region, on the reasoning that it was a torn or partially
    /// visible object and should stay the bounded empty read #290 settled on. The
    /// fleet then produced 313 of them in 29 minutes on
    /// `*.connect.ibmi-offsets` — `KafkaOffsetBackingStore`'s durable state —
    /// with `read_len` pinned at exactly 199 across 216 distinct sequences (#397).
    /// A constant over-claim across hundreds of objects is not tearing, and the
    /// consumer of a partial offsets region is a connector resuming from an offset
    /// map with holes in it, with nothing in its own logs to say so. Nor is
    /// tearing something an immutable, atomically created object can do: a short
    /// read means the entry and the object disagree, full stop. The reader
    /// resolves *which* of them is wrong against the object's own trailer before
    /// this is reached — see [`Self::resolve_short_region`].
    ///
    /// Beyond that, only a region that decodes to *nothing* is treated as corrupt.
    /// A malformed tail after whole batches keeps the frame contract's behaviour —
    /// it cannot be a divergent start, and erroring there would fail reads that
    /// serve data today.
    fn decode_region(
        &self,
        prefix: &str,
        seq: u64,
        entry: &SubstreamEntry,
        encoded: Bytes,
    ) -> Result<Vec<deflated::Batch>> {
        let read = RegionRead {
            prefix,
            seq,
            entry,
            encoded: &encoded,
        };

        // A frame header that parsed over a body that will not decode is the same
        // damage seen one layer down, and it reached the client as a bare protocol
        // error naming no segment. Attribute it here too.
        let (batches, tail) = self
            .decode_frame(encoded.clone())
            .map_err(|error| read.corrupt(0, None, format!("undecodable batch: {error:?}")))?;

        if read.truncated() {
            return Err(read.short_of_extent(format!(
                "region short of its footer extent by {} bytes, {} whole batches: {tail:?}",
                entry.byte_len - encoded.len() as u64,
                batches.len(),
            )));
        }

        match tail {
            FrameTail::Exhausted => Ok(batches),

            _ if batches.is_empty() => Err(read.corrupt(
                tail.at(),
                tail.declared(),
                format!("region holds no whole batch: {tail:?}"),
            )),

            // The documented ignore: bytes past the last whole batch.
            _ => {
                debug!(
                    prefix,
                    seq,
                    topic = entry.topic,
                    partition = entry.partition,
                    batches = batches.len(),
                    ?tail,
                    "ignoring trailing bytes of a segment region"
                );

                Ok(batches)
            }
        }
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

            for (index, batch) in batches.iter().enumerate() {
                // A footer entry must not be able to claim bytes the payload does
                // not hold (#393).
                //
                // `byte_len` below is measured from `body`, so it always covers
                // exactly what was written — which means the only way the two can
                // disagree is a batch whose own `batch_length` header lies about
                // its bytes. `From<Batch> for Bytes` writes that field verbatim,
                // so such a batch serialises a frame declaring a length no
                // reader will find, and the region becomes permanently
                // undecodable: the exact damage #386 had to answer for on the
                // read side.
                //
                // The one shape known to produce it is the pre-v2 husk the
                // decoder returns for `magic != 2` — the wire's `batch_length`
                // over an empty `record_data` (see `declares_its_own_length`).
                // The produce path already refuses that with
                // `UNSUPPORTED_FOR_MESSAGE_FORMAT` (#320), so reaching here is a
                // defect rather than a client error, and this is the invariant
                // asserted where the footer is built rather than where it is
                // read.
                //
                // Refusing costs the caller its tick or its flush, having
                // written nothing — the same trade #388 made for compaction.
                if !batch.declares_its_own_length() {
                    let divergent = DivergentBatch {
                        topic: topition.topic().to_owned(),
                        partition: topition.partition(),
                        base_offset: *base_offset,
                        index,
                        declared: batch.batch_length,
                        encoded: batch.encoded_batch_length().unwrap_or(-1),
                        magic: batch.magic,
                        record_data_len: batch.record_data.len(),
                    };

                    error!(
                        ?divergent,
                        "refusing to encode a batch that misdeclares its length"
                    );

                    return Err(Error::DivergentBatch(Box::new(divergent)));
                }

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
    pub(crate) fn decode_segment_footer(tail: &[u8]) -> Result<Option<SegmentFooter>> {
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
                // The re-read hands the caller the winner's value so the retry
                // can re-apply onto it. It can also find nothing: the object was
                // deleted between the failed CAS and this read — a member the
                // session sweep reaped, an assignment
                // `delete_group_assignments_before` swept — and *that is an
                // answer*, not a fault (#431).
                //
                // It used to be `?`, so the 404 propagated as a raw
                // `ObjectStore` error: not `Outdated`, so no retry loop absorbed
                // it, and `Severity::Failure` at the boundary, so the connection
                // ended with **no response written**. A Kafka client cannot
                // retry an error code it never received; it reconnects and
                // replays, which is the #219 wedge shape. Measured at ~17/h
                // across five of ten replicas on `1.0.0-alpha.4`, every one of
                // them a `JoinGroup`/`Heartbeat`-class call.
                let Some((current, version)) = Self::absent_is_none(self.get(location).await)
                    .inspect_err(|error| error!(%location, ?error))?
                else {
                    CONDITIONAL_PUT_VANISHED.add(1, &[KeyValue::new("class", key_class(location))]);
                    debug!(%location, ?value, "lost the CAS to an object that was then deleted");

                    return Err(UpdateError::Vanished);
                };

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
    async fn create_topic(&self, mut topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        let id = Uuid::now_v7();
        debug!(%id);

        // The single choke point for the broker-level config defaults (#225).
        // Every creation path lands here — `CreateTopics`, auto-create, and
        // anything added later — so a topic's stored config cannot depend on which
        // API materialised it. It used to be applied in the `CreateTopics` service
        // only, and auto-create, which builds its own `CreatableTopic`, stored no
        // config at all: invisible in `DescribeConfigs`, and expiring on Kafka's
        // absent-policy fallback instead of the configured default. Injection is
        // idempotent and never overwrites a value the caller supplied.
        self.topic_defaults
            .apply(topic.configs.get_or_insert_with(Vec::new));

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

        // Pin the routing prefix from the config this topic is created with
        // (#236), so it is never derived from `cleanup.policy` again. Reached only
        // by the creator that won the metadata CAS above, so an overwriting PUT is
        // uncontended here — and it is deliberately an overwrite rather than a
        // create: it clears any pin left behind by a torn delete of a same-named
        // predecessor, which a create-only write would silently adopt.
        let pinned = self.routed_prefix(
            &Topition::new(topic.name.as_str(), 0),
            Self::topic_configs_are_compacted(&topic),
        );
        _ = self
            .object_store
            .put_opts(
                &self.topic_routing_path(topic.name.as_str()),
                serde_json::to_vec(&TopicRouting {
                    prefix: pinned.clone(),
                })
                .map(Bytes::from)
                .map(PutPayload::from)?,
                PutOptions::default(),
            )
            .await?;

        _ = self
            .routing_prefixes
            .lock()
            .map(|mut locked| locked.insert(topic.name.clone(), pinned));

        for partition in 0..topic.num_partitions {
            let topition = Topition::new(topic.name.as_str(), partition);

            let watermark = self.watermarks.lock().map(|mut locked| {
                locked
                    .entry(topition.to_owned())
                    .or_insert(OptiCon::<Watermark>::new(self.cluster.as_str(), &topition))
                    .to_owned()
            })?;

            // Drop any stale next-offset hint (e.g. a topic of the same
            // name was previously deleted) so the fresh, empty partition
            // re-derives its offsets from listing. The cached watermark floor
            // must go with it: the prefix's seq floor is unrelated to topic
            // lifecycle, so it alone would never invalidate a floor cached
            // for the deleted incarnation.
            //
            // The truncation-floor memo (#176) goes too, but for the opposite
            // reason since #246: not to forget the predecessor's floor but to
            // re-read it from the watermark object, which now carries the
            // deleted log end rather than a dead incarnation's stale value.
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

            // Preserve `truncate` (#246).
            //
            // `delete_topic` removes a topic's own objects but cannot remove its
            // slices inside SHARED segments: a segment multiplexes many topics,
            // is immutable, and is reclaimed whole only once every sub-stream in
            // it is past retention (#61). Slices are located by `(topic,
            // partition)` NAME in the footer, so a topic created afterwards with
            // the same name — by an operator, or by auto-create on the next
            // metadata request — found its predecessor's slices, folded its
            // offsets from them, and served those records as its own. A
            // `DeleteTopics` that reads as "the data is gone" left it readable
            // through a same-named successor, silently, for as long as a segment
            // holding a slice survived.
            //
            // This used to clear the floor outright, on the assumption that a
            // fresh partition re-derives offset 0 from listing. That holds only
            // when nothing survives, which is exactly what a shared segment
            // breaks. `delete_topic` now leaves the floor at the deleted log end,
            // so keeping it is what makes the successor start past whatever it
            // would otherwise inherit — through the machinery the read paths
            // already honour (#176), without rewriting a shared segment.
            //
            // Deliberately NOT computed here from `high_watermark`: that answers
            // the same question, but it costs a segment LIST per partition on
            // every create — including auto-create, on the metadata path — which
            // is the cost #40 and #167 exist to remove. A name that never had a
            // predecessor has no watermark object at all, so preserving the field
            // costs nothing and does nothing.
            //
            // `high` is still cleared: it re-derives from the segment fold, and
            // `expire_prefix_segments` stays its single writer (#179, #237).
            watermark
                .with_mut(&self.object_store, |watermark| {
                    _ = watermark.high.take();

                    Ok(())
                })
                .await?;
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

                            // Same reasoning as the offset-commit path (#275),
                            // lower stakes: an admin call rather than a client
                            // hot loop, but a transient storage error is still
                            // worth a retry rather than a fatal answer.
                            DeleteRecordsPartitionResult::default()
                                .partition_index(partition.partition_index)
                                .low_watermark(0)
                                .error_code(storage_error_code(&err).into())
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
            // Data BEFORE metadata (#251). The metadata object is the only handle
            // on this topic's data: maintenance discovers work by listing
            // `topic-metadata/`, so a topic that no longer has one is never
            // revisited by anything. Removing it first meant that any failure
            // in the deletions below — an error, a throttle, a pod restart, a
            // client timeout — stranded whatever had not been reached, for good.
            // A production audit found 878,065 such objects under two deleted
            // topics, paid for indefinitely and skewing every audit of the
            // layout.
            //
            // Ordered this way, a partial delete instead leaves a topic that
            // still exists with some of its data gone: visible, and recoverable
            // by re-issuing `DeleteTopics`. The cost is a wider window in which
            // a topic being deleted is still served, which for an admin
            // operation is the better trade.
            //
            // This does not reopen the offset-reuse hazard of #241: nothing
            // below removes an authority on the log end. The watermark object is
            // rewritten as a truncation tombstone rather than deleted (#246,
            // below), and `coalesced_high_from_index` folds the segment tail
            // over it regardless — so a produce landing inside the widened
            // window still computes the same high watermark from the segments
            // that are still there.
            // Truncate every partition to its log end (#246) rather than
            // deleting its `watermark.json`.
            //
            // The slices this topic left inside SHARED segments cannot be
            // removed — a segment multiplexes many topics, is immutable, and is
            // reclaimed whole only once every sub-stream in it is past retention
            // (#61) — and they are located by `(topic, partition)` NAME, so a
            // same-named successor would find them and serve them as its own.
            // The floor the truncation machinery already maintains
            // (`watermark.truncate`, #176) hides them at read time without
            // rewriting a shared segment, and is as durable as the data it
            // hides. So the watermark object is not deleted: it BECOMES the
            // tombstone, and `create_topic` preserves it.
            //
            // Only `truncate` is written, never `high`: #179 restored
            // `expire_prefix_segments` as that field's single writer, which is
            // what keeps the floor-certified watermark cache's certification
            // argument unconditional (#237).
            //
            // The cost is one small object per partition of a deleted topic,
            // kept indefinitely. It cannot be dropped once the slices are
            // reclaimed without a scan costing more than the object does, and
            // dropping it early resurrects the records it hides.
            for partition in 0..metadata.topic.num_partitions {
                let topition = Topition::new(metadata.topic.name.as_str(), partition);

                _ = self.delete_records_before(&topition, -1).await?;
            }

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

            // And the same topic out of every group's one offsets object (#406).
            // A committed offset that outlives its topic is served against the
            // recreated one, which is #241's shape — 70 topics reporting a
            // committed offset above a high watermark of 0 — so the two layouts
            // have to be swept together or deleting a topic only half works.
            //
            // One delimited listing for the group ids, then a conditional GET
            // each and a write only where the topic was actually held. On an
            // admin path, against a group count, not a partition count.
            let groups = self
                .scan_delimited(
                    Scan::AdminDelete,
                    &Path::from(format!("clusters/{}/groups/consumers/", self.cluster)),
                )
                .await?;

            for group in groups.common_prefixes {
                // A common prefix has no filename, so the group id is its last
                // path component.
                let Some(group_id) = group
                    .parts()
                    .next_back()
                    .map(|part| part.as_ref().to_owned())
                else {
                    continue;
                };

                let Some(offsets) = self.group_offsets(&group_id)? else {
                    continue;
                };

                let held = offsets
                    .get_opt(&self.object_store)
                    .await
                    .inspect_err(|error| warn!(?error, %group_id, "group offsets"))
                    .unwrap_or_default();

                if !held.is_some_and(|group| group.committed.contains_key(&metadata.topic.name)) {
                    continue;
                }

                _ = offsets
                    .with_mut(&self.object_store, |group| {
                        Ok(group.remove_topic(&metadata.topic.name))
                    })
                    .await
                    .inspect_err(|error| {
                        warn!(?error, %group_id, topic = %metadata.topic.name, "dropping a deleted topic's committed offsets")
                    });
            }

            // Only now that the data is gone: the metadata object, its id ->
            // name pointer, and the routing pin. Past this point the topic no
            // longer exists for the API or for maintenance, so nothing above may
            // still need doing.
            self.topic_meta(metadata.topic.name.as_str())?
                .remove(&self.object_store)
                .await?;

            self.invalidate_topic_id(&metadata.id);
            self.invalidate_routing_prefix(metadata.topic.name.as_str());
            self.invalidate_topic_index();

            // The other six per-topic caches (#283). After the tombstone write
            // above, so the watermark handle this drops is not one a later step
            // still needs.
            self.invalidate_topic_caches(metadata.topic.name.as_str());

            for path in [
                self.topic_id_path(&metadata.id),
                // The routing pin goes with the topic (#236): it is immutable for a
                // topic's lifetime, so leaving it would let a same-named successor
                // inherit a dead incarnation's routing.
                self.topic_routing_path(metadata.topic.name.as_str()),
            ] {
                match self.object_store.delete(&path).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(otherwise) => return Err(otherwise.into()),
                }
            }

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

            // Captured before `deflated` is moved into the prefix buffer; the
            // transaction registration below needs both.
            let last_offset_delta = deflated.last_offset_delta;
            let producer_epoch = deflated.producer_epoch;

            // Every batch is buffered into a prefix-coalesced segment (#57/#174):
            // a compacted topic under its own dedicated prefix (#175), everything
            // else under its connector prefix. There is no second write path, and
            // with #178 there is no longer any code that can form a legacy
            // `records/` key — so the #78 dual-offset-authority class is
            // impossible by construction rather than prevented by a routing
            // invariant. That covers transactional and control batches (#174
            // release B; footer v3 indexes them, `meta.json` stays the commit
            // authority) and bulk backfill (#90), which trips the raised byte
            // threshold and flushes as ~its own segment, keeping the 1-PUT parity
            // the old #62 bypass gave.
            //
            // Idempotent dedup belongs to the segment flush, which folds the log's
            // producer coordinates into a `ProducerTable` (#88) — a
            // cross-pod-convergent authority that cannot advance before the batch
            // is durable. The per-pod `producers/{id}.json` gate it replaced
            // diverged across a connection migration (#79) and mishandled i32
            // sequence wraparound (#80); with the legacy path gone, nothing
            // reaches it and it goes too.
            //
            // Deliberately NOT an early return: a transactional produce that
            // skipped the registration below would leave its range out of
            // `meta.transactions`, `txn_end` would find nothing to mark, and a
            // read-committed consumer would read aborted data as committed (the
            // #81 bug class).
            let offset = self.enqueue_prefix_coalesced(topition, deflated).await?;

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
            // Records live in shared segments, located by footer index and read
            // with a ranged GET of exactly the topition's byte span — no
            // cross-topic download. That is the only representation a read path
            // serves (#179): the legacy `records/` seam this used to stitch across
            // is gone, so there is no hybrid branch and no `[0, C)` region to
            // serve first.
            //
            // Nothing below the truncation floor is served (#176): truncated
            // records survive physically in shared segments, so the floor is
            // enforced by clamping the requested offset — skip, not error, and
            // batch-granular by construction via the whole-batch skip in
            // `fetch_prefix_coalesced`. Served from in-process caches on a warm
            // poll (`truncate_floor` memoizes absence, so floor-less partitions
            // add no request, #161).
            let offset = offset.max(self.truncate_floor(topition).await?);

            batches = self
                .fetch_prefix_coalesced(
                    topition,
                    offset,
                    max_bytes,
                    high_watermark,
                    started_at,
                    max_wait,
                )
                .await?;

            // An empty log cannot serve an offset below its end, and saying so is
            // what lets the consumer recover on its own (#337).
            //
            // The state: retention removed every segment, so `log_start` is
            // `high_watermark` (#299) and a group whose committed offset predates
            // that start asks for offsets no segment will ever hold. Answering
            // empty reads to a consumer as "caught up, nothing new", so it polls
            // again, forever — and because `poll()` covers the whole assignment, one
            // such partition stops delivery on every healthy partition sharing it.
            // Production: 77 stranded partitions, 15 of 16 members holding at least
            // one, zero records delivered for days.
            //
            // `OFFSET_OUT_OF_RANGE` is Kafka's defined answer here, and it is
            // load-bearing rather than cosmetic: `auto.offset.reset` then moves the
            // consumer to a live position with no operator action, and `none` fails
            // loudly, which is also correct.
            //
            // Why "no segment at all" and not "below `log_start`":
            //
            // - with segments present, an offset below their base is already served
            //   the records *above* it, so the consumer advances and nothing wedges;
            // - the truncation floor is deliberately a skip and not an error (#176),
            //   and a production fix is not the place to reverse that.
            //
            // Why this is not #292's detector, which reset live consumers on 61
            // topics and was removed in #314: that condition was *an index entry
            // claiming the offset over a read that produced none of it* — a
            // heuristic a stale index could forge. This one is the absence of any
            // segment, which is the definition of an empty log and is already what
            // the broker advertises through `log_start == log_end`. Answering
            // consistently with what ListOffsets already claims adds no new way to
            // be wrong.
            //
            // Cost: nothing when records are served. The index read happens only on
            // a fetch that already came back empty, and it is an index read — no
            // LIST, no confirming re-read.
            if batches.is_empty() && self.segment_region_start(topition).await?.is_none() {
                debug!(
                    ?topition,
                    offset,
                    high_watermark,
                    "no segment holds this offset and the log is empty; \
                     answering OFFSET_OUT_OF_RANGE (#337)"
                );

                return Err(Error::Api(ErrorCode::OffsetOutOfRange));
            }

            // The mid-log sibling of the check above (#290): the log is not
            // empty — segments below the offset are still served — but the
            // offsets from the surviving tail up to the floor were destroyed by
            // a segment expiry, and that expiry certified so in the watermark.
            // Without the certification this state is indistinguishable from a
            // peer having acked offsets this process never listed (where empty
            // is the right answer and an error would reset live consumers — the
            // #292/#314 lesson), so only the certified case errors. A consumer
            // parked here polls empty forever otherwise: `auto.offset.reset`
            // then moves it to a live position, and `none` fails loudly, which
            // is also correct.
            if batches.is_empty()
                && let Some(served) = self.certified_dead_gap(topition).await?
                && served.gap_contains(offset)
            {
                info!(
                    ?topition,
                    offset,
                    end = served.end,
                    at_high = served.at_high,
                    "offset is in a gap certified dead by segment expiry; \
                     answering OFFSET_OUT_OF_RANGE (#290)"
                );

                return Err(Error::Api(ErrorCode::OffsetOutOfRange));
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
        // in-memory hint (#40), and the log start from the oldest surviving
        // segment (#179), clamped to the cached truncation floor (#176). The log
        // start used to come from the cached `watermark.low`, which only the legacy
        // retention paths ever advanced — authoritatively silent for a
        // pure-segment sub-stream, and usually absent (#161). The segment index is
        // the authority instead, and it is more accurate: nothing advances that
        // field after a segment expiry.
        //
        // Still request-free on a warm poll: the index refresh is per prefix and
        // TTL-bounded, so a caught-up consumer resolves its fetch-response offsets
        // with zero per-partition requests, off the meta-object throttle ceiling
        // entirely.
        let high_watermark = self.high_watermark(topition).await?;
        // No segment means an empty log, whose start is its end (#290) — see
        // [`Self::log_start`], which this mirrors on the read-uncommitted path.
        let log_start = self
            .segment_region_start(topition)
            .await?
            .unwrap_or(high_watermark)
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
                let stable = meta.open_transaction_floors();

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

        let log_start = self.log_start(topition, high_watermark).await?;
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
                .with(
                    &self.object_store,
                    |meta| Ok(meta.open_transaction_floors()),
                )
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
        // One index refresh for the whole commit (#387). The existence test below
        // is per *partition*, purely to decide whether to write the offset, so a
        // group committing 1,500 topics × 16 partitions asked the object store
        // 24,000 times for answers the index already holds — the same topic over
        // and over. Served from the index it costs nothing, and only a topic the
        // index does not hold reads its own object.
        self.refresh_index_for_described_reads("offset_commit")
            .await;

        // Resolve which topitions exist, concurrently and off the topics index
        // (#387/#154): a group committing 1,500 topics × 16 partitions asks about
        // the same topic 16 times, and the index answers all of it for free.
        //
        // `try_collect` keeps the fail-fast semantics of the `?` (a metadata read
        // error aborts the commit) and `buffered` preserves response order, which
        // the response the client gets is keyed on.
        const OFFSET_COMMIT_CONCURRENCY: usize = 32;

        // Eagerly collected to pin lifetimes under `async_trait` (see #147).
        let resolutions = offsets
            .iter()
            .map(|(topition, offset_commit)| async move {
                let known = self
                    .described_topic_metadata(&TopicId::from(topition), "offset_commit")
                    .await?
                    .is_some();

                Ok::<_, Error>((topition.to_owned(), offset_commit.clone(), known))
            })
            .collect::<Vec<_>>();

        let resolved = futures::stream::iter(resolutions)
            .buffered(OFFSET_COMMIT_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;

        // One conditional write for the whole request (#406, #111): the layout
        // was one unconditional overwrite per partition, so a commit over `t`
        // partitions cost `t` billed PUTs. Consumer-group writes are 67% of the
        // fleet's PUT plane, and #111's acceptance — "a commit over `t` topitions
        // issues O(1) PUTs, not `1 + t`" — was never met.
        //
        // The CAS matters beyond the count: two replicas committing for the same
        // group used to race per object with last-write-wins per partition, so a
        // commit could interleave with another and leave a group's offsets from
        // two different requests. `with_mut` re-applies the fold on conflict, so
        // the losing writer folds onto the winner's value instead of over it.
        let committable: Vec<(Topition, OffsetCommitRequest)> = resolved
            .iter()
            .filter(|(_, _, known)| *known)
            .map(|(topition, commit, _)| (topition.clone(), commit.clone()))
            .collect();

        let stored = if committable.is_empty() {
            Ok(())
        } else {
            match self.group_offsets(group_id)? {
                Some(offsets) => {
                    offsets
                        .with_mut(&self.object_store, |group| {
                            for (topition, commit) in &committable {
                                group.insert(topition, commit.clone());
                            }

                            Ok(())
                        })
                        .await
                }

                // The widening group id `group_prefix` refuses (#277): there is no
                // object to write, and writing the root would be the bug that
                // issue exists for.
                None => Err(Error::Api(ErrorCode::GroupIdNotFound)),
            }
        };

        // #275: a failure here is an object-store failure, and they are
        // overwhelmingly transient — a 503 SlowDown, a timeout, a 5xx.
        // `UnknownServerError` is non-retriable in Kafka clients, so `commitSync`
        // threw rather than retrying and a connector treating that as engine death
        // restarted: a throttle burst turned into connector restarts. Answer the
        // way produce already does.
        //
        // One write now covers every partition in the request, so the code it
        // fails with covers them all too — which is what a client committing a
        // batch already assumes when it retries the batch.
        let commit_code = match stored {
            Ok(()) => ErrorCode::None,
            Err(error) => {
                error!(?error, group_id, partitions = committable.len());
                storage_error_code(&error)
            }
        };

        Ok(resolved
            .into_iter()
            .map(|(topition, _, known)| {
                let error_code = if known {
                    commit_code
                } else {
                    ErrorCode::UnknownTopicOrPartition
                };

                (topition, error_code)
            })
            .collect())
    }

    async fn committed_offset_topitions(&self, group_id: &str) -> Result<BTreeMap<Topition, i64>> {
        // The union of what the group's one offsets object holds and what the
        // per-partition layout left behind (#406). The set has to be a union, not
        // a preference: a group whose commits only ever landed in the new object
        // has nothing under the `offsets/` prefix to list, and a group that has
        // not committed since the upgrade has nothing in the new object — an
        // `OffsetFetch(all)` that took either alone would answer empty for one of
        // them, which is a consumer resuming from nothing.
        let mut topitions: Vec<Topition> = match self.group_offsets(group_id)? {
            Some(offsets) => offsets
                .get_opt(&self.object_store)
                .await
                .inspect_err(|error| warn!(?error, group_id, "group offsets"))
                .unwrap_or_default()
                .map(|group| group.topitions().collect())
                .unwrap_or_default(),

            None => Vec::new(),
        };

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

                // The broker writes 10-digit zero-padded partition names, so a
                // shorter component means a foreign or truncated object in the
                // bucket rather than anything this cluster produced. Skip it:
                // slicing it panicked the request task (#276).
                let Some(partition) = meta
                    .location
                    .parts()
                    .nth(8)
                    .inspect(|partition| debug!(?partition))
                    .and_then(|partition| {
                        partition
                            .as_ref()
                            .get(0..10)
                            .map(i32::from_str)
                            .or_else(|| {
                                warn!(
                                    location = %meta.location,
                                    "skipping an offset object whose partition component is too short"
                                );
                                None
                            })
                    })
                    .transpose()?
                else {
                    continue;
                };

                debug!(topic, partition);

                topitions.push(Topition::new(topic, partition));
            }
        }

        topitions.sort();
        topitions.dedup();

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

            // The group's one offsets object first (#406): one conditional GET,
            // served from the etag memo, answers every partition it holds. What it
            // does not hold falls through to that partition's own object below —
            // which is how the layout migrates without a fold-everything pass,
            // and why a rollback still finds the pre-upgrade offsets where it
            // expects them.
            let group = match self.group_offsets(group_id)? {
                Some(offsets) => offsets
                    .get_opt(&self.object_store)
                    .await
                    .inspect_err(|error| warn!(?error, group_id, "group offsets"))
                    .unwrap_or_default()
                    .unwrap_or_default(),

                None => GroupOffsets::default(),
            };

            // Eagerly collected to pin lifetimes under `async_trait` (see #147).
            let fetches = topics
                .iter()
                .map(|topition| {
                    let held = group.get(topition).map(|commit| commit.offset);

                    async move {
                        if let Some(offset) = held {
                            return Ok::<_, Error>((topition.to_owned(), offset));
                        }

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
                    }
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
                // One refresh for the whole request (#387). Every topic below is
                // then answered from the in-memory index at zero requests, and
                // only a name the index does not hold — a topic that does not
                // exist, or one created since the snapshot — costs a read of its
                // own object.
                //
                // This is the fan-out that made `topic_metadata` the fleet's
                // largest remaining S3 cost: 2,081 lookups/s against 21.5
                // `Metadata` requests/s, ~97 per request, because consumers here
                // subscribe to hundreds of topics each. Per-topic reads spread
                // thin across ~15k topics missed their window about half the time,
                // so ~1,040/s reached S3 as a conditional GET answered `304`.
                self.refresh_index_for_described_reads("metadata").await;

                // Resolve each topic concurrently. The index lookup does not
                // await, but the fallback read does, and a serial loop over it is
                // O(topics × RTT) — on a high-latency store that blew past the
                // client's metadata/request timeout at scale, so a consumer group
                // leader resolving the union of its members' subscriptions
                // (hundreds–thousands of topics) never completed the fetch, never
                // sent SyncGroup, and the group stayed `Forming` with zero
                // partitions assigned. Bounded concurrency returns the same
                // answers in O(topics / concurrency) wall time. `buffered`
                // preserves response order; collecting `Result`s (not
                // `try_collect`) keeps the per-topic error handling below intact.
                // Same scaling fix as ListOffsets (#147), same concurrency bound.
                const METADATA_FETCH_CONCURRENCY: usize = 32;

                // Eagerly collected to pin every future to this call's lifetime
                // under `async_trait` (see the identical note on ListOffsets);
                // the futures are inert until polled by `buffered`, so this
                // allocates, it does not serialize.
                let fetches = topics
                    .iter()
                    .map(|topic| self.described_topic_metadata(topic, "metadata"))
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
                // #387 moved most of that population out of reach: a topic the
                // index holds is answered from the index, so no object read
                // happens and no spurious 404 can be mistaken for absence. What
                // remains is the fallback — a name the index did not hold — and
                // there this witness still decides, because the index can be
                // refreshed between the lookup that missed and the read that came
                // back empty (a peer's create, a maintenance sweep, another
                // request's refresh after an invalidation).
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
            // Deliberately the authoritative per-topic read, not the
            // index-served `described_topic_metadata` (#387): `topic_is_compacted`
            // reads through here to derive a routing prefix for a pre-#236 topic,
            // and that derivation is pinned create-only and permanently. A stale
            // `cleanup.policy` would route a topic's new records to a prefix its
            // existing segments are not under — unreachable data, not a stale
            // answer. Admin-rate plus a memoized derivation, so the request it
            // costs is not part of the plane #387 removed.
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

                // Not an admin-only path: `topic_is_compacted` calls this, and
                // it runs on produce and fetch via `routed_prefix_of` whenever
                // the memo misses (first use of a topic in a process, or TTL
                // expiry). So any transient object-store error while reading
                // topic metadata reached this arm, and transient storage errors
                // are routine.
                //
                // Propagate rather than panic, and rather than guessing a
                // routing verdict: `topic_is_compacted` already has a `?`, and
                // the storage error then classifies retriable (#275) instead of
                // taking the request task down (#276). Answering "not
                // compacted" on a failed read would be the other option, but
                // routing is pinned create-only (#236) and a wrong pin is
                // permanent — not a guess worth making on a blip.
                Err(err) => Err(err),
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

        // One refresh, then every topic below is described from the index (#387).
        self.refresh_index_for_described_reads("describe_topic_partitions")
            .await;

        for topic in topics.unwrap_or_default() {
            match self
                .described_topic_metadata(topic, "describe_topic_partitions")
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

    /// Every group in the cluster, optionally filtered by state.
    ///
    /// A group is whatever owns something under the consumer root: a `{group}/`
    /// common prefix — its committed offsets, its member documents and its
    /// generation — or a legacy `{group}.json` state object, which after the
    /// #359 cutover is an inert leftover that expiry reaps on its own. Both are
    /// collected, which fixes a group that has state but has never committed an
    /// offset being omitted from its own cluster's listing.
    ///
    /// `states_filter` costs a read fan-out, because that is what the filter
    /// means: the state is derived per group, and deriving it is reading the
    /// group. An **unfiltered** listing stays one delimited listing and reports
    /// `Unknown`, as it always has — a client that did not ask to filter by
    /// state should not pay for one read per group in the cluster to be told
    /// something it did not ask for.
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Vec<ListedGroup>> {
        /// As the describe fan-out.
        const LIST_STATE_CONCURRENCY: usize = 32;

        let root = self.groups_root();
        let list_result = self
            .scan_delimited(Scan::Group, &root)
            .await
            .inspect(|list_result| debug!(?list_result))
            .inspect_err(|error| error!(?error, cluster = self.cluster))?;

        let mut group_ids = BTreeSet::new();

        for prefix in list_result.common_prefixes {
            if let Some(group_id) = prefix.parts().next_back() {
                _ = group_ids.insert(group_id.as_ref().to_owned());
            }
        }

        for meta in list_result.objects {
            if let Some(group_id) = Self::group_of(&root, &meta.location) {
                _ = group_ids.insert(group_id);
            }
        }

        let listed = |group_id: String, state: String| {
            ListedGroup::default()
                .group_id(group_id)
                .protocol_type("consumer".into())
                .group_state(Some(state))
                .group_type(Some("classic".into()))
        };

        let Some(states_filter) = states_filter else {
            return Ok(group_ids
                .into_iter()
                .map(|group_id| listed(group_id, String::from("Unknown")))
                .collect());
        };

        let wanted = states_filter.iter().cloned().collect::<BTreeSet<_>>();

        Ok(
            futures::stream::iter(group_ids.into_iter().map(|group_id| async move {
                // `Unknown` covers three things a caller cannot act on
                // differently: a group that went away between the listing and
                // now, one this replica could not read, and a legacy
                // `{group}.json` left behind by the cutover, which owns a
                // listed name and nothing this binary can describe.
                let state = self
                    .group_view(&group_id)
                    .await
                    .inspect_err(|err| debug!(?err, group_id))
                    .ok()
                    .flatten();

                let state = state.as_ref().map_or_else(
                    || String::from("Unknown"),
                    |detail| ConsumerGroupState::from(detail).to_string(),
                );

                (group_id, state)
            }))
            .buffered(LIST_STATE_CONCURRENCY)
            .filter_map(|(group_id, state)| {
                let keep = wanted.contains(&state);

                async move { keep.then(|| listed(group_id, state)) }
            })
            .collect::<Vec<_>>()
            .await,
        )
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                // #277: an id contributing no path component of its own widens
                // the deletion prefix to the root of the consumer tree, and the
                // `delete_stream` below would then take every group and every
                // committed offset in the cluster with it. Refuse before any
                // path is built, and report it the way Kafka does.
                //
                // Not only reachable from a client: `expire_groups` derives its
                // ids by stripping `.json` off a listing, so a stray object
                // named exactly `.json` under the root yields an empty id from
                // the maintenance loop.
                let Some(prefix) = self.group_prefix(group_id) else {
                    warn!(
                        ?group_id,
                        cluster = self.cluster,
                        "refusing to delete a group id that resolves to the consumer tree root"
                    );

                    results.push(
                        DeletableGroupResult::default()
                            .group_id(group_id.into())
                            .error_code(ErrorCode::InvalidGroupId.into()),
                    );

                    continue;
                };

                let location = Path::from(format!(
                    "clusters/{}/groups/consumers/{}.json",
                    self.cluster, group_id,
                ));

                // The legacy state object. Deleted for as long as one can
                // exist; after the cutover this is a 404 per deleted group and
                // the prefix below is what carries the group.
                let had_legacy_state = self
                    .object_store
                    .delete(&location)
                    .await
                    .inspect(|outcome| debug!(group_id, ?outcome))
                    .inspect_err(|err| debug!(group_id, ?err))
                    .is_ok();

                debug!(group_id, had_legacy_state);

                let locations = self
                    .scan(Scan::AdminDelete, &prefix)
                    .map_ok(|m| m.location)
                    .boxed();

                // Everything the group owns under its prefix: its committed
                // offsets, and since #359 its member documents, its generation
                // and every generation's assignment. One sweep covers all of
                // them because they share the prefix — which is also what makes
                // `expire_groups` layout-agnostic.
                // Drop the memoized handle with the group (#406). `with_mut`
                // would self-heal a stale etag against a recreated group — the
                // conditional PUT fails its precondition and re-reads — but the
                // map would otherwise grow with group churn, and #45 measured
                // ~15k orphaned groups accumulating.
                _ = self
                    .group_offsets
                    .lock()
                    .map(|mut handles| handles.remove(group_id))
                    .inspect_err(|error| debug!(?error, group_id));

                let deleted = self
                    .object_store
                    .delete_stream(locations)
                    .try_collect::<Vec<Path>>()
                    .await?;

                debug!(group_id, ?deleted);

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(
                            // A group existed if anything of it did. Keyed on
                            // the legacy object alone, a group in the
                            // decomposed layout with no committed offsets — one
                            // that has joined but never committed — would be
                            // reported `GroupIdNotFound` after being deleted.
                            if had_legacy_state || !deleted.is_empty() {
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

    /// Describe each group in `group_ids` (#240).
    ///
    /// Concurrent, because this is a fan-out over per-group objects and a client
    /// asks about every group it owns in one call. Sequentially this cost one
    /// round-trip per group, serialized: at a few hundred groups that is tens of
    /// seconds, past any admin client's deadline — observed in production as
    /// `context deadline exceeded` on `group describe`, and as the
    /// `listConsumerGroupOffsets` timeouts that made a consumer's rebalance
    /// callback throw. `buffered` keeps the response in request order while
    /// letting the object store answer in parallel.
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        /// Matches the other per-object fan-outs in this file
        /// (`FETCH_EACH_CONCURRENCY`, `FOOTER_FETCH_CONCURRENCY`).
        const DESCRIBE_CONCURRENCY: usize = 32;

        let Some(group_ids) = group_ids else {
            return Ok(vec![]);
        };

        Ok(
            futures::stream::iter(group_ids.iter().cloned().map(|group_id| async move {
                match self.group_view(&group_id).await {
                    Ok(Some(group_detail)) => NamedGroupDetail::found(group_id, group_detail),

                    // No `generation.json` means the group does not exist,
                    // which is a fact and is reported as an empty group. A
                    // legacy `{group}.json` left behind by the cutover is not
                    // consulted: it describes a membership that a quiesce made
                    // vacuous, and reading it would put a 404 on the describe
                    // path of every group in the cluster, forever, to answer
                    // with something less true than "empty".
                    Ok(None) => NamedGroupDetail::found(group_id, GroupDetail::default()),

                    // Not knowing is not the same as empty, and it is
                    // retriable. Answering `GroupDetail::default()` here made a
                    // throttle or a 5xx report a live group as empty — the same
                    // shape as #214, where an unresolvable topic was reported
                    // absent and clients could not tell it from a deleted one.
                    Err(error) => {
                        error!(?error, group_id, "could not compose the group view");

                        NamedGroupDetail::error_code(
                            group_id,
                            if matches!(error, Error::ObjectStore(_)) {
                                ErrorCode::CoordinatorLoadInProgress
                            } else {
                                ErrorCode::UnknownServerError
                            },
                        )
                    }
                }
            }))
            .buffered(DESCRIBE_CONCURRENCY)
            .collect::<Vec<_>>()
            .await,
        )
    }

    async fn write_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        member: MemberDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<MemberDoc>> {
        let location = self
            .group_member_location(group_id, member_id)
            .ok_or_else(|| UpdateError::Error(Error::Api(ErrorCode::InvalidGroupId)))?;

        self.put(
            &location,
            member,
            json_content_type(),
            version.map(Into::into),
        )
        .await
        .map(Into::into)
    }

    async fn read_group_member(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> Result<Option<(MemberDoc, Version)>> {
        let Some(location) = self.group_member_location(group_id, member_id) else {
            return Ok(None);
        };

        Self::absent_is_none(self.get::<MemberDoc>(&location).await)
    }

    async fn delete_group_member(&self, group_id: &str, member_id: &str) -> Result<()> {
        let Some(location) = self.group_member_location(group_id, member_id) else {
            return Ok(());
        };

        match self.object_store.delete(&location).await {
            Ok(()) => Ok(()),
            // Already gone is the outcome asked for. The caller's contract is
            // best-effort anyway, and a member document is deleted from more
            // than one path (its own leave, and the sweep that evicted it).
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn list_group_members(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<String, (MemberDoc, Version)>> {
        let Some(prefix) = self.group_members_prefix(group_id) else {
            return Ok(BTreeMap::new());
        };

        let mut members = BTreeMap::new();
        let mut listing = self.scan(Scan::Group, &prefix);

        while let Some(meta) = listing.next().await.transpose()? {
            let Some(member_id) = meta
                .location
                .parts()
                .next_back()
                .and_then(|name| name.as_ref().strip_suffix(".json").map(ToOwned::to_owned))
            else {
                continue;
            };

            // A document deleted between the listing and the read is a member
            // that left, not a failure of the listing.
            if let Some(pair) = Self::absent_is_none(self.get::<MemberDoc>(&meta.location).await)? {
                _ = members.insert(member_id, pair);
            }
        }

        Ok(members)
    }

    async fn read_group_generation(
        &self,
        group_id: &str,
    ) -> Result<Option<(GenerationDoc, Version)>> {
        let Some(location) = self.group_generation_location(group_id) else {
            return Ok(None);
        };

        Self::absent_is_none(self.get::<GenerationDoc>(&location).await)
    }

    async fn update_group_generation(
        &self,
        group_id: &str,
        generation: GenerationDoc,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GenerationDoc>> {
        let location = self
            .group_generation_location(group_id)
            .ok_or_else(|| UpdateError::Error(Error::Api(ErrorCode::InvalidGroupId)))?;

        self.put(
            &location,
            generation,
            json_content_type(),
            version.map(Into::into),
        )
        .await
        .map(Into::into)
    }

    async fn create_group_assignment(
        &self,
        group_id: &str,
        generation_id: i32,
        assignment: AssignmentDoc,
    ) -> Result<AssignmentOutcome> {
        let location = self
            .group_assignment_location(group_id, generation_id)
            .ok_or(Error::Api(ErrorCode::InvalidGroupId))?;

        // `None` is `PutMode::Create`, so this races on the key rather than on
        // an etag: exactly one writer creates it, and the loser is handed what
        // is stored.
        match self
            .put(&location, assignment, json_content_type(), None)
            .await
        {
            Ok(put_result) => Ok(AssignmentOutcome::Created(put_result.into())),

            Err(UpdateError::Outdated { current, .. }) => {
                Ok(AssignmentOutcome::AlreadyExists(current))
            }

            // The winner's assignment was deleted before it could be read back,
            // and the only thing that deletes one is
            // `delete_group_assignments_before` — so the group has already moved
            // past this generation and there is nothing here to adopt (#431).
            //
            // That is the same situation the caller's own post-create fence
            // answers, so give it the same retriable code rather than a value it
            // would have to invent: `RebalanceInProgress` is `Severity::Expected`
            // at the boundary, so the client is *told* to re-join instead of
            // having its connection dropped.
            Err(UpdateError::Vanished) => {
                debug!(
                    group_id,
                    generation_id,
                    sync_outcome = ?ErrorCode::RebalanceInProgress,
                    "the assignment this create lost to was swept",
                );

                Err(Error::Api(ErrorCode::RebalanceInProgress))
            }

            Err(UpdateError::Error(error)) => Err(error),
            Err(UpdateError::SerdeJson(error)) => Err(Error::SerdeJson(error)),
            Err(UpdateError::Uuid(error)) => Err(Error::Uuid(error)),
            // `put` never raises it — nothing here reads an etag back — but the
            // variant is part of the type, and a silent `unreachable!()` in a
            // storage path is worse than a named error.
            Err(UpdateError::MissingEtag) => Err(Error::Message(String::from(
                "assignment create reported a missing etag",
            ))),
        }
    }

    async fn read_group_assignment(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Result<Option<AssignmentDoc>> {
        let Some(location) = self.group_assignment_location(group_id, generation_id) else {
            return Ok(None);
        };

        Ok(Self::absent_is_none(self.get::<AssignmentDoc>(&location).await)?.map(|(doc, _)| doc))
    }

    async fn delete_group_assignments_before(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Result<u64> {
        let Some(prefix) = self.group_assignments_prefix(group_id) else {
            return Ok(0);
        };

        // The names are zero-padded, so the listing is in generation order and
        // `start_after` would do — but a generation that overflowed into a
        // wider name, or an object written by a future layout, must not stop
        // the sweep. Decoding the name and comparing is cheap at one object per
        // rebalance.
        let mut condemned = vec![];
        let mut listing = self.scan(Scan::Group, &prefix);

        while let Some(meta) = listing.next().await.transpose()? {
            let below = meta
                .location
                .parts()
                .next_back()
                .and_then(|name| {
                    name.as_ref()
                        .strip_suffix(".json")
                        .and_then(|stem| stem.parse::<i32>().ok())
                })
                .is_some_and(|generation| generation < generation_id);

            if below {
                condemned.push(meta.location);
            }
        }

        let deleted = self
            .object_store
            .delete_stream(futures::stream::iter(condemned.into_iter().map(Ok)).boxed())
            .try_collect::<Vec<Path>>()
            .await?;

        debug!(group_id, generation_id, ?deleted);

        Ok(deleted.len() as u64)
    }

    async fn create_acls(&self, bindings: &[AclBinding]) -> Result<Vec<ErrorCode>> {
        // Every creation reports `None`: a rule that is already there is not a
        // failure. `kafka-acls.sh` is run from configuration management, so
        // re-applying the same file must not start reporting errors on the
        // second run.
        let outcome = vec![ErrorCode::None; bindings.len()];

        if bindings.is_empty() {
            return Ok(outcome);
        }

        self.update_acls(|acls| {
            for binding in bindings {
                _ = acls.bindings.insert(binding.clone());
            }
        })
        .await
        .map(|()| outcome)
    }

    async fn describe_acls(&self, filter: &AclFilter) -> Result<Vec<AclBinding>> {
        self.read_acls()
            .await
            .map(|(acls, _)| acls.matching(filter).cloned().collect())
    }

    async fn delete_acls(&self, filters: &[AclFilter]) -> Result<Vec<Vec<AclBinding>>> {
        if filters.is_empty() {
            return Ok(vec![]);
        }

        // Decided inside the CAS, not before it: a filter evaluated against a
        // snapshot that then lost the race would report deleting rules another
        // writer had already removed, or miss ones it had just added.
        let mut deleted = vec![];

        self.update_acls(|acls| {
            deleted = filters
                .iter()
                .map(|filter| {
                    let selected = acls.matching(filter).cloned().collect::<Vec<_>>();

                    for binding in &selected {
                        _ = acls.bindings.remove(binding);
                    }

                    selected
                })
                .collect();
        })
        .await
        .map(|()| deleted)
    }

    async fn alter_client_quotas(
        &self,
        alterations: &[QuotaAlteration],
        validate_only: bool,
    ) -> Result<Vec<ErrorCode>> {
        // Validated against the current document before anything is written,
        // so that a request naming a key this broker does not enforce is
        // refused whole rather than half-applied — and so `validate_only` is
        // the same check without the write, rather than a second
        // implementation of it that can drift from the first.
        let mut proposed = self.read_quotas().await.map(|(quotas, _)| quotas)?;

        let outcomes = alterations
            .iter()
            .map(|alteration| match proposed.alter(alteration) {
                Ok(()) => ErrorCode::None,

                Err(error) => {
                    warn!(?alteration, %error, "refusing a quota alteration");
                    ErrorCode::InvalidConfig
                }
            })
            .collect::<Vec<_>>();

        if !validate_only && outcomes.contains(&ErrorCode::None) {
            self.update_quotas(|quotas| {
                for alteration in alterations {
                    // Re-applied against whatever document won the CAS, and the
                    // refusals above stay refused: a key this broker cannot
                    // enforce does not become enforceable by another writer
                    // having landed first.
                    _ = quotas.alter(alteration);
                }
            })
            .await?;
        }

        Ok(outcomes)
    }

    async fn describe_client_quotas(
        &self,
        components: &[QuotaFilterComponent],
        strict: bool,
    ) -> Result<Vec<(QuotaEntity, QuotaLimits)>> {
        self.read_quotas()
            .await
            .map(|(quotas, _)| quotas.matching(components, strict))
    }

    async fn client_quotas(&self) -> Result<Quotas> {
        self.read_quotas().await.map(|(quotas, _)| quotas)
    }

    async fn assert_group_schema(&self) -> Result<()> {
        let location = Path::from(format!("clusters/{}/schema/groups.json", self.cluster));

        let refuse = |found: u32| {
            error!(
                cluster = self.cluster,
                found,
                expected = GROUP_SCHEMA_VERSION,
                "refusing to start: this cluster's consumer groups are in a layout \
                 this binary does not write (#359)"
            );

            Err(Error::Message(format!(
                "cluster {} holds consumer groups in layout version {found}, \
                 but this binary writes version {GROUP_SCHEMA_VERSION}",
                self.cluster,
            )))
        };

        match self.get::<GroupSchema>(&location).await {
            Ok((schema, _)) if schema.version == GROUP_SCHEMA_VERSION => Ok(()),

            Ok((schema, _)) => refuse(schema.version),

            Err(Error::ObjectStore(error))
                if matches!(error.as_ref(), object_store::Error::NotFound { .. }) =>
            {
                // Create-only, so two replicas starting together cannot both
                // claim it: the loser is handed what the winner wrote and
                // checks that instead.
                match self
                    .put(
                        &location,
                        GroupSchema {
                            version: GROUP_SCHEMA_VERSION,
                        },
                        json_content_type(),
                        None,
                    )
                    .await
                {
                    Ok(_) => {
                        info!(
                            cluster = self.cluster,
                            version = GROUP_SCHEMA_VERSION,
                            "claimed the consumer group layout for this cluster (#359)"
                        );

                        Ok(())
                    }

                    Err(UpdateError::Outdated { current, .. })
                        if current.version == GROUP_SCHEMA_VERSION =>
                    {
                        Ok(())
                    }

                    Err(UpdateError::Outdated { current, .. }) => refuse(current.version),

                    // Nothing in this codebase deletes the layout claim, so this
                    // is the arm that should never run — but it is the arm where
                    // folding `Vanished` into `Outdated` with a defaulted
                    // document would be actively harmful: `GroupSchema::default()`
                    // is version 0, `refuse` would fire, and the broker would
                    // reject the cluster's group layout over a document nobody
                    // wrote (#431). Named, not defaulted, and not `unreachable!()`.
                    Err(UpdateError::Vanished) => Err(Error::Message(String::from(
                        "the consumer group layout claim was deleted while being claimed",
                    ))),

                    Err(UpdateError::Error(error)) => Err(error),
                    Err(UpdateError::SerdeJson(error)) => Err(Error::SerdeJson(error)),
                    Err(UpdateError::Uuid(error)) => Err(Error::Uuid(error)),
                    Err(UpdateError::MissingEtag) => Err(Error::Message(String::from(
                        "group schema claim reported a missing etag",
                    ))),
                }
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
                                        // An existing transaction whose epoch
                                        // map is empty. Nothing in this binary
                                        // writes that — the vacant arm above
                                        // always inserts epoch 0, and epochs
                                        // are only ever added — so it means a
                                        // `meta.json` this process did not
                                        // write. Refuse rather than fabricate
                                        // producer state for a transaction
                                        // whose history is unknown, answering
                                        // as the degenerate arm below does
                                        // (#276).
                                        error!(transaction_id, "transaction has no epochs");

                                        Ok(InitProducer::Completed(ProducerIdResponse {
                                            id: -1,
                                            epoch: -1,
                                            error: ErrorCode::UnknownServerError,
                                        }))
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
                // Not implemented. A client controls the API version it sends
                // and is not bound by the advertised range, so this needed no
                // error condition at all to panic the request task (#276).
                error!("AddPartitionsToTxn v4+ is not implemented");

                Err(Error::Api(ErrorCode::UnsupportedVersion))
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

        // Retention and segment compaction per prefix, interleaved and
        // concurrent (#140) — not two whole sequential passes, where a delete
        // backlog consumed the bounded run (#131) before compaction ever ran.
        // The per-partition legacy `records/` pass that used to run first went
        // with that layout (#179).
        let (deleted_segments, compacted_segments) =
            self.maintain_prefix_segments(now_ms, owned).await?;
        let expired_groups = self.expire_groups(now).await?;

        // Converge the per-topic caches of topics another replica deleted (#283).
        // Not `?`: this is memory hygiene, and a failed topic listing must not
        // cost the pass its retention and compaction — the next tick retries.
        let evicted_topics = self
            .evict_deleted_topic_caches()
            .await
            .inspect_err(|err| warn!(?err, "could not evict deleted-topic caches"))
            .unwrap_or_default();

        // Measurement only (#283): a failure here must not cost this replica its
        // retention and compaction, which is why it is not `?`.
        let meta = self
            .measure_meta()
            .await
            .inspect_err(|err| debug!(?err, "could not measure meta.json"))
            .ok();

        debug!(
            deleted_segments,
            compacted_segments,
            expired_groups,
            evicted_topics,
            ?meta
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

    /// Deleting a credential nobody has is not a failure.
    ///
    /// The same reasoning as `create_acls`: credentials are applied from
    /// configuration management, and a delete that has already taken effect
    /// must not start reporting an error on the second run.
    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        match self
            .object_store
            .delete(&self.user_scram_credential_location(user, mechanism))
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Last writer wins, as it does in Kafka.
    ///
    /// No CAS: two administrators setting one user's password at once is not a
    /// state worth preserving half of, and a read-modify-write would only make
    /// the loser's password win at a different moment.
    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let payload = serde_json::to_vec(&credential)
            .map(Bytes::from)
            .map(PutPayload::from)?;

        self.object_store
            .put_opts(
                &self.user_scram_credential_location(user, mechanism),
                payload,
                PutOptions {
                    mode: PutMode::Overwrite,
                    attributes: json_content_type(),
                    ..Default::default()
                },
            )
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    /// A principal nobody has written a credential for is `None`, not an error.
    ///
    /// That is what the handshake turns into `unknown-user`, and it must be
    /// distinguishable from a store that could not answer — which stays an
    /// error, so a throttled bucket fails the handshake loudly rather than
    /// quietly telling every client their password is wrong.
    ///
    /// One GET per handshake, uncached on purpose: a cache here would keep a
    /// deleted principal working for its lifetime, and revoking access is the
    /// one operation that must not be eventually consistent. Handshakes are per
    /// connection, not per request, and connections are long-lived.
    #[instrument(skip_all)]
    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Self::absent_is_none(
            self.get::<ScramCredential>(&self.user_scram_credential_location(user, mechanism))
                .await,
        )
        .map(|found| found.map(|(credential, _version)| credential))
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

/// True if `error` looks like a request that ran out of time rather than one the
/// store answered.
///
/// `object_store` has no timeout variant — a connect, read or overall-deadline
/// expiry arrives as `Generic` wrapping the transport error — so this walks the
/// source chain for the text, as [`is_s3_throttle`] does. Same trade-off: coupled
/// to wording that could change under us, and the alternative is not
/// distinguishing a timeout at all (#284).
fn is_timeout(error: &object_store::Error) -> bool {
    let mut current: Option<&dyn std::error::Error> = Some(error);

    while let Some(err) = current {
        let text = err.to_string().to_ascii_lowercase();

        if text.contains("timed out")
            || text.contains("timeout")
            || text.contains("deadline has elapsed")
        {
            return true;
        }

        current = err.source();
    }

    false
}

/// The `reason` label for an object-store failure.
///
/// The interesting failures used to collapse into a single `otherwise` bucket, so
/// a 503 SlowDown, a DNS failure and a TLS reset were indistinguishable in metrics
/// — during an S3 event, the one label that would make the error counter alertable
/// was the one missing (#284). Throttles and timeouts are now named; the throttle
/// signal in particular existed only in logs, via `is_s3_throttle`.
///
/// The two text-matched arms are checked *after* the structured variants, so a
/// `NotFound` whose source happens to mention a timeout is still `not_found`. They
/// are guards on the fallthrough rather than a pre-match for the same reason.
fn object_store_error_name(error: &object_store::Error) -> &'static str {
    match error {
        object_store::Error::Precondition { .. } => "pre_condition",

        object_store::Error::AlreadyExists { .. } => "already_exists",

        object_store::Error::NotModified { .. } => "not_modified",

        object_store::Error::NotFound { .. } => "not_found",

        throttled if is_s3_throttle(throttled) => "throttle",

        timed_out if is_timeout(timed_out) => "timeout",

        otherwise => {
            debug!(?otherwise);
            "otherwise"
        }
    }
}

#[derive(Debug, Clone)]
struct Metron<O> {
    request_duration: Histogram<u64>,

    /// Metered request outcomes that are not a body, labelled by method, reason
    /// and — for a request that addresses a key — key class (#167).
    ///
    /// The class is what makes the `not_modified` and `not_found` populations
    /// attributable *at the layer that bills*: this wrapper sits under the
    /// metadata cache, so it counts only the round trips that actually left the
    /// process. Without it, the 304 plane could only be inferred from
    /// `tansu_objectstore_cache_outcomes` misses by class — and that series
    /// conflates two populations, since a miss is recorded whenever no etag is
    /// memoized, whether the caller presented one (a revalidation, which the
    /// store answers `304`) or not (a body read, e.g. a ranged segment GET).
    /// That inference was right about `watermark` and `topic_metadata` and wrong
    /// about `segment`, which carries no `if_none_match` and is never revalidated
    /// at all.
    ///
    /// Deliberately not added to `request_duration`: a class label multiplies a
    /// bucketed histogram by the number of classes, and "which keys buy
    /// unchanged/absent" is a counting question.
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
            // Labelled by `class` as well as `method` (#409). The error counter
            // below has carried the key class since #203 and this had not, so
            // "the brokers are slow" — a 350 ms `get_opts` mean against the
            // maintainers' 24 ms on the same bucket — could not be narrowed to
            // "slow reading *what*". One label, and it separates a request
            // waiting on the wire from a plane with a pathological caller:
            // `list_with_delimiter` averages 4.5 s at 0.3 req/s on that fleet.
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
        class: &'static str,
        inner: BoxStream<'static, Result<ObjectMeta, object_store::Error>>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        /// Keys per `ListObjectsV2` response, i.e. per metered LIST request.
        const LIST_PAGE_KEYS: u64 = 1_000;

        let attributes = vec![
            KeyValue::new("method", method),
            KeyValue::new("cluster", self.cluster.clone()),
            KeyValue::new("class", class),
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

                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", "prefix"),
                ];
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
            KeyValue::new("class", key_class(location)),
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

                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", key_class(location)),
                ];
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
            KeyValue::new("class", key_class(location)),
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
                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", key_class(location)),
                ];
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
            KeyValue::new("class", key_class(location)),
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

                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", key_class(location)),
                ];
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
                // The stream yields the key only on success, so a failed delete
                // is classed from the error when it carries a path — the
                // `DeleteObjects` per-key failures do.
                let class = match err {
                    object_store::Error::NotFound { path, .. } => key_class(&Path::from(&path[..])),
                    _ => "other",
                };

                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", class),
                ];
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

        self.instrument_listing(
            "list",
            prefix.map_or("other", key_class),
            self.object_store.list(prefix),
        )
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
            prefix.map_or("other", key_class),
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
            KeyValue::new("class", prefix.map_or("other", key_class)),
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

                // A listing addresses a prefix, not a key: the same stand-in the
                // cache metrics use, so `sum by (class)` covers every series
                // rather than leaving listings in an unlabelled bucket.
                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", "prefix"),
                ];
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
            KeyValue::new("class", key_class(to)),
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

                let mut additional = vec![
                    KeyValue::new("reason", object_store_error_name(err)),
                    KeyValue::new("class", key_class(from)),
                ];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }
}

#[cfg(test)]
mod throttle_tests {
    use super::{
        Duration, cas_conflict_backoff, is_s3_throttle, object_store_error_name, throttle_backoff,
    };

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

    /// #284's acceptance: a throttle is distinguishable from a 404 and from a
    /// transport failure in the `reason` label, not just in the logs.
    #[test]
    fn throttle_and_timeout_have_their_own_reason() {
        assert_eq!(
            "throttle",
            object_store_error_name(&generic_s3(
                "Status { status: 503, body: \"<Error><Code>SlowDown</Code></Error>\" }"
            ))
        );

        assert_eq!(
            "timeout",
            object_store_error_name(&generic_s3("error sending request: operation timed out"))
        );

        // The two failures the report names as currently indistinguishable from a
        // throttle. A TLS reset stays in the fallthrough — naming it is not what
        // #284 asks for — but it must not be *mislabelled* as one of the two.
        assert_eq!(
            "not_found",
            object_store_error_name(&object_store::Error::NotFound {
                path: "x".into(),
                source: "missing".into(),
            })
        );
        assert_eq!(
            "otherwise",
            object_store_error_name(&generic_s3("connection reset by peer"))
        );
    }

    /// A structured variant wins over the text match, so an error the store
    /// actually answered is never relabelled by wording in its source chain.
    #[test]
    fn a_structured_variant_is_not_reclassified_by_its_source_text() {
        assert_eq!(
            "not_found",
            object_store_error_name(&object_store::Error::NotFound {
                path: "x".into(),
                source: "request timed out before the object was found".into(),
            })
        );
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

#[cfg(test)]
mod floor_above_tail_tests {
    use super::floor_above_tail;

    /// The state #290 is about: segments survive, and the floor advertises offsets
    /// above their tail. Two records at `[0, 2)` with a floor of 4 leaves offsets 2
    /// and 3 advertised and unreadable.
    #[test]
    fn a_floor_above_a_surviving_tail_is_the_gap() {
        assert_eq!(Some(2), floor_above_tail(Some(2), 4));
    }

    /// **Not** this state, and the distinction the counter exists to preserve: a
    /// sub-stream with no segment at all is a drained partition, whose floor is
    /// legitimately its only authority. #299 already reports it as a log that starts
    /// where it ends, and counting it here would bury the partial case in noise —
    /// drained partitions are common on a fleet with retention.
    #[test]
    fn a_drained_substream_is_not_the_gap() {
        assert_eq!(None, floor_above_tail(None, 3_024_895));
    }

    /// The ordinary case: the segments reach the floor, so nothing is advertised
    /// that cannot be served.
    #[test]
    fn a_tail_that_reaches_the_floor_is_not_the_gap() {
        assert_eq!(None, floor_above_tail(Some(4), 4));
        assert_eq!(None, floor_above_tail(Some(9), 4));
    }
}

#[cfg(test)]
mod served_end_tests {
    use super::ServedEnd;

    /// The honor condition (#290): the pair speaks only for the floor it was
    /// written with. Any other `high` — an older binary's expiry moved it
    /// without re-certifying — silences it.
    #[test]
    fn a_pair_certifies_exactly_its_own_floor() {
        let served = ServedEnd { end: 2, at_high: 4 };
        assert!(served.certifies(4));
        assert!(!served.certifies(9));
        assert!(!served.certifies(2));
    }

    /// The gap is `[end, at_high)`: `end` is the first destroyed offset — a
    /// consumer parked there waits for a record that can never come — and
    /// `at_high` is excluded because it is where the next record will be
    /// assigned (a peer may already be writing it).
    #[test]
    fn the_gap_is_the_surviving_tail_up_to_the_floor() {
        let served = ServedEnd { end: 2, at_high: 4 };
        assert!(!served.gap_contains(1));
        assert!(served.gap_contains(2));
        assert!(served.gap_contains(3));
        assert!(!served.gap_contains(4));
    }

    /// A certification with nothing destroyed above the survivors — the
    /// expiry deleted only lower segments, or none of this sub-stream's —
    /// declares an empty gap and can never error a fetch.
    #[test]
    fn survivors_reaching_the_floor_leave_no_gap() {
        let served = ServedEnd { end: 4, at_high: 4 };
        assert!(!served.gap_contains(3));
        assert!(!served.gap_contains(4));
    }
}
