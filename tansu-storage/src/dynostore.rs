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

//! Dynamic Object Storage engine (S3, memory, ...)
//!
//! This file holds the shapes and nothing else: [`DynoStore`] itself, the
//! documents it reads and writes, the small types the engine passes between
//! its own halves, and — on the one `impl DynoStore` below — the constructor,
//! the builder methods and the tuning constants. Every behaviour lives in a
//! sibling module, each one a further `impl DynoStore` block (#550):
//!
//! | module | concern |
//! |---|---|
//! | [`object`] | the object-store primitives everything else is written in terms of |
//! | [`topics`] | topic metadata, the topic index, `CreateTopics`/`DeleteTopics`/`Metadata` |
//! | [`routing`] | which prefix a topition's records live under |
//! | [`watermarks`] | the offsets at the ends of a log |
//! | [`segments`] | segment naming, the sequence floor, the era epoch, segment creation |
//! | [`index`] | the in-memory per-prefix segment index |
//! | [`fetch`] | the read path |
//! | [`coalesce`] | the write path |
//! | [`groups`] | consumer-group documents |
//! | [`principals`] | ACLs, quotas, SCRAM credentials |
//! | [`retention`] | what gets deleted and when |
//! | [`compaction`] | what gets merged and when |
//! | [`maintenance`] | which prefixes this replica claims each tick |
//! | [`codec`] | the segment wire format |
//! | [`txn`] | transactions |
//! | [`storage`] | the [`Storage`] trait impl: one method per Kafka API |
//! | [`metrics`] | every OpenTelemetry instrument this engine records |
//! | [`metron`] | the metering decorator the object store is wrapped in |
//!
//! A method is `pub(super)` because it is called from a sibling module, not
//! because anything outside this directory may call it — nothing can, the
//! modules are private.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, btree_map::Entry},
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
use caches::{ClientCaches, PrefixCaches, PrefixLocks, TopicCaches};
use config::{StoreIdentity, Tuning};
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

mod caches;
mod coalesce;
mod codec;
mod compaction;
mod config;
mod fetch;
mod groups;
mod index;
mod maintenance;
mod metadata;
mod metrics;
mod metron;
mod object;
mod opticon;
mod principals;
mod retention;
mod routing;
mod segments;
mod storage;
mod topics;
mod txn;
mod watermarks;

use metrics::*;
pub(crate) use metron::{Metron, object_store_error_name};
use metron::{cas_conflict_backoff, is_s3_throttle, throttle_backoff};

// Re-exported rather than opening `metadata` up: `gcs::retry` needs exactly this
// one predicate and nothing else in there (#519).
pub(crate) use metadata::is_immutable;

#[cfg(test)]
mod tests;

use crate::{
    AclBinding, AclFilter, Acls, AssignmentDoc, AssignmentOutcome, AutoTopicCreate,
    BrokerRegistrationRequest, CommittedOffset, ConsumerGroupState, CorruptRegion, DivergentBatch,
    Error, GROUP_SCHEMA_VERSION, GenerationDoc, GroupDetail, GroupMember, GroupSchema, GroupState,
    ListOffsetResponse, METER, MemberDoc, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
    OffsetStage, ProducerIdResponse, QuotaAlteration, QuotaEntity, QuotaFilterComponent,
    QuotaLimits, Quotas, Result, ScramCredential, Storage, TopicDefaults, TopicId, Topition,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState,
    UpdateError, Version, storage_error_code, validation,
};

const APPLICATION_JSON: &str = "application/json";

/// One cached segment's expiry-decision inputs (#61/#176): `(seq, age_ms,
/// [(topic, partition, end_offset)])`, snapshotted under the `prefix_index`
/// lock so the truncation floors can be evaluated outside it.
/// One segment's inputs to the retention decision: its sequence, its age, and
/// where each of its sub-streams ends — identified (#442) as well as named,
/// because two incarnations of one topic can hold slices in the same segment and
/// only one of them is the topic that name resolves to today.
type SegmentExpirySnapshot = (u64, i64, Vec<(Substream, String, i32, i64)>);

/// This process's view of the cluster's retired-prefix markers (#532): prefix ->
/// (the etag it was last read at, the marker), so a refresh GETs only what
/// changed. See [`PrefixCaches`].
type RetiredPrefixCache = BTreeMap<String, (Option<String>, RetiredPrefix)>;

#[derive(Clone, Debug)]
pub struct DynoStore {
    /// Who this store is (#554): its cluster, its broker id, its advertised
    /// listener and this process's writer identity. See [`StoreIdentity`].
    identity: StoreIdentity,

    /// What this store is configured with (#554), as opposed to what it caches:
    /// every value a deployment fixes at build. See [`Tuning`].
    tuning: Tuning,

    /// Optimistic-concurrency handle on the cluster-global `meta.json`: the
    /// producer and transaction registries (#283).
    meta: OptiCon<Meta>,

    /// Every process-local cache whose lifetime is a topic's (#554): the
    /// per-topic and per-partition handles, hints and memos, the id and routing
    /// pointers, and the list-all topic index. Grouped so that "this topic is
    /// gone" has one answer ([`TopicCaches::forget`]) rather than one per
    /// invalidation site — see [`caches`] for the map that answer used to miss.
    topics: TopicCaches,

    /// Every process-local cache keyed by a coalescing prefix (#554): the footer
    /// index and the hints, memos and skip lists that ride alongside it. See
    /// [`PrefixCaches`] for why this group is bounded by the prefix count rather
    /// than by a sweep.
    prefixes: PrefixCaches,

    /// The two per-prefix single-flight locks (#554), which are not caches — see
    /// [`PrefixLocks`].
    prefix_locks: PrefixLocks,

    /// The optimistic-concurrency handle caches keyed by a client's identity
    /// (#554) — a producer id, a group id. See [`ClientCaches`].
    clients: ClientCaches,

    /// Per-prefix coalescing buffer (#57) — the only produce buffer since #177.
    /// Keyed by prefix, so one buffer accumulates `PrefixPending` batches across
    /// many topitions; drained (never held across an await) on a threshold or
    /// linger flush into one create-only segment object.
    ///
    /// Not a cache and deliberately outside every cache group (#554): an entry
    /// here is a batch a producer is waiting on an offset for, and evicting one
    /// would lose an accepted write rather than cost a re-read. It is bounded by
    /// the flush triggers, not by a sweep — a buffer exists only between a
    /// produce and its flush.
    prefix_coalesce_buffers: Arc<Mutex<BTreeMap<String, PrefixCoalesceBuffer>>>,

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
    /// What this batch's sub-stream is identified by in the segment footer
    /// (#442). Resolved where the batch is buffered — the same pinned lookup
    /// that decided which prefix it is buffered under — rather than re-derived
    /// at flush time, so a batch cannot be written under a different identity
    /// from the one its offsets were assigned against.
    substream: Substream,
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
/// steady-state refresh is O(new), not O(total segments). An entry naming a
/// segment a *peer* retired is dropped by the refresh's periodic **reconciling**
/// listing (#408), and by a data GET's 404 in between.
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
    ///
    /// Mutate through [`Self::insert_segment`] / [`Self::remove_segment`] /
    /// [`Self::retain_segments`] only: `by_substream` below is derived from this
    /// map and the two drift silently if it is written directly.
    segments: BTreeMap<u64, CachedSegment>,
    /// `(sub-stream, partition) -> (seq, index into that footer's entries)`,
    /// derived from `segments` (#492).
    ///
    /// Without it [`DynoStore::valid_substream_segments`] answers one
    /// `(sub-stream, partition)` by walking **every segment in the prefix** and,
    /// inside each, linearly scanning **every sub-stream entry in its footer** —
    /// so the cost of asking about one topic is the size of the whole prefix, and
    /// a topic with *nothing* in the prefix pays it in full. That call is made
    /// once per partition per `Fetch`, once per partition per `ListOffsets`, and
    /// twice per topition per flush attempt, all of it holding the process-wide
    /// `prefix_index` lock. Measured at this fleet's prefix scale it is
    /// milliseconds per call, which is where a serving replica's CPU goes and why
    /// every `await` in the process — object-store requests included — reads ~23x
    /// slower on a broker than on a maintainer running the same binary against
    /// the same bucket.
    ///
    /// Keyed by identity **and partition**, because a footer holds a separate
    /// entry per partition. The entry index is stored beside the sequence so the
    /// lookup lands on `footer.entries[i]` directly: resolving the identity again
    /// per segment is the inner scan this exists to remove.
    ///
    /// Cost: one `(u64, u32)` per sub-stream entry, plus one key per
    /// `(sub-stream, partition)` — and the distinct keys are the cluster's
    /// partition count, not the entry count.
    by_substream: HashMap<SubstreamKey, Vec<(u64, u32)>>,
    /// One [`Arc<str>`] per distinct topic name in this prefix, handed to every
    /// [`SubstreamEntry`] that names it (#476 item 2b).
    ///
    /// Sized by the prefix's **topics**, not its entries — 17 k names across the
    /// whole fleet against 6.2 M entries per replica — which is the ratio the
    /// interning buys. It is a set rather than a map because the key *is* the
    /// value: `HashSet::get` returns the stored `Arc`, which is the one to clone.
    ///
    /// Swept, not grown for ever: see [`Self::forget_unused_topic_names`]. A
    /// prefix outlives the topics routed into it, so without a sweep a pod that
    /// has seen a year of topic churn holds a year of names.
    topic_names: HashSet<Arc<str>>,
    /// When the live segment set was last reconciled by a listing; gates the
    /// TTL so a hot prefix lists at most once per [`DynoStore::HIGH_WATERMARK_HINT_TTL`].
    refreshed_at: Option<SystemTime>,

    /// When a listing last reconciled this index *downwards* — dropped entries
    /// whose objects are gone (#408).
    ///
    /// Distinct from `refreshed_at`, which the incremental refresh and the tail
    /// probe both stamp: neither of those can remove an entry, so a replica's
    /// index grows monotonically with every segment a *peer* retires unless
    /// something lists the prefix whole. This is the clock that **drives** that
    /// listing — due once per [`DynoStore::PREFIX_INDEX_RECONCILE_INTERVAL`] for
    /// as long as this process keeps an index for the prefix — and that bounds
    /// what it costs.
    ///
    /// Stamped by any listing of the *whole* prefix, a cold build included: a
    /// cold listing observed the same live set a pass would have, and has nothing
    /// to drop, so demanding a second one a TTL later would be pure waste. What
    /// it may not do is *prune* — see the commit block in
    /// [`DynoStore::refresh_prefix_index_inner`].
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

/// What a refresh knows about a prefix's cached index before it issues a
/// request. See [`DynoStore::prefix_index_freshness`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct IndexFreshness {
    /// The last listing is within [`DynoStore::HIGH_WATERMARK_HINT_TTL`], so a
    /// read may be served from the index with no request at all.
    fresh: bool,

    /// The highest sequence resolved by this process — the incremental listing's
    /// `start_after` cursor and the tail probe's starting point. `None` for a
    /// cold index, which has to list the prefix whole.
    cursor: Option<u64>,

    /// This index holds entries that no listing has checked for
    /// [`DynoStore::PREFIX_INDEX_RECONCILE_INTERVAL`] (#408), so the next listing
    /// must be the whole prefix and must drop what it does not find.
    ///
    /// Overrides `fresh`: a hot prefix stamps `refreshed_at` every TTL and would
    /// otherwise never reach the pass at all — the exact population that grew to
    /// 151 k entries per replica against 17.6 k objects.
    reconcile_due: bool,
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

/// Per-sub-stream offset coverage of a candidate merge run (#398) — the
/// guard that stops a merge from closing a hole.
///
/// The read path re-derives every record's offset by running from the
/// merged footer entry's `base_offset`
/// ([`DynoStore::fetch_prefix_coalesced`]), so a merged region must cover one
/// unbroken offset interval per sub-stream. Normally it does: the segments
/// of a prefix tile the offset axis with no gaps, so any run of them is
/// contiguous whatever order it was selected in. Quarantining a segment
/// (see [`PrefixCaches`]) punches a hole in that tiling, and
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

/// What identifies a sub-stream inside a shared segment object (#442).
///
/// By **name** for every topic created before footer v4, and that is exactly why
/// a topic deleted and recreated under the same name continued its
/// predecessor's offsets instead of starting at 0. The predecessor's records
/// live in shared, immutable segments — a segment multiplexes many topics and
/// is reclaimed whole only once every sub-stream in it is past retention — so
/// they cannot be removed, and a successor's log starting at 0 would be laid
/// directly on top of records that are still there and still found by name.
/// What keeps them apart today is the truncation floor `DeleteTopics` leaves
/// behind (#246): the successor's log starts at the predecessor's end.
///
/// By **topic id** for a topic created under the v4 writer regime
/// ([`Tuning::segment_format_version`]). A recreation is a different uuid, so
/// it is a different sub-stream, its predecessor's slices are unreachable by
/// construction rather than hidden by a floor, and its log starts where an empty
/// log starts. The pinned identity is `topic-routing/{name}.json`'s
/// `substream_id` (see [`TopicRouting`]) — immutable for the topic's lifetime,
/// which is what lets every reader cache it permanently.
///
/// This is the published external-reader contract; see
/// `docs/virtual-topics-format.md`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum Substream {
    Name(String),
    Id(Uuid),
}

/// A sub-stream inside a merge run.
type SubstreamKey = (Substream, i32);

/// One sub-stream's offset coverage within a candidate run: the lowest base
/// offset seen, the highest end, and the records held between them. The spans
/// are one unbroken interval exactly when the records equal the offsets spanned.
#[derive(Copy, Clone, Debug)]
struct OffsetSpan {
    base: i64,
    end: i64,
    records: i64,
}

#[derive(Debug, Default)]
struct RunCoverage {
    spans: BTreeMap<SubstreamKey, OffsetSpan>,
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
#[derive(Clone, Debug, Default)]
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
    /// Freshness window of the in-memory high-watermark view (#500): the
    /// per-partition hint and the prefix index are served without a listing
    /// while younger than this. Widening it keeps periodic wide `endOffsets`
    /// callers (lag diagnostics) on the zero-request path; the price is that a
    /// peer replica's produce stays invisible to `ListOffsets(LATEST)` and to
    /// fetch on this replica for up to the window. Staleness only ever
    /// under-reports the end offset, so a consumer acting on it sees
    /// duplicates, never loss.
    pub watermark_hint_ttl: Option<Duration>,
    /// The segment footer version this deployment writes (#442). See
    /// [`Tuning::segment_format_version`] — including why raising it is a
    /// one-way move.
    pub segment_format_version: Option<u16>,
    /// Leading components of a topic name that form its coalescing prefix
    /// (#464); `0` gives every topic its own prefix. See
    /// [`Tuning::prefix_depth`] — this one is sealed per cluster, not tuned.
    pub prefix_depth: Option<usize>,
    /// The separator those components are split on (#464). See
    /// [`Tuning::prefix_separator`].
    pub prefix_separator: Option<String>,

    /// Upper bound on one `Fetch` response (#539); `None` keeps
    /// [`crate::DEFAULT_FETCH_MAX_BYTES`]. Raising it trades broker memory —
    /// the bound is per fetch in flight — for fewer round trips per record,
    /// which is the only term a single-partition consumer can amortise.
    pub fetch_max_bytes: Option<u32>,
}

/// Process-wide counter making each [`DynoStore`]'s `writer_id` unique (#59), so
/// two stores in one process (or two brokers) are distinguishable holders in a
/// prefix lease.
static WRITER_INSTANCE: AtomicU64 = AtomicU64::new(0);

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
    /// The retired-prefix marker listing that keeps a deleted topic's prefix
    /// expirable (#532).
    RetiredPrefix,
    /// Connectivity check.
    Ping,
}

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
/// **What every leaseless write emits by default** ([`Self::encode_segment_indexed`]: the
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

/// Segment format version adding a per-entry `topic_id` (#442), so a sub-stream
/// can be identified by the topic's **id** rather than by its name — see
/// [`Substream`] for why that is what makes a recreated topic a new log instead
/// of a continuation of the one it replaced.
///
/// Shipped the same way v3 was, and for the same reason: a reader meeting a
/// version it does not know hard-errors, and that error propagates through the
/// index refresh into fetch, so one writer emitting v4 into a shared prefix
/// would take out every older reader's reads of that **whole prefix** — not
/// just of the topic that caused it. External S3-direct readers (kotatsu#82)
/// reject unknown versions by the same contract.
///
/// What is different is that the gate is a flag rather than a release. Nothing
/// emits v4 until a deployment sets `segment_format=4`
/// ([`Tuning::segment_format_version`]), so the ordering is: roll the binary
/// everywhere (every reader now accepts v4, nothing writes it), *then* flip.
/// The flip is one-way in practice — see the warning on
/// [`Tuning::segment_format_version`].
pub(crate) const SEGMENT_FORMAT_VERSION_V4: u16 = 4;

/// The footer versions a deployment may be configured to **write** (#442).
///
/// Not "everything the reader accepts": v1 and v2 stay readable so segments
/// written before they were superseded stay readable in place, but nothing may
/// be asked to emit them again — the version follows the writer regime, and the
/// regime only moves forward.
pub(crate) const SEGMENT_FORMAT_WRITABLE: [u16; 2] =
    [SEGMENT_FORMAT_VERSION_V3, SEGMENT_FORMAT_VERSION_V4];

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
    /// The topic name this sub-stream's records belong to.
    ///
    /// `Arc<str>` rather than `String` because the resident copy is the same
    /// handful of names repeated millions of times (#476 item 2b): a name is a
    /// property of the *topic*, and a topic contributes one entry per segment it
    /// occupies, so a prefix holding a thousand segments of one topic held a
    /// thousand copies of its name. The mean name on the production fleet is
    /// **52.6 chars**: a `String` is 24 B here plus a 64 B allocator bin for it,
    /// against 16 B for a pointer and a length into one shared allocation — ~72 B
    /// an entry, and there were 6.24 M entries per replica on 2026-09-11.
    ///
    /// Sharing is not a property of the type — two `Arc<str>` built from the
    /// same `&str` are two allocations. It is [`PrefixIndex::intern_topic`] that
    /// makes this pay, and it is applied at the one chokepoint into the resident
    /// cache, so an entry decoded but never cached keeps its own copy and costs
    /// what it always did.
    pub(crate) topic: Arc<str>,
    /// The id of the topic incarnation these records belong to (#442, footer
    /// v4), or `None` in a v1/v2/v3 entry and for a name-keyed topic.
    ///
    /// Present is what makes this sub-stream's identity the **id** rather than
    /// the name, which is the whole of [`Substream`]. The name is kept beside it
    /// — an external reader, an audit and a log line all want to say which topic
    /// a region belongs to, and resolving an id back to a name costs an object
    /// read they should not have to make.
    pub(crate) topic_id: Option<Uuid>,
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
    ///
    /// Complete as encoded and as decoded — the footer is the durable dedup
    /// authority and a published external-reader contract. The *resident* copy
    /// is not: [`PrefixIndex::insert_segment`] prunes it to the coordinates that
    /// can still change a [`ProducerTail`] (#543). See
    /// [`Self::retain_foldable_producers`].
    ///
    /// `Box<[_]>` rather than `Vec`: 4.5 M of these are resident per replica and
    /// none is ever pushed to after construction, so the capacity word is 36 MiB
    /// of nothing, and the 12.6 % of entries with no coordinates hold a dangling
    /// pointer instead of an allocation.
    producers: Box<[ProducerCoord]>,
}

/// One sub-stream on its way into a segment object (#57): which log it is,
/// where its first record lands, and the batches themselves.
///
/// The identity travels beside the topition rather than being derived from it
/// (#442). A topition is a *name*, and a name is exactly what a sub-stream stops
/// being keyed by once its topic is id-keyed — deriving it here would silently
/// write an id-keyed topic's records under a key nothing reads.
#[derive(Clone, Debug)]
pub(crate) struct SubstreamWrite {
    pub(crate) topition: Topition,
    pub(crate) substream: Substream,
    pub(crate) base_offset: i64,
    pub(crate) batches: Vec<deflated::Batch>,
}

/// One segment of a sub-stream's epoch-fenced view
/// ([`DynoStore::valid_substream_segments`]), and the first offset it serves.
///
/// `served_from == entry.base_offset` for a disjoint entry. It is **greater**
/// when the entry starts inside the range already covered by higher-priority
/// segments and reaches past it (#461): the head `[entry.base_offset,
/// served_from)` duplicates offsets an earlier segment already serves, and only
/// the tail `[served_from, end())` belongs to this one. The byte fields are
/// untouched — a region's offsets are positional from `entry.base_offset`, so
/// it decodes whole and the consumer of the decode skips the batches wholly
/// below `served_from`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FencedSegment {
    pub(crate) seq: u64,
    pub(crate) served_from: i64,
    pub(crate) entry: SubstreamEntry,
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

/// [`ProducerCoord::flags`] bit 0 (#174): the batch is transactional
/// (wire-batch attribute bit 4). Derived from the batch attributes by the v3
/// writer ([`DynoStore::encode_segment_indexed`]); transactional *data*
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
    /// ([`DynoStore::encode_segment_indexed`]), so every re-encode — conflict
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
#[derive(Clone, Debug, Default, Eq, PartialEq)]
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

type Group = String;
type Offset = i64;
type Partition = i32;
type ProducerEpoch = i16;
type ProducerId = i64;
type Sequence = i32;
type Topic = String;

/// Per-partition next-offset hint (see [`TopicCaches`]).
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

/// The coalescing prefix shape this cluster is sealed at, held once at
/// `clusters/{cluster}/prefix-shape.json` (#464): the depth and separator
/// [`DynoStore::prefix_of`] derives with.
///
/// Written create-only the first time any store is built against the bucket, and
/// never rewritten. A store configured with a different shape fails to build
/// rather than joining the cluster ([`DynoStore::sealed_prefix_shape`]).
///
/// The seal exists because the shape is only half-pinned. Since #236 a topic's
/// routing prefix is pinned per topic, so produce and fetch keep using the
/// prefix a topic's segments are actually under whatever the configuration says
/// — no records are lost or misrouted by a shape change. Maintenance does not
/// read that pin: retention grouping and the compaction universe **re-derive**
/// the prefix from the topic name, because resolving through the pin would cost
/// a GET per topic per tick on a cold pod, the read amplification #407 objects
/// to. Move the shape under a populated cluster and those sweeps go looking
/// under prefixes that hold nothing: segments stop expiring, stop compacting,
/// and the live count per prefix grows without bound — with **nothing logged**,
/// because the empty prefixes really are clean. Sealing the shape makes derived
/// == pinned true by construction, so the free derivation stays correct.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct PrefixShape {
    depth: usize,
    separator: String,
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
    /// The id this topic's sub-streams are keyed by inside a shared segment
    /// (#442), or absent for a topic keyed by name — see [`Substream`].
    ///
    /// Pinned here rather than derived from `topic-metadata/{name}.json`'s `id`
    /// for the reason the prefix is: the identity has to be immutable and
    /// permanently cacheable, and it has to be able to say **name** for a topic
    /// that predates the v4 writer regime. A topic has an id either way; what is
    /// recorded here is whether its records were written under it.
    ///
    /// The `skip_serializing_if` is load-bearing, not stylistic, for the same
    /// reason `Watermark::truncate`'s is: a routing pin written before #442 must
    /// keep its exact byte layout (`{"prefix":…}`), or every topic's pin is
    /// rewritten — and etag-churned — on first touch across a fleet where none
    /// of them are id-keyed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    substream_id: Option<Uuid>,
}

/// The retired-prefix marker at `retired-prefixes/{prefix}.json`: the retention
/// obligation a coalescing prefix keeps after the last topic on it is deleted
/// (#532).
///
/// `delete_topic` cannot delete the topic's records — a coalesced segment
/// multiplexes many topics and is immutable, so its slices are reclaimed only
/// when the whole segment passes retention (#61/#246) — and it writes a
/// truncation tombstone per partition instead. That trade assumes retention
/// still runs on the prefix. It stopped: every maintenance universe is derived
/// from `topic-metadata/`, so deleting the last topic of a prefix removed the
/// only thing that gave the prefix a threshold, and its segments became
/// unreclaimable at any retention setting. A real account kept 27 899 of
/// 27 899 `.seg` objects (2.75 GiB) after every one of its 1 000 topics was
/// deleted.
///
/// This marker is that threshold, outliving the topic. Kept in its own
/// top-level prefix: not under `topic-metadata/`, which
/// [`DynoStore::all_topics`] lists (a marker is not a topic and must never be
/// served as one), and not under `prefixes/{prefix}/`, so it can never be
/// mistaken for a segment or reported as one by `tansu audit`.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct RetiredPrefix {
    /// The effective `retention.ms` this prefix keeps: the **longest** among the
    /// topics retired onto it, `i64::MAX` for retain-forever, in the same
    /// convention [`DynoStore::segment_retention_thresholds`] folds a live
    /// topic's into. Longest wins for the same reason it does among live
    /// siblings (#61): a segment is shared, so the shortest retention on it must
    /// never be the one that deletes it.
    retention_ms: i64,

    /// The topic whose deletion last wrote this marker, and when. Nothing reads
    /// them: they are what tells an operator where a stranded prefix came from,
    /// which is otherwise unrecoverable once the topic metadata is gone.
    topic: String,
    retired_at_ms: i64,
}

/// Pointer object at `topic-ids/{uuid}.json` mapping a topic's id back to its
/// name, so a metadata lookup by topic-id can resolve to the per-topic
/// `topic-metadata/{name}.json` object. Written create-only alongside the
/// topic in [`DynoStore::create_topic`].
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TopicIdRef {
    name: Topic,
}

/// In-memory topic index (see [`TopicCaches`]). `entries` maps a
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
            identity: StoreIdentity {
                cluster: cluster.into(),
                node,
                advertised_listener: Url::parse("tcp://127.0.0.1/").unwrap(),

                // Per-process random component so two ReplicaSet pods are
                // actually distinguishable (#126): `node` is always 111 and
                // `WRITER_INSTANCE` is a per-process counter, so without entropy
                // every pod's first store is "111-0" — harmless for the lease
                // (the etag is the fence) but it makes the lease `holder`
                // useless for forensics and would break any identity-derived
                // scheme.
                writer_id: format!(
                    "{node}-{:016x}-{}",
                    rng().random::<u64>(),
                    WRITER_INSTANCE.fetch_add(1, atomic::Ordering::Relaxed)
                ),
                maintenance_seed: rng().random::<u64>(),
            },
            tuning: Tuning::new(),
            clients: ClientCaches::default(),
            prefix_coalesce_buffers: Arc::new(Mutex::new(BTreeMap::new())),
            topics: TopicCaches::default(),
            prefixes: PrefixCaches::default(),
            prefix_locks: PrefixLocks::default(),
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

    pub fn advertised_listener(mut self, advertised_listener: Url) -> Self {
        self.identity.advertised_listener = advertised_listener;
        self
    }

    pub fn auto_create(mut self, auto_create: AutoTopicCreate) -> Self {
        self.tuning.auto_create = auto_create;
        self
    }

    /// The broker-level topic config defaults this engine injects into every topic
    /// it creates (#225).
    pub fn topic_defaults(mut self, topic_defaults: TopicDefaults) -> Self {
        self.tuning.topic_defaults = topic_defaults;
        self
    }

    /// Override the prefix single-writer lease term (#59). Kept above ~1s in
    /// production so lease renewal stays under GCS's per-object mutation cap
    /// (#13); lowered only in tests to exercise failover/fencing quickly.
    pub fn prefix_lease_ttl(mut self, prefix_lease_ttl: Duration) -> Self {
        self.tuning.prefix_lease_ttl = prefix_lease_ttl;
        self
    }

    /// Override the largest record batch this broker accepts (#443), Kafka's
    /// `message.max.bytes`. Populated from the storage URL; `0` is read as "no
    /// override" rather than "accept nothing", since a cap of zero would refuse
    /// every produce and is never what an operator meant.
    pub fn message_max_bytes(mut self, message_max_bytes: usize) -> Self {
        self.tuning.message_max_bytes = if message_max_bytes == 0 {
            Self::MESSAGE_MAX_BYTES
        } else {
            message_max_bytes
        };
        self
    }

    /// Override the coalescing (#50) / producer-checkpoint (#48) flush
    /// thresholds (#54). Each `None` in `tuning` leaves that trigger at its
    /// current value (the compile-time default), so an all-default `tuning` is a
    /// no-op and reproduces the shipped behaviour.
    pub fn coalesce_tuning(mut self, tuning: CoalesceTuning) -> Self {
        self.tuning.apply(tuning);
        self
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
    ///
    /// The compile-time default; overridable per deployment via
    /// `watermark_hint_ttl` (#500, [`Self::coalesce_tuning`]), so the window can
    /// be aligned with the cadence of the fleet's watermark readers.
    const HIGH_WATERMARK_HINT_TTL: Duration = Duration::from_secs(5);

    /// Default coalescing flush triggers (#50), whichever is reached first:
    /// linger time, batch count, byte size, or offset span. `COALESCE_LINGER`,
    /// `COALESCE_BATCHES` and `COALESCE_BYTES` are overridable per deployment via
    /// `coalesce_linger` / `coalesce_batches` / `coalesce_bytes` (#54). The span
    /// cap ([`Self::COALESCE_MAX_RECORDS`]) stays a fixed safety bound: it also
    /// bounds how far back [`Self::fetch`] must probe to find the object
    /// containing a mid-frame offset.
    /// Kafka's own `message.max.bytes` default: 1 MiB plus the record-batch
    /// overhead it allows on top (`1048576 + 12`). Matching it exactly is the
    /// point — an application that fits here fits on a stock Kafka, which is
    /// what a drop-in replacement owes its users (#443).
    pub const MESSAGE_MAX_BYTES: usize = 1_048_588;

    /// Leading topic-name components that form the coalescing prefix (#57):
    /// Popsink topics are `org.env.conn.<schema>.<table>` and the prefix is the
    /// connector unit `org.env.conn`. Overridable via `prefix_depth` (#464) —
    /// see [`Self::prefix_depth`] for why it is sealed per cluster rather than
    /// freely tunable.
    const PREFIX_DEPTH: usize = 3;

    /// What those components are separated by (#464). Overridable via
    /// `prefix_separator`.
    const PREFIX_SEPARATOR: &'static str = ".";

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

    /// Cap on [`PrefixCaches`]'s quarantine per prefix (#398).
    ///
    /// The set is one `u64` per known-bad object and production holds ~900 of
    /// them across the fleet, so this is far above the incidence it exists for.
    /// Past the cap a further bad segment is logged and *not* recorded, which
    /// restores the pre-#398 behaviour for it — the drain ends on that run — in
    /// preference to letting a pathological prefix grow the set without bound.
    const PREFIX_QUARANTINE_CAP: usize = 4_096;

    /// How often one prefix's index is reconciled downwards by a listing (#408),
    /// and so how stale it may be.
    ///
    /// The pass used to be triggered by a 404 — proof that this replica's index
    /// names an object that is gone. On the fleet that proof never arrives:
    /// consumers read the *tail*, retirement takes the *head*, so a retired
    /// segment is one nothing will fetch. `1.0.0-alpha.11`,
    /// `tansu_prefix_segment_absent{caller="fetch"}` = **0.0007/s**, and with it
    /// `tansu_prefix_index_reconciled` = 0.00014 entries/s while the index grew
    /// monotonically to **1.51 M segments across ten replicas against 17.6 k
    /// objects in the bucket** — 62-73 % of the broker's live heap, naming
    /// objects that are not there. An index only converges if something lists the
    /// prefix whole whether or not a read has tripped over a ghost, which is what
    /// this interval now drives.
    ///
    /// **What it costs.** One tier-1 listing per prefix per window per replica,
    /// and only for a prefix this process is still refreshing — 228 prefixes ×
    /// 10 replicas / 300 s ≈ **0.8 listings/s per replica**, against the 1.1/s
    /// the incremental refresh already spends when the tail probe cannot answer.
    /// The listing itself is the same request the refresh would have issued
    /// (`start_after` or not, a prefix under 1 000 objects is one page); what it
    /// buys is every ghost at once instead of one per 404. Paying it per index
    /// TTL (5 s) instead would be 45/s per replica, which is the affordability
    /// line this sits well inside.
    ///
    /// Five minutes is chosen against the maintenance interval rather than the
    /// index TTL: compaction retires segments in bursts one tick apart, so
    /// re-listing much faster than that pays for a staleness that has not
    /// accrued yet. At the fleet's ~10 retirements/s it bounds a replica's
    /// ghosts at ~3 k entries — a rounding error against the 151 k/pod it held
    /// before.
    const PREFIX_INDEX_RECONCILE_INTERVAL: Duration = Duration::from_secs(300);

    /// How long a member document must have been untouched before the reclaim
    /// will consider it (#486).
    ///
    /// The window that makes deleting an unnamed document safe: it is far longer
    /// than any join can be in flight, and longer than the largest session
    /// timeout Kafka lets a client ask for, so a document this old is one no
    /// generation is about to name.
    const GROUP_MEMBER_ORPHAN_AGE: Duration = Duration::from_hours(1);

    /// Member documents reclaimed per maintenance tick (#486).
    ///
    /// As `GROUP_EXPIRE_CHUNK` bounds expiry: a 46 k-document backlog drains over
    /// ticks rather than issuing tens of thousands of deletes at once, which is
    /// the concentrated object-store pressure #8 was about.
    const GROUP_MEMBER_RECLAIM_CHUNK: u64 = 10_000;

    /// Kafka's `retention.ms` default: 7 days.
    const DEFAULT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
}
