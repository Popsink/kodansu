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

//! What a [`DynoStore`] is configured with, as opposed to what it caches
//! (#554).
//!
//! Twenty-two of the engine's fields were neither state nor cache: they are the
//! values a deployment chooses at store build and never changes afterwards.
//! Held flat beside the caches, they were most of what made the field list
//! unreadable and gave "how is this store configured?" no answer shorter than
//! reading every field. Grouped, `coalesce_tuning` also stops being a
//! twelve-arm struct update.

use std::time::Duration;

use url::Url;

use super::DynoStore;
use crate::{AutoTopicCreate, DEFAULT_FETCH_MAX_BYTES, TopicDefaults};

/// Who this store is: the cluster it serves, the broker it answers as, and the
/// process instance it runs in.
///
/// Immutable for the store's life, and cloned with it — a `DynoStore` is
/// `Clone`, and every clone is the same writer.
#[derive(Clone, Debug)]
pub(super) struct StoreIdentity {
    /// The cluster name every object path is rooted at.
    pub(super) cluster: String,

    /// The broker id this store answers `Metadata` as. Always 111: the design is
    /// single-node and stateless, so there is no second id to be.
    pub(super) node: i32,

    /// The listener advertised to clients in `Metadata`.
    pub(super) advertised_listener: Url,

    /// This writer's identity, recorded in the lease `holder` field (#59) for
    /// observability. Unique per process instance so two brokers (or two test
    /// stores) are distinguishable.
    pub(super) writer_id: String,

    /// Per-process random seed for the maintenance traversal shuffle (#126), so
    /// N stateless maintainers sweep the prefix set in independent orders and
    /// partition the work by first-arrival rather than all starting at prefix 0.
    pub(super) maintenance_seed: u64,
}

/// Every value a deployment configures a store with (#554), each seeded from a
/// compile-time default and overridden — where it is overridable at all — from
/// the storage URL.
///
/// One struct rather than twenty flat fields because they share a lifetime and
/// a source: they are all fixed at build and none of them is state. The
/// `Option`-ful [`super::CoalesceTuning`] is the *input* shape; this is the
/// resolved one, and `DynoStore::coalesce_tuning` is the merge between them.
#[derive(Clone, Debug)]
pub(super) struct Tuning {
    /// Broker auto-topic-creation policy (Kafka `auto.create.topics.enable` /
    /// `num.partitions` / `default.replication.factor`), consulted by the
    /// Metadata handler.
    pub(super) auto_create: AutoTopicCreate,

    /// Broker-level topic config defaults, injected into every topic this engine
    /// creates. Held here rather than in the `CreateTopics` service so the
    /// injection sits at the single creation choke point and cannot be bypassed
    /// by a caller that builds its own `CreatableTopic` — which is exactly how
    /// the auto-create path silently dropped it (#225).
    pub(super) topic_defaults: TopicDefaults,

    /// Kafka's `message.max.bytes`: the largest record batch this broker
    /// accepts, in wire bytes including the length prefix (#443). Defaults to
    /// [`DynoStore::MESSAGE_MAX_BYTES`], Kafka's own default, and is overridable
    /// per deployment from the storage URL.
    pub(super) message_max_bytes: usize,

    /// Prefix-lease term length (#59). A held lease is reused without a write
    /// while more than a third of the term remains, so renewal happens ~once per
    /// `2/3 · ttl` — kept well above GCS's ~1/s/object mutation cap (#13) and
    /// never tied to the flush cadence. Defaults to [`DynoStore::PREFIX_LEASE_TTL`];
    /// lowered in tests to exercise failover.
    pub(super) prefix_lease_ttl: Duration,

    /// Runtime coalescing (#50) / producer-checkpoint (#48) flush thresholds
    /// (#54). Each is seeded from its compile-time default
    /// ([`DynoStore::COALESCE_LINGER`], [`DynoStore::COALESCE_BATCHES`],
    /// [`DynoStore::COALESCE_BYTES`], [`DynoStore::PRODUCER_CHECKPOINT_INTERVAL`],
    /// [`DynoStore::PRODUCER_CHECKPOINT_BATCHES`]) and overridable per deployment via
    /// [`DynoStore::coalesce_tuning`], so a high-topic-fan-out workload can widen the
    /// linger / checkpoint windows from the storage URL without recompiling.
    pub(super) coalesce_linger: Duration,
    pub(super) coalesce_batches: usize,
    pub(super) coalesce_bytes: usize,

    /// Upper bound on one `Fetch` response (#539). Seeded from
    /// [`crate::DEFAULT_FETCH_MAX_BYTES`] and overridable per deployment via
    /// `fetch_max_bytes`, so one replica can be run against a wider bound and
    /// compared with the rest of the fleet without a rebuild.
    pub(super) fetch_max_bytes: u32,

    /// How long the high-watermark view (per-partition hint and prefix index)
    /// is served from memory before a read re-lists (#500). Defaults to
    /// [`DynoStore::HIGH_WATERMARK_HINT_TTL`] and overridable per deployment via
    /// `watermark_hint_ttl`, so a fleet whose watermark readers are periodic
    /// diagnostics (a lag check every 30s) can widen the window to match and
    /// stay on the zero-request path. The cost is cross-replica visibility: a
    /// peer replica's produce can stay invisible to this replica's
    /// `ListOffsets(LATEST)` *and fetch* for up to the window. Staleness only
    /// ever under-reports (the hint is monotonic, never above the true end),
    /// and a same-replica produce advances the hint immediately.
    pub(super) watermark_hint_ttl: Duration,

    /// The shape of the coalescing prefix derivation (#464): how many leading
    /// components of a topic name form the prefix, and what separates them.
    /// [`DynoStore::PREFIX_DEPTH`] / [`DynoStore::PREFIX_SEPARATOR`] by default —
    /// `org.env.conn` out of `org.env.conn.<schema>.<table>` — and overridable
    /// per deployment via `prefix_depth` / `prefix_separator`. A depth of `0`
    /// makes every topic its own prefix.
    ///
    /// This is not a tuning knob like the ones above it: it decides the
    /// tenant/retention/isolation boundary every segment is keyed on, so it
    /// cannot move under a populated bucket. It is sealed per cluster by
    /// [`DynoStore::sealed_prefix_shape`] at store build, and a store whose
    /// configuration disagrees with the seal never gets built.
    pub(super) prefix_depth: usize,
    pub(super) prefix_separator: String,

    /// The segment footer version this deployment **writes** (#442).
    /// [`super::SEGMENT_FORMAT_VERSION_V3`] by default; `segment_format=4` in the
    /// storage URL raises it to [`super::SEGMENT_FORMAT_VERSION_V4`], which is what
    /// turns on id-keyed sub-streams — and so what makes a topic deleted and
    /// recreated under the same name start at offset 0 rather than continue its
    /// predecessor's offsets.
    ///
    /// Readers accept both regardless. Only the writer is gated, and the gate is
    /// a flag rather than a release so the ordering is one deploy instead of
    /// two: roll the binary everywhere, then flip.
    ///
    /// **Flipping it is one-way.** A topic created under the v4 regime has an
    /// `substream_id` pinned for its lifetime; a writer put back to v3 cannot
    /// express that identity and refuses the write
    /// ([`DynoStore::encode_segment_indexed`]) rather than writing records under a
    /// key nothing reads. Going back therefore means those topics stop being
    /// writable, not that they quietly degrade — and older binaries, which do
    /// not model `substream_id` at all, would do the quiet thing. Do not flip
    /// this before every replica is on a build that has this field.
    pub(super) segment_format_version: u16,

    /// Segment-compaction thresholds (#66), each seeded from its compile-time
    /// default and overridable per deployment via [`DynoStore::coalesce_tuning`].
    /// `prefix_compact_min_segments == 0` disables compaction.
    pub(super) prefix_compact_min_segments: usize,
    pub(super) prefix_compact_target_bytes: usize,
    pub(super) prefix_compact_keep_hot: usize,

    /// Bound on the per-key pass's `seen` key set for one partition (#175). The
    /// set is O(distinct keys per partition) — identical to the legacy
    /// compactor's — but a pathological keyspace could balloon a maintainer's
    /// memory; past the cap the pass aborts that partition for this tick
    /// (removing nothing — never corrupting), rather than growing unbounded. A
    /// Kafka-style dirty map is the follow-up if a real workload hits this.
    pub(super) prefix_compact_seen_keys: usize,

    /// Recency window for stateless maintenance scheduling (#126): a prefix
    /// whose compaction lease was last acquired within this window is skipped by
    /// other maintainers (they neither LIST nor re-work it). Set to ~0.9× the
    /// `maintenance_interval` so every prefix is still maintained ~once per
    /// interval by exactly one replica. `0` disables the skip (every maintainer
    /// works every prefix — the single-maintainer default behaviour).
    pub(super) maintenance_recency: Duration,

    /// Wall-clock budget for the leaseless prefix flush's conflict-correction
    /// loop (#157/#192). The loop yields to a competing writer rather than
    /// amplifying LIST+PUT against a contended prefix — but only once it has
    /// made [`DynoStore::MIN_FLUSH_ATTEMPTS`] real attempts, because surrendering
    /// rejects the produce and a rejected produce costs a connector restart
    /// downstream. Overridable via `flush_max_elapsed`.
    pub(super) flush_max_elapsed: Duration,
}

impl Tuning {
    /// Take every override `overrides` states, leaving the rest at their current
    /// value — so an all-`None` `CoalesceTuning` is a no-op and reproduces the
    /// shipped behaviour.
    ///
    /// One field-by-field merge rather than thirteen arms of a `DynoStore`
    /// struct update, which is what it was while these were flat fields (#554).
    pub(super) fn apply(&mut self, overrides: super::CoalesceTuning) {
        let super::CoalesceTuning {
            coalesce_linger,
            coalesce_batches,
            coalesce_bytes,
            fetch_max_bytes,
            prefix_compact_min_segments,
            prefix_compact_target_bytes,
            prefix_compact_keep_hot,
            prefix_compact_seen_keys,
            maintenance_recency,
            flush_max_elapsed,
            watermark_hint_ttl,
            segment_format_version,
            prefix_depth,
            prefix_separator,
        } = overrides;

        macro_rules! take {
            ($($field:ident),* $(,)?) => {
                $(if let Some(value) = $field {
                    self.$field = value;
                })*
            };
        }

        take!(
            coalesce_linger,
            coalesce_batches,
            coalesce_bytes,
            fetch_max_bytes,
            prefix_compact_min_segments,
            prefix_compact_target_bytes,
            prefix_compact_keep_hot,
            prefix_compact_seen_keys,
            maintenance_recency,
            flush_max_elapsed,
            watermark_hint_ttl,
            segment_format_version,
            prefix_depth,
            prefix_separator,
        );
    }

    /// The shipped defaults: what a store built from a URL with no tuning
    /// parameters runs with.
    pub(super) fn new() -> Self {
        Self {
            auto_create: AutoTopicCreate::default(),
            topic_defaults: TopicDefaults::default(),
            message_max_bytes: DynoStore::MESSAGE_MAX_BYTES,
            prefix_lease_ttl: DynoStore::PREFIX_LEASE_TTL,
            coalesce_linger: DynoStore::COALESCE_LINGER,
            coalesce_batches: DynoStore::COALESCE_BATCHES,
            coalesce_bytes: DynoStore::COALESCE_BYTES,
            fetch_max_bytes: DEFAULT_FETCH_MAX_BYTES,
            watermark_hint_ttl: DynoStore::HIGH_WATERMARK_HINT_TTL,
            prefix_depth: DynoStore::PREFIX_DEPTH,
            prefix_separator: DynoStore::PREFIX_SEPARATOR.to_owned(),
            segment_format_version: super::SEGMENT_FORMAT_VERSION_V3,
            prefix_compact_min_segments: DynoStore::PREFIX_COMPACT_MIN_SEGMENTS,
            prefix_compact_target_bytes: DynoStore::PREFIX_COMPACT_TARGET_BYTES,
            prefix_compact_keep_hot: DynoStore::PREFIX_COMPACT_KEEP_HOT,
            prefix_compact_seen_keys: DynoStore::PREFIX_COMPACT_SEEN_KEYS,
            maintenance_recency: DynoStore::MAINTENANCE_RECENCY,
            flush_max_elapsed: DynoStore::FLUSH_MAX_ELAPSED,
        }
    }
}
