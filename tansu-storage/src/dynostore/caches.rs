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

//! The engine's process-local caches of topic lifecycle state, under one owner
//! (#554).
//!
//! [`DynoStore`](super::DynoStore) held these as eight independently-locked
//! fields, and the answer to "what does deleting a topic clear?" was whatever
//! the invalidation sites happened to enumerate. They did not agree: the local
//! `delete_topic` path cleared all eight, and the peer-convergence sweep
//! ([`TopicCaches::retain_live`], #283) cleared six — leaving the topic-id
//! pointer and the *routing pin* on every replica that had not served the
//! delete, forever. A routing pin is the authority for which prefix and which
//! sub-stream identity (#442) a topic's records are written under, so a topic
//! deleted and re-created under the same name had its new records written under
//! the dead incarnation's identity by any peer still holding the pin — records
//! nothing reading the new topic will ever find. That is #532's shape, one map
//! further in.
//!
//! So the maps are private to this module and the lifecycle transitions are
//! methods: [`TopicCaches::forget`] is the single answer to "this topic is
//! gone", [`TopicCaches::retain_live`] is the same answer applied to a whole
//! bucket listing, and a map added here without being reached by them is a
//! compile-time-visible omission in two functions rather than a
//! stale-forever entry nothing can see.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime},
};

use opentelemetry::{KeyValue, metrics::Gauge};
use uuid::Uuid;

use super::{
    CachedWatermark, OffsetHint, OptiCon, ServedEnd, Topic, TopicIndex, TopicMetadata,
    TopicRouting, Watermark,
};
use crate::{Error, METER, Result, Topition};

/// Entries held in one of this process's in-memory caches (#554), broken down by
/// a `cache` attribute naming the map.
///
/// The level, not a count of evictions: every one of these is bounded by a sweep
/// rather than by a size policy, so what says a bound is working is that the
/// series tracks the cluster's live topic (or partition) count instead of
/// climbing past it. Divergence is the signal that something populates a map by
/// a path the sweep does not reach — which is exactly how the routing pin went
/// unnoticed.
static CACHE_ENTRIES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_cache_entries")
        .with_description("entries held in one of this process's in-memory caches")
        .build()
});

/// Topics this process holds metadata-cache entries for, after a maintenance
/// sweep (#283).
///
/// Kept beside the broken-down [`CACHE_ENTRIES`] rather than replaced by it: it
/// is the series the memory investigations (#476, #543) are plotted against, and
/// renaming it would silently empty those dashboards.
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

/// A map behind the engine's cache mutex.
///
/// Wrapping it is what lets a cache group hold eight of these and still be swept
/// by one rule: the lock discipline (a poisoned lock is an empty cache, never a
/// panic) is stated once here instead of at each of the sites that used to reach
/// into the maps directly.
#[derive(Debug)]
pub(super) struct LockedMap<K, V>(Arc<Mutex<BTreeMap<K, V>>>);

impl<K, V> Clone for LockedMap<K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K, V> Default for LockedMap<K, V> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(BTreeMap::new())))
    }
}

impl<K, V> LockedMap<K, V>
where
    K: Ord,
{
    /// Read under the lock, propagating a poisoned lock as [`Error::Poison`].
    ///
    /// For the callers whose answer is load-bearing — an offset hint, a certified
    /// floor — where serving "nothing is cached" out of a poisoned lock would be
    /// a silent behaviour change rather than a miss.
    fn read<T>(&self, f: impl FnOnce(&BTreeMap<K, V>) -> T) -> Result<T> {
        self.0.lock().map(|locked| f(&locked)).map_err(Into::into)
    }

    /// Mutate under the lock, propagating a poisoned lock as [`Error::Poison`].
    fn write<T>(&self, f: impl FnOnce(&mut BTreeMap<K, V>) -> T) -> Result<T> {
        self.0
            .lock()
            .map(|mut locked| f(&mut locked))
            .map_err(Into::into)
    }

    /// Mutate under the lock, doing nothing if it is poisoned.
    ///
    /// For eviction, which is opportunistic by construction: it drops entries
    /// whose authority is in the object store, so a skipped drop costs a re-read
    /// and the next sweep retries.
    fn evict(&self, f: impl FnOnce(&mut BTreeMap<K, V>)) {
        if let Ok(mut locked) = self.0.lock() {
            f(&mut locked);
        }
    }

    /// How many entries are held, or `0` from a poisoned lock — a gauge must not
    /// be able to take a maintenance tick down.
    fn len(&self) -> usize {
        self.0.lock().map_or(0, |locked| locked.len())
    }
}

/// Every cache keyed by a topic, by one of its partitions, or by its id: the
/// state whose lifetime is exactly a topic's, and which therefore has exactly
/// one invalidation trigger — the topic ceasing to exist.
///
/// # Bound
///
/// Each map is bounded by the cluster's live topic count (the partition-keyed
/// ones by its live partition count), and the thing that enforces the bound is
/// [`Self::retain_live`] on every maintenance tick, not a size or age policy.
/// That distinction is load-bearing for [`Self::next_offsets`]: it is offset
/// *assignment* state for a live topic, so evicting it under memory pressure
/// would be a correctness bug rather than a cache miss. Absence from a bucket
/// listing is the only admissible eviction criterion here, which is why the
/// bound is expressed as a sweep.
#[derive(Clone, Debug, Default)]
pub(super) struct TopicCaches {
    /// Per-topic optimistic-concurrency handle on `topic-metadata/{name}.json`,
    /// the authoritative record of a topic's id and config. Decomposing topic
    /// metadata out of the cluster-global `meta.json` removes the create-time CAS
    /// contention on that monolith and, crucially, makes a freshly created topic
    /// immediately visible to every replica: a replica that has never read the
    /// topic holds no cached etag, so the conditional GET cannot be
    /// short-circuited to a stale `NotModified` (the cross-replica
    /// create-then-produce race, #28).
    metas: LockedMap<Topic, OptiCon<TopicMetadata>>,

    /// Cache of the `topic-ids/{uuid}.json` pointer (topic-id -> name),
    /// immutable for a topic's lifetime, so a by-id lookup avoids an uncached
    /// object GET.
    ///
    /// Keyed by id and *valued* by name, so it is the one map here whose dead
    /// entries are found by looking at the value — the reason it was left out of
    /// the key-projecting sweep that #283 wrote, and so the reason it grew
    /// without bound on every replica that did not serve the delete (#554).
    ids: LockedMap<Uuid, Topic>,

    /// Cache of the pinned routing prefix (`topic-routing/{name}.json`), the
    /// prefix a topic's records coalesce under (#236) and the sub-stream
    /// identity they are keyed by inside a shared segment (#442).
    ///
    /// Held **without a TTL**, for the same reason [`Self::ids`] is: the pinned
    /// value is immutable for a topic's lifetime, so there is no staleness
    /// argument to make. That is the whole point of pinning it. Before, routing
    /// was re-derived from `cleanup.policy` — mutable config — so the value could
    /// only be memoized for seconds, and the per-topic conditional GET that
    /// refreshed it was 57% of the fleet's 304 plane (~$38/day).
    ///
    /// "Immutable for a topic's lifetime" is only a safe thing to cache forever
    /// while the *end* of that lifetime evicts it everywhere, which is what
    /// [`Self::retain_live`] now does and did not before.
    routing: LockedMap<Topic, TopicRouting>,

    /// Per-topic memo of whether `cleanup.policy` is `compact`, with a check
    /// time (#113). `produce` consults this on every batch to decide the coalesce
    /// route; the topic config changes only on `AlterConfigs` (rare), so a short
    /// TTL keeps the produce hot path off a per-batch conditional GET of
    /// `topic-metadata/<name>.json` while still picking up a policy change within
    /// the window.
    compacted: LockedMap<Topic, (bool, SystemTime)>,

    /// Per-partition optimistic-concurrency handle on `watermark.json`.
    watermarks: LockedMap<Topition, OptiCon<Watermark>>,

    /// Per-partition cache of the next offset to assign (== the high watermark).
    ///
    /// This is a *hint*, not the authority: the authority is the set of
    /// immutable, create-only batch objects whose names encode their base offset.
    /// The hint lets the common produce path skip a tail listing, and is
    /// reconciled against the listing on a `Create` conflict or a cold read.
    /// Keeping offset assignment off a single mutable `watermark` object is what
    /// takes the produce hot path off the GCS per-object update-rate cap (#13).
    next_offsets: LockedMap<Topition, OffsetHint>,

    /// Per-partition cache of the persisted `watermark.high` floor for
    /// prefix-coalesced sub-streams, paired with the certified seq floor under
    /// which it was read. An entry is valid only while the prefix still certifies
    /// the same floor — for coalesced sub-streams `watermark.high` only ever
    /// advances in an operation that then raises that floor — so an unchanged
    /// floor certifies the cached value. This is what takes the stale-hint LATEST
    /// path off the per-partition `watermark.json` conditional GET: a wide
    /// `endOffsets(assignment)` costs O(prefixes), not O(partitions), in
    /// object-store round-trips.
    coalesced_watermarks: LockedMap<Topition, CachedWatermark>,

    /// Per-partition memo of the resolved truncation floor (#176), including the
    /// **absence** of one (memoized as `0`). Read paths that do not pass through
    /// the watermark slow path (EARLIEST on a fresh process, the fetch clamp)
    /// resolve the floor here, paying at most one `watermark.json` GET per
    /// process per partition and then serving from memory — never a per-call 404
    /// on floor-less partitions (the #161 pathology). Entries are max-folded (the
    /// floor is monotonic). The `OptiCon` watermark cache, when populated, takes
    /// precedence over this memo: it is refreshed by the cold/slow watermark
    /// reads, while the memo is not.
    truncate_floors: LockedMap<Topition, i64>,

    /// In-memory, etag-delta-refreshed index of all topics, serving the list-all
    /// metadata path and the cleanup policies from memory. Without it, list-all
    /// swept every per-topic object (a GET each) on every request — the #29
    /// regression that OOM-crash-looped prod under metadata load. A refresh LISTs
    /// the `topic-metadata/` prefix once and GETs only the objects whose etag
    /// changed, so it scales to tens of thousands of topics.
    index: Arc<Mutex<TopicIndex>>,

    /// Single-flight guard: only one task refreshes [`Self::index`] at a time;
    /// concurrent list-all callers await it rather than each re-listing.
    index_refresh: Arc<tokio::sync::Mutex<()>>,
}

impl TopicCaches {
    /// The optimistic-concurrency handle on `topic`'s
    /// `topic-metadata/{name}.json`, allocating one if this process has not read
    /// the topic yet.
    pub(super) fn meta(&self, cluster: &str, topic: &str) -> Result<OptiCon<TopicMetadata>> {
        self.metas.write(|metas| {
            metas
                .entry(topic.to_owned())
                .or_insert_with(|| OptiCon::<TopicMetadata>::new(cluster, topic))
                .to_owned()
        })
    }

    /// The optimistic-concurrency handle on `topition`'s `watermark.json`,
    /// allocating one if this process has not read the partition yet.
    pub(super) fn watermark(
        &self,
        cluster: &str,
        topition: &Topition,
    ) -> Result<OptiCon<Watermark>> {
        self.watermarks.write(|watermarks| {
            watermarks
                .entry(topition.to_owned())
                .or_insert_with(|| OptiCon::<Watermark>::new(cluster, topition))
                .to_owned()
        })
    }

    /// The truncation floor `topition`'s cached watermark carries, without
    /// allocating a handle for a partition this process has not read.
    ///
    /// Deliberately non-inserting (unlike [`Self::watermark`]) so the expiry
    /// loop's sweep over every footer entry does not populate an `OptiCon` handle
    /// per sub-stream it will never serve.
    pub(super) fn watermark_truncate(&self, topition: &Topition) -> Result<Option<i64>> {
        self.watermarks.read(|watermarks| {
            watermarks
                .get(topition)
                .and_then(OptiCon::cached)
                .map(|watermark| watermark.truncate.unwrap_or(0))
        })
    }

    /// The memoized truncation floor for `topition`, if one was resolved here.
    pub(super) fn truncate_floor(&self, topition: &Topition) -> Result<Option<i64>> {
        self.truncate_floors
            .read(|floors| floors.get(topition).copied())
    }

    /// Max-fold `floor` into the truncation memo (the floor is monotonic, so a
    /// racing older resolution can never regress it).
    pub(super) fn memo_truncate_floor(&self, topition: &Topition, floor: i64) -> Result<()> {
        self.truncate_floors.write(|floors| {
            let entry = floors.entry(topition.to_owned()).or_insert(floor);
            *entry = (*entry).max(floor);
        })
    }

    /// The cached next offset (== high watermark) hint for `topition`, if known
    /// to this process. `None` means the partition has not been read or written
    /// here yet and the tail must be listed. Ignores listing freshness — used for
    /// offset assignment (produce) and as a listing floor, where any known lower
    /// bound is safe (a `Create` conflict / tail listing reconciles it).
    pub(super) fn high(&self, topition: &Topition) -> Result<Option<i64>> {
        self.next_offsets
            .read(|hints| hints.get(topition).map(|hint| hint.next))
    }

    /// The cached next-offset hint for `topition` iff it was last reconciled
    /// against an authoritative tail listing within `ttl`. `None` (cold, or stale
    /// beyond the TTL) means the high-watermark read path must LIST to pick up
    /// batches produced on another replica. Serving from a fresh hint is what
    /// takes the consumer Fetch hot path off the per-poll `ListObjectsV2` request
    /// (#40).
    pub(super) fn high_fresh(&self, topition: &Topition, ttl: Duration) -> Result<Option<i64>> {
        self.next_offsets.read(|hints| {
            hints.get(topition).and_then(|hint| {
                let fresh = hint.listed_at.is_some_and(|at| {
                    SystemTime::now()
                        .duration_since(at)
                        .is_ok_and(|elapsed| elapsed < ttl)
                });
                fresh.then_some(hint.next)
            })
        })
    }

    /// Advance the cached next-offset hint for `topition` after a local produce.
    /// Monotonic: a slower task can never lower a value a faster one already
    /// published, so the hint only ever moves forward (offsets are never reused).
    /// Does **not** touch `listed_at`: a local produce reflects only this
    /// replica's writes, so the TTL clock that forces cross-replica
    /// reconciliation keeps running.
    pub(super) fn set_high(&self, topition: &Topition, high: i64) -> Result<()> {
        self.next_offsets.write(|hints| {
            let entry = hints.entry(topition.to_owned()).or_default();
            entry.next = entry.next.max(high);
        })
    }

    /// Advance the hint after an authoritative tail *listing* and mark it fresh.
    /// The listing observed every batch at/after its floor — including other
    /// replicas' writes in that range — so it resets the [`Self::high_fresh`] TTL
    /// clock.
    ///
    /// `as_of` is *when the underlying data was observed*, not `now`: a live
    /// listing passes the instant captured before the LIST; the prefix-coalesce
    /// read path passes the prefix index's `refreshed_at`, because that index is
    /// itself served from a cache up to one TTL old. Stamping `now` instead let
    /// cross-pod visibility staleness compound toward ~2×TTL — the index could be
    /// a TTL stale and then be treated as fresh for another full TTL (#91).
    pub(super) fn mark_listed(
        &self,
        topition: &Topition,
        high: i64,
        as_of: SystemTime,
    ) -> Result<()> {
        self.next_offsets.write(|hints| {
            let entry = hints.entry(topition.to_owned()).or_default();
            entry.next = entry.next.max(high);
            entry.listed_at = Some(as_of);
        })
    }

    /// The cached `watermark.high` floor (and its #290 served-end certification,
    /// if any) for a prefix-coalesced sub-stream, valid only when it was read
    /// under the still-current certified seq `floor`. `None` means the caller
    /// must pay the `watermark.json` GET — once, since the slow path caches it
    /// via [`Self::cache_coalesced_watermark`].
    pub(super) fn coalesced_watermark(
        &self,
        topition: &Topition,
        floor: u64,
    ) -> Result<Option<(i64, Option<ServedEnd>)>> {
        self.coalesced_watermarks.read(|cached| {
            cached.get(topition).and_then(|cached| {
                (cached.seq_floor == floor).then_some((cached.high, cached.served))
            })
        })
    }

    /// Cache `high` and `served` (a just-read `watermark.json`) for `topition`
    /// under the certified seq `floor` that was current *at or before* the read.
    /// Pairing with an older floor is safe: any watermark advance after the read
    /// raises the floor above `floor`, invalidating this entry; an advance before
    /// the read is already contained in `high`.
    pub(super) fn cache_coalesced_watermark(
        &self,
        topition: &Topition,
        high: i64,
        served: Option<ServedEnd>,
        floor: u64,
    ) -> Result<()> {
        self.coalesced_watermarks.write(|cached| {
            _ = cached.insert(
                topition.to_owned(),
                CachedWatermark {
                    high,
                    served,
                    seq_floor: floor,
                },
            );
        })
    }

    /// Drop the next-offset hint and cached watermark floor of each of
    /// `topitions`, so the next read re-derives from the pruned index and the
    /// re-read watermark.
    ///
    /// The pair, always: `high_watermark` is answered from the hint, so dropping
    /// only the floor would keep the coalesced watermark cache cold and leave the
    /// #290 certification unanswerable on this replica until the hint aged out.
    pub(super) fn forget_hints<'a>(
        &self,
        topitions: impl IntoIterator<Item = &'a Topition> + Clone,
    ) -> Result<()> {
        self.next_offsets.write(|hints| {
            for topition in topitions.clone() {
                _ = hints.remove(topition);
            }
        })?;

        self.coalesced_watermarks.write(|cached| {
            for topition in topitions {
                _ = cached.remove(topition);
            }
        })
    }

    /// Drop everything a freshly created partition must not inherit from a
    /// same-named predecessor: the next-offset hint, the cached watermark floor
    /// and the truncation memo.
    ///
    /// The truncation memo goes for the opposite reason to the other two since
    /// #246 — not to forget the predecessor's floor but to force it to be re-read
    /// from the `watermark.json` that `delete_topic` left as the truncation
    /// tombstone, which is what makes the successor start past whatever it would
    /// otherwise inherit from slices surviving in shared segments.
    pub(super) fn forget_partition(&self, topition: &Topition) -> Result<()> {
        self.forget_hints([topition])?;
        self.truncate_floors.write(|floors| {
            _ = floors.remove(topition);
        })
    }

    /// The name cached for topic id `id`, if this process has resolved it.
    pub(super) fn topic_id(&self, id: &Uuid) -> Option<Topic> {
        self.ids
            .read(|ids| ids.get(id).cloned())
            .unwrap_or_default()
    }

    /// Memoize a resolved `topic-ids/{uuid}.json` pointer.
    pub(super) fn remember_topic_id(&self, id: Uuid, topic: Topic) {
        self.ids.evict(|ids| {
            _ = ids.insert(id, topic);
        });
    }

    /// The pinned routing for `topic`, if this process has resolved it.
    pub(super) fn routing(&self, topic: &str) -> Result<Option<TopicRouting>> {
        self.routing.read(|routing| routing.get(topic).cloned())
    }

    /// Memoize a resolved routing pin. Permanent for the topic's lifetime — see
    /// [`Self::routing`] for why that is only safe while [`Self::forget`] ends
    /// it.
    pub(super) fn remember_routing(&self, topic: Topic, pinned: TopicRouting) {
        self.routing.evict(|routing| {
            _ = routing.insert(topic, pinned);
        });
    }

    /// Whether `topic`'s `cleanup.policy` was `compact` when last checked, iff
    /// that check was within `ttl`.
    pub(super) fn compacted(&self, topic: &str, ttl: Duration) -> Result<Option<bool>> {
        self.compacted.read(|compacted| {
            compacted
                .get(topic)
                .filter(|(_, checked_at)| checked_at.elapsed().is_ok_and(|elapsed| elapsed < ttl))
                .map(|(compacted, _)| *compacted)
        })
    }

    /// Memoize a `cleanup.policy` verdict, stamped now.
    pub(super) fn remember_compacted(&self, topic: Topic, compacted: bool) {
        self.compacted.evict(|memo| {
            _ = memo.insert(topic, (compacted, SystemTime::now()));
        });
    }

    /// The topic index snapshot iff it was refreshed within `ttl`.
    pub(super) fn fresh_index(&self, ttl: Duration) -> Result<Option<Arc<Vec<TopicMetadata>>>> {
        let index = self.index.lock().map_err(Into::<Error>::into)?;
        let fresh = index.refreshed_at.is_some_and(|at| {
            SystemTime::now()
                .duration_since(at)
                .is_ok_and(|elapsed| elapsed < ttl)
        });
        Ok(fresh.then(|| index.snapshot.clone()))
    }

    /// Read the index under its lock.
    pub(super) fn with_index<T>(&self, f: impl FnOnce(&TopicIndex) -> T) -> Result<T> {
        self.index.lock().map(|index| f(&index)).map_err(Into::into)
    }

    /// Replace the index with a freshly built one, stamped now.
    pub(super) fn replace_index(
        &self,
        entries: BTreeMap<Topic, (Option<String>, TopicMetadata)>,
        snapshot: Arc<Vec<TopicMetadata>>,
    ) -> Result<()> {
        self.index
            .lock()
            .map(|mut index| {
                index.entries = entries;
                index.snapshot = snapshot;
                index.refreshed_at = Some(SystemTime::now());
            })
            .map_err(Into::into)
    }

    /// Force the next index read to refresh (after a local create or delete), so
    /// the change is reflected without waiting out the TTL.
    pub(super) fn invalidate_index(&self) {
        if let Ok(mut index) = self.index.lock() {
            index.refreshed_at = None;
        }
    }

    /// The single-flight guard for an index refresh: one task lists, the rest
    /// await it and reuse the result.
    pub(super) fn index_refresh(&self) -> &tokio::sync::Mutex<()> {
        &self.index_refresh
    }

    /// Drop every process-local entry keyed by `topic`, by one of its
    /// topitions, or by its id — the single answer to "this topic is gone"
    /// (#554).
    ///
    /// Called for a topic whose metadata object is gone: by `delete_topic` for the
    /// one it just deleted, by the retention pass for one whose last tombstone it
    /// reclaimed, and by [`Self::retain_live`] for one a peer replica deleted.
    ///
    /// Every one of these is a cache or a hint whose authority is in the object
    /// store, so dropping an entry can only cost a re-read:
    ///
    /// - `metas` — the per-topic `OptiCon`. Dropping it is also what makes a
    ///   same-named successor behave like a topic this replica has never read,
    ///   which is the state #28 needs for a fresh create to be immediately visible
    ///   (a retained handle holds a cached etag that can short-circuit the
    ///   conditional GET to a stale `NotModified`).
    /// - `ids` / `routing` — immutable-for-a-lifetime pointers, so this is the
    ///   *only* thing that can ever evict them, which is why leaving them out was
    ///   permanent rather than merely slow (#554).
    /// - `next_offsets` — a hint, reconciled against the segment listing on a cold
    ///   read or a create conflict. Reached **only** because the topic is gone:
    ///   this is offset-authority state for a live topic, so a size- or
    ///   age-triggered eviction here would be a correctness bug, not a cache miss.
    /// - `coalesced_watermarks` — certified by the prefix's seq floor, which is
    ///   unrelated to topic lifecycle, so nothing else would ever invalidate a
    ///   floor cached for the deleted incarnation.
    /// - `truncate_floors` / `watermarks` — re-read from the `watermark.json` that
    ///   `delete_topic` rewrites as the truncation tombstone (#246). The floor that
    ///   hides the topic's slices inside shared segments lives in that object, not
    ///   in these maps, so dropping the memo cannot resurrect anything: the next
    ///   reader re-reads the same floor.
    /// - `compacted` — a TTL'd memo of `cleanup.policy`.
    ///
    /// Prefix-keyed state is deliberately untouched, and is not this type's to
    /// touch. A prefix is shared between topics (`a.b.c` and `a.b.c.d` route to the
    /// same one), and no caller can tell whether the deleted topic was its last
    /// member without a scan — so evicting a prefix's segment sequence or flush
    /// lock here could put a second sequence authority on a prefix a sibling topic
    /// is still producing to.
    pub(super) fn forget(&self, topic: &str) {
        self.metas.evict(|metas| {
            _ = metas.remove(topic);
        });

        self.routing.evict(|routing| {
            _ = routing.remove(topic);
        });

        self.compacted.evict(|compacted| {
            _ = compacted.remove(topic);
        });

        self.ids.evict(|ids| ids.retain(|_, name| name != topic));

        self.watermarks
            .evict(|watermarks| watermarks.retain(|topition, _| topition.topic() != topic));

        self.next_offsets
            .evict(|hints| hints.retain(|topition, _| topition.topic() != topic));

        self.coalesced_watermarks
            .evict(|cached| cached.retain(|topition, _| topition.topic() != topic));

        self.truncate_floors
            .evict(|floors| floors.retain(|topition, _| topition.topic() != topic));
    }

    /// Drop every entry for a topic outside `live`, returning the names dropped
    /// (#283).
    ///
    /// [`Self::forget`] fixes only the replica that served the `DeleteTopics`.
    /// Eviction is process-local and a stateless fleet puts every topic through
    /// every replica, so on a ten-pod deployment nine pods keep their entries for
    /// a deleted topic — the growth is still monotonic, just at nine tenths of the
    /// rate. This is the half that converges the peers, and it is the bound in
    /// this type's doc comment.
    ///
    /// One `retain` per map against `live`, rather than [`Self::forget`] once per
    /// dead topic: the partition-keyed maps are swept whole by either shape, so
    /// per-topic invalidation made the sweep O(dead topics × cached partitions) —
    /// 400M comparisons on a tick that retired a thousand topics from a replica
    /// holding a hundred thousand partitions.
    ///
    /// The caller decides what `live` means, and must derive it from a listing
    /// that *succeeded*: a listing that did not happen says nothing about what
    /// exists, and passing an empty set for it would drop the whole fleet's caches
    /// at once. An empty listing that succeeded is a cluster with no topics, and
    /// evicting is then correct.
    pub(super) fn retain_live(&self, live: &BTreeSet<Topic>) -> BTreeSet<Topic> {
        let mut evicted = BTreeSet::new();

        retain_live_in(&self.metas, |topic, _| topic, live, &mut evicted);
        retain_live_in(&self.routing, |topic, _| topic, live, &mut evicted);
        retain_live_in(&self.compacted, |topic, _| topic, live, &mut evicted);
        retain_live_in(&self.ids, |_, topic| topic, live, &mut evicted);
        retain_live_in(&self.watermarks, topition_of, live, &mut evicted);
        retain_live_in(&self.next_offsets, topition_of, live, &mut evicted);
        retain_live_in(&self.coalesced_watermarks, topition_of, live, &mut evicted);
        retain_live_in(&self.truncate_floors, topition_of, live, &mut evicted);

        evicted
    }

    /// Record what each map holds, and answer `(topics, partitions)` for the
    /// caller's log line (#554).
    ///
    /// Read from the maps rather than derived from the sweep's `live` set: these
    /// must report what is actually held, so a map populated by a path the sweep
    /// does not reach shows up as divergence from the cluster's topic count
    /// instead of being papered over. Every one is a `len()`, so the gauges cost
    /// nothing even at 14.7k topics.
    pub(super) fn record_occupancy(&self) -> (usize, usize) {
        let topics = self.metas.len();
        let partitions = self.watermarks.len();

        for (cache, entries) in [
            ("topic_metas", topics),
            ("topic_ids", self.ids.len()),
            ("routing_prefixes", self.routing.len()),
            ("compacted_topics", self.compacted.len()),
            ("watermarks", partitions),
            ("next_offsets", self.next_offsets.len()),
            (
                "coalesced_watermark_floors",
                self.coalesced_watermarks.len(),
            ),
            ("truncate_floors", self.truncate_floors.len()),
        ] {
            CACHE_ENTRIES.record(entries as u64, &[KeyValue::new("cache", cache)]);
        }

        TOPIC_CACHE_TOPICS.record(topics as u64, &[]);
        TOPIC_CACHE_PARTITIONS.record(partitions as u64, &[]);

        (topics, partitions)
    }

    /// Rewind every next-offset hint's listing stamp to `at`, so a test can
    /// exercise the stale-hint read path without sleeping out the TTL.
    #[cfg(test)]
    pub(super) fn age_hints(&self, at: SystemTime) {
        self.next_offsets.evict(|hints| {
            for hint in hints.values_mut() {
                hint.listed_at = Some(at);
            }
        });
    }

    /// Rewind one partition's listing stamp to `at`, leaving a hint that was
    /// never listed alone — the state a locally-produced-only partition is in,
    /// which is a different read path from a stale listing.
    #[cfg(test)]
    pub(super) fn age_hint(&self, topition: &Topition, at: SystemTime) {
        self.next_offsets.evict(|hints| {
            if let Some(hint) = hints.get_mut(topition) {
                hint.listed_at = hint.listed_at.map(|_| at);
            }
        });
    }

    /// Every map's entry count, as
    /// `(metas, ids, routing, compacted, watermarks, next_offsets,
    /// coalesced_watermarks, truncate_floors)` — the whole of what this type
    /// holds, for the tests that assert a deleted topic leaves nothing behind.
    ///
    /// A tuple of all eight rather than a per-map accessor: the acceptance
    /// criterion is that *no* map keeps an entry, and a test that names the maps
    /// it checks is a test that silently stops covering the next one added
    /// (#554).
    #[cfg(test)]
    pub(super) fn occupancy(&self) -> [(&'static str, usize); 8] {
        [
            ("topic_metas", self.metas.len()),
            ("topic_ids", self.ids.len()),
            ("routing_prefixes", self.routing.len()),
            ("compacted_topics", self.compacted.len()),
            ("watermarks", self.watermarks.len()),
            ("next_offsets", self.next_offsets.len()),
            (
                "coalesced_watermark_floors",
                self.coalesced_watermarks.len(),
            ),
            ("truncate_floors", self.truncate_floors.len()),
        ]
    }
}

/// The topic a partition-keyed cache entry belongs to.
fn topition_of<'a, V>(topition: &'a Topition, _value: &'a V) -> &'a str {
    topition.topic()
}

/// Drop every entry of `cache` whose topic is absent from `live`, adding the
/// topics dropped to `evicted`.
///
/// Generic over how an entry names its topic — the key for the name- and
/// partition-keyed maps, the *value* for the id-keyed one — so the eight maps
/// are swept by one rule rather than by eight copies of it. The projection
/// taking both halves is what lets `topic_ids` be swept at all, and it not being
/// expressible in the key-only projection #283 wrote is why that map was left
/// out (#554).
fn retain_live_in<K, V, F>(
    cache: &LockedMap<K, V>,
    topic_of: F,
    live: &BTreeSet<Topic>,
    evicted: &mut BTreeSet<Topic>,
) where
    K: Ord,
    F: for<'a> Fn(&'a K, &'a V) -> &'a str,
{
    cache.evict(|entries| {
        entries.retain(|key, value| {
            let topic = topic_of(key, value);

            live.contains(topic) || {
                _ = evicted.insert(topic.to_owned());
                false
            }
        });
    });
}
