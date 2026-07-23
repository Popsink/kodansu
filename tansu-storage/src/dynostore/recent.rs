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

//! LRW (least-recently-written) cache of recently produced object bytes
//! (`docs/design-recent-write-cache.md`): the produce path inserts each
//! just-written `records/` object or prefix segment verbatim, and the fetch
//! path serves those bytes without the GET a tailing consumer otherwise pays
//! to re-download data this process uploaded moments earlier. Immutable,
//! create-only, never-reused object names make a hit coherent by construction
//! (the one exception — topic delete + re-create restarting `records/` offsets
//! at 0 — is handled by a purge on `delete_topic`); a miss always falls
//! through to the object store. Eviction is by insertion (write) order, not
//! access order: the workload is read-once tail consumption, so recency of
//! write predicts the next read, and a replay consumer scanning old offsets
//! cannot evict the hot tail.

use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::LazyLock,
};

use bytes::Bytes;
use object_store::path::Path;
use opentelemetry::metrics::{Counter, Gauge};

use super::METER;

/// Objects larger than this are never cached: a backfill/snapshot object is
/// replay traffic, not tail traffic — one insert would evict ~everything else.
const MAX_ENTRY_BYTES: usize = 8 << 20;

/// Fetches served from the recent-write cache — each is an object-store GET
/// (or ranged segment GET) that did not happen.
static HITS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_recent_write_cache_hits")
        .with_description("fetch data reads served from the recent-write cache")
        .build()
});

/// Fetch data reads that fell through to the object store while the cache was
/// enabled (cross-pod writes, lag past the budget window, or restarts).
static MISSES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_recent_write_cache_misses")
        .with_description("fetch data reads that missed the recent-write cache")
        .build()
});

/// Entries evicted before any fetch read them: a sustained nonzero rate means
/// the budget is undersized for the consumer lag window (or nothing is
/// tailing), so raising `recent_cache_bytes` would convert GETs into hits.
static EVICTED_UNREAD: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_recent_write_cache_evicted_unread")
        .with_description("recent-write cache entries evicted before being read")
        .build()
});

/// Bytes currently held, recorded after every mutation — the gauge to size
/// `recent_cache_bytes` (and the pod memory limit) against.
static BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("tansu_recent_write_cache_bytes")
        .with_description("bytes held by the recent-write cache")
        .build()
});

/// A cached object's bytes; `read` distinguishes a useful residency from one
/// the budget wasted (see [`EVICTED_UNREAD`]).
#[derive(Debug)]
struct Entry {
    data: Bytes,
    read: bool,
}

/// Size-bounded, write-ordered cache of whole, immutable object payloads,
/// keyed by object path. `budget == 0` disables it (the default): every
/// operation is a no-op, reproducing the shipped behaviour byte-for-byte.
pub(super) struct RecentWriteCache {
    entries: HashMap<Path, Entry>,
    /// Insertion order; the eviction queue. May hold a key already removed by
    /// a purge or replacement — eviction skips keys absent from `entries`.
    write_order: VecDeque<Path>,
    bytes: usize,
    budget: usize,
}

impl Debug for RecentWriteCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecentWriteCache")
            .field("entries", &self.entries.len())
            .field("bytes", &self.bytes)
            .field("budget", &self.budget)
            .finish()
    }
}

impl RecentWriteCache {
    pub(super) fn new(budget: usize) -> Self {
        Self {
            entries: HashMap::new(),
            write_order: VecDeque::new(),
            bytes: 0,
            budget,
        }
    }

    /// Insert a just-written object's verbatim payload. Called only after the
    /// create-only PUT succeeded, so the cached bytes are durable by
    /// definition. Evicts in write order until back under budget.
    pub(super) fn insert(&mut self, location: Path, data: Bytes) {
        let len = data.len();
        if self.budget == 0 || len > MAX_ENTRY_BYTES || len > self.budget {
            return;
        }

        if let Some(replaced) = self
            .entries
            .insert(location.clone(), Entry { data, read: false })
        {
            // Create-only names make replacement a non-event in practice; keep
            // the accounting exact and let the stale queue slot lapse.
            self.bytes -= replaced.data.len();
        }
        self.bytes += len;
        self.write_order.push_back(location);

        while self.bytes > self.budget {
            let Some(oldest) = self.write_order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&oldest) {
                self.bytes -= evicted.data.len();
                if !evicted.read {
                    EVICTED_UNREAD.add(1, &[]);
                }
            }
        }

        BYTES.record(self.bytes as u64, &[]);
    }

    /// The cached payload for `location`, if still resident. A `Some` is the
    /// whole immutable object (a segment sub-stream is sliced by the caller
    /// from the footer's byte range, exactly as the ranged GET would be).
    pub(super) fn get(&mut self, location: &Path) -> Option<Bytes> {
        if self.budget == 0 {
            return None;
        }

        match self.entries.get_mut(location) {
            Some(entry) => {
                entry.read = true;
                HITS.add(1, &[]);
                Some(entry.data.clone())
            }
            None => {
                MISSES.add(1, &[]);
                None
            }
        }
    }

    /// Drop every entry under `prefix`. Covers the one create-only name-reuse
    /// case: topic delete + re-create restarts `records/` offsets at 0, so the
    /// deleted names can name different bytes later.
    pub(super) fn purge_prefix(&mut self, prefix: &Path) {
        if self.budget == 0 {
            return;
        }

        let mut purged = 0;
        self.entries.retain(|location, entry| {
            if location.prefix_matches(prefix) {
                purged += entry.data.len();
                false
            } else {
                true
            }
        });
        self.bytes -= purged;

        BYTES.record(self.bytes as u64, &[]);
    }

    #[cfg(test)]
    pub(super) fn contains(&self, location: &Path) -> bool {
        self.entries.contains_key(location)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) fn resident_bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(name: &str) -> Path {
        Path::from(format!(
            "clusters/tansu/topics/t/partitions/0000000000/records/{name}"
        ))
    }

    #[test]
    fn disabled_budget_caches_nothing() {
        let mut cache = RecentWriteCache::new(0);
        cache.insert(path("a"), Bytes::from_static(b"aaaa"));
        assert_eq!(0, cache.len());
        assert_eq!(None, cache.get(&path("a")));
    }

    #[test]
    fn hit_returns_the_inserted_bytes() {
        let mut cache = RecentWriteCache::new(1024);
        cache.insert(path("a"), Bytes::from_static(b"aaaa"));
        assert_eq!(Some(Bytes::from_static(b"aaaa")), cache.get(&path("a")));
        assert_eq!(None, cache.get(&path("b")));
    }

    #[test]
    fn evicts_least_recently_written_first() {
        // Budget fits two 4-byte entries; the third insert evicts the oldest,
        // regardless of reads (LRW, not LRU: reading `a` does not protect it).
        let mut cache = RecentWriteCache::new(8);
        cache.insert(path("a"), Bytes::from_static(b"aaaa"));
        cache.insert(path("b"), Bytes::from_static(b"bbbb"));
        assert!(cache.get(&path("a")).is_some());

        cache.insert(path("c"), Bytes::from_static(b"cccc"));
        assert!(!cache.contains(&path("a")));
        assert!(cache.contains(&path("b")));
        assert!(cache.contains(&path("c")));
        assert_eq!(8, cache.resident_bytes());
    }

    #[test]
    fn oversized_entries_are_skipped_not_evicting() {
        let mut cache = RecentWriteCache::new(8);
        cache.insert(path("a"), Bytes::from_static(b"aaaa"));
        // Larger than the whole budget: never cached, never evicts residents.
        cache.insert(path("big"), Bytes::from_static(b"0123456789"));
        assert!(cache.contains(&path("a")));
        assert!(!cache.contains(&path("big")));
        assert_eq!(4, cache.resident_bytes());
    }

    #[test]
    fn replacement_keeps_accounting_exact() {
        let mut cache = RecentWriteCache::new(64);
        cache.insert(path("a"), Bytes::from_static(b"aaaa"));
        cache.insert(path("a"), Bytes::from_static(b"aaaaaaaa"));
        assert_eq!(1, cache.len());
        assert_eq!(8, cache.resident_bytes());
        // The stale queue slot from the first insert must not corrupt the
        // accounting when it is popped during a later eviction sweep.
        cache.insert(path("b"), Bytes::from(vec![b'0'; 60]));
        assert_eq!(60, cache.resident_bytes());
        assert!(!cache.contains(&path("a")));
    }

    #[test]
    fn purge_prefix_drops_only_matching_entries() {
        let mut cache = RecentWriteCache::new(1024);
        cache.insert(path("a"), Bytes::from_static(b"aaaa"));
        let other = Path::from("clusters/tansu/topics/u/partitions/0000000000/records/x");
        cache.insert(other.clone(), Bytes::from_static(b"xxxx"));

        cache.purge_prefix(&Path::from("clusters/tansu/topics/t/"));
        assert!(!cache.contains(&path("a")));
        assert!(cache.contains(&other));
        assert_eq!(4, cache.resident_bytes());
    }
}
