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

//! Offline segment audit: what a bucket's segments *cannot* serve (#447).
//!
//! Corruption counters ([`crate::CorruptRegion`] and the
//! `tansu_prefix_segment_regions_*` metrics) count reads that **met** damage.
//! They cannot count records that no longer exist to be read: when a merge
//! writes a segment that silently omits a region and then deletes the originals
//! (#386, fixed in `1.0.0-alpha.2`), or when a freed sequence name is reborn
//! under a peer's cached footer (#432, fixed in #434), the survivors are
//! byte-perfect. The loss is visible only as an offset range present in *no*
//! object — and Kafka consumers tolerate offset gaps, so nothing downstream
//! reports anything either.
//!
//! This walks `clusters/{cluster}/prefixes/*/segments/*.seg`, decodes each
//! footer, and asserts per sub-stream that each segment's
//! `base_offset + record_count` is the `base_offset` of the next. It needs no
//! live broker and no metrics pipeline — a full copy of the object store on a
//! local disk (`file:///path/to/copy`) is enough, which is how a deployment
//! already past the damage gets sized.
//!
//! **It measures a floor, not a total.** A hole is only visible *between* two
//! surviving segments: loss at the head or tail of a log, or a wholly erased
//! prefix, leaves nothing to bracket it and is not counted.
//!
//! # Reading the report
//!
//! [`AuditReport::lost_records`] sums only the topics where a mid-log gap has no
//! legitimate cause — `cleanup.policy=delete`. **A compacted topic's gaps are
//! not damage**: per-key compaction removes superseded keys, and a removed key
//! *is* an offset gap. This method cannot separate legitimate compaction from
//! damage on that population, so [`TopicAudit::gaps_expected`] marks those
//! topics and the headline excludes them. They are still reported, because a
//! compacted topic's gap profile is worth eyeballing — just never added in.
//!
//! # What it does not read
//!
//! The record bodies. Every check here comes from the footer plus the object's
//! size in the listing, so the cost is one ranged GET per segment
//! ([`SEGMENT_FOOTER_OVER_READ`] bytes), not the bucket. That covers the
//! structural faults that leave a trace — an entry claiming bytes past the
//! object (#393/#395), regions that do not tile the body (#403) — but not a
//! region whose *bytes* are not the batches its entry describes. Reading those
//! is a separate, whole-bucket pass.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::{
    GetOptions, GetRange, ObjectMeta, ObjectStore, ObjectStoreExt,
    aws::{AmazonS3Builder, S3ConditionalPut},
    gcp::GoogleCloudStorageBuilder,
    local::LocalFileSystem,
    memory::InMemory,
    path::Path,
};
use serde::Serialize;
use tracing::debug;
use url::Url;
use uuid::Uuid;

use crate::{
    Error, Result,
    dynostore::{
        DynoStore, SEGMENT_FOOTER_OVER_READ, SEGMENT_MAGIC, SEGMENT_TRAILER_LEN, SegmentFooter,
        Substream,
    },
};

/// Apache Kafka's default `cleanup.policy`, applied when a topic stores none.
/// Mirrors [`crate::DEFAULT_CLEANUP_POLICY`]; an absent policy is `delete`, so
/// an absent policy means a mid-log gap is damage.
const ABSENT_CLEANUP_POLICY: &str = "delete";

/// Segments fetched concurrently. One ranged GET each, so this is the audit's
/// only real knob against a remote store; a local copy saturates well before it
/// matters.
const DEFAULT_CONCURRENCY: usize = 32;

/// A sub-stream's slice of one segment, carrying enough of the segment with it
/// to name the object in a report.
#[derive(Clone, Debug)]
struct Slice {
    prefix: String,
    seq: u64,
    writer_epoch: i64,
    size: u64,
    base_offset: i64,
    record_count: i64,
    max_timestamp: i64,
}

impl Slice {
    /// One past this slice's last offset.
    fn end(&self) -> i64 {
        self.base_offset + self.record_count
    }

    fn bracket(&self) -> Bracket {
        Bracket {
            prefix: self.prefix.clone(),
            seq: self.seq,
            size: self.size,
            max_timestamp: self.max_timestamp,
        }
    }
}

/// A segment naming one end of a hole. The `max_timestamp` pair bounds *when*
/// the lost records were written, which is what a re-snapshot or a
/// source-journal replay has to target; `size` is what attributes the hole to
/// the merge path — a merged segment is at the ~16 MiB roll target, a flush
/// segment is orders of magnitude smaller.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Bracket {
    pub prefix: String,
    pub seq: u64,
    pub size: u64,
    /// Greatest record timestamp in this segment's slice of the sub-stream,
    /// in milliseconds since the epoch (the footer's `max_timestamp`).
    pub max_timestamp: i64,
}

/// An offset range that no segment covers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Gap {
    /// First offset no segment holds.
    pub lost_from: i64,
    /// Last offset no segment holds (inclusive).
    pub lost_to: i64,
    pub records: i64,
    /// The segment whose slice ends at `lost_from`.
    pub before: Bracket,
    /// The segment whose slice starts at `lost_to + 1`.
    pub after: Bracket,
}

/// A footer that describes the object it is in incorrectly.
///
/// Distinct from a gap: this is damage that *did* leave a trace, and the read
/// path meets it as [`Error::CorruptSegment`] rather than as missing records.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SegmentFault {
    pub prefix: String,
    pub seq: u64,
    pub detail: String,
}

/// One `(topic, partition)` sub-stream's coverage.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PartitionAudit {
    pub partition: i32,
    /// Lowest offset any segment holds. Loss *below* this is invisible to the
    /// method, and a `DeleteRecords` truncation floor sits here legitimately.
    pub first_offset: i64,
    /// One past the highest offset any segment holds.
    pub next_offset: i64,
    /// `next_offset - first_offset`: the offsets this sub-stream should cover.
    pub span: i64,
    pub records_present: i64,
    pub records_lost: i64,
    pub gaps: Vec<Gap>,
    /// Slices wholly inside what an earlier slice already covers — a merged
    /// segment winning over the originals it merged is the normal case, so this
    /// is informational, not a fault.
    pub overlaps_dropped: usize,

    /// Slices that overlapped the frontier and reached past it, so only their
    /// tail was counted. Informational for the same reason, and separate
    /// because a *dropped* slice contributes nothing while a *clipped* one is
    /// the only holder of the offsets past the frontier — reporting them alike
    /// is what made the sweep invent holes.
    pub overlaps_clipped: usize,
    /// Abandoned legacy `records/{offset}.batch` objects (#50) found under this
    /// partition. Since #179 the broker neither writes nor reads them, so they
    /// are not offsets this sub-stream serves — but they say the log predates
    /// segments, and a gap at the head may be theirs.
    pub legacy_batches: usize,
}

/// One topic's coverage, and whether its gaps mean anything.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TopicAudit {
    pub topic: String,
    /// Effective `cleanup.policy`; `delete` when the topic stores none, which
    /// is what the engine reads too.
    pub cleanup_policy: String,
    /// `true` when the policy contains `compact`. Per-key compaction removes
    /// superseded keys and a removed key *is* an offset gap, so this topic's
    /// gaps evidence nothing and are excluded from the headline.
    pub gaps_expected: bool,
    /// `true` when no `topic-metadata/{topic}.json` was found — the topic was
    /// deleted while its segments outlived it, or the copy is partial. Its
    /// policy is assumed `delete`.
    pub metadata_missing: bool,
    pub partitions: Vec<PartitionAudit>,
}

impl TopicAudit {
    pub fn records_lost(&self) -> i64 {
        self.partitions.iter().map(|p| p.records_lost).sum()
    }

    pub fn span(&self) -> i64 {
        self.partitions.iter().map(|p| p.span).sum()
    }
}

/// What one bucket's segments can and cannot serve.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct AuditReport {
    pub cluster: String,
    /// Segments whose footer decoded.
    pub segments: usize,
    /// Objects under a `segments/` prefix that carry no `TSEG` trailer or whose
    /// footer would not decode, and so contribute no coverage at all. Each is
    /// also a [`SegmentFault`] — but not every fault is one of these: a segment
    /// that decodes and *then* describes its object wrongly is counted in
    /// [`Self::segments`] and still contributes its slices.
    pub segments_unreadable: usize,
    /// Footer format versions seen, and how many segments carried each.
    pub versions: BTreeMap<u16, usize>,
    pub prefixes: usize,
    pub legacy_batches: usize,
    pub faults: Vec<SegmentFault>,
    /// Every topic with at least one segment slice, sorted by name.
    pub topics: Vec<TopicAudit>,
    /// Sub-streams belonging to a topic incarnation the bucket no longer
    /// resolves that name to (#442) — a deleted or recreated id-keyed topic
    /// whose slices survive in shared segments until every co-tenant is past
    /// retention.
    ///
    /// Counted rather than audited: no reader can reach them, so measuring their
    /// coverage would report gaps nothing can suffer. Counted rather than
    /// dropped silently, because they are physical debt an operator is paying
    /// for and the number is the only place it shows.
    pub retired_substreams: usize,
}

impl AuditReport {
    /// Topics whose gaps are damage: `cleanup.policy=delete`, where nothing is
    /// entitled to remove records from the middle of the log.
    pub fn damaged(&self) -> impl Iterator<Item = &TopicAudit> {
        self.topics
            .iter()
            .filter(|topic| !topic.gaps_expected && topic.records_lost() > 0)
    }

    /// The headline: records lost across `delete`-policy topics only.
    pub fn lost_records(&self) -> i64 {
        self.topics
            .iter()
            .filter(|topic| !topic.gaps_expected)
            .map(TopicAudit::records_lost)
            .sum()
    }

    /// The offset span those same topics should cover.
    pub fn spanned_records(&self) -> i64 {
        self.topics
            .iter()
            .filter(|topic| !topic.gaps_expected)
            .map(TopicAudit::span)
            .sum()
    }

    /// `lost_records / spanned_records` as a percentage, `0.0` over an empty
    /// span.
    pub fn lost_percentage(&self) -> f64 {
        let span = self.spanned_records();

        if span <= 0 {
            0.0
        } else {
            (self.lost_records() as f64) * 100.0 / (span as f64)
        }
    }
}

/// Walks a cluster's object store and reports the offsets its segments cannot
/// serve. Read-only: it issues nothing but LIST and ranged GET.
#[derive(Clone, Debug)]
pub struct Audit {
    object_store: Arc<dyn ObjectStore>,
    cluster: String,
    concurrency: usize,
}

impl Audit {
    /// An audit over the object store a storage URL names.
    ///
    /// The schemes the broker runs on — `s3://bucket`, `gs://bucket`,
    /// `memory://` — plus **`file:///path/to/copy`**, which is the one this
    /// exists for: the measurement is taken offline, from a copy of the bucket,
    /// so it depends on no live broker and cannot be perturbed by one still
    /// writing.
    pub fn try_from_url(url: &Url, cluster: impl Into<String>) -> Result<Self> {
        let bucket = url.host_str().unwrap_or("tansu");

        let object_store: Arc<dyn ObjectStore> = match url.scheme() {
            "s3" => Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .build()?,
            ),

            "gs" => Arc::new(
                GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()?,
            ),

            // `url::Url` puts the whole path in `path()` for `file://`, and an
            // absolute path is what `LocalFileSystem` wants — a copy pulled with
            // `aws s3 sync` is rooted at the bucket, so the layout below it is
            // the bucket's.
            "file" => Arc::new(LocalFileSystem::new_with_prefix(url.path())?),

            "memory" => Arc::new(InMemory::new()),

            _unsupported => return Err(Error::UnsupportedStorageUrl(url.clone())),
        };

        Ok(Self::new(object_store, cluster))
    }

    pub fn new(object_store: Arc<dyn ObjectStore>, cluster: impl Into<String>) -> Self {
        Self {
            object_store,
            cluster: cluster.into(),
            concurrency: DEFAULT_CONCURRENCY,
        }
    }

    /// Segments fetched concurrently (default [`DEFAULT_CONCURRENCY`]). Zero is
    /// read as one.
    pub fn concurrency(self, concurrency: usize) -> Self {
        Self {
            concurrency: concurrency.max(1),
            ..self
        }
    }

    pub async fn run(&self) -> Result<AuditReport> {
        let mut report = AuditReport {
            cluster: self.cluster.clone(),
            ..Default::default()
        };

        // (sub-stream, topic, partition) -> the slices every segment contributes
        // to it. Identified as well as named (#442): a topic deleted and
        // recreated leaves its predecessor's slices in the shared segments, and
        // auditing the two incarnations as one log reports overlaps and holes
        // that no reader can see — a reader resolves one identity at a time.
        let mut slices: BTreeMap<(Substream, String, i32), Vec<Slice>> = BTreeMap::new();
        let mut prefixes = BTreeSet::new();

        let located = self.locate_segments().await?;

        let mut footers = stream::iter(located.into_iter().map(|(location, meta)| async move {
            let outcome = self.read_footer(&location, meta.size).await;
            (location, meta, outcome)
        }))
        .buffer_unordered(self.concurrency);

        while let Some((location, meta, outcome)) = footers.next().await {
            let Some((prefix, seq)) = segment_coordinates(&location) else {
                // Not `{prefix}/segments/{seq}.seg` — a foreign object under the
                // prefix, which the broker's own listing skips too.
                debug!(%location, "not a segment name");
                continue;
            };

            _ = prefixes.insert(prefix.clone());

            let Decoded {
                version,
                footer_len,
                footer,
            } = match outcome {
                Ok(Some(decoded)) => decoded,

                // No `TSEG` trailer. Under `segments/` that is not the legacy
                // v0 case — nothing writes a bare batch concatenation there —
                // it is a squatter or a truncated object (#157, #50).
                Ok(None) => {
                    report.segments_unreadable += 1;
                    report.faults.push(SegmentFault {
                        prefix,
                        seq,
                        detail: String::from("no TSEG trailer"),
                    });
                    continue;
                }

                Err(error) => {
                    report.segments_unreadable += 1;
                    report.faults.push(SegmentFault {
                        prefix,
                        seq,
                        detail: format!("{error}"),
                    });
                    continue;
                }
            };

            report.segments += 1;
            *report.versions.entry(version).or_default() += 1;

            // A footer that describes its object wrongly still describes
            // offsets, and those offsets *are* in a surviving object — so the
            // slices below still count as coverage and the fault is reported
            // alongside them. Suppressing the coverage would turn one damaged
            // segment into a hole that the bucket does not actually have.
            for detail in structural_faults(meta.size, footer_len, &footer) {
                report.faults.push(SegmentFault {
                    prefix: prefix.clone(),
                    seq,
                    detail,
                });
            }

            for entry in &footer.entries {
                slices
                    .entry((entry.substream(), entry.topic.clone(), entry.partition))
                    .or_default()
                    .push(Slice {
                        prefix: prefix.clone(),
                        seq,
                        writer_epoch: footer.writer_epoch,
                        size: meta.size,
                        base_offset: entry.base_offset,
                        record_count: entry.record_count,
                        max_timestamp: entry.max_timestamp,
                    });
            }
        }

        report.prefixes = prefixes.len();
        report.faults.sort_by(|a, b| {
            a.prefix
                .cmp(&b.prefix)
                .then_with(|| a.seq.cmp(&b.seq))
                .then_with(|| a.detail.cmp(&b.detail))
        });

        let legacy = self.legacy_batches().await?;
        report.legacy_batches = legacy.values().sum();

        let policies = self.cleanup_policies().await?;
        let identities = self.substream_identities().await?;

        // A topition with only legacy objects has no segment slices, so seed the
        // topic list from both: the audit cannot measure its gaps, but silently
        // omitting a topic that has data would read as "clean".
        let topitions: BTreeSet<(Substream, String, i32)> = slices
            .keys()
            .cloned()
            .chain(
                legacy
                    .keys()
                    .map(|(topic, partition)| {
                        // A legacy `records/` object predates segments entirely,
                        // so its sub-stream is keyed by name by construction.
                        (Substream::Name(topic.clone()), topic.clone(), *partition)
                    })
                    .collect::<Vec<_>>(),
            )
            .collect();

        let mut by_topic: BTreeMap<String, Vec<PartitionAudit>> = BTreeMap::new();

        for (substream, topic, partition) in topitions {
            let held = slices
                .remove(&(substream.clone(), topic.clone(), partition))
                .unwrap_or_default();

            // Not the incarnation this name resolves to: unreachable, so
            // counted rather than audited (#442).
            if current_substream(&identities, &topic) != substream {
                report.retired_substreams += 1;
                continue;
            }

            let mut audit = audit_partition(partition, held);

            // Legacy `records/` objects predate segments entirely, so they
            // belong to a name-keyed sub-stream and to no other.
            if matches!(substream, Substream::Name(_)) {
                audit.legacy_batches = legacy
                    .get(&(topic.clone(), partition))
                    .copied()
                    .unwrap_or_default();
            }

            by_topic.entry(topic).or_default().push(audit);
        }

        report.topics = by_topic
            .into_iter()
            .map(|(topic, partitions)| {
                let policy = policies.get(&topic);

                let cleanup_policy = policy
                    .cloned()
                    .flatten()
                    .unwrap_or_else(|| String::from(ABSENT_CLEANUP_POLICY));

                TopicAudit {
                    gaps_expected: cleanup_policy.contains("compact"),
                    metadata_missing: policy.is_none(),
                    cleanup_policy,
                    topic,
                    partitions,
                }
            })
            .collect();

        Ok(report)
    }

    /// Every object under `clusters/{cluster}/prefixes/`, whether or not it
    /// looks like a segment: `era.json` and friends are filtered by name later,
    /// and one listing is cheaper than one per prefix.
    async fn locate_segments(&self) -> Result<Vec<(Path, ObjectMeta)>> {
        let prefix = Path::from(format!("clusters/{}/prefixes", self.cluster));

        self.object_store
            .list(Some(&prefix))
            .map_err(Error::from)
            .try_filter(|meta| {
                let is_segment = meta.location.as_ref().ends_with(".seg");
                async move { is_segment }
            })
            .map_ok(|meta| (meta.location.clone(), meta))
            .try_collect()
            .await
    }

    /// One ranged GET of the segment's tail, decoded into `(version, footer)`.
    ///
    /// `Ok(None)` means the tail carries no `TSEG` magic. The over-read covers
    /// the whole `[footer || trailer]` for all but a pathologically wide prefix;
    /// a larger footer costs a second, exact GET — the same two-step the read
    /// path takes.
    async fn read_footer(&self, location: &Path, size: u64) -> Result<Option<Decoded>> {
        let suffix = size.min(SEGMENT_FOOTER_OVER_READ as u64);

        if suffix < SEGMENT_TRAILER_LEN as u64 {
            return Ok(None);
        }

        let mut tail = self.tail(location, suffix).await?;

        let Some(trailer) = Trailer::of(&tail) else {
            return Ok(None);
        };

        if trailer.span() > tail.len() {
            if trailer.span() as u64 > size {
                return Err(Error::Message(format!(
                    "footer_len {} + trailer exceeds the {size} byte object",
                    trailer.footer_len,
                )));
            }

            tail = self.tail(location, trailer.span() as u64).await?;
        }

        DynoStore::decode_segment_footer(&tail).map(|footer| {
            footer.map(|footer| Decoded {
                version: trailer.version,
                footer_len: trailer.footer_len,
                footer,
            })
        })
    }

    async fn tail(&self, location: &Path, suffix: u64) -> Result<Bytes> {
        self.object_store
            .get_opts(
                location,
                GetOptions {
                    range: Some(GetRange::Suffix(suffix)),
                    ..Default::default()
                },
            )
            .await?
            .bytes()
            .await
            .map_err(Into::into)
    }

    /// Abandoned `records/{offset}.batch` objects (#50) per topition, by
    /// listing alone — the broker has neither written nor read them since #179.
    async fn legacy_batches(&self) -> Result<BTreeMap<(String, i32), usize>> {
        let prefix = Path::from(format!("clusters/{}/topics", self.cluster));

        self.object_store
            .list(Some(&prefix))
            .map_err(Error::from)
            .try_fold(BTreeMap::new(), |mut acc, meta| async move {
                if let Some(topition) = legacy_batch_coordinates(&meta.location) {
                    *acc.entry(topition).or_default() += 1;
                }

                Ok(acc)
            })
            .await
    }

    /// Each topic's stored `cleanup.policy`, `None` for a topic that stores
    /// none. A topic absent from the map has no `topic-metadata` object at all,
    /// which [`TopicAudit::metadata_missing`] reports separately — the two are
    /// different facts.
    /// Each topic's pinned sub-stream id (#442), by name — `None` for a
    /// name-keyed topic, absent for a name with no routing pin at all (which is
    /// what a deleted topic leaves).
    ///
    /// One listing of `topic-routing/` plus a GET each, exactly as
    /// [`Self::cleanup_policies`] does for `topic-metadata/`. Read as untyped
    /// JSON for the same reason: the audit is a forensic tool pointed at buckets
    /// written by other releases, and a field it does not model must not make it
    /// refuse to run.
    async fn substream_identities(&self) -> Result<BTreeMap<String, Option<Uuid>>> {
        let prefix = Path::from(format!("clusters/{}/topic-routing", self.cluster));

        let locations: Vec<Path> = self
            .object_store
            .list(Some(&prefix))
            .map_err(Error::from)
            .try_filter_map(|meta| async move {
                Ok(meta
                    .location
                    .as_ref()
                    .ends_with(".json")
                    .then_some(meta.location))
            })
            .try_collect()
            .await?;

        let mut identities = BTreeMap::new();

        let mut fetched = stream::iter(locations.into_iter().map(|location| async move {
            let bytes = self.object_store.get(&location).await?.bytes().await?;
            Result::<(Path, Bytes)>::Ok((location, bytes))
        }))
        .buffer_unordered(self.concurrency);

        while let Some(fetched) = fetched.next().await {
            let (location, bytes) = fetched?;

            let Some(topic) = location
                .filename()
                .and_then(|name| name.strip_suffix(".json"))
            else {
                continue;
            };

            let routing: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
                Error::Message(format!("{location}: unreadable topic routing: {error}"))
            })?;

            _ = identities.insert(
                topic.to_owned(),
                routing
                    .get("substream_id")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|id| Uuid::parse_str(id).ok()),
            );
        }

        Ok(identities)
    }

    async fn cleanup_policies(&self) -> Result<BTreeMap<String, Option<String>>> {
        let prefix = Path::from(format!("clusters/{}/topic-metadata", self.cluster));

        let locations: Vec<Path> = self
            .object_store
            .list(Some(&prefix))
            .map_err(Error::from)
            .try_filter_map(|meta| async move {
                Ok(meta
                    .location
                    .as_ref()
                    .ends_with(".json")
                    .then_some(meta.location))
            })
            .try_collect()
            .await?;

        let mut policies = BTreeMap::new();

        let mut fetched = stream::iter(locations.into_iter().map(|location| async move {
            let bytes = self.object_store.get(&location).await?.bytes().await?;
            Result::<(Path, Bytes)>::Ok((location, bytes))
        }))
        .buffer_unordered(self.concurrency);

        while let Some(fetched) = fetched.next().await {
            let (location, bytes) = fetched?;

            let Some(topic) = location
                .filename()
                .and_then(|name| name.strip_suffix(".json"))
            else {
                continue;
            };

            let metadata: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
                Error::Message(format!("{location}: unreadable topic metadata: {error}"))
            })?;

            _ = policies.insert(topic.to_owned(), cleanup_policy(&metadata));
        }

        Ok(policies)
    }
}

/// One segment's footer and the two trailer fields the decode does not return.
struct Decoded {
    version: u16,
    footer_len: usize,
    footer: SegmentFooter,
}

/// Ways a footer can describe the object it is in wrongly, checked from the
/// footer and the object's size alone — no body read.
///
/// This is damage that *did* leave a trace, unlike a hole: the read path meets
/// it as [`Error::CorruptSegment`] rather than as missing records. Each of these
/// is a defect that shipped — an entry claiming bytes past the object (#393,
/// #395), a region the index describes wrongly (#397, #403) — so a bucket
/// written by an older release can still be carrying one.
/// What `topic` resolves to in this bucket right now (#442). A name with no
/// routing pin — what a deleted topic leaves behind — resolves to itself, so a
/// name-keyed orphan is still audited as the topic it was.
fn current_substream(identities: &BTreeMap<String, Option<Uuid>>, topic: &str) -> Substream {
    identities
        .get(topic)
        .copied()
        .flatten()
        .map_or_else(|| Substream::Name(topic.to_owned()), Substream::Id)
}

fn structural_faults(size: u64, footer_len: usize, footer: &SegmentFooter) -> Vec<String> {
    let mut faults = Vec::new();

    let Some(body_len) = size.checked_sub(footer_len as u64 + SEGMENT_TRAILER_LEN as u64) else {
        faults.push(format!(
            "footer_len {footer_len} + trailer exceeds the {size} byte object"
        ));

        return faults;
    };

    for entry in &footer.entries {
        let name = format!("{}-{}", entry.topic, entry.partition);

        if entry.record_count < 0 || entry.base_offset < 0 {
            faults.push(format!(
                "{name} claims {} records from base offset {}",
                entry.record_count, entry.base_offset
            ));
        }

        match entry.byte_start.checked_add(entry.byte_len) {
            Some(end) if end > body_len => faults.push(format!(
                "{name} claims bytes [{}, {end}) past the {body_len} byte body",
                entry.byte_start
            )),

            None => faults.push(format!(
                "{name} claims bytes [{}, +{}), which overflows",
                entry.byte_start, entry.byte_len
            )),

            _ => {}
        }
    }

    // Only worth asking once every entry is in bounds: an over-claiming entry
    // fails to tile as well, and saying so twice buries the first finding.
    if faults.is_empty() {
        let mut regions: Vec<(u64, u64)> = footer
            .entries
            .iter()
            .map(|entry| (entry.byte_start, entry.byte_len))
            .collect();

        regions.sort_unstable();

        let covered = regions
            .iter()
            .try_fold(0u64, |at, &(byte_start, byte_len)| {
                (byte_start == at).then_some(at + byte_len)
            });

        match covered {
            Some(covered) if covered != body_len => faults.push(format!(
                "regions cover {covered} of the {body_len} byte body"
            )),

            None => faults.push(String::from(
                "regions do not lie end to end from the start of the object",
            )),

            _ => {}
        }
    }

    faults
}

/// The fixed 18-byte segment trailer (`docs/virtual-topics-format.md`).
///
/// Read here as well as in [`DynoStore::decode_segment_footer`] because the
/// audit reports the version and sizes the `[footer || trailer]` suffix, and
/// the decode returns neither.
struct Trailer {
    footer_len: usize,
    version: u16,
}

impl Trailer {
    fn of(tail: &[u8]) -> Option<Self> {
        if tail.len() < SEGMENT_TRAILER_LEN {
            return None;
        }

        let trailer = &tail[tail.len() - SEGMENT_TRAILER_LEN..];

        if u32::from_be_bytes(trailer[14..18].try_into().ok()?) != SEGMENT_MAGIC {
            return None;
        }

        Some(Self {
            footer_len: u64::from_be_bytes(trailer[0..8].try_into().ok()?) as usize,
            version: u16::from_be_bytes(trailer[12..14].try_into().ok()?),
        })
    }

    /// Bytes a reader must hold to decode this footer: the footer and the
    /// trailer that describes it.
    fn span(&self) -> usize {
        self.footer_len + SEGMENT_TRAILER_LEN
    }
}

/// `(prefix, seq)` of `clusters/{cluster}/prefixes/{prefix}/segments/{seq}.seg`,
/// `None` for anything else. The sequence is the object's name, never its offset
/// order — a merged segment covers old offsets under a fresh, higher sequence.
fn segment_coordinates(location: &Path) -> Option<(String, u64)> {
    let parts: Vec<&str> = location.as_ref().split('/').collect();

    let [.., "prefixes", prefix, "segments", name] = parts[..] else {
        return None;
    };

    name.strip_suffix(".seg")
        .and_then(|seq| seq.parse().ok())
        .map(|seq| (prefix.to_owned(), seq))
}

/// `(topic, partition)` of the legacy
/// `clusters/{c}/topics/{topic}/partitions/{p}/records/{offset}.batch` (#50),
/// `None` for anything else under `topics/` (`watermark.json`, and the
/// partition directories themselves).
fn legacy_batch_coordinates(location: &Path) -> Option<(String, i32)> {
    let parts: Vec<&str> = location.as_ref().split('/').collect();

    let [
        ..,
        "topics",
        topic,
        "partitions",
        partition,
        "records",
        name,
    ] = parts[..]
    else {
        return None;
    };

    name.ends_with(".batch")
        .then(|| partition.parse().ok())
        .flatten()
        .map(|partition| (topic.to_owned(), partition))
}

/// The `cleanup.policy` stored in a `topic-metadata/{topic}.json` document.
///
/// Read out of the JSON rather than through `TopicMetadata` so a document this
/// build cannot fully deserialize — an older or newer writer's — still yields
/// the one field the audit classifies on.
fn cleanup_policy(metadata: &serde_json::Value) -> Option<String> {
    metadata
        .get("topic")?
        .get("configs")?
        .as_array()?
        .iter()
        .find(|config| {
            config.get("name").and_then(serde_json::Value::as_str) == Some("cleanup.policy")
        })?
        .get("value")?
        .as_str()
        .map(str::to_owned)
}

/// Resolve one sub-stream's slices the way a reader does, then report what is
/// left uncovered.
///
/// The order and the overlap rule are the external contract's
/// (`docs/virtual-topics-format.md`): ascending `base_offset`, ties broken by
/// higher `writer_epoch` then higher `seq`, dropping any slice starting below
/// what is already covered. That is what makes a merged segment win over the
/// originals it merged — and it means the gaps found here are the gaps a
/// consumer meets, not an artefact of a different resolution.
fn audit_partition(partition: i32, mut slices: Vec<Slice>) -> PartitionAudit {
    let mut audit = PartitionAudit {
        partition,
        first_offset: 0,
        next_offset: 0,
        span: 0,
        records_present: 0,
        records_lost: 0,
        gaps: Vec::new(),
        overlaps_dropped: 0,
        overlaps_clipped: 0,
        legacy_batches: 0,
    };

    // A slice covering no offsets cannot bracket a hole and cannot advance
    // coverage; keeping it would make an empty entry look like a gap of zero.
    slices.retain(|slice| slice.record_count > 0);

    slices.sort_by(|a, b| {
        a.base_offset
            .cmp(&b.base_offset)
            .then_with(|| b.writer_epoch.cmp(&a.writer_epoch))
            .then_with(|| b.seq.cmp(&a.seq))
    });

    let mut covered: Option<Slice> = None;

    for slice in slices {
        let Some(previous) = covered.as_ref() else {
            audit.first_offset = slice.base_offset;
            audit.records_present += slice.record_count;
            covered = Some(slice);
            continue;
        };

        let frontier = previous.end();

        // Wholly inside what is already covered: it adds nothing. This is the
        // merged segment's originals, and the normal case.
        if slice.end() <= frontier {
            audit.overlaps_dropped += 1;
            continue;
        }

        // Overlaps the frontier and reaches past it: it contributes its TAIL,
        // `[frontier, slice.end())`.
        //
        // **Clipped, not dropped.** The reader's overlap rule
        // (`docs/virtual-topics-format.md`) says to drop an entry whose
        // `base_offset` falls below the range already covered — and that rule is
        // correct for what it is written for, resolving *one offset*, where the
        // higher-priority entry already answers it. It is not a rule for
        // computing *coverage*: applied here it discards the whole slice,
        // including the part no other slice holds, and reports that part as
        // lost.
        //
        // Measured, before this: 27.5 % of a production fleet's offset span
        // reported as lost, 3 210 of 3 356 affected partitions also carrying a
        // dropped overlap. The holes were the discarded tails.
        if slice.base_offset < frontier {
            audit.overlaps_clipped += 1;
            audit.records_present += slice.end() - frontier;
            covered = Some(slice);
            continue;
        }

        if slice.base_offset > frontier {
            audit.gaps.push(Gap {
                lost_from: frontier,
                lost_to: slice.base_offset - 1,
                records: slice.base_offset - frontier,
                before: previous.bracket(),
                after: slice.bracket(),
            });
        }

        audit.records_present += slice.record_count;
        covered = Some(slice);
    }

    if let Some(last) = covered {
        audit.next_offset = last.end();
        audit.span = audit.next_offset - audit.first_offset;
        audit.records_lost = audit.span - audit.records_present;
    }

    audit
}

#[cfg(test)]
mod tests;
