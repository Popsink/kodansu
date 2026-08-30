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

//! The offline segment audit (#447): an offset range present in no object.
//!
//! Every end-to-end case here writes segments through the real produce path and
//! then *deletes* an object, which is the shape the damage leaves behind — the
//! surviving segments stay byte-perfect, so nothing but the missing offsets
//! says anything happened.

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, local::LocalFileSystem, memory::InMemory, path::Path,
};
use tansu_sans_io::{
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
};

use url::Url;

use crate::{Error, Result, Storage, Topition, dynostore::DynoStore};

use super::{Audit, AuditReport, Slice, audit_partition, cleanup_policy, segment_coordinates};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const PREFIX: &str = "org.env.conn";
const TOPIC: &str = "org.env.conn.tab_a";

/// A non-idempotent batch of `records` records (occupies `records` offsets).
fn batch(records: usize) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder();

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("record-{i}").as_bytes(),
        ))));
    }

    builder
        .last_offset_delta(records as i32 - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(storage: &DynoStore, name: &str, configs: &[(&str, &str)]) -> Result<()> {
    let configs: Vec<CreatableTopicConfig> = configs
        .iter()
        .map(|(k, v)| {
            CreatableTopicConfig::default()
                .name((*k).to_owned())
                .value(Some((*v).to_owned()))
        })
        .collect();

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some(configs)),
            false,
        )
        .await?;

    Ok(())
}

fn segment_path(seq: u64) -> Path {
    Path::from(format!(
        "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/{seq:0>20}.seg"
    ))
}

/// Every segment object in the bucket, in name order. The prefix a topic routes
/// under is pinned at creation and is not derivable from its config (#236), so
/// a test that wants "the middle segment" has to look rather than construct.
async fn segments(bucket: &InMemory) -> Vec<Path> {
    let listing = Path::from(format!("clusters/{CLUSTER}/prefixes"));

    let mut located: Vec<Path> = bucket
        .list(Some(&listing))
        .map_ok(|meta| meta.location)
        .try_filter(|location| {
            let is_segment = location.as_ref().ends_with(".seg");
            async move { is_segment }
        })
        .try_collect()
        .await
        .expect("list segments");

    located.sort();
    located
}

/// Three windows of a single sub-stream, one segment each: offsets `[0, 3)`,
/// `[3, 5)`, `[5, 9)` under sequences 0, 1 and 2.
async fn three_windows(bucket: &InMemory, configs: &[(&str, &str)]) -> Result<()> {
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    create_topic(&store, TOPIC, configs).await?;

    let tp = Topition::new(TOPIC, 0);

    assert_eq!(0, store.produce(None, &tp, batch(3)?).await?);
    assert_eq!(3, store.produce(None, &tp, batch(2)?).await?);
    assert_eq!(5, store.produce(None, &tp, batch(4)?).await?);

    Ok(())
}

async fn audit(bucket: &InMemory) -> Result<AuditReport> {
    Audit::new(Arc::new(bucket.clone()) as Arc<dyn ObjectStore>, CLUSTER)
        .run()
        .await
}

/// The whole point: a segment that no longer exists leaves an offset range in
/// no object, and every survivor is intact. The audit has to name the range,
/// and bracket it with the segments on either side — those two `max_timestamp`s
/// are what bounds when the lost records were written.
#[tokio::test]
async fn a_deleted_segment_is_a_hole_between_two_intact_ones() -> Result<(), Error> {
    let bucket = InMemory::new();
    three_windows(&bucket, &[]).await?;

    // The middle window's object, as compaction would leave it: merged into a
    // segment that omitted the region, then retired.
    bucket.delete(&segment_path(1)).await?;

    let report = audit(&bucket).await?;

    assert_eq!(2, report.segments);
    assert_eq!(0, report.segments_unreadable);
    assert!(report.faults.is_empty(), "{:?}", report.faults);
    assert_eq!(1, report.prefixes);

    assert_eq!(1, report.topics.len());
    let topic = &report.topics[0];
    assert_eq!(TOPIC, topic.topic);
    assert!(!topic.gaps_expected);
    assert!(!topic.metadata_missing);

    assert_eq!(1, topic.partitions.len());
    let partition = &topic.partitions[0];

    assert_eq!(0, partition.first_offset);
    assert_eq!(9, partition.next_offset);
    assert_eq!(9, partition.span);
    assert_eq!(7, partition.records_present);
    assert_eq!(2, partition.records_lost);
    assert_eq!(0, partition.overlaps_dropped);

    assert_eq!(1, partition.gaps.len());
    let gap = &partition.gaps[0];
    assert_eq!(3, gap.lost_from);
    assert_eq!(4, gap.lost_to);
    assert_eq!(2, gap.records);
    assert_eq!(PREFIX, gap.before.prefix);
    assert_eq!(0, gap.before.seq);
    assert_eq!(2, gap.after.seq);

    // The headline counts it: `delete` is the policy, so nothing was entitled
    // to remove offsets 3..=4.
    assert_eq!(2, report.lost_records());
    assert_eq!(9, report.spanned_records());
    assert_eq!(1, report.damaged().count());

    Ok(())
}

/// A bucket nothing has damaged reports no loss — the audit has to be quiet on
/// a healthy cluster or it is useless for sweeping a fleet.
#[tokio::test]
async fn contiguous_segments_lose_nothing() -> Result<(), Error> {
    let bucket = InMemory::new();
    three_windows(&bucket, &[]).await?;

    let report = audit(&bucket).await?;

    assert_eq!(3, report.segments);
    assert_eq!(0, report.lost_records());
    assert_eq!(9, report.spanned_records());
    assert_eq!(0.0, report.lost_percentage());

    let partition = &report.topics[0].partitions[0];
    assert!(partition.gaps.is_empty());
    assert_eq!(9, partition.records_present);

    Ok(())
}

/// A compacted topic's gaps are what compaction looks like: per-key compaction
/// removes superseded keys, and a removed key *is* an offset gap. Counting them
/// as loss is how a first pass over this data overstated the damage — the gap
/// is still reported, but it must never reach the headline.
#[tokio::test]
async fn a_compacted_topic_gap_is_reported_but_not_counted() -> Result<(), Error> {
    let bucket = InMemory::new();
    three_windows(&bucket, &[("cleanup.policy", "compact")]).await?;

    let segments = segments(&bucket).await;
    assert_eq!(3, segments.len());
    bucket.delete(&segments[1]).await?;

    let report = audit(&bucket).await?;

    let topic = &report.topics[0];
    assert_eq!("compact", topic.cleanup_policy);
    assert!(topic.gaps_expected);

    // Still measured and still visible...
    assert_eq!(2, topic.records_lost());
    assert_eq!(1, topic.partitions[0].gaps.len());

    // ...and excluded from every number the headline is built from.
    assert_eq!(0, report.lost_records());
    assert_eq!(0, report.spanned_records());
    assert_eq!(0, report.damaged().count());

    Ok(())
}

/// A topic whose `topic-metadata` object is gone — deleted while its segments
/// outlived it, or a partial copy — is audited as `delete`, and says so. The
/// alternative is dropping it from the report, which reads as "clean".
#[tokio::test]
async fn a_topic_without_metadata_is_audited_as_delete() -> Result<(), Error> {
    let bucket = InMemory::new();
    three_windows(&bucket, &[]).await?;

    bucket
        .delete(&Path::from(format!(
            "clusters/{CLUSTER}/topic-metadata/{TOPIC}.json"
        )))
        .await?;
    bucket.delete(&segment_path(1)).await?;

    let report = audit(&bucket).await?;

    let topic = &report.topics[0];
    assert!(topic.metadata_missing);
    assert_eq!("delete", topic.cleanup_policy);
    assert!(!topic.gaps_expected);
    assert_eq!(2, report.lost_records());

    Ok(())
}

/// An object under `segments/` that carries no `TSEG` trailer is a squatter or
/// a truncated write (#157, #50), not the legacy v0 layout — nothing has ever
/// written a bare batch concatenation there. It must be a named fault rather
/// than a silent skip, because a segment the audit cannot read is a segment
/// whose offsets it cannot vouch for.
#[tokio::test]
async fn an_object_with_no_trailer_is_a_named_fault() -> Result<(), Error> {
    let bucket = InMemory::new();
    three_windows(&bucket, &[]).await?;

    _ = bucket
        .put(
            &segment_path(99),
            PutPayload::from(Bytes::from_static(b"not a segment")),
        )
        .await?;

    let report = audit(&bucket).await?;

    assert_eq!(3, report.segments);
    assert_eq!(1, report.segments_unreadable);
    assert_eq!(1, report.faults.len());
    assert_eq!(PREFIX, report.faults[0].prefix);
    assert_eq!(99, report.faults[0].seq);
    assert_eq!("no TSEG trailer", report.faults[0].detail);

    // The intact segments are still audited: one unreadable object does not
    // end the prefix's walk (#402 in the read path, the same rule here).
    assert_eq!(0, report.lost_records());

    Ok(())
}

/// Legacy `records/{offset}.batch` objects (#50) are counted and attributed,
/// never folded into coverage: since #179 the broker neither writes nor reads
/// them, so they are abandoned data, not offsets this sub-stream serves. A gap
/// at the head of a log that has them is theirs, which is exactly why the count
/// has to appear next to the gaps.
#[tokio::test]
async fn legacy_batches_are_counted_but_serve_no_offsets() -> Result<(), Error> {
    let bucket = InMemory::new();
    three_windows(&bucket, &[]).await?;

    for offset in [0u64, 1] {
        _ = bucket
            .put(
                &Path::from(format!(
                    "clusters/{CLUSTER}/topics/{TOPIC}/partitions/{:0>10}/records/{offset:0>20}.batch",
                    0
                )),
                PutPayload::from(Bytes::from_static(b"legacy")),
            )
            .await?;
    }

    let report = audit(&bucket).await?;

    assert_eq!(2, report.legacy_batches);
    assert_eq!(2, report.topics[0].partitions[0].legacy_batches);

    // Coverage is unchanged: the segments still say what is servable.
    assert_eq!(9, report.topics[0].partitions[0].records_present);
    assert_eq!(0, report.lost_records());

    Ok(())
}

/// The case the tool exists for: a copy of the bucket on local disk, audited
/// with no broker running and no credentials. `file://` is not a storage engine
/// the broker accepts — a deployment already past the damage cannot be measured
/// by starting one — so the audit resolves it itself.
#[tokio::test]
async fn an_offline_copy_on_local_disk_is_audited_through_a_file_url() -> Result<(), Error> {
    let copy = tempfile::tempdir()?;

    let bucket = LocalFileSystem::new_with_prefix(copy.path())?;
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    create_topic(&store, TOPIC, &[]).await?;

    let tp = Topition::new(TOPIC, 0);
    assert_eq!(0, store.produce(None, &tp, batch(3)?).await?);
    assert_eq!(3, store.produce(None, &tp, batch(2)?).await?);
    assert_eq!(5, store.produce(None, &tp, batch(4)?).await?);

    let mut located: Vec<Path> = bucket
        .list(None)
        .map_ok(|meta| meta.location)
        .try_filter(|location| {
            let is_segment = location.as_ref().ends_with(".seg");
            async move { is_segment }
        })
        .try_collect()
        .await?;
    located.sort();
    assert_eq!(3, located.len());

    bucket.delete(&located[1]).await?;

    let url = Url::from_directory_path(copy.path()).expect("a file:// url for the copy");
    let report = Audit::try_from_url(&url, CLUSTER)?.run().await?;

    assert_eq!(2, report.segments);
    assert_eq!(2, report.lost_records());
    assert_eq!(9, report.spanned_records());

    Ok(())
}

/// A footer entry that claims bytes past the end of its object (#393, #395) is
/// damage that *did* leave a trace: the read path meets it as a corrupt region
/// rather than as missing records. It has to be named — and the segment must
/// still contribute the offsets it describes, because those offsets are in an
/// object that exists. Suppressing them would turn one damaged segment into a
/// hole the bucket does not have.
#[tokio::test]
async fn a_footer_entry_claiming_bytes_past_the_object_is_a_fault() -> Result<(), Error> {
    let bucket = InMemory::new();
    three_windows(&bucket, &[]).await?;

    let located = segments(&bucket).await;
    let segment = bucket.get(&located[1]).await?.bytes().await?;

    // Locate the sole entry's `byte_len` in the v3 footer: writer_epoch (8) +
    // nonce (8), then topic_len (2) + topic + partition (4) + base_offset (8) +
    // record_count (8) + byte_start (8). See `docs/virtual-topics-format.md`.
    let trailer = &segment[segment.len() - 18..];
    let footer_len = u64::from_be_bytes(trailer[0..8].try_into()?) as usize;
    let footer_start = segment.len() - 18 - footer_len;
    let topic_len =
        u16::from_be_bytes(segment[footer_start + 16..footer_start + 18].try_into()?) as usize;
    let byte_len_at = footer_start + 16 + 2 + topic_len + 4 + 8 + 8 + 8;

    let mut forged = segment.to_vec();
    forged[byte_len_at..byte_len_at + 8].copy_from_slice(&u64::MAX.to_be_bytes());

    _ = bucket
        .put(&located[1], PutPayload::from(Bytes::from(forged)))
        .await?;

    let report = audit(&bucket).await?;

    // Readable, so it is a segment and not "unreadable" — and named.
    assert_eq!(3, report.segments);
    assert_eq!(0, report.segments_unreadable);
    assert_eq!(1, report.faults.len(), "{:?}", report.faults);
    assert!(
        report.faults[0].detail.contains("past the"),
        "{}",
        report.faults[0].detail
    );

    // The offsets it describes still count: nothing is missing from the bucket.
    assert_eq!(0, report.lost_records());
    assert_eq!(9, report.spanned_records());

    Ok(())
}

/// Every segment a healthy produce path writes tiles its body exactly, so the
/// structural checks must be silent on one. A check that fires on valid data is
/// worse than no check: an audit is only useful if a fault means something.
#[tokio::test]
async fn a_healthy_segment_raises_no_structural_fault() -> Result<(), Error> {
    let bucket = InMemory::new();

    // More than one sub-stream per segment, so the tiling check has adjacent
    // regions to walk rather than a single one starting at zero.
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());
    create_topic(&store, TOPIC, &[]).await?;
    create_topic(&store, "org.env.conn.tab_b", &[]).await?;

    let a = Topition::new(TOPIC, 0);
    let b = Topition::new("org.env.conn.tab_b", 0);

    let (a0, b0) = tokio::join!(
        store.produce(None, &a, batch(3)?),
        store.produce(None, &b, batch(2)?)
    );
    _ = a0?;
    _ = b0?;

    let report = audit(&bucket).await?;

    assert!(report.segments >= 1);
    assert!(report.faults.is_empty(), "{:?}", report.faults);
    assert_eq!(0, report.lost_records());

    Ok(())
}

fn slice(seq: u64, writer_epoch: i64, base_offset: i64, record_count: i64) -> Slice {
    Slice {
        prefix: String::from(PREFIX),
        seq,
        writer_epoch,
        size: 16 * 1024 * 1024,
        base_offset,
        record_count,
        max_timestamp: 0,
    }
}

/// The overlap rule of `docs/virtual-topics-format.md`: a merged segment and
/// the originals it merged claim the same offsets, and the merged one wins on
/// the higher sequence. Resolving them any other way would report the
/// *originals* as an overlap-free run and the merge as a gap.
#[test]
fn a_merged_segment_wins_over_the_originals_it_merged() {
    // Sequences 0 and 1 hold [0, 3) and [3, 5); sequence 7 is their merge.
    let audit = audit_partition(
        0,
        vec![
            slice(0, 1, 0, 3),
            slice(1, 1, 3, 2),
            slice(7, 1, 0, 5),
            slice(8, 1, 5, 4),
        ],
    );

    assert_eq!(0, audit.first_offset);
    assert_eq!(9, audit.next_offset);
    assert_eq!(0, audit.records_lost);
    assert!(audit.gaps.is_empty());
    assert_eq!(2, audit.overlaps_dropped, "the two merged originals");
    assert_eq!(0, audit.overlaps_clipped);
    assert_eq!(9, audit.records_present);
}

/// A slice that starts inside the covered range but reaches **past** it holds
/// offsets nothing else holds, and they are counted.
///
/// The reader's overlap rule says to drop such an entry, and that rule is right
/// for what it is written for — resolving *one offset*, where the
/// higher-priority entry already answers it. Applied to *coverage* it discards
/// the tail as well, and the sweep then reports that tail as lost.
///
/// This is not hypothetical: the first run of this audit against a production
/// bucket reported **27.5 % of the offset span lost**, and 3 210 of the 3 356
/// affected partitions also carried a dropped overlap. The holes were the
/// discarded tails.
#[test]
fn a_slice_reaching_past_the_frontier_is_clipped_not_dropped() {
    // [0, 100) then [50, 10_000): the second starts inside the first and
    // reaches far past it.
    let audit = audit_partition(0, vec![slice(0, 1, 0, 100), slice(1, 1, 50, 9_950)]);

    assert!(
        audit.gaps.is_empty(),
        "the second slice holds [100, 10_000): {:?}",
        audit.gaps,
    );

    assert_eq!(0, audit.next_offset - 10_000);
    assert_eq!(10_000, audit.span);
    assert_eq!(10_000, audit.records_present);
    assert_eq!(0, audit.records_lost);

    assert_eq!(1, audit.overlaps_clipped);
    assert_eq!(0, audit.overlaps_dropped);
}

/// And one that is wholly inside contributes nothing, which is the merged
/// segment's originals and must stay a *drop*. Counting its offsets again would
/// make `records_present` exceed the span.
#[test]
fn a_slice_wholly_inside_the_frontier_is_dropped() {
    let audit = audit_partition(0, vec![slice(7, 1, 0, 100), slice(0, 1, 10, 20)]);

    assert_eq!(1, audit.overlaps_dropped);
    assert_eq!(0, audit.overlaps_clipped);
    assert_eq!(100, audit.records_present);
    assert_eq!(0, audit.records_lost);
}

/// A tie on `base_offset` breaks on the higher `writer_epoch` before the
/// sequence: a fenced writer's segment must not be the one that sets coverage.
#[test]
fn a_higher_writer_epoch_wins_a_tie() {
    let audit = audit_partition(0, vec![slice(9, 1, 0, 2), slice(3, 4, 0, 5)]);

    assert_eq!(5, audit.next_offset, "the epoch-4 segment covers [0, 5)");
    assert_eq!(1, audit.overlaps_dropped);
    assert_eq!(0, audit.records_lost);
}

/// Loss below the lowest surviving segment leaves nothing to bracket it, so the
/// span starts there and the audit reports zero. This is why the number is a
/// floor: a head that is gone is a head that was never seen.
#[test]
fn loss_below_the_first_segment_is_invisible() {
    let audit = audit_partition(0, vec![slice(4, 1, 1_000, 5)]);

    assert_eq!(1_000, audit.first_offset);
    assert_eq!(1_005, audit.next_offset);
    assert_eq!(5, audit.span);
    assert_eq!(0, audit.records_lost);
}

/// A sub-stream with no slices at all (only abandoned legacy objects) has no
/// span, so it cannot have lost anything measurable — reporting a span of zero
/// keeps it out of the percentage instead of dividing by it.
#[test]
fn a_substream_with_no_slices_has_no_span() {
    let audit = audit_partition(0, vec![]);

    assert_eq!(0, audit.span);
    assert_eq!(0, audit.records_lost);
    assert!(audit.gaps.is_empty());
}

/// An entry covering no offsets cannot bracket a hole and cannot advance
/// coverage. Letting it set `covered` would make the next real slice look like
/// it starts after a gap.
#[test]
fn a_zero_record_entry_neither_covers_nor_gaps() {
    let audit = audit_partition(
        0,
        vec![slice(0, 1, 0, 3), slice(1, 1, 3, 0), slice(2, 1, 3, 2)],
    );

    assert_eq!(5, audit.next_offset);
    assert_eq!(0, audit.records_lost);
    assert!(audit.gaps.is_empty());
}

#[test]
fn segment_coordinates_come_from_the_path() {
    assert_eq!(
        Some((String::from(PREFIX), 42)),
        segment_coordinates(&segment_path(42))
    );

    // Everything else under `prefixes/` is not a segment.
    for other in [
        format!("clusters/{CLUSTER}/prefixes/{PREFIX}/era.json"),
        format!("clusters/{CLUSTER}/prefixes/{PREFIX}/segments/nonsense.seg"),
        format!("clusters/{CLUSTER}/topics/{TOPIC}/partitions/0000000000/watermark.json"),
    ] {
        assert_eq!(
            None,
            segment_coordinates(&Path::from(other.as_str())),
            "{other}"
        );
    }
}

#[test]
fn cleanup_policy_is_read_out_of_the_metadata_document() {
    let document = serde_json::json!({
        "id": "00000000-0000-0000-0000-000000000000",
        "topic": {
            "name": TOPIC,
            "configs": [
                {"name": "retention.ms", "value": "604800000"},
                {"name": "cleanup.policy", "value": "compact"},
            ],
        },
    });

    assert_eq!(Some(String::from("compact")), cleanup_policy(&document));

    // A topic that stores no policy, and a document shaped unlike this build's,
    // both read as "no stored policy" — which the caller resolves to `delete`.
    assert_eq!(
        None,
        cleanup_policy(&serde_json::json!({"topic": {"configs": []}}))
    );
    assert_eq!(None, cleanup_policy(&serde_json::json!({"unexpected": 1})));
}
