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

//! A footer entry that claims bytes the object does not hold (#397).
//!
//! #395 shipped the write-side invariant and said the one thing that had to be
//! said: it does not repair the regions already on the fleet. `1.0.0-alpha.3`
//! then answered what those cost when something reads them — the arm #395
//! measured as zero. Not `corrupt` but `truncated`: 313 in one 29-minute window
//! on `*.connect.ibmi-offsets`, `read_len` pinned at exactly **199** across 216
//! distinct sequences against a `byte_len` of 321. A constant over-claim across
//! hundreds of objects is not per-object corruption, and the arm returned the
//! batches it could decode and dropped the rest of the region.
//!
//! Segments are immutable and created atomically, so a ranged GET cannot return
//! fewer bytes than the object holds over that range: a short read means the
//! entry the read was issued from and the object disagree. And
//! `encode_segment_v3` measures `byte_len` from the bytes it just appended, so
//! it cannot over-claim for the payload it built. Two things can produce the
//! pairing, and the trailer tells them apart in one suffix GET:
//!
//! 1. **the index is wrong** — the reader locates a region from the in-memory
//!    prefix index, not from the object's trailer, so an entry describing a
//!    different payload is a *cache* fault. The trailer is the authority (#64,
//!    #60): take it, and the region reads whole.
//! 2. **the object is wrong** — the trailer says exactly what the index said and
//!    the object is still short of it. Nothing in the bucket can serve those
//!    offsets, so say so instead of serving part of the region as the whole
//!    thing.
//!
//! The discriminator has not been run against a production object, so which case
//! the fleet's ~900 damaged segments are in is still unrecorded. These tests pin
//! both answers, which is what makes shipping the read path correct without it.

use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{
    ObjectStore as _, ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path,
};
use tansu_sans_io::{
    ErrorCode, IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage as _, Topition,
    dynostore::{
        CoalesceTuning, DynoStore, SEGMENT_FORMAT_VERSION_V3, SEGMENT_MAGIC, SegmentFooter,
    },
    storage_error_code,
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

const PREFIX: &str = "org.env.conn";
const TOPIC: &str = "org.env.conn.table";

fn new_store(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone()).coalesce_tuning(CoalesceTuning {
        prefix_compact_min_segments: Some(2),
        prefix_compact_keep_hot: Some(0),
        prefix_compact_target_bytes: Some(1 << 20),
        ..Default::default()
    })
}

fn batch(value: &str) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::copy_from_slice(value.as_bytes()))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(store: &DynoStore, name: &str) -> Result<()> {
    _ = store
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
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

async fn segments(bucket: &InMemory) -> Vec<Path> {
    let mut paths = bucket
        .list(Some(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{PREFIX}/segments/"
        ))))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .expect("list segments");
    paths.sort();
    paths
}

async fn fetch_from(store: &DynoStore, tp: &Topition, offset: i64) -> Result<Vec<deflated::Batch>> {
    store
        .fetch(
            tp,
            offset,
            0,
            100_000,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(200),
        )
        .await
}

/// The whole `[footer || trailer]` for `footer`, at v3.
fn trailered(footer: &SegmentFooter) -> Vec<u8> {
    let encoded = DynoStore::encode_footer(footer, SEGMENT_FORMAT_VERSION_V3);

    let mut tail = encoded.clone();
    tail.extend_from_slice(&(encoded.len() as u64).to_be_bytes());
    tail.extend_from_slice(&(footer.entries.len() as u32).to_be_bytes());
    tail.extend_from_slice(&SEGMENT_FORMAT_VERSION_V3.to_be_bytes());
    tail.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());
    tail
}

/// Rewrite segment `seq` so its **own** trailer claims `extra` bytes more for
/// the sub-stream than the object holds, leaving the record body untouched.
///
/// This is case 2 built by hand — the object is the liar, so index and trailer
/// agree and the read has nowhere else to look. The fleet's numbers are
/// `byte_len: 321` over a `read_len: 199`, so `extra` stands in for the 122
/// bytes the entry claims past the end.
async fn make_over_claiming(bucket: &InMemory, seq: u64, extra: u64) -> Result<()> {
    let location = segment_path(seq);
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone());

    let mut footer = store
        .read_segment_footer(&location)
        .await?
        .expect("a segment trailer");

    let body: u64 = footer.entries.iter().map(|entry| entry.byte_len).sum();
    for entry in &mut footer.entries {
        entry.byte_len += extra;
    }

    let object = bucket.get(&location).await?.bytes().await?;
    let mut rewritten = object.slice(..body as usize).to_vec();
    rewritten.extend_from_slice(&trailered(&footer));

    _ = bucket
        .put(&location, PutPayload::from(Bytes::from(rewritten)))
        .await?;

    Ok(())
}

/// Case 1: the index serves an entry that does not belong to the object.
///
/// The object is intact; only the cached entry over-claims, exactly as an entry
/// from before a rewrite would against the payload after it. The read is short,
/// the trailer disagrees with the index, and the trailer wins — so the region
/// reads **whole**, which is what the truncated arm silently gave up on.
#[tokio::test]
async fn a_stale_index_entry_loses_to_the_objects_own_trailer() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    _ = store.produce(None, &tp, batch("v0")?).await?;
    assert_eq!(1, segments(&bucket).await.len());

    // Poison this process's cached footer for the segment: the same entry,
    // claiming 122 bytes it does not have — the fleet's own over-claim.
    let mut footer = store
        .read_segment_footer(&segment_path(0))
        .await?
        .expect("a segment trailer");
    let honest = footer.entries[0].byte_len;
    footer.entries[0].byte_len = honest + 122;
    store.index_insert(PREFIX, 0, footer, 0)?;

    // The read used to return whatever fitted and drop the rest. Now it resolves
    // against the trailer and serves the region.
    let fetched = fetch_from(&store, &tp, 0).await?;
    assert_eq!(1, fetched.len());
    assert_eq!(0, fetched[0].base_offset);

    // And the index it left behind is the object's own, so the next read costs
    // nothing extra.
    assert_eq!(
        honest,
        store
            .read_segment_footer(&segment_path(0))
            .await?
            .expect("a segment trailer")
            .entries[0]
            .byte_len
    );
    assert_eq!(1, fetch_from(&store, &tp, 0).await?.len());

    Ok(())
}

/// Case 2: the object is short of what its own trailer claims.
///
/// Index and trailer agree, so there is no other authority to consult and the
/// bytes are simply not there. The read answers `CORRUPT_MESSAGE` rather than
/// returning part of the region and calling it the whole thing — which on
/// `*.connect.ibmi-offsets` is a connector resuming from an offset map with
/// holes in it and nothing in its own logs to say so.
#[tokio::test]
async fn an_object_short_of_its_own_trailer_is_answered_corrupt() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    _ = store.produce(None, &tp, batch("v0")?).await?;
    make_over_claiming(&bucket, 0, 122).await?;

    // A fresh store, so the index is built from the object's own trailer and the
    // two genuinely agree.
    let reader = new_store(&bucket);
    let error = fetch_from(&reader, &tp, 0)
        .await
        .expect_err("an object short of its own footer");

    let Error::CorruptSegment(region) = error else {
        panic!("{error:?}");
    };

    assert_eq!(PREFIX, region.prefix);
    assert_eq!(0, region.seq);
    assert_eq!(TOPIC, region.topic);
    assert!(
        (region.read_len as u64) < region.byte_len,
        "the discriminator: {region:?}"
    );

    // And the read returned everything the object had from `byte_start` on —
    // footer and trailer bytes included, which is why the fleet reports a
    // `read_len` well above the record bytes the region actually holds. The
    // shortfall is the claim past the end of the object, not the claim past the
    // end of the records.
    let object_len = bucket.get(&segment_path(0)).await?.bytes().await?.len() as u64;
    assert_eq!(object_len - region.byte_start, region.read_len as u64);

    assert_eq!(
        ErrorCode::CorruptMessage,
        storage_error_code(&Error::CorruptSegment(region))
    );

    Ok(())
}

/// The same damage on the compaction side, and the reason it is an error there
/// rather than a skip.
///
/// A region whose extent runs past its object used to be dropped from the
/// merge — while `retire_segments` deleted the original it could still have been
/// read from, and the merged region carried the base offset of its first region
/// with everything above the hole slid down into it. Now the run fails, #398's
/// quarantine excludes the object, and the drain proceeds over what is readable
/// with every surviving offset still its own.
#[tokio::test]
async fn a_region_running_past_its_object_never_shifts_the_offsets_above_it() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = new_store(&bucket);
    create_topic(&store, TOPIC).await?;
    let tp = Topition::new(TOPIC, 0);

    for i in 0..6 {
        _ = store.produce(None, &tp, batch(&format!("v{i}"))?).await?;
    }
    make_over_claiming(&bucket, 2, 122).await?;

    // A fresh store: this one's cached footers predate the rewrite, and the
    // point is what a replica reading the object as it now stands does.
    let maintainer = new_store(&bucket);

    for _ in 0..4 {
        _ = maintainer.drain_compact_prefix(PREFIX).await;
    }

    // The damaged object is excluded, not merged away.
    assert_eq!(
        [2].into_iter().collect::<std::collections::BTreeSet<u64>>(),
        maintainer.quarantined_segments_of(PREFIX)?
    );
    assert!(segments(&bucket).await.contains(&segment_path(2)));

    // And nothing above the hole moved.
    assert_eq!(
        vec![3, 4, 5],
        fetch_from(&new_store(&bucket), &tp, 3)
            .await?
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<_>>()
    );

    Ok(())
}
