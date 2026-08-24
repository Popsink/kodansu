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

//! A segment region that is not the batch frames its footer entry claims (#386).
//!
//! On the fleet this arrived as a `TryFromIntError` on a compacted topic's
//! `Fetch` and cost the *connection*: a negative `batch_length` read at the head
//! of a region `?`-ed out of the decode, out of the storage fetch, out of the
//! whole request, and the broker closed the socket with no response written. A
//! client got no error code, no partition attribution and nothing to retry
//! selectively, so it reconnected and replayed the same fetch — the wedge of
//! #219, on exactly the compacted offsets topics a Connect worker reads from 0
//! before it can start.
//!
//! Two failures, pinned separately here: the *mapping* (one partition's damage
//! must not decide whether the request is answered or the connection stays open,
//! the #290 rule beyond `Error::Api`), and the *classification* (a length no
//! frame can carry stops the scan and is reported with the segment it came from,
//! rather than raising an error carrying nothing).

use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{
    ObjectStore as _, ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path,
};
use rama::{Context, Service as _};
use tansu_sans_io::{
    ErrorCode, FetchRequest, IsolationLevel,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    fetch_request::{FetchPartition, FetchTopic},
    record::{Record, deflated, inflated},
};

use crate::{
    Error, FetchService, Result, Storage as _, Topition,
    dynostore::{DynoStore, FrameTail, SubstreamEntry},
    storage_error_code,
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// A compacted topic, prefix-routed under its own name — the shape of the
/// `<prefix>.connect.*-offsets` topics the incident was on.
const OFFSETS: &str = "org.env.conn.offsets";

/// A plain topic, under the shared connector prefix: the healthy partition
/// sharing a client's `poll()` with the damaged one.
const TABLE: &str = "org.env.conn.table";

fn store(bucket: &InMemory) -> DynoStore {
    DynoStore::new(CLUSTER, NODE, bucket.clone())
}

fn keyed_batch(key: &'static [u8], value: &'static [u8]) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(key)))
                .value(Some(Bytes::from_static(value))),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(store: &DynoStore, name: &str, configs: &[(&str, &str)]) -> Result<()> {
    let configs: Vec<CreatableTopicConfig> = configs
        .iter()
        .map(|(k, v)| {
            CreatableTopicConfig::default()
                .name((*k).to_owned())
                .value(Some((*v).to_owned()))
        })
        .collect();

    _ = store
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

/// A frame header: `base_offset (i64) + batch_length (i32)`, big-endian.
fn frame_header(base_offset: i64, batch_length: i32) -> Vec<u8> {
    let mut header = Vec::new();
    header.extend_from_slice(&base_offset.to_be_bytes());
    header.extend_from_slice(&batch_length.to_be_bytes());
    header
}

/// A one-sub-stream segment and the footer entry locating its region.
fn segment_with_one_region(store: &DynoStore) -> Result<(Bytes, SubstreamEntry)> {
    let tp = Topition::new(OFFSETS, 0);

    let (payload, footer) =
        store.encode_segment_v3(&[(tp.clone(), 0, vec![keyed_batch(b"k", b"v")?])], 0, 0)?;

    let entry = footer
        .get(tp.topic(), tp.partition())
        .expect("sub-stream entry")
        .clone();

    Ok((Bytes::from(payload), entry))
}

/// Replace the `batch_length` at the head of `entry`'s region, leaving every
/// other byte — the footer above all — as it was.
///
/// This is the *first* of #386's two candidate causes reproduced exactly: a whole
/// object whose index entry no longer describes it, as a segment rewrite that
/// recorded a byte range against a different payload layout would leave it. The
/// second (a torn or partially-visible object) is a short read, and is
/// [`a_short_read_is_not_read_as_damage`].
fn overwrite_head_length(segment: &Bytes, entry: &SubstreamEntry, declared: i32) -> Bytes {
    let at = entry.byte_start as usize + size_of::<i64>();

    let mut bytes = segment.to_vec();
    bytes[at..at + size_of::<i32>()].copy_from_slice(&declared.to_be_bytes());

    Bytes::from(bytes)
}

/// A negative `batch_length` at the head of a region stops the scan and says so.
/// It used to `?` out as a `TryFromIntError`, which is how a decode failure
/// became a connection's problem.
#[tokio::test]
async fn a_negative_length_stops_the_scan_instead_of_raising() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let (batches, tail) = store(&InMemory::new()).decode_frame(Bytes::from(frame_header(0, -1)))?;

    assert!(batches.is_empty());
    assert_eq!(
        FrameTail::Malformed {
            at: 0,
            declared: -1
        },
        tail
    );

    Ok(())
}

/// The guard is symmetric: a length that overruns what remains and a length no
/// frame can carry are the same finding, at the same place, and neither is an
/// error on its own. Only where the region *started* decides that.
#[tokio::test]
async fn an_overrunning_length_is_the_same_finding_as_a_negative_one() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let (batches, tail) =
        store(&InMemory::new()).decode_frame(Bytes::from(frame_header(0, i32::MAX)))?;

    assert!(batches.is_empty());
    assert_eq!(
        FrameTail::Malformed {
            at: 0,
            declared: i32::MAX
        },
        tail
    );

    Ok(())
}

/// Trailing bytes that cannot form another whole batch stay ignored, whole
/// batches ahead of them still decode: the documented contract, which is what
/// makes it safe to treat a region that decodes to *nothing* as damage.
#[tokio::test]
async fn a_malformed_tail_after_a_whole_batch_is_still_ignored() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let store = store(&InMemory::new());

    let mut bytes = Bytes::from(store.encode_frame(&[keyed_batch(b"k", b"v")?])?).to_vec();
    let at = bytes.len();
    bytes.extend_from_slice(&frame_header(0, -1));

    let (batches, tail) = store.decode_frame(Bytes::from(bytes))?;

    assert_eq!(1, batches.len());
    assert_eq!(FrameTail::Malformed { at, declared: -1 }, tail);

    Ok(())
}

/// A region that arrived whole and holds no frame is damage, and the error names
/// the segment: prefix, sequence, sub-stream, the byte range claimed, the bytes
/// actually read and the header bytes found where a frame should start.
///
/// `read_len == byte_len` is the discriminator the incident lacked — the object
/// was all there, so what is wrong is the footer entry, not the object.
#[tokio::test]
async fn a_whole_region_holding_no_frame_is_reported_with_its_segment() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let store = store(&InMemory::new());
    let (segment, entry) = segment_with_one_region(&store)?;

    let corrupted = overwrite_head_length(&segment, &entry, -1);
    let start = entry.byte_start as usize;
    let end = start + entry.byte_len as usize;

    let error = store
        .decode_region(OFFSETS, 7, &entry, corrupted.slice(start..end))
        .expect_err("a region that holds no frame");

    let Error::CorruptSegment(region) = error else {
        panic!("{error:?}");
    };

    assert_eq!(OFFSETS, region.prefix);
    assert_eq!(7, region.seq);
    assert_eq!(OFFSETS, region.topic);
    assert_eq!(0, region.partition);
    assert_eq!(entry.byte_start, region.byte_start);
    assert_eq!(entry.byte_len, region.byte_len);
    assert_eq!(entry.byte_len as usize, region.read_len);
    assert_eq!(0, region.at);
    assert_eq!(Some(-1), region.declared);
    assert_eq!("0000000000000000ffffffff", region.head);

    // What the client is told, which is the whole point of naming the class:
    // these offsets are unreadable here, not "unknown server error".
    assert_eq!(
        ErrorCode::CorruptMessage,
        storage_error_code(&Error::CorruptSegment(region))
    );

    Ok(())
}

/// A read that came back short of the footer's extent is the other candidate
/// cause — and it is damage too (#397).
///
/// It used to return the batches it managed to decode and drop the rest of the
/// region, on the reasoning that a torn or partially-visible object should stay
/// the bounded, error-free empty read of #290. The fleet then produced 313 of
/// them in 29 minutes on `*.connect.ibmi-offsets`, with `read_len` pinned at
/// exactly 199 across 216 distinct sequences: a constant over-claim, not tearing.
/// A consumer served part of an offsets region and told nothing is a connector
/// resuming from an offset map with holes in it.
///
/// Which side is wrong is resolved against the object's own trailer *before*
/// this — see `short_region.rs`. By the time a short read reaches here it has
/// nowhere left to go.
#[tokio::test]
async fn a_short_read_is_damage_too() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let store = store(&InMemory::new());
    let (segment, entry) = segment_with_one_region(&store)?;

    let start = entry.byte_start as usize;

    // Short of a frame header, as a ranged GET past the end of an object returns
    // it.
    let error = store
        .decode_region(OFFSETS, 7, &entry, segment.slice(start..start + 4))
        .expect_err("a region short of its extent");

    let Error::CorruptSegment(region) = error else {
        panic!("{error:?}");
    };

    // `read_len < byte_len` is the discriminator, and it is what the diagnostic
    // leads with.
    assert_eq!(entry.byte_len, region.byte_len);
    assert_eq!(4, region.read_len);
    assert_eq!(
        ErrorCode::CorruptMessage,
        storage_error_code(&Error::CorruptSegment(region))
    );

    // And short mid-frame, where a length *is* read but its body is not there:
    // the same finding, not a partial region.
    let truncated = entry.byte_len as usize / 2;
    assert!(matches!(
        store.decode_region(OFFSETS, 7, &entry, segment.slice(start..start + truncated)),
        Err(Error::CorruptSegment(_))
    ));

    Ok(())
}

/// An intact region is unchanged by any of it: the frames decode, the scan
/// consumes every byte of the extent.
#[tokio::test]
async fn an_intact_region_decodes_to_its_batches() -> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let store = store(&InMemory::new());
    let (segment, entry) = segment_with_one_region(&store)?;

    let start = entry.byte_start as usize;
    let end = start + entry.byte_len as usize;

    let batches = store.decode_region(OFFSETS, 7, &entry, segment.slice(start..end))?;

    assert_eq!(1, batches.len());
    assert_eq!(entry.record_count, i64::from(batches[0].record_count));

    Ok(())
}

/// Damage the head of the only region under `topic`'s prefix, in the object
/// store, behind the broker's back — leaving the footer index the broker already
/// holds pointing where it always did.
async fn corrupt_stored_region(bucket: &InMemory, store: &DynoStore, topic: &str) -> Result<()> {
    let listing = Path::from(format!("clusters/{CLUSTER}/prefixes/{topic}/segments/"));

    let locations: Vec<Path> = bucket
        .list(Some(&listing))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await?;
    assert_eq!(1, locations.len());

    let segment = bucket.get(&locations[0]).await?.bytes().await?;
    let footer = store
        .decode_segment_footer(&segment)?
        .expect("segment must carry a footer");
    let entry = footer.get(topic, 0).expect("sub-stream entry");

    _ = bucket
        .put(
            &locations[0],
            PutPayload::from(overwrite_head_length(&segment, entry, -1)),
        )
        .await?;

    Ok(())
}

/// The mapping, end to end through the service a client actually talks to: one
/// partition's damaged region answers *that partition* `CORRUPT_MESSAGE`, the
/// request is still answered, and every other partition in it is served.
///
/// Both halves matter. Without the second, a consumer's `poll()` over an
/// assignment that happens to include one damaged partition stops delivering
/// everything else (#290). Without the first, there is no response at all: the
/// broker drops the connection, and a client that cannot distinguish that from
/// the peer closing mid-frame just reconnects and replays the same fetch (#219).
#[tokio::test]
async fn a_corrupt_region_fails_its_partition_and_the_request_is_still_answered()
-> Result<(), Error> {
    let _guard = super::init_tracing()?;

    let bucket = InMemory::new();
    let store = store(&bucket);

    create_topic(&store, OFFSETS, &[("cleanup.policy", "compact")]).await?;
    create_topic(&store, TABLE, &[]).await?;

    let offsets = Topition::new(OFFSETS, 0);
    let table = Topition::new(TABLE, 0);

    assert_eq!(
        0,
        store
            .produce(None, &offsets, keyed_batch(b"k", b"v")?)
            .await?
    );
    assert_eq!(
        0,
        store
            .produce(None, &table, keyed_batch(b"k", b"v")?)
            .await?
    );

    corrupt_stored_region(&bucket, &store, OFFSETS).await?;

    const MAX_BYTES: i32 = 64 * 1024;

    let fetched = |topic: &str| {
        FetchTopic::default()
            .topic(Some(topic.to_owned()))
            .partitions(Some(
                [FetchPartition::default()
                    .partition(0)
                    .fetch_offset(0)
                    .partition_max_bytes(MAX_BYTES)]
                .into(),
            ))
    };

    // The damaged topic first: a request is answered as a whole or not at all, so
    // ordering it ahead of the healthy one is what proves the healthy one is not
    // collateral.
    let response = FetchService
        .serve(
            Context::with_state(store.clone()),
            FetchRequest::default()
                .max_wait_ms(500)
                .min_bytes(1)
                .max_bytes(Some(MAX_BYTES))
                .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
                .topics(Some([fetched(OFFSETS), fetched(TABLE)].into())),
        )
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(2, responses.len());

    let partitions = |topic: &str| {
        responses
            .iter()
            .find(|response| response.topic.as_deref() == Some(topic))
            .and_then(|response| response.partitions.clone())
            .unwrap_or_default()
    };

    let damaged = partitions(OFFSETS);
    assert_eq!(1, damaged.len());
    assert_eq!(
        ErrorCode::CorruptMessage,
        ErrorCode::try_from(damaged[0].error_code)?
    );
    assert!(damaged[0].records.is_none());

    let healthy = partitions(TABLE);
    assert_eq!(1, healthy.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(healthy[0].error_code)?);
    assert_eq!(
        1,
        healthy[0]
            .records
            .as_ref()
            .map(|frame| frame.batches.len())
            .unwrap_or_default()
    );

    // And the storage read still refuses to serve the damaged offsets, rather
    // than quietly returning nothing for them — which would stall a consumer at
    // this offset with no signal anywhere.
    assert!(matches!(
        store
            .fetch(
                &offsets,
                0,
                1,
                MAX_BYTES as u32,
                IsolationLevel::ReadUncommitted,
                Duration::from_millis(500),
            )
            .await,
        Err(Error::CorruptSegment(_))
    ));

    Ok(())
}
