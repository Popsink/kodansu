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

//! Prefix-coalesced segment frame + self-describing footer (#64): a single
//! `.seg` object multiplexes batches from many `(topic, partition)` sub-streams
//! and carries a trailing footer index (offset range, byte range, max
//! timestamp per sub-stream) plus a fixed trailer. A reader locates and decodes
//! any sub-stream from the footer alone — never from the object filename — and
//! legacy single-topic coalesced objects (#50, the v0 case) still decode as a
//! bare batch concatenation.

use bytes::Bytes;
use object_store::{PutPayload, memory::InMemory};
use tansu_sans_io::record::{Record, deflated, inflated};

use crate::{
    Error, Result, Topition,
    dynostore::{DynoStore, SEGMENT_TRAILER_LEN},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

fn store() -> DynoStore {
    DynoStore::new(CLUSTER, NODE, InMemory::new())
}

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

/// Encode three sub-streams (two topics, one with two partitions) into one
/// segment; the footer must locate each with the right offset span and byte
/// range, and each region must decode back to exactly the batches put in.
#[tokio::test]
async fn round_trips_multiple_substreams() -> Result<(), Error> {
    let store = store();

    let alpha0 = Topition::new("alpha", 0);
    let alpha1 = Topition::new("alpha", 1);
    let beta0 = Topition::new("beta", 0);

    // Distinct offset spans per sub-stream, assigned independently (#58): the
    // filename is a segment sequence, not an offset.
    let substreams = vec![
        (alpha0.clone(), 40, vec![batch(2)?, batch(3)?]), // 5 records
        (alpha1.clone(), 7, vec![batch(1)?]),             // 1 record
        (beta0.clone(), 900, vec![batch(4)?]),            // 4 records
    ];

    let (payload, footer) = store.encode_segment(&substreams, 7)?;
    let segment = Bytes::from(payload);

    // The returned footer is the durable index, stamped with the writer epoch.
    assert_eq!(7, footer.writer_epoch);
    assert_eq!(3, footer.entries.len());

    let a0 = footer.get("alpha", 0).expect("alpha-0 entry");
    assert_eq!(40, a0.base_offset);
    assert_eq!(5, a0.record_count);

    let a1 = footer.get("alpha", 1).expect("alpha-1 entry");
    assert_eq!(7, a1.base_offset);
    assert_eq!(1, a1.record_count);

    let b0 = footer.get("beta", 0).expect("beta-0 entry");
    assert_eq!(900, b0.base_offset);
    assert_eq!(4, b0.record_count);

    // Regions are contiguous and non-overlapping, laid end to end.
    assert_eq!(0, a0.byte_start);
    assert_eq!(a0.byte_start + a0.byte_len, a1.byte_start);
    assert_eq!(a1.byte_start + a1.byte_len, b0.byte_start);

    // The footer recovered from the object equals the one returned by encode.
    let decoded = store
        .decode_segment_footer(&segment)?
        .expect("segment must carry a footer");
    assert_eq!(footer, decoded);

    // Each sub-stream's byte range decodes to exactly its batches — no
    // cross-topic data in the range (the #60 ranged-GET contract).
    for entry in &decoded.entries {
        let start = entry.byte_start as usize;
        let end = start + entry.byte_len as usize;
        let batches = store.decode_frame(segment.slice(start..end))?;
        let records: i64 = batches.iter().map(|b| b.last_offset_delta as i64 + 1).sum();
        assert_eq!(entry.record_count, records);
    }

    Ok(())
}

/// The footer must be recoverable from the tail alone (footer + trailer), the
/// slice a #60 reader gets from a ranged GET of the last N bytes — no need to
/// download the record body.
#[tokio::test]
async fn footer_recovered_from_tail_only() -> Result<(), Error> {
    let store = store();

    let substreams = vec![
        (Topition::new("alpha", 0), 0, vec![batch(3)?]),
        (Topition::new("beta", 0), 10, vec![batch(2)?]),
    ];

    let (payload, footer) = store.encode_segment(&substreams, 0)?;
    let segment = Bytes::from(payload);

    // Trailer carries footer_len; a reader takes the last SEGMENT_TRAILER_LEN
    // bytes, reads footer_len, then GETs footer_len + trailer. Emulate that by
    // slicing a tail that starts well after the record body.
    let body_len = footer.entries.iter().map(|e| e.byte_len).sum::<u64>() as usize;
    let tail = segment.slice(body_len..);

    let decoded = store
        .decode_segment_footer(&tail)?
        .expect("footer from tail");
    assert_eq!(footer, decoded);

    Ok(())
}

/// A legacy single-topic coalesced object (#50) has no trailer: the footer probe
/// returns `None` (so the read path falls back to a bare batch concatenation)
/// and `decode_frame` still returns the original batches unchanged.
#[tokio::test]
async fn legacy_object_has_no_footer() -> Result<(), Error> {
    let store = store();

    let batches = vec![batch(2)?, batch(3)?];
    let payload: PutPayload = store.encode_frame(&batches)?;
    let object = Bytes::from(payload);

    assert!(store.decode_segment_footer(&object)?.is_none());

    let decoded = store.decode_frame(object)?;
    assert_eq!(2, decoded.len());
    assert_eq!(1, decoded[0].last_offset_delta); // 2 records
    assert_eq!(2, decoded[1].last_offset_delta); // 3 records

    Ok(())
}

/// A byte string too short to hold a trailer is not a segment — the probe must
/// not panic or misread, it returns `None`.
#[tokio::test]
async fn tail_shorter_than_trailer_is_not_a_segment() -> Result<(), Error> {
    let store = store();
    let tiny = Bytes::from(vec![0u8; SEGMENT_TRAILER_LEN - 1]);
    assert!(store.decode_segment_footer(&tiny)?.is_none());
    Ok(())
}
