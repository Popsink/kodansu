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

//! The write-side half of #386: a batch that misdeclares its length must never
//! reach a segment (#393).
//!
//! #388 taught the read path to answer damage instead of dropping the
//! connection, and its diagnostic then named the cause on the fleet: nine
//! corrupt regions, zero truncated, `read_len == byte_len` every time — whole
//! objects whose footer entry did not describe them. Each failing region began
//! at `byte_start: 0` with a frame declaring more bytes than the entry covered.
//!
//! There is exactly one way that pairing can be produced, and it is not the
//! footer's fault. `encode_segment_v3` measures `byte_len` from the bytes it
//! writes, so it cannot over-claim; but `From<Batch> for Bytes` writes
//! `batch_length` from the *field*, so a batch carrying a length that does not
//! match its `record_data` serialises a frame that lies. The pre-v2 husk the
//! decoder returns for `magic != 2` is such a batch: the wire's `batch_length`
//! over an empty payload.
//!
//! So the invariant belongs where the footer is built. These tests pin it there,
//! and pin the shape that motivated it.

use bytes::Bytes;
use object_store::memory::InMemory;
use tansu_sans_io::{
    ErrorCode,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Topition,
    dynostore::{DynoStore, Substream},
    storage_error_code,
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const TOPIC: &str = "org.env.conn.table";

fn store() -> DynoStore {
    DynoStore::new(CLUSTER, NODE, InMemory::new())
}

fn batch(value: &'static [u8]) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(value))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// A magic-0 MessageSet, decoded the way a request body is decoded — the husk
/// itself, not an imitation of one.
///
/// Same fixture as the produce path's refusal test, for the same reason: no
/// builder in this workspace can emit a record format the broker does not
/// support, so the bytes have to be written out.
fn pre_v2_husk() -> Result<deflated::Batch> {
    deflated::Batch::try_from(Bytes::from_static(&[
        0, 0, 0, 0, 0, 0, 0, 0, // base offset
        0, 0, 0, 80, // message size
        14, 140, 97, 161, // legacy CRC-32
        0,   // magic
        0,   // attributes
        255, 255, 255, 255, // null key
        0, 0, 0, 66, // value length
        181, 164, 112, 10, 42, 24, 68, 168, 93, 201, 190, 85, 75, 81, 82, 227, 134, 137, 91, 20,
        86, 4, 92, 187, 141, 103, 65, 71, 241, 103, 73, 174, 19, 227, 180, 158, 176, 4, 27, 78, 34,
        140, 106, 1, 209, 63, 255, 52, 206, 164, 132, 184, 32, 34, 45, 24, 162, 18, 187, 77, 19, 3,
        161, 102, 20, 14,
    ]))
    .map_err(Into::into)
}

#[test]
fn a_pre_v2_husk_does_not_declare_its_own_length() -> Result<()> {
    let husk = pre_v2_husk()?;

    // The precondition: this is the husk, not some other malformed batch.
    assert_eq!(0, husk.magic);
    assert!(!husk.is_record_batch_v2());
    assert!(husk.record_data.is_empty());

    // The wire said 80 bytes of MessageSet; the struct holds none of them.
    assert_eq!(80, husk.batch_length);
    assert!(!husk.declares_its_own_length());

    // And what it *would* serialise to is the fixed header alone: the 49 bytes
    // from `partition_leader_epoch` through `record_count`, and nothing after.
    assert_eq!(49, husk.encoded_batch_length()?);

    Ok(())
}

#[test]
fn a_batch_built_from_records_declares_its_own_length() -> Result<()> {
    let batch = batch(b"v")?;

    assert!(batch.declares_its_own_length());
    assert_eq!(batch.batch_length, batch.encoded_batch_length()?);

    Ok(())
}

/// The invariant. A batch whose header lies is refused *at the encoder*, so no
/// footer entry can be built over it.
#[test]
fn encode_segment_v3_refuses_a_batch_that_misdeclares_its_length() -> Result<()> {
    let store = store();
    let tp = Topition::new(TOPIC, 0);

    // A well-formed batch with its length field corrupted: the divergence
    // isolated from every other property of the husk, so the assertion below
    // cannot pass for the wrong reason.
    let mut divergent = batch(b"v")?;
    let honest = divergent.batch_length;
    divergent.batch_length = honest + 712;

    match store.encode_segment_v3(&[(tp.clone(), 2_406_599, vec![divergent])], 0, 0) {
        Err(Error::DivergentBatch(divergent)) => {
            assert_eq!(TOPIC, divergent.topic);
            assert_eq!(0, divergent.partition);
            assert_eq!(2_406_599, divergent.base_offset);
            assert_eq!(0, divergent.index);
            assert_eq!(honest + 712, divergent.declared);
            assert_eq!(honest, divergent.encoded);
        }

        otherwise => panic!("expected a refusal, got {otherwise:?}"),
    }

    Ok(())
}

/// The husk itself, through the same door — the shape the fleet would have had
/// to produce for #393's regions to exist.
#[test]
fn encode_segment_v3_refuses_a_pre_v2_husk() -> Result<()> {
    let store = store();
    let tp = Topition::new(TOPIC, 0);

    match store.encode_segment_v3(&[(tp, 0, vec![pre_v2_husk()?])], 0, 0) {
        Err(Error::DivergentBatch(divergent)) => {
            assert_eq!(0, divergent.magic);
            assert_eq!(80, divergent.declared);
            assert_eq!(0, divergent.record_data_len);
        }

        otherwise => panic!("expected a refusal, got {otherwise:?}"),
    }

    Ok(())
}

/// A divergent batch anywhere in the run is refused, not just the first — the
/// region's *first* frame is what stops a scan, but a later one poisons the
/// bytes just as permanently.
#[test]
fn a_divergent_batch_after_a_healthy_one_is_still_refused() -> Result<()> {
    let store = store();
    let tp = Topition::new(TOPIC, 0);

    let mut second = batch(b"second")?;
    second.batch_length += 1;

    match store.encode_segment_v3(&[(tp, 0, vec![batch(b"first")?, second])], 0, 0) {
        Err(Error::DivergentBatch(divergent)) => assert_eq!(1, divergent.index),
        otherwise => panic!("expected a refusal, got {otherwise:?}"),
    }

    Ok(())
}

/// The negative control, and the reason this guard is safe to put on the hot
/// write path: an ordinary segment still encodes, and its footer entry still
/// covers a region whose head *is* a frame.
#[test]
fn a_well_formed_segment_still_encodes_and_its_entry_covers_its_frame() -> Result<()> {
    let store = store();
    let tp = Topition::new(TOPIC, 0);

    let batches = vec![batch(b"one")?, batch(b"two")?];
    let declared: Vec<i32> = batches.iter().map(|batch| batch.batch_length).collect();

    let (payload, footer) = store.encode_segment_v3(&[(tp.clone(), 0, batches)], 0, 0)?;

    let entry = footer
        .get(&Substream::Name(tp.topic().into()), tp.partition())
        .expect("sub-stream entry");

    // What the frames claim, plus their headers, is exactly what the entry
    // covers: the equality #393's regions did not have.
    let framed: u64 = declared
        .iter()
        .map(|len| *len as u64 + size_of::<i64>() as u64 + size_of::<i32>() as u64)
        .sum();

    assert_eq!(framed, entry.byte_len);
    assert_eq!(0, entry.byte_start);

    // And the bytes really are there to be read.
    let segment = Bytes::from(payload);
    assert!(segment.len() as u64 >= entry.byte_start + entry.byte_len);

    Ok(())
}

/// A client is told `CORRUPT_MESSAGE`, the same answer reading the damage would
/// have given (#388) — not `UNKNOWN_SERVER_ERROR`, which says nothing.
#[test]
fn a_refusal_is_answered_corrupt_message() -> Result<()> {
    let store = store();
    let tp = Topition::new(TOPIC, 0);

    let mut divergent = batch(b"v")?;
    divergent.batch_length += 1;

    let error = store
        .encode_segment_v3(&[(tp, 0, vec![divergent])], 0, 0)
        .expect_err("a divergent batch is refused");

    assert_eq!(ErrorCode::CorruptMessage, storage_error_code(&error));

    Ok(())
}
