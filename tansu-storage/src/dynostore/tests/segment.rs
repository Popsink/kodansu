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

use std::time::Duration;

use bytes::Bytes;
use object_store::{PutPayload, memory::InMemory};
use tansu_sans_io::{
    ErrorCode,
    record::{Record, deflated, inflated},
};
use uuid::Uuid;

use crate::{
    Error, Result, Topition,
    dynostore::{
        CoalesceTuning, DynoStore, FrameTail, IdempotentClass, ProducerCoord, ProducerTail,
        SEGMENT_FORMAT_VERSION_V2, SEGMENT_FORMAT_VERSION_V3, SEGMENT_FORMAT_VERSION_V4,
        SEGMENT_MAGIC, SEGMENT_TRAILER_LEN, SegmentFooter, Substream, SubstreamEntry,
        SubstreamWrite,
    },
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

    let a0 = footer
        .get(&Substream::Name("alpha".into()), 0)
        .expect("alpha-0 entry");
    assert_eq!(40, a0.base_offset);
    assert_eq!(5, a0.record_count);

    let a1 = footer
        .get(&Substream::Name("alpha".into()), 1)
        .expect("alpha-1 entry");
    assert_eq!(7, a1.base_offset);
    assert_eq!(1, a1.record_count);

    let b0 = footer
        .get(&Substream::Name("beta".into()), 0)
        .expect("beta-0 entry");
    assert_eq!(900, b0.base_offset);
    assert_eq!(4, b0.record_count);

    // Regions are contiguous and non-overlapping, laid end to end.
    assert_eq!(0, a0.byte_start);
    assert_eq!(a0.byte_start + a0.byte_len, a1.byte_start);
    assert_eq!(a1.byte_start + a1.byte_len, b0.byte_start);

    // The footer recovered from the object equals the one returned by encode.
    let decoded = DynoStore::decode_segment_footer(&segment)?.expect("segment must carry a footer");
    assert_eq!(footer, decoded);

    // Each sub-stream's byte range decodes to exactly its batches — no
    // cross-topic data in the range (the #60 ranged-GET contract).
    for entry in &decoded.entries {
        let start = entry.byte_start as usize;
        let end = start + entry.byte_len as usize;
        let (batches, tail) = store.decode_frame(segment.slice(start..end))?;
        // Every byte of the range is a whole batch: no ignorable tail (#386).
        assert_eq!(FrameTail::Exhausted, tail);
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

    let decoded = DynoStore::decode_segment_footer(&tail)?.expect("footer from tail");
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

    assert!(DynoStore::decode_segment_footer(&object)?.is_none());

    let (decoded, _) = store.decode_frame(object)?;
    assert_eq!(2, decoded.len());
    assert_eq!(1, decoded[0].last_offset_delta); // 2 records
    assert_eq!(2, decoded[1].last_offset_delta); // 3 records

    Ok(())
}

/// A byte string too short to hold a trailer is not a segment — the probe must
/// not panic or misread, it returns `None`.
#[tokio::test]
async fn tail_shorter_than_trailer_is_not_a_segment() -> Result<(), Error> {
    let tiny = Bytes::from(vec![0u8; SEGMENT_TRAILER_LEN - 1]);
    assert!(DynoStore::decode_segment_footer(&tiny)?.is_none());
    Ok(())
}

/// A v2 footer (per-flush nonce + per-batch producer coordinates, #87) round-trips
/// through `encode_footer` / `decode_segment_footer`, and the reader accepts
/// version 2 alongside version 1. (The writer still emits v1 until the leaseless
/// cutover; this pins the format so v2 segments are readable once it flips.)
#[test]
fn footer_v2_round_trips_producer_coords_and_nonce() -> Result<(), Error> {
    let footer = SegmentFooter {
        writer_epoch: 9,
        nonce: 0x0123_4567_89ab_cdef,
        entries: vec![
            // An idempotent sub-stream: two batches → two producer coordinates,
            // in region (offset) order.
            SubstreamEntry {
                topic: "org.env.conn.tab_a".into(),
                topic_id: None,
                partition: 0,
                base_offset: 100,
                record_count: 5,
                byte_start: 0,
                byte_len: 42,
                max_timestamp: 1_700_000_000_000,
                producers: vec![
                    ProducerCoord {
                        producer_id: 7,
                        producer_epoch: 3,
                        base_sequence: 0,
                        last_sequence: 2,
                        offset_delta: 0,
                        flags: 0,
                    },
                    ProducerCoord {
                        producer_id: 7,
                        producer_epoch: 3,
                        base_sequence: 3,
                        last_sequence: 4,
                        offset_delta: 3,
                        flags: 0,
                    },
                ],
            },
            // A non-idempotent sub-stream carries no producer coordinates.
            SubstreamEntry {
                topic: "org.env.conn.tab_b".into(),
                topic_id: None,
                partition: 1,
                base_offset: 0,
                record_count: 2,
                byte_start: 42,
                byte_len: 10,
                max_timestamp: 1_700_000_000_500,
                producers: vec![],
            },
        ],
    };

    // Assemble a v2 segment tail: the encoded footer followed by the fixed
    // trailer (footer_len u64 + entry_count u32 + version u16 + magic u32).
    let footer_bytes = DynoStore::encode_footer(&footer, SEGMENT_FORMAT_VERSION_V2);
    let mut tail = footer_bytes.clone();
    tail.extend_from_slice(&(footer_bytes.len() as u64).to_be_bytes());
    tail.extend_from_slice(&(footer.entries.len() as u32).to_be_bytes());
    tail.extend_from_slice(&SEGMENT_FORMAT_VERSION_V2.to_be_bytes());
    tail.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());

    let decoded = DynoStore::decode_segment_footer(&tail)?.expect("v2 footer must decode");
    assert_eq!(footer, decoded);

    Ok(())
}

/// A v2 footer encodes to the exact bytes of the published v2 contract
/// (`docs/virtual-topics-format.md`), pinned as a golden vector — even when
/// the in-memory [`ProducerCoord`] carries nonzero v3 `flags` (#174), which
/// v2 must drop, not zero-fill. Without this, a change to `ProducerCoord` or
/// `encode_footer` (such as the v3 `flags` field) could silently alter the
/// bytes of v2 objects — which every deployed reader, including S3-direct
/// external ones (kotatsu#82), decodes by this layout. The golden bytes were
/// captured from the encoder *before* the v3 change landed, so passing here is
/// byte-identity with what production has always written, not with whatever
/// the encoder currently does.
#[test]
fn footer_v2_encoding_is_byte_identical_to_golden() -> Result<(), Error> {
    let footer = SegmentFooter {
        writer_epoch: 1,
        nonce: 2,
        entries: vec![SubstreamEntry {
            topic: "t".into(),
            topic_id: None,
            partition: 3,
            base_offset: 4,
            record_count: 5,
            byte_start: 6,
            byte_len: 7,
            max_timestamp: 8,
            producers: vec![ProducerCoord {
                producer_id: 9,
                producer_epoch: 10,
                base_sequence: 11,
                last_sequence: 12,
                offset_delta: 13,
                // Nonzero on purpose: a v2 encode must not let flags reach the
                // bytes at all.
                flags: 0b11,
            }],
        }],
    };

    #[rustfmt::skip]
    const GOLDEN_V2: [u8; 87] = [
        0, 0, 0, 0, 0, 0, 0, 1,     // writer_epoch i64
        0, 0, 0, 0, 0, 0, 0, 2,     // nonce u64 (v2)
        0, 1,                       // topic_len u16
        b't',                       // topic
        0, 0, 0, 3,                 // partition i32
        0, 0, 0, 0, 0, 0, 0, 4,     // base_offset i64
        0, 0, 0, 0, 0, 0, 0, 5,     // record_count i64
        0, 0, 0, 0, 0, 0, 0, 6,     // byte_start u64
        0, 0, 0, 0, 0, 0, 0, 7,     // byte_len u64
        0, 0, 0, 0, 0, 0, 0, 8,     // max_timestamp i64
        0, 1,                       // pcoord_count u16 (v2)
        0, 0, 0, 0, 0, 0, 0, 9,     // producer_id i64
        0, 10,                      // producer_epoch i16
        0, 0, 0, 11,                // base_sequence i32
        0, 0, 0, 12,                // last_sequence i32
        0, 0, 0, 13,                // offset_delta u32
    ];

    let encoded = DynoStore::encode_footer(&footer, SEGMENT_FORMAT_VERSION_V2);
    assert_eq!(GOLDEN_V2.as_slice(), encoded.as_slice());

    // And a v2 decode observes flags = 0 — the layout has no flags byte, so
    // pre-v3 behaviour is bit-for-bit unchanged on existing objects.
    let mut tail = encoded.clone();
    tail.extend_from_slice(&(encoded.len() as u64).to_be_bytes());
    tail.extend_from_slice(&(footer.entries.len() as u32).to_be_bytes());
    tail.extend_from_slice(&SEGMENT_FORMAT_VERSION_V2.to_be_bytes());
    tail.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());

    let decoded = DynoStore::decode_segment_footer(&tail)?.expect("v2 footer must decode");
    assert_eq!(0, decoded.entries[0].producers[0].flags);

    Ok(())
}

/// A v3 footer (per-coordinate `flags` byte, #174) round-trips through
/// `encode_footer` / `decode_segment_footer` with the flags preserved, and the
/// reader accepts version 3 alongside 1 and 2. Without this, the reader-first
/// release of #174 ships nothing: the whole point of release A is that every
/// broker accepts v3 *before* any writer emits it, because an unaccepted
/// version is a hard `decode_segment_footer` error that turns into a
/// partition-wide read outage. (The writer still emits v2 until release B;
/// this pins the v3 layout so those segments are readable once it flips.)
#[test]
fn footer_v3_round_trips_producer_coords_with_flags() -> Result<(), Error> {
    let footer = SegmentFooter {
        writer_epoch: 9,
        nonce: 0x0123_4567_89ab_cdef,
        entries: vec![SubstreamEntry {
            topic: "org.env.conn.tab_a".into(),
            topic_id: None,
            partition: 0,
            base_offset: 100,
            record_count: 6,
            byte_start: 0,
            byte_len: 42,
            max_timestamp: 1_700_000_000_000,
            producers: vec![
                // A plain idempotent batch: no flags set.
                ProducerCoord {
                    producer_id: 7,
                    producer_epoch: 3,
                    base_sequence: 0,
                    last_sequence: 2,
                    offset_delta: 0,
                    flags: 0,
                },
                // A transactional data batch: bit 0.
                ProducerCoord {
                    producer_id: 7,
                    producer_epoch: 3,
                    base_sequence: 3,
                    last_sequence: 4,
                    offset_delta: 3,
                    flags: 0b01,
                },
                // A transaction marker (control batch): bits 0 and 1, and the
                // non-idempotent -1 sequences a marker carries.
                ProducerCoord {
                    producer_id: 7,
                    producer_epoch: 3,
                    base_sequence: -1,
                    last_sequence: -1,
                    offset_delta: 5,
                    flags: 0b11,
                },
            ],
        }],
    };

    let footer_bytes = DynoStore::encode_footer(&footer, SEGMENT_FORMAT_VERSION_V3);
    let mut tail = footer_bytes.clone();
    tail.extend_from_slice(&(footer_bytes.len() as u64).to_be_bytes());
    tail.extend_from_slice(&(footer.entries.len() as u32).to_be_bytes());
    tail.extend_from_slice(&SEGMENT_FORMAT_VERSION_V3.to_be_bytes());
    tail.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());

    let decoded = DynoStore::decode_segment_footer(&tail)?.expect("v3 footer must decode");
    assert_eq!(footer, decoded);

    Ok(())
}

/// A v4 footer (per-entry `topic_id`, #442) round-trips both identities, and the
/// reader accepts version 4 alongside 1, 2 and 3.
///
/// The nil uuid is the wire's "no id", and it decodes back to `None` rather than
/// to a `Some(nil)` nobody can match: "the segment is v4" and "this sub-stream is
/// keyed by id" are independent, because both kinds of topic coexist in one
/// prefix for as long as any pre-flip topic lives.
///
/// This is the reader half of the same two-step #174 took: every broker accepts
/// v4 *before* any writer emits it, because an unaccepted version is a hard
/// `decode_segment_footer` error — and since a segment is shared, one writer
/// emitting v4 into a prefix would take out an older reader's reads of that whole
/// prefix, not just of the topic that caused it.
#[test]
fn footer_v4_round_trips_the_substream_identity() -> Result<(), Error> {
    let id = Uuid::from_u128(0x0192_3f5a_7c11_4d2e_9b83_0f6a_1c4d_5e70);

    let footer = SegmentFooter {
        writer_epoch: 9,
        nonce: 0x0123_4567_89ab_cdef,
        entries: vec![
            // Keyed by id: a topic created under the v4 writer regime.
            SubstreamEntry {
                topic: "org.env.conn.tab_a".into(),
                topic_id: Some(id),
                partition: 0,
                base_offset: 0,
                record_count: 6,
                byte_start: 0,
                byte_len: 42,
                max_timestamp: 1_700_000_000_000,
                producers: vec![ProducerCoord {
                    producer_id: 7,
                    producer_epoch: 3,
                    base_sequence: 0,
                    last_sequence: 5,
                    offset_delta: 0,
                    flags: 0,
                }],
            },
            // Keyed by name, in the same v4 segment: a topic that predates the
            // flip, whose records are still found by the name they were written
            // under.
            SubstreamEntry {
                topic: "org.env.conn.tab_b".into(),
                topic_id: None,
                partition: 3,
                base_offset: 100,
                record_count: 2,
                byte_start: 42,
                byte_len: 18,
                max_timestamp: 1_700_000_000_001,
                producers: vec![],
            },
        ],
    };

    let footer_bytes = DynoStore::encode_footer(&footer, SEGMENT_FORMAT_VERSION_V4);
    let mut tail = footer_bytes.clone();
    tail.extend_from_slice(&(footer_bytes.len() as u64).to_be_bytes());
    tail.extend_from_slice(&(footer.entries.len() as u32).to_be_bytes());
    tail.extend_from_slice(&SEGMENT_FORMAT_VERSION_V4.to_be_bytes());
    tail.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());

    let decoded = DynoStore::decode_segment_footer(&tail)?.expect("v4 footer must decode");
    assert_eq!(footer, decoded);

    // The identities are what a reader resolves by, and they do not collide.
    assert_eq!(
        Some(&decoded.entries[0]),
        decoded.get(&Substream::Id(id), 0)
    );
    assert_eq!(
        None,
        decoded.get(&Substream::Name("org.env.conn.tab_a".into()), 0),
        "an id-keyed entry must not answer to its own name, or a recreated \
         topic's records would be served as its predecessor's"
    );
    assert_eq!(
        Some(&decoded.entries[1]),
        decoded.get(&Substream::Name("org.env.conn.tab_b".into()), 3)
    );

    Ok(())
}

/// Asked for v3, `encode_footer` emits the **exact** pre-v4 bytes: the 16-byte
/// `topic_id` is dropped, not zero-filled.
///
/// Deployed readers — internal and S3-direct external — decode v3 by that byte
/// layout, and a v3 segment that carried an extra 16 bytes per entry would
/// mis-decode every following field of every following entry. The same MUST that
/// kept `flags` out of a v2 footer (#174).
#[test]
fn a_v3_footer_carries_no_topic_id() -> Result<(), Error> {
    let entry = |topic_id| SubstreamEntry {
        topic: "ab".into(),
        topic_id,
        partition: 1,
        base_offset: 2,
        record_count: 3,
        byte_start: 4,
        byte_len: 5,
        max_timestamp: 6,
        producers: vec![],
    };

    let footer = |topic_id| SegmentFooter {
        writer_epoch: 1,
        nonce: 2,
        entries: vec![entry(topic_id)],
    };

    #[rustfmt::skip]
    const GOLDEN_V3: [u8; 48] = [
        0, 0, 0, 0, 0, 0, 0, 1,     // writer_epoch i64
        0, 0, 0, 0, 0, 0, 0, 2,     // nonce u64 (v2+)
        0, 2,                       // topic_len u16
        b'a', b'b',                 // topic
        0, 0, 0, 1,                 // partition i32
        0, 0, 0, 0, 0, 0, 0, 2,     // base_offset i64
        0, 0, 0, 0, 0, 0, 0, 3,     // record_count i64
        0, 0, 0, 0, 0, 0, 0, 4,     // byte_start u64
    ];

    // The identity makes no difference to a v3 encoding: it has nowhere to put
    // one, so both must produce the same prefix.
    let with_id =
        DynoStore::encode_footer(&footer(Some(Uuid::now_v7())), SEGMENT_FORMAT_VERSION_V3);
    let without = DynoStore::encode_footer(&footer(None), SEGMENT_FORMAT_VERSION_V3);

    assert_eq!(without, with_id);
    assert_eq!(GOLDEN_V3.as_slice(), &without[..GOLDEN_V3.len()]);

    // v4 is the same bytes with 16 more, immediately after the topic name.
    let v4 = DynoStore::encode_footer(&footer(None), SEGMENT_FORMAT_VERSION_V4);
    assert_eq!(without.len() + 16, v4.len());

    Ok(())
}

/// A v3 writer refuses an id-keyed sub-stream rather than writing it name-keyed
/// (#442).
///
/// Writing it name-keyed would put acked, durable records where no reader of that
/// topic looks — the failure mode this whole keying exists to prevent, arriving
/// through the writer instead of the reader. The only way to reach it is a
/// deployment that pinned `substream_id` on a topic and then went back to a v3
/// writer regime, which is why raising `segment_format` is documented as one-way.
#[test]
fn a_pre_v4_writer_refuses_an_id_keyed_substream() -> Result<(), Error> {
    let store = store();
    let tp = Topition::new("org.env.conn.tab", 0);

    // A real batch: an empty sub-stream contributes no footer entry at all, so
    // it has no identity to refuse.
    let write = |substream| -> Result<Vec<SubstreamWrite>> {
        Ok(vec![SubstreamWrite {
            topition: tp.clone(),
            substream,
            base_offset: 0,
            batches: vec![batch(1)?],
        }])
    };

    // A name-keyed sub-stream encodes at v3 exactly as it always did.
    assert!(
        store
            .encode_segment_indexed(
                &write(Substream::Name(tp.topic().into()))?,
                0,
                0,
                SEGMENT_FORMAT_VERSION_V3,
            )
            .is_ok()
    );

    // The same sub-stream keyed by id does not.
    assert!(matches!(
        store.encode_segment_indexed(
            &write(Substream::Id(Uuid::now_v7()))?,
            0,
            0,
            SEGMENT_FORMAT_VERSION_V3,
        ),
        Err(Error::Api(ErrorCode::KafkaStorageError))
    ));

    // And does at v4.
    assert!(
        store
            .encode_segment_indexed(
                &write(Substream::Id(Uuid::now_v7()))?,
                0,
                0,
                SEGMENT_FORMAT_VERSION_V4,
            )
            .is_ok()
    );

    Ok(())
}

/// A trailer carrying an unknown version (5) is still rejected, loudly. The
/// external contract (`docs/virtual-topics-format.md`) says a reader MUST
/// accept {1, 2, 3, 4} and MUST reject anything else rather than guessing — an
/// unknown layout would mis-decode, not degrade. Without this, widening the
/// accepted set for v3 (#174) and then v4 (#442) could accidentally have become
/// "accept anything", silently discarding the contract's rejection MUST.
#[test]
fn footer_rejects_unknown_version() -> Result<(), Error> {
    let footer = SegmentFooter {
        writer_epoch: 1,
        nonce: 2,
        entries: vec![],
    };

    let footer_bytes = DynoStore::encode_footer(&footer, SEGMENT_FORMAT_VERSION_V3);
    let mut tail = footer_bytes.clone();
    tail.extend_from_slice(&(footer_bytes.len() as u64).to_be_bytes());
    tail.extend_from_slice(&0u32.to_be_bytes());
    tail.extend_from_slice(&5u16.to_be_bytes());
    tail.extend_from_slice(&SEGMENT_MAGIC.to_be_bytes());

    let error = DynoStore::decode_segment_footer(&tail).expect_err("version 5 must be rejected");
    assert!(
        error
            .to_string()
            .contains("unsupported segment format version 5"),
        "unexpected error: {error}"
    );

    Ok(())
}

/// The folded [`ProducerTail`] (#88) classifies idempotent batches exactly:
/// in-order admit, duplicate-with-offset over the last-five window, a gap → out
/// of order, a lower epoch → fenced, a higher epoch → reset. Sequence arithmetic
/// wraps at `i32::MAX` back to 0 (#80), so a producer that wraps stays deduped.
#[test]
fn producer_tail_classifies_dedup_epoch_and_wraparound() {
    // Fresh producer: only sequence 0 is in order.
    let mut tail = ProducerTail::default();
    assert_eq!(IdempotentClass::Admit, tail.classify(0, 0));
    assert_eq!(IdempotentClass::OutOfOrder, tail.classify(0, 1));

    // Fold seq 0 (1 record) at offset 100, then seq 1..=2 (2 records) at 101.
    tail.fold(0, 0, 0, 100);
    assert_eq!(IdempotentClass::Admit, tail.classify(0, 1));
    tail.fold(0, 1, 2, 101);

    // Next in order is seq 3; retries ack their original offsets; a gap is OOO.
    assert_eq!(IdempotentClass::Admit, tail.classify(0, 3));
    assert_eq!(IdempotentClass::Duplicate(100), tail.classify(0, 0));
    assert_eq!(IdempotentClass::Duplicate(101), tail.classify(0, 1));
    assert_eq!(IdempotentClass::OutOfOrder, tail.classify(0, 5));

    // A higher epoch resets the stream; the old epoch's window no longer applies.
    assert_eq!(IdempotentClass::Admit, tail.classify(1, 0));
    tail.fold(1, 0, 0, 200);
    // A lower epoch is fenced; the old-epoch offset is no longer a duplicate.
    assert_eq!(IdempotentClass::Fenced, tail.classify(0, 1));
    assert_eq!(IdempotentClass::OutOfOrder, tail.classify(1, 5));

    // Wraparound: a batch whose last_sequence is i32::MAX makes the next
    // expected sequence 0, not i32::MIN.
    let mut wrap = ProducerTail::default();
    wrap.fold(0, i32::MAX - 1, i32::MAX, 300);
    assert_eq!(IdempotentClass::Admit, wrap.classify(0, 0));
    assert_eq!(
        IdempotentClass::Duplicate(300),
        wrap.classify(0, i32::MAX - 1)
    );
}

/// The duplicate window holds only the last five batches (Kafka's bound): a
/// sixth fold evicts the oldest, which then reads as out of order rather than a
/// duplicate.
#[test]
fn producer_tail_window_keeps_last_five() {
    let mut tail = ProducerTail::default();
    for seq in 0..6 {
        tail.fold(0, seq, seq, seq as i64);
    }
    // The oldest (seq 0) has been evicted; seq 1..=5 remain; next is seq 6.
    assert_eq!(IdempotentClass::OutOfOrder, tail.classify(0, 0));
    assert_eq!(IdempotentClass::Duplicate(1), tail.classify(0, 1));
    assert_eq!(IdempotentClass::Duplicate(5), tail.classify(0, 5));
    assert_eq!(IdempotentClass::Admit, tail.classify(0, 6));
}

/// #91: the leaseless conflict path derives the next segment sequence from the
/// already force-folded in-memory index (folded-max + 1) instead of a fresh
/// LIST, and still honours the persisted seq floor so a name freed by
/// retention/compaction is never reused.
#[tokio::test]
async fn tail_next_seq_folded_derives_from_index_and_floor() -> Result<(), Error> {
    let store = store();
    let prefix = "topic-0";

    // Cold: no cached segments, no floor → the first free sequence is 0.
    assert_eq!(0, store.tail_next_seq_folded(prefix).await?);

    // Fold three segments into the index (no object LIST). Footer content is
    // irrelevant here — only the sequence keys are read.
    for seq in 0..3 {
        store.index_insert(prefix, seq, SegmentFooter::default(), 0)?;
    }
    assert_eq!(3, store.tail_next_seq_folded(prefix).await?);

    // A persisted floor above the folded max wins (a retention/compaction delete
    // freed those names, which must not be reused).
    store.raise_seq_floor(prefix, 10).await?;
    assert_eq!(10, store.tail_next_seq_folded(prefix).await?);

    Ok(())
}

/// #174: a transaction-marker (control) coordinate in a v3 footer must NOT
/// fold into the producer tail. A marker carries `base_sequence =
/// last_sequence = -1`; folding it would set `next_sequence` to
/// `seq_increment(-1) = 0` and mark the tail seen, so the producer's genuine
/// next in-order data batch (at sequence N) would classify `OutOfOrder` and
/// the produce would be rejected — dedup corrupted by placement metadata.
/// Transactional *data* coordinates carry real sequences and must keep
/// folding: they are the dedup authority for those batches.
#[tokio::test]
async fn control_coordinate_does_not_fold_into_producer_tail() -> Result<(), Error> {
    let store = store();
    let prefix = "org.env.conn";
    let topic = "org.env.conn.tab_a";
    let tp = Topition::new(topic, 0);

    // One segment: two idempotent data batches (seq 0, then 1..=2 — the second
    // transactional) followed by the producer's commit marker, as the v3
    // writer indexes them.
    store.index_insert(
        prefix,
        0,
        SegmentFooter {
            writer_epoch: 1,
            nonce: 42,
            entries: vec![SubstreamEntry {
                topic: topic.into(),
                topic_id: None,
                partition: 0,
                base_offset: 0,
                record_count: 4,
                byte_start: 0,
                byte_len: 64,
                max_timestamp: 0,
                producers: vec![
                    ProducerCoord {
                        producer_id: 7,
                        producer_epoch: 0,
                        base_sequence: 0,
                        last_sequence: 0,
                        offset_delta: 0,
                        flags: 0,
                    },
                    ProducerCoord {
                        producer_id: 7,
                        producer_epoch: 0,
                        base_sequence: 1,
                        last_sequence: 2,
                        offset_delta: 1,
                        flags: 0b01,
                    },
                    // The commit marker: control + transactional, no sequence.
                    ProducerCoord {
                        producer_id: 7,
                        producer_epoch: 0,
                        base_sequence: -1,
                        last_sequence: -1,
                        offset_delta: 3,
                        flags: 0b11,
                    },
                ],
            }],
        },
        0,
    )?;

    let tail = store.producer_tail_folded(prefix, &Substream::Name(tp.topic().into()), &tp, 7)?;

    // The tail reflects the data batches only: next in order is sequence 3.
    // Had the marker folded, expected() would be seq_increment(-1) = 0 and
    // sequence 3 would classify OutOfOrder.
    assert_eq!(IdempotentClass::Admit, tail.classify(0, 3));
    // The transactional data batch folded normally: its retry is a duplicate
    // acked with the original offset.
    assert_eq!(IdempotentClass::Duplicate(1), tail.classify(0, 1));

    Ok(())
}

/// #91: the coalesce linger is jittered ±20% per flush so independent pods
/// de-phase and stop racing the create of the same next segment name. Every
/// draw stays within the band, and across many draws we actually observe spread
/// on both sides of the base (it is not a constant).
#[test]
fn jittered_linger_stays_within_twenty_percent_band() {
    let base = Duration::from_millis(100);
    let store = store().coalesce_tuning(CoalesceTuning {
        coalesce_linger: Some(base),
        ..Default::default()
    });

    let mut below = false;
    let mut above = false;
    for _ in 0..1_000 {
        let linger = store.jittered_linger();
        assert!(
            linger >= Duration::from_millis(80) && linger <= Duration::from_millis(120),
            "jittered linger {linger:?} outside ±20% of {base:?}",
        );
        below |= linger < base;
        above |= linger > base;
    }
    assert!(
        below && above,
        "jitter did not spread on both sides of the base"
    );
}
