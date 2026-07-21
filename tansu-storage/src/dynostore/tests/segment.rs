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
use tansu_sans_io::record::{Record, deflated, inflated};

use crate::{
    Error, Result, Topition,
    dynostore::{
        CoalesceTuning, DynoStore, IdempotentClass, ProducerCoord, ProducerTail,
        SEGMENT_FORMAT_VERSION_V2, SEGMENT_MAGIC, SEGMENT_TRAILER_LEN, SegmentFooter,
        SubstreamEntry,
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

/// A v2 footer (per-flush nonce + per-batch producer coordinates, #87) round-trips
/// through `encode_footer` / `decode_segment_footer`, and the reader accepts
/// version 2 alongside version 1. (The writer still emits v1 until the leaseless
/// cutover; this pins the format so v2 segments are readable once it flips.)
#[test]
fn footer_v2_round_trips_producer_coords_and_nonce() -> Result<(), Error> {
    let store = store();

    let footer = SegmentFooter {
        writer_epoch: 9,
        nonce: 0x0123_4567_89ab_cdef,
        entries: vec![
            // An idempotent sub-stream: two batches → two producer coordinates,
            // in region (offset) order.
            SubstreamEntry {
                topic: "org.env.conn.tab_a".into(),
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
                    },
                    ProducerCoord {
                        producer_id: 7,
                        producer_epoch: 3,
                        base_sequence: 3,
                        last_sequence: 4,
                        offset_delta: 3,
                    },
                ],
            },
            // A non-idempotent sub-stream carries no producer coordinates.
            SubstreamEntry {
                topic: "org.env.conn.tab_b".into(),
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

    let decoded = store
        .decode_segment_footer(&tail)?
        .expect("v2 footer must decode");
    assert_eq!(footer, decoded);

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
