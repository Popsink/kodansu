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

//! Encoding then decoding an arbitrary value gives the value back, for the
//! varints, record headers, records and batches, and the size a varint
//! reports is the size it encodes to.

use bytes::Buf as _;
use proptest::collection::vec;
use proptest::prelude::{Just, ProptestConfig, Strategy, any};
use proptest::{prop_assert_eq, prop_oneof};
use tansu_sans_io::primitive::varint::{LongVarInt, UnsignedVarInt, VarInt};
use tansu_sans_io::record::{Header, Record, deflated, inflated};
use tansu_sans_io::{BatchAttribute, ByteSize as _, Compression};
use tansu_sans_io::{Decode, Decoder, Encode as _, Encoder};

type Octets = bytes::Bytes;

/// Cases per record property, in place of proptest's default 256.
const RECORD_CASES: u32 = 200;

/// Cases per batch property.
const BATCH_CASES: u32 = 50;

/// The top of the batch timestamps generated: half of `i64::MAX`.
const HALF_TIME: i64 = i64::MAX / 2;

/// Up to `most` arbitrary bytes, as one time in four nothing at all.
fn maybe_octets(most: usize) -> impl Strategy<Value = Option<Octets>> {
    let some = vec(any::<u8>(), 0..=most).prop_map(|raw| Some(Octets::from(raw)));
    prop_oneof![1 => Just(None), 3 => some]
}

fn octets(most: usize) -> impl Strategy<Value = Octets> {
    vec(any::<u8>(), 0..=most).prop_map(Octets::from)
}

fn record_header() -> impl Strategy<Value = Header> {
    let pair = (maybe_octets(64), maybe_octets(64));
    pair.prop_map(|(key, value)| Header { key, value })
}

/// The codecs that compress as well as decompress: snappy is left out, as
/// `into_record_data` only decodes it.
fn codec() -> impl Strategy<Value = Compression> {
    let both_ways = [
        Compression::None,
        Compression::Gzip,
        Compression::Lz4,
        Compression::Zstd,
    ];
    proptest::sample::select(both_ways.to_vec())
}

/// `value` through [`Encoder`] and back through [`Decoder`].
fn through_serde<T>(value: &T) -> tansu_sans_io::Result<T>
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let mut encoder = Encoder::new(bytes::BytesMut::new());
    serde::Serialize::serialize(value, &mut encoder)?;
    let mut wire = Octets::from(encoder).reader();
    T::deserialize(&mut Decoder::new(&mut wire))
}

/// `value` through [`Encode`](tansu_sans_io::Encode) and back through
/// [`Decode`], with the bytes left over.
fn through_codec<T: tansu_sans_io::Encode + Decode>(
    value: &T,
) -> tansu_sans_io::Result<(T, usize)> {
    let mut wire = value.encode()?;
    let decoded = T::decode(&mut wire)?;
    Ok((decoded, wire.len()))
}

proptest::proptest! {
    #[test]
    fn a_varint_round_trips_at_its_reported_size(n in any::<i32>()) {
        let varint = VarInt(n);
        prop_assert_eq!((varint, 0), through_codec(&varint)?, "all bytes consumed");
        prop_assert_eq!(varint.size_in_bytes()?, varint.encode()?.len(), "size_in_bytes");
    }

    #[test]
    fn an_unsigned_varint_round_trips_at_its_reported_size(n in any::<u32>()) {
        let varint = UnsignedVarInt(n);
        prop_assert_eq!(varint, through_serde(&varint)?);
        let mut encoder = Encoder::new(bytes::BytesMut::new());
        serde::Serialize::serialize(&varint, &mut encoder)?;
        prop_assert_eq!(varint.size_in_bytes()?, Octets::from(encoder).len(), "size_in_bytes");
    }

    #[test]
    fn a_long_varint_round_trips_at_its_reported_size(n in any::<i64>()) {
        let varint = LongVarInt(n);
        prop_assert_eq!((varint, 0), through_codec(&varint)?, "all bytes consumed");
        prop_assert_eq!(varint.size_in_bytes()?, varint.encode()?.len(), "size_in_bytes");
    }

    #[test]
    fn a_record_header_round_trips(key in maybe_octets(128), value in maybe_octets(128)) {
        let header = Header { key, value };
        prop_assert_eq!((header.clone(), 0), through_codec(&header)?, "all bytes consumed");
    }
}

proptest::proptest! {
    #![proptest_config(ProptestConfig::with_cases(RECORD_CASES))]

    #[test]
    fn a_record_round_trips(
        since_base in any::<i64>(),
        nth in any::<i32>(),
        key in maybe_octets(256),
        value in maybe_octets(512),
        headers in vec(record_header(), 0..=4),
    ) {
        let record = Record::builder().timestamp_delta(since_base);
        let record = record.offset_delta(nth).key(key).value(value);
        let record = headers.into_iter().fold(record, |record, Header { key, value }| {
            let header = tansu_sans_io::record::Header::builder();
            let header = if let Some(key) = key { header.key(key) } else { header };
            let header = if let Some(value) = value { header.value(value) } else { header };
            record.header(header)
        });

        let record = record.build()?;
        prop_assert_eq!((record.clone(), 0), through_codec(&record)?, "all bytes consumed");
    }
}

proptest::proptest! {
    #![proptest_config(ProptestConfig::with_cases(BATCH_CASES))]

    /// Every field of the batch header but `attributes` survives deflating
    /// and inflating again, and so do each record's key, value, deltas and
    /// header count.
    #[test]
    fn a_batch_deflates_and_inflates_back(
        first in any::<i64>(),
        leader_epoch in any::<i32>(),
        earliest in 0..=HALF_TIME,
        latest in 0..=HALF_TIME,
        producer in any::<i64>(),
        bump in any::<i16>(),
        sequence in any::<i32>(),
        compression in codec(),
        keys in vec(maybe_octets(64), 1..=3),
        values in vec(maybe_octets(64), 1..=3),
    ) {
        let attributes = BatchAttribute::default().compression(compression).into();
        let batch = inflated::Batch::builder().base_offset(first).magic(2);
        let batch = batch.partition_leader_epoch(leader_epoch).attributes(attributes);
        let batch = batch.last_offset_delta(0).base_timestamp(earliest);
        let batch = batch.max_timestamp(latest).producer_id(producer);
        let batch = batch.producer_epoch(bump).base_sequence(sequence);
        let pairs = keys.into_iter().zip(values);
        let batch = pairs.fold(batch, |batch, (key, value)| {
            batch.record(Record::builder().key(key).value(value))
        });
        let sent = batch.build()?;

        let compressed = deflated::Batch::try_from(sent.clone())?;
        prop_assert_eq!(sent.records.len(), compressed.record_count as usize);
        let back = inflated::Batch::try_from(compressed)?;

        let header = |b: &inflated::Batch| {
            let ids = (b.base_offset, b.partition_leader_epoch, b.magic, b.last_offset_delta);
            let times = (b.base_timestamp, b.max_timestamp);
            (ids, times, (b.producer_id, b.producer_epoch, b.base_sequence))
        };
        prop_assert_eq!(header(&sent), header(&back));
        prop_assert_eq!(sent.records.len(), back.records.len());

        let content = |r: &Record| {
            (r.key.clone(), r.value.clone(), r.timestamp_delta, r.offset_delta, r.headers.len())
        };
        for (sent, back) in sent.records.iter().zip(&back.records) {
            prop_assert_eq!(content(sent), content(back));
        }
    }

    #[test]
    fn a_deflated_batch_round_trips_through_bytes(
        first in any::<i64>(),
        at in 0..=HALF_TIME,
        producer in any::<i64>(),
        value in octets(128),
    ) {
        let batch = inflated::Batch::builder().base_offset(first);
        let batch = batch.base_timestamp(at).max_timestamp(at);
        let batch = batch.producer_id(producer).record(Record::builder().value(Some(value)));
        let compressed = deflated::Batch::try_from(batch.build()?)?;

        let wire: Octets = compressed.clone().into();
        prop_assert_eq!(compressed, deflated::Batch::try_from(wire)?);
    }
}
