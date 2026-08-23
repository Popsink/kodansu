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
//
//! Deflated (compressed) Kafka Records
use std::{fmt::Formatter, io::Write, result};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::write::GzEncoder;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, Visitor},
};
use tracing::{debug, error, instrument};

use crate::{ByteSize, Compression, Decode as _, Decoder, Encode, Error, Result, record::Record};

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Frame {
    pub batches: Vec<Batch>,
}

impl ByteSize for Frame {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(self
            .batches
            .iter()
            .map(|batch| {
                // base_offset
                size_of::<i64>()
                // batch length
                + size_of::<i32>()
                + FIXED_BATCH_LENGTH
                + batch.record_data.len()
            })
            .sum())
    }
}

impl TryFrom<crate::record::inflated::Frame> for Frame {
    type Error = Error;

    fn try_from(inflated: crate::record::inflated::Frame) -> Result<Self, Self::Error> {
        inflated
            .batches
            .into_iter()
            .map(Batch::try_from)
            .collect::<Result<Vec<_>>>()
            .map(|batches| Self { batches })
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
/// A deflated (compressed) batch of Kafka records
pub struct Batch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: u32,
    pub record_data: Bytes,
}

impl From<Batch> for Bytes {
    fn from(value: Batch) -> Self {
        let mut encoded =
            BytesMut::with_capacity(value.batch_length as usize + size_of_val(&value.base_offset));

        encoded.put_i64(value.base_offset);
        encoded.put_i32(value.batch_length);
        encoded.put_i32(value.partition_leader_epoch);
        encoded.put_i8(value.magic);
        encoded.put_u32(value.crc);
        encoded.put_i16(value.attributes);
        encoded.put_i32(value.last_offset_delta);
        encoded.put_i64(value.base_timestamp);
        encoded.put_i64(value.max_timestamp);
        encoded.put_i64(value.producer_id);
        encoded.put_i16(value.producer_epoch);
        encoded.put_i32(value.base_sequence);
        encoded.put_u32(value.record_count);
        encoded.put(value.record_data);

        Bytes::from(encoded)
    }
}

impl TryFrom<Bytes> for Batch {
    type Error = Error;
    fn try_from(mut encoded: Bytes) -> result::Result<Self, Self::Error> {
        let base_offset = encoded.try_get_i64()?;
        let batch_length = encoded.try_get_i32()?;

        let partition_leader_epoch = encoded.try_get_i32()?;
        let magic = encoded.try_get_i8()?;

        // Decide the format here, before a single v2-only field is read.
        //
        // `magic` sits at the same absolute offset in both layouts — that is
        // by design, and it is how a broker tells them apart. Everything
        // after it differs: a pre-v2 MessageSet carries
        // `attributes | [timestamp] | key | value` where v2 carries
        // `crc | attributes | last_offset_delta | ...`. Parsing on regardless
        // is what produced a batch claiming 2_920_539_060 records from a
        // 92-byte magic-0 message (#320).
        //
        // This struct cannot represent a pre-v2 MessageSet, so it does not
        // try: only the three fields read above are at known positions, and
        // the rest are left at their defaults. The batch is returned rather
        // than refused because the framing is shared — `base_offset` then a
        // `batch_length`-long body — so the request body still decodes, and
        // refusing here would fail the whole frame, which the broker answers
        // by closing the connection with no response at all. `ProduceService`
        // refuses it per-partition instead, where the producer can be told.
        if magic != Self::MAGIC_RECORD_BATCH_V2 {
            debug!(base_offset, batch_length, magic, "pre-v2 message set");

            return Ok(Batch {
                base_offset,
                batch_length,
                magic,
                ..Default::default()
            });
        }

        let crc = encoded.try_get_u32()?;

        debug!(base_offset, batch_length);

        let crc_data_size = usize::try_from(batch_length)
            .map_err(Into::into)
            .and_then(|batch_length| {
                batch_length
                    .checked_sub(size_of_val(&partition_leader_epoch))
                    .ok_or(Error::Overflow)
                    .inspect_err(|err| {
                        debug!(
                            batch_length,
                            size_of_val = size_of_val(&partition_leader_epoch),
                            ?err
                        )
                    })
            })
            .and_then(|batch_length| {
                batch_length
                    .checked_sub(size_of_val(&magic))
                    .ok_or(Error::Overflow)
                    .inspect_err(|err| {
                        debug!(batch_length, size_of_val = size_of_val(&magic), ?err)
                    })
            })
            .and_then(|batch_length| {
                batch_length
                    .checked_sub(size_of_val(&crc))
                    .ok_or(Error::Overflow)
                    .inspect_err(|err| debug!(batch_length, size_of_val = size_of_val(&crc), ?err))
            })?;

        debug!(crc_data_size, encoded = encoded.len(), crc);

        if crc_data_size > encoded.len() {
            return Err(Error::Overflow);
        }

        let crc_data = &encoded[..crc_data_size];

        let computed = {
            let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
            digest.update(crc_data);

            digest.finalize() as u32
        };

        // Log a mismatch, do not refuse the batch. This decoder is shared by
        // the two directions and only one of them can afford to reject:
        //
        //  - decoding a request off the wire: a mismatch means a corrupt
        //    batch, and it *is* refused — but by `ProduceService`, not here.
        //    Failing here fails the whole request body, and a request the
        //    broker cannot decode is answered by ending the connection with
        //    no response at all, so the producer learns nothing. Rejecting
        //    in the produce path instead answers CORRUPT_MESSAGE (2) for the
        //    partition, which is what Kafka does and what a client can act
        //    on.
        //  - decoding bytes back out of storage: a mismatch here does not
        //    imply corruption. `ProduceService` rewrites `base_timestamp`
        //    and `max_timestamp` for a LogAppendTime batch after this check
        //    and before the batch is stored, without recomputing the CRC —
        //    both fields are inside the digested range, so the broker itself
        //    persists batches whose CRC is stale by design. Refusing them on
        //    the way out would refuse data we wrote.
        //
        // So the asymmetry is deliberate: strict on the way in, permissive
        // on the way out. Do not "simplify" it by rejecting here (#271).
        if computed != crc {
            error!(crc, computed);
        }

        let attributes = encoded.try_get_i16()?;
        let last_offset_delta = encoded.try_get_i32()?;
        let base_timestamp = encoded.try_get_i64()?;
        let max_timestamp = encoded.try_get_i64()?;
        let producer_id = encoded.try_get_i64()?;
        let producer_epoch = encoded.try_get_i16()?;
        let base_sequence = encoded.try_get_i32()?;
        let record_count = encoded.try_get_u32()?;

        let record_data_size =
            usize::try_from(batch_length)
                .map_err(Into::into)
                .and_then(|batch_length| {
                    batch_length
                        .checked_sub(FIXED_BATCH_LENGTH)
                        .ok_or(Error::Overflow)
                })?;

        debug!(record_data_size, encoded = encoded.len(), computed);

        if record_data_size > encoded.len() {
            return Err(Error::Overflow);
        }

        let record_data = encoded.slice(..record_data_size);

        let batch = Batch {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            record_count,
            record_data,
        };

        Ok(batch)
    }
}

impl Batch {
    const TRANSACTIONAL_BITMASK: i16 = 0b1_0000i16;
    const CONTROL_BITMASK: i16 = 0b10_0000i16;

    /// The `magic` of the v2 RecordBatch — the only record format this broker
    /// reads or writes.
    ///
    /// Kafka has had three: the magic-0 and magic-1 MessageSets, superseded by
    /// the v2 RecordBatch in 0.11. A producer only sends a pre-v2 MessageSet on
    /// `Produce` v0, v1 or v2, so the API version and the record format move
    /// together.
    pub const MAGIC_RECORD_BATCH_V2: i8 = 2;

    /// Whether this batch is in the v2 RecordBatch format.
    ///
    /// A batch that is not carries no meaningful field beyond `base_offset`,
    /// `batch_length` and `magic`: the decoder stops at `magic` rather than
    /// reading a pre-v2 MessageSet through the v2 field layout (#320). Refuse
    /// such a batch — `UNSUPPORTED_FOR_MESSAGE_FORMAT` — do not act on it.
    pub fn is_record_batch_v2(&self) -> bool {
        self.magic == Self::MAGIC_RECORD_BATCH_V2
    }

    pub fn is_transactional(&self) -> bool {
        self.attributes & Self::TRANSACTIONAL_BITMASK == Self::TRANSACTIONAL_BITMASK
    }

    pub fn is_control(&self) -> bool {
        self.attributes & Self::CONTROL_BITMASK == Self::CONTROL_BITMASK
    }

    pub fn is_idempotent(&self) -> bool {
        self.producer_id != -1 && self.base_sequence != -1
    }

    /// The CRC-32C that this batch's own fields imply.
    ///
    /// Recomputed from `attributes` through `record_data` — the same range,
    /// in the same order, that is digested when a batch is built and when one
    /// is decoded. `base_offset`, `batch_length`, `partition_leader_epoch`
    /// and `magic` precede the CRC on the wire and are not covered by it.
    ///
    /// This goes through `CrcData` rather than digesting the fields directly
    /// so that the digested range has exactly one definition — a field added
    /// there is covered here for free. The price is a payload-sized copy and a
    /// second CRC pass over a batch the decoder already digested once. That is
    /// paid per produced batch, against a produce that ends in an object-store
    /// PUT; if it ever shows up in a profile, the fix is for the decoder to
    /// carry its verdict, not for this to hand-roll the field order.
    pub fn computed_crc(&self) -> Result<u32> {
        CrcData::from(self).crc()
    }

    /// Whether the `crc` field agrees with the payload it is meant to cover.
    ///
    /// Decoding a batch does *not* enforce this (see the note at the mismatch
    /// in the `TryFrom<Bytes>` impl above); callers that want to refuse a
    /// corrupt batch ask for it here.
    pub fn crc_matches(&self) -> Result<bool> {
        self.computed_crc().map(|computed| computed == self.crc)
    }

    /// The `batch_length` this batch's own bytes imply: the fixed header from
    /// `partition_leader_epoch` onwards, plus `record_data`.
    ///
    /// This is what `From<Batch> for Bytes` *writes after* the length field, and
    /// it is computed the same way `CrcData::into_batch` computes the field when
    /// it builds a batch from records.
    pub fn encoded_batch_length(&self) -> Result<i32> {
        i32::try_from(FIXED_BATCH_LENGTH + self.record_data.len()).map_err(Into::into)
    }

    /// Whether the `batch_length` field describes the bytes this batch would
    /// serialize to.
    ///
    /// It normally does, because every batch built from records takes the field
    /// from the payload (`CrcData::into_batch`) and every batch decoded from a
    /// v2 RecordBatch was framed by that same length. One shape breaks it: the
    /// pre-v2 husk `TryFrom<Bytes>` returns for `magic != 2` keeps the wire's
    /// `batch_length` — the length of a MessageSet this struct cannot represent
    /// — over an empty `record_data`, so `From<Batch> for Bytes` re-emits a
    /// header claiming bytes that are not there.
    ///
    /// A husk is meant to be refused before it can matter, with
    /// `UNSUPPORTED_FOR_MESSAGE_FORMAT` on the produce path (#320). This is the
    /// question a *writer* asks before committing bytes to storage, where the
    /// consequence of a mismatch is not a rejected request but a region no
    /// reader can ever decode (#393).
    pub fn declares_its_own_length(&self) -> bool {
        self.encoded_batch_length()
            .is_ok_and(|encoded| encoded == self.batch_length)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct CrcData {
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: u32,
    pub record_data: Bytes,
}

impl TryFrom<&CrcData> for Bytes {
    type Error = Error;

    fn try_from(value: &CrcData) -> result::Result<Self, Self::Error> {
        let mut encoded = value.size_in_bytes().map(BytesMut::with_capacity)?;
        encoded.put_i16(value.attributes);
        encoded.put_i32(value.last_offset_delta);
        encoded.put_i64(value.base_timestamp);
        encoded.put_i64(value.max_timestamp);
        encoded.put_i64(value.producer_id);
        encoded.put_i16(value.producer_epoch);
        encoded.put_i32(value.base_sequence);
        encoded.put_u32(value.record_count);
        encoded.put(&value.record_data[..]);

        Ok(Bytes::from(encoded))
    }
}

impl ByteSize for CrcData {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(size_of_val(&self.attributes)
            + size_of_val(&self.last_offset_delta)
            + size_of_val(&self.base_timestamp)
            + size_of_val(&self.max_timestamp)
            + size_of_val(&self.producer_id)
            + size_of_val(&self.producer_epoch)
            + size_of_val(&self.base_sequence)
            + size_of_val(&self.record_count)
            + self.record_data.len())
    }
}

impl From<&Batch> for CrcData {
    fn from(batch: &Batch) -> Self {
        Self {
            attributes: batch.attributes,
            last_offset_delta: batch.last_offset_delta,
            base_timestamp: batch.base_timestamp,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            record_count: batch.record_count,
            record_data: batch.record_data.clone(),
        }
    }
}

impl CrcData {
    fn into_batch(self, base_offset: i64, partition_leader_epoch: i32, magic: i8) -> Result<Batch> {
        let crc = self
            .crc()
            .inspect(|crc| debug!(?self, base_offset, partition_leader_epoch, magic, crc))?;

        Ok(Batch {
            base_offset,
            batch_length: i32::try_from(FIXED_BATCH_LENGTH + self.record_data.len())?,
            partition_leader_epoch,
            magic,
            crc,
            attributes: self.attributes,
            last_offset_delta: self.last_offset_delta,
            base_timestamp: self.base_timestamp,
            max_timestamp: self.max_timestamp,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self.base_sequence,
            record_count: self.record_count,
            record_data: self.record_data,
        })
    }

    fn crc(&self) -> Result<u32> {
        let encoded = Bytes::try_from(self)?;
        debug!(encoded = ?&encoded[..]);

        let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
        digest.update(&encoded[..]);

        Ok(digest.finalize() as u32)
    }
}

fn into_record_data(records: &[Record], compression: Compression) -> Result<Bytes> {
    let sizing = records
        .iter()
        .map(|record| record.size_in_bytes())
        .collect::<Result<Vec<_>>>()
        .map(|sizes| sizes.iter().sum::<usize>())?;

    debug!(sizing);

    match compression {
        Compression::None => records.encode(),

        Compression::Gzip => {
            let uncompressed = records.encode()?;

            let mut gz = GzEncoder::new(
                BytesMut::with_capacity(uncompressed.len()).writer(),
                flate2::Compression::default(),
            );

            gz.write_all(&uncompressed)?;

            gz.finish()
                .map(|w| w.into_inner())
                .map(Bytes::from)
                .map_err(Into::into)
        }

        Compression::Lz4 => {
            let uncompressed = records.encode()?;

            // Kafka's Java client requires BD.blockIndependence and rejects a
            // linked-block frame ("Dependent block stream is unsupported") —
            // the lz4 crate's default block mode (#253).
            let mut lz4 = lz4::EncoderBuilder::new()
                .block_mode(lz4::BlockMode::Independent)
                .build(BytesMut::with_capacity(uncompressed.len()).writer())?;

            lz4.write_all(&uncompressed[..])?;

            let (w, _) = lz4.finish();
            Ok(Bytes::from(w.into_inner()))
        }

        Compression::Zstd => {
            let uncompressed = records.encode()?;

            let mut zstd = zstd::stream::write::Encoder::new(
                BytesMut::with_capacity(uncompressed.len()).writer(),
                0,
            )?;

            zstd.write_all(&uncompressed[..])?;

            zstd.finish()
                .map(|w| w.into_inner())
                .map(Bytes::from)
                .map_err(Into::into)
        }

        unexpected => Err(Error::UnexpectedType(format!("{unexpected:?}",))),
    }
}

impl TryFrom<crate::record::inflated::Batch> for Batch {
    type Error = Error;

    fn try_from(batch: crate::record::inflated::Batch) -> std::result::Result<Self, Self::Error> {
        CrcData {
            attributes: batch.attributes,
            last_offset_delta: batch.last_offset_delta,
            base_timestamp: batch.base_timestamp,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            record_count: u32::try_from(batch.records.len())?,
            record_data: into_record_data(&batch.records[..], batch.compression()?)?,
        }
        .into_batch(batch.base_offset, batch.partition_leader_epoch, batch.magic)
    }
}

impl Batch {
    pub fn max_offset(&self) -> i64 {
        self.base_offset + i64::from(self.last_offset_delta)
    }

    fn compression(&self) -> Result<Compression> {
        Compression::try_from(self.attributes)
    }

    /// Whether this batch carries an LZ4 frame with **dependent (linked) blocks**,
    /// which no Kafka Java client can decode (#253).
    ///
    /// Every LZ4 frame this broker wrote before the encoder was corrected is one:
    /// the `lz4` crate defaults to `BlockMode::Linked`, and
    /// `KafkaLZ4BlockInputStream` rejects it in its constructor — before reading a
    /// single block, so an emptied compaction remnant compressing zero records is
    /// refused exactly like a full one.
    ///
    /// The frame descriptor is `magic(4) | FLG | BD | ...`; block independence is
    /// bit 5 of FLG. A batch this returns `true` for is durable damage: it stays
    /// unreadable until something re-encodes it, which is why the per-key compaction
    /// pass treats it as a reason to rewrite.
    pub fn has_dependent_lz4_blocks(&self) -> bool {
        /// LZ4 frame magic, little-endian on the wire.
        const MAGIC: [u8; 4] = [0x04, 0x22, 0x4d, 0x18];
        /// FLG bit 5.
        const BLOCK_INDEPENDENCE: u8 = 1 << 5;

        if !matches!(self.compression(), Ok(Compression::Lz4)) {
            return false;
        }

        // A frame shorter than its own descriptor is not one we can judge; leave it
        // to the decoder to reject rather than rewriting it blind.
        self.record_data
            .get(..5)
            .is_some_and(|head| head[..4] == MAGIC && head[4] & BLOCK_INDEPENDENCE == 0)
    }
}

/// Ceiling on how many [`Record`]s may be pre-allocated from a batch header
/// (#271).
///
/// `record_count` is a `u32` read straight off the wire, and
/// `Vec::with_capacity` sized from it is an unbounded allocation: at
/// `u32::MAX` that is hundreds of gibibytes. It is worse than a panic, because
/// `with_capacity` calls `handle_alloc_error` on failure, which **aborts the
/// process** rather than unwinding — so it is not confined to the request task,
/// and one frame takes the broker down with every connection on it.
///
/// The count is still honoured as a loop bound: the decode below fails on the
/// first record the payload cannot supply, so an impossible count is rejected
/// with an error as before. This only stops the wire from deciding how much
/// memory to reserve up front; a genuinely larger batch grows into place, which
/// is amortised and what the generated protocol decoder already relies on
/// (`Seq` implements no `size_hint()`).
const RECORD_PREALLOC_LIMIT: usize = 8 * 1024;

impl TryFrom<Batch> for Vec<Record> {
    type Error = Error;

    #[instrument(skip_all)]
    fn try_from(mut batch: Batch) -> Result<Self, Self::Error> {
        let record_count = usize::try_from(batch.record_count)?;
        let prealloc = record_count.min(RECORD_PREALLOC_LIMIT);

        debug!(?record_count);
        debug!(?batch.record_data);

        if batch
            .compression()
            .is_ok_and(|compression| compression == Compression::None)
        {
            let mut records = Vec::with_capacity(prealloc);

            for _ in 0..record_count {
                let record = Record::decode(&mut batch.record_data)?;
                records.push(record);
            }

            Ok(records)
        } else {
            let mut reader = batch
                .compression()
                .and_then(|compression| compression.inflator(batch.record_data.reader()))?;

            let mut decoder = Decoder::new(&mut reader);
            let mut records = Vec::with_capacity(prealloc);

            for _ in 0..record_count {
                let record = Record::deserialize(&mut decoder)?;
                records.push(record);
            }

            Ok(records)
        }
    }
}

impl TryFrom<&Batch> for Vec<Record> {
    type Error = Error;

    fn try_from(batch: &Batch) -> Result<Self, Self::Error> {
        let record_count = usize::try_from(batch.record_count)?;
        let prealloc = record_count.min(RECORD_PREALLOC_LIMIT);

        debug!(?record_count);
        debug!(?batch.record_data);

        let mut reader = batch
            .compression()
            .and_then(|compression| compression.inflator(batch.record_data.clone().reader()))?;

        let mut decoder = Decoder::new(&mut reader);
        let mut records = Vec::with_capacity(prealloc);

        for _ in 0..record_count {
            let record = Record::deserialize(&mut decoder)?;
            records.push(record);
        }

        Ok(records)
    }
}

const FIXED_BATCH_LENGTH: usize =
    // partition leader epoch
    size_of::<i32>()
    // magic
    + size_of::<i8>()
    // CRC
    + size_of::<u32>()
    // attributes
    + size_of::<i16>()
    // last_offset_delta
    + size_of::<i32>()
    // base timestamp
    + size_of::<i64>()
    // max timestamp
    + size_of::<i64>()
    // producer id
    + size_of::<i64>()
    // producer epoch
    + size_of::<i16>()
    // base sequence
    + size_of::<i32>()
    // record count
    + size_of::<u32>();

impl<'de> Deserialize<'de> for Batch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Batch;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(Batch))
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                debug!(v = ?v[..]);
                Batch::try_from(Bytes::from(v)).map_err(|err| de::Error::custom(err.to_string()))
            }
        }

        deserializer.deserialize_byte_buf(V)
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        BatchAttribute, ControlBatch, EndTransactionMarker, de::BatchDecoder, record::inflated,
    };

    use super::*;

    use tracing::subscriber::DefaultGuard;

    #[cfg(miri)]
    fn init_tracing() -> Result<()> {
        Ok(())
    }

    #[cfg(not(miri))]
    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        use tracing_subscriber::fmt::format::FmtSpan;

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_max_level(tracing::Level::DEBUG)
                .with_span_events(FmtSpan::ACTIVE)
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    const LOREM: &[u8] = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
    eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad \
    minim veniam, quis nostrud exercitation ullamco laboris nisi ut \
    aliquip ex ea commodo consequat. Duis aute irure dolor in \
    reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla \
    pariatur. Excepteur sint occaecat cupidatat non proident, sunt in \
    culpa qui officia deserunt mollit anim id est laborum.";

    #[test]
    fn decode_gzip() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 89, 0, 0, 0, 0, 2, 198, 48, 56, 83, 0, 1, 0, 0, 0, 0,
            0, 0, 1, 145, 183, 231, 239, 158, 0, 0, 1, 145, 183, 231, 239, 158, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 31, 139, 8, 0, 0, 0, 0,
            0, 0, 19, 53, 144, 205, 81, 67, 49, 12, 132, 31, 23, 104, 99, 11, 200, 188, 42, 224,
            198, 149, 2, 132, 172, 4, 205, 248, 47, 182, 148, 73, 9, 169, 153, 19, 50, 15, 110,
            150, 37, 173, 118, 191, 199, 203, 182, 109, 79, 223, 207, 239, 109, 72, 129, 246, 233,
            5, 169, 229, 54, 48, 213, 64, 69, 236, 4, 110, 117, 10, 155, 152, 15, 80, 210, 174,
            147, 181, 94, 32, 89, 163, 57, 37, 197, 2, 68, 125, 150, 150, 96, 82, 122, 44, 107,
            101, 77, 154, 188, 26, 220, 144, 233, 51, 228, 33, 118, 72, 11, 10, 93, 42, 129, 178,
            94, 157, 118, 124, 24, 164, 106, 9, 109, 20, 93, 143, 91, 148, 84, 78, 184, 186, 78,
            212, 54, 109, 120, 130, 220, 101, 176, 26, 153, 182, 10, 207, 153, 10, 183, 67, 121,
            13, 233, 212, 117, 233, 87, 82, 123, 12, 67, 40, 140, 151, 240, 212, 142, 0, 113, 202,
            118, 188, 46, 73, 114, 19, 232, 240, 112, 114, 100, 213, 138, 33, 125, 200, 151, 212,
            36, 35, 130, 199, 199, 173, 101, 239, 113, 78, 194, 78, 36, 133, 204, 41, 96, 205, 249,
            159, 80, 4, 114, 156, 253, 162, 100, 168, 203, 16, 58, 141, 40, 124, 236, 120, 187,
            179, 116, 19, 95, 24, 131, 65, 99, 38, 225, 152, 99, 239, 154, 200, 214, 70, 164, 232,
            163, 105, 146, 186, 40, 46, 82, 113, 148, 61, 119, 90, 185, 209, 206, 103, 101, 37, 36,
            153, 50, 86, 183, 180, 188, 108, 208, 2, 164, 129, 99, 254, 113, 245, 178, 111, 63,
            143, 62, 223, 101, 198, 1, 0, 0, 0, 0, 0,
        ];

        let decoder = BatchDecoder::new(Bytes::from_static(encoded));
        let decoded = Batch::deserialize(decoder)?;

        assert_eq!(
            Compression::Gzip,
            Compression::try_from(decoded.attributes)?
        );

        let mut inflated = crate::record::inflated::Batch::try_from(decoded.clone())
            .inspect(|inflated| debug!(?inflated))?;

        assert_eq!(
            Compression::None,
            Compression::try_from(inflated.attributes)?
        );

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            inflated.records
        );

        inflated.attributes = BatchAttribute::try_from(inflated.attributes)
            .map(|attribute| attribute.compression(Compression::Gzip).into())?;

        let deflated = Batch::try_from(inflated)?;
        assert_eq!(decoded.base_offset, deflated.base_offset);
        assert_eq!(
            decoded.partition_leader_epoch,
            deflated.partition_leader_epoch
        );
        assert_eq!(decoded.magic, deflated.magic);
        assert_eq!(decoded.attributes, deflated.attributes);
        assert_eq!(decoded.last_offset_delta, deflated.last_offset_delta);
        assert_eq!(decoded.base_timestamp, deflated.base_timestamp);

        let records: Vec<Record> = deflated.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    fn decode_zstd() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 91, 0, 0, 0, 0, 2, 200, 21, 172, 244, 0, 4, 0, 0, 0,
            0, 0, 0, 1, 145, 183, 250, 201, 221, 0, 0, 1, 145, 183, 250, 201, 221, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 40, 181, 47, 253, 0,
            88, 13, 9, 0, 70, 217, 64, 36, 160, 37, 73, 7, 255, 255, 255, 255, 143, 174, 211, 102,
            147, 114, 239, 182, 165, 188, 148, 244, 91, 75, 123, 39, 146, 211, 241, 3, 167, 201,
            64, 245, 234, 24, 45, 9, 56, 0, 53, 0, 53, 0, 197, 44, 90, 146, 147, 15, 123, 209, 29,
            99, 44, 57, 147, 242, 238, 145, 18, 167, 14, 240, 197, 53, 216, 71, 250, 57, 169, 162,
            68, 227, 112, 178, 27, 29, 160, 77, 21, 159, 138, 174, 28, 169, 201, 116, 94, 99, 116,
            4, 0, 36, 40, 40, 138, 34, 0, 130, 229, 38, 220, 115, 204, 62, 221, 154, 126, 195, 84,
            41, 89, 187, 99, 225, 7, 114, 194, 37, 137, 48, 157, 53, 61, 15, 29, 152, 186, 25, 81,
            115, 187, 41, 169, 154, 155, 139, 5, 74, 168, 60, 188, 84, 203, 12, 101, 106, 116, 141,
            206, 64, 149, 40, 177, 142, 234, 180, 74, 73, 43, 214, 13, 89, 104, 186, 46, 229, 163,
            78, 201, 197, 23, 35, 106, 44, 60, 89, 14, 81, 123, 241, 200, 196, 124, 192, 232, 24,
            213, 94, 163, 68, 175, 49, 171, 233, 41, 160, 4, 172, 60, 28, 57, 99, 110, 52, 5, 193,
            100, 114, 201, 107, 110, 85, 124, 41, 103, 138, 150, 200, 201, 164, 245, 241, 66, 225,
            250, 60, 180, 78, 201, 97, 153, 58, 64, 51, 249, 161, 83, 59, 43, 22, 22, 249, 201, 81,
            172, 225, 26, 227, 138, 246, 84, 148, 198, 227, 145, 24, 80, 66, 8, 0, 220, 218, 44,
            15, 30, 45, 186, 100, 95, 73, 49, 124, 17, 109, 0, 43, 70, 43, 93, 140, 227, 122, 67,
            136, 2, 0, 0, 0,
        ];

        let decoder = BatchDecoder::new(Bytes::from_static(encoded));
        let decoded = Batch::deserialize(decoder)?;

        assert_eq!(
            Compression::Zstd,
            Compression::try_from(decoded.attributes)?
        );

        let mut inflated = crate::record::inflated::Batch::try_from(decoded.clone())?;

        assert_eq!(
            Compression::None,
            Compression::try_from(inflated.attributes)?
        );

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            inflated.records
        );

        inflated.attributes = BatchAttribute::try_from(inflated.attributes)
            .map(|attribute| attribute.compression(Compression::Zstd).into())?;

        let deflated = Batch::try_from(inflated)?;
        assert_eq!(decoded.base_offset, deflated.base_offset);
        assert_eq!(
            decoded.partition_leader_epoch,
            deflated.partition_leader_epoch
        );
        assert_eq!(decoded.magic, deflated.magic);
        assert_eq!(decoded.attributes, deflated.attributes);
        assert_eq!(decoded.last_offset_delta, deflated.last_offset_delta);
        assert_eq!(decoded.base_timestamp, deflated.base_timestamp);

        let records: Vec<Record> = deflated.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    fn decode_lz4() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 237, 0, 0, 0, 0, 2, 43, 216, 167, 237, 0, 3, 0, 0, 0,
            0, 0, 0, 1, 145, 184, 77, 37, 242, 0, 0, 1, 145, 184, 77, 37, 242, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 4, 34, 77, 24, 96, 64,
            130, 173, 1, 0, 0, 242, 95, 136, 7, 0, 0, 0, 1, 250, 6, 76, 111, 114, 101, 109, 32,
            105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 105, 116, 32, 97, 109,
            101, 116, 44, 32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32, 97, 100,
            105, 112, 105, 115, 99, 105, 110, 103, 32, 101, 108, 105, 116, 44, 32, 115, 101, 100,
            32, 100, 111, 32, 101, 105, 117, 115, 109, 111, 100, 32, 116, 101, 109, 112, 111, 114,
            32, 105, 110, 99, 105, 100, 105, 100, 117, 110, 116, 32, 117, 116, 32, 108, 97, 98,
            111, 114, 101, 32, 101, 116, 91, 0, 240, 14, 101, 32, 109, 97, 103, 110, 97, 32, 97,
            108, 105, 113, 117, 97, 46, 32, 85, 116, 32, 101, 110, 105, 109, 32, 97, 100, 32, 109,
            105, 9, 0, 242, 26, 118, 101, 110, 105, 97, 109, 44, 32, 113, 117, 105, 115, 32, 110,
            111, 115, 116, 114, 117, 100, 32, 101, 120, 101, 114, 99, 105, 116, 97, 116, 105, 111,
            110, 32, 117, 108, 108, 97, 109, 99, 111, 90, 0, 0, 37, 0, 98, 105, 115, 105, 32, 117,
            116, 83, 0, 242, 1, 105, 112, 32, 101, 120, 32, 101, 97, 32, 99, 111, 109, 109, 111,
            100, 111, 193, 0, 112, 113, 117, 97, 116, 46, 32, 68, 83, 0, 162, 97, 117, 116, 101,
            32, 105, 114, 117, 114, 101, 145, 0, 240, 2, 32, 105, 110, 32, 114, 101, 112, 114, 101,
            104, 101, 110, 100, 101, 114, 105, 116, 17, 0, 176, 118, 111, 108, 117, 112, 116, 97,
            116, 101, 32, 118, 234, 0, 164, 32, 101, 115, 115, 101, 32, 99, 105, 108, 108, 34, 1,
            208, 101, 32, 101, 117, 32, 102, 117, 103, 105, 97, 116, 32, 110, 145, 0, 240, 4, 32,
            112, 97, 114, 105, 97, 116, 117, 114, 46, 32, 69, 120, 99, 101, 112, 116, 101, 117, 71,
            1, 240, 4, 110, 116, 32, 111, 99, 99, 97, 101, 99, 97, 116, 32, 99, 117, 112, 105, 100,
            97, 116, 50, 0, 160, 111, 110, 32, 112, 114, 111, 105, 100, 101, 110, 70, 1, 0, 42, 1,
            128, 105, 110, 32, 99, 117, 108, 112, 97, 248, 0, 224, 32, 111, 102, 102, 105, 99, 105,
            97, 32, 100, 101, 115, 101, 114, 30, 0, 64, 109, 111, 108, 108, 147, 1, 0, 33, 1, 240,
            1, 105, 100, 32, 101, 115, 116, 32, 108, 97, 98, 111, 114, 117, 109, 46, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];

        let decoder = BatchDecoder::new(Bytes::from_static(encoded));
        let decoded = Batch::deserialize(decoder)?;
        assert_eq!(Compression::Lz4, Compression::try_from(decoded.attributes)?);

        let mut inflated = crate::record::inflated::Batch::try_from(decoded.clone())?;

        assert_eq!(
            Compression::None,
            Compression::try_from(inflated.attributes)?
        );

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            inflated.records
        );

        inflated.attributes = BatchAttribute::try_from(inflated.attributes)
            .map(|attribute| attribute.compression(Compression::Lz4).into())?;

        let deflated = Batch::try_from(inflated)?;
        assert_eq!(decoded.base_offset, deflated.base_offset);
        assert_eq!(
            decoded.partition_leader_epoch,
            deflated.partition_leader_epoch
        );
        assert_eq!(decoded.magic, deflated.magic);
        assert_eq!(decoded.attributes, deflated.attributes);
        assert_eq!(decoded.last_offset_delta, deflated.last_offset_delta);
        assert_eq!(decoded.base_timestamp, deflated.base_timestamp);

        let records: Vec<Record> = deflated.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    fn lz4_frame_sets_block_independence() -> Result<()> {
        let _guard = init_tracing()?;

        // Kafka's Java client (KafkaLZ4BlockInputStream) mandates
        // BD.blockIndependence and throws "Dependent block stream is
        // unsupported" otherwise (issue #253). Assert on the raw frame
        // descriptor rather than round-tripping through our own decoder:
        // lz4::Decoder accepts both block modes, so a round-trip would pass
        // even when the frame is unreadable by every Kafka Java client.
        let deflated: Batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(LOREM).into()))
            .attributes(
                BatchAttribute::default()
                    .compression(Compression::Lz4)
                    .into(),
            )
            .build()
            .and_then(TryInto::try_into)?;

        let frame = &deflated.record_data[..];

        // LZ4 frame magic number 0x184D2204, little endian.
        assert_eq!([0x04, 0x22, 0x4D, 0x18], frame[..4]);

        // FLG is the byte following the magic; bit 5 is B.Indep.
        let flg = frame[4];
        assert_eq!(
            0b0010_0000,
            flg & 0b0010_0000,
            "FLG {flg:#010b} does not set block independence"
        );

        Ok(())
    }

    #[test]
    fn decode_snappy() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 218, 0, 0, 0, 0, 2, 228, 189, 111, 249, 0, 2, 0, 0, 0,
            0, 0, 0, 1, 145, 184, 92, 90, 192, 0, 0, 1, 145, 184, 92, 90, 192, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 198, 3, 240, 111, 136, 7,
            0, 0, 0, 1, 250, 6, 76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100, 111,
            108, 111, 114, 32, 115, 105, 116, 32, 97, 109, 101, 116, 44, 32, 99, 111, 110, 115,
            101, 99, 116, 101, 116, 117, 114, 32, 97, 100, 105, 112, 105, 115, 99, 105, 110, 103,
            32, 101, 108, 105, 116, 44, 32, 115, 101, 100, 32, 100, 111, 32, 101, 105, 117, 115,
            109, 111, 100, 32, 116, 101, 109, 112, 111, 114, 32, 105, 110, 99, 105, 100, 105, 100,
            117, 110, 116, 32, 117, 116, 32, 108, 97, 98, 111, 114, 101, 32, 101, 116, 32, 100, 1,
            91, 112, 101, 32, 109, 97, 103, 110, 97, 32, 97, 108, 105, 113, 117, 97, 46, 32, 85,
            116, 32, 101, 110, 105, 109, 32, 97, 100, 32, 109, 105, 1, 9, 160, 118, 101, 110, 105,
            97, 109, 44, 32, 113, 117, 105, 115, 32, 110, 111, 115, 116, 114, 117, 100, 32, 101,
            120, 101, 114, 99, 105, 116, 97, 116, 105, 111, 110, 32, 117, 108, 108, 97, 109, 99,
            111, 9, 90, 1, 37, 8, 105, 115, 105, 1, 106, 5, 83, 60, 105, 112, 32, 101, 120, 32,
            101, 97, 32, 99, 111, 109, 109, 111, 100, 111, 9, 193, 24, 113, 117, 97, 116, 46, 32,
            68, 1, 83, 36, 97, 117, 116, 101, 32, 105, 114, 117, 114, 101, 13, 236, 60, 105, 110,
            32, 114, 101, 112, 114, 101, 104, 101, 110, 100, 101, 114, 105, 116, 1, 17, 40, 118,
            111, 108, 117, 112, 116, 97, 116, 101, 32, 118, 1, 234, 36, 32, 101, 115, 115, 101, 32,
            99, 105, 108, 108, 49, 34, 232, 101, 32, 101, 117, 32, 102, 117, 103, 105, 97, 116, 32,
            110, 117, 108, 108, 97, 32, 112, 97, 114, 105, 97, 116, 117, 114, 46, 32, 69, 120, 99,
            101, 112, 116, 101, 117, 114, 32, 115, 105, 110, 116, 32, 111, 99, 99, 97, 101, 99, 97,
            116, 32, 99, 117, 112, 105, 100, 97, 116, 1, 50, 60, 111, 110, 32, 112, 114, 111, 105,
            100, 101, 110, 116, 44, 32, 115, 117, 110, 5, 117, 88, 99, 117, 108, 112, 97, 32, 113,
            117, 105, 32, 111, 102, 102, 105, 99, 105, 97, 32, 100, 101, 115, 101, 114, 1, 30, 12,
            109, 111, 108, 108, 33, 147, 33, 33, 60, 105, 100, 32, 101, 115, 116, 32, 108, 97, 98,
            111, 114, 117, 109, 46, 0, 0, 0, 0,
        ];

        let decoder = BatchDecoder::new(Bytes::from_static(encoded));
        let decoded = Batch::deserialize(decoder)?;
        assert_eq!(
            Compression::Snappy,
            Compression::try_from(decoded.attributes)?
        );

        let records: Vec<Record> = decoded.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    pub fn is_transactional() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch {
            base_offset: 0,
            batch_length: 68,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 3650210183,
            attributes: 16,
            last_offset_delta: 0,
            base_timestamp: 1729509915759,
            max_timestamp: 1729509915759,
            producer_id: 5,
            producer_epoch: 0,
            base_sequence: 0,
            record_count: 1,
            record_data: Bytes::from_static(b"$\0\0\0\x08\0\0\0\0\x10test0-ok\0"),
        };

        assert!(batch.is_transactional());

        Ok(())
    }

    #[test]
    pub fn is_transactional_control() -> Result<()> {
        use crate::record::inflated;

        let _guard = init_tracing()?;

        let deflated = Batch {
            base_offset: 1,
            batch_length: 66,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 820655041,
            attributes: 48,
            last_offset_delta: 0,
            base_timestamp: 1729509916024,
            max_timestamp: 1729509916024,
            producer_id: 5,
            producer_epoch: 0,
            base_sequence: -1,
            record_count: 1,
            record_data: Bytes::from_static(b" \0\0\0\x08\0\0\0\x01\x0c\0\0\0\0\0\0\0"),
        };

        assert!(deflated.is_transactional());
        assert!(deflated.is_control());

        let inflated = inflated::Batch::try_from(deflated)?;

        assert_eq!(1, inflated.records.len());
        assert_eq!(
            Some(Bytes::from_static(b"\0\0\0\x01")),
            inflated.records[0].key
        );

        let control_batch = ControlBatch::try_from(inflated.records[0].clone().key().unwrap())?;
        assert_eq!(0, control_batch.version);
        assert!(control_batch.is_commit());
        assert!(!control_batch.is_abort());

        assert_eq!(
            Some(Bytes::from_static(b"\0\0\0\0\0\0")),
            inflated.records[0].value
        );

        let txn_marker =
            EndTransactionMarker::try_from(inflated.records[0].clone().value.unwrap())?;
        assert_eq!(0, txn_marker.version);
        assert_eq!(0, txn_marker.coordinator_epoch);

        Ok(())
    }

    #[test]
    fn deflate() -> Result<()> {
        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
        let value = Bytes::from_static(b"consectetur adipiscing elit");

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_sequence = 78987;
        let base_offset = 9876789;
        let attributes: i16 = BatchAttribute::default().transaction(true).into();

        let batch: Batch = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .attributes(attributes)
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_offset(base_offset)
            .base_sequence(base_sequence)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        assert_eq!(base_sequence, batch.base_sequence);
        assert_eq!(producer_id, batch.producer_id);
        assert_eq!(producer_epoch, batch.producer_epoch);
        assert_eq!(attributes, batch.attributes);
        assert_eq!(1, batch.record_count);
        assert_eq!(base_offset, batch.base_offset);

        Ok(())
    }

    #[test]
    fn encode_decode() -> Result<()> {
        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
        let value = Bytes::from_static(b"consectetur adipiscing elit");

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_sequence = 78987;
        let base_offset = 9876789;
        let attributes: i16 = BatchAttribute::default().transaction(true).into();

        let batch: Batch = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .attributes(attributes)
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_offset(base_offset)
            .base_sequence(base_sequence)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        let expected = batch.batch_length as usize;

        let encoded = Bytes::from(batch);

        let deflated = Batch::try_from(encoded)?;

        assert_eq!(deflated.producer_id, producer_id);
        assert_eq!(deflated.producer_epoch, producer_epoch);
        assert_eq!(deflated.base_sequence, base_sequence);
        assert_eq!(deflated.base_offset, base_offset);
        assert_eq!(deflated.attributes, attributes);

        let inflated = inflated::Batch::try_from(deflated)?;
        assert_eq!(1, inflated.records.len());

        assert_eq!(Some(key), inflated.records[0].key);
        assert_eq!(Some(value), inflated.records[0].value);

        Ok(())
    }
}

#[cfg(test)]
mod record_prealloc {
    use super::*;
    use crate::{BatchAttribute, record::inflated};

    /// A batch header declaring `u32::MAX` records over a short payload must be
    /// **rejected with an error, and the process must survive** (#271).
    ///
    /// It used to size a `Vec` from that count — hundreds of gibibytes — and
    /// `Vec::with_capacity` calls `handle_alloc_error` on failure, which aborts
    /// the process instead of unwinding. So this was not a panic confined to one
    /// request task: one frame took the broker down with every connection on it.
    ///
    /// A test cannot observe an abort (it would take the test runner with it), so
    /// what is pinned is the reachable half: the conversion returns `Err` rather
    /// than trying to reserve the space first.
    #[test]
    fn an_impossible_record_count_is_rejected() {
        let batch = Batch {
            record_count: u32::MAX,
            record_data: Bytes::from_static(b"\x12\0\0\0\x01\x06foo\0"),
            attributes: BatchAttribute::default().into(),
            ..Default::default()
        };

        assert!(
            Vec::<Record>::try_from(batch).is_err(),
            "a count the payload cannot supply must be an error, not an allocation",
        );
    }

    /// **The converse.** A `record_count` that exactly matches its payload is
    /// accepted, unchanged.
    ///
    /// This is the half whose absence let #302 reach production: a guard was
    /// pinned to catch bad input and nothing pinned that good input still worked.
    /// Here it also covers the boundary the bound is derived from.
    #[test]
    fn a_record_count_matching_its_payload_is_accepted() -> Result<()> {
        for records in [1usize, 5, 100] {
            let mut builder = inflated::Batch::builder();

            for i in 0..records {
                builder = builder
                    .record(Record::builder().value(Some(Bytes::from(format!("value-{i}")))));
            }

            let deflated = builder
                .last_offset_delta(records as i32 - 1)
                .build()
                .and_then(Batch::try_from)?;

            assert_eq!(records as u32, deflated.record_count);

            let decoded = Vec::<Record>::try_from(deflated)?;

            assert_eq!(
                records,
                decoded.len(),
                "a batch of {records} records must decode to {records} records",
            );
        }

        Ok(())
    }

    /// A batch larger than the pre-allocation ceiling still decodes in full — the
    /// bound caps the *reservation*, never the result.
    #[test]
    fn a_batch_above_the_prealloc_ceiling_decodes_completely() -> Result<()> {
        let records = RECORD_PREALLOC_LIMIT + 17;
        let mut builder = inflated::Batch::builder();

        for _ in 0..records {
            builder = builder.record(Record::builder().value(Some(Bytes::from_static(b"v"))));
        }

        let deflated = builder
            .last_offset_delta(records as i32 - 1)
            .build()
            .and_then(Batch::try_from)?;

        assert_eq!(
            records,
            Vec::<Record>::try_from(deflated)?.len(),
            "the ceiling must bound the reservation, not the decode",
        );

        Ok(())
    }
}

#[cfg(test)]
mod crc_verification {
    use super::*;
    use crate::record::inflated;

    fn a_batch() -> Result<Batch> {
        inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(Some(Bytes::from_static(b"key")))
                    .value(Some(Bytes::from_static(b"value"))),
            )
            .build()
            .and_then(Batch::try_from)
    }

    /// **The converse, and the one that matters.** Every batch the builder
    /// produces verifies.
    ///
    /// [`Batch::computed_crc`] recomputes from the struct's fields while
    /// decoding digests a byte range; if those two ever disagreed on the shape
    /// of the digested region, `ProduceService` would reject *all* traffic.
    /// A round trip through the encoder is what pins that they agree.
    #[test]
    fn a_batch_and_its_decoded_form_both_verify() -> Result<()> {
        let batch = a_batch()?;

        assert!(batch.crc_matches()?, "a freshly built batch must verify");

        let decoded = Batch::try_from(Bytes::from(batch.clone()))?;

        assert!(decoded.crc_matches()?, "a decoded batch must verify");
        assert_eq!(batch.crc, decoded.crc);
        assert_eq!(batch.computed_crc()?, decoded.computed_crc()?);

        Ok(())
    }

    /// The digest covers the payload, so altering it is detected.
    #[test]
    fn an_altered_payload_does_not_verify() -> Result<()> {
        let batch = a_batch()?;

        let mut corrupt = BytesMut::from(&batch.record_data[..]);
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;

        let altered = Batch {
            record_data: corrupt.freeze(),
            ..batch.clone()
        };

        assert_ne!(batch.record_data, altered.record_data);
        assert!(
            !altered.crc_matches()?,
            "a payload byte flip must not verify"
        );

        Ok(())
    }

    /// `base_offset` sits before the CRC on the wire and is *not* covered by
    /// it — the broker assigns it at append time, so a batch whose offset was
    /// rewritten must still verify.
    #[test]
    fn assigning_a_base_offset_does_not_invalidate_the_crc() -> Result<()> {
        let batch = Batch {
            base_offset: 91_827,
            ..a_batch()?
        };

        assert!(batch.crc_matches()?, "base_offset is outside the digest");

        Ok(())
    }

    /// The mechanism the asymmetry rests on: `base_timestamp` **is** covered,
    /// so `ProduceService`'s LogAppendTime rewrite leaves the CRC stale.
    ///
    /// This is why the decoder stays permissive. A batch that took that
    /// rewrite is stored with a CRC that no longer matches, and refusing it on
    /// the way out would refuse data the broker itself wrote. If this test
    /// ever fails because the rewrite started recomputing the CRC, the
    /// permissive read side can be revisited — that is the point of pinning it.
    #[test]
    fn rewriting_a_covered_timestamp_leaves_the_crc_stale() -> Result<()> {
        let batch = a_batch()?;

        let rewritten = Batch {
            base_timestamp: batch.base_timestamp + 1,
            max_timestamp: batch.max_timestamp + 1,
            ..batch
        };

        assert!(
            !rewritten.crc_matches()?,
            "timestamps are inside the digest, so a rewrite invalidates the crc"
        );

        Ok(())
    }

    /// Decoding a corrupt batch **succeeds**, deliberately.
    ///
    /// Pinned so that turning the mismatch into an error is a visible decision
    /// with a failing test behind it, not a quiet edit. The rejection belongs
    /// to `ProduceService`, which can answer CORRUPT_MESSAGE; failing here
    /// fails the whole request and the connection dies with no response.
    #[test]
    fn decoding_does_not_enforce_the_crc() -> Result<()> {
        let batch = Batch {
            crc: a_batch()?.crc ^ 0xffff_ffff,
            ..a_batch()?
        };

        let decoded = Batch::try_from(Bytes::from(batch.clone()))?;

        assert_eq!(batch.crc, decoded.crc, "the bad crc is carried through");
        assert!(!decoded.crc_matches()?, "and it is still detectable");

        Ok(())
    }

    /// A real magic-0 MessageSet, captured from `sarama` on `Produce` v0.
    ///
    /// Taken from the `produce_request_v0_000` frame in `tests/decode.rs`: the
    /// 92 bytes of its `records` field, which is exactly what the decoder is
    /// handed for one batch.
    ///
    /// `offset(8) | size(4) = 80 | crc(4) | magic = 0 | attributes(1) |
    ///  key_len = -1 | value_len = 66 | value(66)`
    fn a_magic_0_message_set() -> Bytes {
        Bytes::from_static(&[
            // base offset
            0, 0, 0, 0, 0, 0, 0, 0, //
            // message size: 80
            0, 0, 0, 80, //
            // legacy CRC-32 — not a CRC-32C, and over a different range
            14, 140, 97, 161, //
            // magic
            0, //
            // attributes
            0, //
            // key: null
            255, 255, 255, 255, //
            // value: 66 bytes
            0, 0, 0, 66, //
            181, 164, 112, 10, 42, 24, 68, 168, 93, 201, 190, 85, 75, 81, 82, 227, 134, 137, 91,
            20, 86, 4, 92, 187, 141, 103, 65, 71, 241, 103, 73, 174, 19, 227, 180, 158, 176, 4, 27,
            78, 34, 140, 106, 1, 209, 63, 255, 52, 206, 164, 132, 184, 32, 34, 45, 24, 162, 18,
            187, 77, 19, 3, 161, 102, 20, 14,
        ])
    }

    /// A pre-v2 MessageSet decodes as *itself*, not as a v2 batch full of
    /// garbage (#320).
    ///
    /// Before this, the v2 field layout was read straight over a magic-0
    /// message and `record_count` came out as 2_920_539_060 — bytes 45..49 of
    /// a 66-byte payload, where a v2 batch keeps its record count. Nothing
    /// downstream could tell that apart from a batch that really did claim
    /// 2.9 billion records.
    #[test]
    fn a_pre_v2_message_set_is_not_decoded_as_v2() -> Result<()> {
        let encoded = a_magic_0_message_set();

        // The precondition: the trap is real in *these* bytes. Where a v2
        // batch keeps `record_count`, this capture holds part of its payload,
        // and reading it as a count gives 2_920_539_060. Without this, the
        // assertions below could pass over a capture that never had a
        // plausible-looking count to fabricate.
        assert_eq!(
            2_920_539_060u32,
            u32::from_be_bytes(encoded[57..61].try_into()?),
            "the v2 record_count slot must hold the number from #320"
        );

        let decoded = Batch::try_from(encoded)?;

        assert!(!decoded.is_record_batch_v2());
        assert_eq!(0, decoded.magic);

        // The three fields that are at known positions in both layouts.
        assert_eq!(0, decoded.base_offset);
        assert_eq!(80, decoded.batch_length);

        // And nothing was invented from the v2 layout. `record_count` is the
        // one that mattered: it sized an allocation until #306 bounded it.
        assert_eq!(0, decoded.record_count, "no fabricated record count");
        assert_eq!(0, decoded.attributes);
        assert_eq!(0, decoded.crc);
        assert!(decoded.record_data.is_empty());

        Ok(())
    }

    /// The v2 path is untouched: `magic` is checked, not merely present.
    #[test]
    fn a_v2_batch_still_decodes_in_full() -> Result<()> {
        let batch = a_batch()?;
        let decoded = Batch::try_from(Bytes::from(batch.clone()))?;

        assert!(decoded.is_record_batch_v2());
        assert_eq!(batch, decoded);

        Ok(())
    }
}
