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

use crate::{
    BatchAttribute, ByteSize, Compression, Error, Result, TimestampType,
    record::{Record, codec::Sequence, deflated},
    ser::RecordBatchEncoder,
    to_timestamp,
};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::debug;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Frame {
    pub batches: Vec<Batch>,
}

impl TryFrom<deflated::Frame> for Frame {
    type Error = Error;

    fn try_from(deflated: deflated::Frame) -> Result<Self, Self::Error> {
        let batches = deflated.batches.into_iter().map(Batch::try_from);
        Ok(Self {
            batches: batches.collect::<Result<_>>()?,
        })
    }
}

impl TryFrom<&deflated::Frame> for Frame {
    type Error = Error;

    fn try_from(deflated: &deflated::Frame) -> Result<Self, Self::Error> {
        let batches = deflated.batches.iter().map(Batch::try_from);
        Ok(Self {
            batches: batches.collect::<Result<_>>()?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "deflated::Batch")]
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

    #[serde(serialize_with = "Sequence::<Record>::serialize")]
    pub records: Vec<Record>,
}

const MAGIC: i8 = 2;

impl Default for Batch {
    fn default() -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH);
        let base_timestamp = now.map_or(0, |elapsed| elapsed.as_millis() as i64);
        let create_time = BatchAttribute::default()
            .compression(Compression::None)
            .timestamp(TimestampType::CreateTime);

        Self {
            base_offset: 0,
            batch_length: 0,
            partition_leader_epoch: -1,
            magic: MAGIC,
            crc: 0,
            attributes: create_time.into(),
            last_offset_delta: 0,
            base_timestamp,
            max_timestamp: base_timestamp,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: Vec::new(),
        }
    }
}

/// The header of a deflated batch with `attributes`, around `records`.
fn inflate(deflated: &deflated::Batch, attributes: i16, records: Vec<Record>) -> Batch {
    Batch {
        base_offset: deflated.base_offset,
        batch_length: deflated.batch_length,
        partition_leader_epoch: deflated.partition_leader_epoch,
        magic: deflated.magic,
        crc: deflated.crc,
        attributes,
        last_offset_delta: deflated.last_offset_delta,
        base_timestamp: deflated.base_timestamp,
        max_timestamp: deflated.max_timestamp,
        producer_id: deflated.producer_id,
        producer_epoch: deflated.producer_epoch,
        base_sequence: deflated.base_sequence,
        records,
    }
}

impl TryFrom<deflated::Batch> for Batch {
    type Error = Error;

    fn try_from(value: deflated::Batch) -> Result<Self, Self::Error> {
        let attribute = BatchAttribute::try_from(value.attributes)?;
        debug!(?attribute);
        let uncompressed = attribute.compression(Compression::None).into();

        let mut batch = inflate(&value, uncompressed, Vec::new());
        batch.records = Vec::<Record>::try_from(value)?;
        Ok(batch)
    }
}

impl TryFrom<&deflated::Batch> for Batch {
    type Error = Error;

    fn try_from(value: &deflated::Batch) -> Result<Self, Self::Error> {
        let records = Vec::<Record>::try_from(value)?;
        Ok(inflate(value, value.attributes, records))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Compaction {
    pub batch: Batch,
    pub records: usize,
}

impl Batch {
    pub fn compression(&self) -> Result<Compression> {
        self.attributes.try_into()
    }

    #[must_use]
    pub fn builder() -> Builder {
        Default::default()
    }

    pub fn into_builder(self) -> Builder {
        Builder::from(self)
    }

    pub fn max_offset(&self) -> i64 {
        i64::from(self.last_offset_delta) + self.base_offset
    }

    pub fn keys(&self) -> BTreeSet<Bytes> {
        self.records.iter().filter_map(Record::key).collect()
    }

    pub fn compact(mut self, head: &BTreeSet<Bytes>) -> Result<Compaction> {
        let mut latest = BTreeMap::new();
        let mut records = 0;

        for (key, offset_delta) in self
            .records
            .iter()
            .filter_map(|record| record.key().map(|key| (key, record.offset_delta)))
        {
            let superseded = head.contains(&key) || latest.insert(key, offset_delta).is_some();
            records += usize::from(superseded);
        }
        debug!(?records);

        if records == 0 {
            return Ok(Compaction {
                batch: self,
                records,
            });
        }

        let retained: BTreeSet<i32> = latest.into_values().collect();
        debug!(?retained);
        self.records
            .retain(|record| retained.contains(&record.offset_delta));

        let batch = self.into_builder().build()?;
        Ok(Compaction { batch, records })
    }
}

impl From<Batch> for Builder {
    fn from(value: Batch) -> Self {
        let Batch {
            base_offset,
            partition_leader_epoch,
            magic,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
            ..
        } = value;

        Self {
            base_offset,
            partition_leader_epoch,
            magic,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records: records.into_iter().map(super::Builder::from).collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Sequence<super::Builder>,
}

impl Default for Builder {
    fn default() -> Self {
        let base_timestamp = to_timestamp(&SystemTime::now()).unwrap_or(0);

        Self {
            base_offset: 0,
            partition_leader_epoch: -1,
            magic: MAGIC,
            attributes: 0,
            last_offset_delta: 0,
            base_timestamp,
            max_timestamp: base_timestamp,
            producer_id: -1,
            producer_epoch: 0,
            base_sequence: 0,
            records: Sequence(Vec::new()),
        }
    }
}

impl ByteSize for Builder {
    fn size_in_bytes(&self) -> Result<usize> {
        let fixed = size_of::<i32>()
            + size_of::<i8>()
            + size_of::<u32>()
            + size_of::<i16>()
            + size_of::<i32>()
            + 3 * size_of::<i64>()
            + size_of::<i16>()
            + size_of::<i32>();
        Ok(fixed + self.records.size_in_bytes()?)
    }
}

impl Builder {
    #[must_use]
    pub fn base_offset(mut self, base_offset: i64) -> Self {
        self.base_offset = base_offset;
        self
    }

    #[must_use]
    pub fn partition_leader_epoch(mut self, partition_leader_epoch: i32) -> Self {
        self.partition_leader_epoch = partition_leader_epoch;
        self
    }

    #[must_use]
    pub fn magic(mut self, magic: i8) -> Self {
        self.magic = magic;
        self
    }

    #[must_use]
    pub fn attributes(mut self, attributes: i16) -> Self {
        self.attributes = attributes;
        self
    }

    #[must_use]
    pub fn last_offset_delta(mut self, last_offset_delta: i32) -> Self {
        self.last_offset_delta = last_offset_delta;
        self
    }

    #[must_use]
    pub fn base_timestamp(mut self, base_timestamp: i64) -> Self {
        self.base_timestamp = base_timestamp;
        self
    }

    #[must_use]
    pub fn max_timestamp(mut self, max_timestamp: i64) -> Self {
        self.max_timestamp = max_timestamp;
        self
    }

    #[must_use]
    pub fn producer_id(mut self, producer_id: i64) -> Self {
        self.producer_id = producer_id;
        self
    }

    #[must_use]
    pub fn producer_epoch(mut self, producer_epoch: i16) -> Self {
        self.producer_epoch = producer_epoch;
        self
    }

    #[must_use]
    pub fn base_sequence(mut self, base_sequence: i32) -> Self {
        self.base_sequence = base_sequence;
        self
    }

    #[must_use]
    pub fn record(mut self, record: super::Builder) -> Self {
        self.records.0.push(record);
        self
    }

    /// The CRC-32C of everything after the checksum, from `attributes` on.
    fn crc(&self) -> Result<u32> {
        let mut checked = RecordBatchEncoder::new(BytesMut::with_capacity(self.size_in_bytes()?));
        self.attributes.serialize(&mut checked)?;
        self.last_offset_delta.serialize(&mut checked)?;
        self.base_timestamp.serialize(&mut checked)?;
        self.max_timestamp.serialize(&mut checked)?;
        self.producer_id.serialize(&mut checked)?;
        self.producer_epoch.serialize(&mut checked)?;
        self.base_sequence.serialize(&mut checked)?;
        self.records.serialize(&mut checked)?;

        let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
        digest.update(&Bytes::from(checked));
        Ok(digest.finalize() as u32)
    }

    pub fn build(self) -> Result<Batch> {
        let batch_length = i32::try_from(self.size_in_bytes()?)?;
        let crc = self.crc()?;

        let Self {
            base_offset,
            partition_leader_epoch,
            magic,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        } = self;

        Ok(Batch {
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
            records: records
                .into_iter()
                .map(Record::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use bytes::Bytes;
    use serde::Deserialize as _;

    use super::Batch;
    use crate::{Error, Result, de::BatchDecoder, record::Record};

    /// [`def`] on the wire: a 59-byte batch from producer 1 at sequence 1,
    /// whose one record carries the value `def`.
    const DEF: [u8; 71] = [
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 255, 255, 255, 255, 2, 67, 41, 231, 61, 0, 0, 0, 0, 0,
        0, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 18, 0, 0, 0, 1, 6, 100, 101, 102, 0,
    ];

    /// Logs this test at `debug` for this crate, on top of `RUST_LOG`, into
    /// `../logs/<package>/<thread>.log`: one file per test.
    fn init_tracing() -> Result<tracing::subscriber::DefaultGuard> {
        let directive = format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?;
        let filter = tracing_subscriber::EnvFilter::from_default_env().add_directive(directive);

        let unnamed = || Error::Message(String::from("unnamed thread"));
        let current = std::thread::current();
        let test = current.name().ok_or_else(unnamed)?;
        let log = std::fs::File::create(format!("../logs/{}/{test}.log", env!("CARGO_PKG_NAME")))?;

        let subscriber = tracing_subscriber::fmt().with_level(true);
        let subscriber = subscriber.with_line_number(true).with_thread_names(false);
        let subscriber = subscriber.with_env_filter(filter);
        let subscriber = subscriber.with_writer(std::sync::Arc::new(log));
        Ok(tracing::subscriber::set_default(subscriber.finish()))
    }

    /// A batch of one record with the value `def`, at a fixed time, from
    /// producer 1 at sequence 1.
    fn def() -> Result<Batch> {
        const AT: i64 = 1_707_058_170_165;
        let record = Record::builder().value(Some(Bytes::from_static(b"def")));
        Batch::builder()
            .base_timestamp(AT)
            .max_timestamp(AT)
            .producer_id(1)
            .base_sequence(1)
            .record(record)
            .build()
    }

    /// A record at `offset_delta`, keyed by `key` when there is one.
    fn keyed(offset_delta: i32, key: Option<&'static str>) -> crate::record::Builder {
        Record::builder()
            .offset_delta(offset_delta)
            .key(key.map(Bytes::from))
            .value(Some(Bytes::from(format!("v{offset_delta}"))))
    }

    #[test]
    fn a_built_batch_carries_its_length_and_checksum() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = def()?;
        assert_eq!((59, 1_126_819_645), (batch.batch_length, batch.crc));
        Ok(())
    }

    #[test]
    fn the_wire_bytes_decode_to_the_built_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let decoded = Batch::deserialize(BatchDecoder::new(Bytes::from_static(&DEF)))?;
        assert_eq!(def()?, decoded);
        Ok(())
    }

    #[test]
    fn every_record_added_is_built() -> Result<()> {
        let _guard = init_tracing()?;

        let keys = [
            "k1", "k2", "k1", "k1", "k3", "k2", "k4", "k5", "k5", "k2", "k6",
        ];
        let builder = (0..)
            .zip(keys)
            .fold(Batch::builder(), |builder, (delta, key)| {
                builder.record(keyed(delta, Some(key)))
            });
        assert_eq!(keys.len(), builder.build()?.records.len());
        Ok(())
    }

    /// Compaction keeps the latest record of each key the head does not
    /// already hold, drops keyless records, and counts what it superseded.
    #[test]
    fn compaction_keeps_the_latest_record_per_key() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = [Some("k1"), Some("k2"), Some("k1"), Some("k3"), None]
            .into_iter()
            .zip(0..)
            .fold(Batch::builder(), |builder, (key, delta)| {
                builder.record(keyed(delta, key))
            })
            .build()?;
        assert_eq!(
            BTreeSet::from(["k1", "k2", "k3"].map(Bytes::from)),
            batch.keys()
        );

        let head = BTreeSet::from([Bytes::from_static(b"k3")]);
        let compaction = batch.compact(&head)?;

        let deltas: Vec<_> = compaction
            .batch
            .records
            .iter()
            .map(|r| r.offset_delta)
            .collect();
        assert_eq!((2, vec![1, 2]), (compaction.records, deltas));
        Ok(())
    }
}
