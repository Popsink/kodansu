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
//! Kafka records, and the batches that carry them.
//!
//! A [`Record`] is put together with its [`Builder`], headers included:
//!
//! ```
//! use bytes::Bytes;
//! use tansu_sans_io::record::{Header, Record};
//!
//! let header = |key, value| {
//!     Header::builder()
//!         .key(Bytes::from_static(key))
//!         .value(Bytes::from_static(value))
//! };
//!
//! let record = Record::builder()
//!     .key(Some(Bytes::from_static(b"message")))
//!     .value(Some(Bytes::from_static(b"hello world!")))
//!     .header(header(b"format", b"text"))
//!     .header(header(b"importance", b"high"))
//!     .build()?;
//!
//! assert_eq!(2, record.headers.len());
//! # Ok::<(), tansu_sans_io::Error>(())
//! ```
//!
//! A batch exists in two forms. An [`inflated`] batch holds its records
//! decoded; a [`deflated`] batch holds them encoded, and compressed by the
//! batch's [`Compression`](super::Compression). A [`ProduceRequest`](super::ProduceRequest)
//! sends the deflated form:
//!
//! ```
//! use bytes::Bytes;
//! use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};
//! use tansu_sans_io::record::{Record, deflated, inflated};
//! use tansu_sans_io::{ApiKey as _, BatchAttribute, Compression, Frame, Header, ProduceRequest};
//!
//! let lz4 = BatchAttribute::default().compression(Compression::Lz4);
//! let batch = inflated::Batch::builder()
//!     .attributes(lz4.into())
//!     .record(Record::builder().value(Some(Bytes::from_static(b"hello world!"))))
//!     .build()
//!     .and_then(deflated::Batch::try_from)?;
//!
//! let partition = PartitionProduceData::default()
//!     .index(0)
//!     .records(Some(deflated::Frame { batches: vec![batch] }));
//! let topic = TopicProduceData::default()
//!     .name(String::from("test"))
//!     .partition_data(Some(vec![partition]));
//! let produce = ProduceRequest::default().topic_data(Some(vec![topic]));
//!
//! let header = Header::Request {
//!     api_key: ProduceRequest::KEY,
//!     api_version: 6,
//!     correlation_id: 12321,
//!     client_id: Some(String::from("tansu")),
//! };
//! let _encoded = Frame::request(header, produce.into())?;
//! # Ok::<(), tansu_sans_io::Error>(())
//! ```
//!
//! A [`FetchResponse`](super::FetchResponse) answers with the deflated form,
//! which is inflated before its records can be read:
//!
//! ```
//! use bytes::Bytes;
//! use tansu_sans_io::{ApiKey as _, FetchResponse, Frame, record::inflated};
//!
//! // A v16 fetch response: one topic, one partition, one batch of one record.
//! let wire = [
//!     0, 0, 0, 186, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 28, 205, 172, 195, 142, 19,
//!     71, 71, 182, 128, 13, 18, 65, 142, 210, 222, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//!     0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 74, 0, 0, 0,
//!     0, 0, 0, 0, 0, 0, 0, 0, 61, 255, 255, 255, 255, 2, 153, 143, 24, 144, 0, 0, 0, 0, 0, 0, 0,
//!     0, 1, 144, 238, 148, 84, 54, 0, 0, 1, 144, 238, 148, 84, 54, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
//!     0, 0, 0, 0, 0, 0, 0, 1, 22, 0, 0, 0, 1, 10, 112, 111, 105, 117, 121, 0, 3, 0, 13, 255, 255,
//!     255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
//!     13, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0,
//! ];
//!
//! let frame = Frame::response_from_bytes(&wire[..], FetchResponse::KEY, 16)?;
//! let response = FetchResponse::try_from(frame.body)?;
//!
//! let topics = response.responses.unwrap_or_default();
//! let partitions = topics.into_iter().flat_map(|topic| topic.partitions.unwrap_or_default());
//! let deflated = partitions
//!     .flat_map(|partition| partition.records)
//!     .flat_map(|frame| frame.batches)
//!     .next()
//!     .expect("one batch");
//! assert_eq!(12, deflated.record_data.len());
//!
//! let inflated = inflated::Batch::try_from(&deflated)?;
//! let first = inflated.records.first().and_then(|record| record.value());
//! assert_eq!(Some(Bytes::from_static(b"poiuy")), first);
//! # Ok::<(), tansu_sans_io::Error>(())
//! ```
pub(crate) mod codec;
pub mod deflated;
pub mod header;
pub mod inflated;

use crate::{
    ByteSize, Decode, Encode, Result,
    primitive::varint::{LongVarInt, VarInt},
};
use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use codec::{Octets, VarIntSequence};
pub use header::Header;
use serde::{
    Deserialize, Serialize, Serializer,
    ser::{self, SerializeSeq},
};
use tracing::{debug, instrument};

/// A Kafka API Record.
///
/// Note that is structure uses the same variant encoding as protobuf.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Record {
    #[serde(serialize_with = "VarInt::serialize")]
    #[serde(deserialize_with = "VarInt::deserialize")]
    pub length: i32,

    pub attributes: u8,

    #[serde(serialize_with = "LongVarInt::serialize")]
    #[serde(deserialize_with = "LongVarInt::deserialize")]
    pub timestamp_delta: i64,

    #[serde(serialize_with = "VarInt::serialize")]
    #[serde(deserialize_with = "VarInt::deserialize")]
    pub offset_delta: i32,

    #[serde(serialize_with = "Octets::serialize")]
    #[serde(deserialize_with = "Octets::deserialize")]
    pub key: Option<Bytes>,

    #[serde(serialize_with = "Octets::serialize")]
    #[serde(deserialize_with = "Octets::deserialize")]
    pub value: Option<Bytes>,

    #[serde(serialize_with = "VarIntSequence::<Header>::serialize")]
    #[serde(deserialize_with = "VarIntSequence::<Header>::deserialize")]
    pub headers: Vec<Header>,
}

impl ByteSize for Record {
    fn size_in_bytes(&self) -> Result<usize> {
        let Self {
            length,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
            ..
        } = self;

        let mut total = size_of::<u8>();
        total += VarInt::from(*length).size_in_bytes()?;
        total += LongVarInt::from(*timestamp_delta).size_in_bytes()?;
        total += VarInt::from(*offset_delta).size_in_bytes()?;
        for octets in [key, value] {
            total += Octets(octets.clone()).size_in_bytes()?;
        }
        Ok(total + VarIntSequence(headers.clone()).size_in_bytes()?)
    }
}

impl Encode for &[Record] {
    #[instrument(skip_all)]
    fn encode(&self) -> Result<Bytes> {
        let with_capacity = self.iter().try_fold(0, |total, record| {
            Ok::<_, crate::Error>(total + record.size_in_bytes()?)
        })?;
        debug!(records = self.len(), with_capacity);

        let mut encoded = BytesMut::with_capacity(with_capacity);
        for record in *self {
            encoded.extend_from_slice(&record.encode()?);
        }
        Ok(encoded.freeze())
    }
}

impl Encode for Record {
    #[instrument(skip_all)]
    fn encode(&self) -> Result<Bytes> {
        let with_capacity = self.size_in_bytes()?;
        debug!(with_capacity);

        let Self {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        } = self;

        let mut encoded = BytesMut::with_capacity(with_capacity);
        encoded.put(VarInt::from(*length).encode()?);
        encoded.put_u8(*attributes);
        encoded.put(LongVarInt::from(*timestamp_delta).encode()?);
        encoded.put(VarInt::from(*offset_delta).encode()?);
        for octets in [key, value] {
            encoded.put(Octets(octets.clone()).encode()?);
        }
        encoded.put(VarIntSequence(headers.clone()).encode()?);
        Ok(encoded.freeze())
    }
}

impl Decode for Record {
    #[instrument(skip_all)]
    fn decode(encoded: &mut Bytes) -> Result<Self> {
        debug!(encoded = ?encoded[..]);

        Ok(Self {
            length: VarInt::decode(encoded)?.into(),
            attributes: encoded.get_u8(),
            timestamp_delta: LongVarInt::decode(encoded)?.into(),
            offset_delta: VarInt::decode(encoded)?.into(),
            key: Octets::decode(encoded)?.into(),
            value: Octets::decode(encoded)?.into(),
            headers: VarIntSequence::decode(encoded)?.into(),
        })
    }
}

impl Record {
    #[must_use]
    pub fn builder() -> Builder {
        Default::default()
    }

    pub fn key(&self) -> Option<Bytes> {
        self.key.as_ref().cloned()
    }

    pub fn value(&self) -> Option<Bytes> {
        self.value.as_ref().cloned()
    }
}

impl TryFrom<Builder> for Record {
    type Error = crate::Error;

    fn try_from(builder: Builder) -> Result<Self, Self::Error> {
        let length = i32::try_from(builder.size_in_bytes()?)?;
        let Builder {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        } = builder;

        Ok(Self {
            length,
            attributes,
            timestamp_delta: timestamp_delta.into(),
            offset_delta: offset_delta.into(),
            key: key.into(),
            value: value.into(),
            headers: headers.into_iter().map(Header::from).collect(),
        })
    }
}

impl From<Record> for Builder {
    fn from(value: Record) -> Self {
        let Record {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
            ..
        } = value;

        Self {
            attributes,
            timestamp_delta: LongVarInt::from(timestamp_delta),
            offset_delta: VarInt::from(offset_delta),
            key: Octets::from(key),
            value: Octets::from(value),
            headers: headers.into_iter().map(header::Builder::from).collect(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder {
    attributes: u8,
    timestamp_delta: LongVarInt,
    offset_delta: VarInt,
    key: Octets,
    value: Octets,
    headers: VarIntSequence<header::Builder>,
}

impl Serialize for Builder {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let custom = |error: crate::Error| ser::Error::custom(error.to_string());
        let mut seq = serializer.serialize_seq(None)?;

        let size = self.size_in_bytes().map_err(custom);
        if let Ok(size) = size {
            _ = VarInt::try_from(size)
                .map_err(custom)
                .and_then(|length| seq.serialize_element(&length));
        }

        let attributes = seq.serialize_element(&self.attributes);
        let timestamp_delta = seq.serialize_element(&self.timestamp_delta);
        let offset_delta = seq.serialize_element(&self.offset_delta);
        let key = seq.serialize_element(&self.key);
        let value = seq.serialize_element(&self.value);
        let headers = seq.serialize_element(&self.headers);

        size.and(attributes)
            .and(timestamp_delta)
            .and(offset_delta)
            .and(key)
            .and(value)
            .and(headers)
            .and(seq.end())
    }
}

impl ByteSize for Builder {
    fn size_in_bytes(&self) -> Result<usize> {
        let mut total = size_of::<u8>();
        total += self.timestamp_delta.size_in_bytes()?;
        total += self.offset_delta.size_in_bytes()?;
        for octets in [&self.key, &self.value] {
            total += octets.size_in_bytes()?;
        }
        Ok(total + self.headers.size_in_bytes()?)
    }
}

impl Builder {
    #[must_use]
    pub fn attributes(self, attributes: u8) -> Self {
        Self { attributes, ..self }
    }

    #[must_use]
    pub fn timestamp_delta(self, timestamp_delta: i64) -> Self {
        let timestamp_delta = LongVarInt::from(timestamp_delta);
        Self {
            timestamp_delta,
            ..self
        }
    }

    #[must_use]
    pub fn offset_delta(self, offset_delta: i32) -> Self {
        let offset_delta = VarInt::from(offset_delta);
        Self {
            offset_delta,
            ..self
        }
    }

    #[must_use]
    pub fn key(self, key: Option<Bytes>) -> Self {
        let key = Octets::from(key);
        Self { key, ..self }
    }

    #[must_use]
    pub fn value(self, value: Option<Bytes>) -> Self {
        let value = Octets::from(value);
        Self { value, ..self }
    }

    #[must_use]
    pub fn header(mut self, header: header::Builder) -> Self {
        self.headers.0.push(header);
        self
    }

    pub fn build(self) -> Result<Record> {
        self.try_into()
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use super::{Octets, Record, codec::Sequence};
    use crate::{ByteSize as _, Decode as _, Encode as _, Error, Result, ser::RecordBatchEncoder};

    /// A record carrying only the value `def`, on the wire: length 9, no
    /// attributes, both deltas 0, a null key, a three-byte value, no headers.
    const DEF: [u8; 10] = [18, 0, 0, 0, 1, 6, 100, 101, 102, 0];

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

    fn def() -> super::Builder {
        Record::builder().value(Some(Bytes::from_static(b"def")))
    }

    /// What `value` serializes to through the record batch encoder.
    fn serialized(value: impl serde::Serialize) -> Result<Bytes> {
        let mut encoder = RecordBatchEncoder::new(BytesMut::new());
        value.serialize(&mut encoder)?;
        Ok(Bytes::from(encoder))
    }

    #[test]
    fn octets_are_prefixed_by_their_length() -> Result<()> {
        let _guard = init_tracing()?;
        assert_eq!(4, Octets::from(Some(vec![100, 101, 102])).size_in_bytes()?);
        Ok(())
    }

    #[test]
    fn a_builder_sizes_the_record_without_its_length() -> Result<()> {
        let _guard = init_tracing()?;
        assert_eq!(9, def().size_in_bytes()?);
        Ok(())
    }

    #[test]
    fn a_built_record_carries_the_builder_size_as_its_length() -> Result<()> {
        let _guard = init_tracing()?;
        assert_eq!(9, Record::try_from(def())?.length);
        Ok(())
    }

    #[test]
    fn a_builder_serializes_as_the_record_does() -> Result<()> {
        let _guard = init_tracing()?;
        assert_eq!(&DEF[..], &serialized(def())?[..]);
        Ok(())
    }

    #[test]
    fn a_record_encodes_and_decodes_through_the_wire_bytes() -> Result<()> {
        let _guard = init_tracing()?;

        let record = def().build()?;
        assert_eq!(Bytes::from_static(&DEF), record.encode()?);

        let mut encoded = Bytes::from_static(&DEF);
        assert_eq!(record, Record::decode(&mut encoded)?);
        assert!(encoded.is_empty());
        Ok(())
    }

    #[test]
    fn a_sequence_of_builders_is_prefixed_by_its_count() -> Result<()> {
        let _guard = init_tracing()?;

        let records = Sequence::from(vec![def()]);
        assert_eq!(14, records.size_in_bytes()?);

        let mut expected = vec![0, 0, 0, 1];
        expected.extend_from_slice(&DEF);
        assert_eq!(&expected[..], &serialized(records)?[..]);
        Ok(())
    }

    /// The checksum a batch header and the [`DEF`] record give under
    /// CRC-32C, the algorithm a v2 batch is checked with.
    #[test]
    fn a_batch_checksum_is_crc32c() -> Result<()> {
        let _guard = init_tracing()?;

        let header = [
            0, 0, 0, 0, 0, 0, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 1, 141, 116, 152, 137, 53, 0,
            0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1,
        ];
        let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
        digest.update(&header);
        digest.update(&DEF);
        assert_eq!(1_126_819_645, digest.finalize());
        Ok(())
    }
}
