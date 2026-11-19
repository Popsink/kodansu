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

//! The usage the crate documents, run end to end: decoding a fetch response
//! and inflating its records, building records and batches, and encoding a
//! produce request.

use bytes::Bytes as Octets;
use tansu_sans_io::record::{Record, deflated, inflated};
use tansu_sans_io::{ApiKey as _, BatchAttribute, Compression, Error, Frame};

/// A v16 `Fetch` response, as hex, holding one batch of one record whose
/// value is `poiuy`.
const FETCH_V16: &str = concat!(
    "000000ba000000080000000000000000000000021ccdacc38e134747b6800d12",
    "418ed2de0200000000000000000000000000000000000000000001ffffffffff",
    "ffffff01000000004a00000000000000000000003dffffffff02998f18900000",
    "0000000000000190ee94543600000190ee945436000000000000000100000000",
    "00000000000116000000010a706f6975790003000dffffffffffffffffffffff",
    "ff000109000000000000000000020dffffffffffffffffffffffff000000",
);

fn unhex(text: &str) -> Vec<u8> {
    let pairs = text
        .as_bytes()
        .chunks(2)
        .map(|pair| std::str::from_utf8(pair).unwrap());
    pairs
        .map(|pair| u8::from_str_radix(pair, 16).unwrap())
        .collect()
}

/// The attributes of a batch compressed with LZ4.
fn lz4() -> i16 {
    let compressed = BatchAttribute::default().compression(Compression::Lz4);
    compressed.into()
}

/// The first batch of the first partition of the first topic in `response`.
fn first_batch(response: &tansu_sans_io::FetchResponse) -> Option<&deflated::Batch> {
    let topic = response.responses.as_deref()?.first()?;
    let partition = topic.partitions.as_deref()?.first()?;
    partition.records.as_ref()?.batches.first()
}

#[test]
fn a_fetch_response_decodes_and_its_batch_inflates() -> Result<(), Error> {
    use tansu_sans_io::FetchResponse;

    let encoded = unhex(FETCH_V16);
    let frame = Frame::response_from_bytes(&encoded[..], FetchResponse::KEY, 16)?;
    let response = FetchResponse::try_from(frame.body)?;

    let compressed = first_batch(&response).expect("deflated batch");
    assert_eq!(12, compressed.record_data.len());
    let records = inflated::Batch::try_from(compressed)?.records;
    let value = records.first().and_then(|record| record.value.clone());
    assert_eq!(Some(Octets::from_static(b"poiuy")), value);
    Ok(())
}

#[test]
fn a_record_builds_with_a_key_a_value_and_headers() {
    use tansu_sans_io::record::Header;

    let header = |key: &'static [u8], value: &'static [u8]| {
        let header = Header::builder().key(Octets::from_static(key));
        header.value(Octets::from_static(value))
    };
    let format = header(b"format", b"text");
    let importance = header(b"importance", b"high");
    let keyed = Record::builder().key(Some(Octets::from_static(b"message")));
    let valued = keyed.value(Some(Octets::from_static(b"hello world!")));
    let _record = valued.header(format).header(importance);
}

#[test]
fn an_lz4_batch_deflates_and_inflates_back() -> Result<(), Error> {
    let millis = 1_234_567_890_000;
    let hello = Some(Octets::from_static(b"hello world!"));

    let batch = inflated::Batch::builder().attributes(lz4());
    let batch = batch.base_timestamp(millis).max_timestamp(millis);
    let record = Record::builder().value(hello.clone());
    let batch = batch.record(record).build()?;
    let round_tripped = inflated::Batch::try_from(deflated::Batch::try_from(batch)?)?;

    let first = round_tripped.records.first();
    assert_eq!(hello, first.and_then(|record| record.value.clone()));
    Ok(())
}

#[test]
fn a_produce_request_of_one_lz4_batch_encodes() -> Result<(), Error> {
    use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};
    use tansu_sans_io::{Header, ProduceRequest};

    let hello = Some(Octets::from_static(b"hello world!"));
    let batch = inflated::Batch::builder().attributes(lz4());
    let batch = batch.record(Record::builder().value(hello)).build()?;
    let batches = vec![deflated::Batch::try_from(batch)?];

    let partition = PartitionProduceData::default().index(0);
    let partition = partition.records(Some(deflated::Frame { batches }));
    let topic = TopicProduceData::default().name("test".into());
    let topic = topic.partition_data(Some(vec![partition]));
    let request = ProduceRequest::default().topic_data(Some(vec![topic]));

    let client_id = Some(String::from("tansu"));
    let (api_key, api_version) = (ProduceRequest::KEY, 6);
    let header = Header::Request {
        api_key,
        api_version,
        correlation_id: 12321,
        client_id,
    };
    let _encoded = Frame::request(header, request.into())?;
    Ok(())
}
