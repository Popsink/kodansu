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

//! Encoding the two frames a broker spends its time on: a produce request, as
//! a client sends it, and a fetch response, as the broker answers. Each at a
//! version before and after the protocol turned flexible, since the two take
//! different paths through the encoder.
//!
//! Only the public API is used, so `--save-baseline` / `--baseline` compare two
//! revisions of the crate rather than two benchmarks.

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use tansu_sans_io::fetch_response::{FetchableTopicResponse, PartitionData};
use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};
use tansu_sans_io::record::{Record, deflated, inflated};
use tansu_sans_io::{ApiKey as _, Body, FetchResponse, Frame, Header, ProduceRequest};

/// A deflated batch of 100 records of 256 bytes each.
fn batch() -> deflated::Frame {
    let records = (0..100).fold(inflated::Batch::builder(), |batch, delta| {
        batch.record(
            Record::builder()
                .offset_delta(delta)
                .value(Some(Bytes::from_static(&[b'x'; 256]))),
        )
    });
    let batch = records
        .last_offset_delta(99)
        .build()
        .and_then(deflated::Batch::try_from)
        .expect("batch");
    deflated::Frame {
        batches: vec![batch],
    }
}

fn produce() -> Body {
    let partition = PartitionProduceData::default()
        .index(0)
        .records(Some(batch()));
    let topic = TopicProduceData::default()
        .name(String::from("bench"))
        .partition_data(Some(vec![partition]));
    let request = ProduceRequest::default()
        .transactional_id(None)
        .acks(1)
        .timeout_ms(5_000)
        .topic_data(Some(vec![topic]));
    request.into()
}

fn fetch(flexible: bool) -> Body {
    let partition = PartitionData::default()
        .partition_index(0)
        .error_code(0)
        .high_watermark(100)
        .last_stable_offset(Some(100))
        .log_start_offset(flexible.then_some(0))
        .aborted_transactions(Some(Vec::new()))
        .preferred_read_replica(flexible.then_some(-1))
        .diverging_epoch(None)
        .current_leader(None)
        .snapshot_id(None)
        .records(Some(batch()));
    let topic = FetchableTopicResponse::default()
        .topic(Some(String::from("bench")))
        .topic_id(None)
        .partitions(Some(vec![partition]));
    FetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(flexible.then_some(0))
        .session_id(flexible.then_some(0))
        .node_endpoints(None)
        .responses(Some(vec![topic]))
        .into()
}

fn encoding(c: &mut Criterion) {
    for (name, api_version) in [("v3", 3), ("v9", 9)] {
        let body = produce();
        _ = c.bench_function(&format!("produce_request/{name}"), |b| {
            b.iter(|| {
                let header = Header::Request {
                    api_key: ProduceRequest::KEY,
                    api_version,
                    correlation_id: 1,
                    client_id: Some(String::from("bench")),
                };
                Frame::request(header, body.clone()).expect("produce")
            });
        });
    }

    for (name, api_version, flexible) in [("v4", 4, false), ("v12", 12, true)] {
        let body = fetch(flexible);
        _ = c.bench_function(&format!("fetch_response/{name}"), |b| {
            b.iter(|| {
                let header = Header::Response { correlation_id: 1 };
                Frame::response(header, body.clone(), FetchResponse::KEY, api_version)
                    .expect("fetch")
            });
        });
    }
}

criterion_group!(benches, encoding);
criterion_main!(benches);
