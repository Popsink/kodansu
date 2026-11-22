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

//! Record batches between their two forms: a batch of records built and
//! deflated, as a producer sends it, and a deflated batch inflated, as a
//! consumer reads it. Uncompressed and LZ4, so the codec and the records'
//! own encoding can be told apart.
//!
//! Only the public API is used, so `--save-baseline` / `--baseline` compare two
//! revisions of the crate rather than two benchmarks.

use bytes::Bytes;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use tansu_sans_io::record::{Header, Record, deflated, inflated};
use tansu_sans_io::{BatchAttribute, Compression};

const RECORDS: i32 = 100;

/// A batch of [`RECORDS`] records, each with a key, a 256-byte value and one
/// header: the shape of a small CDC event.
fn batch(compression: Compression) -> inflated::Builder {
    let attributes = BatchAttribute::default().compression(compression);
    let records = (0..RECORDS).fold(inflated::Batch::builder(), |batch, delta| {
        let header = Header::builder()
            .key(Bytes::from_static(b"op"))
            .value(Bytes::from_static(b"u"));
        batch.record(
            Record::builder()
                .offset_delta(delta)
                .key(Some(Bytes::from(delta.to_be_bytes().to_vec())))
                .value(Some(Bytes::from_static(&[b'x'; 256])))
                .header(header),
        )
    });
    records
        .attributes(attributes.into())
        .last_offset_delta(RECORDS - 1)
}

fn record_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch");
    _ = group.throughput(Throughput::Elements(RECORDS as u64));

    for (name, compression) in [("none", Compression::None), ("lz4", Compression::Lz4)] {
        let builder = batch(compression);
        let deflated = builder
            .clone()
            .build()
            .and_then(deflated::Batch::try_from)
            .expect("deflated");

        _ = group.bench_function(format!("deflate/{name}"), |b| {
            b.iter_batched(
                || builder.clone(),
                |builder| {
                    builder
                        .build()
                        .and_then(deflated::Batch::try_from)
                        .expect("deflate")
                },
                BatchSize::SmallInput,
            );
        });

        _ = group.bench_function(format!("inflate/{name}"), |b| {
            b.iter(|| inflated::Batch::try_from(&deflated).expect("inflate"));
        });
    }

    group.finish();
}

criterion_group!(benches, record_batches);
criterion_main!(benches);
