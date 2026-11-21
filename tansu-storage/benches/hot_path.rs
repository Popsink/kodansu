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

//! The request path a broker spends its time on, served the way the broker
//! routes a decoded frame: a produce, a fetch, and the metadata a client asks
//! for between them, each over `memory://`.
//!
//! Only the public services are used, so the same file builds against any
//! revision that exports them and `--save-baseline` / `--baseline` compare two
//! revisions of the engine rather than two benchmarks.
//!
//! `memory://` has no produce batcher (it is only layered over S3 and GCS), and
//! no network, so this measures the engine's own work and nothing else. The
//! coalescer flushes on every batch rather than on its linger: a produce would
//! otherwise measure the timer, the same ~50 ms whatever it carries.

use std::sync::Arc;

use bytes::Bytes;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use rama::{Context, Service};
use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_sans_io::fetch_request::{FetchPartition, FetchTopic};
use tansu_sans_io::metadata_request::MetadataRequestTopic;
use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};
use tansu_sans_io::record::{Record, deflated, inflated};
use tansu_sans_io::{
    CreateTopicsRequest, FetchRequest, IsolationLevel, MetadataRequest, ProduceRequest,
};
use tansu_storage::{
    CreateTopicsService, FetchService, MetadataService, ProduceService, Storage, StorageContainer,
};
use tokio::runtime::Runtime;
use url::Url;

const TOPIC: &str = "hot-path";

/// The value every record carries: the size of a small JSON event.
const VALUE: &[u8; 256] = &[b'x'; 256];

/// How many batches a fetch has to choose from.
const BACKLOG: usize = 100;

type Engine = Arc<dyn Storage>;

fn context(storage: &Engine) -> Context<Engine> {
    Context::with_state(Arc::clone(storage))
}

async fn engine() -> Engine {
    let advertised = Url::parse("tcp://localhost:9092").expect("listener");
    let storage = Url::parse("memory://tansu/?coalesce_batches=1").expect("storage");
    let storage = StorageContainer::builder()
        .cluster_id("bench")
        .node_id(111)
        .advertised_listener(advertised)
        .storage(storage)
        .build()
        .await
        .expect("engine");

    let topic = CreatableTopic::default()
        .name(TOPIC.to_owned())
        .num_partitions(1)
        .replication_factor(1)
        .assignments(Some(Vec::new()))
        .configs(Some(Vec::new()));
    let request = CreateTopicsRequest::default().topics(Some(vec![topic]));
    _ = CreateTopicsService
        .serve(context(&storage), request)
        .await
        .expect("topic");
    storage
}

/// A produce of one batch of `records` records to partition 0 of [`TOPIC`].
fn produce(records: usize) -> ProduceRequest {
    let batch = (0..records).fold(inflated::Batch::builder(), |batch, _| {
        batch.record(Record::builder().value(Some(Bytes::from_static(VALUE))))
    });
    let batch = batch
        .last_offset_delta(i32::try_from(records).expect("records") - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .expect("batch");
    let data = PartitionProduceData::default()
        .index(0)
        .records(Some(deflated::Frame {
            batches: vec![batch],
        }));
    let topic = TopicProduceData::default()
        .name(TOPIC.to_owned())
        .partition_data(Some(vec![data]));
    ProduceRequest::default()
        .acks(1)
        .timeout_ms(5_000)
        .topic_data(Some(vec![topic]))
}

/// A fetch of partition 0 of [`TOPIC`] from `offset`, answered as soon as there
/// is a byte to return.
fn fetch(offset: i64) -> FetchRequest {
    let partition = FetchPartition::default()
        .partition(0)
        .fetch_offset(offset)
        .partition_max_bytes(1024 * 1024);
    let topic = FetchTopic::default()
        .topic(Some(TOPIC.to_owned()))
        .partitions(Some(vec![partition]));
    FetchRequest::default()
        .max_wait_ms(500)
        .min_bytes(1)
        .max_bytes(Some(1024 * 1024))
        .isolation_level(Some(i8::from(IsolationLevel::ReadUncommitted)))
        .topics(Some(vec![topic]))
}

fn metadata() -> MetadataRequest {
    MetadataRequest::default().topics(Some(vec![
        MetadataRequestTopic::default()
            .name(Some(TOPIC.to_owned()))
            .topic_id(None),
    ]))
}

fn producing(c: &mut Criterion, runtime: &Runtime) {
    let mut group = c.benchmark_group("produce");

    for records in [1, 100] {
        let storage = runtime.block_on(engine());
        let request = produce(records);
        _ = group.throughput(Throughput::Elements(records as u64));
        _ = group.bench_function(format!("{records}_records"), |b| {
            b.to_async(runtime).iter_batched(
                || request.clone(),
                |request| async {
                    ProduceService
                        .serve(context(&storage), request)
                        .await
                        .expect("produce")
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn fetching(c: &mut Criterion, runtime: &Runtime) {
    let mut group = c.benchmark_group("fetch");

    let storage = runtime.block_on(async {
        let storage = engine().await;
        for _ in 0..BACKLOG {
            _ = ProduceService
                .serve(context(&storage), produce(10))
                .await
                .expect("backlog");
        }
        storage
    });

    for (name, offset) in [("head", 0), ("middle", 500)] {
        _ = group.bench_function(name, |b| {
            b.to_async(runtime).iter(|| async {
                FetchService
                    .serve(context(&storage), fetch(offset))
                    .await
                    .expect("fetch")
            });
        });
    }

    group.finish();
}

fn describing(c: &mut Criterion, runtime: &Runtime) {
    let storage = runtime.block_on(engine());
    _ = c.bench_function("metadata/one_topic", |b| {
        b.to_async(runtime).iter(|| async {
            MetadataService
                .serve(context(&storage), metadata())
                .await
                .expect("metadata")
        });
    });
}

fn hot_path(c: &mut Criterion) {
    let runtime = Runtime::new().expect("runtime");
    producing(c, &runtime);
    fetching(c, &runtime);
    describing(c, &runtime);
}

criterion_group!(benches, hot_path);
criterion_main!(benches);
