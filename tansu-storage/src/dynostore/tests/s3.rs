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

//! The recent-write cache, wake-on-write long-poll, fetch-triggered flush and
//! durability-gated idempotent checkpoint, exercised against a *real* S3
//! object store (minio via `just ci`) instead of the in-memory store — real
//! conditional PUTs, real etags, real request latencies.
//!
//! Env-gated: each test no-ops (passes) unless `AWS_ENDPOINT` is set, exactly
//! like the CI environment (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/
//! `AWS_ALLOW_HTTP`). Every test uses a fresh uuid cluster namespace inside
//! the shared `tansu` bucket, so runs never contaminate each other.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload,
    aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut},
    path::Path,
};
use tansu_sans_io::{
    ErrorCode, IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use uuid::Uuid;

use crate::{
    Error, Result, Storage, Topition,
    dynostore::{CoalesceTuning, DynoStore, tests::init_tracing},
};

const NODE: i32 = 111;
const BUCKET: &str = "tansu";

/// A handle on the minio bucket, or `None` (test passes as a no-op) when no
/// object store is configured — mirrors how CI provides one.
fn s3() -> Option<AmazonS3> {
    if std::env::var("AWS_ENDPOINT").is_err() {
        eprintln!("skipping: AWS_ENDPOINT unset (start minio via `just ci`)");
        return None;
    }

    AmazonS3Builder::from_env()
        .with_bucket_name(BUCKET)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
        .inspect_err(|error| eprintln!("skipping: {error}"))
        .ok()
}

/// A per-test cluster namespace so concurrent/repeated runs never collide.
fn cluster() -> String {
    format!("s3-{}", Uuid::new_v4())
}

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

fn idempotent_batch(
    producer_id: i64,
    epoch: i16,
    base_sequence: i32,
    records: usize,
) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder()
        .producer_id(producer_id)
        .producer_epoch(epoch)
        .base_sequence(base_sequence)
        .last_offset_delta(records as i32 - 1);

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("record-{i}").as_bytes(),
        ))));
    }

    builder
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(storage: &DynoStore, name: &str) -> Result<()> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

async fn drain(
    store: &DynoStore,
    tp: &Topition,
    mut offset: i64,
    high: i64,
) -> Result<Vec<deflated::Batch>> {
    let mut collected = Vec::new();

    while offset < high {
        let fetched = store
            .fetch(
                tp,
                offset,
                0,
                5 * 1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::from_secs(2),
            )
            .await?;

        if fetched.is_empty() {
            break;
        }

        for batch in fetched {
            offset = offset.max(batch.base_offset + batch.last_offset_delta as i64 + 1);
            collected.push(batch);
        }
    }

    Ok(collected)
}

/// Overwrite every object under `prefix` in the real bucket with garbage that
/// decodes to no batches: any data a later fetch returns came from memory.
async fn corrupt_all(s3: &AmazonS3, prefix: &Path) -> usize {
    let locations: Vec<Path> = s3
        .list(Some(prefix))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .expect("list");

    for location in &locations {
        _ = s3
            .put(location, PutPayload::from_static(b"garbage"))
            .await
            .expect("put");
    }

    locations.len()
}

#[tokio::test]
async fn recent_writes_served_from_memory_not_s3() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let Some(raw) = s3() else { return Ok(()) };
    let Some(backing) = s3() else { return Ok(()) };

    let cluster = cluster();
    let store = DynoStore::new(&cluster, NODE, backing).coalesce_tuning(CoalesceTuning {
        recent_cache_bytes: Some(8 << 20),
        ..Default::default()
    });

    let topic = "tail";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    assert_eq!(0, store.produce(None, &tp, batch(2)?).await?);
    assert_eq!(2, store.produce(None, &tp, batch(2)?).await?);

    // The durable objects on minio are garbage now; only memory has the data.
    let records = Path::from(format!(
        "clusters/{cluster}/topics/{topic}/partitions/{:0>10}/records/",
        0
    ));
    assert_eq!(2, corrupt_all(&raw, &records).await);

    let collected = drain(&store, &tp, 0, 4).await?;
    assert_eq!(
        vec![0, 2],
        collected
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<_>>()
    );

    Ok(())
}

#[tokio::test]
async fn pipeline_under_wide_linger_completes_on_s3() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let Some(backing) = s3() else { return Ok(()) };

    const BATCHES: usize = 10;
    const RECORDS: usize = 2;
    const TOTAL: i64 = (BATCHES * RECORDS) as i64;

    // 10s linger: only the fetch-triggered flush can finish inside the
    // deadline, now with real PUT/GET latencies in the path.
    let cluster = cluster();
    let store = DynoStore::new(&cluster, NODE, backing)
        .produce_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            coalesce_linger: Some(Duration::from_secs(10)),
            recent_cache_bytes: Some(8 << 20),
            ..Default::default()
        });

    let topic = "pipeline";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    let producer = {
        let store = store.clone();
        let tp = tp.clone();
        tokio::spawn(async move {
            for _ in 0..BATCHES {
                _ = store.produce(None, &tp, batch(RECORDS)?).await?;
            }
            Ok::<_, Error>(())
        })
    };

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut next = 0i64;

    while next < TOTAL {
        assert!(
            Instant::now() < deadline,
            "pipeline stalled at offset {next} of {TOTAL}"
        );

        store
            .await_produce(&[(tp.clone(), next)], Duration::from_millis(500))
            .await?;

        for batch in drain(&store, &tp, next, TOTAL).await? {
            assert_eq!(next, batch.base_offset, "out-of-order or gapped read");
            next = batch.base_offset + batch.last_offset_delta as i64 + 1;
        }
    }

    producer
        .await
        .map_err(|error| Error::Message(error.to_string()))??;
    assert_eq!(TOTAL, next);

    Ok(())
}

#[tokio::test]
async fn idempotent_crash_replay_admitted_on_s3() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let Some(before) = s3() else { return Ok(()) };
    let Some(after) = s3() else { return Ok(()) };

    // The durability-gated checkpoint fix, against real conditional writes:
    // a batch parked in a coalesce buffer when the broker dies must be
    // admitted on retry by a restarted broker — never misclassified as a
    // duplicate (which a Kafka client treats as delivered: silent loss).
    let cluster = cluster();
    let crashed = DynoStore::new(&cluster, NODE, before)
        .produce_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            coalesce_linger: Some(Duration::from_secs(600)),
            producer_checkpoint_batches: Some(1),
            ..Default::default()
        });

    let topic = "crashed";
    create_topic(&crashed, topic).await?;
    let tp = Topition::new(topic, 0);

    let response = crashed.init_producer(None, 0, Some(-1), Some(-1)).await?;
    assert_eq!(ErrorCode::None, response.error);
    let producer_id = response.id;

    let parked = {
        let crashed = crashed.clone();
        let tp = tp.clone();
        let parked_batch = idempotent_batch(producer_id, 0, 0, 2)?;
        tokio::spawn(async move { crashed.produce(None, &tp, parked_batch).await })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !parked.is_finished(),
        "the batch should be parked, not flushed"
    );

    parked.abort();
    drop(crashed);

    let restarted = DynoStore::new(&cluster, NODE, after);
    match restarted
        .produce(None, &tp, idempotent_batch(producer_id, 0, 0, 2)?)
        .await
    {
        Ok(offset) => assert_eq!(0, offset),
        Err(error) => panic!("retry of a never-durable batch must be admitted, got {error:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn concurrent_produces_keep_offsets_dense_on_s3() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let Some(backing) = s3() else { return Ok(()) };

    const PRODUCERS: usize = 4;
    const BATCHES: usize = 8;
    const RECORDS: usize = 2;
    const TOTAL: i64 = (PRODUCERS * BATCHES * RECORDS) as i64;

    // No coalescing: every batch is its own create-only conditional PUT, so
    // this exercises real `If-None-Match: *` offset-CAS contention on minio.
    let cluster = cluster();
    let store = DynoStore::new(&cluster, NODE, backing);

    let topic = "dense";
    create_topic(&store, topic).await?;
    let tp = Topition::new(topic, 0);

    let mut tasks = Vec::new();
    for _ in 0..PRODUCERS {
        let store = store.clone();
        let tp = tp.clone();
        tasks.push(tokio::spawn(async move {
            let mut assigned = Vec::with_capacity(BATCHES);
            for _ in 0..BATCHES {
                assigned.push(store.produce(None, &tp, batch(RECORDS)?).await?);
            }
            Ok::<_, Error>(assigned)
        }));
    }

    let mut acked = Vec::new();
    for task in tasks {
        acked.extend(
            task.await
                .map_err(|error| Error::Message(error.to_string()))??,
        );
    }

    acked.sort_unstable();
    assert_eq!(
        (0..TOTAL).step_by(RECORDS).collect::<Vec<_>>(),
        acked,
        "acked base offsets must tile [0, {TOTAL})"
    );

    let collected = drain(&store, &tp, 0, TOTAL).await?;
    assert_eq!(TOTAL / RECORDS as i64, collected.len() as i64);

    Ok(())
}
