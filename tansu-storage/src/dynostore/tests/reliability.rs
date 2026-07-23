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

//! Reliability invariants under concurrency: offsets must stay dense and
//! duplicate-free with many producers racing one partition's coalesce buffer,
//! and the wake-on-write + fetch-triggered-flush pipeline must actually carry
//! a tail consumer through a produce stream without stalls, with the recent-
//! write cache in the path. These tests assert *invariants* (coverage, order,
//! progress within a coarse deadline), not timings.

use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{TryStreamExt as _, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use tansu_sans_io::{
    ErrorCode, IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};

use crate::{
    Error, Result, Storage, TopicId, Topition,
    dynostore::{CoalesceTuning, DynoStore, tests::init_tracing},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// A non-idempotent batch of `records` records tagged with `producer`/`index`
/// so a fetched record is attributable to the produce that wrote it.
fn batch(producer: usize, index: usize, records: usize) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder();

    for i in 0..records {
        builder = builder.record(Record::builder().value(Some(Bytes::copy_from_slice(
            format!("p{producer}-b{index}-r{i}").as_bytes(),
        ))));
    }

    builder
        .last_offset_delta(records as i32 - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(storage: &DynoStore, name: &str, partitions: i32) -> Result<()> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

/// Drain every batch in `[offset, high)` the way the fetch service does:
/// repeated storage fetches, each resuming from the last batch returned.
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
                Duration::from_millis(500),
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

#[tokio::test]
async fn concurrent_coalesced_produces_keep_offsets_dense() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const PRODUCERS: usize = 8;
    const BATCHES: usize = 16;
    const RECORDS: usize = 2;
    const TOTAL: i64 = (PRODUCERS * BATCHES * RECORDS) as i64;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new())
        .produce_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            recent_cache_bytes: Some(8 << 20),
            ..Default::default()
        });

    let topic = "dense";
    create_topic(&store, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    // Many producers race one partition's coalesce buffer; each awaits its ack
    // (so its own batches are ordered) while interleaving with every other.
    let mut tasks = Vec::new();
    for producer in 0..PRODUCERS {
        let store = store.clone();
        let tp = tp.clone();
        tasks.push(tokio::spawn(async move {
            let mut assigned = Vec::with_capacity(BATCHES);
            for index in 0..BATCHES {
                assigned.push(
                    store
                        .produce(None, &tp, batch(producer, index, RECORDS)?)
                        .await?,
                );
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

    // Every ack is a distinct base offset and the set tiles [0, TOTAL) in
    // RECORDS-sized steps: no duplicate assignment, no gap, no overlap.
    acked.sort_unstable();
    assert_eq!(
        (0..TOTAL).step_by(RECORDS).collect::<Vec<_>>(),
        acked,
        "acked base offsets must tile [0, {TOTAL})"
    );

    // And the log reads back the same tiling (served from the cache).
    let collected = drain(&store, &tp, 0, TOTAL).await?;
    let mut read: Vec<i64> = collected.iter().map(|batch| batch.base_offset).collect();
    read.sort_unstable();
    assert_eq!(
        (0..TOTAL).step_by(RECORDS).collect::<Vec<_>>(),
        read,
        "fetched base offsets must tile [0, {TOTAL})"
    );

    Ok(())
}

#[tokio::test]
async fn tail_consumer_pipeline_makes_progress_under_wide_linger() -> Result<(), Error> {
    let _guard = init_tracing()?;

    const BATCHES: usize = 20;
    const RECORDS: usize = 2;
    const TOTAL: i64 = (BATCHES * RECORDS) as i64;

    // A 10s linger: only the fetch-triggered flush (armed by the parked
    // consumer) can carry this pipeline to completion inside the deadline.
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new())
        .produce_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            coalesce_linger: Some(Duration::from_secs(10)),
            recent_cache_bytes: Some(8 << 20),
            ..Default::default()
        });

    let topic = "pipeline";
    create_topic(&store, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    let producer = {
        let store = store.clone();
        let tp = tp.clone();
        tokio::spawn(async move {
            for index in 0..BATCHES {
                _ = store.produce(None, &tp, batch(0, index, RECORDS)?).await?;
            }
            Ok::<_, Error>(())
        })
    };

    // The consumer loop mirrors the fetch service: park (bounded, like a
    // client's max_wait), refetch, advance. Batches must arrive in offset
    // order with no duplicate and no gap.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut next = 0i64;

    while next < TOTAL {
        assert!(
            Instant::now() < deadline,
            "pipeline stalled at offset {next} of {TOTAL}"
        );

        store
            .await_produce(&[(tp.clone(), next)], Duration::from_millis(300))
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

/// Build an idempotent batch for `producer_id`/`epoch` starting at
/// `base_sequence` and carrying `records` records.
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

async fn init_idempotent(storage: &DynoStore) -> Result<i64> {
    let response = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;
    assert_eq!(ErrorCode::None, response.error);
    Ok(response.id)
}

/// Overwrite every object under `prefix` in the raw bucket with bytes that
/// decode to no batches: any data a later fetch returns came from memory.
async fn corrupt_all(bucket: &InMemory, prefix: &Path) -> usize {
    let locations: Vec<Path> = bucket
        .list(Some(prefix))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .expect("list");

    for location in &locations {
        _ = bucket
            .put(location, PutPayload::from_static(b"garbage"))
            .await
            .expect("put");
    }

    locations.len()
}

#[tokio::test]
async fn hybrid_seam_stitches_from_the_cache() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            recent_cache_bytes: Some(64 << 20),
            ..Default::default()
        });

    let topic = "org.env.conn.schema.hybrid";
    create_topic(&store, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    // A backfill-class batch (≥ 1000 records) on a not-yet-segmented topic
    // bypasses to the legacy `records/` path (#62); the small batch after it
    // coalesces into a shared segment — a hybrid topic with a seam at 1000.
    assert_eq!(0, store.produce(None, &tp, batch(0, 0, 1_000)?).await?);
    assert_eq!(1_000, store.produce(None, &tp, batch(0, 1, 2)?).await?);

    // Corrupt every durable object on both sides of the seam: a fetch that
    // still stitches [0, 1002) proves both regions served from the cache.
    let records = Path::from(format!(
        "clusters/{CLUSTER}/topics/{topic}/partitions/{:0>10}/records/",
        0
    ));
    let segments = Path::from(format!(
        "clusters/{CLUSTER}/prefixes/org.env.conn/segments/"
    ));
    assert_eq!(1, corrupt_all(&bucket, &records).await);
    assert_eq!(1, corrupt_all(&bucket, &segments).await);

    let collected = drain(&store, &tp, 0, 1_002).await?;
    assert_eq!(
        vec![0, 1_000],
        collected
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<_>>()
    );

    Ok(())
}

#[tokio::test]
async fn idempotent_retry_after_crash_with_parked_batch_is_admitted() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // Contract under test (#48): the lazy producer checkpoint "can only
    // re-accept a bounded tail on replay, never lose data". A batch parked in
    // a coalesce buffer is not durable; if the broker crashes before the
    // flush, the producer's retry of that batch must be ADMITTED — because
    // classifying it as a duplicate acks data that never reached the log.
    let bucket = InMemory::new();

    // The broker "before the crash": checkpoint on every advance, linger far
    // past the test so the parked batch never flushes.
    let crashed = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .produce_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            coalesce_linger: Some(Duration::from_secs(600)),
            producer_checkpoint_batches: Some(1),
            ..Default::default()
        });

    let topic = "crashed";
    create_topic(&crashed, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    let producer_id = init_idempotent(&crashed).await?;

    // The produce parks awaiting a flush that will never come; the idempotent
    // gate has already advanced — and durably checkpointed — its sequence.
    let parked = {
        let crashed = crashed.clone();
        let tp = tp.clone();
        let parked_batch = idempotent_batch(producer_id, 0, 0, 2)?;
        tokio::spawn(async move { crashed.produce(None, &tp, parked_batch).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !parked.is_finished(),
        "the batch should be parked, not flushed"
    );

    // Crash: the buffered batch is gone; nothing reached the log.
    parked.abort();
    drop(crashed);

    // A restarted broker on the same bucket must admit the producer's retry
    // of the SAME batch at offset 0 — the log provably does not contain it.
    let restarted = DynoStore::new(CLUSTER, NODE, bucket);
    match restarted
        .produce(None, &tp, idempotent_batch(producer_id, 0, 0, 2)?)
        .await
    {
        Ok(offset) => assert_eq!(0, offset),
        Err(error) => panic!(
            "retry of a never-durable batch must be admitted, got {error:?} \
             — the checkpoint ran ahead of the flush and the data is lost"
        ),
    }

    Ok(())
}

/// An `ObjectStore` that fails every `records/` PUT while `outage` is set — a
/// transient S3 outage confined to the batch-data path. Everything else
/// (topic metadata, watermarks, producer state, lists, gets) works normally.
#[derive(Clone)]
struct RecordsPutOutage<O> {
    inner: O,
    outage: Arc<AtomicBool>,
}

impl<O> RecordsPutOutage<O> {
    fn new(inner: O) -> Self {
        Self {
            inner,
            outage: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<O> Debug for RecordsPutOutage<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordsPutOutage").finish()
    }
}

impl<O> Display for RecordsPutOutage<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordsPutOutage").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for RecordsPutOutage<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if self.outage.load(Relaxed) && location.as_ref().contains("/records/") {
            return Err(object_store::Error::Generic {
                store: "outage",
                source: "injected records PUT outage".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

#[tokio::test]
async fn idempotent_retry_after_failed_flush_is_admitted() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // A transient object-store outage fails the coalesced flush, so the
    // parked producer is error-acked and retries. The failed flush wrote
    // NOTHING, so the retry must be admitted — misclassifying it as a
    // duplicate acks data that never reached the log (silent loss), and no
    // crash is required to hit it: an S3 blip suffices.
    let wrapped = RecordsPutOutage::new(InMemory::new());
    let outage = wrapped.outage.clone();
    let store = DynoStore::new(CLUSTER, NODE, wrapped).produce_coalesce(true);

    let topic = "outage";
    create_topic(&store, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    let producer_id = init_idempotent(&store).await?;

    outage.store(true, Relaxed);
    let denied = store
        .produce(None, &tp, idempotent_batch(producer_id, 0, 0, 2)?)
        .await;
    assert!(denied.is_err(), "the flush must fail during the outage");

    outage.store(false, Relaxed);
    match store
        .produce(None, &tp, idempotent_batch(producer_id, 0, 0, 2)?)
        .await
    {
        Ok(offset) => assert_eq!(0, offset),
        Err(error) => panic!(
            "retry after a failed flush must be admitted, got {error:?} \
             — the in-memory gate advanced for a batch the log never saw"
        ),
    }

    Ok(())
}

#[tokio::test]
async fn idempotent_retry_after_failed_flush_with_multiple_batches_is_fully_admitted()
-> Result<(), Error> {
    let _guard = init_tracing()?;

    // Two batches from the SAME producer, both parked in ONE coalesced
    // flush that then fails: the rollback must fully unwind BOTH advances,
    // in reverse arrival order (newest first), so each batch's retry lands
    // on its ORIGINAL base_sequence — not a partial unwind. Rolling back in
    // forward order would no-op on the first batch (by the time it's
    // processed, the live sequence has already moved past its target,
    // advanced further by the second batch), silently leaving the
    // producer's sequence ahead of where a full unwind requires and
    // misclassifying the first batch's retry as a duplicate. Regression
    // test for the `assign_and_create` refactor that centralized this
    // rollback: a forward-order loop would pass the single-batch sibling
    // test above but fail here.
    let wrapped = RecordsPutOutage::new(InMemory::new());
    let outage = wrapped.outage.clone();
    let store = DynoStore::new(CLUSTER, NODE, wrapped).produce_coalesce(true);

    let topic = "outage-multi";
    create_topic(&store, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    let producer_id = init_idempotent(&store).await?;

    outage.store(true, Relaxed);

    // Batch A [seq 0->2), batch B [seq 2->4): spawned concurrently so both
    // land in the SAME coalesce buffer before its flush fires and fails.
    let first = {
        let store = store.clone();
        let tp = tp.clone();
        tokio::spawn(async move {
            store
                .produce(None, &tp, idempotent_batch(producer_id, 0, 0, 2)?)
                .await
        })
    };
    let second = {
        let store = store.clone();
        let tp = tp.clone();
        tokio::spawn(async move {
            store
                .produce(None, &tp, idempotent_batch(producer_id, 0, 2, 2)?)
                .await
        })
    };

    assert!(
        first
            .await
            .map_err(|error| Error::Message(error.to_string()))?
            .is_err(),
        "batch A's flush must fail during the outage"
    );
    assert!(
        second
            .await
            .map_err(|error| Error::Message(error.to_string()))?
            .is_err(),
        "batch B's flush must fail during the outage"
    );

    outage.store(false, Relaxed);

    // Retry BOTH batches at their ORIGINAL sequences: a partial unwind
    // would leave the producer's sequence at 2 (only batch B's advance
    // undone), misclassifying batch A's retry (base_sequence=0) as a
    // duplicate.
    match store
        .produce(None, &tp, idempotent_batch(producer_id, 0, 0, 2)?)
        .await
    {
        Ok(offset) => assert_eq!(0, offset),
        Err(error) => panic!(
            "retry of batch A after a fully-failed multi-batch flush must be \
             admitted, got {error:?} — the rollback did not fully unwind"
        ),
    }

    match store
        .produce(None, &tp, idempotent_batch(producer_id, 0, 2, 2)?)
        .await
    {
        Ok(offset) => assert_eq!(2, offset),
        Err(error) => panic!(
            "retry of batch B after a fully-failed multi-batch flush must be \
             admitted, got {error:?}"
        ),
    }

    Ok(())
}

#[tokio::test]
async fn concurrent_checkpoint_writes_do_not_cross_contaminate_producers() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // Four distinct idempotent producers, each contributing one batch to
    // the SAME coalesced flush, with the checkpoint forced due on every
    // advance: `assign_and_create`'s success path now durability-notes all
    // four concurrently (`join_all`), each writing its own
    // `producers/{id}.json`. Regression test for that parallelization: a
    // bug that let one producer's snapshot or checkpoint write clobber
    // another's would show up here as a wrong post-restart admit/reject
    // decision for SOME producer, not necessarily all of them.
    const PRODUCERS: usize = 4;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .produce_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            producer_checkpoint_batches: Some(1),
            ..Default::default()
        });

    let topic = "cross-contaminate";
    create_topic(&store, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    let mut producer_ids = Vec::with_capacity(PRODUCERS);
    for _ in 0..PRODUCERS {
        producer_ids.push(init_idempotent(&store).await?);
    }

    // Every producer's single batch spawned concurrently, so all land in
    // the same coalesce window and flush together.
    let produces: Vec<_> = producer_ids
        .iter()
        .map(|&producer_id| {
            let store = store.clone();
            let tp = tp.clone();
            tokio::spawn(async move {
                store
                    .produce(None, &tp, idempotent_batch(producer_id, 0, 0, 2)?)
                    .await
            })
        })
        .collect();
    for produce in produces {
        _ = produce
            .await
            .map_err(|error| Error::Message(error.to_string()))??;
    }

    // Restart on the same bucket (cold checkpoint reads): each producer's
    // durable checkpoint must reflect exactly ITS OWN advance — a replay of
    // its original batch is a duplicate (durably written), while the next
    // in-order batch is admitted. Cross-contamination (producer A's
    // checkpoint holding producer B's sequence, or a lost/zeroed entry)
    // would flip one of these two outcomes for at least one producer.
    let restarted = DynoStore::new(CLUSTER, NODE, bucket);
    for &producer_id in &producer_ids {
        match restarted
            .produce(None, &tp, idempotent_batch(producer_id, 0, 0, 2)?)
            .await
        {
            Err(Error::Api(ErrorCode::DuplicateSequenceNumber)) => {}
            other => panic!(
                "producer {producer_id}'s replay of its own durable batch must be \
                 a duplicate, got {other:?} — checkpoint cross-contamination?"
            ),
        }

        match restarted
            .produce(None, &tp, idempotent_batch(producer_id, 0, 2, 2)?)
            .await
        {
            Ok(_) => {}
            other => panic!(
                "producer {producer_id}'s next in-order batch must be admitted, \
                 got {other:?} — checkpoint cross-contamination?"
            ),
        }
    }

    Ok(())
}

#[tokio::test]
async fn restart_recovers_the_tail_of_a_coalesced_object() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).produce_coalesce(true);

    let topic = "recover";
    create_topic(&store, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    // One linger window: three 2-record batches in ONE object named by base
    // offset 0 — the object name alone does not carry the span [0, 6).
    let (a, b, c) = tokio::join!(
        store.produce(None, &tp, batch(0, 0, 2)?),
        store.produce(None, &tp, batch(0, 1, 2)?),
        store.produce(None, &tp, batch(0, 2, 2)?),
    );
    let mut offsets = vec![a?, b?, c?];
    offsets.sort_unstable();
    assert_eq!(vec![0, 2, 4], offsets);

    // A restarted broker (cold hints) must recover next-offset 6 from the
    // multi-batch object, not from its name.
    let restarted = DynoStore::new(CLUSTER, NODE, bucket).produce_coalesce(true);
    assert_eq!(
        6,
        restarted
            .produce(None, &Topition::new(topic, 0), batch(0, 3, 2)?)
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn shared_segment_slices_stay_partition_isolated() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // Three partitions of one topic coalesce into ONE shared segment; each
    // partition's fetch is a byte-range slice of the cached object. With the
    // durable object corrupted, every partition must still read exactly its
    // own records from the cache — a slicing error would surface as another
    // partition's bytes decoding at the wrong offsets.
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            recent_cache_bytes: Some(8 << 20),
            ..Default::default()
        });

    let topic = "org.env.conn.schema.sliced";
    create_topic(&store, topic, 3).await?;

    let produces: Vec<_> = (0..3)
        .map(|partition| {
            let store = store.clone();
            let tp = Topition::new(topic, partition);
            tokio::spawn(async move {
                store
                    .produce(None, &tp, batch(partition as usize, 0, 2)?)
                    .await
            })
        })
        .collect();
    for produce in produces {
        assert_eq!(
            0,
            produce
                .await
                .map_err(|error| Error::Message(error.to_string()))??
        );
    }

    let segments = Path::from(format!(
        "clusters/{CLUSTER}/prefixes/org.env.conn/segments/"
    ));
    assert_eq!(
        1,
        corrupt_all(&bucket, &segments).await,
        "one shared segment"
    );

    for partition in 0..3 {
        let tp = Topition::new(topic, partition);
        let fetched = drain(&store, &tp, 0, 2).await?;
        assert_eq!(1, fetched.len());
        assert_eq!(0, fetched[0].base_offset);

        let inflated = inflated::Batch::try_from(&fetched[0])?;
        assert_eq!(2, inflated.records.len());
        let marker = format!("p{partition}-b0-r0");
        assert_eq!(
            Some(Bytes::copy_from_slice(marker.as_bytes())),
            inflated.records[0].value,
            "partition {partition} must read its own region, not a neighbour's"
        );
    }

    Ok(())
}

#[tokio::test]
async fn fetch_survives_a_segment_deleted_mid_window() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // Retention (or compaction's write→delete window) can delete a segment
    // between the index locating it and the data GET. The fetch must prune
    // the stale entry on the 404, restart cleanly, and return what survives —
    // never error, never loop. Cache off, so the GET path actually runs.
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);

    let topic = "org.env.conn.schema.raced";
    create_topic(&store, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    // Two windows → two segments: [0, 2) and [2, 4).
    assert_eq!(0, store.produce(None, &tp, batch(0, 0, 2)?).await?);
    assert_eq!(2, store.produce(None, &tp, batch(0, 1, 2)?).await?);

    // Retention wins the race: the older segment vanishes under the warm
    // index (deleted directly, so this process gets no invalidation).
    let segments = Path::from(format!(
        "clusters/{CLUSTER}/prefixes/org.env.conn/segments/"
    ));
    let mut locations: Vec<Path> = bucket
        .list(Some(&segments))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .expect("list");
    locations.sort();
    assert_eq!(2, locations.len());
    bucket.delete(&locations[0]).await.expect("delete");

    // A fetch from 0 hits the 404, prunes, restarts, and serves the survivor.
    let fetched = drain(&store, &tp, 0, 4).await?;
    assert_eq!(
        vec![2],
        fetched
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<_>>(),
        "the fetch must skip the deleted region and serve the survivor"
    );

    Ok(())
}

#[tokio::test]
async fn compaction_under_a_hot_cache_keeps_the_log_readable() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let tuning = CoalesceTuning {
        prefix_compact_min_segments: Some(2),
        prefix_compact_keep_hot: Some(0),
        prefix_compact_target_bytes: Some(1 << 30),
        recent_cache_bytes: Some(8 << 20),
        ..Default::default()
    };
    let bucket = InMemory::new();
    let store = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .coalesce_tuning(tuning);

    let topic = "org.env.conn.schema.compacted";
    create_topic(&store, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    // Four windows → four segments, all resident in the recent-write cache.
    for index in 0..4 {
        _ = store.produce(None, &tp, batch(0, index, 2)?).await?;
    }

    // Compaction merges them and deletes the originals: the cache now holds
    // only unreachable names (the index routes to the merged segment).
    assert!(store.compact_prefix_segments("org.env.conn").await? > 0);

    // The whole log stays readable — the merged segment is served from the
    // bucket (compaction never populates the cache, by design: cold data
    // must not evict the hot tail) and offsets are unchanged.
    let collected = drain(&store, &tp, 0, 8).await?;
    assert_eq!(
        vec![0, 2, 4, 6],
        collected
            .iter()
            .map(|batch| batch.base_offset)
            .collect::<Vec<_>>()
    );

    // D3 pinned: no surviving segment object was inserted into the cache.
    let segments = Path::from(format!(
        "clusters/{CLUSTER}/prefixes/org.env.conn/segments/"
    ));
    let survivors: Vec<Path> = bucket
        .list(Some(&segments))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .expect("list");
    assert!(!survivors.is_empty());
    for survivor in &survivors {
        assert!(
            !store
                .recent_writes
                .lock()
                .map(|cache| cache.contains(survivor))
                .map_err(Into::<Error>::into)?,
            "compaction must not populate the recent-write cache: {survivor}"
        );
    }

    Ok(())
}

#[tokio::test]
async fn produce_after_park_still_flushes_at_the_floor() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // The consumer parks BEFORE the window's first batch arrives (an empty
    // buffer at park time arms no trigger). The batch must still flush at
    // the fetch-flush floor — not wait out the full linger — or consumer
    // latency silently degrades to the linger whenever a poll lands ahead
    // of the produce, which for a tailing consumer is the common phase.
    let store = DynoStore::new(CLUSTER, NODE, InMemory::new())
        .produce_coalesce(true)
        .coalesce_tuning(CoalesceTuning {
            coalesce_linger: Some(Duration::from_secs(10)),
            ..Default::default()
        });

    let topic = "parked-first";
    create_topic(&store, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    let waiter = {
        let store = store.clone();
        let tp = tp.clone();
        tokio::spawn(async move {
            store
                .await_produce(&[(tp, 0)], Duration::from_secs(8))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The produce parks until its flush: demand (the parked waiter) must
    // collapse the 10s linger to the 50ms floor.
    let produced_at = Instant::now();
    assert_eq!(0, store.produce(None, &tp, batch(0, 0, 2)?).await?);
    assert!(
        produced_at.elapsed() < Duration::from_secs(5),
        "flush must be demand-triggered, not linger-bound: took {:?}",
        produced_at.elapsed()
    );

    waiter
        .await
        .map_err(|error| Error::Message(error.to_string()))??;

    Ok(())
}

#[tokio::test]
async fn leaseless_crash_replay_is_admitted_by_the_log() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // The SCOS design claim (docs/design-multiwriter-segments.md): under the
    // leaseless arbiter, dedup state is a pure function of the log's folded
    // producer coordinates — so a batch that never reached the log is
    // admitted on retry by ANY replica, with no per-pod gate to poison.
    let bucket = InMemory::new();
    let crashed = DynoStore::new(CLUSTER, NODE, bucket.clone())
        .prefix_coalesce(true)
        .prefix_leaseless(true)
        .coalesce_tuning(CoalesceTuning {
            coalesce_linger: Some(Duration::from_secs(600)),
            ..Default::default()
        });

    let topic = "org.env.conn.schema.leaseless";
    create_topic(&crashed, topic, 1).await?;
    let tp = Topition::new(topic, 0);

    let producer_id = init_idempotent(&crashed).await?;

    let parked = {
        let crashed = crashed.clone();
        let tp = tp.clone();
        let parked_batch = idempotent_batch(producer_id, 0, 0, 2)?;
        tokio::spawn(async move { crashed.produce(None, &tp, parked_batch).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !parked.is_finished(),
        "the batch should be parked, not flushed"
    );

    parked.abort();
    drop(crashed);

    // A restarted replica folds the (empty) log and must admit the retry.
    let restarted = DynoStore::new(CLUSTER, NODE, bucket)
        .prefix_coalesce(true)
        .prefix_leaseless(true);
    match restarted
        .produce(None, &tp, idempotent_batch(producer_id, 0, 0, 2)?)
        .await
    {
        Ok(offset) => assert_eq!(0, offset),
        Err(error) => panic!("the log-folded dedup must admit the retry, got {error:?}"),
    }

    Ok(())
}

#[tokio::test]
#[ignore = "KNOWN FAILURE — pre-existing: deleting a prefix-coalesced topic \
            cannot remove its records from *shared* segments, and footer \
            entries carry only the topic NAME, so a topic re-created under \
            the same name resurrects the old incarnation's log (HWM, fetch, \
            and offset assignment all continue from it). Needs a durable \
            per-prefix topic tombstone (deleted-through segment seq) honoured \
            by valid_substream_segments, compaction, and external S3-direct \
            readers — a format/contract decision, tracked for design review."]
async fn recreated_prefix_topic_starts_empty() -> Result<(), Error> {
    let _guard = init_tracing()?;

    // Kafka contract: deleting a topic deletes its log; a topic re-created
    // under the same name is a NEW topic — empty, offsets restarting at zero.
    // Under prefix coalescing the old records live in *shared* segments that
    // the delete cannot remove, so the recreated topic must not resurrect
    // them through the footer index.
    let bucket = InMemory::new();
    let topic = "org.env.conn.schema.reborn";
    let tp = Topition::new(topic, 0);

    {
        let store = DynoStore::new(CLUSTER, NODE, bucket.clone()).prefix_coalesce(true);
        create_topic(&store, topic, 1).await?;
        assert_eq!(0, store.produce(None, &tp, batch(0, 0, 5)?).await?);
        assert_eq!(
            ErrorCode::None,
            store.delete_topic(&TopicId::Name(topic.into())).await?
        );
    }

    // The re-create may land on any (here: freshly restarted) broker.
    let store = DynoStore::new(CLUSTER, NODE, bucket).prefix_coalesce(true);
    create_topic(&store, topic, 1).await?;

    let stage = store.offset_stage(&tp).await?;
    assert_eq!(
        0,
        stage.high_watermark(),
        "a recreated topic must be empty, not resurrect the old log"
    );

    let fetched = store
        .fetch(
            &tp,
            0,
            0,
            5 * 1024 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(500),
        )
        .await?;
    assert!(
        fetched.is_empty(),
        "fetched {} old batches from the deleted incarnation",
        fetched.len()
    );

    assert_eq!(
        0,
        store.produce(None, &tp, batch(1, 0, 2)?).await?,
        "offsets must restart at zero on a recreated topic"
    );

    Ok(())
}

#[tokio::test]
async fn await_wakes_on_any_of_many_partitions() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = DynoStore::new(CLUSTER, NODE, InMemory::new());
    let topic = "fanin";
    create_topic(&store, topic, 3).await?;

    let watched: Vec<(Topition, i64)> = (0..3).map(|p| (Topition::new(topic, p), 0)).collect();

    let producer = {
        let store = store.clone();
        let tp = Topition::new(topic, 2);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            store.produce(None, &tp, batch(0, 0, 2)?).await
        })
    };

    // A produce to ANY watched partition must wake the parked fetch.
    let parked_at = Instant::now();
    store
        .await_produce(&watched, Duration::from_secs(30))
        .await?;
    assert!(parked_at.elapsed() < Duration::from_secs(5));

    assert_eq!(
        0,
        producer
            .await
            .map_err(|error| Error::Message(error.to_string()))??
    );

    Ok(())
}
