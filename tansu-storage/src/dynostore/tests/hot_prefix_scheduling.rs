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

//! A dedicated maintainer must visit every hot prefix in every interval
//! (DEV-683).
//!
//! Once merges landed on a real GCS bucket (DEV-662), a prefix holding a
//! maintenance slot drained far faster than it was produced — and five of seven
//! hot prefixes still grew to 18–25k segments in an hour, because the
//! maintainer stopped visiting them. Three scheduling defects, each harmless
//! while compaction never landed a merge:
//!
//! 1. **The recency window skipped the maintainer's own prefixes.** A prefix
//!    worked to the end of a pass has its lease renewed — and so re-stamped —
//!    a few seconds before the next tick, so that tick found it "recent" and
//!    claimed only the prefixes the previous pass had *not* worked. Two
//!    disjoint sets alternated, each growing at the full produce rate while the
//!    other was compacted.
//! 2. **A pass never ended under load.** The backlog sweep kept going while a
//!    sweep merged anything, which under sustained produce is always, so the
//!    pass outlived the interval and the broker skipped the next tick.
//! 3. **The sweep read a stale backlog.** It chose prefixes by the cached
//!    segment count, which a maintainer that produces nothing never updates: a
//!    prefix drained below the trigger dropped out of the pass and regrew.
//!
//! The test here is the shape of that bucket run, scaled down: one producer,
//! one dedicated maintainer behind a slowed store, several prefixes under
//! sustained produce, and the broker's tick cadence replayed for several
//! intervals.

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use futures::TryStreamExt as _;
use object_store::{
    ObjectStore as _,
    memory::InMemory,
    path::Path,
    throttle::{ThrottleConfig, ThrottledStore},
};
use tansu_sans_io::{
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use tokio::time::{Instant, sleep, sleep_until};

use crate::{
    Error, LogStorage as _, MetadataStorage as _, Result, Topition,
    dynostore::{CoalesceTuning, DynoStore},
};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;

/// More hot prefixes than the maintainer has slots, as on the bucket.
const PREFIXES: usize = 4;
const CONCURRENCY: usize = 2;

/// The broker's tick, and the two windows the bucket ran at ~0.9× of it.
const INTERVAL: Duration = Duration::from_millis(1_500);
const WINDOW: Duration = Duration::from_millis(1_350);

/// Short against the interval, as 10 s is against 10 min: a prefix worked to
/// the end of a pass has its lease renewed, and its stamp moved, inside it.
const LEASE_TTL: Duration = Duration::from_millis(150);

/// Short against the interval, as 5 s is against 10 min: otherwise every
/// compaction run inside a pass reads the index the first one listed.
const HINT_TTL: Duration = Duration::from_millis(20);

const MIN_SEGMENTS: usize = 8;

/// Ticks replayed, the first of which only warms the maintainer's index.
const TICKS: usize = 6;

/// Each round trip the maintainer makes, so a sweep costs wall time.
const LATENCY: Duration = Duration::from_millis(1);

/// Produce before the first tick.
const WARM_UP: Duration = Duration::from_millis(200);

/// Pause between one produce to a prefix and the next.
const PRODUCE_EVERY: Duration = Duration::from_millis(5);

fn prefix(i: usize) -> String {
    format!("org.p{i}.conn")
}

fn topic(i: usize) -> String {
    format!("{}.table", prefix(i))
}

fn batch(value: &str) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::copy_from_slice(value.as_bytes()))))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(store: &DynoStore, name: &str) -> Result<()> {
    _ = store
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

async fn live(bucket: &InMemory, prefix: &str) -> usize {
    bucket
        .list(Some(&Path::from(format!(
            "clusters/{CLUSTER}/prefixes/{prefix}/segments/"
        ))))
        .try_collect::<Vec<_>>()
        .await
        .expect("list segments")
        .len()
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

/// Every hot prefix is claimed by every pass, every pass yields before the next
/// tick, and at every tick no prefix holds more than half of what was produced
/// to it over the interval just gone — which an interval without a merge of
/// that prefix cannot satisfy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dedicated_maintainer_merges_every_hot_prefix_every_interval() -> Result<(), Error> {
    let bucket = InMemory::new();

    let producer = DynoStore::new(CLUSTER, NODE, bucket.clone());
    for i in 0..PREFIXES {
        create_topic(&producer, &topic(i)).await?;
    }

    let slowed = ThrottledStore::new(
        bucket.clone(),
        ThrottleConfig {
            wait_put_per_call: LATENCY,
            wait_get_per_call: LATENCY,
            wait_list_per_call: LATENCY,
            wait_delete_per_call: LATENCY,
            ..ThrottleConfig::default()
        },
    );
    let maintainer = DynoStore::new(CLUSTER, NODE, slowed)
        .prefix_lease_ttl(LEASE_TTL)
        .coalesce_tuning(CoalesceTuning {
            prefix_compact_min_segments: Some(MIN_SEGMENTS),
            prefix_compact_keep_hot: Some(0),
            prefix_compact_target_bytes: Some(1 << 20),
            prefix_maintenance_concurrency: Some(CONCURRENCY),
            maintenance_recency: Some(WINDOW),
            watermark_hint_ttl: Some(HINT_TTL),
            maintenance_pass_budget: Some(WINDOW),
            ..Default::default()
        });

    let stop = Arc::new(AtomicBool::new(false));
    let produced: Arc<Vec<AtomicU64>> =
        Arc::new((0..PREFIXES).map(|_| AtomicU64::new(0)).collect());

    let producing: Vec<_> = (0..PREFIXES)
        .map(|i| {
            let producer = producer.clone();
            let stop = stop.clone();
            let produced = produced.clone();
            tokio::spawn(async move {
                let tp = Topition::new(topic(i), 0);
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    _ = producer
                        .produce(None, &tp, batch(&format!("v{n}"))?)
                        .await?;
                    _ = produced[i].fetch_add(1, Ordering::Relaxed);
                    n += 1;
                    sleep(PRODUCE_EVERY).await;
                }
                Ok::<_, Error>(())
            })
        })
        .collect();

    let all: BTreeSet<String> = (0..PREFIXES).map(prefix).collect();
    // Every prefix holds a segment before the first pass: an empty prefix has
    // no producer to wait for, and the sweep rightly lets it go.
    let start = Instant::now() + WARM_UP;
    let mut before = [0u64; PREFIXES];
    let mut failures = Vec::new();

    for tick in 0..TICKS {
        sleep_until(start + INTERVAL * tick as u32).await;

        if tick > 0 {
            for (i, before) in before.iter_mut().enumerate() {
                let total = produced[i].load(Ordering::Relaxed);
                let interval = total - *before;
                *before = total;

                let live = live(&bucket, &prefix(i)).await;
                eprintln!("tick {tick}: {} live={live} produced={interval}", prefix(i));
                if live as u64 > MIN_SEGMENTS as u64 + interval / 2 {
                    failures.push(format!(
                        "tick {tick}: {} holds {live} segments, {interval} produced in the interval",
                        prefix(i)
                    ));
                }
            }
        } else {
            for (i, before) in before.iter_mut().enumerate() {
                *before = produced[i].load(Ordering::Relaxed);
            }
        }

        let passed = Instant::now();
        let claimed = maintainer.claim_maintenance_prefixes(now_ms()).await?;
        if !all.is_subset(&claimed) {
            failures.push(format!(
                "tick {tick}: claimed {:?}",
                claimed.intersection(&all).collect::<Vec<_>>()
            ));
        }

        // Still running at the next tick is the broker's in-flight skip: that
        // tick, and the claim it would have made, are lost.
        let pass = tokio::time::timeout(
            INTERVAL,
            maintainer.maintain_prefix_segments(now_ms(), Some(&claimed)),
        )
        .await;
        eprintln!(
            "tick {tick}: claimed {} pass {:?}",
            claimed.len(),
            passed.elapsed()
        );
        match pass {
            Ok(pass) => _ = pass?,
            Err(_) => {
                failures.push(format!(
                    "tick {tick}: the pass outlived the interval, so the next tick is skipped"
                ));
                break;
            }
        }
    }

    stop.store(true, Ordering::Relaxed);
    for producing in producing {
        producing.await.expect("producer")?;
    }

    assert!(failures.is_empty(), "{failures:#?}");
    Ok(())
}
