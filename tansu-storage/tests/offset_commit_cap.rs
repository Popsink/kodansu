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

//! A large group's offset commits under the GCS per-object write cap.
//!
//! Every `OffsetCommit` is one conditional write of the group's `offsets.json`
//! (#406), and the `gs` arm paces writes to one object at one per second
//! ([`PutRateLimiter`], #13), so every member of a group commits through one
//! key. Each commit used to be its own CAS: sixteen members committing at once
//! measured **65.7 s** of wall clock and a p50 of 39 s here, against 1.2 ms
//! uncapped — past a Kafka client's 30 s `request.timeout.ms`, so `commitSync`
//! failed and auto-commit fell behind without bound. Folding the concurrent
//! commits of a group into one write (`dynostore::commits`) brought it to two
//! writes and ~2.1 s.
//!
//! Measured over exactly the `gs` arm's decorator, with the `InMemory`
//! substitution `tansu-broker/tests/group_formation_cap.rs` uses: the limiter
//! is a local delay, so this reproduces the cap and nothing else of GCS — not
//! the 429, not the request latency, which a real bucket adds to every write.
//!
//! Wall clock, not paused time: `governor` keeps its own monotonic clock.

#![cfg(feature = "dynostore")]

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use object_store::memory::InMemory;
use tansu_sans_io::{ErrorCode, create_topics_request::CreatableTopic};
use tansu_storage::{
    DynoStore, LogStorage as _, MetadataStorage as _, OffsetCommitRequest, PutRateLimiter, Topition,
};
use tokio::{task::JoinSet, time::Instant};

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const TOPIC: &str = "capped";
const GROUP: &str = "large";

/// The 30 s default `request.timeout.ms` of a Kafka client: a commit answered
/// later than this is a failed `commitSync`. The margin to it is the point —
/// what has to be caught is a return of the per-commit shape, which is a
/// factor of thirty away, not a slow CI runner.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Exactly what `tansu_storage::gcs::object_store` wraps the client in.
fn gcs_shaped<O>(inner: O) -> PutRateLimiter<O> {
    PutRateLimiter::new(inner, Duration::from_mins(5))
        .with_rate_per_second(NonZeroU32::new(1))
        .with_jitter(Some(Duration::from_millis(50)))
}

/// `replicas` brokers over one bucket, each with its own limiter as each
/// process has and told its spacing as `StorageContainer`'s `gs` arm tells it,
/// and a topic with a partition per member.
async fn fleet(
    bucket: &Arc<InMemory>,
    replicas: usize,
    members: i32,
) -> tansu_storage::Result<Vec<Arc<DynoStore>>> {
    let fleet = (0..replicas)
        .map(|_| {
            Arc::new(
                DynoStore::new(CLUSTER, NODE, gcs_shaped(bucket.clone()))
                    .object_write_interval(Some(Duration::from_secs(1))),
            )
        })
        .collect::<Vec<_>>();

    _ = fleet[0]
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(members)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(fleet)
}

/// Every member commits its own partition at once, as auto-commit does on a
/// shared interval, member `m` through replica `m % replicas`. Returns the
/// slowest commit.
async fn commit_round(
    fleet: &[Arc<DynoStore>],
    members: i32,
    offset: i64,
) -> tansu_storage::Result<Duration> {
    let mut committing = JoinSet::new();

    for partition in 0..members {
        let replica = fleet[partition as usize % fleet.len()].clone();

        _ = committing.spawn(async move {
            let started = Instant::now();
            let topition = Topition::new(TOPIC, partition);
            let committed = replica
                .offset_commit(
                    GROUP,
                    None,
                    &[(
                        topition.clone(),
                        OffsetCommitRequest::default().offset(offset),
                    )],
                )
                .await?;

            assert_eq!(vec![(topition, ErrorCode::None)], committed);
            Ok::<_, tansu_storage::Error>(started.elapsed())
        });
    }

    let mut slowest = Duration::ZERO;
    while let Some(latency) = committing.join_next().await {
        slowest = slowest.max(latency.expect("member")?);
    }
    Ok(slowest)
}

/// Runs a measured round after an unmeasured one — the first write of
/// `offsets.json` is a create, not an update — then checks no commit the fold
/// carried was lost.
///
/// Read back through a replica that served none of it: a serving replica
/// answers a re-read of a key it just wrote from its own etag memo for five
/// seconds (`dynostore::metadata::Cache`), so it can show its own last write
/// rather than the bucket's. That window predates this test and is not what
/// it measures.
async fn measured(replicas: usize, members: i32) -> tansu_storage::Result<Duration> {
    let bucket = Arc::new(InMemory::new());
    let fleet = fleet(&bucket, replicas, members).await?;

    _ = commit_round(&fleet, members, 1).await?;
    let slowest = commit_round(&fleet, members, 2).await?;

    let topitions = (0..members)
        .map(|partition| Topition::new(TOPIC, partition))
        .collect::<Vec<_>>();

    let fetched = DynoStore::new(CLUSTER, NODE, bucket)
        .offset_fetch(Some(GROUP), &topitions, Some(false))
        .await?;
    assert_eq!(topitions.len(), fetched.len());
    assert!(
        fetched.values().all(|committed| committed.offset == 2),
        "{fetched:?}"
    );

    println!("replicas={replicas} members={members} slowest={slowest:?}");
    Ok(slowest)
}

/// Sixteen members on one replica: the shape #427 was measured at.
#[tokio::test(flavor = "multi_thread")]
async fn sixteen_members_commit_inside_a_request_timeout() -> tansu_storage::Result<()> {
    let slowest = measured(1, 16).await?;
    assert!(slowest < REQUEST_TIMEOUT / 6, "{slowest:?}");
    Ok(())
}

/// The fold does not grow with the group: a burst of any size is the write in
/// flight and the one that carries the rest.
#[tokio::test(flavor = "multi_thread")]
async fn a_burst_costs_two_writes_whatever_its_size() -> tansu_storage::Result<()> {
    let slowest = measured(1, 128).await?;
    assert!(slowest < REQUEST_TIMEOUT / 6, "{slowest:?}");
    Ok(())
}

/// Replicas do not coordinate, so three replicas serving one group still meet
/// in the CAS: each folds its own members, and the bound is a write per replica
/// per turn plus the conflicts between them.
#[tokio::test(flavor = "multi_thread")]
async fn three_replicas_meet_in_the_cas_once_each() -> tansu_storage::Result<()> {
    let slowest = measured(3, 48).await?;
    assert!(slowest < REQUEST_TIMEOUT / 2, "{slowest:?}");
    Ok(())
}

/// The acceptance bound of DEV-663: a synchronous commit answered within this,
/// at the 99th percentile, from a large group committing every second through
/// several replicas.
const SUSTAINED_P99: Duration = Duration::from_secs(10);

/// What a member's `commitSync` loop does: commit its own partition, wait a
/// second, commit the next offset, for `lasting`. Member `m` goes through
/// replica `m % replicas`, as a Service spreads connections. Returns every
/// commit's latency and each member's last committed offset.
async fn sustained(
    fleet: &[Arc<DynoStore>],
    members: i32,
    lasting: Duration,
) -> tansu_storage::Result<(Vec<Duration>, Vec<i64>)> {
    let mut committing = JoinSet::new();
    let until = Instant::now() + lasting;

    for partition in 0..members {
        let replica = fleet[partition as usize % fleet.len()].clone();

        _ = committing.spawn(async move {
            let topition = Topition::new(TOPIC, partition);
            let mut latencies = vec![];
            let mut offset = 0;

            while Instant::now() < until {
                offset += 1;
                let started = Instant::now();
                let committed = replica
                    .offset_commit(
                        GROUP,
                        None,
                        &[(
                            topition.clone(),
                            OffsetCommitRequest::default().offset(offset),
                        )],
                    )
                    .await?;
                latencies.push(started.elapsed());

                assert_eq!(vec![(topition.clone(), ErrorCode::None)], committed);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            Ok::<_, tansu_storage::Error>((partition, latencies, offset))
        });
    }

    let mut latencies = vec![];
    let mut last = vec![0; members as usize];
    while let Some(member) = committing.join_next().await {
        let (partition, member, offset) = member.expect("member")?;
        latencies.extend(member);
        last[partition as usize] = offset;
    }
    latencies.sort();
    Ok((latencies, last))
}

/// DEV-663: a large group committing synchronously every second through three
/// replicas. One burst (above) does not catch it: the replicas' turns collide
/// on every write, and each loser re-read, waited out its own pacing and met
/// the next winner. 35 s at p99 here before the fix; 29–80 s on a real bucket.
///
/// What this model leaves out is the bucket's own cap: each replica's limiter
/// is local, so three replicas could land three writes a second on one object
/// here, which GCS answers `429`. A shared limiter over the bucket was tried as
/// that cap and dropped: `governor` does not serve its waiters in order, so a
/// put could starve behind the others for tens of seconds, which measured the
/// limiter rather than the bucket.
///
/// Read back through a replica that served none of it, for the reason
/// [`measured`] gives.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_group_committing_every_second_through_three_replicas() -> tansu_storage::Result<()>
{
    const MEMBERS: i32 = 64;

    let bucket = Arc::new(InMemory::new());
    let fleet = fleet(&bucket, 3, MEMBERS).await?;
    let (latencies, last) = sustained(&fleet, MEMBERS, Duration::from_secs(30)).await?;

    let percentile = |p: usize| latencies[(latencies.len() * p / 100).min(latencies.len() - 1)];
    let (p50, p99, max) = (
        percentile(50),
        percentile(99),
        latencies[latencies.len() - 1],
    );
    println!(
        "commits={} p50={p50:?} p99={p99:?} max={max:?}",
        latencies.len()
    );

    let topitions = (0..MEMBERS)
        .map(|partition| Topition::new(TOPIC, partition))
        .collect::<Vec<_>>();
    let fetched = DynoStore::new(CLUSTER, NODE, bucket)
        .offset_fetch(Some(GROUP), &topitions, Some(false))
        .await?;
    for (topition, offset) in topitions.iter().zip(last) {
        assert_eq!(
            Some(offset),
            fetched.get(topition).map(|c| c.offset),
            "{topition:?}"
        );
    }

    assert!(p99 < SUSTAINED_P99, "p50={p50:?} p99={p99:?} max={max:?}");
    Ok(())
}
