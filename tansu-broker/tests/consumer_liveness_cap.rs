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

//! A large KIP-848 group heartbeats under the GCS per-object write cap (#627).
//!
//! Until #627 every member renewed its liveness by rewriting the group's one
//! document, `consumer.json`, at most once per `session/2`: N members made
//! N / (session/2) CAS writes a second on one object, which GCS caps at about
//! one. Each lost CAS is a re-read and a re-apply, and past
//! `MAX_GENERATION_CAS_ATTEMPTS` the heartbeat is answered
//! `COORDINATOR_LOAD_IN_PROGRESS`. A member now renews with an object of its
//! own, so a stable group leaves its document alone.
//!
//! The group is formed over the same bucket without the cap, because forming
//! it is a write per join, which the cap makes take N seconds and which is not
//! what #627 is about. The heartbeats are then served by two replicas over the
//! capped store, on the wall clock: `governor`, behind the limiter, keeps its
//! own monotonic clock, which `tokio`'s paused time never reaches.
//!
//! Gated on `dynostore`, which is what `DynoStore` and [`PutRateLimiter`] are
//! behind, as `group_formation_cap` is.

#![cfg(feature = "dynostore")]

use std::{
    collections::BTreeSet,
    num::NonZeroU32,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use object_store::{ObjectStore, memory::InMemory};
use tansu_broker::{
    Result,
    coordinator::group::{
        ConsumerGroupConfig, Coordinator as _, MigrationPolicy, administrator::Controller,
    },
};
use tansu_sans_io::{
    Body, ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse, ErrorCode,
    consumer_group_heartbeat_request::TopicPartitions, create_topics_request::CreatableTopic,
};
use tansu_storage::{DynoStore, LatencyIntroducingStorage, PutRateLimiter, Storage};
use tokio::{
    task::JoinSet,
    time::{Instant, sleep},
};
use uuid::Uuid;

const CLUSTER: &str = "tansu";
const NODE: i32 = 111;
const GROUP_ID: &str = "capped-consumers";
const TOPIC: &str = "t";
const PARTITIONS: i32 = 400;
const MEMBERS: usize = 200;

/// A renewal every 15 s per member: 200 members renewing on one object would
/// be ~13 writes a second against a cap of one, which is the storm #627 is
/// about. Long enough that forming the group, a debug build reading a
/// 200-member document per heartbeat, fits inside a session.
const CONFIG: ConsumerGroupConfig = ConsumerGroupConfig {
    session_timeout_ms: 30_000,
    heartbeat_interval_ms: 1_000,
    max_size: None,
    migration: MigrationPolicy::Upgrade,
};

/// How long the steady phase heartbeats: past every member's renewal.
const STEADY: Duration = Duration::from_secs(20);

/// Exactly what `StorageContainer::builder`'s `gs` arm wraps the store in.
fn gcs_shaped<O>(inner: O) -> PutRateLimiter<O> {
    PutRateLimiter::new(inner, Duration::from_mins(5))
        .with_rate_per_second(NonZeroU32::new(1))
        .with_jitter(Some(Duration::from_millis(50)))
}

/// One member, as the Java client drives it: adopt every epoch and assignment
/// it is answered, revoking at once whatever it is no longer given.
struct Member {
    member_id: String,
    epoch: i32,
    owned: BTreeSet<i32>,
}

impl Member {
    fn new() -> Self {
        Self {
            member_id: Uuid::new_v4().to_string(),
            epoch: 0,
            owned: BTreeSet::new(),
        }
    }

    async fn beat<O>(
        &mut self,
        coordinator: &Controller<O>,
        topic_id: Uuid,
    ) -> Result<ConsumerGroupHeartbeatResponse>
    where
        O: Storage + Clone,
    {
        let request = ConsumerGroupHeartbeatRequest::default()
            .group_id(GROUP_ID.into())
            .member_id(self.member_id.clone())
            .member_epoch(self.epoch)
            .rebalance_timeout_ms(if self.epoch == 0 { 300_000 } else { -1 })
            .subscribed_topic_names((self.epoch == 0).then(|| vec![TOPIC.into()]))
            .topic_partitions(Some(vec![
                TopicPartitions::default()
                    .topic_id(topic_id.into_bytes())
                    .partitions(Some(self.owned.iter().copied().collect())),
            ]));

        let Body::ConsumerGroupHeartbeatResponse(response) = coordinator
            .consumer_group_heartbeat(Some("capped"), None, &request)
            .await?
        else {
            panic!("not a heartbeat response");
        };

        if response.error_code == i16::from(ErrorCode::None) {
            self.epoch = response.member_epoch;

            if let Some(assignment) = response.assignment.as_ref() {
                self.owned = assignment
                    .topic_partitions
                    .iter()
                    .flatten()
                    .flat_map(|topic| topic.partitions.iter().flatten().copied())
                    .collect();
            }
        }

        Ok(response)
    }
}

/// A 200-member group heartbeats past every member's renewal over a store capped as
/// GCS caps one, served by two replicas: no heartbeat is refused, the document
/// is not written, and nobody lapses (#627).
#[tokio::test(flavor = "multi_thread")]
async fn a_large_group_heartbeats_under_the_per_object_cap() -> Result<()> {
    let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let uncapped = Arc::new(DynoStore::new(CLUSTER, NODE, bucket.clone()));

    let topic_id = uncapped
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(PARTITIONS)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let forming = Controller::with_storage(uncapped.clone())?.with_consumer_group_config(CONFIG);
    let mut members = (0..MEMBERS).map(|_| Member::new()).collect::<Vec<_>>();

    for _ in 0..16 {
        let before = members
            .iter()
            .map(|member| (member.epoch, member.owned.clone()))
            .collect::<Vec<_>>();

        for member in &mut members {
            let response = member.beat(&forming, topic_id).await?;
            assert_eq!(i16::from(ErrorCode::None), response.error_code);
        }

        if before
            == members
                .iter()
                .map(|member| (member.epoch, member.owned.clone()))
                .collect::<Vec<_>>()
        {
            break;
        }
    }

    let (formed, _) = uncapped
        .read_consumer_group(GROUP_ID)
        .await?
        .expect("a formed group");
    assert_eq!(MEMBERS, formed.members.len());

    let capped = LatencyIntroducingStorage::new(Arc::new(DynoStore::new(
        CLUSTER,
        NODE,
        gcs_shaped(bucket.clone()),
    )))
    .with_latency(0..1);

    let updates = capped.consumer_group_updates_handle();
    let renewals = capped.consumer_renewals_handle();

    let replicas = [
        Controller::with_storage(capped.clone())?.with_consumer_group_config(CONFIG),
        Controller::with_storage(capped.clone())?.with_consumer_group_config(CONFIG),
    ];

    let started = Instant::now();
    let mut beating = JoinSet::new();

    for (index, mut member) in members.into_iter().enumerate() {
        let replicas = replicas.clone();

        _ = beating.spawn(async move {
            let mut slowest = Duration::ZERO;
            let mut beat = 0usize;

            while started.elapsed() < STEADY {
                let sent = Instant::now();
                let response = member
                    .beat(&replicas[(index + beat) % replicas.len()], topic_id)
                    .await?;
                slowest = slowest.max(sent.elapsed());

                assert_eq!(
                    i16::from(ErrorCode::None),
                    response.error_code,
                    "{}: {:?}",
                    member.member_id,
                    response.error_message
                );

                beat += 1;
                sleep(Duration::from_millis(u64::try_from(
                    CONFIG.heartbeat_interval_ms,
                )?))
                .await;
            }

            Ok::<_, tansu_broker::Error>(slowest)
        });
    }

    let mut slowest = Duration::ZERO;

    while let Some(beaten) = beating.join_next().await {
        slowest = slowest.max(beaten.expect("member")?);
    }

    assert_eq!(
        0,
        updates.load(Ordering::Relaxed),
        "a stable group writes no document"
    );

    assert!(
        renewals.load(Ordering::Relaxed) >= MEMBERS as u64,
        "every member renewed its own stamp"
    );

    // Loose on purpose: a renewal queued behind the cap waits a second per
    // write ahead of it, so 200 of them on one object is minutes, and what a
    // loaded CI runner adds to an unqueued heartbeat is not (#627).
    assert!(
        slowest < Duration::from_secs(5),
        "the slowest heartbeat took {slowest:?}: nothing queued behind the cap"
    );

    let (held, _) = capped
        .read_consumer_group(GROUP_ID)
        .await?
        .expect("the group");

    assert_eq!(formed.group_epoch, held.group_epoch);
    assert_eq!(MEMBERS, held.members.len(), "nobody lapsed");

    Ok(())
}
