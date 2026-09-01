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

//! A long poll has more than one way to end, and they meant opposite things
//! from the same silence (#497).
//!
//! A `JoinGroup` answered because the barrier fired and one answered because
//! `session/2` elapsed with nothing to act on were the same event in the
//! metrics — which is the difference between a barrier that works and a timer
//! that ran out, and it is what the deferred half of #498 turns on. A
//! `SyncGroup` answered `RebalanceInProgress` covers five distinct situations
//! calling for different things.
//!
//! Read back from a real meter rather than a test-only shadow, as
//! `group_eviction_signal` does: what is under test is that the instrument is
//! reached on the path that answers.

use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use common::{StorageType, alphanumeric_string, register_broker};
use opentelemetry::global;
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    data::{AggregatedMetrics, MetricData},
};
use tansu_broker::{Result, coordinator::group::administrator::Controller};
use tansu_sans_io::{
    ErrorCode, join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember,
};
use tansu_storage::{AssignmentDoc, GenerationDoc, MemberDoc, MemberRef};
use tokio::time::timeout;
use url::Url;
use uuid::Uuid;

mod common;

/// Short, because a follower now waits **half its session** for the leader's
/// assignment (#498) and this test drives that wait on a real clock — the
/// metric reader exports on a timer of its own, which `tokio`'s paused clock
/// never reaches. Kafka's own floor for a session timeout is 6s.
const SESSION_TIMEOUT_MS: i32 = 6_000;
const REBALANCE_TIMEOUT_MS: Option<i32> = Some(300_000);
const PROTOCOL_TYPE: &str = "consumer";
const RANGE: &str = "range";
const EXPORT_INTERVAL: Duration = Duration::from_millis(20);

fn collector() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporter::default();

    let provider = SdkMeterProvider::builder()
        .with_reader(
            PeriodicReader::builder(exporter.clone())
                .with_interval(EXPORT_INTERVAL)
                .build(),
        )
        .build();

    global::set_meter_provider(provider.clone());

    (provider, exporter)
}

/// The value of counter `name`, over the data points carrying `key=value` — or
/// every data point when no label is given.
fn counted(exporter: &InMemoryMetricExporter, name: &str, label: Option<(&str, &str)>) -> u64 {
    exporter
        .get_finished_metrics()
        .expect("metrics")
        .iter()
        .filter_map(|resource| {
            resource
                .scope_metrics()
                .flat_map(|scope| scope.metrics())
                .filter(|metric| metric.name() == name)
                .filter_map(|metric| match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(
                        sum.data_points()
                            .filter(|point| {
                                label.is_none_or(|(key, value)| {
                                    point.attributes().any(|attribute| {
                                        attribute.key.as_str() == key
                                            && attribute.value.as_str() == value
                                    })
                                })
                            })
                            .map(|point| point.value())
                            .sum::<u64>(),
                    ),
                    _ => None,
                })
                .reduce(|a, b| a + b)
        })
        .next_back()
        .unwrap_or_default()
}

/// Wait for `name{key=value}` to reach `expected`, or give up.
async fn settles_at(
    exporter: &InMemoryMetricExporter,
    name: &str,
    label: (&str, &str),
    expected: u64,
) -> bool {
    timeout(Duration::from_secs(10), async {
        loop {
            if counted(exporter, name, Some(label)) == expected {
                return;
            }

            tokio::time::sleep(EXPORT_INTERVAL).await;
        }
    })
    .await
    .is_ok()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("epoch")
        .as_millis() as i64
}

fn member(member_id: &str) -> MemberDoc {
    MemberDoc {
        last_contact_ms: now_ms(),
        session_timeout_ms: SESSION_TIMEOUT_MS,
        rebalance_timeout_ms: REBALANCE_TIMEOUT_MS,
        join_response: JoinGroupResponseMember::default()
            .member_id(member_id.to_owned())
            .metadata(Bytes::from_static(b"metadata")),
        ..Default::default()
    }
}

fn protocols() -> Vec<JoinGroupRequestProtocol> {
    vec![
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"metadata")),
    ]
}

/// A group of two, as it stands at `generation_id`, with an assignment only if
/// `assigned`. Written directly: a real clock is needed for the metric reader,
/// and forming the group through the API would spend the join window doing it.
async fn group_of_two<S>(
    storage: &S,
    group_id: &str,
    leader: &str,
    follower: &str,
    assigned: bool,
) -> Result<()>
where
    S: tansu_storage::Storage,
{
    for member_id in [leader, follower] {
        _ = storage
            .write_group_member(group_id, member_id, member(member_id), None)
            .await
            .expect("member");
    }

    _ = storage
        .update_group_generation(
            group_id,
            GenerationDoc {
                generation_id: 0,
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(RANGE.into()),
                leader: Some(leader.to_owned()),
                members: BTreeMap::from([
                    (leader.to_owned(), MemberRef::default()),
                    (follower.to_owned(), MemberRef::default()),
                ]),
                session_timeout_ms: SESSION_TIMEOUT_MS,
                rebalance_timeout_ms: REBALANCE_TIMEOUT_MS,
                // Just swept, so none of these requests runs one: what is under
                // test is the long poll, not the sweep.
                swept_at_ms: now_ms(),
                members_changed_at_ms: now_ms(),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("generation");

    if assigned {
        _ = storage
            .create_group_assignment(
                group_id,
                0,
                AssignmentDoc {
                    generation_id: 0,
                    leader: leader.to_owned(),
                    protocol_type: PROTOCOL_TYPE.into(),
                    protocol_name: RANGE.into(),
                    assignments: BTreeMap::from([
                        (leader.to_owned(), Bytes::from_static(b"leader")),
                        (follower.to_owned(), Bytes::from_static(b"follower")),
                    ]),
                    assigned_at_ms: now_ms(),
                },
            )
            .await
            .expect("assignment");
    }

    Ok(())
}

/// The barrier and the cap are different answers, and now they say so.
#[tokio::test(flavor = "multi_thread")]
async fn a_join_reports_why_it_was_answered() -> Result<()> {
    let (_provider, exporter) = collector();

    let cluster = Uuid::now_v7();
    let storage = common::storage_container(
        StorageType::InMemory,
        cluster.to_string(),
        111,
        Url::parse("tcp://127.0.0.1:9092/")?,
    )
    .await?;

    register_broker(cluster.to_string(), 111, storage.clone()).await?;

    let mut controller = Controller::with_storage(storage.clone())?;

    let group_id = alphanumeric_string(15);
    let leader = alphanumeric_string(20);
    let follower = alphanumeric_string(20);

    // Settled: an assignment for the current generation names both, so neither
    // has anything to wait for.
    group_of_two(&storage, &group_id, &leader, &follower, true).await?;

    let join = common::join_group(
        &mut controller,
        Some("console-consumer"),
        &group_id,
        SESSION_TIMEOUT_MS,
        REBALANCE_TIMEOUT_MS,
        &leader,
        None,
        PROTOCOL_TYPE,
        Some(&protocols()[..]),
        None,
    )
    .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(join.error_code)?);

    assert!(
        settles_at(
            &exporter,
            "tansu_group_join_answered",
            ("reason", "leader"),
            1
        )
        .await,
        "the leader is answered as the leader: {}",
        counted(&exporter, "tansu_group_join_answered", None),
    );

    let join = common::join_group(
        &mut controller,
        Some("console-consumer"),
        &group_id,
        SESSION_TIMEOUT_MS,
        REBALANCE_TIMEOUT_MS,
        &follower,
        None,
        PROTOCOL_TYPE,
        Some(&protocols()[..]),
        None,
    )
    .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(join.error_code)?);

    assert!(
        settles_at(
            &exporter,
            "tansu_group_join_answered",
            ("reason", "assigned"),
            1
        )
        .await,
        "a member the assignment names is answered as assigned: {}",
        counted(&exporter, "tansu_group_join_answered", None),
    );

    // Neither of these is the cap, which is the whole point of the counter: a
    // settled group must never answer a member that has nothing to act on.
    assert_eq!(
        0,
        counted(
            &exporter,
            "tansu_group_join_answered",
            Some(("reason", "cap"))
        ),
        "a settled group must answer from the barrier, never from the cap"
    );

    // And the KIP-394 reason stays at zero, because `join` answers a minted
    // member id by returning before any long poll.
    assert_eq!(
        0,
        counted(
            &exporter,
            "tansu_group_join_answered",
            Some(("reason", "member_id_required"))
        ),
    );

    Ok(())
}

/// `RebalanceInProgress` is five situations wearing one error code. The one
/// that matters is the follower that got there before the leader wrote the
/// assignment — the size of the bounce-and-rejoin cycle.
#[tokio::test(flavor = "multi_thread")]
async fn a_sync_reports_why_it_bounced() -> Result<()> {
    let (_provider, exporter) = collector();

    let cluster = Uuid::now_v7();
    let storage = common::storage_container(
        StorageType::InMemory,
        cluster.to_string(),
        111,
        Url::parse("tcp://127.0.0.1:9092/")?,
    )
    .await?;

    register_broker(cluster.to_string(), 111, storage.clone()).await?;

    let mut controller = Controller::with_storage(storage.clone())?;

    let group_id = alphanumeric_string(15);
    let leader = alphanumeric_string(20);
    let follower = alphanumeric_string(20);

    // No assignment yet: the leader has not synced.
    group_of_two(&storage, &group_id, &leader, &follower, false).await?;

    let sync = common::sync_group(
        &mut controller,
        &group_id,
        0,
        &follower,
        None,
        PROTOCOL_TYPE,
        RANGE,
        &[][..],
    )
    .await?;

    assert_eq!(
        ErrorCode::RebalanceInProgress,
        ErrorCode::try_from(sync.error_code)?
    );

    assert!(
        settles_at(
            &exporter,
            "tansu_group_sync_rebalance_in_progress",
            ("cause", "awaiting_leader"),
            1
        )
        .await,
        "a follower ahead of its leader must say so: {}",
        counted(&exporter, "tansu_group_sync_rebalance_in_progress", None),
    );

    // A member whose generation the group has left is a different situation
    // with the same error code, and it is now a different data point.
    let sync = common::sync_group(
        &mut controller,
        &group_id,
        -1,
        &follower,
        None,
        PROTOCOL_TYPE,
        RANGE,
        &[][..],
    )
    .await?;

    assert_eq!(
        ErrorCode::RebalanceInProgress,
        ErrorCode::try_from(sync.error_code)?
    );

    assert!(
        settles_at(
            &exporter,
            "tansu_group_sync_rebalance_in_progress",
            ("cause", "generation_moved"),
            1
        )
        .await,
        "a member behind the generation must say so: {}",
        counted(&exporter, "tansu_group_sync_rebalance_in_progress", None),
    );

    Ok(())
}
