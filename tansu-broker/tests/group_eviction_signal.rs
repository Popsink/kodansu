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

//! Evicting a member is a rebalance, and nothing said how often it happened
//! (#488).
//!
//! The sweep reports an eviction in an `info!` and the production fleet runs
//! `RUST_LOG=warn`, so the only signal that existed was switched off — which is
//! why "are the ~69 rejoins per member per day #486 measured actually
//! evictions?" could not be answered from the fleet at all.
//!
//! Read back from a real meter rather than a test-only shadow, for the same
//! reason `scaling_signal` does: what is under test is that the instrument is
//! reached on the path that evicts, and a shadow counter would reproduce the
//! arithmetic and not the wiring.

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
use tansu_sans_io::{ErrorCode, join_group_response::JoinGroupResponseMember};
use tansu_storage::{GenerationDoc, MemberDoc, MemberRef, Storage as _};
use tokio::time::timeout;
use url::Url;
use uuid::Uuid;

mod common;

const SESSION_TIMEOUT_MS: i32 = 45_000;
const REBALANCE_TIMEOUT_MS: Option<i32> = Some(300_000);
const PROTOCOL_TYPE: &str = "consumer";
const RANGE: &str = "range";

/// How often the reader exports. Short, because the test polls for a value to
/// appear rather than sleeping a guessed amount.
const EXPORT_INTERVAL: Duration = Duration::from_millis(20);

/// The meter every instrument in the process reports through, plus the sink the
/// exports land in. Installed before anything touches an instrument: each is a
/// `LazyLock` bound to whatever provider is global at first use, and `nextest`
/// runs each test in its own process.
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

/// The value of counter `name` as of the most recent export carrying it, summed
/// over its data points — the whole point of the `reason` label is that the
/// unlabelled total is not what anyone reads, so the per-reason value is taken
/// separately below.
fn counter(exporter: &InMemoryMetricExporter, name: &str) -> u64 {
    counted(exporter, name, None)
}

/// As [`counter`], but only the data points carrying `reason`.
fn counted(exporter: &InMemoryMetricExporter, name: &str, reason: Option<&str>) -> u64 {
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
                                reason.is_none_or(|reason| {
                                    point.attributes().any(|attribute| {
                                        attribute.key.as_str() == "reason"
                                            && attribute.value.as_str() == reason
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

/// The count of samples the histogram `name` has recorded, and their sum.
fn histogram(exporter: &InMemoryMetricExporter, name: &str) -> (u64, u64) {
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
                    AggregatedMetrics::U64(MetricData::Histogram(histogram)) => Some(
                        histogram
                            .data_points()
                            .map(|point| (point.count(), point.sum()))
                            .fold((0, 0), |(count, sum), (c, s)| (count + c, sum + s)),
                    ),
                    _ => None,
                })
                .reduce(|(ac, asum), (bc, bsum)| (ac + bc, asum + bsum))
        })
        .next_back()
        .unwrap_or_default()
}

/// Wait for `name` to reach `expected`, or give up. Polled rather than slept for
/// a guessed interval: what is under test is that the count moves at all.
async fn settles_at(exporter: &InMemoryMetricExporter, name: &str, expected: u64) -> bool {
    timeout(Duration::from_secs(10), async {
        loop {
            if counter(exporter, name) == expected {
                return;
            }

            tokio::time::sleep(EXPORT_INTERVAL).await;
        }
    })
    .await
    .is_ok()
}

/// A real clock, not `tokio`'s paused one: the metric reader exports on a timer
/// of its own, which a paused clock never reaches. The state that would take a
/// session timeout to reach is written directly instead — a member last heard
/// from two sessions ago, in a group whose sweep is overdue, which is exactly
/// what the sweep sees after a consumer goes away.
#[tokio::test(flavor = "multi_thread")]
async fn a_member_whose_session_lapses_is_counted_as_evicted() -> Result<()> {
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
    let member_id = alphanumeric_string(20);

    let now_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("epoch")
        .as_millis() as i64;

    // Two sessions ago, so the member is expired however the sweep rounds.
    let lapsed_ms = now_ms - i64::from(SESSION_TIMEOUT_MS) * 2;

    _ = storage
        .write_group_member(
            group_id.as_str(),
            member_id.as_str(),
            MemberDoc {
                last_contact_ms: lapsed_ms,
                session_timeout_ms: SESSION_TIMEOUT_MS,
                join_response: JoinGroupResponseMember::default()
                    .member_id(member_id.clone())
                    .metadata(Bytes::from_static(b"metadata")),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("member");

    _ = storage
        .update_group_generation(
            group_id.as_str(),
            GenerationDoc {
                generation_id: 0,
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(RANGE.into()),
                leader: Some(member_id.clone()),
                members: BTreeMap::from([(member_id.clone(), MemberRef::default())]),
                session_timeout_ms: SESSION_TIMEOUT_MS,
                rebalance_timeout_ms: REBALANCE_TIMEOUT_MS,
                // Overdue, so this request is the one that sweeps.
                swept_at_ms: lapsed_ms,
                ..Default::default()
            },
            None,
        )
        .await
        .expect("generation");

    assert_eq!(
        0,
        counter(&exporter, "tansu_group_members_evicted"),
        "nothing has swept yet"
    );

    let beat = common::heartbeat(
        &mut controller,
        group_id.as_str(),
        0,
        member_id.as_str(),
        None,
    )
    .await?;

    assert_eq!(
        ErrorCode::UnknownMemberId,
        ErrorCode::try_from(beat.error_code)?,
        "the sweep must have taken the membership before this heartbeat was answered"
    );

    assert!(
        settles_at(&exporter, "tansu_group_members_evicted", 1).await,
        "an eviction must be counted: {}",
        counter(&exporter, "tansu_group_members_evicted"),
    );

    assert_eq!(
        1,
        counted(&exporter, "tansu_group_members_evicted", Some("lapsed")),
        "a member with a document is evicted for its stamp, not for the document's absence"
    );

    // And the age it was evicted at is reported, which is the number #488 needs:
    // liveness is persisted once per session/2, so an eviction at an age of
    // barely one session is one where the member may have been quiet for half
    // that. Here it is the two sessions the stamp was backdated by.
    let (samples, total_ms) = histogram(&exporter, "tansu_group_member_eviction_age_milliseconds");

    assert_eq!(1, samples);
    assert!(
        total_ms >= SESSION_TIMEOUT_MS as u64,
        "an eviction at {total_ms}ms cannot be younger than the session it lapsed"
    );

    Ok(())
}
