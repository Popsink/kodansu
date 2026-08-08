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

//! A fetch waiting out `max.wait.ms` is *parked*, not working (#362).
//!
//! Autoscaling needs something that expresses load, and the two figures a
//! scaler would otherwise reach for both lie about a Kafka broker. Connection
//! count never falls while a client stays attached, so an idle consumer group
//! looks like a busy one. Requests in flight is inflated by long polls that are
//! waiting rather than working, so a fleet serving nothing but empty fetches
//! looks saturated. `tansu_requests_parked` is the correction: subtract it from
//! `tansu_requests_in_flight` and what is left is work.
//!
//! Read back from a real meter rather than from a test-only shadow, because the
//! way an up-down counter fails is by **leaking** — an increment whose
//! decrement was skipped by an early return does not decay, so the replica
//! reports load it does not have, forever, and a scaler built on it never
//! scales down again. A shadow counter would reproduce the arithmetic and not
//! the lifetime.

use std::{sync::Arc, time::Duration};

use opentelemetry::global;
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    data::{AggregatedMetrics, MetricData},
};
use rama::{Context, Layer as _, Service as _, layer::MapStateLayer};
use tansu_sans_io::{
    FetchRequest, IsolationLevel,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
};
use tansu_storage::{Error, FetchService, Storage, StorageContainer};
use tokio::time::{sleep, timeout};
use url::Url;

/// How often the reader exports. Short, because the test polls for a value to
/// appear rather than sleeping a guessed amount, and this is the resolution of
/// that poll.
const EXPORT_INTERVAL: Duration = Duration::from_millis(20);

/// The meter every instrument in the process reports through, plus the sink the
/// exports land in.
///
/// Installed before anything touches an instrument: each of them is a
/// `LazyLock` bound to whatever provider is global at first use, and
/// `cargo-nextest` runs each test in its own process, so "before" is the first
/// line of the test rather than a lock somebody has to remember.
///
/// Read by *waiting for an export* rather than by calling `force_flush`, which
/// blocks on a channel with no timeout — from inside a `tokio` worker that is a
/// way to hang a test rather than to read a counter.
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

/// The value of `name` as of the most recent export that carried it, summed
/// over every instrumentation scope reporting it — the fetch poll and the group
/// polls live in different crates, so the metric legitimately arrives more than
/// once.
///
/// The *last* batch, not every batch: exports are cumulative, so summing the
/// history would report how much had ever been parked rather than how much is.
///
/// Absent reads as zero, and has to: an instrument is created on its first
/// recording, so a replica that has never parked a request has no series at
/// all rather than a series at zero. A scaler's `or vector(0)` is the same
/// accommodation.
fn parked(exporter: &InMemoryMetricExporter, name: &str) -> i64 {
    gauge(exporter, name).unwrap_or_default()
}

fn gauge(exporter: &InMemoryMetricExporter, name: &str) -> Option<i64> {
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
                    AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                        Some(sum.data_points().map(|point| point.value()).sum::<i64>())
                    }
                    _ => None,
                })
                .reduce(|a, b| a + b)
        })
        .next_back()
}

/// Wait for `name` to reach `expected`, or give up.
///
/// Polled rather than slept for a guessed interval: what is under test is that
/// the count moves at all, and a fixed sleep would make that a statement about
/// this machine.
async fn settles_at(exporter: &InMemoryMetricExporter, name: &str, expected: i64) -> bool {
    timeout(Duration::from_secs(10), async {
        loop {
            if parked(exporter, name) == expected {
                return;
            }

            sleep(EXPORT_INTERVAL).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fetch_waiting_for_records_is_parked_rather_than_working() -> Result<(), Error> {
    let (_provider, exporter) = collector();

    const TOPIC: &str = "parked";
    const MAX_WAIT: Duration = Duration::from_secs(5);

    let storage: Arc<Box<dyn Storage>> = StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .storage(Url::parse("memory://")?)
        .build()
        .await?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    assert!(
        settles_at(&exporter, "tansu_requests_parked", 0).await,
        "nothing is parked before a request is made: {}",
        parked(&exporter, "tansu_requests_parked"),
    );

    // A fetch on a partition with no records: there is nothing to return, so it
    // waits out `max.wait.ms`. This is the shape a fleet of idle consumers is
    // made of, and the shape that makes requests-in-flight useless.
    let fetching = {
        let fetch = {
            let storage = storage.clone();
            MapStateLayer::new(move |_| storage.clone()).into_layer(FetchService)
        };

        tokio::spawn(async move {
            fetch
                .serve(
                    Context::default(),
                    FetchRequest::default()
                        .max_wait_ms(MAX_WAIT.as_millis() as i32)
                        .min_bytes(1)
                        .max_bytes(Some(1024 * 1024))
                        .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                        .topics(Some(vec![
                            FetchTopic::default()
                                .topic(Some(TOPIC.into()))
                                .partitions(Some(vec![
                                    FetchPartition::default()
                                        .partition(0)
                                        .fetch_offset(0)
                                        .partition_max_bytes(1024 * 1024),
                                ])),
                        ])),
                )
                .await
        })
    };

    assert!(
        settles_at(&exporter, "tansu_requests_parked", 1).await,
        "a fetch waiting out max.wait.ms must report itself parked, or an idle fleet \
         is indistinguishable from a busy one",
    );

    // And it comes back down. This is the half that fails by leaking: an
    // up-down counter left high never recovers, so a scaler reading it never
    // scales down again.
    _ = timeout(MAX_WAIT * 2, fetching)
        .await
        .expect("the fetch must return once max.wait.ms is out")
        .expect("the fetch task must not panic")?;

    assert!(
        settles_at(&exporter, "tansu_requests_parked", 0).await,
        "the count must return to zero once the poll is over: {}",
        parked(&exporter, "tansu_requests_parked"),
    );

    Ok(())
}
