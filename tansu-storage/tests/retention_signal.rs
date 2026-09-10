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

//! Retention has to be *shown* to be bounded, not merely be bounded (#544).
//!
//! `tansu_prefix_oldest_retained_timestamp_ms` (#509) exists to say whether a
//! prefix's retention window is still filling or has plateaued, and it was
//! recorded below the #49 fast path — which returns early precisely when the
//! oldest survivor is newer than the threshold, i.e. while the window is still
//! filling. The gauge was therefore silent for exactly the prefixes it was built
//! to describe: 99 of 266 non-empty prefixes reported on the production fleet at
//! `1.0.0-alpha.18`.
//!
//! Read back from a real meter, and through a **delta** reader. Cumulative is
//! what the broker exports and it is the wrong instrument for this test: a
//! last-value aggregation keeps re-exporting a data point recorded once, so a
//! gauge that has gone silent still reads as present and the regression is
//! invisible. Delta drains on collect, so "was this recorded during *this*
//! maintenance tick?" is a question the exporter can answer.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use opentelemetry::global;
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, InMemoryMetricExporterBuilder, PeriodicReader, SdkMeterProvider,
    Temporality,
    data::{AggregatedMetrics, MetricData},
};
use tansu_sans_io::{
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
};
use tansu_storage::{Error, Storage, StorageContainer, Topition};
use tokio::time::{sleep, timeout};
use url::Url;

/// How often the reader exports. Short, because the tests poll for a value to
/// appear rather than sleeping a guessed amount, and this is the resolution of
/// that poll.
const EXPORT_INTERVAL: Duration = Duration::from_millis(20);

/// Longer than `MAINTENANCE_RECENCY` (9 m), so the second tick claims the prefix
/// again instead of skipping it as one a peer just did (#126). Nothing in these
/// tests sleeps for it — `maintain` takes the tick's clock as an argument.
const TICK_GAP: Duration = Duration::from_secs(10 * 60);

/// Comfortably longer than `TICK_GAP`, so the second tick's threshold is still
/// behind the record just written and the fast path is the one under test.
const RETENTION: Duration = Duration::from_secs(30 * 60);

const GAUGE: &str = "tansu_prefix_oldest_retained_timestamp_ms";
const SKIPPED: &str = "tansu_prefix_expiry_skipped";

/// The meter every instrument in the process reports through, plus the sink the
/// exports land in.
///
/// Installed before anything touches an instrument: each of them is a
/// `LazyLock` bound to whatever provider is global at first use, and
/// `cargo-nextest` runs each test in its own process, so "before" is the first
/// line of the test rather than a lock somebody has to remember.
fn collector() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporterBuilder::new()
        .with_temporality(Temporality::Delta)
        .build();

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

/// Every data point the gauge `name` has carried since the last
/// [`InMemoryMetricExporter::reset`], as `(prefix, value)`.
///
/// All exports rather than the most recent one: under delta each export holds
/// only what was recorded in its own window, so the last export is empty
/// whenever the recording landed a window or two ago.
fn gauge_points(exporter: &InMemoryMetricExporter, name: &str) -> Vec<(String, u64)> {
    exporter
        .get_finished_metrics()
        .expect("metrics")
        .iter()
        .flat_map(|resource| {
            resource
                .scope_metrics()
                .flat_map(|scope| scope.metrics())
                .filter(|metric| metric.name() == name)
                .filter_map(|metric| match metric.data() {
                    AggregatedMetrics::U64(MetricData::Gauge(gauge)) => Some(
                        gauge
                            .data_points()
                            .map(|point| {
                                (
                                    point
                                        .attributes()
                                        .find(|attribute| attribute.key.as_str() == "prefix")
                                        .map(|attribute| attribute.value.as_str().into_owned())
                                        .unwrap_or_default(),
                                    point.value(),
                                )
                            })
                            .collect::<Vec<_>>(),
                    ),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .flatten()
        .collect()
}

/// How many prefix-ticks the counter `name` has attributed to `reason` since the
/// last reset, summed over every export for the same reason as [`gauge_points`].
fn skipped(exporter: &InMemoryMetricExporter, name: &str, reason: &str) -> u64 {
    exporter
        .get_finished_metrics()
        .expect("metrics")
        .iter()
        .flat_map(|resource| {
            resource
                .scope_metrics()
                .flat_map(|scope| scope.metrics())
                .filter(|metric| metric.name() == name)
                .filter_map(|metric| match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(
                        sum.data_points()
                            .filter(|point| {
                                point.attributes().any(|attribute| {
                                    attribute.key.as_str() == "reason"
                                        && attribute.value.as_str() == reason
                                })
                            })
                            .map(|point| point.value())
                            .sum::<u64>(),
                    ),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .sum()
}

/// Wait for `ready` to hold of the exported metrics, or give up.
///
/// Polled rather than slept for a guessed interval: what is under test is that
/// the recording happens at all, and a fixed sleep would make that a statement
/// about this machine.
async fn settles(ready: impl Fn() -> bool) -> bool {
    timeout(Duration::from_secs(10), async {
        loop {
            if ready() {
                return;
            }

            sleep(EXPORT_INTERVAL).await;
        }
    })
    .await
    .is_ok()
}

async fn storage() -> Result<Arc<dyn Storage>, Error> {
    StorageContainer::builder()
        .cluster_id("tansu")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .storage(Url::parse("memory://")?)
        .build()
        .await
}

async fn create_topic(
    storage: &Arc<dyn Storage>,
    name: &str,
    configs: Vec<CreatableTopicConfig>,
) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some(configs)),
            false,
        )
        .await?;

    Ok(())
}

async fn produce(storage: &Arc<dyn Storage>, topition: &Topition) -> Result<(), Error> {
    let batch = inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(b"k")))
                .value(Some(Bytes::from_static(b"v"))),
        )
        .build()
        .and_then(deflated::Batch::try_from)?;

    _ = storage.produce(None, topition, batch).await?;

    Ok(())
}

fn config(name: &str, value: &str) -> CreatableTopicConfig {
    CreatableTopicConfig::default()
        .name(name.into())
        .value(Some(value.into()))
}

/// A tick that skips the scan because nothing can be expirable yet must still
/// report the plateau gauge (#544).
///
/// The first tick scans — the hint is empty, so the fast path cannot fire — and
/// that is the recording the gauge has always had. The second tick is the one
/// that matters: it takes the `not_due` fast path, and before #544 recorded
/// nothing at all, leaving a prefix whose retention window is *provably still
/// filling* with no series to say so.
#[tokio::test(flavor = "multi_thread")]
async fn the_plateau_gauge_reports_on_the_not_due_fast_path() -> Result<(), Error> {
    let (_provider, exporter) = collector();

    const TOPIC: &str = "filling";

    let storage = storage().await?;

    create_topic(
        &storage,
        TOPIC,
        vec![
            config("cleanup.policy", "delete"),
            config("retention.ms", &RETENTION.as_millis().to_string()),
        ],
    )
    .await?;

    produce(&storage, &Topition::new(TOPIC, 0)).await?;

    let first = SystemTime::now();
    storage.maintain(first).await?;

    assert!(
        settles(|| !gauge_points(&exporter, GAUGE).is_empty()).await,
        "the scanning tick reports the gauge, and always has: {:?}",
        gauge_points(&exporter, GAUGE),
    );

    let scanned = gauge_points(&exporter, GAUGE);
    exporter.reset();

    // Far enough ahead to be claimed again, nowhere near far enough for the
    // record to be expirable: the hint the first tick left is newer than this
    // tick's threshold, which is exactly the condition the fast path skips on.
    let second = first
        .checked_add(TICK_GAP)
        .expect("ten minutes past the first tick");
    storage.maintain(second).await?;

    assert!(
        settles(|| skipped(&exporter, SKIPPED, "not_due") > 0).await,
        "the second tick must take the fast path, or this test is not exercising it",
    );

    assert!(
        settles(|| !gauge_points(&exporter, GAUGE).is_empty()).await,
        "a prefix skipped as not_due is one whose window is still filling — the \
         case the gauge exists for — so the fast path has to record it",
    );

    assert_eq!(
        scanned,
        gauge_points(&exporter, GAUGE),
        "and it is the same reading, from the hint the scan stored rather than a \
         second listing",
    );

    Ok(())
}

/// A prefix retention was never asked about is counted, not silent (#544).
///
/// A compact-only topic gets no retention threshold by design (#175: the latest
/// value of a key must survive indefinitely), so no expiry path runs for its
/// prefix and none of the retention series mention it. That is correct, and it
/// used to be indistinguishable from retention having silently stopped visiting
/// the prefix — both are simply nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_prefix_outside_retention_says_so_rather_than_going_quiet() -> Result<(), Error> {
    let (_provider, exporter) = collector();

    const TOPIC: &str = "exempt";

    let storage = storage().await?;

    create_topic(&storage, TOPIC, vec![config("cleanup.policy", "compact")]).await?;

    produce(&storage, &Topition::new(TOPIC, 0)).await?;

    storage.maintain(SystemTime::now()).await?;

    assert!(
        settles(|| skipped(&exporter, SKIPPED, "no_threshold") > 0).await,
        "a maintained prefix with no retention threshold has to name itself, or \
         the denominator of what retention covers is unreadable",
    );

    assert!(
        gauge_points(&exporter, GAUGE).is_empty(),
        "and it reports no plateau: there is no threshold for it to plateau \
         against: {:?}",
        gauge_points(&exporter, GAUGE),
    );

    Ok(())
}
