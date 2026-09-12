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

//! `tansu_cache_entries` has to be readable from a process that only ever
//! *serves* (#573).
//!
//! #554 added the gauge on the maintenance tick, which is the one deployment
//! where the maps it measures are empty: `tansu-maintain` serves no clients, so
//! it populates neither the routing pin nor the topic-id pointer and reported
//! both as `0`, while the ten `tansu-external` replicas that do hold them ran
//! `?maintenance_interval=never` and reported nothing at all. The stale routing
//! pin #554 fixed is a serving replica's failure — a replica that takes a
//! produce holding a deleted topic's pin and writes the successor's records
//! under the dead incarnation's sub-stream identity (#442) — so the instrument
//! was absent from every process that could exhibit the bug, and present on the
//! only one that could not.
//!
//! So these tests never call [`Storage::maintain`]. Everything below is what a
//! client asks for, and the assertion is that the series exists anyway.
//!
//! Read back through a **delta** reader, for the reason `retention_signal`
//! gives: a cumulative last-value aggregation re-exports a point recorded once,
//! so a gauge that has gone silent still reads as present and the regression is
//! invisible.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::Bytes;
use opentelemetry::global;
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, InMemoryMetricExporterBuilder, PeriodicReader, SdkMeterProvider,
    Temporality,
    data::{AggregatedMetrics, MetricData},
};
use tansu_sans_io::{
    ConfigResource, IsolationLevel, ListOffset,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use tansu_storage::{Error, Storage, StorageContainer, TopicId, Topition};
use tokio::time::{sleep, timeout};
use url::Url;

/// How often the reader exports. Short, because the tests poll for a value to
/// appear rather than sleeping a guessed amount.
const EXPORT_INTERVAL: Duration = Duration::from_millis(20);

const GAUGE: &str = "tansu_cache_entries";

const PARTITIONS: i32 = 3;

/// Every map named by the three cache groups' inventories — the whole series the
/// gauge is supposed to carry.
///
/// Spelled out here rather than derived from the engine, so that a map added to
/// a group and left out of its `occupancy` list fails this test as a *missing
/// series* rather than being silently dropped by an assertion that reads the
/// same list twice. It is the same reasoning `topic_churn` gives for reading the
/// group's own inventory: this file sits outside the crate and cannot, which
/// makes it the independent copy.
const CACHES: [&str; 19] = [
    "topic_metas",
    "topic_ids",
    "routing_prefixes",
    "compacted_topics",
    "watermarks",
    "next_offsets",
    "coalesced_watermark_floors",
    "truncate_floors",
    "prefix_index",
    "segment_seqs",
    "oldest_retained_prefix",
    "retired_prefixes",
    "quarantined_segments",
    "compact_seams",
    "era_epochs",
    "served_end_reconciled",
    "segment_reads",
    "producers",
    "group_offsets",
];

/// The meter every instrument in the process reports through, plus the sink the
/// exports land in.
///
/// Installed on the first line of the test: each instrument is a `LazyLock`
/// bound to whatever provider is global at first use, and `cargo-nextest` runs
/// each test in its own process.
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

/// The last value each `cache` has been recorded at since the last reset.
///
/// Every export rather than the most recent one: under delta an export holds
/// only what was recorded in its own window, so the newest export is empty
/// whenever the recording landed a window or two ago.
fn occupancy(exporter: &InMemoryMetricExporter) -> BTreeMap<String, u64> {
    exporter
        .get_finished_metrics()
        .expect("metrics")
        .iter()
        .flat_map(|resource| {
            resource
                .scope_metrics()
                .flat_map(|scope| scope.metrics())
                .filter(|metric| metric.name() == GAUGE)
                .filter_map(|metric| match metric.data() {
                    AggregatedMetrics::U64(MetricData::Gauge(gauge)) => Some(
                        gauge
                            .data_points()
                            .filter_map(|point| {
                                point
                                    .attributes()
                                    .find(|attribute| attribute.key.as_str() == "cache")
                                    .map(|attribute| {
                                        (attribute.value.as_str().into_owned(), point.value())
                                    })
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

/// Wait for `ready` to hold of the exported metrics, or give up.
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

/// Drive a topic through the paths a client reaches, and *only* those: a by-name
/// and a by-id Metadata lookup, a `DescribeConfigs`, a produce and an offset
/// read.
///
/// The by-id lookup is what populates `topic_ids` and the produce is what
/// populates `routing_prefixes` — the two maps the fleet reported as `0`, and
/// the two whose entries are held without a TTL because they are immutable for
/// a topic's lifetime.
async fn exercise(storage: &Arc<dyn Storage>, name: &str) -> Result<(), Error> {
    let id = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(PARTITIONS)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    _ = storage.metadata(Some(&[TopicId::from(name)])).await?;
    _ = storage.metadata(Some(&[TopicId::Id(id)])).await?;

    _ = storage
        .describe_config(name, ConfigResource::Topic, None)
        .await?;

    for partition in 0..PARTITIONS {
        let topition = Topition::new(name, partition);

        let batch = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(Some(Bytes::from_static(b"k")))
                    .value(Some(Bytes::from_static(b"v"))),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;

        _ = storage.produce(None, &topition, batch).await?;

        _ = storage
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[
                    (topition.clone(), ListOffset::Latest),
                    (topition.clone(), ListOffset::Earliest),
                ],
            )
            .await?;
    }

    Ok(())
}

/// A process that never maintains still reports every cache (#573).
#[tokio::test(flavor = "multi_thread")]
async fn serving_alone_reports_the_whole_inventory() -> Result<(), Error> {
    let (_provider, exporter) = collector();

    let storage = storage().await?;

    exercise(&storage, "served-not-maintained").await?;

    assert!(
        settles(|| !occupancy(&exporter).is_empty()).await,
        "a replica that only serves has to report its cache occupancy: \
         `tansu-external` runs ?maintenance_interval=never, and before #573 the \
         maintenance tick was the only recording site",
    );

    let recorded = occupancy(&exporter);
    let missing = CACHES
        .into_iter()
        .filter(|cache| !recorded.contains_key(*cache))
        .collect::<Vec<_>>();

    assert_eq!(
        Vec::<&str>::new(),
        missing,
        "every map in every group is a series, empty or not — an absent one is \
         indistinguishable from a map nothing populates: {recorded:?}",
    );

    Ok(())
}

/// And it reports the two maps the maintainer can only ever report as `0`
/// (#573).
///
/// The level, not merely the series: `tansu-maintain` did carry
/// `routing_prefixes` and `topic_ids`, both at zero, because it serves no
/// clients. A fix that made the serving fleet report the same zeroes would look
/// identical on a dashboard and answer nothing.
#[tokio::test(flavor = "multi_thread")]
async fn the_client_populated_maps_report_what_serving_put_in_them() -> Result<(), Error> {
    let (_provider, exporter) = collector();

    let storage = storage().await?;

    exercise(&storage, "pinned-and-identified").await?;

    assert!(
        settles(|| occupancy(&exporter)
            .get("routing_prefixes")
            .is_some_and(|entries| *entries > 0))
        .await,
        "a produce pins a routing prefix, and the pin is the authority the stale \
         entry #554 fixed came from: {:?}",
        occupancy(&exporter),
    );

    let recorded = occupancy(&exporter);

    assert_eq!(
        Some(&1),
        recorded.get("topic_ids"),
        "a by-id Metadata lookup memoizes the topic-id pointer permanently: \
         {recorded:?}",
    );

    assert_eq!(
        Some(&u64::try_from(PARTITIONS).expect("partitions")),
        recorded.get("watermarks"),
        "and the partition-keyed maps are counted at partition scale, which is \
         the larger denominator the memory work (#476, #543) is looking for: \
         {recorded:?}",
    );

    Ok(())
}
