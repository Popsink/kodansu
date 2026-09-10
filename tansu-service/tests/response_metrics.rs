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

//! What the four `tansu-service` histograms carry, and how far up they reach.
//!
//! Two defects, both invisible from inside the broker and both found from
//! production data rather than from here (#539):
//!
//! - Every histogram took the OTel SDK's default bucket ladder, whose top
//!   finite bucket is 10 000 — of anything, whatever the unit. 9% of 91.6 M
//!   responses exceeded 10 kB and shared one `+Inf` bucket, so #537 moving a
//!   response from 14.7 MB to 5.36 MB did not move a single number the broker
//!   publishes.
//! - `tansu_response_size` carried `cluster_id` and nothing else. #410 added
//!   `api_key` in `process`, but `write` is called from `answer` with the
//!   connection-level attributes, so the response-size series had no per-API
//!   breakdown at all.
//!
//! Both are asserted from the *exported* data point rather than from the
//! builder, because both failure modes look identical to correct code at the
//! call site: a `with_boundaries` on an instrument built before the provider
//! was installed is silently ignored, and a missing attribute is a label that
//! is simply absent.

use std::time::Duration;

use opentelemetry::global;
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, InMemoryMetricExporterBuilder, PeriodicReader, SdkMeterProvider,
    Temporality,
    data::{AggregatedMetrics, MetricData},
};
use rama::{Context, Layer as _, Service as _};
use tansu_sans_io::{ApiKey as _, Frame, Header, MetadataRequest, MetadataResponse};
use tansu_service::{
    BytesFrameLayer, BytesTcpService, FrameBytesLayer, FrameService, TcpBytesLayer,
    TcpContextLayer, TcpListenerLayer,
};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::common::{Error, init_tracing};

mod common;

const EXPORT_INTERVAL: Duration = Duration::from_millis(20);

const RESPONSE_SIZE: &str = "tansu_response_size";
const RESPONSE_WRITE_DURATION: &str = "tansu_response_write_duration";
const REQUEST_SIZE: &str = "tansu_request_size";
const REQUEST_DURATION: &str = "tansu_request_duration";

/// The top finite bucket each ladder has to reach, from `tansu-service`'s
/// `SIZE_BYTES_BOUNDARIES` and `DURATION_MS_BOUNDARIES`.
const TOP_SIZE_BUCKET: f64 = 67_108_864.0;
const TOP_DURATION_BUCKET: f64 = 60_000.0;

const QUEUE_DEPTH: &str = "tansu_runtime_global_queue_depth";
const WORKER_BUSY: &str = "tansu_runtime_worker_busy_ms";

/// Installed on the first line of the test, before anything touches an
/// instrument: each is a `LazyLock` bound to whatever provider is global at
/// first use, and `cargo-nextest` runs each test in its own process, so
/// "before" is a line here rather than a lock somebody has to remember.
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

/// Every exported data point of the histogram `name`, as
/// `(api_key, top finite bucket)`.
///
/// `None` for the key means the label is absent, which is the state #539 found
/// `tansu_response_size` in — distinct from a label that is present and wrong.
fn histogram_points(exporter: &InMemoryMetricExporter, name: &str) -> Vec<(Option<i64>, f64)> {
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
                    AggregatedMetrics::U64(MetricData::Histogram(histogram)) => Some(
                        histogram
                            .data_points()
                            .map(|point| {
                                (
                                    point
                                        .attributes()
                                        .find(|attribute| attribute.key.as_str() == "api_key")
                                        .and_then(|attribute| match attribute.value {
                                            opentelemetry::Value::I64(value) => Some(value),
                                            _ => None,
                                        }),
                                    point.bounds().last().expect("a finite bucket"),
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

/// Whether the observable gauge `name` reported at all.
fn gauge_reported(exporter: &InMemoryMetricExporter, name: &str) -> bool {
    exporter
        .get_finished_metrics()
        .expect("metrics")
        .iter()
        .any(|resource| {
            resource
                .scope_metrics()
                .flat_map(|scope| scope.metrics())
                .filter(|metric| metric.name() == name)
                .any(|metric| match metric.data() {
                    AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                        gauge.data_points().count() > 0
                    }
                    _ => false,
                })
        })
}

async fn server(cancellation: CancellationToken, listener: TcpListener) -> Result<(), Error> {
    let server = (
        TcpListenerLayer::new(cancellation),
        TcpContextLayer::default(),
        TcpBytesLayer::<()>::default(),
        BytesFrameLayer::default(),
    )
        .into_layer(FrameService::new(|_, req: Frame| {
            req.correlation_id()
                .map(|correlation_id| Frame {
                    size: 0,
                    header: Header::Response { correlation_id },
                    body: MetadataResponse::default()
                        .brokers(Some([].into()))
                        .topics(Some([].into()))
                        .cluster_id(Some("abc".into()))
                        .controller_id(Some(111))
                        .throttle_time_ms(Some(0))
                        .cluster_authorized_operations(Some(-1))
                        .into(),
                })
                .map_err(Error::from)
        }));

    server.serve(Context::default(), listener).await
}

#[tokio::test]
async fn every_histogram_is_per_api_and_reaches_its_ladder() -> Result<(), Error> {
    let (provider, exporter) = collector();
    let _guard = init_tracing()?;

    let cancellation = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let local_addr = listener.local_addr()?;

    let mut join = JoinSet::new();

    let _server = {
        let cancellation = cancellation.clone();
        join.spawn(async move { server(cancellation, listener).await })
    };

    let stream = TcpStream::connect(local_addr).await?;
    let client = FrameBytesLayer.into_layer(BytesTcpService);

    let frame = client
        .serve(
            Context::with_state(stream),
            Frame {
                header: Header::Request {
                    api_key: MetadataRequest::KEY,
                    api_version: 12,
                    correlation_id: 0,
                    client_id: Some(env!("CARGO_PKG_NAME").into()),
                },
                body: MetadataRequest::default()
                    .topics(Some([].into()))
                    .allow_auto_topic_creation(Some(false))
                    .include_cluster_authorized_operations(Some(false))
                    .include_topic_authorized_operations(Some(false))
                    .into(),
                size: 0,
            },
        )
        .await?;

    // The response has to have been written for `write` to have recorded
    // anything: this assertion is what makes the ones below about a request
    // that completed rather than one that never got an answer.
    let response = MetadataResponse::try_from(frame.body)?;
    assert_eq!(Some("abc"), response.cluster_id.as_deref());

    cancellation.cancel();
    _ = join.join_all().await;

    provider.force_flush().expect("flush");

    let expected = Some(i64::from(MetadataRequest::KEY));

    // The two `write` records. `tansu_response_size` is the one #410 missed and
    // `tansu_response_write_duration` did not exist; both are called from
    // `answer`, which is why the fix was to derive `api_key` there.
    for name in [RESPONSE_SIZE, RESPONSE_WRITE_DURATION] {
        let points = histogram_points(&exporter, name);
        assert!(!points.is_empty(), "{name} recorded nothing");

        for (api_key, top) in points {
            assert_eq!(expected, api_key, "{name} is not labelled per API");
            assert_eq!(
                if name == RESPONSE_SIZE {
                    TOP_SIZE_BUCKET
                } else {
                    TOP_DURATION_BUCKET
                },
                top,
                "{name} is on the SDK's default ladder"
            );
        }
    }

    // And the two `process` records, which #410 did label — asserted here
    // because #539 moved where the label is derived, and a refactor that drops
    // it again would otherwise be silent.
    for (name, top) in [
        (REQUEST_SIZE, TOP_SIZE_BUCKET),
        (REQUEST_DURATION, TOP_DURATION_BUCKET),
    ] {
        let points = histogram_points(&exporter, name);
        assert!(!points.is_empty(), "{name} recorded nothing");

        for (api_key, bucket) in points {
            assert_eq!(expected, api_key, "{name} is not labelled per API");
            assert_eq!(top, bucket, "{name} is on the SDK's default ladder");
        }
    }

    // The two runtime gauges the accept loop registers. Without them a slow
    // `tansu_response_write_duration` cannot say whether the peer stopped
    // reading or the runtime was saturated, and only the second is a broker
    // defect (#539).
    //
    // Registered from inside the runtime and observed from the exporter's
    // thread: a callback calling `Handle::current()` would panic there, so
    // "the gauge reported a value" is the assertion that catches it.
    for name in [QUEUE_DEPTH, WORKER_BUSY] {
        assert!(gauge_reported(&exporter, name), "{name} reported nothing");
    }

    Ok(())
}
