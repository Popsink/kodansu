// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use opentelemetry::KeyValue;
use opentelemetry_otlp::Protocol;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::Resource;
use url::Url;

use crate::{Result, TracingFormat};

mod allocator;
mod tracing;

#[derive(Debug)]
pub struct Guard {
    #[allow(dead_code)]
    tracer: tracing::Guard,
}

pub fn init(tracing_format: TracingFormat) -> Result<Guard> {
    let tracer = tracing::init_tracing_subscriber(tracing_format)?;
    Ok(Guard { tracer })
}

pub fn metric_exporter(endpoint: Url) -> Result<()> {
    let metrics = endpoint.join("v1/metrics")?;
    ::tracing::debug!(endpoint = %metrics);

    let http = opentelemetry_otlp::MetricExporter::builder().with_http();
    let http = http.with_protocol(Protocol::HttpBinary);
    let exporter = http.with_endpoint(metrics.to_string()).build()?;

    let service = KeyValue::new("service.name", env!("CARGO_PKG_NAME"));
    let identity = Resource::builder_empty().with_attributes([service]).build();
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder();
    let provider = provider
        .with_periodic_exporter(exporter)
        .with_resource(identity);
    opentelemetry::global::set_meter_provider(provider.build());

    // After the provider, never before: an instrument built against the no-op
    // provider is silently never collected.
    allocator::register();

    Ok(())
}
