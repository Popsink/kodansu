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

use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use crate::{Result, TracingFormat};

#[derive(Debug)]
pub(super) struct Guard {
    tracer: Option<SdkTracerProvider>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        let shutdown = self.tracer.as_ref().map(SdkTracerProvider::shutdown);
        if let Some(Err(failure)) = shutdown {
            eprintln!("{failure:?}");
        }
    }
}

pub(super) fn init_tracing_subscriber(tracing_format: TracingFormat) -> Result<Guard> {
    let filtered = tracing_subscriber::registry().with(EnvFilter::from_default_env());
    match tracing_format {
        TracingFormat::Text => {
            let lines = tracing_subscriber::fmt::layer().with_level(true);
            let lines = lines.with_line_number(true).with_thread_ids(false);
            filtered.with(lines.with_span_events(FmtSpan::FULL)).init();
        }
        TracingFormat::Json => filtered
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
    }

    Ok(Guard { tracer: None })
}
