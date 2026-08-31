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

//! What the allocator is holding, by class.
//!
//! The one measurement the broker could not make. In production on
//! `1.0.0-alpha.10` a `tansu-external` replica's working set is 1.0-3.0 GiB,
//! while everything that can be sized accounts for ~0.4 GiB of it: the prefix
//! index measured 232 MiB at fleet scale (130k segments / 1.3M sub-stream
//! entries), a `Fetch`'s response is capped at 5 MiB shared across its
//! partitions, the coalesce buffers hold about a second of a 0.5 MiB/s ingest.
//! And `tansu-maintain` — the same binary against the same bucket, doing *more*
//! index work but serving no Kafka client — sits at 90-290 MiB throughout.
//!
//! So the memory is on the request-serving path, and the next question is a
//! fork: is it live heap the program is still holding, or pages the allocator
//! took and has not given back? Every subsequent decision differs by that
//! answer — the first is a data-structure or a lifetime problem in the serving
//! path, the second is an allocator-tuning one — and no counter in the process
//! could tell them apart. `stats.allocated` (what the program asked for and has
//! not freed) against `stats.resident` (what the OS has actually given us)
//! answers it in one read.
//!
//! # Why the allocator changed
//!
//! `libmimalloc-sys` exports the allocation functions and nothing else: no
//! `mi_process_info`, no `mi_stats_*`, no options API. Reading mimalloc's own
//! counters therefore needs an `extern "C"` block of our own, and `unsafe_code`
//! is `forbid`den workspace-wide — a `forbid` cannot be relaxed by an inner
//! `allow`, so that is a workspace-lint change, not a local one.
//! `tikv-jemalloc-ctl` reports the same class of numbers through a safe API,
//! which is the whole reason `tansu` now allocates through jemalloc.
//!
//! # Reading the classes
//!
//! They nest, outermost last, and the gaps between them are the diagnosis:
//!
//! - `allocated` — live bytes the program asked for. This is the number a leak
//!   grows and a fragmentation problem does not.
//! - `active` — bytes in pages jemalloc has handed to a size class. `active`
//!   minus `allocated` is **fragmentation**: space inside pages the program is
//!   not using and cannot be given back while a neighbour is live.
//! - `metadata` — jemalloc's own bookkeeping.
//! - `resident` — physical pages, the allocator's share of RSS. `resident` minus
//!   `active` is dirty-but-idle memory, which decay would return.
//! - `mapped` — address space in jemalloc's extents.
//! - `retained` — address space taken from the OS and *deliberately not*
//!   returned, kept for reuse. Virtual, not resident: it costs no RSS, and a
//!   large value here is not a leak.
//!
//! `allocated` ≪ `resident` means tuning (decay, arenas). `allocated` tracking
//! the working set means the serving path really is holding that memory, and the
//! next step is a heap profile rather than a knob.
//!
//! # In a test binary the numbers are ~0, not wrong
//!
//! `tikv-jemalloc-ctl` reads whichever jemalloc is linked, and only the `tansu`
//! binary installs it as `#[global_allocator]`. A test or an embedding that
//! allocates through the system allocator will see an idle jemalloc and report
//! near-zero — the instrument is honest about the allocator it was asked about,
//! which is not necessarily the one doing the work.

use std::sync::OnceLock;

use opentelemetry::{KeyValue, global, metrics::ObservableGauge};
use tikv_jemalloc_ctl::{epoch, stats};
use tracing::warn;

/// Kept alive for the process: dropping the instrument unregisters its callback,
/// and this one is only ever registered once.
static ALLOCATOR_BYTES: OnceLock<ObservableGauge<u64>> = OnceLock::new();

/// The classes, in nesting order, paired with the `mallctl` read behind each.
///
/// Function pointers rather than a closure per class so the callback stays one
/// loop: every read has the same signature, and the order is the order the doc
/// comment above explains them in.
type Read = fn() -> tikv_jemalloc_ctl::Result<usize>;

const CLASSES: [(&str, Read); 6] = [
    ("allocated", stats::allocated::read),
    ("active", stats::active::read),
    ("metadata", stats::metadata::read),
    ("resident", stats::resident::read),
    ("mapped", stats::mapped::read),
    ("retained", stats::retained::read),
];

/// Register `tansu_allocator_bytes`, once, against the current global meter.
///
/// Called after the meter provider is installed: an instrument built on the
/// no-op provider that precedes it would never be collected.
pub(super) fn register() {
    _ = ALLOCATOR_BYTES.get_or_init(|| {
        global::meter(env!("CARGO_PKG_NAME"))
            .u64_observable_gauge("tansu_allocator_bytes")
            .with_description(
                "jemalloc's own accounting of this process's heap: live bytes \
                 through to address space retained from the OS",
            )
            .with_unit("By")
            .with_callback(|observer| {
                // Every `stats.*` value is a snapshot taken when the epoch last
                // advanced, so without this they are the numbers from process
                // start, for the life of the process. Advancing here — once per
                // collection interval — is what makes them live.
                if let Err(err) = epoch::advance() {
                    warn!(?err, "jemalloc epoch would not advance; heap stats stale");
                    return;
                }

                for (class, read) in CLASSES {
                    match read() {
                        Ok(bytes) => {
                            observer.observe(bytes as u64, &[KeyValue::new("class", class)])
                        }

                        // One unreadable class is not a reason to lose the other
                        // five: `retained` in particular is absent on a jemalloc
                        // built without it, and that must not blind the gauge.
                        Err(err) => warn!(?err, class, "unreadable jemalloc stat"),
                    }
                }
            })
            .build()
    });
}

#[cfg(test)]
mod tests {
    use opentelemetry::global;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData},
    };

    use super::*;

    /// Registration order is the whole correctness of this module, and it is
    /// invisible: an instrument built before the provider is installed compiles,
    /// runs, and reports nothing for the life of the process. So the test
    /// installs a provider first, registers, and collects.
    ///
    /// It asserts the six classes are there, not their values: a test binary
    /// allocates through the system allocator, so the jemalloc it reads is idle
    /// (see the module docs).
    #[test]
    fn every_class_is_observed() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();

        global::set_meter_provider(provider.clone());
        register();

        provider.force_flush().expect("flush");

        let mut observed = std::collections::BTreeSet::new();

        for resource in exporter.get_finished_metrics().expect("metrics") {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics() {
                    if metric.name() != "tansu_allocator_bytes" {
                        continue;
                    }

                    let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
                        panic!("not a u64 gauge: {:?}", metric.data());
                    };

                    for point in gauge.data_points() {
                        let class = point
                            .attributes()
                            .find(|attribute| attribute.key.as_str() == "class")
                            .map(|attribute| attribute.value.to_string())
                            .expect("class attribute");

                        _ = observed.insert(class);
                    }
                }
            }
        }

        assert_eq!(
            CLASSES
                .iter()
                .map(|(class, _)| (*class).to_owned())
                .collect::<std::collections::BTreeSet<_>>(),
            observed
        );
    }
}
