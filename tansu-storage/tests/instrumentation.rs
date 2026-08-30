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

//! A read that succeeds says nothing at `INFO` (#428).
//!
//! `#[instrument]` with no level is `INFO`, and `ret` records the return value
//! as an event at the span's level. Two object-store decorators carried that
//! annotation on `get_opts` — `Metron` and the metadata `Cache` — and
//! `DynoStore::new` installs both on **every** backend, so one GET emitted two
//! formatted `INFO` events carrying the `Debug` of a whole
//! `Result<GetResult, _>`. On a read path that issues one GET per segment per
//! partition per topic, and a fleet that runs at `RUST_LOG=info`.
//!
//! The issue filed this as a GCS-only defect in a decorator only the `gs` arm
//! installs. It was that too — and two more, on paths every deployment takes.
//!
//! A test rather than a code review, because this is a one-word annotation that
//! reads as harmless and had already been copied three times.

use std::sync::{Arc, Mutex};

use tansu_sans_io::create_topics_request::CreatableTopic;
use tansu_storage::{Storage, StorageContainer};
use tracing::{Level, subscriber::DefaultGuard};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
};
use url::Url;
use uuid::Uuid;

/// Records every event this crate emits at `INFO` or above.
#[derive(Clone, Default)]
struct Loud(Arc<Mutex<Vec<String>>>);

impl Loud {
    fn take(&self) -> Vec<String> {
        self.0.lock().expect("loud events").drain(..).collect()
    }
}

impl<S> Layer<S> for Loud
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();

        if *meta.level() <= Level::INFO
            && meta.target().starts_with("tansu_storage")
            && let Ok(mut events) = self.0.lock()
        {
            events.push(format!("{} {}", meta.level(), meta.target()));
        }
    }
}

fn capturing() -> (Loud, DefaultGuard) {
    let loud = Loud::default();

    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(loud.clone()));

    (loud, guard)
}

/// The healthy path is silent. Anything this crate has to say about a
/// successful read belongs at `debug`, where an operator can ask for it — an
/// `INFO` per GET is not a diagnostic, it is the read path talking to itself at
/// the rate of the read path.
#[tokio::test]
async fn a_successful_read_is_silent_at_info() {
    let (loud, _guard) = capturing();

    let storage = StorageContainer::builder()
        .cluster_id(Uuid::now_v7().to_string())
        .node_id(111)
        .advertised_listener(Url::parse("tcp://localhost:9092").expect("listener"))
        .storage(Url::parse("memory://").expect("storage"))
        .build()
        .await
        .expect("storage");

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("instrumentation".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await
        .expect("create");

    // Whatever creating a topic had to say is not what this is about.
    _ = loud.take();

    _ = storage.metadata(None).await.expect("metadata");

    let events = loud.take();

    assert!(
        events.is_empty(),
        "a successful read emitted {} event(s) at INFO or above: {events:?}",
        events.len(),
    );
}
