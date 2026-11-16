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

//! What the storage tests share: their error type, the store they run
//! against and a fresh cluster for each, the topics they create, and a log
//! file per test.

use std::sync::Arc;

use tansu_sans_io::Error as ProtocolError;
use tansu_storage::Error as StorageError;
use tracing_subscriber::filter::ParseError as FilterError;
use url::ParseError as UrlError;
use url::Url;
use uuid::Uuid;

/// What the tests name it: every storage test returns `Result<_, Error>`.
pub(crate) type Error = Failure;

/// Everything a storage test can fail with. Only the conversions matter: the
/// payloads are there for the `Debug` a failing test prints.
#[derive(thiserror::Error, Clone, Debug)]
#[allow(dead_code, reason = "the payloads are only read through `Debug`")]
pub(crate) enum Failure {
    Io(Arc<std::io::Error>),

    /// A test's own failure, described.
    Failed(String),

    /// Raised only by the conditional-put conformance target (#357), which is the
    /// one test that talks to the object store directly rather than through the
    /// engine — the error *class* a losing conditional writer gets is the thing
    /// under test there, so it cannot be flattened into [`Failure::Storage`].
    #[cfg(feature = "dynostore")]
    ObjectStore(Arc<object_store::Error>),

    ParseFilter(Arc<FilterError>),
    Parse(#[from] UrlError),
    Protocol(#[from] ProtocolError),
    Storage(#[from] StorageError),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

/// `From` for the sources a [`Failure`] holds behind an `Arc`, to stay `Clone`.
macro_rules! shared {
    ($($(#[$gate:meta])* $variant:ident <- $source:ty),* $(,)?) => {
        $($(#[$gate])* impl From<$source> for Failure {
            fn from(source: $source) -> Self {
                Self::$variant(Arc::new(source))
            }
        })*
    };
}

shared!(
    Io <- std::io::Error,
    ParseFilter <- FilterError,
    #[cfg(feature = "dynostore")]
    ObjectStore <- object_store::Error,
);

/// Names the object store the whole `tansu-storage` suite builds against (#357).
///
/// Unset it and every test runs on `memory://tansu/`, which is what `just test`
/// does and what CI does: `object_store`'s `InMemory` needs no service, so a
/// laptop with no Docker runs the suite. Set it to `s3://tansu/` (with the
/// `AWS_*` environment the store reads) and the same tests run against minio or
/// real S3 — `just test-storage-minio`.
///
/// Deliberately *not* `STORAGE_ENGINE`: that is the broker's own variable,
/// `example.env` sets it to `s3://tansu/`, and [`init_tracing`] loads `.env`.
/// Reusing the name would silently point the suite at a store that is not
/// running on any machine that had once run `just broker`.
pub(crate) const STORAGE_URL: &str = "TANSU_TEST_STORAGE_URL";

const DEFAULT_STORAGE_URL: &str = "memory://tansu/";

/// The storage URL for a test that needs no URL query parameters.
#[allow(dead_code)]
pub(crate) fn storage_url() -> Result<Url, Error> {
    storage_url_with_query("")
}

/// The storage URL with `query` appended to whatever [`STORAGE_URL`] already
/// carries, so pointing the suite at `s3://tansu/?batch_min_size=64KiB` does not
/// drop the per-test keys the URL is also how tests set.
#[allow(dead_code)]
pub(crate) fn storage_url_with_query(query: &str) -> Result<Url, Error> {
    _ = dotenv::dotenv();

    let mut url = Url::parse(
        &std::env::var(STORAGE_URL).unwrap_or_else(|_| String::from(DEFAULT_STORAGE_URL)),
    )?;

    if !query.is_empty() {
        let merged = match url.query() {
            Some(existing) if !existing.is_empty() => format!("{existing}&{query}"),
            _ => query.to_owned(),
        };

        url.set_query(Some(&merged));
    }

    Ok(url)
}

/// A cluster id no other test uses.
///
/// Every object the engine writes is keyed under `clusters/{cluster}/`, so the
/// cluster id *is* the per-test prefix (#357). On `memory://` each `build()` gets
/// a fresh `InMemory` and isolation is free; against one real bucket, two tests
/// that both create a topic called `pqr` are otherwise the same object, and the
/// second one either fails or — worse — passes on the first one's state.
///
/// One id per `build()`, not per test: that is exactly the isolation `InMemory`
/// gives today, so a test that builds two containers keeps seeing two unrelated
/// stores on S3 as well.
#[allow(dead_code)]
pub(crate) fn cluster_id() -> String {
    Uuid::now_v7().to_string()
}

/// The node id [`storage`] runs as.
#[allow(dead_code)]
pub(crate) const NODE_ID: i32 = 111;

/// The host [`storage`] advertises.
#[allow(dead_code)]
pub(crate) const HOST: &str = "localhost";

/// The port [`storage`] advertises.
#[allow(dead_code)]
pub(crate) const PORT: i32 = 9092;

/// A storage engine over [`storage_url`], under a fresh [`cluster_id`], for
/// [`NODE_ID`] advertising [`HOST`] and [`PORT`]: the setup a test needs when
/// nothing about the broker itself is under test.
#[allow(dead_code)]
pub(crate) async fn storage() -> Result<Arc<dyn tansu_storage::Storage>, Error> {
    storage_with_query("").await
}

/// [`storage`], over [`storage_url_with_query`] with `query`: for a test that
/// sets an engine option through the URL.
#[allow(dead_code)]
pub(crate) async fn storage_with_query(
    query: &str,
) -> Result<Arc<dyn tansu_storage::Storage>, Error> {
    let advertised = Url::parse(&format!("tcp://{HOST}:{PORT}"))?;
    let engine = tansu_storage::StorageContainer::builder()
        .cluster_id(cluster_id())
        .node_id(NODE_ID)
        .advertised_listener(advertised)
        .storage(storage_url_with_query(query)?);
    Ok(engine.build().await?)
}

/// Sends `request` to `service` with `storage` as its state, the way the broker
/// routes a decoded frame.
#[allow(dead_code)]
pub(crate) async fn serve<S, Request>(
    storage: &Arc<dyn tansu_storage::Storage>,
    service: S,
    request: Request,
) -> Result<S::Response, Error>
where
    S: rama::Service<Arc<dyn tansu_storage::Storage>, Request>,
    Error: From<S::Error>,
{
    let state = rama::Context::with_state(Arc::clone(storage));
    Ok(service.serve(state, request).await?)
}

/// Creates `name` with one partition, one replica and the broker defaults.
#[allow(dead_code)]
pub(crate) async fn topic(
    storage: &Arc<dyn tansu_storage::Storage>,
    name: &str,
) -> Result<(), Error> {
    _ = topic_partitioned(storage, name, 1).await?;
    Ok(())
}

/// Creates `name` with `partitions` partitions, one replica and the broker
/// defaults, answering the id the store gave it.
#[allow(dead_code)]
pub(crate) async fn topic_partitioned(
    storage: &Arc<dyn tansu_storage::Storage>,
    name: &str,
    partitions: i32,
) -> Result<Uuid, Error> {
    let creatable = tansu_sans_io::create_topics_request::CreatableTopic::default()
        .name(name.to_owned())
        .num_partitions(partitions)
        .replication_factor(1)
        .assignments(Some(Vec::new()))
        .configs(Some(Vec::new()));
    Ok(storage.create_topic(creatable, false).await?)
}

/// Loads `.env`, then logs this test at `debug` for this binary, on top of
/// `RUST_LOG`, into `../logs/<package>/<crate>::<thread>.log`.
pub(crate) fn init_tracing() -> Result<tracing::subscriber::DefaultGuard, Error> {
    _ = dotenv::dotenv();
    let directive = format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?;
    let filter = tracing_subscriber::EnvFilter::from_default_env().add_directive(directive);

    let unnamed = || Error::Failed(String::from("unnamed thread"));
    let current = std::thread::current();
    let test = current.name().ok_or_else(unnamed)?;
    let (package, binary) = (env!("CARGO_PKG_NAME"), env!("CARGO_CRATE_NAME"));
    let log = std::fs::File::create(format!("../logs/{package}/{binary}::{test}.log"))?;

    let subscriber = tracing_subscriber::fmt().with_level(true);
    let subscriber = subscriber.with_line_number(true).with_thread_names(false);
    let subscriber = subscriber
        .with_env_filter(filter)
        .with_writer(Arc::new(log));
    Ok(tracing::subscriber::set_default(subscriber.finish()))
}
