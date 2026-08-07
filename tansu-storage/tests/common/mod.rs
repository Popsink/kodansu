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

use std::{env, fmt, io, sync::Arc};

use dotenv::dotenv;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::{EnvFilter, filter::ParseError};
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum Error {
    #[allow(dead_code)]
    Io(Arc<io::Error>),

    #[allow(dead_code)]
    Message(String),

    /// Raised only by the conditional-put conformance target (#357), which is the
    /// one test that talks to the object store directly rather than through the
    /// engine — the error *class* a losing conditional writer gets is the thing
    /// under test there, so it cannot be flattened into [`Error::Storage`].
    #[cfg(feature = "dynostore")]
    #[allow(dead_code)]
    ObjectStore(Arc<object_store::Error>),

    #[allow(dead_code)]
    ParseFilter(Arc<ParseError>),

    Parse(#[from] url::ParseError),

    Protocol(#[from] tansu_sans_io::Error),
    Storage(#[from] tansu_storage::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

#[cfg(feature = "dynostore")]
impl From<object_store::Error> for Error {
    fn from(value: object_store::Error) -> Self {
        Self::ObjectStore(Arc::new(value))
    }
}

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
    _ = dotenv().ok();

    let mut url =
        Url::parse(&env::var(STORAGE_URL).unwrap_or_else(|_| String::from(DEFAULT_STORAGE_URL)))?;

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

pub(crate) fn init_tracing() -> Result<DefaultGuard, Error> {
    use std::{fs::File, sync::Arc, thread};

    _ = dotenv().ok();

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!(
                            "../logs/{}/{}::{name}.log",
                            env!("CARGO_PKG_NAME"),
                            env!("CARGO_CRATE_NAME")
                        ))
                        .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}
