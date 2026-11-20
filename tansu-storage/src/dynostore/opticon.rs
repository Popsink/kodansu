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

use std::fmt::Debug;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use bytes::Bytes;
use object_store::path::Path;
use object_store::{Attributes, GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt as _};
use object_store::{PutMode, PutOptions, PutPayload, TagSet, UpdateVersion};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::{debug, instrument, warn};

use super::METER;
use crate::Result;
use crate::dynostore::object_store_error_name;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DataVersion<D> {
    data: D,
    version: Option<UpdateVersion>,
}

impl<D> From<&DataVersion<D>> for PutMode {
    fn from(value: &DataVersion<D>) -> Self {
        match value.version.clone() {
            Some(known) => PutMode::Update(known),
            None => PutMode::Create,
        }
    }
}

static REQUESTS: LazyLock<Counter<u64>> =
    LazyLock::new(|| counter("tansu_opticon_requests", "OptiCon requests"));

static ERRORS: LazyLock<Counter<u64>> =
    LazyLock::new(|| counter("tansu_opticon_errors", "OptiCon requests"));

/// Number of optimistic-concurrency conflict retries a single `with_mut` call
/// took before committing. On a backend that caps single-object update rate
/// (GCS ~1 write/s/object, #13), a hot object shows up here as a rising
/// distribution — the contention that was previously invisible (30s latency,
/// zero log lines).
static RETRIES: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    let histogram = METER.u64_histogram("tansu_opticon_with_mut_retries");
    histogram
        .with_description("OptiCon with_mut conflict retries per call")
        .build()
});

/// A counter called `name`.
fn counter(name: &'static str, description: &'static str) -> Counter<u64> {
    let counter = METER.u64_counter(name);
    counter.with_description(description).build()
}

/// Count one request by `method`.
fn count(method: &'static str) {
    REQUESTS.add(1, &[KeyValue::new("method", method)]);
}

/// Log and count `outcome` when it is an object-store failure of `method`.
fn noted<T>(method: &'static str, outcome: &std::result::Result<T, object_store::Error>) {
    let Err(error) = outcome else {
        return;
    };
    debug!(error = ?error);
    let reason = object_store_error_name(error);
    let labels = [
        KeyValue::new("method", method),
        KeyValue::new("error", reason),
    ];
    ERRORS.add(1, &labels);
}

/// The version an object store reports for what it holds.
fn version_of(meta: &ObjectMeta) -> UpdateVersion {
    UpdateVersion {
        e_tag: meta.e_tag.clone(),
        version: meta.version.clone(),
    }
}

/// `data` as the JSON body of a put.
fn encoded<D: Serialize>(data: &D) -> Result<PutPayload> {
    let json = serde_json::to_vec(data)?;
    Ok(PutPayload::from(Bytes::from(json)))
}

/// A `with_mut` call that retries more than this many times on conflict is
/// almost certainly contending on a hot object; surface it in the logs.
const RETRY_WARN_THRESHOLD: u64 = 8;

#[derive(Clone, Debug, Default)]
pub(super) struct OptiCon<D> {
    path: Path,
    tags: TagSet,
    attributes: Attributes,
    data_version: Arc<Mutex<Option<DataVersion<D>>>>,
}

impl<D> OptiCon<D> {
    pub(super) fn path(path: impl Into<Path>) -> Self {
        OptiCon {
            path: path.into(),
            tags: TagSet::default(),
            attributes: Attributes::default(),
            data_version: Arc::default(),
        }
    }

    /// The cached value and its version, locked.
    fn guard(&self) -> Result<MutexGuard<'_, Option<DataVersion<D>>>> {
        Ok(self.data_version.lock()?)
    }

    /// Options for a put of this object in `mode`, with its tags and attributes.
    fn put_options(&self, mode: PutMode) -> PutOptions {
        PutOptions {
            mode,
            tags: self.tags.clone(),
            attributes: self.attributes.clone(),
            ..PutOptions::default()
        }
    }
}

impl<D> OptiCon<D>
where
    D: Clone + Debug + Default + DeserializeOwned + PartialEq + Serialize,
{
    #[instrument(skip_all, fields(path = %self.path))]
    async fn get(&self, object_store: &impl ObjectStore) -> Result<()> {
        const METHOD: &str = "get";
        count(METHOD);

        let fetched = object_store.get(&self.path).await;
        noted(METHOD, &fetched);
        match fetched {
            Ok(found) => {
                let version = Some(version_of(&found.meta));
                let body = found.bytes().await;
                noted(METHOD, &body);
                let data = serde_json::from_slice::<D>(&body?)?;
                debug!(version = ?version);
                _ = self.guard()?.replace(DataVersion { data, version });
                Ok(())
            }
            Err(object_store::Error::NotFound { .. }) => {
                _ = self.guard()?.take();
                Ok(())
            }
            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// Refresh the cached value+version with a conditional GET (`if_none_match`
    /// against the cached etag). `NotModified` keeps the cache as-is, `NotFound`
    /// clears it. Shared by [`Self::with`] and [`Self::get_opt`].
    #[instrument(skip_all, fields(path = %self.path))]
    async fn refresh(&self, object_store: &impl ObjectStore) -> Result<()> {
        const METHOD: &str = "refresh";

        let version = {
            let held = self.guard()?;
            held.as_ref().and_then(|current| current.version.clone())
        };
        debug!(version = ?version);

        let if_none_match = version.as_ref().and_then(|known| known.e_tag.clone());
        let options = GetOptions {
            if_none_match,
            ..GetOptions::default()
        };
        let fetched = object_store.get_opts(&self.path, options).await;
        noted(METHOD, &fetched);
        match fetched {
            Ok(found) => {
                let version = Some(version_of(&found.meta));
                debug!(action = "out of date", version = ?version);
                let body = found.bytes().await;
                noted(METHOD, &body);
                let data = serde_json::from_slice::<D>(&body?)?;
                _ = self.guard()?.replace(DataVersion { data, version });
                Ok(())
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!(action = "not found");
                _ = self.guard()?.take();
                Ok(())
            }
            Err(object_store::Error::NotModified { .. }) => {
                debug!(action = "not modified");
                Ok(())
            }
            Err(otherwise) => Err(otherwise.into()),
        }
    }

    #[instrument(skip_all, fields(path = %self.path))]
    pub(super) async fn with<E, F>(&self, object_store: &impl ObjectStore, f: F) -> Result<E>
    where
        F: Fn(&D) -> Result<E>,
    {
        count("with");

        let refreshed = self.refresh(object_store).await;
        let read = self.guard().and_then(|held| match held.as_ref() {
            Some(current) => {
                debug!(dv = ?current);
                f(&current.data)
            }
            None => {
                let data = D::default();
                debug!(data = ?data);
                f(&data)
            }
        });
        refreshed.and(read)
    }

    /// Read the current value, returning `None` when the backing object does
    /// not exist. Unlike [`Self::with`], absence is reported as `None` rather
    /// than the `Default` value, so callers can distinguish an absent key from
    /// one holding default fields (topic existence checks rely on this).
    /// The in-memory cached value **without any object-store request**, or
    /// `None` when nothing has been read into the cache yet. Used to serve a
    /// rarely-changing value (e.g. the log-start watermark) on a hot read path
    /// without a per-call conditional GET (#109). Freshness is the caller's
    /// responsibility — the cache is populated by [`Self::with`]/[`Self::get`]
    /// on the cold/slow path.
    pub(super) fn cached(&self) -> Option<D> {
        let held = self.data_version.lock().ok()?;
        held.as_ref().map(|current| current.data.clone())
    }

    #[instrument(skip_all, fields(path = %self.path))]
    pub(super) async fn get_opt(&self, object_store: &impl ObjectStore) -> Result<Option<D>> {
        count("get_opt");
        self.refresh(object_store).await?;
        let held = self.guard()?;
        Ok(held.as_ref().map(|current| current.data.clone()))
    }

    #[instrument(skip_all, fields(path = %self.path))]
    pub(super) async fn with_mut<E, F>(&self, object_store: &impl ObjectStore, f: F) -> Result<E>
    where
        E: Debug,
        F: Fn(&mut D) -> Result<E>,
    {
        const METHOD: &str = "with_mut";
        count(METHOD);

        let mut retries: u64 = 0;
        loop {
            count("with_mut_loop");

            let (outcome, dv) = {
                let held = self.data_version.lock()?;
                let mut dv = held.clone().unwrap_or_default();
                let outcome = f(&mut dv.data);
                (outcome, dv)
            };

            let payload = encoded(&dv.data)?;
            let options = self.put_options(PutMode::from(&dv));
            let put = object_store.put_opts(&self.path, payload, options).await;
            noted(METHOD, &put);
            match put {
                Ok(written) => {
                    RETRIES.record(retries, &[]);
                    let version = Some(UpdateVersion {
                        e_tag: written.e_tag,
                        version: written.version,
                    });
                    let data = dv.data;
                    let stored = self
                        .guard()
                        .map(|mut held| held.replace(DataVersion { data, version }));
                    return stored.and(outcome);
                }

                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                ) => {
                    retries += 1;
                    if retries >= RETRY_WARN_THRESHOLD {
                        let why = "OptiCon with_mut contending on a hot object (per-object update-rate cap?)";
                        warn!(path = %self.path, retries = retries, "{why}");
                    }
                    self.get(object_store).await?;
                }

                Err(err) => return Err(err.into()),
            }
        }
    }

    /// Create the backing object iff it does not already exist
    /// (`PutMode::Create`). Returns `Ok(true)` when this call created it,
    /// `Ok(false)` when it already existed. On success the cached version is
    /// seeded so a following read is served warm; on conflict the cache is
    /// refreshed from the winner's object.
    #[instrument(skip_all, fields(path = %self.path))]
    pub(super) async fn create(&self, object_store: &impl ObjectStore, data: D) -> Result<bool> {
        count("create");

        let payload = encoded(&data)?;
        let options = self.put_options(PutMode::Create);
        match object_store.put_opts(&self.path, payload, options).await {
            Ok(written) => {
                let version = Some(UpdateVersion {
                    e_tag: written.e_tag,
                    version: written.version,
                });
                _ = self.guard()?.replace(DataVersion { data, version });
                Ok(true)
            }
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.refresh(object_store).await?;
                Ok(false)
            }
            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// Delete the backing object (idempotent on `NotFound`) and clear the cache.
    #[instrument(skip_all, fields(path = %self.path))]
    pub(super) async fn remove(&self, object_store: &impl ObjectStore) -> Result<()> {
        count("remove");
        match object_store.delete(&self.path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                _ = self.guard()?.take();
                Ok(())
            }
            Err(otherwise) => Err(otherwise.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::thread;

    use object_store::memory::InMemory;
    use serde::Deserialize;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;
    use crate::Error;

    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X(i32);

    /// Debug logs from this crate, to this test thread's own file under `logs/`.
    fn init_tracing() -> Result<DefaultGuard> {
        let krate = env!("CARGO_PKG_NAME");
        let directive = format!("{}=debug", krate.replace('-', "_")).parse()?;
        let filter = EnvFilter::from_default_env().add_directive(directive);

        let current = thread::current();
        let name = current
            .name()
            .ok_or(Error::Message(String::from("unnamed thread")))?;
        let log = Arc::new(File::create(format!("../logs/{krate}/{name}.log"))?);

        let subscriber = tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(filter)
            .with_writer(log)
            .finish();
        Ok(tracing::subscriber::set_default(subscriber))
    }

    /// Where every case keeps its object.
    fn location() -> Path {
        Path::from("/abc/test.json")
    }

    /// An object store holding `X(value)` at [`location`], or nothing.
    async fn store(value: Option<i32>) -> Result<InMemory> {
        let store = InMemory::new();
        if let Some(value) = value {
            _ = store.put(&location(), encoded(&X(value))?).await?;
        }
        Ok(store)
    }

    /// What `store` holds at [`location`], `None` when it holds nothing.
    async fn stored(store: &InMemory) -> Result<Option<i32>> {
        match store.get(&location()).await {
            Ok(found) => Ok(Some(serde_json::from_slice::<X>(&found.bytes().await?)?.0)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// One more than the value read.
    fn next(x: &X) -> Result<i32> {
        Ok(x.0 + 1)
    }

    /// Add one to the value in place, and answer the new value.
    fn bump(x: &mut X) -> Result<i32> {
        x.0 += 1;
        Ok(x.0)
    }

    #[tokio::test]
    async fn with_does_not_exist() -> Result<()> {
        let _guard = init_tracing()?;
        let store = store(None).await?;
        let o = OptiCon::path(location());

        for _ in 0..2 {
            assert_eq!(1, o.with(&store, next).await?);
            assert_eq!(None, stored(&store).await?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn with_mut_does_not_exist() -> Result<()> {
        let _guard = init_tracing()?;
        let store = store(None).await?;
        let o = OptiCon::path(location());

        for expected in [1, 2] {
            assert_eq!(expected, o.with_mut(&store, bump).await?);
            assert_eq!(Some(expected), stored(&store).await?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn with_did_exist() -> Result<()> {
        let _guard = init_tracing()?;
        let store = store(Some(6)).await?;
        let o = OptiCon::path(location());

        assert_eq!(7, o.with(&store, next).await?);

        store.delete(&location()).await?;
        for _ in 0..2 {
            assert_eq!(1, o.with(&store, next).await?);
            assert_eq!(None, stored(&store).await?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn with_mut_did_exist() -> Result<()> {
        let _guard = init_tracing()?;
        let store = store(Some(6)).await?;
        let o = OptiCon::path(location());

        assert_eq!(7, o.with_mut(&store, bump).await?);
        assert_eq!(Some(7), stored(&store).await?);

        store.delete(&location()).await?;
        for expected in [1, 2] {
            assert_eq!(expected, o.with_mut(&store, bump).await?);
            assert_eq!(Some(expected), stored(&store).await?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn with_already_exists() -> Result<()> {
        let _guard = init_tracing()?;
        let store = store(Some(6)).await?;
        let o = OptiCon::path(location());

        for _ in 0..2 {
            assert_eq!(7, o.with(&store, next).await?);
            assert_eq!(Some(6), stored(&store).await?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn with_mut_already_exists() -> Result<()> {
        let _guard = init_tracing()?;
        let store = store(Some(6)).await?;
        let o = OptiCon::path(location());
        let six_times = |x: &mut X| bump(x).map(|bumped| 6 * bumped);

        for (answered, held) in [(42, 7), (48, 8)] {
            assert_eq!(answered, o.with_mut(&store, six_times).await?);
            assert_eq!(Some(held), stored(&store).await?);
        }
        Ok(())
    }
}
