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

use std::{
    fmt::Debug,
    sync::{Arc, LazyLock, Mutex},
};

use crate::{Result, dynostore::object_store_error_name};
use bytes::Bytes;
use object_store::{
    Attributes, GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, TagSet,
    UpdateVersion, path::Path,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use serde::{Serialize, de::DeserializeOwned};
use tracing::{debug, instrument, warn};

use super::METER;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DataVersion<D> {
    data: D,
    version: Option<UpdateVersion>,
}

impl<D> From<&DataVersion<D>> for PutMode {
    fn from(value: &DataVersion<D>) -> Self {
        value
            .version
            .clone()
            .map_or(PutMode::Create, PutMode::Update)
    }
}

static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_opticon_requests")
        .with_description("OptiCon requests")
        .build()
});

static ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tansu_opticon_errors")
        .with_description("OptiCon requests")
        .build()
});

/// Number of optimistic-concurrency conflict retries a single `with_mut` call
/// took before committing. On a backend that caps single-object update rate
/// (GCS ~1 write/s/object, #13), a hot object shows up here as a rising
/// distribution — the contention that was previously invisible (30s latency,
/// zero log lines).
static RETRIES: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tansu_opticon_with_mut_retries")
        .with_description("OptiCon with_mut conflict retries per call")
        .build()
});

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
        Self {
            path: path.into(),
            tags: Default::default(),
            attributes: Default::default(),
            data_version: Default::default(),
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
        REQUESTS.add(1, &[KeyValue::new("method", METHOD)]);

        let on_error = |error: &object_store::Error| {
            ERRORS.add(
                1,
                &[
                    KeyValue::new("method", METHOD),
                    KeyValue::new("error", object_store_error_name(error)),
                ],
            );
        };

        match object_store.get(&self.path).await.inspect_err(|error| {
            debug!(?error);
            on_error(error)
        }) {
            Ok(get_result) => {
                let version = Some(UpdateVersion {
                    e_tag: get_result.meta.e_tag.clone(),
                    version: get_result.meta.version.clone(),
                });

                let encoded = get_result.bytes().await.inspect_err(|error| {
                    debug!(?error);
                    on_error(error)
                })?;
                let data = serde_json::from_slice::<D>(&encoded)?;

                debug!(?version);

                self.data_version
                    .lock()
                    .map_err(Into::into)
                    .map(|mut lock| lock.replace(DataVersion { data, version }))
                    .and(Ok(()))
            }

            Err(object_store::Error::NotFound { .. }) => self
                .data_version
                .lock()
                .map_err(Into::into)
                .map(|mut lock| lock.take())
                .and(Ok(())),

            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// Refresh the cached value+version with a conditional GET (`if_none_match`
    /// against the cached etag). `NotModified` keeps the cache as-is, `NotFound`
    /// clears it. Shared by [`Self::with`] and [`Self::get_opt`].
    #[instrument(skip_all, fields(path = %self.path))]
    async fn refresh(&self, object_store: &impl ObjectStore) -> Result<()> {
        const METHOD: &str = "refresh";

        let on_error = |error: &object_store::Error| {
            ERRORS.add(
                1,
                &[
                    KeyValue::new("method", METHOD),
                    KeyValue::new("error", object_store_error_name(error)),
                ],
            );
        };

        let version = self
            .data_version
            .lock()
            .map(|guard| guard.as_ref().and_then(|dv| dv.version.clone()))?;
        debug!(?version);

        match object_store
            .get_opts(
                &self.path,
                GetOptions {
                    if_none_match: version.as_ref().and_then(|version| version.e_tag.clone()),
                    ..GetOptions::default()
                },
            )
            .await
            .inspect_err(|error| {
                debug!(?error);
                on_error(error)
            }) {
            Ok(get_result) => {
                let version = Some(UpdateVersion {
                    e_tag: get_result.meta.e_tag.clone(),
                    version: get_result.meta.version.clone(),
                });

                debug!(action = "out of date", ?version);

                get_result
                    .bytes()
                    .await
                    .inspect_err(|error| {
                        debug!(?error);
                        on_error(error)
                    })
                    .map_err(Into::into)
                    .and_then(|encoded| serde_json::from_slice::<D>(&encoded).map_err(Into::into))
                    .and_then(|data| {
                        self.data_version
                            .lock()
                            .map_err(Into::into)
                            .map(|mut guard| guard.replace(DataVersion { data, version }))
                    })
                    .and(Ok(()))
            }

            Err(object_store::Error::NotFound { .. }) => {
                debug!(action = "not found");
                self.data_version
                    .lock()
                    .map_err(Into::into)
                    .map(|mut guard| guard.take())
                    .and(Ok(()))
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
        REQUESTS.add(1, &[KeyValue::new("method", "with")]);

        self.refresh(object_store)
            .await
            .and(
                self.data_version
                    .lock()
                    .map_err(Into::into)
                    .and_then(|lock| {
                        if let Some(dv @ DataVersion { data, .. }) = lock.as_ref() {
                            debug!(?dv);
                            f(data)
                        } else {
                            let data = D::default();
                            debug!(?data);
                            f(&data)
                        }
                    }),
            )
    }

    /// Read the current value, returning `None` when the backing object does
    /// not exist. Unlike [`Self::with`], absence is reported as `None` rather
    /// than the `Default` value, so callers can distinguish an absent key from
    /// one holding default fields (topic existence checks rely on this).
    #[instrument(skip_all, fields(path = %self.path))]
    pub(super) async fn get_opt(&self, object_store: &impl ObjectStore) -> Result<Option<D>> {
        REQUESTS.add(1, &[KeyValue::new("method", "get_opt")]);

        self.refresh(object_store).await?;
        self.data_version
            .lock()
            .map_err(Into::into)
            .map(|guard| guard.as_ref().map(|dv| dv.data.clone()))
    }

    #[instrument(skip_all, fields(path = %self.path))]
    pub(super) async fn with_mut<E, F>(&self, object_store: &impl ObjectStore, f: F) -> Result<E>
    where
        E: Debug,
        F: Fn(&mut D) -> Result<E>,
    {
        const METHOD: &str = "with_mut";
        REQUESTS.add(1, &[KeyValue::new("method", METHOD)]);

        let on_error = |error: &object_store::Error| {
            ERRORS.add(
                1,
                &[
                    KeyValue::new("method", METHOD),
                    KeyValue::new("error", object_store_error_name(error)),
                ],
            );
        };

        let mut retries: u64 = 0;

        loop {
            REQUESTS.add(1, &[KeyValue::new("method", "with_mut_loop")]);

            let (outcome, dv) = self.data_version.lock().map(|guard| {
                let mut dv = guard.clone().unwrap_or_default();
                let outcome = f(&mut dv.data);
                (outcome, dv)
            })?;

            let payload = serde_json::to_vec(&dv.data)
                .map(Bytes::from)
                .map(PutPayload::from)?;

            let opts = PutOptions {
                mode: PutMode::from(&dv),
                tags: self.tags.clone(),
                attributes: self.attributes.clone(),
                ..Default::default()
            };

            match object_store
                .put_opts(&self.path, payload, opts)
                .await
                .inspect_err(|error| {
                    debug!(?error);
                    on_error(error)
                }) {
                Ok(put_result) => {
                    RETRIES.record(retries, &[]);

                    return self
                        .data_version
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| {
                            guard.replace(DataVersion {
                                data: dv.data,
                                version: Some(UpdateVersion {
                                    e_tag: put_result.e_tag,
                                    version: put_result.version,
                                }),
                            })
                        })
                        .and(outcome);
                }

                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                ) => {
                    retries += 1;

                    if retries >= RETRY_WARN_THRESHOLD {
                        warn!(
                            path = %self.path,
                            retries,
                            "OptiCon with_mut contending on a hot object (per-object update-rate cap?)"
                        );
                    }

                    self.get(object_store).await?;
                    continue;
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
        REQUESTS.add(1, &[KeyValue::new("method", "create")]);

        let payload = serde_json::to_vec(&data)
            .map(Bytes::from)
            .map(PutPayload::from)?;

        let opts = PutOptions {
            mode: PutMode::Create,
            tags: self.tags.clone(),
            attributes: self.attributes.clone(),
            ..Default::default()
        };

        match object_store.put_opts(&self.path, payload, opts).await {
            Ok(put_result) => self
                .data_version
                .lock()
                .map_err(Into::into)
                .map(|mut guard| {
                    _ = guard.replace(DataVersion {
                        data,
                        version: Some(UpdateVersion {
                            e_tag: put_result.e_tag,
                            version: put_result.version,
                        }),
                    });
                })
                .and(Ok(true)),

            Err(object_store::Error::AlreadyExists { .. }) => {
                self.refresh(object_store).await.and(Ok(false))
            }

            Err(otherwise) => Err(otherwise.into()),
        }
    }

    /// Delete the backing object (idempotent on `NotFound`) and clear the cache.
    #[instrument(skip_all, fields(path = %self.path))]
    pub(super) async fn remove(&self, object_store: &impl ObjectStore) -> Result<()> {
        REQUESTS.add(1, &[KeyValue::new("method", "remove")]);

        match object_store.delete(&self.path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => self
                .data_version
                .lock()
                .map_err(Into::into)
                .map(|mut guard| {
                    _ = guard.take();
                })
                .and(Ok(())),

            Err(otherwise) => Err(otherwise.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::{PutPayload, memory::InMemory};
    use serde::{Deserialize, Serialize};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::Error;

    use super::*;

    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X(i32);

    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(EnvFilter::from_default_env().add_directive(
                    format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
                ))
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[tokio::test]
    async fn with_does_not_exist() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        let o = OptiCon::path(path.clone());

        assert_eq!(1, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        assert!(matches!(
            object_store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        assert_eq!(1, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        assert!(matches!(
            object_store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        Ok(())
    }

    #[tokio::test]
    async fn with_mut_does_not_exist() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        let o = OptiCon::path(path.clone());

        let expected = 1;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        let expected = 2;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        Ok(())
    }

    #[tokio::test]
    async fn with_did_exist() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let o = OptiCon::path(path.clone());

        assert_eq!(7, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        object_store.delete(&path).await?;

        assert_eq!(1, o.with(&object_store, |x| Ok(x.0 + 1)).await?);

        assert!(matches!(
            object_store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        assert_eq!(1, o.with(&object_store, |x| Ok(x.0 + 1)).await?);

        assert!(matches!(
            object_store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        Ok(())
    }

    #[tokio::test]
    async fn with_mut_did_exist() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let o = OptiCon::path(path.clone());

        let expected = 7;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        object_store.delete(&path).await?;

        let expected = 1;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        let expected = 2;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        Ok(())
    }

    #[tokio::test]
    async fn with_already_exists() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let o = OptiCon::path(path.clone());

        assert_eq!(7, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(6, data.0);

        assert_eq!(7, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(6, data.0);

        Ok(())
    }

    #[tokio::test]
    async fn with_mut_already_exists() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let o = OptiCon::path(path.clone());

        assert_eq!(
            42,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;

                Ok(6 * x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(7, data.0);

        assert_eq!(
            48,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;

                Ok(6 * x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(8, data.0);

        Ok(())
    }
}
