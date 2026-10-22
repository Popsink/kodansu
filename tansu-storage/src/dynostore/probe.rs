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

//! The boot-time conditional-put probe (#597).
//!
//! Every stateless guarantee of the broker rests on two object-store
//! primitives: create-only (`If-None-Match: *`) for offset assignment, the
//! segment sequence and group assignment, and etag CAS (`If-Match`, a
//! generation precondition on GCS) for `update_group_generation` and the
//! maintenance leases. `tests/conditional_put.rs` pins them for the stores CI
//! runs. Any "S3-compatible" endpoint can be plugged into `s3://`, though, and
//! one that *ignores* either header does not error — it overwrites. `ping()`
//! only proves the store answers, so without this probe a non-conforming store
//! looks exactly like a conforming one until two writers race.
//!
//! Four PUTs and a DELETE on a key of this replica's own, at store build:
//!
//! 1. create-only — must succeed;
//! 2. create-only again — must fail with `AlreadyExists`;
//! 3. an update on a version the store never issued — must fail with
//!    `Precondition`;
//! 4. an update on the version step 1 returned — must succeed;
//! 5. delete the key.
//!
//! Step 2 or 3 succeeding is a store that silently overwrites, and the broker
//! refuses to start, naming the header. Any other error in steps 1–4 is a
//! refusal too, with that error in the message: `NotImplemented` or a `400`
//! means the store does not speak the header at all, which is no safer.

use super::*;

/// The storage-URL key that skips the probe (#597):
/// `?skip_conditional_put_check=true`.
pub(crate) const SKIP_CONDITIONAL_PUT_CHECK: &str = "skip_conditional_put_check";

/// Whether the storage URL asks for the probe to be skipped.
///
/// Only a value that parses as `true` skips it. An unparseable value warns and
/// probes, as every other key falls back to its default: a typo must not be
/// able to switch off the one check that stops a store from silently losing
/// writes.
pub(crate) fn skip_conditional_put_check(storage: &Url) -> bool {
    storage.query_pairs().any(|(key, value)| {
        key == SKIP_CONDITIONAL_PUT_CHECK
            && value
                .parse::<bool>()
                .inspect_err(|err| {
                    warn!(%storage, key = SKIP_CONDITIONAL_PUT_CHECK, value = value.as_ref(), ?err)
                })
                .unwrap_or(false)
    })
}

/// Which probe step an unexpected error came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    Create,
    SecondCreate,
    InventedVersion,
    IssuedVersion,
}

impl Step {
    fn describe(self) -> &'static str {
        match self {
            Self::Create => "create-only PUT (`If-None-Match: *`) of a new key",
            Self::SecondCreate => "second create-only PUT (`If-None-Match: *`) of the same key",
            Self::InventedVersion => {
                "PUT conditioned (`If-Match`) on a version the store never issued"
            }
            Self::IssuedVersion => "PUT conditioned (`If-Match`) on the version the store issued",
        }
    }
}

/// Why the probe refused the store.
#[derive(Debug)]
pub(crate) enum Refusal {
    /// Step 2 succeeded: the store overwrote an existing key on a create-only
    /// PUT.
    IfNoneMatchIgnored,

    /// Step 3 succeeded: the store overwrote a key on a PUT conditioned on a
    /// version it never issued.
    IfMatchIgnored,

    /// A step failed with an error other than the one a conforming store
    /// answers with.
    Failed {
        step: Step,
        error: object_store::Error,
    },
}

impl Refusal {
    /// The metric's `outcome` label.
    pub(crate) fn outcome(&self) -> &'static str {
        match self {
            Self::IfNoneMatchIgnored => "if_none_match_ignored",
            Self::IfMatchIgnored => "if_match_ignored",
            Self::Failed { .. } => "failed",
        }
    }

    /// The operator-facing reason: what the store did, why it matters, and
    /// what to do about it.
    pub(crate) fn reason(&self, scheme: &str) -> String {
        let hatch = format!(
            "use an object store that honours conditional writes, or — only if every \
             writer to this bucket is known to be serialised some other way — set \
             `?{SKIP_CONDITIONAL_PUT_CHECK}=true` on the storage URL"
        );

        match self {
            Self::IfNoneMatchIgnored => format!(
                "the {scheme}:// store accepted a second create-only PUT of the same key: it \
                 ignores `If-None-Match: *` and overwrites instead of answering 412, so two \
                 brokers can both win one offset and one of their batches is lost; {hatch}"
            ),

            Self::IfMatchIgnored => {
                let header = if scheme == "gs" {
                    "`x-goog-if-generation-match`"
                } else {
                    "`If-Match`"
                };

                format!(
                    "the {scheme}:// store accepted a PUT conditioned on a version it never \
                     issued: it ignores {header} and overwrites instead of answering 412, so \
                     consumer-group generations and maintenance leases lose updates under \
                     concurrency; {hatch}"
                )
            }

            Self::Failed { step, error } => format!(
                "the {scheme}:// store failed the conditional-put probe's {}: {error}; the \
                 broker cannot run on a store that does not implement conditional writes, \
                 so check the endpoint and credentials, then {hatch}",
                step.describe()
            ),
        }
    }
}

/// A version no store ever issued, in the shape the store under `scheme`
/// conditions on — the boot-time twin of `invented_version()` in
/// `tests/conditional_put.rs`.
///
/// An etag everywhere but GCS, which conditions on the *generation*: an
/// etag-only version there is refused by `object_store` before a request is
/// sent, which would measure the client rather than the store. `1` is a
/// generation GCS cannot have issued, since its generations are microsecond
/// timestamps.
pub(crate) fn invented_version(scheme: &str) -> UpdateVersion {
    UpdateVersion {
        e_tag: Some(String::from("\"0bad0bad0bad0bad0bad0bad0bad0bad\"")),
        version: (scheme == "gs").then(|| String::from("1")),
    }
}

/// Steps 1–4 against `location`, which must not exist.
///
/// Returns whether step 1 created the key — the caller deletes it if so,
/// whatever the outcome — alongside the verdict.
pub(crate) async fn probe(
    object_store: &dyn ObjectStore,
    location: &Path,
    invented: UpdateVersion,
) -> (bool, std::result::Result<(), Refusal>) {
    let put = |mode| async move {
        object_store
            .put_opts(
                location,
                PutPayload::from_static(b"{}"),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await
    };

    let issued = match put(PutMode::Create).await {
        Ok(put_result) => UpdateVersion {
            e_tag: put_result.e_tag,
            version: put_result.version,
        },
        Err(error) => {
            return (
                false,
                Err(Refusal::Failed {
                    step: Step::Create,
                    error,
                }),
            );
        }
    };

    let verdict = async {
        match put(PutMode::Create).await {
            Ok(_) => return Err(Refusal::IfNoneMatchIgnored),
            Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => {
                return Err(Refusal::Failed {
                    step: Step::SecondCreate,
                    error,
                });
            }
        }

        match put(PutMode::Update(invented)).await {
            Ok(_) => return Err(Refusal::IfMatchIgnored),
            Err(object_store::Error::Precondition { .. }) => {}
            Err(error) => {
                return Err(Refusal::Failed {
                    step: Step::InventedVersion,
                    error,
                });
            }
        }

        put(PutMode::Update(issued))
            .await
            .map(|_| ())
            .map_err(|error| Refusal::Failed {
                step: Step::IssuedVersion,
                error,
            })
    }
    .await;

    (true, verdict)
}

impl DynoStore {
    /// Probe the store's conditional writes (#597) and hand it back, or refuse
    /// to build one that ignores them with [`Error::ConditionalPutRefused`].
    ///
    /// Run by the `s3`, `gs` and `az`/`abfss` arms of the storage builder, and
    /// before [`Self::sealed_prefix_shape`] — the seal is itself a create-only
    /// PUT that replicas race for, so it is only meaningful on a store this
    /// probe has passed. `memory://` is never probed: `InMemory` is the
    /// reference the conformance target measures the others against.
    ///
    /// The key is `__probe/conditional-put/<writer id>-<uuid>`, outside every
    /// cluster's tree, so replicas starting together each probe their own key
    /// and nothing that scans a cluster ever sees one. The outcome is an INFO
    /// (or ERROR) line and a `tansu_conditional_put_probes` count either way, so a
    /// fleet can be audited for which replicas skipped it.
    ///
    /// With `?skip_conditional_put_check=true` the probe is skipped with a WARN
    /// on every start, so a fleet that opted out keeps saying so.
    pub async fn conditional_put_probed(self, storage: &Url) -> Result<Self> {
        let scheme = storage.scheme().to_owned();

        if skip_conditional_put_check(storage) {
            warn!(
                %scheme,
                key = SKIP_CONDITIONAL_PUT_CHECK,
                "skipping the conditional-put probe: a store that ignores If-None-Match or \
                 If-Match will overwrite instead of refusing, and offsets and group state \
                 will be lost under concurrency (#597)"
            );

            CONDITIONAL_PUT_PROBES.add(
                1,
                &[
                    KeyValue::new("scheme", scheme),
                    KeyValue::new("outcome", "skipped"),
                ],
            );

            return Ok(self);
        }

        let location = Path::from(format!(
            "__probe/conditional-put/{}-{}",
            self.identity.writer_id,
            Uuid::now_v7()
        ));

        let (created, verdict) = probe(
            self.object_store.as_ref(),
            &location,
            invented_version(&scheme),
        )
        .await;

        if created {
            _ = self.object_store.delete(&location).await.inspect_err(
                |err| warn!(%location, ?err, "could not delete the conditional-put probe key"),
            );
        }

        let outcome = verdict
            .as_ref()
            .map_or_else(Refusal::outcome, |()| "conforming");

        CONDITIONAL_PUT_PROBES.add(
            1,
            &[
                KeyValue::new("scheme", scheme.clone()),
                KeyValue::new("outcome", outcome),
            ],
        );

        match verdict {
            Ok(()) => {
                info!(%scheme, outcome, "the object store honours conditional writes");
                Ok(self)
            }

            Err(refusal) => {
                let reason = refusal.reason(&scheme);
                error!(%scheme, outcome, reason, "refusing to start");
                Err(Error::ConditionalPutRefused { scheme, reason })
            }
        }
    }
}
