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

//! The one place a storage URL's scheme names a backend (#531).

use url::Url;

use crate::{Error, Result};

/// The object-store backend a storage URL names.
///
/// Three places turn a storage URL into a store, and each grew its own `match`
/// on `url.scheme()`: the broker's [`crate::StorageContainer::builder`], the
/// offline audit ([`crate::Audit`]) and the `conditional_put` conformance
/// target. #418 added the Azure schemes to the first only, so the audit refused
/// `abfss://` outright (#531) and the conformance target had already sprung the
/// same trap in #420, reporting "conditional put is not a property of" a scheme
/// it simply did not know.
///
/// The *routing* is shared here; the construction deliberately is not. The
/// three builders legitimately differ — the broker's arms carry retry budgets,
/// a put rate limiter and the `DynoStore` wrapping, and the audit is read-only
/// — and collapsing them would mean one of them silently inheriting tuning
/// nobody chose for it. What this buys instead is that every caller matches
/// **exhaustively**, so the next backend is a compile error in the builders
/// that have not handled it rather than an `UnsupportedStorageUrl` from the one
/// nobody remembered.
///
/// Which backends a caller *accepts* stays the caller's: [`Backend::Local`] is
/// the audit's alone — a deployment already past the damage cannot be measured
/// by starting a broker on a copy — and [`Backend::Null`] is the broker's.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Backend {
    /// `s3://bucket/` — S3, or anything speaking it (MinIO).
    S3,

    /// `gs://bucket/` — Google Cloud Storage.
    Google,

    /// `abfss://container@account.dfs.core.windows.net/`, `abfs://` or
    /// `az://container/` — Azure Data Lake Storage Gen2 (#418).
    Azure,

    /// `memory://` — in process, for tests and ephemera.
    Memory,

    /// `file:///path/to/copy` — a local directory, which is what the offline
    /// audit exists for.
    Local,

    /// `null://` — the engine that stores nothing.
    Null,
}

impl Backend {
    /// The backend a storage URL's scheme names, or
    /// [`Error::UnsupportedStorageUrl`].
    ///
    /// The Azure-adjacent aliases `object_store`'s own parser understands —
    /// `wasbs://`, `adl://`, `azure://`, a raw
    /// `https://<account>.blob.core.windows.net/` — are deliberately absent.
    /// Every extra spelling is one a deployment can drift onto, and `wasbs://`
    /// in particular is the legacy Blob scheme: it must fail loudly rather than
    /// be quietly treated as Gen2 (#418).
    pub fn try_from_url(url: &Url) -> Result<Self> {
        match url.scheme() {
            "s3" => Ok(Self::S3),
            "gs" => Ok(Self::Google),
            "abfss" | "abfs" | "az" => Ok(Self::Azure),
            "memory" => Ok(Self::Memory),
            "file" => Ok(Self::Local),
            "null" => Ok(Self::Null),

            _unsupported => Err(Error::UnsupportedStorageUrl(url.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_scheme_a_builder_accepts_routes() -> Result<()> {
        for (url, expected) in [
            ("s3://tansu/", Backend::S3),
            ("gs://tansu/", Backend::Google),
            ("abfss://tansu@acct.dfs.core.windows.net/", Backend::Azure),
            ("abfs://tansu@acct.dfs.core.windows.net/", Backend::Azure),
            ("az://tansu/", Backend::Azure),
            ("memory://tansu/", Backend::Memory),
            ("file:///tmp/copy", Backend::Local),
            ("null://tansu/", Backend::Null),
        ] {
            assert_eq!(expected, Backend::try_from_url(&Url::parse(url)?)?, "{url}");
        }

        Ok(())
    }

    /// #418: the Azure-adjacent spellings `object_store` understands and we do
    /// not, plus the shapes that have never been storage URLs.
    #[test]
    fn an_unknown_scheme_is_unsupported() -> Result<()> {
        for url in [
            "wasbs://tansu@acct.blob.core.windows.net/",
            "wasb://tansu@acct.blob.core.windows.net/",
            "adl://tansu/",
            "azure://tansu/",
            "https://acct.blob.core.windows.net/tansu",
            "postgres://localhost/tansu",
        ] {
            assert!(
                matches!(
                    Backend::try_from_url(&Url::parse(url)?),
                    Err(Error::UnsupportedStorageUrl(_))
                ),
                "{url} must not name a backend",
            );
        }

        Ok(())
    }
}
