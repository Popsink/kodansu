// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! Cluster-wide principal state: the ACL object (#363), the client-quota
//! object (#384) and the per-principal SCRAM credentials.

use super::*;

impl DynoStore {
    /// Every ACL in the cluster, as one object (#363).
    ///
    /// One object rather than one per rule because of how it is read: the
    /// request path needs all of them to answer any question, so a per-rule
    /// keyspace would put a LIST on the authorization path — the one place
    /// that can least afford one.
    pub(super) fn acls_location(&self) -> Path {
        Path::from(format!("clusters/{}/acls.json", self.identity.cluster))
    }

    /// Every client quota in the cluster, as one object (#384).
    ///
    /// The same choice as `acls.json` above and for the same reason: the
    /// request path holds a snapshot of all of them to answer any question, so
    /// a key per entity would put a LIST behind the refresh of the one cache
    /// that has to be cheap.
    pub(super) fn quotas_location(&self) -> Path {
        Path::from(format!("clusters/{}/quotas.json", self.identity.cluster))
    }

    /// One object per principal per mechanism.
    ///
    /// The opposite choice to `acls.json` above, and for the opposite reason:
    /// the ACLs are read all-at-once to answer any question, whereas a handshake
    /// knows exactly whose credential it wants and never needs another's. One
    /// key per user means the handshake is a GET of a known key rather than a
    /// LIST, and two administrators changing two passwords do not contend.
    ///
    /// The mechanism is in the key because SCRAM-SHA-256 and SCRAM-SHA-512
    /// derive different keys from the same password: they are two credentials
    /// for one user, and a client picks which to present.
    pub(super) fn user_scram_credential_location(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Path {
        // `Path::from` percent-encodes what it must, so a user name with a
        // slash in it stays one path segment rather than silently becoming two.
        Path::from(format!(
            "clusters/{}/users/{user}/{}.json",
            self.identity.cluster,
            match mechanism {
                ScramMechanism::Scram256 => "scram-sha-256",
                ScramMechanism::Scram512 => "scram-sha-512",
            }
        ))
    }

    /// The cluster's ACLs and the version to CAS the next write against.
    ///
    /// A cluster that has never had an ACL applied has no object, which reads
    /// as an empty set rather than as an error: "no rules" is a state, and on a
    /// fail-closed broker it is the *most* consequential one, so it must not
    /// depend on somebody having written the object first.
    pub(super) async fn read_acls(&self) -> Result<(Acls, Option<Version>)> {
        Ok(
            match Self::absent_is_none(self.get::<Acls>(&self.acls_location()).await)? {
                Some((acls, version)) => (acls, Some(version)),
                None => (Acls::default(), None),
            },
        )
    }

    /// Read-modify-CAS the ACL object.
    ///
    /// `apply` is re-run from scratch on every lost race, against the document
    /// that won — never replayed onto the one that lost. Two operators
    /// applying different rules at the same moment both land; the alternative,
    /// last-writer-wins, silently drops one of them.
    pub(super) async fn update_acls<F>(&self, mut apply: F) -> Result<()>
    where
        F: FnMut(&mut Acls),
    {
        /// Generous: ACL writes are administrative and rare, so a conflict
        /// means two operators at once rather than sustained contention, and
        /// giving up on one is worse than trying again.
        const ATTEMPTS: u32 = 16;

        for attempt in 0..ATTEMPTS {
            let (mut acls, version) = self.read_acls().await?;

            apply(&mut acls);

            match self
                .put(
                    &self.acls_location(),
                    acls,
                    json_content_type(),
                    version.map(Into::into),
                )
                .await
            {
                Ok(_) => return Ok(()),

                // `Vanished` is the same instruction as `Outdated` here and for
                // the same reason: this is a read-modify-write loop, so the next
                // attempt re-reads. It finds the object absent, `version` is
                // `None`, and the put becomes a `PutMode::Create` — which is
                // exactly what re-applying onto "there is no value" means (#431).
                Err(UpdateError::Outdated { .. } | UpdateError::Vanished) => {
                    debug!(
                        attempt,
                        cluster = self.identity.cluster,
                        "acl update lost the CAS"
                    );
                    sleep(Duration::from_millis(5 * u64::from(1 + attempt))).await;
                }

                Err(UpdateError::Error(error)) => return Err(error),
                Err(UpdateError::SerdeJson(error)) => return Err(Error::SerdeJson(error)),
                Err(UpdateError::Uuid(error)) => return Err(Error::Uuid(error)),
                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from(
                        "acl update reported a missing etag",
                    )));
                }
            }
        }

        Err(Error::Message(format!(
            "could not write the acls of cluster {} in {ATTEMPTS} attempts",
            self.identity.cluster,
        )))
    }

    /// The cluster's quotas and the version to CAS the next write against.
    ///
    /// A cluster that has never had a quota applied has no object, which reads
    /// as no quotas rather than as an error — and on a broker that fails open,
    /// "no quotas" has to be a state a fresh cluster can be in without anybody
    /// having written the object first.
    pub(super) async fn read_quotas(&self) -> Result<(Quotas, Option<Version>)> {
        Ok(
            match Self::absent_is_none(self.get::<Quotas>(&self.quotas_location()).await)? {
                Some((quotas, version)) => (quotas, Some(version)),
                None => (Quotas::default(), None),
            },
        )
    }

    /// Read-modify-CAS the quota object.
    ///
    /// `apply` is re-run from scratch against the document that won every lost
    /// race, never replayed onto the one that lost — the reasoning is
    /// [`Self::update_acls`]'s, and so is the attempt count: quota writes are
    /// administrative and rare, so a conflict means two operators at once.
    pub(super) async fn update_quotas<F>(&self, mut apply: F) -> Result<()>
    where
        F: FnMut(&mut Quotas),
    {
        const ATTEMPTS: u32 = 16;

        for attempt in 0..ATTEMPTS {
            let (mut quotas, version) = self.read_quotas().await?;

            apply(&mut quotas);

            match self
                .put(
                    &self.quotas_location(),
                    quotas,
                    json_content_type(),
                    version.map(Into::into),
                )
                .await
            {
                Ok(_) => return Ok(()),

                // `Vanished` is the same instruction as `Outdated` here and for
                // the same reason: this is a read-modify-write loop, so the next
                // attempt re-reads. It finds the object absent, `version` is
                // `None`, and the put becomes a `PutMode::Create` — which is
                // exactly what re-applying onto "there is no value" means (#431).
                Err(UpdateError::Outdated { .. } | UpdateError::Vanished) => {
                    debug!(
                        attempt,
                        cluster = self.identity.cluster,
                        "quota update lost the CAS"
                    );
                    sleep(Duration::from_millis(5 * u64::from(1 + attempt))).await;
                }

                Err(UpdateError::Error(error)) => return Err(error),
                Err(UpdateError::SerdeJson(error)) => return Err(Error::SerdeJson(error)),
                Err(UpdateError::Uuid(error)) => return Err(Error::Uuid(error)),
                Err(UpdateError::MissingEtag) => {
                    return Err(Error::Message(String::from(
                        "quota update reported a missing etag",
                    )));
                }
            }
        }

        Err(Error::Message(format!(
            "could not write the client quotas of cluster {} in {ATTEMPTS} attempts",
            self.identity.cluster,
        )))
    }
}
