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

use rama::Context;
use rama::Service;
use rsasl::mechanisms::scram::tools as scram;
use sha2::Digest as _;
use sha2::{Sha256, Sha512};
use tansu_sans_io::ScramMechanism;
use tansu_sans_io::alter_user_scram_credentials_response::AlterUserScramCredentialsResult;
use tansu_sans_io::{AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse};
use tansu_sans_io::{ApiKey, ErrorCode};

use crate::ScramCredential;
use crate::Storage;
use crate::{Error, Result};

/// The stored and server keys SCRAM keeps in place of a salted password.
type DerivedKeys = (bytes::Bytes, bytes::Bytes);

/// Derives [`DerivedKeys`] from `salted` under the digest `mechanism` names.
fn derived(mechanism: ScramMechanism, salted: &[u8]) -> DerivedKeys {
    let (stored, server) = match mechanism {
        ScramMechanism::Scram256 => {
            let (client, server) = scram::derive_keys::<Sha256>(salted);
            (Sha256::digest(client).to_vec(), server.to_vec())
        }
        ScramMechanism::Scram512 => {
            let (client, server) = scram::derive_keys::<Sha512>(salted);
            (Sha512::digest(client).to_vec(), server.to_vec())
        }
    };
    (stored.into(), server.into())
}

/// One user's answer: [`ErrorCode::None`] when the engine accepted the change,
/// [`ErrorCode::UnsupportedSaslMechanism`] otherwise, and an empty message either way.
fn outcome(user: String, accepted: bool) -> AlterUserScramCredentialsResult {
    let code = if accepted {
        ErrorCode::None
    } else {
        ErrorCode::UnsupportedSaslMechanism
    };
    let result = AlterUserScramCredentialsResult::default().user(user);
    result
        .error_code(i16::from(code))
        .error_message(Some(String::new()))
}

/// Deletes, then upserts, the SCRAM credentials a request names, answering one
/// result per user in that order.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AlterUserScramCredentialsService;

impl ApiKey for AlterUserScramCredentialsService {
    const KEY: i16 = AlterUserScramCredentialsRequest::KEY;
}

impl<G> Service<G, AlterUserScramCredentialsRequest> for AlterUserScramCredentialsService
where
    G: Storage,
{
    type Response = AlterUserScramCredentialsResponse;
    type Error = Error;

    #[tracing::instrument(skip(state, request))]
    async fn serve(
        &self,
        state: Context<G>,
        request: AlterUserScramCredentialsRequest,
    ) -> Result<Self::Response, Self::Error> {
        let engine = state.state();
        let mut answers = Vec::new();

        for removal in request.deletions.into_iter().flatten() {
            let mechanism = ScramMechanism::try_from(removal.mechanism)?;
            let deleting = engine.delete_user_scram_credential(&removal.name, mechanism);
            let accepted = deleting.await.is_ok();
            answers.push(outcome(removal.name, accepted));
        }

        for change in request.upsertions.into_iter().flatten() {
            let mechanism = ScramMechanism::try_from(change.mechanism)?;
            tracing::debug!(?mechanism);
            let (stored, server) = derived(mechanism, &change.salted_password);
            let replacement = ScramCredential {
                salt: change.salt,
                iterations: change.iterations,
                stored_key: stored,
                server_key: server,
            };
            let upserting =
                engine.upsert_user_scram_credential(&change.name, mechanism, replacement);
            let accepted = upserting.await.is_ok();
            answers.push(outcome(change.name, accepted));
        }

        let unthrottled = AlterUserScramCredentialsResponse::default().throttle_time_ms(0);
        Ok(unthrottled.results(Some(answers)))
    }
}

#[cfg(all(feature = "dynostore", test))]
mod tests {
    use rama::Context;
    use rama::Layer as _;
    use rama::Service as _;
    use rama::layer::MapStateLayer;
    use sha2::Digest as _;
    use tansu_sans_io::alter_user_scram_credentials_request::ScramCredentialDeletion;
    use tansu_sans_io::alter_user_scram_credentials_request::ScramCredentialUpsertion;
    use tansu_sans_io::{AlterUserScramCredentialsRequest, ErrorCode, ScramMechanism};

    use super::AlterUserScramCredentialsService;
    use crate::{Error, Storage as _, StorageContainer};

    #[tokio::test]
    async fn an_upsert_stores_the_derived_keys_and_a_deletion_removes_them() -> Result<(), Error> {
        let listener = url::Url::parse("tcp://localhost:9092")?;
        let engine = StorageContainer::builder().cluster_id("tansu").node_id(111);
        let engine = engine.advertised_listener(listener);
        let memory = url::Url::parse("memory://tansu/")?;
        let engine = engine.storage(memory).build().await?;
        let shared = engine.clone();
        let layer = MapStateLayer::new(move |_| shared.clone());
        let credentials = layer.into_layer(AlterUserScramCredentialsService);

        let salted = bytes::Bytes::from_static(b"0123456789abcdef0123456789abcdef");
        let change = ScramCredentialUpsertion::default().name("alice".into());
        let change = change
            .mechanism(ScramMechanism::Scram512.into())
            .iterations(4096);
        let change = change.salt(bytes::Bytes::from_static(b"salt"));
        let change = change.salted_password(salted.clone());
        let upsert = AlterUserScramCredentialsRequest::default().upsertions(Some(vec![change]));
        let answered = credentials.serve(Context::default(), upsert).await?;
        let codes: Vec<_> = answered.results.unwrap_or_default();
        let codes: Vec<_> = codes
            .iter()
            .map(|r| (r.user.as_str(), r.error_code))
            .collect();
        assert_eq!(vec![("alice", i16::from(ErrorCode::None))], codes);

        let derive = rsasl::mechanisms::scram::tools::derive_keys::<sha2::Sha512>;
        let (client, server) = derive(&salted);
        let reading = engine.user_scram_credential("alice", ScramMechanism::Scram512);
        let stored = reading.await?.expect("alice was just upserted");
        assert_eq!((&b"salt"[..], 4096), (&stored.salt[..], stored.iterations));
        assert_eq!(&sha2::Sha512::digest(client)[..], &stored.stored_key[..]);
        assert_eq!(&server[..], &stored.server_key[..]);

        let removal = ScramCredentialDeletion::default().name("alice".into());
        let removal = removal.mechanism(ScramMechanism::Scram512.into());
        let delete = AlterUserScramCredentialsRequest::default().deletions(Some(vec![removal]));
        let _answered = credentials.serve(Context::default(), delete).await?;
        let reading = engine.user_scram_credential("alice", ScramMechanism::Scram512);
        assert!(reading.await?.is_none());
        Ok(())
    }
}
