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

use std::fmt::{self, Debug, Formatter};
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};

use rsasl::callback::{Context, Request, SessionCallback, SessionData};
use rsasl::config::SASLConfig;
use rsasl::mechanisms::scram::properties::ScramStoredPassword;
use rsasl::prelude::{SASLError, SASLServer, Session, SessionError, Validation};
use rsasl::property::{AuthId, AuthzId, Password};
use rsasl::validate::{Validate, ValidationError};
use tansu_sans_io::ScramMechanism;
use tansu_storage::Storage;
use thiserror::Error;
use tokio::task::JoinError;
use tracing::debug;

mod authenticate;
mod handshake;

pub use self::authenticate::SaslAuthenticateService;
pub use self::handshake::SaslHandshakeService;

#[derive(Clone, Debug, Error)]
pub enum Error {
    Join(Arc<JoinError>),
    Poison,
    SansIo(#[from] tansu_sans_io::Error),
    Sasl(Arc<SASLError>),
    SaslSession(Arc<SessionError>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{self:?}"))
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        let shared = Arc::new(value);
        Error::Join(shared)
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Error::Poison
    }
}

impl From<SASLError> for Error {
    fn from(value: SASLError) -> Self {
        let shared = Arc::new(value);
        Error::Sasl(shared)
    }
}

impl From<SessionError> for Error {
    fn from(value: SessionError) -> Self {
        let shared = Arc::new(value);
        Error::SaslSession(shared)
    }
}

#[derive(Clone)]
pub struct Authentication {
    config: Arc<SASLConfig>,
    stage: Arc<Mutex<Option<Stage>>>,
}

pub enum Stage {
    Server(SASLServer<Justification>),
    Session(Session<Justification>),
    Finished(Option<Success>),
}

impl Debug for Stage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("Stage")
    }
}

impl Authentication {
    pub fn server(config: Arc<SASLConfig>) -> Self {
        let first = SASLServer::<Justification>::new(Arc::clone(&config));
        let stage = Arc::new(Mutex::new(Some(Stage::Server(first))));
        Authentication { config, stage }
    }

    /// Who authenticated on this connection, once one has (#363).
    ///
    /// Captured all along — `Stage::Finished` has held the SASL authentication
    /// id since authentication was added — and never exposed, because nothing
    /// downstream had a use for it: the broker asked *whether* a client had
    /// authenticated and never *who* it was. Authorization is the first thing
    /// that needs the second question answered.
    ///
    /// `None` on a connection that has not authenticated, which on a broker
    /// started without `--authentication` is every connection.
    pub fn principal(&self) -> Option<String> {
        let guard = self.stage.lock().ok()?;
        let Some(Stage::Finished(Some(success))) = guard.as_ref() else {
            return None;
        };
        Some(String::from(success.auth_id()))
    }

    pub fn is_authenticated(&self) -> bool {
        let Ok(guard) = self.stage.lock() else {
            return false;
        };
        matches!(guard.as_ref(), Some(Stage::Finished(_)))
    }

    /// A new `Stage::Server` over the same config, for a connection that
    /// authenticates again (KIP-368).
    ///
    /// A client re-authenticates by sending another `SaslHandshake` on a
    /// connection that already succeeded once; the handshake handler starts
    /// the exchange over from this rather than refusing it.
    pub fn fresh_server(&self) -> Stage {
        let restarted = SASLServer::<Justification>::new(Arc::clone(&self.config));
        Stage::Server(restarted)
    }
}

impl Debug for Authentication {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("Authentication")
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
    Bad,
    Io(tansu_sans_io::Error),
    MissingProperty { mechanism: String, property: String },
    NoSuchUser,
    UnknownMechanism(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{self:?}"))
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Success {
    auth_id: String,
}

impl Success {
    /// The authenticated user: the SASL authentication id, which is what a
    /// Kafka principal is built from (#363).
    pub fn auth_id(&self) -> &str {
        let Success { auth_id } = self;
        auth_id
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Justification;

impl Validation for Justification {
    type Value = Result<Success, AuthError>;
}

#[derive(Clone, Debug)]
pub struct Callback<S> {
    storage: S,
}

impl<S> Callback<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self
    where
        S: Storage,
    {
        Callback { storage }
    }

    #[tracing::instrument(skip_all)]
    fn check(
        &self,
        session_data: &SessionData,
        context: &Context<'_>,
    ) -> Result<Result<Success, AuthError>, Error> {
        let mechanism = session_data.mechanism().mechanism;
        debug!(mechanism = %mechanism);
        let missing = |property: &str| AuthError::MissingProperty {
            mechanism: mechanism.to_string(),
            property: property.into(),
        };

        if mechanism == "PLAIN" {
            let password = context.get_ref::<Password>();
            let password = password.ok_or_else(|| missing("Password"));
            let named = context.get_ref::<AuthId>();
            if let Some(auth_id) = named {
                debug!(mechanism = %mechanism, auth_id);
            }
            let success = named.map(|auth_id| Success {
                auth_id: auth_id.to_string(),
            });
            return Ok(password.and(success.ok_or_else(|| missing("AuthId"))));
        }

        if !mechanism.starts_with("SCRAM-") {
            return Ok(Err(AuthError::UnknownMechanism(mechanism.to_string())));
        }

        let Some(auth_id) = context.get_ref::<AuthId>() else {
            return Ok(Err(missing("AuthId")));
        };
        debug!(mechanism = %mechanism, auth_id);
        if let Some(authz_id) = context.get_ref::<AuthzId>() {
            debug!(mechanism = %mechanism, authz_id);
            if authz_id != auth_id {
                return Ok(Err(AuthError::Bad));
            }
        }
        Ok(Ok(Success {
            auth_id: auth_id.to_string(),
        }))
    }
}

impl<S> SessionCallback for Callback<S>
where
    S: Storage,
{
    #[tracing::instrument(skip_all)]
    fn callback(
        &self,
        session_data: &SessionData,
        context: &Context<'_>,
        request: &mut Request<'_>,
    ) -> Result<(), SessionError> {
        debug!(session_data = ?session_data);

        let name = session_data.mechanism().mechanism;
        if !name.starts_with("SCRAM-") {
            return Ok(());
        }

        let mechanism =
            ScramMechanism::from_str(name).map_err(|error| SessionError::Boxed(Box::new(error)))?;
        let unnamed = SessionError::ValidationError(ValidationError::MissingRequiredProperty);
        let auth_id = context.get_ref::<AuthId>().ok_or(unnamed)?;
        debug!(auth_id = ?auth_id, mechanism = ?mechanism);

        let mut builder = tokio::runtime::Builder::new_current_thread();
        let runtime = builder.enable_all().build()?;
        let stored = runtime.block_on(self.storage.user_scram_credential(auth_id, mechanism));
        let logged = stored.inspect_err(|err| debug!(auth_id, mechanism = ?mechanism, err = ?err));
        let Ok(Some(credential)) = logged else {
            return Ok(());
        };

        let iterations = credential.iterations as u32;
        let salt = credential.salt.as_ref();
        let stored_key = credential.stored_key.as_ref();
        let server_key = credential.server_key.as_ref();
        let password = ScramStoredPassword::new(iterations, salt, stored_key, server_key);
        let satisfied = request.satisfy::<ScramStoredPassword<'_>>(&password);
        _ = satisfied.inspect_err(|err| debug!(auth_id, mechanism = ?mechanism, err = ?err))?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn validate(
        &self,
        session_data: &SessionData,
        context: &Context<'_>,
        validate: &mut Validate<'_>,
    ) -> Result<(), ValidationError> {
        debug!(session_data = ?session_data);
        let checked = || {
            let outcome = Self::check(self, session_data, context);
            outcome.map_err(|error| ValidationError::Boxed(Box::new(error)))
        };
        _ = validate.with::<Justification, _>(checked)?;
        Ok(())
    }
}

pub fn configuration<S>(storage: S) -> Result<Arc<SASLConfig>, Error>
where
    S: Storage,
{
    let callback = Callback::new(storage);
    let defaults = SASLConfig::builder().with_defaults();
    let config = defaults.with_callback(callback)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::Authentication;

    fn shareable<T: Send + Sync>() {}

    /// A connection's authentication state is handed between tasks.
    #[test]
    fn an_authentication_is_send_and_sync() {
        shareable::<Authentication>();
    }
}
