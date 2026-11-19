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

use bytes::Bytes as Octets;
use rama::Context;
use rama::Service;
use tansu_sans_io::{ApiKey, ErrorCode};
use tansu_sans_io::{SaslAuthenticateRequest, SaslAuthenticateResponse};

use crate::Authentication;
use crate::Error;
use crate::Stage;

/// Ends the exchange with `code`: its name as the message, no bytes, and no
/// session lifetime.
fn ended(code: ErrorCode) -> SaslAuthenticateResponse {
    let ending = SaslAuthenticateResponse::default().error_code(i16::from(code));
    let ending = ending.error_message(Some(code.to_string()));
    ending
        .auth_bytes(Octets::new())
        .session_lifetime_ms(Some(0))
}

/// Feeds `offered` to the connection's SASL session, recording a finished
/// session's outcome; a missing session or a failed step ends the exchange.
fn step(
    authentication: &Authentication,
    offered: &Octets,
    lifetime: Option<i64>,
) -> Result<SaslAuthenticateResponse, Error> {
    let mut stage = authentication.stage.lock()?;
    let Some(Stage::Session(session)) = stage.as_mut() else {
        _ = stage.take();
        return Ok(ended(ErrorCode::IllegalSaslState));
    };

    let mut outcome = std::io::Cursor::new(Vec::new());
    let stepped = session.step(Some(offered), &mut outcome);
    let stepped = stepped.inspect(|state| tracing::debug!(?state));
    let Ok(state) = stepped.inspect_err(|err| tracing::debug!(?err)) else {
        _ = stage.take();
        return Ok(ended(ErrorCode::SaslAuthenticationFailed));
    };

    let validated = session.validation().transpose().ok().flatten();
    let success = validated.inspect(|success| tracing::debug!(?success));
    if matches!(state, rsasl::prelude::State::Finished(_)) {
        _ = stage.replace(Stage::Finished(success));
    }

    let accepted = SaslAuthenticateResponse::default().error_code(i16::from(ErrorCode::None));
    let accepted = accepted.error_message(Some(String::from("NONE")));
    let accepted = accepted.auth_bytes(Octets::from(outcome.into_inner()));
    Ok(accepted.session_lifetime_ms(lifetime))
}

/// Steps the connection's SASL session with each `SaslAuthenticate`, off the
/// async runtime, answering with the server's bytes and the session lifetime.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SaslAuthenticateService {
    session_lifetime_ms: Option<i64>,
}

impl Default for SaslAuthenticateService {
    fn default() -> Self {
        Self {
            session_lifetime_ms: Some(60_000),
        }
    }
}

impl SaslAuthenticateService {
    pub fn session_lifetime_ms(self, session_lifetime_ms: Option<i64>) -> Self {
        Self {
            session_lifetime_ms,
        }
    }
}

impl ApiKey for SaslAuthenticateService {
    const KEY: i16 = SaslAuthenticateRequest::KEY;
}

impl<S> Service<S, SaslAuthenticateRequest> for SaslAuthenticateService
where
    S: Send + Sync + 'static,
{
    type Response = SaslAuthenticateResponse;
    type Error = Error;

    async fn serve(
        &self,
        ctx: Context<S>,
        req: SaslAuthenticateRequest,
    ) -> Result<Self::Response, Self::Error> {
        let Some(authentication) = ctx.get::<Authentication>().cloned() else {
            return Ok(ended(ErrorCode::IllegalSaslState));
        };
        let lifetime = self.session_lifetime_ms;
        let blocking = move || step(&authentication, &req.auth_bytes, lifetime);
        tokio::task::spawn_blocking(blocking).await?
    }
}
