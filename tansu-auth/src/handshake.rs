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
use tansu_sans_io::{ApiKey, ErrorCode};
use tansu_sans_io::{SaslHandshakeRequest, SaslHandshakeResponse};

use crate::Authentication;
use crate::Error;
use crate::Stage;

/// Refuses the mechanism a client asked for, naming it back.
fn unsupported(mechanism: String) -> SaslHandshakeResponse {
    let refusal = SaslHandshakeResponse::default();
    let refusal = refusal.error_code(i16::from(ErrorCode::UnsupportedSaslMechanism));
    refusal.mechanisms(Some(vec![mechanism]))
}

/// Starts a SASL session on the connection for the mechanism a client names,
/// refusing it when the connection has no authentication or the mechanism is
/// not one the server offers.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SaslHandshakeService;

impl ApiKey for SaslHandshakeService {
    const KEY: i16 = SaslHandshakeRequest::KEY;
}

impl<S> Service<S, SaslHandshakeRequest> for SaslHandshakeService
where
    S: Send + Sync + 'static,
{
    type Response = SaslHandshakeResponse;
    type Error = Error;

    #[tracing::instrument(skip(self, ctx), ret)]
    async fn serve(
        &self,
        ctx: Context<S>,
        req: SaslHandshakeRequest,
    ) -> Result<Self::Response, Self::Error> {
        let Some(authentication) = ctx.get::<Authentication>().cloned() else {
            return Ok(unsupported(req.mechanism));
        };
        let mut stage = authentication.stage.lock()?;

        // A Java client re-authenticates on the same socket (KIP-368) by
        // sending another SaslHandshake. The last exchange left a `Session` or
        // `Finished` behind, not a `Server`, and taking that as it is would
        // refuse a mechanism the server does offer — so anything but a fresh
        // server is replaced by one built from the stored config.
        if !matches!(stage.as_ref(), Some(Stage::Server(_))) {
            tracing::debug!(guard = ?stage);
            _ = stage.replace(authentication.fresh_server());
        }

        if let Some(Stage::Server(server)) = stage.take()
            && let Ok(mechanism) = rsasl::prelude::Mechname::parse(req.mechanism.as_bytes())
        {
            let offered = server.get_available().into_iter();
            let offered: Vec<&str> = offered.map(|each| each.mechanism.as_str()).collect();
            tracing::debug!(available = ?offered);

            let starting = server.start_suggested(mechanism);
            let session = starting.inspect_err(|err| tracing::debug!(?err, ?mechanism))?;
            let chosen = session.get_mechname().to_string();
            _ = stage.replace(Stage::Session(session));

            let started = SaslHandshakeResponse::default().error_code(i16::from(ErrorCode::None));
            Ok(started.mechanisms(Some(vec![chosen])))
        } else {
            Ok(unsupported(req.mechanism))
        }
    }
}
