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

//! What every group service shares: its type, its [`ApiKey`], its
//! [`Service`] signature, the frame it answers with, and how a refusal the
//! coordinator chose becomes that answer.
//!
//! [`ApiKey`]: tansu_sans_io::ApiKey
//! [`Service`]: rama::Service

use tansu_sans_io::{Body, ErrorCode, Frame, Header};

use crate::{Error, Result};

/// The response to the request `correlation_id` names, carrying `body`.
pub(super) fn respond(correlation_id: i32, body: Body) -> Frame {
    Frame {
        size: 0,
        header: Header::Response { correlation_id },
        body,
    }
}

/// `outcome`, with an [`Error::Api`] answered as `refused` answers that code.
///
/// An `Error::Api` is an answer the broker chose, not a failure. Answering it
/// here is what stops it from ending the connection with no response written —
/// `early eof` to the caller (#300). Anything else is still an error. See
/// [`super::answer`].
pub(super) fn answered(
    outcome: Result<Body>,
    refused: impl FnOnce(ErrorCode) -> Body,
) -> Result<Body> {
    outcome.or_else(|error| match error {
        Error::Api(error_code) => Ok(refused(error_code)),
        otherwise => Err(otherwise),
    })
}

/// A group service `$name` for `$request`, whose `serve` is `$body` with the
/// coordinator context bound to `$ctx` and the request frame to `$frame`.
macro_rules! group_service {
    (
        $(#[$doc:meta])*
        $name:ident: $request:ty,
        |$ctx:ident, $frame:ident| $body:expr
    ) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name;

        impl tansu_sans_io::ApiKey for $name {
            const KEY: i16 = <$request as tansu_sans_io::ApiKey>::KEY;
        }

        impl<C: crate::coordinator::group::Coordinator> rama::Service<C, tansu_sans_io::Frame>
            for $name
        {
            type Response = tansu_sans_io::Frame;
            type Error = crate::Error;

            #[tracing::instrument(skip($ctx, $frame))]
            async fn serve(
                &self,
                mut $ctx: rama::Context<C>,
                $frame: tansu_sans_io::Frame,
            ) -> crate::Result<tansu_sans_io::Frame> {
                $body
            }
        }
    };
}
