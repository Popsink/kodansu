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

//! The shape every storage service shares, written once.
//!
//! A storage service is a unit struct that answers one Kafka API by calling
//! into the [`Storage`](crate::Storage) it finds as its [`rama::Context`]
//! state. Everything around that call — the struct, its derives, the
//! [`ApiKey`](tansu_sans_io::ApiKey) it routes on and the [`rama::Service`]
//! impl — is the same for every API, so [`storage_service!`] generates it and
//! a service file holds only what is particular to its API.

/// Defines a unit struct `$name` that serves `$request` with `$response`,
/// running `$body` with the storage context bound to `$ctx` and the request to
/// `$req`.
///
/// `serve` carries `#[instrument]` with both arguments skipped: the span names
/// the service, and neither the context nor the request has a useful `Debug`
/// at that level.
/// The `throttle_time_ms` of a response that was not held back, for the APIs
/// that made the field optional: no quota delays a request here.
pub(crate) const UNTHROTTLED: Option<i32> = Some(0);

macro_rules! storage_service {
    (
        $(#[$doc:meta])*
        $name:ident: $request:ty => $response:ty,
        |$ctx:ident, $req:ident| $body:expr
    ) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name;

        impl tansu_sans_io::ApiKey for $name {
            const KEY: i16 = <$request as tansu_sans_io::ApiKey>::KEY;
        }

        impl<G: crate::Storage> rama::Service<G, $request> for $name {
            type Response = $response;
            type Error = crate::Error;

            #[tracing::instrument(skip($ctx, $req))]
            async fn serve(
                &self,
                $ctx: rama::Context<G>,
                $req: $request,
            ) -> crate::Result<$response> {
                $body
            }
        }
    };
}
