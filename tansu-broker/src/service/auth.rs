// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use rama::Layer as _;
use rama::Service as _;
use rama::layer::MapErrLayer;
use tansu_auth::SaslAuthenticateService;
use tansu_auth::SaslHandshakeService;
use tansu_sans_io::ApiKey as _;
use tansu_sans_io::{SaslAuthenticateRequest, SaslHandshakeRequest};
use tansu_service::FrameRequestLayer;
use tansu_service::FrameRouteBuilder;

use crate::Error;

pub fn services(
    builder: FrameRouteBuilder<(), Error>,
) -> Result<FrameRouteBuilder<(), Error>, Error> {
    let with_authenticate = authenticate(builder)?;
    handshake(with_authenticate)
}

pub fn authenticate(
    builder: FrameRouteBuilder<(), Error>,
) -> Result<FrameRouteBuilder<(), Error>, Error> {
    let decoding = FrameRequestLayer::<SaslAuthenticateRequest>::new();
    let layers = (MapErrLayer::new(Error::from), decoding);
    let route = layers.into_layer(SaslAuthenticateService::default());
    let routed = builder.with_route(SaslAuthenticateRequest::KEY, route.boxed());
    routed.map_err(Error::from)
}

pub fn handshake(
    builder: FrameRouteBuilder<(), Error>,
) -> Result<FrameRouteBuilder<(), Error>, Error> {
    let decoding = FrameRequestLayer::<SaslHandshakeRequest>::new();
    let layers = (MapErrLayer::new(Error::from), decoding);
    let route = layers.into_layer(SaslHandshakeService);
    let routed = builder.with_route(SaslHandshakeRequest::KEY, route.boxed());
    routed.map_err(Error::from)
}
