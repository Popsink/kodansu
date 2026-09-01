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

use std::sync::Arc;

use rama::Layer;
use rsasl::config::SASLConfig;
use tansu_service::{
    BytesFrameLayer, BytesFrameService, FrameRouteService, TcpBytesLayer, TcpBytesService,
    TcpContext, TcpContextLayer, TcpContextService,
};
use tansu_storage::{Authorizer, QuotaEnforcer, Storage};

use crate::service::principal::{RequesterLayer, RequesterService};
use crate::service::quota::{QuotaLayer, QuotaService};
use tracing::debug;

use crate::{Error, Result, coordinator::group::Coordinator};

pub mod auth;
pub mod coordinator;
pub mod principal;
pub mod quota;
pub mod storage;

type TcpRouteFrame = TcpContextService<
    TcpBytesService<
        BytesFrameService<RequesterService<QuotaService<FrameRouteService<(), Error>>>>,
        (),
    >,
>;

/// The per-connection service stack.
///
/// `tcp` carries everything that is a property of the connection rather than of
/// a request: the cluster id every metric is labelled with, the frame cap the
/// listener reads within (#477), and the drain token. It arrives already built
/// because the caller is the only thing that knows an operator's values — and
/// because three more positional arguments here is how a signature stops being
/// readable.
///
/// `tcp.drain` is cancelled when this process has been asked to stop: a
/// connection sitting idle between requests is then closed, and one with a
/// request in flight answers it first (#361). A default token serves connections
/// until the client goes away.
///
/// `authorizer` is `None` on a broker without `--authentication`, and its
/// absence is what disables authorization: no principals, nothing to evaluate
/// (#363). `enforcer` is `None` for the same reason and on the same switch:
/// with no principal there is nothing to write a quota against (#384).
pub fn services<C, S>(
    tcp: TcpContext,
    coordinator: C,
    storage: S,
    sasl_config: Option<Arc<SASLConfig>>,
    authorizer: Option<Authorizer>,
    enforcer: Option<QuotaEnforcer>,
) -> Result<TcpRouteFrame, Error>
where
    S: Storage + Clone,
    C: Coordinator,
{
    storage::services(FrameRouteService::<(), Error>::builder(), storage)
        .inspect(|builder| debug!(?builder))
        .and_then(|builder| {
            coordinator::services(builder, coordinator).inspect(|builder| debug!(?builder))
        })
        .and_then(auth::services)
        .and_then(|builder| builder.build().map_err(Into::into))
        .map(|route| {
            (
                TcpContextLayer::new(tcp),
                TcpBytesLayer::default(),
                BytesFrameLayer::default().with_sasl_config(sasl_config),
                RequesterLayer::new(authorizer),
                // Immediately inside the layer that says who is asking, and
                // outside every API's own service: a quota is a property of the
                // principal, not of the produce path (#384).
                QuotaLayer::new(enforcer),
            )
                .into_layer(route)
        })
}
