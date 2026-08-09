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
use tansu_storage::{Authorizer, Storage};

use crate::service::principal::{RequesterLayer, RequesterService};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{Error, Result, coordinator::group::Coordinator};

pub mod auth;
pub mod coordinator;
pub mod principal;
pub mod storage;

type TcpRouteFrame = TcpContextService<
    TcpBytesService<BytesFrameService<RequesterService<FrameRouteService<(), Error>>>, ()>,
>;

/// The per-connection service stack.
///
/// `drain` is cancelled when this process has been asked to stop: a connection
/// sitting idle between requests is then closed, and one with a request in
/// flight answers it first (#361). Pass a fresh token to serve connections
/// until the client goes away.
///
/// `authorizer` is `None` on a broker without `--authentication`, and its
/// absence is what disables authorization: no principals, nothing to evaluate
/// (#363).
pub fn services<C, S>(
    cluster_id: &str,
    coordinator: C,
    storage: S,
    sasl_config: Option<Arc<SASLConfig>>,
    drain: CancellationToken,
    authorizer: Option<Authorizer>,
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
                TcpContextLayer::new(
                    TcpContext::default()
                        .cluster_id(Some(cluster_id.into()))
                        .drain(drain),
                ),
                TcpBytesLayer::default(),
                BytesFrameLayer::default().with_sasl_config(sasl_config),
                RequesterLayer::new(authorizer),
            )
                .into_layer(route)
        })
}
