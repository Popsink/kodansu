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

use crate::EnvVarExp;

use super::DEFAULT_BROKER;
use clap::Args;
use url::Url;

#[derive(Args, Clone, Debug)]
pub(super) struct Arg {
    /// The proxy will listen on this address
    #[arg(long, env = "LISTENER_URL", default_value = "tcp://0.0.0.0:9092")]
    pub(super) listener_url: EnvVarExp<Url>,

    /// This location is advertised to clients in metadata
    #[arg(
        long,
        env = "ADVERTISED_LISTENER_URL",
        default_value = DEFAULT_BROKER,
    )]
    pub(super) advertised_listener_url: EnvVarExp<Url>,

    /// The proxy will forward traffic to this origin broker
    #[arg(long, env = "ORIGIN_URL", default_value = DEFAULT_BROKER)]
    pub(super) origin_url: EnvVarExp<Url>,

    /// Coordinator RPC address. When set (with --object-store-url), the proxy
    /// becomes a stateless front: produce writes batches to the object store via
    /// the coordinator instead of forwarding to the origin.
    #[arg(long, env = "COORDINATOR_URL")]
    pub(super) coordinator_url: Option<EnvVarExp<Url>>,

    /// Object store for record batches (`memory` or `s3://bucket`, credentials
    /// from the environment). Required alongside --coordinator-url.
    #[arg(long, env = "OBJECT_STORE_URL")]
    pub(super) object_store_url: Option<EnvVarExp<Url>>,

    /// OTEL Exporter OTLP endpoint
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub(super) otlp_endpoint_url: Option<EnvVarExp<Url>>,
}
