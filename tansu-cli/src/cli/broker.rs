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

use std::{
    net::IpAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{EnvVarExp, Result, cli::storage_engines};

use super::DEFAULT_BROKER;
use clap::Parser;
use owo_colors::{OwoColorize as _, Stream, Style};
use rustls::{
    ServerConfig,
    pki_types::{
        CertificateDer, PrivateKeyDer,
        pem::{Error as TlsPkiPemError, PemObject as _},
    },
};
use tansu_broker::{NODE_ID, broker::Broker, coordinator::group::forward::GroupCoordinator};
use tansu_sans_io::ErrorCode;
use tansu_storage::{
    ArcDynStorage, DEFAULT_CLEANUP_POLICY, DEFAULT_RETENTION_MS, StorageContainer, TopicDefaults,
};
use tokio::time::Instant;
use tracing::debug;
use url::Url;
use uuid::Uuid;

#[derive(Clone, Debug, Parser)]
pub(super) struct Arg {
    /// All members of the same cluster should use the same id
    #[arg(
        long,
        env = "CLUSTER_ID",
        default_value = "tansu_cluster",
        visible_alias = "kafka-cluster-id"
    )]
    cluster_id: String,

    /// The broker will listen on this address
    #[arg(
        long,
        env = "LISTENER_URL",
        default_value = "tcp://0.0.0.0:9092",
        visible_alias = "kafka-listener-url"
    )]
    listener_url: EnvVarExp<Url>,

    /// This location is advertised to clients in metadata
    #[arg(
        long,
        env = "ADVERTISED_LISTENER_URL",
        default_value = DEFAULT_BROKER,
        visible_alias = "kafka-advertised-listener-url"
    )]
    advertised_listener_url: EnvVarExp<Url>,

    /// Storage engine examples are: memory://tansu/, s3://tansu/ or gs://tansu/
    #[arg(long, env = "STORAGE_ENGINE", default_value = "memory://tansu/")]
    storage_engine: EnvVarExp<Url>,

    /// OTEL Exporter OTLP endpoint
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otlp_endpoint_url: Option<EnvVarExp<Url>>,

    /// When present, client authentication is required
    #[arg(long)]
    authentication: bool,

    /// Transport Layer Security Certificate
    #[arg(group = "tls", long)]
    cert: Option<PathBuf>,

    /// Transport Layer Security Key
    #[arg(group = "tls", long)]
    key: Option<PathBuf>,

    /// Default `cleanup.policy` applied to topics created without one (Kafka default: delete). Set empty to opt out (infinite retention).
    #[arg(long, env = "DEFAULT_CLEANUP_POLICY", default_value = DEFAULT_CLEANUP_POLICY)]
    default_cleanup_policy: String,

    /// Default `retention.ms` applied to delete-policy topics created without one (Kafka default: 7 days)
    #[arg(long, env = "DEFAULT_RETENTION", value_parser = humantime::parse_duration, default_value = "7days")]
    default_retention: Duration,

    /// Forward each consumer group's coordination APIs to the group's deterministic owner replica (default: off, pure-local coordination)
    #[arg(
        long,
        env = "GROUP_FORWARDING",
        action = clap::ArgAction::SetTrue,
        value_parser = clap::builder::FalseyValueParser::new()
    )]
    group_forwarding: bool,

    /// Headless-Service hostname whose A/AAAA records list the peer replicas eligible to own consumer groups
    #[arg(long, env = "GROUP_FORWARD_PEER_DNS")]
    group_forward_peer_dns: Option<String>,

    /// The internal (broker-to-broker) listener URL; forwarded group requests are sent to each owner at this port
    #[arg(
        long,
        env = "INTERNAL_LISTENER_URL",
        default_value = "tcp://0.0.0.0:9093"
    )]
    internal_listener_url: EnvVarExp<Url>,

    /// This replica's own IP address (in Kubernetes, the pod IP via the Downward API)
    #[arg(long, env = "POD_IP")]
    pod_ip: Option<EnvVarExp<IpAddr>>,

    /// Silent
    #[arg(long)]
    silent: bool,
}

fn load_certs(filename: &Path) -> Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(filename)
        .and_then(|der| der.collect::<Result<Vec<_>, TlsPkiPemError>>())
        .map_err(Into::into)
}

fn load_private_key(filename: &Path) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(filename).map_err(Into::into)
}

fn server_config(certs: &Path, private_key: &Path) -> Result<ServerConfig> {
    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(load_certs(certs)?, load_private_key(private_key)?)
        .map_err(Into::into)
}

fn redact_password(mut url: Url) -> Url {
    if url.password().is_some() {
        _ = url.set_password(None).ok();
    }

    url
}

impl Arg {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        let started = Instant::now();
        self.build()
            .await?
            .main(started)
            .await
            .inspect(|result| debug!(?result))
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    async fn build(self) -> Result<Broker<GroupCoordinator<ArcDynStorage>, ArcDynStorage>> {
        let cluster_id = self.cluster_id;
        let incarnation_id = Uuid::now_v7();
        let otlp_endpoint_url = self
            .otlp_endpoint_url
            .map(|env_var_exp| env_var_exp.into_inner());

        let storage_engine = self.storage_engine.into_inner();
        let advertised_listener = self.advertised_listener_url.into_inner();
        let listener = self.listener_url.into_inner();

        let tls_server_config = self
            .cert
            .and_then(|certs| self.key.and_then(|key| server_config(&certs, &key).ok()));

        let topic_defaults = TopicDefaults {
            cleanup_policy: Some(self.default_cleanup_policy.clone())
                .filter(|policy| !policy.is_empty()),
            retention_ms: i64::try_from(self.default_retention.as_millis())
                .unwrap_or(DEFAULT_RETENTION_MS),
        };

        let broker = Broker::<GroupCoordinator<StorageContainer>, StorageContainer>::builder()
            .node_id(NODE_ID)
            .cluster_id(cluster_id)
            .incarnation_id(incarnation_id)
            .advertised_listener(advertised_listener.clone())
            .otlp_endpoint_url(otlp_endpoint_url)
            .storage(storage_engine.clone())
            .listener(listener.clone())
            .authentication(self.authentication)
            .tls_server_config(tls_server_config)
            .topic_defaults(topic_defaults)
            .group_forwarding(self.group_forwarding)
            .group_forward_peer_dns(self.group_forward_peer_dns)
            .internal_listener_url(Some(self.internal_listener_url.into_inner()))
            .pod_ip(self.pod_ip.map(|env_var_exp| env_var_exp.into_inner()))
            .silent(self.silent);

        if !self.silent {
            let sheet = Sheet::default();

            println!(
                "tansu {} {}",
                "broker".if_supports_color(Stream::Stdout, |text| text.style(sheet.headline)),
                env!("CARGO_PKG_VERSION")
                    .if_supports_color(Stream::Stdout, |text| text.style(sheet.version))
            );

            println!(
                "listening on: {} (advertised: {})",
                listener.if_supports_color(Stream::Stdout, |text| text.style(sheet.listener)),
                advertised_listener.if_supports_color(Stream::Stdout, |text| text
                    .style(sheet.advertised_listener))
            );

            println!(
                "storage: {} {:?}",
                redact_password(storage_engine)
                    .if_supports_color(Stream::Stdout, |text| text.style(sheet.storage)),
                storage_engines()
                    .iter()
                    .map(|storage_engine| storage_engine
                        .if_supports_color(Stream::Stdout, |text| text.style(sheet.storage)))
                    .collect::<Vec<_>>()
            );
        }

        broker.build().await.map_err(Into::into)
    }
}

struct Sheet {
    advertised_listener: Style,
    headline: Style,
    listener: Style,
    storage: Style,
    version: Style,
}

impl Default for Sheet {
    fn default() -> Self {
        Self {
            advertised_listener: Style::new().magenta().bold(),
            headline: Style::new().green().bold(),
            listener: Style::new().magenta().bold(),
            storage: Style::new().magenta().bold(),
            version: Style::new().magenta().bold(),
        }
    }
}
