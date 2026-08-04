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
use tansu_storage::{ArcDynStorage, DEFAULT_CLEANUP_POLICY, StorageContainer, TopicDefaults};
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

    /// Default `cleanup.policy` applied to topics created without one (Kafka default: delete). Set empty to store no policy; note the engine still reads absent as delete, so this does NOT give infinite retention — use retention.ms=-1 per topic for that.
    #[arg(long, env = "DEFAULT_CLEANUP_POLICY", default_value = DEFAULT_CLEANUP_POLICY)]
    default_cleanup_policy: String,

    /// Default `retention.ms` applied to delete-policy topics created without one (Kafka default: 7 days). Accepts a duration (7days, 12h), or -1/infinite/forever to retain forever.
    #[arg(long, env = "DEFAULT_RETENTION", value_parser = parse_retention_ms, default_value = "7days")]
    default_retention_ms: i64,

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

/// Parse `DEFAULT_RETENTION` into a `retention.ms` value.
///
/// `retention.ms=-1` is the engine's only spelling of retain-forever — both expiry
/// paths map a negative value to "never" — but the argument used to parse through
/// `humantime` into a `Duration`, which cannot be negative, so a deployment whose
/// intent is "keep everything unless told otherwise" had no broker-level way to say
/// so (#224). Clearing `DEFAULT_CLEANUP_POLICY` does not help either: the engine
/// reads an absent `cleanup.policy` as Kafka's `delete` and applies the 7-day
/// fallback (#223).
///
/// Durations keep their `humantime` grammar. `-1`, `infinite` and `forever` are
/// accepted as the retain-forever forms; `-1` because it is what the topic config
/// itself uses, the words because that is what an operator reaches for.
fn parse_retention_ms(value: &str) -> Result<i64, String> {
    const FOREVER: i64 = -1;

    match value.trim() {
        "-1" | "infinite" | "forever" => Ok(FOREVER),

        duration => humantime::parse_duration(duration)
            .map_err(|err| format!("{err} (or use -1/infinite/forever to retain forever)"))
            .and_then(|duration| {
                i64::try_from(duration.as_millis()).map_err(|_| {
                    format!("{duration:?} does not fit in retention.ms; use -1 to retain forever")
                })
            }),
    }
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
            retention_ms: self.default_retention_ms,
        };

        let broker = Broker::<GroupCoordinator<ArcDynStorage>, ArcDynStorage>::builder()
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

#[cfg(test)]
mod tests {
    use super::*;
    use tansu_storage::DEFAULT_RETENTION_MS;

    #[test]
    fn retention_accepts_the_humantime_grammar() {
        assert_eq!(Ok(604_800_000), parse_retention_ms("7days"));
        assert_eq!(Ok(43_200_000), parse_retention_ms("12h"));
        assert_eq!(Ok(1_000), parse_retention_ms("1s"));
        assert_eq!(Ok(0), parse_retention_ms("0s"));
    }

    #[test]
    fn retention_accepts_every_retain_forever_spelling() {
        // `-1` is what the topic config itself uses; the words are what an
        // operator reaches for. All three must reach the engine as -1, which is
        // the ONLY value both expiry paths read as "never".
        for spelling in ["-1", "infinite", "forever"] {
            assert_eq!(
                Ok(-1),
                parse_retention_ms(spelling),
                "{spelling} must mean retain forever"
            );
        }
    }

    #[test]
    fn retention_tolerates_surrounding_whitespace() {
        // These arrive through an env var in a Helm values file, where a stray
        // space is easy and silent.
        assert_eq!(Ok(-1), parse_retention_ms("  -1  "));
        assert_eq!(Ok(-1), parse_retention_ms(" forever"));
    }

    #[test]
    fn retention_rejects_nonsense_and_says_how_to_retain_forever() {
        let err = parse_retention_ms("banana").expect_err("not a duration");
        assert!(
            err.contains("-1/infinite/forever"),
            "the error must point at the retain-forever forms, got {err:?}"
        );

        // A negative that is not -1 is not a duration either, and must not be
        // silently rounded into retain-forever.
        assert!(parse_retention_ms("-2").is_err());
        assert!(parse_retention_ms("-1s").is_err());
    }

    #[test]
    fn retention_defaults_to_the_kafka_default() {
        // The clap `default_value` is the string "7days"; pin that it resolves to
        // the same constant the storage layer would have used.
        assert_eq!(Ok(DEFAULT_RETENTION_MS), parse_retention_ms("7days"));
    }
}
