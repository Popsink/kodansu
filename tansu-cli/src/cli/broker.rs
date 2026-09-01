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
    path::{Path, PathBuf},
    str::FromStr as _,
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
use tansu_broker::{
    NODE_ID, SOCKET_REQUEST_MAX_BYTES, broker::Broker,
    coordinator::group::administrator::Controller,
};
use tansu_sans_io::ErrorCode;
use tansu_storage::{ArcDynStorage, DEFAULT_CLEANUP_POLICY, QuotaLimits, TopicDefaults};
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

    // Each `requires` the other, so a half-configured pair is rejected at parse
    // time instead of quietly becoming a plaintext listener. These two used to
    // share `group = "tls"`, and a clap group is mutually exclusive by default:
    // `--cert x --key y` was *refused*, so TLS was unreachable from the CLI even
    // before `Broker::listen` dropped the acceptor it had built (#358).
    /// Transport Layer Security certificate chain (PEM). With --key, the client listener is TLS only.
    #[arg(long, requires = "key")]
    cert: Option<PathBuf>,

    /// Transport Layer Security private key (PEM), for the --cert chain
    #[arg(long, requires = "cert")]
    key: Option<PathBuf>,

    /// Default `cleanup.policy` applied to topics created without one (Kafka default: delete). Set empty to store no policy; note the engine still reads absent as delete, so this does NOT give infinite retention — use retention.ms=-1 per topic for that.
    #[arg(long, env = "DEFAULT_CLEANUP_POLICY", default_value = DEFAULT_CLEANUP_POLICY)]
    default_cleanup_policy: String,

    /// Default `retention.ms` applied to delete-policy topics created without one (Kafka default: 7 days). Accepts a duration (7days, 12h), or -1/infinite/forever to retain forever.
    #[arg(long, env = "DEFAULT_RETENTION", value_parser = parse_retention_ms, default_value = "7days")]
    default_retention_ms: i64,

    /// Principals allowed everything without consulting an ACL, comma separated, e.g. "User:admin,User:ops". Only meaningful with --authentication; without at least one, a cluster with no ACLs can never be given any.
    #[arg(long, env = "SUPER_USERS", value_delimiter = ',')]
    super_users: Vec<String>,

    /// Default produce bytes/second for a principal the cluster's quotas do not name. Only meaningful with --authentication; AlterClientQuotas overrides it.
    #[arg(long, env = "QUOTA_PRODUCER_BYTE_RATE", value_parser = parse_rate)]
    quota_producer_byte_rate: Option<f64>,

    /// Default fetch bytes/second for a principal the cluster's quotas do not name. Only meaningful with --authentication; AlterClientQuotas overrides it.
    #[arg(long, env = "QUOTA_CONSUMER_BYTE_RATE", value_parser = parse_rate)]
    quota_consumer_byte_rate: Option<f64>,

    /// Default requests/second for a principal the cluster's quotas do not name. Only meaningful with --authentication; AlterClientQuotas overrides it.
    #[arg(long, env = "QUOTA_REQUEST_RATE", value_parser = parse_rate)]
    quota_request_rate: Option<f64>,

    /// How many replicas a configured quota is shared between. 1 (the default) enforces each limit on every replica, as Apache Kafka does; higher reads a limit as fleet-wide and divides it, which is approximate while the fleet is scaling.
    #[arg(long, env = "QUOTA_FLEET_SIZE", default_value_t = 1)]
    quota_fleet_size: u32,

    /// Largest request frame this broker will read, Kafka's `socket.request.max.bytes`. Accepts a size in the IEC units the storage keys use (100M is 100MiB, 8m is 8MiB), or 0/unlimited to read whatever a client announces. Must be at least the engine's `message_max_bytes`, or a legitimate oversized produce has its connection closed instead of being answered.
    #[arg(long, env = "SOCKET_REQUEST_MAX_BYTES", value_parser = parse_frame_size, default_value_t = SOCKET_REQUEST_MAX_BYTES)]
    socket_request_max_bytes: usize,

    /// Silent
    #[arg(long)]
    silent: bool,
}

/// Parse the frame cap: a size, or an explicit request for none.
///
/// `0` and `unlimited` both mean "read whatever a client announces", which is
/// what this broker did unconditionally before #477 — kept reachable, because an
/// operator turning a limit off deliberately is not the same thing as a limit
/// that was never armed, and only one of those is a decision somebody made.
/// Zero rather than `None` because clap's derive owns `Option<T>` and will not
/// take a parser that produces one; it becomes the `None` the service layer wants
/// at the point the builder is called.
///
/// Rejected at parse time rather than clamped, for the reason `parse_rate` gives
/// just below: a size limit that silently became something else is worse than one
/// that was refused at startup.
fn parse_frame_size(value: &str) -> Result<usize, String> {
    let value = value.trim();

    if value.eq_ignore_ascii_case("unlimited") {
        return Ok(0);
    }

    human_units::Size::from_str(value)
        .map_err(|err| err.to_string())
        .and_then(|size| usize::try_from(size.0).map_err(|err| err.to_string()))
}

/// Parse a quota rate: a non-negative, finite number.
///
/// Rejected at parse time rather than clamped later, because a broker started
/// with `--quota-producer-byte-rate=-1` meaning "unlimited" and getting
/// "nothing at all" is a fleet that refuses every produce, and finding that out
/// from a throttle is much later than finding it out from `--help`. Leave the
/// option off for unlimited.
fn parse_rate(value: &str) -> Result<f64, String> {
    value
        .trim()
        .parse::<f64>()
        .map_err(|err| err.to_string())
        .and_then(|rate| {
            if rate.is_finite() && rate >= 0.0 {
                Ok(rate)
            } else {
                Err(format!(
                    "{rate} is not a rate; omit the option for no limit"
                ))
            }
        })
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

    async fn build(self) -> Result<Broker<Controller<ArcDynStorage>, ArcDynStorage>> {
        let cluster_id = self.cluster_id;
        let incarnation_id = Uuid::now_v7();
        let otlp_endpoint_url = self
            .otlp_endpoint_url
            .map(|env_var_exp| env_var_exp.into_inner());

        let storage_engine = self.storage_engine.into_inner();
        let advertised_listener = self.advertised_listener_url.into_inner();
        let listener = self.listener_url.into_inner();

        // A cert that will not load fails startup. It used to end in `.ok()`,
        // which turned an unreadable file, a mismatched key or a malformed PEM
        // into `None` — and `None` is a plaintext broker on the port an operator
        // just configured for TLS (#358). The same class of defect as building
        // the acceptor and dropping it: configured, accepted, and inert.
        let tls_server_config = self
            .cert
            .as_deref()
            .zip(self.key.as_deref())
            .map(|(certs, key)| server_config(certs, key))
            .transpose()?;

        let topic_defaults = TopicDefaults {
            cleanup_policy: Some(self.default_cleanup_policy.clone())
                .filter(|policy| !policy.is_empty()),
            retention_ms: self.default_retention_ms,
        };

        let broker = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
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
            .super_users(self.super_users)
            .quota_defaults(QuotaLimits {
                producer_byte_rate: self.quota_producer_byte_rate,
                consumer_byte_rate: self.quota_consumer_byte_rate,
                request_rate: self.quota_request_rate,
            })
            .quota_fleet_size(self.quota_fleet_size)
            // Zero is the CLI's way of saying "no cap"; the service layer says
            // it with `None` (#477).
            .maximum_frame_size(
                (self.socket_request_max_bytes > 0).then_some(self.socket_request_max_bytes),
            )
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
    use clap::CommandFactory as _;
    use tansu_storage::DEFAULT_RETENTION_MS;

    /// A broker started with no flag caps frames where the service layer says it
    /// does (#477). Asserted through clap rather than on the constant, because
    /// the bug this closes was never a wrong value — it was a default that was
    /// never applied.
    #[test]
    fn the_frame_cap_is_armed_by_default() {
        assert_eq!(
            SOCKET_REQUEST_MAX_BYTES,
            Arg::try_parse_from(["tansu"])
                .expect("no arguments")
                .socket_request_max_bytes,
        );
    }

    /// `0` and `unlimited` are how an operator turns the cap off *on purpose* —
    /// which is the state every deployment was in by accident until #477, and
    /// keeping the distinction is the point.
    #[test]
    fn the_frame_cap_can_be_turned_off_deliberately() {
        assert_eq!(Ok(0), parse_frame_size("0"));
        assert_eq!(Ok(0), parse_frame_size("unlimited"));
        assert_eq!(Ok(0), parse_frame_size(" UNLIMITED "));

        // A size parses in the IEC idiom the storage keys already use, and as a
        // plain byte count.
        assert_eq!(Ok(8 * 1024 * 1024), parse_frame_size("8m"));
        assert_eq!(Ok(100 * 1024 * 1024), parse_frame_size("100M"));
        assert_eq!(Ok(104_857_600), parse_frame_size("104857600"));

        // A value that is not a size is refused at startup rather than silently
        // becoming something else. `1MiB` is in here deliberately: it is the
        // spelling `message_max_bytes`' own doc comment used to suggest, and it
        // does not parse.
        assert!(parse_frame_size("plenty").is_err());
        assert!(parse_frame_size("-1").is_err());
        assert!(parse_frame_size("1MiB").is_err());
    }

    /// #360's acceptance criterion, as a test: **no configuration option
    /// mentions a peer, a pod IP, or a DNS name.**
    ///
    /// Consumer group coordination used to need stable pod identity, a
    /// headless-Service hostname, pod-to-pod addressability and a second
    /// listener — the four things an autoscaled fleet has least of, and the
    /// reason a replica could not be added or removed on demand. Deleting the
    /// machinery is what removed them; this is what stops one coming back
    /// without the argument being made again.
    #[test]
    fn no_option_asks_the_operator_where_the_other_replicas_are() {
        const FORBIDDEN: [&str; 4] = ["peer", "pod-ip", "pod_ip", "internal-listener"];

        let command = Arg::command();
        let command = command.get_arguments().filter_map(|argument| {
            argument
                .get_long()
                .map(ToOwned::to_owned)
                .into_iter()
                .chain(
                    argument
                        .get_env()
                        .map(|env| env.to_string_lossy().into_owned()),
                )
                .find(|name| {
                    let name = name.to_lowercase();
                    FORBIDDEN.iter().any(|forbidden| name.contains(forbidden))
                })
        });

        assert_eq!(
            Vec::<String>::new(),
            command.collect::<Vec<_>>(),
            "a replica must not have to be told about its peers",
        );
    }

    /// A quota rate is a rate. `-1` is a spelling of "unlimited" an operator
    /// will reach for from `retention.ms` next door, and it must not be taken
    /// as a limit of nothing — which would refuse every produce on the fleet.
    #[test]
    fn a_quota_rate_is_non_negative_and_finite() {
        assert_eq!(Ok(1024.0), parse_rate("1024"));
        assert_eq!(Ok(0.5), parse_rate("0.5"));
        assert_eq!(Ok(0.0), parse_rate("0"));
        assert_eq!(Ok(1024.0), parse_rate("  1024 "));

        assert!(parse_rate("-1").is_err());
        assert!(parse_rate("inf").is_err());
        assert!(parse_rate("NaN").is_err());
        assert!(parse_rate("lots").is_err());
    }

    /// `--quota-fleet-size` declares how *many* replicas there are, never where
    /// any of them is (#384 against #360).
    ///
    /// The forbidden-option test above covers the naming; this covers the
    /// shape. A count takes one number and cannot become a list, which is the
    /// difference between "the fleet is this big" and a peer set arriving by
    /// the back door.
    #[test]
    fn the_fleet_size_is_a_count_and_not_a_peer_set() {
        let command = Arg::command();

        let fleet_size = command
            .get_arguments()
            .find(|argument| argument.get_long() == Some("quota-fleet-size"))
            .expect("--quota-fleet-size");

        assert_eq!(
            None,
            fleet_size.get_value_delimiter(),
            "a fleet size is one number, not a comma-separated list of replicas",
        );

        // And it parses as one — `--super-users` next door is the delimited
        // option this must not become.
        assert_eq!(
            4,
            Arg::try_parse_from(["tansu", "--quota-fleet-size", "4"])
                .expect("--quota-fleet-size 4")
                .quota_fleet_size,
        );

        assert!(
            Arg::try_parse_from(["tansu", "--quota-fleet-size", "4,5"]).is_err(),
            "a list of replicas must not parse",
        );
    }

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
