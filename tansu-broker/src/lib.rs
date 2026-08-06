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

use std::{
    fmt, io,
    net::AddrParseError,
    num::TryFromIntError,
    result,
    str::FromStr,
    sync::{Arc, LazyLock, PoisonError},
    time::{Duration, SystemTimeError},
};

use opentelemetry::{InstrumentationScope, global, metrics::Meter};
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_semantic_conventions::SCHEMA_URL;
use tansu_sans_io::ErrorCode;
use thiserror::Error;
use tokio::{sync::broadcast::error::SendError, task::JoinError};
use tracing_subscriber::filter::ParseError;

pub mod broker;
pub mod coordinator;
pub mod otel;
pub mod service;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CancelKind {
    Interrupt,
    Terminate,
}

impl From<CancelKind> for Duration {
    fn from(cancellation: CancelKind) -> Self {
        Duration::from_millis(match cancellation {
            CancelKind::Interrupt => 0,
            CancelKind::Terminate => 5_000,
        })
    }
}

pub const NODE_ID: i32 = 111;

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

#[derive(Clone, Debug, Error)]
pub enum Error {
    AddrParse(#[from] AddrParseError),
    Api(ErrorCode),
    Auth(#[from] tansu_auth::Error),
    Client(#[from] tansu_client::Error),
    ExporterBuild(Arc<ExporterBuildError>),

    Io(Arc<io::Error>),

    // `tansu_service`'s TCP service stack requires `Error: From<JoinError>` to
    // spawn its per-connection tasks, so this variant is load-bearing even
    // though nothing in this crate names it.
    Join(Arc<JoinError>),
    Json(Arc<serde_json::Error>),
    KafkaProtocol(#[from] tansu_sans_io::Error),

    Message(String),

    ParseFilter(Arc<ParseError>),
    Poison,

    Service(#[from] tansu_service::Error),
    Storage(#[from] tansu_storage::Error),
    SystemTime(#[from] SystemTimeError),

    TryFromInt(#[from] TryFromIntError),

    UnsupportedTracingFormat(String),
    Url(#[from] url::ParseError),
    Uuid(#[from] uuid::Error),
    Send(Arc<SendError<CancelKind>>),
}

impl tansu_service::Classify for Error {
    fn severity(&self) -> tansu_service::Severity {
        use tansu_service::Severity;

        match self {
            // A retriable answer the broker returns on purpose. `NOT_COORDINATOR`
            // is what #243 added: when the forward hop to a group's owner fails,
            // answer retriably so the client retries against the real owner
            // rather than processing locally and splitting the group. Every pod
            // restart lands some forwards on a terminating peer, so every
            // rollout produces these by design (#289).
            Self::Api(
                ErrorCode::NotCoordinator
                | ErrorCode::CoordinatorLoadInProgress
                | ErrorCode::CoordinatorNotAvailable
                | ErrorCode::RebalanceInProgress
                | ErrorCode::NotLeaderOrFollower
                | ErrorCode::UnknownMemberId
                | ErrorCode::IllegalGeneration,
            ) => Severity::Expected,

            // The peer went away mid-request.
            Self::Io(io)
                if matches!(
                    io.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionReset
                ) =>
            {
                Severity::Expected
            }

            // Any other API error is a real answer to a real client, and the
            // broker chose it — worth seeing, but it is not a broker failure.
            Self::Api(_) => Severity::Unexpected,

            // Defer to the layer that produced it rather than second-guessing.
            Self::Service(service) => service.severity(),

            _ => Severity::Failure,
        }
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        Self::Join(Arc::new(value))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

impl From<Arc<serde_json::Error>> for Error {
    fn from(value: Arc<serde_json::Error>) -> Self {
        Self::Json(value)
    }
}

impl From<ExporterBuildError> for Error {
    fn from(value: ExporterBuildError) -> Self {
        Self::ExporterBuild(Arc::new(value))
    }
}

impl From<SendError<CancelKind>> for Error {
    fn from(value: SendError<CancelKind>) -> Self {
        Self::Send(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Copy, Clone, Debug)]
pub enum TracingFormat {
    Text,
    Json,
}

impl FromStr for TracingFormat {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            otherwise => Err(Error::UnsupportedTracingFormat(otherwise.to_owned())),
        }
    }
}
