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

//! What the tests here share: their error type, a log file per test, and the
//! `Metadata` exchange the channel and TCP round trips both make.

#![allow(dead_code, reason = "not every test binary here uses all of it")]

use std::fs::File;
use std::sync::Arc;
use std::thread;

use tansu_sans_io::Error as ProtocolError;
use tansu_sans_io::{ApiKey as _, Body, Frame, Header, MetadataRequest, MetadataResponse};
use tansu_service::Error as ServiceError;
use tracing::subscriber::DefaultGuard as Guard;
use tracing_subscriber::filter::ParseError as FilterError;

/// Everything a test here can fail with. Only the conversions matter: the
/// payloads are there for the `Debug` a failing test prints.
#[derive(thiserror::Error, Clone, Debug)]
#[allow(dead_code, reason = "the payloads are only read through `Debug`")]
pub(crate) enum Error {
    Io(Arc<std::io::Error>),
    Join(Arc<tokio::task::JoinError>),
    Message(Box<str>),
    ParseFilter(Arc<FilterError>),
    Protocol(#[from] ProtocolError),
    Service(#[from] ServiceError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

// Takes the default — `Severity::Failure` for everything — which is the point of
// the default existing: a type that has not thought about severity keeps the loud
// behaviour rather than being quietly downgraded.
impl tansu_service::Classify for Error {}

/// `From` for the sources an [`Error`] holds behind an `Arc`, to stay `Clone`.
macro_rules! shared {
    ($($variant:ident <- $source:ty),* $(,)?) => {
        $(impl From<$source> for Error {
            fn from(source: $source) -> Self {
                Self::$variant(Arc::new(source))
            }
        })*
    };
}

shared!(Io <- std::io::Error, Join <- tokio::task::JoinError, ParseFilter <- FilterError);

/// Logs this test at `debug` for this binary, on top of `RUST_LOG`, into
/// `../logs/<package>/<crate>::<thread>.log`: one file per test.
pub(crate) fn init_tracing() -> Result<Guard, Error> {
    let directive = format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?;
    let filter = tracing_subscriber::EnvFilter::from_default_env().add_directive(directive);

    let unnamed = || Error::Message("unnamed thread".into());
    let current = thread::current();
    let test = current.name().ok_or_else(unnamed)?;
    let (package, binary) = (env!("CARGO_PKG_NAME"), env!("CARGO_CRATE_NAME"));
    let log = File::create(format!("../logs/{package}/{binary}::{test}.log"))?;

    let subscriber = tracing_subscriber::fmt().with_level(true);
    let subscriber = subscriber.with_line_number(true).with_thread_names(false);
    let subscriber = subscriber
        .with_env_filter(filter)
        .with_writer(Arc::new(log));
    Ok(tracing::subscriber::set_default(subscriber.finish()))
}

/// A frame of `body` under `header`, its size left for the encoder.
pub(crate) fn frame(header: Header, body: Body) -> Frame {
    Frame {
        size: 0,
        header,
        body,
    }
}

/// A v12 `Metadata` request for no topics, asking for nothing optional.
pub(crate) fn metadata_request() -> Frame {
    let request = MetadataRequest::default().topics(Some(vec![]));
    let request = request.allow_auto_topic_creation(Some(false));
    let request = request.include_cluster_authorized_operations(Some(false));
    let request = request.include_topic_authorized_operations(Some(false));
    let client_id = Some(String::from(env!("CARGO_PKG_NAME")));
    let (api_key, api_version) = (MetadataRequest::KEY, 12);
    let header = Header::Request {
        api_key,
        api_version,
        correlation_id: 0,
        client_id,
    };
    frame(header, request.into())
}

/// The answer the servers here give any `Metadata` request: cluster `abc`,
/// controller 111, no brokers and no topics.
pub(crate) fn metadata_response() -> MetadataResponse {
    let response = MetadataResponse::default().cluster_id(Some("abc".into()));
    let response = response.controller_id(Some(111)).throttle_time_ms(Some(0));
    let response = response.brokers(Some(vec![])).topics(Some(vec![]));
    response.cluster_authorized_operations(Some(-1))
}

/// Asserts that `frame` carries [`metadata_response`]'s cluster and controller.
pub(crate) fn assert_answered(frame: Frame) -> Result<(), Error> {
    let answer = MetadataResponse::try_from(frame.body)?;
    let identity = (answer.cluster_id.as_deref(), answer.controller_id);
    assert_eq!((Some("abc"), Some(111)), identity);
    Ok(())
}
