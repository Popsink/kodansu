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

//! Tansu CLI
//!
//! The CLI is a single statically linked binary that contains:
//! - Broker
//! - Proxy: a Kafka API proxy
//! - Topic: Topic administration

use std::process;

use crate::Result;
use clap::{Parser, Subcommand};
use tansu_sans_io::ErrorCode;
use tracing::debug;

mod broker;
mod perf;
mod proxy;
mod topic;
mod user;

const DEFAULT_BROKER: &str = "tcp://localhost:9092";

fn storage_engines() -> Vec<&'static str> {
    vec![
        #[cfg(feature = "dynostore")]
        "dynostore",
    ]
}

fn after_help() -> String {
    format!("Storage engines: {}", storage_engines().join(", "))
}

#[derive(Clone, Debug, Parser)]
#[command(
    name = "tansu",
    version,
    about,
    long_about = None,
    after_help = after_help(),
    args_conflicts_with_subcommands = true
)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[clap(flatten)]
    broker: broker::Arg,
}

#[derive(Clone, Debug, Subcommand)]
enum Command {
    /// Apache Kafka compatible broker backed by an object store (S3, GCS or memory) [default if no command supplied]
    Broker(Box<broker::Arg>),

    /// Performance
    Perf(Box<perf::Arg>),

    /// Apache Kafka compatible proxy
    Proxy(Box<proxy::Arg>),

    /// Create, list or delete topics managed by the broker
    Topic {
        #[command(subcommand)]
        command: topic::Command,
    },

    /// Create, list or delete users managed by the broker
    User {
        #[command(subcommand)]
        command: user::Command,
    },
}

impl Cli {
    pub async fn main() -> Result<ErrorCode> {
        debug!(
            pid = process::id(),
            storage = ?storage_engines()
        );

        let cli = Cli::parse();

        match cli.command.unwrap_or(Command::Broker(Box::new(cli.broker))) {
            Command::Broker(arg) => arg.main().await,
            Command::Perf(arg) => arg.main().await,
            Command::Proxy(arg) => tansu_proxy::Proxy::main(
                arg.listener_url.into_inner(),
                arg.advertised_listener_url.into_inner(),
                arg.origin_url.into_inner(),
                arg.otlp_endpoint_url
                    .map(|otlp_endpoint_url| otlp_endpoint_url.into_inner()),
            )
            .await
            .map_err(Into::into),
            Command::Topic { command } => command.main().await,
            Command::User { command } => command.main().await,
        }
    }
}
