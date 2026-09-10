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
//! - Topic: Topic administration
//! - User: SASL/SCRAM credential administration

use std::process;

use crate::Result;
use clap::{Parser, Subcommand};
use tansu_sans_io::ErrorCode;
use tracing::debug;

#[cfg(feature = "dynostore")]
mod audit;

mod broker;
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

    /// Report the offsets a bucket's segments cannot serve, offline (#447)
    #[cfg(feature = "dynostore")]
    Audit(Box<audit::Arg>),

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
    /// The subcommand to run, which is `broker` when none was supplied.
    ///
    /// Separate from [`Self::main`] because `main` parses the process's own
    /// argv and can only be exercised by running the binary: the default this
    /// resolves is documented on [`Command::Broker`] and was, until #556,
    /// asserted nowhere.
    fn subcommand(self) -> Command {
        self.command
            .unwrap_or_else(|| Command::Broker(Box::new(self.broker)))
    }

    /// The process entry point, and the one thing in this crate no test runs:
    /// it reads the real argv and every arm it dispatches to either serves
    /// until cancelled or is covered by that subcommand's own tests. Left
    /// uncovered rather than excluded from the denominator — a `main` behind
    /// an `--ignore-filename-regex` is a `main` nobody counts again (#556).
    pub async fn main() -> Result<ErrorCode> {
        debug!(
            pid = process::id(),
            storage = ?storage_engines()
        );

        match Cli::parse().subcommand() {
            #[cfg(feature = "dynostore")]
            Command::Audit(arg) => arg.main().await,
            Command::Broker(arg) => arg.main().await,
            Command::Topic { command } => command.main().await,
            Command::User { command } => command.main().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory as _;

    /// clap's own validation of the derived command: duplicate long options,
    /// a `requires` naming an argument that does not exist, a subcommand
    /// whose name collides. It panics on a malformed definition, and the
    /// definition here spans four files.
    #[test]
    fn the_command_is_well_formed() {
        Cli::command().debug_assert();
    }

    /// `tansu` with no subcommand is `tansu broker` — the promise in
    /// `Command::Broker`'s help text, and what every deployment relies on.
    #[test]
    fn no_subcommand_is_the_broker() {
        assert!(matches!(
            Cli::try_parse_from(["tansu"])
                .expect("no arguments")
                .subcommand(),
            Command::Broker(_)
        ));
    }

    /// The broker's arguments are flattened onto the top level so that
    /// `tansu --storage-engine s3://tansu/` works, and they must survive the
    /// default into the `Broker` arm rather than being dropped for a fresh
    /// one built from defaults.
    #[test]
    fn the_flattened_broker_arguments_reach_the_default_subcommand() {
        let Command::Broker(arg) = Cli::try_parse_from(["tansu", "--cluster-id", "abcd"])
            .expect("--cluster-id abcd")
            .subcommand()
        else {
            panic!("no subcommand is the broker")
        };

        assert_eq!("abcd", arg.cluster_id());
    }

    /// `args_conflicts_with_subcommands`: the flattened broker arguments and
    /// a subcommand are refused together, because `tansu --storage-engine
    /// s3://tansu/ topic list` reads as though the topic list would go to
    /// that store, and it would not.
    #[test]
    fn the_broker_arguments_and_a_subcommand_are_refused_together() {
        assert!(
            Cli::try_parse_from(["tansu", "--cluster-id", "abcd", "topic", "list"]).is_err(),
            "a broker argument alongside a subcommand must not parse"
        );
    }

    #[test]
    fn each_subcommand_routes_to_its_own_arm() {
        assert!(matches!(
            Cli::try_parse_from(["tansu", "topic", "list"])
                .expect("topic list")
                .subcommand(),
            Command::Topic { .. }
        ));

        assert!(matches!(
            Cli::try_parse_from(["tansu", "user", "delete", "alice"])
                .expect("user delete")
                .subcommand(),
            Command::User { .. }
        ));

        assert!(matches!(
            Cli::try_parse_from(["tansu", "broker"])
                .expect("broker")
                .subcommand(),
            Command::Broker(_)
        ));
    }

    /// `audit` exists only with the object store built in, and the after-help
    /// naming the engines comes from the same `cfg`. One test, so the two
    /// cannot drift into a build that offers `audit` over no storage.
    #[cfg(feature = "dynostore")]
    #[test]
    fn the_object_store_build_offers_audit_and_says_so() {
        assert!(matches!(
            Cli::try_parse_from(["tansu", "audit"])
                .expect("audit")
                .subcommand(),
            Command::Audit(_)
        ));

        assert_eq!("Storage engines: dynostore", after_help());
    }

    /// Without it there is no storage engine at all, and the after-help must
    /// not claim one.
    #[cfg(not(feature = "dynostore"))]
    #[test]
    fn a_build_with_no_storage_engine_claims_none() {
        assert!(storage_engines().is_empty());
        assert_eq!("Storage engines: ", after_help());
    }
}
