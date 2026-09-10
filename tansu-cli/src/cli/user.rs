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

use crate::Result;
use bytes::{Bytes, BytesMut};
use clap::{Subcommand, ValueEnum};
use pbkdf2::{hmac::Hmac, pbkdf2};
use rand::{Rng as _, rng};
use sha2::{Sha256, Sha512};
use tansu_client::{Client, ConnectionManager};
use tansu_sans_io::{
    AlterUserScramCredentialsRequest, ErrorCode, ScramMechanism,
    alter_user_scram_credentials_request::{ScramCredentialDeletion, ScramCredentialUpsertion},
};
use tracing::debug;
use url::Url;

use super::DEFAULT_BROKER;

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    /// Create a user
    Create {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// The name of the user
        name: String,

        /// Password
        password: String,

        // Iterations
        #[arg(long, default_value = "8192")]
        iterations: Option<u32>,

        // Mode
        #[arg(long, value_enum, default_value = "scram512")]
        mechanism: Mechanism,
    },

    /// Delete a user
    Delete {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// The name of the user
        name: String,

        // Mode
        #[arg(long, value_enum, default_value = "scram512")]
        mechanism: Mechanism,
    },
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, PartialOrd, Ord, ValueEnum)]
pub(super) enum Mechanism {
    /// Scram 256
    Scram256,

    /// Scram 512
    #[default]
    Scram512,
}

impl Mechanism {
    fn salted_password(&self, password: &[u8], iterations: u32, salt: &[u8]) -> Result<Bytes> {
        match self {
            Mechanism::Scram256 => {
                let mut buf = BytesMut::zeroed(32);
                pbkdf2::<Hmac<Sha256>>(password, salt, iterations, &mut buf)?;
                Ok(buf.into())
            }
            Mechanism::Scram512 => {
                let mut buf = BytesMut::zeroed(64);
                pbkdf2::<Hmac<Sha512>>(password, salt, iterations, &mut buf)?;
                Ok(buf.into())
            }
        }
    }
}

impl From<&Mechanism> for i8 {
    fn from(value: &Mechanism) -> Self {
        match value {
            Mechanism::Scram256 => ScramMechanism::Scram256.into(),
            Mechanism::Scram512 => ScramMechanism::Scram512.into(),
        }
    }
}

impl Command {
    const DEFAULT_SALT_LEN: usize = 32;
    const DEFAULT_ITERATIONS: u32 = 2u32.pow(14);

    fn broker(&self) -> Url {
        match self {
            Command::Create { broker, .. } => broker.to_owned(),
            Command::Delete { broker, .. } => broker.to_owned(),
        }
    }

    fn upsertions(&self) -> Option<Vec<ScramCredentialUpsertion>> {
        match self {
            Command::Create {
                name,
                password,
                iterations,
                mechanism,
                ..
            } => {
                let mut salt = BytesMut::zeroed(Self::DEFAULT_SALT_LEN);
                rng().fill_bytes(&mut salt);

                let iterations = iterations.unwrap_or(Self::DEFAULT_ITERATIONS);

                mechanism
                    .salted_password(password.as_bytes(), iterations, &salt[..])
                    .ok()
                    .map(|salted_password| {
                        [ScramCredentialUpsertion::default()
                            .name(name.into())
                            .mechanism(mechanism.into())
                            .iterations(iterations as i32)
                            .salt(salt.into())
                            .salted_password(salted_password)]
                        .into()
                    })
            }

            _ => Some([].into()),
        }
    }

    fn deletions(&self) -> Option<Vec<ScramCredentialDeletion>> {
        match self {
            Command::Create { .. } => Some([].into()),
            Command::Delete {
                name,
                mechanism: mode,
                ..
            } => Some(
                [ScramCredentialDeletion::default()
                    .name(name.into())
                    .mechanism(mode.into())]
                .into(),
            ),
        }
    }

    pub(super) async fn main(self) -> Result<ErrorCode> {
        let client = ConnectionManager::builder(self.broker())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        let req = AlterUserScramCredentialsRequest::default()
            .deletions(self.deletions())
            .upsertions(self.upsertions());

        let response = client
            .call(req)
            .await
            .inspect(|response| debug!(?response))?;

        // The broker answers per user, and this used to discard the answer and
        // return `None` unconditionally — so a credential the broker refused
        // exited 0, and the shell script that created it saw a user that does
        // not exist (#556). One deletion or one upsertion goes out at a time,
        // so the first non-`None` is the whole verdict.
        response
            .results
            .unwrap_or_default()
            .iter()
            .map(|result| ErrorCode::try_from(result.error_code))
            .find_map(|code| match code {
                Ok(ErrorCode::None) => None,
                otherwise => Some(otherwise),
            })
            .unwrap_or(Ok(ErrorCode::None))
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser as _;
    use uuid::Uuid;

    /// `Command` is a `Subcommand`, so it is parsed through a throwaway root
    /// the way `Cli` parses it.
    #[derive(Debug, clap::Parser)]
    struct Root {
        #[command(subcommand)]
        command: Command,
    }

    fn parse<const N: usize>(args: [&str; N]) -> Command {
        Root::try_parse_from(args)
            .unwrap_or_else(|error| panic!("{args:?}: {error}"))
            .command
    }

    #[test]
    fn test_salted_password_256() -> Result<()> {
        let password = "password";
        let salt = b"abcdef";
        let iterations = 1000;
        let mechanism = Mechanism::Scram256;

        let result = mechanism.salted_password(password.as_bytes(), iterations, salt)?;
        assert_eq!(
            [
                145, 219, 38, 255, 206, 134, 237, 218, 6, 231, 82, 1, 148, 149, 161, 210, 185, 243,
                46, 76, 94, 133, 112, 217, 144, 162, 201, 91, 29, 255, 5, 19
            ],
            &result[..]
        );
        Ok(())
    }

    #[test]
    fn test_salted_password_512() -> Result<()> {
        let password = "password";
        let salt = b"abcdef";
        let iterations = 1000;
        let mechanism = Mechanism::Scram512;

        let result = mechanism.salted_password(password.as_bytes(), iterations, salt)?;
        assert_eq!(
            [
                154, 35, 153, 145, 17, 161, 139, 24, 204, 40, 101, 29, 139, 51, 136, 125, 228, 84,
                18, 240, 169, 203, 123, 34, 18, 167, 45, 226, 1, 215, 102, 172, 150, 75, 234, 71,
                238, 187, 194, 46, 5, 38, 119, 248, 202, 110, 44, 39, 62, 227, 46, 210, 208, 201,
                180, 183, 91, 212, 148, 57, 64, 96, 115, 175
            ],
            &result[..]
        );
        Ok(())
    }

    /// The mechanism decides the digest, and the digest decides the length —
    /// 32 bytes for SCRAM-SHA-256, 64 for SCRAM-SHA-512. A credential stored
    /// at the wrong length authenticates nobody.
    #[test]
    fn the_mechanism_decides_the_salted_password_length() -> Result<()> {
        assert_eq!(
            32,
            Mechanism::Scram256
                .salted_password(b"password", 1_000, b"abcdef")?
                .len()
        );

        assert_eq!(
            64,
            Mechanism::Scram512
                .salted_password(b"password", 1_000, b"abcdef")?
                .len()
        );

        Ok(())
    }

    /// The wire values are Kafka's, not this enum's discriminants.
    #[test]
    fn the_mechanism_carries_the_kafka_wire_value() {
        assert_eq!(
            i8::from(ScramMechanism::Scram256),
            i8::from(&Mechanism::Scram256)
        );
        assert_eq!(
            i8::from(ScramMechanism::Scram512),
            i8::from(&Mechanism::Scram512)
        );

        assert_eq!(Mechanism::Scram512, Mechanism::default());
    }

    /// SCRAM-SHA-512 unless asked otherwise, on both subcommands: a `create`
    /// and the `delete` that undoes it have to name the same mechanism or the
    /// deletion misses.
    #[test]
    fn both_subcommands_default_to_the_same_mechanism() {
        let Command::Create { mechanism, .. } = parse(["tansu", "create", "alice", "hunter2"])
        else {
            panic!("create")
        };
        assert_eq!(Mechanism::Scram512, mechanism);

        let Command::Delete { mechanism, .. } = parse(["tansu", "delete", "alice"]) else {
            panic!("delete")
        };
        assert_eq!(Mechanism::Scram512, mechanism);

        let Command::Create { mechanism, .. } = parse([
            "tansu",
            "create",
            "alice",
            "hunter2",
            "--mechanism",
            "scram256",
        ]) else {
            panic!("create --mechanism scram256")
        };
        assert_eq!(Mechanism::Scram256, mechanism);
    }

    #[test]
    fn each_subcommand_carries_the_broker_it_was_given() -> Result<()> {
        assert_eq!(
            Url::parse(DEFAULT_BROKER)?,
            parse(["tansu", "create", "alice", "hunter2"]).broker()
        );

        assert_eq!(
            Url::parse("tcp://example.com:9092")?,
            parse([
                "tansu",
                "delete",
                "alice",
                "--broker",
                "tcp://example.com:9092"
            ])
            .broker()
        );

        Ok(())
    }

    /// `create` is one upsertion and no deletion; `delete` is the mirror. The
    /// request carries both lists, and a `create` that also sent a deletion
    /// would remove the credential it had just written.
    #[test]
    fn a_create_upserts_only_and_a_delete_deletes_only() {
        let create = parse([
            "tansu",
            "create",
            "alice",
            "hunter2",
            "--iterations",
            "4096",
        ]);

        let upsertions = create.upsertions().expect("upsertions");
        assert_eq!(1, upsertions.len());
        assert_eq!("alice", upsertions[0].name);
        assert_eq!(i8::from(&Mechanism::Scram512), upsertions[0].mechanism);
        assert_eq!(4_096, upsertions[0].iterations);
        assert_eq!(Command::DEFAULT_SALT_LEN, upsertions[0].salt.len());
        assert_eq!(64, upsertions[0].salted_password.len());
        assert_eq!(Some(0), create.deletions().map(|deletions| deletions.len()));

        let delete = parse(["tansu", "delete", "alice", "--mechanism", "scram256"]);

        let deletions = delete.deletions().expect("deletions");
        assert_eq!(1, deletions.len());
        assert_eq!("alice", deletions[0].name);
        assert_eq!(i8::from(&Mechanism::Scram256), deletions[0].mechanism);
        assert_eq!(
            Some(0),
            delete.upsertions().map(|upsertions| upsertions.len())
        );
    }

    /// Every `create` gets its own salt. Two credentials for the same password
    /// that hash alike are a credential store that leaks which users share
    /// one.
    #[test]
    fn the_salt_is_drawn_per_credential() {
        let create = parse(["tansu", "create", "alice", "hunter2"]);

        let salt = |command: &Command| command.upsertions().expect("upsertions")[0].salt.clone();

        assert_ne!(salt(&create), salt(&create));
    }

    /// `--iterations` is an `Option` that clap always fills, so the constant
    /// behind it is only reachable from a `Command` built in code. Asserted
    /// here rather than deleted: the field's type is what the CLI declares,
    /// and the fallback is what stops a `None` becoming zero iterations.
    #[test]
    fn a_command_with_no_iterations_falls_back_to_the_constant() {
        let create = Command::Create {
            broker: Url::parse(DEFAULT_BROKER).expect("default broker"),
            name: String::from("alice"),
            password: String::from("hunter2"),
            iterations: None,
            mechanism: Mechanism::Scram512,
        };

        assert_eq!(
            i32::try_from(Command::DEFAULT_ITERATIONS).expect("default iterations"),
            create.upsertions().expect("upsertions")[0].iterations
        );

        assert_eq!(
            8_192,
            parse(["tansu", "create", "alice", "hunter2"])
                .upsertions()
                .expect("upsertions")[0]
                .iterations,
            "the CLI's own default is clap's, and it is not this constant"
        );
    }

    /// A broker that is not there is an error rather than an error *code*.
    #[tokio::test(start_paused = true)]
    async fn an_unreachable_broker_is_an_error() {
        assert!(
            parse(["tansu", "delete", "alice", "--broker", "tcp://localhost:1"])
                .main()
                .await
                .is_err()
        );
    }

    /// The subcommands against a broker that answers.
    ///
    /// This is what a unit test on `upsertions` cannot say: that the request
    /// the CLI builds is one the broker accepts, answers, and reports `None`
    /// for. A request the broker cannot decode does not come back as an error
    /// code — the connection drops — so this is the only place a wrongly
    /// shaped `AlterUserScramCredentials` is visible.
    ///
    /// It stops at the answer because there is no way to read the credential
    /// back: `DescribeUserScramCredentials` is a stub whose default response
    /// does not encode, and a `delete` of a credential that was never there
    /// succeeds. Asserting on the stored bytes needs a full SASL handshake,
    /// which is `tansu-broker`'s `auth.rs`, not this crate's.
    #[tokio::test]
    async fn each_subcommand_is_answered_by_a_broker() -> Result<()> {
        let (broker, cancellation) = crate::harness::broker().await?;
        let name = format!("u{}", Uuid::now_v7().simple());

        for mechanism in [Mechanism::Scram256, Mechanism::Scram512] {
            assert_eq!(
                ErrorCode::None,
                Command::Create {
                    broker: broker.clone(),
                    name: name.clone(),
                    password: String::from("hunter2"),
                    iterations: Some(4_096),
                    mechanism,
                }
                .main()
                .await?,
                "{mechanism:?} create"
            );

            assert_eq!(
                ErrorCode::None,
                Command::Delete {
                    broker: broker.clone(),
                    name: name.clone(),
                    mechanism,
                }
                .main()
                .await?,
                "{mechanism:?} delete"
            );
        }

        cancellation.cancel();

        Ok(())
    }
}
