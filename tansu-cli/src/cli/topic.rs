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

use std::{collections::HashMap, error::Error, str::FromStr};

use crate::Result;
use clap::Subcommand;
use tansu_sans_io::ErrorCode;
use tansu_topic::Topic;
use url::Url;

use super::DEFAULT_BROKER;

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    /// Create a topic
    Create {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// The name of the topic to create
        #[clap(value_parser)]
        name: String,

        /// The number of partitions to create
        #[arg(long, default_value = "3")]
        partitions: i32,

        #[arg(long, value_parser = parse_key_val::<String, String>)]
        config: Vec<(String, String)>,
    },

    /// Delete an existing topic
    Delete {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// The name of the topic to delete
        #[clap(value_parser)]
        name: String,
    },

    /// List existing topics
    List {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,
    },
}

impl From<Command> for Topic {
    fn from(value: Command) -> Self {
        match value {
            Command::Create {
                broker,
                name,
                partitions,
                config,
            } => Topic::create()
                .broker(broker)
                .name(name)
                .partitions(partitions)
                .config(HashMap::from_iter(config))
                .build(),

            Command::Delete { broker, name } => Topic::delete().broker(broker).name(name).build(),

            Command::List { broker } => Topic::list().broker(broker).build(),
        }
    }
}

impl Command {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        Topic::from(self).main().await.map_err(Into::into)
    }
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser as _;

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

    /// A `--config` is `KEY=value`, and the value keeps every `=` after the
    /// first: `retention.ms=-1` and a base64 value both have to survive.
    #[test]
    fn a_config_splits_on_the_first_equals_only() {
        assert_eq!(
            (String::from("retention.ms"), String::from("-1")),
            parse_key_val::<String, String>("retention.ms=-1").expect("retention.ms=-1")
        );

        assert_eq!(
            (String::from("k"), String::from("a=b=c")),
            parse_key_val::<String, String>("k=a=b=c").expect("k=a=b=c")
        );

        assert_eq!(
            (String::from("k"), String::new()),
            parse_key_val::<String, String>("k=").expect("k=")
        );
    }

    /// Without a `=` there is no pair, and the error says what was expected —
    /// a topic created with a config that silently did not apply is the
    /// failure to avoid.
    #[test]
    fn a_config_without_an_equals_is_refused() {
        let error = parse_key_val::<String, String>("retention.ms")
            .expect_err("no `=`")
            .to_string();

        assert!(error.contains("invalid KEY=value"), "{error}");
        assert!(
            Root::try_parse_from(["tansu", "create", "abc", "--config", "retention.ms"]).is_err()
        );
    }

    /// Every subcommand becomes the `tansu-topic` request it names, with the
    /// broker, the name and the configs it was given. The conversion is the
    /// whole of what this module does.
    #[test]
    fn each_subcommand_becomes_its_request() -> Result<()> {
        let broker = Url::parse(DEFAULT_BROKER)?;

        assert_eq!(
            Topic::create()
                .broker(broker.clone())
                .name(String::from("abc"))
                .partitions(3)
                .config(HashMap::new())
                .build(),
            Topic::from(parse(["tansu", "create", "abc"]))
        );

        assert_eq!(
            Topic::create()
                .broker(Url::parse("tcp://example.com:9092")?)
                .name(String::from("abc"))
                .partitions(12)
                .config(HashMap::from_iter([
                    (String::from("cleanup.policy"), String::from("compact")),
                    (String::from("retention.ms"), String::from("-1")),
                ]))
                .build(),
            Topic::from(parse([
                "tansu",
                "create",
                "abc",
                "--broker",
                "tcp://example.com:9092",
                "--partitions",
                "12",
                "--config",
                "cleanup.policy=compact",
                "--config",
                "retention.ms=-1",
            ]))
        );

        assert_eq!(
            Topic::delete()
                .broker(broker.clone())
                .name(String::from("abc"))
                .build(),
            Topic::from(parse(["tansu", "delete", "abc"]))
        );

        assert_eq!(
            Topic::list().broker(broker).build(),
            Topic::from(parse(["tansu", "list"]))
        );

        Ok(())
    }

    /// A broker that is not there is an error rather than an error *code*: a
    /// code would be reported as though the broker had answered.
    #[tokio::test(start_paused = true)]
    async fn an_unreachable_broker_is_an_error() {
        assert!(
            parse(["tansu", "list", "--broker", "tcp://localhost:1"])
                .main()
                .await
                .is_err()
        );
    }

    /// The subcommands against a broker that answers, which is where the
    /// delegation is visible: a `create` that returns `None` and created
    /// nothing passes every assertion above and fails here, because the
    /// second create would not come back `TopicAlreadyExists`.
    #[tokio::test]
    async fn a_created_topic_is_visible_to_a_second_create() -> Result<()> {
        let (broker, cancellation) = crate::harness::broker().await?;
        let name = format!("t{}", uuid::Uuid::now_v7().simple());

        let create = || Command::Create {
            broker: broker.clone(),
            name: name.clone(),
            partitions: 3,
            config: vec![(String::from("retention.ms"), String::from("-1"))],
        };

        assert_eq!(ErrorCode::None, create().main().await?);
        assert_eq!(ErrorCode::TopicAlreadyExists, create().main().await?);

        assert_eq!(
            ErrorCode::None,
            Command::List {
                broker: broker.clone()
            }
            .main()
            .await?
        );

        assert_eq!(
            ErrorCode::None,
            Command::Delete {
                broker: broker.clone(),
                name: name.clone(),
            }
            .main()
            .await?
        );

        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            Command::Delete {
                broker,
                name: name.clone(),
            }
            .main()
            .await?,
            "the delete must have removed the topic, not merely been answered",
        );

        cancellation.cancel();

        Ok(())
    }
}
