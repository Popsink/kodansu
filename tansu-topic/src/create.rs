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

use std::collections::HashMap;

use tansu_sans_io::CreateTopicsRequest;
use tansu_sans_io::ErrorCode;
use tansu_sans_io::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use url::Url;

use super::Topic;
use crate::{Error, Result};

#[derive(Clone, Debug, Default)]
pub struct Builder<B, N, P> {
    broker: B,
    name: N,
    partitions: P,
    configs: HashMap<String, String>,
}

impl<B, N, P> Builder<B, N, P> {
    /// A builder from its four parts, whatever stage each is at.
    fn assembled(at: B, topic: N, count: P, overrides: HashMap<String, String>) -> Self {
        Self {
            configs: overrides,
            partitions: count,
            name: topic,
            broker: at,
        }
    }

    pub fn broker(self, broker: Url) -> Builder<Url, N, P> {
        Builder::assembled(broker, self.name, self.partitions, self.configs)
    }

    pub fn name(self, name: impl Into<String>) -> Builder<B, String, P> {
        Builder::assembled(self.broker, name.into(), self.partitions, self.configs)
    }

    pub fn partitions(self, partitions: i32) -> Builder<B, N, i32> {
        Builder::assembled(self.broker, self.name, partitions, self.configs)
    }

    pub fn config(self, configs: HashMap<String, String>) -> Builder<B, N, P> {
        Self { configs, ..self }
    }
}

impl Builder<Url, String, i32> {
    pub fn build(self) -> Topic {
        let Self {
            configs,
            partitions,
            name,
            broker,
        } = self;
        let configuration = Configuration {
            configs,
            partitions,
            name,
            broker,
        };
        Topic::Create(configuration)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Configuration {
    broker: Url,
    name: String,
    partitions: i32,
    configs: HashMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Create {
    configuration: Configuration,
}

impl TryFrom<Configuration> for Create {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        Ok(Self { configuration })
    }
}

impl Create {
    pub(crate) async fn main(self) -> Result<ErrorCode> {
        let Configuration {
            broker,
            name,
            partitions: count,
            configs: overrides,
        } = self.configuration;
        let client = crate::connected(&broker).await?;

        let to_config = |(key, value)| CreatableTopicConfig::default().name(key).value(Some(value));
        let overrides: Vec<_> = overrides.into_iter().map(to_config).collect();
        let topic = CreatableTopic::default().name(name).num_partitions(count);
        let topic = topic.replication_factor(-1).assignments(Some(vec![]));
        let request =
            CreateTopicsRequest::default().topics(Some(vec![topic.configs(Some(overrides))]));
        let request = request.timeout_ms(30_000).validate_only(Some(false));

        let answered = client.call(request).await?;
        tracing::debug!(response = ?answered);
        let results = answered.topics.unwrap_or_default();
        let codes: Vec<i16> = results.iter().map(|result| result.error_code).collect();
        crate::sole_code(&codes)
    }
}
