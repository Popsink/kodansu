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

use tansu_sans_io::delete_topics_request::DeleteTopicState;
use tansu_sans_io::{DeleteTopicsRequest, ErrorCode, NULL_TOPIC_ID};
use url::Url;

use crate::Topic;
use crate::{Error, Result};

#[derive(Clone, Debug, Default)]
pub struct Builder<B, N> {
    broker: B,
    name: N,
}

impl<B, N> Builder<B, N> {
    pub fn broker(self, broker: Url) -> Builder<Url, N> {
        let name = self.name;
        Builder { broker, name }
    }

    pub fn name(self, name: impl Into<String>) -> Builder<B, String> {
        let (broker, name) = (self.broker, name.into());
        Builder { broker, name }
    }
}

impl Builder<Url, String> {
    pub fn build(self) -> Topic {
        let Self { broker, name } = self;
        Topic::Delete(Configuration { broker, name })
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Configuration {
    broker: Url,
    name: String,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Delete {
    configuration: Configuration,
}

impl TryFrom<Configuration> for Delete {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        Ok(Self { configuration })
    }
}

impl Delete {
    pub(crate) async fn main(self) -> Result<ErrorCode> {
        let client = crate::connected(&self.configuration.broker).await?;

        let named = Some(self.configuration.name);
        let topic = DeleteTopicState::default()
            .name(named)
            .topic_id(NULL_TOPIC_ID);
        let request = DeleteTopicsRequest::default().topics(Some(vec![topic]));
        let request = request.topic_names(None).timeout_ms(30_000);

        let answered = client.call(request).await?;
        tracing::debug!(response = ?answered);
        let results = answered.responses.unwrap_or_default();
        let codes: Vec<i16> = results.iter().map(|result| result.error_code).collect();
        crate::sole_code(&codes)
    }
}
