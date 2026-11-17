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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::sync::{MutexGuard, PoisonError};
use std::time::Duration;

use rama::Context;
use rama::{Layer, Service};
use tansu_sans_io::ApiKey as _;
use tansu_sans_io::consumer::{DynConsumerAssignment, GroupConsumer, MemberAssignment};
use tansu_sans_io::{Body, ErrorCode, Frame, Header, HeartbeatResponse};
use tansu_sans_io::{MetadataResponse, RootMessageMeta};

#[derive(Clone, Default)]
pub struct ConsumerGroupLayer {
    group_id: String,
    topics: Vec<String>,
    metadata: MetadataResponse,
    rebalance_timeout: Option<Duration>,
    session_timeout: Option<Duration>,
    on_assignment: Option<Arc<DynConsumerAssignment>>,
}

impl fmt::Debug for ConsumerGroupLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut shown = f.debug_struct("ConsumerGroupLayer");
        let _ = shown
            .field("group_id", &self.group_id)
            .field("topics", &self.topics);
        let _ = shown.field("metadata", &self.metadata);
        let _ = shown.field("rebalance_timeout", &self.rebalance_timeout);
        let _ = shown.field("session_timeout", &self.session_timeout);
        shown.finish_non_exhaustive()
    }
}

impl ConsumerGroupLayer {
    pub fn new(
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
        metadata: MetadataResponse,
    ) -> Self {
        let (group_id, topics) = (
            group_id.into(),
            topics.into_iter().map(Into::into).collect(),
        );
        Self {
            on_assignment: None,
            session_timeout: None,
            rebalance_timeout: None,
            metadata,
            topics,
            group_id,
        }
    }

    pub fn rebalance_timeout(self, rebalance_timeout: Option<Duration>) -> Self {
        let mut adjusted = self;
        adjusted.rebalance_timeout = rebalance_timeout;
        adjusted
    }

    pub fn session_timeout(self, session_timeout: Option<Duration>) -> Self {
        let mut adjusted = self;
        adjusted.session_timeout = session_timeout;
        adjusted
    }

    pub fn on_assignment(
        self,
        on_assignment: impl Fn(String, MemberAssignment) + 'static + Send + Sync,
    ) -> Self {
        let mut adjusted = self;
        adjusted.on_assignment = Some(Arc::new(on_assignment));
        adjusted
    }
}

impl<S> Layer<S> for ConsumerGroupLayer {
    type Service = ConsumerGroupService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        let consuming = GroupConsumer::builder(self.group_id.clone()).topics(self.topics.clone());
        let consuming = consuming.metadata(self.metadata.clone());
        let consuming = consuming.on_assignment(self.on_assignment.clone()).build();
        let consumer = Arc::new(Mutex::new(consuming));
        let heartbeats = Arc::default();
        ConsumerGroupService {
            heartbeats,
            consumer,
            inner,
        }
    }
}

#[derive(Clone)]
pub struct ConsumerGroupService<S> {
    inner: S,
    consumer: Arc<Mutex<GroupConsumer>>,
    heartbeats: Arc<Mutex<Option<usize>>>,
}

impl<S> ConsumerGroupService<S> {
    pub fn is_leader(&self) -> Result<bool, crate::Error> {
        let consumer = self.consumer.lock()?;
        Ok(consumer.is_leader())
    }

    /// The generation this consumer last saw, or `None` before its first join.
    ///
    /// Exposed so a caller can tell "each consumer has heartbeated a few times"
    /// from "every consumer is heartbeating in the SAME generation" (#195). Only
    /// the second is evidence that a rebalance has settled, and an assignment
    /// sampled without it can straddle two generations.
    pub fn generation(&self) -> Result<Option<i32>, crate::Error> {
        let consumer = self.consumer.lock()?;
        Ok(consumer.generation_id())
    }

    pub fn stable_heartbeat_count(&self) -> Result<Option<usize>, crate::Error> {
        let counted = self.heartbeats.lock()?;
        Ok(*counted)
    }

    pub fn member_assignment(&self) -> Result<Option<MemberAssignment>, crate::Error> {
        let consumer = self.consumer.lock()?;
        Ok(consumer.member_assignment().cloned())
    }

    pub fn rebalance_timeout(&self) -> Result<Duration, crate::Error> {
        let consumer = self.consumer.lock()?;
        Ok(consumer.rebalance_timeout())
    }

    pub fn session_timeout(&self) -> Result<Duration, crate::Error> {
        let consumer = self.consumer.lock()?;
        Ok(consumer.session_timeout())
    }

    pub fn errors(self) -> Result<BTreeMap<ErrorCode, u64>, crate::Error> {
        let consumer = self.consumer.lock()?;
        Ok(consumer.errors()?)
    }
}

impl<S> fmt::Debug for ConsumerGroupService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerGroupService").finish()
    }
}

impl<State, S> Service<State, Option<Body>> for ConsumerGroupService<S>
where
    S: Service<State, Frame, Response = Frame>,
    S::Error: From<tansu_sans_io::Error>
        + for<'a> From<PoisonError<MutexGuard<'a, GroupConsumer>>>
        + for<'a> From<PoisonError<MutexGuard<'a, Option<usize>>>>,
    State: Clone + Send + Sync + 'static,
{
    type Response = Body;

    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        input: Option<Body>,
    ) -> Result<Self::Response, Self::Error> {
        let body = {
            let mut consumer = self.consumer.lock()?;
            let next_action = consumer.next_action(input)?;
            tracing::debug!(?next_action);
            next_action
        };

        let key = body.api_key();
        let declared = RootMessageMeta::messages().requests().get(&key);
        let api_version = declared.map_or(0, |meta| meta.version.valid().end);
        let client_id = Some(String::from(env!("CARGO_PKG_NAME")));
        let header = Header::Request {
            correlation_id: 0,
            api_key: key,
            api_version,
            client_id,
        };
        let request = Frame {
            size: 0,
            header,
            body,
        };

        let answered = self.inner.serve(ctx.clone(), request).await?;
        let output = answered.body;
        tracing::debug!(input.api_name = output.api_name());

        if output.api_key() == HeartbeatResponse::KEY {
            *self.heartbeats.lock()?.get_or_insert_default() += 1;
            let third = match self.session_timeout() {
                Ok(timeout) => timeout.checked_div(3),
                Err(_) => None,
            };
            tokio::time::sleep(third.unwrap_or(Duration::from_secs(3))).await;
        } else {
            _ = self.heartbeats.lock()?.take();
        }

        Ok(output)
    }
}
