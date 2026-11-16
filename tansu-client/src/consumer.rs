// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! A consumer group member as a [`rama`] service: a [`GroupConsumer`] picks
//! each request, and the inner frame service carries it to the broker.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use rama::Context;
use tansu_sans_io::consumer::{DynConsumerAssignment, GroupConsumer};
use tansu_sans_io::{Body, Frame, Header, MetadataResponse, RootMessageMeta};

/// What a request frame carries in `size` and `correlation_id` when it leaves
/// here. The layers below fill both in: [`FrameConnectionService`] takes the
/// correlation id from its connection, and the size is written on encoding.
///
/// [`FrameConnectionService`]: crate::FrameConnectionService
const UNSET: i32 = 0;

/// Wraps a frame service in a [`ConsumerGroupService`] that joins `group_id`
/// and subscribes to `topics`, as `metadata` describes them.
#[derive(Clone, Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConsumerGroupLayer {
    group_id: Box<str>,
    topics: Box<[String]>,
    metadata: Arc<MetadataResponse>,
}

impl ConsumerGroupLayer {
    pub fn new<G, T>(group_id: G, topics: T, metadata: MetadataResponse) -> Self
    where
        G: Into<String>,
        T: IntoIterator<Item: Into<String>>,
    {
        let group_id = group_id.into().into_boxed_str();
        let topics = topics.into_iter().map(Into::into).collect();
        let metadata = Arc::new(metadata);
        Self {
            group_id,
            topics,
            metadata,
        }
    }
}

impl<Inner> rama::Layer<Inner> for ConsumerGroupLayer {
    type Service = ConsumerGroupService<Inner>;

    fn layer(&self, inner: Inner) -> ConsumerGroupService<Inner> {
        let hook: Arc<DynConsumerAssignment> =
            Arc::new(|group, assignment| tracing::debug!(group, %assignment));
        let builder = GroupConsumer::builder(self.group_id.clone());
        let builder = builder.topics(self.topics.clone());
        let builder = builder.metadata(MetadataResponse::clone(&self.metadata));
        let member = Arc::new(Mutex::new(builder.on_assignment(Some(hook)).build()));
        ConsumerGroupService { inner, member }
    }
}

/// A member of the group [`ConsumerGroupLayer`] names. Serving it plays the
/// group protocol through the inner service until that service fails.
pub struct ConsumerGroupService<Inner> {
    inner: Inner,
    member: Arc<Mutex<GroupConsumer>>,
}

impl<Inner: Clone> Clone for ConsumerGroupService<Inner> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        let member = Arc::clone(&self.member);
        Self { inner, member }
    }
}

impl<Inner> fmt::Debug for ConsumerGroupService<Inner> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ConsumerGroupService")
    }
}

impl<Inner> ConsumerGroupService<Inner> {
    /// The member's answer to `input`, framed at the newest version of its
    /// API that this crate knows.
    fn next_request<E>(&self, input: Option<Body>) -> Result<Frame, E>
    where
        E: From<tansu_sans_io::Error>,
        E: for<'a> From<PoisonError<MutexGuard<'a, GroupConsumer>>>,
    {
        let next_action = self.member.lock()?.next_action(input)?;
        tracing::debug!(?next_action);

        let key = next_action.api_key();
        let known = RootMessageMeta::messages().requests().get(&key);
        let version = known.map_or(0, |meta| meta.version.valid().end);
        let header = Header::Request {
            api_key: key,
            api_version: version,
            correlation_id: UNSET,
            client_id: Some(String::from(env!("CARGO_PKG_NAME"))),
        };
        let body = next_action;
        Ok(Frame {
            size: UNSET,
            header,
            body,
        })
    }
}

impl<State, Inner> rama::Service<State, ()> for ConsumerGroupService<Inner>
where
    State: Clone + Send + Sync + 'static,
    Inner: rama::Service<State, Frame, Response = Frame>,
    Inner::Error: From<tansu_sans_io::Error>,
    Inner::Error: for<'a> From<PoisonError<MutexGuard<'a, GroupConsumer>>>,
{
    type Response = ();
    type Error = Inner::Error;

    async fn serve(&self, ctx: Context<State>, (): ()) -> Result<(), Inner::Error> {
        let mut input: Option<Body> = None;
        loop {
            tracing::debug!(?input);
            let request = self.next_request::<Inner::Error>(input)?;
            let answered = self.inner.serve(ctx.clone(), request);
            let body = answered.await?.body;
            tracing::debug!(input.api_name = body.api_name());
            input = Some(body);
        }
    }
}
