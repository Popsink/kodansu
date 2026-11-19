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

use rama::Context;
use rama::Layer;
use rama::Service;
use tansu_sans_io::Frame;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::Error;

/// Wraps a frame service in a [`ChannelFrameService`] that drains a
/// [`FrameReceiver`] until the token is cancelled.
#[derive(Clone, Debug, Default)]
pub struct ChannelFrameLayer {
    cancellation: CancellationToken,
}

impl ChannelFrameLayer {
    pub fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }
}

impl<S> Layer<S> for ChannelFrameLayer {
    type Service = ChannelFrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        let cancellation = self.cancellation.clone();
        ChannelFrameService {
            cancellation,
            inner,
        }
    }
}

/// Serves each frame arriving on a [`FrameReceiver`] with the inner service,
/// replying on the frame's own oneshot, until its token is cancelled.
#[derive(Clone, Debug, Default)]
pub struct ChannelFrameService<S> {
    inner: S,
    cancellation: CancellationToken,
}

/// The receiving half of [`bounded_channel`]: each frame comes with the
/// oneshot its response goes back on.
pub type FrameReceiver = mpsc::Receiver<(Frame, oneshot::Sender<Frame>)>;

impl<S, State> Service<State, FrameReceiver> for ChannelFrameService<S>
where
    S: Service<State, Frame, Response = Frame>,
    State: Clone + Send + Sync + 'static,
    S::Error: From<Error>,
{
    type Response = ();
    type Error = S::Error;

    async fn serve(
        &self,
        ctx: Context<State>,
        mut req: FrameReceiver,
    ) -> Result<Self::Response, Self::Error> {
        let unsendable = |unsent: Frame| S::Error::from(Error::UnableToSend(Box::new(unsent)));
        loop {
            tokio::select! {
                Some((frame, reply)) = req.recv() => {
                    tracing::debug!(?frame, tx = ?reply);
                    let answer = self.inner.serve(ctx.clone(), frame).await?;
                    reply.send(answer).map_err(unsendable)?;
                }

                stopped = self.cancellation.cancelled() => {
                    tracing::debug!(cancelled = ?stopped);
                    return Ok(());
                }
            }
        }
    }
}

/// The sending half of [`bounded_channel`].
pub type FrameSender = mpsc::Sender<(Frame, oneshot::Sender<Frame>)>;

/// Sends each frame down a [`FrameSender`] and waits for the response that
/// comes back on its oneshot.
#[derive(Clone, Debug)]
pub struct FrameChannelService {
    tx: FrameSender,
}

impl FrameChannelService {
    pub fn new(tx: FrameSender) -> Self {
        Self { tx }
    }
}

impl<State> Service<State, Frame> for FrameChannelService
where
    State: Send + Sync + 'static,
{
    type Response = Frame;

    type Error = Error;

    async fn serve(&self, _ctx: Context<State>, req: Frame) -> Result<Self::Response, Self::Error> {
        let (reply, answer) = oneshot::channel();
        let sending = self.tx.send((req, reply));
        let refused = |rejected: mpsc::error::SendError<_>| {
            let (frame, _reply): (Frame, oneshot::Sender<Frame>) = rejected.0;
            Error::UnableToSend(Box::new(frame))
        };
        sending.await.map_err(refused)?;
        answer.await.map_err(Error::OneshotRecv)
    }
}

/// A [`FrameSender`] and [`FrameReceiver`] pair holding up to `buffer` frames in flight.
pub fn bounded_channel(buffer: usize) -> (FrameSender, FrameReceiver) {
    mpsc::channel(buffer)
}
