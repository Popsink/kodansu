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

//! A `Metadata` request through a bounded channel to a route served on a
//! task, until the task is cancelled.

use rama::Context;
use rama::Layer as _;
use rama::Service as _;
use tansu_sans_io::MetadataRequest;
use tansu_service::{ChannelFrameLayer, FrameChannelService, FrameReceiver};
use tansu_service::{FrameRouteService, RequestLayer, ResponseService, bounded_channel};

use common::Error;

mod common;

async fn serve(
    stop: tokio_util::sync::CancellationToken,
    requests: FrameReceiver,
) -> Result<(), Error> {
    let answer = ResponseService::new(|_: Context<()>, _: MetadataRequest| {
        Ok::<_, Error>(common::metadata_response())
    });
    let metadata = RequestLayer::<MetadataRequest>::new().into_layer(answer);
    let routes = FrameRouteService::builder().with_service(metadata)?;
    let channel = ChannelFrameLayer::new(stop).into_layer(routes.build()?);
    channel.serve(Context::default(), requests).await
}

#[tokio::test]
async fn a_metadata_request_round_trips_through_a_channel() -> Result<(), Error> {
    let _guard = common::init_tracing()?;

    let (sender, receiver) = bounded_channel(100);
    let stop = tokio_util::sync::CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();
    let _server = tasks.spawn(serve(stop.clone(), receiver));

    let channel = FrameChannelService::new(sender);
    let answered = channel.serve(Context::default(), common::metadata_request());
    common::assert_answered(answered.await?)?;

    stop.cancel();
    let joined = tasks.join_all().await;
    tracing::debug!(?joined);
    Ok(())
}
