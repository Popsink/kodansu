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

//! A `Metadata` request over TCP to a listener served on a task, until the
//! task is cancelled.

use std::net::Ipv4Addr;

use rama::Context;
use rama::Layer as _;
use rama::Service as _;
use tansu_sans_io::{Frame, Header};
use tansu_service::{BytesFrameLayer, BytesTcpService, FrameBytesLayer, FrameService};
use tansu_service::{TcpBytesLayer, TcpContextLayer, TcpListenerLayer};
use tokio::net::{TcpListener, TcpStream};

use common::Error;

mod common;

async fn serve(
    stop: tokio_util::sync::CancellationToken,
    listener: TcpListener,
) -> Result<(), Error> {
    let answer = FrameService::new(|_, req: Frame| {
        tracing::debug!(?req);
        let header = Header::Response {
            correlation_id: req.correlation_id()?,
        };
        Ok::<_, Error>(common::frame(header, common::metadata_response().into()))
    });
    let framed = (TcpBytesLayer::<()>::default(), BytesFrameLayer::default()).into_layer(answer);
    let accepting = TcpListenerLayer::new(stop);
    let server = (accepting, TcpContextLayer::default()).into_layer(framed);
    let serving = server.serve(Context::default(), listener);
    serving.await
}

#[tokio::test]
async fn a_metadata_request_round_trips_over_tcp() -> Result<(), Error> {
    let _guard = common::init_tracing()?;

    let stop = tokio_util::sync::CancellationToken::new();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    let mut tasks = tokio::task::JoinSet::new();
    let _server = tasks.spawn(serve(stop.clone(), listener));

    let stream = TcpStream::connect(address).await?;
    let wire = FrameBytesLayer.into_layer(BytesTcpService);
    let answered = wire.serve(Context::with_state(stream), common::metadata_request());
    common::assert_answered(answered.await?)?;

    stop.cancel();
    let joined = tasks.join_all().await;
    tracing::debug!(?joined);
    Ok(())
}
