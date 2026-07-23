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

use clap::{Args, Subcommand};
use human_units::iec::iec_unit;
use tansu_perf::{ConsumePerf, Perf};
use tansu_sans_io::ErrorCode;
use url::Url;

use crate::{EnvVarExp, Result, cli::DEFAULT_BROKER};

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    /// Produce messages
    Produce {
        /// The partition to produce messages into
        #[arg(long, default_value = "0")]
        partition: i32,

        /// Message batch size used by every producer
        #[arg(long, default_value = "1")]
        batch_size: u32,

        /// Record size used by every producer
        #[arg(long, default_value = "1k", value_parser=clap::value_parser!(human_units::Size))]
        record_size: human_units::Size,

        /// The maximum number of messages per second
        #[clap(long, group = "output")]
        per_second: Option<u32>,

        /// Message throughput
        #[clap(long, group = "output")]
        throughput: Option<Throughput>,

        /// The number of producers generating messages
        #[arg(long, default_value = "1")]
        producers: u32,

        /// Stop sending messages after this time
        #[arg(long, default_value = "1m", value_parser=clap::value_parser!(human_units::Duration))]
        duration: Option<human_units::Duration>,
    },

    /// Consume messages by long-polling a single partition
    Consume {
        /// The partition to consume from
        #[arg(long, default_value = "0")]
        partition: i32,

        /// The offset to start consuming from
        #[arg(long, default_value = "0")]
        offset: i64,

        /// The maximum time in milliseconds the broker holds a fetch open
        /// waiting for new data (mirrors a real client's poll long-poll)
        #[arg(long, default_value = "500")]
        max_wait_ms: i32,

        /// The maximum bytes fetched per request
        #[arg(long, default_value = "1m", value_parser=clap::value_parser!(human_units::Size))]
        max_bytes: human_units::Size,

        /// The number of independent consumers tailing the same partition
        /// from the same offset (fan-out: N consumer groups reading one hot
        /// partition)
        #[arg(long, default_value = "1")]
        consumers: u32,

        /// Stop consuming after this time
        #[arg(long, default_value = "1m", value_parser=clap::value_parser!(human_units::Duration))]
        duration: Option<human_units::Duration>,
    },
}

#[iec_unit(symbol = "B/s")]
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct Throughput(pub u32);

#[derive(Args, Clone, Debug)]
pub(super) struct Arg {
    #[command(subcommand)]
    command: Command,

    /// The URL of the broker
    #[arg(long, default_value = DEFAULT_BROKER, env = "ADVERTISED_LISTENER_URL")]
    broker: EnvVarExp<Url>,

    /// The topic to generate messages into
    #[clap(value_parser)]
    topic: String,
}

impl Arg {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        match self.command {
            Command::Produce {
                partition,
                batch_size,
                record_size,
                per_second,
                throughput,
                producers,
                duration,
            } => Perf::builder()
                .broker(self.broker.into_inner())
                .topic(self.topic)
                .partition(partition)
                .batch_size(batch_size)
                .record_size(record_size.0 as usize)
                .per_second(per_second)
                .throughput(throughput.map(|throughput| throughput.0))
                .producers(producers)
                .duration(duration.map(|duration| duration.0))
                .build()
                .main()
                .await
                .map_err(Into::into),

            Command::Consume {
                partition,
                offset,
                max_wait_ms,
                max_bytes,
                consumers,
                duration,
            } => ConsumePerf::new(self.broker.into_inner(), self.topic)
                .partition(partition)
                .offset(offset)
                .max_wait_ms(max_wait_ms)
                .max_bytes(max_bytes.0 as i32)
                .consumers(consumers)
                .duration(duration.map(|duration| duration.0))
                .main()
                .await
                .map_err(Into::into),
        }
    }
}
