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

//! Produces one record, with the key and value given, to a topic partition.

use clap::Parser as _;
use tansu_client::Error;
use tansu_sans_io::produce_request::{PartitionProduceData, TopicProduceData};
use tansu_sans_io::record::{Record, deflated, inflated};
use tansu_sans_io::{Ack, ProduceRequest};

mod common;

/// Produce
#[derive(Clone, Debug, clap::Parser)]
#[command(version, long_about = None)]
struct Options {
    #[arg(long, default_value = common::BROKER)]
    broker: url::Url,

    #[arg(id = "topic", value_name = "TOPIC")]
    name: String,

    #[arg(long = "partition", value_name = "PARTITION", default_value = "0")]
    index: i32,

    #[arg(long = "key", value_name = "KEY")]
    record_key: Option<String>,

    #[arg(long = "value", value_name = "VALUE")]
    record_value: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Error> {
    common::start();
    let options = Options::parse();
    let client = common::connect(options.broker).await?;

    let bytes = |text: String| bytes::Bytes::from(text.into_bytes());
    let record = Record::builder().key(options.record_key.map(bytes));
    let record = record.value(options.record_value.map(bytes));
    let batches = vec![inflated::Batch::builder().record(record).build()?];
    let records = deflated::Frame::try_from(inflated::Frame { batches })?;

    let partition = PartitionProduceData::default().index(options.index);
    let partition = partition.records(Some(records));
    let topic = TopicProduceData::default().name(options.name);
    let topic = topic.partition_data(Some(vec![partition]));
    let request = ProduceRequest::default().timeout_ms(5_000);
    let request = request.acks(Ack::Leader.into());
    let response = client.call(request.topic_data(Some(vec![topic]))).await?;
    println!("{response:?}");
    Ok(())
}
