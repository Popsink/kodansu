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

//! Prints the broker's `Metadata` for the topics named, or for all of them.

use clap::Parser as _;
use tansu_client::Error;

mod common;

/// Metadata
#[derive(Clone, Debug, clap::Parser)]
#[command(version, long_about = None)]
struct Options {
    #[arg(long, default_value = common::BROKER)]
    broker: url::Url,

    #[arg(id = "topics", value_name = "TOPICS")]
    names: Option<Vec<String>>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Error> {
    common::start();
    let options = Options::parse();
    let client = common::connect(options.broker).await?;

    let topics = options
        .names
        .map(|names| names.into_iter().map(common::named).collect());
    let response = client.call(common::metadata(topics)).await?;
    println!("{response:?}");
    Ok(())
}
