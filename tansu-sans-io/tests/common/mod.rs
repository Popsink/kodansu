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

//! What every test binary in this crate shares: a tracing subscriber writing
//! one log file per test thread under `logs/tansu-sans-io/`.

#![allow(dead_code, reason = "not every test binary here logs")]

use std::fs::File;
use std::sync::Arc;
use std::thread;

use tansu_sans_io::Error;
use tracing::subscriber::DefaultGuard as Guard;

/// Logs this test at `debug` for `tansu_model` and this crate, on top of
/// `RUST_LOG`, into `../logs/<package>/<crate>::<thread>.log`. The thread is
/// named after the test, so each test gets a file of its own.
pub(crate) fn init_tracing() -> tansu_sans_io::Result<Guard> {
    let this_crate = env!("CARGO_PKG_NAME").replace("-", "_");
    let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("tansu_model=debug".parse()?);
    let filter = filter.add_directive(format!("{this_crate}=debug").parse()?);

    let unnamed = || Error::Message(String::from("unnamed thread"));
    let current = thread::current();
    let test = current.name().ok_or_else(unnamed)?;
    let (package, binary) = (env!("CARGO_PKG_NAME"), env!("CARGO_CRATE_NAME"));
    let log = File::create(format!("../logs/{package}/{binary}::{test}.log"))?;

    let subscriber = tracing_subscriber::fmt().with_level(true);
    let subscriber = subscriber.with_line_number(true).with_thread_names(false);
    let subscriber = subscriber.with_target(true).with_env_filter(filter);
    let subscriber = subscriber.with_writer(Arc::new(log));
    Ok(tracing::subscriber::set_default(subscriber.finish()))
}
