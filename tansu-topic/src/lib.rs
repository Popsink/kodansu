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

use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::result;
use std::sync::Arc;

use tansu_sans_io::ErrorCode;
use url::Url;

use crate::create::Create;
use crate::delete::Delete;
use crate::list::List;

mod create;
mod delete;
mod list;

pub type Result<T, E = Error> = result::Result<T, E>;

/// `Clone` is why `Io` and `SerdeJson` hold an `Arc`: neither source is
/// cloneable, and every error type in this tree is (`CLAUDE.md`). The wrapping
/// was here and the derive was not until #556's tests asked for it.
#[derive(Clone, thiserror::Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    Client(#[from] tansu_client::Error),
    Io(Arc<io::Error>),
    Protocol(#[from] tansu_sans_io::Error),
    SerdeJson(Arc<serde_json::Error>),
}

/// A client of `broker`, identifying itself by this crate's name.
pub(crate) async fn connected(broker: &Url) -> Result<tansu_client::Client> {
    let manager = tansu_client::ConnectionManager::builder(broker.clone());
    let manager = manager.client_id(Some(String::from(env!("CARGO_PKG_NAME"))));
    let pool = manager.build().await?;
    tracing::debug!(?pool);
    Ok(tansu_client::Client::new(pool))
}

/// The code of the one result a single-topic request is answered with.
///
/// # Panics
///
/// When the broker answers with any other number of results.
pub(crate) fn sole_code(codes: &[i16]) -> Result<ErrorCode> {
    assert_eq!(1, codes.len());
    ErrorCode::try_from(codes[0]).map_err(Error::from)
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeJson(Arc::new(value))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Topic {
    Create(create::Configuration),
    Delete(delete::Configuration),
    List(list::Configuration),
}

impl Topic {
    pub fn create() -> create::Builder<PhantomData<Url>, PhantomData<String>, PhantomData<i32>> {
        create::Builder::default()
    }

    pub fn delete() -> delete::Builder<PhantomData<Url>, PhantomData<String>> {
        delete::Builder::default()
    }

    pub fn list() -> list::Builder<PhantomData<Url>> {
        list::Builder::default()
    }

    pub async fn main(self) -> Result<ErrorCode> {
        match self {
            Self::Create(wanted) => Create::try_from(wanted)?.main().await,
            Self::Delete(named) => Delete::try_from(named)?.main().await,
            Self::List(listing) => List::try_from(listing)?.main().await,
        }
    }
}
