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

use std::fmt::{self, Display};
use std::sync::Arc;
use std::{io, num, str, string};

use serde::de;
use serde::ser;

#[derive(Clone, Debug)]
pub enum Error {
    DryPop,
    EmptyStack,
    FromUtf8(string::FromUtf8Error),
    Io(Arc<io::Error>),
    Json(Arc<serde_json::Error>),
    Message(String),
    NoCurrentFieldMeta,
    NoMessageMeta,
    NoSuchField(&'static str),
    NoSuchMessage(&'static str),
    ParseBool(str::ParseBoolError),
    ParseInt(num::ParseIntError),
    Syn(syn::Error),
    TryFromInt(num::TryFromIntError),
    Utf8(str::Utf8Error),
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        let text = format!("{msg}");
        Error::Message(text)
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        let text = format!("{msg}");
        Error::Message(text)
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        let shared = Arc::new(value);
        Error::Io(shared)
    }
}

impl From<str::Utf8Error> for Error {
    fn from(value: str::Utf8Error) -> Self {
        Error::Utf8(value)
    }
}

impl From<string::FromUtf8Error> for Error {
    fn from(value: string::FromUtf8Error) -> Self {
        Error::FromUtf8(value)
    }
}

impl From<num::TryFromIntError> for Error {
    fn from(value: num::TryFromIntError) -> Self {
        Error::TryFromInt(value)
    }
}

impl From<str::ParseBoolError> for Error {
    fn from(value: str::ParseBoolError) -> Self {
        Error::ParseBool(value)
    }
}

impl From<num::ParseIntError> for Error {
    fn from(value: num::ParseIntError) -> Self {
        Error::ParseInt(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        let shared = Arc::new(value);
        Error::Json(shared)
    }
}

impl From<syn::Error> for Error {
    fn from(value: syn::Error) -> Self {
        Error::Syn(value)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Error::Message(text) = self else {
            return f.write_fmt(format_args!("{self:?}"));
        };
        f.write_str(text)
    }
}
