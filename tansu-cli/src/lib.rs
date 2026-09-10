// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{collections::HashMap, env::vars, fmt, result, str::FromStr};

mod cli;

#[cfg(test)]
mod harness;

pub use cli::Cli;
use regex::{Regex, Replacer};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    AddrParse(#[from] std::net::AddrParseError),
    Box(#[from] Box<dyn std::error::Error + Send + Sync>),
    Client(Box<tansu_client::Error>),
    DotEnv(#[from] dotenv::Error),
    InvalidLength(#[from] sha2::digest::InvalidLength),
    Regex(#[from] regex::Error),
    SansIo(#[from] tansu_sans_io::Error),
    Server(Box<tansu_broker::Error>),
    Tls(#[from] rustls::Error),
    TlsPkiPem(#[from] rustls::pki_types::pem::Error),
    Topic(#[from] tansu_topic::Error),
    Url(#[from] url::ParseError),
}

impl From<tansu_client::Error> for Error {
    fn from(value: tansu_client::Error) -> Self {
        Self::Client(Box::new(value))
    }
}

impl From<tansu_broker::Error> for Error {
    fn from(value: tansu_broker::Error) -> Self {
        Self::Server(Box::new(value))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Clone, Debug)]
pub struct VarRep(HashMap<String, String>);

impl From<HashMap<String, String>> for VarRep {
    fn from(value: HashMap<String, String>) -> Self {
        Self(value)
    }
}

impl VarRep {
    fn replace(&self, haystack: &str) -> Result<String> {
        Regex::new(r"\$\{(?<var>[^\}]+)\}")
            .map(|re| re.replace(haystack, self).into_owned())
            .map_err(Into::into)
    }
}

impl Replacer for &VarRep {
    fn replace_append(&mut self, caps: &regex::Captures<'_>, dst: &mut String) {
        if let Some(variable) = caps.name("var")
            && let Some(value) = self.0.get(variable.as_str())
        {
            dst.push_str(value);
        }
    }
}

#[derive(Clone, Debug)]
pub struct EnvVarExp<T>(T);

impl<T> EnvVarExp<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> FromStr for EnvVarExp<T>
where
    T: FromStr,
    Error: From<<T as FromStr>::Err>,
{
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        VarRep::from(vars().collect::<HashMap<_, _>>())
            .replace(s)
            .and_then(|s| T::from_str(&s).map_err(Into::into))
            .map(|t| Self(t))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    fn var_rep(pairs: [(&str, &str); 2]) -> VarRep {
        VarRep::from(
            pairs
                .into_iter()
                .map(|(name, value)| (String::from(name), String::from(value)))
                .collect::<HashMap<_, _>>(),
        )
    }

    /// The `${VAR}` form is the only one expanded: a bare `$VAR` and a
    /// `%VAR%` are left alone, because the values these appear in are URLs
    /// and a `$` in a password is not a reference.
    #[test]
    fn only_the_braced_form_is_a_reference() -> Result<()> {
        let vars = var_rep([("BUCKET", "tansu"), ("PORT", "9092")]);

        assert_eq!("s3://tansu/", vars.replace("s3://${BUCKET}/")?);
        assert_eq!("s3://$BUCKET/", vars.replace("s3://$BUCKET/")?);
        assert_eq!("s3://%BUCKET%/", vars.replace("s3://%BUCKET%/")?);

        Ok(())
    }

    /// An unset variable expands to nothing rather than being left in place.
    /// It is the behaviour that makes `tcp://0.0.0.0:${PORT}` with no `PORT`
    /// fail on the URL — visibly, at startup — instead of a broker listening
    /// on a port nobody asked for.
    #[test]
    fn an_unset_variable_expands_to_nothing() -> Result<()> {
        let vars = var_rep([("BUCKET", "tansu"), ("PORT", "9092")]);

        assert_eq!("s3:///", vars.replace("s3://${MISSING}/")?);

        Ok(())
    }

    /// `Regex::replace` replaces the *first* match only, which is what the
    /// arguments this is used on need — one reference each — and is worth
    /// pinning because it is not what `replace_all` would do.
    #[test]
    fn the_first_reference_is_the_one_expanded() -> Result<()> {
        let vars = var_rep([("BUCKET", "tansu"), ("PORT", "9092")]);

        assert_eq!("tansu:${PORT}", vars.replace("${BUCKET}:${PORT}")?);

        Ok(())
    }

    /// The wrapper is what `--storage-engine 's3://${BUCKET}/'` goes through,
    /// so it has to expand *before* the inner type parses. A URL that would
    /// not parse un-expanded is the assertion: `memory://${UNSET}tansu/`
    /// reaches `Url` as `memory://tansu/`.
    #[test]
    fn the_wrapper_expands_before_the_inner_type_parses() -> Result<()> {
        assert_eq!(
            Url::parse("memory://tansu/")?,
            EnvVarExp::<Url>::from_str("memory://${TANSU_TEST_UNSET_VARIABLE}tansu/")?.into_inner()
        );

        Ok(())
    }

    /// A value with no reference in it is passed through untouched, which is
    /// every argument in a deployment that does not use the feature.
    #[test]
    fn a_value_with_no_reference_is_passed_through() -> Result<()> {
        assert_eq!(
            Url::parse("tcp://localhost:9092")?,
            EnvVarExp::<Url>::from_str("tcp://localhost:9092")?.into_inner()
        );

        Ok(())
    }

    /// The inner type's parse failure is the wrapper's error, not a panic and
    /// not a silent default.
    #[test]
    fn the_inner_type_still_has_to_parse() {
        assert!(matches!(
            EnvVarExp::<Url>::from_str("not a url"),
            Err(Error::Url(_))
        ));
    }

    /// `Display` is `Debug`, which is what makes `main`'s error line name the
    /// variant and its source rather than printing nothing.
    #[test]
    fn an_error_displays_as_its_debug_form() {
        let error = Error::from(Url::parse("not a url").expect_err("not a url"));

        assert_eq!(format!("{error:?}"), format!("{error}"));
        assert!(format!("{error}").contains("RelativeUrlWithoutBase"));
    }
}
