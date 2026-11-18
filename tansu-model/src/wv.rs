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

use std::num::TryFromIntError;

use serde_json::Value;

use crate::Error;
use crate::{Kind, MessageKind, Result, VersionRange};

/// A JSON value, wrapped so that a field of it reads out as a typed value:
/// with [`As`] when the field must be there, with [`AsOption`] when it may not.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Wv<'a>(&'a Value);

impl<'a> Wv<'a> {
    /// The field `name`, `Value::Null` when there is none.
    fn field(&self, name: &str) -> &'a Value {
        let whole: &'a Value = self.0;
        &whole[name]
    }
}

/// The error for a field `name` that is absent, or not of the type asked for.
fn missing(name: &str) -> Error {
    Error::Message(name.to_owned())
}

/// `value` narrowed into `T`, an error if it does not fit, none if it was none.
fn narrowed<W, T>(value: Option<W>) -> Result<Option<T>>
where
    T: TryFrom<W, Error = TryFromIntError>,
{
    let Some(wide) = value else {
        return Ok(None);
    };
    Ok(Some(T::try_from(wide)?))
}

impl<'a, 'b: 'a> From<&'b Value> for Wv<'a> {
    fn from(value: &'b Value) -> Self {
        Wv(value)
    }
}

pub trait As<'a, T>
where
    T: 'a,
{
    fn as_a(&'a self, name: &str) -> Result<T>;
}

impl<'a> As<'a, &'a str> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<&'a str> {
        let found: Option<&'a str> = self.as_option(name)?;
        found.ok_or_else(|| missing(name))
    }
}

impl<'a> As<'a, String> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<String> {
        let found: Option<String> = self.as_option(name)?;
        found.ok_or_else(|| missing(name))
    }
}

impl<'a> As<'a, &'a [Value]> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<&'a [Value]> {
        let found = self.field(name).as_array();
        found.map(Vec::as_slice).ok_or_else(|| missing(name))
    }
}

impl<'a> As<'a, u64> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<u64> {
        let found = self.field(name).as_u64();
        found.ok_or_else(|| missing(name))
    }
}

impl<'a> As<'a, u32> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<u32> {
        let wide: u64 = self.as_a(name)?;
        Ok(u32::try_from(wide)?)
    }
}

impl<'a> As<'a, u16> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<u16> {
        let wide: u64 = self.as_a(name)?;
        Ok(u16::try_from(wide)?)
    }
}

impl<'a> As<'a, i64> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<i64> {
        let found = self.field(name).as_i64();
        found.ok_or_else(|| missing(name))
    }
}

impl<'a> As<'a, i16> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<i16> {
        let wide: i64 = self.as_a(name)?;
        Ok(i16::try_from(wide)?)
    }
}

impl<'a> As<'a, MessageKind> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<MessageKind> {
        let text: &str = self.as_a(name)?;
        text.parse()
    }
}

impl<'a> As<'a, VersionRange> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<VersionRange> {
        let text: &str = self.as_a(name)?;
        text.parse()
    }
}

impl<'a> As<'a, Kind> for Wv<'a> {
    fn as_a(&'a self, name: &str) -> Result<Kind> {
        let text: &str = self.as_a(name)?;
        text.parse()
    }
}

pub trait AsOption<'a, T>
where
    T: 'a,
{
    fn as_option(&'a self, name: &str) -> Result<Option<T>>;
}

impl<'a> AsOption<'a, bool> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<bool>> {
        let field = self.field(name);
        Ok(field.as_bool())
    }
}

impl<'a> AsOption<'a, u64> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<u64>> {
        let field = self.field(name);
        Ok(field.as_u64())
    }
}

impl<'a> AsOption<'a, u32> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<u32>> {
        narrowed(self.field(name).as_u64())
    }
}

impl<'a> AsOption<'a, u16> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<u16>> {
        narrowed(self.field(name).as_u64())
    }
}

impl<'a> AsOption<'a, u8> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<u8>> {
        narrowed(self.field(name).as_u64())
    }
}

impl<'a> AsOption<'a, i64> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<i64>> {
        let field = self.field(name);
        Ok(field.as_i64())
    }
}

impl<'a> AsOption<'a, i32> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<i32>> {
        narrowed(self.field(name).as_i64())
    }
}

impl<'a> AsOption<'a, i16> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<i16>> {
        narrowed(self.field(name).as_i64())
    }
}

impl<'a> AsOption<'a, i8> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<i8>> {
        narrowed(self.field(name).as_i64())
    }
}

impl<'a> AsOption<'a, &'a str> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<&'a str>> {
        let field = self.field(name);
        Ok(field.as_str())
    }
}

impl<'a> AsOption<'a, String> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<String>> {
        let field = self.field(name);
        Ok(field.as_str().map(str::to_owned))
    }
}

impl<'a> AsOption<'a, &'a [Value]> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<&'a [Value]>> {
        let field = self.field(name);
        Ok(field.as_array().map(Vec::as_slice))
    }
}

impl<'a> AsOption<'a, VersionRange> for Wv<'a> {
    fn as_option(&'a self, name: &str) -> Result<Option<VersionRange>> {
        let Some(text) = self.field(name).as_str() else {
            return Ok(None);
        };
        text.parse().map(Some)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use serde_json::{Value, json};

    use super::{As as _, AsOption, Wv};
    use crate::{Error, MessageKind, Result, VersionRange};

    /// Every `(field, expected)` of `cases`, read out of `wv` by `read`.
    fn each<'a, 'b, T: 'a + PartialEq + Debug>(
        wv: &'a Wv<'b>,
        read: impl Fn(&'a Wv<'b>, &'static str) -> Result<Option<T>>,
        cases: &[(&'static str, Option<T>)],
    ) -> Result<(), Error> {
        for (field, expected) in cases {
            assert_eq!(*expected, read(wv, field)?, "field: {field}");
        }
        Ok(())
    }

    /// Every integer type's bounds, the way a message descriptor holds them.
    fn bounds() -> Value {
        json!({
            "u64": u64::MAX,
            "u32": u32::MAX,
            "u16": u16::MAX,
            "u8": u8::MAX,
            "i64_max": i64::MAX,
            "i64_min": i64::MIN,
            "i32_max": i32::MAX,
            "i32_min": i32::MIN,
            "i16_max": i16::MAX,
            "i16_min": i16::MIN,
            "i8_max": i8::MAX,
            "i8_min": i8::MIN,
        })
    }

    #[test]
    fn a_string_field_reads_as_a_str_and_as_a_string() -> Result<()> {
        let greeting = json!({"hello": "world"});
        let wv = Wv::from(&greeting);
        let borrowed: &str = wv.as_a("hello")?;
        let owned: String = wv.as_a("hello")?;
        assert_eq!(("world", "world"), (borrowed, owned.as_str()));
        Ok(())
    }

    #[test]
    fn a_type_field_reads_as_a_message_kind() -> Result<()> {
        let descriptor = json!({"type": "request"});
        let kind: MessageKind = Wv::from(&descriptor).as_a("type")?;
        assert_eq!(MessageKind::Request, kind);
        Ok(())
    }

    /// A number reads as an unsigned option where it is not negative, and as
    /// a narrower one where it also fits; past that, narrowing is an error.
    #[test]
    fn a_number_reads_as_an_unsigned_option_where_it_fits() -> Result<()> {
        let bounds = bounds();
        let wv = Wv::from(&bounds);

        let as_u64 = <Wv<'_> as AsOption<u64>>::as_option;
        let unsigned = [
            ("u64", Some(u64::MAX)),
            ("u32", Some(u64::from(u32::MAX))),
            ("u16", Some(u64::from(u16::MAX))),
            ("u8", Some(u64::from(u8::MAX))),
            ("i64_max", u64::try_from(i64::MAX).ok()),
            ("i64_min", u64::try_from(i64::MIN).ok()),
            ("i32_max", u64::try_from(i32::MAX).ok()),
            ("i32_min", u64::try_from(i32::MIN).ok()),
            ("i16_max", u64::try_from(i16::MAX).ok()),
            ("i16_min", u64::try_from(i16::MIN).ok()),
            ("i8_max", u64::try_from(i8::MAX).ok()),
            ("i8_min", u64::try_from(i8::MIN).ok()),
        ];
        each(&wv, as_u64, &unsigned)?;

        let as_u32 = <Wv<'_> as AsOption<u32>>::as_option;
        for too_wide in ["u64", "i64_max"] {
            let narrowing = as_u32(&wv, too_wide);
            assert!(matches!(narrowing, Err(Error::TryFromInt(_))), "{too_wide}");
        }
        let narrower = [
            ("u32", Some(u32::MAX)),
            ("u16", Some(u32::from(u16::MAX))),
            ("u8", Some(u32::from(u8::MAX))),
            ("i64_min", u32::try_from(i64::MIN).ok()),
            ("i32_max", u32::try_from(i32::MAX).ok()),
            ("i32_min", u32::try_from(i32::MIN).ok()),
            ("i16_max", u32::try_from(i16::MAX).ok()),
            ("i16_min", u32::try_from(i16::MIN).ok()),
            ("i8_max", u32::try_from(i8::MAX).ok()),
            ("i8_min", u32::try_from(i8::MIN).ok()),
        ];
        each(&wv, as_u32, &narrower)
    }

    #[test]
    fn a_number_field_reads_as_a_u64() -> Result<()> {
        let number = json!({"value": 32123});
        let read: u64 = Wv::from(&number).as_a("value")?;
        assert_eq!(32123, read);
        Ok(())
    }

    #[test]
    fn a_version_field_reads_as_a_range() -> Result<()> {
        let descriptor = json!({"flexibleVersions": "2+"});
        let flexible: VersionRange = Wv::from(&descriptor).as_a("flexibleVersions")?;
        let (start, end) = (2, i16::MAX);
        assert_eq!(VersionRange { start, end }, flexible);
        Ok(())
    }

    #[test]
    fn an_array_field_reads_as_a_slice() -> Result<()> {
        let listed = json!({"x": [1, 2, 3]});
        let wv = Wv::from(&listed);
        let values: &[Value] = wv.as_a("x")?;
        assert_eq!([1, 2, 3].map(Value::from), values);
        Ok(())
    }
}
