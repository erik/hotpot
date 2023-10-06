use std::fmt::Formatter;
use std::marker::PhantomData;

use serde::de::Error;
use serde::Deserializer;
use time::Date;

struct Visitor<T>(PhantomData<T>);

pub mod parse {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Date>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(Visitor::<Option<Date>>(PhantomData))
    }
}

impl<'de> serde::de::Visitor<'de> for Visitor<Option<Date>> {
    type Value = Option<Date>;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a date formatted in YYYY-MM-DD")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Date::parse(
            v,
            &time::format_description::well_known::iso8601::Iso8601::DATE,
        )
        .map_err(Error::custom)
        .map(Some)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(None)
    }

    fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        d.deserialize_str(self)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(None)
    }
}
