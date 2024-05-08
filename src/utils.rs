use std::str::FromStr;

use serde::{Deserialize, Serialize};
use sqlx::types::BigDecimal;

#[derive(Debug)]
pub struct Balance(pub BigDecimal);

impl From<BigDecimal> for Balance {
    fn from(value: BigDecimal) -> Self {
        Balance(value)
    }
}

impl Serialize for Balance {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Balance {
    fn deserialize<D>(deserializer: D) -> Result<Balance, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Balance(
            BigDecimal::from_str(&s).map_err(serde::de::Error::custom)?,
        ))
    }
}

#[derive(Debug)]
pub struct OptionalBalance(pub Option<BigDecimal>);

impl From<Option<BigDecimal>> for OptionalBalance {
    fn from(value: Option<BigDecimal>) -> Self {
        OptionalBalance(value)
    }
}

impl Serialize for OptionalBalance {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_ref().map(|v| v.to_string()).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for OptionalBalance {
    fn deserialize<D>(deserializer: D) -> Result<OptionalBalance, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = Option::<String>::deserialize(deserializer)?;
        Ok(OptionalBalance(
            s.map(|s| BigDecimal::from_str(&s).map_err(serde::de::Error::custom))
                .transpose()?,
        ))
    }
}

#[derive(Debug)]
pub struct VecBalance(pub Vec<BigDecimal>);

impl From<Vec<BigDecimal>> for VecBalance {
    fn from(value: Vec<BigDecimal>) -> Self {
        VecBalance(value)
    }
}

impl Serialize for VecBalance {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>()
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for VecBalance {
    fn deserialize<D>(deserializer: D) -> Result<VecBalance, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = Vec::<String>::deserialize(deserializer)?;
        Ok(VecBalance(
            s.into_iter()
                .map(|s| BigDecimal::from_str(&s).map_err(serde::de::Error::custom))
                .collect::<Result<Vec<BigDecimal>, _>>()?,
        ))
    }
}
