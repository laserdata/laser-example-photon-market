pub mod catalog;
pub mod order;
pub mod risk;
pub mod shipping;
pub mod shop;
pub mod ticket;

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ContractVersion(pub u16);

impl ContractVersion {
    pub const CURRENT: Self = Self(1);

    pub fn ensure_current(self) -> Result<(), ContractVersionError> {
        if self == Self::CURRENT {
            Ok(())
        } else {
            Err(ContractVersionError::Unsupported {
                expected: Self::CURRENT,
                got: self,
            })
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ContractVersionError {
    #[error("unsupported contract version {got}, expected {expected}")]
    Unsupported {
        expected: ContractVersion,
        got: ContractVersion,
    },
}

impl Default for ContractVersion {
    fn default() -> Self {
        Self::CURRENT
    }
}

impl fmt::Display for ContractVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Error)]
pub enum IdError {
    #[error("identifier must not be empty")]
    Empty,
    #[error("not a valid ULID: {0}")]
    BadUlid(String),
}

macro_rules! ulid_id {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub Ulid);

        impl $name {
            pub fn new(ulid: Ulid) -> Self {
                Self(ulid)
            }

            pub fn as_u128(self) -> u128 {
                self.0.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Ulid::from_str(value)
                    .map(Self)
                    .map_err(|_| IdError::BadUlid(value.to_owned()))
            }
        }
    };
}

ulid_id!(OrderId);
ulid_id!(TicketId);
ulid_id!(SessionId);

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                if value.trim().is_empty() {
                    return Err(IdError::Empty);
                }
                Ok(Self(value.to_owned()))
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

string_id!(CustomerId);
string_id!(Sku);

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub struct Money(pub u64);

impl Money {
    pub fn cents(self) -> u64 {
        self.0
    }

    pub fn checked_mul(self, quantity: u32) -> Option<Self> {
        self.0.checked_mul(u64::from(quantity)).map(Self)
    }
}

impl fmt::Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${}.{:02}", self.0 / 100, self.0 % 100)
    }
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub fn from_micros(micros: u64) -> Self {
        Self(micros)
    }

    pub fn as_micros(self) -> u64 {
        self.0
    }

    pub fn now() -> Self {
        let micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the unix epoch")
            .as_micros() as u64;
        Self(micros)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_ulid_id_when_parsed_and_rendered_then_should_round_trip() {
        let ulid =
            Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("the fixture ulid parses");
        let order = OrderId::new(ulid);
        assert_eq!(
            order
                .to_string()
                .parse::<OrderId>()
                .expect("the rendered order id parses"),
            order
        );
    }

    #[test]
    fn given_a_non_ulid_string_when_parsed_then_should_reject_it() {
        assert!(matches!(
            "not-a-ulid".parse::<OrderId>(),
            Err(IdError::BadUlid(_))
        ));
    }

    #[test]
    fn given_an_empty_string_when_parsed_as_a_string_id_then_should_reject_it() {
        assert!(matches!("".parse::<Sku>(), Err(IdError::Empty)));
    }

    #[test]
    fn given_money_when_rendered_then_should_show_dollars_and_cents() {
        assert_eq!(Money(12_345).to_string(), "$123.45");
        assert_eq!(Money(5).to_string(), "$0.05");
    }
}
