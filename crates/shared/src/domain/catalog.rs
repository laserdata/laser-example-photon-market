use super::{ContractVersion, Sku};
use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CatalogCommand {
    #[serde(rename = "v")]
    pub version: ContractVersion,
    #[serde(flatten)]
    pub op: CatalogOp,
}

#[derive(Clone, Debug, Deserialize, Display, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum CatalogOp {
    /// Set a sku's stock to an absolute count: first-time seeding.
    UpsertSku { sku: Sku, available: u32 },
    /// Add stock to whatever a sku already has: a repair or a restock, so two
    /// repairs of the same sku accumulate instead of clobbering each other.
    Restock { sku: Sku, add: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn upsert() -> CatalogCommand {
        CatalogCommand {
            version: ContractVersion::CURRENT,
            op: CatalogOp::UpsertSku {
                sku: "sku-1".parse().expect("sku parses"),
                available: 10,
            },
        }
    }

    #[test]
    fn given_an_upsert_when_serialized_then_should_be_flat_and_internally_tagged() {
        let command = upsert();
        let wire = serde_json::to_value(&command).expect("serialize catalog command");
        assert_eq!(
            wire,
            json!({"v": 1, "op": "upsert_sku", "sku": "sku-1", "available": 10})
        );
        let decoded: CatalogCommand = serde_json::from_value(wire).expect("decode catalog command");
        assert_eq!(decoded, command);
    }

    #[test]
    fn given_a_restock_when_serialized_then_should_be_flat_and_internally_tagged() {
        let command = CatalogCommand {
            version: ContractVersion::CURRENT,
            op: CatalogOp::Restock {
                sku: "sku-1".parse().expect("sku parses"),
                add: 25,
            },
        };
        let wire = serde_json::to_value(&command).expect("serialize catalog command");
        assert_eq!(
            wire,
            json!({"v": 1, "op": "restock", "sku": "sku-1", "add": 25})
        );
        let decoded: CatalogCommand = serde_json::from_value(wire).expect("decode catalog command");
        assert_eq!(decoded, command);
    }
}
