use super::{ContractVersion, CustomerId, Money, SessionId, Sku, Timestamp};
use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ShopEvent {
    #[serde(rename = "v")]
    pub version: ContractVersion,
    pub session: SessionId,
    pub customer: CustomerId,
    pub kind: ShopEventKind,
    pub sku: Sku,
    #[serde(rename = "price_cents")]
    pub price: Money,
    #[serde(rename = "at_micros")]
    pub at: Timestamp,
}

#[derive(Clone, Copy, Debug, Deserialize, Display, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ShopEventKind {
    Browse,
    Cart,
    Checkout,
    Abandon,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_shop_event_when_round_tripped_then_should_match() {
        let event = ShopEvent {
            version: ContractVersion::CURRENT,
            session: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                .parse()
                .expect("session id parses"),
            customer: "cust-1".parse().expect("customer id parses"),
            kind: ShopEventKind::Checkout,
            sku: "sku-1".parse().expect("sku parses"),
            price: Money(1999),
            at: Timestamp::from_micros(7),
        };
        let wire = serde_json::to_value(&event).expect("serialize shop event");
        let decoded: ShopEvent = serde_json::from_value(wire).expect("decode shop event");
        assert_eq!(decoded, event);
    }
}
