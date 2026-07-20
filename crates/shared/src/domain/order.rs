use super::{ContractVersion, CustomerId, Money, OrderId, Sku, Timestamp};
use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PlaceOrder {
    #[serde(rename = "v")]
    pub version: ContractVersion,
    pub order: OrderId,
    pub customer: CustomerId,
    pub sku: Sku,
    pub quantity: u32,
    #[serde(rename = "unit_price_cents")]
    pub unit_price: Money,
    pub device: String,
    pub ship_to: String,
    pub card_fingerprint: String,
    #[serde(rename = "placed_at_micros")]
    pub placed_at: Timestamp,
}

impl PlaceOrder {
    pub fn total(&self) -> Option<Money> {
        self.unit_price.checked_mul(self.quantity)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OrderEvent {
    #[serde(rename = "v")]
    pub version: ContractVersion,
    pub order: OrderId,
    pub customer: CustomerId,
    pub sku: Sku,
    pub quantity: u32,
    #[serde(rename = "at_micros")]
    pub at: Timestamp,
    pub kind: OrderEventKind,
}

impl OrderEvent {
    pub fn new(command: &PlaceOrder, at: Timestamp, kind: OrderEventKind) -> Self {
        Self {
            version: ContractVersion::CURRENT,
            order: command.order,
            customer: command.customer.clone(),
            sku: command.sku.clone(),
            quantity: command.quantity,
            at,
            kind,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Display, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum OrderEventKind {
    Received,
    InventoryReserved,
    InventoryReleased,
    Accepted,
    Rejected {
        reason: String,
    },
    Charged {
        #[serde(rename = "amount_cents")]
        amount: Money,
    },
    ChargeRefunded {
        #[serde(rename = "amount_cents")]
        amount: Money,
    },
    ShipmentBooked {
        carrier: String,
        booking: String,
    },
    BookingReleased {
        carrier: String,
        booking: String,
    },
    Shipped {
        carrier: String,
        tracking: String,
    },
    Delivered,
    Refunded {
        #[serde(rename = "amount_cents")]
        amount: Money,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn command() -> PlaceOrder {
        PlaceOrder {
            version: ContractVersion::CURRENT,
            order: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                .parse()
                .expect("order id parses"),
            customer: "cust-1".parse().expect("customer id parses"),
            sku: "sku-1".parse().expect("sku parses"),
            quantity: 2,
            unit_price: Money(1999),
            device: "device-1".to_owned(),
            ship_to: "addr-1".to_owned(),
            card_fingerprint: "card-1".to_owned(),
            placed_at: Timestamp::from_micros(1),
        }
    }

    #[test]
    fn given_a_charged_order_event_when_serialized_then_should_match_the_wire_contract() {
        let event = OrderEvent::new(
            &command(),
            Timestamp::from_micros(2),
            OrderEventKind::Charged {
                amount: Money(3998),
            },
        );
        let wire = serde_json::to_value(&event).expect("serialize order event");
        assert_eq!(
            wire,
            json!({
                "v": 1,
                "order": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                "customer": "cust-1",
                "sku": "sku-1",
                "quantity": 2,
                "at_micros": 2,
                "kind": {"type": "charged", "amount_cents": 3998},
            })
        );
        let decoded: OrderEvent = serde_json::from_value(wire).expect("decode order event");
        assert_eq!(decoded, event);
    }
}
