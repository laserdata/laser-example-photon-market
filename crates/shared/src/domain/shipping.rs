use super::{ContractVersion, OrderId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct QuoteRequest {
    #[serde(rename = "v")]
    pub version: ContractVersion,
    pub order: OrderId,
    pub ship_to: String,
    pub quantity: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Quote {
    #[serde(rename = "v")]
    pub version: ContractVersion,
    pub carrier: String,
    pub order: OrderId,
    // Untrusted carrier input: i64 so the verifier can represent and reject a
    // negative wire value before converting it to Money.
    pub price_cents: i64,
    pub eta_days: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BookRequest {
    #[serde(rename = "v")]
    pub version: ContractVersion,
    pub order: OrderId,
    pub carrier: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Booking {
    #[serde(rename = "v")]
    pub version: ContractVersion,
    pub carrier: String,
    pub order: OrderId,
    pub booking: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ReleaseRequest {
    #[serde(rename = "v")]
    pub version: ContractVersion,
    pub order: OrderId,
    pub carrier: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CarrierRequest {
    Quote(QuoteRequest),
    Book(BookRequest),
    Release(ReleaseRequest),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn given_a_quote_with_a_negative_price_when_decoded_then_should_preserve_the_sign() {
        let wire = json!({
            "v": 1,
            "carrier": "borealis",
            "order": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "price_cents": -500,
            "eta_days": 2,
        });
        let quote: Quote = serde_json::from_value(wire).expect("decode quote");
        assert_eq!(quote.price_cents, -500);
    }

    #[test]
    fn given_a_booking_when_round_tripped_then_should_match() {
        let booking = Booking {
            version: ContractVersion::CURRENT,
            carrier: "hermes".to_owned(),
            order: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                .parse()
                .expect("order id parses"),
            booking: "bk-1".to_owned(),
        };
        let wire = serde_json::to_value(&booking).expect("serialize booking");
        let decoded: Booking = serde_json::from_value(wire).expect("decode booking");
        assert_eq!(decoded, booking);
    }
}
