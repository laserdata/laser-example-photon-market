use super::{ContractVersion, CustomerId, OrderId, Timestamp};
use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ScreenRequest {
    #[serde(rename = "v")]
    pub version: ContractVersion,
    pub order: OrderId,
    pub customer: CustomerId,
    pub amount_cents: u64,
    pub device: String,
    pub card_fingerprint: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RiskCaseEvent {
    #[serde(rename = "v")]
    pub version: ContractVersion,
    pub order: OrderId,
    pub customer: CustomerId,
    // The shared identifiers carried onto the case record, so the risk-link
    // fold can rebuild the ring graph from the event log alone.
    pub device: String,
    pub card_fingerprint: String,
    pub score_bps: u16,
    pub verdict: RiskVerdict,
    pub reasons: Vec<RiskReason>,
    #[serde(rename = "at_micros")]
    pub at: Timestamp,
}

#[derive(Clone, Copy, Debug, Deserialize, Display, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum RiskVerdict {
    Clear,
    Review,
    Reject,
}

#[derive(Clone, Copy, Debug, Deserialize, Display, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum RiskReason {
    HighValue,
    DeviceRing,
    AddressRing,
    CardRing,
    PrecedentReject,
    ManualReview,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_risk_event_when_round_tripped_then_should_match() {
        let event = RiskCaseEvent {
            version: ContractVersion::CURRENT,
            order: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                .parse()
                .expect("order id parses"),
            customer: "cust-1".parse().expect("customer id parses"),
            device: "device-aurora".to_owned(),
            card_fingerprint: "card-1111".to_owned(),
            score_bps: 8200,
            verdict: RiskVerdict::Review,
            reasons: vec![RiskReason::HighValue, RiskReason::DeviceRing],
            at: Timestamp::from_micros(9),
        };
        let wire = serde_json::to_value(&event).expect("serialize risk event");
        let decoded: RiskCaseEvent = serde_json::from_value(wire).expect("decode risk event");
        assert_eq!(decoded, event);
    }
}
