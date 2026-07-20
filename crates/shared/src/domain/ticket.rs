use super::{ContractVersion, CustomerId, OrderId, TicketId, Timestamp};
use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Ticket {
    #[serde(rename = "v")]
    pub version: ContractVersion,
    pub ticket: TicketId,
    pub customer: CustomerId,
    pub order: OrderId,
    pub kind: TicketKind,
    pub body: String,
    #[serde(rename = "opened_at_micros")]
    pub opened_at: Timestamp,
}

#[derive(Clone, Copy, Debug, Deserialize, Display, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TicketKind {
    WhereIsMyOrder,
    Damaged,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_ticket_when_round_tripped_then_should_match() {
        let ticket = Ticket {
            version: ContractVersion::CURRENT,
            ticket: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                .parse()
                .expect("ticket id parses"),
            customer: "cust-1".parse().expect("customer id parses"),
            order: "01ARZ3NDEKTSV4RRFFQ69G5FAW"
                .parse()
                .expect("order id parses"),
            kind: TicketKind::Damaged,
            body: "arrived broken".to_owned(),
            opened_at: Timestamp::from_micros(11),
        };
        let wire = serde_json::to_value(&ticket).expect("serialize ticket");
        let decoded: Ticket = serde_json::from_value(wire).expect("decode ticket");
        assert_eq!(decoded, ticket);
    }
}
