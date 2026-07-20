use async_trait::async_trait;
use laser_sdk::prelude::{Laser, LaserError};
use laser_sdk::wire::graph::{EdgeDir, GraphEdge, GraphNode};
use laser_sdk::wire::query::Value;
use photon_shared::domain::CustomerId;
use photon_shared::domain::risk::{RiskCaseEvent, RiskReason, RiskVerdict, ScreenRequest};
use photon_shared::names::FRAUD_GRAPH;
use std::collections::HashMap;
use std::sync::Mutex;

const CASE_EDGE: &str = "risk_case";
const CASE_LABEL: &str = "RiskCase";

#[async_trait]
pub trait RiskLinks: Send + Sync {
    async fn record(&self, event: &RiskCaseEvent) -> Result<(), LaserError>;
    async fn rings(&self, request: &ScreenRequest) -> Result<Vec<RiskReason>, LaserError>;
}

#[derive(Default)]
pub struct LocalRiskLinks {
    devices: Mutex<HashMap<String, Vec<Case>>>,
    cards: Mutex<HashMap<String, Vec<Case>>>,
}

struct Case {
    customer: CustomerId,
    verdict: RiskVerdict,
}

impl LocalRiskLinks {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl RiskLinks for LocalRiskLinks {
    async fn record(&self, event: &RiskCaseEvent) -> Result<(), LaserError> {
        let case = || Case {
            customer: event.customer.clone(),
            verdict: event.verdict,
        };
        self.devices
            .lock()
            .expect("risk device links lock is not poisoned")
            .entry(event.device.clone())
            .or_default()
            .push(case());
        self.cards
            .lock()
            .expect("risk card links lock is not poisoned")
            .entry(event.card_fingerprint.clone())
            .or_default()
            .push(case());
        Ok(())
    }

    async fn rings(&self, request: &ScreenRequest) -> Result<Vec<RiskReason>, LaserError> {
        let mut reasons = Vec::new();
        if linked_rejection(
            &self
                .devices
                .lock()
                .expect("risk device links lock is not poisoned"),
            &request.device,
            &request.customer,
        ) {
            reasons.push(RiskReason::DeviceRing);
        }
        if linked_rejection(
            &self
                .cards
                .lock()
                .expect("risk card links lock is not poisoned"),
            &request.card_fingerprint,
            &request.customer,
        ) {
            reasons.push(RiskReason::CardRing);
        }
        Ok(reasons)
    }
}

pub struct ManagedRiskLinks {
    laser: Laser,
}

impl ManagedRiskLinks {
    pub fn new(laser: Laser) -> Self {
        Self { laser }
    }

    async fn has_rejected_case(
        &self,
        label: &str,
        value: &str,
        requester: &CustomerId,
    ) -> Result<bool, LaserError> {
        let identifier = GraphNode::entity(label, value);
        let result = self
            .laser
            .graph(FRAUD_GRAPH)
            .neighbors(identifier.id, EdgeDir::Out, Some(CASE_EDGE.to_owned()), 1)
            .await?;
        Ok(result.nodes.iter().any(|node| {
            node.labels.iter().any(|label| label == CASE_LABEL)
                && string_attr(node, "verdict") == Some("reject")
                && string_attr(node, "customer") != Some(requester.as_ref())
        }))
    }
}

#[async_trait]
impl RiskLinks for ManagedRiskLinks {
    async fn record(&self, event: &RiskCaseEvent) -> Result<(), LaserError> {
        let device = GraphNode::entity("Device", &event.device);
        let card = GraphNode::entity("Card", &event.card_fingerprint);
        let mut case = GraphNode::entity(CASE_LABEL, event.order.to_string());
        case.attrs
            .push(("customer".to_owned(), Value::from(event.customer.as_ref())));
        case.attrs
            .push(("verdict".to_owned(), Value::from(event.verdict.to_string())));
        let edges = vec![
            GraphEdge::relate(&device, CASE_EDGE, &case),
            GraphEdge::relate(&card, CASE_EDGE, &case),
        ];
        self.laser
            .graph(FRAUD_GRAPH)
            .upsert(vec![device, card, case], edges)
            .await
    }

    async fn rings(&self, request: &ScreenRequest) -> Result<Vec<RiskReason>, LaserError> {
        let mut reasons = Vec::new();
        if self
            .has_rejected_case("Device", &request.device, &request.customer)
            .await?
        {
            reasons.push(RiskReason::DeviceRing);
        }
        if self
            .has_rejected_case("Card", &request.card_fingerprint, &request.customer)
            .await?
        {
            reasons.push(RiskReason::CardRing);
        }
        Ok(reasons)
    }
}

fn string_attr<'a>(node: &'a GraphNode, name: &str) -> Option<&'a str> {
    node.attrs.iter().find_map(|(key, value)| match value {
        Value::Str(value) if key == name => Some(value.as_str()),
        _ => None,
    })
}

fn linked_rejection(links: &HashMap<String, Vec<Case>>, key: &str, requester: &CustomerId) -> bool {
    links.get(key).is_some_and(|cases| {
        cases
            .iter()
            .any(|case| case.verdict == RiskVerdict::Reject && case.customer != *requester)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use photon_shared::domain::{ContractVersion, Timestamp};

    fn case(customer: &str, device: &str, card: &str, verdict: RiskVerdict) -> RiskCaseEvent {
        RiskCaseEvent {
            version: ContractVersion::CURRENT,
            order: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                .parse()
                .expect("order id parses"),
            customer: customer.parse().expect("customer id parses"),
            device: device.to_owned(),
            card_fingerprint: card.to_owned(),
            score_bps: 9_000,
            verdict,
            reasons: vec![],
            at: Timestamp::from_micros(1),
        }
    }

    fn request(customer: &str, device: &str, card: &str) -> ScreenRequest {
        ScreenRequest {
            version: ContractVersion::CURRENT,
            order: "01ARZ3NDEKTSV4RRFFQ69G5FAW"
                .parse()
                .expect("order id parses"),
            customer: customer.parse().expect("customer id parses"),
            amount_cents: 50_000,
            device: device.to_owned(),
            card_fingerprint: card.to_owned(),
        }
    }

    #[tokio::test]
    async fn given_a_device_shared_with_a_rejected_customer_when_screened_then_should_flag_a_ring()
    {
        let links = LocalRiskLinks::new();
        links
            .record(&case(
                "cust-01",
                "device-aurora",
                "card-1111",
                RiskVerdict::Reject,
            ))
            .await
            .expect("case records");
        assert_eq!(
            links
                .rings(&request("cust-02", "device-aurora", "card-2222"))
                .await
                .expect("rings load"),
            vec![RiskReason::DeviceRing]
        );
    }

    #[tokio::test]
    async fn given_only_clear_history_on_the_identifiers_when_screened_then_should_flag_nothing() {
        let links = LocalRiskLinks::new();
        links
            .record(&case(
                "cust-01",
                "device-aurora",
                "card-1111",
                RiskVerdict::Clear,
            ))
            .await
            .expect("case records");
        assert!(
            links
                .rings(&request("cust-02", "device-aurora", "card-1111"))
                .await
                .expect("rings load")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn given_the_same_customers_own_rejection_when_screened_then_should_not_self_flag() {
        let links = LocalRiskLinks::new();
        links
            .record(&case(
                "cust-01",
                "device-aurora",
                "card-1111",
                RiskVerdict::Reject,
            ))
            .await
            .expect("case records");
        assert!(
            links
                .rings(&request("cust-01", "device-aurora", "card-1111"))
                .await
                .expect("rings load")
                .is_empty()
        );
    }
}
