use laser_sdk::prelude::{AgentId, Capabilities, Laser, LaserError};
use photon_shared::domain::order::{OrderEvent, OrderEventKind};
use photon_shared::domain::{OrderId, Timestamp};
use photon_shared::names::BusinessTopic;
use photon_shared::{deadletter, eventually, topology};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const SETTLE_DEADLINE: Duration = Duration::from_secs(30);
const SETTLE_QUIESCENCE: Duration = Duration::from_secs(1);
// The scripted adversary quarantines exactly two capsules: the undecodable
// poison and the unknown-sku order awaiting catalog repair.
const SCRIPTED_DEAD_LETTERS: usize = 2;

pub async fn capabilities(laser: &Laser) -> Capabilities {
    laser.capabilities().await.clone()
}

/// The spine has flowed once an accepted order reaches shipment on the log.
/// Under the scripted adversary the acts are also complete only when both
/// quarantined capsules sit on the dead-letter topic, ready for triage.
pub async fn spine_settled(laser: &Laser, scripted: bool) -> Result<(), LaserError> {
    eventually(SETTLE_DEADLINE, || async {
        let contained = !scripted
            || deadletter::scan(laser)
                .await
                .is_ok_and(|capsules| capsules.len() >= SCRIPTED_DEAD_LETTERS);
        contained
            && order_outcomes(laser)
                .await
                .is_ok_and(|outcomes| !outcomes.shipped.is_empty())
    })
    .await
}

/// The repair is applied once every redriven order shows an acceptance on the
/// log, proving the installed catalog state let the redelivery through.
pub async fn repair_applied(laser: &Laser, redriven: &[OrderId]) -> Result<(), LaserError> {
    if redriven.is_empty() {
        return Ok(());
    }
    eventually(SETTLE_DEADLINE, || async {
        order_outcomes(laser).await.is_ok_and(|outcomes| {
            redriven
                .iter()
                .all(|order| outcomes.accepted.contains(order))
        })
    })
    .await
}

/// The signed unquarantine has folded once this connection's verifying
/// registry stops excluding the agent from capability resolution.
pub async fn unquarantine_folded(laser: &Laser, agent: &AgentId) -> Result<(), LaserError> {
    eventually(SETTLE_DEADLINE, || async {
        let Ok(mut registry) = laser.agent_registry() else {
            return false;
        };
        registry.refresh(Timestamp::now().as_micros()).await.is_ok()
            && !registry.is_quarantined(agent)
    })
    .await
}

/// Every order that entered intake has reached a terminal business event and
/// the observed set has stayed unchanged long enough to catch queued commands.
pub async fn orders_drained(laser: &Laser) -> Result<(), LaserError> {
    let deadline = Instant::now() + SETTLE_DEADLINE;
    let mut stable: Option<(HashSet<OrderId>, Instant)> = None;
    loop {
        if let Ok(outcomes) = order_outcomes(laser).await
            && outcomes.drained()
        {
            match &stable {
                Some((orders, since))
                    if orders == &outcomes.started && since.elapsed() >= SETTLE_QUIESCENCE =>
                {
                    return Ok(());
                }
                Some((orders, _)) if orders == &outcomes.started => {}
                _ => stable = Some((outcomes.started, Instant::now())),
            }
        } else {
            stable = None;
        }
        if Instant::now() >= deadline {
            return Err(LaserError::Invalid(
                "orders did not drain before the deadline".to_owned(),
            ));
        }
        sleep(Duration::from_millis(20)).await;
    }
}

#[derive(Default)]
struct OrderOutcomes {
    accepted: HashSet<OrderId>,
    shipped: HashSet<OrderId>,
    started: HashSet<OrderId>,
    terminal: HashSet<OrderId>,
}

impl OrderOutcomes {
    fn observe(&mut self, order: OrderId, kind: &OrderEventKind) {
        match kind {
            OrderEventKind::Received => {
                self.started.insert(order);
            }
            OrderEventKind::Accepted => {
                self.accepted.insert(order);
            }
            OrderEventKind::Shipped { .. } => {
                self.shipped.insert(order);
                self.terminal.insert(order);
            }
            OrderEventKind::Rejected { .. } | OrderEventKind::ChargeRefunded { .. } => {
                self.terminal.insert(order);
            }
            _ => {}
        }
    }

    fn drained(&self) -> bool {
        !self.started.is_empty() && self.started.is_subset(&self.terminal)
    }
}

async fn order_outcomes(laser: &Laser) -> Result<OrderOutcomes, LaserError> {
    let offsets = vec![0u64; topology::PARTITIONS as usize];
    let events = BusinessTopic::OrderEvents.topic(laser).json::<OrderEvent>();
    let mut records = events.records("demo-settle-orders")?.from_offsets(offsets);
    let mut outcomes = OrderOutcomes::default();
    while let Some(next) = records.next().await {
        if let Ok(record) = next {
            outcomes.observe(record.value.order, &record.value.kind);
        }
    }
    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_an_accepted_order_without_a_terminal_when_observed_then_should_not_be_drained() {
        let order = order_id();
        let mut outcomes = OrderOutcomes::default();
        outcomes.observe(order, &OrderEventKind::Received);
        outcomes.observe(order, &OrderEventKind::Accepted);
        assert!(outcomes.accepted.contains(&order));
        assert!(outcomes.shipped.is_empty());
        assert!(!outcomes.drained());
    }

    #[test]
    fn given_a_shipped_order_when_observed_then_should_be_drained() {
        let order = order_id();
        let mut outcomes = OrderOutcomes::default();
        outcomes.observe(order, &OrderEventKind::Received);
        outcomes.observe(
            order,
            &OrderEventKind::Shipped {
                carrier: "hermes".to_owned(),
                tracking: "track-1".to_owned(),
            },
        );
        assert!(outcomes.shipped.contains(&order));
        assert!(outcomes.drained());
    }

    fn order_id() -> OrderId {
        "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            .parse()
            .expect("the fixture order id parses")
    }
}
