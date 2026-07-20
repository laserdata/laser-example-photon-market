use async_trait::async_trait;
use laser_sdk::prelude::{Laser, LaserError};
use photon_shared::domain::order::{OrderEvent, OrderEventKind};
use photon_shared::domain::{OrderId, TicketId};
use photon_shared::names::KvSpace;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

// A refund claim is owned by one ticket. The same handler can retry after a
// step-up, while another ticket or replica cannot publish the effect.
#[async_trait]
pub trait RefundLedger: Send + Sync {
    async fn claim(&self, ticket: TicketId, event: &OrderEvent) -> Result<bool, LaserError>;
    async fn release(&self, ticket: TicketId, order: OrderId) -> Result<(), LaserError>;
}

#[derive(Default)]
pub struct LocalRefundLedger {
    issued: Mutex<HashMap<OrderId, Option<TicketId>>>,
}

impl LocalRefundLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn observe(&self, event: &OrderEvent) {
        if !matches!(event.kind, OrderEventKind::Refunded { .. }) {
            return;
        }
        self.issued
            .lock()
            .expect("refund ledger lock is not poisoned")
            .insert(event.order, None);
    }
}

#[async_trait]
impl RefundLedger for LocalRefundLedger {
    async fn claim(&self, ticket: TicketId, event: &OrderEvent) -> Result<bool, LaserError> {
        let mut issued = self
            .issued
            .lock()
            .expect("refund ledger lock is not poisoned");
        match issued.get(&event.order) {
            Some(Some(owner)) if *owner == ticket => Ok(true),
            Some(_) => Ok(false),
            None => {
                issued.insert(event.order, Some(ticket));
                Ok(true)
            }
        }
    }

    async fn release(&self, ticket: TicketId, order: OrderId) -> Result<(), LaserError> {
        let mut issued = self
            .issued
            .lock()
            .expect("refund ledger lock is not poisoned");
        if issued.get(&order) == Some(&Some(ticket)) {
            issued.remove(&order);
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
struct RefundClaim {
    ticket: TicketId,
    event: OrderEvent,
}

pub struct ManagedRefundLedger {
    laser: Laser,
}

impl ManagedRefundLedger {
    pub fn new(laser: Laser) -> Self {
        Self { laser }
    }
}

#[async_trait]
impl RefundLedger for ManagedRefundLedger {
    async fn claim(&self, ticket: TicketId, event: &OrderEvent) -> Result<bool, LaserError> {
        let key = event.order.to_string();
        let claim = RefundClaim {
            ticket,
            event: event.clone(),
        };
        match self
            .laser
            .kv(KvSpace::Refunds.to_string())
            .set(&key)
            .json(&claim)?
            .expect_absent()
            .commit()
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if error.is_version_conflict() => Ok(self
                .laser
                .kv(KvSpace::Refunds.to_string())
                .get_typed::<RefundClaim>(&key)
                .await?
                .is_some_and(|existing| existing.ticket == ticket)),
            Err(error) => Err(error),
        }
    }

    async fn release(&self, ticket: TicketId, order: OrderId) -> Result<(), LaserError> {
        let key = order.to_string();
        let existing = self
            .laser
            .kv(KvSpace::Refunds.to_string())
            .get_typed::<RefundClaim>(&key)
            .await?;
        if existing.is_some_and(|claim| claim.ticket == ticket) {
            self.laser
                .kv(KvSpace::Refunds.to_string())
                .delete(&key)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use photon_shared::domain::{ContractVersion, Money, Timestamp};

    fn order() -> OrderId {
        "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            .parse()
            .expect("order id parses")
    }

    fn refund_event() -> OrderEvent {
        OrderEvent {
            version: ContractVersion::CURRENT,
            order: order(),
            customer: "cust-1".parse().expect("customer id parses"),
            sku: "sku-1".parse().expect("sku parses"),
            quantity: 1,
            at: Timestamp::from_micros(1),
            kind: OrderEventKind::Refunded { amount: Money(100) },
        }
    }

    #[tokio::test]
    async fn given_an_order_when_refunded_once_then_should_treat_the_second_claim_as_a_duplicate() {
        let ledger = LocalRefundLedger::new();
        let event = refund_event();
        let first: TicketId = "01ARZ3NDEKTSV4RRFFQ69G5FAW"
            .parse()
            .expect("ticket id parses");
        let second: TicketId = "01ARZ3NDEKTSV4RRFFQ69G5FAX"
            .parse()
            .expect("ticket id parses");
        assert!(ledger.claim(first, &event).await.expect("claim succeeds"));
        assert!(!ledger.claim(second, &event).await.expect("claim succeeds"));
    }

    #[tokio::test]
    async fn given_a_refund_event_when_rebuilt_then_should_restore_the_refund_ledger() {
        let ledger = LocalRefundLedger::new();
        let event = refund_event();

        ledger.observe(&event);

        let ticket: TicketId = "01ARZ3NDEKTSV4RRFFQ69G5FAW"
            .parse()
            .expect("ticket id parses");
        assert!(!ledger.claim(ticket, &event).await.expect("claim succeeds"));
    }
}
