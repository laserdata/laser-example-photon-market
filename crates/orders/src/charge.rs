use async_trait::async_trait;
use laser_sdk::prelude::{Laser, LaserError};
use photon_shared::domain::{Money, OrderId};
use photon_shared::names::KvSpace;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChargeOutcome {
    Charged,
    AlreadyCharged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefundOutcome {
    Refunded,
    AlreadyRefunded,
}

#[async_trait]
pub trait ChargeLedger: Send + Sync {
    async fn charge(
        &self,
        order: OrderId,
        amount: Money,
        fence_token: Option<u64>,
    ) -> Result<ChargeOutcome, LaserError>;
    async fn refund(&self, order: OrderId, amount: Money) -> Result<RefundOutcome, LaserError>;
}

#[derive(Default)]
pub struct LocalChargeLedger {
    charges: Mutex<HashMap<OrderId, Money>>,
    refunds: Mutex<HashSet<OrderId>>,
}

impl LocalChargeLedger {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ChargeLedger for LocalChargeLedger {
    async fn charge(
        &self,
        order: OrderId,
        amount: Money,
        _fence_token: Option<u64>,
    ) -> Result<ChargeOutcome, LaserError> {
        let mut charges = self
            .charges
            .lock()
            .expect("charge ledger lock is not poisoned");
        Ok(match charges.entry(order) {
            std::collections::hash_map::Entry::Occupied(_) => ChargeOutcome::AlreadyCharged,
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(amount);
                ChargeOutcome::Charged
            }
        })
    }

    async fn refund(&self, order: OrderId, _amount: Money) -> Result<RefundOutcome, LaserError> {
        let mut refunds = self
            .refunds
            .lock()
            .expect("charge refund ledger lock is not poisoned");
        Ok(if refunds.insert(order) {
            RefundOutcome::Refunded
        } else {
            RefundOutcome::AlreadyRefunded
        })
    }
}

pub struct ManagedChargeLedger {
    laser: Laser,
}

impl ManagedChargeLedger {
    pub fn new(laser: Laser) -> Self {
        Self { laser }
    }
}

pub async fn prove_stale_holder_rejected(laser: &Laser) -> Result<(), LaserError> {
    let namespace = KvSpace::Charges.to_string();
    let fence_key = "zombie-charge-proof";
    let old = laser
        .kv(&namespace)
        .lease(fence_key, "old-holder", Duration::from_millis(100))
        .await?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let current = laser
        .kv(&namespace)
        .lease(fence_key, "current-holder", Duration::from_secs(2))
        .await?;
    if current.token <= old.token {
        return Err(LaserError::Invalid(
            "the reacquired fence token did not advance".to_owned(),
        ));
    }
    let current_key = format!("zombie/current/{}", current.token);
    laser
        .kv(&namespace)
        .cas_fenced(&current_key, &namespace, fence_key, current.token)
        .bytes("current-holder")
        .expect_absent()
        .commit()
        .await?;
    let stale_key = format!("zombie/stale/{}", old.token);
    let stale = laser
        .kv(&namespace)
        .cas_fenced(&stale_key, &namespace, fence_key, old.token)
        .bytes("stale-holder")
        .expect_absent()
        .commit()
        .await;
    match stale {
        Err(error) if error.is_lease_lost() => Ok(()),
        Err(error) => Err(LaserError::Invalid(format!(
            "the stale holder failed with the wrong error: {error}"
        ))),
        Ok(_) => Err(LaserError::Invalid(
            "the stale holder committed after its fence was superseded".to_owned(),
        )),
    }
}

#[async_trait]
impl ChargeLedger for ManagedChargeLedger {
    async fn charge(
        &self,
        order: OrderId,
        amount: Money,
        fence_token: Option<u64>,
    ) -> Result<ChargeOutcome, LaserError> {
        let fence_token = fence_token.ok_or_else(|| {
            LaserError::Invalid("a managed charge requires a workflow fence token".to_owned())
        })?;
        let key = order.to_string();
        let namespace = KvSpace::Charges.to_string();
        match self
            .laser
            .kv(&namespace)
            .cas_fenced(&key, namespace, &key, fence_token)
            .json(&amount)?
            .expect_absent()
            .commit()
            .await
        {
            Ok(_) => Ok(ChargeOutcome::Charged),
            Err(error) if error.is_version_conflict() => Ok(ChargeOutcome::AlreadyCharged),
            Err(error) => Err(error),
        }
    }

    async fn refund(&self, order: OrderId, amount: Money) -> Result<RefundOutcome, LaserError> {
        let key = format!("refund/{order}");
        match self
            .laser
            .kv(KvSpace::Charges.to_string())
            .set(&key)
            .json(&amount)?
            .expect_absent()
            .commit()
            .await
        {
            Ok(_) => Ok(RefundOutcome::Refunded),
            Err(error) if error.is_version_conflict() => Ok(RefundOutcome::AlreadyRefunded),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn given_a_charge_when_repeated_for_the_same_order_then_should_charge_once() {
        let ledger = LocalChargeLedger::new();
        assert_eq!(
            ledger
                .charge(order(), Money(3998), None)
                .await
                .expect("charge succeeds"),
            ChargeOutcome::Charged
        );
        assert_eq!(
            ledger
                .charge(order(), Money(3998), None)
                .await
                .expect("repeat charge succeeds"),
            ChargeOutcome::AlreadyCharged
        );
    }

    #[tokio::test]
    async fn given_a_refund_when_repeated_for_the_same_order_then_should_refund_once() {
        let ledger = LocalChargeLedger::new();
        assert_eq!(
            ledger
                .refund(order(), Money(3998))
                .await
                .expect("refund succeeds"),
            RefundOutcome::Refunded
        );
        assert_eq!(
            ledger
                .refund(order(), Money(3998))
                .await
                .expect("repeat refund succeeds"),
            RefundOutcome::AlreadyRefunded
        );
    }

    fn order() -> OrderId {
        "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            .parse()
            .expect("order id parses")
    }
}
