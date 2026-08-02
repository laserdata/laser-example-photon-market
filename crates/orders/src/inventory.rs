use crate::error::InventoryError;
use async_trait::async_trait;
use laser_sdk::prelude::{Laser, LaserError};
use photon_shared::domain::{OrderId, Sku};
use photon_shared::names::KvSpace;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

const MAX_CAS_ATTEMPTS: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReserveOutcome {
    Reserved,
    AlreadyReserved,
}

#[async_trait]
pub trait Inventory: Send + Sync {
    async fn upsert(&self, sku: Sku, available: u32) -> Result<(), InventoryError>;
    /// Add stock to a sku's shelf rather than replacing it, so a second repair
    /// or restock of the same sku accumulates instead of overwriting the first.
    async fn restock(&self, sku: Sku, add: u32) -> Result<(), InventoryError>;
    async fn reserve(
        &self,
        order: OrderId,
        sku: &Sku,
        quantity: u32,
    ) -> Result<ReserveOutcome, InventoryError>;
    /// Return a reservation's stock to the shelf. Idempotent: releasing an
    /// unknown or already-released order is a no-op, so a saga compensation
    /// and a log replay can both apply it safely.
    async fn release(&self, order: OrderId, sku: &Sku) -> Result<(), InventoryError>;
}

#[derive(Default, Deserialize, Serialize)]
struct Ledger {
    available: u32,
    reservations: HashMap<OrderId, u32>,
}

#[derive(Default)]
pub struct LocalInventory {
    ledgers: Mutex<HashMap<Sku, Ledger>>,
}

impl LocalInventory {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Inventory for LocalInventory {
    async fn upsert(&self, sku: Sku, available: u32) -> Result<(), InventoryError> {
        let mut ledgers = self.ledgers.lock().expect("inventory lock is not poisoned");
        ledgers.entry(sku).or_default().available = available;
        Ok(())
    }

    async fn restock(&self, sku: Sku, add: u32) -> Result<(), InventoryError> {
        let mut ledgers = self.ledgers.lock().expect("inventory lock is not poisoned");
        ledgers.entry(sku).or_default().available += add;
        Ok(())
    }

    async fn reserve(
        &self,
        order: OrderId,
        sku: &Sku,
        quantity: u32,
    ) -> Result<ReserveOutcome, InventoryError> {
        let mut ledgers = self.ledgers.lock().expect("inventory lock is not poisoned");
        let ledger = ledgers
            .get_mut(sku)
            .ok_or_else(|| InventoryError::UnknownSku(sku.clone()))?;
        if ledger.reservations.contains_key(&order) {
            return Ok(ReserveOutcome::AlreadyReserved);
        }
        if ledger.available < quantity {
            return Err(InventoryError::InsufficientStock {
                sku: sku.clone(),
                requested: quantity,
                available: ledger.available,
            });
        }
        ledger.available -= quantity;
        ledger.reservations.insert(order, quantity);
        Ok(ReserveOutcome::Reserved)
    }

    async fn release(&self, order: OrderId, sku: &Sku) -> Result<(), InventoryError> {
        let mut ledgers = self.ledgers.lock().expect("inventory lock is not poisoned");
        if let Some(ledger) = ledgers.get_mut(sku)
            && let Some(quantity) = ledger.reservations.remove(&order)
        {
            ledger.available += quantity;
        }
        Ok(())
    }
}

pub struct ManagedInventory {
    laser: Laser,
}

impl ManagedInventory {
    pub fn new(laser: Laser) -> Self {
        Self { laser }
    }
}

#[async_trait]
impl Inventory for ManagedInventory {
    async fn upsert(&self, sku: Sku, available: u32) -> Result<(), InventoryError> {
        for _ in 0..MAX_CAS_ATTEMPTS {
            let current = self.entry(&sku).await?;
            let (mut ledger, version) = match current {
                Some((ledger, version)) => (ledger, Some(version)),
                None => (Ledger::default(), None),
            };
            ledger.available = available;
            match self.commit(&sku, &ledger, version).await {
                Ok(()) => return Ok(()),
                Err(error) if error.is_version_conflict() => continue,
                Err(error) => return Err(managed(error)),
            }
        }
        Err(InventoryError::Managed(format!(
            "inventory CAS for {sku} did not converge after {MAX_CAS_ATTEMPTS} attempts"
        )))
    }

    async fn restock(&self, sku: Sku, add: u32) -> Result<(), InventoryError> {
        for _ in 0..MAX_CAS_ATTEMPTS {
            let current = self.entry(&sku).await?;
            let (mut ledger, version) = match current {
                Some((ledger, version)) => (ledger, Some(version)),
                None => (Ledger::default(), None),
            };
            ledger.available = ledger.available.checked_add(add).ok_or_else(|| {
                InventoryError::Managed(format!("inventory restock overflow for {sku}"))
            })?;
            match self.commit(&sku, &ledger, version).await {
                Ok(()) => return Ok(()),
                Err(error) if error.is_version_conflict() => continue,
                Err(error) => return Err(managed(error)),
            }
        }
        Err(InventoryError::Managed(format!(
            "inventory restock CAS for {sku} did not converge after {MAX_CAS_ATTEMPTS} attempts"
        )))
    }

    async fn reserve(
        &self,
        order: OrderId,
        sku: &Sku,
        quantity: u32,
    ) -> Result<ReserveOutcome, InventoryError> {
        for _ in 0..MAX_CAS_ATTEMPTS {
            let Some((mut ledger, version)) = self.entry(sku).await? else {
                return Err(InventoryError::UnknownSku(sku.clone()));
            };
            if ledger.reservations.contains_key(&order) {
                return Ok(ReserveOutcome::AlreadyReserved);
            }
            if ledger.available < quantity {
                return Err(InventoryError::InsufficientStock {
                    sku: sku.clone(),
                    requested: quantity,
                    available: ledger.available,
                });
            }
            ledger.available -= quantity;
            ledger.reservations.insert(order, quantity);
            match self.commit(sku, &ledger, Some(version)).await {
                Ok(()) => return Ok(ReserveOutcome::Reserved),
                Err(error) if error.is_version_conflict() => continue,
                Err(error) => return Err(managed(error)),
            }
        }
        Err(InventoryError::Managed(format!(
            "inventory reservation CAS for {sku} did not converge after {MAX_CAS_ATTEMPTS} attempts"
        )))
    }

    async fn release(&self, order: OrderId, sku: &Sku) -> Result<(), InventoryError> {
        for _ in 0..MAX_CAS_ATTEMPTS {
            let Some((mut ledger, version)) = self.entry(sku).await? else {
                return Ok(());
            };
            let Some(quantity) = ledger.reservations.remove(&order) else {
                return Ok(());
            };
            ledger.available = ledger.available.checked_add(quantity).ok_or_else(|| {
                InventoryError::Managed(format!("inventory release overflow for {sku}"))
            })?;
            match self.commit(sku, &ledger, Some(version)).await {
                Ok(()) => return Ok(()),
                Err(error) if error.is_version_conflict() => continue,
                Err(error) => return Err(managed(error)),
            }
        }
        Err(InventoryError::Managed(format!(
            "inventory release CAS for {sku} did not converge after {MAX_CAS_ATTEMPTS} attempts"
        )))
    }
}

impl ManagedInventory {
    async fn entry(&self, sku: &Sku) -> Result<Option<(Ledger, u64)>, InventoryError> {
        let entry = self
            .laser
            .kv(KvSpace::Inventory.to_string())
            .get_entry(sku.as_ref())
            .await
            .map_err(managed)?;
        entry
            .map(|entry| {
                serde_json::from_slice(&entry.value)
                    .map(|ledger| (ledger, entry.version))
                    .map_err(|error| {
                        InventoryError::Managed(format!(
                            "inventory ledger for {sku} is undecodable: {error}"
                        ))
                    })
            })
            .transpose()
    }

    async fn commit(
        &self,
        sku: &Sku,
        ledger: &Ledger,
        version: Option<u64>,
    ) -> Result<(), LaserError> {
        let request = self
            .laser
            .kv(KvSpace::Inventory.to_string())
            .set(sku.as_ref())
            .json(ledger)?;
        match version {
            Some(version) => request.expect_version(version).commit().await,
            None => request.expect_absent().commit().await,
        }
        .map(|_| ())
    }
}

fn managed(error: LaserError) -> InventoryError {
    InventoryError::Managed(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn given_existing_stock_when_restocked_twice_then_should_accumulate_not_reset() {
        let inventory = LocalInventory::new();
        inventory.upsert(sku(), 5).await.expect("stock upserts");
        inventory
            .reserve(order("V"), &sku(), 5)
            .await
            .expect("stock reserves");

        inventory.restock(sku(), 25).await.expect("stock restocks");
        inventory.restock(sku(), 25).await.expect("stock restocks");

        assert_eq!(
            inventory.reserve(order("W"), &sku(), 50).await,
            Ok(ReserveOutcome::Reserved),
            "two repairs of the same sku must add up rather than overwrite each other"
        );
    }

    #[tokio::test]
    async fn given_stock_when_reserved_then_should_decrement_once_and_be_idempotent() {
        let inventory = LocalInventory::new();
        inventory.upsert(sku(), 5).await.expect("stock upserts");

        assert_eq!(
            inventory.reserve(order("V"), &sku(), 2).await,
            Ok(ReserveOutcome::Reserved)
        );
        assert_eq!(
            inventory.reserve(order("V"), &sku(), 2).await,
            Ok(ReserveOutcome::AlreadyReserved),
            "a redelivered reservation for the same order must not decrement twice"
        );
        assert_eq!(
            inventory.reserve(order("W"), &sku(), 2).await,
            Ok(ReserveOutcome::Reserved)
        );
    }

    #[tokio::test]
    async fn given_a_reservation_when_released_then_should_restock_once_and_be_idempotent() {
        let inventory = LocalInventory::new();
        inventory.upsert(sku(), 2).await.expect("stock upserts");
        assert_eq!(
            inventory.reserve(order("V"), &sku(), 2).await,
            Ok(ReserveOutcome::Reserved)
        );

        inventory
            .release(order("V"), &sku())
            .await
            .expect("stock releases");
        inventory
            .release(order("V"), &sku())
            .await
            .expect("stock releases idempotently");
        assert_eq!(
            inventory.reserve(order("W"), &sku(), 2).await,
            Ok(ReserveOutcome::Reserved),
            "a released reservation returns its stock exactly once"
        );
    }

    #[tokio::test]
    async fn given_low_stock_when_reserved_then_should_reject_with_insufficient_stock() {
        let inventory = LocalInventory::new();
        inventory.upsert(sku(), 1).await.expect("stock upserts");
        assert_eq!(
            inventory.reserve(order("V"), &sku(), 2).await,
            Err(InventoryError::InsufficientStock {
                sku: sku(),
                requested: 2,
                available: 1,
            })
        );
    }

    #[tokio::test]
    async fn given_an_unknown_sku_when_reserved_then_should_reject() {
        let inventory = LocalInventory::new();
        assert_eq!(
            inventory.reserve(order("V"), &sku(), 1).await,
            Err(InventoryError::UnknownSku(sku()))
        );
    }

    fn sku() -> Sku {
        "sku-1".parse().expect("sku parses")
    }

    fn order(tail: &str) -> OrderId {
        format!("01ARZ3NDEKTSV4RRFFQ69G5FA{tail}")
            .parse()
            .expect("order id parses")
    }
}
