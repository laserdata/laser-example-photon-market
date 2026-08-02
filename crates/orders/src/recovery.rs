use crate::events;
use crate::inventory::Inventory;
use crate::saga;
use crate::state::{ConversationState, OrderStates};
use laser_sdk::prelude::{Laser, LaserError};
use photon_shared::ShutdownWatch;
use photon_shared::domain::OrderId;
use photon_shared::domain::catalog::{CatalogCommand, CatalogOp};
use photon_shared::domain::order::{OrderEvent, OrderEventKind, PlaceOrder};
use photon_shared::names::{BusinessTopic, WorkflowStep};
use photon_shared::topology;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::info;

// A killed coordinator leaves accepted (possibly charged) orders whose runs
// never reached shipment or a settling compensation. Fold the order history,
// look each stranded order's original command back up on the log, and resume
// its run: the workflow journal replays the completed steps, so a charge that
// already happened is never repeated.
pub(crate) async fn resume_incomplete_sagas(
    laser: &Laser,
    inventory: &Arc<dyn Inventory>,
    crash_after: Option<WorkflowStep>,
    publisher: events::Publisher,
    shutdown: ShutdownWatch,
) -> Result<Vec<JoinHandle<()>>, LaserError> {
    let offsets = vec![0u64; topology::PARTITIONS as usize];
    let mut histories: HashMap<OrderId, Vec<OrderEvent>> = HashMap::new();
    let events = BusinessTopic::OrderEvents.topic(laser).json::<OrderEvent>();
    let mut records = events
        .records("orders-saga-resume-events")?
        .from_offsets(offsets.clone());
    while let Some(next) = records.next().await {
        if let Ok(record) = next {
            histories
                .entry(record.value.order)
                .or_default()
                .push(record.value);
        }
    }
    let stranded: HashSet<OrderId> = histories
        .into_iter()
        .filter_map(|(order, events)| {
            ConversationState::load(events)
                .is_some_and(|state| state.needs_resume())
                .then_some(order)
        })
        .collect();
    if stranded.is_empty() {
        return Ok(Vec::new());
    }

    let commands = BusinessTopic::OrderCommands
        .topic(laser)
        .json::<PlaceOrder>();
    let mut records = commands
        .records("orders-saga-resume-commands")?
        .from_offsets(offsets);
    let mut resumable: HashMap<OrderId, PlaceOrder> = HashMap::new();
    while let Some(next) = records.next().await {
        if let Ok(record) = next
            && stranded.contains(&record.value.order)
        {
            resumable.insert(record.value.order, record.value);
        }
    }
    let tasks = resumable
        .into_iter()
        .map(|(order, command)| {
            info!("Resuming interrupted order {order} from its fulfillment journal");
            tokio::spawn(saga::supervise(
                laser.clone(),
                command,
                inventory.clone(),
                crash_after,
                publisher.clone(),
                shutdown.clone(),
            ))
        })
        .collect();
    Ok(tasks)
}

// Rebuild the local inventory from the log before serving: replay every catalog
// upsert, then reapply outstanding reservations from order.events, so a killed
// and restarted orders service recovers its stock truth from offset zero rather
// than starting empty.
pub(crate) async fn rebuild(
    laser: &Laser,
    inventory: &dyn Inventory,
    states: &OrderStates,
) -> Result<(), LaserError> {
    let offsets = vec![0u64; topology::PARTITIONS as usize];

    let catalog = BusinessTopic::CatalogCommands
        .topic(laser)
        .json::<CatalogCommand>();
    let mut catalog_records = catalog
        .records("orders-inventory-rebuild-catalog")?
        .from_offsets(offsets.clone());
    while let Some(next) = catalog_records.next().await {
        if let Ok(record) = next {
            match record.value.op {
                CatalogOp::UpsertSku { sku, available } => {
                    inventory
                        .upsert(sku, available)
                        .await
                        .map_err(|error| LaserError::Invalid(error.to_string()))?
                }
                CatalogOp::Restock { sku, add } => inventory
                    .restock(sku, add)
                    .await
                    .map_err(|error| LaserError::Invalid(error.to_string()))?,
            }
        }
    }

    let events = BusinessTopic::OrderEvents.topic(laser).json::<OrderEvent>();
    let mut event_records = events
        .records("orders-inventory-rebuild-events")?
        .from_offsets(offsets);
    while let Some(next) = event_records.next().await {
        let Ok(record) = next else { continue };
        states.observe(&record.value);
        match record.value.kind {
            OrderEventKind::InventoryReserved => {
                let _ = inventory
                    .reserve(record.value.order, &record.value.sku, record.value.quantity)
                    .await;
            }
            OrderEventKind::InventoryReleased => {
                inventory
                    .release(record.value.order, &record.value.sku)
                    .await
                    .map_err(|error| LaserError::Invalid(error.to_string()))?;
            }
            _ => {}
        }
    }
    Ok(())
}
