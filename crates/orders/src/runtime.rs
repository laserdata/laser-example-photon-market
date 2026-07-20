use crate::charge::{self, ChargeLedger, LocalChargeLedger, ManagedChargeLedger};
use crate::events;
use crate::inventory::{Inventory, LocalInventory, ManagedInventory};
use crate::recovery;
use crate::state::OrderStates;
use laser_sdk::prelude::{Laser, LaserError};
use laser_sdk::query::Consistency;
use photon_shared::output;
use std::sync::Arc;
use tracing::info;

pub struct Runtime {
    pub inventory: Arc<dyn Inventory>,
    pub charges: Arc<dyn ChargeLedger>,
    pub events: events::Publisher,
    pub read_your_writes: bool,
}

impl Runtime {
    /// Select stronger managed backends only when the connected deployment
    /// advertises their semantics. Domain handlers receive traits and do not
    /// branch on the deployment again.
    pub async fn prepare(laser: &Laser, zombie_charge: bool) -> Result<Self, LaserError> {
        let capabilities = laser.capabilities().await;
        let event_schema = if capabilities.query.available {
            Some(photon_shared::schema::ensure_order_event_schema(laser).await?)
        } else {
            output::gate("order.events JSON Schema guard", false);
            None
        };
        let states = Arc::new(OrderStates::default());
        let events = events::Publisher::new(event_schema, states.clone());
        let inventory: Arc<dyn Inventory> = if capabilities.kv.cas {
            Arc::new(ManagedInventory::new(laser.clone()))
        } else {
            output::gate("cross-process inventory KV CAS", false);
            Arc::new(LocalInventory::new())
        };
        recovery::rebuild(laser, inventory.as_ref(), &states).await?;
        let charges: Arc<dyn ChargeLedger> = if capabilities.kv.cas_fenced {
            Arc::new(ManagedChargeLedger::new(laser.clone()))
        } else {
            output::gate("fenced cross-holder charge ledger", false);
            Arc::new(LocalChargeLedger::new())
        };

        if zombie_charge && capabilities.kv.cas_fenced {
            charge::prove_stale_holder_rejected(laser).await?;
            info!("Managed zombie charge was rejected by the superseding fence token");
        } else if zombie_charge {
            output::gate("zombie-charge stale-holder rejection", false);
        }

        Ok(Self {
            inventory,
            charges,
            events,
            read_your_writes: capabilities.serves_consistency(Consistency::ReadYourWrites),
        })
    }
}
