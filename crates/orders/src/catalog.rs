use crate::inventory::Inventory;
use laser_sdk::prelude::{AgentCtx, AgentHandler, AgentMessage, LaserError};
use photon_shared::domain::catalog::{CatalogCommand, CatalogOp};
use std::sync::Arc;
use tracing::debug;

pub struct CatalogHandler {
    pub inventory: Arc<dyn Inventory>,
}

impl AgentHandler for CatalogHandler {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let command: CatalogCommand = serde_json::from_slice(message.body()).map_err(|error| {
            LaserError::Invalid(format!("undecodable catalog command: {error}"))
        })?;
        command
            .version
            .ensure_current()
            .map_err(|error| LaserError::Invalid(error.to_string()))?;
        match command.op {
            CatalogOp::UpsertSku { sku, available } => {
                debug!("Upserted catalog SKU {sku} with {available} available unit(s)");
                self.inventory
                    .upsert(sku, available)
                    .await
                    .map_err(|error| LaserError::Invalid(error.to_string()))?;
            }
            CatalogOp::Restock { sku, add } => {
                debug!("Restocked catalog SKU {sku} with {add} additional unit(s)");
                self.inventory
                    .restock(sku, add)
                    .await
                    .map_err(|error| LaserError::Invalid(error.to_string()))?;
            }
        }
        Ok(())
    }
}
