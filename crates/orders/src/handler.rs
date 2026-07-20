use crate::error::InventoryError;
use crate::events;
use crate::inventory::{Inventory, ReserveOutcome};
use laser_sdk::prelude::full::InboxRoute;
use laser_sdk::prelude::{
    AgentCtx, AgentHandler, AgentMessage, AgentTopic, Contract, Laser, LaserError, RoutePolicy,
    Router,
};
use photon_shared::ShutdownWatch;
use photon_shared::domain::order::{OrderEvent, OrderEventKind, PlaceOrder};
use photon_shared::domain::risk::{RiskCaseEvent, RiskVerdict, ScreenRequest};
use photon_shared::domain::{ContractVersion, Timestamp};
use photon_shared::names::Index;
use photon_shared::names::{AppAgent, Skill, WorkflowStep};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tracing::{info, warn};

const SCREEN_DEADLINE: Duration = Duration::from_secs(5);

pub struct OrdersHandler {
    pub inventory: Arc<dyn Inventory>,
    pub risk_threshold_cents: u64,
    pub crash_after: Option<WorkflowStep>,
    pub events: events::Publisher,
    pub read_your_writes: bool,
    pub workflows: UnboundedSender<JoinHandle<()>>,
    pub shutdown: ShutdownWatch,
}

impl OrdersHandler {
    async fn assert_reservation_visible(
        &self,
        laser: &Laser,
        command: &PlaceOrder,
    ) -> Result<(), LaserError> {
        let index = Index::Orders.to_string();
        for attempt in 1..=20 {
            match laser
                .query(&index)
                .filter_eq("order", command.order.to_string())
                .filter_eq("event_type", "inventory_reserved")
                .read_your_writes()
                .limit(1)
                .fetch()
                .await
            {
                Ok(result) if !result.rows.is_empty() => return Ok(()),
                Ok(_) => {}
                Err(error) if error.is_stale() && attempt < 20 => {}
                Err(error) => return Err(error),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Err(LaserError::Invalid(
            "read-your-writes did not observe the inventory reservation after retries".to_owned(),
        ))
    }

    async fn screen(
        &self,
        laser: &Laser,
        command: &PlaceOrder,
        amount_cents: u64,
    ) -> Result<RiskVerdict, LaserError> {
        let request = ScreenRequest {
            version: ContractVersion::CURRENT,
            order: command.order,
            customer: command.customer.clone(),
            amount_cents,
            device: command.device.clone(),
            card_fingerprint: command.card_fingerprint.clone(),
        };
        let payload = serde_json::to_vec(&request).map_err(|error| {
            LaserError::Invalid(format!("cannot encode screen request: {error}"))
        })?;
        let contract = laser
            .contract(Router::to_capable(
                Skill::ScreenOrder.to_string(),
                RoutePolicy::Any,
            ))
            .from(AppAgent::Orders.id())
            .payload(payload)
            .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
            .deadline(SCREEN_DEADLINE)
            .send()
            .await?;
        match contract {
            Contract::Completed(reply) => Ok(serde_json::from_slice::<RiskCaseEvent>(reply.body())
                .map(|event| event.verdict)
                .unwrap_or(RiskVerdict::Reject)),
            _ => {
                let order = command.order;
                warn!("Risk screening did not complete for order {order}. Rejecting the order.");
                Ok(RiskVerdict::Reject)
            }
        }
    }
}

impl AgentHandler for OrdersHandler {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let command: PlaceOrder = serde_json::from_slice(message.body()).map_err(|error| {
            LaserError::Invalid(format!("undecodable place-order command: {error}"))
        })?;
        command
            .version
            .ensure_current()
            .map_err(|error| LaserError::Invalid(error.to_string()))?;
        if command.quantity == 0 {
            return Err(LaserError::Invalid(
                "an order quantity must be greater than zero".to_owned(),
            ));
        }
        let amount_cents = command
            .total()
            .ok_or_else(|| LaserError::Invalid("the order total overflows Money".to_owned()))?
            .cents();
        let laser = ctx.laser();

        let history = self.events.state(command.order);
        if history
            .as_ref()
            .is_some_and(|state| state.intake_complete())
        {
            let order = command.order;
            info!("Order {order} already has a durable intake decision. Ignoring the redelivery.");
            return Ok(());
        }
        if history.is_none() {
            self.events
                .emit(
                    laser,
                    &OrderEvent::new(&command, Timestamp::now(), OrderEventKind::Received),
                )
                .await?;
        }

        match self
            .inventory
            .reserve(command.order, &command.sku, command.quantity)
            .await
        {
            Ok(outcome) => {
                if outcome == ReserveOutcome::Reserved {
                    self.events
                        .emit(
                            laser,
                            &OrderEvent::new(
                                &command,
                                Timestamp::now(),
                                OrderEventKind::InventoryReserved,
                            ),
                        )
                        .await?;
                }
                if self.read_your_writes {
                    self.assert_reservation_visible(laser, &command).await?;
                }

                let cleared = if amount_cents >= self.risk_threshold_cents {
                    matches!(
                        self.screen(laser, &command, amount_cents).await?,
                        RiskVerdict::Clear
                    )
                } else {
                    true
                };

                if cleared {
                    self.events
                        .emit(
                            laser,
                            &OrderEvent::new(&command, Timestamp::now(), OrderEventKind::Accepted),
                        )
                        .await?;
                    let order = command.order;
                    let quantity = command.quantity;
                    let sku = &command.sku;
                    info!(
                        "Accepted order {order} for {quantity} x {sku} at a total of {amount_cents} cents"
                    );
                    let task = tokio::spawn(crate::saga::supervise(
                        laser.clone(),
                        command.clone(),
                        self.inventory.clone(),
                        self.crash_after,
                        self.events.clone(),
                        self.shutdown.clone(),
                    ));
                    self.workflows.send(task).map_err(|_| {
                        LaserError::Invalid("orders workflow owner has stopped".to_owned())
                    })?;
                } else {
                    self.inventory
                        .release(command.order, &command.sku)
                        .await
                        .map_err(|error| LaserError::Invalid(error.to_string()))?;
                    self.events
                        .emit(
                            laser,
                            &OrderEvent::new(
                                &command,
                                Timestamp::now(),
                                OrderEventKind::InventoryReleased,
                            ),
                        )
                        .await?;
                    let reason = "rejected by risk screening".to_owned();
                    self.events
                        .emit(
                            laser,
                            &OrderEvent::new(
                                &command,
                                Timestamp::now(),
                                OrderEventKind::Rejected { reason },
                            ),
                        )
                        .await?;
                    let order = command.order;
                    info!("Rejected order {order} after risk screening");
                }
            }
            Err(InventoryError::UnknownSku(sku)) => {
                let order = command.order;
                warn!(
                    "Order {order} references unknown SKU {sku}. Sending it to the dead-letter topic for repair."
                );
                return Err(LaserError::Invalid(format!(
                    "unknown sku {sku}, awaiting a catalog upsert"
                )));
            }
            Err(InventoryError::InsufficientStock {
                sku,
                requested,
                available,
            }) => {
                let reason = format!(
                    "insufficient stock for {sku}: requested {requested}, available {available}"
                );
                self.events
                    .emit(
                        laser,
                        &OrderEvent::new(
                            &command,
                            Timestamp::now(),
                            OrderEventKind::Rejected { reason },
                        ),
                    )
                    .await?;
                let order = command.order;
                let sku = &command.sku;
                info!("Rejected order {order} because SKU {sku} has insufficient stock");
            }
            Err(InventoryError::Managed(error)) => {
                return Err(LaserError::Invalid(format!(
                    "managed inventory operation failed: {error}"
                )));
            }
        }
        Ok(())
    }
}
