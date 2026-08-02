use crate::charge::{ChargeLedger, ChargeOutcome, RefundOutcome};
use crate::events;
use crate::quotes::{AttributedQuote, QuotePanel};
use laser_sdk::prelude::full::{CapabilitySelector, GatherPolicy, InboxRoute, RoutePolicy};
use laser_sdk::prelude::{
    AgentCtx, AgentHandler, AgentId, AgentMessage, AgentTopic, Contract, LaserError, Router,
};
use photon_shared::domain::order::{OrderEvent, OrderEventKind};
use photon_shared::domain::shipping::{
    BookRequest, Booking, CarrierRequest, QuoteRequest, ReleaseRequest,
};
use photon_shared::domain::{ContractVersion, CustomerId, Money, OrderId, Sku, Timestamp};
use photon_shared::names::{AppAgent, Skill};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

const QUORUM: usize = 2;
const QUOTE_DEADLINE: Duration = Duration::from_secs(5);
const BOOK_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum FulfillmentTask {
    Charge {
        order: OrderId,
        customer: CustomerId,
        sku: Sku,
        quantity: u32,
        amount_cents: u64,
    },
    Refund {
        order: OrderId,
        customer: CustomerId,
        sku: Sku,
        quantity: u32,
        amount_cents: u64,
    },
    Quote {
        order: OrderId,
        quantity: u32,
        ship_to: String,
    },
    Book {
        order: OrderId,
        customer: CustomerId,
        sku: Sku,
        quantity: u32,
        panel: QuotePanel,
    },
    Release {
        order: OrderId,
        customer: CustomerId,
        sku: Sku,
        quantity: u32,
        carrier: String,
        booking: String,
    },
    Dispatch {
        order: OrderId,
        customer: CustomerId,
        sku: Sku,
        quantity: u32,
        carrier: String,
        booking: String,
    },
}

pub struct Fulfillment {
    pub charges: Arc<dyn ChargeLedger>,
    pub events: events::Publisher,
}

impl AgentHandler for Fulfillment {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let task: FulfillmentTask = serde_json::from_slice(message.body()).map_err(|error| {
            LaserError::Invalid(format!("undecodable fulfillment task: {error}"))
        })?;
        match task {
            FulfillmentTask::Charge {
                order,
                customer,
                sku,
                quantity,
                amount_cents,
            } => {
                let amount = Money(amount_cents);
                if self
                    .charges
                    .charge(order, amount, message.provenance.fence_token)
                    .await?
                    == ChargeOutcome::Charged
                {
                    emit(
                        ctx,
                        &self.events,
                        order,
                        customer,
                        sku,
                        quantity,
                        OrderEventKind::Charged { amount },
                    )
                    .await?;
                    info!("Charged {amount} for order {order}");
                }
                ctx.respond(amount_cents.to_string().into_bytes()).await
            }
            FulfillmentTask::Refund {
                order,
                customer,
                sku,
                quantity,
                amount_cents,
            } => {
                let amount = Money(amount_cents);
                if self.charges.refund(order, amount).await? == RefundOutcome::Refunded {
                    emit(
                        ctx,
                        &self.events,
                        order,
                        customer,
                        sku,
                        quantity,
                        OrderEventKind::ChargeRefunded { amount },
                    )
                    .await?;
                    info!("Refunded {amount} for order {order} as saga compensation");
                }
                ctx.respond(amount_cents.to_string().into_bytes()).await
            }
            FulfillmentTask::Quote {
                order,
                quantity,
                ship_to,
            } => {
                let request = QuoteRequest {
                    version: ContractVersion::CURRENT,
                    order,
                    ship_to,
                    quantity,
                };
                let payload =
                    serde_json::to_vec(&CarrierRequest::Quote(request)).map_err(encode_error)?;
                let selector =
                    CapabilitySelector::new(Skill::QuoteShipment.to_string(), RoutePolicy::Any);
                let gather = match ctx
                    .fan_out(
                        selector,
                        payload,
                        GatherPolicy::Quorum(QUORUM),
                        QUOTE_DEADLINE,
                    )
                    .await
                {
                    Ok(gather) => gather,
                    Err(error) => {
                        warn!("Carrier quote fan-out failed for order {order}. {error}");
                        return Err(error);
                    }
                };
                let quotes = gather
                    .ok
                    .iter()
                    .filter_map(|(agent, reply)| {
                        let quote: photon_shared::domain::shipping::Quote =
                            serde_json::from_slice(reply.body()).ok()?;
                        (quote.version == ContractVersion::CURRENT
                            && quote.order == order
                            && quote.carrier == agent.as_str())
                        .then(|| AttributedQuote {
                            carrier: agent.to_string(),
                            quote,
                        })
                    })
                    .collect();
                let panel = QuotePanel { quotes };
                let quote_count = panel.quotes.len();
                debug!("Gathered {quote_count} verified carrier quote(s) for order {order}");
                ctx.respond(serde_json::to_vec(&panel).map_err(encode_error)?)
                    .await
            }
            FulfillmentTask::Book {
                order,
                customer,
                sku,
                quantity,
                panel,
            } => {
                let booking = book_with_carriers(ctx, order, &panel).await?;
                let kind = OrderEventKind::ShipmentBooked {
                    carrier: booking.carrier.clone(),
                    booking: booking.booking.clone(),
                };
                emit(ctx, &self.events, order, customer, sku, quantity, kind).await?;
                let carrier = &booking.carrier;
                debug!("Booked shipment for order {order} with carrier {carrier}");
                ctx.respond(serde_json::to_vec(&booking).map_err(encode_error)?)
                    .await
            }
            FulfillmentTask::Release {
                order,
                customer,
                sku,
                quantity,
                carrier,
                booking,
            } => {
                release_with_carrier(ctx, order, &carrier).await;
                let kind = OrderEventKind::BookingReleased {
                    carrier: carrier.clone(),
                    booking,
                };
                emit(ctx, &self.events, order, customer, sku, quantity, kind).await?;
                info!("Released {carrier} booking for order {order} as saga compensation");
                ctx.respond(carrier.into_bytes()).await
            }
            FulfillmentTask::Dispatch {
                order,
                customer,
                sku,
                quantity,
                carrier,
                booking,
            } => {
                let tracking = format!("trk-{order}");
                let kind = OrderEventKind::Shipped {
                    carrier: carrier.clone(),
                    tracking: tracking.clone(),
                };
                emit(ctx, &self.events, order, customer, sku, quantity, kind).await?;
                info!(
                    "Shipped order {order} via {carrier}. Booking: {booking}. Tracking: {tracking}."
                );
                ctx.respond(tracking.into_bytes()).await
            }
        }
    }
}

async fn book_with_carriers(
    ctx: &AgentCtx<'_>,
    order: OrderId,
    panel: &QuotePanel,
) -> Result<Booking, LaserError> {
    let winner = panel
        .winner()
        .ok_or_else(|| LaserError::Invalid("quote panel has no sane quote to book".to_owned()))?;
    if let Some(booking) = try_book(ctx, order, &winner.carrier).await? {
        return Ok(booking);
    }
    if let Some(runner_up) = panel.runner_up() {
        let winner_carrier = &winner.carrier;
        let runner_up_carrier = &runner_up.carrier;
        warn!(
            "Carrier {winner_carrier} did not book order {order} in time. Trying {runner_up_carrier}."
        );
        if let Some(booking) = try_book(ctx, order, &runner_up.carrier).await? {
            return Ok(booking);
        }
    }
    Err(LaserError::Invalid(
        "no carrier confirmed the booking".to_owned(),
    ))
}

// Best effort: the compensation event is the durable truth, and a carrier that
// cannot be reached will drop the booking when it expires unconfirmed.
async fn release_with_carrier(ctx: &AgentCtx<'_>, order: OrderId, carrier: &str) {
    let Ok(carrier_id) = carrier.parse::<AgentId>() else {
        warn!(
            "Booked carrier '{carrier}' for order {order} is not a valid agent ID. Skipping the release call."
        );
        return;
    };
    let request = CarrierRequest::Release(ReleaseRequest {
        version: ContractVersion::CURRENT,
        order,
        carrier: carrier.to_owned(),
    });
    let Ok(payload) = serde_json::to_vec(&request) else {
        warn!(
            "Could not encode the release request for order {order} and carrier {carrier}. Skipping the call."
        );
        return;
    };
    let contract = ctx
        .laser()
        .contract(Router::to(carrier_id))
        .from(AppAgent::Fulfillment.id())
        .payload(payload)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .deadline(BOOK_DEADLINE)
        .send()
        .await;
    match contract {
        Ok(Contract::Completed(_)) => {}
        Ok(_) | Err(_) => {
            warn!(
                "Carrier {carrier} did not confirm the booking release for order {order}. The booking will expire."
            )
        }
    }
}

async fn try_book(
    ctx: &AgentCtx<'_>,
    order: OrderId,
    carrier: &str,
) -> Result<Option<Booking>, LaserError> {
    let carrier_id: AgentId = carrier
        .parse()
        .map_err(|_| LaserError::Invalid(format!("carrier {carrier} is not a valid agent id")))?;
    let request = CarrierRequest::Book(BookRequest {
        version: ContractVersion::CURRENT,
        order,
        carrier: carrier.to_owned(),
    });
    let payload = serde_json::to_vec(&request).map_err(encode_error)?;
    let contract = ctx
        .laser()
        .contract(Router::to(carrier_id))
        .from(AppAgent::Fulfillment.id())
        .payload(payload)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .deadline(BOOK_DEADLINE)
        .send()
        .await?;
    match contract {
        Contract::Completed(reply) => Ok(serde_json::from_slice(reply.body()).ok()),
        Contract::TimedOut | Contract::NotConsumed | Contract::Failed(_) => Ok(None),
    }
}

async fn emit(
    ctx: &AgentCtx<'_>,
    publisher: &events::Publisher,
    order: OrderId,
    customer: CustomerId,
    sku: Sku,
    quantity: u32,
    kind: OrderEventKind,
) -> Result<(), LaserError> {
    let event = OrderEvent {
        version: ContractVersion::CURRENT,
        order,
        customer,
        sku,
        quantity,
        at: Timestamp::now(),
        kind,
    };
    publisher.emit(ctx.laser(), &event).await
}

fn encode_error(error: serde_json::Error) -> LaserError {
    warn!("Fulfillment could not encode a reply: {error}");
    LaserError::Invalid(format!("cannot encode fulfillment reply: {error}"))
}
