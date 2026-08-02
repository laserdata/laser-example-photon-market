use laser_sdk::prelude::{Consumer, ConversationId, Laser, LaserError};
use photon_shared::domain::order::{OrderEvent, OrderEventKind};
use photon_shared::domain::ticket::{Ticket, TicketKind};
use photon_shared::domain::{ContractVersion, TicketId, Timestamp};
use photon_shared::names::{AppAgent, BusinessTopic};
use photon_shared::{BusinessProvenance, ShutdownWatch, publish_business};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::time::Duration;
use tracing::warn;
use ulid::Ulid;

const TICKET_RATE: f64 = 0.25;
const DAMAGED_SHARE: f64 = 0.3;

pub async fn run(laser: Laser, seed: u64, mut shutdown: ShutdownWatch) {
    let mut rng = StdRng::seed_from_u64(seed ^ 0x0007_1c6e_7000);
    let mut consumer = match build_consumer(&laser).await {
        Ok(consumer) => consumer,
        Err(error) => {
            warn!("Storefront could not tail order events for tickets: {error}");
            return;
        }
    };
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            next = consumer.next() => match next {
                Some(Ok(received)) => {
                    if let Ok(event) = received.json::<OrderEvent>()
                        && matches!(event.kind, OrderEventKind::Shipped { .. })
                        && rng.random_bool(TICKET_RATE)
                        && let Err(error) = open_ticket(&laser, &event, &mut rng).await
                    {
                        warn!("Storefront could not open a support ticket: {error}");
                    }
                }
                Some(Err(error)) => warn!("Storefront ticket consumer error: {error}"),
                None => break,
            }
        }
    }
    if let Err(error) = consumer.shutdown().await {
        warn!("Storefront ticket consumer did not leave its group cleanly. {error}");
    }
}

async fn build_consumer(laser: &Laser) -> Result<Consumer, LaserError> {
    BusinessTopic::OrderEvents
        .topic(laser)
        .consumer_group("storefront-tickets")
        .batch_length(100)
        .poll_interval(Duration::from_millis(20))
        .build()
        .await
}

async fn open_ticket(
    laser: &Laser,
    event: &OrderEvent,
    rng: &mut StdRng,
) -> Result<(), LaserError> {
    let ticket = TicketId::new(Ulid::from_parts(event.at.as_micros(), rng.random()));
    let (kind, body) = if rng.random_bool(DAMAGED_SHARE) {
        (
            TicketKind::Damaged,
            format!(
                "My order {} arrived damaged and I would like a refund.",
                event.order
            ),
        )
    } else {
        (
            TicketKind::WhereIsMyOrder,
            format!(
                "Where is my order {}? It shows as shipped but has not arrived.",
                event.order
            ),
        )
    };
    let record = Ticket {
        version: ContractVersion::CURRENT,
        ticket,
        customer: event.customer.clone(),
        order: event.order,
        kind,
        body,
        opened_at: Timestamp::now(),
    };
    let conversation: ConversationId = ConversationId::derive(event.customer.as_ref());
    publish_business(
        laser,
        BusinessTopic::SupportTickets,
        &record,
        BusinessProvenance {
            conversation,
            source: AppAgent::Storefront,
            causal_parent: None,
            idempotency_key: format!("ticket/{ticket}"),
        },
    )
    .await
}
