use crate::shoppers::{Checkout, Session, ShopperWalk};
use laser_sdk::prelude::{ConversationId, Laser, LaserError};
use photon_shared::domain::order::PlaceOrder;
use photon_shared::domain::shop::ShopEvent;
use photon_shared::domain::{ContractVersion, Timestamp};
use photon_shared::names::{AppAgent, BusinessTopic};
use photon_shared::{BusinessProvenance, ShutdownWatch, publish_business};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::time::Duration;
use tracing::warn;

pub async fn run(
    laser: Laser,
    seed: u64,
    epoch: u64,
    session_interval: Duration,
    mut shutdown: ShutdownWatch,
) {
    let mut walk = ShopperWalk::new(seed, epoch);
    let mut jitter = StdRng::seed_from_u64(seed ^ 0x5eed_0feed);
    let center = u64::try_from(session_interval.as_millis()).unwrap_or(u64::MAX / 2);
    let range = center / 2..=center.saturating_add(center / 2);
    loop {
        let session = walk.next_session();
        if let Err(error) = publish_session(&laser, &session).await {
            warn!("Storefront could not publish a shopper session: {error}");
        }
        let delay = Duration::from_millis(jitter.random_range(range.clone()));
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

async fn publish_session(laser: &Laser, session: &Session) -> Result<(), LaserError> {
    let topic = BusinessTopic::ShopEvents.topic(laser);
    let mut batch = topic.publish_batch();
    for step in &session.steps {
        let event = ShopEvent {
            version: ContractVersion::CURRENT,
            session: session.session,
            customer: session.customer.clone(),
            kind: step.kind,
            sku: step.sku.clone(),
            price: step.price,
            at: Timestamp::now(),
        };
        batch = batch.add_json(&event)?;
    }
    batch
        .partition_key(session.session.to_string())
        .send()
        .await?;

    if let Some(checkout) = &session.checkout {
        publish_order(laser, session, checkout).await?;
    }
    Ok(())
}

async fn publish_order(
    laser: &Laser,
    session: &Session,
    checkout: &Checkout,
) -> Result<(), LaserError> {
    let order = checkout.order;
    let command = PlaceOrder {
        version: ContractVersion::CURRENT,
        order,
        customer: session.customer.clone(),
        sku: checkout.sku.clone(),
        quantity: checkout.quantity,
        unit_price: checkout.unit_price,
        device: checkout.device.clone(),
        ship_to: checkout.ship_to.clone(),
        card_fingerprint: checkout.card_fingerprint.clone(),
        placed_at: Timestamp::now(),
    };
    let conversation: ConversationId = order
        .to_string()
        .parse()
        .expect("an order id is a valid conversation id");
    publish_business(
        laser,
        BusinessTopic::OrderCommands,
        &command,
        BusinessProvenance {
            conversation,
            source: AppAgent::Storefront,
            causal_parent: None,
            idempotency_key: format!("place-order/{order}"),
        },
    )
    .await
}
