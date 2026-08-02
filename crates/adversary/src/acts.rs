use laser_sdk::prelude::{AgentTopic, ConversationId, Laser, LaserError, Provenance};
use laser_sdk::wire::agent::AgentCard;
use photon_shared::domain::order::PlaceOrder;
use photon_shared::domain::shipping::Booking;
use photon_shared::domain::{ContractVersion, Money, OrderId, Timestamp};
use photon_shared::names::{AppAgent, BusinessTopic};
use photon_shared::{BusinessProvenance, publish_business};
use ulid::Ulid;

const FLOOD_COPIES: usize = 5;
const UNAVAILABLE_SKU: &str = "sku-unavailable";
const KNOWN_SKU: &str = "sku-photon-phone";

pub async fn poison(laser: &Laser) -> Result<(), LaserError> {
    let conversation = ConversationId::new();
    let stamped = Provenance::builder()
        .conversation_id(conversation)
        .agent(AppAgent::Borealis.id())
        .idempotency_key("poison/malformed".to_owned())
        .build();
    BusinessTopic::OrderCommands
        .topic(laser)
        .publish()
        .payload(b"{ this is not a decodable place-order".to_vec())
        .provenance(&stamped)
        .send()
        .await
}

pub async fn duplicate_flood(laser: &Laser, seed: u64) -> Result<OrderId, LaserError> {
    let order = OrderId::new(Ulid::from_parts(1, u128::from(seed)));
    let command = place_order(order, KNOWN_SKU, 1);
    for _ in 0..FLOOD_COPIES {
        publish_business(
            laser,
            BusinessTopic::OrderCommands,
            &command,
            provenance(order),
        )
        .await?;
    }
    Ok(order)
}

// Flood the shared reply topic with fabricated booking confirmations carrying
// guessed correlation ids. A contract waiter only completes on its own live
// correlation, so the forged replies are ignored and the fleet keeps flowing.
pub async fn unsolicited_replies(laser: &Laser, seed: u64) -> Result<(), LaserError> {
    for copy in 0..FLOOD_COPIES {
        let order = OrderId::new(Ulid::from_parts(3, u128::from(seed) + copy as u128));
        let forged = Booking {
            version: ContractVersion::CURRENT,
            carrier: AppAgent::Borealis.to_string(),
            order,
            booking: format!("bk-borealis-forged-{copy}"),
        };
        let body = serde_json::to_vec(&forged)
            .map_err(|error| LaserError::Invalid(format!("cannot encode forged reply: {error}")))?;
        let stamped = Provenance::builder()
            .conversation_id(conversation(order))
            .agent(AppAgent::Borealis.id())
            .correlation_id(format!("forged-{seed}-{copy}"))
            .build();
        laser
            .send_agent(AgentTopic::Responses, body, &stamped)
            .await?;
    }
    Ok(())
}

// Mallory the impersonator: publishes a capability card claiming the honest
// `support` agent id, then forges an UNSIGNED quarantine fact against `hermes`.
// A verifying connection ignores the unsigned privileged fact (hermes keeps
// serving), and the card is claim-only discovery, never verified effect
// authority, so no impersonated effect ever lands. The forged card advertises
// no skill, so it cannot hijack a capability route the honest fleet resolves
// (the demo does not present claim-only discovery as workload identity).
pub async fn impersonate(laser: &Laser) -> Result<(), LaserError> {
    let card = AgentCard {
        name: Some(AppAgent::Support.to_string()),
        version: None,
        capabilities: Vec::new(),
        ttl_micros: None,
    };
    laser.publish_card(AppAgent::Support.id(), &card).await?;
    // Unsigned, so a verifying registry folds nothing: hermes stays live.
    laser
        .quarantine(AppAgent::Mallory.id(), &AppAgent::Hermes.id())
        .await
}

pub async fn unprocessable(laser: &Laser, seed: u64) -> Result<OrderId, LaserError> {
    let order = OrderId::new(Ulid::from_parts(2, u128::from(seed)));
    let command = place_order(order, UNAVAILABLE_SKU, 1);
    publish_business(
        laser,
        BusinessTopic::OrderCommands,
        &command,
        provenance(order),
    )
    .await?;
    Ok(order)
}

fn place_order(order: OrderId, sku: &str, quantity: u32) -> PlaceOrder {
    PlaceOrder {
        version: ContractVersion::CURRENT,
        order,
        customer: "cust-adversary".parse().expect("customer id is non-empty"),
        sku: sku.parse().expect("sku is non-empty"),
        quantity,
        unit_price: Money(9_900),
        device: "device-borealis".to_owned(),
        ship_to: "addr-void".to_owned(),
        card_fingerprint: "card-0000".to_owned(),
        placed_at: Timestamp::now(),
    }
}

fn provenance(order: OrderId) -> BusinessProvenance {
    BusinessProvenance {
        conversation: conversation(order),
        source: AppAgent::Borealis,
        causal_parent: None,
        idempotency_key: format!("place-order/{order}"),
    }
}

fn conversation(order: OrderId) -> ConversationId {
    order
        .to_string()
        .parse()
        .expect("an order id is a valid conversation id")
}
