use laser_sdk::iggy::prelude::{Consumer, ConsumerOffsetClient, Identifier, TopicClient};
use laser_sdk::prelude::{ConversationId, Laser, LaserError};
use laser_sdk::wire::agent::AgentDeadLetter;
use photon_shared::domain::catalog::{CatalogCommand, CatalogOp};
use photon_shared::domain::order::PlaceOrder;
use photon_shared::domain::{ContractVersion, OrderId};
use photon_shared::names::{self, AppAgent, BusinessTopic};
use photon_shared::{BusinessProvenance, ShutdownWatch, deadletter, eventually, publish_business};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::{Instant, interval_at};
use tracing::{info, warn};

/// How much stock the operator installs when repairing a missing catalog item.
const RESTOCK: u32 = 25;
const REPAIR_FOLD_TIMEOUT: Duration = Duration::from_secs(30);
/// The long-running demo's repair cadence. Tests pass their own, shorter
/// interval directly to `repair_loop` instead of waiting on this one.
pub const REPAIR_LOOP_INTERVAL: Duration = Duration::from_secs(20);

/// The operator's dead-letter triage: undecodable poison stays quarantined
/// (bytes cannot be repaired), while a valid order stranded on missing catalog
/// state is repaired by installing that state and then redriven verbatim.
/// Returns the orders that were redriven.
pub async fn repair_and_redrive(laser: &Laser) -> Result<Vec<OrderId>, LaserError> {
    let mut redriven = Vec::new();
    for capsule in deadletter::scan(laser).await? {
        if let Some(order) = repair_one(laser, &capsule).await? {
            redriven.push(order);
        }
    }
    Ok(redriven)
}

/// Repairs and redrives one capsule, or narrates and returns `None` for a
/// capsule bytes cannot repair (undecodable poison). Shared by the one-shot
/// finite triage and the long-running repair loop below.
async fn repair_one(
    laser: &Laser,
    capsule: &AgentDeadLetter,
) -> Result<Option<OrderId>, LaserError> {
    let Ok(command) = serde_json::from_slice::<PlaceOrder>(&capsule.payload) else {
        let partition = capsule.source.partition_id;
        let offset = capsule.source.offset;
        info!(
            "Dead letter at partition {partition}, offset {offset} is not a valid order. It remains quarantined."
        );
        return Ok(None);
    };
    let sku = command.sku.clone();
    let restock = CatalogCommand {
        version: ContractVersion::CURRENT,
        op: CatalogOp::Restock {
            sku: sku.clone(),
            add: RESTOCK,
        },
    };
    publish_business(
        laser,
        BusinessTopic::CatalogCommands,
        &restock,
        BusinessProvenance {
            conversation: ConversationId::derive(sku.as_ref()),
            source: AppAgent::Operator,
            causal_parent: None,
            idempotency_key: format!("catalog-repair/{sku}/{}", command.order),
        },
    )
    .await?;
    // The redrive is honest only once the missing state is actually
    // installed: wait for the orders catalog consumer to fold the repair,
    // or the redriven command races it and strands again.
    catalog_repair_folded(laser).await?;
    laser.redrive_dead_letter(capsule).await?;
    let order = command.order;
    info!("Repaired catalog SKU {sku} and redrove stranded order {order}");
    Ok(Some(command.order))
}

/// The long-running repair loop rescans the dead-letter topic on an interval
/// and repairs anything new. Dead-letter records never leave the log, so every
/// scan sees every capsule ever quarantined. `seen` is this loop's own memory
/// of which source positions it already handled, so a repeat scan neither
/// re-narrates nor re-pays the catalog-fold wait for the same capsule twice.
/// Purely a narration and latency concern, not a correctness one: a repeated
/// redrive of one capsule is itself dedup-safe (the SDK re-keys its
/// idempotency header by source position), and so is a repeated restock (its
/// own idempotency key is per order per sku). Like every other fold in this
/// codebase, `seen` is process-local and legitimately starts over on restart.
pub async fn repair_loop(laser: Laser, interval: Duration, mut shutdown: ShutdownWatch) {
    let mut seen = HashSet::new();
    let start = Instant::now() + interval;
    let mut ticks = interval_at(start, interval);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = ticks.tick() => repair_pass(&laser, &mut seen).await,
        }
    }
}

async fn repair_pass(laser: &Laser, seen: &mut HashSet<(u32, u64)>) {
    let capsules = match deadletter::scan(laser).await {
        Ok(capsules) => capsules,
        Err(error) => {
            warn!("Operator could not scan dead letters: {error}");
            return;
        }
    };
    for capsule in &capsules {
        let position = (capsule.source.partition_id, capsule.source.offset);
        if !seen.insert(position) {
            continue;
        }
        if let Err(error) = repair_one(laser, capsule).await {
            warn!("Operator repair pass failed: {error}");
            seen.remove(&position);
        }
    }
}

async fn catalog_repair_folded(laser: &Laser) -> Result<(), LaserError> {
    let group = Consumer::group(Identifier::named(&format!(
        "{}-catalog",
        AppAgent::Orders.id()
    ))?);
    let stream = Identifier::named(names::STREAM)?;
    let topic = Identifier::named(&BusinessTopic::CatalogCommands.to_string())?;
    eventually(REPAIR_FOLD_TIMEOUT, || {
        let group = group.clone();
        let stream = stream.clone();
        let topic = topic.clone();
        async move {
            let Ok(Some(details)) = laser.client().get_topic(&stream, &topic).await else {
                return false;
            };
            for partition in details.partitions {
                if partition.messages_count == 0 {
                    continue;
                }
                let offset = laser
                    .client()
                    .get_consumer_offset(&group, &stream, &topic, Some(partition.id))
                    .await;
                match offset {
                    Ok(Some(offset)) if offset.stored_offset >= offset.current_offset => {}
                    _ => return false,
                }
            }
            true
        }
    })
    .await
}
