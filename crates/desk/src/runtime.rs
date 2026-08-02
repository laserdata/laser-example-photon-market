use crate::error::Error;
use crate::links::{LocalRiskLinks, ManagedRiskLinks, RiskLinks};
use crate::order_lookup::{self, LocalOrderLookup, ManagedOrderLookup, OrderReader};
use crate::refunds::{LocalRefundLedger, ManagedRefundLedger, RefundLedger};
use laser_sdk::prelude::full::MemoryBackend;
use laser_sdk::prelude::{Laser, MemoryHandle};
use photon_shared::llm::{LlmClient, SkewStrategy, SkewedLlm};
use photon_shared::names::{LlmProvider, MemorySpace};
use photon_shared::semantic::{DeterministicEmbedder, DeterministicReranker};
use photon_shared::{LaserFactory, ServiceHandle, output};
use std::sync::Arc;

pub struct SupportData {
    pub lookup: Arc<dyn OrderReader>,
    pub refunds: Arc<dyn RefundLedger>,
}

pub async fn memory(laser: &Laser, space: MemorySpace) -> MemoryHandle {
    let capabilities = laser.capabilities().await;
    if capabilities.query.available && capabilities.kv.available {
        laser
            .memory_with(space.to_string(), MemoryBackend::Log)
            .reranker(DeterministicReranker)
    } else {
        output::gate("durable risk and support memory", false);
        laser
            .memory_with(space.to_string(), MemoryBackend::Vector)
            .embedder(DeterministicEmbedder)
    }
}

pub async fn risk_links(laser: &Laser) -> Arc<dyn RiskLinks> {
    if laser.capabilities().await.graph {
        Arc::new(ManagedRiskLinks::new(laser.clone()))
    } else {
        output::gate("managed fraud graph traversal", false);
        Arc::new(LocalRiskLinks::new())
    }
}

pub async fn support_data(
    factory: &LaserFactory,
    laser: &Laser,
    handle: &mut ServiceHandle,
) -> Result<SupportData, Error> {
    let capabilities = laser.capabilities().await;
    if capabilities.query.available && capabilities.kv.cas {
        return Ok(SupportData {
            lookup: Arc::new(ManagedOrderLookup::new(laser.clone())),
            refunds: Arc::new(ManagedRefundLedger::new(laser.clone())),
        });
    }

    output::gate("query-backed order lookup and refund KV CAS", false);
    let lookup = Arc::new(LocalOrderLookup::new());
    let refunds = Arc::new(LocalRefundLedger::new());
    let fold_laser = factory.connect(photon_shared::names::STREAM).await?;
    let fold = order_lookup::start_order_fold(&fold_laser).await?;
    order_lookup::rebuild_order_state(&fold_laser, &lookup, &refunds).await?;
    handle.track(tokio::spawn(order_lookup::fold_orders(
        fold,
        lookup.clone(),
        refunds.clone(),
        handle.watch(),
    )));
    Ok(SupportData { lookup, refunds })
}

pub fn llm(
    provider: LlmProvider,
    seed: u64,
    skew_permille: u16,
) -> Result<Arc<dyn LlmClient>, Error> {
    let base = photon_shared::llm::llm_client(provider)?;
    if skew_permille == 0 {
        return Ok(base);
    }
    Ok(Arc::new(SkewedLlm::new(
        base,
        seed,
        skew_permille,
        SkewStrategy::FabricateMemory,
    )))
}
