#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod acts;
mod borealis;

use crate::borealis::Borealis;
use laser_sdk::prelude::full::InboxRoute;
use laser_sdk::prelude::{Agent, AgentTopic, Laser, LaserError};
use laser_sdk::wire::agent::CapabilityDescriptor;
use photon_shared::knobs::{self, ConfigError};
use photon_shared::names::{self, AdversaryMode, AppAgent, BusinessTopic, Skill};
use photon_shared::{HANDLER_SHUTDOWN_GRACE, LaserFactory, ServiceHandle, ShutdownWatch, topology};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::time::Duration;
use thiserror::Error;
use tracing::{info, warn};

const OWNED: [BusinessTopic; 2] = [BusinessTopic::OrderCommands, BusinessTopic::CatalogCommands];

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Laser(#[from] LaserError),
}

#[derive(Debug)]
pub struct Opts {
    pub mode: AdversaryMode,
    pub seed: u64,
    pub rate: u32,
}

impl Opts {
    pub fn from_env() -> Result<Self, Error> {
        Ok(Self {
            mode: knobs::adversary_mode()?,
            seed: knobs::adversary_seed()?,
            rate: knobs::adversary_rate()?,
        })
    }
}

pub async fn run(factory: &LaserFactory, opts: Opts) -> Result<ServiceHandle, Error> {
    let Opts { mode, seed, rate } = opts;
    let mut handle = ServiceHandle::new("adversary");
    if mode == AdversaryMode::Off {
        info!("Fault injection is off. Set LASER_ADVERSARY to scripted or random to enable it.");
        return Ok(handle);
    }

    let laser = factory.connect(names::STREAM).await?;
    topology::ensure_owned(&laser, OWNED).await?;
    topology::ensure_agent_topics(&laser).await?;
    let target = factory.target();
    info!(
        "Adversary is live on {target}. Mode: {mode}. Seed: {seed}. Rate: {rate} acts per minute."
    );

    let connection = factory.connect(names::STREAM).await?;
    let borealis = Agent::builder()
        .id(AppAgent::Borealis.id())
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .shutdown_grace(HANDLER_SHUTDOWN_GRACE)
        .capabilities(vec![
            CapabilityDescriptor {
                skill_id: Skill::QuoteShipment.to_string(),
                ..Default::default()
            },
            CapabilityDescriptor {
                skill_id: Skill::BookShipment.to_string(),
                ..Default::default()
            },
        ])
        .ack_on_pickup(true)
        .handler(Borealis)
        .build()
        .spawn(connection);
    handle.track_agent(borealis).await?;
    info!("Borealis joined as a rogue carrier with quote_shipment and book_shipment capabilities.");

    let shutdown = handle.watch();
    let task = match mode {
        AdversaryMode::Random => tokio::spawn(random(laser, seed, rate, shutdown)),
        _ => tokio::spawn(scripted(laser, seed, shutdown)),
    };
    handle.track(task);
    Ok(handle)
}

async fn scripted(laser: Laser, seed: u64, mut shutdown: ShutdownWatch) {
    run_script(&laser, seed).await;
    shutdown.cancelled().await;
}

async fn run_script(laser: &Laser, seed: u64) {
    if let Err(error) = acts::poison(laser).await {
        warn!("Adversary poison act failed: {error}");
    } else {
        info!("Adversary published a malformed order command. The consumer should dead-letter it.");
    }
    match acts::duplicate_flood(laser, seed).await {
        Ok(order) => {
            info!(
                "Adversary published five copies of order {order}. Deduplication should admit one."
            )
        }
        Err(error) => warn!("Adversary flood act failed: {error}"),
    }
    match acts::unprocessable(laser, seed).await {
        Ok(order) => {
            info!(
                "Adversary placed order {order} for an unknown SKU. The operator should repair and redrive it."
            )
        }
        Err(error) => warn!("Adversary unprocessable act failed: {error}"),
    }
    if let Err(error) = acts::unsolicited_replies(laser, seed).await {
        warn!("Adversary unsolicited-reply act failed: {error}");
    } else {
        info!(
            "Adversary published forged booking replies with guessed correlations. Contract matching should ignore them."
        );
    }
    if let Err(error) = acts::impersonate(laser).await {
        warn!("Adversary impersonation act failed: {error}");
    } else {
        info!(
            "Mallory claimed the support identity and forged an unsigned quarantine of hermes. Signature verification should reject it."
        );
    }
}

async fn random(laser: Laser, seed: u64, rate: u32, mut shutdown: ShutdownWatch) {
    let mut rng = StdRng::seed_from_u64(seed);
    let base = 60_000u64 / u64::from(rate.max(1));
    loop {
        let jitter = rng.random_range(base / 2..=base);
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_millis(jitter)) => {}
        }
        let outcome = match rng.random_range(0..5u8) {
            0 => acts::poison(&laser).await.map(|_| {
                info!("Adversary published a malformed order command. The consumer should dead-letter it.");
            }),
            1 => acts::duplicate_flood(&laser, rng.random()).await.map(|order| {
                info!("Adversary published five copies of order {order}. Deduplication should admit one.");
            }),
            2 => acts::unsolicited_replies(&laser, rng.random()).await.map(|()| {
                info!("Adversary published forged booking replies with guessed correlations. Contract matching should ignore them.");
            }),
            3 => acts::impersonate(&laser).await.map(|()| {
                info!("Mallory claimed the support identity and forged an unsigned quarantine of hermes. Signature verification should reject it.");
            }),
            _ => acts::unprocessable(&laser, rng.random()).await.map(|order| {
                info!("Adversary placed order {order} for an unknown SKU. The operator should repair and redrive it.");
            }),
        };
        if let Err(error) = outcome {
            warn!("Adversary random act failed: {error}");
        }
    }
}
