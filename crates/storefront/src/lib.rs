#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod publisher;
mod shoppers;
mod tickets;

use laser_sdk::prelude::LaserError;
use photon_shared::domain::{Sku, Timestamp};
use photon_shared::knobs::{self, ConfigError};
use photon_shared::names::{self, BusinessTopic};
use photon_shared::{LaserFactory, ServiceHandle, topology};
use std::time::Duration;
use thiserror::Error;
use tracing::info;

const OWNED: [BusinessTopic; 4] = [
    BusinessTopic::ShopEvents,
    BusinessTopic::OrderCommands,
    BusinessTopic::SupportTickets,
    BusinessTopic::OrderEvents,
];

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Laser(#[from] LaserError),
}

const INITIAL_STOCK: u32 = 100_000;

pub fn catalog() -> Vec<(Sku, u32)> {
    shoppers::CATALOG
        .iter()
        .map(|(sku, _price)| {
            (
                sku.parse().expect("a catalog sku is non-empty"),
                INITIAL_STOCK,
            )
        })
        .collect()
}

#[derive(Debug)]
pub struct Opts {
    pub concurrency: usize,
    pub scenario_seed: u64,
    pub session_interval: Duration,
}

impl Opts {
    pub fn from_env() -> Result<Self, Error> {
        Ok(Self {
            concurrency: knobs::concurrency(3),
            scenario_seed: knobs::scenario_seed()?,
            session_interval: knobs::session_interval(250)?,
        })
    }
}

pub async fn run(factory: &LaserFactory, opts: Opts) -> Result<ServiceHandle, Error> {
    let Opts {
        concurrency,
        scenario_seed,
        session_interval,
    } = opts;
    let laser = factory.connect(names::STREAM).await?;
    topology::ensure_owned(&laser, OWNED).await?;
    let target = factory.target();
    let interval_ms = session_interval.as_millis();
    info!(
        "Storefront is live on {target}. Streams: {concurrency}. Interval: about {interval_ms} ms. Seed: {scenario_seed}."
    );

    let mut handle = ServiceHandle::new("storefront");
    // One identity epoch per process start: the seeded walks replay the same
    // behavior every run, while the minted session and order ids stay unique
    // against a log that already carries earlier runs.
    let epoch = Timestamp::now().as_micros();
    for stream in 0..concurrency {
        let laser = laser.clone();
        let seed = scenario_seed.wrapping_add(stream as u64);
        let shutdown = handle.watch();
        handle.track(tokio::spawn(publisher::run(
            laser,
            seed,
            epoch,
            session_interval,
            shutdown,
        )));
    }
    handle.track(tokio::spawn(tickets::run(
        laser.clone(),
        scenario_seed,
        handle.watch(),
    )));
    Ok(handle)
}
