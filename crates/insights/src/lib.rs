#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod dashboards;
mod managed;
mod projections;

use crate::dashboards::Dashboards;
use laser_sdk::prelude::LaserError;
use laser_sdk::prelude::full::AgentTopic;
use photon_shared::knobs::{self, ConfigError};
use photon_shared::names::{self, BusinessTopic};
use photon_shared::{LaserFactory, ServiceHandle, output, topology};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tracing::info;

const OWNED: [BusinessTopic; 2] = [BusinessTopic::ShopEvents, BusinessTopic::OrderEvents];

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Laser(#[from] LaserError),
}

#[derive(Debug)]
pub struct Opts {
    pub apply_plan: bool,
}

impl Opts {
    pub fn from_env() -> Result<Self, Error> {
        Ok(Self {
            apply_plan: knobs::apply_plan()?,
        })
    }
}

pub async fn run(factory: &LaserFactory, opts: Opts) -> Result<ServiceHandle, Error> {
    let Opts { apply_plan } = opts;
    let laser = factory.connect(names::STREAM).await?;
    topology::ensure_owned(&laser, OWNED).await?;
    let dlq = laser
        .stream(names::STREAM)
        .topic(AgentTopic::Dlq.topic_string());
    dlq.ensure(topology::PARTITIONS).await?;
    let capabilities = laser.capabilities().await;
    if capabilities.query.available {
        projections::register(&laser).await?;
        info!("Registered the managed projections, bindings, retention, and watch notifications");
    } else {
        output::gate("projections, query dashboards, watch, and forks", false);
    }

    let dashboards = Arc::new(Mutex::new(Dashboards::default()));
    let mut handle = ServiceHandle::new("insights");

    handle.track(tokio::spawn(dashboards::fold_shop(
        laser.clone(),
        "insights-funnel".to_owned(),
        dashboards.clone(),
        handle.watch(),
    )));
    handle.track(tokio::spawn(dashboards::fold_orders(
        laser.clone(),
        "insights-orders".to_owned(),
        dashboards.clone(),
        handle.watch(),
    )));
    handle.track(tokio::spawn(dashboards::fold_dead_letters(
        dlq,
        "insights-dlq".to_owned(),
        dashboards.clone(),
        handle.watch(),
    )));
    let audit = laser
        .stream(names::STREAM)
        .topic(AgentTopic::Audit.topic_string());
    audit.ensure(topology::PARTITIONS).await?;
    handle.track(tokio::spawn(dashboards::fold_evidence(
        audit,
        "insights-evidence".to_owned(),
        dashboards.clone(),
        handle.watch(),
    )));
    handle.track(tokio::spawn(dashboards::render_loop(
        dashboards,
        handle.watch(),
    )));
    if capabilities.query.available {
        handle.track(tokio::spawn(managed::query_loop(
            laser.clone(),
            handle.watch(),
        )));
    }
    if capabilities.watch {
        handle.track(tokio::spawn(managed::watch_loop(
            laser.clone(),
            handle.watch(),
        )));
    } else {
        output::gate("projection watch feed", false);
    }
    if capabilities.agent_workflow {
        handle.track(tokio::spawn(managed::run_registry_loop(
            laser.clone(),
            handle.watch(),
        )));
    } else {
        output::gate("registered workflow run listing", false);
    }
    if capabilities.forks {
        handle.track(tokio::spawn(managed::flash_sale(laser.clone(), apply_plan)));
    } else {
        output::gate("flash-sale fork what-if", false);
    }

    let target = factory.target();
    info!(
        "Insights is live on {target}. It folds dashboards, dead letters, and policy evidence. Apply flash-sale plan: {apply_plan}."
    );
    Ok(handle)
}
