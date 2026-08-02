use crate::{probe, report};
use laser_sdk::prelude::{ConversationId, Laser, LaserError};
use photon_shared::domain::ContractVersion;
use photon_shared::domain::catalog::{CatalogCommand, CatalogOp};
use photon_shared::knobs::{self, ConfigError};
use photon_shared::names::{self, AdversaryMode, AppAgent, BusinessTopic};
use photon_shared::trust::DemoTrust;
use photon_shared::{
    BusinessProvenance, LaserFactory, ServiceHandle, output, publish_business, shutdown_signal,
    topology,
};
use thiserror::Error;
use tracing::warn;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Laser(#[from] LaserError),
    #[error("{service} failed: {reason}")]
    Service {
        service: &'static str,
        reason: String,
    },
    #[error("{0}\nrun `photon-demo --help` for usage")]
    Usage(String),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DemoMode {
    Finite,
    #[default]
    Live,
}

impl std::fmt::Display for DemoMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DemoMode::Finite => f.write_str("finite demo"),
            DemoMode::Live => f.write_str("live until Ctrl+C"),
        }
    }
}

pub enum DemoCommand {
    Run(DemoMode),
    Help,
}

impl DemoCommand {
    pub fn from_args() -> Result<Self, Error> {
        Self::parse(std::env::args().skip(1))
    }
}

pub fn print_help() {
    output::phase("commands");
    output::fact("live", "run the simulated market until Ctrl+C (default)");
    output::fact("finite", "run the deterministic scenario, then exit");
    output::fact(
        "configuration",
        "use LASER_* environment variables. See README.md",
    );
}

pub async fn run(factory: &LaserFactory, mode: DemoMode) -> Result<(), Error> {
    let trust = DemoTrust::generate("operator");
    let verified = factory
        .clone()
        .with_verifier(trust.registry.clone())
        .with_keyring(trust.keyring.clone());
    let factory = &verified;

    output::phase("bootstrap");
    let admin = factory.connect(names::STREAM).await?;
    topology::bootstrap_all(&admin).await?;
    let caps = probe::capabilities(&admin).await;
    report::run_profile(&mode.to_string(), factory, &caps)?;
    seed_catalog(&admin).await?;
    output::act("catalog seeded");

    output::phase("services");
    let orders = start_orders(factory).await?;
    output::act("orders ready");
    let insights = start_insights(factory).await?;
    output::act("insights ready");
    let desk = start_desk(factory).await?;
    output::act("desk ready");
    let adversary = start_adversary(factory).await?;
    output::act("adversary ready");
    let mut storefront = Some(start_storefront(factory).await?);
    output::act("storefront ready");

    report::managed_summary(&caps);

    output::phase("trust");
    output::act("operator quarantines the rogue carrier with a signed fact");
    admin
        .quarantine_signed(
            AppAgent::Operator.id(),
            &AppAgent::Borealis.id(),
            &trust.operator,
        )
        .await?;

    let mut operator_repair = None;
    match mode {
        DemoMode::Live => {
            output::phase("running");
            output::act("running until Ctrl+C, kill and restart any service to see it recover");
            let mut repair = ServiceHandle::new("operator-repair");
            repair.track(tokio::spawn(photon_demo::operator::repair_loop(
                admin.clone(),
                photon_demo::operator::REPAIR_LOOP_INTERVAL,
                repair.watch(),
            )));
            output::act("operator repairing stranded orders in the background as they land");
            operator_repair = Some(repair);
            shutdown_signal().await;
        }
        DemoMode::Finite => {
            let scripted = knobs::adversary_mode()? == AdversaryMode::Scripted;
            output::phase("acts");
            output::act("letting the spine flow, then stopping new ingress");
            settle(probe::spine_settled(&admin, scripted).await, "spine flow");
            storefront
                .take()
                .expect("the finite storefront is still running")
                .shutdown()
                .await?;
            output::phase("repair");
            output::act(
                "operator triages dead letters: poison stays quarantined, stranded orders get their catalog state and a redrive",
            );
            let redriven = photon_demo::operator::repair_and_redrive(&admin).await?;
            output::act(&format!("redrove {} stranded order(s)", redriven.len()));
            settle(
                probe::repair_applied(&admin, &redriven).await,
                "catalog repair",
            );
            output::act(
                "operator lifts the quarantine with a signed fact. The rogue carrier still loses every quote.",
            );
            admin
                .unquarantine_signed(
                    AppAgent::Operator.id(),
                    &AppAgent::Borealis.id(),
                    &trust.operator,
                )
                .await?;
            settle(
                probe::unquarantine_folded(&admin, &AppAgent::Borealis.id()).await,
                "quarantine lift",
            );
            output::act("draining every order that entered intake to a terminal event");
            settle(probe::orders_drained(&admin).await, "order drain");
        }
    }

    output::phase("shutdown");
    let mut services = Vec::new();
    if let Some(storefront) = storefront {
        services.push(storefront);
    }
    services.push(adversary);
    if let Some(operator_repair) = operator_repair {
        services.push(operator_repair);
    }
    services.extend([orders, desk, insights]);
    let shutdown = shutdown_all(services).await;
    drop(admin);
    shutdown
}

async fn shutdown_all(services: Vec<ServiceHandle>) -> Result<(), Error> {
    let mut first_error = None;
    for service in services {
        if let Err(error) = service.shutdown().await
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), |error| Err(Error::Laser(error)))
}

async fn seed_catalog(laser: &Laser) -> Result<(), Error> {
    for (sku, available) in photon_storefront::catalog() {
        let command = CatalogCommand {
            version: ContractVersion::CURRENT,
            op: CatalogOp::UpsertSku {
                sku: sku.clone(),
                available,
            },
        };
        publish_business(
            laser,
            BusinessTopic::CatalogCommands,
            &command,
            BusinessProvenance {
                conversation: ConversationId::derive(sku.as_ref()),
                source: AppAgent::Operator,
                causal_parent: None,
                idempotency_key: format!("catalog-upsert/{sku}"),
            },
        )
        .await?;
    }
    Ok(())
}

// An unsettled outcome is narrated, never fatal: the demo keeps flowing and
// the next phase states its own findings honestly.
fn settle(observed: Result<(), LaserError>, outcome: &str) {
    if let Err(error) = observed {
        warn!("The {outcome} was not observed before its deadline, continuing: {error}");
    }
}

async fn start_storefront(factory: &LaserFactory) -> Result<ServiceHandle, Error> {
    let opts =
        photon_storefront::Opts::from_env().map_err(|error| Error::service("storefront", error))?;
    photon_storefront::run(factory, opts)
        .await
        .map_err(|error| Error::service("storefront", error))
}

async fn start_orders(factory: &LaserFactory) -> Result<ServiceHandle, Error> {
    let opts = photon_orders::Opts::from_env().map_err(|error| Error::service("orders", error))?;
    photon_orders::run(factory, opts)
        .await
        .map_err(|error| Error::service("orders", error))
}

async fn start_desk(factory: &LaserFactory) -> Result<ServiceHandle, Error> {
    let opts = photon_desk::Opts::from_env().map_err(|error| Error::service("desk", error))?;
    photon_desk::run(factory, opts)
        .await
        .map_err(|error| Error::service("desk", error))
}

async fn start_insights(factory: &LaserFactory) -> Result<ServiceHandle, Error> {
    let opts =
        photon_insights::Opts::from_env().map_err(|error| Error::service("insights", error))?;
    photon_insights::run(factory, opts)
        .await
        .map_err(|error| Error::service("insights", error))
}

async fn start_adversary(factory: &LaserFactory) -> Result<ServiceHandle, Error> {
    let opts =
        photon_adversary::Opts::from_env().map_err(|error| Error::service("adversary", error))?;
    photon_adversary::run(factory, opts)
        .await
        .map_err(|error| Error::service("adversary", error))
}

impl Error {
    fn service(service: &'static str, error: impl std::fmt::Display) -> Self {
        Error::Service {
            service,
            reason: error.to_string(),
        }
    }
}

impl DemoCommand {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Error> {
        let args: Vec<String> = args.into_iter().collect();
        match args.as_slice() {
            [] => Ok(DemoCommand::Run(DemoMode::Live)),
            [arg] if arg == "demo" || arg == "live" => Ok(DemoCommand::Run(DemoMode::Live)),
            [arg] if arg == "once" || arg == "finite" => Ok(DemoCommand::Run(DemoMode::Finite)),
            [arg] if arg == "help" || arg == "-h" || arg == "--help" => Ok(DemoCommand::Help),
            _ => Err(Error::Usage(format!(
                "unknown arguments: {}",
                args.join(" ")
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_no_command_when_parsed_then_should_run_until_shutdown() {
        assert!(matches!(
            DemoCommand::parse([]),
            Ok(DemoCommand::Run(DemoMode::Live))
        ));
    }

    #[test]
    fn given_live_when_parsed_then_should_run_until_shutdown() {
        assert!(matches!(
            DemoCommand::parse(["live".to_owned()]),
            Ok(DemoCommand::Run(DemoMode::Live))
        ));
    }

    #[test]
    fn given_finite_when_parsed_then_should_run_the_bounded_scenario() {
        assert!(matches!(
            DemoCommand::parse(["finite".to_owned()]),
            Ok(DemoCommand::Run(DemoMode::Finite))
        ));
    }

    #[test]
    fn given_an_unknown_command_when_parsed_then_should_reject_it() {
        assert!(matches!(
            DemoCommand::parse(["forever-ish".to_owned()]),
            Err(Error::Usage(_))
        ));
    }
}
