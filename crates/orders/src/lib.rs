#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod carriers;
mod catalog;
mod charge;
mod delivery;
mod error;
mod events;
mod fulfillment;
mod handler;
mod inventory;
mod quotes;
mod recovery;
mod runtime;
mod saga;
mod state;

pub use error::Error;

use crate::carriers::Carrier;
use crate::catalog::CatalogHandler;
use crate::fulfillment::Fulfillment;
use crate::handler::OrdersHandler;
use crate::runtime::Runtime;
use futures::stream::{FuturesUnordered, StreamExt};
use laser_sdk::prelude::full::{ConcurrencyPolicy, InboxRoute, ReliableConsumer, SlidingWindow};
use laser_sdk::prelude::{
    Agent, AgentHandler, AgentId, AgentTopic, ConsumerGroupName, Laser, LaserError,
};
use laser_sdk::wire::agent::CapabilityDescriptor;
use photon_shared::knobs;
use photon_shared::names::{self, AppAgent, BusinessTopic, Skill, WorkflowStep};
use photon_shared::{HANDLER_SHUTDOWN_GRACE, LaserFactory, ServiceHandle, topology};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{info, warn};

const OWNED: [BusinessTopic; 3] = [
    BusinessTopic::OrderCommands,
    BusinessTopic::CatalogCommands,
    BusinessTopic::OrderEvents,
];
const DEDUP_WINDOW: usize = 10_000;

#[derive(Debug)]
pub struct Opts {
    pub risk_threshold_cents: u64,
    pub crash_after: Option<WorkflowStep>,
    pub zombie_charge: bool,
}

impl Opts {
    pub fn from_env() -> Result<Self, Error> {
        Ok(Self {
            risk_threshold_cents: knobs::risk_threshold_cents()?,
            crash_after: knobs::crash_after()?,
            zombie_charge: knobs::zombie_charge()?,
        })
    }
}

pub async fn run(factory: &LaserFactory, opts: Opts) -> Result<ServiceHandle, Error> {
    let Opts {
        risk_threshold_cents,
        crash_after,
        zombie_charge,
    } = opts;
    let laser = factory.connect(names::STREAM).await?;
    topology::ensure_owned(&laser, OWNED).await?;
    topology::ensure_agent_topics(&laser).await?;

    let runtime = Runtime::prepare(&laser, zombie_charge).await?;
    let mut handle = ServiceHandle::new("orders");
    // Carriers stop only after the main drain: a fulfillment handler caught in
    // flight at shutdown completes its quote or booking round-trip instead of
    // waiting out its carrier deadlines and blowing the drain grace.
    let mut carriers = ServiceHandle::new("orders-carriers");

    let Runtime {
        inventory,
        charges,
        events,
        read_your_writes,
    } = runtime;
    let (workflows, workflow_tasks) = mpsc::unbounded_channel();
    let delivery = delivery::consumer(&laser).await?;
    handle.track(tokio::spawn(delivery::run(
        laser.clone(),
        delivery,
        events.clone(),
        handle.watch(),
    )));
    for carrier in [Carrier::hermes(), Carrier::atlas()] {
        let connection = factory.connect(names::STREAM).await?;
        let agent = Agent::builder()
            .id(carrier.carrier.id())
            .listen_on(AgentTopic::Sessions)
            .respond_on(AgentTopic::Sessions)
            .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
            .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions: 8 })
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
            .maybe_signing_key(factory.signing_key(carrier.carrier))
            .handler(carrier)
            .build()
            .spawn(connection);
        carriers.track_agent(agent).await?;
    }
    handle.track_dependency(carriers);
    {
        let connection = factory.connect(names::STREAM).await?;
        let agent = Agent::builder()
            .id(AppAgent::Fulfillment.id())
            .listen_on(AgentTopic::Sessions)
            .respond_on(AgentTopic::Sessions)
            .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
            .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions: 8 })
            .shutdown_grace(HANDLER_SHUTDOWN_GRACE)
            .maybe_signing_key(factory.signing_key(AppAgent::Fulfillment))
            .handler(Fulfillment {
                charges,
                events: events.clone(),
            })
            .build()
            .spawn(connection);
        handle.track_agent(agent).await?;
    }

    let catalog_agent =
        AgentId::new(format!("{}-catalog", AppAgent::Orders.id())).map_err(LaserError::from)?;
    let workflow_shutdown = handle.watch();
    spawn_consumer(
        &mut handle,
        laser.clone(),
        catalog_agent,
        BusinessTopic::CatalogCommands.to_string(),
        CatalogHandler {
            inventory: inventory.clone(),
        },
    )
    .await?;
    spawn_consumer(
        &mut handle,
        laser.clone(),
        AppAgent::Orders.id(),
        BusinessTopic::OrderCommands.to_string(),
        OrdersHandler {
            inventory: inventory.clone(),
            risk_threshold_cents,
            crash_after,
            events: events.clone(),
            read_your_writes,
            workflows: workflows.clone(),
            shutdown: workflow_shutdown,
        },
    )
    .await?;

    for task in
        recovery::resume_incomplete_sagas(&laser, &inventory, crash_after, events, handle.watch())
            .await?
    {
        workflows
            .send(task)
            .map_err(|_| LaserError::Invalid("orders workflow owner has stopped".to_owned()))?;
    }
    drop(workflows);
    handle.track(tokio::spawn(own_workflows(workflow_tasks)));

    let target = factory.target();
    info!(
        "Orders is live on {target}. Risk screening starts at {risk_threshold_cents} cents. Crash simulation: {crash_after:?}. Managed stale-charge test: {zombie_charge}."
    );
    Ok(handle)
}

async fn own_workflows(mut incoming: mpsc::UnboundedReceiver<JoinHandle<()>>) {
    let mut tasks = FuturesUnordered::new();
    let mut open = true;
    while open || !tasks.is_empty() {
        tokio::select! {
            task = incoming.recv(), if open => match task {
                Some(task) => tasks.push(task),
                None => open = false,
            },
            result = tasks.next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    warn!("Orders workflow task did not join cleanly. {error}");
                }
            }
        }
    }
}

async fn spawn_consumer<H>(
    handle: &mut ServiceHandle,
    laser: Laser,
    agent: AgentId,
    topic: String,
    handler: H,
) -> Result<(), Error>
where
    H: AgentHandler + Send + Sync + 'static,
{
    let group = agent.to_string();
    let consumer = ReliableConsumer::builder()
        .group(ConsumerGroupName::for_agent(&agent))
        .agent(agent)
        .topic(topic)
        .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions: 8 })
        .shutdown_grace(HANDLER_SHUTDOWN_GRACE)
        .deduplicator(Box::new(SlidingWindow::new(DEDUP_WINDOW)))
        .build();

    let (ready_tx, ready_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let mut watch = handle.watch();
    handle.track(tokio::spawn(async move {
        watch.cancelled().await;
        let _ = shutdown_tx.send(());
    }));
    handle.track_result(async move {
        consumer
            .run(&laser, handler, ready_tx, shutdown_rx)
            .await
            .map_err(|error| {
                LaserError::Invalid(format!(
                    "orders consumer group '{group}' stopped with an error: {error}"
                ))
            })
    });

    ready_rx.await.map_err(|_| {
        Error::Laser(LaserError::Invalid(
            "orders consumer failed before readiness".to_owned(),
        ))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_the_orders_agents_when_identities_derive_then_should_keep_agent_and_group_distinct_types()
     {
        let orders = AppAgent::Orders.id();
        let catalog = AgentId::new(format!("{orders}-catalog"))
            .expect("the derived catalog agent id is valid");
        assert_ne!(orders, catalog, "each consumer owns its own logical agent");
        let group = ConsumerGroupName::for_agent(&catalog);
        assert_eq!(
            group.as_str(),
            catalog.as_str(),
            "the deployment group defaults from the agent id spelling"
        );
    }
}
