#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod approvals;
mod error;
mod governor;
mod links;
mod order_lookup;
mod refunds;
mod reviewer;
mod risk;
mod runtime;
mod support;

pub use error::Error;

use crate::approvals::ApprovalGrants;
use crate::governor::DeskGovernor;
use crate::links::RiskLinks;
use crate::reviewer::ReviewerAgent;
use crate::risk::RiskAgent;
use crate::runtime::SupportData;
use crate::support::SupportAgent;
use laser_sdk::prelude::full::{
    ActionGovernor, ConcurrencyPolicy, GovernorMode as SdkGovernorMode, InboxRoute,
};
use laser_sdk::prelude::{Agent, AgentTopic};
use laser_sdk::wire::agent::CapabilityDescriptor;
use photon_shared::domain::Money;
use photon_shared::knobs;
use photon_shared::names::{
    self, AppAgent, BusinessTopic, GovernorMode, LlmProvider, MemorySpace, Skill,
};
use photon_shared::{HANDLER_SHUTDOWN_GRACE, LaserFactory, ServiceHandle, topology};
use std::sync::Arc;
use tracing::info;

const OWNED: [BusinessTopic; 3] = [
    BusinessTopic::SupportTickets,
    BusinessTopic::OrderEvents,
    BusinessTopic::RiskEvents,
];

#[derive(Debug)]
pub struct Opts {
    pub governor: GovernorMode,
    pub refund_ceiling: Money,
    pub llm_provider: LlmProvider,
    pub skew_permille: u16,
    pub scenario_seed: u64,
}

impl Opts {
    pub fn from_env() -> Result<Self, Error> {
        Ok(Self {
            governor: knobs::governor_mode()?,
            refund_ceiling: Money(knobs::refund_ceiling_cents()?),
            llm_provider: knobs::llm_provider()?,
            skew_permille: knobs::llm_skew_permille()?,
            scenario_seed: knobs::scenario_seed()?,
        })
    }
}

pub async fn run(factory: &LaserFactory, opts: Opts) -> Result<ServiceHandle, Error> {
    let Opts {
        governor,
        refund_ceiling,
        llm_provider,
        skew_permille,
        scenario_seed,
    } = opts;
    let laser = factory.connect(names::STREAM).await?;
    topology::ensure_owned(&laser, OWNED).await?;
    topology::ensure_agent_topics(&laser).await?;

    let mut handle = ServiceHandle::new("desk");

    let approvals = Arc::new(ApprovalGrants::new());
    let refund_governor: Arc<dyn ActionGovernor> =
        Arc::new(DeskGovernor::new(refund_ceiling, approvals.clone()));
    let mode = sdk_mode(governor);

    let connection = factory
        .connect(names::STREAM)
        .await?
        .with_governor(refund_governor.clone(), mode);
    let memory = runtime::memory(&connection, MemorySpace::Risk).await;
    let links = runtime::risk_links(&connection).await;
    rebuild_risk_links(&connection, links.as_ref()).await?;
    let risk = Agent::builder()
        .id(AppAgent::Risk.id())
        .listen_on(AgentTopic::Commands)
        .respond_on(AgentTopic::Responses)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions: 8 })
        .shutdown_grace(HANDLER_SHUTDOWN_GRACE)
        .capabilities(vec![CapabilityDescriptor {
            skill_id: Skill::ScreenOrder.to_string(),
            ..Default::default()
        }])
        .ack_on_pickup(true)
        .maybe_signing_key(factory.signing_key(AppAgent::Risk))
        .handler(RiskAgent { memory, links })
        .build()
        .spawn(connection);
    handle.track_agent(risk).await?;

    let SupportData { lookup, refunds } =
        runtime::support_data(factory, &laser, &mut handle).await?;
    let llm = runtime::llm(llm_provider, scenario_seed, skew_permille)?;
    let support_connection = factory
        .connect(names::STREAM)
        .await?
        .with_governor(refund_governor.clone(), mode);
    let order_schema_id = if support_connection.capabilities().await.query.available {
        Some(photon_shared::schema::ensure_order_event_schema(&support_connection).await?)
    } else {
        None
    };
    let support_memory = runtime::memory(&support_connection, MemorySpace::Support).await;
    let support = Agent::builder()
        .id(AppAgent::Support.id())
        .listen_on(names::support_tickets_agent_topic())
        .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions: 8 })
        .shutdown_grace(HANDLER_SHUTDOWN_GRACE)
        .dedup_window(10_000)
        .handler(SupportAgent {
            llm,
            lookup,
            refunds,
            approvals,
            memory: support_memory,
            laser: support_connection.clone(),
            order_schema_id,
        })
        .build()
        .spawn(support_connection);
    handle.track_agent(support).await?;

    let reviewer = Agent::builder()
        .id(AppAgent::Reviewer.id())
        .listen_on(AgentTopic::HumanInput)
        .shutdown_grace(HANDLER_SHUTDOWN_GRACE)
        .maybe_signing_key(factory.signing_key(AppAgent::Reviewer))
        .handler(ReviewerAgent)
        .build()
        .spawn(
            factory
                .connect(names::STREAM)
                .await?
                .with_governor(refund_governor, mode),
        );
    handle.track_agent(reviewer).await?;

    let target = factory.target();
    info!(
        "Desk is live on {target}. Governor: {governor}. Refund ceiling: {refund_ceiling}. Model: {llm_provider}. Skew: {skew_permille} permille. Seed: {scenario_seed}."
    );
    Ok(handle)
}

fn sdk_mode(mode: GovernorMode) -> SdkGovernorMode {
    match mode {
        GovernorMode::Observe => SdkGovernorMode::Observe,
        GovernorMode::Enforce => SdkGovernorMode::Enforce,
    }
}

// Replay every prior case from risk.events so a restarted desk knows the same
// rings it knew before the restart.
async fn rebuild_risk_links(
    laser: &laser_sdk::prelude::Laser,
    links: &dyn RiskLinks,
) -> Result<(), Error> {
    let offsets = vec![0u64; topology::PARTITIONS as usize];
    let events = BusinessTopic::RiskEvents
        .topic(laser)
        .json::<photon_shared::domain::risk::RiskCaseEvent>();
    let mut records = events
        .records("desk-risk-links-rebuild")
        .map_err(Error::from)?
        .from_offsets(offsets);
    while let Some(next) = records.next().await {
        if let Ok(record) = next {
            links.record(&record.value).await?;
        }
    }
    Ok(())
}
