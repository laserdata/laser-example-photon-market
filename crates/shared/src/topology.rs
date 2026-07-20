use crate::names::{BusinessTopic, STREAM};
use laser_sdk::prelude::{AgentTopic, Laser, LaserError};
use strum::IntoEnumIterator;

pub const PARTITIONS: u32 = 4;

const AGENT_TOPICS: [AgentTopic<'static>; 10] = [
    AgentTopic::Audit,
    AgentTopic::Commands,
    AgentTopic::Dlq,
    AgentTopic::HumanInput,
    AgentTopic::LlmIo,
    AgentTopic::Responses,
    AgentTopic::Registry,
    AgentTopic::ToolCalls,
    AgentTopic::ToolResults,
    AgentTopic::WorkflowJournal,
];

pub async fn bootstrap_all(laser: &Laser) -> Result<(), LaserError> {
    ensure_owned(laser, BusinessTopic::iter()).await?;
    ensure_agent_topics(laser).await
}

pub async fn ensure_owned(
    laser: &Laser,
    topics: impl IntoIterator<Item = BusinessTopic>,
) -> Result<(), LaserError> {
    for topic in topics {
        topic.topic(laser).ensure(PARTITIONS).await?;
    }
    Ok(())
}

pub async fn ensure_agent_topics(laser: &Laser) -> Result<(), LaserError> {
    for topic in AGENT_TOPICS {
        laser
            .stream(STREAM)
            .topic(topic.topic_string())
            .ensure(PARTITIONS)
            .await?;
    }
    Ok(())
}
