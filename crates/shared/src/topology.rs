use crate::names::{BusinessTopic, STREAM};
use laser_sdk::iggy::prelude::{
    CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize, StreamClient, TopicClient,
};
use laser_sdk::prelude::{AgentTopic, Laser, LaserError};
use std::collections::{HashMap, HashSet};
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

/// Idempotently provisions `topics`, grouped by their owning stream.
///
/// Goes straight at the raw `IggyClient` instead of the per-topic
/// `Topic::ensure()` helper: that helper re-checks its stream's existence on
/// every single call, so N topics on one stream cost 2N round trips even
/// when everything is already provisioned. Against a local broker that's
/// free; against a cloud endpoint each round trip is real latency, and it's
/// the dominant cost of a demo run's startup. Listing a stream's topics once
/// and diffing locally cuts that to two round trips per stream on a warm
/// start.
pub async fn ensure_owned(
    laser: &Laser,
    topics: impl IntoIterator<Item = BusinessTopic>,
) -> Result<(), LaserError> {
    let mut by_stream: HashMap<&str, Vec<String>> = HashMap::new();
    for topic in topics {
        by_stream
            .entry(topic.stream())
            .or_default()
            .push(topic.to_string());
    }
    for (stream, names) in by_stream {
        ensure_stream_topics(laser, stream, &names).await?;
    }
    Ok(())
}

pub async fn ensure_agent_topics(laser: &Laser) -> Result<(), LaserError> {
    let names: Vec<String> = AGENT_TOPICS.iter().map(AgentTopic::topic_string).collect();
    ensure_stream_topics(laser, STREAM, &names).await
}

async fn ensure_stream_topics(
    laser: &Laser,
    stream: &str,
    topics: &[String],
) -> Result<(), LaserError> {
    let client = laser.client();
    let stream_id = Identifier::named(stream)?;

    if client.get_stream(&stream_id).await?.is_none()
        && let Err(error) = client.create_stream(stream).await
        && client.get_stream(&stream_id).await?.is_none()
    {
        return Err(error.into());
    }

    let existing: HashSet<String> = client
        .get_topics(&stream_id)
        .await?
        .into_iter()
        .map(|topic| topic.name)
        .collect();

    for name in topics {
        if existing.contains(name) {
            continue;
        }
        let topic_id = Identifier::named(name)?;
        let result = client
            .create_topic(
                &stream_id,
                name,
                PARTITIONS,
                CompressionAlgorithm::default(),
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await;
        if let Err(error) = result
            && client.get_topic(&stream_id, &topic_id).await?.is_none()
        {
            return Err(error.into());
        }
    }
    Ok(())
}
