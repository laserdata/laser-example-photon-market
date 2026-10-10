use crate::names::{BusinessTopic, STREAM};
use laser_sdk::iggy::prelude::{
    CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize, StreamClient, TopicClient,
    TopicCreateOptions,
};
use laser_sdk::prelude::{AgentTopic, Laser, LaserError, TopicRetention};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use strum::IntoEnumIterator;

pub const PARTITIONS: u32 = 4;

const SESSION_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

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

// The session lane, heartbeats, and agent satellites come from the SDK's own
// bootstrap, which applies the retention each one needs and registers the
// stream as a session source where the deployment indexes sessions. The
// registry topic is otherwise created by the first card, so it is ensured here
// for readers that start before any agent advertises.
pub async fn ensure_agent_topics(laser: &Laser) -> Result<(), LaserError> {
    laser
        .sessions()
        .bootstrap(PARTITIONS, TopicRetention::expire_after(SESSION_RETENTION))
        .await?;
    ensure_stream_topics(laser, STREAM, &[AgentTopic::Registry.topic_string()]).await
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
    let options = TopicCreateOptions {
        partitions_count: Some(PARTITIONS),
        compression_algorithm: Some(CompressionAlgorithm::default()),
        message_expiry: Some(IggyExpiry::NeverExpire),
        max_topic_size: Some(MaxTopicSize::ServerDefault),
        ..TopicCreateOptions::default()
    };

    for name in topics {
        if existing.contains(name) {
            continue;
        }
        let topic_id = Identifier::named(name)?;
        let result = client.create_topic(&stream_id, name, &options).await;
        if let Err(error) = result
            && client.get_topic(&stream_id, &topic_id).await?.is_none()
        {
            return Err(error.into());
        }
    }
    Ok(())
}
