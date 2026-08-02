use crate::names::STREAM;
use crate::topology::PARTITIONS;
use laser_sdk::iggy::prelude::{Consumer, Identifier, MessageClient, PollingStrategy};
use laser_sdk::prelude::{AgentTopic, Laser, LaserError};
use laser_sdk::wire::agent::AgentDeadLetter;
use laser_sdk::wire::framing::decode_named;

const SCAN_BATCH: u32 = 100;

/// Read every dead-letter capsule currently on the log, the operator's view of
/// what is quarantined. A bounded scan from offset zero, so callers see the
/// full history rather than only what arrives after they subscribe.
pub async fn scan(laser: &Laser) -> Result<Vec<AgentDeadLetter>, LaserError> {
    let stream = Identifier::named(STREAM)?;
    let topic = Identifier::named(&AgentTopic::Dlq.topic_string())?;
    let reader = Consumer::new(Identifier::named("dead-letter-scan")?);
    let mut capsules = Vec::new();
    for partition in 0..PARTITIONS {
        let mut offset = 0u64;
        loop {
            let polled = laser
                .client()
                .poll_messages(
                    &stream,
                    &topic,
                    Some(partition),
                    &reader,
                    &PollingStrategy::offset(offset),
                    SCAN_BATCH,
                    false,
                )
                .await?;
            if polled.messages.is_empty() {
                break;
            }
            for message in polled.messages {
                offset = message.header.offset + 1;
                if let Ok(capsule) = decode_named::<AgentDeadLetter>(&message.payload) {
                    capsules.push(capsule);
                }
            }
        }
    }
    Ok(capsules)
}
