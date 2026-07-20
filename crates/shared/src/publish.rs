use crate::names::{AppAgent, BusinessTopic};
use laser_sdk::prelude::{ConversationId, Laser, LaserError, MessageId, Provenance};
use serde::Serialize;

pub struct BusinessProvenance {
    pub conversation: ConversationId,
    pub source: AppAgent,
    pub causal_parent: Option<MessageId>,
    pub idempotency_key: String,
}

impl BusinessProvenance {
    fn into_provenance(self) -> Provenance {
        Provenance::builder()
            .conversation_id(self.conversation)
            .agent(self.source.id())
            .maybe_causal_parent(self.causal_parent)
            .idempotency_key(self.idempotency_key)
            .build()
    }
}

pub async fn publish_business<T: Serialize>(
    laser: &Laser,
    topic: BusinessTopic,
    payload: &T,
    provenance: BusinessProvenance,
) -> Result<(), LaserError> {
    publish_business_with_schema(laser, topic, payload, provenance, None).await
}

pub async fn publish_business_with_schema<T: Serialize>(
    laser: &Laser,
    topic: BusinessTopic,
    payload: &T,
    provenance: BusinessProvenance,
    schema_id: Option<u32>,
) -> Result<(), LaserError> {
    let topic = topic.topic(laser);
    let provenance = provenance.into_provenance();
    let request = topic.publish().json(payload)?.provenance(&provenance);
    match schema_id {
        Some(schema_id) => request.schema_id(schema_id),
        None => request,
    }
    .send()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_sdk::iggy::prelude::{HeaderKey, HeaderValue};
    use laser_sdk::provenance::keys;
    use std::collections::BTreeMap;

    #[test]
    fn given_business_provenance_when_encoded_to_headers_then_should_carry_the_exact_idempotency_key()
     {
        let provenance = BusinessProvenance {
            conversation: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                .parse()
                .expect("conversation id parses"),
            source: AppAgent::Storefront,
            causal_parent: None,
            idempotency_key: "place-order/01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        }
        .into_provenance();

        let headers: BTreeMap<HeaderKey, HeaderValue> =
            (&provenance).try_into().expect("headers encode");
        let idempotency = headers
            .get(&keys::IDEMPOTENCY_KEY.parse().expect("header key"))
            .expect("idempotency header present");
        assert_eq!(
            idempotency.as_str().expect("utf8"),
            "place-order/01ARZ3NDEKTSV4RRFFQ69G5FAV"
        );
        let agent = headers
            .get(&keys::AGENT_ID.parse().expect("header key"))
            .expect("agent header present");
        assert_eq!(agent.as_str().expect("utf8"), "storefront");
    }
}
