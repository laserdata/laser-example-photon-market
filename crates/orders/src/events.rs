use laser_sdk::prelude::{ConversationId, Laser, LaserError};
use photon_shared::domain::order::OrderEvent;
use photon_shared::names::{AppAgent, BusinessTopic};
use photon_shared::{BusinessProvenance, publish_business_with_schema};
use std::sync::Arc;

use crate::state::{ConversationState, OrderStates};

#[derive(Clone)]
pub struct Publisher {
    schema_id: Option<u32>,
    states: Arc<OrderStates>,
}

impl Publisher {
    pub fn new(schema_id: Option<u32>, states: Arc<OrderStates>) -> Self {
        Self { schema_id, states }
    }

    pub fn state(&self, order: photon_shared::domain::OrderId) -> Option<ConversationState> {
        self.states.get(order)
    }

    pub async fn emit(&self, laser: &Laser, event: &OrderEvent) -> Result<(), LaserError> {
        let conversation: ConversationId = event
            .order
            .to_string()
            .parse()
            .expect("an order id is a valid conversation id");
        publish_business_with_schema(
            laser,
            BusinessTopic::OrderEvents,
            event,
            BusinessProvenance {
                conversation,
                source: AppAgent::Orders,
                causal_parent: None,
                idempotency_key: format!("order-event/{}/{}", event.order, event.kind),
            },
            self.schema_id,
        )
        .await?;
        self.states.observe(event);
        Ok(())
    }
}
