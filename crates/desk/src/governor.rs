use crate::approvals::ApprovalGrants;
use async_trait::async_trait;
use laser_sdk::prelude::ConversationId;
use laser_sdk::prelude::LaserError;
use laser_sdk::prelude::full::{
    ActionDecision, ActionGovernor, ActionKind, GovernedAction, PolicyRef,
};
use photon_shared::domain::Money;
use photon_shared::domain::order::{OrderEvent, OrderEventKind};
use photon_shared::names::BusinessTopic;
use std::sync::Arc;

// The scope an approval must grant for a refund over the ceiling to proceed.
// The support handler catches the step-up, runs the gate, and re-sends.
pub const REFUND_SCOPE: &str = "refunds:approve";
const FABRICATED_MEMORY_MARKER: &[u8] = b"[skew:fabricate_memory]";

// Guards the one effect the desk's own agent can get wrong: paying out a
// refund. A refund at or under the ceiling records observed evidence and
// proceeds. One over the ceiling steps up to the approval gate so the reviewer
// sees the amount before it goes out. Every other governed action is allowed,
// so the governor guards this effect without touching the rest of the desk.
pub struct DeskGovernor {
    grants: Arc<ApprovalGrants>,
    ceiling: Money,
    order_events_topic: String,
}

impl DeskGovernor {
    pub fn new(ceiling: Money, grants: Arc<ApprovalGrants>) -> Self {
        Self {
            grants,
            ceiling,
            order_events_topic: BusinessTopic::OrderEvents.to_string(),
        }
    }

    fn refund_event(&self, action: &GovernedAction<'_>) -> Result<Option<OrderEvent>, LaserError> {
        if action.kind != ActionKind::Publish || action.topic != self.order_events_topic {
            return Ok(None);
        }
        let event = serde_json::from_slice(action.payload).map_err(|error| {
            LaserError::Invalid(format!(
                "refund governor could not decode order.events publish: {error}"
            ))
        })?;
        Ok(Some(event))
    }

    fn approved(&self, conversation: Option<ConversationId>, action: &GovernedAction<'_>) -> bool {
        conversation
            .map(|conversation| {
                self.grants
                    .consume(conversation, REFUND_SCOPE, action.payload)
            })
            .unwrap_or(false)
    }

    fn refund_amount(event: &OrderEvent) -> Option<Money> {
        match event.kind {
            OrderEventKind::Refunded { amount } => Some(amount),
            _ => None,
        }
    }

    fn policy(rule: &str) -> PolicyRef {
        PolicyRef {
            pack_id: "desk-refunds".to_owned(),
            pack_version: "1".to_owned(),
            rule_ids: vec![rule.to_owned()],
        }
    }
}

#[async_trait]
impl ActionGovernor for DeskGovernor {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        if action.kind == ActionKind::MemoryWrite
            && action
                .payload
                .windows(FABRICATED_MEMORY_MARKER.len())
                .any(|window| window == FABRICATED_MEMORY_MARKER)
        {
            return Ok(ActionDecision::block("fabricated memory marker")
                .with_policy(Self::policy("memory-hygiene"))
                .with_reason("the proposed support memory contains a skew marker"));
        }
        let Some(event) = self.refund_event(action)? else {
            return Ok(ActionDecision::allow());
        };
        let Some(amount) = Self::refund_amount(&event) else {
            return Ok(ActionDecision::allow());
        };
        if amount > self.ceiling && self.approved(action.conversation, action) {
            return Ok(ActionDecision::observe()
                .with_policy(Self::policy("refund-ceiling"))
                .with_reason(format!(
                    "a reviewer granted the one-use approval for refund {amount}"
                )));
        }
        if amount > self.ceiling {
            Ok(ActionDecision::step_up(REFUND_SCOPE)
                .with_policy(Self::policy("refund-ceiling"))
                .with_reason(format!(
                    "a refund of {amount} is over the {} ceiling and needs approval",
                    self.ceiling
                ))
                .with_risk_score(1.0))
        } else {
            Ok(ActionDecision::observe()
                .with_policy(Self::policy("refund-ceiling"))
                .with_reason(format!(
                    "a refund of {amount} is within the {} ceiling",
                    self.ceiling
                )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_sdk::prelude::full::{ActionCounters, ActionKind, Verdict};
    use photon_shared::domain::order::OrderEvent;
    use photon_shared::domain::{ContractVersion, Timestamp};
    use std::sync::Arc;

    fn refund_event(amount: u64) -> Vec<u8> {
        let event = OrderEvent {
            version: ContractVersion::CURRENT,
            order: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                .parse()
                .expect("order id parses"),
            customer: "cust-1".parse().expect("customer id parses"),
            sku: "sku-1".parse().expect("sku parses"),
            quantity: 1,
            at: Timestamp::from_micros(7),
            kind: OrderEventKind::Refunded {
                amount: Money(amount),
            },
        };
        serde_json::to_vec(&event).expect("serialize refund event")
    }

    fn action<'a>(topic: &'a str, payload: &'a [u8]) -> GovernedAction<'a> {
        GovernedAction {
            kind: ActionKind::Publish,
            stream: "laser",
            topic,
            source: Some("support"),
            target: None,
            conversation: Some(
                "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                    .parse()
                    .expect("conversation parses"),
            ),
            correlation: None,
            operation: None,
            tool: None,
            on_behalf_of: None,
            purpose: None,
            data_classification: None,
            payload,
            signed: false,
            counters: ActionCounters::default(),
        }
    }

    #[tokio::test]
    async fn given_a_refund_over_the_ceiling_when_decided_then_should_step_up() {
        let governor = DeskGovernor::new(Money(30_000), Arc::new(ApprovalGrants::new()));
        let payload = refund_event(45_000);
        let decision = action("order.events", &payload);
        let decision = governor.decide(&decision).await.expect("decides");
        assert_eq!(
            decision.verdict,
            Verdict::StepUp {
                scope: REFUND_SCOPE.to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn given_a_refund_within_the_ceiling_when_decided_then_should_observe() {
        let governor = DeskGovernor::new(Money(30_000), Arc::new(ApprovalGrants::new()));
        let payload = refund_event(12_000);
        let decision = action("order.events", &payload);
        let decision = governor.decide(&decision).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Observe);
    }

    #[tokio::test]
    async fn given_a_non_refund_action_when_decided_then_should_allow() {
        let governor = DeskGovernor::new(Money(30_000), Arc::new(ApprovalGrants::new()));
        let payload = refund_event(99_000);
        let decision = action("support.tickets", &payload);
        let decision = governor.decide(&decision).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Allow);
    }

    #[tokio::test]
    async fn given_a_matching_approval_when_decided_then_should_observe_and_consume_it() {
        let grants = Arc::new(ApprovalGrants::new());
        let governor = DeskGovernor::new(Money(30_000), grants.clone());
        let payload = refund_event(45_000);
        grants.grant(
            "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                .parse()
                .expect("conversation parses"),
            REFUND_SCOPE,
            &payload,
        );

        let decision = governor
            .decide(&action("order.events", &payload))
            .await
            .expect("decides");
        assert_eq!(decision.verdict, Verdict::Observe);
        assert!(
            !grants.consume(
                "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                    .parse()
                    .expect("conversation parses"),
                REFUND_SCOPE,
                &payload,
            ),
            "the approval is one-use"
        );
    }

    #[tokio::test]
    async fn given_a_fabricated_memory_when_decided_then_should_block_it() {
        let governor = DeskGovernor::new(Money(30_000), Arc::new(ApprovalGrants::new()));
        let payload = b"customer likes blue [skew:fabricate_memory]";
        let mut action = action("agent.audit", payload);
        action.kind = ActionKind::MemoryWrite;
        let decision = governor.decide(&action).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Block);
        assert_eq!(
            decision.reason.as_deref(),
            Some("the proposed support memory contains a skew marker")
        );
    }
}
