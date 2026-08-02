use crate::governor::REFUND_SCOPE;
use crate::risk::{RISK_SCOPE, RiskReviewRequest};
use laser_sdk::prelude::{AgentCtx, AgentHandler, AgentMessage, LaserError};
use photon_shared::domain::{Money, OrderId, TicketId};
use serde::{Deserialize, Serialize};

/// The reviewer clears a ring- or precedent-flagged order only under this
/// value. Anything dearer flagged by a link stays rejected.
const MANUAL_CLEAR_CENTS: u64 = 150_000;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ApprovalRequest {
    pub scope: String,
    pub ticket: TicketId,
    pub order: OrderId,
    pub order_value: Money,
    pub proposed_refund: Money,
    pub effect_digest: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ApprovalDecision {
    pub approved: bool,
    pub reason: String,
    pub effect_digest: String,
}

#[derive(Deserialize)]
struct Scoped {
    scope: String,
}

pub struct ReviewerAgent;

impl AgentHandler for ReviewerAgent {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let scoped: Scoped = serde_json::from_slice(message.body()).map_err(|error| {
            LaserError::Invalid(format!("undecodable approval request: {error}"))
        })?;
        let decision = if scoped.scope == RISK_SCOPE {
            let request: RiskReviewRequest =
                serde_json::from_slice(message.body()).map_err(|error| {
                    LaserError::Invalid(format!("undecodable risk review request: {error}"))
                })?;
            Self::decide_risk(&request)
        } else {
            let request: ApprovalRequest =
                serde_json::from_slice(message.body()).map_err(|error| {
                    LaserError::Invalid(format!("undecodable approval request: {error}"))
                })?;
            Self::decide(&request)
        };
        let response = serde_json::to_vec(&decision)
            .map_err(|error| LaserError::Invalid(format!("approval decision failed: {error}")))?;
        ctx.respond_input(laser_sdk::prelude::AgentTopic::Responses, response)
            .await
    }
}

impl ReviewerAgent {
    fn decide_risk(request: &RiskReviewRequest) -> ApprovalDecision {
        let (approved, reason) = if request.amount_cents < MANUAL_CLEAR_CENTS {
            (
                true,
                format!(
                    "flagged for {:?} but under the manual-clear ceiling at {} cents",
                    request.reasons, request.amount_cents
                ),
            )
        } else {
            (
                false,
                format!(
                    "flagged for {:?} at {} cents, over the manual-clear ceiling",
                    request.reasons, request.amount_cents
                ),
            )
        };
        ApprovalDecision {
            approved,
            reason,
            effect_digest: request.case_digest.clone(),
        }
    }

    fn decide(request: &ApprovalRequest) -> ApprovalDecision {
        let (approved, reason) = if request.scope != REFUND_SCOPE {
            (
                false,
                format!("unsupported approval scope {}", request.scope),
            )
        } else if request.proposed_refund == Money(0) {
            (
                false,
                "a zero-value refund is not a valid effect".to_owned(),
            )
        } else if request.proposed_refund != request.order_value {
            (
                false,
                format!(
                    "proposed refund {} does not match order value {}",
                    request.proposed_refund, request.order_value
                ),
            )
        } else {
            (
                true,
                format!(
                    "refund {} matches the grounded order value",
                    request.proposed_refund
                ),
            )
        };
        ApprovalDecision {
            approved,
            reason,
            effect_digest: request.effect_digest.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_grounded_refund_when_reviewed_then_should_approve_with_the_effect_digest() {
        let decision = ReviewerAgent::decide(&request(45_000, 45_000));
        assert!(decision.approved);
        assert_eq!(decision.effect_digest, "effect-digest");
    }

    #[test]
    fn given_an_inflated_refund_when_reviewed_then_should_deny_with_a_reason() {
        let decision = ReviewerAgent::decide(&request(450_000, 45_000));
        assert!(!decision.approved);
        assert_eq!(
            decision.reason,
            "proposed refund $4500.00 does not match order value $450.00"
        );
    }

    #[test]
    fn given_a_modest_flagged_case_when_reviewed_then_should_clear_with_the_case_digest() {
        let decision = ReviewerAgent::decide_risk(&risk_request(119_700));
        assert!(decision.approved);
        assert_eq!(decision.effect_digest, "case-digest");
    }

    #[test]
    fn given_a_dear_flagged_case_when_reviewed_then_should_stay_rejected() {
        let decision = ReviewerAgent::decide_risk(&risk_request(180_000));
        assert!(!decision.approved);
    }

    fn request(proposed_refund: u64, order_value: u64) -> ApprovalRequest {
        ApprovalRequest {
            scope: REFUND_SCOPE.to_owned(),
            ticket: "01ARZ3NDEKTSV4RRFFQ69G5FAW"
                .parse()
                .expect("ticket id parses"),
            order: "01ARZ3NDEKTSV4RRFFQ69G5FAX"
                .parse()
                .expect("order id parses"),
            order_value: Money(order_value),
            proposed_refund: Money(proposed_refund),
            effect_digest: "effect-digest".to_owned(),
        }
    }

    fn risk_request(amount_cents: u64) -> RiskReviewRequest {
        RiskReviewRequest {
            scope: RISK_SCOPE.to_owned(),
            order: "01ARZ3NDEKTSV4RRFFQ69G5FAX"
                .parse()
                .expect("order id parses"),
            customer: "cust-1".parse().expect("customer id parses"),
            amount_cents,
            score_bps: 7_000,
            reasons: vec![photon_shared::domain::risk::RiskReason::DeviceRing],
            case_digest: "case-digest".to_owned(),
        }
    }
}
