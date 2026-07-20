use crate::approvals::effect_digest;
use crate::links::RiskLinks;
use crate::reviewer::ApprovalDecision;
use laser_sdk::prelude::{
    AgentCtx, AgentHandler, AgentMessage, AgentTopic, ConversationId, LaserError, MemoryHandle,
};
use photon_shared::domain::risk::{RiskCaseEvent, RiskReason, RiskVerdict, ScreenRequest};
use photon_shared::domain::{ContractVersion, CustomerId, OrderId, Timestamp};
use photon_shared::names::{AppAgent, BusinessTopic};
use photon_shared::{BusinessProvenance, publish_business};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const REJECT_CENTS: u64 = 200_000;
const RING_SCORE_BPS: u16 = 7_000;
const PRECEDENT_SCORE_BPS: u16 = 6_000;
const PRECEDENT_REVIEW_REJECTS: usize = 2;
const REVIEW_DEADLINE: Duration = Duration::from_secs(3);

pub const RISK_SCOPE: &str = "risk:review";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RiskReviewRequest {
    pub scope: String,
    pub order: OrderId,
    pub customer: CustomerId,
    pub amount_cents: u64,
    pub score_bps: u16,
    pub reasons: Vec<RiskReason>,
    pub case_digest: String,
}

pub struct Assessment {
    pub verdict: RiskVerdict,
    pub score_bps: u16,
    pub reasons: Vec<RiskReason>,
}

// Deterministic policy, never model prose: an outsized order is rejected
// outright, a ring link or repeat-rejection precedent parks the order in
// review for the human gate, everything else clears.
pub fn assess(amount_cents: u64, rings: Vec<RiskReason>, precedent_rejects: usize) -> Assessment {
    if amount_cents >= REJECT_CENTS {
        return Assessment {
            verdict: RiskVerdict::Reject,
            score_bps: 9_000,
            reasons: vec![RiskReason::HighValue],
        };
    }
    if !rings.is_empty() {
        return Assessment {
            verdict: RiskVerdict::Review,
            score_bps: RING_SCORE_BPS,
            reasons: rings,
        };
    }
    if precedent_rejects >= PRECEDENT_REVIEW_REJECTS {
        return Assessment {
            verdict: RiskVerdict::Review,
            score_bps: PRECEDENT_SCORE_BPS,
            reasons: vec![RiskReason::PrecedentReject],
        };
    }
    Assessment {
        verdict: RiskVerdict::Clear,
        score_bps: 1_000,
        reasons: vec![],
    }
}

pub struct RiskAgent {
    pub memory: MemoryHandle,
    pub links: Arc<dyn RiskLinks>,
}

impl RiskAgent {
    // A `Review` verdict is not terminal: the human gate turns it into `Clear`
    // or `Reject`, and an absent or mismatched reviewer fails closed.
    async fn review(
        &self,
        ctx: &AgentCtx<'_>,
        request: &ScreenRequest,
        assessment: &mut Assessment,
    ) -> Result<(), LaserError> {
        let review = RiskReviewRequest {
            scope: RISK_SCOPE.to_owned(),
            order: request.order,
            customer: request.customer.clone(),
            amount_cents: request.amount_cents,
            score_bps: assessment.score_bps,
            reasons: assessment.reasons.clone(),
            case_digest: String::new(),
        };
        let case_digest = effect_digest(&serde_json::to_vec(&review).map_err(|error| {
            LaserError::Invalid(format!("cannot encode review request: {error}"))
        })?);
        let review = RiskReviewRequest {
            case_digest,
            ..review
        };
        let prompt = serde_json::to_vec(&review).map_err(|error| {
            LaserError::Invalid(format!("cannot encode review request: {error}"))
        })?;
        let approved = match ctx
            .approval_gate(AgentTopic::Responses, prompt, REVIEW_DEADLINE)
            .await
        {
            Ok(response) => {
                let decision: ApprovalDecision =
                    serde_json::from_slice(&response).map_err(|error| {
                        LaserError::Invalid(format!("undecodable review decision: {error}"))
                    })?;
                if decision.effect_digest != review.case_digest {
                    let order = request.order;
                    warn!(
                        "Review decision for order {order} does not match the submitted case. Rejecting it."
                    );
                    false
                } else {
                    if !decision.approved {
                        let order = request.order;
                        let reason = &decision.reason;
                        info!("Reviewer rejected risk case for order {order}. {reason}");
                    }
                    decision.approved
                }
            }
            Err(error) => {
                let order = request.order;
                warn!("Risk review did not complete for order {order}. Rejecting it. {error}");
                false
            }
        };
        assessment.verdict = if approved {
            RiskVerdict::Clear
        } else {
            RiskVerdict::Reject
        };
        assessment.reasons.push(RiskReason::ManualReview);
        Ok(())
    }
}

impl AgentHandler for RiskAgent {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let request: ScreenRequest = serde_json::from_slice(message.body())
            .map_err(|error| LaserError::Invalid(format!("undecodable screen request: {error}")))?;
        request
            .version
            .ensure_current()
            .map_err(|error| LaserError::Invalid(error.to_string()))?;

        // Recall this customer's precedent, then remember the new verdict, so
        // repeat customers build a case history the next screen can draw on.
        let scope = ConversationId::derive(request.customer.as_ref());
        let descriptor = format!(
            "amount {} device {} card {}",
            request.amount_cents, request.device, request.card_fingerprint
        );
        let precedent = self
            .memory
            .recall(scope)
            .semantic(descriptor)
            .limit(5)
            .fetch()
            .await?;
        let precedent_rejects = precedent
            .iter()
            .filter(|item| String::from_utf8_lossy(&item.payload).starts_with("verdict reject"))
            .count();

        let rings = self.links.rings(&request).await?;
        let mut assessment = assess(request.amount_cents, rings, precedent_rejects);
        if assessment.verdict == RiskVerdict::Review {
            self.review(ctx, &request, &mut assessment).await?;
        }
        let event = RiskCaseEvent {
            version: ContractVersion::CURRENT,
            order: request.order,
            customer: request.customer.clone(),
            device: request.device.clone(),
            card_fingerprint: request.card_fingerprint.clone(),
            score_bps: assessment.score_bps,
            verdict: assessment.verdict,
            reasons: assessment.reasons,
            at: Timestamp::now(),
        };

        let conversation = request
            .order
            .to_string()
            .parse()
            .expect("an order id is a valid conversation id");
        publish_business(
            ctx.laser(),
            BusinessTopic::RiskEvents,
            &event,
            BusinessProvenance {
                conversation,
                source: AppAgent::Risk,
                causal_parent: None,
                idempotency_key: format!("risk/{}", request.order),
            },
        )
        .await?;
        self.links.record(&event).await?;
        self.memory
            .remember(
                format!(
                    "verdict {} amount {} device {}",
                    event.verdict, request.amount_cents, request.device
                )
                .into_bytes(),
            )
            .scope(scope)
            .send()
            .await?;
        let order = request.order;
        let verdict = event.verdict;
        let score = event.score_bps;
        let reasons = &event.reasons;
        let precedent_count = precedent.len();
        info!(
            "Screened order {order}: verdict {verdict}, score {score} bps, reasons {reasons:?}, {precedent_count} precedent item(s)"
        );
        ctx.respond(
            serde_json::to_vec(&event).map_err(|error| {
                LaserError::Invalid(format!("cannot encode risk verdict: {error}"))
            })?,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_high_value_order_when_assessed_then_should_reject() {
        let assessment = assess(250_000, vec![RiskReason::DeviceRing], 5);
        assert_eq!(assessment.verdict, RiskVerdict::Reject);
        assert_eq!(assessment.reasons, vec![RiskReason::HighValue]);
    }

    #[test]
    fn given_a_ring_link_when_assessed_then_should_park_in_review() {
        let assessment = assess(79_900, vec![RiskReason::CardRing], 0);
        assert_eq!(assessment.verdict, RiskVerdict::Review);
        assert_eq!(assessment.reasons, vec![RiskReason::CardRing]);
    }

    #[test]
    fn given_repeat_rejections_when_assessed_then_should_park_in_review() {
        let assessment = assess(79_900, vec![], PRECEDENT_REVIEW_REJECTS);
        assert_eq!(assessment.verdict, RiskVerdict::Review);
        assert_eq!(assessment.reasons, vec![RiskReason::PrecedentReject]);
    }

    #[test]
    fn given_a_modest_clean_order_when_assessed_then_should_clear() {
        let assessment = assess(79_900, vec![], 1);
        assert_eq!(assessment.verdict, RiskVerdict::Clear);
        assert!(assessment.reasons.is_empty());
    }
}
