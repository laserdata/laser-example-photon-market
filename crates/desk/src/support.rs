use crate::approvals::{ApprovalGrants, effect_digest};
use crate::governor::REFUND_SCOPE;
use crate::order_lookup::OrderReader;
use crate::refunds::RefundLedger;
use crate::reviewer::{ApprovalDecision, ApprovalRequest};
use laser_sdk::prelude::AgentTopic;
use laser_sdk::prelude::full::{ContextAssembler, LastN, SessionPolicy};
use laser_sdk::prelude::{
    AgentCtx, AgentHandler, AgentMessage, ConversationId, Laser, LaserError, MemoryHandle,
};
use laser_sdk::wire::agent::{CorrelationId, OPERATION_CHAT, TokenUsage as AgdxTokenUsage};
use photon_shared::domain::order::{OrderEvent, OrderEventKind};
use photon_shared::domain::ticket::{Ticket, TicketKind};
use photon_shared::domain::{ContractVersion, Money, Timestamp};
use photon_shared::llm::{CompletionRequest, LlmClient, TokenUsage};
use photon_shared::names::{AppAgent, BusinessTopic};
use photon_shared::{BusinessProvenance, publish_business_with_schema};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

const SYSTEM: &str = "You are Photon Market support. Answer the customer concisely and only from the order facts you are given.";

// The support conversation flow: ground the answer, stream it as AGDX chat
// chunks, and put every refund effect behind the governor, the reviewer's
// digest-bound approval, and the refund ledger's single-owner claim. The
// grounding and ledger seams live in order_lookup and refunds.
pub struct SupportAgent {
    pub llm: Arc<dyn LlmClient>,
    pub lookup: Arc<dyn OrderReader>,
    pub refunds: Arc<dyn RefundLedger>,
    pub approvals: Arc<ApprovalGrants>,
    pub memory: MemoryHandle,
    pub laser: Laser,
    pub order_schema_id: Option<u32>,
}

impl AgentHandler for SupportAgent {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let ticket: Ticket = serde_json::from_slice(message.body())
            .map_err(|error| LaserError::Invalid(format!("undecodable support ticket: {error}")))?;
        ticket
            .version
            .ensure_current()
            .map_err(|error| LaserError::Invalid(error.to_string()))?;
        let session = SessionPolicy::PerUser.conversation_for(ticket.customer.as_ref());
        if message.provenance.conversation_id != session {
            return Err(LaserError::Invalid(
                "support ticket conversation does not match its customer session".to_owned(),
            ));
        }
        match ticket.kind {
            TicketKind::WhereIsMyOrder => self.answer(&ticket, session).await,
            TicketKind::Damaged => self.refund(&ticket, ctx).await,
        }
    }
}

impl SupportAgent {
    async fn answer(&self, ticket: &Ticket, session: ConversationId) -> Result<(), LaserError> {
        let status = self
            .lookup
            .status(&ticket.order)
            .await?
            .unwrap_or_else(|| "unknown".to_owned());
        let recalled = self
            .memory
            .recall(session)
            .semantic(&ticket.body)
            .limit(5)
            .fetch()
            .await?;
        let memory = recalled
            .iter()
            .map(|item| String::from_utf8_lossy(&item.payload).into_owned())
            .collect::<Vec<_>>()
            .join("\n");
        let recent = ContextAssembler::builder()
            .conversation_id(session)
            .topics(vec![photon_shared::names::support_tickets_agent_topic()])
            .policy(Box::new(LastN(5)))
            .build()
            .assemble(&self.laser)
            .await?;
        let recent = recent
            .iter()
            .filter_map(|message| serde_json::from_slice::<Ticket>(&message.payload).ok())
            .map(|ticket| format!("{}: {}", ticket.kind, ticket.body))
            .collect::<Vec<_>>()
            .join("\n");
        let request = CompletionRequest {
            request_key: format!("ticket/{}", ticket.ticket),
            system: SYSTEM.to_owned(),
            prompt: format!(
                "Order {} status is `{status}`. Customer asks: {}\nRecent tickets:\n{}\nRelevant support memory:\n{}",
                ticket.order,
                ticket.body,
                if recent.is_empty() { "none" } else { &recent },
                if memory.is_empty() { "none" } else { &memory },
            ),
        };
        let (reply, model, usage) = match self.llm.complete(&request).await {
            Ok(completion) => (completion.text, completion.model, completion.usage),
            Err(error) => {
                let ticket_id = ticket.ticket;
                let order = ticket.order;
                warn!(
                    "Support model failed for ticket {ticket_id} on order {order}. Using the grounded fallback. {error}"
                );
                (
                    format!(
                        "Order {} is currently {status}. Support could not generate an expanded response.",
                        ticket.order
                    ),
                    "grounded-fallback".to_owned(),
                    None,
                )
            }
        };
        let remember = self
            .memory
            .remember(
                format!(
                    "ticket {} order {} status {status} question {} answer {reply}",
                    ticket.ticket, ticket.order, ticket.body
                )
                .into_bytes(),
            )
            .scope(session)
            .agent(AppAgent::Support.id())
            .dedup()
            .send()
            .await;
        match remember {
            Ok(_) => {}
            Err(LaserError::PolicyBlocked(reason)) => {
                let ticket_id = ticket.ticket;
                let order = ticket.order;
                warn!(
                    "Governor blocked proposed memory from ticket {ticket_id} on order {order}. {reason}"
                );
            }
            Err(error) => return Err(error),
        }
        self.stream_reply(ticket, session, &reply, usage).await?;
        let skewed = reply.contains("[skew:");
        let ticket_id = ticket.ticket;
        let customer = &ticket.customer;
        let order = ticket.order;
        let recalled_count = recalled.len();
        let reply_chars = reply.chars().count();
        info!(
            "Answered ticket {ticket_id} for customer {customer} on order {order}: status {status}, model {model}, {recalled_count} recalled item(s), {reply_chars} reply chars, skewed {skewed}"
        );
        debug!("Support reply for ticket {ticket_id}: {reply}");
        Ok(())
    }

    async fn stream_reply(
        &self,
        ticket: &Ticket,
        session: ConversationId,
        reply: &str,
        usage: Option<TokenUsage>,
    ) -> Result<(), LaserError> {
        let mut stream = self
            .laser
            .agdx(
                AgentTopic::LlmIo,
                AppAgent::Support.id().wire_id(),
                session.into(),
            )
            .stream(
                CorrelationId::from_u128(ticket.ticket.as_u128()),
                OPERATION_CHAT,
            )
            .buffered(16, Duration::from_millis(10));
        for chunk in reply.as_bytes().chunks(128) {
            stream.write(chunk.to_vec()).await?;
        }
        stream.finish("stop", usage.map(agdx_usage)).await
    }

    async fn refund(&self, ticket: &Ticket, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let Some(facts) = self.lookup.facts(&ticket.order).await? else {
            let ticket_id = ticket.ticket;
            let order = ticket.order;
            warn!(
                "Support cannot refund ticket {ticket_id} because order {order} has not been observed yet"
            );
            return Ok(());
        };
        let amount = facts.charged.unwrap_or(Money(0));
        let event = OrderEvent {
            version: ContractVersion::CURRENT,
            order: ticket.order,
            customer: ticket.customer.clone(),
            sku: facts.sku,
            quantity: facts.quantity,
            at: Timestamp::now(),
            kind: OrderEventKind::Refunded { amount },
        };
        let conversation: ConversationId = ticket
            .order
            .to_string()
            .parse()
            .expect("an order id is a valid conversation id");
        let payload = serde_json::to_vec(&event).expect("refund event serializes");
        if !self.refunds.claim(ticket.ticket, &event).await? {
            let ticket_id = ticket.ticket;
            let order = ticket.order;
            info!(
                "Skipped duplicate refund from ticket {ticket_id} because order {order} already has a refund owner"
            );
            return Ok(());
        }
        match self.publish_refund(ticket, conversation, &event).await {
            Ok(()) => {}
            Err(LaserError::StepUpRequired(scope)) if scope == REFUND_SCOPE => {
                let request = ApprovalRequest {
                    scope: scope.clone(),
                    ticket: ticket.ticket,
                    order: ticket.order,
                    order_value: amount,
                    proposed_refund: amount,
                    effect_digest: effect_digest(&payload),
                };
                let prompt = serde_json::to_vec(&request).map_err(|error| {
                    LaserError::Invalid(format!("approval request failed: {error}"))
                })?;
                let response = ctx
                    .approval_gate(AgentTopic::Responses, prompt, Duration::from_secs(15))
                    .await;
                match response {
                    Ok(response) => {
                        let decision: ApprovalDecision = serde_json::from_slice(&response)
                            .map_err(|error| {
                                LaserError::Invalid(format!(
                                    "undecodable approval decision: {error}"
                                ))
                            })?;
                        if decision.effect_digest != request.effect_digest {
                            self.refunds.release(ticket.ticket, ticket.order).await?;
                            return Err(LaserError::Invalid(
                                "approval decision does not match the proposed effect".to_owned(),
                            ));
                        }
                        if !decision.approved {
                            self.refunds.release(ticket.ticket, ticket.order).await?;
                            let ticket_id = ticket.ticket;
                            let order = ticket.order;
                            let reason = &decision.reason;
                            warn!(
                                "Reviewer denied refund from ticket {ticket_id} on order {order}. {reason}"
                            );
                            return Ok(());
                        }
                        self.approvals.grant(conversation, REFUND_SCOPE, &payload);
                        self.publish_refund(ticket, conversation, &event).await?;
                    }
                    Err(LaserError::Rejected(reason)) => {
                        self.refunds.release(ticket.ticket, ticket.order).await?;
                        let ticket_id = ticket.ticket;
                        let order = ticket.order;
                        warn!(
                            "Reviewer rejected refund from ticket {ticket_id} on order {order}. {reason}"
                        );
                        return Ok(());
                    }
                    Err(LaserError::Timeout(reason)) => {
                        self.refunds.release(ticket.ticket, ticket.order).await?;
                        let ticket_id = ticket.ticket;
                        let order = ticket.order;
                        warn!(
                            "Refund approval timed out for ticket {ticket_id} on order {order}. {reason}"
                        );
                        return Ok(());
                    }
                    Err(error) => {
                        self.refunds.release(ticket.ticket, ticket.order).await?;
                        return Err(error);
                    }
                }
            }
            Err(error) => {
                self.refunds.release(ticket.ticket, ticket.order).await?;
                return Err(error);
            }
        }
        let order = ticket.order;
        let customer = &ticket.customer;
        let ticket_id = ticket.ticket;
        info!(
            "Support refunded {amount} for damaged order {order} from customer {customer} on ticket {ticket_id}"
        );
        Ok(())
    }

    async fn publish_refund(
        &self,
        ticket: &Ticket,
        conversation: ConversationId,
        event: &OrderEvent,
    ) -> Result<(), LaserError> {
        publish_business_with_schema(
            &self.laser,
            BusinessTopic::OrderEvents,
            event,
            BusinessProvenance {
                conversation,
                source: AppAgent::Support,
                causal_parent: None,
                idempotency_key: format!("order-event/{}/refunded", ticket.order),
            },
            self.order_schema_id,
        )
        .await?;
        Ok(())
    }
}

fn agdx_usage(usage: TokenUsage) -> AgdxTokenUsage {
    AgdxTokenUsage {
        input_tokens: u64::from(usage.input_tokens),
        output_tokens: u64::from(usage.output_tokens),
        ..Default::default()
    }
}
