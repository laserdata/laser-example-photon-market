use laser_sdk::govern::{POLICY_DECISION_OPERATION, PolicyEvidence};
use laser_sdk::prelude::{Consumer, Laser, LaserError, Topic};
use laser_sdk::swarm::SwarmActivity;
use laser_sdk::wire::agent::{AgentDeadLetter, AgentEnvelope};
use laser_sdk::wire::framing::decode_named;
use photon_shared::domain::order::OrderEvent;
use photon_shared::domain::shop::ShopEvent;
use photon_shared::names::BusinessTopic;
use photon_shared::{ShutdownWatch, output};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn};

#[derive(Default)]
pub struct Dashboards {
    funnel: BTreeMap<String, u64>,
    orders: BTreeMap<String, u64>,
    dead_letters: u64,
    evidence: EvidenceChains,
    swarm: SwarmActivity,
}

impl Dashboards {
    fn count_shop(&mut self, event: &ShopEvent) {
        *self.funnel.entry(event.kind.to_string()).or_default() += 1;
    }

    fn count_order(&mut self, event: &OrderEvent) {
        *self.orders.entry(event.kind.to_string()).or_default() += 1;
    }

    fn count_dead_letter(&mut self) {
        self.dead_letters += 1;
    }

    fn fold_evidence(&mut self, evidence: &PolicyEvidence) {
        self.evidence.fold(evidence);
        self.swarm.observe(evidence);
    }

    fn live_status(&self) -> String {
        format!(
            "shop {} browse / {} checkout | orders {} accepted / {} shipped / {} delivered / {} rejected / {} refunded | safety {} dlq / {} decisions / {} violations",
            count(&self.funnel, "browse"),
            count(&self.funnel, "checkout"),
            count(&self.orders, "accepted"),
            count(&self.orders, "shipped"),
            count(&self.orders, "delivered"),
            count(&self.orders, "rejected"),
            count(&self.orders, "refunded"),
            self.dead_letters,
            self.evidence.decisions,
            self.evidence.violations,
        )
    }
}

fn count(counts: &BTreeMap<String, u64>, key: &str) -> u64 {
    counts.get(key).copied().unwrap_or_default()
}

impl std::fmt::Display for Dashboards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "funnel [{}] | orders [{}] | dead letters {} | evidence {} | agents [{}]",
            Counts(&self.funnel),
            Counts(&self.orders),
            self.dead_letters,
            self.evidence,
            AgentCounts(&self.swarm)
        )
    }
}

struct AgentCounts<'a>(&'a SwarmActivity);

impl std::fmt::Display for AgentCounts<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for (agent, activity) in self.0.agents() {
            if !first {
                f.write_str(" ")?;
            }
            write!(f, "{agent}={}", activity.decisions)?;
            first = false;
        }
        if first {
            f.write_str("no policy decisions")?;
        }
        Ok(())
    }
}

struct Counts<'a>(&'a BTreeMap<String, u64>);

impl std::fmt::Display for Counts<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for (kind, count) in self.0 {
            if !first {
                f.write_str(" ")?;
            }
            write!(f, "{kind}={count}")?;
            first = false;
        }
        Ok(())
    }
}

/// Verifies the governor's per-conversation evidence chains as far as they can
/// honestly be verified: each record must link to the digest of the previous
/// record in its conversation, and a record with no predecessor is rendered as
/// a visible segment start. The governor's chain state is process-local, so a
/// restarted service legitimately begins a fresh segment. A non-empty
/// predecessor that does not match the last seen digest is a real violation.
#[derive(Default)]
pub struct EvidenceChains {
    last_digest: HashMap<String, String>,
    seen_decisions: HashSet<String>,
    decisions: u64,
    segments: u64,
    violations: u64,
}

impl EvidenceChains {
    pub fn fold(&mut self, evidence: &PolicyEvidence) {
        if !self.seen_decisions.insert(evidence.decision_id.clone()) {
            return;
        }
        self.decisions += 1;
        let conversation = evidence.conversation.clone().unwrap_or_default();
        match (
            &evidence.previous_digest,
            self.last_digest.get(&conversation),
        ) {
            (None, _) => {
                self.segments += 1;
                let decision = &evidence.decision;
                let outcome = &evidence.outcome;
                info!(
                    "Evidence segment starts for conversation {conversation}: decision {decision}, outcome {outcome} (fresh governor or restart boundary)"
                );
            }
            (Some(previous), Some(last)) if previous == last => {}
            (Some(previous), last) => {
                self.violations += 1;
                let expected = last.map(String::as_str).unwrap_or("none");
                warn!(
                    "Evidence chain violation for conversation {conversation}: expected predecessor {expected}, got {previous}"
                );
            }
        }
        self.last_digest
            .insert(conversation, evidence.receipt_digest.clone());
    }
}

impl std::fmt::Display for EvidenceChains {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} decisions / {} segments / {} violations",
            self.decisions, self.segments, self.violations
        )
    }
}

pub type Shared = Arc<Mutex<Dashboards>>;

pub async fn fold_shop(laser: Laser, group: String, dashboards: Shared, shutdown: ShutdownWatch) {
    match build_consumer(BusinessTopic::ShopEvents.topic(&laser), &group).await {
        Ok(consumer) => {
            run_fold(consumer, shutdown, |payload| {
                if let Ok(event) = serde_json::from_slice::<ShopEvent>(payload) {
                    dashboards
                        .lock()
                        .expect("insights dashboards lock is not poisoned")
                        .count_shop(&event);
                }
            })
            .await
        }
        Err(error) => warn!("Insights could not start the funnel fold: {error}"),
    }
}

pub async fn fold_orders(laser: Laser, group: String, dashboards: Shared, shutdown: ShutdownWatch) {
    match build_consumer(BusinessTopic::OrderEvents.topic(&laser), &group).await {
        Ok(consumer) => {
            run_fold(consumer, shutdown, |payload| {
                if let Ok(event) = serde_json::from_slice::<OrderEvent>(payload) {
                    dashboards
                        .lock()
                        .expect("insights dashboards lock is not poisoned")
                        .count_order(&event);
                }
            })
            .await
        }
        Err(error) => warn!("Insights could not start the order-state fold: {error}"),
    }
}

pub async fn fold_dead_letters(
    dlq: Topic,
    group: String,
    dashboards: Shared,
    shutdown: ShutdownWatch,
) {
    match build_consumer(dlq, &group).await {
        Ok(consumer) => {
            run_fold(consumer, shutdown, |payload| {
                match decode_named::<AgentDeadLetter>(payload) {
                    Ok(capsule) => {
                        let partition = capsule.source.partition_id;
                        let offset = capsule.source.offset;
                        let attempts = capsule.attempts;
                        let reason = capsule.reason;
                        let detail = capsule.detail.as_deref().unwrap_or("none");
                        info!("Contained dead letter at partition {partition}, offset {offset} after {attempts} attempt(s): reason {reason:?}, detail {detail}");
                    }
                    Err(error) => warn!("Dead letter carried an undecodable capsule: {error}"),
                }
                dashboards
                    .lock()
                    .expect("insights dashboards lock is not poisoned")
                    .count_dead_letter();
            })
            .await
        }
        Err(error) => warn!("Insights could not start the dead-letter tail: {error}"),
    }
}

pub async fn fold_evidence(
    audit: Topic,
    group: String,
    dashboards: Shared,
    shutdown: ShutdownWatch,
) {
    match build_consumer(audit, &group).await {
        Ok(consumer) => {
            run_fold(consumer, shutdown, |payload| {
                let Ok(envelope) = decode_named::<AgentEnvelope>(payload) else {
                    return;
                };
                if envelope.operation.as_deref() != Some(POLICY_DECISION_OPERATION) {
                    return;
                }
                match PolicyEvidence::decode(&envelope.body) {
                    Ok(evidence) => dashboards
                        .lock()
                        .expect("insights dashboards lock is not poisoned")
                        .fold_evidence(&evidence),
                    Err(error) => warn!("Audit carried an undecodable policy evidence: {error}"),
                }
            })
            .await
        }
        Err(error) => warn!("Insights could not start the evidence tail: {error}"),
    }
}

pub async fn render_loop(dashboards: Shared, mut shutdown: ShutdownWatch) {
    let started = tokio::time::Instant::now();
    let start = started + Duration::from_secs(2);
    let mut ticks = tokio::time::interval_at(start, Duration::from_secs(4));
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = ticks.tick() => {
                let status = dashboards.lock().expect("insights dashboards lock is not poisoned").live_status();
                output::live_status(started.elapsed(), &status);
            }
        }
    }
}

async fn build_consumer(topic: Topic, group: &str) -> Result<Consumer, LaserError> {
    topic
        .consumer_group(group)
        .batch_length(200)
        .poll_interval(Duration::from_millis(10))
        .build()
        .await
}

async fn run_fold<F: FnMut(&[u8])>(
    mut consumer: Consumer,
    mut shutdown: ShutdownWatch,
    mut apply: F,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            next = consumer.next() => match next {
                Some(Ok(received)) => apply(&received.payload),
                Some(Err(error)) => warn!("Insights consumer error: {error}"),
                None => break,
            }
        }
    }
    if let Err(error) = consumer.shutdown().await {
        warn!("Insights did not leave its consumer group cleanly. {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence(conversation: &str, digest: &str, previous: Option<&str>) -> PolicyEvidence {
        PolicyEvidence {
            decision_id: format!("decision-{digest}"),
            decision: "allow".to_owned(),
            mode: "observe".to_owned(),
            kind: "publish".to_owned(),
            stream: "photon".to_owned(),
            topic: "order.events".to_owned(),
            source: None,
            target: None,
            conversation: Some(conversation.to_owned()),
            correlation: None,
            operation: None,
            tool: None,
            on_behalf_of: None,
            reason: None,
            approved_scope: None,
            policy: None,
            risk_score: None,
            receipt_digest: digest.to_owned(),
            previous_digest: previous.map(str::to_owned),
            outcome: "effected".to_owned(),
            at_micros: 1,
        }
    }

    #[test]
    fn given_a_linked_chain_when_folded_then_should_count_one_segment_and_no_violation() {
        let mut chains = EvidenceChains::default();
        chains.fold(&evidence("conv-1", "d1", None));
        chains.fold(&evidence("conv-1", "d2", Some("d1")));
        chains.fold(&evidence("conv-1", "d3", Some("d2")));
        assert_eq!(
            (chains.decisions, chains.segments, chains.violations),
            (3, 1, 0)
        );
    }

    #[test]
    fn given_a_restart_boundary_when_folded_then_should_start_a_new_segment_not_a_violation() {
        let mut chains = EvidenceChains::default();
        chains.fold(&evidence("conv-1", "d1", None));
        chains.fold(&evidence("conv-1", "d2", None));
        assert_eq!(
            (chains.decisions, chains.segments, chains.violations),
            (2, 2, 0)
        );
    }

    #[test]
    fn given_a_mismatched_predecessor_when_folded_then_should_flag_a_violation() {
        let mut chains = EvidenceChains::default();
        chains.fold(&evidence("conv-1", "d1", None));
        chains.fold(&evidence("conv-1", "d3", Some("not-d1")));
        assert_eq!(
            (chains.decisions, chains.segments, chains.violations),
            (2, 1, 1)
        );
    }

    #[test]
    fn given_replayed_evidence_when_folded_then_should_not_double_count_or_violate() {
        let mut chains = EvidenceChains::default();
        let evidence = evidence("conv-1", "d1", None);
        chains.fold(&evidence);
        chains.fold(&evidence);
        assert_eq!(
            (chains.decisions, chains.segments, chains.violations),
            (1, 1, 0)
        );
    }

    #[test]
    fn given_attributed_evidence_when_folded_then_dashboard_should_show_agent_activity() {
        let mut dashboards = Dashboards::default();
        let mut evidence = evidence("conv-1", "d1", None);
        evidence.source = Some("desk".to_owned());
        dashboards.fold_evidence(&evidence);
        assert!(dashboards.to_string().contains("agents [desk=1]"));
    }

    #[test]
    fn given_business_events_when_rendered_then_live_status_should_show_the_operating_totals() {
        let mut dashboards = Dashboards::default();
        dashboards.funnel.insert("browse".to_owned(), 12);
        dashboards.funnel.insert("checkout".to_owned(), 3);
        dashboards.orders.insert("accepted".to_owned(), 3);
        dashboards.orders.insert("shipped".to_owned(), 2);
        dashboards.orders.insert("delivered".to_owned(), 1);
        assert_eq!(
            dashboards.live_status(),
            "shop 12 browse / 3 checkout | orders 3 accepted / 2 shipped / 1 delivered / 0 rejected / 0 refunded | safety 0 dlq / 0 decisions / 0 violations"
        );
    }
}
