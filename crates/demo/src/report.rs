use laser_sdk::prelude::Capabilities;
use photon_shared::LaserFactory;
use photon_shared::knobs::{self, ConfigError};
use photon_shared::names::AdversaryMode;
use photon_shared::output::{fact, managed_or_skip, phase};

pub fn run_profile(
    mode: &str,
    factory: &LaserFactory,
    caps: &Capabilities,
) -> Result<(), ConfigError> {
    let platform = if caps.is_open_only() {
        "Apache Iggy, open streaming and fabric"
    } else {
        "LaserData Cloud, managed surfaces enabled"
    };
    phase("run profile");
    fact("mode", mode);
    fact("target", factory.target());
    fact("platform", platform);
    fact(
        "workload",
        format!(
            "{} shopper(s), ~{} ms/session, seed {}",
            knobs::concurrency(3),
            knobs::session_interval(250)?.as_millis(),
            knobs::scenario_seed()?
        ),
    );
    fact(
        "agents",
        format!(
            "{} model, {} permille skew, {} governor",
            knobs::llm_provider()?,
            knobs::llm_skew_permille()?,
            knobs::governor_mode()?
        ),
    );
    let adversary = knobs::adversary_mode()?;
    let adversary = if adversary == AdversaryMode::Off {
        adversary.to_string()
    } else {
        format!("{adversary} at {} acts/minute", knobs::adversary_rate()?)
    };
    fact("adversary", adversary);
    Ok(())
}

// The one visible capability checklist: each managed surface prints exactly one
// line, labeled with the Photon behavior it enables. Services gate on the same
// booleans quietly (output::gate), so nothing repeats this list.
pub fn managed_summary(caps: &Capabilities) {
    phase("managed surfaces");
    managed_or_skip(
        "query: dashboards, order lookup, and support grounding",
        caps.query.available,
    );
    managed_or_skip(
        "schema registry: the order.events writer guard",
        caps.query.available,
    );
    managed_or_skip(
        "kv: cross-process inventory and refund idempotency",
        caps.kv.available,
    );
    managed_or_skip(
        "kv fenced cas: stale charge holder rejection",
        caps.kv.cas_fenced,
    );
    managed_or_skip("fork: the flash-sale what-if", caps.forks);
    managed_or_skip("graph: fraud ring traversal", caps.graph);
    managed_or_skip("watch: the dashboards change feed", caps.watch);
    managed_or_skip("runs: the fulfillment run registry", caps.agent_workflow);
}
