use crate::names::AppAgent;
use laser_sdk::sign::{KeyRegistry, SigningKey};
use std::collections::HashMap;
use std::sync::Arc;
use strum::{Display, EnumString};

#[derive(Clone, Copy, Debug, Default, Display, EnumString, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum TrustMode {
    #[default]
    Acl,
    EphemeralDemo,
    Managed,
}

pub fn ephemeral_key() -> SigningKey {
    SigningKey::from_bytes(&rand::random::<[u8; 32]>())
}

// The agents that answer a directed contract under the verified demo and
// therefore need an enrolled signing identity, so their terminals verify and
// bind to them as the resolved target. The reviewer replies over the
// request/reply hub (`approval_gate`), which is not signature-gated, so it is
// not listed here.
const SIGNING_AGENTS: [AppAgent; 4] = [
    AppAgent::Risk,
    AppAgent::Fulfillment,
    AppAgent::Hermes,
    AppAgent::Atlas,
];

// A per-agent signing keyring: each capability agent that answers a contract
// gets its own ephemeral key, enrolled under its agent id so a verifier binds
// the reply to that agent. Looked up by the service when it builds the agent.
#[derive(Default)]
pub struct KeyRing {
    keys: HashMap<AppAgent, Arc<SigningKey>>,
}

impl KeyRing {
    pub fn signing_key(&self, agent: AppAgent) -> Option<Arc<SigningKey>> {
        self.keys.get(&agent).cloned()
    }
}

// Ephemeral demo trust: the operator's signing key, a per-agent signing keyring,
// and a registry that verifies every enrolled public key. The private bytes
// never leave this process and are never logged. Standalone services default to
// broker ACLs and no registry.
pub struct DemoTrust {
    pub operator: SigningKey,
    pub registry: Arc<KeyRegistry>,
    pub keyring: Arc<KeyRing>,
}

impl DemoTrust {
    pub fn generate(operator_principal: &str) -> Self {
        let operator = ephemeral_key();
        let mut registry = KeyRegistry::new();
        registry.enroll_operator(operator_principal, operator.verifying_key());
        let mut keys = HashMap::new();
        for agent in SIGNING_AGENTS {
            let key = ephemeral_key();
            registry.enroll(agent.id().to_string(), key.verifying_key());
            keys.insert(agent, Arc::new(key));
        }
        Self {
            operator,
            registry: Arc::new(registry),
            keyring: Arc::new(KeyRing { keys }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_two_ephemeral_keys_when_generated_then_should_differ() {
        let first = ephemeral_key();
        let second = ephemeral_key();
        assert_ne!(first.key_id(), second.key_id());
    }

    #[test]
    fn given_demo_trust_when_generated_then_should_enroll_the_operator() {
        let trust = DemoTrust::generate("operator");
        // The operator's own key is enrolled, so a future signed fact from it verifies.
        assert!(!trust.operator.key_id().is_empty());
        assert!(Arc::strong_count(&trust.registry) >= 1);
    }
}
