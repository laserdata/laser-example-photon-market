use laser_sdk::prelude::ConversationId;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::Mutex;

#[derive(Default)]
pub struct ApprovalGrants {
    grants: Mutex<HashSet<ApprovalGrant>>,
}

impl ApprovalGrants {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn grant(&self, conversation: ConversationId, scope: &str, payload: &[u8]) {
        self.grants
            .lock()
            .expect("approval grants lock is not poisoned")
            .insert(ApprovalGrant::new(conversation, scope, payload));
    }

    pub fn consume(&self, conversation: ConversationId, scope: &str, payload: &[u8]) -> bool {
        self.grants
            .lock()
            .expect("approval grants lock is not poisoned")
            .remove(&ApprovalGrant::new(conversation, scope, payload))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ApprovalGrant {
    conversation: ConversationId,
    scope: String,
    payload_digest: String,
}

impl ApprovalGrant {
    fn new(conversation: ConversationId, scope: &str, payload: &[u8]) -> Self {
        Self {
            conversation,
            scope: scope.to_owned(),
            payload_digest: effect_digest(payload),
        }
    }
}

pub fn effect_digest(payload: &[u8]) -> String {
    Sha256::digest(payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_grant_when_consumed_once_then_should_not_be_reusable() {
        let grants = ApprovalGrants::new();
        let conversation = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            .parse()
            .expect("conversation id parses");
        grants.grant(conversation, "refunds:approve", br#"{"ok":true}"#);
        assert!(grants.consume(conversation, "refunds:approve", br#"{"ok":true}"#));
        assert!(!grants.consume(conversation, "refunds:approve", br#"{"ok":true}"#));
    }

    #[test]
    fn given_two_effects_when_digested_then_should_not_share_an_approval_key() {
        assert_ne!(effect_digest(b"refund 10"), effect_digest(b"refund 11"));
    }
}
