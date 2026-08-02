use crate::knobs::{self, ConfigError};
use crate::names::LlmProvider;
use async_trait::async_trait;
use std::sync::Arc;
use strum::Display;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct CompletionRequest {
    pub request_key: String,
    pub system: String,
    pub prompt: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Completion {
    pub text: String,
    pub provider: LlmProvider,
    pub model: String,
    pub usage: Option<TokenUsage>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TokenUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
}

#[derive(Debug, Error)]
pub enum LlmError {
    #[error("llm backend request failed: {0}")]
    Backend(String),
    #[error("llm response could not be decoded: {0}")]
    Decode(String),
}

#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn complete(&self, request: &CompletionRequest) -> Result<Completion, LlmError>;
}

#[async_trait]
impl LlmClient for Arc<dyn LlmClient> {
    async fn complete(&self, request: &CompletionRequest) -> Result<Completion, LlmError> {
        (**self).complete(request).await
    }
}

pub struct MockLlm;

#[async_trait]
impl LlmClient for MockLlm {
    async fn complete(&self, request: &CompletionRequest) -> Result<Completion, LlmError> {
        Ok(Completion {
            text: format!("[mock] {}", request.prompt),
            provider: LlmProvider::Mock,
            model: "mock".to_owned(),
            usage: None,
        })
    }
}

#[derive(Clone, Copy, Debug, Display, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum SkewStrategy {
    InflateRefund,
    FabricateMemory,
}

pub struct SkewedLlm<C> {
    inner: C,
    scenario_seed: u64,
    permille: u16,
    strategy: SkewStrategy,
}

pub fn llm_client(provider: LlmProvider) -> Result<Arc<dyn LlmClient>, ConfigError> {
    match provider {
        LlmProvider::Mock => Ok(Arc::new(MockLlm)),
        LlmProvider::Anthropic => backends::anthropic(),
        LlmProvider::OpenAi => backends::openai(),
    }
}

impl<C> SkewedLlm<C> {
    pub fn new(inner: C, scenario_seed: u64, permille: u16, strategy: SkewStrategy) -> Self {
        Self {
            inner,
            scenario_seed,
            permille,
            strategy,
        }
    }
}

#[async_trait]
impl<C: LlmClient> LlmClient for SkewedLlm<C> {
    async fn complete(&self, request: &CompletionRequest) -> Result<Completion, LlmError> {
        let base = self.inner.complete(request).await?;
        if self.should_skew(&request.request_key) {
            Ok(self.corrupt(base))
        } else {
            Ok(base)
        }
    }
}

impl<C> SkewedLlm<C> {
    fn should_skew(&self, request_key: &str) -> bool {
        match self.permille {
            0 => false,
            permille if permille >= 1000 => true,
            permille => skew_bucket(self.scenario_seed, request_key, self.strategy) < permille,
        }
    }

    fn corrupt(&self, base: Completion) -> Completion {
        let text = match self.strategy {
            SkewStrategy::InflateRefund => format!("{} [skew:inflate_refund]", base.text),
            SkewStrategy::FabricateMemory => format!("{} [skew:fabricate_memory]", base.text),
        };
        Completion { text, ..base }
    }
}

fn skew_bucket(seed: u64, request_key: &str, strategy: SkewStrategy) -> u16 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    let mut mix = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
    };
    mix(&seed.to_le_bytes());
    mix(request_key.as_bytes());
    mix(strategy.to_string().as_bytes());
    (hash % 1000) as u16
}

mod backends {
    use super::*;

    #[cfg(feature = "llm-anthropic")]
    pub fn anthropic() -> Result<Arc<dyn LlmClient>, ConfigError> {
        Ok(Arc::new(real::AnthropicLlm::from_env()?))
    }

    #[cfg(not(feature = "llm-anthropic"))]
    pub fn anthropic() -> Result<Arc<dyn LlmClient>, ConfigError> {
        Err(ConfigError::invalid(
            knobs::LLM_PROVIDER,
            LlmProvider::Anthropic.to_string(),
            "rebuild with --features llm-anthropic to enable this provider",
        ))
    }

    #[cfg(feature = "llm-openai")]
    pub fn openai() -> Result<Arc<dyn LlmClient>, ConfigError> {
        Ok(Arc::new(real::OpenAiLlm::from_env()?))
    }

    #[cfg(not(feature = "llm-openai"))]
    pub fn openai() -> Result<Arc<dyn LlmClient>, ConfigError> {
        Err(ConfigError::invalid(
            knobs::LLM_PROVIDER,
            LlmProvider::OpenAi.to_string(),
            "rebuild with --features llm-openai to enable this provider",
        ))
    }
}

#[cfg(any(feature = "llm-anthropic", feature = "llm-openai"))]
mod real {
    use super::*;
    use serde::Deserialize;

    #[cfg(feature = "llm-anthropic")]
    pub struct AnthropicLlm {
        client: reqwest::Client,
        api_key: String,
        model: String,
    }

    #[cfg(feature = "llm-anthropic")]
    impl AnthropicLlm {
        const ENDPOINT: &'static str = "https://api.anthropic.com/v1/messages";
        const API_VERSION: &'static str = "2023-06-01";
        const MAX_TOKENS: u32 = 1024;

        pub fn from_env() -> Result<Self, ConfigError> {
            Ok(Self {
                client: reqwest::Client::new(),
                api_key: key("ANTHROPIC_API_KEY")?,
                model: std::env::var("ANTHROPIC_MODEL")
                    .unwrap_or_else(|_| "claude-sonnet-4-6".to_owned()),
            })
        }
    }

    #[cfg(feature = "llm-anthropic")]
    #[derive(Deserialize)]
    struct AnthropicResponse {
        content: Vec<AnthropicBlock>,
        usage: Option<AnthropicUsage>,
    }

    #[cfg(feature = "llm-anthropic")]
    #[derive(Deserialize)]
    struct AnthropicBlock {
        #[serde(default)]
        text: String,
    }

    #[cfg(feature = "llm-anthropic")]
    #[derive(Deserialize)]
    struct AnthropicUsage {
        input_tokens: u32,
        output_tokens: u32,
    }

    #[cfg(feature = "llm-anthropic")]
    #[async_trait]
    impl LlmClient for AnthropicLlm {
        async fn complete(&self, request: &CompletionRequest) -> Result<Completion, LlmError> {
            let body = serde_json::json!({
                "model": self.model,
                "max_tokens": Self::MAX_TOKENS,
                "system": request.system,
                "messages": [{ "role": "user", "content": request.prompt }],
            });
            let response = self
                .client
                .post(Self::ENDPOINT)
                .header("x-api-key", &self.api_key)
                .header("anthropic-version", Self::API_VERSION)
                .json(&body)
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|error| LlmError::Backend(error.to_string()))?;
            let parsed: AnthropicResponse = response
                .json()
                .await
                .map_err(|error| LlmError::Decode(error.to_string()))?;
            Ok(Completion {
                text: parsed.content.into_iter().map(|block| block.text).collect(),
                provider: LlmProvider::Anthropic,
                model: self.model.clone(),
                usage: parsed.usage.map(|usage| TokenUsage {
                    input_tokens: usage.input_tokens,
                    output_tokens: usage.output_tokens,
                }),
            })
        }
    }

    #[cfg(feature = "llm-openai")]
    pub struct OpenAiLlm {
        client: reqwest::Client,
        api_key: String,
        model: String,
    }

    #[cfg(feature = "llm-openai")]
    impl OpenAiLlm {
        const ENDPOINT: &'static str = "https://api.openai.com/v1/chat/completions";

        pub fn from_env() -> Result<Self, ConfigError> {
            Ok(Self {
                client: reqwest::Client::new(),
                api_key: key("OPENAI_API_KEY")?,
                model: std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o".to_owned()),
            })
        }
    }

    #[cfg(feature = "llm-openai")]
    #[derive(Deserialize)]
    struct OpenAiResponse {
        choices: Vec<OpenAiChoice>,
    }

    #[cfg(feature = "llm-openai")]
    #[derive(Deserialize)]
    struct OpenAiChoice {
        message: OpenAiMessage,
    }

    #[cfg(feature = "llm-openai")]
    #[derive(Deserialize)]
    struct OpenAiMessage {
        #[serde(default)]
        content: String,
    }

    #[cfg(feature = "llm-openai")]
    #[async_trait]
    impl LlmClient for OpenAiLlm {
        async fn complete(&self, request: &CompletionRequest) -> Result<Completion, LlmError> {
            let body = serde_json::json!({
                "model": self.model,
                "messages": [
                    { "role": "system", "content": request.system },
                    { "role": "user", "content": request.prompt },
                ],
            });
            let response = self
                .client
                .post(Self::ENDPOINT)
                .bearer_auth(&self.api_key)
                .json(&body)
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|error| LlmError::Backend(error.to_string()))?;
            let parsed: OpenAiResponse = response
                .json()
                .await
                .map_err(|error| LlmError::Decode(error.to_string()))?;
            Ok(Completion {
                text: parsed
                    .choices
                    .into_iter()
                    .next()
                    .map(|choice| choice.message.content)
                    .unwrap_or_default(),
                provider: LlmProvider::OpenAi,
                model: self.model.clone(),
                usage: None,
            })
        }
    }

    fn key(name: &'static str) -> Result<String, ConfigError> {
        match std::env::var(name) {
            Ok(value) if !value.trim().is_empty() => Ok(value),
            _ => Err(ConfigError::invalid(
                knobs::LLM_PROVIDER,
                name.to_owned(),
                format!("{name} must be set for this provider"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn given_zero_permille_when_completing_then_should_never_skew() {
        let client = SkewedLlm::new(MockLlm, 42, 0, SkewStrategy::InflateRefund);
        let base = MockLlm
            .complete(&request("order-1"))
            .await
            .expect("mock completes");
        let skewed = client
            .complete(&request("order-1"))
            .await
            .expect("skewed completes");
        assert_eq!(skewed.text, base.text);
    }

    #[tokio::test]
    async fn given_full_permille_when_completing_then_should_always_skew() {
        let client = SkewedLlm::new(MockLlm, 42, 1000, SkewStrategy::FabricateMemory);
        let skewed = client
            .complete(&request("order-1"))
            .await
            .expect("skewed completes");
        assert!(skewed.text.ends_with("[skew:fabricate_memory]"));
    }

    #[test]
    fn given_the_same_seed_and_key_when_bucketed_then_should_be_stable_and_order_free() {
        let first = skew_bucket(42, "order-9", SkewStrategy::InflateRefund);
        let second = skew_bucket(42, "order-9", SkewStrategy::InflateRefund);
        assert_eq!(first, second);
        assert!(first < 1000);
    }

    fn request(key: &str) -> CompletionRequest {
        CompletionRequest {
            request_key: key.to_owned(),
            system: "system".to_owned(),
            prompt: "refund the order".to_owned(),
        }
    }
}
