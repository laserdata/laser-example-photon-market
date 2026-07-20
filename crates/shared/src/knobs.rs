use crate::names::{AdversaryMode, GovernorMode, LlmProvider, WorkflowStep};
use std::env::{self, VarError};
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use tracing::warn;

pub const CONCURRENCY: &str = "LASER_CONCURRENCY";
pub const SESSION_INTERVAL_MS: &str = "LASER_SESSION_INTERVAL_MS";
pub const ADVERSARY: &str = "LASER_ADVERSARY";
pub const ADVERSARY_SEED: &str = "LASER_ADVERSARY_SEED";
pub const ADVERSARY_RATE: &str = "LASER_ADVERSARY_RATE";
pub const GOVERNOR: &str = "LASER_GOVERNOR";
pub const LLM_PROVIDER: &str = "LASER_LLM_PROVIDER";
pub const LLM_SKEW: &str = "LASER_LLM_SKEW";
pub const SCENARIO_SEED: &str = "LASER_SCENARIO_SEED";
pub const RISK_THRESHOLD: &str = "LASER_RISK_THRESHOLD";
pub const REFUND_CEILING: &str = "LASER_REFUND_CEILING";
pub const CRASH_AFTER: &str = "LASER_CRASH_AFTER";
pub const ZOMBIE_CHARGE: &str = "LASER_ZOMBIE_CHARGE";
pub const APPLY_PLAN: &str = "LASER_APPLY_PLAN";

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error(
        "LaserData Cloud needs credentials: set LASER_TOKEN, or LASER_USERNAME + LASER_PASSWORD"
    )]
    MissingCredentials,
    #[error("{key}={value} is invalid: {reason}")]
    InvalidValue {
        key: &'static str,
        value: String,
        reason: String,
    },
}

impl ConfigError {
    pub fn invalid(key: &'static str, value: String, reason: impl Into<String>) -> Self {
        ConfigError::InvalidValue {
            key,
            value,
            reason: reason.into(),
        }
    }
}

const DEFAULT_SCENARIO_SEED: u64 = 20260710;
const DEFAULT_ADVERSARY_SEED: u64 = 1312;
const DEFAULT_ADVERSARY_RATE: u32 = 6;
const DEFAULT_RISK_THRESHOLD_CENTS: u64 = 50_000;
const DEFAULT_REFUND_CEILING_CENTS: u64 = 30_000;

pub fn concurrency(default: usize) -> usize {
    volume(CONCURRENCY, default).max(1)
}

pub fn session_interval(default_ms: u64) -> Result<Duration, ConfigError> {
    let millis = behavior_parse(SESSION_INTERVAL_MS, default_ms)?;
    if millis == 0 {
        return Err(ConfigError::invalid(
            SESSION_INTERVAL_MS,
            millis.to_string(),
            "expected a positive number of milliseconds",
        ));
    }
    Ok(Duration::from_millis(millis))
}

pub fn adversary_mode() -> Result<AdversaryMode, ConfigError> {
    behavior_enum(ADVERSARY, AdversaryMode::Off)
}

pub fn governor_mode() -> Result<GovernorMode, ConfigError> {
    behavior_enum(GOVERNOR, GovernorMode::Observe)
}

pub fn llm_provider() -> Result<LlmProvider, ConfigError> {
    behavior_enum(LLM_PROVIDER, LlmProvider::Mock)
}

pub fn scenario_seed() -> Result<u64, ConfigError> {
    behavior_parse(SCENARIO_SEED, DEFAULT_SCENARIO_SEED)
}

pub fn adversary_seed() -> Result<u64, ConfigError> {
    behavior_parse(ADVERSARY_SEED, DEFAULT_ADVERSARY_SEED)
}

pub fn adversary_rate() -> Result<u32, ConfigError> {
    behavior_parse(ADVERSARY_RATE, DEFAULT_ADVERSARY_RATE)
}

pub fn risk_threshold_cents() -> Result<u64, ConfigError> {
    behavior_parse(RISK_THRESHOLD, DEFAULT_RISK_THRESHOLD_CENTS)
}

pub fn refund_ceiling_cents() -> Result<u64, ConfigError> {
    behavior_parse(REFUND_CEILING, DEFAULT_REFUND_CEILING_CENTS)
}

pub fn llm_skew_permille() -> Result<u16, ConfigError> {
    let permille: u16 = behavior_parse(LLM_SKEW, 0)?;
    if permille > 1000 {
        return Err(ConfigError::invalid(
            LLM_SKEW,
            permille.to_string(),
            "expected 0..=1000",
        ));
    }
    Ok(permille)
}

pub fn crash_after() -> Result<Option<WorkflowStep>, ConfigError> {
    match read(CRASH_AFTER) {
        None => Ok(None),
        Some(value) => WorkflowStep::from_str(&value).map(Some).map_err(|_| {
            ConfigError::invalid(
                CRASH_AFTER,
                value,
                "expected charge, quote, book, or dispatch",
            )
        }),
    }
}

pub fn zombie_charge() -> Result<bool, ConfigError> {
    behavior_flag(ZOMBIE_CHARGE)
}

pub fn apply_plan() -> Result<bool, ConfigError> {
    behavior_flag(APPLY_PLAN)
}

fn read(key: &str) -> Option<String> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => Some(value.trim().to_owned()),
        _ => None,
    }
}

fn volume<T: FromStr + std::fmt::Display>(key: &'static str, default: T) -> T {
    match read(key) {
        None => default,
        Some(value) => value.parse().unwrap_or_else(|_| {
            warn!("{key}={value} is not a valid number, falling back to {default}");
            default
        }),
    }
}

fn behavior_parse<T: FromStr>(key: &'static str, default: T) -> Result<T, ConfigError> {
    match read(key) {
        None => Ok(default),
        Some(value) => value
            .parse()
            .map_err(|_| ConfigError::invalid(key, value, "expected a number")),
    }
}

fn behavior_enum<T: FromStr + Default>(key: &'static str, default: T) -> Result<T, ConfigError> {
    match read(key) {
        None => Ok(default),
        Some(value) => {
            T::from_str(&value).map_err(|_| ConfigError::invalid(key, value, "unknown value"))
        }
    }
}

fn behavior_flag(key: &'static str) -> Result<bool, ConfigError> {
    match env::var(key) {
        Err(VarError::NotPresent) => Ok(false),
        Err(VarError::NotUnicode(_)) => Err(ConfigError::invalid(
            key,
            String::new(),
            "not valid unicode",
        )),
        Ok(value) => match value.trim() {
            "" | "0" | "false" | "no" | "off" => Ok(false),
            "1" | "true" | "yes" | "on" => Ok(true),
            other => Err(ConfigError::invalid(
                key,
                other.to_owned(),
                "expected a boolean flag",
            )),
        },
    }
}
