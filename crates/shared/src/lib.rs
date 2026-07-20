#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

pub mod connect;
pub mod deadletter;
pub mod domain;
pub mod knobs;
pub mod lifecycle;
pub mod llm;
pub mod names;
pub mod output;
pub mod publish;
pub mod schema;
pub mod semantic;
pub mod telemetry;
pub mod topology;
pub mod trust;

#[cfg(feature = "testkit")]
pub mod testkit;

pub use connect::{ConnectionTarget, LaserFactory};
pub use knobs::ConfigError;
pub use lifecycle::{
    HANDLER_SHUTDOWN_GRACE, ServiceHandle, ShutdownWatch, eventually, shutdown_signal,
};
pub use publish::{BusinessProvenance, publish_business, publish_business_with_schema};
pub use telemetry::init_tracing;
