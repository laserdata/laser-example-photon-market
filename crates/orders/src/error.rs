use laser_sdk::prelude::LaserError;
use photon_shared::domain::Sku;
use photon_shared::knobs::ConfigError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Laser(#[from] LaserError),
}

#[derive(Debug, Error)]
pub enum SagaError {
    /// The simulated coordinator kill: the run was abandoned mid-flight with
    /// its journal intact, so no compensation or cleanup may run, exactly as
    /// if the process had died. A restart resumes the run from the journal.
    #[error("coordinator crashed after the {0} step")]
    Crashed(photon_shared::names::WorkflowStep),
    #[error(transparent)]
    Failed(#[from] LaserError),
}

#[derive(Debug, Error, PartialEq)]
pub enum InventoryError {
    #[error("unknown sku {0}")]
    UnknownSku(Sku),
    #[error("insufficient stock for {sku}: requested {requested}, available {available}")]
    InsufficientStock {
        sku: Sku,
        requested: u32,
        available: u32,
    },
    #[error("managed inventory failed: {0}")]
    Managed(String),
}
