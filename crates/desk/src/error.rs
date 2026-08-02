use laser_sdk::prelude::LaserError;
use photon_shared::knobs::ConfigError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Laser(#[from] LaserError),
}
