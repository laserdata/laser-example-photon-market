#![forbid(unsafe_code)]

use photon_adversary::{Error, Opts, run};
use photon_shared::{LaserFactory, init_tracing, shutdown_signal};

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("photon-adversary {}", env!("CARGO_PKG_VERSION"));
    init_tracing();
    let factory = LaserFactory::from_env()?;
    let service = run(&factory, Opts::from_env()?).await?;
    shutdown_signal().await;
    service.shutdown().await?;
    Ok(())
}
