#![forbid(unsafe_code)]

mod probe;
mod report;
mod scenario;

use photon_shared::{LaserFactory, init_tracing, output};
use scenario::{DemoCommand, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("{}", output::version_banner(env!("CARGO_PKG_VERSION")));
    init_tracing();
    let command = DemoCommand::from_args()?;
    match command {
        DemoCommand::Run(mode) => {
            let factory = LaserFactory::from_env()?;
            scenario::run(&factory, mode).await
        }
        DemoCommand::Help => {
            scenario::print_help();
            Ok(())
        }
    }
}
