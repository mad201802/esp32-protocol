use std::{env};

use anyhow::Result;
use env_logger::{Builder, Env};
use protocol::{application::_impl::ServiceApplication, sd::ServiceDiscovery};
use tokio::runtime::Runtime;

async fn _main() -> Result<()>{
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    let args: Vec<String> = env::args().collect();
    let service_id = args.get(1)
        .expect("Please provide a service ID")
        .parse::<u16>()
        .expect("Invalid service ID");

    let mut app = ServiceApplication::new(service_id);
    app.init().await?;
    app.start().await;
    Ok(())
}

fn main() -> Result<()> {
    let rt  = Runtime::new()?;

    rt.block_on(async {
        _main().await
    })
}