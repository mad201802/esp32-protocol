use std::{env, sync::Arc};

use anyhow::Result;
use env_logger::{Builder, Env};
use protocol::{
    application::{_impl::ServiceApplication, packets::ApplicationResponseErrorMessage},
    sd::ServiceDiscovery,
};
use tokio::{runtime::Runtime, time};

async fn _main() -> Result<()> {
    env_logger::init_from_env(Env::default().default_filter_or("debug"));

    let mut app = ServiceApplication::new(0x02);
    app.init().await?;
    app.start(false).await;

    time::sleep(time::Duration::from_secs(5)).await;

    println!("Calling method...");
    app.call_method(
        0x01,
        0x01,
        vec![],
        Arc::new(|data| {
            match data {
                Ok(data) => {
                    log::info!("Received data: {:?}", data);
                }
                Err(err) => {
                    log::error!("Error: {:?}", err);
                }
            }

            Ok(vec![])
        }),
    ).await;
    println!("Waiting for response...");

    Ok(())
}

fn main() -> Result<()> {
    let rt = Runtime::new()?;

    rt.block_on(async { _main().await })
}
