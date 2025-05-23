use std::{env, sync::Arc};

use anyhow::Result;
use env_logger::{Builder, Env};
use protocol::{application::{_impl::ServiceApplication, packets::ApplicationResponseErrorMessage}, sd::ServiceDiscovery};
use tokio::runtime::Runtime;

async fn _main() -> Result<()>{
    env_logger::init_from_env(Env::default().default_filter_or("debug"));

    let mut app = ServiceApplication::new(0x01);
    app.init().await?;

    app.offer_method(
        0x01,
        Arc::new(|payload| {
            println!("Received data: {:?}", payload);
            if payload.is_empty() {
                log::error!("Payload is empty");
                return Err(ApplicationResponseErrorMessage {
                    error_code: 0x01,
                    error_message: "Payload is empty".to_string(),
                });
            }

            Ok(vec![])
        }),
    )
    .await;

    app.start(true).await;
    Ok(())
}

fn main() -> Result<()> {
    let rt  = Runtime::new()?;

    rt.block_on(async {
        _main().await
    })
}