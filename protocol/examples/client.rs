use std::{sync::Arc};

use anyhow::Result;
use protocol::{
    application::{_impl::ServiceApplication},
};
use tokio::{runtime::Runtime, time};

async fn _main() -> Result<()> {
    env_logger::init();

    let mut app = ServiceApplication::new(0x02);
    app.init().await?;
    app.start(false).await;

    time::sleep(time::Duration::from_secs(2)).await;

    println!("Calling method...");
    app.call_method(
        0x01,
        0x01,
        vec![0xba, 0xbe, 0xef],
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

    app.subscribe(0x01, 0x02, Arc::new(|data| {
        println!("Received event data: {:?}", data);
    })).await;

    loop {
        time::sleep(time::Duration::from_secs(1)).await;
    }
}

fn main() -> Result<()> {
    let rt = Runtime::new()?;

    rt.block_on(async { _main().await })
}
