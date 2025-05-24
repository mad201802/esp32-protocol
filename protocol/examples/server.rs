use std::{sync::Arc, time::Duration};

use anyhow::Result;
use protocol::application::{_impl::ServiceApplication, packets::ApplicationResponseErrorMessage};
use tokio::{runtime::Runtime, time};

async fn _main() -> Result<()>{
    env_logger::init();

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

    app.offer_event(0x02).await;

    app.start(false).await;
    time::sleep(Duration::from_secs(2)).await;

    loop {
        app.notify(0x02, vec![0xba, 0xbe, 0xef]).await;
        time::sleep(Duration::from_secs(1)).await;
    }
}

fn main() -> Result<()> {
    let rt  = Runtime::new()?;

    rt.block_on(async {
        _main().await
    })
}