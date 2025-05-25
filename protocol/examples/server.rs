use std::{sync::Arc, time::Duration, thread};

use anyhow::Result;
use protocol::application::{_impl_sync::ServiceApplication, packets::ApplicationResponseErrorMessage};

fn main() -> Result<()> {
    env_logger::init();

    let mut app = ServiceApplication::new(0x01);
    app.init()?;

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
    );

    app.offer_event(0x02);
    app.offer_event(0x03);

    app.start(false)?;

    loop {
        app.notify(0x02, vec![0xff; 32]);
        thread::sleep(Duration::from_millis(100));
    }
}
