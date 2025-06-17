use std::{sync::Arc, thread::{self, sleep}, time::Duration};

use anyhow::Result;
use protocol::application::_impl_sync::ServiceApplication;

fn main() -> Result<()> {
    env_logger::init();

    let mut app = ServiceApplication::new(rand::random::<u16>());
    app.init()?;
    app.start(false)?;

    thread::sleep(Duration::from_secs(2));

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
    );

    // app.subscribe(0x01, 0x02, Arc::new(|data| {
    //     println!("Received event data: {:?}", data);
    // }));

    // app.subscribe(0x01, 0x03, Arc::new(|data| {
    //     println!("Received event data: {:?}", data);
    // }));

    loop {
        thread::sleep(Duration::from_secs(1));
    }
}
