use std::sync::Arc;

use anyhow::Result;
use protocol::{application::_impl_sync::ServiceApplication};

const PEER_SERVICE_ID: u16 = 0x01;
const PEER_EVENT_ID: u16 = 0x01;
fn main() -> Result<()> {
    env_logger::init();

    let mut app = ServiceApplication::new(rand::random_range(0..u16::MAX));
    app.init()?;
    app.start(false)?;

    app.subscribe(PEER_SERVICE_ID, PEER_EVENT_ID, Arc::new(move |data| {
        println!("Received data from service {} with length {:?}", PEER_SERVICE_ID, data.len());
    }));

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

}
