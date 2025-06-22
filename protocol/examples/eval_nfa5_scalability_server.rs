use std::thread;

use anyhow::Result;
use protocol::{application::_impl_sync::ServiceApplication};

const CURRENT_SERVICE_ID: u16 = 0x01;
const OFFERED_EVENT_ID: u16 = 0x01;

fn main() -> Result<()> {
    env_logger::init();

    let mut app = ServiceApplication::new(CURRENT_SERVICE_ID);
    app.init()?;

    app.offer_event(OFFERED_EVENT_ID);

    app.start(false)?;

    let data: [u8; 255] = [0xFF; 255];
    loop {
        app.notify(OFFERED_EVENT_ID, data.to_vec());
        thread::sleep(std::time::Duration::from_secs(1));
    }
}
