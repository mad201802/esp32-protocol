use std::thread;

use anyhow::Result;
use protocol::{application::_impl_sync::ServiceApplication};

const CURRENT_SERVICE_ID: u16 = 0x01;
const OFFERED_EVENT_ID: u16 = 0x01;
const EXPECTED_DATA: [u8; 3] = [0xba, 0xbe, 0xef];

fn main() -> Result<()> {
    env_logger::init();

    let mut app = ServiceApplication::new(CURRENT_SERVICE_ID);
    app.init()?;

    app.offer_event(OFFERED_EVENT_ID);

    app.start(false)?;

    for _ in 0..10 {
        app.notify(OFFERED_EVENT_ID, EXPECTED_DATA.to_vec());
        thread::sleep(std::time::Duration::from_secs(1));
    }

    app.shutdown()?;

    Ok(())
}
