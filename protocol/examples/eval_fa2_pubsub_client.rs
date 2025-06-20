use std::sync::Arc;

use anyhow::Result;
use crossbeam::channel;
use protocol::{application::_impl_sync::ServiceApplication};

const CURRENT_SERVICE_ID: u16 = 0x02;
const PEER_SERVICE_ID: u16 = 0x01;
const PEER_EVENT_ID: u16 = 0x01;
const EXPECTED_DATA: [u8; 3] = [0xba, 0xbe, 0xef];

fn main() -> Result<()> {
    env_logger::init();

    let mut app = ServiceApplication::new(CURRENT_SERVICE_ID);
    app.init()?;
    app.start(false)?;

    let (response_tx, response_rx) = channel::bounded::<Vec<u8>>(1);

    app.subscribe(PEER_SERVICE_ID, PEER_EVENT_ID, Arc::new(move |data| {
        response_tx.send(data).unwrap();
    }));

    while let Ok(data) = response_rx.recv() {
        if data == EXPECTED_DATA {
            println!("EVAL PASSED: Received expected data: {:?}", data);
            break;
        } else {
            println!("EVAL FAILED: Received unexpected data: {:?}", data);
        }
    }

    app.unsubscribe(PEER_SERVICE_ID, PEER_EVENT_ID);

    app.shutdown()?;

    Ok(())
}
