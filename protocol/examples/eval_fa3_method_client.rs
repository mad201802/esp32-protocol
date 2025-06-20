use std::sync::Arc;

use anyhow::Result;
use crossbeam::channel;
use protocol::{application::_impl_sync::ServiceApplication};

const CURRENT_SERVICE_ID: u16 = 0x02;
const PEER_SERVICE_ID: u16 = 0x01;
const PEER_METHOD_ID: u16 = 0x01;
const EXPECTED_DATA: [u8; 3] = [0xba, 0xbe, 0xef];

fn main() -> Result<()> {
    env_logger::init();

    let mut app = ServiceApplication::new(CURRENT_SERVICE_ID);
    app.init()?;
    app.start(false)?;

    let (response_tx, response_rx) = channel::bounded::<Vec<u8>>(1);

    app.call_method(
        PEER_SERVICE_ID,
        PEER_METHOD_ID,
        EXPECTED_DATA.to_vec(),
        Arc::new(move |data| {
            match data {
                Ok(data) => {
                    response_tx.send(data).unwrap();
                }
                Err(err) => {
                    println!("EVAL FAILED: {:?}", err);
                }
            }

            Ok(vec![])
        }),
    );

    let response = response_rx.recv().unwrap();
    assert!(response == EXPECTED_DATA, "Expected {:?}, got {:?}", EXPECTED_DATA, response);
    println!("EVAL SUCCESS: {:?}", response);

    app.shutdown()?;

    Ok(())
}
