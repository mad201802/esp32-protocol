use std::sync::Arc;

use anyhow::Result;
use protocol::{application::_impl_sync::ServiceApplication};

const CURRENT_SERVICE_ID: u16 = 0x01;
const OFFERED_METHOD_ID: u16 = 0x01;
const EXPECTED_DATA: [u8; 3] = [0xba, 0xbe, 0xef];

fn main() -> Result<()> {
    env_logger::init();

    let mut app = ServiceApplication::new(CURRENT_SERVICE_ID);
    app.init()?;

    app.offer_method(
        OFFERED_METHOD_ID,
        Arc::new(|payload| {
            println!("Received data: {:?}", payload);
            
            if payload != EXPECTED_DATA {
                panic!("Expected {:?}, got {:?}", EXPECTED_DATA, payload);
            }

            Ok(payload)
        }),
    );

    app.start(true)?;
    
    Ok(())
}
