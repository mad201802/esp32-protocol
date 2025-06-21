use std::sync::Arc;

use anyhow::Result;
use protocol::{application::_impl_sync::ServiceApplication};

const SERVER_SERVICE_ID: u16 = 0x01;
const RTT_METHOD_ID: u16 = 0x01;

fn main() -> Result<()> {
    env_logger::init();

    let mut app = ServiceApplication::new(SERVER_SERVICE_ID);
    app.init()?;

    // Offer RTT test method - simply echo back the payload
    app.offer_method(
        RTT_METHOD_ID,
        Arc::new(|payload| {
            // Simple echo - return the same payload back
            // This simulates a minimal processing time for accurate RTT measurement
            Ok(payload)
        }),
    );

    println!("RTT Test Server starting...");
    println!("Service ID: {}", SERVER_SERVICE_ID);
    println!("Method ID: {}", RTT_METHOD_ID);

    // Start the server in blocking mode
    app.start(true)?;
    
    Ok(())
}