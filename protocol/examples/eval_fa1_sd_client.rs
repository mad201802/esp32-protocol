use std::{thread::{self}, time::Duration};

use anyhow::Result;
use protocol::sd::{ServiceDiscovery, ServiceDiscoveryInterface};

const CURRENT_SERVICE_ID: u16 = 0x01;
const PEER_SERVICE_ID: u16 = 0x02;

fn main() -> Result<()> {
    env_logger::init();

    let mut sd = ServiceDiscovery::new(CURRENT_SERVICE_ID);
    sd.init()?;
    sd.start()?;

    thread::sleep(Duration::from_secs(3));

    // Check if the peer service is available
    assert!(sd.find_service(PEER_SERVICE_ID).is_some(), "EVAL FAILED: Peer service with ID {} is not available.", PEER_SERVICE_ID);
    println!("EVAL PASSED: Peer service with ID {} is available.", PEER_SERVICE_ID);

    sd.stop();
    
    Ok(())
}
