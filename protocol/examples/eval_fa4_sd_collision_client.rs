use std::{thread::{self}, time::Duration};

use anyhow::Result;
use protocol::sd::{ServiceDiscovery, ServiceDiscoveryInterface};

const CURRENT_SERVICE_ID: u16 = 0x01;
fn main() -> Result<()> {
    env_logger::init();

    let mut sd = ServiceDiscovery::new(CURRENT_SERVICE_ID);
    sd.init()?;
    sd.start()?;

    thread::sleep(Duration::from_secs(3));
    
    Ok(())
}
