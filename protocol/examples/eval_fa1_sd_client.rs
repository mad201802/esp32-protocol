use std::{sync::Arc, thread::{self, sleep}, time::Duration};

use anyhow::Result;
use protocol::sd::{ServiceDiscovery, ServiceDiscoveryInterface};

fn main() -> Result<()> {
    env_logger::init();

    let mut sd = ServiceDiscovery::new(0x01);
    sd.init()?;
    sd.start()?;

    loop {
        thread::sleep(Duration::from_secs(1));
    }
}
