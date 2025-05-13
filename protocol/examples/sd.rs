use std::env;

use anyhow::Result;
use protocol::sd::{_impl::ServiceDiscovery};

fn main() -> Result<()>{
    let args: Vec<String> = env::args().collect();
    let mut sd = ServiceDiscovery::new(args[1].parse::<u16>()?);
    sd.init()?;
    sd.start()?;

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}