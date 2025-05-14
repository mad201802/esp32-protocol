use std::{env};

use anyhow::Result;
use protocol::sd::{ServiceDiscovery};

fn main() -> Result<()>{
    let args: Vec<String> = env::args().collect();
    let service_id = args.get(1)
        .expect("Please provide a service ID")
        .parse::<u16>()
        .expect("Invalid service ID");

    let timeout = args.get(2)
        .expect("Please provide a timeout")
        .parse::<u64>()
        .expect("Invalid timeout");

    let mut sd = ServiceDiscovery::new(service_id);
    sd.init()?;
    sd.start()?;
    println!("Service Discovery started with ID: {}, Timeout: {}", service_id, timeout);

    std::thread::sleep(std::time::Duration::from_secs(timeout));
    sd.stop();


    println!("Service Discovery finished after {} seconds", timeout);
    Ok(())
}