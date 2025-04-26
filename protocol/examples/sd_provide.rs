use protocol::sd::{DiscoveryConfig, ServiceDiscovery};
use std::{io::Result, net::Ipv4Addr};

fn main() -> Result<()>{
    let mut provider = ServiceDiscovery::with_config(DiscoveryConfig {
        bind_addr: Ipv4Addr::new(0, 0, 0, 0).into(),
        port: 8765,
        timeout_ms: 1000,
        retries: 3,
        retry_delay_ms: 200,
    });

    provider.init().expect("Failed to initialize service discovery");
    provider.provide_service("my_service").expect("Failed to provide service");
    println!("Service provided");

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}