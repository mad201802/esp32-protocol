use protocol::sd::{DiscoveryConfig, ServiceDiscovery};
use std::{io::Result, net::{Ipv4Addr}};

fn main() -> Result<()>{
    let mut service_provider = ServiceDiscovery::with_config(DiscoveryConfig {
        bind_addr: Ipv4Addr::new(0, 0, 0, 0).into(),
        port: 8765,
        timeout_ms: 1000,
        retries: 3,
        retry_delay_ms: 1000,
    });
    service_provider.init()?;
    
    match service_provider.discover_service("my_service") {
        Ok(ip) => println!("Service discovered at: {}", ip),
        Err(e) => println!("Failed to discover service: {}", e),
    }
    
    service_provider.shutdown();
    
    Ok(())
}