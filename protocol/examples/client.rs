use std::{io::Result, net::Ipv4Addr};

use protocol::application::{Application, ApplicationConfig, DiscoveryConfig};

fn main() -> Result<()>{
    let config = ApplicationConfig {
        bind_addr: Ipv4Addr::new(0, 0, 0, 0).into(),
        port: 8765,
        discovery_config: DiscoveryConfig {

            timeout_ms: 1000,
            retries: 3,
            retry_delay_ms: 200,
        },
    };

    let mut app = Application::new(config);

    

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}