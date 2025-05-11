use std::{io::Result, net::Ipv4Addr, sync::Arc};

use protocol::{application::{Application, ApplicationConfig, DiscoveryConfig}, packets::application::ApplicationResponseErrorMessage};

const OUR_SERVICE_ID: u16 = 0x01;
const OUR_METHOD_ID: u16 = 0x01;

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
    app.init().expect("Failed to initialize service discovery");

    app.offer_method(
        OUR_SERVICE_ID,
        OUR_METHOD_ID,
        Arc::new(|_data: Vec<u8>| -> std::result::Result<Vec<u8>, ApplicationResponseErrorMessage> {
            println!("Received a request!");
            Ok(vec![0x01, 0x02, 0x03])
        }),
    ).expect("Failed to offer method");

    app.start().expect("Failed to start service discovery");

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}