use std::{net::Ipv4Addr, sync::Arc, thread};

use anyhow::Result;
use protocol::{application::_impl_sync::ServiceApplication, sd::ServiceDiscoveryInterface};

const CURRENT_SERVICE_ID: u16 = 0x01;
const OFFERED_EVENT_ID: u16 = 0x02;
const OFFERED_METHOD_ID: u16 = 0x01;

struct MockServiceDiscovery {}

impl ServiceDiscoveryInterface for MockServiceDiscovery {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn start(&mut self) -> Result<()> {
        Ok(())
    }

    fn find_service(&self, service_id: u16) -> Option<std::net::IpAddr> {
        match service_id {
            0x02 => Some(std::net::IpAddr::V4(Ipv4Addr::new(192, 168, 178, 240))),
            _ => None,
        }
    }

    fn stop(&mut self) {}
}

fn main() -> Result<()> {
    env_logger::init();

    let mut app = ServiceApplication::new(CURRENT_SERVICE_ID);
    app.init_with_discovery(Box::new(MockServiceDiscovery {}))?;

    app.offer_event(OFFERED_EVENT_ID);

    app.offer_method(
        OFFERED_METHOD_ID,
        Arc::new(|payload| {
            Ok(payload)
        }),
    );

    app.start(false)?;

    let expected_data: [u8; 255] = [0x01; 255];

    loop {
        app.notify(OFFERED_EVENT_ID, expected_data.to_vec());
        thread::sleep(std::time::Duration::from_secs(1));
    }
}
