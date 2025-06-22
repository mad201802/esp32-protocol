use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::{net::Ipv4Addr};

use anyhow::Result;
use embedded_poc::eth::start_eth;
use esp_idf_svc::log::EspLogger;
use esp_idf_svc::{eventloop::EspSystemEventLoop, hal::prelude::Peripherals, ipv4};
use esp_idf_sys::{esp_get_free_heap_size,};
use protocol::sd::ServiceDiscoveryInterface;
use log::{info};
use protocol::{
    application::{
        _impl_sync::ServiceApplication
    },
};

fn print_free_heap() {
    unsafe {
        info!("Free heap size: {} bytes", esp_get_free_heap_size());
    }
}

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
            0x01 => Some(std::net::IpAddr::V4(Ipv4Addr::new(192, 168, 178, 152))),
            _ => None,
        }
    }

    fn stop(&mut self) {}
}

fn main() -> Result<()> {
    // It is necessary to call this function once. Otherwise some patches to the runtime
    // implemented by esp-idf-sys might not link properly. See https://github.com/esp-rs/esp-idf-template/issues/71
    esp_idf_svc::sys::link_patches();

    // Bind the log crate to the ESP Logging facilities
    EspLogger::initialize_default();

    let p = Peripherals::take()?;
    let pins = p.pins;
    let sys_loop = EspSystemEventLoop::take()?;

    let ipv4_client_settings_home = ipv4::ClientSettings {
        ip: Ipv4Addr::new(192, 168, 178, 240),
        subnet: ipv4::Subnet {
            gateway: (Ipv4Addr::new(192, 168, 178, 1)),
            mask: (ipv4::Mask(24)),
        },
        dns: None,
        secondary_dns: None,
    };

    let (_lan_power, _eth) = start_eth(
        Some(ipv4_client_settings_home),
        p.mac,
        pins.gpio12,
        pins.gpio25,
        pins.gpio26,
        pins.gpio27,
        pins.gpio23,
        pins.gpio22,
        pins.gpio21,
        pins.gpio19,
        pins.gpio18,
        pins.gpio17,
        pins.gpio5,
        &sys_loop,
    );

    info!("Baseline heap size:");
    print_free_heap();

    let mut app = ServiceApplication::new(0x02);

    app.init_with_discovery(Box::new(MockServiceDiscovery{}))?;

    thread::sleep(Duration::from_secs(1));

    app.start(false)?;

    app.subscribe(
        0x01,
        0x02,
        Arc::new(|data| {
            info!("Received event data  with length: {:?}", data.len());
        }),
    );

    app.call_method(
            0x01,
            0x01,
            vec![0xba, 0xbe, 0xef],
            Arc::new(|data| {
                match data {
                    Ok(data) => {
                        log::info!("Received data: {:?}", data);
                    }
                    Err(err) => {
                        log::error!("Error: {:?}", err);
                    }
                }

                Ok(vec![])
            }),
        );

    info!("Heap size after initialization:");
    print_free_heap();

    loop {
        thread::sleep(Duration::from_secs(1));
        info!("Heap size in loop:");
        print_free_heap();
    }
}
