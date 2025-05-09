use std::net::Ipv4Addr;

use esp_idf_svc::{eventloop::EspSystemEventLoop, hal::prelude::Peripherals, ipv4};
use eth::start_eth;
use protocol::sd::{DiscoveryConfig, ServiceDiscovery};

mod eth;

fn main() -> anyhow::Result<()> {
    // It is necessary to call this function once. Otherwise some patches to the runtime
    // implemented by esp-idf-sys might not link properly. See https://github.com/esp-rs/esp-idf-template/issues/71
    esp_idf_svc::sys::link_patches();

    // Bind the log crate to the ESP Logging facilities
    esp_idf_svc::log::EspLogger::initialize_default();

    log::info!("Hello, world!");

    let p = Peripherals::take()?;
    let pins = p.pins;
    let sys_loop = EspSystemEventLoop::take()?;

    let ipv4_client_settings_home = ipv4::ClientSettings {
        ip: Ipv4Addr::new(192, 168, 178, 140),
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
