use std::net::Ipv4Addr;

use anyhow::Result;
use esp_idf_svc::{eventloop::EspSystemEventLoop, hal::prelude::Peripherals, ipv4};
use esp_idf_sys::esp;
use eth::start_eth;
use protocol::sd::ServiceDiscovery;
use tokio::runtime::{self, Runtime};

mod eth;

async fn _main() -> Result<()> {
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

    let service_id = 2;
    let timeout = 5;

    let mut sd = ServiceDiscovery::new(service_id);
    sd.init().await?;
    sd.start().await?;
    println!(
        "Service Discovery started with ID: {}, Timeout: {}",
        service_id, timeout
    );

    std::thread::sleep(std::time::Duration::from_secs(timeout));

    println!("Service Discovery finished after {} seconds", timeout);

    Ok::<(), anyhow::Error>(())
}

fn main() -> Result<()> {
    // It is necessary to call this function once. Otherwise some patches to the runtime
    // implemented by esp-idf-sys might not link properly. See https://github.com/esp-rs/esp-idf-template/issues/71
    esp_idf_svc::sys::link_patches();

    // Bind the log crate to the ESP Logging facilities
    esp_idf_svc::log::EspLogger::initialize_default();

    let config = esp_idf_sys::esp_vfs_eventfd_config_t {
        max_fds: 1,
        ..Default::default()
    };
    esp! { unsafe { esp_idf_sys::esp_vfs_eventfd_register(&config) } }?;

    runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(_main())?;

    Ok(())
}
