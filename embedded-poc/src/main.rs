use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use std::thread;

use anyhow::Result;
use esp_idf_svc::{eventloop::EspSystemEventLoop, hal::prelude::Peripherals, ipv4};
use esp_idf_sys::esp;
use eth::start_eth;
use protocol::application::{_impl_sync::ServiceApplication, packets::ApplicationResponseErrorMessage};

mod eth;

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

    let mut app = ServiceApplication::new(0x01);
    app.init()?;

    app.offer_method(
        0x01,
        Arc::new(|payload| {
            println!("Received data: {:?}", payload);
            if payload.is_empty() {
                log::error!("Payload is empty");
                return Err(ApplicationResponseErrorMessage {
                    error_code: 0x01,
                    error_message: "Payload is empty".to_string(),
                });
            }

            Ok(vec![])
        }),
    );

    app.offer_event(0x02);

    app.start(false)?;

    loop {
        // Use smaller allocations for ESP32
        app.notify(0x02, vec![0xba, 0xbe, 0xef]);
        thread::sleep(Duration::from_millis(500)); // Reduce frequency to prevent memory pressure
    }
}
