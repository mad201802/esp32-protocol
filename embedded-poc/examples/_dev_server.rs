use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use embedded_poc::eth::start_eth;
use esp_idf_svc::hal::gpio::PinDriver;
use esp_idf_svc::log::EspLogger;
use esp_idf_svc::{eventloop::EspSystemEventLoop, hal::prelude::Peripherals, ipv4};
use protocol::application::_impl_sync::ServiceApplication;

fn main() -> Result<()> {
    // It is necessary to call this function once. Otherwise some patches to the runtime
    // implemented by esp-idf-sys might not link properly. See https://github.com/esp-rs/esp-idf-template/issues/71
    esp_idf_svc::sys::link_patches();

    EspLogger::initialize_default();

    let p = Peripherals::take()?;
    let pins = p.pins;
    let sys_loop = EspSystemEventLoop::take()?;

    let ipv4_client_settings_home = ipv4::ClientSettings {
        ip: Ipv4Addr::new(192, 168, 0, 5),
        subnet: ipv4::Subnet {
            gateway: (Ipv4Addr::new(192, 168, 0, 1)),
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

    // Remove mutable led_pin from here, move pin access inside closure
    let gpio14 = pins.gpio14;
    let led_pin = Arc::new(Mutex::new(PinDriver::output(gpio14).unwrap()));
    let led_pin_clone = led_pin.clone();

    app.start(false)?;

    app.subscribe(
        0x02,
        0x01,
        Arc::new(move |payload| {
            let mut led_pin = led_pin_clone.lock().unwrap();
            if payload[0] == 0x01 {
                println!("Turning LED on");
                led_pin.set_high().unwrap();
            } else {
                println!("Turning LED off");
                led_pin.set_low().unwrap();
            }
        }),
    );

    loop {
        thread::sleep(Duration::from_millis(500)); // Reduce frequency to prevent memory pressure
    }
}
