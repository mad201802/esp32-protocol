use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::net::Ipv4Addr;

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

    // Bind the log crate to the ESP Logging facilities
    EspLogger::initialize_default();

    let p = Peripherals::take()?;
    let pins = p.pins;
    let sys_loop = EspSystemEventLoop::take()?;

    let ipv4_client_settings_home = ipv4::ClientSettings {
        ip: Ipv4Addr::new(192, 168, 0, 6),
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

    let mut app = ServiceApplication::new(0x02);

    app.init()?;

    thread::sleep(Duration::from_secs(1));

    app.offer_event(0x01); // Button pressed event

    app.start(false)?;

    // The button is on gpio15 and we want to notify if the button is pressed
    let gpio15 = pins.gpio15;
    let button_pin = Arc::new(Mutex::new(PinDriver::input(gpio15).unwrap()));
    let button_pin_clone = Arc::clone(&button_pin);

    loop {
        let mut button_pin = button_pin_clone.lock().unwrap();
        if button_pin.is_high() {
            println!("Button pressed!");
            app.notify(0x01, vec![0x01]); // Notify that the button is pressed
        } else {
            println!("Button released!");
            app.notify(0x01, vec![0x00]); // Notify that the button is released
        }
        thread::sleep(Duration::from_millis(100)); // Polling interval
    }


}
