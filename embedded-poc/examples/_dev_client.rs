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
use protocol::sd::ServiceDiscovery;

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

    let mut app = ServiceApplication::<ServiceDiscovery>::new(0x02);

    app.init()?;

    thread::sleep(Duration::from_secs(1));

    app.start(false)?;

    // Set up all three buttons
    // Button 1: gpio15 for method_id 0x01
    let gpio15 = pins.gpio15;
    let button1_pin = Arc::new(Mutex::new(PinDriver::input(gpio15).unwrap()));
    let button1_pin_clone = Arc::clone(&button1_pin);

    // Button 2: gpio14 for method_id 0x02
    let gpio14 = pins.gpio14;
    let button2_pin = Arc::new(Mutex::new(PinDriver::input(gpio14).unwrap()));
    let button2_pin_clone = Arc::clone(&button2_pin);

    // Button 3: gpio32 for method_id 0x03
    let gpio32 = pins.gpio32;
    let button3_pin = Arc::new(Mutex::new(PinDriver::input(gpio32).unwrap()));
    let button3_pin_clone = Arc::clone(&button3_pin);

    // Track the previous physical state of all buttons to detect presses
    let mut button1_previous_state = false;
    let mut button2_previous_state = false;
    let mut button3_previous_state = false;

    // Track the virtual switch state for all buttons
    let mut button1_switch_state = false;
    let mut button2_switch_state = false;
    let mut button3_switch_state = false;

    loop {
        // Handle Button 1 (GPIO15 -> method_id 0x01)
        let button1_pin = button1_pin_clone.lock().unwrap();
        let button1_current_state = button1_pin.is_high();
        
        if button1_current_state && !button1_previous_state {
            button1_switch_state = !button1_switch_state;
            
            if button1_switch_state {
                println!("Button 1 Switch ON (GPIO15)");
                app.call_method(0x01, 0x01, vec![0x01], Arc::new(move |_payload| {Ok(vec![0x01])}));
            } else {
                println!("Button 1 Switch OFF (GPIO15)");
                app.call_method(0x01, 0x01, vec![0x00], Arc::new(move |_payload| {Ok(vec![0x01])}));
            }
        }
        button1_previous_state = button1_current_state;
        drop(button1_pin); // Release the lock

        // Handle Button 2 (GPIO14 -> method_id 0x02)
        let button2_pin = button2_pin_clone.lock().unwrap();
        let button2_current_state = button2_pin.is_high();
        
        if button2_current_state && !button2_previous_state {
            button2_switch_state = !button2_switch_state;
            
            if button2_switch_state {
                println!("Button 2 Switch ON (GPIO14)");
                app.call_method(0x01, 0x02, vec![0x01], Arc::new(move |_payload| {Ok(vec![0x01])}));
            } else {
                println!("Button 2 Switch OFF (GPIO14)");
                app.call_method(0x01, 0x02, vec![0x00], Arc::new(move |_payload| {Ok(vec![0x01])}));
            }
        }
        button2_previous_state = button2_current_state;
        drop(button2_pin); // Release the lock

        // Handle Button 3 (GPIO32 -> method_id 0x03)
        let button3_pin = button3_pin_clone.lock().unwrap();
        let button3_current_state = button3_pin.is_high();
        
        if button3_current_state && !button3_previous_state {
            button3_switch_state = !button3_switch_state;
            
            if button3_switch_state {
                println!("Button 3 Switch ON (GPIO32)");
                app.call_method(0x01, 0x03, vec![0x01], Arc::new(move |_payload| {Ok(vec![0x01])}));
            } else {
                println!("Button 3 Switch OFF (GPIO32)");
                app.call_method(0x01, 0x03, vec![0x00], Arc::new(move |_payload| {Ok(vec![0x01])}));
            }
        }
        button3_previous_state = button3_current_state;
        drop(button3_pin); // Release the lock
        
        thread::sleep(Duration::from_millis(10)); // Polling interval
    }


}
