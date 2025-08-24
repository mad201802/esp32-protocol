use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use embedded_poc::eth::start_eth;
use esp_idf_svc::hal::gpio::PinDriver;
use esp_idf_svc::log::EspLogger;
use esp_idf_svc::{eventloop::EspSystemEventLoop, hal::prelude::Peripherals, ipv4};
use protocol::application::_impl_sync::ServiceApplication;

// Turn signal states
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
enum SignalState {
    Off = 0,
    LeftOn = 1,
    RightOn = 2,
    HazardOn = 3,
}

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

    // Shared signal state using AtomicU8 to represent the enum
    let signal_state = Arc::new(AtomicU8::new(SignalState::Off as u8));
    
    let signal_state_blink = signal_state.clone();
    
    // Background thread that handles LED blinking based on current state
    thread::spawn(move || {
        let mut blink_on = false;
        loop {
            let current_state = signal_state_blink.load(Ordering::Relaxed);
            
            match current_state {
                0 => { // SignalState::Off
                    // Turn off both LEDs
                    {
                        println!("Turning off both LEDs");
                    }
                    thread::sleep(Duration::from_millis(100));
                }
                1 => { // SignalState::LeftOn
                    {
                        println!("Blinking left LED");
                    }
                    thread::sleep(Duration::from_millis(500));
                    blink_on = !blink_on;
                }
                2 => { // SignalState::RightOn
                    {
                        println!("Blinking right LED");
                    }
                    thread::sleep(Duration::from_millis(500));
                    blink_on = !blink_on;
                }
                3 => { // SignalState::HazardOn
                    {
                        println!("Blinking both LEDs");
                    }
                    thread::sleep(Duration::from_millis(500));
                    blink_on = !blink_on;
                }
                _ => {
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
    });

    
    // Method 0x01: Left turn signal (toggle)
    let signal_state_left = signal_state.clone();
    app.offer_method(
        0x01,  
        Arc::new(move |_payload | {
            let current_state = signal_state_left.load(Ordering::Relaxed);
            
            let new_state = match current_state {
                1 => { // Currently left on -> turn off
                    println!("Left turn signal OFF");
                    SignalState::Off as u8
                }
                _ => { // Any other state -> turn on left (turning off right/hazard)
                    println!("Left turn signal ON");
                    SignalState::LeftOn as u8
                }
            };
            
            signal_state_left.store(new_state, Ordering::Relaxed);
            Ok(vec![new_state])
        }),
    );

    // Method 0x02: Right turn signal (toggle)
    let signal_state_right = signal_state.clone();
    app.offer_method(
        0x02,
        Arc::new(move |_payload| {
            let current_state = signal_state_right.load(Ordering::Relaxed);
            
            let new_state = match current_state {
                2 => { // Currently right on -> turn off
                    println!("Right turn signal OFF");
                    SignalState::Off as u8
                }
                _ => { // Any other state -> turn on right (turning off left/hazard)
                    println!("Right turn signal ON");
                    SignalState::RightOn as u8
                }
            };
            
            signal_state_right.store(new_state, Ordering::Relaxed);
            Ok(vec![new_state])
        })
    );

    // Method 0x03: Hazard lights (toggle) - can only be turned off by calling 0x03 again
    let signal_state_hazard = signal_state.clone();
    app.offer_method(
        0x03,
        Arc::new(move |_payload| {
            let current_state = signal_state_hazard.load(Ordering::Relaxed);
            
            let new_state = match current_state {
                3 => { // Currently hazard on -> turn off
                    println!("Hazard lights OFF");
                    SignalState::Off as u8
                }
                _ => { // Any other state -> turn on hazard (turning off left/right)
                    println!("Hazard lights ON");
                    SignalState::HazardOn as u8
                }
            };
            
            signal_state_hazard.store(new_state, Ordering::Relaxed);
            Ok(vec![new_state])
        })
    );
    
    app.start(false)?;

    loop {
        thread::sleep(Duration::from_millis(500)); // Reduce frequency to prevent memory pressure
    }
}
