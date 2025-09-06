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
use protocol::sd::ServiceDiscovery;
use log::info;

// Turn signal states
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
enum SignalState {
    Off = 0,
    LeftOn = 1,
    RightOn = 2,
    HazardOn = 3,
}

// Constants for SOMEIP communication
const NOTIFICATION_EVENT_ID: u16 = 0x0002; // Event ID for turn signals notification

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

    // Create the SOMEIP application
    let mut app = ServiceApplication::<ServiceDiscovery>::new(0x01);

    // Initialize the application
    app.init()?;

    // LED pin setup - using GPIO 14 for left, GPIO 15 for right
    let gpio14 = pins.gpio14;
    let gpio15 = pins.gpio15;
    let left_led = Arc::new(Mutex::new(PinDriver::output(gpio14).unwrap()));
    let right_led = Arc::new(Mutex::new(PinDriver::output(gpio15).unwrap()));

    // Shared signal state using AtomicU8 to represent the enum
    let signal_state = Arc::new(AtomicU8::new(SignalState::Off as u8));
    
    // Clone references for the background blinker thread
    let left_led_blink = left_led.clone();
    let right_led_blink = right_led.clone();
    let signal_state_blink = signal_state.clone();
    
    // Create shared state for LED status
    let left_led_state = Arc::new(AtomicU8::new(0));   // 0 = off, 1 = on
    let right_led_state = Arc::new(AtomicU8::new(0));  // 0 = off, 1 = on
    
    // Clone the state for the blinker thread
    let left_led_state_blink = left_led_state.clone();
    let right_led_state_blink = right_led_state.clone();
    
    // Background thread that handles LED blinking based on current state
    thread::spawn(move || {
        let mut blink_on = false;
        let mut prev_left_state = 0;
        let mut prev_right_state = 0;
        
        loop {
            let current_state = signal_state_blink.load(Ordering::Relaxed);
            
            match current_state {
                0 => { // SignalState::Off
                    // Turn off both LEDs
                    {
                        let mut left = left_led_blink.lock().unwrap();
                        let mut right = right_led_blink.lock().unwrap();
                        left.set_low().unwrap();
                        right.set_low().unwrap();
                        
                        // Update LED states if they changed
                        if prev_left_state != 0 {
                            left_led_state_blink.store(0, Ordering::Relaxed);
                            prev_left_state = 0;
                        }
                        
                        if prev_right_state != 0 {
                            right_led_state_blink.store(0, Ordering::Relaxed);
                            prev_right_state = 0;
                        }
                    }
                    thread::sleep(Duration::from_millis(100));
                }
                1 => { // SignalState::LeftOn
                    {
                        let mut left = left_led_blink.lock().unwrap();
                        let mut right = right_led_blink.lock().unwrap();
                        right.set_low().unwrap(); // Ensure right is off
                        
                        // Update right LED state if it changed
                        if prev_right_state != 0 {
                            right_led_state_blink.store(0, Ordering::Relaxed);
                            prev_right_state = 0;
                        }
                        
                        // Blink left LED
                        let new_left_state = if blink_on { 1 } else { 0 };
                        if blink_on {
                            left.set_high().unwrap();
                        } else {
                            left.set_low().unwrap();
                        }
                        
                        // If LED state changed, update
                        if prev_left_state != new_left_state {
                            left_led_state_blink.store(new_left_state, Ordering::Relaxed);
                            prev_left_state = new_left_state;
                        }
                    }
                    thread::sleep(Duration::from_millis(500));
                    blink_on = !blink_on;
                }
                2 => { // SignalState::RightOn
                    {
                        let mut left = left_led_blink.lock().unwrap();
                        let mut right = right_led_blink.lock().unwrap();
                        left.set_low().unwrap(); // Ensure left is off
                        
                        // Update left LED state if it changed
                        if prev_left_state != 0 {
                            left_led_state_blink.store(0, Ordering::Relaxed);
                            prev_left_state = 0;
                        }
                        
                        // Blink right LED
                        let new_right_state = if blink_on { 1 } else { 0 };
                        if blink_on {
                            right.set_high().unwrap();
                        } else {
                            right.set_low().unwrap();
                        }
                        
                        // If LED state changed, update
                        if prev_right_state != new_right_state {
                            right_led_state_blink.store(new_right_state, Ordering::Relaxed);
                            prev_right_state = new_right_state;
                        }
                    }
                    thread::sleep(Duration::from_millis(500));
                    blink_on = !blink_on;
                }
                3 => { // SignalState::HazardOn
                    {
                        let mut left = left_led_blink.lock().unwrap();
                        let mut right = right_led_blink.lock().unwrap();
                        
                        // Blink both LEDs
                        let new_state = if blink_on { 1 } else { 0 };
                        if blink_on {
                            left.set_high().unwrap();
                            right.set_high().unwrap();
                        } else {
                            left.set_low().unwrap();
                            right.set_low().unwrap();
                        }
                        
                        // If LED states changed, update
                        if prev_left_state != new_state {
                            left_led_state_blink.store(new_state, Ordering::Relaxed);
                            prev_left_state = new_state;
                        }
                        if prev_right_state != new_state {
                            right_led_state_blink.store(new_state, Ordering::Relaxed);
                            prev_right_state = new_state;
                        }
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

    
    // Register the notification event
    app.offer_event(NOTIFICATION_EVENT_ID);
    
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
            
            // Log state change
            info!("Turn signals state change: From {} to {}", current_state, new_state);
            
            // Store the new state - LED state changes and notifications will be handled by other threads
            signal_state_left.store(new_state, Ordering::Relaxed);
            
            // Return acknowledgment of the command
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
            
            // Log state change
            info!("Turn signals state change: From {} to {}", current_state, new_state);
            
            // Store the new state - LED state changes and notifications will be handled by other threads
            signal_state_right.store(new_state, Ordering::Relaxed);
            
            // Return acknowledgment of the command
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
            
            // Log state change
            info!("Turn signals state change: From {} to {}", current_state, new_state);
            
            // Store the new state - LED state changes and notifications will be handled by other threads
            signal_state_hazard.store(new_state, Ordering::Relaxed);
            
            // Return acknowledgment of the command
            Ok(vec![new_state])
        })
    );
    
    app.start(false)?;
    
    // Create a separate thread for notifications based on actual LED states
    let notification_app = app;
    let left_led_state_notify = left_led_state.clone();
    let right_led_state_notify = right_led_state.clone();
    let signal_state_notify = signal_state.clone();
    
    thread::spawn(move || {
        // Keep track of last sent LED states to avoid duplicate notifications
        let mut prev_left = 255; // Initialize to invalid value to ensure first notification
        let mut prev_right = 255;
        let mut prev_hazard = 255;
        
        loop {
            // Read current LED states
            let left_state = left_led_state_notify.load(Ordering::Relaxed);
            let right_state = right_led_state_notify.load(Ordering::Relaxed);
            
            // Calculate current hazard state based on the signal state
            let current_state = signal_state_notify.load(Ordering::Relaxed);
            let hazard_state = if current_state == 3 { 1 } else { 0 };
            
            // Only send notification if any state changed
            if left_state != prev_left || right_state != prev_right || hazard_state != prev_hazard {
                info!("Sending LED states notification: left={}, hazard={}, right={}", 
                      left_state, hazard_state, right_state);
                
                // Format for the frontend: [left_state, hazard_state, right_state]
                let data = vec![left_state, hazard_state, right_state];
                notification_app.notify(NOTIFICATION_EVENT_ID, data);
                
                // Update previous state
                prev_left = left_state;
                prev_right = right_state;
                prev_hazard = hazard_state;
            }
            
            // Check frequently to catch all state changes during blinking
            thread::sleep(Duration::from_millis(100));
        }
    });

    loop {
        thread::sleep(Duration::from_millis(500)); // Reduce frequency to prevent memory pressure
    }
}
