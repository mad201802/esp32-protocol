use std::net::Ipv4Addr;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use embedded_poc::eth::start_eth;
use esp_idf_svc::log::EspLogger;
use esp_idf_svc::{eventloop::EspSystemEventLoop, hal::prelude::Peripherals, ipv4};
use protocol::application::_impl_sync::ServiceApplication;
use protocol::application::message::ApplicationResponseErrorMessage;
use crossbeam::channel;

const CURRENT_SERVICE_ID: u16 = 0x01;
const PEER_SERVICE_ID: u16 = 0x02;
const PEER_METHOD_ID: u16 = 0x01;
const NUM_ITERATIONS: usize = 100;

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

    let mut app = ServiceApplication::new(CURRENT_SERVICE_ID);
    app.init()?;
    app.start(false)?;

    // Wait for service discovery to settle
    thread::sleep(Duration::from_secs(2));

    println!("Starting RTT test with {} iterations", NUM_ITERATIONS);
    println!("============================================");

    const PAYLOAD: [u8; 5] = [0x01, 0x02, 0x03, 0x04, 0x05];
    let mut rtt_times = Vec::with_capacity(NUM_ITERATIONS);

    for iteration in 1..=NUM_ITERATIONS {
        let (response_tx, response_rx) = channel::bounded::<Result<Vec<u8>, ApplicationResponseErrorMessage>>(1);
        
        let start_time = Instant::now();
        
        app.call_method(
            PEER_SERVICE_ID,
            PEER_METHOD_ID,
            PAYLOAD.to_vec(),
            Arc::new(move |data| {
                let _ = response_tx.send(data);
                Ok(vec![])
            }),
        );

        // Wait for response with timeout
        match response_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(_response)) => {
                let rtt = start_time.elapsed();
                rtt_times.push(rtt);
                println!("Iteration {}: RTT = {:.2} ms", iteration, rtt.as_secs_f64() * 1000.0);
            }
            Ok(Err(err)) => {
                println!("Iteration {}: Error - {:?}", iteration, err);
            }
            Err(_) => {
                println!("Iteration {}: Timeout (> 5s)", iteration);
            }
        }

        // Small delay between iterations to avoid overwhelming the server
        thread::sleep(Duration::from_millis(100));
    }

    // Calculate statistics
    if !rtt_times.is_empty() {
        let total_time: Duration = rtt_times.iter().sum();
        let avg_time = total_time / rtt_times.len() as u32;
        let min_time = rtt_times.iter().min().unwrap();
        let max_time = rtt_times.iter().max().unwrap();

        println!("============================================");
        println!("RTT Test Results:");
        println!("  Successful requests: {}/{}", rtt_times.len(), NUM_ITERATIONS);
        println!("  Average RTT: {:.2} ms", avg_time.as_secs_f64() * 1000.0);
        println!("  Minimum RTT: {:.2} ms", min_time.as_secs_f64() * 1000.0);
        println!("  Maximum RTT: {:.2} ms", max_time.as_secs_f64() * 1000.0);

        // Print all RTT values (as ms) to copy and paste into a spreadsheet
        println!("RTT Values (ms):");
        for rtt in &rtt_times {
            println!("{:.2}", rtt.as_secs_f64() * 1000.0);
        }
    } else {
        println!("No successful requests completed!");
    }

    Ok(())
}