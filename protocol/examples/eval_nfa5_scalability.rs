use std::{
    thread::{self},
    time::Duration,
};

use anyhow::Result;
use protocol::sd::{ServiceDiscovery, ServiceDiscoveryInterface};

fn main() -> Result<()> {
    env_logger::init();

    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 2 {
        eprintln!("Usage: {} num_available_services", args[0]);
        return Ok(());
    }

    let num_available_services: usize = args[1].parse().unwrap_or_else(|_| {
        eprintln!("Invalid number of available services: {}", args[1]);
        std::process::exit(1);
    });

    let mut sd = ServiceDiscovery::new(rand::random_range(0..=u16::MAX));
    sd.init()?;
    sd.start()?;

    let start_time = std::time::Instant::now();
    let end_time: std::time::Instant;

    loop {
        let available_services = sd.get_service_mapping();
        if available_services.len() == num_available_services - 1 {
            end_time = std::time::Instant::now();
            break;
        }

        // Simulate some processing time
        thread::sleep(Duration::from_millis(100));
    }

    let elapsed_time = end_time.duration_since(start_time);
    println!(
        "Time taken to discover {} services: {:?}",
        num_available_services - 1,
        elapsed_time
    );

    Ok(())
}
