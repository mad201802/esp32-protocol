pub mod eth;

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
use std::thread;

use anyhow::Result;
use esp_idf_svc::{eventloop::EspSystemEventLoop, hal::prelude::Peripherals, ipv4};
use esp_idf_sys::esp;
use eth::start_eth;
use protocol::application::{_impl_sync::ServiceApplication, message::ApplicationResponseErrorMessage};

fn main() -> Result<()> {
    // It is necessary to call this function once. Otherwise some patches to the runtime
    // implemented by esp-idf-sys might not link properly. See https://github.com/esp-rs/esp-idf-template/issues/71
    esp_idf_svc::sys::link_patches();

    // Bind the log crate to the ESP Logging facilities
    esp_idf_svc::log::EspLogger::initialize_default();

    println!("Please use the examples");
    Ok(())
}
