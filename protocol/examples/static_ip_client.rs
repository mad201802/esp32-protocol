use std::{
    net::Ipv4Addr, sync::Arc, thread::{self}, time::Duration
};

use anyhow::Result;
use protocol::{application::{_impl_sync::ServiceApplication, config::ServiceApplicationConfig}, sd::{config::ServiceDiscoveryConfig, ServiceDiscovery}};

fn main() -> Result<()> {
    env_logger::init();

    let mut service_application_config = ServiceApplicationConfig::default();
    let mut service_discovery_config = ServiceDiscoveryConfig::default();

    service_discovery_config
        .add_static_service(0x01, Ipv4Addr::new(192, 168, 0, 5).into());

    service_application_config.discovery_config = service_discovery_config;

    let mut app = ServiceApplication::<ServiceDiscovery>::with_config(0x02, service_application_config);
    app.init()?;
    app.start(false)?;

    thread::sleep(Duration::from_secs(2));

    println!("Left turn signal ...");
    app.call_method(
        0x01,
        0x01,
        vec![],
        Arc::new(|data| {
            match data {
                Ok(data) => {
                    log::info!("Received data: {:?}", data);
                }
                Err(err) => {
                    log::error!("Error: {:?}", err);
                }
            }

            Ok(vec![])
        }),
    );

    thread::sleep(Duration::from_secs(3));

    println!("Right turn signal ...");
    app.call_method(
        0x01,
        0x02,
        vec![],
        Arc::new(|data| {
            match data {
                Ok(data) => {
                    log::info!("Received data: {:?}", data);
                }
                Err(err) => {
                    log::error!("Error: {:?}", err);
                }
            }

            Ok(vec![])
        }),
    );

    thread::sleep(Duration::from_secs(3));

    println!("Hazard light signal ...");
    app.call_method(
        0x01,
        0x03,
        vec![],
        Arc::new(|data| {
            match data {
                Ok(data) => {
                    log::info!("Received data: {:?}", data);
                }
                Err(err) => {
                    log::error!("Error: {:?}", err);
                }
            }

            Ok(vec![])
        }),
    );

    loop {
        thread::sleep(Duration::from_secs(1));
    }
}
