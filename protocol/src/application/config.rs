use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use crate::sd::config::ServiceDiscoveryConfig;

#[derive(Clone)]
pub struct ServiceApplicationConfig {
    pub bind_addr: IpAddr,
    pub port: u16,
    pub discovery_config: ServiceDiscoveryConfig,
    pub method_call_timeout: Duration,
}

impl Default for ServiceApplicationConfig {
    fn default() -> Self {
        Self {
            bind_addr: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            port: 5678,
            discovery_config: ServiceDiscoveryConfig::default(),
            method_call_timeout: Duration::from_secs(5),
        }
    }
}