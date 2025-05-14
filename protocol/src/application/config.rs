use std::net::{IpAddr, Ipv4Addr};

use crate::sd::config::ServiceDiscoveryConfig;

pub struct ApplicationConfig {
    pub bind_addr: IpAddr,
    pub port: u16,
    pub discovery_config: ServiceDiscoveryConfig,
}

impl Default for ApplicationConfig {
    fn default() -> Self {
        Self {
            bind_addr: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            port: 5678,
            discovery_config: ServiceDiscoveryConfig::default(),
        }
    }
}