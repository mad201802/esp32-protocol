use std::net::IpAddr;

use crate::sd::config::ServiceDiscoveryConfig;

use super::constants::{DEFAULT_BIND_ADDR, DEFAULT_PORT};

pub struct ApplicationConfig {
    pub bind_addr: IpAddr,
    pub port: u16,
    pub discovery_config: ServiceDiscoveryConfig,
}

impl Default for ApplicationConfig {
    fn default() -> Self {
        Self {
            bind_addr: DEFAULT_BIND_ADDR,
            port: DEFAULT_PORT,
            discovery_config: ServiceDiscoveryConfig::default(),
        }
    }
}