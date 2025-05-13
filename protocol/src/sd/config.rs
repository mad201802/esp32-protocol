use std::net::IpAddr;

use super::constants::{DEFAULT_SD_BIND_ADDR, DEFAULT_SD_PORT};

#[derive(Debug, Clone)]
pub struct ServiceDiscoveryConfig {
    pub bind_addr: IpAddr,
    pub port: u16,
}

impl Default for ServiceDiscoveryConfig {
    fn default() -> Self {
        Self {
            bind_addr: DEFAULT_SD_BIND_ADDR,
            port: DEFAULT_SD_PORT
        }
    }
}