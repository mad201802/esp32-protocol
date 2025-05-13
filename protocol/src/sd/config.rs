use std::net::{Ipv4Addr};

#[derive(Debug, Clone)]
pub struct ServiceDiscoveryConfig {
    pub bind_addr: Ipv4Addr,
    pub multicast_addr: Ipv4Addr,
    pub port: u16,
}

impl Default for ServiceDiscoveryConfig {
    fn default() -> Self {
        Self {
            bind_addr: Ipv4Addr::new(0, 0, 0, 0),
            multicast_addr: Ipv4Addr::new(239, 255, 0, 1),
            port: 30490,
        }
    }
}