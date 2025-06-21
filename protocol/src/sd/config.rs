use std::net::Ipv4Addr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ServiceDiscoveryConfig {
    pub bind_addr: Ipv4Addr,
    pub multicast_addr: Ipv4Addr,
    pub port: u16,
    pub broadcast_interval: Duration,
    /// Timeout for socket operations
    pub socket_timeout: Duration,
    /// Time-to-live for discovered services (removes stale entries)
    pub service_ttl: Duration,
}

impl Default for ServiceDiscoveryConfig {
    fn default() -> Self {
        Self {
            bind_addr: Ipv4Addr::new(0, 0, 0, 0),
            multicast_addr: Ipv4Addr::new(239, 255, 0, 1),
            port: 30490,
            broadcast_interval: Duration::from_secs(1),
            socket_timeout: Duration::from_secs(5),
            service_ttl: Duration::from_secs(3),
        }
    }
}