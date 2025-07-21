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
            // Optimized for embedded devices - less frequent broadcasts
            broadcast_interval: Duration::from_secs(2),
            socket_timeout: Duration::from_secs(5),
            // Shorter TTL for faster cleanup on resource-constrained devices
            service_ttl: Duration::from_secs(6),
        }
    }
}

impl ServiceDiscoveryConfig {
    /// Configuration optimized for embedded devices with limited resources
    pub fn embedded_optimized() -> Self {
        Self {
            bind_addr: Ipv4Addr::new(0, 0, 0, 0),
            multicast_addr: Ipv4Addr::new(239, 255, 0, 1),
            port: 30490,
            // More conservative broadcast interval to save bandwidth and power
            broadcast_interval: Duration::from_secs(2),
            socket_timeout: Duration::from_secs(3),
            // Shorter TTL for faster cleanup
            service_ttl: Duration::from_secs(10),
        }
    }
}