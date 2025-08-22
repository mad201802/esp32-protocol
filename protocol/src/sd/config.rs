use std::net::Ipv4Addr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ServiceDiscoveryConfig {
    /// Address to bind the service discovery socket to
    pub bind_addr: Ipv4Addr,
    /// Multicast address for service discovery
    pub multicast_addr: Ipv4Addr,
    /// Port for service discovery
    pub port: u16,
    /// Interval for broadcasting service discovery messages
    pub broadcast_interval: Duration,
    /// Timeout for socket operations
    pub socket_timeout: Duration,
    /// Time-to-live for discovered services (removes stale entries)
    pub service_ttl: Duration,
    /// Enable collision detection to avoid conflicts with other services
    pub collision_detection: bool,
}

impl Default for ServiceDiscoveryConfig {
    fn default() -> Self {
        Self {
            bind_addr: Ipv4Addr::new(0, 0, 0, 0),
            multicast_addr: Ipv4Addr::new(239, 255, 0, 1),
            port: 30490,
            broadcast_interval: Duration::from_secs(2),
            socket_timeout: Duration::from_secs(3),
            service_ttl: Duration::from_secs(3),
            collision_detection: true,
        }
    }
}
