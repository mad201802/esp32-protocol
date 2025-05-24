use std::net::Ipv4Addr;
use std::time::Duration;

use super::{ServiceDiscovery, config::ServiceDiscoveryConfig};

/// Builder for creating ServiceDiscovery instances with custom configuration
pub struct ServiceDiscoveryBuilder {
    service_id: u16,
    config: ServiceDiscoveryConfig,
}

impl ServiceDiscoveryBuilder {
    /// Create a new builder with the given service ID
    pub fn new(service_id: u16) -> Self {
        Self {
            service_id,
            config: ServiceDiscoveryConfig::default(),
        }
    }

    /// Set the bind address (default: 0.0.0.0)
    pub fn bind_addr(mut self, addr: Ipv4Addr) -> Self {
        self.config.bind_addr = addr;
        self
    }

    /// Set the multicast address (default: 239.255.0.1)
    pub fn multicast_addr(mut self, addr: Ipv4Addr) -> Self {
        self.config.multicast_addr = addr;
        self
    }

    /// Set the port (default: 30490)
    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    /// Set the broadcast interval (default: 1 second)
    pub fn broadcast_interval(mut self, interval: Duration) -> Self {
        self.config.broadcast_interval = interval;
        self
    }

    /// Set the socket timeout (default: 5 seconds)
    pub fn socket_timeout(mut self, timeout: Duration) -> Self {
        self.config.socket_timeout = timeout;
        self
    }

    /// Set the service TTL (default: 30 seconds)
    pub fn service_ttl(mut self, ttl: Duration) -> Self {
        self.config.service_ttl = ttl;
        self
    }

    /// Build the ServiceDiscovery instance
    pub fn build(self) -> ServiceDiscovery {
        ServiceDiscovery::with_config(self.service_id, self.config)
    }
}

impl ServiceDiscovery {
    /// Create a builder for this service discovery instance
    pub fn builder(service_id: u16) -> ServiceDiscoveryBuilder {
        ServiceDiscoveryBuilder::new(service_id)
    }
}
