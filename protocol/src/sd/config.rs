use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
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
    /// Static mapping of service IDs to IP addresses
    /// These entries take precedence over discovered services and are never removed
    pub static_services: HashMap<u16, IpAddr>,
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
            static_services: HashMap::new(),
        }
    }
}

impl ServiceDiscoveryConfig {
    /// Add a static service mapping
    ///
    /// # Arguments
    /// * `service_id` - The service ID to map
    /// * `ip_addr` - The IP address for this service
    ///
    /// # Returns
    /// * `&mut Self` for method chaining
    pub fn add_static_service(&mut self, service_id: u16, ip_addr: IpAddr) -> &mut Self {
        self.static_services.insert(service_id, ip_addr);
        self
    }

    /// Add multiple static service mappings
    ///
    /// # Arguments
    /// * `services` - Iterator of (service_id, ip_addr) tuples
    ///
    /// # Returns
    /// * `&mut Self` for method chaining
    pub fn add_static_services<I>(&mut self, services: I) -> &mut Self
    where
        I: IntoIterator<Item = (u16, IpAddr)>,
    {
        for (service_id, ip_addr) in services {
            self.static_services.insert(service_id, ip_addr);
        }
        self
    }

    /// Remove a static service mapping
    ///
    /// # Arguments
    /// * `service_id` - The service ID to remove
    ///
    /// # Returns
    /// * `&mut Self` for method chaining
    pub fn remove_static_service(&mut self, service_id: u16) -> &mut Self {
        self.static_services.remove(&service_id);
        self
    }

    /// Check if a service ID has a static mapping
    ///
    /// # Arguments
    /// * `service_id` - The service ID to check
    ///
    /// # Returns
    /// * `true` if the service has a static mapping, `false` otherwise
    pub fn has_static_service(&self, service_id: u16) -> bool {
        self.static_services.contains_key(&service_id)
    }

    /// Get the static IP address for a service ID
    ///
    /// # Arguments
    /// * `service_id` - The service ID to lookup
    ///
    /// # Returns
    /// * `Some(IpAddr)` if a static mapping exists, `None` otherwise
    pub fn get_static_service(&self, service_id: u16) -> Option<IpAddr> {
        self.static_services.get(&service_id).copied()
    }
}
