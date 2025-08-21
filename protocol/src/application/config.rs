use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use crate::sd::config::ServiceDiscoveryConfig;

#[derive(Clone)]
pub struct ServiceApplicationConfig {
    pub bind_addr: IpAddr,
    pub port: u16,
    pub discovery_config: ServiceDiscoveryConfig,
    /// Timeout for method calls
    pub method_call_timeout: Duration,
    /// Maximum number of open requests at a time
    pub max_open_requests: usize,
    /// Timeout for subscriptions
    pub subscription_timeout: Duration,
    /// Interval to sleep between processing requests
    pub sleep_interval: Duration,
    /// Interval to check for request timeouts
    pub request_timeout_check_interval: Duration,
    /// Maximum number of sockets to open
    pub max_sockets: usize,
    /// Maximum number of clients that can connect
    pub max_clients: usize,
}

impl Default for ServiceApplicationConfig {
    fn default() -> Self {
        Self {
            bind_addr: IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            port: 5678,
            discovery_config: ServiceDiscoveryConfig::default(),
            method_call_timeout: Duration::from_secs(5),
            max_open_requests: 16,
            subscription_timeout: Duration::from_millis(1000),
            sleep_interval: Duration::from_millis(10),
            request_timeout_check_interval: Duration::from_millis(1000),
            max_sockets: 8,
            max_clients: 8,
        }
    }
}
