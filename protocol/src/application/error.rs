use std::fmt;
use std::error::Error as StdError;

/// Application-specific error types
#[derive(Debug)]
pub enum ServiceApplicationError {
    /// Service discovery is not initialized
    ServiceDiscoveryNotInitialized,
    /// Failed to connect to a service
    ConnectionFailed { service_id: u16, source: anyhow::Error },
    /// Service not found
    ServiceNotFound(u16),
    /// Network operation failed
    NetworkError(std::io::Error),
    /// Serialization/deserialization error
    SerializationError(anyhow::Error),
    /// Channel communication error
    ChannelError(String),
    /// Configuration error
    ConfigError(String),
}

impl fmt::Display for ServiceApplicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServiceDiscoveryNotInitialized => {
                write!(f, "Service discovery component is not initialized")
            }
            Self::ConnectionFailed { service_id, source } => {
                write!(f, "Failed to connect to service {}: {}", service_id, source)
            }
            Self::ServiceNotFound(id) => {
                write!(f, "Service with ID {} not found", id)
            }
            Self::NetworkError(e) => {
                write!(f, "Network operation failed: {}", e)
            }
            Self::SerializationError(e) => {
                write!(f, "Serialization error: {}", e)
            }
            Self::ChannelError(msg) => {
                write!(f, "Channel communication error: {}", msg)
            }
            Self::ConfigError(msg) => {
                write!(f, "Configuration error: {}", msg)
            }
        }
    }
}

impl StdError for ServiceApplicationError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::ConnectionFailed { source, .. } => Some(source.root_cause()),
            Self::NetworkError(e) => Some(e),
            Self::SerializationError(e) => Some(e.root_cause()),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ServiceApplicationError {
    fn from(error: std::io::Error) -> Self {
        Self::NetworkError(error)
    }
}

pub type Result<T> = std::result::Result<T, ServiceApplicationError>;

/// Health status of the service application
#[derive(Debug, Clone, PartialEq)]
pub enum ServiceHealth {
    Healthy,
    Degraded { reason: String },
    Unhealthy { reason: String },
}

/// Service application metrics
#[derive(Debug, Default, Clone)]
pub struct ServiceMetrics {
    pub active_connections: usize,
    pub total_requests: u64,
    pub failed_requests: u64,
    pub total_events_sent: u64,
    pub open_subscriptions: usize,
}
