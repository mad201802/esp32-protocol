use std::fmt;

/// Errors that can occur during service discovery operations
#[derive(Debug)]
pub enum ServiceDiscoveryError {
    /// Socket is not initialized
    SocketNotInitialized,
    /// Failed to bind to socket
    BindFailed(std::io::Error),
    /// Failed to join multicast group
    MulticastJoinFailed(std::io::Error),
    /// Thread join failed
    ThreadJoinFailed,
    /// Socket operation timed out
    SocketTimeout,
    /// Service discovery is already running
    AlreadyRunning,
    /// Service discovery is not running
    NotRunning,
    /// Service ID is already in use on the network
    ServiceIdConflict(u16),
}

impl fmt::Display for ServiceDiscoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceDiscoveryError::SocketNotInitialized => {
                write!(f, "Socket not initialized")
            }
            ServiceDiscoveryError::BindFailed(e) => {
                write!(f, "Failed to bind socket: {}", e)
            }
            ServiceDiscoveryError::MulticastJoinFailed(e) => {
                write!(f, "Failed to join multicast group: {}", e)
            }
            ServiceDiscoveryError::ThreadJoinFailed => {
                write!(f, "Failed to join worker threads")
            }
            ServiceDiscoveryError::SocketTimeout => {
                write!(f, "Socket operation timed out")
            }
            ServiceDiscoveryError::AlreadyRunning => {
                write!(f, "Service discovery is already running")
            }
            ServiceDiscoveryError::NotRunning => {
                write!(f, "Service discovery is not running")
            }
            ServiceDiscoveryError::ServiceIdConflict(service_id) => {
                write!(f, "Service ID {} is already in use on the network", service_id)
            }
        }
    }
}

impl std::error::Error for ServiceDiscoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ServiceDiscoveryError::BindFailed(e) | ServiceDiscoveryError::MulticastJoinFailed(e) => {
                Some(e)
            }
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, ServiceDiscoveryError>;
