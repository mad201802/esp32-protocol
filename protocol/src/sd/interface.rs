use std::net::IpAddr;
use anyhow::Result;


/// Interface for service discovery implementations
/// 
/// This trait defines the contract for service discovery systems, allowing
/// different implementations to be used interchangeably with the application layer.
pub trait ServiceDiscoveryInterface: Send + Sync {
    /// Initialize the service discovery system
    /// 
    /// # Returns
    /// * `Ok(())` if initialization succeeded
    /// * `Err` if initialization failed
    fn init(&mut self) -> Result<()>;

    /// Start the service discovery system
    /// 
    /// This typically involves starting background threads for announcing
    /// this service and listening for other service announcements.
    /// 
    /// # Returns
    /// * `Ok(())` if started successfully
    /// * `Err` if failed to start
    fn start(&mut self) -> Result<()>;

    /// Find the IP address of a service by its ID
    /// 
    /// # Arguments
    /// * `service_id` - The ID of the service to find
    /// 
    /// # Returns
    /// * `Some(IpAddr)` if the service is found and reachable
    /// * `None` if the service is not found or unreachable
    fn find_service(&self, service_id: u16) -> Option<IpAddr>;

    /// Stop the service discovery system
    /// 
    /// This should gracefully shut down all background threads and
    /// announce that this service is no longer available.
    fn stop(&mut self);
}
