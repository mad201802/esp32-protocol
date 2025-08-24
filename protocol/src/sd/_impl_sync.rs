use anyhow::Result;
use log::{debug, error, info, trace};
use parking_lot::Mutex;
use std::{
    net::{IpAddr, SocketAddrV4, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::sd::{
    constants::{
        CLEANUP_INTERVAL_MS, CONFLICT_CHECK_TIMEOUT_MS, POLL_INTERVAL_MS, SD_RECV_BUFFER_SIZE,
    },
    registry::ServiceRegistry,
};

use super::{
    ServiceDiscoveryInterface, config::ServiceDiscoveryConfig, error::ServiceDiscoveryError,
    message::ServiceDiscoveryMessage,
};

pub struct ServiceDiscovery {
    service_id: u16,
    config: ServiceDiscoveryConfig,
    socket: Option<Arc<UdpSocket>>,
    receiver_thread: Option<JoinHandle<()>>,
    send_thread: Option<JoinHandle<()>>,
    server_running: Arc<AtomicBool>,
    services_registry: Arc<Mutex<ServiceRegistry>>,
}

impl ServiceDiscovery {
    /// Creates a new instance of `ServiceDiscovery` with default configuration.
    ///
    /// # Arguments
    /// * `service_id` - Unique identifier for this service instance
    pub fn new(service_id: u16) -> Self {
        Self::with_config(service_id, ServiceDiscoveryConfig::default())
    }

    /// Creates a new instance of `ServiceDiscovery` with the provided configuration.
    ///
    /// # Arguments
    /// * `service_id` - Unique identifier for this service instance
    /// * `config` - Configuration parameters for service discovery
    pub fn with_config(service_id: u16, config: ServiceDiscoveryConfig) -> Self {
        debug!("Creating new service discovery with ID: {}", service_id);
        Self {
            service_id,
            config,
            socket: None,
            receiver_thread: None,
            send_thread: None,
            server_running: Arc::new(AtomicBool::new(false)),
            services_registry: Arc::new(Mutex::new(ServiceRegistry::new())),
        }
    }

    /// Remove services that haven't been seen within the TTL
    fn cleanup_stale_services(&self) {
        let mut registry = self.services_registry.lock();
        let ttl = self.config.service_ttl;
        registry.cleanup_stale(ttl);
    }

    /// Check if the current service ID is already being broadcasted on the network
    ///
    /// This method will listen on the multicast socket for a brief period to detect
    /// if another service with the same ID is already broadcasting.
    ///
    /// # Returns
    /// * `Ok(())` if no conflict is detected
    /// * `Err(ServiceDiscoveryError::ServiceIdConflict)` if the service ID is already in use
    fn check_service_id_conflict(&self) -> std::result::Result<(), ServiceDiscoveryError> {
        let socket = self
            .socket
            .as_ref()
            .ok_or(ServiceDiscoveryError::SocketNotInitialized)?;

        info!(
            "Checking for service ID conflicts for ID: {}",
            self.service_id
        );

        // Set a reasonable timeout for conflict detection
        let conflict_check_timeout = Duration::from_millis(CONFLICT_CHECK_TIMEOUT_MS);
        let start_time = Instant::now();

        // Temporarily set socket to blocking mode with timeout for conflict check
        socket
            .set_read_timeout(Some(Duration::from_millis(50)))
            .map_err(ServiceDiscoveryError::BindFailed)?;
        socket
            .set_nonblocking(false)
            .map_err(ServiceDiscoveryError::BindFailed)?;

        let mut buf = [0; SD_RECV_BUFFER_SIZE];

        while start_time.elapsed() < conflict_check_timeout {
            match socket.recv_from(&mut buf) {
                Ok((size, src)) => {
                    if let Ok(packet) = ServiceDiscoveryMessage::from_bytes(&buf[..size])
                        && let ServiceDiscoveryMessage::OfferService(id) = packet
                        && id == self.service_id
                    {
                        error!(
                            "Service ID conflict detected: Service ID {} is already being offered by {}",
                            id,
                            src.ip()
                        );
                        // Restore socket to non-blocking mode before returning error
                        if let Err(e) = socket.set_nonblocking(true) {
                            error!("Failed to restore socket to non-blocking mode: {}", e);
                        }
                        return Err(ServiceDiscoveryError::ServiceIdConflict(self.service_id));
                    }
                }
                Err(e) => {
                    // Timeout or would block - continue checking
                    if e.kind() != std::io::ErrorKind::TimedOut
                        && e.kind() != std::io::ErrorKind::WouldBlock
                    {
                        error!("Error during conflict check: {}", e);
                    }
                }
            }
        }

        // Restore socket to non-blocking mode
        socket
            .set_nonblocking(true)
            .map_err(ServiceDiscoveryError::BindFailed)?;

        debug!(
            "No service ID conflict detected for ID: {}",
            self.service_id
        );
        Ok(())
    }

    pub fn get_service_mapping(&self) -> Vec<(u16, IpAddr)> {
        let mut services = Vec::new();
        
        // Add static services first
        for (&service_id, &ip_addr) in &self.config.static_services {
            services.push((service_id, ip_addr));
        }
        
        // Add discovered services that don't conflict with static ones
        let registry = self.services_registry.lock();
        let discovered_services = registry.get_active_services();
        
        for (service_id, ip_addr) in discovered_services {
            // Only add if not already present as a static service
            if !self.config.has_static_service(service_id) {
                services.push((service_id, ip_addr));
            }
        }
        
        services
    }

    /// Add a static service mapping
    /// 
    /// Note: This should be called before `init()` or the service needs to be reinitialized
    /// to take effect.
    /// 
    /// # Arguments
    /// * `service_id` - The service ID to map
    /// * `ip_addr` - The IP address for this service
    pub fn add_static_service(&mut self, service_id: u16, ip_addr: IpAddr) {
        self.config.add_static_service(service_id, ip_addr);
        
        // If already initialized, update the registry immediately
        if self.socket.is_some() {
            let mut registry = self.services_registry.lock();
            if let Err(e) = registry.insert_static(service_id, ip_addr) {
                error!("Failed to add static service {} -> {}: {}", service_id, ip_addr, e);
            }
        }
    }

    /// Remove a static service mapping
    /// 
    /// # Arguments
    /// * `service_id` - The service ID to remove
    pub fn remove_static_service(&mut self, service_id: u16) {
        self.config.remove_static_service(service_id);
        
        // If already initialized, update the registry immediately
        if self.socket.is_some() {
            let mut registry = self.services_registry.lock();
            registry.remove_static(service_id);
        }
    }

    /// Get all static service mappings
    /// 
    /// # Returns
    /// * Vector of (service_id, ip_addr) tuples for all static services
    pub fn get_static_services(&self) -> Vec<(u16, IpAddr)> {
        self.config.static_services.iter().map(|(&id, &ip)| (id, ip)).collect()
    }

    /// Check if a service ID has a static mapping
    /// 
    /// # Arguments
    /// * `service_id` - The service ID to check
    /// 
    /// # Returns
    /// * `true` if the service has a static mapping, `false` otherwise
    pub fn has_static_service(&self, service_id: u16) -> bool {
        self.config.has_static_service(service_id)
    }
}

/// Implement the ServiceDiscoveryInterface trait for ServiceDiscovery
impl ServiceDiscoveryInterface for ServiceDiscovery {
    /// Initializes the UDP socket and joins the multicast group.
    /// Also loads any configured static services into the registry.
    ///
    /// # Returns
    /// * `Ok(())` if initialization succeeded
    /// * `Err` if socket binding or multicast join failed
    fn init(&mut self) -> Result<()> {
        info!(
            "Initializing service discovery with ID: {}",
            self.service_id
        );
        let socket_addr = SocketAddrV4::new(self.config.bind_addr, self.config.port);
        let socket = UdpSocket::bind(socket_addr)?;

        socket.join_multicast_v4(&self.config.multicast_addr, &self.config.bind_addr)?;
        socket.set_multicast_loop_v4(false)?;

        // Set socket to non-blocking mode for polling
        socket.set_nonblocking(true)?;

        let socket = Arc::new(socket);
        self.socket = Some(socket.clone());

        // Load static services into the registry
        if !self.config.static_services.is_empty() {
            info!("Loading {} static services", self.config.static_services.len());
            let mut registry = self.services_registry.lock();
            if let Err(e) = registry.load_static_services(&self.config.static_services) {
                error!("Failed to load some static services: {}", e);
            }
        }

        info!(
            "Service discovery initialized with ID: {} and socket: {:?}",
            self.service_id, socket_addr
        );
        Ok(())
    }

    /// Starts the service discovery by launching receiver and sender threads.
    ///
    /// # Returns
    /// * `Ok(())` if both threads started successfully
    /// * `Err` if socket is not initialized or service ID conflict is detected
    ///
    /// # Behavior
    /// - First checks if the service ID is already in use on the network
    /// - Receiver thread listens for incoming service announcements
    /// - Sender thread broadcasts this service's availability periodically
    fn start(&mut self) -> Result<()> {
        if self.server_running.load(Ordering::SeqCst) {
            debug!("Service discovery with ID {} is already running", self.service_id);
            return Ok(());
        }

        if self.socket.is_none() {
            return Err(anyhow::anyhow!("Socket not initialized"));
        }

        info!("Starting service discovery with ID: {}", self.service_id);

        if self.config.collision_detection {
            // Check for service ID conflicts before starting
            if let Err(conflict_err) = self.check_service_id_conflict() {
                return Err(anyhow::anyhow!("Service ID conflict: {}", conflict_err));
            }
        }

        let socket = self.socket.clone().unwrap();

        self.server_running.store(true, Ordering::SeqCst);

        let services_registry = self.services_registry.clone();

        let server_running = self.server_running.clone();
        let receiver_socket = socket.clone();
        let receiver_thread = {
            let services_registry = services_registry.clone();
            let server_running = server_running.clone();
            let receiver_socket = receiver_socket.clone();
            let cleanup_interval = Duration::from_millis(CLEANUP_INTERVAL_MS);
            let service_ttl = self.config.service_ttl; // Use config TTL
            let mut last_cleanup = Instant::now();

            thread::spawn(move || {
                while server_running.load(Ordering::SeqCst) {
                    let mut buf = [0; SD_RECV_BUFFER_SIZE];

                    match receiver_socket.recv_from(&mut buf) {
                        Ok((size, src)) => {
                            if let Ok(packet) = ServiceDiscoveryMessage::from_bytes(&buf[..size]) {
                                let mut registry = services_registry.lock();
                                match packet {
                                    ServiceDiscoveryMessage::OfferService(id) => {
                                        trace!(
                                            "Received OfferService for ID: {} from {}",
                                            id,
                                            src.ip()
                                        );
                                        let _ = registry.insert(id, src.ip());
                                    }
                                    ServiceDiscoveryMessage::StopOfferService(id) => {
                                        trace!(
                                            "Received StopOfferService for ID: {} from {}",
                                            id,
                                            src.ip()
                                        );
                                        registry.remove(id);
                                    }
                                }
                            } else {
                                trace!(
                                    "Failed to parse packet from {}: invalid format or length",
                                    src.ip()
                                );
                            }
                        }
                        Err(e) => {
                            if e.kind() != std::io::ErrorKind::WouldBlock {
                                error!("Error receiving data: {}", e);
                            }
                        }
                    }

                    // Periodic cleanup of stale services (embedded-friendly)
                    if Instant::now().duration_since(last_cleanup) >= cleanup_interval {
                        let now = Instant::now();
                        let mut registry = services_registry.lock();
                        registry.cleanup_stale(service_ttl); // Use config TTL
                        last_cleanup = now;
                    }

                    // Small sleep to prevent busy loop and save CPU
                    thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
                }
            })
        };

        debug!(
            "Service discovery receiver thread started with ID: {}",
            self.service_id
        );

        let service_id = self.service_id;
        let multicast_addr = SocketAddrV4::new(self.config.multicast_addr, self.config.port);

        // Pre-serialize the broadcast message to avoid repeated allocations
        let broadcast_message = ServiceDiscoveryMessage::OfferService(service_id);
        let broadcast_bytes = broadcast_message.to_bytes_array();

        let send_thread = {
            let socket = socket.clone();
            let server_running = server_running.clone();
            let broadcast_interval = self.config.broadcast_interval;
            thread::spawn(move || {
                while server_running.load(Ordering::SeqCst) {
                    if let Err(e) = socket.send_to(&broadcast_bytes, multicast_addr) {
                        error!("Error broadcasting service ID {}: {}", service_id, e);
                    }
                    thread::sleep(broadcast_interval);
                }
            })
        };

        debug!(
            "Service discovery sender thread started with ID: {}",
            self.service_id
        );

        self.receiver_thread = Some(receiver_thread);
        self.send_thread = Some(send_thread);

        info!("Service discovery started with ID: {}", self.service_id);

        Ok(())
    }

    /// Finds the IP address of a service by its ID.
    /// 
    /// Static services take precedence over discovered services.
    ///
    /// # Arguments
    /// * `service_id` - The ID of the service to find
    ///
    /// # Returns
    /// * `Some(IpAddr)` if the service is found (static or discovered)
    /// * `None` if the service is not found
    fn find_service(&self, service_id: u16) -> Option<IpAddr> {
        // First check static services
        if let Some(static_ip) = self.config.get_static_service(service_id) {
            return Some(static_ip);
        }

        // Then check discovered services
        // Clean up stale entries first
        self.cleanup_stale_services();

        let registry = self.services_registry.lock();
        registry.find(service_id)
    }

    /// Stops the service discovery and cleans up resources.
    ///
    /// This method:
    /// 1. Signals threads to stop
    /// 2. Waits for threads to join with timeout
    /// 3. Sends a stop announcement
    /// 4. Cleans up the socket
    fn stop(&mut self) {
        info!("Stopping service discovery with ID: {}", self.service_id);
        // Signal threads to stop
        self.server_running.store(false, Ordering::SeqCst);

        // Send stop message before joining threads
        let multicast_addr = SocketAddrV4::new(self.config.multicast_addr, self.config.port);
        if let Some(socket) = &self.socket {
            let stop_message = ServiceDiscoveryMessage::StopOfferService(self.service_id);
            let stop_bytes = stop_message.to_bytes_array();
            if let Err(e) = socket.send_to(&stop_bytes, multicast_addr) {
                error!("Failed to send stop message: {}", e);
            }
        }

        // Join receiver thread
        if let Some(receiver_thread) = self.receiver_thread.take() {
            if let Err(e) = receiver_thread.join() {
                error!("Receiver thread panicked: {:?}", e);
            } else {
                trace!("Receiver thread stopped gracefully");
            }
        }

        // Join sender thread
        if let Some(send_thread) = self.send_thread.take() {
            if let Err(e) = send_thread.join() {
                error!("Sender thread panicked: {:?}", e);
            } else {
                trace!("Sender thread stopped gracefully");
            }
            // Now take the socket to ensure cleanup
            if let Some(socket) = self.socket.take() {
                drop(socket);
            }

            info!("Service discovery stopped with ID: {}", self.service_id);
        }
    }
}
