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

use super::{
    ServiceDiscoveryInterface, config::ServiceDiscoveryConfig, error::ServiceDiscoveryError,
    packets::ServiceDiscoveryMessage,
};

// Service discovery packets are small (3 bytes), but allow some buffer for network overhead
const SD_RECV_BUFFER_SIZE: usize = 64;
// Maximum number of services to track (embedded-friendly fixed size)
const MAX_TRACKED_SERVICES: usize = 32;
// Embedded-specific optimizations
const POLL_INTERVAL_MS: u64 = 10;
const CLEANUP_INTERVAL_MS: u64 = 1000;
// Reduced conflict check timeout for faster startup on embedded devices
const CONFLICT_CHECK_TIMEOUT_MS: u64 = 1000;

#[derive(Debug, Clone, Copy)]
pub struct ServiceEntry {
    service_id: u16,
    ip: IpAddr,
    last_seen: Instant,
    active: bool,
}

impl Default for ServiceEntry {
    fn default() -> Self {
        Self {
            service_id: 0,
            ip: IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
            last_seen: Instant::now(),
            active: false,
        }
    }
}

/// Fixed-size service registry optimized for embedded devices
#[derive(Debug)]
struct ServiceRegistry {
    entries: [ServiceEntry; MAX_TRACKED_SERVICES],
    next_slot: usize,
}

impl ServiceRegistry {
    fn new() -> Self {
        Self {
            entries: [ServiceEntry::default(); MAX_TRACKED_SERVICES],
            next_slot: 0,
        }
    }

    fn insert(&mut self, service_id: u16, ip: IpAddr) {
        // First, try to find existing entry for this service
        for entry in self.entries.iter_mut() {
            if entry.active && entry.service_id == service_id {
                entry.ip = ip;
                entry.last_seen = Instant::now();
                return;
            }
        }

        // If not found, try to find an inactive slot
        for entry in self.entries.iter_mut() {
            if !entry.active {
                *entry = ServiceEntry {
                    service_id,
                    ip,
                    last_seen: Instant::now(),
                    active: true,
                };
                return;
            }
        }

        // If no inactive slot, use round-robin replacement
        self.entries[self.next_slot] = ServiceEntry {
            service_id,
            ip,
            last_seen: Instant::now(),
            active: true,
        };
        self.next_slot = (self.next_slot + 1) % MAX_TRACKED_SERVICES;
    }

    fn remove(&mut self, service_id: u16) {
        for entry in self.entries.iter_mut() {
            if entry.active && entry.service_id == service_id {
                entry.active = false;
                break;
            }
        }
    }

    fn find(&self, service_id: u16) -> Option<IpAddr> {
        for entry in self.entries.iter() {
            if entry.active && entry.service_id == service_id {
                return Some(entry.ip);
            }
        }
        None
    }

    fn cleanup_stale(&mut self, ttl: Duration) {
        let now = Instant::now();
        for entry in self.entries.iter_mut() {
            if entry.active && now.duration_since(entry.last_seen) > ttl {
                entry.active = false;
            }
        }
    }

    fn get_active_services(&self) -> Vec<(u16, IpAddr)> {
        self.entries
            .iter()
            .filter(|entry| entry.active)
            .map(|entry| (entry.service_id, entry.ip))
            .collect()
    }
}

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

    /// Creates a new instance of `ServiceDiscovery` optimized for embedded devices.
    ///
    /// # Arguments
    /// * `service_id` - Unique identifier for this service instance
    pub fn new_embedded(service_id: u16) -> Self {
        Self::with_config(service_id, ServiceDiscoveryConfig::embedded_optimized())
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
            .map_err(|e| ServiceDiscoveryError::BindFailed(e))?;
        socket
            .set_nonblocking(false)
            .map_err(|e| ServiceDiscoveryError::BindFailed(e))?;

        let mut buf = [0; SD_RECV_BUFFER_SIZE];

        while start_time.elapsed() < conflict_check_timeout {
            match socket.recv_from(&mut buf) {
                Ok((size, src)) => {
                    if let Ok(packet) = ServiceDiscoveryMessage::from_bytes(&buf[..size]) {
                        if let ServiceDiscoveryMessage::OfferService(id) = packet {
                            if id == self.service_id {
                                error!(
                                    "Service ID conflict detected: Service ID {} is already being offered by {}",
                                    id,
                                    src.ip()
                                );
                                // Restore socket to non-blocking mode before returning error
                                let _ = socket.set_nonblocking(true);
                                return Err(ServiceDiscoveryError::ServiceIdConflict(
                                    self.service_id,
                                ));
                            }
                        }
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
            .map_err(|e| ServiceDiscoveryError::BindFailed(e))?;

        debug!(
            "No service ID conflict detected for ID: {}",
            self.service_id
        );
        Ok(())
    }

    pub fn get_service_mapping(&self) -> Vec<(u16, IpAddr)> {
        let registry = self.services_registry.lock();
        registry.get_active_services()
    }
}

/// Implement the ServiceDiscoveryInterface trait for ServiceDiscovery
impl ServiceDiscoveryInterface for ServiceDiscovery {
    /// Initializes the UDP socket and joins the multicast group.
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

        // Check for service ID conflicts before starting
        if let Err(conflict_err) = self.check_service_id_conflict() {
            return Err(anyhow::anyhow!("Service ID conflict: {}", conflict_err));
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
                                        registry.insert(id, src.ip());
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
                    let now = Instant::now();
                    if now.duration_since(last_cleanup) >= cleanup_interval {
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
    /// # Arguments
    /// * `service_id` - The ID of the service to find
    ///
    /// # Returns
    /// * `Some(IpAddr)` if the service is found and not stale
    /// * `None` if the service is not found or has expired
    fn find_service(&self, service_id: u16) -> Option<IpAddr> {
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
        }

        // Now take the socket to ensure cleanup
        self.socket.take();

        info!("Service discovery stopped with ID: {}", self.service_id);
    }
}
