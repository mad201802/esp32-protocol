use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddrV4},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use log::{error, info, trace};
use tokio::time::{sleep, timeout};

use anyhow::Result;
use tokio::{net::UdpSocket, sync::Mutex, task::JoinHandle};

use super::{config::ServiceDiscoveryConfig, packets::ServiceDiscoveryMessage};

// Service discovery packets are small (3 bytes), but we allow some buffer for network overhead
const SD_RECV_BUFFER_SIZE: usize = 64;

#[derive(Debug, Clone)]
struct ServiceEntry {
    ip: IpAddr,
    last_seen: Instant,
}

#[derive(Clone)]
pub struct ServiceDiscovery {
    service_id: u16,
    config: ServiceDiscoveryConfig,
    socket: Option<Arc<UdpSocket>>,
    receiver_thread: Option<Arc<JoinHandle<()>>>,
    send_thread: Option<Arc<JoinHandle<()>>>,
    server_running: Arc<AtomicBool>,
    services_mapping: Arc<Mutex<HashMap<u16, ServiceEntry>>>,
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
        info!(
            "Creating new service discovery with ID: {}",
            service_id
        );
        Self {
            service_id,
            config,
            socket: None,
            receiver_thread: None,
            send_thread: None,
            server_running: Arc::new(AtomicBool::new(false)),
            services_mapping: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Initializes the UDP socket and joins the multicast group.
    /// 
    /// # Returns
    /// * `Ok(())` if initialization succeeded
    /// * `Err` if socket binding or multicast join failed
    pub async fn init(&mut self) -> Result<()> {
        info!(
            "Initializing service discovery with ID: {}",
            self.service_id
        );
        let socket_addr = SocketAddrV4::new(self.config.bind_addr, self.config.port);
        let socket = UdpSocket::bind(socket_addr).await?;

        socket.join_multicast_v4(self.config.multicast_addr, self.config.bind_addr)?;
        socket.set_multicast_loop_v4(false)?;

        let socket = Arc::new(socket);
        self.socket = Some(socket.clone());

        info!(
            "Service discovery initialized with ID: {} and socket: {:?}",
            self.service_id,
            socket
        );
        Ok(())
    }

    /// Starts the service discovery by launching receiver and sender threads.
    /// 
    /// # Returns
    /// * `Ok(())` if both threads started successfully
    /// * `Err` if socket is not initialized
    /// 
    /// # Behavior
    /// - Receiver thread listens for incoming service announcements
    /// - Sender thread broadcasts this service's availability periodically
    pub async fn start(&mut self) -> Result<()> {
        if self.socket.is_none() {
            return Err(anyhow::anyhow!("Socket not initialized"));
        }

        info!(
            "Starting service discovery with ID: {} and socket: {:?}",
            self.service_id,
            self.socket
        );

        let socket = self.socket.clone().unwrap();

        self.server_running.store(true, Ordering::SeqCst);

        let services_mapping = self.services_mapping.clone();

        let server_running = self.server_running.clone();
        let receiver_socket = socket.clone();
        let receiver_thread = tokio::spawn({
            let services_mapping = services_mapping.clone();
            let server_running = server_running.clone();
            let receiver_socket = receiver_socket.clone();
            async move {
                while server_running.load(Ordering::SeqCst) {
                    let mut buf = [0; SD_RECV_BUFFER_SIZE];

                    match receiver_socket.recv_from(&mut buf).await {
                        Ok((size, src)) => {
                            if let Ok(packet) = ServiceDiscoveryMessage::from_bytes(&buf[..size]) {
                                match packet {
                                    ServiceDiscoveryMessage::OfferService(id) => {
                                        trace!("Received OfferService for ID: {} from {}", id, src.ip());
                                        let mut mapping = services_mapping.lock().await;
                                        mapping.insert(id, ServiceEntry {
                                            ip: src.ip(),
                                            last_seen: Instant::now(),
                                        });
                                    }
                                    ServiceDiscoveryMessage::StopOfferService(id) => {
                                        trace!("Received StopOfferService for ID: {} from {}", id, src.ip());
                                        let mut mapping = services_mapping.lock().await;
                                        mapping.remove(&id);
                                    }
                                }
                            } else {
                                trace!("Failed to parse packet from {}: invalid format or length", src.ip());
                            }
                        }
                        Err(e) => {
                            if e.kind() != std::io::ErrorKind::WouldBlock {
                                error!("Error receiving data: {}", e);
                            }
                        }
                    }
                }
            }
        });

        info!(
            "Service discovery receiver thread started with ID: {} and socket: {:?}",
            self.service_id,
            socket
        );

        let service_id = self.service_id;
        let multicast_addr = SocketAddrV4::new(self.config.multicast_addr, self.config.port);
        
        // Pre-serialize the broadcast message to avoid repeated allocations
        let broadcast_message = ServiceDiscoveryMessage::OfferService(service_id);
        let broadcast_bytes = broadcast_message.to_bytes();

        let send_thread = tokio::spawn({
            let socket = socket.clone();
            let server_running = server_running.clone();
            let broadcast_interval = self.config.broadcast_interval;
            async move {
                while server_running.load(Ordering::SeqCst) {
                    if let Err(e) = socket.send_to(&broadcast_bytes, multicast_addr).await {
                        error!("Error broadcasting service ID {}: {}", service_id, e);
                    }
                    sleep(broadcast_interval).await;
                }
            }
        });

        info!(
            "Service discovery sender thread started with ID: {} and socket: {:?}",
            self.service_id,
            socket
        );

        self.receiver_thread = Some(Arc::new(receiver_thread));
        self.send_thread = Some(Arc::new(send_thread));
        
        info!(
            "Service discovery started with ID: {} and socket: {:?}",
            self.service_id,
            socket
        );

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
    pub async fn find_service(&self, service_id: u16) -> Option<IpAddr> {
        // Clean up stale entries first
        self.cleanup_stale_services().await;
        
        let mapping = self.services_mapping.lock().await;
        if let Some(entry) = mapping.get(&service_id) {
            Some(entry.ip)
        } else {
            None
        }
    }
    
    /// Remove services that haven't been seen within the TTL
    async fn cleanup_stale_services(&self) {
        let mut mapping = self.services_mapping.lock().await;
        let now = Instant::now();
        let ttl = self.config.service_ttl;
        
        mapping.retain(|service_id, entry| {
            let is_fresh = now.duration_since(entry.last_seen) <= ttl;
            if !is_fresh {
                trace!("Removing stale service ID: {}", service_id);
            }
            is_fresh
        });
    }

    /// Stops the service discovery and cleans up resources.
    /// 
    /// This method:
    /// 1. Signals threads to stop
    /// 2. Waits for threads to join with timeout
    /// 3. Sends a stop announcement
    /// 4. Cleans up the socket
    pub async fn stop(&mut self) {
        info!(
            "Stopping service discovery with ID: {} and socket: {:?}",
            self.service_id,
            self.socket
        );
        // Signal threads to stop
        self.server_running.store(false, Ordering::SeqCst);

        // Join receiver thread
        if let Some(receiver_thread) = self.receiver_thread.take() {
            if let Ok(join_handle) = Arc::try_unwrap(receiver_thread) {
                match timeout(self.config.socket_timeout, join_handle).await {
                    Ok(Ok(())) => trace!("Receiver thread stopped gracefully"),
                    Ok(Err(e)) => error!("Receiver thread panicked: {:?}", e),
                    Err(_) => error!("Receiver thread did not stop within timeout"),
                }
            } else {
                error!("Multiple references to receiver thread exist, cannot join cleanly");
            }
        }

        // Join sender thread  
        if let Some(send_thread) = self.send_thread.take() {
            if let Ok(join_handle) = Arc::try_unwrap(send_thread) {
                match timeout(self.config.socket_timeout, join_handle).await {
                    Ok(Ok(())) => trace!("Sender thread stopped gracefully"),
                    Ok(Err(e)) => error!("Sender thread panicked: {:?}", e),
                    Err(_) => error!("Sender thread did not stop within timeout"),
                }
            } else {
                error!("Multiple references to sender thread exist, cannot join cleanly");
            }
        }

        // Send stop message before taking the socket
        let multicast_addr = SocketAddrV4::new(self.config.multicast_addr, self.config.port);
        if let Some(socket) = &self.socket {
            let stop_message = ServiceDiscoveryMessage::StopOfferService(self.service_id);
            let stop_bytes = stop_message.to_bytes();
            if let Err(e) = socket.send_to(&stop_bytes, multicast_addr).await {
                error!("Failed to send stop message: {}", e);
            }
        }
        
        // Now take the socket to ensure cleanup
        self.socket.take();

        info!(
            "Service discovery stopped with ID: {} and socket: {:?}",
            self.service_id,
            self.socket
        );

    }
    
    /// Get the service ID for this instance
    #[cfg(test)]
    pub fn service_id(&self) -> u16 {
        self.service_id
    }
}
