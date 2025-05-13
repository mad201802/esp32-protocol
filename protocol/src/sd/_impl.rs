use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr, UdpSocket},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use anyhow::Result;

use super::{config::ServiceDiscoveryConfig, packets::ServiceDiscoveryMessage};

pub struct ServiceDiscovery {
    service_id: u16,
    config: ServiceDiscoveryConfig,
    socket: Option<Arc<UdpSocket>>,
    server_thread: Option<JoinHandle<()>>,
    services_mapping: Arc<Mutex<HashMap<u16, IpAddr>>>,
}

impl ServiceDiscovery {
    /// Creates a new instance of `ServiceDiscovery` with default configuration.
    pub fn new(service_id: u16) -> Self {
        Self::with_config(service_id, ServiceDiscoveryConfig::default())
    }

    /// Creates a new instance of `ServiceDiscovery` with the provided configuration.
    pub fn with_config(service_id: u16, config: ServiceDiscoveryConfig) -> Self {
        Self {
            service_id: service_id,
            config,
            socket: None,
            server_thread: None,
            services_mapping: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn init(&mut self) -> Result<()> {
        let socket = UdpSocket::bind(SocketAddr::new(self.config.bind_addr, self.config.port))?;

        socket.set_broadcast(true)?;
        socket.set_nonblocking(true)?;
        socket.set_read_timeout(Some(Duration::from_millis(100)))?;

        let socket = Arc::new(socket);
        self.socket = Some(socket.clone());
        Ok(())
    }

    pub fn start(&mut self) -> Result<()> {
        if self.socket.is_none() {
            return Err(anyhow::anyhow!("Socket not initialized"));
        }

        let socket = self.socket.clone().unwrap();

        let services_mapping = self.services_mapping.clone();
        let service_id = self.service_id.clone();
        let config = self.config.clone();
        let server_thread = std::thread::spawn(move || {
            loop {
                let mut buf = [0; 1024];
                match socket.recv_from(&mut buf) {
                    Ok((size, src)) => {
                        if let Ok(packet) = ServiceDiscoveryMessage::from_bytes(&buf[..size]) {
                            match packet {
                                ServiceDiscoveryMessage::FindService(id) => {
                                    if service_id == id {
                                        println!("Received FindService for ID: {}", id);
                                        // Handle FindService
                                        let response =
                                            ServiceDiscoveryMessage::OfferService(service_id);
                                        let response_bytes = response.to_bytes();
                                        socket.send_to(&response_bytes, src).unwrap();
                                    }
                                }
                                ServiceDiscoveryMessage::OfferService(id) => {
                                    if id != service_id {
                                        println!("Received OfferService for ID: {}", id);
                                        // Handle OfferService
                                        let mut mapping = services_mapping.lock().unwrap();
                                        mapping.insert(id, src.ip());
                                    }
                                }
                                ServiceDiscoveryMessage::StopOfferService(id) => {
                                    println!("Received StopOfferService for ID: {}", id);
                                    // Handle StopOfferService
                                    let mut mapping = services_mapping.lock().unwrap();
                                    if mapping.contains_key(&id) {
                                        mapping.remove(&id);
                                        println!("Removed service ID: {}", id);
                                    }
                                }
                            }
                        } else {
                            eprintln!("Failed to parse packet");
                        }
                    }
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::WouldBlock {
                            eprintln!("Error receiving data: {}", e);
                        }
                    }
                }

                // Braodcast own service ID
                let broadcast_message = ServiceDiscoveryMessage::OfferService(service_id);
                let broadcast_bytes = broadcast_message.to_bytes();
                match socket.send_to(
                    &broadcast_bytes,
                    SocketAddr::new(IpAddr::from([255, 255, 255, 255]), config.port),
                ) {
                    Ok(_) => {
                        println!("Broadcasted service ID: {}", service_id);
                    }
                    Err(e) => {
                        eprintln!("Error broadcasting service ID: {}", e);
                    }
                }

                // Print current services mapping
                let mapping = services_mapping.lock().unwrap();
                println!("Current services mapping:");
                for (id, ip) in mapping.iter() {
                    println!("Service ID: {}, IP: {}", id, ip);
                }

                thread::sleep(Duration::from_millis(500));
            }
        });

        self.server_thread = Some(server_thread);
        Ok(())
    }
}
