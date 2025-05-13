use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr, UdpSocket},
    sync::{Arc, Mutex},
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
    server_running: Arc<Mutex<bool>>,
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
            server_running: Arc::new(Mutex::new(false)),
            services_mapping: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn init(&mut self) -> Result<()> {
        let socket = UdpSocket::bind(SocketAddr::new(self.config.bind_addr, self.config.port))?;

        socket.set_broadcast(true)?;
        socket.set_nonblocking(true)?;
        socket.set_ttl(100)?;

        let socket = Arc::new(socket);
        self.socket = Some(socket.clone());
        Ok(())
    }

    pub fn start(&mut self) -> Result<()> {
        if self.socket.is_none() {
            return Err(anyhow::anyhow!("Socket not initialized"));
        }

        let socket = self.socket.clone().unwrap();

        let running = self.server_running.clone();
        {
            let mut running_guard = running.lock().unwrap();
            *running_guard = true;
        }

        let services_mapping = self.services_mapping.clone();
        let service_id = self.service_id.clone();
        let config = self.config.clone();

        let server_running = running.clone();
        let server_thread = std::thread::spawn(move || {

            let mut broadcast_counter = 0;
            let mut offer_service_counter = 0;

            while *server_running.lock().unwrap() {
                let mut buf = [0; 512];

                match socket.recv_from(&mut buf) {
                    Ok((size, src)) => {
                        if let Ok(packet) = ServiceDiscoveryMessage::from_bytes(&buf[..size]) {
                            match packet {
                                ServiceDiscoveryMessage::FindService(id) => {
                                    if id == service_id {
                                        println!("Received FindService for ID: {} ({})", id, 0);
                                        // Handle FindService
                                        let response =
                                            ServiceDiscoveryMessage::OfferService(service_id);
                                        let response_bytes = response.to_bytes();
                                        socket.send_to(&response_bytes, src).unwrap();
                                    }
                                }
                                ServiceDiscoveryMessage::OfferService(id) => {
                                    if id != service_id {
                                        offer_service_counter += 1;
                                        println!("Received OfferService for ID: {} ({})", id, offer_service_counter);
                                        // Handle OfferService
                                        let mut mapping = services_mapping.lock().unwrap();
                                        mapping.insert(id, src.ip());
                                    }
                                }
                                ServiceDiscoveryMessage::StopOfferService(id) => {
                                    if id != service_id {
                                        println!("Received StopOfferService for ID: {}", id);
                                        // Handle StopOfferService
                                        let mut mapping = services_mapping.lock().unwrap();
                                        if mapping.contains_key(&id) {
                                            mapping.remove(&id);
                                            println!("Removed service ID: {}", id);
                                        }
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
                        broadcast_counter += 1;
                        println!(
                            "Broadcasting service ID: {} ({})",
                            service_id, broadcast_counter
                        );
                    }
                    Err(e) => {
                        eprintln!("Error broadcasting service ID: {}", e);
                    }
                }
            }
        });

        self.server_thread = Some(server_thread);
        Ok(())
    }
}

impl Drop for ServiceDiscovery {
    fn drop(&mut self) {
        let running = self.server_running.clone();
        {
            let mut running_guard = running.lock().unwrap();
            *running_guard = false;
        }

        if let Some(thread) = self.server_thread.take() {
            thread.join().unwrap();
        }

        if let Some(socket) = &self.socket {
            let stop_message = ServiceDiscoveryMessage::StopOfferService(self.service_id);
            let stop_bytes = stop_message.to_bytes();
            socket
                .send_to(
                    &stop_bytes,
                    SocketAddr::new(IpAddr::from([255, 255, 255, 255]), self.config.port),
                )
                .unwrap();
        } else {
            eprintln!("Socket not initialized, cannot send StopOfferService message");
        }
    }
}
