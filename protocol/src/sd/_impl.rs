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
        let socket = UdpSocket::bind(SocketAddr::new(
            IpAddr::V4(self.config.bind_addr),
            self.config.port,
        ))?;

        socket.join_multicast_v4(&self.config.multicast_addr, &self.config.bind_addr)?;
        socket.set_multicast_loop_v4(false)?;
        socket.set_nonblocking(true)?;

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
        let multicast_addr = SocketAddr::new(
            IpAddr::V4(self.config.multicast_addr),
            self.config.port,
        );

        let server_running = running.clone();
        let server_thread = std::thread::spawn(move || {
            while *server_running.lock().unwrap() {
                let mut buf = [0; 768];

                match socket.recv_from(&mut buf) {
                    Ok((size, src)) => {
                        if let Ok(packet) = ServiceDiscoveryMessage::from_bytes(&buf[..size]) {
                            match packet {
                                // If a FindService message is received, check if the ID matches
                                ServiceDiscoveryMessage::FindService(id) => {
                                    if id == service_id {
                                        let response =
                                            ServiceDiscoveryMessage::OfferService(service_id);
                                        let response_bytes = response.to_bytes();
                                        socket.send_to(&response_bytes, src).unwrap();
                                    }
                                }
                                ServiceDiscoveryMessage::OfferService(id) => {
                                    println!("Received OfferService for ID: {}", id);
                                    let mut mapping = services_mapping.lock().unwrap();
                                    mapping.insert(id, src.ip());
                                }
                                ServiceDiscoveryMessage::StopOfferService(id) => {
                                    println!(" [RSOS] Received StopOfferService for ID: {}", id);
                                    let mut mapping = services_mapping.lock().unwrap();
                                    if mapping.contains_key(&id) {
                                        mapping.remove(&id);
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
                match socket.send_to(&broadcast_bytes, multicast_addr) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Error broadcasting service ID: {}", e);
                    }
                }

                // Print current services mapping
                {
                    let mapping = services_mapping.lock().unwrap();
                    println!("Current services mapping: {:?}", *mapping);
                }

                // Sleep for a while to avoid busy waiting
                thread::sleep(Duration::from_millis(500));
            }
        });

        self.server_thread = Some(server_thread);
        Ok(())
    }

    pub fn stop(&mut self) {
        let multicast_addr =
            SocketAddr::new(IpAddr::V4(self.config.multicast_addr), self.config.port);

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
            socket.send_to(&stop_bytes, multicast_addr).unwrap();
            println!("[SOS] Sent StopOfferService message");
        } else {
            eprintln!("Socket not initialized, cannot send StopOfferService message");
        }
    }

}
