use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr, UdpSocket},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
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
    receiver_thread: Option<JoinHandle<()>>,
    send_thread: Option<JoinHandle<()>>,
    server_running: Arc<AtomicBool>,
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
            receiver_thread: None,
            send_thread: None,
            server_running: Arc::new(AtomicBool::new(false)),
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

        let socket = Arc::new(socket);
        self.socket = Some(socket.clone());
        Ok(())
    }

    pub fn start(&mut self) -> Result<()> {
        if self.socket.is_none() {
            return Err(anyhow::anyhow!("Socket not initialized"));
        }

        let socket = self.socket.clone().unwrap();

        self.server_running.store(true, Ordering::SeqCst);

        let service_id = self.service_id.clone();
        let services_mapping = self.services_mapping.clone();

        let server_running = self.server_running.clone();
        let receiver_socket = socket.try_clone()?;
        let receiver_thread = std::thread::spawn(move || {
            while server_running.load(Ordering::SeqCst) {
                let mut buf = [0; 768];

                match receiver_socket.recv_from(&mut buf) {
                    Ok((size, src)) => {
                        if let Ok(packet) = ServiceDiscoveryMessage::from_bytes(&buf[..size]) {
                            match packet {
                                // If a FindService message is received, check if the ID matches
                                ServiceDiscoveryMessage::FindService(id) => {
                                    if id == service_id {
                                        let response =
                                            ServiceDiscoveryMessage::OfferService(service_id);
                                        let response_bytes = response.to_bytes();
                                        receiver_socket.send_to(&response_bytes, src).unwrap();
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
            }
        });

        let service_id = self.service_id.clone();
        let multicast_addr =
            SocketAddr::new(IpAddr::V4(self.config.multicast_addr), self.config.port);

        let server_running = self.server_running.clone();
        let send_thread = std::thread::spawn(move || {
            while server_running.load(Ordering::SeqCst) {
                let broadcast_message = ServiceDiscoveryMessage::OfferService(service_id);
                let broadcast_bytes = broadcast_message.to_bytes();
                match socket.send_to(&broadcast_bytes, multicast_addr) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Error broadcasting service ID: {}", e);
                    }
                }

                // Sleep for a while to avoid busy waiting
                thread::sleep(Duration::from_secs(1));
            }
        });

        self.receiver_thread = Some(receiver_thread);
        self.send_thread = Some(send_thread);
        Ok(())
    }

    pub fn stop(&mut self) {
        println!("Stopping service discovery...");
        self.server_running.store(false, Ordering::SeqCst);

        if let Some(receiver_thread) = self.receiver_thread.take() {
            receiver_thread.join().unwrap();
        }
        println!("Receiver thread stopped");

        if let Some(send_thread) = self.send_thread.take() {
            send_thread.join().unwrap();
        }
        println!("Sender thread stopped");

        let multicast_addr =
            SocketAddr::new(IpAddr::V4(self.config.multicast_addr), self.config.port);
        if let Some(socket) = self.socket.take() {
            let stop_message = ServiceDiscoveryMessage::StopOfferService(self.service_id);
            let stop_bytes = stop_message.to_bytes();
            socket.send_to(&stop_bytes, multicast_addr).unwrap();
            println!("[SOS] Sent StopOfferService message");
        }
    }
}

impl Drop for ServiceDiscovery {
    fn drop(&mut self) {
        self.stop();
    }
}