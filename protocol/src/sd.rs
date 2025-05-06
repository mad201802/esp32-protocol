use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::message::DiscoveryMessage;

use crate::constants::{DEFAULT_BIND_ADDR, DEFAULT_PORT, DEFAULT_RETRIES, DEFAULT_RETRY_DELAY_MS, DEFAULT_TIMEOUT_MS, MAX_PACKET_SIZE};

/// Configuration for service discovery
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub bind_addr: IpAddr,
    pub port: u16,
    pub timeout_ms: u64,
    pub retries: u32,
    pub retry_delay_ms: u64,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            bind_addr: DEFAULT_BIND_ADDR,
            port: DEFAULT_PORT,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            retries: DEFAULT_RETRIES,
            retry_delay_ms: DEFAULT_RETRY_DELAY_MS,
        }
    }
}

/// The main service discovery struct
pub struct ServiceDiscovery {
    config: DiscoveryConfig,
    provided_services: Arc<Mutex<HashMap<String, bool>>>,
    socket: Option<Arc<UdpSocket>>,
    server_thread: Option<thread::JoinHandle<()>>,
    running: Arc<Mutex<bool>>,
}

impl ServiceDiscovery {
    /// Create a new ServiceDiscovery instance with default configuration
    pub fn new() -> Self {
        Self::with_config(DiscoveryConfig::default())
    }

    /// Create a new ServiceDiscovery instance with custom configuration
    pub fn with_config(config: DiscoveryConfig) -> Self {
        ServiceDiscovery {
            config,
            provided_services: Arc::new(Mutex::new(HashMap::new())),
            socket: None,
            server_thread: None,
            running: Arc::new(Mutex::new(false)),
        }
    }

    /// Initialize the service discovery system
    pub fn init(&mut self) -> Result<()> {
        let socket = UdpSocket::bind(SocketAddr::new(
            self.config.bind_addr, 
            self.config.port
        ))?;
        
        socket.set_broadcast(true)?;
        
        socket.set_read_timeout(Some(Duration::from_millis(100)))?;
        
        let socket_arc = Arc::new(socket);
        self.socket = Some(socket_arc.clone());

        let running = self.running.clone();
        {
            let mut running_guard = running.lock().unwrap();
            *running_guard = true;
        }

        let provided_services = self.provided_services.clone();
        let thread_socket = socket_arc.clone();
        let thread_running = running.clone();
        
        let server_thread = thread::spawn(move || {
            let mut buf = [0u8; MAX_PACKET_SIZE];
            
            while *thread_running.lock().unwrap() {
                match thread_socket.recv_from(&mut buf) {
                    Ok((size, src)) => {
                        println!("Received discovery message from: {}", src);
                        if let Some(message) = DiscoveryMessage::from_bytes(&buf[..size]) {
                            match message {
                                DiscoveryMessage::Request(service_name) => {
                                    let provides = {
                                        let services = provided_services.lock().unwrap();
                                        services.contains_key(&service_name)
                                    };
                                    
                                    if provides {
                                        let response = DiscoveryMessage::Response(service_name).to_bytes();
                                        let _ = thread_socket.send_to(&response, src);
                                    }
                                },
                                _ => { /* Ignore other message types */ }
                            }
                        }
                    },
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                        thread::sleep(Duration::from_millis(10));
                    },
                    Err(e) => {
                        eprintln!("Error receiving discovery message: {}", e);
                        break;
                    }
                }
            }
        });

        self.server_thread = Some(server_thread);
        Ok(())
    }

    /// Discover a service by name, returns the IP address of the service if found
    pub fn discover_service(&self, service_name: &str) -> Result<IpAddr> {
        let socket = match &self.socket {
            Some(s) => s.clone(),
            None => return Err(Error::new(ErrorKind::NotConnected, "Service discovery not initialized")),
        };

        let broadcast_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)),
            self.config.port
        );

        let request = DiscoveryMessage::Request(service_name.to_string()).to_bytes();
        
        let mut attempts = 0;
        let timeout = Duration::from_millis(self.config.timeout_ms);
        let retry_delay = Duration::from_millis(self.config.retry_delay_ms);
        
        while attempts < self.config.retries {
            socket.send_to(&request, broadcast_addr)?;
            
            let start_time = Instant::now();
            let mut buf = [0u8; MAX_PACKET_SIZE];
            
            while start_time.elapsed() < timeout {
                match socket.recv_from(&mut buf) {
                    Ok((size, src)) => {
                        if let Some(DiscoveryMessage::Response(name)) = DiscoveryMessage::from_bytes(&buf[..size]) {
                            if name == service_name {
                                return Ok(src.ip());
                            }
                        }
                    },
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                        thread::sleep(Duration::from_millis(10));
                    },
                    Err(e) => return Err(e),
                }
            }
            
            attempts += 1;
            if attempts < self.config.retries {
                thread::sleep(retry_delay);
            }
        }
        
        Err(Error::new(ErrorKind::NotFound, "Service not found"))
    }

    /// Register to provide a service with the given name
    pub fn provide_service(&self, service_name: &str) -> Result<()> {
        if self.socket.is_none() {
            return Err(Error::new(ErrorKind::NotConnected, "Service discovery not initialized"));
        }
        
        let mut services = self.provided_services.lock().unwrap();
        services.insert(service_name.to_string(), true);
        
        Ok(())
    }

    /// Stop providing a specific service
    pub fn stop_providing_service(&self, service_name: &str) -> Result<()> {
        let mut services = self.provided_services.lock().unwrap();
        services.remove(service_name);
        
        Ok(())
    }

    /// Shut down the service discovery system
    pub fn shutdown(&mut self) {
        {
            let mut running = self.running.lock().unwrap();
            *running = false;
        }
        
        if let Some(thread) = self.server_thread.take() {
            let _ = thread.join();
        }
        
        self.socket = None;
        self.provided_services.lock().unwrap().clear();
    }
}

impl Drop for ServiceDiscovery {
    fn drop(&mut self) {
        self.shutdown();
    }
}