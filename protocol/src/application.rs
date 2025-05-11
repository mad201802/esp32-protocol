use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    sync::{mpsc::{channel, Sender}, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use crate::{
    constants::{
        APP_PREFIX, DEFAULT_BIND_ADDR, DEFAULT_PORT, DEFAULT_RETRIES, DEFAULT_RETRY_DELAY_MS,
        DEFAULT_TIMEOUT_MS, MAX_PACKET_SIZE, MethodInvokeCallback,
        SD_REQUEST_PREFIX, SD_RESPONSE_PREFIX,
    },
    packets::{
        application::{ApplicationMessage, ApplicationMessageReturnCode, ApplicationMessageType},
        sd::DiscoveryMessage,
    },
};

#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub timeout_ms: u64,
    pub retries: u32,
    pub retry_delay_ms: u64,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            timeout_ms: DEFAULT_TIMEOUT_MS,
            retries: DEFAULT_RETRIES,
            retry_delay_ms: DEFAULT_RETRY_DELAY_MS,
        }
    }
}

pub struct ApplicationConfig {
    pub bind_addr: IpAddr,
    pub port: u16,
    pub discovery_config: DiscoveryConfig,
}

impl ApplicationConfig {
    pub fn new(
        bind_addr: Option<IpAddr>,
        port: Option<u16>,
        discovery_config: DiscoveryConfig,
    ) -> Self {
        ApplicationConfig {
            bind_addr: bind_addr.unwrap_or(DEFAULT_BIND_ADDR),
            port: port.unwrap_or(DEFAULT_PORT),
            discovery_config,
        }
    }
}

pub struct Application {
    config: ApplicationConfig,
    socket: Option<Arc<UdpSocket>>,
    server_thread: Option<thread::JoinHandle<()>>,
    running: Arc<Mutex<bool>>,
    services_mapping: Arc<Mutex<HashMap<u16, IpAddr>>>, // service_id -> ip
    offered_methods: Arc<Mutex<HashMap<(u16, u16), MethodInvokeCallback>>>, // (service_id, method_id) -> callback
    open_responses: Arc<Mutex<HashMap<u16, Sender<ApplicationMessage>>>>, // request_id -> callback
}

impl Application {
    pub fn new(config: ApplicationConfig) -> Self {
        Application {
            config,
            socket: None,
            server_thread: None,
            running: Arc::new(Mutex::new(false)),
            services_mapping: Arc::new(Mutex::new(HashMap::new())),
            offered_methods: Arc::new(Mutex::new(HashMap::new())),
            open_responses: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn init(&mut self) -> anyhow::Result<()> {
        let socket = UdpSocket::bind(SocketAddr::new(self.config.bind_addr, self.config.port))?;

        socket.set_broadcast(true)?;
        socket.set_read_timeout(Some(Duration::from_millis(100)))?;

        self.socket = Some(Arc::new(socket).clone());

        Ok(())
    }

    /// Discover a service by id
    pub fn discover_service(&self, service_id: u16) -> anyhow::Result<IpAddr> {
        let service_mapping = self.services_mapping.lock().unwrap();
        if let Some(ip) = service_mapping.get(&service_id) {
            return Ok(*ip);
        }

        let socket = match &self.socket {
            Some(s) => s.clone(),
            None => {
                return Err(anyhow::Error::new(Error::new(
                    ErrorKind::NotConnected,
                    "Service discovery not initialized",
                )));
            }
        };

        let broadcast_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)),
            self.config.port,
        );

        let request = DiscoveryMessage::Request(service_id).to_bytes();

        let mut attempts = 0;
        let timeout = Duration::from_millis(self.config.discovery_config.timeout_ms);
        let retry_delay = Duration::from_millis(self.config.discovery_config.retry_delay_ms);

        while attempts < self.config.discovery_config.retries {
            socket.send_to(&request, broadcast_addr)?;

            let start_time = Instant::now();
            let mut buf = [0u8; MAX_PACKET_SIZE];

            while start_time.elapsed() < timeout {
                match socket.recv_from(&mut buf) {
                    Ok((size, src)) => {
                        if let Some(DiscoveryMessage::Response(response_service_id)) =
                            DiscoveryMessage::from_bytes(&buf[..size])
                        {
                            if response_service_id == service_id {
                                let mut services_mapping = self.services_mapping.lock().unwrap();
                                services_mapping.insert(service_id, src.ip());
                                return Ok(src.ip());
                            }
                        }
                    }
                    Err(ref e)
                        if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut =>
                    {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => return Err(anyhow::Error::new(e)),
                }
            }

            attempts += 1;
            if attempts < self.config.discovery_config.retries {
                thread::sleep(retry_delay);
            }
        }

        Err(Error::new(ErrorKind::NotFound, "Service not found").into())
    }

    pub fn offer_method(
        &mut self,
        service_id: u16,
        method_id: u16,
        callback: MethodInvokeCallback,
    ) -> anyhow::Result<()> {
        let mut offered_methods = self.offered_methods.lock().unwrap();
        offered_methods.insert((service_id, method_id), callback);
        Ok(())
    }

    pub fn call_method(
        &self,
        service_id: u16,
        method_id: u16,
        data: Vec<u8>
    ) -> anyhow::Result<Vec<u8>> {
        let ip = self.discover_service(service_id)?;

        let message = ApplicationMessage::new(
            service_id,
            method_id,
            None,
            ApplicationMessageType::Request,
            ApplicationMessageReturnCode::Ok,
            data,
        );

        let socket = match &self.socket {
            Some(s) => s.clone(),
            None => {
                return Err(anyhow::Error::new(Error::new(
                    ErrorKind::NotConnected,
                    "Service discovery not initialized",
                )));
            }
        };

        let socket_addr = SocketAddr::new(ip, self.config.port);
        let request = message.to_bytes().unwrap();
        socket.send_to(&request, socket_addr)?;

        let (tx, rx) = channel::<ApplicationMessage>();
        let mut open_responses = self.open_responses.lock().unwrap();
        open_responses.insert(message.request_id, tx);

        let timeout = Duration::from_millis(self.config.discovery_config.timeout_ms);
        let start_time = Instant::now();
        while start_time.elapsed() < timeout {
            match rx.recv_timeout(timeout) {
                Ok(response) => {
                    if response.message_type == ApplicationMessageType::Response {

                        // Remove the response from the open responses
                        open_responses.remove(&response.request_id);

                        return Ok(response.payload);
                    } else {
                        return Err(anyhow::Error::new(Error::new(
                            ErrorKind::InvalidData,
                            "Invalid response type",
                        )));
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    continue;
                }
                Err(e) => return Err(anyhow::Error::new(e)),
            }
        }

        Err(anyhow::Error::new(Error::new(
            ErrorKind::TimedOut,
            "Method call timed out",
        )))

    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        if self.server_thread.is_some() {
            return Err(anyhow::Error::new(Error::new(
                ErrorKind::AlreadyExists,
                "Application already running",
            )));
        }

        if self.socket.is_none() {
            return Err(anyhow::Error::new(Error::new(
                ErrorKind::NotConnected,
                "Application not initialized",
            )));
        }

        let running = self.running.clone();
        {
            let mut running_guard = running.lock().unwrap();
            *running_guard = true;
        }

        let thread_socket = self.socket.clone().unwrap();
        let thread_running = running.clone();

        let server_thread = thread::spawn(move || {
            let mut buf = [0u8; MAX_PACKET_SIZE];

            while *thread_running.lock().unwrap() {
                match thread_socket.recv_from(&mut buf) {
                    Ok((size, src)) => {
                        println!("Received message from: {}", src);

                        if size < 2 {
                            continue;
                        }

                        let prefix = u16::from_be_bytes([buf[0], buf[1]]);

                        match prefix {
                            SD_REQUEST_PREFIX | SD_RESPONSE_PREFIX => {
                                if let Some(message) = DiscoveryMessage::from_bytes(&buf[..size]) {
                                    match message {
                                        DiscoveryMessage::Request(request_service_id) => {
                                            todo!(
                                                "Handle discovery request for service: {}",
                                                request_service_id
                                            );
                                        }
                                        _ => { /* Ignore other message types */ }
                                    }
                                }
                            }
                            APP_PREFIX => {
                                if let Ok(message) = ApplicationMessage::from_bytes(&buf[..size]) {
                                    match message.message_type {
                                        ApplicationMessageType::Request => {}
                                        ApplicationMessageType::Response => {}
                                        ApplicationMessageType::Notification => {}
                                        ApplicationMessageType::Unsubscribe => {}
                                    }
                                }
                            }
                            _ => {
                                eprintln!("Unknown message prefix: {} not {}", prefix, APP_PREFIX);
                            }
                        }
                    }
                    Err(ref e)
                        if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut =>
                    {
                        thread::sleep(Duration::from_millis(10));
                    }
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

    pub fn shutdown(&mut self) {
        {
            let mut running = self.running.lock().unwrap();
            *running = false;
        }

        if let Some(thread) = self.server_thread.take() {
            let _ = thread.join();
        }

        self.socket = None;
    }
}

impl Drop for Application {
    fn drop(&mut self) {
        self.shutdown();
    }
}
