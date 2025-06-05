use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
    net::{IpAddr, SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use anyhow::Result;
use log::{debug, error, info, trace};
use parking_lot::Mutex;
use crossbeam::channel::{self, Receiver, Sender, TryRecvError};

use super::message::{ApplicationMessage, RawMessageData};

// Constants for TCP handling
const MAX_CLIENTS: usize = 8;
const MAX_SOCKETS: usize = 8;
const CLIENT_BUFFER_SIZE: usize = 256;
const TEMP_BUFFER_SIZE: usize = 256;
const SLEEP_INTERVAL_MS: u64 = 10;
const READ_TIMEOUT_MS: u64 = 100;

/// TCP Connection Pool for managing client connections and message routing
pub struct TcpConnectionPool {
    /// Currently connected socket addresses
    connected_sockets: Arc<Mutex<HashSet<IpAddr>>>,
    
    /// Map of client IP addresses to their message senders
    client_senders: Arc<Mutex<HashMap<IpAddr, Sender<ApplicationMessage>>>>,
    
    /// Channel for incoming messages from clients (sent to ServiceApplication)
    message_process_tx: Sender<RawMessageData>,
    message_process_rx: Option<Receiver<RawMessageData>>,
    
    /// Channel for outgoing messages to clients (received from ServiceApplication)
    client_response_tx: Sender<RawMessageData>,
    client_response_rx: Option<Receiver<RawMessageData>>,
    
    /// Server configuration
    bind_addr: IpAddr,
    port: u16,
    
    /// Server control
    server_running: Arc<AtomicBool>,
    server_thread: Option<JoinHandle<()>>,
    message_distributor_thread: Option<JoinHandle<()>>,
}

impl TcpConnectionPool {
    /// Create a new TCP connection pool
    pub fn new(bind_addr: IpAddr, port: u16) -> Self {
        let (message_process_tx, message_process_rx) = channel::bounded(32);
        let (client_response_tx, client_response_rx) = channel::bounded(32);
        
        Self {
            connected_sockets: Arc::new(Mutex::new(HashSet::with_capacity(MAX_SOCKETS))),
            client_senders: Arc::new(Mutex::new(HashMap::with_capacity(MAX_CLIENTS))),
            message_process_tx,
            message_process_rx: Some(message_process_rx),
            client_response_tx,
            client_response_rx: Some(client_response_rx),
            bind_addr,
            port,
            server_running: Arc::new(AtomicBool::new(false)),
            server_thread: None,
            message_distributor_thread: None,
        }
    }
    
    /// Get the message processing receiver (used by ServiceApplication)
    pub fn take_message_receiver(&mut self) -> Option<Receiver<RawMessageData>> {
        self.message_process_rx.take()
    }
    
    /// Get the client response sender (used by ServiceApplication)
    pub fn get_response_sender(&self) -> Sender<RawMessageData> {
        self.client_response_tx.clone()
    }
    
    /// Start the TCP server and message distributor
    pub fn start(&mut self, blocking: bool) -> Result<()> {
        self.server_running.store(true, Ordering::SeqCst);
        
        // Start message distributor thread
        let client_response_rx = self.client_response_rx.take().unwrap();
        let pool_for_distributor = self.clone_for_thread();
        let message_distributor_thread = thread::spawn(move || {
            if let Err(e) = pool_for_distributor.message_distributor(client_response_rx) {
                error!("Message distributor error: {}", e);
            }
        });
        
        if blocking {
            self.start_listening()
        } else {
            let pool_for_server = self.clone_for_thread();
            let server_thread = thread::spawn(move || {
                if let Err(e) = pool_for_server.start_listening() {
                    error!("Failed to start listening: {}", e);
                }
            });
            self.server_thread = Some(server_thread);
            self.message_distributor_thread = Some(message_distributor_thread);
            Ok(())
        }
    }
    
    /// Connect to a remote service
    pub fn connect(&self, ip_addr: IpAddr) -> Result<()> {
        debug!("Connecting to service at {:?}", ip_addr);

        let socket = TcpStream::connect(SocketAddr::new(ip_addr, self.port))
            .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", ip_addr, e))?;

        let pool = self.clone_for_thread();
        
        // Spawn client handler thread
        thread::spawn(move || {
            if let Err(e) = pool.handle_client(socket) {
                error!("Client handler error: {}", e);
            }
        });

        thread::sleep(Duration::from_millis(500)); // Wait for the client to be ready
        Ok(())
    }
    
    /// Check if we're connected to a specific IP address
    pub fn is_connected(&self, ip_addr: IpAddr) -> bool {
        let connected_sockets = self.connected_sockets.lock();
        connected_sockets.contains(&ip_addr)
    }
    
    /// Stop the TCP connection pool
    pub fn stop(&mut self) -> Result<()> {
        info!("Stopping TCP connection pool");
        
        // Signal threads to stop
        self.server_running.store(false, Ordering::SeqCst);
        
        // Join threads
        if let Some(server_thread) = self.server_thread.take() {
            if let Err(e) = server_thread.join() {
                error!("Server thread panicked: {:?}", e);
            }
        }
        
        if let Some(message_distributor_thread) = self.message_distributor_thread.take() {
            if let Err(e) = message_distributor_thread.join() {
                error!("Message distributor thread panicked: {:?}", e);
            }
        }
        
        // Clear all connections
        {
            let mut connected_sockets = self.connected_sockets.lock();
            connected_sockets.clear();
            
            let mut client_senders = self.client_senders.lock();
            client_senders.clear();
        }
        
        info!("TCP connection pool stopped");
        Ok(())
    }
    
    /// Clone the pool for use in threads (without the receivers)
    fn clone_for_thread(&self) -> Self {
        Self {
            connected_sockets: Arc::clone(&self.connected_sockets),
            client_senders: Arc::clone(&self.client_senders),
            message_process_tx: self.message_process_tx.clone(),
            message_process_rx: None,
            client_response_tx: self.client_response_tx.clone(),
            client_response_rx: None,
            bind_addr: self.bind_addr,
            port: self.port,
            server_running: Arc::clone(&self.server_running),
            server_thread: None,
            message_distributor_thread: None,
        }
    }
    
    /// Start listening for incoming TCP connections
    fn start_listening(&self) -> Result<()> {
        let listener = TcpListener::bind((self.bind_addr, self.port))
            .map_err(|e| anyhow::anyhow!("Failed to bind TCP listener to {}:{}: {}", 
                self.bind_addr, self.port, e))?;

        info!(
            "Listening for incoming connections on {}:{}",
            self.bind_addr, self.port
        );

        for stream in listener.incoming() {
            if !self.server_running.load(Ordering::SeqCst) {
                break;
            }

            match stream {
                Ok(socket) => {
                    let addr = socket.peer_addr().unwrap_or_else(|_| {
                        "unknown".parse::<SocketAddr>().unwrap()
                    });
                    info!("[Connected] {:?}", addr);

                    let pool = self.clone_for_thread();
                    thread::spawn(move || {
                        if let Err(e) = pool.handle_client(socket) {
                            error!("Client handler error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
        Ok(())
    }
    
    /// Handle incoming client connections and messages
    fn handle_client(&self, mut tcp_stream: TcpStream) -> Result<()> {
        let socket_addr = tcp_stream.peer_addr()?;
        info!("Handling client connection from {:?}", socket_addr);

        tcp_stream.set_nonblocking(true)?;

        let (this_client_tx, this_client_rx) = channel::unbounded::<ApplicationMessage>();
        
        // Register this client with capacity checks
        self.register_client(socket_addr.ip(), this_client_tx)?;

        let mut buffer: Vec<u8> = Vec::with_capacity(CLIENT_BUFFER_SIZE);
        let mut temp_buffer = [0u8; TEMP_BUFFER_SIZE];

        loop {
            if !self.server_running.load(Ordering::SeqCst) {
                break;
            }

            // Handle incoming data
            match tcp_stream.read(&mut temp_buffer) {
                Ok(0) => {
                    info!("[Disconnected] {:?}", socket_addr);
                    self.unregister_client(socket_addr.ip());
                    break;
                }
                Ok(bytes_read) => {
                    if let Err(e) = self.process_incoming_data(&mut buffer, &temp_buffer[..bytes_read], socket_addr.ip()) {
                        error!("Error processing incoming data: {}", e);
                        buffer.clear();
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No data available, check for outgoing messages
                }
                Err(e) => {
                    error!("Failed to read from socket: {}", e);
                    break;
                }
            }

            // Handle outgoing messages
            if let Err(e) = self.process_outgoing_messages(&mut tcp_stream, &this_client_rx, socket_addr) {
                error!("Error processing outgoing messages: {}", e);
                break;
            }

            thread::sleep(Duration::from_millis(SLEEP_INTERVAL_MS));
        }
        Ok(())
    }
    
    /// Register a new client connection
    fn register_client(&self, ip: IpAddr, sender: Sender<ApplicationMessage>) -> Result<()> {
        {
            let mut client_senders = self.client_senders.lock();
            if client_senders.len() >= MAX_CLIENTS {
                return Err(anyhow::anyhow!("Maximum client connections reached"));
            }
            client_senders.insert(ip, sender);
        }

        {
            let mut connected_sockets = self.connected_sockets.lock();
            if connected_sockets.len() >= MAX_SOCKETS {
                return Err(anyhow::anyhow!("Maximum socket connections reached"));
            }
            connected_sockets.insert(ip);
        }

        Ok(())
    }
    
    /// Unregister a client connection
    fn unregister_client(&self, ip: IpAddr) {
        let mut connected_sockets = self.connected_sockets.lock();
        connected_sockets.remove(&ip);

        let mut client_senders = self.client_senders.lock();
        client_senders.remove(&ip);
    }
    
    /// Process incoming data from a client
    fn process_incoming_data(&self, buffer: &mut Vec<u8>, data: &[u8], ip: IpAddr) -> Result<()> {
        buffer.extend_from_slice(data);
        
        match ApplicationMessage::from_bytes(buffer) {
            Ok(packet) => {
                self.message_process_tx.send((packet, ip))?;
                buffer.clear();
            }
            Err(_) => {
                // Packet might be incomplete, keep the data for next iteration
                // Clear buffer if it gets too large to prevent memory issues
                if buffer.len() > CLIENT_BUFFER_SIZE * 2 {
                    buffer.clear();
                    return Err(anyhow::anyhow!("Buffer overflow, clearing"));
                }
            }
        }
        Ok(())
    }
    
    /// Process outgoing messages to a client
    fn process_outgoing_messages(
        &self, 
        tcp_stream: &mut TcpStream, 
        client_rx: &Receiver<ApplicationMessage>,
        socket_addr: SocketAddr
    ) -> Result<()> {
        match client_rx.try_recv() {
            Ok(msg) => {
                trace!("Sending message to {:?}:{:?}", socket_addr, msg);
                let packet = ApplicationMessage::to_bytes(&msg)?;
                tcp_stream.write_all(&packet)?;
            }
            Err(TryRecvError::Empty) => {
                // No messages to send
            }
            Err(TryRecvError::Disconnected) => {
                return Err(anyhow::anyhow!("Client message channel disconnected"));
            }
        }
        Ok(())
    }
    
    /// Message distributor that forwards messages from the global response channel to client-specific channels
    fn message_distributor(&self, client_response_rx: Receiver<RawMessageData>) -> Result<()> {
        while self.server_running.load(Ordering::SeqCst) {
            match client_response_rx.recv_timeout(Duration::from_millis(READ_TIMEOUT_MS)) {
                Ok((msg, target_addr)) => {
                    let client_senders = self.client_senders.lock();
                    if let Some(client_sender) = client_senders.get(&target_addr) {
                        if let Err(e) = client_sender.send(msg) {
                            error!("Failed to send message to client {}: {}", target_addr, e);
                        }
                    } else {
                        debug!("No client found for address: {}", target_addr);
                    }
                }
                Err(channel::RecvTimeoutError::Timeout) => {
                    continue;
                }
                Err(channel::RecvTimeoutError::Disconnected) => {
                    info!("Message distributor channel disconnected");
                    break;
                }
            }
        }
        Ok(())
    }
}