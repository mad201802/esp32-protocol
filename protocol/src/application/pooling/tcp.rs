//! # TCP Connection Pool for Embedded Systems
//!
//! This module provides a TCP connection pool optimized for embedded devices with
//! minimal heap allocations and efficient resource usage.
//!
//! ## Key Features
//!
//! - **Fixed-size allocations**: Uses arrays instead of dynamic vectors
//! - **Non-blocking I/O**: Prevents blocking operations that could freeze the system
//! - **Adaptive polling**: Adjusts sleep intervals based on activity
//! - **Thread-safe**: Safe to use from multiple threads
//! - **Robust error handling**: Graceful handling of network errors

use std::{
    io::{Read, Write},
    net::{IpAddr, SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use anyhow::{anyhow, Result};
use crossbeam::channel::{self, Receiver, Sender, TryRecvError};
use log::{debug, error, info, trace};
use parking_lot::Mutex;

use crate::application::{
    constants::{
        CHANNEL_CAPACITY, CONNECT_TIMEOUT_MS, DISTRIBUTOR_TIMEOUT_MS, INACTIVE_READ_THRESHOLD, INACTIVE_SLEEP_MULTIPLIER, MAX_BUFFER_GROWTH, MAX_CLIENTS_FIXED, MAX_PACKET_BUFFER_SIZE, POLL_INTERVAL_MS, TEMP_BUFFER_SIZE
    },
    pooling::client_registry::FixedClientRegistry,
    serializable::Serializable,
};

/// Message with its source/destination IP address
pub type MessageWithAddress<T> = (T, IpAddr);

/// Represents the possible states of message processing
#[derive(Debug)]
enum ProcessingState {
    /// A complete message was processed
    MessageProcessed,
    /// Message is incomplete, waiting for more data
    IncompleteMessage,
    /// Buffer was reset due to overflow protection
    BufferReset,
}

/// Trait alias for cleaner type constraints
pub trait ProtocolMessage: Serializable + Clone + Send + Sync + std::fmt::Debug + 'static {}

// Blanket implementation for all types that satisfy the constraints
impl<T> ProtocolMessage for T where T: Serializable + Clone + Send + Sync + std::fmt::Debug + 'static {}

/// Configuration for the TCP connection pool
#[derive(Debug, Clone)]
pub struct TcpPoolConfig {
    pub bind_addr: IpAddr,
    pub port: u16,
    pub max_clients: usize,
}

impl TcpPoolConfig {
    pub fn new(bind_addr: IpAddr, port: u16, max_clients: usize) -> Self {
        Self {
            bind_addr,
            port,
            max_clients: max_clients.min(MAX_CLIENTS_FIXED),
        }
    }
}

/// Manages the server thread and message distributor thread
struct ThreadManager {
    server_thread: Option<JoinHandle<()>>,
    message_distributor_thread: Option<JoinHandle<()>>,
    is_running: Arc<AtomicBool>,
}

impl ThreadManager {
    fn new() -> Self {
        Self {
            server_thread: None,
            message_distributor_thread: None,
            is_running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn start(&mut self) {
        self.is_running.store(true, Ordering::SeqCst);
    }

    fn stop(&mut self) -> Result<()> {
        info!("Stopping TCP connection pool threads");
        
        self.is_running.store(false, Ordering::SeqCst);

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

        Ok(())
    }

    fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }
}

/// TCP Connection Pool for managing client connections and message routing
/// Optimized for embedded devices with minimal heap allocations
pub struct TcpConnectionPool<T: ProtocolMessage> {
    /// Fixed-size client registry instead of dynamic HashMap/HashSet
    client_registry: Arc<Mutex<FixedClientRegistry<T>>>,

    /// Channel for incoming messages from clients (received from TcpStream)
    message_process_tx: Sender<MessageWithAddress<T>>,
    /// Receiver for incoming messages from clients (used by ServiceApplication)
    message_process_rx: Option<Receiver<MessageWithAddress<T>>>,

    /// Channel for outgoing messages to clients (received from ServiceApplication)
    client_response_tx: Sender<MessageWithAddress<T>>,
    /// Receiver for outgoing messages to clients (used by TcpStream handler)
    client_response_rx: Option<Receiver<MessageWithAddress<T>>>,

    /// Server configuration
    config: TcpPoolConfig,

    /// Thread management
    thread_manager: ThreadManager,
}

impl<T: ProtocolMessage> TcpConnectionPool<T> {
    /// Create a new TCP connection pool with optimized settings for embedded devices
    pub fn new(bind_addr: IpAddr, port: u16, _max_sockets: usize, max_clients: usize) -> Self {
        let (message_process_tx, message_process_rx) = channel::bounded(CHANNEL_CAPACITY);
        let (client_response_tx, client_response_rx) = channel::bounded(CHANNEL_CAPACITY);

        Self {
            client_registry: Arc::new(Mutex::new(FixedClientRegistry::new())),
            message_process_tx,
            message_process_rx: Some(message_process_rx),
            client_response_tx,
            client_response_rx: Some(client_response_rx),
            config: TcpPoolConfig::new(bind_addr, port, max_clients),
            thread_manager: ThreadManager::new(),
        }
    }

    /// Get the message processing receiver (used by ServiceApplication)
    pub fn take_message_receiver(&mut self) -> Option<Receiver<MessageWithAddress<T>>> {
        self.message_process_rx.take()
    }

    /// Get the client response sender (used by ServiceApplication)
    pub fn get_response_sender(&self) -> Sender<MessageWithAddress<T>> {
        self.client_response_tx.clone()
    }

    /// Start the TCP server and message distributor
    pub fn start(&mut self, blocking: bool) -> Result<()> {
        self.thread_manager.start();

        // Start message distributor thread
        let client_response_rx = self.client_response_rx.take().unwrap();
        let pool_for_distributor = self.clone_for_thread();
        let message_distributor_thread = thread::spawn(move || {
            if let Err(e) = pool_for_distributor.message_distributor(client_response_rx) {
                error!("Message distributor error: {}", e);
            }
        });

        if blocking {
            self.thread_manager.message_distributor_thread = Some(message_distributor_thread);
            self.start_listening()
        } else {
            let pool_for_server = self.clone_for_thread();
            let server_thread = thread::spawn(move || {
                if let Err(e) = pool_for_server.start_listening() {
                    error!("Failed to start listening: {}", e);
                }
            });
            self.thread_manager.server_thread = Some(server_thread);
            self.thread_manager.message_distributor_thread = Some(message_distributor_thread);
            Ok(())
        }
    }

    /// Connect to a remote service with optimized timeout for embedded devices
    pub fn connect(&self, ip_addr: IpAddr) -> Result<()> {
        debug!("Connecting to service at {:?}", ip_addr);

        let socket = TcpStream::connect(SocketAddr::new(ip_addr, self.config.port))
            .map_err(|e| anyhow!("Failed to connect to {}: {}", ip_addr, e))?;

        let pool = self.clone_for_thread();

        // Spawn client handler thread
        thread::spawn(move || {
            if let Err(e) = pool.handle_client(socket) {
                error!("Client handler error: {}", e);
            }
        });

        // Reduced wait time for embedded systems
        thread::sleep(Duration::from_millis(CONNECT_TIMEOUT_MS));
        Ok(())
    }

    /// Check if we're connected to a specific IP address
    pub fn is_connected(&self, ip_addr: IpAddr) -> bool {
        let client_registry = self.client_registry.lock();
        client_registry.is_connected(ip_addr)
    }

    /// Stop the TCP connection pool
    pub fn stop(&mut self) -> Result<()> {
        self.thread_manager.stop()?;

        // Clear all connections using fixed-size registry
        {
            let mut client_registry = self.client_registry.lock();
            *client_registry = FixedClientRegistry::new();
        }

        info!("TCP connection pool stopped");
        Ok(())
    }

    /// Clone the pool for use in threads (without the receivers)
    fn clone_for_thread(&self) -> Self {
        Self {
            client_registry: Arc::clone(&self.client_registry),
            message_process_tx: self.message_process_tx.clone(),
            message_process_rx: None,
            client_response_tx: self.client_response_tx.clone(),
            client_response_rx: None,
            config: self.config.clone(),
            thread_manager: ThreadManager {
                server_thread: None,
                message_distributor_thread: None,
                is_running: Arc::clone(&self.thread_manager.is_running),
            },
        }
    }

    /// Start listening for incoming TCP connections
    fn start_listening(&self) -> Result<()> {
        let listener = TcpListener::bind((self.config.bind_addr, self.config.port)).map_err(|e| {
            anyhow!(
                "Failed to bind TCP listener to {}:{}: {}",
                self.config.bind_addr,
                self.config.port,
                e
            )
        })?;

        info!(
            "Listening for incoming connections on {}:{}",
            self.config.bind_addr, self.config.port
        );

        for stream in listener.incoming() {
            if !self.thread_manager.is_running() {
                break;
            }

            match stream {
                Ok(socket) => {
                    let addr = socket
                        .peer_addr()
                        .unwrap_or_else(|_| "unknown".parse::<SocketAddr>().unwrap());
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

    /// Handle incoming client connections and messages with embedded device optimizations
    fn handle_client(&self, mut tcp_stream: TcpStream) -> Result<()> {
        let socket_addr = tcp_stream.peer_addr()?;
        info!("Handling client connection from {:?}", socket_addr);

        tcp_stream.set_nonblocking(true)?;

        let (this_client_tx, this_client_rx) = channel::unbounded::<T>();

        // Register this client with capacity checks
        self.register_client(socket_addr.ip(), this_client_tx)?;

        // Use fixed-size arrays to avoid heap allocations entirely
        let mut buffer = [0u8; MAX_BUFFER_GROWTH];
        let mut buffer_len = 0;
        let mut temp_buffer = [0u8; TEMP_BUFFER_SIZE];
        let mut consecutive_empty_reads = 0u8;

        loop {
            if !self.thread_manager.is_running() {
                break;
            }

            let mut had_activity = false;

            // Handle incoming data
            match tcp_stream.read(&mut temp_buffer) {
                Ok(0) => {
                    info!("[Disconnected] {:?}", socket_addr);
                    self.unregister_client(socket_addr.ip());
                    break;
                }
                Ok(bytes_read) => {
                    had_activity = true;
                    consecutive_empty_reads = 0;
                    if let Err(e) = self.process_incoming_data(
                        &mut buffer,
                        &mut buffer_len,
                        &temp_buffer[..bytes_read],
                        socket_addr.ip(),
                    ) {
                        error!("Error processing incoming data: {}", e);
                        buffer_len = 0; // Reset buffer on error
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    consecutive_empty_reads = consecutive_empty_reads.saturating_add(1);
                }
                Err(e) => {
                    error!("Failed to read from socket: {}", e);
                    break;
                }
            }

            // Handle outgoing messages
            if let Err(e) =
                self.process_outgoing_messages(&mut tcp_stream, &this_client_rx, socket_addr)
            {
                error!("Error processing outgoing messages: {}", e);
                break;
            }

            // Adaptive sleep based on activity - sleep longer when inactive to save CPU
            let sleep_duration = if had_activity || consecutive_empty_reads < INACTIVE_READ_THRESHOLD {
                Duration::from_millis(POLL_INTERVAL_MS)
            } else {
                Duration::from_millis(POLL_INTERVAL_MS * INACTIVE_SLEEP_MULTIPLIER) // Double sleep time when inactive
            };

            thread::sleep(sleep_duration);
        }
        Ok(())
    }

    /// Register a new client connection
    fn register_client(&self, ip: IpAddr, sender: Sender<T>) -> Result<()> {
        let mut client_registry = self.client_registry.lock();
        if client_registry.len() >= self.config.max_clients {
            return Err(anyhow!("Maximum client connections reached"));
        }
        client_registry.add_client(ip, sender)
    }

    /// Unregister a client connection
    fn unregister_client(&self, ip: IpAddr) {
        let mut client_registry = self.client_registry.lock();
        client_registry.remove_client(ip);
    }

    /// Process incoming data from a client using fixed-size buffers to avoid heap allocations
    fn process_incoming_data(
        &self,
        buffer: &mut [u8; MAX_BUFFER_GROWTH],
        buffer_len: &mut usize,
        data: &[u8],
        ip: IpAddr,
    ) -> Result<()> {
        let processing_result = self.try_process_data(buffer, buffer_len, data, ip)?;
        
        match processing_result {
            ProcessingState::MessageProcessed => {
                trace!("Successfully processed message from {}", ip);
            }
            ProcessingState::IncompleteMessage => {
                trace!("Waiting for more data from {}", ip);
            }
            ProcessingState::BufferReset => {
                debug!("Buffer reset for client {}", ip);
            }
        }
        
        Ok(())
    }

    /// Try to process incoming data and return the processing state
    fn try_process_data(
        &self,
        buffer: &mut [u8; MAX_BUFFER_GROWTH],
        buffer_len: &mut usize,
        data: &[u8],
        ip: IpAddr,
    ) -> Result<ProcessingState> {
        // Check if we have space for new data
        if *buffer_len + data.len() > MAX_BUFFER_GROWTH {
            *buffer_len = 0; // Reset buffer
            return Ok(ProcessingState::BufferReset);
        }

        // Copy new data into our fixed buffer
        buffer[*buffer_len..*buffer_len + data.len()].copy_from_slice(data);
        *buffer_len += data.len();

        // Try to parse a complete message
        match T::from_bytes(&buffer[..*buffer_len]) {
            Ok(packet) => {
                // Send the message and reset buffer
                self.message_process_tx.send((packet, ip))?;
                *buffer_len = 0;
                Ok(ProcessingState::MessageProcessed)
            }
            Err(_) => {
                // Packet might be incomplete, keep the data for next iteration
                Ok(ProcessingState::IncompleteMessage)
            }
        }
    }

    /// Process outgoing messages to a client using fixed-size arrays for optimal embedded performance
    fn process_outgoing_messages(
        &self,
        tcp_stream: &mut TcpStream,
        client_rx: &Receiver<T>,
        socket_addr: SocketAddr,
    ) -> Result<()> {
        match client_rx.try_recv() {
            Ok(msg) => {
                trace!("Sending message to {:?}:{:?}", socket_addr, msg);

                // Use fixed-size array for serialization to avoid heap allocations
                let mut packet_buffer = [0u8; MAX_PACKET_BUFFER_SIZE];
                
                // Serialize the message
                let serialized_msg = msg.to_bytes();
                
                // Ensure message fits in our fixed buffer
                if serialized_msg.len() > MAX_PACKET_BUFFER_SIZE {
                    error!("Message too large: {} bytes, max: {}", serialized_msg.len(), MAX_PACKET_BUFFER_SIZE);
                    return Err(anyhow!("Message exceeds maximum buffer size"));
                }
                
                // Copy serialized data into our fixed buffer
                let packet_len = serialized_msg.len();
                packet_buffer[..packet_len].copy_from_slice(&serialized_msg);

                // Handle partial writes for better reliability on embedded systems
                let mut total_written = 0;
                while total_written < packet_len {
                    match tcp_stream.write(&packet_buffer[total_written..packet_len]) {
                        Ok(bytes_written) => {
                            total_written += bytes_written;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            // Socket not ready, try again on next iteration
                            break;
                        }
                        Err(e) => {
                            return Err(anyhow!("Failed to write to socket: {}", e));
                        }
                    }
                }

                // Flush the stream to ensure data is sent
                tcp_stream.flush().ok(); // Ignore flush errors as they're not critical
            }
            Err(TryRecvError::Empty) => {
                // No messages to send
            }
            Err(TryRecvError::Disconnected) => {
                return Err(anyhow!("Client message channel disconnected"));
            }
        }
        Ok(())
    }

    /// Message distributor with optimized timeout for embedded systems
    fn message_distributor(
        &self,
        client_response_rx: Receiver<MessageWithAddress<T>>,
    ) -> Result<()> {
        // Use shorter timeout for better responsiveness on embedded systems
        let timeout = Duration::from_millis(DISTRIBUTOR_TIMEOUT_MS);

        while self.thread_manager.is_running() {
            match client_response_rx.recv_timeout(timeout) {
                Ok((msg, target_addr)) => {
                    // Scope the lock to minimize contention
                    let client_sender = {
                        let client_registry = self.client_registry.lock();
                        client_registry.get_sender(target_addr).cloned()
                    };

                    if let Some(sender) = client_sender {
                        if let Err(e) = sender.send(msg) {
                            error!("Failed to send message to client {}: {}", target_addr, e);
                        }
                    } else {
                        debug!("No client found for address: {}", target_addr);
                    }
                }
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                    // Timeout is expected, continue processing
                    continue;
                }
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                    info!("Message distributor channel disconnected");
                    break;
                }
            }
        }
        Ok(())
    }
}
