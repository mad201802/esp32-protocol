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

use anyhow::Result;
use crossbeam::channel::{self, Receiver, Sender, TryRecvError};
use log::{debug, error, info, trace};
use parking_lot::Mutex;

use crate::application::{
    constants::{
        CHANNEL_CAPACITY, CONNECT_TIMEOUT_MS, DISTRIBUTOR_TIMEOUT_MS, MAX_BUFFER_GROWTH,
        MAX_CLIENTS_FIXED, POLL_INTERVAL_MS, TEMP_BUFFER_SIZE,
    },
    message::{ApplicationMessage, RawMessageData},
    pooling::{buffer_pool::BufferPool, client_registry::FixedClientRegistry},
};

/// TCP Connection Pool for managing client connections and message routing
/// Optimized for embedded devices with minimal heap allocations
pub struct TcpConnectionPool {
    /// Fixed-size client registry instead of dynamic HashMap/HashSet
    client_registry: Arc<Mutex<FixedClientRegistry>>,

    /// Buffer pool for reusing serialization buffers
    buffer_pool: Arc<BufferPool>,

    /// Channel for incoming messages from clients (received from TcpStream)
    message_process_tx: Sender<RawMessageData>,
    /// Receiver for incoming messages from clients (used by ServiceApplication)
    message_process_rx: Option<Receiver<RawMessageData>>,

    /// Channel for outgoing messages to clients (received from ServiceApplication)
    client_response_tx: Sender<RawMessageData>,
    /// Receiver for outgoing messages to clients (used by TcpStream handler)
    client_response_rx: Option<Receiver<RawMessageData>>,

    /// Server configuration
    bind_addr: IpAddr,
    port: u16,
    /// Maximum number of clients (now using fixed-size registry)
    max_clients: usize,

    /// Server control
    server_running: Arc<AtomicBool>,
    server_thread: Option<JoinHandle<()>>,
    message_distributor_thread: Option<JoinHandle<()>>,
}

impl TcpConnectionPool {
    /// Create a new TCP connection pool with optimized settings for embedded devices
    pub fn new(bind_addr: IpAddr, port: u16, _max_sockets: usize, max_clients: usize) -> Self {
        let (message_process_tx, message_process_rx) = channel::bounded(CHANNEL_CAPACITY);
        let (client_response_tx, client_response_rx) = channel::bounded(CHANNEL_CAPACITY);

        Self {
            client_registry: Arc::new(Mutex::new(FixedClientRegistry::new())),
            buffer_pool: Arc::new(BufferPool::new(4)), // Small buffer pool for embedded use
            message_process_tx,
            message_process_rx: Some(message_process_rx),
            client_response_tx,
            client_response_rx: Some(client_response_rx),
            bind_addr,
            port,
            max_clients: max_clients.min(MAX_CLIENTS_FIXED), // Ensure max_clients doesn't exceed our fixed array size
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

    /// Connect to a remote service with optimized timeout for embedded devices
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
        info!("Stopping TCP connection pool");

        // Signal threads to stop
        self.server_running.store(false, Ordering::SeqCst);

        // Join threads
        if let Some(server_thread) = self.server_thread.take()
            && let Err(e) = server_thread.join()
        {
            error!("Server thread panicked: {:?}", e);
        }

        if let Some(message_distributor_thread) = self.message_distributor_thread.take()
            && let Err(e) = message_distributor_thread.join()
        {
            error!("Message distributor thread panicked: {:?}", e);
        }

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
            buffer_pool: Arc::clone(&self.buffer_pool),
            message_process_tx: self.message_process_tx.clone(),
            message_process_rx: None,
            client_response_tx: self.client_response_tx.clone(),
            client_response_rx: None,
            bind_addr: self.bind_addr,
            port: self.port,
            max_clients: self.max_clients,
            server_running: Arc::clone(&self.server_running),
            server_thread: None,
            message_distributor_thread: None,
        }
    }

    /// Start listening for incoming TCP connections
    fn start_listening(&self) -> Result<()> {
        let listener = TcpListener::bind((self.bind_addr, self.port)).map_err(|e| {
            anyhow::anyhow!(
                "Failed to bind TCP listener to {}:{}: {}",
                self.bind_addr,
                self.port,
                e
            )
        })?;

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

        let (this_client_tx, this_client_rx) = channel::unbounded::<ApplicationMessage>();

        // Register this client with capacity checks
        self.register_client(socket_addr.ip(), this_client_tx)?;

        // Use fixed-size arrays to avoid heap allocations entirely
        let mut buffer = [0u8; MAX_BUFFER_GROWTH];
        let mut buffer_len = 0;
        let mut temp_buffer = [0u8; TEMP_BUFFER_SIZE];
        let mut consecutive_empty_reads = 0u8;

        loop {
            if !self.server_running.load(Ordering::SeqCst) {
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
                    if let Err(e) = self.process_incoming_data_fixed(
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
            let sleep_duration = if had_activity || consecutive_empty_reads < 10 {
                Duration::from_millis(POLL_INTERVAL_MS)
            } else {
                Duration::from_millis(POLL_INTERVAL_MS * 2) // Double sleep time when inactive
            };

            thread::sleep(sleep_duration);
        }
        Ok(())
    }

    /// Register a new client connection
    fn register_client(&self, ip: IpAddr, sender: Sender<ApplicationMessage>) -> Result<()> {
        let mut client_registry = self.client_registry.lock();
        if client_registry.len() >= self.max_clients {
            return Err(anyhow::anyhow!("Maximum client connections reached"));
        }
        client_registry.add_client(ip, sender)
    }

    /// Unregister a client connection
    fn unregister_client(&self, ip: IpAddr) {
        let mut client_registry = self.client_registry.lock();
        client_registry.remove_client(ip);
    }

    /// Process incoming data from a client using fixed-size buffers to avoid heap allocations
    fn process_incoming_data_fixed(
        &self,
        buffer: &mut [u8; MAX_BUFFER_GROWTH],
        buffer_len: &mut usize,
        data: &[u8],
        ip: IpAddr,
    ) -> Result<()> {
        // Check if we have space for new data
        if *buffer_len + data.len() > MAX_BUFFER_GROWTH {
            *buffer_len = 0; // Reset buffer
            return Err(anyhow::anyhow!(
                "Buffer overflow protection triggered, clearing"
            ));
        }

        // Copy new data into our fixed buffer
        buffer[*buffer_len..*buffer_len + data.len()].copy_from_slice(data);
        *buffer_len += data.len();

        // Try to parse a complete message
        match ApplicationMessage::from_bytes(&buffer[..*buffer_len]) {
            Ok(packet) => {
                // Send the message and reset buffer
                self.message_process_tx.send((packet, ip))?;
                *buffer_len = 0;
            }
            Err(_) => {
                // Packet might be incomplete, keep the data for next iteration
                // The fixed buffer size already provides overflow protection
            }
        }
        Ok(())
    }

    /// Process outgoing messages to a client with buffer pool optimization
    fn process_outgoing_messages(
        &self,
        tcp_stream: &mut TcpStream,
        client_rx: &Receiver<ApplicationMessage>,
        socket_addr: SocketAddr,
    ) -> Result<()> {
        match client_rx.try_recv() {
            Ok(msg) => {
                trace!("Sending message to {:?}:{:?}", socket_addr, msg);

                // Use buffer pool for serialization to avoid heap allocation
                let mut packet_data = self.buffer_pool.get_buffer();
                packet_data.clear();

                // Serialize directly into our pooled buffer
                let serialized_msg = ApplicationMessage::to_bytes(&msg)?;
                packet_data.extend_from_slice(&serialized_msg);

                // Handle partial writes for better reliability on embedded systems
                let mut total_written = 0;
                while total_written < packet_data.len() {
                    match tcp_stream.write(&packet_data[total_written..]) {
                        Ok(bytes_written) => {
                            total_written += bytes_written;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            // Socket not ready, try again on next iteration
                            break;
                        }
                        Err(e) => {
                            // Return buffer to pool before error
                            self.buffer_pool.return_buffer(packet_data);
                            return Err(anyhow::anyhow!("Failed to write to socket: {}", e));
                        }
                    }
                }

                // Return buffer to pool for reuse
                self.buffer_pool.return_buffer(packet_data);

                // Flush the stream to ensure data is sent
                tcp_stream.flush().ok(); // Ignore flush errors as they're not critical
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

    /// Message distributor with optimized timeout for embedded systems
    fn message_distributor(&self, client_response_rx: Receiver<RawMessageData>) -> Result<()> {
        // Use shorter timeout for better responsiveness on embedded systems
        let timeout = Duration::from_millis(DISTRIBUTOR_TIMEOUT_MS);

        while self.server_running.load(Ordering::SeqCst) {
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
                Err(channel::RecvTimeoutError::Timeout) => {
                    // Timeout is expected, continue processing
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
