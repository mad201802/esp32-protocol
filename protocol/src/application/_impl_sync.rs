use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
    net::{IpAddr, SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::Result;
use log::{debug, error, info, trace};
use parking_lot::Mutex;
use crossbeam::channel::{self, Receiver, Sender, TryRecvError};

use crate::{
    application::packets::ApplicationResponseErrorMessage, 
    sd::ServiceDiscovery,
    utils::retry_with_delay_option_sync,
};

use super::{
    config::ServiceApplicationConfig,
    packets::{
        ApplicationMessage, ApplicationMessageReturnCode, ApplicationMessageType,
        MethodInvokeCallback, MethodResponseCallback, OnEventInvokeCallback, RawMessageData,
    },
};

// Type alias for client-specific message channels
type ClientMessageSender = Sender<ApplicationMessage>;

pub struct ServiceApplication {
    service_id: u16,
    config: ServiceApplicationConfig,
    service_discovery: Option<ServiceDiscovery>,

    connected_sockets: Arc<Mutex<HashSet<IpAddr>>>,
    
    // Key: IP Address, Value: Sender for that client
    client_senders: Arc<Mutex<HashMap<IpAddr, ClientMessageSender>>>,

    // Key: Method ID, Value: Callback
    offered_methods: HashMap<u16, MethodInvokeCallback>,

    // Key: Event ID, Value: Set of subscribers
    offered_events: Arc<Mutex<HashMap<u16, HashSet<IpAddr>>>>,
    subscribed_events: Arc<Mutex<HashMap<u16, OnEventInvokeCallback>>>,

    // Key: Request ID, Value: Callback
    open_requests: Arc<Mutex<HashMap<u16, MethodResponseCallback>>>,

    message_process_tx: Sender<RawMessageData>,
    message_process_rx: Option<Receiver<RawMessageData>>,
    client_response_tx: Sender<RawMessageData>,
    client_response_rx: Option<Receiver<RawMessageData>>,

    // Server thread handle
    server_thread: Option<JoinHandle<()>>,
    message_handler_thread: Option<JoinHandle<()>>,
    message_distributor_thread: Option<JoinHandle<()>>,
    server_running: Arc<AtomicBool>,
}

impl Clone for ServiceApplication {
    fn clone(&self) -> Self {
        Self {
            service_id: self.service_id,
            config: self.config.clone(),
            service_discovery: None, // Don't clone ServiceDiscovery as it contains non-cloneable types
            connected_sockets: Arc::clone(&self.connected_sockets),
            client_senders: Arc::clone(&self.client_senders),
            offered_methods: self.offered_methods.clone(), // Clone methods for handlers to access them
            offered_events: Arc::clone(&self.offered_events),
            subscribed_events: Arc::clone(&self.subscribed_events),
            open_requests: Arc::clone(&self.open_requests),
            message_process_tx: self.message_process_tx.clone(),
            message_process_rx: None, // Don't clone receivers to avoid conflicts
            client_response_tx: self.client_response_tx.clone(),
            client_response_rx: None, // Don't clone receivers to avoid conflicts
            server_thread: None,
            message_handler_thread: None,
            message_distributor_thread: None,
            server_running: Arc::clone(&self.server_running),
        }
    }
}

impl ServiceApplication {
    pub fn new(service_id: u16) -> Self {
        Self::with_config(service_id, ServiceApplicationConfig::default())
    }

    pub fn with_config(service_id: u16, config: ServiceApplicationConfig) -> Self {
        info!("Creating new service application with ID: {}", service_id);

        let (message_process_tx, message_process_rx) = channel::bounded(32); // Use smaller bounded channel for ESP32
        let (client_response_tx, client_response_rx) = channel::bounded(32); // Use smaller bounded channel for ESP32

        Self {
            service_id,
            config,
            service_discovery: None,

            connected_sockets: Arc::new(Mutex::new(HashSet::with_capacity(8))), // Pre-allocate for ESP32
            client_senders: Arc::new(Mutex::new(HashMap::with_capacity(8))), // Pre-allocate for ESP32

            offered_events: Arc::new(Mutex::new(HashMap::with_capacity(8))), // Pre-allocate for ESP32
            subscribed_events: Arc::new(Mutex::new(HashMap::with_capacity(8))), // Pre-allocate for ESP32
            offered_methods: HashMap::with_capacity(8), // Pre-allocate for ESP32
            open_requests: Arc::new(Mutex::new(HashMap::with_capacity(16))), // Pre-allocate for ESP32

            message_process_tx,
            message_process_rx: Some(message_process_rx),
            client_response_tx,
            client_response_rx: Some(client_response_rx),

            server_thread: None,
            message_handler_thread: None,
            message_distributor_thread: None,
            server_running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Initialize the application (service discovery, etc.)
    pub fn init(&mut self) -> Result<()> {
        // Initialize the service discovery component
        let mut service_discovery = ServiceDiscovery::new(self.service_id);
        service_discovery.init()?;
        self.service_discovery = Some(service_discovery);

        Ok(())
    }

    /// Offer a method to the service
    pub fn offer_method(&mut self, method_id: u16, callback: MethodInvokeCallback) {
        self.offered_methods.insert(method_id, callback);
        trace!("Method {} offered", method_id);
    }

    /// Offer a event to the service
    pub fn offer_event(&mut self, event_id: u16) {
        self.offered_events
            .lock()
            .insert(event_id, HashSet::new());
        trace!("Event {} offered", event_id);
    }

    // Notifying all subscribing clients of an event
    pub fn notify(&self, event_id: u16, payload: Vec<u8>) {
        let offered_events = self.offered_events.lock();
        if let Some(clients_to_offer) = offered_events.get(&event_id) {
            for client_ip in clients_to_offer {
                let mut notification_packet = ApplicationMessage::new(
                    self.service_id,
                    event_id,
                    Some(0),
                    ApplicationMessageType::Notification,
                    ApplicationMessageReturnCode::Ok,
                    vec![],
                );
                notification_packet.set_payload(payload.clone());

                debug!("Notifying event {} to {:?}", event_id, client_ip);

                if let Err(e) = self.client_response_tx.send((notification_packet, *client_ip)) {
                    error!("Failed to send notification: {}", e);
                }
            }
        }
    }

    /// Connect to a service via ip and port
    fn connect(&self, ip_addr: IpAddr, port: u16) -> Result<()> {
        debug!("Connecting to service at {:?}", ip_addr);

        let socket = TcpStream::connect(SocketAddr::new(ip_addr, port))
            .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", ip_addr, e))?;

        let server = Arc::new(self.clone());
        
        // Spawn client handler thread
        thread::spawn(move || {
            if let Err(e) = server.handle_client(socket) {
                error!("Client handler error: {}", e);
            }
        });

        thread::sleep(Duration::from_millis(500)); // Wait for the client to be ready
        Ok(())
    }

    fn service_to_ip(&mut self, service_id: u16) -> Option<IpAddr> {
        // Check if we can get the ip address of the service

        trace!("Finding service with ID: {}", service_id);

        let service_discovery = self.service_discovery.as_mut().unwrap();
        let ip_addr = retry_with_delay_option_sync(
            || service_discovery.find_service(service_id),
            4,
            500,
        );

        // If we dont have the ip, return None
        if ip_addr.is_none() {
            error!("Could not find service with ID: {}", service_id);
            return None;
        }

        trace!("Found service with ID: {} at {:?}", service_id, ip_addr);

        // If we have the ip, check if we have a open socket
        let ip_addr = ip_addr.unwrap();

        {
            let connected_sockets = self.connected_sockets.lock();
            if connected_sockets.contains(&ip_addr) {
                return Some(ip_addr);
            }
        }

        // If we dont have a open socket, connect to the service
        if let Err(e) = self.connect(ip_addr, self.config.port) {
            error!("Failed to connect to service {}: {}", service_id, e);
            return None;
        }

        // Return the ip address of the service
        trace!(
            "Connected to service with ID: {} at {:?}",
            service_id, ip_addr
        );
        Some(ip_addr)
    }

    pub fn call_method(
        &mut self,
        service_id: u16,
        method_id: u16,
        payload: Vec<u8>,
        callback: MethodResponseCallback,
    ) {
        debug!("Calling method {} on {:?}", method_id, service_id);

        let ip_addr = self.service_to_ip(service_id);
        if ip_addr.is_none() {
            error!("Could not find socket for service with ID: {}", service_id);
            return;
        }

        let request_packet = ApplicationMessage::new(
            self.service_id,
            method_id,
            None,
            ApplicationMessageType::Request,
            ApplicationMessageReturnCode::Ok,
            payload,
        );

        let request_id = request_packet.request_id;
        match self
            .client_response_tx
            .send((request_packet, ip_addr.unwrap()))
        {
            Ok(_) => {
                let mut open_requests = self.open_requests.lock();
                open_requests.insert(request_id, callback);
            }
            Err(err) => {
                error!("Failed to send request packet: {:?}", err);
            }
        }
    }

    pub fn subscribe(
        &mut self,
        service_id: u16,
        event_id: u16,
        callback: OnEventInvokeCallback,
    ) {
        debug!("Subscribing to event {}", event_id);

        let ip_addr = self.service_to_ip(service_id);
        if ip_addr.is_none() {
            error!("Could not find socket for service with ID: {}", service_id);
            return;
        }

        let ip_addr = ip_addr.unwrap();

        // Retry mechanism for subscription
        let subscription_successful = retry_with_delay_option_sync(
            || {
                let subscribe_packet = ApplicationMessage::new(
                    self.service_id,
                    event_id,
                    None,
                    ApplicationMessageType::Subscribe,
                    ApplicationMessageReturnCode::Ok,
                    vec![],
                );

                let request_id = subscribe_packet.request_id;
                
                // Send the subscription request
                match self.client_response_tx.send((subscribe_packet, ip_addr)) {
                    Ok(_) => {
                        debug!("Sent subscription request for event {} with request_id {}", event_id, request_id);
                        
                        // Create a simple channel to wait for the response
                        let (response_tx, response_rx) = channel::bounded(1);
                        
                        // Store the response sender in open_requests
                        {
                            let mut open_requests = self.open_requests.lock();
                            open_requests.insert(request_id, Arc::new(move |result| {
                                let _ = response_tx.try_send(result);
                                Ok(vec![])
                            }));
                        }
                        
                        // Wait for response with timeout
                        let timeout_duration = Duration::from_millis(1000);
                        let start_time = Instant::now();
                        
                        loop {
                            if start_time.elapsed() > timeout_duration {
                                debug!("Subscription timeout for event {}, will retry", event_id);
                                // Clean up the request from open_requests
                                let mut open_requests = self.open_requests.lock();
                                open_requests.remove(&request_id);
                                return None;
                            }
                            
                            match response_rx.try_recv() {
                                Ok(Ok(_)) => {
                                    debug!("Subscription confirmed for event {}", event_id);
                                    return Some(());
                                }
                                Ok(Err(err)) => {
                                    error!("Subscription failed for event {}: {:?}", event_id, err);
                                    return None;
                                }
                                Err(TryRecvError::Empty) => {
                                    thread::sleep(Duration::from_millis(10));
                                    continue;
                                }
                                Err(TryRecvError::Disconnected) => {
                                    error!("Subscription channel disconnected for event {}", event_id);
                                    return None;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        error!("Failed to send subscribe packet: {:?}", err);
                        None
                    }
                }
            },
            4,
            500,
        );

        if subscription_successful.is_some() {
            // Only add to subscribed events after successful confirmation
            let mut subscribed_events = self.subscribed_events.lock();
            subscribed_events.insert(event_id, callback);
            debug!("Successfully subscribed to event {}", event_id);
        } else {
            error!("Failed to subscribe to event {} after retries", event_id);
        }
    }

    fn handle_message_data_with_rx(&self, message_process_rx: Receiver<RawMessageData>) -> Result<()> {
        while self.server_running.load(Ordering::SeqCst) {
            match message_process_rx.recv_timeout(Duration::from_millis(100)) {
                Ok((packet, addr)) => {
                    let mut response_packet = ApplicationMessage::new(
                        packet.service_id,
                        packet.method_id,
                        Some(packet.request_id),
                        ApplicationMessageType::Response,
                        ApplicationMessageReturnCode::Ok,
                        vec![],
                    );

                    match packet.message_type {
                        ApplicationMessageType::Request => {
                            debug!("Received request from {:?}", addr);

                            if let Some(callback) = self.offered_methods.get(&packet.method_id) {
                                let result = callback(packet.payload);
                                response_packet = match result {
                                    Ok(response_data) => {
                                        response_packet.return_code = ApplicationMessageReturnCode::Ok;
                                        response_packet.set_payload(response_data);
                                        response_packet
                                    }
                                    Err(err) => {
                                        error!("Error processing request: {:?}", err);
                                        response_packet.return_code = ApplicationMessageReturnCode::Error;
                                        response_packet.set_payload(err.to_bytes());
                                        response_packet
                                    }
                                };
                            }

                            if let Err(e) = self.client_response_tx.send((response_packet, addr)) {
                                error!("Failed to send response: {}", e);
                            }
                        }
                        ApplicationMessageType::Response => {
                            trace!("Received Response Message: {:?}", packet);
                            let mut open_requests = self.open_requests.lock();
                            if let Some(request_callback) = open_requests.remove(&packet.request_id) {
                                if packet.return_code == ApplicationMessageReturnCode::Ok {
                                    let _result = request_callback(Ok(packet.payload));
                                } else {
                                    let error_message =
                                        ApplicationResponseErrorMessage::from_bytes(&packet.payload)
                                            .unwrap();
                                    let _result = request_callback(Err(error_message));
                                }
                            }
                        }
                        ApplicationMessageType::Notification => {
                            trace!("Received Notification Message: {:?}", packet);
                            let subscribed_events = self.subscribed_events.lock();
                            if let Some(callback) = subscribed_events.get(&packet.method_id) {
                                callback(packet.payload);
                            }
                        }
                        ApplicationMessageType::Subscribe => {
                            // Handle Event Subscriptions
                            let mut offered_events = self.offered_events.lock();
                            if let Some(offered_events_clients) = offered_events.get_mut(&packet.method_id)
                            {
                                println!("!!! New event subscription: {:?}", packet.method_id);

                                // Add the client to the event
                                offered_events_clients.insert(addr);
                                
                                // Send a success response back to the client
                                let response_packet = ApplicationMessage::new(
                                    packet.service_id,
                                    packet.method_id,
                                    Some(packet.request_id),
                                    ApplicationMessageType::Response,
                                    ApplicationMessageReturnCode::Ok,
                                    vec![],
                                );
                                
                                if let Err(e) = self.client_response_tx.send((response_packet, addr)) {
                                    error!("Failed to send subscription response: {}", e);
                                }
                            } else {
                                error!("Event {} not offered, rejecting subscription from {:?}", packet.method_id, addr);
                                
                                // Send an error response back to the client
                                let error_message = ApplicationResponseErrorMessage::new(
                                    0x02,
                                    format!("Event {} not offered", packet.method_id),
                                );
                                
                                let response_packet = ApplicationMessage::new(
                                    packet.service_id,
                                    packet.method_id,
                                    Some(packet.request_id),
                                    ApplicationMessageType::Response,
                                    ApplicationMessageReturnCode::Error,
                                    error_message.to_bytes(),
                                );
                                
                                if let Err(e) = self.client_response_tx.send((response_packet, addr)) {
                                    error!("Failed to send error response: {}", e);
                                }
                            }
                        }
                        ApplicationMessageType::Unsubscribe => {
                            trace!("Received Unsubscribe Message: {:?}", packet);
                            let mut offered_events = self.offered_events.lock();
                            if let Some(connected_clients) = offered_events.get_mut(&packet.method_id) {
                                connected_clients.remove(&addr);
                            }
                        }
                        ApplicationMessageType::SDFindService
                        | ApplicationMessageType::SDOfferService
                        | ApplicationMessageType::SDStopOfferService
                        | ApplicationMessageType::INVALID => {
                            error!(
                                "Did not expect the following message: {:?}",
                                packet.message_type
                            );
                        }
                    }
                },
                Err(channel::RecvTimeoutError::Timeout) => {
                    // Continue the loop on timeout
                    continue;
                },
                Err(channel::RecvTimeoutError::Disconnected) => {
                    info!("Message processing channel disconnected");
                    break;
                }
            }
        }
        Ok(())
    }

    /// Handle incoming client connections and messages
    fn handle_client(
        &self,
        mut tcp_stream: TcpStream,
    ) -> Result<()> {
        let socket_addr = tcp_stream.peer_addr()?;
        info!("Handling client connection from {:?}", socket_addr);

        // Set non-blocking mode for the stream
        tcp_stream.set_nonblocking(true)?;

        // Create a channel for this specific client's outgoing messages
        let (this_client_tx, this_client_rx) = channel::unbounded::<ApplicationMessage>();
        
        // Register this client's sender
        {
            let mut client_senders = self.client_senders.lock();
            client_senders.insert(socket_addr.ip(), this_client_tx);
        }

        {
            let mut connected_sockets = self.connected_sockets.lock();
            connected_sockets.insert(socket_addr.ip());
        }

        let mut buffer: Vec<u8> = Vec::with_capacity(512); // Pre-allocate with smaller capacity for ESP32
        let mut temp_buffer = [0u8; 512]; // Reduce buffer size for ESP32

        loop {
            if !self.server_running.load(Ordering::SeqCst) {
                break;
            }

            // Try to read from socket
            match tcp_stream.read(&mut temp_buffer) {
                Ok(0) => {
                    info!("[Disconnected] {:?}", socket_addr);
                    {
                        let mut connected_sockets = self.connected_sockets.lock();
                        connected_sockets.remove(&socket_addr.ip());

                        let mut client_senders = self.client_senders.lock();
                        client_senders.remove(&socket_addr.ip());

                        let mut offered_events = self.offered_events.lock();
                        for (_, connected_clients) in offered_events.iter_mut() {
                            connected_clients.remove(&socket_addr.ip());
                        }
                    }
                    break;
                }
                Ok(bytes_read) => {
                    buffer.extend_from_slice(&temp_buffer[..bytes_read]);
                    
                    match ApplicationMessage::from_bytes(&buffer.clone()) {
                        Ok(packet) => {
                            if let Err(e) = self.message_process_tx.send((packet, socket_addr.ip())) {
                                error!("Failed to send message to processing queue: {}", e);
                                break;
                            }
                            buffer.clear();
                        },
                        Err(err) => {
                            error!("Error deserializing packet: {:?}", err);
                            buffer.clear();
                        }
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

            // Check for messages to send to this specific client
            match this_client_rx.try_recv() {
                Ok(msg) => {
                    trace!("Sending message to {:?}:{:?}", socket_addr, msg);

                    match ApplicationMessage::to_bytes(&msg) {
                        Ok(packet) => {
                            if let Err(e) = tcp_stream.write_all(&packet) {
                                error!("Failed to write to socket: {}", e);
                                break;
                            }
                        },
                        Err(err) => {
                            error!("Error serializing packet: {:?}", err);
                        }
                    }
                },
                Err(TryRecvError::Empty) => {
                    // No messages to send
                },
                Err(TryRecvError::Disconnected) => {
                    info!("Client message channel disconnected");
                    break;
                }
            }

            // Sleep briefly to prevent busy loop
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    /// Static method to handle incoming client connections without cloning the entire struct
    fn handle_client_static(
        mut tcp_stream: TcpStream,
        connected_sockets: Arc<Mutex<HashSet<IpAddr>>>,
        client_senders: Arc<Mutex<HashMap<IpAddr, ClientMessageSender>>>,
        offered_events: Arc<Mutex<HashMap<u16, HashSet<IpAddr>>>>,
        message_process_tx: Sender<RawMessageData>,
        server_running: Arc<AtomicBool>,
    ) -> Result<()> {
        let socket_addr = tcp_stream.peer_addr()?;
        info!("Handling client connection from {:?}", socket_addr);

        // Set non-blocking mode for the stream
        tcp_stream.set_nonblocking(true)?;

        // Create a channel for this specific client's outgoing messages
        let (this_client_tx, this_client_rx) = channel::unbounded::<ApplicationMessage>();
        
        // Register this client's sender with error handling
        {
            let mut client_senders_guard = client_senders.lock();
            if client_senders_guard.len() >= 8 {
                error!("Maximum client connections reached, rejecting new connection");
                return Err(anyhow::anyhow!("Maximum client connections reached"));
            }
            client_senders_guard.insert(socket_addr.ip(), this_client_tx);
        }

        {
            let mut connected_sockets_guard = connected_sockets.lock();
            if connected_sockets_guard.len() >= 8 {
                error!("Maximum socket connections reached");
                return Err(anyhow::anyhow!("Maximum socket connections reached"));
            }
            connected_sockets_guard.insert(socket_addr.ip());
        }

        let mut buffer: Vec<u8> = Vec::with_capacity(256); // Smaller capacity for ESP32
        let mut temp_buffer = [0u8; 256]; // Smaller buffer size for ESP32

        loop {
            if !server_running.load(Ordering::SeqCst) {
                break;
            }

            // Try to read from socket
            match tcp_stream.read(&mut temp_buffer) {
                Ok(0) => {
                    info!("[Disconnected] {:?}", socket_addr);
                    {
                        let mut connected_sockets_guard = connected_sockets.lock();
                        connected_sockets_guard.remove(&socket_addr.ip());

                        let mut client_senders_guard = client_senders.lock();
                        client_senders_guard.remove(&socket_addr.ip());

                        let mut offered_events_guard = offered_events.lock();
                        for (_, connected_clients) in offered_events_guard.iter_mut() {
                            connected_clients.remove(&socket_addr.ip());
                        }
                    }
                    break;
                }
                Ok(bytes_read) => {
                    buffer.extend_from_slice(&temp_buffer[..bytes_read]);
                    
                    match ApplicationMessage::from_bytes(&buffer.clone()) {
                        Ok(packet) => {
                            if let Err(e) = message_process_tx.send((packet, socket_addr.ip())) {
                                error!("Failed to send message to processing queue: {}", e);
                                break;
                            }
                            buffer.clear();
                        },
                        Err(err) => {
                            error!("Error deserializing packet: {:?}", err);
                            buffer.clear();
                        }
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

            // Check for messages to send to this specific client
            match this_client_rx.try_recv() {
                Ok(msg) => {
                    trace!("Sending message to {:?}:{:?}", socket_addr, msg);

                    match ApplicationMessage::to_bytes(&msg) {
                        Ok(packet) => {
                            if let Err(e) = tcp_stream.write_all(&packet) {
                                error!("Failed to write to socket: {}", e);
                                break;
                            }
                        },
                        Err(err) => {
                            error!("Error serializing packet: {:?}", err);
                        }
                    }
                },
                Err(TryRecvError::Empty) => {
                    // No messages to send
                },
                Err(TryRecvError::Disconnected) => {
                    info!("Client message channel disconnected");
                    break;
                }
            }

            // Sleep briefly to prevent busy loop
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    fn start_listening(&self) -> Result<()> {
        let listener = TcpListener::bind((self.config.bind_addr, self.config.port))
            .map_err(|e| anyhow::anyhow!("Failed to bind TCP listener to {}:{}: {}", 
                self.config.bind_addr, self.config.port, e))?;

        info!(
            "Listening for incoming connections on {}:{}",
            self.config.bind_addr, self.config.port
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

                    // Instead of cloning the entire struct, only clone what we need
                    let connected_sockets = Arc::clone(&self.connected_sockets);
                    let client_senders = Arc::clone(&self.client_senders);
                    let offered_events = Arc::clone(&self.offered_events);
                    let message_process_tx = self.message_process_tx.clone();
                    let server_running = Arc::clone(&self.server_running);
                    
                    thread::spawn(move || {
                        if let Err(e) = Self::handle_client_static(
                            socket,
                            connected_sockets,
                            client_senders,
                            offered_events,
                            message_process_tx,
                            server_running,
                        ) {
                            error!("Client handler error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                    // Continue listening despite individual connection failures
                }
            }
        }
        Ok(())
    }

    /// Message distributor that forwards messages from the global response channel to client-specific channels
    fn message_distributor(&self, client_response_rx: Receiver<RawMessageData>) -> Result<()> {
        while self.server_running.load(Ordering::SeqCst) {
            match client_response_rx.recv_timeout(Duration::from_millis(100)) {
                Ok((msg, target_addr)) => {
                    let client_senders = self.client_senders.lock();
                    if let Some(client_sender) = client_senders.get(&target_addr) {
                        if let Err(e) = client_sender.send(msg) {
                            error!("Failed to send message to client {}: {}", target_addr, e);
                        }
                    } else {
                        debug!("No client found for address: {}", target_addr);
                    }
                },
                Err(channel::RecvTimeoutError::Timeout) => {
                    // Continue the loop on timeout
                    continue;
                },
                Err(channel::RecvTimeoutError::Disconnected) => {
                    info!("Message distributor channel disconnected");
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn start(&mut self, blocking: bool) -> Result<()> {
        if self.service_discovery.is_none() {
            error!("Service discovery is not initialized");
            return Err(anyhow::anyhow!("Service discovery is not initialized"));
        }

        // Start service discovery
        let service_discovery = self.service_discovery.as_mut().unwrap();
        service_discovery.start()?;

        self.server_running.store(true, Ordering::SeqCst);

        // Start message handling thread
        let message_process_rx = self.message_process_rx.take().unwrap();
        let server_for_handler = Arc::new(self.clone());
        let message_handler_thread = {
            thread::spawn(move || {
                if let Err(e) = server_for_handler.handle_message_data_with_rx(message_process_rx) {
                    error!("Message handler error: {}", e);
                }
            })
        };

        // Start message distributor thread
        let client_response_rx = self.client_response_rx.take().unwrap();
        let server_for_distributor = Arc::new(self.clone());
        let message_distributor_thread = {
            thread::spawn(move || {
                if let Err(e) = server_for_distributor.message_distributor(client_response_rx) {
                    error!("Message distributor error: {}", e);
                }
            })
        };

        if blocking {
            self.start_listening()
        } else {
            let server = Arc::new(self.clone());
            let server_thread = thread::spawn(move || {
                if let Err(e) = server.start_listening() {
                    error!("Failed to start listening: {}", e);
                }
            });
            self.server_thread = Some(server_thread);
            self.message_handler_thread = Some(message_handler_thread);
            self.message_distributor_thread = Some(message_distributor_thread);
            Ok(())
        }
    }

    /// Gracefully shutdown the service application
    pub fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down service application with ID: {}", self.service_id);
        
        // Signal threads to stop
        self.server_running.store(false, Ordering::SeqCst);
        
        // Stop service discovery first
        if let Some(mut service_discovery) = self.service_discovery.take() {
            service_discovery.stop();
        }
        
        // Join threads
        if let Some(server_thread) = self.server_thread.take() {
            if let Err(e) = server_thread.join() {
                error!("Server thread panicked: {:?}", e);
            }
        }
        
        if let Some(message_handler_thread) = self.message_handler_thread.take() {
            if let Err(e) = message_handler_thread.join() {
                error!("Message handler thread panicked: {:?}", e);
            }
        }
        
        if let Some(message_distributor_thread) = self.message_distributor_thread.take() {
            if let Err(e) = message_distributor_thread.join() {
                error!("Message distributor thread panicked: {:?}", e);
            }
        }
        
        // Clear all connections and events
        {
            let mut connected_sockets = self.connected_sockets.lock();
            connected_sockets.clear();
            
            let mut offered_events = self.offered_events.lock();
            offered_events.clear();
            
            let mut subscribed_events = self.subscribed_events.lock();
            subscribed_events.clear();
            
            let mut open_requests = self.open_requests.lock();
            open_requests.clear();
        }
        
        self.offered_methods.clear();
        
        info!("Service application shutdown complete");
        Ok(())
    }
}
