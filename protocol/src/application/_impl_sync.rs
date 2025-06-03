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

// Constants for ESP32 optimization
const MAX_CLIENTS: usize = 8;
const MAX_SOCKETS: usize = 8;
const MAX_OPEN_REQUESTS: usize = 16;
const CHANNEL_BUFFER_SIZE: usize = 32;
const CLIENT_BUFFER_SIZE: usize = 256;
const TEMP_BUFFER_SIZE: usize = 256;
const READ_TIMEOUT_MS: u64 = 100;
const SLEEP_INTERVAL_MS: u64 = 10;
const SUBSCRIPTION_TIMEOUT_MS: u64 = 1000;
const CONNECTION_WAIT_MS: u64 = 500;

// Error codes for consistent error handling
const ERROR_CODE_EVENT_NOT_OFFERED: u8 = 0x02;
const ERROR_CODE_METHOD_NOT_FOUND: u8 = 0x03;

use crate::{
    application::message::ApplicationResponseErrorMessage, 
    sd::{ServiceDiscovery, ServiceDiscoveryInterface},
    utils::retry_with_delay_option_sync,
};

use super::{
    config::ServiceApplicationConfig,
    message::{
        ApplicationMessage, ApplicationMessageReturnCode, ApplicationMessageType,
        MethodInvokeCallback, MethodResponseCallback, OnEventInvokeCallback, RawMessageData,
    },
};
pub struct ServiceApplication {
    service_id: u16,
    config: ServiceApplicationConfig,
    service_discovery: Option<Box<dyn ServiceDiscoveryInterface>>,

    connected_sockets: Arc<Mutex<HashSet<IpAddr>>>,
    
    // Key: IP Address, Value: Sender for that client
    client_senders: Arc<Mutex<HashMap<IpAddr, Sender<ApplicationMessage>>>>,

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
    /// Creates a new ServiceApplication with default configuration
    /// 
    /// # Arguments
    /// * `service_id` - Unique identifier for this service
    pub fn new(service_id: u16) -> Self {
        Self::with_config(service_id, ServiceApplicationConfig::default())
    }

    /// Creates a new ServiceApplication with custom configuration
    /// 
    /// # Arguments
    /// * `service_id` - Unique identifier for this service
    /// * `config` - Service configuration parameters
    pub fn with_config(service_id: u16, config: ServiceApplicationConfig) -> Self {
        info!("Creating new service application with ID: {}", service_id);

        let (message_process_tx, message_process_rx) = channel::bounded(CHANNEL_BUFFER_SIZE);
        let (client_response_tx, client_response_rx) = channel::bounded(CHANNEL_BUFFER_SIZE);

        Self {
            service_id,
            config,
            service_discovery: None,

            connected_sockets: Arc::new(Mutex::new(HashSet::with_capacity(MAX_SOCKETS))),
            client_senders: Arc::new(Mutex::new(HashMap::with_capacity(MAX_CLIENTS))),

            offered_events: Arc::new(Mutex::new(HashMap::with_capacity(MAX_SOCKETS))),
            subscribed_events: Arc::new(Mutex::new(HashMap::with_capacity(MAX_SOCKETS))),
            offered_methods: HashMap::with_capacity(MAX_SOCKETS),
            open_requests: Arc::new(Mutex::new(HashMap::with_capacity(MAX_OPEN_REQUESTS))),

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
        self.service_discovery = Some(Box::new(service_discovery));

        Ok(())
    }

    /// Initialize the application with a custom service discovery implementation
    /// 
    /// # Arguments
    /// * `service_discovery` - A custom service discovery implementation
    pub fn init_with_discovery(&mut self, mut service_discovery: Box<dyn ServiceDiscoveryInterface>) -> Result<()> {
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

    /// Notifies all subscribing clients of an event
    /// 
    /// # Arguments
    /// * `event_id` - ID of the event to notify
    /// * `payload` - Event data to send to subscribers
    pub fn notify(&self, event_id: u16, payload: Vec<u8>) {
        let offered_events = self.offered_events.lock();
        if let Some(clients_to_notify) = offered_events.get(&event_id) {
            if clients_to_notify.is_empty() {
                debug!("No subscribers for event {}", event_id);
                return;
            }

            for client_ip in clients_to_notify {
                let notification_packet = ApplicationMessage::new(
                    self.service_id,
                    event_id,
                    Some(0),
                    ApplicationMessageType::Notification,
                    ApplicationMessageReturnCode::Ok,
                    payload.clone(),
                );

                debug!("Notifying event {} to {:?}", event_id, client_ip);

                if let Err(e) = self.client_response_tx.send((notification_packet, *client_ip)) {
                    error!("Failed to send notification to {}: {}", client_ip, e);
                }
            }
        } else {
            error!("Event {} is not offered", event_id);
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

        thread::sleep(Duration::from_millis(CONNECTION_WAIT_MS)); // Wait for the client to be ready
        Ok(())
    }

    fn service_to_ip(&mut self, service_id: u16) -> Option<IpAddr> {
        // Check if we can get the ip address of the service

        trace!("Finding service with ID: {}", service_id);

        let service_discovery = self.service_discovery.as_ref().unwrap();
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

    /// Calls a method on a remote service
    /// 
    /// # Arguments
    /// * `service_id` - ID of the target service
    /// * `method_id` - ID of the method to call
    /// * `payload` - Method parameters
    /// * `callback` - Function to handle the response
    pub fn call_method(
        &mut self,
        service_id: u16,
        method_id: u16,
        payload: Vec<u8>,
        callback: MethodResponseCallback,
    ) {
        debug!("Calling method {} on service {}", method_id, service_id);

        let ip_addr = match self.service_to_ip(service_id) {
            Some(addr) => addr,
            None => {
                error!("Could not find service with ID: {}", service_id);
                return;
            }
        };

        let request_packet = ApplicationMessage::new(
            self.service_id,
            method_id,
            None,
            ApplicationMessageType::Request,
            ApplicationMessageReturnCode::Ok,
            payload,
        );

        let request_id = request_packet.request_id;
        match self.client_response_tx.send((request_packet, ip_addr)) {
            Ok(_) => {
                let mut open_requests = self.open_requests.lock();
                open_requests.insert(request_id, callback);
                debug!("Sent method call request with ID: {}", request_id);
            }
            Err(err) => {
                error!("Failed to send request packet: {:?}", err);
            }
        }
    }

    /// Subscribes to an event from a remote service
    /// 
    /// This method will attempt to subscribe to an event with automatic retries.
    /// The subscription process includes sending a subscription request and waiting
    /// for confirmation from the remote service.
    /// 
    /// # Arguments
    /// * `service_id` - ID of the service offering the event
    /// * `event_id` - ID of the event to subscribe to
    /// * `callback` - Function to handle event notifications
    pub fn subscribe(
        &mut self,
        service_id: u16,
        event_id: u16,
        callback: OnEventInvokeCallback,
    ) {
        debug!("Subscribing to event {}", event_id);

        let ip_addr = match self.service_to_ip(service_id) {
            Some(addr) => addr,
            None => {
                error!("Could not find socket for service with ID: {}", service_id);
                return;
            }
        };

        let subscription_successful = self.attempt_subscription(event_id, ip_addr);

        if subscription_successful {
            let mut subscribed_events = self.subscribed_events.lock();
            subscribed_events.insert(event_id, callback);
            debug!("Successfully subscribed to event {}", event_id);
        } else {
            error!("Failed to subscribe to event {} after retries", event_id);
        }
    }

    fn attempt_subscription(&self, event_id: u16, ip_addr: IpAddr) -> bool {
        retry_with_delay_option_sync(
            || self.send_subscription_request(event_id, ip_addr),
            4,
            500,
        ).is_some()
    }

    fn send_subscription_request(&self, event_id: u16, ip_addr: IpAddr) -> Option<()> {
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
        if let Err(err) = self.client_response_tx.send((subscribe_packet, ip_addr)) {
            error!("Failed to send subscribe packet: {:?}", err);
            return None;
        }

        debug!("Sent subscription request for event {} with request_id {}", event_id, request_id);
        
        self.wait_for_subscription_response(request_id, event_id)
    }

    fn wait_for_subscription_response(&self, request_id: u16, event_id: u16) -> Option<()> {
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
        let timeout_duration = Duration::from_millis(SUBSCRIPTION_TIMEOUT_MS);
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
                    thread::sleep(Duration::from_millis(SLEEP_INTERVAL_MS));
                    continue;
                }
                Err(TryRecvError::Disconnected) => {
                    error!("Subscription channel disconnected for event {}", event_id);
                    return None;
                }
            }
        }
    }

    fn handle_message_data_with_rx(&self, message_process_rx: Receiver<RawMessageData>) -> Result<()> {
        while self.server_running.load(Ordering::SeqCst) {
            match message_process_rx.recv_timeout(Duration::from_millis(READ_TIMEOUT_MS)) {
                Ok((packet, addr)) => {
                    self.process_message(packet, addr);
                }
                Err(channel::RecvTimeoutError::Timeout) => {
                    continue;
                }
                Err(channel::RecvTimeoutError::Disconnected) => {
                    info!("Message processing channel disconnected");
                    break;
                }
            }
        }
        Ok(())
    }

    fn process_message(&self, packet: ApplicationMessage, addr: IpAddr) {
        match packet.message_type {
            ApplicationMessageType::Request => self.handle_request(packet, addr),
            ApplicationMessageType::Response => self.handle_response(packet),
            ApplicationMessageType::Notification => self.handle_notification(packet),
            ApplicationMessageType::Subscribe => self.handle_subscription(packet, addr),
            ApplicationMessageType::Unsubscribe => self.handle_unsubscription(packet, addr),
            ApplicationMessageType::SDFindService
            | ApplicationMessageType::SDOfferService
            | ApplicationMessageType::SDStopOfferService
            | ApplicationMessageType::INVALID => {
                error!("Unexpected message type: {:?}", packet.message_type);
            }
        }
    }

    fn handle_request(&self, packet: ApplicationMessage, addr: IpAddr) {
        debug!("Received request from {:?}", addr);

        let response_packet = if let Some(callback) = self.offered_methods.get(&packet.method_id) {
            match callback(packet.payload) {
                Ok(response_data) => {
                    self.create_response_packet(
                        packet.service_id,
                        packet.method_id,
                        packet.request_id,
                        response_data,
                    )
                }
                Err(err) => {
                    error!("Error processing request: {:?}", err);
                    ApplicationMessage::new(
                        packet.service_id,
                        packet.method_id,
                        Some(packet.request_id),
                        ApplicationMessageType::Response,
                        ApplicationMessageReturnCode::Error,
                        err.to_bytes(),
                    )
                }
            }
        } else {
            error!("Method {} not found", packet.method_id);
            self.create_error_response_packet(
                packet.service_id,
                packet.method_id,
                packet.request_id,
                ERROR_CODE_METHOD_NOT_FOUND,
                format!("Method {} not found", packet.method_id),
            )
        };

        if let Err(e) = self.client_response_tx.send((response_packet, addr)) {
            error!("Failed to send response: {}", e);
        }
    }

    fn handle_response(&self, packet: ApplicationMessage) {
        trace!("Received Response Message: {:?}", packet);
        let mut open_requests = self.open_requests.lock();
        if let Some(request_callback) = open_requests.remove(&packet.request_id) {
            if packet.return_code == ApplicationMessageReturnCode::Ok {
                let _result = request_callback(Ok(packet.payload));
            } else {
                let error_message = ApplicationResponseErrorMessage::from_bytes(&packet.payload)
                    .unwrap();
                let _result = request_callback(Err(error_message));
            }
        }
    }

    fn handle_notification(&self, packet: ApplicationMessage) {
        trace!("Received Notification Message: {:?}", packet);
        let subscribed_events = self.subscribed_events.lock();
        if let Some(callback) = subscribed_events.get(&packet.method_id) {
            callback(packet.payload);
        }
    }

    fn handle_subscription(&self, packet: ApplicationMessage, addr: IpAddr) {
        let mut offered_events = self.offered_events.lock();
        if let Some(offered_events_clients) = offered_events.get_mut(&packet.method_id) {
            debug!("New event subscription: {:?}", packet.method_id);

            offered_events_clients.insert(addr);
            
            let response_packet = self.create_response_packet(
                packet.service_id,
                packet.method_id,
                packet.request_id,
                vec![],
            );
            
            if let Err(e) = self.client_response_tx.send((response_packet, addr)) {
                error!("Failed to send subscription response: {}", e);
            }
        } else {
            error!("Event {} not offered, rejecting subscription from {:?}", packet.method_id, addr);
            
            let response_packet = self.create_error_response_packet(
                packet.service_id,
                packet.method_id,
                packet.request_id,
                ERROR_CODE_EVENT_NOT_OFFERED,
                format!("Event {} not offered", packet.method_id),
            );
            
            if let Err(e) = self.client_response_tx.send((response_packet, addr)) {
                error!("Failed to send error response: {}", e);
            }
        }
    }

    fn handle_unsubscription(&self, packet: ApplicationMessage, addr: IpAddr) {
        trace!("Received Unsubscribe Message: {:?}", packet);
        let mut offered_events = self.offered_events.lock();
        if let Some(connected_clients) = offered_events.get_mut(&packet.method_id) {
            connected_clients.remove(&addr);
        }
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

    fn unregister_client(&self, ip: IpAddr) {
        let mut connected_sockets = self.connected_sockets.lock();
        connected_sockets.remove(&ip);

        let mut client_senders = self.client_senders.lock();
        client_senders.remove(&ip);

        let mut offered_events = self.offered_events.lock();
        for (_, connected_clients) in offered_events.iter_mut() {
            connected_clients.remove(&ip);
        }
    }

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

                    let server = Arc::new(self.clone());
                    thread::spawn(move || {
                        if let Err(e) = server.handle_client(socket) {
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

    /// Helper method to create a success response packet
    fn create_response_packet(
        &self,
        service_id: u16,
        method_id: u16,
        request_id: u16,
        payload: Vec<u8>,
    ) -> ApplicationMessage {
        let mut packet = ApplicationMessage::new(
            service_id,
            method_id,
            Some(request_id),
            ApplicationMessageType::Response,
            ApplicationMessageReturnCode::Ok,
            vec![],
        );
        packet.set_payload(payload);
        packet
    }

    /// Helper method to create an error response packet
    fn create_error_response_packet(
        &self,
        service_id: u16,
        method_id: u16,
        request_id: u16,
        error_code: u8,
        error_message: String,
    ) -> ApplicationMessage {
        let error = ApplicationResponseErrorMessage::new(error_code, error_message);
        ApplicationMessage::new(
            service_id,
            method_id,
            Some(request_id),
            ApplicationMessageType::Response,
            ApplicationMessageReturnCode::Error,
            error.to_bytes(),
        )
    }
}
