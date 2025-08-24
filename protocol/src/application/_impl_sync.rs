use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::Result;
use crossbeam::channel::{self, Receiver, TryRecvError};
use log::{debug, error, info, trace};
use parking_lot::Mutex;

/// Error codes for consistent application protocol error handling
mod error_codes {
    pub const EVENT_NOT_OFFERED: u8 = 0x02;
    pub const METHOD_NOT_FOUND: u8 = 0x03;
    pub const TIMEOUT: u8 = 0x04;
}

use crate::{
    application::{TcpConnectionPool, message::ApplicationResponseErrorMessage},
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

/// Tracks pending method call requests with their timeout deadlines
struct RequestTimeout {
    callback: MethodResponseCallback,
    deadline: Instant,
}

impl RequestTimeout {
    fn new(callback: MethodResponseCallback, timeout_duration: Duration) -> Self {
        Self {
            callback,
            deadline: Instant::now() + timeout_duration,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() > self.deadline
    }
}

/// A service application that handles network communication using the application protocol.
///
/// This service can:
/// - Offer methods that can be called by remote services
/// - Offer events that remote services can subscribe to  
/// - Call methods on remote services
/// - Subscribe to events from remote services
/// - Handle automatic service discovery
///
/// The service runs on a separate thread and uses TCP connections for communication.
/// All operations are thread-safe and can be called from multiple threads.
pub struct ServiceApplication {
    // Core identification
    service_id: u16,
    config: ServiceApplicationConfig,

    // Service discovery for finding other services
    service_discovery: Option<Box<dyn ServiceDiscoveryInterface>>,

    // Network communication
    tcp_pool: Option<TcpConnectionPool<ApplicationMessage>>,

    // Method handling: Key = Method ID, Value = Callback
    offered_methods: HashMap<u16, MethodInvokeCallback>,

    // Event handling: Key = Event ID, Value = Set of subscriber IPs
    offered_events: Arc<Mutex<HashMap<u16, HashSet<IpAddr>>>>,

    // Event subscriptions: Key = Event ID, Value = Callback for incoming events
    subscribed_events: Arc<Mutex<HashMap<u16, OnEventInvokeCallback>>>,

    // Request tracking: Key = Request ID, Value = Request with timeout info
    open_requests: Arc<Mutex<HashMap<u16, RequestTimeout>>>,

    // Threading infrastructure
    message_handler_thread: Option<JoinHandle<()>>,
    timeout_handler_thread: Option<JoinHandle<()>>,
    server_running: Arc<AtomicBool>,
}

impl Clone for ServiceApplication {
    /// Creates a clone of the ServiceApplication for use in worker threads.
    ///
    /// **Important**: Only shared state is cloned. Resource-managing fields like
    /// service_discovery, tcp_pool, and thread handles are set to None to prevent
    /// resource conflicts. This clone is intended for worker threads that need
    /// access to the shared state but shouldn't manage resources.
    fn clone(&self) -> Self {
        Self {
            // Core identification (safe to clone)
            service_id: self.service_id,
            config: self.config.clone(),

            // Resource managers (NOT cloned to avoid conflicts)
            service_discovery: None,
            tcp_pool: None,
            message_handler_thread: None,
            timeout_handler_thread: None,

            // Shared state (cloned via Arc)
            offered_methods: self.offered_methods.clone(),
            offered_events: Arc::clone(&self.offered_events),
            subscribed_events: Arc::clone(&self.subscribed_events),
            open_requests: Arc::clone(&self.open_requests),
            server_running: Arc::clone(&self.server_running),
        }
    }
}

impl ServiceApplication {
    // ================================
    // Construction and Initialization
    // ================================

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

        let max_open_requests = config.max_open_requests;

        Self {
            // Core identification
            service_id,
            config,

            // Resource managers (initialized later)
            service_discovery: None,
            tcp_pool: None,
            message_handler_thread: None,
            timeout_handler_thread: None,

            // Shared state
            offered_events: Arc::new(Mutex::new(HashMap::with_capacity(8))),
            subscribed_events: Arc::new(Mutex::new(HashMap::with_capacity(8))),
            offered_methods: HashMap::with_capacity(8),
            open_requests: Arc::new(Mutex::new(HashMap::with_capacity(max_open_requests))),
            server_running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Initialize the application (service discovery, etc.)
    pub fn init(&mut self) -> Result<()> {
        // Initialize the service discovery component
        let mut service_discovery = ServiceDiscovery::with_config(self.service_id, self.config.discovery_config.clone());
        service_discovery.init()?;
        self.service_discovery = Some(Box::new(service_discovery));

        Ok(())
    }

    // =====================================
    // Service Interface (Methods & Events)
    // =====================================

    /// Offer a method to the service
    /// # Arguments
    /// * `method_id` - ID of the method to offer
    /// * `callback` - Function to handle method invocation
    pub fn offer_method(&mut self, method_id: u16, callback: MethodInvokeCallback) {
        self.offered_methods.insert(method_id, callback);
        trace!("Method {} offered", method_id);
    }

    /// Offer a event to the service
    /// # Arguments
    /// * `event_id` - ID of the event to offer
    pub fn offer_event(&mut self, event_id: u16) {
        self.offered_events.lock().insert(event_id, HashSet::new());
        trace!("Event {} offered", event_id);
    }

    /// Notifies all subscribing clients of an event
    ///
    /// # Arguments
    /// * `event_id` - ID of the event to notify
    /// * `payload` - Event data to send to subscribers
    pub fn notify(&self, event_id: u16, payload: Vec<u8>) {
        debug!("Notifying event {}", event_id);

        // Check if the current service is running
        if !self.server_running.load(Ordering::SeqCst) {
            error!("Service application is not running");
            return;
        }

        let offered_events = self.offered_events.lock();
        if let Some(clients_to_notify) = offered_events.get(&event_id) {
            if clients_to_notify.is_empty() {
                trace!("No subscribers for event {}", event_id);
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

                if let Some(tcp_pool) = &self.tcp_pool {
                    let sender = tcp_pool.get_response_sender();
                    if let Err(e) = sender.send((notification_packet, *client_ip)) {
                        error!("Failed to send notification to {}: {}", client_ip, e);
                    }
                } else {
                    error!("TCP pool not available");
                }
            }
        } else {
            error!("Event {} is not offered", event_id);
        }
    }

    // ==============================
    // Remote Service Communication
    // ==============================

    /// Resolves a service ID to its IP address and ensures TCP connection
    ///
    /// # Arguments
    /// * `service_id` - ID of the service to find
    ///
    /// # Returns
    /// * `Some(IpAddr)` if service found and connected
    /// * `None` if service not found or connection failed
    fn service_to_ip(&mut self, service_id: u16) -> Option<IpAddr> {
        trace!("Finding service with ID: {}", service_id);

        let service_discovery = self.service_discovery.as_ref()?;
        let ip_addr =
            retry_with_delay_option_sync(|| service_discovery.find_service(service_id), 4, 500)?;

        trace!("Found service with ID: {} at {:?}", service_id, ip_addr);

        let tcp_pool = self.tcp_pool.as_ref()?;

        if tcp_pool.is_connected(ip_addr) {
            return Some(ip_addr);
        }

        // If we don't have an open socket, connect to the service
        if let Err(e) = tcp_pool.connect(ip_addr) {
            error!("Failed to connect to service {}: {}", service_id, e);
            return None;
        }

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

        // Check if the current service is running
        if !self.server_running.load(Ordering::SeqCst) {
            error!("Service application is not running");
            return;
        }

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

        if let Some(tcp_pool) = &self.tcp_pool {
            let sender = tcp_pool.get_response_sender();
            match sender.send((request_packet, ip_addr)) {
                Ok(_) => {
                    let mut open_requests = self.open_requests.lock();
                    let request_timeout =
                        RequestTimeout::new(callback, self.config.method_call_timeout);
                    open_requests.insert(request_id, request_timeout);
                    debug!("Sent method call request with ID: {}", request_id);
                }
                Err(err) => {
                    error!("Failed to send request packet: {:?}", err);
                }
            }
        } else {
            error!("TCP pool not available");
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
    pub fn subscribe(&mut self, service_id: u16, event_id: u16, callback: OnEventInvokeCallback) {
        debug!("Subscribing to event {}", event_id);

        let ip_addr = match self.service_to_ip(service_id) {
            Some(addr) => addr,
            None => {
                error!("Could not find socket for service with ID: {}", service_id);
                return;
            }
        };

        if self.attempt_subscription(event_id, ip_addr) {
            let mut subscribed_events = self.subscribed_events.lock();
            subscribed_events.insert(event_id, callback);
            debug!("Successfully subscribed to event {}", event_id);
        } else {
            error!("Failed to subscribe to event {} after retries", event_id);
        }
    }

    /// Unsubscribes from an event on a remote service
    ///
    /// This method sends an unsubscribe request to the remote service and removes
    /// the event callback from the local subscribed events list.
    ///
    /// # Arguments
    /// * `service_id` - ID of the service offering the event
    /// * `event_id` - ID of the event to unsubscribe from
    pub fn unsubscribe(&mut self, service_id: u16, event_id: u16) {
        debug!(
            "Unsubscribing from event {} on service {}",
            event_id, service_id
        );

        // Remove the event callback from local subscriptions first
        {
            let mut subscribed_events = self.subscribed_events.lock();
            if subscribed_events.remove(&event_id).is_some() {
                debug!("Removed local subscription for event {}", event_id);
            } else {
                debug!("No local subscription found for event {}", event_id);
            }
        }

        // Find the service IP address
        let ip_addr = match self.service_to_ip(service_id) {
            Some(addr) => addr,
            None => {
                error!(
                    "Could not find service with ID: {} for unsubscribe",
                    service_id
                );
                return;
            }
        };

        // Send unsubscribe request to the remote service
        let unsubscribe_packet = ApplicationMessage::new(
            self.service_id,
            event_id,
            None,
            ApplicationMessageType::Unsubscribe,
            ApplicationMessageReturnCode::Ok,
            vec![],
        );

        if let Some(tcp_pool) = &self.tcp_pool {
            let sender = tcp_pool.get_response_sender();
            match sender.send((unsubscribe_packet, ip_addr)) {
                Ok(_) => {
                    debug!(
                        "Sent unsubscribe request for event {} to service {}",
                        event_id, service_id
                    );
                }
                Err(err) => {
                    error!("Failed to send unsubscribe packet: {:?}", err);
                }
            }
        } else {
            error!("TCP pool not available for unsubscribe");
        }
    }

    // ===============================
    // Private Subscription Helpers
    // ===============================

    /// Attempts subscription with retries
    fn attempt_subscription(&self, event_id: u16, ip_addr: IpAddr) -> bool {
        retry_with_delay_option_sync(|| self.send_subscription_request(event_id, ip_addr), 4, 500)
            .is_some()
    }

    /// Sends a single subscription request
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
        if let Some(tcp_pool) = &self.tcp_pool {
            let sender = tcp_pool.get_response_sender();
            if let Err(err) = sender.send((subscribe_packet, ip_addr)) {
                error!("Failed to send subscribe packet: {:?}", err);
                return None;
            }
        } else {
            error!("TCP pool not available");
            return None;
        }

        debug!(
            "Sent subscription request for event {} with request_id {}",
            event_id, request_id
        );
        self.wait_for_subscription_response(request_id, event_id)
    }

    /// Waits for subscription confirmation with timeout
    fn wait_for_subscription_response(&self, request_id: u16, event_id: u16) -> Option<()> {
        let (response_tx, response_rx) = channel::bounded(1);

        // Store the response sender in open_requests
        {
            let mut open_requests = self.open_requests.lock();
            let request_timeout = RequestTimeout::new(
                Arc::new(move |result| {
                    let _ = response_tx.try_send(result);
                    Ok(vec![])
                }),
                self.config.subscription_timeout,
            );
            open_requests.insert(request_id, request_timeout);
        }

        // Wait for response with timeout
        let timeout_duration = self.config.subscription_timeout;
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
                    thread::sleep(self.config.sleep_interval);
                    continue;
                }
                Err(TryRecvError::Disconnected) => {
                    error!("Subscription channel disconnected for event {}", event_id);
                    return None;
                }
            }
        }
    }

    // ========================
    // Message Processing Core
    // ========================

    /// Main message processing loop - runs in a dedicated thread
    fn handle_message_data_with_rx_and_sender(
        &self,
        message_process_rx: Receiver<RawMessageData>,
        response_sender: crossbeam::channel::Sender<(ApplicationMessage, IpAddr)>,
    ) -> Result<()> {
        while self.server_running.load(Ordering::SeqCst) {
            match message_process_rx.recv_timeout(Duration::from_millis(100)) {
                Ok((packet, addr)) => {
                    self.process_message_with_sender(packet, addr, &response_sender);
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

    /// Routes incoming messages to appropriate handlers based on message type
    fn process_message_with_sender(
        &self,
        packet: ApplicationMessage,
        addr: IpAddr,
        response_sender: &crossbeam::channel::Sender<(ApplicationMessage, IpAddr)>,
    ) {
        match packet.message_type {
            ApplicationMessageType::Request => {
                self.handle_request_with_sender(packet, addr, response_sender)
            }
            ApplicationMessageType::Response => self.handle_response(packet),
            ApplicationMessageType::Notification => {
                self.handle_notification_with_sender(packet, addr, response_sender)
            }
            ApplicationMessageType::Subscribe => {
                self.handle_subscription_with_sender(packet, addr, response_sender)
            }
            ApplicationMessageType::Unsubscribe => self.handle_unsubscription(packet, addr),
            ApplicationMessageType::INVALID => {
                error!("Unexpected message type: {:?}", packet.message_type);
            }
        }
    }

    // =======================
    // Message Type Handlers
    // =======================

    /// Handles incoming method call requests
    fn handle_request_with_sender(
        &self,
        packet: ApplicationMessage,
        addr: IpAddr,
        response_sender: &crossbeam::channel::Sender<(ApplicationMessage, IpAddr)>,
    ) {
        debug!("Received request from {:?}", addr);

        let response_packet = if let Some(callback) = self.offered_methods.get(&packet.method_id) {
            match callback(packet.payload) {
                Ok(response_data) => self.create_response_packet(
                    packet.service_id,
                    packet.method_id,
                    packet.request_id,
                    response_data,
                ),
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
                error_codes::METHOD_NOT_FOUND,
                format!("Method {} not found", packet.method_id),
            )
        };

        if let Err(e) = response_sender.send((response_packet, addr)) {
            error!("Failed to send response: {}", e);
        }
    }

    /// Handles responses to our method calls
    fn handle_response(&self, packet: ApplicationMessage) {
        trace!("Received Response Message: {:?}", packet);
        let mut open_requests = self.open_requests.lock();
        if let Some(request_timeout) = open_requests.remove(&packet.request_id) {
            let request_callback = request_timeout.callback;
            if packet.return_code == ApplicationMessageReturnCode::Ok {
                let _result: std::result::Result<Vec<u8>, ApplicationResponseErrorMessage> =
                    request_callback(Ok(packet.payload));
            } else {
                let error_message =
                    ApplicationResponseErrorMessage::from_bytes(&packet.payload).unwrap();
                let _result = request_callback(Err(error_message));
            }
        }
    }

    /// Handles event notifications from remote services  
    fn handle_notification_with_sender(
        &self,
        packet: ApplicationMessage,
        addr: IpAddr,
        response_sender: &crossbeam::channel::Sender<(ApplicationMessage, IpAddr)>,
    ) {
        trace!("Received Notification Message: {:?}", packet);
        let subscribed_events = self.subscribed_events.lock();
        if let Some(callback) = subscribed_events.get(&packet.method_id) {
            callback(packet.payload);
        } else {
            // Client received notification for an event it's not subscribed to
            // Send unsubscribe message to the sender
            debug!(
                "Received notification for event {} that we're not subscribed to, sending unsubscribe to {:?}",
                packet.method_id, addr
            );

            let unsubscribe_packet = ApplicationMessage::new(
                self.service_id,
                packet.method_id,
                None,
                ApplicationMessageType::Unsubscribe,
                ApplicationMessageReturnCode::Ok,
                vec![],
            );

            if let Err(e) = response_sender.send((unsubscribe_packet, addr)) {
                error!(
                    "Failed to send unsubscribe message for unwanted notification: {}",
                    e
                );
            } else {
                debug!(
                    "Sent unsubscribe message for event {} to {:?}",
                    packet.method_id, addr
                );
            }
        }
    }

    /// Handles subscription requests from remote clients
    fn handle_subscription_with_sender(
        &self,
        packet: ApplicationMessage,
        addr: IpAddr,
        response_sender: &crossbeam::channel::Sender<(ApplicationMessage, IpAddr)>,
    ) {
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

            if let Err(e) = response_sender.send((response_packet, addr)) {
                error!("Failed to send subscription response: {}", e);
            }
        } else {
            error!(
                "Event {} not offered, rejecting subscription from {:?}",
                packet.method_id, addr
            );

            let response_packet = self.create_error_response_packet(
                packet.service_id,
                packet.method_id,
                packet.request_id,
                error_codes::EVENT_NOT_OFFERED,
                format!("Event {} not offered", packet.method_id),
            );

            if let Err(e) = response_sender.send((response_packet, addr)) {
                error!("Failed to send error response: {}", e);
            }
        }
    }

    /// Handles unsubscription requests from remote clients
    fn handle_unsubscription(&self, packet: ApplicationMessage, addr: IpAddr) {
        trace!("Received Unsubscribe Message: {:?}", packet);
        let mut offered_events = self.offered_events.lock();
        if let Some(connected_clients) = offered_events.get_mut(&packet.method_id) {
            connected_clients.remove(&addr);
        }
    }

    // ========================
    // Service Lifecycle
    // ========================

    /// Starts the service application
    ///
    /// # Arguments
    /// * `blocking` - If true, blocks until service is stopped. If false, returns immediately.
    ///
    /// # Returns
    /// * `Ok(())` on successful startup
    /// * `Err(...)` if startup fails
    pub fn start(&mut self, blocking: bool) -> Result<()> {
        if self.server_running.load(Ordering::SeqCst) {
            error!("Server is already running");
            return Err(anyhow::anyhow!("Server is already running"));
        }

        if self.service_discovery.is_none() {
            error!("Service discovery is not initialized");
            return Err(anyhow::anyhow!("Service discovery is not initialized"));
        }

        // Start service discovery
        let service_discovery = self.service_discovery.as_mut().unwrap();
        service_discovery.start()?;

        self.server_running.store(true, Ordering::SeqCst);

        // Create and start TCP connection pool
        let mut tcp_pool = TcpConnectionPool::new(
            self.config.bind_addr,
            self.config.port,
            self.config.max_clients,
        );

        // Get the message receiver from the TCP pool for our message handler
        let message_process_rx = tcp_pool.take_message_receiver().unwrap();

        // Get the response sender for the message handler
        let response_sender = tcp_pool.get_response_sender();

        tcp_pool.start(false)?; // Start non-blocking
        self.tcp_pool = Some(tcp_pool);

        // Start message handling thread
        let server_for_handler = Arc::new(self.clone());
        let message_handler_thread = thread::spawn(move || {
            if let Err(e) = server_for_handler
                .handle_message_data_with_rx_and_sender(message_process_rx, response_sender)
            {
                error!("Message handler error: {}", e);
            }
        });

        // Start timeout handling thread
        let server_for_timeout = Arc::new(self.clone());
        let timeout_handler_thread = thread::spawn(move || {
            server_for_timeout.handle_request_timeouts();
        });

        if blocking {
            // Block on the message handler thread
            self.message_handler_thread = Some(message_handler_thread);
            self.timeout_handler_thread = Some(timeout_handler_thread);
            loop {
                if !self.server_running.load(Ordering::SeqCst) {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
            Ok(())
        } else {
            self.message_handler_thread = Some(message_handler_thread);
            self.timeout_handler_thread = Some(timeout_handler_thread);
            Ok(())
        }
    }

    /// Gracefully shutdown the service application
    pub fn shutdown(&mut self) -> Result<()> {
        info!(
            "Shutting down service application with ID: {}",
            self.service_id
        );

        // Signal threads to stop
        self.server_running.store(false, Ordering::SeqCst);

        // Stop service discovery first
        if let Some(mut service_discovery) = self.service_discovery.take() {
            service_discovery.stop();
        }

        // Stop TCP connection pool
        if let Some(mut tcp_pool) = self.tcp_pool.take() {
            tcp_pool.stop()?;
        }

        // Join threads
        if let Some(message_handler_thread) = self.message_handler_thread.take()
            && let Err(e) = message_handler_thread.join()
        {
            error!("Message handler thread panicked: {:?}", e);
        }

        if let Some(timeout_handler_thread) = self.timeout_handler_thread.take()
            && let Err(e) = timeout_handler_thread.join()
        {
            error!("Timeout handler thread panicked: {:?}", e);
        }

        // Clear all events and requests
        {
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

    // ===================
    // Background Workers
    // ===================

    /// Handle timeout checking for pending method call requests
    /// Runs in a dedicated background thread
    fn handle_request_timeouts(&self) {
        while self.server_running.load(Ordering::SeqCst) {
            let mut timed_out_requests = Vec::new();

            // Check for timed out requests
            {
                let mut open_requests = self.open_requests.lock();
                let mut to_remove = Vec::new();

                for (&request_id, request_timeout) in open_requests.iter() {
                    if request_timeout.is_expired() {
                        timed_out_requests.push((request_id, request_timeout.callback.clone()));
                        to_remove.push(request_id);
                    }
                }

                // Remove timed out requests
                for request_id in to_remove {
                    open_requests.remove(&request_id);
                }
            }

            // Process timed out requests
            for (request_id, callback) in timed_out_requests {
                debug!("Request {} timed out", request_id);
                let timeout_error = ApplicationResponseErrorMessage::new(
                    error_codes::TIMEOUT,
                    "Request timed out".to_string(),
                );
                let _result = callback(Err(timeout_error));
            }

            thread::sleep(self.config.request_timeout_check_interval);
        }
    }

    // ================
    // Helper Methods
    // ================

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
