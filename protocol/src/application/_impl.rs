use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use log::{debug, error, info, trace};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{
        Mutex,
        broadcast::{Receiver, Sender},
    },
    time,
};

use crate::{
    application::packets::ApplicationResponseErrorMessage, sd::ServiceDiscovery,
    utils::retry_with_delay_option,
};

use super::{
    config::ServiceApplicationConfig,
    packets::{
        ApplicationMessage, ApplicationMessageReturnCode, ApplicationMessageType,
        MethodInvokeCallback, MethodResponseCallback, OnEventInvokeCallback, RawMessageData,
    },
};

#[derive(Clone)]
pub struct ServiceApplication {
    service_id: u16,
    config: ServiceApplicationConfig,
    service_discovery: Option<ServiceDiscovery>,

    connected_sockets: Arc<Mutex<HashSet<IpAddr>>>,

    // Key: Method ID, Value: Callback
    offered_methods: HashMap<u16, MethodInvokeCallback>,

    // Key: Event ID, Value: Set of subscribers
    offered_events: Arc<Mutex<HashMap<u16, HashSet<IpAddr>>>>,
    subscribed_events: Arc<Mutex<HashMap<u16, OnEventInvokeCallback>>>,

    // Key: Request ID, Value: Callback
    open_requests: Arc<Mutex<HashMap<u16, MethodResponseCallback>>>,

    message_process_tx: Sender<RawMessageData>,
    client_response_tx: Sender<RawMessageData>,
}

impl ServiceApplication {
    pub fn new(service_id: u16) -> Self {
        Self::with_config(service_id, ServiceApplicationConfig::default())
    }

    pub fn with_config(service_id: u16, config: ServiceApplicationConfig) -> Self {
        info!("Creating new service application with ID: {}", service_id);

        Self {
            service_id,
            config,
            service_discovery: None,

            connected_sockets: Arc::new(Mutex::new(HashSet::new())),

            offered_events: Arc::new(Mutex::new(HashMap::new())),
            subscribed_events: Arc::new(Mutex::new(HashMap::new())),
            offered_methods: HashMap::new(),
            open_requests: Arc::new(Mutex::new(HashMap::new())),

            message_process_tx: tokio::sync::broadcast::channel(100).0,
            client_response_tx: tokio::sync::broadcast::channel(100).0,
        }
    }

    /// Initialize the application (service discovery, etc.)
    pub async fn init(&mut self) -> Result<()> {
        // Initialize the service discovery component
        let mut service_discovery = ServiceDiscovery::new(self.service_id);
        service_discovery.init().await?;
        self.service_discovery = Some(service_discovery);

        Ok(())
    }

    /// Offer a method to the service
    pub async fn offer_method(&mut self, method_id: u16, callback: MethodInvokeCallback) {
        self.offered_methods.insert(method_id, callback);
        trace!("Method {} offered", method_id);
    }

    /// Offer a event to the service
    pub async fn offer_event(&mut self, event_id: u16) {
        self.offered_events
            .lock()
            .await
            .insert(event_id, HashSet::new());
        trace!("Event {} offered", event_id);
    }

    // Notifying all subscribing clients of an event
    pub async fn notify(&self, event_id: u16, payload: Vec<u8>) {
        let offered_events = self.offered_events.lock().await;
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

                self.client_response_tx
                    .send((notification_packet, *client_ip))
                    .unwrap();
            }
        }
    }

    /// Connect to a service via ip and port
    async fn connect(&self, ip_addr: IpAddr, port: u16) {
        debug!("Connecting to service at {:?}", ip_addr);

        let socket = tokio::net::TcpStream::connect(SocketAddr::new(ip_addr, port))
            .await
            .unwrap();

        let server = Arc::new(self.clone());
        let client_response_rx = server.client_response_tx.subscribe();
        tokio::spawn(async move {
            server.handle_client(socket, client_response_rx).await;
        });

        time::sleep(Duration::from_millis(500)).await; // Wait for the client to be ready
    }

    async fn service_to_ip(&mut self, service_id: u16) -> Option<IpAddr> {
        // Check if we can get the ip address of the service

        trace!("Finding service with ID: {}", service_id);

        let service_discovery = self.service_discovery.as_mut().unwrap();
        let ip_addr = retry_with_delay_option(
            || async { service_discovery.find_service(service_id).await },
            4,
            500,
        )
        .await;

        // If we dont have the ip, return None
        if ip_addr.is_none() {
            error!("Could not find service with ID: {}", service_id);
            return None;
        }

        trace!("Found service with ID: {} at {:?}", service_id, ip_addr);

        // If we have the ip, check if we have a open socket
        let ip_addr = ip_addr.unwrap();

        {
            let connected_sockets = self.connected_sockets.lock().await;
            if connected_sockets.contains(&ip_addr) {
                return Some(ip_addr);
            }
        }

        // If we dont have a open socket, connect to the service
        self.connect(ip_addr, self.config.port).await;

        // Return the ip address of the service
        trace!(
            "Connected to service with ID: {} at {:?}",
            service_id, ip_addr
        );
        Some(ip_addr)
    }

    pub async fn call_method(
        &mut self,
        service_id: u16,
        method_id: u16,
        payload: Vec<u8>,
        callback: MethodResponseCallback,
    ) {
        debug!("Calling method {} on {:?}", method_id, service_id);

        let ip_addr = self.service_to_ip(service_id).await;
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
                let mut open_requests = self.open_requests.lock().await;
                open_requests.insert(request_id, callback);
            }
            Err(err) => {
                error!("Failed to send request packet: {:?}", err);
            }
        }
    }

    pub async fn subscribe(
        &mut self,
        service_id: u16,
        event_id: u16,
        callback: OnEventInvokeCallback,
    ) {
        debug!("Subscribing to event {}", event_id);

        let ip_addr = self.service_to_ip(service_id).await;
        if ip_addr.is_none() {
            error!("Could not find socket for service with ID: {}", service_id);
            return;
        }

        let subscribe_packet = ApplicationMessage::new(
            self.service_id,
            event_id,
            None,
            ApplicationMessageType::Subscribe,
            ApplicationMessageReturnCode::Ok,
            vec![],
        );

        match self
            .client_response_tx
            .send((subscribe_packet, ip_addr.unwrap()))
        {
            Ok(_) => {
                let mut subscribed_events = self.subscribed_events.lock().await;
                subscribed_events.insert(event_id, callback);
                debug!("Subscribed to event {}", event_id);
            }
            Err(err) => {
                error!("Failed to send subscribe packet: {:?}", err);
            }
        }
    }

    async fn handle_message_data(&self) {
        let mut message_process_rx = self.message_process_tx.subscribe();

        while let Ok((packet, addr)) = message_process_rx.recv().await {
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

                    self.client_response_tx
                        .send((response_packet, addr))
                        .unwrap();
                }
                ApplicationMessageType::Response => {
                    trace!("Received Response Message: {:?}", packet);
                    let mut open_requests = self.open_requests.lock().await;
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
                    let subscribed_events = self.subscribed_events.lock().await;
                    if let Some(callback) = subscribed_events.get(&packet.method_id) {
                        callback(packet.payload);
                    }
                }
                ApplicationMessageType::Subscribe => {
                    // Handle Event Subscriptions
                    let mut offered_events = self.offered_events.lock().await;
                    if let Some(offered_events_clients) = offered_events.get_mut(&packet.method_id)
                    {
                        println!("!!! New event subscription: {:?}", packet.method_id);

                        // Add the client to the event
                        offered_events_clients.insert(addr);
                    }
                }
                ApplicationMessageType::Unsubscribe => {
                    trace!("Received Unsubscribe Message: {:?}", packet);
                    let mut offered_events = self.offered_events.lock().await;
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
        }
    }

    /// Handle incoming client connections and messages
    async fn handle_client(
        &self,
        mut tcp_stream: TcpStream,
        mut client_response_rx: Receiver<RawMessageData>,
    ) {
        info!(
            "Handling client connection from {:?}",
            tcp_stream.peer_addr().unwrap()
        );

        let socket_addr = tcp_stream.peer_addr().unwrap();

        {
            let mut connected_sockets = self.connected_sockets.lock().await;
            connected_sockets.insert(socket_addr.ip());
        }

        let (reader, mut writer) = tcp_stream.split();
        let mut reader = BufReader::new(reader);
        let mut buffer: Vec<u8> = Vec::new();

        loop {
            tokio::select! {
                result = reader.read_buf(&mut buffer) => {

                    // Handle disconnection
                    if result.unwrap() == 0 {
                        info!("[Disconnected] {:?}", socket_addr);
                        {
                            let mut connected_sockets = self.connected_sockets.lock().await;
                            connected_sockets.remove(&socket_addr.ip());

                            let mut offered_events = self.offered_events.lock().await;
                            for (_, connected_clients) in offered_events.iter_mut() {
                                connected_clients.remove(&socket_addr.ip());
                            }
                        }
                        break;
                    }

                    match ApplicationMessage::from_bytes(&buffer.clone()) {
                        Ok(packet) => {
                            self.message_process_tx.send((packet, socket_addr.ip())).unwrap();
                        },
                        Err(err) => {
                            error!("Error deserializing packet: {:?}", err);
                        }
                    }

                    buffer.clear();
                },
                result = client_response_rx.recv() => {
                    let (msg, other_addr) = result.unwrap();

                    if other_addr == socket_addr.ip()  {
                        trace!("Sending message to {:?}:{:?}", socket_addr, msg);

                        match ApplicationMessage::to_bytes(&msg) {
                            Ok(packet) => {
                                writer.write_all(&packet).await.unwrap();
                            },
                            Err(err) => {
                                error!("Error serializing packet: {:?}", err);
                            }
                        }
                    } else {
                        debug!("Ignoring message for {:?} as it is not the intended recipient (current {:?})", other_addr, socket_addr);
                    }
                }
            }
        }
    }

    async fn _start_listening(&self) {
        let listener = TcpListener::bind((self.config.bind_addr, self.config.port))
            .await
            .unwrap();

        info!(
            "Listening for incoming connections on {}:{}",
            self.config.bind_addr, self.config.port
        );

        loop {
            let (socket, _addr) = listener.accept().await.unwrap();
            info!("[Connected] {:?}", _addr);

            let client_response_rx: Receiver<RawMessageData> = self.client_response_tx.subscribe();

            let server = Arc::new(self.clone());
            tokio::spawn(async move {
                server.handle_client(socket, client_response_rx).await;
            });
        }
    }

    pub async fn start(&mut self, blocking: bool) {
        if self.service_discovery.is_none() {
            error!("Service discovery is not initialized");
            return;
        }

        // Start service discovery
        let service_discovery = self.service_discovery.as_mut().unwrap();
        service_discovery.start().await.unwrap();

        // Start message handling thread
        let server = Arc::new(self.clone());
        tokio::spawn(async move {
            server.handle_message_data().await;
        });

        if blocking {
            self._start_listening().await;
        } else {
            let server = Arc::new(self.clone());
            tokio::spawn(async move {
                server._start_listening().await;
            });
        }
    }
}
