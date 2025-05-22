use std::sync::Arc;

use anyhow::Result;
use log::{error, info};
use tokio::{net::{TcpListener, TcpStream}, sync::broadcast::{Receiver, Sender}};

use crate::sd::ServiceDiscovery;

use super::{config::ServiceApplicationConfig, packets::{ApplicationMessage, ApplicationMessageReturnCode, ApplicationMessageType, RawMessageData}};

#[derive(Clone)]
pub struct ServiceApplication {
    service_id: u16,
    config: ServiceApplicationConfig,
    service_discovery: Option<ServiceDiscovery>,
    tcp_listener: Option<Arc<TcpListener>>,

    message_process_tx: Sender<RawMessageData>,
    client_response_tx: Sender<RawMessageData>,
}

impl ServiceApplication {
    pub fn new(service_id: u16) -> Self {
        Self::with_config(service_id, ServiceApplicationConfig::default())
    }

    pub fn with_config(service_id: u16, config: ServiceApplicationConfig) -> Self {
        info!(
            "Creating new service application with ID: {}",
            service_id
        );
        
        Self {
            service_id,
            config,
            service_discovery: None,
            tcp_listener: None,

            message_process_tx: tokio::sync::broadcast::channel(100).0,
            client_response_tx: tokio::sync::broadcast::channel(100).0,
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        // Initialize the service discovery component
        let mut service_discovery = ServiceDiscovery::new(self.service_id);
        service_discovery.init().await?;
        self.service_discovery = Some(service_discovery);

        // Initialize the TCP listener
        let listener = TcpListener::bind((self.config.bind_addr, self.config.port)).await?;
        self.tcp_listener = Some(Arc::new(listener));

        Ok(())
    }

    async fn handle_message_data(&self) {
        let mut message_process_rx = self.message_process_tx.subscribe();

        while let Ok((packet, addr)) = message_process_rx.recv().await {
            let mut response_packet = ApplicationMessage::new(packet.service_id, packet.method_id, Some(packet.request_id), ApplicationMessageType::Response, ApplicationMessageReturnCode::Ok, vec![]);

            match packet.message_type {
                ApplicationMessageType::Request => todo!(),
                ApplicationMessageType::Notification => todo!(),
                ApplicationMessageType::Unsubscribe => todo!(),
                ApplicationMessageType::Response => todo!(),
                ApplicationMessageType::SDFindService | ApplicationMessageType::SDOfferService | ApplicationMessageType::SDStopOfferService | ApplicationMessageType::INVALID => {
                    error!("Did not expect the following message: {:?}", packet.message_type);
                },
            }

        }
    }

    async fn handle_client(&self, mut socket: TcpStream, mut client_response_rx: Receiver<RawMessageData>) {
        // Handle client connection
        info!(
            "Handling client connection from {:?}",
            socket.peer_addr().unwrap()
        );
    }

    pub async fn start(&mut self) {
        if self.service_discovery.is_none() {
            error!("Service discovery is not initialized");
            return
        }

        if self.tcp_listener.is_none() {
            error!("TCP listener is not initialized");
            return
        }

        // Start service discovery
        let service_discovery = self.service_discovery.as_mut().unwrap();
        service_discovery.start().await.unwrap();

        // Start message handling thread
        let server = Arc::new(self.clone());
        tokio::spawn(async move {
            server.handle_message_data().await;
        });

        let listener = self.tcp_listener.as_ref().unwrap();

        info!("Listening for incoming connections on {}:{}", self.config.bind_addr, self.config.port);
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
}
