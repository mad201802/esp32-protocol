use std::{net::{TcpListener, TcpStream}, sync::{mpsc::{channel, Receiver, Sender}, Arc}};

use anyhow::Result;

use crate::sd::ServiceDiscovery;

use super::{config::ServiceApplicationConfig};

pub struct ServiceApplication {
    service_id: u16,
    config: ServiceApplicationConfig,
    service_discovery: Option<ServiceDiscovery>,
    tcp_listener: Option<Arc<TcpListener>>,
}

impl ServiceApplication {
    pub fn new(service_id: u16) -> Self {
        Self::with_config(service_id, ServiceApplicationConfig::default())
    }

    pub fn with_config(
        service_id: u16,
        config: ServiceApplicationConfig,
    ) -> Self {
        Self {
            service_id,
            config,
            service_discovery: None,
            tcp_listener: None,
        }
    }

    pub fn init(&mut self) -> Result<()> {
        // Initialize the service discovery component
        let mut service_discovery = ServiceDiscovery::new(self.service_id);
        self.service_discovery = Some(service_discovery);

        // Initialize the TCP listener
        let listener = TcpListener::bind((self.config.bind_addr, self.config.port))?;
        self.tcp_listener = Some(Arc::new(listener));

        Ok(())
    }

    pub fn handle_client(&self, socket: TcpStream, client_response_rx: bool) {
        // Handle client connection
        println!("Handling client connection from {:?}", socket.peer_addr().unwrap());
    }

    pub fn start(&mut self, blocking: bool) -> Result<()> {
        if self.service_discovery.is_none() {
            return Err(anyhow::anyhow!("Service discovery not initialized"));
        }

        if self.tcp_listener.is_none() {
            return Err(anyhow::anyhow!("TCP listener not initialized"));
        }

        // Start message handling thread (todo)

        let listener = self.tcp_listener.as_ref().unwrap();

        if blocking {

        } else {
            loop {
                let (socket, _addr) = listener.accept()?;
                println!("New connection from {:?}", socket.peer_addr()?);
            }
        }

        Ok(())
    }
}