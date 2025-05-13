use crate::application::packets::{
    ApplicationMessage, ApplicationMessageReturnCode, ApplicationMessageType,
};

pub enum ServiceDiscoveryMessage {
    FindService(u16),
    OfferService(u16),
    StopOfferService(u16)
}

impl  ServiceDiscoveryMessage {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ()> {
        if bytes.len() < 2 {
            return Err(());
        }

        let message_type = bytes[0];
        let service_id = u16::from_be_bytes([bytes[1], bytes[2]]);

        match message_type {
            0x01 => Ok(ServiceDiscoveryMessage::FindService(service_id)),
            0x02 => Ok(ServiceDiscoveryMessage::OfferService(service_id)),
            0x03 => Ok(ServiceDiscoveryMessage::StopOfferService(service_id)),
            _ => Err(()),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            ServiceDiscoveryMessage::FindService(id) => vec![0x01, (*id >> 8) as u8, *id as u8],
            ServiceDiscoveryMessage::OfferService(id) => vec![0x02, (*id >> 8) as u8, *id as u8],
            ServiceDiscoveryMessage::StopOfferService(id) => vec![0x03, (*id >> 8) as u8, *id as u8],
        }
    }
}
