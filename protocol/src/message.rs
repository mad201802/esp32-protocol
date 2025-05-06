use crate::constants::{SD_REQUEST_PREFIX, SD_RESPONSE_PREFIX};

/// Message format for service discovery protocol
#[derive(Debug, Clone)]
pub enum DiscoveryMessage {
    Request(String),
    Response(String),
}

impl DiscoveryMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            DiscoveryMessage::Request(name) => {
                let mut bytes = vec![];
                bytes.extend_from_slice(&SD_REQUEST_PREFIX.to_be_bytes());
                bytes.extend_from_slice(name.as_bytes());
                bytes
            }
            DiscoveryMessage::Response(name) => {
                let mut bytes = vec![];
                bytes.extend_from_slice(&SD_RESPONSE_PREFIX.to_be_bytes());
                bytes.extend_from_slice(name.as_bytes());
                bytes
            }
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 2 {
            return None;
        }

        let prefix = u16::from_be_bytes([bytes[0], bytes[1]]);
        let content = &bytes[2..];
        let name = String::from_utf8_lossy(content).to_string();

        match prefix {
            SD_REQUEST_PREFIX => Some(DiscoveryMessage::Request(name)),
            SD_RESPONSE_PREFIX => Some(DiscoveryMessage::Response(name)),
            _ => None,
        }
    }
}