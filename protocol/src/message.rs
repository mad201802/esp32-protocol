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

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};

/// Represents a message in the custom protocol
#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolMessage {
    pub service_id: u16,
    pub method_id: u16,
    pub client_id: u16,
    pub protocol_version: u8,
    pub length: u32,
    pub payload: Vec<u8>,
}

impl ProtocolMessage {
    /// Create a new protocol message
    pub fn new(
        service_id: u16,
        method_id: u16,
        client_id: u16,
        protocol_version: u8,
        payload: Vec<u8>,
    ) -> Self {
        // Calculate the total message length (header + payload)
        // Header size: 2 + 2 + 4 + 2 + 1 + 4 = 15 bytes
        let length = 15 + payload.len() as u32;

        ProtocolMessage {
            service_id,
            method_id,
            length,
            client_id,
            protocol_version,
            payload,
        }
    }

    /// Serialize the message to a writer
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write header fields
        writer.write_u16::<BigEndian>(self.service_id)?;
        writer.write_u16::<BigEndian>(self.method_id)?;
        writer.write_u32::<BigEndian>(self.length)?;
        writer.write_u16::<BigEndian>(self.client_id)?;
        writer.write_u8(self.protocol_version)?;
        writer.write_u32::<BigEndian>(self.payload.len() as u32)?;
        
        // Write payload
        writer.write_all(&self.payload)?;
        
        Ok(())
    }

    /// Serialize the message to a byte vector
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.serialize(&mut buffer)?;
        Ok(buffer)
    }

    /// Deserialize a message from a reader
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let service_id = reader.read_u16::<BigEndian>()?;
        let method_id = reader.read_u16::<BigEndian>()?;
        let length = reader.read_u32::<BigEndian>()?;
        let client_id = reader.read_u16::<BigEndian>()?;
        let protocol_version = reader.read_u8()?;
        let payload_length = reader.read_u32::<BigEndian>()?;
        
        let mut payload = vec![0u8; payload_length as usize];
        reader.read_exact(&mut payload)?;
        
        Ok(ProtocolMessage {
            service_id,
            method_id,
            length,
            client_id,
            protocol_version,
            payload,
        })
    }

    /// Deserialize a message from bytes
    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        Self::deserialize(&mut cursor)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_message_serialization() {
        let msg = ProtocolMessage::new(
            1, // service_id
            2, // method_id
            3, // client_id
            1, // protocol_version
            vec![1, 2, 3, 4, 5], // payload
        );

        let bytes = msg.to_bytes().unwrap();
        let deserialized = ProtocolMessage::from_bytes(&bytes).unwrap();

        assert_eq!(msg, deserialized);
    }

    #[test]
    fn test_empty_payload() {
        let msg = ProtocolMessage::new(10, 20, 30, 2, vec![]);
        
        let bytes = msg.to_bytes().unwrap();
        let deserialized = ProtocolMessage::from_bytes(&bytes).unwrap();
        
        assert_eq!(deserialized.payload.len(), 0);
        assert_eq!(msg, deserialized);
    }

    #[test]
    fn test_discovery_request() {
        let service_name = "test_service";
        let message = DiscoveryMessage::Request(service_name.to_string());
        let bytes = message.to_bytes();
        
        let decoded = DiscoveryMessage::from_bytes(&bytes).unwrap();
        match decoded {
            DiscoveryMessage::Request(name) => assert_eq!(name, service_name),
            _ => panic!("Expected Request variant"),
        }
    }

    #[test]
    fn test_discovery_response() {
        let service_name = "response_service";
        let message = DiscoveryMessage::Response(service_name.to_string());
        let bytes = message.to_bytes();
        
        let decoded = DiscoveryMessage::from_bytes(&bytes).unwrap();
        match decoded {
            DiscoveryMessage::Response(name) => assert_eq!(name, service_name),
            _ => panic!("Expected Response variant"),
        }
    }

    #[test]
    fn test_discovery_invalid_prefix() {
        // Create invalid bytes with incorrect prefix
        let mut bytes = vec![0xFF, 0xFF]; // Invalid prefix
        bytes.extend_from_slice(b"test");
        
        assert!(DiscoveryMessage::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_discovery_too_short() {
        assert!(DiscoveryMessage::from_bytes(&[0x01]).is_none());
        assert!(DiscoveryMessage::from_bytes(&[]).is_none());
    }
}
