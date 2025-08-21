use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use rand::Rng;
use std::{
    io::{Read, Write},
    sync::Arc,
};

pub const MAX_PACKET_SIZE: usize = 1024;

pub type RawMessageData = (ApplicationMessage, std::net::IpAddr);

/// Callback type for handling method invocations.
pub type MethodInvokeCallback =
    Arc<dyn Fn(Vec<u8>) -> Result<Vec<u8>, ApplicationResponseErrorMessage> + Send + Sync>;

/// Callback type for handling method invocations.
pub type MethodResponseCallback = Arc<
    dyn Fn(
            Result<Vec<u8>, ApplicationResponseErrorMessage>,
        ) -> Result<Vec<u8>, ApplicationResponseErrorMessage>
        + Send
        + Sync,
>;

/// Callback type for handling events.
pub type OnEventInvokeCallback = Arc<dyn Fn(Vec<u8>) + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum ApplicationMessageType {
    Request = 0x00,
    Notification = 0x02,
    Subscribe = 0x01,
    Unsubscribe = 0x03,
    Response = 0x04,
    INVALID = 0xFF,
}

impl From<u8> for ApplicationMessageType {
    fn from(value: u8) -> Self {
        match value {
            0x00 => ApplicationMessageType::Request,
            0x02 => ApplicationMessageType::Notification,
            0x01 => ApplicationMessageType::Subscribe,
            0x03 => ApplicationMessageType::Unsubscribe,
            0x04 => ApplicationMessageType::Response,
            _ => ApplicationMessageType::INVALID,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum ApplicationMessageReturnCode {
    Ok = 0x00,
    Error = 0x01,
    INVALID = 0xFF,
}

impl From<u8> for ApplicationMessageReturnCode {
    fn from(value: u8) -> Self {
        match value {
            0x00 => ApplicationMessageReturnCode::Ok,
            0x01 => ApplicationMessageReturnCode::Error,
            _ => ApplicationMessageReturnCode::INVALID,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApplicationResponseErrorMessage {
    pub error_code: u8,
    pub error_message: String,
}

impl ApplicationResponseErrorMessage {
    pub fn new(error_code: u8, error_message: String) -> Self {
        ApplicationResponseErrorMessage {
            error_code,
            error_message,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.push(self.error_code);
        buffer.extend_from_slice(self.error_message.as_bytes());
        buffer
    }

    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.is_empty() {
            return Err(anyhow::anyhow!("Empty byte array"));
        }

        let error_code = bytes[0];
        let error_message = String::from_utf8_lossy(&bytes[1..]).to_string();

        Ok(ApplicationResponseErrorMessage {
            error_code,
            error_message,
        })
    }
}

/// Represents a message in the protocol
#[derive(Debug, Clone, PartialEq)]
pub struct ApplicationMessage {
    pub service_id: u16,
    pub method_id: u16,
    pub request_id: u16,
    pub message_type: ApplicationMessageType,
    pub return_code: ApplicationMessageReturnCode,
    pub length: u32,
    pub payload: Vec<u8>,
}

impl ApplicationMessage {
    /// Create a new protocol message
    pub fn new(
        service_id: u16,
        method_id: u16,
        request_id: Option<u16>,
        message_type: ApplicationMessageType,
        return_code: ApplicationMessageReturnCode,
        payload: Vec<u8>,
    ) -> Self {
        // Calculate the total message length (header + payload)
        // Header size: 2 (service_id) + 2 (method_id) + 4 (length) + 2 (client_id) + 1 (message_type) + 1 (return_code)
        let length = 2 + 2 + 4 + 2 + 1 + 1 + payload.len() as u32;

        ApplicationMessage {
            service_id,
            method_id,
            message_type,
            return_code,
            request_id: request_id.unwrap_or_else(ApplicationMessage::random_request_id),
            length,
            payload,
        }
    }

    pub fn set_payload(&mut self, payload: Vec<u8>) {
        self.length = 2 + 2 + 4 + 2 + 1 + 1 + payload.len() as u32;
        self.payload = payload;
    }

    /// Serialize the message to a writer
    fn serialize<W: Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        // Write header fields
        writer.write_u16::<BigEndian>(self.service_id)?;
        writer.write_u16::<BigEndian>(self.method_id)?;
        writer.write_u32::<BigEndian>(self.length)?;
        writer.write_u16::<BigEndian>(self.request_id)?;
        writer.write_u8(self.message_type as u8)?;
        writer.write_u8(self.return_code as u8)?;
        writer.write_u32::<BigEndian>(self.payload.len() as u32)?;

        // Write payload
        writer.write_all(&self.payload)?;

        Ok(())
    }

    /// Serialize the message to a byte vector
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut buffer = Vec::new();
        self.serialize(&mut buffer)?;
        Ok(buffer)
    }

    /// Deserialize a message from a reader
    fn deserialize<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let service_id = reader.read_u16::<BigEndian>()?;
        let method_id = reader.read_u16::<BigEndian>()?;
        let length = reader.read_u32::<BigEndian>()?;
        let request_id = reader.read_u16::<BigEndian>()?;
        let message_type = reader.read_u8()?;
        let return_code = reader.read_u8()?;
        let payload_length = reader.read_u32::<BigEndian>()?;

        let mut payload = vec![0u8; payload_length as usize];
        reader.read_exact(&mut payload)?;

        Ok(ApplicationMessage {
            service_id,
            method_id,
            length,
            request_id,
            message_type: ApplicationMessageType::from(message_type),
            return_code: ApplicationMessageReturnCode::from(return_code),
            payload,
        })
    }

    /// Deserialize a message from bytes
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        Self::deserialize(&mut cursor)
    }

    pub fn random_request_id() -> u16 {
        let mut rng = rand::rng();
        rng.random_range(0..=u16::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_application_message_serialization() {
        let msg = ApplicationMessage::new(
            0x1234,
            0x5678,
            Some(0xABCD),
            ApplicationMessageType::Request,
            ApplicationMessageReturnCode::Ok,
            vec![0x01, 0x02, 0x03, 0x04],
        );

        let bytes = msg.to_bytes().unwrap();
        let deserialized = ApplicationMessage::from_bytes(&bytes).unwrap();

        assert_eq!(msg, deserialized);
    }

    #[test]
    fn test_application_message_types() {
        assert_eq!(
            ApplicationMessageType::from(0x00),
            ApplicationMessageType::Request
        );
        assert_eq!(
            ApplicationMessageType::from(0x02),
            ApplicationMessageType::Notification
        );
        assert_eq!(
            ApplicationMessageType::from(0x03),
            ApplicationMessageType::Unsubscribe
        );
        assert_eq!(
            ApplicationMessageType::from(0x04),
            ApplicationMessageType::Response
        );
    }

    #[test]
    fn test_return_codes() {
        assert_eq!(
            ApplicationMessageReturnCode::from(0x00),
            ApplicationMessageReturnCode::Ok
        );
        assert_eq!(
            ApplicationMessageReturnCode::from(0x01),
            ApplicationMessageReturnCode::Error
        );
    }

    #[test]
    fn test_empty_payload() {
        let msg = ApplicationMessage::new(
            0x0001,
            0x0002,
            Some(0x01),
            ApplicationMessageType::Response,
            ApplicationMessageReturnCode::Ok,
            vec![],
        );

        let bytes = msg.to_bytes().unwrap();
        let deserialized = ApplicationMessage::from_bytes(&bytes).unwrap();

        assert_eq!(msg, deserialized);
    }

    #[test]
    fn test_large_payload() {
        let large_payload = vec![0xAA; 1000];
        let msg = ApplicationMessage::new(
            0x0001,
            0x0002,
            Some(0xFFFF),
            ApplicationMessageType::Notification,
            ApplicationMessageReturnCode::Error,
            large_payload.clone(),
        );

        let bytes = msg.to_bytes().unwrap();
        let deserialized = ApplicationMessage::from_bytes(&bytes).unwrap();

        assert_eq!(msg.payload.len(), large_payload.len());
        assert_eq!(msg, deserialized);
    }

    #[test]
    fn test_invalid_message_type() {
        let invalid_type = ApplicationMessageType::from(0xFF);
        assert_eq!(invalid_type, ApplicationMessageType::INVALID);
    }

    #[test]
    fn test_invalid_return_code() {
        let invalid_code = ApplicationMessageReturnCode::from(0xFF);
        assert_eq!(invalid_code, ApplicationMessageReturnCode::INVALID);
    }
}
