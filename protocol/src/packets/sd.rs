use crate::constants::{SD_REQUEST_PREFIX, SD_RESPONSE_PREFIX};

/// Message format for service discovery protocol
#[derive(Debug, Clone, PartialEq)]
pub enum DiscoveryMessage {
    Request(u16),
    Response(u16),
}

impl DiscoveryMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            DiscoveryMessage::Request(name) => {
                let mut bytes = vec![];
                bytes.extend_from_slice(&SD_REQUEST_PREFIX.to_be_bytes());
                bytes.extend_from_slice(name.to_be_bytes().as_ref());
                bytes
            }
            DiscoveryMessage::Response(name) => {
                let mut bytes = vec![];
                bytes.extend_from_slice(&SD_RESPONSE_PREFIX.to_be_bytes());
                bytes.extend_from_slice(name.to_be_bytes().as_ref());
                bytes
            }
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 2 {
            return None;
        }

        let prefix = u16::from_be_bytes([bytes[0], bytes[1]]);
        let name = u16::from_be_bytes([bytes[2], bytes[3]]);
        match prefix {
            SD_REQUEST_PREFIX => Some(DiscoveryMessage::Request(name)),
            SD_RESPONSE_PREFIX => Some(DiscoveryMessage::Response(name)),
            _ => None,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_to_bytes_and_back() {
        let original_id = 0x01;
        let request = DiscoveryMessage::Request(original_id.clone());
        let bytes = request.to_bytes();

        assert_eq!(bytes[0..2], SD_REQUEST_PREFIX.to_be_bytes());
        assert_eq!(&bytes[2..], original_id.to_be_bytes());

        let parsed_request = DiscoveryMessage::from_bytes(&bytes).unwrap();
        match parsed_request {
            DiscoveryMessage::Request(name) => assert_eq!(name, original_id),
            _ => panic!("Expected DiscoveryMessage::Request"),
        }
    }

    #[test]
    fn test_response_to_bytes_and_back() {
        let original_id = 0x01;
        let response = DiscoveryMessage::Response(original_id.clone());
        let bytes = response.to_bytes();

        assert_eq!(bytes[0..2], SD_RESPONSE_PREFIX.to_be_bytes());
        assert_eq!(&bytes[2..], original_id.to_be_bytes());

        let parsed_response = DiscoveryMessage::from_bytes(&bytes).unwrap();
        match parsed_response {
            DiscoveryMessage::Response(name) => assert_eq!(name, original_id),
            _ => panic!("Expected DiscoveryMessage::Response"),
        }
    }

    #[test]
    fn test_from_bytes_empty() {
        assert_eq!(DiscoveryMessage::from_bytes(&[]), None);
    }

    #[test]
    fn test_from_bytes_too_short() {
        assert_eq!(DiscoveryMessage::from_bytes(&[0x01]), None);
    }

    #[test]
    fn test_from_bytes_unknown_prefix() {
        let bytes = vec![0xFF, 0xFF, 0x01, 0x02, 0x03]; // Unknown prefix
        assert_eq!(DiscoveryMessage::from_bytes(&bytes), None);
    }
}
