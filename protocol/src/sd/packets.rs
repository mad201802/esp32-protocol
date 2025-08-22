use anyhow::Result;

#[derive(Debug, Clone, PartialEq)]
pub enum ServiceDiscoveryMessage {
    OfferService(u16),
    StopOfferService(u16),
}

impl ServiceDiscoveryMessage {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 3 {
            return Err(anyhow::anyhow!("Invalid message length"));
        }

        let message_type = bytes[0];
        let service_id = u16::from_be_bytes([bytes[1], bytes[2]]);

        match message_type {
            0x01 => Ok(ServiceDiscoveryMessage::OfferService(service_id)),
            0x02 => Ok(ServiceDiscoveryMessage::StopOfferService(service_id)),
            _ => Err(anyhow::anyhow!("Unknown message type: {}", message_type)),
        }
    }

    /// Serialize to a fixed-size byte array to avoid heap allocation
    pub fn to_bytes_array(&self) -> [u8; 3] {
        match self {
            ServiceDiscoveryMessage::OfferService(id) => [0x01, (*id >> 8) as u8, *id as u8],
            ServiceDiscoveryMessage::StopOfferService(id) => [0x02, (*id >> 8) as u8, *id as u8],
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offer_service_to_bytes_and_from_bytes() {
        let msg = ServiceDiscoveryMessage::OfferService(0x1234);
        let bytes = msg.to_bytes_array();
        assert_eq!(bytes, [0x01, 0x12, 0x34]);
        let parsed = ServiceDiscoveryMessage::from_bytes(&bytes).unwrap();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_stop_offer_service_to_bytes_and_from_bytes() {
        let msg = ServiceDiscoveryMessage::StopOfferService(0xABCD);
        let bytes = msg.to_bytes_array();
        assert_eq!(bytes, [0x02, 0xAB, 0xCD]);
        let parsed = ServiceDiscoveryMessage::from_bytes(&bytes).unwrap();
        assert_eq!(parsed, msg);
    }

    #[test]
    fn test_from_bytes_invalid_type() {
        let bytes = vec![0xFF, 0x00, 0x01];
        assert!(ServiceDiscoveryMessage::from_bytes(&bytes).is_err());
    }

    #[test]
    fn test_from_bytes_too_short() {
        let bytes = vec![0x01];
        assert!(ServiceDiscoveryMessage::from_bytes(&bytes).is_err());
        let bytes = vec![0x01, 0x02];
        assert!(ServiceDiscoveryMessage::from_bytes(&bytes).is_err());
    }

    #[test]
    fn test_to_bytes_and_from_bytes_roundtrip() {
        let ids = [0x0000, 0x0001, 0x00FF, 0xFFFF];
        for &id in &ids {
            let offer = ServiceDiscoveryMessage::OfferService(id);
            let stop = ServiceDiscoveryMessage::StopOfferService(id);
            assert_eq!(
                ServiceDiscoveryMessage::from_bytes(&offer.to_bytes_array()).unwrap(),
                offer
            );
            assert_eq!(
                ServiceDiscoveryMessage::from_bytes(&stop.to_bytes_array()).unwrap(),
                stop
            );
        }
    }
}
