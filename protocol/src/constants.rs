use std::net::{IpAddr, Ipv4Addr};

pub const DEFAULT_BIND_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
pub const DEFAULT_PORT: u16 = 8765;
pub const DEFAULT_TIMEOUT_MS: u64 = 1000;
pub const DEFAULT_RETRIES: u32 = 3;
pub const DEFAULT_RETRY_DELAY_MS: u64 = 200;
pub const MAX_PACKET_SIZE: usize = 1024;

pub const SD_REQUEST_PREFIX: u16 = 0xcafe;
pub const SD_RESPONSE_PREFIX: u16 = 0xbabe;