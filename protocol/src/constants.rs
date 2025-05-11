use std::{net::{IpAddr, Ipv4Addr}, sync::Arc};

use crate::packets::application::ApplicationResponseErrorMessage;

pub const DEFAULT_BIND_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
pub const DEFAULT_PORT: u16 = 8765;
pub const DEFAULT_TIMEOUT_MS: u64 = 1000;
pub const DEFAULT_RETRIES: u32 = 3;
pub const DEFAULT_RETRY_DELAY_MS: u64 = 200;
pub const MAX_PACKET_SIZE: usize = 1024;

pub const SD_REQUEST_PREFIX: u16 = 0x1234;
pub const SD_RESPONSE_PREFIX: u16 = 0x4321;
pub const APP_PREFIX: u16 = 0xbabe;

/// Callback type for handling method invocations.
pub type MethodInvokeCallback =
    Arc<dyn Fn(Vec<u8>) -> Result<Vec<u8>, ApplicationResponseErrorMessage> + Send + Sync>;

/// Callback type for handling method invocations.
pub type MethodResponseCallback =
    Arc<dyn Fn(Result<Vec<u8>, ApplicationResponseErrorMessage>) -> Result<Vec<u8>, ApplicationResponseErrorMessage> + Send + Sync>;