// Optimized constants for embedded devices - further reduced for minimal heap usage
pub const TEMP_BUFFER_SIZE: usize = 32; // Further reduced from 64 - temporary read buffer  
pub const MAX_BUFFER_GROWTH: usize = 256; // Further reduced to prevent buffer growth
pub const POLL_INTERVAL_MS: u64 = 5; // Reduced from 10ms for better responsiveness
pub const CHANNEL_CAPACITY: usize = 8; // Further reduced from 16 for memory efficiency
pub const CONNECT_TIMEOUT_MS: u64 = 100; // Reduced connection wait time
pub const DISTRIBUTOR_TIMEOUT_MS: u64 = 50; // Message distributor timeout
pub const MAX_PACKET_BUFFER_SIZE: usize = 512; // Fixed size for packet serialization buffer
pub const MAX_CLIENTS_FIXED: usize = 8; // Fixed maximum clients for embedded use

/// Constants for improved readability
pub const INACTIVE_READ_THRESHOLD: u8 = 10;
pub const INACTIVE_SLEEP_MULTIPLIER: u64 = 2;
