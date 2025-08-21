// ======
// REGISTRY
// ======

// Maximum number of services to track inside the service discovery module.
// This is a trade-off between memory usage and the number of services we can handle.
pub const SD_MAX_TRACKED_SERVICES: usize = 8;

// Service discovery packets are small (3 bytes), but allow some buffer for network overhead
// and future expansion.
pub const SD_RECV_BUFFER_SIZE: usize = 64;

// Polling interval for checking for new services or updates. This is used to prevent
// busy-waiting and allows the system to be more responsive to changes in the network.
pub const POLL_INTERVAL_MS: u64 = 10;

// Cleanup interval for removing stale services from the registry.
pub const CLEANUP_INTERVAL_MS: u64 = 1000;

// Reduced conflict check timeout for faster startup on embedded devices
pub const CONFLICT_CHECK_TIMEOUT_MS: u64 = 1000;
