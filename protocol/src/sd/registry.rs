use std::{
    collections::HashMap,
    net::IpAddr,
    time::{Duration, Instant},
};

use anyhow::Result;

use crate::sd::constants::SD_MAX_TRACKED_SERVICES;

#[derive(Debug, Clone, Copy)]
pub struct ServiceEntry {
    service_id: u16,
    ip: IpAddr,
    last_seen: Instant,
    active: bool,
    /// Whether this is a static entry that should never be removed
    is_static: bool,
}

impl Default for ServiceEntry {
    fn default() -> Self {
        Self {
            service_id: 0,
            ip: IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
            last_seen: Instant::now(),
            active: false,
            is_static: false,
        }
    }
}

#[derive(Debug)]
pub struct ServiceRegistry {
    entries: [ServiceEntry; SD_MAX_TRACKED_SERVICES],
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            entries: [ServiceEntry::default(); SD_MAX_TRACKED_SERVICES],
        }
    }

    pub fn insert(&mut self, service_id: u16, ip: IpAddr) -> Result<()> {
        // First, try to find existing entry for this service
        for entry in self.entries.iter_mut() {
            if entry.active && entry.service_id == service_id {
                // Don't update static entries from network discovery
                if !entry.is_static {
                    entry.ip = ip;
                    entry.last_seen = Instant::now();
                }
                return Ok(());
            }
        }

        // If not found, try to find an inactive slot
        for entry in self.entries.iter_mut() {
            if !entry.active {
                *entry = ServiceEntry {
                    service_id,
                    ip,
                    last_seen: Instant::now(),
                    active: true,
                    is_static: false,
                };
                return Ok(());
            }
        }

        Err(anyhow::anyhow!(
            "Service registry is full, cannot insert new service"
        ))
    }

    pub fn remove(&mut self, service_id: u16) {
        for entry in self.entries.iter_mut() {
            if entry.active && entry.service_id == service_id && !entry.is_static {
                entry.active = false;
                break;
            }
        }
    }

    /// Insert a static service entry that won't be removed by cleanup or network updates
    pub fn insert_static(&mut self, service_id: u16, ip: IpAddr) -> Result<()> {
        // First, try to find existing entry for this service
        for entry in self.entries.iter_mut() {
            if entry.active && entry.service_id == service_id {
                // Update existing entry to be static
                entry.ip = ip;
                entry.last_seen = Instant::now();
                entry.is_static = true;
                return Ok(());
            }
        }

        // If not found, try to find an inactive slot
        for entry in self.entries.iter_mut() {
            if !entry.active {
                *entry = ServiceEntry {
                    service_id,
                    ip,
                    last_seen: Instant::now(),
                    active: true,
                    is_static: true,
                };
                return Ok(());
            }
        }

        Err(anyhow::anyhow!(
            "Service registry is full, cannot insert new static service"
        ))
    }

    /// Remove a static service entry
    pub fn remove_static(&mut self, service_id: u16) {
        for entry in self.entries.iter_mut() {
            if entry.active && entry.service_id == service_id && entry.is_static {
                entry.active = false;
                break;
            }
        }
    }

    pub fn find(&self, service_id: u16) -> Option<IpAddr> {
        for entry in self.entries.iter() {
            if entry.active && entry.service_id == service_id {
                return Some(entry.ip);
            }
        }
        None
    }

    pub fn cleanup_stale(&mut self, ttl: Duration) {
        let now = Instant::now();
        for entry in self.entries.iter_mut() {
            // Only clean up non-static entries that have exceeded TTL
            if entry.active && !entry.is_static && now.duration_since(entry.last_seen) > ttl {
                entry.active = false;
            }
        }
    }

    pub fn get_active_services(&self) -> Vec<(u16, IpAddr)> {
        self.entries
            .iter()
            .filter(|entry| entry.active)
            .map(|entry| (entry.service_id, entry.ip))
            .collect()
    }

    /// Load static services from a HashMap into the registry
    pub fn load_static_services(&mut self, static_services: &HashMap<u16, IpAddr>) -> Result<()> {
        for (&service_id, &ip_addr) in static_services.iter() {
            if let Err(e) = self.insert_static(service_id, ip_addr) {
                // Log error but continue loading other services
                log::warn!(
                    "Failed to load static service {} -> {}: {}",
                    service_id,
                    ip_addr,
                    e
                );
            }
        }
        Ok(())
    }
}
