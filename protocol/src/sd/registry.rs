use std::{
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
}

impl Default for ServiceEntry {
    fn default() -> Self {
        Self {
            service_id: 0,
            ip: IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
            last_seen: Instant::now(),
            active: false,
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
                entry.ip = ip;
                entry.last_seen = Instant::now();
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
                };
                return Ok(());
            }
        }

        return Err(anyhow::anyhow!(
            "Service registry is full, cannot insert new service"
        ));
    }

    pub fn remove(&mut self, service_id: u16) {
        for entry in self.entries.iter_mut() {
            if entry.active && entry.service_id == service_id {
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
            if entry.active && now.duration_since(entry.last_seen) > ttl {
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
}
