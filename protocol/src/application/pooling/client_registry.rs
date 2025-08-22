use std::net::IpAddr;

use anyhow::Result;
use crossbeam::channel::Sender;

use crate::application::{constants::MAX_CLIENTS_FIXED, message::ApplicationMessage};

/// Fixed-size client connection tracking for embedded devices
#[derive(Clone, Copy)]
struct ClientEntry {
    ip: IpAddr,
    sender_index: Option<usize>,
    active: bool,
}

impl Default for ClientEntry {
    fn default() -> Self {
        Self {
            ip: IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
            sender_index: None,
            active: false,
        }
    }
}

pub struct FixedClientRegistry {
    entries: [ClientEntry; MAX_CLIENTS_FIXED],
    senders: [Option<Sender<ApplicationMessage>>; MAX_CLIENTS_FIXED],
}

impl FixedClientRegistry {
    pub fn new() -> Self {
        Self {
            entries: [ClientEntry::default(); MAX_CLIENTS_FIXED],
            senders: std::array::from_fn(|_| None),
        }
    }

    pub fn add_client(&mut self, ip: IpAddr, sender: Sender<ApplicationMessage>) -> Result<()> {
        // Find existing entry or empty slot
        for (i, entry) in self.entries.iter_mut().enumerate() {
            if !entry.active || entry.ip == ip {
                entry.ip = ip;
                entry.sender_index = Some(i);
                entry.active = true;
                self.senders[i] = Some(sender);
                return Ok(());
            }
        }
        Err(anyhow::anyhow!("No available client slots"))
    }

    pub fn remove_client(&mut self, ip: IpAddr) {
        for (i, entry) in self.entries.iter_mut().enumerate() {
            if entry.active && entry.ip == ip {
                entry.active = false;
                self.senders[i] = None;
                break;
            }
        }
    }

    pub fn get_sender(&self, ip: IpAddr) -> Option<&Sender<ApplicationMessage>> {
        for entry in &self.entries {
            if entry.active && entry.ip == ip {
                if let Some(idx) = entry.sender_index {
                    return self.senders[idx].as_ref();
                }
            }
        }
        None
    }

    pub fn is_connected(&self, ip: IpAddr) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.active && entry.ip == ip)
    }

    pub fn len(&self) -> usize {
        self.entries.iter().filter(|entry| entry.active).count()
    }
}
