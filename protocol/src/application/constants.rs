use std::net::{IpAddr, Ipv4Addr};

pub const DEFAULT_BIND_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
pub const DEFAULT_PORT: u16 = 5678;