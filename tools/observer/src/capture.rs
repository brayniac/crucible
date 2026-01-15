//! Packet capture and TCP stream extraction.
//!
//! This module handles:
//! - libpcap initialization and packet capture
//! - Ethernet/IP/TCP header parsing
//! - Direction detection (request vs response)

use pcap::{Capture, Device};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use thiserror::Error;

/// Errors that can occur during packet capture.
#[derive(Debug, Error)]
pub enum CaptureError {
    #[error("pcap error: {0}")]
    Pcap(#[from] pcap::Error),

    #[error("interface not found: {0}")]
    InterfaceNotFound(String),
}

/// Direction of data flow within a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Client to server (request)
    Request,
    /// Server to client (response)
    Response,
}

/// Identifies a TCP connection.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct ConnectionId {
    pub client: SocketAddr,
    pub server: SocketAddr,
}

/// A captured TCP segment with metadata.
#[derive(Debug)]
pub struct TcpSegment {
    pub conn_id: ConnectionId,
    pub direction: Direction,
    pub timestamp_ns: u64,
    pub payload: Vec<u8>,
    pub seq: u32,
    pub syn: bool,
    pub fin: bool,
    pub rst: bool,
}

/// Packet capture engine using libpcap.
pub struct PacketCapture {
    cap: Capture<pcap::Active>,
    server_port: u16,
}

impl PacketCapture {
    /// Create a new packet capture on the specified interface.
    pub fn new(
        interface: &str,
        server_port: u16,
        filter: Option<&str>,
    ) -> Result<Self, CaptureError> {
        // Find the device
        let devices = Device::list()?;
        let device = devices
            .into_iter()
            .find(|d| d.name == interface)
            .ok_or_else(|| CaptureError::InterfaceNotFound(interface.to_string()))?;

        // Open capture with promiscuous mode
        let mut cap = Capture::from_device(device)?
            .promisc(true)
            .snaplen(65535)
            .timeout(100) // 100ms timeout for non-blocking behavior
            .open()?;

        // Set BPF filter
        let default_filter = format!("tcp port {}", server_port);
        let filter_str = filter.unwrap_or(&default_filter);
        cap.filter(filter_str, true)?;

        Ok(Self { cap, server_port })
    }

    /// Capture the next packet and parse it into a TCP segment.
    ///
    /// Returns `None` if no packet is available (timeout) or if the packet
    /// is not a valid TCP packet with payload.
    pub fn next_packet(&mut self) -> Result<Option<TcpSegment>, CaptureError> {
        match self.cap.next_packet() {
            Ok(packet) => {
                // Copy packet data and timestamp before releasing borrow
                let data = packet.data.to_vec();
                let ts = packet.header.ts;
                self.parse_packet(&data, ts)
            }
            Err(pcap::Error::TimeoutExpired) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Parse a captured packet into a TCP segment.
    fn parse_packet(
        &self,
        data: &[u8],
        ts: libc::timeval,
    ) -> Result<Option<TcpSegment>, CaptureError> {
        // Parse Ethernet header (14 bytes)
        if data.len() < 14 {
            return Ok(None);
        }

        let ethertype = u16::from_be_bytes([data[12], data[13]]);
        let ip_offset = match ethertype {
            0x0800 => 14, // IPv4
            0x8100 => {
                // VLAN tagged
                if data.len() < 18 {
                    return Ok(None);
                }
                let inner_ethertype = u16::from_be_bytes([data[16], data[17]]);
                if inner_ethertype != 0x0800 {
                    return Ok(None); // Not IPv4
                }
                18
            }
            _ => return Ok(None), // Not IPv4
        };

        // Parse IPv4 header (minimum 20 bytes)
        if data.len() < ip_offset + 20 {
            return Ok(None);
        }

        let ip_data = &data[ip_offset..];
        let ip_version = (ip_data[0] >> 4) & 0x0F;
        if ip_version != 4 {
            return Ok(None);
        }

        let ip_header_len = ((ip_data[0] & 0x0F) as usize) * 4;
        if ip_header_len < 20 || data.len() < ip_offset + ip_header_len {
            return Ok(None);
        }

        let protocol = ip_data[9];
        if protocol != 6 {
            // Not TCP
            return Ok(None);
        }

        let src_ip = Ipv4Addr::new(ip_data[12], ip_data[13], ip_data[14], ip_data[15]);
        let dst_ip = Ipv4Addr::new(ip_data[16], ip_data[17], ip_data[18], ip_data[19]);

        // Parse TCP header (minimum 20 bytes)
        let tcp_offset = ip_offset + ip_header_len;
        if data.len() < tcp_offset + 20 {
            return Ok(None);
        }

        let tcp_data = &data[tcp_offset..];
        let src_port = u16::from_be_bytes([tcp_data[0], tcp_data[1]]);
        let dst_port = u16::from_be_bytes([tcp_data[2], tcp_data[3]]);
        let seq = u32::from_be_bytes([tcp_data[4], tcp_data[5], tcp_data[6], tcp_data[7]]);
        let tcp_header_len = (((tcp_data[12] >> 4) & 0x0F) as usize) * 4;
        let flags = tcp_data[13];
        let syn = (flags & 0x02) != 0;
        let fin = (flags & 0x01) != 0;
        let rst = (flags & 0x04) != 0;

        if tcp_header_len < 20 || data.len() < tcp_offset + tcp_header_len {
            return Ok(None);
        }

        // Extract payload
        let payload_offset = tcp_offset + tcp_header_len;
        let payload = if data.len() > payload_offset {
            data[payload_offset..].to_vec()
        } else {
            Vec::new()
        };

        // Skip packets with no payload unless they are SYN/FIN/RST
        if payload.is_empty() && !syn && !fin && !rst {
            return Ok(None);
        }

        // Determine direction based on server port
        let (direction, client, server) = if dst_port == self.server_port {
            (
                Direction::Request,
                SocketAddr::new(IpAddr::V4(src_ip), src_port),
                SocketAddr::new(IpAddr::V4(dst_ip), dst_port),
            )
        } else {
            (
                Direction::Response,
                SocketAddr::new(IpAddr::V4(dst_ip), dst_port),
                SocketAddr::new(IpAddr::V4(src_ip), src_port),
            )
        };

        // Calculate timestamp in nanoseconds
        let timestamp_ns = (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_usec as u64) * 1_000;

        Ok(Some(TcpSegment {
            conn_id: ConnectionId { client, server },
            direction,
            timestamp_ns,
            payload,
            seq,
            syn,
            fin,
            rst,
        }))
    }
}
