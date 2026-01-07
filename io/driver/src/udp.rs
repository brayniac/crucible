//! UDP socket helpers for ECN and control message handling.
//!
//! This module provides shared functionality for UDP sockets used by
//! both the io_uring and mio backends. It handles:
//!
//! - Socket configuration for ECN and pktinfo
//! - Control message (cmsg) parsing for received datagrams
//! - Control message building for sent datagrams
//! - Address conversion helpers

use crate::types::{Ecn, RecvMeta};
use std::io;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::unix::io::RawFd;

/// Size of control message buffer for recvmsg.
/// Needs space for IP_TOS/IPV6_TCLASS and IP_PKTINFO/IPV6_PKTINFO.
pub const CMSG_BUFFER_SIZE: usize = 128;

/// Configure a UDP socket for QUIC requirements.
///
/// Enables:
/// - `IP_RECVTOS` / `IPV6_RECVTCLASS` - receive ECN bits
/// - `IP_PKTINFO` / `IPV6_RECVPKTINFO` - receive destination address
pub fn configure_socket(fd: RawFd, addr: &SocketAddr) -> io::Result<()> {
    let on: libc::c_int = 1;

    unsafe {
        match addr {
            SocketAddr::V4(_) => {
                // Receive TOS (contains ECN bits)
                if libc::setsockopt(
                    fd,
                    libc::IPPROTO_IP,
                    libc::IP_RECVTOS,
                    &on as *const _ as *const libc::c_void,
                    mem::size_of_val(&on) as libc::socklen_t,
                ) < 0
                {
                    return Err(io::Error::last_os_error());
                }

                // Receive packet info (destination address)
                if libc::setsockopt(
                    fd,
                    libc::IPPROTO_IP,
                    libc::IP_PKTINFO,
                    &on as *const _ as *const libc::c_void,
                    mem::size_of_val(&on) as libc::socklen_t,
                ) < 0
                {
                    return Err(io::Error::last_os_error());
                }
            }
            SocketAddr::V6(_) => {
                // Receive traffic class (contains ECN bits)
                if libc::setsockopt(
                    fd,
                    libc::IPPROTO_IPV6,
                    libc::IPV6_RECVTCLASS,
                    &on as *const _ as *const libc::c_void,
                    mem::size_of_val(&on) as libc::socklen_t,
                ) < 0
                {
                    return Err(io::Error::last_os_error());
                }

                // Receive packet info (destination address)
                if libc::setsockopt(
                    fd,
                    libc::IPPROTO_IPV6,
                    libc::IPV6_RECVPKTINFO,
                    &on as *const _ as *const libc::c_void,
                    mem::size_of_val(&on) as libc::socklen_t,
                ) < 0
                {
                    return Err(io::Error::last_os_error());
                }

                // Allow both IPv4 and IPv6
                let off: libc::c_int = 0;
                // Ignore error - not all systems support this
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_IPV6,
                    libc::IPV6_V6ONLY,
                    &off as *const _ as *const libc::c_void,
                    mem::size_of_val(&off) as libc::socklen_t,
                );
            }
        }
    }

    Ok(())
}

/// Set the ECN codepoint for outgoing packets.
pub fn set_ecn(fd: RawFd, addr: &SocketAddr, ecn: Ecn) -> io::Result<()> {
    let tos = ecn.to_tos() as libc::c_int;

    unsafe {
        match addr {
            SocketAddr::V4(_) => {
                if libc::setsockopt(
                    fd,
                    libc::IPPROTO_IP,
                    libc::IP_TOS,
                    &tos as *const _ as *const libc::c_void,
                    mem::size_of_val(&tos) as libc::socklen_t,
                ) < 0
                {
                    return Err(io::Error::last_os_error());
                }
            }
            SocketAddr::V6(_) => {
                if libc::setsockopt(
                    fd,
                    libc::IPPROTO_IPV6,
                    libc::IPV6_TCLASS,
                    &tos as *const _ as *const libc::c_void,
                    mem::size_of_val(&tos) as libc::socklen_t,
                ) < 0
                {
                    return Err(io::Error::last_os_error());
                }
            }
        }
    }

    Ok(())
}

/// Parse control messages from a received datagram.
///
/// Extracts ECN codepoint and destination address from the msghdr.
pub fn parse_control_messages(msghdr: &libc::msghdr, source: SocketAddr, len: usize) -> RecvMeta {
    let mut ecn = Ecn::NotEct;
    let mut local: Option<SocketAddr> = None;

    // Iterate over control messages
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(msghdr);
        while !cmsg.is_null() {
            let cmsg_ref = &*cmsg;

            match (cmsg_ref.cmsg_level, cmsg_ref.cmsg_type) {
                // IPv4 TOS (contains ECN bits)
                (libc::IPPROTO_IP, libc::IP_TOS) => {
                    let tos = *(libc::CMSG_DATA(cmsg) as *const u8);
                    ecn = Ecn::from_tos(tos);
                }
                // IPv6 Traffic Class (contains ECN bits)
                (libc::IPPROTO_IPV6, libc::IPV6_TCLASS) => {
                    let tclass = *(libc::CMSG_DATA(cmsg) as *const libc::c_int);
                    ecn = Ecn::from_tos(tclass as u8);
                }
                // IPv4 packet info (destination address)
                (libc::IPPROTO_IP, libc::IP_PKTINFO) => {
                    let pktinfo = &*(libc::CMSG_DATA(cmsg) as *const libc::in_pktinfo);
                    let ip = Ipv4Addr::from(u32::from_be(pktinfo.ipi_addr.s_addr));
                    // We don't know the port from pktinfo, use the bound port
                    if let SocketAddr::V4(src) = source {
                        local = Some(SocketAddr::V4(SocketAddrV4::new(ip, src.port())));
                    }
                }
                // IPv6 packet info (destination address)
                (libc::IPPROTO_IPV6, libc::IPV6_PKTINFO) => {
                    let pktinfo = &*(libc::CMSG_DATA(cmsg) as *const libc::in6_pktinfo);
                    let ip = Ipv6Addr::from(pktinfo.ipi6_addr.s6_addr);
                    if let SocketAddr::V6(src) = source {
                        local = Some(SocketAddr::V6(SocketAddrV6::new(
                            ip,
                            src.port(),
                            0,
                            pktinfo.ipi6_ifindex,
                        )));
                    }
                }
                _ => {}
            }

            cmsg = libc::CMSG_NXTHDR(msghdr, cmsg);
        }
    }

    RecvMeta {
        source,
        local,
        ecn,
        len,
    }
}

/// Convert a libc sockaddr_storage to a Rust SocketAddr.
pub fn sockaddr_to_std(
    storage: &libc::sockaddr_storage,
    len: libc::socklen_t,
) -> io::Result<SocketAddr> {
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            if len < mem::size_of::<libc::sockaddr_in>() as libc::socklen_t {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "address too short",
                ));
            }
            let addr = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr));
            let port = u16::from_be(addr.sin_port);
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 => {
            if len < mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "address too short",
                ));
            }
            let addr = unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
            let ip = Ipv6Addr::from(addr.sin6_addr.s6_addr);
            let port = u16::from_be(addr.sin6_port);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                port,
                u32::from_be(addr.sin6_flowinfo),
                addr.sin6_scope_id,
            )))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported address family",
        )),
    }
}

/// Convert a Rust SocketAddr to libc sockaddr_storage.
pub fn std_to_sockaddr(addr: &SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };

    match addr {
        SocketAddr::V4(v4) => {
            let sockaddr = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in) };
            sockaddr.sin_family = libc::AF_INET as libc::sa_family_t;
            sockaddr.sin_port = v4.port().to_be();
            sockaddr.sin_addr.s_addr = u32::from(*v4.ip()).to_be();
            (
                storage,
                mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        }
        SocketAddr::V6(v6) => {
            let sockaddr = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in6) };
            sockaddr.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sockaddr.sin6_port = v6.port().to_be();
            sockaddr.sin6_flowinfo = v6.flowinfo().to_be();
            sockaddr.sin6_addr.s6_addr = v6.ip().octets();
            sockaddr.sin6_scope_id = v6.scope_id();
            (
                storage,
                mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ecn_from_tos() {
        assert_eq!(Ecn::from_tos(0b00), Ecn::NotEct);
        assert_eq!(Ecn::from_tos(0b01), Ecn::Ect1);
        assert_eq!(Ecn::from_tos(0b10), Ecn::Ect0);
        assert_eq!(Ecn::from_tos(0b11), Ecn::Ce);
        // Test with upper bits set
        assert_eq!(Ecn::from_tos(0b11111100), Ecn::NotEct);
        assert_eq!(Ecn::from_tos(0b11111101), Ecn::Ect1);
        assert_eq!(Ecn::from_tos(0b11111110), Ecn::Ect0);
        assert_eq!(Ecn::from_tos(0b11111111), Ecn::Ce);
    }

    #[test]
    fn test_ecn_to_tos() {
        assert_eq!(Ecn::NotEct.to_tos(), 0b00);
        assert_eq!(Ecn::Ect1.to_tos(), 0b01);
        assert_eq!(Ecn::Ect0.to_tos(), 0b10);
        assert_eq!(Ecn::Ce.to_tos(), 0b11);
    }

    #[test]
    fn test_sockaddr_roundtrip_v4() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let (storage, len) = std_to_sockaddr(&addr);
        let result = sockaddr_to_std(&storage, len).unwrap();
        assert_eq!(addr, result);
    }

    #[test]
    fn test_sockaddr_roundtrip_v6() {
        let addr: SocketAddr = "[::1]:8080".parse().unwrap();
        let (storage, len) = std_to_sockaddr(&addr);
        let result = sockaddr_to_std(&storage, len).unwrap();
        assert_eq!(addr, result);
    }
}
