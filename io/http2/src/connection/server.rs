//! HTTP/2 server connection.
//!
//! This module implements the server-side HTTP/2 connection, handling:
//! - Connection preface validation from client
//! - Accepting incoming request streams
//! - Sending response headers and data

use super::{ConnectionError, ConnectionSettings, ConnectionState, FlowControl, Stream};
use crate::frame::{
    self, DataFrame, ErrorCode, Frame, FrameDecoder, FrameEncoder, GoAwayFrame, HeadersFrame,
    PingFrame, RstStreamFrame, Setting, SettingId, SettingsFrame, WindowUpdateFrame,
};
use crate::hpack::{HeaderField, HpackDecoder, HpackEncoder};

use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::io;

/// Events produced by the server connection.
#[derive(Debug)]
pub enum ServerEvent {
    /// Connection is ready to accept requests.
    Ready,
    /// New request stream opened by client.
    Request {
        stream_id: frame::StreamId,
        headers: Vec<HeaderField>,
        end_stream: bool,
    },
    /// Received data for a request stream.
    Data {
        stream_id: frame::StreamId,
        data: Bytes,
        end_stream: bool,
    },
    /// Stream was reset by client.
    StreamReset {
        stream_id: frame::StreamId,
        error_code: ErrorCode,
    },
    /// Client sent GOAWAY.
    GoAway {
        last_stream_id: frame::StreamId,
        error_code: ErrorCode,
    },
    /// Connection error occurred.
    Error(ConnectionError),
}

/// HTTP/2 server connection.
pub struct ServerConnection {
    /// Connection state.
    state: ConnectionState,
    /// Local settings (what we advertise).
    local_settings: ConnectionSettings,
    /// Remote settings (what client advertises).
    remote_settings: ConnectionSettings,
    /// Whether we've received the client's preface.
    got_preface: bool,
    /// Whether we've received the client's settings.
    got_settings: bool,
    /// Frame encoder.
    frame_encoder: FrameEncoder,
    /// Frame decoder.
    frame_decoder: FrameDecoder,
    /// HPACK encoder.
    hpack_encoder: HpackEncoder,
    /// HPACK decoder.
    hpack_decoder: HpackDecoder,
    /// Active streams.
    streams: HashMap<u32, Stream>,
    /// Connection-level flow control.
    flow_control: FlowControl,
    /// Buffer for encoding frames.
    write_buf: BytesMut,
    /// Buffer for incoming data.
    read_buf: BytesMut,
    /// Pending events.
    events: Vec<ServerEvent>,
    /// Highest stream ID received from client.
    last_client_stream_id: u32,
}

impl ServerConnection {
    /// Create a new HTTP/2 server connection.
    pub fn new() -> Self {
        Self {
            state: ConnectionState::WaitingPreface,
            local_settings: ConnectionSettings::default(),
            remote_settings: ConnectionSettings::default(),
            got_preface: false,
            got_settings: false,
            frame_encoder: FrameEncoder::new(),
            frame_decoder: FrameDecoder::new(),
            hpack_encoder: HpackEncoder::new(),
            hpack_decoder: HpackDecoder::new(),
            streams: HashMap::new(),
            flow_control: FlowControl::new(frame::DEFAULT_INITIAL_WINDOW_SIZE),
            write_buf: BytesMut::with_capacity(16384),
            read_buf: BytesMut::with_capacity(16384),
            events: Vec::new(),
            last_client_stream_id: 0,
        }
    }

    /// Get the connection state.
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// Check if the connection is ready for requests.
    pub fn is_ready(&self) -> bool {
        self.state == ConnectionState::Open
    }

    /// Feed data received from the client.
    pub fn on_recv(&mut self, data: &[u8]) {
        self.read_buf.extend_from_slice(data);
    }

    /// Feed plaintext data directly into the connection's frame buffer.
    ///
    /// Equivalent to `on_recv()` followed by `process()`, but named
    /// consistently with `Connection::feed_data()` for kompio integration.
    pub fn feed_data(&mut self, data: &[u8]) -> io::Result<()> {
        self.read_buf.extend_from_slice(data);
        self.process()
    }

    /// Process received data and return events.
    pub fn process(&mut self) -> io::Result<()> {
        // First, check for connection preface if we haven't seen it
        if !self.got_preface {
            if self.read_buf.len() >= frame::CONNECTION_PREFACE.len() {
                if &self.read_buf[..frame::CONNECTION_PREFACE.len()] == frame::CONNECTION_PREFACE {
                    let _ = self.read_buf.split_to(frame::CONNECTION_PREFACE.len());
                    self.got_preface = true;
                    // Send our settings
                    self.send_settings()?;
                } else {
                    self.events
                        .push(ServerEvent::Error(ConnectionError::Protocol(
                            "invalid connection preface".to_string(),
                        )));
                    return Ok(());
                }
            } else {
                // Need more data
                return Ok(());
            }
        }

        // Process frames
        self.process_frames()
    }

    /// Send server settings.
    fn send_settings(&mut self) -> io::Result<()> {
        let settings = vec![
            Setting {
                id: SettingId::MaxConcurrentStreams,
                value: self.local_settings.max_concurrent_streams,
            },
            Setting {
                id: SettingId::InitialWindowSize,
                value: self.local_settings.initial_window_size,
            },
            Setting {
                id: SettingId::MaxFrameSize,
                value: self.local_settings.max_frame_size,
            },
        ];

        let frame = SettingsFrame {
            ack: false,
            settings,
        };
        self.frame_encoder
            .encode(&Frame::Settings(frame), &mut self.write_buf);

        self.state = ConnectionState::WaitingSettings;
        Ok(())
    }

    /// Process complete frames in the read buffer.
    fn process_frames(&mut self) -> io::Result<()> {
        loop {
            match self.frame_decoder.decode(&mut self.read_buf) {
                Ok(Some(frame)) => {
                    self.handle_frame(frame)?;
                }
                Ok(None) => break, // Need more data
                Err(e) => {
                    self.events.push(ServerEvent::Error(e.into()));
                    break;
                }
            }
        }
        Ok(())
    }

    /// Handle a received frame.
    fn handle_frame(&mut self, frame: Frame) -> io::Result<()> {
        match frame {
            Frame::Settings(f) => self.handle_settings(f)?,
            Frame::Ping(f) => self.handle_ping(f)?,
            Frame::GoAway(f) => self.handle_goaway(f)?,
            Frame::WindowUpdate(f) => self.handle_window_update(f)?,
            Frame::Headers(f) => self.handle_headers(f)?,
            Frame::Data(f) => self.handle_data(f)?,
            Frame::RstStream(f) => self.handle_rst_stream(f)?,
            Frame::Priority(_) => {} // Ignore priority frames
            Frame::PushPromise(_) => {
                // Clients shouldn't send PUSH_PROMISE
                self.events
                    .push(ServerEvent::Error(ConnectionError::Protocol(
                        "unexpected PUSH_PROMISE from client".to_string(),
                    )));
            }
            Frame::Continuation(_) => {
                // Should be handled with preceding HEADERS frame
                self.events
                    .push(ServerEvent::Error(ConnectionError::Protocol(
                        "unexpected CONTINUATION frame".to_string(),
                    )));
            }
            Frame::Unknown(_) => {} // Ignore unknown frame types per spec
        }
        Ok(())
    }

    /// Handle SETTINGS frame.
    fn handle_settings(&mut self, frame: SettingsFrame) -> io::Result<()> {
        if frame.ack {
            // This is an ACK of our settings
            return Ok(());
        }

        // Apply settings
        for setting in &frame.settings {
            match setting.id {
                SettingId::HeaderTableSize => {
                    self.hpack_encoder.set_table_size(setting.value as usize);
                }
                SettingId::MaxConcurrentStreams => {
                    self.remote_settings.max_concurrent_streams = setting.value;
                }
                SettingId::InitialWindowSize => {
                    let delta =
                        setting.value as i32 - self.remote_settings.initial_window_size as i32;
                    self.remote_settings.initial_window_size = setting.value;

                    // Adjust all stream windows
                    for stream in self.streams.values_mut() {
                        stream.adjust_send_window(delta);
                    }
                }
                SettingId::MaxFrameSize => {
                    self.remote_settings.max_frame_size = setting.value;
                    self.frame_encoder.set_max_frame_size(setting.value);
                }
                SettingId::MaxHeaderListSize => {
                    self.remote_settings.max_header_list_size = setting.value;
                }
                _ => {} // Ignore unknown settings
            }
        }

        // Send ACK
        let ack = SettingsFrame {
            ack: true,
            settings: Vec::new(),
        };
        self.frame_encoder
            .encode(&Frame::Settings(ack), &mut self.write_buf);

        if !self.got_settings {
            self.got_settings = true;
            self.state = ConnectionState::Open;
            self.events.push(ServerEvent::Ready);
        }

        Ok(())
    }

    /// Handle PING frame.
    fn handle_ping(&mut self, frame: PingFrame) -> io::Result<()> {
        if frame.ack {
            // Response to our ping (we don't send pings as server)
            return Ok(());
        }

        // Send PING ACK
        let ack = PingFrame {
            ack: true,
            data: frame.data,
        };
        self.frame_encoder
            .encode(&Frame::Ping(ack), &mut self.write_buf);
        Ok(())
    }

    /// Handle GOAWAY frame.
    fn handle_goaway(&mut self, frame: GoAwayFrame) -> io::Result<()> {
        self.state = ConnectionState::Draining;
        self.events.push(ServerEvent::GoAway {
            last_stream_id: frame.last_stream_id,
            error_code: ErrorCode::from_u32(frame.error_code),
        });
        Ok(())
    }

    /// Handle WINDOW_UPDATE frame.
    fn handle_window_update(&mut self, frame: WindowUpdateFrame) -> io::Result<()> {
        if frame.stream_id.is_connection_level() {
            self.flow_control.increase_window(frame.increment);
        } else if let Some(stream) = self.streams.get_mut(&frame.stream_id.value()) {
            stream.increase_send_window(frame.increment);
        }
        Ok(())
    }

    /// Handle HEADERS frame (new request from client).
    fn handle_headers(&mut self, frame: HeadersFrame) -> io::Result<()> {
        let stream_id = frame.stream_id;

        // Validate stream ID (must be odd for client-initiated, greater than last)
        if stream_id.value().is_multiple_of(2) {
            self.events
                .push(ServerEvent::Error(ConnectionError::Protocol(
                    "client used even stream ID".to_string(),
                )));
            return Ok(());
        }

        if stream_id.value() <= self.last_client_stream_id {
            self.events
                .push(ServerEvent::Error(ConnectionError::Protocol(
                    "stream ID not greater than previous".to_string(),
                )));
            return Ok(());
        }

        self.last_client_stream_id = stream_id.value();

        // Decode HPACK headers
        let headers = match self.hpack_decoder.decode(&frame.header_block) {
            Ok(h) => h,
            Err(e) => {
                self.events
                    .push(ServerEvent::Error(ConnectionError::Hpack(format!(
                        "{:?}",
                        e
                    ))));
                return Ok(());
            }
        };

        // Create new stream
        let stream = Stream::new(stream_id, self.remote_settings.initial_window_size);
        self.streams.insert(stream_id.value(), stream);

        // Update stream state if end_stream
        if frame.end_stream
            && let Some(s) = self.streams.get_mut(&stream_id.value())
        {
            s.recv_end_stream();
        }

        self.events.push(ServerEvent::Request {
            stream_id,
            headers,
            end_stream: frame.end_stream,
        });

        Ok(())
    }

    /// Handle DATA frame.
    fn handle_data(&mut self, frame: DataFrame) -> io::Result<()> {
        let data_len = frame.data.len() as u32;

        // Update stream state
        if let Some(stream) = self.streams.get_mut(&frame.stream_id.value()) {
            stream.recv_data(data_len);
            if frame.end_stream {
                stream.recv_end_stream();
            }
        }

        // Update flow control and send WINDOW_UPDATE if needed
        self.flow_control.consume(data_len);
        if self.flow_control.should_update() {
            let increment = self.flow_control.pending_update();
            self.flow_control.reset_pending();

            let wu = WindowUpdateFrame {
                stream_id: frame::StreamId::CONNECTION,
                increment,
            };
            self.frame_encoder
                .encode(&Frame::WindowUpdate(wu), &mut self.write_buf);
        }

        self.events.push(ServerEvent::Data {
            stream_id: frame.stream_id,
            data: frame.data,
            end_stream: frame.end_stream,
        });

        Ok(())
    }

    /// Handle RST_STREAM frame.
    fn handle_rst_stream(&mut self, frame: RstStreamFrame) -> io::Result<()> {
        if let Some(stream) = self.streams.get_mut(&frame.stream_id.value()) {
            stream.reset();
        }

        self.events.push(ServerEvent::StreamReset {
            stream_id: frame.stream_id,
            error_code: ErrorCode::from_u32(frame.error_code),
        });

        Ok(())
    }

    /// Send response headers on a stream.
    pub fn send_headers(
        &mut self,
        stream_id: frame::StreamId,
        headers: &[HeaderField],
        end_stream: bool,
    ) -> io::Result<()> {
        // Verify stream exists
        if !self.streams.contains_key(&stream_id.value()) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "stream not found"));
        }

        // Encode headers with HPACK
        let mut header_block = Vec::new();
        self.hpack_encoder.encode(headers, &mut header_block);

        // Send HEADERS frame
        let frame = HeadersFrame {
            stream_id,
            end_stream,
            end_headers: true,
            priority: None,
            header_block: Bytes::from(header_block),
        };
        self.frame_encoder
            .encode(&Frame::Headers(frame), &mut self.write_buf);

        // Update stream state
        if end_stream && let Some(stream) = self.streams.get_mut(&stream_id.value()) {
            stream.send_end_stream();
        }

        Ok(())
    }

    /// Send data on a stream.
    pub fn send_data(
        &mut self,
        stream_id: frame::StreamId,
        data: &[u8],
        end_stream: bool,
    ) -> io::Result<usize> {
        // Check stream exists and get flow control info
        let (to_send, is_end) = {
            let stream = self
                .streams
                .get(&stream_id.value())
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "stream not found"))?;

            let available = std::cmp::min(
                self.flow_control.available() as usize,
                stream.send_window() as usize,
            );
            let to_send = std::cmp::min(available, data.len());
            let is_end = end_stream && to_send == data.len();

            (to_send, is_end)
        };

        if to_send == 0 && !data.is_empty() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }

        // Update flow control
        self.flow_control.consume(to_send as u32);

        // Update stream and send frame
        if let Some(stream) = self.streams.get_mut(&stream_id.value()) {
            stream.send_data(to_send as u32);
            if is_end {
                stream.send_end_stream();
            }
        }

        // Send DATA frame
        let frame = DataFrame {
            stream_id,
            end_stream: is_end,
            data: Bytes::copy_from_slice(&data[..to_send]),
        };
        self.frame_encoder
            .encode(&Frame::Data(frame), &mut self.write_buf);

        Ok(to_send)
    }

    /// Send a GOAWAY frame and start draining.
    pub fn send_goaway(&mut self, error_code: ErrorCode, debug_data: &[u8]) {
        let frame = GoAwayFrame {
            last_stream_id: frame::StreamId::new(self.last_client_stream_id),
            error_code: error_code.to_u32(),
            debug_data: Bytes::copy_from_slice(debug_data),
        };
        self.frame_encoder
            .encode(&Frame::GoAway(frame), &mut self.write_buf);
        self.state = ConnectionState::Draining;
    }

    /// Reset a stream.
    pub fn reset_stream(&mut self, stream_id: frame::StreamId, error_code: ErrorCode) {
        let frame = RstStreamFrame {
            stream_id,
            error_code: error_code.to_u32(),
        };
        self.frame_encoder
            .encode(&Frame::RstStream(frame), &mut self.write_buf);

        if let Some(stream) = self.streams.get_mut(&stream_id.value()) {
            stream.reset();
        }
    }

    /// Get pending events.
    pub fn poll_events(&mut self) -> Vec<ServerEvent> {
        std::mem::take(&mut self.events)
    }

    /// Get data to send to the client.
    pub fn pending_send(&self) -> &[u8] {
        &self.write_buf
    }

    /// Mark data as sent.
    pub fn advance_send(&mut self, n: usize) {
        let _ = self.write_buf.split_to(n);
    }

    /// Check if there's data to send.
    pub fn has_pending_send(&self) -> bool {
        !self.write_buf.is_empty()
    }

    /// Remove a closed stream.
    pub fn remove_stream(&mut self, stream_id: frame::StreamId) {
        self.streams.remove(&stream_id.value());
    }
}

impl Default for ServerConnection {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{FrameEncoder, SettingId};

    fn create_settings_frame(settings: &[Setting]) -> Vec<u8> {
        let encoder = FrameEncoder::new();
        let mut buf = BytesMut::new();
        let frame = SettingsFrame {
            ack: false,
            settings: settings.to_vec(),
        };
        encoder.encode(&Frame::Settings(frame), &mut buf);
        buf.to_vec()
    }

    fn create_headers_frame(stream_id: u32, headers: &[u8], end_stream: bool) -> Vec<u8> {
        let encoder = FrameEncoder::new();
        let mut buf = BytesMut::new();
        let frame = HeadersFrame {
            stream_id: frame::StreamId::new(stream_id),
            end_stream,
            end_headers: true,
            priority: None,
            header_block: Bytes::copy_from_slice(headers),
        };
        encoder.encode(&Frame::Headers(frame), &mut buf);
        buf.to_vec()
    }

    fn create_data_frame(stream_id: u32, data: &[u8], end_stream: bool) -> Vec<u8> {
        let encoder = FrameEncoder::new();
        let mut buf = BytesMut::new();
        let frame = DataFrame {
            stream_id: frame::StreamId::new(stream_id),
            end_stream,
            data: Bytes::copy_from_slice(data),
        };
        encoder.encode(&Frame::Data(frame), &mut buf);
        buf.to_vec()
    }

    fn create_ping_frame(data: [u8; 8], ack: bool) -> Vec<u8> {
        let encoder = FrameEncoder::new();
        let mut buf = BytesMut::new();
        let frame = PingFrame { ack, data };
        encoder.encode(&Frame::Ping(frame), &mut buf);
        buf.to_vec()
    }

    fn create_window_update_frame(stream_id: u32, increment: u32) -> Vec<u8> {
        let encoder = FrameEncoder::new();
        let mut buf = BytesMut::new();
        let frame = WindowUpdateFrame {
            stream_id: frame::StreamId::new(stream_id),
            increment,
        };
        encoder.encode(&Frame::WindowUpdate(frame), &mut buf);
        buf.to_vec()
    }

    fn create_rst_stream_frame(stream_id: u32, error_code: u32) -> Vec<u8> {
        let encoder = FrameEncoder::new();
        let mut buf = BytesMut::new();
        let frame = RstStreamFrame {
            stream_id: frame::StreamId::new(stream_id),
            error_code,
        };
        encoder.encode(&Frame::RstStream(frame), &mut buf);
        buf.to_vec()
    }

    fn create_goaway_frame(last_stream_id: u32, error_code: u32) -> Vec<u8> {
        let encoder = FrameEncoder::new();
        let mut buf = BytesMut::new();
        let frame = GoAwayFrame {
            last_stream_id: frame::StreamId::new(last_stream_id),
            error_code,
            debug_data: Bytes::new(),
        };
        encoder.encode(&Frame::GoAway(frame), &mut buf);
        buf.to_vec()
    }

    // Basic tests

    #[test]
    fn test_server_connection_new() {
        let conn = ServerConnection::new();
        assert_eq!(conn.state(), ConnectionState::WaitingPreface);
        assert!(!conn.is_ready());
    }

    #[test]
    fn test_server_connection_default() {
        let conn = ServerConnection::default();
        assert_eq!(conn.state(), ConnectionState::WaitingPreface);
    }

    #[test]
    fn test_preface_validation() {
        let mut conn = ServerConnection::new();

        // Feed valid preface
        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();

        assert_eq!(conn.state(), ConnectionState::WaitingSettings);
        assert!(conn.has_pending_send()); // Should have sent settings
    }

    #[test]
    fn test_invalid_preface() {
        let mut conn = ServerConnection::new();

        // Feed invalid preface
        conn.on_recv(b"INVALID PREFACE DATA!!!!!");
        conn.process().unwrap();

        let events = conn.poll_events();
        assert!(events.iter().any(|e| matches!(e, ServerEvent::Error(_))));
    }

    #[test]
    fn test_partial_preface() {
        let mut conn = ServerConnection::new();

        // Feed partial preface (not enough bytes)
        conn.on_recv(&frame::CONNECTION_PREFACE[..10]);
        conn.process().unwrap();

        // Should still be waiting for preface
        assert_eq!(conn.state(), ConnectionState::WaitingPreface);
        assert!(!conn.has_pending_send());
    }

    // Settings tests

    #[test]
    fn test_handle_settings() {
        let mut conn = ServerConnection::new();

        // Send preface
        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();

        // Consume the settings frame we sent
        let _ = conn.pending_send();
        conn.advance_send(conn.pending_send().len());

        // Send client settings
        let settings = create_settings_frame(&[Setting {
            id: SettingId::MaxConcurrentStreams,
            value: 100,
        }]);
        conn.on_recv(&settings);
        conn.process().unwrap();

        // Should be ready now
        assert_eq!(conn.state(), ConnectionState::Open);
        assert!(conn.is_ready());

        let events = conn.poll_events();
        assert!(events.iter().any(|e| matches!(e, ServerEvent::Ready)));
    }

    #[test]
    fn test_handle_settings_ack() {
        let mut conn = ServerConnection::new();

        // Send preface and settings
        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        // Send client settings first to get to Open state
        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();

        // Send settings ACK - should not change state
        let encoder = FrameEncoder::new();
        let mut buf = BytesMut::new();
        let ack = SettingsFrame {
            ack: true,
            settings: Vec::new(),
        };
        encoder.encode(&Frame::Settings(ack), &mut buf);
        conn.on_recv(&buf);
        conn.process().unwrap();

        assert!(conn.is_ready());
    }

    #[test]
    fn test_settings_updates_remote() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[
            Setting {
                id: SettingId::MaxConcurrentStreams,
                value: 50,
            },
            Setting {
                id: SettingId::InitialWindowSize,
                value: 32768,
            },
            Setting {
                id: SettingId::MaxFrameSize,
                value: 32768,
            },
            Setting {
                id: SettingId::MaxHeaderListSize,
                value: 8192,
            },
            Setting {
                id: SettingId::HeaderTableSize,
                value: 2048,
            },
        ]);
        conn.on_recv(&settings);
        conn.process().unwrap();

        assert_eq!(conn.remote_settings.max_concurrent_streams, 50);
        assert_eq!(conn.remote_settings.initial_window_size, 32768);
        assert_eq!(conn.remote_settings.max_frame_size, 32768);
        assert_eq!(conn.remote_settings.max_header_list_size, 8192);
    }

    // Ping tests

    #[test]
    fn test_handle_ping() {
        let mut conn = ServerConnection::new();

        // Setup connection
        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        // Send PING
        let ping = create_ping_frame([1, 2, 3, 4, 5, 6, 7, 8], false);
        conn.on_recv(&ping);
        conn.process().unwrap();

        // Should have sent PING ACK
        assert!(conn.has_pending_send());
    }

    #[test]
    fn test_handle_ping_ack() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        // Send PING ACK - should be ignored
        let ping_ack = create_ping_frame([1, 2, 3, 4, 5, 6, 7, 8], true);
        conn.on_recv(&ping_ack);
        conn.process().unwrap();

        // No response expected
        assert!(!conn.has_pending_send());
    }

    // GOAWAY tests

    #[test]
    fn test_handle_goaway() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        // Send GOAWAY
        let goaway = create_goaway_frame(0, 0);
        conn.on_recv(&goaway);
        conn.process().unwrap();

        assert_eq!(conn.state(), ConnectionState::Draining);

        let events = conn.poll_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ServerEvent::GoAway { .. }))
        );
    }

    // WINDOW_UPDATE tests

    #[test]
    fn test_handle_window_update_connection() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let initial_window = conn.flow_control.available();

        // Send connection-level WINDOW_UPDATE
        let wu = create_window_update_frame(0, 1000);
        conn.on_recv(&wu);
        conn.process().unwrap();

        assert_eq!(conn.flow_control.available(), initial_window + 1000);
    }

    #[test]
    fn test_handle_window_update_stream() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        // First create a stream via HEADERS
        let headers = create_headers_frame(1, &[0x82], false); // :method: GET
        conn.on_recv(&headers);
        conn.process().unwrap();

        // Now send stream-level WINDOW_UPDATE
        let wu = create_window_update_frame(1, 1000);
        conn.on_recv(&wu);
        conn.process().unwrap();

        // Stream window should be updated
        let stream = conn.streams.get(&1).unwrap();
        assert_eq!(
            stream.send_window(),
            conn.remote_settings.initial_window_size as i32 + 1000
        );
    }

    // HEADERS tests

    #[test]
    fn test_handle_headers_new_request() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());
        let _ = conn.poll_events();

        // Send HEADERS
        let headers = create_headers_frame(1, &[0x82, 0x84], false); // :method: GET, :path: /
        conn.on_recv(&headers);
        conn.process().unwrap();

        let events = conn.poll_events();
        assert!(events.iter().any(
            |e| matches!(e, ServerEvent::Request { stream_id, .. } if stream_id.value() == 1)
        ));

        assert!(conn.streams.contains_key(&1));
    }

    #[test]
    fn test_handle_headers_end_stream() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());
        let _ = conn.poll_events();

        // Send HEADERS with end_stream
        let headers = create_headers_frame(1, &[0x82], true);
        conn.on_recv(&headers);
        conn.process().unwrap();

        let events = conn.poll_events();
        assert!(events.iter().any(|e| matches!(
            e,
            ServerEvent::Request {
                end_stream: true,
                ..
            }
        )));
    }

    #[test]
    fn test_handle_headers_even_stream_id_error() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());
        let _ = conn.poll_events();

        // Send HEADERS with even stream ID (invalid for client)
        let headers = create_headers_frame(2, &[0x82], false);
        conn.on_recv(&headers);
        conn.process().unwrap();

        let events = conn.poll_events();
        assert!(events.iter().any(|e| matches!(e, ServerEvent::Error(_))));
    }

    #[test]
    fn test_handle_headers_stream_id_not_increasing() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());
        let _ = conn.poll_events();

        // Send first request on stream 3
        let headers1 = create_headers_frame(3, &[0x82], false);
        conn.on_recv(&headers1);
        conn.process().unwrap();
        let _ = conn.poll_events();

        // Try to send request on stream 1 (lower than 3)
        let headers2 = create_headers_frame(1, &[0x82], false);
        conn.on_recv(&headers2);
        conn.process().unwrap();

        let events = conn.poll_events();
        assert!(events.iter().any(|e| matches!(e, ServerEvent::Error(_))));
    }

    // DATA tests

    #[test]
    fn test_handle_data() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());
        let _ = conn.poll_events();

        // Create stream
        let headers = create_headers_frame(1, &[0x82], false);
        conn.on_recv(&headers);
        conn.process().unwrap();
        let _ = conn.poll_events();

        // Send DATA
        let data = create_data_frame(1, b"hello", false);
        conn.on_recv(&data);
        conn.process().unwrap();

        let events = conn.poll_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ServerEvent::Data { data, .. } if data.as_ref() == b"hello"))
        );
    }

    #[test]
    fn test_handle_data_end_stream() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());
        let _ = conn.poll_events();

        let headers = create_headers_frame(1, &[0x82], false);
        conn.on_recv(&headers);
        conn.process().unwrap();
        let _ = conn.poll_events();

        // Send DATA with end_stream
        let data = create_data_frame(1, b"body", true);
        conn.on_recv(&data);
        conn.process().unwrap();

        let events = conn.poll_events();
        assert!(events.iter().any(|e| matches!(
            e,
            ServerEvent::Data {
                end_stream: true,
                ..
            }
        )));
    }

    // RST_STREAM tests

    #[test]
    fn test_handle_rst_stream() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());
        let _ = conn.poll_events();

        // Create stream
        let headers = create_headers_frame(1, &[0x82], false);
        conn.on_recv(&headers);
        conn.process().unwrap();
        let _ = conn.poll_events();

        // Send RST_STREAM
        let rst = create_rst_stream_frame(1, 8); // CANCEL
        conn.on_recv(&rst);
        conn.process().unwrap();

        let events = conn.poll_events();
        assert!(events.iter().any(
            |e| matches!(e, ServerEvent::StreamReset { stream_id, .. } if stream_id.value() == 1)
        ));
    }

    // Send methods tests

    #[test]
    fn test_send_headers() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());
        let _ = conn.poll_events();

        // Create stream
        let headers = create_headers_frame(1, &[0x82], false);
        conn.on_recv(&headers);
        conn.process().unwrap();
        let _ = conn.poll_events();
        conn.advance_send(conn.pending_send().len());

        // Send response headers
        let response_headers = vec![HeaderField::new(b":status".to_vec(), b"200".to_vec())];
        conn.send_headers(frame::StreamId::new(1), &response_headers, false)
            .unwrap();

        assert!(conn.has_pending_send());
    }

    #[test]
    fn test_send_headers_nonexistent_stream() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        // Try to send on nonexistent stream
        let result = conn.send_headers(
            frame::StreamId::new(99),
            &[HeaderField::new(b":status".to_vec(), b"200".to_vec())],
            false,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_send_data() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());
        let _ = conn.poll_events();

        let headers = create_headers_frame(1, &[0x82], false);
        conn.on_recv(&headers);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        // Send data
        let sent = conn
            .send_data(frame::StreamId::new(1), b"response body", false)
            .unwrap();

        assert_eq!(sent, 13);
        assert!(conn.has_pending_send());
    }

    #[test]
    fn test_send_data_nonexistent_stream() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let result = conn.send_data(frame::StreamId::new(99), b"data", false);
        assert!(result.is_err());
    }

    #[test]
    fn test_send_goaway() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        conn.send_goaway(ErrorCode::NoError, b"goodbye");

        assert_eq!(conn.state(), ConnectionState::Draining);
        assert!(conn.has_pending_send());
    }

    #[test]
    fn test_reset_stream() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());
        let _ = conn.poll_events();

        let headers = create_headers_frame(1, &[0x82], false);
        conn.on_recv(&headers);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        conn.reset_stream(frame::StreamId::new(1), ErrorCode::Cancel);

        assert!(conn.has_pending_send());
    }

    // Helper method tests

    #[test]
    fn test_poll_events() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();

        let events = conn.poll_events();
        assert!(!events.is_empty());

        // Second call should return empty
        let events2 = conn.poll_events();
        assert!(events2.is_empty());
    }

    #[test]
    fn test_pending_send_and_advance() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();

        let pending = conn.pending_send();
        assert!(!pending.is_empty());

        let len = pending.len();
        conn.advance_send(len);

        assert!(!conn.has_pending_send());
    }

    #[test]
    fn test_remove_stream() {
        let mut conn = ServerConnection::new();

        conn.on_recv(frame::CONNECTION_PREFACE);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());

        let settings = create_settings_frame(&[]);
        conn.on_recv(&settings);
        conn.process().unwrap();
        conn.advance_send(conn.pending_send().len());
        let _ = conn.poll_events();

        let headers = create_headers_frame(1, &[0x82], false);
        conn.on_recv(&headers);
        conn.process().unwrap();

        assert!(conn.streams.contains_key(&1));

        conn.remove_stream(frame::StreamId::new(1));

        assert!(!conn.streams.contains_key(&1));
    }

    // ServerEvent debug tests

    #[test]
    fn test_server_event_debug() {
        let event = ServerEvent::Ready;
        let debug = format!("{:?}", event);
        assert!(debug.contains("Ready"));

        let event = ServerEvent::Request {
            stream_id: frame::StreamId::new(1),
            headers: vec![],
            end_stream: false,
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("Request"));

        let event = ServerEvent::Data {
            stream_id: frame::StreamId::new(1),
            data: Bytes::from_static(b"test"),
            end_stream: true,
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("Data"));
    }
}
