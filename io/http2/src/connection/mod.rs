//! HTTP/2 connection state machine.
//!
//! This module implements the HTTP/2 connection layer, handling:
//! - Connection preface and settings exchange
//! - Stream lifecycle management
//! - Flow control (connection and stream level)
//! - Frame dispatching

mod flow_control;
mod server;
mod settings;
mod stream;

pub use flow_control::FlowControl;
pub use server::{ServerConnection, ServerEvent};
pub use settings::ConnectionSettings;
pub use stream::{Stream, StreamId, StreamState};

use crate::frame::{
    self, DataFrame, ErrorCode, Frame, FrameDecoder, FrameEncoder, FrameError, GoAwayFrame,
    HeadersFrame, PingFrame, RstStreamFrame, Setting, SettingId, SettingsFrame, WindowUpdateFrame,
};
use crate::hpack::{HeaderField, HpackDecoder, HpackEncoder};
use crate::transport::Transport;

use bytes::BytesMut;
use std::collections::HashMap;
use std::io;

/// Connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Waiting to send connection preface.
    WaitingPreface,
    /// Sent preface, waiting for server settings.
    WaitingSettings,
    /// Connection is open and ready.
    Open,
    /// Received GOAWAY, draining existing streams.
    Draining,
    /// Connection is closed.
    Closed,
}

/// Events produced by the connection.
#[derive(Debug)]
pub enum ConnectionEvent {
    /// Connection is ready for requests.
    Ready,
    /// Received headers for a stream.
    Headers {
        stream_id: frame::StreamId,
        headers: Vec<HeaderField>,
        end_stream: bool,
    },
    /// Received data for a stream.
    Data {
        stream_id: frame::StreamId,
        data: bytes::Bytes,
        end_stream: bool,
    },
    /// Stream was reset by peer.
    StreamReset {
        stream_id: frame::StreamId,
        error_code: ErrorCode,
    },
    /// Received GOAWAY from server.
    GoAway {
        last_stream_id: frame::StreamId,
        error_code: ErrorCode,
    },
    /// Connection error occurred.
    Error(ConnectionError),
}

/// Connection-level error.
#[derive(Debug)]
pub enum ConnectionError {
    /// Frame parsing error.
    Frame(FrameError),
    /// Protocol error.
    Protocol(String),
    /// I/O error.
    Io(io::Error),
    /// HPACK error.
    Hpack(String),
}

impl From<FrameError> for ConnectionError {
    fn from(e: FrameError) -> Self {
        ConnectionError::Frame(e)
    }
}

impl From<io::Error> for ConnectionError {
    fn from(e: io::Error) -> Self {
        ConnectionError::Io(e)
    }
}

/// HTTP/2 client connection.
pub struct Connection<T: Transport> {
    /// The underlying transport.
    transport: T,
    /// Connection state.
    state: ConnectionState,
    /// Local settings (what we advertise).
    local_settings: ConnectionSettings,
    /// Remote settings (what server advertises).
    remote_settings: ConnectionSettings,
    /// Whether we've received the server's initial settings.
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
    /// Next stream ID to use (client uses odd numbers).
    next_stream_id: u32,
    /// Connection-level flow control.
    flow_control: FlowControl,
    /// Buffer for encoding frames.
    write_buf: BytesMut,
    /// Buffer for incoming data.
    read_buf: BytesMut,
    /// Pending events.
    events: Vec<ConnectionEvent>,
    /// Last stream ID we initiated.
    last_stream_id: u32,
}

impl<T: Transport> Connection<T> {
    /// Create a new HTTP/2 client connection.
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            state: ConnectionState::WaitingPreface,
            local_settings: ConnectionSettings::default(),
            remote_settings: ConnectionSettings::default(),
            got_settings: false,
            frame_encoder: FrameEncoder::new(),
            frame_decoder: FrameDecoder::new(),
            hpack_encoder: HpackEncoder::new(),
            hpack_decoder: HpackDecoder::new(),
            streams: HashMap::new(),
            next_stream_id: 1, // Client uses odd stream IDs
            flow_control: FlowControl::new(frame::DEFAULT_INITIAL_WINDOW_SIZE),
            write_buf: BytesMut::with_capacity(16384),
            read_buf: BytesMut::with_capacity(16384),
            events: Vec::new(),
            last_stream_id: 0,
        }
    }

    /// Get the connection state.
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// Check if the connection is ready for requests.
    pub fn is_ready(&self) -> bool {
        self.state == ConnectionState::Open && self.transport.is_ready()
    }

    /// Process transport readiness and advance connection state.
    ///
    /// Call this after the transport becomes ready (e.g., TLS handshake completes).
    pub fn on_transport_ready(&mut self) -> io::Result<()> {
        if self.state == ConnectionState::WaitingPreface {
            self.send_preface()?;
        }
        Ok(())
    }

    /// Send the HTTP/2 connection preface.
    fn send_preface(&mut self) -> io::Result<()> {
        // Send magic string
        self.write_buf.extend_from_slice(frame::CONNECTION_PREFACE);

        // Send our settings
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
        self.flush_write_buffer()?;

        Ok(())
    }

    /// Flush the write buffer to the transport.
    fn flush_write_buffer(&mut self) -> io::Result<()> {
        if !self.write_buf.is_empty() {
            let n = self.transport.send(&self.write_buf)?;
            let _ = self.write_buf.split_to(n);
        }
        Ok(())
    }

    /// Feed data received from the transport.
    pub fn on_recv(&mut self, data: &[u8]) -> io::Result<()> {
        self.transport.on_recv(data)?;
        self.process_transport_data()
    }

    /// Feed plaintext data directly into the connection's frame buffer.
    ///
    /// More efficient than `on_recv()` when TLS is handled externally (e.g.,
    /// by kompio's native TLS). Skips the Transport recv buffer entirely.
    pub fn feed_data(&mut self, data: &[u8]) -> io::Result<()> {
        self.read_buf.extend_from_slice(data);
        self.process_frames()
    }

    /// Process data from the transport.
    fn process_transport_data(&mut self) -> io::Result<()> {
        // Read decrypted data from transport
        let mut buf = [0u8; 16384];
        loop {
            match self.transport.recv(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    self.read_buf.extend_from_slice(&buf[..n]);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        // Process frames
        self.process_frames()
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
                    self.events.push(ConnectionEvent::Error(e.into()));
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
            Frame::Priority(_) => {} // We can ignore priority frames
            Frame::PushPromise(_) => {
                // Clients should disable server push; ignore for now
            }
            Frame::Continuation(_) => {
                // Should be handled with preceding HEADERS frame
                // For now, protocol error
                self.events
                    .push(ConnectionEvent::Error(ConnectionError::Protocol(
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
            self.events.push(ConnectionEvent::Ready);
        }

        self.flush_write_buffer()
    }

    /// Handle PING frame.
    fn handle_ping(&mut self, frame: PingFrame) -> io::Result<()> {
        if frame.ack {
            // Response to our ping
            return Ok(());
        }

        // Send PING ACK
        let ack = PingFrame {
            ack: true,
            data: frame.data,
        };
        self.frame_encoder
            .encode(&Frame::Ping(ack), &mut self.write_buf);
        self.flush_write_buffer()
    }

    /// Handle GOAWAY frame.
    fn handle_goaway(&mut self, frame: GoAwayFrame) -> io::Result<()> {
        self.state = ConnectionState::Draining;
        self.events.push(ConnectionEvent::GoAway {
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

    /// Handle HEADERS frame.
    fn handle_headers(&mut self, frame: HeadersFrame) -> io::Result<()> {
        // Decode HPACK headers
        let headers = match self.hpack_decoder.decode(&frame.header_block) {
            Ok(h) => h,
            Err(e) => {
                self.events
                    .push(ConnectionEvent::Error(ConnectionError::Hpack(format!(
                        "{:?}",
                        e
                    ))));
                return Ok(());
            }
        };

        // Update stream state
        if let Some(stream) = self.streams.get_mut(&frame.stream_id.value())
            && frame.end_stream
        {
            stream.recv_end_stream();
        }

        self.events.push(ConnectionEvent::Headers {
            stream_id: frame.stream_id,
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
            self.flush_write_buffer()?;
        }

        self.events.push(ConnectionEvent::Data {
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

        self.events.push(ConnectionEvent::StreamReset {
            stream_id: frame.stream_id,
            error_code: ErrorCode::from_u32(frame.error_code),
        });

        Ok(())
    }

    /// Start a new request stream.
    ///
    /// Returns the stream ID for the new stream.
    pub fn start_request(
        &mut self,
        headers: &[HeaderField],
        end_stream: bool,
    ) -> io::Result<frame::StreamId> {
        if self.state != ConnectionState::Open {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "connection not ready",
            ));
        }

        let stream_id = frame::StreamId::new(self.next_stream_id);
        self.next_stream_id += 2; // Client uses odd stream IDs
        self.last_stream_id = stream_id.value();

        // Create stream
        let stream = Stream::new(stream_id, self.remote_settings.initial_window_size);
        self.streams.insert(stream_id.value(), stream);

        // Encode headers with HPACK
        let mut header_block = Vec::new();
        self.hpack_encoder.encode(headers, &mut header_block);

        // Send HEADERS frame
        let frame = HeadersFrame {
            stream_id,
            end_stream,
            end_headers: true, // For simplicity, we always send all headers in one frame
            priority: None,
            header_block: bytes::Bytes::from(header_block),
        };
        self.frame_encoder
            .encode(&Frame::Headers(frame), &mut self.write_buf);
        self.flush_write_buffer()?;

        Ok(stream_id)
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
            let max_frame = self.frame_encoder.max_frame_size() as usize;
            let to_send = data.len().min(available).min(max_frame);
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
            data: bytes::Bytes::copy_from_slice(&data[..to_send]),
        };
        self.frame_encoder
            .encode(&Frame::Data(frame), &mut self.write_buf);
        self.flush_write_buffer()?;

        Ok(to_send)
    }

    /// Get pending events.
    pub fn poll_events(&mut self) -> Vec<ConnectionEvent> {
        std::mem::take(&mut self.events)
    }

    /// Get data to send on the socket.
    pub fn pending_send(&self) -> &[u8] {
        self.transport.pending_send()
    }

    /// Mark data as sent on the socket.
    pub fn advance_send(&mut self, n: usize) {
        self.transport.advance_send(n);
    }

    /// Check if there's data to send.
    pub fn has_pending_send(&self) -> bool {
        self.transport.has_pending_send() || !self.write_buf.is_empty()
    }

    /// Get the underlying transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Get mutable access to the underlying transport.
    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportState;

    // ConnectionState tests

    #[test]
    fn test_connection_state_debug() {
        let states = [
            ConnectionState::WaitingPreface,
            ConnectionState::WaitingSettings,
            ConnectionState::Open,
            ConnectionState::Draining,
            ConnectionState::Closed,
        ];

        for state in states {
            let debug = format!("{:?}", state);
            assert!(!debug.is_empty());
        }
    }

    #[test]
    fn test_connection_state_eq() {
        assert_eq!(ConnectionState::Open, ConnectionState::Open);
        assert_ne!(ConnectionState::Open, ConnectionState::Closed);
    }

    #[test]
    fn test_connection_state_clone() {
        let state = ConnectionState::Open;
        let cloned = state;
        assert_eq!(state, cloned);
    }

    // ConnectionEvent tests

    #[test]
    fn test_connection_event_ready_debug() {
        let event = ConnectionEvent::Ready;
        let debug = format!("{:?}", event);
        assert!(debug.contains("Ready"));
    }

    #[test]
    fn test_connection_event_headers_debug() {
        let event = ConnectionEvent::Headers {
            stream_id: frame::StreamId::new(1),
            headers: vec![],
            end_stream: false,
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("Headers"));
    }

    #[test]
    fn test_connection_event_data_debug() {
        let event = ConnectionEvent::Data {
            stream_id: frame::StreamId::new(1),
            data: bytes::Bytes::from_static(b"test"),
            end_stream: true,
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("Data"));
    }

    #[test]
    fn test_connection_event_stream_reset_debug() {
        let event = ConnectionEvent::StreamReset {
            stream_id: frame::StreamId::new(1),
            error_code: ErrorCode::Cancel,
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("StreamReset"));
    }

    #[test]
    fn test_connection_event_goaway_debug() {
        let event = ConnectionEvent::GoAway {
            last_stream_id: frame::StreamId::new(0),
            error_code: ErrorCode::NoError,
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("GoAway"));
    }

    #[test]
    fn test_connection_event_error_debug() {
        let event = ConnectionEvent::Error(ConnectionError::Protocol("test".to_string()));
        let debug = format!("{:?}", event);
        assert!(debug.contains("Error"));
    }

    // ConnectionError tests

    #[test]
    fn test_connection_error_frame_debug() {
        let err = ConnectionError::Frame(FrameError::Incomplete);
        let debug = format!("{:?}", err);
        assert!(debug.contains("Frame"));
    }

    #[test]
    fn test_connection_error_protocol_debug() {
        let err = ConnectionError::Protocol("test error".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Protocol"));
        assert!(debug.contains("test error"));
    }

    #[test]
    fn test_connection_error_io_debug() {
        let err = ConnectionError::Io(io::Error::other("io error"));
        let debug = format!("{:?}", err);
        assert!(debug.contains("Io"));
    }

    #[test]
    fn test_connection_error_hpack_debug() {
        let err = ConnectionError::Hpack("hpack error".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Hpack"));
    }

    #[test]
    fn test_connection_error_from_frame_error() {
        let frame_err = FrameError::Incomplete;
        let conn_err: ConnectionError = frame_err.into();
        assert!(matches!(conn_err, ConnectionError::Frame(_)));
    }

    #[test]
    fn test_connection_error_from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionReset, "reset");
        let conn_err: ConnectionError = io_err.into();
        assert!(matches!(conn_err, ConnectionError::Io(_)));
    }

    // Mock Transport for testing Connection

    struct MockTransport {
        ready: bool,
        recv_data: Vec<u8>,
        send_data: Vec<u8>,
        recv_pos: usize,
    }

    impl MockTransport {
        fn new() -> Self {
            Self {
                ready: true,
                recv_data: Vec::new(),
                send_data: Vec::new(),
                recv_pos: 0,
            }
        }
    }

    impl Transport for MockTransport {
        fn state(&self) -> TransportState {
            if self.ready {
                TransportState::Ready
            } else {
                TransportState::Handshaking
            }
        }

        fn is_ready(&self) -> bool {
            self.ready
        }

        fn on_recv(&mut self, data: &[u8]) -> io::Result<()> {
            self.recv_data.extend_from_slice(data);
            Ok(())
        }

        fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.recv_pos >= self.recv_data.len() {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            let remaining = &self.recv_data[self.recv_pos..];
            let to_copy = std::cmp::min(buf.len(), remaining.len());
            buf[..to_copy].copy_from_slice(&remaining[..to_copy]);
            self.recv_pos += to_copy;
            Ok(to_copy)
        }

        fn send(&mut self, data: &[u8]) -> io::Result<usize> {
            self.send_data.extend_from_slice(data);
            Ok(data.len())
        }

        fn pending_send(&self) -> &[u8] {
            &[]
        }

        fn advance_send(&mut self, _n: usize) {}

        fn has_pending_send(&self) -> bool {
            false
        }

        fn shutdown(&mut self) -> io::Result<()> {
            self.ready = false;
            Ok(())
        }
    }

    // Connection tests with mock transport

    #[test]
    fn test_connection_new() {
        let transport = MockTransport::new();
        let conn = Connection::new(transport);

        assert_eq!(conn.state(), ConnectionState::WaitingPreface);
        assert!(!conn.is_ready());
    }

    #[test]
    fn test_connection_on_transport_ready() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);

        conn.on_transport_ready().unwrap();

        assert_eq!(conn.state(), ConnectionState::WaitingSettings);
    }

    #[test]
    fn test_connection_on_transport_ready_not_waiting_preface() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);

        // First call to move to WaitingSettings
        conn.on_transport_ready().unwrap();
        assert_eq!(conn.state(), ConnectionState::WaitingSettings);

        // Second call should be a no-op
        conn.on_transport_ready().unwrap();
        assert_eq!(conn.state(), ConnectionState::WaitingSettings);
    }

    #[test]
    fn test_connection_poll_events_empty() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);

        let events = conn.poll_events();
        assert!(events.is_empty());
    }

    #[test]
    fn test_connection_transport_access() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);

        // Test immutable access
        let _ = conn.transport();

        // Test mutable access
        conn.transport_mut().ready = false;
        assert!(!conn.transport().is_ready());
    }

    #[test]
    fn test_connection_has_pending_send() {
        let transport = MockTransport::new();
        let conn = Connection::new(transport);

        // Initially no pending send
        assert!(!conn.has_pending_send());
    }

    #[test]
    fn test_connection_start_request_not_ready() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);

        // Should fail because connection is not open
        let result = conn.start_request(
            &[HeaderField::new(b":method".to_vec(), b"GET".to_vec())],
            false,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_connection_send_data_stream_not_found() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);

        // Move to open state by simulating the handshake
        conn.state = ConnectionState::Open;

        // Try to send on non-existent stream
        let result = conn.send_data(frame::StreamId::new(1), b"test", false);
        assert!(result.is_err());
    }

    // Test connection handles settings frame

    #[test]
    fn test_connection_handle_settings_transitions_to_open() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);

        // Send preface
        conn.on_transport_ready().unwrap();
        assert_eq!(conn.state(), ConnectionState::WaitingSettings);

        // Create and feed a settings frame
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = SettingsFrame {
            ack: false,
            settings: vec![],
        };
        encoder.encode(&Frame::Settings(frame), &mut buf);

        // Feed settings directly to read buffer and process
        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Should now be open
        assert_eq!(conn.state(), ConnectionState::Open);

        // Should have Ready event
        let events = conn.poll_events();
        assert!(events.iter().any(|e| matches!(e, ConnectionEvent::Ready)));
    }

    #[test]
    fn test_connection_handle_ping() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create and feed a ping frame
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = PingFrame {
            ack: false,
            data: [1, 2, 3, 4, 5, 6, 7, 8],
        };
        encoder.encode(&Frame::Ping(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Should have sent a ping ACK (check transport's send buffer)
        assert!(!conn.transport().send_data.is_empty());
    }

    #[test]
    fn test_connection_handle_goaway() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create and feed a goaway frame
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = GoAwayFrame {
            last_stream_id: frame::StreamId::new(0),
            error_code: 0,
            debug_data: bytes::Bytes::new(),
        };
        encoder.encode(&Frame::GoAway(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Should be draining
        assert_eq!(conn.state(), ConnectionState::Draining);

        // Should have GoAway event
        let events = conn.poll_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ConnectionEvent::GoAway { .. }))
        );
    }

    #[test]
    fn test_connection_handle_window_update_connection_level() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        let initial_window = conn.flow_control.available();

        // Create and feed a window update frame
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = WindowUpdateFrame {
            stream_id: frame::StreamId::CONNECTION,
            increment: 1000,
        };
        encoder.encode(&Frame::WindowUpdate(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        assert_eq!(conn.flow_control.available(), initial_window + 1000);
    }

    #[test]
    fn test_connection_handle_rst_stream() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Add a stream first
        let stream = Stream::new(frame::StreamId::new(1), 65535);
        conn.streams.insert(1, stream);

        // Create and feed RST_STREAM
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = RstStreamFrame {
            stream_id: frame::StreamId::new(1),
            error_code: 8, // CANCEL
        };
        encoder.encode(&Frame::RstStream(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Should have StreamReset event
        let events = conn.poll_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ConnectionEvent::StreamReset { .. }))
        );
    }

    #[test]
    fn test_connection_handle_unknown_frame() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create unknown frame manually (type 0xFF)
        let mut buf = bytes::BytesMut::new();
        buf.extend_from_slice(&[
            0x00, 0x00, 0x05, // Length: 5
            0xff, // Type: Unknown
            0x00, // Flags: none
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            b'h', b'e', b'l', b'l', b'o',
        ]);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Should not have any errors - unknown frames are ignored
        let events = conn.poll_events();
        assert!(events.is_empty());
    }

    #[test]
    fn test_connection_handle_priority_frame() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create priority frame
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = frame::PriorityFrame {
            stream_id: frame::StreamId::new(1),
            priority: frame::Priority {
                exclusive: false,
                dependency: frame::StreamId::new(0),
                weight: 16,
            },
        };
        encoder.encode(&Frame::Priority(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Should be ignored - no events
        let events = conn.poll_events();
        assert!(events.is_empty());
    }

    #[test]
    fn test_connection_handle_received_headers() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create a stream first (as if we sent a request)
        let stream = Stream::new(frame::StreamId::new(1), 65535);
        conn.streams.insert(1, stream);

        // Create an HPACK-encoded header block with a simple :status header
        let mut hpack = HpackEncoder::new();
        let headers = vec![HeaderField::new(b":status".to_vec(), b"200".to_vec())];
        let mut header_block = Vec::new();
        hpack.encode(&headers, &mut header_block);

        // Create headers frame
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = HeadersFrame {
            stream_id: frame::StreamId::new(1),
            end_stream: false,
            end_headers: true,
            priority: None,
            header_block: bytes::Bytes::from(header_block),
        };
        encoder.encode(&Frame::Headers(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Should have Headers event
        let events = conn.poll_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ConnectionEvent::Headers { .. }))
        );
    }

    #[test]
    fn test_connection_handle_received_headers_with_end_stream() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create a stream first
        let stream = Stream::new(frame::StreamId::new(1), 65535);
        conn.streams.insert(1, stream);

        // Create header block
        let mut hpack = HpackEncoder::new();
        let headers = vec![HeaderField::new(b":status".to_vec(), b"204".to_vec())];
        let mut header_block = Vec::new();
        hpack.encode(&headers, &mut header_block);

        // Create headers frame with end_stream
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = HeadersFrame {
            stream_id: frame::StreamId::new(1),
            end_stream: true,
            end_headers: true,
            priority: None,
            header_block: bytes::Bytes::from(header_block),
        };
        encoder.encode(&Frame::Headers(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Should have Headers event with end_stream
        let events = conn.poll_events();
        assert!(events.iter().any(|e| matches!(
            e,
            ConnectionEvent::Headers {
                end_stream: true,
                ..
            }
        )));
    }

    #[test]
    fn test_connection_handle_received_data() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create a stream first
        let stream = Stream::new(frame::StreamId::new(1), 65535);
        conn.streams.insert(1, stream);

        // Create data frame
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = DataFrame {
            stream_id: frame::StreamId::new(1),
            end_stream: false,
            data: bytes::Bytes::from_static(b"hello world"),
        };
        encoder.encode(&Frame::Data(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Should have Data event
        let events = conn.poll_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ConnectionEvent::Data { .. }))
        );
    }

    #[test]
    fn test_connection_handle_received_data_with_end_stream() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create a stream first
        let stream = Stream::new(frame::StreamId::new(1), 65535);
        conn.streams.insert(1, stream);

        // Create data frame with end_stream
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = DataFrame {
            stream_id: frame::StreamId::new(1),
            end_stream: true,
            data: bytes::Bytes::from_static(b"final"),
        };
        encoder.encode(&Frame::Data(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Should have Data event with end_stream
        let events = conn.poll_events();
        assert!(events.iter().any(|e| matches!(
            e,
            ConnectionEvent::Data {
                end_stream: true,
                ..
            }
        )));
    }

    #[test]
    fn test_connection_start_request() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        let headers = vec![
            HeaderField::new(b":method".to_vec(), b"GET".to_vec()),
            HeaderField::new(b":path".to_vec(), b"/".to_vec()),
            HeaderField::new(b":scheme".to_vec(), b"https".to_vec()),
            HeaderField::new(b":authority".to_vec(), b"example.com".to_vec()),
        ];

        let result = conn.start_request(&headers, true);
        assert!(result.is_ok());
        let stream_id = result.unwrap();
        assert_eq!(stream_id.value(), 1);
        assert!(conn.streams.contains_key(&1));
    }

    #[test]
    fn test_connection_start_request_creates_stream() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        let headers = vec![
            HeaderField::new(b":method".to_vec(), b"POST".to_vec()),
            HeaderField::new(b":path".to_vec(), b"/api".to_vec()),
            HeaderField::new(b":scheme".to_vec(), b"https".to_vec()),
            HeaderField::new(b":authority".to_vec(), b"example.com".to_vec()),
        ];

        let result = conn.start_request(&headers, false);
        assert!(result.is_ok());

        // Verify stream was created
        assert!(conn.streams.contains_key(&1));

        // Send on another stream
        let result2 = conn.start_request(&headers, false);
        assert!(result2.is_ok());
        let stream_id2 = result2.unwrap();
        assert_eq!(stream_id2.value(), 3); // Client uses odd numbers, increments by 2
    }

    #[test]
    fn test_connection_send_data_success() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create a stream
        let stream = Stream::new(frame::StreamId::new(1), 65535);
        conn.streams.insert(1, stream);

        let data = b"test data";
        let result = conn.send_data(frame::StreamId::new(1), data, false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data.len());
    }

    #[test]
    fn test_connection_send_data_with_end_stream() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create a stream
        let stream = Stream::new(frame::StreamId::new(1), 65535);
        conn.streams.insert(1, stream);

        let data = b"final data";
        let result = conn.send_data(frame::StreamId::new(1), data, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_connection_send_data_flow_control() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create a stream with very small window
        let mut stream = Stream::new(frame::StreamId::new(1), 10);
        stream.send_data(10); // Exhaust window
        conn.streams.insert(1, stream);

        let data = b"this is more data than the window allows";
        let result = conn.send_data(frame::StreamId::new(1), data, false);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn test_connection_handle_settings_updates() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create settings frame with various settings
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = SettingsFrame {
            ack: false,
            settings: vec![
                Setting {
                    id: SettingId::HeaderTableSize,
                    value: 8192,
                },
                Setting {
                    id: SettingId::MaxConcurrentStreams,
                    value: 100,
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
                    value: 16384,
                },
            ],
        };
        encoder.encode(&Frame::Settings(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Verify settings were applied
        assert_eq!(conn.remote_settings.max_concurrent_streams, 100);
        assert_eq!(conn.remote_settings.initial_window_size, 32768);
        assert_eq!(conn.remote_settings.max_frame_size, 32768);
        assert_eq!(conn.remote_settings.max_header_list_size, 16384);
    }

    #[test]
    fn test_connection_handle_settings_adjusts_stream_windows() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create a stream with default window size (65535)
        let stream = Stream::new(frame::StreamId::new(1), 65535);
        conn.streams.insert(1, stream);

        // Send settings with new initial window size
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = SettingsFrame {
            ack: false,
            settings: vec![Setting {
                id: SettingId::InitialWindowSize,
                value: 100000,
            }],
        };
        encoder.encode(&Frame::Settings(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Stream window should be adjusted by delta (100000 - 65535 = 34465)
        let stream = conn.streams.get(&1).unwrap();
        assert_eq!(stream.send_window(), 100000);
    }

    #[test]
    fn test_connection_handle_push_promise() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create push promise frame (manually since servers send these)
        // Frame header: length (4 bytes for promised stream id + 0 bytes header block)
        // Type: 0x05 (PUSH_PROMISE)
        // Flags: 0x04 (END_HEADERS)
        // Stream ID: 1
        // Promised Stream ID: 2
        let frame_data = vec![
            0x00, 0x00, 0x04, // Length: 4 bytes (just promised stream ID, no headers)
            0x05, // Type: PUSH_PROMISE
            0x04, // Flags: END_HEADERS
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1
            0x00, 0x00, 0x00, 0x02, // Promised Stream ID: 2
        ];

        conn.read_buf.extend_from_slice(&frame_data);
        conn.process_frames().unwrap();

        // Push promise should be ignored (we disabled server push)
        let events = conn.poll_events();
        assert!(events.is_empty());
    }

    #[test]
    fn test_connection_handle_continuation() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create continuation frame without preceding HEADERS
        // This should trigger a protocol error
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = frame::ContinuationFrame {
            stream_id: frame::StreamId::new(1),
            end_headers: true,
            header_block: bytes::Bytes::from_static(b"\x82"), // Indexed header
        };
        encoder.encode(&Frame::Continuation(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Should have error event for unexpected CONTINUATION
        let events = conn.poll_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ConnectionEvent::Error(_)))
        );
    }

    #[test]
    fn test_connection_handle_window_update_for_stream() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create a stream
        let stream = Stream::new(frame::StreamId::new(1), 65535);
        conn.streams.insert(1, stream);

        // Create window update for stream 1
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = WindowUpdateFrame {
            stream_id: frame::StreamId::new(1),
            increment: 10000,
        };
        encoder.encode(&Frame::WindowUpdate(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Stream window should be increased
        let stream = conn.streams.get(&1).unwrap();
        assert_eq!(stream.send_window(), 75535);
    }

    #[test]
    fn test_connection_on_recv() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create a ping frame
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = PingFrame {
            ack: false,
            data: [1, 2, 3, 4, 5, 6, 7, 8],
        };
        encoder.encode(&Frame::Ping(frame), &mut buf);

        // Use on_recv to feed data
        conn.on_recv(&buf).unwrap();

        // Should have processed the ping and sent ACK
        assert!(!conn.transport().send_data.is_empty());
    }

    #[test]
    fn test_connection_frame_decode_error() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create malformed PING frame (PING on non-zero stream ID is a protocol error)
        let bad_frame = vec![
            0x00, 0x00, 0x08, // Length: 8 bytes
            0x06, // Type: PING
            0x00, // Flags
            0x00, 0x00, 0x00, 0x01, // Stream ID: 1 (should be 0 for PING)
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // 8 bytes of data
        ];

        conn.read_buf.extend_from_slice(&bad_frame);
        conn.process_frames().unwrap();

        // Should have error event
        let events = conn.poll_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ConnectionEvent::Error(_)))
        );
    }

    #[test]
    fn test_connection_pending_send_and_advance() {
        let transport = MockTransport::new();
        let conn = Connection::new(transport);

        // Check pending_send (should be empty initially)
        let pending = conn.pending_send();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_connection_has_pending_send_with_write_buf() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);

        // Initially no pending send
        assert!(!conn.has_pending_send());

        // Add something to write buffer
        conn.write_buf.extend_from_slice(b"test");
        assert!(conn.has_pending_send());
    }

    #[test]
    fn test_connection_handle_headers_hpack_error() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::Open;
        conn.got_settings = true;

        // Create a stream
        let stream = Stream::new(frame::StreamId::new(1), 65535);
        conn.streams.insert(1, stream);

        // Create headers frame with invalid HPACK data
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = HeadersFrame {
            stream_id: frame::StreamId::new(1),
            end_stream: false,
            end_headers: true,
            priority: None,
            header_block: bytes::Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]), // Invalid HPACK
        };
        encoder.encode(&Frame::Headers(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // Should have error event for HPACK decode failure
        let events = conn.poll_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ConnectionEvent::Error(ConnectionError::Hpack(_))))
        );
    }

    #[test]
    fn test_connection_settings_ack_when_waiting() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);
        conn.state = ConnectionState::WaitingSettings;
        conn.got_settings = false;

        // Create settings ACK frame
        let encoder = FrameEncoder::new();
        let mut buf = bytes::BytesMut::new();
        let frame = SettingsFrame {
            ack: true,
            settings: vec![],
        };
        encoder.encode(&Frame::Settings(frame), &mut buf);

        conn.read_buf.extend_from_slice(&buf);
        conn.process_frames().unwrap();

        // ACK without initial settings should be ignored (still waiting)
        assert_eq!(conn.state, ConnectionState::WaitingSettings);
    }

    #[test]
    fn test_connection_flush_write_buffer() {
        let transport = MockTransport::new();
        let mut conn = Connection::new(transport);

        // Add data to write buffer
        conn.write_buf.extend_from_slice(b"test data");
        assert!(!conn.write_buf.is_empty());

        // Flush
        conn.flush_write_buffer().unwrap();

        // Write buffer should be cleared (sent to transport)
        assert!(conn.write_buf.is_empty());

        // Transport should have received the data
        assert!(!conn.transport().send_data.is_empty());
    }
}
