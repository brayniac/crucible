//! Momento credential handling.
//!
//! Momento API tokens contain encoded endpoint information.

use crate::WireFormat;
use crate::error::{Error, Result};

/// Momento credential containing API token, endpoint, and wire format configuration.
#[derive(Debug, Clone)]
pub struct Credential {
    /// The API token for authentication.
    token: String,
    /// The cache endpoint (host:port or just host).
    endpoint: String,
    /// The wire format to use for communication.
    wire_format: WireFormat,
    /// Optional SNI hostname for TLS (used when connecting to IP addresses).
    sni_host: Option<String>,
}

impl Credential {
    /// Create a credential from an API token.
    ///
    /// The token is expected to be a Momento API key. The endpoint
    /// can be extracted from the token or provided separately.
    /// Uses gRPC wire format by default.
    pub fn from_token(token: impl Into<String>) -> Result<Self> {
        let token = token.into();

        // Try to extract endpoint from token
        // Momento tokens are typically: header.payload.signature (JWT-like)
        // The payload contains the endpoint info
        let endpoint = Self::extract_endpoint(&token)
            .unwrap_or_else(|| "cache.cell-us-east-1-1.prod.a.momentohq.com".to_string());

        Ok(Self {
            token,
            endpoint,
            wire_format: WireFormat::default(),
            sni_host: None,
        })
    }

    /// Create a credential with explicit endpoint.
    /// Uses gRPC wire format by default.
    pub fn with_endpoint(token: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            endpoint: endpoint.into(),
            wire_format: WireFormat::default(),
            sni_host: None,
        }
    }

    /// Create a credential from environment variables.
    ///
    /// Uses:
    /// - `MOMENTO_API_KEY` or `MOMENTO_AUTH_TOKEN` for the token
    /// - `MOMENTO_ENDPOINT` for explicit endpoint (e.g., "cache.us-west-2.momentohq.com" or IP:port)
    /// - `MOMENTO_REGION` for region-based endpoint (e.g., "us-west-2")
    /// - `MOMENTO_WIRE_FORMAT` for wire format ("grpc" or "protosocket")
    /// - `MOMENTO_SNI_HOST` for TLS hostname when connecting to IP addresses
    pub fn from_env() -> Result<Self> {
        let token = std::env::var("MOMENTO_API_KEY")
            .or_else(|_| std::env::var("MOMENTO_AUTH_TOKEN"))
            .map_err(|_| Error::Config("MOMENTO_API_KEY environment variable not set".into()))?;

        // Check for wire format
        let wire_format = match std::env::var("MOMENTO_WIRE_FORMAT").as_deref() {
            Ok("protosocket") => WireFormat::Protosocket,
            _ => WireFormat::Grpc,
        };

        // Check for SNI hostname (used when connecting to IP addresses)
        let sni_host = std::env::var("MOMENTO_SNI_HOST").ok();

        // Check for explicit endpoint
        if let Ok(endpoint) = std::env::var("MOMENTO_ENDPOINT") {
            // Add "cache." prefix if not already present
            let endpoint = if endpoint.starts_with("cache.") {
                endpoint
            } else {
                format!("cache.{}", endpoint)
            };
            return Ok(Self {
                token,
                endpoint,
                wire_format,
                sni_host,
            });
        }

        // Check for region-based endpoint
        if let Ok(region) = std::env::var("MOMENTO_REGION") {
            // Momento endpoint format: cache.cell-<region>-1.prod.a.momentohq.com
            let endpoint = format!("cache.cell-{}-1.prod.a.momentohq.com", region);
            return Ok(Self {
                token,
                endpoint,
                wire_format,
                sni_host,
            });
        }

        // Try to extract from token (legacy tokens)
        let mut cred = Self::from_token(token)?;
        cred.wire_format = wire_format;
        cred.sni_host = sni_host;
        Ok(cred)
    }

    /// Set the wire format for this credential.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use protocol_momento::{Credential, WireFormat};
    ///
    /// let cred = Credential::from_token("token")?
    ///     .with_wire_format(WireFormat::Protosocket);
    /// ```
    pub fn with_wire_format(mut self, format: WireFormat) -> Self {
        self.wire_format = format;
        self
    }

    /// Get the API token.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Get the cache endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Get the wire format.
    pub fn wire_format(&self) -> WireFormat {
        self.wire_format
    }

    /// Get the host portion of the endpoint.
    pub fn host(&self) -> &str {
        self.endpoint.split(':').next().unwrap_or(&self.endpoint)
    }

    /// Get the TLS hostname for SNI.
    ///
    /// Returns the explicit SNI host if set, otherwise returns the endpoint host.
    /// Use this when creating TLS connections to ensure proper certificate verification.
    pub fn tls_host(&self) -> &str {
        self.sni_host.as_deref().unwrap_or_else(|| self.host())
    }

    /// Get the HTTP API endpoint for fetching addresses.
    ///
    /// The HTTP API endpoint is derived from the cache endpoint by adding
    /// the `api.` prefix. For example:
    /// - Cache endpoint: `cache.cell-4-us-west-2-1.prod.a.momentohq.com`
    /// - HTTP endpoint: `api.cache.cell-4-us-west-2-1.prod.a.momentohq.com`
    pub fn http_endpoint(&self) -> String {
        let host = self.host();
        format!("api.{}", host)
    }

    /// Get the port based on wire format.
    ///
    /// If an explicit port is specified in the endpoint, that port is used.
    /// Otherwise, defaults to:
    /// - Port 443 for gRPC (HTTP/2 over TLS)
    /// - Port 9004 for Protosocket (TLS without HTTP/2)
    pub fn port(&self) -> u16 {
        // Check for explicit port in endpoint
        if let Some(port_str) = self.endpoint.split(':').nth(1) {
            if let Ok(port) = port_str.parse() {
                return port;
            }
        }

        // Default port based on wire format
        match self.wire_format {
            WireFormat::Grpc => 443,
            WireFormat::Protosocket => 9004,
        }
    }

    /// Extract endpoint from a Momento token.
    ///
    /// Momento tokens are JWT-like with base64-encoded JSON payloads.
    /// The token contains the base endpoint (e.g., "cell-4-us-west-2-1.prod.a.momentohq.com")
    /// which needs the "cache." prefix added to form the cache endpoint.
    fn extract_endpoint(token: &str) -> Option<String> {
        // Split by '.' to get JWT parts
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() < 2 {
            return None;
        }

        // Decode the payload (second part)
        // Note: JWT uses base64url encoding without padding
        let payload = parts[1];
        let decoded = Self::base64url_decode(payload)?;

        // Parse as JSON and extract endpoint
        // We do minimal JSON parsing to avoid dependencies
        let json = String::from_utf8(decoded).ok()?;

        // Try various field names Momento has used
        let base_endpoint = Self::extract_json_field(&json, "c") // Legacy: 'c' field contains cache endpoint
            .or_else(|| Self::extract_json_field(&json, "endpoint"))
            .or_else(|| Self::extract_json_field(&json, "cp"))?; // Control plane, derive cache from it

        // Prepend "cache." prefix if not already present
        // The token contains the base cell endpoint, cache API needs "cache." prefix
        if base_endpoint.starts_with("cache.") {
            Some(base_endpoint)
        } else {
            Some(format!("cache.{}", base_endpoint))
        }
    }

    /// Debug: dump the JWT payload for inspection.
    pub fn debug_jwt_payload(token: &str) -> Option<String> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() < 2 {
            return None;
        }
        let payload = parts[1];
        let decoded = Self::base64url_decode(payload)?;
        String::from_utf8(decoded).ok()
    }

    /// Decode base64url (URL-safe base64 without padding).
    fn base64url_decode(input: &str) -> Option<Vec<u8>> {
        // Convert base64url to standard base64
        let mut s = input.replace('-', "+").replace('_', "/");

        // Add padding if needed
        match s.len() % 4 {
            2 => s.push_str("=="),
            3 => s.push('='),
            _ => {}
        }

        // Simple base64 decode
        Self::base64_decode(&s)
    }

    /// Simple base64 decoder.
    fn base64_decode(input: &str) -> Option<Vec<u8>> {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

        let mut output = Vec::with_capacity(input.len() * 3 / 4);
        let mut buffer = 0u32;
        let mut bits = 0;

        for c in input.bytes() {
            if c == b'=' {
                break;
            }

            let value = ALPHABET.iter().position(|&x| x == c)? as u32;
            buffer = (buffer << 6) | value;
            bits += 6;

            if bits >= 8 {
                bits -= 8;
                output.push((buffer >> bits) as u8);
                buffer &= (1 << bits) - 1;
            }
        }

        Some(output)
    }

    /// Extract a string field from JSON (minimal parsing).
    fn extract_json_field(json: &str, field: &str) -> Option<String> {
        // Look for "field":"value" or "field": "value"
        let pattern = format!("\"{}\"", field);
        let start = json.find(&pattern)?;
        let rest = &json[start + pattern.len()..];

        // Skip whitespace and colon
        let rest = rest.trim_start();
        let rest = rest.strip_prefix(':')?;
        let rest = rest.trim_start();

        // Extract quoted string value
        let rest = rest.strip_prefix('"')?;
        let end = rest.find('"')?;

        Some(rest[..end].to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_credential_with_endpoint() {
        let cred = Credential::with_endpoint("my-token", "cache.example.com:443");
        assert_eq!(cred.token(), "my-token");
        assert_eq!(cred.endpoint(), "cache.example.com:443");
        assert_eq!(cred.host(), "cache.example.com");
        assert_eq!(cred.port(), 443);
        assert_eq!(cred.wire_format(), WireFormat::Grpc); // Default
    }

    #[test]
    fn test_credential_http_endpoint() {
        let cred =
            Credential::with_endpoint("token", "cache.cell-4-us-west-2-1.prod.a.momentohq.com");
        assert_eq!(
            cred.http_endpoint(),
            "api.cache.cell-4-us-west-2-1.prod.a.momentohq.com"
        );
    }

    #[test]
    fn test_credential_http_endpoint_with_port() {
        let cred = Credential::with_endpoint("token", "cache.example.com:443");
        assert_eq!(cred.http_endpoint(), "api.cache.example.com");
    }

    #[test]
    fn test_credential_default_wire_format() {
        let cred = Credential::with_endpoint("token", "endpoint.com");
        assert_eq!(cred.wire_format(), WireFormat::Grpc);
    }

    #[test]
    fn test_credential_with_wire_format() {
        let cred = Credential::with_endpoint("token", "endpoint.com")
            .with_wire_format(WireFormat::Protosocket);
        assert_eq!(cred.wire_format(), WireFormat::Protosocket);
    }

    #[test]
    fn test_credential_wire_format_chain() {
        let cred = Credential::from_token("token")
            .unwrap()
            .with_wire_format(WireFormat::Protosocket);
        assert_eq!(cred.wire_format(), WireFormat::Protosocket);
    }

    #[test]
    fn test_credential_default_port_grpc() {
        let cred = Credential::with_endpoint("token", "cache.example.com");
        // Default wire format is gRPC, which uses port 443
        assert_eq!(cred.port(), 443);
    }

    #[test]
    fn test_credential_default_port_protosocket() {
        let cred = Credential::with_endpoint("token", "cache.example.com")
            .with_wire_format(WireFormat::Protosocket);
        // Protosocket uses port 9004
        assert_eq!(cred.port(), 9004);
    }

    #[test]
    fn test_credential_custom_port() {
        let cred = Credential::with_endpoint("token", "cache.example.com:8080");
        assert_eq!(cred.port(), 8080);
        assert_eq!(cred.host(), "cache.example.com");
    }

    #[test]
    fn test_credential_invalid_port_grpc() {
        let cred = Credential::with_endpoint("token", "cache.example.com:notaport");
        // Invalid port should fall back to wire format default (gRPC = 443)
        assert_eq!(cred.port(), 443);
    }

    #[test]
    fn test_credential_invalid_port_protosocket() {
        let cred = Credential::with_endpoint("token", "cache.example.com:notaport")
            .with_wire_format(WireFormat::Protosocket);
        // Invalid port should fall back to wire format default (protosocket = 9004)
        assert_eq!(cred.port(), 9004);
    }

    #[test]
    fn test_credential_from_token_simple() {
        // Simple token without JWT structure
        let cred = Credential::from_token("simple-api-key").unwrap();
        assert_eq!(cred.token(), "simple-api-key");
        // Should use default endpoint
        assert!(cred.endpoint().contains("momentohq.com"));
    }

    #[test]
    fn test_credential_from_token_with_jwt() {
        // Create a fake JWT-like token with endpoint in payload
        // Payload: {"c":"cell-test.example.com"} encoded as base64url
        // The "cache." prefix is added automatically
        let payload = r#"{"c":"cell-test.example.com"}"#;
        let encoded_payload = base64url_encode(payload.as_bytes());
        let token = format!("header.{}.signature", encoded_payload);

        let cred = Credential::from_token(&token).unwrap();
        assert_eq!(cred.endpoint(), "cache.cell-test.example.com");
    }

    #[test]
    fn test_credential_from_token_with_cache_prefix() {
        // If the endpoint already has "cache." prefix, don't double it
        let payload = r#"{"c":"cache.already-prefixed.example.com"}"#;
        let encoded_payload = base64url_encode(payload.as_bytes());
        let token = format!("header.{}.signature", encoded_payload);

        let cred = Credential::from_token(&token).unwrap();
        assert_eq!(cred.endpoint(), "cache.already-prefixed.example.com");
    }

    #[test]
    fn test_credential_from_token_with_endpoint_field() {
        // Payload with "endpoint" field instead of "c"
        // The "cache." prefix is added automatically
        let payload = r#"{"endpoint":"cell-endpoint.example.com"}"#;
        let encoded_payload = base64url_encode(payload.as_bytes());
        let token = format!("header.{}.signature", encoded_payload);

        let cred = Credential::from_token(&token).unwrap();
        assert_eq!(cred.endpoint(), "cache.cell-endpoint.example.com");
    }

    #[test]
    fn test_credential_clone() {
        let cred = Credential::with_endpoint("token", "endpoint.com")
            .with_wire_format(WireFormat::Protosocket);
        let cloned = cred.clone();
        assert_eq!(cloned.token(), cred.token());
        assert_eq!(cloned.endpoint(), cred.endpoint());
        assert_eq!(cloned.wire_format(), WireFormat::Protosocket);
    }

    #[test]
    fn test_credential_debug() {
        let cred = Credential::with_endpoint("secret-token", "endpoint.com");
        let debug = format!("{:?}", cred);
        assert!(debug.contains("Credential"));
    }

    // Base64 decode tests

    #[test]
    fn test_base64_decode() {
        // "hello" in base64
        let decoded = Credential::base64_decode("aGVsbG8=").unwrap();
        assert_eq!(&decoded, b"hello");
    }

    #[test]
    fn test_base64_decode_no_padding() {
        // "hi" in base64 without padding
        let decoded = Credential::base64_decode("aGk").unwrap();
        assert_eq!(&decoded, b"hi");
    }

    #[test]
    fn test_base64_decode_empty() {
        let decoded = Credential::base64_decode("").unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_base64_decode_invalid_char() {
        // Invalid base64 character
        let result = Credential::base64_decode("!!!!");
        assert!(result.is_none());
    }

    #[test]
    fn test_base64_decode_longer_string() {
        // "Hello, World!" in base64
        let decoded = Credential::base64_decode("SGVsbG8sIFdvcmxkIQ==").unwrap();
        assert_eq!(&decoded, b"Hello, World!");
    }

    // Base64url decode tests

    #[test]
    fn test_base64url_decode_with_url_chars() {
        // Test conversion of URL-safe chars
        // Standard base64 uses + and /, base64url uses - and _
        let input = "dGVzdC1kYXRh"; // "test-data" but without URL-unsafe chars
        let decoded = Credential::base64url_decode(input);
        assert!(decoded.is_some());
    }

    #[test]
    fn test_base64url_decode_padding_2() {
        // String that needs 2 padding chars
        let decoded = Credential::base64url_decode("YQ").unwrap();
        assert_eq!(&decoded, b"a");
    }

    #[test]
    fn test_base64url_decode_padding_1() {
        // String that needs 1 padding char
        let decoded = Credential::base64url_decode("YWI").unwrap();
        assert_eq!(&decoded, b"ab");
    }

    #[test]
    fn test_base64url_decode_no_padding_needed() {
        // String that doesn't need padding (length divisible by 4)
        let decoded = Credential::base64url_decode("YWJj").unwrap();
        assert_eq!(&decoded, b"abc");
    }

    // JSON field extraction tests

    #[test]
    fn test_extract_json_field() {
        let json = r#"{"c":"cache.example.com","other":"value"}"#;
        let endpoint = Credential::extract_json_field(json, "c");
        assert_eq!(endpoint, Some("cache.example.com".to_string()));
    }

    #[test]
    fn test_extract_json_field_with_spaces() {
        let json = r#"{ "c" : "cache.example.com" }"#;
        let endpoint = Credential::extract_json_field(json, "c");
        assert_eq!(endpoint, Some("cache.example.com".to_string()));
    }

    #[test]
    fn test_extract_json_field_not_found() {
        let json = r#"{"other":"value"}"#;
        let result = Credential::extract_json_field(json, "c");
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_json_field_empty_value() {
        let json = r#"{"c":""}"#;
        let result = Credential::extract_json_field(json, "c");
        assert_eq!(result, Some(String::new()));
    }

    #[test]
    fn test_extract_json_field_nested() {
        let json = r#"{"outer":{"c":"nested.value"},"c":"top.value"}"#;
        let result = Credential::extract_json_field(json, "c");
        // Should find the first occurrence
        assert!(result.is_some());
    }

    // Extract endpoint tests

    #[test]
    fn test_extract_endpoint_invalid_jwt() {
        // Not a JWT (no dots)
        let result = Credential::extract_endpoint("not-a-jwt");
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_endpoint_single_dot() {
        // Only one part, not enough for JWT
        let _result = Credential::extract_endpoint("header.payload");
        // Might work if payload decodes to valid JSON with endpoint
        // For invalid base64 payload, should return None
    }

    #[test]
    fn test_extract_endpoint_invalid_base64() {
        // Invalid base64 in payload
        let result = Credential::extract_endpoint("header.!!!!.signature");
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_endpoint_invalid_json() {
        // Valid base64 but not JSON
        let payload = base64url_encode(b"not json");
        let token = format!("header.{}.signature", payload);
        let result = Credential::extract_endpoint(&token);
        assert!(result.is_none());
    }

    // Debug JWT payload tests

    #[test]
    fn test_debug_jwt_payload() {
        let payload = r#"{"test":"value"}"#;
        let encoded = base64url_encode(payload.as_bytes());
        let token = format!("header.{}.signature", encoded);

        let result = Credential::debug_jwt_payload(&token);
        assert_eq!(result, Some(payload.to_string()));
    }

    #[test]
    fn test_debug_jwt_payload_invalid_token() {
        let result = Credential::debug_jwt_payload("no-dots-here");
        assert!(result.is_none());
    }

    #[test]
    fn test_debug_jwt_payload_invalid_base64() {
        let result = Credential::debug_jwt_payload("header.!!!invalid!!!.sig");
        assert!(result.is_none());
    }

    // Helper function for tests
    fn base64url_encode(data: &[u8]) -> String {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

        let mut result = String::new();
        let mut bits = 0u32;
        let mut num_bits = 0;

        for &byte in data {
            bits = (bits << 8) | byte as u32;
            num_bits += 8;

            while num_bits >= 6 {
                num_bits -= 6;
                let index = ((bits >> num_bits) & 0x3F) as usize;
                result.push(ALPHABET[index] as char);
            }
        }

        if num_bits > 0 {
            bits <<= 6 - num_bits;
            let index = (bits & 0x3F) as usize;
            result.push(ALPHABET[index] as char);
        }

        // Convert to base64url
        result.replace('+', "-").replace('/', "_")
    }

    #[test]
    #[serial]
    fn test_from_env_with_explicit_endpoint() {
        // Save original values
        let orig_key = std::env::var("MOMENTO_API_KEY").ok();
        let orig_endpoint = std::env::var("MOMENTO_ENDPOINT").ok();
        let orig_region = std::env::var("MOMENTO_REGION").ok();

        // SAFETY: This test runs with --test-threads=1 to avoid data races
        unsafe {
            // Set test values - endpoint without cache. prefix
            std::env::set_var("MOMENTO_API_KEY", "test-token");
            std::env::set_var("MOMENTO_ENDPOINT", "cell-test.example.com");
            std::env::remove_var("MOMENTO_REGION");
        }

        let cred = Credential::from_env().expect("from_env should succeed");
        assert_eq!(cred.token(), "test-token");
        // Should add cache. prefix
        assert_eq!(cred.host(), "cache.cell-test.example.com");

        // SAFETY: Restore original values
        unsafe {
            if let Some(val) = orig_key {
                std::env::set_var("MOMENTO_API_KEY", val);
            } else {
                std::env::remove_var("MOMENTO_API_KEY");
            }
            if let Some(val) = orig_endpoint {
                std::env::set_var("MOMENTO_ENDPOINT", val);
            } else {
                std::env::remove_var("MOMENTO_ENDPOINT");
            }
            if let Some(val) = orig_region {
                std::env::set_var("MOMENTO_REGION", val);
            }
        }
    }

    #[test]
    #[serial]
    fn test_from_env_with_cache_prefixed_endpoint() {
        // Save original values
        let orig_key = std::env::var("MOMENTO_API_KEY").ok();
        let orig_endpoint = std::env::var("MOMENTO_ENDPOINT").ok();
        let orig_region = std::env::var("MOMENTO_REGION").ok();

        // SAFETY: This test runs with --test-threads=1 to avoid data races
        unsafe {
            // Set test values - endpoint already has cache. prefix
            std::env::set_var("MOMENTO_API_KEY", "test-token");
            std::env::set_var("MOMENTO_ENDPOINT", "cache.already-prefixed.example.com");
            std::env::remove_var("MOMENTO_REGION");
        }

        let cred = Credential::from_env().expect("from_env should succeed");
        assert_eq!(cred.token(), "test-token");
        // Should not double the prefix
        assert_eq!(cred.host(), "cache.already-prefixed.example.com");

        // SAFETY: Restore original values
        unsafe {
            if let Some(val) = orig_key {
                std::env::set_var("MOMENTO_API_KEY", val);
            } else {
                std::env::remove_var("MOMENTO_API_KEY");
            }
            if let Some(val) = orig_endpoint {
                std::env::set_var("MOMENTO_ENDPOINT", val);
            } else {
                std::env::remove_var("MOMENTO_ENDPOINT");
            }
            if let Some(val) = orig_region {
                std::env::set_var("MOMENTO_REGION", val);
            }
        }
    }

    #[test]
    #[serial]
    fn test_from_env_with_region() {
        // Save original values
        let orig_key = std::env::var("MOMENTO_API_KEY").ok();
        let orig_endpoint = std::env::var("MOMENTO_ENDPOINT").ok();
        let orig_region = std::env::var("MOMENTO_REGION").ok();

        // SAFETY: This test runs with --test-threads=1 to avoid data races
        unsafe {
            // Set test values
            std::env::set_var("MOMENTO_API_KEY", "test-token");
            std::env::remove_var("MOMENTO_ENDPOINT");
            std::env::set_var("MOMENTO_REGION", "us-west-2");
        }

        let cred = Credential::from_env().expect("from_env should succeed");
        assert_eq!(cred.token(), "test-token");
        assert!(cred.host().contains("us-west-2"));

        // SAFETY: Restore original values
        unsafe {
            if let Some(val) = orig_key {
                std::env::set_var("MOMENTO_API_KEY", val);
            } else {
                std::env::remove_var("MOMENTO_API_KEY");
            }
            if let Some(val) = orig_endpoint {
                std::env::set_var("MOMENTO_ENDPOINT", val);
            } else {
                std::env::remove_var("MOMENTO_ENDPOINT");
            }
            if let Some(val) = orig_region {
                std::env::set_var("MOMENTO_REGION", val);
            } else {
                std::env::remove_var("MOMENTO_REGION");
            }
        }
    }

    #[test]
    #[serial]
    fn test_from_env_missing_token() {
        // Save original values
        let orig_key = std::env::var("MOMENTO_API_KEY").ok();
        let orig_auth = std::env::var("MOMENTO_AUTH_TOKEN").ok();

        // SAFETY: This test runs with --test-threads=1 to avoid data races
        unsafe {
            // Remove token variables
            std::env::remove_var("MOMENTO_API_KEY");
            std::env::remove_var("MOMENTO_AUTH_TOKEN");
        }

        let result = Credential::from_env();
        assert!(result.is_err());

        // SAFETY: Restore original values
        unsafe {
            if let Some(val) = orig_key {
                std::env::set_var("MOMENTO_API_KEY", val);
            }
            if let Some(val) = orig_auth {
                std::env::set_var("MOMENTO_AUTH_TOKEN", val);
            }
        }
    }
}
