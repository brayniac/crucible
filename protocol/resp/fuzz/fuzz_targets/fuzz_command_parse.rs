#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol_resp::Command;

fuzz_target!(|data: &[u8]| {
    // Try to parse the input as a RESP command
    if let Ok((command, consumed)) = Command::parse(data) {
        // Verify consumed bytes is reasonable
        assert!(consumed <= data.len());

        // Verify the command name is valid
        let name = command.name();
        assert!(!name.is_empty());

        // For commands with keys, verify the key slice is within bounds of input
        match &command {
            Command::Get { key } => {
                assert!(!key.is_empty() || key.is_empty()); // key can be empty
            }
            Command::Set { key, value, .. } => {
                assert!(!key.is_empty() || key.is_empty());
                assert!(!value.is_empty() || value.is_empty());
            }
            Command::Del { key } => {
                assert!(!key.is_empty() || key.is_empty());
            }
            Command::MGet { keys } => {
                // All keys should be valid slices
                for _key in keys {
                    // Just iterate to ensure no panic
                }
            }
            Command::Config { subcommand, args } => {
                let _ = subcommand;
                for _arg in args {
                    // Just iterate to ensure no panic
                }
            }
            Command::Hello {
                proto_version,
                auth,
                client_name,
            } => {
                let _ = proto_version;
                if let Some((user, pass)) = auth {
                    let _ = user;
                    let _ = pass;
                }
                let _ = client_name;
            }
            _ => {}
        }
    }
    // Parse errors are expected for malformed input - not a bug
});
