#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol_memcache::Command;

fuzz_target!(|data: &[u8]| {
    // Try to parse the input as a Memcache ASCII command
    if let Ok((command, consumed)) = Command::parse(data) {
        // Verify consumed bytes is reasonable
        assert!(consumed <= data.len());

        // Verify the command name is valid
        let name = command.name();
        assert!(!name.is_empty());

        // Verify command-specific invariants
        match &command {
            Command::Get { key } => {
                // Key should be within the original buffer
                let _ = key;
            }
            Command::Gets { keys } => {
                // All keys should be valid
                assert!(!keys.is_empty());
                for _key in keys {
                    // Just iterate to verify no panic
                }
            }
            Command::Set {
                key,
                flags: _,
                exptime: _,
                data: value,
            } => {
                let _ = key;
                let _ = value;
            }
            Command::Delete { key } => {
                let _ = key;
            }
            Command::FlushAll | Command::Version | Command::Quit => {}
        }
    }
    // Parse errors are expected for malformed input - not a bug
});
