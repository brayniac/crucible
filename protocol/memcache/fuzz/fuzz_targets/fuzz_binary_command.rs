#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol_memcache::binary::BinaryCommand;

fuzz_target!(|data: &[u8]| {
    // Try to parse the input as a Memcache binary command
    if let Ok((command, consumed)) = BinaryCommand::parse(data) {
        // Verify consumed bytes is reasonable
        assert!(consumed <= data.len());

        // Verify opaque can be retrieved
        let _ = command.opaque();

        // Verify is_quiet returns a valid result
        let _ = command.is_quiet();

        // Verify command-specific invariants
        match &command {
            BinaryCommand::Get { key, opaque: _ }
            | BinaryCommand::GetQ { key, opaque: _ }
            | BinaryCommand::GetK { key, opaque: _ }
            | BinaryCommand::GetKQ { key, opaque: _ } => {
                let _ = key;
            }
            BinaryCommand::Set {
                key,
                value,
                flags: _,
                expiration: _,
                cas: _,
                opaque: _,
            }
            | BinaryCommand::SetQ {
                key,
                value,
                flags: _,
                expiration: _,
                cas: _,
                opaque: _,
            } => {
                let _ = key;
                let _ = value;
            }
            BinaryCommand::Add {
                key,
                value,
                flags: _,
                expiration: _,
                opaque: _,
            } => {
                let _ = key;
                let _ = value;
            }
            BinaryCommand::Replace {
                key,
                value,
                flags: _,
                expiration: _,
                cas: _,
                opaque: _,
            } => {
                let _ = key;
                let _ = value;
            }
            BinaryCommand::Delete {
                key,
                cas: _,
                opaque: _,
            }
            | BinaryCommand::DeleteQ {
                key,
                cas: _,
                opaque: _,
            } => {
                let _ = key;
            }
            BinaryCommand::Increment {
                key,
                delta: _,
                initial: _,
                expiration: _,
                cas: _,
                opaque: _,
            }
            | BinaryCommand::Decrement {
                key,
                delta: _,
                initial: _,
                expiration: _,
                cas: _,
                opaque: _,
            } => {
                let _ = key;
            }
            BinaryCommand::Append {
                key,
                value,
                cas: _,
                opaque: _,
            }
            | BinaryCommand::Prepend {
                key,
                value,
                cas: _,
                opaque: _,
            } => {
                let _ = key;
                let _ = value;
            }
            BinaryCommand::Touch {
                key,
                expiration: _,
                opaque: _,
            }
            | BinaryCommand::Gat {
                key,
                expiration: _,
                opaque: _,
            } => {
                let _ = key;
            }
            BinaryCommand::Flush {
                expiration: _,
                opaque: _,
            }
            | BinaryCommand::Noop { opaque: _ }
            | BinaryCommand::Version { opaque: _ }
            | BinaryCommand::Quit { opaque: _ } => {}
            BinaryCommand::Stat { key, opaque: _ } => {
                let _ = key;
            }
        }
    }
    // Parse errors are expected for malformed input - not a bug
});
