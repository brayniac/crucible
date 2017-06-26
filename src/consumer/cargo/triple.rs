use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Triple {
    Native,
    Aarch64LinuxGnu,
    ArmLinuxGnueabi,
    ArmLinuxGnueabihf,
    Armv7LinuxGnueabihf,
    I686LinuxGnu,
    I686LinuxMusl,
    X86LinuxGnu,
    X86LinuxMusl,
}

impl fmt::Display for Triple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Triple::Native => write!(f, "native"),
            Triple::Aarch64LinuxGnu => write!(f, "aarch64-unknown-linux-gnu"),
            Triple::ArmLinuxGnueabi => write!(f, "arm-unknown-linux-gnueabi"),
            Triple::ArmLinuxGnueabihf => write!(f, "arm-unknown-linux-gnueabihf"),
            Triple::Armv7LinuxGnueabihf => write!(f, "armv7-unknown-linux-gnueabihf"),
            Triple::I686LinuxGnu => write!(f, "i686-unknown-linux-gnu"),
            Triple::I686LinuxMusl => write!(f, "i686-unknown-linux-musl"),
            Triple::X86LinuxGnu => write!(f, "x86_64-unknown-linux-gnu"),
            Triple::X86LinuxMusl => write!(f, "x86_64-unknown-linux-musl"),
        }
    }
}

pub fn list() -> Vec<Triple> {
    vec![
        Triple::Aarch64LinuxGnu,
        Triple::ArmLinuxGnueabi,
        Triple::ArmLinuxGnueabihf,
        Triple::Armv7LinuxGnueabihf,
        Triple::I686LinuxGnu,
        Triple::I686LinuxMusl,
        Triple::X86LinuxGnu,
        Triple::X86LinuxMusl,
    ]
}
