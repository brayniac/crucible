# Installation

Crucible provides pre-built packages for Debian/Ubuntu (APT) and RHEL/CentOS/Fedora (YUM/DNF).

## Debian / Ubuntu (APT)

### Add the repository

```bash
# Download the GPG key
curl -fsSL https://apt.thermitesolutions.com/gpg-key.asc | sudo gpg --dearmor -o /usr/share/keyrings/crucible-archive-keyring.gpg

# Add the repository
echo "deb [signed-by=/usr/share/keyrings/crucible-archive-keyring.gpg] https://apt.thermitesolutions.com stable main" | sudo tee /etc/apt/sources.list.d/crucible.list

# Update package list
sudo apt update
```

### Install packages

```bash
# Install the cache server
sudo apt install crucible-server

# Install the benchmark tool
sudo apt install crucible-benchmark
```

## RHEL / CentOS / Fedora (YUM/DNF)

### Add the repository

```bash
# Add the repository configuration
sudo tee /etc/yum.repos.d/crucible.repo << 'EOF'
[crucible]
name=Crucible Repository
baseurl=https://yum.thermitesolutions.com
enabled=1
gpgcheck=1
gpgkey=https://yum.thermitesolutions.com/gpg-key.asc
EOF
```

### Install packages

```bash
# Install the cache server
sudo dnf install crucible-server

# Install the benchmark tool
sudo dnf install crucible-benchmark
```

For older systems using yum instead of dnf:

```bash
sudo yum install crucible-server
sudo yum install crucible-benchmark
```

## Building from source

If you prefer to build from source, see the [development guide](development.md).

```bash
# Clone the repository
git clone https://github.com/brayniac/crucible.git
cd crucible

# Build release binaries
cargo build --release

# Binaries are in target/release/
ls target/release/crucible-server target/release/crucible-benchmark
```

## Verifying installation

After installation, verify the binaries are available:

```bash
crucible-server --version
crucible-benchmark --version
```

## Next steps

- [Configuration guide](configuration.md) - Configure the cache server
- [Operations guide](operations.md) - Running and monitoring in production
