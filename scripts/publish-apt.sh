#!/bin/bash
set -euo pipefail

# Publish .deb packages to S3 APT repository
#
# Required environment variables:
#   S3_BUCKET       - S3 bucket name (e.g., apt.cruciblecache.com)
#   GPG_KEY_ID      - GPG key ID for signing
#   GPG_PASSPHRASE  - GPG passphrase
#
# Expected input:
#   debs/*.deb      - .deb packages to publish (amd64 and arm64)

DIST="stable"
COMPONENT="main"
ARCHITECTURES="amd64 arm64"

# Create local repository structure
REPO_DIR=$(mktemp -d)
trap "rm -rf $REPO_DIR" EXIT

# Create pool directories for each package
mkdir -p "$REPO_DIR/pool/$COMPONENT/c/crucible-server"
mkdir -p "$REPO_DIR/pool/$COMPONENT/c/crucible-benchmark"

# Create binary directories for each architecture
for arch in $ARCHITECTURES; do
    mkdir -p "$REPO_DIR/dists/$DIST/$COMPONENT/binary-$arch"
done

echo "=== Syncing existing pool from S3 ==="
aws s3 sync "s3://$S3_BUCKET/pool/" "$REPO_DIR/pool/" --quiet || true

echo "=== Copying new .deb packages ==="
for deb in debs/*.deb; do
    if [ -f "$deb" ]; then
        basename_deb=$(basename "$deb")
        # Determine which package pool directory based on filename
        if [[ "$basename_deb" == crucible-server* ]]; then
            cp "$deb" "$REPO_DIR/pool/$COMPONENT/c/crucible-server/"
        elif [[ "$basename_deb" == crucible-benchmark* ]]; then
            cp "$deb" "$REPO_DIR/pool/$COMPONENT/c/crucible-benchmark/"
        else
            echo "Warning: Unknown package $basename_deb, skipping"
            continue
        fi
        echo "Added: $basename_deb"
    fi
done

cd "$REPO_DIR"

echo "=== Generating Packages indexes ==="
for arch in $ARCHITECTURES; do
    # Generate Packages file filtering by architecture
    apt-ftparchive --arch "$arch" packages "pool/$COMPONENT" > "dists/$DIST/$COMPONENT/binary-$arch/Packages"
    gzip -k "dists/$DIST/$COMPONENT/binary-$arch/Packages"

    # Generate per-arch Release file
    cat > "dists/$DIST/$COMPONENT/binary-$arch/Release" << EOF
Archive: $DIST
Component: $COMPONENT
Architecture: $arch
EOF
    echo "Generated Packages for $arch"
done

echo "=== Generating Release file ==="
# Generate main Release file
apt-ftparchive release "dists/$DIST" > "dists/$DIST/Release.tmp"

# Add additional metadata
cat > "dists/$DIST/Release" << EOF
Origin: Crucible
Label: Crucible APT Repository
Suite: $DIST
Codename: $DIST
Architectures: $ARCHITECTURES
Components: $COMPONENT
Description: Crucible cache server and benchmark packages
$(cat "dists/$DIST/Release.tmp")
EOF
rm "dists/$DIST/Release.tmp"

echo "=== Signing Release file ==="
echo "$GPG_PASSPHRASE" | gpg --batch --yes --passphrase-fd 0 \
    --default-key "$GPG_KEY_ID" \
    --detach-sign --armor -o "dists/$DIST/Release.gpg" \
    "dists/$DIST/Release"

echo "$GPG_PASSPHRASE" | gpg --batch --yes --passphrase-fd 0 \
    --default-key "$GPG_KEY_ID" \
    --clearsign -o "dists/$DIST/InRelease" \
    "dists/$DIST/Release"

echo "=== Exporting public key ==="
gpg --armor --export "$GPG_KEY_ID" > "gpg-key.asc"

echo "=== Uploading to S3 ==="
aws s3 sync "$REPO_DIR/" "s3://$S3_BUCKET/" \
    --delete \
    --exclude ".git/*" \
    --cache-control "max-age=300"

# Set appropriate content types
aws s3 cp "s3://$S3_BUCKET/dists/$DIST/Release" "s3://$S3_BUCKET/dists/$DIST/Release" \
    --content-type "text/plain" --metadata-directive REPLACE

aws s3 cp "s3://$S3_BUCKET/gpg-key.asc" "s3://$S3_BUCKET/gpg-key.asc" \
    --content-type "application/pgp-keys" --metadata-directive REPLACE

echo "=== APT repository published successfully ==="
