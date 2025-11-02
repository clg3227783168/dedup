#!/bin/bash
set -e

echo "Installing dedup-snapshotter..."

if ! command -v mkfs.erofs &> /dev/null; then
    echo "Error: mkfs.erofs not found. Please install erofs-utils first."
    echo "On OpenCloudOS/CentOS: sudo yum install erofs-utils"
    echo "On Ubuntu/Debian: sudo apt-get install erofs-utils"
    exit 1
fi

if [ ! -d "/sys/kernel/mm/ksm" ]; then
    echo "Warning: KSM (Kernel Samepage Merging) not available in kernel."
    echo "Memory deduplication will use madvise only."
fi

CONFIG_DIR="/etc/dedup-snapshotter"
DATA_DIR="/var/lib/containerd/io.containerd.snapshotter.v1.dedup"
LOG_DIR="/var/log/dedup-snapshotter"

mkdir -p "$CONFIG_DIR"
mkdir -p "$DATA_DIR"
mkdir -p "$LOG_DIR"

if [ ! -f "$CONFIG_DIR/config.json" ]; then
    cat > "$CONFIG_DIR/config.json" << EOF
{
  "root": "$DATA_DIR",
  "enable_erofs": true,
  "enable_lazy": true,
  "enable_mem_dedup": true,
  "registry": "",
  "chunk_size": 4194304,
  "log_level": "info",
  "prefetch": {
    "enabled": true,
    "workers": 4,
    "queue_size": 1000,
    "trace_dir": "$DATA_DIR/traces"
  },
  "ksm": {
    "enabled": true,
    "scan_interval": 100,
    "pages_to_scan": 100,
    "merge_across_nodes": false
  }
}
EOF
    echo "Created default config at $CONFIG_DIR/config.json"
fi

if [ ! -f "/etc/systemd/system/dedup-snapshotter.service" ]; then
    cat > /etc/systemd/system/dedup-snapshotter.service << EOF
[Unit]
Description=Dedup Snapshotter for Containerd
Documentation=https://github.com/opencloudos/dedup-snapshotter
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/dedup-snapshotter
Restart=always
RestartSec=5
Environment="ADDRESS=/run/containerd/dedup-snapshotter.sock"
Environment="ROOT=$DATA_DIR"
Environment="CONFIG=$CONFIG_DIR/config.json"

[Install]
WantedBy=multi-user.target
EOF
    echo "Created systemd service file"
    systemctl daemon-reload
fi

if [ -f "/etc/containerd/config.toml" ]; then
    if ! grep -q "dedup-snapshotter" /etc/containerd/config.toml; then
        echo ""
        echo "Please add the following to your /etc/containerd/config.toml:"
        echo ""
        cat << EOF
[proxy_plugins]
  [proxy_plugins.dedup]
    type = "snapshot"
    address = "/run/containerd/dedup-snapshotter.sock"
EOF
        echo ""
        echo "Then restart containerd: systemctl restart containerd"
    fi
fi
systemctl start dedup-snapshotter

echo ""
echo "Installation complete!"
echo "To start the snapshotter:"
echo ""
echo "To check status:"
echo "  systemctl status dedup-snapshotter"
echo "  journalctl -u dedup-snapshotter -f"
