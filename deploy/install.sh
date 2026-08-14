#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="/opt/stocker-list"
QUADLET_DIR="/etc/containers/systemd"
ENV_DIR="/etc/stocker-list"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [[ $EUID -ne 0 ]]; then
  echo "Error: run as root (sudo ./install.sh)" >&2
  exit 1
fi

echo "==> Installing project to $INSTALL_DIR"
mkdir -p "$INSTALL_DIR"
cp -r "$PROJECT_DIR"/Containerfile "$PROJECT_DIR"/go.mod "$PROJECT_DIR"/go.sum \
      "$PROJECT_DIR"/cmd "$PROJECT_DIR"/internal "$PROJECT_DIR"/proto "$INSTALL_DIR/"
# Remove local bin/ and gen/ artifacts if present -- the build regenerates them.
rm -rf "$INSTALL_DIR/bin" "$INSTALL_DIR/gen"

echo "==> Installing Quadlet units to $QUADLET_DIR"
mkdir -p "$QUADLET_DIR"
cp "$SCRIPT_DIR"/stocker-list.build "$QUADLET_DIR/"
cp "$SCRIPT_DIR"/stocker-list.container "$QUADLET_DIR/"

echo "==> Installing environment file to $ENV_DIR"
mkdir -p "$ENV_DIR"
cp "$PROJECT_DIR"/.env.podman "$ENV_DIR/"
chmod 600 "$ENV_DIR/.env.podman"

echo "==> Reloading systemd"
systemctl daemon-reload

echo ""
echo "Done. Next steps:"
echo ""
echo "  1. Edit $ENV_DIR/.env.podman and fill in DB_USER, DB_PASSWORD"
echo ""
echo "  2. Build the image:"
echo "       systemctl start tsx-tracker-build.service"
echo ""
echo "  3. Enable and start the service:"
echo "       systemctl enable --now tsx-tracker.service"
echo ""
echo "  4. Check status:"
echo "       systemctl status tsx-tracker"
echo "       podman logs tsx-tracker"
