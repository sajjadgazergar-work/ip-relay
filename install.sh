#!/usr/bin/env bash
# One-shot install for a fresh server (Ubuntu/Debian, root or sudo).
# Usage: bash install.sh
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/ip-relay}"

echo "==> Installing ip-relay to $APP_DIR"
mkdir -p "$APP_DIR"
cp ip_relay.py main.py "$APP_DIR/"
cp .env.example "$APP_DIR/.env"
chmod 600 "$APP_DIR/.env"

echo "==> Creating venv + installing deps"
cd "$APP_DIR"
python3 -m venv .venv
.venv/bin/pip install --upgrade pip -q
.venv/bin/pip install -r /tmp/ip-relay-install/requirements.txt -q

echo "==> Installing systemd unit (edit .env first if needed!)"
cp /tmp/ip-relay-install/deploy/ip-relay.service /etc/systemd/system/ip-relay.service
systemctl daemon-reload
systemctl enable --now ip-relay

sleep 2
echo "==> Health check:"
curl -s http://127.0.0.1:8080/healthz || echo "not up yet — check: journalctl -u ip-relay -f"
echo
echo "Done. Point your client at http://<this-server-ip>:8080/v1"