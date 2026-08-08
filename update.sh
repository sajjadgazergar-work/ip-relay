#!/usr/bin/env bash
# Update an existing ip-relay install (from git, via install.sh, or manual).
# Usage: bash update.sh [APP_DIR]
# Default APP_DIR: /opt/ip-relay (matches install.sh). Pass another path for desktop installs.
set -euo pipefail

APP_DIR="${1:-/opt/ip-relay}"
echo "==> Updating ip-relay at $APP_DIR"

if [ ! -f "$APP_DIR/ip_relay.py" ]; then
  echo "ERROR: $APP_DIR/ip_relay.py not found — is this the right install dir?"
  echo "Usage: bash update.sh /path/to/your/install"
  exit 1
fi

# fetch the latest release
TMP="$(mktemp -d)"
echo "==> Downloading v0.3.0..."
curl -sL https://github.com/sajjadgazergar-work/ip-relay/archive/refs/tags/v0.3.0.tar.gz -o "$TMP/relay.tar.gz"
tar -xzf "$TMP/relay.tar.gz" -C "$TMP"
SRC="$TMP/ip-relay-0.3.0"

# backup current code
cp "$APP_DIR/ip_relay.py" "$APP_DIR/ip_relay.py.bak"
cp "$APP_DIR/main.py" "$APP_DIR/main.py.bak" 2>/dev/null || true

# replace code
cp "$SRC/ip_relay.py" "$SRC/main.py" "$APP_DIR/"
echo "==> Code updated. (backups: ip_relay.py.bak, main.py.bak)"

# deps
echo "==> Syncing dependencies..."
. "$APP_DIR/.venv/bin/activate" 2>/dev/null && pip install -q -r "$SRC/requirements.txt" || echo "no venv found at $APP_DIR/.venv — skip deps"

# restart if systemd-managed, else just print instructions
if systemctl is-active --quiet ip-relay 2>/dev/null; then
  echo "==> Restarting systemd service..."
  systemctl restart ip-relay
  sleep 2
  curl -s http://127.0.0.1:8080/healthz || echo "not up yet — journalctl -u ip-relay -f"
elif systemctl is-active --quiet oc-rotator 2>/dev/null; then
  echo "==> Restarting oc-rotator service..."
  systemctl restart oc-rotator
  sleep 2
  curl -s http://127.0.0.1:8080/healthz || echo "check: journalctl -u oc-rotator -f"
else
  echo "==> No systemd service found for ip-relay. Restart it yourself:"
  echo "    uvicorn ip_relay:app --host 0.0.0.0 --port 8080"
fi

rm -rf "$TMP"
echo
echo "Done. Your ip-relay is now v0.3.0 (check the dashboard /healthz for stats)."