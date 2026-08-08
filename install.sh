#!/usr/bin/env bash
# ip-relay — one-shot install & update
#
#   bash install.sh                 fresh install (or update) into /opt/ip-relay
#   bash install.sh --dir /custom   install into /custom
#   bash install.sh --docker        run via Docker instead of systemd
#   bash install.sh --update        force update of an existing install
#
# Safe: keeps settings.json + .env, backs up code before overwriting, works as
# root or sudo, only needs curl + python3 (venv) or docker.
set -euo pipefail

APP_DIR="/opt/ip-relay"
MODE="auto"          # auto | update | docker | manual
REPO="sajjadgazergar-work/ip-relay"
TAG="v0.3.0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)  APP_DIR="$2"; shift 2 ;;
    --docker) MODE="docker"; shift ;;
    --update) MODE="update"; shift ;;
    --manual) MODE="manual"; shift ;;
    *) echo "unknown flag: $1"; exit 1 ;;
  esac
done

log()  { printf '\033[1;36m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33mWARN\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31mERROR\033[0m %s\n' "$*" >&2; exit 1; }

command -v curl >/dev/null || die "curl is required (apt install curl)"
if [[ "$MODE" == "docker" ]]; then
  command -v docker >/dev/null || die "docker is required for --docker mode"
else
  command -v python3 >/dev/null || die "python3 is required"
fi

# ── decide action ────────────────────────────────────────────────
if [[ "$MODE" == "auto" && -d "$APP_DIR" && -f "$APP_DIR/ip_relay.py" ]]; then
  MODE="update"
fi
if [[ "$MODE" == "auto" ]]; then
  MODE="install"
fi

# ── fetch the release ────────────────────────────────────────────
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
log "Fetching ${REPO}@${TAG}..."
curl -fsSL "https://github.com/${REPO}/archive/refs/tags/${TAG}.tar.gz" -o "$TMP/relay.tar.gz" \
  || die "download failed — check network / tag"
tar -xzf "$TMP/relay.tar.gz" -C "$TMP"
SRC="$TMP/ip-relay-${TAG#v}"

# ── docker mode ──────────────────────────────────────────────────
if [[ "$MODE" == "docker" ]]; then
  log "Building Docker image ip-relay:${TAG}..."
  docker build -t "ip-relay:${TAG}" "$SRC"
  log "Running container (port 8080 → 8080)..."
  docker rm -f ip-relay >/dev/null 2>&1 || true
  docker run -d --name ip-relay --restart unless-stopped \
    -p 8080:8080 -e PORT=8080 \
    -v "${APP_DIR}:/data" \
    "ip-relay:${TAG}"
  sleep 2
  curl -s http://127.0.0.1:8080/healthz && echo && log "Done. Dashboard: http://<server>:8080"
  exit 0
fi

# ── systemd / venv mode ──────────────────────────────────────────
mkdir -p "$APP_DIR"
cd "$APP_DIR"

# preserve user config
if [[ -f "$APP_DIR/ip_relay.py" ]]; then
  cp "$APP_DIR/ip_relay.py" "$APP_DIR/ip_relay.py.bak"
fi
[[ -f "$APP_DIR/main.py" ]] && cp "$APP_DIR/main.py" "$APP_DIR/main.py.bak" || true

# code
cp "$SRC/ip_relay.py" "$SRC/main.py" "$APP_DIR/"
# env — create only if missing (never clobber existing config)
if [[ ! -f "$APP_DIR/.env" ]]; then
  cp "$SRC/.env.example" "$APP_DIR/.env"
  chmod 600 "$APP_DIR/.env"
  log "Created .env — edit it to change the upstream/key (optional)"
fi

# venv + deps
if [[ ! -d "$APP_DIR/.venv" ]]; then
  log "Creating Python venv..."
  python3 -m venv "$APP_DIR/.venv"
fi
log "Installing dependencies..."
"$APP_DIR/.venv/bin/pip" install --upgrade pip -q
"$APP_DIR/.venv/bin/pip" install -q -r "$SRC/requirements.txt"

# systemd unit (skip in manual mode)
if [[ "$MODE" != "manual" ]]; then
  log "Installing systemd service..."
  cp "$SRC/deploy/ip-relay.service" /etc/systemd/system/ip-relay.service
  systemctl daemon-reload
  systemctl enable ip-relay >/dev/null 2>&1 || true
  systemctl restart ip-relay

  sleep 3
  if curl -s --max-time 5 http://127.0.0.1:8080/healthz >/dev/null; then
    log "Healthy ✓  Dashboard: http://<this-server>:8080"
  else
    warn "Health check not ready yet — run: journalctl -u ip-relay -f"
  fi
else
  log "Manual mode — start it yourself:"
  echo "    $APP_DIR/.venv/bin/uvicorn ip_relay:app --host 0.0.0.0 --port 8080"
fi
log "Done. Point your client at http://<this-server>:8080/v1"
