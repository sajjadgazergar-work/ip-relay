# ip-relay

**Turn one free API key into a whole pool of free API keys — automatically.**

ip-relay sits between your AI apps and an API provider whose free tier is limited **per IP address**. Instead of hitting the limit after a few dozen requests, ip-relay routes each request through a rotating pool of proxies — so every request can come from a fresh IP, with its own fresh quota.

> ⚠️ **Please read this first**: this tool works around rate limits that providers set per IP. Check the upstream provider's terms of service before using it, and don't be a jerk with it. The maintainers are not responsible for how you use it.

## What problem does this solve?

| Without ip-relay | With ip-relay |
|---|---|
| Your app hits `deepseek-v4-flash-free` from one IP | Your app hits it from **42 different IPs** |
| After ~N requests/day: **429 — quota exhausted** | Each IP carries its own quota |
| You wait 24h for the limit to reset | Rotation happens automatically on quota errors |

## Features

- ✅ **Zero-config default**: works out of the box against opencode's free tier (just run it)
- ✅ **Rotates egress IPs automatically** when the quota error appears
- ✅ **OpenAI-compatible**: drop-in for any OpenAI client, gateway, or aggregator
- ✅ **Streaming**: full SSE pass-through for chat completions
- ✅ **Web dashboard**: status, live stats, live logs, and settings — no tech knowledge needed
- ✅ **Settings persist** across restarts (via `settings.json` / the dashboard)
- ✅ **Docker + systemd**: one-command deploy on any server

## Quickstart (non-technical)

### Option A — Docker (easiest)

```bash
docker run -d --name ip-relay -p 8080:8080 -e PORT=8080 ghcr.io/<you>/ip-relay
```

Then open **http://localhost:8080** in your browser — you'll see the dashboard.

### Option B — One-shot server install

```bash
# on a fresh Ubuntu/Debian server (root or sudo):
curl -L <release-url>/install.sh | bash
```

Then open **http://<your-server-ip>:8080** in your browser.

### Option C — From source

```bash
git clone https://github.com/sajjadgazergar-work/ip-relay
cd ip-relay
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn ip_relay:app --host 0.0.0.0 --port 8080
```

Then open **http://localhost:8080**.

## Using the dashboard

The web UI at `/` shows you:

- **Status** — is the relay healthy?
- **Proxy pool** — how many working egress IPs are ready
- **Requests / Rotations** — live usage counters
- **Configuration** — change the upstream URL/key, proxy refresh rate, etc. (no editing files)
- **Live log** — what the relay is doing right now

## Connecting your AI app

Point any OpenAI-compatible client at your server:

```
Base URL: http://<your-server>:8080/v1
API key:  whatever you set as the relay key (or leave blank)
```

Example with curl:

```bash
curl http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer public' \
  -d '{"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"hi"}]}'
```

Works with:
- **OpenAI SDKs** (Python, Node, etc.) — just change `base_url`
- **9router / OpenRouter-style aggregators** — add it as a provider
- **Anything that speaks the OpenAI API**

## Configuration

All settings are editable from the dashboard, or via environment variables:

| Variable | Default | Description |
|---|---|---|
| `UPSTREAM_BASE_URL` | `https://opencode.ai/zen/v1` | Upstream OpenAI-compatible API |
| `UPSTREAM_API_KEY` | `public` | Key sent upstream (opencode free tier uses `public`) |
| `RELAY_API_KEY` | *(empty)* | Optional — protect your relay & dashboard with a key |
| `PROXY_REFRESH_SEC` | `600` | How often to refresh the proxy pool (seconds) |
| `PROXY_TEST_CONCURRENCY` | `12` | Proxies tested in parallel during refresh |
| `PROXY_MAX_CANDIDATES` | `150` | Max candidates scanned per refresh |
| `DIRECT_LANE` | `1` | Allow direct (your server IP) egress |
| `PROBE_MODEL` | `deepseek-v4-flash-free` | Model used to test proxies |
| `PORT` | `8080` | Listen port |

Legacy aliases `OPENCODE_API_KEY` / `OPENCODE_BASE_URL` still work.

## How it works

```
your app / gateway
      │  OpenAI-compatible requests
      ▼
  ip-relay ──▶ proxy pool (rotating egress IPs)
      │            ▲
      │            └── quota error (429) → mark IP burned, try next
      ▼
  upstream API (opencode.ai/zen/v1)
```

- The relay **fetches** free HTTP proxies from public lists
- It **tests** each one with a real 1-token request (proves it works *and* its IP isn't already burned)
- On a quota error it **parks** the burned IP and tries the next lane — transparently
- Your server's real IP **never touches the upstream** (unless you enable `DIRECT_LANE`)

## Development

```bash
pip install -r requirements-dev.txt
ruff check .          # lint
pytest                # tests (17 tests, network-free, mocked upstream)
```

## License

MIT
