# ip-relay

**Turn one free API key into a whole pool of free API keys — automatically.**

[🇮🇷 راهنمای فارسی](README.fa.md) · [Benchmark — does it work?](BENCHMARK.md) · [Dashboard screenshot](https://raw.githubusercontent.com/sajjadgazergar-work/ip-relay/main/docs/dashboard.png)

ip-relay sits between your AI apps and an API provider whose free tier is limited **per IP address**. Instead of hitting the limit after a few dozen requests, ip-relay routes each request through a rotating pool of proxies — so every request can come from a fresh IP, with its own fresh quota.

![Dashboard](https://raw.githubusercontent.com/sajjadgazergar-work/ip-relay/main/docs/dashboard.png)

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
- ✅ **Anthropic-compatible**: `/v1/messages` endpoint for Claude Code & Anthropic SDKs (auto-translated)
- ✅ **Streaming**: full SSE pass-through for chat completions
- ✅ **Web dashboard**: status, live stats, live logs, and settings — no tech knowledge needed
- ✅ **Settings persist** across restarts (via `settings.json` / the dashboard)
- ✅ **Docker + systemd**: one-command deploy on any server

## Quickstart (non-technical)

### One command — install & update

```bash
curl -sL https://raw.githubusercontent.com/sajjadgazergar-work/ip-relay/main/install.sh | bash
```

> Always installs the **latest** code from `main` (no frozen release tag). Re-running the same command updates an existing install in place.

That's it. The script:
- Installs into `/opt/ip-relay` (fresh) **or** updates an existing install automatically
- Creates a Python venv, installs deps, sets up a systemd service that auto-starts
- **Never overwrites your config** (`.env`, `settings.json`) — backs up code first
- Finishes with a health check

**Customize:**
```bash
# install somewhere else
curl -sL ...install.sh | bash -s -- --dir /home/you/ip-relay
# run without systemd (manual)
curl -sL ...install.sh | bash -s -- --manual
# run with Docker instead
curl -sL ...install.sh | bash -s -- --docker
```

Then open **http://localhost:8080** (or your server IP) — you'll see the dashboard.

### Windows (PowerShell)

```powershell
# download the installer (PowerShell — note: `curl` here is Invoke-WebRequest, so use irm):
irm https://raw.githubusercontent.com/sajjadgazergar-work/ip-relay/main/install.ps1 -OutFile install.ps1

# run it (no admin needed):
powershell -ExecutionPolicy Bypass -File .\install.ps1
```

Or all-in-one:

```powershell
iex (irm https://raw.githubusercontent.com/sajjadgazergar-work/ip-relay/main/install.ps1)
```

> Always pulls the **latest** code from `main`. Re-run the same command to update.

The script installs into `%LOCALAPPDATA%\ip-relay`, creates a venv, installs deps, and leaves you a **`start-ip-relay.bat`** you can double-click to run. Flags: `-Dir D:\ip-relay`, `-Manual` (no launcher), `-Docker`.

> ⚠️ If you pasted the `curl -sL ...` Linux command into PowerShell, that's why it failed — PowerShell aliases `curl` to `Invoke-WebRequest`, which doesn't have `-sL`. Use the `irm` form above.

### Docker — manual

```bash
docker run -d --name ip-relay -p 8080:8080 -e PORT=8080 ghcr.io/<you>/ip-relay
```

Then open **http://localhost:8080** in your browser — you'll see the dashboard.

### From source (developers)

```bash
git clone https://github.com/sajjadgazergar-work/ip-relay
cd ip-relay
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn ip_relay:app --host 0.0.0.0 --port 8080
```

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
Model:    deepseek-v4-flash-free   (no prefix — see note below)
```

> **⚠️ Prefix confusion (important):**
> - **Directly to the relay** — use the **bare model name** (`deepseek-v4-flash-free`, `claude-fable-5`, ...). The relay strips any prefix, so `whatever/deepseek-v4-flash-free` also works.
> - **Through 9router** (or another aggregator) — the model gets the **provider prefix you configured** in the aggregator, e.g. `ocr/deepseek-v4-flash-free`. That prefix lives in 9router's config, **not** in the relay.
> - **Claude Code** — set `ANTHROPIC_BASE_URL=http://<server>:8080/v1` and `ANTHROPIC_AUTH_TOKEN=public`, and the model id is the bare name.

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
| `PROXY_REFRESH_SEC` | `600` | How often to re-pull the public proxy lists (seconds) |
| `PROXY_TEST_CONCURRENCY` | `25` | Proxies tested in parallel |
| `PROXY_MAX_CANDIDATES` | `2000` | Max untested candidates held in the reservoir |
| `PROXY_POOL_TARGET` | `25` | Working lanes the churner tries to maintain |
| `PROBE_TIMEOUT` | `20` | Timeout (s) for the real upstream probe |
| `DIRECT_LANE` | `1` | Allow direct (your server IP) egress |
| `PROBE_MODEL` | `deepseek-v4-flash-free` | Model used to test proxies |
| `ALLOW_SOCKS` | *(off)* | Also fetch protocol:// lists (needs `httpx[socks]`) |
| `PORT` | `8080` | Listen port |

Legacy aliases `OPENCODE_API_KEY` / `OPENCODE_BASE_URL` still work.

## How the pool stays alive (v0.6+)

Free proxies die fast and many public ones are already rate-limited by other
users. v0.6 introduced a highly resilient multi-tier proxy architecture:

1. **A candidate reservoir** — thousands of SOCKS4/5 and HTTP/S `ip:port` entries pulled from 20+ public lists and custom Webshare API keys.
2. **A continuous churner** — tests a small batch every few seconds:
   stage 1 = cheap TCP/HTTP check (6s, no upstream quota cost),
   stage 2 = real 1-token upstream request through the proxy.
3. **Scored lanes & latency ranking** — every lane carries an EWMA score and measured latency; requests are automatically routed to the fastest healthy lane.
4. **Transparent request failover** — when a proxy lane fails mid-request, it is silently bypassed and retried on the next-best lane without the client ever detecting it.
5. **Webshare key integration** — configure multiple Webshare API tokens via the dashboard UI or `.env` settings to automatically populate high-quality authenticated proxy lanes.
6. **Auto top-up** — when working lanes drop below `PROXY_POOL_TARGET`, the churner keeps testing until the pool refills. Burned lanes are parked for cooldown, dead ones are removed automatically.

First working lanes usually appear within 2–5 minutes of startup; the pool
then keeps topping itself up. Watch it live on the dashboard's **Pool status**
panel.

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
