# ip-relay // Egress Proxy Rotator

**Turn any per-IP rate-limited OpenAI-compatible endpoint into a high-concurrency rotating proxy pool.**

[🇮🇷 راهنمای فارسی (Persian)](README.fa.md) · [Benchmark](BENCHMARK.md) · [Dashboard Screenshot](https://raw.githubusercontent.com/sajjadgazergar-work/ip-relay/main/docs/dashboard.png)

---

`ip-relay` sits between your AI applications and any OpenAI-compatible API provider whose free tier or rate limits are enforced **per IP address**.

Instead of getting blocked by `429 Too Many Requests` after exhausting your single server IP quota, `ip-relay` dynamically routes requests through a continuous pool of rotating proxy lanes (HTTP/S, SOCKS4/5, Webshare). Each request egresses through a fresh IP carrying its own independent quota.

![Dashboard Screenshot](https://raw.githubusercontent.com/sajjadgazergar-work/ip-relay/main/docs/dashboard.png)

> ⚠️ **Notice**: This tool is designed to route requests across independent egress IPs to prevent single-IP rate-limit exhaustion. Always verify and comply with your upstream provider's terms of service.

---

## ⚡ What Problem Does This Solve?

| Scenario | Without ip-relay | With ip-relay |
|---|---|---|
| **Egress IP** | All requests originate from your 1 server IP | Requests rotate across **40+ active proxy IPs** |
| **Rate Limit** | Hits `429 Rate Limit Exceeded` after N requests | Each proxy lane maintains its own isolated quota |
| **Downtime** | Must wait 24h for single-IP quota resets | **Transparent failover**: failed IPs auto-bypass instantly |
| **Compatibility** | Locked to 1 provider API format | Speaks **OpenAI API** & **Anthropic (`/v1/messages`)** |

---

## ✨ Features

- **🌐 Provider Agnostic**: Works out of the box with `opencode.ai/zen/v1`, Groq, SambaNova, Together, DeepSeek, or any custom OpenAI-compatible endpoint.
- **🔄 Transparent Mid-Request Failover**: If a proxy lane dies or gets rate-limited mid-request, `ip-relay` silently retries on the next-best lane without failing your client call.
- **⚡ EWMA Scored Lanes & Latency Ranking**: Tracks lane health, latency, and success rates. Fast, healthy proxies automatically win request routing.
- **🔑 Webshare Multi-Account Integration**: Paste multiple Webshare API tokens (one per line, comma, or semicolon) via the dashboard to combine multiple free or paid proxy accounts into one pool.
- **🤖 Anthropic & Claude Code Protocol Gateway**: Native `/v1/messages` endpoint with full tool-calling, system prompts, and SSE streaming translation.
- **📊 Spatial 3D Network Telemetry Dashboard**: High-polish dark glassmorphism dashboard with an interactive 3D spatial node visualizer, live status table, logs, and settings editor.
- **🛡️ Secure Credential Protection**: Raw API keys stay encrypted locally in `settings.json` (git-ignored) and return masked in UI payloads.
- **🚀 One-Command Deployment**: Instant setup for Linux/macOS (`bash`), Windows (`PowerShell`), Docker, or systemd.

---

## 📦 Quickstart

### 1. Linux & macOS (One-Command Install & Update)

```bash
curl -sL https://raw.githubusercontent.com/sajjadgazergar-work/ip-relay/main/install.sh | bash
```

> **Note**: Re-running the installer command automatically updates an existing installation to the latest version on `main` without overwriting your `.env` or `settings.json` configuration.

**Custom Install Options:**
```bash
# Custom directory
curl -sL ...install.sh | bash -s -- --dir /opt/ip-relay

# Run manually (without systemd)
curl -sL ...install.sh | bash -s -- --manual

# Deploy via Docker container
curl -sL ...install.sh | bash -s -- --docker
```

### 2. Windows (PowerShell)

Run in PowerShell (no Administrator privileges required):

```powershell
iex (irm https://raw.githubusercontent.com/sajjadgazergar-work/ip-relay/main/install.ps1)
```

> **Note**: Avoid using `curl` in PowerShell (PowerShell aliases `curl` to `Invoke-WebRequest` which lacks `-sL`). Use the `irm` command above.

### 3. Docker (Manual)

```bash
docker run -d \
  --name ip-relay \
  -p 8080:8080 \
  -e PORT=8080 \
  -e UPSTREAM_BASE_URL="https://opencode.ai/zen/v1" \
  ghcr.io/sajjadgazergar-work/ip-relay:latest
```

### 4. From Source

```bash
git clone https://github.com/sajjadgazergar-work/ip-relay.git
cd ip-relay
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn ip_relay:app --host 0.0.0.0 --port 8080
```

Access the Web Dashboard at **http://localhost:8080** (or `http://<your-server-ip>:8080`).

---

## 🔌 Connecting Your AI Applications

Point any OpenAI-compatible client, SDK, or gateway at your relay:

```
Base URL:  http://<server-ip>:8080/v1
API Key:   (Your RELAY_API_KEY, or leave blank if unset)
Model:     deepseek-v4-flash-free (or any model hosted by your upstream)
```

### Example: curl (OpenAI Format)
```bash
curl http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer public" \
  -d '{
    "model": "deepseek-v4-flash-free",
    "messages": [{"role": "user", "content": "Hello world!"}]
  }'
```

### Example: Claude Code CLI
```bash
export ANTHROPIC_BASE_URL="http://localhost:8080/v1"
export ANTHROPIC_AUTH_TOKEN="public"
claude
```

### Example: Python OpenAI SDK
```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/v1",
    api_key="public"
)

response = client.chat.completions.create(
    model="deepseek-v4-flash-free",
    messages=[{"role": "user", "content": "Explain relativity briefly."}]
)
print(response.choices[0].message.content)
```

---

## ⚙️ Configuration Reference

All settings can be configured via environment variables or directly edited live through the **Web Dashboard** (`settings.json` overlays automatically):

| Environment Variable | Default Value | Description |
|---|---|---|
| `UPSTREAM_BASE_URL` | `https://opencode.ai/zen/v1` | Upstream OpenAI-compatible API base endpoint URL |
| `UPSTREAM_API_KEY` | `public` | API Bearer key forwarded to the upstream provider |
| `RELAY_API_KEY` | *(empty)* | Optional authentication key to protect your relay & dashboard |
| `PROBE_MODEL` | `deepseek-v4-flash-free` | Model ID used to test candidate proxy lanes |
| `PROXY_POOL_TARGET` | `30` | Number of healthy warm proxy lanes to maintain |
| `PROXY_MAX_CANDIDATES` | `3000` | Maximum candidate proxies held in the testing queue |
| `PROXY_TEST_CONCURRENCY` | `60` | Concurrent proxy testing connections |
| `PROXY_PROBE_TIMEOUT` | `25` | Timeout (seconds) for 1-token proxy candidate probing |
| `RELAY_PROXY_TIMEOUT` | `40` | Max duration (seconds) allowed for live proxy request routing |
| `RELAY_ATTEMPTS` | `6` | Maximum transparent failover retries per request |
| `LANE_COOLDOWN_SEC` | `90` | Cooldown duration (seconds) before re-testing a rate-limited lane |
| `LANE_RECOVER_SEC` | `240` | Interval (seconds) for re-probing parked lanes |
| `WEBSHARE_TOKEN` | *(empty)* | Webshare API tokens (supports multiple line-by-line keys) |
| `DIRECT_LANE` | `false` | Enable your server's own IP address as an egress lane |
| `ALLOW_SOCKS` | `true` | Enable SOCKS4 and SOCKS5 proxy scraping sources |
| `PORT` | `8080` | HTTP server port |

---

## 🏗️ Architecture & Egress Resilience Engine (v0.6+)

```
  [ Your App / Gateway ]
            │  (OpenAI / Anthropic API Request)
            ▼
┌─────────────────────────────────────────────────────────┐
│                       ip-relay                          │
│                                                         │
│   ┌─────────────────────────────────────────────────┐   │
│   │           Multi-Source Candidate Queue          │   │
│   │  (SOCKS4/5 + HTTP/S + 20+ Feeds + Webshare)     │   │
│   └────────────────────────┬────────────────────────┘   │
│                            │ (Parallel Prober)          │
│                            ▼                            │
│   ┌─────────────────────────────────────────────────┐   │
│   │        EWMA Scored & Latency-Ranked Lanes       │   │
│   └────────────────────────┬────────────────────────┘   │
│                            │ (Transparent Failover)     │
└────────────────────────────┼────────────────────────────┘
                             │ (Egress via Proxy IP)
                             ▼
               [ Upstream API Provider ]
```

1. **Multi-Source Scraping**: Collects fresh egress candidates continuously across SOCKS4, SOCKS5, HTTP/S public feeds, and Webshare API accounts.
2. **Two-Stage Validation**: Candidates undergo cheap TCP screening followed by a 1-token upstream completion test.
3. **EWMA Scoring & Latency Ranking**: Healthy lanes earn high EWMA scores based on response speed and success history; low-latency lanes are prioritized automatically.
4. **Transparent Failover**: On a 429 quota exhaustion or proxy drop, the relay silently re-routes the active connection to the next-best lane within milliseconds.

---

## 🛠️ Development & Testing

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run linter
ruff check .

# Run unit tests (17 passed, fully mocked)
pytest
```

---

## 📄 License

Distributed under the [MIT License](LICENSE).
