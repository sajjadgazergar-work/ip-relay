# ip-relay // Egress Proxy Rotator

**Turn any per-IP rate-limited OpenAI-compatible endpoint into a high-concurrency rotating proxy pool.**

[🇮🇷 راهنمای فارسی (Persian)](README.fa.md) · [Benchmark](BENCHMARK.md) · [Changelog v1.1 (فارسی)](docs/CHANGELOG-v1.1.fa.md)

---

`ip-relay` sits between your AI applications and any OpenAI-compatible API provider whose free tier or rate limits are enforced **per IP address**.

Instead of getting blocked by `429 Too Many Requests` after exhausting your single server IP quota, `ip-relay` dynamically routes requests through a continuous pool of rotating proxy lanes (HTTP/S, SOCKS4/5, Webshare, Tor circuits). Each request egresses through a fresh IP carrying its own independent quota.

![Dashboard Screenshot](https://raw.githubusercontent.com/sajjadgazergar-work/ip-relay/main/docs/dashboard.png)

<sub>The dashboard also ships a full Persian (RTL) interface — switch it in **Settings → Interface**:</sub>

![Persian Dashboard](https://raw.githubusercontent.com/sajjadgazergar-work/ip-relay/main/docs/dashboard-fa.png)

> ⚠️ **Notice**: This tool is designed to route requests across independent egress IPs to prevent single-IP rate-limit exhaustion. Always verify and comply with your upstream provider's terms of service.

---

## 🔑 The thing that actually matters (v0.9)

`opencode.ai/zen/v1` gates its free tier on **two** independent things: the egress IP's budget **and** the `User-Agent` header. The UA gate was the reason nothing in this project worked.

Measured 2026-08-15 through **one single proxy egress IP** (`http://91.236.153.5:3128`, 1269ms), one key, requests interleaved back-to-back (`/tmp/uamatrix_proxy.py`):

| `User-Agent` sent | Result |
|---|---|
| `opencode/1.0` · `opencode` · `OpenCode/1.0` · `opencode-ai/1.0` · `opencode/1.0 (linux; x64)` | **200 OK — 5/5** |
| `python-httpx/0.27.0` · `curl/8.5.0` · `Mozilla/5.0 … Chrome/127` · `axios/1.7.2` · `Go-http-client/2.0` | **429 `FreeUsageLimitError` — 0/5** |
| *(no UA header at all)* | `403 error code: 1010` (Cloudflare) |

Any UA whose lowercase form contains `opencode` passes; everything else is refused instantly on an IP that answers 200 one second earlier. Up to v0.8.1 this relay sent a Chrome UA, so **every** request 429'd, the prober concluded every proxy was "burned", and the pool starved. The rotation machinery was never the problem.

Consequences worth knowing:

- **Rotation is still required.** Each IP does have a finite free budget; once spent, that IP 429s even with the correct UA. Correct UA + rotation is what gives sustained throughput.
- `UPSTREAM_USER_AGENT` is a first-class setting, defaults to `opencode/1.0`, and is **rejected** (dashboard validation + startup warning) if it does not contain `opencode`.
- A client's own UA is forwarded only when it already says `opencode`; anything else is rewritten, because forwarding it guarantees a 429. Your client can send whatever it likes.
- The prober does a real 1-token completion again (v0.8.1 had reduced it to a `/models` reachability check — `/models` answers `200` from fully quota-burned IPs, so screening alone promoted dead lanes).
- A 429 from a lane now **parks** that lane (`burn=True`). Before, the burned lane stayed warm and got picked again immediately — that is how a 1.4k-request run logged 15k failovers.

Measured through the relay after the fix, client sending a deliberately wrong UA (`python-requests/2.31`, rewritten by the relay):

```
N=200  concurrency=8   wall=88.5s   codes={200: 200}
p50=2.5s  p95=6.5s  max=14.8s   24,047 tokens   0 errors
```

---

## 🆕 What's new in v1.1

Everything below was measured on this box, not estimated.

**The dashboard was rendering at 0.7 fps.** A full-resolution WebGL backdrop forced 22 `backdrop-filter` regions to re-blur every frame — the two costs multiply, they don't add. Both dashboards were served from the same live relay and sampled with `requestAnimationFrame`, median of 3 runs:

| Build | Median frame | ~fps |
|---|---|---|
| v1.0 as shipped | 1416.6 ms | 0.7 |
| **v1.1 active** | **75.0 ms** | **13.3** |
| **v1.1 idle** | **50.0 ms** | **20.0** |

**19× faster while you interact, 28× when idle.** The glass is now baked into gradients (one real blur survives, on the modal backdrop, where the shader is paused anyway), the shader renders at half resolution capped to 30 fps, and it stops scheduling entirely after 4 s of no interaction — pointer, keyboard, scroll, or relay traffic wakes it.

- **Token metering per lane and globally.** `/api/usage` and the dashboard's hero KPI now show what each egress burned. Verified live: a non-stream request moved the meter `+85 in / +12 out`, exactly matching the upstream's reported usage; a streaming request `+86 / +25` with tokens backfilled after `[DONE]`. Per-lane attribution makes "which IP spent the quota" answerable.
- **Persian (RTL) interface.** English stays the default; Persian is opt-in in **Settings → Interface** and persists server-side as `ui_lang`. RTL is done with logical CSS properties, not a mirrored stylesheet. IPs, telemetry, and code stay LTR inside the RTL layout, because a bidi-reordered IP address is a *different* address.
- **Zero external requests.** Inter and Vazirmatn are self-hosted under `/static`. The Google Fonts stylesheet measured **6.96 s** from Iran and blocked first paint; that dependency is gone.
- **Gzipped dashboard: 125,296 → 30,505 bytes**, without breaking SSE. Starlette's stock `GZipMiddleware` coalesces streams — a real-socket probe showed 2 wire chunks instead of 7 and first byte at 1.26 s instead of 0.02 s — so the relay uses a single-shot middleware that skips `text/event-stream` entirely.
- **`/metrics` now requires auth** (`metrics_require_auth`, default on). It was unauthenticated before, which leaked lane addresses and traffic volume to anyone who could reach the port.
- **Gooey topology visualiser.** Lanes melt into the relay core as they close in (blur + contrast snap on a quarter-area offscreen canvas). At full resolution this cost 200 ms/frame on a GPU-less box, which is why it renders small.

**171 tests pass.** Persian copy is gated by a deterministic linter (`tools/persian_lint_dashboard.py`) over a single source-of-truth string table that also generates the JS dict and stamps `data-i18n` — 188 strings, 0 errors.

Full Persian changelog: [docs/CHANGELOG-v1.1.fa.md](docs/CHANGELOG-v1.1.fa.md)

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
- **📊 Live Telemetry Dashboard**: Dark-glass dashboard with a gooey egress-topology visualiser, hero KPIs (warm lanes / tokens spent / p95), sparklines, live status table, streaming logs, and a settings editor. English and Persian (RTL), fully self-hosted fonts, no external requests.
- **📈 Per-Lane Token Metering**: Every request's prompt/completion tokens are attributed to the egress lane that served it, so you can see which IP burned which quota (`/api/usage`, authenticated `/metrics`).
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

Dashboard-only settings (edit in the UI, stored in `settings.json`):

| Setting | Default | Description |
|---|---|---|
| `ui_lang` | `en` | Dashboard language: `en` or `fa` (Persian, RTL) |
| `metrics_require_auth` | `true` | Require the relay key on `/metrics` (it exposes lane addresses and volume) |
| `tor_enabled` | `false` | Supply egress from local Tor circuits (1,384 exit IPs, ~65% usable vs ~2% for scraped lists) |
| `tor_lanes` | `4` | Number of independent Tor circuit lanes |
| `tor_socks_port` | `9150` | Local Tor SOCKS port |
| `burn_memory` | `true` | Remember spent (429'd) IPs instead of re-probing them |
| `burn_ttl_sec` | `86400` | How long a burned IP stays remembered |

> 💡 **Troubleshooting Residential Networks (e.g. in Iran)**:
> If you are running `ip-relay` on a home network, high test concurrency can saturate your router's NAT tables, causing connection timeouts, packet loss, or `503 Too many open connections` errors.
> - **Decrease Concurrency**: Change `PROXY_TEST_CONCURRENCY` to `15` or `25` in your settings or `.env` to prevent overloading your home router.
> - **Censorship Resilience**: The prober automatically checks connection directly against your upstream's `/models` endpoint and falls back to Cloudflare/Firefox, avoiding issues with censored test targets (like Google) on local residential connections.

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

# Run unit tests (171 passed, fully mocked — no network)
IP_RELAY_NO_BACKGROUND=1 pytest -q
```

### Dashboard & i18n tooling

The Persian strings live in ONE table, `tools/i18n_strings.py`, which feeds three
consumers — so they cannot drift apart:

```bash
python3 tools/persian_lint_dashboard.py   # deterministic Persian checks (ZWNJ, letterforms, digits)
python3 tools/i18n_gen.py                 # regenerate the JS I18N dict inside dashboard.html
python3 tools/i18n_annotate.py            # stamp data-i18n="key" onto the markup (idempotent)
```

Run all three after editing any UI copy. `i18n_annotate.py` reports keys it could
not find, which is the signal that the English text drifted and a key is dead.

Verification harnesses (need a running relay + `RELAY_KEY` in the environment):

```bash
python3 tools/verify_dashboard_v10.py       # asserts dashboard features in a real browser
python3 tools/perf_compare_v10_v11.py       # frame-cost A/B, both builds off the same relay
```

---

## 📄 License

Distributed under the [MIT License](LICENSE).
