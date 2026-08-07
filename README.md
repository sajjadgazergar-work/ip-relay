# ip-relay

Rotating egress relay for per-IP-quota OpenAI-compatible APIs.

Sits between your gateway/client and an upstream API whose free tier is limited **per IP address** (e.g. `opencode.ai/zen/v1`). Each unique egress IP carries its own quota, so this relay rotates through a proxy pool on quota errors and lets one base URL serve more free-model traffic than a single IP ever could. Your server's real IP never touches the upstream.

> ⚠️ **Responsible use**: this tool works around rate limits that providers impose per IP. Read the upstream's terms before deploying. You're responsible for how you use it; the maintainers are not.

## How it works

```
your client / 9router / aggregator
        │  OpenAI-compatible (POST /v1/chat/completions)
        ▼
   ip-relay ──▶ proxy pool (rotates egress IPs)
        │         ▲
        │         └── on FreeUsageLimitError / 429 → mark IP burned, try next
        ▼
   upstream (opencode.ai/zen/v1, or any compatible API)
```

- **OpenAI-compatible**: `/v1/chat/completions` (stream + non-stream), `/v1/models`, `/healthz`.
- **Rotation**: on quota errors the offending egress IP is parked in cooldown and the next lane is tried, transparently.
- **Pool**: fetches free HTTP proxies from public lists, validates each with a real 1-token probe through the proxy (proves both liveness *and* that the IP isn't already burned).
- **Lane ordering**: direct egress (your server IP) first when healthy, then the proxy pool — with the direct lane automatically parked if it gets burned.
- **Safe streaming**: pass-through SSE that survives upstream mid-stream closes (no dangling clients).

## Quickstart

```bash
git clone https://github.com/<you>/ip-relay && cd ip-relay
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn ip_relay:app --host 0.0.0.0 --port 8080
```

Then point your client at `http://localhost:8080/v1`:

```bash
curl http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer public' \
  -d '{"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"hi"}]}'
```

## Docker

```bash
docker build -t ip-relay .
docker run -d --name ip-relay -p 8080:8080 -e PORT=8080 ip-relay
```

## Configuration

All via environment variables:

| Variable | Default | Description |
|---|---|---|
| `UPSTREAM_BASE_URL` | `https://opencode.ai/zen/v1` | Upstream OpenAI-compatible base URL |
| `UPSTREAM_API_KEY` | `public` | Key sent upstream (opencode free tier uses literal `public`; not secret) |
| `RELAY_API_KEY` | *(empty)* | If set, require this Bearer on incoming requests |
| `PROXY_REFRESH_SEC` | `600` | Seconds between proxy pool refreshes |
| `PROXY_TEST_CONCURRENCY` | `12` | Concurrent probes during pool refresh |
| `PROXY_MAX_CANDIDATES` | `150` | Max candidates scanned per refresh |
| `DIRECT_LANE` | `1` | Allow direct (server IP) egress as a lane |
| `PROBE_MODEL` | `deepseek-v4-flash-free` | Model used for pool validation probes |
| `PORT` | `8080` | Listen port |

### Deprecated aliases
`OPENCODE_API_KEY` → `UPSTREAM_API_KEY`, `OPENCODE_BASE_URL` → `UPSTREAM_BASE_URL` (accepted for backward compat).

## Integrating with 9router

In 9router's dashboard, add an OpenAI-compatible provider pointing at ip-relay:

```
baseUrl: http://<ip-relay-host>:8080/v1
apiKey:  (anything; the relay uses its own UPSTREAM_API_KEY / RELAY_API_KEY)
```

Model ids that carry a provider prefix (e.g. `ocr/deepseek-v4-flash-free`) get the leading segment stripped before hitting upstream.

## Development

```bash
pip install -r requirements-dev.txt
ruff check .          # lint
pytest                # tests (network-free, mocked upstream)
```

## Why this exists / background

Verified against opencode's free tier (2026-08-07):

- Fresh IP + `Bearer public` → **200**
- Fresh IP + fake key → `AuthError` (keys ARE validated)
- Burned IP + any key → `FreeUsageLimitError`

So the game is pure IP rotation; keys are secondary. This project generalizes that to any per-IP-quota API.

## License

MIT
