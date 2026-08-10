# ip-relay — Road to Exceptional (v0.7.1 → v1.0)

Audit date: 2026-08-10. Codebase: `ip_relay.py` 1684 LOC, `dashboard.html` 2220 LOC, 17 unit tests, systemd + Docker + installers, EN/FA docs.

Verdict: the engine is genuinely good — scored lanes, transparent failover, quota-state backoff, stream peeking, revalidation. What separates it from "exceptional" is **state durability, observability, and the operator trust surface**. Below is a ranked list, hardest-hitting first.

---

## 0. FIXED DURING THIS AUDIT

**Credential leak in `/api/logs`.** The prober logged full lane identity, including Webshare `user:pass@host:port`. `/api/pool` masked userinfo, but the log ring did not — and `/api/logs` is unauthenticated whenever `relay_api_key` is empty (its default). Anyone who could reach port 18080 could scrape working paid proxy credentials from the live log stream.

Fix shipped: `scrub_creds()` + `_CRED_RE` regex applied inside `RingHandler.emit`, so userinfo is stripped before it ever enters `LOG_RING`. Verified: `curl /api/logs | grep -c "@"` → `0` after restart, while masked host:port still shows.

---

## 1. TIER 1 — CORRECTNESS AND TRUST (do these first)

### 1.1 Lane state persistence across restarts
Every restart throws away the entire scored pool and rebuilds from zero: ~40s to first warm lane, ~3min to a healthy pool, and all EWMA history is lost. Warm lanes are the single most expensive asset the process owns and they are currently held only in RAM.

- Write `lanes.json` every 30s: `addr`, `proto`, `score`, `lat_ms`, `ok`, `fails`, `last_ok`.
- On startup, load lanes with `score > 0.3` into `POOL.priority_candidates` (not straight into `lanes` — they must re-verify, but they jump the queue).
- Result: warm pool in ~5s instead of ~3min, and a restart stops being a self-inflicted outage.

### 1.2 Default-deny the control plane
`relay_api_key` defaults to `""`, and `_check_relay_auth` returns `True` when it is empty. On the VPS this means `/api/settings`, `/api/pool`, `/api/logs`, `/api/refresh`, and `POST /api/settings` are world-writable — a stranger can rewrite `upstream_base_url` and point your relay at their collector.

- Generate a random key on first boot if the file has none; print it once to stdout and persist it.
- Alternatively bind to `127.0.0.1` by default and require an explicit `--expose` flag / `BIND_HOST` env for `0.0.0.0`.
- Rate-limit `POST /api/settings` and `/api/keys/validate` (they trigger outbound fetches — a trivial amplification vector).

### 1.3 Test the new v0.7.1 logic
The 17 tests cover `Lane` scoring, addr validation, failover, Anthropic translation. Nothing covers what was just added: `QUOTA_STATE` transitions, `relay_stream` first-chunk peek/failover, `_revalidate_warm`, exponential parked backoff, or `hmac` auth rejection. These are exactly the paths that silently rot.

Add: quota-exhaust → pause → user-success → resume cycle; a stream whose first chunk is a captive-portal HTML page must fail over, not reach the client; `_check_relay_auth` rejects a wrong key and accepts both `Bearer x` and bare `x`; parked backoff caps at 16×.

### 1.4 Structured metrics instead of counter soup
`STATS` is a flat dict of lifetime counters — you cannot answer "was the p95 worse in the last 5 minutes than an hour ago?"

- Keep a `collections.deque(maxlen=1000)` of `(ts, lane, status, latency_ms)` request records.
- Derive p50/p95/p99, per-minute success rate, and failover ratio from that window.
- Expose `/metrics` in Prometheus text format. This is a ~40-line addition that makes the project instantly credible to any infra person who finds it.

---

## 2. TIER 2 — ENGINE INTELLIGENCE (this is where it becomes best-in-class)

### 2.1 Per-lane concurrency limiting
Right now `_pick_lane` rotates the first pick through the top 3 lanes, but nothing stops 20 concurrent requests from stacking on the same fast lane. Free proxies collapse under parallel load — that is the difference between "10 warm lanes" and "10 usable lanes."

Give each `Lane` an `asyncio.Semaphore(2)` and an `inflight` counter; skip lanes at capacity during selection. This alone should measurably cut failover rate under load.

### 2.2 Subnet and ASN diversity
The current pool routinely holds multiple ports of the same host (`198.105.121.200:6462` and siblings) and many IPs from the same Webshare /24. Per-IP rate limits are frequently enforced per-subnet upstream, so a pool of 30 lanes across 3 subnets has an effective width of 3.

- Track `addr.rsplit(".", 1)[0]` (/24) per lane; cap lanes per /24 (default 3).
- Prefer candidates from unseen /24s in `_churn_batch`.
- Surface "unique subnets" as a first-class dashboard number — it is the real capacity metric, not lane count.

### 2.3 Adaptive concurrency instead of a fixed number
`proxy_test_concurrency` is a static 60, and the entire Iran/residential NAT-collapse problem traces to that constant. The daemon should discover its own ceiling.

Start at 20; if the connect-error ratio in a batch exceeds ~40%, halve it; if it stays under 10%, grow by 25% up to the configured max. Log every adjustment. This deletes an entire class of support question and the README troubleshooting section that currently papers over it.

### 2.4 Latency-tiered routing
Streaming requests want the lowest-latency lane; a long non-streaming completion tolerates 4s of connect time. Tag lanes fast (<1.5s) / medium / slow and route by request shape: `stream=true` → fast tier only, batch → any tier. Keeps the good lanes free for the requests where latency is visible to a human.

### 2.5 Cheap upstream reachability preflight
Before an expensive `chat/completions` probe, confirm the lane can reach the upstream host at all with a HEAD/`/models` call. Already partly there via `_revalidate_warm`; making it Stage 1.5 for new candidates would raise probe throughput without spending quota.

### 2.6 Sticky sessions (optional, opt-in)
Some upstreams bind conversation state or anti-abuse fingerprints to the egress IP. An optional `X-Relay-Session: <id>` header that pins a session to a lane (with fallback on burn) makes the relay usable for stateful providers, not just stateless completions.

### 2.7 Provider profiles
The relay is provider-agnostic in config, but every new upstream requires the operator to know its quirks. Ship named profiles (`opencode-zen`, `openrouter-free`, `groq`, `generic-openai`) bundling base URL, probe model, and the 429 signature. One dropdown replaces three text fields and a debugging session.

---

## 3. TIER 3 — AESTHETICS AND OPERATOR EXPERIENCE

The dashboard is already strong (liquid-metal WebGL, glassmorphism, 3D topology). The gaps are polling architecture, accessibility, and narrative.

### 3.1 Replace polling with SSE
Three `setInterval` loops (1s tick, 3s settings+pool, 2.5s logs) hammer the API forever, drift out of sync, and make the log feed feel laggy. One `EventSource` on `/api/stream` pushing `stats`, `pool`, and `log` events is fewer requests, lower latency, and less code. It also makes the visualizer able to animate on real events rather than on a timer.

### 3.2 Event-driven visualizer
Right now the topology animates on a poll cadence. Wire it to real events: a node pulses green the instant a lane serves a request, flashes amber on failover, dims and drifts outward when parked, and a new node flies in on promotion. Same visual language, but it becomes an instrument rather than an ornament.

### 3.3 Accessibility — currently zero
`grep` for `aria-`, `role=`, `tabindex`, `prefers-reduced-motion` in `dashboard.html` returns **0 matches**. For a project meant to be shared publicly this is the most visible quality gap.

- `role="status"` + `aria-live="polite"` on the warm/parked counters and the engine banner.
- `aria-label` on every icon-only control; visible focus rings; full keyboard path through settings.
- `@media (prefers-reduced-motion: reduce)` that stops the WebGL backdrop and node animation — the current design is heavy motion with no escape hatch.
- Verify contrast on the dim glass text against `#070707` with computed WCAG ratios, not by eye.

### 3.4 Timeline / sparkline strip
A 60-second sparkline of warm-lane count, request rate, and p95 latency above the pool table turns "is it working?" into a glance. Pair each stat card with a 30-point mini-chart.

### 3.5 First-run wizard
A new user currently lands on a dashboard full of zeros and has to guess. Three steps: pick a provider profile → paste upstream key (validated live) → optionally add Webshare tokens (validated live), with a progress ring while the first lanes warm and a clear "you are ready" state.

### 3.6 Diagnostics panel
Turn the v0.6.2 log-forensics work into UI. A "Diagnose" button that runs: upstream reachability, key validity, socksio presence, direct-IP burn check, sample-of-5 candidate connect test — then renders a pass/fail checklist with the exact remediation line. That converts every "no warm proxies" support conversation into a single button.

### 3.7 Copy-ready client snippets
A panel that emits the exact env block for Claude Code, OpenAI SDK, curl, and 9router, pre-filled with the reachable base URL and a masked key, with a copy button. Removes the last manual step between install and use.

### 3.8 Terminal aesthetic for logs
Keep the log stream, add level-based coloring, a filter chip row (all / prober / relay / errors), pause-on-hover, and a monospace ligature font. Persist filter state in `localStorage`.

---

## 4. TIER 4 — PROJECT MATURITY

- **Dark/light is not the issue; the README is.** Lead with a 15-second animated GIF of the dashboard warming up. That is what makes a stranger try it.
- **`/api/pool` pagination**: hardcoded `[:50]` slices silently lie once the pool is larger.
- **Docker Compose** with a named volume for `settings.json` + `lanes.json`, plus a healthcheck on `/healthz`.
- **Version endpoint + update notice**: compare `VERSION` against the latest GitHub release, show a subtle badge. You are already tagging releases; use them.
- **Kill the `on_event` deprecation** — move to a `lifespan` handler (two warnings in every test run).
- **`ip_relay.py.bak` / `main.py.bak`** should not ship in archives; add `*.bak` to `.gitignore`.
- **Config validation**: reject nonsense (`relay_attempts=0`, `pool_target=5000`) at the API boundary with a clear error rather than absorbing it.

---

## 5. SUGGESTED SEQUENCE

| Phase | Scope | Payoff |
|---|---|---|
| v0.8 | 1.1 persistence, 1.2 default-deny, 1.3 tests, 2.1 per-lane concurrency | restart resilience + safe to expose + load stability |
| v0.9 | 1.4 metrics + `/metrics`, 2.2 subnet diversity, 2.3 adaptive concurrency, 3.1 SSE | real observability, real capacity, no more NAT tuning |
| v0.10 | 3.2 event visualizer, 3.3 accessibility, 3.4 sparklines, 3.6 diagnostics | the dashboard becomes the reason people share it |
| v1.0 | 2.7 provider profiles, 3.5 wizard, 3.7 snippets, README GIF, compose | a product, not a script |

## 6. THE ONE-LINE SUMMARY

The engine is 80% of the way to exceptional; what is missing is that it forgets everything on restart, trusts anyone who can reach the port, cannot tell you its p95, and treats 30 lanes in 3 subnets as 30 lanes. Fix those four and the polish list turns a good tool into a project people star.
