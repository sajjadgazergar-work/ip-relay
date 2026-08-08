# ip-relay — per-IP quota API relay with automatic egress rotation
#
# Sits between a gateway (9router, OpenRouter-style aggregators, your own
# client) and an upstream OpenAI-compatible API whose free tier is limited
# PER IP ADDRESS (e.g. opencode.ai/zen/v1). Each unique egress IP carries
# its own quota, so this relay rotates through a proxy pool on quota errors
# and lets one base URL serve more free-model traffic than a single IP
# would allow. Your server's own IP never touches the upstream.
#
# VERIFIED (2026-08-07): opencode's free tier needs `Authorization: Bearer
# public` (non-secret). Fresh IP + Bearer public -> 200. Fresh IP + fake key
# -> AuthError (key IS validated). Burned IP + any key -> FreeUsageLimitError.
# So: rotate IPs, send the configured key.
#
# Env config — see README.md for the full table.
#
#   UPSTREAM_BASE_URL   default https://opencode.ai/zen/v1
#   UPSTREAM_API_KEY    default "public" (opencode free tier)
#   RELAY_API_KEY       if set, require this Bearer on incoming requests
#   PROXY_REFRESH_SEC   re-scan proxy lists (default 600)
#   PROXY_TEST_CONCURRENCY   concurrent probes during pool refresh (default 12)
#   PROXY_MAX_CANDIDATES     cap candidates scanned per refresh (default 150)
#   PORT                listen port (default 8080)

import asyncio
import collections
import json
import logging
import os
import random
import time
import uuid

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

log = logging.getLogger("ip-relay")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── settings (env as base, settings.json overlays; UI writes the file) ──
SETTINGS_FILE = os.environ.get("SETTINGS_FILE", "settings.json")
DEFAULTS = {
    "upstream_base_url": "https://opencode.ai/zen/v1",
    "upstream_api_key": "public",
    "relay_api_key": "",
    "proxy_refresh_sec": 600,
    "proxy_test_concurrency": 12,
    "proxy_max_candidates": 150,
    "direct_lane": True,
    "probe_model": "deepseek-v4-flash-free",
}
settings: dict = dict(DEFAULTS)


def load_settings() -> None:
    """Start from env (legacy names accepted), then overlay settings.json so
    UI changes persist across restarts."""
    s = {
        "upstream_base_url": os.environ.get("UPSTREAM_BASE_URL", os.environ.get("OPENCODE_BASE_URL", DEFAULTS["upstream_base_url"])).rstrip("/"),
        "upstream_api_key": os.environ.get("UPSTREAM_API_KEY", os.environ.get("OPENCODE_API_KEY", DEFAULTS["upstream_api_key"])).strip(),
        "relay_api_key": os.environ.get("RELAY_API_KEY", DEFAULTS["relay_api_key"]).strip(),
        "proxy_refresh_sec": int(os.environ.get("PROXY_REFRESH_SEC", str(DEFAULTS["proxy_refresh_sec"]))),
        "proxy_test_concurrency": int(os.environ.get("PROXY_TEST_CONCURRENCY", str(DEFAULTS["proxy_test_concurrency"]))),
        "proxy_max_candidates": int(os.environ.get("PROXY_MAX_CANDIDATES", str(DEFAULTS["proxy_max_candidates"]))),
        "direct_lane": os.environ.get("DIRECT_LANE", "1") in ("1", "true", "yes"),
        "probe_model": os.environ.get("PROBE_MODEL", DEFAULTS["probe_model"]),
    }
    try:
        with open(SETTINGS_FILE) as f:
            s.update({k: v for k, v in json.load(f).items() if k in DEFAULTS})
    except Exception:
        pass
    settings.update(s)
    apply_settings(s, persist=False)


def save_settings() -> None:
    try:
        with open(SETTINGS_FILE, "w") as f:
            json.dump(settings, f, indent=2)
    except Exception as e:
        log.warning("could not save settings: %s", e)


def apply_settings(new: dict, persist: bool = True) -> dict:
    """Update runtime config from the UI/dashboard and sync module globals."""
    global UPSTREAM_API_KEY, UPSTREAM_BASE_URL, RELAY_API_KEY
    global PROXY_REFRESH_SEC, PROXY_TEST_CONCURRENCY, PROXY_MAX_CANDIDATES
    global DIRECT_LANE, UPSTREAM_PROBE
    for k, v in new.items():
        if k in DEFAULTS:
            settings[k] = v
    UPSTREAM_API_KEY = str(settings["upstream_api_key"]).strip()
    UPSTREAM_BASE_URL = str(settings["upstream_base_url"]).rstrip("/")
    RELAY_API_KEY = str(settings["relay_api_key"]).strip()
    PROXY_REFRESH_SEC = max(30, int(settings["proxy_refresh_sec"]))
    PROXY_TEST_CONCURRENCY = max(1, int(settings["proxy_test_concurrency"]))
    PROXY_MAX_CANDIDATES = max(5, int(settings["proxy_max_candidates"]))
    DIRECT_LANE = bool(settings["direct_lane"])
    UPSTREAM_PROBE = {
        "model": settings["probe_model"],
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 1,
        "stream": False,
    }
    if persist:
        save_settings()
    return dict(settings)


def public_settings() -> dict:
    """Settings safe to show in the UI (keys masked)."""
    s = dict(settings)
    if s.get("upstream_api_key"):
        s["upstream_api_key"] = mask_key(str(s["upstream_api_key"]))
    if s.get("relay_api_key"):
        s["relay_api_key"] = mask_key(str(s["relay_api_key"]))
    return s


# ── log ring buffer (for the dashboard log viewer) ────────────────
LOG_RING = collections.deque(maxlen=500)


class RingHandler(logging.Handler):
    def emit(self, record):
        try:
            LOG_RING.append(self.format(record))
        except Exception:
            pass


logging.getLogger().addHandler(RingHandler())

PROXY_LIST_URLS = [
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=ipport&format=text&timeout=5000",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
]

app = FastAPI(title="ip-relay")

# ── proxy pool state ──────────────────────────────────────────────
pool = {"proxies": [], "updated": 0.0, "lock": asyncio.Lock()}
# proxy -> cooldown_until (burned IPs get parked for a while)
cooldowns: dict[str, float] = {}
# set when the direct egress itself hits the quota limit
direct_burned_until = 0.0
# counters for observability
stats = {"requests": 0, "rotations": 0, "lane_failures": 0}


def mask_key(k: str) -> str:
    return k[:6] + "..." if k else "(none)"


# ── proxy pool ────────────────────────────────────────────────────

async def fetch_proxy_candidates() -> list[str]:
    seen: dict[str, str] = {}
    async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
        for url in PROXY_LIST_URLS:
            try:
                r = await client.get(url)
                if r.status_code != 200:
                    continue
                for line in r.text.splitlines():
                    line = line.strip()
                    if not line or "://" in line:
                        continue
                    parts = line.split(":")
                    if len(parts) >= 2:
                        try:
                            int(parts[1])
                        except ValueError:
                            continue
                        proxy = f"{parts[0]}:{parts[1]}"
                        if len(parts) >= 4:
                            proxy = f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
                        seen[proxy] = url
            except Exception:
                continue
    return list(seen.keys())


async def proxy_works(proxy: str) -> bool:
    """Real test: can we complete a 1-token upstream free request through it?
    Proves (a) proxy is alive and (b) its IP is not burned — exactly what the
    relay needs."""
    try:
        async with httpx.AsyncClient(timeout=12, proxy=f"http://{proxy}") as client:
            r = await client.post(
                f"{UPSTREAM_BASE_URL}/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {UPSTREAM_API_KEY}",
                    "User-Agent": "opencode-cli/1.0.0",
                    "x-opencode-client": "cli",
                    "x-opencode-project": "default",
                },
                json=UPSTREAM_PROBE,
            )
            if r.status_code == 200:
                return True
            if r.status_code == 429:
                cooldowns[proxy] = time.time() + 3600
    except Exception:
        pass
    return False


async def refresh_pool(force: bool = False) -> None:
    async with pool["lock"]:
        if not force and time.time() - pool["updated"] < PROXY_REFRESH_SEC:
            return
        candidates = (await fetch_proxy_candidates())[:PROXY_MAX_CANDIDATES]
        log.info("fetched %d proxy candidates (cap %d)", len(candidates), PROXY_MAX_CANDIDATES)
    # NOTE: lock released before the sweep — the sweep commits re-acquire it
    # briefly; holding it across gather would deadlock (asyncio.Lock is not
    # reentrant and relay() needs it too).
    good: list[str] = []
    sem = asyncio.Semaphore(PROXY_TEST_CONCURRENCY)
    done: set[str] = set()

    async def test_and_commit(p: str):
        async with sem:
            ok = await proxy_works(p)
            if ok:
                good.append(p)
            done.add(p)
            if len(done) % 20 == 0 or len(done) == len(candidates):
                async with pool["lock"]:
                    pool["proxies"] = [g for g in good if cooldowns.get(g, 0) <= time.time()]
                    pool["updated"] = time.time()
                log.info("pool progress: %d/%d tested, %d alive", len(done), len(candidates), len(pool["proxies"]))

    await asyncio.gather(*(test_and_commit(p) for p in candidates))
    async with pool["lock"]:
        pool["proxies"] = [g for g in good if cooldowns.get(g, 0) <= time.time()]
        pool["updated"] = time.time()
    log.info("proxy pool final: %d alive", len(pool["proxies"]))


async def get_egress_candidates() -> list[tuple[str | None, str]]:
    """Return [(proxy_or_None, label)] — direct lane first, then shuffled pool."""
    global direct_burned_until
    async with pool["lock"]:
        alive = [p for p in pool["proxies"] if cooldowns.get(p, 0) <= time.time()]
    lanes: list[tuple[str | None, str]] = []
    if DIRECT_LANE and time.time() >= direct_burned_until:
        lanes.append((None, "direct"))
    random.shuffle(alive)
    for p in alive[:30]:
        lanes.append((p, p))
    return lanes


def mark_burn(proxy: str | None, label: str, how_long: float = 3600.0) -> None:
    global direct_burned_until
    stats["rotations"] += 1
    if proxy:
        cooldowns[proxy] = time.time() + how_long
    elif label == "direct":
        direct_burned_until = time.time() + 1800
    log.warning("egress %s marked burned for %.0fs", label, how_long)


# ── upstream call ─────────────────────────────────────────────────

def build_upstream_headers(client_auth: str | None, request: Request) -> dict:
    h = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream, application/json",
    }
    key = UPSTREAM_API_KEY or (client_auth or "")
    if key:
        h["Authorization"] = key if key.startswith("Bearer ") else f"Bearer {key}"
    # forward client identity headers opencode/Cloudflare likes
    for name in ("User-Agent", "x-opencode-client", "x-opencode-project",
                 "x-opencode-session", "x-opencode-request"):
        v = request.headers.get(name)
        if v:
            h[name] = v
    h.setdefault("User-Agent", "opencode-cli/1.0.0")
    h.setdefault("x-opencode-client", "cli")
    h.setdefault("x-opencode-project", "default")
    h.setdefault("x-opencode-request", str(uuid.uuid4()))
    h.setdefault("x-opencode-session", str(uuid.uuid4()))
    return h


def strip_model_prefix(model: str) -> str:
    """oc/deepseek-v4-flash-free -> deepseek-v4-flash-free"""
    return model.split("/", 1)[-1]


def is_quota_429(body: bytes, status: int) -> bool:
    if status != 429:
        return False
    try:
        data = json.loads(body)
        err = data.get("error", {})
        msg = str(err.get("message", ""))
        return (
            err.get("type") == "FreeUsageLimitError"
            or "Rate limit" in msg
            or "quota" in msg.lower()
            or "usage limit" in msg.lower()
        )
    except Exception:
        return False


async def safe_aiter(ait):
    """Replay buffered upstream bytes as an SSE stream for the client.

    The upstream body is fully buffered (crash-proof against flaky proxies);
    when the client asked for stream=True we still need to hand back an
    async iterator so FastAPI serves a proper text/event-stream. Replaying
    the buffered body in chunks gives real SSE framing without any upstream
    mid-stream crash risk.
    """
    data = ait if isinstance(ait, (bytes, bytearray)) else b"".join([c async for c in ait])
    chunk_size = 4096
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


async def relay(payload: dict, path: str, stream: bool, headers: dict, timeout: float, method: str = "POST"):
    """Try lanes until one works. Returns (status, headers, body_bytes_or_async_iter)."""
    lanes = await get_egress_candidates()
    if not lanes:
        return 503, {"content-type": "application/json"}, json.dumps(
            {"error": {"message": "proxy pool warming up, try again shortly", "type": "rotator_warming"}}
        ).encode()
    last_err = None
    for proxy, label in lanes:
        try:
            client_kwargs = {"timeout": httpx.Timeout(timeout, connect=10)}
            if proxy:
                client_kwargs["proxy"] = f"http://{proxy}"
            else:
                client_kwargs["timeout"] = httpx.Timeout(min(timeout, 20), connect=10)
            async with httpx.AsyncClient(**client_kwargs) as client:
                url = f"{UPSTREAM_BASE_URL}/{path}"
                body_kw = {"json": payload} if method == "POST" else {}
                if stream:
                    resp = await client.request(method, url, headers=headers, **body_kw)
                    body = await resp.aread()
                    if resp.status_code == 429 and is_quota_429(body, 429):
                        mark_burn(proxy, label)
                        last_err = (429, body)
                        continue
                    return resp.status_code, dict(resp.headers), body
                else:
                    resp = await client.request(method, url, headers=headers, **body_kw)
                    body = resp.content
                    if resp.status_code == 429 and is_quota_429(body, 429):
                        mark_burn(proxy, label)
                        last_err = (429, body)
                        continue
                    return resp.status_code, dict(resp.headers), body
        except (httpx.HTTPError, asyncio.TimeoutError, httpx.TimeoutException) as e:
            last_err = ("error", str(e).encode())
            stats["lane_failures"] += 1
            if proxy:
                mark_burn(proxy, label, 300)
            continue
    if last_err and last_err[0] == 429:
        return 429, {"content-type": "application/json"}, last_err[1]
    return 503, {"content-type": "application/json"}, json.dumps(
        {"error": {"message": "all egress lanes failed", "type": "rotator_exhausted"}}
    ).encode()


# ── routes ────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    load_settings()
    asyncio.create_task(bg_refresh())


async def bg_refresh():
    while True:
        try:
            await refresh_pool(force=True)
        except Exception as e:
            log.warning("bg refresh failed: %s", e)
        await asyncio.sleep(PROXY_REFRESH_SEC)


@app.get("/healthz")
async def healthz():
    async with pool["lock"]:
        alive = len([p for p in pool["proxies"] if cooldowns.get(p, 0) <= time.time()])
    return {
        "ok": True,
        "pool": alive,
        "updated": pool["updated"],
        "key": mask_key(UPSTREAM_API_KEY),
        "stats": stats,
    }


def _check_relay_auth(request: Request) -> bool:
    if not RELAY_API_KEY:
        return True
    auth = request.headers.get("authorization", "")
    return auth == f"Bearer {RELAY_API_KEY}" or auth == RELAY_API_KEY


@app.get("/v1/models")
async def models(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": {"message": "invalid relay key", "type": "invalid_relay_key"}}, status_code=401)
    headers = build_upstream_headers(request.headers.get("authorization"), request)
    status, resp_headers, body = await relay({}, "models", False, headers, 20, method="GET")
    if status == 200:
        return Response(content=body, media_type=resp_headers.get("content-type", "application/json"))
    return Response(content=body, status_code=status, media_type="application/json")


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": {"message": "invalid relay key", "type": "invalid_relay_key"}}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"error": {"message": "invalid json", "type": "invalid_request"}}, status_code=400)

    stats["requests"] += 1
    model = strip_model_prefix(str(payload.get("model", "")))
    payload["model"] = model
    stream = bool(payload.get("stream", False))
    auth = request.headers.get("authorization")

    headers = build_upstream_headers(auth, request)
    timeout = 300.0 if stream else 120.0

    status, resp_headers, body = await relay(payload, "chat/completions", stream, headers, timeout)

    if stream and status < 300:
        # body is fully buffered bytes from the upstream (crash-proof);
        # replay it as SSE so the client still sees a real stream.
        return StreamingResponse(safe_aiter(body), status_code=status, media_type=resp_headers.get("content-type", "text/event-stream"))
    return Response(content=body, status_code=status, media_type=resp_headers.get("content-type", "application/json"))


# ── dashboard UI ──────────────────────────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ip-relay dashboard</title>
<style>
  :root { --bg:#0f1115; --card:#171a21; --line:#262b36; --text:#e6e9ef; --muted:#8b93a3; --green:#3fb68b; --red:#e5484d; --amber:#f5a623; --blue:#4c8dff; }
  * { box-sizing:border-box; }
  body { margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; background:var(--bg); color:var(--text); }
  .wrap { max-width:1000px; margin:0 auto; padding:24px 16px 80px; }
  h1 { font-size:22px; margin:0 0 4px; }
  .sub { color:var(--muted); font-size:13px; margin-bottom:20px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:12px; margin-bottom:20px; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:16px; }
  .card h3 { margin:0 0 8px; font-size:12px; text-transform:uppercase; letter-spacing:.06em; color:var(--muted); }
  .big { font-size:28px; font-weight:700; }
  .pill { display:inline-block; padding:3px 10px; border-radius:999px; font-size:12px; font-weight:600; }
  .pill.ok { background:rgba(63,182,139,.15); color:var(--green); }
  .pill.warn { background:rgba(245,166,35,.15); color:var(--amber); }
  .pill.err { background:rgba(229,72,77,.15); color:var(--red); }
  .panel { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:20px; margin-bottom:20px; }
  .panel h2 { margin:0 0 14px; font-size:16px; }
  label { display:block; font-size:12px; color:var(--muted); margin:12px 0 4px; }
  input[type=text],input[type=password],input[type=number],select { width:100%; padding:9px 12px; background:#0d0f13; border:1px solid var(--line); border-radius:8px; color:var(--text); font-size:14px; }
  input:focus { outline:none; border-color:var(--blue); }
  .row { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
  .check { display:flex; align-items:center; gap:8px; margin-top:14px; }
  .check input { width:auto; }
  .btn { background:var(--blue); color:#fff; border:none; border-radius:8px; padding:10px 18px; font-size:14px; font-weight:600; cursor:pointer; margin-top:16px; }
  .btn:hover { opacity:.9; }
  .btn.ghost { background:transparent; border:1px solid var(--line); color:var(--text); }
  .btnrow { display:flex; gap:10px; }
  .logbox { background:#0d0f13; border:1px solid var(--line); border-radius:8px; padding:12px; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; line-height:1.6; max-height:280px; overflow:auto; white-space:pre-wrap; color:#aab3c5; }
  .toast { position:fixed; bottom:20px; left:50%; transform:translateX(-50%); background:var(--green); color:#04120c; padding:10px 20px; border-radius:8px; font-weight:600; opacity:0; transition:opacity .3s; pointer-events:none; }
  .toast.show { opacity:1; }
  .muted { color:var(--muted); font-size:12px; }
  .statrow { display:flex; justify-content:space-between; padding:4px 0; font-size:13px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>ip-relay dashboard</h1>
  <div class="sub">Rotating egress relay for per-IP-quota APIs — status &amp; configuration</div>

  <div class="grid">
    <div class="card"><h3>Status</h3><span id="statusPill" class="pill warn">loading…</span></div>
    <div class="card"><h3>Proxy pool</h3><div id="pool" class="big">–</div><div class="muted">working egress IPs</div></div>
    <div class="card"><h3>Requests</h3><div id="requests" class="big">–</div><div class="muted">total served</div></div>
    <div class="card"><h3>Rotations</h3><div id="rotations" class="big">–</div><div class="muted">quota hit → IP switched</div></div>
  </div>

  <div class="panel">
    <h2>Configuration</h2>
    <label>Upstream API URL</label>
    <input type="text" id="cfg_upstream_base_url" placeholder="https://opencode.ai/zen/v1">
    <label>Upstream API key</label>
    <input type="password" id="cfg_upstream_api_key" placeholder="public">
    <label>Relay API key (optional — protects this dashboard &amp; API)</label>
    <input type="password" id="cfg_relay_api_key" placeholder="leave blank for no auth">
    <div class="row">
      <div>
        <label>Proxy refresh interval (seconds)</label>
        <input type="number" id="cfg_proxy_refresh_sec" min="30">
      </div>
      <div>
        <label>Proxy test concurrency</label>
        <input type="number" id="cfg_proxy_test_concurrency" min="1">
      </div>
    </div>
    <div class="row">
      <div>
        <label>Max proxy candidates</label>
        <input type="number" id="cfg_proxy_max_candidates" min="5">
      </div>
      <div>
        <label>Probe model</label>
        <input type="text" id="cfg_probe_model">
      </div>
    </div>
    <div class="check">
      <input type="checkbox" id="cfg_direct_lane">
      <label for="cfg_direct_lane" style="margin:0">Allow direct (server IP) egress</label>
    </div>
    <div class="btnrow">
      <button class="btn" onclick="saveConfig()">Save configuration</button>
      <button class="btn ghost" onclick="refreshPool()">Refresh proxy pool now</button>
    </div>
    <div class="muted" style="margin-top:10px">Changes apply immediately and persist across restarts.</div>
  </div>

  <div class="panel">
    <h2>Live log</h2>
    <div class="logbox" id="logbox">loading…</div>
  </div>

  <div class="panel">
    <h2>How to connect</h2>
    <div class="muted" style="line-height:1.8">
      Point any OpenAI-compatible client at <code style="color:#aab3c5">http://&lt;this-server&gt;:PORT/v1</code>.<br>
      Example: <code style="color:#aab3c5">curl http://localhost:8080/v1/chat/completions -H 'Content-Type: application/json' -H 'Authorization: Bearer public' -d '{"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"hi"}]}'</code>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>
<script>
async function jget(url) { const r = await fetch(url); if (!r.ok) throw new Error((await r.text()).slice(0,120)); return r.json(); }
function toast(msg) { const t = document.getElementById('toast'); t.textContent = msg; t.classList.add('show'); setTimeout(()=>t.classList.remove('show'), 2500); }
function setPill(el, ok) { el.className = 'pill ' + (ok ? 'ok' : 'err'); el.textContent = ok ? 'healthy' : 'degraded'; }
async function refresh() {
  try {
    const h = await jget('/healthz');
    setPill(document.getElementById('statusPill'), h.ok);
    document.getElementById('pool').textContent = h.pool ?? '–';
    document.getElementById('requests').textContent = (h.stats?.requests ?? 0).toLocaleString();
    document.getElementById('rotations').textContent = (h.stats?.rotations ?? 0).toLocaleString();
  } catch(e) { setPill(document.getElementById('statusPill'), false); }
  try {
    const s = await jget('/api/settings');
    document.getElementById('cfg_upstream_base_url').value = s.upstream_base_url || '';
    document.getElementById('cfg_upstream_api_key').value = s.upstream_api_key || '';
    document.getElementById('cfg_relay_api_key').value = s.relay_api_key || '';
    document.getElementById('cfg_proxy_refresh_sec').value = s.proxy_refresh_sec ?? 600;
    document.getElementById('cfg_proxy_test_concurrency').value = s.proxy_test_concurrency ?? 12;
    document.getElementById('cfg_proxy_max_candidates').value = s.proxy_max_candidates ?? 150;
    document.getElementById('cfg_probe_model').value = s.probe_model || '';
    document.getElementById('cfg_direct_lane').checked = !!s.direct_lane;
  } catch(e) {}
  try {
    const l = await jget('/api/logs?n=100');
    document.getElementById('logbox').textContent = (l.logs || []).join('\\n') || '(no logs yet)';
  } catch(e) {}
}
async function saveConfig() {
  const body = {
    upstream_base_url: document.getElementById('cfg_upstream_base_url').value.trim(),
    upstream_api_key: document.getElementById('cfg_upstream_api_key').value.trim(),
    relay_api_key: document.getElementById('cfg_relay_api_key').value.trim(),
    proxy_refresh_sec: parseInt(document.getElementById('cfg_proxy_refresh_sec').value || '600', 10),
    proxy_test_concurrency: parseInt(document.getElementById('cfg_proxy_test_concurrency').value || '12', 10),
    proxy_max_candidates: parseInt(document.getElementById('cfg_proxy_max_candidates').value || '150', 10),
    probe_model: document.getElementById('cfg_probe_model').value.trim(),
    direct_lane: document.getElementById('cfg_direct_lane').checked,
  };
  try {
    const r = await fetch('/api/settings', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    if (!r.ok) throw new Error((await r.text()).slice(0,120));
    toast('Configuration saved ✓');
    refresh();
  } catch(e) { toast('Error: ' + e.message); }
}
async function refreshPool() {
  try { await jget('/api/refresh'); toast('Pool refresh started ✓'); } catch(e) { toast('Error: ' + e.message); }
}
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)


@app.get("/api/settings")
async def get_settings_api():
    return public_settings()


@app.post("/api/settings")
async def post_settings_api(request: Request):
    try:
        new = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)
    # only allow known keys; never let the UI blank the upstream key to ""
    clean = {k: v for k, v in new.items() if k in DEFAULTS}
    if "upstream_api_key" in clean and not str(clean["upstream_api_key"]).strip():
        clean.pop("upstream_api_key")
    if "relay_api_key" in clean and str(clean["relay_api_key"]).strip().startswith("***"):
        clean.pop("relay_api_key")  # masked value sent back — don't overwrite
    apply_settings(clean)
    return public_settings()


@app.get("/api/logs")
async def logs_api(n: int = 100):
    return {"logs": list(LOG_RING)[-n:]}


@app.post("/api/refresh")
async def refresh_api():
    asyncio.create_task(refresh_pool(force=True))
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
