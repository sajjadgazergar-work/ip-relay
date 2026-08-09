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
    "proxy_test_concurrency": 25,
    "proxy_max_candidates": 2000,
    "proxy_min_pool": 10,
    "proxy_pool_target": 25,
    "relay_proxy_timeout": 25,
    "probe_timeout": 20,
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
        "proxy_min_pool": int(os.environ.get("PROXY_MIN_POOL", str(DEFAULTS["proxy_min_pool"]))),
        "proxy_pool_target": int(os.environ.get("PROXY_POOL_TARGET", str(DEFAULTS["proxy_pool_target"]))),
        "relay_proxy_timeout": int(os.environ.get("RELAY_PROXY_TIMEOUT", str(DEFAULTS["relay_proxy_timeout"]))),
        "probe_timeout": int(os.environ.get("PROBE_TIMEOUT", str(DEFAULTS["probe_timeout"]))),
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
    global PROXY_MIN_POOL, PROXY_POOL_TARGET, RELAY_PROXY_TIMEOUT, PROBE_TIMEOUT
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
    PROXY_MIN_POOL = int(settings.get("proxy_min_pool", 10))
    PROXY_POOL_TARGET = max(PROXY_MIN_POOL, int(settings.get("proxy_pool_target", 25)))
    RELAY_PROXY_TIMEOUT = max(5, int(settings.get("relay_proxy_timeout", 25)))
    PROBE_TIMEOUT = max(5, int(settings.get("probe_timeout", 20)))
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
    # plain-text ip:port lists (HTTP/HTTPS proxies)
    "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=ipport&format=text&timeout=5000",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://www.proxy-list.download/api/v1/get?type=http",
    "https://www.proxy-list.download/api/v1/get?type=https",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/https/data.txt",
    # uptime-ranked JSON API — sorted by last-checked + uptime, best candidates first
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps",
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=upTime&sort_type=desc&protocols=http%2Chttps",
]

# protocol://ip:port lists (proxyscrape v3); http/https entries are used,
# socks entries need httpx[socks] + ALLOW_SOCKS=1
PROTOCOL_LIST_URLS = [
    "https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&proxy_format=protocolipport&format=text",
]

app = FastAPI(title="ip-relay")

# ── proxy pool state ──────────────────────────────────────────────
pool = {"proxies": [], "updated": 0.0, "lock": asyncio.Lock()}
# candidate reservoir — the pool is fed from here by a continuous churner,
# because free-proxy lists are thousands of entries long and most are dead
# or already rate-limited. A one-shot sweep of a capped subset was the old
# design; it starved whenever the first N candidates happened to be bad.
candidates: dict[str, float] = {}          # proxy -> first-seen ts (untested)
tried: dict[str, float] = {}               # proxy -> last-try ts (failed probes)
# proxy -> cooldown_until (burned IPs get parked for a while)
cooldowns: dict[str, float] = {}
# set when the direct egress itself hits the quota limit
direct_burned_until = 0.0
# counters for observability
stats = {"requests": 0, "rotations": 0, "lane_failures": 0,
         "candidates_tested": 0, "probes_ok": 0, "probes_burned": 0}
sweep = {"state": "idle", "tested": 0, "found": 0, "last_refresh": 0.0, "sources_ok": 0}


def mask_key(k: str) -> str:
    return k[:6] + "..." if k else "(none)"


# ── proxy pool ────────────────────────────────────────────────────

def _parse_text_list(text: str) -> list[str]:
    out = []
    for line in text.splitlines():
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
            out.append(proxy)
    return out


def _parse_geonode(text: str) -> list[str]:
    try:
        data = json.loads(text)
        out = []
        for it in data.get("data", []):
            ip, port = it.get("ip"), it.get("port")
            if ip and port:
                out.append(f"{ip}:{port}")
        return out
    except Exception:
        return []


async def fetch_proxy_candidates() -> list[str]:
    seen: dict[str, str] = {}
    ok_sources = 0
    async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
        for url in PROXY_LIST_URLS:
            try:
                headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126 Safari/537.36"} if "geonode" in url else {}
                r = await client.get(url, headers=headers)
                if r.status_code != 200:
                    continue
                found = _parse_geonode(r.text) if "geonode" in url else _parse_text_list(r.text)
                if found:
                    ok_sources += 1
                for proxy in found:
                    seen[proxy] = url
            except Exception:
                continue
        if os.environ.get("ALLOW_SOCKS", "") in ("1", "true", "yes"):
            for url in PROTOCOL_LIST_URLS:
                try:
                    r = await client.get(url)
                    if r.status_code != 200:
                        continue
                    for line in r.text.splitlines():
                        line = line.strip()
                        if line.startswith(("http://", "https://")):
                            seen[line.split("://", 1)[1]] = url
                except Exception:
                    continue
    sweep["sources_ok"] = ok_sources
    return list(seen.keys())


async def proxy_connectable(proxy: str) -> bool:
    """Stage 1: cheap TCP+HTTP check — does this proxy relay anything at all?
    Uses a tiny http:// endpoint so it costs no upstream quota and is fast."""
    try:
        async with httpx.AsyncClient(timeout=6, proxy=f"http://{proxy}") as client:
            r = await client.get("http://ip-api.com/json/")
            return r.status_code == 200
    except Exception:
        return False


async def proxy_works(proxy: str) -> bool:
    """Stage 2: real test — complete a 1-token upstream free request through it.
    Proves (a) the proxy relays HTTPS CONNECT and (b) its IP is not burned."""
    try:
        async with httpx.AsyncClient(timeout=PROBE_TIMEOUT, proxy=f"http://{proxy}") as client:
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


async def _alive_pool() -> list[str]:
    async with pool["lock"]:
        return [p for p in pool["proxies"] if cooldowns.get(p, 0) <= time.time()]


async def refresh_candidates(force: bool = False) -> None:
    """Refill the candidate reservoir from the public lists."""
    if not force and time.time() - sweep["last_refresh"] < PROXY_REFRESH_SEC:
        return
    fresh = await fetch_proxy_candidates()
    now = time.time()
    added = 0
    for p in fresh:
        if p in pool["proxies"] or p in candidates:
            continue
        # retest failed candidates after 10 min; burned ones stay parked via cooldowns
        if tried.get(p, 0) > now - 600:
            continue
        if cooldowns.get(p, 0) > now:
            continue
        candidates[p] = now
        added += 1
    sweep["last_refresh"] = now
    # cap the reservoir, dropping oldest first
    if len(candidates) > PROXY_MAX_CANDIDATES:
        overflow = len(candidates) - PROXY_MAX_CANDIDATES
        for p, _ in sorted(candidates.items(), key=lambda kv: kv[1])[:overflow]:
            candidates.pop(p, None)
    parked = len([1 for t in cooldowns.values() if t > now])
    log.info("candidates: +%d new, %d queued, %d parked (burned/cooldown)", added, len(candidates), parked)


async def churn_batch(batch_size: int = 60) -> None:
    """Test a small batch of candidates and commit good ones to the pool.
    Runs continuously in the background — the pool fills up in minutes
    instead of one giant slow sweep, and keeps topping up as lanes burn."""
    if not candidates:
        await refresh_candidates(force=False)
        if not candidates:
            return
    batch = list(candidates.keys())[:batch_size]
    for p in batch:
        candidates.pop(p, None)
    sweep["state"] = "testing"
    sweep["tested"] = 0
    sem = asyncio.Semaphore(PROXY_TEST_CONCURRENCY)

    async def stage1(p: str) -> bool:
        async with sem:
            return await proxy_connectable(p)

    results = await asyncio.gather(*(stage1(p) for p in batch))
    alive = [p for p, ok in zip(batch, results) if ok]
    for p, ok in zip(batch, results):
        if not ok:
            tried[p] = time.time()
    good: list[str] = []

    async def stage2(p: str):
        async with sem:
            ok = await proxy_works(p)
            stats["candidates_tested"] += 1
            sweep["tested"] += 1
            if ok:
                good.append(p)
                stats["probes_ok"] += 1
            elif cooldowns.get(p, 0) > time.time():
                stats["probes_burned"] += 1
            else:
                tried[p] = time.time()

    await asyncio.gather(*(stage2(p) for p in alive))
    if good:
        async with pool["lock"]:
            known = set(pool["proxies"])
            pool["proxies"].extend([g for g in good if g not in known])
            pool["updated"] = time.time()
        sweep["found"] += len(good)
    log.info("churn: tested %d (%d connectable), +%d good -> pool %d",
             len(batch), len(alive), len(good), len(pool["proxies"]))
    sweep["state"] = "idle"


async def refresh_pool(force: bool = False) -> None:
    """Manual/full refresh: refill candidates, then churn until the pool hits
    target or candidates run out. Backs the dashboard 'Refresh pool' button."""
    await refresh_candidates(force=force)
    rounds = 0
    while candidates and len(await _alive_pool()) < PROXY_POOL_TARGET and rounds < 8:
        await churn_batch(batch_size=max(60, PROXY_TEST_CONCURRENCY * 3))
        rounds += 1


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
            client_kwargs = {"timeout": httpx.Timeout(min(timeout, RELAY_PROXY_TIMEOUT), connect=10)}
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
    # warm the models cache immediately so /v1/models never 503s on first hit
    asyncio.create_task(get_models_cached())


async def bg_refresh():
    """Continuous pool maintenance: keep the pool topped up to target by
    churning small candidate batches. When the pool is healthy this idles;
    when it's thin it works constantly. This replaced the old one-shot
    sweep-every-10-min design that left the pool at 0 for long stretches."""
    while True:
        try:
            alive = await _alive_pool()
            if len(alive) < PROXY_POOL_TARGET:
                if not candidates:
                    # reservoir dry — pull fresh lists right away; still rate-limited
                    # to one fetch per 120s so we never hammer the public lists
                    if time.time() - sweep["last_refresh"] > 120:
                        await refresh_candidates(force=True)
                    else:
                        await asyncio.sleep(10)
                        continue
                await churn_batch(batch_size=max(300, PROXY_TEST_CONCURRENCY * 12))
                await asyncio.sleep(2 if len(alive) < PROXY_MIN_POOL else 6)
            else:
                # pool healthy — slow cadence; refresh lists on the normal interval
                await refresh_candidates(force=False)
                await asyncio.sleep(60)
        except Exception as e:
            log.warning("bg refresh failed: %s", e)
            await asyncio.sleep(20)


@app.get("/healthz")
async def healthz():
    async with pool["lock"]:
        alive = len([p for p in pool["proxies"] if cooldowns.get(p, 0) <= time.time()])
    models: list[str] = []
    try:
        if _models_cache["body"]:
            data = json.loads(_models_cache["body"])
            models = [m.get("id", "") for m in data.get("data", [])]
    except Exception:
        pass
    return {
        "ok": True,
        "pool": alive,
        "pool_target": PROXY_POOL_TARGET,
        "candidates": len(candidates),
        "parked": len([1 for t in cooldowns.values() if t > time.time()]),
        "updated": pool["updated"],
        "key": mask_key(UPSTREAM_API_KEY),
        "stats": stats,
        "sweep": dict(sweep),
        "models": models,
        "models_cached": bool(_models_cache["body"]),
    }


def _check_relay_auth(request: Request) -> bool:
    if not RELAY_API_KEY:
        return True
    auth = request.headers.get("authorization", "")
    return auth == f"Bearer {RELAY_API_KEY}" or auth == RELAY_API_KEY


def _check_dashboard_auth(request: Request) -> bool:
    """Dashboard data endpoints require the relay key via cookie (set at /login)
    or Authorization header. If no relay key is set, dashboard is open."""
    if not RELAY_API_KEY:
        return True
    auth = request.headers.get("authorization", "")
    if auth == f"Bearer {RELAY_API_KEY}" or auth == RELAY_API_KEY:
        return True
    try:
        import hashlib
        cookie = request.cookies.get("ip_relay_auth", "")
        expected = hashlib.sha256(f"ip-relay:{RELAY_API_KEY}".encode()).hexdigest()
        return cookie == expected
    except Exception:
        return False


# static model list cache — models don't need the proxy pool (metadata only)
# fetched directly from upstream and cached for MODELS_CACHE_SEC
MODELS_CACHE_SEC = 3600
_models_cache: dict = {"updated": 0.0, "status": 503, "body": b"", "content_type": "application/json"}


async def get_models_cached() -> tuple[int, str, bytes]:
    """Return (status, content_type, body) for /v1/models — direct upstream
    fetch (no proxy rotation needed), cached for an hour."""
    if time.time() - _models_cache["updated"] < MODELS_CACHE_SEC and _models_cache["body"]:
        return _models_cache["status"], _models_cache["content_type"], _models_cache["body"]
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20, connect=10)) as client:
            resp = await client.get(f"{UPSTREAM_BASE_URL}/models", headers={"Authorization": f"Bearer {UPSTREAM_API_KEY}"})
            body = resp.content
            ct = resp.headers.get("content-type", "application/json")
            _models_cache.update({"updated": time.time(), "status": resp.status_code, "body": body, "content_type": ct})
            return resp.status_code, ct, body
    except Exception as e:
        log.warning("models fetch failed: %s", e)
        # serve stale cache if we have it
        if _models_cache["body"]:
            return _models_cache["status"], _models_cache["content_type"], _models_cache["body"]
        return 503, "application/json", json.dumps({"error": {"message": "models fetch failed", "type": "models_unavailable"}}).encode()


@app.get("/v1/models")
async def models(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": {"message": "invalid relay key", "type": "invalid_relay_key"}}, status_code=401)
    status, ct, body = await get_models_cached()
    return Response(content=body, media_type=ct, status_code=status)


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
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'><circle cx='8' cy='8' r='6' fill='none' stroke='%2322d3ee' stroke-width='2'/></svg>">
<title>ip-relay dashboard</title>
<style>
  :root {
    --bg0:#06080f; --bg1:#0a0f1c;
    --surface:rgba(15,21,36,.78);
    --surface2:#111828;
    --line:rgba(148,163,184,.13);
    --text:#e9edf5; --muted:#8794ab; --faint:#5b6780;
    --cyan:#22d3ee; --violet:#8b5cf6; --magenta:#e879f9;
    --green:#34d399; --amber:#fbbf24; --red:#f87171;
    --grad:linear-gradient(135deg,var(--cyan),var(--violet) 55%,var(--magenta));
    --mono:ui-monospace,'SF Mono','Cascadia Code','JetBrains Mono',Menlo,Consolas,monospace;
  }
  * { box-sizing:border-box; }
  html { scrollbar-color: #1c2436 transparent; }
  body {
    margin:0; min-height:100vh; color:var(--text);
    font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;
    background:var(--bg0); overflow-x:hidden;
    -webkit-font-smoothing:antialiased;
  }
  ::selection { background:rgba(139,92,246,.35); }

  /* ── aurora background ── */
  .aurora { position:fixed; inset:0; z-index:-2; overflow:hidden; background:
    radial-gradient(1200px 800px at 80% -10%, rgba(139,92,246,.14), transparent 60%),
    radial-gradient(1000px 700px at -10% 110%, rgba(34,211,238,.10), transparent 60%),
    var(--bg0); }
  .blob { position:absolute; border-radius:50%; filter:blur(90px); opacity:.5; will-change:transform; }
  .blob.a { width:560px; height:560px; top:-180px; left:-140px;
    background:radial-gradient(circle at 35% 35%, #0ea5e9, transparent 62%);
    animation:driftA 30s ease-in-out infinite alternate; }
  .blob.b { width:640px; height:640px; top:20%; right:-260px;
    background:radial-gradient(circle at 60% 40%, #7c3aed, transparent 62%);
    animation:driftB 38s ease-in-out infinite alternate; }
  .blob.c { width:420px; height:420px; bottom:-160px; left:28%;
    background:radial-gradient(circle at 50% 50%, #c026d3, transparent 62%);
    animation:driftC 46s ease-in-out infinite alternate; }
  @keyframes driftA { to { transform:translate(240px,160px) scale(1.18); } }
  @keyframes driftB { to { transform:translate(-180px,-120px) scale(1.1); } }
  @keyframes driftC { to { transform:translate(120px,-140px) scale(1.22); } }
  .gridlines { position:fixed; inset:0; z-index:-1; pointer-events:none;
    background-image:linear-gradient(rgba(148,163,184,.045) 1px,transparent 1px),
                     linear-gradient(90deg,rgba(148,163,184,.045) 1px,transparent 1px);
    background-size:46px 46px;
    -webkit-mask-image:radial-gradient(ellipse 90% 70% at 50% 0%, #000 35%, transparent 78%);
            mask-image:radial-gradient(ellipse 90% 70% at 50% 0%, #000 35%, transparent 78%); }

  .wrap { max-width:1140px; margin:0 auto; padding:26px 20px 70px; }

  /* ── header ── */
  header { display:flex; align-items:center; justify-content:space-between; gap:16px; margin-bottom:22px; }
  .brand { display:flex; align-items:center; gap:14px; }
  .logo-ring { position:relative; width:40px; height:40px; border-radius:50%; flex:none;
    background:conic-gradient(from 0deg, transparent 0 296deg, var(--cyan) 296deg 322deg, var(--violet) 322deg 348deg, var(--magenta) 348deg 360deg);
    -webkit-mask:radial-gradient(farthest-side, transparent calc(100% - 3.5px), #000 calc(100% - 2.5px));
            mask:radial-gradient(farthest-side, transparent calc(100% - 3.5px), #000 calc(100% - 2.5px));
    animation:spin 7s linear infinite; }
  .logo-ring::after { content:''; position:absolute; inset:50% auto auto 50%; width:8px; height:8px; border-radius:50%;
    background:var(--cyan); transform:translate(-50%,-50%); box-shadow:0 0 12px 2px rgba(34,211,238,.8); }
  @keyframes spin { to { transform:rotate(360deg); } }
  .brand h1 { margin:0; font-size:21px; letter-spacing:-.02em; }
  .brand .tag { font-size:10.5px; text-transform:uppercase; letter-spacing:.18em; color:var(--muted); }
  .hdr-right { display:flex; align-items:center; gap:14px; }
  .baseurl-chip { display:inline-flex; align-items:center; gap:8px; padding:7px 13px; border-radius:10px;
    background:rgba(34,211,238,.07); border:1px solid rgba(34,211,238,.28); cursor:pointer;
    color:var(--text); transition:border-color .2s, background .2s; }
  .baseurl-chip:hover { border-color:var(--cyan); background:rgba(34,211,238,.14); }
  .baseurl-chip svg { width:13px; height:13px; color:var(--cyan); flex:none; }
  .baseurl-chip .bu-lbl { font-size:9px; font-weight:700; letter-spacing:.14em; color:var(--muted); }
  .baseurl-chip .bu-val { font-family:var(--mono); font-size:12px; color:var(--cyan); }
  @media (max-width:640px){ .baseurl-chip .bu-lbl{ display:none; } .baseurl-chip .bu-val{ max-width:46vw; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; } }
  .uptime { font-family:var(--mono); font-size:12px; color:var(--muted); }

  /* ── pills ── */
  .pill { display:inline-flex; align-items:center; gap:8px; padding:6px 14px; border-radius:999px;
    font-size:12px; font-weight:600; letter-spacing:.03em; border:1px solid transparent; }
  .pill::before { content:''; width:7px; height:7px; border-radius:50%; background:currentColor;
    box-shadow:0 0 8px 1px currentColor; animation:pulse 2.2s ease-in-out infinite; }
  @keyframes pulse { 50% { opacity:.45; transform:scale(.82); } }
  .pill.ok { color:var(--green); background:rgba(52,211,153,.1); border-color:rgba(52,211,153,.25); }
  .pill.warn { color:var(--amber); background:rgba(251,191,36,.1); border-color:rgba(251,191,36,.25); }
  .pill.err { color:var(--red); background:rgba(248,113,113,.1); border-color:rgba(248,113,113,.25); }

  /* ── stat cards ── */
  .stats { display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin-bottom:16px; }
  .card { background:var(--surface); border:1px solid var(--line); border-radius:16px; padding:18px 18px 16px;
    position:relative; overflow:hidden; backdrop-filter:blur(14px); -webkit-backdrop-filter:blur(14px);
    box-shadow:0 10px 30px -18px rgba(0,0,0,.8), inset 0 1px 0 rgba(255,255,255,.05);
    transition:transform .25s cubic-bezier(.2,.7,.3,1), border-color .25s, box-shadow .25s; }
  .card::before { content:''; position:absolute; inset:0 0 auto 0; height:1px;
    background:linear-gradient(90deg, transparent, rgba(148,163,184,.35), transparent); opacity:.5; }
  .card:hover { transform:translateY(-3px); border-color:rgba(34,211,238,.35);
    box-shadow:0 18px 40px -20px rgba(0,0,0,.9), 0 0 24px -10px rgba(34,211,238,.35), inset 0 1px 0 rgba(255,255,255,.07); }
  .stat { animation:rise .55s cubic-bezier(.2,.7,.3,1) both; animation-delay:calc(var(--i)*70ms); }
  @keyframes rise { from { opacity:0; transform:translateY(16px); } to { opacity:1; transform:none; } }
  .stat-top { display:flex; align-items:center; justify-content:space-between; margin-bottom:10px; }
  .lbl { font-size:10.5px; text-transform:uppercase; letter-spacing:.16em; color:var(--muted); font-weight:600; }
  .ic { width:30px; height:30px; border-radius:9px; display:grid; place-items:center;
    background:rgba(148,163,184,.08); border:1px solid var(--line); }
  .ic svg { width:16px; height:16px; }
  .ic.cyan { color:var(--cyan); } .ic.violet { color:var(--violet); } .ic.mag { color:var(--magenta); } .ic.grn { color:var(--green); }
  .big { font-family:var(--mono); font-size:30px; font-weight:700; letter-spacing:-.02em; font-variant-numeric:tabular-nums; line-height:1; }
  .sub { color:var(--muted); font-size:11.5px; margin-top:8px; }
  .spark-wrap { margin-top:10px; }

  /* ── panels ── */
  .panel { background:var(--surface); border:1px solid var(--line); border-radius:16px; padding:20px;
    backdrop-filter:blur(14px); -webkit-backdrop-filter:blur(14px);
    box-shadow:0 10px 30px -18px rgba(0,0,0,.8), inset 0 1px 0 rgba(255,255,255,.05); }
  .panel-h { display:flex; align-items:baseline; justify-content:space-between; gap:12px; margin-bottom:6px; }
  .panel-h h2 { margin:0; font-size:14px; font-weight:700; letter-spacing:.01em; }
  .panel-h .hint { font-size:11px; color:var(--muted); font-family:var(--mono); }

  /* mesh */
  .mesh-panel { margin-bottom:16px; }
  .mesh-wrap { position:relative; }
  .mesh-wrap svg { display:block; width:100%; height:auto; }
  .mesh-core { position:absolute; left:50%; top:50%; transform:translate(-50%,-50%);
    text-align:center; pointer-events:none; }
  .mesh-core .ring { position:absolute; inset:-26px -34px; border-radius:50%;
    border:1.5px dashed rgba(34,211,238,.45); animation:spin 16s linear infinite; }
  .mesh-core .count { font-family:var(--mono); font-size:34px; font-weight:700; line-height:1; }
  .mesh-core .cap { font-size:10px; text-transform:uppercase; letter-spacing:.16em; color:var(--muted); margin-top:4px; }
  .mesh-core.empty .count { color:var(--amber); }
  .mesh-core.empty .ring { border-color:rgba(251,191,36,.5); animation:pulse 1.6s ease-in-out infinite; }
  .mesh-note { text-align:center; font-size:11px; color:var(--faint); margin-top:2px; font-family:var(--mono); }

  /* main grid */
  .main-grid { display:grid; grid-template-columns:1.35fr 1fr; gap:16px; align-items:start; }
  .main-grid > .panel { min-width:0; }

  /* log */
  .logbox { background:rgba(6,8,15,.72); border:1px solid var(--line); border-radius:12px;
    padding:12px 14px; font-family:var(--mono); font-size:11.5px; line-height:1.75;
    height:330px; overflow:auto; white-space:pre-wrap; color:#aab3c5; position:relative; }
  .logbox::after { content:''; position:sticky; bottom:-12px; display:block; height:24px; pointer-events:none;
    background:linear-gradient(transparent, rgba(6,8,15,.55)); }
  .ll { animation:logIn .3s ease both; }
  @keyframes logIn { from { opacity:0; transform:translateY(4px); } to { opacity:1; transform:none; } }
  .ll.info { color:#9bd6e8; } .ll.warn { color:#f5c97b; } .ll.err { color:#f39c9c; }
  .ll .ts { color:#5b6780; }

  /* config */
  .cfg-grid { display:grid; grid-template-columns:1fr 1fr; gap:10px 12px; margin-top:12px; }
  .cfg-grid label { display:block; font-size:11px; color:var(--muted); margin-bottom:4px; letter-spacing:.04em; }
  .cfg-grid input, .cfg-grid select { width:100%; padding:9px 11px; background:rgba(6,8,15,.7);
    border:1px solid var(--line); border-radius:10px; color:var(--text); font-size:13px;
    font-family:var(--mono); transition:border-color .2s, box-shadow .2s; }
  .cfg-grid input:focus, .cfg-grid select:focus { outline:none; border-color:var(--cyan);
    box-shadow:0 0 0 3px rgba(34,211,238,.14); }
  .cfg-grid input::placeholder { color:#4b5568; }
  .cfg-grid .wide { grid-column:1 / -1; }

  /* toggle */
  .switch-row { display:flex; align-items:center; justify-content:space-between; margin-top:16px;
    padding:10px 2px 2px; border-top:1px solid var(--line); }
  .switch-row .swlbl { font-size:12.5px; color:var(--text); display:flex; align-items:center; gap:8px; }
  .switch-row .swlbl small { color:var(--muted); font-weight:400; }
  .switch { position:relative; display:inline-block; width:42px; height:23px; flex:none; }
  .switch input { opacity:0; width:0; height:0; }
  .slider { position:absolute; cursor:pointer; inset:0; border-radius:999px; background:#1a2233;
    border:1px solid var(--line); transition:.25s; }
  .slider::before { content:''; position:absolute; width:17px; height:17px; left:2px; top:2px;
    border-radius:50%; background:#8a93a6; transition:.25s cubic-bezier(.2,.7,.3,1); }
  .switch input:checked + .slider { background:linear-gradient(135deg,var(--cyan),var(--violet)); border-color:transparent; }
  .switch input:checked + .slider::before { transform:translateX(19px); background:#fff; box-shadow:0 0 10px rgba(34,211,238,.7); }
  .switch input:focus-visible + .slider { box-shadow:0 0 0 3px rgba(34,211,238,.25); }

  .btnrow { display:flex; gap:10px; margin-top:18px; }
  .btn { border:none; border-radius:11px; padding:11px 18px; font-size:13.5px; font-weight:600;
    cursor:pointer; transition:transform .18s, box-shadow .18s, opacity .18s; position:relative; overflow:hidden; }
  .btn:active { transform:scale(.97); }
  .btn.primary { background:var(--grad); color:#fff; box-shadow:0 8px 22px -10px rgba(139,92,246,.7); }
  .btn.primary::after { content:''; position:absolute; top:0; left:-80%; width:50%; height:100%;
    background:linear-gradient(100deg, transparent, rgba(255,255,255,.35), transparent);
    transform:skewX(-20deg); transition:left .5s; }
  .btn.primary:hover::after { left:130%; }
  .btn.primary:hover { transform:translateY(-1px); box-shadow:0 12px 26px -10px rgba(139,92,246,.85); }
  .btn.ghost { background:transparent; border:1px solid var(--line); color:var(--text); }
  .btn.ghost:hover { border-color:rgba(34,211,238,.5); color:var(--cyan); }
  .btn.saved { background:linear-gradient(135deg,#10b981,#059669); }
  .btn:disabled { opacity:.55; cursor:not-allowed; }

  footer { margin-top:26px; display:flex; align-items:center; justify-content:center; gap:10px;
    font-size:11.5px; color:var(--muted); font-family:var(--mono); }
  footer a { color:var(--muted); text-decoration:none; border-bottom:1px dashed rgba(148,163,184,.35); }
  footer a:hover { color:var(--cyan); }
  .dotsep { color:#3a4458; }

  .toast { position:fixed; bottom:24px; left:50%; transform:translate(-50%, 80px); z-index:50;
    background:linear-gradient(135deg,#0ea5e9,#7c3aed); color:#fff; padding:12px 22px; border-radius:12px;
    font-size:13.5px; font-weight:600; box-shadow:0 16px 40px -12px rgba(0,0,0,.7);
    opacity:0; transition:transform .35s cubic-bezier(.2,.7,.3,1), opacity .35s; pointer-events:none; }
  .toast.show { transform:translate(-50%,0); opacity:1; }

  /* guide */
  .guide-intro { font-size:13px; color:var(--muted); margin:0 0 16px; line-height:1.6; }
  .code-inline { font-family:var(--mono); font-size:12px; color:var(--cyan); background:rgba(34,211,238,.08);
    padding:2px 7px; border-radius:6px; }
  .guide-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
  .guide-block { background:rgba(6,8,15,.5); border:1px solid var(--line); border-radius:12px; padding:14px; position:relative; }
  .guide-block h3 { margin:0 0 10px; font-size:12.5px; color:var(--text); font-weight:600; }
  .code-block { margin:0 0 10px; font-family:var(--mono); font-size:11.5px; line-height:1.7; color:#aab3c5;
    white-space:pre-wrap; word-break:break-all; }
  .btn.mini { padding:5px 12px; font-size:11px; border-radius:7px; position:absolute; top:10px; right:10px; }
  .models-list { display:flex; flex-wrap:wrap; gap:8px; margin-top:10px; }
  .model-chip { font-family:var(--mono); font-size:11.5px; color:var(--cyan); background:rgba(34,211,238,.07);
    border:1px solid rgba(34,211,238,.2); border-radius:8px; padding:5px 11px; cursor:pointer;
    transition:background .2s, border-color .2s; }
  .model-chip:hover { background:rgba(34,211,238,.16); border-color:rgba(34,211,238,.5); }
  .model-chip.used { color:var(--green); border-color:rgba(52,211,153,.4); }
  @media (max-width:920px) { .guide-grid { grid-template-columns:1fr; } }

  /* pool status */
  .pool-rows { margin-top:12px; }
  .prow { display:flex; align-items:center; justify-content:space-between; gap:10px;
    padding:9px 2px; border-bottom:1px solid var(--line); font-size:12.5px; color:var(--muted); }
  .prow:last-child { border-bottom:none; }
  .prow b { color:var(--text); font-size:13px; }
  .mono { font-family:var(--mono); }
  .pool-tip { margin-top:12px; font-size:11.5px; color:var(--faint); line-height:1.6;
    background:rgba(251,191,36,.05); border:1px solid rgba(251,191,36,.15); border-radius:10px; padding:10px 12px; display:none; }
  .pool-tip.show { display:block; }

  /* how it works + steps */
  .howit p { font-size:12.5px; color:var(--muted); line-height:1.65; margin:0 0 10px; }
  .howit p b { color:var(--cyan); }
  .howit p.dim { color:var(--faint); border-top:1px solid var(--line); padding-top:10px; margin-top:12px; }
  .steps { display:grid; gap:10px; margin-bottom:16px; }
  .step { display:flex; gap:12px; align-items:flex-start; background:rgba(6,8,15,.5);
    border:1px solid var(--line); border-radius:12px; padding:12px 14px; font-size:12.5px; color:var(--muted); line-height:1.6; }
  .step b { color:var(--text); }
  .step .n { flex:none; width:24px; height:24px; border-radius:50%; display:grid; place-items:center;
    background:var(--grad); color:#fff; font-size:12px; font-weight:700; font-family:var(--mono); }

  @media (max-width:920px) {
    .stats { grid-template-columns:repeat(2,1fr); }
    .main-grid { grid-template-columns:1fr; }
  }
  @media (prefers-reduced-motion: reduce) {
    *, *::before, *::after { animation:none !important; transition:none !important; }
  }
</style>
</head>
<body>
<div class="aurora"><div class="blob a"></div><div class="blob b"></div><div class="blob c"></div></div>
<div class="gridlines"></div>

<div class="wrap">
  <header>
    <div class="brand">
      <div class="logo-ring"></div>
      <div>
        <h1>ip-relay</h1>
        <div class="tag">egress control plane</div>
      </div>
    </div>
    <div class="hdr-right">
      <button class="baseurl-chip" id="baseUrlChip" title="Copy base URL">
        <span class="bu-lbl">BASE URL</span>
        <span class="bu-val" id="baseUrlVal">…</span>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
      </button>
      <span class="uptime" id="poolAge">pool warming…</span>
      <span id="statusPill" class="pill warn">loading</span>
    </div>
  </header>

  <section class="stats">
    <div class="card stat" style="--i:0">
      <div class="stat-top"><span class="lbl">Proxy pool</span><span class="ic cyan">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><circle cx="12" cy="12" r="8.2"/><circle cx="12" cy="12" r="2.6" fill="currentColor" stroke="none"/><path d="M12 3.8v2.4M12 17.8v2.4M3.8 12h2.4M17.8 12h2.4"/></svg>
      </span></div>
      <div class="big" id="pool">–</div>
      <div class="sub">working egress IPs</div>
    </div>
    <div class="card stat" style="--i:1">
      <div class="stat-top"><span class="lbl">Requests</span><span class="ic violet">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12h4l3-8 4 16 3-8h4"/></svg>
      </span></div>
      <div class="big" id="requests">–</div>
      <div class="sub">total served</div>
      <div class="spark-wrap"><canvas id="spark" width="140" height="34" style="width:140px;height:34px"></canvas></div>
    </div>
    <div class="card stat" style="--i:2">
      <div class="stat-top"><span class="lbl">Rotations</span><span class="ic mag">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 10a8 8 0 0 1 14.9-2M20 14a8 8 0 0 1-14.9 2"/><path d="M19 4v4h-4M5 20v-4h4"/></svg>
      </span></div>
      <div class="big" id="rotations">–</div>
      <div class="sub">quota hit → IP switched</div>
    </div>
    <div class="card stat" style="--i:3">
      <div class="stat-top"><span class="lbl">Lane failures</span><span class="ic grn">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 21a9 9 0 1 0-9-9"/><path d="M3 12h6M3 12l3-3M3 12l3 3"/></svg>
      </span></div>
      <div class="big" id="laneFailures">–</div>
      <div class="sub">dead lanes parked</div>
    </div>
  </section>

  <section class="panel mesh-panel">
    <div class="panel-h"><h2>Egress mesh</h2><span class="hint" id="meshHint">live traffic through rotating IPs</span></div>
    <div class="mesh-wrap">
      <svg id="mesh" viewBox="0 0 800 210" preserveAspectRatio="xMidYMid meet" role="img" aria-label="egress mesh visualization">
        <defs>
          <linearGradient id="laneGrad" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0" stop-color="#22d3ee" stop-opacity=".8"/>
            <stop offset=".55" stop-color="#8b5cf6" stop-opacity=".8"/>
            <stop offset="1" stop-color="#e879f9" stop-opacity=".8"/>
          </linearGradient>
          <radialGradient id="dotGrad" cx=".5" cy=".5" r=".5">
            <stop offset="0" stop-color="#cffafe"/>
            <stop offset="1" stop-color="#22d3ee"/>
          </radialGradient>
          <filter id="glow" x="-80%" y="-80%" width="260%" height="260%">
            <feGaussianBlur stdDeviation="3" result="b"/>
            <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
          </filter>
        </defs>
        <g fill="none" stroke="url(#laneGrad)" stroke-width="1.4" opacity=".5">
          <path d="M 96 105 C 210 50, 300 50, 400 105 S 590 160, 704 105" opacity=".55"/>
          <path d="M 96 105 C 210 40, 300 40, 400 105 S 590 170, 704 105" opacity=".22"/>
          <path d="M 96 105 C 210 60, 300 60, 400 105 S 590 150, 704 105" opacity=".22"/>
        </g>
        <g id="dots"></g>
        <g font-family="ui-monospace,Menlo,monospace" font-size="11" fill="#8794ab">
          <circle cx="96" cy="105" r="9" fill="none" stroke="#22d3ee" stroke-width="1.4" opacity=".9"/>
          <circle cx="96" cy="105" r="2.6" fill="#22d3ee"/>
          <text x="96" y="136" text-anchor="middle">client</text>
          <circle cx="704" cy="105" r="9" fill="none" stroke="#e879f9" stroke-width="1.4" opacity=".9"/>
          <circle cx="704" cy="105" r="2.6" fill="#e879f9"/>
          <text x="704" y="136" text-anchor="middle">upstream</text>
        </g>
      </svg>
      <div class="mesh-core" id="meshCore">
        <div class="ring"></div>
        <div class="count" id="meshCount">0</div>
        <div class="cap">egress IPs</div>
      </div>
    </div>
    <div class="mesh-note" id="meshNote">each dot = a request flowing through a fresh proxy IP</div>
  </section>

  <div class="main-grid">
    <section class="panel">
      <div class="panel-h"><h2>Live log</h2><span class="hint" id="logCount"></span></div>
      <div class="logbox" id="logbox">(no logs yet — pool is warming)</div>
    </section>

    <section class="panel">
      <div class="panel-h"><h2>Pool status</h2><span class="hint" id="poolStateHint"></span></div>
      <div class="pool-rows">
        <div class="prow"><span>Working lanes</span><b id="psAlive" class="mono">–</b></div>
        <div class="prow"><span>Parked (burned, cooling down)</span><b id="psParked" class="mono">–</b></div>
        <div class="prow"><span>Untested candidates queued</span><b id="psQueue" class="mono">–</b></div>
        <div class="prow"><span>Probes run / found / burned</span><b id="psProbes" class="mono">–</b></div>
        <div class="prow"><span>Direct lane (this server's IP)</span><b id="psDirect" class="mono">–</b></div>
      </div>
      <div class="pool-tip" id="poolTip"></div>
    </section>
  </div>

  <div class="main-grid" style="margin-top:16px">
    <section class="panel">
      <div class="panel-h"><h2>Configuration</h2><span class="hint">applies live</span></div>
      <div class="cfg-grid">
        <label class="wide">Upstream API URL
          <input type="text" id="cfg_upstream_base_url" placeholder="https://opencode.ai/zen/v1" spellcheck="false">
        </label>
        <label>Upstream key
          <input type="text" id="cfg_upstream_api_key" placeholder="public" spellcheck="false">
        </label>
        <label>Relay key (protects dashboard)
          <input type="password" id="cfg_relay_api_key" placeholder="leave empty for open" autocomplete="off">
        </label>
        <label>Refresh interval (s)
          <input type="number" id="cfg_proxy_refresh_sec" min="30" step="30">
        </label>
        <label>Test concurrency
          <input type="number" id="cfg_proxy_test_concurrency" min="1">
        </label>
        <label>Max candidates
          <input type="number" id="cfg_proxy_max_candidates" min="5">
        </label>
        <label>Pool target (lanes)
          <input type="number" id="cfg_proxy_pool_target" min="1">
        </label>
        <label>Probe timeout (s)
          <input type="number" id="cfg_probe_timeout" min="5">
        </label>
        <label>Probe model
          <input type="text" id="cfg_probe_model" placeholder="deepseek-v4-flash-free" spellcheck="false">
        </label>
      </div>
      <div class="switch-row">
        <span class="swlbl">Direct lane <small>use this server's own IP as a fallback</small></span>
        <label class="switch"><input type="checkbox" id="cfg_direct_lane"><span class="slider"></span></label>
      </div>
      <div class="btnrow">
        <button id="saveBtn" class="btn primary">Save changes</button>
        <button id="refreshBtn" class="btn ghost">Refresh pool</button>
      </div>
    </section>

    <section class="panel">
      <div class="panel-h"><h2>How it works</h2><span class="hint">60-second read</span></div>
      <div class="howit">
        <p><b>1.</b> Your app sends a normal OpenAI-style request to this relay.</p>
        <p><b>2.</b> The relay forwards it to the upstream <em>through a proxy</em>, so the upstream sees the proxy's IP — not yours.</p>
        <p><b>3.</b> When an IP hits its free quota (HTTP 429), the relay parks it for an hour and retries on the next IP automatically.</p>
        <p><b>4.</b> A background churner constantly tests fresh proxies from public lists to keep the pool full — you don't manage anything.</p>
        <p class="dim">Reality check: free proxies die fast and many are already rate-limited by other users. Expect the pool to hover well below the candidate count — that's normal. More tested candidates = more working lanes.</p>
      </div>
    </section>
  </div>

  <section class="panel guide" style="margin-top:16px">
    <div class="panel-h"><h2>How to connect</h2><span class="hint" id="guideModelCount"></span></div>
    <p class="guide-intro">This relay speaks the OpenAI API. Point any OpenAI-compatible client at <code class="code-inline" id="relayBase">…</code> and use one of the models below. Three steps:</p>
    <div class="steps">
      <div class="step"><span class="n">1</span><div><b>Copy your base URL</b> — it's filled in below, already pointing at this server.<br><span class="code-inline" id="relayBase2">…</span> <button class="btn ghost mini" style="position:static" data-copy="relayBase2">Copy</button></div></div>
      <div class="step"><span class="n">2</span><div><b>Pick a model</b> from the list at the bottom (click any chip to copy). No provider prefix needed — the relay strips any.</div></div>
      <div class="step"><span class="n">3</span><div><b>Use any key as the API key</b> — e.g. <span class="code-inline">public</span> — unless you set a relay key in Configuration, then use that.</div></div>
    </div>

    <div class="guide-grid">
      <div class="guide-block">
        <h3>📋 curl</h3>
        <pre class="code-block" id="codeCurl">curl BASE/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer public" \
  -d '{"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"hi"}]}'</pre>
        <button class="btn ghost mini" data-copy="codeCurl">Copy</button>
      </div>
      <div class="guide-block">
        <h3>🤖 Claude Code</h3>
        <pre class="code-block" id="codeClaude"># ~/.claude/settings.json
{
  "env": {
    "ANTHROPIC_BASE_URL": "BASE",
    "ANTHROPIC_AUTH_TOKEN": "public"
  }
}
# then: claude --model deepseek-v4-flash-free</pre>
        <button class="btn ghost mini" data-copy="codeClaude">Copy</button>
      </div>
      <div class="guide-block">
        <h3>🐍 OpenAI SDK (Python)</h3>
        <pre class="code-block" id="codePy">from openai import OpenAI
client = OpenAI(
    base_url="BASE",
    api_key="public",
)
r = client.chat.completions.create(
    model="deepseek-v4-flash-free",
    messages=[{"role":"user","content":"hi"}],
)
print(r.choices[0].message.content)</pre>
        <button class="btn ghost mini" data-copy="codePy">Copy</button>
      </div>
      <div class="guide-block">
        <h3>🔀 Through 9router</h3>
        <pre class="code-block" id="code9r"># 9router dashboard → Providers → Add:
#   Type:     OpenAI Compatible
#   Base URL: BASE   ← use the host's docker bridge IP
#                      (e.g. http://172.17.0.1:PORT/v1) if
#                      9router runs in Docker on this machine
#   API Key:  public
# then call: PROVIDER_PREFIX/deepseek-v4-flash-free</pre>
        <button class="btn ghost mini" data-copy="code9r">Copy</button>
      </div>
    </div>

    <div class="models-row">
      <div class="panel-h" style="margin-top:14px"><h2>Available models</h2><span class="hint">from upstream, cached — click to copy</span></div>
      <div id="modelsList" class="models-list">loading…</div>
    </div>
  </section>

  <footer>
    <span>ip-relay v0.5.0</span><span class="dotsep">·</span>
    <a href="https://github.com/sajjadgazergar-work/ip-relay" target="_blank" rel="noopener">github</a><span class="dotsep">·</span>
    <span id="footState">—</span>
  </footer>
</div>

<div class="toast" id="toast"></div>

<script>
function $(id){ return document.getElementById(id); }
function esc(s){ return s.replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }
function toast(msg){ const t=$('toast'); t.textContent=msg; t.classList.add('show'); clearTimeout(t._h); t._h=setTimeout(()=>t.classList.remove('show'),2600); }

// dynamic base URL — the dashboard knows where it's being served from
const BASE = location.origin + '/v1';
['relayBase','relayBase2','baseUrlVal'].forEach(id => { const el=$(id); if(el) el.textContent = BASE; });
['codeCurl','codeClaude','codePy','code9r'].forEach(id => {
  const el=$(id); if(el) el.innerHTML = el.innerHTML.split('BASE').join(BASE);
});
$('baseUrlChip').addEventListener('click', ()=>{ copyText(BASE); toast('Base URL copied: ' + BASE); });

async function jget(url){ const r=await fetch(url); if(r.status===401){ window.location.href='/login'; throw new Error('unauthorized'); } if(!r.ok) throw new Error((await r.text()).slice(0,120)); return r.json(); }

function setPill(el, state, label){
  el.className = 'pill ' + state;
  el.textContent = label;
}

function countUp(el, to, dur){
  dur = dur || 600;
  const from = parseInt(String(el.textContent).replace(/,/g,'')) || 0;
  if(from === to){ el.textContent = to.toLocaleString(); return; }
  const t0 = performance.now();
  function tick(t){
    const p = Math.min(1, (t - t0) / dur);
    const v = Math.round(from + (to - from) * (1 - Math.pow(1 - p, 3)));
    el.textContent = v.toLocaleString();
    if(p < 1) requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
}

// sparkline of request rate
const sparkData = [];
let lastReq = null;
function drawSpark(){
  const cv = $('spark'); if(!cv) return;
  const dpr = window.devicePixelRatio || 1;
  const W = 140, H = 34;
  cv.width = W*dpr; cv.height = H*dpr; cv.style.width = W+'px'; cv.style.height = H+'px';
  const ctx = cv.getContext('2d'); ctx.scale(dpr, dpr); ctx.clearRect(0,0,W,H);
  if(sparkData.length < 2) return;
  const max = Math.max.apply(null, sparkData.concat([1]));
  ctx.beginPath();
  sparkData.forEach((v,i)=>{
    const x = i/(sparkData.length-1)*W;
    const y = H - 3 - (v/max)*(H-8);
    i ? ctx.lineTo(x,y) : ctx.moveTo(x,y);
  });
  const g = ctx.createLinearGradient(0,0,W,0);
  g.addColorStop(0,'#22d3ee'); g.addColorStop(1,'#e879f9');
  ctx.strokeStyle = g; ctx.lineWidth = 2; ctx.lineJoin = 'round'; ctx.stroke();
  const last = sparkData[sparkData.length-1];
  const lx = (sparkData.length-1)/(sparkData.length-1)*W;
  const ly = H - 3 - (last/max)*(H-8);
  ctx.lineTo(lx, H); ctx.lineTo(0, H); ctx.closePath();
  const fg = ctx.createLinearGradient(0,0,0,H);
  fg.addColorStop(0,'rgba(34,211,238,.28)'); fg.addColorStop(1,'rgba(34,211,238,0)');
  ctx.fillStyle = fg; ctx.fill();
}
function pushSpark(v){ sparkData.push(v); if(sparkData.length > 30) sparkData.shift(); drawSpark(); }

// mesh animation — one glowing dot per request lane, count = pool size
const MESH_PATH = 'M 96 105 C 210 50, 300 50, 400 105 S 590 160, 704 105';
function renderMesh(pool){
  const g = $('dots'), core = $('meshCore'), cnt = $('meshCount'); if(!g) return;
  cnt.textContent = pool;
  const want = Math.max(0, Math.min(pool, 26));
  while(g.children.length > want) g.removeChild(g.lastChild);
  while(g.children.length < want){
    const c = document.createElementNS('http://www.w3.org/2000/svg','circle');
    c.setAttribute('r', 3.2);
    c.setAttribute('fill', 'url(#dotGrad)');
    c.setAttribute('filter', 'url(#glow)');
    const am = document.createElementNS('http://www.w3.org/2000/svg','animateMotion');
    am.setAttribute('dur','2.6s');
    am.setAttribute('repeatCount','indefinite');
    am.setAttribute('path', MESH_PATH);
    am.setAttribute('begin', (-g.children.length * 0.12) + 's');
    c.appendChild(am);
    g.appendChild(c);
  }
  core.classList.toggle('empty', pool === 0);
  $('meshNote').textContent = pool === 0
    ? 'warming up — testing proxy candidates, first lanes take a few minutes'
    : 'each dot = a request flowing through a fresh proxy IP';
}

function renderPoolStatus(h){
  if(!h) return;
  const st = h.stats || {}, sw = h.sweep || {};
  $('psAlive').textContent = (h.pool ?? 0) + ' / ' + (h.pool_target ?? '?') + ' target';
  $('psParked').textContent = h.parked ?? 0;
  $('psQueue').textContent = h.candidates ?? 0;
  $('psProbes').textContent = (st.candidates_tested||0) + ' / ' + (st.probes_ok||0) + ' / ' + (st.probes_burned||0);
  $('psDirect').textContent = 'unknown';
  $('poolStateHint').textContent = sw.state === 'testing' ? 'testing batch…' : (sw.state || 'idle');
  const tip = $('poolTip');
  if((h.pool ?? 0) === 0){
    tip.classList.add('show');
    tip.textContent = (h.candidates > 0)
      ? 'Pool is empty but ' + h.candidates + ' candidates are queued — the churner is testing them now. First working lanes usually appear within a few minutes. Watch the live log.'
      : 'Pool is empty and no candidates are queued — the proxy lists may be unreachable from this server. Check the live log for fetch errors.';
  } else if((h.pool ?? 0) < 5){
    tip.classList.add('show');
    tip.textContent = 'Pool is thin. Free proxies burn out fast — the churner keeps testing new candidates continuously. Raising "Max candidates" in Configuration widens the search.';
  } else {
    tip.classList.remove('show');
  }
}

// log rendering
let logShown = 0;
function lineClass(l){
  if(/ERROR|Traceback|Exception/.test(l)) return 'err';
  if(/WARNING|WARN /.test(l)) return 'warn';
  if(/429|502|503|500|504/.test(l)) return 'warn';
  return 'info';
}
function renderLogs(lines){
  const box = $('logbox'); if(!box) return;
  if(lines.length < logShown){ box.innerHTML=''; logShown=0; }
  const fresh = lines.slice(logShown);
  logShown = lines.length;
  const frag = document.createDocumentFragment();
  fresh.forEach(l=>{
    const div = document.createElement('div');
    div.className = 'll ' + lineClass(l);
    div.textContent = l;
    frag.appendChild(div);
  });
  box.appendChild(frag);
  while(box.children.length > 220) box.removeChild(box.firstChild);
  box.scrollTop = box.scrollHeight;
  $('logCount').textContent = logShown + ' lines';
}

let h = null;
async function refresh(){
  try{
    h = await jget('/healthz');
    const ok = !!h.ok;
    setPill($('statusPill'), ok ? 'ok' : (h.pool === 0 ? 'warn' : 'err'),
            ok ? 'healthy' : (h.pool === 0 ? 'warming' : 'degraded'));
    countUp($('pool'), h.pool || 0);
    countUp($('requests'), h.stats ? (h.stats.requests||0) : 0);
    countUp($('rotations'), h.stats ? (h.stats.rotations||0) : 0);
    countUp($('laneFailures'), h.stats ? (h.stats.lane_failures||0) : 0);
    if(lastReq !== null) pushSpark(Math.max(0, (h.stats.requests||0) - lastReq));
    lastReq = h.stats ? (h.stats.requests||0) : 0;
    renderMesh(h.pool || 0);
    renderPoolStatus(h);
    renderModels(h.models || []);
    const logs = await jget('/api/logs?n=100');
    renderLogs(logs.logs || []);
  }catch(e){ setPill($('statusPill'),'err','offline'); }
}

function renderModels(models){
  const el = $('modelsList'); if(!el) return;
  if(!models.length){ el.innerHTML = '<span class="muted">no models yet — pool warming</span>'; return; }
  el.innerHTML = '';
  models.forEach(m=>{
    const chip = document.createElement('span');
    chip.className = 'model-chip';
    chip.textContent = m;
    chip.title = 'Click to copy: ' + m;
    chip.addEventListener('click', ()=>{ copyText(m); toast('Copied: ' + m); });
    el.appendChild(chip);
  });
  $('guideModelCount').textContent = models.length + ' models';
}

function copyText(t){
  if(navigator.clipboard){ navigator.clipboard.writeText(t).catch(()=>{}); }
  else { const ta=document.createElement('textarea'); ta.value=t; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); ta.remove(); }
}
document.querySelectorAll('[data-copy]').forEach(b=>{
  b.addEventListener('click', ()=>{ const el=$(b.dataset.copy); if(el) copyText(el.textContent.trim()); toast('Copied ✓'); });
});

function ageTick(){
  if(!h || !h.updated) return;
  const s = Math.max(0, Math.round(Date.now()/1000 - h.updated));
  $('poolAge').textContent = 'pool updated ' + s + 's ago';
  $('footState').textContent = h.key ? ('upstream key: ' + h.key) : 'no upstream key';
}
setInterval(ageTick, 1000);

async function loadSettings(){
  try{
    const s = await jget('/api/settings');
    $('cfg_upstream_base_url').value = s.upstream_base_url || '';
    $('cfg_upstream_api_key').value = s.upstream_api_key || '';
    $('cfg_relay_api_key').value = s.relay_api_key || '';
    $('cfg_proxy_refresh_sec').value = s.proxy_refresh_sec ?? 600;
    $('cfg_proxy_test_concurrency').value = s.proxy_test_concurrency ?? 25;
    $('cfg_proxy_max_candidates').value = s.proxy_max_candidates ?? 2000;
    $('cfg_proxy_pool_target').value = s.proxy_pool_target ?? 25;
    $('cfg_probe_timeout').value = s.probe_timeout ?? 20;
    $('cfg_probe_model').value = s.probe_model || '';
    $('cfg_direct_lane').checked = !!s.direct_lane;
  }catch(e){}
}

async function saveConfig(){
  const body = {
    upstream_base_url: $('cfg_upstream_base_url').value.trim(),
    upstream_api_key: $('cfg_upstream_api_key').value.trim(),
    relay_api_key: $('cfg_relay_api_key').value.trim(),
    proxy_refresh_sec: parseInt($('cfg_proxy_refresh_sec').value || '600', 10),
    proxy_test_concurrency: parseInt($('cfg_proxy_test_concurrency').value || '25', 10),
    proxy_max_candidates: parseInt($('cfg_proxy_max_candidates').value || '2000', 10),
    proxy_pool_target: parseInt($('cfg_proxy_pool_target').value || '25', 10),
    probe_timeout: parseInt($('cfg_probe_timeout').value || '20', 10),
    probe_model: $('cfg_probe_model').value.trim(),
    direct_lane: $('cfg_direct_lane').checked,
  };
  const btn = $('saveBtn');
  btn.disabled = true; btn.textContent = 'Saving…';
  try{
    const r = await fetch('/api/settings', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
    if(!r.ok) throw new Error((await r.text()).slice(0,120));
    btn.classList.add('saved'); btn.textContent = 'Saved ✓';
    setTimeout(()=>{ btn.classList.remove('saved'); btn.textContent = 'Save changes'; btn.disabled = false; }, 1600);
    toast('Configuration saved');
    refresh();
  }catch(e){
    btn.textContent = 'Save changes'; btn.disabled = false;
    toast('Error: ' + e.message);
  }
}

async function refreshPool(){
  try{ await jget('/api/refresh'); toast('Pool refresh started'); }
  catch(e){ toast('Error: ' + e.message); }
}

$('saveBtn').addEventListener('click', saveConfig);
$('refreshBtn').addEventListener('click', refreshPool);

refresh();
loadSettings();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)


LOGIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ip-relay — login</title>
<style>
  :root { --bg:#0f1115; --card:#171a21; --line:#262b36; --text:#e6e9ef; --muted:#8b93a3; --blue:#4c8dff; }
  * { box-sizing:border-box; }
  body { margin:0; min-height:100vh; display:flex; align-items:center; justify-content:center; background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; }
  .box { background:var(--card); border:1px solid var(--line); border-radius:14px; padding:36px; width:340px; }
  h1 { font-size:20px; margin:0 0 6px; }
  .sub { color:var(--muted); font-size:13px; margin-bottom:22px; }
  label { display:block; font-size:12px; color:var(--muted); margin-bottom:6px; }
  input[type=password] { width:100%; padding:10px 12px; background:#0d0f13; border:1px solid var(--line); border-radius:8px; color:var(--text); font-size:14px; }
  input:focus { outline:none; border-color:var(--blue); }
  button { width:100%; margin-top:16px; background:var(--blue); color:#fff; border:none; border-radius:8px; padding:11px; font-size:14px; font-weight:600; cursor:pointer; }
  button:hover { opacity:.9; }
  .err { color:#e5484d; font-size:13px; margin-top:10px; min-height:18px; }
</style>
</head>
<body>
<div class="box">
  <h1>ip-relay dashboard</h1>
  <div class="sub">Enter the relay key to continue</div>
  <form id="f">
    <label>Relay key</label>
    <input type="password" id="key" autofocus>
    <div class="err" id="err"></div>
    <button type="submit">Unlock</button>
  </form>
</div>
<script>
document.getElementById('f').addEventListener('submit', async (e) => {
  e.preventDefault();
  const key = document.getElementById('key').value.trim();
  const r = await fetch('/login', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({key}) });
  if (r.ok) { window.location.href = '/'; }
  else { document.getElementById('err').textContent = 'Wrong key'; }
});
</script>
</body>
</html>
"""


@app.get("/login", response_class=HTMLResponse)
async def login_page():
    return HTMLResponse(LOGIN_HTML)


@app.post("/login")
async def login_submit(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)
    if not RELAY_API_KEY:
        return {"ok": True}
    if str(body.get("key", "")) == RELAY_API_KEY:
        import hashlib
        cookie = hashlib.sha256(f"ip-relay:{RELAY_API_KEY}".encode()).hexdigest()
        resp = JSONResponse({"ok": True})
        resp.set_cookie("ip_relay_auth", cookie, httponly=True, samesite="lax", max_age=60 * 60 * 24)
        return resp
    return JSONResponse({"error": "wrong key"}, status_code=401)


@app.get("/api/settings")
async def get_settings_api(request: Request):
    if not _check_dashboard_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return public_settings()


@app.post("/api/settings")
async def post_settings_api(request: Request):
    if not _check_dashboard_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
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
async def logs_api(request: Request, n: int = 100):
    if not _check_dashboard_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return {"logs": list(LOG_RING)[-n:]}


@app.post("/api/refresh")
async def refresh_api(request: Request):
    if not _check_dashboard_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    asyncio.create_task(refresh_pool(force=True))
    return {"ok": True}


# ── pool status & diagnostics ─────────────────────────────────────

@app.get("/api/pool")
async def pool_api(request: Request):
    """Full pool breakdown: alive lanes, parked (burned) lanes, queue depth."""
    if not _check_dashboard_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    now = time.time()
    async with pool["lock"]:
        alive = [p for p in pool["proxies"] if cooldowns.get(p, 0) <= now]
        parked = [{"proxy": p, "ready_in_s": round(cooldowns[p] - now)}
                  for p in pool["proxies"] if cooldowns.get(p, 0) > now]
    return {
        "alive": alive,
        "parked": sorted(parked, key=lambda x: x["ready_in_s"]),
        "queue": len(candidates),
        "target": PROXY_POOL_TARGET,
        "sweep": dict(sweep),
        "stats": stats,
        "direct_lane": DIRECT_LANE,
        "direct_burned": time.time() < direct_burned_until,
        "updated": pool["updated"],
    }


@app.get("/api/stats")
async def stats_api(request: Request):
    """Compact stats for the dashboard cards."""
    if not _check_dashboard_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    async with pool["lock"]:
        alive = len([p for p in pool["proxies"] if cooldowns.get(p, 0) <= time.time()])
    return {
        "pool": alive,
        "pool_target": PROXY_POOL_TARGET,
        "candidates": len(candidates),
        "parked": len([1 for t in cooldowns.values() if t > time.time()]),
        "stats": stats,
        "sweep": dict(sweep),
        "updated": pool["updated"],
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
