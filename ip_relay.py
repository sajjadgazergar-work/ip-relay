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
import json
import logging
import os
import random
import time
import uuid

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

log = logging.getLogger("ip-relay")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

UPSTREAM_API_KEY = os.environ.get("UPSTREAM_API_KEY", os.environ.get("OPENCODE_API_KEY", "public")).strip()
UPSTREAM_BASE_URL = os.environ.get("UPSTREAM_BASE_URL", os.environ.get("OPENCODE_BASE_URL", "https://opencode.ai/zen/v1")).rstrip("/")
RELAY_API_KEY = os.environ.get("RELAY_API_KEY", "").strip()
PROXY_REFRESH_SEC = int(os.environ.get("PROXY_REFRESH_SEC", "600"))
PROXY_TEST_CONCURRENCY = int(os.environ.get("PROXY_TEST_CONCURRENCY", "12"))
PROXY_MAX_CANDIDATES = int(os.environ.get("PROXY_MAX_CANDIDATES", "150"))
DIRECT_LANE = os.environ.get("DIRECT_LANE", "1") in ("1", "true", "yes")

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

UPSTREAM_PROBE = {
    "model": os.environ.get("PROBE_MODEL", "deepseek-v4-flash-free"),
    "messages": [{"role": "user", "content": "hi"}],
    "max_tokens": 1,
    "stream": False,
}


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
    """Wrap an upstream raw-stream iterator: treat StreamClosed / client-abort
    as a normal end-of-stream, not a crash. The relay is a pass-through, so
    when the upstream closes (or the client went away) we just stop."""
    try:
        async for chunk in ait:
            yield chunk
    except (httpx.StreamClosed, httpx.RemoteProtocolError, asyncio.CancelledError):
        return
    except Exception:
        try:
            yield b"data: [DONE]\n\n"
        except Exception:
            pass
        return


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
                    async with client.stream(method, url, headers=headers, **body_kw) as resp:
                        if resp.status_code == 429:
                            body = await resp.aread()
                            if is_quota_429(body, 429):
                                mark_burn(proxy, label)
                                last_err = (429, body)
                                continue
                        return resp.status_code, dict(resp.headers), resp.aiter_raw()
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
        return StreamingResponse(safe_aiter(body), status_code=status, media_type=resp_headers.get("content-type", "text/event-stream"))
    return Response(content=body, status_code=status, media_type=resp_headers.get("content-type", "application/json"))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
