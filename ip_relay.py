# ip-relay — per-IP quota API relay with automatic egress rotation
#
# Sits between a gateway (9router, OpenRouter-style aggregators, your own
# client) and ANY upstream OpenAI-compatible API whose free tier is limited
# PER IP ADDRESS (e.g. opencode.ai/zen/v1, Groq, SambaNova, Together, DeepSeek,
# or a custom endpoint). Each unique egress IP carries its own quota, so this
# relay rotates through a proxy pool and lets one base URL serve more
# free-model traffic than a single IP would allow.
# Your server's own IP never touches the upstream.
#
# ── design notes (v0.6, the resilience rewrite) ─────────────────────────
#
# Free public proxies are ephemeral: measured survival is ~1 request before
# death or rate-limit. So the pool manager can't promise "N stable lanes" —
# it must make requests survive *proxy churn* instead. This build does that:
#
#   1. MULTI-PROTOCOL SOURCES — HTTP/HTTPS + SOCKS4 + SOCKS5 + (optional)
#      Webshare free-tier API token, ~20 feeds. Wider net = more fresh IPs.
#   2. SCORED LANES — every lane carries an EWMA score (success, latency,
#      recency). Requests go to the best lane first, not a random one.
#   3. TRANSPARENT FAILOVER — a request that hits a dead/burned proxy is
#      retried on the next-best lane *within the same HTTP request*, so the
#      client never sees a proxy failure. Burn-on-429 is cheap because the
#      next lane is already warm.
#   4. RECOVERY LOOPS — quota windows reset. Parked (burned) lanes are
#      re-probed after a short cooldown and return to service automatically.
#      Nothing is discarded permanently.
#   5. CHEAP PRE-PROBE, REAL CONFIRM — candidates are first TCP/CONNECT
#      screened (no upstream quota spent), then confirmed with a 1-token
#      upstream probe. Slow-but-working proxies (up to PROXY_PROBE_TIMEOUT)
#      are kept — the old 12s cutoff threw away good lanes.
#
# Env config — see README.md for the full table.

from __future__ import annotations

import asyncio
import collections
import json
import logging
import os
import time
import uuid

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

log = logging.getLogger("ip-relay")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

VERSION = "0.6.0"

# ── settings (env as base, settings.json overlays; UI writes the file) ──
SETTINGS_FILE = os.environ.get("SETTINGS_FILE", "settings.json")
DEFAULTS = {
    "upstream_base_url": "https://opencode.ai/zen/v1",
    "upstream_api_key": "public",
    "relay_api_key": "",
    "probe_model": "deepseek-v4-flash-free",
    # egress / pool
    "proxy_pool_target": 30,          # how many confirmed lanes to keep warm
    "proxy_max_candidates": 3000,     # cap candidates held in the reservoir
    "proxy_test_concurrency": 60,     # concurrent probes
    "proxy_probe_timeout": 25,        # real-probe timeout (slow proxies OK)
    "relay_proxy_timeout": 40,        # per-attempt upstream timeout via proxy
    "relay_attempts": 6,              # lanes tried per client request (failover)
    "lane_cooldown_sec": 90,          # how long a burned lane is parked
    "lane_recover_sec": 240,          # parked lane re-probe interval
    "direct_lane": False,             # include the server's own IP as a lane
    "webshare_token": "",             # optional Webshare free-tier API token
    "allow_socks": True,              # use SOCKS4/5 sources (needs httpx[socks])
}
settings: dict = dict(DEFAULTS)

# runtime globals synced by apply_settings()
UPSTREAM_BASE_URL = DEFAULTS["upstream_base_url"]
UPSTREAM_API_KEY = DEFAULTS["upstream_api_key"]
RELAY_API_KEY = ""
PROBE_MODEL = DEFAULTS["probe_model"]
POOL_TARGET = DEFAULTS["proxy_pool_target"]
MAX_CANDIDATES = DEFAULTS["proxy_max_candidates"]
TEST_CONCURRENCY = DEFAULTS["proxy_test_concurrency"]
PROBE_TIMEOUT = DEFAULTS["proxy_probe_timeout"]
RELAY_PROXY_TIMEOUT = DEFAULTS["relay_proxy_timeout"]
RELAY_ATTEMPTS = DEFAULTS["relay_attempts"]
LANE_COOLDOWN_SEC = DEFAULTS["lane_cooldown_sec"]
LANE_RECOVER_SEC = DEFAULTS["lane_recover_sec"]
DIRECT_LANE = DEFAULTS["direct_lane"]
WEBSHARE_TOKEN = ""
ALLOW_SOCKS = DEFAULTS["allow_socks"]


def load_settings() -> None:
    s = {
        "upstream_base_url": os.environ.get("UPSTREAM_BASE_URL", os.environ.get("OPENCODE_BASE_URL", DEFAULTS["upstream_base_url"])).rstrip("/"),
        "upstream_api_key": os.environ.get("UPSTREAM_API_KEY", os.environ.get("OPENCODE_API_KEY", DEFAULTS["upstream_api_key"])).strip(),
        "relay_api_key": os.environ.get("RELAY_API_KEY", DEFAULTS["relay_api_key"]).strip(),
        "probe_model": os.environ.get("PROBE_MODEL", DEFAULTS["probe_model"]),
        "proxy_pool_target": int(os.environ.get("PROXY_POOL_TARGET", DEFAULTS["proxy_pool_target"])),
        "proxy_max_candidates": int(os.environ.get("PROXY_MAX_CANDIDATES", DEFAULTS["proxy_max_candidates"])),
        "proxy_test_concurrency": int(os.environ.get("PROXY_TEST_CONCURRENCY", DEFAULTS["proxy_test_concurrency"])),
        "proxy_probe_timeout": int(os.environ.get("PROXY_PROBE_TIMEOUT", DEFAULTS["proxy_probe_timeout"])),
        "relay_proxy_timeout": int(os.environ.get("RELAY_PROXY_TIMEOUT", DEFAULTS["relay_proxy_timeout"])),
        "relay_attempts": int(os.environ.get("RELAY_ATTEMPTS", DEFAULTS["relay_attempts"])),
        "lane_cooldown_sec": int(os.environ.get("LANE_COOLDOWN_SEC", DEFAULTS["lane_cooldown_sec"])),
        "lane_recover_sec": int(os.environ.get("LANE_RECOVER_SEC", DEFAULTS["lane_recover_sec"])),
        "direct_lane": os.environ.get("DIRECT_LANE", "0") in ("1", "true", "yes"),
        "webshare_token": os.environ.get("WEBSHARE_TOKEN", "").strip(),
        "allow_socks": os.environ.get("ALLOW_SOCKS", "1") in ("1", "true", "yes"),
    }
    try:
        with open(SETTINGS_FILE) as f:
            s.update({k: v for k, v in json.load(f).items() if k in DEFAULTS})
    except Exception:
        pass
    apply_settings(s, persist=False)


def save_settings() -> None:
    try:
        with open(SETTINGS_FILE, "w") as f:
            json.dump(settings, f, indent=2)
    except Exception as e:
        log.warning("could not save settings: %s", e)


def apply_settings(new: dict, persist: bool = True) -> dict:
    global UPSTREAM_API_KEY, UPSTREAM_BASE_URL, RELAY_API_KEY, PROBE_MODEL
    global POOL_TARGET, MAX_CANDIDATES, TEST_CONCURRENCY, PROBE_TIMEOUT
    global RELAY_PROXY_TIMEOUT, RELAY_ATTEMPTS, LANE_COOLDOWN_SEC, LANE_RECOVER_SEC
    global DIRECT_LANE, WEBSHARE_TOKEN, ALLOW_SOCKS
    for k, v in new.items():
        if k in DEFAULTS:
            settings[k] = v
    UPSTREAM_API_KEY = str(settings["upstream_api_key"]).strip()
    UPSTREAM_BASE_URL = str(settings["upstream_base_url"]).rstrip("/")
    RELAY_API_KEY = str(settings["relay_api_key"]).strip()
    PROBE_MODEL = str(settings["probe_model"])
    POOL_TARGET = max(1, int(settings["proxy_pool_target"]))
    MAX_CANDIDATES = max(10, int(settings["proxy_max_candidates"]))
    TEST_CONCURRENCY = max(1, int(settings["proxy_test_concurrency"]))
    PROBE_TIMEOUT = max(5, int(settings["proxy_probe_timeout"]))
    RELAY_PROXY_TIMEOUT = max(10, int(settings["relay_proxy_timeout"]))
    RELAY_ATTEMPTS = max(1, int(settings["relay_attempts"]))
    LANE_COOLDOWN_SEC = max(30, int(settings["lane_cooldown_sec"]))
    LANE_RECOVER_SEC = max(60, int(settings["lane_recover_sec"]))
    DIRECT_LANE = bool(settings["direct_lane"])
    WEBSHARE_TOKEN = str(settings["webshare_token"]).strip()
    ALLOW_SOCKS = bool(settings["allow_socks"])
    if persist:
        save_settings()
    return dict(settings)


def mask_key(k: str) -> str:
    return k[:6] + "..." if k else "(none)"


async def validate_webshare_token(token: str) -> dict:
    """Validate a single Webshare API token directly against Webshare API."""
    tok = token.strip()
    if not tok or "..." in tok or tok == "(none)":
        return {"token": mask_key(tok), "valid": False, "proxies": 0, "error": "Empty or masked token"}
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(8, connect=4)) as client:
            r = await client.get(
                "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=1",
                headers={"Authorization": f"Token {tok}"}
            )
            if r.status_code == 200:
                count = r.json().get("count", 0)
                return {"token": mask_key(tok), "valid": True, "proxies": count, "error": None}
            elif r.status_code == 401:
                return {"token": mask_key(tok), "valid": False, "proxies": 0, "error": "Invalid token (401 Unauthorized)"}
            else:
                return {"token": mask_key(tok), "valid": False, "proxies": 0, "error": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"token": mask_key(tok), "valid": False, "proxies": 0, "error": str(e)}


async def validate_upstream(base_url: str, api_key: str) -> dict:
    """Validate the upstream OpenAI-compatible base URL and API key."""
    url = base_url.rstrip("/")
    if not url:
        return {"valid": False, "error": "Base URL missing"}
    try:
        headers = {}
        if api_key and api_key != "(none)" and "..." not in api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        async with httpx.AsyncClient(timeout=httpx.Timeout(8, connect=4), verify=False) as client:
            r = await client.get(f"{url}/models", headers=headers)
            if r.status_code == 200:
                try:
                    models = [m.get("id") for m in r.json().get("data", []) if m.get("id")]
                    return {"valid": True, "models_count": len(models), "error": None}
                except Exception:
                    return {"valid": True, "models_count": 0, "error": None}
            else:
                return {"valid": False, "error": f"HTTP {r.status_code} from /models"}
    except Exception as e:
        return {"valid": False, "error": str(e)}


def public_settings() -> dict:
    s = dict(settings)
    if s.get("upstream_api_key"):
        s["upstream_api_key"] = mask_key(str(s["upstream_api_key"]))
    if s.get("relay_api_key"):
        s["relay_api_key"] = mask_key(str(s["relay_api_key"]))
    if s.get("webshare_token"):
        s["webshare_token"] = mask_key(str(s["webshare_token"]))
    return s


# ── log ring buffer (dashboard log viewer) ──────────────────────
LOG_RING = collections.deque(maxlen=600)


class RingHandler(logging.Handler):
    def emit(self, record):
        try:
            LOG_RING.append(self.format(record))
        except Exception:
            pass


logging.getLogger().addHandler(RingHandler())

app = FastAPI(title="ip-relay")


# ══════════════════════════════════════════════════════════════════
# EGRESS ENGINE
# ══════════════════════════════════════════════════════════════════

class Lane:
    """One egress route (a proxy, or the direct lane). Carries health score.

    score: EWMA in [0,1]. Success pushes toward 1, failure toward 0. Requests
    prefer the highest-scored warm lane. Latency is tracked for ranking."""
    __slots__ = ("addr", "proto", "score", "lat_ms", "ok", "fails",
                 "parked_until", "last_ok", "last_probe", "probe_tries")

    def __init__(self, addr: str, proto: str):
        self.addr = addr            # "ip:port" or "" for the direct lane
        self.proto = proto          # "http" | "https" | "socks4" | "socks5" | "direct"
        self.score = 0.5            # start neutral; probes move it
        self.lat_ms = 0.0
        self.ok = 0                 # lifetime successes
        self.fails = 0              # lifetime failures
        self.parked_until = 0.0     # burned: no requests until this ts
        self.last_ok = 0.0
        self.last_probe = 0.0
        self.probe_tries = 0

    @property
    def warm(self) -> bool:
        return self.parked_until < time.time() and self.score > 0.05

    def url(self) -> str | None:
        if self.proto == "direct":
            return None
        scheme = "http" if self.proto in ("http", "https") else self.proto
        return f"{scheme}://{self.addr}"

    def mark_ok(self, lat_ms: float):
        self.ok += 1
        self.last_ok = time.time()
        self.lat_ms = lat_ms
        self.score = min(1.0, self.score * 0.6 + 0.4)   # reward
        self.parked_until = 0.0

    def mark_fail(self, burn: bool = False):
        self.fails += 1
        self.score = max(0.0, self.score * 0.4)         # punish
        if burn:
            self.parked_until = time.time() + LANE_COOLDOWN_SEC


class Pool:
    """Thread-safe-ish (async) collection of lanes + candidate reservoir."""

    def __init__(self):
        self.lanes: dict[str, Lane] = {}      # addr -> Lane (confirmed)
        self.candidates: dict[str, float] = {}  # "proto://addr" -> first-seen
        self.priority_candidates: dict[str, float] = {}  # "proto://addr" -> first-seen (priority)
        self.tried: dict[str, float] = {}     # candidate -> last-fail ts
        self.lock = asyncio.Lock()
        self.sources_ok = 0
        self.last_fetch = 0.0

    def warm_lanes(self) -> list[Lane]:
        now = time.time()
        warm = [ln for ln in self.lanes.values() if ln.parked_until < now and ln.score > 0.05]
        # rank by effective latency (unknown latency = middling), then score.
        # a lane that answered in 5s beats one that took 34s at equal score.
        warm.sort(key=lambda ln: (ln.lat_ms if ln.lat_ms else 8000, -ln.score))
        return warm

    def parked_lanes(self) -> list[Lane]:
        now = time.time()
        return [ln for ln in self.lanes.values() if ln.parked_until >= now]

    def stats(self) -> dict:
        warm = self.warm_lanes()
        return {
            "warm": len(warm),
            "parked": len(self.parked_lanes()),
            "queue": len(self.candidates) + len(self.priority_candidates),
            "sources_ok": self.sources_ok,
            "best_latency_ms": warm[0].lat_ms if warm else None,
        }


POOL = Pool()

# request counters (dashboard)
STATS = {"requests": 0, "failovers": 0, "lane_failures": 0,
         "probes_ok": 0, "probes_burned": 0, "candidates_tested": 0,
         "started": time.time()}

# ── proxy sources ─────────────────────────────────────────────────
# Each entry: (url, kind). kind "text" = ip:port lines (assumed proto).
# kind "proto" = protocol://ip:port lines. kind "geonode" = JSON api.
TEXT_SOURCES: list[tuple[str, str]] = [
    ("https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&proxy_format=ipport&format=text&timeout=8000", "http"),
    ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", "http"),
    ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt", "socks4"),
    ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt", "socks5"),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", "http"),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt", "socks4"),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "socks5"),
    ("https://www.proxy-list.download/api/v1/get?type=http", "http"),
    ("https://www.proxy-list.download/api/v1/get?type=https", "https"),
    ("https://www.proxy-list.download/api/v1/get?type=socks4", "socks4"),
    ("https://www.proxy-list.download/api/v1/get?type=socks5", "socks5"),
    ("https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt", "http"),
    ("https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks4/data.txt", "socks4"),
    ("https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt", "socks5"),
    ("https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt", "http"),
    ("https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt", "socks5"),
    ("https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "http"),
    ("https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt", "proto"),
    ("https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&proxy_format=protocolipport&format=text", "proto"),
]

GEONODE = "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps%2Csocks4%2Csocks5"

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}


def _valid_addr(addr: str) -> bool:
    parts = addr.split(":")
    if len(parts) != 2:
        return False
    try:
        port = int(parts[1])
    except ValueError:
        return False
    if not (0 < port < 65536):
        return False
    octs = parts[0].split(".")
    if len(octs) != 4:
        return False
    try:
        return all(0 <= int(o) <= 255 for o in octs)
    except ValueError:
        return False


async def _fetch_sources() -> None:
    """Pull all feeds, fill the candidate reservoir with proto://addr entries."""
    now = time.time()
    if now - POOL.last_fetch < 30:
        return
    POOL.last_fetch = now
    log.info("Scraper: Scanning 20+ proxy lists & Webshare API accounts...")
    added = 0
    ok = 0
    async with httpx.AsyncClient(timeout=25, headers=UA, follow_redirects=True) as c:
        async def one(url: str, kind: str):
            nonlocal added, ok
            try:
                r = await c.get(url)
                if r.status_code != 200:
                    return
                ok += 1
                for line in r.text.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    proto, addr = kind, line
                    if "://" in line:
                        proto, addr = line.split("://", 1)
                    proto = proto.lower()
                    if proto not in ("http", "https", "socks4", "socks5"):
                        proto = "http"
                    if proto.startswith("socks") and not ALLOW_SOCKS:
                        continue
                    addr = addr.strip().rstrip("/")
                    if not _valid_addr(addr):
                        continue
                    key = f"{proto}://{addr}"
                    if addr in POOL.lanes or key in POOL.candidates:
                        continue
                    if len(POOL.candidates) >= MAX_CANDIDATES:
                        break
                    POOL.candidates[key] = now
                    added += 1
            except Exception:
                pass

        # geonode JSON
        async def geonode():
            nonlocal added, ok
            try:
                r = await c.get(GEONODE)
                if r.status_code != 200:
                    return
                ok += 1
                for e in r.json().get("data", []):
                    addr = f"{e.get('ip')}:{e.get('port')}"
                    protos = [p.lower() for p in e.get("protocols", ["http"])]
                    for proto in protos:
                        if proto.startswith("socks") and not ALLOW_SOCKS:
                            continue
                        if proto not in ("http", "https", "socks4", "socks5"):
                            proto = "http"
                        if not _valid_addr(addr):
                            continue
                        key = f"{proto}://{addr}"
                        if addr in POOL.lanes or key in POOL.candidates:
                            continue
                        if len(POOL.candidates) >= MAX_CANDIDATES:
                            break
                        POOL.candidates[key] = now
                        added += 1
            except Exception:
                pass

        # webshare free-tier API (supports single or comma-separated tokens)
        async def webshare():
            nonlocal added, ok
            if not WEBSHARE_TOKEN:
                return
            # Split tokens by comma, semicolon, newline, or whitespace to support multiple accounts/keys
            import re
            tokens = [t.strip() for t in re.split(r"[\s,;\n\r]+", WEBSHARE_TOKEN) if t.strip()]
            
            async def fetch_one_token(token):
                nonlocal added, ok
                try:
                    r = await c.get(
                        "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=100",
                        headers={"Authorization": f"Token {token}"})
                    if r.status_code != 200:
                        return
                    ok += 1
                    for e in r.json().get("results", []):
                        if not e.get("valid"):
                            continue
                        user = e.get("username")
                        pw = e.get("password")
                        ip = e.get("proxy_address")
                        port = e.get("port")
                        if user and pw:
                            addr = f"{user}:{pw}@{ip}:{port}"
                        else:
                            addr = f"{ip}:{port}"
                        key = f"http://{addr}"
                        if ip in POOL.lanes or addr in POOL.lanes or key in POOL.candidates or key in POOL.priority_candidates:
                            continue
                        POOL.priority_candidates[key] = now
                        added += 1
                except Exception:
                    pass

            await asyncio.gather(*[fetch_one_token(tok) for tok in tokens])

        await asyncio.gather(*[one(u, k) for u, k in TEXT_SOURCES], geonode(), webshare())
    POOL.sources_ok = ok
    if added:
        log.info("sources: +%d candidates (queue=%d, sources_ok=%d)", added, len(POOL.candidates), ok)


# ── probing ───────────────────────────────────────────────────────
# Cheap screen first (TCP connect via the proxy to a tiny http endpoint, no
# upstream quota spent), then a real 1-token upstream probe to confirm the IP
# isn't already quota-burned.
_SCREEN_URL = "http://clients3.google.com/generate_204"


async def _screen(c: httpx.AsyncClient) -> bool:
    try:
        r = await c.get(_SCREEN_URL, timeout=8)
        return r.status_code in (200, 204)
    except Exception:
        return False


async def _probe_candidate(key: str) -> Lane | None:
    proto, addr = key.split("://", 1)
    proxy_url = f"{'http' if proto in ('http','https') else proto}://{addr}"
    try:
        async with httpx.AsyncClient(proxy=proxy_url, timeout=PROBE_TIMEOUT, verify=False) as c:
            t0 = time.time()
            r = await c.post(
                f"{UPSTREAM_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {UPSTREAM_API_KEY}", "Content-Type": "application/json"},
                json={"model": PROBE_MODEL, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 1},
            )
            lat = (time.time() - t0) * 1000
            if r.status_code == 200 and _looks_like_completion(r.content):
                # reject lanes too slow to ever serve a real request
                if lat > RELAY_PROXY_TIMEOUT * 1000 * 0.9:
                    ln = Lane(addr, proto)
                    ln.mark_fail(burn=True)
                    ln.last_probe = time.time()
                    return ln
                ln = Lane(addr, proto)
                ln.mark_ok(lat)
                log.info("Prober: +Lane %s passed completion test (%dms)", addr, int(lat))
                STATS["probes_ok"] += 1
                return ln
            if r.status_code == 429 and is_quota_429(r.content, 429):
                # burned IP — still keep it, parked; it recovers when quota resets
                ln = Lane(addr, proto)
                ln.mark_fail(burn=True)
                ln.last_probe = time.time()
                STATS["probes_burned"] += 1
                return ln
    except Exception:
        pass
    return None


async def _churn_batch() -> None:
    """Test a batch of candidates, promoting the good into the pool."""
    if not POOL.candidates and not POOL.priority_candidates:
        return
    
    batch_size = TEST_CONCURRENCY * 8
    batch = []
    
    # 1. Pop from priority queue first
    while POOL.priority_candidates and len(batch) < batch_size:
        k = next(iter(POOL.priority_candidates.keys()))
        POOL.priority_candidates.pop(k, None)
        batch.append(k)
        
    # 2. Fill remainder from standard queue
    while POOL.candidates and len(batch) < batch_size:
        k = next(iter(POOL.candidates.keys()))
        POOL.candidates.pop(k, None)
        batch.append(k)
        
    if not batch:
        return
        
    STATS["candidates_tested"] += len(batch)
    log.info("Prober: Testing %d candidate proxies (concurrency=%d)...", len(batch), TEST_CONCURRENCY)
    sem = asyncio.Semaphore(TEST_CONCURRENCY)

    async def guarded(k):
        async with sem:
            return await _probe_candidate(k)

    results = await asyncio.gather(*[guarded(k) for k in batch], return_exceptions=True)
    new = 0
    for k, res in zip(batch, results):
        if isinstance(res, Lane):
            POOL.lanes[res.addr] = res
            new += 1
        else:
            POOL.tried[k] = time.time()
    if new:
        log.info("churn: tested %d -> +%d lanes (warm=%d parked=%d queue=%d)",
                 len(batch), new, len(POOL.warm_lanes()), len(POOL.parked_lanes()), len(POOL.candidates))


async def _recover_parked() -> None:
    """Re-probe parked (burned) lanes whose cooldown elapsed — quota windows
    reset, so a burned IP often becomes usable again. This is the recovery
    loop that makes the pool self-healing instead of one-shot."""
    now = time.time()
    due = [ln for ln in POOL.parked_lanes() if now - ln.last_probe > LANE_RECOVER_SEC]
    if not due:
        return
    sem = asyncio.Semaphore(max(4, TEST_CONCURRENCY // 4))

    async def recheck(ln: Lane):
        async with sem:
            proxy_url = ln.url()
            try:
                async with httpx.AsyncClient(proxy=proxy_url, timeout=PROBE_TIMEOUT, verify=False) as c:
                    t0 = time.time()
                    r = await c.post(
                        f"{UPSTREAM_BASE_URL}/chat/completions",
                        headers={"Authorization": f"Bearer {UPSTREAM_API_KEY}", "Content-Type": "application/json"},
                        json={"model": PROBE_MODEL, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 1},
                    )
                    ln.last_probe = now
                    if r.status_code == 200 and _looks_like_completion(r.content):
                        ln.mark_ok((time.time() - t0) * 1000)
                        log.info("lane recovered: %s", ln.addr)
            except Exception:
                ln.last_probe = now

    await asyncio.gather(*[recheck(ln) for ln in due[:20]])


async def _drop_dead() -> None:
    """Prune lanes that have never succeeded and have exhausted retries, and
    stale tried markers, so state doesn't grow forever."""
    now = time.time()
    dead = [a for a, ln in POOL.lanes.items()
            if ln.ok == 0 and ln.probe_tries > 2 and now - ln.last_probe > 1800]
    for a in dead:
        POOL.lanes.pop(a, None)
    stale = [k for k, ts in POOL.tried.items() if now - ts > 3600]
    for k in stale:
        POOL.tried.pop(k, None)


async def pool_manager() -> None:
    """Background loop: keep the pool at target by fetching sources and
    churning candidates; recover parked lanes; prune dead state."""
    while True:
        try:
            warm = len(POOL.warm_lanes())
            if warm < POOL_TARGET:
                if not POOL.candidates:
                    await _fetch_sources()
                await _churn_batch()
            else:
                await _recover_parked()
                await _drop_dead()
                # slow re-fetch so fresh proxies keep flowing in
                if not POOL.candidates:
                    await _fetch_sources()
        except Exception as e:
            log.warning("pool manager error: %s", e)
        await asyncio.sleep(6)


# ── relay core ────────────────────────────────────────────────────

def is_quota_429(body: bytes, status: int) -> bool:
    """True when the response signals rate-limiting / quota exhaustion.

    Provider-agnostic: any HTTP 429 counts, plus generic rate-limit markers
    found in the response body (works for opencode, Groq, SambaNova, Together,
    DeepSeek, and any OpenAI-compatible gateway)."""
    if status != 429:
        return False
    if not body:
        return True
    try:
        data = json.loads(body)
        err = data.get("error", {})
        msg = str(err.get("message", ""))
        err_type = str(err.get("type", ""))
        markers = ("rate limit", "quota", "usage limit", "exhausted", "too many requests",
                   "429", "limit reached", "ratelimit", "rate_limit")
        return any(m in msg.lower() for m in markers) or any(m in err_type.lower() for m in markers)
    except Exception:
        # Unparseable body on a 429 is still a rate-limit signal
        return True


async def _attempt(lane: Lane, payload: dict, path: str, headers: dict, timeout: float):
    """Single upstream attempt through one lane. Returns (status, resp_headers, body).

    A 200 is only accepted if the body actually looks like an upstream chat
    completion — malicious/echo proxies can reflect our own request back with
    a 200, which must be treated as a lane failure, not a success."""
    proxy_url = lane.url()
    kwargs: dict = {"timeout": httpx.Timeout(timeout, connect=12), "verify": False}
    if proxy_url:
        kwargs["proxy"] = proxy_url
    t0 = time.time()
    async with httpx.AsyncClient(**kwargs) as client:
        resp = await client.post(f"{UPSTREAM_BASE_URL}/{path}", headers=headers, json=payload)
        body = resp.content
        lat = (time.time() - t0) * 1000
        if resp.status_code == 200:
            if _looks_like_completion(body):
                lane.mark_ok(lat)
            else:
                # 200 but garbage — echo proxy / captive portal / MITM. Burn it.
                lane.mark_fail(burn=True)
                log.info("lane %s returned 200 with non-completion body — burned", lane.addr)
                return 502, {"content-type": "application/json"}, json.dumps(
                    {"error": {"message": "lane returned invalid body", "type": "lane_invalid"}}).encode()
        elif resp.status_code == 429 and is_quota_429(body, 429):
            lane.mark_fail(burn=True)
        return resp.status_code, dict(resp.headers), body


def _looks_like_completion(body: bytes) -> bool:
    """True if the body parses as a chat completion (or an SSE stream of one)."""
    try:
        d = json.loads(body)
        return "choices" in d or "id" in d
    except Exception:
        pass
    # SSE stream?
    head = body[:200].decode("utf-8", "ignore")
    return head.startswith("data:") or "chat.completion" in head


async def relay(payload: dict, path: str, stream: bool, headers: dict, timeout: float):
    """Try lanes in score order until one answers. Transparent failover: a
    burned/dead lane burns, the request silently moves to the next lane."""
    attempts = max(1, RELAY_ATTEMPTS)
    last_err: tuple | None = None
    tried_addrs: set[str] = set()

    for _ in range(attempts):
        lanes = [ln for ln in POOL.warm_lanes() if ln.addr not in tried_addrs]
        if not lanes:
            break
        lane = lanes[0]
        tried_addrs.add(lane.addr)
        try:
            status, resp_headers, body = await _attempt(lane, payload, path, headers, min(timeout, RELAY_PROXY_TIMEOUT))
            if status == 200:
                return status, resp_headers, body
            if status == 429 and is_quota_429(body, 429):
                STATS["failovers"] += 1
                last_err = (429, body)
                continue
            # invalid-body 502 from a burned lane: fail over, don't surface
            if status == 502:
                try:
                    if json.loads(body).get("error", {}).get("type") == "lane_invalid":
                        STATS["failovers"] += 1
                        last_err = (502, body)
                        continue
                except Exception:
                    pass
            # other upstream error (400/500): return as-is, not a proxy problem
            return status, resp_headers, body
        except Exception as e:
            lane.mark_fail()
            STATS["lane_failures"] += 1
            STATS["failovers"] += 1
            last_err = ("error", str(e).encode())
            continue

    if last_err and last_err[0] == 429:
        return 429, {"content-type": "application/json"}, last_err[1]
    return 503, {"content-type": "application/json"}, json.dumps(
        {"error": {"message": "all egress lanes busy or failed — pool is refilling, retry shortly", "type": "rotator_exhausted"}}
    ).encode()


async def safe_aiter(body: bytes, chunk: int = 4096):
    """Replay buffered upstream bytes as an SSE stream for the client."""
    for i in range(0, len(body), chunk):
        yield body[i:i + chunk]


def build_upstream_headers(auth: str | None, request: Request) -> dict:
    return {
        "Authorization": f"Bearer {UPSTREAM_API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": request.headers.get("user-agent", "ip-relay"),
    }


def _check_relay_auth(request: Request) -> bool:
    if not RELAY_API_KEY:
        return True
    auth = request.headers.get("authorization", "")
    return auth == f"Bearer {RELAY_API_KEY}" or auth == RELAY_API_KEY


# ── model resolution (Claude Code tier aliases -> real upstream model) ──
_FALLBACK_MODEL: str | None = None
_FALLBACK_MODEL_AT: float = 0.0
FALLBACK_MODEL_SEC = 300

MODELS_CACHE_SEC = 3600
_models_cache: dict = {"updated": 0.0, "status": 503, "body": b"", "content_type": "application/json"}


async def get_models_cached() -> tuple[int, str, bytes]:
    if time.time() - _models_cache["updated"] < MODELS_CACHE_SEC and _models_cache["body"]:
        return _models_cache["status"], _models_cache["content_type"], _models_cache["body"]
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20, connect=10)) as client:
            resp = await client.get(f"{UPSTREAM_BASE_URL}/models", headers={"Authorization": f"Bearer {UPSTREAM_API_KEY}"})
            _models_cache.update({"updated": time.time(), "status": resp.status_code,
                                  "body": resp.content, "content_type": resp.headers.get("content-type", "application/json")})
            return resp.status_code, _models_cache["content_type"], resp.content
    except Exception as e:
        log.warning("models fetch failed: %s", e)
        if _models_cache["body"]:
            return _models_cache["status"], _models_cache["content_type"], _models_cache["body"]
        return 503, "application/json", json.dumps({"error": {"message": "models fetch failed", "type": "models_unavailable"}}).encode()


async def fallback_model() -> str:
    global _FALLBACK_MODEL, _FALLBACK_MODEL_AT
    if _FALLBACK_MODEL and time.time() - _FALLBACK_MODEL_AT < FALLBACK_MODEL_SEC:
        return _FALLBACK_MODEL
    chosen = PROBE_MODEL
    try:
        status, _, body = await get_models_cached()
        if status == 200:
            ids = [m.get("id", "") for m in json.loads(body).get("data", [])]
            if ids:
                # Prefer a free/flash/lightweight model if one exists, else the first
                chosen = next((i for i in ids if "-free" in i or "flash" in i or "mini" in i), ids[0])
    except Exception:
        pass
    _FALLBACK_MODEL = chosen
    _FALLBACK_MODEL_AT = time.time()
    return chosen


_CLAUDE_TIER_HINTS = ("claude-", "haiku", "sonnet", "opus")


def strip_model_prefix(model: str) -> str:
    return model.split("/", 1)[-1]


async def resolve_model(model: str) -> str:
    """Map a client-requested model to one the configured upstream key can serve.

    Provider-agnostic: if the requested model exists upstream, pass it through.
    If it doesn't (wrong tier, alias, or a provider that serves different ids),
    resolve to the configured PROBE_MODEL or a sensible free model from the
    upstream /models list. This makes the relay work with any OpenAI-compatible
    provider — not just opencode's free tier."""
    name = strip_model_prefix(model)
    if name:
        try:
            status, _, body = await get_models_cached()
            if status == 200:
                ids = {m.get("id", "") for m in json.loads(body).get("data", [])}
                if name in ids:
                    return name
        except Exception:
            pass
    return await fallback_model()


# ══════════════════════════════════════════════════════════════════
# Anthropic (/v1/messages) translation — proven layer, ported verbatim
# ══════════════════════════════════════════════════════════════════

def anthropic_to_openai(payload: dict) -> dict:
    msgs: list[dict] = []
    system = payload.get("system")
    if system:
        text = system if isinstance(system, str) else " ".join(
            b.get("text", "") for b in system if isinstance(b, dict))
        if text.strip():
            msgs.append({"role": "system", "content": text})

    for m in payload.get("messages", []):
        role = m.get("role", "user")
        content = m.get("content")
        if isinstance(content, str):
            msgs.append({"role": role, "content": content})
            continue
        if not isinstance(content, list):
            msgs.append({"role": role, "content": str(content or "")})
            continue
        if role == "assistant":
            text_parts: list[str] = []
            tool_calls: list[dict] = []
            for b in content:
                if not isinstance(b, dict):
                    continue
                if b.get("type") == "text":
                    text_parts.append(b.get("text", ""))
                elif b.get("type") == "tool_use":
                    tool_calls.append({
                        "id": b.get("id", "call_" + uuid.uuid4().hex[:24]),
                        "type": "function",
                        "function": {"name": b.get("name", ""), "arguments": json.dumps(b.get("input", {}))},
                    })
            msg: dict = {"role": "assistant", "content": "\n".join(text_parts) or None}
            if tool_calls:
                msg["tool_calls"] = tool_calls
            msgs.append(msg)
        else:
            pending_text: list[str] = []
            for b in content:
                if not isinstance(b, dict):
                    continue
                btype = b.get("type")
                if btype == "text":
                    pending_text.append(b.get("text", ""))
                elif btype == "tool_result":
                    if pending_text:
                        msgs.append({"role": "user", "content": "\n".join(pending_text)})
                        pending_text = []
                    tres = b.get("content")
                    if isinstance(tres, list):
                        tres = "\n".join(x.get("text", "") for x in tres if isinstance(x, dict))
                    msgs.append({"role": "tool", "tool_call_id": b.get("tool_use_id", ""),
                                 "content": str(tres if tres is not None else "")})
            if pending_text:
                msgs.append({"role": "user", "content": "\n".join(pending_text)})

    out: dict = {
        "model": strip_model_prefix(str(payload.get("model", ""))),
        "messages": msgs,
        "stream": bool(payload.get("stream", False)),
    }
    if payload.get("max_tokens"):
        out["max_tokens"] = payload["max_tokens"]
    if payload.get("temperature") is not None:
        out["temperature"] = payload["temperature"]
    if payload.get("top_p") is not None:
        out["top_p"] = payload["top_p"]
    tools = payload.get("tools")
    if tools:
        out["tools"] = [
            {"type": "function", "function": {
                "name": t.get("name", ""), "description": t.get("description", ""),
                "parameters": t.get("input_schema", {"type": "object", "properties": {}})}}
            for t in tools if isinstance(t, dict)
        ]
    tc = payload.get("tool_choice")
    if isinstance(tc, dict):
        tct = tc.get("type")
        if tct == "auto":
            out["tool_choice"] = "auto"
        elif tct in ("any", "required"):
            out["tool_choice"] = "required"
        elif tct == "tool" and tc.get("name"):
            out["tool_choice"] = {"type": "function", "function": {"name": tc["name"]}}
    elif isinstance(tc, str):
        out["tool_choice"] = tc
    return out


_ANTHROPIC_STOP = {"end_turn": "end_turn", "tool_use": "tool_use", "max_tokens": "max_tokens",
                   "stop_sequence": "stop_sequence", "length": "max_tokens", "stop": "end_turn",
                   "tool_calls": "tool_use", "content_filter": "end_turn"}


def _oai_finish_to_anthropic(finish: str | None) -> str:
    return _ANTHROPIC_STOP.get(finish or "", "end_turn")


def openai_to_anthropic(body: bytes, model: str) -> bytes:
    try:
        d = json.loads(body)
        choice = d["choices"][0]
        ch = choice["message"]
        content_blocks: list[dict] = []
        text = ch.get("content") or ""
        if text.strip():
            content_blocks.append({"type": "text", "text": text})
        for tc in ch.get("tool_calls") or []:
            fn = tc.get("function", {})
            try:
                inp = json.loads(fn.get("arguments", "{}"))
            except Exception:
                inp = {}
            content_blocks.append({
                "type": "tool_use", "id": tc.get("id", "toolu_" + uuid.uuid4().hex[:24]),
                "name": fn.get("name", ""), "input": inp,
            })
        if not content_blocks:
            content_blocks = [{"type": "text", "text": ch.get("reasoning_content") or ""}]
        usage = d.get("usage", {})
        return json.dumps({
            "id": d.get("id", "msg_" + uuid.uuid4().hex[:24]), "type": "message",
            "role": "assistant", "model": model, "content": content_blocks,
            "stop_reason": _oai_finish_to_anthropic(choice.get("finish_reason")),
            "usage": {"input_tokens": usage.get("prompt_tokens", 0),
                      "output_tokens": usage.get("completion_tokens", 0)},
        }).encode()
    except Exception:
        return body


def openai_sse_to_anthropic(body: bytes, model: str) -> bytes:
    out: list[str] = []
    msg_id = "msg_" + uuid.uuid4().hex[:24]

    def ev(name: str, data: dict) -> str:
        return f"event: {name}\ndata: {json.dumps(data)}\n"

    out.append(ev("message_start", {"type": "message_start", "message": {
        "id": msg_id, "type": "message", "role": "assistant", "model": model,
        "content": [], "stop_reason": None, "usage": {"input_tokens": 0, "output_tokens": 0}}}))

    text_buf = ""
    tool_calls: dict[int, dict] = {}
    finish = "end_turn"
    for raw in body.decode("utf-8", "ignore").splitlines():
        if not raw.startswith("data:"):
            continue
        data = raw[5:].strip()
        if data == "[DONE]":
            break
        try:
            c = json.loads(data)
            choice = c["choices"][0]
            if choice.get("finish_reason"):
                finish = choice["finish_reason"]
            delta = choice.get("delta", {})
            piece = delta.get("content")
            if piece:
                text_buf += piece
            for tc in delta.get("tool_calls") or []:
                idx = tc.get("index", 0)
                slot = tool_calls.setdefault(idx, {"id": "", "name": "", "args": ""})
                if tc.get("id"):
                    slot["id"] = tc["id"]
                fn = tc.get("function", {})
                if fn.get("name"):
                    slot["name"] = fn["name"]
                if fn.get("arguments"):
                    slot["args"] += fn["arguments"]
        except Exception:
            continue

    block_index = 0
    if text_buf:
        out.append(ev("content_block_start", {"type": "content_block_start", "index": block_index,
                                              "content_block": {"type": "text", "text": ""}}))
        out.append(ev("content_block_delta", {"type": "content_block_delta", "index": block_index,
                                              "delta": {"type": "text_delta", "text": text_buf}}))
        out.append(ev("content_block_stop", {"type": "content_block_stop", "index": block_index}))
        block_index += 1
    for idx in sorted(tool_calls):
        slot = tool_calls[idx]
        tid = slot["id"] or ("toolu_" + uuid.uuid4().hex[:24])
        out.append(ev("content_block_start", {"type": "content_block_start", "index": block_index,
                                              "content_block": {"type": "tool_use", "id": tid,
                                                                "name": slot["name"], "input": {}}}))
        if slot["args"]:
            out.append(ev("content_block_delta", {"type": "content_block_delta", "index": block_index,
                                                  "delta": {"type": "input_json_delta",
                                                            "partial_json": slot["args"]}}))
        out.append(ev("content_block_stop", {"type": "content_block_stop", "index": block_index}))
        block_index += 1
    out.append(ev("message_delta", {"type": "message_delta",
                                    "delta": {"stop_reason": _oai_finish_to_anthropic(finish)},
                                    "usage": {"output_tokens": 0}}))
    out.append("event: message_stop\ndata: {\"type\":\"message_stop\"}\n")
    return "\n".join(out).encode()


# ══════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    load_settings()
    asyncio.create_task(pool_manager())
    asyncio.create_task(get_models_cached())


@app.get("/healthz")
async def healthz():
    s = POOL.stats()
    return {"ok": s["warm"] > 0, "version": VERSION, **s}


@app.get("/api/stats")
async def api_stats(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    s = POOL.stats()
    return {"version": VERSION, "uptime_sec": int(time.time() - STATS["started"]),
            "pool": s, "stats": STATS, "settings": public_settings()}


@app.get("/api/pool")
async def api_pool(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    now = time.time()
    
    def display_addr(a: str) -> str:
        return a.split("@", 1)[-1] if "@" in a else a
        
    warm = [{"addr": display_addr(ln.addr), "proto": ln.proto, "score": round(ln.score, 3),
             "lat_ms": round(ln.lat_ms), "ok": ln.ok, "fails": ln.fails}
            for ln in POOL.warm_lanes()[:50]]
    parked = [{"addr": display_addr(ln.addr), "proto": ln.proto, "until_in": int(ln.parked_until - now)}
              for ln in POOL.parked_lanes()[:50]]
    return {"warm": warm, "parked": parked, "queue": len(POOL.candidates)}


@app.get("/api/settings")
async def get_settings(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return public_settings()


@app.post("/api/settings")
async def post_settings(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)
    
    updates = {}
    webshare_changed = False
    for k, v in body.items():
        if k in DEFAULTS:
            # If the input looks masked (contains '...' or is '(none)'),
            # ignore it so we don't overwrite the actual key on disk.
            if k in ("upstream_api_key", "relay_api_key", "webshare_token") and isinstance(v, str):
                if "..." in v or v == "(none)":
                    continue
            if k == "webshare_token" and v != settings.get("webshare_token"):
                webshare_changed = True
            updates[k] = v
            
    res = apply_settings(updates)
    
    if webshare_changed:
        log.info("Settings: Webshare tokens updated — resetting scraper interval to force immediate proxy check.")
        POOL.last_fetch = 0.0
        # Immediately kick off scraping & batch churning in the background
        asyncio.create_task(_fetch_sources())
        
    return res


@app.post("/api/keys/validate")
async def api_validate_keys(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)
    
    # 1. Validate Webshare Tokens
    webshare_tokens = body.get("webshare_token", "").strip()
    import re
    orig_token = str(settings.get("webshare_token", ""))
    orig_tokens = [o.strip() for o in re.split(r"[\s,;\n\r]+", orig_token) if o.strip()]
    
    if not webshare_tokens:
        tokens = list(orig_tokens)
    else:
        tokens = [t.strip() for t in re.split(r"[\s,;\n\r]+", webshare_tokens) if t.strip()]
        
    resolved_tokens = []
    for t in tokens:
        if "..." in t or t == "(none)":
            matched = False
            for o in orig_tokens:
                if o.startswith(t.split("...")[0]):
                    resolved_tokens.append(o)
                    matched = True
                    break
            if not matched and orig_tokens:
                resolved_tokens.append(orig_tokens[0])
        else:
            resolved_tokens.append(t)
            
    webshare_results = await asyncio.gather(*[validate_webshare_token(tok) for tok in resolved_tokens])
    
    # 2. Validate Upstream URL & Key
    upstream_url = body.get("upstream_base_url", "").strip() or UPSTREAM_BASE_URL
    upstream_key = body.get("upstream_api_key", "").strip()
    if not upstream_key or "..." in upstream_key or upstream_key == "(none)":
        upstream_key = str(settings.get("upstream_api_key", ""))
        
    upstream_result = await validate_upstream(upstream_url, upstream_key)
    
    return {
        "webshare": webshare_results,
        "upstream": upstream_result
    }


@app.get("/api/logs")
async def api_logs(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return {"lines": list(LOG_RING)[-200:]}


@app.post("/api/refresh")
async def api_refresh(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    POOL.last_fetch = 0.0
    await _fetch_sources()
    return {"queue": len(POOL.candidates), "sources_ok": POOL.sources_ok}


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
    STATS["requests"] += 1
    payload["model"] = await resolve_model(str(payload.get("model", "")))
    stream = bool(payload.get("stream", False))
    headers = build_upstream_headers(request.headers.get("authorization"), request)
    timeout = 300.0 if stream else 120.0
    status, resp_headers, body = await relay(payload, "chat/completions", stream, headers, timeout)
    if stream and status < 300:
        return StreamingResponse(safe_aiter(body), status_code=status,
                                 media_type=resp_headers.get("content-type", "text/event-stream"))
    return Response(content=body, status_code=status,
                    media_type=resp_headers.get("content-type", "application/json"))


@app.post("/v1/messages")
async def anthropic_messages(request: Request):
    auth = request.headers.get("authorization") or request.headers.get("x-api-key", "")
    if RELAY_API_KEY and auth not in (f"Bearer {RELAY_API_KEY}", RELAY_API_KEY):
        return JSONResponse({"type": "error", "error": {"type": "authentication_error", "message": "invalid relay key"}}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"type": "error", "error": {"type": "invalid_request_error", "message": "invalid json"}}, status_code=400)
    STATS["requests"] += 1
    payload["model"] = await resolve_model(str(payload.get("model", "")))
    oai = anthropic_to_openai(payload)
    stream = oai["stream"]
    headers = build_upstream_headers(request.headers.get("authorization"), request)
    timeout = 300.0 if stream else 120.0
    status, resp_headers, body = await relay(oai, "chat/completions", stream, headers, timeout)
    if status >= 300:
        try:
            msg = json.loads(body).get("error", {}).get("message", "upstream error")
        except Exception:
            msg = "upstream error"
        etype = "rate_limit_error" if status == 429 else "api_error"
        return JSONResponse({"type": "error", "error": {"type": etype, "message": msg}}, status_code=status)
    if stream:
        return StreamingResponse(iter([openai_sse_to_anthropic(body, payload["model"])]), media_type="text/event-stream")
    return Response(content=openai_to_anthropic(body, payload["model"]), media_type="application/json")


DASHBOARD_HTML = open(os.path.join(os.path.dirname(__file__), "dashboard.html")).read() \
    if os.path.exists(os.path.join(os.path.dirname(__file__), "dashboard.html")) else "<h1>dashboard.html missing</h1>"


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML.replace("__VERSION__", VERSION))


@app.get("/login", response_class=HTMLResponse)
async def login():
    return HTMLResponse("<h2>ip-relay</h2><p>Set RELAY_API_KEY off to disable auth, or pass Authorization header.</p>")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
