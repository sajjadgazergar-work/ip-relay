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
#      client never sees a proxy failure. Key-global 429s (FreeUsageLimitError)
#      burn NO lanes — the IPs are fine, the key is flat-out; the relay fails
#      fast and pauses its own probing until the quota window resets.
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
import contextlib
import hmac
import json
import logging
import os
import re
import time
import uuid

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

log = logging.getLogger("ip-relay")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

VERSION = "1.0.0"

# ── settings (env as base, settings.json overlays; UI writes the file) ──
SETTINGS_FILE = os.environ.get("SETTINGS_FILE", "settings.json")
LANES_FILE = os.environ.get("LANES_FILE", "lanes.json")
DEFAULTS = {
    "upstream_base_url": "https://opencode.ai/zen/v1",
    "upstream_api_key": "public",
    "relay_api_key": "",
    "probe_model": "deepseek-v4-flash-free",
    # egress / pool
    "proxy_pool_target": 30,          # how many confirmed lanes to keep warm
    "proxy_max_candidates": 3000,     # cap candidates held in the reservoir
    "proxy_test_concurrency": 60,     # concurrent probes (ceiling when adaptive)
    "proxy_probe_timeout": 25,        # real-probe timeout (slow proxies OK)
    "relay_proxy_timeout": 40,        # per-attempt upstream timeout via proxy
    "relay_attempts": 6,              # lanes tried per client request (failover)
    "lane_cooldown_sec": 3600,        # how long a burned lane is parked
    "lane_recover_sec": 3600,         # parked lane re-probe interval
    "direct_lane": True,              # include the server's own IP as a lane (v0.8.1: on by default)
    "webshare_token": "",             # optional Webshare free-tier API token
    "allow_socks": True,              # use SOCKS4/5 sources (needs httpx[socks])
    # v0.8: capacity + safety
    "lane_max_inflight": 2,           # concurrent requests allowed per lane
    "max_lanes_per_subnet": 3,        # /24 diversity cap (0 = unlimited)
    "adaptive_concurrency": True,     # auto-tune probe concurrency to the link
    "persist_lanes": True,            # remember scored lanes across restarts
    # The upstream free tier gates on User-Agent (see build_upstream_headers).
    # Anything containing "opencode" (case-insensitive) passes; everything else
    # gets an instant 429 regardless of IP or key.
    "upstream_user_agent": "opencode/1.0",
    # ── v1.0: burned-IP memory ─────────────────────────────────────
    # Measured 2026-08-15: a 429'd egress IP does NOT recover on a short timer.
    # A 40-minute continuous poll of one burned IP returned 429 on all ~80
    # attempts, and that IP had been refused for 8 days. So burn is effectively
    # permanent, and the old behaviour (park 90s, re-probe, re-add from the
    # scrape feeds next cycle) spent the entire probe budget re-testing corpses
    # — measured ~200 wasted probes/hour on 10 Webshare IPs alone.
    "burn_memory": True,              # remember burned IPs across scrape cycles
    "burn_ttl_sec": 86400,            # how long an IP stays blocklisted (24h)
    # ── v1.0: Tor as an egress source ──────────────────────────────
    # Tor with IsolateSOCKSAuth gives one circuit (hence one exit IP) per
    # distinct SOCKS username: arbitrarily many lanes, no signup, no cost.
    # Measured against opencode: 14 circuits -> 14 distinct exits; 40 circuits
    # -> 65% live (vs ~2% for scraped public lists); one live exit served 40/40
    # consecutive requests; 1,384 distinct exit IPs available.
    "tor_enabled": False,             # use Tor circuits as lanes
    "tor_lanes": 12,                  # how many isolated circuits to maintain
    "tor_socks_port": 9150,           # SocksPort of a tor with IsolateSOCKSAuth
    # ── v1.0: pin-and-drain ────────────────────────────────────────
    # A live exit survives 40+ requests, so spreading load thin re-pays the
    # ~35% miss cost on every request. Pin a few proven lanes and drain them.
    "lane_pin_count": 3,              # proven lanes kept hot for traffic
}
settings: dict = dict(DEFAULTS)

UPSTREAM_UA: str = str(DEFAULTS["upstream_user_agent"])

UPSTREAM_API_KEYS: list[str] = ["public"]
_upstream_key_idx: int = 0


def get_upstream_key() -> str:
    """Return the next upstream API key in round-robin sequence."""
    global _upstream_key_idx
    if not UPSTREAM_API_KEYS:
        return "public"
    key = UPSTREAM_API_KEYS[_upstream_key_idx % len(UPSTREAM_API_KEYS)]
    _upstream_key_idx += 1
    return key

# Setting bounds enforced at the API boundary (min, max). Anything outside is
# rejected with a clear error instead of silently clamped, so the operator
# learns the constraint instead of wondering why their value did nothing.
SETTING_BOUNDS: dict[str, tuple[int, int]] = {
    "proxy_pool_target": (1, 500),
    "proxy_max_candidates": (100, 100000),
    "proxy_test_concurrency": (1, 500),
    "proxy_probe_timeout": (5, 120),
    "relay_proxy_timeout": (10, 300),
    "relay_attempts": (1, 20),
    "lane_cooldown_sec": (10, 86400),
    "lane_recover_sec": (30, 86400),
    "lane_max_inflight": (1, 32),
    "max_lanes_per_subnet": (0, 100),
    "burn_ttl_sec": (60, 2592000),
    "tor_lanes": (0, 200),
    "tor_socks_port": (1, 65535),
    "lane_pin_count": (1, 50),
}

# Named upstream profiles: base URL + probe model in one pick, so a new user
# does not have to know a provider's quirks to get a working pool.
PROVIDER_PROFILES: dict[str, dict] = {
    "opencode-zen": {
        "label": "OpenCode Zen (free tier)",
        "upstream_base_url": "https://opencode.ai/zen/v1",
        "probe_model": "deepseek-v4-flash-free",
        "upstream_api_key": "public",
        "note": "Per-IP free tier. Key 'public' is shared and often pre-burned.",
    },
    "openrouter-free": {
        "label": "OpenRouter (:free models)",
        "upstream_base_url": "https://openrouter.ai/api/v1",
        "probe_model": "meta-llama/llama-3.3-70b-instruct:free",
        "upstream_api_key": "",
        "note": "Needs your own key; free models are rate-limited per IP + key.",
    },
    "groq": {
        "label": "Groq",
        "upstream_base_url": "https://api.groq.com/openai/v1",
        "probe_model": "llama-3.3-70b-versatile",
        "upstream_api_key": "",
        "note": "Generous free tier, per-key RPM limits.",
    },
    "generic-openai": {
        "label": "Generic OpenAI-compatible",
        "upstream_base_url": "",
        "probe_model": "",
        "upstream_api_key": "",
        "note": "Any /v1 endpoint exposing /models and /chat/completions.",
    },
}

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
LANE_MAX_INFLIGHT = DEFAULTS["lane_max_inflight"]
MAX_LANES_PER_SUBNET = DEFAULTS["max_lanes_per_subnet"]
ADAPTIVE_CONCURRENCY = DEFAULTS["adaptive_concurrency"]
PERSIST_LANES = DEFAULTS["persist_lanes"]
BURN_MEMORY = DEFAULTS["burn_memory"]
BURN_TTL_SEC = DEFAULTS["burn_ttl_sec"]
TOR_ENABLED = DEFAULTS["tor_enabled"]
TOR_LANES = DEFAULTS["tor_lanes"]
TOR_SOCKS_PORT = DEFAULTS["tor_socks_port"]
LANE_PIN_COUNT = DEFAULTS["lane_pin_count"]

# Adaptive probe concurrency. A fixed number is wrong on every link: 60 melts
# an Iranian residential CPE's NAT table (the root cause of the "0 warm
# proxies" reports) while barely loading a datacenter uplink. Start modest,
# grow while the link is clean, halve on a connect-error storm.
ADAPT = {
    "current": 20,        # concurrency in force right now
    "min": 5,
    "last_change": 0.0,
    "last_reason": "init",
    "err_ratio": 0.0,
}


def load_settings() -> None:
    load_dotenv()
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
        "lane_max_inflight": int(os.environ.get("LANE_MAX_INFLIGHT", DEFAULTS["lane_max_inflight"])),
        "max_lanes_per_subnet": int(os.environ.get("MAX_LANES_PER_SUBNET", DEFAULTS["max_lanes_per_subnet"])),
        "adaptive_concurrency": os.environ.get("ADAPTIVE_CONCURRENCY", "1") in ("1", "true", "yes"),
        "persist_lanes": os.environ.get("PERSIST_LANES", "1") in ("1", "true", "yes"),
        "upstream_user_agent": str(os.environ.get("UPSTREAM_USER_AGENT") or DEFAULTS["upstream_user_agent"]).strip(),
        "burn_memory": os.environ.get("BURN_MEMORY", "1") in ("1", "true", "yes"),
        "burn_ttl_sec": int(os.environ.get("BURN_TTL_SEC", DEFAULTS["burn_ttl_sec"])),
        "tor_enabled": os.environ.get("TOR_ENABLED", "0") in ("1", "true", "yes"),
        "tor_lanes": int(os.environ.get("TOR_LANES", DEFAULTS["tor_lanes"])),
        "tor_socks_port": int(os.environ.get("TOR_SOCKS_PORT", DEFAULTS["tor_socks_port"])),
        "lane_pin_count": int(os.environ.get("LANE_PIN_COUNT", DEFAULTS["lane_pin_count"])),
    }
    try:
        with open(SETTINGS_FILE, encoding="utf-8") as f:
            s.update({k: v for k, v in json.load(f).items() if k in DEFAULTS})
    except Exception:
        pass
    apply_settings(s, persist=False)
    # Default-deny the control plane: an empty relay key means every /api route
    # (including POST /api/settings, which can repoint the upstream at an
    # attacker's collector) is world-open the moment the port is reachable.
    # Mint one, persist it, and print it once so the operator can copy it.
    if not RELAY_API_KEY and os.environ.get("RELAY_ALLOW_ANONYMOUS", "0") not in ("1", "true", "yes"):
        generated = "rly_" + uuid.uuid4().hex
        apply_settings({"relay_api_key": generated}, persist=True)
        log.warning("=" * 68)
        log.warning("SECURITY: no relay_api_key was set — generated one for you:")
        log.warning("    %s", generated)
        log.warning("Saved to %s. Use it as the Bearer token from clients and the", SETTINGS_FILE)
        log.warning("dashboard. Set RELAY_ALLOW_ANONYMOUS=1 to run without auth.")
        log.warning("=" * 68)


def save_settings() -> None:
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=2)
    except Exception as e:
        log.warning("could not save settings: %s", e)


def apply_settings(new: dict, persist: bool = True) -> dict:
    global UPSTREAM_API_KEY, UPSTREAM_BASE_URL, RELAY_API_KEY, PROBE_MODEL
    global POOL_TARGET, MAX_CANDIDATES, TEST_CONCURRENCY, PROBE_TIMEOUT
    global RELAY_PROXY_TIMEOUT, RELAY_ATTEMPTS, LANE_COOLDOWN_SEC, LANE_RECOVER_SEC
    global DIRECT_LANE, WEBSHARE_TOKEN, ALLOW_SOCKS
    global LANE_MAX_INFLIGHT, MAX_LANES_PER_SUBNET, ADAPTIVE_CONCURRENCY, PERSIST_LANES
    global UPSTREAM_API_KEYS, UPSTREAM_UA
    global BURN_MEMORY, BURN_TTL_SEC, TOR_ENABLED, TOR_LANES, TOR_SOCKS_PORT, LANE_PIN_COUNT
    old_base, old_key = UPSTREAM_BASE_URL, UPSTREAM_API_KEY
    for k, v in new.items():
        if k in DEFAULTS:
            settings[k] = v
    UPSTREAM_API_KEY = str(settings["upstream_api_key"]).strip()
    raw_keys = [k.strip() for k in UPSTREAM_API_KEY.replace("\n", ",").split(",") if k.strip()]
    UPSTREAM_API_KEYS = raw_keys if raw_keys else ["public"]
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
    LANE_MAX_INFLIGHT = max(1, int(settings["lane_max_inflight"]))
    MAX_LANES_PER_SUBNET = max(0, int(settings["max_lanes_per_subnet"]))
    ADAPTIVE_CONCURRENCY = bool(settings["adaptive_concurrency"])
    PERSIST_LANES = bool(settings["persist_lanes"])
    BURN_MEMORY = bool(settings.get("burn_memory", DEFAULTS["burn_memory"]))
    BURN_TTL_SEC = max(60, int(settings.get("burn_ttl_sec", DEFAULTS["burn_ttl_sec"])))
    TOR_ENABLED = bool(settings.get("tor_enabled", DEFAULTS["tor_enabled"]))
    TOR_LANES = max(0, int(settings.get("tor_lanes", DEFAULTS["tor_lanes"])))
    TOR_SOCKS_PORT = int(settings.get("tor_socks_port", DEFAULTS["tor_socks_port"]))
    LANE_PIN_COUNT = max(1, int(settings.get("lane_pin_count", DEFAULTS["lane_pin_count"])))
    UPSTREAM_UA = str(settings.get("upstream_user_agent") or DEFAULTS["upstream_user_agent"]).strip() \
        or str(DEFAULTS["upstream_user_agent"])
    if "opencode" not in UPSTREAM_UA.lower():
        log.warning("upstream_user_agent %r does not contain 'opencode' — the free tier "
                    "will answer 429 for every request. Falling back to %r.",
                    UPSTREAM_UA, DEFAULTS["upstream_user_agent"])
        UPSTREAM_UA = str(DEFAULTS["upstream_user_agent"])
    if not ADAPTIVE_CONCURRENCY:
        ADAPT["current"] = TEST_CONCURRENCY
    else:
        ADAPT["current"] = min(ADAPT["current"], TEST_CONCURRENCY)
    if UPSTREAM_BASE_URL != old_base or UPSTREAM_API_KEY != old_key:
        _models_cache["updated"] = 0.0
    if persist:
        save_settings()
    return dict(settings)


def validate_settings_payload(body: dict) -> tuple[dict, list[str]]:
    """Split an incoming settings payload into (accepted updates, errors).

    Masked secrets are skipped (never overwrite a real key with 'abc...'),
    numeric fields are bounds-checked, and unknown keys are ignored.
    """
    updates: dict = {}
    errors: list[str] = []
    for k, v in body.items():
        if k not in DEFAULTS:
            continue
        if k in ("upstream_api_key", "relay_api_key", "webshare_token") and isinstance(v, str):
            if "..." in v or v == "(none)":
                continue
        if k in SETTING_BOUNDS:
            lo, hi = SETTING_BOUNDS[k]
            try:
                iv = int(v)
            except (TypeError, ValueError):
                errors.append(f"{k}: must be a number")
                continue
            if not (lo <= iv <= hi):
                errors.append(f"{k}: must be between {lo} and {hi} (got {iv})")
                continue
            updates[k] = iv
            continue
        if isinstance(DEFAULTS[k], bool):
            updates[k] = v if isinstance(v, bool) else str(v).lower() in ("1", "true", "yes", "on")
            continue
        if k == "upstream_base_url" and isinstance(v, str) and v.strip():
            if not v.strip().lower().startswith(("http://", "https://")):
                errors.append("upstream_base_url: must start with http:// or https://")
                continue
        if k == "upstream_user_agent" and isinstance(v, str):
            if not v.strip():
                errors.append("upstream_user_agent: cannot be empty")
                continue
            if "opencode" not in v.lower():
                errors.append("upstream_user_agent: must contain 'opencode' — the free "
                              "tier answers 429 to every other User-Agent")
                continue
        updates[k] = v
    return updates, errors


def mask_key(k: str) -> str:
    return k[:6] + "..." if k else "(none)"


def _display_addr(a: str) -> str:
    """Never surface proxy credentials: 'user:pass@ip:port' -> 'ip:port'.

    Tor lanes would otherwise all render as '127.0.0.1:9150', which is useless
    in the dashboard — every circuit would look like the same lane. Show the
    circuit identity instead, since that IS the distinguishing egress.
    """
    if a == "":
        return "direct"
    if a.startswith(TOR_LANE_PREFIX):
        parsed = tor_slot_of(a)
        return f"tor#{parsed[0]}.g{parsed[1]}" if parsed else "tor"
    return a.split("@", 1)[-1] if "@" in a else a


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
        keys = str(s["upstream_api_key"]).split(",")
        s["upstream_api_key"] = ",".join(mask_key(k) for k in keys)
    if s.get("relay_api_key"):
        s["relay_api_key"] = mask_key(str(s["relay_api_key"]))
    if s.get("webshare_token"):
        tokens = str(s["webshare_token"]).split(",")
        s["webshare_token"] = ",".join(mask_key(tok) for tok in tokens)
    return s


# ── log ring buffer (dashboard log viewer) ──────────────────────
LOG_RING = collections.deque(maxlen=600)

# Proxy credentials (user:pass@host:port) must never reach the log ring:
# /api/logs is reachable by anyone who can reach the dashboard, and when
# relay_api_key is empty that is the whole internet. Strip the userinfo part.
_CRED_RE = re.compile(r"(?<![\w.\-])[A-Za-z0-9_\-.]{1,64}:[^\s/@]{1,64}@(?=[\w.\-]+:\d{2,5})")


def scrub_creds(text: str) -> str:
    return _CRED_RE.sub("", text)


class RingHandler(logging.Handler):
    def emit(self, record):
        try:
            line = scrub_creds(self.format(record))
            LOG_RING.append(line)
            # Push the line to SSE subscribers so the dashboard log is live
            # instead of polled. Guarded: logging can happen from a non-async
            # thread (or before the loop exists), where queue mutation is unsafe.
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return
            if _subscribers:
                publish("log", {"line": line})
        except Exception:
            pass


handler = RingHandler()
handler.setFormatter(logging.Formatter(
    fmt="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
))
logging.getLogger().addHandler(handler)

app = FastAPI(title="ip-relay")


# ══════════════════════════════════════════════════════════════════
# EGRESS ENGINE
# ══════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════
# BURN MEMORY — the blocklist of spent egress IPs
# ══════════════════════════════════════════════════════════════════
#
# Why this exists (measured 2026-08-15, not assumed):
#
#   * A 429'd egress IP does not come back. One IP was polled continuously for
#     40 minutes — ~80 attempts, zero 200s — and had already been refused for 8
#     days. Verified with a live key while other lanes were returning 200, so
#     the key was fine and the IP was what was refused.
#   * The scrape feeds re-serve the same dead addresses every cycle. The 10
#     Webshare free IPs alone were re-probed ~20 times each in one hour: ~200
#     probes spent on addresses that will never work again.
#
# So burn is remembered on disk, keyed on the IP (never ip:port — the quota
# follows the address, and the same host shows up on many ports across feeds),
# and consulted at two choke points: candidate intake and probe dispatch. That
# redirects the probe budget at addresses that could plausibly be live.

BURNED_FILE = os.environ.get("BURNED_FILE", "burned.json")

# ip -> {"at": epoch, "reason": str, "hits": int}
BURNED: dict[str, dict] = {}
BURN_STATS = {"blocked_intake": 0, "blocked_probe": 0, "recorded": 0, "expired": 0}


def _ip_of(addr: str) -> str:
    """Bare IP from 'user:pass@ip:port', 'ip:port', or a synthetic lane addr."""
    if not addr:
        return ""
    return addr.split("@")[-1].split(":")[0]


def is_burned(addr_or_ip: str) -> bool:
    """True when this egress address is a known-spent IP still inside its TTL.

    Tor lanes are exempt: their addr points at the local SOCKS port, so the IP
    would be 127.0.0.1 for every circuit — blocklisting that would kill the
    whole Tor provider on the first 429. Circuit rotation is how a burned Tor
    exit is discarded (see tor_rotate_lane).
    """
    if not BURN_MEMORY or not addr_or_ip:
        return False
    if addr_or_ip.startswith(TOR_LANE_PREFIX):
        return False
    ip = _ip_of(addr_or_ip)
    if not ip or ip in ("127.0.0.1", "localhost"):
        return False
    rec = BURNED.get(ip)
    if not rec:
        return False
    if time.time() - rec.get("at", 0) > BURN_TTL_SEC:
        BURNED.pop(ip, None)
        BURN_STATS["expired"] += 1
        return False
    return True


def mark_burned(addr: str, reason: str = "429") -> None:
    """Remember that this egress IP's free-tier budget is spent."""
    if not BURN_MEMORY or not addr:
        return
    if addr.startswith(TOR_LANE_PREFIX):
        return          # rotate the circuit instead; see tor_rotate_lane
    ip = _ip_of(addr)
    if not ip or ip in ("127.0.0.1", "localhost"):
        return
    rec = BURNED.get(ip)
    if rec:
        rec["hits"] = rec.get("hits", 1) + 1
        rec["at"] = time.time()
        rec["reason"] = reason
    else:
        BURNED[ip] = {"at": time.time(), "reason": reason, "hits": 1}
        BURN_STATS["recorded"] += 1


def prune_burned() -> int:
    """Drop expired entries so the file and the dict stay bounded."""
    now = time.time()
    dead = [ip for ip, r in BURNED.items() if now - r.get("at", 0) > BURN_TTL_SEC]
    for ip in dead:
        BURNED.pop(ip, None)
    BURN_STATS["expired"] += len(dead)
    return len(dead)


def save_burned() -> int:
    if not BURN_MEMORY:
        return 0
    try:
        tmp = BURNED_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"saved": time.time(), "ips": BURNED}, f)
        os.replace(tmp, BURNED_FILE)
        return len(BURNED)
    except Exception as e:
        log.warning("could not save burn list: %s", e)
        return 0


def load_burned() -> int:
    if not BURN_MEMORY:
        return 0
    try:
        with open(BURNED_FILE, encoding="utf-8") as f:
            data = json.load(f)
        ips = data.get("ips", {})
        now = time.time()
        loaded = 0
        for ip, rec in ips.items():
            if not isinstance(rec, dict):
                continue
            if now - float(rec.get("at", 0)) > BURN_TTL_SEC:
                continue
            BURNED[ip] = rec
            loaded += 1
        if loaded:
            log.info("burn memory: %d known-spent IPs loaded (ttl=%ds)", loaded, BURN_TTL_SEC)
        return loaded
    except FileNotFoundError:
        return 0
    except Exception as e:
        log.warning("could not load burn list: %s", e)
        return 0


# ══════════════════════════════════════════════════════════════════
# TOR EGRESS PROVIDER
# ══════════════════════════════════════════════════════════════════
#
# Measured 2026-08-15 against opencode.ai/zen/v1:
#
#   14 isolated circuits -> 14 DISTINCT exit IPs (zero repeats)
#   40 isolated circuits -> 26x200 / 14x429  =  65% live
#                           p50 2128ms, p95 3037ms on success
#   one live exit        -> 40/40 consecutive requests (twice, independently)
#   supply              -> 3,224 exit relays / 1,384 distinct exit IPs
#
# For comparison the scraped public-proxy feeds yield ~2% (16 usable out of
# 1,040 probed). Tor is ~30x better per probe, at equal latency, and its supply
# renews itself instead of churning.
#
# Mechanism: tor's `IsolateSOCKSAuth` binds a circuit to the SOCKS *username*,
# so socks5://<name>:x@127.0.0.1:9150 is a private egress per name. httpx takes
# the proxy per-client, so this needs no new transport code and is safe under
# concurrency (verified: 10 parallel circuits, no cross-talk).
#
# Rotation is the important part: because a burned exit never recovers, a 429 on
# a Tor lane must not park it — it must change the circuit name, which yields a
# brand-new exit IP. That is what makes discovery self-renewing.

TOR_LANE_PREFIX = "tor-"
_tor_rotations = 0


def tor_lane_addr(slot: int, generation: int = 0) -> str:
    """Synthetic lane address for Tor slot `slot` at circuit `generation`.

    The username encodes slot+generation, so bumping the generation forces tor
    to build a fresh circuit (new exit IP) while keeping the slot identity.
    """
    return f"{TOR_LANE_PREFIX}{slot}g{generation}:x@127.0.0.1:{TOR_SOCKS_PORT}"


def tor_slot_of(addr: str) -> tuple[int, int] | None:
    """(slot, generation) from a Tor lane addr, or None if not a Tor lane."""
    if not addr.startswith(TOR_LANE_PREFIX):
        return None
    try:
        name = addr.split(":", 1)[0][len(TOR_LANE_PREFIX):]
        slot_s, gen_s = name.split("g", 1)
        return int(slot_s), int(gen_s)
    except Exception:
        return None


def tor_available() -> bool:
    """Is a tor SOCKS port actually listening? Cheap, synchronous, no traffic."""
    import socket as _socket
    try:
        with _socket.create_connection(("127.0.0.1", TOR_SOCKS_PORT), timeout=1.5):
            return True
    except Exception:
        return False


def tor_rotate_lane(ln: "Lane") -> str | None:
    """Replace a burned Tor lane with a fresh circuit in the same slot.

    Returns the new lane key, or None when `ln` is not a Tor lane. The old lane
    is removed outright rather than parked: its exit is spent, and parking would
    just re-probe a dead address on the recover loop.
    """
    global _tor_rotations
    parsed = tor_slot_of(ln.addr)
    if parsed is None:
        return None
    slot, gen = parsed
    POOL.lanes.pop(f"{ln.proto}://{ln.addr}", None)
    new_addr = tor_lane_addr(slot, gen + 1)
    fresh = Lane(new_addr, "socks5")
    fresh.score = 0.5
    key = f"socks5://{new_addr}"
    POOL.lanes[key] = fresh
    _tor_rotations += 1
    log.info("tor: rotated slot %d to generation %d (new exit circuit)", slot, gen + 1)
    return key


def tor_ensure_lanes() -> int:
    """Keep TOR_LANES circuit slots present in the pool. Idempotent.

    Lanes start unproven (score 0.5) and are confirmed by the normal probe path,
    which is what filters the ~35% of exits whose budget is already spent.
    """
    if not TOR_ENABLED:
        return 0
    if not tor_available():
        return 0
    have = {tor_slot_of(ln.addr)[0] for ln in POOL.lanes.values()
            if ln.addr.startswith(TOR_LANE_PREFIX) and tor_slot_of(ln.addr)}
    added = 0
    for slot in range(max(0, TOR_LANES)):
        if slot in have:
            continue
        addr = tor_lane_addr(slot, 0)
        POOL.lanes[f"socks5://{addr}"] = Lane(addr, "socks5")
        added += 1
    if added:
        log.info("tor: added %d circuit lanes (target=%d, socks=127.0.0.1:%d)",
                 added, TOR_LANES, TOR_SOCKS_PORT)
    return added


class Lane:
    """One egress route (a proxy, or the direct lane). Carries health score.

    score: EWMA in [0,1]. Success pushes toward 1, failure toward 0. Requests
    prefer the highest-scored warm lane. Latency is tracked for ranking."""
    __slots__ = ("addr", "proto", "score", "lat_ms", "ok", "fails",
                 "parked_until", "last_ok", "last_probe", "probe_tries",
                 "consec_fails", "inflight", "_sem", "created")

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
        self.consec_fails = 0       # consecutive failures; >=3 self-parks
        self.inflight = 0           # requests currently in flight on this lane
        self._sem: asyncio.Semaphore | None = None
        self.created = time.time()

    # ── capacity ───────────────────────────────────────────────────
    # Free proxies collapse under parallel load: without a per-lane cap the
    # whole fleet of concurrent requests stacks onto whichever lane is
    # currently fastest and burns it. The semaphore is created lazily so Lane
    # stays constructible outside a running event loop (tests, deserialization).
    @property
    def sem(self) -> asyncio.Semaphore:
        if self._sem is None:
            self._sem = asyncio.Semaphore(max(1, LANE_MAX_INFLIGHT))
        return self._sem

    @property
    def at_capacity(self) -> bool:
        return self.inflight >= max(1, LANE_MAX_INFLIGHT)

    @property
    def subnet(self) -> str:
        """The /24 this lane lives in. Upstream per-IP limits are frequently
        enforced per-subnet, so /24 is the real unit of pool capacity.

        Tor lanes all share 127.0.0.1, but each circuit is an INDEPENDENT exit
        in a different AS — so they must not collapse into one subnet bucket or
        max_lanes_per_subnet would cap the whole Tor provider at 3 lanes. Each
        circuit slot therefore reports its own synthetic subnet.
        """
        if not self.addr:
            return "direct"
        if self.addr.startswith(TOR_LANE_PREFIX):
            parsed = tor_slot_of(self.addr)
            return f"tor-{parsed[0]}" if parsed else "tor"
        host = self.addr.split("@")[-1].split(":")[0]
        parts = host.split(".")
        return ".".join(parts[:3]) if len(parts) == 4 else host

    @property
    def tier(self) -> str:
        """fast / medium / slow — used to route streaming requests to lanes
        where latency is visible to a human."""
        if not self.lat_ms:
            return "medium"
        if self.lat_ms < 1500:
            return "fast"
        return "medium" if self.lat_ms < 4000 else "slow"

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
        self.consec_fails = 0

    def mark_alive(self, lat_ms: float):
        """Reachability-only success (GET /models), NOT a completion.

        This must never touch `ok`. `/models` answers 200 even from an egress IP
        whose completion quota is fully spent — measured — so counting it as a
        success would mark a dead lane "proven" and pin traffic to it (see
        _pick_lane). Refresh liveness and latency, leave the proof alone.
        """
        self.last_probe = time.time()
        self.lat_ms = lat_ms
        self.score = min(1.0, self.score * 0.8 + 0.1)   # mild reward
        self.consec_fails = 0

    def mark_fail(self, burn: bool = False):
        self.fails += 1
        self.consec_fails += 1
        self.score = max(0.0, self.score * 0.4)         # punish
        # Explicit burn OR repeated drops/5xx without 429s (upstream never
        # sends 429) — self-park so broken IPs leave rotation.
        if burn or self.consec_fails >= 3:
            was_warm = self.parked_until < time.time()
            self.parked_until = time.time() + LANE_COOLDOWN_SEC
            if was_warm:
                # transition warm -> parked is the interesting event for the
                # visualizer; repeated failures on an already-parked lane are not
                try:
                    publish("lane_down", {"addr": _display_addr(self.addr), "proto": self.proto,
                                          "subnet": self.subnet,
                                          "cooldown_sec": LANE_COOLDOWN_SEC})
                except Exception:
                    pass

    # ── persistence ────────────────────────────────────────────────
    def to_dict(self) -> dict:
        return {"addr": self.addr, "proto": self.proto, "score": round(self.score, 4),
                "lat_ms": round(self.lat_ms, 1), "ok": self.ok, "fails": self.fails,
                "last_ok": round(self.last_ok, 1)}

    @classmethod
    def from_dict(cls, d: dict) -> "Lane":
        ln = cls(str(d.get("addr", "")), str(d.get("proto", "http")))
        ln.score = float(d.get("score", 0.5))
        ln.lat_ms = float(d.get("lat_ms", 0.0))
        ln.ok = int(d.get("ok", 0))
        ln.fails = int(d.get("fails", 0))
        ln.last_ok = float(d.get("last_ok", 0.0))
        return ln


class Pool:
    """Thread-safe-ish (async) collection of lanes + candidate reservoir."""

    def __init__(self):
        self.lanes: dict[str, Lane] = {}      # "proto://addr" -> Lane (confirmed)
        self.candidates: dict[str, float] = {}  # "proto://addr" -> first-seen
        self.priority_candidates: dict[str, float] = {}  # "proto://addr" -> first-seen (priority)
        self.tried: dict[str, float] = {}     # "proto://addr" candidate -> last-fail ts
        self.sources_ok = 0
        self.last_fetch = 0.0

    def warm_lanes(self) -> list[Lane]:
        now = time.time()
        warm = [ln for ln in self.lanes.values() if ln.parked_until < now and ln.score > 0.05]
        # Rank by effective latency (unknown latency = middling), then score.
        # A lane that answered in 500ms beats one that took 3s at equal score.
        # No provider-specific ordering here: a lane only reaches the pool after
        # passing a real completion probe, so "Webshare last" style heuristics
        # are unnecessary — burned IPs never get promoted in the first place.
        warm.sort(key=lambda ln: (ln.lat_ms if ln.lat_ms else 8000, -ln.score))
        return warm

    def available_lanes(self, tier: str | None = None) -> list[Lane]:
        """Warm lanes that still have capacity, optionally restricted to a
        latency tier. This is what the router should use — warm_lanes() alone
        will happily hand back a lane that already has N requests on it."""
        lanes = [ln for ln in self.warm_lanes() if not ln.at_capacity]
        if tier:
            tiered = [ln for ln in lanes if ln.tier == tier]
            if tiered:
                return tiered
        return lanes

    def parked_lanes(self) -> list[Lane]:
        now = time.time()
        return [ln for ln in self.lanes.values() if ln.parked_until >= now]

    def subnet_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for ln in self.lanes.values():
            if not ln.addr:
                continue
            counts[ln.subnet] = counts.get(ln.subnet, 0) + 1
        return counts

    def subnet_full(self, subnet: str) -> bool:
        if MAX_LANES_PER_SUBNET <= 0 or subnet == "direct":
            return False
        return self.subnet_counts().get(subnet, 0) >= MAX_LANES_PER_SUBNET

    def stats(self) -> dict:
        warm = self.warm_lanes()
        tiers = {"fast": 0, "medium": 0, "slow": 0}
        for ln in warm:
            tiers[ln.tier] = tiers.get(ln.tier, 0) + 1
        return {
            "warm": len(warm),
            "parked": len(self.parked_lanes()),
            "queue": len(self.candidates) + len(self.priority_candidates),
            "sources_ok": self.sources_ok,
            "best_latency_ms": warm[0].lat_ms if warm else None,
            # capacity truth: 30 lanes across 3 subnets behave like 3 lanes
            "subnets": len({ln.subnet for ln in warm if ln.addr}),
            "capacity": len(warm) * max(1, LANE_MAX_INFLIGHT),
            "inflight": sum(ln.inflight for ln in self.lanes.values()),
            "tiers": tiers,
        }


POOL = Pool()

# request counters (dashboard)
STATS = {"requests": 0, "failovers": 0, "lane_failures": 0,
         "probes_ok": 0, "probes_burned": 0, "candidates_tested": 0,
         "streams": 0, "upstream_429s": 0, "started": time.time()}

# Key-global quota tracking. opencode's free tier returns 429
# FreeUsageLimitError when the *key's* budget is exhausted (every egress IP
# 429s at once) — at that point probing is pure quota-burning, so the pool
# manager pauses churn/recover with exponential backoff until the window
# resets. A successful user relay clears the flag immediately.
#
# Important distinction (v0.8.1): most 429s in a public-proxy pool are PER-IP
# (that specific egress IP is burned by upstream, or the proxy is shared with
# thousands of other scrapers that already exhausted it). Only when enough
# DISTINCT lanes 429 within a short window — or the direct lane 429s, which
# proves the key itself is dead from our own egress — do we treat it as
# key-global. Single per-IP 429s park that lane and keep probing.
QUOTA_STATE = {"exhausted": False, "backoff_sec": 90, "backoff_until": 0.0, "announced": False,
               "_429_window": [], "_direct_429s": 0}
# key-global confidence: ≥3 distinct lanes 429ing within 45s, or ≥3 direct 429s
QUOTA_LANE_THRESHOLD = 3
QUOTA_WINDOW_SEC = 45


def _note_lane_429(addr: str) -> None:
    """Record a per-IP 429.

    With the correct opencode UA in place, a 429 means THAT egress IP has spent
    its free-tier budget — it is never a statement about our API key. So the
    normal action is: park the lane, keep serving from the others.

    A key-global escalation is still worth having, because it is the signature
    of a systemic fault (upstream changed the UA gate, or every key is dead):
    when ≥QUOTA_LANE_THRESHOLD *distinct* lanes 429 inside QUOTA_WINDOW_SEC, the
    problem is not the proxies. We back off probing and say so loudly, since
    hammering in that state just burns fresh IPs for nothing.
    """
    now = time.time()
    # Burn memory: this address is spent for the TTL. Measured — it does not
    # recover on a 90s cooldown, so remembering it is what stops the prober
    # re-testing the same corpses every scrape cycle.
    mark_burned(addr, "relay-429")
    w = QUOTA_STATE["_429_window"]
    w.append((now, addr))
    w[:] = [(t, a) for t, a in w if now - t <= QUOTA_WINDOW_SEC]
    distinct = {a for _, a in w}
    if addr == "":
        QUOTA_STATE["_direct_429s"] += 1
        if QUOTA_STATE["_direct_429s"] >= QUOTA_LANE_THRESHOLD:
            _note_upstream_429()
            QUOTA_STATE["_direct_429s"] = 0
        return
    if len(distinct) >= QUOTA_LANE_THRESHOLD:
        # Guard against false escalation: a single client request can burn
        # several lanes in one failover chain, which is normal operation, not a
        # systemic fault. Only escalate when NOTHING has succeeded in the same
        # window — that is the real signature of "upstream is refusing us
        # everywhere" (UA gate changed, or all keys dead).
        cutoff = now - QUOTA_WINDOW_SEC
        recent_ok = any(r[0] >= cutoff and r[2] == 200 for r in REQ_LOG)
        if recent_ok:
            return
        log.warning("quota: %d distinct egress IPs 429'd within %ds with zero successes — "
                    "this is systemic, not per-proxy. Check upstream_user_agent (currently "
                    "%r; it must contain 'opencode') and that the keys are live.",
                    len(distinct), QUOTA_WINDOW_SEC, UPSTREAM_UA)
        _note_upstream_429()
        QUOTA_STATE["_429_window"] = []


def _note_upstream_429() -> None:
    """Record a key-global rate-limit hit and grow the probe backoff."""
    now = time.time()
    QUOTA_STATE["exhausted"] = True
    QUOTA_STATE["backoff_sec"] = min(QUOTA_STATE["backoff_sec"] * 2, 1800)
    QUOTA_STATE["backoff_until"] = now + QUOTA_STATE["backoff_sec"]
    STATS["upstream_429s"] += 1


def _note_quota_ok() -> None:
    """A successful upstream call — quota is back, resume probing.
    v0.8.1: success decays the backoff instead of hard-resetting only on a
    user relay, so a long-lived pool stops fighting a freed quota window."""
    if QUOTA_STATE["exhausted"]:
        QUOTA_STATE["backoff_sec"] = max(30, QUOTA_STATE["backoff_sec"] // 2)
        if QUOTA_STATE["backoff_sec"] == 30:
            log.info("upstream quota recovered (200) — probing resumed")
            QUOTA_STATE["exhausted"] = False
            QUOTA_STATE["backoff_until"] = 0.0
        else:
            QUOTA_STATE["backoff_until"] = min(QUOTA_STATE["backoff_until"],
                                               time.time() + QUOTA_STATE["backoff_sec"])
    if not QUOTA_STATE["exhausted"]:
        QUOTA_STATE["backoff_until"] = 0.0


# ══════════════════════════════════════════════════════════════════
# METRICS — rolling window, so questions like "is p95 worse than an
# hour ago?" are answerable. Lifetime counters alone cannot do that.
# ══════════════════════════════════════════════════════════════════

REQ_LOG: collections.deque = collections.deque(maxlen=2000)  # (ts, lane, status, ms, stream)


def record_request(lane: str, status: int, ms: float, stream: bool = False) -> None:
    REQ_LOG.append((time.time(), lane, status, ms, stream))


def _pct(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


def metrics_window(seconds: int = 300) -> dict:
    cutoff = time.time() - seconds
    rows = [r for r in REQ_LOG if r[0] >= cutoff]
    lat = sorted(r[3] for r in rows if r[2] == 200)
    ok = sum(1 for r in rows if r[2] == 200)
    return {
        "window_sec": seconds,
        "requests": len(rows),
        "ok": ok,
        "errors": len(rows) - ok,
        "success_rate": round(ok / len(rows), 4) if rows else None,
        "rpm": round(len(rows) / (seconds / 60.0), 2),
        "p50_ms": round(_pct(lat, 0.50)),
        "p95_ms": round(_pct(lat, 0.95)),
        "p99_ms": round(_pct(lat, 0.99)),
    }


def sparkline_series(buckets: int = 30, bucket_sec: int = 2) -> dict:
    """Per-bucket request counts and p95 for the dashboard sparklines."""
    now = time.time()
    counts = [0] * buckets
    lats: list[list[float]] = [[] for _ in range(buckets)]
    for ts, _lane, status, ms, _stream in REQ_LOG:
        age = now - ts
        idx = buckets - 1 - int(age // bucket_sec)
        if 0 <= idx < buckets:
            counts[idx] += 1
            if status == 200:
                lats[idx].append(ms)
    p95 = [round(_pct(sorted(b), 0.95)) for b in lats]
    return {"bucket_sec": bucket_sec, "requests": counts, "p95_ms": p95}


# ══════════════════════════════════════════════════════════════════
# EVENT BUS — the dashboard subscribes over SSE instead of polling
# three endpoints on three different timers.
# ══════════════════════════════════════════════════════════════════

_subscribers: set[asyncio.Queue] = set()
EVENT_RING: collections.deque = collections.deque(maxlen=200)


def publish(kind: str, data: dict) -> None:
    """Fan an event out to every live SSE subscriber. Never blocks, never
    raises: a slow/dead client must not be able to stall the relay."""
    evt = {"kind": kind, "ts": time.time(), **data}
    EVENT_RING.append(evt)
    for q in list(_subscribers):
        try:
            q.put_nowait(evt)
        except asyncio.QueueFull:
            pass
        except Exception:
            _subscribers.discard(q)


# ══════════════════════════════════════════════════════════════════
# LANE PERSISTENCE — the scored pool is the most expensive asset the
# process owns; losing it on every restart is a self-inflicted outage.
# ══════════════════════════════════════════════════════════════════

def save_lanes() -> int:
    if not PERSIST_LANES:
        return 0
    # Tor lanes are deliberately excluded: a circuit is bound to a running tor
    # process and its exit is gone after a restart, so persisting them would
    # re-seed dead circuits as priority candidates. tor_ensure_lanes() recreates
    # the slots on boot instead.
    keep = [ln.to_dict() for ln in POOL.lanes.values()
            if ln.addr and ln.score > 0.25 and not ln.addr.startswith(TOR_LANE_PREFIX)]
    try:
        tmp = LANES_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"saved": time.time(), "lanes": keep}, f)
        os.replace(tmp, LANES_FILE)   # atomic: never leave a torn file behind
        return len(keep)
    except Exception as e:
        log.warning("could not save lanes: %s", e)
        return 0


def load_lanes() -> int:
    """Re-seed remembered lanes as PRIORITY candidates (not straight into the
    pool): they still have to prove themselves, they just skip the queue."""
    if not PERSIST_LANES:
        return 0
    try:
        with open(LANES_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0
    age = time.time() - float(data.get("saved", 0))
    if age > 86400:
        log.info("lanes.json is %.1fh old — ignoring stale egress state", age / 3600)
        return 0
    n = 0
    now = time.time()
    for d in data.get("lanes", []):
        try:
            ln = Lane.from_dict(d)
        except Exception:
            continue
        if not ln.addr or ln.score <= 0.25:
            continue
        if ln.addr.startswith(TOR_LANE_PREFIX):
            continue          # stale circuit from a previous tor process
        if is_burned(ln.addr):
            BURN_STATS["blocked_intake"] += 1
            continue          # remembered lane whose IP has since burned
        POOL.priority_candidates[f"{ln.proto}://{ln.addr}"] = now
        n += 1
    if n:
        log.info("restored %d remembered lanes into the priority queue (%.0fs old)", n, age)
    return n


async def lane_persist_loop() -> None:
    while True:
        await asyncio.sleep(30)
        try:
            save_lanes()
            prune_burned()
            save_burned()
        except Exception as e:
            log.warning("lane persist loop error: %s", e)


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
                    if proto not in ("http", "https", "socks5"):
                        proto = "socks5" if proto.startswith("socks") else "http"
                    if proto.startswith("socks") and not ALLOW_SOCKS:
                        continue
                    addr = addr.strip().rstrip("/")
                    if not _valid_addr(addr):
                        continue
                    key = f"{proto}://{addr}"
                    if key in POOL.lanes or key in POOL.candidates or key in POOL.tried:
                        continue
                    # Known-spent IP: the feeds re-serve these every cycle and
                    # they never recover. Refusing them at intake is what frees
                    # the probe budget for addresses that could be live.
                    if is_burned(addr):
                        BURN_STATS["blocked_intake"] += 1
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
                        if proto not in ("http", "https", "socks5"):
                            proto = "socks5" if proto.startswith("socks") else "http"
                        if not _valid_addr(addr):
                            continue
                        key = f"{proto}://{addr}"
                        if key in POOL.lanes or key in POOL.candidates or key in POOL.tried:
                            continue
                        if is_burned(addr):
                            BURN_STATS["blocked_intake"] += 1
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
                        if key in POOL.lanes or key in POOL.candidates or key in POOL.priority_candidates or key in POOL.tried:
                            continue
                        if is_burned(addr):
                            BURN_STATS["blocked_intake"] += 1
                            continue
                        POOL.priority_candidates[key] = now
                        added += 1
                except Exception:
                    pass

            await asyncio.gather(*[fetch_one_token(tok) for tok in tokens])

        await asyncio.gather(*[one(u, k) for u, k in TEXT_SOURCES], geonode(), webshare())
    POOL.last_fetch = now
    POOL.sources_ok = ok
    if added:
        log.info("sources: +%d candidates (queue=%d, sources_ok=%d)", added, len(POOL.candidates), ok)


# ── probing ───────────────────────────────────────────────────────
# Two stages. Stage 1 is a cheap reachability screen through the proxy (no
# completion tokens spent). Stage 2 is a real 1-token completion, which is the
# ONLY way to tell a usable egress IP from one whose free-tier quota is already
# burned — /models answers 200 even from a fully rate-limited IP, so screening
# alone promotes dead lanes. Both stages carry the opencode UA (see
# build_upstream_headers): with any other UA every probe 429s and the pool
# starves.
async def _screen(c: httpx.AsyncClient) -> bool:
    try:
        r = await c.get(f"{UPSTREAM_BASE_URL}/models",
                        headers={"Authorization": "Bearer public", "User-Agent": UPSTREAM_UA},
                        timeout=4)
        # 403 = Cloudflare blocking this proxy IP outright (error code 1010).
        if r.status_code in (200, 401):
            return True
    except Exception:
        pass
    return False


async def _probe_candidate(key: str) -> Lane | None:
    proto, addr = key.split("://", 1)
    # Second choke point for burn memory: a candidate can enter the reservoir
    # before its IP is burned (e.g. it burned while queued), so re-check at
    # dispatch. Cheap dict lookup vs a wasted two-stage upstream probe.
    if is_burned(addr):
        BURN_STATS["blocked_probe"] += 1
        _LAST_PROBE_ERR[key] = "burned-known"
        return None
    # httpx has no socks4 transport — everything socks* is dialled as socks5.
    scheme = "socks5" if proto.startswith("socks") else "http" if proto in ("http", "https") else proto
    proxy_url = f"{scheme}://{addr}"
    try:
        # Stage 1: reachability (max 4s), no quota spent.
        async with httpx.AsyncClient(proxy=proxy_url, timeout=4, verify=False) as c:
            if not await _screen(c):
                _LAST_PROBE_ERR[key] = "proxy"   # proxy reached but screen failed
                return None

        # Stage 2: real 1-token completion — proves the egress IP is not
        # quota-burned. Costs one token of THIS proxy IP's free budget, which is
        # exactly what we are shopping for.
        async with httpx.AsyncClient(proxy=proxy_url, timeout=PROBE_TIMEOUT, verify=False) as c:
            t0 = time.time()
            r = await c.post(
                f"{UPSTREAM_BASE_URL}/chat/completions",
                headers=upstream_headers(),
                json={"model": PROBE_MODEL, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 1},
            )
            lat = (time.time() - t0) * 1000

        if r.status_code == 200 and _looks_like_completion(r.content):
            cutoff = min(RELAY_PROXY_TIMEOUT * 1000, PROBE_TIMEOUT * 1000) * 0.9
            if lat > cutoff:
                _LAST_PROBE_ERR[key] = "slow"
                log.info("Prober: %s answered but too slow (%dms > %dms) — skipped",
                         _display_addr(addr), int(lat), int(cutoff))
                return None
            ln = Lane(addr, proto)
            ln.mark_ok(lat)
            log.info("Prober: +Lane %s passed completion probe (%dms)", _display_addr(addr), int(lat))
            STATS["probes_ok"] += 1
            return ln

        if r.status_code == 429:
            # This egress IP's free quota is already spent (shared public proxies
            # usually are). Not our key's problem — drop the candidate, keep going.
            # Remember it: measured, a burned IP does not come back, and the
            # feeds will re-serve this same address on the next scrape cycle.
            _LAST_PROBE_ERR[key] = "burned"
            STATS["probes_burned"] += 1
            mark_burned(addr, "probe-429")
            log.info("Prober: %s is quota-burned (429) — dropped + blocklisted", _display_addr(addr))
            return None

        _LAST_PROBE_ERR[key] = "http"
        log.info("Prober: %s failed stage 2: HTTP %d %s", _display_addr(addr),
                 r.status_code, r.text[:80].strip())
        return None
    except Exception as e:
        _LAST_PROBE_ERR[key] = _classify_probe_error(e)
        log.warning("Prober: Lane %s failed: %s", _display_addr(addr), type(e).__name__)
    return None


async def _churn_batch() -> None:
    """Test a batch of candidates, promoting the good into the pool."""
    if not POOL.candidates and not POOL.priority_candidates:
        return

    conc = ADAPT["current"] if ADAPTIVE_CONCURRENCY else TEST_CONCURRENCY
    conc = max(1, min(conc, TEST_CONCURRENCY))

    # Cap the batch: huge batches stall the manager loop for minutes and burn
    # upstream quota on hundreds of Stage-2 probes; smaller batches keep the
    # cadence tight and the key's budget intact.
    batch_size = min(conc * 8, 300)
    batch = []

    # 1. Pop from priority queue first
    while POOL.priority_candidates and len(batch) < batch_size:
        k = next(iter(POOL.priority_candidates.keys()))
        POOL.priority_candidates.pop(k, None)
        batch.append(k)

    # 2. Fill remainder from standard queue, preferring unseen /24s so the pool
    #    spreads across subnets instead of stacking 20 ports of one host.
    if POOL.candidates and len(batch) < batch_size:
        seen_subnets = set(POOL.subnet_counts().keys())
        deferred: list[str] = []
        for k in list(POOL.candidates.keys()):
            if len(batch) >= batch_size:
                break
            POOL.candidates.pop(k, None)
            sub = _key_subnet(k)
            if MAX_LANES_PER_SUBNET > 0 and POOL.subnet_full(sub):
                POOL.tried[k] = time.time()   # subnet already saturated
                continue
            if sub in seen_subnets and len(deferred) < batch_size:
                deferred.append(k)            # try fresh subnets first
                continue
            seen_subnets.add(sub)
            batch.append(k)
        for k in deferred:
            if len(batch) >= batch_size:
                POOL.candidates[k] = time.time()   # put back, try next round
            else:
                batch.append(k)

    if not batch:
        return

    STATS["candidates_tested"] += len(batch)
    log.info("Prober: Testing %d candidate proxies (concurrency=%d%s)...",
             len(batch), conc, ", adaptive" if ADAPTIVE_CONCURRENCY else "")
    publish("churn_start", {"batch": len(batch), "concurrency": conc})
    sem = asyncio.Semaphore(conc)
    conn_errors = 0
    completed = 0

    async def guarded(k):
        nonlocal conn_errors, completed
        async with sem:
            try:
                res = await _probe_candidate(k)
                completed += 1
                if isinstance(res, Lane):
                    if res.addr and POOL.subnet_full(res.subnet) and f"{res.proto}://{res.addr}" not in POOL.lanes:
                        POOL.tried[k] = time.time()
                        return
                    POOL.lanes[f"{res.proto}://{res.addr}"] = res
                    if res.warm:
                        log.info("churn: promoted new warm lane: %s (score=%.2f, lat=%dms, %s)",
                                 res.addr, res.score, int(res.lat_ms), res.tier)
                        publish("lane_up", {"addr": _display_addr(res.addr), "proto": res.proto,
                                            "lat_ms": round(res.lat_ms), "tier": res.tier,
                                            "subnet": res.subnet})
                else:
                    POOL.tried[k] = time.time()
                    if _LAST_PROBE_ERR.get(k) == "conn":
                        conn_errors += 1
                    _LAST_PROBE_ERR.pop(k, None)
            except Exception:
                completed += 1
                POOL.tried[k] = time.time()

    try:
        await asyncio.wait_for(asyncio.gather(*[guarded(k) for k in batch], return_exceptions=True), timeout=90)
    except asyncio.TimeoutError:
        # Probes are bounded per-read but a slow-dripping upstream can stretch
        # them for minutes; never let one batch stall the whole manager loop.
        abandoned = [k for k in batch if k not in POOL.lanes and k not in POOL.tried]
        for k in abandoned:
            POOL.tried[k] = time.time()
        log.warning("churn: batch timed out after 90s; %d candidates abandoned as tried", len(abandoned))

    _adapt_concurrency(conn_errors, max(1, completed))
    s = POOL.stats()
    log.info("churn: batch completed. Pool stats: warm=%d, parked=%d, queue=%d, subnets=%d",
             s["warm"], s["parked"], s["queue"], s["subnets"])
    publish("churn_done", {"pool": s})


def _key_subnet(key: str) -> str:
    """/24 of a 'proto://[user:pass@]ip:port' candidate key."""
    addr = key.split("://", 1)[-1]
    host = addr.split("@")[-1].split(":")[0]
    parts = host.split(".")
    return ".".join(parts[:3]) if len(parts) == 4 else host


# Tracks why the last probe of a candidate failed, so the adaptive controller
# can tell "the link is saturated" (connect errors) from "these proxies are
# simply dead" (HTTP errors). Only connect storms should shrink concurrency.
_LAST_PROBE_ERR: dict[str, str] = {}


def _classify_probe_error(e: Exception) -> str:
    """Classify a probe exception for adaptive-concurrency purposes.

    "conn"  — true link/NAT exhaustion (cannot reach the proxy host at all)
    "proxy" — the proxy exists but is dead/broken/rejecting (free-list norm)
    "http"  — upstream returned a non-2xx HTTP error
    """
    name = type(e).__name__
    if name in ("ConnectError", "ConnectTimeout", "NetworkError"):
        return "conn"
    if name in ("ProxyError", "RemoteProtocolError", "ReadError", "ReadTimeout",
                "WriteError", "WriteTimeout", "ProtocolError", "LocalProtocolError"):
        return "proxy"
    return "proxy"  # unknown exceptions from a public proxy are proxy noise too


def _adapt_concurrency(conn_errors: int, completed: int) -> None:
    """Halve on a connect-error storm, grow 25% while the link is clean.

    This is the durable fix for the residential-NAT collapse: a fixed 60 melts
    an Iranian CPE's connection table, and the operator has no way to know that
    is what happened. The daemon now finds its own ceiling and logs every move.
    """
    if not ADAPTIVE_CONCURRENCY:
        return
    ratio = conn_errors / completed
    ADAPT["err_ratio"] = round(ratio, 3)
    cur = ADAPT["current"]
    if ratio > 0.4 and cur > ADAPT["min"]:
        new = max(ADAPT["min"], cur // 2)
        reason = f"connect-error storm {ratio:.0%} — link/NAT saturated"
    elif ratio < 0.1 and cur < TEST_CONCURRENCY:
        new = min(TEST_CONCURRENCY, max(cur + 1, int(cur * 1.25)))
        reason = f"link clean ({ratio:.0%} connect errors) — scaling up"
    else:
        return
    if new == cur:
        return
    ADAPT["current"] = new
    ADAPT["last_change"] = time.time()
    ADAPT["last_reason"] = reason
    log.info("adaptive concurrency: %d → %d (%s)", cur, new, reason)
    publish("adapt", {"from": cur, "to": new, "reason": reason, "err_ratio": ADAPT["err_ratio"]})


async def _recover_parked() -> None:
    """Re-probe parked (burned) lanes whose cooldown elapsed — quota windows
    reset, so a burned IP often becomes usable again. This is the recovery
    loop that makes the pool self-healing instead of one-shot."""
    now = time.time()
    due = [ln for ln in POOL.parked_lanes()
           if ln.url() is not None
           and now - ln.last_probe > LANE_RECOVER_SEC * min(2 ** ln.probe_tries, 16)]
    if not due:
        return
    sem = asyncio.Semaphore(max(4, TEST_CONCURRENCY // 4))

    async def recheck(ln: Lane):
        async with sem:
            if ln.url() is None:
                return  # direct lane — nothing to re-probe through
            try:
                async with httpx.AsyncClient(proxy=ln.url(), timeout=PROBE_TIMEOUT, verify=False) as c:
                    t0 = time.time()
                    r = await c.post(
                        f"{UPSTREAM_BASE_URL}/chat/completions",
                        headers=upstream_headers(),
                        json={"model": PROBE_MODEL, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 1},
                    )
                    ln.last_probe = now
                    if r.status_code == 429 and is_quota_429(r.content, 429):
                        _note_lane_429(ln.addr)
                        # Tor lane: rotate the circuit instead of re-parking a
                        # spent exit. Public lane: _note_lane_429 blocklisted the
                        # IP, and _drop_dead will retire the lane.
                        if ln.addr.startswith(TOR_LANE_PREFIX):
                            tor_rotate_lane(ln)
                        return  # this lane's IP is burned; keep parked, don't re-burn
                    if r.status_code == 200 and _looks_like_completion(r.content):
                        _note_quota_ok()
                        ln.mark_ok((time.time() - t0) * 1000)
                        ln.probe_tries = 0
                        log.info("lane recovered: %s", _display_addr(ln.addr))
                        publish("lane_up", {"addr": _display_addr(ln.addr), "proto": ln.proto,
                                            "lat_ms": round(ln.lat_ms), "tier": ln.tier,
                                            "subnet": ln.subnet, "recovered": True})
                    else:
                        ln.probe_tries += 1
            except Exception:
                ln.last_probe = now
                ln.probe_tries += 1

    try:
        await asyncio.wait_for(asyncio.gather(*[recheck(ln) for ln in due[:20]]), timeout=60)
    except asyncio.TimeoutError:
        log.warning("recover: batch timed out after 60s")


async def _drop_dead() -> None:
    """Prune lanes that have never succeeded and have exhausted retries, and
    stale tried markers, so state doesn't grow forever.

    v1.0 also retires lanes whose IP is on the burn list. Previously a burned
    public lane sat parked forever, getting re-probed by the recover loop on a
    cooldown that measurement showed never helps — the IP is spent. Retiring it
    keeps the pool honest about its real capacity.
    """
    now = time.time()
    dead = [a for a, ln in POOL.lanes.items()
            if ln.addr != "" and ln.ok == 0 and ln.probe_tries > 4 and now - ln.last_probe > 1800]
    burned_lanes = [a for a, ln in POOL.lanes.items() if ln.addr and is_burned(ln.addr)]
    for a in dead + burned_lanes:
        POOL.lanes.pop(a, None)
    if burned_lanes:
        log.info("pool: retired %d lanes on known-spent IPs", len(burned_lanes))
    stale = [k for k, ts in POOL.tried.items() if now - ts > 3600]
    for k in stale:
        POOL.tried.pop(k, None)


async def _revalidate_warm() -> None:
    """Cheap GET /models revalidation of warm lanes that haven't answered in a
    while — catches silently-burned IPs before they serve a client request.
    The models endpoint is quota-exempt for the key, so this costs nothing
    upstream."""
    now = time.time()
    due = [ln for ln in POOL.warm_lanes()
           if ln.url() is not None
           and now - ln.last_ok > 300 and now - ln.last_probe > 120]
    if not due:
        return
    sem = asyncio.Semaphore(max(8, TEST_CONCURRENCY // 10))

    async def check(ln: Lane):
        async with sem:
            if ln.url() is None:
                return  # direct lane — nothing to revalidate through
            try:
                async with httpx.AsyncClient(proxy=ln.url(), timeout=httpx.Timeout(6, connect=4), verify=False) as c:
                    t0 = time.time()
                    r = await c.get(f"{UPSTREAM_BASE_URL}/models", headers=upstream_headers())
                    ln.last_probe = now
                    if r.status_code in (200, 401, 403, 404, 405):
                        # Reachability only — see mark_alive. Counting this as a
                        # completion success would let a quota-dead lane look
                        # proven and attract pinned traffic.
                        ln.mark_alive((time.time() - t0) * 1000)
                    else:
                        ln.mark_fail(burn=True)
            except Exception:
                ln.mark_fail(burn=True)
                ln.last_probe = now

    try:
        await asyncio.wait_for(asyncio.gather(*[check(ln) for ln in due[:20]]), timeout=60)
    except asyncio.TimeoutError:
        log.warning("revalidate: batch timed out after 60s")


async def _trim_pool() -> None:
    """Cap the pool: when it outgrows the target, evict the weakest lanes so
    revalidation/state stays bounded. Never evicts the direct lane."""
    cap = max(POOL_TARGET * 2, POOL_TARGET + 20)
    if len(POOL.lanes) <= cap:
        return
    now = time.time()
    order = sorted(
        (ln for ln in POOL.lanes.values() if ln.addr != ""),
        key=lambda ln: (ln.ok == 0, ln.parked_until > now, ln.score, ln.last_ok))
    excess = len(POOL.lanes) - cap
    for ln in order[:excess]:
        POOL.lanes.pop(f"{ln.proto}://{ln.addr}", None)
    if excess:
        log.info("pool trimmed: removed %d weakest lanes (cap=%d)", excess, cap)


def _ensure_direct_lane() -> None:
    """Idempotent: add the direct (server IP) lane when enabled, so it works
    from startup and from runtime settings toggles without a restart."""
    if DIRECT_LANE and "direct://" not in POOL.lanes:
        ln = Lane("", "direct")
        ln.score = 1.0   # never a weak link; always preferred when direct is on
        POOL.lanes["direct://"] = ln
        log.info("direct lane enabled")


async def pool_manager() -> None:
    """Background loop: keep the pool at target by fetching sources and
    churning candidates; recover parked lanes; revalidate warm lanes; prune
    dead state; ensure the direct lane."""
    while True:
        try:
            warm = len(POOL.warm_lanes())
            paused = QUOTA_STATE["exhausted"] and time.time() < QUOTA_STATE["backoff_until"]
            if paused and not QUOTA_STATE["announced"]:
                log.warning("upstream quota exhausted (429) — churn/recover paused for %.0fs; user traffic still relayed",
                            QUOTA_STATE["backoff_until"] - time.time())
            QUOTA_STATE["announced"] = paused
            if warm < POOL_TARGET:
                if not POOL.candidates:
                    await _fetch_sources()
                if not paused:
                    await _churn_batch()
            else:
                if not paused:
                    await _recover_parked()
                await _revalidate_warm()
                await _drop_dead()
                await _trim_pool()
                if not POOL.candidates:
                    await _fetch_sources()
            _ensure_direct_lane()
            tor_ensure_lanes()
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
    # Fast timeouts: 2s connect, max 4s read per attempt so dead proxies fail over instantly!
    attempt_timeout = min(4.0, timeout)
    kwargs: dict = {"timeout": httpx.Timeout(attempt_timeout, connect=2.0), "verify": False}
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
                log.info("lane %s returned 200 with non-completion body — burned", _display_addr(lane.addr))
                return 502, {"content-type": "application/json"}, json.dumps(
                    {"error": {"message": "lane returned invalid body", "type": "lane_invalid"}}).encode()
        elif resp.status_code == 429 and is_quota_429(body, 429):
            lane.mark_fail(burn=False)
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


_lane_cursor = 0


def _pick_lane(lanes: list[Lane]) -> Lane:
    """Pin-and-drain: prefer a small set of PROVEN lanes and keep using them.

    v0.9 rotated across the top 3 latency-ranked lanes regardless of whether
    they had ever served a request. That is the wrong shape for this upstream:
    measured, a live egress IP serves 40+ consecutive requests before its budget
    is spent, while ~35% of fresh candidates are already burned on first use. So
    spreading traffic thin re-pays the miss cost over and over, whereas draining
    a known-good lane amortises one probe across dozens of requests.

    Ranking, in order:
      1. lanes that have actually succeeded (ok > 0), best score first — these
         are the pins, and they get the traffic until they burn;
      2. unproven lanes, fastest first — used only when no pin has capacity,
         which is also how new lanes get their first real request.

    Within the pin set we still round-robin, because a single lane has
    LANE_MAX_INFLIGHT capacity and concurrent requests must not queue behind it.
    """
    global _lane_cursor
    if not lanes:
        raise IndexError("no lanes")
    proven = [ln for ln in lanes if ln.ok > 0]
    if proven:
        proven.sort(key=lambda ln: (-ln.score, ln.lat_ms if ln.lat_ms else 8000))
        pins = proven[: max(1, min(LANE_PIN_COUNT, len(proven)))]
        lane = pins[_lane_cursor % len(pins)]
        _lane_cursor += 1
        return lane
    top = lanes[: min(3, len(lanes))]
    lane = top[_lane_cursor % len(top)]
    _lane_cursor += 1
    return lane


async def relay(payload: dict, path: str, stream: bool, headers: dict, timeout: float):
    """Try lanes in score order until one answers. Transparent failover: a
    burned/dead lane burns, the request silently moves to the next lane."""
    attempts = max(1, RELAY_ATTEMPTS)
    deadline = time.time() + timeout
    last_err: tuple | None = None
    tried: set[str] = set()
    t_start = time.time()

    # To prevent key-global 429 locks when we have multiple keys to cycle through,
    # generate headers on a per-attempt basis using a fresh picked key.
    for i in range(attempts):
        # available_lanes() respects the per-lane in-flight cap so concurrent
        # requests spread out instead of stacking onto the fastest lane and
        # burning it — free proxies collapse under parallel load.
        lanes = [ln for ln in POOL.available_lanes() if f"{ln.proto}://{ln.addr}" not in tried]
        if not lanes:
            lanes = [ln for ln in POOL.warm_lanes() if f"{ln.proto}://{ln.addr}" not in tried]
        if not lanes:
            # Fallback to parked lanes if no warm lanes are available
            lanes = [ln for ln in POOL.parked_lanes() if f"{ln.proto}://{ln.addr}" not in tried]
            lanes.sort(key=lambda ln: ln.parked_until)
            if not lanes:
                break
        lane = _pick_lane(lanes) if i == 0 else lanes[0]
        tried.add(f"{lane.proto}://{lane.addr}")
        
        # Pick next round-robin key and build headers for this specific attempt
        attempt_headers = build_upstream_headers(request=None, custom_key=None)
        if headers:
            for k, v in headers.items():
                if k.lower() != "authorization":
                    attempt_headers[k] = v
                    
        try:
            async with lane.sem:
                lane.inflight += 1
                try:
                    status, resp_headers, body = await _attempt(lane, payload, path, attempt_headers, min(timeout, RELAY_PROXY_TIMEOUT))
                finally:
                    lane.inflight -= 1
            if status == 200:
                _note_quota_ok()
                ms = (time.time() - t_start) * 1000
                record_request(_display_addr(lane.addr), 200, ms, stream)
                publish("request", {"lane": _display_addr(lane.addr), "status": 200,
                                    "ms": round(ms), "attempt": i + 1, "tier": lane.tier})
                return status, resp_headers, body
            if status == 429 and is_quota_429(body, 429):
                # This egress IP's free-tier budget is spent. Park it for the
                # cooldown — leaving it warm (the pre-0.9 behaviour) meant the
                # next request picked the same dead lane again, which is how a
                # 1.4k-request pool logged 15k failovers. Rotation only works if
                # a burned IP actually leaves rotation.
                lane.mark_fail(burn=True)
                _note_lane_429(lane.addr)
                # v1.0: a Tor lane is not parked — its exit is spent, but the
                # slot can get a brand-new exit by changing the circuit name.
                # That is the self-renewing part of the Tor provider.
                if lane.addr.startswith(TOR_LANE_PREFIX):
                    tor_rotate_lane(lane)
                STATS["failovers"] += 1
                last_err = (429, body)
                if i >= (attempts - 1) or time.time() > deadline or QUOTA_STATE["exhausted"]:
                    break
                continue
            # invalid-body 502 from a burned lane: fail over, don't surface
            if status == 502:
                try:
                    if json.loads(body).get("error", {}).get("type") == "lane_invalid":
                        STATS["failovers"] += 1
                        last_err = (502, body)
                        if time.time() > deadline:
                            break
                        continue
                except Exception:
                    pass
            # 5xx from the lane or upstream: fail over to the next lane; keep
            # the last 5xx body as the answer if every attempt fails.
            if status >= 500:
                lane.mark_fail()
                STATS["failovers"] += 1
                last_err = (status, body)
                if time.time() > deadline:
                    break
                continue
            # 3xx from a hijacking/captive proxy: never legitimate from the
            # upstream — fail over and burn the lane.
            if 300 <= status < 400:
                lane.mark_fail(burn=True)
                STATS["lane_failures"] += 1
                STATS["failovers"] += 1
                last_err = (status, body)
                if time.time() > deadline:
                    break
                continue
            # other upstream error (400/401/403): return as-is, not a proxy problem
            return status, resp_headers, body
        except Exception as e:
            lane.mark_fail()
            STATS["lane_failures"] += 1
            STATS["failovers"] += 1
            last_err = ("error", str(e).encode())
            if time.time() > deadline:
                break
            continue

    if last_err and last_err[0] == 429:
        # Avoid returning HTTP 429 to 9router on proxy-wide limits, so 9router does not set a modelLock.
        # Respond with 503 Service Unavailable, letting 9router retry or fall back cleanly.
        record_request("-", 503, (time.time() - t_start) * 1000, stream)
        publish("request", {"lane": "-", "status": 503, "ms": round((time.time() - t_start) * 1000)})
        return 503, {"content-type": "application/json"}, last_err[1]
    if last_err and isinstance(last_err[0], int) and last_err[0] >= 500:
        record_request("-", last_err[0], (time.time() - t_start) * 1000, stream)
        publish("request", {"lane": "-", "status": last_err[0], "ms": round((time.time() - t_start) * 1000)})
        return last_err[0], {"content-type": "application/json"}, last_err[1]
    record_request("-", 503, (time.time() - t_start) * 1000, stream)
    publish("request", {"lane": "-", "status": 503, "ms": round((time.time() - t_start) * 1000)})
    return 503, {"content-type": "application/json"}, json.dumps(
        {"error": {"message": "all egress lanes busy or failed — pool is refilling, retry shortly", "type": "rotator_exhausted"}}
    ).encode()


async def _stream_one(body: bytes):
    """Yield a single buffered body as an SSE stream (failure/4xx path)."""
    yield body


async def relay_stream(payload: dict, path: str, headers: dict, timeout: float):
    """Try lanes in score order; stream from the first lane that answers.
    Returns (status, resp_headers, chunks) where chunks is an async generator
    of SSE bytes. Failover/accounting/deadline mirror relay()."""
    attempts = max(1, RELAY_ATTEMPTS)
    deadline = time.time() + timeout
    last_err: tuple | None = None
    tried: set[str] = set()
    t_start = time.time()

    for i in range(attempts):
        # Streaming is where latency is visible to a human, so prefer the fast
        # tier; available_lanes() falls back to any capacity-free lane when no
        # fast lane exists.
        lanes = [ln for ln in POOL.available_lanes(tier="fast") if f"{ln.proto}://{ln.addr}" not in tried]
        if not lanes:
            lanes = [ln for ln in POOL.warm_lanes() if f"{ln.proto}://{ln.addr}" not in tried]
        if not lanes:
            # Fallback to parked lanes if no warm lanes are available
            lanes = [ln for ln in POOL.parked_lanes() if f"{ln.proto}://{ln.addr}" not in tried]
            lanes.sort(key=lambda ln: ln.parked_until)
            if not lanes:
                break
        lane = _pick_lane(lanes) if i == 0 else lanes[0]
        tried.add(f"{lane.proto}://{lane.addr}")

        # Pick next round-robin key and build headers for this specific attempt
        attempt_headers = build_upstream_headers(request=None, custom_key=None)
        if headers:
            for k, v in headers.items():
                if k.lower() != "authorization":
                    attempt_headers[k] = v

        client = None
        resp = None
        t0 = time.time()
        try:
            client = httpx.AsyncClient(
                proxy=lane.url(),
                timeout=httpx.Timeout(min(timeout, RELAY_PROXY_TIMEOUT), connect=12),
                verify=False)
            req = client.build_request("POST", f"{UPSTREAM_BASE_URL}/{path}", headers=attempt_headers, json=payload)
            resp = await client.send(req, stream=True)
            status = resp.status_code
            if status == 200:
                # peek the first non-empty chunk to validate the lane before
                # committing to it (echo/captive portals fail the peek)
                aiter = resp.aiter_bytes()
                first = None
                for _ in range(5):
                    try:
                        chunk = await aiter.__anext__()
                    except StopAsyncIteration:
                        break
                    if chunk.strip():
                        first = chunk
                        break
                if first is None or not _looks_like_completion(first):
                    lane.mark_fail(burn=True)
                    STATS["lane_failures"] += 1
                    log.info("lane %s returned 200 with non-completion stream — burned", _display_addr(lane.addr))
                    await resp.aclose()
                    await client.aclose()
                    continue
                lane.mark_ok((time.time() - t0) * 1000)
                _note_quota_ok()
                STATS["streams"] += 1
                owned_resp, owned_client = resp, client
                # A stream occupies the lane for its whole lifetime, not just
                # the handshake — count it in-flight until the generator closes.
                lane.inflight += 1
                ms_ttfb = (time.time() - t_start) * 1000
                record_request(_display_addr(lane.addr), 200, ms_ttfb, True)
                publish("request", {"lane": _display_addr(lane.addr), "status": 200,
                                    "ms": round(ms_ttfb), "attempt": i + 1,
                                    "tier": lane.tier, "stream": True})

                async def chunks():
                    try:
                        yield first
                        async for chunk in aiter:
                            yield chunk
                    except Exception as e:
                        log.warning("stream on lane %s interrupted mid-stream: %s", _display_addr(lane.addr), repr(e))
                        yield b'data: {"error":{"message":"upstream stream interrupted","type":"stream_interrupted"}}\n\n'
                    finally:
                        lane.inflight = max(0, lane.inflight - 1)
                        await owned_resp.aclose()
                        await owned_client.aclose()

                return 200, dict(resp.headers), chunks()
            if status >= 500:
                body = await resp.aread()
                lane.mark_fail()
                STATS["failovers"] += 1
                last_err = (status, body)
                await resp.aclose()
                await client.aclose()
                if time.time() > deadline:
                    break
                continue
            if 300 <= status < 400:
                # hijacking/captive proxy redirect — never legitimate from the
                # upstream; burn the lane and fail over.
                await resp.aclose()
                await client.aclose()
                lane.mark_fail(burn=True)
                STATS["lane_failures"] += 1
                STATS["failovers"] += 1
                last_err = (status, b"")
                if time.time() > deadline:
                    break
                continue
            if status == 429 and is_quota_429(await resp.aread(), 429):
                # Same as the non-streaming path: burn, don't just demote.
                lane.mark_fail(burn=True)
                _note_lane_429(lane.addr)
                if lane.addr.startswith(TOR_LANE_PREFIX):
                    tor_rotate_lane(lane)
                STATS["failovers"] += 1
                last_err = (429, b"")
                await resp.aclose()
                await client.aclose()
                if i >= (attempts - 1) or time.time() > deadline or QUOTA_STATE["exhausted"]:
                    break
                continue
            # other 4xx (400/401/403): surface immediately, not a proxy problem
            body = await resp.aread()
            resp_headers = dict(resp.headers)
            await resp.aclose()
            await client.aclose()
            return status, resp_headers, _stream_one(body)
        except Exception as e:
            if resp is not None:
                try:
                    await resp.aclose()
                except Exception:
                    pass
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass
            lane.mark_fail()
            STATS["lane_failures"] += 1
            STATS["failovers"] += 1
            last_err = ("error", str(e).encode())
            if time.time() > deadline:
                break
            continue

    if last_err and last_err[0] == 429:
        # Avoid returning HTTP 429 to 9router on proxy-wide limits, so 9router does not set a modelLock.
        return 503, {"content-type": "application/json"}, _stream_one(last_err[1] or b"")
    if last_err and isinstance(last_err[0], int) and last_err[0] >= 500:
        return last_err[0], {"content-type": "application/json"}, _stream_one(last_err[1])
    return 503, {"content-type": "application/json"}, _stream_one(json.dumps(
        {"error": {"message": "all egress lanes busy or failed — pool is refilling, retry shortly", "type": "rotator_exhausted"}}
    ).encode())


def upstream_headers(custom_key: str | None = None) -> dict:
    """Headers for upstream calls. The Authorization header is OMITTED when the
    key is empty — httpx rejects 'Bearer ' (empty value) with
    LocalProtocolError, which silently killed every probe and relayed request.

    The opencode User-Agent is included on EVERY upstream call (probes, models
    fetch, relayed requests) — see build_upstream_headers for why."""
    h = {"Content-Type": "application/json", "User-Agent": UPSTREAM_UA}
    key = custom_key or get_upstream_key()
    if key:
        h["Authorization"] = f"Bearer {key}"
    return h


def build_upstream_headers(request: Request | None = None, custom_key: str | None = None) -> dict:
    """Headers for upstream calls.

    CRITICAL (measured 2026-08-15 against opencode.ai/zen/v1): the free-tier
    gate is keyed on the User-Agent, NOT only on the egress IP. A UA whose
    lowercase form contains "opencode" gets HTTP 200; every other UA
    (Chrome, python-httpx, curl, axios, node-fetch, Go-http-client) gets an
    instant 429 FreeUsageLimitError on the *same* IP and the *same* key.

    So we always present an opencode client UA. The client's own UA is
    forwarded only when it already identifies as opencode — otherwise it is
    replaced, because forwarding it guarantees a 429.
    """
    h = upstream_headers(custom_key=custom_key)
    ua = UPSTREAM_UA
    if request:
        user_ua = request.headers.get("user-agent", "")
        if user_ua and "opencode" in user_ua.lower():
            ua = user_ua
    h["User-Agent"] = ua
    return h


def _check_relay_auth(request: Request, allow_query: bool = False) -> bool:
    """Constant-time relay-key check.

    allow_query exists only for /api/events: the browser EventSource API cannot
    set an Authorization header, so the key has to ride in the URL there. It is
    deliberately opt-in per endpoint — a key in a query string can land in
    access logs and Referer headers, so no other route accepts it.
    """
    if not RELAY_API_KEY:
        return True
    auth = request.headers.get("authorization", "")
    if hmac.compare_digest(auth, f"Bearer {RELAY_API_KEY}") or hmac.compare_digest(auth, RELAY_API_KEY):
        return True
    if allow_query:
        qk = request.query_params.get("key", "")
        if qk and hmac.compare_digest(qk, RELAY_API_KEY):
            return True
    return False


# ── model resolution (Claude Code tier aliases -> real upstream model) ──
_FALLBACK_MODEL: str | None = None
_FALLBACK_MODEL_AT: float = 0.0
FALLBACK_MODEL_SEC = 300

MODELS_CACHE_SEC = 3600
_models_cache: dict = {"updated": 0.0, "status": 503, "body": b"", "content_type": "application/json"}
_models_retry_at = 0.0   # outage backoff: don't hammer upstream every call


async def get_models_cached() -> tuple[int, str, bytes]:
    # _models_retry_at is assigned below, so it MUST be declared global —
    # without this the very first read raises UnboundLocalError and every
    # models fetch fails (silently, because callers wrap this in try/except).
    global _models_retry_at
    now = time.time()
    if now < _models_retry_at:
        # outage backoff: serve the stale body (or 503) without hammering
        if _models_cache["body"]:
            return _models_cache["status"], _models_cache["content_type"], _models_cache["body"]
        return 503, "application/json", json.dumps({"error": {"message": "models fetch failed", "type": "models_unavailable"}}).encode()
    elif now - _models_cache["updated"] < MODELS_CACHE_SEC and _models_cache["body"]:
        return _models_cache["status"], _models_cache["content_type"], _models_cache["body"]
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(20, connect=10)) as client:
            resp = await client.get(f"{UPSTREAM_BASE_URL}/models", headers=upstream_headers())
            _models_retry_at = 0.0
            
            # Filter OpenCode models to return ONLY -free models and prioritize hy3-free at index 0
            body_bytes = resp.content
            if resp.status_code == 200:
                try:
                    data = json.loads(resp.content)
                    if "data" in data and isinstance(data["data"], list):
                        free_models = [m for m in data["data"] if m.get("id", "").endswith("-free")]
                        free_models.sort(key=lambda m: 0 if m.get("id") == "hy3-free" else 1)
                        data["data"] = free_models
                        body_bytes = json.dumps(data).encode()
                except Exception as je:
                    log.warning("failed to parse models JSON: %s", je)
            
            _models_cache.update({"updated": time.time(), "status": resp.status_code,
                                  "body": body_bytes, "content_type": resp.headers.get("content-type", "application/json")})
            return resp.status_code, _models_cache["content_type"], body_bytes
    except Exception as e:
        log.warning("models fetch failed: %s", e)
        _models_retry_at = time.time() + 30
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

@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    """Modern FastAPI lifespan: startup work, then graceful shutdown.

    Replaces the deprecated @app.on_event hooks and, unlike them, guarantees
    the pool is persisted even when the process is asked to stop mid-churn.
    """
    load_settings()

    # 1. SOCKS library validation
    try:
        import socksio  # noqa: F401
    except ImportError:
        log.error("CRITICAL ERROR: SOCKS proxy support is missing! SOCKS4/5 proxies will be ignored. "
                  "Please run: pip install 'httpx[socks]' to install SOCKS dependencies.")

    # 2. Configuration sanity — surface misconfiguration at boot instead of
    #    letting the operator discover it as "0 warm proxies" an hour later.
    for line in config_warnings():
        log.warning("CONFIG: %s", line)

    # 3. Re-seed remembered lanes so a restart is not a cold start.
    #    Burn memory loads FIRST so the re-seed can filter out lanes whose IPs
    #    have burned since the last run instead of re-probing them.
    load_burned()
    restored = load_lanes()
    if restored:
        log.info("warm-start: %d remembered lanes queued for fast revalidation", restored)

    # 3b. Tor egress. Enabled by config; verified by an actual connect to the
    #     SOCKS port, so a misconfigured port degrades to "no tor lanes" with a
    #     clear warning instead of silently adding lanes that can never dial.
    if TOR_ENABLED:
        if tor_available():
            added = tor_ensure_lanes()
            log.info("tor: enabled, %d circuit lanes on 127.0.0.1:%d", added, TOR_SOCKS_PORT)
        else:
            log.warning("tor_enabled=true but nothing is listening on 127.0.0.1:%d — "
                        "no tor lanes will be created. Start tor with "
                        "'SocksPort %d IsolateSOCKSAuth'.", TOR_SOCKS_PORT, TOR_SOCKS_PORT)

    # Background workers are skipped when IP_RELAY_NO_BACKGROUND=1 so tests (and
    # anyone importing the app for inspection) don't start scraping the internet.
    tasks: list[asyncio.Task] = []
    if os.environ.get("IP_RELAY_NO_BACKGROUND", "0") not in ("1", "true", "yes"):
        tasks = [asyncio.create_task(pool_manager()),
                 asyncio.create_task(lane_persist_loop())]

        # 4. Direct validation check against the Upstream on startup
        async def verify_upstream():
            try:
                status, _, body = await get_models_cached()
                if status == 200:
                    log.info("Upstream verification: Upstream base URL and API key are VALID.")
                else:
                    log.error("CRITICAL Upstream verification FAILED (HTTP %d). "
                              "Your configured UPSTREAM_BASE_URL ('%s') or UPSTREAM_API_KEY might be invalid. "
                              "Response: %s", status, UPSTREAM_BASE_URL, body[:150].decode('utf-8', errors='ignore'))
            except Exception as e:
                log.error("CRITICAL Upstream connection FAILED: Could not reach upstream on startup: %s. "
                          "Verify your network or UPSTREAM_BASE_URL ('%s').", e, UPSTREAM_BASE_URL)

        tasks.append(asyncio.create_task(verify_upstream()))

    try:
        yield
    finally:
        n = save_lanes()
        if n:
            log.info("shutdown: persisted %d lanes to %s", n, LANES_FILE)
        b = save_burned()
        if b:
            log.info("shutdown: persisted %d burned IPs to %s", b, BURNED_FILE)
        for t in tasks:
            t.cancel()


app.router.lifespan_context = lifespan


def config_warnings() -> list[str]:
    """Human-readable configuration problems. Returned by /api/diagnostics and
    logged at startup — misconfiguration should never be silent."""
    out: list[str] = []
    if not UPSTREAM_BASE_URL:
        out.append("upstream_base_url is empty — no upstream to relay to.")
    elif not UPSTREAM_BASE_URL.startswith(("http://", "https://")):
        out.append(f"upstream_base_url ('{UPSTREAM_BASE_URL}') has no http(s):// scheme.")
    if UPSTREAM_API_KEY == "public":
        out.append("upstream_api_key is the shared literal 'public' — it is globally "
                   "rate-limited and usually pre-burned. Expect widespread 429s.")
    if "opencode" not in UPSTREAM_UA.lower():
        out.append(f"upstream_user_agent ('{UPSTREAM_UA}') does not contain 'opencode'. "
                   "The upstream free tier gates on the User-Agent: every other UA gets "
                   "an instant 429 regardless of egress IP or key.")
    if not PROBE_MODEL:
        out.append("probe_model is empty — lanes cannot be verified with a real completion.")
    if not RELAY_API_KEY:
        out.append("relay_api_key is empty — the control plane and /v1 are UNAUTHENTICATED. "
                   "Anyone who can reach this port can read your proxy pool and repoint the upstream.")
    try:
        import socksio  # noqa: F401
    except ImportError:
        if ALLOW_SOCKS:
            out.append("allow_socks is on but socksio is not installed — every SOCKS "
                       "candidate will fail. Install with: pip install 'httpx[socks]'")
    if TEST_CONCURRENCY > 40:
        out.append(f"proxy_test_concurrency is {TEST_CONCURRENCY}: on a residential/CPE link this "
                   "saturates the NAT table and every probe fails with connect errors. "
                   "15–25 is the safe range (adaptive concurrency will also back off on its own).")
    if LANE_MAX_INFLIGHT > 4:
        out.append(f"lane_max_inflight is {LANE_MAX_INFLIGHT}: free proxies typically survive "
                   "1–2 concurrent requests before dropping.")
    if TOR_ENABLED and not tor_available():
        out.append(f"tor_enabled is on but nothing is listening on 127.0.0.1:{TOR_SOCKS_PORT}. "
                   f"Start tor with 'SocksPort {TOR_SOCKS_PORT} IsolateSOCKSAuth' — without "
                   "IsolateSOCKSAuth every lane shares ONE circuit and one exit IP, which "
                   "defeats the entire point.")
    if not BURN_MEMORY:
        out.append("burn_memory is off. Measured: a 429'd egress IP does not recover (40 min of "
                   "continuous polling, zero successes), and the scrape feeds re-serve the same "
                   "dead addresses every cycle — so the prober will keep spending its budget on "
                   "known-spent IPs.")
    if LANE_COOLDOWN_SEC < 600:
        out.append(f"lane_cooldown_sec is {LANE_COOLDOWN_SEC}s. A burned IP was measured 429ing "
                   "for 40+ minutes straight (and 8 days on one host), so a short cooldown just "
                   "re-probes corpses. 3600s or more is realistic.")
    return out


@app.get("/healthz")
async def healthz():
    s = POOL.stats()
    return {"ok": s["warm"] > 0, "version": VERSION, **s}


@app.get("/metrics")
async def metrics():
    """Prometheus text exposition. Unauthenticated on purpose: it contains no
    credentials and no proxy addresses, and scrapers rarely carry bearer
    tokens. Bind to localhost or firewall the port if that is not acceptable."""
    s = POOL.stats()
    m5 = metrics_window(300)
    lines = [
        "# HELP iprelay_warm_lanes Warm egress lanes",
        "# TYPE iprelay_warm_lanes gauge",
        f"iprelay_warm_lanes {s['warm']}",
        "# HELP iprelay_parked_lanes Parked (cooling down) lanes",
        "# TYPE iprelay_parked_lanes gauge",
        f"iprelay_parked_lanes {s['parked']}",
        "# HELP iprelay_unique_subnets Distinct /24s among warm lanes",
        "# TYPE iprelay_unique_subnets gauge",
        f"iprelay_unique_subnets {s['subnets']}",
        "# HELP iprelay_capacity Concurrent requests the pool can absorb",
        "# TYPE iprelay_capacity gauge",
        f"iprelay_capacity {s['capacity']}",
        "# HELP iprelay_inflight Requests currently in flight",
        "# TYPE iprelay_inflight gauge",
        f"iprelay_inflight {s['inflight']}",
        "# HELP iprelay_candidate_queue Candidates awaiting probe",
        "# TYPE iprelay_candidate_queue gauge",
        f"iprelay_candidate_queue {s['queue']}",
        "# HELP iprelay_probe_concurrency Probe concurrency currently in force",
        "# TYPE iprelay_probe_concurrency gauge",
        f"iprelay_probe_concurrency {ADAPT['current']}",
        "# HELP iprelay_requests_total Client requests served",
        "# TYPE iprelay_requests_total counter",
        f"iprelay_requests_total {STATS['requests']}",
        "# HELP iprelay_failovers_total Lane failovers inside client requests",
        "# TYPE iprelay_failovers_total counter",
        f"iprelay_failovers_total {STATS['failovers']}",
        "# HELP iprelay_lane_failures_total Lane-level failures",
        "# TYPE iprelay_lane_failures_total counter",
        f"iprelay_lane_failures_total {STATS['lane_failures']}",
        "# HELP iprelay_upstream_429_total Key-global rate-limit responses",
        "# TYPE iprelay_upstream_429_total counter",
        f"iprelay_upstream_429_total {STATS['upstream_429s']}",
        "# HELP iprelay_probes_ok_total Candidates that passed verification",
        "# TYPE iprelay_probes_ok_total counter",
        f"iprelay_probes_ok_total {STATS['probes_ok']}",
        "# HELP iprelay_latency_p95_ms 95th percentile latency, 5m window",
        "# TYPE iprelay_latency_p95_ms gauge",
        f"iprelay_latency_p95_ms {m5['p95_ms']}",
        "# HELP iprelay_quota_exhausted Upstream key quota exhausted (1/0)",
        "# TYPE iprelay_quota_exhausted gauge",
        f"iprelay_quota_exhausted {1 if QUOTA_STATE['exhausted'] else 0}",
    ]
    return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


@app.get("/api/metrics")
async def api_metrics(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return {"w60": metrics_window(60), "w300": metrics_window(300),
            "w3600": metrics_window(3600), "spark": sparkline_series(),
            "adaptive": ADAPT}


@app.get("/api/diagnostics")
async def api_diagnostics(request: Request):
    """Everything needed to answer "why do I have 0 warm lanes?" in one call:
    config problems, probe-failure breakdown, and a plain-language verdict."""
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    s = POOL.stats()
    reasons: dict[str, int] = {}
    for line in list(LOG_RING)[-400:]:
        if "failed Stage 2 connection" in line or "failed Stage 1" in line:
            reasons["connect_error"] = reasons.get("connect_error", 0) + 1
        elif "failed Stage 2: HTTP 401" in line:
            reasons["http_401_bad_key"] = reasons.get("http_401_bad_key", 0) + 1
        elif "failed Stage 2: HTTP 403" in line:
            reasons["http_403_blocked"] = reasons.get("http_403_blocked", 0) + 1
        elif "failed Stage 2: HTTP 407" in line:
            reasons["http_407_proxy_auth"] = reasons.get("http_407_proxy_auth", 0) + 1
        elif "failed Stage 2: HTTP" in line:
            reasons["http_other"] = reasons.get("http_other", 0) + 1
        elif "rate-limited (429)" in line:
            reasons["upstream_429"] = reasons.get("upstream_429", 0) + 1
        elif "latency was too slow" in line:
            reasons["too_slow"] = reasons.get("too_slow", 0) + 1

    verdict = "healthy"
    advice = "Pool is serving traffic."
    if s["warm"] == 0:
        top = max(reasons, key=lambda k: reasons[k]) if reasons else None
        if top == "connect_error":
            verdict = "egress_blocked"
            advice = ("Probes cannot open outbound connections to proxy ports. A VPN, firewall, "
                      "or ISP filter is blocking non-80/443 traffic, or the link's NAT table is "
                      "saturated. Lower proxy_test_concurrency (15–25) and retry without the VPN.")
        elif top == "http_401_bad_key":
            verdict = "bad_upstream_key"
            advice = "The upstream rejects the API key (401). Fix upstream_api_key."
        elif top == "http_403_blocked":
            verdict = "upstream_blocks_proxies"
            advice = "The upstream 403s these egress IPs (datacenter/VPN ranges are often blocked)."
        elif top == "upstream_429":
            verdict = "quota_exhausted"
            advice = "The key's own quota is exhausted; every IP 429s. Use a private key."
        elif not reasons:
            verdict = "warming_up"
            advice = "No probe results yet — the pool is still fetching and testing candidates."
        else:
            verdict = "no_usable_proxies"
            advice = "Candidates connect but fail verification. Check the reason breakdown."
    elif QUOTA_STATE["exhausted"]:
        verdict = "quota_exhausted"
        advice = "Lanes are alive but the upstream key's quota is exhausted; probing is paused."
    return {
        "verdict": verdict, "advice": advice,
        "config_warnings": config_warnings(),
        "probe_failures": reasons,
        "pool": s,
        "adaptive": ADAPT,
        "quota": {"exhausted": QUOTA_STATE["exhausted"],
                  "resumes_in": max(0, int(QUOTA_STATE["backoff_until"] - time.time()))},
        "socks_available": _socks_available(),
        "persisted_lanes": os.path.exists(LANES_FILE),
    }


def _socks_available() -> bool:
    try:
        import socksio  # noqa: F401
        return True
    except ImportError:
        return False


@app.get("/api/profiles")
async def api_profiles(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return {"profiles": [{"id": k, **v} for k, v in PROVIDER_PROFILES.items()]}


@app.get("/api/events")
async def api_events(request: Request):
    """Server-sent events: lane_up / lane_down / request / churn / adapt.

    Replaces three independent polling loops in the dashboard. Each subscriber
    gets a bounded queue — a stalled browser tab drops events instead of
    applying backpressure to the relay.
    """
    if not _check_relay_auth(request, allow_query=True):
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    _subscribers.add(q)

    async def gen():
        try:
            yield f"data: {json.dumps({'kind': 'hello', 'ts': time.time(), 'pool': POOL.stats(), 'version': VERSION})}\n\n"
            while True:
                try:
                    evt = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {json.dumps(evt)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"   # keep proxies from closing the stream
        except asyncio.CancelledError:
            raise
        finally:
            _subscribers.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/stats")
async def api_stats(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    s = POOL.stats()
    return {"version": VERSION, "uptime_sec": int(time.time() - STATS["started"]),
            "pool": s, "stats": STATS, "settings": public_settings(),
            "adaptive": ADAPT, "metrics": metrics_window(300),
            "burn": {"known_spent_ips": len(BURNED), "ttl_sec": BURN_TTL_SEC,
                     "enabled": BURN_MEMORY, **BURN_STATS},
            "tor": {"enabled": TOR_ENABLED, "socks_port": TOR_SOCKS_PORT,
                    "reachable": tor_available() if TOR_ENABLED else False,
                    "target_lanes": TOR_LANES, "rotations": _tor_rotations,
                    "lanes": sum(1 for ln in POOL.lanes.values()
                                 if ln.addr.startswith(TOR_LANE_PREFIX)),
                    "warm_lanes": sum(1 for ln in POOL.warm_lanes()
                                      if ln.addr.startswith(TOR_LANE_PREFIX))},
            "quota": {"exhausted": QUOTA_STATE["exhausted"],
                      "resumes_in": max(0, int(QUOTA_STATE["backoff_until"] - time.time()))}}


@app.get("/api/pool")
async def api_pool(request: Request):
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    now = time.time()
    try:
        limit = max(1, min(200, int(request.query_params.get("limit", 50))))
        offset = max(0, int(request.query_params.get("offset", 0)))
    except ValueError:
        limit, offset = 50, 0

    all_warm = POOL.warm_lanes()
    all_parked = POOL.parked_lanes()
    warm = [{"addr": _display_addr(ln.addr), "proto": ln.proto, "score": round(ln.score, 3),
             "lat_ms": round(ln.lat_ms), "ok": ln.ok, "fails": ln.fails,
             "tier": ln.tier, "subnet": ln.subnet, "inflight": ln.inflight,
             "capacity": max(1, LANE_MAX_INFLIGHT),
             "kind": "tor" if ln.addr.startswith(TOR_LANE_PREFIX) else ("direct" if not ln.addr else "proxy"),
             "proven": ln.ok > 0,
             "age_sec": int(now - ln.created),
             "last_ok_ago": int(now - ln.last_ok) if ln.last_ok else -1}
            for ln in all_warm[offset:offset + limit]]
    parked = [{"addr": _display_addr(ln.addr), "proto": ln.proto, "until_in": int(ln.parked_until - now),
               "subnet": ln.subnet, "score": round(ln.score, 3), "fails": ln.fails,
               "probe_tries": ln.probe_tries,
               "last_probe_ago": int(now - ln.last_probe) if ln.last_probe else -1}
              for ln in all_parked[offset:offset + limit]]
    return {"warm": warm, "parked": parked,
            "queue": len(POOL.candidates) + len(POOL.priority_candidates),
            "total_warm": len(all_warm), "total_parked": len(all_parked),
            "limit": limit, "offset": offset,
            "subnets": POOL.subnet_counts()}


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
    
    updates, errors = validate_settings_payload(body)
    if errors:
        return JSONResponse({"error": "invalid settings", "details": errors}, status_code=400)
    webshare_changed = ("webshare_token" in updates
                        and updates["webshare_token"] != settings.get("webshare_token"))

    res = apply_settings(updates)
    log.info("settings updated via API: %s", ", ".join(sorted(updates.keys())) or "(no changes)")

    if webshare_changed:
        log.info("Settings: Webshare tokens updated — resetting scraper interval to force immediate proxy check.")
        POOL.last_fetch = 0.0
        # Immediately kick off scraping & batch churning in the background
        asyncio.create_task(_fetch_sources())

    return res


@app.post("/api/profiles/{profile_id}")
async def api_apply_profile(profile_id: str, request: Request):
    """One-click upstream setup. Never clobbers an existing key with a blank."""
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    prof = PROVIDER_PROFILES.get(profile_id)
    if not prof:
        return JSONResponse({"error": "unknown profile"}, status_code=404)
    updates = {"upstream_base_url": prof["upstream_base_url"], "probe_model": prof["probe_model"]}
    if prof.get("upstream_api_key"):
        updates["upstream_api_key"] = prof["upstream_api_key"]
    apply_settings(updates)
    _models_cache["updated"] = 0.0
    log.info("provider profile applied: %s (%s)", profile_id, prof["label"])
    return {"applied": profile_id, "settings": public_settings(), "note": prof.get("note", "")}


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


@app.get("/api/burned")
async def api_burned(request: Request):
    """The blocklist of spent egress IPs, newest first.

    Operator-facing: without this the burn list is invisible and a wrongly
    blocklisted IP would be impossible to diagnose.
    """
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    now = time.time()
    try:
        limit = max(1, min(1000, int(request.query_params.get("limit", 100))))
    except ValueError:
        limit = 100
    rows = sorted(BURNED.items(), key=lambda kv: -kv[1].get("at", 0))[:limit]
    return {
        "enabled": BURN_MEMORY,
        "ttl_sec": BURN_TTL_SEC,
        "total": len(BURNED),
        "stats": BURN_STATS,
        "ips": [{"ip": ip, "reason": r.get("reason", ""), "hits": r.get("hits", 1),
                 "age_sec": int(now - r.get("at", 0)),
                 "expires_in": max(0, int(BURN_TTL_SEC - (now - r.get("at", 0))))}
                for ip, r in rows],
    }


@app.post("/api/burned/clear")
async def api_burned_clear(request: Request):
    """Forget the burn list (or one IP via ?ip=...).

    Needed because burn is a heuristic: if the upstream ever resets its per-IP
    accounting, or an IP is blocklisted for the wrong reason, the operator has
    to be able to give it another chance without editing JSON by hand.
    """
    if not _check_relay_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    ip = request.query_params.get("ip", "").strip()
    if ip:
        existed = BURNED.pop(ip, None) is not None
        save_burned()
        return {"cleared": 1 if existed else 0, "ip": ip, "remaining": len(BURNED)}
    n = len(BURNED)
    BURNED.clear()
    save_burned()
    log.info("burn memory cleared by operator (%d IPs forgotten)", n)
    return {"cleared": n, "remaining": 0}


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
    headers = build_upstream_headers(request)
    timeout = 300.0 if stream else 120.0
    if stream:
        status, resp_headers, chunks = await relay_stream(payload, "chat/completions", headers, timeout)
        return StreamingResponse(chunks, status_code=status,
                                 media_type=resp_headers.get("content-type") or "text/event-stream")
    status, resp_headers, body = await relay(payload, "chat/completions", stream, headers, timeout)
    return Response(content=body, status_code=status,
                    media_type=resp_headers.get("content-type") or "application/json")


@app.post("/v1/messages")
async def anthropic_messages(request: Request):
    auth = request.headers.get("authorization") or request.headers.get("x-api-key", "")
    if RELAY_API_KEY and not (hmac.compare_digest(auth, f"Bearer {RELAY_API_KEY}")
                              or hmac.compare_digest(auth, RELAY_API_KEY)):
        return JSONResponse({"type": "error", "error": {"type": "authentication_error", "message": "invalid relay key"}}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"type": "error", "error": {"type": "invalid_request_error", "message": "invalid json"}}, status_code=400)
    STATS["requests"] += 1
    payload["model"] = await resolve_model(str(payload.get("model", "")))
    oai = anthropic_to_openai(payload)
    stream = oai["stream"]
    headers = build_upstream_headers(request)
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

DASHBOARD_HTML = open(os.path.join(os.path.dirname(__file__), "dashboard.html"), encoding="utf-8").read() \
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
