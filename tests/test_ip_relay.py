"""Tests for ip_relay core logic — no network, all mocked."""

import json
import asyncio

import httpx
import pytest
from fastapi.testclient import TestClient

import ip_relay


@pytest.fixture(autouse=True)
def fresh_state(monkeypatch):
    """Reset module-level state so tests are isolated."""
    ip_relay.pool["proxies"] = []
    ip_relay.pool["updated"] = 0.0
    ip_relay.cooldowns = {}
    ip_relay.direct_burned_until = 0.0
    ip_relay.stats = {"requests": 0, "rotations": 0, "lane_failures": 0}
    # deterministic, no direct lane in most tests
    monkeypatch.setattr(ip_relay, "DIRECT_LANE", False)
    monkeypatch.setattr(ip_relay, "UPSTREAM_API_KEY", "public")
    monkeypatch.setattr(ip_relay, "UPSTREAM_BASE_URL", "https://upstream.test/v1")
    yield


# ── unit tests ────────────────────────────────────────────────────

def test_strip_model_prefix():
    assert ip_relay.strip_model_prefix("ocr/deepseek-v4-flash-free") == "deepseek-v4-flash-free"
    assert ip_relay.strip_model_prefix("deepseek-v4-flash-free") == "deepseek-v4-flash-free"
    # multi-segment: only the leading provider prefix is stripped (matches 9router)
    assert ip_relay.strip_model_prefix("Ra/oc/deepseek-v4-flash-free") == "oc/deepseek-v4-flash-free"


def test_is_quota_429_detects_free_usage_limit():
    body = json.dumps({"error": {"type": "FreeUsageLimitError", "message": "free usage limit reached"}}).encode()
    assert ip_relay.is_quota_429(body, 429) is True


def test_is_quota_429_detects_rate_limit_text():
    body = json.dumps({"error": {"type": "rate_limit_error", "message": "Rate limit reached"}}).encode()
    assert ip_relay.is_quota_429(body, 429) is True


def test_is_quota_429_false_for_other_status():
    body = json.dumps({"error": {"message": "nope"}}).encode()
    assert ip_relay.is_quota_429(body, 400) is False


def test_is_quota_429_false_for_plain_429():
    body = json.dumps({"error": {"type": "other", "message": "slow down"}}).encode()
    assert ip_relay.is_quota_429(body, 429) is False


def test_mask_key():
    assert ip_relay.mask_key("public") == "public..."
    assert ip_relay.mask_key("") == "(none)"


# ── relay rotation (mocked upstream) ──────────────────────────────

class FakeResponse:
    def __init__(self, status_code, body=b"{}", headers=None):
        self.status_code = status_code
        self.content = body
        self.headers = headers or {"content-type": "application/json"}


def test_relay_rotates_on_quota_429(monkeypatch):
    """A 429 FreeUsageLimit on the first proxy should park it and try the next."""
    import ip_relay as ir

    calls = []
    burned_body = json.dumps({"error": {"type": "FreeUsageLimitError", "message": "quota"}}).encode()
    ok_body = json.dumps({"ok": True}).encode()

    def handler(request):
        proxy = request.headers.get("x-proxy-label", "?")
        calls.append((proxy, request.url.path))
        if proxy == "burned":
            return httpx.Response(429, json=json.loads(burned_body), headers={"content-type": "application/json"})
        return httpx.Response(200, json=json.loads(ok_body), headers={"content-type": "application/json"})

    # Route by proxy: inject a per-lane MockTransport by monkeypatching AsyncClient
    RealClient = httpx.AsyncClient  # captured BEFORE patch

    def _labeled_handler(request, label):
        request.headers["x-proxy-label"] = label
        return handler(request)

    def fake_async_client(**kwargs):
        proxy = kwargs.get("proxy", None)
        label = "burned" if proxy == "http://1.2.3.4:8080" else "good"
        kwargs.pop("proxy", None)
        kwargs["transport"] = httpx.MockTransport(lambda req: _labeled_handler(req, label))
        return RealClient(**kwargs)

    monkeypatch.setattr(ir.httpx, "AsyncClient", fake_async_client)
    ir.pool["proxies"] = ["1.2.3.4:8080", "5.6.7.8:8080"]
    ir.cooldowns = {}

    async def run():
        return await ir.relay(
            {"model": "x", "messages": []},
            "chat/completions",
            False,
            {"Content-Type": "application/json"},
            5,
        )

    status, headers, body = asyncio.run(run())
    assert status == 200
    # burned proxy should be in cooldown, good one not
    assert ir.cooldowns.get("1.2.3.4:8080", 0) > 0
    assert "5.6.7.8:8080" not in ir.cooldowns
    # both lanes were tried in order
    assert len(calls) == 2
    assert calls[0][0] == "burned"
    assert calls[1][0] == "good"


def test_relay_returns_429_when_all_burned(monkeypatch):
    import ip_relay as ir

    burned_body = json.dumps({"error": {"type": "FreeUsageLimitError", "message": "quota"}}).encode()

    def handler(request):
        return httpx.Response(429, json=json.loads(burned_body), headers={"content-type": "application/json"})

    RealClient = httpx.AsyncClient  # captured BEFORE patch

    def fake_async_client(**kwargs):
        kwargs.pop("proxy", None)
        kwargs["transport"] = httpx.MockTransport(handler)
        return RealClient(**kwargs)

    monkeypatch.setattr(ir.httpx, "AsyncClient", fake_async_client)
    ir.pool["proxies"] = ["1.2.3.4:8080", "5.6.7.8:8080"]

    async def run():
        return await ir.relay(
            {"model": "x", "messages": []},
            "chat/completions",
            False,
            {"Content-Type": "application/json"},
            5,
        )

    status, headers, body = asyncio.run(run())
    assert status == 429
    assert ir.cooldowns.get("1.2.3.4:8080", 0) > 0
    assert ir.cooldowns.get("5.6.7.8:8080", 0) > 0


def test_relay_warming_503_when_no_lanes():
    import ip_relay as ir
    ir.pool["proxies"] = []
    ir.direct_burned_until = 1e18  # direct parked

    async def run():
        return await ir.relay(
            {"model": "x", "messages": []},
            "chat/completions",
            False,
            {},
            5,
        )

    status, headers, body = asyncio.run(run())
    assert status == 503
    assert b"warming" in body


# ── FastAPI routes ────────────────────────────────────────────────

def test_relay_auth_required(monkeypatch):
    import ip_relay as ir
    monkeypatch.setattr(ir, "RELAY_API_KEY", "sekret")
    client = TestClient(ir.app)
    r = client.post("/v1/chat/completions", headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401
    # passes auth; no JSON body -> 400 invalid json (auth check happens first)
    r2 = client.post("/v1/chat/completions", headers={"Authorization": "Bearer sekret"})
    assert r2.status_code == 400


def test_healthz_shape(monkeypatch):
    import ip_relay as ir
    monkeypatch.setattr(ir, "RELAY_API_KEY", "")
    ir.pool["proxies"] = ["1.2.3.4:8080"]
    client = TestClient(ir.app)
    r = client.get("/healthz")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["pool"] == 1
    assert "stats" in data


def test_chat_completions_invalid_json(monkeypatch):
    import ip_relay as ir
    monkeypatch.setattr(ir, "RELAY_API_KEY", "")
    client = TestClient(ir.app)
    r = client.post("/v1/chat/completions", content=b"not-json", headers={"Content-Type": "application/json"})
    assert r.status_code == 400
