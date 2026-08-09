import json
import time

import pytest
from fastapi.testclient import TestClient

import ip_relay as ir


@pytest.fixture(autouse=True)
def reset(monkeypatch, tmp_path):
    monkeypatch.setattr(ir, "SETTINGS_FILE", str(tmp_path / "settings.json"))
    ir.POOL.lanes.clear()
    ir.POOL.candidates.clear()
    ir.POOL.priority_candidates.clear()
    ir.POOL.tried.clear()
    ir.STATS.update({"requests": 0, "failovers": 0, "lane_failures": 0,
                     "probes_ok": 0, "probes_burned": 0, "candidates_tested": 0})
    ir.apply_settings({"relay_api_key": ""}, persist=False)
    yield


# ── Lane scoring ─────────────────────────────────────────────────

def test_lane_scoring():
    ln = ir.Lane("1.2.3.4:80", "http")
    assert ln.score == 0.5
    ln.mark_ok(500)
    assert ln.score > 0.6
    assert ln.ok == 1
    assert ln.warm
    ln.mark_fail(burn=True)
    assert not ln.warm
    assert ln.parked_until > time.time()


def test_lane_url():
    assert ir.Lane("1.2.3.4:80", "http").url() == "http://1.2.3.4:80"
    assert ir.Lane("1.2.3.4:80", "socks5").url() == "socks5://1.2.3.4:80"
    assert ir.Lane("", "direct").url() is None


def test_valid_addr():
    assert ir._valid_addr("1.2.3.4:8080")
    assert not ir._valid_addr("1.2.3.4:99999")
    assert not ir._valid_addr("nope")
    assert not ir._valid_addr("999.1.1.1:80")
    assert not ir._valid_addr("1.2.3:80")


def test_pool_warm_ordering():
    a = ir.Lane("1.1.1.1:80", "http")
    a.score = 0.9
    a.lat_ms = 300
    b = ir.Lane("2.2.2.2:80", "http")
    b.score = 0.5
    b.lat_ms = 50
    c = ir.Lane("3.3.3.3:80", "http")
    c.parked_until = time.time() + 100
    ir.POOL.lanes = {x.addr: x for x in (a, b, c)}
    warm = ir.POOL.warm_lanes()
    # latency-first ranking: fast lane wins even at lower score; parked excluded
    assert [x.addr for x in warm] == ["2.2.2.2:80", "1.1.1.1:80"]


# ── relay failover ────────────────────────────────────────────────

def test_relay_failover_on_burn(monkeypatch):
    """First lane returns 429-quota -> burned -> second lane answers 200."""
    good = ir.Lane("2.2.2.2:80", "http")
    good.score = 0.5
    bad = ir.Lane("1.1.1.1:80", "http")
    bad.score = 0.9  # tried first
    ir.POOL.lanes = {x.addr: x for x in (good, bad)}

    calls = []
    quota = json.dumps({"error": {"type": "FreeUsageLimitError", "message": "quota"}}).encode()
    ok = json.dumps({"choices": [{"message": {"content": "hi"}, "finish_reason": "stop"}]}).encode()

    async def fake_attempt(lane, payload, path, headers, timeout):
        calls.append(lane.addr)
        if lane.addr == "1.1.1.1:80":
            lane.mark_fail(burn=True)
            return 429, {"content-type": "application/json"}, quota
        lane.mark_ok(100)
        return 200, {"content-type": "application/json"}, ok

    monkeypatch.setattr(ir, "_attempt", fake_attempt)
    import asyncio
    status, _, body = asyncio.run(ir.relay({"model": "m"}, "chat/completions", False, {}, 30))
    assert status == 200
    assert calls == ["1.1.1.1:80", "2.2.2.2:80"]   # failed over
    assert ir.STATS["failovers"] == 1


def test_relay_exhausted(monkeypatch):
    ir.POOL.lanes.clear()
    import asyncio
    status, _, body = asyncio.run(ir.relay({"model": "m"}, "chat/completions", False, {}, 30))
    assert status == 503
    assert b"rotator_exhausted" in body


def test_echo_proxy_burned(monkeypatch):
    """A 200 with reflected-garbage body must fail over, not be served."""
    good = ir.Lane("2.2.2.2:80", "http")
    good.score = 0.5
    echo = ir.Lane("1.1.1.1:80", "http")
    echo.score = 0.9
    ir.POOL.lanes = {x.addr: x for x in (good, echo)}
    ok = json.dumps({"choices": [{"message": {"content": "hi"}, "finish_reason": "stop"}]}).encode()
    garbage = b"REMOTE_ADDR = 1.2.3.4\r\nREQUEST_METHOD = POST\r\n"

    async def fake_attempt(lane, payload, path, headers, timeout):
        if lane.addr == "1.1.1.1:80":
            # simulate _attempt's echo-detection path
            if ir._looks_like_completion(garbage):
                lane.mark_ok(100)
                return 200, {"content-type": "text/plain"}, garbage
            lane.mark_fail(burn=True)
            return 502, {"content-type": "application/json"}, json.dumps(
                {"error": {"type": "lane_invalid"}}).encode()
        lane.mark_ok(100)
        return 200, {"content-type": "application/json"}, ok

    monkeypatch.setattr(ir, "_attempt", fake_attempt)
    import asyncio
    status, _, body = asyncio.run(ir.relay({"model": "m"}, "chat/completions", False, {}, 30))
    assert status == 200
    assert json.loads(body)["choices"][0]["message"]["content"] == "hi"
    assert not ir._looks_like_completion(garbage)
    assert ir._looks_like_completion(ok)


def test_is_quota_429():
    # opencode-style quota body
    body = json.dumps({"error": {"type": "FreeUsageLimitError", "message": "quota exceeded"}}).encode()
    assert ir.is_quota_429(body, 429)
    # generic provider 429 (Groq / SambaNova style)
    generic = json.dumps({"error": {"message": "Rate limit reached for org"}}).encode()
    assert ir.is_quota_429(generic, 429)
    # empty body 429 still counts
    assert ir.is_quota_429(b"", 429)
    # non-429 status is never a quota signal
    assert not ir.is_quota_429(body, 200)
    assert not ir.is_quota_429(b"not json", 200)


def test_masked_key_preservation():
    ir.apply_settings({"webshare_token": "secret_real_token"}, persist=False)
    client = TestClient(ir.app)
    # Post masked string
    r = client.post("/api/settings", json={"webshare_token": "secret..."})
    assert r.status_code == 200
    assert ir.WEBSHARE_TOKEN == "secret_real_token"
    # Post real new string
    r2 = client.post("/api/settings", json={"webshare_token": "brand_new_token"})
    assert r2.status_code == 200
    assert ir.WEBSHARE_TOKEN == "brand_new_token"


# ── model resolution ──────────────────────────────────────────────

def test_resolve_model_claude_alias(monkeypatch):
    models = json.dumps({"data": [{"id": "deepseek-v4-flash-free"}]}).encode()

    async def fake_models():
        return 200, "application/json", models

    monkeypatch.setattr(ir, "get_models_cached", fake_models)
    ir._FALLBACK_MODEL = None
    import asyncio
    out = asyncio.run(ir.resolve_model("claude-haiku-4-5-20251001"))
    assert out == "deepseek-v4-flash-free"
    out2 = asyncio.run(ir.resolve_model("deepseek-v4-flash-free"))
    assert out2 == "deepseek-v4-flash-free"


# ── anthropic translation (ported, regression-guarded) ────────────

def test_anthropic_to_openai_basic():
    out = ir.anthropic_to_openai({
        "model": "ocr/deepseek-v4-flash-free", "max_tokens": 50,
        "system": "You are terse.",
        "messages": [{"role": "user", "content": "hi"}],
    })
    assert out["model"] == "deepseek-v4-flash-free"
    assert out["max_tokens"] == 50
    assert out["messages"][0] == {"role": "system", "content": "You are terse."}
    assert out["messages"][1] == {"role": "user", "content": "hi"}


def test_tool_call_roundtrip():
    out = ir.anthropic_to_openai({
        "model": "m", "max_tokens": 10,
        "messages": [{"role": "user", "content": "weather?"}],
        "tools": [{"name": "get_weather", "description": "w",
                   "input_schema": {"type": "object", "properties": {"city": {"type": "string"}}}}],
        "tool_choice": {"type": "auto"},
    })
    assert out["tools"][0]["function"]["name"] == "get_weather"
    assert out["tool_choice"] == "auto"

    body = json.dumps({
        "id": "x",
        "choices": [{"finish_reason": "tool_calls", "message": {
            "role": "assistant", "content": "",
            "tool_calls": [{"id": "call_1", "type": "function",
                            "function": {"name": "get_weather", "arguments": '{"city":"Paris"}'}}]}}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1},
    }).encode()
    d = json.loads(ir.openai_to_anthropic(body, "m"))
    assert d["stop_reason"] == "tool_use"
    assert d["content"][0]["type"] == "tool_use"
    assert d["content"][0]["input"] == {"city": "Paris"}

    out2 = ir.anthropic_to_openai({
        "model": "m",
        "messages": [
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": "call_1", "name": "get_weather", "input": {"city": "Paris"}}]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "call_1", "content": "sunny"}]},
        ],
    })
    assert out2["messages"][0]["tool_calls"][0]["function"]["name"] == "get_weather"
    assert out2["messages"][1] == {"role": "tool", "tool_call_id": "call_1", "content": "sunny"}


def test_openai_sse_to_anthropic_tool_call():
    body = (
        'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"get_weather","arguments":"{\\"city\\""}}]}}]}\n'
        'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":":\\"Paris\\"}"}}]}}]}\n'
        'data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}\n'
        "data: [DONE]\n"
    ).encode()
    out = ir.openai_sse_to_anthropic(body, "m").decode()
    assert '"type": "tool_use", "id": "call_1", "name": "get_weather"' in out
    assert '"stop_reason": "tool_use"' in out


# ── endpoints ─────────────────────────────────────────────────────

def test_healthz():
    client = TestClient(ir.app)
    r = client.get("/healthz")
    assert r.status_code == 200
    assert "warm" in r.json()
    assert r.json()["version"] == ir.VERSION


def test_stats_and_pool_apis():
    ln = ir.Lane("1.2.3.4:80", "http")
    ln.mark_ok(100)
    ir.POOL.lanes[ln.addr] = ln
    ir.POOL.candidates["http://9.9.9.9:80"] = time.time()
    client = TestClient(ir.app)
    s = client.get("/api/stats").json()
    assert s["pool"]["warm"] == 1
    assert s["pool"]["queue"] == 1
    p = client.get("/api/pool").json()
    assert p["warm"][0]["addr"] == "1.2.3.4:80"


def test_anthropic_messages_endpoint(monkeypatch):
    oai_body = json.dumps({
        "id": "x", "model": "m",
        "choices": [{"message": {"role": "assistant", "content": "hi there"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 5, "completion_tokens": 4},
    }).encode()

    async def fake_relay(payload, path, stream, headers, timeout):
        return 200, {"content-type": "application/json"}, oai_body

    async def fake_resolve(model):
        return "deepseek-v4-flash-free"

    monkeypatch.setattr(ir, "relay", fake_relay)
    monkeypatch.setattr(ir, "resolve_model", fake_resolve)
    client = TestClient(ir.app)
    r = client.post("/v1/messages", json={
        "model": "deepseek-v4-flash-free", "max_tokens": 10,
        "messages": [{"role": "user", "content": "hi"}],
    })
    assert r.status_code == 200
    d = r.json()
    assert d["type"] == "message"
    assert d["content"][0]["text"] == "hi there"


def test_dashboard_serves():
    client = TestClient(ir.app)
    r = client.get("/")
    assert r.status_code == 200
    assert "Egress lanes" in r.text
    assert "Connect your app" in r.text
