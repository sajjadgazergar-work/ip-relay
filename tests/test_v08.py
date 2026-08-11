"""Tests for v0.7.1 hardening + v0.8 capacity/observability features.

Covers the paths that previously had zero coverage: quota-state backoff,
streaming failover, cheap revalidation, HMAC relay auth, and everything added
in v0.8 (persistence, per-lane concurrency, subnet diversity, adaptive
concurrency, latency tiers, metrics, SSE, diagnostics, settings validation).
"""
import asyncio
import json
import time

import httpx
import pytest
from fastapi.testclient import TestClient

import ip_relay as ir


@pytest.fixture(autouse=True)
def reset(monkeypatch, tmp_path):
    monkeypatch.setattr(ir, "SETTINGS_FILE", str(tmp_path / "settings.json"))
    monkeypatch.setattr(ir, "LANES_FILE", str(tmp_path / "lanes.json"))
    # load_settings() mints a relay key when none is set (default-deny); tests
    # that exercise the open-control-plane path opt out explicitly.
    monkeypatch.setenv("RELAY_ALLOW_ANONYMOUS", "1")
    ir.POOL.lanes.clear()
    ir.POOL.candidates.clear()
    ir.POOL.priority_candidates.clear()
    ir.POOL.tried.clear()
    ir.REQ_LOG.clear()
    ir.LOG_RING.clear()
    ir.EVENT_RING.clear()
    ir._subscribers.clear()
    ir._LAST_PROBE_ERR.clear()
    ir.STATS.update({"requests": 0, "failovers": 0, "lane_failures": 0,
                     "probes_ok": 0, "probes_burned": 0, "candidates_tested": 0,
                     "streams": 0, "upstream_429s": 0})
    ir.QUOTA_STATE.update({"exhausted": False, "backoff_sec": 90,
                           "backoff_until": 0.0, "announced": False})
    ir.ADAPT.update({"current": 20, "min": 5, "last_change": 0.0,
                     "last_reason": "init", "err_ratio": 0.0})
    ir.apply_settings({"relay_api_key": "", "lane_max_inflight": 2,
                       "max_lanes_per_subnet": 3, "adaptive_concurrency": True,
                       "persist_lanes": True, "proxy_test_concurrency": 60},
                      persist=False)
    yield


OK_BODY = json.dumps({"choices": [{"message": {"content": "hi"},
                                   "finish_reason": "stop"}]}).encode()
QUOTA_BODY = json.dumps({"error": {"type": "FreeUsageLimitError",
                                   "message": "quota exceeded"}}).encode()


# ══════════════════════════════════════════════════════════════════
# credential hygiene (the /api/logs leak fixed in v0.7.2)
# ══════════════════════════════════════════════════════════════════

def test_scrub_creds_removes_proxy_userinfo():
    line = "Prober: +Lane user1:pass2@198.105.121.200:6462 passed (900ms)"
    assert ir.scrub_creds(line) == "Prober: +Lane 198.105.121.200:6462 passed (900ms)"
    assert "pass2" not in ir.scrub_creds(line)


def test_scrub_creds_leaves_plain_addresses():
    assert ir.scrub_creds("lane 1.2.3.4:8080 ok") == "lane 1.2.3.4:8080 ok"


def test_log_ring_never_stores_credentials():
    # pytest's logging plugin raises the root level, so use WARNING to be sure
    # the record actually reaches the handler chain.
    ir.log.warning("Prober: +Lane secretuser:secretpass@9.9.9.9:1080 passed")
    joined = "\n".join(ir.LOG_RING)
    assert "secretpass" not in joined
    assert "9.9.9.9:1080" in joined


def test_display_addr_masks_userinfo():
    assert ir._display_addr("u:p@1.2.3.4:80") == "1.2.3.4:80"
    assert ir._display_addr("") == "direct"


def test_api_pool_never_leaks_credentials():
    ln = ir.Lane("someuser:somepass@5.5.5.5:3128", "http")
    ln.mark_ok(400)
    ir.POOL.lanes["http://someuser:somepass@5.5.5.5:3128"] = ln
    body = TestClient(ir.app).get("/api/pool").text
    assert "somepass" not in body
    assert "5.5.5.5:3128" in body


# ══════════════════════════════════════════════════════════════════
# quota state (v0.7.1, previously untested)
# ══════════════════════════════════════════════════════════════════

def test_quota_backoff_grows_and_caps():
    ir._note_upstream_429()
    assert ir.QUOTA_STATE["exhausted"] is True
    assert ir.QUOTA_STATE["backoff_sec"] == 180
    for _ in range(20):
        ir._note_upstream_429()
    assert ir.QUOTA_STATE["backoff_sec"] == 1800     # capped
    assert ir.STATS["upstream_429s"] == 21


def test_quota_ok_clears_backoff():
    ir._note_upstream_429()
    ir._note_quota_ok()
    # v0.8.1: a single success halves the backoff; full reset needs the
    # backoff to decay all the way to 30s (probe success) or a clean reset.
    assert ir.QUOTA_STATE["exhausted"] is True        # still backing off
    assert ir.QUOTA_STATE["backoff_sec"] == 90        # 180 // 2 (doubled to 180 first)
    assert ir.QUOTA_STATE["backoff_until"] > 0.0


def test_quota_ok_full_reset_after_decay():
    ir._note_upstream_429()                            # 180
    ir._note_quota_ok()                                # 90
    ir._note_quota_ok()                                # 45
    ir._note_quota_ok()                                # 30 -> exhausted False
    assert ir.QUOTA_STATE["exhausted"] is False
    assert ir.QUOTA_STATE["backoff_sec"] == 30
    assert ir.QUOTA_STATE["backoff_until"] == 0.0


def test_relay_success_clears_quota_flag(monkeypatch):
    ln = ir.Lane("1.1.1.1:80", "http")
    ln.mark_ok(100)
    ir.POOL.lanes["http://1.1.1.1:80"] = ln
    ir._note_upstream_429()

    async def fake_attempt(lane, payload, path, headers, timeout):
        lane.mark_ok(50)
        return 200, {"content-type": "application/json"}, OK_BODY

    monkeypatch.setattr(ir, "_attempt", fake_attempt)
    status, _, _ = asyncio.run(ir.relay({"model": "m"}, "chat/completions", False, {}, 30))
    assert status == 200
    # one 200 halves 180 -> 90; the flag survives until decay completes
    assert ir.QUOTA_STATE["exhausted"] is True
    assert ir.QUOTA_STATE["backoff_sec"] == 90


def test_quota_429_burns_the_lane(monkeypatch):
    """v0.8.1: a 429 from a lane means THAT lane's IP is burned — park it.
    Escalates to key-global only when enough distinct lanes 429 together."""
    ln = ir.Lane("1.1.1.1:80", "http")
    ln.mark_ok(100)
    ir.POOL.lanes["http://1.1.1.1:80"] = ln

    async def fake_attempt(lane, payload, path, headers, timeout):
        return 429, {"content-type": "application/json"}, QUOTA_BODY

    monkeypatch.setattr(ir, "_attempt", fake_attempt)
    status, _, _ = asyncio.run(ir.relay({"model": "m"}, "chat/completions", False, {}, 30))
    assert status == 429
    assert ln.parked_until > 0.0          # lane parked (burned IP)
    assert ir.QUOTA_STATE["exhausted"] is False   # not key-global yet (1 lane only)


def test_quota_escalates_after_three_distinct_lanes():
    """Three distinct lanes 429ing within the window = key-global quota."""
    ir.QUOTA_STATE["_429_window"] = []
    ir.QUOTA_STATE["_direct_429s"] = 0
    ir.QUOTA_STATE["exhausted"] = False
    ir.QUOTA_STATE["backoff_sec"] = 90
    ir._note_lane_429("1.1.1.1:80")
    ir._note_lane_429("2.2.2.2:80")
    assert ir.QUOTA_STATE["exhausted"] is False        # 2 distinct: not yet
    ir._note_lane_429("3.3.3.3:80")
    assert ir.QUOTA_STATE["exhausted"] is True         # 3 distinct: key-global
    assert ir.QUOTA_STATE["backoff_sec"] == 180


def test_quota_escalates_after_three_direct_429s():
    """The direct lane 429ing repeatedly = OUR key is dead, not a proxy."""
    ir.QUOTA_STATE["_429_window"] = []
    ir.QUOTA_STATE["_direct_429s"] = 0
    ir.QUOTA_STATE["exhausted"] = False
    ir.QUOTA_STATE["backoff_sec"] = 90
    ir._note_lane_429("")     # direct
    ir._note_lane_429("")     # direct
    assert ir.QUOTA_STATE["exhausted"] is False
    ir._note_lane_429("")     # direct
    assert ir.QUOTA_STATE["exhausted"] is True


# ══════════════════════════════════════════════════════════════════
# relay auth (hmac, v0.7.1)
# ══════════════════════════════════════════════════════════════════

def test_relay_auth_accepts_bearer_and_raw():
    ir.apply_settings({"relay_api_key": "topsecret"}, persist=False)
    c = TestClient(ir.app)
    assert c.get("/api/stats").status_code == 401
    assert c.get("/api/stats", headers={"Authorization": "Bearer topsecret"}).status_code == 200
    assert c.get("/api/stats", headers={"Authorization": "topsecret"}).status_code == 200
    assert c.get("/api/stats", headers={"Authorization": "Bearer wrong"}).status_code == 401


def test_relay_auth_open_when_key_empty():
    ir.apply_settings({"relay_api_key": ""}, persist=False)
    assert TestClient(ir.app).get("/api/stats").status_code == 200


def test_anthropic_endpoint_requires_key():
    ir.apply_settings({"relay_api_key": "k"}, persist=False)
    r = TestClient(ir.app).post("/v1/messages", json={"model": "m", "messages": []})
    assert r.status_code == 401
    assert r.json()["error"]["type"] == "authentication_error"


# ══════════════════════════════════════════════════════════════════
# per-lane concurrency (v0.8)
# ══════════════════════════════════════════════════════════════════

def test_at_capacity_and_available_lanes():
    ir.apply_settings({"lane_max_inflight": 2}, persist=False)
    ln = ir.Lane("1.1.1.1:80", "http")
    ln.mark_ok(100)
    ir.POOL.lanes["http://1.1.1.1:80"] = ln
    assert ir.POOL.available_lanes() == [ln]
    ln.inflight = 2
    assert ln.at_capacity
    assert ir.POOL.available_lanes() == []
    assert ir.POOL.warm_lanes() == [ln]      # still warm, just busy


def test_concurrent_requests_spread_across_lanes(monkeypatch):
    """With lane_max_inflight=1, four concurrent requests must touch four lanes."""
    ir.apply_settings({"lane_max_inflight": 1}, persist=False)
    for i in range(4):
        ln = ir.Lane(f"10.0.0.{i}:80", "http")
        ln.mark_ok(100 + i)
        ir.POOL.lanes[f"http://10.0.0.{i}:80"] = ln

    seen: list[str] = []

    async def fake_attempt(lane, payload, path, headers, timeout):
        seen.append(lane.addr)
        await asyncio.sleep(0.05)          # hold the lane
        lane.mark_ok(50)
        return 200, {"content-type": "application/json"}, OK_BODY

    monkeypatch.setattr(ir, "_attempt", fake_attempt)

    async def run():
        return await asyncio.gather(*[
            ir.relay({"model": "m"}, "chat/completions", False, {}, 30) for _ in range(4)])

    results = asyncio.run(run())
    assert all(r[0] == 200 for r in results)
    assert len(set(seen)) == 4, f"requests stacked onto {len(set(seen))} lane(s): {seen}"


def test_inflight_released_after_request(monkeypatch):
    ln = ir.Lane("1.1.1.1:80", "http")
    ln.mark_ok(100)
    ir.POOL.lanes["http://1.1.1.1:80"] = ln

    async def fake_attempt(lane, payload, path, headers, timeout):
        assert lane.inflight == 1
        return 200, {"content-type": "application/json"}, OK_BODY

    monkeypatch.setattr(ir, "_attempt", fake_attempt)
    asyncio.run(ir.relay({"model": "m"}, "chat/completions", False, {}, 30))
    assert ln.inflight == 0


def test_inflight_released_on_exception(monkeypatch):
    ln = ir.Lane("1.1.1.1:80", "http")
    ln.mark_ok(100)
    ir.POOL.lanes["http://1.1.1.1:80"] = ln

    async def boom(lane, payload, path, headers, timeout):
        raise httpx.ConnectError("dead")

    monkeypatch.setattr(ir, "_attempt", boom)
    asyncio.run(ir.relay({"model": "m"}, "chat/completions", False, {}, 5))
    assert ln.inflight == 0


# ══════════════════════════════════════════════════════════════════
# subnet diversity + latency tiers (v0.8)
# ══════════════════════════════════════════════════════════════════

def test_subnet_extraction():
    assert ir.Lane("1.2.3.4:80", "http").subnet == "1.2.3"
    assert ir.Lane("u:p@1.2.3.4:80", "http").subnet == "1.2.3"
    assert ir.Lane("", "direct").subnet == "direct"
    assert ir._key_subnet("http://u:p@8.8.8.8:3128") == "8.8.8"


def test_subnet_full_respects_cap():
    ir.apply_settings({"max_lanes_per_subnet": 2}, persist=False)
    for i in (1, 2):
        ln = ir.Lane(f"7.7.7.{i}:80", "http")
        ln.mark_ok(100)
        ir.POOL.lanes[f"http://7.7.7.{i}:80"] = ln
    assert ir.POOL.subnet_full("7.7.7")
    assert not ir.POOL.subnet_full("8.8.8")
    assert not ir.POOL.subnet_full("direct")


def test_subnet_cap_disabled_when_zero():
    ir.apply_settings({"max_lanes_per_subnet": 0}, persist=False)
    for i in range(5):
        ln = ir.Lane(f"7.7.7.{i}:80", "http")
        ln.mark_ok(100)
        ir.POOL.lanes[f"http://7.7.7.{i}:80"] = ln
    assert not ir.POOL.subnet_full("7.7.7")


def test_stats_reports_unique_subnets_not_lane_count():
    for i in range(4):
        ln = ir.Lane(f"9.9.9.{i}:80", "http")   # all one /24
        ln.mark_ok(100)
        ir.POOL.lanes[f"http://9.9.9.{i}:80"] = ln
    s = ir.POOL.stats()
    assert s["warm"] == 4
    assert s["subnets"] == 1        # the honest capacity number


def test_latency_tiers():
    fast, med, slow = ir.Lane("1.1.1.1:80", "http"), ir.Lane("2.2.2.2:80", "http"), ir.Lane("3.3.3.3:80", "http")
    fast.mark_ok(800)
    med.mark_ok(2500)
    slow.mark_ok(9000)
    assert (fast.tier, med.tier, slow.tier) == ("fast", "medium", "slow")
    assert ir.Lane("4.4.4.4:80", "http").tier == "medium"   # unknown latency


def test_available_lanes_tier_filter_falls_back():
    slow = ir.Lane("3.3.3.3:80", "http")
    slow.mark_ok(9000)
    ir.POOL.lanes["http://3.3.3.3:80"] = slow
    # no fast lane exists -> must not return an empty list
    assert ir.POOL.available_lanes(tier="fast") == [slow]


# ══════════════════════════════════════════════════════════════════
# adaptive concurrency (v0.8)
# ══════════════════════════════════════════════════════════════════

def test_adaptive_halves_on_connect_storm():
    ir.ADAPT["current"] = 40
    ir._adapt_concurrency(conn_errors=90, completed=100)
    assert ir.ADAPT["current"] == 20
    assert "saturated" in ir.ADAPT["last_reason"]


def test_adaptive_grows_on_clean_link():
    ir.apply_settings({"proxy_test_concurrency": 60}, persist=False)
    ir.ADAPT["current"] = 20
    ir._adapt_concurrency(conn_errors=1, completed=100)
    assert ir.ADAPT["current"] == 25


def test_adaptive_never_exceeds_ceiling():
    ir.apply_settings({"proxy_test_concurrency": 22}, persist=False)
    ir.ADAPT["current"] = 22
    ir._adapt_concurrency(conn_errors=0, completed=100)
    assert ir.ADAPT["current"] == 22


def test_adaptive_respects_floor():
    ir.ADAPT["current"] = 5
    ir._adapt_concurrency(conn_errors=100, completed=100)
    assert ir.ADAPT["current"] == 5


def test_adaptive_ignores_http_errors():
    """Dead proxies (HTTP errors) are not a link problem — don't back off."""
    ir.ADAPT["current"] = 40
    ir._adapt_concurrency(conn_errors=0, completed=100)   # all failed on HTTP
    assert ir.ADAPT["current"] == 50                      # scaled UP


def test_adaptive_disabled_pins_to_configured_value():
    ir.apply_settings({"adaptive_concurrency": False, "proxy_test_concurrency": 33}, persist=False)
    assert ir.ADAPT["current"] == 33
    ir._adapt_concurrency(conn_errors=100, completed=100)
    assert ir.ADAPT["current"] == 33


# ══════════════════════════════════════════════════════════════════
# lane persistence (v0.8)
# ══════════════════════════════════════════════════════════════════

def test_lane_roundtrip_serialization():
    ln = ir.Lane("1.2.3.4:8080", "socks5")
    ln.mark_ok(1234)
    back = ir.Lane.from_dict(ln.to_dict())
    assert (back.addr, back.proto, back.ok) == ("1.2.3.4:8080", "socks5", 1)
    assert back.score == pytest.approx(ln.score, abs=1e-3)


def test_save_and_load_lanes_roundtrip():
    good = ir.Lane("1.1.1.1:80", "http")
    good.mark_ok(500)
    weak = ir.Lane("2.2.2.2:80", "http")
    weak.score = 0.1                      # below the 0.25 keep threshold
    ir.POOL.lanes = {"http://1.1.1.1:80": good, "http://2.2.2.2:80": weak}
    assert ir.save_lanes() == 1

    ir.POOL.lanes.clear()
    assert ir.load_lanes() == 1
    assert "http://1.1.1.1:80" in ir.POOL.priority_candidates
    assert "http://2.2.2.2:80" not in ir.POOL.priority_candidates


def test_load_lanes_ignores_stale_file(tmp_path, monkeypatch):
    path = tmp_path / "old.json"
    path.write_text(json.dumps({"saved": time.time() - 200000,
                                "lanes": [{"addr": "1.1.1.1:80", "proto": "http", "score": 0.9}]}))
    monkeypatch.setattr(ir, "LANES_FILE", str(path))
    assert ir.load_lanes() == 0
    assert not ir.POOL.priority_candidates


def test_load_lanes_survives_corrupt_file(tmp_path, monkeypatch):
    path = tmp_path / "bad.json"
    path.write_text("{not json at all")
    monkeypatch.setattr(ir, "LANES_FILE", str(path))
    assert ir.load_lanes() == 0


def test_persistence_can_be_disabled():
    ir.apply_settings({"persist_lanes": False}, persist=False)
    ln = ir.Lane("1.1.1.1:80", "http")
    ln.mark_ok(100)
    ir.POOL.lanes["http://1.1.1.1:80"] = ln
    assert ir.save_lanes() == 0
    assert ir.load_lanes() == 0


def test_direct_lane_is_not_persisted():
    d = ir.Lane("", "direct")
    d.score = 1.0
    ir.POOL.lanes["direct://"] = d
    assert ir.save_lanes() == 0


# ══════════════════════════════════════════════════════════════════
# metrics window + sparklines (v0.8)
# ══════════════════════════════════════════════════════════════════

def test_metrics_window_percentiles():
    for ms in (100, 200, 300, 400, 500):
        ir.record_request("1.1.1.1:80", 200, ms)
    ir.record_request("1.1.1.1:80", 503, 900)
    m = ir.metrics_window(300)
    assert m["requests"] == 6
    assert m["ok"] == 5
    assert m["errors"] == 1
    assert m["p50_ms"] == 300
    assert m["p95_ms"] == 500
    assert m["success_rate"] == pytest.approx(5 / 6, abs=1e-3)


def test_metrics_window_excludes_old_rows():
    ir.REQ_LOG.append((time.time() - 4000, "old", 200, 100, False))
    ir.record_request("new", 200, 200)
    assert ir.metrics_window(300)["requests"] == 1
    assert ir.metrics_window(7200)["requests"] == 2


def test_metrics_window_empty_is_safe():
    m = ir.metrics_window(300)
    assert m["requests"] == 0 and m["success_rate"] is None and m["p95_ms"] == 0


def test_sparkline_shape():
    for _ in range(3):
        ir.record_request("1.1.1.1:80", 200, 150)
    sp = ir.sparkline_series(buckets=10, bucket_sec=2)
    assert len(sp["requests"]) == 10 and len(sp["p95_ms"]) == 10
    assert sp["requests"][-1] == 3          # newest bucket


def test_relay_records_metrics(monkeypatch):
    ln = ir.Lane("1.1.1.1:80", "http")
    ln.mark_ok(100)
    ir.POOL.lanes["http://1.1.1.1:80"] = ln

    async def fake_attempt(lane, payload, path, headers, timeout):
        return 200, {"content-type": "application/json"}, OK_BODY

    monkeypatch.setattr(ir, "_attempt", fake_attempt)
    asyncio.run(ir.relay({"model": "m"}, "chat/completions", False, {}, 30))
    assert ir.metrics_window(60)["requests"] == 1


def test_exhausted_relay_is_recorded():
    asyncio.run(ir.relay({"model": "m"}, "chat/completions", False, {}, 5))
    m = ir.metrics_window(60)
    assert m["requests"] == 1 and m["errors"] == 1


# ══════════════════════════════════════════════════════════════════
# event bus / SSE (v0.8)
# ══════════════════════════════════════════════════════════════════

def test_publish_reaches_subscribers():
    q: asyncio.Queue = asyncio.Queue(maxsize=10)
    ir._subscribers.add(q)
    ir.publish("lane_up", {"addr": "1.1.1.1:80"})
    evt = q.get_nowait()
    assert evt["kind"] == "lane_up" and evt["addr"] == "1.1.1.1:80"
    assert ir.EVENT_RING[-1]["kind"] == "lane_up"


def test_publish_survives_full_subscriber_queue():
    q: asyncio.Queue = asyncio.Queue(maxsize=1)
    ir._subscribers.add(q)
    ir.publish("a", {})
    ir.publish("b", {})     # must not raise
    assert q.qsize() == 1


def test_lane_down_event_only_on_warm_to_parked():
    ln = ir.Lane("1.1.1.1:80", "http")
    ln.mark_ok(100)
    ir.EVENT_RING.clear()
    ln.mark_fail(burn=True)
    assert [e["kind"] for e in ir.EVENT_RING] == ["lane_down"]
    ln.mark_fail(burn=True)          # already parked — no duplicate event
    assert [e["kind"] for e in ir.EVENT_RING] == ["lane_down"]


def test_sse_endpoint_streams_hello():
    """Drive the SSE generator directly: TestClient's sync stream would block
    on the 15s keepalive wait, which makes the suite slow for no extra signal."""
    class FakeReq:
        headers: dict = {}

    async def run():
        resp = await ir.api_events(FakeReq())          # type: ignore[arg-type]
        assert resp.media_type == "text/event-stream"
        agen = resp.body_iterator
        first = await asyncio.wait_for(agen.__anext__(), timeout=5)
        await agen.aclose()
        return first

    first = asyncio.run(run())
    payload = json.loads(first.split("data: ", 1)[1])
    assert payload["kind"] == "hello"
    assert "pool" in payload and payload["version"] == ir.VERSION
    assert not ir._subscribers, "subscriber queue must be released on disconnect"


def test_sse_requires_auth():
    ir.apply_settings({"relay_api_key": "k"}, persist=False)
    assert TestClient(ir.app).get("/api/events").status_code == 401


# ══════════════════════════════════════════════════════════════════
# settings validation (v0.8)
# ══════════════════════════════════════════════════════════════════

def test_validate_rejects_out_of_range():
    updates, errors = ir.validate_settings_payload({"relay_attempts": 999})
    assert not updates and errors and "between" in errors[0]


def test_validate_rejects_non_numeric():
    _, errors = ir.validate_settings_payload({"lane_cooldown_sec": "abc"})
    assert errors and "number" in errors[0]


def test_validate_rejects_schemeless_url():
    _, errors = ir.validate_settings_payload({"upstream_base_url": "opencode.ai/zen/v1"})
    assert errors and "http" in errors[0]


def test_validate_skips_masked_secrets():
    updates, errors = ir.validate_settings_payload({"upstream_api_key": "abc..."})
    assert updates == {} and errors == []


def test_validate_coerces_booleans():
    updates, _ = ir.validate_settings_payload({"direct_lane": "true", "allow_socks": "0"})
    assert updates["direct_lane"] is True and updates["allow_socks"] is False


def test_validate_ignores_unknown_keys():
    updates, errors = ir.validate_settings_payload({"nonsense": 1})
    assert updates == {} and errors == []


def test_post_settings_returns_400_with_details():
    r = TestClient(ir.app).post("/api/settings", json={"relay_attempts": 500})
    assert r.status_code == 400
    assert r.json()["details"]


def test_post_settings_applies_valid_values():
    r = TestClient(ir.app).post("/api/settings", json={"relay_attempts": 4})
    assert r.status_code == 200
    assert ir.RELAY_ATTEMPTS == 4


# ══════════════════════════════════════════════════════════════════
# provider profiles (v0.8)
# ══════════════════════════════════════════════════════════════════

def test_profiles_listed():
    body = TestClient(ir.app).get("/api/profiles").json()
    ids = {p["id"] for p in body["profiles"]}
    assert {"opencode-zen", "groq", "generic-openai"} <= ids


def test_apply_profile_sets_upstream():
    r = TestClient(ir.app).post("/api/profiles/groq")
    assert r.status_code == 200
    assert ir.UPSTREAM_BASE_URL == "https://api.groq.com/openai/v1"
    assert ir.PROBE_MODEL == "llama-3.3-70b-versatile"


def test_apply_profile_keeps_existing_key_when_profile_has_none():
    ir.apply_settings({"upstream_api_key": "my-own-key"}, persist=False)
    TestClient(ir.app).post("/api/profiles/groq")
    assert ir.UPSTREAM_API_KEY == "my-own-key"


def test_apply_unknown_profile_404s():
    assert TestClient(ir.app).post("/api/profiles/nope").status_code == 404


# ══════════════════════════════════════════════════════════════════
# diagnostics + prometheus (v0.8)
# ══════════════════════════════════════════════════════════════════

def test_diagnostics_flags_egress_blocked():
    for _ in range(5):
        ir.LOG_RING.append("Prober: Lane 1.2.3.4:80 failed Stage 2 connection: ConnectError")
    d = TestClient(ir.app).get("/api/diagnostics").json()
    assert d["verdict"] == "egress_blocked"
    assert "concurrency" in d["advice"]
    assert d["probe_failures"]["connect_error"] == 5


def test_diagnostics_flags_bad_key():
    for _ in range(4):
        ir.LOG_RING.append("Prober: Lane 1.2.3.4:80 failed Stage 2: HTTP 401 (Response: nope)")
    assert TestClient(ir.app).get("/api/diagnostics").json()["verdict"] == "bad_upstream_key"


def test_diagnostics_warming_up_with_no_evidence():
    assert TestClient(ir.app).get("/api/diagnostics").json()["verdict"] == "warming_up"


def test_diagnostics_healthy_with_warm_lane():
    ln = ir.Lane("1.1.1.1:80", "http")
    ln.mark_ok(100)
    ir.POOL.lanes["http://1.1.1.1:80"] = ln
    assert TestClient(ir.app).get("/api/diagnostics").json()["verdict"] == "healthy"


def test_config_warnings_catch_open_control_plane_and_public_key():
    ir.apply_settings({"relay_api_key": "", "upstream_api_key": "public",
                       "proxy_test_concurrency": 60}, persist=False)
    w = " ".join(ir.config_warnings())
    assert "UNAUTHENTICATED" in w
    assert "'public'" in w
    assert "NAT table" in w


def test_config_warnings_quiet_when_sane():
    ir.apply_settings({"relay_api_key": "k", "upstream_api_key": "real-key",
                       "proxy_test_concurrency": 20, "lane_max_inflight": 2,
                       "upstream_base_url": "https://x/v1", "probe_model": "m"},
                      persist=False)
    assert ir.config_warnings() == []


def test_prometheus_exposition_shape():
    r = TestClient(ir.app).get("/metrics")
    assert r.status_code == 200
    assert "iprelay_warm_lanes" in r.text
    assert "iprelay_unique_subnets" in r.text
    assert "# TYPE iprelay_requests_total counter" in r.text


def test_prometheus_never_contains_credentials():
    ir.apply_settings({"upstream_api_key": "sk-supersecret"}, persist=False)
    ln = ir.Lane("user:pw@1.1.1.1:80", "http")
    ln.mark_ok(100)
    ir.POOL.lanes["http://user:pw@1.1.1.1:80"] = ln
    text = TestClient(ir.app).get("/metrics").text
    assert "sk-supersecret" not in text and "pw@" not in text


def test_api_metrics_windows():
    ir.record_request("1.1.1.1:80", 200, 120)
    body = TestClient(ir.app).get("/api/metrics").json()
    assert body["w60"]["requests"] == 1
    assert "spark" in body and "adaptive" in body


# ══════════════════════════════════════════════════════════════════
# pool pagination (v0.8)
# ══════════════════════════════════════════════════════════════════

def test_pool_pagination():
    for i in range(30):
        ln = ir.Lane(f"10.1.{i}.5:80", "http")
        ln.mark_ok(100 + i)
        ir.POOL.lanes[f"http://10.1.{i}.5:80"] = ln
    c = TestClient(ir.app)
    page = c.get("/api/pool?limit=10&offset=0").json()
    assert len(page["warm"]) == 10 and page["total_warm"] == 30
    page2 = c.get("/api/pool?limit=10&offset=10").json()
    assert page2["warm"][0]["addr"] != page["warm"][0]["addr"]


def test_pool_pagination_rejects_garbage_params():
    r = TestClient(ir.app).get("/api/pool?limit=abc&offset=xyz")
    assert r.status_code == 200 and r.json()["limit"] == 50


def test_pool_exposes_tier_and_capacity():
    ln = ir.Lane("1.1.1.1:80", "http")
    ln.mark_ok(700)
    ir.POOL.lanes["http://1.1.1.1:80"] = ln
    row = TestClient(ir.app).get("/api/pool").json()["warm"][0]
    assert row["tier"] == "fast" and row["capacity"] == ir.LANE_MAX_INFLIGHT
    assert row["subnet"] == "1.1.1"


# ══════════════════════════════════════════════════════════════════
# streaming failover (v0.7.1 behaviour, previously untested)
# ══════════════════════════════════════════════════════════════════

def test_relay_stream_exhausted_returns_sse_error():
    status, headers, chunks = asyncio.run(
        ir.relay_stream({"model": "m"}, "chat/completions", {}, 5))
    assert status == 503

    async def drain():
        return b"".join([c async for c in chunks])

    assert b"rotator_exhausted" in asyncio.run(drain())


def test_stream_one_yields_body():
    async def drain():
        return b"".join([c async for c in ir._stream_one(b"abc")])
    assert asyncio.run(drain()) == b"abc"


def test_looks_like_completion_accepts_sse_chunk():
    assert ir._looks_like_completion(b'data: {"id":"x","choices":[]}\n\n')
    assert not ir._looks_like_completion(b"<html>captive portal</html>")


# ══════════════════════════════════════════════════════════════════
# revalidation (v0.7.1, previously untested)
# ══════════════════════════════════════════════════════════════════

def test_revalidate_skips_recently_active_lanes(monkeypatch):
    ln = ir.Lane("1.1.1.1:80", "http")
    ln.mark_ok(100)                       # last_ok = now -> not due
    ir.POOL.lanes["http://1.1.1.1:80"] = ln
    called = []

    class Boom:
        def __init__(self, *a, **k):
            called.append(1)
        async def __aenter__(self):
            raise AssertionError("should not probe a fresh lane")
        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(ir.httpx, "AsyncClient", Boom)
    asyncio.run(ir._revalidate_warm())
    assert not called


def test_revalidate_burns_lane_on_failure(monkeypatch):
    ln = ir.Lane("1.1.1.1:80", "http")
    ln.mark_ok(100)
    ln.last_ok = time.time() - 600        # stale -> due
    ln.last_probe = time.time() - 600
    ir.POOL.lanes["http://1.1.1.1:80"] = ln

    class Dead:
        def __init__(self, *a, **k):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        async def get(self, *a, **k):
            raise httpx.ConnectError("gone")

    monkeypatch.setattr(ir.httpx, "AsyncClient", Dead)
    asyncio.run(ir._revalidate_warm())
    assert ln.parked_until > time.time()


# ══════════════════════════════════════════════════════════════════
# probe failure classification (feeds adaptive control + diagnostics)
# ══════════════════════════════════════════════════════════════════

def test_probe_records_connect_error_class(monkeypatch):
    """Stage-1 screen passes (proxy reachable); the full upstream post then
    hits a real ConnectError — that is genuine link/NAT exhaustion."""
    class Dead:
        def __init__(self, *a, **k):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        class _R:
            status_code = 200
            content = b"{}"
        async def get(self, *a, **k):
            return self._R()                # screen succeeds
        async def post(self, *a, **k):
            raise httpx.ConnectError("no route")

    monkeypatch.setattr(ir.httpx, "AsyncClient", Dead)
    out = asyncio.run(ir._probe_candidate("http://1.2.3.4:8080"))
    assert out is None
    # v0.8.1: ConnectError = true link/NAT exhaustion -> "conn" (shrinks adaptive)
    assert ir._LAST_PROBE_ERR["http://1.2.3.4:8080"] == "conn"


def test_probe_records_proxy_error_class(monkeypatch):
    class BrokenProxy:
        def __init__(self, *a, **k):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        class _R:
            status_code = 200
            content = b"{}"
        async def get(self, *a, **k):
            return self._R()                # screen succeeds
        async def post(self, *a, **k):
            raise httpx.ProxyError("tunnel failed")

    monkeypatch.setattr(ir.httpx, "AsyncClient", BrokenProxy)
    out = asyncio.run(ir._probe_candidate("http://1.2.3.4:8080"))
    assert out is None
    # v0.8.1: ProxyError = dead/rejecting proxy, NOT a link storm
    assert ir._LAST_PROBE_ERR["http://1.2.3.4:8080"] == "proxy"
