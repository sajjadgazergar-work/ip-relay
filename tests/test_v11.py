"""v1.1 tests: token meter, gzip-without-breaking-streams, /metrics gating.

The invariants worth protecting here are not "does it count" but:
  * REQ_LOG stays readable by pre-v1.1 5-tuple writers (the other test files
    append raw tuples — that must never break again),
  * a streamed response is passed through BYTE-IDENTICALLY while being metered,
  * one stream produces exactly ONE request row and ONE usage booking,
  * usage.json survives a restart and cannot grow without bound,
  * gzip never touches a streaming response.
"""
import asyncio
import json
import time

import pytest
from fastapi.testclient import TestClient

import ip_relay as ir


@pytest.fixture(autouse=True)
def reset(monkeypatch, tmp_path):
    monkeypatch.setattr(ir, "SETTINGS_FILE", str(tmp_path / "settings.json"))
    monkeypatch.setattr(ir, "LANES_FILE", str(tmp_path / "lanes.json"))
    monkeypatch.setattr(ir, "BURNED_FILE", str(tmp_path / "burned.json"))
    monkeypatch.setattr(ir, "USAGE_FILE", str(tmp_path / "usage.json"))
    monkeypatch.setenv("RELAY_ALLOW_ANONYMOUS", "1")
    ir.BURNED.clear()
    ir.POOL.lanes.clear()
    ir.POOL.candidates.clear()
    ir.POOL.priority_candidates.clear()
    ir.REQ_LOG.clear()
    ir.USAGE["days"] = {}
    ir.USAGE["lifetime"] = {"in": 0, "out": 0, "requests": 0}
    ir.apply_settings({"relay_api_key": "", "metrics_require_auth": True},
                      persist=False)
    yield


# ══════════════════════════════════════════════════════════════════
# usage extraction — non-stream, stream, and the malformed cases
# ══════════════════════════════════════════════════════════════════

def test_extract_usage_from_nonstream_body():
    body = json.dumps({"choices": [{"message": {"content": "hi"}}],
                       "usage": {"prompt_tokens": 89, "completion_tokens": 20,
                                 "total_tokens": 109}}).encode()
    assert ir.extract_usage(body) == (89, 20)


def test_extract_usage_from_sse_stream():
    """Shape measured live against opencode zen: usage rides on the final
    frames even though include_usage was never requested, and a non-standard
    {"choices":[],"cost":"0"} frame trails [DONE]."""
    body = (b'data: {"choices":[{"delta":{"content":"he"}}]}\n\n'
            b'data: {"choices":[{"delta":{"content":"llo"}}],'
            b'"usage":{"prompt_tokens":87,"completion_tokens":20,"total_tokens":107}}\n\n'
            b'data: {"choices":[],"usage":{"prompt_tokens":87,"completion_tokens":20}}\n\n'
            b'data: [DONE]\n\n'
            b'data: {"choices":[],"cost":"0"}\n\n')
    assert ir.extract_usage(body) == (87, 20)


def test_extract_usage_is_never_fatal():
    # A token meter must not be able to break a relay: every malformed shape
    # degrades to (0, 0).
    assert ir.extract_usage(b"") == (0, 0)
    assert ir.extract_usage(b"<html>captive portal</html>") == (0, 0)
    assert ir.extract_usage(b"data: {not json\n\n") == (0, 0)
    assert ir.extract_usage(b'data: {"choices":[]}\n\n') == (0, 0)
    assert ir.extract_usage(b"\xff\xfe\x00binary") == (0, 0)


def test_extract_usage_prefers_the_last_frame():
    # Providers send cumulative usage; the final frame is authoritative.
    body = (b'data: {"usage":{"prompt_tokens":10,"completion_tokens":1}}\n\n'
            b'data: {"usage":{"prompt_tokens":10,"completion_tokens":99}}\n\n'
            b'data: [DONE]\n\n')
    assert ir.extract_usage(body) == (10, 99)


# ══════════════════════════════════════════════════════════════════
# REQ_LOG backward compatibility — the landmine from the analysis
# ══════════════════════════════════════════════════════════════════

def test_req_log_readers_tolerate_pre_v11_rows():
    """Other test modules append raw 5-tuples. Readers must not unpack blindly."""
    ir.REQ_LOG.append((time.time(), "1.1.1.1:80", 200, 120.0, False))     # old shape
    ir.record_request("2.2.2.2:80", 200, 90.0, False, 50, 7)              # new shape
    w = ir.metrics_window(300)
    assert w["requests"] == 2
    assert w["tokens_in"] == 50 and w["tokens_out"] == 7
    spark = ir.sparkline_series()
    assert sum(spark["requests"]) == 2
    assert sum(spark["tokens"]) == 57


def test_record_request_returns_a_mutable_row():
    row = ir.record_request("1.1.1.1:80", 200, 10.0, True)
    assert isinstance(row, list)          # streams fill tokens in later
    row[5], row[6] = 3, 4
    assert ir.metrics_window(300)["tokens"] == 7


def test_record_request_books_usage_once():
    ir.record_request("1.1.1.1:80", 200, 10.0, False, 100, 25)
    assert ir.USAGE["lifetime"] == {"in": 100, "out": 25, "requests": 1}


def test_zero_token_requests_do_not_create_usage_rows():
    # A 429 or a failed lane must not inflate the "requests" count in the meter.
    ir.record_request("1.1.1.1:80", 429, 10.0, False)
    assert ir.USAGE["lifetime"]["requests"] == 0


# ══════════════════════════════════════════════════════════════════
# daily buckets, rollover, pruning, persistence
# ══════════════════════════════════════════════════════════════════

def test_usage_summary_splits_today_week_lifetime():
    today = ir._usage_day()
    old = ir._usage_day(time.time() - 10 * 86400)
    week = ir._usage_day(time.time() - 3 * 86400)
    ir.USAGE["days"] = {today: {"in": 10, "out": 5, "requests": 1},
                        week: {"in": 100, "out": 50, "requests": 2},
                        old: {"in": 1000, "out": 500, "requests": 3}}
    ir.USAGE["lifetime"] = {"in": 1110, "out": 555, "requests": 6}
    s = ir.usage_summary()
    assert s["today"]["total"] == 15
    assert s["week"]["total"] == 165          # today + 3d ago, NOT the 10d one
    assert s["lifetime"]["total"] == 1665
    assert len(s["series"]) == 3


def test_usage_prune_caps_history():
    ir.USAGE["days"] = {ir._usage_day(time.time() - d * 86400): {"in": 1, "out": 1, "requests": 1}
                        for d in range(0, 45)}
    dropped = ir.usage_prune()
    assert dropped > 0
    assert len(ir.USAGE["days"]) <= ir.USAGE_KEEP_DAYS + 1
    # lifetime totals are NOT pruned — history rolls off, the total does not
    assert "lifetime" in ir.USAGE


def test_usage_survives_a_restart():
    ir.record_request("1.1.1.1:80", 200, 10.0, False, 300, 120)
    assert ir.save_usage() is True
    ir.USAGE["days"] = {}
    ir.USAGE["lifetime"] = {"in": 0, "out": 0, "requests": 0}
    assert ir.load_usage() is True
    assert ir.USAGE["lifetime"] == {"in": 300, "out": 120, "requests": 1}
    assert ir.usage_summary()["today"]["total"] == 420


def test_load_usage_tolerates_a_corrupt_file(tmp_path, monkeypatch):
    p = tmp_path / "broken.json"
    p.write_text("{not json at all")
    monkeypatch.setattr(ir, "USAGE_FILE", str(p))
    assert ir.load_usage() is False        # starts fresh instead of crashing
    p.write_text(json.dumps({"days": {"2020-01-01": "not-a-dict"}, "lifetime": {}}))
    assert ir.load_usage() is True
    assert ir.USAGE["lifetime"] == {"in": 0, "out": 0, "requests": 0}


# ══════════════════════════════════════════════════════════════════
# per-lane tokens + lanes.json forward/backward compatibility
# ══════════════════════════════════════════════════════════════════

def test_lane_tokens_round_trip():
    ln = ir.Lane("1.2.3.4:8080", "http")
    ln.add_tokens(120, 30)
    ln.add_tokens(5, 1)
    assert (ln.tok_in, ln.tok_out) == (125, 31)
    back = ir.Lane.from_dict(ln.to_dict())
    assert (back.tok_in, back.tok_out) == (125, 31)


def test_lane_from_pre_v11_dict_defaults_tokens():
    """Existing lanes.json files have no token columns — they must still load."""
    old = {"addr": "9.9.9.9:80", "proto": "http", "score": 0.9,
           "lat_ms": 300.0, "ok": 4, "fails": 0, "last_ok": time.time()}
    ln = ir.Lane.from_dict(old)
    assert (ln.tok_in, ln.tok_out) == (0, 0)
    assert ln.score == 0.9


# ══════════════════════════════════════════════════════════════════
# streaming: byte-identity + exactly-once accounting
# ══════════════════════════════════════════════════════════════════

STREAM_FRAMES = [
    b'data: {"id":"1","choices":[{"delta":{"content":"he"}}]}\n\n',
    b'data: {"id":"1","choices":[{"delta":{"content":"llo"}}]}\n\n',
    b'data: {"id":"1","choices":[{"delta":{},"finish_reason":"stop"}],'
    b'"usage":{"prompt_tokens":40,"completion_tokens":9,"total_tokens":49}}\n\n',
    b'data: [DONE]\n\n',
]


class _FakeStreamResponse:
    """Minimal httpx.Response stand-in for the streaming path."""
    def __init__(self, frames):
        self.status_code = 200
        self.headers = {"content-type": "text/event-stream"}
        self._frames = list(frames)
        self.closed = False

    async def aiter_bytes(self):
        for f in self._frames:
            yield f

    async def aclose(self):
        self.closed = True


class _FakeClient:
    def __init__(self, frames):
        self._frames = frames
        self.closed = False

    def build_request(self, *a, **k):
        return object()

    async def send(self, *a, **k):
        return _FakeStreamResponse(self._frames)

    async def aclose(self):
        self.closed = True


def _run_stream(monkeypatch, frames):
    """Drive relay_stream and drain its generator INSIDE ONE event loop.

    This matters: asyncio.run() calls loop.shutdown_asyncgens() on the way out,
    which closes the suspended httpx aiter that relay_stream captured. Calling
    relay_stream in one asyncio.run() and draining in a second one therefore
    yields exactly one chunk and then silently stops — a test artifact that looks
    identical to a broken passthrough. Keep both in the same loop.
    """
    ln = ir.Lane("5.5.5.5:8080", "http")
    ln.score, ln.ok = 0.9, 3
    ln.mark_ok(100)
    ir.POOL.lanes["http://5.5.5.5:8080"] = ln
    monkeypatch.setattr(ir.httpx, "AsyncClient", lambda *a, **k: _FakeClient(frames))

    async def go():
        status, _headers, gen = await ir.relay_stream(
            {"model": "m", "stream": True}, "chat/completions", {}, 30)
        return status, [c async for c in gen]

    status, chunks = asyncio.run(go())
    return status, ln, chunks


def test_stream_is_passed_through_byte_identically(monkeypatch):
    status, _ln, chunks = _run_stream(monkeypatch, STREAM_FRAMES)
    assert status == 200
    # Not just "same total bytes" — same chunk boundaries too, so no consumer
    # can observe the meter.
    assert chunks == STREAM_FRAMES
    assert b"".join(chunks) == b"".join(STREAM_FRAMES)


def test_stream_meters_tokens_exactly_once(monkeypatch):
    _status, ln, _chunks = _run_stream(monkeypatch, STREAM_FRAMES)
    assert (ln.tok_in, ln.tok_out) == (40, 9)
    assert ir.USAGE["lifetime"] == {"in": 40, "out": 9, "requests": 1}
    # One stream = one REQ_LOG row, with tokens backfilled in place.
    rows = [r for r in ir.REQ_LOG if r[2] == 200]
    assert len(rows) == 1
    assert ir._row_tokens(rows[0]) == (40, 9)
    assert ir.metrics_window(300)["tokens"] == 49


def test_stream_without_usage_still_completes(monkeypatch):
    frames = [b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n', b'data: [DONE]\n\n']
    _status, ln, chunks = _run_stream(monkeypatch, frames)
    assert chunks == frames
    assert (ln.tok_in, ln.tok_out) == (0, 0)
    assert ir.USAGE["lifetime"]["requests"] == 0


def test_stream_releases_the_lane_and_closes_upstream(monkeypatch):
    _status, ln, _chunks = _run_stream(monkeypatch, STREAM_FRAMES)
    assert ln.inflight == 0        # the finally block ran


def test_usage_tail_is_bounded(monkeypatch):
    """A long completion must not be buffered in full just to read usage."""
    tail = ir.UsageTail(limit=512)
    for _ in range(200):
        tail.feed(b'data: {"choices":[{"delta":{"content":"padpadpadpad"}}]}\n\n')
    tail.feed(b'data: {"usage":{"prompt_tokens":7,"completion_tokens":3}}\n\n')
    assert len(tail.buf) <= 512
    assert tail.result() == (7, 3)     # still recovered from the tail window


def test_usage_tail_survives_a_frame_split_mid_window():
    tail = ir.UsageTail(limit=64)
    tail.feed(b'data: {"usage":{"prompt_tok')      # truncated leading frame
    tail.feed(b'ens":1,"completion_tokens":2}}\n\n')
    # The window now starts mid-frame; unparsable lines are skipped, so the
    # complete trailing frame is what counts.
    tail.feed(b'data: {"usage":{"prompt_tokens":11,"completion_tokens":22}}\n\n')
    assert tail.result() == (11, 22)


# ══════════════════════════════════════════════════════════════════
# Anthropic translation: output_tokens used to be hardcoded to 0
# ══════════════════════════════════════════════════════════════════

def test_sse_to_anthropic_reports_real_token_counts():
    out = ir.openai_sse_to_anthropic(b"".join(STREAM_FRAMES), "claude-x").decode()
    assert '"output_tokens": 9' in out
    assert '"input_tokens": 40' in out
    assert '"output_tokens": 0' not in out


def test_sse_to_anthropic_usage_on_a_choiceless_frame():
    """Usage often arrives on a frame with an EMPTY choices list — the old
    parser hit IndexError there and skipped it."""
    body = (b'data: {"choices":[{"delta":{"content":"x"}}]}\n\n'
            b'data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":6}}\n\n'
            b'data: [DONE]\n\n')
    out = ir.openai_sse_to_anthropic(body, "claude-x").decode()
    assert '"input_tokens": 5' in out and '"output_tokens": 6' in out
    assert '"text": "x"' in out          # content still translated


def test_json_to_anthropic_still_maps_usage():
    body = json.dumps({"choices": [{"message": {"content": "hi"}, "finish_reason": "stop"}],
                       "usage": {"prompt_tokens": 3, "completion_tokens": 4}}).encode()
    d = json.loads(ir.openai_to_anthropic(body, "claude-x"))
    assert d["usage"] == {"input_tokens": 3, "output_tokens": 4}


# ══════════════════════════════════════════════════════════════════
# /metrics gating (it now publishes free-tier consumption)
# ══════════════════════════════════════════════════════════════════

def test_metrics_requires_auth_by_default():
    ir.apply_settings({"relay_api_key": "secret", "metrics_require_auth": True},
                      persist=False)
    # Bare TestClient, NOT `with TestClient(...)`: entering the context manager
    # runs the lifespan, whose load_settings() re-reads the (tmp, empty) settings
    # file and wipes the key this test just set — so the request would arrive
    # with auth disabled and the assertion would fail for the wrong reason.
    c = TestClient(ir.app)
    assert c.get("/metrics").status_code == 401
    r = c.get("/metrics", headers={"Authorization": "Bearer secret"})
    assert r.status_code == 200
    assert "iprelay_tokens_in_total" in r.text


def test_metrics_accepts_the_key_as_a_query_param():
    # Prometheus cannot send an Authorization header in a plain static_config.
    ir.apply_settings({"relay_api_key": "secret", "metrics_require_auth": True},
                      persist=False)
    c = TestClient(ir.app)
    assert c.get("/metrics?key=secret").status_code == 200
    assert c.get("/metrics?key=wrong").status_code == 401


def test_metrics_can_be_opened_for_a_local_scraper():
    ir.apply_settings({"relay_api_key": "secret", "metrics_require_auth": False},
                      persist=False)
    c = TestClient(ir.app)
    assert c.get("/metrics").status_code == 200


def test_metrics_exposes_token_counters():
    ir.apply_settings({"relay_api_key": "", "metrics_require_auth": True}, persist=False)
    ir.record_request("1.1.1.1:80", 200, 10.0, False, 111, 22)
    with TestClient(ir.app) as c:
        text = c.get("/metrics").text
    assert "iprelay_tokens_in_total 111" in text
    assert "iprelay_tokens_out_total 22" in text
    assert "iprelay_tokens_today 133" in text


def test_api_stats_carries_the_usage_rollup():
    ir.record_request("1.1.1.1:80", 200, 10.0, False, 9, 1)
    with TestClient(ir.app) as c:
        d = c.get("/api/stats").json()
    assert d["usage"]["today"]["total"] == 10
    assert d["metrics"]["tokens"] == 10


# ══════════════════════════════════════════════════════════════════
# gzip must never touch a streamed response
# ══════════════════════════════════════════════════════════════════

def test_dashboard_is_gzipped():
    with TestClient(ir.app) as c:
        r = c.get("/", headers={"Accept-Encoding": "gzip"})
    assert r.status_code == 200
    # httpx transparently decodes; the header is the proof it was compressed.
    assert r.headers.get("content-encoding") == "gzip"
    assert "ip-relay" in r.text


def test_streaming_response_is_not_gzipped(monkeypatch):
    """The regression this middleware exists to avoid: starlette's GZipMiddleware
    coalesces SSE frames (measured 7 wire chunks -> 2), which would silently
    break token-by-token relaying."""
    ln = ir.Lane("5.5.5.5:8080", "http")
    ln.score, ln.ok = 0.9, 3
    ln.mark_ok(100)
    ir.POOL.lanes["http://5.5.5.5:8080"] = ln
    monkeypatch.setattr(ir.httpx, "AsyncClient", lambda *a, **k: _FakeClient(STREAM_FRAMES))
    with TestClient(ir.app) as c:
        r = c.post("/v1/chat/completions",
                   json={"model": "m", "stream": True},
                   headers={"Accept-Encoding": "gzip"})
    assert r.status_code == 200
    assert r.headers.get("content-encoding") != "gzip"
    assert b"".join(STREAM_FRAMES) in r.content


def test_small_json_is_not_gzipped():
    with TestClient(ir.app) as c:
        r = c.get("/healthz", headers={"Accept-Encoding": "gzip"})
    assert r.status_code == 200
    assert r.headers.get("content-encoding") != "gzip"
