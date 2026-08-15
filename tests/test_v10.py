"""Tests for v1.0: burn memory, Tor egress provider, pin-and-drain routing.

These cover the three mechanisms added after measuring that (a) a 429'd egress
IP never recovers, and (b) a live Tor exit serves 40+ requests before it does
burn. The point of each test is stated in its docstring, because the behaviour
is counter-intuitive if you assume 429 is an ordinary rate limit.
"""
import json
import time

import pytest

import ip_relay as ir


@pytest.fixture(autouse=True)
def reset(monkeypatch, tmp_path):
    monkeypatch.setattr(ir, "SETTINGS_FILE", str(tmp_path / "settings.json"))
    monkeypatch.setattr(ir, "LANES_FILE", str(tmp_path / "lanes.json"))
    monkeypatch.setattr(ir, "BURNED_FILE", str(tmp_path / "burned.json"))
    monkeypatch.setenv("RELAY_ALLOW_ANONYMOUS", "1")
    ir.POOL.lanes.clear()
    ir.POOL.candidates.clear()
    ir.POOL.priority_candidates.clear()
    ir.POOL.tried.clear()
    ir.BURNED.clear()
    ir.BURN_STATS.update({"blocked_intake": 0, "blocked_probe": 0,
                          "recorded": 0, "expired": 0})
    ir.apply_settings({"relay_api_key": "", "burn_memory": True,
                       "burn_ttl_sec": 86400, "tor_enabled": False,
                       "tor_lanes": 12, "tor_socks_port": 9150,
                       "lane_pin_count": 3, "max_lanes_per_subnet": 3},
                      persist=False)
    yield
    ir.BURNED.clear()


# ══════════════════════════════════════════════════════════════════
# BURN MEMORY
# ══════════════════════════════════════════════════════════════════

def test_burn_is_keyed_on_ip_not_ip_port():
    """The quota follows the ADDRESS. The same host appears on many ports across
    the scrape feeds, so keying on ip:port would re-probe the same dead host
    once per port."""
    ir.mark_burned("5.5.5.5:8080")
    assert ir.is_burned("5.5.5.5:8080")
    assert ir.is_burned("5.5.5.5:3128")      # different port, same spent IP
    assert ir.is_burned("5.5.5.5")
    assert not ir.is_burned("5.5.5.6:8080")


def test_burn_ignores_credentials_in_addr():
    """'user:pass@ip:port' must resolve to the bare IP, or authenticated proxy
    lanes would never be recognised as burned."""
    ir.mark_burned("user:secret@7.7.7.7:1080")
    assert ir.is_burned("7.7.7.7:1080")
    assert ir.is_burned("other:creds@7.7.7.7:9999")


def test_burn_expires_after_ttl():
    ir.mark_burned("8.8.8.8:80")
    ir.BURNED["8.8.8.8"]["at"] = time.time() - (ir.BURN_TTL_SEC + 10)
    assert not ir.is_burned("8.8.8.8:80")
    assert "8.8.8.8" not in ir.BURNED          # lazily evicted on read


def test_burn_counts_repeat_hits():
    """Hit count is the evidence that the feeds keep re-serving dead addresses."""
    for _ in range(4):
        ir.mark_burned("9.9.9.9:80")
    assert ir.BURNED["9.9.9.9"]["hits"] == 4
    assert ir.BURN_STATS["recorded"] == 1     # one IP, not four


def test_burn_memory_off_is_a_no_op(monkeypatch):
    monkeypatch.setattr(ir, "BURN_MEMORY", False)
    ir.mark_burned("10.10.10.10:80")
    assert not ir.is_burned("10.10.10.10:80")
    assert ir.BURNED == {}


def test_burn_never_blocklists_localhost():
    """Tor and any local SOCKS front end live on 127.0.0.1. Blocklisting it
    would kill every local-transport lane at once."""
    ir.mark_burned("127.0.0.1:9150")
    assert not ir.is_burned("127.0.0.1:9150")
    assert ir.BURNED == {}


def test_burn_persists_and_reloads():
    ir.mark_burned("11.11.11.11:80", "relay-429")
    assert ir.save_burned() == 1
    ir.BURNED.clear()
    assert ir.load_burned() == 1
    assert ir.is_burned("11.11.11.11:80")
    assert ir.BURNED["11.11.11.11"]["reason"] == "relay-429"


def test_load_burned_drops_expired_records(tmp_path, monkeypatch):
    path = tmp_path / "old_burn.json"
    path.write_text(json.dumps({"saved": time.time(), "ips": {
        "12.12.12.12": {"at": time.time() - 999999, "reason": "old", "hits": 1},
        "13.13.13.13": {"at": time.time(), "reason": "fresh", "hits": 1},
    }}))
    monkeypatch.setattr(ir, "BURNED_FILE", str(path))
    assert ir.load_burned() == 1
    assert ir.is_burned("13.13.13.13")
    assert not ir.is_burned("12.12.12.12")


def test_prune_burned_bounds_the_dict():
    ir.mark_burned("14.14.14.14:80")
    ir.mark_burned("15.15.15.15:80")
    ir.BURNED["14.14.14.14"]["at"] = time.time() - (ir.BURN_TTL_SEC + 1)
    assert ir.prune_burned() == 1
    assert set(ir.BURNED) == {"15.15.15.15"}


def test_relay_429_records_the_burn():
    """_note_lane_429 is the single funnel every 429 path goes through, so the
    blocklist cannot be bypassed by the streaming vs non-streaming split."""
    ir._note_lane_429("16.16.16.16:8080")
    assert ir.is_burned("16.16.16.16:8080")


def test_load_lanes_skips_burned_ips(monkeypatch, tmp_path):
    """A remembered lane whose IP burned while the process was down must not be
    re-queued — that was ~200 wasted probes/hour on the Webshare IPs alone."""
    monkeypatch.setattr(ir, "LANES_FILE", str(tmp_path / "l.json"))
    good = ir.Lane("17.17.17.17:80", "http")
    good.mark_ok(100)
    bad = ir.Lane("18.18.18.18:80", "http")
    bad.mark_ok(100)
    ir.POOL.lanes = {"http://17.17.17.17:80": good, "http://18.18.18.18:80": bad}
    assert ir.save_lanes() == 2

    ir.POOL.lanes.clear()
    ir.mark_burned("18.18.18.18:80")
    assert ir.load_lanes() == 1
    assert "http://17.17.17.17:80" in ir.POOL.priority_candidates
    assert "http://18.18.18.18:80" not in ir.POOL.priority_candidates
    assert ir.BURN_STATS["blocked_intake"] == 1


@pytest.mark.asyncio
async def test_drop_dead_retires_burned_lanes():
    """A burned public lane is retired, not parked forever: the cooldown re-probe
    can never succeed, and a parked corpse inflates apparent pool size."""
    live = ir.Lane("19.19.19.19:80", "http")
    live.mark_ok(100)
    spent = ir.Lane("20.20.20.20:80", "http")
    spent.mark_ok(100)
    ir.POOL.lanes = {"http://19.19.19.19:80": live, "http://20.20.20.20:80": spent}
    ir.mark_burned("20.20.20.20:80")
    await ir._drop_dead()
    assert "http://19.19.19.19:80" in ir.POOL.lanes
    assert "http://20.20.20.20:80" not in ir.POOL.lanes


def test_default_cooldown_is_not_fiction():
    """90s was measured to be useless — the IP is still 429ing 40 minutes later.
    Guard the default so it can't silently regress."""
    assert ir.DEFAULTS["lane_cooldown_sec"] >= 3600


def test_config_warns_on_short_cooldown(monkeypatch):
    monkeypatch.setattr(ir, "LANE_COOLDOWN_SEC", 90)
    assert any("lane_cooldown_sec" in w for w in ir.config_warnings())


def test_config_warns_when_burn_memory_disabled(monkeypatch):
    monkeypatch.setattr(ir, "BURN_MEMORY", False)
    assert any("burn_memory" in w for w in ir.config_warnings())


# ══════════════════════════════════════════════════════════════════
# TOR EGRESS PROVIDER
# ══════════════════════════════════════════════════════════════════

def test_tor_lane_addr_roundtrip():
    addr = ir.tor_lane_addr(7, 3)
    assert ir.tor_slot_of(addr) == (7, 3)
    assert addr.startswith(ir.TOR_LANE_PREFIX)
    assert f":{ir.TOR_SOCKS_PORT}" in addr


def test_tor_slot_of_rejects_normal_addrs():
    assert ir.tor_slot_of("1.2.3.4:8080") is None
    assert ir.tor_slot_of("") is None


def test_tor_lane_url_is_a_socks5_proxy_with_circuit_username():
    """The username IS the isolation key — IsolateSOCKSAuth gives each distinct
    username its own circuit, hence its own exit IP."""
    ln = ir.Lane(ir.tor_lane_addr(2, 0), "socks5")
    url = ln.url()
    assert url.startswith("socks5://")
    assert "tor-2g0:x@127.0.0.1:" in url


def test_tor_lanes_get_distinct_subnets():
    """All Tor lanes share 127.0.0.1. If subnet collapsed to one bucket,
    max_lanes_per_subnet would cap the whole provider at 3 lanes."""
    subnets = {ir.Lane(ir.tor_lane_addr(i, 0), "socks5").subnet for i in range(12)}
    assert len(subnets) == 12


def test_tor_lane_is_never_blocklisted():
    """Burn memory keys on IP, and every circuit's addr is 127.0.0.1 — so a Tor
    429 must rotate the circuit instead of blocklisting the loopback."""
    addr = ir.tor_lane_addr(0, 0)
    ir.mark_burned(addr)
    assert not ir.is_burned(addr)
    assert ir.BURNED == {}


def test_tor_ensure_lanes_creates_target_count(monkeypatch):
    monkeypatch.setattr(ir, "TOR_ENABLED", True)
    monkeypatch.setattr(ir, "TOR_LANES", 5)
    monkeypatch.setattr(ir, "tor_available", lambda: True)
    assert ir.tor_ensure_lanes() == 5
    assert len([ln for ln in ir.POOL.lanes.values()
                if ln.addr.startswith(ir.TOR_LANE_PREFIX)]) == 5


def test_tor_ensure_lanes_is_idempotent(monkeypatch):
    monkeypatch.setattr(ir, "TOR_ENABLED", True)
    monkeypatch.setattr(ir, "TOR_LANES", 4)
    monkeypatch.setattr(ir, "tor_available", lambda: True)
    assert ir.tor_ensure_lanes() == 4
    assert ir.tor_ensure_lanes() == 0            # already present
    assert len(ir.POOL.lanes) == 4


def test_tor_ensure_lanes_noop_when_socks_port_is_dead(monkeypatch):
    """Better to have zero Tor lanes than lanes that can never dial."""
    monkeypatch.setattr(ir, "TOR_ENABLED", True)
    monkeypatch.setattr(ir, "TOR_LANES", 6)
    monkeypatch.setattr(ir, "tor_available", lambda: False)
    assert ir.tor_ensure_lanes() == 0
    assert ir.POOL.lanes == {}


def test_tor_ensure_lanes_noop_when_disabled(monkeypatch):
    monkeypatch.setattr(ir, "TOR_ENABLED", False)
    monkeypatch.setattr(ir, "tor_available", lambda: True)
    assert ir.tor_ensure_lanes() == 0


def test_tor_rotate_replaces_slot_with_new_generation(monkeypatch):
    """This is the self-renewing mechanism: a burned exit is discarded and the
    slot gets a brand-new circuit, i.e. a never-before-used exit IP."""
    monkeypatch.setattr(ir, "TOR_ENABLED", True)
    monkeypatch.setattr(ir, "TOR_LANES", 1)
    monkeypatch.setattr(ir, "tor_available", lambda: True)
    ir.tor_ensure_lanes()
    old_key = f"socks5://{ir.tor_lane_addr(0, 0)}"
    ln = ir.POOL.lanes[old_key]

    new_key = ir.tor_rotate_lane(ln)
    assert new_key == f"socks5://{ir.tor_lane_addr(0, 1)}"
    assert old_key not in ir.POOL.lanes        # spent circuit is gone, not parked
    assert new_key in ir.POOL.lanes
    assert ir.POOL.lanes[new_key].ok == 0      # fresh lane must re-prove itself


def test_tor_rotate_ignores_non_tor_lanes():
    ln = ir.Lane("21.21.21.21:80", "http")
    assert ir.tor_rotate_lane(ln) is None


def test_tor_lanes_are_not_persisted(monkeypatch, tmp_path):
    """A circuit dies with the tor process. Persisting Tor lanes would re-seed
    dead circuits as priority candidates on every restart."""
    monkeypatch.setattr(ir, "LANES_FILE", str(tmp_path / "l.json"))
    tor = ir.Lane(ir.tor_lane_addr(0, 0), "socks5")
    tor.mark_ok(100)
    normal = ir.Lane("22.22.22.22:80", "http")
    normal.mark_ok(100)
    ir.POOL.lanes = {f"socks5://{tor.addr}": tor, "http://22.22.22.22:80": normal}
    assert ir.save_lanes() == 1                # only the public lane
    ir.POOL.lanes.clear()
    ir.load_lanes()
    assert "http://22.22.22.22:80" in ir.POOL.priority_candidates
    assert not any(k.startswith("socks5://tor-") for k in ir.POOL.priority_candidates)


def test_tor_display_addr_shows_circuit_identity():
    """Every Tor lane would otherwise render as '127.0.0.1:9150' and be
    indistinguishable in the dashboard."""
    assert ir._display_addr(ir.tor_lane_addr(3, 2)) == "tor#3.g2"
    assert "127.0.0.1" not in ir._display_addr(ir.tor_lane_addr(3, 2))


def test_config_warns_when_tor_enabled_but_unreachable(monkeypatch):
    monkeypatch.setattr(ir, "TOR_ENABLED", True)
    monkeypatch.setattr(ir, "tor_available", lambda: False)
    warns = ir.config_warnings()
    assert any("IsolateSOCKSAuth" in w for w in warns)


# ══════════════════════════════════════════════════════════════════
# PIN-AND-DRAIN ROUTING
# ══════════════════════════════════════════════════════════════════

def test_pick_lane_prefers_proven_over_faster_unproven():
    """Measured: a live exit serves 40+ requests, and ~35% of fresh candidates
    are already burned. So a lane that has actually completed a request is worth
    more than an unproven lane with a better latency number."""
    proven = ir.Lane("23.23.23.23:80", "http")
    proven.mark_ok(3000)                       # slow but PROVEN
    unproven = ir.Lane("24.24.24.24:80", "http")
    unproven.lat_ms = 200                      # fast but never completed
    picks = {ir._pick_lane([unproven, proven]).addr for _ in range(10)}
    assert picks == {"23.23.23.23:80"}


def test_pick_lane_round_robins_within_the_pin_set(monkeypatch):
    """One lane has finite in-flight capacity, so the pins must share load
    instead of queueing every concurrent request behind lanes[0]."""
    monkeypatch.setattr(ir, "LANE_PIN_COUNT", 3)
    lanes = []
    for i, lat in enumerate((100, 200, 300, 400)):
        ln = ir.Lane(f"25.25.25.{i}:80", "http")
        ln.mark_ok(lat)
        lanes.append(ln)
    picked = {ir._pick_lane(lanes).addr for _ in range(24)}
    assert len(picked) == 3                    # exactly the pin set, not all 4


def test_pick_lane_respects_pin_count_of_one(monkeypatch):
    monkeypatch.setattr(ir, "LANE_PIN_COUNT", 1)
    lanes = []
    for i in range(3):
        ln = ir.Lane(f"26.26.26.{i}:80", "http")
        ln.mark_ok(100 * (i + 1))
        lanes.append(ln)
    assert len({ir._pick_lane(lanes).addr for _ in range(9)}) == 1


def test_pick_lane_falls_back_to_unproven_when_no_lane_has_succeeded():
    """Cold start: nothing is proven yet, so unproven lanes must still be used —
    that is how a lane earns its first real completion."""
    lanes = [ir.Lane(f"27.27.27.{i}:80", "http") for i in range(3)]
    for i, ln in enumerate(lanes):
        ln.lat_ms = 100 * (i + 1)
    assert ir._pick_lane(lanes) is not None


def test_pick_lane_raises_on_empty_list():
    with pytest.raises(IndexError):
        ir._pick_lane([])


def test_mark_alive_does_not_count_as_a_completion():
    """GET /models answers 200 even from a fully spent IP — measured. Counting
    it as success would make a dead lane look proven and pin traffic to it."""
    ln = ir.Lane("28.28.28.28:80", "http")
    ln.mark_alive(150)
    assert ln.ok == 0                          # not proven
    assert ln.lat_ms == 150                    # but liveness/latency refreshed
    assert ln.consec_fails == 0


def test_mark_ok_still_counts_as_a_completion():
    ln = ir.Lane("29.29.29.29:80", "http")
    ln.mark_ok(150)
    assert ln.ok == 1
