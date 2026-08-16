"""v1.1 Phase 2/3 tests: static font mount, i18n table integrity, ui_lang.

These lock the invariants that a future copy edit or refactor would silently
break — a dead translation key, a font 404, path traversal through /static, or
an untranslated string shipping as "Persian".
"""
from __future__ import annotations

import os
import re
import sys
import json

import pytest
from fastapi.testclient import TestClient

import ip_relay as ir

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import i18n_strings as tbl  # noqa: E402

DASH = open(os.path.join(ROOT, "dashboard.html"), encoding="utf-8").read()


# ── /static mount ────────────────────────────────────────────────────
@pytest.mark.parametrize("name", [
    "inter-latin-400.woff2", "inter-latin-500.woff2",
    "inter-latin-600.woff2", "inter-latin-700.woff2",
    "vazirmatn-400.woff2", "vazirmatn-500.woff2",
    "vazirmatn-600.woff2", "vazirmatn-700.woff2",
])
def test_every_font_referenced_by_css_is_actually_served(name):
    """A 404 font is invisible in code review and obvious to every user."""
    assert f"/static/fonts/{name}" in DASH, f"{name} not referenced in CSS"
    with TestClient(ir.app) as c:
        r = c.get(f"/static/fonts/{name}")
    assert r.status_code == 200
    assert r.headers["content-type"] == "font/woff2"
    assert r.content[:4] == b"wOF2", "not a real woff2 payload"


def test_fonts_are_served_without_auth():
    """The auth modal renders BEFORE a key exists, so its fonts must not 401 —
    otherwise the login screen is unstyled for every new user."""
    ir.RELAY_API_KEY = "secret-key"
    try:
        with TestClient(ir.app) as c:
            assert c.get("/static/fonts/vazirmatn-400.woff2").status_code == 200
            assert c.get("/api/stats").status_code == 401
    finally:
        ir.RELAY_API_KEY = ""


@pytest.mark.parametrize("attack", [
    "../ip_relay.py",
    "../../etc/passwd",
    "..%2f..%2fetc%2fpasswd",
    "fonts/../../settings.json",
    "fonts/../../.env",
])
def test_static_rejects_path_traversal(attack):
    with TestClient(ir.app) as c:
        r = c.get(f"/static/{attack}")
    assert r.status_code in (400, 404), f"traversal not blocked: {attack}"
    assert b"UPSTREAM_API_KEY" not in r.content
    assert b"relay_api_key" not in r.content


def test_no_external_font_requests_remain():
    """The Google Fonts stylesheet measured 6.96s from Iran and blocked first
    paint. Self-hosting is only true if no external reference survives.
    Comment prose may name the domains (the file documents why it left); actual
    url()/link href/stylesheet references must not."""
    css_forbidden = re.findall(r"(?:url\([^)]*|href=[\"'][^\"']*)[\"']?", DASH)
    for ref in css_forbidden:
        for bad in ("fonts.googleapis.com", "fonts.gstatic.com", "cdn.jsdelivr.net",
                    "unpkg.com", "cdnjs.cloudflare.com"):
            assert bad not in ref, f"external dependency still present: {bad}"


# ── i18n table integrity ─────────────────────────────────────────────
def test_no_duplicate_keys_in_the_table():
    keys = [k for k, _, _ in tbl.STRINGS] + [k for k, _, _ in tbl.JS_STRINGS]
    dupes = {k for k in keys if keys.count(k) > 1}
    assert not dupes, f"duplicate keys: {dupes}"


def test_every_string_has_a_persian_translation():
    for key, en, fa in tbl.STRINGS + tbl.JS_STRINGS:
        assert fa.strip(), f"{key}: empty Persian"
        assert fa != en or not re.search(r"[A-Za-z]{4}", en), \
            f"{key}: Persian is identical to English ({en!r})"


def test_persian_has_no_arabic_letterforms():
    """ك/ي render subtly differently and break search — the linter enforces this
    too, but a test makes it a build gate."""
    for key, _, fa in tbl.STRINGS + tbl.JS_STRINGS:
        assert "\u0643" not in fa, f"{key}: Arabic KAF"
        assert "\u064a" not in fa, f"{key}: Arabic YEH"


def test_ui_strings_use_ascii_digits():
    """Persian digits beside monospace telemetry and Latin model names read
    worse; the UI policy is ASCII digits (prose docs differ)."""
    for key, _, fa in tbl.STRINGS + tbl.JS_STRINGS:
        assert not re.search("[\u06f0-\u06f9\u0660-\u0669]", fa), \
            f"{key}: non-ASCII digit in UI string"


def test_generated_dict_matches_the_table():
    """i18n_gen.py must have been re-run after editing the table. A stale dict
    means the dashboard ships different text than the linter checked."""
    for key, en, fa in tbl.STRINGS + tbl.JS_STRINGS:
        assert f'"{key}":' in DASH, f"{key} missing from the generated dict"
    en_keys = len(re.findall(r'^\s*"[\w.]+":', DASH, re.M))
    assert en_keys >= 2 * len(tbl.STRINGS + tbl.JS_STRINGS), \
        "generated dict looks smaller than the table — re-run tools/i18n_gen.py"


def test_markup_keys_all_exist_in_the_table():
    """A data-i18n key with no table entry renders as English forever, silently."""
    known = {k for k, _, _ in tbl.STRINGS} | {k for k, _, _ in tbl.JS_STRINGS}
    stamped = set(re.findall(r'data-i18n="([^"]+)"', DASH))
    # diag.* keys are assigned at runtime from the server's verdict slug.
    orphans = {k for k in stamped - known if not k.startswith("diag.")}
    assert not orphans, f"markup references unknown keys: {orphans}"


def test_version_placeholder_is_substituted_in_both_languages():
    """The footer tagline lives in the i18n dict, and the server does a global
    __VERSION__ replace — so the dict copies must be substituted too."""
    with TestClient(ir.app) as c:
        body = c.get("/").text
    assert "__VERSION__" not in body, "unsubstituted placeholder reached the client"
    assert ir.VERSION in body


# ── ui_lang setting ──────────────────────────────────────────────────
def test_ui_lang_defaults_to_english():
    assert ir.DEFAULTS["ui_lang"] == "en"


def test_ui_lang_round_trips_through_settings(tmp_path, monkeypatch):
    """apply_settings mutates the module-level `settings` dict and persists it;
    load_settings() returns None (it applies as a side effect), so read the dict."""
    monkeypatch.setattr(ir, "SETTINGS_FILE", str(tmp_path / "s.json"))
    ir.apply_settings({"ui_lang": "fa"})
    assert ir.settings["ui_lang"] == "fa"
    assert json.load(open(tmp_path / "s.json"))["ui_lang"] == "fa"


def test_ui_lang_rejects_unknown_values(tmp_path, monkeypatch):
    """An unknown language would render a dashboard of missing-key placeholders."""
    monkeypatch.setattr(ir, "SETTINGS_FILE", str(tmp_path / "s.json"))
    ir.apply_settings({"ui_lang": "klingon"})
    assert ir.settings["ui_lang"] == "en"


def test_dashboard_ships_english_markup_as_the_fallback():
    """Source text stays in the HTML so a missing key degrades to readable
    English rather than a blank element."""
    assert 'lang="en"' in DASH
    assert "Warm lanes" in DASH


def test_rtl_uses_logical_properties_not_a_mirrored_sheet():
    """padding-left/margin-right in the stylesheet would need a second RTL sheet
    to stay correct; logical properties flip for free."""
    css = DASH[DASH.index("<style>"):DASH.index("</style>")]
    css = re.sub(r"\[dir=\"rtl\"\][^}]*}", "", css)   # the explicit RTL block may
    for prop in ("padding-left", "padding-right", "margin-left", "margin-right",
                 "border-left:", "border-right:"):
        assert prop not in css, f"physical property {prop} needs an RTL override"
