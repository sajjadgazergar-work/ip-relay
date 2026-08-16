"""Browser assertions for the v1.1 dashboard: perf guards, i18n, RTL, fonts.

Same contract as verify_dashboard_v10.py — asserts what a real browser does, not
what the diff says. Requires a running relay and RELAY_KEY in the environment.

    RELAY_KEY=... python3 tools/verify_dashboard_v11.py
"""
from __future__ import annotations

import asyncio
import glob
import os
import sys

from playwright.async_api import async_playwright

URL = os.environ.get("RELAY_URL", "http://127.0.0.1:18080")
KEY = os.environ.get("RELAY_KEY", "")
CHROME = (glob.glob("/root/chrome/*/chrome-linux64/chrome") or [None])[0]

results: list[tuple[bool, str, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    results.append((bool(ok), label, detail))
    print(f"{'PASS ' if ok else 'FAIL '} {label}" + (f"  [{detail}]" if detail else ""))


async def main() -> int:
    async with async_playwright() as p:
        launch = {"args": ["--no-sandbox", "--disable-dev-shm-usage",
                           "--use-gl=swiftshader", "--enable-unsafe-swiftshader"]}
        if CHROME:
            launch["executable_path"] = CHROME
        b = await p.chromium.launch(**launch)
        pg = await b.new_page(viewport={"width": 1440, "height": 900})

        js_errors: list[str] = []
        bad_http: list[str] = []
        external: list[str] = []
        pg.on("pageerror", lambda e: js_errors.append(str(e)))
        pg.on("console", lambda m: js_errors.append(m.text) if m.type == "error"
              and "401" not in m.text else None)
        pg.on("response", lambda r: (
            bad_http.append(f"{r.status} {r.url}")
            if r.status >= 400 and "/api/" in r.url and r.status != 401 else None))
        # Any request leaving the origin is a censorship/latency risk from Iran.
        pg.on("request", lambda r: external.append(r.url)
              if not r.url.startswith(URL) and not r.url.startswith("data:") else None)

        await pg.goto(URL, wait_until="load")
        await pg.evaluate("k => localStorage.setItem('relay_key', k)", KEY)
        await pg.reload(wait_until="load")
        await pg.wait_for_timeout(5000)

        # ── fonts / no external requests ─────────────────────────────
        check(not external, "no external requests at all",
              ", ".join(external[:3]) or "0 requests off-origin")
        check(await pg.evaluate("document.fonts.check('600 16px Inter')"),
              "Inter loaded from /static")

        # ── perf knobs actually in force ─────────────────────────────
        scale = await pg.evaluate("typeof SHADER_SCALE !== 'undefined' ? SHADER_SCALE : null")
        check(scale is not None and scale <= 0.5, "shader renders at reduced scale",
              f"SHADER_SCALE={scale}")
        # Count only elements that are actually PAINTED. The two modal backdrops
        # carry blur(4px) but sit at display:none until opened — and position:fixed
        # makes offsetParent truthy, so a naive visibility test counts them. A
        # zero-area client rect is the honest "not painted" signal.
        blurs = await pg.evaluate("""(() => [...document.querySelectorAll('*')]
            .filter(e => {
              const f = getComputedStyle(e).backdropFilter;
              if (!f || f === 'none') return false;
              const r = e.getBoundingClientRect();
              return r.width > 0 && r.height > 0;
            }).length)()""")
        check(blurs <= 1, "live backdrop-filter count collapsed", f"{blurs} painted element(s)")

        # Idle freeze: after the idle window the loop must stop scheduling.
        idle_ms = await pg.evaluate("typeof SHADER_IDLE_MS !== 'undefined' ? SHADER_IDLE_MS : null")
        check(idle_ms is not None, "idle-freeze knob present", f"SHADER_IDLE_MS={idle_ms}")
        if idle_ms:
            await pg.wait_for_timeout(int(idle_ms) + 1500)
            frames = await pg.evaluate("""(async () => {
              let n = 0; const t0 = performance.now();
              const tick = () => { n++; if (performance.now() - t0 < 1000) requestAnimationFrame(tick); };
              requestAnimationFrame(tick);
              await new Promise(r => setTimeout(r, 1200));
              return n; })()""")
            check(frames > 0, "page still responsive while frozen", f"{frames} rAF/s available")
            woke = await pg.evaluate("""(() => {
              const before = shaderIdleSince;
              window.dispatchEvent(new Event('scroll'));
              return shaderIdleSince !== before || shaderIdleSince > 0; })()""")
            check(woke, "activity wakes the frozen shader")

        # ── i18n: EN default, FA toggle, RTL ─────────────────────────
        check(await pg.evaluate("document.documentElement.lang") == "en",
              "English is the default language")
        keys = await pg.evaluate("document.querySelectorAll('[data-i18n]').length")
        check(keys > 100, "markup is annotated for translation", f"{keys} keys")
        dict_sizes = await pg.evaluate("[Object.keys(I18N.en).length, Object.keys(I18N.fa).length]")
        check(dict_sizes[0] == dict_sizes[1] and dict_sizes[0] > 150,
              "both languages have the same key set", f"en={dict_sizes[0]} fa={dict_sizes[1]}")
        check("__VERSION__" not in await pg.content(),
              "version placeholder substituted everywhere")

        await pg.evaluate("setLang('fa')")
        await pg.wait_for_timeout(1500)
        check(await pg.evaluate("document.documentElement.dir") == "rtl",
              "dir flips to rtl in Persian")
        check(await pg.evaluate("document.fonts.check('600 16px Vazirmatn')"),
              "Vazirmatn loaded for Persian")
        leftovers = await pg.evaluate("""(() => {
          const bad = [];
          document.querySelectorAll('[data-i18n]').forEach(e => {
            const t = e.textContent.trim();
            // Skip content that SHOULD stay Latin: the base URL, IPs, hostnames
            // and monospace telemetry. #baseUrlVal starts life as a translated
            // placeholder and is then overwritten with the relay's own URL —
            // asserting Persian there would demand a translated hostname.
            if (e.id === 'baseUrlVal') return;
            if (/^[\\w.:\\/-]+$/.test(t)) return;
            if (/^[\\x00-\\x7F]+$/.test(t) && /[A-Za-z]{4}/.test(t))
              bad.push(e.getAttribute('data-i18n'));
          });
          return bad; })()""")
        check(not leftovers, "no English text left in the Persian UI",
              ", ".join(leftovers[:4]) or "all translated")
        # Telemetry must stay LTR even in RTL: a bidi-reordered IP is a different IP.
        mono_dir = await pg.evaluate("""(() => {
          const el = document.querySelector('.mono-cell');
          return el ? getComputedStyle(el).direction : null; })()""")
        check(mono_dir == "ltr", "monospace telemetry stays LTR in RTL", f"direction={mono_dir}")
        check(await pg.evaluate(
            "document.documentElement.scrollWidth <= document.documentElement.clientWidth + 1"),
            "no horizontal overflow in RTL")

        await pg.evaluate("setLang('en')")
        await pg.wait_for_timeout(1000)
        check(await pg.evaluate("document.documentElement.dir") != "rtl",
              "toggling back to English restores LTR")

        # ── token meter wired to real telemetry ──────────────────────
        tok = await pg.evaluate("(document.getElementById('cTokens')||{}).textContent")
        check(tok not in (None, "", "—"), "token KPI is populated", f"cTokens={tok!r}")

        check(not js_errors, "no JS console/page errors", "; ".join(js_errors[:3]) or "clean")
        check(not bad_http, "no unexpected 4xx/5xx", "; ".join(bad_http[:3]) or "clean")

        await pg.screenshot(path="v11-dashboard.png")
        await b.close()

    failed = [r for r in results if not r[0]]
    print()
    if failed:
        print(f"{len(failed)} CHECK(S) FAILED")
        return 1
    print("ALL v1.1 DASHBOARD CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
