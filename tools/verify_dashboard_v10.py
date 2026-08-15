"""Empirical dashboard verification for ip-relay v1.0.

Drives the real dashboard in Chromium (per the project's rule: UI is verified by
a browser, not by reading the diff) and asserts the v1.0 additions actually
render with live values:

  * the "Egress supply" telemetry line shows tor circuits + burned IP count
  * the settings pane hydrates tor/burn/pin fields from /api/settings
  * warm lane rows carry a 'tor' chip for circuit lanes
  * a settings round-trip through the UI persists tor_lanes

Screenshots land in /root/opencode-rotator/artifacts/.
"""
import json
import pathlib
import sys

from playwright.sync_api import sync_playwright

ART = pathlib.Path("/root/opencode-rotator/artifacts")
ART.mkdir(exist_ok=True)


def find_chrome() -> str:
    """Locate a real Chromium build.

    There is NO ms-playwright cache on this box; the only browser is a
    standalone Chrome-for-Testing tree under /root/chrome/. Search both so this
    script keeps working if playwright browsers are installed later.
    """
    for pat in ("/root/.cache/ms-playwright/chromium-*/chrome-linux64/chrome",
                "/root/.cache/ms-playwright/chromium-*/chrome-linux/chrome",
                "/root/chrome/*/chrome-linux64/chrome"):
        hits = sorted(pathlib.Path("/").glob(pat.lstrip("/")))
        if hits:
            return str(hits[-1])
    raise SystemExit("no chromium binary found — install one or fix find_chrome()")


CHROME = find_chrome()
KEY = json.load(open("/root/opencode-rotator/settings.json"))["relay_api_key"]
BASE = "http://127.0.0.1:18080"

fails: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("PASS  " if cond else "FAIL  ") + name + (f"  [{detail}]" if detail else ""))
    if not cond:
        fails.append(name)


with sync_playwright() as p:
    browser = p.chromium.launch(
        executable_path=CHROME,
        args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"],
    )
    # The key MUST be injected before any page script runs. Setting localStorage
    # after the first load makes the dashboard's initial poll fire unauthenticated,
    # which produces 401s that look like an app bug but are purely a harness
    # artifact (verified: with add_init_script there are zero 4xx and zero
    # console errors).
    ctx = browser.new_context(viewport={"width": 1600, "height": 1100})
    ctx.add_init_script(f"localStorage.setItem('relay_key', {KEY!r})")
    page = ctx.new_page()
    errors: list[str] = []
    bad_responses: list[str] = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
    page.on("response",
            lambda r: bad_responses.append(f"{r.status} {r.url.split('18080')[-1]}")
            if r.status >= 400 else None)

    page.goto(BASE, wait_until="networkidle")
    page.wait_for_timeout(5000)

    # ── 1. egress supply telemetry ────────────────────────────────
    supply = page.text_content("#cSupply") or ""
    check("supply line rendered", supply.strip() not in ("", "—"), supply)
    check("supply shows tor circuits", "tor" in supply and "/" in supply, supply)
    check("supply shows burned IP count", "burned IPs" in supply, supply)
    check("tor is not reported unreachable", "UNREACHABLE" not in supply, supply)

    # ── 2. lane table shows tor lanes distinctly ──────────────────
    # Poll: lane rows are populated by a background /api/pool fetch, and which
    # lanes land on page 1 depends on live scores, so a fixed sleep is flaky.
    lane_html = ""
    for _ in range(20):
        lane_html = page.inner_html("#laneRows") or ""
        if 'class="chip ok">tor<' in lane_html:
            break
        page.wait_for_timeout(1500)
    rows = page.text_content("#laneRows") or ""
    check("lane table has tor chip", 'class="chip ok">tor<' in lane_html, "")
    check("tor lanes show circuit id not loopback", "127.0.0.1:9150" not in rows, "")
    check("no credentials leaked into lane table", "@" not in rows.replace("&#64;", ""), "")

    page.screenshot(path=str(ART / "v10-dashboard-top.png"))

    # ── 3. settings pane hydration ────────────────────────────────
    page.click("text=Configuration")
    page.wait_for_timeout(1200)
    # Open every collapsed section so the new fields are laid out.
    page.evaluate("document.querySelectorAll('details').forEach(d => d.open = true)")
    page.wait_for_timeout(800)

    live = json.loads(
        page.evaluate(
            """async k => {
                const r = await fetch('/api/settings', {headers:{Authorization:'Bearer '+k}});
                return JSON.stringify(await r.json());
            }""",
            KEY,
        )
    )
    for field, sid in (("tor_enabled", "cfgTorEnabled"), ("tor_lanes", "cfgTorLanes"),
                       ("tor_socks_port", "cfgTorSocksPort"), ("burn_memory", "cfgBurnMemory"),
                       ("burn_ttl_sec", "cfgBurnTtl"), ("lane_pin_count", "cfgLanePinCount"),
                       ("lane_cooldown_sec", "cfgCooldownSec")):
        shown = page.input_value(f"#{sid}")
        want = live.get(field)
        want_s = ("true" if want else "false") if isinstance(want, bool) else str(want)
        check(f"settings field {field} hydrated", shown == want_s, f"ui={shown} api={want_s}")

    # cooldown must no longer be capped at the old 3600 max, or 1h+ is unsettable
    cd_max = page.get_attribute("#cfgCooldownSec", "max")
    check("cooldown max raised above 3600", int(cd_max) > 3600, f"max={cd_max}")

    page.screenshot(path=str(ART / "v10-dashboard-settings.png"), full_page=True)

    # ── 4. round-trip a setting through the real form ─────────────
    page.fill("#cfgTorLanes", "14")
    page.click("#btnApply")
    page.wait_for_timeout(4000)
    after = json.loads(
        page.evaluate(
            """async k => {
                const r = await fetch('/api/settings', {headers:{Authorization:'Bearer '+k}});
                return JSON.stringify(await r.json());
            }""",
            KEY,
        )
    )
    check("tor_lanes round-tripped through the UI", after.get("tor_lanes") == 14,
          f"got {after.get('tor_lanes')}")
    check("burn_memory survived the save", after.get("burn_memory") is True,
          f"got {after.get('burn_memory')}")

    # restore
    page.fill("#cfgTorLanes", "12")
    page.click("#btnApply")
    page.wait_for_timeout(3000)
    restored = json.loads(
        page.evaluate(
            """async k => {
                const r = await fetch('/api/settings', {headers:{Authorization:'Bearer '+k}});
                return JSON.stringify(await r.json());
            }""",
            KEY,
        )
    )
    check("tor_lanes restored to 12", restored.get("tor_lanes") == 12,
          f"got {restored.get('tor_lanes')}")

    real_errors = [e for e in errors if "favicon" not in e.lower()]
    check("no JS console/page errors", not real_errors, "; ".join(real_errors[:3]))
    check("no 4xx/5xx API responses", not bad_responses, "; ".join(bad_responses[:4]))

    browser.close()

print()
print(f"{'ALL DASHBOARD CHECKS PASSED' if not fails else 'FAILURES: ' + ', '.join(fails)}")
print("screenshots:", ", ".join(sorted(f.name for f in ART.glob('v10-*.png'))))
sys.exit(1 if fails else 0)
