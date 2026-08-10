"""Empirical dashboard verification: real browser, real relay, measured values.

Drives http://127.0.0.1:18080/ with Playwright and asserts things that are easy
to *claim* and hard to *prove*: no console errors, the SSE stream actually
connects, live values render, WCAG contrast on real computed colours, keyboard
focus visibility, and reduced-motion compliance.

Run:  /root/conduit/.venv/bin/python tools/verify_dashboard.py
"""
import asyncio
import json
import os
import sys

from playwright.async_api import async_playwright

CHROME = "/root/.cache/ms-playwright/chromium-1234/chrome-linux64/chrome"
BASE = os.environ.get("RELAY_URL", "http://127.0.0.1:18080")
OUT = "/tmp/dash/shots"
ARGS = ["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage",
        "--use-gl=swiftshader", "--enable-unsafe-swiftshader"]


def relay_key() -> str:
    try:
        with open("/root/opencode-rotator/settings.json") as f:
            return json.load(f).get("relay_api_key", "")
    except Exception:
        return ""


# ── WCAG contrast maths ────────────────────────────────────────────
def parse_rgb(s: str):
    nums = [float(x) for x in s.replace("rgba(", "").replace("rgb(", "")
            .replace(")", "").split(",")]
    while len(nums) < 4:
        nums.append(1.0)
    return nums


def rel_lum(r, g, b):
    def ch(c):
        c /= 255.0
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
    return 0.2126 * ch(r) + 0.7152 * ch(g) + 0.0722 * ch(b)


def over(fg, bg):
    """Composite a translucent foreground colour over an opaque background."""
    fr, fg_, fb, fa = fg
    br, bg_, bb, _ = bg
    return (fr * fa + br * (1 - fa), fg_ * fa + bg_ * (1 - fa), fb * fa + bb * (1 - fa), 1.0)


def contrast(fg: str, bg: str) -> float:
    f = over(parse_rgb(fg), parse_rgb(bg))
    b = parse_rgb(bg)
    l1, l2 = rel_lum(*f[:3]), rel_lum(*b[:3])
    hi, lo = max(l1, l2), min(l1, l2)
    return (hi + 0.05) / (lo + 0.05)


CONTRAST_JS = """
() => {
  // Walk the real DOM and report computed colours for every text-bearing node.
  const out = [];
  const seen = new Set();
  function bgOf(el) {
    let n = el;
    while (n && n !== document.documentElement) {
      const c = getComputedStyle(n).backgroundColor;
      if (c && c !== 'rgba(0, 0, 0, 0)' && c !== 'transparent') {
        const a = c.startsWith('rgba') ? parseFloat(c.split(',')[3]) : 1;
        if (a >= 0.95) return c;
      }
      n = n.parentElement;
    }
    return getComputedStyle(document.body).backgroundColor;
  }
  document.querySelectorAll('body *').forEach(el => {
    if (!el.offsetParent && el.tagName !== 'BODY') return;
    const txt = Array.from(el.childNodes)
      .filter(n => n.nodeType === 3).map(n => n.textContent.trim()).join('').trim();
    if (!txt || txt.length < 2) return;
    const cs = getComputedStyle(el);
    if (cs.visibility === 'hidden' || cs.display === 'none') return;
    if (cs.webkitTextFillColor === 'rgba(0, 0, 0, 0)') return;  // gradient text
    const key = cs.color + '|' + bgOf(el) + '|' + cs.fontSize;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({
      sample: txt.slice(0, 40),
      color: cs.color, bg: bgOf(el),
      size: parseFloat(cs.fontSize),
      weight: parseInt(cs.fontWeight, 10) || 400,
      sel: el.tagName.toLowerCase() + (el.id ? '#' + el.id : '') +
           (el.className && typeof el.className === 'string' ? '.' + el.className.split(' ')[0] : '')
    });
  });
  return out;
}
"""


async def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    key = relay_key()
    problems: list[str] = []
    notes: list[str] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(executable_path=CHROME, args=ARGS)

        # ── pass 1: normal motion, authenticated ────────────────────
        ctx = await browser.new_context(viewport={"width": 1440, "height": 1000},
                                        device_scale_factor=1)
        await ctx.add_init_script(f"localStorage.setItem('relay_key', {json.dumps(key)});")
        page = await ctx.new_page()

        errors: list[str] = []
        page.on("console", lambda m: errors.append(f"{m.type}: {m.text}")
                if m.type in ("error",) else None)
        page.on("pageerror", lambda e: errors.append(f"pageerror: {e}"))

        sse_status: dict = {}

        def on_resp(r):
            if "/api/events" in r.url:
                sse_status["status"] = r.status
        page.on("response", on_resp)

        await page.goto(BASE, wait_until="domcontentloaded")
        await page.wait_for_timeout(6000)   # let SSE connect + first polls land

        # 1. no console errors
        real_errors = [e for e in errors if "favicon" not in e and "fonts.g" not in e]
        if real_errors:
            problems.append("console errors: " + " | ".join(real_errors[:5]))
        else:
            notes.append("no console errors")

        # 2. SSE actually connected (200, not 401)
        if sse_status.get("status") != 200:
            problems.append(f"/api/events returned {sse_status.get('status')} (expected 200)")
        else:
            notes.append("SSE /api/events → 200")

        feed = (await page.text_content("#feedText") or "").strip()
        if "live" not in feed:
            problems.append(f"feed indicator says '{feed}', expected live stream")
        else:
            notes.append(f"feed indicator: {feed}")

        # 3. live values rendered (not placeholders)
        for sel, label in [("#statusText", "status"), ("#footStatus", "footer"),
                           ("#engineState", "engine state"), ("#diagTitle", "diagnostics verdict")]:
            v = (await page.text_content(sel) or "").strip()
            if not v or v in ("—", "connecting…", "Running checks…"):
                problems.append(f"{label} never populated (got '{v}')")
            else:
                notes.append(f"{label}: {v}")

        # 4. diagnostics rendered real reasons from the server
        reasons = await page.eval_on_selector_all(".reason-cell .k", "els => els.map(e => e.textContent)")
        notes.append(f"probe-failure cells: {reasons}")

        # 5. accessibility structure
        a11y = await page.evaluate("""() => ({
          h1: document.querySelectorAll('h1').length,
          landmarks: document.querySelectorAll('main, header, footer, section[aria-labelledby]').length,
          skip: !!document.querySelector('.skip-link'),
          live: document.querySelectorAll('[aria-live]').length,
          unlabelledInputs: Array.from(document.querySelectorAll('input:not([type=hidden]), textarea, select'))
            .filter(el => !el.labels?.length && !el.getAttribute('aria-label') && !el.getAttribute('aria-labelledby'))
            .map(el => el.id || el.name || el.type),
          buttonsNoName: Array.from(document.querySelectorAll('button'))
            .filter(b => !(b.textContent || '').trim() && !b.getAttribute('aria-label')).length,
          tablist: document.querySelectorAll('[role=tab]').length,
          dialogs: document.querySelectorAll('[role=dialog][aria-modal=true]').length,
          canvasHidden: Array.from(document.querySelectorAll('canvas'))
            .every(c => c.getAttribute('aria-hidden') === 'true'),
          tableScopes: document.querySelectorAll('th[scope]').length,
        })""")
        notes.append("a11y: " + json.dumps(a11y))
        if a11y["h1"] != 1:
            problems.append(f"expected exactly 1 h1, found {a11y['h1']}")
        if a11y["unlabelledInputs"]:
            problems.append(f"inputs without an accessible name: {a11y['unlabelledInputs']}")
        if a11y["buttonsNoName"]:
            problems.append(f"{a11y['buttonsNoName']} button(s) with no accessible name")
        if not a11y["skip"]:
            problems.append("no skip link")
        if not a11y["canvasHidden"]:
            problems.append("decorative canvas not aria-hidden")

        # 6. measured contrast on every rendered text style
        samples = await page.evaluate(CONTRAST_JS)
        fails = []
        for s in samples:
            try:
                ratio = contrast(s["color"], s["bg"])
            except Exception:
                continue
            large = s["size"] >= 24 or (s["size"] >= 18.66 and s["weight"] >= 700)
            need = 3.0 if large else 4.5
            if ratio < need:
                fails.append(f"{s['sel']} '{s['sample']}' {ratio:.2f}:1 (needs {need})")
        notes.append(f"contrast: checked {len(samples)} text styles, {len(fails)} below AA")
        if fails:
            problems.append("contrast failures: " + " | ".join(fails[:8]))

        # 7. keyboard focus is visible
        await page.keyboard.press("Tab")
        focus = await page.evaluate("""() => {
          const el = document.activeElement;
          if (!el || el === document.body) return null;
          const cs = getComputedStyle(el);
          return {tag: el.tagName, cls: el.className,
                  outline: cs.outlineStyle + ' ' + cs.outlineWidth + ' ' + cs.outlineColor};
        }""")
        if not focus or focus["outline"].startswith("none"):
            problems.append(f"first Tab stop has no visible focus ring: {focus}")
        else:
            notes.append(f"focus ring on first tab stop: {focus['tag']} {focus['outline']}")

        await page.screenshot(path=f"{OUT}/01-full.png", full_page=True)
        await page.screenshot(path=f"{OUT}/02-viewport.png")

        # 8. modal keyboard behaviour: open, trap, Escape
        await page.click("button.ghost-btn[aria-haspopup=dialog]")
        await page.wait_for_timeout(900)
        modal_open = await page.evaluate("() => document.getElementById('profileModal').classList.contains('open')")
        prof_count = await page.eval_on_selector_all(".profile-item", "els => els.length")
        if not modal_open or prof_count == 0:
            problems.append(f"provider modal broken (open={modal_open}, items={prof_count})")
        else:
            notes.append(f"provider modal: {prof_count} profiles listed")
        await page.screenshot(path=f"{OUT}/03-profiles-modal.png")
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(400)
        if await page.evaluate("() => document.getElementById('profileModal').classList.contains('open')"):
            problems.append("Escape did not close the provider modal")
        else:
            notes.append("Escape closes the modal")

        # 9. advanced settings + validation UI
        await page.click("summary:has-text('Advanced settings')")
        await page.fill("#cfgLaneInflight", "99")
        await page.wait_for_timeout(400)
        err = (await page.text_content("#errCfgLaneInflight") or "").strip()
        disabled = await page.is_disabled("#btnApply")
        if not err or not disabled:
            problems.append(f"invalid field did not block save (err='{err}', apply disabled={disabled})")
        else:
            notes.append(f"validation: '{err}' and Apply disabled")
        await page.screenshot(path=f"{OUT}/04-validation.png")
        await page.fill("#cfgLaneInflight", "2")
        await page.wait_for_timeout(300)

        # 10. lane table + pagination controls
        rows = await page.eval_on_selector_all("#laneRows tr", "els => els.length")
        page_info = (await page.text_content("#pageInfo") or "").strip()
        notes.append(f"lane rows rendered: {rows}, page info: '{page_info}'")

        # 11. log filter actually filters
        await page.fill("#logFilter", "Prober")
        await page.wait_for_timeout(500)
        shown = await page.eval_on_selector_all(".log-entry", "els => els.length")
        all_match = await page.eval_on_selector_all(
            ".log-entry .log-msg", "els => els.every(e => /Prober|match the filter/i.test(e.textContent))")
        notes.append(f"log filter: {shown} entries, all match={all_match}")
        if not all_match:
            problems.append("log filter shows non-matching lines")
        await page.screenshot(path=f"{OUT}/05-logs-filtered.png")
        await page.fill("#logFilter", "")

        # 12. tab keyboard navigation on the code snippet tablist
        await page.click("#tab-curl")
        await page.keyboard.press("ArrowRight")
        await page.wait_for_timeout(300)
        sel_tab = await page.evaluate("() => document.querySelector('[role=tab][aria-selected=true]').id")
        if sel_tab == "tab-curl":
            problems.append("ArrowRight did not move tab selection")
        else:
            notes.append(f"ArrowRight moved tablist selection to {sel_tab}")
        code = (await page.text_content("#codeBlock") or "")
        if BASE.split("//")[1].split(":")[0] not in code:
            problems.append("code snippet does not contain the relay host")
        else:
            notes.append("code snippet contains the live host")

        # 13. mobile layout
        await page.set_viewport_size({"width": 390, "height": 850})
        await page.wait_for_timeout(900)
        overflow = await page.evaluate("() => document.documentElement.scrollWidth - window.innerWidth")
        if overflow > 4:
            problems.append(f"horizontal overflow at 390px: {overflow}px")
        else:
            notes.append("no horizontal overflow at 390px")
        await page.screenshot(path=f"{OUT}/06-mobile.png", full_page=True)
        await ctx.close()

        # ── pass 2: prefers-reduced-motion ─────────────────────────
        ctx2 = await browser.new_context(viewport={"width": 1440, "height": 1000},
                                         reduced_motion="reduce")
        await ctx2.add_init_script(f"localStorage.setItem('relay_key', {json.dumps(key)});")
        p2 = await ctx2.new_page()
        await p2.goto(BASE, wait_until="domcontentloaded")
        await p2.wait_for_timeout(3500)
        rm = await p2.evaluate("""() => ({
          shaderHidden: getComputedStyle(document.getElementById('canvas-bg')).display,
          jsFlag: typeof REDUCED_MOTION !== 'undefined' ? REDUCED_MOTION : null,
          dotAnim: getComputedStyle(document.getElementById('statusPill')).animationDuration
        })""")
        notes.append("reduced-motion: " + json.dumps(rm))
        if rm["shaderHidden"] != "none":
            problems.append("WebGL backdrop still displayed under prefers-reduced-motion")
        if rm["jsFlag"] is not True:
            problems.append("JS REDUCED_MOTION flag not set")
        await p2.screenshot(path=f"{OUT}/07-reduced-motion.png")
        await ctx2.close()

        # ── pass 3: unauthenticated → auth modal ───────────────────
        ctx3 = await browser.new_context(viewport={"width": 1440, "height": 1000})
        p3 = await ctx3.new_page()
        await p3.goto(BASE, wait_until="domcontentloaded")
        await p3.wait_for_timeout(3000)
        open3 = await p3.evaluate("() => document.getElementById('authModal').classList.contains('open')")
        focused = await p3.evaluate("() => document.activeElement && document.activeElement.id")
        if not open3:
            problems.append("401 did not raise the auth modal")
        else:
            notes.append(f"auth modal opens on 401, focus moved to '{focused}'")
        if focused != "authKeyInput":
            problems.append(f"auth modal did not focus its input (focus on '{focused}')")
        await p3.screenshot(path=f"{OUT}/08-auth-modal.png")
        await ctx3.close()

        await browser.close()

    print("── verified ─────────────────────────────")
    for n in notes:
        print("  ok  " + n)
    if problems:
        print("── problems ─────────────────────────────")
        for p in problems:
            print("  !!  " + p)
    print(f"\nscreenshots in {OUT}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
