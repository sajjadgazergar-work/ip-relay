"""Empirical UI performance probe for the ip-relay dashboard.

Answers "why is the animation glitchy?" with numbers instead of opinion:
  * frame-time distribution while idle (rAF deltas)
  * frame-time distribution while scrolling
  * long tasks (>50ms) via PerformanceObserver
  * forced synchronous layouts caused by the .flash restart trick
  * topology node churn (nodes appearing/disappearing between pool refreshes)

Run with the interpreter that HAS playwright (the hermes venv), not the project venv:
  /usr/local/lib/hermes-agent/venv/bin/python tools/perf_probe.py
"""
import json
import pathlib
import statistics

from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:18080"
KEY = json.load(open("/root/opencode-rotator/settings.json"))["relay_api_key"]
ART = pathlib.Path("/root/opencode-rotator/artifacts")
ART.mkdir(exist_ok=True)


def find_chrome() -> str:
    for pat in ("root/.cache/ms-playwright/chromium-*/chrome-linux64/chrome",
                "root/.cache/ms-playwright/chromium-*/chrome-linux/chrome",
                "root/chrome/*/chrome-linux64/chrome"):
        hits = sorted(pathlib.Path("/").glob(pat))
        if hits:
            return str(hits[-1])
    raise SystemExit("no chromium binary found")


INSTRUMENT = """
window.__perf = { frames: [], longtasks: [], nodeCounts: [], syncCalls: 0 };
(() => {
  let last = performance.now();
  function tick(t) {
    window.__perf.frames.push(t - last);
    last = t;
    requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
  try {
    new PerformanceObserver(list => {
      for (const e of list.getEntries()) window.__perf.longtasks.push(Math.round(e.duration));
    }).observe({ entryTypes: ['longtask'] });
  } catch (e) {}
})();
"""

WRAP_SYNC = """
(() => {
  const orig = window.syncNetworkGraph;
  if (typeof orig !== 'function') return 'no syncNetworkGraph';
  window.syncNetworkGraph = function (lanes) {
    window.__perf.syncCalls++;
    const before = (window.netNodes || []).length;
    const r = orig.apply(this, arguments);
    window.__perf.nodeCounts.push([before, (window.netNodes || []).length,
                                   Array.isArray(lanes) ? lanes.length : -1]);
    return r;
  };
  return 'wrapped';
})()
"""


def summarize(name, frames):
    if not frames:
        print(f"  {name}: no frames")
        return
    fs = sorted(frames)
    over16 = sum(1 for f in frames if f > 20)
    over50 = sum(1 for f in frames if f > 50)
    print(f"  {name}: n={len(frames)} median={statistics.median(fs):.1f}ms "
          f"p95={fs[int(0.95 * (len(fs) - 1))]:.1f}ms max={fs[-1]:.1f}ms "
          f"| >20ms: {over16} ({100 * over16 / len(frames):.0f}%) | >50ms: {over50}")


with sync_playwright() as p:
    browser = p.chromium.launch(
        executable_path=find_chrome(),
        args=["--no-sandbox", "--disable-dev-shm-usage",
              "--use-gl=swiftshader", "--enable-unsafe-swiftshader"],
    )
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    ctx.add_init_script(f"localStorage.setItem('relay_key', {json.dumps(KEY)});")
    ctx.add_init_script(INSTRUMENT)
    page = ctx.new_page()
    console_errors = []
    page.on("console", lambda m: console_errors.append(m.text) if m.type in ("error", "warning") else None)
    page.on("pageerror", lambda e: console_errors.append("PAGEERROR " + str(e)))

    page.goto(BASE, wait_until="load")
    page.wait_for_timeout(1500)
    print("wrap syncNetworkGraph:", page.evaluate(WRAP_SYNC))

    # ── idle: nothing but the ambient animations ─────────────────
    page.evaluate("window.__perf.frames = []")
    page.wait_for_timeout(6000)
    idle = page.evaluate("window.__perf.frames")
    print("\nFRAME TIMES")
    summarize("idle    ", idle)

    # ── scrolling: compositor cost of fixed blur layers ──────────
    page.evaluate("window.__perf.frames = []")
    for _ in range(14):
        page.mouse.wheel(0, 220)
        page.wait_for_timeout(120)
    for _ in range(14):
        page.mouse.wheel(0, -220)
        page.wait_for_timeout(120)
    scroll = page.evaluate("window.__perf.frames")
    summarize("scrolling", scroll)

    # ── hidden tab: does the topology loop keep burning frames? ──
    page.evaluate("window.__perf.frames = []")
    page.evaluate("Object.defineProperty(document, 'hidden', {get: () => true, configurable: true});"
                  "document.dispatchEvent(new Event('visibilitychange'));")
    page.wait_for_timeout(3000)
    hidden = page.evaluate("window.__perf.frames")
    print(f"  hidden-tab frames in 3s: {len(hidden)} "
          f"(0 = loop paused, ~180 = still rendering)")
    page.evaluate("Object.defineProperty(document, 'hidden', {get: () => false, configurable: true});"
                  "document.dispatchEvent(new Event('visibilitychange'));")

    # ── topology churn: how violently does the node set change? ──
    page.wait_for_timeout(9000)
    perf = page.evaluate("window.__perf")
    print("\nLONG TASKS (>50ms):", perf["longtasks"] or "none")
    print("\nTOPOLOGY NODE CHURN  [nodes_before, nodes_after, warm_lanes_from_api]")
    for row in perf["nodeCounts"][:14]:
        print("  ", row)
    print("  syncNetworkGraph calls:", perf["syncCalls"])

    diag = page.evaluate("""({
      netNodes: (window.netNodes || []).length,
      particles: (window.netParticles || []).length,
      rafLoops: 'shader + topology',
      blurLayers: Array.from(document.querySelectorAll('*')).filter(el => {
        const s = getComputedStyle(el);
        return (s.backdropFilter && s.backdropFilter !== 'none');
      }).length,
      fixedFullscreen: Array.from(document.querySelectorAll('*')).filter(el => {
        const s = getComputedStyle(el);
        return s.position === 'fixed' && el.getBoundingClientRect().height >= window.innerHeight * 0.9;
      }).map(el => el.id || el.className),
      canvasBg: (() => { const c = document.getElementById('canvas-bg');
        return c ? c.width + 'x' + c.height : 'none'; })(),
      netCanvas: (() => { const c = document.getElementById('networkCanvas');
        return c ? c.width + 'x' + c.height + ' css ' + Math.round(c.clientWidth) + 'x' + Math.round(c.clientHeight) : 'none'; })(),
      docHeight: document.documentElement.scrollHeight,
      statPills: document.querySelectorAll('.stat-pill').length
    })""")
    print("\nDOM / RENDER FACTS")
    for k, v in diag.items():
        print(f"  {k}: {v}")

    print("\nCONSOLE (errors/warnings):", console_errors[:10] or "clean")

    page.screenshot(path=str(ART / "perf-top.png"))
    page.screenshot(path=str(ART / "perf-full.png"), full_page=True)
    print("\nscreenshots:", ART / "perf-top.png", ART / "perf-full.png")
    browser.close()
