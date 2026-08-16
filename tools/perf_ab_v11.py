"""v1.1 A/B/C/D frame-cost attribution, same conditions as the v1.0 baseline.

Same method as tools/perf_ab.py so the numbers are comparable: progressively
remove layers and measure the rAF frame-time distribution in a real Chromium.

This box has NO GPU (SwiftShader), so absolutes are pessimistic — the RATIOS
between conditions are the signal, and they are what a weak/throttled GPU sees.

Conditions:
  A  as shipped (v1.1: half-res 30fps shader, painted glass, gooey topology)
  B  minus the WebGL backdrop
  C  minus topology canvas too
  D  minus noise overlay too
"""
import asyncio
import glob
import json
import os
import statistics
import sys

from playwright.async_api import async_playwright

URL = os.environ.get("DASH_URL", "http://127.0.0.1:18080/")
KEY = os.environ.get("RELAY_KEY", "")
CHROME = glob.glob("/root/chrome/*/chrome-linux64/chrome")
SAMPLE_MS = 5000

PROBE = """
(ms) => new Promise(resolve => {
  const frames = [];
  let last = performance.now();
  const t0 = last;
  function tick(now) {
    frames.push(now - last);
    last = now;
    if (now - t0 < ms) requestAnimationFrame(tick);
    else resolve(frames);
  }
  requestAnimationFrame(tick);
})
"""

STRIP = {
    "A": "() => {}",
    "B": """() => {
        const c = document.getElementById('canvas-bg');
        if (c) c.style.display = 'none';
        if (typeof setShaderPaused === 'function') setShaderPaused(true);
    }""",
    "C": """() => {
        const c = document.getElementById('canvas-bg');
        if (c) c.style.display = 'none';
        if (typeof setShaderPaused === 'function') setShaderPaused(true);
        const n = document.getElementById('networkCanvas');
        if (n) { n.style.display = 'none'; }
        window.netVisible = false;   // stop the topology loop
        if (typeof netVisible !== 'undefined') { try { eval('netVisible = false'); } catch (e) {} }
    }""",
    "D": """() => {
        const c = document.getElementById('canvas-bg');
        if (c) c.style.display = 'none';
        if (typeof setShaderPaused === 'function') setShaderPaused(true);
        const n = document.getElementById('networkCanvas');
        if (n) n.style.display = 'none';
        try { eval('netVisible = false'); } catch (e) {}
        document.querySelectorAll('.noise-overlay').forEach(e => e.style.display = 'none');
    }""",
}

LABEL = {
    "A": "as shipped (v1.1)",
    "B": "minus WebGL backdrop",
    "C": "minus topology canvas too",
    "D": "minus noise overlay too",
}


async def measure(browser, cond):
    page = await browser.new_page(viewport={"width": 1440, "height": 900})
    await page.goto(URL, wait_until="load")
    if KEY:
        await page.evaluate("k => localStorage.setItem('relay_key', k)", KEY)
        await page.reload(wait_until="load")
    await page.wait_for_timeout(2500)          # let telemetry + viz settle
    await page.evaluate(STRIP[cond])
    await page.wait_for_timeout(600)
    frames = await page.evaluate(PROBE, SAMPLE_MS)
    await page.close()
    frames = [f for f in frames if f > 0]
    return frames


async def main():
    if not CHROME:
        print("no chrome binary found under /root/chrome/*/chrome-linux64/chrome")
        return 1
    out = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            executable_path=CHROME[0],
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--use-gl=swiftshader", "--enable-unsafe-swiftshader"])
        for cond in ("A", "B", "C", "D"):
            frames = await measure(browser, cond)
            if not frames:
                out[cond] = None
                continue
            frames_sorted = sorted(frames)
            out[cond] = {
                "n": len(frames),
                "median_ms": round(statistics.median(frames), 1),
                "p95_ms": round(frames_sorted[int(len(frames_sorted) * 0.95) - 1], 1),
                "fps": round(1000 / statistics.median(frames), 1),
            }
        await browser.close()

    print(json.dumps(out, indent=1))
    print()
    print("| Condition | Median frame | ~fps | p95 frame |")
    print("|---|---|---|---|")
    for cond in ("A", "B", "C", "D"):
        r = out.get(cond)
        if not r:
            print(f"| {cond} — {LABEL[cond]} | (no frames) | | |")
            continue
        print(f"| {cond} — {LABEL[cond]} | {r['median_ms']} ms | {r['fps']} | {r['p95_ms']} ms |")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
