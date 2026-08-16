"""Honest v1.0 vs v1.1 dashboard frame-cost comparison.

Method: serve BOTH dashboards from the SAME live relay so the data conditions are
identical (same lane count, same SSE traffic, same telemetry payloads) — a
static-file server would show an empty topology and flatter the new build.

It swaps dashboard.html on disk, restarts oc-rotator, measures, and ALWAYS
restores. Repeats each condition and takes the median of medians because this
box is noisy (single run varied 50–100ms on identical code).

Usage: RELAY_KEY=... python tools/perf_compare_v10_v11.py
"""
import asyncio
import glob
import os
import shutil
import statistics
import subprocess
import sys

from playwright.async_api import async_playwright

CHROME = glob.glob("/root/chrome/*/chrome-linux64/chrome")
KEY = os.environ.get("RELAY_KEY", "")
LIVE = "/root/opencode-rotator/dashboard.html"
V11 = "/tmp/dashboard.v11.current"
V10 = "/tmp/dashboard.v10.bak"
REPEATS = 3
SAMPLE_MS = 4000

PROBE = """(ms) => new Promise(res => {
  const f = []; let l = performance.now(); const t0 = l;
  function t(n) { f.push(n - l); l = n; if (n - t0 < ms) requestAnimationFrame(t); else res(f); }
  requestAnimationFrame(t);
})"""


def restart():
    subprocess.run(["systemctl", "restart", "oc-rotator"], check=True)
    subprocess.run(["sleep", "6"], check=True)


async def sample(browser, idle_wait):
    page = await browser.new_page(viewport={"width": 1440, "height": 900})
    await page.goto("http://127.0.0.1:18080/", wait_until="load")
    if KEY:
        await page.evaluate("k => localStorage.setItem('relay_key', k)", KEY)
        await page.reload(wait_until="load")
    await page.wait_for_timeout(idle_wait)
    frames = [x for x in await page.evaluate(PROBE, SAMPLE_MS) if x > 0]
    await page.close()
    return statistics.median(frames) if frames else None


async def measure(label, idle_wait):
    out = []
    async with async_playwright() as p:
        b = await p.chromium.launch(
            executable_path=CHROME[0],
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--use-gl=swiftshader", "--enable-unsafe-swiftshader"])
        for _ in range(REPEATS):
            m = await sample(b, idle_wait)
            if m:
                out.append(m)
        await b.close()
    if not out:
        return None
    med = statistics.median(out)
    print(f"  {label:34s} median-of-{len(out)} {med:7.1f} ms  "
          f"({1000/med:4.1f} fps)  runs={[round(x,1) for x in out]}")
    return med


async def main():
    if not CHROME:
        print("no chrome found")
        return 1
    shutil.copy(LIVE, V11)
    results = {}
    try:
        print("v1.1 (current):")
        restart()
        results["v11_active"] = await measure("active / just interacted", 2500)
        results["v11_idle"] = await measure("idle >4s (shader frozen)", 7000)

        print("v1.0 (pristine backup):")
        shutil.copy(V10, LIVE)
        restart()
        results["v10"] = await measure("as shipped", 2500)
    finally:
        shutil.copy(V11, LIVE)
        restart()
        print("restored v1.1 dashboard, service restarted")

    print()
    v10 = results.get("v10")
    print("| Build | Median frame | ~fps | vs v1.0 |")
    print("|---|---|---|---|")
    for key, name in (("v10", "v1.0 as shipped"),
                      ("v11_active", "v1.1 active"),
                      ("v11_idle", "v1.1 idle")):
        m = results.get(key)
        if not m:
            continue
        speedup = f"{v10/m:.1f}x faster" if v10 and key != "v10" else "—"
        print(f"| {name} | {m:.1f} ms | {1000/m:.1f} | {speedup} |")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
