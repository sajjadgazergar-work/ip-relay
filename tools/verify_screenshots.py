"""Verify the README screenshots without a vision model.

Two independent signals, because each catches what the other cannot:

 1. DOM truth — read the actual rendered text and geometry from the same page
    state the screenshot captured. Catches "—" placeholders and clipped text.
 2. Pixel truth — per-region stddev on the saved PNG. A blank or broken panel is
    flat; real content has variance. Catches "element exists in the DOM but
    painted nothing", which no DOM query can see.

    RELAY_KEY=... python3 tools/verify_screenshots.py
"""
from __future__ import annotations

import asyncio
import glob
import os
import sys

from PIL import Image, ImageStat
from playwright.async_api import async_playwright

CHROME = glob.glob('/root/chrome/*/chrome-linux64/chrome')[0]
KEY = os.environ['RELAY_KEY']
URL = os.environ.get('RELAY_URL', 'http://127.0.0.1:18080')
fails: list[str] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    print(f"{'PASS ' if ok else 'FAIL '} {label}" + (f"  [{detail}]" if detail else ""))
    if not ok:
        fails.append(label)


async def dom_checks() -> None:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            executable_path=CHROME,
            args=['--no-sandbox', '--disable-dev-shm-usage',
                  '--use-gl=swiftshader', '--enable-unsafe-swiftshader'])
        pg = await browser.new_page(viewport={'width': 1440, 'height': 1035},
                                    device_scale_factor=2)
        await pg.goto(URL, wait_until='load')
        await pg.evaluate("k => localStorage.setItem('relay_key', k)", KEY)
        await pg.reload(wait_until='load')
        await pg.wait_for_timeout(9000)

        for lang in ('en', 'fa'):
            await pg.evaluate(f"setLang('{lang}')")
            await pg.wait_for_timeout(3000)

            kpis = await pg.evaluate("""(() => ['cWarm','cTokens','cP95'].map(id => {
                const e = document.getElementById(id);
                return {id, text: e ? e.textContent.trim() : null}; }))()""")
            for k in kpis:
                check(k['text'] not in (None, '', '—', '-'),
                      f"[{lang}] KPI {k['id']} populated", repr(k['text']))

            # Sample the canvas BACKING STORE, not the CSS box: a sized canvas
            # that drew nothing still has a non-zero client rect.
            drawn = await pg.evaluate("""(() => {
                const c = document.getElementById('networkCanvas');
                if (!c) return -1;
                const g = c.getContext('2d');
                const d = g.getImageData(0, 0, c.width, c.height).data;
                let n = 0;
                for (let i = 3; i < d.length; i += 4) if (d[i] > 8) n++;
                return n; })()""")
            check(drawn > 500, f"[{lang}] topology canvas has painted pixels", f"{drawn} px")

            clipped = await pg.evaluate("""(() => {
                const bad = [];
                document.querySelectorAll('[data-i18n], .kpi-value, .stat-value').forEach(e => {
                  // .sr-only is clipped BY DESIGN (1x1 box for screen readers) —
                  // counting it as clipped text made this check cry wolf 3x.
                  if (e.closest('.sr-only') || e.classList.contains('sr-only')) return;
                  if (e.scrollWidth > e.clientWidth + 2 &&
                      getComputedStyle(e).overflow !== 'visible')
                    bad.push((e.getAttribute('data-i18n') || e.className) +
                             ':' + e.scrollWidth + '>' + e.clientWidth);
                });
                return bad; })()""")
            check(not clipped, f"[{lang}] no clipped text",
                  ", ".join(clipped[:3]) or "clean")

            overflow = await pg.evaluate(
                "document.documentElement.scrollWidth - document.documentElement.clientWidth")
            check(overflow <= 1, f"[{lang}] no horizontal overflow", f"{overflow}px")
        await browser.close()


def pixel_checks() -> None:
    for tag, path in (('en', 'docs/dashboard.png'), ('fa', 'docs/dashboard-fa.png')):
        im = Image.open(path).convert('L')
        width, height = im.size
        check((width, height) == (1920, 1380),
              f"[{tag}] screenshot dimensions", f"{width}x{height}")

        flat = []
        for gy in range(3):
            for gx in range(4):
                box = (gx * width // 4, gy * height // 3,
                       (gx + 1) * width // 4, (gy + 1) * height // 3)
                sd = ImageStat.Stat(im.crop(box)).stddev[0]
                if sd < 3.0:
                    flat.append(f"cell({gx},{gy}) sd={sd:.1f}")
        check(not flat, f"[{tag}] no flat/blank regions",
              ", ".join(flat) or "all cells textured")

        kb = os.path.getsize(path) // 1024
        check(kb < 2048, f"[{tag}] file size reasonable for a README", f"{kb} KB")


if __name__ == '__main__':
    asyncio.run(dom_checks())
    pixel_checks()
    print()
    if fails:
        print("SCREENSHOT VERIFICATION FAILED: " + ", ".join(fails))
        sys.exit(1)
    print("ALL SCREENSHOT CHECKS PASSED")
