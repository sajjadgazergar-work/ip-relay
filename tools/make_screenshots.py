"""Regenerate the README dashboard screenshots (English + Persian).

Shoots at dpr=2 then downsamples to 1920px wide — GitHub renders the README
column at ~900px, so 1920 stays 2x-crisp at roughly half the bytes.

Run tools/verify_screenshots.py afterwards: it asserts the KPIs are populated,
the topology canvas actually painted, and no region came out blank.

    RELAY_KEY=... python3 tools/make_screenshots.py
"""
from __future__ import annotations

import asyncio
import glob
import os

from PIL import Image
from playwright.async_api import async_playwright

CHROME = glob.glob('/root/chrome/*/chrome-linux64/chrome')[0]
KEY = os.environ['RELAY_KEY']
URL = os.environ.get('RELAY_URL', 'http://127.0.0.1:18080')
SHOTS = (('en', 'docs/dashboard.png'), ('fa', 'docs/dashboard-fa.png'))


async def shoot() -> None:
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
        # Long settle: the lane table, sparklines and topology all need at least
        # one telemetry poll before they hold real values.
        await pg.wait_for_timeout(9000)
        for lang, path in SHOTS:
            await pg.evaluate(f"setLang('{lang}')")
            await pg.wait_for_timeout(3500)
            await pg.screenshot(path=path)
            direction = await pg.evaluate("document.documentElement.dir || 'ltr'")
            tokens = await pg.evaluate("(document.getElementById('cTokens')||{}).textContent")
            print(f"{lang} {path} dir={direction} tokens={tokens}")
        await browser.close()


def downsample() -> None:
    """Never commit the raw dpr=2 file — it is ~2.5 MB for no visible gain."""
    for _, path in SHOTS:
        im = Image.open(path).convert('RGB')
        im.thumbnail((1920, 1920), Image.LANCZOS)
        im.save(path, optimize=True)
        print(f"{path} {im.size} {os.path.getsize(path) // 1024} KB")


if __name__ == '__main__':
    asyncio.run(shoot())
    downsample()
