"""A/B: how much of the frame cost is the WebGL backdrop vs the rest.

Same browser, same page, three conditions:
  A) as shipped
  B) shader canvas removed (topology + CSS only)
  C) shader + topology both removed (DOM/blur only)

The VPS has no GPU (SwiftShader), so absolute ms are pessimistic — the RATIO
between conditions is the signal, and it is the same ratio a weak laptop GPU sees.
"""
import json
import pathlib
import statistics

from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:18080"
KEY = json.load(open("/root/opencode-rotator/settings.json"))["relay_api_key"]

INSTRUMENT = """
window.__f = [];
(() => { let last = performance.now();
  function tick(t) { window.__f.push(t - last); last = t; requestAnimationFrame(tick); }
  requestAnimationFrame(tick); })();
"""


def find_chrome():
    hits = sorted(pathlib.Path("/").glob("root/chrome/*/chrome-linux64/chrome"))
    if not hits:
        hits = sorted(pathlib.Path("/").glob("root/.cache/ms-playwright/chromium-*/chrome-linux*/chrome"))
    return str(hits[-1])


def stats(frames):
    if len(frames) < 3:
        return "n/a"
    fs = sorted(frames)
    return (f"n={len(fs)} median={statistics.median(fs):6.1f}ms "
            f"p95={fs[int(0.95*(len(fs)-1))]:7.1f}ms fps~{1000/statistics.median(fs):4.1f}")


with sync_playwright() as p:
    br = p.chromium.launch(executable_path=find_chrome(),
                           args=["--no-sandbox", "--disable-dev-shm-usage",
                                 "--use-gl=swiftshader", "--enable-unsafe-swiftshader"])
    ctx = br.new_context(viewport={"width": 1440, "height": 900})
    ctx.add_init_script(f"localStorage.setItem('relay_key', {json.dumps(KEY)});")
    ctx.add_init_script(INSTRUMENT)
    page = ctx.new_page()
    page.goto(BASE, wait_until="load")
    page.wait_for_timeout(2500)

    page.evaluate("window.__f = []")
    page.wait_for_timeout(6000)
    print("A) as shipped              :", stats(page.evaluate("window.__f")))

    page.evaluate("const c=document.getElementById('canvas-bg'); if(c) c.remove();")
    page.wait_for_timeout(1200)
    page.evaluate("window.__f = []")
    page.wait_for_timeout(6000)
    print("B) minus WebGL backdrop    :", stats(page.evaluate("window.__f")))

    page.evaluate("const n=document.getElementById('networkCanvas'); if(n) n.remove();")
    page.wait_for_timeout(1200)
    page.evaluate("window.__f = []")
    page.wait_for_timeout(6000)
    print("C) minus topology canvas too:", stats(page.evaluate("window.__f")))

    page.evaluate("""document.querySelectorAll('*').forEach(el => {
        el.style.backdropFilter = 'none'; el.style.webkitBackdropFilter = 'none'; });
        const o=document.querySelector('.noise-overlay'); if(o) o.remove();""")
    page.wait_for_timeout(1200)
    page.evaluate("window.__f = []")
    page.wait_for_timeout(6000)
    print("D) minus blur + noise too  :", stats(page.evaluate("window.__f")))
    br.close()
