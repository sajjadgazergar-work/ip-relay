"""Nail down two remaining questions with actual numbers.

1. Topology churn: netNodes / netParticles are `let` bindings at script scope,
   so `window.netNodes` is undefined — read them by bare name instead (the
   earlier probe read them via window and got a false 0).
2. Which rAF loop keeps running in a hidden tab.
3. Does the .flash class restart force a synchronous layout on every stat tick?
"""
import json
import pathlib

from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:18080"
KEY = json.load(open("/root/opencode-rotator/settings.json"))["relay_api_key"]


def find_chrome():
    return str(sorted(pathlib.Path("/").glob("root/chrome/*/chrome-linux64/chrome"))[-1])


with sync_playwright() as p:
    br = p.chromium.launch(executable_path=find_chrome(),
                           args=["--no-sandbox", "--disable-dev-shm-usage",
                                 "--use-gl=swiftshader", "--enable-unsafe-swiftshader"])
    ctx = br.new_context(viewport={"width": 1440, "height": 900})
    ctx.add_init_script(f"localStorage.setItem('relay_key', {json.dumps(KEY)});")
    page = ctx.new_page()
    page.goto(BASE, wait_until="load")
    page.wait_for_timeout(3000)

    print("scope check:",
          page.evaluate("({viaWindow: typeof window.netNodes, viaName: typeof netNodes})"))
    print("topology state:", page.evaluate(
        "({nodes: netNodes.length, particles: netParticles.length,"
        " addrs: netNodes.map(n => n.addr)})"))

    # count how many times each loop advances while hidden
    page.evaluate("""(() => {
      window.__c = {topo: 0};
      // count topology frames by hooking the clearRect it calls once per frame
      const cr = netCtx.clearRect.bind(netCtx);
      netCtx.clearRect = (...a) => { window.__c.topo++; return cr(...a); };
      return 'hooked';
    })()""")
    page.evaluate("window.__c.topo = 0")
    page.wait_for_timeout(3000)
    visible_frames = page.evaluate("window.__c.topo")

    page.evaluate("Object.defineProperty(document,'hidden',{get:()=>true,configurable:true});"
                  "document.dispatchEvent(new Event('visibilitychange'));")
    page.evaluate("window.__c.topo = 0")
    page.wait_for_timeout(3000)
    hidden_frames = page.evaluate("window.__c.topo")
    print(f"topology canvas frames / 3s: visible={visible_frames}  hidden={hidden_frames}"
          "   (hidden>0 means the loop never pauses)")

    # forced layout from the .flash restart (void el.offsetWidth)
    page.evaluate("""
      window.__layouts = 0;
      const proto = Object.getPrototypeOf(document.body);
      const d = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'offsetWidth');
      Object.defineProperty(HTMLElement.prototype, 'offsetWidth', {
        get() { window.__layouts++; return d.get.call(this); }, configurable: true });
    """)
    page.evaluate("Object.defineProperty(document,'hidden',{get:()=>false,configurable:true});"
                  "document.dispatchEvent(new Event('visibilitychange'));")
    page.wait_for_timeout(11000)
    print("forced offsetWidth reads in 11s (each = sync layout):",
          page.evaluate("window.__layouts"))

    print("stat pills per row:", page.evaluate("""
      Array.from(document.querySelectorAll('.stats-row')).map(r => r.children.length)"""))
    print("grid columns:", page.evaluate(
        "getComputedStyle(document.querySelector('.stats-row')).gridTemplateColumns"))
    br.close()
