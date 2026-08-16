"""UA A/B on a SINGLE fresh proxy egress IP.

Direct-from-VPS is useless for this test once the VPS budget is spent: every UA
429s and the result proves nothing. So: pick a warm lane from the live pool,
then alternate opencode-UA / other-UA requests through THAT one IP. If the gate
is the UA, the opencode ones pass while the others 429 on the same IP, in the
same seconds. If the gate is purely the IP, both behave identically.
"""
import json
import sys
import time
import urllib.request
import urllib.error

S = json.load(open('/root/opencode-rotator/settings.json'))
RELAY_KEY = S['relay_api_key']
UP_KEY = S['upstream_api_key'].split(',')[0].strip()
BASE = S['upstream_base_url']

req = urllib.request.Request("http://127.0.0.1:18080/api/pool",
                             headers={"Authorization": f"Bearer {RELAY_KEY}"})
pool = json.load(urllib.request.urlopen(req, timeout=15))
warm = [lane for lane in pool['warm']
        if lane['proto'] in ('http', 'https', 'socks5', 'socks4')]
if not warm:
    sys.exit("no warm lanes in the pool right now — rerun in a minute")
lane = warm[0]
scheme = 'socks5h' if lane['proto'].startswith('socks') else 'http'
PROXY = f"{scheme}://{lane['addr']}"
print(f"egress IP under test: {lane['proto']}://{lane['addr']}  (lat {lane['lat_ms']}ms)\n")

opener = urllib.request.build_opener(
    urllib.request.ProxyHandler({'http': PROXY, 'https': PROXY}))

PAIRS = [
    ("opencode/1.0", "python-httpx/0.27.0"),
    ("opencode", "curl/8.5.0"),
    ("OpenCode/1.0", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/127.0.0.0"),
    ("opencode-ai/1.0", "axios/1.7.2"),
    ("opencode/1.0 (linux; x64)", "Go-http-client/2.0"),
]


def call(ua):
    h = {"Authorization": f"Bearer {UP_KEY}", "Content-Type": "application/json", "User-Agent": ua}
    r = urllib.request.Request(
        BASE + "/chat/completions",
        data=json.dumps({"model": "deepseek-v4-flash-free",
                         "messages": [{"role": "user", "content": "hi"}],
                         "max_tokens": 1}).encode(), headers=h)
    try:
        resp = opener.open(r, timeout=70)
        resp.read()
        return 200, ""
    except urllib.error.HTTPError as e:
        b = e.read()[:60].decode("utf8", "ignore").replace("\n", " ")
        return e.code, b
    except Exception as e:
        return "ERR", type(e).__name__


rows = []
for good, bad in PAIRS:
    for ua in (good, bad):
        st, msg = call(ua)
        tag = "opencode-ish" if "opencode" in ua.lower() else "other      "
        print(f"{st!s:>4}  [{tag}] {ua[:52]!r:<56} {msg}", flush=True)
        rows.append(("oc" if "opencode" in ua.lower() else "other", st))
        time.sleep(3)

oc = [s for k, s in rows if k == "oc"]
ot = [s for k, s in rows if k == "other"]
print(f"\nopencode UAs: {oc.count(200)}/{len(oc)} 200    other UAs: {ot.count(200)}/{len(ot)} 200")
