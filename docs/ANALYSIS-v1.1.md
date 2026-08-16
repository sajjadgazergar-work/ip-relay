# ip-relay v1.1 — current-state analysis & update plan

Analysis date: 2026-08-16. Live service: `oc-rotator.service`, v1.0.0, uptime 21h, port 18080.
Baseline verified before touching anything: **140/140 tests pass**, working tree clean, `HEAD == origin/main` (`a471946`).

---

## 0. What exists today (the "don't break it" inventory)

| Asset | Shape | Fragility |
|---|---|---|
| `ip_relay.py` | 3150 LOC, single module | `Lane` uses `__slots__` — new fields need slots + `to_dict`/`from_dict` |
| `dashboard.html` | 2697 LOC (639 CSS / 587 markup / 1471 JS), 125 KB served inline | read once at import → **edits need a service restart** |
| `REQ_LOG` | `deque(maxlen=2000)` of 5-tuples `(ts, lane, status, ms, stream)` | unpacked positionally in 3 places; **tests append raw 5-tuples** (`test_v08.py:490`) |
| `STATS` | flat lifetime counter dict | **not persisted** — every restart zeroes it |
| `lanes.json` / `burned.json` | persisted pool + spent-IP memory | `from_dict` uses `.get()` defaults → forward-compatible, safe to extend |
| `tools/verify_dashboard*.py` | Playwright assertions on **English** strings/selectors | Persian UI breaks these unless language stays togglable |
| `__VERSION__` | placeholder replaced at serve time, 2 occurrences | keep |

Live pool right now: 16 warm / 60 parked, 16 unique /24s, 1513 lifetime requests, 268 streams, Tor 12 circuits (0 warm), 273 burned IPs.

---

## 1. Persian language — nothing exists yet

Current state: `<html lang="en">`, no `dir`, zero i18n scaffolding. String inventory I counted mechanically:

- **136** visible text nodes in markup
- **12** `aria-label` / `title` / `placeholder` attributes
- **~86** prose string literals inside JS (toasts, status text, engine states, validation messages)

≈ **230 strings** total. That is a one-pass job, but only if it is done with a dictionary rather than by hand-editing the markup — otherwise the next feature silently ships English.

### Decision: bilingual with a toggle, not a hard switch
The repo is public and English-facing (both `README.md` and `README.fa.md` exist), and both `verify_dashboard*.py` harnesses assert on English strings. A hard replace makes the verification tooling useless and closes the project to non-Persian users. Plan: `I18N = {en:{...}, fa:{...}}`, `data-i18n` attributes on every text node, `?lang=fa` + `localStorage` persistence, default from `navigator.language` with `fa` winning when ambiguous.

### The font problem is the real work, not the translation
- Current fonts are Inter + Roboto Mono from **Google Fonts**. I measured the fetch from this box: **6.96s** for the CSS alone. From Iran that is render-blocking on every single load, and it's geo-fragile.
- Persian needs a real Persian face — **Vazirmatn** (SIL OFL, correct ZWNJ handling, matching Latin, tabular digits).
- There is **no static asset route and no `static/` directory** — everything is inline. So: add a `/static` mount, self-host a Vazirmatn woff2 subset, drop the Google Fonts `<link>` entirely. This also fixes load time for the English UI.

### RTL is mostly free, with four specific exceptions
The layout is grid/flex and direction-agnostic, but these are hardcoded physical offsets and will land on the wrong side under `dir="rtl"`:
- `.viz-legend { bottom: 9px; left: 12px }`
- `.skip-link { left: 50% }` + `translateX`
- `.toast-container`
- `.log-toolbar` / countdown alignment

Fix with logical properties (`inset-inline-start`), not with an RTL override sheet.

### Islands that must stay LTR inside an RTL page
Force `dir="ltr"` on: the telemetry trace pane, the lane table's address/subnet cells, all code snippets in the Connect guide, the base-URL chip, and every numeric/mono field. Per your own Farsi rules, digits stay **ASCII** — `font-variant-numeric: tabular-nums` already works and must be preserved.

---

## 2. Token meter — backend has zero token accounting

`STATS` counts requests, failovers, probes, streams, 429s. **No token data anywhere.**

### The good news: the upstream already gives us usage, measured live

Non-streaming (`POST /v1/chat/completions`, real call through the relay):
```json
"usage": {"prompt_tokens": 89, "completion_tokens": 20, "total_tokens": 109}
```

Streaming — and this is the important measurement — opencode zen emits usage **without** `stream_options.include_usage`:
```
data: {... "delta":{"content":"."}, "usage":{"prompt_tokens":87,"completion_tokens":20,"total_tokens":107}}
data: {... "choices":[], "usage":{"prompt_tokens":87,"completion_tokens":20,"total_tokens":107}}
data: [DONE]
data: {"choices":[],"cost":"0"}
```
So both paths are instrumentable with no upstream request changes. Usage rides on the final chunks, and there is a non-standard trailing `cost` frame after `[DONE]`.

### Where it plugs in

- **Non-stream** (`relay()`, line ~1935): body is already fully buffered at the `status == 200` point. Parse usage there. Trivial, zero risk.
- **Stream** (`relay_stream()`, the `chunks()` generator, line ~2097): must **tee** bytes — scan each chunk for a `usage` object while yielding it through **byte-identical and unbuffered**. Record in the generator's existing `finally` block, which already runs on close. This is the only delicate edit in the whole update: no added latency, no altered bytes, no swallowed exceptions.
- **Anthropic path**: `openai_to_anthropic()` already maps usage correctly. `openai_sse_to_anthropic()` **hardcodes `output_tokens: 0`** — a real bug worth fixing in the same pass.

### Three landmines to route around

1. **`REQ_LOG` tuple shape.** `sparkline_series()` does `for ts, _lane, status, ms, _stream in REQ_LOG` — a 6th element breaks that unpack, and tests append raw 5-tuples. Fix: index-based access (`row[0]`, `row[2]`…) plus default args on `record_request()` so all 140 existing tests keep passing untouched.
2. **`Lane.__slots__`.** Per-lane token columns need `tok_in`/`tok_out` in the slots tuple *and* in `to_dict`/`from_dict`. Old `lanes.json` files lack the keys → `.get(…, 0)` defaults keep them loadable.
3. **Restart amnesia.** A token meter that resets on every restart is decoration. `STATS` is currently not persisted at all, while lanes and burned IPs are — so the pattern already exists. Add `usage.json` with **daily buckets** (today / 7d / lifetime), capped at 30 days so it can't grow unbounded.

### Surfacing
Dashboard pill: `tokens today` as the headline number, `in / out` split as the sub-line, sparkline off the server-side buckets so it survives a page reload. Plus `iprelay_tokens_in_total` / `iprelay_tokens_out_total` on `/metrics` — **see the security note in §4 before exposing that publicly.**

---

## 3. Animation & layout — measured, not eyeballed

Ran a real Chromium (`tools/perf_probe.py`, `tools/perf_ab.py`) against the live dashboard and progressively removed layers. **Caveat, stated plainly:** this VPS has no GPU, so Chromium falls back to SwiftShader. The absolute ms are far worse than your laptop. **The ratios between conditions are the signal** — and they're the same ratios a weak/throttled GPU sees.

| Condition | Median frame | ~fps |
|---|---|---|
| A — as shipped | **816.6 ms** | 1.2 |
| B — minus the WebGL backdrop | 216.6 ms | 4.6 |
| C — minus topology canvas too | 225.0 ms | 4.4 |
| D — minus `backdrop-filter` + noise too | **50.0 ms** | 20.0 |

Read that top to bottom:

- **The fullscreen WebGL shader is ~74% of the frame cost.** It renders the entire viewport every rAF with a 3-iteration per-fragment loop, and it sits at `inset:0` *behind* **17 `backdrop-filter: blur(18–22px)` elements**. So every shader frame invalidates every glass panel and forces a re-blur of all 17 regions. The shader and the glass are not two independent costs — they multiply. That is the glitch.
- **The topology canvas is nearly free** (B→C changed nothing). It is not the problem, and the request "keep what's good" applies to it directly.
- **Blur + noise are the remaining half** (C→D: 225ms → 50ms, a 4.5× win).

### Four concrete bugs behind the "glitchy" feel

1. **Every animation is frame-rate-coupled.** `n.angle += n.speed`, `n.x += (n.tx - n.x) * 0.08`, `globalTime += 0.005`, `n.pulse -= 0.025` — all fixed-step-per-frame with no delta time. At 20fps the whole visualisation moves ~3× slower than at 60fps, so it doesn't degrade gracefully, it *lurches*. This is the classic stutter source and it's independent of raw GPU cost.
2. **The topology rAF loop never pauses on a hidden tab.** Measured: 17 frames in 3s with `document.hidden === true`. The shader loop *does* check `visibilitychange`; the topology loop doesn't. Burns CPU and battery in a background tab.
3. **No `ResizeObserver` on the topology canvas** — only `window.resize`. When the surrounding grid reflows (settings pane expanding, lane-table pagination changing card height) the canvas keeps a stale backing-store size and renders **stretched and blurry** until the window itself is resized. Directly visible as "glitchy".
4. **Ring layout clumps as lanes churn.** `angle: (i / count) * 2π` is assigned **only to new nodes**; existing nodes keep their original angle while `count` changes. With 16 warm lanes and 60 parked churning constantly, the ring becomes visibly lumpy and never redistributes. Same for `layer: i % 2`, frozen at creation.

### Layout findings
- 9 stat pills in rows of **4 / 4 / 1** — a flat wall of numbers with no hierarchy, and the last row is a single pill stretched by an inline `flex:1`. `repeat(auto-fit, minmax(150px,1fr))` computes to 7 tracks with 3 collapsed to 0px. Harmless, but it's why the rows look inconsistent.
- Document height **2367px** at 1440×900 — 2.6 viewports of scrolling to see the whole relay.
- Adding a token pill makes it **10** pills, so the row structure has to change anyway. Right moment to introduce hierarchy: 3 hero KPIs (warm lanes / tokens today / p95) + a compact secondary strip.

### Cheap non-visual win found along the way
No `GZipMiddleware` — the 125 KB dashboard ships **uncompressed on every load**. One line, ~4× smaller.

---

## 4. Blindspots you didn't ask about (worth 60 seconds each)

- **`/metrics` is unauthenticated and returns 200 to the world**, and port 18080 has no firewall rule. Today it leaks pool size and request totals. Adding token counters there would publish your exact quota consumption — useful to anyone timing their own abuse of the same free tier. Gate `/metrics` behind the relay key, or bind it to localhost, *before* the token counters land.
- **`/` serves the dashboard HTML unauthenticated** (200, 125 KB). Not a credential leak, but it advertises the version and full structure.
- **Restart amnesia on `STATS`** — worth fixing now that a number a human actually watches (tokens) depends on it.
- **`openai_sse_to_anthropic` reports `output_tokens: 0`.** Any Anthropic-protocol client doing its own token accounting through the relay is currently reading zeros.
- **`.flash` forced sync layout** (`void el.offsetWidth`) — I checked, it fires only **3 times in 11s**. Not a problem. Leaving it alone.

---

## 5. Plan (ordered, each step independently verifiable)

**Phase 0 — safety net**
1. Branch `v1.1`, tag the working v1.0.0 state.
2. Extend `tools/verify_dashboard_v10.py` into a v1.1 harness asserting on **stable selectors/ids**, not English copy, so it survives the Persian switch.

**Phase 1 — token meter (backend first, pure logic, fully testable)**
3. `record_request()` gains optional `tok_in`/`tok_out`; `REQ_LOG` access converted to index-based. Run the 140 tests — must stay green with zero test edits.
4. Parse usage in `relay()` (buffered, trivial) and tee it in `relay_stream()`'s `chunks()` generator (byte-identical passthrough, record in the existing `finally`).
5. `Lane.tok_in`/`tok_out` + slots + `to_dict`/`from_dict`, backward-compatible with existing `lanes.json`.
6. `usage.json`: today / 7d / lifetime daily buckets, 30-day cap.
7. Fix `openai_sse_to_anthropic` output_tokens.
8. Add token counters to `/metrics` **and gate `/metrics`** in the same commit.
9. New tests: usage parsing (stream + non-stream), bucket rollover, persistence round-trip, byte-identity of the streamed passthrough.

**Phase 2 — animation & layout (the part that must feel different)**
10. Replace the fullscreen WebGL shader with a **static CSS gradient mesh + one small animated element**, or keep the shader but render it at ≤0.5× resolution to an offscreen buffer and stop it whenever the tab is hidden or the pointer is idle. Target: kill the shader×blur multiplication.
11. Cut `backdrop-filter` from ~17 elements to ≤3 (headers/modals only); the stat pills and cards get solid `--surface-solid` with a border. This is the 4.5× win.
12. Delta-time all animation: pass `dt` through `animNetworkGraph`, convert every `+= constant` to `* dt`. Fixes stutter at every frame rate.
13. `visibilitychange` guard on the topology loop; `ResizeObserver` on the canvas.
14. Redistribute node angles on every `syncNetworkGraph` (animate to new targets) so the ring stays even under churn.
15. Layout: 3 hero KPIs + compact secondary strip; token pill integrated rather than appended.
16. Add `GZipMiddleware`.
17. Re-run `tools/perf_ab.py` and report the before/after table. Target: **as-shipped median under the current condition-D number (50ms)** on this same GPU-less box.

**Phase 3 — Persian**
18. `/static` mount + self-hosted Vazirmatn woff2 subset; delete the Google Fonts `<link>`.
19. `I18N` dictionary + `data-i18n` attributes; `?lang=` + localStorage; `fa` default with `en` intact.
20. Logical properties for the 4 physical-offset rules; `dir="ltr"` islands for logs/code/addresses/numbers; ASCII digits enforced.
21. Persian lint pass on all `fa` strings (ZWNJ, `ی`/`ک` codepoints, `،` punctuation, ≤2 English words per string, sentences starting in Persian).
22. Re-run the v1.1 harness in both languages + screenshots per state.

**Phase 4 — ship**
23. Full test suite, live smoke test of a real completion through the running relay, then `README.md` / `README.fa.md` updates and push to the `v1.1` branch (not `main`).

---

## Open question (one, and it changes Phase 3's shape)

Persian as the **default** with an EN toggle, or EN default with a FA toggle? You asked for Persian, so my default assumption is **FA default, EN kept** — the toggle exists so the public repo and the verification harnesses still work. Say the word if you want it FA-only and I'll drop the toggle and rewrite the harnesses instead.
