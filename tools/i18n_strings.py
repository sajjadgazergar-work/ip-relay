"""Single source of truth for the dashboard's translatable strings.

Why a Python file and not JSON: this table is consumed by TWO things — the
annotator (tools/i18n_annotate.py, which stamps data-i18n="key" onto the
markup) and the Persian linter (tools/persian_lint_dashboard.py). Keeping the
English original next to its key means the annotator can find the element by its
CURRENT text, so re-running it after an English copy edit is safe.

Persian rules applied (Sajjad's, see the persian-localization skill):
  * Persian letterforms only — ک U+06A9 and ی U+06CC, never the Arabic kaf/yeh.
  * ZWNJ (U+200C) where the word demands it: می‌شود, نمی‌ماند, لِیس‌ها.
  * Persian punctuation: ، ؛ ؟ — never the ASCII comma.
  * Latin technical nouns stay Latin (Tor, SOCKS, Webshare, opencode) with one
    plain space either side and NO ZWNJ touching them.
  * Digits stay ASCII: they sit beside Latin product names and monospace
    telemetry, and mixing Persian digits into a metrics readout is unreadable.
  * Sentence starts in Persian; at most ~2 English words per sentence.

Each entry: (key, english, persian)
"""

STRINGS: list[tuple[str, str, str]] = [
    # ── chrome / header ────────────────────────────────────────────
    ("a11y.skip", "Skip to main content", "پرش به محتوای اصلی"),
    ("hdr.tagline", "resilient egress rotator", "چرخاننده‌ی مقاوم مسیر خروج"),
    ("hdr.baseurl", "Base URL:", "آدرس پایه:"),
    ("hdr.loading", "loading…", "در حال بارگذاری…"),
    ("hdr.provider_setup", "Provider setup", "راه‌اندازی سرویس‌دهنده"),
    ("hdr.relay_status", "Relay status:", "وضعیت رله:"),
    ("hdr.connecting", "connecting…", "در حال اتصال…"),

    # ── metrics ───────────────────────────────────────────────────
    ("m.section", "Live metrics", "سنجه‌های زنده"),
    ("m.warm", "Warm lanes", "مسیرهای آماده"),
    ("m.tokens_today", "Tokens today", "توکن امروز"),
    ("m.p95", "p95 latency", "تأخیر p95"),
    ("m.window5", "5 min window", "بازه‌ی 5 دقیقه‌ای"),
    ("m.rpm", "Requests / min", "درخواست در دقیقه"),
    ("m.subnets", "Unique /24 subnets", "زیرشبکه‌های /24 یکتا"),
    ("m.realwidth", "real width", "پهنای واقعی"),
    ("m.success", "Success rate", "نرخ موفقیت"),
    ("m.tpm", "Tokens / min", "توکن در دقیقه"),
    ("m.uptime", "Uptime", "زمان کارکرد"),
    ("m.bestlane", "Best lane", "بهترین مسیر"),
    ("m.probes", "Probes ok / burned", "پروب موفق / سوخته"),
    ("m.supply", "Egress supply", "منبع خروج"),
    ("m.supply_sub", "Tor circuits &amp; burn memory", "مدارهای Tor و حافظه‌ی سوخته"),

    # ── topology ──────────────────────────────────────────────────
    ("t.section", "Live egress topology", "توپولوژی زنده‌ی خروج"),
    ("t.engine", "ENGINE", "موتور"),
    ("t.idle", "idle", "بی‌کار"),
    ("t.queue", "QUEUE", "صف"),
    ("t.tested", "TESTED", "آزموده"),
    ("t.warm", "WARM", "آماده"),
    ("t.fails", "FAILS", "خطاها"),
    ("t.streams", "STREAMS", "استریم‌ها"),
    ("t.conc", "CONC", "همروندی"),
    ("t.refresh", "Refresh proxies now", "به‌روزرسانی پروکسی‌ها"),
    ("t.empty", "no warm lanes yet — discovering egress pathways…",
     "هنوز مسیر آماده‌ای نیست — مسیرهای خروج در حال کشف است…"),
    ("t.fast", "fast &lt;1.5s", "سریع &lt;1.5s"),
    ("t.medium", "medium &lt;4s", "متوسط &lt;4s"),
    ("t.slow", "slow", "کند"),
    ("t.core", "relay core", "هسته‌ی رله"),
    ("t.a11y", "Topology visualisation. The egress lanes table contains the same data in text form.",
     "نمای توپولوژی. جدول مسیرهای خروج همین داده‌ها را به شکل متنی دارد."),

    # ── connect guide ─────────────────────────────────────────────
    ("g.section", "Connect your application", "اتصال برنامه‌ی شما"),

    # ── lanes table ───────────────────────────────────────────────
    ("l.section", "Egress lanes", "مسیرهای خروج"),
    ("l.caption", "Warm and parked egress lanes with score, latency, subnet and usage counters",
     "مسیرهای آماده و پارک‌شده همراه با امتیاز، تأخیر، زیرشبکه و شمارنده‌های مصرف"),
    ("l.addr", "Address", "نشانی"),
    ("l.proto", "Proto", "پروتکل"),
    ("l.score", "Score", "امتیاز"),
    ("l.latency", "Latency", "تأخیر"),
    ("l.load", "Load", "بار"),
    ("l.okfail", "OK / Fail", "موفق / ناموفق"),
    ("l.discovering", "Discovering egress pathways…", "کشف مسیرهای خروج…"),
    ("l.prev", "‹ Prev", "‹ قبلی"),
    ("l.next", "Next ›", "بعدی ›"),

    # ── diagnostics ───────────────────────────────────────────────
    ("d.section", "Diagnostics", "عیب‌یابی"),
    ("d.rerun", "Re-run", "اجرای مجدد"),
    ("d.running", "Running checks…", "در حال بررسی…"),
    ("d.collecting", "Collecting probe results and configuration warnings.",
     "گردآوری نتایج پروب و هشدارهای تنظیمات."),
    ("d.breakdown", "Probe failure breakdown", "تفکیک خطاهای پروب"),
    ("d.nodata", "no data yet", "هنوز داده‌ای نیست"),

    # ── settings ──────────────────────────────────────────────────
    ("set.section", "Rotator settings", "تنظیمات چرخاننده"),
    ("set.language", "Interface", "رابط کاربری"),
    ("set.lang.label", "Dashboard language", "زبان داشبورد"),
    ("set.lang.hint",
     "Applies instantly. Persian switches the layout to right-to-left; numbers stay Latin.",
     "بی‌درنگ اعمال می‌شود. فارسی چیدمان را راست‌به‌چپ می‌کند؛ اعداد لاتین می‌مانند."),
    ("set.upstream", "Upstream provider", "سرویس‌دهنده‌ی بالادست"),
    ("set.baseurl", "Base URL", "آدرس پایه"),
    ("set.upkey", "Upstream API key", "کلید API بالادست"),
    ("set.probemodel", "Probe model", "مدل پروب"),
    ("set.probemodel.hint", "Cheapest model on the upstream — used for every lane probe.",
     "ارزان‌ترین مدل بالادست — برای پروب همه‌ی مسیرها استفاده می‌شود."),
    ("set.ua", "Upstream User-Agent", "عامل کاربر بالادست"),
    ("set.pooltarget", "Target pool size", "اندازه‌ی هدف استخر"),
    ("set.pooltarget.hint", "Lanes to maintain", "تعداد مسیری که نگه داشته می‌شود"),
    ("set.conc", "Test concurrency", "همروندی آزمون"),
    ("set.conc.hint", "Simultaneous lane checks", "بررسی هم‌زمان مسیرها"),
    ("set.conc.warn", "Residential / CPE links: keep this at 15–25 or every probe fails.",
     "روی خطوط خانگی یا CPE این مقدار را بین 15 تا 25 نگه دارید وگرنه همه‌ی پروب‌ها شکست می‌خورند."),
    ("set.webshare", "Webshare API tokens", "توکن‌های API وب‌شر"),
    ("set.webshare.hint", "Stored locally in settings.json (git-ignored). Shown masked after saving.",
     "به‌صورت محلی در settings.json ذخیره می‌شود و در git نمی‌رود. پس از ذخیره پوشیده نشان داده می‌شود."),
    ("set.direct", "Direct connection fallback", "اتصال مستقیم به‌عنوان جایگزین"),
    ("set.direct.hint", "Use this host's own IP when every proxy fails",
     "وقتی همه‌ی پروکسی‌ها شکست خوردند، از IP خود این میزبان استفاده کن"),
    ("set.advanced", "Advanced settings", "تنظیمات پیشرفته"),
    ("set.relaykey", "Relay API key", "کلید API رله"),
    ("set.show", "Show", "نمایش"),
    ("set.relaykey.hint",
     "Saving a key here applies it to the relay and stores it as this browser's key.",
     "ذخیره‌ی کلید در این‌جا آن را روی رله اعمال می‌کند و به‌عنوان کلید همین مرورگر نگه می‌دارد."),
    ("set.socks", "Allow SOCKS sources", "پذیرش منابع SOCKS"),
    ("set.socks.hint", "Include SOCKS4/5 proxies in the candidate pool",
     "پروکسی‌های SOCKS4/5 هم در استخر نامزدها بیایند"),
    ("set.adaptive", "Adaptive concurrency", "همروندی سازگارشو"),
    ("set.adaptive.hint", "Back off automatically when probes fail en masse",
     "وقتی پروب‌ها دسته‌جمعی شکست خوردند، خودکار عقب بکش"),
    ("set.persist", "Remember lanes across restarts", "یادآوری مسیرها پس از راه‌اندازی مجدد"),
    ("set.persist.hint", "Warm-start from lanes.json instead of rebuilding the pool",
     "شروع گرم از lanes.json به‌جای ساخت دوباره‌ی استخر"),
    ("set.inflight", "Per-lane concurrency", "همروندی هر مسیر"),
    ("set.subnetcap", "Max lanes per /24", "بیشترین مسیر در هر /24"),
    ("set.maxcand", "Max candidates", "بیشترین تعداد نامزد"),
    ("set.probetimeout", "Probe timeout (s)", "مهلت پروب (ثانیه)"),
    ("set.relaytimeout", "Relay timeout (s)", "مهلت رله (ثانیه)"),
    ("set.attempts", "Relay attempts", "تلاش‌های رله"),
    ("set.cooldown", "Lane cooldown (s)", "استراحت مسیر (ثانیه)"),
    ("set.recover", "Lane recover (s)", "بازیابی مسیر (ثانیه)"),
    ("set.pin", "Pinned lanes (drain)", "مسیرهای سنجاق‌شده (تخلیه)"),
    ("set.supply", "Egress supply — Tor &amp; burn memory", "منبع خروج — Tor و حافظه‌ی سوخته"),
    ("set.supply.note",
     "Measured: a 429'd egress IP never recovers, and one live Tor exit serves 40+ requests. "
     "So spent IPs are remembered instead of re-probed, and Tor circuits supply fresh exits on "
     "demand (1,384 exit IPs, ~65% usable vs ~2% for scraped lists).",
     "اندازه‌گیری شد: یک IP خروج که 429 گرفته هرگز برنمی‌گردد و یک خروج زنده‌ی Tor بیش از 40 "
     "درخواست را سرویس می‌دهد. پس IPهای سوخته به‌جای پروب دوباره به یاد می‌مانند و مدارهای Tor "
     "خروج تازه را در لحظه تأمین می‌کنند (1384 آدرس خروج، حدود 65% قابل استفاده در برابر حدود 2% "
     "برای فهرست‌های اسکرپ‌شده)."),
    ("set.tor", "Tor egress lanes", "مسیرهای خروج Tor"),
    ("set.enabled", "enabled", "فعال"),
    ("set.disabled", "disabled", "غیرفعال"),
    ("set.torlanes", "Tor circuit lanes", "مسیرهای مدار Tor"),
    ("set.torport", "Tor SOCKS port", "درگاه SOCKS مربوط به Tor"),
    ("set.burn", "Burn memory", "حافظه‌ی سوخته"),
    ("set.burn.on", "remember spent IPs", "IPهای مصرف‌شده را به یاد بسپار"),
    ("set.burn.off", "disabled (re-probe everything)", "غیرفعال (همه دوباره پروب شوند)"),
    ("set.burnttl", "Burn TTL (s)", "عمر حافظه‌ی سوخته (ثانیه)"),
    ("set.apply", "Apply &amp; save", "اعمال و ذخیره"),
    ("set.validate", "Validate keys", "بررسی کلیدها"),
    ("set.keyverify", "Key verification", "بررسی صحت کلید"),
    ("set.up_pending", "Upstream: pending", "بالادست: در انتظار"),

    # ── log pane ──────────────────────────────────────────────────
    ("log.section", "Telemetry trace", "ردیابی سنجه‌ها"),
    ("log.filter", "Filter", "پالایه"),
    ("log.errors", "Errors only", "فقط خطاها"),
    ("log.autoscroll", "Auto-scroll", "پیمایش خودکار"),
    ("log.copy", "Copy", "رونوشت"),
    ("log.clear", "Clear", "پاک‌سازی"),
    ("log.listening", "Listening for telemetry…", "در انتظار سنجه‌ها…"),

    # ── footer / modals ───────────────────────────────────────────
    ("foot.tagline", "ip-relay v__VERSION__ — provider-agnostic egress rotator",
     "ip-relay v__VERSION__ — چرخاننده‌ی خروج مستقل از سرویس‌دهنده"),
    ("auth.title", "Relay key required", "کلید رله لازم است"),
    ("auth.body",
     "The relay rejected this request (401). Enter the relay API key to reconnect — live updates "
     "resume automatically.",
     "رله این درخواست را رد کرد (401). برای اتصال مجدد کلید API رله را وارد کنید — به‌روزرسانی "
     "زنده خودکار ادامه می‌یابد."),
    ("auth.save", "Save &amp; retry", "ذخیره و تلاش مجدد"),
    ("prov.body",
     "Pick an upstream to preload its base URL and probe model. Your API key is never overwritten "
     "with a blank.",
     "یک بالادست انتخاب کنید تا آدرس پایه و مدل پروب آن پیش‌بارگذاری شود. کلید API شما هرگز با "
     "مقدار خالی بازنویسی نمی‌شود."),
    ("prov.close", "Close", "بستن"),
]

# Attribute strings (placeholder / aria-label / title).
ATTRS: list[tuple[str, str, str]] = [
    ("attr.filter_ph", "filter…", "پالایش…"),
    ("attr.search_ph", "search address or subnet…", "جست‌وجوی نشانی یا زیرشبکه…"),
]

# Strings built in JS at runtime (toasts, status lines, table chips).
JS_STRINGS: list[tuple[str, str, str]] = [
    # Diagnostics verdicts. The server sends an English slug + English advice
    # prose; the slug is the stable key, so translate on the client and fall
    # back to the server text for any verdict added later on the backend.
    ("diag.v.healthy", "healthy", "سالم"),
    ("diag.v.warming_up", "warming up", "در حال گرم شدن"),
    ("diag.v.egress_blocked", "egress blocked", "خروج مسدود است"),
    ("diag.v.bad_upstream_key", "bad upstream key", "کلید بالادست نامعتبر"),
    ("diag.v.upstream_blocks_proxies", "upstream blocks proxies",
     "بالادست پروکسی‌ها را رد می‌کند"),
    ("diag.v.quota_exhausted", "quota exhausted", "سهمیه تمام شد"),
    ("diag.v.no_usable_proxies", "no usable proxies", "پروکسی قابل استفاده نیست"),
    ("diag.a.healthy", "Pool is serving traffic.", "استخر در حال سرویس‌دهی است."),
    ("diag.a.warming_up", "No probe results yet — the pool is still fetching and testing candidates.",
     "هنوز نتیجه‌ی پروبی نیست — استخر همچنان نامزدها را می‌گیرد و می‌آزماید."),
    ("diag.a.egress_blocked",
     "Probes cannot open outbound connections to proxy ports. A VPN, firewall, or ISP filter is "
     "blocking non-80/443 traffic, or the link's NAT table is saturated. Lower "
     "proxy_test_concurrency (15–25) and retry without the VPN.",
     "پروب‌ها نمی‌توانند به درگاه‌های پروکسی اتصال بیرونی باز کنند. یک VPN، فایروال یا پالایه‌ی "
     "اپراتور ترافیک غیر 80/443 را می‌بندد، یا جدول NAT خط پر شده است. مقدار "
     "proxy_test_concurrency را به 15 تا 25 کم کنید و بدون VPN دوباره تلاش کنید."),
    ("diag.a.bad_upstream_key", "The upstream rejects the API key (401). Fix upstream_api_key.",
     "بالادست کلید API را رد می‌کند (401). مقدار upstream_api_key را درست کنید."),
    ("diag.a.upstream_blocks_proxies",
     "The upstream 403s these egress IPs (datacenter/VPN ranges are often blocked).",
     "بالادست به این IPهای خروج پاسخ 403 می‌دهد؛ محدوده‌های مرکز داده و VPN اغلب بسته‌اند."),
    ("diag.a.quota_exhausted", "The key's own quota is exhausted; every IP 429s. Use a private key.",
     "سهمیه‌ی خود کلید تمام شده و هر IP پاسخ 429 می‌گیرد. از یک کلید خصوصی استفاده کنید."),
    ("diag.a.no_usable_proxies",
     "Candidates connect but fail verification. Check the reason breakdown.",
     "نامزدها وصل می‌شوند اما در بررسی رد می‌شوند. تفکیک دلایل را ببینید."),

    ("js.fix_invalid", "Fix the invalid fields before applying",
     "پیش از اعمال، فیلدهای نامعتبر را درست کنید"),
    ("js.saving", "Saving…", "در حال ذخیره…"),
    ("js.settings_saved", "Settings applied", "تنظیمات اعمال شد"),
    ("js.copied", "Copied to clipboard", "در بریده‌دان رونوشت شد"),
    ("js.cleared", "Log cleared", "گزارش پاک شد"),
    ("js.refresh_started", "Proxy refresh started", "به‌روزرسانی پروکسی‌ها آغاز شد"),
    ("js.diag_refreshed", "Diagnostics refreshed", "عیب‌یابی به‌روز شد"),
    ("js.operational", "operational", "عملیاتی"),
    ("js.lanes_suffix", "lanes", "مسیر"),
    ("js.discovering", "discovering routes…", "کشف مسیرها…"),
    ("js.quota_exhausted", "quota exhausted · retry in", "سهمیه تمام شد · تلاش مجدد در"),
    ("js.paused_quota", "paused (quota)", "متوقف (سهمیه)"),
    ("js.probing", "probing", "در حال پروب"),
    ("js.queued", "queued", "در صف"),
    ("js.idle_warm", "idle (pool warm)", "بی‌کار (استخر آماده)"),
    ("js.scraping", "scraping feeds", "خواندن فهرست‌ها"),
    ("js.no_match", "No lines match the filter.", "هیچ خطی با پالایه نمی‌خواند."),
    ("js.cooldown", "cooldown", "استراحت"),
    ("js.now", "now", "اکنون"),
    ("js.try", "try", "تلاش"),
    ("js.ago", "ago", "پیش"),
    ("js.probed", "probed", "پروب‌شده"),
    ("js.requests_total", "requests total", "درخواست در کل"),
    ("js.failovers", "failovers", "جابه‌جایی اضطراری"),
    ("js.errors_of", "errors /", "خطا از"),
    ("js.req", "req", "درخواست"),
    ("js.in_prefix", "in", "ورودی"),
    ("js.out_prefix", "out", "خروجی"),
    ("js.lifetime", "lifetime", "از ابتدا"),
    ("js.per_req", "/req", "به‌ازای هر درخواست"),
    ("js.tor_off", "tor off", "Tor خاموش"),
    ("js.tor_unreachable", "tor UNREACHABLE", "Tor دست‌نیافتنی"),
    ("js.rotations", "rotations", "چرخش"),
    ("js.burned_ips", "burned IPs", "IP سوخته"),
    ("js.burn_off", "burn off", "حافظه‌ی سوخته خاموش"),
    ("js.fast", "fast", "سریع"),
    ("js.medium", "med", "متوسط"),
    ("js.slow", "slow", "کند"),
    ("js.no_failures", "no probe failures", "خطای پروبی نیست"),
    ("js.no_warnings", "No configuration warnings.", "هشدار تنظیماتی نیست"),
    ("js.loading", "Loading…", "در حال بارگذاری…"),
    ("js.profiles_fail", "Could not load profiles.", "بارگذاری نمایه‌ها ممکن نشد."),
    ("js.no_webshare", "No Webshare accounts configured.", "هیچ حساب Webshare تنظیم نشده است."),
    ("js.valid", "valid", "معتبر"),
    ("js.failed", "failed", "ناموفق"),
    ("js.models", "models", "مدل"),
    ("js.proxies", "proxies", "پروکسی"),
    ("js.in_window", "in", "در"),
    ("js.min", "min", "دقیقه"),
    ("js.polling_fallback", "polling fallback", "بازگشت به نظرسنجی دوره‌ای"),

    # These three overwrite an element that ALSO carries data-i18n. The attribute
    # alone is not enough: the render path rewrites textContent on every poll, so
    # the English rebuilt itself ~5s after a language switch. Verified by the
    # browser harness (hdr.loading, m.realwidth, t.idle were the last holdouts).
    ("js.parked", "parked", "پارک‌شده"),
    ("js.slots", "slots", "جایگاه"),
    ("js.concentrated", "concentrated", "متمرکز"),
    ("js.lanes_per_24", "lanes per /24", "مسیر در هر /24"),
    ("js.real_pool_width", "real pool width", "پهنای واقعی استخر"),
    ("js.eng.paused_quota", "paused (quota)", "متوقف (سهمیه)"),
    ("js.eng.probing", "probing", "در حال پروب"),
    ("js.eng.queued", "queued", "در صف"),
    ("js.eng.idle_warm", "idle (pool warm)", "بی‌کار (استخر آماده)"),
    ("js.eng.scraping", "scraping feeds", "در حال جمع‌آوری از منابع"),
    ("js.foot.warm", "warm", "آماده"),
    ("js.foot.subnets", "subnets", "زیرشبکه"),
    ("js.foot.updated", "updated", "به‌روزرسانی"),
]
