"""Stamp data-i18n keys onto dashboard.html markup, matching by current text.

Idempotent: an element that already carries data-i18n is skipped, so this can be
re-run after an English copy edit. Reports anything in the table it could NOT
find, which is the signal that the English text drifted and the key is now dead.

Only touches the markup region (<body> up to the first <script>) so it can never
rewrite a JS string literal that happens to match a UI string.
"""
from __future__ import annotations

import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))
from i18n_strings import STRINGS  # noqa: E402

DASH = os.path.join(os.path.dirname(__file__), "..", "dashboard.html")


def main() -> int:
    html = open(DASH, encoding="utf-8").read()
    body = html.index("<body")
    script = html.index("<script>", body)
    head, region, tail = html[:body], html[body:script], html[script:]

    stamped, missing = 0, []
    for key, en, _fa in STRINGS:
        if f'data-i18n="{key}"' in region:
            continue
        # Match an opening tag whose text content is exactly this string.
        # \s* on both sides tolerates the indentation/newlines in the source.
        pat = re.compile(
            r"(<(?P<tag>[a-zA-Z0-9]+)(?P<attrs>(?:[^<>]|\n)*?)>)"
            r"(?P<ws1>\s*)" + re.escape(en) + r"(?P<ws2>\s*)"
            r"(</(?P=tag)>)")
        m = pat.search(region)
        if not m:
            # Text may be split over lines in the source; normalise whitespace.
            flat = re.sub(r"\s+", r"\\s+", re.escape(en))
            pat = re.compile(
                r"(<(?P<tag>[a-zA-Z0-9]+)(?P<attrs>(?:[^<>]|\n)*?)>)"
                r"(?P<ws1>\s*)" + flat + r"(?P<ws2>\s*)"
                r"(</(?P=tag)>)")
            m = pat.search(region)
        if not m:
            missing.append((key, en[:60]))
            continue
        if "data-i18n=" in m.group("attrs"):
            continue
        open_tag = m.group(1)
        new_open = open_tag[:-1] + f' data-i18n="{key}">'
        region = region[:m.start(1)] + new_open + region[m.end(1):]
        stamped += 1

    open(DASH, "w", encoding="utf-8").write(head + region + tail)
    print(f"stamped {stamped} elements")
    if missing:
        print(f"\nNOT FOUND in markup ({len(missing)}) — english text drifted "
              f"or the string is JS-built:")
        for key, en in missing:
            print(f"  {key:24s} {en!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
