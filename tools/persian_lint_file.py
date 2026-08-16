"""Persian lint for a standalone markdown/text file (changelog, README.fa.md).

Reuses the exact same rule set as tools/persian_lint_dashboard.py — importing the
checker instead of reimplementing it, so the changelog cannot be held to a
different standard than the UI strings.

Skips fenced code blocks and inline code: `IP_RELAY_NO_BACKGROUND=1` and shell
snippets are Latin by definition and would trip the punctuation rules.

Usage: python3 tools/persian_lint_file.py docs/CHANGELOG-v1.1.fa.md
"""
from __future__ import annotations

import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))
import persian_lint_dashboard as pl  # noqa: E402


def strip_code(text: str) -> list[tuple[int, str]]:
    """Return (line_no, text) for lines outside code blocks, inline code removed."""
    out: list[tuple[int, str]] = []
    in_fence = False
    for i, line in enumerate(text.splitlines(), 1):
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        # drop inline code and markdown link targets
        line = re.sub(r"`[^`]*`", " ", line)
        line = re.sub(r"\]\([^)]*\)", "] ", line)
        out.append((i, line))
    return out


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    path = sys.argv[1]
    text = open(path, encoding="utf-8").read()

    pl.errors.clear()
    pl.warnings.clear()
    # Prose, not UI chrome: Persian digits are correct here.
    pl.ALLOW_PERSIAN_DIGITS = True
    checked = 0
    for lineno, line in strip_code(text):
        if not re.search(r"[\u0600-\u06FF]", line):
            continue          # no Persian on this line, nothing to check
        checked += 1
        # The dashboard checker compares fa against en to detect untranslated
        # strings; there is no en counterpart here, so pass the line as both.
        pl.check(f"{os.path.basename(path)}:{lineno}", "", line)

    for w in pl.warnings:
        print(f"WARN  {w}")
    for e in pl.errors:
        print(f"ERROR {e}")
    print()
    print(f"checked {checked} Persian lines in {path} — "
          f"{len(pl.errors)} errors, {len(pl.warnings)} warnings")
    return 1 if pl.errors else 0


if __name__ == "__main__":
    sys.exit(main())
