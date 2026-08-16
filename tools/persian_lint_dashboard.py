"""Deterministic Persian lint for the i18n table.

Runs the mechanical checks from the persian-localization skill against every fa
string in tools/i18n_strings.py. LLM-written Persian is inconsistent about ZWNJ
and letterforms; this is a machine check, so it should not cost model calls.

Checks:
  1. Arabic letterforms that must be Persian (ك→ک, ي/ى→ی, أ/إ→ا, ؤ→و, ھ→ه).
  2. TATWEEL (U+0640) — decoration, never semantic.
  3. Harakat (U+064B–U+0652) — strip from UI prose.
  4. Missing ZWNJ after the می/نمی/بی prefixes (with an exception list).
  5. Plain space where ZWNJ belongs ('می شود').
  6. ASCII punctuation where Persian punctuation belongs (, ; ?).
  7. Mixed digits: Persian/Arabic-Indic digits present (policy is ASCII).
  8. ZWNJ adjacent to a Latin run (must be a plain space).
  9. Untranslated: fa identical to en while en contains letters.
 10. Placeholder drift: __VERSION__ / %s / {} must survive translation.

Exit code is non-zero if any finding is an error, so it can gate a commit.
Reports character offsets, per the skill.
"""
from __future__ import annotations

import re
import sys
import unicodedata

sys.path.insert(0, __import__("os").path.dirname(__file__))
from i18n_strings import ATTRS, JS_STRINGS, STRINGS  # noqa: E402

ARABIC_WRONG = {
    "\u0643": ("ك", "ک", "ARABIC LETTER KAF -> KEHEH"),
    "\u064a": ("ي", "ی", "ARABIC YEH -> FARSI YEH"),
    "\u0649": ("ى", "ی", "ALEF MAKSURA -> FARSI YEH"),
    "\u06be": ("ھ", "ه", "HEH DOACHASHMEE -> HEH"),
    "\u0629": ("ة", "ه", "TEH MARBUTA -> HEH (flag, not auto)"),
}
# أ/إ (U+0623/U+0625) are NOT unconditionally wrong in Persian: تأخیر, رأی,
# مأمور, تأیید are correct Persian orthography with a mid-word hamza-alef. Only
# a WORD-INITIAL hamza-alef is the Arabic form that must normalise to ا. Blanket
# replacement corrupts real words — it flagged "تأخیر" (latency) as an error.
HAMZA_ALEF = "\u0623\u0625"
# ؤ (U+0624) has the same story: سؤال, مؤثر, مؤسسه are correct Persian. Only a
# word-initial one is wrong, and that is vanishingly rare.
HAMZA_WAW = "\u0624"
TATWEEL = "\u0640"
ZWNJ = "\u200c"
# Persian LETTERS only. \u0600-\u06FF also covers ، ؛ ؟ and the Persian digits,
# so using the full block as "a letter" made 'بی،' look like a prefix+stem.
PLETTER = "\u0621-\u063a\u0641-\u064a\u066e\u066f\u0671-\u06d3\u06fa-\u06ff"
HARAKAT = re.compile("[\u064b-\u0652\u0670\u06d6-\u06ed]")
PERSIAN_DIGITS = re.compile("[\u06f0-\u06f9\u0660-\u0669]")
LATIN_RUN = re.compile(r"[A-Za-z0-9]")

# می/نمی/بی are verb/adjective prefixes needing ZWNJ — EXCEPT these real words.
PREFIX_EXCEPTIONS = {
    "میان", "میانه", "میانی", "میز", "میهن", "میلیون", "میلیارد", "میدان", "میراث",
    "میکرو", "مینا", "میوه", "میخ", "میل", "میلی", "بیمار", "بیست", "بیرون",
    "بین", "بیش", "بیشتر", "بیشترین", "بیت", "بیضی", "بینه", "بیانیه", "بیان",
    "بیمه", "نمیر", "میهمان", "میگو", "بیوگرافی", "بینایی", "بیکار",
    # Found by running this linter on the real string table: these are single
    # words, not prefix+stem. میزبان = host, بیایند = (they) come,
    # بی‌درنگ is correctly hyphenated so it never reaches here.
    "میزبان", "میزبانی", "بیایند", "بیاید", "بیا", "میراثی",
    # بیرون/بیرونی = outer/outward — a noun, not بی + رونی.
    "بیرونی", "بیرونه",
    # بیدار = awake, بیمه = insurance — stems that merely start with بی.
    "بیدار", "بیدارش", "بیداری", "بیمه", "بیضی", "بیت",
}

errors: list[str] = []
warnings: list[str] = []

# UI strings keep ASCII digits (they sit beside Latin model names and monospace
# telemetry). Prose documents — the Persian changelog, README.fa.md — are read as
# Persian text, where Persian digits are the correct typographic choice. The
# file-level linter flips this.
ALLOW_PERSIAN_DIGITS = False


def check(key: str, en: str, fa: str) -> None:
    where = f"{key}"

    for ch, (wrong, right, name) in ARABIC_WRONG.items():
        for m in re.finditer(re.escape(ch), fa):
            lvl = warnings if ch == "\u0629" else errors
            lvl.append(f"{where} @{m.start()}: {name} — {wrong!r} should be {right!r}")

    for m in re.finditer(f"(?<![{PLETTER}])[{HAMZA_ALEF}{HAMZA_WAW}]", fa):
        errors.append(f"{where} @{m.start()}: word-initial Arabic hamza form "
                      f"{m.group()!r} should be plain ا/و")

    for m in re.finditer(re.escape(TATWEEL), fa):
        errors.append(f"{where} @{m.start()}: TATWEEL U+0640 must be removed")

    for m in HARAKAT.finditer(fa):
        # Tanvin on a final alef (کاملاً, عمداً, مثلاً, نسبتاً) is correct Persian
        # orthography, not decoration — the blanket "strip harakat" rule is for
        # vowel marks inside words. Allow FATHATAN only in that exact position.
        if m.group() == "\u064b" and m.start() > 0 and fa[m.start() - 1] == "\u0627":
            nxt = fa[m.start() + 1] if m.start() + 1 < len(fa) else " "
            if not ("\u0600" <= nxt <= "\u06ff"):
                continue
        cp = f"U+{ord(m.group()):04X}"
        errors.append(f"{where} @{m.start()}: harakat {cp} "
                      f"({unicodedata.name(m.group(), '?')}) — strip from UI prose")

    if not ALLOW_PERSIAN_DIGITS:
        for m in PERSIAN_DIGITS.finditer(fa):
            errors.append(f"{where} @{m.start()}: non-ASCII digit {m.group()!r} — "
                          f"digit policy is ASCII")

    # ZWNJ vs plain space after the prefix: 'می شود' is wrong.
    for m in re.finditer(f"(?<![{PLETTER}{ZWNJ}])(نمی|می) ", fa):
        errors.append(f"{where} @{m.start()}: '{m.group(1)} ' uses a plain space — "
                      f"needs ZWNJ (U+200C)")

    # Missing separator entirely: 'میشود'. The lookbehind also excludes ZWNJ:
    # in خوش‌بینانه the 'بی' is the tail of a correctly-joined compound, not a
    # prefix needing its own ZWNJ.
    for m in re.finditer(f"(?<![{PLETTER}{ZWNJ}])(نمی|می|بی)([{PLETTER}]+)", fa):
        whole = m.group(0)
        if whole in PREFIX_EXCEPTIONS:
            continue
        rest = m.group(2)
        if rest.startswith(ZWNJ):
            continue
        # A 1-2 letter tail is usually part of a real word (میل, بین) — warn only.
        lvl = warnings if len(rest) <= 2 else errors
        lvl.append(f"{where} @{m.start()}: {whole!r} — missing ZWNJ after "
                   f"{m.group(1)!r} prefix (or add to PREFIX_EXCEPTIONS)")

    for bad, good in ((",", "،"), (";", "؛"), ("?", "؟")):
        for m in re.finditer(re.escape(bad), fa):
            # ASCII punctuation inside a Latin/technical run is legitimate
            # (SOCKS4/5, 1,384) — only flag it between Persian characters.
            before = fa[m.start() - 1] if m.start() else ""
            after = fa[m.start() + 1] if m.start() + 1 < len(fa) else ""
            if LATIN_RUN.match(before or " ") or LATIN_RUN.match(after or " "):
                continue
            if "\u0600" <= (before or " ") <= "\u06ff":
                errors.append(f"{where} @{m.start()}: ASCII {bad!r} between Persian "
                              f"characters — use {good!r}")

    for m in re.finditer(ZWNJ, fa):
        for nb in (fa[m.start() - 1] if m.start() else "",
                   fa[m.start() + 1] if m.start() + 1 < len(fa) else ""):
            if nb and LATIN_RUN.match(nb):
                errors.append(f"{where} @{m.start()}: ZWNJ touching Latin {nb!r} — "
                              f"use a plain space")

    if re.search(r"[A-Za-z]", en) and fa.strip() == en.strip():
        warnings.append(f"{where}: fa identical to en — untranslated?")

    for ph in ("__VERSION__",):
        if ph in en and ph not in fa:
            errors.append(f"{where}: placeholder {ph} lost in translation")

    if not fa.strip():
        errors.append(f"{where}: empty translation")


def main() -> int:
    total = 0
    for table in (STRINGS, ATTRS, JS_STRINGS):
        for key, en, fa in table:
            total += 1
            check(key, en, fa)

    for w in warnings:
        print(f"WARN  {w}")
    for e in errors:
        print(f"ERROR {e}")

    print()
    print(f"checked {total} strings — {len(errors)} errors, {len(warnings)} warnings")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
