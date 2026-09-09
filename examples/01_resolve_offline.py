"""Resolve indicator names to LOINC — offline, no key, no config, no network.

    pip install mirobody
    python examples/01_resolve_offline.py

This is the whole of ② Translate in library form. Everything below runs against the
data bundles shipped inside the package: no PostgreSQL, no Redis, no server, no
API key, and no outbound connection. Unplug the network and it still works.

Why that matters for health data specifically: standardizing a lab report is
normally the step that forces you to send it somewhere.
"""

import unicodedata

from mirobody.engine import get_resolver, resolve_reading

# One resolver, reused. Construction rebuilds the 921k-entry alias index and
# costs a couple of seconds; lookups afterwards are microseconds. get_resolver()
# caches the instance, so call it freely.
resolver = get_resolver()


# CJK and kana are double-width in a terminal, so `f"{s:<13}"` — which counts
# CHARACTERS — misaligns every column of a table that mixes scripts.
def pad(text: str, width: int) -> str:
    shown = sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in text)
    return text + " " * max(1, width - shown)


# ── 1. one test, four languages, one code ────────────────────────────────────
# Note the third row. 血紅素 is not 血红蛋白 in different glyphs — Taiwan and the
# mainland use DIFFERENT WORDS for haemoglobin, and a character conversion of
# one gives you 血红素, which the raw index answers with the code for HbA1c: a
# different test entirely. Script folding alone gets this wrong; the vocabulary
# has to be curated. That is most of what a standardization layer is for.
print("The point of a standardization layer:\n")
for label, term in (
    ("English",   "hemoglobin"),
    ("简体中文",   "血红蛋白"),
    ("繁體中文",   "血紅素"),
    ("日本語",     "ヘモグロビン"),
):
    r = resolver.resolve(term)
    print(f"  {pad(label, 11)}{pad(term, 15)}-> LOINC {r.loinc or '(none)':<10} {r.canonical}")


# ── 2. the same panel, as four countries print it ────────────────────────────
# Every row is one analyte in four scripts. They must land on ONE code each, or
# a family with reports from two places gets two series for one measurement.
panel = [
    ("total cholesterol", "总胆固醇",   "總膽固醇",   "総コレステロール"),
    ("triglycerides",     "甘油三酯",   "三酸甘油酯", "中性脂肪"),
    ("HbA1c",             "糖化血红蛋白", "糖化血色素", "ヘモグロビンA1c"),
    ("creatinine",        "肌酐",       "肌酸酐",     "クレアチニン"),
    ("uric acid",         "尿酸",       "尿酸",       "尿酸値"),
    ("white blood cells", "白细胞",     "白血球",     "白血球数"),
    ("ALT",               "谷丙转氨酶",  "穀丙轉氨酶", "ALT"),
    ("TSH",               "促甲状腺激素", "甲狀腺刺激素", "甲状腺刺激ホルモン"),
]
print("\nOne panel, four scripts, one code per row:\n")
print(f"  {pad('English', 20)}{pad('简体', 15)}{pad('繁體', 15)}{pad('日本語', 21)}LOINC")
for row in panel:
    codes = {resolver.resolve(t).loinc for t in row}
    agree = len(codes) == 1 and "" not in codes
    mark = "" if agree else "   <- the four do not agree"
    print(f"  {pad(row[0], 20)}{pad(row[1], 15)}{pad(row[2], 15)}{pad(row[3], 21)}"
          f"{'/'.join(sorted(c or '-' for c in codes))}{mark}")


# ── 3. the reading decides the code, not just the name ───────────────────────
# LOINC puts the unit AND the result type into the identity, so one measurement
# has several codes and the reading itself says which. If you have the value and
# the unit, pass them: filing a mmol/L result under the mg/dL code is how a
# series ends up with two units in it and nobody notices.
print("\nSame indicator, different readings, different codes:\n")
for name, value, unit in [
    ("total cholesterol", "5.0", "mmol/L"),
    ("total cholesterol", "193", "mg/dL"),
    ("尿糖", "阴性", None),            # a dipstick result is not a number
    ("尿糖", "5.6", "mmol/L"),
]:
    r = resolve_reading(name, value, unit)
    shown = f"{value} {unit}" if unit else value
    print(f"  {name:<20} {shown:<10} -> {r.loinc:<10} {r.canonical[:44]}")


# ── 4. what a miss looks like, and what a refusal looks like ─────────────────
# The resolver never guesses, and it distinguishes two kinds of non-answer.
# `method == ""` is a gap: never seen this term. `method == "refused"` is a
# decision: `lipid panel` is four analytes, and `血糖(HbA1c)` names two
# different tests in one string — no single code can be right, and the
# alternative is confidently picking one.
print("\nA gap and a refusal are different things:\n")
for term in ("lipid panel", "血糖(HbA1c)", "some indicator that does not exist"):
    r = resolver.resolve(term)
    kind = {"refused": "refused — no single code can be right",
            "": "not found"}[r.method]
    print(f"  {term:<36} {kind}")
print("\nOnly method == 'lexical' should be used as an identity.")

# A panel term with a real panel code is the opposite case, and it looks alike:
# `blood pressure` resolves — to the panel, not to one of its two numbers.
bp = resolver.resolve("blood pressure")
print(f"\n  blood pressure -> {bp.loinc} ({bp.canonical}),")
print("  a panel code: it says 'expect components', which is what a refusal")
print("  would only have been trying to tell you.")


# ── 5. ambiguity is reported, not hidden ─────────────────────────────────────
# `candidates` is how many corpus rows matched. A high count means the term is
# genuinely ambiguous (specimen, method, timing) and one default was chosen;
# check it when you need to be certain rather than merely correct-looking.
r = resolver.resolve("glucose")
print(f"\n'glucose' matched {r.candidates} LOINC rows; the default is "
      f"{r.loinc} ({r.canonical}).")
print("Use .candidates to decide when a human should confirm the choice.")
