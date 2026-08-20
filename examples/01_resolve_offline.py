"""Resolve indicator names to LOINC — offline, no key, no config, no network.

    pip install mirobody
    python examples/01_resolve_offline.py

This is the whole of ② Standardize in library form. Everything below runs against the
data bundles shipped inside the package: no PostgreSQL, no Redis, no server, no
API key, and no outbound connection. Unplug the network and it still works.

Why that matters for health data specifically: standardizing a lab report is
normally the step that forces you to send it somewhere.
"""

from mirobody.engine import get_resolver, resolve_reading

# One resolver, reused. Construction rebuilds the 921k-entry alias index and
# costs a couple of seconds; lookups afterwards are microseconds. get_resolver()
# caches the instance, so call it freely.
resolver = get_resolver()


# ── 1. the same test, written three ways, in three languages ─────────────────
print("The point of a standardization layer:\n")
for term in ("hemoglobin", "血红蛋白", "ヘモグロビン", "Hämoglobin"):
    r = resolver.resolve(term)
    print(f"  {term:<14} -> LOINC {r.loinc or '(none)':<10} {r.canonical}")


# ── 2. a whole lab panel ─────────────────────────────────────────────────────
panel = [
    "total cholesterol", "LDL cholesterol", "HDL cholesterol", "triglycerides",
    "fasting glucose", "HbA1c", "creatinine", "urea nitrogen", "ALT", "AST", "TSH",
]
print("\nA lipid + metabolic panel:\n")
for term in panel:
    r = resolver.resolve(term)
    print(f"  {term:<20} {r.loinc:<10} {r.canonical[:52]}")


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
