"""Resolve indicator names to LOINC — offline, no key, no config, no network.

    pip install mirobody
    python examples/01_resolve_offline.py

This is the whole of ② Sort in library form. Everything below runs against the
data bundles shipped inside the package: no PostgreSQL, no Redis, no server, no
API key, and no outbound connection. Unplug the network and it still works.

Why that matters for health data specifically: standardizing a lab report is
normally the step that forces you to send it somewhere.
"""

from mirobody.engine import get_resolver

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
    "fasting glucose", "HbA1c", "creatinine", "eGFR", "ALT", "AST", "TSH",
]
print("\nA lipid + metabolic panel:\n")
for term in panel:
    r = resolver.resolve(term)
    print(f"  {term:<20} {r.loinc:<10} {r.canonical[:52]}")


# ── 3. what a miss looks like ────────────────────────────────────────────────
# The resolver never guesses. `血圧` (blood pressure) names a panel rather than
# a single observation, so the honest answer is nothing at all — the alternative
# would be confidently returning the diastolic code.
print("\nMisses are honest, never guesses:\n")
for term in ("血圧", "some indicator that does not exist"):
    r = resolver.resolve(term)
    print(f"  {term:<36} resolved={r.resolved}")


# ── 4. ambiguity is reported, not hidden ─────────────────────────────────────
# `candidates` is how many corpus rows matched. A high count means the term is
# genuinely ambiguous (specimen, method, timing) and one default was chosen;
# check it when you need to be certain rather than merely correct-looking.
r = resolver.resolve("glucose")
print(f"\n'glucose' matched {r.candidates} LOINC rows; the default is "
      f"{r.loinc} ({r.canonical}).")
print("Use .candidates to decide when a human should confirm the choice.")
