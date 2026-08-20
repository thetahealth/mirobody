"""Turn a lab report into standardized indicators — one LLM call, no server.

    pip install mirobody
    export OPENAI_API_KEY=...          # or ANTHROPIC_API_KEY / GOOGLE_API_KEY
    python examples/03_parse_a_lab_report.py path/to/report.pdf

This is ① Collect + ② Standardize in a single function. `parse_file()` sends the
document to one model to extract name/value/unit, then resolves every extracted
name **offline** against the shipped bundles. No database, no `mirobody serve`,
no agent framework.

The split matters: the model reads the page, but it does not get to invent the
code. Extraction is the fuzzy step; standardization is deterministic and
auditable, and it is the half you would otherwise have to trust a model with.

Without an argument this runs the resolver half on a synthetic panel, so you can
see the shape of the output without a key or a file.
"""

import os
import sys

from mirobody.engine import parse_file, resolve


def show(readings):
    print(f"\n{len(readings)} readings\n")
    print(f"  {'name':<28} {'value':>10} {'unit':<10} {'LOINC':<10} canonical")
    print(f"  {'-'*28} {'-'*10} {'-'*10} {'-'*10} {'-'*40}")
    for r in readings:
        res = r.resolution
        loinc = (res.loinc if res and res.resolved else "") or "—"
        canon = (res.canonical if res and res.resolved else "(unresolved)")
        print(f"  {r.name[:28]:<28} {str(r.value)[:10]:>10} {(r.unit or '')[:10]:<10} {loinc:<10} {canon[:40]}")

    unresolved = [r for r in readings if not (r.resolution and r.resolution.resolved)]
    if unresolved:
        print(f"\n  {len(unresolved)} did not resolve. That is deliberate — an honest gap")
        print("  beats a confident wrong code. Add a row to res/resolver_overrides.tsv.")


if len(sys.argv) > 1:
    path = sys.argv[1]
    if not any(os.environ.get(k) for k in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GOOGLE_API_KEY")):
        sys.exit("Set OPENAI_API_KEY (or ANTHROPIC_API_KEY / GOOGLE_API_KEY) first — "
                 "extraction is the one step that needs a model.")
    print(f"Parsing {path} …")
    show(parse_file(path))
    raise SystemExit

# ── no file given: demonstrate the deterministic half on its own ─────────────
print(__doc__.strip().splitlines()[0])
print("\nNo file given, so this runs the standardization half only — the part that")
print("needs no key. These are the names as a report would actually print them:\n")

panel = [
    ("Hemoglobin A1c",          5.4,  "%"),
    ("LDL cholesterol",       120.0,  "mg/dL"),
    ("血糖",                     5.5,  "mmol/L"),
    ("γ-GTP",                  28.0,  "U/L"),
    ("Thyroid stimulating hormone", 2.1, "uIU/mL"),
    ("血糖(HbA1c)",             None,  None),      # two different tests in one string
]

print(f"  {'as printed':<30} {'LOINC':<10} canonical")
print(f"  {'-'*30} {'-'*10} {'-'*44}")
for name, _value, _unit in panel:
    r = resolve(name)
    print(f"  {name[:30]:<30} {(r.loinc or '—'):<10} {r.canonical or '(deliberately unresolved)'}")

print("\n`血糖(HbA1c)` resolves to nothing on purpose: the stem says glucose and the")
print("parenthetical says HbA1c, so either answer files the reading into the wrong")
print("series. A panel term with a real panel code is the other case and does")
print("resolve — see 01_resolve_offline.py.")
print("\nGive this script a PDF/JPG/CSV path to run the full extraction.")
