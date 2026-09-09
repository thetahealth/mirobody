# Units

UCUM units, unit families and conversions — the half of ② Translate that
answers "is this number comparable to that one". Pure Python, no data bundle,
no network: `pip install mirobody` gets all of it.

    normalize.py   normalize_unit, parse_value_unit, ParsedQuantity
    families.py    UCUM_FAMILY (~310 families), AMBIGUOUS_UNITS, unit_family
    tokens.py      MORPHEMES and ALIASES (~600 multilingual surface tokens)
    convert.py     dimensional analysis and the molar-mass bridge

It used to live at `indicator/fhir/units/`, which put the one module a bare
install is for inside the tree the wheel prunes; it moved to the package root
in 1.3.0. This page is the section that documented it there.

---

## Unit normalization

This package parses free-text "value + unit" strings into structured `ParsedQuantity(comparator, value, canonical_ucum)` and looks up the corresponding LOINC PROPERTY family. Designed for ingesting clinical and wearable data where the same indicator gets written different ways across languages, locales, and devices. Pure local computation — no DB, no embedding API.

```python
from mirobody.units import (
    normalize_unit, parse_value_unit, unit_family, unit_families,
)

normalize_unit("毫摩尔每升")          # → "mmol/L"
normalize_unit("Millimol pro Liter") # → "mmol/L"
normalize_unit("MG/DL")              # → "mg/dL"

q = parse_value_unit("90次每分钟")
# ParsedQuantity(comparator="", value=90.0, unit="/min")

unit_family("mmol/L")    # → "SCnc"   (primary LOINC PROPERTY)
unit_families("%")       # → frozenset({"MFr", "NFr", "AFr", "VFr", ...})  (ambiguous)
```

| Layer | Purpose | Examples |
|---|---|---|
| **morpheme** (`tokens.MORPHEMES`) | atomic tokens the tokenizer concatenates left-to-right | `Millimol` + `pro` + `Liter` → `mmol/L`; adding a new prefix (`Femtomol → fmol`) auto-composes with all stems |
| **alias** (`tokens.ALIASES`) | full-string mappings for irreducible compounds | `mmHg → mm[Hg]`, `毫米汞柱 → mm[Hg]`, `10⁹/L → 10*9/L`, `eGFR → mL/min/{1.73_m2}` |
| **family** (`families.UCUM_FAMILY`) | canonical UCUM → LOINC PROPERTY (`MCnc`, `SCnc`, `NRat`, `Pres`, ...) | covers 98%+ of LOINC `EXAMPLE_UCUM_UNITS` |

**Languages covered**: en, zh-CN, zh-TW, ja, ko, ru, de, fr, es. Adding a new language is a single dict literal under `tokens.py` — the tokenizer is language-agnostic (longest-match-first across a global token table).

**Edge cases handled**:

- Comparators (`<5.6`, `>=180 mmHg`, `≤5.6`, `~5.6`, double-char `<=`/`>=`)
- Unicode normalization (`°C`, `µg/L`, `10⁹/L`, full-width `ｍｇ／ｄＬ`)
- UCUM annotation strip (`ug/g{creat}` → `ug/g`; `{copies}/mL` → `/mL`) while preserving canonical annotation forms (`mL/min/{1.73_m2}` round-trips)
- Value-anywhere parsing (`每分钟90次` Chinese SVO order, `mg/dL 90` unit-before-value)
- European decimal comma (`5,6 mmol/L`)
- Wearable count "units" via UCUM annotation form (`600步` → `(0, 600, {steps})`, family `Num`)
- Imperial units (`ft` / `lb` / `oz` / `gallon` etc., normalized to bracketed UCUM `[ft_us]` / `[lb_av]` / ...)
- Ambiguity API: `unit_family("%")` returns the primary (`MFr`); `unit_families("%")` returns all 9 fraction-type PROPERTYs

**CLI**:

```bash
python scripts/vocabulary_build.py normalize "90次每分钟" "<5.6 mg/dL" "600步"
# {"input": "90次每分钟", "comparator": "", "value": 90.0,  "unit": "/min",    "family": "NRat"}
# {"input": "<5.6 mg/dL", "comparator": "<", "value": 5.6,  "unit": "mg/dL",   "family": "MCnc"}
# {"input": "600步",      "comparator": "", "value": 600.0, "unit": "{steps}", "family": "Num"}
```

