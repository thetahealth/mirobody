# Units

UCUM units, unit families and conversions — the half of ② Translate that
answers "is this number comparable to that one". Pure Python, no data bundle,
no network: `pip install mirobody` gets all of it.

    normalize.py   normalize_unit, parse_value_unit, ParsedQuantity
    families.py    UCUM_FAMILY (328 units over 59 families), AMBIGUOUS_UNITS, unit_family
    tokens.py      MORPHEMES and ALIASES (~600 multilingual surface tokens)
    convert.py     dimensional analysis, the molar-mass bridge, canonical form

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
| **family** (`families.UCUM_FAMILY`) | canonical UCUM → LOINC PROPERTY (`MCnc`, `SCnc`, `NRat`, `Pres`, ...) | 99.0% of the dimensional units in LOINC 2.83's `EXAMPLE_UCUM_UNITS`; bare annotations (`{titer}`) are not units and are out by design |

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


---

## Canonical form — the comparison key

`normalize_unit` answers "how is this unit spelled". `canonicalize` answers the
next question: **is this number comparable to that one**, and it answers it as a
value you can store rather than a conversion you have to run per query.

```python
from mirobody.units import canonicalize, canonical_unit

canonicalize(1000, "mm")                          # (1.0, 'm')
canonicalize(5, "10*9/L")                         # (5000000000.0, '/L')
canonicalize(100, "mg/dL", loinc_code="2345-7")   # ≈ (0.005551, 'mol/L')
canonicalize(5.6, "mmol/L", loinc_code="2345-7")  #   (0.0056,   'mol/L')
canonical_unit("%")                               # None — atomic
```

Write the pair into a second column beside the value **as recorded**, and two
readings are comparable when their canonical pairs agree — a `GROUP BY` instead
of a conversion pass. The original is never rewritten: provenance, the right to
be forgotten and FHIR fidelity all depend on the reading as written down.
`convert_value` remains the way to ask "in THIS unit, what is it".

Three properties worth knowing before you store it:

* **The unit rides along with the number, and both are the key.** The canonical
  basis is not a property of the input alone — without a code `mg/dL` folds to
  `g/L`, with `2345-7` it folds to `mol/L`, where the mmol/L half of the world's
  glucose readings already is. That is the whole point (a Chinese report's
  5.6 mmol/L and a US report's 100 mg/dL are one series only across the molar
  bridge), but it means a bare canonical number is ambiguous and a pair is not.
* **The bridge is keyed by LOINC code, never by name.** Same code in, same basis
  out, every time. A code `MOLAR_MASS` does not carry is not bridged and folds
  to `g/L` — declining, not guessing.
* **An unfoldable unit comes back untouched.** `%`, `mm[Hg]`, `meq/L`, `个/HP`
  are already their own canonical form: equal only to themselves, which is
  exactly what an unchanged pair means to a caller comparing pairs.

The pattern is borrowed from the Android FHIR SDK's `ResourceIndexer`, which
writes two index rows per `Quantity` — the human-readable unit as given, and the
UCUM code folded to base units — then folds the query the same way at search
time, so `1000 mm` matches a search for `1 m` without the stored resource ever
being edited. What it cannot do is the molar bridge: UCUM alone will not cross
`mg/dL` ↔ `mmol/L`, because they are different dimensions. That crossing needs
the analyte, which is why the bridge here is keyed by code.
