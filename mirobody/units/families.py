"""Canonical UCUM unit → LOINC PROPERTY family.

LOINC's PROPERTY axis is the dimensional family of a measurement
(``MCnc`` = mass concentration, ``SCnc`` = substance/molar concentration,
``NCnc`` = number concentration, ...). For a given analyte every
PROPERTY family selects a fixed set of units — glucose in plasma is
``MCnc`` ⇒ mg/dL and ``SCnc`` ⇒ mmol/L, never the other way around. So
once we know a measurement's unit, we know its PROPERTY family, and we
can use that to filter / boost LOINC candidates whose PROPERTY column
matches.

Two tables:

* :data:`UCUM_FAMILY` — unique unit → primary PROPERTY family. For
  units that pin down a single family (mg/dL → MCnc, mmol/L → SCnc),
  this lookup is exact.

* :data:`AMBIGUOUS_UNITS` — units that legitimately span multiple
  PROPERTYs (the worst offender is ``%``, which appears under
  MFr/NFr/AFr/VFr/SFr/CFr/LenFr/RelACnc/RelRto — basically every
  fraction-like PROPERTY in LOINC). The entry value is the full
  ``frozenset`` of possible families. :func:`unit_family` returns the
  primary (most-common) one from :data:`UCUM_FAMILY` for backward
  compatibility; callers that want all candidates use
  :func:`unit_families`.

LOINC has 231 distinct PROPERTY values; only ~30-40 carry real
(non-annotation) units. Coverage stats from
``benchmarks/audit_ucum_family.py`` against LOINC 2.82: ~95% of
unit-bearing code rows have their primary unit→family entry here. The
long tail (CCnt with ``nmol/h/mg{protein}``, ArVRat with
``mL/min/{1.73_m2}``, etc.) needs the LOINC source for exact lookup
and is intentionally not enumerated.

UCUM keys are case-sensitive. Bracketed units (``[IU]``, ``[U]``,
``[diop]``, ``[pH]``, ``[degF]``) use the formal UCUM syntax — see
ucum.org for the full spec.
"""

from __future__ import annotations


UCUM_FAMILY: dict[str, str] = {
    # ── Mass concentration (MCnc) ─────────────────────────────────────
    "g/L":      "MCnc",
    "g/dL":     "MCnc",
    "g/mL":     "MCnc",
    "mg/L":     "MCnc",
    "mg/dL":    "MCnc",
    "mg/mL":    "MCnc",
    "ug/L":     "MCnc",
    "ug/dL":    "MCnc",
    "ug/mL":    "MCnc",
    "ug/uL":    "MCnc",
    "ng/L":     "MCnc",
    "ng/mL":    "MCnc",
    "ng/dL":    "MCnc",
    "pg/mL":    "MCnc",
    "pg/L":     "MCnc",
    "fg/mL":    "MCnc",

    # ── Substance/molar concentration (SCnc) ──────────────────────────
    "mol/L":    "SCnc",
    "mol/dL":   "SCnc",
    "mmol/L":   "SCnc",
    "umol/L":   "SCnc",
    "nmol/L":   "SCnc",
    "pmol/L":   "SCnc",
    "fmol/L":   "SCnc",
    "meq/L":    "SCnc",   # milliequivalents — treated as SCnc family
    "mmol/dL":  "SCnc",
    "umol/dL":  "SCnc",
    "nmol/mL":  "SCnc",
    "umol/mL":  "SCnc",
    "pmol/mL":  "SCnc",

    # ── Substance ratio (SRto) ────────────────────────────────────────
    "mmol/mol": "SRto",   # HbA1c IFCC unit

    # ── Substance rate (SRat) — 24h excretion etc. ────────────────────
    "mmol/d":         "SRat",
    "umol/d":         "SRat",
    "mmol/(24.h)":    "SRat",
    "umol/(24.h)":    "SRat",
    "nmol/(24.h)":    "SRat",
    "pmol/(24.h)":    "SRat",
    "mmol/(12.h)":    "SRat",
    "mmol/(8.h)":     "SRat",
    "mmol/(6.h)":     "SRat",
    "umol/(12.h)":    "SRat",
    "umol/(8.h)":     "SRat",

    # ── Mass rate (MRat) — 24h urinary excretion, drug dosing ─────────
    "mg/d":         "MRat",
    "ug/d":         "MRat",
    "g/d":          "MRat",
    "mg/(24.h)":    "MRat",
    "ug/(24.h)":    "MRat",
    "g/(24.h)":     "MRat",
    "ng/(24.h)":    "MRat",
    "g/(6.h)":      "MRat",         # 6/8/10/12-hour urine collection windows
    "g/(8.h)":      "MRat",
    "g/(10.h)":     "MRat",
    "g/(12.h)":     "MRat",
    "mg/(6.h)":     "MRat",
    "mg/(8.h)":     "MRat",
    "mg/(12.h)":    "MRat",
    "mg/(18.h)":    "MRat",
    "g/h":          "MRat",
    "mg/h":         "MRat",
    "mg/min":       "MRat",
    "ug/min":       "MRat",
    "mg/kg/d":      "MRat",
    "mmol/(5.h)":   "SRat",
    "g/(5.h)":      "MRat",
    "umol/min/g":   "CCnt",

    # ── Mass ratio (MRto) — albumin/creatinine etc. ───────────────────
    # ``mg/g`` / ``ng/mg`` / ``ug/mg`` etc. are also legitimate Mass
    # content (MCnt) when bare (no creatinine annotation). The flip
    # depends on the analyte; see AMBIGUOUS_UNITS.
    "mg/g":         "MRto",
    "ug/g":         "MRto",
    "ng/mg":        "MRto",
    "ug/mg":        "MRto",
    "g/g":          "MRto",
    "nmol/mg":      "MRto",
    "pmol/mg":      "MRto",
    "mg/mmol":      "MRto",
    "ug/mmol":      "MRto",
    "umol/mmol":    "SRto",
    "umol/mol":     "SRto",
    "nmol/mol":     "SRto",
    "nmol/mmol":    "SRto",
    "pmol/mmol":    "SRto",
    "umol/umol":    "SRto",
    "mmol/mmol":    "SRto",
    "d/(7.d)":      "NRat",         # days per 7-day week
    "d/(30.d)":     "NRat",         # days per 30-day month

    # ── Catalytic content (CCnt) — enzyme activity per mass ───────────
    "nmol/h/mg":    "CCnt",
    "nmol/min/mg":  "CCnt",
    "umol/h/mg":    "CCnt",
    "umol/min/mg":  "CCnt",
    "nmol/h/mL":    "CCnc",         # catalytic concentration (not per mass)
    "U/g":          "CCnt",
    "umol/10*6":    "EntSub",       # per million cells (RBCs etc.)

    # ── Length ratio (LenRto) — vision acuity etc. ────────────────────
    "[ft_us]/[ft_us]": "LenRto",

    # ── Sound intensity ───────────────────────────────────────────────
    "dB":           "RelSoundInt",

    # ── Frequency (Freq) — auditory thresholds, EEG ───────────────────
    "Hz":           "Freq",
    "kHz":          "Freq",
    "MHz":          "Freq",

    # ── Electric resistance (Resis) — skin / bioimpedance ─────────────
    "Ohm":          "Resis",
    "kOhm":         "Resis",

    # ── Radioactivity (Acty) — nuclear medicine ───────────────────────
    "mCi":          "Acty",
    "uCi":          "Acty",
    "Bq":           "Acty",
    "kBq":          "Acty",
    "MBq":          "Acty",
    "GBq":          "Acty",

    # ── Energy (Engy) — caloric intake, exercise expenditure ──────────
    "kcal":         "Engy",
    "cal":          "Engy",     # nutritional context — same as kcal usually
    "J":            "Engy",
    "kJ":           "Engy",
    "MJ":           "Engy",

    # ── Energy difference (EngDiff) — biochemistry ────────────────────
    "kJ/mol":       "EngDiff",
    "J/mol":        "EngDiff",
    "kcal/mol":     "EngDiff",

    # ── Energy rate (EngRat) — caloric intake / expenditure ───────────
    "kcal/h":       "EngRat",
    "kcal/d":       "EngRat",
    "kcal/(24.h)":  "EngRat",
    "kcal/min":     "EngRat",
    "kJ/d":         "EngRat",
    "kJ/(24.h)":    "EngRat",
    "kJ/min":       "EngRat",
    "kcal/kg/d":    "EngRat",

    # ── Power (Pwr) — exercise power output, treadmill watts ──────────
    "W":            "Pwr",
    "mW":           "Pwr",
    "kW":           "Pwr",
    "W/kg":         "Pwr",      # relative power (cycling FTP)

    # ── Viscosity ─────────────────────────────────────────────────────
    "cP":           "Visc",         # centipoise
    "mPa.s":        "Visc",

    # ── Bethesda units (coagulation inhibitor titers) ─────────────────
    "[beth'U]":     "Arb",
    "[beth'U]/mL":  "ACnc",

    # ── Catalytic concentration rate (CCncRat) ────────────────────────
    "umol/L/h":     "CCncRat",
    "nmol/L/h":     "CCncRat",

    # ── MET — metabolic equivalents (exercise dose) ───────────────────
    "[MET]":        "ARat",

    # ── Catalytic concentration variants ──────────────────────────────
    "umol/h/L":     "CCnc",
    "umol/min/L":   "CCnc",

    # ── Mass content (MCnt) — per tissue / dry weight ─────────────────
    "mg/kg":        "MCnt",
    "ug/kg":        "MCnt",
    "ng/kg":        "MCnt",
    "g/kg":         "MCnt",
    "pg/mg":        "MCnt",
    "ng/g":         "MCnt",

    # ── Substance content (SCnt) ──────────────────────────────────────
    "mmol/kg":      "SCnt",
    "umol/kg":      "SCnt",
    "nmol/g":       "SCnt",
    "umol/g":       "SCnt",
    "mmol/g":       "SCnt",

    # ── Arbitrary concentration (ACnc) — IU = "international units" ───
    "[IU]/L":     "ACnc",
    "[IU]/mL":    "ACnc",
    "[IU]/dL":    "ACnc",
    "m[IU]/L":    "ACnc",
    "m[IU]/mL":   "ACnc",
    "k[IU]/L":    "ACnc",
    "k[IU]/mL":   "ACnc",
    "u[IU]/mL":   "ACnc",   # micro-IU per mL — uncommon but in LOINC
    "u[IU]/L":    "ACnc",
    "[arb'U]/mL":  "ACnc",  # arbitrary units per mL — allergen IgE assays
    "[arb'U]/L":   "ACnc",
    "k[arb'U]/L":  "ACnc",
    "k[arb'U]/mL": "ACnc",
    "[arb'U]":     "Arb",   # bare arbitrary unit

    # ── Catalytic concentration (CCnc) — enzyme activity ──────────────
    "U/L":      "CCnc",
    "U/mL":     "CCnc",
    "mU/L":     "CCnc",
    "mU/mL":    "CCnc",
    "kU/L":     "CCnc",
    "kat/L":    "CCnc",   # katal per litre (SI enzyme unit)
    "ukat/L":   "CCnc",
    "nkat/L":   "CCnc",

    # ── Number concentration (NCnc) ───────────────────────────────────
    "10*3/uL":  "NCnc",
    "10*6/uL":  "NCnc",
    "10*6/mL":  "NCnc",
    "10*6/L":   "NCnc",
    "10*9/L":   "NCnc",
    "10*12/L":  "NCnc",
    "/uL":      "NCnc",
    "/mL":      "NCnc",
    "/L":       "NCnc",
    "/dL":      "NCnc",
    "/g":       "NCnt",         # count per gram — bacterial counts
    "/kg":      "NCnt",

    # ── Bare counts (Num) ─────────────────────────────────────────────
    "10*6":     "Num",
    "10*9":     "Num",
    "10*12":    "Num",
    "{#}":      "Num",      # generic count
    "{steps}":  "Num",      # pedometer step count
    "{floors}": "Num",      # floors climbed
    "{breaths}": "Num",     # respiratory cycles (when context is total, not rate)
    "{beats}":  "Num",      # heartbeats (total)

    # ── Score (Score) — sleep / recovery / risk scores ────────────────
    "{score}":  "Score",

    # ── Number areic (Naric) — per visual field, microscopy ───────────
    "/HPF":     "Naric",
    "/LPF":     "Naric",
    "/[HPF]":   "Naric",   # strict UCUM bracketed form
    "/[LPF]":   "Naric",

    # ── Fractions ─────────────────────────────────────────────────────
    # ``%`` is ambiguous (MFr/NFr/AFr/VFr/SFr/CFr) — primary is MFr
    # (most common in LOINC); see AMBIGUOUS_UNITS for the full set.
    "%":        "MFr",
    "[ppm]":    "VFr",
    "[ppb]":    "VFr",
    "[ppth]":   "VFr",
    "mL/dL":    "VFr",
    "mL/L":     "VFr",

    # ── Ratios / dimensionless ────────────────────────────────────────
    "1":        "Ratio",   # INR, indices

    # ── Volume (Vol) — sample volume, urine output ────────────────────
    "L":        "Vol",
    "dL":       "Vol",
    "cL":       "Vol",
    "mL":       "Vol",
    "uL":       "Vol",
    "nL":       "Vol",
    "cm3":      "Vol",
    "mm3":      "Vol",
    "[foz_us]":   "Vol",    # fluid ounce
    "[cup_us]":   "Vol",    # cup
    "[pt_us]":    "Vol",    # pint
    "[qt_us]":    "Vol",    # quart
    "[gal_us]":   "Vol",    # gallon
    "[tbs_us]":   "Vol",    # tablespoon
    "[tsp_us]":   "Vol",    # teaspoon

    # ── Volume rate (VRat) — urine output, flow ───────────────────────
    "L/min":         "VRat",
    "mL/min":        "VRat",
    "mL/h":          "VRat",
    "L/h":           "VRat",
    "mL/(24.h)":     "VRat",
    "L/(24.h)":      "VRat",
    "mL/min/{1.73_m2}": "ArVRat",   # eGFR — areic volume rate
    "L/min/m2":      "ArVRat",      # cardiac index — areic flow
    "mL/min/m2":     "ArVRat",
    "mL/(8.h)":      "VRat",        # urinary output windows
    "mL/(10.h)":     "VRat",
    "mL/(12.h)":     "VRat",
    "mL/(6.h)":      "VRat",
    "mL/h/kg":       "ArVRat",
    "L/s":           "VRat",        # spirometry peak flow
    "mL/s":          "VRat",
    "ng/mL/h":       "CCnc",         # plasma renin activity
    "nmol/mL/h":     "CCnc",
    "mL/m2":         "ArVol",        # stroke volume index
    "mL/min/kg":     "ArVRat",       # VO2 max — per body weight
                                     # paren form mL/(min.kg) is in tokens.py aliases

    # ── Time durations ────────────────────────────────────────────────
    "s":        "Time",
    "ms":       "Time",
    "min":      "Time",
    "h":        "Time",
    "d":        "Time",
    "wk":       "Time",
    "mo":       "Time",
    "a":        "Time",   # annum (year)

    # ── Number rate (NRat) — heart rate, respiratory rate ─────────────
    "/min":     "NRat",
    "/h":       "NRat",
    "/s":       "NRat",
    "/d":       "NRat",
    "/wk":      "NRat",
    "/mo":      "NRat",
    "/a":       "NRat",   # per annum
    "min/d":    "NRat",   # minutes per day — sleep, exercise duration
    "h/d":      "NRat",
    "d/wk":     "NRat",
    "h/wk":     "NRat",
    "min/wk":   "NRat",
    "[MET].min/wk": "ARat",     # metabolic equivalents — exercise dose
    "[MET].h/wk":   "ARat",

    # ── Anthropometric / vital signs ──────────────────────────────────
    "kg":       "Mass",
    "g":        "Mass",
    "mg":       "Mass",
    "ug":       "Mass",
    "ng":       "Mass",
    "fg":       "Mass",     # ``pg`` is in the Hematology section below as EntMass
    "[lb_av]":  "Mass",     # pound (avoirdupois)
    "[oz_av]":  "Mass",     # ounce (avoirdupois)
    "cm":       "Len",
    "m":        "Len",
    "mm":       "Len",
    "um":       "Len",
    "km":       "Len",
    "[in_us]":  "Len",      # inch (US survey — what the aliases resolve to)
    "[ft_us]":  "Len",      # foot (US survey)
    "[in_i]":   "Len",      # inch (international, exactly 0.0254 m) — the
    "[ft_i]":   "Len",      # spelling convert._BASE can actually convert;
                            # [in_us]/[ft_us] have no _BASE atom and stay atomic
    "[mi_us]":  "Len",      # mile
    "[yd_us]":  "Len",      # yard

    # ── Compound / derived ────────────────────────────────────────────
    "kg/m2":    "MCnc",   # BMI — treated as concentration family
    "g/m2":     "MCnc",

    # ── Pressure (Pres) ───────────────────────────────────────────────
    # ``mm[Hg]`` is also used for partial pressure (PPres) in blood
    # gases (pO2, pCO2) — see AMBIGUOUS_UNITS.
    "mm[Hg]":   "Pres",
    "cm[H2O]":  "Pres",
    "kPa":      "Pres",
    "Pa":       "Pres",
    "mbar":     "Pres",
    "bar":      "Pres",
    "[psi]":    "Pres",

    # ── Electric potential (Elpot) — EKG, EEG, EMG ────────────────────
    "mV":       "Elpot",
    "uV":       "Elpot",
    "V":        "Elpot",
    "mV/s":     "ElpotRat",     # rate of potential change (EEG)
    "uV.ms":    "TmElpot",      # time-integrated potential
    "uV.s":     "TmElpot",

    # ── Velocity (Vel) — echo Doppler, conduction velocity ────────────
    "cm/s":     "Vel",
    "m/s":      "Vel",
    "mm/s":     "Vel",
    "km/h":     "Vel",

    # ── Area ──────────────────────────────────────────────────────────
    "cm2":      "Area",
    "mm2":      "Area",
    "m2":       "Area",

    # ── Areic mass (ArMass) — bone densitometry, body composition ─────
    "g/cm2":    "ArMass",
    "mg/cm2":   "ArMass",
    "kg/cm2":   "ArMass",

    # ── Areic length (ArLen) — body morphometrics ─────────────────────
    "cm/m2":    "ArLen",
    "mm/m2":    "ArLen",

    # ── Temperature (Temp) ────────────────────────────────────────────
    "Cel":      "Temp",
    "[degF]":   "Temp",
    "K":        "Temp",

    # ── Angle ─────────────────────────────────────────────────────────
    "deg":      "Angle",
    "rad":      "Angle",

    # ── Inverse length (InvLen) — refractive optics ───────────────────
    "[diop]":   "InvLen",
    "[p'diop]": "InvLen",

    # ── pH ────────────────────────────────────────────────────────────
    "[pH]":     "LsCnc",

    # ── Hematology indices ────────────────────────────────────────────
    "fL":       "EntVol",
    "pg":       "EntMass",

    # ── Osmolality (Osmol) ────────────────────────────────────────────
    "mosm/kg":  "Osmol",
    "mosm/L":   "Osmolarity",

    # ── Bare atomic units (substance amount / arbitrary / catalytic) ──
    # Standalone clinical use is rare — these are here so the morpheme
    # tokenizer can recognize them mid-string. Without ``mmol`` in this
    # table the scanner falls back to ``mm`` (length) when it sees
    # ``mmol/Tag``.
    "mmol":     "Sub",
    "umol":     "Sub",
    "nmol":     "Sub",
    "pmol":     "Sub",
    "fmol":     "Sub",
    "mol":      "Sub",
    "meq":      "Sub",
    "mosm":     "Sub",
    "osmol":    "Sub",
    "[IU]":     "Arb",
    "m[IU]":    "Arb",
    "k[IU]":    "Arb",
    "U":        "CCnt",
    "mU":       "CCnt",
    "kU":       "CCnt",
}


# Units that map to multiple PROPERTYs depending on context. The primary
# entry in :data:`UCUM_FAMILY` is the most common one; this table
# enumerates every family a unit can legitimately belong to. Callers
# that want to soft-rank candidates from any of the listed families use
# :func:`unit_families`.
AMBIGUOUS_UNITS: dict[str, frozenset[str]] = {
    "%": frozenset({
        "MFr", "NFr", "AFr", "VFr", "SFr", "CFr",
        "LenFr", "RelACnc", "RelRto",
    }),
    "mm[Hg]": frozenset({"Pres", "PPres"}),    # BP vs blood-gas pO2/pCO2
    "cm[H2O]": frozenset({"Pres", "PPres"}),
    "[ppm]":  frozenset({"VFr", "MFr", "SFr"}),
    "deg":    frozenset({"Angle", "Temp"}),    # Temp uses [degF]/Cel canonically
    "1":      frozenset({"Ratio", "NRto", "SRto", "MRto", "VRto", "CRto", "ARto"}),
    # Mass-ratio / mass-content collisions: bare form is dimensionally
    # ambiguous between "analyte/creatinine ratio" (MRto) and "analyte
    # per tissue mass" (MCnt). LOINC uses ``{creat}`` annotation to pin
    # MRto; we strip the annotation in _clean so the bare form is what
    # the resolver sees.
    "mg/g":   frozenset({"MRto", "MCnt"}),
    "ug/g":   frozenset({"MRto", "MCnt"}),
    "ng/g":   frozenset({"MCnt", "MRto"}),
    "ng/mg":  frozenset({"MRto", "MCnt"}),
    "ug/mg":  frozenset({"MRto", "MCnt"}),
}


def unit_family(ucum: str | None) -> str | None:
    """Return the primary LOINC PROPERTY family for a canonical UCUM string.

    For units that appear under multiple PROPERTYs (see
    :data:`AMBIGUOUS_UNITS`), the most common family is returned. Use
    :func:`unit_families` when you need the full candidate set.
    """
    if not ucum:
        return None
    return UCUM_FAMILY.get(ucum)


def unit_families(ucum: str | None) -> frozenset[str]:
    """Return every LOINC PROPERTY family compatible with a UCUM string.

    Returns an empty frozenset for unknown units. Single-family units
    yield a 1-element set; ambiguous units (``%``, ``mm[Hg]``, ...)
    yield their full set from :data:`AMBIGUOUS_UNITS`.
    """
    if not ucum:
        return frozenset()
    if ucum in AMBIGUOUS_UNITS:
        return AMBIGUOUS_UNITS[ucum]
    fam = UCUM_FAMILY.get(ucum)
    return frozenset({fam}) if fam else frozenset()
