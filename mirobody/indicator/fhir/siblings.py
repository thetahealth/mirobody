"""Build LOINC and SNOMED CT sibling groups for concept expansion.

Sibling groups cluster codes that are semantically related along one or more
axes (COMPONENT base, SYSTEM+METHOD, Long Common Name prefix, IS-A parent,
or shared CUI).  The groups are saved as CSV files and can also be returned
as a weighted dict for use in mapping expansion.
"""

import enum
import logging
import os
import re
from argparse import Namespace
from collections import defaultdict

import polars as pl

from .common import (
    LoincAxisData,
    MappingRow,
    TARGET_SYSTEMS,
    code_to_int,
    int_to_code,
    parse_loinc_axes,
    read_rrf,
)

log = logging.getLogger(__name__)


class Rel(enum.IntEnum):
    """Sibling relation types, ordered roughly by reliability."""
    CUI  = 0   # UMLS CUI synonym — curated, most reliable
    ISA  = 1   # SNOMED IS-A shared parent
    COMP = 2   # LOINC COMPONENT base match
    LCN  = 3   # LOINC Long Common Name prefix match
    SM   = 4   # LOINC SYSTEM+METHOD match (same specimen+method, different analyte)
    ATC  = 5   # ATC 5-char (chemical subgroup) — finest level with multi-code groups

    @property
    def label(self) -> str:
        return self.name.lower()


# ─── Abbreviation expansion ─────────────────────────────────────────

# Common medical abbreviations extracted from LOINC LN vs MTH_LN comparison.
# Used to annotate sibling group names for better embedding quality.
ABBREV_EXPAND = {
    "Ab": "Antibody",
    "Ag": "Antigen",
    "AIDS": "Acquired immunodeficiency syndrome",
    "AJCC": "American Joint Committee on Cancer",
    "AP": "Anteroposterior",
    "ART": "Antiretroviral therapy",
    "BP": "Blood pressure",
    "CD": "Cluster of differentiation",
    "CDC": "Centers for Disease Control",
    "CNS": "Central nervous system",
    "CoA": "Coenzyme A",
    "CoV": "Coronavirus",
    "CSF": "Cerebrospinal fluid",
    "CT": "Computed tomography",
    "DNA": "Deoxyribonucleic acid",
    "ECG": "Electrocardiogram",
    "EDTA": "Ethylenediaminetetraacetic acid",
    "ENT": "Ear nose and throat",
    "FC": "Flow cytometry",
    "FH": "Family history",
    "FIGO": "International Federation of Gynecology and Obstetrics",
    "GE": "Gastroesophageal",
    "GP": "General practitioner",
    "Hct": "Hematocrit",
    "Hgb": "Hemoglobin",
    "HIV": "Human immunodeficiency virus",
    "HLA": "Human leukocyte antigen",
    "HTLV": "Human T-lymphotropic virus",
    "IA": "Immunoassay",
    "IgA": "Immunoglobulin A",
    "IgD": "Immunoglobulin D",
    "IgE": "Immunoglobulin E",
    "IgG": "Immunoglobulin G",
    "IgM": "Immunoglobulin M",
    "IV": "Intravenous",
    "MR": "Magnetic resonance",
    "MRA": "Magnetic resonance angiography",
    "MRI": "Magnetic resonance imaging",
    "NAA": "Nucleic acid amplification",
    "NM": "Nuclear medicine",
    "RAST": "Radioallergosorbent test",
    "RBC": "Red blood cells",
    "RF": "Rheumatoid factor",
    "RFA": "Radiofrequency ablation",
    "Rh": "Rhesus",
    "RNA": "Ribonucleic acid",
    "rRNA": "Ribosomal RNA",
    "SARS": "Severe acute respiratory syndrome",
    "TNM": "Tumor Node Metastasis",
    "UICC": "Union for International Cancer Control",
    "US": "Ultrasound",
    "WBC": "White blood cells",
    "WHO": "World Health Organization",
    "XR": "X-ray",
}
_ABBREV_RE = re.compile(
    r'\b(' + '|'.join(re.escape(k) for k in sorted(ABBREV_EXPAND, key=len, reverse=True)) + r')\b'
)


def expand_abbrevs(text: str) -> str:
    """Append full form after known abbreviations: 'IgE Ab' → 'IgE (Immunoglobulin E) Ab (Antibody)'."""
    def _repl(m: re.Match) -> str:
        abbr = m.group(1)
        return f"{abbr} ({ABBREV_EXPAND[abbr]})"
    return _ABBREV_RE.sub(_repl, text)


# ─── LOINC siblings ─────────────────────────────────────────────────

# Generic COMPONENT bases to skip — these create overly large, noisy sibling groups.
COMPONENT_BASE_BLACKLIST = {
    # Imaging / radiology
    "multisection", "views", "view ap", "views ap and lateral",
    "view lateral", "views 2", "views 3", "single view",
    "guidance for percutaneous biopsy", "guidance for percutaneous fine",
    "guidance for percutaneous fluid", "guidance for injection",
    # Document types
    "note", "consultation note", "progress note", "procedure note",
    "plan of care note", "discharge summary note", "history and physical note",
    "initial evaluation note", "admission evaluation note",
    "admission history and physical",
    # Too generic
    "cells", "observation", "study observation", "physical findings",
    "diagnosis", "cause of death", "class", "total score",
    "symptoms and diseases", "mds v3",
}


from .common import load_loinc_skip_codes as _load_loinc_skip_codes


def _base_component(component: str) -> str:
    """Extract base from LOINC COMPONENT, stripping ^-modifiers and ./suffixes."""
    if not component:
        return ""
    component = re.sub(r'\^[^()]*?(?=\))', '', component)
    caret_pos = component.find('^')
    if caret_pos >= 0:
        component = component[:caret_pos]
    return re.split(r'[./]', component)[0].strip().lower()


def _load_part_display(loinc_dir: str, loinc_axes: LoincAxisData) -> dict[str, str]:
    """Load Part.csv display names, mapping axis values to full display names."""
    part_csv = os.path.join(loinc_dir, "AccessoryFiles", "PartFile", "Part.csv")
    partlink_csv = os.path.join(loinc_dir, "AccessoryFiles", "PartFile", "LoincPartLink_Primary.csv")
    if not (os.path.isfile(part_csv) and os.path.isfile(partlink_csv)):
        return {}

    loinc_core_csv = os.path.join(loinc_dir, "LoincTableCore", "LoincTableCore.csv")
    if not os.path.isfile(loinc_core_csv):
        return {}

    # PartNumber → DisplayName via polars
    parts = pl.read_csv(part_csv).select(["PartNumber", "PartDisplayName"]).drop_nulls()
    pn_to_display = dict(zip(
        parts["PartNumber"].to_list(),
        parts["PartDisplayName"].to_list(),
    ))

    # Build display mapping from PartLink
    part_display: dict[str, str] = {}
    # PartLink uses plain csv.reader because column positions matter more than names
    with open(partlink_csv, "r", encoding="utf-8") as f:
        import csv
        for row in csv.reader(f):
            if len(row) < 6:
                continue
            code_str, pn, link_type = row[0], row[2], row[5]
            dn = pn_to_display.get(pn, "")
            if not dn:
                continue
            code_int = code_to_int(code_str, "LOINC")
            if link_type == "SYSTEM":
                val = loinc_axes.code_to_system.get(code_int, "")
            elif link_type == "METHOD":
                val = loinc_axes.code_to_method.get(code_int, "")
            elif link_type == "COMPONENT":
                val = loinc_axes.code_to_component.get(code_int, "")
            else:
                continue
            if val and val not in part_display:
                part_display[val] = dn

    log.info(f"  Part display names: {len(part_display):,}")
    return part_display


def _build_pairwise_siblings(
    labeled_groups: list[tuple[Rel, dict]],
) -> tuple[dict[int, dict[int, set[Rel]]], list[tuple[Rel, str, int]]]:
    """Merge groups into sibling map recording which relations link each pair.

    Returns: (code → {sibling_code: {Rel}}, skipped_groups).
    """
    siblings: dict[int, dict[int, set[Rel]]] = defaultdict(lambda: defaultdict(set))
    skipped: list[tuple[Rel, str, int]] = []

    for rel, groups in labeled_groups:
        for key, group in groups.items():
            if len(group) <= 1:
                continue
            if key in COMPONENT_BASE_BLACKLIST:
                skipped.append((rel, str(key), len(group)))
                continue
            members = list(group)
            for i, a in enumerate(members):
                for b in members[i + 1:]:
                    siblings[a][b].add(rel)
                    siblings[b][a].add(rel)

    return dict(siblings), skipped


def build_siblings_loinc(
    loinc_axes: LoincAxisData,
    out_dir: str,
    loinc_dir: str = "",
    umls_dir: str = "",
) -> dict[int, dict[int, set[Rel]]]:
    """Pre-build LOINC sibling groups by COMPONENT base and SYSTEM+METHOD.

    Returns: loinc_code → {sibling_code: {Rel}}.
    Also saves to _siblings_loinc.csv for caching.
    """
    # Load Part.csv for full display names (optional, for readable CSV keys)
    part_display = _load_part_display(loinc_dir, loinc_axes) if loinc_dir else {}

    # Load LOINC Chinese names from MRCONSO (SAB=LNC-ZH-CN)
    loinc_cn: dict[str, str] = {}  # LOINC code → Chinese name
    if umls_dir:
        mrconso_path = os.path.join(umls_dir, "META", "MRCONSO.RRF")
        if os.path.isfile(mrconso_path):
            cn_df = read_rrf(mrconso_path, ["SAB", "CODE", "STR"])
            cn_df = cn_df.filter(pl.col("SAB") == "LNC-ZH-CN") \
                .unique(subset=["CODE"], keep="first")
            loinc_cn = dict(zip(cn_df["CODE"].to_list(), cn_df["STR"].to_list()))
            log.info(f"  LOINC Chinese names: {len(loinc_cn):,}")

    # Exclude non-lab/non-clinical codes: surveys, attachments, PhenX, docs, admin
    skip_codes: set[int] = set()
    loinc_core_csv = os.path.join(loinc_dir, "LoincTableCore", "LoincTableCore.csv") if loinc_dir else ""
    if loinc_core_csv and os.path.isfile(loinc_core_csv):
        skip_codes = {code_to_int(c, "LOINC") for c in _load_loinc_skip_codes(loinc_core_csv)}
        log.info(f"  Excluding {len(skip_codes):,} non-lab codes")

    # ── 1. COMPONENT base groups ──
    comp_base_groups: dict[str, set[int]] = defaultdict(set)
    for lc_code, component in loinc_axes.code_to_component.items():
        if lc_code in skip_codes:
            continue
        base = _base_component(component)
        if base:
            comp_base_groups[base].add(lc_code)

    # ── 1b. Build English→Chinese maps for SYSTEM and METHOD axes ──
    # Extract from 6-axis Chinese names: Component:Property:Time:System:Scale:Method
    sys_en_to_cn: dict[str, str] = {}
    meth_en_to_cn: dict[str, str] = {}
    for lc_code, cn_name in loinc_cn.items():
        parts = cn_name.split(":")
        if len(parts) < 6:
            continue
        code_int = code_to_int(lc_code, "LOINC")
        en_sys = loinc_axes.code_to_system.get(code_int, "")
        en_meth = loinc_axes.code_to_method.get(code_int, "")
        cn_sys = parts[3].strip()
        cn_meth = parts[5].strip()
        if en_sys and cn_sys and en_sys not in sys_en_to_cn:
            sys_en_to_cn[en_sys] = cn_sys
        if en_meth and cn_meth and en_meth not in meth_en_to_cn:
            meth_en_to_cn[en_meth] = cn_meth
    log.info(f"  System EN→CN: {len(sys_en_to_cn):,}, Method EN→CN: {len(meth_en_to_cn):,}")

    # ── 2. SYSTEM+METHOD groups ──
    # Skip SYSTEM values starting with '^' (e.g. '^Patient') — these are
    # context modifiers, not real specimen types.
    sys_meth_groups: dict[tuple[str, str], set[int]] = defaultdict(set)
    for lc_code, sys_val in loinc_axes.code_to_system.items():
        if lc_code in skip_codes or sys_val.startswith("^"):
            continue
        meth_val = loinc_axes.code_to_method.get(lc_code)
        if meth_val:
            sys_meth_groups[(sys_val, meth_val)].add(lc_code)

    # ── 3. LONG_COMMON_NAME groups (via polars) ──
    lcn_groups: dict[str, set[int]] = defaultdict(set)
    code_lcn_prefix: dict[int, str] = {}
    if loinc_core_csv and os.path.isfile(loinc_core_csv):
        skip_prefixes = TARGET_SYSTEMS["LOINC"]["skip_prefixes"]
        lcn_df = pl.read_csv(loinc_core_csv, columns=["LOINC_NUM", "LONG_COMMON_NAME"])
        lcn_df = lcn_df.drop_nulls().filter(
            ~pl.col("LOINC_NUM").str.starts_with(skip_prefixes[0])
        )
        for prefix in skip_prefixes[1:]:
            lcn_df = lcn_df.filter(~pl.col("LOINC_NUM").str.starts_with(prefix))

        for row in lcn_df.iter_rows(named=True):
            code_str = row["LOINC_NUM"]
            lcn = row["LONG_COMMON_NAME"]
            if not lcn:
                continue
            code_int = code_to_int(code_str, "LOINC")
            if code_int in skip_codes:
                continue
            prefix = lcn.split("[")[0].strip() if "[" in lcn else lcn.strip()
            if prefix:
                code_lcn_prefix[code_int] = prefix
            key_lower = prefix.lower() if prefix and len(prefix) >= 3 else ""
            if key_lower:
                lcn_groups[key_lower].add(code_int)

    # ── 4. Merge into sibling map ──
    siblings, skipped_groups = _build_pairwise_siblings([
        (Rel.COMP, comp_base_groups),
        (Rel.SM, sys_meth_groups),
        (Rel.LCN, lcn_groups),
    ])

    # Save skipped groups
    if skipped_groups:
        skipped_path = os.path.join(out_dir, "_siblings_skipped.csv")
        pl.DataFrame(
            [(r.label, n, s) for r, n, s in skipped_groups],
            schema=["relation", "name", "size"], orient="row",
        ) \
            .sort("size", descending=True) \
            .write_csv(skipped_path, quote_style="always")
        log.info(f"  Skipped {len(skipped_groups)} blacklisted groups → {skipped_path}")

    n_comp = sum(1 for g in comp_base_groups.values() if len(g) > 1)
    n_sm = sum(1 for g in sys_meth_groups.values() if len(g) > 1)
    n_lcn = sum(1 for g in lcn_groups.values() if len(g) > 1)
    log.info(f"  LOINC siblings: {len(siblings):,} codes, "
             f"{n_comp:,} comp groups, {n_sm:,} sm groups, {n_lcn:,} lcn groups")

    # ── 5. Save cache (group-based format with full display names) ──
    def _display(val: str) -> str:
        dn = part_display.get(val, "")
        if dn and dn.lower() != val.lower():
            val_chars = set(re.findall(r'[a-zA-Z0-9]', val.lower()))
            dn_chars = set(re.findall(r'[a-zA-Z0-9]', dn.lower()))
            if dn_chars - val_chars:
                return f"{val} ({dn})"
        return val

    _CN_CONC_RE = re.compile(r'\s+\d[\d.]*\s*\S+$')

    def _with_cn(name: str, group: set[int]) -> str:
        """Append unique English + Chinese component base names for group members.

        Extracts the component (first axis before ':'), then strips:
        - ^-modifiers (e.g. '^在XXX刺激之后1小时')
        - concentration suffixes (e.g. ' 1.0 微克/毫升')
        to keep only the base analyte name for embedding.
        """
        en_bases = set()
        cn_bases = set()
        for c in group:
            code_str = int_to_code(c, "LOINC")
            # English component
            en = loinc_axes.code_to_component.get(c, "")
            if en:
                en_bases.add(_base_component(en))
            # Chinese component
            cn = loinc_cn.get(code_str, "")
            if cn:
                component = cn.split(":")[0]
                base = component.split("^")[0]
                base = _CN_CONC_RE.sub("", base)
                if base:
                    cn_bases.add(base)
        parts = sorted((en_bases | cn_bases) - {""})
        if parts:
            return name + "; " + "; ".join(parts)
        return name

    rows: list[dict] = []
    for base, group in sorted(comp_base_groups.items()):
        if len(group) > 1:
            prefixes = sorted({code_lcn_prefix[c] for c in group if c in code_lcn_prefix})
            name = expand_abbrevs("; ".join(prefixes)) if prefixes else base
            rows.append({
                "relation": "comp", "name": _with_cn(name, group),
                "codes": "|".join(int_to_code(c, "LOINC") for c in sorted(group)),
                "note": "",
            })
    for (sys_val, meth_val), group in sorted(sys_meth_groups.items()):
        if len(group) > 1:
            en_name = expand_abbrevs(f"{_display(sys_val)}+{_display(meth_val)}")
            cn_parts = [v for v in (sys_en_to_cn.get(sys_val), meth_en_to_cn.get(meth_val)) if v]
            name = en_name + "; " + "+".join(cn_parts) if cn_parts else en_name
            rows.append({
                "relation": "sm",
                "name": name,
                "codes": "|".join(int_to_code(c, "LOINC") for c in sorted(group)),
                "note": "",
            })
    for key, group in sorted(lcn_groups.items()):
        if len(group) > 1:
            orig_prefix = key
            for c in group:
                if c in code_lcn_prefix:
                    orig_prefix = code_lcn_prefix[c]
                    break
            rows.append({
                "relation": "lcn", "name": _with_cn(expand_abbrevs(orig_prefix), group),
                "codes": "|".join(int_to_code(c, "LOINC") for c in sorted(group)),
                "note": "",
            })

    cache_path = os.path.join(out_dir, "_siblings_loinc.csv")
    pl.DataFrame(rows).write_csv(cache_path, quote_style="always")
    log.info(f"  Saved {len(rows):,} groups to {cache_path}")

    return siblings


# ─── SNOMED siblings ────────────────────────────────────────────────

def build_siblings_snomed(snomed_dir: str, out_dir: str) -> None:
    """Build SNOMED CT sibling groups from IS-A relationships.

    Siblings = concepts sharing a direct IS-A parent.
    Saves to _siblings_snomed.csv.
    """
    rel_path = os.path.join(snomed_dir, "Full", "Terminology")
    rel_file = None
    for fname in os.listdir(rel_path):
        if fname.startswith("sct2_Relationship_Full") and fname.endswith(".txt"):
            rel_file = os.path.join(rel_path, fname)
            break
    if not rel_file:
        log.error(f"sct2_Relationship file not found in {rel_path}")
        return

    desc_file = None
    for fname in os.listdir(rel_path):
        if fname.startswith("sct2_Description_Full-en") and fname.endswith(".txt"):
            desc_file = os.path.join(rel_path, fname)
            break

    # Load IS-A relationships via polars
    log.info(f"Loading SNOMED IS-A from: {rel_file}")
    rel_df = pl.read_csv(rel_file, separator='\t', infer_schema=False)
    rel_df = rel_df.filter(
        (pl.col("active") == "1") & (pl.col("typeId") == "116680003")
    ).select(["destinationId", "sourceId"])

    # Group by parent → list of children
    groups_df = rel_df.group_by("destinationId").agg(
        pl.col("sourceId")
    ).filter(pl.col("sourceId").list.len() >= 2)

    # Load concept names (FSN) — line-based parsing because term field may contain quotes
    concept_names: dict[str, str] = {}
    if desc_file:
        log.info(f"Loading concept names from: {desc_file}")
        with open(desc_file, "r", encoding="utf-8") as f:
            next(f)  # header
            for line in f:
                fields = line.split("\t")
                if fields[2] == "1" and fields[6] == "900000000000003001":  # active FSN
                    concept_names.setdefault(fields[4], fields[7])

    # Build output rows
    rows: list[dict] = []
    for row in groups_df.iter_rows(named=True):
        parent = row["destinationId"]
        children = row["sourceId"]
        parent_name = concept_names.get(parent, "")
        if parent_name and " (" in parent_name:
            parent_name = parent_name[:parent_name.rfind(" (")]
        rows.append({
            "relation": "isa",
            "name": expand_abbrevs(parent_name),
            "codes": "|".join(sorted(children)),
            "note": parent,
        })

    cache_path = os.path.join(out_dir, "_siblings_snomed.csv")
    pl.DataFrame(rows).write_csv(cache_path, quote_style="always")
    log.info(f"  SNOMED siblings: {len(rows):,} groups, saved to {cache_path}")


# ─── RxNorm siblings (ATC-based) ────────────────────────────────────

def build_siblings_rxnorm(
    rxnorm_dir: str,
    out_dir: str,
    locales: list | None = None,
) -> None:
    """Build RxNorm sibling groups based on WHO ATC classification.

    Groups RxNorm codes by shared ATC chemical subgroup (5-char, e.g. A10BA).
    This is the finest ATC level that produces multi-code sibling groups.
    Coarser levels (atc3/2/1) are strict supersets and can be derived at
    query time by truncating the ATC code stored in the note column.

    If locales are provided, enriches group names with local drug names.
    Saves to res/_siblings_rxnorm.csv.
    """
    rxnconso_path = os.path.join(rxnorm_dir, "rrf", "RXNCONSO.RRF")
    if not os.path.isfile(rxnconso_path):
        log.error(f"RXNCONSO.RRF not found: {rxnconso_path}")
        return

    log.info(f"Loading RxNorm ATC data from: {rxnconso_path}")

    conso_df = read_rrf(rxnconso_path, ["CUI", "SAB", "TTY", "CODE", "STR"])

    # ATC rows
    atc_df = conso_df.filter(pl.col("SAB") == "ATC")
    atc_name: dict[str, str] = {}
    for row in atc_df.unique(subset=["CODE"], keep="first").iter_rows(named=True):
        atc_name[row["CODE"]] = row["STR"]

    cui_atc: dict[str, set[str]] = defaultdict(set)
    for cui, code in atc_df.select(["CUI", "CODE"]).iter_rows():
        cui_atc[cui].add(code)

    # RxNorm rows — collect generic names per code (IN, PIN)
    rxn_df = conso_df.filter(pl.col("SAB") == "RXNORM")
    cui_rxn: dict[str, set[str]] = defaultdict(set)
    rxn_names: dict[str, set[str]] = defaultdict(set)  # code → {all names}
    bn_name: dict[str, str] = {}  # BN RXCUI → brand name
    for row in rxn_df.iter_rows(named=True):
        cui, code, tty, name = row["CUI"], row["CODE"], row["TTY"], row["STR"]
        cui_rxn[cui].add(code)
        if tty in ("IN", "PIN"):
            rxn_names[code].add(name)
        elif tty == "BN":
            bn_name[code] = name

    # Add brand names via tradename_of in RXNREL (IN RXCUI → BN RXCUI)
    rxnrel_path = os.path.join(rxnorm_dir, "rrf", "RXNREL.RRF")
    if os.path.isfile(rxnrel_path):
        with open(rxnrel_path, "r", encoding="utf-8") as f:
            for line in f:
                fields = line.split("|")
                if fields[7] == "tradename_of":
                    in_rxcui, bn_rxcui = fields[0], fields[4]
                    name = bn_name.get(bn_rxcui)
                    if name and in_rxcui in rxn_names:
                        rxn_names[in_rxcui].add(name)
        all_bn_names = set(bn_name.values())
        n_with_bn = sum(1 for names in rxn_names.values() if names & all_bn_names)
        log.info(f"  Brand names: {len(bn_name):,} BNs, {n_with_bn:,} INs with trade names")

    # Build ATC4 groups (5-char, e.g. A10BA — chemical subgroup).
    # This is the finest level that produces multi-code groups.
    # Coarser levels (atc3/2/1) are redundant (strict supersets);
    # finer level (atc5, 7-char) maps 1:1 to RxNorm codes.
    # The ATC code is stored in the note column, so coarser groupings
    # can be derived at query time by prefix truncation.
    atc4_groups: dict[str, set[str]] = defaultdict(set)

    shared_cuis = set(cui_atc) & set(cui_rxn)
    for cui in shared_cuis:
        rxn_codes = cui_rxn[cui]
        for atc_code in cui_atc[cui]:
            if len(atc_code) == 7:
                atc4_groups[atc_code[:5]].update(rxn_codes)

    n4 = sum(1 for g in atc4_groups.values() if len(g) > 1)
    log.info(f"  ATC groups: {n4} atc4 groups (from {len(shared_cuis):,} shared CUIs)")

    # Load local drug names from locale plugins (optional)
    locale_drug_names: dict[str, set[str]] = defaultdict(set)
    for locale in (locales or []):
        for atc_prefix, names in locale.drug_names().items():
            locale_drug_names[atc_prefix].update(names)

    def _group_name(atc_code: str, member_codes: set[str]) -> str:
        """Build group name: ATC English name + RxNorm member names + locale names."""
        parts: list[str] = []
        # ATC category name
        parts.append(atc_name.get(atc_code, atc_code))
        # RxNorm IN/PIN/BN names for all members
        member_names: set[str] = set()
        for code in member_codes:
            member_names.update(rxn_names.get(code, set()))
        if member_names:
            parts.extend(sorted(member_names))
        # Local drug names from locale plugins
        local: set[str] = set()
        for prefix, names in locale_drug_names.items():
            if prefix.startswith(atc_code):
                local.update(names)
        if local:
            parts.extend(sorted(local))
        return "; ".join(parts)

    # Build output rows
    rows: list[dict] = []
    for atc_code, rxn_codes in sorted(atc4_groups.items()):
        if len(rxn_codes) > 1:
            rows.append({
                "relation": "atc",
                "name": _group_name(atc_code, rxn_codes),
                "codes": "|".join(sorted(rxn_codes)),
                "note": atc_code,
            })

    cache_path = os.path.join(out_dir, "_siblings_rxnorm.csv")
    pl.DataFrame(rows).write_csv(cache_path, quote_style="always")
    log.info(f"  Saved {len(rows):,} groups to {cache_path}")


# ─── CUI enrichment ─────────────────────────────────────────────────

def enrich_siblings_with_cui(
    loinc_csv_path: str,
    snomed_csv_path: str,
    mrconso_path: str,
    loinc_skip_codes: set[str] | None = None,
) -> None:
    """Append CUI-based sibling groups to existing LOINC and SNOMED siblings CSVs.

    Same CUI = synonym codes → they are siblings within the same vocabulary.
    loinc_skip_codes: LOINC code strings to exclude (e.g. surveys, attachments).
    """
    skip_prefixes = TARGET_SYSTEMS["LOINC"]["skip_prefixes"]

    log.info(f"Scanning MRCONSO for CUI-based siblings: {mrconso_path}")
    conso_df = read_rrf(mrconso_path, ["CUI", "LAT", "STT", "ISPREF", "SAB", "CODE", "STR"])

    # LOINC CUI groups
    lnc_df = conso_df.filter(pl.col("SAB") == "LNC").select(["CUI", "CODE"])
    for prefix in skip_prefixes:
        lnc_df = lnc_df.filter(~pl.col("CODE").str.starts_with(prefix))
    if loinc_skip_codes:
        lnc_df = lnc_df.filter(~pl.col("CODE").is_in(list(loinc_skip_codes)))
    lnc_groups = lnc_df.group_by("CUI").agg(
        pl.col("CODE").unique()
    ).filter(pl.col("CODE").list.len() >= 2)

    # SNOMED CUI groups
    sno_df = conso_df.filter(pl.col("SAB") == "SNOMEDCT_US").select(["CUI", "CODE"])
    sno_groups = sno_df.group_by("CUI").agg(
        pl.col("CODE").unique()
    ).filter(pl.col("CODE").list.len() >= 2)

    # Preferred names
    pref_df = conso_df.filter(
        (pl.col("LAT") == "ENG") &
        (pl.col("ISPREF") == "Y") &
        (pl.col("STT") == "PF")
    ).select(["CUI", "STR"]).unique(subset=["CUI"], keep="first")
    cui_pref_name = dict(zip(pref_df["CUI"].to_list(), pref_df["STR"].to_list()))

    def _append_cui_siblings(groups_df: pl.DataFrame, csv_path: str, label: str) -> None:
        if len(groups_df) == 0:
            return
        rows: list[dict] = []
        for row in groups_df.sort("CUI").iter_rows(named=True):
            cui = row["CUI"]
            codes = sorted(row["CODE"])
            name = cui_pref_name.get(cui, "")
            rows.append({
                "relation": "cui",
                "name": expand_abbrevs(name),
                "codes": "|".join(codes),
                "note": cui,
            })
        with open(csv_path, "a", encoding="utf-8") as f:
            f.write(pl.DataFrame(rows).write_csv(include_header=False, quote_style="always"))
        log.info(f"  {label} CUI siblings: {len(rows)} groups appended")

    _append_cui_siblings(lnc_groups, loinc_csv_path, "LOINC")
    _append_cui_siblings(sno_groups, snomed_csv_path, "SNOMED")


# ─── Sibling expansion for mapping ──────────────────────────────────

def expand_with_siblings(
    rows: list[MappingRow],
    siblings: dict[int, dict[int, set[Rel]]],
    existing_pairs: set[tuple[int, int]],
    snomed_fan_out: dict[int, int],
    max_targets: int = 0,
) -> list[MappingRow]:
    """Expand mapping rows: for each (SNOMED, LOINC), add sibling LOINC codes.

    Siblings sharing >=2 grouping axes get distance=1 (strong);
    siblings sharing only 1 axis get distance=2 (weak).
    The path field records which relations linked the pair (e.g. "sibling:comp+lcn").
    """
    new_rows: list[MappingRow] = []

    loinc_pairs: list[tuple[int, str, str, int]] = []
    for row in rows:
        if row["target_system"] != "LOINC":
            continue
        sc_int = code_to_int(row["snomed_code"])
        lc_int = code_to_int(row["target_code"], "LOINC")
        loinc_pairs.append((sc_int, row["snomed_name"], row["cui"], lc_int))

    n_new = 0
    for sc, sc_name, cui, lc in loinc_pairs:
        sib_rels = siblings.get(lc, {})
        for sib, rels in sorted(sib_rels.items(), key=lambda x: -len(x[1])):
            pair = (sc, sib)
            if pair in existing_pairs:
                continue
            if max_targets and snomed_fan_out.get(sc, 0) >= max_targets:
                break
            existing_pairs.add(pair)
            snomed_fan_out[sc] = snomed_fan_out.get(sc, 0) + 1
            n_new += 1
            distance = 1 if len(rels) >= 2 else 2
            new_rows.append(MappingRow(
                snomed_code=str(sc),
                snomed_name=sc_name,
                cui=cui,
                target_system="LOINC",
                target_code=int_to_code(sib, "LOINC"),
                target_name="",
                target_tty="LN",
                path="sibling:" + "+".join(r.label for r in sorted(rels)),
                distance=distance,
            ))

    if n_new:
        log.info(f"  Sibling expansion: {n_new:,} new LOINC rows")
    return new_rows


# ─── CLI subcommand ──────────────────────────────────────────────────

def cmd_siblings(args: Namespace) -> None:
    """Subcommand: siblings — build _siblings_loinc.csv + _siblings_snomed.csv."""
    out_dir = args.output
    os.makedirs(out_dir, exist_ok=True)

    # LOINC siblings
    loinc_csv = os.path.join(args.loinc_dir, "LoincTableCore", "LoincTableCore.csv")
    loinc_axes = parse_loinc_axes(loinc_csv_path=loinc_csv)
    if not loinc_axes:
        log.error("Failed to parse LOINC axes")
    else:
        build_siblings_loinc(loinc_axes, out_dir, loinc_dir=args.loinc_dir,
                             umls_dir=getattr(args, "umls_dir", ""))

    # SNOMED siblings
    if args.snomed_dir:
        build_siblings_snomed(args.snomed_dir, out_dir)
    else:
        log.warning("--snomed-dir not set, skipping SNOMED siblings")

    # RxNorm siblings (ATC-based)
    rxnorm_dir = args.rxnorm_dir
    if rxnorm_dir:
        from .locales import discover_locales
        build_siblings_rxnorm(rxnorm_dir, out_dir, locales=discover_locales(args))
    else:
        log.warning("--rxnorm-dir not set, skipping RxNorm siblings")

    # CUI enrichment (optional)
    if args.umls_dir:
        mrconso_path = os.path.join(args.umls_dir, "META", "MRCONSO.RRF")
        if os.path.isfile(mrconso_path):
            loinc_skip: set[str] | None = None
            if os.path.isfile(loinc_csv):
                loinc_skip = _load_loinc_skip_codes(loinc_csv)
            loinc_sib_path = os.path.join(out_dir, "_siblings_loinc.csv")
            snomed_sib_path = os.path.join(out_dir, "_siblings_snomed.csv")
            enrich_siblings_with_cui(loinc_sib_path, snomed_sib_path, mrconso_path,
                                     loinc_skip_codes=loinc_skip)
        else:
            log.warning(f"MRCONSO.RRF not found: {mrconso_path}")
