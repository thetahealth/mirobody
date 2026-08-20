# mirobody.indicator

Extensible indicator search engine with domain-specific adapters and graph-based concept expansion.

## Architecture

```
indicator/
  fhir/                  # FHIR-vocabulary domain implementation
    search.py            # FhirAdapter: FHIR tables, th_series_data, etc.
    graph_builder.py     # Build fhir_id graph binary from bridge/sibling CSVs
    bridge.py            # Cross-vocabulary bridge files
    siblings.py          # Same-system sibling groups
    merge.py             # Merge pipeline + trigger graph build
    common.py            # SYSTEMS, code_to_fhir_id, RRF reader, shared types
    test.py              # Verify output against known test cases
    locales/             # Locale plugins for local drug/vaccine names
    units/               # Free-text unit string → canonical UCUM + LOINC PROPERTY family
      normalize.py       # normalize_unit, parse_value_unit, ParsedQuantity
      families.py        # UCUM_FAMILY (~310) + AMBIGUOUS_UNITS + unit_family / unit_families
      tokens.py          # MORPHEMES + ALIASES data (~600 multilingual tokens)
    embeddings/          # Offline embedding bundle (mirobody/res/fhir_*)
      db.py              # Producer: from fhir_indicators DB (compat mode)
      ref.py             # Producer: from ~/ref + Gemini API (terminal mode)
      local.py           # Loader + dtype/path constants + atomic_swap_keep_backup
      names.py           # ~/ref name parsers + `code-names` post-step CLI
      migrate.py         # One-shot recovery: legacy 4-file → new layout
  concept_graph.py       # ConceptGraphBuilder ABC + ConceptGraph (load/save/query)
  search.py              # DomainAdapter ABC + search/resolve engines + ResolveResult
  embed.py               # Batch-fill embedding_gemini for DB tables
  main.py                # CLI entry point
```

## Two retrieval modes

`DomainAdapter` exposes two complementary entry points:

| Method | Scope | Input | Returns | Use case |
|---|---|---|---|---|
| `search(user_id, embeddings, top_k, ...)` | per-user | pre-computed query embeddings | `(dict[fhir_id, score], list[non_fhir])` | "What does *this user* have that matches the query?" |
| `resolve(term, top_k, *, systems=None)` | global | free text | `list[ResolveResult(system, code, name, score)]` | "What canonical codes does this term map to?" — ETL / terminology mapping |
| `resolve_many(terms, top_k, *, systems=None)` | global | list of free text | `list[list[ResolveResult]]` (positional, `None` → `[]`) | Batched bulk version: one embedding-API call + one chunked GEMM. **Use this any time you have more than a handful of terms** — ~20–30× faster than looping `resolve()`. |

`search` joins `th_series_data` to scope to one user; `resolve` runs cosine over the full vocabulary (top_k *per system*, sorted globally by score).

## How search works

1. **Embed** -- Gemini embeddings (1024-dim) for user keywords (orchestrator-side; adapter receives vectors)
2. **Vector recall** -- adapter queries domain tables via pgvector cosine distance
3. **Graph expansion** -- ``adapter.expand(top_ids)`` pulls in bridge + sibling neighbors. Default is identity; ``FhirAdapter`` overrides it to consult ``fhir_concept_graph.bin``. New domains can opt out by leaving ``expand`` as the default.
4. **Merge & rank** -- combine results, filter by score threshold, sort descending

## How resolve works

1. **Embed** -- adapter computes the term's embedding internally (callers pass plain text)
2. **Per-system top_k** -- score against the full standard vocabulary; for each code system (LOINC / SNOMED_CT / RXNORM / CVX / DCM / THETA), keep its `top_k` best matches
3. **Global score sort** -- merged result is sorted by score descending, so callers can compare candidates across vocabularies and judge by relative score (no opaque threshold knob)

```python
from mirobody.indicator.fhir.search import FhirAdapter

adapter = FhirAdapter(bundle_dir=...)
results = await adapter.resolve("blood glucose", top_k=3, systems=["LOINC"])
# [ResolveResult(system='LOINC', code='2345-7', name='Glucose [Mass/Vol]', score=0.91), ...]

# Bulk: one shared embedding-API call + one chunked GEMM over the index.
batch = await adapter.resolve_many(
    ["blood glucose", "metformin", "chest x-ray"],
    top_k=1, systems=["LOINC", "SNOMED_CT"],
)
# batch[i] aligns with input[i]; positions with empty/un-embeddable terms map to [].
```

Sweet spot batch size is **~100** — the no-waste intersection of both providers' embedding `batch_limit` (gemini=100, qwen=10). Cosine matmul cost scales sub-linearly with batch size (BLAS GEMM efficiency), so going larger still helps but pays a `(B × N × 4B)` score matrix in RAM.

### Ranking layers

Beyond raw cosine, `resolve` filters and re-ranks candidates in several layers. When a wrong top-1 needs fixing, identify which layer should have caught it — different callers exercise different layers.

| Layer | File | Effect | Gating |
|---|---|---|---|
| `loinc_skip_mask` | `common.py` (`_SKIP_CLASSTYPES`, `_SKIP_CLASS_PREFIXES`, `_SKIP_STATUSES`) | hard drop: surveys / docs / admin / deprecated / discouraged | unconditional |
| `loinc_demote_mask` | `common.py` (`_DEMOTE_STATUSES`, `_DEMOTE_CLASSES`, `_HAND_DEMOTE_NAME_PATTERNS`) | -2.0 cosine penalty in `FhirAdapter._gsort` (any non-demoted peer wins) | unconditional |
| `FAMILIES` | `resolve/specificity.py` → `_LOINC_FILTERS` | per-family hard drop in v2 CLI pipeline (intake-recall, treatment-goal, challenge-test, baseline, trough/peak, posture, dialysis, ...) | per-family query-side license tokens (`摄入 / 目标 / pre / post / ...`) — a query that licenses a family doesn't drop its members |
| `_nonspecific_specimen_keep` | `resolve/pipeline.py` | drops `SYSTEM=XXX` (in Specimen) LOINC rows when a same-COMPONENT non-XXX peer exists (~2,300 rows) | query-side license (`样品 / specimen / food / supplement / 补充剂 / ...`) |
| `_SYSTEM_PREFERENCE` tier | `resolve/pipeline.py` (stages 2 & 4 of `_loinc_picks_topk`) | within-family AND cross-specimen rerank prefers `Ser/Plas > Bld > Plas > Ser > Bld.cap > ...` for same analyte | none — corpus structure only (specimen variants exist for the same analyte) |
| `_component_nonlab_mask` / `_component_ratio_mask` | `embeddings/axes.py` | drops non-lab (`X intake`, `X goal`) and ratio (`X/Y`, fractional excretion, clearance) COMPONENT vocab rows at the axis-vocab argmax — prevents wrong anchor selection | query-side license (`_QUERY_WANTS_NONLAB_RE`, `_QUERY_WANTS_RATIO_RE`) |

Three caller paths exercise different subsets:

- **`FhirAdapter.resolve_many`** (library / pgvector callers): runs `skip` + `demote` only. Demote is unconditional — calibrated for lab-results callers (体检报告). Order-entry callers (医嘱) where `LDL goal` etc. is a valid answer would need the `_HAND_DEMOTE_NAME_PATTERNS` patterns migrated to query-license gating; not done yet because no such caller exists.
- **CLI `resolve` v2 path**: runs `skip` + every `_LOINC_FILTERS` entry. Filter gating is query-content-driven (more flexible than a binary report-type flag), so the same pipeline serves all caller intents.
- **CLI `resolve --axes` axes path**: runs the two COMPONENT-vocab masks at axis argmax, then `_lookup_loinc_codes` filters survivors by anchored COMPONENT. When ``--axes`` is set, the v2 CLI path also runs in parallel and `_merge_axes_and_legacy` (in `mirobody.indicator.resolve`) arbitrates between them.

#### Axes-vs-legacy arbitration (`--axes` mode)

When `resolve --axes` is invoked, both the axes-vocab pipeline and the v2 CLI pipeline run on the same query. `_merge_axes_and_legacy` picks the winner via three sequential rules:

1. **Alias-locked axes COMPONENT** — if some alias key in the query (from `aliases/<lang>.tsv` in the LOINC bundle) maps to a target that **exactly equals** the axes COMPONENT name (case-insensitive), and that name appears in the axes top pick's LCN, and the legacy top's LCN does NOT contain that name, axes wins regardless of score. Catches ``β-葡萄糖醛酸苷酶`` → axes anchors on ``Beta glucuronidase`` (alias 1:1 map confirms) while legacy's cosine drifts onto ``Glucose in Stool`` because the embedder sees ``粪便`` + ``葡萄糖`` together. Exact-match (not substring) is critical — a substring rule would have falsely locked ``维生素 → Vitamin`` against axes COMPONENT ``Vitamin A/Retinol binding protein``, ``Glucose standard deviation``, etc.

2. **Close-tie → legacy** — when `|axes_score - legacy_score| < 0.001`, legacy wins. At this scale the two pipelines agree the top candidate is in a tight cosine cluster and legacy's deeper rerank stack (specificity / family / digit / scale / system-tier) is the more reliable arbitrator. Catches ``肺吸虫 IgG`` where axes 0.7410 / legacy 0.7408 disagree on genus and legacy hits the right Paragonimus code. Threshold is narrow on purpose — a wider 0.005 would falsely flip ``用力肺活量`` (axes FVC 0.7593 / legacy ``FVC percent change`` 0.7545) to the wrong sibling.

3. **Score arbitration** — default `max(axes_score, legacy_score)`. Both scores live in the same query × LongCommonName cosine space, so direct comparison is meaningful.

**Empty-legacy rule** (overrides everything): when legacy returns no LOINC result, the merged pick is also empty. Legacy emptiness signals "no confident match" from its 10+ filter stages and should not be auto-filled with axes's best-guess — e.g. ``HPV-23 / HPV-46 / HPV-83 / HPV-8`` where LOINC has no specific code, axes-pipeline picks a different-numbered HPV variant by closest cosine.

#### Status taxonomy

LOINC's own `STATUS` column drives the skip vs. demote split. Trusting STATUS is intentional: LOINC documents what each value means.

| Status | Treatment | Why |
|---|---|---|
| `DEPRECATED` | hard drop (`_deprecated_drop_mask`) | LOINC ships a same-concept successor — always reachable |
| `DISCOURAGED` | hard drop (`_SKIP_STATUSES`) | LOINC ships a `MAP_TO` replacement |
| `TRIAL` | soft demote (`_DEMOTE_STATUSES`) | provisional; surfaces only when no ACTIVE peer exists |
| `LABORDERS.ONTOLOGY` (CLASS) | soft demote (`_DEMOTE_CLASSES`) | abstract `[Measurement]` placeholders that drag generic queries |

`TRIAL` and the ontology placeholders use soft demote so that if no non-demoted peer exists in the family, they still surface — the "fall back to deprecated/trial when nothing else fits" semantics.

#### Specimen preference

LOINC ships many SYSTEM-generic codes (`SYSTEM=XXX`, displayed as `in Specimen`) alongside specimen-specific peers (`in Serum or Plasma`, `in Blood`, `in Urine`, ...). For lab queries the canonical answer is almost always the specific specimen; the generic-Specimen form is the right pick only when LOINC literally ships no specific peer (HPV DNA, Vit A / E mass/mass, CD8/Lymph in Specimen).

`_nonspecific_specimen_keep` enforces this in the v2 CLI pipeline:

- Corpus mask: LOINC rows where `SYSTEM=XXX` AND a same-COMPONENT peer with a non-XXX SYSTEM exists (~2,300 rows). Built once via `load_axis_centroids`'s `row_components` + `row_value_idx['SYSTEM']`.
- License gating: queries carrying `样品 / 标本 / specimen / food / supplement / 补充剂 / ...` bypass the filter — food / supplement callers that genuinely want generic XXX get it.
- COMPONENTs with NO non-XXX peer (HPV DNA, Vit A / E, CD8/Lymph in Specimen) are not in the mask — those XXX codes ARE the canonical form.

Catches misses like `钙 → 87477-6 Calcium in Specimen` (now `17861-6 Calcium in Ser/Plas`), `维生素D3 → 87671-4 Mass/mass in Specimen` (now `33958-0 in Ser/Plas`), and the lymphocyte panel `CD3+CD4+ cells in Specimen` (now `in Blood`).

#### Specimen tier (`_SYSTEM_PREFERENCE`)

Within an analyte family, surviving cosine candidates are reranked by specimen tier so the clinically canonical specimen wins even when cosine puts a less-common variant ahead. `_SYSTEM_PREFERENCE` ordering:

```
Ser/Plas > Bld > Plas > Ser > Bld.cap > BldA > BldV > Bld.dot > BldC
```

Tuple position = preference tier (lower wins). SYSTEM values not in the tuple get tier `-1` and don't affect the sort — rare specimens (Body fld, Tiss, ...) ride cosine + scale alone.

Two stages consume the tier:

1. **Stage 2 (within-family rerank)** — when matching rows share `loinc_family_key` (or family-prefix anchor), sort by `(scale_tier, system_tier, -cosine)`. Picks the canonical specimen variant within one family.
2. **Stage 4 (cross-specimen rerank)** — strip the trailing `in <specimen> [by <method>]` from `loinc_family_key` to get a bare analyte key. If a row in `topN_picks` has the same bare key AND a strictly better system tier than the current pick, swap it in. Required because `loinc_family_key` keeps the specimen in the family key (`calcium in blood` vs `calcium in serum or plasma` are distinct families), so without stage 4 the tier preference can't cross between them.

Bare-key match (not prefix) for stage 4 prevents `beef ige ab` ≠ `beef igg ab` false positives — different antibody classes are different analytes, not specimen variants.

Catches `钙 in Blood → 2000-8 Calcium in Ser/Plas`, `维生素D3 Mass/mass in Specimen → 33958-0 in Ser/Plas`, `Folate in Blood → 2284-8 in Ser/Plas`. Analytes with no Ser/Plas variant in LOINC (HbA1c, Lead, Hb, Hct) stay in `Bld` — those families have no tier-0 slot, so tier-1 `Bld` wins on its own merit.

#### Tiebreakers

When multiple codes survive every filter and rerank stage:

1. **`common_test_rank`** — LOINC-published per-code observation frequency (lower rank = more common; 0 = never observed in real labs, pushed to back).
2. **LOINC code** alphabetically — deterministic final tiebreak.

## Unit normalization

`fhir/units/` parses free-text "value + unit" strings into structured `ParsedQuantity(comparator, value, canonical_ucum)` and looks up the corresponding LOINC PROPERTY family. Designed for ingesting clinical and wearable data where the same indicator gets written different ways across languages, locales, and devices. Pure local computation — no DB, no embedding API.

```python
from mirobody.indicator.fhir.units import (
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
python -m mirobody.indicator normalize "90次每分钟" "<5.6 mg/dL" "600步"
# {"input": "90次每分钟", "comparator": "", "value": 90.0,  "unit": "/min",    "family": "NRat"}
# {"input": "<5.6 mg/dL", "comparator": "<", "value": 5.6,  "unit": "mg/dL",   "family": "MCnc"}
# {"input": "600步",      "comparator": "", "value": 600.0, "unit": "{steps}", "family": "Num"}
```

## Concept graph

`concept_graph.py` defines two roles:

- **`ConceptGraphBuilder`** (ABC) -- interface that domain-specific builders implement
- **`ConceptGraph`** -- binary loader + query engine

### ConceptGraphBuilder interface

```python
from mirobody.indicator.concept_graph import ConceptGraphBuilder

class MyDomainBuilder(ConceptGraphBuilder):
    def load_bridges(self, src_dir: str) -> dict[int, set[int]]:
        # return {node_id: {neighbor_ids}} — symmetric edges
        return {1: {2, 3}, 2: {1}, 3: {1}}

    def load_siblings(self, src_dir: str) -> list[list[int]]:
        # return [[id, ...], ...] — each inner list is a cluster
        return [[4, 5, 6]]
```

| Method | Returns | Description |
|--------|---------|-------------|
| `load_bridges(src_dir)` | `dict[int, set[int]]` | Cross-system edges (must be symmetric) |
| `load_siblings(src_dir)` | `list[list[int]]` | Sibling groups (clusters of related nodes) |
| `build(src_dir, dest_path)` | `None` | Template method: calls load methods then serializes to binary |

### ConceptGraph

| Method | Description |
|--------|-------------|
| `ConceptGraph.get(file_path)` | Load binary and return cached graph instance (path-keyed; required arg, no implicit "bundled default") |
| `graph.bridge_neighbors(id)` | Cross-system neighbor IDs |
| `graph.sibling_neighbors(id, max_per_id=50)` | Same-system neighbor IDs (smaller groups first) |
| `graph.neighbors(id)` | All neighbors (bridge + sibling) |
| `graph.stats()` | Dict with bridge/sibling counts |

### Typical pipeline usage

```python
from .fhir.graph_builder import FhirGraphBuilder

builder = FhirGraphBuilder()
builder.build(out_dir)                  # writes <out_dir>/fhir_concept_graph.bin
```

The output filename comes from the subclass's ``DEFAULT_BIN_NAME`` (e.g. ``FHIR_GRAPH_BIN = "fhir_concept_graph.bin"``). The framework has no opinion — each domain owns its filename so multiple domains can coexist in the same dir without ambiguity.

## Adding a new domain

1. Create `<domain>/search.py` with a `DomainAdapter` subclass (set `domain = "<name>"` for auto-registration). Override `expand(top_ids)` if the domain has a graph; default is identity.
2. Create `<domain>/graph_builder.py` implementing `ConceptGraphBuilder`. Set `DEFAULT_BIN_NAME = "<domain>_concept_graph.bin"` so `build(src_dir)` knows the output filename.
3. Add pipeline scripts (`siblings.py`, `bridge.py`, `merge.py`, etc.) under `<domain>/`.
4. Import the adapter in `cmd_search` to trigger registration, then call `search(adapter, keywords=..., user_id=...)`.

---

# FHIR Indicator Pipeline

This guide walks through setting up the FHIR indicator knowledge graph from scratch -- from downloading reference data to producing the final `fhir_concept_graph.bin` used by the search service. The graph cross-links concepts across LOINC / SNOMED CT / RxNorm / DCM / CVX (the FHIR-recognised code systems).

## Step 1: Obtain reference data

All external datasets default to `~/ref/`. UMLS, SNOMED CT, and RxNorm require a free [UMLS license from NLM](https://www.nlm.nih.gov/research/umls/index.html).

### 1.1 UMLS Metathesaurus

Download the "UMLS Metathesaurus Full Subset" from [NLM](https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html) and extract:

```
~/ref/umls-2025AB/
  META/
    MRCONSO.RRF    # Concept names and sources
    MRREL.RRF      # Relationships between concepts
```

Used by `siblings` (CUI-based enrichment) and `bridge` (cross-vocabulary code mappings).

### 1.2 SNOMED CT US Edition

Download the "US Edition RF2 Release" from [NLM](https://www.nlm.nih.gov/healthit/snomedct/us_edition.html) and extract:

```
~/ref/SnomedCT_ManagedServiceUS_PRODUCTION_US1000124_YYYYMMDD/
  Full/Terminology/
    sct2_Concept_Full_*.txt
    sct2_Description_Full_*.txt
    sct2_Relationship_Full_*.txt
```

Used by `siblings` (IS-A hierarchy, concept names).

### 1.3 LOINC

Download from [loinc.org](https://loinc.org/downloads/) and extract:

```
~/ref/Loinc_2.78/
  LoincTableCore/
    LoincTableCore.csv              # Core table (COMPONENT, SYSTEM, METHOD_TYP, etc.)
  AccessoryFiles/
    PartFile/
      Part.csv                      # Display name expansion
      LoincPartLink_Primary.csv     # Part-to-LOINC linkage
```

Used by `siblings` (axis parsing, LCN groups, skip-code filtering, part display names).

### 1.4 RxNorm

Download the "RxNorm Full Monthly Release" from [NLM](https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html) and extract:

```
~/ref/RxNorm_full_MMDDYYYY/
  rrf/
    RXNCONSO.RRF    # RxNorm concept names
    RXNREL.RRF      # RxNorm relationships (tradename_of, etc.)
```

Used by `siblings` (ATC classification, generic/brand names).

### 1.5 Local drug/vaccine catalogs (optional)

The pipeline supports **locale plugins** (`fhir/locales/`) for enriching RxNorm sibling groups with local drug and vaccine names. Plugins are auto-discovered at build time — any `LocalePlugin` subclass in the `locales/` package is loaded if its required CLI arg is provided. Each locale is optional -- the pipeline runs without them.

Currently available locales:

| Locale | Plugin | Data source | CLI arg |
|--------|--------|------------|---------|
| China | `locales/cn.py` | [NHSA drug catalog](https://github.com/badman200/medicine) (`medicine_data.json`) | `--nhsa-catalog ~/ref/medicine_data.json` |

#### Adding a new locale

1. Create `fhir/locales/<locale>.py` implementing `LocalePlugin`:

```python
from . import LocalePlugin

class JapanLocale(LocalePlugin):
    @property
    def name(self) -> str:
        return "jp"

    @classmethod
    def from_args(cls, args) -> "JapanLocale | None":
        path = getattr(args, "jp_catalog", "")
        return cls(path) if path and os.path.isfile(path) else None

    def drug_names(self) -> dict[str, set[str]]:
        """Return {ATC code prefix -> set of Japanese drug names}."""
        ...

    def vaccine_names(self) -> dict[str, str]:
        """Return {CVX code -> local vaccine name}."""
        ...
```

2. Add the corresponding CLI arg (e.g. `--jp-catalog`) in `main.py` under the `siblings` subparser. The plugin is auto-discovered — no changes needed in `siblings.py`.

## Step 2: Build the knowledge graph

The pipeline has three stages that must be run in order. Each stage reads from `~/ref/` and writes to the output directory (default: `out/` relative to the package).

### 2.1 Build sibling groups

Sibling groups cluster codes that are semantically related within the same vocabulary system.

```bash
# Basic (no locale enrichment)
python -m mirobody.indicator siblings

# With Chinese drug names
python -m mirobody.indicator siblings --nhsa-catalog ~/ref/medicine_data.json
```

This produces:

| Output file | Content |
|------------|---------|
| `_siblings_loinc.csv` | LOINC code groups by COMPONENT, SYSTEM+METHOD, LCN prefix, and CUI. Excludes non-lab codes (CLASSTYPE 3/4, SURVEY, PHENX, ATTACH, DOC, ADMIN). Chinese names from LNC-ZH-CN. |
| `_siblings_snomed.csv` | SNOMED CT groups by IS-A parent and CUI sharing. |
| `_siblings_rxnorm.csv` | RxNorm groups by WHO ATC chemical subgroup (5-char, e.g. `A10BA`). Includes generic (IN/PIN), brand (BN) names via `tradename_of`, plus local names from locale plugins. |
| `_siblings_skipped.csv` | Groups blacklisted as too generic. |

**Sibling relation types** (ordered by reliability, lower = more reliable):

LOINC:
- `cui` (0) -- UMLS synonym codes
- `comp` (2) -- Same COMPONENT base (e.g. "neutrophils")
- `lcn` (3) -- Same LONG_COMMON_NAME prefix (e.g. "Hematocrit")
- `sm` (4) -- Same SYSTEM+METHOD (e.g. "Bld+DXA")

SNOMED:
- `cui` (0) -- UMLS synonym codes
- `isa` (1) -- Children of same IS-A parent

RxNorm:
- `atc` (5) -- Same ATC 5-char chemical subgroup (e.g. `A10BA`)

Medical abbreviations in group names are automatically expanded for better embedding quality (e.g. `IgE` -> `IgE (Immunoglobulin E)`). See `ABBREV_EXPAND` in `siblings.py` for the full list.

### 2.2 Build cross-vocabulary bridges

Bridges connect codes across different vocabulary systems.

```bash
python -m mirobody.indicator bridge
```

This produces:

| Output file | Bridge path |
|------------|-------------|
| `_bridges_icd.csv` | SNOMED -> ICD -> LOINC (transitive) |
| `_bridges_mrrel.csv` | SNOMED <-> LOINC (direct UMLS relationships) |
| `_bridges_rxnorm.csv` | SNOMED <-> RxNorm (CUI sharing + MRREL) |
| `_bridges_loinc_rxnorm.csv` | LOINC <-> RxNorm (drug tests <-> ingredients) |
| `_bridges_jaccard.csv` | SNOMED <-> LOINC (name similarity, Jaccard >= 0.5) |
| `_bridged_snomed.csv` | All SNOMED codes that appear in any bridge |
| `_unbridged_loinc.csv` | LOINC sibling groups with no SNOMED bridge |

### 2.3 Merge and build binary graph

Two-phase step: merge CSV files into `concepts.csv`, then build the binary concept graph for runtime use.

```bash
python -m mirobody.indicator merge
```

**Phase 1 -- `concepts.csv`:**

1. Load all sibling CSVs and snapshot native codes per name.
2. Inject orphan LOINC codes (from `LoincTableCore.csv`) not in any sibling group.
3. Inject bridge-linked codes into rows. Bridged codes go into separate columns (`loinc_bridged`, `rxnorm_bridged`) to preserve provenance.
4. Filter SNOMED-only rows with no LOINC/RxNorm bridge (unreachable from lab data).
5. Prefix-subset dedup via trie.

Output columns: `name`, `snomed_codes`, `loinc_codes`, `loinc_bridged`, `rxnorm_codes`, `rxnorm_bridged`.

**Phase 2 -- `fhir_concept_graph.bin`:**

1. Stream bridge CSVs, encode codes as canonical packed fhir_ids via `code_to_fhir_id(system, code)`, build bidirectional adjacency. Jaccard bridges capped at 150 codes/row.
2. Stream sibling CSVs, encode codes as canonical packed fhir_ids, store as flat groups. Retired/inactive codes are retained for query-expansion recall — graph topology is decoupled from corpus membership.
3. Serialize to zlib-compressed binary:
   - Header: magic (`CGPH`) + version (2) + counts
   - Bridges: half-edge storage (src < dst), expanded at load time. IDs are 64-bit.
   - Siblings: per-group members + reverse index for O(1) lookup. IDs are 64-bit.

### 2.4 Batch-fill embeddings

After importing new indicators into `fhir_indicators` or `th_series_dim`, run `embed` to compute Gemini embeddings for rows that don't have one yet:

```bash
python -m mirobody.indicator embed          # both tables
python -m mirobody.indicator embed series   # th_series_dim only
python -m mirobody.indicator embed fhir     # fhir_indicators only
```

Processes rows in batches of 100, skipping rows where `embedding_gemini` is already set. For `fhir_indicators`, only rows with a non-NULL `llm_description` are embedded.

### 2.5 Inspect (optional)

```bash
python -m mirobody.indicator inspect --system LOINC --code 718-7
```

Shows the concept-graph bridges and siblings recorded for one `(SYSTEM, CODE)`
node — the quickest way to confirm a merge produced what you expected.

(There was a `test` subcommand documented here that does not exist: running it
exits with `invalid choice: 'test'`. The resolver's actual regression gate is
`pytest mirobody/test_engine_coverage.py`, the 98-case benchmark the README
headline number comes from.)

### 2.6 All-in-one

```bash
python -m mirobody.indicator siblings [--nhsa-catalog ~/ref/medicine_data.json]
python -m mirobody.indicator bridge
python -m mirobody.indicator merge
python -m mirobody.indicator embed
```

> **`benchmarks/` is not in this repository.** Several docstrings in this
> package reference scripts under `benchmarks/` (`build_loinc_bundle.py`,
> `run_resolve.py`, `mine_phase2.py`, …). That is a maintainer-side working
> directory and was never tracked here. Everything required to *use* the
> shipped bundles is present; the scripts that mint them from raw LOINC/UMLS
> releases are not, since those releases are licensed per user — see
> `LICENSE-3RD-PARTY`.

## Other build steps

These produce the shipped bundles and were undocumented here despite being
required to regenerate `mirobody/res/`. One line each; `--help` carries the
full contract.

| Command | Produces |
|---------|----------|
| `loinc-alias` | `fhir_alias_index.pkl` — multilingual lexical alias → corpus-row inverted index |
| `loinc-lexicon` | `aliases/{lang}.tsv` in the bundle — per-language src → canonical-EN mapping |
| `loinc-skip` | `fhir_loinc_skip.npy` — row-aligned mask of LOINC codes resolve must exclude |
| `loinc-rank` | `fhir_loinc_rank_bonus.npy` — cosine bonus from LOINC's COMMON_TEST_RANK |
| `loinc-axis-vocab` | `res/loinc_axes/<AXIS>.tsv` — per-axis LOINC Part vocabularies |
| `loinc-axis-emb` | `res/loinc_axes/<AXIS>.npy` — embeddings of the above |
| `analyte-digit` | `analyte_digit.tsv` in the bundle — chemical name → numeric-subtype aliases |
| `dose-index` | `fhir_dose_index.npz` — (value, UCUM unit) pairs mined from display names |
| `snomed-axis-aliases` | `res/snomed_axes/aliases/{lang}.tsv` — SNOMED aliases from UMLS MRCONSO |
| `taxonomy` | concept taxonomy tables |

## Output summary

After running all three steps, the output directory contains:

```
concepts.csv                  # Final merged concept table
fhir_concept_graph.bin        # Binary graph for runtime search
_siblings_*.csv               # Intermediate sibling groups
_bridges_*.csv                # Intermediate bridge files
_bridged_snomed.csv           # SNOMED codes with bridges
_unbridged_loinc.csv          # LOINC groups without bridges
_siblings_skipped.csv         # Blacklisted groups
```

Files prefixed with `_` are intermediate. The runtime search service only needs `fhir_concept_graph.bin`.

## Step 3: Build the offline embedding bundle

> **Note** — Don't confuse this step with Step 2.4. Two CLI commands have similar names but do different jobs:
>
> | Command | Reads | Writes | When to run |
> |---|---|---|---|
> | `embed` (Step 2.4) | `th_series_dim` / `fhir_indicators` rows missing `embedding_gemini` | DB `embedding_gemini` column | After importing new indicators, so pgvector queries can find them |
> | `embeddings` (Step 3) | `fhir_indicators` (or ~/ref source files) | Local files under `mirobody/res/` | When you want `resolve` / `search` to skip pgvector entirely (offline mode) |
>
> Both call the Gemini API but write to different places. `embed` is **DB maintenance**; `embeddings` is **offline-bundle production**.

`resolve` and `search` can run without DB queries (10–100× faster, no network) when three artifacts exist under `mirobody/res/`:

| Artifact | Content | Required? |
|----------|---------|-----------|
| `fhir_embeddings.npy` | structured `(N,)` of `[fhir_id i8, emb f2[1024]]`, fp16 L2-normalised | yes |
| `fhir_meta.csv.gz` | `(N,)` rows of `name` + `code_str` (latter only for DCM/THETA hash rows) | optional (search works without; resolve `name` empty) |
| `fhir_id_map.npy` | `(N,)` int64 — `db_pks[r]` is the `fhir_indicators.id` for embedding row `r` | optional (compat mode only) |

**None of these files is provider-tagged, and the filename is fixed** —
`local.py::EMB_BASENAME` is the literal `fhir_embeddings.npy`. A bundle
directory therefore holds the vectors of exactly ONE embedding model, and
nothing in the artifact records which one. (This paragraph used to describe a
`DIM_EMBEDDING_PROVIDER` config key selecting between `fhir_embeddings.npy` and
sibling `fhir_embeddings_<provider>.npy` files via an `emb_basename()` helper.
No such key is read by any Python file and no such helper exists; the scheme was
documented but never built. Removed rather than left as a description of
imaginary behaviour.)

That the model is unrecorded is the sharp edge here, because a mismatched
corpus/query pair does not fail — it returns confident nonsense. A matrix built
by one Qwen3-Embedding serving config, queried with another, answered `空腹血糖`
with *"Widespread delusions [DI-PAD]"*. When swapping providers, re-export
**all three** files together against the same `fhir_indicators` snapshot, and
keep the query side on the same `EMBEDDING_PROVIDER`.

All three files are **row-aligned by index** to the active emb npy — the i-th meta row and the i-th id_map entry describe the same concept as `arr[i]`. Loaders abort if row counts disagree; never half-aligned.

`fhir_id` is a packed canonical id derived from `(system, code)` via `code_to_fhir_id` — **independent of** `fhir_indicators.id`. Layout: bits 60–62 = system enum index (matches `SYSTEMS` tuple in `common.py`), bits 0–59 = code int (`int(code)` for numeric vocabs, blake2b digest `>> 4` for DCM and THETA where the original string lives in `meta.code_str`). `SYSTEMS` is **append-only** because its index is bit-packed into every existing fhir_id. (The DB column keeps its legacy name `indicator_standard`; in code we use the FHIR `system` vocabulary throughout.)

### 3.1 From DB (compat mode)

Use when `fhir_indicators` is populated and `th_series_data.fhir_id` rows still key by DB pk:

```bash
python -m mirobody.indicator embeddings --from-db    # writes all three artifacts
python -m mirobody.indicator code-names              # fills name column from ~/ref
```

`embeddings --from-db` streams `fhir_indicators` rows with the active provider's embedding column set (`embedding_gemini` / `embedding_qwen3`, selected via `EMBEDDING_PROVIDER` through `resolve_fhir_embedding_column`) in a **single pass** that produces all three artifacts at once: each fetched row contributes its embedding (→ npy `emb`), canonical fhir_id (→ npy `fhir_id`), DB pk (→ id_map `db_pks[r]`), and original code string for hash rows (→ meta `code_str`).

Embedding download is checkpoint-resumable via memmap partials + `progress.json` in `out/` (handles Ctrl-C / DB disconnects across hours).

`code-names` is a separate post-step because the DB query carries no display-name column — names come from ~/ref's LOINC LCN, SNOMED FSN, RxNorm best-TTY, CVX full-name, and DCM Annex D Code Meanings.

After `th_series_data.fhir_id` is backfilled to canonical (terminal mode), delete `fhir_id_map.npy` and consumers fall through to terminal-mode lookup transparently.

### 3.2 From ~/ref (terminal mode)

Use for fresh deployments where `fhir_indicators` is empty:

```bash
python -m mirobody.indicator embeddings --from-ref
```

Phase 1 parses ~/ref (SNOMED + LOINC + RxNorm + DCM, ~677K concepts) and writes `out/fhir_ref_texts.csv`. Phase 2 calls the Gemini embedding API in batches of 256, resumable via memmap partials. Display names are filled inline (no separate `code-names` step). **No** `fhir_id_map.npy` — there is no DB pk to bridge, so upstream code (the part that writes `th_series_data.fhir_id`) **must** populate that column with `code_to_fhir_id(system, code)` directly.

### 3.3 Recovery utilities

```bash
# Rebuild only the id_map (cheap: SELECT id/standard/code, no vectors).
# Requires fhir_embeddings.npy to already exist — id_map's row order
# is defined by it. Reads canonical from arr['fhir_id'], queries DB,
# writes db_pks[r] aligned per row.
python -m mirobody.indicator id-map

# Migrate legacy 4-file artifacts (fhir_embedding_ids.npy +
# fhir_code_index.csv.gz + fhir_embedding_names.csv.gz + old npy) into
# the new structured layout. No DB / no Gemini calls — the legacy
# fhir_embedding_ids.npy is already row-aligned DB pks, so it doubles
# as the new fhir_id_map.npy after a dtype change.
python -m mirobody.indicator.fhir.embeddings.migrate
```

### 3.4 Backup safety

Re-running `embeddings` keeps the previous emb npy as `<name>.bak` (e.g. `fhir_embeddings.npy.bak` for gemini, `fhir_embeddings_qwen.npy.bak` for qwen). Exactly one prior version per provider is retained — each successful run atomically overwrites the older `.bak`. Recover via:

```bash
mv mirobody/res/fhir_embeddings.npy.bak mirobody/res/fhir_embeddings.npy
```

`fhir_meta.csv.gz` and `fhir_id_map.npy` use plain atomic replace; both are cheap (seconds to minutes) to regenerate.

### 3.5 External bundle directory (deployment)

#### Distribution matrix

| File | Size | pip wheel | Git LFS | GitHub Releases | Required by |
|---|---:|:-:|:-:|:-:|---|
| `fhir_concept_graph.bin` | ~9 MB | ✓ | ✓ | — | `FhirAdapter.expand` (search) |
| `fhir_taxonomy.bin` | ~180 KB | ✓ | ✓ | — | `Taxonomy.get` (FHIR API category view) |
| `fhir_embeddings.npy` (or `_<provider>.npy`) | 1.4 GB each | ✗ | ✗ (gitignored) | ✓ | `FhirAdapter.search` / `FhirAdapter.resolve` local path |
| `fhir_id_map.npy` | 5.4 MB | ✗ | ✓ | ✓ | `FhirAdapter.search` local path in compat mode |
| `fhir_meta.csv.gz` | 6.9 MB | ✗ | ✓ | ✓ | `FhirAdapter.resolve` (display names) |

`pyproject.toml` package-data only matches `**/*.bin` under `mirobody/res/`, so `pip install` ships exactly the two `.bin` files. The `.npy` / `.csv.gz` trio is fetched out-of-band:

- **Search-only deployment.** Two `.bin` files are enough — `FhirAdapter.search` falls back to pgvector on `fhir_indicators` when `fhir_embeddings.npy` is absent, no behavioural difference except DB hit + latency.
- **Offline / fast deployment.** Need all three `.npy` / `.csv.gz` files in the same directory. Mount them on a virtual disk and set `FHIR_INDICATORS_DIR` (see below).
- **Resolve-only deployment.** Same as offline — `fhir_meta.csv.gz` is **mandatory** for `ResolveResult.name` to populate; without it, resolve silently returns `name=""`.

#### Mounting an external bundle

The 1.4 GB `fhir_embeddings.npy` is too large for the pip wheel and Git LFS quota. In container deployments, host the bundle on a virtual disk and point the application at it via the `FHIR_INDICATORS_DIR` config. The three artifacts must stay co-located (row-aligned) — a release tarball with all three goes to the mount as a unit.

**Resolution** (caller responsibility — `local.py` itself does not read app config):

1. Caller passes `bundle_dir` to `FhirAdapter(bundle_dir=...)`. CLI / service code reads `FHIR_INDICATORS_DIR` via `safe_read_cfg` and forwards the value:

   ```python
   from mirobody.utils import safe_read_cfg
   from mirobody.indicator.fhir.search import FhirAdapter

   bundle_dir = safe_read_cfg("FHIR_INDICATORS_DIR")  # None if unset
   adapter = FhirAdapter(bundle_dir=bundle_dir)
   ```

2. `_resolve_bundle_dir()` validates `bundle_dir` by checking that `fhir_embeddings.npy` (`local.py::EMB_BASENAME`) exists in it. If yes → use it. If no (or `bundle_dir is None`) → fall back to `mirobody/res/` and log a warning.

   An explicit `bundle_dir` that fails validation does **not** then re-check `FHIR_INDICATORS_DIR` — explicit caller intent isn't quietly redirected to ambient config (mirrors `ConceptGraph.get`'s "explicit path → bundled fallback" model).

**Path-keyed cache.** `load(bundle_dir=...)` keys its singleton on the resolved path, so multiple `FhirAdapter` instances pinned to different bundles each get their own cache (~200 MB of Python heap each, plus a shared mmap). Reuse the same adapter instance for the same path + provider; different paths with the same physical file still get separate dict copies.

**Note.** `fhir_concept_graph.bin` is small (~9 MB) and stays bundled in the pip wheel under `mirobody/res/`. `FhirAdapter` looks under ``bundle_dir`` first then falls back to the bundled location, so external mounts can ship a custom graph if they want, but the default deployment doesn't need to.

## Step 4: Search and resolve (optional)

Two retrieval CLIs are available — see "Two retrieval modes" at the top for the conceptual difference.

```bash
# Per-user search: ranked indicators from this user's data, with graph expansion
python -m mirobody.indicator search <user_id> <keywords...> [--start-time YYYY-MM-DD] [--end-time YYYY-MM-DD]

# Global resolve: free text → standard codes (top_k per system, sorted by score)
python -m mirobody.indicator resolve "blood glucose"
python -m mirobody.indicator resolve "metformin" --systems LOINC RXNORM --top-k 3

# Bulk resolve: positional terms or --input FILE (one term per line, avoids ARG_MAX).
# With --output FILE, results stream as JSON Lines and the run is resumable —
# re-running skips terms already present in the output file.
python -m mirobody.indicator resolve "blood glucose" "metformin" "chest x-ray"
python -m mirobody.indicator resolve --input terms.txt --output results.jsonl -k 1 -s LOINC SNOMED_CT

# Unit normalization: free-text "value + unit" → (comparator, value, canonical UCUM, family)
# Pure local — no DB, no embedding API. See "Unit normalization" section above.
python -m mirobody.indicator normalize "90次每分钟" "<5.6 mg/dL" "600步"
python -m mirobody.indicator normalize --input units.txt
```

`search` requires DB access (for FHIR vector recall) and a built `fhir_concept_graph.bin`. `resolve` runs offline if the embedding bundle is mounted (see Step 3); otherwise it falls back to pgvector on `fhir_indicators`. `normalize` is fully offline — only reads the in-package `tokens.py` / `families.py`.

## Runtime

At search time, `FhirAdapter.expand(top_ids)` lazy-loads `fhir_concept_graph.bin` via `ConceptGraph.get(path)` and caches it path-keyed. The graph provides:

- `bridge_neighbors(fhir_id)` -- cross-vocabulary fhir_ids
- `sibling_neighbors(fhir_id, max_per_id=50)` -- same-vocabulary fhir_ids (smaller groups first)
- `neighbors(fhir_id)` -- union of both

The `FhirAdapter` in `fhir/search.py` handles all database queries (FHIR vector recall, non-FHIR recall, global resolve) and the graph-based expansion. The domain-agnostic engine in `search.py` only knows the `DomainAdapter.search` / `expand` / `fetch` / `resolve` interface — adding a new domain means subclassing those, not touching the engine.

## Data Attribution

- **LOINC** -- Copyright Regenstrief Institute, Inc. Licensed under the [LOINC License](https://loinc.org/license/).
- **SNOMED CT** -- Registered trademark of SNOMED International. US Edition via [NLM](https://www.nlm.nih.gov/healthit/snomedct/snomed_licensing.html).
- **UMLS / RxNorm** -- U.S. National Library of Medicine. [UMLS License](https://uts.nlm.nih.gov/uts/license.html).
- **WHO ATC / CDC CVX** -- WHO Collaborating Centre for Drug Statistics Methodology (ATC) and U.S. CDC (CVX). Used via RxNorm/UMLS mappings.
