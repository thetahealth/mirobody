# Health Indicator Pipeline

This guide walks through setting up the health indicator knowledge graph from scratch -- from downloading reference data to producing the final `concept_graph.bin` used by the search service.

## Directory structure

```
mirobody/indicator/
  health/
    search.py              # HealthAdapter: FHIR tables, th_series_data, etc.
    graph_builder.py       # Build fhir_id graph binary from bridge/sibling CSVs
    common.py              # Shared types, constants, RRF reader
    siblings.py            # LOINC + SNOMED + RxNorm sibling group builders
    bridge.py              # ICD/MRREL/Jaccard cross-vocabulary bridges
    merge.py               # Merge into concepts.csv + concept_graph.bin
    test.py                # Verify output against known test cases
    locales/               # Locale plugins for local drug/vaccine names
      __init__.py          # LocalePlugin interface + auto-discovery
      cn.py                # China: NHSA drug catalog
  concept_graph.py         # ConceptGraphBuilder ABC + ConceptGraph (load/query)
  search.py                # DomainAdapter ABC + domain-agnostic search engine
  mapping.py               # Free-text → LOINC/RxNorm/SNOMED via embeddings
  main.py                  # CLI entry point (siblings, bridge, merge, search, mapping, test)
```

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

The pipeline supports **locale plugins** (`health/locales/`) for enriching RxNorm sibling groups with local drug and vaccine names. Plugins are auto-discovered at build time — any `LocalePlugin` subclass in the `locales/` package is loaded if its required CLI arg is provided. Each locale is optional -- the pipeline runs without them.

Currently available locales:

| Locale | Plugin | Data source | CLI arg |
|--------|--------|------------|---------|
| China | `locales/cn.py` | [NHSA drug catalog](https://github.com/badman200/medicine) (`medicine_data.json`) | `--nhsa-catalog ~/ref/medicine_data.json` |

#### Adding a new locale

1. Create `health/locales/<locale>.py` implementing `LocalePlugin`:

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

**Phase 2 -- `concept_graph.bin`:**

1. Sync `code -> fhir_id` mapping from DB (`fhir_indicators` table) into `_fhir_id_codes.csv` cache.
2. Stream bridge CSVs, resolve codes to fhir_ids, build bidirectional adjacency. Jaccard bridges capped at 150 codes/row.
3. Stream sibling CSVs, resolve codes to fhir_ids, store as flat groups.
4. Serialize to zlib-compressed binary:
   - Header: magic (`CGPH`) + version (1) + counts
   - Bridges: half-edge storage (src < dst), expanded at load time
   - Siblings: per-group members + reverse index for O(1) lookup

### 2.4 Verify (optional)

```bash
python -m mirobody.indicator test
```

Checks `concepts.csv` against known medical codes defined in `test.py` (e.g. LOINC DXA codes, HbA1c, etc.).

### 2.5 All-in-one

```bash
python -m mirobody.indicator siblings [--nhsa-catalog ~/ref/medicine_data.json]
python -m mirobody.indicator bridge
python -m mirobody.indicator merge
python -m mirobody.indicator test
```

## Output summary

After running all three steps, the output directory contains:

```
concepts.csv               # Final merged concept table
concept_graph.bin          # Binary graph for runtime search
_fhir_id_codes.csv         # code -> fhir_id cache
_siblings_*.csv            # Intermediate sibling groups
_bridges_*.csv             # Intermediate bridge files
_bridged_snomed.csv        # SNOMED codes with bridges
_unbridged_loinc.csv       # LOINC groups without bridges
_siblings_skipped.csv      # Blacklisted groups
```

Files prefixed with `_` are intermediate. The runtime search service only needs `concept_graph.bin`.

## Step 3: Search (optional)

You can test the search pipeline locally via the CLI:

```bash
python -m mirobody.indicator search <user_id> <keywords...> [--start-time YYYY-MM-DD] [--end-time YYYY-MM-DD]
```

This requires DB access (for FHIR vector recall) and a built `concept_graph.bin`.

## Runtime

At search time, `ConceptGraph.get(file_path)` loads and caches `concept_graph.bin`. The graph provides:

- `bridge_neighbors(fhir_id)` -- cross-vocabulary fhir_ids
- `sibling_neighbors(fhir_id, max_per_id=50)` -- same-vocabulary fhir_ids (smaller groups first)
- `neighbors(fhir_id)` -- union of both

The `HealthAdapter` in `health/search.py` handles all database queries (FHIR vector recall, non-FHIR recall, graph expansion matching) and is called by the domain-agnostic search engine in `search.py`.

## Data Attribution

- **LOINC** -- Copyright Regenstrief Institute, Inc. Licensed under the [LOINC License](https://loinc.org/license/).
- **SNOMED CT** -- Registered trademark of SNOMED International. US Edition via [NLM](https://www.nlm.nih.gov/healthit/snomedct/snomed_licensing.html).
- **UMLS / RxNorm** -- U.S. National Library of Medicine. [UMLS License](https://uts.nlm.nih.gov/uts/license.html).
- **WHO ATC / CDC CVX** -- WHO Collaborating Centre for Drug Statistics Methodology (ATC) and U.S. CDC (CVX). Used via RxNorm/UMLS mappings.
