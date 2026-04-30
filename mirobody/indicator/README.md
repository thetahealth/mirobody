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
from .fhir.graph_builder import FhirGraphBuilder, sync_fhir_ids

await sync_fhir_ids(out_dir)           # domain-specific pre-step
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

1. Sync `code -> fhir_id` mapping from DB (`fhir_indicators` table) into `_fhir_id_codes.csv` cache.
2. Stream bridge CSVs, resolve codes to fhir_ids, build bidirectional adjacency. Jaccard bridges capped at 150 codes/row.
3. Stream sibling CSVs, resolve codes to fhir_ids, store as flat groups.
4. Serialize to zlib-compressed binary:
   - Header: magic (`CGPH`) + version (1) + counts
   - Bridges: half-edge storage (src < dst), expanded at load time
   - Siblings: per-group members + reverse index for O(1) lookup

### 2.4 Batch-fill embeddings

After importing new indicators into `fhir_indicators` or `th_series_dim`, run `embed` to compute Gemini embeddings for rows that don't have one yet:

```bash
python -m mirobody.indicator embed          # both tables
python -m mirobody.indicator embed series   # th_series_dim only
python -m mirobody.indicator embed fhir     # fhir_indicators only
```

Processes rows in batches of 100, skipping rows where `embedding_gemini` is already set. For `fhir_indicators`, only rows with a non-NULL `llm_description` are embedded.

### 2.5 Verify (optional)

```bash
python -m mirobody.indicator test
```

Checks `concepts.csv` against known medical codes defined in `test.py` (e.g. LOINC DXA codes, HbA1c, etc.).

### 2.6 All-in-one

```bash
python -m mirobody.indicator siblings [--nhsa-catalog ~/ref/medicine_data.json]
python -m mirobody.indicator bridge
python -m mirobody.indicator merge
python -m mirobody.indicator embed
python -m mirobody.indicator test
```

## Output summary

After running all three steps, the output directory contains:

```
concepts.csv                  # Final merged concept table
fhir_concept_graph.bin        # Binary graph for runtime search
_fhir_id_codes.csv            # code -> fhir_id cache
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
| `fhir_embeddings.npy` (gemini) / `fhir_embeddings_<provider>.npy` (others) | structured `(N,)` of `[fhir_id i8, emb f2[1024]]`, fp16 L2-normalised | yes |
| `fhir_meta.csv.gz` | `(N,)` rows of `name` + `code_str` (latter only for DCM/THETA hash rows) | optional (search works without; resolve `name` empty) |
| `fhir_id_map.npy` | `(N,)` int64 — `db_pks[r]` is the `fhir_indicators.id` for embedding row `r` | optional (compat mode only) |

The active provider is read from `DIM_EMBEDDING_PROVIDER` (default `gemini`). gemini keeps the unprefixed `fhir_embeddings.npy` so existing disk mounts don't need a rename; other providers (e.g. `qwen`) get a sibling `fhir_embeddings_<provider>.npy` in the same directory. `fhir_meta.csv.gz` and `fhir_id_map.npy` are **not** provider-tagged — they're row-aligned to whichever emb npy was just exported, and a fresh export overwrites them. Switching provider therefore requires re-exporting both providers' emb npys against the same `fhir_indicators` snapshot to keep all bundles row-consistent.

All three files are **row-aligned by index** to the active emb npy — the i-th meta row and the i-th id_map entry describe the same concept as `arr[i]`. Loaders abort if row counts disagree; never half-aligned.

`fhir_id` is a packed canonical id derived from `(system, code)` via `code_to_fhir_id` — **independent of** `fhir_indicators.id`. Layout: bits 60–62 = system enum index (matches `SYSTEMS` tuple in `common.py`), bits 0–59 = code int (`int(code)` for numeric vocabs, blake2b digest `>> 4` for DCM and THETA where the original string lives in `meta.code_str`). `SYSTEMS` is **append-only** because its index is bit-packed into every existing fhir_id. (The DB column keeps its legacy name `indicator_standard`; in code we use the FHIR `system` vocabulary throughout.)

### 3.1 From DB (compat mode)

Use when `fhir_indicators` is populated and `th_series_data.fhir_id` rows still key by DB pk:

```bash
python -m mirobody.indicator embeddings --from-db    # writes all three artifacts
python -m mirobody.indicator code-names              # fills name column from ~/ref
```

`embeddings --from-db` streams `fhir_indicators` rows with the active provider's embedding column set (`embedding_gemini` / `embedding_qwen3`, selected via `DIM_EMBEDDING_PROVIDER`) in a **single pass** that produces all three artifacts at once: each fetched row contributes its embedding (→ npy `emb`), canonical fhir_id (→ npy `fhir_id`), DB pk (→ id_map `db_pks[r]`), and original code string for hash rows (→ meta `code_str`).

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

2. `_resolve_bundle_dir()` validates `bundle_dir` by checking that the active provider's emb npy (per `DIM_EMBEDDING_PROVIDER`, see `emb_basename()`) exists in it. If yes → use it. If no (or `bundle_dir is None`) → fall back to `mirobody/res/` and log a warning.

   An explicit `bundle_dir` that fails validation does **not** then re-check `FHIR_INDICATORS_DIR` — explicit caller intent isn't quietly redirected to ambient config (mirrors `ConceptGraph.get`'s "explicit path → bundled fallback" model).

**Path-keyed cache.** `load(bundle_dir=...)` keys its singleton on `(resolved_path, emb_basename)`, so multiple `FhirAdapter` instances pinned to different bundles — or to different providers in the same bundle — each get their own cache (~200 MB of Python heap each, plus a shared mmap). Reuse the same adapter instance for the same path + provider; different paths with the same physical file still get separate dict copies.

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
```

`search` requires DB access (for FHIR vector recall) and a built `fhir_concept_graph.bin`. `resolve` runs offline if the embedding bundle is mounted (see Step 3); otherwise it falls back to pgvector on `fhir_indicators`.

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
