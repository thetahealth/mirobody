# Rebuilding the terminology bundles

`mirobody/res/` holds the shipped LOINC corpus, the concept graph and the alias
tables. This is how they are made, from downloading the raw releases to
producing the `fhir_concept_graph.bin` the search service reads. The graph
cross-links concepts across LOINC, SNOMED CT, RxNorm, DCM and CVX — the code
systems FHIR recognises.

**You need this only to regenerate the bundles.** Consuming them needs nothing
from here: `pip install mirobody` ships the built artifacts and
`mirobody.engine.resolve` reads them. The passes below need
`pip install 'mirobody[indicator-build]'` and, for the reference data, a free
UMLS licence from the NLM.

The driver is [`scripts/vocabulary_build.py`](../scripts/vocabulary_build.py),
beside the three sibling passes that rebuild the same artifacts. The passes it
calls live in `mirobody/indicator/fhir/embeddings/` and
`mirobody/indicator/fhir/resolve/`, both pruned from the wheel and the sdist
(`scripts/build_backend.py::_BUILD_ONLY_CODE`) — 19,000 lines nobody who
installs the package can run, so they stay in git and out of the artifact. What the package itself is, and how retrieval works at runtime,
is [`mirobody/indicator/README.md`](../mirobody/indicator/README.md).

---

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

2. Add the corresponding CLI arg (e.g. `--jp-catalog`) in `scripts/vocabulary_build.py` under the `siblings` subparser. The plugin is auto-discovered — no changes needed in `siblings.py`.

## Step 2: Build the knowledge graph

The pipeline has three stages that must be run in order. Each stage reads from `~/ref/` and writes to the output directory (default: `out/` relative to the package).

### 2.1 Build sibling groups

Sibling groups cluster codes that are semantically related within the same vocabulary system.

```bash
# Basic (no locale enrichment)
python scripts/vocabulary_build.py siblings

# With Chinese drug names
python scripts/vocabulary_build.py siblings --nhsa-catalog ~/ref/medicine_data.json
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
python scripts/vocabulary_build.py bridge
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
python scripts/vocabulary_build.py merge
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
python scripts/vocabulary_build.py embed          # both tables
python scripts/vocabulary_build.py embed series   # th_series_dim only
python scripts/vocabulary_build.py embed fhir     # fhir_indicators only
```

Processes rows in batches of 100, skipping rows where `embedding_gemini` is already set. For `fhir_indicators`, only rows with a non-NULL `llm_description` are embedded.

### 2.5 Inspect (optional)

```bash
python scripts/vocabulary_build.py inspect --system LOINC --code 718-7
```

Shows the concept-graph bridges and siblings recorded for one `(SYSTEM, CODE)`
node — the quickest way to confirm a merge produced what you expected.

(There was a `test` subcommand documented here that does not exist: running it
exits with `invalid choice: 'test'`. The resolver's actual regression gate is
`pytest mirobody/test_engine_coverage.py`, the 197-case benchmark the README
headline number comes from.)

### 2.6 All-in-one

```bash
python scripts/vocabulary_build.py siblings [--nhsa-catalog ~/ref/medicine_data.json]
python scripts/vocabulary_build.py bridge
python scripts/vocabulary_build.py merge
python scripts/vocabulary_build.py embed
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
| `fhir_id_map.npy` | `(N,)` int64 — `db_pks[r]` is the `fhir_indicators.id` for embedding row `r` | optional, and **no longer in this repo**: it maps to one database's PRIMARY KEYS, so it is meaningless in any other deployment. Regenerate with `indicator id-map` against your own `fhir_indicators`. |

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
python scripts/vocabulary_build.py embeddings --from-db    # writes all three artifacts
python scripts/vocabulary_build.py code-names              # fills name column from ~/ref
```

`embeddings --from-db` streams `fhir_indicators` rows with the active provider's embedding column set (`embedding_gemini` / `embedding_qwen3`, selected via `EMBEDDING_PROVIDER` through `resolve_fhir_embedding_column`) in a **single pass** that produces all three artifacts at once: each fetched row contributes its embedding (→ npy `emb`), canonical fhir_id (→ npy `fhir_id`), DB pk (→ id_map `db_pks[r]`), and original code string for hash rows (→ meta `code_str`).

Embedding download is checkpoint-resumable via memmap partials + `progress.json` in `out/` (handles Ctrl-C / DB disconnects across hours).

`code-names` is a separate post-step because the DB query carries no display-name column — names come from ~/ref's LOINC LCN, SNOMED FSN, RxNorm best-TTY, CVX full-name, and DCM Annex D Code Meanings.

After `th_series_data.fhir_id` is backfilled to canonical (terminal mode), delete `fhir_id_map.npy` and consumers fall through to terminal-mode lookup transparently.

### 3.2 From ~/ref (terminal mode)

Use for fresh deployments where `fhir_indicators` is empty:

```bash
python scripts/vocabulary_build.py embeddings --from-ref
```

Phase 1 parses ~/ref (SNOMED + LOINC + RxNorm + DCM, ~677K concepts) and writes `out/fhir_ref_texts.csv`. Phase 2 calls the embedding API of the configured `EMBEDDING_PROVIDER` (default: openrouter — this sentence used to hardcode "the Gemini embedding API", which `ref.py` itself no longer does), resumable via memmap partials. Display names are filled inline (no separate `code-names` step). **No** `fhir_id_map.npy` — there is no DB pk to bridge, so upstream code (the part that writes `th_series_data.fhir_id`) **must** populate that column with `code_to_fhir_id(system, code)` directly.

### 3.3 Recovery utilities

```bash
# Rebuild only the id_map (cheap: SELECT id/standard/code, no vectors).
# Requires fhir_embeddings.npy to already exist — id_map's row order
# is defined by it. Reads canonical from arr['fhir_id'], queries DB,
# writes db_pks[r] aligned per row.
python scripts/vocabulary_build.py id-map

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

| File | Size | pip wheel | Git LFS | Required by |
|---|---:|:-:|:-:|---|
| `fhir_loinc_bundle.tar.gz` | 15.5 MB | ✓ | ✓ | `engine.resolve` — alias index, axis table, commonness prior |
| `fhir_meta.csv.gz` | 6.9 MB | ✓ | ✓ | `engine.resolve` — the 677k-name corpus the index points into |
| `aliases_src/*.tsv` | 1.9 MB | ✓ | — | `engine.resolve` — ~48k multilingual alias rows |
| `resolver_overrides.tsv` | 20 KB | ✓ | — | `engine.resolve` — corrections and deliberate non-answers |
| `fhir_concept_graph.bin` | 22.5 MB | ✗ | ✓ | `FhirAdapter.expand`, and the build tooling in this package |
| `fhir_snomed_ct_bundle.tar.gz` | 140 KB | ✗ | ✓ | the v2 pipeline's body-structure mask |
| `fhir_embeddings.npy` | 198 MB (LOINC-only) – 1.4 GB (full corpus) | ✗ | ✗ | the semantic tier; build it with `scripts/build_loinc_embeddings.py` |
| `fhir_id_map.npy` | 5.4 MB | ✗ | ✗ | **not in this repo** — see below |

The three ✗-in-wheel `.bin`/`.tar.gz` files are pruned by
`scripts/build_backend.py::_BUILD_ONLY_DATA`, and
`scripts/check_wheel_data.py` fails the build if any of them reappears — or if
any of the four ✓ files goes missing. Both directions are gated, because both
have gone wrong: release 1.0.62 shipped LFS pointer stubs for the ✓ files, and
every release before this one shipped 28 MB of the ✗ files that nothing at
runtime reads.

`fhir_id_map.npy` maps canonical ids to `fhir_indicators.id` — **one database's
primary keys**. It is meaningless in any other deployment and was deleted rather
than merely unshipped; regenerate your own with `indicator id-map` if you are
running in compat mode.


- **Search-only deployment.** Two `.bin` files are enough — `FhirAdapter.search` falls back to pgvector on `fhir_indicators` when `fhir_embeddings.npy` is absent, no behavioural difference except DB hit + latency.
- **Offline / fast deployment.** Needs `fhir_embeddings.npy` and `fhir_meta.csv.gz` in the same directory (plus `fhir_id_map.npy` in compat mode, which you regenerate). Mount them on a virtual disk and set `FHIR_INDICATORS_DIR` (see below).
- **Resolve-only deployment.** Same as offline — `fhir_meta.csv.gz` is **mandatory** for `ResolveResult.name` to populate; without it, resolve silently returns `name=""`.

#### Mounting an external bundle

The 1.4 GB `fhir_embeddings.npy` is too large for the pip wheel and Git LFS quota. In container deployments, host the bundle on a virtual disk and point the application at it via the `FHIR_INDICATORS_DIR` config. The three artifacts must stay co-located (row-aligned) — a release tarball with all three goes to the mount as a unit.

**Resolution** (caller responsibility — `local.py` itself does not read app config):

1. Caller passes `bundle_dir` to `FhirAdapter(bundle_dir=...)`. CLI / service code reads `FHIR_INDICATORS_DIR` via `safe_read_cfg` and forwards the value:

   ```python
   from mirobody.utils import safe_read_cfg
   from mirobody.indicator.fhir.adapter import FhirAdapter

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
python scripts/vocabulary_build.py search <user_id> <keywords...> [--start-time YYYY-MM-DD] [--end-time YYYY-MM-DD]

# Global resolve: free text → standard codes (top_k per system, sorted by score)
python scripts/vocabulary_build.py resolve "blood glucose"
python scripts/vocabulary_build.py resolve "metformin" --systems LOINC RXNORM --top-k 3

# Bulk resolve: positional terms or --input FILE (one term per line, avoids ARG_MAX).
# With --output FILE, results stream as JSON Lines and the run is resumable —
# re-running skips terms already present in the output file.
python scripts/vocabulary_build.py resolve "blood glucose" "metformin" "chest x-ray"
python scripts/vocabulary_build.py resolve --input terms.txt --output results.jsonl -k 1 -s LOINC SNOMED_CT

# Unit normalization: free-text "value + unit" → (comparator, value, canonical UCUM, family)
# Pure local — no DB, no embedding API. See "Unit normalization" section above.
python scripts/vocabulary_build.py normalize "90次每分钟" "<5.6 mg/dL" "600步"
python scripts/vocabulary_build.py normalize --input units.txt
```

`search` requires DB access (for FHIR vector recall) and a built `fhir_concept_graph.bin`. `resolve` runs offline if the embedding bundle is mounted (see Step 3); otherwise it falls back to pgvector on `fhir_indicators`. `normalize` is fully offline — only reads the in-package `tokens.py` / `families.py`.

