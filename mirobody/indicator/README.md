# mirobody.indicator

Extensible indicator search engine with domain-specific adapters and graph-based concept expansion.

## Architecture

```
indicator/
  fhir/                  the FHIR-vocabulary domain
    adapter.py           FhirAdapter — vector recall, graph expansion, resolve
    common.py            SYSTEMS, code_to_fhir_id, FHIR_GRAPH_BIN, the masks
    index.py             READ the embedding index and its derived masks
    loinc_lookups.py     generated: LOINC Part names in 22 locales (no reader)
    locales/             local drug and vaccine names (zh)
    bridge.py            build cross-vocabulary bridge files          ┐
    siblings.py          build same-system sibling groups             │ build
    merge.py             merge, then trigger the graph build          │ only —
    graph_builder.py     write the fhir_id graph binary               │ pruned
    inspect.py           show a node's bridges and siblings           │ from
    embeddings/          WRITE mirobody/res/fhir_* from raw releases  │ the
    resolve/             the v2 semantic pipeline (~200 MB matrix)    │ artifact
  concept_graph.py       ConceptGraphBuilder ABC + ConceptGraph (load/save/query)
  search.py              DomainAdapter ABC + the search/resolve engines
  semantic.py            the cosine tier over the embedding index (off by default)
  resolve.py             the v2 pipeline's subcommand body            │
  embed.py               batch-fill an embedding column for DB tables ┘
```

The CLI that drives the build passes is
[`scripts/vocabulary_build.py`](../../scripts/vocabulary_build.py), not a
module here. It was `python -m mirobody.indicator`, which made the entry point
of a build toolchain into library code: it shipped in every wheel while
importing modules the wheel prunes, so the documented command raised
`ModuleNotFoundError` on any published install. Each subcommand now imports its
own pass on dispatch, so `--help` and the offline `normalize` work without the
build dependencies and a missing one is reported against the pass that needs it.

**Reads ship; writes do not.** `index.py` is the reader of the embedding
index; `embeddings/` is the twelve passes that mint what it reads. They used
to be one directory, so a `pip install` shipped `adapter.py` — the class
`pulse/query.py` reaches for on every semantic lookup — while pruning the
module it imports, and the one caller swallowed the `ModuleNotFoundError` and
fell back to lexical recall without saying so. The same split is recorded in
`mirobody/_bundle.py` for the tarball reader; this is the module it stopped
one short of. Everything the artifact still carries can be imported from an
install: `scripts/check_wheel_data.py` is the gate.

The vocabulary layer a `pip install` actually resolves with —
`mirobody/engine.py`, `mirobody/lexical.py`, `mirobody/units/`,
`mirobody/value_scale.py`, `mirobody/zh_fold.py` — sits at the package root,
not here. It moved there in 1.3.0 so that the modules a bare install can use
are next to the front door rather than inside the tree that is pruned from the
wheel. This package is what stands behind them: the corpus build, and the
semantic tier that is opt-in.

Rebuilding the bundles is [docs/vocabulary-build.md](../../docs/vocabulary-build.md),
which is where the four-step manual that used to be the second half of this
file now lives — it needs UMLS, SNOMED CT, LOINC and RxNorm licences, so it is
contributor documentation, not something to ship in everyone's site-packages.

**Runtime vs build, in one package on purpose.** Roughly 6.7k of these lines are
what `mirobody.engine` and the agent actually import; the rest is the corpus
pipeline that produces `mirobody/res/`. Splitting the two into separate trees was
tried and reverted: it moved 1.4% of the wheel (the artifact is 24 MB of LOINC
data, so the Python is noise) while putting the package's duplicated pairs on
opposite sides of a boundary, which makes merging them harder rather than easier.

Integrating them instead is what that produced. `taxonomy.py` was a second copy
of `concept_graph.py`'s design — same opening docstring word for word except the
noun, same builder/loader class pair, same path-keyed cache — and its READER had
zero importers: the build wrote a 183 KB `fhir_taxonomy.bin` that nothing in this
repo opened, deliberately excluded from the wheel. It is deleted, not merged.

The other pair stays and is deliberate: `fhir/adapter.py:resolve_many` is the
lite lexical path that ships, `fhir/resolve/pipeline.py:resolve_many` is the v2
semantic pipeline that needs a ~200 MB matrix which is not distributed.
`engine.py`'s module docstring says which one it is and why.

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
from mirobody.indicator.fhir.adapter import FhirAdapter

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

## Runtime

At search time, `FhirAdapter.expand(top_ids)` lazy-loads `fhir_concept_graph.bin` via `ConceptGraph.get(path)` and caches it path-keyed. The graph provides:

- `bridge_neighbors(fhir_id)` -- cross-vocabulary fhir_ids
- `sibling_neighbors(fhir_id, max_per_id=50)` -- same-vocabulary fhir_ids (smaller groups first)
- `neighbors(fhir_id)` -- union of both

The `FhirAdapter` in `fhir/adapter.py` handles all database queries (FHIR vector recall, non-FHIR recall, global resolve) and the graph-based expansion. The domain-agnostic engine in `search.py` only knows the `DomainAdapter.search` / `expand` / `fetch` / `resolve` interface — adding a new domain means subclassing those, not touching the engine.

## Data Attribution

- **LOINC** -- Copyright Regenstrief Institute, Inc. Licensed under the [LOINC License](https://loinc.org/license/).
- **SNOMED CT** -- Registered trademark of SNOMED International. US Edition via [NLM](https://www.nlm.nih.gov/healthit/snomedct/snomed_licensing.html).
- **UMLS / RxNorm** -- U.S. National Library of Medicine. [UMLS License](https://uts.nlm.nih.gov/uts/license.html).
- **WHO ATC / CDC CVX** -- WHO Collaborating Centre for Drug Statistics Methodology (ATC) and U.S. CDC (CVX). Used via RxNorm/UMLS mappings.
