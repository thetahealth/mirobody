# mirobody.indicator

Extensible indicator search engine with domain-specific adapters and graph-based concept expansion.

## Architecture

```
indicator/
  health/                # Health domain implementation
    search.py            # HealthAdapter: FHIR tables, th_series_data, etc.
    graph_builder.py     # Build fhir_id graph binary from bridge/sibling CSVs
    bridge.py            # Cross-vocabulary bridge files
    siblings.py          # Same-system sibling groups
    merge.py             # Merge pipeline + trigger graph build
    common.py            # Shared types, constants, RRF reader
    test.py              # Verify output against known test cases
    locales/             # Locale plugins for local drug/vaccine names
  concept_graph.py       # ConceptGraphBuilder ABC + ConceptGraph (load/save/query)
  search.py              # DomainAdapter ABC + domain-agnostic search engine
  mapping.py             # Free-text → LOINC/RxNorm/SNOMED via embeddings
  main.py                # CLI entry point
```

## How search works

1. **Embed** -- Gemini embeddings (1024-dim) for user keywords
2. **Vector recall** -- adapter queries domain tables via pgvector cosine distance
3. **Graph expansion** -- concept graph expands top-K IDs via bridges + siblings
4. **Merge & rank** -- combine results, filter by score threshold, sort descending

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
| `ConceptGraph.get(file_path)` | Load binary and return cached graph instance |
| `graph.bridge_neighbors(id)` | Cross-system neighbor IDs |
| `graph.sibling_neighbors(id, max_per_id=50)` | Same-system neighbor IDs (smaller groups first) |
| `graph.neighbors(id)` | All neighbors (bridge + sibling) |
| `graph.stats()` | Dict with bridge/sibling counts |

### Typical pipeline usage

```python
from mirobody.indicator.concept_graph import GRAPH_BIN
from .health.graph_builder import HealthGraphBuilder, sync_fhir_ids

await sync_fhir_ids(out_dir)           # domain-specific pre-step
builder = HealthGraphBuilder()
builder.build(out_dir)                  # load bridges + siblings, serialize to binary
```

## Adding a new domain

1. Create `<domain>/search.py` with a `DomainAdapter` subclass (set `domain = "<name>"` for auto-registration)
2. Create `<domain>/graph_builder.py` implementing `ConceptGraphBuilder`
3. Add pipeline scripts (`siblings.py`, `bridge.py`, `merge.py`, etc.) under `<domain>/`
4. Import the adapter in `cmd_search` to trigger registration, then call `_search(adapter, keywords=..., user_id=...)`

## Domain guides

- [health/README.md](health/README.md) -- Health indicator pipeline (data preparation, build steps, output files)
