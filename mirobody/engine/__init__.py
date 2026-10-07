"""Parse a health document, resolve indicator names to standard codes.

Zero infrastructure: no PostgreSQL, no Redis, no server. ``resolve`` needs no
credentials at all (offline lookup against the shipped data bundles);
``parse`` needs one model key, for whichever provider ``config.llm.yaml``
routes the text surface to (:mod:`mirobody.utils.config.llm`).

    from mirobody.engine import resolve, parse_file

    resolve("血红蛋白").loinc          # -> "718-7", offline
    await parse_file("labs.pdf")       # -> readings + resolutions, one LLM call

The first two engine stages (① Collect, ② Translate) without persistence:

* **Lexical resolution** against the shipped LOINC bundle: a multilingual
  alias index of LOINC's own designations, the long name of every code the
  cut keeps, a per-row commonness prior, and the LOINC axis table each posting
  row is. Plus ``res/loinc/resolver_overrides.tsv``, hand-written corrections
  for terms the index gets wrong (measured by ``test_engine_coverage.py``).
* **Not** an embedding pipeline. A term that misses here returns
  ``unresolved``, not a guess. 1.4.x shipped an opt-in semantic tier behind a
  matrix that was never published; measured in both a wheel and a source tree,
  it loaded nothing and answered nothing, so 1.5.0 removed it.
* **Unit normalization** via :mod:`mirobody.units` (offline).

The candidate picker is a heuristic: commonness prior, then a preference for
plain Serum/Plasma/Blood variants over cord/capillary specials. It gives one
default answer; ``candidates`` carries the match count so callers can tell
when a term was ambiguous.
"""

from mirobody.engine.observation import standardize_reading
from mirobody.engine.parse import Reading, parse_file, parse_text
from mirobody.engine.resolver import OfflineResolver, Resolution, UnitVerdict, get_resolver, resolve, resolve_reading

#: The stable surface. `mirobody/__init__.py` re-exports it lazily, so
#: `from mirobody import resolve` and `from mirobody.engine import resolve`
#: are the same function. Anything unlisted is internal and changes freely,
#: `OfflineResolver`'s underscore attributes especially.
__all__ = [
    "OfflineResolver",
    "Reading",
    "Resolution",
    # What `OfflineResolver.variant_for_reading` returns: public by that.
    "UnitVerdict",
    "get_resolver",
    "parse_file",
    "resolve",
    "resolve_reading",
    "standardize_reading",
]
