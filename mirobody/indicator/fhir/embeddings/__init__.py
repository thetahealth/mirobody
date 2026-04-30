"""FHIR embedding bundle: produce, load, fix-up.

  - :mod:`db`     producer when ``fhir_indicators`` is populated (compat
                  mode — also emits the ``fhir_id_map.npy`` sidecar)
  - :mod:`ref`    producer from ~/ref source files (terminal mode — no
                  id_map; upstream must populate ``th_series_data.fhir_id``
                  with canonical packed values)
  - :mod:`local`  consumer: lazy loader + dtype/path constants
  - :mod:`names`  display-name parsers (LOINC LCN / SNOMED FSN / RxNorm
                  best-TTY / CVX) + ``code-names`` post-step CLI
  - :mod:`migrate` one-shot recovery: convert legacy 4-file artifacts to
                  the new structured-npy + meta + id_map layout
"""

from .db import build_id_map, cmd_embeddings_db, cmd_id_map
from .names import cmd_code_names
from .ref import cmd_embeddings_ref

__all__ = [
    "build_id_map",
    "cmd_code_names",
    "cmd_embeddings_db",
    "cmd_embeddings_ref",
    "cmd_id_map",
]
