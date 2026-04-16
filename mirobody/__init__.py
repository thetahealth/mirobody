"""Version in YYYYMMDD.HHMMSS format.

Resolution order:
  1. importlib.metadata — works after the package is installed (prod + editable dev).
  2. MIROBODY_VERSION env var — CI sets this before `python -m build`.
  3. "0.0.0.dev0" sentinel — running from source tree without installation.
"""

try:
    from importlib.metadata import version as _version
    __version__ = _version("mirobody")
except Exception:
    import os
    __version__ = os.environ.get("MIROBODY_VERSION") or "0.0.0.dev0"
