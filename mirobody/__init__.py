"""The package version, resolved in this order:

  1. importlib.metadata — after the package is installed (wheel or editable).
  2. MIROBODY_VERSION env var — CI exports the git tag (or a timestamped
     0.0.0.dev* test version) before `python -m build`; the isolated build
     env has no installed metadata, so this is what release wheels get.
  3. The literal below — the CANONICAL version of this source tree. It is
     what a source checkout reports and what `pip install -e .` bakes into
     its metadata (pip's isolated build sees neither 1 nor 2). Bump it with
     each release, matching the CHANGELOG's top entry and the git tag; the
     release workflow refuses a tag that disagrees, and the test suite pins
     it to the CHANGELOG and the READMEs. (It used to be a "0.0.0.dev0"
     sentinel, so every source install reported a version that identified
     nothing.)
"""

try:
    from importlib.metadata import version as _version
    __version__ = _version("mirobody")
except Exception:
    import os
    __version__ = os.environ.get("MIROBODY_VERSION") or "1.2.1"
