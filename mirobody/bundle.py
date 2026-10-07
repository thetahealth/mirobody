"""Read the shipped terminology bundle: the stable surface for build-time tools.

``mirobody.resolve`` answers "which LOINC code is this term". A different job (
generating a seed table, a corpus, or an embedding index from LOINC) needs the
axis table itself (``COMPONENT``, ``PROPERTY``, ``SYSTEM``,
``LONG_COMMON_NAME``) and the alias sources in their precedence order. That is
what this module exposes.

Until now the only route was :mod:`mirobody._bundle`, whose leading underscore
declares it internal and free to move. The helpers there were already the right
ones: a consumer that avoided them re-implemented a tar read and a blob decode
against a private on-disk format instead, which is strictly worse. So they are
re-exported here under a name that carries a stability promise, and the
underscore module stays free to change shape behind it.

    from mirobody.bundle import load_axis, bundle_version

    axis, by_code, by_name = load_axis()
    assert bundle_version().startswith("loinc-2.83+")

Importing this does **not** load the resolver: ``import mirobody`` stays lazy
(PEP 562) and nothing here touches the runtime index. It is also deliberately
absent from ``mirobody.__all__``, so the runtime surface is unchanged by its
existence.

Everything here works from a plain ``pip install``
--------------------------------------------------

An installed package carries the same bundle members a checkout does:
``scripts/build_backend.py`` repacks the copy that goes into a wheel down to the
runtime members, which since 1.5.0 are all of them, and
``scripts/check_wheel_data.py`` enforces both directions, required members
present and build inputs absent.

* :func:`load_axis` reads ``axis_fields.bin`` / ``axis_index.npz``: one row per
  code, with LOINC_NUM, COMPONENT, PROPERTY, SCALE_TYP, SYSTEM, METHOD_TYP,
  LONG_COMMON_NAME, TIME_ASPCT and CLASS (``_bundle.AXIS_*`` name the fields).
* the alias sources are loose files under ``res/loinc/aliases_src/``, not bundle
  members, and the resolver reads them on every load, so they ship too.

What neither profile carries is the rest of the LOINC release: ``CLASSTYPE``,
``STATUS`` and every code the cut leaves out (``loinc_skip.txt`` lists the
ACTIVE ones). A tool that needs those reads the licensed release at build time
and vendors what it derives; :func:`bundle_version` is there so you can assert
the vendored artifact and the installed package came from one corpus.
"""

from __future__ import annotations

from ._bundle import (
    ALIAS_SRC_DIR,
    BUNDLE_PATH,
    RES_DIR,
    alias_source_files,
    bundle_version,
    is_lfs_pointer,
    list_members,
    load_alias_sources,
    load_axis,
    read_member,
)

__all__ = [
    "ALIAS_SRC_DIR",
    "BUNDLE_PATH",
    "RES_DIR",
    "alias_source_files",
    "bundle_version",
    "is_lfs_pointer",
    "list_members",
    "load_alias_sources",
    "load_axis",
    "read_member",
]
