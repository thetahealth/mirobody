"""Read the shipped terminology bundle — the stable surface for build-time tools.

``mirobody.resolve`` answers "which LOINC code is this term". A different job —
generating a seed table, a corpus, or an embedding index from LOINC — needs the
axis table itself (``COMPONENT``, ``PROPERTY``, ``SYSTEM``,
``LONG_COMMON_NAME``) and the alias sources in their precedence order. That is
what this module exposes.

Until now the only route was :mod:`mirobody._bundle`, whose leading underscore
declares it internal and free to move. The helpers there were already the right
ones — a consumer that avoided them re-implemented a tar read and a blob decode
against a private on-disk format instead, which is strictly worse. So they are
re-exported here under a name that carries a stability promise, and the
underscore module stays free to change shape behind it.

    from mirobody.bundle import load_axis, bundle_version

    axis, by_code, by_name = load_axis()
    assert bundle_version().startswith("loinc-2.82+")

Importing this does **not** load the resolver: ``import mirobody`` stays lazy
(PEP 562) and nothing here touches the runtime index. It is also deliberately
absent from ``mirobody.__all__``, so the runtime surface is unchanged by its
existence.

Everything here works from a plain ``pip install``
--------------------------------------------------

``mirobody/res/fhir_loinc_bundle.tar.gz`` is repacked on the way into a wheel —
16 members / 39.7 MB in a checkout, 9 / 24.9 MB installed — and
``scripts/check_wheel_data.py`` enforces both directions, required members
present and build inputs absent. Two things make that repack invisible here:

* :func:`load_axis` reads ``axis_fields.bin`` / ``axis_index.npz``, which are
  runtime members and ship. It does **not** parse ``loinc_axis.csv``.
* the alias sources are loose files under ``res/aliases_src/``, not bundle
  members, and the resolver reads them on every load — so they ship too.

The one thing a wheel does not carry is ``loinc_axis.csv``, and with it the
four columns the runtime table has no use for: ``TIME_ASPCT``, ``CLASS``,
``CLASSTYPE``, ``STATUS``. A tool that needs those — filtering to
``CLASSTYPE=1`` laboratory rows, or dropping ``STATUS=DEPRECATED`` — must read
that member from a checkout, and will find :func:`read_member` returns ``None``
against an installed package. Derive what you need at build time and vendor it;
:func:`bundle_version` is there so you can assert the vendored artifact and the
installed package came from one corpus. The ``VERSION`` member survives the
repack, so that identifier means the same thing in both profiles.
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
