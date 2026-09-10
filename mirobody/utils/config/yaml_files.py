"""Which YAML files a run loads, and in what order.

The project's config is deliberately split, and the rules compose:

    config.yaml            the defaults, checked into git; its INCLUDE list
                           names the sibling files that load right after it
                           (config.llm.yaml, config.devices.yaml)
    config.key.yaml        its secrets, which are not
    config.{env}.yaml      per-environment overrides
    config.{env}.key.yaml  their secrets

`INCLUDE` is Home Assistant's `!include`, spelled as a plain list so any YAML
loader reads it: the file a newcomer reads holds what a newcomer needs, the
model table has its own file, and the wearable-vendor OAuth apps have theirs.
An explicit list rather than a directory scan, so a `config.prod.yaml` sitting
next to the files is never mistaken for a concern file when ENV is something
else, and the reader sees the order.

So one requested filename fans out to as many as four candidates, order
matters (later wins), and duplicates must not be loaded twice. That expansion
is pure — filenames and an env name in, an ordered list out — but it used to
live inside `Config.init`, an async staticmethod that also configures logging
and fetches remote config over the network. 100 of that method's 170 lines
were this, and none of it could be exercised without the other two thirds.

Here it is a function, and `test_yaml_files.py` covers the cases that used to
be verifiable only by starting the app: the `.key.yaml` pairing, the `{env}`
fan-out, dedup, and the "no env" and "no filenames" paths.
"""

from __future__ import annotations

import os
import re

_YAML_RE = re.compile(r".*\.yaml$", re.IGNORECASE)
_KEY_YAML_RE = re.compile(r".*\.key\.yaml$", re.IGNORECASE)

_YAML_SUFFIX_LEN = len(".yaml")
_KEY_YAML_SUFFIX_LEN = len(".key.yaml")


def _clean(names) -> list[str]:
    """Non-empty stripped strings, in order, without duplicates."""
    out: list[str] = []
    for n in names or []:
        if not isinstance(n, str):
            continue
        n = n.strip()
        if n and n not in out:
            out.append(n)
    return out


def _with_key_files(names: list[str]) -> list[str]:
    """After each `x.yaml`, its `x.key.yaml` sibling.

    This is the split that keeps secrets out of the file checked into git.
    A name that IS already a `.key.yaml` is passed through (it has no sibling
    of its own), and anything not ending in `.yaml` is dropped — the historical
    behaviour, which quietly ignores a non-YAML entry rather than failing the
    boot on it.
    """
    out: list[str] = []
    for name in names:
        if name in out:
            continue
        out.append(name)

        if _KEY_YAML_RE.match(name):
            continue
        if not _YAML_RE.match(name):
            continue
        out.append(f"{name[:-_YAML_SUFFIX_LEN]}.key.yaml")
    return out


def _with_env_files(names: list[str], env: str) -> list[str]:
    """After each name, its `{env}` variant.

    `config.yaml` -> `config.{env}.yaml`,
    `config.key.yaml` -> `config.{env}.key.yaml`.
    """
    out: list[str] = []
    for name in names:
        if name in out:
            continue
        out.append(name)

        if _KEY_YAML_RE.match(name):
            variant = f"{name[:-_KEY_YAML_SUFFIX_LEN]}.{env}.key.yaml"
        elif _YAML_RE.match(name):
            variant = f"{name[:-_YAML_SUFFIX_LEN]}.{env}.yaml"
        else:
            continue

        if variant not in out:
            out.append(variant)
    return out


def include_paths(base: str | None, includes) -> list[str]:
    """The files an `INCLUDE` list names, resolved next to the file that
    declared them (or against the working directory when the declaring
    "file" was a stream), in the order written, without duplicates. Neither
    `.key.yaml` siblings nor `{env}` variants are expanded for an included
    file — it is a plain file; environment differences go in the overlay."""
    if not isinstance(includes, list):
        return []
    directory = os.path.dirname(base) if isinstance(base, str) else ""
    out: list[str] = []
    for name in includes:
        if not isinstance(name, str) or not name.strip() or not _YAML_RE.match(name.strip()):
            continue
        path = os.path.normpath(os.path.join(directory, name.strip()))
        if path not in out:
            out.append(path)
    return out


def expand_yaml_filenames(yaml_filenames: str | list[str] | None, env: str) -> list[str]:
    """The ordered candidate list for `yaml_filenames` under `env`.

    Existence is NOT checked here — the caller filters, because one of the
    entries it adds is a remote config that has no path at all.

    With no filenames and an env, the convention applies on its own:
    `config.{env}.yaml` + `config.{env}.key.yaml`.
    """
    if isinstance(yaml_filenames, str):
        requested = [yaml_filenames]
    elif isinstance(yaml_filenames, list):
        requested = list(yaml_filenames)
    else:
        requested = []

    names = _with_key_files(_clean(requested))

    if env:
        if not names:
            return [f"config.{env}.yaml", f"config.{env}.key.yaml"]
        names = _with_env_files(names, env)

    return names
