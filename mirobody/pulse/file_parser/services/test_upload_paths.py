"""Where an upload is allowed to land, and what it is allowed to be.

`?folder=` on `POST /files/upload` reaches `generate_file_key` unchanged, and
`AbstractStorage._build_object_key` only strips a leading `/`. That was an
arbitrary file write outside the storage root on the local backend — reproduced
below, not inferred. S3 and OSS hide it: `../` is a literal key segment to an
object store, so the bug is invisible in exactly the deployment most people run
and live in the one a self-hoster runs.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from mirobody.pulse.file_parser.services.file_uploader import generate_file_key
from mirobody.utils.config.storage.local import LocalStorage


@pytest.mark.parametrize("folder", [
    "../secrets", "..", ".", "a/../../b", "/etc", "up\\loads",
    "uploads\x00x", "", "uploads/../..",
])
def test_a_traversing_folder_is_rejected(folder):
    with pytest.raises(ValueError):
        generate_file_key("payload.pdf", folder_prefix=folder)


@pytest.mark.parametrize("folder", ["uploads", "uploads/2026", "my.files", "a-b_c"])
def test_ordinary_folders_still_work(folder):
    key = generate_file_key("report.pdf", folder_prefix=folder)
    assert key.startswith(folder + "/") and key.endswith(".pdf")


def test_dot_is_allowed_inside_a_segment_but_not_as_one():
    """Why the check is a segment scan and not just the regex: `.` has to be a
    legal character (folders like `my.files` exist), and that alone makes `..`
    match the character class. The first version of this guard accepted
    `../secrets` for precisely this reason."""
    assert generate_file_key("x.pdf", folder_prefix="my.files")
    with pytest.raises(ValueError):
        generate_file_key("x.pdf", folder_prefix="..")


def test_the_storage_layer_refuses_to_write_outside_its_root():
    """Second boundary, independent of the first. Every read and write funnels
    through `_get_file_path`, and not every caller builds its key with
    `generate_file_key` — the agent's virtual filesystem does not."""
    root = Path(tempfile.mkdtemp())
    base = root / "storage"
    base.mkdir()
    (root / "secrets").mkdir()
    storage = LocalStorage(base_path=str(base))

    _url, err = asyncio.run(storage.put("../secrets/pwn.pdf", b"OWNED"))
    assert err and "outside the storage root" in err
    assert list((root / "secrets").glob("*")) == []


def test_a_normal_key_still_writes_and_reads_back():
    base = tempfile.mkdtemp()
    storage = LocalStorage(base_path=base)
    _url, err = asyncio.run(storage.put("uploads/ok.pdf", b"%PDF-1.4"))
    assert err is None
    content, err = asyncio.run(storage.get("uploads/ok.pdf"))
    assert err is None and content == b"%PDF-1.4"
