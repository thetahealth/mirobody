"""The VFS reads the same MIME table as object storage, not a copy of it.

A second extension→MIME table in `agent/deep/filetype` would let an upload be
stored as one content-type and served to the model as another — and the model's
branch (native file block vs extracted text vs base64) is decided from it. The
single table lives in `utils/file_types`, engine side, because object storage
and the presigned-URL helper cannot import the agent layer.

`utils/test_content_type.py` pins the values. What is pinned here is that this
module has not grown a second opinion.
"""

from __future__ import annotations

from mirobody.agent.deep import filetype
from mirobody.utils import file_types


def test_the_vfs_guesser_is_the_engine_guesser():
    assert filetype.guess_mime is file_types.guess_mime


def test_this_module_holds_no_mime_table_of_its_own():
    """By shape, not by name: any dict here mapping `.ext` to a `type/subtype`."""
    tables = [
        name for name, value in vars(filetype).items()
        if isinstance(value, dict)
        and any(str(k).startswith(".") and "/" in str(v) for k, v in value.items())
    ]
    assert not tables, (
        f"{tables} is a second extension→MIME table; the one in "
        "utils/file_types is what storage writes into the object"
    )


def test_every_multimodal_extension_has_a_pinned_type():
    """These are the extensions served as content blocks rather than text, so an
    `application/octet-stream` here is a file the model cannot open."""
    unpinned = sorted(filetype.MULTIMODAL_EXTS - set(file_types.MIME_BY_EXT))
    assert not unpinned, f"{unpinned} would fall back to the host's /etc/mime.types"


def test_multimodal_and_text_do_not_overlap():
    """A file cannot be both served as base64 and decoded as text."""
    overlap = filetype.MULTIMODAL_EXTS & file_types.TEXT_EXTENSIONS
    assert not overlap, overlap
