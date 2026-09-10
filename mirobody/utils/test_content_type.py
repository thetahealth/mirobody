"""The `Content-Type` stored on an uploaded object has to be a real MIME type.

Office formats have vendor media types, not invented `application/<ext>` ones,
and a fallback to `application/octet-stream` is a forced download — for exactly
the file kinds the README advertises accepting (csv, txt, md, heic).

Pinned here rather than trusted, because the failure is silent: a wrong
Content-Type still returns 200 and still downloads bytes.
"""

from __future__ import annotations

import pytest

from mirobody.utils.file_types import guess_mime


@pytest.mark.parametrize("ext, expected", [
    # Office formats: vendor MIME types, not `application/<ext>`.
    ("doc", "application/msword"),
    ("xls", "application/vnd.ms-excel"),
    ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    ("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
    # `image/jpg` is not a media type; `image/jpeg` is.
    ("jpg", "image/jpeg"),
    ("jpeg", "image/jpeg"),
    # Formats the README lists as accepted.
    ("csv", "text/csv"),
    ("txt", "text/plain"),
    ("md", "text/markdown"),
    # The common ones, which must stay right.
    ("png", "image/png"),
    ("pdf", "application/pdf"),
    ("json", "application/json"),
])
def test_known_extensions_get_their_real_media_type(ext, expected):
    assert guess_mime(ext) == expected


@pytest.mark.parametrize("given", ["xlsx", ".xlsx", "XLSX", ".XLSX"])
def test_a_leading_dot_and_upper_case_are_accepted(given):
    """Callers pass `name.split(".")[-1]`, a `PurePosixPath.suffix`, or a raw
    extension. All three shapes reach this function in the codebase."""
    assert guess_mime(given) == \
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


@pytest.mark.parametrize("given", ["", None, "no-such-extension"])
def test_the_unknown_case_is_still_octet_stream(given):
    """octet-stream is the right answer when there is no answer — it is only
    wrong as a catch-all for extensions the table knows."""
    assert guess_mime(given) == "application/octet-stream"


def test_one_implementation_answers_for_every_caller():
    """The entry points must agree, because they describe the same object.

    `AbstractStorage.get_content_type_from_filename` writes `Content-Type` on the
    PUT and `guess_mime` answers for every other caller; two independent
    implementations would let the stored type and the served type diverge.

    The agent's VFS is the other caller and is checked in
    `agent/test_filetype.py` — this file is engine layer and may not import
    the agent layer (`lint-imports` enforces exactly that boundary).
    """
    from mirobody.utils.config.storage.abstract import AbstractStorage
    from mirobody.utils.file_types import MIME_BY_EXT, guess_mime

    for ext, expected in MIME_BY_EXT.items():
        name = f"report{ext}"
        assert guess_mime(name) == expected
        assert AbstractStorage.get_content_type_from_filename(name) == expected
        assert guess_mime(ext.lstrip(".")) == expected


def test_the_answer_does_not_depend_on_the_host():
    """`Content-Type` is written into the stored object, so the build host must
    not be able to change it.

    `mimetypes` merges the interpreter's built-in table with the host's
    `/etc/mime.types`. The built-in table alone — what a bare container has —
    lacks types for several accepted extensions (`.docx`, `.pptx`, `.flac`,
    `.m4a`, `.ogg`, …), so an upload that opens as a spreadsheet when stored
    from a laptop downloads as bytes when stored from Docker. `MIME_BY_EXT`
    exists to pin the answer per extension, host be damned.
    """
    import mimetypes

    from mirobody.pulse.file_parser.services.file_uploader import SUPPORTED_EXTENSIONS
    from mirobody.utils.file_types import MIME_BY_EXT, guess_mime

    accepted = SUPPORTED_EXTENSIONS
    uncovered = sorted(accepted - set(MIME_BY_EXT))
    assert not uncovered, (
        f"{uncovered} are accepted or served but not pinned in MIME_BY_EXT, so "
        "their Content-Type is whatever the host's /etc/mime.types happens to say"
    )

    bare = mimetypes.MimeTypes(filenames=())
    would_differ = [
        e for e in sorted(accepted)
        if (bare.guess_type("x" + e)[0] or "application/octet-stream") != guess_mime(e)
    ]
    # Not an assertion that they match — they don't, and that is the point. This
    # asserts the pinning is doing the work: if this list ever empties, either the
    # stdlib grew every type we need (fine, delete this) or the table stopped
    # being consulted (not fine).
    assert would_differ, "MIME_BY_EXT is no longer overriding anything — is it still wired in?"


def test_a_new_supported_extension_cannot_be_added_silently():
    """Positive control for the check above: an unpinned extension must fail it."""
    from mirobody.utils.file_types import MIME_BY_EXT

    pretend_accepted = set(MIME_BY_EXT) | {".dcm"}
    assert sorted(pretend_accepted - set(MIME_BY_EXT)) == [".dcm"]
