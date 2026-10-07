"""What the person who uploaded a file is told when processing it failed.

The reason travels with the upload, to the client and to the file row's
`error`: "Image processing failed" alone sent the reporter of #68 into the
server logs for a cause that was one sentence long. But an exception's
message is not a sentence written for anyone: a database driver's quotes the
statement and its parameters, a vendor's can echo the request. So only the
errors written for the person keep their message.
"""

from __future__ import annotations


class UploadError(Exception):
    """A failure whose message is written for the person who uploaded the
    file: what is wrong with it, or how to fix the deployment, never its
    contents."""


def failure_reason(e: BaseException) -> str:
    """The message of an error written for the person who uploaded the file
    (`UploadError`, no routable model, a vision model that read nothing),
    else the error's type: the traceback is in the server log."""
    from mirobody.utils.config.llm import NoProviderError
    from mirobody.utils.llm import ImageNotRead

    if isinstance(e, UploadError | NoProviderError | ImageNotRead):
        return str(e)
    return type(e).__name__
