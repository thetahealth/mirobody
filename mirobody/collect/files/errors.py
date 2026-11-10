"""What the person who uploaded a file is told when processing it failed.

The reason travels with the upload, to the client and to the file row's
`error`: "Image processing failed" alone sent the reporter of #68 into the
server logs for a cause that was one sentence long. But an exception's
message is not a sentence written for anyone: a database driver's quotes the
statement and its parameters, a vendor's can echo the request. So only the
errors written for the person keep their message.

The row also keeps the reason as phrases (`error_phrases`), each a bundle key
and its parameters or a literal text, and the files list says them in the
language of the page reading it. Text written at upload time stayed in the
upload's language after the page switched, and an English page in a Chinese
browser was answered in Chinese (#275).
"""

from __future__ import annotations

from typing import Any

#: A reason, phrase by phrase: `{"key": ..., "params": {...}, "module": ...}`
#: for an entry of `utils/locales/<module>.json` (module `file_processor`
#: unless named), or `{"text": ...}` for a sentence with no translation.
Phrases = list[dict[str, Any]]


class UploadError(Exception):
    """A failure whose message is written for the person who uploaded the
    file: what is wrong with it, or how to fix the deployment, never its
    contents. `phrases` is the same reason for the row, when it has them."""

    def __init__(self, message: str, phrases: Phrases | None = None) -> None:
        super().__init__(message)
        self.phrases = phrases

    @classmethod
    def phrase(cls, key: str, module: str = "file_processor", **params: Any) -> UploadError:
        """The error a bundle entry states, in the request's language."""
        phrases = [{"key": key, "module": module, "params": params}]
        return cls(say(phrases), phrases)


def reason_phrases(e: BaseException) -> Phrases:
    """`e`'s reason as phrases: translated where it carries a locale key (a
    local model still loading or failed to load, a vision model that read
    nothing, a provider that sent back an empty response), its own message
    where it was written for the person (`UploadError`, no routable model),
    else its type: the traceback is in the server log."""
    from mirobody.utils.config.llm import NoProviderError
    from mirobody.utils.config.served import ModelNotReady
    from mirobody.utils.llm import ImageNotRead, NoResponse

    if isinstance(e, ModelNotReady | ImageNotRead | NoResponse):
        return [{"key": e.locale_key, "params": dict(e.locale_params)}]
    if isinstance(e, UploadError) and e.phrases:
        return list(e.phrases)
    if isinstance(e, UploadError | NoProviderError):
        return [{"text": str(e)}]
    return [{"text": type(e).__name__}]


def say(phrases: Phrases, language: str | None = None) -> str:
    """`phrases` in `language` (the request's by default), joined as
    "what: why: why"."""
    from mirobody.utils.i18n import localize
    from mirobody.utils.req_ctx import request_language

    language = language or request_language()
    texts = [p["text"] if "text" in p else
             localize(p["key"], language, p.get("module") or "file_processor", **(p.get("params") or {}))
             for p in phrases]
    said = texts[-1]
    for text in reversed(texts[:-1]):
        said = localize("with_reason", language, "file_processor", what=text, reason=said)
    return said


def failure_reason(e: BaseException) -> str:
    """`reason_phrases(e)` said in the request's language."""
    return say(reason_phrases(e))


def stored_reason(content: dict[str, Any], language: str) -> str:
    """What a failed file row says in `language`: its phrases when it has
    them, else the text written at the time (rows from before phrases)."""
    phrases = content.get("error_phrases")
    if isinstance(phrases, list) and phrases:
        try:
            return say(phrases, language)
        except (AttributeError, KeyError, TypeError):
            pass  # a row is data; its text still says something
    return content.get("error", "")
