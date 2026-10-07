"""Translated user-facing strings, one JSON file per calling module.

`localize("file_empty", language, "file_uploader")` reads
`locales/file_uploader.json`,
whose entries are `{key: {lang: text}}` in five languages (en/zh/fr/ja/es), and
falls back requested language → en → zh → the key itself. Language codes come
from the request's `Accept-Language` (`server/middlewares.py`) through the
request context, and `LANGUAGE_CODES` folds their spellings.

This used to be an `I18n` class with a global instance, a module-level wrapper
around each method and (when `module` was omitted) an `inspect` walk up the
stack to guess the calling file from its filename. Every caller now names its
module. The JSON never changes while the process runs, so the cache is a
`functools.cache` and the `clear_translation_cache` that one upload path called on
every request had nothing to clear.
"""

import json
import logging
from functools import cache
from pathlib import Path

logger = logging.getLogger(__name__)

_LOCALES_DIR = Path(__file__).with_name("locales")

LANGUAGE_CODES = {
    "zh": "zh",
    "zh-cn": "zh",
    "zh_cn": "zh",
    "zh-hans": "zh",
    "zh_hans": "zh",
    # Traditional variants map to the Simplified bundle: the zh-TW README is
    # archived, and Chinese is closer to a reader than the English default.
    "zh-tw": "zh",
    "zh_tw": "zh",
    "zh-hant": "zh",
    "zh_hant": "zh",
    "en": "en",
    "fr": "fr",
    "ja": "ja",
    "es": "es",
    "chinese": "zh",
    "english": "en",
    "french": "fr",
    "japanese": "ja",
    "spanish": "es",
}


@cache
def _translations(module: str) -> dict[str, dict[str, str]]:
    path = _LOCALES_DIR / f"{module}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Failed to load translations for %s: %s", module, e)  # phi: ok a module name and a JSON/OS error from our own locales tree
        return {}


@cache
def _missing(module: str, key: str) -> None:
    """Log, once per key, a key its bundle does not have: the person is shown
    the key itself (`excel_processing_success`), which went unnoticed while
    eleven keys the upload path used were in no bundle."""
    logger.warning("locale key missing: module=%s key=%s", module, key)  # phi: ok a key from the code, not a value


def localize(key: str, language: str, module: str, **kwargs) -> str:
    """The text for `key` in `language`, from `locales/<module>.json`.

    Named for what it does. It was `t`, which is the JavaScript convention
    (i18next, vue-i18n); Python's is gettext's `_`, and neither says anything
    at a call site. `translate` was the obvious alternative and is taken:
    `mirobody.translate` is the ② stage, and a reader seeing `translate(...)`
    in a progress message would have to check which one it is.
    """
    lang_code = LANGUAGE_CODES.get(language.lower(), "en")
    text_dict = _translations(module).get(key, {})
    if not text_dict:
        _missing(module, key)
    text = text_dict.get(lang_code) or text_dict.get("en") or text_dict.get("zh") or key
    if kwargs:
        try:
            text = text.format(**kwargs)
        except (KeyError, ValueError):
            pass  # a placeholder mismatch is the JSON's bug; the raw text still says something
    return text


#: The headers a client may state its language in, most specific first. Both
#: spellings of each: a WebSocket handshake's headers are not normalized the
#: way Starlette normalizes a request's.
_LANGUAGE_HEADERS = ("x-language", "X-Language", "accept-language", "Accept-Language")


def language_from_headers(headers) -> str:
    """The client's language, or "" when it stated none.

    One implementation, because there are two callers that must agree: the HTTP
    middleware and the WebSocket upload handshake. They did not agree before,
    and the WebSocket half simply had no language, so every progress message
    during an upload came back in English.
    """
    for key in _LANGUAGE_HEADERS:
        value = headers.get(key) if hasattr(headers, "get") else None
        if not value:
            continue
        for part in value.split(","):
            code = part.split(";")[0].strip()
            if code and code != "*":
                return code
    return ""
