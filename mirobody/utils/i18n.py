"""Translated user-facing strings, one JSON file per calling module.

`t("file_empty", language, "file_uploader")` reads `locales/file_uploader.json`,
whose entries are `{key: {lang: text}}` in five languages (en/zh/fr/ja/es), and
falls back requested language → en → zh → the key itself. Language codes come
from the request's `Accept-Language` (`server/middlewares.py`) through the
request context, and `LANGUAGE_CODES` folds their spellings.

This used to be an `I18n` class with a global instance, a module-level wrapper
around each method and — when `module` was omitted — an `inspect` walk up the
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
    # Traditional variants map to the Simplified bundle: we ship a
    # archived/README.zh-TW.md, and Chinese text is closer than the English default.
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


def t(key: str, language: str, module: str, **kwargs) -> str:
    """The text for `key` in `language`, from `locales/<module>.json`."""
    lang_code = LANGUAGE_CODES.get(language.lower(), "en")
    text_dict = _translations(module).get(key, {})
    text = text_dict.get(lang_code) or text_dict.get("en") or text_dict.get("zh") or key
    if kwargs:
        try:
            text = text.format(**kwargs)
        except (KeyError, ValueError):
            pass  # a placeholder mismatch is the JSON's bug; the raw text still says something
    return text
