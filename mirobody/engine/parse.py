"""Document to readings: one LLM call, then the offline resolver.

The only part of the engine that needs a model key. Its imports of the model
clients and document extractors are inside the functions, so `import
mirobody.engine` stays numpy-only.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date

from mirobody.engine.resolver import Resolution, get_resolver, resolve_reading


@dataclass(frozen=True)
class Reading:
    """One extracted observation from a document."""

    name: str
    value: str = ""
    unit: str = ""
    reference_range: str = ""
    #: The collection date printed on the document, ISO `YYYY-MM-DD`, or "".
    #: A reading with no date is not a reading dated today: a caller that
    #: substitutes its own clock puts a May report after an August one.
    collected: str = ""
    resolution: Resolution | None = None


_EXTRACT_PROMPT = """You are a medical lab-report extraction engine.
Extract EVERY health indicator measurement from this document.
Return ONLY a JSON array, no prose. Each element:
{"name": "<indicator name exactly as printed>", "value": "<numeric or textual result>", "unit": "<unit as printed, empty if none>", "reference_range": "<as printed, empty if none>", "collected": "<YYYY-MM-DD the document prints as the collection or examination date, empty if it prints none>"}
Rules: keep the original language of names; do not translate; do not invent
values; skip section headers and non-measurements. `collected` is the same
date for every reading of one report unless the report prints a different one
per row; leave it empty rather than guessing, and never use today's date."""



async def parse_text(document: str, *, resolve_names: bool = True) -> list[Reading]:
    """Parse report TEXT into readings: the same one LLM call as
    :func:`parse_file`, without a file.

    Split out of ``parse_file`` for ``POST /api/standardize``, which is handed
    raw text by the caller and had no way in short of writing a temp file.
    """
    from mirobody.utils import Config
    from mirobody.utils.llm import async_get_text_completion

    await Config.init()
    raw = await async_get_text_completion(
        [
            {"role": "system", "content": _EXTRACT_PROMPT},
            {"role": "user", "content": document},
        ]
    )
    if raw is None:
        # `None` is "no model answered", not "the model answered nothing":
        # either no provider is configured, or every configured one failed.
        # Feeding it to the JSON parser produced "extraction returned non-JSON
        # output: ", an empty quote where the cause should be.
        from mirobody.utils.config.llm import no_provider_message, resolve_route

        if resolve_route("text") is None:
            raise RuntimeError(no_provider_message("text"))
        raise RuntimeError(
            "extraction failed: every configured provider returned an error "
            "(the server log has the provider's message)"
        )
    return _readings_from_json(raw, resolve_names=resolve_names)


async def parse_file(path: str, *, resolve_names: bool = True) -> list[Reading]:
    """Parse a lab report / health document into readings, optionally resolving
    each indicator name to its canonical LOINC identity (offline).

    The document becomes TEXT first (`mirobody.documents.extract`): a PDF's
    embedded text layer page by page, a spreadsheet or Word file as a table,
    and only a scanned page or a photo through the vision provider: one image
    at a time, never the whole file. Then the same one extraction call as
    :func:`parse_text`. A born-digital PDF therefore needs a text model key
    only. Raises RuntimeError with a plain message when no provider key is
    configured or nothing readable was found.
    """
    # The provider auto-detection reads keys through the config system (which
    # also loads .env); standalone callers (the CLI, a bare library user) 
    # haven't initialized it. Init is idempotent and works with zero yaml files.
    from mirobody.utils import Config

    await Config.init()

    from mirobody.documents import extract as documents
    from mirobody.documents.ocr import vision_ocr

    with open(path, "rb") as f:
        data = f.read()
    text = await documents.extract_text(os.path.basename(path), None, data, ocr=vision_ocr)
    if not text.strip():
        raise RuntimeError(f"no readable text could be extracted from {os.path.basename(path)}")
    return await parse_text(text, resolve_names=resolve_names)


def _iso_day(raw: object) -> str:
    """`YYYY-MM-DD` from what the model put in `collected`, or "".

    Models answer this field with "2026-05-06", "2026/05/06", "May 6, 2026"
    and "2026-05-06 09:15:00". Only the first is worth keeping as-is; the
    rest go through `date.fromisoformat` after the separators are squared up,
    and anything else is dropped. An unparseable date is no date: a caller
    that guesses gets a wrong time axis, which is worse than a missing one.
    """
    text = str(raw or "").strip()
    if not text:
        return ""
    text = text.replace("/", "-").replace(".", "-").split(" ")[0].split("T")[0]
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return ""


def _readings_from_json(raw: str, *, resolve_names: bool) -> list[Reading]:
    """The extraction model's JSON array -> Readings. Shared by both parsers."""
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?|```$", "", text).strip()
    try:
        items = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"extraction returned non-JSON output: {text[:200]}") from exc
    if not isinstance(items, list):
        raise RuntimeError(f"extraction returned {type(items).__name__}, expected a JSON array")

    resolver = get_resolver() if resolve_names else None
    readings: list[Reading] = []
    for it in items:
        if not isinstance(it, dict) or not it.get("name"):
            continue
        name = str(it["name"]).strip()
        value = str(it.get("value") or "").strip()
        unit = str(it.get("unit") or "").strip()
        readings.append(
            Reading(
                name=name,
                value=value,
                unit=unit,
                reference_range=str(it.get("reference_range") or "").strip(),
                collected=_iso_day(it.get("collected")),
                # The unit is right here, and LOINC codes the unit into the
                # identity: resolving on the name alone would file a mmol/L
                # reading under the mg/dL code.
                resolution=(
                    resolve_reading(name, value, unit) if resolver else None
                ),
            )
        )
    return readings


__all__ = ["Reading", "parse_file", "parse_text"]
